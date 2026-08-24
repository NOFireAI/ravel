//! Integration tests for the RLOG block-range fetcher (ADR-0107,
//! [`ravel_query::BlockRangeFetcher`]).
//!
//! The fetcher reads only the blocks skip-index pruning proved relevant instead
//! of one whole-object GET per segment, assembling an object-sized buffer with
//! only the directory sections and candidate blocks populated. These tests pin
//! the five acceptance properties of the ADR:
//!
//! 1. Differential: over a candidate subset, the block-range path's decoded rows
//!    are byte-identical to the whole-object path's.
//! 2. Etag pinning: an etag change on a later block-range GET surfaces the typed
//!    [`LogFetchError::EtagChanged`].
//! 3. NotFound mapping: a `NotFound` on a GET surfaces the same
//!    [`LogFetchError::Store`]`{ NotFound }` a whole-object fetch produces, the
//!    shape `ravel_sql`'s `is_segment_not_found` maps to `SnapshotInvalidated`.
//! 4. Corrupt-hit: a corrupted per-block cache hit fails closed, exactly like a
//!    corrupt live fetch (ADR-0046 §4).
//! 5. GET-count: the block-range GET count and byte volume are proportional to
//!    the candidate fraction, not the object size.
//!
//! Tests 1-5 all force the ranged path on a small fixture
//! (`with_whole_object_threshold(0)`) or run without a cache. Tests 6-8 at the
//! end of this file are the counterpart at PRODUCTION defaults: an object above
//! the real 512 KiB threshold, a cache attached, and several partitions striping
//! one segment -- the configuration ADR-0102 decision 1's fan-out gate grants,
//! whose single-flight premise the block-range path has to keep true.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::footer::{self, kind};
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, LogRecord, RlogConfig, RlogWriter, read_section, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, LogFetchError, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CONTENT_HASH: [u8; 32] = [9u8; 32];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        // Must match the fetch tenant: the RLOG read path enforces a footer
        // tenant_hash check.
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One record per block, so an N-record object has N blocks and a ts-range
/// query can select a strict, nontrivial subset of them.
fn one_record_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

fn record(name: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str(name.to_string()))];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Build one RLOG object's bytes from `records` (one block each).
fn build_object(records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(one_record_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

/// A `SegmentRef` for an object of `size` bytes spanning `records`' ts range.
fn seg_ref(key: &str, size: u64, records: &[LogRecord]) -> SegmentRef {
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The object's BLOCKS-section absolute offset and its byte length, and the
/// object's tail length (the bytes after the BLOCKS section, namely
/// SKIP_IDX/BLOOM/POSTINGS then footer and trailer). A probe suffix of exactly
/// `tail_len` covers the whole tail (so no tail-section GET is needed) but
/// reaches no block.
fn layout(bytes: &[u8]) -> (u64, u64, u64) {
    let footer = footer::open(bytes).expect("footer");
    let blocks = footer.section(kind::BLOCKS).expect("BLOCKS");
    let tail_len = bytes.len() as u64 - (blocks.offset + blocks.len);
    (blocks.offset, blocks.len, tail_len)
}

/// The candidate block extents `(abs_start, len, crc32c)` a ts query resolves,
/// mirroring the fetcher, for the GET-count oracle.
fn candidate_extents(bytes: &[u8], ts_min: i64, ts_max: i64) -> Vec<(u64, u64, u32)> {
    let footer = footer::open(bytes).expect("footer");
    let blocks = footer.section(kind::BLOCKS).expect("BLOCKS");
    let skip_desc = footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let skip_raw = read_section(bytes, skip_desc, &RlogConfig::default()).expect("skip raw");
    let skip = SkipIndex::decode(&skip_raw, 1 << 24).expect("skip decode");
    skip.candidate_blocks(ts_min, ts_max, None, &[])
        .into_iter()
        .map(|i| {
            let e = &skip.l0[i];
            (blocks.offset + e.block_offset, e.block_len, e.block_crc32c)
        })
        .collect()
}

/// Number of maximal contiguous runs (gap 0) among candidate extents: the exact
/// coalesced-GET count the fetcher issues with `coalesce_gap == 0`.
fn contiguous_runs(mut ext: Vec<(u64, u64, u32)>) -> usize {
    ext.sort_by_key(|e| e.0);
    let mut runs = 0usize;
    let mut prev_end: Option<u64> = None;
    for (start, len, _) in ext {
        match prev_end {
            Some(e) if start <= e => {}
            _ => runs += 1,
        }
        prev_end = Some(prev_end.map_or(start + len, |e| e.max(start + len)));
    }
    runs
}

// ---- store doubles -------------------------------------------------------

/// Counts `get` calls; everything else delegates.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        CountingStore {
            inner,
            gets: AtomicU64::new(0),
        }
    }
    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

/// Swaps the etag of any `Range` GET starting at or beyond `blocks_offset` to a
/// different value, so a block-range GET (never the front-metadata GETs, which
/// start before `blocks_offset`, nor the `Suffix` probe) observes an etag
/// different from the probe's.
struct EtagSwapStore {
    inner: Arc<MemoryStore>,
    blocks_offset: u64,
}

/// Returns `NotFound` for any `Range` GET starting at or beyond `blocks_offset`,
/// simulating a segment compacted away after its blocks were resolved.
struct NotFoundOnBlockStore {
    inner: Arc<MemoryStore>,
    blocks_offset: u64,
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for EtagSwapStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let mut got = self.inner.get(key, range).await?;
        if let GetRange::Range(start, _) = range
            && start >= self.blocks_offset
        {
            got.etag = Etag("swapped-mid-sequence".to_string());
        }
        Ok(got)
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for NotFoundOnBlockStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if let GetRange::Range(start, _) = range
            && start >= self.blocks_offset
        {
            return Err(StoreError::NotFound);
        }
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// A 20-block object (ts 0..=19, one record per block) whose ts query [6,13]
/// selects a strict subset of blocks.
fn subset_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records: Vec<LogRecord> = (0..20)
        .map(|ts| record("api", ts, if ts == 8 { "connection timeout" } else { "ok" }))
        .collect();
    let bytes = build_object(&records);
    (records, bytes)
}

// ---- Test 1: differential -----------------------------------------------

/// The block-range path's decoded rows are byte-identical to the whole-object
/// path's over the same candidate (ts) set, even though the block-range buffer
/// holds only the candidate blocks (pruned blocks are zeroed gaps the reader
/// never touches). The probe suffix is sized to the tail only, so the ranged
/// path genuinely range-fetches the front metadata and the candidate blocks.
#[tokio::test]
async fn block_range_rows_match_whole_object_over_same_candidate_set() {
    let (records, bytes) = subset_object();
    let (blocks_offset, _blocks_len, tail_len) = layout(&bytes);
    assert!(blocks_offset > 0 && tail_len > 0);

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/s.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/s.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;

    let query = LogQuery::new(6, 13);

    // Whole-object path: plain `fetch` issues one GetRange::Full and scans it.
    let whole = LogSegmentFetcher::new(Arc::clone(&store))
        .fetch(&seg, &query)
        .await
        .expect("whole fetch")
        .expect("in range");

    // Block-range path: route every object through it, probe only the tail, and
    // disable the coverage crossover so the true per-block ranged path runs.
    let br = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0);
    let ranged = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(br)
        .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("ranged fetch")
        .expect("in range");

    assert!(!whole.records.is_empty(), "the subset is nontrivial");
    assert_eq!(
        whole.records, ranged.records,
        "block-range decoded rows must be byte-identical to whole-object rows"
    );

    // The candidate set is a strict, nontrivial subset of the 20 blocks.
    let cands = candidate_extents(&bytes, 6, 13);
    assert!(
        cands.len() > 1 && cands.len() < 20,
        "candidate blocks must be a strict nontrivial subset, got {}",
        cands.len()
    );
}

// ---- Test 2: etag pinning ------------------------------------------------

/// An etag change between the probe and a later block-range GET surfaces the
/// typed [`LogFetchError::EtagChanged`]. The store swaps the etag on any GET at
/// or beyond the BLOCKS offset, so the probe (a suffix over the tail) and the
/// front-metadata GETs keep the pinned etag and a block-range GET is the one
/// that observes the change.
///
/// Prove-the-test: with the etag check in `BlockRangeFetcher::store_get_pinned`
/// removed (the `if &got.etag != expected` guard), the store still returns the
/// correct bytes (only the etag differs), so the fetch succeeds silently and
/// this assertion fails with `Ok`. Restoring the guard makes it pass.
#[tokio::test]
async fn etag_change_on_block_range_get_is_a_typed_error() {
    let (records, bytes) = subset_object();
    let (blocks_offset, _blocks_len, tail_len) = layout(&bytes);

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/e.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/e.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(EtagSwapStore {
        inner: mem,
        blocks_offset,
    });

    let br = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0);

    let err = br
        .fetch_object(&seg, TENANT, 6, 13, &QueryAccounting::new())
        .await
        .expect_err("etag change must surface as an error");
    assert!(
        matches!(err, LogFetchError::EtagChanged { .. }),
        "expected EtagChanged, got {err:?}"
    );
}

// ---- Test 3: NotFound mapping -------------------------------------------

/// A `NotFound` on a block-range GET surfaces the SAME
/// `LogFetchError::Store { source: NotFound }` a whole-object fetch produces for
/// a vanished pinned segment. That shape is exactly what
/// `ravel_sql::SqlError::is_segment_not_found` maps to the `SnapshotInvalidated`
/// retry path, so no second mapping is introduced (ADR-0107 decision 1).
#[tokio::test]
async fn not_found_on_block_range_get_is_the_segment_not_found_shape() {
    let (records, bytes) = subset_object();
    let (blocks_offset, _blocks_len, tail_len) = layout(&bytes);

    // Whole-object path oracle: a plain fetch of a missing object.
    let empty = Arc::new(MemoryStore::new());
    let seg_missing = seg_ref("logs/missing.rlog", bytes.len() as u64, &records);
    let whole_err = LogSegmentFetcher::new(empty as Arc<dyn ObjectStoreBackend>)
        .fetch(&seg_missing, &LogQuery::new(6, 13))
        .await
        .expect_err("missing object");
    assert!(
        matches!(
            whole_err,
            LogFetchError::Store {
                source: StoreError::NotFound,
                ..
            }
        ),
        "whole-object NotFound shape, got {whole_err:?}"
    );

    // Block-range path: the segment exists for the probe/metadata but its blocks
    // vanish (compacted away) before the block-range GET.
    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/nf.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/nf.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(NotFoundOnBlockStore {
        inner: mem,
        blocks_offset,
    });
    let br = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0);
    let err = br
        .fetch_object(&seg, TENANT, 6, 13, &QueryAccounting::new())
        .await
        .expect_err("block NotFound");
    assert!(
        matches!(
            err,
            LogFetchError::Store {
                source: StoreError::NotFound,
                ..
            }
        ),
        "block-range NotFound must be the same Store{{NotFound}} shape, got {err:?}"
    );
}

// ---- Test 4: corrupt cache hit ------------------------------------------

/// A corrupted per-block cache hit fails closed exactly like a corrupt live
/// fetch (ADR-0046 §4 / ADR-0107 decision 3). Using `Cache::with_corruption`,
/// the first `fetch_object` populates the per-block cache (a miss, clean bytes);
/// the second is a genuine hit against the now-corrupted entry, which the
/// fetcher's own `block_crc32c` re-verification must reject.
///
/// Prove-the-test: with the cache-hit `verify_block_crc(key, &bytes, ext)?`
/// removed from `BlockRangeFetcher::fetch_blocks`, the corrupted block is placed
/// into the assembled buffer and `fetch_object` returns `Ok`, so this assertion
/// fails. Restoring the check makes it fail closed with `Corrupt`.
#[tokio::test]
async fn corrupt_per_block_cache_hit_fails_closed() {
    let (records, bytes) = subset_object();
    let (blocks_offset, _blocks_len, tail_len) = layout(&bytes);
    let _ = blocks_offset;

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/c.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/c.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;

    let cache = Arc::new(Cache::with_corruption(CacheLimits::new(
        1 << 20,
        1024,
        1 << 20,
    )));
    let br = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0)
        .with_cache(cache);

    // First call: cache miss, clean bytes, admits per-block entries.
    let (_bytes, _stats) = br
        .fetch_object(&seg, TENANT, 6, 13, &QueryAccounting::new())
        .await
        .expect("first fetch (miss) succeeds");

    // Second call: a genuine per-block cache hit returns corrupted bytes.
    let err = br
        .fetch_object(&seg, TENANT, 6, 13, &QueryAccounting::new())
        .await
        .expect_err("corrupted cache hit must fail closed");
    assert!(
        matches!(err, LogFetchError::Corrupt { .. }),
        "expected Corrupt on a corrupted block hit, got {err:?}"
    );
}

// ---- Test 5: GET-count proportionality ----------------------------------

/// The block-range GET count and byte volume are proportional to the candidate
/// fraction, not the object size. With `coalesce_gap == 0`, the block-range GET
/// count equals the number of maximal contiguous runs among candidate blocks; a
/// suffix sized to the tail makes the fixed overhead exactly one probe GET plus
/// one GET per uncovered front section (STREAM_DIR, FIELD_DIR). The asserted
/// figures are exact, not "fewer than before".
#[tokio::test]
async fn block_range_get_count_is_proportional_to_candidate_fraction() {
    let (records, bytes) = subset_object();
    let (blocks_offset, blocks_len, tail_len) = layout(&bytes);
    assert!(blocks_offset > 0 && blocks_len > 0);

    let (ts_min, ts_max) = (6, 13);
    let cands = candidate_extents(&bytes, ts_min, ts_max);
    let candidate_bytes: u64 = cands.iter().map(|(_, len, _)| *len).sum();
    let expected_runs = contiguous_runs(cands.clone());
    let expected_candidates = cands.len() as u64;

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/g.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/g.rlog", bytes.len() as u64, &records);
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();

    let br = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coalesce_gap(0)
        .with_coverage_threshold(2.0);

    let (_assembled, stats) = br
        .fetch_object(&seg, TENANT, ts_min, ts_max, &QueryAccounting::new())
        .await
        .expect("ranged fetch");

    // The candidate set is a known strict fraction of the 20 blocks.
    assert_eq!(stats.candidate_blocks, expected_candidates);
    assert!(
        (2..=12).contains(&expected_candidates),
        "sanity: subset fraction, got {expected_candidates}/20"
    );

    // Block-range GETs: exactly one per contiguous candidate run (gap 0).
    assert_eq!(
        stats.block_range_gets, expected_runs as u64,
        "one coalesced GET per contiguous candidate run"
    );
    // Bytes read for blocks equal the candidate extents' total, and are a strict
    // fraction of the object's block region (pruning-proportional).
    assert_eq!(stats.block_bytes_fetched, candidate_bytes);
    assert!(
        candidate_bytes * 2 < blocks_len,
        "candidate bytes {candidate_bytes} must be far under the block region {blocks_len}"
    );

    // Total store GETs: probe (1) + front metadata (STREAM_DIR + FIELD_DIR = 2)
    // + one per contiguous candidate run. The tail (SKIP_IDX/BLOOM/POSTINGS +
    // footer) is covered by the probe, so it costs no extra GET.
    let expected_total = 1 + 2 + expected_runs as u64;
    assert_eq!(
        counting.get_count(),
        expected_total,
        "probe(1) + front-meta(2) + block runs({expected_runs})"
    );
    assert_eq!(stats.probe_gets, 1);
    assert_eq!(stats.metadata_gets, 2);
    assert!(!stats.whole_object);
}

// ---- Tests 6-8: production defaults, cached, multi-partition ---------------
//
// Everything below runs at the REAL 512 KiB `block_range_threshold` with a real
// cache attached, on an object big enough to route there on its own. Tests 1-5
// above all avoid that configuration (threshold 0, or no cache), which is
// exactly the configuration ADR-0102 decision 1's fan-out gate grants: several
// partitions striping ONE segment, coalescing onto one request each through
// ADR-0046's single-flight.

/// Records whose bodies are high-entropy enough that zstd level 3 cannot shrink
/// the object back under the 512 KiB threshold: 24 KiB of 64-alphabet noise per
/// record, one record per block. Deterministic (a fixed LCG), so the object's
/// size and layout are the same on every run.
fn big_records(count: i64) -> Vec<LogRecord> {
    const ALPHABET: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    (0..count)
        .map(|ts| {
            let mut body = String::with_capacity(24 * 1024);
            for i in 0..24 * 1024 {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                // A space every 16 bytes keeps the body tokenizable rather than
                // one enormous term, without lowering entropy meaningfully.
                if i % 16 == 15 {
                    body.push(' ');
                } else {
                    body.push(ALPHABET[(state >> 33) as usize % 64] as char);
                }
            }
            record("api", ts, &body)
        })
        .collect()
}

/// An object above the production 512 KiB threshold: 40 one-record blocks of
/// ~24 KiB each. Returns the records and the object bytes.
fn large_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records = big_records(40);
    let bytes = build_object(&records);
    (records, bytes)
}

/// Non-BLOCKS sections the probe window `[total - suffix, total)` does NOT
/// already cover: one metadata GET each.
fn uncovered_sections(bytes: &[u8], suffix: u64) -> u64 {
    let footer = footer::open(bytes).expect("footer");
    let total = bytes.len() as u64;
    let probe_start = total.saturating_sub(suffix);
    footer
        .sections
        .iter()
        .filter(|s| s.kind != kind::BLOCKS)
        .filter(|s| s.offset < probe_start || s.offset + s.len > total)
        .count() as u64
}

/// Coalesced runs among candidate extents at a gap threshold: the exact number
/// of block-range GETs the fetcher issues for them.
fn coalesced_runs(mut ext: Vec<(u64, u64, u32)>, gap: u64) -> u64 {
    ext.sort_by_key(|e| e.0);
    let mut runs = 0u64;
    let mut prev_end: Option<u64> = None;
    for (start, len, _) in ext {
        match prev_end {
            Some(end) if start <= end + gap => {}
            _ => runs += 1,
        }
        prev_end = Some(prev_end.map_or(start + len, |e| e.max(start + len)));
    }
    runs
}

/// The whole fixed cost of one segment's block-range read at production
/// defaults: the etag-establishing probe, one GET per non-BLOCKS section the
/// probe missed, and one GET per coalesced candidate run. Asserts along the way
/// that the fixture really exercises the path under test (above the threshold,
/// candidates outside the probe window, coverage under the crossover).
fn expected_segment_gets(bytes: &[u8], ts_min: i64, ts_max: i64) -> u64 {
    let total = bytes.len() as u64;
    assert!(
        total > ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        "fixture must be above the production threshold, got {total} bytes"
    );
    let suffix = ravel_query::DEFAULT_LOG_SUFFIX_LEN;
    let cands = candidate_extents(bytes, ts_min, ts_max);
    assert!(
        cands.len() > 1 && cands.len() < 40,
        "candidates must be a strict nontrivial subset, got {}",
        cands.len()
    );
    // No candidate may fall inside the probe window: a probe-covered block costs
    // no GET, and the oracle below counts one GET per run.
    let probe_start = total - suffix;
    assert!(
        cands
            .iter()
            .all(|(start, len, _)| start + len <= probe_start),
        "fixture must select blocks outside the probe window"
    );
    // Under the coverage crossover, so the ranged path (not a whole-object GET)
    // is what runs.
    let footer = footer::open(bytes).expect("footer");
    let blocks = footer.section(kind::BLOCKS).expect("BLOCKS");
    let candidate_bytes: u64 = cands.iter().map(|(_, len, _)| *len).sum();
    let coverage = candidate_bytes as f64 / blocks.len as f64;
    assert!(
        coverage < ravel_query::DEFAULT_LOG_COVERAGE_THRESHOLD,
        "fixture must stay under the coverage crossover, got {coverage}"
    );
    1 + uncovered_sections(bytes, suffix)
        + coalesced_runs(cands, ravel_query::DEFAULT_LOG_COALESCE_GAP)
}

/// [`CountingStore`] with a real object store's latency (a few ms per GET, far
/// below S3's 15-80 ms). Concurrency tests need it: with a zero-latency store a
/// leader can finish its whole fetch before a peer even asks, and
/// `Cache::get_or_fetch` releases its in-flight slot just before it admits the
/// bytes, so a peer landing in that microsecond window becomes a second leader
/// for the same key and the request count stops being a function of the fetch
/// protocol. Under any latency at all, concurrent callers for one key subscribe
/// while the leader is still in flight, which is the behavior ADR-0046's
/// single-flight exists to provide and the one this test is asserting about.
struct SlowCountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
}

impl SlowCountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        SlowCountingStore {
            inner,
            gets: AtomicU64::new(0),
        }
    }
    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for SlowCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn big_cache() -> Arc<Cache<ravel_query::CacheFetchError>> {
    // Far larger than the fixture, so nothing is evicted mid-test: an eviction
    // would turn a cache hit into a real GET and make the count nondeterministic
    // for a reason that has nothing to do with single-flight.
    Arc::new(Cache::new(CacheLimits::new(1 << 26, 1 << 23, 1 << 26)))
}

/// Test 6: the production shape ADR-0102 decision 1 ships -- one shared
/// `plan_segment` (the `OnceCell` in `LogsScanExec::compute_plan_counts`),
/// then N partitions each draining its own stripe of the same segment's
/// surviving blocks -- costs the SAME number of store GETs for any N.
///
/// The count asserted is exact, not "fewer than before": one etag-establishing
/// probe, one GET per non-BLOCKS section the probe did not cover, and one GET
/// per coalesced candidate run. Every partition after the planning read finds
/// each of those extents already resident under its own cache key and issues
/// nothing.
///
/// Prove-the-test: this fails on the pre-fix fetcher on the probe alone. The
/// etag-establishing suffix GET was never cache-checked, so every partition and
/// every `plan_segment` call paid one unconditionally: the count was
/// `expected + N` rather than `expected`, and differed between N = 2 and N = 8.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_partitions_striping_one_large_segment_cost_a_partition_independent_get_count() {
    let (records, bytes) = large_object();
    let (ts_min, ts_max) = (2, 11);
    let expected = expected_segment_gets(&bytes, ts_min, ts_max);

    let mut counts = Vec::new();
    for partitions in [2usize, 8] {
        let mem = Arc::new(MemoryStore::new());
        mem.put("logs/big.rlog", bytes.clone().into(), PutOptions::default())
            .await
            .expect("put");
        let counting = Arc::new(CountingStore::new(mem));
        let store: Arc<dyn ObjectStoreBackend> = counting.clone();
        let seg = seg_ref("logs/big.rlog", bytes.len() as u64, &records);
        let query = LogQuery::new(ts_min, ts_max);

        // Production defaults throughout: no threshold override, no probe-length
        // override, no coverage override. Only the cache is wired, exactly as
        // `build_sql_state` wires it.
        let fetcher = LogSegmentFetcher::new(store).with_cache(big_cache());
        assert!(
            fetcher.has_cache(),
            "the ADR-0102 fan-out gate this test stands in for reads has_cache()"
        );

        // The shared planning read, then the per-partition stripes.
        let accounting = QueryAccounting::new();
        let (surviving, _stats) = fetcher
            .plan_segment(&seg, TENANT, &query, &accounting)
            .await
            .expect("plan")
            .expect("in range");
        assert!(
            surviving > partitions,
            "the stripe must be nontrivial: {surviving} surviving blocks over {partitions} \
             partitions"
        );

        let mut tasks = Vec::new();
        for p in 0..partitions {
            let indices: Vec<usize> = (p..surviving).step_by(partitions).collect();
            let fetcher = fetcher.clone();
            let seg = seg.clone();
            let query = query.clone();
            tasks.push(tokio::spawn(async move {
                let mut scan = fetcher
                    .scan_accounted_with_tenant_subset(
                        &seg,
                        TENANT,
                        &query,
                        &ravel_logseg::ColumnSelection::all(),
                        &indices,
                        &QueryAccounting::new(),
                    )
                    .await
                    .expect("subset scan")
                    .expect("in range");
                let mut rows = 0usize;
                while let Some(block) = scan.next_block().expect("decode") {
                    rows += block.len();
                }
                rows
            }));
        }
        let mut rows = 0usize;
        for task in tasks {
            rows += task.await.expect("partition task");
        }
        assert_eq!(
            rows,
            (ts_max - ts_min + 1) as usize,
            "the partitions together must return every matching row exactly once"
        );

        assert_eq!(
            counting.get_count(),
            expected,
            "{partitions} partitions striping one segment must cost probe(1) + uncovered \
             sections + coalesced runs = {expected} store GETs, independent of the partition count"
        );
        counts.push(counting.get_count());
    }
    assert_eq!(
        counts[0], counts[1],
        "the store GET count must not grow with the partition count"
    );
}

/// Test 7: the same segment read by N partitions CONCURRENTLY from a cold
/// cache, with no planning read to warm it. Nothing but ADR-0046's single-flight
/// can hold the request count down here: every partition resolves the same
/// probe, the same sections, and the same candidate run at the same moment.
///
/// The asserted count is exact and identical to test 6's: one GET per distinct
/// extent, for any number of concurrent partitions. The pre-fix figure is
/// `N * expected`. The store carries a few ms of latency per GET
/// ([`SlowCountingStore`]) so the count is a function of the fetch protocol
/// rather than of who happened to win a microsecond race on a zero-latency
/// store; see that type's doc.
///
/// This also pins the per-partition memory shape the docs quote: each partition
/// assembles its OWN object-sized buffer, so resident raw bytes above the
/// threshold are `N * object_size`, not one shared object (the whole-object path
/// below the threshold hands every partition a clone of one `Bytes`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_partitions_collapse_onto_one_get_per_extent() {
    let (records, bytes) = large_object();
    let (ts_min, ts_max) = (2, 11);
    let expected = expected_segment_gets(&bytes, ts_min, ts_max);
    const PARTITIONS: usize = 8;

    let mem = Arc::new(MemoryStore::new());
    mem.put(
        "logs/cold.rlog",
        bytes.clone().into(),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let counting = Arc::new(SlowCountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let seg = seg_ref("logs/cold.rlog", bytes.len() as u64, &records);

    let br = BlockRangeFetcher::new(store).with_cache(big_cache());
    let mut tasks = Vec::new();
    for _ in 0..PARTITIONS {
        let br = br.clone();
        let seg = seg.clone();
        tasks.push(tokio::spawn(async move {
            br.fetch_object(&seg, TENANT, ts_min, ts_max, &QueryAccounting::new())
                .await
                .expect("concurrent block-range fetch")
        }));
    }
    let mut resident_bytes = 0u64;
    for task in tasks {
        let (assembled, stats) = task.await.expect("partition task");
        assert!(
            !stats.whole_object,
            "the fixture must take the ranged path, not a crossover"
        );
        assert_eq!(
            assembled.len() as u64,
            bytes.len() as u64,
            "every partition assembles its own object-sized buffer"
        );
        resident_bytes += assembled.len() as u64;
    }

    assert_eq!(
        counting.get_count(),
        expected,
        "{PARTITIONS} concurrent cold fetches of one segment must issue exactly one GET per \
         distinct extent ({expected}: probe + uncovered sections + coalesced runs); \
         un-coalesced the same work is {}",
        expected * PARTITIONS as u64
    );
    assert_eq!(
        resident_bytes,
        bytes.len() as u64 * PARTITIONS as u64,
        "resident raw bytes above the threshold are one object-sized buffer per partition"
    );
}

/// Test 8: the per-block corrupt-hit gate (test 4's property) at production
/// defaults, with ONLY the block entries resident.
///
/// Test 4 populates the whole cache and reads it back corrupted, which after
/// probe caching (ADR-0107 + this fix) can trip at the probe's own decode rather
/// than at the block gate. This one leaves the probe and the sections as misses
/// -- `get_or_fetch` returns the leader's live bytes, which the corrupting cache
/// never touches -- and pre-admits exactly the candidate blocks, so the ONLY
/// corrupted hit is a block and the gate under test is the block gate.
///
/// Prove-the-test: with the cache-hit `verify_block_crc` removed from
/// `fetch_blocks`, the corrupted block bytes are placed into the assembled
/// buffer and this returns `Ok`.
#[tokio::test]
async fn corrupt_block_hit_fails_closed_with_only_blocks_resident() {
    let (records, bytes) = large_object();
    let (ts_min, ts_max) = (2, 11);
    let cands = candidate_extents(&bytes, ts_min, ts_max);

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/cb.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/cb.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;

    let cache: Arc<Cache<ravel_query::CacheFetchError>> = Arc::new(Cache::with_corruption(
        CacheLimits::new(1 << 26, 1 << 23, 1 << 26),
    ));
    // Pre-admit the candidate blocks (clean bytes, their real keys) and nothing
    // else. `Cache::with_corruption` corrupts what a `get` serves, so each of
    // these is a genuine corrupted per-block hit.
    for (start, len, _) in &cands {
        let (s, e) = (*start as usize, (*start + *len) as usize);
        cache.insert(
            ravel_cache::CacheKey::new(TENANT.0, CONTENT_HASH, *start, *len),
            bytes::Bytes::copy_from_slice(&bytes[s..e]),
        );
    }

    let err = LogSegmentFetcher::new(store)
        .with_cache(cache)
        .fetch_accounted_with_tenant(
            &seg,
            TENANT,
            &LogQuery::new(ts_min, ts_max),
            &QueryAccounting::new(),
        )
        .await
        .expect_err("a corrupted per-block cache hit must fail closed");
    assert!(
        matches!(err, LogFetchError::Corrupt { .. }),
        "expected Corrupt from the per-block gate, got {err:?}"
    );
}
