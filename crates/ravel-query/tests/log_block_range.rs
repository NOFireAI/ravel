//! Integration tests for the RLOG block-range fetcher (ADR-0107,
//! [`ravel_query::BlockRangeFetcher`]).
//!
//! The fetcher reads only the sections and blocks skip-index pruning proved
//! relevant instead of one whole-object GET per segment, assembling an
//! object-sized buffer with only the parts it fetched populated. These tests pin
//! the acceptance properties of the ADR that survive ADR-0892:
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
//! 5. Probe sizing: the suffix probe is derived from the object size, and a
//!    trailer larger than it costs exactly one extra counted request.
//!
//! The GET-count laws ADR-0107 stated over version-3 block extents, and the
//! production-defaults striping tests built on them, are NOT here: ADR-0892
//! deleted the version-3 reader, so no readable object addresses its blocks by
//! byte range any more and those laws describe an unreachable path. The
//! version-4 fetch shape they become is `tests/log_page_dir_fetch.rs`'s
//! subject.
//!
//! Every test below forces the ranged path on a small fixture
//! (`with_whole_object_threshold(0)`) or runs without a cache.

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
use ravel_query::{
    AssemblyBufferStats, BlockRangeFetcher, LogFetchError, LogQuery, LogSegmentFetcher,
};
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
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

/// A 20-block object over the same ts range as [`subset_object`] but with
/// different, longer bodies, so its object bytes differ at essentially every
/// offset and its total size is larger. Used to DIRTY a pooled assembly buffer
/// before the object under test reuses it (issue #894).
fn dirtying_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records: Vec<LogRecord> = (0..20)
        .map(|ts| {
            record(
                "billing",
                ts,
                "an entirely different body, long enough that this object is the larger of the two",
            )
        })
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

// ---- Test 1b: the assembly buffer is pooled, not allocated per object -----

/// Issue #894: three ranged reads of one object cost ONE object-sized buffer
/// allocation and ONE whole-object zeroing between them, not three of each.
/// The figures are exact: the second and third reads check out the buffer the
/// first returned, at no allocation and no `memset`.
///
/// Prove-the-test: restore `ObjectAssembler::new`'s body to
/// `buf: vec![0u8; total_size]` (dropping the `pool.acquire` call) and this
/// asserts `allocated: 3, reused: 0, zeroed_bytes: 3 * object_size` instead.
#[tokio::test]
async fn ranged_reads_reuse_one_pooled_assembly_buffer() {
    let (records, bytes) = subset_object();
    let (_blocks_offset, _blocks_len, tail_len) = layout(&bytes);
    let object_size = bytes.len() as u64;

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/s.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/s.rlog", object_size, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;

    let br = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0);
    assert_eq!(br.assembly_buffer_stats(), AssemblyBufferStats::default());

    for _ in 0..3 {
        let (assembled, stats) = br
            .fetch_object(&seg, TENANT, 6, 13, &QueryAccounting::new())
            .await
            .expect("ranged fetch");
        assert!(!stats.whole_object, "the ranged path ran, not a crossover");
        assert_eq!(assembled.len() as u64, object_size);
        // The buffer goes back to the pool when the assembled `Bytes` that
        // borrows it is dropped, which is what the next iteration reuses.
        drop(assembled);
    }

    assert_eq!(
        br.assembly_buffer_stats(),
        AssemblyBufferStats {
            allocated: 1,
            reused: 2,
            zeroed_bytes: object_size,
        }
    );
}

// ---- Test 1c: differential ON a dirty reused buffer ----------------------

/// The differential above, but with the pooled buffer already carrying ANOTHER
/// object's bytes in the gaps: the second read's rows must still be
/// byte-identical to the whole-object path's. Without the pool the gaps were
/// zeros, so this is the case reuse introduced (issue #894).
///
/// Prove-the-test: this passes before the change too (there the buffer is
/// freshly zeroed); it is the guard that the change did not break the
/// invariant, and it fails if `place`/`slice` ever stop covering a byte the
/// reader interprets -- as it does if `ObjectAssembler::new` reuses a buffer
/// while `AssemblyBuffer::as_slice` returns the whole vector rather than
/// `[..len]`.
#[tokio::test]
async fn ranged_rows_match_whole_object_on_a_buffer_dirtied_by_another_object() {
    let (dirty_records, dirty_bytes) = dirtying_object();
    let (records, bytes) = subset_object();
    let (_blocks_offset, _blocks_len, tail_len) = layout(&bytes);
    assert!(
        dirty_bytes.len() >= bytes.len(),
        "the dirtying object must be at least as large, or the buffer it leaves \
         behind does not span the object under test: {} vs {}",
        dirty_bytes.len(),
        bytes.len()
    );

    let mem = Arc::new(MemoryStore::new());
    mem.put(
        "logs/dirty.rlog",
        dirty_bytes.clone().into(),
        PutOptions::default(),
    )
    .await
    .expect("put dirty");
    mem.put("logs/s.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let dirty_seg = seg_ref("logs/dirty.rlog", dirty_bytes.len() as u64, &dirty_records);
    let seg = seg_ref("logs/s.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;

    let query = LogQuery::new(6, 13);

    let whole = LogSegmentFetcher::new(Arc::clone(&store))
        .fetch(&seg, &query)
        .await
        .expect("whole fetch")
        .expect("in range");

    let br = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(0)
        .with_suffix_len(tail_len)
        .with_coverage_threshold(2.0);
    let ranged = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(0)
        .with_block_range(br.clone());

    // First read: the other object, purely to leave its bytes in the buffer.
    let dirtying = ranged
        .fetch_accounted_with_tenant(&dirty_seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("dirtying fetch")
        .expect("in range");
    assert!(
        !dirtying.records.is_empty(),
        "the dirtying read decoded rows"
    );
    drop(dirtying);
    assert_eq!(
        br.assembly_buffer_stats().allocated,
        1,
        "one buffer so far, now idle in the pool"
    );

    // Second read: the object under test, on that same buffer.
    let reused = ranged
        .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("ranged fetch")
        .expect("in range");
    assert_eq!(
        br.assembly_buffer_stats().reused,
        1,
        "the second read ran on the first read's buffer, not a fresh one"
    );

    assert!(!whole.records.is_empty(), "the subset is nontrivial");
    assert_eq!(
        whole.records, reused.records,
        "a ranged read on a buffer dirtied by another object must decode the \
         same rows as the whole-object path"
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

/// A corrupted cache hit fails closed exactly like a corrupt live fetch
/// (ADR-0046 §4 / ADR-0107 decision 3). Using `Cache::with_corruption`, the
/// first `fetch_object` populates the cache (misses, clean bytes -- probe,
/// sections, and blocks alike); the second call re-reads every one of those
/// entries, now corrupted.
///
/// This does NOT isolate the per-block gate: since this fix's probe caching
/// (decision 2), the second call's probe hit is corrupted too, and its
/// `open_from_suffix` decode fails with a bad-magic `Corrupted` error before
/// any block is ever reached. That is still a genuine fail-closed result (a
/// corrupted cache entry, wherever it lives, must never surface as `Ok`), but
/// the failure this test actually exercises is the probe's decode-time
/// rejection, not `fetch_blocks`'s `verify_block_crc`. Test
/// `corrupt_block_hit_fails_closed_with_only_blocks_resident` isolates the
/// per-block gate specifically, by pre-admitting only block entries so the
/// probe and sections stay clean.
///
/// Prove-the-test: with the etag/footer decode's own corruption handling
/// intact but every OTHER integrity check in the fetch path bypassed, this
/// assertion still fails closed purely on the probe's corrupted bytes -- this
/// test does not need `fetch_blocks`'s block-crc gate to pass, and does not
/// prove it. See the isolated test above for that proof.
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

// ---- Decode-time page accounting (ADR-0107 decision 4) --------------------

// ---- Test 9: RLOG version 4 ----------------------------------------------
//
// A version-4 object does not take the protocol this file tests: its pages are
// stored column-major inside row groups, so a block is neither a contiguous
// byte range nor covered by a crc over one, and `BlockRangeFetcher` routes it
// through ADR-0699 decision 5's column-chunk path instead. That path, and the
// test that used to pin the interim whole-object fallback here
// (`version_4_object_is_read_whole_until_the_page_dir_fetcher_lands`, now
// `version_4_projected_read_fetches_one_range_per_group_and_column`), live in
// `tests/log_page_dir_fetch.rs`.

// ---- Tests 10-11: the request-cost-driven decisions (issues #835, #862) -----
//
// The whole-object-vs-ranged decision is driven from one configurable quantity,
// the request cost (a latency-bandwidth product; `BlockRangeFetcher::
// with_request_cost_bytes`), not from a byte-only size threshold. Test 10 pins
// that by reading whole an object the byte-only rule would have range-fetched,
// because the request cost says so; test 11 pins that a large coalescing gap (also
// driven from the request cost) never turns a genuinely narrow read into a
// whole-object one.

// ---- Object-size-derived suffix probe (issue #883) ------------------------

/// An object above the 512 KiB threshold whose TAIL is small: a few blocks of
/// many records each, inflated by high-entropy NUMERIC columns (which land in
/// BLOCKS and in compact per-block SKIP_IDX stats, never in BLOOM) with a
/// one-token body (so BLOOM stays tiny). This is the ClickBench shape -- few
/// blocks, structured columns -- rather than `large_object`'s one-record,
/// high-entropy-text blocks whose per-block BLOOM alone makes the tail exceed
/// the derived floor. Returns the records and the object bytes.
fn small_tail_object() -> (Vec<LogRecord>, Vec<u8>) {
    const COLUMNS: usize = 12;
    const RECORDS: i64 = 12_288; // three full 4096-record blocks
    let cfg = RlogConfig {
        block_target_records: 4096,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut records = Vec::with_capacity(RECORDS as usize);
    for ts in 0..RECORDS {
        let attrs: Vec<(String, AttrValue)> = (0..COLUMNS)
            .map(|c| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (format!("n{c:02}"), AttrValue::I64(state as i64))
            })
            .collect();
        let r = LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "x".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        };
        w.push(r.clone()).expect("push");
        records.push(r);
    }
    let bytes = w.finish().expect("finish");
    (records, bytes)
}

/// The l0 shape of a decoded skip index, `(block_offset, block_len,
/// block_crc32c)` per block: the byte-level oracle a ranged read must reproduce
/// no matter how short its probe was.
fn skip_shape(skip: &SkipIndex) -> Vec<(u64, u64, u32)> {
    skip.l0
        .iter()
        .map(|e| (e.block_offset, e.block_len, e.block_crc32c))
        .collect()
}

/// The SKIP_IDX decoded straight from the object bytes, the oracle for the
/// fetcher's own decode.
fn oracle_skip(bytes: &[u8]) -> SkipIndex {
    let footer = footer::open(bytes).expect("footer");
    let skip_desc = footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let raw = read_section(bytes, skip_desc, &RlogConfig::default()).expect("skip raw");
    SkipIndex::decode(&raw, 1 << 24).expect("skip decode")
}

/// The probe length is chosen from the object size, not a flat 256 KiB (issue
/// #883): `object_size / LOG_SUFFIX_SIZE_DIVISOR`, floored at
/// `LOG_SUFFIX_FLOOR_BYTES` and ceilinged at `DEFAULT_LOG_SUFFIX_LEN`. This pins
/// the derivation at representative sizes so a later change to the constants or
/// the formula fails loudly rather than drifting the probe.
///
/// Prove-the-test: changing `LOG_SUFFIX_SIZE_DIVISOR` (e.g. 32 -> 16) or
/// `LOG_SUFFIX_FLOOR_BYTES` moves these expected values, and the `assert_eq!`s
/// fire. Demonstrated failing during development by setting the divisor to 16
/// (the 6 MiB case then derives 393216, not 196608).
#[test]
fn derives_probe_from_object_size() {
    // The three constants the formula is built from.
    assert_eq!(ravel_query::LOG_SUFFIX_FLOOR_BYTES, 128 * 1024);
    assert_eq!(ravel_query::DEFAULT_LOG_SUFFIX_LEN, 256 * 1024);
    assert_eq!(ravel_query::LOG_SUFFIX_SIZE_DIVISOR, 32);

    let d = ravel_query::derive_suffix_len;

    // The reference ClickBench tenant's 3.47 MB mean object probes the FLOOR,
    // 128 KiB rather than the old flat 256 KiB: half the plan-phase probe bytes.
    assert_eq!(d(3_500_000), 128 * 1024, "3.47 MB mean -> floor");

    // Floor break-even: at 4 MiB the raw fraction equals the floor exactly.
    assert_eq!(d(4 * 1024 * 1024), 128 * 1024, "4 MiB -> floor exactly");

    // Between the two break-evens the size-proportional value is used verbatim.
    assert_eq!(d(6 * 1024 * 1024), 192 * 1024, "6 MiB -> 6 MiB / 32");

    // Ceiling break-even: at 8 MiB the fraction equals the ceiling exactly.
    assert_eq!(d(8 * 1024 * 1024), 256 * 1024, "8 MiB -> ceiling exactly");

    // Above the ceiling break-even the probe is capped at the ceiling: a large
    // object never probes more than the widest measured object needs.
    assert_eq!(d(64 * 1024 * 1024), 256 * 1024, "64 MiB -> ceiling");

    // Just above the whole-object threshold: still the floor, never a sliver.
    assert_eq!(d(600 * 1024), 128 * 1024, "600 KiB -> floor");
}

/// An object whose trailer FITS inside the derived probe is read in exactly ONE
/// request: the size-derived suffix (the 128 KiB floor for this ~1 MB fixture)
/// covers footer + SKIP_IDX, so a plan-phase `fetch_skip_index` issues the probe
/// GET and nothing else, and reports zero probe misses. The decoded index is
/// byte-identical to the oracle.
///
/// Prove-the-test: this pins the exact GET count (1) and `probe_misses == 0`,
/// which a too-small derivation breaks. Demonstrated failing during development
/// by setting `LOG_SUFFIX_FLOOR_BYTES` to 4 KiB: the derived probe then misses
/// SKIP_IDX, the count becomes 2 and `probe_misses` becomes 1.
#[tokio::test]
async fn derived_probe_covering_the_trailer_reads_in_one_request() {
    let (records, bytes) = small_tail_object();
    let total = bytes.len() as u64;
    let (_blocks_offset, _blocks_len, tail) = layout(&bytes);

    // Fixture precondition: above the whole-object threshold, and its tail sits
    // inside the derived floor so the probe covers SKIP_IDX in one GET. A
    // fixture-generation change that violated either would fail here, not
    // silently pass a weaker property.
    assert!(
        total > ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        "fixture must be above the whole-object threshold, got {total}"
    );
    let derived = ravel_query::derive_suffix_len(total);
    assert!(
        tail < derived,
        "the trailer ({tail} B) must fit inside the derived probe ({derived} B)"
    );

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/fit.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let seg = seg_ref("logs/fit.rlog", total, &records);

    // Default suffix: no `with_suffix_len`, so the probe is derived from size.
    let (skip, stats) = BlockRangeFetcher::new(store)
        .fetch_skip_index(&seg, TENANT, &QueryAccounting::new())
        .await
        .expect("fetch skip index");

    assert_eq!(stats.probe_gets, 1, "one etag-establishing suffix probe");
    assert_eq!(stats.metadata_gets, 0, "no follow-up section GET");
    assert_eq!(stats.probe_misses, 0, "the derived probe covered SKIP_IDX");
    assert_eq!(
        counting.get_count(),
        1,
        "a trailer that fits the derived probe is read in exactly one request"
    );
    assert_eq!(
        skip_shape(&skip),
        skip_shape(&oracle_skip(&bytes)),
        "the decoded skip index is byte-identical to the oracle"
    );
}

/// An object whose trailer EXCEEDS the probe is still read correctly, and the
/// miss costs exactly one extra request that the per-object accounting counts.
/// The probe is pinned just past SKIP_IDX's end (a deliberately-too-small
/// window, standing in for an under-sized derivation) so it covers the footer
/// but not SKIP_IDX: the read then pays the probe plus one follow-up section
/// GET, `probe_misses` is 1, and the decoded index still matches the oracle.
///
/// Prove-the-test: this pins the exact GET count (2) and `probe_misses == 1`. A
/// miss that silently cost an extra round trip on every object would be a
/// regression wearing a byte win; the count assertion is what catches it.
/// Demonstrated failing during development by removing `stats.probe_misses +=
/// 1` from `ensure_tail_plan_sections` (the `probe_misses == 1` assertion then
/// reads 0), and by widening the pinned suffix to cover SKIP_IDX (the count
/// drops to 1).
#[tokio::test]
async fn a_trailer_larger_than_the_probe_costs_one_extra_counted_request() {
    let (records, bytes) = large_object();
    let total = bytes.len() as u64;
    let footer = footer::open(&bytes).expect("footer");
    let skip_desc = footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let skip_end = skip_desc.offset + skip_desc.len;

    // A probe that starts exactly at SKIP_IDX's end: it covers everything after
    // SKIP_IDX (BLOOM/POSTINGS and the footer, so no footer chase) but not
    // SKIP_IDX itself, which is the section the plan phase must decode.
    let suffix = total - skip_end;
    assert!(
        suffix < total - skip_desc.offset,
        "the pinned probe must not reach SKIP_IDX's start"
    );

    let mem = Arc::new(MemoryStore::new());
    mem.put(
        "logs/exceed.rlog",
        bytes.clone().into(),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let seg = seg_ref("logs/exceed.rlog", total, &records);

    let (skip, stats) = BlockRangeFetcher::new(store)
        .with_suffix_len(suffix)
        .fetch_skip_index(&seg, TENANT, &QueryAccounting::new())
        .await
        .expect("fetch skip index");

    assert_eq!(stats.probe_gets, 1, "one etag-establishing suffix probe");
    assert_eq!(
        stats.metadata_gets, 1,
        "one follow-up GET for the missed SKIP_IDX"
    );
    assert_eq!(stats.probe_misses, 1, "the probe missed SKIP_IDX");
    assert_eq!(
        counting.get_count(),
        2,
        "a trailer larger than the probe costs exactly one extra request"
    );
    assert_eq!(
        skip_shape(&skip),
        skip_shape(&oracle_skip(&bytes)),
        "the index is decoded correctly despite the probe miss"
    );
}

// ---- One miss, one count: the three-way carried-footer state (#883) --------
