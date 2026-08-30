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
use ravel_cache::{Cache, CacheLimits, DiskCache, TieredCache};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::footer::{self, kind};
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, RlogConfig, RlogWriter, read_section, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{
    AssemblyBufferStats, BlockRangeFetcher, CarriedFooter, LogFetchError, LogQuery,
    LogSegmentFetcher, ReadPhases,
};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
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

/// A record carrying five distinct string attributes, so its block gets five
/// dynamic columns a narrow projection can skip (on top of the always-present
/// fixed columns). Used by the decode-accounting test to create real
/// column-filtering waste.
fn record_with_attrs(name: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str(name.to_string()))];
    let attrs = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|k| (k.to_string(), AttrValue::Str(format!("{k}{ts}"))))
        .collect();
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
        attrs,
    }
}

/// Build one RLOG version-3 object's bytes from `records` (one block each).
///
/// The block-range protocol under test is the version-3 one: it addresses each
/// block by its SKIP_IDX byte range and verifies `block_crc32c` over exactly
/// those bytes. RLOG version 4 (ADR-0699) makes neither true -- a block's pages
/// are spread column-major across its row group -- so a version-4 object takes
/// decision 5's column-chunk path instead, which `tests/log_page_dir_fetch.rs`
/// covers. These fixtures therefore stay at version 3, which is what keeps this
/// protocol covered.
fn build_object(records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(one_record_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish_v3_for_tests().expect("finish v3")
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

/// The block-range GET count and byte volume are proportional to the candidate
/// fraction, not the object size. With `coalesce_gap == 0`, the block-range GET
/// count equals the number of maximal contiguous runs among candidate blocks; a
/// suffix sized to the tail makes the fixed overhead exactly one probe GET plus
/// one coalesced GET for the two adjacent front sections (STREAM_DIR, FIELD_DIR,
/// deliverable 4). The asserted figures are exact, not "fewer than before".
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

    // Total store GETs: probe (1) + front metadata (STREAM_DIR + FIELD_DIR are
    // adjacent at the object front and fetched together in ONE coalesced GET,
    // deliverable 4) + one per contiguous candidate run. The tail
    // (SKIP_IDX/BLOOM/POSTINGS + footer) is covered by the probe, so it costs no
    // extra GET.
    let expected_total = 1 + 1 + expected_runs as u64;
    assert_eq!(
        counting.get_count(),
        expected_total,
        "probe(1) + coalesced front-meta(1) + block runs({expected_runs})"
    );
    assert_eq!(stats.probe_gets, 1);
    assert_eq!(
        stats.metadata_gets, 1,
        "STREAM_DIR + FIELD_DIR fetched in one coalesced front-section GET"
    );
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

/// An object inside the ONLY interval where a byte-only 512 KiB threshold and a
/// 700 KiB request-cost break-even disagree: 27 one-record blocks of ~24 KiB
/// each, 612181 bytes. That is 87893 B above
/// [`ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`], which would range-fetch
/// it, and 104619 B below the 700 KiB break-even
/// `whole_object_vs_ranged_is_driven_by_the_request_cost` configures, which
/// reads it whole. `big_records` is deterministic, so the size is the same on
/// every run; that test asserts both bounds so a fixture-generation change
/// cannot drift it out of the window unnoticed.
fn medium_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records = big_records(27);
    let bytes = build_object(&records);
    (records, bytes)
}

/// The fixed non-BLOCKS section GET cost the probe window `[total - suffix,
/// total)` does NOT already cover: the two adjacent FRONT sections (STREAM_DIR,
/// FIELD_DIR) as ONE coalesced GET when either is uncovered (deliverable 4;
/// always uncovered here, since a suffix probe never reaches the front), plus
/// one GET per uncovered TAIL section.
fn uncovered_sections(bytes: &[u8], suffix: u64) -> u64 {
    let footer = footer::open(bytes).expect("footer");
    let total = bytes.len() as u64;
    let probe_start = total.saturating_sub(suffix);
    let is_front = |k: u32| k == kind::STREAM_DIR || k == kind::FIELD_DIR;
    let mut front_get = 0u64;
    let mut tail_gets = 0u64;
    for s in &footer.sections {
        if s.kind == kind::BLOCKS {
            continue;
        }
        let uncovered = s.offset < probe_start || s.offset + s.len > total;
        if !uncovered {
            continue;
        }
        if is_front(s.kind) {
            front_get = 1;
        } else {
            tail_gets += 1;
        }
    }
    front_get + tail_gets
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
    // The probe window is the size-derived suffix (#883), not a flat 256 KiB:
    // this fixture's tail exceeds the derived probe, so the uncovered tail
    // sections it leaves are part of the expected per-segment GET count.
    let suffix = ravel_query::derive_suffix_len(total);
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
        // 50ms, not 5ms (see #618): under real host contention, tokio task
        // scheduling delays alone can exceed a few ms, reopening the race
        // this sleep exists to close (see the type doc above). 50ms is still
        // well under S3's real 15-80ms and gives ample headroom.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
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

/// A `TieredCache` (RAM over a temp-dir `DiskCache`) sized like [`big_cache`] so
/// nothing is evicted mid-test, constructed exactly as #97's server wiring will
/// (and as `cache_correctness.rs`'s disk-tier tests already do). Both tiers are
/// generous: the point of the tiered test below is the single-flight collapse,
/// not eviction.
fn big_tiered_cache(dir: &std::path::Path) -> Arc<TieredCache<ravel_query::CacheFetchError>> {
    let limits = CacheLimits::new(1 << 26, 1 << 23, 1 << 26);
    let ram = Cache::new(limits);
    let disk = DiskCache::new(dir.to_path_buf(), limits);
    Arc::new(TieredCache::new(ram, disk))
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

        // Production defaults for probe length and coverage; the cache is wired
        // exactly as `build_sql_state` wires it. The request cost is lowered to
        // 64 KiB so this deliberately sub-MB fixture stays ABOVE the
        // request-aware whole-object threshold (5 x request-cost, floored at 512
        // KiB) and exercises the ranged protocol this test is about. The
        // single-flight/coalescing property under test is independent of the
        // request cost, and at 64 KiB the effective coalescing gap is the
        // 64 KiB `DEFAULT_LOG_COALESCE_GAP` the oracle uses.
        let fetcher = LogSegmentFetcher::new(store)
            .with_cache(big_cache())
            .with_request_cost_bytes(ravel_query::DEFAULT_LOG_COALESCE_GAP);
        assert!(
            fetcher.has_cache(),
            "the ADR-0102 fan-out gate this test stands in for reads has_cache()"
        );

        // The shared planning read, then the per-partition stripes.
        let accounting = QueryAccounting::new();
        let (surviving, _stats, _footer) = fetcher
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
                        None,
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

    // Request cost lowered to 64 KiB so this sub-MB fixture stays above the
    // request-aware whole-object threshold and takes the ranged path; see test 6.
    let br = BlockRangeFetcher::new(store)
        .with_cache(big_cache())
        .with_request_cost_bytes(ravel_query::DEFAULT_LOG_COALESCE_GAP);
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

/// Test 7b (issue #662): the reachability proof for the tiered tier. The same
/// property test 7 pins on a RAM-only cache -- N cold concurrent partitions
/// striping one segment collapse onto exactly one GET per distinct extent --
/// must hold when the fetcher's cache is a `ReadCache::Tiered` (RAM over a real
/// disk `DiskCache`) instead. This is the disk-backed tier ADR-0046's
/// single-flight (decision 5) has to span, and the funnel reaches it through
/// `BlockRangeFetcher::fetch_run`'s `ReadCache::fetch_peeked` -- now
/// `TieredCache::resolve_peeked_miss` -- exactly as the RAM branch reaches
/// `Cache::get_or_fetch`.
///
/// The count asserted is identical to test 7's: one GET per distinct extent for
/// any number of concurrent partitions. `SlowCountingStore`'s 50ms padding (per
/// #618's flake history) keeps the count a function of the fetch protocol rather
/// than of a microsecond race, exactly as in test 7.
///
/// Prove-the-test: this is the tiered-tier equivalent of test 7's proof. Revert
/// `TieredCache::resolve_peeked_miss` to run `fetch().await` directly with no
/// single-flight (the pre-#662 `ReadCache::fetch_peeked` Tiered arm), and the
/// count becomes `N * expected` rather than `expected`, so this assertion fires.
/// (Demonstrated failing by that revert during development.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cold_partitions_collapse_onto_one_get_per_extent_tiered() {
    let (records, bytes) = large_object();
    let (ts_min, ts_max) = (2, 11);
    let expected = expected_segment_gets(&bytes, ts_min, ts_max);
    const PARTITIONS: usize = 8;

    let mem = Arc::new(MemoryStore::new());
    mem.put(
        "logs/cold-tiered.rlog",
        bytes.clone().into(),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let counting = Arc::new(SlowCountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let seg = seg_ref("logs/cold-tiered.rlog", bytes.len() as u64, &records);

    let tmp = tempfile::TempDir::new().expect("temp dir for the disk cache tier");
    // Request cost lowered to 64 KiB so this sub-MB fixture stays above the
    // request-aware whole-object threshold and takes the ranged path; see test 6.
    let br = BlockRangeFetcher::new(store)
        .with_cache(big_tiered_cache(tmp.path()))
        .with_request_cost_bytes(ravel_query::DEFAULT_LOG_COALESCE_GAP);
    let mut tasks = Vec::new();
    for _ in 0..PARTITIONS {
        let br = br.clone();
        let seg = seg.clone();
        tasks.push(tokio::spawn(async move {
            br.fetch_object(&seg, TENANT, ts_min, ts_max, &QueryAccounting::new())
                .await
                .expect("concurrent tiered block-range fetch")
        }));
    }
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
    }

    assert_eq!(
        counting.get_count(),
        expected,
        "{PARTITIONS} concurrent cold fetches of one segment through a TIERED cache must issue \
         exactly one GET per distinct extent ({expected}: probe + uncovered sections + coalesced \
         runs); un-coalesced the same work is {}",
        expected * PARTITIONS as u64
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
/// This calls `BlockRangeFetcher::fetch_object` directly rather than going
/// through `LogSegmentFetcher::fetch_accounted_with_tenant`: the latter decodes
/// the assembled buffer afterward, and the reader's own `read_block_columns`
/// crc check (`ravel-logseg`) fires first and masks the fetcher's own cache-hit
/// gate this test names -- confirmed by mutating the fetcher's gate alone and
/// observing the suite stay green through the decode path. Calling
/// `fetch_object` directly returns the assembled bytes without decoding them,
/// so only `BlockRangeFetcher::fetch_blocks`'s own `verify_block_crc` can catch
/// the corruption.
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

    // Request cost lowered to 64 KiB so this sub-MB fixture stays above the
    // request-aware whole-object threshold and takes the per-block ranged path
    // whose cache-hit gate is under test (see test 6); a whole-object read would
    // never consult the pre-admitted per-block entries.
    let br = BlockRangeFetcher::new(store)
        .with_cache(cache)
        .with_request_cost_bytes(ravel_query::DEFAULT_LOG_COALESCE_GAP);
    let err = br
        .fetch_object(&seg, TENANT, ts_min, ts_max, &QueryAccounting::new())
        .await
        .expect_err("a corrupted per-block cache hit must fail closed");
    assert!(
        matches!(err, LogFetchError::Corrupt { .. }),
        "expected Corrupt from the per-block gate, got {err:?}"
    );
}

// ---- Decode-time page accounting (ADR-0107 decision 4) --------------------

/// `page_bytes_fetched`/`page_bytes_decoded` (ADR-0107 decision 4) populate on
/// the same `QueryAccounting` handle T1 records wire bytes against, as a
/// SEPARATE, ADDITIVE axis -- not a repurposing of the wire-byte counters.
///
/// The fixture forces the T1 block-range path (`with_block_range_threshold(0)`),
/// so both axes are exercised in one scan. Two scans over the same segment, one
/// all-columns and one projecting a single attribute, prove the split:
///
/// - Wire bytes (`total_s3_bytes`, `AccountedOp::Get`) are IDENTICAL across the
///   two projections and non-zero: column filtering is decode-time only, so the
///   block-range path fetches the same bytes regardless of the projection. This
///   is the regression guard that the new pair did not repurpose an existing
///   wire counter.
/// - `page_bytes_fetched` is filter-independent (every present page counts) and
///   equal across the two scans; `page_bytes_decoded` is strictly smaller under
///   the narrow projection -- the column-filtering waste this axis measures.
/// - The `QueryAccounting` totals equal the scan's own `ScanStats` totals,
///   proving `LogSegmentScan::finish` folds them through exactly.
#[tokio::test]
async fn page_decode_accounting_is_a_separate_axis_from_wire_bytes() {
    let records: Vec<LogRecord> = (0..12)
        .map(|ts| record_with_attrs("api", ts, "body"))
        .collect();
    let bytes = build_object(&records);
    let (_blocks_offset, _blocks_len, tail_len) = layout(&bytes);
    let (ts_min, ts_max) = (3, 9);

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/pa.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/pa.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let query = LogQuery::new(ts_min, ts_max);

    // Fresh fetcher per scan (no cache), forced onto the T1 block-range path so
    // the wire-byte counters are genuinely exercised alongside the new pair.
    let run = |columns: ravel_logseg::ColumnSelection| {
        let store = Arc::clone(&store);
        let seg = seg.clone();
        let query = query.clone();
        async move {
            let br = BlockRangeFetcher::new(Arc::clone(&store))
                .with_whole_object_threshold(0)
                .with_suffix_len(tail_len)
                .with_coverage_threshold(2.0);
            let fetcher = LogSegmentFetcher::new(store)
                .with_block_range_threshold(0)
                .with_block_range(br);
            let acc = QueryAccounting::new();
            let mut scan = fetcher
                .scan_accounted_with_tenant(&seg, TENANT, &query, &columns, &acc)
                .await
                .expect("scan")
                .expect("in range");
            let mut rows = 0usize;
            while let Some(block) = scan.next_block().expect("decode") {
                rows += block.len();
            }
            let stats = scan.stats();
            (acc.snapshot(), stats, rows)
        }
    };

    let (all_snap, all_stats, all_rows) = run(ravel_logseg::ColumnSelection::all()).await;
    let (proj_snap, proj_stats, proj_rows) =
        run(ravel_logseg::ColumnSelection::fixed_only().with_attr("a")).await;

    // Both projections match the same rows (projection changes decode, not which
    // rows survive the exact filter).
    assert_eq!(
        all_rows, proj_rows,
        "projection must not change matched rows"
    );
    assert_eq!(
        all_rows,
        (ts_max - ts_min + 1) as usize,
        "ts range [3,9] selects seven one-record blocks"
    );

    // finish() folds the scan's ScanStats page-byte totals into the handle
    // exactly (deliverable 3 wiring).
    assert_eq!(all_snap.page_bytes_fetched, all_stats.page_bytes_fetched);
    assert_eq!(all_snap.page_bytes_decoded, all_stats.page_bytes_decoded);
    assert_eq!(proj_snap.page_bytes_fetched, proj_stats.page_bytes_fetched);
    assert_eq!(proj_snap.page_bytes_decoded, proj_stats.page_bytes_decoded);

    // An all-columns scan decodes every fetched page byte.
    assert!(all_snap.page_bytes_fetched > 0, "some pages were decoded");
    assert_eq!(
        all_snap.page_bytes_decoded, all_snap.page_bytes_fetched,
        "an all-columns scan decodes every fetched page byte"
    );

    // Fetched is filter-independent: the projection fetched the same pages.
    assert_eq!(
        proj_snap.page_bytes_fetched, all_snap.page_bytes_fetched,
        "page_bytes_fetched counts every present page, projection or not"
    );
    // Decoded is strictly smaller under the narrow projection: real waste.
    assert!(
        proj_snap.page_bytes_decoded < proj_snap.page_bytes_fetched,
        "narrow projection must decode strictly fewer page bytes: {} < {}",
        proj_snap.page_bytes_decoded,
        proj_snap.page_bytes_fetched
    );

    // The regression guard: wire bytes are UNCHANGED by projection and by the
    // new pair. Same query, same pruning, same block-range fetch, so T1's
    // wire-byte and GET counters are byte-identical across the two scans; the
    // page-byte pair is additive, not a repurposing of these counters.
    assert!(
        all_snap.total_s3_bytes() > 0,
        "T1 block-range fetch must record wire bytes"
    );
    assert_eq!(
        all_snap.total_s3_bytes(),
        proj_snap.total_s3_bytes(),
        "wire bytes are unchanged by projection; page-byte pair is a separate axis"
    );
    assert!(all_snap.s3_requests(AccountedOp::Get) > 0);
    assert_eq!(
        all_snap.s3_requests(AccountedOp::Get),
        proj_snap.s3_requests(AccountedOp::Get),
        "the T1 GET count is unmoved by the decode-time pair"
    );
}

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

/// The size-threshold crossover is request-cost driven: with the request cost
/// set so the break-even (`WHOLE_OBJECT_REQUEST_MULTIPLE` request-costs, read
/// from the production constant rather than restated here)
/// lands at 700 KiB, the crossover sits at 700 KiB and not at the 512 KiB
/// byte-only floor, WITHOUT any explicit `with_whole_object_threshold` -- the
/// decision consults the request cost alone. Request counts are exact, not
/// "fewer than before".
///
/// Prove-the-test: the byte-only rule (`<= self.whole_object_threshold`, a `u64`
/// defaulting to 512 KiB) and the request-cost rule (`<=
/// self.effective_whole_object_threshold()`, 700 KiB here) disagree on exactly
/// one interval, [512 KiB, 700 KiB): ranged under the former, whole under the
/// latter. So only a fixture inside that interval can fail. The small
/// (`subset_object`, tiny bodies, far under 512 KiB) and large (960 KiB)
/// fixtures sit outside it and are read the same way under both rules -- they
/// anchor the two ends, they do not discriminate. `medium_object` (612181 B,
/// above 512 KiB and below 700 KiB) is inside it: reverting that
/// comparison in `fetch_object_with_footer` to the byte-only field puts it on
/// the probe+ranged path, so `medium_stats.whole_object` and the exact
/// one-GET count both fail. Its ts window is narrow enough to stay under the
/// coverage crossover, so nothing routes it back to a whole-object read once the
/// size rule sends it ranged, and both of its size bounds are asserted below: a
/// fixture-generation change that drifted it out of the interval would restore
/// the vacuity with no assertion firing.
#[tokio::test]
async fn whole_object_vs_ranged_is_driven_by_the_request_cost() {
    // 5 * 140 KiB = 700 KiB break-even, above the 512 KiB floor so the
    // request-derived value (not the floor) is what decides.
    const REQUEST_COST: u64 = 140 * 1024;
    // Derived from the production constant, never a literal: if the policy's
    // multiple changes, the fixture bounds below must move with it or this test
    // silently stops covering the interval where the two rules disagree.
    let break_even = ravel_query::WHOLE_OBJECT_REQUEST_MULTIPLE * REQUEST_COST;

    // Small side: the 20-block subset object, far under the break-even.
    let (small_recs, small_bytes) = subset_object();
    let small_total = small_bytes.len() as u64;
    assert!(
        small_total < break_even,
        "small fixture {small_total} must sit below the {break_even} B break-even"
    );
    let small_mem = Arc::new(MemoryStore::new());
    small_mem
        .put(
            "logs/small.rlog",
            small_bytes.clone().into(),
            PutOptions::default(),
        )
        .await
        .expect("put small");
    let small_seg = seg_ref("logs/small.rlog", small_total, &small_recs);
    let small_counting = Arc::new(CountingStore::new(small_mem));
    let small_store: Arc<dyn ObjectStoreBackend> = small_counting.clone();
    let (_small_buf, small_stats) = BlockRangeFetcher::new(small_store)
        .with_request_cost_bytes(REQUEST_COST)
        .fetch_object(
            &small_seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &QueryAccounting::new(),
        )
        .await
        .expect("small fetch");
    assert!(
        small_stats.whole_object,
        "an object below the break-even is read whole"
    );
    assert_eq!(
        small_counting.get_count(),
        1,
        "the whole-object read is exactly one GET"
    );
    assert_eq!(small_stats.block_range_gets, 0, "no ranged block GETs");

    // Large side: the 40-block, ~960 KiB object, above the break-even.
    let (large_recs, large_bytes) = large_object();
    let large_total = large_bytes.len() as u64;
    assert!(
        large_total > break_even,
        "large fixture {large_total} must sit above the {break_even} B break-even"
    );
    let (_blocks_offset, _blocks_len, tail) = layout(&large_bytes);
    let (ts_min, ts_max) = (2, 11);
    let cands = candidate_extents(&large_bytes, ts_min, ts_max);
    let effective_gap = REQUEST_COST.max(ravel_query::DEFAULT_LOG_COALESCE_GAP);
    let expected_runs = coalesced_runs(cands.clone(), effective_gap);
    assert!(
        !cands.is_empty() && (cands.len() as u64) < 40,
        "a strict nontrivial candidate subset, got {}",
        cands.len()
    );

    let large_mem = Arc::new(MemoryStore::new());
    large_mem
        .put(
            "logs/large.rlog",
            large_bytes.clone().into(),
            PutOptions::default(),
        )
        .await
        .expect("put large");
    let large_seg = seg_ref("logs/large.rlog", large_total, &large_recs);
    let large_counting = Arc::new(CountingStore::new(large_mem));
    let large_store: Arc<dyn ObjectStoreBackend> = large_counting.clone();
    let (_large_buf, large_stats) = BlockRangeFetcher::new(large_store)
        .with_request_cost_bytes(REQUEST_COST)
        // Probe the tail exactly, so the front sections are the only uncovered
        // metadata and the candidate GETs are clean.
        .with_suffix_len(tail)
        .fetch_object(&large_seg, TENANT, ts_min, ts_max, &QueryAccounting::new())
        .await
        .expect("large fetch");
    assert!(
        !large_stats.whole_object,
        "an object above the break-even takes the ranged path"
    );
    assert_eq!(
        large_stats.probe_gets, 1,
        "one etag-establishing suffix probe"
    );
    assert_eq!(
        large_stats.metadata_gets, 1,
        "STREAM_DIR + FIELD_DIR in one coalesced front-section GET"
    );
    assert_eq!(
        large_stats.block_range_gets, expected_runs,
        "one coalesced GET per candidate run at the request-cost-derived gap"
    );
    assert_eq!(
        large_counting.get_count(),
        1 + 1 + expected_runs,
        "probe(1) + coalesced front-meta(1) + block runs({expected_runs})"
    );

    // Medium side (the discriminator): the ONLY fixture inside the interval where
    // the two rules disagree, [DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, break_even) =
    // [512 KiB, 700 KiB). Under the request-cost rule its size is below the
    // break-even, so it is read whole in one GET; under the byte-only 512 KiB
    // rule it is above the floor and would take the ranged path. Both bounds are
    // asserted (mirroring `small_total < break_even`) so a fixture-generation
    // change that drifted its encoded size out of the window would fail here
    // rather than silently restoring the vacuity this test exists to remove. The
    // ts window is narrow ([2, 11] of 27 blocks, coverage under the crossover) so
    // that reverting the size rule to byte-only genuinely sends it ranged and the
    // coverage backstop does not route it back to a whole-object read.
    let (medium_recs, medium_bytes) = medium_object();
    let medium_total = medium_bytes.len() as u64;
    assert!(
        medium_total > ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        "medium fixture {medium_total} must sit above the {} B byte-only threshold",
        ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
    );
    assert!(
        medium_total < break_even,
        "medium fixture {medium_total} must sit below the {break_even} B break-even"
    );
    let medium_mem = Arc::new(MemoryStore::new());
    medium_mem
        .put(
            "logs/medium.rlog",
            medium_bytes.clone().into(),
            PutOptions::default(),
        )
        .await
        .expect("put medium");
    let medium_seg = seg_ref("logs/medium.rlog", medium_total, &medium_recs);
    let medium_counting = Arc::new(CountingStore::new(medium_mem));
    let medium_store: Arc<dyn ObjectStoreBackend> = medium_counting.clone();
    let (_medium_buf, medium_stats) = BlockRangeFetcher::new(medium_store)
        .with_request_cost_bytes(REQUEST_COST)
        .fetch_object(&medium_seg, TENANT, 2, 11, &QueryAccounting::new())
        .await
        .expect("medium fetch");
    assert!(
        medium_stats.whole_object,
        "an object inside [byte-only threshold, break-even) is read whole under \
         the request-cost rule"
    );
    assert_eq!(
        medium_counting.get_count(),
        1,
        "the whole-object read is exactly one GET"
    );
    assert_eq!(
        medium_stats.block_range_gets, 0,
        "a whole-object read issues no ranged block GETs"
    );
}

/// The coverage crossover is the backstop that keeps a large coalescing gap from
/// silently turning a genuinely narrow read into a whole-object one: coverage is
/// measured on the real candidate bytes (gap-independent), so a narrow candidate
/// set stays under the crossover and on the ranged path no matter how large the
/// gap coalesces its runs, while a set that genuinely covers most of the object
/// still crosses over. This is what stops the item-3 gap widening from undoing
/// the item-2 whole-object decision.
///
/// Prove-the-test: if coverage were instead measured on the post-coalesce run
/// bytes, the huge gap would inflate the narrow read's run to nearly the whole
/// object and `!narrow_stats.whole_object` would fail. It holds because the
/// crossover's numerator is the candidate extents' own bytes.
#[tokio::test]
async fn a_large_coalesce_gap_does_not_cross_a_narrow_read_over_to_whole_object() {
    const REQUEST_COST: u64 = 140 * 1024;
    let (records, bytes) = large_object();
    let total = bytes.len() as u64;
    let (_blocks_offset, blocks_len, tail) = layout(&bytes);
    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/gap.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/gap.rlog", total, &records);

    // A gap as large as the whole object: every candidate run would fuse.
    let make = |store: Arc<dyn ObjectStoreBackend>| {
        BlockRangeFetcher::new(store)
            .with_request_cost_bytes(REQUEST_COST)
            .with_suffix_len(tail)
            .with_coalesce_gap(total)
    };

    // Narrow: a two-block ts window. Its candidate bytes are a small fraction of
    // BLOCKS, so coverage is far under the 0.75 crossover.
    let (narrow_min, narrow_max) = (2, 3);
    let narrow_cands = candidate_extents(&bytes, narrow_min, narrow_max);
    let narrow_bytes: u64 = narrow_cands.iter().map(|(_, l, _)| *l).sum();
    let narrow_coverage = narrow_bytes as f64 / blocks_len as f64;
    assert!(
        narrow_coverage < ravel_query::DEFAULT_LOG_COVERAGE_THRESHOLD,
        "the narrow set must be genuinely under the coverage crossover, got {narrow_coverage}"
    );
    let narrow_counting = Arc::new(CountingStore::new(mem.clone()));
    let (_buf, narrow_stats) = make(narrow_counting.clone())
        .fetch_object(
            &seg,
            TENANT,
            narrow_min,
            narrow_max,
            &QueryAccounting::new(),
        )
        .await
        .expect("narrow fetch");
    assert!(
        !narrow_stats.whole_object,
        "a genuinely narrow read stays ranged even at a whole-object-sized gap"
    );
    assert_eq!(
        narrow_stats.candidate_blocks,
        narrow_cands.len() as u64,
        "the candidate set is the narrow ts window's blocks"
    );
    assert_eq!(
        narrow_stats.block_range_gets, 1,
        "the huge gap fuses the narrow candidates into exactly one run"
    );

    // Wide control: every block a candidate. Coverage reaches ~1.0, so the SAME
    // configuration crosses over to one whole-object GET -- the crossover tracks
    // genuine coverage, not the gap.
    let wide_counting = Arc::new(CountingStore::new(mem));
    let (_buf, wide_stats) = make(wide_counting)
        .fetch_object(&seg, TENANT, i64::MIN, i64::MAX, &QueryAccounting::new())
        .await
        .expect("wide fetch");
    assert!(
        wide_stats.whole_object,
        "a read whose candidates cover the object still crosses over to whole-object"
    );
}

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
    let bytes = w.finish_v3_for_tests().expect("finish v3");
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

/// A probe pinned to start exactly at SKIP_IDX's end on the version-3 fixture:
/// it covers the footer and every tail section after SKIP_IDX (so there is no
/// footer chase) but not SKIP_IDX itself, the one section a version-3 read has
/// to locate candidate blocks through. Returns the records, the object bytes,
/// and that suffix.
fn probe_missing_skip_idx() -> (Vec<LogRecord>, Vec<u8>, u64) {
    let (records, bytes) = large_object();
    let total = bytes.len() as u64;
    let footer = footer::open(&bytes).expect("footer");
    let skip_desc = footer.section(kind::SKIP_IDX).expect("SKIP_IDX");
    let suffix = total - (skip_desc.offset + skip_desc.len);
    assert!(
        footer.section(kind::PAGE_DIR).is_none(),
        "this fixture must be version 3, so the version-3 miss site is the one \
         under test"
    );
    assert!(
        skip_desc.offset < total - suffix,
        "the pinned probe must not reach SKIP_IDX's start"
    );
    (records, bytes, suffix)
}

async fn ranged_v3(bytes: &[u8], suffix: u64) -> BlockRangeFetcher {
    let mem = Arc::new(MemoryStore::new());
    mem.put(
        "logs/three-way.rlog",
        bytes::Bytes::copy_from_slice(bytes),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let store: Arc<dyn ObjectStoreBackend> = mem;
    BlockRangeFetcher::new(store)
        .with_whole_object_threshold(0)
        .with_suffix_len(suffix)
}

/// The version-3 scan's tail-section miss is counted exactly once per object,
/// by whichever layer issued the probe, across all three states a read can be
/// in.
///
/// 1. No carried footer: this read probed, so it counts its own miss.
/// 2. A footer carried by `fetch_plan_sections`, which already counted the miss
///    into the plan phase's own stats: the scan counts nothing, and the total
///    across the two phases is one, not two.
/// 3. A footer carried by `fetch_footer`, which read the footer alone and
///    counted nothing about SKIP_IDX: the scan counts the miss, because it is
///    the read that pays for it and no other layer has.
///
/// All three counts are exact rather than bounds, so each fails in both
/// directions.
///
/// Prove-the-test, per case, against
/// `crates/ravel-query/src/log_fetcher.rs`'s `if count_tail_misses` in
/// `fetch_object_with_footer`'s version-3 branch:
///
/// - case 1 fails (0 against 1) with the whole `stats.probe_misses += 1` under
///   that gate deleted;
/// - case 2 fails (2 against the expected total of 1) with the gate deleted so
///   the site counts unconditionally, which is the double-count the previous
///   commit fixed;
/// - case 3 fails (0 against 1) with the gate restored to the previous commit's
///   `plan_footer.is_none()`, which is false here even though the carried
///   footer's read counted nothing. That is the under-count this test exists
///   for.
#[tokio::test]
async fn a_version_3_tail_miss_is_counted_once_by_whoever_probed() {
    let (records, bytes, suffix) = probe_missing_skip_idx();
    let total = bytes.len() as u64;
    let seg = seg_ref("logs/three-way.rlog", total, &records);
    let fetcher = ranged_v3(&bytes, suffix).await;
    let acc = QueryAccounting::new();
    let all = ColumnSelection::all();

    // Case 1: this read issues the probe.
    let (_bytes, own) = fetcher
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &all,
            None,
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("unprompted fetch");
    assert_eq!(own.probe_gets, 1, "the probe covered the footer, no chase");
    assert_eq!(
        own.probe_misses, 1,
        "this read probed and SKIP_IDX fell outside its window"
    );

    // Case 2: the plan read counted, so the scan must not.
    let (planned_footer, _skip, _fd, plan_counted) = fetcher
        .fetch_plan_sections(&seg, TENANT, &acc)
        .await
        .expect("plan sections");
    assert_eq!(
        plan_counted.probe_misses, 1,
        "ensure_tail_plan_sections counts the SKIP_IDX the window missed"
    );
    let (_bytes, after_counted) = fetcher
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &all,
            Some(CarriedFooter {
                footer: &planned_footer,
                tail_misses_counted: true,
            }),
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("scan on a counted footer");
    assert_eq!(
        after_counted.probe_misses, 0,
        "the plan phase already counted this object"
    );
    assert_eq!(
        plan_counted.probe_misses + after_counted.probe_misses,
        1,
        "one miss, one count across the plan and the scan"
    );

    // Case 3: the plan read the footer alone and counted nothing.
    let (bare_footer, footer_only) = fetcher
        .fetch_footer(&seg, TENANT, &acc)
        .await
        .expect("footer-only plan read");
    assert_eq!(
        footer_only.probe_misses, 0,
        "fetch_footer reads no tail section, so it counts no tail miss"
    );
    let (_bytes, after_bare) = fetcher
        .fetch_object_with_footer(
            &seg,
            TENANT,
            i64::MIN,
            i64::MAX,
            &[],
            &all,
            Some(CarriedFooter {
                footer: &bare_footer,
                tail_misses_counted: false,
            }),
            ReadPhases::SCAN,
            &acc,
        )
        .await
        .expect("scan on an uncounted footer");
    assert_eq!(
        after_bare.probe_misses, 1,
        "nobody counted this object's SKIP_IDX miss yet, so this read does"
    );
    assert_eq!(
        footer_only.probe_misses + after_bare.probe_misses,
        1,
        "one miss, one count across the plan and the scan"
    );
}
