//! Tests for issue #693: the un-cached logs scan assigns whole segments to
//! partitions instead of striping every segment's blocks across all
//! partitions, so each segment is opened by exactly one partition.
//!
//! ADR-0102 stripes a snapshot's surviving blocks round-robin across
//! `target_partitions`, which puts every segment's blocks into every partition
//! and makes each partition open the segment itself. With ADR-0046's read cache
//! wired, those re-opens single-flight onto one object-store request per extent.
//! Without a cache nothing coalesces them, so the scan pays one whole-object GET
//! per partition per segment. The fix (deliverable of #693) makes the un-cached
//! path segment-granular: relevant segment `j` (snapshot order) goes to
//! partition `j % n` and drains all of that segment's surviving blocks, so the
//! scan issues exactly one plan read plus one scan read per segment. The cached
//! path keeps the intra-segment block striping unchanged.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{CacheFetchError, LogSegmentFetcher};
use ravel_sql::LogsTableProvider;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

use util::CountingStore;

/// Number of segments (objects) in the fixture.
const SEGMENTS: usize = 6;
/// Records per object. With `block_target_records: 3` this is 4 blocks, each
/// surviving the full-window scan, so every segment has exactly 4 surviving
/// blocks (>= 4, the count the task pins).
const RECORDS_PER_SEG: usize = 12;
/// Surviving blocks per segment (`RECORDS_PER_SEG / block_target_records`).
const BLOCKS_PER_SEG: usize = 4;
/// Partitions requested for every plan in these tests.
const PARTS: usize = 4;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 3 records so each 12-record object holds 4 blocks.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A record on the single-`service.name` stream `svc`, with a unique `(ts, body)`.
fn record(ts: i64, body: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
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

/// Write one RLOG object from `records`, put it at `key`, and return a matching
/// L0 [`SegmentRef`] carrying the object's true ts span. `content_hash` must be
/// unique per object: the read cache keys on it
/// (`CacheKey::new(tenant, content_hash, ..)`), so a shared value would collide
/// distinct segments in the cached test.
async fn write_object(
    store: &dyn ObjectStoreBackend,
    key: &str,
    content_hash: [u8; 32],
    records: &[LogRecord],
) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

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
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The records for segment `s`: `RECORDS_PER_SEG` rows with globally unique
/// `(ts, body)` so the row-set comparison is a genuine set equality.
fn seg_records(s: usize) -> Vec<LogRecord> {
    (0..RECORDS_PER_SEG)
        .map(|i| {
            let ts = (s * 1000 + i) as i64;
            record(ts, &format!("s{s}-r{i}"))
        })
        .collect()
}

/// Build the six-object fixture on `store` and return the assembled snapshot
/// plus the full `(ts, body)` row set that was written.
async fn build_fixture(store: &dyn ObjectStoreBackend) -> (Snapshot, BTreeSet<(i64, String)>) {
    let mut segments = Vec::with_capacity(SEGMENTS);
    let mut want = BTreeSet::new();
    for s in 0..SEGMENTS {
        let recs = seg_records(s);
        for r in &recs {
            want.insert((r.ts_ns, r.body.clone()));
        }
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        let seg = write_object(store, &format!("logs/seg{s}.rlog"), content_hash, &recs).await;
        segments.push(seg);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (snapshot, want)
}

fn provider(snapshot: Snapshot, fetcher: LogSegmentFetcher) -> LogsTableProvider {
    LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        QueryAccounting::new(),
    )
}

async fn collect_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

/// The `(ts, body)` pairs in `batches` (public logs schema: ts col 0, body col 4).
fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let body = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body col");
        for i in 0..batch.num_rows() {
            out.insert((ts.value(i), body.value(i).to_string()));
        }
    }
    out
}

/// Walk to the `LogsScanExec` leaf.
fn find_by_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_by_name(c, name))
}

/// Per-partition `blocks_scanned` from the executed `LogsScanExec`, partition
/// index ascending. This is the observable that separates the two assignment
/// modes: striping splits a 4-block segment across partitions, so a partition's
/// count need not be a whole-segment multiple; segment-granular gives each
/// partition only whole segments, so every count is a multiple of
/// [`BLOCKS_PER_SEG`].
fn per_partition_blocks_scanned(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let set = find_by_name(plan, "LogsScanExec")
        .expect("a LogsScanExec leaf")
        .metrics()
        .expect("the scan publishes metrics");
    let mut by_partition: BTreeMap<usize, usize> = BTreeMap::new();
    for m in set.iter() {
        if m.value().name() == "blocks_scanned" {
            let p = m
                .partition()
                .expect("blocks_scanned is a per-partition metric");
            *by_partition.entry(p).or_default() += m.value().as_usize();
        }
    }
    by_partition.into_values().collect()
}

/// Build ADR-0046's RAM read cache the way `ravel-bench`'s `build_read_cache`
/// (crates/ravel-bench/src/sql_latency.rs) does: the whole budget as the
/// per-entry cap so no small fixture object is rejected, one entry per 4 KiB of
/// budget floored at 64.
fn build_read_cache(cache_bytes: u64) -> Arc<Cache<CacheFetchError>> {
    let max_entries = (cache_bytes / 4096).max(64) as usize;
    Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        max_entries,
        cache_bytes,
    )))
}

/// The pinned request-count property (#693, #680). Without a read cache the scan
/// must open each of the six segments exactly once: one plan read per segment
/// plus one scan read per segment, `2 * 6 = 12` whole-object GETs total.
///
/// Against the pre-fix ADR-0102 code this fails: block striping puts every
/// segment's four blocks into all four partitions, each partition opens the
/// segment itself, and nothing coalesces the re-opens, so the scan costs
/// `6 + 6 * 4 = 30` GETs (one plan read per segment plus one scan read per
/// partition per segment).
#[tokio::test]
async fn uncached_scan_opens_each_segment_once() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let (snapshot, _want) = build_fixture(store.as_ref()).await;

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    assert!(
        !fetcher.has_cache(),
        "this fixture must exercise the un-cached path"
    );
    let provider = provider(snapshot, fetcher);

    let plan = provider.plan(PARTS).expect("plan");
    let _ = collect_plan(plan).await;

    assert_eq!(
        counting.gets(),
        (2 * SEGMENTS) as u64,
        "un-cached scan must open each segment once: {SEGMENTS} plan reads + \
         {SEGMENTS} scan reads. The pre-fix striped path issues \
         {} (= {SEGMENTS} + {SEGMENTS} * {PARTS}).",
        SEGMENTS + SEGMENTS * PARTS,
    );
}

/// Segment-granular assignment loses and duplicates nothing: the rows returned
/// are exactly the rows written.
#[tokio::test]
async fn uncached_segment_granular_returns_every_row_once() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let (snapshot, want) = build_fixture(store.as_ref()).await;

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let provider = provider(snapshot, fetcher);

    let plan = provider.plan(PARTS).expect("plan");
    let batches = collect_plan(Arc::clone(&plan)).await;
    let got = batches_to_rows(&batches);

    assert_eq!(
        got.len(),
        SEGMENTS * RECORDS_PER_SEG,
        "no row may be dropped or duplicated"
    );
    assert_eq!(
        got, want,
        "segment-granular scan output must equal the written rows"
    );

    // Every partition drains only whole segments, so each partition's
    // blocks_scanned is a multiple of BLOCKS_PER_SEG (the striped path breaks
    // this; see the cached test). Read off the executed `plan`.
    let per = per_partition_blocks_scanned(&plan);
    assert!(
        per.iter().all(|&b| b % BLOCKS_PER_SEG == 0),
        "segment-granular: every partition scans whole segments (multiples of \
         {BLOCKS_PER_SEG}), got {per:?}"
    );
    assert_eq!(
        per.iter().sum::<usize>(),
        SEGMENTS * BLOCKS_PER_SEG,
        "every surviving block is scanned exactly once"
    );
}

/// With a read cache attached the intra-segment block striping is unchanged: a
/// single segment's blocks are spread across partitions. This is asserted
/// through the assignment (per-partition `blocks_scanned`), not the store: at
/// least one partition scans a block count that is not a whole-segment multiple,
/// which is only possible when a segment's four blocks land in more than one
/// partition. The cached GET count stays within the segment-granular bound
/// (`<= 2 * 6`) because single-flight coalesces the re-opens.
#[tokio::test]
async fn cached_path_still_stripes_blocks_across_partitions() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let (snapshot, want) = build_fixture(store.as_ref()).await;

    let cache = build_read_cache(64 << 20);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store)).with_cache(cache);
    assert!(
        fetcher.has_cache(),
        "this fixture must exercise the cached path"
    );
    let provider = provider(snapshot, fetcher);

    let plan = provider.plan(PARTS).expect("plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    // Correctness is unchanged either way.
    assert_eq!(
        batches_to_rows(&batches),
        want,
        "cached scan output must equal the written rows"
    );

    let per = per_partition_blocks_scanned(&plan);
    assert!(
        per.iter().any(|&b| b % BLOCKS_PER_SEG != 0),
        "striped path must split a segment's {BLOCKS_PER_SEG} blocks across \
         partitions, so some partition's blocks_scanned is not a multiple of \
         {BLOCKS_PER_SEG}; got {per:?} (segment-granular would give only \
         multiples of {BLOCKS_PER_SEG})"
    );
    assert_eq!(
        per.iter().sum::<usize>(),
        SEGMENTS * BLOCKS_PER_SEG,
        "every surviving block is still scanned exactly once"
    );

    assert!(
        counting.gets() <= (2 * SEGMENTS) as u64,
        "single-flight must coalesce the re-opens to within the segment-granular \
         bound of {} GETs; got {}",
        2 * SEGMENTS,
        counting.gets(),
    );
}
