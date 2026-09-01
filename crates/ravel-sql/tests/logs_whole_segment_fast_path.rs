//! Tests for issue #693 part 3 deliverable 1, as amended by issue #739: a
//! predicate-free, full-window logs statement whose window fully contains every
//! relevant segment (and there are at least `target_partitions` of them) skips
//! the plan phase entirely and reads each segment whole in ONE object-store GET.
//!
//! Post-#707 such a statement paid, per segment, a plan-phase suffix probe
//! (`plan_segment_fast`) plus a scan-side re-probe per partition-open plus a
//! whole-object read (the coverage crossover), and the plan probe never
//! coalesced with the scan reads because the `OnceCell` plan barrier made it the
//! first, cold touch. This deliverable removes both probe classes for the
//! whole-window case: the request count is exactly one whole-object GET per
//! segment and zero suffix probes.
//!
//! Most fixtures here use objects ABOVE the block-range threshold, which is what
//! lets the probe-count assertion mean something: the striped path would probe,
//! the fast path does not.
//!
//! Issue #739 then removed the threshold as a conjunct. It was query-wide, so one
//! small object -- the tail a bulk load leaves per `(shard, hour)` -- vetoed the
//! whole snapshot; a sub-threshold segment is read whole by the whole-segment
//! entry and by the striped path's ranged entry alike, on the same
//! `(0, object_size)` cache key, so it joins the assignment without changing what
//! is read. `mixed_threshold_snapshot_takes_the_fast_path` pins that, and the
//! `rejection_reason_*` tests pin the `fast_path_rejected_*` counter the scan
//! publishes when a query-wide conjunct does fail.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{CacheFetchError, LogSegmentFetcher};
use ravel_sql::LogsTableProvider;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

/// Segments in the fixture, and the partitions requested for the fast-path plan.
/// Equal so `relevant_segments >= target_partitions` holds and each segment is
/// assigned whole to one partition.
const SEGMENTS: usize = 4;
const PARTS: usize = 4;
/// One record per block, three records per object, so each segment has exactly
/// three blocks (2-3, the count the task pins) and every one survives the full
/// window.
const RECORDS_PER_SEG: usize = 3;
const BLOCKS_PER_SEG: usize = 3;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn one_record_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

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

async fn write_object(
    store: &dyn ObjectStoreBackend,
    key: &str,
    content_hash: [u8; 32],
    records: &[LogRecord],
) -> SegmentRef {
    let mut w = RlogWriter::new(one_record_blocks(), identity());
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

fn seg_records(s: usize) -> Vec<LogRecord> {
    (0..RECORDS_PER_SEG)
        .map(|i| {
            let ts = (s * 1000 + i) as i64;
            record(ts, &format!("s{s}-r{i}"))
        })
        .collect()
}

/// Build the fixture on `store` and return the snapshot plus the full `(ts, body)`
/// row set written.
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

/// [`build_fixture`] plus one deliberately smaller tail segment, the shape a bulk
/// load leaves behind (one small object per `(shard, hour)`). Returns the
/// snapshot, the full written row set, and the tail's object size, which callers
/// use as the block-range threshold so the tail lands at or below it and the
/// `SEGMENTS` full segments land above it.
async fn build_fixture_with_tail(
    store: &dyn ObjectStoreBackend,
) -> (Snapshot, BTreeSet<(i64, String)>, u64) {
    let (mut snapshot, mut want) = build_fixture(store).await;
    let ts = (SEGMENTS * 1000) as i64;
    let tail_record = record(ts, "tail-r0");
    want.insert((ts, tail_record.body.clone()));
    let mut content_hash = [0u8; 32];
    content_hash[0] = (SEGMENTS + 1) as u8;
    let tail = write_object(store, "logs/tail.rlog", content_hash, &[tail_record]).await;
    let tail_size = tail.object_size;

    // Prove the fixture actually splits across the threshold it is about to set:
    // if a one-record object were not strictly smaller than a three-record one,
    // every segment would sit on the same side and the test would pass for the
    // wrong reason.
    let smallest_full = snapshot
        .segments
        .iter()
        .map(|s| s.object_size)
        .min()
        .expect("SEGMENTS full segments");
    assert!(
        tail_size < smallest_full,
        "the tail object ({tail_size} B) must be strictly smaller than every full \
         segment (smallest {smallest_full} B) for the threshold split to be real"
    );

    snapshot.segments.push(tail);
    (snapshot, want, tail_size)
}

fn provider_with_accounting(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    accounting: QueryAccounting,
) -> LogsTableProvider {
    LogsTableProvider::new(snapshot, TenantHash([7u8; 16]), fetcher, accounting)
}

fn provider(snapshot: Snapshot, fetcher: LogSegmentFetcher) -> LogsTableProvider {
    provider_with_accounting(snapshot, fetcher, QueryAccounting::new())
}

async fn collect_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

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

fn find_by_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_by_name(c, name))
}

/// Sum a per-partition counter metric across every partition of the executed
/// `LogsScanExec`.
fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let set = find_by_name(plan, "LogsScanExec")
        .expect("a LogsScanExec leaf")
        .metrics()
        .expect("the scan publishes metrics");
    set.iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

fn build_read_cache(cache_bytes: u64) -> Arc<Cache<CacheFetchError>> {
    let max_entries = (cache_bytes / 4096).max(64) as usize;
    Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        max_entries,
        cache_bytes,
    )))
}

// ---- shape-counting store ------------------------------------------------

/// Counts `get` calls split by `GetRange` shape, so a test can pin the exact
/// number of suffix probes vs whole-object reads vs byte-range GETs a scan
/// issues.
struct ShapeCountingStore {
    inner: Arc<MemoryStore>,
    full: AtomicU64,
    suffix: AtomicU64,
    range: AtomicU64,
}

impl ShapeCountingStore {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(ShapeCountingStore {
            inner,
            full: AtomicU64::new(0),
            suffix: AtomicU64::new(0),
            range: AtomicU64::new(0),
        })
    }
    fn full_gets(&self) -> u64 {
        self.full.load(Ordering::SeqCst)
    }
    fn suffix_gets(&self) -> u64 {
        self.suffix.load(Ordering::SeqCst)
    }
    fn range_gets(&self) -> u64 {
        self.range.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for ShapeCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        match range {
            GetRange::Full => self.full.fetch_add(1, Ordering::SeqCst),
            GetRange::Suffix(_) => self.suffix.fetch_add(1, Ordering::SeqCst),
            GetRange::Range(_, _) => self.range.fetch_add(1, Ordering::SeqCst),
        };
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

/// A fetcher whose block-range threshold is 0, so every non-empty object counts
/// as ABOVE the threshold (the band the fast path is gated to), with ADR-0046's
/// read cache wired.
fn above_threshold_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    LogSegmentFetcher::new(store)
        .with_cache(build_read_cache(64 << 20))
        .with_block_range_threshold(0)
}

// ---- deliverable 1: the fast path ----------------------------------------

/// The whole-window fast path issues exactly one whole-object GET per segment and
/// ZERO suffix probes, returns every written row once, and its BlockMetrics
/// totals equal the snapshot's block totals exactly once.
#[tokio::test]
async fn predicate_free_full_window_reads_one_whole_object_per_segment() {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, want) = build_fixture(base.as_ref()).await;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let fetcher = above_threshold_fetcher(store);
    let plan = provider(snapshot, fetcher).plan(PARTS).expect("plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    // Exact request-count law: one whole-object read per segment, no probes,
    // no byte-range GETs.
    assert_eq!(
        counting.suffix_gets(),
        0,
        "fast path issues no suffix probes: got {}",
        counting.suffix_gets()
    );
    assert_eq!(
        counting.range_gets(),
        0,
        "fast path issues no byte-range GETs: got {}",
        counting.range_gets()
    );
    assert_eq!(
        counting.full_gets(),
        SEGMENTS as u64,
        "fast path issues exactly one whole-object read per segment"
    );

    // Rows are exactly the written set (identical to what the striped path
    // returns; see `fast_path_rows_match_the_striped_path`).
    assert_eq!(
        batches_to_rows(&batches),
        want,
        "fast path returns every written row once"
    );

    // BlockMetrics totals equal the snapshot's block totals, exactly once: a
    // segment opened twice (a broken assignment) would double these.
    assert_eq!(
        sum_metric(&plan, "blocks_total"),
        SEGMENTS * BLOCKS_PER_SEG,
        "blocks_total is the whole snapshot's block count, recorded once"
    );
    assert_eq!(
        sum_metric(&plan, "blocks_scanned"),
        SEGMENTS * BLOCKS_PER_SEG,
        "every block is scanned exactly once"
    );
}

/// The fast path returns byte-identical rows to the striped path over the same
/// data. The striped path is forced by asking for more partitions than there are
/// relevant segments, so the `relevant_segments >= target_partitions` conjunct
/// fails and the unchanged plan-then-stripe path runs. (It used to be forced by
/// raising the block-range threshold above the object size; #739 removed that
/// conjunct, so a high threshold no longer stripes anything.)
#[tokio::test]
async fn fast_path_rows_match_the_striped_path() {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, want) = build_fixture(base.as_ref()).await;
    let store: Arc<dyn ObjectStoreBackend> = base;

    let fast = above_threshold_fetcher(Arc::clone(&store));
    let fast_rows = batches_to_rows(
        &collect_plan(
            provider(snapshot.clone(), fast)
                .plan(PARTS)
                .expect("fast plan"),
        )
        .await,
    );

    let striped = above_threshold_fetcher(Arc::clone(&store));
    let striped_plan = provider(snapshot, striped)
        .plan(SEGMENTS * 2)
        .expect("striped plan");
    let striped_rows = batches_to_rows(&collect_plan(Arc::clone(&striped_plan)).await);
    assert_eq!(
        sum_metric(
            &striped_plan,
            "fast_path_rejected_fewer_segments_than_partitions"
        ),
        SEGMENTS * 2,
        "the comparison run must actually be on the striped path"
    );

    assert_eq!(fast_rows, striped_rows, "fast and striped rows must match");
    assert_eq!(fast_rows, want, "and both equal the written rows");
}

// ---- deliverable 1: each fail-closed conjunct takes the striped path ------

/// Every conjunct-failing shape runs the plan-then-stripe path instead of the
/// fast path, which a big-cache fixture makes observable as exactly `SEGMENTS`
/// suffix probes (one per segment, from the plan phase; the per-partition opens
/// hit the cached probe or carry the plan footer and add none) and zero
/// whole-object reads through the fast-path entry. The fast path issues the
/// opposite: zero suffix probes.
async fn assert_takes_striped_path(
    filters: &[datafusion::logical_expr::Expr],
    parts: usize,
    pending_erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> u64 {
    run_striped(filters, parts, pending_erasure).await.0
}

/// [`assert_takes_striped_path`], also handing back the executed plan so a caller
/// can read the `fast_path_rejected_*` counters off its metrics.
async fn run_striped(
    filters: &[datafusion::logical_expr::Expr],
    parts: usize,
    pending_erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> (u64, Arc<dyn ExecutionPlan>) {
    let base = Arc::new(MemoryStore::new());
    let (mut snapshot, _want) = build_fixture(base.as_ref()).await;
    snapshot.pending_erasure = pending_erasure;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let fetcher = above_threshold_fetcher(store);
    let plan = provider(snapshot, fetcher)
        .plan_filters(parts, filters)
        .expect("plan");
    let _ = collect_plan(Arc::clone(&plan)).await;
    (counting.suffix_gets(), plan)
}

#[tokio::test]
async fn conjunct_has_word_predicate_takes_striped_path() {
    use datafusion::logical_expr::{col, lit};
    // `has_word(body, 's0-r0')` -> a content predicate, so not predicate-free.
    let expr = ravel_sql::has_word_udf().call(vec![col("body"), lit("s0-r0")]);
    let probes = assert_takes_striped_path(&[expr], PARTS, Vec::new()).await;
    assert_eq!(
        probes, SEGMENTS as u64,
        "a content predicate runs the plan phase: one suffix probe per segment"
    );
}

#[tokio::test]
async fn conjunct_partial_ts_window_takes_striped_path() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};
    // A ts upper bound that cuts the last segment's last block: every segment
    // still overlaps (so all are relevant), but the last is not contained, so
    // the containment conjunct fails.
    let global_max = ((SEGMENTS - 1) * 1000 + (RECORDS_PER_SEG - 1)) as i64;
    let cut = global_max - 1;
    let expr = col("ts").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(cut), None)));
    let probes = assert_takes_striped_path(&[expr], PARTS, Vec::new()).await;
    assert_eq!(
        probes, SEGMENTS as u64,
        "a segment-cutting ts bound runs the plan phase: one suffix probe per segment"
    );
}

#[tokio::test]
async fn conjunct_pending_erasure_takes_striped_path() {
    // A pending erasure request on the snapshot makes the query
    // not-block-predicate-free even with no WHERE clause.
    let erasure = vec![ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "service.name".to_string(),
            value: "svc".to_string(),
        }],
        ..Default::default()
    }];
    let probes = assert_takes_striped_path(&[], PARTS, erasure).await;
    assert_eq!(
        probes, SEGMENTS as u64,
        "a pending erasure runs the plan phase: one suffix probe per segment"
    );
}

#[tokio::test]
async fn conjunct_fewer_segments_than_partitions_takes_striped_path() {
    // relevant_segments (4) < target_partitions (8): the whole-segment
    // assignment cannot fill every partition, so the striped path runs. The plan
    // footer it reads is carried to each block-subset open (deliverable 2), so
    // the opens add no probe and the count is exactly one per segment.
    let probes = assert_takes_striped_path(&[], SEGMENTS * 2, Vec::new()).await;
    assert_eq!(
        probes, SEGMENTS as u64,
        "undersubscribed striped path: one plan suffix probe per segment, none per open"
    );
}

// ---- issue #739: the threshold is not a query-wide conjunct ---------------

/// Every `fast_path_rejected_*` counter the scan can publish. A test that expects
/// one of them asserts the other three are absent, so a reason is pinned rather
/// than merely present.
const REJECTION_METRICS: [&str; 4] = [
    "fast_path_rejected_pending_erasure",
    "fast_path_rejected_block_predicate",
    "fast_path_rejected_segment_not_contained",
    "fast_path_rejected_fewer_segments_than_partitions",
];

/// Assert `expected` is the only rejection reason the executed `plan` recorded,
/// with one increment per partition. `None` asserts no rejection at all, i.e.
/// the fast path fired.
fn assert_rejection(plan: &Arc<dyn ExecutionPlan>, expected: Option<(&str, usize)>) {
    for name in REJECTION_METRICS {
        let got = sum_metric(plan, name);
        let want = match expected {
            Some((n, count)) if n == name => count,
            _ => 0,
        };
        assert_eq!(got, want, "{name}: expected {want} increments, got {got}");
    }
}

/// #739: a snapshot mixing `SEGMENTS` above-threshold segments with one
/// sub-threshold tail segment still takes the fast path, and reads each of the
/// `SEGMENTS + 1` segments in exactly one whole-object GET with zero probes.
///
/// Before #739 the threshold conjunct was query-wide, so the single tail object
/// disqualified all `SEGMENTS + 1`: the whole statement striped, paying one plan
/// suffix probe per above-threshold segment. That is what this test is red
/// against.
#[tokio::test]
async fn mixed_threshold_snapshot_takes_the_fast_path() {
    const RELEVANT: usize = SEGMENTS + 1;

    let base = Arc::new(MemoryStore::new());
    let (snapshot, want, tail_size) = build_fixture_with_tail(base.as_ref()).await;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    // `object_size > threshold` is false at exactly `tail_size`, so the tail is
    // the one sub-threshold segment and every full segment is above it.
    let fetcher = LogSegmentFetcher::new(store)
        .with_cache(build_read_cache(64 << 20))
        .with_block_range_threshold(tail_size);

    // PARTS <= RELEVANT, so the `relevant >= target_partitions` conjunct holds.
    let plan = provider(snapshot, fetcher).plan(PARTS).expect("plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    assert_eq!(
        counting.suffix_gets(),
        0,
        "one sub-threshold segment must not put the statement back on the probing \
         striped path (full={}, range={})",
        counting.full_gets(),
        counting.range_gets()
    );
    assert_eq!(
        counting.range_gets(),
        0,
        "the fast path issues no byte-range GETs"
    );
    assert_eq!(
        counting.full_gets(),
        RELEVANT as u64,
        "exactly one whole-object read per relevant segment, tail included"
    );
    assert_rejection(&plan, None);

    assert_eq!(
        batches_to_rows(&batches),
        want,
        "every written row, including the tail's, exactly once"
    );

    // Recorded once per segment, not once per partition and not once per open.
    assert_eq!(
        sum_metric(&plan, "blocks_total"),
        SEGMENTS * BLOCKS_PER_SEG + 1,
        "blocks_total covers the full segments plus the tail's single block"
    );
    assert_eq!(
        sum_metric(&plan, "blocks_scanned"),
        SEGMENTS * BLOCKS_PER_SEG + 1,
        "every block is scanned exactly once"
    );
}

/// The mixed snapshot's rows are identical to what the same data returns on the
/// striped path (forced by oversubscribing partitions), so joining the tail to
/// the whole-segment assignment changed the read shape and nothing else.
#[tokio::test]
async fn mixed_threshold_rows_match_the_striped_path() {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, want, tail_size) = build_fixture_with_tail(base.as_ref()).await;
    let store: Arc<dyn ObjectStoreBackend> = base;

    let fetcher = || {
        LogSegmentFetcher::new(Arc::clone(&store))
            .with_cache(build_read_cache(64 << 20))
            .with_block_range_threshold(tail_size)
    };

    let fast_plan = provider(snapshot.clone(), fetcher())
        .plan(PARTS)
        .expect("fast");
    let fast_rows = batches_to_rows(&collect_plan(Arc::clone(&fast_plan)).await);
    assert_rejection(&fast_plan, None);

    let striped_parts = (SEGMENTS + 1) * 2;
    let striped_plan = provider(snapshot, fetcher())
        .plan(striped_parts)
        .expect("striped");
    let striped_rows = batches_to_rows(&collect_plan(Arc::clone(&striped_plan)).await);
    assert_rejection(
        &striped_plan,
        Some((
            "fast_path_rejected_fewer_segments_than_partitions",
            striped_parts,
        )),
    );

    assert_eq!(fast_rows, striped_rows, "fast and striped rows must match");
    assert_eq!(fast_rows, want, "and both equal the written rows");
}

// ---- issue #739 deliverable 2: the recorded rejection reason -------------

#[tokio::test]
async fn rejection_reason_block_predicate() {
    use datafusion::logical_expr::{col, lit};
    let expr = ravel_sql::has_word_udf().call(vec![col("body"), lit("s0-r0")]);
    let (_probes, plan) = run_striped(&[expr], PARTS, Vec::new()).await;
    assert_rejection(&plan, Some(("fast_path_rejected_block_predicate", PARTS)));
}

#[tokio::test]
async fn rejection_reason_segment_not_contained() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};
    let global_max = ((SEGMENTS - 1) * 1000 + (RECORDS_PER_SEG - 1)) as i64;
    let expr = col("ts").lt_eq(lit(ScalarValue::TimestampNanosecond(
        Some(global_max - 1),
        None,
    )));
    let (_probes, plan) = run_striped(&[expr], PARTS, Vec::new()).await;
    assert_rejection(
        &plan,
        Some(("fast_path_rejected_segment_not_contained", PARTS)),
    );
}

#[tokio::test]
async fn rejection_reason_pending_erasure() {
    let erasure = vec![ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "service.name".to_string(),
            value: "svc".to_string(),
        }],
        ..Default::default()
    }];
    let (_probes, plan) = run_striped(&[], PARTS, erasure).await;
    assert_rejection(&plan, Some(("fast_path_rejected_pending_erasure", PARTS)));
}

#[tokio::test]
async fn rejection_reason_fewer_segments_than_partitions() {
    let parts = SEGMENTS * 2;
    let (_probes, plan) = run_striped(&[], parts, Vec::new()).await;
    assert_rejection(
        &plan,
        Some(("fast_path_rejected_fewer_segments_than_partitions", parts)),
    );
}

// ---- issue #1006: the fast path's `data_objects_touched` recorder ---------
//
// ADR-0996 decision 3 makes `data_objects_touched` the denominator of
// `range_amplification = data_GET_requests / data_objects_touched`, and the
// fast path is exactly the full-scan shape the amplification gate targets. It
// has no plan phase, so it must pick its own single recorder:
// `PartitionCtx::record_data_object_touched`, called from the owning partition
// once per relevant segment. These tests pin the count exactly, because the
// failure that matters is a per-partition site multiplying it by
// `target_partitions` rather than one that never fires.

/// The fast path records exactly one touch per segment: `SEGMENTS`, not
/// `SEGMENTS * PARTS`. The equality against the store's whole-object GET count
/// is the amplification ratio this counter exists to make computable -- one
/// data GET per object here, so the ratio is exactly 1.
#[tokio::test]
async fn fast_path_records_one_data_object_touched_per_segment() {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, want) = build_fixture(base.as_ref()).await;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let accounting = QueryAccounting::new();
    let plan =
        provider_with_accounting(snapshot, above_threshold_fetcher(store), accounting.clone())
            .plan(PARTS)
            .expect("plan");
    let batches = collect_plan(Arc::clone(&plan)).await;

    // The run really is on the fast path, and really did read the data.
    assert_rejection(&plan, None);
    assert_eq!(batches_to_rows(&batches), want, "every written row once");

    let snap = accounting.snapshot();
    assert_eq!(
        snap.data_objects_touched,
        SEGMENTS as u64,
        "one touch per distinct data object; a per-partition recording site would \
         report {} instead",
        SEGMENTS * PARTS
    );
    assert_eq!(
        snap.data_objects_touched,
        counting.full_gets(),
        "the denominator matches the scan-phase data GETs, so range amplification \
         is exactly 1 for a whole-object fast-path read"
    );
    // The counter is fast-path-only, so it equals the fast path's own open
    // count; the planned route's recorder lives elsewhere (see
    // `one_statement_takes_exactly_one_route`).
    assert_eq!(
        snap.data_objects_touched,
        snap.logs_whole_object_opens + snap.logs_ranged_opens,
        "one touch per fast-path open"
    );
}

/// A second run of the same statement over the same read cache issues no store
/// request at all and still records `SEGMENTS`: the counter is the query's
/// object working set, not its cache misses (the snapshot field's third
/// exclusion).
#[tokio::test]
async fn fast_path_counts_segments_served_entirely_from_cache() {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, want) = build_fixture(base.as_ref()).await;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    // One cache shared by both runs, so the second run finds every segment's
    // bytes already resident.
    let cache = build_read_cache(64 << 20);
    let fetcher = || {
        LogSegmentFetcher::new(Arc::clone(&store))
            .with_cache(Arc::clone(&cache))
            .with_block_range_threshold(0)
    };

    let warm_accounting = QueryAccounting::new();
    let warm_plan = provider_with_accounting(snapshot.clone(), fetcher(), warm_accounting.clone())
        .plan(PARTS)
        .expect("warming plan");
    let warm_rows = batches_to_rows(&collect_plan(Arc::clone(&warm_plan)).await);
    assert_rejection(&warm_plan, None);
    let gets_after_warm = counting.full_gets();
    assert_eq!(
        gets_after_warm, SEGMENTS as u64,
        "the warming run reads each segment once from the store"
    );
    assert_eq!(
        warm_accounting.snapshot().data_objects_touched,
        SEGMENTS as u64,
        "the warming run touches each segment once"
    );

    let cached_accounting = QueryAccounting::new();
    let cached_plan = provider_with_accounting(snapshot, fetcher(), cached_accounting.clone())
        .plan(PARTS)
        .expect("cached plan");
    let cached_rows = batches_to_rows(&collect_plan(Arc::clone(&cached_plan)).await);
    assert_rejection(&cached_plan, None);

    // The precondition the test's claim rests on: the second run issued no
    // store request of any shape, so every block it read came from the cache.
    assert_eq!(
        counting.full_gets(),
        gets_after_warm,
        "the second run must be served entirely from cache"
    );
    assert_eq!(counting.suffix_gets(), 0, "and probe nothing");
    assert_eq!(counting.range_gets(), 0, "and range over nothing");

    assert_eq!(
        cached_accounting.snapshot().data_objects_touched,
        SEGMENTS as u64,
        "a fully cache-served fast-path statement still touches every segment: the \
         counter measures the query's object working set, not its misses"
    );
    assert_eq!(cached_rows, want, "and returns every written row once");
    assert_eq!(
        cached_rows, warm_rows,
        "identical to the warming run's rows"
    );
}

/// A statement with a content predicate defeats the fast path and plans
/// instead, so this crate's recorder must not fire for it.
///
/// The total is asserted at zero rather than at `SEGMENTS` because on this tree
/// the planned route has no recorder yet: 996-3 adds one at
/// `ravel_query::LogSegmentFetcher::plan_segment`, which owns that route. When
/// it lands, this assertion moves to the fast-path counters alone (already
/// asserted zero here) and the total becomes `SEGMENTS`.
#[tokio::test]
async fn planned_route_records_nothing_at_the_fast_path_site() {
    use datafusion::logical_expr::{col, lit};

    let base = Arc::new(MemoryStore::new());
    let (snapshot, _want) = build_fixture(base.as_ref()).await;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let accounting = QueryAccounting::new();
    let expr = ravel_sql::has_word_udf().call(vec![col("body"), lit("s0-r0")]);
    let plan =
        provider_with_accounting(snapshot, above_threshold_fetcher(store), accounting.clone())
            .plan_filters(PARTS, &[expr])
            .expect("plan");
    let _ = collect_plan(Arc::clone(&plan)).await;

    // The run really planned: every partition rejected the fast path, and the
    // plan phase probed once per segment.
    assert_rejection(&plan, Some(("fast_path_rejected_block_predicate", PARTS)));
    assert_eq!(
        counting.suffix_gets(),
        SEGMENTS as u64,
        "the plan phase issues one suffix probe per segment"
    );

    let snap = accounting.snapshot();
    assert_eq!(
        snap.logs_whole_object_opens + snap.logs_ranged_opens,
        0,
        "no fast-path open ran, so the fast path's recorder was never reached"
    );
    assert_eq!(
        snap.data_objects_touched, 0,
        "this crate's fast-path recorder does not fire on the planned route"
    );
}

/// What one statement's run reveals about which route it took.
struct RouteObservation {
    /// `data_objects_touched`, recorded only by the fast path's site.
    touched: u64,
    /// Fast-path opens by either read shape, recorded only by the fast path.
    fast_opens: u64,
    /// `fast_path_rejected_*` increments summed over every reason and partition.
    rejected: usize,
    /// Suffix probes, which only the plan phase issues on this fixture.
    suffix_gets: u64,
}

async fn observe_route(
    filters: &[datafusion::logical_expr::Expr],
    parts: usize,
    pending_erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> RouteObservation {
    let base = Arc::new(MemoryStore::new());
    let (mut snapshot, _want) = build_fixture(base.as_ref()).await;
    snapshot.pending_erasure = pending_erasure;

    let counting = ShapeCountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let accounting = QueryAccounting::new();
    let plan =
        provider_with_accounting(snapshot, above_threshold_fetcher(store), accounting.clone())
            .plan_filters(parts, filters)
            .expect("plan");
    let _ = collect_plan(Arc::clone(&plan)).await;

    let snap = accounting.snapshot();
    RouteObservation {
        touched: snap.data_objects_touched,
        fast_opens: snap.logs_whole_object_opens + snap.logs_ranged_opens,
        rejected: REJECTION_METRICS
            .iter()
            .map(|name| sum_metric(&plan, name))
            .sum(),
        suffix_gets: counting.suffix_gets(),
    }
}

/// Route exclusivity: one statement takes the fast path or the planned path,
/// never both, so the fast path's recorder and 996-3's `plan_segment` recorder
/// can never both count the same object.
///
/// Both routes are observed across the fast-path shape and every conjunct that
/// defeats it. A statement is on the fast path for ALL of its partitions (zero
/// rejections, zero plan probes) or on the planned path for all of them (one
/// rejection per partition, one plan probe per segment) -- the routing decision
/// reads only the snapshot and the query, so it cannot differ by partition. And
/// in every case `data_objects_touched` equals the fast-path open count, which
/// is what makes this crate's recorder provably fast-path-only.
#[tokio::test]
async fn one_statement_takes_exactly_one_route() {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};

    let global_max = ((SEGMENTS - 1) * 1000 + (RECORDS_PER_SEG - 1)) as i64;
    let erasure = vec![ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "service.name".to_string(),
            value: "svc".to_string(),
        }],
        ..Default::default()
    }];

    let cases: Vec<(&str, Vec<Expr>, usize, Vec<_>, bool)> = vec![
        (
            "predicate-free full window",
            Vec::new(),
            PARTS,
            Vec::new(),
            true,
        ),
        (
            "content predicate",
            vec![ravel_sql::has_word_udf().call(vec![col("body"), lit("s0-r0")])],
            PARTS,
            Vec::new(),
            false,
        ),
        (
            "segment-cutting ts bound",
            vec![col("ts").lt_eq(lit(ScalarValue::TimestampNanosecond(
                Some(global_max - 1),
                None,
            )))],
            PARTS,
            Vec::new(),
            false,
        ),
        ("pending erasure", Vec::new(), PARTS, erasure, false),
        (
            "fewer segments than partitions",
            Vec::new(),
            SEGMENTS * 2,
            Vec::new(),
            false,
        ),
    ];

    for (label, filters, parts, pending_erasure, expect_fast) in cases {
        let obs = observe_route(&filters, parts, pending_erasure).await;
        if expect_fast {
            assert_eq!(
                obs.rejected, 0,
                "{label}: no partition may reject the fast path"
            );
            assert_eq!(
                obs.suffix_gets, 0,
                "{label}: the fast path runs no plan phase"
            );
            assert_eq!(
                obs.touched, SEGMENTS as u64,
                "{label}: one touch per segment, recorded by the fast path's site"
            );
        } else {
            assert_eq!(
                obs.rejected, parts,
                "{label}: every partition must reject the fast path"
            );
            assert_eq!(
                obs.suffix_gets, SEGMENTS as u64,
                "{label}: the plan phase probes once per segment"
            );
            assert_eq!(
                obs.touched, 0,
                "{label}: the planned route is 996-3's `plan_segment` recorder's, \
                 not this site's"
            );
        }
        assert_eq!(
            obs.touched, obs.fast_opens,
            "{label}: touches are recorded exactly where fast-path opens are, so a \
             statement cannot be counted by both routes' recorders"
        );
    }
}
