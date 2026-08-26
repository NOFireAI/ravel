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
