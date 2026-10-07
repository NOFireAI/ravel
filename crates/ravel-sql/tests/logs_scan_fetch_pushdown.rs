//! Tests for issue #362: `LogsScanExec::fetch()`/`with_fetch()` let
//! DataFusion's `LimitPushdown` physical optimizer push a LIMIT straight into
//! the scan instead of wrapping every partition's leaf in a `LocalLimitExec`.
//!
//! Since issue #2616 the pushed fetch is an exact per-partition cap: each
//! partition truncates the batch that reaches it to `fetch` rows, ends its
//! stream there even partway through a segment, and opens no further owned
//! segment. The tests pin:
//!
//! - `limit_removes_local_limit_and_reports_fetch_on_the_scan`: the optimized
//!   plan of `SELECT ts FROM logs LIMIT 10` has no `LocalLimitExec`, and the
//!   scan reports `fetch() == Some(10)`.
//! - `fetch_pushdown_returns_exact_counts_and_correct_rows`: over four
//!   partitions, a LIMIT below, at, and above the row count returns exactly
//!   `min(k, total)` written rows, for a plain scan, `ORDER BY`, and a
//!   residual `WHERE`.
//! - `single_partition_fetch_is_exact_with_no_limit_above_the_scan`: with one
//!   partition no limit operator is left above the scan, and the scan alone
//!   returns exactly `min(k, total)` rows, including a limit that ends inside a
//!   segment or a block.
//! - `fetch_stops_opening_segments_once_the_partition_limit_is_met`: a
//!   partition that owns two whole segments opens only the first when that
//!   one already meets its fetch.
//! - `fetch_stopped_segment_publishes_its_scan_metrics`: a fast-path segment
//!   the fetch stops inside its first block publishes the blocks and pages
//!   decoded up to the stop and the segment's whole `blocks_total`.
//! - `fetch_stopped_segment_keeps_its_postings_prune_figure`: under an
//!   attribute equality the postings index prunes one of three blocks, and a
//!   fetch that stops the scan inside the first surviving block still reports
//!   that one pruned block.
//! - `fetch_narrows_the_scan_statistics`: the scan's `partition_statistics`
//!   count what it emits under a fetch, not every committed row.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::LogsTableProvider;
use ravel_types::TenantHash;
use uuid::Uuid;

use util::CountingStore;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 3 records so a several-record object still has more than
/// one block.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A record on the single-`service.name` stream `svc`.
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
    write_object_indexed(store, key, content_hash, records, &[]).await
}

/// [`write_object`] with POSTINGS built for the `indexed` record attributes.
async fn write_object_indexed(
    store: &dyn ObjectStoreBackend,
    key: &str,
    content_hash: [u8; 32],
    records: &[LogRecord],
    indexed: &[&str],
) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity())
        .with_indexed_fields(indexed.iter().map(|s| s.to_string()).collect());
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

fn provider(snapshot: Snapshot, fetcher: LogSegmentFetcher) -> LogsTableProvider {
    LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        PhaseAccounting::new(),
    )
}

async fn collect_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The `(ts, body)` pairs in `batches` from a `SELECT ts, body FROM logs ...`
/// projection (col 0 = ts, col 1 = body).
fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let body = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body col");
        for i in 0..batch.num_rows() {
            out.insert((ts.value(i), body.value(i).to_string()));
        }
    }
    out
}

/// The `ts` values of `batches`, in emission order (col 0 of a
/// `SELECT ts, ...` projection).
fn ts_sequence(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        for i in 0..batch.num_rows() {
            out.push(ts.value(i));
        }
    }
    out
}

/// The `LogsScanExec` leaf of a physical plan, found by walking (the plan
/// above it is whatever the optimizer built).
fn find_by_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_by_name(c, name))
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// The assertion that distinguishes this change from its no-op predecessor:
/// with `fetch`/`with_fetch` implemented, `LimitPushdown` absorbs the LIMIT
/// into the scan instead of wrapping it in a `LocalLimitExec`, and the scan
/// reports the pushed value back.
///
/// Before this change, `LogsScanExec` used the trait's default `fetch()`
/// (`None`) and had no `with_fetch` override, so `LimitPushdown` could not
/// push into it and inserted a `LocalLimitExec` per partition instead: both
/// assertions below fail on that code (`find_by_name(&plan,
/// "LocalLimitExec")` finds one, and there is no scan-reported fetch to read
/// because `LogsScanExec` never overrode `fetch()`).
#[tokio::test]
async fn limit_removes_local_limit_and_reports_fetch_on_the_scan() {
    let store = MemoryStore::new();
    let mut segments = Vec::new();
    for s in 0..3usize {
        let recs: Vec<LogRecord> = (0..5)
            .map(|i| record((s * 10 + i) as i64, &format!("s{s}-r{i}")))
            .collect();
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        segments
            .push(write_object(&store, &format!("logs/seg{s}.rlog"), content_hash, &recs).await);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(backend);
    let table_provider = provider(snapshot, fetcher);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");

    let plan = ctx
        .sql("SELECT ts FROM logs LIMIT 10")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    assert!(
        find_by_name(&plan, "LocalLimitExec").is_none(),
        "LimitPushdown must not need a LocalLimitExec once the scan implements \
         `fetch`/`with_fetch` (issue #362); plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let scan = find_by_name(&plan, "LogsScanExec").expect("a LogsScanExec leaf");
    assert_eq!(
        scan.fetch(),
        Some(10),
        "the leaf must report the pushed LIMIT via `ExecutionPlan::fetch`; plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
}

// ---------------------------------------------------------------------------
// Soundness: exact counts, correct rows, ORDER BY, residual WHERE
// ---------------------------------------------------------------------------

const SEGS: usize = 4;
const PER_SEG: usize = 5;
const TOTAL: usize = SEGS * PER_SEG;

/// Records whose `ts` interleaves across segments (`i * SEGS + s`, not
/// `s`-then-`i`), so concatenating segments in snapshot order is not
/// ts-ascending and the `ORDER BY` case below is non-vacuous. Each body
/// carries "even"/"odd" by ts parity so a `WHERE body LIKE '%odd%'` residual
/// has a real, known-size matching subset.
fn seg_records(s: usize) -> Vec<LogRecord> {
    (0..PER_SEG)
        .map(|i| {
            let ts = (i * SEGS + s) as i64;
            let parity = if ts % 2 == 0 { "even" } else { "odd" };
            record(ts, &format!("{parity}-s{s}-r{i}"))
        })
        .collect()
}

async fn build_fixture(store: &dyn ObjectStoreBackend) -> (Snapshot, BTreeSet<(i64, String)>) {
    let mut segments = Vec::with_capacity(SEGS);
    let mut want = BTreeSet::new();
    for s in 0..SEGS {
        let recs = seg_records(s);
        for r in &recs {
            want.insert((r.ts_ns, r.body.clone()));
        }
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        let seg = write_object(
            store,
            &format!("logs/soundness-seg{s}.rlog"),
            content_hash,
            &recs,
        )
        .await;
        segments.push(seg);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (snapshot, want)
}

/// Per-partition fetch is a bound the partition MAY act on, never a divided
/// share: DataFusion pushes the same LIMIT value to every partition of a
/// multi-partition leaf, and the exact cross-partition cap is enforced above
/// by `CoalescePartitionsExec`'s own `fetch`. This test pins the failure mode
/// the task calls out as worst: a wrong early stop that returns fewer rows
/// than requested. Every case below asserts the exact count (never fewer,
/// never more than available) and that every returned row is one that was
/// actually written and (for the WHERE case) actually matches the predicate.
#[tokio::test]
async fn fetch_pushdown_returns_exact_counts_and_correct_rows() {
    let store = MemoryStore::new();
    let (snapshot, want) = build_fixture(&store).await;
    assert_eq!(
        want.len(),
        TOTAL,
        "fixture must have no colliding (ts, body) pairs"
    );
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(backend);
    let table_provider = provider(snapshot, fetcher);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");

    // Case A: a plain LIMIT below, at, and above TOTAL returns exactly
    // min(k, TOTAL) rows, every one of them a real written row.
    for k in [1usize, PER_SEG, TOTAL, TOTAL + 7] {
        let batches = ctx
            .sql(&format!("SELECT ts, body FROM logs LIMIT {k}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let want_count = k.min(TOTAL);
        assert_eq!(
            total_rows(&batches),
            want_count,
            "LIMIT {k} over {TOTAL} written rows must return exactly {want_count}, never fewer"
        );
        let got = batches_to_rows(&batches);
        assert!(
            got.is_subset(&want),
            "LIMIT {k} must return only rows that were actually written; got {got:?}"
        );
    }

    // Case B: ORDER BY ts LIMIT k returns exactly the first k of the fully
    // sorted oracle, ts ascending.
    let mut sorted_ts: Vec<i64> = want.iter().map(|(ts, _)| *ts).collect();
    sorted_ts.sort_unstable();
    for k in [3usize, TOTAL, TOTAL + 4] {
        let batches = ctx
            .sql(&format!("SELECT ts, body FROM logs ORDER BY ts LIMIT {k}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let got = ts_sequence(&batches);
        let want_count = k.min(TOTAL);
        assert_eq!(
            got,
            sorted_ts[..want_count],
            "ORDER BY ts LIMIT {k} must return exactly the first {want_count} ts \
             values ascending, never fewer"
        );
    }

    // Case C: a residual WHERE above the scan (a LIKE pattern the scan does
    // not push as a content prune) plus LIMIT returns exactly the right count
    // out of the matching subset, never fewer.
    let matching: BTreeSet<(i64, String)> = want
        .iter()
        .filter(|(_, b)| b.contains("odd"))
        .cloned()
        .collect();
    assert!(
        !matching.is_empty() && matching.len() < TOTAL,
        "fixture must have a proper, nonempty odd/even split"
    );
    for k in [2usize, matching.len(), matching.len() + 3] {
        let batches = ctx
            .sql(&format!(
                "SELECT ts, body FROM logs WHERE body LIKE '%odd%' LIMIT {k}"
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let want_count = k.min(matching.len());
        assert_eq!(
            total_rows(&batches),
            want_count,
            "WHERE body LIKE '%odd%' LIMIT {k} must return exactly {want_count} \
             matching rows, never fewer"
        );
        let got = batches_to_rows(&batches);
        assert!(
            got.is_subset(&matching),
            "every WHERE + LIMIT row must satisfy the residual predicate; got {got:?}"
        );
    }
}

/// With one partition, `LimitPushdown` leaves nothing above the scan to cap
/// its output: no `GlobalLimitExec` and no `CoalescePartitionsExec` (issue
/// #2616). The scan's own fetch is then the only limit, so it must be exact,
/// including a limit that ends partway through a segment or a block.
#[tokio::test]
async fn single_partition_fetch_is_exact_with_no_limit_above_the_scan() {
    let store = MemoryStore::new();
    let (snapshot, want) = build_fixture(&store).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(backend);
    let table_provider = provider(snapshot, fetcher);

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");

    for k in [1usize, 2, PER_SEG, PER_SEG + 2, TOTAL, TOTAL + 7] {
        let sql = format!("SELECT ts, body FROM logs LIMIT {k}");
        let df = ctx.sql(&sql).await.expect("plan");
        let plan = df.create_physical_plan().await.expect("physical plan");
        for above in ["GlobalLimitExec", "CoalescePartitionsExec"] {
            assert!(
                find_by_name(&plan, above).is_none(),
                "{sql}: the premise is a plan with no {above}; plan:\n{}",
                displayable(plan.as_ref()).indent(true)
            );
        }
        let batches = collect_plan(plan).await;
        let want_count = k.min(TOTAL);
        assert_eq!(
            total_rows(&batches),
            want_count,
            "{sql} over {TOTAL} written rows must return exactly {want_count}"
        );
        let got = batches_to_rows(&batches);
        assert!(
            got.is_subset(&want),
            "{sql} must return only rows that were actually written; got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// GET-count regression: the scan stops opening segments once its partition's
// pushed fetch is satisfied
// ---------------------------------------------------------------------------

const GC_SEGMENTS: usize = 8;
const GC_PARTS: usize = 4;
const GC_RECORDS_PER_SEG: usize = 3;

fn gc_seg_records(s: usize) -> Vec<LogRecord> {
    (0..GC_RECORDS_PER_SEG)
        .map(|i| record((s * 1000 + i) as i64, &format!("s{s}-r{i}")))
        .collect()
}

/// A predicate-free, full-window fixture with `GC_SEGMENTS` segments and
/// `GC_SEGMENTS >= GC_PARTS`, which is exactly `LogsScanExec::
/// whole_segment_fast_path`'s admission rule: each of the `GC_PARTS`
/// partitions owns `GC_SEGMENTS / GC_PARTS` whole segments, assigned
/// `segment j -> partition j % GC_PARTS`, opened in ascending ordinal order.
async fn build_gc_fixture(store: &dyn ObjectStoreBackend) -> Snapshot {
    let mut segments = Vec::with_capacity(GC_SEGMENTS);
    for s in 0..GC_SEGMENTS {
        let recs = gc_seg_records(s);
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        let seg = write_object(store, &format!("logs/gc-seg{s}.rlog"), content_hash, &recs).await;
        segments.push(seg);
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The property #362 is actually for: once a partition's own emitted row
/// count reaches its pushed `fetch`, it must stop opening further segments,
/// even though (per the plan-shape test above) nothing above it in the plan
/// is left to enforce that by polling less. Each `GC_PARTS` partition here
/// owns two whole segments; a fetch of 1 is satisfied by the first segment's
/// `GC_RECORDS_PER_SEG` rows alone, so the second must never be opened.
///
/// Without the early stop in `LogScanStream::poll_inner`, every partition
/// would drain both of its owned segments and this would read `GC_SEGMENTS`
/// (8) objects instead of `GC_PARTS` (4).
#[tokio::test]
async fn fetch_stops_opening_segments_once_the_partition_limit_is_met() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let snapshot = build_gc_fixture(store.as_ref()).await;
    assert_eq!(
        counting.gets(),
        0,
        "building the fixture must only PUT, never GET"
    );

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let table_provider = provider(snapshot, fetcher);

    let plan = table_provider.plan(GC_PARTS).expect("plan");
    assert_eq!(
        counting.gets(),
        0,
        "building the plan must do no I/O (the fast path has no plan phase)"
    );

    let with_fetch = plan
        .with_fetch(Some(1))
        .expect("LogsScanExec must implement ExecutionPlan::with_fetch (issue #362)");
    assert_eq!(with_fetch.fetch(), Some(1));

    let batches = collect_plan(with_fetch).await;
    assert!(
        !batches.is_empty(),
        "each of the {GC_PARTS} partitions must still emit its first owned segment's rows"
    );

    assert_eq!(
        counting.gets(),
        GC_PARTS as u64,
        "each of the {GC_PARTS} partitions owns {} whole segments \
         (GC_SEGMENTS / GC_PARTS); with fetch = 1, every partition's first \
         owned segment (which alone has {GC_RECORDS_PER_SEG} rows) already \
         satisfies it, so no partition opens its second segment. At most \
         `partitions` segments opened, plus zero resolve GETs (the fast path \
         skips the plan phase entirely).",
        GC_SEGMENTS / GC_PARTS,
    );
}

// ---------------------------------------------------------------------------
// Metrics: a segment the fetch stops partway through still reports its scan
// ---------------------------------------------------------------------------

/// Sum a per-partition counter metric across every partition of the executed
/// `LogsScanExec` in `plan`.
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

const MB_BLOCKS: usize = 3;

/// Run `sql` over one segment of `MB_BLOCKS` three-record blocks on one
/// partition, and return the executed plan so its metrics can be read.
async fn run_one_segment(sql: &str) -> (Arc<dyn ExecutionPlan>, usize) {
    let store = MemoryStore::new();
    let recs: Vec<LogRecord> = (0..MB_BLOCKS * 3)
        .map(|i| record(i as i64, &format!("r{i}")))
        .collect();
    let seg = write_object(&store, "logs/mb-seg0.rlog", [9u8; 32], &recs).await;
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let table_provider = provider(snapshot, LogSegmentFetcher::new(backend));
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let rows = total_rows(&collect_plan(Arc::clone(&plan)).await);
    (plan, rows)
}

/// A LIMIT met inside the first block of a three-block segment ends the stream
/// with that segment's scan still open. Its counters must still be published:
/// one block scanned, and exactly a third of the pages the full scan decodes
/// and skips (every block holds three records of the same shape). The
/// predicate-free one-partition query takes the whole-segment fast path, which
/// records `blocks_total` at the segment's end, so that total must be
/// published at the stop too, and equal the full scan's.
#[tokio::test]
async fn fetch_stopped_segment_publishes_its_scan_metrics() {
    let (full, full_rows) = run_one_segment("SELECT body FROM logs").await;
    assert_eq!(full_rows, MB_BLOCKS * 3);
    assert_eq!(
        sum_metric(&full, "fast_path_whole_object_segments")
            + sum_metric(&full, "fast_path_ranged_segments"),
        1,
        "the premise is the whole-segment fast path"
    );
    assert_eq!(sum_metric(&full, "blocks_total"), MB_BLOCKS);
    assert_eq!(sum_metric(&full, "blocks_scanned"), MB_BLOCKS);
    let full_decoded = sum_metric(&full, "pages_decoded");
    let full_skipped = sum_metric(&full, "pages_skipped");
    assert!(
        full_decoded > 0 && full_decoded.is_multiple_of(MB_BLOCKS),
        "every block decodes the same pages; got {full_decoded}"
    );
    assert!(
        full_skipped > 0 && full_skipped.is_multiple_of(MB_BLOCKS),
        "every block skips the same unprojected pages; got {full_skipped}"
    );

    let (limited, limited_rows) = run_one_segment("SELECT body FROM logs LIMIT 1").await;
    assert_eq!(limited_rows, 1);
    assert_eq!(
        find_by_name(&limited, "LogsScanExec")
            .expect("a LogsScanExec leaf")
            .fetch(),
        Some(1),
        "the premise is a fetch pushed into the scan"
    );
    assert_eq!(
        sum_metric(&limited, "blocks_scanned"),
        1,
        "the stop came inside the first block"
    );
    assert_eq!(
        sum_metric(&limited, "pages_decoded"),
        full_decoded / MB_BLOCKS
    );
    assert_eq!(
        sum_metric(&limited, "pages_skipped"),
        full_skipped / MB_BLOCKS
    );
    assert_eq!(
        sum_metric(&limited, "blocks_total"),
        MB_BLOCKS,
        "the whole-segment total is known from the open"
    );
}

/// The physical plan of `sql` over one segment of `MB_BLOCKS` three-record
/// blocks on one partition, where the indexed record attribute `region` is
/// `us` in the first block and `eu` in the rest. Not executed, so the caller
/// may install a fetch on its scan first.
async fn region_segment_plan(sql: &str) -> Arc<dyn ExecutionPlan> {
    let store = MemoryStore::new();
    let recs: Vec<LogRecord> = (0..MB_BLOCKS * 3)
        .map(|i| {
            let region = if i < 3 { "us" } else { "eu" };
            LogRecord {
                attrs: vec![("region".to_string(), AttrValue::Str(region.to_string()))],
                ..record(i as i64, &format!("r{i}"))
            }
        })
        .collect();
    let seg = write_object_indexed(
        &store,
        "logs/region-seg0.rlog",
        [10u8; 32],
        &recs,
        &["region"],
    )
    .await;
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let table_provider = provider(snapshot, LogSegmentFetcher::new(backend));
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");
    ctx.sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan")
}

/// An attribute equality the postings index answers prunes the first of the
/// three blocks. A fetch of 1 then stops the scan inside the first surviving
/// block, and `blocks_pruned_by_postings` must still read the one block the
/// postings step dropped from the segment, as it does on the full scan.
#[tokio::test]
async fn fetch_stopped_segment_keeps_its_postings_prune_figure() {
    // The function form of `attrs['region']`, which a bare `SessionContext`
    // plans without ravel-sql's expression planner.
    let sql = "SELECT body FROM logs WHERE get_field(attrs, 'region') = 'eu'";

    let full = region_segment_plan(sql).await;
    assert_eq!(
        total_rows(&collect_plan(Arc::clone(&full)).await),
        (MB_BLOCKS - 1) * 3
    );
    assert_eq!(sum_metric(&full, "blocks_total"), MB_BLOCKS);
    assert_eq!(sum_metric(&full, "blocks_scanned"), MB_BLOCKS - 1);
    assert_eq!(
        sum_metric(&full, "blocks_pruned_by_postings"),
        1,
        "postings drops the one block holding only region = us"
    );

    let scan = find_by_name(&region_segment_plan(sql).await, "LogsScanExec")
        .expect("a LogsScanExec leaf")
        .with_fetch(Some(1))
        .expect("the scan takes a fetch");
    assert_eq!(total_rows(&collect_plan(Arc::clone(&scan)).await), 1);
    assert_eq!(
        sum_metric(&scan, "blocks_scanned"),
        1,
        "the stop came inside the first surviving block"
    );
    assert_eq!(sum_metric(&scan, "blocks_total"), MB_BLOCKS);
    assert_eq!(
        sum_metric(&scan, "blocks_pruned_by_postings"),
        1,
        "the fetch stop keeps the segment's postings prune figure"
    );
}

// ---------------------------------------------------------------------------
// Statistics under a fetch
// ---------------------------------------------------------------------------

/// The `(num_rows, ts min/max)` statistics of `plan` after `with_fetch(fetch)`.
fn fetched_stats(
    plan: &Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
) -> (
    Precision<usize>,
    Precision<ScalarValue>,
    Precision<ScalarValue>,
) {
    let plan = match fetch {
        Some(_) => plan.with_fetch(fetch).expect("the scan takes a fetch"),
        None => Arc::clone(plan),
    };
    let ts = plan.schema().index_of("ts").expect("a ts column");
    let stats = plan.partition_statistics(None).expect("statistics");
    let col = &stats.column_statistics[ts];
    (stats.num_rows, col.min_value.clone(), col.max_value.clone())
}

/// Over the 20-row fixture, one partition under `fetch = 1` emits exactly one
/// row, so `num_rows` is `Exact(1)`; four partitions each stop at one row
/// they own, so the count is only bounded and reads `Inexact(1)`. Either way
/// the emitted row is a subset, so the `ts` extrema the catalog proves for the
/// whole table drop to `Inexact`. A fetch at or above the total cuts nothing
/// and leaves every figure `Exact`.
#[tokio::test]
async fn fetch_narrows_the_scan_statistics() {
    let store = MemoryStore::new();
    let (snapshot, _) = build_fixture(&store).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let table_provider = provider(snapshot, LogSegmentFetcher::new(backend));
    let ts_min = Precision::Exact(ScalarValue::TimestampNanosecond(Some(0), None));
    let ts_max = Precision::Exact(ScalarValue::TimestampNanosecond(
        Some(TOTAL as i64 - 1),
        None,
    ));

    let one = table_provider.plan(1).expect("plan");
    let four = table_provider.plan(SEGS).expect("plan");
    assert_eq!(one.properties().partitioning.partition_count(), 1);
    assert_eq!(four.properties().partitioning.partition_count(), SEGS);

    for plan in [&one, &four] {
        assert_eq!(
            fetched_stats(plan, None),
            (Precision::Exact(TOTAL), ts_min.clone(), ts_max.clone())
        );
        assert_eq!(
            fetched_stats(plan, Some(TOTAL)),
            (Precision::Exact(TOTAL), ts_min.clone(), ts_max.clone())
        );
    }
    assert_eq!(
        fetched_stats(&one, Some(1)),
        (
            Precision::Exact(1),
            ts_min.clone().to_inexact(),
            ts_max.clone().to_inexact()
        )
    );
    assert_eq!(
        fetched_stats(&four, Some(1)),
        (
            Precision::Inexact(1),
            ts_min.to_inexact(),
            ts_max.to_inexact()
        )
    );
}
