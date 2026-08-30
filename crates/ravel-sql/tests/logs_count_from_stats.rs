//! Integration tests for answering a predicate-free `SELECT COUNT(*) FROM
//! logs` from catalog row counts instead of scanning. See issue #698.
//!
//! `LogsScanExec::partition_statistics` reports `num_rows =
//! Precision::Exact(sum of SegmentRef::sample_count)` for a predicate-free,
//! erasure-free scan, so DataFusion's `AggregateStatistics` physical-optimizer
//! rule rewrites `COUNT(*)` into a literal and never executes the scan. These
//! tests pin both halves: the fast path (no `LogsScanExec` in the plan, zero
//! object-store GETs) and the fail-closed cases (any pushed predicate or any
//! pending erasure keeps the scan and answers by scanning).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{Statistics, collect, displayable};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    LogsTableProvider, SessionTable, SpanSegmentFetcher, SqlConfig, SqlExecutor,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

use futures::StreamExt;

mod util;
use util::CountingStore;

const TENANT: TenantHash = TenantHash([7u8; 16]);

fn identity() -> ObjectIdentity {
    // The RLOG read path enforces a footer tenant_hash check, so the footer
    // must name the same tenant the fetch uses.
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 3 records so the small test objects have several blocks
/// and a scanning count has real work to do.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A record on the single-`service.name` stream `api`, carrying the given
/// per-record dynamic attrs.
fn record_with_attrs(ts: i64, body: &str, attrs: &[(String, AttrValue)]) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
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
        attrs: attrs.to_vec(),
    }
}

fn record(ts: i64, body: &str) -> LogRecord {
    record_with_attrs(ts, body, &[])
}

/// `n` records at `base..base+n`, bodies `body <ts>`.
fn seg_records(base: i64, n: i64) -> Vec<LogRecord> {
    (0..n)
        .map(|i| record(base + i, &format!("body {}", base + i)))
        .collect()
}

/// Write one RLOG object from `records`, put it at `key`, and return a matching
/// L0 [`SegmentRef`] whose `sample_count` is the object's true record count.
async fn write_object(
    store: &dyn ObjectStoreBackend,
    key: &str,
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
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

fn snapshot_of(
    segments: Vec<SegmentRef>,
    pending_erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> Snapshot {
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure,
    }
}

/// The single scalar of a `COUNT(*)` result.
fn count_from_batches(batches: &[RecordBatch]) -> i64 {
    let mut out = 0i64;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count column");
        out = arr.value(0);
    }
    out
}

/// A logs session over `provider`, built exactly as [`SqlExecutor`] builds one
/// (`build_session`, so the default physical optimizer, including
/// `AggregateStatistics`, is in force).
fn logs_session(provider: LogsTableProvider) -> datafusion::error::Result<SessionContext> {
    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    build_session(&config, pool, SessionTable::Logs(Arc::new(provider)), false, false)
}

fn provider_over(store: Arc<CountingStore>, snapshot: Snapshot) -> LogsTableProvider {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    LogsTableProvider::new(
        snapshot,
        TENANT,
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    )
}

/// Deliverable 2: a predicate-free `SELECT COUNT(*) FROM logs` planned and run
/// through the real `SqlExecutor` answers from the catalog's row counts. The
/// physical plan contains no `LogsScanExec` and the executor issues zero
/// object-store GETs.
///
/// Three objects with 7, 11, and 13 records: the answer is exactly 31.
///
/// Pre-fix (with `LogsScanExec::partition_statistics` reverted to the trait
/// default that reports `num_rows: Absent`): the plan contains `LogsScanExec`
/// and `store.gets()` is 3 (one GET per object). Post-fix: no `LogsScanExec`
/// and `store.gets()` is 0.
#[tokio::test]
async fn predicate_free_count_answered_from_catalog_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));

    let s1 = write_object(&*store, "logs/o1.rlog", &seg_records(100, 7)).await;
    let s2 = write_object(&*store, "logs/o2.rlog", &seg_records(200, 11)).await;
    let s3 = write_object(&*store, "logs/o3.rlog", &seg_records(300, 13)).await;
    let snapshot = snapshot_of(vec![s1, s2, s3], Vec::new());

    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig::default(),
        1 << 30,
    );

    let accounting = QueryAccounting::new();
    let pinned = executor
        .plan_pinned(
            TENANT,
            snapshot,
            "SELECT COUNT(*) FROM logs",
            &accounting,
            &[],
        )
        .await
        .expect("plan");

    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !plan_str.contains("LogsScanExec"),
        "predicate-free COUNT(*) must not scan; plan was:\n{plan_str}"
    );

    let mut stream = pinned.execute().await.expect("execute");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    assert_eq!(count_from_batches(&batches), 31, "7 + 11 + 13");
    assert_eq!(store.gets(), 0, "the count answer must read no objects");
}

/// Fail closed on a pushed ts bound: `COUNT(*) ... WHERE ts >= <inside object
/// 2>` keeps the scan and answers by scanning.
///
/// Objects at ts 100..106, 200..210, 300..312. `ts >= 205` selects 6 rows from
/// object 2 and all 13 from object 3: exactly 19.
#[tokio::test]
async fn count_with_ts_bound_scans() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let s1 = write_object(&*store, "logs/o1.rlog", &seg_records(100, 7)).await;
    let s2 = write_object(&*store, "logs/o2.rlog", &seg_records(200, 11)).await;
    let s3 = write_object(&*store, "logs/o3.rlog", &seg_records(300, 13)).await;
    let snapshot = snapshot_of(vec![s1, s2, s3], Vec::new());

    let ctx = logs_session(provider_over(Arc::clone(&store), snapshot)).expect("session");
    let df = ctx
        .table("logs")
        .await
        .expect("table")
        .filter(col("ts").gt_eq(ts_lit(205)))
        .expect("filter")
        .aggregate(vec![], vec![count(lit(1)).alias("c")])
        .expect("aggregate");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("LogsScanExec"),
        "a ts bound must fail closed to a scan; plan was:\n{plan_str}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_from_batches(&batches), 19, "6 in [205,210] + 13");
    assert!(store.gets() > 0, "a scanning count must read objects");
}

/// Fail closed on a content predicate: `COUNT(*) ... WHERE has_word(body, ...)`
/// keeps the scan and answers by scanning. Two of four bodies match.
#[tokio::test]
async fn count_with_content_predicate_scans() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let records = vec![
        record(1, "needle here"),
        record(2, "nothing"),
        record(3, "a needle b"),
        record(4, "nope"),
    ];
    let seg = write_object(&*store, "logs/c.rlog", &records).await;
    let snapshot = snapshot_of(vec![seg], Vec::new());

    let ctx = logs_session(provider_over(Arc::clone(&store), snapshot)).expect("session");
    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE has_word(body, 'needle')")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("LogsScanExec"),
        "a content predicate must fail closed to a scan; plan was:\n{plan_str}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(
        count_from_batches(&batches),
        2,
        "two bodies token-contain 'needle'"
    );
    assert!(store.gets() > 0, "a scanning count must read objects");
}

/// Fail closed on a pending selective erasure (ADR-0064): the committed row
/// counts still include rows the erasure removes, so the scan runs and answers
/// with the erasure-filtered count. Five records, two carry `purge = yes`; the
/// erasure removes those two, leaving 3.
#[tokio::test]
async fn count_with_pending_erasure_scans() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let purge = || vec![("purge".to_string(), AttrValue::Str("yes".to_string()))];
    let records = vec![
        record_with_attrs(1, "a", &purge()),
        record(2, "b"),
        record(3, "c"),
        record_with_attrs(4, "d", &purge()),
        record(5, "e"),
    ];
    let seg = write_object(&*store, "logs/e.rlog", &records).await;

    let erasure = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "purge".to_string(),
            value: "yes".to_string(),
        }],
        ..Default::default()
    };
    let snapshot = snapshot_of(vec![seg], vec![erasure]);

    let ctx = logs_session(provider_over(Arc::clone(&store), snapshot)).expect("session");
    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("LogsScanExec"),
        "a pending erasure must fail closed to a scan; plan was:\n{plan_str}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_from_batches(&batches), 3, "5 records minus 2 erased");
    assert!(store.gets() > 0, "a scanning count must read objects");
}

// ---------------------------------------------------------------------------
// Issue #723: widen the exact-stats condition so a ts bound that fully contains
// every resolved segment still reports an exact row count and an exact ts span.
//
// These exercise `LogsScanExec::partition_statistics(None)` directly, over the
// bare scan `LogsTableProvider::plan_filters` builds. That is the entry point
// DataFusion's `AggregateStatistics` rule consults, and it needs no object-store
// I/O, so a `CountingStore` pins exactly zero GETs. The three predicate-fallback
// tests above still cover the whole-plan rewrite for the no-predicate case.

const NON_TS_COLS: &[usize] = &[ravel_sql::LOG_COL_OBSERVED_TS, ravel_sql::LOG_COL_BODY];

/// Build the bare scan for `filters` over `snapshot` and return its whole-plan
/// statistics. `plan_filters` returns a `LogsScanExec` with no residual filter,
/// so the reported stats are the scan's own.
fn scan_stats(store: &Arc<CountingStore>, snapshot: Snapshot, filters: &[Expr]) -> Arc<Statistics> {
    let provider = provider_over(Arc::clone(store), snapshot);
    let plan = provider.plan_filters(4, filters).expect("plan_filters");
    plan.partition_statistics(None)
        .expect("partition_statistics")
}

fn ts_ns(v: i64) -> Precision<ScalarValue> {
    Precision::Exact(ScalarValue::TimestampNanosecond(Some(v), None))
}

/// Three objects at ts 100..106, 200..210, 300..312 (7 + 11 + 13 = 31 records).
async fn three_objects(store: &Arc<CountingStore>) -> Snapshot {
    let s1 = write_object(&**store, "logs/o1.rlog", &seg_records(100, 7)).await;
    let s2 = write_object(&**store, "logs/o2.rlog", &seg_records(200, 11)).await;
    let s3 = write_object(&**store, "logs/o3.rlog", &seg_records(300, 13)).await;
    snapshot_of(vec![s1, s2, s3], Vec::new())
}

/// Deliverables 1 and 2: a ts bound that strictly contains every segment
/// (`ts BETWEEN 50 AND 400`, segments span 100..312) reports an `Exact` count
/// of 31 and an `Exact` ts span of `[100, 312]`, with zero object-store GETs.
/// Every non-ts column's stats stay `Absent`.
///
/// Pre-fix (`stats_are_exact` reverted to `count_is_exact`'s
/// `self.ts_min == i64::MIN && self.ts_max == i64::MAX`): a present ts bound
/// makes the condition false, so `num_rows` and the ts column are both `Absent`
/// and both assertions below fail.
#[tokio::test]
async fn contained_ts_bound_reports_exact_count_and_span_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let filters = vec![
        col("ts")
            .gt_eq(ts_lit(50))
            .and(col("ts").lt_eq(ts_lit(400))),
    ];
    let stats = scan_stats(&store, snapshot, &filters);

    assert_eq!(stats.num_rows, Precision::Exact(31), "7 + 11 + 13");
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].min_value,
        ts_ns(100),
        "ts min is the smallest segment min_event_ts_ns"
    );
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].max_value,
        ts_ns(312),
        "ts max is the largest segment max_event_ts_ns"
    );
    for &c in NON_TS_COLS {
        assert_eq!(
            stats.column_statistics[c].min_value,
            Precision::Absent,
            "non-ts column {c} min stays Absent"
        );
        assert_eq!(
            stats.column_statistics[c].max_value,
            Precision::Absent,
            "non-ts column {c} max stays Absent"
        );
    }
    assert_eq!(store.gets(), 0, "answering from stats reads no objects");
}

/// Boundary case for deliverable 1's inclusive containment: a bound whose edges
/// exactly equal the extreme segment `min_event_ts_ns`/`max_event_ts_ns`
/// (`ts BETWEEN 100 AND 312`) still counts as fully contained, so the count and
/// span are still `Exact`.
#[tokio::test]
async fn ts_bound_touching_segment_edges_is_contained() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let filters = vec![
        col("ts")
            .gt_eq(ts_lit(100))
            .and(col("ts").lt_eq(ts_lit(312))),
    ];
    let stats = scan_stats(&store, snapshot, &filters);

    assert_eq!(stats.num_rows, Precision::Exact(31));
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].min_value,
        ts_ns(100)
    );
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].max_value,
        ts_ns(312)
    );
    assert_eq!(store.gets(), 0);
}

/// Deliverable 3: a bound that clips even one segment (`ts BETWEEN 100 AND 305`,
/// which excludes ts 306..312 of the third segment) must fail closed. Both the
/// count and the ts span stay `Absent` so the plan falls back to a real scan;
/// this must never regress into an inexact count. Reading the clipped segment's
/// real count from its block index is issue #721, not attempted here.
#[tokio::test]
async fn clipping_ts_bound_reports_absent() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let filters = vec![
        col("ts")
            .gt_eq(ts_lit(100))
            .and(col("ts").lt_eq(ts_lit(305))),
    ];
    let stats = scan_stats(&store, snapshot, &filters);

    assert_eq!(
        stats.num_rows,
        Precision::Absent,
        "a clipped segment makes the summed count an overcount"
    );
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].min_value,
        Precision::Absent,
        "the ts span is unknown when a segment is clipped"
    );
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].max_value,
        Precision::Absent
    );
    assert_eq!(store.gets(), 0, "computing statistics reads no objects");
}

// ---------------------------------------------------------------------------
// Issue #733: the caller-side half that makes #723's leaf statistic reachable.
// `LogsTableProvider::supports_filters_pushdown` reports a filter that resolves
// purely to a `ts` bound (or a `has_word` call) as `Exact`, so no `FilterExec`
// survives above the scan to report its own non-exact statistics in place of
// the leaf's. The two tests below pin the whole physical-plan rewrite, which
// #723's own checkpoint review named as the missing coverage.

/// A contained `ts` bound answers `COUNT(*)` from the catalog with no scan at
/// all: `AggregateStatistics` rewrites the aggregate into a literal because
/// nothing sits between it and `LogsScanExec`'s `Exact` leaf count. So the
/// physical plan holds neither a `FilterExec` nor a `LogsScanExec`, and the
/// query reads zero objects.
///
/// `ts BETWEEN 50 AND 400` (ns) fully contains the three segments' 100..312
/// span, which is exactly #723's `contained_ts_bound_reports_exact_count_and_\
/// span_with_zero_gets` fixture.
///
/// Pre-fix (with `supports_filters_pushdown` reverted to
/// `Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])`): the plan
/// contains a real `FilterExec` over `LogsScanExec` and `store.gets()` is 3.
#[tokio::test]
async fn contained_ts_bound_count_needs_no_filter_and_no_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let ctx = logs_session(provider_over(Arc::clone(&store), snapshot)).expect("session");
    let plan = ctx
        .sql(
            "SELECT COUNT(*) FROM logs \
             WHERE ts BETWEEN TIMESTAMP '1970-01-01 00:00:00.000000050' \
             AND TIMESTAMP '1970-01-01 00:00:00.000000400'",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !plan_str.contains("FilterExec"),
        "an exactly-pushed ts bound must leave no residual filter; plan was:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("LogsScanExec"),
        "the count must be answered from statistics, not scanned; plan was:\n{plan_str}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_from_batches(&batches), 31, "7 + 11 + 13");
    assert_eq!(store.gets(), 0, "the count answer must read no objects");
}

/// The fix must not over-widen to the prune-only channel. The SAME contained
/// `ts` bound, AND-ed with an `attrs['k'] = 'v'` equality, keeps a real
/// `FilterExec` (the equality is `Inexact`: the reader uses a prune arm for
/// block pruning only and never evaluates it per row) and a real
/// `LogsScanExec`, and reads objects.
///
/// Every record carries the resource attribute `service.name = api`, so the
/// answer is still 31: the surviving residual changes the plan, not the rows.
#[tokio::test]
async fn ts_bound_with_an_attrs_equality_keeps_the_filter_and_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let ctx = logs_session(provider_over(Arc::clone(&store), snapshot)).expect("session");
    let plan = ctx
        .sql(
            "SELECT COUNT(*) FROM logs \
             WHERE ts BETWEEN TIMESTAMP '1970-01-01 00:00:00.000000050' \
             AND TIMESTAMP '1970-01-01 00:00:00.000000400' \
             AND attrs['service.name'] = 'api'",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("FilterExec"),
        "the prune-only attrs equality must stay Inexact and survive as a \
         residual; plan was:\n{plan_str}"
    );
    assert!(
        plan_str.contains("LogsScanExec"),
        "a prune-channel predicate must fail closed to a scan; plan was:\n{plan_str}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(
        count_from_batches(&batches),
        31,
        "every record carries service.name = api"
    );
    assert!(store.gets() > 0, "a scanning count must read objects");
}

/// Regression guard on the trivial instance (no ts bound at all): the widened
/// condition must still report the exact count, and now also the exact ts span,
/// since `[i64::MIN, i64::MAX]` contains every segment.
#[tokio::test]
async fn no_ts_bound_reports_exact_count_and_span() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = three_objects(&store).await;

    let stats = scan_stats(&store, snapshot, &[]);

    assert_eq!(stats.num_rows, Precision::Exact(31));
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].min_value,
        ts_ns(100)
    );
    assert_eq!(
        stats.column_statistics[ravel_sql::LOG_COL_TS].max_value,
        ts_ns(312)
    );
    assert_eq!(store.gets(), 0);
}
