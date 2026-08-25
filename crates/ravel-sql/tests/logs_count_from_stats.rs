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
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::physical_plan::{collect, displayable};
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
    build_session(&config, pool, SessionTable::Logs(Arc::new(provider)), false)
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
