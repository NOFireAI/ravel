//! A `LIKE` scan under a `LIMIT` with no `ORDER BY` skips the whole-segment
//! fast path's serve-if-fits verdict (ADR-3326, serve-if-fits amendment).
//!
//! A `LIKE` is never exact, so a `FilterExec` always sits above its scan and
//! DataFusion's limit pushdown installs the fetch there, not on the scan. The
//! `LogsLimitHint` rule copies that fetch onto the scan as a limit hint, which
//! never truncates, and a scan carrying one with no confirmed TopK starts its
//! opens without awaiting the verdict's directory reads. Every fixture here
//! has [`OBJECTS`] segments, as many as the read-cache executor's partitions,
//! so each scan takes the fast path, and a decoded cache, so a `Contains`
//! scan without the hint makes the verdict, cold, with one plan-phase
//! directory read per segment.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::decoded_cache::DecodedColumnCache;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{CacheFetchError, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlOutcome,
    SqlRequest, StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccountingSnapshot;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// Segments in every fixture: the read-cache executor's 8 partitions, so the
/// whole-segment fast path takes each scan.
const OBJECTS: u64 = 8;
const ROWS_PER_BLOCK: usize = 6;
const BLOCKS_PER_OBJECT: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_OBJECT;

/// The statement the hint exists for.
const PLAIN_SQL: &str = "SELECT ts, c FROM logs WHERE c LIKE '%x%' LIMIT 3";
/// [`PLAIN_SQL`] with a second `LIKE` on the fixed `body` column, which the
/// scan does not evaluate: only the `FilterExec` drops rows it fails.
const SPARSE_SQL: &str =
    "SELECT ts, c, body FROM logs WHERE c LIKE '%x%' AND body LIKE '%tail%' LIMIT 3";
/// The same filter under a TopK: keeps its verdict.
const TOPK_SQL: &str = "SELECT ts, c FROM logs WHERE c LIKE '%x%' ORDER BY ts LIMIT 3";
/// A filter with a fetch under a TopK: the inner `LIMIT` puts a fetch on the
/// `LIKE` filter, and the outer `ORDER BY ts LIMIT` is a TopK above it.
const NESTED_TOPK_SQL: &str =
    "SELECT ts, c FROM (SELECT ts, c FROM logs WHERE c LIKE '%x%' LIMIT 5) ORDER BY ts LIMIT 3";
/// A filter with a fetch (the `HAVING`) over an aggregate over the `LIKE`
/// filter and its scan: the aggregate is not a pass-through node.
const AGGREGATE_SQL: &str =
    "SELECT d, COUNT(*) AS k FROM logs WHERE c LIKE '%x%' GROUP BY d HAVING COUNT(*) > 0 LIMIT 1";

fn tenant() -> TenantId {
    TenantId::new("limit-hint-3740".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("d", DeclaredType::Str),
        DeclaredColumn::new("c", DeclaredType::Str),
    ]
}

/// Row `i` at `ts`: `c` matches `LIKE '%x%'` when `row.c`, and `body` matches
/// `LIKE '%tail%'` when `row.tail`.
fn record(ts: i64, i: usize, row: Row) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let c = if row.c {
        format!("has-x-{ts}")
    } else {
        format!("none-{ts}")
    };
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: if row.tail {
            format!("tail at {ts}")
        } else {
            format!("row at {ts}")
        },
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            (
                "d".to_string(),
                AttrValue::Str(["red", "blue"][i % 2].to_string()),
            ),
            ("c".to_string(), AttrValue::Str(c)),
        ],
    }
}

/// Write object `seq`, whose row `i` is `rows(seq, i)`, and publish its
/// commit record.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    rows: fn(u64, usize) -> Row,
) {
    let base = seq as i64 * 1000;
    let records: Vec<LogRecord> = (0..ROWS_PER_OBJECT)
        .map(|i| record(base + i as i64 + 1, i, rows(seq, i)))
        .collect();
    let cfg = RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(3_740),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// Which of a row's two filterable values match.
#[derive(Clone, Copy)]
struct Row {
    c: bool,
    tail: bool,
}

/// Every third row of every object matches `c LIKE '%x%'`.
fn dense(_seq: u64, i: usize) -> Row {
    Row {
        c: i.is_multiple_of(3),
        tail: false,
    }
}

/// Every row matches `c LIKE '%x%'`, which the scan applies itself through its
/// `Contains` arm, and only the last two rows of each of the last two objects
/// match `body LIKE '%tail%'`, which only the `FilterExec` applies. No
/// partition's first 3 rows survive the filter, so a scan that stopped each
/// partition at 3 rows would return none.
fn sparse(seq: u64, i: usize) -> Row {
    Row {
        c: true,
        tail: seq > OBJECTS - 2 && i >= ROWS_PER_OBJECT - 2,
    }
}

async fn store_with(rows: fn(u64, usize) -> Row) -> Arc<dyn ObjectStoreBackend> {
    let t = tenant();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem;
    for seq in 1..=OBJECTS {
        publish_object(store.as_ref(), &t, seq, rows).await;
    }
    store
}

/// An executor whose logs fetcher carries a read cache (so its scans run the
/// default 8 partitions and take the fast path over [`OBJECTS`] segments) and
/// a fresh 1 GiB decoded cache (so a `Contains` scan makes the verdict).
fn executor(store: &Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    let bytes = 64 << 20;
    let logs = LogSegmentFetcher::new(Arc::clone(store))
        .with_cache(Arc::new(Cache::<CacheFetchError>::new(CacheLimits::new(
            bytes, 16_384, bytes,
        ))))
        .with_decoded_cache(Arc::new(DecodedColumnCache::new(
            1 << 30,
            Some(Arc::new(MemoryBudget::unlimited())),
        )));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        logs,
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The plan phase's requests plus its read-cache hits: on the fast path,
/// which plans nothing, the segments whose directories the verdict read.
fn plan_directory_reads(outcome: &SqlOutcome) -> u64 {
    let plan: &QueryAccountingSnapshot = &outcome.phase_accounting.plan;
    plan.s3_requests.iter().sum::<u64>() + plan.cache_hits
}

/// Every value of column `c` in the result.
fn c_values(outcome: &SqlOutcome) -> Vec<String> {
    column_values(outcome, "c")
}

/// Every value of string column `name` in the result.
fn column_values(outcome: &SqlOutcome, name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        let idx = batch.schema().index_of(name).expect("column projected");
        let col = cast(batch.column(idx), &DataType::Utf8).expect("casts to Utf8");
        let col = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8");
        for row in 0..col.len() {
            out.push(col.value(row).to_string());
        }
    }
    out
}

async fn run(store: &Arc<dyn ObjectStoreBackend>, sql: &str) -> SqlOutcome {
    executor(store)
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("statement executes")
}

async fn plan_text(store: &Arc<dyn ObjectStoreBackend>, sql: &str) -> String {
    executor(store)
        .explain(tenant().hash(), &request(sql))
        .await
        .expect("statement explains")
        .plan_text
}

/// The scan line of `plan`.
fn scan_line(plan: &str) -> &str {
    let lines: Vec<&str> = plan
        .lines()
        .filter(|l| l.contains("LogsScanExec:"))
        .collect();
    assert_eq!(lines.len(), 1, "one logs scan:\n{plan}");
    lines[0]
}

/// The plan of [`PLAIN_SQL`] carries `limit_hint=3` on its scan and nowhere
/// else, and the scan has no fetch; none of the other statements' scans
/// carries a hint.
#[tokio::test]
async fn only_a_filter_fetch_over_pass_through_nodes_installs_the_hint() {
    let store = store_with(dense).await;
    let plain = plan_text(&store, PLAIN_SQL).await;
    let scan = scan_line(&plain);
    assert!(scan.contains(", limit_hint=3"), "{plain}");
    assert!(!scan.contains("fetch"), "{plain}");
    assert_eq!(plain.matches("limit_hint=").count(), 1, "{plain}");
    for sql in [TOPK_SQL, NESTED_TOPK_SQL, AGGREGATE_SQL] {
        let plan = plan_text(&store, sql).await;
        assert!(!plan.contains("limit_hint"), "{sql}:\n{plan}");
    }
}

/// [`PLAIN_SQL`] over the dense fixture: the verdict is skipped, so the plan
/// phase reads no directory, and the result is exactly 3 matching rows.
///
/// [`TOPK_SQL`] on the same fixture keeps its verdict: one directory read per
/// segment, 8, the count main reads for it.
#[tokio::test]
async fn a_like_limit_scan_skips_the_verdict_and_a_topk_keeps_it() {
    let store = store_with(dense).await;
    let plain = run(&store, PLAIN_SQL).await;
    assert_eq!(plan_directory_reads(&plain), 0, "no verdict read");
    let c = c_values(&plain);
    assert_eq!(c.len(), 3, "exactly the limit: {c:?}");
    assert!(c.iter().all(|v| v.contains('x')), "{c:?}");

    let topk = run(&store, TOPK_SQL).await;
    assert_eq!(plan_directory_reads(&topk), OBJECTS, "the verdict reads");
    assert_eq!(
        c_values(&topk),
        vec!["has-x-1001", "has-x-1004", "has-x-1007"],
        "the first three matches by ts"
    );
}

/// [`SPARSE_SQL`] over the sparse fixture, where the filter drops all but 4
/// of the scan's 192 rows and no partition's first 3 rows survive it: the
/// scan carries `limit_hint=3` and makes no verdict read, and the result is
/// still exactly 3 matching rows, because the hint does not truncate the scan.
#[tokio::test]
async fn the_hint_does_not_truncate_a_scan_whose_filter_drops_most_rows() {
    let store = store_with(sparse).await;
    let plan = plan_text(&store, SPARSE_SQL).await;
    assert!(scan_line(&plan).contains(", limit_hint=3"), "{plan}");
    let sparse = run(&store, SPARSE_SQL).await;
    assert_eq!(plan_directory_reads(&sparse), 0, "no verdict read");
    let c = c_values(&sparse);
    assert_eq!(c.len(), 3, "exactly the limit: {c:?}");
    assert!(c.iter().all(|v| v.contains('x')), "{c:?}");
    let body = column_values(&sparse, "body");
    assert_eq!(body.len(), 3, "{body:?}");
    assert!(body.iter().all(|v| v.starts_with("tail at ")), "{body:?}");
}

/// A filter with a fetch under a TopK ([`NESTED_TOPK_SQL`]) and one over an
/// aggregate ([`AGGREGATE_SQL`]) install no hint, so each keeps its verdict:
/// one directory read per segment.
#[tokio::test]
async fn a_topk_or_an_aggregate_above_the_scan_keeps_the_verdict() {
    let store = store_with(dense).await;
    let nested = run(&store, NESTED_TOPK_SQL).await;
    assert_eq!(plan_directory_reads(&nested), OBJECTS, "{NESTED_TOPK_SQL}");
    assert_eq!(c_values(&nested).len(), 3, "{NESTED_TOPK_SQL}");
    let aggregate = run(&store, AGGREGATE_SQL).await;
    assert_eq!(plan_directory_reads(&aggregate), OBJECTS, "{AGGREGATE_SQL}");
    assert_eq!(aggregate.output.num_rows(), 1, "{AGGREGATE_SQL}");
}
