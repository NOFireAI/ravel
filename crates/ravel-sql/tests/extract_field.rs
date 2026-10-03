//! Integration test for `DatetimeFunctionPlanner` (issue #2458) against the
//! `logs` table: `EXTRACT(field FROM expr)` must plan and return the exact
//! same rows as the already-working `date_part('field', expr)` call it
//! rewrites to. `crates/ravel-sql/tests/parquet_tables.rs` carries the
//! equivalent Parquet-table coverage, plus ClickBench Q19's exact text.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::util::display::array_value_to_string;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlOutcome, SqlRequest};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

fn tenant() -> TenantId {
    TenantId::new("extract-field-2458".to_string())
}

fn identity(tenant_hash: [u8; 16]) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64) -> LogRecord {
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
        body: "hit".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Write one RLOG object from `records` and publish its `Signal::Logs` commit
/// record, so a real `Catalog::resolve` finds the segment. Mirrors
/// `logs_declared_columns.rs::publish_logs`.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let writer_id = Uuid::from_u128(24_580);
    let mut w = RlogWriter::new(RlogConfig::default(), identity(tenant.hash().0));
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

fn executor_with(store: Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

fn rows(outcome: &SqlOutcome) -> Vec<String> {
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| array_value_to_string(batch.column(column), row).expect("cell"))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// Three timestamps a second, a minute, and an hour apart, so minute/hour/year
/// extraction is exercised against values that actually move each field.
const TS_NS: [i64; 3] = [
    1_700_000_000_000_000_000,
    1_700_000_060_000_000_000,
    1_700_003_600_000_000_000,
];

async fn executor_over_fixture() -> (TenantId, SqlExecutor) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let t = tenant();
    let records: Vec<LogRecord> = TS_NS.iter().map(|&ts| record(ts)).collect();
    publish_logs(store.as_ref(), &t, &records).await;
    let executor = executor_with(Arc::clone(&store));
    (t, executor)
}

async fn assert_extract_matches_date_part(
    executor: &SqlExecutor,
    tenant_hash: ravel_types::TenantHash,
    field: &str,
) {
    let date_part_sql = format!("SELECT ts, date_part('{field}', ts) FROM logs ORDER BY ts");
    let extract_sql = format!("SELECT ts, EXTRACT({field} FROM ts) FROM logs ORDER BY ts");

    let expected = executor
        .execute(tenant_hash, &request(&date_part_sql))
        .await
        .unwrap_or_else(|e| panic!("{date_part_sql} must plan and execute: {e}"));
    let actual = executor
        .execute(tenant_hash, &request(&extract_sql))
        .await
        .unwrap_or_else(|e| panic!("{extract_sql} must plan and execute: {e}"));

    let expected_rows = rows(&expected);
    let actual_rows = rows(&actual);
    assert_eq!(
        actual_rows, expected_rows,
        "EXTRACT({field} FROM ts) must match date_part('{field}', ts) row for row"
    );
    assert_eq!(
        actual_rows.len(),
        TS_NS.len(),
        "all three records must be returned"
    );
}

/// Issue #2458: `EXTRACT(minute FROM ts)` plans (via `DatetimeFunctionPlanner`,
/// registered in `crate::session::build_session`) into the already-admitted
/// `date_part('minute', ts)` call, and returns identical rows, on the `logs`
/// table -- the same `/api/v1/sql` entry point ClickBench Q19 plans on.
#[tokio::test]
async fn extract_minute_matches_date_part_on_logs_table() {
    let (t, executor) = executor_over_fixture().await;
    assert_extract_matches_date_part(&executor, t.hash(), "minute").await;
}

/// The same equivalence for two fields beyond minute: `hour` and `year`.
#[tokio::test]
async fn extract_hour_and_year_match_date_part_on_logs_table() {
    let (t, executor) = executor_over_fixture().await;
    for field in ["hour", "year"] {
        assert_extract_matches_date_part(&executor, t.hash(), field).await;
    }
}
