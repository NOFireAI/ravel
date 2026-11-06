//! Checked integer arithmetic (issue #3847) end to end, through a real
//! [`ravel_sql::SqlExecutor`] over a published logs object whose `a` and `b`
//! attributes are declared `i64` columns.
//!
//! - `every_rewritten_exec_fails_on_overflow`: with
//!   `SqlConfig::checked_integer_arithmetic` on, each statement over a row
//!   holding `i64::MAX` fails with `SqlError::IntegerArithmeticOverflow`, one
//!   statement per exec kind the rule rewrites; with it off the same
//!   statements succeed.
//! - `in_range_results_are_identical_on_and_off`: over in-range rows every
//!   statement returns the same result with the option on and off.
//! - `unary_minus_wraps_and_abs_fails_in_both_modes`: unary minus and `abs`
//!   on `i64::MIN` are outside the rule (they are not `BinaryExpr`s). In both
//!   modes `-a` wraps to `i64::MIN` and `abs(a)` fails with the generic
//!   execution error arrow's own check raises, not
//!   `IntegerArithmeticOverflow`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::util::pretty::pretty_format_batches;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, ErrorClass, SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor,
    SqlRequest, StaticDeclaredColumns,
};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// One statement per exec kind the rule rewrites, in the issue's order,
/// plus the nested forms. Each overflows on the row `(a, b) = (i64::MAX,
/// -1)`, and each orders its output so results compare row by row.
const STATEMENTS: &[(&str, &str)] = &[
    ("projection", "SELECT a + 1 AS v FROM logs ORDER BY ts"),
    ("filter", "SELECT ts FROM logs WHERE a + 1 > 0 ORDER BY ts"),
    ("aggregate argument", "SELECT SUM(a * 2) AS v FROM logs"),
    (
        "group key",
        "SELECT a + 1 AS k, COUNT(*) AS n FROM logs GROUP BY a + 1 ORDER BY k",
    ),
    ("sort key", "SELECT ts FROM logs ORDER BY a - b, ts"),
    (
        "window order key",
        "SELECT ts, SUM(a) OVER (ORDER BY a + 1) AS v FROM logs ORDER BY ts",
    ),
    (
        "window argument",
        "SELECT ts, SUM(a + 1) OVER (ORDER BY ts) AS v FROM logs ORDER BY ts",
    ),
    (
        "having",
        "SELECT body, COUNT(*) AS n FROM logs GROUP BY body HAVING SUM(a) + 1 > 0 ORDER BY body",
    ),
    (
        "aggregate filter",
        "SELECT COUNT(*) FILTER (WHERE a + 1 > 0) AS n FROM logs",
    ),
    (
        "nested, outer wraps back",
        "SELECT (a + 1) - b AS v FROM logs ORDER BY ts",
    ),
    (
        "nested, outer overflows",
        "SELECT (a + 1) * 2 AS v FROM logs ORDER BY ts",
    ),
];

fn identity(tenant_hash: [u8; 16]) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64, body: &str, a: i64, b: i64) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
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
        attrs: vec![
            ("a".to_string(), AttrValue::I64(a)),
            ("b".to_string(), AttrValue::I64(b)),
        ],
    }
}

/// Write one RLOG object from `records` and publish its logs commit record.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
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
        writer_id: Uuid::from_u128(3_847),
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

fn executor(store: &Arc<dyn ObjectStoreBackend>, checked: bool) -> SqlExecutor {
    let catalog = Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("cat"));
    let config = SqlConfig {
        checked_integer_arithmetic: checked,
        ..SqlConfig::default()
    };
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        config,
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("b", DeclaredType::I64),
    ])))
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

async fn tenant_with(name: &str, rows: &[LogRecord]) -> (Arc<dyn ObjectStoreBackend>, TenantId) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(name.to_string());
    publish_logs(store.as_ref(), &tenant, rows).await;
    (store, tenant)
}

async fn run(executor: &SqlExecutor, tenant: &TenantId, sql: &str) -> Result<String, SqlError> {
    let outcome = executor.execute(tenant.hash(), &request(sql)).await?;
    Ok(pretty_format_batches(outcome.output.batches())
        .expect("format")
        .to_string())
}

#[tokio::test]
async fn every_rewritten_exec_fails_on_overflow() {
    let (store, tenant) = tenant_with(
        "checked-arith-overflow",
        &[record(1, "x", i64::MAX, -1), record(2, "x", 0, 0)],
    )
    .await;
    let on = executor(&store, true);
    let off = executor(&store, false);
    for (exec, sql) in STATEMENTS {
        let err = run(&on, &tenant, sql)
            .await
            .expect_err(&format!("{exec}: on must fail: {sql}"));
        assert!(
            matches!(err, SqlError::IntegerArithmeticOverflow),
            "{exec}: {sql}: {err:?}"
        );
        run(&off, &tenant, sql)
            .await
            .unwrap_or_else(|e| panic!("{exec}: off must wrap, not fail: {sql}: {e:?}"));
    }
}

/// The error is the 422 class `IntegerSumOverflow` has (Flight SQL maps that
/// class to `FAILED_PRECONDITION`), and its client text names no value.
#[test]
fn the_overflow_error_is_a_valueless_unsupported_class_error() {
    let err = SqlError::IntegerArithmeticOverflow;
    let sum = SqlError::IntegerSumOverflow {
        result_type: "Int64",
    };
    assert_eq!(err.class(), ErrorClass::Unsupported);
    assert_eq!(err.class(), sum.class());
    let text = err.client_message();
    assert_eq!(text, err.to_string());
    assert!(!text.chars().any(|c| c.is_ascii_digit()), "{text}");
}

#[tokio::test]
async fn in_range_results_are_identical_on_and_off() {
    let (store, tenant) = tenant_with(
        "checked-arith-small",
        &[
            record(1, "x", 3, 2),
            record(2, "y", -4, 5),
            record(3, "x", 10, -7),
        ],
    )
    .await;
    let on = executor(&store, true);
    let off = executor(&store, false);
    for (exec, sql) in STATEMENTS {
        let off_text = run(&off, &tenant, sql).await.expect("off");
        let on_text = run(&on, &tenant, sql).await.expect("on");
        assert_eq!(off_text, on_text, "{exec}: {sql}");
    }
}

async fn single_i64(
    executor: &SqlExecutor,
    tenant: &TenantId,
    sql: &str,
) -> Result<Option<i64>, SqlError> {
    let outcome = executor.execute(tenant.hash(), &request(sql)).await?;
    let batch = outcome
        .output
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one row");
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64");
    Ok((!column.is_null(0)).then(|| column.value(0)))
}

#[tokio::test]
async fn unary_minus_wraps_and_abs_fails_in_both_modes() {
    let (store, tenant) = tenant_with("checked-arith-min", &[record(1, "x", i64::MIN, 0)]).await;
    for checked in [false, true] {
        let executor = executor(&store, checked);
        let minus = single_i64(&executor, &tenant, "SELECT -a AS v FROM logs").await;
        assert!(
            matches!(minus, Ok(Some(i64::MIN))),
            "checked={checked}: -a = {minus:?}"
        );
        let abs = single_i64(&executor, &tenant, "SELECT abs(a) AS v FROM logs").await;
        assert!(
            matches!(abs, Err(SqlError::Execution(ref m)) if m.contains("overflow on abs")),
            "checked={checked}: abs(a) = {abs:?}"
        );
    }
}
