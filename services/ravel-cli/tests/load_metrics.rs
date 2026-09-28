//! End-to-end coverage for `ravel-cli load --signal metrics` (ADR-1751
//! decisions 1 and 2, follow-up task 1; issues #1751 and #1712).
//!
//! These drive the loader's real entry points in-process against a shared
//! `MemoryStore` and read the loaded samples back through the real
//! `/api/v1/query_range` handler, for the same reason `tests/load.rs` does it
//! that way: a subprocess against `--store memory` gets its own empty store,
//! so no second process could ever see the first's writes.
//!
//! The read-back goes through the HTTP query handler rather than
//! `QueryEngine::range` directly because the handler renders the result as
//! JSON, which is what lets these assert an exact value and an exact
//! timestamp without `ravel-cli` taking a dependency on the query engine's
//! internal value type.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use parquet::arrow::ArrowWriter;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadError, MetricsLoadReport};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::SystemClock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{CommitToken, TenantId};
use serde_json::Value;
use tower::ServiceExt;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_DAY: i64 = 86_400 * NS_PER_SEC;
const TOKEN: &str = "test-token";

/// Wall clock floored to a whole second, so every derived timestamp is an
/// exact-second value and the step boundary a range query evaluates at lands
/// exactly on the sample's own event time.
///
/// Real wall time, not a pinned constant: the loader buckets by the flush-open
/// clock, and every later query's catalog listing window runs from the query's
/// `start` to `now`. Pinning the load clock to a fixed past instant while the
/// handler reads the system clock would make that window span the years
/// between the two, which is hundreds of thousands of hour probes rather than
/// a test.
fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn f64_col(vals: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(vals))
}

fn opt_f64_col(vals: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(vals))
}

fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

fn metrics_mapping(text: &str) -> load::MetricsMapping {
    load::parse_metrics_mapping(text).expect("valid metrics mapping")
}

/// The real query HTTP surface over the same store the load wrote to.
fn query_app(store: Arc<dyn ObjectStoreBackend>, tenant: &TenantId) -> Router {
    let catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: 1,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog"),
    );
    let engine = Arc::new(QueryEngine::new(catalog, store, EngineConfig::default()));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    router(AppState::new(
        engine,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
    ))
}

/// One `/api/v1/query_range` call at a single step, pinned to `at_ns` so the
/// evaluated step timestamp is the sample's own event time.
async fn query_range_at(
    app: &Router,
    query: &str,
    at_ns: i64,
    min_tokens: &[CommitToken],
) -> Value {
    let secs = at_ns / NS_PER_SEC;
    let mut uri = format!("/api/v1/query_range?query={query}&start={secs}&end={secs}&step=1s");
    for token in min_tokens {
        uri.push_str("&min_commit_token=");
        uri.push_str(&token.encode());
    }
    let request = Request::builder()
        .method("GET")
        .uri(&uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("parse response json");
    assert_eq!(status, StatusCode::OK, "query_range failed: {json}");
    json
}

/// The `data.result` array of a range-query response.
fn range_results(body: &Value) -> Vec<Value> {
    body["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("no data.result array in {body}"))
        .clone()
}

/// The single `(step timestamp ns, value)` pair of a one-step series.
fn single_point(series: &Value) -> (i64, f64) {
    let values = series["values"]
        .as_array()
        .unwrap_or_else(|| panic!("no values array in {series}"));
    assert_eq!(values.len(), 1, "expected exactly one step in {series}");
    let ts_secs = values[0][0]
        .as_f64()
        .unwrap_or_else(|| panic!("step timestamp is not a number in {series}"));
    let raw = values[0][1]
        .as_str()
        .unwrap_or_else(|| panic!("sample value is not a string in {series}"));
    let value = raw
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("sample value {raw:?} does not parse as f64"));
    ((ts_secs * 1e9) as i64, value)
}

/// `(name, le, value)` for one exploded classic-histogram series, with `le`
/// absent on the `_sum` and `_count` series.
fn histogram_series(body: &Value) -> Vec<(String, Option<String>, f64)> {
    let mut out: Vec<(String, Option<String>, f64)> = range_results(body)
        .iter()
        .map(|series| {
            let name = series["metric"]["__name__"]
                .as_str()
                .unwrap_or_else(|| panic!("no __name__ in {series}"))
                .to_string();
            let le = series["metric"]["le"].as_str().map(str::to_string);
            let (_, value) = single_point(series);
            (name, le, value)
        })
        .collect();
    out.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    out
}

/// Acceptance (ADR-1751 follow-up task 1): one thirty-day-old sample loaded
/// from Parquet through the real `load_metrics` entry point into a
/// `MemoryStore` reads back through `query_range` with the exact value and
/// the exact timestamp.
///
/// The thirty-day gap is the point of the test: ADR-0089's past-event-time
/// lag relaxation (widened to every signal by ADR-1751 decision 1) is what
/// admits the sample at all, and the object buckets by the load-time flush
/// clock rather than the event time, which is what keeps it discoverable.
#[tokio::test]
async fn thirty_day_old_metric_sample_loads_and_reads_back_through_query_range() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("metrics.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 30 * NS_PER_DAY;

    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![event_ns])),
        ("value".to_string(), f64_col(vec![1234.5])),
        ("svc".to_string(), str_col(vec!["checkout"])),
    ])
    .expect("record batch");
    write_parquet(&pq, &batch);

    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "ravel_load_demo"
        value_column = "value"
        ts_column = "ts"
        ts_unit = "nanos"
        kind = "gauge"

        [[metrics.label]]
        name = "job"
        column = "svc"
        "#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");
    let report: MetricsLoadReport = load::load_metrics(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect("a thirty-day-old sample is admitted: the past-lag check is relaxed on this path");

    assert_eq!(report.rows_processed, 1, "one source row");
    assert_eq!(report.points_written, 1, "one scalar point");
    assert_eq!(report.objects_written(), 1, "one RSEG object");

    let app = query_app(Arc::clone(&store), &tenant);
    let body = query_range_at(&app, "ravel_load_demo", event_ns, &report.tokens).await;
    let results = range_results(&body);
    assert_eq!(
        results.len(),
        1,
        "exactly one series must come back, got {body}"
    );
    assert_eq!(results[0]["metric"]["__name__"], "ravel_load_demo");
    assert_eq!(results[0]["metric"]["job"], "checkout");
    let (ts_ns, value) = single_point(&results[0]);
    assert_eq!(
        ts_ns, event_ns,
        "the sample reads back at its own thirty-day-old event time, not at load time"
    );
    assert_eq!(
        value.to_bits(),
        1234.5f64.to_bits(),
        "the sample's exact value survives the round trip"
    );
}

/// A classic-histogram row set explodes into exactly the Prometheus
/// convention series `ravel_otlp::normalize`'s `explode_histogram` produces:
/// one cumulative `_bucket` per finite bound, a `+Inf` bucket carrying the
/// data point's own count, `_sum`, and `_count`.
///
/// The row values are the per-bucket counts (2, 3, 1), so the cumulative
/// bucket values must be 2, 5, 6 and the `+Inf` bucket must be the count
/// column's 7, not the accumulated 6: OTLP takes the `+Inf` bucket and
/// `_count` from the point's raw count, and a data point whose buckets do not
/// sum to its count is exactly what distinguishes the two.
#[tokio::test]
async fn classic_histogram_rows_explode_into_exact_bucket_sum_and_count_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("histogram.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;

    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(vec![event_ns, event_ns, event_ns]),
        ),
        ("le".to_string(), f64_col(vec![0.1, 1.0, 10.0])),
        ("bucket_count".to_string(), f64_col(vec![2.0, 3.0, 1.0])),
        ("sum".to_string(), f64_col(vec![12.5, 12.5, 12.5])),
        ("count".to_string(), i64_col(vec![7, 7, 7])),
        ("svc".to_string(), str_col(vec!["api", "api", "api"])),
    ])
    .expect("record batch");
    write_parquet(&pq, &batch);

    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "http_request_duration_seconds"
        value_column = "bucket_count"
        ts_column = "ts"
        ts_unit = "nanos"

        [[metrics.label]]
        name = "job"
        column = "svc"

        [metrics.histogram]
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");
    let report = load::load_metrics(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect("the classic-histogram rows load");

    assert_eq!(report.rows_processed, 3, "three source rows");
    assert_eq!(
        report.histogram_points_exploded, 1,
        "the three rows are one data point"
    );
    assert_eq!(
        report.points_written, 6,
        "three finite bounds explode into 3 buckets + the +Inf bucket + _sum + _count"
    );

    let app = query_app(Arc::clone(&store), &tenant);
    let mut series: Vec<(String, Option<String>, f64)> = Vec::new();
    for name in [
        "http_request_duration_seconds_bucket",
        "http_request_duration_seconds_sum",
        "http_request_duration_seconds_count",
    ] {
        series.extend(histogram_series(
            &query_range_at(&app, name, event_ns, &report.tokens).await,
        ));
    }

    let expected: Vec<(String, Option<String>, f64)> = vec![
        (
            "http_request_duration_seconds_bucket".to_string(),
            Some("+Inf".to_string()),
            7.0,
        ),
        (
            "http_request_duration_seconds_bucket".to_string(),
            Some("0.1".to_string()),
            2.0,
        ),
        (
            "http_request_duration_seconds_bucket".to_string(),
            Some("1".to_string()),
            5.0,
        ),
        (
            "http_request_duration_seconds_bucket".to_string(),
            Some("10".to_string()),
            6.0,
        ),
        ("http_request_duration_seconds_count".to_string(), None, 7.0),
        ("http_request_duration_seconds_sum".to_string(), None, 12.5),
    ];
    series.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    assert_eq!(
        series.len(),
        expected.len(),
        "exactly the exploded series, no more and no fewer"
    );
    for (got, want) in series.iter().zip(&expected) {
        assert_eq!((&got.0, &got.1), (&want.0, &want.1), "series identity");
        assert_eq!(
            got.2.to_bits(),
            want.2.to_bits(),
            "value of {} le={:?}: got {}, want {}",
            got.0,
            got.1,
            got.2,
            want.2
        );
    }

    // Every exploded series carries the mapped label, and none carries a
    // stray `le` on `_sum`/`_count`.
    let body = query_range_at(
        &app,
        "http_request_duration_seconds_sum",
        event_ns,
        &report.tokens,
    )
    .await;
    let results = range_results(&body);
    assert_eq!(results[0]["metric"]["job"], "api");
    assert!(
        results[0]["metric"]["le"].is_null(),
        "_sum carries no le label, got {}",
        results[0]["metric"]
    );
}

/// A mapping whose one signal section does not match `--signal` is refused,
/// naming both (ADR-1751 decision 2).
#[test]
fn mapping_with_the_wrong_section_for_the_signal_is_refused() {
    let logs_section = r#"
        [logs]
        ts_column = "ts"
        ts_unit = "nanos"
        body_column = "message"
        "#;
    let err = load::parse_mapping_document(logs_section, SignalArg::Metrics)
        .expect_err("a [logs] section cannot serve --signal metrics");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("[logs]") && message.contains("metrics"),
        "the refusal names both the section and the signal: {message}"
    );

    let metrics_section = r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"
        "#;
    let err = load::parse_mapping_document(metrics_section, SignalArg::Logs)
        .expect_err("a [metrics] section cannot serve --signal logs");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("[metrics]") && message.contains("logs"),
        "the refusal names both the section and the signal: {message}"
    );

    // Two sections is also a refusal, whichever signal is asked for.
    let both = format!("{logs_section}\n{metrics_section}");
    let err = load::parse_mapping_document(&both, SignalArg::Metrics)
        .expect_err("exactly one section must be present");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("Exactly one"),
        "the refusal states the rule: {message}"
    );
}

/// A mapping that names a native (exponential) histogram is refused, both in
/// the `histogram.type` spelling and as a section of its own (ADR-1751
/// decision 2: native histograms are not mappable in this version).
#[test]
fn mapping_naming_a_native_histogram_is_refused() {
    for text in [
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.histogram]
        type = "native"
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.histogram]
        type = "exponential"
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.native_histogram]
        scale_column = "scale"
        "#,
    ] {
        let err =
            load::parse_metrics_mapping(text).expect_err("a native histogram mapping is refused");
        let LoadError::Setup(message) = err else {
            panic!("expected a setup error");
        };
        assert!(
            message.contains("native"),
            "the refusal names what it refuses: {message}"
        );
    }
}

/// The pre-ADR-1751 logs mapping shape, whose keys sit at the document root
/// with no signal section, still parses as the logs section, and the same
/// mapping wrapped in an explicit `[logs]` section parses identically.
#[test]
fn the_pre_adr_1751_logs_only_mapping_shape_still_parses() {
    let flat = r#"
ts_column = "ts"
ts_unit = "millis"
body_column = "message"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#;
    let flat_mapping = load::parse_mapping(flat).expect("the pre-ADR-1751 shape still parses");
    assert_eq!(flat_mapping.ts_column, "ts");
    assert_eq!(flat_mapping.body_column.as_deref(), Some("message"));
    assert_eq!(flat_mapping.resource_attributes.len(), 1);

    let sectioned = r#"
[logs]
ts_column = "ts"
ts_unit = "millis"
body_column = "message"

[[logs.resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#;
    let sectioned_mapping =
        load::parse_mapping(sectioned).expect("the [logs] section spelling parses");
    assert_eq!(sectioned_mapping.ts_column, flat_mapping.ts_column);
    assert_eq!(sectioned_mapping.body_column, flat_mapping.body_column);
    assert_eq!(
        sectioned_mapping.resource_attributes[0].key,
        flat_mapping.resource_attributes[0].key
    );

    // Mixing the two is refused rather than resolved by an invisible
    // precedence rule.
    let mixed = format!("{flat}\n[metrics]\nname = \"m\"\n");
    let err = load::parse_mapping(&mixed).expect_err("a mixed mapping is refused");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("mixes"),
        "the refusal states what is wrong: {message}"
    );
}

/// `--signal spans` is refused by name and never falls back to another
/// signal (ADR-1751 follow-up task 2 is what adds it).
#[test]
fn spans_signal_is_refused_rather_than_falling_back() {
    let err = load::parse_mapping_document(
        r#"
        [spans]
        trace_id_column = "trace_id"
        "#,
        SignalArg::Spans,
    )
    .expect_err("--signal spans is not supported yet");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("not yet supported") && message.contains("does not fall back"),
        "the refusal says both what is missing and that nothing was written: {message}"
    );
}

/// A histogram whose rows are not contiguous is refused rather than exploded
/// twice: a contiguous-run grouping cannot read interleaved data points, and
/// a second explosion of the same identity would write two conflicting
/// cumulative ladders for one `(series, ts)`.
#[tokio::test]
async fn interleaved_histogram_rows_are_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("interleaved.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;

    // Two data points (job=a, job=b) whose bucket rows alternate.
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(vec![event_ns, event_ns, event_ns, event_ns]),
        ),
        ("le".to_string(), f64_col(vec![0.1, 0.1, 1.0, 1.0])),
        (
            "bucket_count".to_string(),
            f64_col(vec![1.0, 1.0, 1.0, 1.0]),
        ),
        (
            "sum".to_string(),
            opt_f64_col(vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0)]),
        ),
        ("count".to_string(), i64_col(vec![2, 2, 2, 2])),
        ("svc".to_string(), str_col(vec!["a", "b", "a", "b"])),
    ])
    .expect("record batch");
    write_parquet(&pq, &batch);

    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "interleaved"
        value_column = "bucket_count"
        ts_column = "ts"
        ts_unit = "nanos"

        [[metrics.label]]
        name = "job"
        column = "svc"

        [metrics.histogram]
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load::load_metrics(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("interleaved bucket rows are refused");
    let LoadError::RowRejected { reason, .. } = err else {
        panic!("expected a per-row rejection");
    };
    assert!(
        reason.contains("not contiguous"),
        "the rejection names the problem: {reason}"
    );
}
