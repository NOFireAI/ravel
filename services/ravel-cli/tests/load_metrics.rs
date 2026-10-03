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
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.exponential_histogram]
        scale_column = "scale"
        "#,
    ] {
        let err =
            load::parse_metrics_mapping(text).expect_err("a native histogram mapping is refused");
        let LoadError::Setup(message) = err else {
            panic!("expected a setup error");
        };
        // Asserting on the word "native" alone would pass on serde's own
        // unknown-field error, which quotes the offending key and therefore
        // contains "native" too: the section-key cases would then pass
        // whether or not anything refuses them by name. This phrase appears
        // only in the named refusal.
        assert!(
            message.contains("which this version does not map"),
            "the named refusal, not a generic unknown-field error: {message}"
        );
        assert!(
            !message.contains("unknown field"),
            "a native histogram is refused by name, not as a typo: {message}"
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

/// A range query over a span of steps, for the cases that need more than one
/// sample of the same series.
async fn query_range_span(
    app: &Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    min_tokens: &[CommitToken],
) -> Value {
    let start = start_ns / NS_PER_SEC;
    let end = end_ns / NS_PER_SEC;
    let mut uri = format!("/api/v1/query_range?query={query}&start={start}&end={end}&step=1s");
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

/// The `[metrics]` mapping the ladder tests share: the metric name comes from
/// a column, so two adjacent data points can carry different names and each
/// can be queried on its own without a label matcher.
const LADDER_MAPPING: &str = r#"
[metrics]
name_column = "metric"
value_column = "bucket_count"
ts_column = "ts"
ts_unit = "nanos"

[metrics.histogram]
le_column = "le"
sum_column = "sum"
count_column = "count"
"#;

/// Six rows: two three-bucket data points, `ladder_a` on rows 0-2 and
/// `ladder_b` on rows 3-5. `poison_last_row` makes row 5's bucket count
/// fractional, which the loader refuses when it reads that row.
fn ladder_batch(event_ns: i64, poison_last_row: bool) -> RecordBatch {
    let last = if poison_last_row { 6.5 } else { 6.0 };
    RecordBatch::try_from_iter(vec![
        (
            "metric".to_string(),
            str_col(vec![
                "ladder_a", "ladder_a", "ladder_a", "ladder_b", "ladder_b", "ladder_b",
            ]),
        ),
        ("ts".to_string(), i64_col(vec![event_ns; 6])),
        (
            "le".to_string(),
            f64_col(vec![0.1, 1.0, 10.0, 0.1, 1.0, 10.0]),
        ),
        (
            "bucket_count".to_string(),
            f64_col(vec![1.0, 2.0, 3.0, 4.0, 5.0, last]),
        ),
        (
            "sum".to_string(),
            f64_col(vec![6.0, 6.0, 6.0, 21.0, 21.0, 21.0]),
        ),
        ("count".to_string(), i64_col(vec![9, 9, 9, 20, 20, 20])),
    ])
    .expect("record batch")
}

/// `ladder_b`'s exploded series: cumulative 4, 9, 15 over the three bounds,
/// a `+Inf` bucket carrying the data point's own count, `_sum` and `_count`.
fn expected_ladder_b() -> Vec<(String, Option<String>, f64)> {
    let mut want = vec![
        ("ladder_b_bucket", Some("+Inf"), 20.0),
        ("ladder_b_bucket", Some("0.1"), 4.0),
        ("ladder_b_bucket", Some("1"), 9.0),
        ("ladder_b_bucket", Some("10"), 15.0),
        ("ladder_b_count", None, 20.0),
        ("ladder_b_sum", None, 21.0),
    ]
    .into_iter()
    .map(|(n, le, v): (&str, Option<&str>, f64)| (n.to_string(), le.map(str::to_string), v))
    .collect::<Vec<_>>();
    want.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    want
}

fn assert_series_eq(got: &[(String, Option<String>, f64)], want: &[(String, Option<String>, f64)]) {
    assert_eq!(
        got.len(),
        want.len(),
        "exactly the exploded series, no more and no fewer: got {got:?}, want {want:?}"
    );
    for (got, want) in got.iter().zip(want) {
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
}

/// A batch that closes one histogram data point and opens the next credits
/// only the CLOSED point's rows to its write, so the resume offset a failed
/// load prints lands on a data-point boundary.
///
/// Rows 0-2 are one data point and rows 3-5 are the next. At
/// `--batch-rows 4` the first batch reads rows 0-3: rows 0-2 close the first
/// data point, row 3 only opens the second. Crediting the write with the
/// batch's four rows would report three durable rows plus one that is still
/// buffered, and a resume at that offset would load rows 4-5 as a data point
/// of two buckets instead of three -- a silently truncated ladder, since a
/// two-bucket group is perfectly well-formed.
#[tokio::test]
async fn a_failed_histogram_load_resumes_on_a_data_point_boundary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let poisoned = dir.path().join("poisoned.parquet");
    let clean = dir.path().join("clean.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;
    write_parquet(&poisoned, &ladder_batch(event_ns, true));
    write_parquet(&clean, &ladder_batch(event_ns, false));

    let mapping = metrics_mapping(LADDER_MAPPING);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");

    let err = load::load_metrics(
        Arc::clone(&store),
        &poisoned,
        "acme",
        &mapping,
        1,
        // --batch-rows 4, so the first batch straddles the boundary between
        // the two data points; --pipeline-depth 1, the only geometry whose
        // resume offset means anything.
        4,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("row 5's fractional bucket count is refused");

    let LoadError::RowRejected { row, resume, .. } = &err else {
        panic!("expected a per-row rejection, got {err:?}");
    };
    assert_eq!(*row, 5, "the refusal names the row it read");
    let resume = *resume;
    assert_eq!(
        resume.rows_written, 3,
        "only the rows of the data point whose write acked are durable; row 3 opened the \
         second data point and is not one of them"
    );
    assert_eq!(resume.rows_skipped, 0);
    assert_eq!(
        resume.next_skip_rows(),
        3,
        "the resume offset is the boundary between the two data points"
    );

    // Resuming at that offset over a clean file loads the second data point
    // whole: three bounds, not the two a truncated group would have.
    let resumed = load::load_metrics(
        Arc::clone(&store),
        &clean,
        "acme",
        &mapping,
        1,
        4,
        resume.next_skip_rows(),
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect("the resumed load succeeds");
    assert_eq!(resumed.rows_skipped, 3);
    assert_eq!(resumed.rows_processed, 3, "the second data point's rows");
    assert_eq!(
        resumed.histogram_points_exploded, 1,
        "exactly one data point was exploded by the resumed load"
    );
    assert_eq!(
        resumed.points_written, 6,
        "three bounds explode into 3 buckets + the +Inf bucket + _sum + _count"
    );

    let app = query_app(Arc::clone(&store), &tenant);
    let mut series: Vec<(String, Option<String>, f64)> = Vec::new();
    for name in ["ladder_b_bucket", "ladder_b_sum", "ladder_b_count"] {
        series.extend(histogram_series(
            &query_range_at(&app, name, event_ns, &resumed.tokens).await,
        ));
    }
    series.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    assert_series_eq(&series, &expected_ladder_b());
}

/// One data point whose bucket rows straddle a batch boundary is still one
/// data point: the open group is carried into the next batch and closed
/// there, and its rows are credited to the write that carries its points.
#[tokio::test]
async fn a_data_point_split_across_batches_explodes_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("split.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;
    write_parquet(&pq, &ladder_batch(event_ns, false));

    let mapping = metrics_mapping(LADDER_MAPPING);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");

    // --batch-rows 2 cuts both data points: rows 0-1, 2-3, 4-5.
    let report = load::load_metrics(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        2,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect("a data point split across batches loads");

    assert_eq!(
        report.rows_processed, 6,
        "every source row is credited once"
    );
    assert_eq!(
        report.histogram_points_exploded, 2,
        "two data points, not four halves"
    );
    assert_eq!(report.points_written, 12, "six exploded series each");

    let app = query_app(Arc::clone(&store), &tenant);
    let mut series: Vec<(String, Option<String>, f64)> = Vec::new();
    for name in ["ladder_b_bucket", "ladder_b_sum", "ladder_b_count"] {
        series.extend(histogram_series(
            &query_range_at(&app, name, event_ns, &report.tokens).await,
        ));
    }
    series.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    assert_series_eq(&series, &expected_ladder_b());
}

/// An empty label cell is dropped from the series exactly as a null one is,
/// and exactly as OTLP drops an empty attribute value: the two rows below are
/// ONE series carrying both samples, not `{job=""}` beside `{}`.
#[tokio::test]
async fn an_empty_label_cell_is_the_same_series_as_a_missing_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("empty_label.parquet");

    let load_ns = now_ns();
    let first_ns = load_ns - 120 * NS_PER_SEC;
    let second_ns = first_ns + NS_PER_SEC;

    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![first_ns, second_ns])),
        ("value".to_string(), f64_col(vec![1.0, 2.0])),
        (
            "svc".to_string(),
            Arc::new(StringArray::from(vec![Some(""), None])) as ArrayRef,
        ),
    ])
    .expect("record batch");
    write_parquet(&pq, &batch);

    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "empty_label_demo"
        value_column = "value"
        ts_column = "ts"
        ts_unit = "nanos"

        [[metrics.label]]
        name = "job"
        column = "svc"
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
    .expect("both rows load");
    assert_eq!(report.points_written, 2);

    let app = query_app(Arc::clone(&store), &tenant);
    let body = query_range_span(
        &app,
        "empty_label_demo",
        first_ns,
        second_ns,
        &report.tokens,
    )
    .await;
    let results = range_results(&body);
    assert_eq!(
        results.len(),
        1,
        "the empty cell and the null cell are one series, got {body}"
    );
    assert!(
        results[0]["metric"]["job"].is_null(),
        "the empty label value is absent from the series, got {}",
        results[0]["metric"]
    );
    let values = results[0]["values"]
        .as_array()
        .unwrap_or_else(|| panic!("no values array in {}", results[0]));
    assert_eq!(values.len(), 2, "both samples are on that one series");
    assert_eq!(values[0][1], "1", "the first step carries the first sample");
    assert_eq!(values[1][1], "2", "the second step carries the second");
}

/// A null `sum` cell emits no `_sum` series, matching an OTLP histogram data
/// point with no `sum` field. The `_bucket` and `_count` series are unaffected.
#[tokio::test]
async fn a_null_sum_emits_no_sum_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("no_sum.parquet");

    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;

    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![event_ns, event_ns])),
        ("le".to_string(), f64_col(vec![0.5, 2.0])),
        ("bucket_count".to_string(), f64_col(vec![1.0, 2.0])),
        ("sum".to_string(), opt_f64_col(vec![None, None])),
        ("count".to_string(), i64_col(vec![4, 4])),
    ])
    .expect("record batch");
    write_parquet(&pq, &batch);

    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "no_sum"
        value_column = "bucket_count"
        ts_column = "ts"
        ts_unit = "nanos"

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
    .expect("a data point with no sum loads");
    assert_eq!(
        report.points_written, 4,
        "two buckets + the +Inf bucket + _count, and NO _sum"
    );

    let app = query_app(Arc::clone(&store), &tenant);
    assert!(
        range_results(&query_range_at(&app, "no_sum_sum", event_ns, &report.tokens).await)
            .is_empty(),
        "no _sum series exists for a data point whose sum column was null"
    );
    let mut series =
        histogram_series(&query_range_at(&app, "no_sum_bucket", event_ns, &report.tokens).await);
    series.extend(histogram_series(
        &query_range_at(&app, "no_sum_count", event_ns, &report.tokens).await,
    ));
    series.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    let mut want: Vec<(String, Option<String>, f64)> = vec![
        ("no_sum_bucket", Some("+Inf"), 4.0),
        ("no_sum_bucket", Some("0.5"), 1.0),
        ("no_sum_bucket", Some("2"), 3.0),
        ("no_sum_count", None, 4.0),
    ]
    .into_iter()
    .map(|(n, le, v): (&str, Option<&str>, f64)| (n.to_string(), le.map(str::to_string), v))
    .collect();
    want.sort_by(|a, b| (a.0.as_str(), a.1.as_deref()).cmp(&(b.0.as_str(), b.1.as_deref())));
    assert_series_eq(&series, &want);
}

/// A histogram shape whose rows disagree about the data point's own figures
/// is refused, naming the first row of the group. `sum` and `count` describe
/// the data point, not the bucket, so a row that changes one of them is
/// either a mis-sorted file or a mis-declared mapping, and exploding it would
/// write a ladder whose `_count` contradicts its `+Inf` bucket.
#[tokio::test]
async fn rows_of_one_data_point_that_disagree_on_sum_or_count_are_refused() {
    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;
    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "disagree"
        value_column = "bucket_count"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.histogram]
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
    );

    for (case, sums, counts, wanted) in [
        (
            "count",
            vec![3.0, 3.0],
            vec![5_i64, 6],
            "the count column carries the DATA POINT's total",
        ),
        (
            "sum",
            vec![3.0, 4.0],
            vec![5_i64, 5],
            "the sum column carries the DATA POINT's sum",
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join(format!("{case}.parquet"));
        let batch = RecordBatch::try_from_iter(vec![
            ("ts".to_string(), i64_col(vec![event_ns, event_ns])),
            ("le".to_string(), f64_col(vec![0.5, 2.0])),
            ("bucket_count".to_string(), f64_col(vec![1.0, 2.0])),
            ("sum".to_string(), f64_col(sums)),
            ("count".to_string(), i64_col(counts)),
        ])
        .expect("record batch");
        write_parquet(&pq, &batch);

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
        .expect_err("a disagreeing row is refused");
        let LoadError::RowRejected { row, reason, .. } = &err else {
            panic!("expected a per-row rejection for the {case} case, got {err:?}");
        };
        assert_eq!(*row, 1, "the refusal names the row that disagreed");
        assert!(
            reason.contains(wanted),
            "the {case} refusal states the rule: {reason}"
        );
        assert!(
            reason.contains("row 0"),
            "the {case} refusal points at the first row of the group: {reason}"
        );
    }
}

/// A `le` that is not finite, and bounds that do not strictly increase, are
/// each refused. The `+Inf` bucket is synthesized from the count column and
/// must never be a row of its own, and a ladder built from unsorted bounds
/// would accumulate counts into the wrong buckets.
#[tokio::test]
async fn a_non_finite_le_and_non_increasing_bounds_are_refused() {
    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;
    let mapping = metrics_mapping(
        r#"
        [metrics]
        name = "bounds"
        value_column = "bucket_count"
        ts_column = "ts"
        ts_unit = "nanos"

        [metrics.histogram]
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
    );

    for (case, les, wanted) in [
        (
            "infinite",
            vec![0.5, f64::INFINITY],
            "not a finite bucket bound",
        ),
        (
            "unsorted",
            vec![2.0, 0.5],
            "do not strictly increase in row order",
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join(format!("{case}.parquet"));
        let batch = RecordBatch::try_from_iter(vec![
            ("ts".to_string(), i64_col(vec![event_ns, event_ns])),
            ("le".to_string(), f64_col(les)),
            ("bucket_count".to_string(), f64_col(vec![1.0, 2.0])),
            ("sum".to_string(), f64_col(vec![3.0, 3.0])),
            ("count".to_string(), i64_col(vec![5, 5])),
        ])
        .expect("record batch");
        write_parquet(&pq, &batch);

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
        .expect_err("the bad bound is refused");
        let LoadError::RowRejected { reason, .. } = &err else {
            panic!("expected a per-row rejection for the {case} case, got {err:?}");
        };
        assert!(
            reason.contains(wanted),
            "the {case} refusal names the problem: {reason}"
        );
    }
}

/// `kind` alongside `[metrics.histogram]` is refused rather than ignored:
/// OTLP has no monotonic histogram, so nothing the loader could do with it
/// would match the OTLP path, and a silently ignored mapping key reads as a
/// setting that took effect.
#[test]
fn a_counter_kind_on_a_histogram_mapping_is_refused() {
    let err = load::parse_metrics_mapping(
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"
        kind = "counter"

        [metrics.histogram]
        le_column = "le"
        sum_column = "sum"
        count_column = "count"
        "#,
    )
    .expect_err("kind and a histogram shape cannot both be set");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("kind") && message.contains("_total"),
        "the refusal names the key and what it would have meant: {message}"
    );
}

/// Two mapped label names that differ only in characters the OTLP sanitizer
/// rewrites are ONE label name, and declaring both is refused at setup rather
/// than discovered as a duplicate-label rejection on the first row.
#[test]
fn label_names_that_sanitize_to_the_same_name_are_refused() {
    let err = load::parse_metrics_mapping(
        r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"

        [[metrics.label]]
        name = "http.method"
        column = "a"

        [[metrics.label]]
        name = "http_method"
        column = "b"
        "#,
    )
    .expect_err("two labels that sanitize to one name are refused");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("http_method"),
        "the refusal names the name they collide on: {message}"
    );
}

/// A pre-ADR-1751 logs mapping keeps the error prefix it has always had. Its
/// author never wrote a section, so a message naming one would point at
/// nothing in the file.
#[test]
fn a_pre_adr_1751_logs_mapping_keeps_its_original_error_prefix() {
    let err = load::parse_mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"
no_such_key = "x"
"#,
    )
    .expect_err("an unknown key is still refused");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.starts_with("invalid --mapping TOML:"),
        "the top-level form keeps its original prefix: {message}"
    );

    // The sectioned spelling names the section, since that one is in the file.
    let err = load::parse_mapping(
        r#"
[logs]
ts_column = "ts"
ts_unit = "nanos"
no_such_key = "x"
"#,
    )
    .expect_err("an unknown key is still refused");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.starts_with("invalid --mapping [logs] section:"),
        "a written section is named: {message}"
    );
}

/// A schema error in a mapping names the line it is on, in both the
/// pre-ADR-1751 top-level form and a section. The unknown key sits on line 4
/// of each document (the raw string opens with a newline).
///
/// Non-vacuity: with the section deserialized through
/// `toml::Value::try_into`, which has no source text and so no spans, every
/// assertion on "line 4" fails.
#[test]
fn a_mapping_schema_error_names_its_line() {
    for (text, signal) in [
        (
            "\nts_column = \"ts\"\nts_unit = \"nanos\"\nno_such_key = \"x\"\n",
            SignalArg::Logs,
        ),
        (
            "\n[logs]\nts_column = \"ts\"\nno_such_key = \"x\"\nts_unit = \"nanos\"\n",
            SignalArg::Logs,
        ),
        (
            "\n[metrics]\nname = \"m\"\nno_such_key = \"x\"\nvalue_column = \"v\"\n\
             ts_column = \"ts\"\nts_unit = \"nanos\"\n",
            SignalArg::Metrics,
        ),
    ] {
        let err =
            load::parse_mapping_document(text, signal).expect_err("an unknown key is refused");
        let LoadError::Setup(message) = err else {
            panic!("expected a setup error");
        };
        assert!(
            message.contains("line 4"),
            "the error names the offending line: {message}"
        );
        assert!(
            message.contains("no_such_key"),
            "and the offending key: {message}"
        );
    }
}
