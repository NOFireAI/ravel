//! End-to-end coverage for ADR-1103 (PromQL over logs, issue #1109): a real
//! OTLP logs export against an in-process server, read back through the
//! Prometheus-compatible HTTP API's `ravel_log_lines`/`ravel_log_bytes`
//! reserved metric names, exactly as a Grafana Prometheus datasource would.
//!
//! Every figure below is a named constant computed from the fixture bodies
//! below, not a magic number, so the arithmetic behind each assertion is
//! auditable.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use serde_json::Value as JsonValue;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

/// One second in nanoseconds; every fixture record's timestamp is `BASE`
/// plus a whole multiple of this, so second-granularity HTTP `time`/`start`/
/// `end` params (`secs`, below) land exactly, with no truncation ambiguity.
const NS: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// Fixture bodies, api resource (`service.name=api`, `k8s.pod.name=p1`).
// Byte lengths are `str::len()` on ASCII text, so they equal both the char
// count and what `wc -c` reports; each is spelled out here rather than
// computed at test time so the expected totals below are auditable against
// the literal bodies.
// ---------------------------------------------------------------------------

/// Shares its timestamp with `API_ERR_2_BODY` (the "shared timestamp" the
/// range-query test straddles). Contains "timeout" (one of the two
/// `__body__=~".*timeout.*"` matches).
const API_ERR_1_BODY: &str = "request failed with timeout";
const API_ERR_1_LEN: usize = 27;
/// Shares `API_ERR_1_BODY`'s timestamp.
const API_ERR_2_BODY: &str = "upstream unavailable";
const API_ERR_2_LEN: usize = 20;
/// Contains "timeout" (the second `__body__=~".*timeout.*"` match).
const API_ERR_3_BODY: &str = "disk write timeout occurred";
const API_ERR_3_LEN: usize = 27;
const API_ERR_4_BODY: &str = "connection reset by peer";
const API_ERR_4_LEN: usize = 24;
const API_ERR_5_BODY: &str = "null pointer dereference";
const API_ERR_5_LEN: usize = 24;

const API_INFO_1_BODY: &str = "request completed";
const API_INFO_1_LEN: usize = 17;
const API_INFO_2_BODY: &str = "cache warmed";
const API_INFO_2_LEN: usize = 12;
const API_INFO_3_BODY: &str = "health check ok";
const API_INFO_3_LEN: usize = 15;

/// api's 5 ERROR lines.
const API_ERROR_LINES: usize = 5;
/// api's 3 INFO lines.
const API_INFO_LINES: usize = 3;
/// api's 8 lines total (`job="api"` line count).
const API_TOTAL_LINES: usize = API_ERROR_LINES + API_INFO_LINES;
/// Byte total of api's 5 ERROR bodies.
const API_ERROR_BYTES: usize =
    API_ERR_1_LEN + API_ERR_2_LEN + API_ERR_3_LEN + API_ERR_4_LEN + API_ERR_5_LEN;
/// Byte total of api's 3 INFO bodies.
const API_INFO_BYTES: usize = API_INFO_1_LEN + API_INFO_2_LEN + API_INFO_3_LEN;
/// Byte total of all 8 api bodies (`sum(sum_over_time(ravel_log_bytes{job="api"}[1h]))`).
const API_TOTAL_BYTES: usize = API_ERROR_BYTES + API_INFO_BYTES;
/// Lines whose body contains "timeout": `API_ERR_1_BODY` and `API_ERR_3_BODY`.
const API_TIMEOUT_LINES: usize = 2;

// ---------------------------------------------------------------------------
// Fixture bodies, worker resource (`service.name=worker`, no other resource
// attributes, so no `instance` and no attribute-derived label beyond `job`).
// ---------------------------------------------------------------------------

const WORKER_ERR_1_BODY: &str = "job failed";
const WORKER_ERR_1_LEN: usize = 10;
const WORKER_ERR_2_BODY: &str = "job retrying";
const WORKER_ERR_2_LEN: usize = 12;
const WORKER_ERR_3_BODY: &str = "job failed permanently";
const WORKER_ERR_3_LEN: usize = 22;
const WORKER_ERR_4_BODY: &str = "queue backlog critical";
const WORKER_ERR_4_LEN: usize = 22;

/// worker's 4 ERROR lines (worker carries no INFO lines).
const WORKER_ERROR_LINES: usize = 4;
/// Byte total of worker's 4 bodies; not directly asserted over HTTP, kept
/// here so the fixture sanity check below exercises every body constant.
const WORKER_TOTAL_BYTES: usize =
    WORKER_ERR_1_LEN + WORKER_ERR_2_LEN + WORKER_ERR_3_LEN + WORKER_ERR_4_LEN;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// The HTTP API's `time`/`start`/`end` params are Prometheus-style unix
/// seconds. Every fixture timestamp is `base_ns + k * NS` for an integer
/// `k`, so `secs` divides out exactly: `(a + k*NS) / NS == a/NS + k` for any
/// remainder `a` has against `NS`, with no rounding ambiguity between
/// adjacent fixture timestamps.
fn secs(ts_ns: i64) -> i64 {
    ts_ns / NS
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn log_record(
    ts_ns: i64,
    severity_number: SeverityNumber,
    severity_text: &str,
    body: &str,
) -> LogRecord {
    LogRecord {
        time_unix_nano: ts_ns as u64,
        observed_time_unix_nano: ts_ns as u64,
        severity_number: severity_number as i32,
        severity_text: severity_text.to_string(),
        body: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(body.to_string())),
        }),
        ..Default::default()
    }
}

fn scope() -> Option<InstrumentationScope> {
    Some(InstrumentationScope {
        name: "scope".to_string(),
        version: "1.0".to_string(),
        ..Default::default()
    })
}

/// One OTLP logs export carrying both resources described in issue #1109:
/// `api` (with `k8s.pod.name=p1`, 5 ERROR + 3 INFO lines, two ERROR lines
/// sharing `base_ns`) and `worker` (no extra resource attributes, 4 ERROR
/// lines).
fn logs_export_request(base_ns: i64) -> ExportLogsServiceRequest {
    let api_resource = Resource {
        attributes: vec![
            string_kv("service.name", "api"),
            string_kv("k8s.pod.name", "p1"),
        ],
        ..Default::default()
    };
    let api_records = vec![
        log_record(base_ns, SeverityNumber::Error, "ERROR", API_ERR_1_BODY),
        log_record(base_ns, SeverityNumber::Error, "ERROR", API_ERR_2_BODY),
        log_record(base_ns + NS, SeverityNumber::Error, "ERROR", API_ERR_3_BODY),
        log_record(
            base_ns + 2 * NS,
            SeverityNumber::Error,
            "ERROR",
            API_ERR_4_BODY,
        ),
        log_record(
            base_ns + 3 * NS,
            SeverityNumber::Error,
            "ERROR",
            API_ERR_5_BODY,
        ),
        log_record(
            base_ns + 4 * NS,
            SeverityNumber::Info,
            "INFO",
            API_INFO_1_BODY,
        ),
        log_record(
            base_ns + 5 * NS,
            SeverityNumber::Info,
            "INFO",
            API_INFO_2_BODY,
        ),
        log_record(
            base_ns + 6 * NS,
            SeverityNumber::Info,
            "INFO",
            API_INFO_3_BODY,
        ),
    ];

    let worker_resource = Resource {
        attributes: vec![string_kv("service.name", "worker")],
        ..Default::default()
    };
    let worker_records = vec![
        log_record(
            base_ns + 7 * NS,
            SeverityNumber::Error,
            "ERROR",
            WORKER_ERR_1_BODY,
        ),
        log_record(
            base_ns + 8 * NS,
            SeverityNumber::Error,
            "ERROR",
            WORKER_ERR_2_BODY,
        ),
        log_record(
            base_ns + 9 * NS,
            SeverityNumber::Error,
            "ERROR",
            WORKER_ERR_3_BODY,
        ),
        log_record(
            base_ns + 10 * NS,
            SeverityNumber::Error,
            "ERROR",
            WORKER_ERR_4_BODY,
        ),
    ];

    ExportLogsServiceRequest {
        resource_logs: vec![
            ResourceLogs {
                resource: Some(api_resource),
                scope_logs: vec![ScopeLogs {
                    scope: scope(),
                    log_records: api_records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceLogs {
                resource: Some(worker_resource),
                scope_logs: vec![ScopeLogs {
                    scope: scope(),
                    log_records: worker_records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ],
    }
}

fn metrics_export_request(
    metric_name: &str,
    job: &str,
    value: f64,
    ts_ns: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", job)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    params: &[(&str, &str)],
) -> (reqwest::StatusCode, JsonValue) {
    let response = client
        .get(url)
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(params)
        .send()
        .await
        .expect("query request succeeds");
    let status = response.status();
    let json: JsonValue = response.json().await.expect("query response is JSON");
    (status, json)
}

fn vector_results(body: &JsonValue) -> &Vec<JsonValue> {
    assert_eq!(body["status"], "success", "unexpected response: {body}");
    assert_eq!(body["data"]["resultType"], "vector", "body: {body}");
    body["data"]["result"]
        .as_array()
        .expect("result is an array")
}

fn instant_value(sample: &JsonValue) -> f64 {
    sample["value"][1]
        .as_str()
        .expect("value is a string")
        .parse()
        .expect("value parses as f64")
}

/// Finds the one vector entry whose `metric` map is exactly `want` (same
/// keys, same values, nothing extra), panicking with the full result set
/// otherwise -- an exact match, not a "contains" check.
fn find_exact_series<'a>(results: &'a [JsonValue], want: &[(&str, &str)]) -> &'a JsonValue {
    let matches: Vec<&JsonValue> = results
        .iter()
        .filter(|entry| {
            let metric = entry["metric"].as_object().expect("metric is an object");
            metric.len() == want.len()
                && want
                    .iter()
                    .all(|(k, v)| metric.get(*k).and_then(JsonValue::as_str) == Some(*v))
        })
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one series with metric == {want:?}, found {}: {results:?}",
        matches.len()
    );
    matches[0]
}

#[tokio::test]
async fn otlp_logs_are_queryable_through_promql_over_http() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // Whole-second aligned: a sub-second remainder would push each record's
    // true `time_unix_nano` just past its nominal integer-second grid
    // boundary, since `secs()` truncates it away when building query params,
    // silently shifting every count_over_time grid point below by one step.
    let base_ns = secs(now_ns()) * NS;

    // --- Ingest: one OTLP logs export, two resources, twelve records. ---
    let logs_request = logs_export_request(base_ns);
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(logs_request.encode_to_vec())
        .send()
        .await
        .expect("logs export request succeeds");
    assert_eq!(response.status(), 200, "logs export should succeed");
    let logs_commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    // --- Ingest: one OTLP metric, to prove the logs export left the
    // metrics lane untouched. ---
    const METRIC_VALUE: f64 = 99.5;
    let metrics_request =
        metrics_export_request("demo_requests_total", "checkout", METRIC_VALUE, base_ns);
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(metrics_request.encode_to_vec())
        .send()
        .await
        .expect("metrics export request succeeds");
    assert_eq!(response.status(), 200, "metrics export should succeed");
    let metrics_commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    let query_url = format!("{base}/api/v1/query");
    let query_range_url = format!("{base}/api/v1/query_range");
    let series_url = format!("{base}/api/v1/series");
    let name_values_url = format!("{base}/api/v1/label/__name__/values");
    let metadata_url = format!("{base}/api/v1/metadata");

    // Well past every fixture timestamp (base_ns + 10*NS is the last one),
    // but inside every window used below.
    let eval_time = secs(base_ns + 20 * NS).to_string();

    // --- sum by (job) (count_over_time(ravel_log_lines{severity_text="ERROR"}[1h])) ---
    let (status, body) = get_json(
        &client,
        &query_url,
        &[
            (
                "query",
                r#"sum by (job) (count_over_time(ravel_log_lines{severity_text="ERROR"}[1h]))"#,
            ),
            ("time", &eval_time),
            ("min_commit_token", &logs_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let results = vector_results(&body);
    assert_eq!(results.len(), 2, "expected api and worker: {results:?}");
    let api_series = find_exact_series(results, &[("job", "api")]);
    assert!(
        (instant_value(api_series) - API_ERROR_LINES as f64).abs() < f64::EPSILON,
        "job=api ERROR count: {api_series}"
    );
    let worker_series = find_exact_series(results, &[("job", "worker")]);
    assert!(
        (instant_value(worker_series) - WORKER_ERROR_LINES as f64).abs() < f64::EPSILON,
        "job=worker ERROR count: {worker_series}"
    );

    // --- sum(sum_over_time(ravel_log_bytes{job="api"}[1h])) ---
    // (`ravel_log_bytes{job="api"}` alone matches two series -- ERROR and
    // INFO differ by `severity_text` -- so the outer `sum` is what turns
    // "per-series byte totals" into "the byte total of api's 8 bodies".)
    let (status, body) = get_json(
        &client,
        &query_url,
        &[
            (
                "query",
                r#"sum(sum_over_time(ravel_log_bytes{job="api"}[1h]))"#,
            ),
            ("time", &eval_time),
            ("min_commit_token", &logs_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let results = vector_results(&body);
    assert_eq!(
        results.len(),
        1,
        "expected one aggregated series: {results:?}"
    );
    assert!(
        (instant_value(&results[0]) - API_TOTAL_BYTES as f64).abs() < f64::EPSILON,
        "job=api byte total: {}, want {API_TOTAL_BYTES}",
        results[0]
    );

    // --- count_over_time(ravel_log_lines{job="api", __body__=~".*timeout.*"}[1h]) ---
    let (status, body) = get_json(
        &client,
        &query_url,
        &[
            (
                "query",
                r#"count_over_time(ravel_log_lines{job="api", __body__=~".*timeout.*"}[1h])"#,
            ),
            ("time", &eval_time),
            ("min_commit_token", &logs_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let results = vector_results(&body);
    assert_eq!(
        results.len(),
        1,
        "expected one matching series: {results:?}"
    );
    assert!(
        (instant_value(&results[0]) - API_TIMEOUT_LINES as f64).abs() < f64::EPSILON,
        "timeout line count: {}, want {API_TIMEOUT_LINES}",
        results[0]
    );
    let metric = results[0]["metric"]
        .as_object()
        .expect("metric is an object");
    assert!(
        !metric.contains_key("__body__"),
        "__body__ must never appear in a returned label set: {metric:?}"
    );

    // --- query_range: sum(count_over_time(ravel_log_lines{job="api"}[10m])) ---
    // straddling the shared timestamp at base_ns. The window (600s) is far
    // wider than the fixture's ~10s spread, so each step's count is the
    // cumulative line count up to that step. A range-vector selector with no
    // samples in a step's window contributes no point at all (not a 0), so
    // the series starts at base_ns with value 2, not before it with 0; that
    // first value being 2 (not 1) proves both same-timestamp ERROR records
    // survive the step (neither dropped nor merged into one). The window
    // keeps counting 8 for one extra step (base_ns + 7s) after the last
    // write, proving cumulative counts persist rather than expiring the
    // instant no new line arrives.
    let range_start = secs(base_ns - NS).to_string();
    let range_end = secs(base_ns + 7 * NS).to_string();
    let (status, body) = get_json(
        &client,
        &query_range_url,
        &[
            (
                "query",
                r#"sum(count_over_time(ravel_log_lines{job="api"}[10m]))"#,
            ),
            ("start", &range_start),
            ("end", &range_end),
            ("step", "1s"),
            ("min_commit_token", &logs_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["status"], "success", "body: {body}");
    assert_eq!(body["data"]["resultType"], "matrix", "body: {body}");
    let matrix = body["data"]["result"]
        .as_array()
        .expect("matrix result array");
    assert_eq!(
        matrix.len(),
        1,
        "expected one aggregated series: {matrix:?}"
    );
    let values: Vec<f64> = matrix[0]["values"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|pair| {
            pair[1]
                .as_str()
                .expect("value is a string")
                .parse::<f64>()
                .expect("value parses as f64")
        })
        .collect();
    assert_eq!(
        values,
        vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0],
        "per-step cumulative api line count: {matrix:?}"
    );

    // --- /api/v1/series?match[]=ravel_log_lines{job="api"} ---
    let window_start = secs(base_ns - NS).to_string();
    let window_end = secs(base_ns + 20 * NS).to_string();
    let (status, body) = get_json(
        &client,
        &series_url,
        &[
            ("match[]", r#"ravel_log_lines{job="api"}"#),
            ("start", &window_start),
            ("end", &window_end),
            ("min_commit_token", &logs_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let series = body["data"].as_array().expect("series result array");
    assert_eq!(
        series.len(),
        2,
        "job=api must split into an ERROR series and an INFO series: {series:?}"
    );
    let mut severities: Vec<&str> = series
        .iter()
        .map(|s| {
            s.as_object()
                .expect("label set is an object")
                .get("severity_text")
                .and_then(JsonValue::as_str)
                .expect("severity_text label present")
        })
        .collect();
    severities.sort_unstable();
    assert_eq!(severities, vec!["ERROR", "INFO"], "series: {series:?}");
    for label_set in series {
        let obj = label_set.as_object().expect("label set is an object");
        assert_eq!(
            obj.get("__name__").and_then(JsonValue::as_str),
            Some("ravel_log_lines")
        );
        assert_eq!(obj.get("job").and_then(JsonValue::as_str), Some("api"));
        assert_eq!(
            obj.get("k8s_pod_name").and_then(JsonValue::as_str),
            Some("p1")
        );
        assert_eq!(
            obj.get("otel_scope_name").and_then(JsonValue::as_str),
            Some("scope")
        );
        assert_eq!(
            obj.get("otel_scope_version").and_then(JsonValue::as_str),
            Some("1.0")
        );
        assert!(
            obj.get("severity_text")
                .and_then(JsonValue::as_str)
                .is_some(),
            "label set: {obj:?}"
        );
        assert!(
            !obj.contains_key("instance"),
            "fixture sets no service.instance.id, so no series may carry `instance`: {obj:?}"
        );
        assert_eq!(
            obj.len(),
            6,
            "expected exactly __name__, job, k8s_pod_name, otel_scope_name, \
             otel_scope_version, severity_text: {obj:?}"
        );
    }

    // --- /api/v1/label/__name__/values ---
    // No `match[]`, so `resolve_matched_series` resolves the metrics catalog
    // only (`log_metric_of` never sees a selector to recognize); the two
    // reserved names are added afterward, unconditionally, by
    // `include_log_metric_names` rather than through any log-catalog read.
    // `min_commit_token` must therefore be a token the metrics catalog can
    // satisfy, not `logs_commit_token`.
    let (status, body) = get_json(
        &client,
        &name_values_url,
        &[
            ("start", &window_start),
            ("end", &window_end),
            ("min_commit_token", &metrics_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let values: Vec<&str> = body["data"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|v| v.as_str().expect("value is a string"))
        .collect();
    assert!(
        values.contains(&"ravel_log_lines"),
        "__name__ values must contain ravel_log_lines: {values:?}"
    );
    assert!(
        values.contains(&"ravel_log_bytes"),
        "__name__ values must contain ravel_log_bytes: {values:?}"
    );

    // --- /api/v1/metadata ---
    let (status, body) = get_json(&client, &metadata_url, &[]).await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"]["ravel_log_lines"][0]["type"], "gauge");
    assert_eq!(
        body["data"]["ravel_log_lines"][0]["help"],
        "One sample per log line, value 1, derived from the logs signal at query \
         time (ADR-1103); count_over_time counts lines."
    );
    assert_eq!(body["data"]["ravel_log_lines"][0]["unit"], "");
    assert_eq!(body["data"]["ravel_log_bytes"][0]["type"], "gauge");
    assert_eq!(
        body["data"]["ravel_log_bytes"][0]["help"],
        "One sample per log line whose value is the line body's length in bytes \
         (ADR-1103); sum_over_time sums bytes."
    );
    assert_eq!(body["data"]["ravel_log_bytes"][0]["unit"], "bytes");

    // --- The metrics lane is unaffected by the logs export: querying the
    // metric ingested above returns exactly what it would with no logs
    // present at all. ---
    let (status, body) = get_json(
        &client,
        &query_url,
        &[
            ("query", "demo_requests_total"),
            ("time", &eval_time),
            ("min_commit_token", &metrics_commit_token),
        ],
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let results = vector_results(&body);
    assert_eq!(results.len(), 1, "expected exactly one series: {results:?}");
    assert_eq!(results[0]["metric"]["__name__"], "demo_requests_total");
    assert_eq!(results[0]["metric"]["job"], "checkout");
    assert!(
        (instant_value(&results[0]) - METRIC_VALUE).abs() < f64::EPSILON,
        "metric value: {}",
        results[0]
    );
    // Total lines/bytes fixture sanity: named constants, not restated
    // literals, so a change to a body above is caught here rather than
    // silently drifting the assertions that use the totals.
    assert_eq!(API_TOTAL_LINES, 8);
    assert_eq!(API_TOTAL_BYTES, 166);
    assert_eq!(WORKER_TOTAL_BYTES, 66);

    running.shutdown().await.expect("graceful shutdown");
}
