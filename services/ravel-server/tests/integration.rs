//! End-to-end coverage: real OTLP HTTP export against an in-process server
//! backed by `MemoryStore`, followed by a query for the ingested sample.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
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

const TOKEN: &str = "testtoken";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
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

fn export_request(
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
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
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
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

#[tokio::test]
async fn ingest_then_query_round_trips_sample() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let request = export_request("cpu_usage", "demo", 42.5, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "export should succeed");
    let commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    let query_response = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "cpu_usage"),
            ("min_commit_token", commit_token.as_str()),
        ])
        .send()
        .await
        .expect("query request succeeds");

    assert_eq!(query_response.status(), 200, "query should succeed");
    let body: serde_json::Value = query_response.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success");
    let result = body["data"]["result"]
        .as_array()
        .expect("result is an array");
    assert_eq!(result.len(), 1, "expected exactly one series: {body}");
    let sample = &result[0];
    assert_eq!(sample["metric"]["__name__"], "cpu_usage");
    assert_eq!(sample["metric"]["job"], "demo");
    let value_str = sample["value"][1].as_str().expect("value is a string");
    let value: f64 = value_str.parse().expect("value parses as f64");
    assert!(
        (value - 42.5).abs() < f64::EPSILON,
        "unexpected value: {value_str}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn buffered_mode_header_is_honored() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let request = export_request("buffered_metric", "demo", 7.0, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-ravel-ingest-mode", "buffered")
        .body(body)
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "buffered export should succeed");

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn missing_credentials_yield_401() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let request = export_request("cpu_usage", "demo", 1.0, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export request completes");

    assert_eq!(
        response.status(),
        401,
        "missing bearer token should be rejected"
    );

    let query_response = client
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", "cpu_usage")])
        .send()
        .await
        .expect("query request completes");
    assert_eq!(
        query_response.status(),
        401,
        "unauthenticated query should be rejected"
    );

    running.shutdown().await.expect("graceful shutdown");
}
