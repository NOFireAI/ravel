//! Phase 3 gateway wiring (issue #12): drive one encoded OTAP metrics batch
//! through the `otap`-gated `ArrowMetricsService` in-process and assert the
//! strict ack carries a commit token, then that the stored, queryable data
//! matches what the equivalent OTLP request produces on the same server.
//!
//! Compiled only under the `otap` feature (the whole gRPC service is), so this
//! file is empty in the default build.
#![cfg(feature = "otap")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_otap::encode::{DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder};
use ravel_otap::proto::experimental::arrow::v1::StatusCode;
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_client::ArrowMetricsServiceClient;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const VALUE: f64 = 42.5;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
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
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
    };
    ravel_server::start(config, store)
        .await
        .expect("server starts")
}

/// One encoded OTAP batch: a single gauge `metric_name` with one point at
/// `ts_ns` valued [`VALUE`] and no attributes. This is the columnar
/// equivalent of [`otlp_gauge_request`] below.
fn otap_gauge_batch(
    metric_name: &str,
    ts_ns: i64,
) -> ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords {
    let metrics = vec![MetricRow {
        name: metric_name.to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: ts_ns,
            value: VALUE,
            flags: 0,
            attrs: vec![],
        }],
    }];
    let mut encoder = MetricsStreamEncoder::new("otap-grpc-test").expect("new encoder");
    encoder.encode_batch(0, &metrics).expect("encode batch")
}

/// The OTLP equivalent: one gauge, one point, no resource (so no synthesized
/// `job`/`instance`), matching the OTAP encoder's known scope (it emits no
/// RESOURCE_ATTRS). The two paths must therefore produce byte-identical
/// series identity and value.
fn otlp_gauge_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(VALUE)),
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

/// Sends one OTAP batch over the bidirectional `ArrowMetrics` stream and
/// returns the single `BatchStatus` the server replies with. Default write
/// mode is Strict (no `x-ravel-ingest-mode` header), so the ack blocks until
/// the batch is durable and carries commit tokens.
async fn send_one_batch(
    grpc_addr: std::net::SocketAddr,
    batch: ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords,
) -> ravel_otap::proto::experimental::arrow::v1::BatchStatus {
    let mut client = ArrowMetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect to gRPC listener");
    let mut request = tonic::Request::new(futures::stream::iter(vec![batch]));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("valid metadata"),
    );
    let mut inbound = client
        .arrow_metrics(request)
        .await
        .expect("arrow_metrics stream opens")
        .into_inner();
    inbound
        .message()
        .await
        .expect("read batch status")
        .expect("exactly one batch status")
}

/// Instant query for `metric_name`, returning the single result series as
/// JSON. `min_commit_token` pins read-your-write visibility.
async fn query_one_series(
    http_addr: std::net::SocketAddr,
    metric_name: &str,
    min_commit_token: &str,
) -> serde_json::Value {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{http_addr}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", metric_name),
            ("min_commit_token", min_commit_token),
        ])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(response.status(), 200, "query should succeed");
    let body: serde_json::Value = response.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success", "query body: {body}");
    let result = body["data"]["result"]
        .as_array()
        .expect("result is an array")
        .clone();
    assert_eq!(result.len(), 1, "expected exactly one series: {body}");
    result.into_iter().next().unwrap()
}

fn series_value(series: &serde_json::Value) -> f64 {
    series["value"][1]
        .as_str()
        .expect("value is a string")
        .parse()
        .expect("value parses as f64")
}

/// A single OTAP batch acks OK with a non-empty commit token, and the sample
/// it carried is queryable at that token with the right series and value.
#[tokio::test]
async fn otap_batch_acks_with_commit_token_and_round_trips() {
    let running = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");

    let ts = now_ns();
    let status = send_one_batch(grpc_addr, otap_gauge_batch("otap_gauge", ts)).await;

    assert_eq!(status.batch_id, 0, "ack names the batch it acks");
    assert_eq!(
        status.status_code,
        StatusCode::Ok as i32,
        "strict OTAP write should ack OK, got message: {}",
        status.status_message
    );
    assert!(
        !status.status_message.is_empty(),
        "strict ack must carry a commit token"
    );

    let series = query_one_series(running.http_addr, "otap_gauge", &status.status_message).await;
    assert_eq!(series["metric"]["__name__"], "otap_gauge");
    assert!(
        (series_value(&series) - VALUE).abs() < f64::EPSILON,
        "unexpected value: {series}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The stored, queryable result of an OTAP batch is identical to the OTLP
/// request's for the same logical metric: same series identity (the whole
/// `metric` label map) and same value. This is the phase-2 differential gate
/// (docs/otap-ingest.md) observed end to end through the gateway rather than
/// at the normalizer.
#[tokio::test]
async fn otap_and_otlp_produce_identical_stored_series() {
    let running = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");
    let http_base = format!("http://{}", running.http_addr);
    let ts = now_ns();

    // OTAP path over gRPC.
    let otap_status = send_one_batch(grpc_addr, otap_gauge_batch("gauge_otap", ts)).await;
    assert_eq!(otap_status.status_code, StatusCode::Ok as i32);

    // OTLP path over HTTP, equivalent input.
    let http = reqwest::Client::new();
    let otlp_response = http
        .post(format!("{http_base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(otlp_gauge_request("gauge_otlp", ts).encode_to_vec())
        .send()
        .await
        .expect("OTLP export succeeds");
    assert_eq!(otlp_response.status(), 200);
    let otlp_token = otlp_response
        .headers()
        .get("x-ravel-commit-token")
        .expect("OTLP commit token header")
        .to_str()
        .expect("token header is ascii")
        .to_string();

    let otap_series =
        query_one_series(running.http_addr, "gauge_otap", &otap_status.status_message).await;
    let otlp_series = query_one_series(running.http_addr, "gauge_otlp", &otlp_token).await;

    // Values match.
    assert!((series_value(&otap_series) - series_value(&otlp_series)).abs() < f64::EPSILON);

    // Series identity matches: both `metric` maps carry only `__name__` (their
    // own metric name) and no other labels. Comparing the maps with the name
    // removed proves the OTAP path admitted the same label set OTLP did.
    let strip_name = |mut m: serde_json::Value| {
        let obj = m.as_object_mut().expect("metric is an object");
        obj.remove("__name__");
        m
    };
    assert_eq!(
        strip_name(otap_series["metric"].clone()),
        strip_name(otlp_series["metric"].clone()),
        "OTAP and OTLP series carry different labels"
    );

    running.shutdown().await.expect("graceful shutdown");
}
