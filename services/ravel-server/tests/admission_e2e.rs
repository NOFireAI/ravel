//! End-to-end coverage for ADR-0051 admission control wired into
//! `ravel-server`'s ingest paths: layer 4 (series/stream admission) proven
//! over real HTTP OTLP export against an in-process server backed by
//! `MemoryStore`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

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

/// One `ExportMetricsServiceRequest` carrying `metric_names.len()` distinct
/// series (distinct `__name__`, same job), each with one data point. Layer 4
/// tracks identity per series id, so distinct metric names is the simplest
/// way to demand distinct series ids in one request.
fn multi_series_export_request(metric_names: &[&str], ts_ns: i64) -> ExportMetricsServiceRequest {
    let metrics = metric_names
        .iter()
        .map(|name| Metric {
            name: name.to_string(),
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: ts_ns as u64,
                    value: Some(NumberValue::AsDouble(1.0)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "flood")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Starts a full server with `acme`'s admission limits overridden to
/// `tenant_limits`; every other tenant and every other layer keeps this
/// service's shipped defaults, which are generous enough not to interfere
/// with a test's own tight override.
async fn start_test_server_with_limits(tenant_limits: AdmissionLimits) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let mut tenants = HashMap::new();
    tenants.insert(TenantId::new(TENANT), tenant_limits);
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
        limits: LimitsConfig {
            defaults: ravel_server::config::limits::shipped_defaults(),
            tenants,
        },
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        indexed_fields: Default::default(),
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

/// A single request demanding more fresh series than the tenant's active-
/// series cap allows is admitted per-series, not rejected whole (ADR-0051
/// section 1, layer 4): the response is still 200, and
/// `ExportMetricsPartialSuccess.rejected_data_points` reports exactly the
/// count the cap turned away.
#[tokio::test]
async fn fresh_series_flood_capped_per_tenant() {
    const CAP: usize = 3;
    const FLOOD: usize = 10;

    let running = start_test_server_with_limits(AdmissionLimits {
        max_active_series: CountLimit::Bounded(CAP as u64),
        ..AdmissionLimits::default()
    })
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let metric_names: Vec<String> = (0..FLOOD).map(|i| format!("flood_series_{i}")).collect();
    let metric_name_refs: Vec<&str> = metric_names.iter().map(String::as_str).collect();
    let request = multi_series_export_request(&metric_name_refs, now_ns());
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request completes");

    assert_eq!(
        response.status(),
        200,
        "an active-series-cap breach is a partial success, never a 4xx"
    );
    let body = response.bytes().await.expect("response body");
    let decoded = ExportMetricsServiceResponse::decode(body.as_ref())
        .expect("response is an ExportMetricsServiceResponse");
    let partial_success = decoded
        .partial_success
        .expect("cap breach must report a partial success");
    assert_eq!(
        partial_success.rejected_data_points,
        (FLOOD - CAP) as i64,
        "exactly the series beyond the cap must be rejected: {partial_success:?}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Layer 1 (ADR-0051 section 1): a request body over the 16 MiB
/// `DefaultBodyLimit` on `/v1/metrics` is rejected at the transport layer,
/// before tenant resolution or decode even run.
#[tokio::test]
async fn http_body_over_16mib_yields_413() {
    let running = start_test_server_with_limits(AdmissionLimits::default()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let oversized_body = vec![0u8; 16 * 1024 * 1024 + 1];
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(oversized_body)
        .send()
        .await
        .expect("request completes");

    assert_eq!(
        response.status(),
        413,
        "a body over the 16 MiB cap must be rejected before decode"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Layer 1/2 (ADR-0051 section 1) over gRPC: tonic maps both a
/// `max_decoding_message_size` breach and an admission byte-rate rejection
/// to `RESOURCE_EXHAUSTED`, never an HTTP-shaped status.
#[tokio::test]
async fn grpc_byte_rate_exceeded_yields_resource_exhausted() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let running = start_test_server_with_limits(AdmissionLimits {
        ingest_byte_rate: RateLimit::Bounded {
            per_sec: 1,
            burst: 1,
        },
        ..AdmissionLimits::default()
    })
    .await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");

    let mut request = tonic::Request::new(multi_series_export_request(
        &["grpc_rate_limited"],
        now_ns(),
    ));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );

    let status = client
        .export(request)
        .await
        .expect_err("byte-rate breach must reject the request");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    running.shutdown().await.expect("graceful shutdown");
}

/// One `ExportLogsServiceRequest` carrying `scope_names.len()` distinct log
/// streams (distinct scope name, same resource), one `LogRecord` each.
/// `log_stream_id` hashes resource attrs + scope name/version/attrs
/// (docs/consistency-model.md, ADR-0029), so distinct scope names is the
/// simplest way to demand distinct stream ids in one request.
fn multi_stream_logs_export_request(scope_names: &[&str], ts_ns: i64) -> ExportLogsServiceRequest {
    let scope_logs = scope_names
        .iter()
        .map(|name| ScopeLogs {
            scope: Some(InstrumentationScope {
                name: name.to_string(),
                version: "1".to_string(),
                ..Default::default()
            }),
            log_records: vec![LogRecord {
                time_unix_nano: ts_ns as u64,
                observed_time_unix_nano: ts_ns as u64,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue("flood".to_string())),
                }),
                ..Default::default()
            }],
            schema_url: String::new(),
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "flood")],
                ..Default::default()
            }),
            scope_logs,
            ..Default::default()
        }],
    }
}

/// A single request demanding more fresh log streams than the tenant's
/// active-stream cap allows is admitted per-stream, not rejected whole
/// (ADR-0051 section 1, layer 4, the log-pipeline counterpart of
/// `fresh_series_flood_capped_per_tenant`): the response is still 200, and
/// `ExportLogsPartialSuccess.rejected_log_records` reports exactly the
/// stream count the cap turned away, distinguishing the ack from a fully
/// accepted batch.
#[tokio::test]
async fn fresh_log_streams_flood_capped_per_tenant() {
    const CAP: usize = 3;
    const FLOOD: usize = 10;

    let running = start_test_server_with_limits(AdmissionLimits {
        max_active_streams: CountLimit::Bounded(CAP as u64),
        ..AdmissionLimits::default()
    })
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let scope_names: Vec<String> = (0..FLOOD).map(|i| format!("flood_stream_{i}")).collect();
    let scope_name_refs: Vec<&str> = scope_names.iter().map(String::as_str).collect();
    let request = multi_stream_logs_export_request(&scope_name_refs, now_ns());
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request completes");

    assert_eq!(
        response.status(),
        200,
        "an active-stream-cap breach is a partial success, never a 4xx"
    );
    let body = response.bytes().await.expect("response body");
    let decoded = ExportLogsServiceResponse::decode(body.as_ref())
        .expect("response is an ExportLogsServiceResponse");
    let partial_success = decoded
        .partial_success
        .expect("cap breach must report a partial success");
    assert_eq!(
        partial_success.rejected_log_records,
        (FLOOD - CAP) as i64,
        "exactly the streams beyond the cap must be rejected: {partial_success:?}"
    );
    assert!(
        partial_success.error_message.contains("stream"),
        "the ack must name the drop reason, not just a bare count: {partial_success:?}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
