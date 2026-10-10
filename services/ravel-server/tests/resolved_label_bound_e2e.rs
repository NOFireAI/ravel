//! End-to-end coverage for the projected resolved-label bound (ADR-2708 D2,
//! issue #2710). Every OTLP signal, over HTTP and gRPC, is refused whole when
//! the labels its normalizer would build exceed
//! `max_resolved_label_bytes_per_request`, with the refusal reported as a
//! partial success, counted under `reason="resolved_label_bytes"`, and
//! nothing written. A request under the bound has its projection charged to
//! the ingest byte budget before normalization, so a small budget sheds it
//! with a 429.
//!
//! Every fixture stays under the 16 MiB body cap while projecting past the
//! 256 MiB default: the bound exists for exactly that amplification.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, IngestByteBudgetLimit, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

const HOUR_NS: i64 = 3_600 * 1_000_000_000;

/// The HTTP body cap (`MAX_REQUEST_BODY_BYTES` in `otlp_http.rs`).
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Classic histogram points in the over-bound metrics fixture. Each projects
/// about 46 MB (163 label sets of 64 maximum-size attributes), so six clear
/// the 256 MiB default and five do not.
const HISTOGRAM_POINTS: usize = 6;

/// Records (or spans) in the over-bound logs and traces fixtures. Each copies
/// about 1.1 MB of resource attributes, so 300 clear the 256 MiB default.
const STREAM_COPIES: usize = 300;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// `count` distinct attributes, each with a `key_len`-byte key and a
/// `value_len`-byte value.
fn wide_attributes(count: usize, key_len: usize, value_len: usize) -> Vec<KeyValue> {
    (0..count)
        .map(|i| {
            let prefix = format!("k{i:03}");
            let key = format!("{prefix}{}", "x".repeat(key_len - prefix.len()));
            string_kv(key, "v".repeat(value_len))
        })
        .collect()
}

/// `HISTOGRAM_POINTS` classic histograms at the default bucket cap (160
/// bounds), each carrying 64 attributes at the default key and value caps.
fn exploding_histogram_request() -> ExportMetricsServiceRequest {
    let limits = ravel_otlp::IngestLimits::default();
    let bounds: Vec<f64> = (1..=limits.max_histogram_buckets)
        .map(|b| b as f64)
        .collect();
    let ts = now_ns() as u64;
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "latency".to_string(),
                    data: Some(MetricData::Histogram(Histogram {
                        data_points: (0..HISTOGRAM_POINTS)
                            .map(|i| HistogramDataPoint {
                                attributes: wide_attributes(
                                    limits.max_attributes_per_point,
                                    limits.max_label_name_len,
                                    limits.max_label_value_len,
                                ),
                                time_unix_nano: ts + i as u64,
                                count: bounds.len() as u64 + 1,
                                sum: Some(1.0),
                                bucket_counts: vec![1; bounds.len() + 1],
                                explicit_bounds: bounds.clone(),
                                ..Default::default()
                            })
                            .collect(),
                        // AGGREGATION_TEMPORALITY_CUMULATIVE.
                        aggregation_temporality: 2,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// `STREAM_COPIES` small records under one resource carrying the maximum
/// number of maximum-size attributes: each record copies the stream.
fn wide_stream_log_request() -> ExportLogsServiceRequest {
    let limits = ravel_otlp::LogIngestLimits::default();
    let ts = now_ns() as u64;
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: wide_attributes(
                    limits.max_resource_attributes,
                    limits.max_attribute_key_len,
                    limits.max_attribute_value_len,
                ),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: (0..STREAM_COPIES)
                    .map(|i| LogRecord {
                        time_unix_nano: ts + i as u64,
                        observed_time_unix_nano: ts + i as u64,
                        severity_number: 9,
                        body: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue("hi".to_string())),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// `STREAM_COPIES` small spans under the same wide resource: each span merges
/// the resource attributes into its own.
fn wide_resource_span_request() -> ExportTraceServiceRequest {
    let limits = ravel_otlp::SpanIngestLimits::default();
    let end = now_ns();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: wide_attributes(
                    limits.max_resource_attributes,
                    limits.max_attribute_key_len,
                    limits.max_attribute_value_len,
                ),
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: (0..STREAM_COPIES)
                    .map(|i| Span {
                        trace_id: [0xa1; 16].to_vec(),
                        span_id: (i as u64 + 1).to_be_bytes().to_vec(),
                        name: "op".to_string(),
                        kind: 2,
                        start_time_unix_nano: (end - 1_000) as u64,
                        end_time_unix_nano: end as u64,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Points in the measured gauge shape: the default per-request point cap.
const GAUGE_POINTS: usize = 100_000;

/// The `--max-ingest-buffer-bytes` default (512 MiB).
const DEFAULT_INGEST_BUFFER_BYTES: u64 = 512 * 1024 * 1024;

/// The shape issue #2710 measured: one resource with nine resource labels of
/// maximum-size values (`job`, `instance` and the seven default allowlisted
/// keys), and one gauge of `GAUGE_POINTS` points whose single attribute
/// alternates between two values, so no two neighbours share a label set.
fn measured_gauge_request() -> ExportMetricsServiceRequest {
    let limits = ravel_otlp::IngestLimits::default();
    let wide = "r".repeat(limits.max_label_value_len);
    let mut attributes = vec![
        string_kv("service.name".to_string(), wide.clone()),
        string_kv("service.instance.id".to_string(), wide.clone()),
    ];
    assert_eq!(limits.resource_attribute_allowlist.len(), 7);
    attributes.extend(
        limits
            .resource_attribute_allowlist
            .iter()
            .map(|key| string_kv(key.clone(), wide.clone())),
    );
    let ts = now_ns() as u64;
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes,
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "g".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: (0..GAUGE_POINTS)
                            .map(|i| NumberDataPoint {
                                attributes: vec![string_kv(
                                    "k".to_string(),
                                    if i % 2 == 0 { "a" } else { "b" }.to_string(),
                                )],
                                time_unix_nano: ts + i as u64,
                                value: Some(NumberValue::AsDouble(i as f64)),
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Two gauge points three hours old (past the 2h default lag, so every point
/// is a skew rejection if normalization runs), with a 1 KiB attribute so the
/// projection is well above a 512-byte budget and far below the bound.
fn stale_gauge_request() -> ExportMetricsServiceRequest {
    let ts = (now_ns() - 3 * HOUR_NS) as u64;
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "temperature".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: (0..2)
                            .map(|i| NumberDataPoint {
                                attributes: wide_attributes(1, 8, 1024),
                                time_unix_nano: ts + i,
                                value: Some(NumberValue::AsDouble(i as f64)),
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A full in-process server over `MemoryStore` with the shipped OTLP limits
/// and the given ingest byte budget.
async fn start_server(
    budget: IngestByteBudgetLimit,
) -> (ravel_server::Running, Arc<dyn ObjectStoreBackend>) {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        ingest_buffer_budget_limit: budget,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    (running, store)
}

async fn post_protobuf(base: &str, path: &str, body: Vec<u8>) -> reqwest::Response {
    assert!(
        body.len() < MAX_BODY_BYTES,
        "fixture must fit the body cap: {} bytes",
        body.len()
    );
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export request completes")
}

/// The value of the `ravel_admission_rejected_total` sample for `signal` and
/// `reason`, or `None` when no such sample is rendered.
async fn rejected_sample(base: &str, signal: &str, reason: &str) -> Option<u64> {
    let body = reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .find(|line| {
            line.starts_with("ravel_admission_rejected_total{")
                && line.contains(&format!("signal=\"{signal}\""))
                && line.contains(&format!("reason=\"{reason}\""))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|v| v.parse().ok())
                .expect("numeric sample")
        })
}

/// Data objects and commit records the tenant has in the store. Shutdown
/// flushes every buffered write, so after it this is everything ingest wrote.
async fn stored_data_keys(store: &Arc<dyn ObjectStoreBackend>) -> Vec<String> {
    let prefix = format!("t/{}/", TenantId::new(TENANT).hash().to_hex());
    ravel_object_store::list_all(store.as_ref(), &prefix)
        .await
        .expect("list tenant objects")
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.contains("/l0/") || key.ends_with(".cmt"))
        .collect()
}

#[tokio::test]
async fn metrics_over_the_bound_are_rejected_whole_and_counted() {
    let request = exploding_histogram_request();
    let projected =
        ravel_otlp::project_resolved_label_bytes(&request, &ravel_otlp::IngestLimits::default());
    assert!(
        projected > ravel_otlp::DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST,
        "fixture must project past the default bound: {projected}"
    );

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/metrics", request.encode_to_vec()).await;
    assert_eq!(response.status(), 200);
    let decoded = ExportMetricsServiceResponse::decode(
        response.bytes().await.expect("response body").as_ref(),
    )
    .expect("metrics response");
    let partial = decoded
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_data_points, HISTOGRAM_POINTS as i64);
    assert!(
        partial.error_message.contains("resolved-label bytes"),
        "got: {}",
        partial.error_message
    );
    assert_eq!(
        rejected_sample(&base, "metrics", "resolved_label_bytes").await,
        Some(HISTOGRAM_POINTS as u64)
    );

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

#[tokio::test]
async fn logs_over_the_bound_are_rejected_whole_and_counted() {
    let request = wide_stream_log_request();
    let projected = ravel_otlp::project_log_resolved_label_bytes(
        &request,
        &ravel_otlp::LogIngestLimits::default(),
    );
    assert!(
        projected > ravel_otlp::DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST,
        "fixture must project past the default bound: {projected}"
    );

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/logs", request.encode_to_vec()).await;
    assert_eq!(response.status(), 200);
    let decoded =
        ExportLogsServiceResponse::decode(response.bytes().await.expect("response body").as_ref())
            .expect("logs response");
    let partial = decoded
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_log_records, STREAM_COPIES as i64);
    assert_eq!(
        rejected_sample(&base, "logs", "resolved_label_bytes").await,
        Some(STREAM_COPIES as u64)
    );

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

#[tokio::test]
async fn traces_over_the_bound_are_rejected_whole_and_counted() {
    let request = wide_resource_span_request();
    let projected = ravel_otlp::project_span_resolved_label_bytes(
        &request,
        &ravel_otlp::SpanIngestLimits::default(),
    );
    assert!(
        projected > ravel_otlp::DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST,
        "fixture must project past the default bound: {projected}"
    );

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/traces", request.encode_to_vec()).await;
    assert_eq!(response.status(), 200);
    let decoded =
        ExportTraceServiceResponse::decode(response.bytes().await.expect("response body").as_ref())
            .expect("traces response");
    let partial = decoded
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_spans, STREAM_COPIES as i64);
    assert_eq!(
        rejected_sample(&base, "spans", "resolved_label_bytes").await,
        Some(STREAM_COPIES as u64)
    );

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

/// The measured gauge shape at the server's default limits and the default
/// ingest buffer budget: a body of a few megabytes that would build gigabytes
/// of label sets is refused whole, and nothing is written.
#[tokio::test]
async fn the_measured_gauge_shape_is_rejected_at_default_limits() {
    let request = measured_gauge_request();
    let projected =
        ravel_otlp::project_resolved_label_bytes(&request, &ravel_otlp::IngestLimits::default());
    assert!(
        projected > ravel_otlp::DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST,
        "fixture must project past the default bound: {projected}"
    );
    let body = request.encode_to_vec();
    assert!(
        (2_000_000..4_000_000).contains(&body.len()),
        "encoded body: {} bytes",
        body.len()
    );

    let (running, store) =
        start_server(IngestByteBudgetLimit::Bounded(DEFAULT_INGEST_BUFFER_BYTES)).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/metrics", body).await;
    assert_eq!(response.status(), 200);
    let decoded = ExportMetricsServiceResponse::decode(
        response.bytes().await.expect("response body").as_ref(),
    )
    .expect("metrics response");
    let partial = decoded
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_data_points, GAUGE_POINTS as i64);
    assert_eq!(
        rejected_sample(&base, "metrics", "resolved_label_bytes").await,
        Some(GAUGE_POINTS as u64)
    );

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

/// The gRPC surface reaches the same handler: an over-bound metrics export is
/// an OK response carrying the partial success, not a status error.
#[tokio::test]
async fn grpc_metrics_over_the_bound_are_a_partial_success() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let grpc_addr = running.grpc_addr.expect("Mode::All binds gRPC");
    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(exploding_histogram_request());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let response = client
        .export(request)
        .await
        .expect("an over-bound export is OK with a partial success")
        .into_inner();
    let partial = response
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_data_points, HISTOGRAM_POINTS as i64);

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

/// An over-bound logs export over gRPC is an OK response carrying the partial
/// success.
#[tokio::test]
async fn grpc_logs_over_the_bound_are_a_partial_success() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let grpc_addr = running.grpc_addr.expect("Mode::All binds gRPC");
    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(wide_stream_log_request());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let response = client
        .export(request)
        .await
        .expect("an over-bound export is OK with a partial success")
        .into_inner();
    let partial = response
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_log_records, STREAM_COPIES as i64);

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

/// An over-bound traces export over gRPC is an OK response carrying the
/// partial success.
#[tokio::test]
async fn grpc_traces_over_the_bound_are_a_partial_success() {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;

    let (running, store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let grpc_addr = running.grpc_addr.expect("Mode::All binds gRPC");
    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(wide_resource_span_request());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let response = client
        .export(request)
        .await
        .expect("an over-bound export is OK with a partial success")
        .into_inner();
    let partial = response
        .partial_success
        .expect("the over-bound request is a partial success");
    assert_eq!(partial.rejected_spans, STREAM_COPIES as i64);

    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(stored_data_keys(&store).await, Vec::<String>::new());
}

/// A request inside the bound still has its projection charged to the ingest
/// byte budget before normalization. Its points are all stale, so once
/// normalized nothing reaches the router and the router charges nothing: the
/// 429 can only come from the projection charge, and the skew counter that
/// normalization would have moved stays at zero. The same request under an
/// unlimited budget is the control: a 200 with every point counted as skew.
#[tokio::test]
async fn a_projection_over_the_byte_budget_is_shed_with_429() {
    const BUDGET: u64 = 512;
    let projected = ravel_otlp::project_resolved_label_bytes(
        &stale_gauge_request(),
        &ravel_otlp::IngestLimits::default(),
    );
    assert!(
        projected as u64 > BUDGET
            && projected <= ravel_otlp::DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST,
        "fixture must fit the bound and not the budget: {projected}"
    );

    let (running, _store) = start_server(IngestByteBudgetLimit::Bounded(BUDGET)).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/metrics", stale_gauge_request().encode_to_vec()).await;
    assert_eq!(response.status(), 429);
    assert_eq!(
        rejected_sample(&base, "metrics", "skew").await.unwrap_or(0),
        0,
        "a shed request never reaches normalization"
    );
    running.shutdown().await.expect("graceful shutdown");

    let (running, _store) = start_server(IngestByteBudgetLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let response = post_protobuf(&base, "/v1/metrics", stale_gauge_request().encode_to_vec()).await;
    assert_eq!(response.status(), 200);
    let decoded = ExportMetricsServiceResponse::decode(
        response.bytes().await.expect("response body").as_ref(),
    )
    .expect("metrics response");
    assert_eq!(
        decoded.partial_success.map(|p| p.rejected_data_points),
        Some(2)
    );
    assert_eq!(rejected_sample(&base, "metrics", "skew").await, Some(2));
    running.shutdown().await.expect("graceful shutdown");
}
