//! End-to-end coverage for the configurable ingest-lag bound (issue #1682,
//! ADR-0051 section 4): a `--max-ingest-lag` value raised above the shipped 2h
//! must reach every OTLP admission surface AND the catalog listing window, so a
//! deployment can replay telemetry older than 2h after an outage or a bulk
//! import. The three signal limits are threaded separately, so there is one
//! admit test per signal; a miss on one is invisible from the others.

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
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
const DAY_NS: i64 = 24 * HOUR_NS;

/// A 720h (30-day) window, the ticket's replay bound. Well above the 2h default.
const REPLAY_LAG: Duration = Duration::from_secs(720 * 3_600);

/// The age of the "old" point every replay test admits. 29 days sits
/// comfortably inside the 720h (30-day) window with margin for the processing
/// delay between building the request and the server reading its admission
/// clock, and is far outside the 2h default the current tree rejects.
const OLD_POINT_AGE_NS: i64 = 29 * DAY_NS;

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

fn metric_export_request(name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "replay")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
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

fn log_export_request(body: &str, ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "replay")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(body.to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    }
}

fn span_export_request(name: &str, end_ts_ns: i64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "replay")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "tracer".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: [0xa1; 16].to_vec(),
                    span_id: [0xb2; 8].to_vec(),
                    name: name.to_string(),
                    kind: 2,
                    // Both edges anchor on the end (ADR-0051); place the span so
                    // its end lags ingest by `OLD_POINT_AGE_NS`.
                    start_time_unix_nano: (end_ts_ns - SECOND_NS) as u64,
                    end_time_unix_nano: end_ts_ns as u64,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    }
}

/// Start a full in-process server backed by `MemoryStore`, with `max_ingest_lag`
/// set to `max_ingest_lag`. Everything else is the shipped default.
async fn start_server(max_ingest_lag: Duration) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
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
        max_ingest_lag,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
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

async fn post_protobuf(base: &str, path: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export request completes")
}

/// A server started with `--max-ingest-lag 720h` admits an OTLP metric point ~30
/// days old that the 2h default would reject. The admitted value is asserted:
/// `rejected_data_points` is exactly zero, not merely a 200 status (a TooOld
/// rejection is itself a 200 partial success).
#[tokio::test]
async fn replay_window_admits_old_metric_point() {
    let running = start_server(REPLAY_LAG).await;
    let base = format!("http://{}", running.http_addr);
    let ts = now_ns() - OLD_POINT_AGE_NS;

    let response = post_protobuf(
        &base,
        "/v1/metrics",
        metric_export_request("replay_metric", ts).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), 200);
    let decoded = ExportMetricsServiceResponse::decode(
        response.bytes().await.expect("response body").as_ref(),
    )
    .expect("metrics response");
    let rejected = decoded
        .partial_success
        .map_or(0, |p| p.rejected_data_points);
    assert_eq!(
        rejected, 0,
        "a ~30-day-old metric point must be admitted under a 720h ingest lag"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The same replay window on the logs surface. The logs limit is a separate
/// struct threaded separately, so it gets its own admit test.
#[tokio::test]
async fn replay_window_admits_old_log_record() {
    let running = start_server(REPLAY_LAG).await;
    let base = format!("http://{}", running.http_addr);
    let ts = now_ns() - OLD_POINT_AGE_NS;

    let response = post_protobuf(
        &base,
        "/v1/logs",
        log_export_request("replay log", ts).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), 200);
    let decoded =
        ExportLogsServiceResponse::decode(response.bytes().await.expect("response body").as_ref())
            .expect("logs response");
    let rejected = decoded
        .partial_success
        .map_or(0, |p| p.rejected_log_records);
    assert_eq!(
        rejected, 0,
        "a ~30-day-old log record must be admitted under a 720h ingest lag"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The same replay window on the spans surface, its own separately-threaded
/// limit struct.
#[tokio::test]
async fn replay_window_admits_old_span() {
    let running = start_server(REPLAY_LAG).await;
    let base = format!("http://{}", running.http_addr);
    let end_ts = now_ns() - OLD_POINT_AGE_NS;

    let response = post_protobuf(
        &base,
        "/v1/traces",
        span_export_request("replay_span", end_ts).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), 200);
    let decoded =
        ExportTraceServiceResponse::decode(response.bytes().await.expect("response body").as_ref())
            .expect("traces response");
    let rejected = decoded.partial_success.map_or(0, |p| p.rejected_spans);
    assert_eq!(
        rejected, 0,
        "a span whose end is ~30 days old must be admitted under a 720h ingest lag"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// With no `--max-ingest-lag` (the shipped 2h default), a point 3h old is still
/// rejected as `Rejection::TooOld`, so raising the flag is the only thing that
/// changes admission and an unset deployment is unchanged.
#[tokio::test]
async fn default_still_rejects_a_three_hour_old_point() {
    let running = start_server(ravel_server::DEFAULT_MAX_INGEST_LAG).await;
    let base = format!("http://{}", running.http_addr);
    let ts = now_ns() - 3 * HOUR_NS;

    let response = post_protobuf(
        &base,
        "/v1/metrics",
        metric_export_request("too_old_metric", ts).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), 200);
    let decoded = ExportMetricsServiceResponse::decode(
        response.bytes().await.expect("response body").as_ref(),
    )
    .expect("metrics response");
    let partial = decoded
        .partial_success
        .expect("a 3h-old point must be a partial success under the 2h default");
    assert_eq!(
        partial.rejected_data_points, 1,
        "exactly the one too-old point is rejected"
    );
    assert!(
        partial.error_message.contains("behind ingest time")
            && partial.error_message.contains("max ingest lag"),
        "the rejection must be Rejection::TooOld, got: {}",
        partial.error_message
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The catalog listing window moves with the flag: `build_catalog` given the
/// resolved window builds a `CatalogConfig` carrying exactly that value. Asserts
/// the window's own value on the built catalog, not a downstream query effect.
#[test]
fn catalog_listing_window_moves_with_the_flag() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let window_ns = ravel_server::resolve_ingest_lag(REPLAY_LAG)
        .expect("pair resolves")
        .catalog_window_ns;
    assert_eq!(
        window_ns,
        720 * HOUR_NS,
        "720h resolves to the listing window"
    );

    let catalog = ravel_server::query::build_catalog(
        Arc::clone(&store),
        1,
        false,
        ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        None,
        None,
        Some(window_ns),
    )
    .expect("catalog builds");
    assert_eq!(
        catalog.config().max_ingest_lag_ns,
        window_ns,
        "the configured window must reach the catalog it is built with"
    );

    // With no override the catalog keeps its own 2h default, so an unset
    // deployment is unchanged.
    let default_catalog = ravel_server::query::build_catalog(
        store,
        1,
        false,
        ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        None,
        None,
        None,
    )
    .expect("catalog builds");
    assert_eq!(
        default_catalog.config().max_ingest_lag_ns,
        ravel_catalog::DEFAULT_MAX_INGEST_LAG_NS,
    );
}

/// Startup refuses an ingest-lag pair whose admission bound exceeds the catalog
/// listing window, with both values named in the typed error. The production
/// path always feeds equal values, so the guard is exercised through
/// `validate_ingest_lag_window` directly.
#[test]
fn startup_refuses_an_inconsistent_pair() {
    let window_ns = 2 * HOUR_NS;
    let admission_ns = 720 * HOUR_NS;
    let err = ravel_server::validate_ingest_lag_window(window_ns, admission_ns)
        .expect_err("admission bound above the window must be refused");
    assert_eq!(err.catalog_window_ns, window_ns);
    assert_eq!(err.admission_lag_ns, admission_ns);
    let message = err.to_string();
    assert!(
        message.contains(&window_ns.to_string()) && message.contains(&admission_ns.to_string()),
        "both values must appear in the error: {message}"
    );

    // Equal values (the production path) pass.
    ravel_server::validate_ingest_lag_window(admission_ns, admission_ns)
        .expect("a consistent pair is accepted");
}

/// The server-side default cannot drift from the catalog listing window's own
/// default or from the three OTLP limit defaults: one raised without the others
/// reintroduces exactly the coordination bug this feature removes.
#[test]
fn default_max_ingest_lag_matches_every_signal_and_the_catalog() {
    let server_ns = ravel_server::resolve_ingest_lag(ravel_server::DEFAULT_MAX_INGEST_LAG)
        .expect("default resolves")
        .admission_lag_ns;
    assert_eq!(server_ns, ravel_catalog::DEFAULT_MAX_INGEST_LAG_NS);
    assert_eq!(
        server_ns,
        ravel_otlp::IngestLimits::default().max_ingest_lag_ns
    );
    assert_eq!(
        server_ns,
        ravel_otlp::LogIngestLimits::default().max_ingest_lag_ns
    );
    assert_eq!(
        server_ns,
        ravel_otlp::SpanIngestLimits::default().max_ingest_lag_ns
    );
}
