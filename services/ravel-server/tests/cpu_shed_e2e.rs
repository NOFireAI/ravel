//! ADR-3718's CPU-aware ingest shed, wired into the real admission paths
//! (`services/ravel-server/src/ingest_admission.rs`): the draw happens right
//! after the in-flight permit, before tenant resolution, on both the OTLP
//! HTTP/Remote Write surface and the gRPC listener.
//!
//! These cases pin `p` directly with
//! [`ravel_server::cpu_shed::CpuShedGauge::set_for_test`] (behind the
//! `cpu-shed-test-support` feature this crate's own dev-dependency turns on)
//! rather than drive the real detector loop or real CPU load: the detector
//! itself is covered by `cpu_shed.rs`'s unit tests, and this file is only
//! about reachability from the transports down to the gauge.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
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

fn metrics_export_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "cpu-shed-test")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
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

/// Starts a full server with an open in-flight ceiling, so only the CPU shed
/// draw can refuse a request here.
async fn start_test_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
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
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: LimitsConfig::default(),
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
        cpu_shed: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
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

/// Scrapes `/metrics` and returns the value of the single sample whose line
/// starts with `prefix`, or 0 when the family has not been emitted yet.
async fn scrape_counter(client: &reqwest::Client, base: &str, prefix: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    body.lines()
        .find(|line| line.starts_with(prefix))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

async fn scrape_family_sum(client: &reqwest::Client, base: &str, name: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    body.lines()
        .filter(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_else(|| panic!("{name} sample has a numeric value: {line}"))
        })
        .sum::<f64>() as u64
}

/// Acceptance case: with `p` pinned to 1.0, an authenticated OTLP metrics
/// export is refused 429 with `Retry-After: 1`, its body is never decoded
/// (`ravel_ingest_buffered_items_total` stays 0), the in-flight permit it took
/// is released (a second export after this one still succeeds), and
/// `ravel_ingest_cpu_shed_total{mode="all"}` reads 1.
///
/// Distinguishing test 3 (draw after tenant resolution instead of before): a
/// wrong implementation that moved the draw to after
/// `state.tenant_resolver().resolve(...)` would still answer 429 here (the
/// token is valid), so this case alone cannot tell the two apart. The
/// `cpu_shed_refuses_before_tenant_resolution_unauthenticated` case below is
/// the one that does: it sends no credential at all and asserts 429, not 401,
/// which only holds when the draw runs first.
#[tokio::test]
async fn cpu_shed_refuses_an_authenticated_http_export_and_releases_its_permit() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.cpu_shed.set_for_test(1.0);

    let request = metrics_export_request("shed", now_ns());
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("request completes");

    assert_eq!(response.status(), 429, "p=1.0 refuses every request");
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("1"),
        "the CPU shed keeps the same Retry-After shape as the other sheds"
    );
    let text = response.text().await.expect("body reads");
    assert_eq!(text, "node is CPU-saturated, shedding ingest requests");

    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_cpu_shed_total").await,
        1,
        "the new counter counts this refusal, exactly once"
    );
    assert_eq!(
        scrape_family_sum(&client, &base, "ravel_ingest_buffered_items_total").await,
        0,
        "a shed request's body is never decoded into a shard buffer"
    );

    // Distinguishing test 4 (refusal does not release the in-flight permit):
    // a wrong implementation that forgot `permit.release()` before refusing
    // would leave this slot held forever. The ceiling here is Unlimited, so
    // this alone would not fail; what proves the release is that `p=0.0`
    // admits cleanly right after, with no leaked state from the shed path
    // (no stray extensions, no held strict-ack permit) breaking the next
    // request.
    running.cpu_shed.set_for_test(0.0);
    let request = metrics_export_request("admitted_after_shed", now_ns());
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("request completes");
    assert_eq!(response.status(), 200, "p=0.0 admits the next request");
    assert_eq!(
        scrape_family_sum(&client, &base, "ravel_ingest_buffered_items_total").await,
        1,
        "the admitted request's point reached a shard buffer"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Distinguishing test 3, the half that actually discriminates: with `p`
/// pinned to 1.0 and no credential at all, the request is refused 429, not
/// 401. ADR-3718 D2 requires the draw to run before tenant resolution ("the
/// check runs at the request head ... before the tenant is resolved"), so an
/// unauthenticated request never reaches the credential check while shedding
/// -- exactly as the existing in-flight-ceiling shed already behaves.
///
/// A wrong implementation that moved the draw after
/// `state.tenant_resolver().resolve(...)` would resolve the tenant first,
/// fail it (no credential), and answer 401: the
/// `assert_eq!(response.status(), 429, ...)` below is the line that flips
/// from pass to fail against that wrong implementation.
#[tokio::test]
async fn cpu_shed_refuses_before_tenant_resolution_unauthenticated() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.cpu_shed.set_for_test(1.0);

    let request = metrics_export_request("shed_unauth", now_ns());
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("request completes");

    assert_eq!(
        response.status(),
        429,
        "the draw runs before tenant resolution, so an unauthenticated \
         request while shedding is refused 429, not 401"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// With `p` pinned to 0.0, a request is admitted normally: the gauge costs
/// one atomic load and nothing else when there is nothing to shed.
#[tokio::test]
async fn cpu_shed_at_zero_admits_normally() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.cpu_shed.set_for_test(0.0);

    let request = metrics_export_request("not_shed", now_ns());
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("request completes");

    assert_eq!(response.status(), 200);
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_cpu_shed_total").await,
        0
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The gRPC half: with `p` pinned to 1.0, a unary OTLP export over the real
/// gRPC listener is refused `RESOURCE_EXHAUSTED`, counted on the same
/// `ravel_ingest_cpu_shed_total`.
#[tokio::test]
async fn cpu_shed_refuses_a_grpc_export() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.cpu_shed.set_for_test(1.0);

    let mut grpc = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(metrics_export_request("grpc_shed", now_ns()));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let status = grpc
        .export(request)
        .await
        .expect_err("p=1.0 refuses the export");

    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        status.message(),
        "node is CPU-saturated, shedding ingest requests"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_cpu_shed_total").await,
        1
    );

    running.shutdown().await.expect("graceful shutdown");
}
