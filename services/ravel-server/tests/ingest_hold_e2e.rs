//! ADR-3718's flush-backlog head hold, wired into the real admission paths
//! (`services/ravel-server/src/ingest_admission.rs`): the hold happens right
//! after the in-flight permit, before tenant resolution, on both the OTLP
//! HTTP/Remote Write surface and the gRPC listener.
//!
//! These cases force the congested flag with
//! [`ravel_server::ingest_hold::IngestHoldGate::force_for_test`] (behind the
//! `ingest-hold-test-support` feature this crate's own dev-dependency turns
//! on) rather than drive a real flush backlog: the detector itself is
//! covered by `ingest_hold.rs`'s unit tests, and this file is only about
//! reachability from the transports down to the gate.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
use ravel_server::config::IngestHoldConfig;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const HOLD_MESSAGE: &str =
    "node flush backlog stayed over its bound for the ingest hold's max wait";
const IN_FLIGHT_SHED_MESSAGE: &str = "process in-flight ingest-request limit reached";
const ADMITTED: &str = "ravel_ingest_head_hold_total{mode=\"all\",outcome=\"admitted\"}";
const REFUSED: &str = "ravel_ingest_head_hold_total{mode=\"all\",outcome=\"refused\"}";
const HELD_COUNT: &str = "ravel_ingest_head_hold_seconds_count{mode=\"all\"}";
const CONGESTED: &str = "ravel_ingest_head_congested{mode=\"all\"}";

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
                attributes: vec![string_kv("service.name", "ingest-hold-test")],
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

/// A reqwest request body that trickles `payload` out over `chunks` pieces,
/// each delayed by `delay`. Used to prove a refused request's in-flight
/// permit is released before its body finishes draining: with the ceiling at
/// `Bounded(1)`, a concurrent second request can only be admitted while this
/// one's `discard_body` drain (`ingest_admission.rs`) is still running if the
/// permit was already freed.
fn slow_body(payload: Vec<u8>, chunks: usize, delay: Duration) -> reqwest::Body {
    let chunk_len = payload.len().div_ceil(chunks).max(1);
    let pieces: Vec<Vec<u8>> = payload.chunks(chunk_len).map(<[u8]>::to_vec).collect();
    reqwest::Body::wrap_stream(futures::stream::unfold(
        pieces.into_iter(),
        move |mut remaining| async move {
            let piece = remaining.next()?;
            tokio::time::sleep(delay).await;
            Some((Ok::<_, std::io::Error>(piece), remaining))
        },
    ))
}

/// Starts a full server with the given in-flight ceiling and hold max wait;
/// the congested flag is forced separately via
/// `running.ingest_hold.force_for_test`.
async fn start_test_server(
    store: Arc<dyn ObjectStoreBackend>,
    ingest_concurrency_limit: IngestConcurrencyLimit,
    max_wait: Duration,
) -> ravel_server::Running {
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
        ingest_hold: IngestHoldConfig {
            queued_flushes: None,
            max_wait,
        },
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit,
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

/// Scrapes `/metrics` and returns the raw text.
async fn scrape_metrics(client: &reqwest::Client, base: &str) -> String {
    client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// True when `body` declares `name`'s `# HELP` and `# TYPE` lines: proof the
/// family was actually rendered on this scrape, which a sample read
/// defaulting to 0 cannot tell apart from a family that was never emitted.
fn metric_family_registered(body: &str, name: &str) -> bool {
    let help = format!("# HELP {name} ");
    let kind = format!("# TYPE {name} ");
    body.lines().any(|line| line.starts_with(&help))
        && body.lines().any(|line| line.starts_with(&kind))
}

/// The value of the single sample named exactly `series` (name plus label
/// set). Panics when it is absent, so a missing series never reads as 0.
fn sample(body: &str, series: &str) -> u64 {
    let prefix = format!("{series} ");
    let values: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .collect();
    assert_eq!(values.len(), 1, "{series} renders exactly once");
    values[0]
        .parse()
        .unwrap_or_else(|_| panic!("{series} has an integer value: {}", values[0]))
}

async fn scrape_family_sum(client: &reqwest::Client, base: &str, name: &str) -> u64 {
    let body = scrape_metrics(client, base).await;
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

fn post_export(
    client: &reqwest::Client,
    base: &str,
    body: impl Into<reqwest::Body>,
) -> reqwest::RequestBuilder {
    client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
}

/// Forced congested with a 200 ms max wait, an authenticated OTLP/HTTP
/// export is held and then refused 429 with `Retry-After: 1`, no sooner than
/// 200 ms after it was sent.
///
/// The ceiling is `Bounded(1)` and the held request's body trickles in for
/// about 480 ms, so the two permit claims are read off concurrent requests:
/// one sent 100 ms into the hold is shed by the in-flight ceiling (the held
/// request still has the only slot), and one sent at 320 ms, after the
/// refusal but while the refused request's body is still draining, is
/// admitted (the refusal released the slot before the drain). An
/// implementation that released the permit at the start of the hold fails
/// the first; one that kept it through the drain fails the second.
#[tokio::test]
async fn forced_congestion_refuses_an_http_export_at_the_max_wait_and_releases_its_permit() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(
        store,
        IngestConcurrencyLimit::Bounded(1),
        Duration::from_millis(200),
    )
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.ingest_hold.force_for_test(Some(true));

    let slow_payload = metrics_export_request("held", now_ns()).encode_to_vec();
    let slow_client = client.clone();
    let slow_base = base.clone();
    let sent = Instant::now();
    let held_request = tokio::spawn(async move {
        let response = post_export(
            &slow_client,
            &slow_base,
            slow_body(slow_payload, 8, Duration::from_millis(60)),
        )
        .send()
        .await;
        (response, sent.elapsed())
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let during_hold = post_export(
        &client,
        &base,
        metrics_export_request("during_hold", now_ns()).encode_to_vec(),
    )
    .send()
    .await
    .expect("request completes");
    assert_eq!(during_hold.status(), 429);
    assert_eq!(
        during_hold.text().await.expect("body reads"),
        IN_FLIGHT_SHED_MESSAGE,
        "the held request keeps its in-flight permit, so the Bounded(1) \
         ceiling has no slot for this one"
    );

    tokio::time::sleep(Duration::from_millis(220)).await;
    running.ingest_hold.force_for_test(Some(false));
    let after_refusal = post_export(
        &client,
        &base,
        metrics_export_request("after_refusal", now_ns()).encode_to_vec(),
    )
    .send()
    .await
    .expect("request completes");
    assert_eq!(
        after_refusal.status(),
        200,
        "the refused request released its permit before draining its body"
    );

    let (held_response, elapsed) = held_request.await.expect("task does not panic");
    let held_response = held_response.expect("request completes");
    assert_eq!(held_response.status(), 429);
    // hygiene-allow: wall-clock -- a lower bound, not a band: the hold's
    // tokio sleeps cannot end before the max wait, so a loaded box only
    // lengthens it. The server answers on real sockets, so paused time
    // cannot drive this test.
    assert!(
        elapsed >= Duration::from_millis(200),
        "refused no sooner than the max wait, after {elapsed:?}"
    );
    assert_eq!(
        held_response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("1"),
    );
    assert_eq!(
        held_response.text().await.expect("body reads"),
        HOLD_MESSAGE
    );

    let body = scrape_metrics(&client, &base).await;
    assert_eq!(sample(&body, REFUSED), 1, "one request was refused");
    assert_eq!(
        sample(&body, ADMITTED),
        0,
        "the request admitted after the force was lifted was never held"
    );
    assert_eq!(sample(&body, HELD_COUNT), 1);
    assert_eq!(
        scrape_family_sum(&client, &base, "ravel_ingest_buffered_items_total").await,
        1,
        "the refused request's body is never decoded; only the admitted \
         request's point reached a shard buffer"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Forced congested, then cleared 100 ms later: the held export is admitted
/// and succeeds, counted as one admitted hold. The three families are
/// checked for their HELP and TYPE lines before any sample is read, and the
/// congested gauge follows the force.
#[tokio::test]
async fn congestion_cleared_during_the_hold_admits_the_export() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(
        store,
        IngestConcurrencyLimit::Unlimited,
        Duration::from_secs(1),
    )
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.ingest_hold.force_for_test(Some(true));
    let body = scrape_metrics(&client, &base).await;
    for family in [
        "ravel_ingest_head_hold_total",
        "ravel_ingest_head_hold_seconds",
        "ravel_ingest_head_congested",
    ] {
        assert!(
            metric_family_registered(&body, family),
            "{family} has HELP and TYPE lines"
        );
    }
    assert_eq!(sample(&body, CONGESTED), 1);
    assert_eq!(sample(&body, ADMITTED), 0);
    assert_eq!(sample(&body, REFUSED), 0);
    assert_eq!(sample(&body, HELD_COUNT), 0);

    let held_client = client.clone();
    let held_base = base.clone();
    let sent = Instant::now();
    let held_request = tokio::spawn(async move {
        let response = post_export(
            &held_client,
            &held_base,
            metrics_export_request("cleared", now_ns()).encode_to_vec(),
        )
        .send()
        .await;
        (response, sent.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    running.ingest_hold.force_for_test(Some(false));

    let (response, elapsed) = held_request.await.expect("task does not panic");
    assert_eq!(response.expect("request completes").status(), 200);
    // hygiene-allow: wall-clock -- a lower bound, not a band: the clear is
    // forced 100 ms after the send and a loaded box only delays the answer.
    // The server answers on real sockets, so paused time cannot drive it.
    assert!(
        elapsed >= Duration::from_millis(100),
        "held until the clear, answered after {elapsed:?}"
    );

    let body = scrape_metrics(&client, &base).await;
    assert_eq!(sample(&body, CONGESTED), 0);
    assert_eq!(sample(&body, ADMITTED), 1);
    assert_eq!(sample(&body, REFUSED), 0);
    assert_eq!(sample(&body, HELD_COUNT), 1);

    running.shutdown().await.expect("graceful shutdown");
}

/// The hold runs before tenant resolution: forced congested with no
/// credential at all, the request is refused 429, not 401. An implementation
/// that held after `state.tenant_resolver().resolve(...)` answers 401 here.
#[tokio::test]
async fn hold_refuses_before_tenant_resolution_unauthenticated() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(
        store,
        IngestConcurrencyLimit::Unlimited,
        Duration::from_millis(50),
    )
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.ingest_hold.force_for_test(Some(true));

    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(metrics_export_request("unauth", now_ns()).encode_to_vec())
        .send()
        .await
        .expect("request completes");
    assert_eq!(response.status(), 429);
    assert_eq!(response.text().await.expect("body reads"), HOLD_MESSAGE);

    running.shutdown().await.expect("graceful shutdown");
}

/// `--ingest-hold-max-wait 0` turns the gate off: forced congested, a
/// request is admitted at once and nothing is counted.
#[tokio::test]
async fn zero_max_wait_never_holds() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited, Duration::ZERO).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.ingest_hold.force_for_test(Some(true));

    let response = post_export(
        &client,
        &base,
        metrics_export_request("not_held", now_ns()).encode_to_vec(),
    )
    .send()
    .await
    .expect("request completes");
    assert_eq!(response.status(), 200);

    let body = scrape_metrics(&client, &base).await;
    assert_eq!(sample(&body, CONGESTED), 0, "an off gate never holds");
    assert_eq!(sample(&body, ADMITTED), 0);
    assert_eq!(sample(&body, REFUSED), 0);
    assert_eq!(sample(&body, HELD_COUNT), 0);

    running.shutdown().await.expect("graceful shutdown");
}

/// The gRPC half: forced congested, a unary OTLP export over the real gRPC
/// listener is refused `RESOURCE_EXHAUSTED` at the max wait, counted on the
/// same refused series.
#[tokio::test]
async fn hold_refuses_a_grpc_export_at_the_max_wait() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(
        store,
        IngestConcurrencyLimit::Unlimited,
        Duration::from_millis(200),
    )
    .await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    running.ingest_hold.force_for_test(Some(true));

    let mut grpc = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(metrics_export_request("grpc_held", now_ns()));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let sent = Instant::now();
    let status = grpc
        .export(request)
        .await
        .expect_err("a congested node refuses the export at the max wait");
    let elapsed = sent.elapsed();

    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(status.message(), HOLD_MESSAGE);
    // hygiene-allow: wall-clock -- a lower bound, as for the HTTP refusal:
    // a loaded box only lengthens the hold, and real sockets rule out
    // paused time.
    assert!(
        elapsed >= Duration::from_millis(200),
        "refused after {elapsed:?}"
    );
    let body = scrape_metrics(&client, &base).await;
    assert_eq!(sample(&body, REFUSED), 1);

    running.shutdown().await.expect("graceful shutdown");
}
