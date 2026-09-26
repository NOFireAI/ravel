//! Admission ordering on the ingest surfaces: the process-wide in-flight
//! ceiling and tenant authentication both decide before a request body is
//! read or decoded (issue #1705).
//!
//! Every case here runs against a listener built by `ravel_server::start`,
//! not against a handler function called directly, because the defect lives
//! in the order the transport runs its extractors and layers in, which a
//! direct call cannot exhibit.
//!
//! The HTTP cases prove "the body was not read" structurally rather than by
//! timing: they open a raw TCP connection, write the request head with a
//! large `content-length`, send **zero** body bytes, and then read the
//! response. A server that reaches its body extractor before deciding has
//! nothing to respond with until those bytes arrive, so it never answers; a
//! server that refuses on the head alone answers immediately. The read
//! timeout is a test-failure guard, not the assertion.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use ravel_object_store::fault::{FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

/// Declared `content-length` for the head-only requests: large, but under the
/// 16 MiB `DefaultBodyLimit`, so the refusal under test is the admission or
/// authentication one and not a 413 the body limit would return from the
/// `content-length` alone.
const DECLARED_BODY_BYTES: usize = 15 * 1024 * 1024;

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

fn metrics_export_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "admission-order-test")],
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

/// Starts a full server against `store` with the process-wide in-flight
/// ingest ceiling set to `limit`. One shard, so every metrics write here
/// lands on the same shard actor and the same flush.
async fn start_test_server(
    store: Arc<dyn ObjectStoreBackend>,
    limit: IngestConcurrencyLimit,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
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
        ingest_concurrency_limit: limit,
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

/// The response head a server returned for a request whose declared body was
/// never sent.
struct HeadOnlyExchange {
    status_line: String,
    headers: Vec<(String, String)>,
    /// Bytes of the declared request body actually written to the socket.
    /// Always zero by construction: it is carried here so the assertion that
    /// the refusal happened without a body reads off the exchange itself.
    body_bytes_sent: usize,
}

impl HeadOnlyExchange {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Opens a raw connection to `addr`, writes an OTLP metrics request head
/// declaring `DECLARED_BODY_BYTES` of body, sends none of it, and reads back
/// whatever response head the server produces.
///
/// A server that runs its body extractor before deciding admission and
/// authentication cannot produce a response here at all, because the bytes it
/// is waiting for are never sent; the `timeout` below then fails the test.
async fn post_head_only(addr: SocketAddr, bearer: Option<&str>) -> HeadOnlyExchange {
    let mut head = format!(
        "POST /v1/metrics HTTP/1.1\r\n\
         host: {addr}\r\n\
         content-type: application/x-protobuf\r\n\
         content-length: {DECLARED_BODY_BYTES}\r\n"
    );
    if let Some(token) = bearer {
        head.push_str(&format!("authorization: Bearer {token}\r\n"));
    }
    head.push_str("\r\n");

    let mut stream = TcpStream::connect(addr).await.expect("connect to listener");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write request head");
    stream.flush().await.expect("flush request head");

    let mut raw = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(20), async {
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await.expect("read response");
            if n == 0 {
                return;
            }
            raw.extend_from_slice(&chunk[..n]);
            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                return;
            }
        }
    })
    .await;
    assert!(
        read.is_ok(),
        "the server answered nothing within 20s while zero body bytes had been sent, \
         so it was waiting on the request body before deciding"
    );

    let text = String::from_utf8_lossy(&raw).to_string();
    let mut lines = text.split("\r\n");
    let status_line = lines.next().unwrap_or_default().to_string();
    let headers = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect();

    HeadOnlyExchange {
        status_line,
        headers,
        body_bytes_sent: 0,
    }
}

/// Releases every currently held call on `gate`, repeatedly, until every
/// spawned task in `tasks` has completed.
async fn drain_until_done<T>(gate: &GateHandle, tasks: &[tokio::task::JoinHandle<T>]) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            for id in gate.held() {
                gate.release(id);
            }
            if tasks.iter().all(|t| t.is_finished()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("held requests drain within 30s");
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

/// Polls `/metrics` until `want` metric points have been admitted into shard
/// buffers: a request whose point is buffered has passed the in-flight
/// ceiling and is parked awaiting its flush's ack, so it holds a permit.
async fn wait_for_buffered_items(client: &reqwest::Client, base: &str, want: u64) -> u64 {
    let mut seen = 0;
    for _ in 0..2_000 {
        seen = scrape_counter(client, base, "ravel_ingest_buffered_items_total").await;
        if seen >= want {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    seen
}

/// Issue #1705, the named acceptance case. With the process-wide ceiling at
/// `LIMIT`, `LIMIT` concurrent authenticated exports are admitted and parked
/// mid-flush, saturating it. The `LIMIT + 1`th request is unauthenticated and
/// declares a 15 MiB body it never sends: it is refused 429 by the in-flight
/// ceiling on its head alone.
///
/// Three things are pinned, and each fails against the pre-#1705 code:
///
///   1. A response arrives at all while zero body bytes have been written.
///      Before the fix the handler's `Bytes` extractor ran first, so the
///      request sat in the extractor and no response was ever produced.
///   2. The status is exactly 429, not 401. Admission is decided before
///      authentication, so an unauthenticated request over the ceiling is
///      shed rather than rejected, and no credential check is reached.
///   3. `ravel_ingest_concurrency_shed_total` reads exactly 1 and
///      `ravel_ingest_buffered_items_total` is still exactly `LIMIT`: the
///      refused request was counted by the existing shed counter, and nothing
///      of its body was decoded into a shard buffer.
#[tokio::test]
async fn ingest_refuses_overflow_before_reading_the_body() {
    const LIMIT: u64 = 2;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(LIMIT)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let data_key_prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());
    let gate = fault.hold(Op::Put, Some(data_key_prefix), Occurrence::Always);

    let ts_ns = now_ns();
    let held_tasks: Vec<_> = (0..LIMIT)
        .map(|i| {
            let client = client.clone();
            let base = base.clone();
            let request = metrics_export_request(&format!("held_{i}"), ts_ns);
            tokio::spawn(async move {
                client
                    .post(format!("{base}/v1/metrics"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await
                    .expect("held export completes once released")
            })
        })
        .collect();

    let buffered = wait_for_buffered_items(&client, &base, LIMIT).await;
    assert_eq!(
        buffered, LIMIT,
        "every admitted request must have buffered its point, so all {LIMIT} permits are held"
    );
    assert!(
        held_tasks.iter().all(|t| !t.is_finished()),
        "the admitted requests must still be in flight, blocked on the held flush"
    );

    let exchange = post_head_only(running.http_addr, None).await;

    assert_eq!(
        exchange.body_bytes_sent, 0,
        "the refused request declared {DECLARED_BODY_BYTES} bytes of body and sent none"
    );
    assert_eq!(
        exchange.status_line, "HTTP/1.1 429 Too Many Requests",
        "the request over the ceiling must be shed 429 from its head alone"
    );
    assert_eq!(
        exchange.header("retry-after"),
        Some("1"),
        "the shed response keeps its Retry-After"
    );
    assert_eq!(
        exchange.header("x-ravel-commit-token"),
        None,
        "a shed request never buffered anything, so it carries no commit token"
    );

    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        1,
        "the existing shed counter counts this refusal, exactly once"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_buffered_items_total").await,
        LIMIT,
        "no part of the refused request's body was decoded into a shard buffer"
    );

    drain_until_done(&gate, &held_tasks).await;
    for task in held_tasks {
        let response = task.await.expect("task joins");
        assert_eq!(
            response.status(),
            200,
            "an admitted request must succeed once released"
        );
    }

    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #1705, the authentication half: a request carrying no credentials is
/// refused 401 on its head, with none of its declared 15 MiB body read.
///
/// The ceiling is wide open here, so nothing but the credential check can
/// produce the refusal, and the 401 body/status the pre-#1705 handler
/// returned is unchanged.
#[tokio::test]
async fn ingest_refuses_unauthenticated_before_reading_the_body() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited).await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", running.http_addr);

    let exchange = post_head_only(running.http_addr, None).await;

    assert_eq!(
        exchange.body_bytes_sent, 0,
        "the refused request declared {DECLARED_BODY_BYTES} bytes of body and sent none"
    );
    assert_eq!(
        exchange.status_line, "HTTP/1.1 401 Unauthorized",
        "a request without valid credentials is refused 401 from its head alone"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        0,
        "a 401 is not a shed: the ceiling was never reached"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_buffered_items_total").await,
        0,
        "no part of the unauthenticated request's body was decoded into a shard buffer"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #1705, the gRPC half: with the same process-wide ceiling saturated
/// by in-flight HTTP exports, a gRPC export over it is refused
/// `RESOURCE_EXHAUSTED` with the shared shed message, by the admission layer
/// that runs before tonic decodes the request message.
///
/// The permit is taken by `GrpcIngestAdmissionLayer` on the request head;
/// that the layer refuses without ever polling the request body is pinned by
/// its own unit test in `services/ravel-server/src/ingest_admission.rs`,
/// which is the only place that property is observable. What this case pins
/// is that the layer is wired onto the production listener `ravel_server::start`
/// builds, and that its refusal reaches a real client as the documented status.
#[tokio::test]
async fn grpc_refuses_overflow_with_resource_exhausted() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    const LIMIT: u64 = 2;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(LIMIT)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let data_key_prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());
    let gate = fault.hold(Op::Put, Some(data_key_prefix), Occurrence::Always);

    let ts_ns = now_ns();
    let held_tasks: Vec<_> = (0..LIMIT)
        .map(|i| {
            let client = client.clone();
            let base = base.clone();
            let request = metrics_export_request(&format!("held_{i}"), ts_ns);
            tokio::spawn(async move {
                client
                    .post(format!("{base}/v1/metrics"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await
                    .expect("held export completes once released")
            })
        })
        .collect();

    let buffered = wait_for_buffered_items(&client, &base, LIMIT).await;
    assert_eq!(
        buffered, LIMIT,
        "every admitted request must have buffered its point, so all {LIMIT} permits are held"
    );

    let mut grpc = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut over_limit = tonic::Request::new(metrics_export_request("shed", ts_ns));
    over_limit.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let status = grpc
        .export(over_limit)
        .await
        .expect_err("a gRPC export over the in-flight ceiling must be refused");

    assert_eq!(
        status.code(),
        tonic::Code::ResourceExhausted,
        "gRPC overflow is RESOURCE_EXHAUSTED"
    );
    assert_eq!(
        status.message(),
        "process in-flight ingest-request limit reached",
        "the refusal carries the shared shed message, the same string HTTP's 429 body uses"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        1,
        "the existing shed counter counts the gRPC refusal too, exactly once"
    );

    drain_until_done(&gate, &held_tasks).await;
    for task in held_tasks {
        let response = task.await.expect("task joins");
        assert_eq!(response.status(), 200);
    }

    running.shutdown().await.expect("graceful shutdown");
}
