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
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
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
        max_pending_strict_acks: 8_192,
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
    /// Zero for [`post_head_only`] by construction: it is carried here so the
    /// assertion that the refusal happened without a body reads off the
    /// exchange itself.
    body_bytes_sent: usize,
    /// From before the connection opened to the end of the response head, so
    /// it is never shorter than the server's own time to answer.
    elapsed: Duration,
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
    let head = request_head(
        addr,
        "/v1/metrics",
        bearer,
        &format!("content-length: {DECLARED_BODY_BYTES}"),
    );
    exchange(addr, &head, &[], Duration::from_secs(20)).await
}

/// An HTTP/1.1 POST head for `path`, framed by `framing` (a `content-length`
/// or `transfer-encoding` header line).
fn request_head(addr: SocketAddr, path: &str, bearer: Option<&str>, framing: &str) -> String {
    let mut head = format!(
        "POST {path} HTTP/1.1\r\n\
         host: {addr}\r\n\
         content-type: application/x-protobuf\r\n\
         {framing}\r\n"
    );
    if let Some(token) = bearer {
        head.push_str(&format!("authorization: Bearer {token}\r\n"));
    }
    head.push_str("\r\n");
    head
}

/// Opens a raw connection to `addr`, writes `head` and then `body_prefix`,
/// sends nothing more, and reads back the response head the server produces
/// within `guard`.
async fn exchange(
    addr: SocketAddr,
    head: &str,
    body_prefix: &[u8],
    guard: Duration,
) -> HeadOnlyExchange {
    let started = tokio::time::Instant::now();
    let mut stream = TcpStream::connect(addr).await.expect("connect to listener");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write request head");
    stream
        .write_all(body_prefix)
        .await
        .expect("write body prefix");
    stream.flush().await.expect("flush request");

    let mut raw = Vec::new();
    let read = tokio::time::timeout(guard, async {
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
    let elapsed = started.elapsed();
    assert!(
        read.is_ok(),
        "the server answered nothing within {guard:?} while {} body bytes had been sent, \
         so it was waiting on the rest of the request body before answering",
        body_prefix.len()
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
        body_bytes_sent: body_prefix.len(),
        elapsed,
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

/// Scrapes `/metrics` and sums every sample of the family `name`, whatever
/// its labels, so a per-signal family reads as one total.
async fn scrape_family_sum(client: &reqwest::Client, base: &str, name: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    let samples: Vec<f64> = body
        .lines()
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
        .collect();
    // A per-tenant family has no sample until a tenant is charged, so the
    // family's presence is read off its TYPE line.
    assert!(
        body.lines().any(|line| line
            .strip_prefix("# TYPE ")
            .and_then(|rest| rest.strip_prefix(name))
            .is_some_and(|rest| rest.starts_with(' '))),
        "{name} is on /metrics:\n{body}"
    );
    samples.iter().sum::<f64>() as u64
}

/// The `ravel_ingest_head_refusal_undrained_total` sample for `reason` on the
/// real `/metrics` surface. Panics when the sample is absent: every reason is
/// rendered from process start, zero included.
async fn undrained(client: &reqwest::Client, base: &str, reason: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    let prefix =
        format!("ravel_ingest_head_refusal_undrained_total{{mode=\"all\",reason=\"{reason}\"}} ");
    let samples: Vec<u64> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.parse::<u64>().expect("counter value is an integer"))
        .collect();
    assert_eq!(
        samples.len(),
        1,
        "exactly one {prefix}sample on /metrics:\n{body}"
    );
    samples[0]
}

/// Holds every read of the tenant's metrics provisioning record. The first
/// write of a (tenant, signal) in a process reads that record after the
/// request was admitted and before its points are enqueued, so a request
/// parked on this gate holds its in-flight permit.
fn hold_provisioning_reads(fault: &FaultStore<MemoryStore>) -> GateHandle {
    let key = format!("t/{}/m/prov", TenantId::new(TENANT).hash().to_hex());
    fault.hold(Op::Get, Some(key), Occurrence::Always)
}

/// Waits until `want` admitted requests are parked on `gate`, each holding
/// one of the in-flight permits.
async fn wait_for_parked(gate: &GateHandle, want: usize) {
    tokio::time::timeout(Duration::from_secs(10), gate.wait_until_held(want))
        .await
        .expect("the admitted requests reach the provisioning read");
    assert_eq!(
        gate.held_count(),
        want,
        "exactly the admitted requests are parked: {:?}",
        gate.held_details()
    );
}

/// Issue #1705, the named acceptance case. With the process-wide ceiling at
/// `LIMIT`, `LIMIT` concurrent authenticated exports are admitted and parked
/// before enqueue on a held provisioning read, saturating it. The `LIMIT + 1`th request is unauthenticated and
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
///      shed rather than rejected. The shed path does run the credential
///      check, only to decide whether to drain the body, and with no side
///      effect: it answers no 401, and for an unauthenticated shed like this
///      one the body is not read (issue #2926).
///   3. `ravel_ingest_concurrency_shed_total` reads exactly 1 and
///      `ravel_ingest_buffered_items_total` is still 0: the refused request
///      was counted by the existing shed counter, and nothing of its body was
///      decoded into a shard buffer. It also counts once under
///      `ravel_ingest_head_refusal_undrained_total{reason="unauthenticated"}`.
#[tokio::test]
async fn ingest_refuses_overflow_before_reading_the_body() {
    const LIMIT: u64 = 2;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(LIMIT)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let gate = hold_provisioning_reads(&fault);

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

    wait_for_parked(&gate, LIMIT as usize).await;
    assert!(
        held_tasks.iter().all(|t| !t.is_finished()),
        "the admitted requests must still be in flight, parked before enqueue"
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
        0,
        "the admitted requests are parked before enqueue, and no part of the refused \
         request's body was decoded into a shard buffer"
    );
    assert_eq!(
        exchange.header("connection"),
        Some("close"),
        "an unauthenticated shed leaves its body unread and says the connection closes"
    );
    assert_eq!(undrained(&client, &base, "unauthenticated").await, 1);

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

/// The four HTTP ingest routes behind the admission layer.
const HTTP_INGEST_ROUTES: [&str; 4] = ["/v1/metrics", "/v1/logs", "/v1/traces", "/api/v1/write"];

/// `len` bytes from a xorshift stream: incompressible enough that no layer
/// could shrink them, and deterministic so a failure replays.
fn incompressible_body(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// An OTLP metrics export of about `len` encoded bytes that the metrics
/// handler would decode and buffer: one gauge whose points carry xorshift
/// timestamps within the last hour and xorshift values, so most of its bytes
/// are as incompressible as [`incompressible_body`]'s.
fn decodable_metrics_body(len: usize, seed: u64) -> Vec<u8> {
    // Each point encodes as a 2-byte message header, a 9-byte fixed64
    // timestamp and a 9-byte double.
    const POINT_BYTES: usize = 20;
    let now = now_ns() as u64;
    let mut state = seed | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut request = metrics_export_request("head_refusal_drain", now as i64);
    let points = (0..len / POINT_BYTES)
        .map(|_| NumberDataPoint {
            time_unix_nano: now - next() % 3_600_000_000_000,
            value: Some(NumberValue::AsDouble(f64::from_bits(
                (next() >> 12) | 0x3ff0_0000_0000_0000,
            ))),
            ..Default::default()
        })
        .collect();
    if let Some(MetricData::Gauge(gauge)) =
        &mut request.resource_metrics[0].scope_metrics[0].metrics[0].data
    {
        gauge.data_points = points;
    }
    request.encode_to_vec()
}

/// One request body per HTTP ingest route: a decodable export for
/// `/v1/metrics`, so a refusal path that decoded or charged a drained body
/// would buffer its points, and `len` xorshift bytes for the other three.
fn route_body(route: &str, len: usize) -> bytes::Bytes {
    if route == "/v1/metrics" {
        let body = decodable_metrics_body(len, 0x2926);
        assert!(
            body.len().abs_diff(len) < len / 20,
            "the decodable body is within 5% of {len} bytes: {}",
            body.len()
        );
        bytes::Bytes::from(body)
    } else {
        bytes::Bytes::from(incompressible_body(len, 0x2926))
    }
}

/// Issue #2926, the acceptance case. With the in-flight ceiling at zero every
/// ingest request is shed, so a pooled HTTP/1.1 client sending 512 KiB bodies
/// eight at a time to each of the four HTTP ingest routes sees nothing but
/// shed refusals. Every one must arrive as a readable 429 with its
/// Retry-After: a refusal written while the client is still sending, on a
/// connection the server then closes with the body unread, reaches the
/// client as a reset or a broken pipe instead, and the transport-error count
/// below is that failure. No 429 carries `Connection: close`: a drained
/// refusal keeps the pooled connection open. That assertion, not the error
/// count, is what tells a drain from a close-only answer here, because on
/// loopback a 512 KiB body fits in the socket buffers and the client reads
/// the 429 before the close reaches it.
///
/// The drained bodies are never decoded and never charged (case F): the
/// `/v1/metrics` bodies are valid exports, yet no point reaches a shard
/// buffer, no byte is admitted to one, no wire byte is counted, nothing is
/// charged to the byte-rate layer, and the ingest byte-budget gauge reads 0
/// afterwards. Every refusal was drained, so
/// no reason of `ravel_ingest_head_refusal_undrained_total` moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pooled_client_sees_every_shed_refusal_as_a_readable_429() {
    use futures::StreamExt;

    const PER_ROUTE: usize = 400;
    const IN_FLIGHT: usize = 8;
    const BODY_BYTES: usize = 512 * 1024;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::builder()
        .http1_only()
        .build()
        .expect("client builds");

    let mut sent = 0u64;
    let mut transport_errors = Vec::new();
    for route in HTTP_INGEST_ROUTES {
        let body = route_body(route, BODY_BYTES);
        let outcomes: Vec<_> = futures::stream::iter(0..PER_ROUTE)
            .map(|_| {
                let request = client
                    .post(format!("{base}{route}"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/x-protobuf")
                    .body(body.clone());
                async move {
                    let response = request.send().await?;
                    let status = response.status();
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    let connection = response
                        .headers()
                        .get("connection")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    let text = response.text().await?;
                    Ok::<_, reqwest::Error>((status, retry_after, connection, text))
                }
            })
            .buffer_unordered(IN_FLIGHT)
            .collect()
            .await;
        for outcome in outcomes {
            sent += 1;
            match outcome {
                Ok((status, retry_after, connection, text)) => {
                    assert_eq!(status, 429, "{route}: every request is shed: {text}");
                    assert_eq!(retry_after.as_deref(), Some("1"), "{route}: Retry-After");
                    assert_eq!(
                        connection, None,
                        "{route}: a drained refusal leaves the pooled connection open"
                    );
                    assert_eq!(text, "process in-flight ingest-request limit reached");
                }
                Err(error) => transport_errors.push(format!("{route}: {error:?}")),
            }
        }
    }

    assert!(
        transport_errors.is_empty(),
        "{} of {sent} shed requests reached the client as a transport error, first: {:?}",
        transport_errors.len(),
        transport_errors.first()
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        sent,
        "every request was shed exactly once, so no transport-level retry resent one"
    );

    for family in [
        "ravel_ingest_buffered_items_total",
        "ravel_ingest_buffered_bytes_total",
        "ravel_ingest_wire_bytes_total",
        "ravel_admission_admitted_bytes_total",
        "ravel_ingest_buffer_bytes",
    ] {
        assert_eq!(
            scrape_family_sum(&client, &base, family).await,
            0,
            "{family}: a drained body is never decoded or charged"
        );
    }
    for reason in ["unauthenticated", "over_cap", "busy", "timeout"] {
        assert_eq!(
            undrained(&client, &base, reason).await,
            0,
            "every refusal was drained, none left undrained for {reason}"
        );
    }

    running.shutdown().await.expect("graceful shutdown");
}

/// Within this the server answered on the head, without waiting out the
/// one-second drain timeout.
const PROMPT: Duration = Duration::from_millis(500);

/// Issue #2926, case B: only an authenticated caller's body is drained.
///
/// (i) With the ceiling free, a request with no credential is refused 401 at
/// once and the 401 is answered as before, with no `Connection` header.
/// (ii) With the ceiling held, the same request is shed 429 at once, with
/// `Connection: close`, and counted under `unauthenticated`. Neither reads a
/// byte of the 512 KiB the head declares.
#[tokio::test]
async fn only_an_authenticated_shed_drains_its_body() {
    let declared = format!("content-length: {}", 512 * 1024);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited).await;
    let head = request_head(running.http_addr, "/v1/metrics", None, &declared);
    let refused = exchange(running.http_addr, &head, &[], Duration::from_secs(10)).await;
    assert_eq!(refused.status_line, "HTTP/1.1 401 Unauthorized");
    assert_eq!(
        refused.header("connection"),
        None,
        "the 401 is answered exactly as before"
    );
    // hygiene-allow: wall-clock -- the claim is that the refusal does not
    // wait out the 1 s drain timeout; the counters are the exact half.
    assert!(refused.elapsed < PROMPT, "401 after {:?}", refused.elapsed);
    running.shutdown().await.expect("graceful shutdown");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let head = request_head(running.http_addr, "/v1/metrics", None, &declared);
    let shed = exchange(running.http_addr, &head, &[], Duration::from_secs(10)).await;
    assert_eq!(shed.status_line, "HTTP/1.1 429 Too Many Requests");
    assert_eq!(shed.header("retry-after"), Some("1"));
    assert_eq!(shed.header("connection"), Some("close"));
    // hygiene-allow: wall-clock -- as above.
    assert!(shed.elapsed < PROMPT, "429 after {:?}", shed.elapsed);
    assert_eq!(undrained(&client, &base, "unauthenticated").await, 1);
    assert_eq!(undrained(&client, &base, "timeout").await, 0);
    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #2926, case C: an authenticated shed whose `Content-Length` is
/// above the 1 MiB drain cap is answered at once, without reading, with
/// `Connection: close`, and counted under `over_cap`.
#[tokio::test]
async fn an_authenticated_shed_over_the_cap_is_answered_without_a_drain() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let head = request_head(
        running.http_addr,
        "/v1/metrics",
        Some(TOKEN),
        &format!("content-length: {}", 2 * 1024 * 1024),
    );
    let shed = exchange(running.http_addr, &head, &[], Duration::from_secs(10)).await;

    assert_eq!(shed.status_line, "HTTP/1.1 429 Too Many Requests");
    assert_eq!(shed.header("retry-after"), Some("1"));
    assert_eq!(shed.header("connection"), Some("close"));
    // hygiene-allow: wall-clock -- the claim is that the refusal does not
    // wait out the 1 s drain timeout; the counters are the exact half.
    assert!(shed.elapsed < PROMPT, "429 after {:?}", shed.elapsed);
    assert_eq!(undrained(&client, &base, "over_cap").await, 1);
    assert_eq!(undrained(&client, &base, "timeout").await, 0);
    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #2926, case D: an authenticated shed whose chunked body sends 64 KiB
/// and stalls is answered once the 1 s drain timeout passes, with
/// `Connection: close`, and counted under `timeout`. The answer comes well
/// before the 30 s body-read timeout an admitted request gets.
#[tokio::test]
async fn a_stalled_authenticated_shed_is_answered_at_the_drain_timeout() {
    use ravel_server::ingest_admission::HEAD_REFUSAL_DRAIN_TIMEOUT;

    const GUARD: Duration = Duration::from_secs(10);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let head = request_head(
        running.http_addr,
        "/v1/metrics",
        Some(TOKEN),
        "transfer-encoding: chunked",
    );
    let chunk = incompressible_body(64 * 1024, 0x2926);
    let mut first_chunk = format!("{:x}\r\n", chunk.len()).into_bytes();
    first_chunk.extend_from_slice(&chunk);
    first_chunk.extend_from_slice(b"\r\n");
    let shed = exchange(running.http_addr, &head, &first_chunk, GUARD).await;

    assert_eq!(shed.status_line, "HTTP/1.1 429 Too Many Requests");
    assert_eq!(shed.header("retry-after"), Some("1"));
    assert_eq!(shed.header("connection"), Some("close"));
    // hygiene-allow: wall-clock -- the server runs on real time; the subject
    // is that the drain ends at its own timeout, measured from before the
    // connection opened so the band cannot start late.
    assert!(
        shed.elapsed >= HEAD_REFUSAL_DRAIN_TIMEOUT && shed.elapsed < GUARD,
        "answered at the drain timeout, after {:?}",
        shed.elapsed
    );
    assert_eq!(undrained(&client, &base, "timeout").await, 1);
    assert_eq!(undrained(&client, &base, "over_cap").await, 0);
    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #2926, case H: an unauthenticated shed whose body is also above the
/// drain cap is named by the first reason in order, `unauthenticated`, and
/// never also counted `over_cap`.
#[tokio::test]
async fn an_unauthenticated_shed_over_the_cap_counts_as_unauthenticated() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let head = request_head(
        running.http_addr,
        "/api/v1/write",
        Some("not-a-token"),
        &format!("content-length: {}", 2 * 1024 * 1024),
    );
    let shed = exchange(running.http_addr, &head, &[], Duration::from_secs(10)).await;

    assert_eq!(shed.status_line, "HTTP/1.1 429 Too Many Requests");
    assert_eq!(shed.header("connection"), Some("close"));
    assert_eq!(undrained(&client, &base, "unauthenticated").await, 1);
    assert_eq!(undrained(&client, &base, "over_cap").await, 0);
    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #2926: `ravel_ingest_head_refusal_undrained_total` is on the real
/// `/metrics` surface from process start, as a counter, with one sample for
/// each of its four reasons, and its HELP says `Connection: close` does not
/// prevent the reset.
#[tokio::test]
async fn head_refusal_undrained_family_is_on_metrics_with_every_reason() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    for reason in ["unauthenticated", "over_cap", "busy", "timeout"] {
        assert_eq!(undrained(&client, &base, reason).await, 0, "{reason}");
    }
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    assert!(body.contains("# TYPE ravel_ingest_head_refusal_undrained_total counter\n"));
    let help = body
        .lines()
        .find(|line| line.starts_with("# HELP ravel_ingest_head_refusal_undrained_total "))
        .expect("the family has a HELP line");
    for needle in [
        "unauthenticated",
        "over_cap",
        "busy",
        "timeout",
        "Connection: close, which does not prevent the reset",
    ] {
        assert!(help.contains(needle), "HELP names {needle:?}: {help}");
    }
    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #2926, case G, a check on gRPC rather than a change to it: with the
/// ceiling held, 512 KiB exports over one HTTP/2 connection are each refused
/// `RESOURCE_EXHAUSTED`, never with a transport error. HTTP/2 refuses a
/// stream without closing the connection, so no drain is needed there.
#[tokio::test]
async fn grpc_shed_of_a_large_export_is_resource_exhausted_not_a_transport_error() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(0)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let export =
        ExportMetricsServiceRequest::decode(decodable_metrics_body(512 * 1024, 0x2926).as_slice())
            .expect("the body is a valid export");
    let mut grpc = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    const EXPORTS: u64 = 32;
    for i in 0..EXPORTS {
        let mut request = tonic::Request::new(export.clone());
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
        );
        let status = grpc
            .export(request)
            .await
            .expect_err("every export is shed");
        assert_eq!(
            status.code(),
            tonic::Code::ResourceExhausted,
            "export {i}: refused by the ceiling, not a transport error: {status:?}"
        );
    }
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        EXPORTS
    );
    for reason in ["unauthenticated", "over_cap", "busy", "timeout"] {
        assert_eq!(
            undrained(&client, &base, reason).await,
            0,
            "a gRPC shed is not an HTTP/1 head refusal"
        );
    }
    running.shutdown().await.expect("graceful shutdown");
}

const GRPC_METRICS_EXPORT: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
const GRPC_LOGS_EXPORT: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";
const GRPC_TRACES_EXPORT: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

/// A message that is not a valid OTLP export request of any signal. Field 1 of
/// `Export{Metrics,Logs,Trace}ServiceRequest` is a repeated, length-delimited
/// message; this encodes field 1 as a varint, so prost refuses the frame with
/// a wire-type error the moment tonic tries to decode it. A refusal that
/// arrives as anything other than a decode error therefore came from
/// something that ran before the decode.
#[derive(Clone, PartialEq, prost::Message)]
struct Undecodable {
    #[prost(uint64, tag = "1")]
    not_a_resource_list: u64,
}

/// Sends one `Undecodable` message as a unary gRPC call to `path` and returns
/// the status it was refused with.
async fn grpc_send_undecodable(
    grpc_addr: SocketAddr,
    path: &'static str,
    bearer: Option<&str>,
) -> tonic::Status {
    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("gRPC client connects");
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("channel ready");
    let mut request = tonic::Request::new(Undecodable {
        not_a_resource_list: 7,
    });
    if let Some(token) = bearer {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("ascii metadata"),
        );
    }
    grpc.unary::<Undecodable, Undecodable, _>(
        request,
        http::uri::PathAndQuery::from_static(path),
        tonic_prost::ProstCodec::default(),
    )
    .await
    .expect_err("an undecodable export is never accepted")
}

/// Issue #1705, the gRPC half of the ceiling: with the process-wide ceiling
/// saturated by in-flight HTTP exports, a gRPC export over it is refused
/// `RESOURCE_EXHAUSTED` with the shared shed message, even though its body is
/// not a valid `ExportMetricsServiceRequest`.
///
/// The body is what makes this discriminating. A server that takes the permit
/// inside the handler has to decode the message first, and this message does
/// not decode, so it answers `INTERNAL` with a decode error and never reaches
/// the ceiling; only a refusal decided on the request head can answer
/// `RESOURCE_EXHAUSTED` here.
#[tokio::test]
async fn grpc_refuses_overflow_before_decoding_the_message() {
    const LIMIT: u64 = 2;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(LIMIT)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let gate = hold_provisioning_reads(&fault);

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

    wait_for_parked(&gate, LIMIT as usize).await;

    let status = grpc_send_undecodable(grpc_addr, GRPC_METRICS_EXPORT, Some(TOKEN)).await;

    assert_eq!(
        status.code(),
        tonic::Code::ResourceExhausted,
        "an export over the ceiling is shed on its head, before the undecodable body is decoded: \
         {status:?}"
    );
    assert_eq!(
        status.message(),
        "process in-flight ingest-request limit reached",
        "the refusal carries the shared shed message, the same string HTTP's 429 body uses"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        1,
        "the existing shed counter counts the gRPC refusal, exactly once"
    );

    drain_until_done(&gate, &held_tasks).await;
    for task in held_tasks {
        let response = task.await.expect("task joins");
        assert_eq!(response.status(), 200);
    }

    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #1705, gRPC authentication: an export carrying no credentials is
/// refused `UNAUTHENTICATED`, with the message the handlers have always used,
/// on all three OTLP gRPC services, even though its body does not decode.
///
/// A server that authenticates inside the handler decodes the message first
/// and answers `INTERNAL` with a decode error instead, so an anonymous caller
/// could make it decode arbitrary bytes.
#[tokio::test]
async fn grpc_refuses_unauthenticated_before_decoding_the_message() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    for path in [GRPC_METRICS_EXPORT, GRPC_LOGS_EXPORT, GRPC_TRACES_EXPORT] {
        let status = grpc_send_undecodable(grpc_addr, path, None).await;
        assert_eq!(
            status.code(),
            tonic::Code::Unauthenticated,
            "{path}: no credentials is refused before the body is decoded: {status:?}"
        );
        assert_eq!(
            status.message(),
            "invalid or missing tenant credentials",
            "{path}: the refusal keeps the message the handler returned"
        );
    }

    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        0,
        "an authentication refusal is not a shed"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #1705: an authenticated gRPC export through the real listener, with
/// the ceiling at 1, is admitted and acknowledged, and the shed counter stays
/// at exactly 0.
///
/// The admission layer takes the only permit on the request head. If the
/// handler behind it did not see the layer's marker and took a permit of its
/// own, the second `try_admit` on a ceiling of 1 would shed the request the
/// layer had just admitted, and the counter would read 1.
#[tokio::test]
async fn grpc_authenticated_export_at_a_ceiling_of_one_is_charged_once() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(1)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let mut grpc = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(metrics_export_request("charged_once", now_ns()));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let response = grpc
        .export(request)
        .await
        .expect("an authenticated export under the ceiling is admitted");

    assert_eq!(
        response.into_inner().partial_success,
        None,
        "the single point was accepted whole"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_concurrency_shed_total").await,
        0,
        "one request takes exactly one permit, so a ceiling of 1 sheds nothing"
    );
    assert_eq!(
        scrape_counter(&client, &base, "ravel_ingest_buffered_items_total").await,
        1,
        "the admitted request's single point reached a shard buffer"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Issue #1705: the ingest ceiling does not throttle Flight SQL, which shares
/// the gRPC listener. With the ceiling at 1, one Flight SQL stream is held
/// open (a `DoPut` whose request stream never yields, so the server keeps
/// waiting for its first message) and a second Flight SQL call on the same
/// connection still completes.
///
/// A listener that advertised `SETTINGS_MAX_CONCURRENT_STREAMS` equal to the
/// ingest ceiling would make the client queue the second stream behind the
/// held one, and it would never be answered. The first `ListFlights` is a
/// round trip that guarantees the client has applied the server's SETTINGS
/// before the held stream opens.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn flight_sql_is_not_throttled_by_the_ingest_ceiling() {
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::{Criteria, FlightData};

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Bounded(1)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");

    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
        .expect("valid endpoint uri")
        .connect()
        .await
        .expect("Flight client connects");
    let mut client = FlightServiceClient::new(channel);

    let warm_up = client
        .list_flights(Criteria::default())
        .await
        .expect_err("ListFlights is not implemented");
    assert_eq!(warm_up.code(), tonic::Code::Unimplemented);

    let mut held_client = client.clone();
    let mut held = Box::pin(async move {
        held_client
            .do_put(futures::stream::pending::<FlightData>())
            .await
    });
    for _ in 0..16 {
        assert!(
            futures::poll!(&mut held).is_pending(),
            "the held DoPut waits for a first message that never comes"
        );
        tokio::task::yield_now().await;
    }

    let second = tokio::time::timeout(
        Duration::from_secs(20),
        client.list_flights(Criteria::default()),
    )
    .await;
    let status = second
        .expect(
            "a second Flight SQL stream on the connection was not answered within 20s, so it was \
             queued behind the held one by a stream cap",
        )
        .expect_err("ListFlights is not implemented");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    drop(held);
    running.shutdown().await.expect("graceful shutdown");
}
