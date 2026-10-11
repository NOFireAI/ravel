//! What the ADR-1702 write gate costs the ingest codec work (issue #2669).
//!
//! Each path runs GATED, on the server's write gate (default permits for this
//! host's cores, default inline floor, attached where `ravel_server::start`
//! attaches it: the gateway state, the Remote Write state and the metrics
//! router), and UNGATED, with no gate anywhere:
//!
//! - `otlp_gzip`: an OTLP-HTTP metrics export with a gzip body through
//!   `otlp_http::router`, in buffered mode, so a request is the inflate, the
//!   protobuf decode, normalization and the buffer append rather than a flush
//!   wait. Buffers still flush in the background when they reach
//!   `target_bytes`, on the gate when there is one.
//! - `rw_snappy`: a Remote Write 1.0 request with a snappy body through
//!   `remote_write::router`. Remote Write is strict only, so every request
//!   waits for its flush. The metrics router runs a 10 ms flush delay instead
//!   of the 2 s default: at the default every request waits about 2 s and the
//!   codec work is lost in that wait.
//! - `flush_encode`: one strict `IngestRouter::write_values` whose object
//!   estimate reaches the default 8 MiB `target_bytes`, on a one-shard router,
//!   so the size trigger fires on the write itself and its encode is the
//!   gated unit. One tenant per client. `max_queued_flushes` is raised to the
//!   client count so 16 concurrent writes into the one shard are not refused
//!   at the default cap of 8.
//!
//! Each runs at 1 and 16 concurrent clients. Criterion times every
//! configuration first; then the pre-registered table: per configuration,
//! interleaved gated and ungated runs on fresh state, each reporting
//! throughput, p50 and p99 latency, and the gate's per-site job and inline
//! counts read before and after the measured requests.
//!
//! Environment: `RAVEL_GATE_BENCH_RUNS` (runs per mode, default 5),
//! `RAVEL_GATE_BENCH_RUN_SECS` (target length of one run, default 1.0),
//! `RAVEL_GATE_BENCH_SKIP_CRITERION=1`, `RAVEL_GATE_BENCH_SKIP_TABLE=1`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{HeaderMap, Request};
use bytes::Bytes;
use criterion::{Criterion, SamplingMode, Throughput};
use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_cpu_gate::{GateSite as _, WriteGate, WriteSite};
use ravel_ingest::{
    AdmissionController, AdmissionLimits, CountLimit, IngestByteBudget, IngestByteBudgetLimit,
    IngestConfig, IngestPoint, IngestRouter, IngestValue, LogIngestRouter, RateLimit,
    STRICT_VISIBILITY_RESERVE_NS, SpanIngestRouter, SystemClock, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
use ravel_query::http::{AuthError, TenantResolver};
use ravel_remote_write::proto::prometheus::{
    Label as PbLabel, Sample as PbSample, TimeSeries as PbTimeSeries, WriteRequest,
};
use ravel_server::config::CpuGatePermits;
use ravel_server::cpu_gates::CpuGates;
use ravel_server::ingest_byte_metrics::IngestByteMetrics;
use ravel_server::ingest_concurrency::{IngestConcurrencyController, IngestConcurrencyLimit};
use ravel_server::normalize_reject_metrics::NormalizeRejectMetrics;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};
use tower::ServiceExt;

const KIB: usize = 1024;
/// Compressed body sizes for the two HTTP lanes.
const BODY_SIZES: [usize; 4] = [16 * KIB, 64 * KIB, 256 * KIB, 1024 * KIB];
const CONCURRENCIES: [usize; 2] = [1, 16];
/// The pre-registered band applies at and above this compressed size.
const BAND_FLOOR_BYTES: usize = 64 * KIB;
/// Gated throughput must stay within this fraction of ungated.
const BAND: f64 = 0.02;
/// Series per pod in the HTTP payloads: 16 metrics times 4 route/method/status
/// combinations.
const SERIES_PER_POD: usize = 64;
const FLUSH_SERIES: usize = 2_000;
const FLUSH_SAMPLES_PER_SERIES: usize = 256;
const ACK_DEADLINE: Duration = Duration::from_secs(60);
const OTLP_TENANT: &str = "bench-otlp";
const RW_TENANT: &str = "bench-rw";

const METRICS: [(&str, bool); 16] = [
    ("http.server.requests", true),
    ("http.server.active_requests", false),
    ("http.server.request.body.size", true),
    ("http.server.response.body.size", true),
    ("process.cpu.time", true),
    ("process.memory.usage", false),
    ("jvm.gc.collections", true),
    ("jvm.threads.count", false),
    ("db.client.connections.usage", false),
    ("db.client.operations", true),
    ("rpc.client.requests", true),
    ("rpc.client.in_flight", false),
    ("messaging.publish.messages", true),
    ("messaging.consumer.lag", false),
    ("cache.hits", true),
    ("cache.entries", false),
];
const ROUTES: [(&str, &str, i64); 4] = [
    ("GET", "/api/v1/cart/{id}", 200),
    ("POST", "/api/v1/checkout", 201),
    ("GET", "/api/v1/products", 200),
    ("PUT", "/api/v1/cart/{id}/items", 409),
];

fn wall_now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// xorshift64*: deterministic payload values without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One series of the shared universe: pod, metric and route are derived from
/// the index, and the value from the seeded generator.
struct SeriesShape {
    pod: usize,
    metric: usize,
    route: usize,
}

fn series_shape(index: usize) -> SeriesShape {
    let within = index % SERIES_PER_POD;
    SeriesShape {
        pod: index / SERIES_PER_POD,
        metric: within % METRICS.len(),
        route: within / METRICS.len(),
    }
}

fn pod_name(pod: usize) -> String {
    format!("checkout-7f9c6d{:04}-x{:03}", pod, (pod * 37) % 1000)
}

fn sample_value(rng: &mut Rng, counter: bool) -> f64 {
    if counter {
        (rng.next() % 5_000_000) as f64
    } else {
        (rng.unit() * 10_000.0).round() / 100.0
    }
}

fn kv(key: &str, value: AnyValueVariant) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    kv(key, AnyValueVariant::StringValue(value.to_string()))
}

/// An OTLP metrics export of `pods` resources, one point per series as a
/// periodic exporter sends it: per pod, 16 metrics (half cumulative sums, half
/// gauges) times 4 data points with 4 attributes each.
fn otlp_export(pods: usize, now_ns: i64) -> ExportMetricsServiceRequest {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let start_ns = (now_ns - 3_600_000_000_000) as u64;
    let resource_metrics = (0..pods)
        .map(|pod| {
            let name = pod_name(pod);
            let resource = Resource {
                attributes: vec![
                    kv_str("service.name", "checkout"),
                    kv_str("service.namespace", "shop"),
                    kv_str("service.instance.id", &name),
                    kv_str("host.name", &format!("node-{:02}", pod % 40)),
                    kv_str("k8s.namespace.name", "shop"),
                    kv_str("k8s.pod.name", &name),
                    kv_str("k8s.container.name", "checkout"),
                ],
                ..Default::default()
            };
            let metrics = METRICS
                .iter()
                .map(|&(metric, counter)| {
                    let data_points = ROUTES
                        .iter()
                        .map(|&(method, route, status)| {
                            let value = sample_value(&mut rng, counter);
                            let jitter = (rng.next() % 1_000_000) as i64;
                            NumberDataPoint {
                                attributes: vec![
                                    kv_str("http.request.method", method),
                                    kv_str("http.route", route),
                                    kv(
                                        "http.response.status_code",
                                        AnyValueVariant::IntValue(status),
                                    ),
                                    kv_str("server.address", "checkout.shop.svc"),
                                ],
                                start_time_unix_nano: start_ns,
                                time_unix_nano: (now_ns - jitter) as u64,
                                value: Some(if counter {
                                    NumberValue::AsInt(value as i64)
                                } else {
                                    NumberValue::AsDouble(value)
                                }),
                                ..Default::default()
                            }
                        })
                        .collect();
                    let data = if counter {
                        MetricData::Sum(Sum {
                            data_points,
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })
                    } else {
                        MetricData::Gauge(Gauge { data_points })
                    };
                    Metric {
                        name: metric.to_string(),
                        unit: "1".to_string(),
                        data: Some(data),
                        ..Default::default()
                    }
                })
                .collect();
            ResourceMetrics {
                resource: Some(resource),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "io.opentelemetry.instrumentation.http".to_string(),
                        version: "2.10.0".to_string(),
                        ..Default::default()
                    }),
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest { resource_metrics }
}

fn pb_label(name: &str, value: &str) -> PbLabel {
    PbLabel {
        name: name.to_string(),
        value: value.to_string(),
    }
}

/// A Remote Write 1.0 request over the same series universe, one sample per
/// series as Prometheus sends a fresh scrape, labels sorted by name.
fn rw_request(pods: usize, now_ns: i64) -> WriteRequest {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let timeseries = (0..pods * SERIES_PER_POD)
        .map(|index| {
            let shape = series_shape(index);
            let (metric, counter) = METRICS[shape.metric];
            let (method, route, status) = ROUTES[shape.route];
            let pod = pod_name(shape.pod);
            let name = format!(
                "{}{}",
                metric.replace('.', "_"),
                if counter { "_total" } else { "" }
            );
            let value = sample_value(&mut rng, counter);
            let jitter_ms = (rng.next() % 1_000) as i64;
            PbTimeSeries {
                labels: vec![
                    pb_label("__name__", &name),
                    pb_label("http_request_method", method),
                    pb_label("http_response_status_code", &status.to_string()),
                    pb_label("http_route", route),
                    pb_label("instance", &format!("{pod}:9464")),
                    pb_label("job", "shop/checkout"),
                    pb_label("namespace", "shop"),
                    pb_label("pod", &pod),
                ],
                samples: vec![PbSample {
                    value,
                    timestamp: now_ns / 1_000_000 - jitter_ms,
                }],
                ..Default::default()
            }
        })
        .collect();
    WriteRequest {
        timeseries,
        ..Default::default()
    }
}

fn gzip(raw: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(raw).expect("gzip");
    encoder.finish().expect("gzip")
}

fn snappy(raw: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(raw).expect("snappy")
}

/// A body for one HTTP lane at one size: the compressed bytes and the shape
/// that produced them.
struct Payload {
    body: Bytes,
    pods: usize,
    series: usize,
    raw_bytes: usize,
}

/// Finds the pod count whose compressed body is closest to `target` bytes.
/// `build` returns (raw, compressed) for a pod count.
fn fit_payload(target: usize, build: impl Fn(usize) -> (Vec<u8>, Vec<u8>)) -> Payload {
    let mut pods = 4usize;
    let mut best: Option<(usize, Vec<u8>, Vec<u8>)> = None;
    for _ in 0..8 {
        let (raw, compressed) = build(pods);
        let error = compressed.len().abs_diff(target);
        let better = best
            .as_ref()
            .is_none_or(|(_, _, c)| error < c.len().abs_diff(target));
        let next = ((pods as f64) * target as f64 / compressed.len() as f64).round() as usize;
        if better {
            best = Some((pods, raw, compressed));
        }
        if error * 100 <= target || next.max(1) == pods {
            break;
        }
        pods = next.max(1);
    }
    let (pods, raw, compressed) = best.expect("at least one candidate");
    Payload {
        body: Bytes::from(compressed),
        pods,
        series: pods * SERIES_PER_POD,
        raw_bytes: raw.len(),
    }
}

/// One flush-lane client's batch: `FLUSH_SERIES` series of
/// `FLUSH_SAMPLES_PER_SERIES` scalar samples at 1 s spacing for its tenant,
/// series-major so a series' points share one label set.
fn flush_batch(tenant: &TenantId, now_ns: i64) -> Vec<IngestPoint> {
    let mut rng = Rng(0xA076_1D64_78BD_642F);
    let mut points = Vec::with_capacity(FLUSH_SERIES * FLUSH_SAMPLES_PER_SERIES);
    for index in 0..FLUSH_SERIES {
        let shape = series_shape(index);
        let (metric, counter) = METRICS[shape.metric];
        let (method, route, status) = ROUTES[shape.route];
        let name = metric.replace('.', "_");
        let pod = pod_name(shape.pod);
        let status = status.to_string();
        let mut labels: Vec<Label> = [
            ("__name__", name.as_str()),
            ("http_request_method", method),
            ("http_response_status_code", status.as_str()),
            ("http_route", route),
            ("job", "shop/checkout"),
            ("pod", pod.as_str()),
        ]
        .iter()
        .map(|(name, value)| Label {
            name: (*name).to_string(),
            value: (*value).to_string(),
        })
        .collect();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let labels = LabelSet::new(labels).expect("labels");
        let series_id = SeriesId::compute(tenant, &name, &labels).expect("series id");
        let labels = Arc::new(labels);
        let mut value = sample_value(&mut rng, counter);
        for k in 0..FLUSH_SAMPLES_PER_SERIES {
            let ts_ns = now_ns - ((FLUSH_SAMPLES_PER_SERIES - k) as i64) * 1_000_000_000;
            value += if counter {
                (rng.next() % 50) as f64
            } else {
                (rng.unit() - 0.5).round()
            };
            points.push(IngestPoint {
                series_id,
                labels: Arc::clone(&labels),
                value: IngestValue::Scalar(Sample { ts_ns, value }),
            });
        }
    }
    points
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    OtlpGzip,
    RwSnappy,
    FlushEncode,
}

impl Lane {
    fn name(self) -> &'static str {
        match self {
            Lane::OtlpGzip => "otlp_gzip",
            Lane::RwSnappy => "rw_snappy",
            Lane::FlushEncode => "flush_encode",
        }
    }

    fn site(self) -> WriteSite {
        match self {
            Lane::OtlpGzip => WriteSite::OtlpHttpGzip,
            Lane::RwSnappy => WriteSite::RemoteWriteSnappy,
            Lane::FlushEncode => WriteSite::MetricsFlush,
        }
    }
}

struct FixedTenantResolver(TenantId);

impl TenantResolver for FixedTenantResolver {
    fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
        Ok(self.0.clone())
    }
}

fn unlimited_admission() -> Arc<AdmissionController> {
    Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        AdmissionLimits {
            max_active_series: CountLimit::Unlimited,
            max_active_streams: CountLimit::Unlimited,
            ingest_byte_rate: RateLimit::Unlimited,
            series_creation_rate: RateLimit::Unlimited,
        },
    ))
}

/// Fresh state for one lane: a `MemoryStore`, the routers, and the gate when
/// `gated`. Must be called inside the runtime, which the routers spawn on.
struct Fixture {
    app: Option<axum::Router>,
    router: Arc<IngestRouter>,
    gate: Option<Arc<WriteGate>>,
}

fn build_fixture(lane: Lane, gated: bool, permits: CpuGatePermits, clients: usize) -> Fixture {
    let gate = gated.then(|| CpuGates::new(permits).write);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let config = match lane {
        Lane::OtlpGzip => IngestConfig::default(),
        Lane::RwSnappy => {
            let max_flush_delay = Duration::from_millis(10);
            IngestConfig {
                max_flush_delay,
                flush_tick: Duration::from_millis(5),
                strict_visibility_budget_ns: max_flush_delay.as_nanos() as i64
                    + STRICT_VISIBILITY_RESERVE_NS,
                ..IngestConfig::default()
            }
        }
        Lane::FlushEncode => IngestConfig {
            shard_count: 1,
            max_queued_flushes: clients.max(IngestConfig::default().max_queued_flushes),
            ..IngestConfig::default()
        },
    };
    let mut router = IngestRouter::new(
        config,
        Arc::clone(&store),
        ravel_types::Signal::Metrics,
        Arc::new(SystemClock),
    )
    .with_budget(Arc::clone(&budget));
    if let Some(gate) = &gate {
        router = router.with_write_gate(Arc::clone(gate));
    }
    let router = Arc::new(router);
    let admission = unlimited_admission();
    let concurrency = IngestConcurrencyController::shared(IngestConcurrencyLimit::Unlimited);
    let app = match lane {
        Lane::OtlpGzip => {
            let mut logs = LogIngestRouter::new(
                IngestConfig::default(),
                Arc::clone(&store),
                Arc::new(SystemClock),
            )
            .with_budget(Arc::clone(&budget));
            let mut spans = SpanIngestRouter::new(
                IngestConfig::default(),
                Arc::clone(&store),
                Arc::new(SystemClock),
            )
            .with_budget(Arc::clone(&budget));
            if let Some(gate) = &gate {
                logs = logs.with_write_gate(Arc::clone(gate));
                spans = spans.with_write_gate(Arc::clone(gate));
            }
            let normalize_metrics = Arc::new(NormalizeRejectMetrics::new());
            let state = Arc::new(ravel_server::otlp_http::GatewayState {
                tenant_resolver: Arc::new(FixedTenantResolver(TenantId::new(OTLP_TENANT))),
                ingest: ravel_server::ingest::IngestState {
                    router: Arc::clone(&router),
                    limits: IngestLimits::default(),
                    ack_deadline: ACK_DEADLINE,
                    admission: Arc::clone(&admission),
                    recovery: None,
                    provisioning: None,
                    metadata_sink: None,
                    normalize_metrics: Arc::clone(&normalize_metrics),
                    budget: Arc::clone(&budget),
                },
                logs_ingest: ravel_server::logs_ingest::LogIngestState {
                    router: Arc::new(logs),
                    limits: LogIngestLimits::default(),
                    ack_deadline: ACK_DEADLINE,
                    admission: Arc::clone(&admission),
                    store: Arc::clone(&store),
                    recovery: None,
                    provisioning: None,
                    normalize_metrics: Arc::clone(&normalize_metrics),
                    budget: Arc::clone(&budget),
                },
                traces_ingest: ravel_server::traces_ingest::SpanIngestState {
                    router: Arc::new(spans),
                    limits: SpanIngestLimits::default(),
                    ack_deadline: ACK_DEADLINE,
                    admission: Arc::clone(&admission),
                    store: Arc::clone(&store),
                    recovery: None,
                    provisioning: None,
                    normalize_metrics,
                    budget: Arc::clone(&budget),
                },
                admission,
                budget,
                ingest_concurrency: concurrency,
                ingest_byte_metrics: Arc::new(IngestByteMetrics::new()),
                write_gate: gate.clone(),
            });
            Some(ravel_server::otlp_http::router(state))
        }
        Lane::RwSnappy => {
            let state = Arc::new(ravel_server::remote_write::RemoteWriteState {
                tenant_resolver: Arc::new(FixedTenantResolver(TenantId::new(RW_TENANT))),
                router: Arc::clone(&router),
                limits: IngestLimits::default(),
                ack_deadline: ACK_DEADLINE,
                metrics: ravel_server::remote_write::RemoteWriteMetrics::default(),
                admission,
                recovery: None,
                provisioning: None,
                ingest_concurrency: concurrency,
                clock: Arc::new(SystemClock),
                metadata_sink: None,
                budget,
                write_gate: gate.clone(),
            });
            Some(ravel_server::remote_write::router(state))
        }
        Lane::FlushEncode => None,
    };
    Fixture { app, router, gate }
}

fn http_request(lane: Lane, body: &Bytes) -> Request<Body> {
    let builder = Request::builder()
        .method("POST")
        .header("content-type", "application/x-protobuf");
    let builder = match lane {
        Lane::OtlpGzip => builder
            .uri("/v1/metrics")
            .header("content-encoding", "gzip")
            .header(ravel_server::otlp_http::INGEST_MODE_HEADER, "buffered"),
        Lane::RwSnappy | Lane::FlushEncode => builder
            .uri("/api/v1/write")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0"),
    };
    builder.body(Body::from(body.clone())).expect("request")
}

/// What one client saw: per-request latencies and the span it was busy.
#[derive(Default)]
struct ClientLog {
    latencies: Vec<Duration>,
    /// Per round (flush lane) or per client (HTTP lanes): first start, last end.
    spans: Vec<(Instant, Instant)>,
    errors: u64,
    first_error: Option<String>,
}

/// The input of one configuration.
#[derive(Clone)]
enum Input {
    Http(Bytes),
    /// One batch per client, each for that client's own tenant.
    Flush(Arc<Vec<(TenantId, Vec<IngestPoint>)>>),
}

/// Runs `rounds` requests per client at `clients` concurrency against
/// `fixture` and returns the per-client logs.
///
/// HTTP clients run free: each sends its next request when the last returns.
/// Flush clients copy their batch outside the timed span, then meet at a
/// barrier, so a round's span is the writes alone and the copy never runs
/// beside a measured encode.
async fn drive(fixture: &Fixture, input: &Input, clients: usize, rounds: usize) -> Vec<ClientLog> {
    let mut handles = Vec::with_capacity(clients);
    match input {
        Input::Http(body) => {
            let app = fixture.app.clone().expect("http lane has a router");
            let start = Arc::new(tokio::sync::Barrier::new(clients));
            for _ in 0..clients {
                let app = app.clone();
                let body = body.clone();
                let start = Arc::clone(&start);
                handles.push(tokio::spawn(async move {
                    let mut log = ClientLog::default();
                    start.wait().await;
                    let first = Instant::now();
                    for _ in 0..rounds {
                        let request = http_request(lane_of(&body), &body);
                        let began = Instant::now();
                        let response = app.clone().oneshot(request).await.expect("infallible");
                        log.latencies.push(began.elapsed());
                        if !response.status().is_success() {
                            log.errors += 1;
                            if log.first_error.is_none() {
                                let status = response.status();
                                let text = axum::body::to_bytes(response.into_body(), 4096)
                                    .await
                                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                                    .unwrap_or_default();
                                log.first_error = Some(format!("{status}: {text}"));
                            }
                        }
                    }
                    log.spans.push((first, Instant::now()));
                    log
                }));
            }
        }
        Input::Flush(batches) => {
            let barrier = Arc::new(tokio::sync::Barrier::new(clients));
            for client in 0..clients {
                let router = Arc::clone(&fixture.router);
                let batches = Arc::clone(batches);
                let barrier = Arc::clone(&barrier);
                handles.push(tokio::spawn(async move {
                    let mut log = ClientLog::default();
                    let (tenant, batch) = &batches[client];
                    for _ in 0..rounds {
                        let points = batch.clone();
                        barrier.wait().await;
                        let began = Instant::now();
                        let result = router
                            .write_values(tenant.clone(), points, WriteMode::Strict, ACK_DEADLINE)
                            .await;
                        let ended = Instant::now();
                        log.latencies.push(ended - began);
                        log.spans.push((began, ended));
                        if let Err(err) = result {
                            log.errors += 1;
                            log.first_error.get_or_insert_with(|| err.to_string());
                        }
                        barrier.wait().await;
                    }
                    log
                }));
            }
        }
    }
    let mut logs = Vec::with_capacity(clients);
    for handle in handles {
        logs.push(handle.await.expect("client task"));
    }
    logs
}

/// The HTTP lane a body belongs to: a gzip member starts with 0x1f 0x8b, a
/// snappy block body with its varint length.
fn lane_of(body: &Bytes) -> Lane {
    if body.starts_with(&[0x1f, 0x8b]) {
        Lane::OtlpGzip
    } else {
        Lane::RwSnappy
    }
}

/// The measured wall time of a run: for HTTP lanes the span from the first
/// client's start to the last client's end; for the flush lane the sum over
/// rounds of each round's first start to last end.
fn measured_wall(lane: Lane, logs: &[ClientLog]) -> Duration {
    match lane {
        Lane::FlushEncode => {
            let rounds = logs.first().map_or(0, |log| log.spans.len());
            (0..rounds)
                .map(|round| {
                    let start = logs.iter().map(|log| log.spans[round].0).min();
                    let end = logs.iter().map(|log| log.spans[round].1).max();
                    match (start, end) {
                        (Some(start), Some(end)) => end - start,
                        _ => Duration::ZERO,
                    }
                })
                .sum()
        }
        _ => {
            let start = logs
                .iter()
                .flat_map(|log| log.spans.iter().map(|s| s.0))
                .min();
            let end = logs
                .iter()
                .flat_map(|log| log.spans.iter().map(|s| s.1))
                .max();
            match (start, end) {
                (Some(start), Some(end)) => end - start,
                _ => Duration::ZERO,
            }
        }
    }
}

/// Jobs and inline runs for one site.
fn site_counts(gate: &Option<Arc<WriteGate>>, site: WriteSite) -> (u64, u64) {
    gate.as_ref().map_or((0, 0), |gate| {
        gate.snapshot()
            .sites
            .iter()
            .find(|s| s.site == site)
            .map_or((0, 0), |s| (s.jobs, s.inline))
    })
}

struct RunResult {
    requests_per_sec: f64,
    bytes_per_sec: f64,
    p50: Duration,
    p99: Duration,
    latencies: Vec<Duration>,
    /// (jobs, inline) at the lane's own site over the measured requests.
    site: (u64, u64),
    /// (jobs, inline) at `metrics_flush` over the measured requests, for the
    /// HTTP lanes whose buffers flush behind them.
    flush: (u64, u64),
    errors: u64,
    first_error: Option<String>,
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

struct Config {
    lane: Lane,
    /// Compressed body bytes, or the flush lane's buffered input estimate.
    size_label: String,
    unit_bytes: usize,
    band_applies: bool,
    clients: usize,
    input: Input,
}

const WARMUP_ROUNDS: usize = 2;

async fn run_once(
    config: &Config,
    gated: bool,
    permits: CpuGatePermits,
    rounds: usize,
) -> RunResult {
    let fixture = build_fixture(config.lane, gated, permits, config.clients);
    let warm = drive(&fixture, &config.input, config.clients, WARMUP_ROUNDS).await;
    let site_before = site_counts(&fixture.gate, config.lane.site());
    let flush_before = site_counts(&fixture.gate, WriteSite::MetricsFlush);
    let logs = drive(&fixture, &config.input, config.clients, rounds).await;
    let site_after = site_counts(&fixture.gate, config.lane.site());
    let flush_after = site_counts(&fixture.gate, WriteSite::MetricsFlush);
    let wall = measured_wall(config.lane, &logs).as_secs_f64();
    let mut latencies: Vec<Duration> = logs.iter().flat_map(|l| l.latencies.clone()).collect();
    latencies.sort_unstable();
    let requests = latencies.len();
    let errors = warm.iter().chain(logs.iter()).map(|l| l.errors).sum();
    let first_error = warm
        .iter()
        .chain(logs.iter())
        .find_map(|l| l.first_error.clone());
    drop(fixture);
    RunResult {
        requests_per_sec: requests as f64 / wall,
        bytes_per_sec: (requests * config.unit_bytes) as f64 / wall,
        p50: percentile(&latencies, 0.50),
        p99: percentile(&latencies, 0.99),
        latencies,
        site: (site_after.0 - site_before.0, site_after.1 - site_before.1),
        flush: (
            flush_after.0 - flush_before.0,
            flush_after.1 - flush_before.1,
        ),
        errors,
        first_error,
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn build_configs(now_ns: i64) -> Vec<Config> {
    let mut configs = Vec::new();
    println!("payload shapes:");
    for &size in &BODY_SIZES {
        let otlp = fit_payload(size, |pods| {
            let raw = otlp_export(pods, now_ns).encode_to_vec();
            let compressed = gzip(&raw);
            (raw, compressed)
        });
        let rw = fit_payload(size, |pods| {
            let raw = rw_request(pods, now_ns).encode_to_vec();
            let compressed = snappy(&raw);
            (raw, compressed)
        });
        for (lane, payload) in [(Lane::OtlpGzip, otlp), (Lane::RwSnappy, rw)] {
            println!(
                "  {:<10} target {:>7} B: {:>4} pods, {:>6} series x 1 point, raw {:>9} B, \
                 compressed {:>8} B, ratio {:.2}",
                lane.name(),
                size,
                payload.pods,
                payload.series,
                payload.raw_bytes,
                payload.body.len(),
                payload.raw_bytes as f64 / payload.body.len() as f64
            );
            for &clients in &CONCURRENCIES {
                configs.push(Config {
                    lane,
                    size_label: format!("{}KiB", size / KIB),
                    unit_bytes: payload.body.len(),
                    band_applies: size >= BAND_FLOOR_BYTES,
                    clients,
                    input: Input::Http(payload.body.clone()),
                });
            }
        }
    }
    let max_clients = CONCURRENCIES.iter().copied().max().unwrap_or(1);
    let batches: Vec<(TenantId, Vec<IngestPoint>)> = (0..max_clients)
        .map(|client| {
            let tenant = TenantId::new(format!("bench-flush-{client:02}"));
            let batch = flush_batch(&tenant, now_ns);
            (tenant, batch)
        })
        .collect();
    // The figures the shard's size trigger reads (`IngestPoint::
    // est_object_sample_bytes` and `est_object_series_bytes`).
    let label_bytes: usize = batches[0]
        .1
        .chunks(FLUSH_SAMPLES_PER_SERIES)
        .map(|run| {
            run[0]
                .labels
                .iter()
                .map(|l| l.name.len() + l.value.len())
                .sum::<usize>()
        })
        .sum();
    let object_estimate =
        FLUSH_SERIES * FLUSH_SAMPLES_PER_SERIES * 16 + FLUSH_SERIES * 32 + label_bytes;
    println!(
        "  {:<10} {} series x {} samples = {} points per write, object estimate {} B \
         (target_bytes {} B), {} B per IngestPoint",
        Lane::FlushEncode.name(),
        FLUSH_SERIES,
        FLUSH_SAMPLES_PER_SERIES,
        FLUSH_SERIES * FLUSH_SAMPLES_PER_SERIES,
        object_estimate,
        IngestConfig::default().target_bytes,
        std::mem::size_of::<IngestPoint>()
    );
    let batches = Arc::new(batches);
    for &clients in &CONCURRENCIES {
        configs.push(Config {
            lane: Lane::FlushEncode,
            size_label: "8MiB".to_string(),
            unit_bytes: object_estimate,
            band_applies: true,
            clients,
            input: Input::Flush(Arc::clone(&batches)),
        });
    }
    configs
}

fn criterion_pass(rt: &tokio::runtime::Runtime, configs: &[Config], permits: CpuGatePermits) {
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .configure_from_args();
    let mut group = criterion.benchmark_group("ingest_write_gate_overhead");
    group.sampling_mode(SamplingMode::Flat);
    for config in configs {
        group.throughput(Throughput::Bytes(
            (config.unit_bytes * config.clients) as u64,
        ));
        for gated in [false, true] {
            let id = format!(
                "{}/{}/c{}/{}",
                config.lane.name(),
                config.size_label,
                config.clients,
                if gated { "gated" } else { "ungated" }
            );
            let fixture = rt.block_on(async {
                let fixture = build_fixture(config.lane, gated, permits, config.clients);
                drive(&fixture, &config.input, config.clients, WARMUP_ROUNDS).await;
                fixture
            });
            group.bench_function(id, |b| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let logs =
                            drive(&fixture, &config.input, config.clients, iters as usize).await;
                        measured_wall(config.lane, &logs)
                    })
                });
            });
            drop(fixture);
        }
    }
    group.finish();
}

struct Spread {
    median: f64,
    min: f64,
    max: f64,
}

fn spread(values: &[f64]) -> Spread {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let median = if sorted.is_empty() {
        0.0
    } else if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    };
    Spread {
        median,
        min: sorted.first().copied().unwrap_or(0.0),
        max: sorted.last().copied().unwrap_or(0.0),
    }
}

fn fmt_spread(s: &Spread, scale: f64, unit: &str) -> String {
    format!(
        "{:.2}{unit} [{:.2}-{:.2}]",
        s.median * scale,
        s.min * scale,
        s.max * scale
    )
}

fn table_pass(rt: &tokio::runtime::Runtime, configs: &[Config], permits: CpuGatePermits) {
    let runs: usize = env_or("RAVEL_GATE_BENCH_RUNS", 5).max(3);
    let run_secs: f64 = env_or("RAVEL_GATE_BENCH_RUN_SECS", 1.0);
    println!();
    println!(
        "pre-registered table: {runs} runs per mode, interleaved, ~{run_secs:.1} s each; \
         median [min-max] across runs; ratio = gated/ungated median throughput"
    );
    println!(
        "band: gated throughput within {:.0}% of ungated at >= 64 KiB; \
         inside if even the worst run pair (gated min / ungated max) holds it, \
         outside if even the best (gated max / ungated min) misses it",
        BAND * 100.0
    );
    let mut summary = Vec::new();
    for config in configs {
        // Rounds per client so one run lasts about `run_secs`, from an ungated
        // calibration run; both modes then run the same request count.
        let calibration = rt.block_on(run_once(config, false, permits, 3));
        let per_round = calibration.latencies.len() as f64 / calibration.requests_per_sec / 3.0;
        let min_rounds = if config.lane == Lane::FlushEncode {
            3
        } else {
            10
        };
        let rounds = ((run_secs / per_round).ceil() as usize).clamp(min_rounds, 20_000);

        let mut ungated = Vec::new();
        let mut gated = Vec::new();
        for run in 0..runs {
            let order = if run % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for is_gated in order {
                let result = rt.block_on(run_once(config, is_gated, permits, rounds));
                if result.errors > 0 {
                    println!(
                        "  ERRORS {} {} c{} {}: {} failed requests, first: {}",
                        config.lane.name(),
                        config.size_label,
                        config.clients,
                        if is_gated { "gated" } else { "ungated" },
                        result.errors,
                        result.first_error.clone().unwrap_or_default()
                    );
                }
                if is_gated {
                    gated.push(result);
                } else {
                    ungated.push(result);
                }
            }
        }
        let stat = |results: &[RunResult], f: &dyn Fn(&RunResult) -> f64| {
            spread(&results.iter().map(f).collect::<Vec<_>>())
        };
        let u_rps = stat(&ungated, &|r| r.requests_per_sec);
        let g_rps = stat(&gated, &|r| r.requests_per_sec);
        let u_bps = stat(&ungated, &|r| r.bytes_per_sec);
        let g_bps = stat(&gated, &|r| r.bytes_per_sec);
        let u_p50 = stat(&ungated, &|r| r.p50.as_secs_f64());
        let g_p50 = stat(&gated, &|r| r.p50.as_secs_f64());
        let u_p99 = stat(&ungated, &|r| r.p99.as_secs_f64());
        let g_p99 = stat(&gated, &|r| r.p99.as_secs_f64());
        let ratio = g_rps.median / u_rps.median;
        let worst = g_rps.min / u_rps.max;
        let best = g_rps.max / u_rps.min;
        let verdict = if !config.band_applies {
            "no band (<64 KiB)".to_string()
        } else if worst >= 1.0 - BAND {
            "INSIDE".to_string()
        } else if best < 1.0 - BAND {
            "OUTSIDE".to_string()
        } else {
            "CANNOT TELL (spread)".to_string()
        };
        let site_jobs: u64 = gated.iter().map(|r| r.site.0).sum();
        let site_inline: u64 = gated.iter().map(|r| r.site.1).sum();
        let flush_jobs: u64 = gated.iter().map(|r| r.flush.0).sum();
        let flush_inline: u64 = gated.iter().map(|r| r.flush.1).sum();
        let requests: usize = gated.iter().map(|r| r.latencies.len()).sum();
        let errors: u64 = gated.iter().chain(ungated.iter()).map(|r| r.errors).sum();
        let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
        let name = format!(
            "{} {} c{}",
            config.lane.name(),
            config.size_label,
            config.clients
        );
        println!();
        println!(
            "{name}: {rounds} rounds x {} clients per run, loadavg after: {}",
            config.clients,
            load.trim()
        );
        println!(
            "  req/s    ungated {}  gated {}  ratio {:.4} (worst {:.4}, best {:.4})",
            fmt_spread(&u_rps, 1.0, ""),
            fmt_spread(&g_rps, 1.0, ""),
            ratio,
            worst,
            best
        );
        println!(
            "  MiB/s    ungated {}  gated {}",
            fmt_spread(&u_bps, 1.0 / (1024.0 * 1024.0), ""),
            fmt_spread(&g_bps, 1.0 / (1024.0 * 1024.0), "")
        );
        println!(
            "  p50 ms   ungated {}  gated {}",
            fmt_spread(&u_p50, 1e3, ""),
            fmt_spread(&g_p50, 1e3, "")
        );
        println!(
            "  p99 ms   ungated {}  gated {}",
            fmt_spread(&u_p99, 1e3, ""),
            fmt_spread(&g_p99, 1e3, "")
        );
        println!(
            "  gate     {} requests measured gated: site {} jobs {} inline {}; \
             metrics_flush jobs {} inline {}; failed requests (both modes) {}",
            requests,
            config.lane.site().name(),
            site_jobs,
            site_inline,
            flush_jobs,
            flush_inline,
            errors
        );
        println!("  verdict  {verdict}");
        summary.push(format!(
            "{name:<24} ratio {ratio:.4} [{worst:.4}-{best:.4}]  site jobs/inline {site_jobs}/{site_inline} \
             flush jobs {flush_jobs}  {verdict}"
        ));
    }
    println!();
    println!("summary:");
    for line in summary {
        println!("  {line}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let listing = args.iter().any(|a| a == "--list" || a == "--test");
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let permits = CpuGatePermits::derive(cores);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    println!(
        "host: {cores} cores, write gate {} permits, inline floor {} B",
        permits.write,
        ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
    );
    let configs = build_configs(wall_now_ns());
    if std::env::var("RAVEL_GATE_BENCH_SKIP_CRITERION").is_err() || listing {
        criterion_pass(&rt, &configs, permits);
    }
    if !listing && std::env::var("RAVEL_GATE_BENCH_SKIP_TABLE").is_err() {
        table_pass(&rt, &configs, permits);
    }
}
