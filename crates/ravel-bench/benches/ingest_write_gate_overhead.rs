//! ADR-1702 write gate overhead on the ingest write path (issue #2669).
//!
//! Each path runs GATED, with a write gate built the way `ravel_server::start`
//! builds it (`CpuGates::new` over `CpuGatePermits::derive(cores)`: default
//! permits for this host, default inline floor) and attached where `start`
//! attaches it, and UNGATED, with no gate anywhere:
//!
//! - `otlp_gzip`: `POST /v1/metrics` with a gzip body through
//!   `ravel_server::otlp_http::router`, buffered mode, so the request costs
//!   the inflate, decode, normalize and buffer append rather than a flush
//!   wait. The size-triggered flushes it causes run on the same gate.
//! - `rw_snappy`: `POST /api/v1/write` with a snappy Remote Write 1.0 body
//!   through `ravel_server::remote_write::router`. That surface is strict
//!   only, so its router flushes on arrival (`target_bytes = 1`) instead of
//!   waiting out the 2 s age clock, and every request pays a flush.
//! - `flush_encode`: one strict `IngestRouter::write_values` whose points
//!   reach the default 8 MiB target object size on a one-shard router, so
//!   each write is one size-triggered flush and its encode is the gated unit.
//!
//! Criterion times every configuration first. A fixed-duration pass then
//! prints the table the issue's band is read from: per configuration, runs
//! interleaved gated/ungated, throughput and p50/p99 per run, the paired
//! gated/ungated throughput ratio, and the gate's per-site job and inline
//! counts, since the inline floor decides whether a body went through the
//! gate at all. The table pass runs only under `cargo bench`.
//!
//! The binary runs under jemalloc with its background thread configured, as
//! the shipped server does: a gated job allocates on a blocking-pool thread
//! and its result is freed on a worker, so the allocator is part of the cost.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
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
    IngestConfig, IngestPoint, IngestRouter, IngestValue, LogIngestRouter, PendingAckBudget,
    RateLimit, SpanIngestRouter, SystemClock, WriteMode,
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
use ravel_server::otlp_http::{GatewayState, INGEST_MODE_HEADER};
use ravel_server::remote_write::{RemoteWriteMetrics, RemoteWriteState};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use tower::ServiceExt;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const TENANT: &str = "bench-tenant";
const COMPRESSED_SIZES: [(&str, usize); 4] = [
    ("16KiB", 16 * 1024),
    ("64KiB", 64 * 1024),
    ("256KiB", 256 * 1024),
    ("1MiB", 1024 * 1024),
];
const CONCURRENCIES: [usize; 2] = [1, 16];
/// Distinct bodies per size, each with its own timestamps, cycled through.
const BODY_VARIANTS: usize = 4;
/// The issue's pre-registered band: gated within 2% of ungated.
const BAND_RATIO: f64 = 0.98;
/// The band applies from this compressed size up.
const BAND_FROM_BYTES: usize = 64 * 1024;
const TABLE_RUNS: usize = 5;
const HTTP_RUN: Duration = Duration::from_millis(1500);
const FLUSH_RUN: Duration = Duration::from_secs(3);
const FLUSH_SERIES: usize = 2_000;

struct FixedTenant;

impl TenantResolver for FixedTenant {
    fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
        Ok(TenantId::new(TENANT))
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// The write gate `ravel_server::start` builds for this host.
/// `RAVEL_BENCH_WRITE_PERMITS` overrides its permits, as the server's
/// `--cpu-gate-write-permits` does, so the decision-3 default can be
/// compared against more permits on the same host.
fn production_write_gate() -> Arc<WriteGate> {
    let mut permits = CpuGatePermits::derive(cores());
    if let Some(n) = std::env::var("RAVEL_BENCH_WRITE_PERMITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        permits.write = n;
    }
    CpuGates::new(permits).write
}

/// Admission that never refuses: the bench measures the gate, not the
/// per-tenant byte rate or series caps.
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

/// A deterministic xorshift, so every run sees the same payload bytes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------------------------------------------------------------------------
// Payloads
// ---------------------------------------------------------------------------

fn kv(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

fn number_point(
    attributes: Vec<KeyValue>,
    start_ns: u64,
    ts_ns: u64,
    value: NumberValue,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes,
        start_time_unix_nano: start_ns,
        time_unix_nano: ts_ns,
        value: Some(value),
        ..Default::default()
    }
}

fn gauge(name: &str, unit: &str, data_points: Vec<NumberDataPoint>) -> Metric {
    Metric {
        name: name.to_string(),
        unit: unit.to_string(),
        data: Some(MetricData::Gauge(Gauge { data_points })),
        ..Default::default()
    }
}

fn counter(name: &str, unit: &str, data_points: Vec<NumberDataPoint>) -> Metric {
    Metric {
        name: name.to_string(),
        unit: unit.to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points,
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    }
}

/// Data points one host contributes to an OTLP export.
const OTLP_POINTS_PER_HOST: usize = 16 + 4 + 24 + 1 + 4 + 4;

/// One collector batch: `hosts` resources, each a host-metrics and
/// HTTP-server scope as the OpenTelemetry collector's hostmetrics receiver and
/// an instrumented service emit them. Gauges carry full-precision doubles,
/// counters cumulative integers, one point per series.
fn otlp_export(hosts: usize, variant: usize, ts_ns: u64) -> ExportMetricsServiceRequest {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (variant as u64 + 1));
    let start_ns = ts_ns - 3_600_000_000_000;
    let resource_metrics = (0..hosts)
        .map(|h| {
            let service = format!("checkout-{}", h % 8);
            let resource = Resource {
                attributes: vec![
                    kv("service.name", service.clone()),
                    kv("service.instance.id", format!("{:016x}", h as u64 * 7919)),
                    kv("host.name", format!("ip-10-0-{}-{}", h / 256, h % 256)),
                    kv("cloud.provider", "aws"),
                    kv("cloud.region", "us-east-1"),
                    kv("k8s.namespace.name", "prod"),
                    kv("k8s.pod.name", format!("{service}-7d9f8b-{h:05}")),
                ],
                ..Default::default()
            };
            let mut cpu = Vec::new();
            for c in 0..4 {
                for state in ["user", "system", "idle", "iowait"] {
                    cpu.push(number_point(
                        vec![kv("cpu", format!("cpu{c}")), kv("state", state)],
                        0,
                        ts_ns,
                        NumberValue::AsDouble(rng.unit()),
                    ));
                }
            }
            let memory = ["used", "free", "cached", "buffered"]
                .into_iter()
                .map(|state| {
                    number_point(
                        vec![kv("state", state)],
                        start_ns,
                        ts_ns,
                        NumberValue::AsInt((rng.next() % (16 << 30)) as i64),
                    )
                })
                .collect();
            let mut http = Vec::new();
            for method in ["GET", "POST"] {
                for status in ["200", "404", "500"] {
                    for route in ["/api/cart", "/api/checkout", "/api/items/{id}", "/healthz"] {
                        http.push(number_point(
                            vec![
                                kv("http.request.method", method),
                                kv("http.response.status_code", status),
                                kv("http.route", route),
                            ],
                            start_ns,
                            ts_ns,
                            NumberValue::AsInt((rng.next() % 5_000_000) as i64),
                        ));
                    }
                }
            }
            let threads = vec![number_point(
                Vec::new(),
                0,
                ts_ns,
                NumberValue::AsInt((rng.next() % 400) as i64),
            )];
            let mut network = Vec::new();
            let mut disk = Vec::new();
            for device in ["eth0", "lo"] {
                for direction in ["transmit", "receive"] {
                    network.push(number_point(
                        vec![kv("device", device), kv("network.io.direction", direction)],
                        start_ns,
                        ts_ns,
                        NumberValue::AsInt((rng.next() % (1 << 40)) as i64),
                    ));
                }
            }
            for device in ["nvme0n1", "nvme1n1"] {
                for direction in ["read", "write"] {
                    disk.push(number_point(
                        vec![kv("device", device), kv("disk.io.direction", direction)],
                        start_ns,
                        ts_ns,
                        NumberValue::AsInt((rng.next() % (1 << 40)) as i64),
                    ));
                }
            }
            ResourceMetrics {
                resource: Some(resource),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "otelcol/hostmetricsreceiver".to_string(),
                        version: "0.120.0".to_string(),
                        ..Default::default()
                    }),
                    metrics: vec![
                        gauge("system.cpu.utilization", "1", cpu),
                        counter("system.memory.usage", "By", memory),
                        counter("http.server.request.count", "{request}", http),
                        gauge("process.runtime.jvm.threads.count", "{thread}", threads),
                        counter("system.network.io", "By", network),
                        counter("system.disk.io", "By", disk),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest { resource_metrics }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

/// Series one instance contributes to a Remote Write body.
const RW_SERIES_PER_INSTANCE: usize = 64 + 10 + 3 + 24;

/// One Remote Write 1.0 batch: `instances` node-exporter and service targets
/// as a Prometheus agent ships them, one sample per series.
fn rw_write_request(instances: usize, variant: usize, ts_ms: i64) -> WriteRequest {
    let mut rng = Rng(0xd1b5_4a32_d192_ed03 ^ (variant as u64 + 1));
    let mut timeseries = Vec::with_capacity(instances * RW_SERIES_PER_INSTANCE);
    for i in 0..instances {
        let instance = format!("10.0.{}.{}:9100", i / 256, i % 256);
        let pod = format!("checkout-{}-7d9f8b-{i:05}", i % 8);
        let base = |name: &str| {
            vec![
                PbLabel {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: name.to_string(),
                },
                PbLabel {
                    name: "cluster".to_string(),
                    value: "prod-us-east-1".to_string(),
                },
                PbLabel {
                    name: "instance".to_string(),
                    value: instance.clone(),
                },
                PbLabel {
                    name: "job".to_string(),
                    value: "node".to_string(),
                },
                PbLabel {
                    name: "namespace".to_string(),
                    value: "prod".to_string(),
                },
                PbLabel {
                    name: "pod".to_string(),
                    value: pod.clone(),
                },
            ]
        };
        let mut push = |mut labels: Vec<PbLabel>, extra: &[(&str, &str)], value: f64| {
            for (name, v) in extra {
                labels.push(PbLabel {
                    name: (*name).to_string(),
                    value: (*v).to_string(),
                });
            }
            labels.sort_by(|a, b| a.name.cmp(&b.name));
            timeseries.push(PbTimeSeries {
                labels,
                samples: vec![PbSample {
                    value,
                    timestamp: ts_ms,
                }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            });
        };
        for cpu in ["0", "1", "2", "3", "4", "5", "6", "7"] {
            for mode in [
                "idle", "iowait", "irq", "nice", "softirq", "steal", "system", "user",
            ] {
                let value = (rng.next() % 10_000_000) as f64 / 100.0;
                push(
                    base("node_cpu_seconds_total"),
                    &[("cpu", cpu), ("mode", mode)],
                    value,
                );
            }
        }
        for name in [
            "node_memory_Active_bytes",
            "node_memory_Buffers_bytes",
            "node_memory_Cached_bytes",
            "node_memory_Dirty_bytes",
            "node_memory_Inactive_bytes",
            "node_memory_MemAvailable_bytes",
            "node_memory_MemFree_bytes",
            "node_memory_MemTotal_bytes",
            "node_memory_Shmem_bytes",
            "node_memory_SwapFree_bytes",
        ] {
            let value = (rng.next() % (64 << 30)) as f64;
            push(base(name), &[], value);
        }
        for device in ["eth0", "eth1", "lo"] {
            let value = (rng.next() % (1 << 40)) as f64;
            push(
                base("node_network_receive_bytes_total"),
                &[("device", device)],
                value,
            );
        }
        for method in ["GET", "POST"] {
            for code in ["200", "404", "500"] {
                for handler in ["/api/cart", "/api/checkout", "/api/items", "/healthz"] {
                    let value = (rng.next() % 5_000_000) as f64;
                    push(
                        base("http_requests_total"),
                        &[("code", code), ("handler", handler), ("method", method)],
                        value,
                    );
                }
            }
        }
    }
    WriteRequest {
        timeseries,
        metadata: Vec::new(),
    }
}

fn snappy(data: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(data)
        .expect("snappy compress")
}

/// One size's bodies and the shape they were built with.
struct BodySet {
    bodies: Vec<Bytes>,
    units: usize,
    series_per_body: usize,
    compressed_bytes: usize,
    uncompressed_bytes: usize,
}

/// Scales `units` until the compressed body is within 2% of `target`, then
/// builds [`BODY_VARIANTS`] bodies of that unit count.
fn body_set(
    target: usize,
    series_per_unit: usize,
    build: impl Fn(usize, usize) -> (Vec<u8>, Vec<u8>),
) -> BodySet {
    let mut units = 4usize;
    for _ in 0..8 {
        let (_, compressed) = build(units, 0);
        let len = compressed.len();
        if len.abs_diff(target) * 50 <= target {
            break;
        }
        units = ((units as f64 * target as f64 / len as f64).round() as usize).max(1);
    }
    let mut bodies = Vec::with_capacity(BODY_VARIANTS);
    let mut compressed_bytes = 0;
    let mut uncompressed_bytes = 0;
    for variant in 0..BODY_VARIANTS {
        let (plain, compressed) = build(units, variant);
        compressed_bytes += compressed.len();
        uncompressed_bytes += plain.len();
        bodies.push(Bytes::from(compressed));
    }
    BodySet {
        bodies,
        units,
        series_per_body: units * series_per_unit,
        compressed_bytes: compressed_bytes / BODY_VARIANTS,
        uncompressed_bytes: uncompressed_bytes / BODY_VARIANTS,
    }
}

fn otlp_bodies(target: usize) -> BodySet {
    let ts_ns = now_ns();
    body_set(target, OTLP_POINTS_PER_HOST, |hosts, variant| {
        let plain =
            otlp_export(hosts, variant, ts_ns + variant as u64 * 1_000_000_000).encode_to_vec();
        let compressed = gzip(&plain);
        (plain, compressed)
    })
}

fn rw_bodies(target: usize) -> BodySet {
    let ts_ms = (now_ns() / 1_000_000) as i64;
    body_set(target, RW_SERIES_PER_INSTANCE, |instances, variant| {
        let plain =
            rw_write_request(instances, variant, ts_ms + variant as i64 * 1000).encode_to_vec();
        let compressed = snappy(&plain);
        (plain, compressed)
    })
}

/// Points one strict write carries so its one-shard buffer's object-bytes
/// estimate reaches `IngestConfig::default().target_bytes` (16 bytes a scalar
/// sample, 32 bytes plus label bytes a series): one size-triggered flush.
fn flush_points(tenant: &TenantId) -> (Vec<IngestPoint>, usize) {
    let target = IngestConfig::default().target_bytes;
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut series = Vec::with_capacity(FLUSH_SERIES);
    let mut series_bytes = 0usize;
    for s in 0..FLUSH_SERIES {
        let name = [
            "node_cpu_seconds_total",
            "http_requests_total",
            "node_memory_bytes",
        ][s % 3];
        let labels = vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: name.to_string(),
            },
            Label {
                name: "cluster".to_string(),
                value: "prod-us-east-1".to_string(),
            },
            Label {
                name: "instance".to_string(),
                value: format!("10.0.{}.{}:9100", s / 256, s % 256),
            },
            Label {
                name: "job".to_string(),
                value: "node".to_string(),
            },
            Label {
                name: "mode".to_string(),
                value: ["idle", "user", "system", "iowait"][s % 4].to_string(),
            },
        ];
        series_bytes += 32
            + labels
                .iter()
                .map(|l| l.name.len() + l.value.len())
                .sum::<usize>();
        let label_set = LabelSet::new(labels).expect("labels");
        let id = SeriesId::compute(tenant, name, &label_set).expect("series id");
        series.push((id, Arc::new(label_set)));
    }
    let samples_per_series = (target - series_bytes).div_ceil(16 * FLUSH_SERIES) + 1;
    let end_ns = now_ns() as i64;
    let mut points = Vec::with_capacity(FLUSH_SERIES * samples_per_series);
    for (id, labels) in &series {
        let mut value = rng.unit() * 1000.0;
        for i in 0..samples_per_series {
            value += rng.unit();
            points.push(IngestPoint {
                series_id: *id,
                labels: Arc::clone(labels),
                value: IngestValue::Scalar(Sample {
                    ts_ns: end_ns - (samples_per_series - i) as i64 * 1_000_000_000,
                    value,
                }),
            });
        }
    }
    (points, samples_per_series)
}

// ---------------------------------------------------------------------------
// Server state, built as the otlp_http and remote_write tests build it
// ---------------------------------------------------------------------------

fn gateway_state(
    gate: Option<Arc<WriteGate>>,
) -> (
    Arc<GatewayState>,
    Arc<IngestRouter>,
    Arc<dyn ObjectStoreBackend>,
) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let mut metrics_router = IngestRouter::new(
        IngestConfig::default(),
        store.clone(),
        Signal::Metrics,
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget.clone());
    let mut log_router = LogIngestRouter::new(
        IngestConfig::default(),
        store.clone(),
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget.clone());
    let mut span_router = SpanIngestRouter::new(
        IngestConfig::default(),
        store.clone(),
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget.clone());
    if let Some(gate) = &gate {
        metrics_router = metrics_router.with_write_gate(gate.clone());
        log_router = log_router.with_write_gate(gate.clone());
        span_router = span_router.with_write_gate(gate.clone());
    }
    let metrics_router = Arc::new(metrics_router);
    let admission = unlimited_admission();
    let normalize_metrics = Arc::new(NormalizeRejectMetrics::new());
    let state = Arc::new(GatewayState {
        tenant_resolver: Arc::new(FixedTenant),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        ingest: ravel_server::ingest::IngestState {
            router: metrics_router.clone(),
            limits: IngestLimits::default(),
            ack_deadline: Duration::from_secs(30),
            admission: admission.clone(),
            recovery: None,
            provisioning: None,
            metadata_sink: None,
            normalize_metrics: normalize_metrics.clone(),
            budget: budget.clone(),
        },
        logs_ingest: ravel_server::logs_ingest::LogIngestState {
            router: Arc::new(log_router),
            limits: LogIngestLimits::default(),
            ack_deadline: Duration::from_secs(30),
            admission: admission.clone(),
            store: store.clone(),
            recovery: None,
            provisioning: None,
            normalize_metrics: normalize_metrics.clone(),
            budget: budget.clone(),
        },
        traces_ingest: ravel_server::traces_ingest::SpanIngestState {
            router: Arc::new(span_router),
            limits: SpanIngestLimits::default(),
            ack_deadline: Duration::from_secs(30),
            admission: admission.clone(),
            store: store.clone(),
            recovery: None,
            provisioning: None,
            normalize_metrics,
            budget: budget.clone(),
        },
        admission,
        budget,
        ingest_concurrency: IngestConcurrencyController::shared(IngestConcurrencyLimit::Unlimited),
        ingest_byte_metrics: Arc::new(IngestByteMetrics::new()),
        write_gate: gate,
    });
    (state, metrics_router, store)
}

fn remote_write_state(gate: Option<Arc<WriteGate>>) -> (Arc<RemoteWriteState>, Arc<IngestRouter>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let config = IngestConfig {
        target_bytes: 1,
        ..IngestConfig::default()
    };
    let mut router = IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(budget.clone());
    if let Some(gate) = &gate {
        router = router.with_write_gate(gate.clone());
    }
    let router = Arc::new(router);
    let state = Arc::new(RemoteWriteState {
        tenant_resolver: Arc::new(FixedTenant),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        router: router.clone(),
        limits: IngestLimits::default(),
        ack_deadline: Duration::from_secs(30),
        metrics: RemoteWriteMetrics::default(),
        admission: unlimited_admission(),
        recovery: None,
        provisioning: None,
        ingest_concurrency: IngestConcurrencyController::shared(IngestConcurrencyLimit::Unlimited),
        clock: Arc::new(SystemClock),
        metadata_sink: None,
        budget,
        write_gate: gate,
    });
    (state, router)
}

fn flush_router(gate: Option<Arc<WriteGate>>) -> Arc<IngestRouter> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let config = IngestConfig {
        shard_count: 1,
        ..IngestConfig::default()
    };
    let mut router = IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        Arc::new(SystemClock),
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited));
    if let Some(gate) = gate {
        router = router.with_write_gate(gate);
    }
    Arc::new(router)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Path {
    OtlpGzip,
    RwSnappy,
    FlushEncode,
}

impl Path {
    fn name(self) -> &'static str {
        match self {
            Path::OtlpGzip => "otlp_gzip",
            Path::RwSnappy => "rw_snappy",
            Path::FlushEncode => "flush_encode",
        }
    }

    /// The gate site that decides whether this path's unit went through it.
    fn site(self) -> WriteSite {
        match self {
            Path::OtlpGzip => WriteSite::OtlpHttpGzip,
            Path::RwSnappy => WriteSite::RemoteWriteSnappy,
            Path::FlushEncode => WriteSite::MetricsFlush,
        }
    }
}

/// The payload a workload sends, shared by both modes.
enum Payload {
    Http(Arc<BodySet>),
    Flush(Arc<Vec<IngestPoint>>),
}

/// One mode's server side, rebuilt for every run so no run inherits another
/// run's buffers or stored objects.
enum Target {
    Http {
        app: axum::Router,
        router: Arc<IngestRouter>,
        bodies: Arc<BodySet>,
        path: Path,
        _store: Option<Arc<dyn ObjectStoreBackend>>,
    },
    Flush {
        router: Arc<IngestRouter>,
        points: Arc<Vec<IngestPoint>>,
    },
}

impl Target {
    fn build(path: Path, payload: &Payload, gate: Option<Arc<WriteGate>>) -> Self {
        match (path, payload) {
            (Path::OtlpGzip, Payload::Http(bodies)) => {
                let (state, router, store) = gateway_state(gate);
                Target::Http {
                    app: ravel_server::otlp_http::router(state),
                    router,
                    bodies: bodies.clone(),
                    path,
                    _store: Some(store),
                }
            }
            (Path::RwSnappy, Payload::Http(bodies)) => {
                let (state, router) = remote_write_state(gate);
                Target::Http {
                    app: ravel_server::remote_write::router(state),
                    router,
                    bodies: bodies.clone(),
                    path,
                    _store: None,
                }
            }
            (Path::FlushEncode, Payload::Flush(points)) => Target::Flush {
                router: flush_router(gate),
                points: points.clone(),
            },
            _ => panic!("payload does not match path"),
        }
    }

    fn request_bytes(&self, i: usize) -> u64 {
        match self {
            Target::Http { bodies, .. } => bodies.bodies[i % bodies.bodies.len()].len() as u64,
            Target::Flush { points, .. } => points.len() as u64 * 16,
        }
    }

    /// Sends request `i` and returns its latency. Building the request (and,
    /// for the flush, cloning the points it moves into the router) is outside
    /// the timed span.
    async fn send(&self, i: usize) -> Duration {
        match self {
            Target::Http {
                app, bodies, path, ..
            } => {
                let body = bodies.bodies[i % bodies.bodies.len()].clone();
                let request = match path {
                    Path::OtlpGzip => Request::post("/v1/metrics")
                        .header("content-type", "application/x-protobuf")
                        .header("content-encoding", "gzip")
                        .header(INGEST_MODE_HEADER, "buffered"),
                    Path::RwSnappy => Request::post("/api/v1/write")
                        .header("content-type", "application/x-protobuf")
                        .header("content-encoding", "snappy")
                        .header("x-prometheus-remote-write-version", "0.1.0"),
                    Path::FlushEncode => unreachable!("flush is not an HTTP path"),
                }
                .body(Body::from(body))
                .expect("request");
                let app = app.clone();
                let started = Instant::now();
                let response = app.oneshot(request).await.expect("infallible router");
                let elapsed = started.elapsed();
                let status = response.status();
                if status != StatusCode::OK && status != StatusCode::NO_CONTENT {
                    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                        .await
                        .unwrap_or_default();
                    panic!(
                        "{} answered {status}: {}",
                        path.name(),
                        String::from_utf8_lossy(&body)
                    );
                }
                elapsed
            }
            Target::Flush { router, points } => {
                let batch = points.as_ref().clone();
                let started = Instant::now();
                router
                    .write_values(
                        TenantId::new(TENANT),
                        batch,
                        WriteMode::Strict,
                        Duration::from_secs(60),
                    )
                    .await
                    .expect("strict flush write");
                started.elapsed()
            }
        }
    }

    fn ingest_router(&self) -> &Arc<IngestRouter> {
        match self {
            Target::Http { router, .. } | Target::Flush { router, .. } => router,
        }
    }

    /// Drains the buffers this run left so the next run starts with no
    /// background flush still encoding.
    async fn drain(&self) {
        self.ingest_router().flush_all().await;
    }
}

/// `clients` concurrent senders until `deadline`, or until `budget` requests
/// in total when it is set. Returns every request's latency and the wall time.
async fn drive(
    target: Arc<Target>,
    clients: usize,
    run_for: Option<Duration>,
    budget: Option<u64>,
) -> (Vec<Duration>, Duration) {
    let started = Instant::now();
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(clients);
    for client in 0..clients {
        let target = target.clone();
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut i = client;
            loop {
                if let Some(run_for) = run_for
                    && started.elapsed() >= run_for
                {
                    break;
                }
                if let Some(budget) = budget
                    && counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= budget
                {
                    break;
                }
                latencies.push(target.send(i).await);
                i += clients;
            }
            latencies
        }));
    }
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.expect("client task"));
    }
    (latencies, started.elapsed())
}

/// Every configuration of one path and size, both modes.
struct Workload {
    path: Path,
    size: &'static str,
    compressed_target: Option<usize>,
    payload: Payload,
    shape: String,
}

fn workloads() -> Vec<Workload> {
    let mut out = Vec::new();
    for (size, target) in COMPRESSED_SIZES {
        let set = otlp_bodies(target);
        let shape = format!(
            "{} hosts, {} series and {} points per body, {} B gzip of {} B protobuf, ratio {:.2}",
            set.units,
            set.series_per_body,
            set.series_per_body,
            set.compressed_bytes,
            set.uncompressed_bytes,
            set.uncompressed_bytes as f64 / set.compressed_bytes as f64
        );
        out.push(Workload {
            path: Path::OtlpGzip,
            size,
            compressed_target: Some(target),
            payload: Payload::Http(Arc::new(set)),
            shape,
        });
    }
    for (size, target) in COMPRESSED_SIZES {
        let set = rw_bodies(target);
        let shape = format!(
            "{} instances, {} series and {} samples per body, {} B snappy of {} B protobuf, ratio {:.2}",
            set.units,
            set.series_per_body,
            set.series_per_body,
            set.compressed_bytes,
            set.uncompressed_bytes,
            set.uncompressed_bytes as f64 / set.compressed_bytes as f64
        );
        out.push(Workload {
            path: Path::RwSnappy,
            size,
            compressed_target: Some(target),
            payload: Payload::Http(Arc::new(set)),
            shape,
        });
    }
    let (points, per_series) = flush_points(&TenantId::new(TENANT));
    let shape = format!(
        "{FLUSH_SERIES} series x {per_series} samples = {} points per strict write, one shard, \
         target_bytes {}",
        points.len(),
        IngestConfig::default().target_bytes
    );
    out.push(Workload {
        path: Path::FlushEncode,
        size: "8MiB",
        compressed_target: None,
        payload: Payload::Flush(Arc::new(points)),
        shape,
    });
    out
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

// ---------------------------------------------------------------------------
// Criterion
// ---------------------------------------------------------------------------

/// The jemalloc background purge thread, configured as the server binary's
/// `configure_allocator` does before it binds anything.
fn allocator() -> &'static ravel_server::mem_stats::BackgroundThread {
    static STATE: std::sync::OnceLock<ravel_server::mem_stats::BackgroundThread> =
        std::sync::OnceLock::new();
    STATE.get_or_init(|| {
        let malloc_conf = std::env::var(ravel_server::mem_stats::MALLOC_CONF_ENV).ok();
        ravel_server::mem_stats::configure_background_thread(malloc_conf.as_deref())
    })
}

fn bench_criterion(c: &mut Criterion) {
    allocator();
    let rt = runtime();
    // Router construction spawns the shard actors.
    let _runtime = rt.enter();
    let workloads = workloads();
    for w in &workloads {
        let mut group = c.benchmark_group(format!("ingest_write_gate_overhead/{}", w.path.name()));
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(2));
        for clients in CONCURRENCIES {
            for gated in [false, true] {
                let gate = gated.then(production_write_gate);
                let target = Arc::new(Target::build(w.path, &w.payload, gate));
                group.throughput(Throughput::Bytes(target.request_bytes(0)));
                let id = format!(
                    "{}/c{clients}/{}",
                    w.size,
                    if gated { "gated" } else { "ungated" }
                );
                group.bench_function(id, |b| {
                    b.iter_custom(|iters| {
                        let (_, wall) =
                            rt.block_on(drive(target.clone(), clients, None, Some(iters)));
                        wall
                    });
                });
                rt.block_on(target.drain());
            }
        }
        group.finish();
    }
}

// ---------------------------------------------------------------------------
// Table pass
// ---------------------------------------------------------------------------

struct RunResult {
    requests: usize,
    throughput: f64,
    bytes_per_sec: f64,
    p50: Duration,
    p99: Duration,
    site_jobs: u64,
    site_inline: u64,
    flush_jobs: u64,
    flush_inline: u64,
    flushes: u64,
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn site_counts(gate: &WriteGate, site: WriteSite) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|s| s.site == site)
        .map_or((0, 0), |s| (s.jobs, s.inline))
}

fn one_run(rt: &tokio::runtime::Runtime, w: &Workload, clients: usize, gated: bool) -> RunResult {
    let gate = gated.then(production_write_gate);
    let target = Arc::new(Target::build(w.path, &w.payload, gate.clone()));
    let run_for = if w.path == Path::FlushEncode {
        FLUSH_RUN
    } else {
        HTTP_RUN
    };
    // Warm the router (shard actors, first-series paths) before counting.
    rt.block_on(drive(target.clone(), clients, None, Some(clients as u64)));
    rt.block_on(target.drain());
    let flushes_before = flush_count(target.ingest_router());
    let site_before = gate
        .as_ref()
        .map_or((0, 0), |g| site_counts(g, w.path.site()));
    let flush_before = gate
        .as_ref()
        .map_or((0, 0), |g| site_counts(g, WriteSite::MetricsFlush));
    let (mut latencies, wall) = rt.block_on(drive(target.clone(), clients, Some(run_for), None));
    let site_after = gate
        .as_ref()
        .map_or((0, 0), |g| site_counts(g, w.path.site()));
    let flush_after = gate
        .as_ref()
        .map_or((0, 0), |g| site_counts(g, WriteSite::MetricsFlush));
    let flushes = flush_count(target.ingest_router()) - flushes_before;
    rt.block_on(target.drain());
    latencies.sort_unstable();
    let requests = latencies.len();
    let bytes: u64 = (0..requests).map(|i| target.request_bytes(i)).sum();
    let secs = wall.as_secs_f64();
    RunResult {
        requests,
        throughput: requests as f64 / secs,
        bytes_per_sec: bytes as f64 / secs,
        p50: percentile(&latencies, 0.50),
        p99: percentile(&latencies, 0.99),
        site_jobs: site_after.0 - site_before.0,
        site_inline: site_after.1 - site_before.1,
        flush_jobs: flush_after.0 - flush_before.0,
        flush_inline: flush_after.1 - flush_before.1,
        flushes,
    }
}

fn flush_count(router: &IngestRouter) -> u64 {
    let s = router.metrics().snapshot();
    s.flushes_by_size
        + s.flushes_by_age
        + s.flushes_by_age_adaptive
        + s.flushes_by_age_floor
        + s.flushes_manual
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `median [min-max]` of one figure across runs.
fn spread(runs: &[RunResult], figure: impl Fn(&RunResult) -> f64, scale: f64) -> String {
    let mut values: Vec<f64> = runs.iter().map(|r| figure(r) * scale).collect();
    let mid = median(&mut values);
    format!(
        "{mid:.3} [{:.3}-{:.3}]",
        values.first().copied().unwrap_or(0.0),
        values.last().copied().unwrap_or(0.0)
    )
}

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unavailable".to_string())
}

fn report_table(_c: &mut Criterion) {
    if !std::env::args().any(|a| a == "--bench") {
        return;
    }
    let rt = runtime();
    // Router construction spawns the shard actors.
    let _runtime = rt.enter();
    let workloads = workloads();
    let gate = production_write_gate();
    println!();
    println!("== ingest_write_gate_overhead table (issue #2669) ==");
    println!(
        "host: {} cores (available_parallelism); write gate permits {}, inline floor {} B; \
         {TABLE_RUNS} runs per configuration, gated/ungated interleaved; \
         run length {} ms (HTTP) / {} ms (flush)",
        cores(),
        gate.permits(),
        ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES,
        HTTP_RUN.as_millis(),
        FLUSH_RUN.as_millis()
    );
    drop(gate);
    let alloc = allocator();
    println!(
        "allocator background thread enabled={} (source {})",
        alloc.enabled, alloc.source
    );
    for w in &workloads {
        println!("payload {} {}: {}", w.path.name(), w.size, w.shape);
    }
    println!();
    println!(
        "figures are median [min-max] over runs; throughput req/s, MiB/s of request bytes \
         (compressed body for HTTP, 16 B a sample for flush), latency ms"
    );
    println!(
        "ratio = gated/ungated throughput, paired run by run: median [min-max]; band: \
         every paired ratio >= {BAND_RATIO} inside, every one below outside, otherwise \
         cannot tell"
    );
    println!();
    for w in &workloads {
        for clients in CONCURRENCIES {
            println!(
                "loadavg before {} {} c{clients}: {}",
                w.path.name(),
                w.size,
                loadavg()
            );
            let mut ungated = Vec::with_capacity(TABLE_RUNS);
            let mut gated = Vec::with_capacity(TABLE_RUNS);
            for run in 0..TABLE_RUNS {
                if run % 2 == 0 {
                    ungated.push(one_run(&rt, w, clients, false));
                    gated.push(one_run(&rt, w, clients, true));
                } else {
                    gated.push(one_run(&rt, w, clients, true));
                    ungated.push(one_run(&rt, w, clients, false));
                }
            }
            let mut ratios: Vec<f64> = gated
                .iter()
                .zip(&ungated)
                .map(|(g, u)| g.throughput / u.throughput)
                .collect();
            let ratio_mid = median(&mut ratios);
            let (rmin, rmax) = (ratios[0], ratios[ratios.len() - 1]);
            let banded = w
                .compressed_target
                .is_none_or(|bytes| bytes >= BAND_FROM_BYTES);
            let verdict = if !banded {
                "no band (below 64 KiB)"
            } else if rmin >= BAND_RATIO {
                "inside"
            } else if rmax < BAND_RATIO {
                "outside"
            } else {
                "cannot tell from the spread"
            };
            let label = format!("{} {} c{clients}", w.path.name(), w.size);
            for (mode, runs) in [("ungated", &ungated), ("gated", &gated)] {
                let requests: usize = runs.iter().map(|r| r.requests).sum();
                println!(
                    "{label:<24} {mode:<8} req/s {:<28} MiB/s {:<28} p50 {:<28} p99 {:<28} \
                     requests {requests}",
                    spread(runs, |r| r.throughput, 1.0),
                    spread(runs, |r| r.bytes_per_sec, 1.0 / (1024.0 * 1024.0)),
                    spread(runs, |r| ms(r.p50), 1.0),
                    spread(runs, |r| ms(r.p99), 1.0),
                );
            }
            let sum = |f: fn(&RunResult) -> u64| gated.iter().map(f).sum::<u64>();
            let gated_requests: usize = gated.iter().map(|r| r.requests).sum();
            println!(
                "{label:<24} ratio {ratio_mid:.4} [{rmin:.4}-{rmax:.4}] verdict: {verdict}; \
                 gated runs: {gated_requests} requests, {} {} jobs, {} {} inline, \
                 metrics_flush {} jobs {} inline, {} flushes",
                sum(|r| r.site_jobs),
                w.path.site().name(),
                sum(|r| r.site_inline),
                w.path.site().name(),
                sum(|r| r.flush_jobs),
                sum(|r| r.flush_inline),
                sum(|r| r.flushes),
            );
            println!();
        }
    }
    println!("loadavg after: {}", loadavg());
}

criterion_group!(benches, bench_criterion, report_table);
criterion_main!(benches);
