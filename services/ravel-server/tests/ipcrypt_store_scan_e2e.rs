//! End-to-end proof that an `--ipcrypt-config` rule reaches every ingest
//! surface (ADR-2993, issue #3048): a real server ingests one address under a
//! deterministic rule on `client.ip` through every surface, flushes, and then
//! every byte of every stored object, with every zstd frame inside it
//! decompressed, is searched for the address in each form it could take. A
//! control run with no policy finds the raw address, so the scan can see it.
//!
//! Each production rule site and the ingest case that drives it:
//!
//! - `ingest::handle_export_releasing` (OTLP metrics): `otlp_metrics`, over
//!   `POST /v1/metrics` and the gRPC `MetricsService`, a point attribute and an
//!   exemplar's filtered attribute.
//! - `logs_ingest::handle_export_logs_releasing`: `otlp_logs`, over
//!   `POST /v1/logs` and the gRPC `LogsService`, a resource attribute and a
//!   record attribute.
//! - `traces_ingest::handle_export_traces_releasing`: `otlp_traces`, over
//!   `POST /v1/traces` and the gRPC `TraceService`, a resource attribute, a span
//!   attribute, a span event attribute and a span link attribute.
//! - `remote_write::remote_write`: `remote_write_body`, a sample series
//!   labelled `client_ip`, one labelled with the dotted `client.ip`, and an
//!   exemplar labelled `client.ip`.
//! - `otap_grpc::submit_batch` (`otap` feature only): `otap_batch`, a point
//!   attribute on the `ArrowMetricsService` stream.
//!
//! The server is given no `ServerConfig::deployment_key`: the IPCrypt policy
//! carries its own copy of the key, and leaving the server's unset keeps
//! durable auth and recovery out of a test about attribute values.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value as ExemplarValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_remote_write::proto::prometheus::{
    Exemplar as RwExemplar, Label as RwLabel, Sample as RwSample, TimeSeries as RwTimeSeries,
    WriteRequest as RwWriteRequest,
};
use ravel_server::ipcrypt::IpcryptPolicy;
use ravel_server::{FoldTaskConfig, IngestByteBudgetLimit, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

/// The address every surface carries (RFC 5737 TEST-NET-2).
const ADDR: &str = "198.51.100.73";
const ADDR_V4: [u8; 4] = [198, 51, 100, 73];

/// A ruled value that is not an address: stored as sent and counted
/// `unparsed`.
const UNPARSED_KEY: &str = "forwarded.for";
const UNPARSED_VALUE: &str = "unknown";

const DEPLOYMENT_KEY: [u8; 32] = [0x5a; 32];

const POLICY: &str = r#"
[[defaults.rules]]
key = "client.ip"
variant = "deterministic"

[[defaults.rules]]
key = "forwarded.for"
variant = "deterministic"
"#;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn ruled_attrs() -> Vec<KeyValue> {
    vec![kv("client.ip", ADDR), kv(UNPARSED_KEY, UNPARSED_VALUE)]
}

fn policy() -> Arc<IpcryptPolicy> {
    let file =
        ravel_ipcrypt::policy::parse_policy_file(POLICY, ravel_server::ipcrypt::sanitize_rule_key)
            .expect("policy parses");
    Arc::new(IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads"))
}

/// What the rule stores for [`ADDR`], computed through the same policy the
/// server runs.
fn ciphertext(policy: &IpcryptPolicy) -> String {
    let rules = policy
        .rules_for(&TenantId::new(TENANT))
        .expect("acme resolves");
    let (stored, outcome) = rules.apply("client.ip", ADDR).expect("client.ip is ruled");
    assert_eq!(outcome, ravel_ipcrypt::render::RenderOutcome::Encrypted);
    assert_ne!(stored, ADDR);
    stored
}

/// One gauge point and one exemplar, each carrying the ruled attributes.
fn otlp_metrics(name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: ruled_attrs(),
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
                            exemplars: vec![Exemplar {
                                filtered_attributes: vec![kv("client.ip", ADDR)],
                                time_unix_nano: ts_ns as u64,
                                value: Some(ExemplarValue::AsDouble(1.0)),
                                span_id: vec![0x11; 8],
                                trace_id: vec![0x22; 16],
                            }],
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

fn otlp_logs(service: &str, ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service), kv("client.ip", ADDR)],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: 9,
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("request served".into())),
                    }),
                    attributes: ruled_attrs(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn otlp_traces(service: &str, trace_byte: u8, ts_ns: i64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service), kv("client.ip", ADDR)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![trace_byte; 16],
                    span_id: vec![trace_byte; 8],
                    name: "GET /".to_string(),
                    kind: 2,
                    start_time_unix_nano: (ts_ns - 1_000) as u64,
                    end_time_unix_nano: ts_ns as u64,
                    attributes: ruled_attrs(),
                    events: vec![Event {
                        time_unix_nano: (ts_ns - 500) as u64,
                        name: "accepted".to_string(),
                        attributes: vec![kv("client.ip", ADDR)],
                        ..Default::default()
                    }],
                    links: vec![Link {
                        trace_id: vec![0x33; 16],
                        span_id: vec![0x44; 8],
                        attributes: vec![kv("client.ip", ADDR)],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn rw_label(name: &str, value: &str) -> RwLabel {
    RwLabel {
        name: name.to_string(),
        value: value.to_string(),
    }
}

/// Two sample series, one labelled `client_ip` and one with the dotted
/// `client.ip` (looked up by its sanitized name), the first carrying an
/// exemplar labelled `client.ip`.
fn remote_write_body(ts_ms: i64) -> Vec<u8> {
    let request = RwWriteRequest {
        timeseries: vec![
            RwTimeSeries {
                labels: vec![
                    rw_label("__name__", "rw_requests"),
                    rw_label("client_ip", ADDR),
                    rw_label("job", "edge"),
                ],
                samples: vec![RwSample {
                    value: 1.0,
                    timestamp: ts_ms,
                }],
                exemplars: vec![RwExemplar {
                    labels: vec![rw_label("client.ip", ADDR)],
                    value: 1.0,
                    timestamp: ts_ms,
                }],
                histograms: vec![],
            },
            RwTimeSeries {
                labels: vec![
                    rw_label("__name__", "rw_dotted"),
                    rw_label("client.ip", ADDR),
                    rw_label("job", "edge"),
                    rw_label(UNPARSED_KEY, UNPARSED_VALUE),
                ],
                samples: vec![RwSample {
                    value: 2.0,
                    timestamp: ts_ms,
                }],
                exemplars: vec![],
                histograms: vec![],
            },
        ],
        metadata: vec![],
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy")
}

#[cfg(feature = "otap")]
fn otap_batch(ts_ns: i64) -> ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords {
    use ravel_otap::encode::{AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow};
    let attr = |key: &str, value: &str| AttrRow {
        key: key.to_string(),
        value: AttrValue::Str(value.to_string()),
    };
    let metrics = vec![MetricRow {
        name: "otap_requests".to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: ts_ns,
            value: 1.0,
            flags: 0,
            exemplars: vec![],
            attrs: vec![attr("client.ip", ADDR), attr(UNPARSED_KEY, UNPARSED_VALUE)],
        }],
    }];
    let mut encoder =
        ravel_otap::encode::MetricsStreamEncoder::new("ipcrypt-scan").expect("new encoder");
    encoder.encode_batch(0, &metrics).expect("encode batch")
}

async fn start_server(
    policy: Arc<IpcryptPolicy>,
) -> (ravel_server::Running, Arc<dyn ObjectStoreBackend>) {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
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
        otap: cfg!(feature = "otap"),
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
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start_with_ipcrypt(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
        policy,
    )
    .await
    .expect("server starts");
    (running, store)
}

async fn post(base: &str, path: &str, content_type: &str, body: Vec<u8>) -> u16 {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .expect("request completes")
        .status()
        .as_u16()
}

fn authorized<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("valid metadata"),
    );
    request
}

/// Drives every ingest case in the module docs, asserting each was accepted
/// with nothing rejected.
async fn ingest_everything(running: &ravel_server::Running) {
    let base = format!("http://{}", running.http_addr);
    let grpc = format!(
        "http://{}",
        running.grpc_addr.expect("Mode::All binds gRPC")
    );
    let ts = now_ns();
    let protobuf = "application/x-protobuf";

    let http_metrics = post(
        &base,
        "/v1/metrics",
        protobuf,
        otlp_metrics("http_requests", ts).encode_to_vec(),
    );
    assert_eq!(http_metrics.await, 200, "OTLP HTTP metrics");
    let http_logs = post(
        &base,
        "/v1/logs",
        protobuf,
        otlp_logs("http-svc", ts).encode_to_vec(),
    );
    assert_eq!(http_logs.await, 200, "OTLP HTTP logs");
    let http_traces = post(
        &base,
        "/v1/traces",
        protobuf,
        otlp_traces("http-svc", 0xa1, ts).encode_to_vec(),
    );
    assert_eq!(http_traces.await, 200, "OTLP HTTP traces");

    let metrics = MetricsServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(otlp_metrics("grpc_requests", ts)))
        .await
        .expect("OTLP gRPC metrics")
        .into_inner();
    assert!(metrics.partial_success.is_none(), "{metrics:?}");
    let logs = LogsServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(otlp_logs("grpc-svc", ts)))
        .await
        .expect("OTLP gRPC logs")
        .into_inner();
    assert!(logs.partial_success.is_none(), "{logs:?}");
    let traces = TraceServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(otlp_traces("grpc-svc", 0xb2, ts)))
        .await
        .expect("OTLP gRPC traces")
        .into_inner();
    assert!(traces.partial_success.is_none(), "{traces:?}");

    let remote_write = post(
        &base,
        "/api/v1/write",
        "application/x-protobuf;proto=prometheus.WriteRequest",
        remote_write_body(ts / 1_000_000),
    );
    assert_eq!(remote_write.await, 204, "Remote Write");

    #[cfg(feature = "otap")]
    {
        use ravel_otap::proto::experimental::arrow::v1::StatusCode;
        use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_client::ArrowMetricsServiceClient;
        let mut inbound = ArrowMetricsServiceClient::connect(grpc)
            .await
            .expect("connect")
            .arrow_metrics(authorized(futures::stream::iter(vec![otap_batch(ts)])))
            .await
            .expect("arrow_metrics stream opens")
            .into_inner();
        let status = inbound
            .message()
            .await
            .expect("read batch status")
            .expect("one batch status");
        assert_eq!(
            status.status_code,
            StatusCode::Ok as i32,
            "OTAP: {status:?}"
        );
    }
}

/// One stored object: its key, its bytes, and every zstd frame inside it,
/// decompressed recursively.
struct Stored {
    key: String,
    blobs: Vec<Vec<u8>>,
}

impl Stored {
    /// The signal segment of a tenant key (`t/<tenant_hash>/<signal>/...`).
    fn signal(&self) -> Option<&str> {
        let mut parts = self.key.split('/');
        (parts.next() == Some("t")).then_some(())?;
        parts.nth(1)
    }

    fn contains(&self, needle: &[u8]) -> bool {
        self.blobs.iter().any(|blob| find(blob, needle).is_some())
    }
}

async fn stored_objects(store: &Arc<dyn ObjectStoreBackend>) -> Vec<Stored> {
    let objects = list_all(store.as_ref(), "")
        .await
        .expect("list every object");
    let mut stored = Vec::new();
    for object in objects {
        let data = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("read object")
            .data
            .to_vec();
        let mut blobs = Vec::new();
        expand_zstd(&data, 0, &mut blobs);
        blobs.push(data);
        stored.push(Stored {
            key: object.key,
            blobs,
        });
    }
    stored
}

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;

/// Decodes one zstd frame at every offset carrying the frame magic, so a
/// frame followed by other bytes (a footer, the next section) still decodes.
fn expand_zstd(data: &[u8], depth: usize, out: &mut Vec<Vec<u8>>) {
    if depth > 3 {
        return;
    }
    let mut at = 0;
    while let Some(offset) = find(&data[at..], &ZSTD_MAGIC) {
        let start = at + offset;
        if let Some(decoded) = decode_frame(&data[start..]) {
            expand_zstd(&decoded, depth + 1, out);
            out.push(decoded);
        }
        at = start + 1;
    }
}

fn decode_frame(bytes: &[u8]) -> Option<Vec<u8>> {
    let decoder = zstd::stream::read::Decoder::new(bytes).ok()?.single_frame();
    let mut decoded = Vec::new();
    decoder
        .take(MAX_FRAME_BYTES)
        .read_to_end(&mut decoded)
        .ok()?;
    Some(decoded)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The forms a stored address could take: text, 4-byte binary, 16-byte
/// v4-mapped binary, and the lowercase hex of each binary form.
fn address_forms() -> Vec<(&'static str, Vec<u8>)> {
    let mut mapped = [0u8; 16];
    mapped[10] = 0xff;
    mapped[11] = 0xff;
    mapped[12..].copy_from_slice(&ADDR_V4);
    vec![
        ("text", ADDR.as_bytes().to_vec()),
        ("ipv4 binary", ADDR_V4.to_vec()),
        ("v4-mapped binary", mapped.to_vec()),
        ("ipv4 hex", hex(&ADDR_V4).into_bytes()),
        ("v4-mapped hex", hex(&mapped).into_bytes()),
    ]
}

/// Keys of the objects carrying `needle`.
fn holding<'a>(stored: &'a [Stored], needle: &[u8]) -> Vec<&'a str> {
    stored
        .iter()
        .filter(|object| object.contains(needle))
        .map(|object| object.key.as_str())
        .collect()
}

/// Whether some object of `signal` (`m`, `l` or `s`) carries `needle`.
fn signal_holds(stored: &[Stored], signal: &str, needle: &[u8]) -> bool {
    stored
        .iter()
        .any(|object| object.signal() == Some(signal) && object.contains(needle))
}

const SIGNALS: [&str; 3] = ["m", "l", "s"];

/// The `ravel_ingest_ipcrypt_values_total` sample for one label set.
async fn ipcrypt_sample(base: &str, signal: &str, outcome: &str) -> u64 {
    let body = reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body");
    let matching: Vec<&str> = body
        .lines()
        .filter(|line| {
            line.starts_with("ravel_ingest_ipcrypt_values_total{")
                && line.contains(&format!("signal=\"{signal}\""))
                && line.contains("variant=\"deterministic\"")
                && line.contains(&format!("outcome=\"{outcome}\""))
        })
        .collect();
    assert_eq!(matching.len(), 1, "one sample: {matching:?}");
    matching[0]
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .expect("numeric sample")
}

#[tokio::test]
async fn no_stored_byte_carries_the_address_under_a_rule() {
    let policy = policy();
    let expected = ciphertext(&policy);
    let (running, store) = start_server(policy).await;
    ingest_everything(&running).await;
    running.shutdown().await.expect("graceful shutdown");

    let stored = stored_objects(&store).await;
    for (form, needle) in address_forms() {
        let found = holding(&stored, &needle);
        assert!(
            found.is_empty(),
            "the address's {form} form is stored, of {} objects, in {found:?}",
            stored.len()
        );
    }
    for signal in SIGNALS {
        assert!(
            signal_holds(&stored, signal, expected.as_bytes()),
            "no `{signal}` object carries the ciphertext {expected}"
        );
    }
}

/// The control: with no policy the same ingest stores the raw address, and
/// the scan sees it in every signal's objects.
#[tokio::test]
async fn the_scan_finds_the_address_with_no_policy() {
    let (running, store) = start_server(IpcryptPolicy::disabled()).await;
    ingest_everything(&running).await;
    running.shutdown().await.expect("graceful shutdown");

    let stored = stored_objects(&store).await;
    for signal in SIGNALS {
        assert!(
            signal_holds(&stored, signal, ADDR.as_bytes()),
            "no `{signal}` object carries the raw address, of {} objects",
            stored.len()
        );
    }
}

#[tokio::test]
async fn each_outcome_is_counted_on_the_metrics_route() {
    let (running, _store) = start_server(policy()).await;
    ingest_everything(&running).await;
    let base = format!("http://{}", running.http_addr);
    for signal in ["metrics", "logs", "spans"] {
        assert!(
            ipcrypt_sample(&base, signal, "encrypted").await > 0,
            "{signal} encrypted"
        );
        assert!(
            ipcrypt_sample(&base, signal, "unparsed").await > 0,
            "{signal} unparsed"
        );
    }
    running.shutdown().await.expect("graceful shutdown");
}
