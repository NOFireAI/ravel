//! End-to-end proof that an `--ipcrypt-config` rule reaches every ingest
//! surface (ADR-2993, issue #3048). A real server on a keyed bucket ingests a
//! distinct address per ingest case under a deterministic rule on
//! `client.ip`, shuts down (flushing every buffer), and the test then folds
//! every signal past its seal window with an injected clock and submits one
//! erasure request. Every byte of every object in the bucket, with every zstd
//! frame inside it decompressed, is then searched for each case's address in
//! each form it could take, and for each case's ciphertext. A control run
//! with no policy finds every case's raw address, so the scan can see each
//! one.
//!
//! The scanned bucket holds data objects, commit records, catalog snapshot
//! parts and HEADs, name postings, column statistics (the tenant declares
//! `client.ip` as a typed logs column), an erasure request under `del/`, the
//! tenancy marker and the tenant's recovery manifest, besides the tenant
//! config, metric metadata, and each signal's provenance and admission
//! snapshot. It holds no key-index
//! leaf, because the fold builds none unless
//! `CatalogConfig::build_key_index_leaves` is set, and no erasure completion,
//! because no maintenance pass runs.
//!
//! Each production rule site and the ingest cases that drive it, every case
//! over both HTTP and gRPC unless noted:
//!
//! - `ingest::handle_export_releasing` (OTLP metrics): a point attribute and
//!   an exemplar's filtered attribute.
//! - `logs_ingest::handle_export_logs_releasing`: a resource attribute and a
//!   record attribute.
//! - `traces_ingest::handle_export_traces_releasing`: a resource attribute
//!   (under its own ruled key `host.ip`), a span attribute, a span event
//!   attribute and a span link attribute.
//! - `remote_write::remote_write` (HTTP only): a sample series labelled
//!   `client_ip` and one labelled with the dotted `client.ip`.
//! - `otap_grpc::submit_batch` (`otap` feature only): a point attribute on
//!   the `ArrowMetricsService` stream.
//!
//! Remote Write and OTAP each also carry an exemplar with its own address.
//! Neither path stores exemplars, so those addresses must be absent in both
//! runs and their values must not reach `ravel_ingest_ipcrypt_values_total`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::net::Ipv4Addr;
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
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, DeclaredColumnType, DeclaredTypedColumn, TenantConfig,
    TenantLifecycleState,
};
use ravel_ingest::{AdmissionLimits, CountLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_remote_write::proto::prometheus::{
    Exemplar as RwExemplar, Label as RwLabel, Sample as RwSample, TimeSeries as RwTimeSeries,
    WriteRequest as RwWriteRequest,
};
use ravel_server::ipcrypt::IpcryptPolicy;
use ravel_server::tenancy::ConfiguredScheme;
use ravel_server::{FoldTaskConfig, IngestByteBudgetLimit, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

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

[[defaults.rules]]
key = "host.ip"
variant = "deterministic"
"#;

/// The ruled key a span's resource carries its address under. A span merges
/// its resource attributes over its own, so a resource `client.ip` would
/// replace the span's.
const SPAN_RESOURCE_KEY: &str = "host.ip";

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SEAL_MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// Last octet of each case's address in 198.51.100.0/24 (RFC 5737
/// TEST-NET-2). Every octet has three digits, so no case's text form is a
/// substring of another's.
mod octet {
    pub const HTTP_METRIC_POINT: u8 = 101;
    pub const HTTP_METRIC_EXEMPLAR: u8 = 102;
    pub const GRPC_METRIC_POINT: u8 = 103;
    pub const GRPC_METRIC_EXEMPLAR: u8 = 104;
    pub const HTTP_LOG_RESOURCE: u8 = 105;
    pub const HTTP_LOG_RECORD: u8 = 106;
    pub const GRPC_LOG_RESOURCE: u8 = 107;
    pub const GRPC_LOG_RECORD: u8 = 108;
    pub const HTTP_SPAN_RESOURCE: u8 = 109;
    pub const HTTP_SPAN_ATTR: u8 = 110;
    pub const HTTP_SPAN_EVENT: u8 = 111;
    pub const HTTP_SPAN_LINK: u8 = 112;
    pub const GRPC_SPAN_RESOURCE: u8 = 113;
    pub const GRPC_SPAN_ATTR: u8 = 114;
    pub const GRPC_SPAN_EVENT: u8 = 115;
    pub const GRPC_SPAN_LINK: u8 = 116;
    pub const RW_UNDERSCORE: u8 = 117;
    pub const RW_DOTTED: u8 = 118;
    pub const RW_EXEMPLAR: u8 = 119;
    #[cfg(feature = "otap")]
    pub const OTAP_POINT: u8 = 120;
    #[cfg(feature = "otap")]
    pub const OTAP_EXEMPLAR: u8 = 121;
}

fn addr(octet: u8) -> String {
    Ipv4Addr::new(198, 51, 100, octet).to_string()
}

/// Every ingest case whose address the server stores.
fn stored_cases() -> Vec<(&'static str, u8)> {
    use octet::*;
    #[allow(unused_mut)]
    let mut cases = vec![
        ("OTLP HTTP metrics point attribute", HTTP_METRIC_POINT),
        ("OTLP HTTP metrics exemplar", HTTP_METRIC_EXEMPLAR),
        ("OTLP gRPC metrics point attribute", GRPC_METRIC_POINT),
        ("OTLP gRPC metrics exemplar", GRPC_METRIC_EXEMPLAR),
        ("OTLP HTTP logs resource attribute", HTTP_LOG_RESOURCE),
        ("OTLP HTTP logs record attribute", HTTP_LOG_RECORD),
        ("OTLP gRPC logs resource attribute", GRPC_LOG_RESOURCE),
        ("OTLP gRPC logs record attribute", GRPC_LOG_RECORD),
        ("OTLP HTTP span resource attribute", HTTP_SPAN_RESOURCE),
        ("OTLP HTTP span attribute", HTTP_SPAN_ATTR),
        ("OTLP HTTP span event", HTTP_SPAN_EVENT),
        ("OTLP HTTP span link", HTTP_SPAN_LINK),
        ("OTLP gRPC span resource attribute", GRPC_SPAN_RESOURCE),
        ("OTLP gRPC span attribute", GRPC_SPAN_ATTR),
        ("OTLP gRPC span event", GRPC_SPAN_EVENT),
        ("OTLP gRPC span link", GRPC_SPAN_LINK),
        ("Remote Write sample, client_ip", RW_UNDERSCORE),
        ("Remote Write sample, client.ip", RW_DOTTED),
    ];
    #[cfg(feature = "otap")]
    cases.push(("OTAP point attribute", OTAP_POINT));
    cases
}

/// The ruled key a case's address is sent under.
fn case_key(case_octet: u8) -> &'static str {
    match case_octet {
        octet::HTTP_SPAN_RESOURCE | octet::GRPC_SPAN_RESOURCE => SPAN_RESOURCE_KEY,
        _ => "client.ip",
    }
}

/// Every ingest case whose address the server discards with its exemplar.
fn dropped_cases() -> Vec<(&'static str, u8)> {
    #[allow(unused_mut)]
    let mut cases = vec![("Remote Write exemplar", octet::RW_EXEMPLAR)];
    #[cfg(feature = "otap")]
    cases.push(("OTAP exemplar", octet::OTAP_EXEMPLAR));
    cases
}

/// The exact `ravel_ingest_ipcrypt_values_total{variant="deterministic"}`
/// values one [`ingest_everything`] produces, per `(signal, outcome)`: one per
/// ruled value in [`stored_cases`] and one `unparsed` per `forwarded.for`
/// value sent on a stored point, record or span, and none for
/// [`dropped_cases`].
fn expected_counts() -> [(&'static str, &'static str, u64); 6] {
    let otap = u64::from(cfg!(feature = "otap"));
    [
        ("metrics", "encrypted", 4 + 2 + otap),
        ("metrics", "unparsed", 2 + 1 + otap),
        ("logs", "encrypted", 4),
        ("logs", "unparsed", 2),
        ("spans", "encrypted", 8),
        ("spans", "unparsed", 2),
    ]
}

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

fn ruled_attrs(octet: u8) -> Vec<KeyValue> {
    vec![
        kv("client.ip", &addr(octet)),
        kv(UNPARSED_KEY, UNPARSED_VALUE),
    ]
}

fn policy() -> Arc<IpcryptPolicy> {
    let file =
        ravel_ipcrypt::policy::parse_policy_file(POLICY, ravel_server::ipcrypt::sanitize_rule_key)
            .expect("policy parses");
    Arc::new(IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads"))
}

/// What the rule on `key` stores for `address`, computed through the same
/// policy the server runs.
fn ciphertext(policy: &IpcryptPolicy, key: &str, address: &str) -> String {
    let rules = policy
        .rules_for(&TenantId::new(TENANT))
        .expect("acme resolves");
    let (stored, outcome) = rules.apply(key, address).expect("the key is ruled");
    assert_eq!(outcome, ravel_ipcrypt::render::RenderOutcome::Encrypted);
    assert_ne!(stored, address);
    stored
}

/// A fresh bucket pinned to the keyed tenant-hash scheme the way `main`
/// pins it: `resolve_and_pin` writes `sys/tenancy`, and the resolved scheme is
/// installed process-wide. Every test in this binary calls this before it
/// hashes a tenant, and all use [`DEPLOYMENT_KEY`], so the scheme a second
/// install refuses is the one already in force.
async fn keyed_bucket() -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let resolved = ravel_server::tenancy::resolve_and_pin(
        store.as_ref(),
        ConfiguredScheme::Keyed(Box::new(DEPLOYMENT_KEY)),
        now_ns(),
    )
    .await
    .expect("a fresh bucket pins the keyed scheme");
    let _already_installed = ravel_types::install_tenant_hash_scheme(resolved.scheme);
    store
}

/// Declares `client.ip` as a typed logs attribute column, so the logs fold
/// builds a column-statistics object over its values.
async fn declare_typed_client_ip(store: &dyn ObjectStoreBackend) {
    let config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: "client.ip".to_string(),
            ty: DeclaredColumnType::Str,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(store, &TenantId::new(TENANT).hash(), &config, now_ns())
        .await
        .expect("tenant config");
}

/// One gauge point and one exemplar, each with its own address.
fn otlp_metrics(name: &str, ts_ns: i64, point: u8, exemplar: u8) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: ruled_attrs(point),
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
                            exemplars: vec![Exemplar {
                                filtered_attributes: vec![kv("client.ip", &addr(exemplar))],
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

fn otlp_logs(service: &str, ts_ns: i64, resource: u8, record: u8) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", service),
                    kv("client.ip", &addr(resource)),
                ],
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
                    attributes: ruled_attrs(record),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Octets of one trace request's four address-carrying sites.
struct SpanOctets {
    resource: u8,
    span: u8,
    event: u8,
    link: u8,
}

fn otlp_traces(
    service: &str,
    trace_byte: u8,
    ts_ns: i64,
    octets: &SpanOctets,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", service),
                    kv(SPAN_RESOURCE_KEY, &addr(octets.resource)),
                ],
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
                    attributes: ruled_attrs(octets.span),
                    events: vec![Event {
                        time_unix_nano: (ts_ns - 500) as u64,
                        name: "accepted".to_string(),
                        attributes: vec![kv("client.ip", &addr(octets.event))],
                        ..Default::default()
                    }],
                    links: vec![Link {
                        trace_id: vec![0x33; 16],
                        span_id: vec![0x44; 8],
                        attributes: vec![kv("client.ip", &addr(octets.link))],
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
                    rw_label("client_ip", &addr(octet::RW_UNDERSCORE)),
                    rw_label("job", "edge"),
                ],
                samples: vec![RwSample {
                    value: 1.0,
                    timestamp: ts_ms,
                }],
                exemplars: vec![RwExemplar {
                    labels: vec![rw_label("client.ip", &addr(octet::RW_EXEMPLAR))],
                    value: 1.0,
                    timestamp: ts_ms,
                }],
                histograms: vec![],
            },
            RwTimeSeries {
                labels: vec![
                    rw_label("__name__", "rw_dotted"),
                    rw_label("client.ip", &addr(octet::RW_DOTTED)),
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

/// One gauge point carrying the ruled attributes and one exemplar with its
/// own address.
#[cfg(feature = "otap")]
fn otap_batch(ts_ns: i64) -> ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords {
    use ravel_otap::encode::{
        AttrRow, AttrValue, DataPointRow, ExemplarRow, ExemplarValue as OtapExemplarValue,
        MetricKind, MetricRow,
    };
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
            exemplars: vec![ExemplarRow {
                time_unix_nano: ts_ns,
                value: OtapExemplarValue::Double(1.0),
                trace_id: Some([0x55; 16]),
                span_id: Some([0x66; 8]),
                attrs: vec![attr("client.ip", &addr(octet::OTAP_EXEMPLAR))],
            }],
            attrs: vec![
                attr("client.ip", &addr(octet::OTAP_POINT)),
                attr(UNPARSED_KEY, UNPARSED_VALUE),
            ],
        }],
    }];
    let mut encoder =
        ravel_otap::encode::MetricsStreamEncoder::new("ipcrypt-scan").expect("new encoder");
    encoder.encode_batch(0, &metrics).expect("encode batch")
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    policy: Arc<IpcryptPolicy>,
    limits: LimitsConfig,
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
        otap: cfg!(feature = "otap"),
        metrics_tenant_labels: false,
        limits,
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: Some(Box::new(DEPLOYMENT_KEY)),
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
    ravel_server::start_with_ipcrypt(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
        policy,
    )
    .await
    .expect("server starts")
}

/// `acme` admits no active series and no active stream: every metrics point
/// and every log record normalizes and is then dropped by the layer-4 cap.
fn zero_cap_limits() -> LimitsConfig {
    let mut tenants = HashMap::new();
    tenants.insert(
        TenantId::new(TENANT),
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(0),
            max_active_streams: CountLimit::Bounded(0),
            ..AdmissionLimits::default()
        },
    );
    LimitsConfig {
        defaults: ravel_server::config::limits::shipped_defaults(),
        tenants,
        ..LimitsConfig::default()
    }
}

async fn post(base: &str, path: &str, content_type: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .expect("request completes")
}

fn authorized<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("valid metadata"),
    );
    request
}

const PROTOBUF: &str = "application/x-protobuf";
const RW_CONTENT_TYPE: &str = "application/x-protobuf;proto=prometheus.WriteRequest";

/// Sends the OTAP batch and returns its one `BatchStatus`.
#[cfg(feature = "otap")]
async fn send_otap(grpc: &str, ts: i64) -> ravel_otap::proto::experimental::arrow::v1::BatchStatus {
    use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_client::ArrowMetricsServiceClient;
    let mut inbound = ArrowMetricsServiceClient::connect(grpc.to_string())
        .await
        .expect("connect")
        .arrow_metrics(authorized(futures::stream::iter(vec![otap_batch(ts)])))
        .await
        .expect("arrow_metrics stream opens")
        .into_inner();
    inbound
        .message()
        .await
        .expect("read batch status")
        .expect("one batch status")
}

/// Drives every ingest case in the module docs, asserting each was accepted
/// with nothing rejected.
async fn ingest_everything(running: &ravel_server::Running) {
    use octet::*;
    let base = format!("http://{}", running.http_addr);
    let grpc = format!(
        "http://{}",
        running.grpc_addr.expect("Mode::All binds gRPC")
    );
    let ts = now_ns();

    let http_metrics = otlp_metrics("http_requests", ts, HTTP_METRIC_POINT, HTTP_METRIC_EXEMPLAR);
    let response = post(&base, "/v1/metrics", PROTOBUF, http_metrics.encode_to_vec()).await;
    assert_eq!(response.status(), 200, "OTLP HTTP metrics");
    let http_logs = otlp_logs("http-svc", ts, HTTP_LOG_RESOURCE, HTTP_LOG_RECORD);
    let response = post(&base, "/v1/logs", PROTOBUF, http_logs.encode_to_vec()).await;
    assert_eq!(response.status(), 200, "OTLP HTTP logs");
    let http_span_octets = SpanOctets {
        resource: HTTP_SPAN_RESOURCE,
        span: HTTP_SPAN_ATTR,
        event: HTTP_SPAN_EVENT,
        link: HTTP_SPAN_LINK,
    };
    let http_traces = otlp_traces("http-svc", 0xa1, ts, &http_span_octets);
    let response = post(&base, "/v1/traces", PROTOBUF, http_traces.encode_to_vec()).await;
    assert_eq!(response.status(), 200, "OTLP HTTP traces");

    let grpc_metrics = otlp_metrics("grpc_requests", ts, GRPC_METRIC_POINT, GRPC_METRIC_EXEMPLAR);
    let metrics = MetricsServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(grpc_metrics))
        .await
        .expect("OTLP gRPC metrics")
        .into_inner();
    assert!(metrics.partial_success.is_none(), "{metrics:?}");
    let logs = LogsServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(otlp_logs(
            "grpc-svc",
            ts,
            GRPC_LOG_RESOURCE,
            GRPC_LOG_RECORD,
        )))
        .await
        .expect("OTLP gRPC logs")
        .into_inner();
    assert!(logs.partial_success.is_none(), "{logs:?}");
    let grpc_span_octets = SpanOctets {
        resource: GRPC_SPAN_RESOURCE,
        span: GRPC_SPAN_ATTR,
        event: GRPC_SPAN_EVENT,
        link: GRPC_SPAN_LINK,
    };
    let traces = TraceServiceClient::connect(grpc.clone())
        .await
        .expect("connect")
        .export(authorized(otlp_traces(
            "grpc-svc",
            0xb2,
            ts,
            &grpc_span_octets,
        )))
        .await
        .expect("OTLP gRPC traces")
        .into_inner();
    assert!(traces.partial_success.is_none(), "{traces:?}");

    let response = post(
        &base,
        "/api/v1/write",
        RW_CONTENT_TYPE,
        remote_write_body(ts / 1_000_000),
    )
    .await;
    assert_eq!(response.status(), 204, "Remote Write");

    #[cfg(feature = "otap")]
    {
        use ravel_otap::proto::experimental::arrow::v1::StatusCode;
        let status = send_otap(&grpc, ts).await;
        assert_eq!(
            status.status_code,
            StatusCode::Ok as i32,
            "OTAP: {status:?}"
        );
        assert!(
            !status.status_message.starts_with("partial-success"),
            "OTAP: {status:?}"
        );
    }
}

/// Folds every signal of `acme` past the seal window of every hour up to
/// now, with an injected clock, so the bucket holds catalog snapshot parts,
/// HEADs and index objects.
async fn fold_everything(store: &Arc<dyn ObjectStoreBackend>) {
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let tenant_hash = TenantId::new(TENANT).hash();
    let hour = now_ns() / NS_PER_HOUR;
    let sealed_now = (hour + 1) * NS_PER_HOUR + SEAL_MARGIN_NS;
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans] {
        let report = catalog
            .fold(&tenant_hash, signal, Uuid::new_v4(), sealed_now, &[], None)
            .await
            .expect("fold");
        assert!(
            report.entry_count > 0,
            "{signal:?} folded nothing: {report:?}"
        );
    }
}

/// Submits one logs erasure request on `client.ip` = `value`, as
/// `ravel-cli erase submit` writes it.
async fn submit_erasure(store: &Arc<dyn ObjectStoreBackend>, value: &str) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let request_id = Uuid::new_v4();
    let request = ravel_proto::commit::v1::ErasureRequest {
        format_version: ravel_commit::erasure::FORMAT_VERSION,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Logs) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: now_ns(),
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "client.ip".to_string(),
            value: value.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: "dsar".to_string(),
    };
    ravel_commit::erasure::validate_request(&request).expect("valid request");
    let key = ravel_commit::keys::erasure_request_key(&tenant_hash, Signal::Logs, request_id)
        .expect("dreq key");
    store
        .put(
            &key,
            ravel_commit::erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("submit .dreq");
}

/// One stored object: its key, its bytes, and every zstd frame inside it,
/// decompressed recursively.
struct Stored {
    key: String,
    blobs: Vec<Vec<u8>>,
}

impl Stored {
    fn contains(&self, needle: &[u8]) -> bool {
        self.blobs.iter().any(|blob| find(blob, needle).is_some())
    }

    fn is_erasure_request(&self) -> bool {
        self.key.contains("/del/")
    }

    /// The key class this object belongs to, from the key layout in
    /// docs/catalog-and-mvcc.md.
    fn class(&self) -> &'static str {
        let key = self.key.as_str();
        let segments: Vec<&str> = key.split('/').collect();
        match segments.as_slice() {
            ["sys", "tenancy"] => "tenancy marker",
            ["sys", "t", _] => "recovery manifest",
            ["sys", "ipcrypt", "policy"] => "ipcrypt marker",
            ["t", _, "catalog", _, "HEAD"] => "catalog",
            ["t", _, "catalog", _, "snap", ..] => "catalog",
            ["t", _, "catalog", _, "idx", ..] if key.ends_with(".npost") => "index",
            ["t", _, "catalog", _, "idx", ..] if key.ends_with(".kidx") => "index",
            ["t", _, "catalog", _, "idx", ..] if key.ends_with(".cstat") => "stats",
            ["t", _, _, "del", ..] => "erasure",
            ["t", _, _, "l0", ..] => "data",
            ["t", _, _, "c", ..] => "commit",
            _ => "other",
        }
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

/// Object count per [`Stored::class`].
fn class_counts(stored: &[Stored]) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for object in stored {
        *counts.entry(object.class()).or_default() += 1;
    }
    counts
}

/// Asserts the bucket holds at least one object of every class the module
/// docs name, and returns the per-class counts.
fn assert_every_class(stored: &[Stored]) -> BTreeMap<&'static str, usize> {
    let counts = class_counts(stored);
    eprintln!("object classes scanned: {counts:?}");
    let other: Vec<&str> = stored
        .iter()
        .filter(|object| object.class() == "other")
        .map(|object| object.key.as_str())
        .collect();
    eprintln!("objects of no named class: {other:?}");
    for class in [
        "data",
        "commit",
        "catalog",
        "index",
        "stats",
        "erasure",
        "tenancy marker",
        "recovery manifest",
    ] {
        assert!(
            counts.get(class).copied().unwrap_or(0) > 0,
            "no `{class}` object in the bucket: {counts:?}"
        );
    }
    counts
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
/// v4-mapped binary, and the lowercase hex of each of those three. The hex of
/// the text is how a span event's or link's attributes are stored, inside the
/// lowercase hex of the event's or link's protobuf bytes.
fn address_forms(octet: u8) -> Vec<(&'static str, Vec<u8>)> {
    let text = addr(octet);
    let v4 = [198, 51, 100, octet];
    let mut mapped = [0u8; 16];
    mapped[10] = 0xff;
    mapped[11] = 0xff;
    mapped[12..].copy_from_slice(&v4);
    vec![
        ("text", text.as_bytes().to_vec()),
        ("ipv4 binary", v4.to_vec()),
        ("v4-mapped binary", mapped.to_vec()),
        ("text hex", hex(text.as_bytes()).into_bytes()),
        ("ipv4 hex", hex(&v4).into_bytes()),
        ("v4-mapped hex", hex(&mapped).into_bytes()),
    ]
}

/// The forms a stored ciphertext could take: its text and the hex of its
/// text.
fn ciphertext_forms(ciphertext: &str) -> [Vec<u8>; 2] {
    [
        ciphertext.as_bytes().to_vec(),
        hex(ciphertext.as_bytes()).into_bytes(),
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

/// Whether some object other than an erasure request carries any of
/// `forms`. The erasure request carries the value its predicate names, so it
/// proves nothing about what ingest stored.
fn ingest_stored_any(stored: &[Stored], forms: &[Vec<u8>]) -> bool {
    stored
        .iter()
        .filter(|object| !object.is_erasure_request())
        .any(|object| forms.iter().any(|form| object.contains(form)))
}

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

/// Every `(signal, outcome)` sample [`expected_counts`] names, as read.
async fn ipcrypt_samples(base: &str) -> Vec<(&'static str, &'static str, u64)> {
    let mut samples = Vec::new();
    for (signal, outcome, _) in expected_counts() {
        samples.push((signal, outcome, ipcrypt_sample(base, signal, outcome).await));
    }
    samples
}

/// Runs the whole pipeline on a fresh keyed bucket under `policy`: ingest
/// every case, read the IPCrypt counter, shut down, fold, submit one erasure
/// request on the HTTP log record's stored value, and scan the bucket.
async fn ingest_fold_and_scan(
    policy: Arc<IpcryptPolicy>,
    erased_value: &str,
) -> (Vec<(&'static str, &'static str, u64)>, Vec<Stored>) {
    let store = keyed_bucket().await;
    declare_typed_client_ip(store.as_ref()).await;
    let running = start_server(store.clone(), policy, LimitsConfig::default()).await;
    ingest_everything(&running).await;
    let base = format!("http://{}", running.http_addr);
    let counts = ipcrypt_samples(&base).await;
    running.shutdown().await.expect("graceful shutdown");
    fold_everything(&store).await;
    submit_erasure(&store, erased_value).await;
    (counts, stored_objects(&store).await)
}

#[tokio::test]
async fn no_stored_byte_carries_an_address_under_a_rule() {
    let _ = keyed_bucket().await;
    let policy = policy();
    let erased = ciphertext(&policy, "client.ip", &addr(octet::HTTP_LOG_RECORD));
    let (counts, stored) = ingest_fold_and_scan(policy.clone(), &erased).await;

    assert_every_class(&stored);
    eprintln!("ravel_ingest_ipcrypt_values_total: {counts:?}");

    for (case, octet) in stored_cases().into_iter().chain(dropped_cases()) {
        for (form, needle) in address_forms(octet) {
            let found = holding(&stored, &needle);
            assert!(
                found.is_empty(),
                "{case}: the address's {form} form is stored, of {} objects, in {found:?}",
                stored.len()
            );
        }
    }
    for (case, octet) in stored_cases() {
        let expected = ciphertext(&policy, case_key(octet), &addr(octet));
        assert!(
            ingest_stored_any(&stored, &ciphertext_forms(&expected)),
            "{case}: no object carries the ciphertext {expected}"
        );
    }
    for (case, octet) in dropped_cases() {
        let dropped = ciphertext(&policy, "client.ip", &addr(octet));
        let found: Vec<&str> = ciphertext_forms(&dropped)
            .iter()
            .flat_map(|form| holding(&stored, form))
            .collect();
        assert!(
            found.is_empty(),
            "{case}: the exemplar is dropped, yet its ciphertext is stored in {found:?}"
        );
    }
    let expected: Vec<_> = expected_counts().to_vec();
    assert_eq!(counts, expected, "IPCrypt values counted, per signal");
}

/// The control: with no policy the same pipeline stores each case's raw
/// address, and the scan sees each one.
#[tokio::test]
async fn the_scan_finds_every_address_with_no_policy() {
    let (counts, stored) =
        ingest_fold_and_scan(IpcryptPolicy::disabled(), &addr(octet::HTTP_LOG_RECORD)).await;

    assert_every_class(&stored);
    assert!(
        counts.iter().all(|(_, _, count)| *count == 0),
        "no policy counts nothing: {counts:?}"
    );
    for (case, octet) in stored_cases() {
        let forms: Vec<Vec<u8>> = address_forms(octet)
            .into_iter()
            .map(|(_, form)| form)
            .collect();
        assert!(
            ingest_stored_any(&stored, &forms),
            "{case}: no object carries the raw address {}, of {} objects",
            addr(octet),
            stored.len()
        );
    }
    let stats_hold_raw = stored.iter().any(|object| {
        object.class() == "stats" && object.contains(addr(octet::HTTP_LOG_RECORD).as_bytes())
    });
    assert!(
        stats_hold_raw,
        "the column statistics carry the typed column's raw values, so the scan can see them"
    );
    for (case, octet) in dropped_cases() {
        for (form, needle) in address_forms(octet) {
            let found = holding(&stored, &needle);
            assert!(
                found.is_empty(),
                "{case}: the exemplar is dropped, yet its {form} form is stored in {found:?}"
            );
        }
    }
}

/// A batch the admission layer refuses after normalization counts nothing:
/// with a zero active-series and active-stream cap, every point and record
/// normalizes, its ruled values are rendered, and the cap then drops it.
#[tokio::test]
async fn a_batch_refused_after_normalization_counts_nothing() {
    let store = keyed_bucket().await;
    let running = start_server(store, policy(), zero_cap_limits()).await;
    let base = format!("http://{}", running.http_addr);
    let ts = now_ns();

    let metrics = otlp_metrics(
        "capped_requests",
        ts,
        octet::HTTP_METRIC_POINT,
        octet::HTTP_METRIC_EXEMPLAR,
    );
    let response = post(&base, "/v1/metrics", PROTOBUF, metrics.encode_to_vec()).await;
    assert_eq!(response.status(), 200, "OTLP HTTP metrics");
    let body = response.bytes().await.expect("metrics response body");
    let reply =
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse::decode(
            body,
        )
        .expect("metrics response decodes");
    let rejected = reply
        .partial_success
        .expect("the cap reports a partial success")
        .rejected_data_points;
    assert_eq!(rejected, 1, "the one point is capped");

    let logs = otlp_logs(
        "capped-svc",
        ts,
        octet::HTTP_LOG_RESOURCE,
        octet::HTTP_LOG_RECORD,
    );
    let response = post(&base, "/v1/logs", PROTOBUF, logs.encode_to_vec()).await;
    assert_eq!(response.status(), 200, "OTLP HTTP logs");
    let body = response.bytes().await.expect("logs response body");
    let reply =
        opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse::decode(body)
            .expect("logs response decodes");
    let rejected = reply
        .partial_success
        .expect("the cap reports a partial success")
        .rejected_log_records;
    assert_eq!(rejected, 1, "the one record is capped");

    let response = post(
        &base,
        "/api/v1/write",
        RW_CONTENT_TYPE,
        remote_write_body(ts / 1_000_000),
    )
    .await;
    assert_eq!(response.status(), 204, "Remote Write");

    #[cfg(feature = "otap")]
    {
        let grpc = format!(
            "http://{}",
            running.grpc_addr.expect("Mode::All binds gRPC")
        );
        let status = send_otap(&grpc, ts).await;
        assert!(
            status.status_message.starts_with("partial-success: 1 "),
            "OTAP: {status:?}"
        );
    }

    let counts = ipcrypt_samples(&base).await;
    running.shutdown().await.expect("graceful shutdown");
    assert!(
        counts.iter().all(|(_, _, count)| *count == 0),
        "a refused batch counts nothing: {counts:?}"
    );
}
