//! ADR-1746 decisions 4 and 5 reachability: a server built by
//! [`ravel_server::start`] checks a tenant's recorded format floor on its
//! metrics, log and span ingest paths, with the policy `start` resolves from
//! the compactor config.
//!
//! The tenant's provisioning records are written and their floors raised with
//! the catalog's own functions, one above the version each pipeline writes.
//! One server runs with the refusing default and one with the override, in
//! that order, inside one test: the counters on `/metrics` are process-wide,
//! so the second server's figures include the first's, and the test reads
//! each family's sample exactly once per scrape.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_ingest::FormatFamily;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

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

fn metrics_body() -> Vec<u8> {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "floor_e2e".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: now_ns() as u64,
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
    .encode_to_vec()
}

fn logs_body() -> Vec<u8> {
    let ts = now_ns() as u64;
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "api")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: ts,
                    observed_time_unix_nano: ts,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("line".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn traces_body() -> Vec<u8> {
    let start = now_ns() as u64;
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![1; 8],
                    name: "handle".to_string(),
                    kind: 2,
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 1_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// A store holding the tenant's metrics, logs and spans provisioning records,
/// each with its family's floor one above the version this build writes.
async fn floored_store() -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT).hash();
    for (signal, family) in [
        (Signal::Metrics, FormatFamily::Rseg),
        (Signal::Logs, FormatFamily::Rlog),
        (Signal::Spans, FormatFamily::Rspan),
    ] {
        ravel_catalog::validate_or_adopt(
            store.as_ref(),
            &tenant,
            signal,
            1,
            1,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        ravel_catalog::raise_format_floor(
            store.as_ref(),
            &tenant,
            signal,
            family.as_str(),
            family.writable_version() + 1,
            ravel_catalog::FloorBasis {
                observed_entries: 0,
                observed_newest_created_unix_ns: 0,
                observed_shards: 1,
            },
            "test",
            2,
        )
        .await
        .expect("raise floor");
    }
    store
}

/// A gateway server over `store`. `write_below_floor` is
/// `--write-below-format-floor`, which reaches `start` the way the CLI
/// resolves it: as the compactor config's format-floor policy.
async fn start_ingest_server(
    store: Arc<dyn ObjectStoreBackend>,
    write_below_floor: bool,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let mut maintain = ravel_server::MaintenanceTaskConfig::default();
    maintain.compactor.format_floor = Some(ravel_ingest::FormatFloorPolicy::new(
        write_below_floor,
        ravel_ingest::format_floor_metrics(),
    ));
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(200),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Gateway,
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
        maintain,
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 2, write: 2 },
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
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// POST one strict OTLP export to `path`, returning its status and body.
async fn post(client: &reqwest::Client, base: &str, path: &str, body: Vec<u8>) -> (u16, String) {
    let response = client
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export completes");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// The value of `metric{mode="gateway",family="<family>"}` on a scrape,
/// required to render exactly once.
fn floor_sample(body: &str, metric: &str, family: FormatFamily) -> u64 {
    let prefix = format!("{metric}{{");
    let labels = format!("family=\"{}\"", family.as_str());
    let samples: Vec<&str> = body
        .lines()
        .filter(|line| line.starts_with(prefix.as_str()) && line.contains(labels.as_str()))
        .filter_map(|line| line.rsplit_once(' ').map(|(_, value)| value))
        .collect();
    assert_eq!(
        samples.len(),
        1,
        "{metric} for {family} must render once:\n{body}"
    );
    samples[0].parse::<u64>().expect("sample value")
}

async fn scrape(client: &reqwest::Client, base: &str) -> String {
    client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// With the refusing default, a strict metrics, log and span write into the
/// floored tenant are each answered 503 naming the floor, and each is counted
/// once under `ravel_format_floor_refused_total{family}`. A log or span
/// refusal mapped as non-retryable answers 400 instead.
///
/// With `--write-below-format-floor` the same metrics and log writes land
/// (200) and count under `ravel_format_floor_writes_below_total{family}`.
/// Deleting `start`'s `.with_format_floor_policy(..)` on the metrics or log
/// router leaves that router on the refusing default, so the second server
/// answers that write 503 and the 200 assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floored_writes_are_refused_with_503_and_the_override_writes_them() {
    let client = reqwest::Client::new();

    let running = start_ingest_server(floored_store().await, false).await;
    let base = format!("http://{}", running.http_addr);
    for (path, body, family) in [
        ("/v1/metrics", metrics_body(), FormatFamily::Rseg),
        ("/v1/logs", logs_body(), FormatFamily::Rlog),
        ("/v1/traces", traces_body(), FormatFamily::Rspan),
    ] {
        let (status, text) = post(&client, &base, path, body).await;
        assert_eq!(status, 503, "{path}: {text}");
        assert!(
            text.contains(&format!("recorded {family} format floor is")),
            "{path}: {text}"
        );
    }
    let body = scrape(&client, &base).await;
    for family in FormatFamily::ALL {
        assert_eq!(
            floor_sample(&body, "ravel_format_floor_refused_total", family),
            1,
            "{family} refusals"
        );
        assert_eq!(
            floor_sample(&body, "ravel_format_floor_writes_below_total", family),
            0,
            "{family} overridden writes"
        );
    }
    running.shutdown().await.expect("server shuts down");

    let running = start_ingest_server(floored_store().await, true).await;
    let base = format!("http://{}", running.http_addr);
    for (path, body) in [("/v1/metrics", metrics_body()), ("/v1/logs", logs_body())] {
        let (status, text) = post(&client, &base, path, body).await;
        assert_eq!(status, 200, "{path} under the override: {text}");
    }
    let body = scrape(&client, &base).await;
    assert_eq!(
        floor_sample(
            &body,
            "ravel_format_floor_writes_below_total",
            FormatFamily::Rseg
        ),
        1
    );
    assert_eq!(
        floor_sample(
            &body,
            "ravel_format_floor_writes_below_total",
            FormatFamily::Rlog
        ),
        1
    );
    assert_eq!(
        floor_sample(
            &body,
            "ravel_format_floor_refused_total",
            FormatFamily::Rseg
        ),
        1,
        "the override refuses nothing more"
    );
    running.shutdown().await.expect("server shuts down");
}
