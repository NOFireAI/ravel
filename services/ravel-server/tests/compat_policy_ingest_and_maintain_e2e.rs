//! Issue #4301 reachability: a real [`ravel_server::start`] installs the
//! `sys/compat` write policy on both the ingest routers (#4244) and the
//! maintenance compactor (#4245), so an activation this build cannot honour
//! stops every writer in the shipping binary.
//!
//! On a keyed bucket whose `sys/compat` names an `rseg`/`rlog`/`rspan` active
//! version newer than this build writes, a gateway server refuses metrics, log
//! and span ingest at admission, and a maintain server refuses the compaction
//! of a sealed-hour bucket, each counted on `/metrics` under
//! `ravel_compat_publish_refused_total{family,reason="active_version_not_writable"}`.
//!
//! The maintenance counters are process-wide and every `/metrics` body sums
//! them in, so the two tests run one at a time under [`SERIAL`] and the
//! gateway test asserts the exact rise its own writes cause.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord as OtlpLogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_catalog::compat::{CompatKeys, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, create_compat};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const DEPLOYMENT_KEY: [u8; 32] = [0x4d; 32];
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Held by each test for its whole run: a maintenance refusal counted while
/// the gateway test runs would land in the gateway's `/metrics` body too.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
                    name: "compat_e2e".to_string(),
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
                log_records: vec![OtlpLogRecord {
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

/// `sys/compat` naming each family one version above what this build writes,
/// sealed under the deployment key so the server authenticates it.
async fn write_unwritable_compat(store: &dyn ObjectStoreBackend) {
    let rseg = u32::from(ravel_segment::SegmentVersion::CURRENT.number()) + 1;
    let rlog = u32::from(ravel_logseg::footer::SUPPORTED_VERSIONS.newest()) + 1;
    let rspan = u32::from(ravel_rspan::footer::SUPPORTED_VERSIONS.newest()) + 1;
    let family = |name: &str, version: u32| sysproto::CompatFamily {
        family: name.to_string(),
        active_write_version: version,
        activated_epoch: 1,
        ..Default::default()
    };
    create_compat(
        store,
        &CompatKeys::from_file_keys(&[DEPLOYMENT_KEY]).expect("key"),
        sysproto::CompatRecord {
            compat_epoch: 1,
            families: vec![
                family(FAMILY_RSEG, rseg),
                family(FAMILY_RLOG, rlog),
                family(FAMILY_RSPAN, rspan),
            ],
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
}

fn base_config(store_mode: Mode, maintain_enabled: bool) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        emit_declared_stat_aggregates: true,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(200),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: store_mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![TenantId::new(TENANT).hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: maintain_enabled,
            interval: Duration::from_secs(1),
            shard_count: 1,
            // Force compaction of the tiny seeded bucket: the production
            // geometry floor would leave it uncompacted and never reach the
            // compat gate.
            compactor: ravel_maintain::CompactorConfig {
                geometry_floor_decoded_rows: 0,
                ..ravel_maintain::CompactorConfig::default()
            },
            worker_record_keys: Some(
                ravel_maintain::worker_set::MaintainRecordKeys::from_file_keys(&[[0x5C; 32]])
                    .expect("one maintain key"),
            ),
            ..ravel_server::MaintenanceTaskConfig::default()
        },
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: LimitsConfig::default(),
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: 2,
            write: 2,
            ..Default::default()
        },
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
    }
}

async fn start(store: Arc<dyn ObjectStoreBackend>, config: ServerConfig) -> ravel_server::Running {
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

async fn post(client: &reqwest::Client, base: &str, path: &str, body: Vec<u8>) -> u16 {
    client
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export completes")
        .status()
        .as_u16()
}

async fn scrape(addr: std::net::SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The value of `ravel_compat_publish_refused_total{mode,family,reason}`, or 0
/// if the series is absent.
fn refused(body: &str, mode: &str, family: &str) -> u64 {
    let needle = format!(
        "ravel_compat_publish_refused_total{{mode=\"{mode}\",family=\"{family}\",reason=\"active_version_not_writable\"}} "
    );
    body.lines()
        .find_map(|line| line.strip_prefix(needle.as_str()))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// The ingest path installs the policy (#4244): a metrics, log and span write
/// into a keyed bucket whose `sys/compat` names an unwritable version for each
/// family is refused 503 at admission, and each is counted on `/metrics` under
/// its family, exactly once. A server that passed `None` to
/// `with_compat_policy` would accept all three (200) and leave every counter
/// where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_is_refused_at_admission_and_counted_on_metrics() {
    let _serial = SERIAL.lock().await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    write_unwritable_compat(store.as_ref()).await;
    let running = start(Arc::clone(&store), base_config(Mode::Gateway, false)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let before = scrape(running.http_addr).await;

    for (path, body) in [
        ("/v1/metrics", metrics_body()),
        ("/v1/logs", logs_body()),
        ("/v1/traces", traces_body()),
    ] {
        assert_eq!(
            post(&client, &base, path, body).await,
            503,
            "{path} is refused by the compat policy"
        );
    }

    let body = scrape(running.http_addr).await;
    for family in [FAMILY_RSEG, FAMILY_RLOG, FAMILY_RSPAN] {
        assert_eq!(
            refused(&body, "gateway", family),
            refused(&before, "gateway", family) + 1,
            "one ingest refusal counted for {family}:\n{body}"
        );
    }
    running.shutdown().await.expect("graceful shutdown");
}

/// Input `input`'s two tiny RLOG records in `hour`, enough for the default
/// `min_compaction_inputs` of 2 to form a compaction unit.
fn input_records(hour: u32, input: u32) -> Vec<LogRecord> {
    let base = i64::from(hour) * NS_PER_HOUR + 1_000_000_000;
    (0..2u32)
        .map(|j| {
            let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
            let ts = base + i64::from(j) * 1_000 + i64::from(input);
            LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1", &[]),
                stream_attrs: ravel_logseg::stream_attrs_bytes(&resource, "scope", "1", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("in{input} r{j}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("a".to_string(), AttrValue::I64(i64::from(j)))],
            }
        })
        .collect()
}

/// Publishes three small RLOG inputs into `hour`, each its own writer.
async fn publish_inputs(store: &MemoryStore, tenant: &TenantId, hour: u32) {
    let tenant_hash = tenant.hash();
    for input in 0..3u32 {
        let records = input_records(hour, input);
        let writer_id = Uuid::from_u128(u128::from(input) + 1);
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        for rec in &records {
            writer.push(rec.clone()).expect("push log record");
        }
        let bytes = writer.finish().expect("finish rlog object");
        let created_unix_ns = i64::from(hour) * NS_PER_HOUR + 2_000_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records.len() as u64,
            series_count: 1,
            min_event_ts_ns: records.first().expect("records").ts_ns,
            max_event_ts_ns: records.last().expect("records").ts_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns,
            ingest_hour_bucket: hour,
        })
        .expect("valid log commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put log data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish log commit");
    }
}

/// A sealed ingest hour, five hours back on the wall clock the server reads.
fn sealed_hour() -> u32 {
    (now_ns() / NS_PER_HOUR) as u32 - 5
}

/// The maintenance compactor installs the policy (#4245): a `Mode::Maintain`
/// server over a sealed-hour RLOG bucket whose `sys/compat` names an unwritable
/// `rlog` version refuses the compaction run before it writes anything, counted
/// on `/metrics` under `family="rlog"`. A compactor config left with
/// `compat_policy: None` would compact the bucket and leave the counter at 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_maintenance_run_is_refused_and_counted_on_metrics() {
    let _serial = SERIAL.lock().await;
    let store = Arc::new(MemoryStore::new());
    write_unwritable_compat(store.as_ref()).await;
    let tenant = TenantId::new(TENANT);
    let hour = sealed_hour();
    publish_inputs(&store, &tenant, hour).await;

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let running = start(backend, base_config(Mode::Maintain, true)).await;
    let mut body = String::new();
    let mut refused_rlog = false;
    for _ in 0..1_200 {
        body = scrape(running.http_addr).await;
        if refused(&body, "maintain", FAMILY_RLOG) >= 1 {
            refused_rlog = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    running.shutdown().await.expect("graceful shutdown");
    assert!(
        refused_rlog,
        "the maintenance compaction was refused and counted under rlog:\n{body}"
    );
}
