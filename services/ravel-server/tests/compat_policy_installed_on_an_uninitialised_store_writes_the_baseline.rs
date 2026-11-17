//! ADR-4233 decision 1 (the v0.34.0 release bar, #4301): on a keyed store that
//! never ran compat init (`sys/compat` absent), a real [`ravel_server::start`]
//! writes nothing a v0.33.0 binary refuses, with the write policy installed on
//! every writer it runs.
//!
//! The bucket is keyed, so `start` builds a [`ravel_catalog::compat::CompatView`]
//! and hands it to the ingest routers, the query audit pipeline, the catalog
//! fold and the maintenance compactor. `/metrics` renders the
//! `ravel_compat_publish_refused_total` family, which only a process that
//! installed the policy renders, and no series in it moves. With no
//! `sys/compat` the view answers each family's baseline, the `CURRENT` version
//! a build with no policy writes.
//!
//! [`compat_policy_installed_on_an_uninitialised_store_writes_the_baseline`]
//! runs a `Mode::All` server (metrics, log and span ingest, a query whose
//! audit record is written in `required` mode, and the scheduled fold of a
//! sealed log hour), then a `Mode::Maintain` server that compacts that hour.
//! [`a_5xx_at_startup_refuses_until_a_refresh_then_writes_the_baseline`]
//! starts a server whose first `sys/compat` read fails with a 503: ingest is
//! refused at admission, so nothing is acknowledged that could be lost, until
//! a successful read, after which writes land at the baseline.
//!
//! Each test then checks the store as a v0.33.0 reader would meet it: every
//! data object's trailer version and every commit record's
//! `segment_format_version` is its family's `CURRENT`, no `sys/compat` object
//! exists, and every provisioning record is below format version 4 with no
//! compat field set.
//!
//! `ravel_compat_publish_refused_total` sums process-wide counters, so the two
//! tests run one at a time under [`SERIAL`] and compare scrapes, not absolute
//! values.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
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
use ravel_catalog::PROVISIONING_FENCED_FORMAT_VERSION;
use ravel_catalog::compat::COMPAT_KEY;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const DEPLOYMENT_KEY: [u8; 32] = [0x4d; 32];
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const REFUSED: &str = "ravel_compat_publish_refused_total";

/// Held by each test for its whole run, so neither sees the other's
/// process-wide refusal counts move.
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
                    name: "baseline_e2e".to_string(),
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

fn config(mode: Mode) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        // `required`: a refused audit write fails the query, so a 200 proves
        // the audit record was written.
        audit_pipeline: matches!(mode, Mode::All).then(|| ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(100),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 0,
        idle_flush_byte_floor: 0,
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![TenantId::new(TENANT).hash()],
        fold: FoldTaskConfig {
            enabled: !matches!(mode, Mode::Gateway),
            fold_interval: Duration::from_millis(250),
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: matches!(mode, Mode::Maintain),
            interval: Duration::from_secs(1),
            shard_count: 1,
            // Compact the tiny seeded bucket: the production geometry floor
            // would leave it uncompacted.
            compactor: ravel_maintain::CompactorConfig {
                geometry_floor_decoded_rows: 0,
                ..ravel_maintain::CompactorConfig::default()
            },
            worker_record_keys: matches!(mode, Mode::Maintain).then(|| {
                ravel_maintain::worker_set::MaintainRecordKeys::from_file_keys(&[[0x5C; 32]])
                    .expect("one maintain key")
            }),
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

async fn start(store: Arc<dyn ObjectStoreBackend>, mode: Mode) -> ravel_server::Running {
    ravel_server::start(
        config(mode),
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

/// Every `ravel_compat_publish_refused_total` sample in `body`, by its label
/// set. Empty when the family is not rendered.
fn refusals(body: &str) -> BTreeMap<String, u64> {
    body.lines()
        .filter_map(|line| line.strip_prefix(REFUSED))
        .filter(|rest| rest.starts_with('{'))
        .map(|rest| {
            let (labels, value) = rest.rsplit_once(' ').expect("a sample ends in its value");
            (labels.to_string(), value.parse().expect("a counter value"))
        })
        .collect()
}

/// The policy is installed (the family renders, with the pre-filled
/// `view_stale` series of `family` under `mode`), and no refusal series moved
/// since `before`.
fn assert_installed_and_nothing_refused(before: &str, after: &str, mode: &str, family: &str) {
    let series = format!("{{mode=\"{mode}\",family=\"{family}\",reason=\"view_stale\"}}");
    let after = refusals(after);
    assert!(
        after.contains_key(&series),
        "the compat write policy is installed: {REFUSED}{series} renders:\n{after:?}"
    );
    let before = refusals(before);
    for (labels, count) in &after {
        assert_eq!(
            *count,
            before.get(labels).copied().unwrap_or(0),
            "no write was refused under the compat policy: {REFUSED}{labels}"
        );
    }
}

/// The trailer format version each signal's writers emit with no policy.
fn current_version(signal: Signal) -> u32 {
    match signal {
        Signal::Metrics => u32::from(ravel_ingest::SEGMENT_FORMAT_VERSION),
        Signal::Logs | Signal::Audit => u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        Signal::Spans => u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        other => panic!("no data writer in this test for {other:?}"),
    }
}

/// What the store holds, read as a v0.33.0 binary meets it.
#[derive(Debug, Default)]
struct Written {
    /// Trailer version of every L0 data object, by signal.
    l0: BTreeMap<String, Vec<u32>>,
    /// Trailer version of every L1 part, by signal.
    l1: BTreeMap<String, Vec<u32>>,
    /// `segment_format_version` of every commit record, by signal.
    commits: BTreeMap<String, Vec<u32>>,
    compaction_records: usize,
    /// `(key, format_version, compat_epoch, compat_fenced_unix_ns)`.
    provisioning: Vec<(String, u32, u64, i64)>,
    catalog_heads: Vec<String>,
    compat_object: bool,
}

/// The format version in an RSEG, RLOG or RSPAN trailer: all three end in a
/// 16-byte trailer whose bytes 8..10 are the version, little-endian
/// (docs/segment-format.md, docs/log-segment-format.md, the RSPAN footer).
fn trailer_version(bytes: &[u8]) -> u32 {
    let trailer = &bytes[bytes.len() - 16..];
    u32::from(u16::from_le_bytes([trailer[8], trailer[9]]))
}

async fn written(store: &dyn ObjectStoreBackend) -> Written {
    let get = |key: String| async move {
        store
            .get(&key, GetRange::Full)
            .await
            .expect("listed object reads")
            .data
    };
    let mut out = Written {
        compat_object: store.head(COMPAT_KEY).await.is_ok(),
        ..Written::default()
    };
    for meta in list_all(store, "").await.expect("list the bucket") {
        let key = meta.key;
        if let Ok(parsed) = keys::parse_data_key(&key) {
            let version = trailer_version(&get(key).await);
            out.l0
                .entry(format!("{:?}", parsed.signal))
                .or_default()
                .push(version);
        } else if let Ok(parsed) = keys::parse_l1_part_key(&key) {
            let version = trailer_version(&get(key).await);
            out.l1
                .entry(format!("{:?}", parsed.signal))
                .or_default()
                .push(version);
        } else if let Ok(entry) = keys::partition_bucket_entry(&key) {
            match entry {
                keys::BucketEntry::CommitRecord(parsed) => {
                    let commit = record::decode(&get(key).await).expect("decode commit record");
                    out.commits
                        .entry(format!("{:?}", parsed.signal))
                        .or_default()
                        .push(commit.segment_format_version);
                }
                keys::BucketEntry::CompactionRecord(_) => out.compaction_records += 1,
                _ => {}
            }
        } else if key.ends_with("/prov") {
            let prov = sysproto::ProvisioningRecord::decode(get(key.clone()).await.as_ref())
                .expect("decode provisioning record");
            out.provisioning.push((
                key,
                prov.format_version,
                prov.compat_epoch,
                prov.compat_fenced_unix_ns,
            ));
        } else if key.contains("/catalog/") && key.ends_with("/HEAD") {
            out.catalog_heads.push(key);
        }
    }
    out
}

/// Every object a v0.33.0 binary reads is at its family's `CURRENT`, and no
/// object the activation protocol writes exists.
fn assert_v0_33_readable(written: &Written) {
    for (what, by_signal) in [
        ("L0 data object trailer", &written.l0),
        ("L1 part trailer", &written.l1),
        ("commit record segment_format_version", &written.commits),
    ] {
        for (signal, versions) in by_signal {
            let current = [Signal::Metrics, Signal::Logs, Signal::Spans, Signal::Audit]
                .into_iter()
                .find(|s| format!("{s:?}") == *signal)
                .map(current_version)
                .unwrap_or_else(|| panic!("unexpected signal {signal}"));
            assert!(
                versions.iter().all(|v| *v == current),
                "every {signal} {what} is at CURRENT {current}: {versions:?}"
            );
        }
    }
    assert!(
        !written.compat_object,
        "no {COMPAT_KEY} object is written on an uninitialised store"
    );
    assert!(
        !written.provisioning.is_empty(),
        "ingest wrote provisioning records: {written:?}"
    );
    for (key, format_version, compat_epoch, fenced) in &written.provisioning {
        assert!(
            *format_version < PROVISIONING_FENCED_FORMAT_VERSION,
            "{key} is below the fenced format version 4: {format_version}"
        );
        assert_eq!(
            (*compat_epoch, *fenced),
            (0, 0),
            "{key} carries no compat field"
        );
    }
}

/// Polls `written` until `done` holds, for up to 60 s.
async fn wait_for(store: &dyn ObjectStoreBackend, done: impl Fn(&Written) -> bool) -> Written {
    let mut seen = Written::default();
    for _ in 0..600 {
        seen = written(store).await;
        if done(&seen) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out; the store holds {seen:?}");
}

fn has(by_signal: &BTreeMap<String, Vec<u32>>, signal: Signal) -> bool {
    by_signal
        .get(&format!("{signal:?}"))
        .is_some_and(|v| !v.is_empty())
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

/// Publishes three small RLOG inputs into `hour`, each its own writer, at the
/// build's current version.
async fn publish_inputs(store: &dyn ObjectStoreBackend, tenant: &TenantId, hour: u32) {
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

/// Every writer `start` runs, with the policy installed on a store with no
/// `sys/compat`, writes only what v0.33.0 reads: ingest of all three signals
/// and a `required` query audit write in `Mode::All` (each answered 200), the
/// scheduled fold of a sealed log hour there, and the compaction of that hour
/// in `Mode::Maintain`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_policy_installed_on_an_uninitialised_store_writes_the_baseline() {
    let _serial = SERIAL.lock().await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    publish_inputs(store.as_ref(), &tenant, sealed_hour()).await;

    let all = start(store.clone(), Mode::All).await;
    let before = scrape(all.http_addr).await;
    let base = format!("http://{}", all.http_addr);
    let client = reqwest::Client::new();
    for (path, body) in [
        ("/v1/metrics", metrics_body()),
        ("/v1/logs", logs_body()),
        ("/v1/traces", traces_body()),
    ] {
        assert_eq!(
            post(&client, &base, path, body).await,
            200,
            "{path} lands on an uninitialised store"
        );
    }
    let now_s = now_ns() / 1_000_000_000;
    let query = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "baseline_e2e".to_string()),
            ("start", (now_s - 600).to_string()),
            ("end", now_s.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query_range request sent");
    assert_eq!(
        query.status(),
        200,
        "the query succeeds, so its required audit write was not refused"
    );
    let logs_head = format!("t/{}/catalog/l/HEAD", tenant.hash().to_hex());
    wait_for(store.as_ref(), |w| {
        has(&w.commits, Signal::Metrics)
            && has(&w.commits, Signal::Spans)
            && has(&w.commits, Signal::Audit)
            && w.commits.get("Logs").is_some_and(|v| v.len() > 3)
            && w.catalog_heads.contains(&logs_head)
    })
    .await;
    assert_installed_and_nothing_refused(&before, &scrape(all.http_addr).await, "all", "rseg");
    all.shutdown().await.expect("graceful shutdown");

    let maintain = start(store.clone(), Mode::Maintain).await;
    let before = scrape(maintain.http_addr).await;
    let compacted = wait_for(store.as_ref(), |w| {
        w.compaction_records >= 1 && has(&w.l1, Signal::Logs)
    })
    .await;
    assert_installed_and_nothing_refused(
        &before,
        &scrape(maintain.http_addr).await,
        "maintain",
        "rlog",
    );
    maintain.shutdown().await.expect("graceful shutdown");

    let written = written(store.as_ref()).await;
    for signal in [Signal::Metrics, Signal::Logs, Signal::Spans, Signal::Audit] {
        assert!(has(&written.l0, signal), "{signal:?} wrote L0: {written:?}");
    }
    assert!(has(&compacted.l1, Signal::Logs));
    assert_v0_33_readable(&written);
}

/// A 503 on the first `sys/compat` read leaves the view unread and not blind,
/// so the server cannot tell an uninitialised store from an activated one.
/// The metrics tenant was provisioned by an earlier v0.33.0 writer, so its
/// write passes first touch and is refused by the ingest router's admission;
/// the log tenant is new and is refused at first touch. Both answer 503,
/// count `view_stale` under their family and write nothing. The refusal
/// wakes the refresh loop, which reads `sys/compat` again at once instead of
/// on its 60 s tick; that read is held here, so the view stays unread for
/// both requests. Once it is let through it finds no record, the view is
/// fresh with no refresh by the test, and the same writes land at the
/// baseline (#4362).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_5xx_at_startup_refuses_until_a_refresh_then_writes_the_baseline() {
    let _serial = SERIAL.lock().await;
    let plan = FaultPlan::empty().with_sequence(
        Sequence::new(Op::Get)
            .with_key_contains(COMPAT_KEY)
            .then_fault(ScriptedFault::Transient(
                "503 Service Unavailable".to_string(),
            )),
    );
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = faults.clone();
    let tenant = TenantId::new(TENANT).hash();
    let now_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos(),
    )
    .expect("fits");
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant,
        Signal::Metrics,
        1,
        now_ns,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("a v0.33.0 writer provisioned the metrics tenant");
    let provisioned = vec![ravel_catalog::provisioning_key(&tenant, Signal::Metrics)];

    let running = start(store.clone(), Mode::Gateway).await;
    assert_eq!(
        faults.fault_count(Op::Get, FaultKind::Transient),
        1,
        "the startup read of sys/compat failed with a 5xx"
    );
    let woken_read = faults.hold(Op::Get, Some(COMPAT_KEY.to_string()), Occurrence::Nth(1));
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    for (path, body, family) in [
        ("/v1/metrics", metrics_body(), "rseg"),
        ("/v1/logs", logs_body(), "rlog"),
    ] {
        let stale = format!("{{mode=\"gateway\",family=\"{family}\",reason=\"view_stale\"}}");
        let before = refusals(&scrape(running.http_addr).await);
        assert_eq!(
            post(&client, &base, path, body).await,
            503,
            "{path}: a view that never read sys/compat refuses ingest"
        );
        let after = refusals(&scrape(running.http_addr).await);
        assert_eq!(
            after.get(&stale).copied().unwrap_or(0),
            before.get(&stale).copied().unwrap_or(0) + 1,
            "{path}: the refusal is counted view_stale: {after:?}"
        );
    }
    assert_eq!(
        list_all(store.as_ref(), "t/")
            .await
            .expect("list tenants")
            .into_iter()
            .map(|meta| meta.key)
            .collect::<Vec<_>>(),
        provisioned,
        "nothing is written while the view is unread"
    );

    tokio::time::timeout(Duration::from_secs(5), woken_read.wait_until_held(1))
        .await
        .expect("the refusal woke a sys/compat read well before the 60 s tick");
    for id in woken_read.held() {
        assert!(woken_read.release(id));
    }
    let view = running.compat_view().expect("a keyed bucket builds a view");
    tokio::time::timeout(Duration::from_secs(5), async {
        while view.last_read_unix_ns().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the woken read succeeds and the view is fresh with no test refresh");
    let before = scrape(running.http_addr).await;
    for (path, body) in [
        ("/v1/metrics", metrics_body()),
        ("/v1/logs", logs_body()),
        ("/v1/traces", traces_body()),
    ] {
        assert_eq!(
            post(&client, &base, path, body).await,
            200,
            "{path} lands once sys/compat reads absent"
        );
    }
    wait_for(store.as_ref(), |w| {
        [Signal::Metrics, Signal::Logs, Signal::Spans]
            .into_iter()
            .all(|s| has(&w.commits, s))
    })
    .await;
    assert_installed_and_nothing_refused(
        &before,
        &scrape(running.http_addr).await,
        "gateway",
        "rseg",
    );
    running.shutdown().await.expect("graceful shutdown");
    assert_v0_33_readable(&written(store.as_ref()).await);
}
