//! ADR-4233 decision 1 (the v0.34.0 release bar, #4301): on a keyed store that
//! never ran compat init -- `sys/compat` absent -- a real [`ravel_server::start`]
//! with the ingest and maintenance write policies installed must write nothing
//! a v0.33.0 binary refuses. Here the policy is installed (the bucket is keyed,
//! so [`ravel_server::start`] builds a [`CompatView`] and hands it to every
//! writer), yet with no `sys/compat` the view answers each family's baseline,
//! which is exactly the `CURRENT` version a build with no policy (`policy None`,
//! the parent commit) writes.
//!
//! A metrics, log and span write lands (200), and every commit record's
//! `segment_format_version` is its family's `CURRENT`. Two wrong policies this
//! rules out: one that refuses on an uninitialised store writes no object (the
//! per-family commit-record assertion finds none); one that writes a different
//! version fails the version equality.

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
use ravel_commit::{keys, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const DEPLOYMENT_KEY: [u8; 32] = [0x4d; 32];

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

fn config() -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        audit_pipeline: Default::default(),
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
        mode: Mode::Gateway,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
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

/// Every commit record's `segment_format_version` under `tenant`'s `signal`
/// shard-0 commit prefix.
async fn committed_versions(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> Vec<u32> {
    let prefix = keys::commit_shard_prefix(&tenant.hash(), signal, 0).expect("commit prefix");
    let mut versions = Vec::new();
    for meta in list_all(store, &prefix).await.expect("list commits") {
        if !matches!(
            keys::partition_bucket_entry(&meta.key),
            Ok(keys::BucketEntry::CommitRecord(_))
        ) {
            continue;
        }
        let bytes = store
            .get(&meta.key, ravel_object_store::GetRange::Full)
            .await
            .expect("commit record")
            .data;
        versions.push(
            record::decode(&bytes)
                .expect("decode commit record")
                .segment_format_version,
        );
    }
    versions
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_policy_installed_on_an_uninitialised_store_writes_the_baseline() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // No create_compat: sys/compat is absent, as on a store that never ran
    // compat init. The deployment key still installs the policy.
    let running = ravel_server::start(
        config(),
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
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

    let tenant = TenantId::new(TENANT);
    let expected = [
        (
            Signal::Metrics,
            u32::from(ravel_ingest::SEGMENT_FORMAT_VERSION),
        ),
        (
            Signal::Logs,
            u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        ),
        (
            Signal::Spans,
            u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        ),
    ];

    // The flush fires on max_flush_delay; poll until every family has committed.
    let mut committed: Vec<Vec<u32>> = Vec::new();
    for _ in 0..600 {
        committed = Vec::new();
        for (signal, _) in expected {
            committed.push(committed_versions(store.as_ref(), &tenant, signal).await);
        }
        if committed.iter().all(|v| !v.is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    running.shutdown().await.expect("graceful shutdown");

    for ((signal, current), versions) in expected.into_iter().zip(committed) {
        assert!(
            !versions.is_empty(),
            "{signal:?} committed at least one object"
        );
        assert!(
            versions.iter().all(|v| *v == current),
            "{signal:?} commit records are all at CURRENT {current}, the policy-None version: {versions:?}"
        );
    }
}
