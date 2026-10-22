//! ADR-3072's read-bytes amendment through the server's own wiring: the
//! size trigger's threshold a [`ravel_server::start`] server installs on its
//! SQL executor is `--cache-bulk-scan-min-bytes` raised to `--cache-max-bytes`.
//!
//! Two servers share one store holding one ingested log object. Both set the
//! flag to 1 byte; they differ only in the read cache's size. Under a cache
//! larger than anything the statement reads, the floor keeps every run
//! interactive. Under a cache smaller than what it reads, the same statement
//! reads as bulkScan from its second run, so the trigger itself is on.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::ReadCache;
use ravel_server::config::QueryBudgets;
use ravel_server::store::build_cache;
use ravel_server::{Cli, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};
use serde_json::Value;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const ROWS: usize = 4_000;
/// Far above anything the statement reads.
const LARGE_CACHE: u64 = 256 * 1024 * 1024;
/// Below what the statement reads; asserted, not assumed.
const SMALL_CACHE: u64 = 4_096;
const STATEMENT: &str = "SELECT count(*) AS n FROM logs WHERE severity_text = 'INFO'";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// The fetcher cache `main` builds for `--cache-max-bytes max_bytes`.
fn server_cache(max_bytes: u64) -> ReadCache {
    let max = max_bytes.to_string();
    let cli = Cli::try_parse_from(["ravel-server", "--cache-max-bytes", max.as_str()])
        .expect("cache flags parse");
    build_cache(&cli, max_bytes).expect("a nonzero cache is built")
}

async fn start(store: Arc<dyn ObjectStoreBackend>, cache_max_bytes: u64) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ravel_server::start(
        server_config(tokens, cache_max_bytes),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        Some(server_cache(cache_max_bytes)),
    )
    .await
    .expect("server starts")
}

/// One strict-mode OTLP export of [`ROWS`] records from `base_ts_ns` on.
async fn ingest(client: &reqwest::Client, base: &str, base_ts_ns: i64) {
    let log_records = (0..ROWS)
        .map(|i| {
            let ts = (base_ts_ns + i as i64) as u64;
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(format!(
                        "request {:016x}",
                        (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    ))),
                }),
                ..Default::default()
            }
        })
        .collect();
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("web".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    };
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export sent");
    assert_eq!(response.status(), 200, "export accepted");
}

/// Run [`STATEMENT`] over `base_ts_ns` plus or minus an hour and return its
/// `stats` object.
async fn run(client: &reqwest::Client, base: &str, base_ts_ns: i64) -> Value {
    let start = (base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
    let end = (base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(serde_json::json!({ "query": STATEMENT, "start": start, "end": end }).to_string())
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let value: Value = response.json().await.expect("sql response is JSON");
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["data"]["rows"][0][0], ROWS as u64, "{value}");
    value["stats"].clone()
}

fn intent(stats: &Value) -> String {
    stats["cache"]["intent"]
        .as_str()
        .unwrap_or_else(|| panic!("stats.cache.intent: {stats}"))
        .to_string()
}

/// The bytes the size trigger records for a run: cache served, late served
/// and store GET bytes, every phase.
fn read_bytes(stats: &Value) -> u64 {
    ["cacheBytes", "lateServedBytes", "s3GetBytes"]
        .iter()
        .map(|field| {
            stats["accounting"][field]
                .as_u64()
                .unwrap_or_else(|| panic!("stats.accounting.{field}: {stats}"))
        })
        .sum()
}

/// FLIP: passing `bulk_scan_min_bytes.flag` instead of
/// `bulk_scan_min_bytes.value` to `build_sql_state_with_parquet` in
/// `ravel_server::start` (`services/ravel-server/src/lib.rs`), or `None` as
/// the cache size `resolve_cache_bulk_scan_min_bytes` floors to, applies the
/// 1-byte flag as given and reads the large-cache server's second run as
/// bulkScan.
#[tokio::test]
async fn the_server_floors_the_bulk_scan_threshold_at_its_cache_size() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns();

    let large = start(Arc::clone(&store), LARGE_CACHE).await;
    let large_base = format!("http://{}", large.http_addr);
    ingest(&client, &large_base, base_ts_ns).await;
    let mut under_large = Vec::new();
    for _ in 0..3 {
        under_large.push(run(&client, &large_base, base_ts_ns).await);
    }
    large
        .shutdown()
        .await
        .expect("large-cache server shuts down");

    let small = start(store, SMALL_CACHE).await;
    let small_base = format!("http://{}", small.http_addr);
    let mut under_small = Vec::new();
    for _ in 0..3 {
        under_small.push(run(&client, &small_base, base_ts_ns).await);
    }
    small
        .shutdown()
        .await
        .expect("small-cache server shuts down");

    for (server, runs) in [("large", &under_large), ("small", &under_small)] {
        let first_read = read_bytes(&runs[0]);
        assert!(
            (SMALL_CACHE..LARGE_CACHE).contains(&first_read),
            "the {server}-cache server's first run read {first_read} bytes, \
             not between the two caches"
        );
    }
    assert_eq!(
        under_large.iter().map(intent).collect::<Vec<_>>(),
        ["interactive", "interactive", "interactive"],
        "a 1-byte flag under a {LARGE_CACHE}-byte cache: {under_large:?}"
    );
    assert_eq!(
        under_small.iter().map(intent).collect::<Vec<_>>(),
        ["interactive", "bulkScan", "bulkScan"],
        "a 1-byte flag under a {SMALL_CACHE}-byte cache: {under_small:?}"
    );
}

fn server_config(tokens: HashMap<String, TenantId>, cache_max_bytes: u64) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            sql_cache_bulk_scan_min_bytes: Some(1),
            ..QueryBudgets::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(50),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant_hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
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
        cache_max_bytes,
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
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}
