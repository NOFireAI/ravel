//! The decoded cache gives way to a statement through the process budget's
//! pressure evictor (ADR-3326 decision 4), through the server's own wiring.
//!
//! A wide statement fills the cache with a log object's decoded columns, then
//! the test takes every byte of the process budget still free, so the budget
//! sits at its limit with the cache holding part of it. A narrow statement
//! over the same object then needs reservations the full budget would refuse:
//! it succeeds only because the evictor frees the cache's unheld entries, and
//! its `stats.decodedCache.pressureEvictions` says so.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::QueryBudgets;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};
use serde_json::Value;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const ROWS: usize = 1_000;
const BODY_BYTES: usize = 1_536;
/// 64 MiB above the decoded cache's 1 GiB admission margin, so the wide
/// statement's columns can be admitted.
const PROCESS_BUDGET: u64 = (1 << 30) + 64 * 1024 * 1024;
const DECODED_CACHE: u64 = 64 * 1024 * 1024;
const WIDE: &str = "SELECT count(*) AS n, sum(length(body)) AS b FROM logs \
                    WHERE severity_text = 'INFO'";
const NARROW: &str = "SELECT count(*) AS n FROM logs WHERE severity_text = 'INFO'";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// A body of hex digits from a per-row xorshift, so the decoded column is
/// several times the stored object.
fn body(row: usize) -> String {
    let mut x = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..BODY_BYTES)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from_digit((x & 0xF) as u32, 16).expect("a hex digit")
        })
        .collect()
}

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
                    value: Some(AnyValueVariant::StringValue(body(i))),
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

/// Runs `statement` and returns its `stats.decodedCache` object.
async fn run(client: &reqwest::Client, base: &str, base_ts_ns: i64, statement: &str) -> Value {
    let start = (base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
    let end = (base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(serde_json::json!({ "query": statement, "start": start, "end": end }).to_string())
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let body = response.text().await.expect("sql response body");
    assert_eq!(status, 200, "{statement}: {body}");
    let value: Value = serde_json::from_str(&body).expect("sql response is JSON");
    assert_eq!(value["data"]["rows"][0][0], ROWS as u64, "{value}");
    assert_eq!(body.matches("\"decodedCache\"").count(), 1, "{body}");
    value["stats"]["decodedCache"].clone()
}

fn figure(decoded: &Value, field: &str) -> u64 {
    decoded[field]
        .as_u64()
        .unwrap_or_else(|| panic!("decodedCache.{field}: {decoded}"))
}

/// FLIP: make the server's registered evictor return 0 without evicting
/// (`services/ravel-server/src/lib.rs`, the `register_evictor` call) and the
/// narrow statement's object fetch is refused ("fetch memory exhausted"), a
/// 422 where `run` asserts 200.
#[tokio::test]
async fn a_full_budget_admits_a_statement_by_evicting_unheld_cache_entries() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns();
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let running = ravel_server::start(
        server_config(tokens),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let budget = running.process_memory_budget();
    ingest(&client, &base, base_ts_ns).await;

    // Twice, so a statement read as a bulk scan admits on its second touch.
    let mut filled = Value::Null;
    for _ in 0..2 {
        filled = run(&client, &base, base_ts_ns, WIDE).await;
        assert_eq!(figure(&filled, "pressureEvictions"), 0, "{filled}");
        assert_eq!(figure(&filled, "pressureFreedBytes"), 0, "{filled}");
    }
    let resident = figure(&filled, "residentBytes");
    assert!(
        resident > (ROWS * BODY_BYTES) as u64,
        "the body column is cached: {filled}"
    );

    // Everything else the process could still reserve, taken on the
    // best-effort path, which never asks the evictor.
    let headroom = budget.limit() - budget.reserved();
    let held = budget
        .reserve_best_effort(headroom)
        .expect("the free headroom fits");
    assert_eq!(budget.reserved(), budget.limit(), "the budget is full");
    let refusals = (budget.accounted_refusals(), budget.gate_refusals());

    let narrow = run(&client, &base, base_ts_ns, NARROW).await;
    let evictions = figure(&narrow, "pressureEvictions");
    let freed = figure(&narrow, "pressureFreedBytes");
    assert!(evictions > 0, "{narrow}");
    assert!(freed > 0 && freed <= resident, "{narrow}");
    // The narrow statement may re-admit its own columns afterwards, but never
    // the body column it does not read.
    assert!(figure(&narrow, "residentBytes") < resident, "{narrow}");
    assert_eq!(
        (budget.accounted_refusals(), budget.gate_refusals()),
        refusals,
        "every refusal the statement met was turned into an admission"
    );

    drop(held);
    running.shutdown().await.expect("server shuts down");
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            decoded_cache_max_bytes: DECODED_CACHE,
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
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
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
        disable_cache: true,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: PROCESS_BUDGET,
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
