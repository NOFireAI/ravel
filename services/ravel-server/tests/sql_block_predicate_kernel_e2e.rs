//! Reachability proof for ADR-2773 decision D2: the exact content predicate
//! the SQL logs scan pushes into the RLOG reader is evaluated per block, and
//! `POST /api/v1/sql` reports it under `stats.pruning`.
//!
//! Three OTLP exports through `POST /v1/logs` in strict mode write three
//! objects of one block each, at consecutive event times. A `ts` window that
//! covers every block leaves no block an arm to evaluate; a window that cuts
//! the first block at its low edge and the last at its high edge evaluates
//! exactly those two and drops the middle one.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Exports, each one object holding one block.
const OBJECTS: i64 = 3;
/// Rows per export.
const ROWS: i64 = 4;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn export_request(rows: i64, base_ts_ns: i64) -> ExportLogsServiceRequest {
    let log_records = (0..rows)
        .map(|i| LogRecord {
            time_unix_nano: (base_ts_ns + i) as u64,
            observed_time_unix_nano: (base_ts_ns + i) as u64,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(format!("row {i}"))),
            }),
            ..Default::default()
        })
        .collect();
    ExportLogsServiceRequest {
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
    }
}

struct Fixture {
    memory: Arc<MemoryStore>,
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
    /// Event time of the first row of the first export.
    first_ts_ns: i64,
    /// Event time of the first row of the next export, advanced per export so
    /// no two rows share a timestamp.
    next_ts_ns: i64,
}

impl Fixture {
    async fn start() -> Self {
        let memory = Arc::new(MemoryStore::new());
        let store: Arc<dyn ObjectStoreBackend> = memory.clone();
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
        let first_ts_ns = now_ns();
        Fixture {
            memory,
            running,
            base,
            client: reqwest::Client::new(),
            first_ts_ns,
            next_ts_ns: first_ts_ns,
        }
    }

    async fn data_keys(&self) -> BTreeSet<String> {
        let prefix = format!("t/{}/l/l0/", tenant_hash().to_hex());
        list_all(self.memory.as_ref(), &prefix)
            .await
            .expect("list log data objects")
            .into_iter()
            .map(|object| object.key)
            .collect()
    }

    /// Export `rows` rows as one strict-mode OTLP request, checking its flush
    /// wrote exactly one data object.
    async fn ingest(&mut self, rows: i64) {
        let before = self.data_keys().await;
        let request = export_request(rows, self.next_ts_ns);
        self.next_ts_ns += rows;
        let response = self
            .client
            .post(format!("{}/v1/logs", self.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
            .await
            .expect("export sent");
        assert_eq!(response.status(), 200, "export accepted");
        let after = self.data_keys().await;
        assert_eq!(
            after.difference(&before).count(),
            1,
            "one export, one data object"
        );
    }

    /// `POST /api/v1/sql` over a request window around every exported row.
    async fn sql(&self, query: &str) -> serde_json::Value {
        let start = (self.next_ts_ns - NS_PER_HOUR) as f64 / 1e9;
        let end = (self.next_ts_ns + NS_PER_HOUR) as f64 / 1e9;
        let response = self
            .client
            .post(format!("{}/api/v1/sql", self.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "query": query, "start": start, "end": end }).to_string())
            .send()
            .await
            .expect("sql request sent");
        let status = response.status();
        let value: serde_json::Value = response.json().await.expect("sql response is JSON");
        assert_eq!(status, 200, "{value}");
        value
    }

    /// The bodies of the rows whose event time is in `[lo, hi]`, as offsets
    /// from the first exported row, through a `ts` filter the scan pushes into
    /// the reader.
    async fn window(&self, lo: i64, hi: i64) -> serde_json::Value {
        let (lo, hi) = (self.first_ts_ns + lo, self.first_ts_ns + hi);
        self.sql(&format!(
            "SELECT body FROM logs \
             WHERE ts >= arrow_cast({lo}, 'Timestamp(Nanosecond, None)') \
               AND ts <= arrow_cast({hi}, 'Timestamp(Nanosecond, None)')"
        ))
        .await
    }
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
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
        disable_cache: true,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
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

fn pruning(value: &serde_json::Value, key: &str) -> u64 {
    value["stats"]["pruning"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("no stats.pruning.{key} in {value}"))
}

fn row_count(value: &serde_json::Value) -> usize {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {value}"))
        .len()
}

/// A window covering every block evaluates no row and drops the predicate on
/// every scanned block; a window cutting the first block at its low edge and
/// the last at its high edge returns exactly the rows inside it, evaluating
/// the two cut blocks' rows and dropping the predicate on the middle block.
#[tokio::test]
async fn block_predicate_counters_reach_the_sql_stats_json() {
    let mut fx = Fixture::start().await;
    for _ in 0..OBJECTS {
        fx.ingest(ROWS).await;
    }
    let last = OBJECTS * ROWS - 1;

    let value = fx.window(0, last).await;
    assert_eq!(row_count(&value), (OBJECTS * ROWS) as usize, "{value}");
    assert_eq!(pruning(&value, "blocksScanned"), OBJECTS as u64, "{value}");
    assert_eq!(pruning(&value, "rowsPredicateEvaluated"), 0, "{value}");
    assert_eq!(
        pruning(&value, "blocksPredicateDropped"),
        pruning(&value, "blocksScanned"),
        "{value}"
    );

    // Rows 2..=9: the first block's last two rows, the whole second block, and
    // the last block's first two.
    let value = fx.window(2, last - 2).await;
    assert_eq!(row_count(&value), 8, "{value}");
    assert_eq!(pruning(&value, "blocksScanned"), OBJECTS as u64, "{value}");
    assert_eq!(pruning(&value, "blocksPredicateDropped"), 1, "{value}");
    assert_eq!(
        pruning(&value, "rowsPredicateEvaluated"),
        2 * ROWS as u64,
        "{value}"
    );

    fx.running.shutdown().await.expect("graceful shutdown");
}
