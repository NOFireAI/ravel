//! Reachability proof for ADR-2773 decision 1: declared `I64` columns built
//! from the decoded value and validity buffers answer `POST /api/v1/sql` on a
//! real [`ravel_server::start`] server with the values the written records
//! hold, NULLs included.
//!
//! The tenant's durable config declares `Amount` and `Weight` as `I64`
//! columns before the server starts. Each object is written by one OTLP
//! export through `POST /v1/logs` in strict mode, so each holds one block: the
//! first object's first row leaves `Amount` unset and the second object's
//! last row does, and `Weight` is unset on interior rows of both. A record
//! that does not set a key reads NULL for it, and no resource attribute
//! carries either key, so no fallback value fills a NULL.

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
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// One log row's declared attributes; `None` leaves the key unset.
#[derive(Clone, Copy)]
struct Row {
    amount: Option<i64>,
    weight: Option<i64>,
}

const fn row(amount: Option<i64>, weight: Option<i64>) -> Row {
    Row { amount, weight }
}

/// The first object: `Amount` unset on its first row, `Weight` on an
/// interior one.
const FIRST: [Row; 5] = [
    row(None, Some(1)),
    row(Some(5), Some(2)),
    row(Some(7), None),
    row(Some(-3), Some(4)),
    row(Some(11), Some(9)),
];

/// The second object: `Amount` unset on its last row and an interior one,
/// `Weight` on an interior one.
const SECOND: [Row; 5] = [
    row(Some(10), Some(6)),
    row(Some(4), None),
    row(None, Some(3)),
    row(Some(2), Some(8)),
    row(None, Some(7)),
];

fn int_kv(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::IntValue(value)),
        }),
        ..Default::default()
    }
}

fn export_request(rows: &[Row], base_ts_ns: i64) -> ExportLogsServiceRequest {
    let log_records = rows
        .iter()
        .zip(0i64..)
        .map(|(row, i)| {
            let mut attributes = Vec::new();
            if let Some(v) = row.amount {
                attributes.push(int_kv("Amount", v));
            }
            if let Some(v) = row.weight {
                attributes.push(int_kv("Weight", v));
            }
            LogRecord {
                time_unix_nano: (base_ts_ns + i) as u64,
                observed_time_unix_nano: (base_ts_ns + i) as u64,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue("hit".to_string())),
                }),
                attributes,
                ..Default::default()
            }
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
    /// Event time of the first row of the next export, advanced per export so
    /// no two rows share a timestamp.
    next_ts_ns: i64,
}

impl Fixture {
    async fn start() -> Self {
        let memory = Arc::new(MemoryStore::new());
        let declared = ["Amount", "Weight"]
            .into_iter()
            .map(|key| DeclaredTypedColumn {
                key: key.to_string(),
                ty: DeclaredColumnType::I64,
            })
            .collect();
        let config = TenantConfig {
            typed_attr_columns: Some(declared),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        ravel_catalog::set_tenant_config(memory.as_ref(), &tenant_hash(), &config, now_ns())
            .await
            .expect("tenant config");

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
        Fixture {
            memory,
            running,
            base,
            client: reqwest::Client::new(),
            next_ts_ns: now_ns(),
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

    /// Export `rows` as one strict-mode OTLP request, checking its flush wrote
    /// exactly one data object.
    async fn ingest(&mut self, rows: &[Row]) {
        let before = self.data_keys().await;
        let request = export_request(rows, self.next_ts_ns);
        self.next_ts_ns += rows.len() as i64;
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

    /// `POST /api/v1/sql` over a window around every exported row.
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
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        audit_pipeline: ravel_maintain::AuditPipelineConfig::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
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

/// SUM, COUNT, AVG, MIN and MAX of one column over the present cells, as SQL
/// computes them: SUM, AVG, MIN and MAX are NULL over no present cell.
fn expected_aggregates(cells: &[Option<i64>]) -> serde_json::Value {
    let present: Vec<i64> = cells.iter().flatten().copied().collect();
    if present.is_empty() {
        return serde_json::json!([null, 0, null, null, null]);
    }
    let sum: i64 = present.iter().sum();
    serde_json::json!([
        sum,
        present.len(),
        sum as f64 / present.len() as f64,
        present.iter().min(),
        present.iter().max(),
    ])
}

/// The one result row's five aggregates for `column` and then the five for
/// `other`, as the statement lists them.
fn aggregate_row(value: &serde_json::Value) -> &[serde_json::Value] {
    let rows = value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {value}"));
    assert_eq!(rows.len(), 1, "one aggregate row: {value}");
    rows[0]
        .as_array()
        .unwrap_or_else(|| panic!("row: {value}"))
}

fn int_values_buffers(value: &serde_json::Value) -> u64 {
    value["stats"]["accounting"]["intValuesBuffers"]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.accounting.intValuesBuffers: {value}"))
}

/// Every row of both objects, then only the rows whose `Weight` is above 3,
/// aggregated over both columns through HTTP: each figure equals the one
/// computed from the written records, and the scan reports the integer value
/// buffers it built.
///
/// Flipped assertion: `normalized_validity` in ravel-logseg's `block.rs`
/// returning `None` whatever the presence bitmap says (every row present)
/// reads the absent `Amount` slots as 0, and the first statement's
/// `COUNT("Amount")` reads `left: 10, right: 7`.
#[tokio::test]
async fn declared_integer_aggregates_match_the_written_records_through_http() {
    let mut fx = Fixture::start().await;
    fx.ingest(&FIRST).await;
    fx.ingest(&SECOND).await;
    let all: Vec<Row> = FIRST.iter().chain(&SECOND).copied().collect();

    let statement = |filter: &str| {
        format!(
            "SELECT SUM(\"Amount\"), COUNT(\"Amount\"), AVG(\"Amount\"), \
             MIN(\"Amount\"), MAX(\"Amount\"), SUM(\"Weight\"), COUNT(\"Weight\"), \
             AVG(\"Weight\"), MIN(\"Weight\"), MAX(\"Weight\") FROM logs{filter}"
        )
    };
    for (filter, keep) in [
        ("", (|_: &Row| true) as fn(&Row) -> bool),
        (" WHERE \"Weight\" > 3", |r: &Row| r.weight.is_some_and(|w| w > 3)),
    ] {
        let kept: Vec<Row> = all.iter().copied().filter(keep).collect();
        let amount: Vec<Option<i64>> = kept.iter().map(|r| r.amount).collect();
        let weight: Vec<Option<i64>> = kept.iter().map(|r| r.weight).collect();
        let value = fx.sql(&statement(filter)).await;
        let got = aggregate_row(&value);
        for (offset, name, cells) in [(0, "Amount", &amount), (5, "Weight", &weight)] {
            let expected = expected_aggregates(cells);
            let expected = expected.as_array().expect("array");
            for (k, label) in ["SUM", "COUNT", "AVG", "MIN", "MAX"].iter().enumerate() {
                let (got, want) = (&got[offset + k], &expected[k]);
                match (got.as_f64(), want.as_f64()) {
                    (Some(g), Some(w)) if *label == "AVG" => assert!(
                        (g - w).abs() < 1e-9,
                        "{label}(\"{name}\"){filter}: {g} != {w}: {value}"
                    ),
                    _ => assert_eq!(
                        got.as_i64(),
                        want.as_i64(),
                        "{label}(\"{name}\"){filter}: {value}"
                    ),
                }
                assert_eq!(got.is_null(), want.is_null(), "{label}(\"{name}\"){filter}");
            }
        }
        assert!(int_values_buffers(&value) > 0, "{value}");
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}
