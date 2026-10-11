//! Reachability proof for ADR-2773 decision D3: a declared `Str` column read
//! through a shared dictionary answers `POST /api/v1/sql` on a real
//! [`ravel_server::start`] server with the values the written records hold,
//! a resource fallback value included, and the scan builds the dictionary's
//! Arrow values once rather than once per block.
//!
//! The tenant's durable config declares `Region` as a `Str` column before the
//! server starts. One strict-mode OTLP export through `POST /v1/logs` writes
//! one object: a `web` stream whose records set `Region` from a pool of three
//! values, over several blocks, and a `batch` stream whose records omit
//! `Region` and whose resource carries `Region = "emea"`, so those rows read
//! the resource value. The two streams fill exactly three default-size blocks,
//! so the `batch` rows share a block, and its dictionary page, with `web`
//! rows.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_logseg::{ColumnSelection, DictValues, FieldType, Predicate, RlogConfig, RlogReader};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The `web` stream's `Region` values, assigned round robin.
const POOL: [&str; 3] = ["apac", "eu", "us"];
/// Rows of the `batch` stream, none of which sets `Region`.
const BATCH_ROWS: usize = 10;
/// Rows of the `web` stream: with the `batch` rows, three default-size
/// blocks' worth.
const WEB_ROWS: usize = 3 * 8192 - BATCH_ROWS;
/// The `batch` stream's resource value of `Region`.
const FALLBACK: &str = "emea";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn str_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One stream's records: `resource` is its resource attributes, and row `i`
/// sets `Region` to `region(i)` when that is `Some`.
fn resource_logs(
    resource: Vec<KeyValue>,
    rows: usize,
    base_ts_ns: i64,
    region: impl Fn(usize) -> Option<&'static str>,
) -> ResourceLogs {
    let log_records = (0..rows)
        .map(|i| {
            let ts = (base_ts_ns + i as i64) as u64;
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue("hit".to_string())),
                }),
                attributes: region(i)
                    .map(|r| vec![str_kv("Region", r)])
                    .unwrap_or_default(),
                ..Default::default()
            }
        })
        .collect();
    ResourceLogs {
        resource: Some(Resource {
            attributes: resource,
            ..Default::default()
        }),
        scope_logs: vec![ScopeLogs {
            scope: None,
            log_records,
            schema_url: String::new(),
        }],
        ..Default::default()
    }
}

struct Fixture {
    memory: Arc<MemoryStore>,
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
    base_ts_ns: i64,
}

impl Fixture {
    async fn start() -> Self {
        let memory = Arc::new(MemoryStore::new());
        let config = TenantConfig {
            typed_attr_columns: Some(vec![DeclaredTypedColumn {
                key: "Region".to_string(),
                ty: DeclaredColumnType::Str,
            }]),
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
            base_ts_ns: now_ns(),
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

    /// Export both streams as one strict-mode OTLP request, checking its
    /// flush wrote exactly one data object.
    async fn ingest(&self) {
        let before = self.data_keys().await;
        let request = ExportLogsServiceRequest {
            resource_logs: vec![
                resource_logs(
                    vec![str_kv("service.name", "web")],
                    WEB_ROWS,
                    self.base_ts_ns,
                    |i| Some(POOL[i % POOL.len()]),
                ),
                resource_logs(
                    vec![str_kv("service.name", "batch"), str_kv("Region", FALLBACK)],
                    BATCH_ROWS,
                    self.base_ts_ns + WEB_ROWS as i64,
                    |_| None,
                ),
            ],
        };
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

    /// How the written object lays out `Region`: its blocks, the distinct
    /// dictionaries they read, the blocks whose `Region` cells are not on a
    /// dictionary page, and the dictionary-page blocks holding a row that sets
    /// no `Region` (the `batch` rows).
    async fn region_layout(&self) -> (usize, usize, usize, usize) {
        let keys = self.data_keys().await;
        assert_eq!(keys.len(), 1, "one data object");
        let key = keys.first().expect("one key");
        let bytes = self
            .memory
            .get(key, GetRange::Full)
            .await
            .expect("get the data object")
            .data;
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&bytes, &cfg).expect("open the RLOG object");
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let mut dicts: Vec<Arc<DictValues>> = Vec::new();
        let (mut blocks, mut plain, mut with_fallback) = (0, 0, 0);
        while let Some(view) = scan.next_block_columnar(&bytes).expect("columnar block") {
            blocks += 1;
            let page = view
                .attr_columns_for("Region")
                .find(|c| c.ty == FieldType::Str)
                .and_then(|c| view.str_dict(c.column_id));
            match page {
                Some(page) => {
                    if !dicts.iter().any(|d| Arc::ptr_eq(d, page.values())) {
                        dicts.push(Arc::clone(page.values()));
                    }
                    if (0..view.surviving_count()).any(|i| page.key_at(i).is_none()) {
                        with_fallback += 1;
                    }
                }
                None => plain += 1,
            }
        }
        (blocks, dicts.len(), plain, with_fallback)
    }

    /// `POST /api/v1/sql` over a window around every exported row.
    async fn sql(&self, query: &str) -> serde_json::Value {
        let start = (self.base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
        let end = (self.base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
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
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: Default::default(),
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

fn rows(value: &serde_json::Value) -> &Vec<serde_json::Value> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {value}"))
}

fn str_dict_values_built(value: &serde_json::Value) -> u64 {
    value["stats"]["accounting"]["strDictValuesBuilt"]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.accounting.strDictValuesBuilt: {value}"))
}

/// GROUP BY and COUNT(DISTINCT) over the declared `Region` column equal the
/// counts computed from the written records, the `batch` stream's rows
/// grouped under its resource value. The column keeps its
/// `Dictionary(Int32, Utf8)` type. The object is three blocks on one row group
/// dictionary, the last also holding the `batch` rows, and each statement
/// builds 2 values arrays: the dictionary's, and that dictionary with the
/// resource value appended for the block whose rows read it.
///
/// Flipped: `StrValuesCache::values` in ravel-sql's `logs_scan.rs` looking
/// up no cached entry rebuilds the values per block, and the counter reads
/// 4: one per block plus the appended copy.
#[tokio::test]
async fn declared_str_dictionary_answers_group_by_through_http() {
    let fx = Fixture::start().await;
    fx.ingest().await;

    let mut want: BTreeMap<String, u64> = BTreeMap::new();
    for i in 0..WEB_ROWS {
        *want.entry(POOL[i % POOL.len()].to_string()).or_default() += 1;
    }
    *want.entry(FALLBACK.to_string()).or_default() += BATCH_ROWS as u64;

    let grouped = fx
        .sql("SELECT \"Region\", COUNT(*) FROM logs GROUP BY \"Region\" ORDER BY \"Region\"")
        .await;
    let got: BTreeMap<String, u64> = rows(&grouped)
        .iter()
        .map(|row| {
            let region = row[0]
                .as_str()
                .unwrap_or_else(|| panic!("a non-NULL region: {grouped}"));
            (region.to_string(), row[1].as_u64().expect("a count"))
        })
        .collect();
    assert_eq!(got, want, "{grouped}");

    let distinct = fx.sql("SELECT COUNT(DISTINCT \"Region\") FROM logs").await;
    assert_eq!(
        rows(&distinct)[0][0].as_u64(),
        Some(want.len() as u64),
        "{distinct}"
    );

    let projected = fx
        .sql("SELECT \"Region\" FROM logs WHERE \"Region\" = 'emea'")
        .await;
    assert_eq!(
        projected["data"]["columns"][0]["type"],
        serde_json::json!("Dictionary(Int32, Utf8)"),
        "{projected}"
    );
    assert_eq!(rows(&projected).len(), BATCH_ROWS, "{projected}");
    for row in rows(&projected) {
        assert_eq!(row[0], serde_json::json!(FALLBACK), "{projected}");
    }

    assert_eq!(
        fx.region_layout().await,
        (3, 1, 0, 1),
        "(blocks, distinct dictionaries, plain-page blocks, dictionary blocks holding rows with no Region)"
    );
    for value in [&grouped, &distinct] {
        assert_eq!(str_dict_values_built(value), 2, "{value}");
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}
