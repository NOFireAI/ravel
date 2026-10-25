//! Reachability proof for the ADR-2773 contains-arm amendment: `POST
//! /api/v1/sql` pushes `col LIKE '%literal%'` and `col NOT LIKE '%literal%'`
//! on a declared `Str` column into the RLOG reader as `Predicate::Contains`,
//! returns the rows the unpushed plan returns, reports the arm's work under
//! `stats.pruning`, and keeps the whole-segment fast path the unpushed plan
//! takes (issue #3241).
//!
//! The server runs SQL scans on two partitions, one per object, so the fast
//! path's partition conjunct holds and the predicate decides the route.
//! `stats.timings.planningWaitMaxMs` is the route as the endpoint reports it:
//! only a scan the fast path rejected waits on the plan barrier.
//!
//! The tenant's durable config declares `svc` as a `Str` column before the
//! server starts. Two strict-mode OTLP exports through `POST /v1/logs` write
//! two objects of one block each. Object A, one `web` stream: `a0` sets
//! `svc = "alice-checkout-1"`, `a1` `svc = "bob"`, `a2` no `svc` anywhere,
//! `a3` `svc = 7` (an integer, so the `Str` column reads NULL). Object B:
//! a `batch` stream whose resource sets `svc = "checkout-svc"`, with `b0`
//! setting no `svc` (it reads the resource value) and `b2` setting
//! `svc = "zzz"` (the record value wins), and a `web` row `b1` with
//! `svc = "xcheckoutx"`. Every `svc` value is distinct, so each block's
//! `svc` cells sit on a plain page.
//!
//! The unpushed plan is the same statement with `ESCAPE '#'`: the pattern
//! holds no `#`, so it matches the same rows, and the extractor declines any
//! `LIKE` that carries an escape clause.

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
use ravel_logseg::{ColumnSelection, FieldType, Predicate, RlogConfig, RlogReader};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const KEY: &str = "svc";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn kv(key: &str, value: AnyValueVariant) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn str_kv(key: &str, value: &str) -> KeyValue {
    kv(key, AnyValueVariant::StringValue(value.to_string()))
}

/// One stream: its resource attributes and, per row, the body and the
/// record's `svc` attribute, if any.
fn resource_logs(
    resource: Vec<KeyValue>,
    rows: Vec<(&str, Option<AnyValueVariant>)>,
    base_ts_ns: i64,
) -> ResourceLogs {
    let log_records = rows
        .into_iter()
        .enumerate()
        .map(|(i, (body, svc))| {
            let ts = (base_ts_ns + i as i64) as u64;
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(body.to_string())),
                }),
                attributes: svc.map(|v| vec![kv(KEY, v)]).unwrap_or_default(),
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

fn text(s: &str) -> Option<AnyValueVariant> {
    Some(AnyValueVariant::StringValue(s.to_string()))
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
                key: KEY.to_string(),
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

    /// Export `resource_logs` as one strict-mode OTLP request, checking its
    /// flush wrote exactly one data object.
    async fn ingest(&self, resource_logs: Vec<ResourceLogs>) {
        let before = self.data_keys().await;
        let request = ExportLogsServiceRequest { resource_logs };
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

    /// Per data object, its blocks and how many of them hold `svc` cells on
    /// a dictionary page.
    async fn svc_layout(&self) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for key in self.data_keys().await {
            let bytes = self
                .memory
                .get(&key, GetRange::Full)
                .await
                .expect("get the data object")
                .data;
            let cfg = RlogConfig::default();
            let reader = RlogReader::new(&bytes, &cfg).expect("open the RLOG object");
            let mut scan = reader
                .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
                .expect("scan");
            let (mut blocks, mut dict) = (0, 0);
            while let Some(view) = scan.next_block_columnar(&bytes).expect("columnar block") {
                blocks += 1;
                if view
                    .attr_columns_for(KEY)
                    .find(|c| c.ty == FieldType::Str)
                    .and_then(|c| view.str_dict(c.column_id))
                    .is_some()
                {
                    dict += 1;
                }
            }
            out.push((blocks, dict));
        }
        out
    }

    /// `POST /api/v1/sql` over a window around every exported row: the
    /// parsed response and its raw text.
    async fn sql(&self, query: &str) -> (serde_json::Value, String) {
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
        let raw = response.text().await.expect("sql response body");
        let value: serde_json::Value = serde_json::from_str(&raw).expect("sql response is JSON");
        assert_eq!(status, 200, "{value}");
        (value, raw)
    }
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: ravel_server::QueryBudgets {
            sql_partition_count: 2,
            ..Default::default()
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
        cache_max_bytes: 256 * 1024 * 1024,
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

/// The returned bodies, sorted.
fn bodies(value: &serde_json::Value) -> Vec<String> {
    let mut out: Vec<String> = value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {value}"))
        .iter()
        .map(|row| {
            row[0]
                .as_str()
                .unwrap_or_else(|| panic!("a body: {value}"))
                .to_string()
        })
        .collect();
    out.sort();
    out
}

/// `(containsCellsTested, strCellsValidated)` from `stats.pruning`, each
/// checked to appear exactly once in the response text.
fn contains_counters(value: &serde_json::Value, raw: &str) -> (u64, u64) {
    let read = |key: &str| {
        assert_eq!(
            raw.matches(&format!("\"{key}\":")).count(),
            1,
            "{key} appears exactly once: {raw}"
        );
        value["stats"]["pruning"][key]
            .as_u64()
            .unwrap_or_else(|| panic!("no stats.pruning.{key} in {value}"))
    };
    (read("containsCellsTested"), read("strCellsValidated"))
}

/// `stats.timings.planningWaitMaxMs`, checked to appear exactly once. Only a
/// scan that waits on the plan barrier records it, so it reads exactly 0 on
/// the whole-segment fast path and above 0 on the planned route.
fn planning_wait_ms(value: &serde_json::Value, raw: &str) -> f64 {
    assert_eq!(
        raw.matches("\"planningWaitMaxMs\":").count(),
        1,
        "planningWaitMaxMs appears exactly once: {raw}"
    );
    value["stats"]["timings"]["planningWaitMaxMs"]
        .as_f64()
        .unwrap_or_else(|| panic!("no stats.timings.planningWaitMaxMs in {value}"))
}

/// `LIKE '%checkout%'` and `NOT LIKE '%checkout%'` on the declared `svc`
/// column return the same rows pushed and unpushed, and `stats.pruning`
/// carries the exact counts of each plan.
///
/// Pushed, the `Contains` arm tests each present `Str` cell of both blocks
/// once: `a0`, `a1`, `b1`, `b2`, so `containsCellsTested` is 4 for either
/// statement. Every `svc` cell is on a plain page, so the reader validates
/// nothing and `strCellsValidated` is the `Str` cells the declared build reads
/// for the rows the arm kept: `a0` and `b1` for `LIKE` (`b0` reads its
/// stream), `a1` and `b2` for `NOT LIKE` (`a2` and `a3` have no `Str` cell).
/// Unpushed, the arm never runs and the build reads all 4 `Str` cells. Both
/// plans read `planningWaitMaxMs` 0: the pushed arm keeps the whole-segment
/// fast path.
///
/// Flipped assertion, seen failing: `contains_predicate` in ravel-sql's
/// `logs_pushdown.rs` returning `None` makes the pushed `LIKE` report
/// `(0, 4)` where `(4, 2)` is expected.
#[tokio::test]
async fn like_contains_pushdown_reaches_the_sql_stats_json() {
    let fx = Fixture::start().await;
    let web = || vec![str_kv("service.name", "web")];
    fx.ingest(vec![resource_logs(
        web(),
        vec![
            ("a0", text("alice-checkout-1")),
            ("a1", text("bob")),
            ("a2", None),
            ("a3", Some(AnyValueVariant::IntValue(7))),
        ],
        fx.base_ts_ns,
    )])
    .await;
    fx.ingest(vec![
        resource_logs(
            vec![str_kv("service.name", "batch"), str_kv(KEY, "checkout-svc")],
            vec![("b0", None), ("b2", text("zzz"))],
            fx.base_ts_ns + 10,
        ),
        resource_logs(web(), vec![("b1", text("xcheckoutx"))], fx.base_ts_ns + 20),
    ])
    .await;
    assert_eq!(
        fx.svc_layout().await,
        vec![(1, 0), (1, 0)],
        "two objects of one block each, no `svc` dictionary page"
    );

    let cases = [
        ("LIKE", vec!["a0", "b0", "b1"], (4, 2)),
        ("NOT LIKE", vec!["a1", "b2"], (4, 2)),
    ];
    for (op, want, pushed_counters) in cases {
        let pushed = format!("SELECT body FROM logs WHERE {KEY} {op} '%checkout%'");
        let unpushed = format!("{pushed} ESCAPE '#'");

        let (value, raw) = fx.sql(&pushed).await;
        assert_eq!(bodies(&value), want, "{op} pushed: {value}");
        assert_eq!(
            contains_counters(&value, &raw),
            pushed_counters,
            "{op} pushed: {value}"
        );
        assert_eq!(
            planning_wait_ms(&value, &raw),
            0.0,
            "{op} pushed stays on the whole-segment route: {value}"
        );

        let (value, raw) = fx.sql(&unpushed).await;
        assert_eq!(bodies(&value), want, "{op} unpushed: {value}");
        assert_eq!(
            contains_counters(&value, &raw),
            (0, 4),
            "{op} unpushed: {value}"
        );
        assert_eq!(
            planning_wait_ms(&value, &raw),
            0.0,
            "{op} unpushed is on the whole-segment route: {value}"
        );
    }

    // A `has_word` arm beside the `Contains` arm is a block predicate, so
    // this statement waits on the plan barrier: the route pin above can read
    // non-zero.
    let (value, raw) = fx
        .sql(&format!(
            "SELECT body FROM logs WHERE {KEY} LIKE '%checkout%' AND has_word(body, 'a0')"
        ))
        .await;
    assert_eq!(bodies(&value), vec!["a0"], "{value}");
    assert!(
        planning_wait_ms(&value, &raw) > 0.0,
        "has_word plans: {value}"
    );

    fx.running.shutdown().await.expect("graceful shutdown");
}
