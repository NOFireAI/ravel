//! ADR-4628 test plan item 12 for ravel-server's two stamp producers, the
//! ingest role and the maintain role, and `--no-declared-stat-aggregates`
//! through `Cli` parsing.
//!
//! Ingest role: a real [`ravel_server::start`] server in `--mode all`, its
//! `emit_declared_stat_aggregates` taken from a parsed `Cli`, ingests thirty
//! rows of a tenant that declares `AdvEngineID`, `ResolutionWidth` and
//! `UserID` as `I64` through `POST /v1/logs`, three strict exports of ten
//! rows, one RLOG object each. Without the flag every commit record carries
//! fields 6 to 8; with it none does. The four statements of ADR-4628 then run
//! through a real `POST /api/v1/sql` router over the unfolded catalog: the
//! emitting world answers from the stamps with zero data-object GETs, the
//! withholding world scans every object, and the answers are equal. The one
//! hop this does not run is `main`'s copy of the parsed flag onto
//! `ServerConfig`, which `main.rs`'s own tests pin.
//!
//! Maintain role: the `CompactorConfig` `Cli::resolve_compactor_config`
//! builds for `--mode maintain`, the config `main` hands every maintain task,
//! compacts an emitting world's hour with `ravel_maintain::compact_bucket`.
//! Without the flag the part carries fields 6 to 8, with it none; the
//! statements answer from the emitting part's stamp and scan the withholding
//! part, with equal answers. The maintenance loop's scheduling, which runs
//! that same config, is not run here.
//!
//! GETs are counted below every cache by a key-classifying wrapper, the shape
//! of `KeyCountingStore` in `services/ravel-cli/tests/
//! load_declared_stat_aggregates_e2e.rs`.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_catalog::{
    Catalog, CatalogConfig, DeclaredColumnType, DeclaredTypedColumn, TenantConfig,
    TenantLifecycleState,
};
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, CompactionRecord, DeclaredColumnMinMax as DeclaredColumnStat,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::config::{Cli, HostProfile};
use ravel_server::declared_columns::TenantConfigDeclaredColumns;
use ravel_server::typed_attr_config::TypedAttrColumnConfig;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;

const TENANT: &str = "stamp-server";
const TOKEN: &str = "stamp-server-token";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const TEST_HORIZON_NS: i64 = 1_000;
const TEST_BACKOFF_NS: i64 = 1_000;

/// Rows ingested, rows per strict export, and the RLOG objects the exports
/// write: one per export.
const ROWS: usize = 30;
const BATCH_ROWS: usize = 10;
const OBJECTS: u64 = 3;

/// Thirty rows compact into one L1 part.
const PARTS: u64 = 1;

/// `UserID` of the first export's rows: ten of them sum past `i64::MAX`, so
/// that object's stamp carries the sum in field 7.
const BIG_USER: i64 = 1 << 62;

const Q1: &str = "SELECT COUNT(*) FROM logs WHERE \"AdvEngineID\" <> 0";
const Q2: &str = "SELECT SUM(\"AdvEngineID\"), COUNT(*), AVG(\"ResolutionWidth\") FROM logs";
const Q3: &str = "SELECT AVG(\"UserID\") FROM logs";

fn clickbench_q29() -> String {
    let terms: Vec<String> = (0..90)
        .map(|k| {
            if k == 0 {
                "SUM(\"ResolutionWidth\")".to_string()
            } else {
                format!("SUM(\"ResolutionWidth\" + {k})")
            }
        })
        .collect();
    format!("SELECT {} FROM logs", terms.join(", "))
}

/// 0, 2, 5 repeating: every export holds zero and nonzero values, so 0 lies
/// strictly inside each object's min and max and no min or max answers Q1.
fn adv_engine_id(row: usize) -> i64 {
    [0, 2, 5][row % 3]
}

fn resolution_width(row: usize) -> i64 {
    1_000 + row as i64
}

fn user_id(row: usize) -> i64 {
    if row < BATCH_ROWS {
        BIG_USER + row as i64
    } else {
        1_000_000 + row as i64
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

/// GETs of data objects (`t/<tenant>/<signal>/l0|l1|rw/...`), counted below
/// every cache.
#[derive(Default)]
struct KeyCountingStore {
    inner: MemoryStore,
    data_gets: AtomicU64,
}

fn is_data_key(key: &str) -> bool {
    let parts: Vec<&str> = key.split('/').collect();
    parts.len() > 3 && matches!(parts[3], "l0" | "l1" | "rw")
}

#[async_trait::async_trait]
impl ObjectStoreBackend for KeyCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if is_data_key(key) {
            self.data_gets.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// The parsed `Cli` of a process in `mode`, with the flag when `withhold`.
fn parsed_cli(mode: &str, withhold: bool, extra: &[&str]) -> Cli {
    let mut args = vec!["ravel-server", "--mode", mode];
    if withhold {
        args.push("--no-declared-stat-aggregates");
    }
    args.extend_from_slice(extra);
    Cli::parse_validated_from(args).expect("the flags parse")
}

fn int_kv(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::IntValue(value)),
        }),
        ..Default::default()
    }
}

fn export_request(rows: std::ops::Range<usize>, base_ts_ns: i64) -> ExportLogsServiceRequest {
    let log_records = rows
        .map(|row| LogRecord {
            time_unix_nano: (base_ts_ns + row as i64 * 1_000) as u64,
            observed_time_unix_nano: (base_ts_ns + row as i64 * 1_000) as u64,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(format!("row{row}"))),
            }),
            attributes: vec![
                int_kv("AdvEngineID", adv_engine_id(row)),
                int_kv("ResolutionWidth", resolution_width(row)),
                int_kv("UserID", user_id(row)),
            ],
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

fn server_config(emit_declared_stat_aggregates: bool) -> ServerConfig {
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
    ServerConfig {
        emit_declared_stat_aggregates,
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
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
        ingest_hold: Default::default(),
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

/// A store holding the thirty rows as ingested by a `--mode all` server whose
/// stamp setting came from `cli`, and the ingest hour they landed in.
struct World {
    store: Arc<KeyCountingStore>,
    hour: u32,
}

/// Wait out the last minute of an hour, so the three exports land in one
/// ingest hour and the resolve GET count does not depend on when the test
/// runs.
async fn away_from_an_hour_boundary() {
    let into_hour = now_ns().rem_euclid(NS_PER_HOUR);
    let left = NS_PER_HOUR - into_hour;
    if left < 60_000_000_000 {
        tokio::time::sleep(Duration::from_nanos((left + 1_000_000_000) as u64)).await;
    }
}

async fn ingested_world(cli: &Cli) -> World {
    let store = Arc::new(KeyCountingStore::default());
    let declared = ["AdvEngineID", "ResolutionWidth", "UserID"]
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
    ravel_catalog::set_tenant_config(&store.inner, &tenant_hash(), &config, now_ns())
        .await
        .expect("tenant config");

    away_from_an_hour_boundary().await;
    let hour = u32::try_from(now_ns() / NS_PER_HOUR).expect("hour fits u32");
    let backend = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let running = ravel_server::start(
        server_config(cli.emit_declared_stat_aggregates),
        backend.clone(),
        backend,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns() - 60_000_000_000;
    for first in (0..ROWS).step_by(BATCH_ROWS) {
        let response = client
            .post(format!("{base}/v1/logs"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf")
            .body(export_request(first..first + BATCH_ROWS, base_ts_ns).encode_to_vec())
            .send()
            .await
            .expect("export sent");
        assert_eq!(response.status(), 200, "export accepted");
        assert!(
            response.headers().get("x-ravel-commit-token").is_some(),
            "a strict export acks after its commit"
        );
    }
    running.shutdown().await.expect("graceful shutdown");
    assert_eq!(
        hour,
        u32::try_from(now_ns() / NS_PER_HOUR).expect("hour fits u32"),
        "the exports stayed in one ingest hour"
    );
    World { store, hour }
}

/// The record files under the tenant's logs commit prefix: `(name, bytes)`.
async fn record_files(store: &KeyCountingStore) -> Vec<(String, bytes::Bytes)> {
    let prefix = format!("t/{}/l/c/", tenant_hash().to_hex());
    let mut out = Vec::new();
    for meta in list_all(&store.inner, &prefix).await.expect("list") {
        let name = meta.key.rsplit('/').next().expect("a name").to_string();
        if !name.ends_with(".cmt") {
            continue;
        }
        let data = store
            .inner
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get record")
            .data;
        out.push((name, data));
    }
    out
}

/// Every commit record (not a compaction, rewrite or tombstone).
async fn commit_records(store: &KeyCountingStore) -> Vec<CommitRecord> {
    record_files(store)
        .await
        .into_iter()
        .filter(|(name, _)| !name.starts_with("l1.") && !name.starts_with("rw."))
        .map(|(_, data)| ravel_commit::record::decode(&data).expect("a commit record"))
        .collect()
}

/// Every compaction record (`l1.*.cmt`).
async fn compaction_records(store: &KeyCountingStore) -> Vec<CompactionRecord> {
    record_files(store)
        .await
        .into_iter()
        .filter(|(name, _)| name.starts_with("l1."))
        .map(|(_, data)| ravel_commit::record::decode_compaction(&data).expect("a compaction"))
        .collect()
}

/// The three declared columns, each with min, max and a null count of 0,
/// and fields 6 to 8 present exactly when `emitted`. When they are, the
/// `UserID` entry of an object whose sum leaves i64 carries field 7.
fn assert_entries(entries: &[DeclaredColumnStat], emitted: bool, what: &str) {
    let mut names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["AdvEngineID", "ResolutionWidth", "UserID"],
        "{what}"
    );
    for entry in entries {
        assert!(
            entry.min.is_some() && entry.max.is_some(),
            "{what}: {entry:?}"
        );
        assert_eq!(entry.null_count, 0, "{what}: {entry:?}");
        if emitted {
            assert!(
                entry.sum.is_some() != !entry.sum_wide.is_empty(),
                "{what}: exactly one of fields 6 and 7: {entry:?}"
            );
            assert!(entry.zero_count.is_some(), "{what}: field 8: {entry:?}");
        } else {
            assert_eq!(
                (entry.sum, entry.sum_wide.len(), entry.zero_count),
                (None, 0, None),
                "{what}: {entry:?}"
            );
        }
    }
}

/// The validated `(sum, zero_count)` of every declared column of `part`
/// equals the figures the thirty rows give.
fn assert_part_figures(part: &CompactionPart, what: &str) {
    let validated = ravel_commit::declared_stats::read_compaction_part(part);
    for (name, value) in [
        ("AdvEngineID", adv_engine_id as fn(usize) -> i64),
        ("ResolutionWidth", resolution_width),
        ("UserID", user_id),
    ] {
        let stat = validated
            .covered()
            .iter()
            .find(|s| s.name() == name)
            .unwrap_or_else(|| panic!("{what}: {name} covered"));
        let sum: i128 = (0..ROWS).map(|r| i128::from(value(r))).sum();
        let zeros = (0..ROWS).filter(|&r| value(r) == 0).count() as u64;
        assert_eq!(
            (stat.sum(), stat.zero_count()),
            (Some(sum), Some(zeros)),
            "{what}: {name}"
        );
    }
}

struct AdvancingClock(Arc<AtomicI64>);

impl Clock for AdvancingClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A fresh `/api/v1/sql` router over `store`, its caches empty.
fn build_app(store: Arc<KeyCountingStore>) -> Router {
    let store = store as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let tokens = HashMap::from([(TOKEN.to_string(), TenantId::new(TENANT))]);
    let declared = Arc::new(TenantConfigDeclaredColumns::with_intervals(
        TypedAttrColumnConfig::default(),
        Arc::clone(&store),
        TEST_HORIZON_NS,
        TEST_BACKOFF_NS,
    ));
    let mut state = ravel_server::query::build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        Some(declared),
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("build_sql_state");
    state.clock = Arc::new(AdvancingClock(Arc::new(AtomicI64::new(now_ns()))));
    ravel_server::sql::router(state)
}

struct Run {
    row: Vec<Value>,
    stats: Value,
    data_gets: u64,
}

async fn run(store: &Arc<KeyCountingStore>, sql: &str) -> Run {
    let app = build_app(Arc::clone(store));
    let payload = serde_json::json!({ "query": sql, "timeout": 60.0 }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    let before = store.data_gets.load(Ordering::Acquire);
    let response = app.oneshot(request).await.expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let data_gets = store.data_gets.load(Ordering::Acquire) - before;
    let body: Value = serde_json::from_slice(&bytes).expect("a JSON body");
    assert_eq!(status, StatusCode::OK, "{sql}: {body}");
    let rows = body["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{sql}: one row: {body}");
    Run {
        row: rows[0].as_array().expect("a row array").clone(),
        stats: body["stats"].clone(),
        data_gets,
    }
}

#[derive(Debug, Clone, Copy)]
enum Cell {
    Int(i64),
    Float(f64),
}

fn assert_cells(sql: &str, got: &[Value], want: &[Cell]) {
    assert_eq!(got.len(), want.len(), "{sql}: {got:?}");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        match w {
            Cell::Int(v) => assert_eq!(g.as_i64(), Some(*v), "{sql}: column {i}: {got:?}"),
            Cell::Float(v) => assert_eq!(
                g.as_f64().map(f64::to_bits),
                Some(v.to_bits()),
                "{sql}: column {i}: {g} against {v}"
            ),
        }
    }
}

fn assert_same_answer(sql: &str, got: &[Value], want: &[Value]) {
    assert_eq!(got.len(), want.len(), "{sql}");
    for (g, w) in got.iter().zip(want) {
        match (g.as_i64(), w.as_i64()) {
            (Some(a), Some(b)) => assert_eq!(a, b, "{sql}"),
            _ => assert_eq!(
                g.as_f64().map(f64::to_bits),
                w.as_f64().map(f64::to_bits),
                "{sql}: {g} against {w}"
            ),
        }
    }
}

fn statements() -> Vec<(String, Vec<Cell>)> {
    let rows = 0..ROWS;
    let n = ROWS as i64;
    let q1 = rows.clone().filter(|&r| adv_engine_id(r) != 0).count() as i64;
    let adv_sum: i64 = rows.clone().map(adv_engine_id).sum();
    let rw_sum: i64 = rows.clone().map(resolution_width).sum();
    let user_sum: i128 = rows.map(|r| i128::from(user_id(r))).sum();
    vec![
        (Q1.to_string(), vec![Cell::Int(q1)]),
        (
            Q2.to_string(),
            vec![
                Cell::Int(adv_sum),
                Cell::Int(n),
                Cell::Float(rw_sum as f64 / n as f64),
            ],
        ),
        (
            Q3.to_string(),
            vec![Cell::Float(user_sum as f64 / n as f64)],
        ),
        (
            clickbench_q29(),
            (0..90).map(|k| Cell::Int(rw_sum + k * n)).collect(),
        ),
    ]
}

fn pruning(stats: &Value, key: &str) -> u64 {
    stats["pruning"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("no stats.pruning.{key} in {stats}"))
}

fn phase_gets(stats: &Value) -> Vec<(String, u64)> {
    stats["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("no stats.phases in {stats}"))
        .iter()
        .map(|p| {
            (
                p["phase"].as_str().expect("phase name").to_string(),
                p["s3GetRequests"].as_u64().expect("s3GetRequests"),
            )
        })
        .collect()
}

/// The exact per-phase GETs of one statement: one resolve GET for the absent
/// catalog HEAD and one per record the bucket lists (the `OBJECTS` commit
/// records and `compaction_records`), `plan` GETs (the statistics load
/// reading the absent catalog HEAD, which a statement answered from the
/// stamps skips), and `scan`.
fn phases_of(compaction_records: u64, plan: u64, scan: u64) -> Vec<(String, u64)> {
    [
        ("resolve", 1 + OBJECTS + compaction_records),
        ("plan", plan),
        ("probe", 0),
        ("scan", scan),
        ("keyIndex", 0),
        ("fetch", 0),
    ]
    .into_iter()
    .map(|(phase, gets)| (phase.to_string(), gets))
    .collect()
}

/// The four statements over an emitting and a withholding world holding
/// `segments` segments: the emitting one answers each from the stamps with
/// no data-object GET, the withholding one scans every segment with one GET
/// each, and the answers are equal and exact. MIN and MAX over the
/// withholding world still answer from its stamps.
async fn assert_statements(
    emitted: &World,
    withheld: &World,
    segments: u64,
    compaction_records: u64,
    label: &str,
) {
    for (sql, want) in statements() {
        let short = format!("{label} {}", &sql[..sql.len().min(60)]);
        let e = run(&emitted.store, &sql).await;
        let w = run(&withheld.store, &sql).await;
        let e_phases = phase_gets(&e.stats);
        let w_phases = phase_gets(&w.stats);
        println!(
            "{short}: emitted stamp={} gets={} phases {e_phases:?}; withheld stamp={} gets={} \
             phases {w_phases:?}",
            pruning(&e.stats, "segmentsAnsweredFromStamp"),
            e.data_gets,
            pruning(&w.stats, "segmentsAnsweredFromStamp"),
            w.data_gets,
        );
        assert_cells(&short, &e.row, &want);
        assert_cells(&short, &w.row, &want);
        assert_same_answer(&short, &w.row, &e.row);
        assert_eq!(pruning(&e.stats, "segments"), segments, "{short}");
        assert_eq!(
            pruning(&e.stats, "segmentsAnsweredFromStamp"),
            segments,
            "{short}"
        );
        assert_eq!(e.data_gets, 0, "{short}");
        assert_eq!(phases_of(compaction_records, 0, 0), e_phases, "{short}");
        assert_eq!(pruning(&w.stats, "segments"), segments, "{short}");
        assert_eq!(pruning(&w.stats, "segmentsAnsweredFromStamp"), 0, "{short}");
        assert_eq!(pruning(&w.stats, "segmentsAnsweredFromCstat"), 0, "{short}");
        assert_eq!(w.data_gets, segments, "{short}: one GET per segment");
        assert_eq!(
            phases_of(compaction_records, 1, segments),
            w_phases,
            "{short}"
        );
    }

    let min_max = "SELECT MIN(\"UserID\"), MAX(\"UserID\") FROM logs";
    let r = run(&withheld.store, min_max).await;
    let users: Vec<i64> = (0..ROWS).map(user_id).collect();
    assert_cells(
        min_max,
        &r.row,
        &[
            Cell::Int(*users.iter().min().expect("rows")),
            Cell::Int(*users.iter().max().expect("rows")),
        ],
    );
    assert_eq!(
        r.data_gets, 0,
        "{label}: MIN and MAX answer from the stamps"
    );
}

/// Test 12, ingest role: a `--mode all` server parsed with
/// `--no-declared-stat-aggregates` writes commit records with min, max and
/// null count and none of fields 6 to 8; one parsed without it writes them.
/// The statements answer from the emitting stamps and scan the withholding
/// objects, with equal answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ingest_role_withholds_the_stamp_aggregates_with_the_flag() {
    let emitting_cli = parsed_cli("all", false, &[]);
    let withholding_cli = parsed_cli("all", true, &[]);
    assert!(emitting_cli.emit_declared_stat_aggregates);
    assert!(!withholding_cli.emit_declared_stat_aggregates);
    let emitted = ingested_world(&emitting_cli).await;
    let withheld = ingested_world(&withholding_cli).await;

    for (world, is_emitted) in [(&emitted, true), (&withheld, false)] {
        let records = commit_records(&world.store).await;
        assert_eq!(records.len() as u64, OBJECTS, "one record per export");
        let mut wide = 0;
        for record in &records {
            assert_eq!(record.sample_count, BATCH_ROWS as u64);
            assert_entries(&record.declared_column_stats, is_emitted, "commit record");
            wide += record
                .declared_column_stats
                .iter()
                .filter(|e| !e.sum_wide.is_empty())
                .count();
        }
        assert_eq!(
            wide,
            usize::from(is_emitted),
            "only the first export's UserID sum leaves i64, and only an emitting record says so"
        );
    }
    assert_statements(&emitted, &withheld, OBJECTS, 0, "ingest").await;
}

/// The `CompactorConfig` a `--mode maintain` process resolves from `cli`,
/// as `main` does before handing it to every maintain task.
fn maintain_compactor(cli: &Cli) -> ravel_maintain::CompactorConfig {
    let gc_runtime = cli
        .resolve_gc_runtime(Duration::from_secs(30))
        .expect("gc runtime");
    let host = HostProfile::new(8, Some(32 << 30), Some(32 << 30), None, None, None);
    let performance = cli.resolve_performance(host).expect("performance defaults");
    cli.resolve_compactor_config(&gc_runtime, &performance)
        .expect("compactor config")
}

/// An emitting world's hour compacted with the maintain role's compactor.
async fn compacted_world(cli: &Cli) -> World {
    let world = ingested_world(&parsed_cli("all", false, &[])).await;
    let config = maintain_compactor(cli);
    let bucket = ravel_maintain::Bucket::new(tenant_hash(), Signal::Logs, 0, world.hour);
    let outcome = ravel_maintain::compact_bucket(
        world.store.as_ref(),
        &ravel_maintain::FixedClock::new((i64::from(world.hour) + 3) * NS_PER_HOUR),
        &config,
        &bucket,
    )
    .await
    .expect("compact");
    assert!(
        matches!(
            outcome,
            ravel_maintain::CompactionOutcome::Compacted {
                publish: ravel_maintain::PublishOutcome::Published,
                ..
            }
        ),
        "the hour compacts: {outcome:?}"
    );
    world
}

/// Test 12, maintain role: the compactor `resolve_compactor_config` builds
/// for `--mode maintain` emits without the flag and withholds with it; a
/// compaction through it writes a part with or without fields 6 to 8, and
/// the statements answer from the emitting part and scan the withholding one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_maintain_role_withholds_the_stamp_aggregates_with_the_flag() {
    let fence_off = ["--late-commit-fence", "off"];
    let emitting_cli = parsed_cli("maintain", false, &fence_off);
    let withholding_cli = parsed_cli("maintain", true, &fence_off);
    assert!(maintain_compactor(&emitting_cli).emit_declared_stat_aggregates);
    assert!(!maintain_compactor(&withholding_cli).emit_declared_stat_aggregates);
    let emitted = compacted_world(&emitting_cli).await;
    let withheld = compacted_world(&withholding_cli).await;

    for (world, is_emitted) in [(&emitted, true), (&withheld, false)] {
        let records = compaction_records(&world.store).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].parts.len() as u64, PARTS);
        let part = &records[0].parts[0];
        assert_eq!(part.sample_count, ROWS as u64);
        assert_entries(&part.declared_column_stats, is_emitted, "compaction part");
        if is_emitted {
            assert_part_figures(part, "compaction part");
            let user = part
                .declared_column_stats
                .iter()
                .find(|e| e.name == "UserID")
                .expect("UserID");
            assert!(
                user.sum.is_none() && !user.sum_wide.is_empty(),
                "field 7 carries the UserID sum past i64: {user:?}"
            );
        }
    }
    assert_statements(&emitted, &withheld, PARTS, 1, "maintain").await;
}
