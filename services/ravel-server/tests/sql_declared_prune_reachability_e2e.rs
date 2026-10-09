//! Reachability proof for ADR-2677 decision 5: declared-column pruning reaches
//! `POST /api/v1/sql` on a real [`ravel_server::start`] server, not only
//! `SqlExecutor::execute` (`crates/ravel-sql/src/logs_provider.rs`'s
//! `a_segment_whose_stats_exclude_the_predicate_is_never_fetched`).
//!
//! The tenant's durable config declares `UserID`, `CounterID` and `EventDate`
//! as `I64` columns before the server starts, so the ingest path stamps every
//! flush's commit record with their extrema and the SQL path resolves the
//! same declaration. Every object is written by an OTLP export through
//! `POST /v1/logs` in strict mode, one export per object, and every request
//! the server issues passes a recording wrapper in front of the store. The
//! read cache is disabled, so a statement that needs an object GETs it and
//! the per-key GET count is exact.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// The ingest writer's default `RlogConfig::block_target_records`.
const BLOCK_ROWS: i64 = 8_192;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// The GET keys the server issued, in order.
#[derive(Clone, Default)]
struct GetLog(Arc<Mutex<Vec<String>>>);

impl GetLog {
    fn len(&self) -> usize {
        self.0.lock().expect("lock").len()
    }

    /// GETs of `key` issued after the first `since` entries.
    fn gets_of_since(&self, key: &str, since: usize) -> usize {
        self.0.lock().expect("lock")[since..]
            .iter()
            .filter(|k| k.as_str() == key)
            .count()
    }
}

struct RecordingStore {
    inner: Arc<MemoryStore>,
    gets: GetLog,
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.0.lock().expect("lock").push(k.to_string());
        self.inner.get(k, r).await
    }
    async fn put_multipart<'a>(
        &'a self,
        k: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(k).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(p, t).await
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(p, start_after, t).await
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// One log row's declared attributes.
#[derive(Clone, Copy)]
struct Row {
    user_id: i64,
    counter_id: i64,
    event_date: i64,
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

fn export_request(rows: &[Row], base_ts_ns: i64) -> ExportLogsServiceRequest {
    let log_records = rows
        .iter()
        .zip(0i64..)
        .map(|(row, i)| LogRecord {
            time_unix_nano: (base_ts_ns + i) as u64,
            observed_time_unix_nano: (base_ts_ns + i) as u64,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue("hit".to_string())),
            }),
            attributes: vec![
                int_kv("UserID", row.user_id),
                int_kv("CounterID", row.counter_id),
                int_kv("EventDate", row.event_date),
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

struct Fixture {
    memory: Arc<MemoryStore>,
    gets: GetLog,
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
        let declared = ["UserID", "CounterID", "EventDate"]
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

        let gets = GetLog::default();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(RecordingStore {
            inner: Arc::clone(&memory),
            gets: gets.clone(),
        });
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
            gets,
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

    /// Export `rows` as one strict-mode OTLP request and return the key of the
    /// one data object its flush wrote.
    async fn ingest(&mut self, rows: &[Row]) -> String {
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
        assert!(
            response.headers().get("x-ravel-commit-token").is_some(),
            "a strict export acks after its commit"
        );
        let after = self.data_keys().await;
        let new: Vec<String> = after.difference(&before).cloned().collect();
        assert_eq!(new.len(), 1, "one export, one data object: {new:?}");
        new.into_iter().next().expect("one key")
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

/// The `UserID` column of a JSON SQL response, in row order.
fn user_ids(value: &serde_json::Value) -> Vec<i64> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows: {value}"))
        .iter()
        .map(|row| row[0].as_i64().unwrap_or_else(|| panic!("UserID: {value}")))
        .collect()
}

fn pruning(value: &serde_json::Value, key: &str) -> u64 {
    value["stats"]["pruning"][key]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.pruning.{key}: {value}"))
}

/// The timing fields present for every statement, and the three that are
/// present only when the plan holds exactly one logs scan.
const TIMINGS_ALWAYS: [&str; 11] = [
    "attempts",
    "resolveMs",
    "planMs",
    "startMs",
    "firstBatchMs",
    "drainMs",
    "auditMs",
    "scans",
    "planningWaitMaxMs",
    "openMaxMs",
    "decodeBuildMaxMs",
];
const TIMINGS_SINGLE_SCAN: [&str; 3] = ["planInitMs", "firstBatchMinMs", "streamMaxMs"];

/// `stats.timings.scans`, after checking the object's key set is exactly the
/// one that scan count renders.
fn scans(value: &serde_json::Value) -> u64 {
    let timings = value["stats"]["timings"]
        .as_object()
        .unwrap_or_else(|| panic!("stats.timings: {value}"));
    let scans = timings["scans"]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.timings.scans: {value}"));
    let mut expected: BTreeSet<&str> = TIMINGS_ALWAYS.into_iter().collect();
    if scans == 1 {
        expected.extend(TIMINGS_SINGLE_SCAN);
    }
    let keys: BTreeSet<&str> = timings.keys().map(String::as_str).collect();
    assert_eq!(keys, expected, "stats.timings for scans = {scans}: {value}");
    scans
}

fn rows(user_ids: std::ops::Range<i64>, counter_id: i64, event_date: i64) -> Vec<Row> {
    user_ids
        .map(|user_id| Row {
            user_id,
            counter_id,
            event_date: event_date + user_id % 3,
        })
        .collect()
}

/// Ingest objects `a` to `d` as laid out on
/// [`declared_column_equality_prunes_segments_before_fetch_through_http`],
/// returning their keys in that order.
async fn ingest_four(fx: &mut Fixture) -> [String; 4] {
    let a = fx.ingest(&rows(1..4, 62, 100)).await;
    let b = fx.ingest(&rows(10..13, 10, 100)).await;
    let c = fx.ingest(&rows(20..23, 62, 200)).await;
    let d = fx
        .ingest(&[
            Row {
                user_id: 30,
                counter_id: 62,
                event_date: 140,
            },
            Row {
                user_id: 31,
                counter_id: 63,
                event_date: 145,
            },
            Row {
                user_id: 32,
                counter_id: 62,
                event_date: 150,
            },
            Row {
                user_id: 33,
                counter_id: 62,
                event_date: 160,
            },
        ])
        .await;
    [a, b, c, d]
}

/// Four stamped objects, each laid out so its stamp alone decides whether a
/// predicate can match it:
///
/// - `a`: CounterID 62, EventDate 100..=102, UserID 1..=3 (matches the range
///   statement).
/// - `b`: CounterID 10, EventDate 100..=102, UserID 10..=12 (excluded by
///   `CounterID = 62`).
/// - `c`: CounterID 62, EventDate 200..=202, UserID 20..=22 (excluded by the
///   `EventDate` range).
/// - `d`: CounterID 62 and 63, EventDate 140..=160, UserID 30..=33 (straddles
///   the range end, so it is read and filtered row by row).
///
/// `CounterID = 62 AND EventDate >= 100 AND EventDate <= 150` returns `a`'s
/// three rows and `d`'s two `CounterID = 62` rows at or before 150, and fetches
/// neither `b` nor `c`; so does the `BETWEEN` spelling of the same statement. `UserID = 21` returns one row of `c` and fetches none
/// of `a`, `b`, `d`. Each of those plans holds one logs scan (`scans == 1`).
///
/// Flipped assertions: making `prune_segments_by_stats` keep every segment
/// (`if true || arms.is_empty()`) fetches `b` and fails the zero-GET
/// assertion. Counting the skip but scanning the unpruned list (the provider
/// building `LogsScanExec` over `segments.clone()`) fails the same assertion.
/// Publishing a zero for the counter while still skipping
/// (`with_segments_pruned_by_stats(0)` in the provider) leaves the GETs at zero
/// and fails the count alone (`0 != 2`).
#[tokio::test]
async fn declared_column_equality_prunes_segments_before_fetch_through_http() {
    let mut fx = Fixture::start().await;
    let [a, b, c, d] = ingest_four(&mut fx).await;

    let since = fx.gets.len();
    let value = fx
        .sql(
            "SELECT \"UserID\" FROM logs WHERE \"CounterID\" = 62 \
             AND \"EventDate\" >= 100 AND \"EventDate\" <= 150 ORDER BY \"UserID\"",
        )
        .await;
    assert_eq!(user_ids(&value), vec![1, 2, 3, 30, 32], "{value}");
    for (name, key) in [("b", &b), ("c", &c)] {
        assert_eq!(
            fx.gets.gets_of_since(key, since),
            0,
            "{name}'s stamp excludes the predicate, so it is never fetched"
        );
    }
    for (name, key) in [("a", &a), ("d", &d)] {
        assert!(
            fx.gets.gets_of_since(key, since) > 0,
            "{name} can match and must be read"
        );
    }
    assert_eq!(pruning(&value, "segments"), 4, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByStats"), 2, "{value}");
    assert_eq!(scans(&value), 1, "{value}");

    // The same statement spelled with `BETWEEN`, as ADR-2677 decision 5
    // writes it.
    let since = fx.gets.len();
    let value = fx
        .sql(
            "SELECT \"UserID\" FROM logs WHERE \"CounterID\" = 62 \
             AND \"EventDate\" BETWEEN 100 AND 150 ORDER BY \"UserID\"",
        )
        .await;
    assert_eq!(user_ids(&value), vec![1, 2, 3, 30, 32], "{value}");
    for key in [&b, &c] {
        assert_eq!(fx.gets.gets_of_since(key, since), 0, "{key} never fetched");
    }
    assert_eq!(pruning(&value, "segmentsPrunedByStats"), 2, "{value}");
    assert_eq!(scans(&value), 1, "{value}");

    let since = fx.gets.len();
    let value = fx
        .sql("SELECT \"UserID\" FROM logs WHERE \"UserID\" = 21")
        .await;
    assert_eq!(user_ids(&value), vec![21], "{value}");
    for (name, key) in [("a", &a), ("b", &b), ("d", &d)] {
        assert_eq!(
            fx.gets.gets_of_since(key, since),
            0,
            "{name}'s UserID stamp excludes 21, so it is never fetched"
        );
    }
    assert!(fx.gets.gets_of_since(&c, since) > 0, "c holds 21");
    assert_eq!(pruning(&value, "segments"), 4, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByStats"), 3, "{value}");
    assert_eq!(scans(&value), 1, "{value}");

    fx.running.shutdown().await.expect("graceful shutdown");
}

/// `UserID = 21 UNION ALL UserID = 2` over the same four objects: two branches
/// with different predicates, which the optimizer keeps as two logs scans.
/// The response reports `scans == 2`, omits `planInitMs`, `firstBatchMinMs`
/// and `streamMaxMs` while carrying every other timing field, and its pruning
/// counters are the two single-branch figures added together, so
/// `segmentsPrunedByStats` (3 + 3) exceeds `segments` (4).
///
/// Flipped assertions: rendering the three fields whatever `scans` says, and
/// counting every plan node rather than every `LogsScanExec`, each fail the
/// union's own assertions before either branch runs alone.
#[tokio::test]
async fn a_union_all_of_two_logs_branches_reports_two_scans_through_http() {
    let mut fx = Fixture::start().await;
    ingest_four(&mut fx).await;

    let value = fx
        .sql(
            "SELECT \"UserID\" FROM logs WHERE \"UserID\" = 21 \
             UNION ALL SELECT \"UserID\" FROM logs WHERE \"UserID\" = 2 \
             ORDER BY \"UserID\"",
        )
        .await;
    assert_eq!(user_ids(&value), vec![2, 21], "{value}");
    assert_eq!(scans(&value), 2, "{value}");
    let timings = &value["stats"]["timings"];
    for key in TIMINGS_SINGLE_SCAN {
        assert!(timings.get(key).is_none(), "{key} is omitted: {value}");
    }
    for key in TIMINGS_ALWAYS {
        assert!(timings[key].as_f64().is_some(), "{key} is present: {value}");
    }
    assert_eq!(pruning(&value, "segments"), 4, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByStats"), 6, "{value}");

    // Each branch alone: one scan each, and the union's pruning counters are
    // exactly their sum.
    let user_21 = fx
        .sql("SELECT \"UserID\" FROM logs WHERE \"UserID\" = 21")
        .await;
    assert_eq!(user_ids(&user_21), vec![21], "{user_21}");
    assert_eq!(pruning(&user_21, "segmentsPrunedByStats"), 3, "{user_21}");
    assert_eq!(scans(&user_21), 1, "{user_21}");
    let user_2 = fx
        .sql("SELECT \"UserID\" FROM logs WHERE \"UserID\" = 2")
        .await;
    assert_eq!(user_ids(&user_2), vec![2], "{user_2}");
    assert_eq!(pruning(&user_2, "segmentsPrunedByStats"), 3, "{user_2}");
    assert_eq!(scans(&user_2), 1, "{user_2}");
    for key in [
        "segmentsPrunedByStats",
        "blocksTotal",
        "blocksScanned",
        "blocksPrunedByPostings",
    ] {
        assert_eq!(
            pruning(&value, key),
            pruning(&user_21, key) + pruning(&user_2, key),
            "{key} sums the two scans: {value}"
        );
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}

/// One object of three blocks whose `UserID` stamp `[0, 2 * BLOCK_ROWS]`
/// admits `UserID = 5` but whose blocks partition that range: the object is
/// read, and the block-level `NumRange` skip reads only the first block.
///
/// Flipped assertions: handing `LogsScanExec` an empty prune list, so the
/// reader scans every block, fails `blocksScanned < blocksTotal`. A stamp skip
/// that fires on any bounded arm, whatever the stamp, skips this object and
/// fails the row assertion.
#[tokio::test]
async fn declared_column_block_level_range_pruning_reduces_blocks_scanned_through_http() {
    let mut fx = Fixture::start().await;
    let total = 2 * BLOCK_ROWS + 1;
    let key = fx.ingest(&rows(0..total, 62, 100)).await;

    let since = fx.gets.len();
    let value = fx
        .sql("SELECT \"UserID\" FROM logs WHERE \"UserID\" = 5")
        .await;
    assert_eq!(user_ids(&value), vec![5], "{value}");
    assert!(fx.gets.gets_of_since(&key, since) > 0, "the object is read");
    assert_eq!(pruning(&value, "segments"), 1, "{value}");
    assert_eq!(pruning(&value, "segmentsPrunedByStats"), 0, "{value}");
    let blocks_total = pruning(&value, "blocksTotal");
    let blocks_scanned = pruning(&value, "blocksScanned");
    assert_eq!(
        blocks_total, 3,
        "{total} rows in blocks of {BLOCK_ROWS}: {value}"
    );
    assert!(
        blocks_scanned < blocks_total,
        "the blocks whose UserID range excludes 5 are skipped: {value}"
    );
    assert_eq!(blocks_scanned, 1, "only the first block holds 5: {value}");
    assert_eq!(
        pruning(&value, "blocksPrunedByPostings"),
        0,
        "the skipped blocks are the NumRange skip, not POSTINGS: {value}"
    );
    assert_eq!(scans(&value), 1, "{value}");

    fx.running.shutdown().await.expect("graceful shutdown");
}
