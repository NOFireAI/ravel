//! Reachability proof for ADR-2509's per-statement floor mechanisms (issue
//! #2540): a real [`ravel_server::start`] server answers `POST /api/v1/sql`
//! over an unfolded logs tenant, and the requests the statement issues show
//! that each mechanism's effect reaches the shipping path, not only the crate
//! level.
//!
//! - The prefix listing drains its shards concurrently (decision 1).
//! - A missing catalog HEAD is cached across statements (decision 3).
//! - An audit event that finds the pipeline idle flushes at once instead of
//!   waiting `max_age` (decision 2, ADR-0062's idle-flush amendment).
//!
//! `MemoryStore` never yields, so concurrency is observed with `FaultStore`
//! holds, and every request is logged by a recording wrapper in front of the
//! `FaultStore`, so a held request counts as issued the moment it is sent.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Logs shards the server's catalog scans, each holding commit records.
const SHARDS: u32 = 4;
/// RLOG objects (and commit records) per shard, one per ingest hour.
const OBJECTS_PER_SHARD: u32 = 2;
/// Log records per RLOG object.
const RECORDS_PER_OBJECT: usize = 3;
const TOTAL_RECORDS: i64 = (SHARDS * OBJECTS_PER_SHARD) as i64 * RECORDS_PER_OBJECT as i64;
/// The statement window `[0 s, WINDOW_END_HOUR h]`. At [`SHARDS`] shards the
/// resolve estimates at least `4 * 201 = 804` suffix buckets, at or above the
/// default `prefix_list_crossover_requests` (720), so it takes the prefix path
/// with no crossover override. The assertion below fails the build if that
/// default moves above this fixture; it does not detect a change to how the
/// path is chosen.
const WINDOW_END_HOUR: i64 = 200;
const _: () = assert!(
    SHARDS as u64 * (WINDOW_END_HOUR as u64 + 1)
        >= ravel_catalog::DEFAULT_PREFIX_LIST_CROSSOVER_REQUESTS
);
const SQL: &str = "SELECT COUNT(*) FROM logs";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

/// The whole-shard commit prefix both listing paths LIST, starting at the
/// listing's first hour.
fn shard_prefix(shard: u32) -> String {
    keys::commit_shard_prefix(&tenant_hash(), Signal::Logs, shard).expect("shard prefix")
}

/// The logs catalog HEAD key (docs/catalog-and-mvcc.md key layout).
fn head_key() -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash().to_hex(),
        Signal::Logs.key_prefix()
    )
}

/// The audit data-object and commit-record prefixes of the tenant's query-audit
/// shard.
fn audit_data_marker() -> String {
    format!(
        "t/{}/{}/l0/",
        tenant_hash().to_hex(),
        Signal::Audit.key_prefix()
    )
}

fn audit_commit_prefix() -> String {
    keys::commit_shard_prefix(&tenant_hash(), Signal::Audit, QUERY_AUDIT_SHARD)
        .expect("audit commit prefix")
}

/// Publish one RLOG object of [`RECORDS_PER_OBJECT`] records in `(shard,
/// hour)` plus its `Signal::Logs` commit record, as the log shard actor does.
async fn publish_log_object(store: &dyn ObjectStoreBackend, shard: u32, hour: u32) {
    let tenant_hash = tenant_hash();
    let writer_id = Uuid::from_u128(u128::from(shard) * 100 + u128::from(hour) + 1);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let first_ts = i64::from(hour) * NS_PER_HOUR + i64::from(shard) * 1_000;
    for i in 0..RECORDS_PER_OBJECT {
        let ts = first_ts + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("shard {shard} hour {hour} record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let max_ts = first_ts + RECORDS_PER_OBJECT as i64 - 1;
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: RECORDS_PER_OBJECT as u64,
        series_count: 1,
        min_event_ts_ns: first_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: first_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: first_ts,
        ingest_hour_bucket: hour,
    })
    .expect("valid log commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
}

/// Every request that reached the store, as `(op, key or prefix)`, in arrival
/// order.
#[derive(Clone, Default)]
struct RequestLog(Arc<Mutex<Vec<(Op, String)>>>);

impl RequestLog {
    fn push(&self, op: Op, key: &str) {
        self.0.lock().unwrap().push((op, key.to_string()));
    }

    fn count(&self, op: Op, matches: impl Fn(&str) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(o, k)| *o == op && matches(k))
            .count()
    }

    /// Every logs commit-record LIST prefix issued so far.
    fn logs_commit_lists(&self) -> Vec<String> {
        let marker = format!("t/{}/l/c/", tenant_hash().to_hex());
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(o, k)| *o == Op::List && k.starts_with(&marker))
            .map(|(_, k)| k.clone())
            .collect()
    }

    fn head_gets(&self) -> usize {
        let head = head_key();
        self.count(Op::Get, |k| k == head)
    }

    /// `(audit data-object PUTs, audit commit-record PUTs)` so far.
    fn audit_puts(&self) -> (usize, usize) {
        let data = audit_data_marker();
        let commit = audit_commit_prefix();
        (
            self.count(Op::Put, |k| k.starts_with(&data)),
            self.count(Op::Put, |k| k.starts_with(&commit)),
        )
    }
}

/// Logs every call, then forwards it to the `FaultStore` underneath.
struct RecordingStore {
    inner: Arc<FaultStore<MemoryStore>>,
    log: RequestLog,
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.log.push(Op::Put, k);
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.log.push(Op::Get, k);
        self.inner.get(k, r).await
    }
    async fn put_multipart<'a>(
        &'a self,
        k: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.log.push(Op::Put, k);
        self.inner.put_multipart(k).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.log.push(Op::Head, k);
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.log.push(Op::List, p);
        self.inner.list(p, t).await
    }
    async fn list_after(
        &self,
        p: &str,
        start_after: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.log.push(Op::List, p);
        self.inner.list_after(p, start_after, t).await
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        self.log.push(Op::List, p);
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.log.push(Op::Delete, k);
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

struct Fixture {
    fault: Arc<FaultStore<MemoryStore>>,
    log: RequestLog,
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
}

/// An unfolded logs tenant ([`SHARDS`] shards, [`OBJECTS_PER_SHARD`] commit
/// records in each, no catalog HEAD) behind a recording `FaultStore`, served
/// by a real `ravel_server::start` in query mode with the background fold off
/// and `audit_pipeline` as its resolved `--audit-max-batch`/`--audit-max-age`.
async fn start_fixture(audit_pipeline: ravel_maintain::AuditPipelineConfig) -> Fixture {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    for shard in 0..SHARDS {
        for hour in 0..OBJECTS_PER_SHARD {
            publish_log_object(fault.as_ref(), shard, hour).await;
        }
    }
    assert!(
        matches!(
            fault.get(&head_key(), GetRange::Full).await,
            Err(StoreError::NotFound)
        ),
        "the fixture tenant must be unfolded: no catalog HEAD"
    );
    let log = RequestLog::default();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(RecordingStore {
        inner: fault.clone(),
        log: log.clone(),
    });
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let running = ravel_server::start(
        server_config(tokens, audit_pipeline),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    Fixture {
        fault,
        log,
        running,
        base,
        client: reqwest::Client::new(),
    }
}

fn server_config(
    tokens: HashMap<String, TenantId>,
    audit_pipeline: ravel_maintain::AuditPipelineConfig,
) -> ServerConfig {
    ServerConfig {
        audit_pipeline,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: SHARDS,
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

/// Spawn one `POST /api/v1/sql` of [`SQL`] over the window `[0, WINDOW_END_HOUR
/// h]`, returning its status and decoded body when it completes.
fn spawn_statement(
    fx: &Fixture,
) -> tokio::task::JoinHandle<(reqwest::StatusCode, serde_json::Value)> {
    let client = fx.client.clone();
    let url = format!("{}/api/v1/sql", fx.base);
    tokio::spawn(async move {
        let response = client
            .post(url)
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "query": SQL,
                    "start": 0.0,
                    "end": (WINDOW_END_HOUR * 3_600) as f64,
                })
                .to_string(),
            )
            .send()
            .await
            .expect("sql request sent");
        let status = response.status();
        let value: serde_json::Value = response.json().await.expect("sql response is JSON");
        (status, value)
    })
}

async fn run_statement(fx: &Fixture) -> (reqwest::StatusCode, serde_json::Value) {
    spawn_statement(fx).await.expect("statement task")
}

/// A 200 whose single row counts every fixture record.
fn assert_counts_every_record(status: reqwest::StatusCode, value: &serde_json::Value) {
    assert_eq!(status, 200, "statement must succeed: {value}");
    let rows = value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"));
    assert_eq!(rows.len(), 1, "COUNT(*) returns one row: {value}");
    assert_eq!(
        rows[0][0].as_i64(),
        Some(TOTAL_RECORDS),
        "every record in every shard must be counted: {value}"
    );
}

/// Wait until `issued` holds for the logs commit LISTs issued so far, or fail
/// after a bound that only a never-issued LIST reaches.
async fn wait_for_commit_lists(log: &RequestLog, issued: impl Fn(&[String]) -> bool, why: &str) {
    let reached = async {
        while !issued(&log.logs_commit_lists()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    if tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .is_err()
    {
        panic!("{why}: issued {:?}", log.logs_commit_lists());
    }
}

/// Wait until every prefix in `want` has been listed at least once.
async fn wait_for_lists(log: &RequestLog, want: &[String], why: &str) {
    wait_for_commit_lists(log, |issued| want.iter().all(|p| issued.contains(p)), why).await
}

/// Wait until at least `n` logs commit LISTs have been issued.
async fn wait_for_list_count(log: &RequestLog, n: usize, why: &str) {
    wait_for_commit_lists(log, |issued| issued.len() >= n, why).await
}

/// Decision 1: the prefix traversal lists the shards concurrently. Shard 0's
/// commit LIST is held, and shards 1 to 3 are still listed while it is held. A
/// sequential shard loop never issues them, and the wait fails.
///
/// The bounded (non-prefix) traversal issues the same LIST per shard, also
/// concurrently, so the request log cannot tell the two apart. The window
/// size ([`WINDOW_END_HOUR`]) is what selects the prefix path here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_prefix_listing_runs_shards_concurrently_through_http() {
    let fx = start_fixture(Default::default()).await;
    let gate = fx
        .fault
        .hold(Op::List, Some(shard_prefix(0)), Occurrence::Nth(1));

    let statement = spawn_statement(&fx);
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "only shard 0's first LIST is held: {held:?}");
    assert_eq!(held[0].2, shard_prefix(0));

    let others: Vec<String> = (1..SHARDS).map(shard_prefix).collect();
    wait_for_lists(
        &fx.log,
        &others,
        "shards 1 to 3 must be listed while shard 0's LIST is held",
    )
    .await;
    assert!(
        !statement.is_finished(),
        "the statement cannot finish while shard 0's LIST is held"
    );
    assert!(gate.release(held[0].0));

    let (status, value) = statement.await.expect("statement task");
    assert_counts_every_record(status, &value);

    let mut listed = fx.log.logs_commit_lists();
    listed.sort();
    let whole_shards: Vec<String> = (0..SHARDS).map(shard_prefix).collect();
    assert_eq!(
        listed, whole_shards,
        "the resolve must issue exactly one whole-shard LIST per shard"
    );
    drop(fx.running);
}

/// Decision 3: a missing catalog HEAD is cached on the resolve path, so two
/// sequential statements GET the HEAD key once between them. Without the
/// cached absence each statement repeats the GET.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_missing_catalog_head_is_read_once_through_http() {
    let fx = start_fixture(Default::default()).await;

    let (status, value) = run_statement(&fx).await;
    assert_counts_every_record(status, &value);
    assert_eq!(
        fx.log.head_gets(),
        1,
        "the first statement reads the missing HEAD once"
    );

    let (status, value) = run_statement(&fx).await;
    assert_counts_every_record(status, &value);
    assert_eq!(
        fx.log.head_gets(),
        1,
        "the second statement must use the cached absence, not GET {} again",
        head_key()
    );
    assert_eq!(
        fx.log.logs_commit_lists().len(),
        2 * SHARDS as usize,
        "a cached absence still lists the whole window on every statement"
    );
    drop(fx.running);
}

/// Decision 2 and ADR-0062's idle-flush amendment: with `max_age` at 10 s, a
/// statement that finds the audit pipeline idle returns well inside 5 s, after
/// exactly one audit data-object PUT and one audit commit-record PUT.
///
/// The second half is the control. On a fresh server, a second statement that
/// follows the first within `max_age` is not idle: the loop opens a full
/// `max_age` window for it, so it is still pending 5 s after the first
/// returned. The test does not order the second statement's audit submission
/// against the first one's held flush, so which idle condition refuses it is
/// not pinned here: at least the previous-event gap does, since the first
/// event was received less than `max_age` earlier. The in-flight-flush
/// condition has its own test in `ravel-maintain`. A loop that always waited
/// `max_age` would make the first statement look like the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_idle_audit_event_flushes_without_waiting_max_age() {
    let max_age = Duration::from_secs(10);
    let bound = Duration::from_secs(5);
    let audit = ravel_maintain::AuditPipelineConfig {
        max_age,
        ..Default::default()
    };

    let fx = start_fixture(audit.clone()).await;
    assert_eq!(fx.log.audit_puts(), (0, 0), "no audit write before a query");
    // hygiene-allow: wall-clock -- the server runs on real time and takes no
    // injected clock; the assertion is an upper bound two times below the
    // max_age deadline it distinguishes from, not a timing band.
    let started = std::time::Instant::now();
    let (status, value) = run_statement(&fx).await;
    let took = started.elapsed();
    assert_counts_every_record(status, &value);
    // hygiene-allow: wall-clock -- upper bound at half of max_age; see above.
    assert!(
        took < bound,
        "an idle audit event must not wait max_age ({max_age:?}): took {took:?}"
    );
    assert_eq!(
        fx.log.audit_puts(),
        (1, 1),
        "one audit data object and one audit commit record per statement"
    );
    drop(fx.running);

    // The control: the same configuration, with a statement that is not idle.
    let fx = start_fixture(audit).await;
    let gate = fx
        .fault
        .hold(Op::Put, Some(audit_data_marker()), Occurrence::Nth(1));
    let first = spawn_statement(&fx);
    gate.wait_until_held(1).await;
    let second = spawn_statement(&fx);
    wait_for_list_count(
        &fx.log,
        2 * SHARDS as usize,
        "the second statement must list while the first one's flush is held",
    )
    .await;
    let held = gate.held();
    assert_eq!(held.len(), 1, "only the first audit data PUT is held");
    assert!(gate.release(held[0]));
    let (status, value) = first.await.expect("first statement task");
    assert_counts_every_record(status, &value);

    let mut second = second;
    // hygiene-allow: wall-clock -- a lower bound: the second event's max_age
    // window opens when the loop receives it, after the held flush returns, and
    // tokio timers never fire early. A stall of `bound` or more between the
    // release above and this timeout would make it a false red.
    let pending = tokio::time::timeout(bound, &mut second).await;
    assert!(
        pending.is_err(),
        "a statement that follows another within max_age ({max_age:?}) is not \
         idle and must wait its window, but it returned within {bound:?}"
    );
    let (status, value) = second.await.expect("second statement task");
    assert_counts_every_record(status, &value);
    assert_eq!(
        fx.log.audit_puts(),
        (2, 2),
        "the two statements flush separately, one PUT pair each"
    );
    drop(fx.running);
}
