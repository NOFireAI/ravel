//! The projection of committed agent history into logs (ADR-3331 decision
//! 12) through a real `ravel_server::start`: a committed event is queryable
//! as a log line carrying its stable event id, a projection outage changes
//! no agent state, payloads are redacted unless the tenant opts in, a full
//! backlog stops reading without touching the coordinator, a process that
//! cannot project refuses to start, and a restart repeats records under the
//! ids they carried before.
//!
//! The outage is staged through `AgentsProjectionConfig::tap`, which runs
//! before every delivery with the event ids it carries and fails the attempt
//! while the test holds it down.

#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_ingest::{AdmissionLimits, RateLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::config::{
    AgentsClock, AgentsConfig, AgentsProjectionConfig, AgentsProjectionSwitch, ProjectionTap,
};
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{
    DevHeaderTenantResolver, FallbackResolver, Principal, StaticBearerTokenResolver,
};
use ravel_types::TenantId;
use reqwest::StatusCode;
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const SECRET: &str = "card4111secret";

/// A `MemoryStore` that counts the writes landing under an agent key
/// (`t/<tenant>/ag/...`), whoever issues them.
#[derive(Default)]
struct CountingStore {
    inner: MemoryStore,
    agent_writes: AtomicU64,
}

impl CountingStore {
    fn note(&self, key: &str) {
        if key.contains("/ag/") {
            self.agent_writes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn agent_writes(&self) -> u64 {
        self.agent_writes.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.note(key);
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.note(key);
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Every delivery attempt's event ids, and the ones let through.
#[derive(Default)]
struct Tap {
    down: AtomicBool,
    attempts: Mutex<Vec<String>>,
    delivered: Mutex<Vec<String>>,
}

impl Tap {
    fn hook(self: &Arc<Self>) -> ProjectionTap {
        let tap = self.clone();
        ProjectionTap(Some(Arc::new(move |ids: &[String]| {
            tap.attempts.lock().unwrap().extend(ids.iter().cloned());
            if tap.down.load(Ordering::SeqCst) {
                return Err("projection sink held down by the test".to_string());
            }
            tap.delivered.lock().unwrap().extend(ids.iter().cloned());
            Ok(())
        })))
    }

    fn delivered(&self) -> Vec<String> {
        self.delivered.lock().unwrap().clone()
    }

    async fn wait_delivered(&self, want: usize) -> Vec<String> {
        for _ in 0..2_000 {
            let delivered = self.delivered();
            if delivered.len() >= want {
                return delivered;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("never delivered {want}: {:?}", self.delivered());
    }
}

fn agents_config(projection: AgentsProjectionConfig) -> AgentsConfig {
    AgentsConfig {
        enabled: true,
        activation_concurrency: 16,
        poll_concurrency: 16,
        max_body_bytes: 16 * 1024,
        sweep_interval_ms: 0,
        projection,
        ..AgentsConfig::default()
    }
}

fn projecting(tap: &Arc<Tap>) -> AgentsProjectionConfig {
    AgentsProjectionConfig {
        retry_ms: 20,
        tap: tap.hook(),
        ..AgentsProjectionConfig::default()
    }
}

fn server_config(agents: AgentsConfig, mode: Mode) -> ServerConfig {
    let mut tokens = HashMap::new();
    for (token, agents, agents_worker) in [(CLIENT, true, false), (WORKER, false, true)] {
        tokens.insert(
            token.to_string(),
            Principal {
                agents,
                agents_worker,
                ..Principal::tenant_only(TenantId::new("acme"))
            },
        );
    }
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents,
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(200),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: Arc::new(StaticBearerTokenResolver::with_principals(tokens)),
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

async fn try_start(
    store: Arc<dyn ObjectStoreBackend>,
    agents: AgentsConfig,
    mode: Mode,
) -> anyhow::Result<ravel_server::Running> {
    try_start_with(store, server_config(agents, mode)).await
}

async fn try_start_with(
    store: Arc<dyn ObjectStoreBackend>,
    config: ServerConfig,
) -> anyhow::Result<ravel_server::Running> {
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
}

fn real_now_ns() -> i64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch");
    i64::try_from(since.as_nanos()).expect("fits i64")
}

const MINUTE_NS: i64 = 60_000_000_000;

/// An agents clock running at real time plus an offset the test moves, so a
/// turn can finish in the past and be delivered in the present.
#[derive(Clone, Default)]
struct Shifted(Arc<AtomicI64>);

impl Shifted {
    fn clock(&self) -> AgentsClock {
        let offset = self.0.clone();
        AgentsClock(Some(Arc::new(move || {
            real_now_ns() + offset.load(Ordering::SeqCst)
        })))
    }

    fn set(&self, offset_ns: i64) {
        self.0.store(offset_ns, Ordering::SeqCst);
    }
}

async fn start(store: Arc<dyn ObjectStoreBackend>, agents: AgentsConfig) -> ravel_server::Running {
    try_start(store, agents, Mode::All)
        .await
        .expect("server starts")
}

#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    base: String,
}

impl Api {
    fn new(running: &ravel_server::Running) -> Self {
        Api {
            http: reqwest::Client::new(),
            base: format!("http://{}", running.http_addr),
        }
    }

    async fn post(&self, token: &str, path: &str, body: &Value) -> Value {
        let response = self
            .http
            .post(format!("{}/api/v1/agents/{path}", self.base))
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .expect("request completes");
        let status = response.status();
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        body
    }

    async fn get(&self, token: &str, path_and_query: &str) -> Value {
        let response = self
            .http
            .get(format!("{}/api/v1/agents/{path_and_query}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .expect("request completes");
        let status = response.status();
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(status, StatusCode::OK, "{path_and_query}: {body}");
        body
    }

    async fn projection(&self) -> Value {
        self.get(CLIENT, "health").await["projection"].clone()
    }

    /// Poll the health route until `field` of its projection block satisfies
    /// `done`.
    async fn wait_projection(&self, field: &str, done: impl Fn(u64) -> bool) -> Value {
        for _ in 0..2_000 {
            let projection = self.projection().await;
            if projection[field].as_u64().is_some_and(&done) {
                return projection;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!(
            "projection {field} never reached its mark: {}",
            self.projection().await
        );
    }

    async fn scrape(&self) -> String {
        self.http
            .get(format!("{}/metrics", self.base))
            .send()
            .await
            .expect("scrape completes")
            .text()
            .await
            .expect("scrape body")
    }

    /// The number of log lines whose body matches `regex`, through PromQL on
    /// `/api/v1/query`.
    async fn log_lines(&self, regex: &str) -> u64 {
        let query = format!(r#"sum(count_over_time(ravel_log_lines{{__body__=~"{regex}"}}[1h]))"#);
        let response = self
            .http
            .get(format!("{}/api/v1/query", self.base))
            .bearer_auth(CLIENT)
            .query(&[("query", query.as_str())])
            .send()
            .await
            .expect("query completes");
        let status = response.status();
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        let result = body["data"]["result"].as_array().expect("vector result");
        match result.as_slice() {
            [] => 0,
            [sample] => sample["value"][1]
                .as_str()
                .and_then(|v| v.parse().ok())
                .expect("integer sample"),
            more => panic!("{query}: one sum expected, got {more:?}"),
        }
    }

    /// Poll until exactly `want` lines match `regex`; a line that arrives
    /// late is waited for, and a count that overshoots fails at once.
    async fn wait_log_lines(&self, regex: &str, want: u64) {
        let mut seen = 0;
        for _ in 0..300 {
            seen = self.log_lines(regex).await;
            assert!(seen <= want, "{regex}: {seen} lines, want {want}");
            if seen == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{regex}: {seen} lines, want {want}");
    }
}

fn pins() -> Value {
    json!([{"handler": "place", "version": "1.0.0", "digest": "01".repeat(32)}])
}

fn agent() -> Value {
    json!({"kind": "order", "key": "o-1"})
}

/// The `n` events a turn commits, each with a payload naming [`SECRET`].
fn events(turn: &str, n: usize) -> Value {
    (0..n)
        .map(|i| json!({"event_type": "paid", "payload": {"card": SECRET, "turn": turn, "i": i}}))
        .collect()
}

/// Create the agent and register a worker; the worker id.
async fn setup(api: &Api) -> String {
    api.post(CLIENT, "create", &json!({"agent": agent()})).await;
    let body = api
        .post(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await;
    body["worker_id"].as_str().expect("worker id").to_string()
}

/// Send one input, take its turn and commit it with `n` events; the
/// operation id.
async fn turn(api: &Api, worker: &str, request_id: &str, n: usize) -> String {
    commit(api, worker, request_id, events(request_id, n)).await
}

/// [`turn`], committing `events` as given.
async fn commit(api: &Api, worker: &str, request_id: &str, events: Value) -> String {
    let sent = api
        .post(
            CLIENT,
            "send",
            &json!({"agent": agent(), "handler": "place", "request_id": request_id, "payload": {}}),
        )
        .await;
    let polled = api
        .post(
            WORKER,
            "worker/poll",
            &json!({"worker_id": worker, "handlers": pins(), "wait_ms": 2_000}),
        )
        .await;
    let assignment = &polled["assignment"];
    assert_eq!(assignment["operation"], sent["operation"]);
    api.post(
        WORKER,
        "worker/commit",
        &json!({
            "lease": assignment["lease"],
            "outcome": "completed",
            "state": {"last": request_id},
            "output": {"done": request_id},
            "events": events,
            "timers": [],
            "intents": [],
        }),
    )
    .await;
    sent["operation"].as_str().expect("operation").to_string()
}

/// Every `ravel_agents_store_*` write sample the coordinator reports: puts,
/// CAS writes and conflicts, by phase.
fn coordinator_writes(scrape: &str) -> Vec<String> {
    scrape
        .lines()
        .filter(|line| {
            ["puts", "cas", "conflicts"]
                .iter()
                .any(|kind| line.starts_with(&format!("ravel_agents_store_{kind}_total{{")))
        })
        .map(str::to_string)
        .collect()
}

/// An operation's result, less its wall-clock finish time.
async fn result(api: &Api, operation: &str) -> Value {
    let mut result = api
        .get(CLIENT, &format!("result?operation={operation}"))
        .await["result"]
        .clone();
    result
        .as_object_mut()
        .expect("result object")
        .remove("finished_unix_ns")
        .expect("a finish time");
    result
}

/// What a run leaves behind that projection must not change.
#[derive(Debug, PartialEq)]
struct Observed {
    state: Value,
    first: Value,
    second: Value,
    coordinator_writes: Vec<String>,
    agent_writes: u64,
    delivered: Vec<String>,
}

/// Commit one turn of one event, check its log line, then commit a second
/// turn of two events, with the sink held down across the second when
/// `outage` is set.
async fn run(outage: bool) -> Observed {
    let store = Arc::new(CountingStore::default());
    let tap = Arc::new(Tap::default());
    let running = start(store.clone(), agents_config(projecting(&tap))).await;
    let api = Api::new(&running);
    let worker = setup(&api).await;

    let first = turn(&api, &worker, "r1", 1).await;
    let ids = tap.wait_delivered(1).await;
    assert_eq!(ids.len(), 1, "{ids:?}");
    let id = &ids[0];
    // The committed event is a log line under its stable id, carrying the
    // payload's hash and size and never its text.
    api.wait_log_lines(&format!(".*event_id={id} .*event_seq=1 .*type=paid .*"), 1)
        .await;
    api.wait_log_lines(
        &format!(".*event_id={id} .*payload_hash=[0-9a-f]+ payload_size=[0-9]+ payload=redacted"),
        1,
    )
    .await;
    assert_eq!(api.log_lines(&format!(".*{SECRET}.*")).await, 0);

    if outage {
        tap.down.store(true, Ordering::SeqCst);
    }
    let second = turn(&api, &worker, "r2", 2).await;
    if outage {
        let projection = api.wait_projection("sink_failures", |n| n >= 2).await;
        assert_eq!(projection["lag_max"], json!(2), "{projection}");
        assert_eq!(projection["events_projected"], json!(1), "{projection}");
        assert_eq!(tap.delivered().len(), 1);
    }
    let state = api.get(CLIENT, "state?kind=order&key=o-1").await;
    let first = result(&api, &first).await;
    let second = result(&api, &second).await;
    let coordinator_writes = coordinator_writes(&api.scrape().await);
    let agent_writes = store.agent_writes();

    tap.down.store(false, Ordering::SeqCst);
    let delivered = tap.wait_delivered(3).await;
    api.wait_projection("lag_max", |n| n == 0).await;
    // The missed events arrive once each, under the ids every failed attempt
    // carried.
    let attempts = tap.attempts.lock().unwrap().clone();
    for missed in &delivered[1..] {
        assert!(
            attempts.iter().filter(|a| *a == missed).count() >= if outage { 2 } else { 1 },
            "{missed}: {attempts:?}"
        );
        api.wait_log_lines(&format!(".*event_id={missed} .*"), 1)
            .await;
    }
    api.wait_log_lines(&format!(".*event_id={id} .*"), 1).await;
    for attempted in &attempts {
        assert!(delivered.contains(attempted), "{attempted} never delivered");
    }
    assert_eq!(
        store.agent_writes(),
        agent_writes,
        "projection wrote an agent key"
    );

    running.shutdown().await.expect("clean shutdown");
    Observed {
        state,
        first,
        second,
        coordinator_writes,
        agent_writes,
        delivered,
    }
}

/// ADR-3331 decision 12: "A projection failure never changes a committed
/// transition". The same two turns, with and without the sink held down
/// across the second, leave the same state, the same operation results, the
/// same coordinator writes and the same agent-key writes, and deliver the
/// same ids, each once, once the sink recovers.
#[tokio::test]
async fn committed_turn_is_queryable_as_a_log_with_its_stable_event_id_and_a_projection_outage_changes_no_state()
 {
    let clean = run(false).await;
    let outage = run(true).await;
    assert_eq!(clean.state["state"], json!({"last": "r2"}));
    assert_eq!(clean.second["output"], json!({"done": "r2"}));
    assert_eq!(
        clean
            .coordinator_writes
            .iter()
            .filter(|l| l.starts_with("ravel_agents_store_cas_total{")
                && l.contains("phase=\"publish\""))
            .count(),
        1,
        "{:?}",
        clean.coordinator_writes
    );
    assert_eq!(clean.delivered.len(), 3);
    assert_eq!(outage, clean);
}

/// Payloads are redacted by default; a tenant named by
/// `--agents-projection-payload-tenant` gets the payload text.
#[tokio::test]
async fn payload_text_is_projected_only_for_an_opted_in_tenant() {
    let tap = Arc::new(Tap::default());
    let running = start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig {
            payload_tenants: vec!["acme".to_string()],
            ..projecting(&tap)
        }),
    )
    .await;
    let api = Api::new(&running);
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;
    let ids = tap.wait_delivered(1).await;
    api.wait_log_lines(&format!(".*event_id={} .*payload=.*{SECRET}.*", ids[0]), 1)
        .await;
    assert_eq!(api.log_lines(".*payload=redacted.*").await, 0);
    running.shutdown().await.expect("clean shutdown");
}

/// A backlog at its bound stops reading and counts, and the coordinator
/// commits on with the same writes it makes when nothing is backed up.
#[tokio::test]
async fn a_full_backlog_counts_and_leaves_the_coordinator_untouched() {
    let mut writes = Vec::new();
    for backed_up in [false, true] {
        let tap = Arc::new(Tap::default());
        tap.down.store(backed_up, Ordering::SeqCst);
        let running = start(
            Arc::new(MemoryStore::new()),
            agents_config(AgentsProjectionConfig {
                backlog: 2,
                ..projecting(&tap)
            }),
        )
        .await;
        let api = Api::new(&running);
        let worker = setup(&api).await;
        turn(&api, &worker, "r1", 3).await;
        if backed_up {
            let projection = api.wait_projection("backlog_full", |n| n >= 1).await;
            assert_eq!(projection["backlog"], json!(2), "{projection}");
            assert_eq!(projection["backlog_bound"], json!(2), "{projection}");
        }
        turn(&api, &worker, "r2", 1).await;
        if backed_up {
            let projection = api.projection().await;
            assert_eq!(projection["lag_max"], json!(4), "{projection}");
            assert_eq!(projection["events_projected"], json!(0), "{projection}");
            tap.down.store(false, Ordering::SeqCst);
        }
        api.wait_projection("events_projected", |n| n == 4).await;
        api.wait_projection("lag_max", |n| n == 0).await;
        writes.push(coordinator_writes(&api.scrape().await));
        running.shutdown().await.expect("clean shutdown");
    }
    assert!(!writes[0].is_empty());
    assert_eq!(writes[0], writes[1]);
}

/// The one `mode="all"` sample of `name` in a scrape.
fn sample(scrape: &str, name: &str) -> u64 {
    let prefix = format!("{name}{{mode=\"all\"}} ");
    let lines: Vec<&str> = scrape
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .collect();
    assert_eq!(lines.len(), 1, "{name}: {lines:?}");
    lines[0]
        .strip_prefix(&prefix)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("{name}: {}", lines[0]))
}

/// The `ravel_agents_projection_*` family carries the health route's figures:
/// during an outage that fills the backlog, and after the sink recovers.
#[tokio::test]
async fn projection_metrics_scrape_matches_health_after_an_outage() {
    let tap = Arc::new(Tap::default());
    tap.down.store(true, Ordering::SeqCst);
    let running = start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig {
            backlog: 2,
            ..projecting(&tap)
        }),
    )
    .await;
    let api = Api::new(&running);
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 2).await;
    turn(&api, &worker, "r2", 1).await;
    api.wait_projection("backlog_full", |n| n >= 1).await;
    let scrape = api.scrape().await;
    for (name, want) in [
        ("ravel_agents_projection_lag_max", 3),
        ("ravel_agents_projection_backlog", 2),
        ("ravel_agents_projection_backlog_bound", 2),
        ("ravel_agents_projection_events_projected_total", 0),
        ("ravel_agents_projection_spans_projected_total", 0),
        ("ravel_agents_projection_read_failures_total", 0),
        ("ravel_agents_projection_records_rejected_total", 0),
    ] {
        assert_eq!(sample(&scrape, name), want, "{name} during the outage");
    }
    assert!(sample(&scrape, "ravel_agents_projection_sink_failures_total") >= 1);
    assert!(sample(&scrape, "ravel_agents_projection_backlog_full_total") >= 1);

    tap.down.store(false, Ordering::SeqCst);
    api.wait_projection("events_projected", |n| n == 3).await;
    api.wait_projection("lag_max", |n| n == 0).await;
    let health = api.projection().await;
    let scrape = api.scrape().await;
    for (name, field, want) in [
        ("ravel_agents_projection_lag_max", "lag_max", Some(0)),
        ("ravel_agents_projection_backlog", "backlog", Some(0)),
        (
            "ravel_agents_projection_backlog_bound",
            "backlog_bound",
            Some(2),
        ),
        (
            "ravel_agents_projection_events_projected_total",
            "events_projected",
            Some(3),
        ),
        (
            "ravel_agents_projection_spans_projected_total",
            "spans_projected",
            Some(2),
        ),
        (
            "ravel_agents_projection_read_failures_total",
            "read_failures",
            Some(0),
        ),
        (
            "ravel_agents_projection_records_rejected_total",
            "records_rejected",
            Some(0),
        ),
        (
            "ravel_agents_projection_sink_failures_total",
            "sink_failures",
            None,
        ),
        (
            "ravel_agents_projection_backlog_full_total",
            "backlog_full",
            None,
        ),
    ] {
        let value = sample(&scrape, name);
        assert_eq!(json!(value), health[field], "{name} against {health}");
        match want {
            Some(want) => assert_eq!(value, want, "{name}"),
            None => assert!(value >= 1, "{name}"),
        }
    }
    running.shutdown().await.expect("clean shutdown");
}

/// `--agents-projection off` starts and says so; a process with no ingest
/// buffer and no endpoint refuses to start, naming both ways out; with an
/// endpoint it starts and reports it.
#[tokio::test]
async fn a_process_that_cannot_project_refuses_to_start_and_off_is_reported() {
    let tap = Arc::new(Tap::default());
    let running = start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig {
            switch: AgentsProjectionSwitch::Off,
            ..projecting(&tap)
        }),
    )
    .await;
    let api = Api::new(&running);
    let projection = api.projection().await;
    assert_eq!(projection["mode"], json!("off"));
    assert_eq!(projection["endpoint"], json!(null));
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;
    assert_eq!(api.projection().await["events_projected"], json!(0));
    assert!(tap.attempts.lock().unwrap().is_empty());
    running.shutdown().await.expect("clean shutdown");

    let refused = try_start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig::default()),
        Mode::Query,
    )
    .await
    .err()
    .expect("query mode with no endpoint refuses to start");
    let message = format!("{refused:#}");
    assert!(
        message.contains("--agents-projection-endpoint"),
        "{message}"
    );
    assert!(message.contains("--agents-projection off"), "{message}");

    let running = try_start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig {
            endpoint: Some("http://127.0.0.1:4317".to_string()),
            ..AgentsProjectionConfig::default()
        }),
        Mode::Query,
    )
    .await
    .expect("query mode with an endpoint starts");
    let projection = Api::new(&running).projection().await;
    assert_eq!(projection["mode"], json!("endpoint"));
    assert_eq!(projection["endpoint"], json!("http://127.0.0.1:4317"));
    running.shutdown().await.expect("clean shutdown");

    for (switch, endpoint, names) in [
        (AgentsProjectionSwitch::Auto, "http://gateway:4317", "#3360"),
        (
            AgentsProjectionSwitch::Auto,
            "https://127.0.0.1:4317",
            "#3360",
        ),
        (
            AgentsProjectionSwitch::Off,
            "http://127.0.0.1:4317",
            "contradict",
        ),
    ] {
        let refused = try_start(
            Arc::new(MemoryStore::new()),
            agents_config(AgentsProjectionConfig {
                switch,
                endpoint: Some(endpoint.to_string()),
                ..AgentsProjectionConfig::default()
            }),
            Mode::Query,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{switch:?} with {endpoint} refuses to start"));
        let message = format!("{refused:#}");
        assert!(
            message.contains(names) && message.contains(endpoint),
            "{message}"
        );
    }
}

/// A query-mode process with `--agents` projects over OTLP gRPC to a
/// loopback gateway, and the committed event is a log line on that
/// gateway's query path, under the id the delivery carried. The two
/// processes share no store, so the line can only have arrived over OTLP.
#[tokio::test]
async fn a_query_mode_process_projects_to_a_loopback_gateway() {
    let mut gateway_config = server_config(AgentsConfig::default(), Mode::All);
    let mut tokens = HashMap::new();
    tokens.insert(
        CLIENT.to_string(),
        Principal::tenant_only(TenantId::new("acme")),
    );
    gateway_config.tenant_resolver = Arc::new(FallbackResolver::new(vec![
        Arc::new(StaticBearerTokenResolver::with_principals(tokens)),
        Arc::new(DevHeaderTenantResolver::default()),
    ]));
    let gateway = try_start_with(Arc::new(MemoryStore::new()), gateway_config)
        .await
        .expect("gateway starts");
    let grpc = gateway.grpc_addr.expect("the gateway listens for gRPC");
    let endpoint = format!("http://{grpc}");

    let tap = Arc::new(Tap::default());
    let running = try_start(
        Arc::new(MemoryStore::new()),
        agents_config(AgentsProjectionConfig {
            endpoint: Some(endpoint.clone()),
            ..projecting(&tap)
        }),
        Mode::Query,
    )
    .await
    .expect("query mode with a loopback endpoint starts");
    let api = Api::new(&running);
    assert_eq!(api.projection().await["endpoint"], json!(endpoint));
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;
    let ids = tap.wait_delivered(1).await;
    let projection = api.wait_projection("events_projected", |n| n == 1).await;
    assert_eq!(projection["lag_max"], json!(0), "{projection}");
    assert_eq!(projection["records_rejected"], json!(0), "{projection}");
    Api::new(&gateway)
        .wait_log_lines(&format!(".*event_id={} .*type=paid .*", ids[0]), 1)
        .await;

    running.shutdown().await.expect("clean shutdown");
    gateway.shutdown().await.expect("clean shutdown");
}

/// A restarted process projects the agent's history again as it touches the
/// agent, under the ids the first process used, so a query sees the
/// duplicate under one id and the stable id is the deduplication key.
#[tokio::test]
async fn a_restart_repeats_history_under_the_same_ids() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let before = Arc::new(Tap::default());
    let running = start(store.clone(), agents_config(projecting(&before))).await;
    let api = Api::new(&running);
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;
    let first = before.wait_delivered(1).await;
    api.wait_log_lines(&format!(".*event_id={} .*", first[0]), 1)
        .await;
    running.shutdown().await.expect("clean shutdown");

    let after = Arc::new(Tap::default());
    let running = start(store, agents_config(projecting(&after))).await;
    let api = Api::new(&running);
    let worker = api
        .post(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await["worker_id"]
        .as_str()
        .expect("worker id")
        .to_string();
    turn(&api, &worker, "r2", 1).await;
    let delivered = after.wait_delivered(2).await;
    assert_eq!(delivered[0], first[0], "{delivered:?}");
    assert_ne!(delivered[1], first[0]);
    api.wait_log_lines(&format!(".*event_id={} .*", first[0]), 2)
        .await;
    api.wait_log_lines(&format!(".*event_id={} .*", delivered[1]), 1)
        .await;
    running.shutdown().await.expect("clean shutdown");
}

/// One OTLP HTTP log export under its own `service.name`, at real time.
async fn post_log(api: &Api, service: &str) {
    let text = |s: &str| AnyValue {
        value: Some(AnyValueKind::StringValue(s.to_string())),
    };
    let ts = u64::try_from(real_now_ns()).expect("positive time");
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(text(service)),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: ts,
                    observed_time_unix_nano: ts,
                    body: Some(text("from another client")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let response = api
        .http
        .post(format!("{}/v1/logs", api.base))
        .bearer_auth(CLIENT)
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export completes");
    let status = response.status();
    assert_eq!(status, StatusCode::OK, "{}", response.text().await.unwrap());
}

/// A turn committed `age_ns` in the past by the agents clock, under a
/// server whose `--max-ingest-lag` is `max_ingest_lag`, delivered only once
/// the clock reads the present. Its log record and its turn span are older
/// than the bound: each is skipped and counted, the agent's lag clears, and
/// the next turn projects. Returns the health block once it has.
async fn an_old_turn_is_rejected_and_skipped(max_ingest_lag: Duration, age_ns: i64) -> Value {
    let shifted = Shifted::default();
    shifted.set(-age_ns);
    let tap = Arc::new(Tap::default());
    tap.down.store(true, Ordering::SeqCst);
    let mut config = server_config(
        AgentsConfig {
            clock: shifted.clock(),
            ..agents_config(projecting(&tap))
        },
        Mode::All,
    );
    config.max_ingest_lag = max_ingest_lag;
    let running = try_start_with(Arc::new(MemoryStore::new()), config)
        .await
        .expect("server starts");
    let api = Api::new(&running);
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;
    api.wait_projection("sink_failures", |n| n >= 1).await;
    let old = tap.attempts.lock().unwrap()[0].clone();

    shifted.set(0);
    tap.down.store(false, Ordering::SeqCst);
    let projection = api.wait_projection("records_rejected", |n| n >= 2).await;
    assert_eq!(projection["records_rejected"], json!(2), "{projection}");
    let projection = api.wait_projection("lag_max", |n| n == 0).await;
    assert_eq!(projection["events_projected"], json!(0), "{projection}");
    let reason = projection["last_rejection"].as_str().expect("a rejection");
    let lag_ns = max_ingest_lag.as_nanos();
    assert!(
        reason.contains(&format!("max ingest lag of {lag_ns} ns")),
        "{reason}"
    );

    turn(&api, &worker, "r2", 1).await;
    let projection = api.wait_projection("events_projected", |n| n >= 1).await;
    assert_eq!(projection["events_projected"], json!(1), "{projection}");
    assert_eq!(projection["records_rejected"], json!(2), "{projection}");
    let fresh = tap.delivered().last().expect("r2 delivered").clone();
    assert_ne!(fresh, old);
    api.wait_log_lines(&format!(".*event_id={fresh} .*"), 1)
        .await;
    assert_eq!(api.log_lines(&format!(".*event_id={old} .*")).await, 0);
    let projection = api.projection().await;
    running.shutdown().await.expect("clean shutdown");
    projection
}

/// A record carries its turn's finish time, never the delivery time, so a
/// turn that finished more than the default two-hour bound before an outage
/// ended is rejected for good instead of retried forever: counted, skipped,
/// and the agent's later turns still project.
#[tokio::test]
async fn a_turn_older_than_the_ingest_lag_after_an_outage_is_rejected_and_never_blocks_the_agent() {
    an_old_turn_is_rejected_and_skipped(ravel_server::DEFAULT_MAX_INGEST_LAG, 121 * MINUTE_NS)
        .await;
}

/// The in-process sink applies the process's configured `--max-ingest-lag`,
/// not the default: an eleven-minute-old turn the two-hour default admits is
/// rejected under a ten-minute bound.
#[tokio::test]
async fn the_configured_max_ingest_lag_is_the_bound_projection_meets() {
    an_old_turn_is_rejected_and_skipped(Duration::from_secs(600), 11 * MINUTE_NS).await;
}

/// A carried payload larger than the log body limit is cut to fit, the
/// record says so, and it is projected instead of rejected.
#[tokio::test]
async fn a_carried_payload_over_the_body_limit_is_truncated_and_projected() {
    let tap = Arc::new(Tap::default());
    let running = start(
        Arc::new(MemoryStore::new()),
        AgentsConfig {
            max_body_bytes: 256 * 1024,
            ..agents_config(AgentsProjectionConfig {
                payload_tenants: vec!["acme".to_string()],
                ..projecting(&tap)
            })
        },
    )
    .await;
    let api = Api::new(&running);
    let worker = setup(&api).await;
    let blob = "x".repeat(70_000);
    commit(
        &api,
        &worker,
        "r1",
        json!([{"event_type": "big", "payload": {"blob": blob}}]),
    )
    .await;
    let projection = api.wait_projection("events_projected", |n| n >= 1).await;
    assert_eq!(projection["events_projected"], json!(1), "{projection}");
    assert_eq!(projection["records_rejected"], json!(0), "{projection}");
    let id = &tap.delivered()[0];
    api.wait_log_lines(
        &format!(".*event_id={id} .*payload_truncated=true payload=.*xxxx.*"),
        1,
    )
    .await;
    running.shutdown().await.expect("clean shutdown");
}

/// A refusal a retry can get past is retried, never skipped: with the
/// tenant's stream-creation budget spent by another client, the projected
/// record fails delivery and is not counted rejected; once the budget
/// refills it is delivered once.
#[tokio::test]
async fn a_transient_admission_refusal_is_retried_not_skipped() {
    let shifted = Shifted::default();
    // Behind the time the other client spends the budget at, so the bucket
    // does not refill until the test moves the clock past it.
    shifted.set(-MINUTE_NS);
    let tap = Arc::new(Tap::default());
    let mut config = server_config(
        AgentsConfig {
            clock: shifted.clock(),
            ..agents_config(projecting(&tap))
        },
        Mode::All,
    );
    config.limits.tenants.insert(
        TenantId::new("acme"),
        AdmissionLimits {
            series_creation_rate: RateLimit::Bounded {
                per_sec: 1,
                burst: 1,
            },
            ..AdmissionLimits::default()
        },
    );
    let running = try_start_with(Arc::new(MemoryStore::new()), config)
        .await
        .expect("server starts");
    let api = Api::new(&running);
    post_log(&api, "another-client").await;
    let worker = setup(&api).await;
    turn(&api, &worker, "r1", 1).await;

    let projection = api.wait_projection("sink_failures", |n| n >= 3).await;
    assert_eq!(projection["events_projected"], json!(0), "{projection}");
    assert_eq!(projection["records_rejected"], json!(0), "{projection}");
    assert_eq!(projection["lag_max"], json!(1), "{projection}");
    let error = projection["last_sink_error"].as_str().expect("an error");
    assert!(error.contains("creation rate exceeded"), "{error}");

    shifted.set(5_000_000_000);
    let projection = api.wait_projection("events_projected", |n| n >= 1).await;
    assert_eq!(projection["events_projected"], json!(1), "{projection}");
    assert_eq!(projection["records_rejected"], json!(0), "{projection}");
    let id = &tap.delivered()[0];
    api.wait_log_lines(&format!(".*event_id={id} .*"), 1).await;
    running.shutdown().await.expect("clean shutdown");
}
