//! The agents timer, events, sweep and inspector routes (ADR-3331 decisions
//! 8, 9 and 11) through a real `ravel_server::start`.
//!
//! The agents clock is injected, so a timer comes due when a test advances
//! it, and `sweep_interval_ms` is 0, so the only sweep passes are the ones a
//! test asks for with `POST /api/v1/agents/sweep`. No worker polls in the
//! timer tests, so no poll-driven pass runs either. The interval sweep tests
//! set their own interval; it runs on the tokio timer, which the agents
//! clock does not drive, so they wait real time for it. The store is a
//! `FaultStore` over `MemoryStore` behind a wrapper that counts the agent
//! LISTs and HEAD reads, so a sweep's reported counts are checked against
//! the requests the store actually served.

#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_core::head::Head;
use ravel_agents_core::identity::agent_id;
use ravel_agents_core::keys::{AgentKey, AgentKind, head_key};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutMode, PutOptions, PutOutcome, StoreError,
};
use ravel_server::config::{AgentsClock, AgentsConfig};
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver};
use ravel_types::TenantId;
use reqwest::StatusCode;
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const ADMIN: &str = "admin-token";
const PLAIN: &str = "plain-token";

const TENANT: &str = "acme";

/// The injected clock's starting point: 2026-01-01T00:00:00Z.
const T0_NS: i64 = 1_767_225_600_000_000_000;
const SECOND_NS: i64 = 1_000_000_000;

/// The agent LISTs and HEAD reads the store served, by what they were for.
#[derive(Default)]
struct Requests {
    wake_lists: AtomicU64,
    dir_lists: AtomicU64,
    head_gets: AtomicU64,
}

impl Requests {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.wake_lists.load(Ordering::SeqCst),
            self.dir_lists.load(Ordering::SeqCst),
            self.head_gets.load(Ordering::SeqCst),
        )
    }
}

/// `FaultStore<MemoryStore>` with the agent request counts of [`Requests`].
struct CountingStore {
    inner: Arc<FaultStore<MemoryStore>>,
    requests: Arc<Requests>,
}

impl CountingStore {
    fn count_list(&self, prefix: &str) {
        if prefix.contains("/ag/_wake/") {
            self.requests.wake_lists.fetch_add(1, Ordering::SeqCst);
        } else if prefix.contains("/ag/_dir/") {
            self.requests.dir_lists.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if key.contains("/ag/") && key.ends_with("/head") {
            self.requests.head_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.count_list(prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.count_list(prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// One store a test can restart a server over, its fault layer, its request
/// counts and the clock every server over it reads.
struct Fixture {
    faults: Arc<FaultStore<MemoryStore>>,
    requests: Arc<Requests>,
    store: Arc<dyn ObjectStoreBackend>,
    now: Arc<AtomicI64>,
}

impl Fixture {
    fn new(plan: FaultPlan) -> Self {
        let faults = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let requests = Arc::new(Requests::default());
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(CountingStore {
            inner: faults.clone(),
            requests: requests.clone(),
        });
        Fixture {
            faults,
            requests,
            store,
            now: Arc::new(AtomicI64::new(T0_NS)),
        }
    }

    fn advance(&self, by_ns: i64) {
        self.now.fetch_add(by_ns, Ordering::SeqCst);
    }

    fn now(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }

    async fn start(&self) -> ravel_server::Running {
        self.start_with_interval(0).await
    }

    /// A server whose tenants run the periodic sweep every
    /// `sweep_interval_ms`.
    async fn start_with_interval(&self, sweep_interval_ms: u64) -> ravel_server::Running {
        let now = self.now.clone();
        start_server(
            self.store.clone(),
            AgentsClock(Some(Arc::new(move || now.load(Ordering::SeqCst)))),
            sweep_interval_ms,
        )
        .await
    }

    /// The agent's HEAD as the store holds it.
    async fn read_head(&self, key: &str) -> Head {
        let got = self
            .store
            .get(&self.head_key(key), GetRange::Full)
            .await
            .expect("HEAD present");
        Head::decode(&got.data).expect("HEAD decodes")
    }

    async fn write_head(&self, key: &str, head: &Head) {
        self.store
            .put(
                &self.head_key(key),
                Bytes::from(head.encode()),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await
            .expect("HEAD written");
    }

    fn head_key(&self, key: &str) -> String {
        let agent = agent_id(
            &AgentKind::new("order").expect("kind"),
            &AgentKey::new(key).expect("key"),
        );
        head_key(&TenantId::new(TENANT).hash(), &agent).expect("head key")
    }
}

fn agents_config(clock: AgentsClock, sweep_interval_ms: u64) -> AgentsConfig {
    AgentsConfig {
        enabled: true,
        activation_concurrency: 16,
        poll_concurrency: 16,
        sweep_interval_ms,
        clock,
        ..AgentsConfig::default()
    }
}

/// A client, a worker, an admin and a capability-less token, all for
/// tenant `acme`. The admin holds `agents_admin` only.
fn principals() -> Arc<dyn ravel_query::http::TenantResolver> {
    let mut tokens = HashMap::new();
    for (token, agents, agents_worker, agents_admin) in [
        (CLIENT, true, false, false),
        (WORKER, false, true, false),
        (ADMIN, false, false, true),
        (PLAIN, false, false, false),
    ] {
        tokens.insert(
            token.to_string(),
            Principal {
                agents,
                agents_worker,
                agents_admin,
                ..Principal::tenant_only(TenantId::new(TENANT))
            },
        );
    }
    Arc::new(StaticBearerTokenResolver::with_principals(tokens))
}

/// The `--mode all` process `agents_effects_e2e.rs` starts, with the agents
/// clock injected.
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    clock: AgentsClock,
    sweep_interval_ms: u64,
) -> ravel_server::Running {
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents: agents_config(clock, sweep_interval_ms),
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: principals(),
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
    };
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    base: String,
}

impl Api {
    fn public(running: &ravel_server::Running) -> Self {
        Api {
            http: reqwest::Client::new(),
            base: format!("http://{}", running.http_addr),
        }
    }

    async fn post(&self, token: &str, path: &str, body: &Value) -> (StatusCode, Value) {
        let response = self
            .http
            .post(format!("{}/api/v1/agents/{path}", self.base))
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .expect("request completes");
        read(response).await
    }

    async fn get(&self, token: &str, path_and_query: &str) -> (StatusCode, Value) {
        let response = self
            .http
            .get(format!("{}/api/v1/agents/{path_and_query}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .expect("request completes");
        read(response).await
    }

    async fn ok(&self, token: &str, path: &str, body: &Value) -> Value {
        let (status, body) = self.post(token, path, body).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        body
    }

    async fn ok_get(&self, token: &str, path_and_query: &str) -> Value {
        let (status, body) = self.get(token, path_and_query).await;
        assert_eq!(status, StatusCode::OK, "{path_and_query}: {body}");
        body
    }

    async fn sweep(&self) -> Value {
        self.ok(ADMIN, "sweep", &json!({})).await
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
}

async fn read(response: reqwest::Response) -> (StatusCode, Value) {
    let status = response.status();
    let text = response.text().await.expect("response body");
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, body)
}

fn agent(key: &str) -> Value {
    json!({"kind": "order", "key": key})
}

async fn create(api: &Api, key: &str) {
    api.ok(CLIENT, "create", &json!({"agent": agent(key)}))
        .await;
}

async fn schedule(api: &Api, key: &str, timer_key: &str, due_unix_ns: i64) -> Value {
    api.ok(
        CLIENT,
        "timers",
        &json!({
            "agent": agent(key),
            "timer_key": timer_key,
            "due_unix_ns": due_unix_ns.to_string(),
            "handler": "remind",
            "payload": {"timer_key": timer_key},
        }),
    )
    .await
}

async fn cancel(api: &Api, key: &str, timer_key: &str) -> Value {
    api.ok(
        CLIENT,
        "timers/cancel",
        &json!({"agent": agent(key), "timer_key": timer_key}),
    )
    .await
}

async fn state(api: &Api, key: &str) -> Value {
    api.ok_get(CLIENT, &format!("state?kind=order&key={key}"))
        .await
}

async fn timers(api: &Api, key: &str) -> Value {
    api.ok_get(CLIENT, &format!("timers?kind=order&key={key}"))
        .await
}

/// The `source` of every mailbox entry a fired timer put there.
fn timer_inputs(state: &Value) -> Vec<Value> {
    state["mailbox"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter(|e| e["source"]["kind"] == json!("timer"))
                .map(|e| e["source"].clone())
                .collect()
        })
        .unwrap_or_default()
}

/// The one sample of `name` carrying `label`, as an integer.
fn value(scrape: &str, name: &str, label: &str) -> u64 {
    let lines: Vec<&str> = scrape
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")) && line.contains(label))
        .collect();
    assert_eq!(lines.len(), 1, "{name} {label}: {lines:?}");
    lines[0]
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .expect("integer sample")
}

/// ADR-3331 decision 8: a due timer reaches its agent through the sweep
/// alone. The `_wake` hint PUT fails on every attempt, so nothing but the
/// HEAD records the timer; the coordinator that scheduled it is shut down,
/// and a fresh process over the same store finds the agent through `_dir`,
/// reads its HEAD and fires the timer once. A second pass fires nothing.
#[tokio::test]
async fn due_timer_is_delivered_once_across_a_coordinator_restart_with_no_hint() {
    let fixture = Fixture::new(
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("hint PUT refused".to_string()),
            )
            .with_key_contains("/_wake/")
            .with_occurrence(Occurrence::Always),
        ),
    );
    let first = fixture.start().await;
    let api = Api::public(&first);
    create(&api, "o-1").await;
    let due = fixture.now() + 60 * SECOND_NS;
    let scheduled = schedule(&api, "o-1", "remind", due).await;
    assert_eq!(scheduled["generation"], json!(1));
    assert_eq!(
        fixture.faults.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the hint PUT was refused"
    );
    first.shutdown().await.expect("first server stops");

    fixture.advance(120 * SECOND_NS);
    let second = fixture.start().await;
    let api = Api::public(&second);
    let before = fixture.requests.snapshot();
    let report = api.sweep().await;
    let after = fixture.requests.snapshot();
    let fired = timer_inputs(&state(&api, "o-1").await);
    assert_eq!(fired.len(), 1, "delivered once: {fired:?}");
    assert_eq!(fired[0]["timer"], scheduled["timer"]);
    assert_eq!(fired[0]["generation"], json!(1));
    assert_eq!(report["timers_fired"], json!(1), "{report}");
    assert_eq!(report["hints_read"], json!(0), "{report}");
    assert_eq!(report["wake_list_pages"], json!(1), "{report}");
    assert_eq!(report["dir_list_pages"], json!(1), "{report}");
    assert_eq!(report["head_gets"], json!(1), "{report}");
    assert_eq!(after.0 - before.0, 1, "wake LISTs the store served");
    assert_eq!(after.1 - before.1, 1, "dir LISTs the store served");
    assert_eq!(
        after.2 - before.2,
        2,
        "HEAD GETs: the pass's read and the fire CAS's"
    );

    let listed = timers(&api, "o-1").await;
    assert_eq!(listed["timers"][0]["status"], json!("delivered"));

    let again = api.sweep().await;
    assert_eq!(again["timers_fired"], json!(0), "{again}");
    assert_eq!(timer_inputs(&state(&api, "o-1").await).len(), 1);
    assert_eq!(
        fixture.faults.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "no later write went to _wake"
    );
    second.shutdown().await.expect("second server stops");
}

/// A cancel after the firing CAS answers `already_fired` and leaves the
/// input in the mailbox; a cancel before it answers `cancelled`, and the
/// sweep then enqueues nothing for it.
#[tokio::test]
async fn cancel_after_fire_is_already_fired_and_before_fire_is_cancelled() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    let due = fixture.now() + 60 * SECOND_NS;
    schedule(&api, "o-1", "fires", due).await;
    fixture.advance(120 * SECOND_NS);
    assert_eq!(api.sweep().await["timers_fired"], json!(1));
    assert_eq!(
        cancel(&api, "o-1", "fires").await,
        json!({"outcome": "already_fired"})
    );
    assert_eq!(timer_inputs(&state(&api, "o-1").await).len(), 1);

    let due = fixture.now() + 60 * SECOND_NS;
    schedule(&api, "o-1", "cancelled", due).await;
    assert_eq!(
        cancel(&api, "o-1", "cancelled").await,
        json!({"outcome": "cancelled"})
    );
    fixture.advance(120 * SECOND_NS);
    assert_eq!(api.sweep().await["timers_fired"], json!(0));
    assert_eq!(timer_inputs(&state(&api, "o-1").await).len(), 1);
    let listed = timers(&api, "o-1").await;
    let status_of = |timer_key: &str| {
        listed["timers"]
            .as_array()
            .expect("timers")
            .iter()
            .find(|t| t["timer_key"] == json!(timer_key))
            .map(|t| t["status"].clone())
    };
    assert_eq!(status_of("fires"), Some(json!("delivered")));
    assert_eq!(status_of("cancelled"), Some(json!("cancelled")));
    assert_eq!(
        cancel(&api, "o-1", "never").await,
        json!({"outcome": "unknown"})
    );
    running.shutdown().await.expect("server stops");
}

/// Scheduling a key again supersedes it: the new generation is the one that
/// fires, once, and the old one never does.
#[tokio::test]
async fn a_reschedule_supersedes_the_earlier_generation() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    let first = schedule(&api, "o-1", "remind", fixture.now() + 30 * SECOND_NS).await;
    let second = schedule(&api, "o-1", "remind", fixture.now() + 60 * SECOND_NS).await;
    assert_eq!(first["generation"], json!(1));
    assert_eq!(second["generation"], json!(2));
    assert_eq!(first["timer"], second["timer"]);
    let listed = timers(&api, "o-1").await;
    assert_eq!(listed["timers"].as_array().map(Vec::len), Some(1));
    assert_eq!(listed["timers"][0]["generation"], json!(2));

    fixture.advance(120 * SECOND_NS);
    assert_eq!(api.sweep().await["timers_fired"], json!(1));
    assert_eq!(api.sweep().await["timers_fired"], json!(0));
    let fired = timer_inputs(&state(&api, "o-1").await);
    assert_eq!(fired.len(), 1, "one message: {fired:?}");
    assert_eq!(fired[0]["generation"], json!(2));
    running.shutdown().await.expect("server stops");
}

fn pins() -> Value {
    json!([{"handler": "place", "version": "1.0.0", "digest": "01".repeat(32)}])
}

/// Send one input and commit its turn with `events` events. Answers the
/// operation handle.
async fn turn_with_events(api: &Api, worker_id: &str, request_id: &str, events: u64) -> String {
    let sent = api
        .ok(
            CLIENT,
            "send",
            &json!({"agent": agent("o-1"), "handler": "place", "request_id": request_id, "payload": {}}),
        )
        .await;
    let operation = sent["operation"].as_str().expect("operation").to_string();
    let polled = api
        .ok(
            WORKER,
            "worker/poll",
            &json!({"worker_id": worker_id, "handlers": pins(), "wait_ms": 2_000}),
        )
        .await;
    let assigned = &polled["assignment"];
    assert_eq!(assigned["operation"], json!(operation));
    let events: Vec<Value> = (1..=events)
        .map(|n| json!({"event_type": "progress", "payload": {"request": request_id, "n": n}}))
        .collect();
    api.ok(
        WORKER,
        "worker/commit",
        &json!({
            "lease": assigned["lease"],
            "outcome": "completed",
            "state": {"request": request_id},
            "output": {},
            "events": events,
            "timers": [],
            "intents": [],
        }),
    )
    .await;
    operation
}

async fn events_page(
    api: &Api,
    cursor: Option<&str>,
    limit: u32,
    operation: Option<&str>,
) -> (StatusCode, Value) {
    let mut body = json!({"agent": agent("o-1"), "cursor": cursor, "limit": limit});
    if let Some(operation) = operation {
        body["operation"] = json!(operation);
    }
    api.post(CLIENT, "events", &body).await
}

fn seqs(page: &Value) -> Vec<u64> {
    page["events"]
        .as_array()
        .expect("events")
        .iter()
        .map(|e| e["event_seq"].as_u64().expect("event_seq"))
        .collect()
}

/// Read every page after `cursor`, `limit` at a time, until a page comes
/// back empty or terminal. Answers each page's seqs and the last page.
async fn drain(
    api: &Api,
    mut cursor: Option<String>,
    limit: u32,
    operation: Option<&str>,
) -> (Vec<Vec<u64>>, Value) {
    let mut pages = Vec::new();
    loop {
        let (status, page) = events_page(api, cursor.as_deref(), limit, operation).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let got = seqs(&page);
        let done = got.is_empty() || page.get("terminal").is_some();
        if !got.is_empty() {
            pages.push(got);
        }
        cursor = Some(page["next_cursor"].as_str().expect("cursor").to_string());
        if done {
            return (pages, page);
        }
        assert!(pages.len() < 16, "the cursor stopped advancing");
    }
}

/// Events resume from a cursor page by page, the operation filter keeps
/// only its turns' events and marks the page that drains them terminal, a
/// cursor below the retained window is 410 `cursor_expired` naming the
/// oldest cursor, and a limit of 0 is 422.
#[tokio::test]
async fn events_resume_from_a_cursor_to_terminal_and_expire_below_the_window() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    let registered = api
        .ok(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await;
    let worker_id = registered["worker_id"].as_str().expect("worker id");
    let op1 = turn_with_events(&api, worker_id, "req-1", 3).await;
    let op2 = turn_with_events(&api, worker_id, "req-2", 2).await;

    let (pages, last) = drain(&api, None, 2, None).await;
    assert_eq!(pages, vec![vec![1, 2], vec![3, 4], vec![5]]);
    assert_eq!(last["next_cursor"], json!("5"));
    assert!(last.get("terminal").is_none(), "{last}");

    let (pages, last) = drain(&api, None, 2, Some(&op1)).await;
    assert_eq!(pages, vec![vec![1, 2], vec![3]]);
    assert_eq!(last["terminal"], json!("completed"));
    let (pages, last) = drain(&api, Some("3".to_string()), 1, Some(&op2)).await;
    assert_eq!(pages, vec![vec![4], vec![5]]);
    assert_eq!(last["terminal"], json!("completed"));
    let (status, first) = events_page(&api, None, 1, Some(&op2)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(seqs(&first), vec![4]);
    assert!(first.get("terminal").is_none(), "{first}");

    // Retain only events 3 onward: cursor "2" still reads them, "1" does
    // not.
    let mut head = fixture.read_head("o-1").await;
    head.events.retain(|e| e.event_seq >= 3);
    fixture.write_head("o-1", &head).await;
    let (pages, _) = drain(&api, Some("2".to_string()), 10, None).await;
    assert_eq!(pages, vec![vec![3, 4, 5]]);
    for cursor in ["0", "1"] {
        let (status, body) = events_page(&api, Some(cursor), 10, None).await;
        assert_eq!(status, StatusCode::GONE, "{body}");
        assert_eq!(body["code"], json!("cursor_expired"));
        assert_eq!(body["cursor"], json!(cursor));
        assert_eq!(body["oldest_cursor"], json!("2"));
        assert_eq!(body["restart"], json!("restart_from_oldest_cursor"));
    }

    let (status, body) = events_page(&api, None, 0, None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    running.shutdown().await.expect("server stops");
}

/// `effects/{effect}` lists at most the latest 64 attempts. The effect's
/// attempt counter is raised to 70 in HEAD directly, since driving 70 real
/// attempts would take 70 lease expiries.
#[tokio::test]
async fn effect_inspect_lists_at_most_the_latest_64_attempts() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    api.ok(
        CLIENT,
        "send",
        &json!({"agent": agent("o-1"), "handler": "place", "request_id": "req-1", "payload": {}}),
    )
    .await;
    let registered = api
        .ok(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await;
    let polled = api
        .ok(
            WORKER,
            "worker/poll",
            &json!({"worker_id": registered["worker_id"], "handlers": pins(), "wait_ms": 2_000}),
        )
        .await;
    let stepped = api
        .ok(
            WORKER,
            "worker/step",
            &json!({
                "lease": polled["assignment"]["lease"],
                "step_key": "price",
                "tool_name": "catalog.price",
                "tool_version": "1",
                "policy": "at_least_once",
                "input_hash": "ab".repeat(32),
            }),
        )
        .await;
    let effect = stepped["effect"].as_str().expect("effect").to_string();

    let mut head = fixture.read_head("o-1").await;
    assert_eq!(head.effects.len(), 1);
    head.effects[0].attempt = 70;
    fixture.write_head("o-1", &head).await;
    let inspected = api.ok_get(CLIENT, &format!("effects/{effect}")).await;
    let attempts: Vec<u64> = inspected["attempts"]
        .as_array()
        .expect("attempts")
        .iter()
        .map(|a| a["attempt"].as_u64().expect("attempt"))
        .collect();
    assert_eq!(attempts, (7..=70).collect::<Vec<u64>>());
    assert_eq!(inspected["attempt"], json!(70));
    running.shutdown().await.expect("server stops");
}

/// `POST /api/v1/agents/sweep` needs `agents_admin`, and the counts it
/// answers are the requests the store served for it and the deltas of the
/// sweep families on `/metrics`.
#[tokio::test]
async fn sweep_route_counts_match_the_store_and_needs_agents_admin() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    for token in [CLIENT, WORKER, PLAIN] {
        let (status, body) = api.post(token, "sweep", &json!({})).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token}: {body}");
    }
    for key in ["o-1", "o-2"] {
        create(&api, key).await;
        schedule(&api, key, "remind", fixture.now() + 60 * SECOND_NS).await;
    }
    fixture.advance(120 * SECOND_NS);

    let scrape = api.scrape().await;
    assert_eq!(value(&scrape, "ravel_agents_sweep_interval_ms", "mode="), 0);
    let passes = value(&scrape, "ravel_agents_sweep_passes_total", "mode=");
    let cycles = value(
        &scrape,
        "ravel_agents_sweep_directory_cycles_total",
        "mode=",
    );
    let before = fixture.requests.snapshot();
    let report = api.sweep().await;
    let after = fixture.requests.snapshot();
    assert_eq!(report["timers_fired"], json!(2), "{report}");
    assert_eq!(report["hints_read"], json!(2), "{report}");
    assert_eq!(
        report["wake_list_pages"],
        json!(after.0 - before.0),
        "{report}"
    );
    assert_eq!(
        report["dir_list_pages"],
        json!(after.1 - before.1),
        "{report}"
    );
    assert_eq!(report["head_gets"], json!(2), "{report}");
    assert_eq!(
        after.2 - before.2,
        4,
        "the pass's reads and the fire CASes'"
    );

    let scrape = api.scrape().await;
    let sample = |name: &str| value(&scrape, name, "mode=");
    assert_eq!(sample("ravel_agents_sweep_passes_total"), passes + 1);
    // Both agents fit one pass, so the pass walked the whole directory and
    // wrapped; the agents clock did not move during it.
    assert_eq!(
        sample("ravel_agents_sweep_directory_cycles_total"),
        cycles + 1
    );
    assert_eq!(sample("ravel_agents_sweep_last_directory_cycle_ms"), 0);
    assert!(scrape.contains("# TYPE ravel_agents_sweep_directory_cycles_total counter\n"));
    assert!(scrape.contains("# TYPE ravel_agents_sweep_last_directory_cycle_ms gauge\n"));
    assert_eq!(
        json!(sample("ravel_agents_sweep_timers_fired_total")),
        report["timers_fired"]
    );
    assert_eq!(
        json!(sample("ravel_agents_sweep_hints_read_total")),
        report["hints_read"]
    );
    assert_eq!(
        json!(sample("ravel_agents_sweep_head_gets_total")),
        report["head_gets"]
    );
    assert_eq!(
        json!(value(
            &scrape,
            "ravel_agents_sweep_list_pages_total",
            "phase=\"wake\""
        )),
        report["wake_list_pages"]
    );
    assert_eq!(
        json!(value(
            &scrape,
            "ravel_agents_sweep_list_pages_total",
            "phase=\"dir\""
        )),
        report["dir_list_pages"]
    );
    running.shutdown().await.expect("server stops");
}

const SWEEP_INTERVAL_MS: u64 = 50;

fn passes(scrape: &str) -> u64 {
    value(scrape, "ravel_agents_sweep_passes_total", "mode=")
}

/// The periodic sweep runs with no worker poll and no `sweep` call: once
/// the tenant is served, each interval runs one pass, which lists `_wake/`.
/// Waits on `/metrics` up to a deadline, so a slow
/// box only makes it wait longer.
#[tokio::test]
async fn the_interval_sweep_runs_a_pass_per_interval_with_no_poll() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start_with_interval(SWEEP_INTERVAL_MS).await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    let scrape = api.scrape().await;
    assert_eq!(
        value(&scrape, "ravel_agents_sweep_interval_ms", "mode="),
        SWEEP_INTERVAL_MS
    );
    let first = passes(&scrape);
    let lists_before = fixture.requests.snapshot().0;
    let started = std::time::Instant::now();
    let deadline = started + Duration::from_secs(30);
    let mut last = first;
    while last < first + 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(SWEEP_INTERVAL_MS)).await;
        last = passes(&api.scrape().await);
    }
    assert!(last >= first + 3, "{} passes in 30 s", last - first);
    assert!(
        fixture.requests.snapshot().0 >= lists_before + 3,
        "each pass lists _wake/"
    );
    running.shutdown().await.expect("server stops");
}

/// `sweep_interval_ms` 0 runs no periodic pass. Worker polls still sweep,
/// at most once per [`ravel_server::agents::POLL_SWEEP_FLOOR_MS`]
/// of the agents clock: two idle polls in a row run one pass, and a poll
/// after the clock moved past the floor runs one more.
#[tokio::test]
async fn interval_zero_runs_no_periodic_pass_and_polls_sweep_at_the_floor() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    create(&api, "o-1").await;
    tokio::time::sleep(Duration::from_millis(10 * SWEEP_INTERVAL_MS)).await;
    let scrape = api.scrape().await;
    assert_eq!(value(&scrape, "ravel_agents_sweep_interval_ms", "mode="), 0);
    assert_eq!(passes(&scrape), 0, "a periodic pass ran");

    let registered = api
        .ok(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await;
    let idle_poll = || async {
        let polled = api
            .ok(
                WORKER,
                "worker/poll",
                &json!({"worker_id": registered["worker_id"], "handlers": pins(), "wait_ms": 0}),
            )
            .await;
        assert_eq!(polled["assignment"], Value::Null, "{polled}");
    };
    idle_poll().await;
    idle_poll().await;
    assert_eq!(passes(&api.scrape().await), 1);
    fixture.advance(
        i64::try_from(ravel_server::agents::POLL_SWEEP_FLOOR_MS).expect("floor") * 1_000_000,
    );
    idle_poll().await;
    assert_eq!(passes(&api.scrape().await), 2);
    running.shutdown().await.expect("server stops");
}

/// The inspector page over a real listener: HTML for an `agents` token,
/// refused without one, and carrying every section the page renders.
#[tokio::test]
async fn inspector_is_served_to_agents_tokens_with_every_section() {
    let fixture = Fixture::new(FaultPlan::empty());
    let running = fixture.start().await;
    let api = Api::public(&running);
    let url = format!("{}/api/v1/agents/inspector", api.base);

    let anonymous = api.http.get(&url).send().await.expect("request completes");
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    for token in [WORKER, ADMIN, PLAIN] {
        let (status, _) = api.get(token, "inspector").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token}");
    }

    let page = api
        .http
        .get(&url)
        .bearer_auth(CLIENT)
        .send()
        .await
        .expect("request completes");
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(page.headers()["content-type"], "text/html; charset=utf-8");
    let csp = page.headers()["content-security-policy"]
        .to_str()
        .expect("ascii policy")
        .to_string();
    assert!(csp.contains("default-src 'none'"), "{csp}");
    let html = page.text().await.expect("page body");
    for id in [
        "inspector-form",
        "inspector-token",
        "inspector-kind",
        "inspector-key",
        "inspector-error",
        "inspector-blocked",
        "inspector-state",
        "inspector-active-turn",
        "inspector-mailbox",
        "inspector-timers",
        "inspector-steps",
        "inspector-effects",
        "inspector-events",
    ] {
        assert_eq!(
            html.matches(&format!("id=\"{id}\"")).count(),
            1,
            "marker {id}"
        );
    }
    running.shutdown().await.expect("server stops");
}
