//! The agents step, effect and reconcile routes (ADR-3331 decision 9,
//! milestone 3) through a real `ravel_server::start` over `MemoryStore`, with
//! the agents audit records read back from the tenant's audit shard.
//!
//! Each test drives one recovery policy or refusal end to end: a worker
//! journals a step, its lease expires, a second worker replays the turn, and
//! the first worker's result arrives late. Waits for a lease to expire are
//! sleeps; no assertion is about elapsed time.

#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_runtime::UNKNOWN_OUTCOME_RETRIES;
use ravel_commit::{keys, record};
use ravel_logseg::{AttrValue, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_server::config::AgentsConfig;
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver, Subject, SubjectSource};
use ravel_types::{Signal, TenantId};
use reqwest::StatusCode;
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const ADMIN: &str = "admin-token";
const PLAIN: &str = "plain-token";
/// Two more `agents_admin` tokens, each configured with a subject label.
const ADMIN_ALICE: &str = "admin-token-a";
const ADMIN_BOB: &str = "admin-token-b";

const LEASE_MS: u64 = 300;
const GRACE_MS: u64 = 100;
const BODY_LIMIT: usize = 16 * 1024;

const TENANT: &str = "acme";

fn agents_config() -> AgentsConfig {
    AgentsConfig {
        enabled: true,
        activation_concurrency: 16,
        poll_concurrency: 16,
        max_body_bytes: BODY_LIMIT,
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        ..AgentsConfig::default()
    }
}

/// A client, a worker, three admins and a capability-less token, all for
/// tenant `acme`. Each admin holds `agents_admin` only; `ADMIN` carries no
/// subject, `ADMIN_ALICE` and `ADMIN_BOB` carry the subject a
/// `subject=LABEL` token entry configures.
fn principals() -> Arc<dyn ravel_query::http::TenantResolver> {
    let mut tokens = HashMap::new();
    for (token, agents, agents_worker, agents_admin, subject) in [
        (CLIENT, true, false, false, None),
        (WORKER, false, true, false, None),
        (ADMIN, false, false, true, None),
        (ADMIN_ALICE, false, false, true, Some("alice")),
        (ADMIN_BOB, false, false, true, Some("bob@example.com")),
        (PLAIN, false, false, false, None),
    ] {
        tokens.insert(
            token.to_string(),
            Principal {
                agents,
                agents_worker,
                agents_admin,
                subject: subject.map(|label| {
                    Subject::new(SubjectSource::StaticToken, label).expect("valid label")
                }),
                ..Principal::tenant_only(TenantId::new(TENANT))
            },
        );
    }
    Arc::new(StaticBearerTokenResolver::with_principals(tokens))
}

/// The `--mode all` process `agents_e2e.rs` starts, over `store`, with the
/// audit pipeline installed in required mode.
async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    start_server_with(store, agents_config()).await
}

async fn start_server_with(
    store: Arc<dyn ObjectStoreBackend>,
    agents: AgentsConfig,
) -> ravel_server::Running {
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents,
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

    /// [`Api::ok`], also sending a client-chosen subject in
    /// `x-ravel-subject` and in the dev mTLS header, neither of which this
    /// server reads.
    async fn ok_claiming_mallory(&self, token: &str, path: &str, body: &Value) -> Value {
        let response = self
            .http
            .post(format!("{}/api/v1/agents/{path}", self.base))
            .bearer_auth(token)
            .header("x-ravel-subject", "mallory")
            .header("x-ravel-client-cert-cn", "mallory")
            .json(body)
            .send()
            .await
            .expect("request completes");
        let (status, body) = read(response).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        body
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

fn agent_keyed(key: &str) -> Value {
    json!({"kind": "order", "key": key})
}

fn pins() -> Value {
    json!([{"handler": "place", "version": "1.0.0", "digest": "01".repeat(32)}])
}

async fn register(api: &Api) -> String {
    let body = api
        .ok(WORKER, "worker/register", &json!({"handlers": pins()}))
        .await;
    body["worker_id"].as_str().expect("worker id").to_string()
}

async fn poll(api: &Api, worker_id: &str, wait_ms: u64) -> Value {
    let body = api
        .ok(
            WORKER,
            "worker/poll",
            &json!({"worker_id": worker_id, "handlers": pins(), "wait_ms": wait_ms}),
        )
        .await;
    body["assignment"].clone()
}

/// Create the agent, send one input, and hand its turn to worker A.
/// Answers the operation handle and A's assignment.
async fn begin(api: &Api) -> (String, Value) {
    begin_on(api, "o-1").await
}

/// [`begin`] on the agent keyed `key`.
async fn begin_on(api: &Api, key: &str) -> (String, Value) {
    api.ok(CLIENT, "create", &json!({"agent": agent_keyed(key)}))
        .await;
    let sent = api
        .ok(
            CLIENT,
            "send",
            &json!({"agent": agent_keyed(key), "handler": "place", "request_id": "req-1", "payload": {"sku": "A-1"}}),
        )
        .await;
    let operation = sent["operation"].as_str().expect("operation").to_string();
    let worker_a = register(api).await;
    let assigned = poll(api, &worker_a, 2_000).await;
    assert_eq!(assigned["operation"], json!(operation));
    (operation, assigned)
}

/// Let A's lease and the grace pass, then hand the same turn to worker B.
async fn take_over(api: &Api, assigned_a: &Value) -> Value {
    tokio::time::sleep(Duration::from_millis(2 * (LEASE_MS + GRACE_MS))).await;
    let worker_b = register(api).await;
    let assigned_b = poll(api, &worker_b, 2_000).await;
    assert_eq!(assigned_b["lease"]["owner_id"], json!(worker_b));
    assert_eq!(
        assigned_b["lease"]["turn_id"],
        assigned_a["lease"]["turn_id"]
    );
    assigned_b
}

const INPUT_HASH: &str = "5892e7a2b7b5a3aa11b9984ec594f8eb935e25f2f1a69ce17c2387d2bb1c74f0";

fn step_body(assigned: &Value, step_key: &str, policy: &str, input_hash: &str) -> Value {
    json!({
        "lease": assigned["lease"],
        "step_key": step_key,
        "tool_name": "catalog.price",
        "tool_version": "1",
        "policy": policy,
        "input_hash": input_hash,
    })
}

async fn step(api: &Api, assigned: &Value, policy: &str) -> (StatusCode, Value) {
    api.post(
        WORKER,
        "worker/step",
        &step_body(assigned, "price", policy, INPUT_HASH),
    )
    .await
}

async fn complete(api: &Api, effect: &str, attempt: u64, result: Value) -> (StatusCode, Value) {
    api.post(
        WORKER,
        "worker/effects/complete",
        &json!({"effect": effect, "attempt": attempt, "result": result}),
    )
    .await
}

fn commit_body(assigned: &Value, output: Value) -> Value {
    json!({
        "lease": assigned["lease"],
        "outcome": "completed",
        "state": {"placed": true},
        "output": output,
        "events": [],
        "timers": [],
        "intents": [],
    })
}

fn failed_body(assigned: &Value, error: &str) -> Value {
    failed_body_with(assigned, error, true)
}

fn failed_body_with(assigned: &Value, error: &str, terminal: bool) -> Value {
    json!({
        "lease": assigned["lease"],
        "outcome": "failed",
        "error": error,
        "terminal": terminal,
    })
}

async fn operation_status(api: &Api, operation: &str) -> Value {
    api.ok_get(CLIENT, &format!("operation?operation={operation}"))
        .await
}

async fn inspect(api: &Api, effect: &str) -> Value {
    api.ok_get(CLIENT, &format!("effects/{effect}")).await
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

fn attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
    row.attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

/// Every `kind=agents_reconcile` record in the tenant's audit shard.
async fn reconcile_audit_records(store: &dyn ObjectStoreBackend) -> Vec<LogRecord> {
    audit_records(store, "agents_reconcile").await
}

/// Every record of `kind` in the tenant's audit shard (the read
/// `audit_pipeline_installed.rs::query_audit_records` does).
async fn audit_records(store: &dyn ObjectStoreBackend, kind: &str) -> Vec<LogRecord> {
    let tenant_hash = TenantId::new(TENANT).hash();
    let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Audit, QUERY_AUDIT_SHARD)
        .expect("audit commit prefix");
    let metas = list_all(store, &prefix).await.expect("list audit commits");
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in metas {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit");
        let commit = record::decode(&got.data).expect("decode commit");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(object.data.as_ref(), &cfg).expect("rlog reader");
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        for row in rows {
            if attr(&row, "kind") == Some(kind) {
                out.push(row);
            }
        }
    }
    out
}

/// `at_least_once`: A journals the step and is authorized attempt 1, and a
/// second step while that attempt is open is refused. A dies; B replays the
/// step and is authorized attempt 2. A's late result for attempt 1 is kept as
/// evidence, B's result for attempt 2 is the step's result, B commits once,
/// and inspect shows both attempts and the evidence.
///
/// HEAD publications (CAS under `publish`): send 1, A's takeover 1, A's
/// `BeginTurn` 1, A's step 3 (record step, commit intent, authorize 1), B's
/// takeover 1, B's step 1 (authorize 2; the step itself is already
/// journaled), B's result 1, B's replayed step 1 (journal it completed), B's
/// commit 1. A's refused second step, A's late result and B's `BeginTurn`
/// publish nothing. 11 in all.
#[tokio::test]
async fn completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn()
 {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    assert_eq!(a_step["attempt"], json!(1));
    assert_eq!(a_step["step"]["status"], json!("started"));
    assert_eq!(a_step["step"]["output"], Value::Null);
    assert_eq!(a_step["failure"], Value::Null);
    let effect = a_step["effect"].as_str().expect("effect").to_string();

    // Steps are sequential: a second step while attempt 1 is open.
    let (status, refused) = api
        .post(
            WORKER,
            "worker/step",
            &step_body(&assigned_a, "ship", "at_least_once", INPUT_HASH),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));

    let assigned_b = take_over(&api, &assigned_a).await;
    assert_eq!(assigned_b["steps"][0]["step_key"], json!("price"));
    assert_eq!(assigned_b["steps"][0]["status"], json!("started"));
    let (status, b_step) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{b_step}");
    assert_eq!(b_step["attempt"], json!(2));
    assert_eq!(b_step["effect"], json!(effect));

    // A's attempt 1 reports after attempt 2 was authorized: evidence.
    let (status, late) = complete(&api, &effect, 1, json!({"cents": 1100})).await;
    assert_eq!(status, StatusCode::OK, "{late}");
    assert_eq!(late["late"], json!(true));
    assert_eq!(late["status"], json!("authorized"));

    let (status, recorded) = complete(&api, &effect, 2, json!({"cents": 1299})).await;
    assert_eq!(status, StatusCode::OK, "{recorded}");
    assert_eq!(recorded["late"], json!(false));
    assert_eq!(recorded["status"], json!("completed"));

    let (status, replayed) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["step"]["status"], json!("completed"));
    assert_eq!(replayed["step"]["output"], json!({"cents": 1299}));
    assert_eq!(replayed["attempt"], json!(2));
    assert_eq!(replayed["failure"], Value::Null);

    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &commit_body(&assigned_b, json!({"total_cents": 1299})),
        )
        .await;
    assert_eq!(committed["status"], json!("completed"));
    let result = api
        .ok_get(CLIENT, &format!("result?operation={operation}"))
        .await;
    assert_eq!(result["result"]["output"], json!({"total_cents": 1299}));

    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["effect"], json!(effect));
    assert_eq!(inspected["step_key"], json!("price"));
    assert_eq!(inspected["status"], json!("completed"));
    assert_eq!(inspected["policy"], json!("at_least_once"));
    assert_eq!(inspected["attempt"], json!(2));
    assert_eq!(inspected["result_attempt"], json!(2));
    assert_eq!(inspected["result"], json!({"cents": 1299}));
    assert_eq!(
        inspected["attempts"],
        json!([
            {"attempt": 1, "status": "superseded"},
            {"attempt": 2, "status": "result"},
        ])
    );
    assert_eq!(
        inspected["evidence"],
        json!([{"attempt": 1, "result": {"cents": 1100}}])
    );

    let listed = api.ok_get(CLIENT, "effects?kind=order&key=o-1").await;
    let effects = listed["effects"].as_array().expect("effects");
    assert_eq!(effects.len(), 1, "{listed}");
    assert_eq!(effects[0]["effect"], json!(effect));
    assert_eq!(effects[0]["tool_name"], json!("catalog.price"));
    assert_eq!(effects[0]["tool_version"], json!("1"));
    assert_eq!(effects[0]["policy"], json!("at_least_once"));
    assert_eq!(effects[0]["status"], json!("completed"));
    assert_eq!(effects[0]["attempt"], json!(2));
    assert_eq!(effects[0]["turn_id"], assigned_b["lease"]["turn_id"]);

    let scrape = api.scrape().await;
    assert_eq!(
        value(
            &scrape,
            "ravel_agents_turns_committed_total",
            "mode=\"all\""
        ),
        1
    );
    assert_eq!(
        value(&scrape, "ravel_agents_store_cas_total", "phase=\"publish\""),
        11
    );
    running.shutdown().await.expect("clean shutdown");
}

/// `provider_idempotency`: B's replay is handed the same attempt 1 with no
/// new authorization, so A's late result for it is the step's result, and a
/// different result for that attempt afterwards is refused.
#[tokio::test]
async fn provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "provider_idempotency").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    assert_eq!(a_step["attempt"], json!(1));
    let effect = a_step["effect"].as_str().expect("effect").to_string();

    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, b_step) = step(&api, &assigned_b, "provider_idempotency").await;
    assert_eq!(status, StatusCode::OK, "{b_step}");
    assert_eq!(b_step["attempt"], json!(1));
    assert_eq!(b_step["effect"], json!(effect));

    let (status, a_result) = complete(&api, &effect, 1, json!({"cents": 1100})).await;
    assert_eq!(status, StatusCode::OK, "{a_result}");
    assert_eq!(a_result["late"], json!(false));
    assert_eq!(a_result["status"], json!("completed"));

    let (status, b_result) = complete(&api, &effect, 1, json!({"cents": 1299})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{b_result}");
    assert_eq!(b_result["code"], json!("idempotency_conflict"));

    let (status, replayed) = step(&api, &assigned_b, "provider_idempotency").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["step"]["output"], json!({"cents": 1100}));
    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(&assigned_b, json!({"total_cents": 1100})),
    )
    .await;
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("completed"), "{op}");

    let inspected = inspect(&api, &effect).await;
    assert_eq!(
        inspected["attempts"],
        json!([{"attempt": 1, "status": "result"}])
    );
    assert_eq!(inspected["evidence"], json!([]));
    running.shutdown().await.expect("clean shutdown");
}

/// `manual`: B's replay is not authorized a new attempt. The effect latches
/// in doubt, the step answers `effect_in_doubt`, the turn stays blocked and
/// may not fail, also under C after a takeover. Only an `agents_admin`
/// reconcile unblocks it, once, with one audit record naming the caller; C's
/// replay then reads the attached result and C commits once.
#[tokio::test]
async fn manual_blocks_until_reconciled_and_audits_the_decision_once() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    assert_eq!(a_step["attempt"], json!(1));
    let effect = a_step["effect"].as_str().expect("effect").to_string();

    let assigned_b = take_over(&api, &assigned_a).await;
    for _ in 0..2 {
        let (status, refused) = step(&api, &assigned_b, "manual").await;
        assert_eq!(status, StatusCode::CONFLICT, "{refused}");
        assert_eq!(refused["code"], json!("effect_in_doubt"));
        let op = operation_status(&api, &operation).await;
        assert_eq!(op["status"], json!("blocked"), "{op}");
        assert_eq!(op["blocked_effect"], json!(effect), "{op}");
    }
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["status"], json!("in_doubt"));
    assert_eq!(inspected["attempt"], json!(1), "{inspected}");
    assert_eq!(
        inspected["attempts"],
        json!([{"attempt": 1, "status": "in_doubt"}]),
        "{inspected}"
    );
    let attempt = 1;

    let (status, refused) = api
        .post(
            WORKER,
            "worker/commit",
            &failed_body(&assigned_b, "gave up"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"), "{refused}");
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("blocked"), "{op}");
    // Blocked across a takeover: once B's lease and the grace passed, a poll
    // hands the turn to C (a poll does not skip a blocked turn), and C's
    // replay of the step is refused in doubt too.
    let assigned_c = take_over(&api, &assigned_b).await;
    let (status, refused) = step(&api, &assigned_c, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("blocked"), "{op}");
    assert_eq!(op["blocked_effect"], json!(effect), "{op}");

    let reconcile = json!({
        "effect": effect,
        "attempt": attempt,
        "decision": "record_result",
        "result": {"cents": 1500},
    });
    for token in [CLIENT, WORKER, PLAIN] {
        let (status, refused) = api.post(token, "reconcile", &reconcile).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token}: {refused}");
        assert!(
            refused
                .to_string()
                .contains("requires the agents_admin capability"),
            "{token}: {refused}"
        );
    }
    let reconciled = api.ok(ADMIN, "reconcile", &reconcile).await;
    assert_eq!(reconciled["effect"], json!(effect));
    assert_eq!(reconciled["status"], json!("completed"));
    // Idempotent: the same decision again changes nothing and is not audited.
    let again = api.ok(ADMIN, "reconcile", &reconcile).await;
    assert_eq!(again, reconciled);

    let (status, replayed) = step(&api, &assigned_c, "manual").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["step"]["status"], json!("completed"));
    assert_eq!(replayed["step"]["output"], json!({"cents": 1500}));
    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &commit_body(&assigned_c, json!({"total_cents": 1500})),
        )
        .await;
    assert_eq!(committed["status"], json!("completed"));
    let scrape = api.scrape().await;
    assert_eq!(
        value(
            &scrape,
            "ravel_agents_turns_committed_total",
            "mode=\"all\""
        ),
        1
    );

    let records = reconcile_audit_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let row = &records[0];
    assert_eq!(
        attr(row, "agents.tenant"),
        Some(TenantId::new(TENANT).hash().to_hex().as_str())
    );
    assert_eq!(attr(row, "agents.principal.tenant"), Some(TENANT));
    assert_eq!(
        attr(row, "agents.principal.capabilities"),
        Some("agents_admin")
    );
    assert_eq!(attr(row, "agents.effect"), Some(effect.as_str()));
    assert_eq!(
        attr(row, "agents.attempt"),
        Some(attempt.to_string().as_str())
    );
    assert_eq!(attr(row, "agents.decision"), Some("record_result"));
    running.shutdown().await.expect("clean shutdown");
}

/// Drive the agent keyed `key` until its one `manual` effect is in doubt:
/// A is authorized attempt 1 and dies, and B's replay is refused. Answers
/// the effect id.
async fn effect_in_doubt(api: &Api, key: &str) -> String {
    let (_operation, assigned_a) = begin_on(api, key).await;
    let (status, a_step) = step(api, &assigned_a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    assert_eq!(a_step["attempt"], json!(1));
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let assigned_b = take_over(api, &assigned_a).await;
    let (status, refused) = step(api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));
    effect
}

fn has_attr(row: &LogRecord, key: &str) -> bool {
    row.attrs.iter().any(|(k, _)| k == key)
}

/// ADR-3541: the reconcile and destroy records name the subject the admin's
/// token entry configures, with source `static_token`, and a token with no
/// subject writes neither attribute. Every call also sends `mallory` as
/// `x-ravel-subject` and as the dev mTLS header; no record carries it. No
/// response names a subject.
///
/// Flip to watch it fail: set the principal's subject from the
/// `x-ravel-subject` request header in the reconcile route, and `mallory`
/// lands in a record; or have `principal_attrs` in `agents/audit.rs` push
/// both keys with an empty string when the subject is `None`, and the
/// subject-less record carries them.
#[tokio::test]
async fn reconcile_and_destroy_records_name_each_admins_configured_subject_and_a_token_without_one_writes_no_subject_attr()
 {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let reconcile = |effect: &str| {
        json!({
            "effect": effect,
            "attempt": 1,
            "decision": "record_result",
            "result": {"cents": 1500},
        })
    };
    let mut answers = Vec::new();

    let key = "o-subject-1";
    let effect_1 = effect_in_doubt(&api, key).await;
    let reconciled = api
        .ok_claiming_mallory(ADMIN_ALICE, "reconcile", &reconcile(&effect_1))
        .await;
    assert_eq!(reconciled["status"], json!("completed"), "{reconciled}");
    answers.push(reconciled);
    let destroyed = api
        .ok_claiming_mallory(ADMIN_BOB, "destroy", &destroy_body(key, None))
        .await;
    assert_eq!(destroyed["destroyed"], json!(true), "{destroyed}");
    answers.push(destroyed);

    let effect_2 = effect_in_doubt(&api, "o-subject-2").await;
    let reconciled = api
        .ok_claiming_mallory(ADMIN, "reconcile", &reconcile(&effect_2))
        .await;
    assert_eq!(reconciled["status"], json!("completed"), "{reconciled}");
    answers.push(reconciled);

    for answer in &answers {
        let text = answer.to_string();
        for name in ["alice", "bob@example.com", "mallory"] {
            assert!(!text.contains(name), "{name} in a response: {text}");
        }
    }

    let reconciles = reconcile_audit_records(store.as_ref()).await;
    assert_eq!(reconciles.len(), 2, "{reconciles:?}");
    let by_effect = |effect: &str| {
        reconciles
            .iter()
            .find(|row| attr(row, "agents.effect") == Some(effect))
            .expect("one reconcile record per effect")
    };
    let alices = by_effect(&effect_1);
    assert_eq!(attr(alices, "agents.principal.subject"), Some("alice"));
    assert_eq!(
        attr(alices, "agents.principal.subject_source"),
        Some("static_token")
    );
    let subjectless = by_effect(&effect_2);
    assert_eq!(
        attr(subjectless, "agents.principal.capabilities"),
        Some("agents_admin")
    );
    assert!(
        !has_attr(subjectless, "agents.principal.subject"),
        "{subjectless:?}"
    );
    assert!(
        !has_attr(subjectless, "agents.principal.subject_source"),
        "{subjectless:?}"
    );

    let destroys = audit_records(store.as_ref(), "agents_destroy").await;
    assert_eq!(destroys.len(), 1, "{destroys:?}");
    assert_eq!(
        attr(&destroys[0], "agents.principal.subject"),
        Some("bob@example.com")
    );
    assert_eq!(
        attr(&destroys[0], "agents.principal.subject_source"),
        Some("static_token")
    );

    for row in reconciles.iter().chain(&destroys) {
        assert!(
            !format!("{:?}", row.attrs).contains("mallory"),
            "a client-sent subject reached a record: {row:?}"
        );
    }
    running.shutdown().await.expect("clean shutdown");
}

/// A replay of a journaled step with another input hash is
/// `non_deterministic_replay`, and the operation reads blocked.
#[tokio::test]
async fn a_drifted_replay_is_non_deterministic_and_blocks_the_turn() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let (status, recorded) = complete(&api, &effect, 1, json!({"cents": 1100})).await;
    assert_eq!(status, StatusCode::OK, "{recorded}");

    let drifted = "ab".repeat(32);
    let (status, refused) = api
        .post(
            WORKER,
            "worker/step",
            &step_body(&assigned_a, "price", "at_least_once", &drifted),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("non_deterministic_replay"));
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("blocked"), "{op}");
    running.shutdown().await.expect("clean shutdown");
}

/// Cancel while A's attempt is open: B takes the turn over, and its replay
/// of the step is refused a new attempt because the input is being
/// cancelled. B's commit settles the input cancelled, and A's result for
/// its attempt then arrives late and is kept as evidence.
#[tokio::test]
async fn cancel_during_an_open_attempt_refuses_the_replay_and_the_result_is_late() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();

    let cancelled = api
        .ok(CLIENT, "cancel", &json!({"operation": operation}))
        .await;
    assert_eq!(cancelled["requested"], json!(true), "{cancelled}");

    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, refused) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("ownership_lost"));
    assert!(
        refused["reason"]
            .as_str()
            .is_some_and(|r| r.contains("cancelled")),
        "{refused}"
    );

    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(&assigned_b, json!({"ok": true})),
    )
    .await;
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("cancelled"), "{op}");

    let (status, late) = complete(&api, &effect, 1, json!({"cents": 1100})).await;
    assert_eq!(status, StatusCode::OK, "{late}");
    assert_eq!(late["late"], json!(true));
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["result"], Value::Null);
    assert_eq!(
        inspected["evidence"],
        json!([{"attempt": 1, "result": {"cents": 1100}}])
    );
    running.shutdown().await.expect("clean shutdown");
}

/// A tool failure reported through `effects/fail` is the step's result: the
/// replayed step answers it as `failure`, the poll does not hand it out as
/// a completed output, and the turn may then fail.
#[tokio::test]
async fn a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let failed = api
        .ok(
            WORKER,
            "worker/effects/fail",
            &json!({"effect": effect, "attempt": 1, "error": "out of stock"}),
        )
        .await;
    assert_eq!(failed["late"], json!(false));
    assert_eq!(failed["status"], json!("completed"));

    for _ in 0..2 {
        let (status, replayed) = step(&api, &assigned_a, "at_least_once").await;
        assert_eq!(status, StatusCode::OK, "{replayed}");
        assert_eq!(replayed["failure"], json!("out of stock"));
        assert_eq!(replayed["step"]["status"], json!("started"));
        assert_eq!(replayed["step"]["output"], Value::Null);
        assert_eq!(replayed["attempt"], json!(1));
    }

    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["failure"], json!("out of stock"), "{inspected}");
    assert_eq!(inspected["result"], Value::Null, "{inspected}");

    // A retryable failure is accepted; the retried turn replays the failure.
    let retried = api
        .ok(
            WORKER,
            "worker/commit",
            &failed_body_with(&assigned_a, "out of stock", false),
        )
        .await;
    assert_eq!(retried["status"], json!("accepted"), "{retried}");
    let op = operation_status(&api, &operation).await;
    assert_eq!(
        op["status"],
        json!("accepted"),
        "the turn is queued again: {op}"
    );
    let worker_b = register(&api).await;
    let assigned_b = poll(&api, &worker_b, 2_000).await;
    assert_eq!(assigned_b["operation"], json!(operation), "{assigned_b}");
    let steps = assigned_b["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 1, "{assigned_b}");
    assert_eq!(steps[0]["status"], json!("started"), "{assigned_b}");
    assert_eq!(steps[0]["output"], Value::Null, "{assigned_b}");
    assert_ne!(
        assigned_b["lease"]["turn_id"], assigned_a["lease"]["turn_id"],
        "the retry runs under a new turn id"
    );
    assert_eq!(
        steps[0]["step_id"], a_step["step"]["step_id"],
        "{assigned_b}"
    );
    let (status, replayed) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(
        replayed["effect"],
        json!(effect),
        "the retry reuses the effect"
    );
    assert_eq!(replayed["attempt"], json!(1));
    assert_eq!(replayed["failure"], json!("out of stock"));
    assert_eq!(replayed["step"]["status"], json!("started"));
    assert_eq!(replayed["step"]["output"], Value::Null);

    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &failed_body(&assigned_b, "out of stock"),
        )
        .await;
    assert_eq!(committed["status"], json!("failed"), "{committed}");
    let result = api
        .ok_get(CLIENT, &format!("result?operation={operation}"))
        .await;
    assert_eq!(result["result"]["error"], json!("out of stock"), "{result}");
    running.shutdown().await.expect("clean shutdown");
}

/// `manual` retry through `reconcile`: the route authorizes attempt 2 and
/// dispatches nothing; the owner's next `worker/step` for the key is handed
/// attempt 2 and runs the tool once more. A retry naming attempt 0 while
/// attempt 1 is open is refused `idempotency_conflict` and not audited, a
/// repeated retry of attempt 1 answers `authorized`/2 again without a second
/// audit record, a second retry while attempt 2 is open is refused, and once
/// attempt 2's result is recorded the turn commits once.
#[tokio::test]
async fn a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    assert_eq!(a_step["attempt"], json!(1));
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let retry = |attempt: u64| json!({"effect": effect, "attempt": attempt, "decision": "retry"});

    // Attempt 1 is open: a retry naming attempt 0, which never existed, is
    // refused before the audit write.
    let (status, refused) = api.post(ADMIN, "reconcile", &retry(0)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));

    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, refused) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));

    let reconciled = api.ok(ADMIN, "reconcile", &retry(1)).await;
    assert_eq!(reconciled["status"], json!("authorized"), "{reconciled}");
    assert_eq!(reconciled["attempt"], json!(2), "{reconciled}");
    // The same retry again is a repeat: answered from HEAD, not audited.
    let repeated = api.ok(ADMIN, "reconcile", &retry(1)).await;
    assert_eq!(repeated, reconciled);
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("running"), "{op}");

    let (status, handed) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::OK, "{handed}");
    assert_eq!(handed["attempt"], json!(2), "{handed}");
    assert_eq!(handed["effect"], json!(effect));
    assert_eq!(handed["step"]["status"], json!("started"));
    assert_eq!(handed["step"]["output"], Value::Null);
    assert_eq!(handed["failure"], Value::Null);
    assert_eq!(
        inspect(&api, &effect).await["attempts"],
        json!([
            {"attempt": 1, "status": "superseded"},
            {"attempt": 2, "status": "open"},
        ])
    );

    // Attempt 2 is open: a retry of it is refused, and a repeat of the step
    // is not handed attempt 2 a second time.
    let (status, refused) = api.post(ADMIN, "reconcile", &retry(2)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("ownership_lost"));
    let (status, refused) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));

    let (status, recorded) = complete(&api, &effect, 2, json!({"cents": 1700})).await;
    assert_eq!(status, StatusCode::OK, "{recorded}");
    assert_eq!(recorded["late"], json!(false));
    let (status, replayed) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["step"]["status"], json!("completed"));
    assert_eq!(replayed["step"]["output"], json!({"cents": 1700}));
    assert_eq!(replayed["attempt"], json!(2));

    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &commit_body(&assigned_b, json!({"total_cents": 1700})),
        )
        .await;
    assert_eq!(committed["status"], json!("completed"));
    let scrape = api.scrape().await;
    assert_eq!(
        value(
            &scrape,
            "ravel_agents_turns_committed_total",
            "mode=\"all\""
        ),
        1
    );
    let inspected = inspect(&api, &effect).await;
    assert_eq!(
        inspected["attempts"],
        json!([
            {"attempt": 1, "status": "superseded"},
            {"attempt": 2, "status": "result"},
        ])
    );
    // One record: the refused retry(0) and the repeated retry(1) wrote none.
    let records = reconcile_audit_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(attr(&records[0], "agents.decision"), Some("retry"));
    assert_eq!(attr(&records[0], "agents.attempt"), Some("1"));
    running.shutdown().await.expect("clean shutdown");
}

/// A reconcile retry is answered as a repeat only for an attempt a reconcile
/// in this process handed over and no step has claimed. An `at_least_once`
/// effect whose attempt B's step raised to 2 after a takeover was never in
/// doubt, so a retry naming attempt 1 is `ownership_lost`, writes no audit
/// record, and hands nothing over: B's next step is still refused while its
/// own attempt 2 is open.
#[tokio::test]
async fn a_reconcile_retry_of_an_attempt_a_takeover_raised_is_ownership_lost() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let (_, assigned_a) = begin(&api).await;
    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, b_step) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{b_step}");
    assert_eq!(b_step["attempt"], json!(2));

    let retry = json!({"effect": effect, "attempt": 1, "decision": "retry"});
    let (status, refused) = api.post(ADMIN, "reconcile", &retry).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("ownership_lost"), "{refused}");
    assert!(
        refused["reason"]
            .as_str()
            .is_some_and(|r| r.contains("hand-over")),
        "{refused}"
    );
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["status"], json!("authorized"), "{inspected}");
    assert_eq!(inspected["attempt"], json!(2), "{inspected}");
    let (status, refused) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));
    assert!(reconcile_audit_records(store.as_ref()).await.is_empty());
    running.shutdown().await.expect("clean shutdown");
}

/// The evidence index gives a settled effect's slot to a new one. With room
/// for one effect: o-1's superseded `at_least_once` attempt is evidence
/// until attempt 2's result settles the effect; o-2's late result arrives
/// after its input was cancelled and takes o-1's slot; o-3's `manual`
/// effect, reconciled to attempt 2 and then in doubt again under a third
/// owner, still lists attempt 1's late result.
#[tokio::test]
async fn settled_effects_give_their_evidence_slot_to_an_effect_in_doubt() {
    let running = start_server_with(
        Arc::new(MemoryStore::new()),
        AgentsConfig {
            max_evidence_effects: 1,
            ..agents_config()
        },
    )
    .await;
    let api = Api::public(&running);

    let (_, a1) = begin_on(&api, "o-1").await;
    let (status, a_step) = step(&api, &a1, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let e1 = a_step["effect"].as_str().expect("effect").to_string();
    let b1 = take_over(&api, &a1).await;
    let (status, b_step) = step(&api, &b1, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{b_step}");
    let (_, late) = complete(&api, &e1, 1, json!({"cents": 1100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(
        inspect(&api, &e1).await["evidence"],
        json!([{"attempt": 1, "result": {"cents": 1100}}])
    );
    let (_, recorded) = complete(&api, &e1, 2, json!({"cents": 1299})).await;
    assert_eq!(recorded["status"], json!("completed"), "{recorded}");
    let (_, replayed) = step(&api, &b1, "at_least_once").await;
    assert_eq!(replayed["step"]["status"], json!("completed"), "{replayed}");
    api.ok(WORKER, "worker/commit", &commit_body(&b1, json!({})))
        .await;

    let (op2, a2) = begin_on(&api, "o-2").await;
    let (status, a_step) = step(&api, &a2, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let e2 = a_step["effect"].as_str().expect("effect").to_string();
    api.ok(CLIENT, "cancel", &json!({"operation": op2})).await;
    let b2 = take_over(&api, &a2).await;
    api.ok(WORKER, "worker/commit", &commit_body(&b2, json!({})))
        .await;
    let (_, late) = complete(&api, &e2, 1, json!({"cents": 2100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(
        inspect(&api, &e2).await["evidence"],
        json!([{"attempt": 1, "result": {"cents": 2100}}])
    );
    assert_eq!(inspect(&api, &e1).await["evidence"], json!([]), "evicted");

    let (_, a3) = begin_on(&api, "o-3").await;
    let (status, a_step) = step(&api, &a3, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let e3 = a_step["effect"].as_str().expect("effect").to_string();
    let b3 = take_over(&api, &a3).await;
    let (_, refused) = step(&api, &b3, "manual").await;
    assert_eq!(refused["code"], json!("effect_in_doubt"), "{refused}");
    api.ok(
        ADMIN,
        "reconcile",
        &json!({"effect": e3, "attempt": 1, "decision": "retry"}),
    )
    .await;
    let (_, handed) = step(&api, &b3, "manual").await;
    assert_eq!(handed["attempt"], json!(2), "{handed}");
    let (_, late) = complete(&api, &e3, 1, json!({"cents": 3100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    let c3 = take_over(&api, &b3).await;
    let (_, refused) = step(&api, &c3, "manual").await;
    assert_eq!(refused["code"], json!("effect_in_doubt"), "{refused}");

    let inspected = inspect(&api, &e3).await;
    assert_eq!(inspected["status"], json!("in_doubt"), "{inspected}");
    assert_eq!(
        inspected["evidence"],
        json!([{"attempt": 1, "result": {"cents": 3100}}]),
        "{inspected}"
    );
    assert_eq!(inspect(&api, &e2).await["evidence"], json!([]), "evicted");
    running.shutdown().await.expect("clean shutdown");
}

/// A late result the full evidence index cannot list is counted on
/// `/metrics`. With room for one effect and two `at_least_once` effects whose
/// attempt 2 is open after a takeover, o-1's late result takes the slot and
/// o-2's is dropped: o-1's effect is not settled, so its slot stays.
#[tokio::test]
async fn a_late_result_the_full_evidence_index_drops_is_counted() {
    let running = start_server_with(
        Arc::new(MemoryStore::new()),
        AgentsConfig {
            max_evidence_effects: 1,
            ..agents_config()
        },
    )
    .await;
    let api = Api::public(&running);
    let dropped = |scrape: &str| {
        value(
            scrape,
            "ravel_agents_evidence_dropped_total",
            "mode=\"all\"",
        )
    };

    // Both turns are begun before one lease expiry, so the two takeover
    // polls hand exactly these two turns to two new workers.
    let mut effects = Vec::new();
    for key in ["o-1", "o-2"] {
        let (_, a) = begin_on(&api, key).await;
        let (status, a_step) = step(&api, &a, "at_least_once").await;
        assert_eq!(status, StatusCode::OK, "{a_step}");
        assert_eq!(a_step["attempt"], json!(1), "{a_step}");
        effects.push(a_step["effect"].as_str().expect("effect").to_string());
    }
    tokio::time::sleep(Duration::from_millis(2 * (LEASE_MS + GRACE_MS))).await;
    let mut raised = Vec::new();
    for _ in 0..2 {
        let worker_b = register(&api).await;
        let b = poll(&api, &worker_b, 2_000).await;
        assert_eq!(b["lease"]["owner_id"], json!(worker_b), "{b}");
        let (status, b_step) = step(&api, &b, "at_least_once").await;
        assert_eq!(status, StatusCode::OK, "{b_step}");
        assert_eq!(b_step["attempt"], json!(2), "{b_step}");
        raised.push(b_step["effect"].as_str().expect("effect").to_string());
    }
    raised.sort();
    let mut begun = effects.clone();
    begun.sort();
    assert_eq!(raised, begun, "B took over both turns");
    let (e1, e2) = (&effects[0], &effects[1]);

    let (_, late) = complete(&api, e1, 1, json!({"cents": 1100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(dropped(&api.scrape().await), 0, "o-1's result is listed");

    let (_, late) = complete(&api, e2, 1, json!({"cents": 2100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(dropped(&api.scrape().await), 1, "o-2's result is dropped");
    assert_eq!(inspect(&api, e2).await["evidence"], json!([]));
    assert_eq!(
        inspect(&api, e1).await["evidence"],
        json!([{"attempt": 1, "result": {"cents": 1100}}]),
        "the drop came from the cap, not from evicting o-1"
    );
    running.shutdown().await.expect("clean shutdown");
}

/// A tool output of exactly `{"error": <string>}` reported through
/// `effects/complete` is an output like any other: the replayed step and the
/// next poll return it as the output, never as a failure.
#[tokio::test]
async fn an_output_shaped_like_a_failure_replays_as_the_output() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (operation, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let (status, recorded) = complete(&api, &effect, 1, json!({"error": "x"})).await;
    assert_eq!(status, StatusCode::OK, "{recorded}");

    let (status, replayed) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["failure"], Value::Null, "{replayed}");
    assert_eq!(replayed["step"]["status"], json!("completed"));
    assert_eq!(replayed["step"]["output"], json!({"error": "x"}));

    let assigned_b = take_over(&api, &assigned_a).await;
    assert_eq!(assigned_b["steps"][0]["status"], json!("completed"));
    assert_eq!(assigned_b["steps"][0]["output"], json!({"error": "x"}));
    let (status, replayed) = step(&api, &assigned_b, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["failure"], Value::Null, "{replayed}");
    assert_eq!(replayed["step"]["output"], json!({"error": "x"}));
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["result"], json!({"error": "x"}));
    assert_eq!(inspected["failure"], Value::Null);

    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(&assigned_b, json!({"ok": true})),
    )
    .await;
    let op = operation_status(&api, &operation).await;
    assert_eq!(op["status"], json!("completed"), "{op}");
    running.shutdown().await.expect("clean shutdown");
}

/// Two different results for one attempt: the first is the result, an
/// equal repeat is accepted, and a different one is `idempotency_conflict`.
#[tokio::test]
async fn a_conflicting_final_result_for_the_same_attempt_is_refused() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (_, assigned_a) = begin(&api).await;

    let (status, a_step) = step(&api, &assigned_a, "at_least_once").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    for _ in 0..2 {
        let (status, recorded) = complete(&api, &effect, 1, json!({"cents": 1100})).await;
        assert_eq!(status, StatusCode::OK, "{recorded}");
        assert_eq!(recorded["late"], json!(false));
    }
    let (status, refused) = complete(&api, &effect, 1, json!({"cents": 9999})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["result"], json!({"cents": 1100}));
    running.shutdown().await.expect("clean shutdown");
}

/// The retry outlives B: B abandons the blocked turn, its lease expires after
/// the reconcile, and the worker that takes the turn over is handed attempt 2
/// by its first `worker/step` for the key, once.
#[tokio::test]
async fn a_reconcile_retry_is_handed_to_the_owner_after_a_takeover() {
    let running = start_server(Arc::new(MemoryStore::new())).await;
    let api = Api::public(&running);
    let (_, assigned_a) = begin(&api).await;
    let (status, a_step) = step(&api, &assigned_a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, refused) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));

    let retry = json!({"effect": effect, "attempt": 1, "decision": "retry"});
    let reconciled = api.ok(ADMIN, "reconcile", &retry).await;
    assert_eq!(reconciled["status"], json!("authorized"), "{reconciled}");
    assert_eq!(reconciled["attempt"], json!(2), "{reconciled}");

    let assigned_c = take_over(&api, &assigned_b).await;
    let (status, handed) = step(&api, &assigned_c, "manual").await;
    assert_eq!(status, StatusCode::OK, "{handed}");
    assert_eq!(handed["attempt"], json!(2), "{handed}");
    assert_eq!(handed["effect"], json!(effect));
    let (status, refused) = step(&api, &assigned_c, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("idempotency_conflict"));
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["status"], json!("authorized"), "{inspected}");
    assert_eq!(
        inspected["attempts"],
        json!([
            {"attempt": 1, "status": "superseded"},
            {"attempt": 2, "status": "open"},
        ])
    );

    let (status, recorded) = complete(&api, &effect, 2, json!({"cents": 1700})).await;
    assert_eq!(status, StatusCode::OK, "{recorded}");
    let (status, replayed) = step(&api, &assigned_c, "manual").await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["step"]["output"], json!({"cents": 1700}));
    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &commit_body(&assigned_c, json!({"total_cents": 1700})),
        )
        .await;
    assert_eq!(committed["status"], json!("completed"));
    running.shutdown().await.expect("clean shutdown");
}

/// The `mode="all"` sample of `name`.
fn counter(scrape: &str, name: &str) -> u64 {
    value(scrape, name, "mode=\"all\"")
}

/// Every sample of `name` summed, whatever its labels.
fn total(scrape: &str, name: &str) -> u64 {
    scrape
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")))
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .expect("integer sample")
        })
        .sum()
}

/// Scrape until the `mode="all"` sample of `name` reaches `want`, so the
/// next request is ordered after a state the server reports.
async fn wait_for_counter(api: &Api, name: &str, want: u64) {
    for _ in 0..2_000 {
        if counter(&api.scrape().await, name) >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{name} never reached {want}");
}

/// A timer due long after any test ends, so nothing fires it.
const FAR_DUE_UNIX_NS: &str = "4102444800000000000";

async fn schedule_far_timer(api: &Api, key: &str) {
    api.ok(
        CLIENT,
        "timers",
        &json!({
            "agent": agent_keyed(key),
            "timer_key": "remind",
            "due_unix_ns": FAR_DUE_UNIX_NS,
            "handler": "place",
            "payload": {},
        }),
    )
    .await;
}

fn destroy_body(key: &str, incarnation: Option<u64>) -> Value {
    match incarnation {
        Some(n) => json!({"agent": agent_keyed(key), "incarnation": n}),
        None => json!({"agent": agent_keyed(key)}),
    }
}

fn assert_agent_gone(what: &str, status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::NOT_FOUND, "{what}: {body}");
    assert_eq!(body["code"], json!("agent_gone"), "{what}: {body}");
}

/// The destroy acceptance path (ADR-3331, destroy amendment). Worker A holds
/// the lease on input 1 of two, has attempt 1 of a `manual` effect
/// authorized, and a timer is scheduled. Only `agents_admin` may destroy.
/// The destroy answers what it abandoned, publishes one HEAD CAS, wakes a
/// result long poll already parked on input 1, and leaves every handle and
/// route on the agent `agent_gone`: A's heartbeat, its effect result, its
/// commit, the operation, its result, the state and a reconcile of the
/// authorized effect. A second destroy is `destroyed: false` with the same
/// counts and is not audited; the one `agents_destroy` record carries the
/// response's counts. A create under the same key starts incarnation 2,
/// with fresh state and an empty mailbox, and the old handle stays gone.
#[tokio::test]
async fn destroy_fences_the_lease_holder_audits_once_and_a_recreate_starts_incarnation_two() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let key = "o-destroy";
    let (operation, assigned) = begin_on(&api, key).await;
    api.ok(
        CLIENT,
        "send",
        &json!({"agent": agent_keyed(key), "handler": "place", "request_id": "req-2", "payload": {"sku": "B-2"}}),
    )
    .await;
    let (status, stepped) = step(&api, &assigned, "manual").await;
    assert_eq!(status, StatusCode::OK, "{stepped}");
    assert_eq!(stepped["attempt"], json!(1));
    let effect = stepped["effect"].as_str().expect("effect").to_string();
    schedule_far_timer(&api, key).await;

    let waiter = {
        let api = api.clone();
        let operation = operation.clone();
        tokio::spawn(async move {
            api.get(
                CLIENT,
                &format!("result?operation={operation}&wait_ms=15000"),
            )
            .await
        })
    };
    wait_for_counter(&api, "ravel_agents_result_waits_total", 1).await;
    let before = api.scrape().await;
    assert_eq!(counter(&before, "ravel_agents_result_wakes_total"), 0);

    for token in [CLIENT, WORKER, PLAIN] {
        let (status, refused) = api.post(token, "destroy", &destroy_body(key, None)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token}: {refused}");
        assert!(
            refused
                .to_string()
                .contains("requires the agents_admin capability"),
            "{token}: {refused}"
        );
    }
    let destroyed = api.ok(ADMIN, "destroy", &destroy_body(key, None)).await;
    let agent_id = destroyed["agent_id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let abandoned = json!({
        "inputs": 2,
        "active_turn": true,
        "effects_in_doubt": 0,
        "effects_authorized": 1,
        "timers_scheduled": 1,
    });
    assert_eq!(
        destroyed,
        json!({
            "agent_id": agent_id,
            "incarnation": 1,
            "destroyed": true,
            "abandoned": abandoned,
        })
    );
    let after = api.scrape().await;
    let publish = "phase=\"publish\"";
    assert_eq!(
        value(&after, "ravel_agents_store_cas_total", publish)
            - value(&before, "ravel_agents_store_cas_total", publish),
        1,
        "the tombstone is the destroy's only HEAD publication"
    );

    // The parked long poll ends inside its request, woken by the destroy
    // rather than by its periodic HEAD re-read or its deadline.
    let (status, waited) = waiter.await.expect("waiter joins");
    assert_agent_gone("parked result", status, &waited);
    let woken = api.scrape().await;
    assert_eq!(counter(&woken, "ravel_agents_result_wakes_total"), 1);
    assert_eq!(
        counter(&woken, "ravel_agents_result_wait_expiries_total"),
        0
    );

    let (status, refused) = api
        .post(
            WORKER,
            "worker/heartbeat",
            &json!({"lease": assigned["lease"]}),
        )
        .await;
    assert_agent_gone("heartbeat", status, &refused);
    let (status, refused) = complete(&api, &effect, 1, json!({"cents": 900})).await;
    assert_agent_gone("effect result", status, &refused);
    let (status, refused) = api
        .post(
            WORKER,
            "worker/commit",
            &commit_body(&assigned, json!({"total_cents": 900})),
        )
        .await;
    assert_agent_gone("commit", status, &refused);
    let (status, refused) = api
        .get(CLIENT, &format!("operation?operation={operation}"))
        .await;
    assert_agent_gone("operation", status, &refused);
    let (status, refused) = api
        .get(CLIENT, &format!("result?operation={operation}&wait_ms=0"))
        .await;
    assert_agent_gone("result", status, &refused);
    let (status, refused) = api
        .get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_agent_gone("state", status, &refused);
    let (status, refused) = api
        .post(
            ADMIN,
            "reconcile",
            &json!({"effect": effect, "attempt": 1, "decision": "retry"}),
        )
        .await;
    assert_agent_gone("reconcile", status, &refused);

    let again = api.ok(ADMIN, "destroy", &destroy_body(key, None)).await;
    assert_eq!(
        again,
        json!({
            "agent_id": agent_id,
            "incarnation": 1,
            "destroyed": false,
            "abandoned": abandoned,
        })
    );

    let records = audit_records(store.as_ref(), "agents_destroy").await;
    assert_eq!(records.len(), 1, "{records:?}");
    let row = &records[0];
    assert_eq!(
        attr(row, "agents.tenant"),
        Some(TenantId::new(TENANT).hash().to_hex().as_str())
    );
    assert_eq!(attr(row, "agents.principal.tenant"), Some(TENANT));
    assert_eq!(
        attr(row, "agents.principal.capabilities"),
        Some("agents_admin")
    );
    assert_eq!(attr(row, "agents.agent_id"), Some(agent_id.as_str()));
    assert_eq!(attr(row, "agents.incarnation"), Some("1"));
    // The record's counts are read before the tombstone CAS and the
    // response's from the HEAD it was applied to; nothing writes between.
    for (field, want) in [
        ("inputs", "2"),
        ("active_turn", "true"),
        ("effects_in_doubt", "0"),
        ("effects_authorized", "1"),
        ("timers_scheduled", "1"),
    ] {
        assert_eq!(
            attr(row, &format!("agents.abandoned.{field}")),
            Some(want),
            "{field}"
        );
        assert_eq!(abandoned[field].to_string(), want, "{field}");
    }

    let recreated = api
        .ok(CLIENT, "create", &json!({"agent": agent_keyed(key)}))
        .await;
    assert_eq!(
        recreated,
        json!({"agent_id": agent_id, "incarnation": 2, "created": true})
    );
    let (status, refused) = api
        .get(CLIENT, &format!("operation?operation={operation}"))
        .await;
    assert_agent_gone("old operation after recreate", status, &refused);
    let state = api
        .ok_get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_eq!(state["incarnation"], json!(2), "{state}");
    assert_eq!(state["state_version"], json!(0), "{state}");
    assert!(state.get("mailbox").is_none(), "{state}");
    assert!(state.get("active_turn").is_none(), "{state}");
    let sent = api
        .ok(
            CLIENT,
            "send",
            &json!({"agent": agent_keyed(key), "handler": "place", "request_id": "req-1", "payload": {"sku": "A-1"}}),
        )
        .await;
    assert_eq!(sent["accept_seq"], json!(1), "{sent}");
    assert_eq!(sent["duplicate"], json!(false), "{sent}");
    assert_ne!(sent["operation"], json!(operation));
    running.shutdown().await.expect("clean shutdown");
}

/// A destroy of an agent that never existed, or pinned to an incarnation
/// that is no longer current, is `agent_gone`, writes nothing and is not
/// audited; the current incarnation stays active.
#[tokio::test]
async fn destroy_of_an_unknown_agent_or_a_stale_incarnation_is_agent_gone_and_not_audited() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);

    let before = api.scrape().await;
    let (status, refused) = api
        .post(ADMIN, "destroy", &destroy_body("never-created", None))
        .await;
    assert_agent_gone("unknown agent", status, &refused);
    let after = api.scrape().await;
    for name in [
        "ravel_agents_store_puts_total",
        "ravel_agents_store_cas_total",
    ] {
        assert_eq!(total(&after, name), total(&before, name), "{name}");
    }
    assert!(
        audit_records(store.as_ref(), "agents_destroy")
            .await
            .is_empty()
    );

    let key = "o-stale";
    api.ok(CLIENT, "create", &json!({"agent": agent_keyed(key)}))
        .await;
    let first = api.ok(ADMIN, "destroy", &destroy_body(key, None)).await;
    assert_eq!(first["destroyed"], json!(true), "{first}");
    let recreated = api
        .ok(CLIENT, "create", &json!({"agent": agent_keyed(key)}))
        .await;
    assert_eq!(recreated["incarnation"], json!(2), "{recreated}");
    assert_eq!(
        audit_records(store.as_ref(), "agents_destroy").await.len(),
        1
    );

    let before = api.scrape().await;
    let (status, refused) = api
        .post(ADMIN, "destroy", &destroy_body(key, Some(1)))
        .await;
    assert_agent_gone("stale incarnation", status, &refused);
    assert_eq!(refused["ids"]["incarnation"], json!(1), "{refused}");
    let after = api.scrape().await;
    for name in [
        "ravel_agents_store_puts_total",
        "ravel_agents_store_cas_total",
    ] {
        assert_eq!(total(&after, name), total(&before, name), "{name}");
    }
    assert_eq!(
        audit_records(store.as_ref(), "agents_destroy").await.len(),
        1
    );
    let state = api
        .ok_get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_eq!(state["incarnation"], json!(2), "{state}");

    let second = api.ok(ADMIN, "destroy", &destroy_body(key, Some(2))).await;
    assert_eq!(second["incarnation"], json!(2), "{second}");
    assert_eq!(second["destroyed"], json!(true), "{second}");
    assert_eq!(
        audit_records(store.as_ref(), "agents_destroy").await.len(),
        2
    );
    running.shutdown().await.expect("clean shutdown");
}

/// A memory store whose next HEAD writes, once armed, go through a
/// `FaultStore` rule that answers each with `fault`: `DuplicateDelivery`
/// applies the write and loses its acknowledgement, `Timeout` never reaches
/// the store. Either way the writer cannot tell whether it landed. Every
/// other request goes straight to the memory store.
struct LoseNextHeadAck {
    faults: Arc<FaultStore<MemoryStore>>,
    armed: AtomicU64,
}

impl LoseNextHeadAck {
    fn new(fault: ScriptedFault) -> Arc<Self> {
        let plan =
            FaultPlan::empty().with_rule(Rule::new(Op::Put, fault).with_key_contains("/head"));
        Arc::new(LoseNextHeadAck {
            faults: Arc::new(FaultStore::new(MemoryStore::new(), plan)),
            armed: AtomicU64::new(0),
        })
    }

    /// Lose the acknowledgement of the next `writes` HEAD writes.
    fn arm(&self, writes: u64) {
        self.armed.store(writes, Ordering::SeqCst);
    }

    fn lost(&self, kind: FaultKind) -> u64 {
        self.faults.fault_count(Op::Put, kind)
    }
}

#[async_trait]
impl ObjectStoreBackend for LoseNextHeadAck {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let lose = key.ends_with("/head")
            && self
                .armed
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
        if lose {
            self.faults.put(key, data, opts).await
        } else {
            self.faults.inner().put(key, data, opts).await
        }
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.faults.inner().get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.faults.inner().head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.faults.inner().list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.faults.inner().list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.faults.inner().delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.faults.inner().capabilities()
    }
}

/// A destroy whose tombstone CAS applied but whose acknowledgement was lost
/// is `outcome_unknown` with retry hint `backoff` (it mints no operation
/// handle to resolve) and names the incarnation it read. Retrying the same
/// destroy pinned to that incarnation answers `destroyed: false`, and only
/// the first attempt is audited.
///
/// The retry is where the tombstone is known to have landed, so it forgets
/// the agent's evidence. No route reads the evidence index of a tombstoned
/// agent, so the index's one slot shows it: o-lost's late result holds the
/// slot through the unknown outcome, and after the retry o-other's late
/// result is listed in it rather than counted dropped.
#[tokio::test]
async fn a_destroy_whose_head_write_is_lost_answers_backoff_and_a_retry_answers_destroyed_false() {
    let faulty = LoseNextHeadAck::new(ScriptedFault::DuplicateDelivery);
    let store: Arc<dyn ObjectStoreBackend> = faulty.clone();
    let running = start_server_with(
        store.clone(),
        AgentsConfig {
            max_evidence_effects: 1,
            ..agents_config()
        },
    )
    .await;
    let api = Api::public(&running);
    let dropped = |scrape: &str| {
        value(
            scrape,
            "ravel_agents_evidence_dropped_total",
            "mode=\"all\"",
        )
    };
    let key = "o-lost";

    // Both turns are begun before one lease expiry, so the two takeover
    // polls hand exactly these two turns to two new workers, and each
    // effect's attempt 2 is open when its attempt 1 reports late.
    let mut effects = Vec::new();
    for agent in [key, "o-other"] {
        let (_, a) = begin_on(&api, agent).await;
        let (status, a_step) = step(&api, &a, "at_least_once").await;
        assert_eq!(status, StatusCode::OK, "{a_step}");
        effects.push(a_step["effect"].as_str().expect("effect").to_string());
    }
    tokio::time::sleep(Duration::from_millis(2 * (LEASE_MS + GRACE_MS))).await;
    for _ in 0..2 {
        let worker_b = register(&api).await;
        let b = poll(&api, &worker_b, 2_000).await;
        let (status, b_step) = step(&api, &b, "at_least_once").await;
        assert_eq!(status, StatusCode::OK, "{b_step}");
        assert_eq!(b_step["attempt"], json!(2), "{b_step}");
    }
    let (lost_effect, other_effect) = (&effects[0], &effects[1]);
    let (_, late) = complete(&api, lost_effect, 1, json!({"cents": 1100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(
        inspect(&api, lost_effect).await["evidence"],
        json!([{"attempt": 1, "result": {"cents": 1100}}])
    );

    faulty.arm(1);
    let (status, unknown) = api.post(ADMIN, "destroy", &destroy_body(key, None)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{unknown}");
    assert_eq!(unknown["code"], json!("outcome_unknown"), "{unknown}");
    assert_eq!(unknown["retry"], json!("backoff"), "{unknown}");
    assert_eq!(unknown["ids"]["incarnation"], json!(1), "{unknown}");
    assert_eq!(faulty.lost(FaultKind::DuplicateDelivery), 1);

    let pinned = unknown["ids"]["incarnation"].as_u64().expect("incarnation");
    let settled = api
        .ok(ADMIN, "destroy", &destroy_body(key, Some(pinned)))
        .await;
    assert_eq!(settled["destroyed"], json!(false), "{settled}");
    assert_eq!(settled["incarnation"], json!(1), "{settled}");
    let (status, refused) = api
        .get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_agent_gone("state", status, &refused);
    let (status, refused) = api
        .get(CLIENT, &format!("effects?kind=order&key={key}"))
        .await;
    assert_agent_gone("effects", status, &refused);
    assert_eq!(
        audit_records(store.as_ref(), "agents_destroy").await.len(),
        1
    );

    let (_, late) = complete(&api, other_effect, 1, json!({"cents": 2100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    assert_eq!(
        dropped(&api.scrape().await),
        0,
        "o-lost's evidence was forgotten, so o-other's result takes the slot"
    );
    assert_eq!(
        inspect(&api, other_effect).await["evidence"],
        json!([{"attempt": 1, "result": {"cents": 2100}}])
    );
    running.shutdown().await.expect("clean shutdown");
}

/// A destroy whose tombstone write times out is `outcome_unknown` with retry
/// hint `backoff`, and the agent may still be active, so the evidence an
/// operator needs to reconcile its effect in doubt is kept: the agent's
/// state still reads, and inspect still lists the late result.
#[tokio::test]
async fn a_destroy_whose_head_write_times_out_keeps_the_effect_evidence() {
    let faulty = LoseNextHeadAck::new(ScriptedFault::Timeout);
    let store: Arc<dyn ObjectStoreBackend> = faulty.clone();
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let key = "o-timed-out";

    // A `manual` effect reconciled to attempt 2, attempt 1's result late,
    // and the effect in doubt again under a third owner.
    let (_, a) = begin_on(&api, key).await;
    let (status, a_step) = step(&api, &a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let b = take_over(&api, &a).await;
    let (_, refused) = step(&api, &b, "manual").await;
    assert_eq!(refused["code"], json!("effect_in_doubt"), "{refused}");
    api.ok(
        ADMIN,
        "reconcile",
        &json!({"effect": effect, "attempt": 1, "decision": "retry"}),
    )
    .await;
    let (_, handed) = step(&api, &b, "manual").await;
    assert_eq!(handed["attempt"], json!(2), "{handed}");
    let (_, late) = complete(&api, &effect, 1, json!({"cents": 3100})).await;
    assert_eq!(late["late"], json!(true), "{late}");
    let c = take_over(&api, &b).await;
    let (_, refused) = step(&api, &c, "manual").await;
    assert_eq!(refused["code"], json!("effect_in_doubt"), "{refused}");

    faulty.arm(1);
    let (status, unknown) = api.post(ADMIN, "destroy", &destroy_body(key, None)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{unknown}");
    assert_eq!(unknown["code"], json!("outcome_unknown"), "{unknown}");
    assert_eq!(unknown["retry"], json!("backoff"), "{unknown}");
    assert_eq!(unknown["ids"]["incarnation"], json!(1), "{unknown}");
    assert_eq!(faulty.lost(FaultKind::Timeout), 1);

    let state = api
        .ok_get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_eq!(state["incarnation"], json!(1), "{state}");
    let inspected = inspect(&api, &effect).await;
    assert_eq!(inspected["status"], json!("in_doubt"), "{inspected}");
    let evidence = inspected["evidence"].as_array().expect("evidence");
    assert_eq!(evidence.len(), 1, "{inspected}");
    let entry = evidence[0].as_object().expect("evidence entry");
    assert_eq!(entry.len(), 2, "attempt and result only: {inspected}");
    assert_eq!(entry["attempt"], json!(1), "{inspected}");
    assert_eq!(entry["result"], json!({"cents": 3100}), "{inspected}");
    running.shutdown().await.expect("clean shutdown");
}

/// The runtime runs a reconcile whose HEAD write has an unknown outcome
/// again, up to `UNKNOWN_OUTCOME_RETRIES` times, and a run that finds the
/// decision applied answers it. When every run times out the route answers
/// `outcome_unknown` with retry hint `reconcile`: the same decision again
/// settles it. Each run of the route audits before it writes, so the retry
/// is a second record.
#[tokio::test]
async fn a_reconcile_whose_head_write_times_out_answers_the_reconcile_hint() {
    let faulty = LoseNextHeadAck::new(ScriptedFault::Timeout);
    let store: Arc<dyn ObjectStoreBackend> = faulty.clone();
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let (_, assigned_a) = begin_on(&api, "o-lost-reconcile").await;
    let (status, a_step) = step(&api, &assigned_a, "manual").await;
    assert_eq!(status, StatusCode::OK, "{a_step}");
    let effect = a_step["effect"].as_str().expect("effect").to_string();
    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, refused) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));

    let reconcile = json!({
        "effect": effect,
        "attempt": 1,
        "decision": "record_result",
        "result": {"cents": 1500},
    });
    let writes = u64::from(UNKNOWN_OUTCOME_RETRIES) + 1;
    faulty.arm(writes);
    let (status, unknown) = api.post(ADMIN, "reconcile", &reconcile).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{unknown}");
    assert_eq!(unknown["code"], json!("outcome_unknown"), "{unknown}");
    assert_eq!(unknown["retry"], json!("reconcile"), "{unknown}");
    assert_eq!(faulty.lost(FaultKind::Timeout), writes);

    let settled = api.ok(ADMIN, "reconcile", &reconcile).await;
    assert_eq!(settled["status"], json!("completed"), "{settled}");
    assert_eq!(
        inspect(&api, &effect).await["result"],
        json!({"cents": 1500})
    );
    assert_eq!(reconcile_audit_records(store.as_ref()).await.len(), 2);
    running.shutdown().await.expect("clean shutdown");
}

/// A destroy whose audit record cannot be written (every audit data write
/// is refused, so the required-mode pipeline answers the submission with an
/// error) is `store_unavailable` and changes nothing: no HEAD publication,
/// the agent still active in incarnation 1, and no record.
#[tokio::test]
async fn a_destroy_whose_audit_record_is_refused_leaves_the_agent_active() {
    let audit_data = format!("t/{}/u/l0/", TenantId::new(TENANT).hash().to_hex());
    let faults = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("refused".to_string()))
                .with_key_contains(audit_data),
        ),
    ));
    let store: Arc<dyn ObjectStoreBackend> = faults.clone();
    let running = start_server(store.clone()).await;
    let api = Api::public(&running);
    let key = "o-unaudited";
    api.ok(CLIENT, "create", &json!({"agent": agent_keyed(key)}))
        .await;

    let before = api.scrape().await;
    let (status, refused) = api.post(ADMIN, "destroy", &destroy_body(key, None)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{refused}");
    assert_eq!(refused["code"], json!("store_unavailable"), "{refused}");
    assert!(faults.fault_count(Op::Put, FaultKind::Permanent) >= 1);
    let after = api.scrape().await;
    let publish = "phase=\"publish\"";
    assert_eq!(
        value(&after, "ravel_agents_store_cas_total", publish),
        value(&before, "ravel_agents_store_cas_total", publish)
    );
    let state = api
        .ok_get(CLIENT, &format!("state?kind=order&key={key}"))
        .await;
    assert_eq!(state["incarnation"], json!(1), "{state}");
    assert!(
        audit_records(store.as_ref(), "agents_destroy")
            .await
            .is_empty()
    );
    running.shutdown().await.expect("clean shutdown");
}
