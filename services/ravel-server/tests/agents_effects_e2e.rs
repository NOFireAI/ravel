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
use std::time::Duration;

use ravel_commit::{keys, record};
use ravel_logseg::{AttrValue, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_server::config::AgentsConfig;
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver};
use ravel_types::{Signal, TenantId};
use reqwest::StatusCode;
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const ADMIN: &str = "admin-token";
const PLAIN: &str = "plain-token";

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

/// The `--mode all` process `agents_e2e.rs` starts, over `store`, with the
/// audit pipeline installed in required mode.
async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents: agents_config(),
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

fn agent() -> Value {
    json!({"kind": "order", "key": "o-1"})
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
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let sent = api
        .ok(
            CLIENT,
            "send",
            &json!({"agent": agent(), "handler": "place", "request_id": "req-1", "payload": {"sku": "A-1"}}),
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

/// Every `kind=agents_reconcile` record in the tenant's audit shard (the
/// read `audit_pipeline_installed.rs::query_audit_records` does).
async fn reconcile_audit_records(store: &dyn ObjectStoreBackend) -> Vec<LogRecord> {
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
            if attr(&row, "kind") == Some("agents_reconcile") {
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
/// attempt 2 and runs the tool once more. A second retry while attempt 2 is
/// open is refused, and once attempt 2's result is recorded the turn commits
/// once.
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

    let assigned_b = take_over(&api, &assigned_a).await;
    let (status, refused) = step(&api, &assigned_b, "manual").await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));

    let retry = |attempt: u64| json!({"effect": effect, "attempt": attempt, "decision": "retry"});
    let reconciled = api.ok(ADMIN, "reconcile", &retry(1)).await;
    assert_eq!(reconciled["status"], json!("authorized"), "{reconciled}");
    assert_eq!(reconciled["attempt"], json!(2), "{reconciled}");
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
    let records = reconcile_audit_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(attr(&records[0], "agents.decision"), Some("retry"));
    assert_eq!(attr(&records[0], "agents.attempt"), Some("1"));
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
