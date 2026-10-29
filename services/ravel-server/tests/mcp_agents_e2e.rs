//! The Ravel Agents MCP tools (ADR-3331 decision 10, issue #3357) through a
//! real `ravel_server::start` with both `--mcp` and `--agents` on, over
//! `MemoryStore` and, for the lost-acknowledgement case, a `FaultStore`
//! around it. Every tool call goes through an rmcp client to `POST /mcp`, and
//! every agents tool reaches the agents route handlers in process, so the
//! capability check a tool call meets is the route's own.

#![cfg(all(feature = "mcp", feature = "agents"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{AgentsConfig, McpConfig};
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver};
use ravel_types::TenantId;
use reqwest::StatusCode;
use rmcp::model::{CallToolRequestParams, ListToolsResult, ProtocolVersion};
use rmcp::service::{ClientLifecycleMode, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientServiceExt, RoleClient};
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const PLAIN: &str = "plain-token";

const LEASE_MS: u64 = 300;
const GRACE_MS: u64 = 100;

/// The D2 budget a `tools/list` answer must stay inside.
const TOOLS_LIST_BUDGET: usize = 24_576;
/// The nine-tool `tools/list` answer, as `ravel-mcp`'s catalog pin measures it.
const TOOLS_LIST_BYTES: usize = 15_700;
/// The nine tools plus the six agents tools.
const TOOLS_LIST_WITH_AGENTS_BYTES: usize = 23_589;

const INPUT_HASH: &str = "5892e7a2b7b5a3aa11b9984ec594f8eb935e25f2f1a69ce17c2387d2bb1c74f0";

/// No periodic sweep, so the only store requests a test sees are the ones
/// its own calls issue.
fn agents_config(enabled: bool) -> AgentsConfig {
    AgentsConfig {
        enabled,
        activation_concurrency: 16,
        poll_concurrency: 16,
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        sweep_interval_ms: 0,
        ..AgentsConfig::default()
    }
}

fn principals() -> Arc<dyn ravel_query::http::TenantResolver> {
    let mut tokens = HashMap::new();
    for (token, agents, agents_worker) in [
        (CLIENT, true, false),
        (WORKER, false, true),
        (PLAIN, false, false),
    ] {
        tokens.insert(
            token.to_string(),
            Principal {
                agents,
                agents_worker,
                ..Principal::tenant_only(TenantId::new("acme"))
            },
        );
    }
    Arc::new(StaticBearerTokenResolver::with_principals(tokens))
}

/// The `--mode all` process with `--mcp` on and `--agents` as `agents` says.
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<ravel_object_store::StoreMetrics>,
    agents: AgentsConfig,
) -> ravel_server::Running {
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents,
            mcp: McpConfig {
                enabled: true,
                allowed_origins: vec!["https://console.example".to_string()],
                max_body_bytes: 16 * 1024,
            },
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
        // Pushed past any test's lifetime so the store-call pins count only
        // what the tool calls issue.
        store_probe_interval: Duration::from_secs(3600),
        admission_reconcile_interval: Duration::from_secs(3600),
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
    ravel_server::start(config, store.clone(), store, store_metrics, None)
        .await
        .expect("server starts")
}

/// A `MemoryStore` behind the instrumentation decorator the store-call pins
/// read.
fn instrumented_memory() -> (
    Arc<dyn ObjectStoreBackend>,
    Arc<ravel_object_store::StoreMetrics>,
) {
    let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
    let store = Arc::new(ravel_object_store::InstrumentedStore::with_metrics(
        MemoryStore::new(),
        Arc::clone(&metrics),
    ));
    (store, metrics)
}

async fn start_memory(enabled: bool) -> ravel_server::Running {
    let (store, metrics) = instrumented_memory();
    start_server(store, metrics, agents_config(enabled)).await
}

/// Completed store calls by operation kind: put, get, head, list,
/// delimited list, delete.
fn store_calls(metrics: &ravel_object_store::StoreMetrics) -> [u64; 6] {
    let snapshot = metrics.snapshot();
    [
        snapshot.put.calls,
        snapshot.get.calls,
        snapshot.head.calls,
        snapshot.list.calls,
        snapshot.list_delimited.calls,
        snapshot.delete.calls,
    ]
}

/// The store calls `step` issues, by operation kind.
async fn store_calls_of<F: std::future::Future>(
    metrics: &ravel_object_store::StoreMetrics,
    step: F,
) -> (F::Output, [u64; 6]) {
    let before = store_calls(metrics);
    let output = step.await;
    let after = store_calls(metrics);
    (output, std::array::from_fn(|i| after[i] - before[i]))
}

type Client = RunningService<RoleClient, ()>;

/// An rmcp client on the current revision, carrying `token`.
async fn mcp(running: &ravel_server::Running, token: &str) -> Client {
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{}/mcp", running.http_addr))
            .auth_header(token),
    );
    ().serve_with_lifecycle(
        transport,
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        },
    )
    .await
    .expect("the client reaches the server")
}

/// One tool call, answering `isError` and the envelope.
async fn call(client: &Client, name: &str, arguments: Value) -> (bool, Value) {
    let Value::Object(arguments) = arguments else {
        panic!("tool arguments are an object");
    };
    let result = client
        .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(arguments))
        .await
        .expect("the call answers a tool result");
    let envelope = result
        .structured_content
        .clone()
        .expect("the tool result carries the envelope");
    (result.is_error == Some(true), envelope)
}

/// The envelope's one row, by column name.
fn row(envelope: &Value) -> HashMap<String, Value> {
    let columns = envelope["data"]["columns"].as_array().expect("columns");
    let rows = envelope["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{envelope}");
    columns
        .iter()
        .zip(rows[0].as_array().expect("row"))
        .map(|(column, cell)| {
            (
                column["name"].as_str().expect("name").to_string(),
                cell.clone(),
            )
        })
        .collect()
}

fn agent_args() -> Value {
    json!({"kind": "order", "key": "o-1"})
}

/// The store key of the `agent_args()` agent's HEAD object.
fn head_key() -> String {
    let kind = ravel_agents_core::keys::AgentKind::new("order").expect("kind");
    let key = ravel_agents_core::keys::AgentKey::new("o-1").expect("key");
    format!(
        "{}/head",
        ravel_agents_core::identity::agent_id(&kind, &key).to_hex()
    )
}

fn send_args(idempotency_key: &str) -> Value {
    json!({
        "kind": "order",
        "key": "o-1",
        "handler": "place",
        "payload": {"sku": "A-1"},
        "idempotency_key": idempotency_key,
    })
}

/// A plain HTTP client for the agents routes MCP does not carry: create and
/// the worker protocol.
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

    async fn post(&self, token: &str, path: &str, body: &Value) -> (StatusCode, Value) {
        let response = self
            .http
            .post(format!("{}/api/v1/agents/{path}", self.base))
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .expect("request completes");
        let status = response.status();
        let text = response.text().await.expect("response body");
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn ok(&self, token: &str, path: &str, body: &Value) -> Value {
        let (status, body) = self.post(token, path, body).await;
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
        assert_eq!(response.status(), StatusCode::OK);
        response.json().await.expect("json body")
    }

    async fn create(&self) {
        self.ok(CLIENT, "create", &json!({"agent": agent_args()}))
            .await;
    }

    async fn register(&self) -> String {
        let body = self
            .ok(WORKER, "worker/register", &json!({"handlers": pins()}))
            .await;
        body["worker_id"].as_str().expect("worker id").to_string()
    }

    async fn poll(&self, worker_id: &str) -> Value {
        let body = self
            .ok(
                WORKER,
                "worker/poll",
                &json!({"worker_id": worker_id, "handlers": pins(), "wait_ms": 2_000}),
            )
            .await;
        body["assignment"].clone()
    }

    /// The `phase="publish"` sample of the agents store CAS counter.
    async fn publish_cas(&self) -> u64 {
        let scrape = self
            .http
            .get(format!("{}/metrics", self.base))
            .send()
            .await
            .expect("scrape completes")
            .text()
            .await
            .expect("scrape body");
        let lines: Vec<&str> = scrape
            .lines()
            .filter(|line| {
                line.starts_with("ravel_agents_store_cas_total{")
                    && line.contains("phase=\"publish\"")
                    && line.contains("mode=\"all\"")
            })
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        lines[0]
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .expect("integer sample")
    }
}

fn pins() -> Value {
    json!([{"handler": "place", "version": "1.0.0", "digest": "01".repeat(32)}])
}

fn commit_body(assignment: &Value, state: Value, events: Value) -> Value {
    json!({
        "lease": assignment["lease"],
        "outcome": "completed",
        "state": state,
        "output": null,
        "events": events,
        "timers": [],
        "intents": [],
    })
}

/// The acceptance test. A token whose principal lacks the agents capability
/// is refused by the send route's own check and publishes nothing; with the
/// capability, the same idempotency key twice answers the same operation and
/// the inbox holds one input (one publish CAS); a different key is a second
/// operation.
#[tokio::test]
async fn ravel_agent_send_with_an_idempotency_key_requires_the_agents_capability_and_returns_the_same_operation_twice()
 {
    let running = start_memory(true).await;
    let api = Api::new(&running);
    api.create().await;
    let plain = mcp(&running, PLAIN).await;
    let client = mcp(&running, CLIENT).await;

    let (is_error, refused) = call(&plain, "ravel_agent_send", send_args("k-1")).await;
    assert!(is_error, "{refused}");
    assert_eq!(refused["status"], json!("error"));
    assert_eq!(refused["failure"]["class"], json!("unauthorized"));
    let message = refused["failure"]["message"].as_str().expect("message");
    assert!(message.contains("agents capability"), "{message}");
    assert_eq!(api.publish_cas().await, 0);

    let (is_error, first) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(!is_error, "{first}");
    let first = row(&first);
    assert_eq!(first["duplicate"], json!(false));
    let (is_error, second) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(!is_error, "{second}");
    let second = row(&second);
    assert_eq!(second["operation"], first["operation"]);
    assert_eq!(second["accept_seq"], first["accept_seq"]);
    assert_eq!(second["duplicate"], json!(true));

    let (_, state) = call(&client, "ravel_agent_state", agent_args()).await;
    let state = row(&state);
    let mailbox = state["mailbox"]["items"].as_array().expect("mailbox items");
    assert_eq!(mailbox.len(), 1, "{mailbox:?}");
    assert_eq!(api.publish_cas().await, 1);

    let (is_error, other) = call(&client, "ravel_agent_send", send_args("k-2")).await;
    assert!(!is_error, "{other}");
    let other = row(&other);
    assert_ne!(other["operation"], first["operation"]);
    assert_eq!(other["duplicate"], json!(false));
    let (_, state) = call(&client, "ravel_agent_state", agent_args()).await;
    assert_eq!(
        row(&state)["mailbox"]["items"]
            .as_array()
            .expect("mailbox items")
            .len(),
        2
    );
    assert_eq!(api.publish_cas().await, 2);

    // Every agents tool meets the same check, not only send.
    for (name, args) in [
        ("ravel_agent_describe", agent_args()),
        ("ravel_agent_send", send_args("k-3")),
        (
            "ravel_agent_operation",
            json!({"operation": first["operation"]}),
        ),
        ("ravel_agent_state", agent_args()),
        ("ravel_agent_events", agent_args()),
        ("ravel_agent_blocked_effects", agent_args()),
    ] {
        let (is_error, refused) = call(&plain, name, args).await;
        assert!(is_error, "{name}: {refused}");
        assert_eq!(refused["failure"]["class"], json!("unauthorized"), "{name}");
    }
    assert_eq!(api.publish_cas().await, 2);

    plain.cancel().await.expect("client stops");
    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

fn serialized(tools: Vec<rmcp::model::Tool>) -> Vec<u8> {
    serde_json::to_vec(&ListToolsResult::with_all_items(tools)).expect("tools/list serializes")
}

/// Without `--agents` the answer is the nine-tool catalog byte for byte, and
/// an agents tool name is not routed at all.
#[tokio::test]
async fn tools_list_without_agents_is_the_pinned_catalog_byte_for_byte() {
    let running = start_memory(false).await;
    let client = mcp(&running, CLIENT).await;

    let listed = serialized(client.list_all_tools().await.expect("tools/list answers"));
    assert_eq!(listed, serialized(ravel_mcp::catalog::tool_catalog()));
    assert_eq!(listed.len(), TOOLS_LIST_BYTES);

    let Value::Object(arguments) = send_args("k-1") else {
        unreachable!("send_args is an object")
    };
    let refused = client
        .call_tool(CallToolRequestParams::new("ravel_agent_send").with_arguments(arguments))
        .await
        .expect_err("an agents tool is not routed without --agents");
    assert!(
        refused.to_string().contains("ravel_agent_send"),
        "{refused}"
    );

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// With both flags the answer is the nine tools then the six agents tools, at
/// the measured size, inside the D2 budget.
#[tokio::test]
async fn tools_list_with_agents_adds_six_tools_inside_the_budget() {
    let running = start_memory(true).await;
    let client = mcp(&running, CLIENT).await;

    let tools = client.list_all_tools().await.expect("tools/list answers");
    let names: Vec<String> = tools.iter().map(|tool| tool.name.to_string()).collect();
    assert_eq!(names.len(), 15);
    assert_eq!(
        names[9..],
        ravel_mcp::tools::agents::AGENTS_TOOLS.map(str::to_string)
    );
    let mut expected = ravel_mcp::catalog::tool_catalog();
    expected.extend(ravel_mcp::catalog::agents_tool_catalog());
    let listed = serialized(tools);
    assert_eq!(listed, serialized(expected));
    assert_eq!(listed.len(), TOOLS_LIST_WITH_AGENTS_BYTES);
    assert!(listed.len() < TOOLS_LIST_BUDGET);

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// Three events, read two per page: the first page is an `ok_page` whose
/// cursor resumes after event 2, and the second carries event 3 alone and
/// names the cursor after it.
#[tokio::test]
async fn ravel_agent_events_pages_with_the_cursor() {
    let running = start_memory(true).await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;
    let worker = api.register().await;

    let (_, sent) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    let operation = row(&sent)["operation"].clone();
    let assignment = api.poll(&worker).await;
    assert_eq!(assignment["operation"], operation);
    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(
            &assignment,
            json!({"placed": true}),
            json!([
                {"event_type": "placed", "payload": {"n": 1}},
                {"event_type": "priced", "payload": {"n": 2}},
                {"event_type": "paid", "payload": {"n": 3}},
            ]),
        ),
    )
    .await;

    let (is_error, first) = call(
        &client,
        "ravel_agent_events",
        json!({"kind": "order", "key": "o-1", "max_rows": 2}),
    )
    .await;
    assert!(!is_error, "{first}");
    assert_eq!(first["status"], json!("ok_page"), "{first}");
    assert_eq!(first["presentation"]["row_cap_hit"], json!(true));
    assert_eq!(first["presentation"]["cursor"], json!("2"));
    let seqs = |envelope: &Value| -> Vec<Value> {
        envelope["data"]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| row[0].clone())
            .collect()
    };
    assert_eq!(seqs(&first), vec![json!("1"), json!("2")]);

    let (is_error, second) = call(
        &client,
        "ravel_agent_events",
        json!({"kind": "order", "key": "o-1", "max_rows": 2, "cursor": "2"}),
    )
    .await;
    assert!(!is_error, "{second}");
    assert_eq!(second["status"], json!("ok"), "{second}");
    assert_eq!(seqs(&second), vec![json!("3")]);
    assert_eq!(second["data"]["rows"][0][3], json!("paid"));
    assert_eq!(second["data"]["rows"][0][4], json!({"n": 3}));
    assert_eq!(second["next_steps"][0]["action"], json!("resume_events"));
    assert_eq!(
        second["next_steps"][0]["detail"],
        json!("ravel_agent_events with cursor 3 returns events after this page")
    );

    // The state the commit wrote, at the version the route reports.
    let (is_error, state) = call(&client, "ravel_agent_state", agent_args()).await;
    assert!(!is_error, "{state}");
    let state = row(&state);
    let route = api.get(CLIENT, "state?kind=order&key=o-1").await;
    let version = route["state_version"].as_u64().expect("state version");
    assert!(version >= 1, "{route}");
    assert_eq!(state["state_version"], json!(version.to_string()));
    assert_eq!(state["state"], json!({"placed": true}));
    assert_eq!(state["mailbox"], json!({"items": []}));

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// A manual-policy effect left in doubt by a dead worker blocks the turn:
/// `ravel_agent_operation` reports it blocked on that effect and points at
/// `ravel_agent_blocked_effects`, which lists the effect with the inspector's
/// unblocking text and the two reconcile requests.
#[tokio::test]
async fn ravel_agent_blocked_effects_lists_an_effect_in_doubt_with_the_inspector_text() {
    let running = start_memory(true).await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;

    let (_, sent) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    let operation = row(&sent)["operation"].clone();
    let worker_a = api.register().await;
    let assigned_a = api.poll(&worker_a).await;
    assert_eq!(assigned_a["operation"], operation);
    let step = |assigned: &Value| {
        json!({
            "lease": assigned["lease"],
            "step_key": "price",
            "tool_name": "catalog.price",
            "tool_version": "1",
            "policy": "manual",
            "input_hash": INPUT_HASH,
        })
    };
    let a_step = api.ok(WORKER, "worker/step", &step(&assigned_a)).await;
    let effect = a_step["effect"].clone();

    // A dies holding the turn; B takes it over and its replay of the step is
    // refused in doubt.
    tokio::time::sleep(Duration::from_millis(2 * (LEASE_MS + GRACE_MS))).await;
    let worker_b = api.register().await;
    let assigned_b = api.poll(&worker_b).await;
    assert_eq!(
        assigned_b["lease"]["turn_id"],
        assigned_a["lease"]["turn_id"]
    );
    let (status, refused) = api.post(WORKER, "worker/step", &step(&assigned_b)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("effect_in_doubt"));

    let (is_error, op) = call(
        &client,
        "ravel_agent_operation",
        json!({"operation": operation}),
    )
    .await;
    assert!(!is_error, "{op}");
    assert_eq!(op["next_steps"][0]["action"], json!("read_blocked_effects"));
    let detail = op["next_steps"][0]["detail"].as_str().expect("detail");
    let effect_id = effect.as_str().expect("effect id");
    assert!(detail.contains(effect_id), "{detail}");
    let op = row(&op);
    assert_eq!(op["status"], json!("blocked"));
    assert_eq!(op["blocked_effect"], effect);

    let (is_error, blocked) = call(&client, "ravel_agent_blocked_effects", agent_args()).await;
    assert!(!is_error, "{blocked}");
    assert_eq!(blocked["status"], json!("ok"));
    let blocked = row(&blocked);
    assert_eq!(blocked["effect"], effect);
    assert_eq!(blocked["turn_id"], assigned_a["lease"]["turn_id"]);
    assert_eq!(blocked["tool_name"], json!("catalog.price"));
    assert_eq!(blocked["policy"], json!("manual"));
    assert_eq!(blocked["attempt"], json!("1"));
    let unblock = &blocked["unblock"];
    assert_eq!(unblock["reason"], json!("EffectInDoubt"));
    let action = unblock["action"].as_str().expect("action");
    assert_eq!(
        action,
        "effect catalog.price attempt 1 may or may not have run. An agents_admin caller \
         sends POST /api/v1/agents/reconcile with one of:"
    );
    // The inspector page renders the same sentence for the same effect.
    let inspector = include_str!("../src/agents/inspector.html");
    assert!(inspector.contains("\" may or may not have run. \" +"));
    assert!(
        inspector.contains(
            "\"An agents_admin caller sends POST /api/v1/agents/reconcile with one of:\""
        )
    );
    assert_eq!(
        unblock["requests"],
        json!([
            {"effect": effect, "attempt": 1, "decision": "retry"},
            {
                "effect": effect,
                "attempt": 1,
                "decision": "record_result",
                "result": "<the verified result>",
            },
        ])
    );

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// Store requests per tool call, through the whole MCP and agents stack: a
/// first send, the same send again, and an operation read.
#[tokio::test]
async fn send_and_operation_store_calls_are_pinned() {
    let (store, metrics) = instrumented_memory();
    let running = start_server(store, Arc::clone(&metrics), agents_config(true)).await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;

    let ((_, sent), first_send) = store_calls_of(
        &metrics,
        call(&client, "ravel_agent_send", send_args("k-1")),
    )
    .await;
    let operation = row(&sent)["operation"].clone();
    let (_, repeated_send) = store_calls_of(
        &metrics,
        call(&client, "ravel_agent_send", send_args("k-1")),
    )
    .await;
    let ((is_error, op), operation_read) = store_calls_of(
        &metrics,
        call(
            &client,
            "ravel_agent_operation",
            json!({"operation": operation}),
        ),
    )
    .await;
    assert!(!is_error, "{op}");

    // put, get, head, list, delimited list, delete. The first send reads
    // HEAD, publishes it by CAS and writes one advisory object; the repeat
    // finds its receipt on the HEAD it reads and writes nothing; an operation
    // read is one HEAD read.
    assert_eq!(first_send, [2, 1, 0, 0, 0, 0], "first send");
    assert_eq!(repeated_send, [0, 1, 0, 0, 0, 0], "repeated send");
    assert_eq!(operation_read, [0, 1, 0, 0, 0, 0], "operation");

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// The send CAS applies and its acknowledgement is lost, and the settle
/// cannot read HEAD back: `ravel_agent_send` answers an `internal` envelope
/// naming `outcome_unknown` and the resend step. Resending with the same
/// idempotency key answers the operation the lost write accepted, as a
/// duplicate, and publishes nothing more.
#[tokio::test]
async fn a_lost_send_ack_resent_with_the_same_key_answers_the_accepted_operation() {
    let head = head_key();
    let plan = FaultPlan::empty()
        .with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(head.clone())
                .then_passthrough()
                .then_fault(ScriptedFault::DuplicateDelivery),
        )
        .with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(head)
                .then_passthrough()
                .then_passthrough()
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_fault(ScriptedFault::NotFoundBlip),
        );
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let running = start_server(
        faults.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        agents_config(true),
    )
    .await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;

    let (is_error, lost) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(is_error, "{lost}");
    assert_eq!(lost["failure"]["class"], json!("internal"));
    let message = lost["failure"]["message"].as_str().expect("message");
    assert!(message.starts_with("outcome_unknown: "), "{message}");
    assert_eq!(
        lost["next_steps"][0]["action"],
        json!("resend_with_the_same_key")
    );
    assert_eq!(faults.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(faults.fault_count(Op::Get, FaultKind::NotFoundBlip), 3);
    assert_eq!(faults.sequence_progress(0), 2);
    assert_eq!(faults.sequence_progress(1), 5);
    let published = api.publish_cas().await;

    let (is_error, resent) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(!is_error, "{resent}");
    let resent = row(&resent);
    assert_eq!(resent["duplicate"], json!(true));
    let (_, op) = call(
        &client,
        "ravel_agent_operation",
        json!({"operation": resent["operation"]}),
    )
    .await;
    assert_eq!(row(&op)["status"], json!("accepted"));
    let (_, state) = call(&client, "ravel_agent_state", agent_args()).await;
    assert_eq!(
        row(&state)["mailbox"]["items"]
            .as_array()
            .expect("mailbox items")
            .len(),
        1
    );
    assert_eq!(api.publish_cas().await, published);

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// A `FaultStore` with no scripted faults, so the only thing slowing a call
/// down is a hold gate the test arms.
async fn start_faulted() -> (Arc<FaultStore<MemoryStore>>, ravel_server::Running) {
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let running = start_server(
        faults.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        agents_config(true),
    )
    .await;
    (faults, running)
}

/// One tool call that must end on its own effective deadline. The outer
/// bound is far above every deadline these tests set; it only turns a call
/// that never ends into a failure instead of a hung test.
async fn call_ending_by_deadline(client: &Client, name: &str, arguments: Value) -> (bool, Value) {
    tokio::time::timeout(Duration::from_secs(30), call(client, name, arguments))
        .await
        .expect("the call ends at its effective deadline")
}

/// An agents read whose route is held at the store past the call's
/// `deadline_ms` answers the `deadline` class with that deadline as the
/// effective one, and the same read under the default deadline succeeds.
#[tokio::test]
async fn an_agents_read_held_past_its_deadline_answers_the_deadline_class() {
    let (faults, running) = start_faulted().await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;
    let (is_error, sent) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(!is_error, "{sent}");
    let operation = row(&sent)["operation"].clone();

    // The operation read is one HEAD read; the gate holds the first one
    // after it is armed and lets every later one through.
    let gate = faults.hold(Op::Get, Some(head_key()), Occurrence::Nth(1));
    let (is_error, late) = call_ending_by_deadline(
        &client,
        "ravel_agent_operation",
        json!({"operation": operation, "deadline_ms": 50}),
    )
    .await;
    assert!(is_error, "{late}");
    assert_eq!(late["status"], json!("error"));
    assert_eq!(late["failure"]["class"], json!("deadline"), "{late}");
    assert_eq!(late["budget"]["effective"]["deadline_ms"], json!(50));
    // The route reached the store and was still held there when the call
    // ended, so the deadline, not the route, ended it.
    let held = gate.held();
    assert_eq!(held.len(), 1);
    for id in held {
        gate.release(id);
    }

    let (is_error, op) = call(
        &client,
        "ravel_agent_operation",
        json!({"operation": operation}),
    )
    .await;
    assert!(!is_error, "{op}");
    assert_eq!(row(&op)["status"], json!("accepted"));

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}

/// A send whose HEAD write is held past the call's `deadline_ms` answers the
/// `deadline` class, says its outcome is unknown, and carries the resend
/// step. The held write never applied, so the resend with the same key under
/// the default deadline accepts the input once, not as a duplicate.
#[tokio::test]
async fn a_send_held_past_its_deadline_reports_an_unknown_outcome_and_the_resend_step() {
    let (faults, running) = start_faulted().await;
    let api = Api::new(&running);
    api.create().await;
    let client = mcp(&running, CLIENT).await;

    let gate = faults.hold(Op::Put, Some(head_key()), Occurrence::Nth(1));
    let mut args = send_args("k-1");
    args["deadline_ms"] = json!(50);
    let (is_error, late) = call_ending_by_deadline(&client, "ravel_agent_send", args).await;
    assert!(is_error, "{late}");
    assert_eq!(late["failure"]["class"], json!("deadline"), "{late}");
    let message = late["failure"]["message"].as_str().expect("message");
    assert!(message.contains("outcome is unknown"), "{message}");
    assert_eq!(
        late["next_steps"][0]["action"],
        json!("resend_with_the_same_key")
    );
    let held = gate.held();
    assert_eq!(held.len(), 1);
    for id in held {
        gate.release(id);
    }

    let (is_error, resent) = call(&client, "ravel_agent_send", send_args("k-1")).await;
    assert!(!is_error, "{resent}");
    assert_eq!(row(&resent)["duplicate"], json!(false));

    client.cancel().await.expect("client stops");
    running.shutdown().await.expect("clean shutdown");
}
