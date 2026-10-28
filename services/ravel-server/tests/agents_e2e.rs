//! The agents client and worker routes (ADR-3331 decision 9, milestones 1 and
//! 2) through a real `ravel_server::start`, over `MemoryStore` and, for the
//! lost-acknowledgement case, a `FaultStore` around it.

#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::AgentsConfig;
use ravel_server::{FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver};
use ravel_types::TenantId;
use reqwest::StatusCode;
use serde_json::{Value, json};

const CLIENT: &str = "client-token";
const WORKER: &str = "worker-token";
const PLAIN: &str = "plain-token";
const MTLS_CLIENT: &str = "mtls-client-token";
const MTLS_WORKER: &str = "mtls-worker-token";
const MTLS_PLAIN: &str = "mtls-plain-token";

const LEASE_MS: u64 = 300;
const GRACE_MS: u64 = 100;
const BODY_LIMIT: usize = 16 * 1024;

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

fn principals(entries: [(&str, bool, bool); 3]) -> Arc<dyn ravel_query::http::TenantResolver> {
    let mut tokens = HashMap::new();
    for (token, agents, agents_worker) in entries {
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

/// The same `--mode all` process `agents_seam.rs` starts, over `store`, with
/// a client, a worker and a capability-less token on each listener, all for
/// tenant `acme`.
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    agents: AgentsConfig,
    query_concurrency_limit: ravel_query::QueryConcurrencyLimit,
) -> ravel_server::Running {
    let tenant_resolver = principals([
        (CLIENT, true, false),
        (WORKER, false, true),
        (PLAIN, false, false),
    ]);
    let mtls_resolver = principals([
        (MTLS_CLIENT, true, false),
        (MTLS_WORKER, false, true),
        (MTLS_PLAIN, false, false),
    ]);
    let config = ServerConfig {
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
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: Some(ravel_server::MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver: mtls_resolver,
        }),
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
        query_concurrency_limit,
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

async fn start_memory() -> ravel_server::Running {
    start_server(
        Arc::new(MemoryStore::new()),
        agents_config(),
        ravel_query::QueryConcurrencyLimit::Unlimited,
    )
    .await
}

/// A client bound to one listener's base URL.
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

    fn mtls(running: &ravel_server::Running) -> Self {
        let addr = running.mtls_addr.expect("an mTLS listener was configured");
        Api {
            http: reqwest::Client::new(),
            base: format!("http://{addr}"),
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

fn send_body(request_id: &str, payload: Value) -> Value {
    json!({"agent": agent(), "handler": "place", "request_id": request_id, "payload": payload})
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

fn commit_body(assignment: &Value, state: Value, output: Value, events: Value) -> Value {
    json!({
        "lease": assignment["lease"],
        "outcome": "completed",
        "state": state,
        "output": output,
        "events": events,
        "timers": [],
        "intents": [],
    })
}

/// Every sample line of `name` carrying `label`, e.g. `mode="all"`.
fn samples<'a>(scrape: &'a str, name: &str, label: &str) -> Vec<&'a str> {
    scrape
        .lines()
        .filter(|line| line.starts_with(&format!("{name}{{")) && line.contains(label))
        .collect()
}

/// The one sample of `name` carrying `label`, as an integer.
fn value(scrape: &str, name: &str, label: &str) -> u64 {
    let lines = samples(scrape, name, label);
    assert_eq!(lines.len(), 1, "{name} {label}: {lines:?}");
    lines[0]
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .expect("integer sample")
}

const ALL: &str = "mode=\"all\"";

/// The process's `mode="all"` sample of `name`.
async fn counter(api: &Api, name: &str) -> u64 {
    value(&api.scrape().await, name, ALL)
}

/// Scrape until the `mode="all"` sample of `name` reaches `want`, so a test
/// orders its next request after a state the server reports rather than
/// after a sleep.
async fn wait_for_counter(api: &Api, name: &str, want: u64) {
    for _ in 0..2_000 {
        if counter(api, name).await >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{name} never reached {want}");
}

/// The acceptance path. Worker A takes the turn and dies holding it; once
/// its lease and the grace have passed, worker B polls and is handed the same
/// turn id and input, A's late commit is refused on the lease's own fence,
/// and B's commit completes the operation exactly once.
///
/// HEAD publications: create is a `CreateIfAbsent` put, and the rest are CAS
/// writes: send 1, A's takeover 1, A's `BeginTurn` 1, B's takeover 1, B's
/// commit 1. B's `BeginTurn` finds the turn already active and resumes it
/// without a write, and A's refused commit publishes nothing. So 5 CAS under
/// `publish`: "send, two takeovers, one commit" plus A's `BeginTurn`.
#[tokio::test]
async fn send_then_result_through_real_server_survives_worker_replacement() {
    let running = start_memory().await;
    let api = Api::public(&running);

    let created = api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    assert_eq!(created["created"], json!(true));
    let sent = api
        .ok(CLIENT, "send", &send_body("req-1", json!({"sku": "A-1"})))
        .await;
    let operation = sent["operation"].as_str().expect("operation").to_string();
    assert_eq!(sent["duplicate"], json!(false));

    let worker_a = register(&api).await;
    let assigned_a = poll(&api, &worker_a, 2_000).await;
    assert_eq!(assigned_a["operation"], json!(operation));
    assert_eq!(assigned_a["lease"]["owner_id"], json!(worker_a));

    // A dies holding the turn: no heartbeat, no commit, until the lease and
    // the grace have both passed.
    tokio::time::sleep(Duration::from_millis(2 * (LEASE_MS + GRACE_MS))).await;

    let worker_b = register(&api).await;
    assert_ne!(worker_a, worker_b, "register mints a fresh id per process");
    let assigned_b = poll(&api, &worker_b, 2_000).await;
    assert_eq!(assigned_b["lease"]["owner_id"], json!(worker_b));
    assert_eq!(
        assigned_b["lease"]["turn_id"],
        assigned_a["lease"]["turn_id"]
    );
    assert_eq!(assigned_b["payload"], json!({"sku": "A-1"}));
    assert_eq!(assigned_b["payload"], assigned_a["payload"]);
    assert_eq!(assigned_b["operation"], json!(operation));
    assert_eq!(
        assigned_b["lease"]["recorded_unix_ns"],
        assigned_a["lease"]["recorded_unix_ns"]
    );

    // A wakes up and commits on its own lease: fenced out.
    let (status, refused) = api
        .post(
            WORKER,
            "worker/commit",
            &commit_body(
                &assigned_a,
                json!({"by": "a"}),
                json!({"by": "a"}),
                json!([]),
            ),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("ownership_lost"));
    // A's heartbeat on the same stale lease is refused the same way.
    let (status, refused) = api
        .post(
            WORKER,
            "worker/heartbeat",
            &json!({"lease": assigned_a["lease"]}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["code"], json!("ownership_lost"));

    // A result long-poll parked before B's commit is woken by the commit,
    // not by its periodic HEAD re-read or its deadline.
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
    assert_eq!(counter(&api, "ravel_agents_result_wakes_total").await, 0);

    let committed = api
        .ok(
            WORKER,
            "worker/commit",
            &commit_body(
                &assigned_b,
                json!({"by": "b"}),
                json!({"total_cents": 1299}),
                json!([]),
            ),
        )
        .await;
    assert_eq!(committed["operation"], json!(operation));
    assert_eq!(committed["status"], json!("completed"));

    let (status, answer) = waiter.await.expect("waiter joins");
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["operation"], json!(operation));
    assert_eq!(answer["result"]["status"], json!("completed"));
    assert_eq!(answer["result"]["output"], json!({"total_cents": 1299}));
    assert_eq!(counter(&api, "ravel_agents_result_wakes_total").await, 1);
    assert_eq!(
        counter(&api, "ravel_agents_result_wait_expiries_total").await,
        0
    );

    // Completed once: a second read sees the same single result.
    let (status, again) = api
        .get(CLIENT, &format!("result?operation={operation}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["result"], answer["result"]);
    let (status, state) = api.get(CLIENT, "state?kind=order&key=o-1").await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state["state"], json!({"by": "b"}));
    assert_eq!(state["state_version"], json!(1));

    let scrape = api.scrape().await;
    let all = ALL;
    assert_eq!(value(&scrape, "ravel_agents_turns_committed_total", all), 1);
    assert_eq!(value(&scrape, "ravel_agents_takeovers_total", all), 2);
    assert_eq!(
        value(&scrape, "ravel_agents_ownership_lost_refusals_total", all),
        2
    );
    let publish = "phase=\"publish\"";
    assert_eq!(value(&scrape, "ravel_agents_store_cas_total", publish), 5);
    assert_eq!(
        value(&scrape, "ravel_agents_store_conflicts_total", publish),
        0
    );
    for name in SETTINGS_GAUGES {
        assert_eq!(
            scrape.matches(&format!("# HELP {name} ")).count(),
            1,
            "{name}"
        );
        assert_eq!(
            scrape.matches(&format!("# TYPE {name} gauge\n")).count(),
            1,
            "{name}"
        );
        assert_eq!(samples(&scrape, name, all).len(), 1, "{name}");
    }
    for name in [
        "ravel_agents_turns_committed_total",
        "ravel_agents_takeovers_total",
    ] {
        assert_eq!(
            scrape.matches(&format!("# HELP {name} ")).count(),
            1,
            "{name}"
        );
        assert_eq!(
            scrape.matches(&format!("# TYPE {name} counter\n")).count(),
            1,
            "{name}"
        );
    }

    running.shutdown().await.expect("clean shutdown");
}

/// The ten settings gauges `agents_seam.rs` pins the values of.
const SETTINGS_GAUGES: [&str; 10] = [
    "ravel_agents_enabled",
    "ravel_agents_activation_concurrency_limit",
    "ravel_agents_poll_concurrency_limit",
    "ravel_agents_memory_budget_bytes",
    "ravel_agents_max_agents",
    "ravel_agents_max_mailbox",
    "ravel_agents_max_state_bytes",
    "ravel_agents_max_receipts",
    "ravel_agents_max_timers",
    "ravel_agents_sweep_interval_ms",
];

/// A client token without `agents` is refused on `send` and a token without
/// `agents_worker` on `worker/poll`, on the public and the mTLS listener
/// alike, each with a 403 naming the capability; the capable token on each
/// listener is served.
#[tokio::test]
async fn capabilities_are_enforced_on_both_listeners() {
    let running = start_memory().await;
    for (api, client, worker, plain) in [
        (Api::public(&running), CLIENT, WORKER, PLAIN),
        (Api::mtls(&running), MTLS_CLIENT, MTLS_WORKER, MTLS_PLAIN),
    ] {
        let base = api.base.clone();
        for token in [plain, worker] {
            let (status, body) = api
                .post(token, "send", &send_body("req-1", json!(null)))
                .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{base} {token}: {body}");
            assert!(
                body.to_string().contains("requires the agents capability"),
                "{base} {token}: {body}"
            );
        }
        for token in [plain, client] {
            let (status, body) = api
                .post(
                    token,
                    "worker/poll",
                    &json!({"worker_id": "w-1", "handlers": pins(), "wait_ms": 0}),
                )
                .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{base} {token}: {body}");
            assert!(
                body.to_string()
                    .contains("requires the agents_worker capability"),
                "{base} {token}: {body}"
            );
        }
        let (status, body) = api.post(client, "create", &json!({"agent": agent()})).await;
        assert_eq!(status, StatusCode::OK, "{base}: {body}");
        let (status, body) = api
            .post(
                worker,
                "worker/poll",
                &json!({"worker_id": "w-1", "handlers": pins(), "wait_ms": 0}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{base}: {body}");
    }
    running.shutdown().await.expect("clean shutdown");
}

/// Three events across two turns, read in pages of two after the cursor:
/// every event once, in order, then an empty page once the operation is
/// terminal (where the SDK iterator stops), and an empty page again when
/// resuming from the last cursor.
#[tokio::test]
async fn events_page_after_the_durable_cursor_without_duplicates() {
    let running = start_memory().await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let worker = register(&api).await;

    let mut last_operation = String::new();
    for (request_id, events) in [
        (
            "req-1",
            json!([
                {"event_type": "placed", "payload": {"n": 1}},
                {"event_type": "priced", "payload": {"n": 2}},
            ]),
        ),
        (
            "req-2",
            json!([{"event_type": "paid", "payload": {"n": 3}}]),
        ),
    ] {
        let sent = api
            .ok(CLIENT, "send", &send_body(request_id, json!({})))
            .await;
        let assignment = poll(&api, &worker, 2_000).await;
        assert_eq!(assignment["operation"], sent["operation"]);
        api.ok(
            WORKER,
            "worker/commit",
            &commit_body(&assignment, json!({}), json!(null), events),
        )
        .await;
        last_operation = sent["operation"].as_str().expect("operation").to_string();
    }

    let mut cursor = Value::Null;
    let mut seen = Vec::new();
    let mut pages = 0;
    loop {
        let page = api
            .ok(
                CLIENT,
                "events",
                &json!({"agent": agent(), "cursor": cursor, "limit": 2}),
            )
            .await;
        pages += 1;
        let events = page["events"].as_array().expect("events").clone();
        assert!(events.len() <= 2, "{page}");
        cursor = page["next_cursor"].clone();
        if events.is_empty() {
            let (_, op) = api
                .get(CLIENT, &format!("operation?operation={last_operation}"))
                .await;
            assert_eq!(op["status"], json!("completed"), "{op}");
            break;
        }
        for event in events {
            seen.push((event["event_seq"].clone(), event["payload"]["n"].clone()));
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(
        seen,
        vec![
            (json!(1), json!(1)),
            (json!(2), json!(2)),
            (json!(3), json!(3)),
        ]
    );
    assert_eq!(cursor, json!("3"));
    let resumed = api
        .ok(
            CLIENT,
            "events",
            &json!({"agent": agent(), "cursor": cursor, "limit": 2}),
        )
        .await;
    assert_eq!(resumed["events"], json!([]));
    assert_eq!(resumed["next_cursor"], json!("3"));

    let (status, body) = api
        .post(
            CLIENT,
            "events",
            &json!({"agent": agent(), "cursor": null, "limit": 0}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], json!("unsupported_format"));
    assert_eq!(body["reason"], json!("events limit must be at least 1"));
    running.shutdown().await.expect("clean shutdown");
}

/// A path under `/api/v1/agents` outside the route table is 404, an unmounted
/// timer path included, on both listeners, and with a capable credential.
#[tokio::test]
async fn a_path_outside_the_route_table_is_404() {
    let running = start_memory().await;
    for (api, client) in [
        (Api::public(&running), CLIENT),
        (Api::mtls(&running), MTLS_CLIENT),
    ] {
        for path in ["timers/schedule", "timers/fire", "no-such-route"] {
            let (status, body) = api.post(client, path, &json!({})).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "POST {path}: {body}");
            let (status, body) = api.get(client, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "GET {path}: {body}");
        }
    }
    running.shutdown().await.expect("clean shutdown");
}

/// A `send` body over `AgentsConfig::max_body_bytes` is a 413 carrying the typed
/// error body, and the store is never touched.
#[tokio::test]
async fn send_over_the_body_limit_is_413_with_a_typed_body() {
    let running = start_memory().await;
    let api = Api::public(&running);
    let payload = json!({"blob": "x".repeat(BODY_LIMIT)});
    let (status, body) = api.post(CLIENT, "send", &send_body("req-1", payload)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["code"], json!("budget_exceeded"));
    assert_eq!(body["retry"], json!("never"));
    let scrape = api.scrape().await;
    assert_eq!(
        value(
            &scrape,
            "ravel_agents_store_gets_total",
            "phase=\"resolve\""
        ),
        0
    );
    running.shutdown().await.expect("clean shutdown");
}

/// With one poll permit held by a waiting `worker/poll`, a second is refused
/// with 503 `store_unavailable`, while a PromQL query under a query
/// concurrency limit of one is still admitted: the agents route holds no
/// query permit. The holder is released by a send, not by its deadline.
#[tokio::test]
async fn admission_refusal_is_503_and_takes_no_query_permit() {
    let running = start_server(
        Arc::new(MemoryStore::new()),
        AgentsConfig {
            poll_concurrency: 1,
            ..agents_config()
        },
        ravel_query::QueryConcurrencyLimit::Bounded(1),
    )
    .await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let holder = {
        let api = api.clone();
        tokio::spawn(async move {
            api.post(
                WORKER,
                "worker/poll",
                &json!({"worker_id": "w-1", "handlers": pins(), "wait_ms": 15_000}),
            )
            .await
        })
    };
    // The coordinator counts a poll as waited only once it parks, and the
    // route took the permit before calling it.
    wait_for_counter(&api, "ravel_agents_polls_waited_total", 1).await;

    let (status, body) = api
        .post(
            WORKER,
            "worker/poll",
            &json!({"worker_id": "w-2", "handlers": pins(), "wait_ms": 0}),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], json!("store_unavailable"));
    assert_eq!(body["reason"], json!("agents admission limit reached"));

    let query = api
        .http
        .get(format!("{}/api/v1/query", api.base))
        .bearer_auth(CLIENT)
        .query(&[("query", "1")])
        .send()
        .await
        .expect("query completes");
    assert_eq!(
        query.status(),
        StatusCode::OK,
        "the one query permit is free while agents polls hold theirs: {}",
        query.text().await.unwrap_or_default()
    );

    let sent = api.ok(CLIENT, "send", &send_body("req-1", json!({}))).await;
    let (status, held) = holder.await.expect("holder joins");
    assert_eq!(status, StatusCode::OK, "{held}");
    assert_eq!(held["assignment"]["operation"], sent["operation"], "{held}");
    running.shutdown().await.expect("clean shutdown");
}

/// The acknowledgement of the send CAS is lost (the write applies), and the
/// coordinator's own settle cannot read HEAD back (three `NotFound` blips),
/// so `send` answers 500 `outcome_unknown` carrying the handle. `resolve`
/// with the same request id then finds the receipt: the same operation,
/// accepted.
#[tokio::test]
async fn resolve_after_a_lost_send_ack_returns_the_same_operation() {
    let kind = ravel_agents_core::keys::AgentKind::new("order").expect("kind");
    let key = ravel_agents_core::keys::AgentKey::new("o-1").expect("key");
    let head = format!(
        "{}/head",
        ravel_agents_core::identity::agent_id(&kind, &key).to_hex()
    );
    let plan = FaultPlan::empty()
        // create's put passes, send's CAS applies and loses its ack.
        .with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(head.clone())
                .then_passthrough()
                .then_fault(ScriptedFault::DuplicateDelivery),
        )
        // create's read and send's read pass, then the settle's three
        // re-reads blip.
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
        agents_config(),
        ravel_query::QueryConcurrencyLimit::Unlimited,
    )
    .await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;

    let (status, lost) = api
        .post(CLIENT, "send", &send_body("req-1", json!({"sku": "A-1"})))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{lost}");
    assert_eq!(lost["code"], json!("outcome_unknown"));
    assert_eq!(lost["retry"], json!("resolve_operation"));
    let operation = lost["ids"]["operation"]
        .as_str()
        .expect("handle")
        .to_string();
    assert_eq!(faults.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    assert_eq!(faults.fault_count(Op::Get, FaultKind::NotFoundBlip), 3);
    assert_eq!(faults.sequence_progress(0), 2);
    assert_eq!(faults.sequence_progress(1), 5);

    let resolved = api
        .ok(
            CLIENT,
            "resolve",
            &json!({"agent": agent(), "handler": "place", "request_id": "req-1"}),
        )
        .await;
    assert_eq!(resolved["operation"], json!(operation));
    assert_eq!(resolved["status"], json!("accepted"));
    running.shutdown().await.expect("clean shutdown");
}

/// An operation handle this tenant never issued answers 404 `agent_gone`
/// with the handle in the safe ids.
#[tokio::test]
async fn unknown_operation_handle_is_agent_gone_with_the_handle() {
    let running = start_memory().await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let sent = api.ok(CLIENT, "send", &send_body("req-1", json!({}))).await;
    let real = sent["operation"].as_str().expect("operation");
    // The same agent and incarnation at an accept_seq never issued.
    let handle = ravel_agents_core::state::Handle::decode(real).expect("handle decodes");
    let unknown =
        ravel_agents_core::state::Handle::operation(handle.agent, handle.incarnation, 99).encode();
    let (status, body) = api
        .get(CLIENT, &format!("operation?operation={unknown}"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], json!("agent_gone"));
    assert_eq!(body["ids"]["operation"], json!(unknown));
    assert_eq!(body["ids"]["agent_id"], json!(handle.agent.to_hex()));

    // A token that is not a handle is not echoed back.
    let (status, body) = api.get(CLIENT, "operation?operation=not-a-handle").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], json!("agent_gone"));
    assert!(!body.to_string().contains("not-a-handle"), "{body}");
    assert_eq!(body["ids"]["operation"], Value::Null);
    running.shutdown().await.expect("clean shutdown");
}

/// `call` waits until its `wait_ms` deadline and answers the operation's
/// status when nothing finished; `worker/heartbeat` moves the lease expiry
/// forward on each renewal; a repeated `call` under the same request id
/// answers the finished result without waiting; `cancel` stops an input no
/// worker has started.
#[tokio::test]
async fn call_heartbeat_and_cancel_through_real_server() {
    let running = start_memory().await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let call = json!({
        "agent": agent(),
        "handler": "place",
        "request_id": "req-1",
        "payload": {"sku": "A-1"},
        "wait_ms": 300,
    });

    let pending = api.ok(CLIENT, "call", &call).await;
    assert_eq!(pending["status"], json!("accepted"), "{pending}");
    assert_eq!(pending["result"], Value::Null);
    // The call parked and was answered by its deadline firing.
    assert_eq!(counter(&api, "ravel_agents_result_waits_total").await, 1);
    assert_eq!(
        counter(&api, "ravel_agents_result_wait_expiries_total").await,
        1
    );
    let operation = pending["operation"].clone();

    let worker = register(&api).await;
    let assignment = poll(&api, &worker, 2_000).await;
    assert_eq!(assignment["operation"], operation);
    let expiry = |body: &Value| -> i64 {
        body["lease_expires_unix_ns"]
            .as_str()
            .and_then(|v| v.parse().ok())
            .expect("expiry")
    };
    let lease = json!({"lease": assignment["lease"]});
    let first = expiry(&api.ok(WORKER, "worker/heartbeat", &lease).await);
    let second = expiry(&api.ok(WORKER, "worker/heartbeat", &lease).await);
    assert!(second > first, "{second} <= {first}");
    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(&assignment, json!({}), json!({"ok": true}), json!([])),
    )
    .await;

    let mut again = call.clone();
    again["wait_ms"] = json!(15_000);
    let done = api.ok(CLIENT, "call", &again).await;
    // Terminal at the first read: no second wait was parked.
    assert_eq!(counter(&api, "ravel_agents_result_waits_total").await, 1);
    assert_eq!(done["operation"], operation);
    assert_eq!(done["status"], json!("completed"));
    assert_eq!(done["result"]["output"], json!({"ok": true}));

    let sent = api.ok(CLIENT, "send", &send_body("req-2", json!({}))).await;
    let cancelled = api
        .ok(CLIENT, "cancel", &json!({"operation": sent["operation"]}))
        .await;
    assert_eq!(cancelled["operation"], sent["operation"]);
    let (status, op) = api
        .get(
            CLIENT,
            &format!(
                "operation?operation={}",
                sent["operation"].as_str().expect("operation")
            ),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{op}");
    assert_eq!(op["status"], json!("cancelled"), "{cancelled} {op}");
    running.shutdown().await.expect("clean shutdown");
}

/// `cancel` on an input a worker is running records the request and answers
/// `running`; the parked `result` long poll stays parked through that
/// non-terminal change, and the worker's commit then settles the input as
/// cancelled without applying its state.
#[tokio::test]
async fn cancel_of_a_running_turn_settles_it_cancelled_at_commit() {
    let running = start_memory().await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let sent = api
        .ok(CLIENT, "send", &send_body("req-1", json!({"sku": "A-1"})))
        .await;
    let operation = sent["operation"].as_str().expect("operation").to_string();
    let worker = register(&api).await;
    let assignment = poll(&api, &worker, 2_000).await;
    assert_eq!(assignment["operation"], json!(operation));

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

    let cancelled = api
        .ok(CLIENT, "cancel", &json!({"operation": operation}))
        .await;
    assert_eq!(cancelled["status"], json!("running"), "{cancelled}");
    assert_eq!(cancelled["requested"], json!(true), "{cancelled}");
    wait_for_counter(&api, "ravel_agents_result_wakes_total", 1).await;
    let (status, op) = api
        .get(CLIENT, &format!("operation?operation={operation}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{op}");
    assert_eq!(op["status"], json!("running"), "{op}");

    api.ok(
        WORKER,
        "worker/commit",
        &commit_body(
            &assignment,
            json!({"placed": true}),
            json!({"ok": true}),
            json!([]),
        ),
    )
    .await;
    let (status, answer) = waiter.await.expect("waiter joins");
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["result"]["status"], json!("cancelled"), "{answer}");
    assert_eq!(
        counter(&api, "ravel_agents_result_wait_expiries_total").await,
        0
    );
    let (status, state) = api.get(CLIENT, "state?kind=order&key=o-1").await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state["state"], Value::Null, "{state}");
    assert_eq!(state["state_version"], json!(0), "{state}");
    running.shutdown().await.expect("clean shutdown");
}

/// A HEAD read that finds nothing while `result` holds an operation handle
/// is a failed read, not a deleted agent, since a HEAD is never deleted:
/// 500 `outcome_unknown` carrying the handle, and the next read answers.
#[tokio::test]
async fn a_not_found_blip_under_a_handle_is_outcome_unknown_not_agent_gone() {
    let kind = ravel_agents_core::keys::AgentKind::new("order").expect("kind");
    let key = ravel_agents_core::keys::AgentKey::new("o-1").expect("key");
    let head = format!(
        "{}/head",
        ravel_agents_core::identity::agent_id(&kind, &key).to_hex()
    );
    // create's read and send's read pass; every attempt of result's read
    // (the store confirms an absence over `MAX_READ_ATTEMPTS` reads) blips.
    let blips = u64::from(ravel_agents_store::publish::MAX_READ_ATTEMPTS);
    let mut sequence = Sequence::new(Op::Get)
        .with_key_contains(head)
        .then_passthrough()
        .then_passthrough();
    for _ in 0..blips {
        sequence = sequence.then_fault(ScriptedFault::NotFoundBlip);
    }
    let plan = FaultPlan::empty().with_sequence(sequence);
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let running = start_server(
        faults.clone(),
        agents_config(),
        ravel_query::QueryConcurrencyLimit::Unlimited,
    )
    .await;
    let api = Api::public(&running);
    api.ok(CLIENT, "create", &json!({"agent": agent()})).await;
    let sent = api.ok(CLIENT, "send", &send_body("req-1", json!({}))).await;
    let operation = sent["operation"].as_str().expect("operation").to_string();

    let (status, body) = api
        .get(CLIENT, &format!("result?operation={operation}"))
        .await;
    assert_eq!(faults.fault_count(Op::Get, FaultKind::NotFoundBlip), blips);
    assert_eq!(faults.sequence_progress(0), 2 + blips);
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["code"], json!("outcome_unknown"), "{body}");
    assert_eq!(body["retry"], json!("resolve_operation"), "{body}");
    assert_eq!(body["ids"]["operation"], json!(operation), "{body}");

    let (status, body) = api
        .get(CLIENT, &format!("operation?operation={operation}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("accepted"), "{body}");
    running.shutdown().await.expect("clean shutdown");
}
