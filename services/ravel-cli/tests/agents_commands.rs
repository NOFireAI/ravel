//! `ravel-cli agents` (ADR-3331 decision 11, issue #3355). The read commands
//! run in-process against a `MemoryStore` seeded through `AgentStore`, with no
//! server anywhere in the test; `dev` runs against a recording host so the
//! order of its side effects is observable; `reconcile` runs against a
//! loopback axum server that records what the client sent.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use bytes::Bytes;
use ravel_agents_core::command::{
    BeginTurn, Command, CompleteTurn, EventSpec, HandlerPin, IncarnationFence, OwnerFence,
    ScheduleTimer, Takeover, TimerSpec, TurnFence,
};
use ravel_agents_core::error::ErrorKind;
use ravel_agents_core::head::ObjectRef;
use ravel_agents_core::identity::{AgentId, Hash256, agent_id};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::wire::{
    EffectsResponse, EventsResponse, OperationResponse, OperationStatus, ReconcileRequest,
    ReconcileResponse, StateResponse,
};
use ravel_agents_store::{AgentStore, Clock, StoreLimits};
use ravel_cli::agents::{
    self, AgentsCliError, DevHost, DevOptions, InspectOutput, LIST_LIMIT_CAP, ListOutput,
    ReconcileChoice,
};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::TenantHash;
use serde_json::{Value, json};

const TENANT: TenantHash = TenantHash([7; 16]);
const NOW: i64 = 1_800_000_000_000_000_000;
const SECOND: i64 = 1_000_000_000;

struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ns(&self) -> i64 {
        NOW
    }
}

fn writer(backend: Arc<dyn ObjectStoreBackend>) -> AgentStore {
    AgentStore::new(
        backend,
        TENANT,
        StoreLimits::default(),
        Arc::new(FixedClock),
        Arc::new(DerivedIds),
    )
}

fn kind() -> AgentKind {
    AgentKind::new("assistant").expect("kind")
}

fn key(name: &str) -> AgentKey {
    AgentKey::new(name).expect("key")
}

fn pin() -> HandlerPin {
    HandlerPin {
        handler: "chat".to_string(),
        version: "1".to_string(),
        digest: Hash256([1; 32]),
    }
}

struct Seeded {
    agent: AgentId,
    /// The operation the completed turn consumed.
    completed: String,
    /// The operation the active turn is running.
    running: String,
}

/// One agent with a completed turn (state version 1, three events), a second
/// input whose turn is active, and one scheduled timer.
async fn seed(backend: Arc<dyn ObjectStoreBackend>) -> Seeded {
    let store = writer(backend);
    let created = store.create(kind(), key("a-1")).await.expect("create");
    let agent = created.agent;
    let incarnation = created.head.incarnation;
    let first = store
        .enqueue(
            &agent,
            incarnation,
            "r1",
            "chat",
            Bytes::from_static(br#"{"text":"hi"}"#),
        )
        .await
        .expect("enqueue r1");
    let completed = first.handle().expect("r1 handle").encode();
    let owned = store
        .execute(
            &agent,
            &Command::Takeover(Takeover {
                incarnation,
                owner_id: "w-1".to_string(),
                observed_epoch: first.head.epoch,
                lease_hint_unix_ns: NOW,
            }),
        )
        .await
        .expect("takeover");
    let owner = OwnerFence {
        incarnation,
        owner_id: "w-1".to_string(),
        epoch: owned.head.epoch,
    };
    let begun = store
        .execute(
            &agent,
            &Command::BeginTurn(BeginTurn {
                fence: owner.clone(),
                handlers: vec![pin()],
            }),
        )
        .await
        .expect("begin r1");
    let turn = begun.head.active_turn.clone().expect("active turn r1");
    let state = Bytes::from_static(br#"{"count":1}"#);
    let payloads: Vec<Bytes> = (1..=3)
        .map(|n| Bytes::from(format!(r#"{{"n":{n}}}"#)))
        .collect();
    let mut objects = vec![state.clone()];
    objects.extend(payloads.iter().cloned());
    store
        .execute_with(
            &agent,
            &Command::CompleteTurn(CompleteTurn {
                fence: TurnFence {
                    owner: owner.clone(),
                    turn_id: turn.turn_id,
                    base_state_version: turn.base_state_version,
                    handler_version: "1".to_string(),
                    handler_digest: Hash256([1; 32]),
                },
                observed_root_revision: begun.head.root_revision,
                new_state: ObjectRef::of(&state),
                output: None,
                events: payloads
                    .iter()
                    .map(|p| EventSpec {
                        event_type: "said".to_string(),
                        payload: Some(ObjectRef::of(p)),
                    })
                    .collect(),
                timer_changes: Vec::new(),
                intents: Vec::new(),
            }),
            &objects,
        )
        .await
        .expect("complete r1");
    let second = store
        .enqueue(
            &agent,
            incarnation,
            "r2",
            "chat",
            Bytes::from_static(br#"{"text":"again"}"#),
        )
        .await
        .expect("enqueue r2");
    let running = second.handle().expect("r2 handle").encode();
    store
        .execute(
            &agent,
            &Command::BeginTurn(BeginTurn {
                fence: owner,
                handlers: vec![pin()],
            }),
        )
        .await
        .expect("begin r2");
    store
        .execute(
            &agent,
            &Command::ScheduleTimer(ScheduleTimer {
                fence: IncarnationFence { incarnation },
                timer: TimerSpec {
                    timer_key: "remind".to_string(),
                    due_unix_ns: NOW + 60 * SECOND,
                    handler: "chat".to_string(),
                    payload: None,
                },
            }),
        )
        .await
        .expect("schedule timer");
    Seeded {
        agent,
        completed,
        running,
    }
}

/// A `DevHost` that records each call and starts nothing.
#[derive(Default)]
struct RecordingHost {
    calls: Vec<&'static str>,
    store_up: bool,
}

impl DevHost for RecordingHost {
    async fn store_answers(&mut self) -> bool {
        self.calls.push("store_answers");
        self.store_up
    }
    async fn compose_up(&mut self) -> anyhow::Result<()> {
        self.calls.push("compose_up");
        Ok(())
    }
    async fn create_bucket(&mut self) -> anyhow::Result<()> {
        self.calls.push("create_bucket");
        Ok(())
    }
    async fn qualify(&mut self) -> anyhow::Result<()> {
        self.calls.push("qualify");
        Ok(())
    }
    async fn start_server(&mut self, _token: &str) -> anyhow::Result<()> {
        self.calls.push("start_server");
        Ok(())
    }
    async fn wait_server(&mut self) -> anyhow::Result<()> {
        self.calls.push("wait_server");
        Ok(())
    }
}

fn dev_options(store: StoreSelection) -> DevOptions {
    DevOptions {
        store,
        listen_http: "127.0.0.1:4318".parse().expect("addr"),
        listen_grpc: "127.0.0.1:4317".parse().expect("addr"),
    }
}

fn emitted<T: serde::Serialize>(value: &T, text: impl FnOnce(&T) -> String) -> String {
    let mut out = Vec::new();
    agents::emit(&mut out, true, value, text).expect("emit");
    String::from_utf8(out).expect("utf-8")
}

#[tokio::test]
async fn dev_command_refuses_the_memory_store_and_inspect_reads_head_from_the_bucket() {
    for (selection, defaulted) in [
        (StoreSelection::defaulted_memory(), true),
        (StoreSelection::explicit(StoreKind::Memory), false),
    ] {
        let mut host = RecordingHost::default();
        let mut out = Vec::new();
        let err = agents::run_dev(&dev_options(selection), &mut host, "tok", &mut out, false)
            .await
            .expect_err("dev on the memory store must refuse");
        match err.downcast_ref::<AgentsCliError>() {
            Some(AgentsCliError::DevRefusesMemoryStore { defaulted: got }) => {
                assert_eq!(*got, defaulted)
            }
            other => panic!("expected DevRefusesMemoryStore, got {other:?}"),
        }
        assert!(
            host.calls.is_empty(),
            "a refused dev started {:?}",
            host.calls
        );
        assert!(out.is_empty(), "a refused dev printed a ready line");
    }

    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let seeded = seed(backend.clone()).await;
    let reader = agents::reader(backend, TENANT);
    let out = agents::inspect(&reader, seeded.agent)
        .await
        .expect("inspect reads HEAD from the bucket");
    assert_eq!(out.state.agent_id, seeded.agent.to_hex());
    assert_eq!(out.state.state_version, 1);
    assert_eq!(out.state.state, json!({"count": 1}));
    assert_eq!(out.kind, "assistant");
    assert_eq!(out.key, "a-1");
    assert_eq!(out.mailbox_depth, 1);
    assert_eq!(out.next_accept_seq, 3);
    let turn = out.active_turn.as_ref().expect("active turn");
    assert_eq!(turn.input_accept_seq, 2);
    assert_eq!(turn.base_state_version, 1);
    assert_eq!(turn.handler, "chat");
    assert_eq!(turn.handler_version, "1");
    assert_eq!(turn.started_unix_ns, NOW.to_string());
    assert!(!turn.cancel_requested);
    assert!(!turn.blocked);
    assert_eq!(out.timers.len(), 1);
    assert_eq!(out.timers[0].timer_key, "remind");
    assert_eq!(out.timers[0].due_unix_ns, (NOW + 60 * SECOND).to_string());
    assert_eq!(out.timers[0].handler, "chat");
    assert_eq!(out.timers[0].status, "scheduled");

    let text = agents::render_inspect(&out);
    assert!(text.contains("state version 1"), "{text}");
    assert!(text.contains("active turn: "), "{text}");
    assert!(text.contains("handler chat@1 input 2"), "{text}");
    assert!(text.contains("timers: 1\n  remind gen "), "{text}");

    let printed = emitted(&out, agents::render_inspect);
    let as_state: StateResponse = serde_json::from_str(&printed).expect("state route body");
    assert_eq!(as_state, out.state);
    let round: InspectOutput = serde_json::from_str(&printed).expect("inspect output");
    assert_eq!(round, out);
}

#[tokio::test]
async fn dev_runs_its_steps_in_order_once_the_store_is_not_memory() {
    for (store_up, expected) in [
        (
            true,
            vec![
                "store_answers",
                "create_bucket",
                "qualify",
                "start_server",
                "wait_server",
            ],
        ),
        (
            false,
            vec![
                "store_answers",
                "compose_up",
                "create_bucket",
                "qualify",
                "start_server",
                "wait_server",
            ],
        ),
    ] {
        let mut host = RecordingHost {
            store_up,
            ..RecordingHost::default()
        };
        let mut out = Vec::new();
        agents::run_dev(
            &dev_options(StoreSelection::explicit(StoreKind::S3)),
            &mut host,
            "tok",
            &mut out,
            true,
        )
        .await
        .expect("dev on s3");
        assert_eq!(host.calls, expected);
        let ready: agents::DevReady = serde_json::from_slice(&out).expect("ready JSON");
        assert_eq!(ready.url, "http://127.0.0.1:4318");
        assert_eq!(ready.tenant, "dev");
        assert_eq!(ready.token, "tok");
        assert_eq!(
            ready.capabilities,
            vec!["agents", "agents_worker", "agents_admin"]
        );
    }
}

#[tokio::test]
async fn dev_refuses_a_listener_off_loopback_before_starting_anything() {
    let mut options = dev_options(StoreSelection::explicit(StoreKind::S3));
    options.listen_http = "0.0.0.0:4318".parse().expect("addr");
    let mut host = RecordingHost::default();
    let err = agents::run_dev(&options, &mut host, "tok", &mut Vec::new(), false)
        .await
        .expect_err("non-loopback listener");
    assert!(
        matches!(
            err.downcast_ref::<AgentsCliError>(),
            Some(AgentsCliError::ListenNotLoopback { flag: "listen", .. })
        ),
        "{err}"
    );
    assert!(host.calls.is_empty());
}

#[test]
fn dev_binary_refuses_the_default_store_without_starting_the_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let marker = dir.path().join("server-started");
    let fake_server = dir.path().join("fake-ravel-server");
    std::fs::write(
        &fake_server,
        format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
    )
    .expect("write fake server");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_server, std::fs::Permissions::from_mode(0o755))
            .expect("chmod");
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["agents", "dev", "--server-bin"])
        .arg(&fake_server)
        .output()
        .expect("run ravel-cli");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("agents dev refuses the memory store (no --store given"),
        "{stderr}"
    );
    assert!(output.stdout.is_empty());
    assert!(!marker.exists(), "the server was started");
}

async fn create_agents(backend: Arc<dyn ObjectStoreBackend>, n: usize) -> Vec<String> {
    let store = writer(backend);
    let mut ids = Vec::new();
    for i in 0..n {
        let created = store
            .create(kind(), key(&format!("a-{i}")))
            .await
            .expect("create");
        ids.push(created.agent.to_hex());
    }
    ids.sort();
    ids
}

fn list_requests(store: &AgentStore) -> u64 {
    let c = store.counters();
    c.resolve.list + c.build.list + c.store.list + c.publish.list
}

#[tokio::test]
async fn list_pages_are_bounded_and_resume_at_the_cursor() {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::with_page_size(2));
    let mut expected = create_agents(backend.clone(), 5).await;
    let reader = agents::reader(backend, TENANT);

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = Vec::new();
    loop {
        let before = list_requests(&reader);
        let page = agents::list(&reader, &TENANT, cursor.as_deref(), 3)
            .await
            .expect("list");
        pages.push((page.agents.len(), list_requests(&reader) - before));
        let printed = emitted(&page, agents::render_list);
        let round: ListOutput = serde_json::from_str(&printed).expect("list JSON");
        assert_eq!(round, page);
        for entry in &page.agents {
            assert_eq!(entry.lifecycle, "active");
            assert_eq!(entry.kind.as_deref(), Some("assistant"));
            assert_eq!(entry.state_version, Some(0));
            seen.push(entry.agent_id.clone());
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(pages.len() < 5, "list never ended");
    }
    // Three agents span two listing pages of two keys. The last two fill a
    // page, so the store reports more and one empty page ends the walk.
    assert_eq!(pages, vec![(3, 2), (2, 2)]);
    seen.sort();
    expected.dedup();
    assert_eq!(seen, expected);

    for (limit, refusal) in [(0, "LimitZero"), (LIST_LIMIT_CAP + 1, "LimitOverCap")] {
        let before = list_requests(&reader);
        let err = agents::list(&reader, &TENANT, None, limit)
            .await
            .expect_err("limit out of range");
        assert!(format!("{err:?}").starts_with(refusal), "{err:?}");
        assert_eq!(list_requests(&reader), before, "a refused list listed");
    }
    let err = agents::list(&reader, &TENANT, Some("a/b"), 2)
        .await
        .expect_err("cursor with a slash");
    assert!(matches!(err, AgentsCliError::BadListCursor(_)), "{err:?}");
}

#[tokio::test]
async fn operation_events_and_effects_print_the_wire_types() {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let seeded = seed(backend.clone()).await;
    let reader = agents::reader(backend, TENANT);

    for (handle, status, seq) in [
        (&seeded.completed, OperationStatus::Completed, 1),
        (&seeded.running, OperationStatus::Running, 2),
    ] {
        let out = agents::operation(&reader, handle).await.expect("operation");
        assert_eq!(out.operation, *handle);
        assert_eq!(out.status, status);
        assert_eq!(out.accept_seq, seq);
        assert_eq!(out.blocked_effect, None);
        let round: OperationResponse =
            serde_json::from_str(&emitted(&out, agents::render_operation)).expect("op JSON");
        assert_eq!(round, out);
    }
    let err = agents::operation(&reader, "not-a-handle")
        .await
        .expect_err("malformed handle");
    assert!(
        matches!(err, AgentsCliError::MalformedOperation(_)),
        "{err:?}"
    );

    let first = agents::events(&reader, seeded.agent, None, 2)
        .await
        .expect("events");
    assert_eq!(
        first.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(first.events[0].payload, json!({"n": 1}));
    assert_eq!(first.events[0].event_type, "said");
    assert_eq!(first.events[0].accept_seq, 1);
    assert_eq!(first.next_cursor, "2");
    let round: EventsResponse =
        serde_json::from_str(&emitted(&first, agents::render_events)).expect("events JSON");
    assert_eq!(round, first);
    let rest = agents::events(&reader, seeded.agent, Some(&first.next_cursor), 2)
        .await
        .expect("events after the cursor");
    assert_eq!(
        rest.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(rest.events[0].payload, json!({"n": 3}));
    assert_eq!(rest.next_cursor, "3");
    let end = agents::events(&reader, seeded.agent, Some("3"), 2)
        .await
        .expect("events at the end");
    assert!(end.events.is_empty());
    assert_eq!(end.next_cursor, "3");
    for (cursor, limit) in [
        (None, 0),
        (None, agents::EVENTS_LIMIT_CAP + 1),
        (Some("x"), 2),
    ] {
        agents::events(&reader, seeded.agent, cursor, limit)
            .await
            .expect_err("bad events page");
    }

    let effects = agents::effects(&reader, seeded.agent)
        .await
        .expect("effects");
    assert!(effects.effects.is_empty());
    let round: EffectsResponse =
        serde_json::from_str(&emitted(&effects, agents::render_effects)).expect("effects JSON");
    assert_eq!(round, effects);
}

#[tokio::test]
async fn reads_of_an_unknown_agent_are_agent_gone() {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let reader = agents::reader(backend, TENANT);
    let absent = agent_id(&kind(), &key("nobody"));
    let err = agents::inspect(&reader, absent)
        .await
        .expect_err("absent agent");
    match err {
        AgentsCliError::Agent(e) => assert_eq!(e.kind, ErrorKind::AgentGone),
        other => panic!("expected AgentGone, got {other:?}"),
    }
    let by_key = agents::AgentSelector::from_flags(
        None,
        Some("assistant".to_string()),
        Some("nobody".to_string()),
    )
    .expect("selector")
    .resolve()
    .expect("resolve");
    assert_eq!(by_key, absent);
}

#[test]
fn scaffold_writes_the_example_and_refuses_a_non_empty_dir() {
    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("my-agent");
    let written = agents::scaffold(&dir).expect("scaffold");
    assert_eq!(written.len(), agents::SCAFFOLD_FILES.len());
    for (name, body) in agents::SCAFFOLD_FILES {
        assert_eq!(std::fs::read_to_string(dir.join(name)).expect("read"), body);
    }
    let err = agents::scaffold(&dir).expect_err("non-empty dir");
    assert!(
        matches!(
            err.downcast_ref::<AgentsCliError>(),
            Some(AgentsCliError::ScaffoldDirNotEmpty { .. })
        ),
        "{err}"
    );
}

type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

async fn reconcile_route(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<ReconcileResponse> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let request: ReconcileRequest = serde_json::from_value(body.clone()).expect("request");
    seen.lock().expect("seen").push((auth, body));
    Json(ReconcileResponse {
        effect: request.effect,
        status: ravel_agents_core::wire::EffectState::Authorized,
        attempt: request.attempt + 1,
    })
}

#[tokio::test]
async fn reconcile_posts_the_decision_with_the_admin_token() {
    let seen: Seen = Arc::default();
    let app = Router::new()
        .route("/api/v1/agents/reconcile", post(reconcile_route))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let client =
        ravel_agents_client::AgentsClient::new(&format!("http://{addr}"), "admin-tok".to_string())
            .expect("client");

    let attach = ReconcileChoice::parse(Some(r#"{"charged":true}"#), false).expect("choice");
    let out = agents::reconcile(&client, "eff-1", 1, attach)
        .await
        .expect("reconcile attach");
    assert_eq!(out.effect, "eff-1");
    assert_eq!(out.attempt, 2);
    let round: ReconcileResponse =
        serde_json::from_str(&emitted(&out, agents::render_reconcile)).expect("JSON");
    assert_eq!(round, out);
    agents::reconcile(&client, "eff-1", 2, ReconcileChoice::AuthorizeAttempt)
        .await
        .expect("reconcile retry");

    let seen = seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 2);
    for (auth, _) in &seen {
        assert_eq!(auth.as_deref(), Some("Bearer admin-tok"));
    }
    let first: ReconcileRequest = serde_json::from_value(seen[0].1.clone()).expect("first");
    assert_eq!(first.effect, "eff-1");
    assert_eq!(first.attempt, 1);
    assert_eq!(
        first.decision,
        ravel_agents_core::wire::ReconcileDecision::RecordResult {
            result: json!({"charged": true})
        }
    );
    let second: ReconcileRequest = serde_json::from_value(seen[1].1.clone()).expect("second");
    assert_eq!(
        second.decision,
        ravel_agents_core::wire::ReconcileDecision::Retry
    );

    for (attach, authorize) in [(None, false), (Some("{}"), true), (Some("not json"), false)] {
        assert!(
            ReconcileChoice::parse(attach, authorize).is_err(),
            "{attach:?} {authorize}"
        );
    }
    server.abort();
}
