//! `ravel-cli agents` (issue #3355): `dev` refuses the memory store before it
//! starts anything, and the read commands decode HEAD straight from the bucket
//! with no server running.

#![allow(clippy::expect_used)]

use std::net::{SocketAddr, TcpListener};
use std::process::Command as Process;
use std::sync::Arc;

use bytes::Bytes;
use ravel_agents_core::command::{
    BeginTurn, Command, CompleteTurn, EventSpec, HandlerPin, IncarnationFence, OwnerFence,
    ScheduleTimer, Takeover, TimerSpec, TurnFence,
};
use ravel_agents_core::head::{Head, ObjectRef};
use ravel_agents_core::identity::{AgentId, Hash256};
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::wire::{EffectsResponse, EventsResponse, OperationResponse, OperationStatus};
use ravel_agents_store::{AgentStore, StoreLimits};
use ravel_cli::agents::{
    self, AgentListing, AgentSelector, AgentView, AgentsCliError, DevHost, DevOptions, ServerPlan,
};
use ravel_cli::store::StoreKind;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::{TenantHash, TenantId};

const TIMER_DUE_NS: i64 = 4_102_444_800_000_000_000;

fn tenant() -> TenantHash {
    TenantId::new("acme").hash()
}

fn pin() -> HandlerPin {
    HandlerPin {
        handler: "chat".to_string(),
        version: "1".to_string(),
        digest: Hash256([7; 32]),
    }
}

fn owner(head: &Head) -> OwnerFence {
    OwnerFence {
        incarnation: head.incarnation,
        owner_id: head.owner_id.clone(),
        epoch: head.epoch,
    }
}

/// An agent whose first turn completed with one event, whose second input
/// is the active turn, and which has one scheduled timer: every HEAD field
/// `inspect` prints has a non-default value to compare.
async fn seed(store: Arc<dyn ObjectStoreBackend>, key: &str) -> (AgentId, Head) {
    let agents = AgentStore::with_system_clock(store, tenant(), StoreLimits::default());
    let created = agents
        .create(
            AgentKind::new("assistant").expect("kind"),
            AgentKey::new(key).expect("key"),
        )
        .await
        .expect("create");
    let agent = created.agent;
    let incarnation = created.head.incarnation;
    agents
        .enqueue(&agent, incarnation, "r1", "chat", Bytes::from_static(b"{}"))
        .await
        .expect("enqueue r1");
    let taken = agents
        .execute(
            &agent,
            &Command::Takeover(Takeover {
                incarnation,
                owner_id: "worker-1".to_string(),
                observed_epoch: created.head.epoch,
                lease_hint_unix_ns: 0,
            }),
        )
        .await
        .expect("takeover");
    let begun = agents
        .execute(
            &agent,
            &Command::BeginTurn(BeginTurn {
                fence: owner(&taken.head),
                handlers: vec![pin()],
            }),
        )
        .await
        .expect("begin first turn");
    let turn = begun.head.active_turn.clone().expect("first turn is active");
    let state = Bytes::from_static(b"{\"messages\":1}");
    let event = Bytes::from_static(b"{\"text\":\"hello\"}");
    agents
        .execute_with(
            &agent,
            &Command::CompleteTurn(CompleteTurn {
                fence: TurnFence {
                    owner: owner(&begun.head),
                    turn_id: turn.turn_id,
                    base_state_version: turn.base_state_version,
                    handler_version: "1".to_string(),
                    handler_digest: Hash256([7; 32]),
                },
                observed_root_revision: begun.head.root_revision,
                new_state: ObjectRef::of(&state),
                output: None,
                events: vec![EventSpec {
                    event_type: "reply".to_string(),
                    payload: Some(ObjectRef::of(&event)),
                }],
                timer_changes: Vec::new(),
                intents: Vec::new(),
            }),
            &[state, event],
        )
        .await
        .expect("complete first turn");
    agents
        .enqueue(&agent, incarnation, "r2", "chat", Bytes::from_static(b"{}"))
        .await
        .expect("enqueue r2");
    let second = agents
        .execute(
            &agent,
            &Command::BeginTurn(BeginTurn {
                fence: owner(&taken.head),
                handlers: vec![pin()],
            }),
        )
        .await
        .expect("begin second turn");
    let scheduled = agents
        .execute(
            &agent,
            &Command::ScheduleTimer(ScheduleTimer {
                fence: IncarnationFence {
                    incarnation: second.head.incarnation,
                },
                timer: TimerSpec {
                    timer_key: "reminder".to_string(),
                    due_unix_ns: TIMER_DUE_NS,
                    handler: "chat".to_string(),
                    payload: None,
                },
            }),
        )
        .await
        .expect("schedule timer");
    (agent, scheduled.head)
}

/// Records every step `agents::dev` asks of the host. `start_server` binds
/// the plan's HTTP address, the way the real server would, so a dev that
/// reached it leaves a listener behind.
#[derive(Default)]
struct FakeHost {
    healthy: bool,
    calls: Vec<&'static str>,
    listener: Option<TcpListener>,
}

impl DevHost for FakeHost {
    async fn store_healthy(&mut self) -> bool {
        self.calls.push("store_healthy");
        self.healthy
    }
    async fn compose_up(&mut self) -> anyhow::Result<()> {
        self.calls.push("compose_up");
        Ok(())
    }
    async fn wait_for_bucket(&mut self) -> anyhow::Result<()> {
        self.calls.push("wait_for_bucket");
        Ok(())
    }
    async fn qualify(&mut self) -> anyhow::Result<()> {
        self.calls.push("qualify");
        Ok(())
    }
    async fn start_server(&mut self, plan: &ServerPlan) -> anyhow::Result<()> {
        self.calls.push("start_server");
        self.listener = Some(TcpListener::bind(plan.listen_http)?);
        Ok(())
    }
    async fn wait_server(&mut self) -> anyhow::Result<()> {
        self.calls.push("wait_server");
        Ok(())
    }
}

fn free_loopback() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
}

fn dev_options(store_kind: StoreKind) -> DevOptions {
    DevOptions {
        store_kind,
        listen_http: free_loopback(),
        listen_grpc: free_loopback(),
        tenant: "dev".to_string(),
        server_store_args: vec!["--store".into(), "s3".into()],
        client_token: "client-token".to_string(),
        worker_token: "worker-token".to_string(),
        json: true,
    }
}

#[tokio::test]
async fn dev_command_refuses_the_memory_store_and_inspect_reads_head_from_the_bucket() {
    // dev on a memory store: the typed refusal, no host step, no listener.
    let options = dev_options(StoreKind::Memory);
    let mut host = FakeHost::default();
    let mut out = Vec::new();
    let err = agents::dev(&options, &mut host, &mut out)
        .await
        .expect_err("dev refuses the memory store");
    assert_eq!(
        err.downcast_ref::<AgentsCliError>(),
        Some(&AgentsCliError::MemoryStore)
    );
    assert!(host.listener.is_none(), "no listener was opened");
    TcpListener::bind(options.listen_http).expect("the HTTP address is still free");
    assert!(host.calls.is_empty(), "no step ran: {:?}", host.calls);
    assert!(out.is_empty(), "nothing was printed as ready");

    // inspect on a MemoryStore bucket, with no server anywhere.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let (agent, head) = seed(store.clone(), "user-1").await;
    let turn = head.active_turn.clone().expect("seeded with an active turn");
    let mut out = Vec::new();
    let view = agents::inspect(
        store.clone(),
        tenant(),
        &AgentSelector::by_id(&agent),
        true,
        &mut out,
    )
    .await
    .expect("inspect reads HEAD from the bucket");
    let parsed: AgentView = serde_json::from_slice(&out).expect("--json is an AgentView");
    assert_eq!(parsed, view);
    assert_eq!(view.state_version, head.state_version);
    assert_eq!(view.state_version, 1, "the first turn's state was committed");
    let active = view.active_turn.expect("the active turn is printed");
    assert_eq!(active.accept_seq, turn.input_accept_seq);
    assert_eq!(active.turn_id, turn.turn_id.to_hex());
    assert_eq!(active.base_state_version, 1);
    assert_eq!(active.handler, "chat");
    assert_eq!(view.timers.len(), 1);
    assert_eq!(view.timers[0].timer_key, "reminder");
    assert_eq!(view.timers[0].due_unix_ns, TIMER_DUE_NS);
    assert_eq!(view.timers[0].status, "scheduled");

    let mut text = Vec::new();
    agents::inspect(
        store,
        tenant(),
        &AgentSelector {
            agent: None,
            kind: Some("assistant".to_string()),
            key: Some("user-1".to_string()),
        },
        false,
        &mut text,
    )
    .await
    .expect("inspect by kind and key");
    let text = String::from_utf8(text).expect("utf-8");
    assert!(text.contains("state_version   1\n"), "{text}");
    assert!(text.contains(&format!("active_turn     {}\n", turn.turn_id.to_hex())), "{text}");
    assert!(
        text.contains(&format!(
            "  reminder generation {} due_unix_ns {TIMER_DUE_NS} handler chat scheduled\n",
            head.timers[0].generation
        )),
        "{text}"
    );
}

#[tokio::test]
async fn dev_runs_its_steps_in_order_and_composes_only_when_the_store_is_down() {
    for healthy in [true, false] {
        let options = dev_options(StoreKind::S3);
        let mut host = FakeHost {
            healthy,
            ..FakeHost::default()
        };
        let mut out = Vec::new();
        let ready = agents::dev(&options, &mut host, &mut out)
            .await
            .expect("dev runs");
        let mut expected = vec!["store_healthy"];
        if !healthy {
            expected.push("compose_up");
        }
        expected.extend(["wait_for_bucket", "qualify", "start_server", "wait_server"]);
        assert_eq!(host.calls, expected);
        assert_eq!(ready.compose_started, !healthy);
        let parsed: agents::DevReady =
            serde_json::from_slice(&out).expect("--json is a DevReady");
        assert_eq!(parsed, ready);
        assert_eq!(parsed.url, format!("http://{}", options.listen_http));
        assert_eq!(parsed.scaffold_command, "ravel-cli agents scaffold ./my-agent");
    }
}

#[tokio::test]
async fn dev_passes_agents_loopback_listeners_and_scoped_tokens_to_the_server() {
    struct Capture(Option<ServerPlan>);
    impl DevHost for Capture {
        async fn store_healthy(&mut self) -> bool {
            true
        }
        async fn compose_up(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn wait_for_bucket(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn qualify(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn start_server(&mut self, plan: &ServerPlan) -> anyhow::Result<()> {
            self.0 = Some(plan.clone());
            Ok(())
        }
        async fn wait_server(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }
    let options = dev_options(StoreKind::S3);
    let mut host = Capture(None);
    agents::dev(&options, &mut host, &mut Vec::new())
        .await
        .expect("dev runs");
    let plan = host.0.expect("the server was started");
    let expected: Vec<String> = [
        "--store",
        "s3",
        "--agents",
        "--listen-http",
        &options.listen_http.to_string(),
        "--listen-grpc",
        &options.listen_grpc.to_string(),
        "--tenant-token",
        "client-token=dev;agents,agents_admin",
        "--tenant-token",
        "worker-token=dev;agents_worker",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(plan.args, expected);
}

#[tokio::test]
async fn dev_refuses_a_listener_that_is_not_loopback() {
    let mut options = dev_options(StoreKind::S3);
    options.listen_http = "0.0.0.0:4318".parse().expect("address");
    let mut host = FakeHost::default();
    let err = agents::dev(&options, &mut host, &mut Vec::new())
        .await
        .expect_err("a wildcard listener is refused");
    assert_eq!(
        err.downcast_ref::<AgentsCliError>(),
        Some(&AgentsCliError::NotLoopback {
            flag: "listen-http",
            addr: options.listen_http,
        })
    );
    assert!(host.calls.is_empty(), "{:?}", host.calls);
}

#[test]
fn the_binary_refuses_dev_without_a_store_flag() {
    let http = free_loopback();
    let output = Process::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "agents",
            "dev",
            "--listen-http",
            &http.to_string(),
            "--server-bin",
            "/nonexistent/ravel-server",
            "--compose-file",
            "/nonexistent/rustfs.yml",
        ])
        .env_remove("RAVEL_SERVER_BIN")
        .output()
        .expect("run ravel-cli");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("agents dev refuses the memory store"),
        "{stderr}"
    );
    assert!(stderr.contains("--store s3 --s3-endpoint http://127.0.0.1:9000"), "{stderr}");
    TcpListener::bind(http).expect("nothing listens on the HTTP address");
}

#[tokio::test]
async fn list_pages_the_directory_and_refuses_a_limit_above_the_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut ids = Vec::new();
    for key in ["a", "b", "c"] {
        ids.push(seed(store.clone(), key).await.0.to_hex());
    }
    ids.sort();

    let err = agents::list(
        store.clone(),
        tenant(),
        agents::LIST_LIMIT_CAP + 1,
        None,
        true,
        &mut Vec::new(),
    )
    .await
    .expect_err("a limit above the cap is refused");
    assert_eq!(
        err.downcast_ref::<AgentsCliError>(),
        Some(&AgentsCliError::LimitOverCap {
            limit: 1_001,
            cap: 1_000
        })
    );

    let mut out = Vec::new();
    let first = agents::list(store.clone(), tenant(), 2, None, true, &mut out)
        .await
        .expect("first page");
    let parsed: AgentListing = serde_json::from_slice(&out).expect("--json is an AgentListing");
    assert_eq!(parsed, first);
    assert_eq!(first.agents, ids[..2]);
    assert_eq!(first.next_cursor.as_deref(), Some(ids[1].as_str()));

    let second = agents::list(
        store,
        tenant(),
        2,
        first.next_cursor.as_deref(),
        true,
        &mut Vec::new(),
    )
    .await
    .expect("second page");
    assert_eq!(second.agents, ids[2..]);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn the_binary_refuses_a_list_limit_above_the_cap() {
    let output = Process::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["agents", "list", "--tenant", "acme", "--limit", "1001"])
        .output()
        .expect("run ravel-cli");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--limit 1001 is above the cap of 1000"), "{stderr}");
}

#[tokio::test]
async fn operation_events_and_effects_print_the_route_bodies() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let (agent, head) = seed(store.clone(), "user-2").await;
    let selector = AgentSelector::by_id(&agent);

    let turn = head.active_turn.clone().expect("active turn");
    let handle = ravel_agents_core::state::Handle::operation(
        agent,
        head.incarnation,
        turn.input_accept_seq,
    )
    .encode();
    let mut out = Vec::new();
    let op = agents::operation(store.clone(), tenant(), &handle, true, &mut out)
        .await
        .expect("operation");
    let parsed: OperationResponse = serde_json::from_slice(&out).expect("OperationResponse");
    assert_eq!(parsed, op);
    assert_eq!(op.status, OperationStatus::Running);
    assert_eq!(op.blocked_effect, None);

    let mut out = Vec::new();
    let page = agents::events(store.clone(), tenant(), &selector, None, 10, true, &mut out)
        .await
        .expect("events");
    let parsed: EventsResponse = serde_json::from_slice(&out).expect("EventsResponse");
    assert_eq!(parsed, page);
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].event_type, "reply");
    assert_eq!(page.events[0].payload, serde_json::json!({"text": "hello"}));

    // Resuming from the printed cursor returns nothing new and holds it.
    let resumed = agents::events(
        store.clone(),
        tenant(),
        &selector,
        Some(&page.next_cursor),
        10,
        true,
        &mut Vec::new(),
    )
    .await
    .expect("resumed events");
    assert!(resumed.events.is_empty());
    assert_eq!(resumed.next_cursor, page.next_cursor);

    let err = agents::events(
        store.clone(),
        tenant(),
        &selector,
        None,
        agents::EVENTS_LIMIT_CAP + 1,
        true,
        &mut Vec::new(),
    )
    .await
    .expect_err("an events limit above the cap is refused");
    assert!(err.downcast_ref::<AgentsCliError>().is_some(), "{err}");

    let mut out = Vec::new();
    let effects = agents::effects(store, tenant(), &selector, true, &mut out)
        .await
        .expect("effects");
    let parsed: EffectsResponse = serde_json::from_slice(&out).expect("EffectsResponse");
    assert_eq!(parsed, effects);
    assert!(effects.effects.is_empty());
}

#[tokio::test]
async fn reading_an_absent_agent_is_agent_gone() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let missing = AgentId::from_hex("00112233445566778899aabbccddeeff").expect("hex id");
    let err = agents::inspect(
        store,
        tenant(),
        &AgentSelector::by_id(&missing),
        true,
        &mut Vec::new(),
    )
    .await
    .expect_err("absent");
    assert!(err.to_string().starts_with("agent_gone:"), "{err}");
}

#[test]
fn scaffold_writes_the_example_and_refuses_a_non_empty_dir() {
    let root = tempfile::tempdir().expect("tempdir");
    let sdk = root.path().join("sdk");
    std::fs::create_dir(&sdk).expect("sdk dir");
    std::fs::write(sdk.join("package.json"), "{}").expect("sdk package.json");

    let dir = root.path().join("my-agent");
    let mut out = Vec::new();
    let report = agents::scaffold(&dir, &sdk, true, &mut out).expect("scaffold");
    let parsed: agents::ScaffoldReport = serde_json::from_slice(&out).expect("ScaffoldReport");
    assert_eq!(parsed, report);
    for name in ["main.ts", "assistant.ts", "ts-hooks.js", "package.json"] {
        assert!(dir.join(name).is_file(), "{name} written");
    }
    let package: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.join("package.json")).expect("read package.json"),
    )
    .expect("package.json is JSON");
    let sdk_abs = std::fs::canonicalize(&sdk).expect("canonical sdk");
    assert_eq!(
        package["dependencies"]["@ravel/agents"],
        format!("file:{}", sdk_abs.display())
    );

    let err = agents::scaffold(&dir, &sdk, true, &mut Vec::new()).expect_err("non-empty");
    assert_eq!(
        err.downcast_ref::<AgentsCliError>(),
        Some(&AgentsCliError::NotEmpty(dir.clone()))
    );
    let missing = root.path().join("no-sdk");
    let err = agents::scaffold(&root.path().join("other"), &missing, true, &mut Vec::new())
        .expect_err("no sdk package");
    assert_eq!(
        err.downcast_ref::<AgentsCliError>(),
        Some(&AgentsCliError::NoSdkPackage(missing))
    );
}
