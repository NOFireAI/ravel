//! `ravel-cli agents` (ADR-3331, issue #3355): a local development loop and
//! operator inspection of Ravel Agents.
//!
//! Read commands (`list`, `inspect`, `operation`, `events`, `effects`) decode
//! HEAD straight from the bucket through `ravel-agents-store`, so they work
//! with no server running and answer the same as the server's routes would.
//! The one write command, `reconcile`, goes through a running server with
//! `ravel-agents-client`, because the server audits every reconcile decision.

use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Subcommand};
use ravel_agents_client::{AgentsClient, ClientError};
use ravel_agents_core::error::AgentError;
use ravel_agents_core::head::{
    Cancellation, EffectStatus, Head, Lifecycle, ObjectRef, TimerStatus, TurnBlock,
};
use ravel_agents_core::identity::{AgentId, agent_id, effect_id};
use ravel_agents_core::keys::{AgentKey, AgentKind, dir_key};
use ravel_agents_core::state::{Handle, HandleItem, OperationState, operation_state};
use ravel_agents_core::wire::{
    EffectBody, EffectsResponse, EventBody, EventsResponse, OperationResponse, OperationStatus,
    ReconcileDecision, ReconcileRequest, ReconcileResponse,
};
use ravel_agents_store::{AgentStore, HeadState, StoreLimits};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::StoreKind;

/// The most agents one `agents list` page returns. A larger `--limit` is
/// refused rather than clamped, so a script never mistakes a short page for
/// the end of the listing.
pub const LIST_LIMIT_CAP: usize = 1_000;

/// The most events one `agents events` page returns, the server's own cap.
pub const EVENTS_LIMIT_CAP: u32 = ravel_agents_client::MAX_EVENTS_PAGE;

/// The compose file `agents dev` brings RustFS up from, relative to the
/// repository root.
pub const DEFAULT_COMPOSE_FILE: &str = "deploy/docker-compose/rustfs.yml";

/// The store flags that select the compose RustFS bucket, with its published
/// development credentials.
pub const COMPOSE_STORE_FLAGS: &str = "--store s3 --s3-endpoint http://127.0.0.1:9000 \
     --s3-bucket ravel-dev --s3-region us-east-1 --s3-access-key ravel \
     --s3-secret-key ravel-dev-secret";

#[derive(Debug, Subcommand)]
pub enum AgentsCommand {
    /// Run a local development server for Ravel Agents. Refuses the memory
    /// store before doing anything else: agents keep their state in the
    /// bucket, and an in-process store would lose it on exit. Brings up the
    /// compose RustFS when the store does not answer, waits for its
    /// createbucket job, runs `store qualify`, then starts ravel-server with
    /// --agents and freshly generated development tokens on loopback
    /// listeners only, and prints the `agents scaffold` command to start an
    /// example against it. Runs until the server exits.
    Dev(DevArgs),
    /// Write the TypeScript SDK's assistant example into DIR, ready for
    /// `npm install` and `npm start` against `agents dev`. Refuses a DIR that
    /// exists and is not empty.
    Scaffold {
        /// Directory to write the example into; created when absent.
        dir: PathBuf,
        /// The `@ravel/agents` package directory the example depends on.
        #[arg(long, value_name = "PATH", default_value = "sdk/typescript/packages/agents")]
        sdk_package: PathBuf,
        /// Print the report as JSON.
        #[arg(long)]
        json: bool,
    },
    /// List one tenant's agent ids from the bucket's agent directory, one
    /// page at a time. Reads the bucket directly; no server needed.
    List {
        #[arg(long)]
        tenant: String,
        /// Agents per page, at most 1000; a larger value is refused.
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Resume after this agent id, the `next_cursor` a previous page
        /// printed.
        #[arg(long)]
        cursor: Option<String>,
        /// Print the page as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print one agent's HEAD: state_version, the active turn, timers and
    /// queue depths. Reads the bucket directly; no server needed.
    Inspect {
        #[arg(long)]
        tenant: String,
        #[command(flatten)]
        agent: AgentSelector,
        /// Print HEAD as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print an operation's status, as `GET /api/v1/agents/operation` reports
    /// it. Reads the bucket directly; no server needed.
    Operation {
        #[arg(long)]
        tenant: String,
        /// The operation handle `send` or `call` returned.
        handle: String,
        /// Print the status as JSON (the route's response body).
        #[arg(long)]
        json: bool,
    },
    /// Print one page of an agent's event log, as `POST
    /// /api/v1/agents/events` returns it. Reads the bucket directly; no server
    /// needed.
    Events {
        #[arg(long)]
        tenant: String,
        #[command(flatten)]
        agent: AgentSelector,
        /// Resume after this cursor, the `next_cursor` a previous page
        /// printed. Unset starts at the first event.
        #[arg(long)]
        cursor: Option<String>,
        /// Events per page, at most 1000; a larger value is refused.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Print the page as JSON (the route's response body).
        #[arg(long)]
        json: bool,
    },
    /// List an agent's effects, as `GET /api/v1/agents/effects` reports them.
    /// Reads the bucket directly; no server needed.
    Effects {
        #[arg(long)]
        tenant: String,
        #[command(flatten)]
        agent: AgentSelector,
        /// Print the list as JSON (the route's response body).
        #[arg(long)]
        json: bool,
    },
    /// Settle an in-doubt effect through a running server: attach the result
    /// an operator confirmed out of band (--result), or authorize one more
    /// attempt (--retry). The token needs the agents_admin capability.
    Reconcile {
        /// The effect handle `effects` or `operation` printed.
        effect: String,
        /// The attempt the decision is about, the effect's latest.
        #[arg(long)]
        attempt: u32,
        /// Authorize the attempt after --attempt.
        #[arg(long, conflicts_with = "result", required_unless_present = "result")]
        retry: bool,
        /// Record this JSON value as the effect's result.
        #[arg(long, value_name = "JSON")]
        result: Option<String>,
        /// Base URL of the server.
        #[arg(long, default_value = "http://127.0.0.1:4318", env = "RAVEL_URL")]
        url: String,
        /// Bearer token with the agents_admin capability.
        #[arg(long, env = "RAVEL_AGENTS_ADMIN_TOKEN", hide_env_values = true)]
        token: String,
        /// Print the server's answer as JSON (the route's response body).
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Args)]
pub struct DevArgs {
    /// Address of the server's HTTP listener; must be loopback.
    #[arg(long, default_value = "127.0.0.1:4318")]
    pub listen_http: SocketAddr,
    /// Address of the server's gRPC listener; must be loopback.
    #[arg(long, default_value = "127.0.0.1:4317")]
    pub listen_grpc: SocketAddr,
    /// The ravel-server binary to start, built with the agents feature.
    #[arg(long, value_name = "PATH", default_value = "ravel-server", env = "RAVEL_SERVER_BIN")]
    pub server_bin: PathBuf,
    /// The compose file that defines the rustfs and createbucket services.
    #[arg(long, value_name = "PATH", default_value = DEFAULT_COMPOSE_FILE)]
    pub compose_file: PathBuf,
    /// Tenant the development tokens are bound to.
    #[arg(long, default_value = "dev")]
    pub tenant: String,
    /// Seconds to wait for the server to report ready.
    #[arg(long, default_value_t = 60)]
    pub ready_timeout_secs: u64,
    /// Print the ready report as one JSON line.
    #[arg(long)]
    pub json: bool,
}

/// Names one agent: by the id `list` prints, or by kind and key.
#[derive(Debug, Clone, Args)]
pub struct AgentSelector {
    /// The agent id (32 hex characters), as `list` prints it.
    #[arg(required_unless_present = "kind", conflicts_with_all = ["kind", "key"])]
    pub agent: Option<String>,
    /// The agent's kind; with --key, names the agent instead of its id.
    #[arg(long, requires = "key")]
    pub kind: Option<String>,
    /// The agent's key; with --kind, names the agent instead of its id.
    #[arg(long, requires = "kind")]
    pub key: Option<String>,
}

impl AgentSelector {
    pub fn by_id(agent: &AgentId) -> Self {
        AgentSelector {
            agent: Some(agent.to_hex()),
            kind: None,
            key: None,
        }
    }

    pub fn resolve(&self) -> Result<AgentId, AgentsCliError> {
        match (&self.agent, &self.kind, &self.key) {
            (Some(text), _, _) => {
                AgentId::from_hex(text).ok_or_else(|| AgentsCliError::BadAgentId(text.clone()))
            }
            (None, Some(kind), Some(key)) => {
                let kind =
                    AgentKind::new(kind.as_str()).map_err(|e| AgentsCliError::BadName(e.to_string()))?;
                let key =
                    AgentKey::new(key.as_str()).map_err(|e| AgentsCliError::BadName(e.to_string()))?;
                Ok(agent_id(&kind, &key))
            }
            _ => Err(AgentsCliError::NoAgent),
        }
    }
}

/// Refusals this module raises before touching a store or a server.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AgentsCliError {
    #[error(
        "agents dev refuses the memory store: agents keep their state in the bucket, and the \
         in-process store loses it when the command exits. Select the compose RustFS bucket \
         with the top-level flags, before `agents`: ravel-cli {COMPOSE_STORE_FLAGS} agents dev"
    )]
    MemoryStore,
    #[error("--{flag} {addr} is not a loopback address; agents dev binds loopback only")]
    NotLoopback { flag: &'static str, addr: SocketAddr },
    #[error("--limit {limit} is above the cap of {cap}")]
    LimitOverCap { limit: u64, cap: u64 },
    #[error("--limit must be at least 1")]
    LimitZero,
    #[error("{0:?} is not an agent id (32 hex characters)")]
    BadAgentId(String),
    #[error("{0}")]
    BadName(String),
    #[error("name the agent by id, or by --kind and --key")]
    NoAgent,
    #[error("{0:?} is not an operation handle")]
    BadOperation(String),
    #[error("--cursor {0:?} is not an events cursor")]
    BadEventsCursor(String),
    #[error("--result is not a JSON value: {0}")]
    BadResult(String),
    #[error("{} exists and is not an empty directory; scaffold refuses to write into it", .0.display())]
    NotEmpty(PathBuf),
    #[error(
        "{} is not the @ravel/agents package directory (no package.json); pass --sdk-package",
        .0.display()
    )]
    NoSdkPackage(PathBuf),
}

fn agent_error(error: AgentError) -> anyhow::Error {
    anyhow::anyhow!(
        "{}: {} (retry: {})",
        error.kind.code(),
        error.reason,
        error.retry().code()
    )
}

fn store_for(store: Arc<dyn ObjectStoreBackend>, tenant: TenantHash) -> AgentStore {
    AgentStore::with_system_clock(store, tenant, StoreLimits::default())
}

fn write_json<T: Serialize>(out: &mut dyn Write, value: &T) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    writeln!(out)?;
    Ok(())
}

/// A wire enum's snake_case spelling, for text output.
fn wire_word<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// `agents list --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentListing {
    pub agents: Vec<String>,
    /// Pass as `--cursor` for the next page; null when this page is the last.
    pub next_cursor: Option<String>,
}

/// `agents list`: up to `limit` agent ids after `cursor`, in id order.
pub async fn list(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    limit: usize,
    cursor: Option<&str>,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<AgentListing> {
    if limit == 0 {
        return Err(AgentsCliError::LimitZero.into());
    }
    if limit > LIST_LIMIT_CAP {
        return Err(AgentsCliError::LimitOverCap {
            limit: limit as u64,
            cap: LIST_LIMIT_CAP as u64,
        }
        .into());
    }
    let mut start_after = match cursor {
        Some(text) => {
            let agent =
                AgentId::from_hex(text).ok_or_else(|| AgentsCliError::BadAgentId(text.into()))?;
            Some(dir_key(&tenant, &agent).map_err(|e| anyhow::anyhow!("{e}"))?)
        }
        None => None,
    };
    let agents_store = store_for(store, tenant);
    let mut agents = Vec::new();
    let mut more = true;
    while agents.len() < limit && more {
        let page = agents_store
            .list_dir_after(start_after.as_deref())
            .await
            .map_err(agent_error)?;
        more = page.more;
        let mut taken_all = true;
        for entry in page.items {
            if agents.len() == limit {
                taken_all = false;
                break;
            }
            agents.push(entry.agent);
        }
        if !taken_all {
            more = true;
            break;
        }
        match page.last_key {
            Some(key) => start_after = Some(key),
            None => more = false,
        }
    }
    let listing = AgentListing {
        next_cursor: if more {
            agents.last().map(AgentId::to_hex)
        } else {
            None
        },
        agents: agents.iter().map(AgentId::to_hex).collect(),
    };
    if json {
        write_json(out, &listing)?;
    } else {
        for agent in &listing.agents {
            writeln!(out, "{agent}")?;
        }
        if let Some(next) = &listing.next_cursor {
            writeln!(out, "next_cursor {next}")?;
        }
    }
    Ok(listing)
}

/// The active turn in `agents inspect --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveTurnView {
    /// The operation handle of the input the turn runs.
    pub operation: String,
    pub accept_seq: u64,
    pub turn_id: String,
    pub handler: String,
    pub handler_version: String,
    pub base_state_version: u64,
    pub cancellation_requested: bool,
    /// Why the turn cannot proceed, when something blocks it.
    pub blocked: Option<String>,
    pub started_unix_ns: i64,
}

/// One timer in `agents inspect --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerView {
    pub timer_id: String,
    pub timer_key: String,
    pub generation: u64,
    pub due_unix_ns: i64,
    pub handler: String,
    /// `scheduled`, `delivered` or `cancelled`.
    pub status: String,
}

/// `agents inspect --json`: what HEAD records, without payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentView {
    pub agent_id: String,
    pub kind: String,
    pub key: String,
    pub incarnation: u64,
    /// `active` or `tombstoned`.
    pub lifecycle: String,
    pub root_revision: u64,
    /// Empty when no worker has owned the agent in this incarnation.
    pub owner_id: String,
    pub epoch: u64,
    pub state_version: u64,
    /// Bytes of the current state object; null before the first state.
    pub state_bytes: Option<u64>,
    pub next_accept_seq: u64,
    pub next_event_seq: u64,
    pub active_turn: Option<ActiveTurnView>,
    pub timers: Vec<TimerView>,
    pub inbox: usize,
    pub results: usize,
    pub events: usize,
    pub effects: usize,
}

impl AgentView {
    fn of(agent: &AgentId, head: &Head) -> AgentView {
        AgentView {
            agent_id: agent.to_hex(),
            kind: head.kind.as_str().to_string(),
            key: head.key.as_str().to_string(),
            incarnation: head.incarnation,
            lifecycle: match head.lifecycle {
                Lifecycle::Active => "active",
                Lifecycle::Tombstoned => "tombstoned",
            }
            .to_string(),
            root_revision: head.root_revision,
            owner_id: head.owner_id.clone(),
            epoch: head.epoch,
            state_version: head.state_version,
            state_bytes: head.state.map(|s| s.size_bytes),
            next_accept_seq: head.next_accept_seq,
            next_event_seq: head.next_event_seq,
            active_turn: head.active_turn.as_ref().map(|turn| ActiveTurnView {
                operation: Handle::operation(*agent, head.incarnation, turn.input_accept_seq)
                    .encode(),
                accept_seq: turn.input_accept_seq,
                turn_id: turn.turn_id.to_hex(),
                handler: turn.handler.clone(),
                handler_version: turn.handler_version.clone(),
                base_state_version: turn.base_state_version,
                cancellation_requested: turn.cancellation == Cancellation::Requested,
                blocked: match turn.block {
                    TurnBlock::None => None,
                    TurnBlock::NonDeterministicReplay => Some("non_deterministic_replay".into()),
                },
                started_unix_ns: turn.started_unix_ns,
            }),
            timers: head
                .timers
                .iter()
                .map(|timer| TimerView {
                    timer_id: timer.timer_id.to_hex(),
                    timer_key: timer.timer_key.clone(),
                    generation: timer.generation,
                    due_unix_ns: timer.due_unix_ns,
                    handler: timer.handler.clone(),
                    status: match timer.status {
                        TimerStatus::Scheduled => "scheduled",
                        TimerStatus::Delivered => "delivered",
                        TimerStatus::Cancelled => "cancelled",
                    }
                    .to_string(),
                })
                .collect(),
            inbox: head.inbox.len(),
            results: head.results.len(),
            events: head.events.len(),
            effects: head.effects.len(),
        }
    }
}

/// HEAD as the server's routes read it: absent and tombstoned are
/// `agent_gone`, a newer format is `unsupported_format`.
async fn read_head(agents_store: &AgentStore, agent: &AgentId) -> anyhow::Result<Head> {
    match agents_store.read_head(agent).await.map_err(agent_error)? {
        HeadState::Present { head, .. } if head.lifecycle != Lifecycle::Tombstoned => Ok(*head),
        HeadState::Present { .. } | HeadState::Absent => Err(anyhow::anyhow!(
            "agent_gone: agent {} does not exist (retry: never)",
            agent.to_hex()
        )),
        HeadState::Unsupported { found } => Err(anyhow::anyhow!(
            "unsupported_format: HEAD format version {found} is newer than this release \
             (retry: never)"
        )),
    }
}

/// `agents inspect`.
pub async fn inspect(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    agent: &AgentSelector,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<AgentView> {
    let agent = agent.resolve()?;
    let agents_store = store_for(store, tenant);
    let head = match agents_store.read_head(&agent).await.map_err(agent_error)? {
        // Unlike the routes, inspect shows a tombstoned HEAD: an operator
        // looking at one wants to see that it is tombstoned.
        HeadState::Present { head, .. } => *head,
        HeadState::Absent => anyhow::bail!(
            "agent_gone: agent {} does not exist (retry: never)",
            agent.to_hex()
        ),
        HeadState::Unsupported { found } => anyhow::bail!(
            "unsupported_format: HEAD format version {found} is newer than this release \
             (retry: never)"
        ),
    };
    let view = AgentView::of(&agent, &head);
    if json {
        write_json(out, &view)?;
        return Ok(view);
    }
    writeln!(out, "agent           {}", view.agent_id)?;
    writeln!(out, "kind            {}", view.kind)?;
    writeln!(out, "key             {}", view.key)?;
    writeln!(out, "incarnation     {}", view.incarnation)?;
    writeln!(out, "lifecycle       {}", view.lifecycle)?;
    writeln!(out, "root_revision   {}", view.root_revision)?;
    writeln!(
        out,
        "owner           {}",
        if view.owner_id.is_empty() {
            "-"
        } else {
            &view.owner_id
        }
    )?;
    writeln!(out, "epoch           {}", view.epoch)?;
    writeln!(out, "state_version   {}", view.state_version)?;
    match view.state_bytes {
        Some(bytes) => writeln!(out, "state_bytes     {bytes}")?,
        None => writeln!(out, "state_bytes     -")?,
    }
    writeln!(out, "next_accept_seq {}", view.next_accept_seq)?;
    match &view.active_turn {
        None => writeln!(out, "active_turn     none")?,
        Some(turn) => {
            writeln!(out, "active_turn     {}", turn.turn_id)?;
            writeln!(out, "  operation     {}", turn.operation)?;
            writeln!(out, "  accept_seq    {}", turn.accept_seq)?;
            writeln!(
                out,
                "  handler       {} {}",
                turn.handler, turn.handler_version
            )?;
            writeln!(out, "  base_state    {}", turn.base_state_version)?;
            writeln!(out, "  started       {}", turn.started_unix_ns)?;
            if turn.cancellation_requested {
                writeln!(out, "  cancellation  requested")?;
            }
            if let Some(block) = &turn.blocked {
                writeln!(out, "  blocked       {block}")?;
            }
        }
    }
    writeln!(out, "timers          {}", view.timers.len())?;
    for timer in &view.timers {
        writeln!(
            out,
            "  {} generation {} due_unix_ns {} handler {} {}",
            timer.timer_key, timer.generation, timer.due_unix_ns, timer.handler, timer.status
        )?;
    }
    writeln!(out, "inbox           {}", view.inbox)?;
    writeln!(out, "results         {}", view.results)?;
    writeln!(out, "events          {}", view.events)?;
    writeln!(out, "effects         {}", view.effects)?;
    Ok(view)
}

/// `agents operation`: the body `GET /api/v1/agents/operation` returns.
pub async fn operation(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    token: &str,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<OperationResponse> {
    let handle = Handle::decode_kind(token, "op")
        .map_err(|_| AgentsCliError::BadOperation(token.to_string()))?;
    let HandleItem::Operation(seq) = handle.item else {
        return Err(AgentsCliError::BadOperation(token.to_string()).into());
    };
    let agents_store = store_for(store, tenant);
    let head = read_head(&agents_store, &handle.agent).await?;
    let state = (head.incarnation == handle.incarnation)
        .then(|| operation_state(&head, seq))
        .flatten()
        .ok_or_else(|| {
            anyhow::anyhow!("agent_gone: operation is not known to this agent (retry: never)")
        })?;
    let response = OperationResponse {
        operation: token.to_string(),
        status: OperationStatus::from(state),
        accept_seq: seq,
        blocked_effect: blocked_effect(&head, &handle, seq, state),
    };
    if json {
        write_json(out, &response)?;
    } else {
        writeln!(out, "operation      {}", response.operation)?;
        writeln!(out, "status         {}", wire_word(&response.status))?;
        writeln!(out, "accept_seq     {}", response.accept_seq)?;
        writeln!(
            out,
            "blocked_effect {}",
            response.blocked_effect.as_deref().unwrap_or("-")
        )?;
    }
    Ok(response)
}

/// The in-doubt effect a blocked operation waits on, as the operation route
/// names it.
fn blocked_effect(head: &Head, handle: &Handle, seq: u64, state: OperationState) -> Option<String> {
    if state != OperationState::Blocked {
        return None;
    }
    head.effects
        .iter()
        .find(|e| {
            e.status == EffectStatus::InDoubt
                && e.effect_id == effect_id(head.incarnation, seq, &e.step_key)
        })
        .map(|e| {
            Handle {
                agent: handle.agent,
                incarnation: handle.incarnation,
                item: HandleItem::Effect(e.effect_id),
            }
            .encode()
        })
}

async fn read_json(
    agents_store: &AgentStore,
    agent: &AgentId,
    dep: Option<ObjectRef>,
) -> anyhow::Result<Value> {
    let Some(dep) = dep else {
        return Ok(Value::Null);
    };
    let bytes = agents_store
        .read_object(agent, dep)
        .await
        .map_err(agent_error)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("unsupported_format: object is not a JSON value"))
}

/// `agents events`: one page, the body `POST /api/v1/agents/events` returns.
/// `next_cursor` is the last event's sequence, or `cursor` itself when the
/// page is empty, so passing it back resumes without a gap or a repeat.
pub async fn events(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    agent: &AgentSelector,
    cursor: Option<&str>,
    limit: u32,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<EventsResponse> {
    if limit == 0 {
        return Err(AgentsCliError::LimitZero.into());
    }
    if limit > EVENTS_LIMIT_CAP {
        return Err(AgentsCliError::LimitOverCap {
            limit: u64::from(limit),
            cap: u64::from(EVENTS_LIMIT_CAP),
        }
        .into());
    }
    let after = match cursor {
        None => 0,
        Some(text) => text
            .parse::<u64>()
            .map_err(|_| AgentsCliError::BadEventsCursor(text.to_string()))?,
    };
    let agent = agent.resolve()?;
    let agents_store = store_for(store, tenant);
    let head = read_head(&agents_store, &agent).await?;
    let mut page = Vec::new();
    for event in head
        .events
        .iter()
        .filter(|e| e.event_seq > after)
        .take(limit as usize)
    {
        page.push(EventBody {
            event_seq: event.event_seq,
            accept_seq: event.accept_seq,
            turn_id: event.turn_id.to_hex(),
            event_type: event.event_type.clone(),
            payload: read_json(&agents_store, &agent, event.payload).await?,
        });
    }
    let next = page.last().map_or(after, |e| e.event_seq);
    let response = EventsResponse {
        events: page,
        next_cursor: next.to_string(),
    };
    if json {
        write_json(out, &response)?;
    } else {
        for event in &response.events {
            writeln!(
                out,
                "{} accept_seq {} {} {}",
                event.event_seq, event.accept_seq, event.event_type, event.payload
            )?;
        }
        writeln!(out, "next_cursor {}", response.next_cursor)?;
    }
    Ok(response)
}

/// `agents effects`: the body `GET /api/v1/agents/effects` returns.
pub async fn effects(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    agent: &AgentSelector,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<EffectsResponse> {
    let agent = agent.resolve()?;
    let agents_store = store_for(store, tenant);
    let head = read_head(&agents_store, &agent).await?;
    let response = EffectsResponse {
        effects: head
            .effects
            .iter()
            .map(|record| EffectBody {
                effect: Handle {
                    agent,
                    incarnation: head.incarnation,
                    item: HandleItem::Effect(record.effect_id),
                }
                .encode(),
                turn_id: record.turn_id.to_hex(),
                tool_name: record.tool_name.clone(),
                tool_version: record.tool_version.clone(),
                policy: record.policy.into(),
                status: record.status.into(),
                attempt: record.attempt,
            })
            .collect(),
    };
    if json {
        write_json(out, &response)?;
    } else {
        for effect in &response.effects {
            writeln!(
                out,
                "{} {} {} {} attempt {} {}",
                effect.effect,
                effect.tool_name,
                effect.tool_version,
                wire_word(&effect.policy),
                effect.attempt,
                wire_word(&effect.status)
            )?;
        }
        if response.effects.is_empty() {
            writeln!(out, "no effects")?;
        }
    }
    Ok(response)
}

/// `agents reconcile`, through the server at `client`.
pub async fn reconcile(
    client: &AgentsClient,
    effect: &str,
    attempt: u32,
    result: Option<&str>,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<ReconcileResponse> {
    let decision = match result {
        Some(text) => ReconcileDecision::RecordResult {
            result: serde_json::from_str(text)
                .map_err(|e| AgentsCliError::BadResult(e.to_string()))?,
        },
        None => ReconcileDecision::Retry,
    };
    let request = ReconcileRequest {
        effect: effect.to_string(),
        attempt,
        decision,
    };
    let response = client.reconcile(&request).await.map_err(client_error)?;
    if json {
        write_json(out, &response)?;
    } else {
        writeln!(out, "effect  {}", response.effect)?;
        writeln!(out, "status  {}", wire_word(&response.status))?;
        writeln!(out, "attempt {}", response.attempt)?;
    }
    Ok(response)
}

fn client_error(error: ClientError) -> anyhow::Error {
    anyhow::Error::new(error)
}

/// What `agents dev` hands the server it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPlan {
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
    /// Every argument, store and tenancy flags first.
    pub args: Vec<String>,
}

/// The token `agents dev` grants the client role: agents and agents_admin.
/// `--tenant-token` takes `TOKEN=TENANT;CAPABILITIES`.
pub fn dev_token_args(tenant: &str, client_token: &str, worker_token: &str) -> Vec<String> {
    vec![
        "--tenant-token".into(),
        format!("{client_token}={tenant};agents,agents_admin"),
        "--tenant-token".into(),
        format!("{worker_token}={tenant};agents_worker"),
    ]
}

/// Everything `agents dev` does to the outside world, so the order of the
/// steps can be tested without docker or a server.
pub trait DevHost {
    /// Whether the selected store answers a listing.
    fn store_healthy(&mut self) -> impl Future<Output = bool>;
    /// `docker compose up -d rustfs`.
    fn compose_up(&mut self) -> impl Future<Output = anyhow::Result<()>>;
    /// `docker compose run --rm createbucket`, which exits once the bucket
    /// exists.
    fn wait_for_bucket(&mut self) -> impl Future<Output = anyhow::Result<()>>;
    /// `store qualify` against the selected store.
    fn qualify(&mut self) -> impl Future<Output = anyhow::Result<()>>;
    /// Start the server and wait until it reports ready.
    fn start_server(&mut self, plan: &ServerPlan) -> impl Future<Output = anyhow::Result<()>>;
    /// Wait for the server to exit.
    fn wait_server(&mut self) -> impl Future<Output = anyhow::Result<()>>;
}

/// `agents dev --json`, printed once the server is ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevReady {
    pub url: String,
    pub tenant: String,
    /// Bearer token with agents and agents_admin.
    pub agents_token: String,
    /// Bearer token with agents_worker.
    pub worker_token: String,
    pub compose_started: bool,
    pub scaffold_command: String,
    pub run_command: String,
}

/// The options `agents dev` runs with, after flag parsing.
#[derive(Debug, Clone)]
pub struct DevOptions {
    pub store_kind: StoreKind,
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
    pub tenant: String,
    /// The store and tenancy flags the server is started with.
    pub server_store_args: Vec<String>,
    pub client_token: String,
    pub worker_token: String,
    pub json: bool,
}

/// `agents dev`. The store kind is checked before anything else runs: a
/// memory store is refused with [`AgentsCliError::MemoryStore`] and no host
/// step is taken.
pub async fn dev<H: DevHost>(
    options: &DevOptions,
    host: &mut H,
    out: &mut dyn Write,
) -> anyhow::Result<DevReady> {
    if options.store_kind == StoreKind::Memory {
        return Err(AgentsCliError::MemoryStore.into());
    }
    for (flag, addr) in [
        ("listen-http", options.listen_http),
        ("listen-grpc", options.listen_grpc),
    ] {
        if !addr.ip().is_loopback() {
            return Err(AgentsCliError::NotLoopback { flag, addr }.into());
        }
    }
    let mut compose_started = false;
    if !host.store_healthy().await {
        host.compose_up().await?;
        compose_started = true;
    }
    host.wait_for_bucket().await?;
    host.qualify().await?;
    let mut args = options.server_store_args.clone();
    args.extend([
        "--agents".to_string(),
        "--listen-http".to_string(),
        options.listen_http.to_string(),
        "--listen-grpc".to_string(),
        options.listen_grpc.to_string(),
    ]);
    args.extend(dev_token_args(
        &options.tenant,
        &options.client_token,
        &options.worker_token,
    ));
    let plan = ServerPlan {
        listen_http: options.listen_http,
        listen_grpc: options.listen_grpc,
        args,
    };
    host.start_server(&plan).await?;
    let url = format!("http://{}", options.listen_http);
    let ready = DevReady {
        scaffold_command: "ravel-cli agents scaffold ./my-agent".to_string(),
        run_command: format!(
            "cd ./my-agent && npm install && RAVEL_URL={url} RAVEL_AGENTS_TOKEN={} \
             RAVEL_WORKER_TOKEN={} npm start",
            options.client_token, options.worker_token
        ),
        url,
        tenant: options.tenant.clone(),
        agents_token: options.client_token.clone(),
        worker_token: options.worker_token.clone(),
        compose_started,
    };
    if options.json {
        write_json(out, &ready)?;
    } else {
        writeln!(out, "ravel-server --agents ready at {}", ready.url)?;
        writeln!(out, "tenant        {}", ready.tenant)?;
        writeln!(out, "agents token  {}", ready.agents_token)?;
        writeln!(out, "worker token  {}", ready.worker_token)?;
        writeln!(out, "Start the assistant example with:")?;
        writeln!(out, "  {}", ready.scaffold_command)?;
        writeln!(out, "  {}", ready.run_command)?;
    }
    out.flush()?;
    host.wait_server().await?;
    Ok(ready)
}

/// The [`DevHost`] that runs docker compose and ravel-server.
pub struct SystemDevHost {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub compose_file: PathBuf,
    pub server_bin: PathBuf,
    /// Environment for the server: the S3 credentials, so they never appear
    /// on its command line.
    pub server_env: Vec<(String, String)>,
    pub ready_timeout: Duration,
    qualify: Option<QualifyRun>,
    child: Option<tokio::process::Child>,
}

/// A `store qualify` run against the selected store, not yet polled.
pub type QualifyRun = std::pin::Pin<Box<dyn Future<Output = anyhow::Result<()>>>>;

impl SystemDevHost {
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        compose_file: PathBuf,
        server_bin: PathBuf,
        server_env: Vec<(String, String)>,
        ready_timeout: Duration,
        qualify: QualifyRun,
    ) -> Self {
        SystemDevHost {
            store,
            compose_file,
            server_bin,
            server_env,
            ready_timeout,
            qualify: Some(qualify),
            child: None,
        }
    }

    async fn compose(&self, args: &[&str]) -> anyhow::Result<()> {
        let status = tokio::process::Command::new("docker")
            .arg("compose")
            .arg("--file")
            .arg(&self.compose_file)
            .args(args)
            .status()
            .await
            .map_err(|e| anyhow::anyhow!("could not run docker compose: {e}"))?;
        if !status.success() {
            anyhow::bail!("docker compose {} failed: {status}", args.join(" "));
        }
        Ok(())
    }
}

impl DevHost for SystemDevHost {
    async fn store_healthy(&mut self) -> bool {
        let probe = self.store.list("sys/", None);
        matches!(
            tokio::time::timeout(Duration::from_secs(5), probe).await,
            Ok(Ok(_))
        )
    }

    async fn compose_up(&mut self) -> anyhow::Result<()> {
        self.compose(&["up", "-d", "rustfs"]).await
    }

    async fn wait_for_bucket(&mut self) -> anyhow::Result<()> {
        self.compose(&["run", "--rm", "createbucket"]).await
    }

    async fn qualify(&mut self) -> anyhow::Result<()> {
        match self.qualify.take() {
            Some(run) => run.await,
            None => Ok(()),
        }
    }

    async fn start_server(&mut self, plan: &ServerPlan) -> anyhow::Result<()> {
        let mut child = tokio::process::Command::new(&self.server_bin)
            .args(&plan.args)
            .envs(self.server_env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                anyhow::anyhow!(
                    "could not start {}: {e}; build it with `cargo build -p ravel-server \
                     --features agents` and pass --server-bin",
                    self.server_bin.display()
                )
            })?;
        let ready_url = format!("http://{}/readyz", plan.listen_http);
        let http = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + self.ready_timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("ravel-server exited before it was ready: {status}");
            }
            let probe = http
                .get(&ready_url)
                .timeout(Duration::from_secs(2))
                .send()
                .await;
            if probe.is_ok_and(|r| r.status().is_success()) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "ravel-server was not ready within {} s",
                    self.ready_timeout.as_secs()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        self.child = Some(child);
        Ok(())
    }

    async fn wait_server(&mut self) -> anyhow::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let status = child.wait().await?;
        if !status.success() {
            anyhow::bail!("ravel-server exited: {status}");
        }
        Ok(())
    }
}

/// The SDK's assistant example, as `agents scaffold` writes it.
pub const SCAFFOLD_FILES: [(&str, &str); 7] = [
    (
        "README.md",
        include_str!("../../../sdk/typescript/examples/assistant/README.md"),
    ),
    (
        "assistant.ts",
        include_str!("../../../sdk/typescript/examples/assistant/assistant.ts"),
    ),
    (
        "fixture.ts",
        include_str!("../../../sdk/typescript/examples/assistant/fixture.ts"),
    ),
    (
        "kill-and-recover.ts",
        include_str!("../../../sdk/typescript/examples/assistant/kill-and-recover.ts"),
    ),
    (
        "main.ts",
        include_str!("../../../sdk/typescript/examples/assistant/main.ts"),
    ),
    (
        "scenario.ts",
        include_str!("../../../sdk/typescript/examples/assistant/scenario.ts"),
    ),
    (
        "ts-hooks.js",
        include_str!("../../../sdk/typescript/examples/ts-hooks.js"),
    ),
];

/// The zod version the example imports, the one `@ravel/agents` pins.
const ZOD_VERSION: &str = "4.6.7";

/// `agents scaffold --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaffoldReport {
    pub dir: String,
    pub files: Vec<String>,
    pub run_command: String,
}

fn package_json(sdk_package: &Path) -> anyhow::Result<String> {
    let value = serde_json::json!({
        "name": "ravel-agents-assistant",
        "private": true,
        "type": "module",
        "engines": {"node": ">=22.15.0 <23"},
        "scripts": {
            "start": "node --experimental-transform-types --disable-warning=ExperimentalWarning --import ./ts-hooks.js main.ts",
            "kill-and-recover": "node --experimental-transform-types --disable-warning=ExperimentalWarning --import ./ts-hooks.js kill-and-recover.ts"
        },
        "dependencies": {
            "@ravel/agents": format!("file:{}", sdk_package.display()),
            "zod": ZOD_VERSION
        }
    });
    Ok(serde_json::to_string_pretty(&value)? + "\n")
}

/// `agents scaffold`: writes the assistant example into `dir`, refusing a
/// `dir` that exists and is not an empty directory.
pub fn scaffold(
    dir: &Path,
    sdk_package: &Path,
    json: bool,
    out: &mut dyn Write,
) -> anyhow::Result<ScaffoldReport> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(AgentsCliError::NotEmpty(dir.to_path_buf()).into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(AgentsCliError::NotEmpty(dir.to_path_buf()).into()),
    }
    if !sdk_package.join("package.json").is_file() {
        return Err(AgentsCliError::NoSdkPackage(sdk_package.to_path_buf()).into());
    }
    let sdk_package = std::fs::canonicalize(sdk_package)?;
    std::fs::create_dir_all(dir)?;
    let mut files = Vec::new();
    for (name, contents) in SCAFFOLD_FILES {
        std::fs::write(dir.join(name), contents)?;
        files.push(name.to_string());
    }
    std::fs::write(dir.join("package.json"), package_json(&sdk_package)?)?;
    files.push("package.json".to_string());
    let report = ScaffoldReport {
        dir: dir.display().to_string(),
        run_command: format!(
            "cd {} && npm install && RAVEL_URL=http://127.0.0.1:4318 RAVEL_AGENTS_TOKEN=<agents token> \
             RAVEL_WORKER_TOKEN=<worker token> npm start",
            dir.display()
        ),
        files,
    };
    if json {
        write_json(out, &report)?;
    } else {
        writeln!(out, "wrote {} files to {}", report.files.len(), report.dir)?;
        writeln!(out, "run it with the tokens `agents dev` printed:")?;
        writeln!(out, "  {}", report.run_command)?;
    }
    Ok(report)
}
