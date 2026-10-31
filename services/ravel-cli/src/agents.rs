//! `ravel-cli agents` (ADR-3331 decision 11).
//!
//! The read commands (`list`, `inspect`, `operation`, `events`, `effects`)
//! read the agent store objects directly through [`AgentStore`], so they work
//! against a bucket with no server running. The one write, `reconcile`, goes
//! through the server's HTTP API with [`AgentsClient`], because settling an
//! effect is a server-side decision that needs an `agents_admin` token.
//! `dev` starts a development server on the compose RustFS store, and
//! `scaffold` runs the TypeScript SDK's generator to write a starter project.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ravel_agents_client::{AgentsClient, ClientError};
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{
    Cancellation, EffectRecord, EffectStatus, Head, InputSource, Lifecycle, ObjectRef, TimerStatus,
    TurnBlock,
};
use ravel_agents_core::identity::{AgentId, effect_id};
use ravel_agents_core::kernel::Usage;
use ravel_agents_core::keys::{AgentKey, AgentKind, dir_prefix};
use ravel_agents_core::state::{Handle, HandleItem, OperationState, operation_state};
use ravel_agents_core::wire::{
    ActiveTurnBody, AgentRef, DestroyAgentResponse, EffectBody, EffectsResponse, EventBody,
    EventsResponse, InputSourceBody, MailboxEntryBody, OperationResponse, ReconcileResponse,
    StateResponse, UsageBody,
};
use ravel_agents_store::{AgentStore, HeadState, StoreLimits};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::{StoreKind, StoreSelection};

/// Default `agents list --limit`.
pub const LIST_LIMIT_DEFAULT: usize = 100;

/// Largest `agents list --limit`; a larger one is refused, not clamped.
pub const LIST_LIMIT_CAP: usize = 1000;

/// Largest `agents events --limit`, the server's page cap.
pub const EVENTS_LIMIT_CAP: u32 = ravel_agents_client::EVENTS_PAGE_CAP;

/// The compose file `agents dev` starts RustFS from.
pub const DEV_COMPOSE_FILE: &str = "deploy/docker-compose/rustfs.yml";

/// The store flags that select the compose RustFS bucket.
pub const DEV_STORE_FLAGS: &str = "--store s3 --s3-endpoint http://127.0.0.1:9000 \
     --s3-bucket ravel-dev --s3-region us-east-1";

/// Capabilities of the dev token: client, worker and operator in one.
pub const DEV_TOKEN_CAPABILITIES: &str = "agents,agents_worker,agents_admin";

/// The tenant the dev token names.
pub const DEV_TENANT: &str = "dev";

/// How long `agents dev` waits for the server's agents health route.
const DEV_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the store probe waits before calling the store down.
const STORE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The SDK's scaffold generator, relative to the repository root.
pub const SCAFFOLD_BIN: &str = "sdk/typescript/packages/agents/bin/ravel-agents-scaffold.js";

/// The command `agents dev` prints for starting a project against it.
pub const DEV_SCAFFOLD_COMMAND: &str = "ravel-cli agents scaffold my-agent";

/// Every refusal `ravel-cli agents` makes before or instead of doing work.
#[derive(Debug, thiserror::Error)]
pub enum AgentsCliError {
    #[error(
        "agents dev refuses the memory store{}: a server on the in-process store \
         loses every agent when it exits. Start against the compose RustFS bucket: \
         {DEV_STORE_FLAGS} with RAVEL_S3_ACCESS_KEY and RAVEL_S3_SECRET_KEY set to \
         the credentials in {DEV_COMPOSE_FILE}",
        if *defaulted { " (no --store given, which means memory)" } else { "" }
    )]
    DevRefusesMemoryStore { defaulted: bool },
    #[error(
        "agents dev refuses --store gcs: it starts, creates and qualifies the compose \
         RustFS bucket ({DEV_COMPOSE_FILE}), which is an S3 endpoint. Against a GCS bucket, \
         qualify it with ravel-cli --store gcs store qualify and start ravel-server on it \
         yourself; the other agents commands take --store gcs"
    )]
    DevRefusesGcsStore,
    #[error("--{flag} {addr} is not a loopback address; agents dev binds loopback only")]
    ListenNotLoopback {
        flag: &'static str,
        addr: SocketAddr,
    },
    #[error("--limit {limit} is above the cap of {cap}")]
    LimitOverCap { limit: usize, cap: usize },
    #[error("--limit must be at least 1")]
    LimitZero,
    #[error("{} exists and is not empty; scaffold writes only into a new or empty directory", dir.display())]
    ScaffoldDirNotEmpty { dir: PathBuf },
    #[error(
        "scaffold generator {} not found; run from the repository root or pass --scaffold-bin",
        bin.display()
    )]
    ScaffoldBinMissing { bin: PathBuf },
    #[error("{} could not start: {reason}", node.display())]
    ScaffoldSpawn { node: PathBuf, reason: String },
    #[error(
        "scaffold generator {} exited with {}: {}",
        bin.display(),
        match code { Some(c) => format!("code {c}"), None => "a signal".to_string() },
        stderr.trim_end()
    )]
    ScaffoldFailed {
        bin: PathBuf,
        code: Option<i32>,
        stderr: String,
    },
    #[error("agent id {0:?} is not 32 hex characters")]
    InvalidAgentId(String),
    #[error("agent kind or key is not valid")]
    InvalidAgent,
    #[error("name an agent by its id, or by --kind and --key")]
    NoAgent,
    #[error("{0:?} is not an operation handle")]
    MalformedOperation(String),
    #[error("events cursor {0:?} is not a decimal event sequence")]
    BadCursor(String),
    #[error("list cursor {0:?} is not an agent id this command printed")]
    BadListCursor(String),
    #[error("--attach-result is not JSON: {0}")]
    BadResult(String),
    #[error("{0}")]
    Agent(#[from] AgentError),
    #[error("{0}")]
    Client(#[from] ClientError),
}

/// Which agent a read names: the id `list` prints, or its kind and key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSelector {
    Id(String),
    KindKey { kind: String, key: String },
}

impl AgentSelector {
    pub fn from_flags(
        id: Option<String>,
        kind: Option<String>,
        key: Option<String>,
    ) -> Result<Self, AgentsCliError> {
        match (id, kind, key) {
            (Some(id), None, None) => Ok(AgentSelector::Id(id)),
            (None, Some(kind), Some(key)) => Ok(AgentSelector::KindKey { kind, key }),
            _ => Err(AgentsCliError::NoAgent),
        }
    }

    pub fn resolve(&self) -> Result<AgentId, AgentsCliError> {
        match self {
            AgentSelector::Id(id) => {
                AgentId::from_hex(id).ok_or_else(|| AgentsCliError::InvalidAgentId(id.clone()))
            }
            AgentSelector::KindKey { kind, key } => {
                let kind =
                    AgentKind::new(kind.as_str()).map_err(|_| AgentsCliError::InvalidAgent)?;
                let key = AgentKey::new(key.as_str()).map_err(|_| AgentsCliError::InvalidAgent)?;
                Ok(ravel_agents_core::identity::agent_id(&kind, &key))
            }
        }
    }
}

/// The store the read commands go through, for one tenant.
pub fn reader(backend: Arc<dyn ObjectStoreBackend>, tenant: TenantHash) -> AgentStore {
    AgentStore::with_system_clock(backend, tenant, StoreLimits::default())
}

fn ids_for(agent: &AgentId) -> SafeIds {
    SafeIds {
        agent_id: Some(agent.to_hex()),
        ..SafeIds::default()
    }
}

/// HEAD of a live agent with every sealed page attached, refused the way the
/// server's routes refuse it.
async fn live_head(store: &AgentStore, agent: &AgentId) -> Result<Head, AgentsCliError> {
    live(agent, store.read_head_full(agent).await?)
}

/// [`live_head`] in one GET, sealed pages not attached.
async fn live_root(store: &AgentStore, agent: &AgentId) -> Result<Head, AgentsCliError> {
    live(agent, store.read_head_shallow(agent).await?)
}

fn live(agent: &AgentId, state: HeadState) -> Result<Head, AgentsCliError> {
    match state {
        HeadState::Present { head, .. } if head.lifecycle != Lifecycle::Tombstoned => Ok(*head),
        HeadState::Present { .. } | HeadState::Absent => {
            Err(
                AgentError::new(ErrorKind::AgentGone, ids_for(agent), "agent does not exist")
                    .into(),
            )
        }
        HeadState::Unsupported { .. } => Err(AgentError::new(
            ErrorKind::UnsupportedFormat,
            ids_for(agent),
            "HEAD format version is newer than this release",
        )
        .into()),
    }
}

async fn read_json(
    store: &AgentStore,
    agent: &AgentId,
    dep: Option<ObjectRef>,
) -> Result<Value, AgentsCliError> {
    let Some(dep) = dep else {
        return Ok(Value::Null);
    };
    let bytes = store.read_object(agent, dep).await?;
    serde_json::from_slice(&bytes).map_err(|_| {
        AgentError::new(
            ErrorKind::UnsupportedFormat,
            ids_for(agent),
            "object is not a JSON value",
        )
        .into()
    })
}

/// `agents inspect`: the state route's body plus what only HEAD shows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InspectOutput {
    #[serde(flatten)]
    pub state: StateResponse,
    pub kind: String,
    pub key: String,
    pub root_revision: u64,
    pub owner_id: String,
    pub epoch: u64,
    pub next_accept_seq: u64,
    pub mailbox_depth: usize,
    pub timers: Vec<TimerOutput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerOutput {
    pub timer_id: String,
    pub timer_key: String,
    pub generation: u64,
    pub due_unix_ns: String,
    pub handler: String,
    pub status: String,
}

pub async fn inspect(store: &AgentStore, agent: AgentId) -> Result<InspectOutput, AgentsCliError> {
    let head = live_head(store, &agent).await?;
    let state = read_json(store, &agent, head.state).await?;
    let timers = head
        .timers
        .iter()
        .map(|timer| TimerOutput {
            timer_id: timer.timer_id.to_hex(),
            timer_key: timer.timer_key.clone(),
            generation: timer.generation,
            due_unix_ns: timer.due_unix_ns.to_string(),
            handler: timer.handler.clone(),
            status: match timer.status {
                TimerStatus::Scheduled => "scheduled",
                TimerStatus::Delivered => "delivered",
                TimerStatus::Cancelled => "cancelled",
            }
            .to_string(),
        })
        .collect();
    Ok(InspectOutput {
        state: StateResponse {
            agent_id: agent.to_hex(),
            incarnation: head.incarnation,
            state_version: head.state_version,
            content_type: "application/json".to_string(),
            state,
            active_turn: head.active_turn.as_ref().map(|turn| ActiveTurnBody {
                turn_id: turn.turn_id.to_hex(),
                accept_seq: turn.input_accept_seq,
                handler: turn.handler.clone(),
                handler_version: turn.handler_version.clone(),
                handler_digest: turn.handler_digest.to_hex(),
                base_state_version: turn.base_state_version,
                started_unix_ns: turn.started_unix_ns,
                lease_hint_unix_ns: head.owner_lease_hint_unix_ns,
                cancel_requested: turn.cancellation == Cancellation::Requested,
                non_deterministic_replay: turn.block == TurnBlock::NonDeterministicReplay,
            }),
            mailbox: head
                .inbox
                .iter()
                .map(|entry| MailboxEntryBody {
                    accept_seq: entry.accept_seq,
                    handler: entry.handler.clone(),
                    source: match &entry.source {
                        InputSource::Message { request_id, .. } => InputSourceBody::Message {
                            request_id: request_id.clone(),
                        },
                        InputSource::Timer {
                            timer_id,
                            generation,
                        } => InputSourceBody::Timer {
                            timer: timer_id.to_hex(),
                            generation: *generation,
                        },
                    },
                    accepted_unix_ns: entry.accepted_unix_ns,
                    failed_attempts: entry.failed_attempts,
                })
                .collect(),
            usage: Some(UsageBody::new(
                Usage::of(&head),
                &StoreLimits::default().kernel,
            )),
        },
        kind: head.kind.as_str().to_string(),
        key: head.key.as_str().to_string(),
        root_revision: head.root_revision,
        owner_id: head.owner_id.clone(),
        epoch: head.epoch,
        next_accept_seq: head.next_accept_seq,
        mailbox_depth: head.inbox.len(),
        timers,
    })
}

pub fn render_inspect(out: &InspectOutput) -> String {
    let mut text = format!(
        "agent {} ({} / {})\nincarnation {}  root revision {}  state version {}\n\
         owner {:?}  epoch {}  mailbox {}  next accept seq {}\n",
        out.state.agent_id,
        out.kind,
        out.key,
        out.state.incarnation,
        out.root_revision,
        out.state.state_version,
        out.owner_id,
        out.epoch,
        out.mailbox_depth,
        out.next_accept_seq,
    );
    match &out.state.active_turn {
        None => text.push_str("active turn: none\n"),
        Some(turn) => text.push_str(&format!(
            "active turn: {} handler {}@{} input {} base state {} started {}{}{}\n",
            turn.turn_id,
            turn.handler,
            turn.handler_version,
            turn.accept_seq,
            turn.base_state_version,
            turn.started_unix_ns,
            if turn.cancel_requested {
                " cancel-requested"
            } else {
                ""
            },
            if turn.non_deterministic_replay {
                " blocked: non-deterministic replay"
            } else {
                ""
            },
        )),
    }
    if let Some(usage) = &out.state.usage {
        text.push_str(&format!(
            "usage (default caps): receipts {}/{}  sealed pages results {} events {} timers {} of {}  \
             head bytes {}/{}\n",
            usage.receipts,
            usage.max_receipts,
            usage.results_pages,
            usage.events_pages,
            usage.timers_pages,
            usage.max_index_pages,
            usage.head_bytes,
            usage.max_head_bytes,
        ));
    }
    text.push_str(&format!("timers: {}\n", out.timers.len()));
    for timer in &out.timers {
        text.push_str(&format!(
            "  {} gen {} due {} handler {} {}\n",
            timer.timer_key, timer.generation, timer.due_unix_ns, timer.handler, timer.status
        ));
    }
    text.push_str(&format!("state: {}\n", out.state.state));
    text
}

/// `agents operation`: the operation route's body, read from HEAD.
pub async fn operation(
    store: &AgentStore,
    handle_text: &str,
) -> Result<OperationResponse, AgentsCliError> {
    let malformed = || AgentsCliError::MalformedOperation(handle_text.to_string());
    let handle = Handle::decode_kind(handle_text, "op").map_err(|_| malformed())?;
    let HandleItem::Operation(seq) = handle.item else {
        return Err(malformed());
    };
    let gone = || {
        AgentError::new(
            ErrorKind::AgentGone,
            SafeIds {
                operation: Some(handle.encode()),
                ..ids_for(&handle.agent)
            },
            "operation is not known to this agent",
        )
    };
    let head = match store.read_head_full(&handle.agent).await? {
        HeadState::Present { head, .. } if head.lifecycle != Lifecycle::Tombstoned => *head,
        HeadState::Present { .. } => return Err(gone().into()),
        HeadState::Absent => {
            return Err(AgentError::new(
                ErrorKind::OutcomeUnknown,
                SafeIds {
                    operation: Some(handle.encode()),
                    ..ids_for(&handle.agent)
                },
                "HEAD could not be read; resolve the operation again",
            )
            .into());
        }
        HeadState::Unsupported { .. } => {
            return Err(AgentError::new(
                ErrorKind::UnsupportedFormat,
                ids_for(&handle.agent),
                "HEAD format version is newer than this release",
            )
            .into());
        }
    };
    if head.incarnation != handle.incarnation {
        return Err(gone().into());
    }
    let state = operation_state(&head, seq).ok_or_else(gone)?;
    let blocked_effect = (state == OperationState::Blocked)
        .then(|| {
            head.effects.iter().find(|e| {
                e.status == EffectStatus::InDoubt
                    && e.effect_id == effect_id(head.incarnation, seq, &e.step_key)
            })
        })
        .flatten()
        .map(|e| effect_handle(handle.agent, head.incarnation, e));
    Ok(OperationResponse {
        operation: handle_text.to_string(),
        status: state.into(),
        accept_seq: seq,
        blocked_effect,
    })
}

pub fn render_operation(out: &OperationResponse) -> String {
    let status = serde_json::to_value(out.status)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    let mut text = format!(
        "operation {}\nstatus {status}  accept seq {}\n",
        out.operation, out.accept_seq
    );
    if let Some(effect) = &out.blocked_effect {
        text.push_str(&format!("blocked on effect {effect}\n"));
    }
    text
}

/// `agents events`: one page after `cursor`, as the events route pages it.
pub async fn events(
    store: &AgentStore,
    agent: AgentId,
    cursor: Option<&str>,
    limit: u32,
) -> Result<EventsResponse, AgentsCliError> {
    if limit == 0 {
        return Err(AgentsCliError::LimitZero);
    }
    if limit > EVENTS_LIMIT_CAP {
        return Err(AgentsCliError::LimitOverCap {
            limit: limit as usize,
            cap: EVENTS_LIMIT_CAP as usize,
        });
    }
    let after = match cursor {
        None => 0,
        Some(text) => text
            .parse::<u64>()
            .map_err(|_| AgentsCliError::BadCursor(text.to_string()))?,
    };
    let head = live_root(store, &agent).await?;
    let mut page = Vec::new();
    for event in store
        .events_in(&agent, head, after, limit as usize, None)
        .await?
    {
        page.push(EventBody {
            event_seq: event.event_seq,
            accept_seq: event.accept_seq,
            turn_id: event.turn_id.to_hex(),
            event_type: event.event_type.clone(),
            payload: read_json(store, &agent, event.payload).await?,
        });
    }
    let next = page.last().map_or(after, |e| e.event_seq);
    Ok(EventsResponse {
        events: page,
        next_cursor: next.to_string(),
        terminal: None,
    })
}

pub fn render_events(out: &EventsResponse) -> String {
    let mut text = String::new();
    for event in &out.events {
        text.push_str(&format!(
            "{} {} accept {} turn {} {}\n",
            event.event_seq, event.event_type, event.accept_seq, event.turn_id, event.payload
        ));
    }
    text.push_str(&format!(
        "{} events; next cursor {}\n",
        out.events.len(),
        out.next_cursor
    ));
    text
}

fn effect_handle(agent: AgentId, incarnation: u64, record: &EffectRecord) -> String {
    Handle {
        agent,
        incarnation,
        item: HandleItem::Effect(record.effect_id),
    }
    .encode()
}

/// `agents effects`: every effect HEAD journals.
pub async fn effects(
    store: &AgentStore,
    agent: AgentId,
) -> Result<EffectsResponse, AgentsCliError> {
    let head = live_head(store, &agent).await?;
    Ok(EffectsResponse {
        effects: head
            .effects
            .iter()
            .map(|e| EffectBody {
                effect: effect_handle(agent, head.incarnation, e),
                turn_id: e.turn_id.to_hex(),
                tool_name: e.tool_name.clone(),
                tool_version: e.tool_version.clone(),
                policy: e.policy.into(),
                status: e.status.into(),
                attempt: e.attempt,
            })
            .collect(),
    })
}

fn wire_word<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

pub fn render_effects(out: &EffectsResponse) -> String {
    let mut text = String::new();
    for effect in &out.effects {
        text.push_str(&format!(
            "{} {}@{} {} {} attempt {}\n",
            effect.effect,
            effect.tool_name,
            effect.tool_version,
            wire_word(&effect.policy),
            wire_word(&effect.status),
            effect.attempt
        ));
    }
    text.push_str(&format!("{} effects\n", out.effects.len()));
    text
}

/// `agents list`: one bounded page of the tenant's agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListOutput {
    pub agents: Vec<ListEntry>,
    /// Pass as `--cursor` for the next page; absent on the last page.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListEntry {
    pub agent_id: String,
    /// `active`, `tombstoned`, `absent` (a directory entry whose HEAD is not
    /// written) or `unsupported` (a HEAD newer than this release).
    pub lifecycle: String,
    pub kind: Option<String>,
    pub key: Option<String>,
    pub incarnation: Option<u64>,
    pub state_version: Option<u64>,
}

pub fn check_list_limit(limit: usize) -> Result<(), AgentsCliError> {
    if limit == 0 {
        return Err(AgentsCliError::LimitZero);
    }
    if limit > LIST_LIMIT_CAP {
        return Err(AgentsCliError::LimitOverCap {
            limit,
            cap: LIST_LIMIT_CAP,
        });
    }
    Ok(())
}

pub async fn list(
    store: &AgentStore,
    tenant: &TenantHash,
    cursor: Option<&str>,
    limit: usize,
) -> Result<ListOutput, AgentsCliError> {
    check_list_limit(limit)?;
    let prefix = dir_prefix(tenant).map_err(|_| {
        AgentError::new(
            ErrorKind::StoreUnavailable,
            SafeIds::default(),
            "tenant prefix is not addressable",
        )
    })?;
    let mut start_after = match cursor {
        None => None,
        Some(text) if !text.is_empty() && !text.contains('/') => Some(format!("{prefix}{text}")),
        Some(text) => return Err(AgentsCliError::BadListCursor(text.to_string())),
    };
    let mut agents = Vec::new();
    let mut next_cursor = None;
    // A page that lists at least one agent adds one entry, so `limit` pages
    // fill the limit; the extra one lets a page of malformed keys pass.
    for _ in 0..=limit {
        let page = store.list_dir_after(start_after.as_deref()).await?;
        let mut cut = false;
        for entry in page.items {
            if agents.len() == limit {
                cut = true;
                break;
            }
            agents.push(list_entry(store, entry.agent).await?);
            start_after = Some(entry.key);
        }
        if cut {
            next_cursor = start_after.as_deref().map(|k| suffix(&prefix, k));
            break;
        }
        if !page.more {
            break;
        }
        if page.last_key.is_some() {
            start_after = page.last_key;
        }
        if agents.len() == limit {
            next_cursor = start_after.as_deref().map(|k| suffix(&prefix, k));
            break;
        }
    }
    Ok(ListOutput {
        agents,
        next_cursor,
    })
}

fn suffix(prefix: &str, key: &str) -> String {
    key.strip_prefix(prefix).unwrap_or(key).to_string()
}

async fn list_entry(store: &AgentStore, agent: AgentId) -> Result<ListEntry, AgentsCliError> {
    let mut entry = ListEntry {
        agent_id: agent.to_hex(),
        lifecycle: String::new(),
        kind: None,
        key: None,
        incarnation: None,
        state_version: None,
    };
    match store.read_head_shallow(&agent).await? {
        HeadState::Present { head, .. } => {
            entry.lifecycle = match head.lifecycle {
                Lifecycle::Active => "active",
                Lifecycle::Tombstoned => "tombstoned",
            }
            .to_string();
            entry.kind = Some(head.kind.as_str().to_string());
            entry.key = Some(head.key.as_str().to_string());
            entry.incarnation = Some(head.incarnation);
            entry.state_version = Some(head.state_version);
        }
        HeadState::Absent => entry.lifecycle = "absent".to_string(),
        HeadState::Unsupported { .. } => entry.lifecycle = "unsupported".to_string(),
    }
    Ok(entry)
}

pub fn render_list(out: &ListOutput) -> String {
    let mut text = String::new();
    for agent in &out.agents {
        text.push_str(&format!(
            "{} {} {} {} incarnation {} state version {}\n",
            agent.agent_id,
            agent.lifecycle,
            agent.kind.as_deref().unwrap_or("-"),
            agent.key.as_deref().unwrap_or("-"),
            agent
                .incarnation
                .map_or_else(|| "-".to_string(), |v| v.to_string()),
            agent
                .state_version
                .map_or_else(|| "-".to_string(), |v| v.to_string()),
        ));
    }
    match &out.next_cursor {
        Some(cursor) => text.push_str(&format!(
            "{} agents; more after --cursor {cursor}\n",
            out.agents.len()
        )),
        None => text.push_str(&format!("{} agents; end of list\n", out.agents.len())),
    }
    text
}

/// What `agents reconcile` decides.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileChoice {
    /// The operator confirmed the effect's result out of band.
    AttachResult(Value),
    /// Run the effect once more.
    AuthorizeAttempt,
}

impl ReconcileChoice {
    pub fn parse(
        attach_result: Option<&str>,
        authorize_attempt: bool,
    ) -> Result<Self, AgentsCliError> {
        match (attach_result, authorize_attempt) {
            (Some(text), false) => serde_json::from_str(text)
                .map(ReconcileChoice::AttachResult)
                .map_err(|e| AgentsCliError::BadResult(e.to_string())),
            (None, true) => Ok(ReconcileChoice::AuthorizeAttempt),
            _ => Err(AgentsCliError::BadResult(
                "pass exactly one of --attach-result and --authorize-attempt".to_string(),
            )),
        }
    }
}

/// `agents reconcile`, through the server.
pub async fn reconcile(
    client: &AgentsClient,
    effect: &str,
    attempt: u32,
    choice: ReconcileChoice,
) -> Result<ReconcileResponse, AgentsCliError> {
    let response = match choice {
        ReconcileChoice::AttachResult(result) => {
            client
                .reconcile_attach_result(effect, attempt, result)
                .await?
        }
        ReconcileChoice::AuthorizeAttempt => {
            client.reconcile_authorize_attempt(effect, attempt).await?
        }
    };
    Ok(response)
}

pub fn render_reconcile(out: &ReconcileResponse) -> String {
    format!(
        "effect {} {} attempt {}\n",
        out.effect,
        wire_word(&out.status),
        out.attempt
    )
}

/// `agents destroy`, through the server. The kind and key are checked the
/// way [`AgentSelector::resolve`] checks them before any request is sent.
pub async fn destroy(
    client: &AgentsClient,
    kind: &str,
    key: &str,
    incarnation: Option<u64>,
) -> Result<DestroyAgentResponse, AgentsCliError> {
    AgentKind::new(kind).map_err(|_| AgentsCliError::InvalidAgent)?;
    AgentKey::new(key).map_err(|_| AgentsCliError::InvalidAgent)?;
    let agent = AgentRef {
        kind: kind.to_string(),
        key: key.to_string(),
    };
    Ok(client.destroy(&agent, incarnation).await?)
}

pub fn render_destroy(out: &DestroyAgentResponse) -> String {
    let abandoned = &out.abandoned;
    format!(
        "agent {} incarnation {} {}\n\
         abandoned: inputs {}, active turn {}, effects in doubt {}, effects authorized {}, timers scheduled {}\n",
        out.agent_id,
        out.incarnation,
        if out.destroyed {
            "destroyed"
        } else {
            "already destroyed"
        },
        abandoned.inputs,
        if abandoned.active_turn { "yes" } else { "no" },
        abandoned.effects_in_doubt,
        abandoned.effects_authorized,
        abandoned.timers_scheduled,
    )
}

/// Print `value` as pretty JSON under `--json`, else as `text`.
pub fn emit<T: Serialize>(
    out: &mut dyn Write,
    json: bool,
    value: &T,
    text: impl FnOnce(&T) -> String,
) -> anyhow::Result<()> {
    if json {
        writeln!(out, "{}", serde_json::to_string_pretty(value)?)?;
    } else {
        write!(out, "{}", text(value))?;
    }
    Ok(())
}

/// `agents scaffold <dir>`: run the SDK generator as `node <bin> <dir>`.
/// A `dir` that exists and is not empty is refused before anything is
/// spawned. Returns the generator's stdout.
pub fn scaffold(node: &Path, bin: &Path, dir: &Path) -> Result<String, AgentsCliError> {
    if let Ok(mut entries) = std::fs::read_dir(dir)
        && entries.next().is_some()
    {
        return Err(AgentsCliError::ScaffoldDirNotEmpty {
            dir: dir.to_path_buf(),
        });
    }
    if !bin.is_file() {
        return Err(AgentsCliError::ScaffoldBinMissing {
            bin: bin.to_path_buf(),
        });
    }
    let output = std::process::Command::new(node)
        .arg(bin)
        .arg(dir)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| AgentsCliError::ScaffoldSpawn {
            node: node.to_path_buf(),
            reason: e.to_string(),
        })?;
    if !output.status.success() {
        return Err(AgentsCliError::ScaffoldFailed {
            bin: bin.to_path_buf(),
            code: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    std::io::stderr().write_all(&output.stderr).ok();
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// What `agents scaffold --json` prints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaffoldOutput {
    pub dir: String,
    /// The generator's stdout: the files it wrote and the next command.
    pub output: String,
}

/// The settings `agents dev` runs with.
#[derive(Debug, Clone, PartialEq)]
pub struct DevOptions {
    pub store: StoreSelection,
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
}

/// The refusal `agents dev` makes before it starts anything.
pub fn check_dev(options: &DevOptions) -> Result<(), AgentsCliError> {
    if options.store.kind() == StoreKind::Memory {
        return Err(AgentsCliError::DevRefusesMemoryStore {
            defaulted: options.store.is_defaulted_memory(),
        });
    }
    if options.store.kind() == StoreKind::Gcs {
        return Err(AgentsCliError::DevRefusesGcsStore);
    }
    for (flag, addr) in [
        ("listen", options.listen_http),
        ("listen-grpc", options.listen_grpc),
    ] {
        if !addr.ip().is_loopback() {
            return Err(AgentsCliError::ListenNotLoopback { flag, addr });
        }
    }
    Ok(())
}

/// The side effects of `agents dev`, so the order they run in is testable
/// without docker or a server.
#[allow(async_fn_in_trait)]
pub trait DevHost {
    /// Whether the configured bucket answers a listing.
    async fn store_answers(&mut self) -> bool;
    /// `docker compose up -d rustfs`.
    async fn compose_up(&mut self) -> anyhow::Result<()>;
    /// `docker compose run --rm createbucket`, which waits for RustFS.
    async fn create_bucket(&mut self) -> anyhow::Result<()>;
    /// `ravel-cli store qualify` against the bucket.
    async fn qualify(&mut self) -> anyhow::Result<()>;
    /// Start `ravel-server --agents` with `token` and wait until its agents
    /// health route answers.
    async fn start_server(&mut self, token: &str) -> anyhow::Result<()>;
    /// Wait for the server to exit.
    async fn wait_server(&mut self) -> anyhow::Result<()>;
}

/// What `agents dev` prints once the server is ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevReady {
    pub url: String,
    pub tenant: String,
    pub token: String,
    pub capabilities: Vec<String>,
    /// The command that writes a starter project to run against the server.
    pub scaffold: String,
}

pub fn render_dev_ready(ready: &DevReady) -> String {
    format!(
        "agents dev server ready (tenant {}, capabilities {}); stop it with Ctrl-C\n\
         RAVEL_URL={}\nRAVEL_AGENTS_TOKEN={}\nRAVEL_WORKER_TOKEN={}\n\
         start a project with: {}\n",
        ready.tenant,
        ready.capabilities.join(","),
        ready.url,
        ready.token,
        ready.token,
        ready.scaffold
    )
}

/// `agents dev`. The store kind and the listeners are checked before any
/// side effect, so a refused run starts nothing.
pub async fn run_dev<H: DevHost>(
    options: &DevOptions,
    host: &mut H,
    token: &str,
    out: &mut dyn Write,
    json: bool,
) -> anyhow::Result<()> {
    check_dev(options)?;
    if !host.store_answers().await {
        host.compose_up().await?;
    }
    host.create_bucket().await?;
    host.qualify().await?;
    host.start_server(token).await?;
    let ready = DevReady {
        url: format!("http://{}", options.listen_http),
        tenant: DEV_TENANT.to_string(),
        token: token.to_string(),
        capabilities: DEV_TOKEN_CAPABILITIES
            .split(',')
            .map(str::to_string)
            .collect(),
        scaffold: DEV_SCAFFOLD_COMMAND.to_string(),
    };
    emit(out, json, &ready, render_dev_ready)?;
    out.flush()?;
    host.wait_server().await
}

/// The [`DevHost`] that runs docker, this binary and `ravel-server`.
pub struct ProcessDevHost {
    pub compose_file: PathBuf,
    pub server_bin: PathBuf,
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
    /// The store flags, without credentials, for both child processes.
    pub store_flags: Vec<String>,
    /// `--tenant-hash-key-file PATH` or `--tenant-hash-unkeyed`.
    pub tenancy_flags: Vec<String>,
    /// Credentials, handed to the children through the environment only.
    pub store_env: Vec<(String, String)>,
    pub probe: Arc<dyn ObjectStoreBackend>,
    pub child: Option<std::process::Child>,
    pub token_dir: Option<PathBuf>,
}

impl ProcessDevHost {
    fn run_to_stderr(&self, mut command: std::process::Command, what: &str) -> anyhow::Result<()> {
        command
            .stdout(std::process::Stdio::from(std::io::stderr()))
            .envs(self.store_env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let exit = command
            .status()
            .map_err(|e| anyhow::anyhow!("{what} could not start: {e}"))?;
        if !exit.success() {
            anyhow::bail!("{what} failed: {exit}");
        }
        Ok(())
    }

    fn compose(&self) -> std::process::Command {
        let mut command = std::process::Command::new("docker");
        command.arg("compose").arg("--file").arg(&self.compose_file);
        command
    }

    fn write_token_file(&mut self, token: &str) -> anyhow::Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!("ravel-agents-dev-{}", uuid::Uuid::new_v4()));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&dir)?;
        self.token_dir = Some(dir.clone());
        let file = dir.join("tokens");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut handle = options.open(&file)?;
        writeln!(handle, "{token}={DEV_TENANT};{DEV_TOKEN_CAPABILITIES}")?;
        Ok(file)
    }
}

impl Drop for ProcessDevHost {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(dir) = &self.token_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

impl DevHost for ProcessDevHost {
    async fn store_answers(&mut self) -> bool {
        let probe = self.probe.list("sys/", None);
        matches!(
            tokio::time::timeout(STORE_PROBE_TIMEOUT, probe).await,
            Ok(Ok(_))
        )
    }

    async fn compose_up(&mut self) -> anyhow::Result<()> {
        let mut command = self.compose();
        command.args(["up", "--detach", "rustfs"]);
        self.run_to_stderr(command, "docker compose up rustfs")
    }

    async fn create_bucket(&mut self) -> anyhow::Result<()> {
        let mut command = self.compose();
        command.args(["run", "--rm", "createbucket"]);
        self.run_to_stderr(command, "docker compose run createbucket")
    }

    async fn qualify(&mut self) -> anyhow::Result<()> {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command.args(&self.store_flags).args(["store", "qualify"]);
        self.run_to_stderr(command, "ravel-cli store qualify")
    }

    async fn start_server(&mut self, token: &str) -> anyhow::Result<()> {
        let token_file = self.write_token_file(token)?;
        let mut command = std::process::Command::new(&self.server_bin);
        command
            .args(&self.store_flags)
            .args(&self.tenancy_flags)
            .args(["--mode", "all", "--agents"])
            .arg("--listen-http")
            .arg(self.listen_http.to_string())
            .arg("--listen-grpc")
            .arg(self.listen_grpc.to_string())
            .arg("--tenant-token-file")
            .arg(&token_file)
            .stdout(std::process::Stdio::from(std::io::stderr()))
            .envs(self.store_env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let child = command
            .spawn()
            .map_err(|e| anyhow::anyhow!("{} could not start: {e}", self.server_bin.display()))?;
        self.child = Some(child);
        let client = AgentsClient::new(&format!("http://{}", self.listen_http), "")?;
        let deadline = tokio::time::Instant::now() + DEV_READY_TIMEOUT;
        loop {
            if let Some(child) = self.child.as_mut()
                && let Some(exit) = child.try_wait()?
            {
                anyhow::bail!("ravel-server exited before it was ready: {exit}");
            }
            if client.health().await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "ravel-server did not answer /api/v1/agents/health within {} s",
                    DEV_READY_TIMEOUT.as_secs()
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_server(&mut self) -> anyhow::Result<()> {
        loop {
            let Some(child) = self.child.as_mut() else {
                return Ok(());
            };
            if let Some(exit) = child.try_wait()? {
                self.child = None;
                if exit.success() {
                    return Ok(());
                }
                anyhow::bail!("ravel-server exited: {exit}");
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }
}
