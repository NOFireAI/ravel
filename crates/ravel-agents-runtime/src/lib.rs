//! Ravel Agents coordinator (ADR-3331 decisions 6 to 9).
//!
//! [`Coordinator`] drives the transition kernel of `ravel-agents-core`
//! through the publication skeleton of `ravel-agents-store` on behalf of
//! workers: it activates agents and takes them over under a new ownership
//! epoch, leases each worker the oldest input of an agent its handler pins
//! can serve, publishes the worker's completion or failure under the fence
//! the lease carries, and drops idle agents from memory.
//!
//! Every durable fact lives in the agent's HEAD. The coordinator's own maps
//! (loaded roots, the lease table) are caches: a fresh
//! coordinator over an existing store resumes every agent from HEAD alone
//! (`tests::cold_restart_resumes_the_active_turn_from_head_alone`).
//!
//! Timers ([`Coordinator::schedule_timer`], [`Coordinator::cancel_timer`])
//! fire only from the reconciliation sweep in `sweep`, which reads HEAD for
//! every agent it acts on. The sweep runs from [`Coordinator::sweep_once`]
//! and from worker polls on an injected interval ([`SweepConfig`]); it is
//! also how a poll finds an agent given work through another coordinator.
//! `ravel-server` runs it per tenant on `agents.sweep_interval_ms`. The
//! simulation crate (T13, #3346) drives the coordinator directly.
//!
//! The bodies are the `ravel_agents_core::wire` types wherever one exists.
//! Worker registration, heartbeat, activation, takeover and a failure's
//! retry verdict take and return crate types; where `ravel-server`'s agents
//! routes (`services/ravel-server/src/agents/routes.rs`, T11 #3345) expose
//! one, the route maps its wire body onto them.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use ravel_agents_core::command::{
    Cancel, CompleteTurn, EventSpec, HandlerPin, IncarnationFence, IntentSpec, OwnerFence,
    TimerChange, TimerSpec, TurnFence,
};
use ravel_agents_core::error::{AgentError, ErrorKind, SafeIds};
use ravel_agents_core::head::{Head, ObjectRef};
use ravel_agents_core::identity::{AgentId, Hash256, Id128, agent_id, step_input_hash};
use ravel_agents_core::kernel::{Effect, Reply};
use ravel_agents_core::keys::{AgentKey, AgentKind};
use ravel_agents_core::state::Handle;
use ravel_agents_core::wire::{
    AgentRef, CancelRequest, CancelResponse, CreateAgentRequest, CreateAgentResponse,
    HandlerPinBody, OperationStatus, SendRequest, SendResponse, TimerChangeBody, TurnLease,
    TurnOutcome, WorkerCommitRequest, WorkerCommitResponse, WorkerPollRequest,
};
use ravel_agents_store::{AgentStore, Clock, HeadState, Outcome};
use serde_json::Value;
use tokio::sync::Notify;

mod dispatch;
mod effects;
mod failure;
mod metrics;
mod ownership;
mod sweep;
mod timers;
mod unload;

pub use effects::{
    Dispatch, EffectDispatcher, EffectRef, EffectResult, EffectSettled, FAILURE_RESULT_PREFIX,
    StepJournal, StepProgress, StepSpec, UNKNOWN_OUTCOME_RETRIES,
};
pub use failure::FailOutcome;
pub use metrics::{AgentsMetricsSnapshot, SweepMetricsSnapshot};
pub use ownership::TakeoverReason;
pub use sweep::{
    DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_SWEEP_MAX_HEAD_GETS, DEFAULT_SWEEP_MAX_LIST_PAGES,
    SweepConfig, SweepReport,
};

use metrics::Metrics;
use ownership::LeaseTable;

/// Randomness seam: the coordinator draws worker ids from it and nothing
/// else.
pub trait Rng: Send + Sync {
    fn next_u64(&self) -> u64;
}

/// The production [`Rng`]: splitmix64 over a counter seeded from the
/// process's hash-map key randomness.
#[derive(Debug)]
pub struct SystemRng(AtomicU64);

impl SystemRng {
    pub fn new() -> SystemRng {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(0);
        SystemRng(AtomicU64::new(hasher.finish()))
    }
}

impl Default for SystemRng {
    fn default() -> Self {
        SystemRng::new()
    }
}

impl Rng for SystemRng {
    fn next_u64(&self) -> u64 {
        let mut z = self
            .0
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Coordinator tuning. All durations are milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// How long a lease runs from its grant or last heartbeat.
    pub lease_ms: u64,
    /// How long past expiry another worker waits before taking over.
    pub heartbeat_grace_ms: u64,
    /// Failed attempts at one input before it is dead-lettered.
    pub max_turn_attempts: u32,
    /// How long an agent with no work stays loaded.
    pub idle_unload_after_ms: u64,
    /// Upper bound on one worker long-poll.
    pub poll_wait_cap_ms: u64,
}

/// The long-poll cap of ADR-3331 decision 9.
pub const DEFAULT_POLL_WAIT_CAP_MS: u64 = 20_000;

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            lease_ms: 30_000,
            heartbeat_grace_ms: 10_000,
            max_turn_attempts: 3,
            idle_unload_after_ms: 300_000,
            poll_wait_cap_ms: DEFAULT_POLL_WAIT_CAP_MS,
        }
    }
}

/// One loaded agent: the newest root this coordinator has seen.
#[derive(Debug, Clone)]
pub(crate) struct Loaded {
    pub(crate) head: Head,
    pub(crate) touched_unix_ns: i64,
    /// A poll is dispatching this agent; other polls skip it.
    pub(crate) dispatching: bool,
}

/// The coordinator over one tenant's [`AgentStore`].
pub struct Coordinator {
    pub(crate) store: Arc<AgentStore>,
    pub(crate) config: RuntimeConfig,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) rng: Arc<dyn Rng>,
    pub(crate) agents: Mutex<HashMap<AgentId, Loaded>>,
    pub(crate) leases: LeaseTable,
    /// Agents the sweep has already reported as unreadable by this release,
    /// so a rollback is logged once per agent, not once per pass.
    pub(crate) unsupported_reported: Mutex<HashSet<AgentId>>,
    pub(crate) wake: Notify,
    pub(crate) metrics: Metrics,
    /// The effect attempts this coordinator dispatched and has seen no
    /// result for, by agent and effect id (see `effects`). An entry is
    /// dropped in three ways: a root of a later epoch is seen, the agent
    /// unloads, or a root shows the entry's input settled
    /// (`prune_open_attempts`).
    pub(crate) open_attempts: Mutex<HashMap<(AgentId, Id128), effects::OpenAttempt>>,
    pub(crate) sweep: SweepConfig,
    /// Where the next sweep pass resumes. Held for the whole pass, so two
    /// passes never run at once.
    pub(crate) sweep_cursor: tokio::sync::Mutex<sweep::SweepCursor>,
    /// When a poll last started a sweep pass, in the injected clock's
    /// nanoseconds; `i64::MIN` before the first.
    pub(crate) last_sweep_unix_ns: AtomicI64,
}

impl Coordinator {
    pub fn new(
        store: Arc<AgentStore>,
        config: RuntimeConfig,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn Rng>,
    ) -> Coordinator {
        Coordinator {
            store,
            config,
            clock,
            rng,
            agents: Mutex::new(HashMap::new()),
            leases: LeaseTable::default(),
            unsupported_reported: Mutex::new(HashSet::new()),
            wake: Notify::new(),
            metrics: Metrics::default(),
            open_attempts: Mutex::new(HashMap::new()),
            sweep: SweepConfig::default(),
            sweep_cursor: tokio::sync::Mutex::new(sweep::SweepCursor::default()),
            last_sweep_unix_ns: AtomicI64::new(i64::MIN),
        }
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub(crate) fn now(&self) -> i64 {
        self.clock.now_unix_ns()
    }

    /// Mint a worker id after checking that `handlers` parse. Records
    /// nothing: a poll is served on the pins it carries, so a worker keeps
    /// working against a restarted or a different coordinator that never
    /// saw this call (`tests::a_poll_needs_no_registration_on_this_coordinator`).
    /// The id is the one the worker names in every later poll, and the owner
    /// id the agents it takes over record.
    pub fn register_worker(&self, handlers: &[HandlerPinBody]) -> Result<String, AgentError> {
        parse_pins(handlers)?;
        Ok(format!("w-{:016x}", self.rng.next_u64()))
    }

    /// Create the agent, or report the existing one.
    pub async fn create(
        &self,
        req: &CreateAgentRequest,
    ) -> Result<CreateAgentResponse, AgentError> {
        let (kind, key) = parse_agent(&req.agent)?;
        let out = self.store.create(kind, key).await?;
        let (incarnation, created) = match out.reply {
            Reply::Created { incarnation } => (incarnation, true),
            Reply::Exists { incarnation } => (incarnation, false),
            _ => (out.head.incarnation, false),
        };
        self.observe(&out.agent, &out.head);
        Ok(CreateAgentResponse {
            agent_id: out.agent.to_hex(),
            incarnation,
            created,
        })
    }

    /// Accept one input. Loads the agent from HEAD when it is not loaded.
    pub async fn send(&self, req: &SendRequest) -> Result<SendResponse, AgentError> {
        let (kind, key) = parse_agent(&req.agent)?;
        let agent = agent_id(&kind, &key);
        let payload = Bytes::from(
            serde_json::to_vec(&req.payload)
                .map_err(|_| refused(&agent, ErrorKind::BudgetExceeded, "payload is not JSON"))?,
        );
        let mut incarnation = self.incarnation(&agent).await?;
        let mut reloaded = false;
        let out = loop {
            match self
                .store
                .enqueue(
                    &agent,
                    incarnation,
                    &req.request_id,
                    &req.handler,
                    payload.clone(),
                )
                .await
            {
                Ok(out) => break out,
                Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                    break self
                        .settle_enqueue(&agent, incarnation, req, &payload, e)
                        .await?;
                }
                // A cached incarnation can be older than HEAD's.
                Err(e) if e.kind == ErrorKind::AgentGone && !reloaded => {
                    reloaded = true;
                    incarnation = self.reload(&agent).await?.incarnation;
                }
                Err(e) => return Err(e),
            }
        };
        self.observe(&agent, &out.head);
        let Reply::Accepted {
            accept_seq,
            duplicate,
        } = out.reply
        else {
            return Err(refused(
                &agent,
                ErrorKind::OutcomeUnknown,
                "enqueue answered without an accept_seq",
            ));
        };
        if out.effects.contains(&Effect::WakeWorker) || !duplicate {
            self.wake.notify_waiters();
        }
        Ok(SendResponse {
            operation: Handle::operation(agent, out.head.incarnation, accept_seq).encode(),
            accept_seq,
            duplicate,
        })
    }

    /// Long-poll for a turn. See [`dispatch`] for the order and the fences.
    pub async fn poll(
        &self,
        req: &WorkerPollRequest,
        deadline: tokio::time::Instant,
    ) -> Result<Option<ravel_agents_core::wire::TurnAssignment>, AgentError> {
        self.poll_turn(req, deadline).await
    }

    /// Publish a worker's outcome under the fence its lease carries.
    ///
    /// The fence's owner, epoch, turn id, base state version, handler pin and
    /// observed root revision all come from `req.lease`, never from the
    /// coordinator's lease table or a fresh HEAD read, so a worker whose
    /// ownership moved on is refused by the kernel's own check
    /// (`tests::worker_paused_past_lease_cannot_commit_after_takeover_epoch_advanced`).
    pub async fn commit(
        &self,
        req: &WorkerCommitRequest,
    ) -> Result<WorkerCommitResponse, AgentError> {
        let (agent, fence) = fence_from_lease(&req.lease)?;
        match &req.outcome {
            TurnOutcome::Completed {
                state,
                output,
                events,
                timers,
                intents,
            } => {
                let mut deps = Vec::new();
                let new_state = put_dep(&mut deps, &agent, state)?;
                let output = opt_dep(&mut deps, &agent, output)?;
                let mut event_specs = Vec::with_capacity(events.len());
                for event in events {
                    event_specs.push(EventSpec {
                        event_type: event.event_type.clone(),
                        payload: opt_dep(&mut deps, &agent, &event.payload)?,
                    });
                }
                let mut timer_changes = Vec::with_capacity(timers.len());
                for timer in timers {
                    timer_changes.push(match timer {
                        TimerChangeBody::Schedule {
                            timer_key,
                            due_unix_ns,
                            handler,
                            payload,
                        } => TimerChange::Schedule(TimerSpec {
                            timer_key: timer_key.clone(),
                            due_unix_ns: *due_unix_ns,
                            handler: handler.clone(),
                            payload: opt_dep(&mut deps, &agent, payload)?,
                        }),
                        TimerChangeBody::Cancel { timer_key } => TimerChange::Cancel {
                            timer_key: timer_key.clone(),
                        },
                    });
                }
                let mut intent_specs = Vec::with_capacity(intents.len());
                for intent in intents {
                    let bytes = json_bytes(&agent, &intent.input)?;
                    let input_hash =
                        step_input_hash(&intent.tool_name, &intent.tool_version, &bytes);
                    let input = ObjectRef::of(&bytes);
                    deps.push(bytes);
                    intent_specs.push(IntentSpec {
                        intent_key: intent.intent_key.clone(),
                        tool_name: intent.tool_name.clone(),
                        tool_version: intent.tool_version.clone(),
                        input_hash,
                        policy: intent.policy.into(),
                        input: Some(input),
                    });
                }
                let command = ravel_agents_core::command::Command::CompleteTurn(CompleteTurn {
                    fence: fence.clone(),
                    observed_root_revision: req.lease.observed_root_revision,
                    new_state,
                    output,
                    events: event_specs,
                    timer_changes,
                    intents: intent_specs,
                });
                let settled = self.publish_turn_command(&agent, &command, &deps).await?;
                self.complete_response(&agent, &fence, settled)
            }
            TurnOutcome::Failed { error, terminal } => {
                let verdict = self.fail(&req.lease, error, *terminal).await?;
                let (accept_seq, status) = match verdict {
                    FailOutcome::Retry { accept_seq, .. } => {
                        (accept_seq, OperationStatus::Accepted)
                    }
                    FailOutcome::Failed { accept_seq, .. } => (accept_seq, OperationStatus::Failed),
                    FailOutcome::Cancelled { accept_seq } => {
                        (accept_seq, OperationStatus::Cancelled)
                    }
                    FailOutcome::AlreadyCommitted { accept_seq } => {
                        (accept_seq, OperationStatus::Completed)
                    }
                };
                let state_version = self
                    .cached(&agent)
                    .map(|h| h.state_version)
                    .unwrap_or(fence.base_state_version);
                Ok(WorkerCommitResponse {
                    operation: Handle::operation(agent, fence.owner.incarnation, accept_seq)
                        .encode(),
                    status,
                    state_version,
                    rebased: false,
                    effects: Vec::new(),
                })
            }
        }
    }

    fn complete_response(
        &self,
        agent: &AgentId,
        fence: &TurnFence,
        settled: failure::Settled,
    ) -> Result<WorkerCommitResponse, AgentError> {
        let out = settled.outcome;
        let (accept_seq, state_version, rebased) = match out.reply {
            Reply::TurnCommitted {
                accept_seq,
                state_version,
                rebased,
            } => (accept_seq, state_version, rebased),
            // A retry of a commit that already published, or a lost
            // acknowledgement settled through `resolve`: the root's version
            // is the committed one or a later one.
            Reply::AlreadyCommitted { accept_seq } => (accept_seq, out.head.state_version, false),
            _ => {
                return Err(refused(
                    agent,
                    ErrorKind::OwnershipLost,
                    "completion answered with something other than a commit",
                ));
            }
        };
        if out.published || settled.confirmed_lost_ack {
            self.metrics.turns_committed.fetch_add(1, Ordering::Relaxed);
        }
        let status = out
            .head
            .results
            .iter()
            .find(|r| r.accept_seq == accept_seq)
            .map_or(OperationStatus::Completed, |r| r.status.into());
        let effects = out
            .effects
            .iter()
            .filter_map(|e| match e {
                Effect::DispatchIntents { effect_ids } => Some(effect_ids.iter()),
                _ => None,
            })
            .flatten()
            .map(Id128::to_hex)
            .collect();
        Ok(WorkerCommitResponse {
            operation: Handle::operation(*agent, fence.owner.incarnation, accept_seq).encode(),
            status,
            state_version,
            rebased,
            effects,
        })
    }

    /// Request cancellation of the operation a handle names.
    pub async fn cancel(&self, req: &CancelRequest) -> Result<CancelResponse, AgentError> {
        let handle = Handle::decode_kind(&req.operation, "op").map_err(|_| {
            AgentError::new(
                ErrorKind::AgentGone,
                SafeIds::default(),
                "operation handle is malformed",
            )
        })?;
        let ravel_agents_core::state::HandleItem::Operation(accept_seq) = handle.item else {
            return Err(refused(
                &handle.agent,
                ErrorKind::AgentGone,
                "handle does not name an operation",
            ));
        };
        let command = ravel_agents_core::command::Command::Cancel(Cancel {
            fence: IncarnationFence {
                incarnation: handle.incarnation,
            },
            accept_seq,
        });
        let out = match self.store.execute(&handle.agent, &command).await {
            Ok(out) => out,
            Err(e) if e.kind == ErrorKind::OutcomeUnknown => {
                self.settle_unknown(&handle.agent, &command, &[], e)
                    .await?
                    .outcome
            }
            Err(e) => return Err(e),
        };
        self.observe(&handle.agent, &out.head);
        let (status, requested) = match out.reply {
            Reply::CancelRequested { .. } => (OperationStatus::Running, true),
            Reply::Cancelled { .. } => (OperationStatus::Cancelled, true),
            Reply::AlreadyTerminal { status, .. } => (status.into(), false),
            _ => (OperationStatus::Accepted, false),
        };
        if out.effects.contains(&Effect::WakeWorker) {
            self.wake.notify_waiters();
        }
        Ok(CancelResponse {
            operation: req.operation.clone(),
            status,
            requested,
        })
    }

    pub fn metrics(&self) -> AgentsMetricsSnapshot {
        self.metrics_snapshot()
    }

    /// The incarnation of a loaded agent, loading it from HEAD if needed.
    async fn incarnation(&self, agent: &AgentId) -> Result<u64, AgentError> {
        if let Some(head) = self.cached(agent) {
            return Ok(head.incarnation);
        }
        Ok(self.reload(agent).await?.incarnation)
    }

    /// The loaded root of `agent`, if any.
    pub(crate) fn cached(&self, agent: &AgentId) -> Option<Head> {
        self.agents.lock().get(agent).map(|l| l.head.clone())
    }

    /// Read HEAD and replace the loaded root with it. The root carries the
    /// inbox pages dispatch reads ([`dispatch::dispatch_needs`]) and no
    /// other sealed page: one GET for HEAD plus at most two page GETs. A
    /// caller that reads another list attaches its page itself.
    pub(crate) async fn reload(&self, agent: &AgentId) -> Result<Head, AgentError> {
        match self.store.read_head_shallow(agent).await? {
            HeadState::Present { head, .. } => {
                let mut head = *head;
                self.store
                    .attach(agent, &mut head, &dispatch::dispatch_needs(&head))
                    .await?;
                self.replace(agent, &head);
                Ok(head)
            }
            HeadState::Absent => Err(refused(agent, ErrorKind::AgentGone, "agent does not exist")),
            HeadState::Unsupported { .. } => Err(refused(
                agent,
                ErrorKind::UnsupportedFormat,
                "HEAD format version is newer than this release",
            )),
        }
    }

    /// Record a root an operation returned, unless a newer one is loaded.
    pub(crate) fn observe(&self, agent: &AgentId, head: &Head) {
        self.prune_open_attempts(agent, head);
        let now = self.now();
        let mut agents = self.agents.lock();
        match agents.get_mut(agent) {
            Some(loaded) => {
                if head.incarnation != loaded.head.incarnation
                    || head.root_revision >= loaded.head.root_revision
                {
                    loaded.head = head.clone();
                }
                loaded.touched_unix_ns = now;
            }
            None => {
                agents.insert(
                    *agent,
                    Loaded {
                        head: head.clone(),
                        touched_unix_ns: now,
                        dispatching: false,
                    },
                );
            }
        }
    }

    /// Record a root read fresh from HEAD, whatever is loaded.
    pub(crate) fn replace(&self, agent: &AgentId, head: &Head) {
        self.prune_open_attempts(agent, head);
        let now = self.now();
        let mut agents = self.agents.lock();
        let loaded = agents.entry(*agent).or_insert_with(|| Loaded {
            head: head.clone(),
            touched_unix_ns: now,
            dispatching: false,
        });
        loaded.head = head.clone();
        loaded.touched_unix_ns = now;
    }

    pub(crate) fn after_outcome(&self, out: &Outcome) {
        self.observe(&out.agent, &out.head);
        if out.effects.contains(&Effect::WakeWorker) {
            self.wake.notify_waiters();
        }
    }
}

pub(crate) fn ids_for(agent: &AgentId) -> SafeIds {
    SafeIds {
        agent_id: Some(agent.to_hex()),
        ..SafeIds::default()
    }
}

pub(crate) fn refused(agent: &AgentId, kind: ErrorKind, reason: &'static str) -> AgentError {
    AgentError::new(kind, ids_for(agent), reason)
}

fn parse_agent(agent: &AgentRef) -> Result<(AgentKind, AgentKey), AgentError> {
    let invalid = || {
        AgentError::new(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "agent kind or key is not valid",
        )
    };
    let kind = AgentKind::new(agent.kind.as_str()).map_err(|_| invalid())?;
    let key = AgentKey::new(agent.key.as_str()).map_err(|_| invalid())?;
    Ok((kind, key))
}

pub(crate) fn parse_pins(handlers: &[HandlerPinBody]) -> Result<Vec<HandlerPin>, AgentError> {
    handlers
        .iter()
        .map(|h| {
            let digest = decode_hash(&h.digest).ok_or_else(|| {
                AgentError::new(
                    ErrorKind::IncompatibleHandler,
                    SafeIds::default(),
                    "handler digest is not 32 bytes of hex",
                )
            })?;
            Ok(HandlerPin {
                handler: h.handler.clone(),
                version: h.version.clone(),
                digest,
            })
        })
        .collect()
}

fn decode_hash(text: &str) -> Option<Hash256> {
    Hash256::from_slice(&hex::decode(text).ok()?)
}

fn decode_id(text: &str) -> Option<Id128> {
    Id128::from_slice(&hex::decode(text).ok()?)
}

/// The turn fence a lease names, taken from the lease alone.
pub(crate) fn fence_from_lease(lease: &TurnLease) -> Result<(AgentId, TurnFence), AgentError> {
    let agent = AgentId::from_hex(&lease.agent_id).ok_or_else(|| {
        AgentError::new(
            ErrorKind::AgentGone,
            SafeIds::default(),
            "lease agent id is malformed",
        )
    })?;
    let turn_id = decode_id(&lease.turn_id).ok_or_else(|| {
        refused(
            &agent,
            ErrorKind::OwnershipLost,
            "lease turn id is malformed",
        )
    })?;
    let handler_digest = decode_hash(&lease.handler_digest).ok_or_else(|| {
        refused(
            &agent,
            ErrorKind::IncompatibleHandler,
            "lease handler digest is malformed",
        )
    })?;
    Ok((
        agent,
        TurnFence {
            owner: OwnerFence {
                incarnation: lease.incarnation,
                owner_id: lease.owner_id.clone(),
                epoch: lease.epoch,
            },
            turn_id,
            base_state_version: lease.base_state_version,
            handler_version: lease.handler_version.clone(),
            handler_digest,
        },
    ))
}

pub(crate) fn json_bytes(agent: &AgentId, value: &Value) -> Result<Bytes, AgentError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|_| refused(agent, ErrorKind::BudgetExceeded, "value is not JSON"))
}

fn put_dep(deps: &mut Vec<Bytes>, agent: &AgentId, value: &Value) -> Result<ObjectRef, AgentError> {
    let bytes = json_bytes(agent, value)?;
    let dep = ObjectRef::of(&bytes);
    deps.push(bytes);
    Ok(dep)
}

/// A JSON `null` is no object.
fn opt_dep(
    deps: &mut Vec<Bytes>,
    agent: &AgentId,
    value: &Value,
) -> Result<Option<ObjectRef>, AgentError> {
    if value.is_null() {
        return Ok(None);
    }
    put_dep(deps, agent, value).map(Some)
}

#[cfg(test)]
mod tests;
