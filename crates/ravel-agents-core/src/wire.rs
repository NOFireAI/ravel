//! JSON bodies of the `/api/v1/agents/...` routes (ADR-3331 decision 9).
//!
//! These types are the wire contract the TypeScript SDK decodes. Every one
//! has a golden fixture under `sdk/fixtures/<name>.json`, written by the
//! `wire::fixtures` test; a change to any body shows up as a fixture diff. Field names are snake_case, handles are the opaque
//! tokens of [`crate::state::Handle`], ids and hashes are lowercase hex, and
//! application payloads are arbitrary JSON values.
//!
//! Every `*_unix_ns` field is a JSON string holding the canonical decimal
//! form of an `i64` (no sign on zero or positives, no leading zeros), and a
//! JSON number there is refused. Nanoseconds since the epoch exceed 2^53, the
//! largest integer a JavaScript number holds exactly, so a number would be
//! rounded by the SDK's `JSON.parse`
//! (`wire::fixtures::unix_ns_fields_are_decimal_strings`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Serde codec for `*_unix_ns` fields: an `i64` as a canonical decimal
/// string.
mod unix_ns {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let text = String::deserialize(deserializer)?;
        match text.parse::<i64>() {
            Ok(value) if value.to_string() == text => Ok(value),
            _ => Err(D::Error::custom(
                "a *_unix_ns field must be a canonical decimal i64 string",
            )),
        }
    }
}

use crate::error::{AgentError, SafeIds};
use crate::head::{EffectStatus, Head, RecoveryPolicy, ResultStatus, StepStatus, TimerStatus};
use crate::kernel::{Limits, Usage};
use crate::state::OperationState;

/// The `(kind, key)` pair a client names an agent by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRef {
    pub kind: String,
    pub key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Accepted,
    Running,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl From<OperationState> for OperationStatus {
    fn from(state: OperationState) -> Self {
        match state {
            OperationState::Accepted => OperationStatus::Accepted,
            OperationState::Running => OperationStatus::Running,
            OperationState::Blocked => OperationStatus::Blocked,
            OperationState::Completed => OperationStatus::Completed,
            OperationState::Failed => OperationStatus::Failed,
            OperationState::Cancelled => OperationStatus::Cancelled,
        }
    }
}

impl From<ResultStatus> for OperationStatus {
    fn from(status: ResultStatus) -> Self {
        match status {
            ResultStatus::Completed => OperationStatus::Completed,
            ResultStatus::Failed => OperationStatus::Failed,
            ResultStatus::Cancelled => OperationStatus::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    ProviderIdempotency,
    AtLeastOnce,
    Manual,
}

impl From<RecoveryPolicy> for Policy {
    fn from(policy: RecoveryPolicy) -> Self {
        match policy {
            RecoveryPolicy::ProviderIdempotency => Policy::ProviderIdempotency,
            RecoveryPolicy::AtLeastOnce => Policy::AtLeastOnce,
            RecoveryPolicy::Manual => Policy::Manual,
        }
    }
}

impl From<Policy> for RecoveryPolicy {
    fn from(policy: Policy) -> Self {
        match policy {
            Policy::ProviderIdempotency => RecoveryPolicy::ProviderIdempotency,
            Policy::AtLeastOnce => RecoveryPolicy::AtLeastOnce,
            Policy::Manual => RecoveryPolicy::Manual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Intent,
    Authorized,
    InDoubt,
    Completed,
}

impl From<EffectStatus> for EffectState {
    fn from(status: EffectStatus) -> Self {
        match status {
            EffectStatus::Intent => EffectState::Intent,
            EffectStatus::Authorized => EffectState::Authorized,
            EffectStatus::InDoubt => EffectState::InDoubt,
            EffectStatus::Completed => EffectState::Completed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    Started,
    Completed,
}

impl From<StepStatus> for StepState {
    fn from(status: StepStatus) -> Self {
        match status {
            StepStatus::Started => StepState::Started,
            StepStatus::Completed => StepState::Completed,
        }
    }
}

/// `POST /api/v1/agents/create`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateAgentRequest {
    pub agent: AgentRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateAgentResponse {
    pub agent_id: String,
    pub incarnation: u64,
    /// False when the agent already existed in this incarnation.
    pub created: bool,
}

/// `POST /api/v1/agents/destroy` (ADR-3331, destroy amendment): tombstone
/// the agent's current incarnation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DestroyAgentRequest {
    pub agent: AgentRef,
    /// The incarnation the caller means to destroy. When set and not the
    /// current one the destroy is `agent_gone` and writes nothing, so a
    /// destroy typed against incarnation `n` never tombstones `n + 1`
    /// (`Coordinator::destroy` in ravel-agents-runtime). Omitted, the
    /// current incarnation is destroyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<u64>,
}

/// What a tombstone left in HEAD, read from the HEAD it was applied to. The
/// tombstone changes only the lifecycle and the root revision
/// (`kernel::tests::a_tombstone_changes_only_the_lifecycle_and_the_root_revision`),
/// so these counts are the same before and after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AbandonedCounts {
    /// Accepted inputs never consumed, sealed inbox pages included; the
    /// active turn's input is one of them while a turn runs.
    pub inputs: u64,
    /// Whether a turn was open.
    pub active_turn: bool,
    /// Effects whose last attempt is in doubt.
    pub effects_in_doubt: u64,
    /// Effects with an attempt authorized and no result.
    pub effects_authorized: u64,
    /// Timers still scheduled.
    pub timers_scheduled: u64,
}

impl AbandonedCounts {
    /// The counts of `head`. `queued_inputs` is the inbox depth with sealed
    /// pages included, which HEAD's open page alone does not hold; the
    /// caller reads it the way the `state` route reads the mailbox, or from
    /// [`Head::inbox_depth`]. Effects are never paged, and scheduled timers
    /// are counted by [`Head::scheduled_timers`], which adds the index's
    /// count for unloaded pages, so the other counts are complete from a
    /// shallow HEAD.
    pub fn from_head(head: &Head, queued_inputs: u64) -> AbandonedCounts {
        let effects = |status: EffectStatus| -> u64 {
            head.effects.iter().filter(|e| e.status == status).count() as u64
        };
        AbandonedCounts {
            inputs: queued_inputs,
            active_turn: head.active_turn.is_some(),
            effects_in_doubt: effects(EffectStatus::InDoubt),
            effects_authorized: effects(EffectStatus::Authorized),
            timers_scheduled: head.scheduled_timers(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DestroyAgentResponse {
    pub agent_id: String,
    /// The incarnation that is now tombstoned.
    pub incarnation: u64,
    /// False when HEAD was already tombstoned at this incarnation; nothing
    /// was written and `abandoned` is read from that HEAD.
    pub destroyed: bool,
    pub abandoned: AbandonedCounts,
}

/// `POST /api/v1/agents/send`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendRequest {
    pub agent: AgentRef,
    pub handler: String,
    /// The idempotency key; a retry reuses it.
    pub request_id: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendResponse {
    pub operation: String,
    pub accept_seq: u64,
    pub duplicate: bool,
}

/// `POST /api/v1/agents/call`: send, then wait up to `wait_ms` for the result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallRequest {
    pub agent: AgentRef,
    pub handler: String,
    pub request_id: String,
    pub payload: Value,
    pub wait_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallResponse {
    pub operation: String,
    pub status: OperationStatus,
    /// Present once `status` is terminal.
    pub result: Option<ResultBody>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultBody {
    pub status: OperationStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub dead_lettered: bool,
    #[serde(with = "unix_ns")]
    pub finished_unix_ns: i64,
}

/// `GET /api/v1/agents/operation?operation=<handle>`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationResponse {
    pub operation: String,
    pub status: OperationStatus,
    pub accept_seq: u64,
    /// Set while the operation is blocked on an effect awaiting
    /// reconciliation.
    pub blocked_effect: Option<String>,
}

/// `GET /api/v1/agents/result?operation=<handle>&wait_ms=<n>`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultResponse {
    pub operation: String,
    /// Absent when the wait elapsed before the operation finished.
    pub result: Option<ResultBody>,
}

/// `POST /api/v1/agents/resolve`: find the operation a request id created,
/// the answer to an `outcome_unknown` error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolveRequest {
    pub agent: AgentRef,
    pub handler: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolveResponse {
    /// Absent when the request was never accepted; resubmitting it under
    /// the same request id is then safe.
    pub operation: Option<String>,
    pub status: Option<OperationStatus>,
}

/// `POST /api/v1/agents/cancel`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelResponse {
    pub operation: String,
    pub status: OperationStatus,
    /// True when the operation was running and the cancellation is
    /// observed at its next commit.
    pub requested: bool,
}

/// `GET /api/v1/agents/state?kind=<k>&key=<k>`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateResponse {
    pub agent_id: String,
    pub incarnation: u64,
    pub state_version: u64,
    pub content_type: String,
    pub state: Value,
    /// The turn HEAD holds open, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<ActiveTurnBody>,
    /// Accepted inputs not yet consumed, oldest first; the head of the list
    /// is the active turn's input while one runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mailbox: Vec<MailboxEntryBody>,
    /// How far the agent has grown toward its lifetime caps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageBody>,
}

/// The counts behind each lifetime cap of one agent, from its HEAD as stored.
/// A send is refused `budget_exceeded` once `receipts` reaches
/// `max_receipts`; a list's publish once its sealed pages reach
/// `max_index_pages`; any publish that would encode HEAD past
/// `max_head_bytes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageBody {
    pub receipts: u64,
    pub max_receipts: u64,
    pub results_pages: u64,
    pub events_pages: u64,
    pub timers_pages: u64,
    pub max_index_pages: u64,
    pub head_bytes: u64,
    pub max_head_bytes: u64,
}

impl UsageBody {
    pub fn new(usage: Usage, limits: &Limits) -> UsageBody {
        let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        UsageBody {
            receipts: usage.receipts,
            max_receipts: limits.max_receipts,
            results_pages: wide(usage.results_pages),
            events_pages: wide(usage.events_pages),
            timers_pages: wide(usage.timers_pages),
            max_index_pages: wide(limits.max_index_pages),
            head_bytes: wide(usage.head_bytes),
            max_head_bytes: wide(limits.max_head_bytes),
        }
    }
}

/// The open turn of a [`StateResponse`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveTurnBody {
    pub turn_id: String,
    /// The accepted input the turn consumes.
    pub accept_seq: u64,
    pub handler: String,
    /// The handler version the turn is bound to: only a worker serving this
    /// version and digest can resume it.
    pub handler_version: String,
    pub handler_digest: String,
    pub base_state_version: u64,
    #[serde(with = "unix_ns")]
    pub started_unix_ns: i64,
    /// The owner's lease as last recorded in HEAD; a heartbeat may have
    /// extended it since.
    #[serde(with = "unix_ns")]
    pub lease_hint_unix_ns: i64,
    /// A cancel was requested and the turn has not yet observed it.
    pub cancel_requested: bool,
    /// A replay diverged from the journaled steps; the turn waits for an
    /// operator.
    pub non_deterministic_replay: bool,
}

/// Where an accepted input came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputSourceBody {
    Message {
        request_id: String,
    },
    /// A fired timer: the id and the generation that fired.
    Timer {
        timer: String,
        generation: u64,
    },
}

/// One accepted input of a [`StateResponse`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MailboxEntryBody {
    pub accept_seq: u64,
    pub handler: String,
    pub source: InputSourceBody,
    #[serde(with = "unix_ns")]
    pub accepted_unix_ns: i64,
    pub failed_attempts: u32,
}

/// `POST /api/v1/agents/events`: one bounded page after a durable cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsRequest {
    pub agent: AgentRef,
    /// Opaque cursor from a previous page; absent reads from the start.
    pub cursor: Option<String>,
    pub limit: u32,
    /// An operation handle of `agent`: the page holds only the events its
    /// turns committed, and the page that drains them once the operation is
    /// finished carries [`EventsResponse::terminal`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventBody {
    pub event_seq: u64,
    pub accept_seq: u64,
    pub turn_id: String,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsResponse {
    pub events: Vec<EventBody>,
    /// The cursor to send for the next page. A page with no events echoes
    /// the cursor it was asked for.
    pub next_cursor: String,
    /// Set only on a page for [`EventsRequest::operation`] that holds the
    /// operation's last event and finds it finished: its final status. No
    /// later page holds an event of that operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<OperationStatus>,
}

/// What a caller holding an expired events cursor does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorRestart {
    /// Read again from [`CursorExpiredBody::oldest_cursor`]; the events
    /// between the expired cursor and it are no longer retained.
    RestartFromOldestCursor,
}

/// The `410 Gone` body of `events` for a cursor below the retained window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CursorExpiredBody {
    /// Always `cursor_expired`.
    pub code: String,
    /// The cursor the request sent.
    pub cursor: String,
    /// The oldest cursor this agent's events are still served from.
    pub oldest_cursor: String,
    pub restart: CursorRestart,
    pub reason: String,
    pub ids: SafeIdsBody,
}

/// The `code` of every [`CursorExpiredBody`].
pub const CURSOR_EXPIRED_CODE: &str = "cursor_expired";

/// `POST /api/v1/agents/timers`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerRequest {
    pub agent: AgentRef,
    pub timer_key: String,
    #[serde(with = "unix_ns")]
    pub due_unix_ns: i64,
    pub handler: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerResponse {
    pub timer: String,
    pub generation: u64,
}

/// `POST /api/v1/agents/timers/cancel`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelTimerRequest {
    pub agent: AgentRef,
    pub timer_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerCancelOutcome {
    Cancelled,
    /// The firing committed first; its input is in the mailbox.
    AlreadyFired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelTimerResponse {
    pub outcome: TimerCancelOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerState {
    /// Armed: the sweep fires it once it is due.
    Scheduled,
    /// Fired: its message was enqueued by the firing CAS.
    Delivered,
    Cancelled,
}

impl From<TimerStatus> for TimerState {
    fn from(status: TimerStatus) -> Self {
        match status {
            TimerStatus::Scheduled => TimerState::Scheduled,
            TimerStatus::Delivered => TimerState::Delivered,
            TimerStatus::Cancelled => TimerState::Cancelled,
        }
    }
}

/// One timer as HEAD holds it: the latest generation of its key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerBody {
    pub timer: String,
    pub timer_key: String,
    pub generation: u64,
    #[serde(with = "unix_ns")]
    pub due_unix_ns: i64,
    pub handler: String,
    pub status: TimerState,
}

/// `GET /api/v1/agents/timers?kind=<k>&key=<k>`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimersResponse {
    pub timers: Vec<TimerBody>,
}

/// `POST /api/v1/agents/sweep`: what one sweep pass over the caller's
/// tenant did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SweepResponse {
    /// `_wake/` LIST pages.
    pub wake_list_pages: u64,
    /// `_dir/` LIST pages.
    pub dir_list_pages: u64,
    pub head_gets: u64,
    pub hints_read: u64,
    pub hints_deleted: u64,
    pub timers_fired: u64,
    pub agents_loaded: u64,
    pub dir_entries_repaired: u64,
}

/// `GET /api/v1/agents/effects?kind=<k>&key=<k>`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectBody {
    pub effect: String,
    pub turn_id: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: Policy,
    pub status: EffectState,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectsResponse {
    pub effects: Vec<EffectBody>,
}

/// What one attempt of an effect came to, as `GET /api/v1/agents/effects/{effect}`
/// reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    /// The current authorized attempt, with no result yet.
    Open,
    /// The current attempt of an effect latched in doubt.
    InDoubt,
    /// The attempt whose result is the effect's result.
    Result,
    /// An earlier attempt a later one replaced.
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectAttemptBody {
    pub attempt: u32,
    pub status: AttemptState,
}

/// A result reported for an attempt after another attempt's result was
/// recorded: kept, never the effect's result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectEvidenceBody {
    pub attempt: u32,
    /// The reported output; null when the report was a failure.
    pub result: Value,
    /// The reported tool failure, when the report was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// `GET /api/v1/agents/effects/{effect}`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectInspectResponse {
    #[serde(flatten)]
    pub effect: EffectBody,
    pub step_key: String,
    /// The attempt whose result is recorded, once one is.
    pub result_attempt: Option<u32>,
    /// The recorded output; null when the recorded result is a failure.
    pub result: Option<Value>,
    /// The recorded tool failure, when the result is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub attempts: Vec<EffectAttemptBody>,
    pub evidence: Vec<EffectEvidenceBody>,
}

/// `POST /api/v1/agents/reconcile`, `agents_admin` only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ReconcileDecision {
    /// Authorize one more attempt.
    Retry,
    /// Record the result an operator confirmed out of band.
    RecordResult { result: Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcileRequest {
    pub effect: String,
    pub attempt: u32,
    #[serde(flatten)]
    pub decision: ReconcileDecision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcileResponse {
    pub effect: String,
    pub status: EffectState,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HandlerPinBody {
    pub handler: String,
    pub version: String,
    pub digest: String,
}

/// `POST /api/v1/agents/worker/poll`, a long poll capped at 20 s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerPollRequest {
    pub worker_id: String,
    pub handlers: Vec<HandlerPinBody>,
    pub wait_ms: u32,
}

/// Everything a commit must echo back so the kernel can fence it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnLease {
    pub agent_id: String,
    pub incarnation: u64,
    pub owner_id: String,
    pub epoch: u64,
    pub turn_id: String,
    pub base_state_version: u64,
    pub handler_version: String,
    pub handler_digest: String,
    pub observed_root_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepBody {
    pub step_id: String,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub input_hash: String,
    pub status: StepState,
    pub output: Option<Value>,
}

/// The lease `worker/poll` hands out: [`TurnLease`] plus two facts a worker
/// needs to replay the turn deterministically. Both are fixed when the turn
/// begins and carried across takeovers, so a replacement worker sees the
/// values the first one did. A commit or heartbeat that echoes the whole
/// object is read as its [`TurnLease`]; the two extra fields are ignored there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolledLease {
    #[serde(flatten)]
    pub lease: TurnLease,
    /// When the coordinator began the turn, recorded in HEAD; the handler's
    /// clock reads this, never the worker's own.
    #[serde(with = "unix_ns")]
    pub recorded_unix_ns: i64,
    /// The turn's 128-bit randomness seed, lowercase hex
    /// ([`turn_random_seed`]).
    pub random_seed: String,
}

/// The seed of a turn's deterministic randomness: the first 16 bytes of the
/// identity hash with purpose `random` over the agent id's hex text, the
/// incarnation and the input's accept_seq (both u64 little-endian), as
/// lowercase hex. Every attempt at one input derives the same seed.
pub fn turn_random_seed(agent_id_hex: &str, incarnation: u64, accept_seq: u64) -> String {
    crate::identity::derive_128(
        "random",
        &[
            agent_id_hex.as_bytes(),
            &incarnation.to_le_bytes(),
            &accept_seq.to_le_bytes(),
        ],
    )
    .to_hex()
}

/// One leased turn. `worker/poll` delivers it with a [`PolledLease`]
/// ([`PolledAssignment`]); the coordinator builds it with a [`TurnLease`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnAssignment<L = TurnLease> {
    pub lease: L,
    pub agent: AgentRef,
    pub operation: String,
    pub accept_seq: u64,
    pub handler: String,
    pub state: Value,
    pub payload: Value,
    /// Steps already journaled for this turn, replayed instead of re-run.
    pub steps: Vec<StepBody>,
}

/// A [`TurnAssignment`] as `worker/poll` delivers it.
pub type PolledAssignment = TurnAssignment<PolledLease>;

impl TurnAssignment {
    /// This assignment with its lease extended by the turn's recorded start
    /// time and its [`turn_random_seed`].
    pub fn polled(self, recorded_unix_ns: i64) -> PolledAssignment {
        let random_seed = turn_random_seed(
            &self.lease.agent_id,
            self.lease.incarnation,
            self.accept_seq,
        );
        TurnAssignment {
            lease: PolledLease {
                lease: self.lease,
                recorded_unix_ns,
                random_seed,
            },
            agent: self.agent,
            operation: self.operation,
            accept_seq: self.accept_seq,
            handler: self.handler,
            state: self.state,
            payload: self.payload,
            steps: self.steps,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerPollResponse {
    /// Absent when the wait elapsed with no work.
    pub assignment: Option<PolledAssignment>,
}

/// `POST /api/v1/agents/worker/register`: the handler pins a worker process
/// serves. Answered with the worker id the process names in every poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub handlers: Vec<HandlerPinBody>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerRegisterResponse {
    /// Minted for this call; never reused by another registration.
    pub worker_id: String,
}

/// `POST /api/v1/agents/worker/heartbeat`: extend a running turn's lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerHeartbeatRequest {
    pub lease: TurnLease,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerHeartbeatResponse {
    /// When the extended lease expires.
    #[serde(with = "unix_ns")]
    pub lease_expires_unix_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventSpecBody {
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum TimerChangeBody {
    Schedule {
        timer_key: String,
        #[serde(with = "unix_ns")]
        due_unix_ns: i64,
        handler: String,
        payload: Value,
    },
    Cancel {
        timer_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntentBody {
    pub intent_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: Policy,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TurnOutcome {
    Completed {
        state: Value,
        output: Value,
        events: Vec<EventSpecBody>,
        timers: Vec<TimerChangeBody>,
        intents: Vec<IntentBody>,
    },
    Failed {
        error: String,
        /// True for an error a retry cannot fix; the input fails without
        /// being dead-lettered.
        terminal: bool,
    },
}

/// `POST /api/v1/agents/worker/commit`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerCommitRequest {
    pub lease: TurnLease,
    #[serde(flatten)]
    pub outcome: TurnOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerCommitResponse {
    pub operation: String,
    pub status: OperationStatus,
    pub state_version: u64,
    /// True when the commit landed on a root that moved after the poll.
    pub rebased: bool,
    /// Effect handles the server dispatches after this commit.
    pub effects: Vec<String>,
}

/// `POST /api/v1/agents/worker/step`: journal a step, or read back the
/// record it already has, and authorize the attempt the caller runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepRecordRequest {
    pub lease: TurnLease,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub policy: Policy,
    /// Lowercase hex of [`crate::identity::step_input_hash`] over
    /// `tool_name`, `tool_version` and the canonical encoding of the step's
    /// input: its JSON form with object keys sorted by UTF-16 code unit at
    /// every depth, no insignificant whitespace, strings escaped and numbers
    /// written as ECMAScript `JSON.stringify` writes them. A replay whose hash
    /// differs from the journaled one is `NonDeterministicReplay`.
    /// `sdk/fixtures/identity/step_input_hash.json` pins one input, its
    /// canonical text and its hash for both implementations.
    pub input_hash: String,
}

/// The step's record. A step with a recorded result comes back as it is:
/// `step.status` `completed` with its output, or `failure` set and `output`
/// null. Otherwise `attempt` is the attempt the caller is authorized to run
/// and `effect` the handle its result is completed under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepRecordResponse {
    pub step: StepBody,
    pub effect: String,
    pub attempt: u32,
    /// The tool failure recorded as the step's result, or null.
    pub failure: Option<String>,
}

/// `POST /api/v1/agents/worker/effects/complete`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectCompleteRequest {
    pub effect: String,
    pub attempt: u32,
    pub result: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectCompleteResponse {
    pub effect: String,
    pub status: EffectState,
    /// True when another attempt's result was recorded first; this one is
    /// kept as evidence only.
    pub late: bool,
}

/// `POST /api/v1/agents/worker/effects/fail`: record a tool's failure as its
/// attempt's result, under the same `late` rule as `effects/complete`. The
/// answer is an [`EffectCompleteResponse`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectFailRequest {
    pub effect: String,
    pub attempt: u32,
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SafeIdsBody {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub incarnation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub effect_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timer_id: Option<String>,
}

impl From<&SafeIds> for SafeIdsBody {
    fn from(ids: &SafeIds) -> Self {
        SafeIdsBody {
            agent_id: ids.agent_id.clone(),
            incarnation: ids.incarnation,
            operation: ids.operation.clone(),
            turn_id: ids.turn_id.clone(),
            step_id: ids.step_id.clone(),
            effect_id: ids.effect_id.clone(),
            timer_id: ids.timer_id.clone(),
        }
    }
}

/// The body of every non-2xx response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// One of the eleven [`crate::error::ErrorKind`] codes.
    pub code: String,
    pub retry: String,
    pub reason: String,
    pub ids: SafeIdsBody,
    /// The lifetime cap a `budget_exceeded` or `mailbox_full` refusal hit:
    /// one of the [`crate::error::BudgetCap`] codes. Absent for every other
    /// error and for a refusal of a per-command bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cap: Option<String>,
}

impl From<&AgentError> for ErrorBody {
    fn from(error: &AgentError) -> Self {
        ErrorBody {
            code: error.kind.code().to_string(),
            retry: error.kind.retry().code().to_string(),
            reason: error.reason.to_string(),
            ids: SafeIdsBody::from(&*error.ids),
            cap: error.cap.map(|cap| cap.code().to_string()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod fixtures;
