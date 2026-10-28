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
use crate::head::{EffectStatus, RecoveryPolicy, ResultStatus, StepStatus};
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
}

/// `POST /api/v1/agents/events`: one bounded page after a durable cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsRequest {
    pub agent: AgentRef,
    /// Opaque cursor from a previous page; absent reads from the start.
    pub cursor: Option<String>,
    pub limit: u32,
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
    pub next_cursor: String,
}

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

/// `DELETE /api/v1/agents/timers`
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
    let mut hasher = blake3::Hasher::new();
    hasher.update(crate::identity::DOMAIN);
    let parts: [&[u8]; 4] = [
        b"random",
        agent_id_hex.as_bytes(),
        &incarnation.to_le_bytes(),
        &accept_seq.to_le_bytes(),
    ];
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hex::encode(&hasher.finalize().as_bytes()[..16])
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

/// `POST /api/v1/agents/effects/complete`
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
}

impl From<&AgentError> for ErrorBody {
    fn from(error: &AgentError) -> Self {
        ErrorBody {
            code: error.kind.code().to_string(),
            retry: error.kind.retry().code().to_string(),
            reason: error.reason.to_string(),
            ids: SafeIdsBody::from(&*error.ids),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod fixtures;
