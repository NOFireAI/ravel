//! Every durable command and the preconditions the kernel checks for it
//! (ADR-3331 decisions 4 to 8).
//!
//! [`Command::preconditions`] returns those checks as data, in the order the
//! kernel applies them, so the store's publication skeleton, the TLA+
//! traceability rows and the tests read one list instead of re-deriving it
//! from the kernel's control flow.

use ravel_types::TenantHash;

use crate::head::{ObjectRef, RecoveryPolicy, StepStatus};
use crate::identity::{Hash256, Id128};
use crate::keys::{AgentKey, AgentKind};

/// Fence on the agent incarnation a client handle was minted under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncarnationFence {
    pub incarnation: u64,
}

/// Fence an owner-constrained worker command carries (decision 6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerFence {
    pub incarnation: u64,
    pub owner_id: String,
    pub epoch: u64,
}

/// Fence a turn-scoped worker command carries: the owner fence plus the
/// turn binding made by `BeginTurn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnFence {
    pub owner: OwnerFence,
    pub turn_id: Id128,
    pub base_state_version: u64,
    pub handler_version: String,
    pub handler_digest: Hash256,
}

/// A handler a worker can run, as it advertises it on poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerPin {
    pub handler: String,
    pub version: String,
    pub digest: Hash256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub tenant: TenantHash,
    pub kind: AgentKind,
    pub key: AgentKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enqueue {
    pub fence: IncarnationFence,
    pub request_id: String,
    pub handler: String,
    pub request_hash: Hash256,
    pub payload: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Takeover {
    pub incarnation: u64,
    pub owner_id: String,
    /// The epoch the caller read; the CAS advances it by one.
    pub observed_epoch: u64,
    pub lease_hint_unix_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginTurn {
    pub fence: OwnerFence,
    pub handlers: Vec<HandlerPin>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSpec {
    pub event_type: String,
    pub payload: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerSpec {
    pub timer_key: String,
    pub due_unix_ns: i64,
    pub handler: String,
    pub payload: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerChange {
    Schedule(TimerSpec),
    Cancel { timer_key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentSpec {
    pub intent_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub input_hash: Hash256,
    pub policy: RecoveryPolicy,
    pub input: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteTurn {
    pub fence: TurnFence,
    /// The root revision the worker built against. A different revision is
    /// a rebase: allowed while the fence still matches.
    pub observed_root_revision: u64,
    pub new_state: ObjectRef,
    pub output: Option<ObjectRef>,
    pub events: Vec<EventSpec>,
    pub timer_changes: Vec<TimerChange>,
    pub intents: Vec<IntentSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailTurn {
    pub fence: TurnFence,
    pub error: String,
    /// A terminal failure consumes the input now; otherwise the input is
    /// retried until it has failed `max_attempts` times, then dead-lettered.
    pub terminal: bool,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancel {
    pub fence: IncarnationFence,
    pub accept_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTimer {
    pub fence: IncarnationFence,
    pub timer: TimerSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FireTimer {
    pub fence: IncarnationFence,
    pub timer_id: Id128,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelTimer {
    pub fence: IncarnationFence,
    pub timer_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    pub fence: IncarnationFence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordStep {
    pub fence: TurnFence,
    pub step_key: String,
    pub tool_name: String,
    pub tool_version: String,
    pub input_hash: Hash256,
    pub status: StepStatus,
    pub output: Option<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitIntent {
    pub fence: TurnFence,
    pub intent: IntentSpec,
}

/// Who authorizes an effect attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// The current owner, under its epoch.
    Owner(OwnerFence),
    /// An `agents_admin` reconciliation (decision 7).
    Reconcile(IncarnationFence),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizeAttempt {
    pub authority: Authority,
    pub effect_id: Id128,
    /// The attempt number being authorized: one more than the attempt the
    /// caller observed. A retry of a published authorization finds it equal
    /// to the recorded attempt and is answered without a second attempt.
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordEffectResult {
    pub fence: IncarnationFence,
    pub effect_id: Id128,
    pub attempt: u32,
    pub result: ObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Create(Create),
    Enqueue(Enqueue),
    Takeover(Takeover),
    BeginTurn(BeginTurn),
    CompleteTurn(CompleteTurn),
    FailTurn(FailTurn),
    Cancel(Cancel),
    ScheduleTimer(ScheduleTimer),
    FireTimer(FireTimer),
    CancelTimer(CancelTimer),
    Tombstone(Tombstone),
    RecordStep(RecordStep),
    CommitIntent(CommitIntent),
    AuthorizeAttempt(AuthorizeAttempt),
    RecordEffectResult(RecordEffectResult),
}

/// One check against the root the command is about to be published on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precondition {
    /// The agent exists and is not tombstoned.
    AgentActive,
    /// The fence's incarnation is the root's.
    IncarnationMatches,
    /// The fence's owner id and epoch are the root's.
    EpochMatches,
    /// The epoch the caller observed is the root's.
    ObservedEpochMatches,
    /// The root has an active turn with the fence's turn id.
    ActiveTurnMatches,
    /// The turn's base state version is the root's state version.
    BaseStateVersionMatches,
    /// The turn's handler version and digest are the fence's.
    HandlerMatches,
    /// The worker advertises a handler for the next input.
    HandlerAvailable,
    /// The turn is not blocked by a non-deterministic replay.
    TurnNotBlocked,
    /// No effect of the turn's input is in doubt.
    NoEffectInDoubt,
    /// No effect of the turn's input has an authorized attempt without a
    /// result.
    NoAuthorizedAttemptPending,
    /// A request id already recorded carries the same request hash.
    RequestHashMatches,
    /// The mailbox is below its depth cap.
    MailboxBelowCap,
    /// The receipt list is below its cap.
    ReceiptsBelowCap,
    /// The scheduled timer count stays within its cap.
    TimersBelowCap,
    /// A recorded step key carries the same tool identity and input hash.
    StepMatchesJournal,
    /// The effect exists and its state admits the transition.
    EffectStateAdmits,
    /// The attempt was authorized and carries no different final result.
    AttemptAuthorized,
    /// The timer generation is the scheduled one and is due.
    TimerGenerationCurrent,
    /// The encoded root stays under the byte cap. A publish the growth guard
    /// applies to (`Enqueue`, `RecordStep`, `CommitIntent`, `ScheduleTimer`,
    /// `FireTimer`, `CompleteTurn`, an owner's `AuthorizeAttempt`, unless it
    /// answers `Reply::Blocked`) also leaves `Limits::reserved_bytes` free.
    /// What that buys is exactly this: every outstanding input can still be
    /// ended by `FailTurn` with an error within `Limits::max_error_bytes`, or
    /// by `Cancel`, all of them, in any order
    /// (`tests::the_terminal_paths_fit_at_every_cap_after_the_worst_exempt_growth`).
    /// A `CompleteTurn` that does not fit is refused `BudgetExceeded`
    /// (`tests::a_completion_that_does_not_fit_is_refused_and_the_turn_can_still_fail`).
    HeadBelowByteCap,
    /// Every caller-supplied string in the command is at most
    /// `Limits::max_string_bytes`, an owner id at most
    /// `Limits::max_owner_bytes` and a handler version at most
    /// `Limits::max_version_bytes`.
    CallerStringsWithinCap,
    /// A turn commit buffers at most `Limits::max_events_per_turn` events.
    EventsBelowCap,
    /// A turn commit journals at most `Limits::max_intents_per_turn` intents.
    IntentsBelowCap,
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::Create(_) => "create",
            Command::Enqueue(_) => "enqueue",
            Command::Takeover(_) => "takeover",
            Command::BeginTurn(_) => "begin_turn",
            Command::CompleteTurn(_) => "complete_turn",
            Command::FailTurn(_) => "fail_turn",
            Command::Cancel(_) => "cancel",
            Command::ScheduleTimer(_) => "schedule_timer",
            Command::FireTimer(_) => "fire_timer",
            Command::CancelTimer(_) => "cancel_timer",
            Command::Tombstone(_) => "tombstone",
            Command::RecordStep(_) => "record_step",
            Command::CommitIntent(_) => "commit_intent",
            Command::AuthorizeAttempt(_) => "authorize_attempt",
            Command::RecordEffectResult(_) => "record_effect_result",
        }
    }

    pub fn preconditions(&self) -> &'static [Precondition] {
        use Precondition::*;
        match self {
            Command::Create(_) => &[HeadBelowByteCap],
            Command::Enqueue(_) => &[
                AgentActive,
                IncarnationMatches,
                RequestHashMatches,
                MailboxBelowCap,
                ReceiptsBelowCap,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::Takeover(_) => &[
                AgentActive,
                IncarnationMatches,
                ObservedEpochMatches,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::BeginTurn(_) => &[
                AgentActive,
                IncarnationMatches,
                EpochMatches,
                HandlerAvailable,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::CompleteTurn(_) => &[
                AgentActive,
                IncarnationMatches,
                EpochMatches,
                ActiveTurnMatches,
                BaseStateVersionMatches,
                HandlerMatches,
                TurnNotBlocked,
                NoEffectInDoubt,
                TimersBelowCap,
                EventsBelowCap,
                IntentsBelowCap,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::FailTurn(_) => &[
                AgentActive,
                IncarnationMatches,
                EpochMatches,
                ActiveTurnMatches,
                BaseStateVersionMatches,
                HandlerMatches,
                NoEffectInDoubt,
                NoAuthorizedAttemptPending,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::Cancel(_) => &[AgentActive, IncarnationMatches, HeadBelowByteCap],
            Command::ScheduleTimer(_) => &[
                AgentActive,
                IncarnationMatches,
                TimersBelowCap,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::FireTimer(_) => &[
                AgentActive,
                IncarnationMatches,
                TimerGenerationCurrent,
                MailboxBelowCap,
                HeadBelowByteCap,
            ],
            Command::CancelTimer(_) => &[
                AgentActive,
                IncarnationMatches,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::Tombstone(_) => &[IncarnationMatches, HeadBelowByteCap],
            Command::RecordStep(_) | Command::CommitIntent(_) => &[
                AgentActive,
                IncarnationMatches,
                EpochMatches,
                ActiveTurnMatches,
                BaseStateVersionMatches,
                HandlerMatches,
                TurnNotBlocked,
                StepMatchesJournal,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::AuthorizeAttempt(AuthorizeAttempt {
                authority: Authority::Owner(_),
                ..
            }) => &[
                AgentActive,
                IncarnationMatches,
                EpochMatches,
                EffectStateAdmits,
                HeadBelowByteCap,
                CallerStringsWithinCap,
            ],
            Command::AuthorizeAttempt(AuthorizeAttempt {
                authority: Authority::Reconcile(_),
                ..
            }) => &[
                AgentActive,
                IncarnationMatches,
                EffectStateAdmits,
                HeadBelowByteCap,
            ],
            Command::RecordEffectResult(_) => &[
                AgentActive,
                IncarnationMatches,
                EffectStateAdmits,
                AttemptAuthorized,
                HeadBelowByteCap,
            ],
        }
    }
}
