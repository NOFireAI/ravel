//! The pure transition kernel (ADR-3331 decisions 4 to 8).
//!
//! [`Kernel::apply`] is a function of `(head, command, now_unix_ns, ids)`
//! and nothing else: no store, no clock, no randomness. It either refuses
//! with a typed [`AgentError`], answers without a change
//! ([`Transition::Unchanged`], the idempotent-retry path), or returns the one
//! new root to CAS together with the side effects the coordinator performs
//! after the CAS succeeds ([`Transition::Publish`]).
//!
//! Every publish sets `root_revision` to the observed root's plus one,
//! recomputes the quota counters from the lists, and refuses a root whose
//! encoding exceeds the byte cap with `BudgetExceeded`. `state_version`
//! changes only in `complete_turn` (and resets on recreate, with the
//! incarnation). A turn commit publishes the new state, the consumed input,
//! its result, the buffered events, the timer changes and the new effect
//! intents in that one root.
//!
//! A retryable `FailTurn` keeps the input in the inbox and keeps its step
//! transcript and effect journal. Step and effect ids derive from the input's
//! `accept_seq`, not from the turn id, so the retry (a new turn id) replays
//! the failed attempt's completed steps and finds its journaled effects under
//! the same ids
//! (`tests::a_retry_reuses_the_effect_and_step_identities_of_the_failed_attempt`).
//! `FailTurn` is refused with `EffectInDoubt` while any effect of the input
//! is `InDoubt` (`NoEffectInDoubt`) or `Authorized` without a result
//! (`NoAuthorizedAttemptPending`)
//! (`tests::fail_turn_is_refused_while_a_manual_attempt_is_in_doubt`,
//! `tests::fail_turn_is_refused_while_an_attempt_is_authorized_without_a_result`).
//! So a retry never re-dispatches an effect the failed attempt authorized:
//! that effect has a recorded result before the input can fail, and the
//! retry's `AuthorizeAttempt` answers with it. The only effect an
//! `at_least_once` retry may execute is one that was never authorized (an
//! intent the failed attempt journaled and did not dispatch, or one the retry
//! commits itself). Besides effects, a retry re-runs the handler code between
//! journaled steps and any step recorded `Started` but never `Completed`.
//!
//! A cancel requested while a turn runs is never lost to a retry: a
//! `FailTurn` on that turn, retryable or not, consumes the input as
//! cancelled (`tests::a_cancel_requested_before_a_retryable_failure_consumes_the_input_as_cancelled`),
//! and a cancel arriving between the failure and the retry's `BeginTurn`
//! settles the waiting input at once
//! (`tests::a_cancel_between_a_retryable_failure_and_the_retry_settles_the_input`).
//! The request lives on the active turn; every path that ends a turn reads it.

use crate::command::{
    Authority, AuthorizeAttempt, BeginTurn, Cancel, CancelTimer, Command, CommitIntent,
    CompleteTurn, Create, Enqueue, FailTurn, FireTimer, IncarnationFence, IntentSpec, OwnerFence,
    RecordEffectResult, RecordStep, ScheduleTimer, Takeover, TimerChange, TimerSpec, Tombstone,
    TurnFence,
};
use crate::error::{AgentError, ErrorKind, SafeIds};
use crate::head::{
    ActiveTurn, Cancellation, EffectRecord, EffectStatus, EventEntry, Head, InboxEntry,
    InputSource, Lifecycle, MAX_HEAD_BYTES, ObjectRef, OutboxIntent, Quota, Receipt,
    RecoveryPolicy, ResultEntry, ResultStatus, StepRecord, StepStatus, TimerEntry, TimerStatus,
    TurnBlock,
};
use crate::identity::{self, Hash256, Id128};

/// Logical caps enforced inside the kernel (decision 9's per-tenant quotas
/// are applied per agent here; the store adds the tenant totals).
///
/// The byte cap carries one guarantee: it never refuses a `FailTurn` with an
/// error of at most `max_error_bytes`, or a `Cancel`, that ends an
/// outstanding input, and that holds for every outstanding input together,
/// in any order, with any interleaving of the commands the growth guard
/// exempts (`tests::every_outstanding_input_can_be_failed_at_the_byte_cap`,
/// `tests::the_terminal_paths_fit_at_every_cap_after_the_worst_exempt_growth`).
/// Their other preconditions, such as `NoEffectInDoubt`, still apply.
/// A `CompleteTurn` carries no such guarantee: one whose root would enter the
/// reserve below is refused `BudgetExceeded`, and the handler fails the turn
/// or emits less
/// (`tests::a_completion_that_does_not_fit_is_refused_and_the_turn_can_still_fail`).
///
/// The guard leaves three amounts free under `max_head_bytes`, each
/// measured on the prost encoding
/// (`tests::the_reserves_cover_the_growth_they_stand_for`):
///
/// - A fixed slack for the exempt growth of which a root holds at most one
///   copy: one `ActiveTurn` at its widest (handler `max_string_bytes`,
///   version `max_version_bytes`, every integer at its longest varint, 1186
///   bytes at the defaults) plus a `Takeover` delta (owner id
///   `max_owner_bytes` and its tag 66, epoch 11, lease hint 11), the
///   `root_revision` (11) and `quota.referenced_bytes` (11) varints at their
///   longest: 1296 bytes at the defaults. `BeginTurn` and `Takeover` replace
///   these fields rather than
///   add to them, so one slack covers any number of them.
/// - `terminal_reserve_per_input_bytes` for each input in the inbox.
/// - `PENDING_EFFECT_RESERVE_BYTES` for each journaled effect that is not
///   `Completed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_mailbox_depth: u64,
    pub max_receipts: u64,
    pub max_timers: u64,
    pub max_head_bytes: usize,
    /// Longest caller-supplied string (request id, handler, keys, tool
    /// identity) a command may carry.
    pub max_string_bytes: usize,
    /// Longest owner id a `Takeover`, `BeginTurn` or turn fence may carry.
    /// The root holds one, so it is part of the fixed slack.
    pub max_owner_bytes: usize,
    /// Longest handler version a turn fence or `HandlerPin` may carry; the
    /// active turn copies one.
    pub max_version_bytes: usize,
    /// Longest error text a `FailTurn` may carry; it is retained verbatim in
    /// the input's result entry, so it is bounded tighter than other strings.
    pub max_error_bytes: usize,
    /// Most events one `CompleteTurn` may buffer.
    pub max_events_per_turn: u64,
    /// Most effect intents one `CompleteTurn` may journal.
    pub max_intents_per_turn: u64,
    /// Bytes the growth guard leaves free for EACH input in the inbox. An
    /// input's own exempt growth is a retryable `FailTurn` (failed attempts
    /// 6, last failed turn id 18, the entry's length prefix 1: 25 bytes) and
    /// then the result entry its terminal `FailTurn` or `Cancel` publishes
    /// (field tag 2, length prefix 2, accept seq 11, status 2, turn id 18,
    /// dead-lettered flag 2, finish time 11, the error's tag 1 and length
    /// varint: 51 bytes plus the error at the defaults). Both length varints
    /// widen with the error, so the floor is
    /// [`result_entry_overhead_bytes`]`(max_error_bytes)` above the error:
    /// 332 at the defaults, which reserve 512, and 16462 at a 16384-byte
    /// error (`tests::the_result_floor_covers_the_varint_widths_of_a_wide_error`).
    /// The entry the terminal publish removes is not credited.
    pub terminal_reserve_per_input_bytes: usize,
}

/// Bytes the growth guard leaves free for each journaled effect that is not
/// `Completed`: an owner's retry authorization of an effect already
/// `Authorized` or `InDoubt`, or a `RecordEffectResult`, may still grow it,
/// and both are exempt. The attempt varint (6), the result reference (47),
/// the result attempt varint (6) and the record's length prefix (1) come to
/// 60; the status change costs nothing, since prost writes every status in
/// one byte. A result that settles an `InDoubt` effect also appends one audit
/// event (see [`AUDIT_RECONCILE_RESULT`]): its entry at the widest sequence
/// numbers and a `next_event_seq` varint one byte wider add 68, so 128 in all
/// (`tests::the_reserves_cover_the_growth_they_stand_for`).
pub const PENDING_EFFECT_RESERVE_BYTES: usize = 132;

/// Event type of the audit event a result appends when it settles an
/// `InDoubt` effect (`tests::reconciliation_is_audited_once_in_the_event_stream`).
pub const AUDIT_RECONCILE_RESULT: &str = "audit.reconcile_result";

/// Event type of the audit event a `Reconcile` authorization appends when it
/// authorizes another attempt
/// (`tests::reconciliation_is_audited_once_in_the_event_stream`).
pub const AUDIT_RECONCILE_ATTEMPT: &str = "audit.reconcile_attempt";

/// Bytes a terminal result entry costs beyond its error text, plus the
/// retryable `FailTurn` growth that precedes it, at the default
/// `max_error_bytes`: the 51 plus 25 the `Limits` doc derives. A wider error
/// widens two length varints; [`result_entry_overhead_bytes`] is the general
/// form.
pub const RESULT_ENTRY_OVERHEAD_BYTES: usize = 76;

/// Bytes a terminal result entry with a `max_error_bytes` error costs beyond
/// the error text, plus the 25 bytes of retryable `FailTurn` growth before
/// it. The entry body is 45 fixed bytes, the error's length varint and the
/// error; the entry adds a 2-byte field tag and its own length varint.
/// `Kernel::new` keeps `terminal_reserve_per_input_bytes` at or above
/// `max_error_bytes` plus this
/// (`tests::the_result_floor_covers_the_varint_widths_of_a_wide_error`).
pub fn result_entry_overhead_bytes(max_error_bytes: usize) -> usize {
    fn varint_len(v: usize) -> usize {
        let mut n = 1;
        let mut v = v >> 7;
        while v > 0 {
            n += 1;
            v >>= 7;
        }
        n
    }
    let error_len = varint_len(max_error_bytes);
    let body = 45usize
        .saturating_add(error_len)
        .saturating_add(max_error_bytes);
    45 + error_len + 2 + varint_len(body) + 25
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_mailbox_depth: 256,
            max_receipts: 1024,
            max_timers: 64,
            max_head_bytes: MAX_HEAD_BYTES,
            max_string_bytes: 1024,
            max_owner_bytes: 64,
            max_version_bytes: 64,
            max_error_bytes: 256,
            max_events_per_turn: 32,
            max_intents_per_turn: 16,
            terminal_reserve_per_input_bytes: 512,
        }
    }
}

impl Limits {
    /// The fixed slack described on [`Limits`], measured as the encoded size
    /// of a root whose singleton fields sit at their widest minus the same
    /// root at its narrowest. `template` supplies the agent's identity, which
    /// both roots share.
    pub fn exempt_growth_slack_bytes(&self, template: &Head) -> usize {
        let narrow = Head {
            tenant: template.tenant,
            kind: template.kind.clone(),
            key: template.key.clone(),
            incarnation: template.incarnation,
            root_revision: 0,
            owner_id: String::new(),
            epoch: 0,
            owner_lease_hint_unix_ns: 0,
            state_version: 0,
            state: None,
            next_accept_seq: 1,
            active_turn: None,
            timer_generation: 0,
            next_event_seq: 1,
            lifecycle: Lifecycle::Active,
            quota: Quota::default(),
            inbox: Vec::new(),
            receipts: Vec::new(),
            results: Vec::new(),
            events: Vec::new(),
            timers: Vec::new(),
            steps: Vec::new(),
            effects: Vec::new(),
            outbox: Vec::new(),
        };
        let wide = Head {
            root_revision: u64::MAX,
            owner_id: "o".repeat(self.max_owner_bytes),
            epoch: u64::MAX,
            owner_lease_hint_unix_ns: i64::MIN,
            active_turn: Some(ActiveTurn {
                input_accept_seq: u64::MAX,
                turn_id: Id128([u8::MAX; 16]),
                base_state_version: u64::MAX,
                handler: "h".repeat(self.max_string_bytes),
                handler_version: "v".repeat(self.max_version_bytes),
                handler_digest: Hash256([u8::MAX; 32]),
                cancellation: Cancellation::Requested,
                block: TurnBlock::NonDeterministicReplay,
                started_unix_ns: i64::MIN,
            }),
            quota: Quota {
                referenced_bytes: u64::MAX,
                ..Quota::default()
            },
            ..narrow.clone()
        };
        wide.encode().len().saturating_sub(narrow.encode().len())
    }

    /// Bytes the growth guard keeps free under `max_head_bytes` for `head`.
    pub fn reserved_bytes(&self, head: &Head) -> usize {
        let pending = head
            .effects
            .iter()
            .filter(|e| e.status != EffectStatus::Completed)
            .count();
        self.exempt_growth_slack_bytes(head)
            .saturating_add(
                self.terminal_reserve_per_input_bytes
                    .saturating_mul(head.inbox.len()),
            )
            .saturating_add(PENDING_EFFECT_RESERVE_BYTES.saturating_mul(pending))
    }
}

/// Source of the ids a transition mints. The production source derives
/// them, so a retried command mints the same id.
pub trait IdSource {
    fn turn_id(&self, incarnation: u64, accept_seq: u64, epoch: u64, failed_attempts: u32)
    -> Id128;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DerivedIds;

impl IdSource for DerivedIds {
    fn turn_id(
        &self,
        incarnation: u64,
        accept_seq: u64,
        epoch: u64,
        failed_attempts: u32,
    ) -> Id128 {
        identity::turn_id(incarnation, accept_seq, epoch, failed_attempts)
    }
}

/// Work the coordinator does after the CAS that published the root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// `CreateIfAbsent` the `_dir/<agent_id>` listing entry. The one effect
    /// done before the CAS rather than after it: the store writes it ahead
    /// of the HEAD write that creates the incarnation and refuses the create
    /// when it fails, so every agent with a HEAD has an entry.
    WriteDirEntry,
    /// Offer the agent to a polling worker.
    WakeWorker,
    /// `Overwrite` a `_wake/<bucket>/<agent_id>` hint for this due time.
    WriteWakeHint { due_unix_ns: i64 },
    /// Hand committed outbox intents to the effect dispatcher.
    DispatchIntents { effect_ids: Vec<Id128> },
    /// Release waiters on this operation.
    OperationTerminal { accept_seq: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Created {
        incarnation: u64,
    },
    Exists {
        incarnation: u64,
    },
    Accepted {
        accept_seq: u64,
        duplicate: bool,
    },
    Owned {
        epoch: u64,
    },
    TurnStarted(ActiveTurn),
    TurnResumed(ActiveTurn),
    Idle,
    TurnCommitted {
        accept_seq: u64,
        state_version: u64,
        rebased: bool,
    },
    /// A retry of a turn commit or failure that already published.
    AlreadyCommitted {
        accept_seq: u64,
    },
    TurnRetry {
        accept_seq: u64,
        failed_attempts: u32,
    },
    TurnFailed {
        accept_seq: u64,
        dead_lettered: bool,
    },
    CancelRequested {
        accept_seq: u64,
    },
    Cancelled {
        accept_seq: u64,
    },
    AlreadyTerminal {
        accept_seq: u64,
        status: ResultStatus,
    },
    TimerScheduled {
        timer_id: Id128,
        generation: u64,
    },
    TimerFired {
        accept_seq: u64,
    },
    TimerNotDue,
    TimerStale,
    TimerCancelled,
    /// A cancel that arrived after the firing CAS (decision 8).
    AlreadyFired,
    UnknownTimer,
    Tombstoned,
    AlreadyTombstoned,
    StepRecorded(StepRecord),
    StepReplayed(StepRecord),
    IntentCommitted {
        effect_id: Id128,
    },
    AttemptAuthorized {
        effect_id: Id128,
        attempt: u32,
    },
    EffectCompleted {
        effect_id: Id128,
        result: Option<ObjectRef>,
        attempt: u32,
    },
    EffectResultRecorded {
        effect_id: Id128,
    },
    /// A result for an attempt other than the recorded one, after the
    /// effect completed. Retained by the caller as evidence only.
    LateResultIgnored {
        effect_id: Id128,
    },
    /// The root was published to record a block, and the caller gets this
    /// error: `NonDeterministicReplay` or `EffectInDoubt`.
    Blocked(AgentError),
}

// Publish is the common result, so boxing its head would allocate on nearly
// every call to save space only on the rarer Unchanged.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    Publish {
        head: Head,
        effects: Vec<Effect>,
        reply: Reply,
    },
    Unchanged {
        reply: Reply,
    },
}

impl Transition {
    pub fn reply(&self) -> &Reply {
        match self {
            Transition::Publish { reply, .. } | Transition::Unchanged { reply } => reply,
        }
    }

    pub fn head(&self) -> Option<&Head> {
        match self {
            Transition::Publish { head, .. } => Some(head),
            Transition::Unchanged { .. } => None,
        }
    }
}

/// The limits are fixed by [`Kernel::new`], which raises the terminal
/// reserve to its floor, and read through [`Kernel::limits`]; a caller
/// cannot set a reserve below the floor by building the struct directly
/// (`tests::a_reserve_below_the_result_floor_is_raised_to_it`).
#[derive(Debug, Clone, Copy)]
pub struct Kernel {
    limits: Limits,
}

impl Default for Kernel {
    fn default() -> Self {
        Kernel::new(Limits::default())
    }
}

type Outcome = Result<Transition, AgentError>;

fn safe_ids(head: &Head) -> SafeIds {
    SafeIds {
        agent_id: Some(identity::agent_id(&head.kind, &head.key).to_hex()),
        incarnation: Some(head.incarnation),
        ..SafeIds::default()
    }
}

fn fail(head: &Head, kind: ErrorKind, reason: &'static str) -> AgentError {
    AgentError::new(kind, safe_ids(head), reason)
}

fn unchanged(reply: Reply) -> Outcome {
    Ok(Transition::Unchanged { reply })
}

fn turn_fence_strings(fence: &TurnFence) -> [&str; 2] {
    [
        fence.owner.owner_id.as_str(),
        fence.handler_version.as_str(),
    ]
}

fn intent_strings(intent: &IntentSpec) -> [&str; 3] {
    [
        intent.intent_key.as_str(),
        intent.tool_name.as_str(),
        intent.tool_version.as_str(),
    ]
}

impl Kernel {
    /// `terminal_reserve_per_input_bytes` is raised to the floor the `Limits`
    /// doc derives (`max_error_bytes + result_entry_overhead_bytes(..)`) when
    /// a caller configures less, so the terminal guarantee does not depend on
    /// the caller reading that doc
    /// (`tests::a_reserve_below_the_result_floor_is_raised_to_it`).
    pub fn new(mut limits: Limits) -> Kernel {
        let floor = limits
            .max_error_bytes
            .saturating_add(result_entry_overhead_bytes(limits.max_error_bytes));
        if limits.terminal_reserve_per_input_bytes < floor {
            limits.terminal_reserve_per_input_bytes = floor;
        }
        Kernel { limits }
    }

    /// The limits in force, with the terminal reserve at or above its floor.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn apply(
        &self,
        head: Option<&Head>,
        command: &Command,
        now_unix_ns: i64,
        ids: &dyn IdSource,
    ) -> Outcome {
        let Some(head) = head else {
            return match command {
                Command::Create(create) => self.create_new(create),
                _ => Err(AgentError::new(
                    ErrorKind::AgentGone,
                    SafeIds::default(),
                    "agent does not exist",
                )),
            };
        };
        self.check_strings(head, command)?;
        let outcome = match command {
            Command::Create(c) => self.create(head, c),
            Command::Enqueue(c) => self.enqueue(head, c, now_unix_ns),
            Command::Takeover(c) => self.takeover(head, c),
            Command::BeginTurn(c) => self.begin_turn(head, c, now_unix_ns, ids),
            Command::CompleteTurn(c) => self.complete_turn(head, c, now_unix_ns),
            Command::FailTurn(c) => self.fail_turn(head, c, now_unix_ns),
            Command::Cancel(c) => self.cancel(head, c, now_unix_ns),
            Command::ScheduleTimer(c) => self.schedule_timer(head, c),
            Command::FireTimer(c) => self.fire_timer(head, c, now_unix_ns),
            Command::CancelTimer(c) => self.cancel_timer(head, c),
            Command::Tombstone(c) => self.tombstone(head, c),
            Command::RecordStep(c) => self.record_step(head, c),
            Command::CommitIntent(c) => self.commit_intent(head, c),
            Command::AuthorizeAttempt(c) => self.authorize_attempt(head, c),
            Command::RecordEffectResult(c) => self.record_effect_result(head, c),
        };
        self.check_growth_headroom(head, command, outcome)
    }

    /// Every caller-supplied string a command carries is at most
    /// `Limits::max_string_bytes`, an owner id at most `max_owner_bytes`, a
    /// handler version at most `max_version_bytes`, and a `CompleteTurn`
    /// carries at most `max_events_per_turn` events and
    /// `max_intents_per_turn` intents. All are checked before any transition:
    /// the fixed slack the growth guard reserves is sized from the owner and
    /// version caps, so an owner id or version past them would void it.
    fn check_strings(&self, head: &Head, command: &Command) -> Result<(), AgentError> {
        let cap = self.limits.max_string_bytes;
        if caller_strings(command).iter().any(|s| s.len() > cap) {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "a caller-supplied string exceeds the string cap",
            ));
        }
        let (owners, versions) = identity_strings(command);
        if owners.iter().any(|s| s.len() > self.limits.max_owner_bytes) {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "the owner id exceeds its cap",
            ));
        }
        if versions
            .iter()
            .any(|s| s.len() > self.limits.max_version_bytes)
        {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "a handler version exceeds its cap",
            ));
        }
        if let Command::CompleteTurn(c) = command {
            if c.events.len() as u64 > self.limits.max_events_per_turn {
                return Err(fail(
                    head,
                    ErrorKind::BudgetExceeded,
                    "EventsBelowCap: the turn buffers too many events",
                ));
            }
            if c.intents.len() as u64 > self.limits.max_intents_per_turn {
                return Err(fail(
                    head,
                    ErrorKind::BudgetExceeded,
                    "IntentsBelowCap: the turn journals too many intents",
                ));
            }
        }
        if let Command::FailTurn(c) = command
            && c.error.len() > self.limits.max_error_bytes
        {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "the failure error text exceeds its cap",
            ));
        }
        Ok(())
    }
}

/// Every caller-supplied string a command carries, in a fixed order; the
/// precondition table lists `CallerStringsWithinCap` exactly for the commands
/// that return a non-empty set here.
pub(crate) fn caller_strings(command: &Command) -> Vec<&str> {
    {
        let mut strings: Vec<&str> = Vec::new();
        match command {
            Command::Create(_) | Command::Cancel(_) | Command::Tombstone(_) => {}
            Command::Enqueue(c) => strings.extend([c.request_id.as_str(), c.handler.as_str()]),
            Command::Takeover(c) => strings.push(c.owner_id.as_str()),
            Command::BeginTurn(c) => {
                strings.push(c.fence.owner_id.as_str());
                for pin in &c.handlers {
                    strings.extend([pin.handler.as_str(), pin.version.as_str()]);
                }
            }
            Command::CompleteTurn(c) => {
                strings.extend(turn_fence_strings(&c.fence));
                for e in &c.events {
                    strings.push(e.event_type.as_str());
                }
                for t in &c.timer_changes {
                    match t {
                        TimerChange::Schedule(spec) => {
                            strings.extend([spec.timer_key.as_str(), spec.handler.as_str()]);
                        }
                        TimerChange::Cancel { timer_key } => strings.push(timer_key.as_str()),
                    }
                }
                for i in &c.intents {
                    strings.extend(intent_strings(i));
                }
            }
            Command::FailTurn(c) => {
                strings.extend(turn_fence_strings(&c.fence));
                strings.push(c.error.as_str());
            }
            Command::ScheduleTimer(c) => {
                strings.extend([c.timer.timer_key.as_str(), c.timer.handler.as_str()]);
            }
            Command::FireTimer(_) => {}
            Command::CancelTimer(c) => strings.push(c.timer_key.as_str()),
            Command::RecordStep(c) => {
                strings.extend(turn_fence_strings(&c.fence));
                strings.extend([
                    c.step_key.as_str(),
                    c.tool_name.as_str(),
                    c.tool_version.as_str(),
                ]);
            }
            Command::CommitIntent(c) => {
                strings.extend(turn_fence_strings(&c.fence));
                strings.extend(intent_strings(&c.intent));
            }
            Command::AuthorizeAttempt(c) => {
                if let Authority::Owner(o) = &c.authority {
                    strings.push(o.owner_id.as_str());
                }
            }
            Command::RecordEffectResult(_) => {}
        }
        strings
    }
}

/// The owner ids and handler versions a command carries, bounded by
/// `Limits::max_owner_bytes` and `Limits::max_version_bytes`.
fn identity_strings(command: &Command) -> (Vec<&str>, Vec<&str>) {
    let mut owners: Vec<&str> = Vec::new();
    let mut versions: Vec<&str> = Vec::new();
    let fence = match command {
        Command::CompleteTurn(c) => Some(&c.fence),
        Command::FailTurn(c) => Some(&c.fence),
        Command::RecordStep(c) => Some(&c.fence),
        Command::CommitIntent(c) => Some(&c.fence),
        _ => None,
    };
    if let Some(fence) = fence {
        owners.push(fence.owner.owner_id.as_str());
        versions.push(fence.handler_version.as_str());
    }
    match command {
        Command::Takeover(c) => owners.push(c.owner_id.as_str()),
        Command::BeginTurn(c) => {
            owners.push(c.fence.owner_id.as_str());
            versions.extend(c.handlers.iter().map(|p| p.version.as_str()));
        }
        Command::AuthorizeAttempt(c) => {
            if let Authority::Owner(o) = &c.authority {
                owners.push(o.owner_id.as_str());
            }
        }
        Command::CompleteTurn(_)
        | Command::FailTurn(_)
        | Command::RecordStep(_)
        | Command::CommitIntent(_)
        | Command::Create(_)
        | Command::Enqueue(_)
        | Command::Cancel(_)
        | Command::ScheduleTimer(_)
        | Command::FireTimer(_)
        | Command::CancelTimer(_)
        | Command::Tombstone(_)
        | Command::RecordEffectResult(_) => {}
    }
    (owners, versions)
}

/// Whether the growth guard applies to a publish. Exempt are the terminal
/// paths (`FailTurn`, `Cancel`;
/// `tests::terminal_commands_keep_a_path_through_the_byte_cap`), the
/// commands that only move a turn or an owner along (`Takeover`,
/// `BeginTurn`, `CancelTimer`, `Tombstone`, `Create`;
/// `tests::the_lifecycle_commands_publish_at_the_byte_cap`,
/// `tests::the_terminal_paths_fit_at_every_cap_after_the_worst_exempt_growth`),
/// the latches (a publish answering `Reply::Blocked`, which blocks the turn
/// or marks an effect `InDoubt`; `tests::a_blocking_replay_is_recorded_inside_the_reserve`),
/// and what an effect needs to settle (`RecordEffectResult`;
/// `tests::pending_effects_settle_inside_the_reserve`). Refusing a latch would let the next
/// `CompleteTurn` commit past it. A `Reconcile` authorization is guarded: it
/// appends an audit event every time it publishes, and an effect can be
/// latched and reconciled any number of times, so its growth has no bound a
/// reserve could cover. An effect near the cap still settles through a
/// reconciled result, which is exempt
/// (`tests::a_manual_attempt_enters_in_doubt_inside_the_reserve`).
///
/// Audit events are not counted against `Limits::max_events_per_turn`, which
/// covers only the events a `CompleteTurn` carries; they are bounded by the
/// byte cap alone. A reconciled result (an attach) is exempt and appends one
/// inside the per-effect reserve; a `Reconcile` authorization is guarded and
/// appends one only while the root stays outside the reserves, so
/// a latch-and-reconcile loop ends in `BudgetExceeded` on a decodable root
/// (`tests::latch_and_reconcile_cycles_are_bounded_by_the_byte_cap`).
fn guarded(observed: &Head, command: &Command, reply: &Reply) -> bool {
    if matches!(reply, Reply::Blocked(_)) {
        return false;
    }
    match command {
        Command::Enqueue(_)
        | Command::RecordStep(_)
        | Command::CommitIntent(_)
        | Command::ScheduleTimer(_)
        | Command::FireTimer(_)
        | Command::CompleteTurn(_) => true,
        // The first owner authorization of an effect adds the attempt record
        // and is guarded. A later owner authorization of an effect already
        // `Authorized` or `InDoubt` is the retry a new owner issues after a
        // crash (decision 7's at_least_once and provider_idempotency
        // recovery); it only replaces the attempt number, fits in the
        // per-effect reserve, and must not be refused inside the reserve, or
        // the input could neither be retried nor failed
        // (`tests::an_owner_retry_authorization_is_answered_inside_the_reserve`).
        Command::AuthorizeAttempt(c) => match &c.authority {
            Authority::Reconcile(_) => true,
            Authority::Owner(_) => !observed.effects.iter().any(|e| {
                e.effect_id == c.effect_id
                    && matches!(e.status, EffectStatus::Authorized | EffectStatus::InDoubt)
            }),
        },
        Command::Create(_)
        | Command::Takeover(_)
        | Command::BeginTurn(_)
        | Command::FailTurn(_)
        | Command::Cancel(_)
        | Command::CancelTimer(_)
        | Command::Tombstone(_)
        | Command::RecordEffectResult(_) => false,
    }
}

impl Kernel {
    /// A guarded publish (see [`guarded`]) must leave
    /// `Limits::reserved_bytes` of the published root free under the byte
    /// cap: the fixed slack, the per-input reserve for every input in the
    /// inbox and the per-effect reserve for every effect not yet completed.
    /// Every exempt publish fits inside what those reserve, so the terminal
    /// paths named on [`Limits`] stay open
    /// (`tests::the_terminal_paths_fit_at_every_cap_after_the_worst_exempt_growth`,
    /// `tests::pending_effects_settle_inside_the_reserve`), and a latch is
    /// recorded however far into the reserve the root already is
    /// (`tests::a_blocking_replay_is_recorded_inside_the_reserve`,
    /// `tests::a_manual_attempt_enters_in_doubt_inside_the_reserve`). An
    /// exempt publish still faces the cap itself in `publish`. The check
    /// reads the published root, not the observed one: an idempotent retry
    /// that answers `Unchanged` (a duplicate enqueue, a step replay, an
    /// already-authorized attempt) is never refused, so a client near the cap
    /// can still resolve what it already published.
    fn check_growth_headroom(
        &self,
        observed: &Head,
        command: &Command,
        outcome: Outcome,
    ) -> Outcome {
        let transition = outcome?;
        let Transition::Publish { head, reply, .. } = &transition else {
            return Ok(transition);
        };
        if !guarded(observed, command, reply) {
            return Ok(transition);
        }
        let cap = self
            .limits
            .max_head_bytes
            .saturating_sub(self.limits.reserved_bytes(head));
        if head.encode().len() > cap {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "HEAD would enter the terminal reserve of its byte cap",
            ));
        }
        Ok(transition)
    }

    fn publish(
        &self,
        before: Option<&Head>,
        mut head: Head,
        effects: Vec<Effect>,
        reply: Reply,
    ) -> Outcome {
        head.root_revision = match before {
            Some(before) => before.root_revision.checked_add(1).ok_or_else(|| {
                fail(
                    before,
                    ErrorKind::BudgetExceeded,
                    "root revision is exhausted",
                )
            })?,
            None => 1,
        };
        head.quota = head.computed_quota();
        if head.encode().len() > self.limits.max_head_bytes {
            return Err(fail(
                &head,
                ErrorKind::BudgetExceeded,
                "HEAD would exceed its byte cap",
            ));
        }
        Ok(Transition::Publish {
            head,
            effects,
            reply,
        })
    }

    fn create_new(&self, c: &Create) -> Outcome {
        let head = Head {
            tenant: c.tenant,
            kind: c.kind.clone(),
            key: c.key.clone(),
            incarnation: 1,
            root_revision: 0,
            owner_id: String::new(),
            epoch: 0,
            owner_lease_hint_unix_ns: 0,
            state_version: 0,
            state: None,
            next_accept_seq: 1,
            active_turn: None,
            timer_generation: 0,
            next_event_seq: 1,
            lifecycle: Lifecycle::Active,
            quota: Quota::default(),
            inbox: Vec::new(),
            receipts: Vec::new(),
            results: Vec::new(),
            events: Vec::new(),
            timers: Vec::new(),
            steps: Vec::new(),
            effects: Vec::new(),
            outbox: Vec::new(),
        };
        self.publish(
            None,
            head,
            vec![Effect::WriteDirEntry],
            Reply::Created { incarnation: 1 },
        )
    }

    fn create(&self, head: &Head, c: &Create) -> Outcome {
        if head.tenant != c.tenant || head.kind != c.kind || head.key != c.key {
            return Err(fail(
                head,
                ErrorKind::AgentGone,
                "HEAD names a different agent",
            ));
        }
        if head.lifecycle == Lifecycle::Active {
            return unchanged(Reply::Exists {
                incarnation: head.incarnation,
            });
        }
        let incarnation = head
            .incarnation
            .checked_add(1)
            .ok_or_else(|| fail(head, ErrorKind::BudgetExceeded, "incarnation is exhausted"))?;
        // The epoch is kept so an owner of the old incarnation stays fenced.
        let new = Head {
            incarnation,
            owner_id: String::new(),
            owner_lease_hint_unix_ns: 0,
            state_version: 0,
            state: None,
            next_accept_seq: 1,
            active_turn: None,
            timer_generation: 0,
            next_event_seq: 1,
            lifecycle: Lifecycle::Active,
            inbox: Vec::new(),
            receipts: Vec::new(),
            results: Vec::new(),
            events: Vec::new(),
            timers: Vec::new(),
            steps: Vec::new(),
            effects: Vec::new(),
            outbox: Vec::new(),
            ..head.clone()
        };
        self.publish(
            Some(head),
            new,
            vec![Effect::WriteDirEntry],
            Reply::Created { incarnation },
        )
    }

    fn check_active(head: &Head) -> Result<(), AgentError> {
        match head.lifecycle {
            Lifecycle::Active => Ok(()),
            Lifecycle::Tombstoned => Err(fail(head, ErrorKind::AgentGone, "agent is tombstoned")),
        }
    }

    fn check_incarnation(head: &Head, fence: &IncarnationFence) -> Result<(), AgentError> {
        Self::check_active(head)?;
        if fence.incarnation != head.incarnation {
            return Err(fail(
                head,
                ErrorKind::AgentGone,
                "handle names another incarnation",
            ));
        }
        Ok(())
    }

    fn check_owner(head: &Head, fence: &OwnerFence) -> Result<(), AgentError> {
        Self::check_incarnation(
            head,
            &IncarnationFence {
                incarnation: fence.incarnation,
            },
        )?;
        if fence.epoch != head.epoch || fence.owner_id != head.owner_id {
            return Err(fail(
                head,
                ErrorKind::OwnershipLost,
                "owner epoch no longer matches",
            ));
        }
        Ok(())
    }

    /// The owner and turn checks shared by every turn-scoped command.
    /// `Ok(Err(seq))` is a retry of a commit that already published.
    fn check_turn<'h>(
        head: &'h Head,
        fence: &TurnFence,
    ) -> Result<Result<&'h ActiveTurn, u64>, AgentError> {
        Self::check_owner(head, &fence.owner)?;
        let Some(turn) = head
            .active_turn
            .as_ref()
            .filter(|t| t.turn_id == fence.turn_id)
        else {
            return match head
                .results
                .iter()
                .find(|r| r.turn_id == Some(fence.turn_id))
            {
                Some(result) => Ok(Err(result.accept_seq)),
                None => Err(fail(
                    head,
                    ErrorKind::OwnershipLost,
                    "turn is no longer active",
                )),
            };
        };
        // Only complete_turn moves state_version, and it ends the turn, so an
        // active turn's base is always the root's state_version (asserted on
        // every published root by the property test's `check_root`).
        if fence.base_state_version != turn.base_state_version {
            return Err(fail(
                head,
                ErrorKind::OwnershipLost,
                "base state version moved",
            ));
        }
        if fence.handler_version != turn.handler_version
            || fence.handler_digest != turn.handler_digest
        {
            return Err(fail(
                head,
                ErrorKind::IncompatibleHandler,
                "handler version differs from the turn's",
            ));
        }
        Ok(Ok(turn))
    }

    /// `check_turn` for commands that only make sense while the turn runs.
    fn require_running<'h>(
        head: &'h Head,
        fence: &TurnFence,
    ) -> Result<&'h ActiveTurn, AgentError> {
        let turn = Self::check_turn(head, fence)?
            .map_err(|_| fail(head, ErrorKind::OwnershipLost, "turn already committed"))?;
        if turn.block != TurnBlock::None {
            return Err(fail(
                head,
                ErrorKind::NonDeterministicReplay,
                "turn is blocked by a non-deterministic replay",
            ));
        }
        Ok(turn)
    }

    fn enqueue(&self, head: &Head, c: &Enqueue, now: i64) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        if let Some(receipt) = head
            .receipts
            .iter()
            .find(|r| r.request_id == c.request_id && r.handler == c.handler)
        {
            if receipt.request_hash != c.request_hash {
                return Err(fail(
                    head,
                    ErrorKind::IdempotencyConflict,
                    "request id reused with a different request",
                ));
            }
            return unchanged(Reply::Accepted {
                accept_seq: receipt.accept_seq,
                duplicate: true,
            });
        }
        if head.inbox.len() as u64 >= self.limits.max_mailbox_depth {
            return Err(fail(head, ErrorKind::MailboxFull, "mailbox is full"));
        }
        if head.receipts.len() as u64 >= self.limits.max_receipts {
            return Err(fail(
                head,
                ErrorKind::BudgetExceeded,
                "receipt list is at its cap",
            ));
        }
        let mut new = head.clone();
        let accept_seq = new.next_accept_seq;
        new.next_accept_seq += 1;
        new.inbox.push(InboxEntry {
            accept_seq,
            handler: c.handler.clone(),
            source: InputSource::Message {
                request_id: c.request_id.clone(),
                request_hash: c.request_hash,
            },
            payload: c.payload,
            accepted_unix_ns: now,
            failed_attempts: 0,
            last_failed_turn_id: None,
        });
        new.receipts.push(Receipt {
            request_id: c.request_id.clone(),
            handler: c.handler.clone(),
            request_hash: c.request_hash,
            accept_seq,
        });
        self.publish(
            Some(head),
            new,
            vec![Effect::WakeWorker],
            Reply::Accepted {
                accept_seq,
                duplicate: false,
            },
        )
    }

    fn takeover(&self, head: &Head, c: &Takeover) -> Outcome {
        Self::check_incarnation(
            head,
            &IncarnationFence {
                incarnation: c.incarnation,
            },
        )?;
        if c.observed_epoch != head.epoch {
            return Err(fail(
                head,
                ErrorKind::OwnershipLost,
                "another owner advanced the epoch",
            ));
        }
        let mut new = head.clone();
        new.epoch = head
            .epoch
            .checked_add(1)
            .ok_or_else(|| fail(head, ErrorKind::BudgetExceeded, "epoch is exhausted"))?;
        new.owner_id = c.owner_id.clone();
        new.owner_lease_hint_unix_ns = c.lease_hint_unix_ns;
        let has_work = !new.inbox.is_empty() || new.active_turn.is_some();
        let epoch = new.epoch;
        self.publish(
            Some(head),
            new,
            if has_work {
                vec![Effect::WakeWorker]
            } else {
                Vec::new()
            },
            Reply::Owned { epoch },
        )
    }

    fn begin_turn(&self, head: &Head, c: &BeginTurn, now: i64, ids: &dyn IdSource) -> Outcome {
        Self::check_owner(head, &c.fence)?;
        if let Some(turn) = &head.active_turn {
            let pinned = c.handlers.iter().any(|p| {
                p.handler == turn.handler
                    && p.version == turn.handler_version
                    && p.digest == turn.handler_digest
            });
            if !pinned {
                return Err(fail(
                    head,
                    ErrorKind::IncompatibleHandler,
                    "worker lacks the active turn's handler version",
                ));
            }
            return unchanged(Reply::TurnResumed(turn.clone()));
        }
        let Some(next) = head.inbox.first() else {
            return unchanged(Reply::Idle);
        };
        let Some(pin) = c.handlers.iter().find(|p| p.handler == next.handler) else {
            return Err(fail(
                head,
                ErrorKind::IncompatibleHandler,
                "worker has no handler for the next input",
            ));
        };
        let turn = ActiveTurn {
            input_accept_seq: next.accept_seq,
            turn_id: ids.turn_id(
                head.incarnation,
                next.accept_seq,
                head.epoch,
                next.failed_attempts,
            ),
            base_state_version: head.state_version,
            handler: pin.handler.clone(),
            handler_version: pin.version.clone(),
            handler_digest: pin.digest,
            cancellation: Cancellation::None,
            block: TurnBlock::None,
            started_unix_ns: now,
        };
        let mut new = head.clone();
        new.active_turn = Some(turn.clone());
        self.publish(Some(head), new, Vec::new(), Reply::TurnStarted(turn))
    }

    fn take_input(head: &Head, new: &mut Head, accept_seq: u64) -> Result<(), AgentError> {
        let at = new
            .inbox
            .iter()
            .position(|e| e.accept_seq == accept_seq)
            .ok_or_else(|| {
                fail(
                    head,
                    ErrorKind::OwnershipLost,
                    "active turn's input is not in the inbox",
                )
            })?;
        new.inbox.remove(at);
        Ok(())
    }

    /// Consume a waiting input as cancelled: no turn is active on it. Its
    /// result names the turn that last failed on it, if one ran.
    fn settle_cancelled(&self, head: &Head, accept_seq: u64, now: i64) -> Outcome {
        let turn_id = head
            .inbox
            .iter()
            .find(|e| e.accept_seq == accept_seq)
            .and_then(|e| e.last_failed_turn_id);
        let mut new = head.clone();
        Self::take_input(head, &mut new, accept_seq)?;
        drop_input_steps(&mut new, accept_seq);
        new.results.push(ResultEntry {
            accept_seq,
            status: ResultStatus::Cancelled,
            output: None,
            error: String::new(),
            turn_id,
            dead_lettered: false,
            finished_unix_ns: now,
        });
        self.publish(
            Some(head),
            new,
            vec![Effect::OperationTerminal { accept_seq }],
            Reply::Cancelled { accept_seq },
        )
    }

    /// `CompleteTurn` is refused while an effect of the input is in doubt, but
    /// not while one is merely authorized without a result: a handler that
    /// completed awaited the attempt it authorized and knows its outcome, which
    /// a failing handler does not, so `FailTurn` refuses both.
    ///
    /// An intent listed in `intents` whose key the input already journaled
    /// (by a `CommitIntent` during this or an earlier turn of the input) is
    /// refused `IdempotencyConflict` when the spec is the same, since the
    /// outbox would never dispatch it
    /// (`tests::a_completion_relisting_a_mid_turn_intent_is_refused_typed`), and
    /// blocks the turn `NonDeterministicReplay` when the spec differs, as two
    /// differing intents under one key in the list do
    /// (`tests::a_completion_with_a_conflicting_intent_key_blocks_the_turn`).
    /// The input's step transcript is dropped, and no other input's
    /// (`tests::a_terminal_turn_drops_only_its_own_input_steps`).
    fn complete_turn(&self, head: &Head, c: &CompleteTurn, now: i64) -> Outcome {
        let turn = match Self::check_turn(head, &c.fence)? {
            Ok(turn) => turn,
            Err(accept_seq) => return unchanged(Reply::AlreadyCommitted { accept_seq }),
        };
        if turn.block != TurnBlock::None {
            return Err(fail(
                head,
                ErrorKind::NonDeterministicReplay,
                "turn is blocked by a non-deterministic replay",
            ));
        }
        if input_effects(head, turn.input_accept_seq).any(|e| e.status == EffectStatus::InDoubt) {
            return Err(fail(
                head,
                ErrorKind::EffectInDoubt,
                "NoEffectInDoubt: an effect of this turn is in doubt",
            ));
        }
        let accept_seq = turn.input_accept_seq;
        let turn_id = turn.turn_id;
        if turn.cancellation != Cancellation::Requested {
            for intent in &c.intents {
                let effect_id =
                    identity::effect_id(head.incarnation, accept_seq, &intent.intent_key);
                let Some(existing) = head.effects.iter().find(|e| e.effect_id == effect_id) else {
                    continue;
                };
                if !same_intent(existing, intent) {
                    return self
                        .block_turn(head, "intent key reused with a different tool or input");
                }
                return Err(fail(
                    head,
                    ErrorKind::IdempotencyConflict,
                    "intent key already journaled by a CommitIntent of this input",
                ));
            }
        }
        let mut new = head.clone();
        Self::take_input(head, &mut new, accept_seq)?;
        new.active_turn = None;
        drop_input_steps(&mut new, accept_seq);
        let mut effects = vec![Effect::OperationTerminal { accept_seq }];
        if turn.cancellation == Cancellation::Requested {
            new.results.push(ResultEntry {
                accept_seq,
                status: ResultStatus::Cancelled,
                output: None,
                error: String::new(),
                turn_id: Some(turn_id),
                dead_lettered: false,
                finished_unix_ns: now,
            });
        } else {
            new.state_version += 1;
            new.state = Some(c.new_state);
            new.results.push(ResultEntry {
                accept_seq,
                status: ResultStatus::Completed,
                output: c.output,
                error: String::new(),
                turn_id: Some(turn_id),
                dead_lettered: false,
                finished_unix_ns: now,
            });
            for event in &c.events {
                new.events.push(EventEntry {
                    event_seq: new.next_event_seq,
                    accept_seq,
                    turn_id,
                    event_type: event.event_type.clone(),
                    payload: event.payload,
                });
                new.next_event_seq += 1;
            }
            for change in &c.timer_changes {
                match change {
                    TimerChange::Schedule(spec) => {
                        schedule(&mut new, spec);
                        effects.push(Effect::WriteWakeHint {
                            due_unix_ns: spec.due_unix_ns,
                        });
                    }
                    TimerChange::Cancel { timer_key } => {
                        let id = identity::timer_id(new.incarnation, timer_key);
                        if let Some(t) = new
                            .timers
                            .iter_mut()
                            .find(|t| t.timer_id == id && t.status == TimerStatus::Scheduled)
                        {
                            t.status = TimerStatus::Cancelled;
                        }
                    }
                }
            }
            self.check_timer_cap(&new)?;
            let mut dispatched = Vec::new();
            for intent in &c.intents {
                let added = match add_intent(head, &mut new, accept_seq, turn_id, intent) {
                    Ok(added) => added,
                    Err(e) if e.kind == ErrorKind::NonDeterministicReplay => {
                        return self
                            .block_turn(head, "intent key reused with a different tool or input");
                    }
                    Err(e) => return Err(e),
                };
                if let Some(effect_id) = added {
                    new.outbox.push(OutboxIntent {
                        effect_id,
                        intent_key: intent.intent_key.clone(),
                        tool_name: intent.tool_name.clone(),
                        tool_version: intent.tool_version.clone(),
                        policy: intent.policy,
                        input: intent.input,
                        turn_id,
                    });
                    dispatched.push(effect_id);
                }
            }
            if !dispatched.is_empty() {
                effects.push(Effect::DispatchIntents {
                    effect_ids: dispatched,
                });
            }
        }
        if !new.inbox.is_empty() {
            effects.push(Effect::WakeWorker);
        }
        let state_version = new.state_version;
        self.publish(
            Some(head),
            new,
            effects,
            Reply::TurnCommitted {
                accept_seq,
                state_version,
                rebased: c.observed_root_revision != head.root_revision,
            },
        )
    }

    /// A retry of the latest retryable failure is answered `TurnRetry` from the
    /// inbox entry's `last_failed_turn_id`. Only the latest failed turn is
    /// kept there, so a replay of an OLDER attempt's `FailTurn`, after a newer
    /// attempt of the same input also failed, is refused `OwnershipLost`: its
    /// turn is neither active nor the one the entry names, and that caller is
    /// a superseded worker whose verdict was already overtaken
    /// (`tests::an_older_attempts_fail_replay_after_a_newer_failure_is_ownership_lost`).
    /// The terminal paths drop only this input's step transcript
    /// (`tests::a_terminal_turn_drops_only_its_own_input_steps`).
    fn fail_turn(&self, head: &Head, c: &FailTurn, now: i64) -> Outcome {
        Self::check_owner(head, &c.fence.owner)?;
        // A retry of a retryable failure that already published: the input
        // still waits, and its entry names the turn that failed.
        if let Some(entry) = head
            .inbox
            .iter()
            .find(|e| e.last_failed_turn_id == Some(c.fence.turn_id))
        {
            return unchanged(Reply::TurnRetry {
                accept_seq: entry.accept_seq,
                failed_attempts: entry.failed_attempts,
            });
        }
        let turn = match Self::check_turn(head, &c.fence)? {
            Ok(turn) => turn,
            Err(accept_seq) => return unchanged(Reply::AlreadyCommitted { accept_seq }),
        };
        let accept_seq = turn.input_accept_seq;
        for effect in input_effects(head, accept_seq) {
            match effect.status {
                EffectStatus::InDoubt => {
                    return Err(fail(
                        head,
                        ErrorKind::EffectInDoubt,
                        "NoEffectInDoubt: an effect of this input awaits reconciliation",
                    ));
                }
                EffectStatus::Authorized => {
                    return Err(fail(
                        head,
                        ErrorKind::EffectInDoubt,
                        "NoAuthorizedAttemptPending: an authorized attempt has no result",
                    ));
                }
                EffectStatus::Intent | EffectStatus::Completed => {}
            }
        }
        if turn.cancellation == Cancellation::Requested {
            let mut new = head.clone();
            Self::take_input(head, &mut new, accept_seq)?;
            new.active_turn = None;
            drop_input_steps(&mut new, accept_seq);
            new.results.push(ResultEntry {
                accept_seq,
                status: ResultStatus::Cancelled,
                output: None,
                error: String::new(),
                turn_id: Some(turn.turn_id),
                dead_lettered: false,
                finished_unix_ns: now,
            });
            let mut effects = vec![Effect::OperationTerminal { accept_seq }];
            if !new.inbox.is_empty() {
                effects.push(Effect::WakeWorker);
            }
            return self.publish(Some(head), new, effects, Reply::Cancelled { accept_seq });
        }
        let mut new = head.clone();
        new.active_turn = None;
        let entry = new
            .inbox
            .iter_mut()
            .find(|e| e.accept_seq == accept_seq)
            .ok_or_else(|| {
                fail(
                    head,
                    ErrorKind::OwnershipLost,
                    "active turn's input is not in the inbox",
                )
            })?;
        let failed_attempts = entry.failed_attempts.saturating_add(1);
        if !c.terminal && failed_attempts < c.max_attempts {
            entry.failed_attempts = failed_attempts;
            entry.last_failed_turn_id = Some(turn.turn_id);
            // The transcript and journal stay for the retry.
            return self.publish(
                Some(head),
                new,
                vec![Effect::WakeWorker],
                Reply::TurnRetry {
                    accept_seq,
                    failed_attempts,
                },
            );
        }
        let dead_lettered = !c.terminal;
        Self::take_input(head, &mut new, accept_seq)?;
        drop_input_steps(&mut new, accept_seq);
        new.results.push(ResultEntry {
            accept_seq,
            status: ResultStatus::Failed,
            output: None,
            error: c.error.clone(),
            turn_id: Some(turn.turn_id),
            dead_lettered,
            finished_unix_ns: now,
        });
        let mut effects = vec![Effect::OperationTerminal { accept_seq }];
        if !new.inbox.is_empty() {
            effects.push(Effect::WakeWorker);
        }
        self.publish(
            Some(head),
            new,
            effects,
            Reply::TurnFailed {
                accept_seq,
                dead_lettered,
            },
        )
    }

    fn cancel(&self, head: &Head, c: &Cancel, now: i64) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        if let Some(result) = head.results.iter().find(|r| r.accept_seq == c.accept_seq) {
            return unchanged(Reply::AlreadyTerminal {
                accept_seq: c.accept_seq,
                status: result.status,
            });
        }
        if let Some(turn) = head
            .active_turn
            .as_ref()
            .filter(|t| t.input_accept_seq == c.accept_seq)
        {
            if turn.cancellation == Cancellation::Requested {
                return unchanged(Reply::CancelRequested {
                    accept_seq: c.accept_seq,
                });
            }
            let mut new = head.clone();
            if let Some(turn) = new.active_turn.as_mut() {
                turn.cancellation = Cancellation::Requested;
            }
            return self.publish(
                Some(head),
                new,
                Vec::new(),
                Reply::CancelRequested {
                    accept_seq: c.accept_seq,
                },
            );
        }
        if !head.inbox.iter().any(|e| e.accept_seq == c.accept_seq) {
            return Err(fail(
                head,
                ErrorKind::AgentGone,
                "operation was never accepted",
            ));
        }
        self.settle_cancelled(head, c.accept_seq, now)
    }

    fn check_timer_cap(&self, new: &Head) -> Result<(), AgentError> {
        let scheduled = new
            .timers
            .iter()
            .filter(|t| t.status == TimerStatus::Scheduled)
            .count() as u64;
        if scheduled > self.limits.max_timers {
            return Err(fail(
                new,
                ErrorKind::BudgetExceeded,
                "timer count is at its cap",
            ));
        }
        Ok(())
    }

    fn schedule_timer(&self, head: &Head, c: &ScheduleTimer) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        let mut new = head.clone();
        let (timer_id, generation) = schedule(&mut new, &c.timer);
        self.check_timer_cap(&new)?;
        self.publish(
            Some(head),
            new,
            vec![Effect::WriteWakeHint {
                due_unix_ns: c.timer.due_unix_ns,
            }],
            Reply::TimerScheduled {
                timer_id,
                generation,
            },
        )
    }

    fn fire_timer(&self, head: &Head, c: &FireTimer, now: i64) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        let Some(timer) = head.timers.iter().find(|t| {
            t.timer_id == c.timer_id
                && t.generation == c.generation
                && t.status == TimerStatus::Scheduled
        }) else {
            return unchanged(Reply::TimerStale);
        };
        if timer.due_unix_ns > now {
            return unchanged(Reply::TimerNotDue);
        }
        if head.inbox.len() as u64 >= self.limits.max_mailbox_depth {
            return Err(fail(head, ErrorKind::MailboxFull, "mailbox is full"));
        }
        let mut new = head.clone();
        let accept_seq = new.next_accept_seq;
        new.next_accept_seq += 1;
        new.inbox.push(InboxEntry {
            accept_seq,
            handler: timer.handler.clone(),
            source: InputSource::Timer {
                timer_id: timer.timer_id,
                generation: timer.generation,
            },
            payload: timer.payload,
            accepted_unix_ns: now,
            failed_attempts: 0,
            last_failed_turn_id: None,
        });
        if let Some(t) = new.timers.iter_mut().find(|t| t.timer_id == c.timer_id) {
            t.status = TimerStatus::Delivered;
        }
        self.publish(
            Some(head),
            new,
            vec![Effect::WakeWorker],
            Reply::TimerFired { accept_seq },
        )
    }

    fn cancel_timer(&self, head: &Head, c: &CancelTimer) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        let id = identity::timer_id(head.incarnation, &c.timer_key);
        let Some(timer) = head.timers.iter().find(|t| t.timer_id == id) else {
            return unchanged(Reply::UnknownTimer);
        };
        match timer.status {
            TimerStatus::Delivered => unchanged(Reply::AlreadyFired),
            TimerStatus::Cancelled => unchanged(Reply::TimerCancelled),
            TimerStatus::Scheduled => {
                let mut new = head.clone();
                if let Some(t) = new.timers.iter_mut().find(|t| t.timer_id == id) {
                    t.status = TimerStatus::Cancelled;
                }
                self.publish(Some(head), new, Vec::new(), Reply::TimerCancelled)
            }
        }
    }

    fn tombstone(&self, head: &Head, c: &Tombstone) -> Outcome {
        if head.lifecycle == Lifecycle::Tombstoned && c.fence.incarnation == head.incarnation {
            return unchanged(Reply::AlreadyTombstoned);
        }
        Self::check_incarnation(head, &c.fence)?;
        let mut new = head.clone();
        new.lifecycle = Lifecycle::Tombstoned;
        self.publish(Some(head), new, Vec::new(), Reply::Tombstoned)
    }

    fn block_turn(&self, head: &Head, reason: &'static str) -> Outcome {
        let mut new = head.clone();
        if let Some(turn) = new.active_turn.as_mut() {
            turn.block = TurnBlock::NonDeterministicReplay;
        }
        let error = fail(head, ErrorKind::NonDeterministicReplay, reason);
        self.publish(Some(head), new, Vec::new(), Reply::Blocked(error))
    }

    fn record_step(&self, head: &Head, c: &RecordStep) -> Outcome {
        let turn = Self::require_running(head, &c.fence)?;
        let step_id = identity::step_id(head.incarnation, turn.input_accept_seq, &c.step_key);
        if let Some(existing) = head.steps.iter().find(|s| s.step_id == step_id) {
            if existing.tool_name != c.tool_name
                || existing.tool_version != c.tool_version
                || existing.input_hash != c.input_hash
            {
                return self.block_turn(head, "step key reused with a different tool or input");
            }
            if existing.status == StepStatus::Completed || c.status == StepStatus::Started {
                return unchanged(Reply::StepReplayed(existing.clone()));
            }
        }
        let record = StepRecord {
            step_id,
            step_key: c.step_key.clone(),
            tool_name: c.tool_name.clone(),
            tool_version: c.tool_version.clone(),
            input_hash: c.input_hash,
            status: c.status,
            output: c.output,
        };
        let mut new = head.clone();
        match new.steps.iter_mut().find(|s| s.step_id == step_id) {
            Some(slot) => *slot = record.clone(),
            None => new.steps.push(record.clone()),
        }
        self.publish(Some(head), new, Vec::new(), Reply::StepRecorded(record))
    }

    fn commit_intent(&self, head: &Head, c: &CommitIntent) -> Outcome {
        let turn = Self::require_running(head, &c.fence)?;
        let effect_id = identity::effect_id(
            head.incarnation,
            turn.input_accept_seq,
            &c.intent.intent_key,
        );
        if let Some(existing) = head.effects.iter().find(|e| e.effect_id == effect_id) {
            if !same_intent(existing, &c.intent) {
                return self.block_turn(head, "intent key reused with a different tool or input");
            }
            return unchanged(Reply::IntentCommitted { effect_id });
        }
        let mut new = head.clone();
        add_intent(
            head,
            &mut new,
            turn.input_accept_seq,
            turn.turn_id,
            &c.intent,
        )?;
        self.publish(
            Some(head),
            new,
            Vec::new(),
            Reply::IntentCommitted { effect_id },
        )
    }

    /// An `Owner` authorization that changes the effect needs the active turn
    /// to be running the effect's own input: a late authorization after the
    /// input's turn ended (a retryable `FailTurn`, a completion) is refused
    /// `OwnershipLost`
    /// (`tests::a_late_owner_authorization_after_a_retryable_failure_is_refused`).
    /// While cancellation is requested an owner may not authorize a new
    /// attempt; a manual effect's in-doubt latch is still recorded, since it
    /// authorizes nothing
    /// (`tests::a_new_attempt_for_a_cancelled_input_is_refused`). A
    /// `Reconcile` authorization is an operator's and has neither check; each
    /// one that publishes appends an [`AUDIT_RECONCILE_ATTEMPT`] event, and a
    /// repeat of it answers without a publish, so it is audited once
    /// (`tests::reconciliation_is_audited_once_in_the_event_stream`).
    fn authorize_attempt(&self, head: &Head, c: &AuthorizeAttempt) -> Outcome {
        let owner = match &c.authority {
            Authority::Owner(fence) => {
                Self::check_owner(head, fence)?;
                true
            }
            Authority::Reconcile(fence) => {
                Self::check_incarnation(head, fence)?;
                false
            }
        };
        let Some(effect) = head.effects.iter().find(|e| e.effect_id == c.effect_id) else {
            return Err(fail(head, ErrorKind::AgentGone, "effect is not journaled"));
        };
        if effect.status == EffectStatus::Completed {
            return unchanged(Reply::EffectCompleted {
                effect_id: c.effect_id,
                result: effect.result,
                attempt: effect.result_attempt,
            });
        }
        if c.attempt == effect.attempt && effect.status == EffectStatus::Authorized {
            return unchanged(Reply::AttemptAuthorized {
                effect_id: c.effect_id,
                attempt: effect.attempt,
            });
        }
        if Some(c.attempt) != effect.attempt.checked_add(1) {
            return Err(fail(
                head,
                ErrorKind::OwnershipLost,
                "attempt does not follow the journaled attempt",
            ));
        }
        if owner {
            let Some(turn) = head.active_turn.as_ref().filter(|t| {
                identity::effect_id(head.incarnation, t.input_accept_seq, &effect.step_key)
                    == effect.effect_id
            }) else {
                return Err(fail(
                    head,
                    ErrorKind::OwnershipLost,
                    "EffectStateAdmits: the effect's input is not the active turn's",
                ));
            };
            let latch = effect.status == EffectStatus::Authorized
                && effect.policy == RecoveryPolicy::Manual;
            if turn.cancellation == Cancellation::Requested && !latch {
                return Err(fail(
                    head,
                    ErrorKind::OwnershipLost,
                    "EffectStateAdmits: the effect's input is being cancelled",
                ));
            }
        }
        let mut new = head.clone();
        let Some(slot) = new.effects.iter_mut().find(|e| e.effect_id == c.effect_id) else {
            return Err(fail(head, ErrorKind::AgentGone, "effect is not journaled"));
        };
        match (effect.status, owner) {
            (EffectStatus::InDoubt, true) => {
                return Err(fail(
                    head,
                    ErrorKind::EffectInDoubt,
                    "effect awaits reconciliation",
                ));
            }
            (EffectStatus::Authorized, true) if effect.policy == RecoveryPolicy::Manual => {
                slot.status = EffectStatus::InDoubt;
                let error = fail(
                    head,
                    ErrorKind::EffectInDoubt,
                    "a manual effect's attempt is ambiguous",
                );
                return self.publish(Some(head), new, Vec::new(), Reply::Blocked(error));
            }
            _ => {
                slot.status = EffectStatus::Authorized;
                slot.attempt = c.attempt;
            }
        }
        if !owner {
            push_audit(&mut new, effect, AUDIT_RECONCILE_ATTEMPT);
        }
        self.publish(
            Some(head),
            new,
            Vec::new(),
            Reply::AttemptAuthorized {
                effect_id: c.effect_id,
                attempt: c.attempt,
            },
        )
    }

    /// Only the journaled attempt's result settles the effect. A result for an
    /// earlier attempt (superseded by a retry authorization), a different
    /// attempt's result after the effect completed, and any result for an
    /// effect whose input ended cancelled are answered `LateResultIgnored`
    /// without a publish: the caller keeps them as evidence
    /// (`tests::superseded_and_cancelled_results_are_evidence`). A result that
    /// settles an `InDoubt` effect is a reconciliation whoever supplied it and
    /// appends one [`AUDIT_RECONCILE_RESULT`] event; its replay answers without
    /// a publish
    /// (`tests::reconciliation_is_audited_once_in_the_event_stream`).
    fn record_effect_result(&self, head: &Head, c: &RecordEffectResult) -> Outcome {
        Self::check_incarnation(head, &c.fence)?;
        let Some(effect) = head.effects.iter().find(|e| e.effect_id == c.effect_id) else {
            return Err(fail(head, ErrorKind::AgentGone, "effect is not journaled"));
        };
        if c.attempt == 0 || c.attempt > effect.attempt {
            return Err(fail(
                head,
                ErrorKind::OwnershipLost,
                "attempt was never authorized",
            ));
        }
        if effect.status == EffectStatus::Completed {
            if effect.result_attempt != c.attempt {
                return unchanged(Reply::LateResultIgnored {
                    effect_id: c.effect_id,
                });
            }
            if effect.result.map(|r| r.hash) != Some(c.result.hash) {
                return Err(fail(
                    head,
                    ErrorKind::IdempotencyConflict,
                    "attempt already has a different result",
                ));
            }
            return unchanged(Reply::EffectResultRecorded {
                effect_id: c.effect_id,
            });
        }
        let input_cancelled = head.results.iter().any(|r| {
            r.status == ResultStatus::Cancelled
                && identity::effect_id(head.incarnation, r.accept_seq, &effect.step_key)
                    == effect.effect_id
        });
        if c.attempt < effect.attempt || input_cancelled {
            return unchanged(Reply::LateResultIgnored {
                effect_id: c.effect_id,
            });
        }
        let mut new = head.clone();
        if effect.status == EffectStatus::InDoubt {
            push_audit(&mut new, effect, AUDIT_RECONCILE_RESULT);
        }
        if let Some(slot) = new.effects.iter_mut().find(|e| e.effect_id == c.effect_id) {
            slot.status = EffectStatus::Completed;
            slot.result = Some(c.result);
            slot.result_attempt = c.attempt;
        }
        new.outbox.retain(|o| o.effect_id != c.effect_id);
        self.publish(
            Some(head),
            new,
            vec![Effect::WakeWorker],
            Reply::EffectResultRecorded {
                effect_id: c.effect_id,
            },
        )
    }
}

fn schedule(new: &mut Head, spec: &TimerSpec) -> (Id128, u64) {
    let timer_id = identity::timer_id(new.incarnation, &spec.timer_key);
    new.timer_generation += 1;
    let generation = new.timer_generation;
    let entry = TimerEntry {
        timer_id,
        timer_key: spec.timer_key.clone(),
        generation,
        due_unix_ns: spec.due_unix_ns,
        handler: spec.handler.clone(),
        payload: spec.payload,
        status: TimerStatus::Scheduled,
    };
    match new.timers.iter_mut().find(|t| t.timer_id == timer_id) {
        Some(slot) => *slot = entry,
        None => new.timers.push(entry),
    }
    (timer_id, generation)
}

fn same_intent(existing: &EffectRecord, intent: &IntentSpec) -> bool {
    existing.tool_name == intent.tool_name
        && existing.tool_version == intent.tool_version
        && existing.input_hash == intent.input_hash
        && existing.policy == intent.policy
}

/// Append an audit event for `effect` to the stream of the turn that
/// journaled it, under the effect's input.
fn push_audit(new: &mut Head, effect: &EffectRecord, event_type: &str) {
    let incarnation = new.incarnation;
    let accept_seq = new
        .active_turn
        .iter()
        .map(|t| t.input_accept_seq)
        .chain(new.inbox.iter().map(|e| e.accept_seq))
        .chain(new.results.iter().map(|r| r.accept_seq))
        .find(|&seq| identity::effect_id(incarnation, seq, &effect.step_key) == effect.effect_id)
        .unwrap_or(0);
    new.events.push(EventEntry {
        event_seq: new.next_event_seq,
        accept_seq,
        turn_id: effect.turn_id,
        event_type: event_type.to_string(),
        payload: None,
    });
    new.next_event_seq += 1;
}

/// The effects journaled for the input `accept_seq`, by any of its turns.
fn input_effects(head: &Head, accept_seq: u64) -> impl Iterator<Item = &EffectRecord> {
    head.effects.iter().filter(move |e| {
        e.effect_id == identity::effect_id(head.incarnation, accept_seq, &e.step_key)
    })
}

/// Drop the step transcript of the input `accept_seq`, carried over from a
/// retryable failure.
fn drop_input_steps(new: &mut Head, accept_seq: u64) {
    let incarnation = new.incarnation;
    new.steps
        .retain(|s| s.step_id != identity::step_id(incarnation, accept_seq, &s.step_key));
}

/// Journal an intent. `Ok(None)` when the same intent is already journaled.
fn add_intent(
    head: &Head,
    new: &mut Head,
    accept_seq: u64,
    turn_id: Id128,
    intent: &IntentSpec,
) -> Result<Option<Id128>, AgentError> {
    let effect_id = identity::effect_id(new.incarnation, accept_seq, &intent.intent_key);
    if let Some(existing) = new.effects.iter().find(|e| e.effect_id == effect_id) {
        if same_intent(existing, intent) {
            return Ok(None);
        }
        return Err(fail(
            head,
            ErrorKind::NonDeterministicReplay,
            "intent key reused with a different tool or input",
        ));
    }
    new.effects.push(EffectRecord {
        effect_id,
        step_key: intent.intent_key.clone(),
        tool_name: intent.tool_name.clone(),
        tool_version: intent.tool_version.clone(),
        input_hash: intent.input_hash,
        policy: intent.policy,
        status: EffectStatus::Intent,
        attempt: 0,
        result: None,
        result_attempt: 0,
        turn_id,
    });
    Ok(Some(effect_id))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests;
