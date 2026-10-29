//! Drives the production store and runtime through the scheduler and turns
//! what they answered into a [`History`] for the reference machine.
//!
//! A [`Rig`] owns one in-memory store for one agent, layered as the
//! scheduler expects:
//!
//! ```text
//! actor gate FaultStore -> rules FaultStore -> StampStore -> MemoryStore
//! ```
//!
//! The scheduler gives each actor its own gate layer. Fault rules live in
//! the one shared rules layer, so [`Rig::rules`] counts every fault that
//! fired, whichever actor's call it hit. [`StampStore`] numbers every
//! successful HEAD read and write with one global sequence.
//!
//! Each actor is a [`Proc`]: one [`AgentStore`] and one [`Coordinator`] over
//! that actor's store, standing for one process. Its methods call the
//! production API and record the call with the [`Recorder`].
//!
//! # Linearization
//!
//! A recorded call takes the sequence number of the last HEAD write it
//! applied, else of the last HEAD read it made, and [`Recorder::build`]
//! orders calls by that number. A write is the point its effect became
//! visible, and a call that wrote nothing answered from the last root it
//! read. One exception: a step that journaled its start and then answered
//! `OwnershipLost` decided it on a later read, the cancel check before an
//! authorization, so it takes that read's number when it follows the write
//! (`schedules::effects::cancels_across_seeds`, where the cancel lands
//! between the two). A step refused with any other error keeps the rule:
//! a manual latch in doubt and a replay block are decided by their own
//! write, and the reload after the refusal is not that decision. The
//! numbers reach the call through a task-local cell
//! ([`tokio::task_local!`]); nothing in the store or the runtime spawns, so
//! the bottom of the stack runs inside the recording task.
//!
//! # Mapping production answers to [`Reply`]
//!
//! Each choice below names the test that exercises it, or says that no
//! schedule reaches it yet.
//!
//! - `Coordinator::create` answers [`Reply::Created`] with its incarnation
//!   and `created` flag (every schedule's setup).
//! - `Coordinator::send` answers [`Reply::Accepted`] with the handle decoded
//!   from `operation` and the `duplicate` flag. The event's payload is
//!   `serde_json::to_vec` of the request's JSON, the bytes production
//!   uploads (`schedules::senders::same_request_twice_is_a_duplicate`). An
//!   `accept_seq` that disagrees with the handle is unmappable; no schedule
//!   reaches this.
//! - A send whose HEAD write applied and whose acknowledgement was lost
//!   answers through the coordinator's settle; it is recorded at the write
//!   it applied, not at the settle's reads
//!   (`schedules::senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`).
//! - A worker poll that returns an assignment is [`Event::TurnStarted`] on
//!   the assignment's operation, answered [`Reply::TurnStarted`]. A poll that
//!   returns nothing is not an event and is not recorded
//!   (`schedules::workers::worker_killed_before_begin_turn_cas`).
//! - `Coordinator::commit` of a completion is [`Event::TurnCompleted`] with
//!   the output's `serde_json::to_vec` bytes, answered [`Reply::Committed`]
//!   with the response's status and state version
//!   (`schedules::races::cancel_racing_completion_one_wins`, where the status
//!   is `cancelled`). A response with effects is unmappable: the reference
//!   has no intents. No schedule reaches this.
//! - `Coordinator::cancel` answers [`Reply::Cancel`] with its status and
//!   `requested` flag (`schedules::races::cancel_racing_completion_one_wins`).
//! - [`Proc::operation`] reads through `AgentStore::resolve` and answers
//!   [`Reply::Status`] (`schedules::senders::competing_senders_take_distinct_accept_seqs`),
//!   or [`Reply::UnknownOperation`] when the root has no input under the
//!   handle (`schedules::effects::query_of_a_handle_with_no_input`, which
//!   also reaches the `UnknownOperation` answer of [`Proc::result`] and the
//!   `Resolved(None)` answer of [`Proc::resolve`]).
//! - [`Proc::result`] and [`Proc::resolve`] are the driver's own reads of
//!   HEAD: the `Coordinator` has no result or resolve call.
//! - [`Proc::result`] reads HEAD once and answers [`Reply::Result`]: `None`
//!   while the input is not terminal, else its status, the output object's
//!   bytes, and the error string with `""` read as no error
//!   (`schedules::senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`).
//! - [`Proc::resolve`] reads HEAD once and answers [`Reply::Resolved`] with
//!   the receipt matching both handler and request id
//!   (`schedules::senders::same_id_different_payload_conflicts`). A receipt
//!   whose input has no state is unmappable; no schedule reaches this.
//! - `Coordinator::record_step` is [`Event::StepStarted`], answered
//!   [`Reply::StepJournaled`] with the step's completed flag and its
//!   effect's phase and attempt
//!   (`schedules::effects::journaled_step_with_a_failed_attempt_replays_the_failure`).
//!   An effect showing an intent at attempt 0 is no effect yet, as
//!   `crashed_step` reads it; an intent past attempt 0 is unmappable, and no
//!   schedule reaches this.
//! - `Coordinator::run_step` is [`Event::Step`], answered
//!   [`Reply::StepDispatched`] with the attempt it handed the dispatcher,
//!   [`Reply::StepCompleted`] with the output's bytes, or, for
//!   `StepProgress::Failed`, [`Reply::StepFailed`] with the error
//!   (`schedules::effects::at_least_once_killed_after_execution`, where the
//!   second dispatch is attempt 2;
//!   `schedules::effects::journaled_step_with_a_failed_attempt_replays_the_failure`
//!   for the failure).
//! - `Coordinator::complete_effect` is [`Event::EffectResult`] with the
//!   output's bytes, and `fail_effect` is [`Event::EffectFailed`] with the
//!   error: production stores a failure as a typed result object, which
//!   the reference keeps as a failure rather than as bytes. Both answer
//!   [`Reply::EffectRecorded`] or [`Reply::EffectEvidence`]
//!   (`schedules::effects::at_least_once_late_result_after_takeover_is_evidence`,
//!   `schedules::effects::journaled_step_with_a_failed_attempt_replays_the_failure`).
//! - `Coordinator::reconcile_attach` is [`Event::ReconcileAttach`], answered
//!   like a result, and `reconcile_authorize` is
//!   [`Event::ReconcileAuthorize`], answered [`Reply::Reconciled`] with its
//!   `handed_over` flag (`schedules::effects::manual_killed_after_execution`,
//!   `schedules::effects::manual_killed_before_tool_call`). The
//!   authorization dispatches nothing: the coordinator that ran it holds the
//!   attempt, and the next `run_step` of the effect's step through that
//!   coordinator dispatches it and answers [`Reply::StepDispatched`] with
//!   it. So the schedules authorize from the process that runs the next
//!   step; a step through another process would find the attempt lost,
//!   which the reference does not model.
//! - The error kinds the reference models, `OwnershipLost`, `AgentGone`,
//!   `IdempotencyConflict`, `EffectInDoubt` and `NonDeterministicReplay`,
//!   answer [`Reply::Error`]
//!   (`schedules::workers::suspended_worker_commit_is_refused_after_takeover`,
//!   `schedules::senders::same_id_different_payload_conflicts`,
//!   `schedules::effects::manual_killed_before_tool_call`). No schedule
//!   changes a step's call between runs, so `NonDeterministicReplay` is
//!   reached only by the reference's own
//!   `a_changed_step_call_blocks_the_agent`.
//! - The kinds it does not model (`StoreUnavailable`, `MailboxFull`,
//!   `BudgetExceeded`, `UnsupportedFormat`, `IncompatibleHandler`, and any
//!   other) are not fed to the reference. They are collected as
//!   [`Refusal`]s, which each schedule asserts exactly
//!   (`schedules::errors`).
//! - `OutcomeUnknown` reaching the caller is unmappable: the coordinator
//!   settles it on every path, so a client never has to. No schedule
//!   reaches this.
//! - `Coordinator::schedule_timer` is a client `SetTimer` with the bytes of
//!   a null payload, answered [`Reply::TimerSet`] with the generation and
//!   the timer handle whose id the response names, found by trying
//!   incarnations 1 to `MAX_INCARNATION` under the timer key
//!   (`schedules::timers::a_cancel_before_the_fire_wins_and_the_sweep_fires_nothing`).
//!   An id matching no incarnation is unmappable; no schedule reaches this.
//! - `Coordinator::cancel_timer` answers [`Reply::TimerCancel`] with its
//!   outcome (`Cancelled` in
//!   `schedules::timers::a_cancel_before_the_fire_wins_and_the_sweep_fires_nothing`,
//!   `AlreadyFired` in
//!   `schedules::timers::a_cancel_after_the_fire_is_already_fired`).
//! - The wake hint a schedule writes after its HEAD write is not an event:
//!   the reference has no hints, and a lost or missing hint only changes
//!   which half of a sweep pass reaches the timer
//!   (`schedules::timers::a_lost_hint_put_is_delivered_by_the_sweep_within_its_interval`,
//!   `schedules::timers::a_crash_between_the_root_write_and_the_hint_write_still_fires`).
//! - A sweep pass ([`Proc::sweep`]) is one [`Event::TimerDue`] per HEAD
//!   write it applied, answered [`Reply::TimerFired`] with the input that
//!   write enqueued
//!   (`schedules::timers::two_sweepers_on_one_hint_fire_it_once`), then an
//!   [`Event::Wake`] at its last HEAD access, from `Hint` when it read a
//!   hint and from `Sweep` otherwise, answered with nothing left to fire
//!   (`schedules::timers::a_premature_hint_fires_nothing_and_is_kept`,
//!   `schedules::timers::a_lost_hint_put_due_on_a_pass_fires_on_that_pass`).
//!   A write that does not decode, or whose newest input no timer enqueued,
//!   is unmappable; no schedule reaches this.
//! - A pass with a failed HEAD write is a [`Refusal`] of `StoreUnavailable`
//!   in place of its wake
//!   (`schedules::timers::a_sweep_whose_fire_write_fails_is_a_store_unavailable_refusal`).
//!   A crashed pass keeps only its writes; no schedule crashes a pass.
//!   A pass answered with an error the history cannot take
//!   (`OutcomeUnknown`, or a modelled error such as `AgentGone`) is
//!   unmappable
//!   (`tests::a_sweep_answered_with_an_error_the_history_cannot_take_is_unmappable`);
//!   no schedule reaches this.
//!
//! # Calls that never answered
//!
//! An actor crashed while a call is in flight leaves no answer. When that
//! call applied a HEAD write, its effect is real, so the call is kept and
//! its reply is the reference's own ("inferred"). The event of a send,
//! commit or cancel is known before the call; a poll's is read from the
//! root it wrote: `TurnStarted` on the active turn's input when the root has
//! one, nothing otherwise (`schedules::workers::worker_killed_after_begin_turn_cas`).
//! The same holds for a call that applied a write and then answered a
//! refusal (`schedules::errors::corrupt_dependency_is_store_unavailable`).
//! A crashed call that wrote nothing changed nothing and is dropped.
//!
//! An inferred reply that is an error means the reference refuses a call
//! whose write applied, so that call is listed in [`Run::unmappable`]
//! (`tests::a_crashed_write_the_reference_refuses_outside_the_exemption_is_unmappable`,
//! a completion with no started turn answered `OwnershipLost`). Three
//! shapes are exempt, because production writes before it answers them:
//! a manual step whose attempt was lost latches its effect in doubt and
//! answers `EffectInDoubt`
//! (`tests::a_crashed_manual_step_whose_attempt_was_lost_infers_effect_in_doubt`);
//! a step whose call changed blocks the turn and answers
//! `NonDeterministicReplay` (no test builds that shape); and a step that
//! journaled its start, with no attempt, after a cancel and died before
//! the cancel check would have answered `OwnershipLost`, the crashed form
//! of the exception under Linearization
//! (`tests::a_step_crashed_after_journaling_past_a_cancel_infers_ownership_lost`,
//! reached by
//! `schedules::effects::a_worker_dead_between_its_step_journal_and_its_cancel_check_is_fenced`).
//! `OwnershipLost` inferred for any other crashed write stays unmappable,
//! including a step whose root holds no journaled start
//! (`tests::a_step_crashed_past_a_cancel_with_no_journaled_start_is_unmappable`).
//!
//! Anything else the driver cannot map is listed in [`Run::unmappable`],
//! and every schedule asserts that list is empty.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::{EffectStatus, Head, InputSource, StepStatus};
use ravel_agents_core::identity::{AgentId, agent_id, effect_id, step_input_hash, timer_id};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::{AgentKey, AgentKind, head_key, object_key};
use ravel_agents_core::pages::attach_page;
use ravel_agents_core::state::{Handle, HandleItem, operation_state};
use ravel_agents_core::wire::{
    AgentRef, CancelRequest, CancelResponse, CancelTimerRequest, CancelTimerResponse,
    CreateAgentRequest, CreateAgentResponse, HandlerPinBody, OperationStatus, SendRequest,
    SendResponse, TimerRequest, TimerResponse, TurnAssignment, TurnOutcome, WorkerCommitRequest,
    WorkerCommitResponse, WorkerPollRequest,
};
use ravel_agents_runtime::{
    Coordinator, DEFAULT_POLL_WAIT_CAP_MS, Dispatch, EffectDispatcher, EffectRef, EffectSettled,
    Rng, RuntimeConfig, StepJournal, StepProgress, StepSpec, SweepConfig, SweepReport,
};
use ravel_agents_store::backoff::NoBackoff;
use ravel_agents_store::{AgentStore, Clock, HeadState, StoreLimits};
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_types::TenantHash;
use serde_json::Value;

use crate::history::History;
use crate::reference::{
    ClientOp, EffectPhase, EffectView, Event, Outcome, RecoveryPolicy, ReferenceMachine, Reply,
    StepCall, WakeSource,
};

/// The tenant every rig's store serves.
pub const TENANT: TenantHash = TenantHash([7; 16]);
/// The simulated agent's kind and key.
pub const KIND: &str = "support";
pub const KEY: &str = "ticket-7";
/// The one handler every input names.
pub const HANDLER: &str = "reply";
/// The handler version every worker pins.
pub const VERSION: &str = "v1";
/// Start of simulated time, in unix nanoseconds.
pub const T0_NS: i64 = 1_800_000_000_000_000_000;
pub const LEASE_MS: u64 = 1_000;
pub const GRACE_MS: u64 = 500;

/// Milliseconds a clock advance needs to put any lease past its grace.
pub const PAST_LEASE_MS: u64 = LEASE_MS + GRACE_MS + 1;

/// The simulated agent's id.
pub fn agent() -> AgentId {
    match (AgentKind::new(KIND), AgentKey::new(KEY)) {
        (Ok(kind), Ok(key)) => agent_id(&kind, &key),
        // Both names are constants the reference machine also parses; the
        // reference's own tests fail first if either stops parsing.
        _ => AgentId([0; 16]),
    }
}

/// A fresh reference machine for the simulated agent.
pub fn machine() -> Result<ReferenceMachine, String> {
    ReferenceMachine::new(KIND, KEY).map_err(|e| format!("reference machine: {e:?}"))
}

/// The HEAD key of the simulated agent.
/// Environment variable that sets every rig's index page size, so the
/// suite runs a second time with lists sealing every few entries.
pub const PAGE_ENTRIES_ENV: &str = "RAVEL_AGENTS_PAGE_ENTRIES";

/// The store limits a rig uses unless a schedule sets its own: the default,
/// with `max_page_entries` from [`PAGE_ENTRIES_ENV`] when it is set.
///
/// # Panics
///
/// When the variable is set to something other than a positive integer, so
/// a mistyped run does not pass at the default size.
#[allow(clippy::expect_used)]
pub fn sim_limits() -> StoreLimits {
    let mut limits = StoreLimits::default();
    if let Ok(text) = std::env::var(PAGE_ENTRIES_ENV) {
        let entries: usize = text
            .parse()
            .ok()
            .filter(|&n| n > 0)
            .expect("RAVEL_AGENTS_PAGE_ENTRIES is a positive integer");
        limits.kernel.max_page_entries = entries;
    }
    limits
}

/// The page size [`sim_limits`] applies.
pub fn page_entries() -> usize {
    sim_limits().kernel.max_page_entries
}

pub fn head_object_key() -> Result<String, String> {
    head_key(&TENANT, &agent()).map_err(|e| format!("head key: {e:?}"))
}

pub fn agent_ref() -> AgentRef {
    AgentRef {
        kind: KIND.to_string(),
        key: KEY.to_string(),
    }
}

/// The handler pin every worker presents.
pub fn pin() -> HandlerPinBody {
    HandlerPinBody {
        handler: HANDLER.to_string(),
        version: VERSION.to_string(),
        digest: hex::encode([1u8; 32]),
    }
}

/// A manual clock shared by every process of a rig.
#[derive(Debug)]
pub struct SimClock(AtomicI64);

impl SimClock {
    pub fn advance_ms(&self, ms: u64) {
        let ns = i64::try_from(ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        self.0.fetch_add(ns, Ordering::SeqCst);
    }
}

impl Clock for SimClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Counts up from its seed, so worker ids are fixed per coordinator.
#[derive(Debug)]
pub struct CountingRng(AtomicU64);

impl Rng for CountingRng {
    fn next_u64(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

/// What one recorded call did to HEAD.
#[derive(Debug, Default, Clone)]
struct Stamps {
    /// Sequence number and body of the last HEAD write it applied.
    write: Option<(u64, Bytes)>,
    /// Every HEAD write it applied, in order.
    writes: Vec<(u64, Bytes)>,
    /// Sequence number of the last HEAD read it made.
    read: Option<u64>,
    /// HEAD writes its process saw fail with anything but a lost race,
    /// counted at the process's own store boundary, above every fault.
    failed_puts: u64,
}

type Cell = Arc<Mutex<Stamps>>;

tokio::task_local! {
    static CALL: Cell;
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Numbers every successful HEAD read and write. See the module docs.
pub struct StampStore {
    inner: Arc<dyn ObjectStoreBackend>,
    seq: AtomicU64,
    head_puts: AtomicU64,
    /// A process's boundary layer: it stamps nothing and only counts the
    /// HEAD writes that failed, as the process saw them.
    boundary: bool,
}

impl StampStore {
    pub fn new(inner: Arc<dyn ObjectStoreBackend>) -> StampStore {
        StampStore {
            inner,
            seq: AtomicU64::new(0),
            head_puts: AtomicU64::new(0),
            boundary: false,
        }
    }

    /// The layer [`Rig::proc`] puts directly under a process's store.
    fn boundary(inner: Arc<dyn ObjectStoreBackend>) -> StampStore {
        StampStore {
            boundary: true,
            ..StampStore::new(inner)
        }
    }

    /// HEAD writes that reached the store, applied or refused.
    pub fn head_puts(&self) -> u64 {
        self.head_puts.load(Ordering::SeqCst)
    }

    fn stamp(&self, f: impl FnOnce(&mut Stamps, u64)) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        // Calls made outside a recorded call (rig setup, raw reads) carry
        // no cell and are not part of any history.
        let _ = CALL.try_with(|cell| f(&mut lock(cell), seq));
    }
}

fn is_head(key: &str) -> bool {
    key.ends_with("/head")
}

#[async_trait]
impl ObjectStoreBackend for StampStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let head = is_head(key);
        if self.boundary {
            let out = self.inner.put(key, data, opts).await;
            if head
                && let Err(e) = &out
                && !matches!(
                    e,
                    StoreError::PreconditionFailed | StoreError::AlreadyExists
                )
            {
                let _ = CALL.try_with(|cell| lock(cell).failed_puts += 1);
            }
            return out;
        }
        if head {
            self.head_puts.fetch_add(1, Ordering::SeqCst);
        }
        let body = data.clone();
        let out = self.inner.put(key, data, opts).await;
        if head && out.is_ok() {
            self.stamp(|s, seq| {
                s.writes.push((seq, body.clone()));
                s.write = Some((seq, body));
            });
        }
        out
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let out = self.inner.get(key, range).await;
        if !self.boundary && is_head(key) && out.is_ok() {
            self.stamp(|s, seq| s.read = Some(seq));
        }
        out
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

/// How a recorded call was answered, as the history sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    /// A reply the reference machine models.
    Modelled(Reply),
    /// An error kind the reference does not model.
    Refused(ErrorKind),
    /// Something the reference has no variant for.
    Unmappable(String),
    /// Not an event: a poll that found nothing.
    Skip,
    /// A sweep pass that completed. Its events are read from what it wrote;
    /// see [`Recorder::build`].
    Swept,
}

/// A recorded call answered with an error the reference does not model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub actor: String,
    pub call: &'static str,
    pub kind: ErrorKind,
}

#[derive(Debug)]
struct Entry {
    actor: String,
    call: &'static str,
    /// `None` for a poll until it answers.
    event: Option<Event>,
    cell: Cell,
    observed: Option<Observed>,
}

/// Every call the processes of one rig made, in the order they began.
#[derive(Debug, Default)]
pub struct Recorder {
    entries: Mutex<Vec<Entry>>,
}

/// A recorded history with what was left out of it.
#[derive(Debug, Clone)]
pub struct Run {
    pub history: History,
    /// `(actor, event)` of each call whose reply is the reference's own
    /// because the call applied a write and never answered a modelled
    /// reply.
    pub inferred: Vec<(String, Event)>,
    /// Parallel to the history's entries: whether each call applied a HEAD
    /// write.
    pub applied: Vec<bool>,
    pub refusals: Vec<Refusal>,
    pub unmappable: Vec<String>,
}

impl Run {
    /// Check the history against a fresh reference machine.
    pub fn check(&self) -> Result<(), String> {
        let machine = machine()?;
        self.history.check(machine).map_err(|d| d.to_string())
    }
}

impl Recorder {
    fn begin(&self, actor: &str, call: &'static str, event: Option<Event>) -> (usize, Cell) {
        let cell = Cell::default();
        let mut entries = lock(&self.entries);
        entries.push(Entry {
            actor: actor.to_string(),
            call,
            event,
            cell: Arc::clone(&cell),
            observed: None,
        });
        (entries.len() - 1, cell)
    }

    fn finish(&self, index: usize, event: Option<Event>, observed: Observed) {
        if let Some(entry) = lock(&self.entries).get_mut(index) {
            if event.is_some() {
                entry.event = event;
            }
            entry.observed = Some(observed);
        }
    }

    /// Order the calls by their stamps and build the history. See the
    /// module docs for what is kept, inferred and left out.
    pub fn build(&self) -> Result<Run, String> {
        let entries = lock(&self.entries);
        let mut unmappable = Vec::new();
        let mut refusals = Vec::new();
        let mut items: Vec<Item> = Vec::new();
        for (order, entry) in entries.iter().enumerate() {
            let stamps = lock(&entry.cell).clone();
            let written = stamps.write.as_ref().map(|(s, _)| *s);
            let refused_step = matches!(entry.event, Some(Event::Step { .. }))
                && matches!(
                    entry.observed,
                    Some(Observed::Modelled(Reply::Error(ErrorKind::OwnershipLost)))
                );
            let stamp = match (written, stamps.read) {
                (Some(w), Some(r)) if refused_step && r > w => Some(r),
                _ => written.or(stamps.read),
            };
            let applied = stamps.write.as_ref().map(|(_, body)| body.clone());
            if let Some(Observed::Refused(kind)) = &entry.observed {
                refusals.push(Refusal {
                    actor: entry.actor.clone(),
                    call: entry.call,
                    kind: *kind,
                });
            }
            if entry.call == SWEEP {
                sweep_items(
                    entry,
                    order,
                    &stamps,
                    &mut items,
                    &mut unmappable,
                    &mut refusals,
                );
                continue;
            }
            match (&entry.observed, applied, stamp) {
                (Some(Observed::Unmappable(what)), _, _) => {
                    unmappable.push(format!("{} {}: {what}", entry.actor, entry.call));
                }
                (Some(Observed::Skip | Observed::Swept), _, _) => {}
                (Some(Observed::Modelled(reply)), applied, Some(stamp)) => match &entry.event {
                    Some(event) => items.push(Item {
                        stamp,
                        order,
                        actor: entry.actor.clone(),
                        event: event.clone(),
                        observed: Some(reply.clone()),
                        applied,
                    }),
                    None => unmappable.push(format!(
                        "{} {}: answered {reply:?} with no event",
                        entry.actor, entry.call
                    )),
                },
                (Some(Observed::Modelled(reply)), _, None) => unmappable.push(format!(
                    "{} {}: answered {reply:?} without reading HEAD",
                    entry.actor, entry.call
                )),
                (Some(Observed::Refused(_)) | None, Some(body), Some(stamp)) => {
                    let event = match &entry.event {
                        Some(event) => Some(event.clone()),
                        None => match Head::decode(&body) {
                            Ok(head) => head.active_turn.map(|turn| Event::TurnStarted {
                                operation: Handle::operation(
                                    agent(),
                                    head.incarnation,
                                    turn.input_accept_seq,
                                ),
                            }),
                            Err(e) => {
                                unmappable.push(format!(
                                    "{} {}: wrote a root that does not decode: {e:?}",
                                    entry.actor, entry.call
                                ));
                                None
                            }
                        },
                    };
                    if let Some(event) = event {
                        items.push(Item {
                            stamp,
                            order,
                            actor: entry.actor.clone(),
                            event,
                            observed: None,
                            applied: Some(body),
                        });
                    }
                }
                (Some(Observed::Refused(_)) | None, _, _) => {}
            }
        }
        items.sort_by_key(|item| (item.stamp, item.order));
        let mut shadow = machine()?;
        let mut history = History::default();
        let mut inferred = Vec::new();
        let mut applied = Vec::new();
        for item in items {
            let mut start_only = false;
            let event = match (&item.observed, &item.event, &item.applied) {
                (None, Event::Step { operation, step }, Some(body)) => {
                    match crashed_step(&shadow, operation, step, body) {
                        Ok((event, journaled_start)) => {
                            start_only = journaled_start;
                            event
                        }
                        Err(what) => {
                            unmappable.push(format!("{} step: {what}", item.actor));
                            continue;
                        }
                    }
                }
                _ => item.event,
            };
            let expected = shadow.apply(&event);
            let reply = match item.observed {
                Some(reply) => reply,
                None => {
                    let exempt = match expected {
                        Reply::Error(
                            ErrorKind::EffectInDoubt | ErrorKind::NonDeterministicReplay,
                        ) => true,
                        Reply::Error(ErrorKind::OwnershipLost) => start_only,
                        _ => false,
                    };
                    if let Reply::Error(kind) = expected
                        && !exempt
                    {
                        unmappable.push(format!(
                            "{}: applied a write the reference refuses {kind:?}: {event:?}",
                            item.actor
                        ));
                    }
                    inferred.push((item.actor, event.clone()));
                    expected
                }
            };
            history.record(event, reply);
            applied.push(item.applied.is_some());
        }
        Ok(Run {
            history,
            inferred,
            applied,
            refusals,
            unmappable,
        })
    }
}

/// The name a sweep pass is recorded under.
const SWEEP: &str = "sweep";

/// Place a sweep pass in the history: one [`Event::TimerDue`] per HEAD
/// write it applied, answered with the input that write enqueued, then,
/// for a pass that completed with no failed HEAD write, an [`Event::Wake`]
/// at its last HEAD access answered with nothing left to fire. A pass with
/// a failed HEAD write is a [`Refusal`] of `StoreUnavailable` instead of
/// that wake. A crashed or refused pass keeps only its writes; a pass
/// answered with anything else is unmappable.
fn sweep_items(
    entry: &Entry,
    order: usize,
    stamps: &Stamps,
    items: &mut Vec<Item>,
    unmappable: &mut Vec<String>,
    refusals: &mut Vec<Refusal>,
) {
    let Some(Event::Wake { now_unix_ns, by }) = &entry.event else {
        unmappable.push(format!("{} sweep: recorded without its clock", entry.actor));
        return;
    };
    for (seq, body) in &stamps.writes {
        match fired_by(body, *now_unix_ns) {
            Ok((event, reply)) => items.push(Item {
                stamp: *seq,
                order,
                actor: entry.actor.clone(),
                event,
                observed: Some(reply),
                applied: Some(body.clone()),
            }),
            Err(what) => unmappable.push(format!("{} sweep: {what}", entry.actor)),
        }
    }
    match &entry.observed {
        Some(Observed::Swept) => {}
        Some(Observed::Refused(_)) | None => return,
        Some(Observed::Unmappable(what)) => {
            unmappable.push(format!("{} sweep: {what}", entry.actor));
            return;
        }
        Some(Observed::Modelled(reply)) => {
            unmappable.push(format!("{} sweep: answered {reply:?}", entry.actor));
            return;
        }
        Some(Observed::Skip) => {
            unmappable.push(format!("{} sweep: answered with no event", entry.actor));
            return;
        }
    }
    if stamps.failed_puts > 0 {
        refusals.push(Refusal {
            actor: entry.actor.clone(),
            call: entry.call,
            kind: ErrorKind::StoreUnavailable,
        });
        return;
    }
    let last = stamps.writes.last().map(|(seq, _)| *seq).max(stamps.read);
    if let Some(stamp) = last {
        items.push(Item {
            stamp,
            order,
            actor: entry.actor.clone(),
            event: Event::Wake {
                now_unix_ns: *now_unix_ns,
                by: *by,
            },
            observed: Some(Reply::Woken(Vec::new())),
            applied: None,
        });
    }
}

/// The fire a sweep's HEAD write `body` applied: the newest inbox entry,
/// which a `FireTimer` enqueues from a timer, and that timer's key.
fn fired_by(body: &Bytes, now_unix_ns: i64) -> Result<(Event, Reply), String> {
    let head = Head::decode(body).map_err(|e| format!("root does not decode: {e:?}"))?;
    let newest = head.next_accept_seq.saturating_sub(1);
    let Some(input) = head.inbox.iter().find(|e| e.accept_seq == newest) else {
        return Err(format!("wrote a root whose inbox lacks input {newest}"));
    };
    let InputSource::Timer {
        timer_id,
        generation,
    } = input.source
    else {
        return Err(format!("wrote input {newest} from {:?}", input.source));
    };
    let Some(timer) = head.timers.iter().find(|t| t.timer_id == timer_id) else {
        return Err(format!(
            "input {newest} names a timer the root does not hold"
        ));
    };
    Ok((
        Event::TimerDue {
            timer_key: timer.timer_key.clone(),
            generation,
            now_unix_ns,
        },
        Reply::TimerFired(Some(Handle::operation(agent(), head.incarnation, newest))),
    ))
}

/// One call placed in the history.
struct Item {
    stamp: u64,
    order: usize,
    actor: String,
    event: Event,
    /// The reply it answered, or `None` to take the reference's.
    observed: Option<Reply>,
    /// The body of the last HEAD write it applied.
    applied: Option<Bytes>,
}

/// The event a step that applied a write and never answered stands for.
/// `run_step` journals the step, then commits and authorizes an attempt,
/// each its own HEAD write, so a crash between them leaves the step
/// journaled with no attempt: [`Event::StepStarted`]. A root holding the
/// committed intent at attempt 0 is that shape, since the reference has no
/// intents; an intent at any other attempt is unmappable. When the root it
/// wrote holds what [`Event::Step`] would leave, the step ran through.
/// The flag is true when that root holds the step's journaled start alone,
/// no attempt and no completion: the only crashed step whose inferred
/// `OwnershipLost` maps.
fn crashed_step(
    shadow: &ReferenceMachine,
    operation: &Handle,
    step: &StepCall,
    body: &Bytes,
) -> Result<(Event, bool), String> {
    let head = Head::decode(body).map_err(|e| format!("root does not decode: {e:?}"))?;
    let HandleItem::Operation(seq) = operation.item else {
        return Err(format!("{operation:?} names no input"));
    };
    let id = effect_id(operation.incarnation, seq, &step.key);
    let written = match head.effects.iter().find(|e| e.effect_id == id) {
        None => None,
        Some(e) => match phase_of(e.status) {
            Some(phase) => Some(EffectView {
                phase,
                attempt: e.attempt,
            }),
            None if e.attempt == 0 => None,
            None => {
                return Err(format!(
                    "step journal shows an intent past attempt 0: {e:?}"
                ));
            }
        },
    };
    let completed = head
        .steps
        .iter()
        .any(|s| s.step_key == step.key && s.status == StepStatus::Completed);
    let journaled = head.steps.iter().any(|s| s.step_key == step.key);
    let start_only = journaled && written.is_none() && !completed;
    let mut ran = shadow.clone();
    let event = Event::Step {
        operation: *operation,
        step: step.clone(),
    };
    ran.apply(&event);
    if ran.effect_view(operation, &step.key) == written
        && ran.step_completed(&step.key) == completed
    {
        Ok((event, start_only))
    } else {
        Ok((
            Event::StepStarted {
                operation: *operation,
                step: step.clone(),
            },
            false,
        ))
    }
}

/// Map an error to what the history records.
pub fn map_error(kind: ErrorKind) -> Observed {
    match kind {
        ErrorKind::OwnershipLost
        | ErrorKind::AgentGone
        | ErrorKind::IdempotencyConflict
        | ErrorKind::EffectInDoubt
        | ErrorKind::NonDeterministicReplay => Observed::Modelled(Reply::Error(kind)),
        ErrorKind::OutcomeUnknown => {
            Observed::Unmappable("OutcomeUnknown reached the caller".to_string())
        }
        other => Observed::Refused(other),
    }
}

fn observe<T>(result: &Result<T, AgentError>, ok: impl FnOnce(&T) -> Observed) -> Observed {
    match result {
        Ok(value) => ok(value),
        Err(e) => map_error(e.kind),
    }
}

fn decode_operation(token: &str) -> Result<Handle, Observed> {
    Handle::decode(token).map_err(|e| Observed::Unmappable(format!("bad handle {token}: {e:?}")))
}

/// JSON bytes as production serializes a payload or an output.
pub fn json_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

/// The coordinator settings every process uses: short leases and an idle
/// unload of zero. With [`poll_sweep`], which [`Rig::proc`] sets, a poll
/// always sees work that arrived through another process, through one
/// directory LIST.
pub fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        max_turn_attempts: 3,
        idle_unload_after_ms: 0,
        poll_wait_cap_ms: DEFAULT_POLL_WAIT_CAP_MS,
    }
}

/// The sweep every [`Rig::proc`] runs: an interval of zero, so every idle
/// poll runs a pass, and one LIST a pass, which goes to the directory half.
pub fn poll_sweep() -> SweepConfig {
    SweepConfig {
        interval_ms: 0,
        max_list_pages: 1,
        ..SweepConfig::default()
    }
}

/// One store, one clock and one recorder for one simulated agent. Clones
/// share all of them.
#[derive(Clone)]
pub struct Rig {
    pub clock: Arc<SimClock>,
    pub memory: Arc<MemoryStore>,
    pub stamps: Arc<StampStore>,
    pub rules: Arc<FaultStore<Arc<StampStore>>>,
    pub recorder: Arc<Recorder>,
    /// Every attempt any process handed to its dispatcher, in order.
    pub dispatches: Arc<Mutex<Vec<(String, Dispatch)>>>,
    limits: StoreLimits,
    procs: Arc<Mutex<Vec<ProcHandle>>>,
}

/// A process's store and coordinator, kept after its actor ends or crashes
/// so a schedule can read its counters.
#[derive(Clone)]
pub struct ProcHandle {
    pub name: String,
    pub store: Arc<AgentStore>,
    pub co: Arc<Coordinator>,
}

impl Rig {
    /// A rig whose shared store applies `plan`.
    pub fn new(plan: FaultPlan) -> Rig {
        Rig::with_limits(plan, sim_limits())
    }

    pub fn with_limits(plan: FaultPlan, limits: StoreLimits) -> Rig {
        Rig::over(MemoryStore::new(), plan, limits)
    }

    /// A rig over `memory`, for a store with its own page size.
    pub fn over(memory: MemoryStore, plan: FaultPlan, limits: StoreLimits) -> Rig {
        let memory = Arc::new(memory);
        let stamps = Arc::new(StampStore::new(memory.clone()));
        let rules = Arc::new(FaultStore::new(stamps.clone(), plan));
        Rig {
            clock: Arc::new(SimClock(AtomicI64::new(T0_NS))),
            memory,
            stamps,
            rules,
            recorder: Arc::new(Recorder::default()),
            dispatches: Arc::default(),
            limits,
            procs: Arc::default(),
        }
    }

    /// The store the scheduler shares among actors.
    pub fn shared(&self) -> Arc<dyn ObjectStoreBackend> {
        self.rules.clone()
    }

    /// A process named `name` over `backend`, its coordinator's ids drawn
    /// from `seed`, sweeping with [`poll_sweep`].
    pub fn proc(&self, name: &str, backend: Arc<dyn ObjectStoreBackend>, seed: u64) -> Proc {
        self.proc_with(name, backend, seed, runtime_config(), Some(poll_sweep()))
    }

    /// A client's process: [`Rig::proc`] with the runtime's default sweep, so
    /// a pass it runs with [`Proc::sweep`] lists both halves. A client does
    /// not poll, so [`poll_sweep`] would only take the wake half's LISTs
    /// from those passes.
    pub fn client_proc(&self, name: &str, backend: Arc<dyn ObjectStoreBackend>, seed: u64) -> Proc {
        self.proc_with(name, backend, seed, runtime_config(), None)
    }

    /// [`Rig::proc`] whose coordinator runs with `config` and sweeps with
    /// `sweep`, or with the runtime's default sweep when that is `None`. A
    /// coordinator's idle polls run passes on the sweep's interval.
    pub fn proc_with(
        &self,
        name: &str,
        backend: Arc<dyn ObjectStoreBackend>,
        seed: u64,
        config: RuntimeConfig,
        sweep: Option<SweepConfig>,
    ) -> Proc {
        let store = Arc::new(
            AgentStore::new(
                Arc::new(StampStore::boundary(backend.clone())),
                TENANT,
                self.limits,
                self.clock.clone(),
                Arc::new(DerivedIds),
            )
            .with_backoff(Arc::new(NoBackoff)),
        );
        let co = Coordinator::new(
            store.clone(),
            config,
            self.clock.clone(),
            Arc::new(CountingRng(AtomicU64::new(seed))),
        );
        let co = Arc::new(match sweep {
            Some(config) => co.with_sweep(config),
            None => co,
        });
        let worker_id = co
            .register_worker(&[pin()])
            .unwrap_or_else(|_| format!("w-{seed}"));
        lock(&self.procs).push(ProcHandle {
            name: name.to_string(),
            store: store.clone(),
            co: co.clone(),
        });
        Proc {
            name: name.to_string(),
            store,
            co,
            recorder: self.recorder.clone(),
            worker_id,
            backend,
            dispatches: self.dispatches.clone(),
            clock: self.clock.clone(),
        }
    }

    /// Run `body` against a process over the shared store, outside any
    /// schedule, on its own paused runtime. Its calls are recorded.
    pub fn setup<F, Fut, T>(&self, body: F) -> Result<T, String>
    where
        F: FnOnce(Proc) -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let proc = self.client_proc("setup", self.shared(), 1);
        runtime.block_on(body(proc))
    }

    /// Read the agent's HEAD straight from memory, every sealed page
    /// attached, bypassing every fault and every recorder.
    pub fn head(&self) -> Result<Head, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let key = head_object_key()?;
        let got = runtime
            .block_on(self.memory.get(&key, GetRange::Full))
            .map_err(|e| format!("head read: {e}"))?;
        let mut head = Head::decode(&got.data).map_err(|e| format!("head decode: {e:?}"))?;
        for (list, page) in head.index.unloaded() {
            let dep = head.index.directory(list).pages[page].page;
            let key =
                object_key(&TENANT, &agent(), &dep.hash).map_err(|e| format!("page key: {e:?}"))?;
            let got = runtime
                .block_on(self.memory.get(&key, GetRange::Full))
                .map_err(|e| format!("page read: {e}"))?;
            attach_page(&mut head, list, page, &got.data)
                .map_err(|e| format!("page attach: {e:?}"))?;
        }
        Ok(head)
    }

    pub fn build(&self) -> Result<Run, String> {
        self.recorder.build()
    }

    /// The process named `name`; the last one when the name repeats.
    pub fn proc_named(&self, name: &str) -> Option<ProcHandle> {
        lock(&self.procs)
            .iter()
            .rev()
            .find(|p| p.name == name)
            .cloned()
    }

    /// Head CAS attempts and conflicts summed over every process.
    pub fn publish_totals(&self) -> (u64, u64) {
        lock(&self.procs).iter().fold((0, 0), |(cas, conflict), p| {
            let c = p.store.counters();
            (cas + c.publish.cas, conflict + c.publish.conflict)
        })
    }

    /// Every physical tool execution the simulated provider performed: the
    /// keys it wrote under [`PROVIDER_PREFIX`], read straight from memory.
    pub fn executions(&self) -> Result<Vec<String>, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let mut keys = Vec::new();
        let mut page = None;
        loop {
            let listed = runtime
                .block_on(self.memory.list(PROVIDER_PREFIX, page))
                .map_err(|e| format!("provider list: {e}"))?;
            keys.extend(listed.objects.into_iter().map(|o| o.key));
            match listed.next {
                Some(next) => page = Some(next),
                None => break,
            }
        }
        keys.sort();
        Ok(keys)
    }

    /// The attempts handed to a dispatcher, as (step key, attempt).
    pub fn dispatched(&self) -> Vec<(String, u32)> {
        lock(&self.dispatches)
            .iter()
            .map(|(_, d)| (d.step_key.clone(), d.attempt))
            .collect()
    }
}

/// Incarnations [`timer_handle`] searches.
const MAX_INCARNATION: u64 = 16;

/// The handle of timer `hex`, found as the id of `timer_key` in the
/// incarnation that derives it.
fn timer_handle(hex: &str, timer_key: &str) -> Option<Handle> {
    (1..=MAX_INCARNATION)
        .find(|&inc| timer_id(inc, timer_key).to_hex() == hex)
        .map(|incarnation| Handle {
            agent: agent(),
            incarnation,
            item: HandleItem::Timer(timer_id(incarnation, timer_key)),
        })
}

/// Where the simulated tool provider records each execution.
pub const PROVIDER_PREFIX: &str = "provider/";

/// The production step spec for `call`.
pub fn step_spec(call: &StepCall) -> StepSpec {
    StepSpec {
        step_key: call.key.clone(),
        tool_name: call.tool.clone(),
        tool_version: call.version.clone(),
        policy: call.policy,
        input_hash: step_input_hash(&call.tool, &call.version, &call.input),
    }
}

/// The effect of step `step_key` of input `accept_seq` in `incarnation`.
pub fn effect_ref(incarnation: u64, accept_seq: u64, step_key: &str) -> EffectRef {
    EffectRef {
        agent: agent(),
        incarnation,
        effect_id: effect_id(incarnation, accept_seq, step_key),
    }
}

/// Hands each attempt to the rig's log.
struct LogDispatcher<'a> {
    proc: &'a str,
    log: &'a Mutex<Vec<(String, Dispatch)>>,
}

impl EffectDispatcher for LogDispatcher<'_> {
    fn dispatch(&self, dispatch: &Dispatch) {
        lock(self.log).push((self.proc.to_string(), dispatch.clone()));
    }
}

fn phase_of(status: EffectStatus) -> Option<EffectPhase> {
    match status {
        EffectStatus::Authorized => Some(EffectPhase::Authorized),
        EffectStatus::InDoubt => Some(EffectPhase::InDoubt),
        EffectStatus::Completed => Some(EffectPhase::Completed),
        EffectStatus::Intent => None,
    }
}

fn settled(s: &EffectSettled) -> Observed {
    Observed::Modelled(match s {
        EffectSettled::Recorded => Reply::EffectRecorded,
        EffectSettled::Evidence { .. } => Reply::EffectEvidence,
    })
}

/// One simulated process: a store and a coordinator over one actor's view
/// of the shared store.
pub struct Proc {
    pub name: String,
    pub store: Arc<AgentStore>,
    pub co: Arc<Coordinator>,
    recorder: Arc<Recorder>,
    pub worker_id: String,
    /// The actor's own view of the shared store, under its gates; the
    /// simulated tool provider writes through it.
    backend: Arc<dyn ObjectStoreBackend>,
    dispatches: Arc<Mutex<Vec<(String, Dispatch)>>>,
    clock: Arc<SimClock>,
}

impl Proc {
    async fn record<T, Fut>(
        &self,
        call: &'static str,
        event: Option<Event>,
        fut: Fut,
        map: impl FnOnce(&T) -> (Option<Event>, Observed),
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        let (index, cell) = self.recorder.begin(&self.name, call, event);
        let out = CALL.scope(cell, fut).await;
        let (event, observed) = map(&out);
        self.recorder.finish(index, event, observed);
        out
    }

    pub async fn create(&self) -> Result<CreateAgentResponse, AgentError> {
        let req = CreateAgentRequest { agent: agent_ref() };
        self.record(
            "create",
            Some(Event::Client(ClientOp::Create)),
            self.co.create(&req),
            |r| {
                let observed = observe(r, |c| {
                    Observed::Modelled(Reply::Created {
                        incarnation: c.incarnation,
                        created: c.created,
                    })
                });
                (None, observed)
            },
        )
        .await
    }

    pub async fn send(
        &self,
        request_id: &str,
        payload: &Value,
    ) -> Result<SendResponse, AgentError> {
        let req = SendRequest {
            agent: agent_ref(),
            handler: HANDLER.to_string(),
            request_id: request_id.to_string(),
            payload: payload.clone(),
        };
        let event = Event::Client(ClientOp::Send {
            handler: HANDLER.to_string(),
            request_id: request_id.to_string(),
            payload: json_bytes(payload),
        });
        self.record("send", Some(event), self.co.send(&req), |r| {
            let observed = observe(r, |s| match decode_operation(&s.operation) {
                Ok(handle) if handle.item == HandleItem::Operation(s.accept_seq) => {
                    Observed::Modelled(Reply::Accepted {
                        operation: handle,
                        duplicate: s.duplicate,
                    })
                }
                Ok(handle) => Observed::Unmappable(format!(
                    "accept_seq {} disagrees with handle {handle:?}",
                    s.accept_seq
                )),
                Err(o) => o,
            });
            (None, observed)
        })
        .await
    }

    /// A poll that does not wait.
    pub async fn poll(&self) -> Result<Option<TurnAssignment>, AgentError> {
        self.poll_with(vec![pin()]).await
    }

    pub async fn poll_with(
        &self,
        handlers: Vec<HandlerPinBody>,
    ) -> Result<Option<TurnAssignment>, AgentError> {
        let req = WorkerPollRequest {
            worker_id: self.worker_id.clone(),
            handlers,
            wait_ms: 0,
        };
        self.record(
            "poll",
            None,
            self.co.poll(&req, tokio::time::Instant::now()),
            |r| match r {
                Ok(Some(a)) => match decode_operation(&a.operation) {
                    Ok(operation) => (
                        Some(Event::TurnStarted { operation }),
                        Observed::Modelled(Reply::TurnStarted),
                    ),
                    Err(o) => (None, o),
                },
                Ok(None) => (None, Observed::Skip),
                Err(e) => (None, map_error(e.kind)),
            },
        )
        .await
    }

    /// Commit a completion of `assignment` with `state` and `output`.
    pub async fn commit(
        &self,
        assignment: &TurnAssignment,
        state: &Value,
        output: &Value,
    ) -> Result<WorkerCommitResponse, AgentError> {
        let operation = match Handle::decode(&assignment.operation) {
            Ok(handle) => handle,
            Err(_) => {
                let (index, _) = self.recorder.begin(&self.name, "commit", None);
                self.recorder.finish(
                    index,
                    None,
                    Observed::Unmappable(format!("bad handle {}", assignment.operation)),
                );
                return self.co.commit(&completion(assignment, state, output)).await;
            }
        };
        let event = Event::TurnCompleted {
            operation,
            output: json_bytes(output),
        };
        let req = completion(assignment, state, output);
        self.record("commit", Some(event), self.co.commit(&req), |r| {
            let observed = observe(r, |c| {
                if c.effects.is_empty() {
                    Observed::Modelled(Reply::Committed {
                        status: c.status,
                        state_version: c.state_version,
                    })
                } else {
                    Observed::Unmappable(format!("commit dispatched effects {:?}", c.effects))
                }
            });
            (None, observed)
        })
        .await
    }

    pub async fn cancel(&self, operation: &Handle) -> Result<CancelResponse, AgentError> {
        let req = CancelRequest {
            operation: operation.encode(),
        };
        let event = Event::Client(ClientOp::Cancel {
            operation: *operation,
        });
        self.record("cancel", Some(event), self.co.cancel(&req), |r| {
            let observed = observe(r, |c| {
                Observed::Modelled(Reply::Cancel {
                    status: c.status,
                    requested: c.requested,
                })
            });
            (None, observed)
        })
        .await
    }

    /// The status of an operation, through `AgentStore::resolve`.
    pub async fn operation(&self, operation: &Handle) -> Result<Reply, AgentError> {
        let event = Event::Client(ClientOp::Operation {
            operation: *operation,
        });
        let fut = async {
            let resolution = self.store.resolve(operation).await?;
            Ok(match resolution.state {
                Some(state) => Reply::Status(state.into()),
                None => Reply::UnknownOperation,
            })
        };
        self.record("operation", Some(event), fut, |r| {
            (None, observe(r, |reply| Observed::Modelled(reply.clone())))
        })
        .await
    }

    /// The result of an operation from one HEAD read.
    pub async fn result(&self, operation: &Handle) -> Result<Reply, AgentError> {
        let event = Event::Client(ClientOp::Result {
            operation: *operation,
        });
        let fut = async {
            let head = self.live_head(operation.incarnation).await?;
            let HandleItem::Operation(seq) = operation.item else {
                return Ok(Reply::UnknownOperation);
            };
            if operation_state(&head, seq).is_none() {
                return Ok(Reply::UnknownOperation);
            }
            let Some(result) = head.results.iter().find(|r| r.accept_seq == seq) else {
                return Ok(Reply::Result(None));
            };
            let output = match result.output {
                Some(dep) => Some(
                    self.store
                        .read_object(&operation.agent, dep)
                        .await?
                        .to_vec(),
                ),
                None => None,
            };
            Ok(Reply::Result(Some(Outcome {
                status: result.status.into(),
                output,
                error: (!result.error.is_empty()).then(|| result.error.clone()),
            })))
        };
        self.record("result", Some(event), fut, |r| {
            (None, observe(r, |reply| Observed::Modelled(reply.clone())))
        })
        .await
    }

    /// The operation a request id was accepted as, from one HEAD read.
    pub async fn resolve(&self, request_id: &str) -> Result<Reply, AgentError> {
        let event = Event::Client(ClientOp::Resolve {
            handler: HANDLER.to_string(),
            request_id: request_id.to_string(),
        });
        let fut = async {
            let head = self.live_head(0).await?;
            let Some(receipt) = head
                .receipts
                .iter()
                .find(|r| r.request_id == request_id && r.handler == HANDLER)
            else {
                return Ok(Ok(Reply::Resolved(None)));
            };
            let handle = Handle::operation(agent(), head.incarnation, receipt.accept_seq);
            Ok(match operation_state(&head, receipt.accept_seq) {
                Some(state) => Ok(Reply::Resolved(Some((handle, state.into())))),
                None => Err(format!(
                    "receipt for {request_id} names accept_seq {} with no state",
                    receipt.accept_seq
                )),
            })
        };
        let out: Result<Result<Reply, String>, AgentError> = self
            .record(
                "resolve",
                Some(event),
                fut,
                |r: &Result<Result<Reply, String>, AgentError>| {
                    let observed = match r {
                        Ok(Ok(reply)) => Observed::Modelled(reply.clone()),
                        Ok(Err(what)) => Observed::Unmappable(what.clone()),
                        Err(e) => map_error(e.kind),
                    };
                    (None, observed)
                },
            )
            .await;
        match out {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Ok(Reply::Resolved(None)),
            Err(e) => Err(e),
        }
    }

    /// The operation of `assignment`; a handle that does not decode is
    /// recorded as unmappable and refused.
    fn assigned(&self, call: &'static str, a: &TurnAssignment) -> Result<Handle, AgentError> {
        Handle::decode(&a.operation).map_err(|_| {
            let (index, _) = self.recorder.begin(&self.name, call, None);
            self.recorder.finish(
                index,
                None,
                Observed::Unmappable(format!("bad handle {}", a.operation)),
            );
            AgentError::new(
                ErrorKind::OwnershipLost,
                ravel_agents_core::error::SafeIds::default(),
                "assignment handle does not decode",
            )
        })
    }

    /// Journal a step without running it: `Coordinator::record_step`.
    pub async fn record_step(
        &self,
        a: &TurnAssignment,
        call: &StepCall,
    ) -> Result<StepJournal, AgentError> {
        let operation = self.assigned("record_step", a)?;
        let event = Event::StepStarted {
            operation,
            step: call.clone(),
        };
        let spec = step_spec(call);
        self.record(
            "record_step",
            Some(event),
            self.co.record_step(&a.lease, &spec),
            |r| {
                let observed = observe(r, |j| {
                    let effect = match &j.effect {
                        None => None,
                        Some(e) => match phase_of(e.status) {
                            Some(phase) => Some(EffectView {
                                phase,
                                attempt: e.attempt,
                            }),
                            None if e.attempt == 0 => None,
                            None => {
                                return Observed::Unmappable(format!(
                                    "step journal shows an intent past attempt 0: {e:?}"
                                ));
                            }
                        },
                    };
                    Observed::Modelled(Reply::StepJournaled {
                        completed: j.step.status == StepStatus::Completed,
                        effect,
                    })
                });
                (None, observed)
            },
        )
        .await
    }

    /// Run a step: `Coordinator::run_step`, its attempt handed to the rig's
    /// dispatch log.
    pub async fn step(
        &self,
        a: &TurnAssignment,
        call: &StepCall,
    ) -> Result<StepProgress, AgentError> {
        let operation = self.assigned("step", a)?;
        let event = Event::Step {
            operation,
            step: call.clone(),
        };
        let spec = step_spec(call);
        let dispatcher = LogDispatcher {
            proc: &self.name,
            log: &self.dispatches,
        };
        self.record(
            "step",
            Some(event),
            self.co.run_step(&a.lease, &spec, &dispatcher),
            |r| {
                let observed = observe(r, |p| {
                    Observed::Modelled(match p {
                        StepProgress::Completed { output } => Reply::StepCompleted {
                            output: json_bytes(output),
                        },
                        StepProgress::Failed { error } => Reply::StepFailed {
                            error: error.clone(),
                        },
                        StepProgress::Dispatched(d) => Reply::StepDispatched { attempt: d.attempt },
                    })
                });
                (None, observed)
            },
        )
        .await
    }

    /// Report `result` for the attempt `d` of a step of `operation`.
    pub async fn complete_effect(
        &self,
        operation: &Handle,
        d: &Dispatch,
        result: &Value,
    ) -> Result<EffectSettled, AgentError> {
        let event = Event::EffectResult {
            operation: *operation,
            step_key: d.step_key.clone(),
            attempt: d.attempt,
            result: json_bytes(result),
        };
        self.record(
            "complete_effect",
            Some(event),
            self.co.complete_effect(&d.effect, d.attempt, result),
            |r| (None, observe(r, settled)),
        )
        .await
    }

    /// Report a failed attempt: [`Event::EffectFailed`] with the error.
    pub async fn fail_effect(
        &self,
        operation: &Handle,
        d: &Dispatch,
        error: &str,
    ) -> Result<EffectSettled, AgentError> {
        let event = Event::EffectFailed {
            operation: *operation,
            step_key: d.step_key.clone(),
            attempt: d.attempt,
            error: error.to_string(),
        };
        self.record(
            "fail_effect",
            Some(event),
            self.co.fail_effect(&d.effect, d.attempt, error),
            |r| (None, observe(r, settled)),
        )
        .await
    }

    /// The effect of step `step_key` of `operation`'s input.
    pub fn effect_of(operation: &Handle, step_key: &str) -> EffectRef {
        let seq = match operation.item {
            HandleItem::Operation(seq) => seq,
            _ => 0,
        };
        effect_ref(operation.incarnation, seq, step_key)
    }

    /// Attach a verified result to an effect in doubt.
    pub async fn reconcile_attach(
        &self,
        operation: &Handle,
        step_key: &str,
        result: &Value,
    ) -> Result<EffectSettled, AgentError> {
        let event = Event::ReconcileAttach {
            operation: *operation,
            step_key: step_key.to_string(),
            result: json_bytes(result),
        };
        let effect = Proc::effect_of(operation, step_key);
        self.record(
            "reconcile_attach",
            Some(event),
            self.co.reconcile_attach(&effect, result),
            |r| (None, observe(r, settled)),
        )
        .await
    }

    /// Authorize `attempt` of an effect in doubt, as an operator. Nothing
    /// is dispatched: this process's coordinator holds the attempt for the
    /// next [`Proc::step`] of the effect's step through this same process.
    pub async fn reconcile_authorize(
        &self,
        operation: &Handle,
        step_key: &str,
        attempt: u32,
    ) -> Result<bool, AgentError> {
        let event = Event::ReconcileAuthorize {
            operation: *operation,
            step_key: step_key.to_string(),
            attempt,
        };
        let effect = Proc::effect_of(operation, step_key);
        self.record(
            "reconcile_authorize",
            Some(event),
            self.co.reconcile_authorize(&effect, attempt),
            |r| {
                let observed = observe(r, |handed_over| {
                    Observed::Modelled(Reply::Reconciled {
                        handed_over: *handed_over,
                    })
                });
                (None, observed)
            },
        )
        .await
    }

    /// Schedule `timer_key` due at `due_unix_ns` with an empty payload.
    pub async fn schedule_timer(
        &self,
        timer_key: &str,
        due_unix_ns: i64,
    ) -> Result<TimerResponse, AgentError> {
        let req = TimerRequest {
            agent: agent_ref(),
            timer_key: timer_key.to_string(),
            due_unix_ns,
            handler: HANDLER.to_string(),
            payload: Value::Null,
        };
        let event = Event::Client(ClientOp::SetTimer {
            timer_key: timer_key.to_string(),
            due_unix_ns,
            handler: HANDLER.to_string(),
            payload: json_bytes(&Value::Null),
        });
        self.record(
            "schedule_timer",
            Some(event),
            self.co.schedule_timer(&req),
            |r| {
                let observed = observe(r, |t| match timer_handle(&t.timer, timer_key) {
                    Some(timer) => Observed::Modelled(Reply::TimerSet {
                        timer,
                        generation: t.generation,
                    }),
                    None => Observed::Unmappable(format!(
                        "timer {} is {timer_key} in no incarnation up to {MAX_INCARNATION}",
                        t.timer
                    )),
                });
                (None, observed)
            },
        )
        .await
    }

    pub async fn cancel_timer(&self, timer_key: &str) -> Result<CancelTimerResponse, AgentError> {
        let req = CancelTimerRequest {
            agent: agent_ref(),
            timer_key: timer_key.to_string(),
        };
        let event = Event::Client(ClientOp::CancelTimer {
            timer_key: timer_key.to_string(),
        });
        self.record(
            "cancel_timer",
            Some(event),
            self.co.cancel_timer(&req),
            |r| {
                let observed = observe(r, |c| Observed::Modelled(Reply::TimerCancel(c.outcome)));
                (None, observed)
            },
        )
        .await
    }

    /// One sweep pass over the tenant: `Coordinator::sweep_once`. It is
    /// recorded as a wake from a hint when the pass read a due hint, else
    /// from the sweep; the reference answers both alike.
    pub async fn sweep(&self) -> Result<SweepReport, AgentError> {
        let now_unix_ns = self.clock.now_unix_ns();
        let begun = Event::Wake {
            now_unix_ns,
            by: WakeSource::Sweep,
        };
        self.record(
            SWEEP,
            Some(begun),
            self.co.sweep_once(&TENANT),
            |r| match r {
                Ok(report) => {
                    let by = if report.hints_read > 0 {
                        WakeSource::Hint
                    } else {
                        WakeSource::Sweep
                    };
                    (Some(Event::Wake { now_unix_ns, by }), Observed::Swept)
                }
                Err(e) => (None, map_error(e.kind)),
            },
        )
        .await
    }

    /// The last attempt this process handed to its dispatcher.
    pub fn last_dispatch(&self) -> Option<Dispatch> {
        lock(&self.dispatches)
            .iter()
            .rev()
            .find(|(name, _)| *name == self.name)
            .map(|(_, d)| d.clone())
    }

    /// The simulated tool provider executes `d` through this process's
    /// store view and answers the tool's result. Under
    /// `provider_idempotency` it keys the execution by effect id, so a
    /// re-dispatch of the same effect executes nothing new; otherwise every
    /// attempt is a new execution.
    pub async fn execute(&self, d: &Dispatch) -> Result<Value, StoreError> {
        let id = d.effect.effect_id.to_hex();
        let (key, result) = match d.policy {
            RecoveryPolicy::ProviderIdempotency => (
                format!("{PROVIDER_PREFIX}{id}"),
                serde_json::json!({ "step": d.step_key }),
            ),
            _ => (
                format!("{PROVIDER_PREFIX}{id}/a{}", d.attempt),
                serde_json::json!({ "step": d.step_key, "attempt": d.attempt }),
            ),
        };
        match self
            .backend
            .put(
                &key,
                Bytes::from(json_bytes(&result)),
                PutOptions::create_if_absent(),
            )
            .await
        {
            Ok(_) | Err(StoreError::AlreadyExists) => Ok(result),
            Err(e) => Err(e),
        }
    }

    /// HEAD, refused as `AgentGone` when it is absent, tombstoned, or of
    /// another incarnation than `incarnation` (0 accepts any).
    async fn live_head(&self, incarnation: u64) -> Result<Head, AgentError> {
        let gone = |reason| {
            AgentError::new(
                ErrorKind::AgentGone,
                ravel_agents_core::error::SafeIds::default(),
                reason,
            )
        };
        match self.store.read_head_full(&agent()).await? {
            HeadState::Present { head, .. } => {
                if (incarnation != 0 && head.incarnation != incarnation)
                    || head.lifecycle == ravel_agents_core::head::Lifecycle::Tombstoned
                {
                    return Err(gone("handle names an incarnation that is not current"));
                }
                Ok(*head)
            }
            HeadState::Absent => Err(gone("agent does not exist")),
            HeadState::Unsupported { .. } => Err(AgentError::new(
                ErrorKind::UnsupportedFormat,
                ravel_agents_core::error::SafeIds::default(),
                "HEAD format version is newer than this release",
            )),
        }
    }

    /// Poll and commit up to `turns` times, stopping at the first poll that
    /// finds nothing. Each completion's state and output name the input.
    /// Errors are recorded and end the loop; returns the turns committed.
    pub async fn work(&self, turns: usize) -> usize {
        let mut done = 0;
        for _ in 0..turns {
            let Ok(Some(a)) = self.poll().await else {
                break;
            };
            let (state, output) = turn_values(&a);
            if self.commit(&a, &state, &output).await.is_err() {
                break;
            }
            done += 1;
        }
        done
    }
}

/// The state and output a simulated handler produces for an assignment.
pub fn turn_values(a: &TurnAssignment) -> (Value, Value) {
    (
        serde_json::json!({ "last": a.accept_seq }),
        serde_json::json!({ "seq": a.accept_seq, "echo": a.payload }),
    )
}

fn completion(a: &TurnAssignment, state: &Value, output: &Value) -> WorkerCommitRequest {
    WorkerCommitRequest {
        lease: a.lease.clone(),
        outcome: TurnOutcome::Completed {
            state: state.clone(),
            output: output.clone(),
            events: Vec::new(),
            timers: Vec::new(),
            intents: Vec::new(),
        },
    }
}

/// Every accepted input of `head` reached exactly one terminal result, and
/// nothing is left to run.
pub fn one_terminal_result_per_input(head: &Head) -> Result<(), String> {
    let mut seqs: Vec<u64> = head.results.iter().map(|r| r.accept_seq).collect();
    seqs.sort_unstable();
    let expected: Vec<u64> = (1..head.next_accept_seq).collect();
    if seqs != expected {
        return Err(format!(
            "results {seqs:?}, accepted {expected:?}, inbox {:?}",
            head.inbox.iter().map(|e| e.accept_seq).collect::<Vec<_>>()
        ));
    }
    if !head.inbox.is_empty() || head.active_turn.is_some() {
        return Err(format!(
            "work left: inbox {}, active turn {:?}",
            head.inbox.len(),
            head.active_turn
        ));
    }
    Ok(())
}

/// The status a result entry reports, for assertions.
pub fn status_of(head: &Head, accept_seq: u64) -> Option<OperationStatus> {
    head.results
        .iter()
        .find(|r| r.accept_seq == accept_seq)
        .map(|r| r.status.into())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_agents_core::head::{EffectRecord, RecoveryPolicy as HeadPolicy, StepRecord};
    use ravel_agents_core::identity::{Hash256, Id128, step_id};

    use super::*;

    /// Record a call that answered what `reference` answers, stamped as a
    /// read at `seq`.
    fn answered(rec: &Recorder, reference: &mut ReferenceMachine, event: Event, seq: u64) {
        let reply = reference.apply(&event);
        let (index, cell) = rec.begin("w1", "call", Some(event));
        lock(&cell).read = Some(seq);
        rec.finish(index, None, Observed::Modelled(reply));
    }

    /// Record a call that applied a write of `body` at `seq` and never
    /// answered.
    fn crashed(rec: &Recorder, event: Event, seq: u64, body: Bytes) {
        let (_, cell) = rec.begin("w2", "call", Some(event));
        lock(&cell).write = Some((seq, body));
    }

    /// Create the agent and accept input 1.
    fn accepted(rec: &Recorder, reference: &mut ReferenceMachine) -> Handle {
        answered(rec, reference, Event::Client(ClientOp::Create), 1);
        let send = Event::Client(ClientOp::Send {
            handler: HANDLER.to_string(),
            request_id: "r1".to_string(),
            payload: b"{}".to_vec(),
        });
        answered(rec, reference, send, 2);
        reference.operation(1, 1)
    }

    #[test]
    fn a_crashed_write_the_reference_refuses_outside_the_exemption_is_unmappable() {
        let rec = Recorder::default();
        let mut reference = machine().expect("machine");
        let op = accepted(&rec, &mut reference);
        // No turn started: the reference refuses the completion.
        let commit = Event::TurnCompleted {
            operation: op,
            output: b"out".to_vec(),
        };
        crashed(&rec, commit.clone(), 3, Bytes::from_static(b"root"));
        let run = rec.build().expect("build");
        assert_eq!(
            run.unmappable,
            vec![format!(
                "w2: applied a write the reference refuses OwnershipLost: {commit:?}"
            )]
        );
        assert_eq!(run.inferred, vec![("w2".to_string(), commit)]);
    }

    #[test]
    fn a_crashed_manual_step_whose_attempt_was_lost_infers_effect_in_doubt() {
        let rec = Recorder::default();
        let mut reference = machine().expect("machine");
        let op = accepted(&rec, &mut reference);
        let step = StepCall {
            key: "lookup".to_string(),
            tool: "search".to_string(),
            version: "v1".to_string(),
            input: b"q".to_vec(),
            policy: RecoveryPolicy::Manual,
        };
        let run_step = Event::Step {
            operation: op,
            step: step.clone(),
        };
        let begin = Event::TurnStarted { operation: op };
        answered(&rec, &mut reference, begin.clone(), 3);
        answered(&rec, &mut reference, run_step.clone(), 4);
        // A takeover: attempt 1 was dispatched by a turn that is gone.
        answered(&rec, &mut reference, begin, 5);
        // The step latched the effect in doubt, then the process died.
        let rig = Rig::new(FaultPlan::empty());
        rig.setup(|p| async move {
            p.create().await.map_err(|e| format!("create: {e:?}"))?;
            Ok(())
        })
        .expect("setup");
        let mut head = rig.head().expect("head");
        head.effects.push(EffectRecord {
            effect_id: effect_id(1, 1, &step.key),
            step_key: step.key.clone(),
            tool_name: step.tool.clone(),
            tool_version: step.version.clone(),
            input_hash: Hash256([0; 32]),
            policy: HeadPolicy::Manual,
            status: EffectStatus::InDoubt,
            attempt: 1,
            result: None,
            result_attempt: 0,
            turn_id: Id128([0; 16]),
        });
        crashed(&rec, run_step.clone(), 6, Bytes::from(head.encode()));
        let run = rec.build().expect("build");
        assert_eq!(run.unmappable, Vec::<String>::new());
        assert_eq!(run.inferred, vec![("w2".to_string(), run_step.clone())]);
        let last = run.history.calls().last().expect("a call");
        assert_eq!(last.event, run_step);
        assert_eq!(last.observed, Reply::Error(ErrorKind::EffectInDoubt));
        assert_eq!(run.check(), Ok(()));
    }

    /// Build the run of a step that crashed after a cancel, having written
    /// a root that holds the step's journaled start when `journaled`, and
    /// no trace of the step otherwise.
    fn crashed_past_a_cancel(journaled: bool) -> (Run, Event) {
        let rec = Recorder::default();
        let mut reference = machine().expect("machine");
        let op = accepted(&rec, &mut reference);
        let step = StepCall {
            key: "lookup".to_string(),
            tool: "search".to_string(),
            version: "v1".to_string(),
            input: b"q".to_vec(),
            policy: RecoveryPolicy::AtLeastOnce,
        };
        let run_step = Event::Step {
            operation: op,
            step: step.clone(),
        };
        answered(
            &rec,
            &mut reference,
            Event::TurnStarted { operation: op },
            3,
        );
        let cancel = Event::Client(ClientOp::Cancel { operation: op });
        answered(&rec, &mut reference, cancel, 4);
        let rig = Rig::new(FaultPlan::empty());
        rig.setup(|p| async move {
            p.create().await.map_err(|e| format!("create: {e:?}"))?;
            Ok(())
        })
        .expect("setup");
        let mut head = rig.head().expect("head");
        if journaled {
            head.steps.push(StepRecord {
                step_id: step_id(1, 1, &step.key),
                step_key: step.key.clone(),
                tool_name: step.tool.clone(),
                tool_version: step.version.clone(),
                input_hash: Hash256([0; 32]),
                status: StepStatus::Started,
                output: None,
            });
        }
        crashed(&rec, run_step.clone(), 5, Bytes::from(head.encode()));
        (rec.build().expect("build"), run_step)
    }

    #[test]
    fn a_step_crashed_after_journaling_past_a_cancel_infers_ownership_lost() {
        // The step journaled its start, no attempt, then the process died
        // before the cancel check that would have refused it.
        let (run, run_step) = crashed_past_a_cancel(true);
        assert_eq!(run.unmappable, Vec::<String>::new());
        assert_eq!(run.inferred, vec![("w2".to_string(), run_step.clone())]);
        let last = run.history.calls().last().expect("a call");
        assert_eq!(last.event, run_step);
        assert_eq!(last.observed, Reply::Error(ErrorKind::OwnershipLost));
        assert_eq!(run.check(), Ok(()));
    }

    #[test]
    fn a_step_crashed_past_a_cancel_with_no_journaled_start_is_unmappable() {
        let (run, run_step) = crashed_past_a_cancel(false);
        assert_eq!(
            run.unmappable,
            vec![format!(
                "w2: applied a write the reference refuses OwnershipLost: {run_step:?}"
            )]
        );
        assert_eq!(run.inferred, vec![("w2".to_string(), run_step)]);
    }

    #[test]
    fn a_sweep_answered_with_an_error_the_history_cannot_take_is_unmappable() {
        let rec = Recorder::default();
        let wake = Event::Wake {
            now_unix_ns: 10,
            by: WakeSource::Sweep,
        };
        let (unknown, cell) = rec.begin("w1", SWEEP, Some(wake.clone()));
        lock(&cell).read = Some(1);
        rec.finish(unknown, None, map_error(ErrorKind::OutcomeUnknown));
        let (gone, cell) = rec.begin("w2", SWEEP, Some(wake));
        lock(&cell).read = Some(2);
        rec.finish(gone, None, map_error(ErrorKind::AgentGone));
        let run = rec.build().expect("build");
        assert_eq!(
            run.unmappable,
            vec![
                "w1 sweep: OutcomeUnknown reached the caller".to_string(),
                "w2 sweep: answered Error(AgentGone)".to_string(),
            ]
        );
        assert_eq!(run.history.calls().len(), 0);
        assert_eq!(run.refusals, Vec::new());
    }

    /// Build the run of a step that crashed after writing a root whose
    /// effect is an intent at `attempt`; answer it and the intent.
    fn crashed_at_intent(attempt: u32) -> (Run, Event, EffectRecord) {
        let rec = Recorder::default();
        let mut reference = machine().expect("machine");
        let op = accepted(&rec, &mut reference);
        let step = StepCall {
            key: "lookup".to_string(),
            tool: "search".to_string(),
            version: "v1".to_string(),
            input: b"q".to_vec(),
            policy: RecoveryPolicy::AtLeastOnce,
        };
        let run_step = Event::Step {
            operation: op,
            step: step.clone(),
        };
        answered(
            &rec,
            &mut reference,
            Event::TurnStarted { operation: op },
            3,
        );
        let rig = Rig::new(FaultPlan::empty());
        rig.setup(|p| async move {
            p.create().await.map_err(|e| format!("create: {e:?}"))?;
            Ok(())
        })
        .expect("setup");
        let mut head = rig.head().expect("head");
        let intent = EffectRecord {
            effect_id: effect_id(1, 1, &step.key),
            step_key: step.key.clone(),
            tool_name: step.tool.clone(),
            tool_version: step.version.clone(),
            input_hash: Hash256([0; 32]),
            policy: HeadPolicy::AtLeastOnce,
            status: EffectStatus::Intent,
            attempt,
            result: None,
            result_attempt: 0,
            turn_id: Id128([0; 16]),
        };
        head.effects.push(intent.clone());
        crashed(&rec, run_step, 4, Bytes::from(head.encode()));
        let started = Event::StepStarted {
            operation: op,
            step,
        };
        (rec.build().expect("build"), started, intent)
    }

    #[test]
    fn a_crashed_step_whose_root_shows_an_intent_is_journaled_with_no_attempt() {
        let (run, started, _) = crashed_at_intent(0);
        assert_eq!(run.unmappable, Vec::<String>::new());
        assert_eq!(run.inferred, vec![("w2".to_string(), started.clone())]);
        let last = run.history.calls().last().expect("a call");
        assert_eq!(last.event, started);
        assert_eq!(run.check(), Ok(()));
    }

    #[test]
    fn a_crashed_step_whose_root_shows_an_intent_past_attempt_zero_is_unmappable() {
        let (run, _, intent) = crashed_at_intent(1);
        assert_eq!(
            run.unmappable,
            vec![format!(
                "w2 step: step journal shows an intent past attempt 0: {intent:?}"
            )]
        );
        assert_eq!(run.inferred, Vec::new());
    }

    /// Record a step that wrote at `write`, read at `read` and answered
    /// `kind`, beside a state read at `between`; answer the order the
    /// history places the two in.
    fn refused_step_order(kind: ErrorKind, write: u64, between: u64, read: u64) -> Vec<Event> {
        let rec = Recorder::default();
        let mut reference = machine().expect("machine");
        let op = accepted(&rec, &mut reference);
        let step = Event::Step {
            operation: op,
            step: StepCall {
                key: "lookup".to_string(),
                tool: "search".to_string(),
                version: "v1".to_string(),
                input: b"q".to_vec(),
                policy: RecoveryPolicy::AtLeastOnce,
            },
        };
        let (index, cell) = rec.begin("w1", "run_step", Some(step));
        {
            let mut stamps = lock(&cell);
            stamps.write = Some((write, Bytes::from_static(b"root")));
            stamps.read = Some(read);
        }
        rec.finish(index, None, Observed::Modelled(Reply::Error(kind)));
        answered(
            &rec,
            &mut reference,
            Event::Client(ClientOp::State),
            between,
        );
        let run = rec.build().expect("build");
        run.history
            .calls()
            .iter()
            .skip(2)
            .map(|c| c.event.clone())
            .collect()
    }

    #[test]
    fn only_an_ownership_lost_step_takes_its_later_read_stamp() {
        let state = Event::Client(ClientOp::State);
        let lost = refused_step_order(ErrorKind::OwnershipLost, 3, 4, 5);
        assert_eq!(lost.len(), 2);
        assert_eq!(lost[0], state);
        for kind in [
            ErrorKind::EffectInDoubt,
            ErrorKind::NonDeterministicReplay,
            ErrorKind::IdempotencyConflict,
            ErrorKind::AgentGone,
        ] {
            let order = refused_step_order(kind, 3, 4, 5);
            assert_eq!(order.len(), 2, "{kind:?}");
            assert_eq!(order[1], state, "{kind:?}");
        }
    }
}
