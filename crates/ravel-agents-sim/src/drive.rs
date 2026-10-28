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
//! read. The numbers reach the call through a task-local cell
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
//!   `accept_seq` that disagrees with the handle is unmappable.
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
//!   has no intents.
//! - `Coordinator::cancel` answers [`Reply::Cancel`] with its status and
//!   `requested` flag (`schedules::races::cancel_racing_completion_one_wins`).
//! - [`Proc::operation`] reads through `AgentStore::resolve` and answers
//!   [`Reply::Status`] (`schedules::senders::competing_senders_take_distinct_accept_seqs`),
//!   or [`Reply::UnknownOperation`] when the root has no input under the
//!   handle. No schedule queries a handle with no input yet, so the
//!   `UnknownOperation` answers of [`Proc::operation`] and [`Proc::result`]
//!   and the `Resolved(None)` answer of [`Proc::resolve`] are mappings no
//!   test has reached; the milestone 3 schedules add one.
//! - [`Proc::result`] reads HEAD once and answers [`Reply::Result`]: `None`
//!   while the input is not terminal, else its status, the output object's
//!   bytes, and the error string with `""` read as no error
//!   (`schedules::senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation`).
//! - [`Proc::resolve`] reads HEAD once and answers [`Reply::Resolved`] with
//!   the receipt matching both handler and request id
//!   (`schedules::senders::same_id_different_payload_conflicts`). A receipt
//!   whose input has no state is unmappable.
//! - The error kinds the reference models, `OwnershipLost`, `AgentGone` and
//!   `IdempotencyConflict`, answer [`Reply::Error`]
//!   (`schedules::workers::suspended_worker_commit_is_refused_after_takeover`,
//!   `schedules::senders::same_id_different_payload_conflicts`).
//! - The kinds it does not model (`StoreUnavailable`, `MailboxFull`,
//!   `BudgetExceeded`, `UnsupportedFormat`, `IncompatibleHandler`, and any
//!   other) are not fed to the reference. They are collected as
//!   [`Refusal`]s, which each schedule asserts exactly
//!   (`schedules::errors`).
//! - `OutcomeUnknown` reaching the caller is unmappable: the coordinator
//!   settles it on every path, so a client never has to.
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
//! Anything else the driver cannot map is listed in [`Run::unmappable`],
//! and every schedule asserts that list is empty.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_core::error::{AgentError, ErrorKind};
use ravel_agents_core::head::Head;
use ravel_agents_core::identity::{AgentId, agent_id};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::{AgentKey, AgentKind, head_key};
use ravel_agents_core::state::{Handle, HandleItem, operation_state};
use ravel_agents_core::wire::{
    AgentRef, CancelRequest, CancelResponse, CreateAgentRequest, CreateAgentResponse,
    HandlerPinBody, OperationStatus, SendRequest, SendResponse, TurnAssignment, TurnOutcome,
    WorkerCommitRequest, WorkerCommitResponse, WorkerPollRequest,
};
use ravel_agents_runtime::{Coordinator, DEFAULT_POLL_WAIT_CAP_MS, Rng, RuntimeConfig};
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
use crate::reference::{ClientOp, Event, Outcome, ReferenceMachine, Reply};

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
    /// Sequence number of the last HEAD read it made.
    read: Option<u64>,
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
}

impl StampStore {
    pub fn new(inner: Arc<dyn ObjectStoreBackend>) -> StampStore {
        StampStore {
            inner,
            seq: AtomicU64::new(0),
            head_puts: AtomicU64::new(0),
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
        if head {
            self.head_puts.fetch_add(1, Ordering::SeqCst);
        }
        let body = data.clone();
        let out = self.inner.put(key, data, opts).await;
        if head && out.is_ok() {
            self.stamp(|s, seq| s.write = Some((seq, body)));
        }
        out
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let out = self.inner.get(key, range).await;
        if is_head(key) && out.is_ok() {
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
        // (stamp, begin order, actor, event, observed reply or None to infer)
        let mut items: Vec<(u64, usize, String, Event, Option<Reply>)> = Vec::new();
        for (order, entry) in entries.iter().enumerate() {
            let stamps = lock(&entry.cell).clone();
            let stamp = stamps.write.as_ref().map(|(s, _)| *s).or(stamps.read);
            let applied = stamps.write.as_ref().map(|(_, body)| body.clone());
            if let Some(Observed::Refused(kind)) = &entry.observed {
                refusals.push(Refusal {
                    actor: entry.actor.clone(),
                    call: entry.call,
                    kind: *kind,
                });
            }
            match (&entry.observed, applied, stamp) {
                (Some(Observed::Unmappable(what)), _, _) => {
                    unmappable.push(format!("{} {}: {what}", entry.actor, entry.call));
                }
                (Some(Observed::Skip), _, _) => {}
                (Some(Observed::Modelled(reply)), _, Some(stamp)) => match &entry.event {
                    Some(event) => items.push((
                        stamp,
                        order,
                        entry.actor.clone(),
                        event.clone(),
                        Some(reply.clone()),
                    )),
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
                        items.push((stamp, order, entry.actor.clone(), event, None));
                    }
                }
                (Some(Observed::Refused(_)) | None, _, _) => {}
            }
        }
        items.sort_by_key(|(stamp, order, ..)| (*stamp, *order));
        let mut shadow = machine()?;
        let mut history = History::default();
        let mut inferred = Vec::new();
        for (_, _, actor, event, observed) in items {
            let expected = shadow.apply(&event);
            let reply = match observed {
                Some(reply) => reply,
                None => {
                    inferred.push((actor, event.clone()));
                    expected
                }
            };
            history.record(event, reply);
        }
        Ok(Run {
            history,
            inferred,
            refusals,
            unmappable,
        })
    }
}

/// Map an error to what the history records.
pub fn map_error(kind: ErrorKind) -> Observed {
    match kind {
        ErrorKind::OwnershipLost | ErrorKind::AgentGone | ErrorKind::IdempotencyConflict => {
            Observed::Modelled(Reply::Error(kind))
        }
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

/// The coordinator settings every process uses: short leases, and an idle
/// unload and rescan interval of zero so a poll always sees work that
/// arrived through another process.
pub fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        max_turn_attempts: 3,
        idle_unload_after_ms: 0,
        poll_wait_cap_ms: DEFAULT_POLL_WAIT_CAP_MS,
        rescan_interval_ms: 0,
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
        Rig::with_limits(plan, StoreLimits::default())
    }

    pub fn with_limits(plan: FaultPlan, limits: StoreLimits) -> Rig {
        let memory = Arc::new(MemoryStore::new());
        let stamps = Arc::new(StampStore::new(memory.clone()));
        let rules = Arc::new(FaultStore::new(stamps.clone(), plan));
        Rig {
            clock: Arc::new(SimClock(AtomicI64::new(T0_NS))),
            memory,
            stamps,
            rules,
            recorder: Arc::new(Recorder::default()),
            limits,
            procs: Arc::default(),
        }
    }

    /// The store the scheduler shares among actors.
    pub fn shared(&self) -> Arc<dyn ObjectStoreBackend> {
        self.rules.clone()
    }

    /// A process named `name` over `backend`, its coordinator's ids drawn
    /// from `seed`.
    pub fn proc(&self, name: &str, backend: Arc<dyn ObjectStoreBackend>, seed: u64) -> Proc {
        let store = Arc::new(
            AgentStore::new(
                backend,
                TENANT,
                self.limits,
                self.clock.clone(),
                Arc::new(DerivedIds),
            )
            .with_backoff(Arc::new(NoBackoff)),
        );
        let co = Arc::new(Coordinator::new(
            store.clone(),
            runtime_config(),
            self.clock.clone(),
            Arc::new(CountingRng(AtomicU64::new(seed))),
        ));
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
        let proc = self.proc("setup", self.shared(), 1);
        runtime.block_on(body(proc))
    }

    /// Read the agent's HEAD straight from memory, bypassing every fault
    /// and every recorder.
    pub fn head(&self) -> Result<Head, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let key = head_object_key()?;
        let got = runtime
            .block_on(self.memory.get(&key, GetRange::Full))
            .map_err(|e| format!("head read: {e}"))?;
        Head::decode(&got.data).map_err(|e| format!("head decode: {e:?}"))
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
}

/// One simulated process: a store and a coordinator over one actor's view
/// of the shared store.
pub struct Proc {
    pub name: String,
    pub store: Arc<AgentStore>,
    pub co: Arc<Coordinator>,
    recorder: Arc<Recorder>,
    pub worker_id: String,
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
        match self.store.read_head(&agent()).await? {
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
