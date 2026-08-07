//! Fault-injecting wrapper: `FaultStore<S>` wraps any [`ObjectStoreBackend`]
//! and applies a deterministic, scriptable [`FaultPlan`] on top of it, plus
//! an optional seeded-random fault mode. Used to test that callers (commit
//! protocol, catalog, GC) handle the full `StoreError` taxonomy correctly
//! without needing a flaky real backend.
//!
//! Design summary (see docs/object-store-contract.md, "Implementations" §2):
//! - Rules match by operation kind, an optional key substring, and an
//!   occurrence count local to that rule (`Always` or the `Nth` matching
//!   call). The first rule whose `op` and `key_contains` match a call fully
//!   governs that call: either it fires (the scripted fault is applied) or
//!   it passes the call through untouched. Rules never combine.
//! - [`Sequence`]s script an ordered list of outcomes for successive matching
//!   calls to one `(op, key)` (call 1 returns a transient error, call 2 a
//!   `NotFound` blip, call 3 succeeds, ...). Sequences are evaluated before
//!   rules: the first sequence with an unconsumed step whose `op` and
//!   `key_contains` match governs the call. Once a sequence's steps are
//!   exhausted it stops matching and the call falls through to rules and, if
//!   configured, random mode. A single `FaultStore` never sees sequences
//!   unless a plan carries them, so scripted-rule behavior is unchanged when
//!   no sequence is configured.
//! - `Op::List` matches both `list()` and `list_delimited()` calls.
//! - If no rule matches, and a [`RandomFaultConfig`] is configured, each
//!   fault kind applicable to the operation is tried in a fixed order
//!   against a seeded `StdRng` owned by this store (no global RNG state);
//!   the first Bernoulli trial that succeeds fires that fault with default
//!   parameters.
//! - `PartialWriteThenError` and `FailedConditionalWrite` never call the
//!   wrapped backend: the object must not become visible. `DuplicateDelivery`
//!   calls the wrapped backend for real (so the operation applies) and then
//!   returns `Transient`, modeling an acknowledgement lost after the op
//!   completed. `CorruptRange` calls through to get the real bytes and then
//!   flips every bit before returning them. Everything else short-circuits
//!   without touching the backend.
//! - Every fault actually injected (win in `resolve`) is counted under its
//!   `(Op, FaultKind)` pair, regardless of whether the call ultimately
//!   surfaces as an `Err` (most kinds) or an `Ok` with corrupted contents
//!   (`CorruptRange`).
//!
//! Beyond that synchronous fault plan, a separate hold-and-release gate
//! controls *completion ordering* across concurrently in-flight calls
//! (ADR-0059 decision 5). A gate registered with [`FaultStore::hold`] matches
//! a call the same way a [`Rule`] does -- by [`Op`], optional key substring,
//! and [`Occurrence`] -- but instead of resolving to a per-call outcome it
//! blocks the matching call inside `FaultStore` until a test-side
//! [`GateHandle`] releases that specific held call. This is a hold/release
//! protocol orthogonal to the rule/sequence outcome table: a gated call still
//! flows through the fault plan once released, so a gate composes with a
//! scripted fault rather than replacing it. It is a test-only mechanism (not a
//! general store capability, and no production path depends on completion
//! order, per ADR-0059) for driving the two places completion order is
//! contract-relevant but was never exercised before: multipart part completion
//! and cross-shard commit-record visibility ordering.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::sync::{Notify, oneshot};

use crate::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutMode, PutOptions, PutOutcome, StoreError,
};

/// Operation kind used to match [`Rule`]s. `List` matches both `list()` and
/// `list_delimited()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Put,
    Get,
    Head,
    List,
    Delete,
}

/// Fault-kind tag: `Copy`/`Eq`/`Hash` so it can key counters and random-mode
/// probability tables. [`ScriptedFault`] is the payload-carrying counterpart
/// scripted into a [`Rule`] or produced (with default parameters) by random
/// mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultKind {
    Timeout,
    Throttled,
    PartialWriteThenError,
    FailedConditionalWrite,
    DuplicateDelivery,
    NotFoundBlip,
    CorruptRange,
    EtagChange,
    Transient,
    Permanent,
}

/// A scripted fault with its parameters (if any).
#[derive(Debug, Clone, PartialEq)]
pub enum ScriptedFault {
    /// No response before the caller's deadline; the backend's own state is
    /// left untouched (the call never reaches the wrapped backend).
    Timeout,
    /// Backend pushback; the call never reaches the wrapped backend.
    Throttled {
        retry_after_ms: u64,
    },
    /// `put` only: models a connection dropped mid-upload. The wrapped `put`
    /// is NOT called; the object must stay invisible.
    PartialWriteThenError,
    /// `put` only: a conditional write that the backend rejects without
    /// applying it. Surfaces as `AlreadyExists` under `CreateIfAbsent` and
    /// `PreconditionFailed` under `CasVersion`, matching the contract's
    /// mode-aware mapping rule.
    FailedConditionalWrite,
    /// The wrapped operation is called for real (so it applies), but the
    /// acknowledgement is modeled as lost: the caller sees `Transient`.
    DuplicateDelivery,
    /// `get`/`head` only: a transient "not found" that does not reflect
    /// true absence (an eventual-consistency blip). The wrapped backend is
    /// not called.
    NotFoundBlip,
    /// `get` only: the wrapped `get` is called for real and every byte of
    /// the returned data is bit-flipped before being handed back.
    CorruptRange,
    /// `get` only: the wrapped `get` is called for real and its bytes are
    /// returned unchanged, but the etag is replaced with a deterministic
    /// value distinct from any this store would otherwise produce. Models a
    /// concurrent overwrite landing between two reads of one segment: the
    /// second read sees fresh bytes under a new etag, so a caller pinning a
    /// snapshot by etag (the fetcher's `EtagChanged` guard) must abort rather
    /// than mix bytes from two object versions. Scripting it as one step of a
    /// [`Sequence`] (a passthrough first read, then this on a later read) is
    /// the only way to drive that guard deterministically, since a real
    /// re-`put` cannot be interleaved inside a single in-memory fetch.
    EtagChange,
    Transient(String),
    Permanent(String),
}

impl ScriptedFault {
    pub fn kind(&self) -> FaultKind {
        match self {
            ScriptedFault::Timeout => FaultKind::Timeout,
            ScriptedFault::Throttled { .. } => FaultKind::Throttled,
            ScriptedFault::PartialWriteThenError => FaultKind::PartialWriteThenError,
            ScriptedFault::FailedConditionalWrite => FaultKind::FailedConditionalWrite,
            ScriptedFault::DuplicateDelivery => FaultKind::DuplicateDelivery,
            ScriptedFault::NotFoundBlip => FaultKind::NotFoundBlip,
            ScriptedFault::CorruptRange => FaultKind::CorruptRange,
            ScriptedFault::EtagChange => FaultKind::EtagChange,
            ScriptedFault::Transient(_) => FaultKind::Transient,
            ScriptedFault::Permanent(_) => FaultKind::Permanent,
        }
    }
}

/// How many matching calls it takes for a [`Rule`] to fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Occurrence {
    /// Fires on every matching call.
    Always,
    /// Fires only on the `n`-th matching call (1-indexed); every other
    /// matching call passes through untouched.
    Nth(u64),
}

/// One scripted rule: matches calls by operation kind and an optional key
/// substring, then fires on the configured [`Occurrence`].
#[derive(Debug, Clone)]
pub struct Rule {
    pub op: Op,
    pub key_contains: Option<String>,
    pub occurrence: Occurrence,
    pub fault: ScriptedFault,
}

impl Rule {
    pub fn new(op: Op, fault: ScriptedFault) -> Self {
        Rule {
            op,
            key_contains: None,
            occurrence: Occurrence::Always,
            fault,
        }
    }

    #[must_use]
    pub fn with_key_contains(mut self, pattern: impl Into<String>) -> Self {
        self.key_contains = Some(pattern.into());
        self
    }

    #[must_use]
    pub fn with_occurrence(mut self, occurrence: Occurrence) -> Self {
        self.occurrence = occurrence;
        self
    }
}

/// One outcome in a [`Sequence`]: either inject a fault or let the call pass
/// straight through to the wrapped backend (a scripted success).
#[derive(Debug, Clone, PartialEq)]
pub enum SequenceStep {
    /// This matching call injects `fault` (exactly as a [`Rule`] would) and is
    /// counted under its `(Op, FaultKind)` pair.
    Fault(ScriptedFault),
    /// This matching call is passed through to the wrapped backend untouched.
    /// No fault counter moves; the sequence still advances by one step.
    Passthrough,
}

/// A scripted, ordered sequence of outcomes for successive matching calls to
/// one `(operation, key matcher)`. Each matching call consumes the next step:
/// step 0 governs the first matching call, step 1 the second, and so on. Once
/// every step is consumed the sequence stops matching and calls fall through
/// to rules and random mode (see [`FaultStore`]'s module docs for precedence).
///
/// Use this for deterministic multi-call failure scenarios that a single
/// [`Rule`] cannot express, where one key must return *different* outcomes on
/// successive calls (retry-budget and fetcher retry/etag reproducers, for
/// example). [`FaultStore::sequence_progress`] reports how many steps a
/// sequence has consumed so a test can assert each step fired.
///
/// # Example
///
/// Script a `get` on any key containing `commit/` to fail with a transient
/// error, then report a `NotFound` blip, then succeed. The fourth and later
/// calls fall through to normal behavior.
///
/// ```
/// use ravel_object_store::fault::{FaultPlan, Op, ScriptedFault, Sequence};
///
/// let plan = FaultPlan::empty().with_sequence(
///     Sequence::new(Op::Get)
///         .with_key_contains("commit/")
///         .then_fault(ScriptedFault::Transient("blip".into()))
///         .then_fault(ScriptedFault::NotFoundBlip)
///         .then_passthrough(),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Sequence {
    pub op: Op,
    pub key_contains: Option<String>,
    pub steps: Vec<SequenceStep>,
}

impl Sequence {
    /// A new, empty sequence matching `op` on any key. Add outcomes with
    /// [`Sequence::then_fault`] / [`Sequence::then_passthrough`], or set them
    /// wholesale via [`Sequence::with_steps`].
    pub fn new(op: Op) -> Self {
        Sequence {
            op,
            key_contains: None,
            steps: Vec::new(),
        }
    }

    /// Restrict this sequence to keys containing `pattern` (same substring
    /// semantics as [`Rule::with_key_contains`]).
    #[must_use]
    pub fn with_key_contains(mut self, pattern: impl Into<String>) -> Self {
        self.key_contains = Some(pattern.into());
        self
    }

    /// Append a fault outcome as the next step.
    #[must_use]
    pub fn then_fault(mut self, fault: ScriptedFault) -> Self {
        self.steps.push(SequenceStep::Fault(fault));
        self
    }

    /// Append a pass-through (scripted success) outcome as the next step.
    #[must_use]
    pub fn then_passthrough(mut self) -> Self {
        self.steps.push(SequenceStep::Passthrough);
        self
    }

    /// Replace the sequence's steps wholesale.
    #[must_use]
    pub fn with_steps(mut self, steps: Vec<SequenceStep>) -> Self {
        self.steps = steps;
        self
    }
}

/// Seeded random fault mode: independent Bernoulli trials per fault kind,
/// tried in the order given, restricted to kinds applicable to the
/// operation being evaluated.
#[derive(Debug, Clone)]
pub struct RandomFaultConfig {
    pub seed: u64,
    /// `(kind, probability in [0, 1])`, tried in order. A `Vec` (not a map)
    /// so iteration order -- and therefore which kind fires when several
    /// probabilities would hit -- is deterministic for a fixed seed.
    pub probabilities: Vec<(FaultKind, f64)>,
}

/// A fault plan: deterministic scripted rules plus an optional random mode.
/// An empty plan (`FaultPlan::default()`) makes `FaultStore` fully
/// transparent: every call passes straight through to the wrapped backend.
#[derive(Debug, Clone, Default)]
pub struct FaultPlan {
    pub rules: Vec<Rule>,
    pub sequences: Vec<Sequence>,
    pub random: Option<RandomFaultConfig>,
}

impl FaultPlan {
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_rule(mut self, rule: Rule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Register an ordered [`Sequence`]. Sequences are evaluated in
    /// registration order and before any [`Rule`]; see the module docs for
    /// precedence. Its registration index (the returned plan's
    /// `sequences.len() - 1`) is the handle used with
    /// [`FaultStore::sequence_progress`].
    #[must_use]
    pub fn with_sequence(mut self, sequence: Sequence) -> Self {
        self.sequences.push(sequence);
        self
    }

    #[must_use]
    pub fn with_random(mut self, config: RandomFaultConfig) -> Self {
        self.random = Some(config);
        self
    }
}

/// A registered hold gate (ADR-0059 decision 5). Matches calls exactly as a
/// [`Rule`] does -- by [`Op`], optional key substring, and [`Occurrence`] --
/// but rather than resolving to an outcome it holds each firing call open
/// until a [`GateHandle`] releases it.
struct Gate {
    op: Op,
    key_contains: Option<String>,
    occurrence: Occurrence,
    /// Matching calls seen so far, to evaluate `Occurrence::Nth`.
    matches: AtomicU64,
}

/// One call currently held by a gate, waiting for its release signal.
struct HeldCall {
    id: u64,
    op: Op,
    key: String,
    /// Sending `()` unblocks the held call. A dropped receiver (the caller
    /// cancelled) just means nobody is waiting; the send fails harmlessly.
    release: oneshot::Sender<()>,
}

/// Shared hold/release state, owned by the `FaultStore` and every
/// [`GateHandle`] it hands out. Independent of the synchronous fault plan:
/// arming a gate never touches rule/sequence/counter state, so a gate and a
/// scripted fault compose on the same call.
#[derive(Default)]
struct GateRegistry {
    gates: Mutex<Vec<Gate>>,
    held: Mutex<Vec<HeldCall>>,
    next_id: AtomicU64,
    /// Notified whenever a call is added to `held`, so
    /// [`GateHandle::wait_until_held`] parks instead of busy-polling.
    changed: Notify,
}

impl GateRegistry {
    /// If a gate matches and fires for this call, register it as held and
    /// return the receiver the caller must await; otherwise return `None` to
    /// pass straight through. Never blocks: the actual await happens in the
    /// caller, so the registry locks are only ever held across in-memory work.
    fn arm(&self, op: Op, key: &str) -> Option<oneshot::Receiver<()>> {
        let should_hold = {
            let gates = self.gates.lock();
            let mut hold = false;
            for gate in gates.iter() {
                if gate.op != op {
                    continue;
                }
                if let Some(pattern) = &gate.key_contains
                    && !key.contains(pattern.as_str())
                {
                    continue;
                }
                // First matching gate governs the call, same precedence rule as
                // rules: it either holds this call or lets it pass.
                let n = gate.matches.fetch_add(1, Ordering::SeqCst) + 1;
                hold = match gate.occurrence {
                    Occurrence::Always => true,
                    Occurrence::Nth(target) => n == target,
                };
                break;
            }
            hold
        };
        if !should_hold {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.held.lock().push(HeldCall {
            id,
            op,
            key: key.to_string(),
            release: tx,
        });
        self.changed.notify_waiters();
        Some(rx)
    }
}

/// Test-side control over the hold gates registered on a [`FaultStore`].
/// Cloneable and `'static` (it shares the store's gate state through an
/// `Arc`), so a test can move clones into spawned tasks while driving release
/// order from the outside. Every handle from one store sees the same held set,
/// regardless of which [`FaultStore::hold`] call produced it.
#[derive(Clone)]
pub struct GateHandle {
    registry: Arc<GateRegistry>,
}

impl GateHandle {
    /// Ids of the calls currently held, in the order they were held (ascending
    /// id). Empty once every held call has been released.
    pub fn held(&self) -> Vec<u64> {
        self.registry.held.lock().iter().map(|c| c.id).collect()
    }

    /// `(id, op, key)` for every currently held call, in hold order, so a test
    /// can pick a specific call to release by the key it matched (e.g. one
    /// shard's commit-record PUT among several concurrently held).
    pub fn held_details(&self) -> Vec<(u64, Op, String)> {
        self.registry
            .held
            .lock()
            .iter()
            .map(|c| (c.id, c.op, c.key.clone()))
            .collect()
    }

    /// How many calls are held right now.
    pub fn held_count(&self) -> usize {
        self.registry.held.lock().len()
    }

    /// Wait until at least `n` calls are held simultaneously. Parks on a
    /// `Notify` woken each time a call is held; no busy-polling.
    pub async fn wait_until_held(&self, n: usize) {
        loop {
            let notified = self.registry.changed.notified();
            tokio::pin!(notified);
            // Register before checking, so a hold landing between the check and
            // the await is not lost.
            notified.as_mut().enable();
            if self.registry.held.lock().len() >= n {
                return;
            }
            notified.await;
        }
    }

    /// Release the held call with `id`, letting its blocked operation proceed
    /// through the rest of the fault plan and into the backend. Returns `true`
    /// if a call with that id was held, `false` if it was not (already
    /// released, or never held).
    pub fn release(&self, id: u64) -> bool {
        let call = {
            let mut held = self.registry.held.lock();
            held.iter()
                .position(|c| c.id == id)
                .map(|pos| held.remove(pos))
        };
        match call {
            Some(call) => {
                let _ = call.release.send(());
                true
            }
            None => false,
        }
    }
}

struct RuleState {
    rule: Rule,
    /// Count of calls that matched this rule's `op` + `key_contains`, used
    /// to evaluate `Occurrence::Nth`.
    hits: AtomicU64,
}

struct SequenceState {
    seq: Sequence,
    /// Count of matching calls seen so far; indexes `seq.steps`. Keeps
    /// incrementing after the last step is consumed (harmless: `steps.get`
    /// then returns `None` and the call falls through), so reporting clamps
    /// it to the step count.
    cursor: AtomicU64,
}

struct RandomState {
    rng: Mutex<StdRng>,
    probabilities: Vec<(FaultKind, f64)>,
}

fn applicable_kinds(op: Op) -> &'static [FaultKind] {
    use FaultKind::*;
    match op {
        Op::Put => &[
            Timeout,
            Throttled,
            PartialWriteThenError,
            FailedConditionalWrite,
            DuplicateDelivery,
            Transient,
            Permanent,
        ],
        Op::Get => &[
            Timeout,
            Throttled,
            NotFoundBlip,
            CorruptRange,
            EtagChange,
            DuplicateDelivery,
            Transient,
            Permanent,
        ],
        Op::Head => &[
            Timeout,
            Throttled,
            NotFoundBlip,
            DuplicateDelivery,
            Transient,
            Permanent,
        ],
        Op::List | Op::Delete => &[Timeout, Throttled, DuplicateDelivery, Transient, Permanent],
    }
}

fn default_fault(kind: FaultKind) -> ScriptedFault {
    match kind {
        FaultKind::Timeout => ScriptedFault::Timeout,
        FaultKind::Throttled => ScriptedFault::Throttled {
            retry_after_ms: 250,
        },
        FaultKind::PartialWriteThenError => ScriptedFault::PartialWriteThenError,
        FaultKind::FailedConditionalWrite => ScriptedFault::FailedConditionalWrite,
        FaultKind::DuplicateDelivery => ScriptedFault::DuplicateDelivery,
        FaultKind::NotFoundBlip => ScriptedFault::NotFoundBlip,
        FaultKind::CorruptRange => ScriptedFault::CorruptRange,
        FaultKind::EtagChange => ScriptedFault::EtagChange,
        FaultKind::Transient => ScriptedFault::Transient("fault: random transient".into()),
        FaultKind::Permanent => ScriptedFault::Permanent("fault: random permanent".into()),
    }
}

/// Wraps any [`ObjectStoreBackend`] and applies a [`FaultPlan`] on top of
/// it. `Send + Sync` as long as `S` is (all interior mutability is via
/// `parking_lot`, which never poisons, so no fault-plan bookkeeping
/// operation can panic on a lock).
pub struct FaultStore<S> {
    inner: S,
    sequences: Vec<SequenceState>,
    rules: Vec<RuleState>,
    random: Option<RandomState>,
    counters: RwLock<HashMap<(Op, FaultKind), u64>>,
    /// Hold-and-release gates (ADR-0059 decision 5). Separate from the fault
    /// plan above: gates are registered at runtime via [`FaultStore::hold`],
    /// not carried in the [`FaultPlan`], and control completion ordering rather
    /// than per-call outcomes.
    gates: Arc<GateRegistry>,
}

impl<S: ObjectStoreBackend> FaultStore<S> {
    pub fn new(inner: S, plan: FaultPlan) -> Self {
        let sequences = plan
            .sequences
            .into_iter()
            .map(|seq| SequenceState {
                seq,
                cursor: AtomicU64::new(0),
            })
            .collect();
        let rules = plan
            .rules
            .into_iter()
            .map(|rule| RuleState {
                rule,
                hits: AtomicU64::new(0),
            })
            .collect();
        let random = plan.random.map(|config| RandomState {
            rng: Mutex::new(StdRng::seed_from_u64(config.seed)),
            probabilities: config.probabilities,
        });
        FaultStore {
            inner,
            sequences,
            rules,
            random,
            counters: RwLock::new(HashMap::new()),
            gates: Arc::new(GateRegistry::default()),
        }
    }

    /// Borrow the wrapped backend, e.g. to assert on its state directly in
    /// tests.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// How many times this `(operation, fault kind)` pair has actually
    /// fired (scripted or random), since this store was created.
    pub fn fault_count(&self, op: Op, kind: FaultKind) -> u64 {
        self.counters.read().get(&(op, kind)).copied().unwrap_or(0)
    }

    /// Snapshot of every counter that has fired at least once.
    pub fn counters_snapshot(&self) -> HashMap<(Op, FaultKind), u64> {
        self.counters.read().clone()
    }

    /// How many steps the sequence registered at `index` (its
    /// [`FaultPlan::with_sequence`] order) has consumed, clamped to the
    /// sequence's step count. Equals the number of matching calls that
    /// sequence governed; when it reaches the step count every step has
    /// fired and further matching calls fall through. Returns `0` for an
    /// out-of-range index.
    pub fn sequence_progress(&self, index: usize) -> u64 {
        self.sequences.get(index).map_or(0, |state| {
            let len = state.seq.steps.len() as u64;
            state.cursor.load(Ordering::SeqCst).min(len)
        })
    }

    fn record(&self, op: Op, kind: FaultKind) {
        *self.counters.write().entry((op, kind)).or_insert(0) += 1;
    }

    /// Register a hold gate that blocks every matching call inside
    /// `FaultStore` until the returned [`GateHandle`] releases it (ADR-0059
    /// decision 5). Matching is identical to a [`Rule`]'s (`op` + optional
    /// `key_contains` + `occurrence`), but a gate holds a concurrently
    /// in-flight call rather than resolving it to an outcome, so it composes
    /// with -- does not replace -- any rule or sequence on the same call.
    /// Registering several gates shares one held-call registry: every returned
    /// handle sees the same held set.
    pub fn hold(&self, op: Op, key_contains: Option<String>, occurrence: Occurrence) -> GateHandle {
        self.gates.gates.lock().push(Gate {
            op,
            key_contains,
            occurrence,
            matches: AtomicU64::new(0),
        });
        GateHandle {
            registry: Arc::clone(&self.gates),
        }
    }

    /// Block this call if a hold gate is armed for it, until released. Called
    /// at the head of every operation, before [`FaultStore::resolve`], so the
    /// gate's hold/release protocol stays independent of the synchronous fault
    /// table: `resolve` still performs no I/O and needs no `await`, and a
    /// released call still flows through whatever rule or sequence governs it.
    async fn gate_hold(&self, op: Op, key: &str) {
        if let Some(rx) = self.gates.arm(op, key) {
            let _ = rx.await;
        }
    }

    /// Decide whether a fault fires for this call, without performing any
    /// I/O. Returns `None` if the call should pass straight through.
    fn resolve(&self, op: Op, key: &str) -> Option<ScriptedFault> {
        for state in &self.sequences {
            if state.seq.op != op {
                continue;
            }
            if let Some(pattern) = &state.seq.key_contains
                && !key.contains(pattern.as_str())
            {
                continue;
            }
            // Matched by op + key. Consume the next step; an exhausted
            // sequence (index past the last step) stops governing and the
            // call falls through to the rule loop below.
            let idx = state.cursor.fetch_add(1, Ordering::SeqCst) as usize;
            match state.seq.steps.get(idx) {
                None => continue,
                Some(SequenceStep::Passthrough) => return None,
                Some(SequenceStep::Fault(fault)) => {
                    self.record(op, fault.kind());
                    return Some(fault.clone());
                }
            }
        }

        for state in &self.rules {
            if state.rule.op != op {
                continue;
            }
            if let Some(pattern) = &state.rule.key_contains
                && !key.contains(pattern.as_str())
            {
                continue;
            }
            // Matched: this rule fully governs the call, whether or not it
            // fires this time.
            let occurrence = state.hits.fetch_add(1, Ordering::SeqCst) + 1;
            let fires = match state.rule.occurrence {
                Occurrence::Always => true,
                Occurrence::Nth(target) => occurrence == target,
            };
            if fires {
                self.record(op, state.rule.fault.kind());
                return Some(state.rule.fault.clone());
            }
            return None;
        }

        let random = self.random.as_ref()?;
        let applicable = applicable_kinds(op);
        let mut rng = random.rng.lock();
        for (kind, probability) in &random.probabilities {
            if !applicable.contains(kind) {
                continue;
            }
            if rng.random_bool(probability.clamp(0.0, 1.0)) {
                let kind = *kind;
                drop(rng);
                self.record(op, kind);
                return Some(default_fault(kind));
            }
        }
        None
    }
}

/// Multipart wrapper that routes each part completion through the gate
/// registry (ADR-0059 decision 5). `put_part` forwards to the inner handle
/// unchanged -- part ordering, size rules, and poisoning stay the inner
/// backend's, so an ungated `FaultStore` is fully transparent (the contract
/// suite runs its whole multipart assertion set against one). `complete` first
/// arms one gate check per submitted part, all concurrently, modeling the
/// parts' independent uploads completing: a gate holding this key blocks every
/// part completion at once, and a test releases them in any order. Assembly
/// order is fixed by submission (the inner handle buffered parts in `put_part`
/// call order), so the completed object is byte-correct regardless of the order
/// completions are released -- the property the object-store contract has
/// always claimed for multipart but nothing drove until now.
struct GatedMultipartUpload<'a> {
    inner: Box<dyn MultipartUpload + 'a>,
    registry: Arc<GateRegistry>,
    key: String,
    /// Count of accepted `put_part` calls, so `complete` arms one gate check
    /// per part.
    parts: u32,
}

#[async_trait::async_trait]
impl MultipartUpload for GatedMultipartUpload<'_> {
    async fn put_part(
        &mut self,
        data: Bytes,
        checksum: Option<crate::UploadChecksum>,
    ) -> Result<(), StoreError> {
        // Forward first: a rejected part (checksum mismatch, sequence-rule
        // violation, backend failure) must not count toward the completion
        // barriers `complete` arms below.
        self.inner.put_part(data, checksum).await?;
        self.parts += 1;
        Ok(())
    }

    async fn complete(&mut self) -> Result<PutOutcome, StoreError> {
        // Arm one gate check per submitted part, concurrently, so a gate on
        // this key holds every part completion simultaneously. `join_all` polls
        // them together; each firing check registers a distinct held call, so a
        // test can wait for all N to be held and release them in any order
        // before assembly proceeds. With no gate armed, every check passes
        // straight through and this is a plain delegated `complete`.
        let barriers = (0..self.parts).map(|_| {
            let registry = Arc::clone(&self.registry);
            let key = self.key.clone();
            async move {
                if let Some(rx) = registry.arm(Op::Put, &key) {
                    let _ = rx.await;
                }
            }
        });
        futures::future::join_all(barriers).await;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

fn duplicate_delivery_error() -> StoreError {
    StoreError::Transient("fault: duplicate delivery, ack lost after apply".into())
}

fn not_applicable(op_name: &str) -> StoreError {
    StoreError::Permanent(format!(
        "fault: scripted fault kind is not applicable to {op_name}"
    ))
}

#[async_trait::async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for FaultStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.gate_hold(Op::Put, key).await;
        match self.resolve(Op::Put, key) {
            None => self.inner.put(key, data, opts).await,
            Some(ScriptedFault::PartialWriteThenError) => Err(StoreError::Transient(
                "fault: partial write, connection dropped before completion (not applied)".into(),
            )),
            Some(ScriptedFault::FailedConditionalWrite) => Err(match &opts.mode {
                PutMode::CreateIfAbsent => StoreError::AlreadyExists,
                PutMode::CasVersion(_) | PutMode::Overwrite => StoreError::PreconditionFailed,
            }),
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.put(key, data, opts).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(ScriptedFault::NotFoundBlip) => Err(StoreError::NotFound),
            Some(ScriptedFault::CorruptRange | ScriptedFault::EtagChange) => {
                Err(not_applicable("put"))
            }
        }
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gate_hold(Op::Get, key).await;
        match self.resolve(Op::Get, key) {
            None => self.inner.get(key, range).await,
            Some(ScriptedFault::NotFoundBlip) => Err(StoreError::NotFound),
            Some(ScriptedFault::CorruptRange) => {
                let mut outcome = self.inner.get(key, range).await?;
                let flipped: Vec<u8> = outcome.data.iter().map(|b| b ^ 0xFF).collect();
                outcome.data = Bytes::from(flipped);
                Ok(outcome)
            }
            Some(ScriptedFault::EtagChange) => {
                let mut outcome = self.inner.get(key, range).await?;
                outcome.etag = crate::Etag("fault: etag changed between reads".into());
                Ok(outcome)
            }
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.get(key, range).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(ScriptedFault::PartialWriteThenError | ScriptedFault::FailedConditionalWrite) => {
                Err(not_applicable("get"))
            }
        }
    }

    /// A multipart upload carries no scripted (`Rule`/`Sequence`) fault today,
    /// but its part completions are gate sites for the hold-and-release
    /// primitive (ADR-0059 decision 5). The returned [`GatedMultipartUpload`]
    /// delegates the whole part sequence to the inner handle, so an ungated
    /// `FaultStore` over a multipart-capable backend still satisfies the
    /// multipart contract its `capabilities()` passthrough claims; when a gate
    /// matches this key, `complete` holds every submitted part's completion
    /// until released. A backend that refuses multipart still refuses here (the
    /// error propagates before any wrapper is built).
    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        let inner = self.inner.put_multipart(key).await?;
        Ok(Box::new(GatedMultipartUpload {
            inner,
            registry: Arc::clone(&self.gates),
            key: key.to_string(),
            parts: 0,
        }))
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.gate_hold(Op::Head, key).await;
        match self.resolve(Op::Head, key) {
            None => self.inner.head(key).await,
            Some(ScriptedFault::NotFoundBlip) => Err(StoreError::NotFound),
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.head(key).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(
                ScriptedFault::PartialWriteThenError
                | ScriptedFault::FailedConditionalWrite
                | ScriptedFault::CorruptRange
                | ScriptedFault::EtagChange,
            ) => Err(not_applicable("head")),
        }
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.gate_hold(Op::List, prefix).await;
        match self.resolve(Op::List, prefix) {
            None => self.inner.list(prefix, page).await,
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.list(prefix, page).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(_) => Err(not_applicable("list")),
        }
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.gate_hold(Op::List, prefix).await;
        match self.resolve(Op::List, prefix) {
            None => self.inner.list_delimited(prefix).await,
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.list_delimited(prefix).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(_) => Err(not_applicable("list_delimited")),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.gate_hold(Op::Delete, key).await;
        match self.resolve(Op::Delete, key) {
            None => self.inner.delete(key).await,
            Some(ScriptedFault::DuplicateDelivery) => {
                self.inner.delete(key).await?;
                Err(duplicate_delivery_error())
            }
            Some(ScriptedFault::Timeout) => Err(StoreError::Timeout),
            Some(ScriptedFault::Throttled { retry_after_ms }) => {
                Err(StoreError::Throttled { retry_after_ms })
            }
            Some(ScriptedFault::Transient(msg)) => Err(StoreError::Transient(msg)),
            Some(ScriptedFault::Permanent(msg)) => Err(StoreError::Permanent(msg)),
            Some(_) => Err(not_applicable("delete")),
        }
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::memory::MemoryStore;

    #[tokio::test]
    async fn empty_plan_is_fully_transparent() {
        let store = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("put");
        let got = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&got.data[..], b"v");
        assert_eq!(store.counters_snapshot().len(), 0);
    }

    #[tokio::test]
    async fn partial_write_then_error_leaves_no_visible_object() {
        let plan =
            FaultPlan::empty().with_rule(Rule::new(Op::Put, ScriptedFault::PartialWriteThenError));
        let store = FaultStore::new(MemoryStore::new(), plan);
        let err = store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect_err("partial write must fail");
        assert!(matches!(err, StoreError::Transient(_)));
        assert!(matches!(
            store.inner().head("k").await,
            Err(StoreError::NotFound)
        ));
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::PartialWriteThenError),
            1
        );
    }

    #[tokio::test]
    async fn failed_conditional_write_maps_by_mode_without_applying() {
        let plan =
            FaultPlan::empty().with_rule(Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite));
        let store = FaultStore::new(MemoryStore::new(), plan);
        let err = store
            .put(
                "k",
                Bytes::from_static(b"v"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect_err("must fail");
        assert!(matches!(err, StoreError::AlreadyExists));
        assert!(matches!(
            store.inner().head("k").await,
            Err(StoreError::NotFound)
        ));
    }

    #[tokio::test]
    async fn duplicate_delivery_applies_put_exactly_once() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::DuplicateDelivery)
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        let err = store
            .put("k", Bytes::from_static(b"v1"), PutOptions::default())
            .await
            .expect_err("first put must report duplicate-delivery transient error");
        assert!(matches!(err, StoreError::Transient(_)));
        // The op WAS applied despite the error.
        let got = store
            .get("k", GetRange::Full)
            .await
            .expect("get after duplicate delivery");
        assert_eq!(&got.data[..], b"v1");
        // Second call: rule already fired once (Nth(1)), passes through cleanly.
        store
            .put("k", Bytes::from_static(b"v2"), PutOptions::default())
            .await
            .expect("second put is unaffected by the rule");
        let got = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&got.data[..], b"v2");
        assert_eq!(store.fault_count(Op::Put, FaultKind::DuplicateDelivery), 1);
    }

    #[tokio::test]
    async fn duplicate_delivery_applies_delete_exactly_once() {
        let inner = MemoryStore::new();
        inner
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");
        let plan =
            FaultPlan::empty().with_rule(Rule::new(Op::Delete, ScriptedFault::DuplicateDelivery));
        let store = FaultStore::new(inner, plan);
        let err = store.delete("k").await.expect_err("must report transient");
        assert!(matches!(err, StoreError::Transient(_)));
        assert!(matches!(
            store.inner().head("k").await,
            Err(StoreError::NotFound)
        ));
    }

    #[tokio::test]
    async fn nth_occurrence_fires_on_the_right_call() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_occurrence(Occurrence::Nth(3)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");
        for i in 1..=5u32 {
            let result = store.get("k", GetRange::Full).await;
            if i == 3 {
                assert!(
                    matches!(result, Err(StoreError::NotFound)),
                    "call {i} should fail"
                );
            } else {
                assert!(result.is_ok(), "call {i} should succeed, got {result:?}");
            }
        }
        assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    }

    #[tokio::test]
    async fn corrupt_range_flips_bytes_only_when_scripted() {
        let inner = MemoryStore::new();
        inner
            .put("k", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("seed");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::CorruptRange).with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);
        let corrupted = store.get("k", GetRange::Full).await.expect("get");
        let expected: Vec<u8> = b"hello".iter().map(|b| b ^ 0xFF).collect();
        assert_eq!(&corrupted.data[..], expected.as_slice());
        // Second call: rule already fired once, passes through clean.
        let clean = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&clean.data[..], b"hello");
        assert_eq!(store.fault_count(Op::Get, FaultKind::CorruptRange), 1);
    }

    #[tokio::test]
    async fn etag_change_returns_real_bytes_under_a_new_etag() {
        let inner = MemoryStore::new();
        inner
            .put("k", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("seed");
        let clean_etag = inner.get("k", GetRange::Full).await.expect("seed get").etag;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::EtagChange).with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(inner, plan);

        // First read: rule has not fired, real etag and bytes pass through.
        let first = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&first.data[..], b"hello");
        assert_eq!(first.etag, clean_etag);

        // Second read: bytes are still real, but the etag has changed, so a
        // snapshot-pinning caller can detect the concurrent overwrite.
        let second = store.get("k", GetRange::Full).await.expect("get");
        assert_eq!(&second.data[..], b"hello");
        assert_ne!(second.etag, clean_etag);
        assert_eq!(store.fault_count(Op::Get, FaultKind::EtagChange), 1);
    }

    #[tokio::test]
    async fn etag_change_in_a_sequence_flips_only_the_scripted_read() {
        // The shape the fetcher's EtagChanged guard needs: first read pins an
        // etag (passthrough), a later read returns fresh bytes under a new
        // etag (the concurrent overwrite).
        let inner = MemoryStore::new();
        inner
            .put("seg", Bytes::from_static(b"payload"), PutOptions::default())
            .await
            .expect("seed");
        let pinned = inner
            .get("seg", GetRange::Full)
            .await
            .expect("seed get")
            .etag;
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains("seg")
                .then_passthrough()
                .then_fault(ScriptedFault::EtagChange),
        );
        let store = FaultStore::new(inner, plan);

        let a = store.get("seg", GetRange::Full).await.expect("get");
        assert_eq!(a.etag, pinned);
        assert_eq!(store.sequence_progress(0), 1);
        let b = store.get("seg", GetRange::Full).await.expect("get");
        assert_ne!(b.etag, pinned, "the scripted read must change the etag");
        assert_eq!(&b.data[..], b"payload", "but keep the real bytes");
        assert_eq!(store.sequence_progress(0), 2);
        assert_eq!(store.fault_count(Op::Get, FaultKind::EtagChange), 1);
    }

    #[tokio::test]
    async fn etag_change_is_not_applicable_to_non_get_ops() {
        let plan = FaultPlan::empty().with_rule(Rule::new(Op::Head, ScriptedFault::EtagChange));
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");
        assert!(matches!(
            store.head("k").await,
            Err(StoreError::Permanent(_))
        ));
    }

    #[tokio::test]
    async fn key_pattern_only_matches_scoped_keys() {
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains("secret"));
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("secret/1", Bytes::from_static(b"a"), PutOptions::default())
            .await
            .expect("seed");
        store
            .put("public/1", Bytes::from_static(b"b"), PutOptions::default())
            .await
            .expect("seed");
        assert!(matches!(
            store.get("secret/1", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
        assert!(store.get("public/1", GetRange::Full).await.is_ok());
    }

    #[tokio::test]
    async fn counters_count_every_fired_fault() {
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::Timeout))
            .with_rule(Rule::new(Op::Head, ScriptedFault::Permanent("nope".into())));
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");
        for _ in 0..3 {
            assert!(matches!(
                store.get("k", GetRange::Full).await,
                Err(StoreError::Timeout)
            ));
        }
        assert!(matches!(
            store.head("k").await,
            Err(StoreError::Permanent(_))
        ));
        assert_eq!(store.fault_count(Op::Get, FaultKind::Timeout), 3);
        assert_eq!(store.fault_count(Op::Head, FaultKind::Permanent), 1);
        assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 0);
    }

    #[tokio::test]
    async fn seeded_random_mode_is_deterministic_for_a_fixed_seed() {
        async fn run(store: &FaultStore<MemoryStore>) -> Vec<bool> {
            let mut out = Vec::new();
            for i in 0..20 {
                let key = format!("k{i}");
                let result = store.get(&key, GetRange::Full).await;
                out.push(matches!(result, Err(StoreError::Timeout)));
            }
            out
        }
        let make_plan = || {
            FaultPlan::empty().with_random(RandomFaultConfig {
                seed: 42,
                probabilities: vec![(FaultKind::Timeout, 0.5)],
            })
        };
        let store_a = FaultStore::new(MemoryStore::new(), make_plan());
        let store_b = FaultStore::new(MemoryStore::new(), make_plan());
        let outcomes_a = run(&store_a).await;
        let outcomes_b = run(&store_b).await;
        assert_eq!(outcomes_a, outcomes_b);
        // Not a degenerate all-same-result run (seed 42 / p=0.5 over 20 draws).
        assert!(outcomes_a.contains(&true));
        assert!(outcomes_a.contains(&false));
    }

    #[tokio::test]
    async fn sequence_fires_outcomes_in_order_then_falls_through() {
        // a4-F02 shape: a transient blip, then a NotFound blip, then success
        // on the same key.
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains("commit/")
                .then_fault(ScriptedFault::Transient("blip".into()))
                .then_fault(ScriptedFault::NotFoundBlip)
                .then_passthrough(),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("commit/1", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");

        assert!(matches!(
            store.get("commit/1", GetRange::Full).await,
            Err(StoreError::Transient(_))
        ));
        assert_eq!(store.sequence_progress(0), 1);
        assert!(matches!(
            store.get("commit/1", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
        assert_eq!(store.sequence_progress(0), 2);
        let ok = store
            .get("commit/1", GetRange::Full)
            .await
            .expect("third call is the scripted passthrough");
        assert_eq!(&ok.data[..], b"v");
        assert_eq!(store.sequence_progress(0), 3);

        // Exhausted: further calls fall through to the real backend.
        let ok = store
            .get("commit/1", GetRange::Full)
            .await
            .expect("fourth call passes through, sequence exhausted");
        assert_eq!(&ok.data[..], b"v");
        assert_eq!(
            store.sequence_progress(0),
            3,
            "progress clamps at the step count once exhausted"
        );

        // Each fault step fired exactly once; the passthrough moved no counter.
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    }

    #[tokio::test]
    async fn exhausted_sequence_falls_through_to_rules() {
        // A sequence and a rule match the same (op, key). The sequence governs
        // while it has steps; once exhausted the rule takes over every call.
        let plan = FaultPlan::empty()
            .with_sequence(Sequence::new(Op::Get).then_fault(ScriptedFault::Timeout))
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip));
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");

        assert!(matches!(
            store.get("k", GetRange::Full).await,
            Err(StoreError::Timeout)
        ));
        for _ in 0..3 {
            assert!(matches!(
                store.get("k", GetRange::Full).await,
                Err(StoreError::NotFound)
            ));
        }
        assert_eq!(store.sequence_progress(0), 1);
        assert_eq!(store.fault_count(Op::Get, FaultKind::Timeout), 1);
        assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 3);
    }

    #[tokio::test]
    async fn sequence_key_pattern_only_scopes_matching_keys() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains("secret")
                .then_fault(ScriptedFault::NotFoundBlip),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("secret/1", Bytes::from_static(b"a"), PutOptions::default())
            .await
            .expect("seed");
        store
            .put("public/1", Bytes::from_static(b"b"), PutOptions::default())
            .await
            .expect("seed");

        // A non-matching key never advances the sequence.
        assert!(store.get("public/1", GetRange::Full).await.is_ok());
        assert_eq!(store.sequence_progress(0), 0);
        // The matching key consumes step 0.
        assert!(matches!(
            store.get("secret/1", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));
        assert_eq!(store.sequence_progress(0), 1);
        assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
    }

    #[tokio::test]
    async fn multiple_sequences_consume_in_registration_order() {
        let plan = FaultPlan::empty()
            .with_sequence(Sequence::new(Op::Head).then_fault(ScriptedFault::Timeout))
            .with_sequence(Sequence::new(Op::Head).then_fault(ScriptedFault::NotFoundBlip));
        let store = FaultStore::new(MemoryStore::new(), plan);
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");

        // The first-registered sequence governs until exhausted, then the next.
        assert!(matches!(store.head("k").await, Err(StoreError::Timeout)));
        assert!(matches!(store.head("k").await, Err(StoreError::NotFound)));
        // Both exhausted: the call reaches the real backend.
        assert!(store.head("k").await.is_ok());
        assert_eq!(store.sequence_progress(0), 1);
        assert_eq!(store.sequence_progress(1), 1);
    }

    #[tokio::test]
    async fn sequences_default_empty_and_do_not_disturb_rules() {
        assert!(FaultPlan::empty().sequences.is_empty());

        // A Put sequence and a Get rule coexist without interfering; existing
        // rule/occurrence semantics are unchanged by the presence of a
        // sequence on a different operation.
        let plan = FaultPlan::empty()
            .with_sequence(
                Sequence::new(Op::Put)
                    .then_fault(ScriptedFault::Timeout)
                    .then_passthrough(),
            )
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip));
        let store = FaultStore::new(MemoryStore::new(), plan);

        // Put step 0 fires.
        assert!(matches!(
            store
                .put("k", Bytes::from_static(b"v"), PutOptions::default())
                .await,
            Err(StoreError::Timeout)
        ));
        // Put step 1 passes through: the object lands.
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("second put is the scripted passthrough");
        // The Get rule fires exactly as it does without any sequence present.
        assert!(matches!(
            store.get("k", GetRange::Full).await,
            Err(StoreError::NotFound)
        ));

        assert_eq!(store.sequence_progress(0), 2);
        assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);
        assert_eq!(store.fault_count(Op::Get, FaultKind::NotFoundBlip), 1);
        // The out-of-range index is safe and reports zero.
        assert_eq!(store.sequence_progress(99), 0);
    }

    #[tokio::test]
    async fn hold_gate_blocks_a_matching_call_until_released() {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed happens before the gate is registered, so it is not held");
        let gate = store.hold(Op::Get, Some("k".into()), Occurrence::Always);

        let task_store = Arc::clone(&store);
        let task = tokio::spawn(async move { task_store.get("k", GetRange::Full).await });

        // The get is genuinely in flight and parked inside the gate.
        gate.wait_until_held(1).await;
        assert_eq!(gate.held_count(), 1);
        let ids = gate.held();
        assert_eq!(ids.len(), 1);

        // Releasing an unknown id is a no-op; releasing the held one unblocks it.
        assert!(!gate.release(ids[0] + 999));
        assert!(gate.release(ids[0]));
        let got = task.await.expect("join").expect("get after release");
        assert_eq!(&got.data[..], b"v");
        assert_eq!(gate.held_count(), 0);
        // A second release of the same id finds nothing left.
        assert!(!gate.release(ids[0]));
    }

    #[tokio::test]
    async fn hold_gate_releases_concurrent_calls_in_any_order() {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let gate = store.hold(Op::Put, Some("part".into()), Occurrence::Always);

        // Three puts issued concurrently, every one held at once.
        let mut tasks = Vec::new();
        for i in 0..3u8 {
            let task_store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                task_store
                    .put(
                        &format!("part-{i}"),
                        Bytes::copy_from_slice(&[i]),
                        PutOptions::default(),
                    )
                    .await
            }));
        }
        gate.wait_until_held(3).await;
        let ids = gate.held();
        assert_eq!(ids.len(), 3, "all three puts are held simultaneously");

        // Release middle, then last, then first: order fully under test control.
        assert!(gate.release(ids[1]));
        assert!(gate.release(ids[2]));
        assert!(gate.release(ids[0]));
        for task in tasks {
            task.await.expect("join").expect("put after release");
        }
        assert_eq!(gate.held_count(), 0);

        // Every object landed, regardless of the order completions were released.
        for i in 0..3u8 {
            let got = store
                .get(&format!("part-{i}"), GetRange::Full)
                .await
                .expect("get");
            assert_eq!(&got.data[..], &[i]);
        }
    }

    #[tokio::test]
    async fn hold_gate_nth_holds_only_the_target_call() {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        store
            .put("k", Bytes::from_static(b"v"), PutOptions::default())
            .await
            .expect("seed");
        // Only the 2nd matching get is held; the 1st and 3rd pass straight through.
        let gate = store.hold(Op::Get, Some("k".into()), Occurrence::Nth(2));

        store
            .get("k", GetRange::Full)
            .await
            .expect("first get passes");
        assert_eq!(gate.held_count(), 0);

        let task_store = Arc::clone(&store);
        let held = tokio::spawn(async move { task_store.get("k", GetRange::Full).await });
        gate.wait_until_held(1).await;
        assert_eq!(gate.held_count(), 1);

        // A concurrent 3rd get is not held and completes on its own.
        store
            .get("k", GetRange::Full)
            .await
            .expect("third get passes");
        assert_eq!(gate.held_count(), 1);

        let ids = gate.held();
        assert!(gate.release(ids[0]));
        held.await.expect("join").expect("second get after release");
        assert_eq!(gate.held_count(), 0);
    }
}
