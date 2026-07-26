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
//! Deliberately out of scope: "reordered completion" is mentioned as an
//! aspiration in the contract doc's implementation summary but is not part
//! of this crate's fault-plan surface (see the issue's deliverable list).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutMode, PutOptions, PutOutcome, StoreError,
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

    #[must_use]
    pub fn with_random(mut self, config: RandomFaultConfig) -> Self {
        self.random = Some(config);
        self
    }
}

struct RuleState {
    rule: Rule,
    /// Count of calls that matched this rule's `op` + `key_contains`, used
    /// to evaluate `Occurrence::Nth`.
    hits: AtomicU64,
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
    rules: Vec<RuleState>,
    random: Option<RandomState>,
    counters: RwLock<HashMap<(Op, FaultKind), u64>>,
}

impl<S: ObjectStoreBackend> FaultStore<S> {
    pub fn new(inner: S, plan: FaultPlan) -> Self {
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
            rules,
            random,
            counters: RwLock::new(HashMap::new()),
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

    fn record(&self, op: Op, kind: FaultKind) {
        *self.counters.write().entry((op, kind)).or_insert(0) += 1;
    }

    /// Decide whether a fault fires for this call, without performing any
    /// I/O. Returns `None` if the call should pass straight through.
    fn resolve(&self, op: Op, key: &str) -> Option<ScriptedFault> {
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
            Some(ScriptedFault::CorruptRange) => Err(not_applicable("put")),
        }
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        match self.resolve(Op::Get, key) {
            None => self.inner.get(key, range).await,
            Some(ScriptedFault::NotFoundBlip) => Err(StoreError::NotFound),
            Some(ScriptedFault::CorruptRange) => {
                let mut outcome = self.inner.get(key, range).await?;
                let flipped: Vec<u8> = outcome.data.iter().map(|b| b ^ 0xFF).collect();
                outcome.data = Bytes::from(flipped);
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

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
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
                | ScriptedFault::CorruptRange,
            ) => Err(not_applicable("head")),
        }
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
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
}
