//! Fault-schedule generator (ADR-0068 decision 4).
//!
//! From the master seed this derives a per-run [`FaultSchedule`]: a
//! [`FaultPlan`] of scripted rules (operation kind + key substring + Nth
//! occurrence) plus a list of hold/release [`GateScript`]s. Same seed, same
//! plan, byte for byte -- the whole point of the harness is that a reported
//! seed replays the identical fault interleaving.
//!
//! # Why only retryable PUT faults on `/l0/` and `/c/`
//!
//! The acceptance test (`compaction_equivalence_under_faults`) requires two
//! things at once: every generated fault actually fires (its FaultStore
//! counter moves), *and* every system invariant stays green. That rules out
//! any fault the exercised code path does not recover from. The one class
//! that satisfies both here is a **retryable** [`ScriptedFault`]
//! (`Transient`/`Throttled`, see `StoreError::is_retryable`) on the ingest
//! write path, because `ravel-ingest`'s shard actor retries both the data
//! object PUT (`put_data_object_with_retry`) and the commit-record publish
//! (`publish_with_retry`, over `ravel_commit::publish`'s
//! `CreateIfAbsent`-with-backoff) up to a bounded budget. A single
//! `Occurrence::Nth` fault is absorbed by one retry and leaves the stored
//! bytes, and therefore every query result, unchanged.
//!
//! The two safe targets are the object key substrings the ingest path writes:
//! - [`L0_DATA_SUBSTR`] (`/l0/`) -- the RSEG L0 data objects.
//! - [`COMMIT_SUBSTR`] (`/c/`) -- the L0 commit records.
//!
//! Faults are deliberately *not* placed on GET (the query and compaction read
//! paths do not all retry) or on the `l1/` parts by the rules below: a fault
//! there would fail a phase this ingest-only rule set has no retry hook for and
//! turn a green invariant red for reasons unrelated to the bug classes this
//! task hunts. The compaction, sweep, and fold phases get their own retryable
//! faults -- including LIST and DELETE on the sweep path and PUT on the catalog
//! snapshot prefix -- from the phase-isolated schedules in
//! [`generate_compaction_faults`] and [`generate_fold_fault`], never from the
//! rules below. `Occurrence::Nth` (rather than `Always`) is used so each rule
//! fires exactly once, during ingest, and is fully consumed before the
//! fold/compact/sweep phases, so a leftover rule can never fire on a
//! compaction-record PUT that has no retry hook of its own.
//!
//! # Is `Catalog::fold` idempotent on retry after a throttled catalog PUT?
//!
//! Yes, and the fold-phase rule in [`generate_fold_fault`] rests on that
//! decision: the driver retries the fold once and requires the post-fold cycle
//! digest to equal the digest of the same seed run with no faults at all
//! (`tests/compaction_phase_faults.rs`,
//! `catalog_put_fault_on_fold_recovers_or_returns_typed_error`).
//!
//! The evidence is in `crates/ravel-catalog/src/fold.rs`, read as of this
//! commit:
//!
//! - A snapshot part is PUT under a content-addressed key
//!   (`part_object_key`, keyed on the blake3 of the part's own encoded bytes)
//!   with `PutOptions::create_if_absent()`, and the call site treats
//!   `StoreError::AlreadyExists` as success ("bytes are identical by
//!   construction"). A re-run that recomputes the same part from the same
//!   commit layout therefore converges whether or not the first attempt's
//!   bytes landed. `docs/catalog-and-mvcc.md` states the same rule
//!   normatively: "two folders that fold the same input independently write
//!   the same key and `PutMode::CreateIfAbsent` `AlreadyExists` is idempotent
//!   success".
//! - Any other PUT error on that path returns `CatalogError::Store(e)`
//!   immediately: `fold_inner` has no internal retry, so a throttled catalog
//!   PUT is already a *typed* error at the `Catalog::fold` boundary, never a
//!   panic and never a partial success.
//! - The HEAD CAS is the fold's last write (`head_put_result`, under
//!   `PutMode::CasVersion`/`CreateIfAbsent`), and the typed error above
//!   returns before it. So a faulted attempt publishes nothing a reader can
//!   reach: HEAD still names the pre-fold snapshot, and the orphaned part (if
//!   any landed) is exactly the "orphan part" crash case
//!   `sweep_unreferenced_catalog_objects` already collects.
//! - Under this harness the faulted PUT does not even reach the backend:
//!   `FaultStore::put` short-circuits a `Throttled`/`Transient` scripted fault
//!   before calling the wrapped store (`crates/ravel-object-store/src/fault.rs`),
//!   so the retry's PUT lands as a plain create.
//!
//! The retry is bounded and the non-recoverable branch is preserved: an
//! `Occurrence::Always` fault on the same target exhausts the budget and
//! surfaces a typed `CycleError::Fold`, which is the flip that proves the
//! recover branch is not vacuous.

use rand::RngExt;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault, Sequence, SequenceStep,
};

use crate::seed::MasterSeed;

/// Key substring matching the RSEG L0 data objects an ingest flush writes
/// (`t/<tenant>/<signal>/l0/<shard>/...`).
pub const L0_DATA_SUBSTR: &str = "/l0/";
/// Key substring matching the L0 commit records an ingest flush publishes
/// (`t/<tenant>/<signal>/c/<shard>/<hour>/...`). Also matches the compaction
/// record and retention tombstone, which is why every rule keyed on it uses
/// [`Occurrence::Nth`]: the ingest-plan rules are consumed during ingest, and
/// the sweep-plan delete rule on the first commit-record delete of a sweep
/// pass (see the module docs).
pub const COMMIT_SUBSTR: &str = "/c/";
/// Key substring matching every object a compaction publishes and nothing an
/// ingest flush writes: the `l1/` part directory (`.../l1/<shard>/...`) and
/// the `l1.`-tagged compaction record filename (`.../c/<shard>/<hour>/l1.<hash>.cmt`)
/// both contain `/l1`, while L0 data (`/l0/`) and L0 commit records
/// (`<writer>.<epoch>.<seq>.cmt`) never do. A `Put` rule keyed on this fires
/// only on the compaction write path.
pub const L1_SUBSTR: &str = "/l1";
/// Key substring matching the catalog snapshot parts a fold publishes
/// (`t/<tenant>/catalog/<signal>/snap/<watermark>.<hash>.csnap`, see
/// `part_object_key` in `crates/ravel-catalog/src/fold.rs`) and nothing any
/// other phase writes. The commit records (`.../c/<shard>/<hour>/<name>.cmt`),
/// the L0 data objects (`.../l0/...`), and the compaction outputs (`.../l1...`)
/// contain no `/snap/` segment, so a `Put` rule keyed on this can never reach a
/// commit-record PUT under any seed -- the phase isolation the module docs
/// require. It is also narrower than the whole `catalog/` prefix on purpose:
/// the postings object (`catalog/<signal>/idx/*.npost`) has a PUT failure path
/// that warns and folds on without the index rather than returning an error,
/// so a fault there would exercise a different contract than the
/// recover-or-typed-error one this rule claims.
pub const CATALOG_SNAPSHOT_SUBSTR: &str = "/snap/";

/// One hold/release gate the schedule can arm on a [`FaultStore`], holding
/// each matching call open until released (ADR-0059 decision 5). Matching is
/// identical to a [`Rule`]'s: operation kind, optional key substring, and
/// [`Occurrence`].
///
/// [`FaultStore`]: ravel_object_store::fault::FaultStore
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateScript {
    pub op: Op,
    pub key_contains: Option<String>,
    pub occurrence: Occurrence,
}

/// Knobs for [`generate`]. Every field is read deterministically from the
/// `"faults"` sub-seed; two calls with the same [`MasterSeed`] and config
/// produce an identical [`FaultSchedule`].
#[derive(Debug, Clone)]
pub struct FaultScheduleConfig {
    /// Upper bound on scripted rules emitted. Capped internally at the number
    /// of distinct safe `(op, key)` targets (currently two: `/l0/` and
    /// `/c/`), because the [`FaultStore`] lets only the first matching rule
    /// govern a call, so two rules on the same target would leave the second
    /// permanently unfired.
    ///
    /// [`FaultStore`]: ravel_object_store::fault::FaultStore
    pub max_rules: usize,
    /// Upper bound on hold/release gates emitted.
    pub max_gates: usize,
}

impl Default for FaultScheduleConfig {
    fn default() -> Self {
        FaultScheduleConfig {
            max_rules: 2,
            max_gates: 1,
        }
    }
}

/// A deterministic, seed-derived fault schedule.
#[derive(Debug, Clone)]
pub struct FaultSchedule {
    /// The scripted plan handed to [`FaultStore::new`] for the ingest phase.
    ///
    /// [`FaultStore::new`]: ravel_object_store::fault::FaultStore::new
    pub plan: FaultPlan,
    /// The scripted plan the driver wraps only around `compact_bucket`
    ///: a partial write on the L1 write path and a missing-object
    /// blip on the L0 read path. Isolated to the compaction phase by the
    /// dedicated [`FaultStore`] the driver builds from it, and recovered by the
    /// driver's idempotent re-run, so a single firing surfaces a typed
    /// retryable error the re-run absorbs.
    ///
    /// [`FaultStore`]: ravel_object_store::fault::FaultStore
    pub compact_plan: FaultPlan,
    /// The scripted plan the driver wraps only around the sweep's action
    /// pass: a retryable failure on the sweep's paginated listing, and a
    /// retryable failure on the first commit-record delete of rule 2's phase
    /// C (the first delete the sweep issues), so the re-run re-gathers the same
    /// cleared groups and deletes them through rule 2.
    pub sweep_plan: FaultPlan,
    /// The scripted plan the driver wraps only around `Catalog::fold`: one
    /// retryable PUT on the catalog snapshot prefix
    /// ([`CATALOG_SNAPSHOT_SUBSTR`]). Isolated to the fold phase by the
    /// dedicated [`FaultStore`] the driver builds from it, and recovered by the
    /// driver's bounded fold re-run, which the module docs justify from the
    /// fold's own code.
    ///
    /// [`FaultStore`]: ravel_object_store::fault::FaultStore
    pub fold_plan: FaultPlan,
    /// Hold/release gates the driver may arm on the store.
    pub gates: Vec<GateScript>,
    /// The `(Op, FaultKind)` pair each emitted ingest-phase rule injects, in
    /// emission order. A test asserts the store's counter for each pair fired
    /// at least once, proving the faults were actually injected rather than
    /// silently skipped.
    pub expected_faults: Vec<(Op, FaultKind)>,
    /// The `(Op, FaultKind)` pairs the [`compact_plan`]/[`sweep_plan`] rules
    /// inject, in emission order. The driver merges these into
    /// [`crate::driver::CycleOutcome::expected_faults`] so the same
    /// "every expected fault fired" acceptance assertion covers the
    /// compaction/sweep phase.
    ///
    /// [`compact_plan`]: FaultSchedule::compact_plan
    /// [`sweep_plan`]: FaultSchedule::sweep_plan
    pub expected_compaction_faults: Vec<(Op, FaultKind)>,
    /// The `(Op, FaultKind)` the [`fold_plan`] rule injects, or `None` for a
    /// schedule that arms no fold fault ([`FaultSchedule::none`]). The driver
    /// merges it into [`crate::driver::CycleOutcome::expected_faults`] and
    /// reports the exact number of times it fired in
    /// [`crate::driver::CycleOutcome::fold_faults_fired`].
    ///
    /// [`fold_plan`]: FaultSchedule::fold_plan
    pub expected_fold_fault: Option<(Op, FaultKind)>,
}

impl FaultSchedule {
    /// A schedule that injects nothing: a fully transparent [`FaultStore`].
    pub fn none() -> Self {
        FaultSchedule {
            plan: FaultPlan::empty(),
            compact_plan: FaultPlan::empty(),
            sweep_plan: FaultPlan::empty(),
            fold_plan: FaultPlan::empty(),
            gates: Vec::new(),
            expected_faults: Vec::new(),
            expected_compaction_faults: Vec::new(),
            expected_fold_fault: None,
        }
    }

    /// Register this schedule's [`GateScript`]s on `store`, returning one
    /// [`GateHandle`] per gate in emission order. Every returned handle shares
    /// the store's single held-call registry.
    ///
    /// [`GateHandle`]: ravel_object_store::fault::GateHandle
    pub fn arm_gates<S>(
        &self,
        store: &ravel_object_store::fault::FaultStore<S>,
    ) -> Vec<ravel_object_store::fault::GateHandle>
    where
        S: ravel_object_store::ObjectStoreBackend,
    {
        self.gates
            .iter()
            .map(|g| store.hold(g.op, g.key_contains.clone(), g.occurrence))
            .collect()
    }
}

/// The safe `(op, key substring)` targets a scripted rule may use, in a fixed
/// order. See the module docs for why the set is exactly these two.
const SAFE_TARGETS: [(Op, &str); 2] = [(Op::Put, L0_DATA_SUBSTR), (Op::Put, COMMIT_SUBSTR)];

/// Generate a [`FaultSchedule`] from `master_seed`'s `"faults"` sub-seed.
/// Fully deterministic: no OS entropy and no wall clock.
pub fn generate(master_seed: &MasterSeed, config: &FaultScheduleConfig) -> FaultSchedule {
    let mut rng = master_seed.rng("faults");

    // Pick a subset of the safe targets, preserving their fixed order so the
    // choice is a deterministic function of the seed alone. At least one is
    // always chosen, so a run always injects something.
    let want = config.max_rules.clamp(1, SAFE_TARGETS.len());
    let mut chosen: Vec<(Op, &str)> = Vec::new();
    for target in SAFE_TARGETS.into_iter().take(want) {
        if rng.random_bool(0.7) {
            chosen.push(target);
        }
    }
    if chosen.is_empty() {
        let idx = rng.random_range(0..want);
        chosen.push(SAFE_TARGETS[idx]);
    }

    let mut plan = FaultPlan::empty();
    let mut expected_faults = Vec::new();
    for (op, key) in &chosen {
        // A retryable fault only: Transient or Throttled. Both are absorbed by
        // the ingest path's bounded retry, so the write still lands.
        let fault = if rng.random_bool(0.5) {
            ScriptedFault::Transient(format!("sim fault schedule: transient on {key}"))
        } else {
            ScriptedFault::Throttled { retry_after_ms: 50 }
        };
        // Nth in 1..=2 so the rule fires exactly once, early, and well within
        // the retry budget (put_retry_max_attempts defaults to 4).
        let nth = rng.random_range(1u64..=2);
        expected_faults.push((*op, fault.kind()));
        plan = plan.with_rule(
            Rule::new(*op, fault)
                .with_key_contains(*key)
                .with_occurrence(Occurrence::Nth(nth)),
        );
    }

    let gates = generate_gates(&mut rng, config, &chosen);

    // Compaction/sweep-phase faults, drawn from the SAME `"faults"` sub-seed
    // after the ingest rules and gates so the ingest schedule stays byte-for-byte
    // what it was before this addition.
    let (compact_plan, sweep_plan, expected_compaction_faults) =
        generate_compaction_faults(&mut rng);

    // Fold-phase fault, drawn last for the same reason: appending the draw
    // leaves every earlier phase's schedule byte-for-byte what it was.
    let (fold_plan, expected_fold_fault) = generate_fold_fault(&mut rng);

    FaultSchedule {
        plan,
        compact_plan,
        sweep_plan,
        fold_plan,
        gates,
        expected_faults,
        expected_compaction_faults,
        expected_fold_fault: Some(expected_fold_fault),
    }
}

/// Derive the fold-phase fault plan: one retryable PUT on the catalog
/// snapshot prefix ([`CATALOG_SNAPSHOT_SUBSTR`]), `Occurrence::Nth(1)` so it
/// fires exactly once per cycle, on the first snapshot part the first fold
/// publishes. Only the flavor (transient vs throttled) varies with the seed.
///
/// Phase isolation is by construction, as for the compaction and sweep plans:
/// the driver builds a [`FaultStore`] from this plan and folds through it
/// alone, so the rule cannot fire during ingest, compaction, sweep, or the
/// query probes. It is also isolated by key: `/snap/` appears in no commit
/// record, data object, or compaction output, so no seed can steer this rule
/// onto a commit-record PUT.
///
/// The recovery contract is the one the module docs derive from the fold's own
/// code: `Catalog::fold` publishes content-addressed parts and CAS-swaps HEAD
/// last, so a faulted attempt publishes nothing reachable and a re-run
/// converges on the identical snapshot. The driver retries once and asserts
/// the post-fold digest against the fault-free run.
///
/// [`FaultStore`]: ravel_object_store::fault::FaultStore
fn generate_fold_fault(rng: &mut rand::rngs::StdRng) -> (FaultPlan, (Op, FaultKind)) {
    let (fault, kind) = if rng.random_bool(0.5) {
        (
            ScriptedFault::Transient(
                "sim fault schedule: transient on catalog snapshot".to_string(),
            ),
            FaultKind::Transient,
        )
    } else {
        (
            ScriptedFault::Throttled { retry_after_ms: 50 },
            FaultKind::Throttled,
        )
    };
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, fault)
            .with_key_contains(CATALOG_SNAPSHOT_SUBSTR)
            .with_occurrence(Occurrence::Nth(1)),
    );
    (plan, (Op::Put, kind))
}

/// Derive the compaction- and sweep-phase fault plans. All four kinds are
/// armed on every run; the three compaction-phase faults fire whenever
/// compaction runs, and the sweep delete fault fires only when rule 2 clears
/// a group in that cycle (the default workload may leave it unfired). Only
/// the pagination and delete faults' flavor (transient vs throttled) varies
/// with the seed, keeping the draw deterministic. Every rule is
/// retryable-once (`Occurrence::Nth(1)`):
/// the driver wraps the compaction and sweep entry points in a bounded
/// idempotent re-run, so each fault surfaces a typed error on the first
/// attempt and the re-run recovers to an equivalent result (the
/// recover-or-typed-error invariant).
///
/// Phase isolation is by construction, not by key matching: the driver builds
/// one [`FaultStore`] from `compact_plan` used only around `compact_bucket`
/// and another from `sweep_plan` used only around the sweep's action pass, so
/// neither rule can fire during ingest, fold, or the query probes.
///
/// [`FaultStore`]: ravel_object_store::fault::FaultStore
fn generate_compaction_faults(
    rng: &mut rand::rngs::StdRng,
) -> (FaultPlan, FaultPlan, Vec<(Op, FaultKind)>) {
    let mut expected = Vec::new();

    // Compaction phase: partial write on the L1 write path, then a missing
    // object on the L0 read path.
    let mut compact_plan = FaultPlan::empty();
    // Partial write: the connection drops before the L1 part lands, so the
    // object stays invisible (`put` is not applied). Surfaces as a retryable
    // `Transient`; the re-run rewrites the content-addressed part.
    compact_plan = compact_plan.with_rule(
        Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
            .with_key_contains(L1_SUBSTR)
            .with_occurrence(Occurrence::Nth(1)),
    );
    expected.push((Op::Put, FaultKind::PartialWriteThenError));
    // Missing object: an eventual-consistency not-found blip on an L0 input
    // read. Surfaces as `StoreError::NotFound`; the idempotent re-run reads the
    // object that is really there (the blip fired once).
    compact_plan = compact_plan.with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(L0_DATA_SUBSTR)
            .with_occurrence(Occurrence::Nth(1)),
    );
    expected.push((Op::Get, FaultKind::NotFoundBlip));

    // Sweep phase: a retryable failure on the paginated shard listing. No key
    // filter -- it governs the first `list` the sweep issues, which is the page
    // fetch that drives rules 2/3's superseded/unreferenced scan.
    let mut sweep_plan = FaultPlan::empty();
    let (list_fault, list_kind) = if rng.random_bool(0.5) {
        (
            ScriptedFault::Transient("sim fault schedule: transient on sweep listing".to_string()),
            FaultKind::Transient,
        )
    } else {
        (
            ScriptedFault::Throttled { retry_after_ms: 50 },
            FaultKind::Throttled,
        )
    };
    sweep_plan =
        sweep_plan.with_rule(Rule::new(Op::List, list_fault).with_occurrence(Occurrence::Nth(1)));
    expected.push((Op::List, list_kind));

    // Sweep phase: a retryable failure on the FIRST commit-record delete of
    // rule 2's phase C. Keyed on `COMMIT_SUBSTR` (`/c/`) and `Nth(1)` so it
    // lands on the very first delete the sweep issues.
    //
    // Placement matters for what the re-run exercises. Rule 2's phase C deletes
    // every cleared group's commit records first, then every cleared group's
    // data objects, then the chains' own records (`sweep_superseded_impl`,
    // crates/ravel-maintain/src/sweep.rs), and `sweep_shard_with_holds` runs
    // superseded (rule 2) before unreferenced parts (rule 3) and orphan GC
    // (rule 1). So the first delete of the whole pass is a superseded input
    // commit record, and this fault fires there. A retryable `Delete` fault in
    // `FaultStore` short-circuits before the wrapped backend runs
    // (crates/ravel-object-store/src/fault.rs), so when it fires no object has
    // been deleted yet: the re-gather on retry finds every input commit record
    // still present, collects the same cleared groups, and rule 2 deletes them
    // in full. The delete counts the faulted pass reports are therefore rule
    // 2's real work, not the zero a fault on the first `/l0/` data delete would
    // leave (that fires only after every commit record of the pass is already
    // gone, so the retry's re-gather reads `NotFound` for each input, collects
    // nothing through rule 2, and the keyspace converges through rule 1's
    // orphan GC instead -- a different recovery path than the harness claims to
    // exercise).
    let (delete_fault, delete_kind) = if rng.random_bool(0.5) {
        (
            ScriptedFault::Transient("sim fault schedule: transient on sweep delete".to_string()),
            FaultKind::Transient,
        )
    } else {
        (
            ScriptedFault::Throttled { retry_after_ms: 50 },
            FaultKind::Throttled,
        )
    };
    sweep_plan = sweep_plan.with_rule(
        Rule::new(Op::Delete, delete_fault)
            .with_key_contains(COMMIT_SUBSTR)
            .with_occurrence(Occurrence::Nth(1)),
    );
    expected.push((Op::Delete, delete_kind));

    (compact_plan, sweep_plan, expected)
}

/// Derive the hold/release gate scripts. Kept on the same safe targets as the
/// rules and on a later `Nth` than any rule, so a gate holds a *different*
/// matching call than the one a rule faults, exercising the hold/release path
/// without colliding with the scripted fault on the same call.
fn generate_gates(
    rng: &mut rand::rngs::StdRng,
    config: &FaultScheduleConfig,
    chosen: &[(Op, &str)],
) -> Vec<GateScript> {
    if config.max_gates == 0 || chosen.is_empty() {
        return Vec::new();
    }
    let want = rng.random_range(0..=config.max_gates);
    let mut gates = Vec::with_capacity(want);
    for _ in 0..want {
        let (op, key) = chosen[rng.random_range(0..chosen.len())];
        // Later occurrences than the rules (which use 1..=2) so the gate holds
        // a distinct call.
        let nth = rng.random_range(3u64..=4);
        gates.push(GateScript {
            op,
            key_contains: Some(key.to_string()),
            occurrence: Occurrence::Nth(nth),
        });
    }
    gates
}

/// A convenience for callers that want an ordered multi-outcome scenario on
/// one key rather than the seed-derived plan: builds a [`Sequence`] of
/// alternating faults and pass-throughs. Not used by [`generate`]; provided so
/// bespoke tests can script deterministic multi-call interleavings against the
/// same key layout the generator targets.
pub fn transient_then_pass_sequence(op: Op, key: &str, faults: usize) -> Sequence {
    let mut steps: Vec<SequenceStep> = Vec::with_capacity(faults + 1);
    for _ in 0..faults {
        steps.push(SequenceStep::Fault(ScriptedFault::Transient(
            "sim fault sequence".to_string(),
        )));
    }
    steps.push(SequenceStep::Passthrough);
    Sequence::new(op).with_key_contains(key).with_steps(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_schedule() {
        let cfg = FaultScheduleConfig::default();
        let a = generate(&MasterSeed::new(99), &cfg);
        let b = generate(&MasterSeed::new(99), &cfg);
        assert_eq!(a.expected_faults, b.expected_faults);
        assert_eq!(a.expected_compaction_faults, b.expected_compaction_faults);
        assert_eq!(a.expected_fold_fault, b.expected_fold_fault);
        assert_eq!(a.gates, b.gates);
        assert_eq!(a.plan.rules.len(), b.plan.rules.len());
        for (ra, rb) in a.plan.rules.iter().zip(b.plan.rules.iter()) {
            assert_eq!(ra.op, rb.op);
            assert_eq!(ra.key_contains, rb.key_contains);
            assert_eq!(ra.occurrence, rb.occurrence);
            assert_eq!(ra.fault, rb.fault);
        }
        // The compaction/sweep plans replay identically too.
        for (pa, pb) in [
            (&a.compact_plan, &b.compact_plan),
            (&a.sweep_plan, &b.sweep_plan),
            (&a.fold_plan, &b.fold_plan),
        ] {
            assert_eq!(pa.rules.len(), pb.rules.len());
            for (ra, rb) in pa.rules.iter().zip(pb.rules.iter()) {
                assert_eq!(ra.op, rb.op);
                assert_eq!(ra.key_contains, rb.key_contains);
                assert_eq!(ra.occurrence, rb.occurrence);
                assert_eq!(ra.fault, rb.fault);
            }
        }
    }

    #[test]
    fn compaction_faults_are_armed_and_phase_isolatable() {
        // Every run arms all four compaction/sweep fault kinds,
        // each `Nth(1)` and on a phase-isolated target, so the nightly sweep
        // exercises them deterministically.
        for seed in 1u64..=64 {
            let s = generate(&MasterSeed::new(seed), &FaultScheduleConfig::default());
            assert_eq!(
                s.expected_compaction_faults.len(),
                4,
                "seed {seed}: expected exactly four compaction/sweep faults"
            );
            // Partial write on the L1 write path.
            assert!(
                s.compact_plan.rules.iter().any(|r| r.op == Op::Put
                    && r.key_contains.as_deref() == Some(L1_SUBSTR)
                    && matches!(r.fault, ScriptedFault::PartialWriteThenError)),
                "seed {seed}: no partial-write rule on the L1 write path"
            );
            // Missing object on the L0 read path.
            assert!(
                s.compact_plan.rules.iter().any(|r| r.op == Op::Get
                    && r.key_contains.as_deref() == Some(L0_DATA_SUBSTR)
                    && matches!(r.fault, ScriptedFault::NotFoundBlip)),
                "seed {seed}: no missing-object rule on the L0 read path"
            );
            // Retryable pagination fault on the sweep listing.
            assert!(
                s.sweep_plan.rules.iter().any(|r| r.op == Op::List
                    && matches!(
                        r.fault,
                        ScriptedFault::Transient(_) | ScriptedFault::Throttled { .. }
                    )),
                "seed {seed}: no retryable pagination rule on the sweep listing"
            );
            // Retryable delete fault on the sweep's first phase-C commit-record
            // delete (keyed on `/c/`, the first delete the sweep issues).
            assert!(
                s.sweep_plan.rules.iter().any(|r| r.op == Op::Delete
                    && r.key_contains.as_deref() == Some(COMMIT_SUBSTR)
                    && matches!(
                        r.fault,
                        ScriptedFault::Transient(_) | ScriptedFault::Throttled { .. }
                    )),
                "seed {seed}: no retryable delete rule on the sweep's first commit-record delete"
            );
            // Every compaction/sweep rule fires exactly once.
            for rule in s.compact_plan.rules.iter().chain(s.sweep_plan.rules.iter()) {
                assert_eq!(
                    rule.occurrence,
                    Occurrence::Nth(1),
                    "seed {seed}: compaction/sweep rule must be Nth(1)"
                );
            }
        }
    }

    /// Object keys in the shapes `docs/catalog-and-mvcc.md` fixes, one per
    /// writer the cycle runs, so the fold rule's key filter can be checked
    /// against the real layout rather than against its own definition.
    const SAMPLE_KEYS: [(&str, bool); 7] = [
        // (key, whether the fold rule's `/snap/` filter must match it)
        (
            "t/aabb/catalog/m/snap/20231114T22.0123456789abcdef.csnap",
            true,
        ),
        ("t/aabb/catalog/m/HEAD", false),
        (
            "t/aabb/catalog/m/idx/20231114T22.0123456789abcdef.npost",
            false,
        ),
        (
            "t/aabb/catalog/m/idx/20231114T22.0123456789abcdef.cstat",
            false,
        ),
        ("t/aabb/m/c/0000/20231114T22/0123456789ab.1.7.cmt", false),
        ("t/aabb/m/c/0000/20231114T22/l1.0123456789abcdef.cmt", false),
        ("t/aabb/m/l0/0000/20231114T22/0123456789abcdef.rseg", false),
    ];

    #[test]
    fn generate_arms_a_retryable_catalog_snapshot_put() {
        let cfg = FaultScheduleConfig::default();
        for seed in 1u64..=64 {
            let s = generate(&MasterSeed::new(seed), &cfg);
            assert_eq!(
                s.fold_plan.rules.len(),
                1,
                "seed {seed}: the fold plan must carry exactly one rule"
            );
            let rule = &s.fold_plan.rules[0];
            assert_eq!(rule.op, Op::Put, "seed {seed}: fold fault is not a PUT");
            assert_eq!(
                rule.key_contains.as_deref(),
                Some(CATALOG_SNAPSHOT_SUBSTR),
                "seed {seed}: fold fault is not keyed on the catalog snapshot prefix"
            );
            assert_eq!(
                rule.occurrence,
                Occurrence::Nth(1),
                "seed {seed}: fold rule must be Nth(1) so it fires exactly once"
            );
            assert!(
                matches!(
                    rule.fault,
                    ScriptedFault::Transient(_) | ScriptedFault::Throttled { .. }
                ),
                "seed {seed}: non-retryable fold fault {:?}",
                rule.fault
            );
            assert_eq!(
                s.expected_fold_fault,
                Some((Op::Put, rule.fault.kind())),
                "seed {seed}: expected fold-fault tuple does not describe the armed rule"
            );

            // The rule can never reach a commit-record PUT (nor any other
            // phase's object) under this seed, because its key filter does not
            // match one.
            let key = rule.key_contains.as_deref().unwrap_or("");
            for (sample, want_match) in SAMPLE_KEYS {
                assert_eq!(
                    sample.contains(key),
                    want_match,
                    "seed {seed}: fold rule key {key:?} matches {sample:?} = {}, want {want_match}",
                    sample.contains(key)
                );
            }
        }
    }

    #[test]
    fn always_injects_at_least_one_rule() {
        let cfg = FaultScheduleConfig::default();
        for seed in 1u64..=64 {
            let s = generate(&MasterSeed::new(seed), &cfg);
            assert!(
                !s.expected_faults.is_empty(),
                "seed {seed}: schedule injected no faults"
            );
        }
    }

    #[test]
    fn every_rule_targets_a_safe_retryable_put() {
        let cfg = FaultScheduleConfig::default();
        for seed in 1u64..=64 {
            let s = generate(&MasterSeed::new(seed), &cfg);
            for rule in &s.plan.rules {
                assert_eq!(rule.op, Op::Put, "seed {seed}: non-PUT fault target");
                let key = rule.key_contains.as_deref().unwrap_or("");
                assert!(
                    key == L0_DATA_SUBSTR || key == COMMIT_SUBSTR,
                    "seed {seed}: unsafe fault key {key:?}"
                );
                assert!(
                    matches!(
                        rule.fault,
                        ScriptedFault::Transient(_) | ScriptedFault::Throttled { .. }
                    ),
                    "seed {seed}: non-retryable fault {:?}",
                    rule.fault
                );
            }
        }
    }

    #[test]
    fn rule_targets_are_distinct() {
        // The FaultStore only lets the first matching rule govern a call, so
        // duplicate (op, key) targets would leave later rules permanently
        // unfired -- the generator must never emit them.
        let cfg = FaultScheduleConfig::default();
        for seed in 1u64..=64 {
            let s = generate(&MasterSeed::new(seed), &cfg);
            let mut seen = std::collections::HashSet::new();
            for rule in &s.plan.rules {
                let target = (rule.op, rule.key_contains.clone());
                assert!(
                    seen.insert(target.clone()),
                    "seed {seed}: duplicate rule target {target:?}"
                );
            }
        }
    }
}
