//! Fault-schedule generator (ADR-0068 decision 4, issue #818 deliverable 1).
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
//! paths do not all retry), on LIST/DELETE (the sweep path), on the `l1/`
//! parts, or on the `catalog/` snapshot objects a fold writes: a fault there
//! would fail a phase the harness has no retry hook for and turn a green
//! invariant red for reasons unrelated to the bug classes this task hunts.
//! `Occurrence::Nth` (rather than `Always`) is used so each rule fires exactly
//! once, during ingest, and is fully consumed before the compact/sweep phases,
//! so a leftover rule can never fire on a compaction-record PUT that has no
//! retry hook of its own.

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
/// record and retention tombstone, which is why every rule uses
/// [`Occurrence::Nth`] and is consumed during ingest (see the module docs).
pub const COMMIT_SUBSTR: &str = "/c/";

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
    /// The scripted plan handed to [`FaultStore::new`].
    ///
    /// [`FaultStore::new`]: ravel_object_store::fault::FaultStore::new
    pub plan: FaultPlan,
    /// Hold/release gates the driver may arm on the store.
    pub gates: Vec<GateScript>,
    /// The `(Op, FaultKind)` pair each emitted rule injects, in emission
    /// order. A test asserts the store's counter for each pair fired at least
    /// once, proving the faults were actually injected rather than silently
    /// skipped.
    pub expected_faults: Vec<(Op, FaultKind)>,
}

impl FaultSchedule {
    /// A schedule that injects nothing: a fully transparent [`FaultStore`].
    pub fn none() -> Self {
        FaultSchedule {
            plan: FaultPlan::empty(),
            gates: Vec::new(),
            expected_faults: Vec::new(),
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

    FaultSchedule {
        plan,
        gates,
        expected_faults,
    }
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
        assert_eq!(a.gates, b.gates);
        assert_eq!(a.plan.rules.len(), b.plan.rules.len());
        for (ra, rb) in a.plan.rules.iter().zip(b.plan.rules.iter()) {
            assert_eq!(ra.op, rb.op);
            assert_eq!(ra.key_contains, rb.key_contains);
            assert_eq!(ra.occurrence, rb.occurrence);
            assert_eq!(ra.fault, rb.fault);
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
