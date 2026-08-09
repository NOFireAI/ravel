//! Determinism guard (issue #818): the same master seed twice produces an
//! identical fault plan and an identical cycle digest. A seed that failed once
//! must fail identically on replay, which requires both the injected fault
//! schedule and the observable cycle output to be pure functions of the seed.
//!
//! Two layers are checked:
//! - The fault-schedule generator: same seed -> same rules (op, key, Nth,
//!   fault kind), same gates, same expected-fault list.
//! - The whole cycle: same seed -> same reproducibility digest and the same
//!   fired-fault counters, even though a `FaultStore` and re-fold/compaction
//!   run in between.

use ravel_sim::fault_plan::{FaultScheduleConfig, generate};
use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

#[test]
fn same_seed_produces_identical_fault_plan() {
    let cfg = FaultScheduleConfig::default();
    for seed in 1u64..=16 {
        let a = generate(&MasterSeed::new(seed), &cfg);
        let b = generate(&MasterSeed::new(seed), &cfg);

        assert_eq!(
            a.expected_faults, b.expected_faults,
            "seed {seed}: expected-fault list differs across two generations"
        );
        assert_eq!(a.gates, b.gates, "seed {seed}: gate scripts differ");
        assert_eq!(
            a.plan.rules.len(),
            b.plan.rules.len(),
            "seed {seed}: rule count differs"
        );
        for (ra, rb) in a.plan.rules.iter().zip(b.plan.rules.iter()) {
            assert_eq!(ra.op, rb.op, "seed {seed}: rule op differs");
            assert_eq!(
                ra.key_contains, rb.key_contains,
                "seed {seed}: rule key differs"
            );
            assert_eq!(
                ra.occurrence, rb.occurrence,
                "seed {seed}: rule occurrence differs"
            );
            assert_eq!(ra.fault, rb.fault, "seed {seed}: rule fault differs");
        }
    }
}

#[test]
fn same_seed_produces_identical_cycle_digest() {
    let config = CycleConfig::default();
    for seed in 1u64..=4 {
        let a = run_cycle(MasterSeed::new(seed), &config)
            .unwrap_or_else(|e| panic!("seed {seed}: first cycle failed: {e}"));
        let b = run_cycle(MasterSeed::new(seed), &config)
            .unwrap_or_else(|e| panic!("seed {seed}: second cycle failed: {e}"));

        assert_eq!(
            a.digest, b.digest,
            "seed {seed}: same seed produced different cycle digests"
        );
        assert_eq!(
            a.buckets_compacted, b.buckets_compacted,
            "seed {seed}: compaction count not deterministic"
        );
        assert_eq!(
            a.records_conserved, b.records_conserved,
            "seed {seed}: conserved record count not deterministic"
        );
        assert_eq!(
            a.fault_counters, b.fault_counters,
            "seed {seed}: fired-fault counters not deterministic"
        );
    }
}
