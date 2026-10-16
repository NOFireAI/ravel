//! Determinism guard: the same master seed twice produces an
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
use ravel_sim::workload::{CardinalityShape, WorkloadConfig};
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
            a.expected_fold_fault, b.expected_fold_fault,
            "seed {seed}: expected fold-fault tuple differs across two generations"
        );
        // The fold plan replays identically too: same op, key, Nth and fault
        // flavor, so a fold fault that fired once on a failing seed fires the
        // same way on replay.
        assert_eq!(
            a.fold_plan.rules.len(),
            b.fold_plan.rules.len(),
            "seed {seed}: fold rule count differs"
        );
        for (ra, rb) in a.fold_plan.rules.iter().zip(b.fold_plan.rules.iter()) {
            assert_eq!(ra.op, rb.op, "seed {seed}: fold rule op differs");
            assert_eq!(
                ra.key_contains, rb.key_contains,
                "seed {seed}: fold rule key differs"
            );
            assert_eq!(
                ra.occurrence, rb.occurrence,
                "seed {seed}: fold rule occurrence differs"
            );
            assert_eq!(ra.fault, rb.fault, "seed {seed}: fold rule fault differs");
        }
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
        assert_eq!(
            a.faulted_sweep_pass, b.faulted_sweep_pass,
            "seed {seed}: faulted sweep pass not deterministic"
        );
        assert_eq!(
            a.faulted_pass_superseded_records_deleted, b.faulted_pass_superseded_records_deleted,
            "seed {seed}: faulted-pass superseded records not deterministic"
        );
        assert_eq!(
            a.faulted_pass_superseded_data_deleted, b.faulted_pass_superseded_data_deleted,
            "seed {seed}: faulted-pass superseded data not deterministic"
        );
        assert_eq!(
            a.faulted_pass_unreferenced_parts_deleted, b.faulted_pass_unreferenced_parts_deleted,
            "seed {seed}: faulted-pass unreferenced parts not deterministic"
        );
        // The fold fault fired the same number of times both runs, and that
        // number is the exact one the `Nth(1)` rule allows -- a comparison of
        // two zeroes would be equal and prove nothing.
        assert_eq!(
            a.fold_faults_fired, 1,
            "seed {seed}: fold fault fired {} times in the first cycle, want exactly 1",
            a.fold_faults_fired
        );
        assert_eq!(
            a.fold_faults_fired, b.fold_faults_fired,
            "seed {seed}: fold-fault fired count not deterministic"
        );
    }
}

/// The same seed twice, under a workload that guarantees the sweep delete fault
/// fires (so the per-pass faulted fields are non-trivial), produces an
/// identical faulted-pass report. This exercises the new per-pass fields with a
/// positive value, which the default workload above may leave at `None`.
#[test]
fn same_seed_produces_identical_faulted_pass_report() {
    let config = CycleConfig {
        workload: WorkloadConfig {
            tenant_count: 2,
            series_per_tenant: 8,
            samples_per_series: 6,
            cardinality: CardinalityShape::ManySmallLabels,
            histogram_fraction: 0.25,
            queries_per_tenant: 4,
            ..WorkloadConfig::default()
        },
        inject_faults: true,
        ..CycleConfig::default()
    };
    let seed = 3u64;
    let a = run_cycle(MasterSeed::new(seed), &config)
        .unwrap_or_else(|e| panic!("seed {seed}: first cycle failed: {e}"));
    let b = run_cycle(MasterSeed::new(seed), &config)
        .unwrap_or_else(|e| panic!("seed {seed}: second cycle failed: {e}"));

    // The fault really landed, so the comparison is over a positive value.
    assert_eq!(
        a.faulted_sweep_pass.as_ref(),
        Some(&("sim-tenant-000".to_string(), 0)),
        "seed {seed}: delete fault did not fire on the expected pass"
    );
    assert!(
        a.faulted_pass_superseded_records_deleted > 0,
        "seed {seed}: faulted pass reported no superseded-record deletes"
    );

    assert_eq!(
        a.faulted_sweep_pass, b.faulted_sweep_pass,
        "seed {seed}: faulted sweep pass not deterministic"
    );
    assert_eq!(
        a.faulted_pass_superseded_records_deleted, b.faulted_pass_superseded_records_deleted,
        "seed {seed}: faulted-pass superseded records not deterministic"
    );
    assert_eq!(
        a.faulted_pass_superseded_data_deleted, b.faulted_pass_superseded_data_deleted,
        "seed {seed}: faulted-pass superseded data not deterministic"
    );
}
