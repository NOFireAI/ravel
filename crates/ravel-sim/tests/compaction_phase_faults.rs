//! Compaction/sweep-phase fault injection.
//!
//! Two properties, one per branch of the recover-or-typed-error invariant:
//!
//! - `each_compaction_fault_kind_recovers`: a seeded batch runs the full cycle
//!   with the generated compaction/sweep faults armed, and every one fires (its
//!   `FaultStore` counter moves) while the cycle stays green -- the recover
//!   branch. This proves the injection is reachable in the same seeded cycle
//!   the nightly sweep drives, not a separate unwired harness, and that the
//!   driver's idempotent re-run absorbs each fault into an equivalent result.
//! - `an_unrecoverable_compaction_fault_surfaces_a_typed_error`: an
//!   `Occurrence::Always` partial write on the L1 path can never be recovered,
//!   so the bounded re-run gives up and surfaces a typed `CycleError::Compact`
//!   -- the typed-error branch. Never a panic, never a silent success. This is
//!   the flip proving the recover-branch test is non-vacuous: the same fault
//!   armed `Nth(1)` recovers, armed `Always` fails loud.
//!
//! Replay a failing seed: `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim
//! each_compaction_fault_kind_recovers` (the seed is also in the panic
//! message, since the batch names each seed directly).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_object_store::fault::{FaultKind, FaultPlan, Occurrence, Op, Rule, ScriptedFault};
use ravel_sim::fault_plan::{FaultSchedule, L1_SUBSTR};
use ravel_sim::workload::{CardinalityShape, WorkloadConfig};
use ravel_sim::{CycleConfig, CycleError, MasterSeed, run_cycle};

/// A workload whose samples land in a single ingest hour with several strict
/// flushes per shard, so every populated bucket clears `min_compaction_inputs`
/// and compaction is guaranteed to run (the same shape the equivalence
/// acceptance test uses).
fn compacting_config() -> CycleConfig {
    CycleConfig {
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
    }
}

#[test]
fn each_compaction_fault_kind_recovers() {
    let config = compacting_config();

    for seed in 1u64..=6 {
        let outcome = run_cycle(MasterSeed::new(seed), &config).unwrap_or_else(|err| {
            panic!("seed {seed}: compaction-phase faults violated an invariant: {err}")
        });

        // Compaction actually ran, so the faults on the compaction/sweep path
        // had something to fire against.
        assert!(
            outcome.buckets_compacted > 0,
            "seed {seed}: no bucket was compacted, so the compaction faults were unreachable"
        );

        // The partial-write and missing-object faults fired during compaction.
        let partial_write = outcome
            .fault_counters
            .get(&(Op::Put, FaultKind::PartialWriteThenError))
            .copied()
            .unwrap_or(0);
        assert!(
            partial_write > 0,
            "seed {seed}: partial-write fault never fired on the L1 write path \
             (counters: {:?})",
            outcome.fault_counters
        );
        let missing_object = outcome
            .fault_counters
            .get(&(Op::Get, FaultKind::NotFoundBlip))
            .copied()
            .unwrap_or(0);
        assert!(
            missing_object > 0,
            "seed {seed}: missing-object fault never fired on the L0 read path \
             (counters: {:?})",
            outcome.fault_counters
        );

        // The pagination fault fired during the sweep (either flavor).
        let pagination = outcome
            .fault_counters
            .get(&(Op::List, FaultKind::Transient))
            .copied()
            .unwrap_or(0)
            + outcome
                .fault_counters
                .get(&(Op::List, FaultKind::Throttled))
                .copied()
                .unwrap_or(0);
        assert!(
            pagination > 0,
            "seed {seed}: pagination fault never fired on the sweep listing \
             (counters: {:?})",
            outcome.fault_counters
        );

        // The delete fault fired during the sweep (either flavor), on the
        // sweep's first phase-C commit-record delete. `Occurrence::Nth(1)`
        // fires exactly once, so the sum is exactly 1, not just positive.
        let delete = outcome
            .fault_counters
            .get(&(Op::Delete, FaultKind::Transient))
            .copied()
            .unwrap_or(0)
            + outcome
                .fault_counters
                .get(&(Op::Delete, FaultKind::Throttled))
                .copied()
                .unwrap_or(0);
        assert_eq!(
            delete, 1,
            "seed {seed}: delete fault on the sweep's first commit-record delete fired {delete} \
             times, want exactly 1 (counters: {:?})",
            outcome.fault_counters
        );

        // The delete fault landed on the first tenant's shard 0 -- the first
        // sweep pass, where the `Nth(1)` rules on the one `sweep_store` fire.
        assert_eq!(
            outcome.faulted_sweep_pass.as_ref(),
            Some(&("sim-tenant-000".to_string(), 0)),
            "seed {seed}: delete fault fired on an unexpected sweep pass"
        );

        // The faulted pass re-ran rule 2 after the re-run absorbed the fault:
        // its OWN superseded-delete counts are positive and exact.
        // `compacting_config` fixes the workload shape (2 tenants x 8 series x 6
        // samples across 2 shards) independently of the seed, so compaction
        // supersedes the same L0 inputs every run. The 6 comes from the flush
        // shape, not the series count: strict-mode ingest flushes once per
        // sample-index batch per shard, so each (tenant, shard) publishes 6 L0
        // commit records with 6 data objects, and rule 2 deletes all of them in
        // the faulted pass (tenant-000/shard 0), with no unreferenced `/l1`
        // part (this workload's compaction leaves none).
        // On the first round's placement (delete keyed on `/l0/`, firing after
        // every commit record of the pass was already gone) this pass reported
        // 0/0 and the keyspace converged through orphan GC instead; these exact
        // figures are what distinguish the rule-2 recovery from that.
        assert_eq!(
            outcome.faulted_pass_superseded_records_deleted, 6,
            "seed {seed}: faulted pass superseded-records-deleted changed"
        );
        assert_eq!(
            outcome.faulted_pass_superseded_data_deleted, 6,
            "seed {seed}: faulted pass superseded-data-deleted changed"
        );
        assert_eq!(
            outcome.faulted_pass_unreferenced_parts_deleted, 0,
            "seed {seed}: faulted pass unexpectedly deleted an unreferenced part"
        );

        // The cycle's cumulative superseded deletes across all four passes:
        // 4 passes x 6 = 24 records and 24 data objects.
        assert_eq!(
            outcome.sweep_superseded_records_deleted, 24,
            "seed {seed}: cumulative superseded-records-deleted changed"
        );
        assert_eq!(
            outcome.sweep_superseded_data_deleted, 24,
            "seed {seed}: cumulative superseded-data-deleted changed"
        );
        assert_eq!(
            outcome.sweep_unreferenced_parts_deleted, 0,
            "seed {seed}: cumulative unreferenced parts changed"
        );

        // The suppression check: run the SAME seed and config with the whole
        // fault schedule disabled, and the cumulative superseded deletes must
        // be identical. A delete the fault suppressed (as the first round's
        // `/l0/` placement did, leaving the faulted pass at 0 and the cumulative
        // at 18 instead of 24) would show up here as a shortfall against the
        // fault-free baseline.
        let clean_config = CycleConfig {
            inject_faults: false,
            ..compacting_config()
        };
        let clean = run_cycle(MasterSeed::new(seed), &clean_config)
            .unwrap_or_else(|err| panic!("seed {seed}: fault-free baseline cycle failed: {err}"));
        assert_eq!(
            outcome.sweep_superseded_records_deleted, clean.sweep_superseded_records_deleted,
            "seed {seed}: faulted-plan superseded records ({}) differ from the fault-free \
             baseline ({}); a delete was suppressed",
            outcome.sweep_superseded_records_deleted, clean.sweep_superseded_records_deleted
        );
        assert_eq!(
            outcome.sweep_superseded_data_deleted, clean.sweep_superseded_data_deleted,
            "seed {seed}: faulted-plan superseded data ({}) differ from the fault-free \
             baseline ({}); a delete was suppressed",
            outcome.sweep_superseded_data_deleted, clean.sweep_superseded_data_deleted
        );

        // Every fault the schedule declared (ingest and compaction/sweep) fired.
        for (op, kind) in &outcome.expected_faults {
            let fired = outcome
                .fault_counters
                .get(&(*op, *kind))
                .copied()
                .unwrap_or(0);
            assert!(
                fired > 0,
                "seed {seed}: expected fault {op:?}/{kind:?} never fired (counters: {:?})",
                outcome.fault_counters
            );
        }
    }
}

/// An `Always` partial write on the L1 path that the bounded re-run can never
/// clear, injected via `fault_schedule_override` so its reachability is exact
/// (not a search over the generator's random output).
fn unrecoverable_compaction_schedule() -> FaultSchedule {
    let compact_plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::PartialWriteThenError)
            .with_key_contains(L1_SUBSTR)
            .with_occurrence(Occurrence::Always),
    );
    FaultSchedule {
        compact_plan,
        ..FaultSchedule::none()
    }
}

#[test]
fn an_unrecoverable_compaction_fault_surfaces_a_typed_error() {
    let config = CycleConfig {
        fault_schedule_override: Some(unrecoverable_compaction_schedule()),
        ..compacting_config()
    };

    let err = run_cycle(MasterSeed::new(1), &config).expect_err(
        "a compaction fault the re-run cannot clear must surface a typed error, \
         not recover or silently succeed",
    );
    assert!(
        matches!(err, CycleError::Compact { .. }),
        "expected a typed CycleError::Compact, got: {err}"
    );
}
