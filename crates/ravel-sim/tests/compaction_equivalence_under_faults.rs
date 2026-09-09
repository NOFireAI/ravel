//! Acceptance test (ADR-0068 decisions 3-5): a seeded batch
//! runs the full ingest -> fold -> compact -> sweep -> query cycle with
//! generated fault schedules, and every invariant stays green:
//!
//! - (a) bit-exact query equivalence before vs after compaction,
//! - (b) record-count conservation across compaction,
//! - (c) no orphan objects or unreferenced parts left past the sweep horizon.
//!
//! `run_cycle` checks all three internally and returns `Err` carrying the
//! master seed on any violation, so a clean `Ok` per seed is the invariant
//! assertion. On top of that this test proves the run was not vacuous:
//!
//! - `buckets_compacted > 0`: compaction actually ran (otherwise invariant
//!   (a) would compare two identical uncompacted snapshots and prove nothing).
//! - every `(Op, FaultKind)` the schedule intended to inject has a positive
//!   `FaultStore` counter: the faults were actually injected and fired, not
//!   silently skipped, so the invariants held *under* fault injection rather
//!   than because no fault ever landed.
//!
//! To replay a failing seed: `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim
//! compaction_equivalence_under_faults` (the seed is also printed in the
//! panic message either way, since the batch names each seed directly).

use ravel_sim::workload::{CardinalityShape, WorkloadConfig};
use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

/// A workload whose samples land in a single ingest hour with several strict
/// flushes per shard, so every populated bucket clears
/// `min_compaction_inputs` and compaction is guaranteed to run.
fn config() -> CycleConfig {
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
fn compaction_equivalence_under_faults() {
    let config = config();

    for seed in 1u64..=6 {
        let outcome = run_cycle(MasterSeed::new(seed), &config)
            .unwrap_or_else(|err| panic!("seed {seed}: cycle violated an invariant: {err}"));

        // The run exercised compaction, so invariant (a) compared a real
        // pre/post-compaction pair rather than two identical snapshots.
        assert!(
            outcome.buckets_compacted > 0,
            "seed {seed}: no bucket was compacted, so the equivalence invariant was vacuous"
        );
        assert!(
            outcome.records_conserved > 0,
            "seed {seed}: conservation probe saw zero records"
        );

        // The faults the schedule intended really fired: prove injection, not
        // a green run that simply never hit a fault.
        assert!(
            !outcome.expected_faults.is_empty(),
            "seed {seed}: schedule declared no faults to inject"
        );
        for (op, kind) in &outcome.expected_faults {
            let fired = outcome
                .fault_counters
                .get(&(*op, *kind))
                .copied()
                .unwrap_or(0);
            assert!(
                fired > 0,
                "seed {seed}: expected fault {op:?}/{kind:?} never fired \
                 (FaultStore counters: {:?})",
                outcome.fault_counters
            );
        }
    }
}
