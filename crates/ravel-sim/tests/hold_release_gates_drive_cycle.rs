//! Exercises the schedule's hold/release gate scripts (ADR-0068 decision 4,
//! ADR-0059 decision 5): with `enable_gates` on, the driver arms the
//! seed-derived gates on the `FaultStore` and a background releaser frees each
//! held call as soon as it is held. This drives the hold/release path (a call
//! parking on its release channel, then resuming) through a real cycle without
//! deadlocking, and every invariant must still hold.
//!
//! "prove-the-test": the gates only matter if they actually hold a call. The
//! `fault_plan` unit tests assert the generator emits gate scripts on later
//! `Nth` occurrences than the rules; this test proves arming them and running
//! the cycle to a clean, all-invariants-green finish is reachable (a gate that
//! never released would hang this test, and a gate whose held call was dropped
//! would fail an invariant). Determinism of the gate scripts themselves is
//! covered by `fault_plan_and_cycle_determinism`.

use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

#[test]
fn hold_release_gates_drive_cycle() {
    let config = CycleConfig {
        enable_gates: true,
        ..CycleConfig::default()
    };

    for seed in 1u64..=4 {
        let outcome = run_cycle(MasterSeed::new(seed), &config).unwrap_or_else(|err| {
            panic!("seed {seed}: cycle with gates armed violated an invariant: {err}")
        });
        assert!(outcome.buckets_compacted > 0, "seed {seed}: no compaction");
        assert!(outcome.queries_run > 0, "seed {seed}: no queries");
    }
}
