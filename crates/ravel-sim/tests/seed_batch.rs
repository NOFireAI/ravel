//! A small fixed batch of seeds run through a full cycle end to end. Fixed
//! (not random) so a CI failure names a reproducible seed directly in its
//! assertion message, without needing `RAVEL_SIM_SEED` to reproduce it.

use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

#[test]
fn seed_batch_runs_clean() {
    let config = CycleConfig::default();

    for seed in 1u64..=5 {
        let outcome = run_cycle(MasterSeed::new(seed), &config)
            .unwrap_or_else(|err| panic!("seed {seed} cycle failed: {err}"));
        assert!(
            outcome.queries_run > 0,
            "seed {seed}: cycle ran zero queries"
        );
        assert!(
            outcome.series_generated > 0,
            "seed {seed}: cycle generated zero series"
        );
    }
}
