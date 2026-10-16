//! Nightly CI seed batch (ADR-0068 decision 6): run a contiguous range of
//! master seeds through a full cycle each and report every failing seed.
//!
//! The range comes from `RAVEL_SIM_BATCH_START` (default 1) and
//! `RAVEL_SIM_BATCH_COUNT` (default 200). Every failure prints its seed, so
//! reproducing one locally is a single command:
//! `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim` (or re-run this example
//! with a batch of one). A seed either always passes or always fails; a
//! "flaky" seed is a determinism bug by definition.

use ravel_sim::{CycleConfig, MasterSeed, run_cycle};

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(raw) => match raw.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("{name}={raw} is not a u64");
                std::process::exit(2);
            }
        },
        Err(_) => default,
    }
}

fn main() {
    let start = env_u64("RAVEL_SIM_BATCH_START", 1);
    let count = env_u64("RAVEL_SIM_BATCH_COUNT", 200);
    let config = CycleConfig::default();

    let mut failures = 0u64;
    for seed in start..start.saturating_add(count) {
        match run_cycle(MasterSeed::new(seed), &config) {
            Ok(outcome) => {
                if outcome.queries_run == 0 || outcome.series_generated == 0 {
                    eprintln!(
                        "seed {seed}: degenerate cycle (queries_run={}, series_generated={})",
                        outcome.queries_run, outcome.series_generated
                    );
                    failures += 1;
                }
            }
            Err(err) => {
                eprintln!("seed {seed}: cycle failed: {err}");
                failures += 1;
            }
        }
    }

    if failures > 0 {
        eprintln!("seed batch [{start}, +{count}): {failures} failing seed(s), listed above");
        std::process::exit(1);
    }
    println!("seed batch [{start}, +{count}): all seeds passed");
}
