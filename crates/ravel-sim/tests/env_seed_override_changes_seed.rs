//! Proves F2's fix: `RAVEL_SIM_SEED` actually changes the seed a driver
//! entry point resolves to. Before this fix, `MasterSeed::from_env_or` had
//! zero call sites anywhere in the crate, so setting the variable changed
//! nothing anywhere -- three doc sites (`src/seed.rs`'s module doc,
//! `src/driver.rs`'s `CycleError` doc, and this crate's own
//! `RAVEL_SIM_SEED` doc comments) promised it as the replay mechanism for a
//! claim that had no code behind it.
//!
//! This has to shell out rather than call `MasterSeed::from_env_or`
//! in-process: setting the variable for one test would require
//! `std::env::set_var`, which is `unsafe` in edition 2024 and unsound under
//! a multi-threaded test runner regardless. (This crate does not yet opt
//! into the workspace `[lints]` table -- tracked separately -- so the
//! workspace-level `unsafe_code = "forbid"` does not currently reach it;
//! the subprocess design is chosen on soundness grounds, not because the
//! lint would reject the alternative.) `examples/print_master_seed.rs`
//! exists solely so this test has an external process whose environment it
//! can control freely.
//!
//! "prove-the-test": run below, both with the variable unset (must print
//! the example's own default, `1`) and set to `999999` (must print
//! `999999`); the two must differ from each other and the second must
//! match the override exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;

fn run_example(seed_env: Option<&str>) -> u64 {
    let mut cmd = Command::new(env!("CARGO"));
    // `--locked` keeps the nested build's resolution identical to every
    // other cargo invocation in a CI job; without it this inner `cargo run`
    // could resolve dependencies differently from the outer `--locked` run.
    cmd.args([
        "run",
        "--quiet",
        "--locked",
        "-p",
        "ravel-sim",
        "--example",
        "print_master_seed",
    ]);
    match seed_env {
        Some(v) => {
            cmd.env("RAVEL_SIM_SEED", v);
        }
        None => {
            cmd.env_remove("RAVEL_SIM_SEED");
        }
    }
    let output = cmd
        .output()
        .expect("run print_master_seed example as a subprocess");
    assert!(
        output.status.success(),
        "print_master_seed exited with {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("example stdout is utf8")
        .trim()
        .parse::<u64>()
        .expect("example prints a plain u64")
}

#[test]
fn env_seed_override_changes_resolved_seed() {
    let default_seed = run_example(None);
    assert_eq!(
        default_seed, 1,
        "unset RAVEL_SIM_SEED should fall back to the example's default of 1"
    );

    let overridden_seed = run_example(Some("999999"));
    assert_eq!(
        overridden_seed, 999999,
        "RAVEL_SIM_SEED=999999 should override the default"
    );

    assert_ne!(default_seed, overridden_seed);
}
