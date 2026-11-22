//! Proves F2's fix: `RAVEL_SIM_SEED` actually changes the seed a driver
//! entry point resolves to. Before this fix, `MasterSeed::from_env_or` had
//! zero call sites anywhere in the crate, so setting the variable changed
//! nothing anywhere -- three doc sites (`src/seed.rs`'s module doc,
//! `src/driver.rs`'s `CycleError` doc, and this crate's own
//! `RAVEL_SIM_SEED` doc comments) promised it as the replay mechanism for a
//! claim that had no code behind it.
//!
//! Setting `RAVEL_SIM_SEED` for one case needs `std::env::set_var`, which is
//! `unsafe` in edition 2024 and unsound under a multi-threaded test runner
//! regardless, so each case re-runs this test binary as a child with the
//! variable set (or removed) and one ignored child test selected, the same
//! pattern as `ravel-invariant/tests/fault_init_from_env.rs`. The child
//! resolves `MasterSeed::from_env_or` in-process against the expected value
//! named by a marker variable, so no external example binary is needed.
//!
//! "prove-the-test": run below, both with the variable unset (must resolve
//! to `1`) and set to `999999` (must resolve to `999999`); the two must
//! differ from each other and the second must match the override exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;

use ravel_sim::MasterSeed;

/// Names the seed the parent expects `child_resolves_expected_seed` to see.
/// Absent, the child is a no-op so a plain test run never fails on it.
const EXPECT_ENV: &str = "RAVEL_SIM_TEST_EXPECT_SEED";

/// Runs `child_resolves_expected_seed` in a child process with `seed_env`
/// applied to `RAVEL_SIM_SEED` and `expect` as the marker, and asserts it ran
/// exactly once and passed.
fn run_child(seed_env: Option<&str>, expect: &str) {
    let exe = std::env::current_exe().expect("the test binary's path");
    let mut child = Command::new(exe);
    child.args([
        "child_resolves_expected_seed",
        "--exact",
        "--ignored",
        "--test-threads=1",
    ]);
    child.env(EXPECT_ENV, expect);
    match seed_env {
        Some(v) => {
            child.env("RAVEL_SIM_SEED", v);
        }
        None => {
            child.env_remove("RAVEL_SIM_SEED");
        }
    }
    let output = child.output().expect("the child test ran");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "child with RAVEL_SIM_SEED={seed_env:?} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("test result: ok. 1 passed"),
        "child with RAVEL_SIM_SEED={seed_env:?} did not run exactly one test:\n{stdout}"
    );
}

#[test]
fn env_seed_override_changes_resolved_seed() {
    run_child(None, "1");
    run_child(Some("999999"), "999999");
    assert_ne!(1_u64, 999999_u64);
}

#[test]
#[ignore = "a child of env_seed_override_changes_resolved_seed"]
fn child_resolves_expected_seed() {
    let Ok(expect) = std::env::var(EXPECT_ENV) else {
        // Not running as a child of env_seed_override_changes_resolved_seed:
        // no-op so a plain `cargo test`/`cargo nextest run` never fails here.
        return;
    };
    let expected: u64 = expect
        .parse()
        .unwrap_or_else(|_| panic!("{EXPECT_ENV} is not a valid u64: {expect:?}"));
    assert_eq!(MasterSeed::from_env_or(1).0, expected);
}
