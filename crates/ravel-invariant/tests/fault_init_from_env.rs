//! `init` reads `RAVEL_ANTITHESIS_FAULTS` into the process rates that
//! `inv_fault!` draws at. The process rates are set once per process and the
//! variable cannot be set in-process, so each case re-runs this test binary
//! as a child with the variable set and one ignored child test selected.
#![cfg(feature = "capture")]
#![allow(clippy::expect_used)]

use std::process::Command;

use ravel_invariant::{FAULT_ENV, InitError, MUTANT_ENV, fault};

// Names the fault the parent expects to fire at rate 1 in the child.
const EXPECT_ENV: &str = "RAVEL_INVARIANT_TEST_EXPECT_FIRES";

/// Runs one ignored test of this binary in a child process and asserts it ran
/// exactly once and passed.
fn run_child(test: &str, faults: &str, expect_fires: Option<&str>) {
    let exe = std::env::current_exe().expect("the test binary's path");
    let mut child = Command::new(exe);
    child
        .args([test, "--exact", "--ignored", "--test-threads=1"])
        .env(FAULT_ENV, faults)
        .env_remove(MUTANT_ENV)
        .env_remove(EXPECT_ENV);
    if let Some(name) = expect_fires {
        child.env(EXPECT_ENV, name);
    }
    let output = child.output().expect("the child test ran");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "child {test} with {FAULT_ENV}={faults:?} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("test result: ok. 1 passed"),
        "child {test} did not run exactly one test:\n{stdout}"
    );
}

#[test]
fn init_sets_the_process_rates_from_the_variable() {
    for name in fault::FAULTS.iter().copied() {
        let others: Vec<String> = fault::FAULTS
            .iter()
            .filter(|other| **other != name)
            .map(|other| format!("{other}=0"))
            .collect();
        let value = std::iter::once(format!("{name}=1"))
            .chain(others)
            .collect::<Vec<_>>()
            .join(",");
        run_child("child_init_fires_the_named_fault", &value, Some(name));
    }
}

#[test]
fn init_refuses_an_invalid_variable() {
    run_child(
        "child_init_refuses_an_unknown_fault",
        "ingest/no-such-fault=1",
        None,
    );
}

fn variable() -> String {
    std::env::var(FAULT_ENV)
        .unwrap_or_else(|_| panic!("{FAULT_ENV} unset: this test runs only as a child"))
}

#[test]
#[ignore = "a child of init_sets_the_process_rates_from_the_variable"]
fn child_init_fires_the_named_fault() {
    let value = variable();
    let expected = std::env::var(EXPECT_ENV)
        .unwrap_or_else(|_| panic!("{EXPECT_ENV} unset: this test runs only as a child"));
    for name in fault::FAULTS.iter().copied() {
        assert!(!fault::fires(name), "{name} fired before init");
    }
    ravel_invariant::init().expect("a valid value starts");
    for name in fault::FAULTS.iter().copied() {
        let fired = (0..100).filter(|_| fault::fires(name)).count();
        let want = if name == expected { 100 } else { 0 };
        assert_eq!(fired, want, "{name} with {FAULT_ENV}={value:?}");
    }
}

#[test]
#[ignore = "a child of init_refuses_an_invalid_variable"]
fn child_init_refuses_an_unknown_fault() {
    let value = variable();
    assert_eq!(value, "ingest/no-such-fault=1");
    assert_eq!(
        ravel_invariant::init(),
        Err(InitError::UnknownFault {
            name: "ingest/no-such-fault".to_owned(),
        })
    );
    assert!(
        fault::FAULTS.iter().all(|name| !fault::fires(name)),
        "a refused value enables no fault"
    );
}
