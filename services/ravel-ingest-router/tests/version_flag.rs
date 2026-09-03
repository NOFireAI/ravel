//! `--version` reachability (issue #1177): runs the built
//! `ravel-ingest-router` binary itself, not a disconnected formatting
//! helper, and asserts on the exact stdout it prints. The expected string is
//! obtained from `ravel_version::version()` -- the same function
//! `ravel_version::parse()` wires into the binary's `--version` -- so this
//! proves the flag reaches that resolution, not merely that some
//! version-shaped text appears.
#![allow(clippy::expect_used)]

use std::process::Command;

#[test]
fn version_flag_prints_resolved_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-ingest-router"))
        .arg("--version")
        .output()
        .expect("ravel-ingest-router runs");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert_eq!(
        stdout,
        format!("ravel-ingest-router {}\n", ravel_version::version())
    );
}
