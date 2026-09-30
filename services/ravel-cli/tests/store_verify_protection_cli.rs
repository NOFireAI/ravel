//! Subprocess coverage that `ravel-cli store verify-protection` exits with the
//! code its report decides, through the real binary: a store with no bucket
//! control plane, and one that cannot be built at all, both exit 2, never 0.
#![allow(clippy::expect_used)]

use std::process::Command;

fn verify_protection(store_flags: &[&str]) -> std::process::Output {
    verify_protection_with(store_flags, &[])
}

fn verify_protection_with(store_flags: &[&str], extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(store_flags)
        .args([
            "store",
            "verify-protection",
            "--expected-noncurrent-days",
            "30",
        ])
        .args(extra)
        .env_remove("RAVEL_S3_BUCKET")
        .output()
        .expect("ravel-cli runs")
}

/// The memory store has no bucket control plane: every condition is unknown,
/// so the command exits 2 and names every expected condition.
#[test]
fn verify_protection_against_the_memory_store_exits_2() {
    let output = verify_protection(&["--store", "memory"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(2), "stdout:\n{stdout}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        10,
        "one line per condition plus a summary:\n{stdout}"
    );
    assert_eq!(
        lines[9],
        "verify-protection: UNKNOWN: could not verify: versioning, noncurrent-expiration, \
         expired-delete-marker, abort-multipart, rule-scope, no-foreign-rule, object-lock"
    );
}

/// A store that cannot be built never reaches a control plane, which is
/// "could not verify": exit 2, with the reason on every condition line.
#[test]
fn verify_protection_without_a_bucket_exits_2() {
    let output = verify_protection(&["--store", "s3"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(2), "stdout:\n{stdout}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines[0],
        "versioning                 unknown could not reach the bucket control plane: --store \
         s3 requires RAVEL_S3_BUCKET"
    );
    assert!(
        lines[9].starts_with("verify-protection: UNKNOWN: could not verify: versioning, "),
        "{stdout}"
    );
}

/// `--expect-object-retention` without `--retention-coverage-window` is a
/// usage error naming the option and why, and nothing is read: stdout stays
/// empty. The window alone is refused too.
#[test]
fn expect_object_retention_requires_the_coverage_window() {
    let output = verify_protection_with(&["--store", "memory"], &["--expect-object-retention"]);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "error: --expect-object-retention requires --retention-coverage-window: an object \
         newer than the bucket's retention mechanism's coverage lag (a scheduled batch job's \
         schedule interval plus its inventory delay) carries no retention yet, so the sample \
         reads only objects older than that window\n"
    );
    assert!(output.stdout.is_empty(), "nothing is read or printed");

    let output = verify_protection_with(
        &["--store", "memory"],
        &["--retention-coverage-window", "25h"],
    );
    assert_eq!(output.status.code(), Some(2), "clap's usage error");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--expect-object-retention"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A report that could not be written to stdout is "could not report", exit
/// 2, never the exit 1 that means an expected condition failed.
#[cfg(target_os = "linux")]
#[test]
fn a_stdout_write_error_exits_2() {
    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .expect("/dev/full opens");
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "--store",
            "memory",
            "store",
            "verify-protection",
            "--expected-noncurrent-days",
            "30",
        ])
        .stdout(full)
        .output()
        .expect("ravel-cli runs");
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("could not write the verify-protection report"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `--expected-noncurrent-days` is required: a deployment states its `E_v`.
#[test]
fn verify_protection_requires_the_expected_noncurrent_days() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "store", "verify-protection"])
        .output()
        .expect("ravel-cli runs");
    assert_eq!(output.status.code(), Some(2), "clap's usage error");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--expected-noncurrent-days"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
