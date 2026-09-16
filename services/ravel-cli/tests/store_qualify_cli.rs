//! Subprocess coverage that `ravel-cli store qualify` prints the informational
//! Object Lock / versioning probe result (ADR-0055 section 3).
//!
//! Driven through the real CLI entry point (the built binary), not by calling
//! the conformance function directly, so it proves the operator actually sees
//! the informational line in what the command prints. `--store memory` gives a
//! fresh in-memory oracle per subprocess: qualification passes and the record
//! is written, while the Object Lock probe reports "unknown" (the honest answer
//! through the `ObjectStoreBackend` contract).
#![allow(clippy::expect_used)]

use std::process::Command;

#[test]
fn store_qualify_prints_informational_object_lock_probe() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "store", "qualify"])
        .output()
        .expect("ravel-cli runs");

    assert!(
        output.status.success(),
        "store qualify against the memory oracle must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // The informational line appears, clearly labeled informational.
    assert!(
        stdout.contains("object_lock/versioning"),
        "store qualify output must include the Object Lock probe line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("(informational"),
        "the Object Lock line must be labeled informational; got:\n{stdout}"
    );
    // Through the trait contract the honest answer is "unknown".
    assert!(
        stdout.contains("unknown"),
        "the memory backend reports Object Lock status unknown; got:\n{stdout}"
    );
    // The required-bucket-configuration report (ADR-0064 §7) also appears,
    // labeled informational. Through the trait contract every field is unknown,
    // so it raises no alarm and does not gate the outcome.
    assert!(
        stdout.contains("bucket/versioning"),
        "store qualify output must include the bucket versioning report line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("lifecycle/noncurrent_version_expiration"),
        "store qualify output must include the lifecycle-rule report lines; got:\n{stdout}"
    );

    // And the probe never gates the outcome: qualification still passes.
    assert!(
        stdout.contains("qualified"),
        "qualification must still pass regardless of the probe; got:\n{stdout}"
    );
}

/// `--list-page-size` reaches `run_conformance_suite` end to end: `build_store`
/// builds the memory backend with that same page size (`build_store_with_
/// list_page_size`), so declaring 10 makes both the store's real pagination
/// boundary and the probe's declared size 10, and the cross-page probe writes
/// `page_size + 2` = 12 keys, split as 10 then 2 across exactly two key-bearing
/// pages (issue #1695). A flag that failed to reach the suite would leave the
/// default 1000-key probe shape in the output instead of this one.
#[test]
fn store_qualify_list_page_size_flag_reaches_the_cross_page_probe() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "--store",
            "memory",
            "store",
            "qualify",
            "--list-page-size",
            "10",
        ])
        .output()
        .expect("ravel-cli runs");

    assert!(
        output.status.success(),
        "store qualify with --list-page-size 10 against the memory oracle must \
         succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("12 distinct keys across 2 pages (2 carrying keys)"),
        "the cross-page probe detail must reflect the declared --list-page-size \
         of 10 (12 = page_size + 2 keys, split 10 then 2); got:\n{stdout}"
    );
}
