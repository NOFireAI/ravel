//! Subprocess coverage that `ravel-cli store qualify` prints the informational
//! Object Lock / versioning probe result (ADR-0055 section 3, issue #663).
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
