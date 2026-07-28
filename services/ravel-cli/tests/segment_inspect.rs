//! Integration tests for `ravel-cli segment inspect`, run as a subprocess
//! against the built binary (`segment_inspect` is private to `main.rs`, and
//! the error-path tests need the printed error text, not just a typed
//! `Result`, so exercising the actual CLI is the only way to check both).
//!
//! ADR-0027 left RSEG v5 the only version. The fixture is read directly from
//! `ravel-segment`'s own test corpus (`crates/ravel-segment/tests/fixtures/
//! golden_v5_no_sparse.bin`, the same object `golden_bytes_v5.rs` pins for
//! the writer) by absolute path rather than copied here, so there is one
//! source of truth for what a valid RSEG object looks like. Expected
//! `segment inspect` stdout is itself pinned as a golden fixture under
//! `tests/fixtures/` (captured from a real run of this binary): a regression
//! in the inspect output fails these tests the way `golden_bytes_v5.rs`
//! catches a regression in the writer's output.
#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Path to a fixture in `ravel-segment`'s own test corpus, not a copy of it.
fn segment_fixture(name: &str) -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/ravel-segment/tests/fixtures"
    ))
    .join(name)
}

/// Path to one of this crate's own golden `segment inspect` stdout fixtures.
fn inspect_fixture(name: &str) -> String {
    let path = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures")).join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading fixture {}: {err}", path.display()))
}

fn run_inspect(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "segment", "inspect"])
        .arg(path)
        .output()
        .expect("ravel-cli runs")
}

/// A unique temp path so parallel test runs never collide.
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ravel-cli-{tag}-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ))
}

#[test]
fn v5_inspect_output_matches_golden_fixture() {
    let output = run_inspect(&segment_fixture("golden_v5_no_sparse.bin"));
    assert!(
        output.status.success(),
        "ravel-cli segment inspect failed on a known-good v5 fixture, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("golden_v5_inspect.txt");
    assert_eq!(
        stdout, expected,
        "v5 `segment inspect` output regressed; RSEG v5 is frozen \
         (docs/segment-format.md) -- this must not change without a version \
         bump and ADR"
    );
    assert!(
        expected.contains("value_kind=HIST_SPANS") && expected.contains("HIST_PAGES"),
        "the v5 golden fixture is mixed scalar+histogram; if the golden text \
         stops containing HIST_PAGES/HIST_SPANS the fixture or the inspector \
         silently stopped exercising histogram runs"
    );
}

/// A corrupt object must fail with the typed `SegmentError` text on stderr and
/// a non-zero exit status, never a panic. Flips a byte inside the SERIES_META
/// section's stored bytes (located via the footer), which corrupts that
/// section's crc32c, checked before any SERIES_META grammar is parsed.
#[test]
fn corrupt_series_meta_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v5_no_sparse.bin")).expect("reads fixture");
    let loc = ravel_segment::open_from_full(&good, ravel_segment::ReaderLimits::default())
        .expect("known-good fixture opens");
    let meta = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == 6)
        .expect("below-threshold v5 object has a whole SERIES_META");
    let flip_at = meta.offset as usize + (meta.len as usize / 2);
    let mut corrupt = good.clone();
    assert!(
        flip_at < corrupt.len(),
        "flip offset lands inside the object"
    );
    corrupt[flip_at] ^= 0xFF;

    let path = temp_path("corrupt-meta");
    std::fs::write(&path, &corrupt).expect("writes corrupt fixture");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a corrupt object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("section crc32c mismatch")
            || stderr.contains("failed to decode series catalog"),
        "expected the typed SegmentError text on stderr, got: {stderr}"
    );
}

/// Truncating the object (cutting bytes off the end, so the trailer itself is
/// gone) must also fail with a typed error, never a panic -- a different
/// failure path (`Truncated`/`TooSmall` from `parse_footer`, before any
/// section is reached).
#[test]
fn truncated_object_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v5_no_sparse.bin")).expect("reads fixture");
    let truncated = &good[..good.len() / 2];

    let path = temp_path("truncated");
    std::fs::write(&path, truncated).expect("writes truncated fixture");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a truncated object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("failed to parse segment"),
        "expected the typed parse-failure text on stderr, got: {stderr}"
    );
}
