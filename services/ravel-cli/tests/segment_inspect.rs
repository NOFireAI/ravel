//! Integration tests for `ravel-cli segment inspect`, run as a subprocess
//! against the built binary (`segment_inspect` is private to `main.rs`, and
//! requirement 4 below needs the printed error text, not just a typed
//! `Result`, so exercising the actual CLI is the only way to check both).
//!
//! Fixtures are read directly from `ravel-segment`'s own test corpus
//! (`crates/ravel-segment/tests/fixtures/`) by absolute path rather than
//! copied here, so there is exactly one source of truth for what a valid
//! RSEG v1/v2/v3 object looks like: `golden_v1_a3.bin` is the same fixture
//! `ravel-segment/tests/golden_bytes.rs` pins for the v1 writer,
//! `golden_v2_multi_schema.bin` / `golden_v2_raw_f64_padding.bin` are two of
//! the six `golden_bytes_v2.rs` pins for the v2 writer, and
//! `golden_v3_mixed_scalar_and_histogram.bin` is one of the
//! `golden_bytes_v3.rs` pins for the v3 writer (docs/rseg-v3-plan.md phase
//! C6).
//!
//! Expected `segment inspect` stdout is itself pinned as a golden fixture
//! under `tests/fixtures/` (captured from a real run of this binary against
//! the corresponding RSEG fixture): a regression in the inspect output
//! (v1, v2, or v3) fails these tests exactly the way `golden_bytes.rs`
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

#[test]
fn v1_inspect_output_matches_golden_fixture() {
    let output = run_inspect(&segment_fixture("golden_v1_a3.bin"));
    assert!(
        output.status.success(),
        "ravel-cli segment inspect failed on a known-good v1 fixture, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("golden_v1_inspect.txt");
    assert_eq!(
        stdout, expected,
        "v1 `segment inspect` output regressed; RSEG v1 CLI output must not \
         change as a side effect of v2 support (docs/rseg-v2-plan.md phase P5)"
    );
}

#[test]
fn v2_multi_schema_inspect_output_matches_golden_fixture() {
    let output = run_inspect(&segment_fixture("golden_v2_multi_schema.bin"));
    assert!(
        output.status.success(),
        "ravel-cli segment inspect failed on a known-good v2 fixture, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("golden_v2_multi_schema_inspect.txt");
    assert_eq!(stdout, expected, "v2 `segment inspect` output regressed");
}

#[test]
fn v2_raw_f64_padding_inspect_output_matches_golden_fixture() {
    let output = run_inspect(&segment_fixture("golden_v2_raw_f64_padding.bin"));
    assert!(
        output.status.success(),
        "ravel-cli segment inspect failed on a known-good v2 fixture, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("golden_v2_raw_f64_padding_inspect.txt");
    assert_eq!(stdout, expected, "v2 `segment inspect` output regressed");
    assert!(
        expected.contains("ALIGNMENT_GAP"),
        "this fixture is named for exercising v2's alignment padding; if \
         the golden text stops containing ALIGNMENT_GAP, the fixture or the \
         flagging logic silently stopped exercising it"
    );
}

/// A corrupt v2 object must fail with the typed `SegmentError` text on
/// stderr and a non-zero exit status, never a panic. Flips one byte inside
/// the SERIES_META section's stored bytes of `golden_v2_multi_schema.bin`,
/// which corrupts either that section's crc32c (checked before any
/// SERIES_META parsing) or, if the flip lands past decompression on
/// already-parsed structure, one of the typed `SegmentError` grammar
/// violations `decode_catalog_v2` enforces (docs/segment-format.md "RSEG v2
/// amendment"): either way, a typed, printed error, not a panic.
#[test]
fn corrupt_v2_object_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v2_multi_schema.bin")).expect("reads fixture");
    let mut corrupt = good.clone();
    // SERIES_META section per the captured golden fixture's footer: offset
    // 171, len 85 (see tests/fixtures/golden_v2_multi_schema_inspect.txt's
    // `kind=6 name=SERIES_META offset=171 len=85`). Flip a byte in the
    // middle of its stored (zstd-compressed) bytes: this is caught by
    // `decode_section_bytes`'s crc32c check before any SERIES_META grammar
    // is even parsed, so this exercises the section-crc error path
    // specifically.
    let flip_at = 171 + 40;
    assert!(
        flip_at < corrupt.len(),
        "flip offset must land inside the object"
    );
    corrupt[flip_at] ^= 0xFF;

    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ravel-cli-corrupt-v2-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, &corrupt).expect("writes corrupt fixture");

    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a corrupt v2 object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("section crc32c mismatch")
            || stderr.contains("failed to decode series catalog"),
        "expected the typed SegmentError text on stderr, got: {stderr}"
    );
}

#[test]
fn v3_mixed_scalar_and_histogram_inspect_output_matches_golden_fixture() {
    let output = run_inspect(&segment_fixture("golden_v3_mixed_scalar_and_histogram.bin"));
    assert!(
        output.status.success(),
        "ravel-cli segment inspect failed on a known-good v3 fixture, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("golden_v3_mixed_scalar_and_histogram_inspect.txt");
    assert_eq!(stdout, expected, "v3 `segment inspect` output regressed");
    assert!(
        expected.contains("HIST_PAGES") && expected.contains("value_kind=HIST_SPANS"),
        "this fixture is named for exercising v3's histogram series; if the \
         golden text stops containing HIST_PAGES/HIST_SPANS, the fixture or \
         the v3 dispatch silently stopped exercising it"
    );
}

/// A corrupt v3 object must fail with the typed `SegmentError` text on
/// stderr and a non-zero exit status, never a panic. Flips one byte inside
/// the SERIES_META section's stored bytes of
/// `golden_v3_mixed_scalar_and_histogram.bin`, which corrupts that
/// section's crc32c, checked before any SERIES_META (including the v3-only
/// `value_kind`/`hist_page_gap`/`hist_page_len` columns) is parsed.
#[test]
fn corrupt_v3_series_meta_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v3_mixed_scalar_and_histogram.bin"))
        .expect("reads fixture");
    let mut corrupt = good.clone();
    // SERIES_META section per the captured golden fixture's footer: offset
    // 113, len 61 (see tests/fixtures/
    // golden_v3_mixed_scalar_and_histogram_inspect.txt's `kind=6
    // name=SERIES_META offset=113 len=61`).
    let flip_at = 113 + 30;
    assert!(
        flip_at < corrupt.len(),
        "flip offset must land inside the object"
    );
    corrupt[flip_at] ^= 0xFF;

    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ravel-cli-corrupt-v3-meta-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, &corrupt).expect("writes corrupt fixture");

    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a corrupt v3 object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("section crc32c mismatch")
            || stderr.contains("failed to decode series catalog"),
        "expected the typed SegmentError text on stderr, got: {stderr}"
    );
}

/// A corrupt HIST_PAGES payload (as opposed to a corrupt SERIES_META
/// section above) must also fail with the typed error, never a panic. This
/// exercises the page-crc check inside `decode_histogram_pages`
/// specifically (docs/rseg-v3-plan.md section 4's checksum coverage
/// review: "HIST_PAGES page header + payload ... per page, on the
/// selective access path"), a different failure path than the
/// SERIES_META-section corruption test above.
#[test]
fn corrupt_v3_hist_page_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v3_mixed_scalar_and_histogram.bin"))
        .expect("reads fixture");
    let mut corrupt = good.clone();
    // HIST_PAGES section per the captured golden fixture's footer: offset
    // 240, len 32 (see tests/fixtures/
    // golden_v3_mixed_scalar_and_histogram_inspect.txt's `kind=7
    // name=HIST_PAGES offset=240 len=32`). Flip a byte inside the one
    // page's payload, past its 6-byte header (enc/comp/crc), so this is
    // caught by the page crc check, not the section crc.
    let flip_at = 240 + 10;
    assert!(
        flip_at < corrupt.len(),
        "flip offset must land inside the object"
    );
    corrupt[flip_at] ^= 0xFF;

    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ravel-cli-corrupt-v3-hist-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, &corrupt).expect("writes corrupt fixture");

    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a corrupt v3 object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("page crc32c mismatch")
            || stderr.contains("failed to decode histogram pages"),
        "expected the typed SegmentError text on stderr, got: {stderr}"
    );
}

/// Truncating a v2 object (cutting bytes off the end, so the trailer itself
/// is gone) must also fail with a typed error, never a panic. Exercises a
/// different failure path than the byte-flip test above (`Truncated`/
/// `TooSmall` from `parse_footer`, before any section is even reached).
#[test]
fn truncated_v2_object_prints_typed_error_not_panic() {
    let good = std::fs::read(segment_fixture("golden_v2_multi_schema.bin")).expect("reads fixture");
    let truncated = &good[..good.len() / 2];

    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ravel-cli-truncated-v2-{}-{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, truncated).expect("writes truncated fixture");

    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a truncated v2 object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("failed to parse segment"),
        "expected the typed parse-failure text on stderr, got: {stderr}"
    );
}
