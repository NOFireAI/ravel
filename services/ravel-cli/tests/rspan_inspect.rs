//! Integration tests for `ravel-cli rspan inspect`, run as a subprocess against
//! the built binary (the inspector is private to `main.rs`, and the error-path
//! test needs the printed error text and exit status, not just a typed
//! `Result`, so exercising the real CLI is the only way to check both).
//!
//! RSPAN v1 is the frozen contract in `docs/span-segment-format.md` (ADR-0041).
//! `ravel-rspan` has no on-disk golden object yet, so this test builds one
//! deterministically with `RspanWriter` (the writer's output is byte-identical
//! for identical input) and pins the expected `rspan inspect` stdout as a golden
//! fixture under `tests/fixtures/`. A regression in the inspect output fails
//! this test the way the writer's own determinism test catches a regression in
//! its output.
#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};

/// Path to one of this crate's own golden `rspan inspect` stdout fixtures.
fn inspect_fixture(name: &str) -> String {
    let path = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures")).join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading fixture {}: {err}", path.display()))
}

fn run_inspect(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "rspan", "inspect"])
        .arg(path)
        .output()
        .expect("ravel-cli runs")
}

/// A unique temp path so parallel test runs never collide.
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ravel-cli-{tag}-{}-{}.rspan",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ))
}

fn span(trace: u8, span: u8, start: i64, end: i64, code: StatusCode, name: &str) -> SpanRecord {
    SpanRecord {
        trace_id: [trace; 16],
        span_id: [span; 8],
        parent_span_id: if span == 0 { None } else { Some([span - 1; 8]) },
        name: name.into(),
        start_ts_ns: start,
        end_ts_ns: end,
        status_code: code,
        status_message: match code {
            StatusCode::Error => Some("boom".into()),
            _ => None,
        },
        attrs: vec![("service.name".into(), format!("svc{trace}"))],
    }
}

/// A small deterministic two-trace, two-block object. `block_target_records` is
/// 2 so the four sorted records split into two blocks, exercising the per-block
/// skip listing without needing thousands of records.
fn build_object() -> Vec<u8> {
    let cfg = RspanConfig {
        block_target_records: 2,
        ..RspanConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [0xabu8; 16],
        shard: 3,
        writer_id: [0xcdu8; 16],
        writer_epoch: 7,
        writer_seq: 42,
    };
    let mut w = RspanWriter::new(cfg, identity);
    for r in [
        span(1, 0, 100, 200, StatusCode::Ok, "GET /api"),
        span(1, 1, 120, 180, StatusCode::Unset, "db.query"),
        span(2, 0, 150, 400, StatusCode::Error, "POST /login"),
        span(2, 1, 160, 300, StatusCode::Ok, "auth.check"),
    ] {
        w.push(r);
    }
    w.finish().expect("finish")
}

#[test]
fn rspan_inspect_output_matches_golden_fixture() {
    let path = temp_path("golden");
    std::fs::write(&path, build_object()).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        output.status.success(),
        "ravel-cli rspan inspect failed on a known-good object, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("rspan_inspect.txt");
    assert_eq!(
        stdout, expected,
        "`rspan inspect` output regressed; the RSPAN format is frozen \
         (docs/span-segment-format.md, currently trailer v2) -- this must not \
         change without a version bump and ADR"
    );
    assert!(
        expected.contains("name=SKIP_IDX")
            && expected.contains("name=BLOCKS")
            && expected.contains("skip_index"),
        "the golden fixture stopped exercising the section and skip-index listing"
    );
}

/// A corrupt SKIP_IDX must surface a typed `Corrupted` error and a non-zero
/// exit, never a panic. SKIP_IDX corruption is loud by design
/// (docs/span-segment-format.md "Pruning soundness"): its bytes carry the block
/// framing and per-block checksums, so a whole-section crc catches the flip
/// before any skip-entry grammar is parsed.
#[test]
fn corrupt_skip_index_prints_typed_error_not_panic() {
    let good = build_object();
    let footer = ravel_rspan::open(&good).expect("known-good object opens");
    let skip = footer
        .section(ravel_rspan::footer::kind::SKIP_IDX)
        .expect("object has a SKIP_IDX section");
    let flip_at = skip.offset as usize + (skip.len as usize / 2);
    let mut corrupt = good.clone();
    assert!(
        flip_at < corrupt.len(),
        "flip offset lands inside the object"
    );
    corrupt[flip_at] ^= 0xFF;

    let path = temp_path("corrupt-skip");
    std::fs::write(&path, &corrupt).expect("writes corrupt object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a corrupt object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("crc mismatch") || stderr.contains("skip index"),
        "expected the typed SKIP_IDX corruption text on stderr, got: {stderr}"
    );
}

/// Truncating the object (cutting the trailer off) fails through the footer open
/// protocol with a typed error, never a panic.
#[test]
fn truncated_object_prints_typed_error_not_panic() {
    let good = build_object();
    let truncated = &good[..good.len() / 2];

    let path = temp_path("truncated");
    std::fs::write(&path, truncated).expect("writes truncated object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        !output.status.success(),
        "a truncated object must not be reported as successfully inspected"
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("failed to parse rspan"),
        "expected the typed parse-failure text on stderr, got: {stderr}"
    );
}
