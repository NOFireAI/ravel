//! Integration tests for `ravel-cli rlog inspect`, run as a subprocess against
//! the built binary (the inspector is private to `main.rs`, and the error-path
//! test needs the printed error text and exit status, not just a typed
//! `Result`, so exercising the real CLI is the only way to check both).
//!
//! RLOG v1 is the frozen contract in `docs/log-segment-format.md` (ADR-0029).
//! Unlike the RSEG inspector, which reads a checked-in fixture from
//! `ravel-segment`'s corpus, `ravel-logseg` has no on-disk golden object yet, so
//! this test builds one deterministically with `RlogWriter` (the writer's output
//! is byte-identical for identical input, task 12) and pins the expected
//! `rlog inspect` stdout as a golden fixture under `tests/fixtures/`. A
//! regression in the inspect output fails this test the way the writer's own
//! golden-bytes test catches a regression in its output.
#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};

/// Path to one of this crate's own golden `rlog inspect` stdout fixtures.
fn inspect_fixture(name: &str) -> String {
    let path = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures")).join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading fixture {}: {err}", path.display()))
}

fn run_inspect(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["--store", "memory", "rlog", "inspect"])
        .arg(path)
        .output()
        .expect("ravel-cli runs")
}

/// A unique temp path so parallel test runs never collide.
fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ravel-cli-{tag}-{}-{}.rlog",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ))
}

fn sid(n: u8) -> LogStreamId {
    let mut a = [0u8; 16];
    a[0] = n;
    LogStreamId(a)
}

fn rec(stream: u8, ts: i64, severity: u8, body: &str, svc: &str, code: i64) -> LogRecord {
    LogRecord {
        stream_id: sid(stream),
        ts_ns: ts,
        observed_ts_ns: ts + 5,
        severity_num: severity,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("svc".into(), AttrValue::Str(svc.into())),
            ("code".into(), AttrValue::I64(code)),
        ],
    }
}

/// A small deterministic two-stream, two-block object. `block_target_records`
/// is 2 so the four sorted records split into two blocks, exercising the
/// per-block skip listing without needing thousands of records.
fn build_object() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: 2,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: [0xabu8; 16],
        shard: 3,
        writer_id: [0xcdu8; 16],
        writer_epoch: 7,
        writer_seq: 42,
    };
    let mut w = RlogWriter::new(cfg, identity);
    for r in [
        rec(1, 100, 9, "get /api ok", "api", 200),
        rec(1, 200, 17, "get /api timeout", "api", 504),
        rec(2, 150, 9, "post /login ok", "auth", 200),
        rec(2, 250, 13, "post /login fail", "auth", 401),
    ] {
        w.push(r).expect("push");
    }
    w.finish().expect("finish")
}

#[test]
fn rlog_inspect_output_matches_golden_fixture() {
    let path = temp_path("golden");
    std::fs::write(&path, build_object()).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(
        output.status.success(),
        "ravel-cli rlog inspect failed on a known-good object, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let expected = inspect_fixture("rlog_inspect.txt");
    assert_eq!(
        stdout, expected,
        "`rlog inspect` output regressed; RLOG v1 is frozen \
         (docs/log-segment-format.md) -- this must not change without a version \
         bump and ADR"
    );
    assert!(
        expected.contains("name=SKIP_IDX")
            && expected.contains("stream_dir")
            && expected.contains("field_dir"),
        "the golden fixture stopped exercising a section listing"
    );
}

/// A corrupt SKIP_IDX must surface a typed `Corrupted` error and a non-zero
/// exit, never a panic. SKIP_IDX corruption is loud by design
/// (docs/log-segment-format.md "Pruning soundness"): its bytes carry the block
/// framing and per-block checksums, so a whole-section crc catches the flip
/// before any skip-entry grammar is parsed.
#[test]
fn corrupt_skip_index_prints_typed_error_not_panic() {
    let good = build_object();
    let footer = ravel_logseg::footer::open(&good).expect("known-good object opens");
    let skip = footer
        .section(ravel_logseg::footer::kind::SKIP_IDX)
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
        stderr.contains("crc32c mismatch") || stderr.contains("skip index"),
        "expected the typed SKIP_IDX corruption text on stderr, got: {stderr}"
    );
}

/// Truncating the object (cutting the trailer off) fails through the footer
/// open protocol with a typed error, never a panic.
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
        stderr.contains("failed to parse rlog"),
        "expected the typed parse-failure text on stderr, got: {stderr}"
    );
}
