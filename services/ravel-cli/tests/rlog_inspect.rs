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

use ravel_logseg::footer::{SortBucketWidth, SortDescriptor, SortKeyColumn, SortKeyType};
use ravel_logseg::{
    AttrValue, BloomScope, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter,
};

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
        stream_attrs: ravel_logseg::stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(format!("s{stream}")))],
            "scope",
            "1",
            &[],
        ),
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
    build_object_with(|w| w)
}

/// [`build_object`]'s records through a writer `configure` adjusts first.
fn build_object_with(configure: impl FnOnce(RlogWriter) -> RlogWriter) -> Vec<u8> {
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
    let mut w =
        configure(RlogWriter::new(cfg, identity).with_indexed_fields(vec!["svc".to_string()]));
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
        "`rlog inspect` output regressed; the RLOG format is frozen \
         (docs/log-segment-format.md, currently trailer v5) -- this must not \
         change without a version bump and ADR"
    );
    assert!(
        expected.contains("name=SKIP_IDX")
            && expected.contains("name=PAGE_DIR")
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

/// Pins where the printed `version:` line comes from: bytes 8..10 of the
/// 16-byte trailer, little-endian, read by `footer::trailer_version`, which is
/// what `rlog inspect` prints.
///
/// The end-to-end half cannot tell the trailer from the build's format
/// constant: the reader accepts one version (version 5 refuses version 4
/// objects) and every object carries it. The byte-offset half below is the
/// part that can fail: it patches the trailer's version bytes and calls the
/// byte reader directly, without asking the reader to open a version it would
/// refuse.
#[test]
fn rlog_inspect_version_line_reads_the_trailer_bytes() {
    let bytes = build_object();
    let at = bytes.len() - 16 + 8;
    let trailer_version = u16::from_le_bytes([bytes[at], bytes[at + 1]]);

    let mut patched = bytes.clone();
    patched[at..at + 2].copy_from_slice(&0xBEEFu16.to_le_bytes());
    assert_eq!(
        ravel_logseg::footer::trailer_version(&patched).expect("trailer present"),
        0xBEEF
    );

    let path = temp_path("trailer-version");
    std::fs::write(&path, &bytes).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let printed: Vec<&str> = stdout
        .lines()
        .filter_map(|l| l.strip_prefix("version: "))
        .collect();
    assert_eq!(printed, vec![trailer_version.to_string().as_str()]);
}

/// The object [`build_object_with`] writes under a two-column key, SixHours,
/// clustering generation 5, and bloom scope text.
fn build_clustered_object() -> Vec<u8> {
    build_object_with(|w| {
        w.with_sort_descriptor(
            Some(SortDescriptor {
                bucket_width: SortBucketWidth::SixHours,
                key_columns: vec![
                    SortKeyColumn {
                        name: "svc".to_string(),
                        ty: SortKeyType::Str,
                    },
                    SortKeyColumn {
                        name: "code".to_string(),
                        ty: SortKeyType::I64,
                    },
                ],
            }),
            5,
        )
        .with_bloom_scope(BloomScope::Text)
    })
}

fn inspect_stdout(bytes: &[u8], tag: &str) -> String {
    let path = temp_path(tag);
    std::fs::write(&path, bytes).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);
    assert!(
        output.status.success(),
        "ravel-cli rlog inspect failed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is UTF-8")
}

/// The lines from `header` through the end of its indented block.
fn block<'a>(stdout: &'a str, header: &str) -> Vec<&'a str> {
    let mut lines = stdout.lines().skip_while(|l| !l.starts_with(header));
    let Some(first) = lines.next() else {
        return Vec::new();
    };
    std::iter::once(first)
        .chain(lines.take_while(|l| l.starts_with("  ")))
        .collect()
}

#[test]
fn rlog_inspect_prints_the_sort_descriptor_generation_and_bloom_coverage() {
    let clustered = inspect_stdout(&build_clustered_object(), "clustered");
    assert_eq!(
        block(&clustered, "sort_descriptor: "),
        [
            "sort_descriptor: bucket_width=6h key_columns=2",
            "  key[0] name=svc type=str",
            "  key[1] name=code type=i64",
        ]
    );
    assert_eq!(
        block(&clustered, "clustering_generation: "),
        ["clustering_generation: 5"]
    );
    // Scope text covers only the two fixed text columns, not `svc`.
    assert_eq!(
        block(&clustered, "bloom_coverage "),
        [
            "bloom_coverage (2 column(s)):",
            "  column_id=4 name=severity_text kind=fixed",
            "  column_id=5 name=body kind=fixed",
        ]
    );

    // Scope all adds the string attribute column, named through FIELD_DIR.
    let unclustered = inspect_stdout(&build_object(), "unclustered");
    assert_eq!(
        block(&unclustered, "sort_descriptor: "),
        ["sort_descriptor: none"]
    );
    assert_eq!(
        block(&unclustered, "clustering_generation: "),
        ["clustering_generation: 0"]
    );
    assert_eq!(
        block(&unclustered, "bloom_coverage "),
        [
            "bloom_coverage (3 column(s)):",
            "  column_id=4 name=severity_text kind=fixed",
            "  column_id=5 name=body kind=fixed",
            "  column_id=11 name=svc kind=str",
        ]
    );
}

/// A version-4 object is refused before any footer field is printed, so none
/// of the ADR-2135 lines appear for it.
#[test]
fn rlog_inspect_prints_no_clustering_lines_for_a_v4_object() {
    let v5 = build_clustered_object();
    let footer = ravel_logseg::footer::open(&v5).expect("v5 object opens");
    let n = v5.len();
    let footer_len = u32::from_le_bytes(v5[n - 16..n - 12].try_into().expect("4 bytes"));
    let mut v4 = v5[..n - 16 - footer_len as usize].to_vec();
    ravel_logseg::footer::write_footer_and_trailer_versioned(&mut v4, &footer, 4);
    assert_eq!(
        ravel_logseg::footer::trailer_version(&v4).expect("trailer present"),
        4
    );

    let path = temp_path("v4");
    std::fs::write(&path, &v4).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);

    assert!(!output.status.success(), "a v4 object must be refused");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("failed to parse rlog segment: unsupported format version 4"),
        "expected the typed version refusal on stderr, got: {stderr}"
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        !stdout.contains("sort_descriptor") && !stdout.contains("clustering_generation"),
        "a v4 object printed a clustering line: {stdout}"
    );
}

/// Regenerates `tests/fixtures/rlog_inspect.txt` after a deliberate, versioned
/// format change (never for an internal refactor). Run explicitly:
///   cargo test -p ravel-cli --test rlog_inspect -- --ignored capture
#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_rlog_inspect_fixture() {
    let path = temp_path("capture");
    std::fs::write(&path, build_object()).expect("writes object");
    let output = run_inspect(&path);
    let _ = std::fs::remove_file(&path);
    assert!(output.status.success());
    std::fs::write(
        PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/rlog_inspect.txt"
        )),
        output.stdout,
    )
    .expect("writes fixture");
}
