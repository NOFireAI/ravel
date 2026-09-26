//! Reachability coverage for `ravel-cli export` through the built binary
//! (issue #1712).
//!
//! `tests/export_logs.rs` proves the export itself round-trips, but it calls
//! the library entry points in-process, because `--store memory` gives each
//! subprocess its own empty store and a load-then-export round trip across two
//! invocations could never see the loaded data. So the wiring from argv to
//! `export::run` is proven here instead: each test picks an argument whose
//! effect is visible in a refusal, and a flag that never reached the command
//! would produce a different message or none at all.
#![allow(clippy::expect_used)]

use std::process::Command;

use ravel_cli::export::unsupported_signal_message;
use ravel_cli::maintain::SignalArg;

/// 2023-11-14T22:13:20Z, the instant `tests/export_logs.rs` builds its window
/// around, in whole seconds so the nanosecond figure below is checkable by eye.
const BASE_RFC3339: &str = "2023-11-14T22:13:20Z";
const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn write_mapping(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path = dir.path().join("mapping.toml");
    std::fs::write(
        &path,
        "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n",
    )
    .expect("write mapping");
    path
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(args)
        .output()
        .expect("ravel-cli runs")
}

/// `--signal metrics` is refused by the binary, with the message that names the
/// bulk-import follow-up metrics export waits on. This is the dispatch arm's
/// only observable behavior that depends on the parsed `--signal`, so a
/// `Command::Export` variant wired to the wrong argument fails here.
#[test]
fn export_signal_metrics_is_refused_through_the_binary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapping = write_mapping(&dir);
    let out = dir.path().join("out.parquet");
    let output = run(&[
        "--store",
        "memory",
        "export",
        "--signal",
        "metrics",
        "--tenant",
        "acme",
        "--start",
        BASE_RFC3339,
        "--end",
        "2023-11-14T22:13:21Z",
        "--parquet",
        &out.display().to_string(),
        "--mapping",
        &mapping.display().to_string(),
    ]);

    assert!(
        !output.status.success(),
        "export --signal metrics must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected = unsupported_signal_message(SignalArg::Metrics)
        .expect("metrics is an unsupported export signal");
    assert_eq!(
        stderr.trim_end(),
        format!("Error: {expected}"),
        "the binary must print exactly the unsupported-signal refusal"
    );
    assert!(
        !out.exists(),
        "a refused export must not create the --parquet file"
    );
}

/// `--start` and `--end` are parsed as RFC 3339 and reach the window check as
/// nanoseconds. The empty-window refusal quotes both bounds, so the exact
/// conversion is asserted rather than the fact that some error appeared.
#[test]
fn export_window_flags_reach_the_command_as_rfc3339_nanoseconds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapping = write_mapping(&dir);
    let out = dir.path().join("out.parquet");
    let output = run(&[
        "--store",
        "memory",
        "export",
        "--signal",
        "logs",
        "--tenant",
        "acme",
        "--start",
        BASE_RFC3339,
        "--end",
        BASE_RFC3339,
        "--parquet",
        &out.display().to_string(),
        "--mapping",
        &mapping.display().to_string(),
    ]);

    assert!(
        !output.status.success(),
        "an empty export window must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.trim_end(),
        format!(
            "Error: --end must be after --start: the export window is half-open [start, end), \
             and [{BASE_NS}, {BASE_NS}) is empty"
        ),
        "both bounds must arrive as the nanoseconds the RFC 3339 instants name"
    );
}

/// Runs `export --signal logs` over a one-second window with `extra`
/// appended to the arguments.
fn run_export_with(dir: &tempfile::TempDir, parquet: &str, extra: &[&str]) -> std::process::Output {
    let mapping = write_mapping(dir);
    let mapping = mapping.display().to_string();
    let mut args = vec![
        "--store",
        "memory",
        "export",
        "--signal",
        "logs",
        "--tenant",
        "acme",
        "--start",
        BASE_RFC3339,
        "--end",
        "2023-11-14T22:13:21Z",
        "--parquet",
        parquet,
        "--mapping",
        &mapping,
    ];
    args.extend_from_slice(extra);
    run(&args)
}

/// `--max-ingest-lag` exists under the name the server uses and parses as a
/// humantime duration. A refused value is the observable effect: clap runs the
/// value parser before the command, so a message naming the flag proves the
/// flag reached the parser this crate wired to it, and a flag that did not
/// exist would be an unknown-argument error instead.
#[test]
fn export_max_ingest_lag_parses_as_a_humantime_duration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out.parquet").display().to_string();
    let output = run_export_with(&dir, &out, &["--max-ingest-lag", "later"]);

    assert!(
        !output.status.success(),
        "--max-ingest-lag with an unparseable duration must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid --max-ingest-lag 'later'"),
        "--max-ingest-lag must be refused by its own parser, got: {stderr}"
    );
}

/// A zero `--max-ingest-lag` is refused, as `ravel-server --max-ingest-lag`
/// refuses it, so an export cannot resolve a window no server's queries do.
#[test]
fn export_refuses_a_zero_max_ingest_lag_like_the_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out.parquet");
    let output = run_export_with(
        &dir,
        &out.display().to_string(),
        &["--max-ingest-lag", "0s"],
    );

    assert!(
        !output.status.success(),
        "--max-ingest-lag 0s must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "--max-ingest-lag '0s' must be a positive duration: ravel-server refuses a zero \
             lag, so no deployment's queries resolve the window a zero lag would"
        ),
        "a zero lag must be refused with the server's reason, got: {stderr}"
    );
    assert!(
        !out.exists(),
        "a refused export must not create the --parquet file"
    );
}

/// `export` takes no `--max-flush-lifetime`: the resolve never reads the
/// flush lifetime (only a fold does), so a flag for it would change nothing.
#[test]
fn export_has_no_max_flush_lifetime_flag() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out.parquet").display().to_string();
    let output = run_export_with(&dir, &out, &["--max-flush-lifetime", "2h"]);

    assert!(
        !output.status.success(),
        "an export given --max-flush-lifetime must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error: unexpected argument '--max-flush-lifetime' found"),
        "--max-flush-lifetime must be an unknown argument to export, got: {stderr}"
    );
}

/// A `--parquet` under `/dev` is refused before the export reads anything:
/// the output is replaced by a rename, which cannot write to a device.
#[test]
fn export_refuses_a_device_output_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run_export_with(&dir, "/dev/stdout", &[]);

    assert!(
        !output.status.success(),
        "export --parquet /dev/stdout must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.trim_end(),
        "Error: --parquet /dev/stdout is under /dev: the export replaces its output by \
         renaming a finished file over it, so it cannot write to a device; pass a regular file \
         path",
    );
}

/// `--tenant` reaches the store layer: with `--store` omitted the defaulted
/// memory store holds nothing, and the walk precondition refuses by naming this
/// command and the tenant it was pointed at.
#[test]
fn export_logs_carries_the_tenant_flag_into_the_store_walk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapping = write_mapping(&dir);
    let out = dir.path().join("out.parquet");
    let output = run(&[
        "export",
        "--signal",
        "logs",
        "--tenant",
        "acme",
        "--start",
        BASE_RFC3339,
        "--end",
        "2023-11-14T22:13:21Z",
        "--parquet",
        &out.display().to_string(),
        "--mapping",
        &mapping.display().to_string(),
    ]);

    assert!(
        !output.status.success(),
        "export against the defaulted empty memory store must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.trim_end(),
        "Error: --store defaulted to memory, which holds no data for tenant \"acme\"; export \
         found no objects there and would have reported a healthy zero-work result. Pass --store \
         s3 (with RAVEL_S3_BUCKET and its credentials) to run against the real bucket, or load \
         data first. An explicit --store memory keeps the zero-count report.",
        "the refusal must name this command and the tenant the flag carried"
    );
    assert!(
        !out.exists(),
        "a refused export must not create the --parquet file"
    );
}
