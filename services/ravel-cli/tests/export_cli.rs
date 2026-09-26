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
