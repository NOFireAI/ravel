//! The startup parse of `ravel-server`'s flags fits a small thread stack
//! (issue #3973).
//!
//! clap's derive builds every flag of one `Args` struct in a single
//! `augment_args` frame, and an unoptimized build gives each flag its own
//! stack slots, so one struct holding every flag grew that frame by about
//! 8 KB per flag, past 1.5 MiB. `Cli` is split into flattened groups so no
//! single frame holds most of the flags. This test parses a realistic
//! argument list on a 512 KiB thread, a quarter of the 2 MiB default for a
//! spawned thread. A stack overflow aborts the whole process rather than
//! failing one test, so the parse runs in a child process (this same test
//! binary, re-run with [`CHILD_ENV`] set) and the parent asserts on how the
//! child exited.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::Command;

use ravel_server::config::Cli;

/// Set in the child's environment: the child runs the parse itself instead
/// of spawning another child.
const CHILD_ENV: &str = "RAVEL_CLI_SMALL_STACK_CHILD";

/// The stack the parse thread gets.
const STACK_BYTES: usize = 512 * 1024;

/// Printed by the child after a successful parse, so the parent can tell a
/// child that parsed from one that ran no test at all.
const PARSED_MARKER: &str = "small-stack parse ok";

/// A flag from most areas of the command line: listeners, store, tenancy,
/// audit, fold and maintenance, retention, alerting, OIDC and mTLS, limits,
/// SQL and SQL spill, query fan-out, ingest and flush, MCP, agents, caches,
/// CPU and memory gates, GC, distribution, federation and store scheduling.
/// Every flag here exists in a default-feature build.
const ARGV: &[&str] = &[
    "ravel-server",
    "--mode",
    "all",
    "--listen-http",
    "127.0.0.1:8080",
    "--listen-grpc",
    "127.0.0.1:4317",
    "--listen-health",
    "127.0.0.1:8081",
    "--store",
    "memory",
    "--shards",
    "8",
    "--tenant-token",
    "tok-a=acme",
    "--maintain-tenant",
    "acme",
    "--format-window-check-timeout",
    "5s",
    "--audit-mode",
    "best-effort",
    "--audit-max-batch",
    "64",
    "--s3-region",
    "us-east-1",
    "--fold-interval-secs",
    "120",
    "--fold-key-index-leaves",
    "--fold-key-index-build-max-pairs",
    "1000",
    "--maintain-interval-secs",
    "60",
    "--maintain-unit-concurrency",
    "2",
    "--maintain-claim-lease",
    "5m",
    "--retention-default",
    "30d",
    "--alert-eval-interval-secs",
    "30",
    "--alert-sql-lookback",
    "10m",
    "--oidc-jwks-refresh-interval-secs",
    "600",
    "--max-concurrent-queries",
    "16",
    "--max-s3-requests",
    "4096",
    "--sql-max-query-bytes",
    "268435456",
    "--sql-spill",
    "off",
    "--sql-spill-merge-fan-in",
    "4",
    "--logs-max-fetch-run-bytes",
    "1048576",
    "--fetch-concurrency",
    "8",
    "--max-series",
    "100000",
    "--max-inflight-ingest-requests",
    "256",
    "--max-pending-strict-acks",
    "1024",
    "--max-inflight-flushes",
    "4",
    "--max-queued-flushes",
    "64",
    "--max-flush-delay",
    "10s",
    "--mcp-max-body-bytes",
    "65536",
    "--agents-poll-concurrency",
    "2",
    "--memory-budget-bytes",
    "1073741824",
    "--cache-max-bytes",
    "268435456",
    "--decoded-cache-max-bytes",
    "67108864",
    "--cpu-gate-read-permits",
    "4",
    "--memory-gate-interval-ms",
    "500",
    "--gc-grace",
    "1h",
    "--shutdown-timeout",
    "30s",
    "--max-inflight-fragments",
    "8",
    "--max-parallel-slices",
    "4",
    "--remote-cluster-soft-timeout",
    "2s",
    "--store-fg-permits",
    "32",
];

/// The child's half: parse [`ARGV`] on a [`STACK_BYTES`] thread through the
/// same entry point `main` uses, and check a value from several groups so a
/// parse that silently dropped a group's flags fails here too.
fn parse_on_small_stack() {
    let handle = std::thread::Builder::new()
        .name("small-stack-parse".to_string())
        .stack_size(STACK_BYTES)
        .spawn(|| Cli::parse_validated_from(ARGV).map(|cli| format!("{cli:?}")))
        .expect("spawn the parse thread");
    let debug = handle
        .join()
        .expect("parse thread panicked")
        .expect("the argument list parses");
    for expected in [
        "mode: All",
        "shards: 8",
        "fold_interval_secs: 120",
        "max_s3_requests: Some(4096)",
        "sql_spill_merge_fan_in: 4",
        "sql_spill_merge_fan_in_passed: true",
        "max_series: Some(100000)",
        "max_pending_strict_acks: 1024",
        "decoded_cache_max_bytes: 67108864",
        "max_parallel_slices: 4",
        "store_fg_permits: 32",
    ] {
        assert!(
            debug.contains(expected),
            "parsed flags lack `{expected}`: {debug}"
        );
    }
    println!("{PARSED_MARKER}");
}

#[test]
fn cli_parses_on_a_512_kib_thread() {
    if std::env::var_os(CHILD_ENV).is_some() {
        parse_on_small_stack();
        return;
    }
    let exe = std::env::current_exe().expect("current test binary");
    let output = Command::new(exe)
        .args([
            "--exact",
            "cli_parses_on_a_512_kib_thread",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, "1")
        .output()
        .expect("run the child test process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "parsing the flags on a {STACK_BYTES}-byte stack failed ({}); if stderr \
         says the thread overflowed its stack, a clap `augment_args` frame has \
         grown back toward the whole stack\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains(PARSED_MARKER),
        "the child exited 0 without running the parse\nstdout:\n{stdout}"
    );
}
