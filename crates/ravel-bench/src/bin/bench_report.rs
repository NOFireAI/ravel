//! Machine-readable benchmark report CLI (epic #1053, T2). Runs one
//! ingest-then-query workload via `ravel_bench::report::run` and writes a
//! single JSON document to `--out`, so a performance/cost number can be
//! committed next to the command and environment that produced it instead of
//! pasted from a log.
//!
//! `--store memory|s3` selects the backend (`ravel_bench::harness`); the `s3`
//! store reads `RAVEL_S3_*` env vars. The emitted `environment.store_backend`
//! records which backend actually ran, and `s3_requests.backend_bills_requests`
//! records whether its requests are billed -- true only for S3, per ADR-0075
//! decision 3.
//!
//! Provenance (`git_commit`, `toolchain`) is gathered here, not in the library:
//! `git rev-parse HEAD` (overridable via `GITHUB_SHA`, which CI sets) and
//! `rustc --version`. The library stays subprocess-free and deterministic for
//! its test.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::Command;

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::report::{ReportRunConfig, WorkloadShape, run};

#[derive(Parser, Debug)]
#[command(about = "Emit a machine-readable ravel benchmark report as JSON")]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 500)]
    max_flush_delay_ms: u64,
    #[arg(long, default_value_t = 1_000)]
    target_series: usize,
    #[arg(long, default_value_t = 50_000)]
    points_per_sec: u64,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 200)]
    batch_size: usize,
    #[arg(long, default_value_t = 5)]
    ack_timeout_secs: u64,
    /// PromQL instant selector run for the query-latency numbers; must match
    /// the workload generator's metric name.
    #[arg(long, default_value = "bench_gauge")]
    query: String,
    #[arg(long, default_value_t = 20)]
    warm_query_count: usize,
    /// Path the JSON report is written to. Defaults under `bench/reports/`,
    /// the directory CI already treats as committed benchmark evidence (see
    /// `.github/workflows/ci.yml`'s docs-only classifier), so a future change
    /// can land numbers alongside the commands that produced them.
    #[arg(long, default_value = "bench/reports/report.json")]
    out: PathBuf,
}

/// Best-effort command output, trimmed. Returns `None` on any failure so the
/// caller can decide the fallback; provenance must never be a silent wrong
/// value.
fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn git_commit() -> String {
    // CI (`GITHUB_SHA`) wins so a report from a checked-out detached HEAD still
    // names the branch tip; otherwise ask git directly.
    if let Ok(sha) = std::env::var("GITHUB_SHA")
        && !sha.trim().is_empty()
    {
        return sha.trim().to_string();
    }
    command_stdout("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string())
}

fn toolchain() -> String {
    command_stdout("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let bills = matches!(args.store, StoreKind::S3);
    let region = match args.store {
        StoreKind::Memory => "n/a-memory".to_string(),
        StoreKind::S3 => std::env::var("RAVEL_S3_REGION").unwrap_or_else(|_| "unknown".to_string()),
    };

    let config = ReportRunConfig {
        store: store_from_env(args.store),
        store_backend: args.store.to_string(),
        region,
        backend_bills_requests: bills,
        shards: args.shards,
        max_flush_delay_ms: args.max_flush_delay_ms,
        workload: WorkloadShape {
            target_series: args.target_series,
            points_per_sec: args.points_per_sec,
            duration_secs: args.duration_secs,
            batch_size: args.batch_size,
            query: args.query,
            warm_query_count: args.warm_query_count,
        },
        ack_timeout_secs: args.ack_timeout_secs,
        git_commit: git_commit(),
        toolchain: toolchain(),
    };

    let report = run(&config).await;
    let json = serde_json::to_string_pretty(&report).expect("serialize report");

    if let Some(parent) = args.out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).expect("create output directory");
    }
    std::fs::write(&args.out, format!("{json}\n")).expect("write report JSON");

    // Also print to stdout so a run's numbers are visible in CI logs without
    // opening the artifact.
    println!("{json}");
    eprintln!("wrote report to {}", args.out.display());
}
