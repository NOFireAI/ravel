//! ADR-0103 (epic #64) `count_over_time` pushdown crossover benchmark. Thin
//! wrapper around `ravel_bench::pushdown_crossover::run`: parses the series
//! sweep and repetition count plus `--store memory|s3` via
//! `ravel_bench::harness` (same convention as `distrib_crossover_bench`), runs
//! the sweep, and prints the report as JSON plus a human table.
//!
//! Both arms (pushdown-eligible and pushdown-ineligible) run over the same
//! corpus at each series count through the real ADR-0071 distributed loopback
//! worker; on `MemoryStore` this measures the pushdown decision's CPU/transport
//! overhead against a zero-latency store, a lower bound on its benefit (see the
//! module docs). A latency-regime run uses `--store s3`.
//!
//! Run (full sweep, a separate out-of-band measurement run):
//! `cargo run -p ravel-bench --release --bin pushdown_crossover_bench -- \
//!   --target-series 10,100,1000,10000`
//!
//! Report-only: a standalone measurement tool, not wired into any production
//! code path.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::pushdown_crossover::{PushdownCrossoverConfig, Report, run};

#[derive(Parser, Debug)]
#[command(
    about = "count_over_time pushdown crossover benchmark (ADR-0103 order-insensitive aggregation pushdown)"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// Comma-separated distinct-series counts to sweep, all under `bench_metric`.
    #[arg(long, value_delimiter = ',', default_value = "10,100,1000,10000")]
    target_series: Vec<usize>,
    /// Samples per series per segment (two segments); total in-window samples
    /// per series is twice this. Must stay below 150.
    #[arg(long, default_value_t = 100)]
    samples_per_series: usize,
    /// Timed instant queries per (target_series, arm), for the percentiles.
    #[arg(long, default_value_t = 20)]
    query_reps: usize,
    /// Per-query wall deadline in seconds.
    #[arg(long, default_value_t = 30)]
    deadline_secs: u64,
    /// Small series counts and few reps for a fast CI-sized run. Overrides
    /// `--target-series`/`--samples-per-series`/`--query-reps`.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = if args.smoke {
        PushdownCrossoverConfig {
            deadline_secs: args.deadline_secs,
            ..PushdownCrossoverConfig::smoke(store_from_env(args.store), args.store.to_string())
        }
    } else {
        PushdownCrossoverConfig {
            store: store_from_env(args.store),
            store_label: args.store.to_string(),
            target_series: args.target_series,
            samples_per_series: args.samples_per_series,
            query_reps: args.query_reps,
            deadline_secs: args.deadline_secs,
        }
    };
    let report = run(&config).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!();
    println!("pushdown_crossover_bench report");
    println!("  store             : {}", report.config.store);
    println!("  query             : {}", report.config.query);
    println!("  samples_per_series: {}", report.config.samples_per_series);
    println!("  query_reps        : {}", report.config.query_reps);
    println!("  target_series     : {:?}", report.config.target_series);
    println!();
    println!(
        "{:>8} | {:>9} | {:>9} | {:>9} | {:>9} | {:>9} | {:>7} | {:>10} | {:>9} | {:>12}",
        "series",
        "eligible",
        "gate",
        "p50 ms",
        "p90 ms",
        "p99 ms",
        "matched",
        "count_sum",
        "s3_reqs",
        "s3_bytes",
    );
    println!(
        "{:->8}-+-{:->9}-+-{:->9}-+-{:->9}-+-{:->9}-+-{:->9}-+-{:->7}-+-{:->10}-+-{:->9}-+-{:->12}",
        "", "", "", "", "", "", "", "", "", "",
    );
    for a in &report.arms {
        println!(
            "{:>8} | {:>9} | {:>9} | {:>9.3} | {:>9.3} | {:>9.3} | {:>7} | {:>10.0} | {:>9} | {:>12}",
            a.target_series,
            a.eligible,
            a.pushdown_eligible,
            a.wall_ms_p50,
            a.wall_ms_p90,
            a.wall_ms_p99,
            a.matched_series,
            a.evaluated_count_sum,
            a.s3_requests,
            a.s3_bytes,
        );
    }
}
