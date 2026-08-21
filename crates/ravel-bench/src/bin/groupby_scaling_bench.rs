//! Group-by aggregation core-count scaling benchmark (ADR-0102 decision 4).
//! Thin wrapper around `ravel_bench::groupby_scaling::run`: parses the sweep
//! parameters and `--store memory|s3` via `ravel_bench::harness` (same
//! convention as `query_latency_bench`/`flight_sql_egress`), then prints the
//! report as JSON plus a human table.
//!
//! Sweeps two axes over one fixed group-by query and cardinality: DataFusion
//! `target_partitions` (`--target-partitions 1,2,4,8`) and
//! `SqlConfig::parallel_final_aggregation` (both states, always). See the
//! module docs for why the dataset is multi-part and why the query is
//! exact-typed.
//!
//! Report-only: a new standalone measurement tool, not wired into any
//! production code path. Gated on the `sql-latency` feature so the default
//! build never compiles ravel-sql/datafusion or this bin.
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin groupby_scaling_bench -- --store memory`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::groupby_scaling::{GroupbyScalingConfig, Report, run};
use ravel_bench::harness::{StoreKind, store_from_env};

#[derive(Parser, Debug)]
#[command(
    about = "Group-by aggregation core-count scaling benchmark (target_partitions x parallel_final_aggregation)"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// RSEG objects the tenant is split across. Should be at least the largest
    /// `--target-partitions` value for the scan to fan out that far.
    #[arg(long, default_value_t = 8)]
    parts: usize,
    /// Total distinct series (equals the group cardinality of the query).
    #[arg(long, default_value_t = 2_000)]
    series: usize,
    #[arg(long, default_value_t = 500)]
    samples_per_series: usize,
    /// Comma-separated DataFusion `target_partitions` values to sweep.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8")]
    target_partitions: Vec<usize>,
    /// Timed repetitions per (partitions x flag) combination.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// Small dataset and few partition values for a fast CI-sized run.
    /// Overrides `--parts`/`--series`/`--samples-per-series`/
    /// `--target-partitions`/`--runs`.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let config = if args.smoke {
        GroupbyScalingConfig::smoke(store_from_env(args.store), &args.store.to_string())
    } else {
        GroupbyScalingConfig {
            store: store_from_env(args.store),
            store_label: args.store.to_string(),
            parts: args.parts,
            series: args.series,
            samples_per_series: args.samples_per_series,
            target_partitions: args.target_partitions,
            runs: args.runs,
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
    println!("groupby_scaling_bench report");
    println!("  store             : {}", report.config.store);
    println!("  query             : {}", report.config.query);
    println!("  parts             : {}", report.config.parts);
    println!("  series (groups)   : {}", report.config.series);
    println!("  samples_per_series: {}", report.config.samples_per_series);
    println!("  total_samples     : {}", report.config.total_samples);
    println!("  result groups     : {}", report.config.groups);
    println!("  runs per combo    : {}", report.config.runs);
    println!();
    println!(
        "{:>7} | {:>8} | {:>9} | {:>10} | {:>10} | {:>10} | {:>16}",
        "target", "parallel", "segments", "min (ms)", "med (ms)", "max (ms)", "rows/sec"
    );
    println!(
        "{:->7}-+-{:->8}-+-{:->9}-+-{:->10}-+-{:->10}-+-{:->10}-+-{:->16}",
        "", "", "", "", "", "", ""
    );
    for c in &report.combos {
        println!(
            "{:>7} | {:>8} | {:>9} | {:>10.3} | {:>10.3} | {:>10.3} | {:>16.0}",
            c.target_partitions,
            c.parallel_final_aggregation,
            c.segments_scanned,
            c.min_ms,
            c.median_ms,
            c.max_ms,
            c.rows_per_sec,
        );
    }
}
