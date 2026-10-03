//! Intra-segment scan-partitioning scaling benchmark (ADR-0102 decision 1,
//! epic #361 item 1). Thin wrapper around
//! `ravel_bench::logs_scan_scaling::run`: parses the sweep parameters and
//! `--store memory|s3` via `ravel_bench::harness` (same convention as
//! `groupby_scaling_bench`), then prints the report as JSON plus a human table.
//!
//! Sweeps DataFusion `target_partitions` (`--target-partitions 1,2,4,8,16`)
//! over one fixed `logs` scan query and a FEW-segment / MANY-block dataset, so
//! the undersubscribed case (segment count < target_partitions) this item fixes
//! is what is measured. Each partition value is run four ways -- read cache
//! wired and un-wired, crossed with ADR-0107's whole-object and above-threshold
//! read shapes (issue #693) -- so the report shows the scan fanning out past the
//! segment count, the object-store GET count on each shape, and the `reads/sg`
//! and `bytes_x` columns that turn each row's readings into a single
//! amplification factor.
//!
//! Report-only: a new standalone measurement tool, not wired into any
//! production path. Gated on the `sql-latency` feature so the default build
//! never compiles ravel-sql/datafusion or this bin.
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin logs_scan_scaling_bench -- --store memory`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::logs_scan_scaling::{LogsScanScalingConfig, Report, run};

#[derive(Parser, Debug)]
#[command(
    about = "Intra-segment scan-partitioning scaling benchmark (target_partitions sweep, cache on/off)"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// RLOG objects the tenant is split across. Smaller than the largest
    /// `--target-partitions` so the undersubscribed case is exercised, but large
    /// enough that the planning prune's per-segment serialized await is
    /// measurable.
    #[arg(long, default_value_t = 32)]
    segments: usize,
    /// Records per object. With a small block target this sets the block count
    /// the striping fans out across.
    #[arg(long, default_value_t = 4_000)]
    records_per_object: usize,
    /// Writer block target. Small so each segment holds many blocks.
    #[arg(long, default_value_t = 64)]
    block_target_records: usize,
    /// Log body length in bytes. Must be large enough that each published object
    /// clears ADR-0107's 512 KiB block-range threshold, or the `over_threshold`
    /// rows measure a whole-object read under an above-threshold label.
    #[arg(long, default_value_t = 256)]
    body_bytes: usize,
    /// Comma-separated DataFusion `target_partitions` values to sweep. Straddle
    /// `--segments` so the report shows both the un-cached cap binding and the
    /// cache-wired fan-out continuing past it.
    #[arg(long, value_delimiter = ',', default_value = "1,8,32,64,128")]
    target_partitions: Vec<usize>,
    /// Timed repetitions per combination.
    #[arg(long, default_value_t = 5)]
    runs: usize,
    /// Per-query wall deadline in seconds.
    #[arg(long, default_value_t = 30)]
    deadline_secs: u64,
    /// Small dataset and few partition values for a fast CI-sized run.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let config = if args.smoke {
        LogsScanScalingConfig {
            deadline: Duration::from_secs(args.deadline_secs),
            ..LogsScanScalingConfig::smoke(store_from_env(args.store), &args.store.to_string())
        }
    } else {
        LogsScanScalingConfig {
            store: store_from_env(args.store),
            store_label: args.store.to_string(),
            segments: args.segments,
            records_per_object: args.records_per_object,
            block_target_records: args.block_target_records,
            body_bytes: args.body_bytes,
            target_partitions: args.target_partitions,
            runs: args.runs,
            deadline: Duration::from_secs(args.deadline_secs),
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
    println!("logs_scan_scaling_bench report");
    println!("  store               : {}", report.config.store);
    println!("  cores               : {}", report.config.cores);
    println!("  profile             : {}", report.config.profile);
    println!("  query               : {}", report.config.query);
    println!("  segments            : {}", report.config.segments);
    println!(
        "  records_per_object  : {}",
        report.config.records_per_object
    );
    println!(
        "  block_target_records: {}",
        report.config.block_target_records
    );
    println!("  body_bytes          : {}", report.config.body_bytes);
    println!("  total_records       : {}", report.config.total_records);
    println!("  dataset_bytes       : {}", report.config.dataset_bytes);
    println!(
        "  min_object_bytes    : {} (over-threshold rows use block-range \
         threshold {}; whole-object rows use {})",
        report.config.min_object_bytes,
        report.config.over_threshold_block_range_threshold,
        report.config.whole_object_block_range_threshold,
    );
    println!("  runs per combo      : {}", report.config.runs);
    println!();
    println!(
        "  planning prune ({} segments, {} blocks): {:.3} ms serial (what \
         compute_plan_counts pays), {:.3} ms concurrent",
        report.planning.segments,
        report.planning.total_blocks,
        report.planning.serial_ms,
        report.planning.concurrent_ms,
    );
    println!();
    println!(
        "{:>7} | {:>6} | {:>7} | {:>8} | {:>9} | {:>8} | {:>7} | {:>8} | {:>9} | {:>9} | {:>10} | {:>10} | {:>10} | {:>14}",
        "target",
        "cache",
        "overthr",
        "scanpart",
        "nonempty",
        "segments",
        "s3_gets",
        "cachehit",
        "reads/sg",
        "bytes_x",
        "med (ms)",
        "min (ms)",
        "max (ms)",
        "rows/sec",
    );
    println!(
        "{:->7}-+-{:->6}-+-{:->7}-+-{:->8}-+-{:->9}-+-{:->8}-+-{:->7}-+-{:->8}-+-{:->9}-+-{:->9}-+-{:->10}-+-{:->10}-+-{:->10}-+-{:->14}",
        "", "", "", "", "", "", "", "", "", "", "", "", "", ""
    );
    for c in &report.combos {
        if let Some(err) = &c.error {
            println!(
                "{:>7} | {:>6} | {:>7} | FAILED: {}",
                c.target_partitions, c.cache_wired, c.over_threshold, err,
            );
            continue;
        }
        println!(
            "{:>7} | {:>6} | {:>7} | {:>8} | {:>9} | {:>8} | {:>7} | {:>8} | {:>9.2} | {:>9.2} | {:>10.3} | {:>10.3} | {:>10.3} | {:>14.0}",
            c.target_partitions,
            c.cache_wired,
            c.over_threshold,
            c.scan_partitions,
            c.non_empty_partitions,
            c.segments_scanned,
            c.object_store_get_requests,
            c.cache_hits,
            c.reads_per_segment,
            c.bytes_amplification,
            c.median_ms,
            c.min_ms,
            c.max_ms,
            c.rows_per_sec,
        );
    }
}
