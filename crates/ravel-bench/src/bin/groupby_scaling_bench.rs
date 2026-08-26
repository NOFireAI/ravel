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
//! ADR-0102 decision 2 requires measuring against a real S3-backed store, not
//! `MemoryStore` (an in-memory store is exactly what ADR-0094's rejected
//! preliminary measurement used), so a real sweep runs with `--store s3` and
//! the `RAVEL_S3_*` env vars set (see `ravel_bench::harness`):
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin groupby_scaling_bench -- --store s3`
//!
//! `--distinct-sweep` switches to the second instrument (issue #680): the same
//! `target_partitions` axis crossed with a distinct-count axis `D`, over the
//! `logs` table, reporting peak memory-pool bytes instead of latency. See
//! `ravel_bench::groupby_scaling::run_distinct`.
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin groupby_scaling_bench -- --distinct-sweep \
//!   --target-partitions 1,4,16,32 --distinct-values 10000,100000,1000000`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use clap::Parser;
use ravel_bench::groupby_scaling::{
    DEFAULT_DEADLINE_SECS, DEFAULT_MAX_TENANT_BYTES, DistinctReport, DistinctScalingConfig,
    GroupbyScalingConfig, Report, run, run_distinct,
};
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
    /// Timed repetitions per (partitions x flag) combination. The default of 3
    /// is a thin sample: publishing an effect the size of ADR-0094's ~14%
    /// regression needs this raised well above the default.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// Per-tenant memory ceiling (bytes) handed to the SQL executor. Default
    /// matches the pre-flag behavior. With the disk manager disabled (ADR-0102
    /// decision 3) an aggregation over this ceiling returns a typed error and
    /// that one combination is recorded as failed; raise it for a real sweep.
    #[arg(long, default_value_t = DEFAULT_MAX_TENANT_BYTES)]
    max_tenant_bytes: usize,
    /// Per-query wall deadline in seconds. Default matches the pre-flag
    /// behavior; raise it for a large production sweep.
    #[arg(long, default_value_t = DEFAULT_DEADLINE_SECS)]
    deadline_secs: u64,
    /// Small dataset and few partition values for a fast CI-sized run.
    /// Overrides `--parts`/`--series`/`--samples-per-series`/
    /// `--target-partitions`/`--runs`.
    #[arg(long, default_value_t = false)]
    smoke: bool,
    /// Run the distinct-key memory sweep (issue #680) instead of the latency
    /// sweep: peak memory-pool bytes for `COUNT(DISTINCT key)` and
    /// `GROUP BY low, COUNT(DISTINCT high)` over the `logs` table, across
    /// `--distinct-values` x `--target-partitions`.
    #[arg(long, default_value_t = false)]
    distinct_sweep: bool,
    /// Comma-separated distinct counts `D` of the high-cardinality key.
    #[arg(long, value_delimiter = ',', default_value = "10000,100000,1000000")]
    distinct_values: Vec<usize>,
    /// RLOG objects the distinct-sweep tenant is split across. Each carries
    /// EVERY distinct value, so the dataset is `objects x D x repeats` rows and
    /// a partition owning one object still sees all `D`. Should be at least the
    /// largest `--target-partitions`.
    #[arg(long, default_value_t = 32)]
    objects: usize,
    /// Rows per distinct value per object in the distinct sweep.
    #[arg(long, default_value_t = 1)]
    repeats_per_value: usize,
    /// Distinct low-cardinality group keys in the distinct sweep.
    #[arg(long, default_value_t = 100)]
    low_cardinality: usize,
    /// Per-query and per-tenant memory ceiling for the distinct sweep, in
    /// bytes. Large by default: the sweep measures what a query reaches, so a
    /// ceiling that cuts it off destroys the measurement. Default 16 GiB.
    #[arg(long, default_value_t = 16 << 30)]
    distinct_max_bytes: usize,
    /// Reproduce the pre-#680 session in the distinct sweep: leave DataFusion's
    /// own skip-partial-aggregation probe thresholds in place instead of
    /// Ravel's tightened ones. This is the "before" side of the fix's A/B.
    #[arg(long, default_value_t = false)]
    no_skip_partial_aggregation: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();

    if args.distinct_sweep {
        let skip_partial_aggregation = !args.no_skip_partial_aggregation;
        let config = if args.smoke {
            DistinctScalingConfig {
                skip_partial_aggregation,
                ..DistinctScalingConfig::smoke(store_from_env(args.store), &args.store.to_string())
            }
        } else {
            DistinctScalingConfig {
                store: store_from_env(args.store),
                store_label: args.store.to_string(),
                distinct_values: args.distinct_values,
                target_partitions: args.target_partitions,
                objects: args.objects,
                repeats_per_value: args.repeats_per_value,
                low_cardinality: args.low_cardinality,
                max_bytes: args.distinct_max_bytes,
                deadline: Duration::from_secs(args.deadline_secs),
                skip_partial_aggregation,
            }
        };
        let report = run_distinct(&config).await;
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("serialize report")
        );
        print_distinct_table(&report);
        return;
    }

    let config = if args.smoke {
        GroupbyScalingConfig {
            max_tenant_bytes: args.max_tenant_bytes,
            deadline: Duration::from_secs(args.deadline_secs),
            ..GroupbyScalingConfig::smoke(store_from_env(args.store), &args.store.to_string())
        }
    } else {
        GroupbyScalingConfig {
            store: store_from_env(args.store),
            store_label: args.store.to_string(),
            parts: args.parts,
            series: args.series,
            samples_per_series: args.samples_per_series,
            target_partitions: args.target_partitions,
            runs: args.runs,
            max_tenant_bytes: args.max_tenant_bytes,
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

fn print_distinct_table(report: &DistinctReport) {
    println!();
    println!("groupby_scaling_bench distinct-key memory sweep (issue #680)");
    println!("  store           : {}", report.config.store);
    println!("  cores           : {}", report.config.cores);
    println!("  profile         : {}", report.config.profile);
    println!("  objects         : {}", report.config.objects);
    println!("  repeats/value   : {}", report.config.repeats_per_value);
    println!("  low cardinality : {}", report.config.low_cardinality);
    println!("  max bytes       : {}", report.config.max_bytes);
    println!(
        "  skip partial agg: {}",
        report.config.skip_partial_aggregation
    );
    for q in &report.config.queries {
        println!("  query           : {q}");
    }
    println!();
    println!(
        "{:>17} | {:>9} | {:>7} | {:>8} | {:>12} | {:>15} | {:>12} | {:>10} | {:>10}",
        "query",
        "D",
        "target",
        "scanpart",
        "records",
        "peak pool bytes",
        "bytes/entry",
        "rows",
        "ms"
    );
    println!(
        "{:->17}-+-{:->9}-+-{:->7}-+-{:->8}-+-{:->12}-+-{:->15}-+-{:->12}-+-{:->10}-+-{:->10}",
        "", "", "", "", "", "", "", "", ""
    );
    for r in &report.results {
        if let Some(err) = &r.error {
            println!(
                "{:>17} | {:>9} | {:>7} | {:>8} | {:>12} | FAILED: {}",
                r.query_label,
                r.distinct_values,
                r.target_partitions,
                r.scan_partitions,
                r.total_records,
                err
            );
            continue;
        }
        println!(
            "{:>17} | {:>9} | {:>7} | {:>8} | {:>12} | {:>15} | {:>12.1} | {:>10} | {:>10.1}",
            r.query_label,
            r.distinct_values,
            r.target_partitions,
            r.scan_partitions,
            r.total_records,
            r.peak_pool_bytes,
            r.bytes_per_distinct,
            r.result_rows,
            r.elapsed_ms,
        );
    }

    println!();
    println!("fitted partition axis (0.0 = peak ~ D, 1.0 = peak ~ D x partitions)");
    println!(
        "{:>17} | {:>9} | {:>9} | {:>15} | {:>15} | {:>10} | {:>10}",
        "query", "D", "part 1->N", "peak at min", "peak at max", "ratio", "exponent"
    );
    println!(
        "{:->17}-+-{:->9}-+-{:->9}-+-{:->15}-+-{:->15}-+-{:->10}-+-{:->10}",
        "", "", "", "", "", "", ""
    );
    for f in &report.fits {
        println!(
            "{:>17} | {:>9} | {:>4}->{:<4} | {:>15} | {:>15} | {:>10.3} | {:>10.3}",
            f.query_label,
            f.distinct_values,
            f.min_partitions,
            f.max_partitions,
            f.peak_at_min,
            f.peak_at_max,
            f.peak_ratio,
            f.partition_exponent,
        );
    }
}

fn print_human_table(report: &Report) {
    println!();
    println!("groupby_scaling_bench report");
    println!("  store             : {}", report.config.store);
    println!("  cores             : {}", report.config.cores);
    println!("  profile           : {}", report.config.profile);
    println!("  query             : {}", report.config.query);
    println!("  parts             : {}", report.config.parts);
    println!("  series (groups)   : {}", report.config.series);
    println!("  samples_per_series: {}", report.config.samples_per_series);
    println!("  total_samples     : {}", report.config.total_samples);
    println!("  result groups     : {}", report.config.groups);
    println!("  runs per combo    : {}", report.config.runs);
    println!();
    println!(
        "{:>7} | {:>8} | {:>8} | {:>8} | {:>8} | {:>5} | {:>10} | {:>10} | {:>10} | {:>10} | {:>16}",
        "target",
        "parallel",
        "fanout",
        "scanpart",
        "segments",
        "runs",
        "min (ms)",
        "med (ms)",
        "max (ms)",
        "sd (ms)",
        "rows/sec"
    );
    println!(
        "{:->7}-+-{:->8}-+-{:->8}-+-{:->8}-+-{:->8}-+-{:->5}-+-{:->10}-+-{:->10}-+-{:->10}-+-{:->10}-+-{:->16}",
        "", "", "", "", "", "", "", "", "", "", ""
    );
    for c in &report.combos {
        if let Some(err) = &c.error {
            println!(
                "{:>7} | {:>8} | {:>8} | {:>8} | {:>8} | FAILED: {}",
                c.target_partitions,
                c.parallel_final_aggregation,
                c.fanned_out,
                c.scan_partitions,
                c.segments_scanned,
                err,
            );
            continue;
        }
        println!(
            "{:>7} | {:>8} | {:>8} | {:>8} | {:>8} | {:>5} | {:>10.3} | {:>10.3} | {:>10.3} | {:>10.3} | {:>16.0}",
            c.target_partitions,
            c.parallel_final_aggregation,
            c.fanned_out,
            c.scan_partitions,
            c.segments_scanned,
            c.runs_taken,
            c.min_ms,
            c.median_ms,
            c.max_ms,
            c.stddev_ms,
            c.rows_per_sec,
        );
    }
}
