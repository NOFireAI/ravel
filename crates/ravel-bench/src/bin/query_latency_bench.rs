//! End-to-end PromQL query-latency benchmark. Thin wrapper around
//! `ravel_bench::query_latency::run`: parses `--store memory|s3` via
//! `ravel_bench::harness`, same convention as `s3_e2e_bench` and
//! `concurrent_bench`, then prints the report.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::query_latency::{QueryLatencyConfig, Report, run};

#[derive(Parser, Debug)]
#[command(about = "End-to-end PromQL instant/range query latency benchmark")]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, default_value_t = 4)]
    shards: u32,
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
    /// PromQL selector both query phases evaluate; must match the workload
    /// generator's metric name.
    #[arg(long, default_value = "bench_gauge")]
    query: String,
    #[arg(long, default_value_t = 200)]
    instant_query_count: usize,
    #[arg(long, default_value_t = 200)]
    range_query_count: usize,
    /// Number of steps the range query covers across the ingested window.
    #[arg(long, default_value_t = 10)]
    range_steps: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = QueryLatencyConfig {
        store: store_from_env(args.store),
        store_label: args.store.to_string(),
        shards: args.shards,
        target_series: args.target_series,
        points_per_sec: args.points_per_sec,
        duration_secs: args.duration_secs,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        query: args.query,
        instant_query_count: args.instant_query_count,
        range_query_count: args.range_query_count,
        range_steps: args.range_steps,
    };
    let report = run(&config).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!("\nquery_latency_bench report");
    println!("  store             : {}", report.config.store);
    println!("  shards            : {}", report.config.shards);
    println!("  target_series     : {}", report.config.target_series);
    println!("  points_per_sec cfg: {}", report.config.points_per_sec);
    println!("  duration_secs     : {}", report.config.duration_secs);
    println!("  batch_size        : {}", report.config.batch_size);
    println!("  query             : {}", report.config.query);
    println!("  accepted_points   : {}", report.accepted_points);
    println!(
        "  instant latency ms: p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={}, matched={})",
        report.instant_latency_ms.p50,
        report.instant_latency_ms.p95,
        report.instant_latency_ms.p99,
        report.instant_latency_ms.max,
        report.instant_latency_ms.count,
        report.instant_matched_series
    );
    println!(
        "  range latency ms  : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={}, steps={}, matched={})",
        report.range_latency_ms.p50,
        report.range_latency_ms.p95,
        report.range_latency_ms.p99,
        report.range_latency_ms.max,
        report.range_latency_ms.count,
        report.config.range_steps,
        report.range_matched_series
    );
}
