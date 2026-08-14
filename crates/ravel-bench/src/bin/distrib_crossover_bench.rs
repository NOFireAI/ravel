//! Local-vs-distributed read crossover benchmark (ADR-0071). Thin
//! wrapper around `ravel_bench::distrib_crossover::run`: parses `--store
//! memory|s3` via `ravel_bench::harness` (same convention as `s3_e2e_bench` and
//! `query_latency_bench`), then prints the report.
//!
//! The distributed panels run in-process `tonic` loopback workers over the
//! chosen store; on `MemoryStore` this measures the fan-out overhead against a
//! zero-latency store, a lower bound on the crossover (see the module docs).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::distrib_crossover::{CrossoverConfig, Report, run};
use ravel_bench::harness::{StoreKind, store_from_env};

#[derive(Parser, Debug)]
#[command(
    about = "Local-vs-distributed PromQL read crossover benchmark (ADR-0071 distributed read fan-out)"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 64)]
    target_series: usize,
    #[arg(long, default_value_t = 8)]
    samples_per_series: usize,
    #[arg(long, default_value_t = 128)]
    batch_size: usize,
    #[arg(long, default_value_t = 10)]
    ack_timeout_secs: u64,
    /// PromQL selector both paths evaluate; must match the workload generator's
    /// metric name.
    #[arg(long, default_value = "bench_gauge")]
    query: String,
    /// Corpus time span in seconds: widens the window the samples spread across
    /// so a sweep can grow the corpus past the gate constants (256 MiB / 256
    /// segments) without changing series cardinality. Default 1 preserves the
    /// original one-second corpus.
    #[arg(long, default_value_t = 1)]
    corpus_span_secs: u64,
    #[arg(long, default_value_t = 20)]
    query_reps: usize,
    /// Worker counts the distributed path is measured at (repeatable), e.g.
    /// `--worker-counts 1 --worker-counts 4`.
    #[arg(long = "worker-counts", num_args = 1.., default_values_t = [1usize, 4])]
    worker_counts: Vec<usize>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = CrossoverConfig {
        store: store_from_env(args.store),
        store_label: args.store.to_string(),
        shards: args.shards,
        target_series: args.target_series,
        samples_per_series: args.samples_per_series,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        query: args.query,
        corpus_span_secs: args.corpus_span_secs,
        query_reps: args.query_reps,
        worker_counts: args.worker_counts,
    };
    let report = run(&config).await;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!("\ndistrib_crossover_bench report");
    println!("  store            : {}", report.config.store);
    println!("  shards           : {}", report.config.shards);
    println!("  target_series    : {}", report.config.target_series);
    println!("  samples/series   : {}", report.config.samples_per_series);
    println!("  corpus_span_secs : {}", report.config.corpus_span_secs);
    println!("  query            : {}", report.config.query);
    println!("  query_reps       : {}", report.config.query_reps);
    println!("  accepted_points  : {}", report.accepted_points);
    println!(
        "  corpus           : {} segments across {} shards",
        report.corpus_segments, report.corpus_shards
    );
    println!(
        "  {:<12} {:>7}  {:>9} {:>9} {:>9} {:>9}  {:>9}  {:>7} {:>6}  {:>9} {:>7}  {:>11}",
        "path",
        "workers",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "max ms",
        "cpu ms",
        "matched",
        "segs",
        "s3_reqs",
        "gets",
        "s3_bytes"
    );
    for p in &report.panels {
        println!(
            "  {:<12} {:>7}  {:>9.3} {:>9.3} {:>9.3} {:>9.3}  {:>9.1}  {:>7} {:>6}  {:>9} {:>7}  {:>11}",
            p.path,
            p.worker_count,
            p.wall_ms_p50,
            p.wall_ms_p95,
            p.wall_ms_p99,
            p.wall_ms_max,
            p.coordinator_cpu_ms,
            p.matched_series,
            p.segments_fetched,
            p.s3_requests,
            p.s3_get_requests,
            p.s3_bytes,
        );
    }
}
