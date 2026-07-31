//! Concurrent readers-and-writers + cold-vs-warm cache benchmark
//! (docs/benchmarking.md "End-to-end", issue #269). Thin wrapper around
//! `ravel_bench::concurrent::run`: parses `--store memory|s3` via
//! `ravel_bench::harness`, same convention as `s3_e2e_bench`, then prints
//! the report.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::concurrent::{ConcurrentConfig, Report, run};
use ravel_bench::harness::{StoreKind, store_from_env};

#[derive(Parser, Debug)]
#[command(
    about = "Concurrent readers-and-writers + cold/warm cache benchmark (docs/benchmarking.md \"End-to-end\")"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 4)]
    writers: usize,
    #[arg(long, default_value_t = 4)]
    readers: usize,
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
    /// PromQL instant selector every reader task (and the cold/warm phase)
    /// repeats; must match the workload generator's metric name.
    #[arg(long, default_value = "bench_gauge")]
    query: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = ConcurrentConfig {
        store: store_from_env(args.store),
        store_label: args.store.to_string(),
        shards: args.shards,
        writers: args.writers,
        readers: args.readers,
        target_series: args.target_series,
        points_per_sec: args.points_per_sec,
        duration_secs: args.duration_secs,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        query: args.query,
    };
    let report = run(&config).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!("\nconcurrent_bench report");
    println!("  store             : {}", report.config.store);
    println!("  shards            : {}", report.config.shards);
    println!(
        "  writers/readers   : {}/{}",
        report.config.writers, report.config.readers
    );
    println!("  target_series     : {}", report.config.target_series);
    println!("  points_per_sec cfg: {}", report.config.points_per_sec);
    println!("  duration_secs     : {}", report.config.duration_secs);
    println!("  batch_size        : {}", report.config.batch_size);
    println!(
        "  writer throughput : {:.1} points/s",
        report.writer_throughput_points_per_sec
    );
    println!(
        "  writer latency ms : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={})",
        report.writer_latency_ms.p50,
        report.writer_latency_ms.p95,
        report.writer_latency_ms.p99,
        report.writer_latency_ms.max,
        report.writer_latency_ms.count
    );
    for w in &report.writers {
        println!(
            "    writer[{}]: accepted={} ok={} err={} p50={:.3} p95={:.3} max={:.3}",
            w.writer_id,
            w.accepted_points,
            w.batches_ok,
            w.batches_err,
            w.latency_ms.p50,
            w.latency_ms.p95,
            w.latency_ms.max
        );
    }
    println!(
        "  reader throughput : {:.1} queries/s",
        report.reader_throughput_queries_per_sec
    );
    println!(
        "  reader latency ms : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={})",
        report.reader_latency_ms.p50,
        report.reader_latency_ms.p95,
        report.reader_latency_ms.p99,
        report.reader_latency_ms.max,
        report.reader_latency_ms.count
    );
    for r in &report.readers {
        println!(
            "    reader[{}]: queries={} non_empty={} p50={:.3} p95={:.3} max={:.3}",
            r.reader_id,
            r.queries_run,
            r.non_empty_results,
            r.latency_ms.p50,
            r.latency_ms.p95,
            r.latency_ms.max
        );
    }
    println!(
        "  cold query        : latency={:.3}ms matched={} series",
        report.cache.cold_latency_ms, report.cache.cold_matched_series
    );
    println!(
        "  warm query        : latency={:.3}ms matched={} series",
        report.cache.warm_latency_ms, report.cache.warm_matched_series
    );
}
