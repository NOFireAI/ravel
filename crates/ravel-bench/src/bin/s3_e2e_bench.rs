//! End-to-end ingest-then-query benchmark CLI (docs/benchmarking.md
//! "End-to-end"). Thin wrapper around `ravel_bench::e2e::run`: parses
//! `--store memory|s3` via `ravel_bench::harness`, then prints the report.
//! The `s3` store reads `RAVEL_S3_*` env vars (`ravel_bench::harness::
//! s3_config_from_env`), same convention as `ingest_bench`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::e2e::{Report, run};
use ravel_bench::harness::{StoreKind, store_from_env};

#[derive(Parser, Debug)]
#[command(about = "End-to-end ravel ingest+query benchmark (Phase 1, docs/benchmarking.md)")]
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
    /// PromQL instant selector run repeatedly after ingest to measure query
    /// latency; must match the workload generator's metric name.
    #[arg(long, default_value = "bench_gauge")]
    query: String,
    #[arg(long, default_value_t = 20)]
    query_count: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = ravel_bench::e2e::E2eConfig {
        store: store_from_env(args.store),
        store_label: args.store.to_string(),
        shards: args.shards,
        target_series: args.target_series,
        points_per_sec: args.points_per_sec,
        duration_secs: args.duration_secs,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        query: args.query,
        query_count: args.query_count,
    };
    let report = run(&config).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!("\ns3_e2e_bench report");
    println!("  store             : {}", report.config.store);
    println!("  shards            : {}", report.config.shards);
    println!("  target_series     : {}", report.config.target_series);
    println!("  points_per_sec cfg: {}", report.config.points_per_sec);
    println!("  duration_secs     : {}", report.config.duration_secs);
    println!("  batch_size        : {}", report.config.batch_size);
    println!(
        "  accepted points/s : {:.1}",
        report.accepted_points_per_sec
    );
    println!("  accepted points   : {}", report.accepted_points);
    println!(
        "  ack latency ms    : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={})",
        report.ack_latency_ms.p50,
        report.ack_latency_ms.p95,
        report.ack_latency_ms.p99,
        report.ack_latency_ms.max,
        report.ack_latency_ms.count
    );
    println!(
        "  flushes           : size={} age={} manual={}",
        report.flushes_by_size, report.flushes_by_age, report.flushes_manual
    );
    println!("  put_retries       : {}", report.put_retries);
    println!(
        "  abandoned         : retry_exhausted={} input_rejected={}",
        report.abandoned_retry_exhausted, report.abandoned_input_rejected
    );
    println!(
        "  acks ok/err       : {}/{}",
        report.acks_ok, report.acks_err
    );
    println!("  estimated PUTs    : {}", report.estimated_put_count);
    println!("  bytes_written     : {}", report.bytes_written);
    println!("  logical_bytes     : {}", report.logical_bytes);
    println!("  write_amplification: {:.3}", report.write_amplification);
    println!(
        "  visibility lag ms : avg={:.3} max={:.3} resolved={} unresolved={}",
        report.visibility_lag_ms.avg,
        report.visibility_lag_ms.max,
        report.visibility_lag_ms.resolved_count,
        report.visibility_lag_ms.unresolved_count
    );
    println!(
        "  query             : \"{}\" x{}",
        report.config.query, report.config.query_count
    );
    println!(
        "  query matched     : {} series",
        report.query_matched_series
    );
    println!(
        "  query latency ms  : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={})",
        report.query_latency_ms.p50,
        report.query_latency_ms.p95,
        report.query_latency_ms.p99,
        report.query_latency_ms.max,
        report.query_latency_ms.count
    );
    println!(
        "  query requests    : get={} list={} bytes_read={}",
        report.query_get_count, report.query_list_count, report.query_bytes_read
    );
}
