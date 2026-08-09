//! End-to-end ingest benchmark: drives `IngestRouter` directly (no HTTP)
//! against a real or in-memory object store (docs/benchmarking.md
//! "End-to-end"). Thin wrapper around `ravel_bench::ingest::run`, mirroring
//! `concurrent_bench`/`s3_e2e_bench`: the flag surface, validation, and
//! report structure all live in the lib so `tests/ingest_smoke.rs` exercises
//! the same path this bin runs. Report-only: never changes
//! ravel-ingest/ravel-catalog behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::ingest::{IngestBenchArgs, Report, run};

#[tokio::main]
async fn main() {
    let args = IngestBenchArgs::parse();
    if let Err(err) = args.validate() {
        eprintln!("error: {err}");
        std::process::exit(2);
    }
    let config = args.to_config();
    let report = run(&config).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

fn print_human_table(report: &Report) {
    println!("\ningest_bench report");
    println!("  store             : {}", report.config.store);
    println!("  shards            : {}", report.config.shards);
    println!("  target_series     : {}", report.config.target_series);
    println!("  points_per_sec cfg: {}", report.config.points_per_sec);
    println!("  duration_secs     : {}", report.config.duration_secs);
    println!("  batch_size        : {}", report.config.batch_size);
    println!(
        "  max_inflight_flush: {}",
        report.config.max_inflight_flushes
    );
    println!("  flush_delay_policy: {}", report.config.flush_delay_policy);
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
        "  flushes           : size={} age={} age_adaptive={} manual={}",
        report.flushes_by_size,
        report.flushes_by_age,
        report.flushes_by_age_adaptive,
        report.flushes_manual
    );
    print!(
        "  in-flight depth   : max_observed={} (n={}, {}) histogram=[",
        report.in_flight_depth.max_observed,
        report.in_flight_depth.samples,
        report.in_flight_depth.source
    );
    for (i, bucket) in report.in_flight_depth.histogram.iter().enumerate() {
        if i > 0 {
            print!(" ");
        }
        print!("{}:{}", bucket.depth, bucket.samples);
    }
    println!("]");
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
}
