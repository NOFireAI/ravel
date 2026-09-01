//! Smoke tests for the `ingest_bench` pipeline-depth / flush-delay sweep.
//! `depth_and_adaptive_policy_memory_smoke`
//! is the acceptance test: it runs the bench harness end to end against an
//! in-process `MemoryStore` with `--max-inflight-flushes 3` and the adaptive
//! policy, and asserts the report carries the new fields with sane values.
//! `depth_zero_rejected` covers the flag-parsing edge, rejected the same way
//! `services/ravel-server`'s `Cli::validate` rejects it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::ingest::{FlushDelayPolicy, IngestBenchArgs, run};

/// Acceptance test. `flavor = "multi_thread"` for the same reason
/// `concurrent_smoke.rs` needs it: a single-threaded runtime starves the
/// router's background per-shard flush task against the write and sampler
/// tasks, so no flush would land inside the short window.
#[tokio::test(flavor = "multi_thread")]
async fn depth_and_adaptive_policy_memory_smoke() {
    let args = IngestBenchArgs::try_parse_from([
        "ingest_bench",
        "--store",
        "memory",
        "--shards",
        "2",
        "--target-series",
        "50",
        "--points-per-sec",
        "5000",
        "--duration-secs",
        "2",
        "--batch-size",
        "50",
        "--max-inflight-flushes",
        "3",
        "--flush-delay-policy",
        "adaptive",
    ])
    .expect("parse args");
    args.validate().expect("args valid");
    assert_eq!(args.max_inflight_flushes, 3);
    assert_eq!(args.flush_delay_policy, FlushDelayPolicy::Adaptive);

    let config = args.to_config();
    let report = run(&config).await;

    // Config knobs echoed back so a panel row is self-describing.
    assert_eq!(report.config.max_inflight_flushes, 3);
    assert_eq!(report.config.flush_delay_policy, "adaptive");

    // Flush-cause breakdown present and nonzero: the trailing manual drain
    // alone guarantees at least one flush.
    let flushes = report.flushes_by_size
        + report.flushes_by_age
        + report.flushes_by_age_adaptive
        + report.flushes_manual;
    assert!(flushes > 0, "expected a nonzero flush count, got {flushes}");

    // In-flight depth histogram sampled over the run: the sampler must have
    // taken at least one sample, and the max observed depth cannot exceed the
    // per-shard bound the semaphore enforces.
    assert!(
        report.in_flight_depth.samples > 0,
        "depth sampler took no samples"
    );
    assert!(
        report.in_flight_depth.max_observed <= 3,
        "max observed depth {} exceeds --max-inflight-flushes 3",
        report.in_flight_depth.max_observed
    );
    let histogram_max = report
        .in_flight_depth
        .histogram
        .iter()
        .map(|b| b.depth)
        .max()
        .unwrap_or(0);
    assert!(
        histogram_max <= 3,
        "histogram bucket depth {histogram_max} exceeds --max-inflight-flushes 3"
    );

    // Ack latency percentiles present and monotonic.
    let ack = &report.ack_latency_ms;
    assert!(ack.count > 0, "no ack latency samples");
    assert!(
        ack.p50 <= ack.p95 && ack.p95 <= ack.p99 && ack.p99 <= ack.max,
        "ack percentiles not monotonic: p50={} p95={} p99={} max={}",
        ack.p50,
        ack.p95,
        ack.p99,
        ack.max
    );
}

/// Flag-parsing edge: `--max-inflight-flushes 0` parses (it is a valid
/// `u32`) but `validate` rejects it, mirroring `services/ravel-server`'s
/// `Cli::validate`, which refuses the same value because it deadlocks every
/// flush.
#[test]
fn depth_zero_rejected() {
    let args = IngestBenchArgs::try_parse_from(["ingest_bench", "--max-inflight-flushes", "0"])
        .expect("0 is a valid u32 and parses");
    let err = args.validate().expect_err("depth 0 must be rejected");
    assert!(
        err.contains("deadlock"),
        "rejection message should explain the deadlock, got: {err}"
    );
}

/// A positive depth validates, confirming the rejection above is specific to
/// zero and not a blanket failure.
#[test]
fn positive_depth_validates() {
    let args = IngestBenchArgs::try_parse_from(["ingest_bench", "--max-inflight-flushes", "4"])
        .expect("parse");
    args.validate().expect("positive depth is valid");
}
