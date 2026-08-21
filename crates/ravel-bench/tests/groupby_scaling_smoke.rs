//! Acceptance test for the `groupby_scaling_bench` core-count scaling path.
//!
//! Drives `groupby_scaling::run` in its smoke configuration end to end against
//! an in-process `MemoryStore` and asserts the report structurally covers both
//! swept axes: an entry per `target_partitions` value AND per
//! `parallel_final_aggregation` state, each with non-zero timing.
//!
//! The whole file is gated on the `sql-latency` feature (the same gate the
//! module and bin sit behind), so a default `cargo test -p ravel-bench` build
//! -- which never compiles ravel-sql/datafusion -- sees an empty test crate
//! rather than a link error. Run it with
//! `cargo test -p ravel-bench --features sql-latency`.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;

use ravel_bench::groupby_scaling::{GroupbyScalingConfig, run};
use ravel_object_store::memory::MemoryStore;

#[tokio::test(flavor = "multi_thread")]
async fn groupby_scaling_smoke_covers_both_axes() {
    let config = GroupbyScalingConfig::smoke(Arc::new(MemoryStore::new()), "memory");
    let expected_partitions: Vec<usize> = config.target_partitions.clone();
    let report = run(&config).await;

    // The dataset was actually built and scanned.
    assert!(
        report.config.total_samples > 0,
        "smoke run must ingest a non-zero sample count"
    );
    assert!(
        report.config.groups > 0,
        "smoke run must return a non-zero group count"
    );

    // Every (target_partitions x flag) combination is present exactly once.
    assert_eq!(
        report.combos.len(),
        expected_partitions.len() * 2,
        "one entry per (target_partitions x parallel flag) combination"
    );

    // Per-target_partitions-value coverage.
    let seen_partitions: HashSet<usize> =
        report.combos.iter().map(|c| c.target_partitions).collect();
    for tp in &expected_partitions {
        assert!(
            seen_partitions.contains(tp),
            "report must contain an entry for target_partitions={tp}"
        );
    }

    // Per-flag-state coverage: both false and true appear, at every partition.
    for tp in &expected_partitions {
        let flags: HashSet<bool> = report
            .combos
            .iter()
            .filter(|c| c.target_partitions == *tp)
            .map(|c| c.parallel_final_aggregation)
            .collect();
        assert!(
            flags.contains(&false) && flags.contains(&true),
            "target_partitions={tp} must have both parallel_final_aggregation on and off"
        );
    }

    // Every combination produced non-zero timing and the same group count.
    for c in &report.combos {
        assert!(
            c.median_ms > 0.0 && c.max_ms > 0.0,
            "combo tp={} parallel={} must report non-zero timing",
            c.target_partitions,
            c.parallel_final_aggregation
        );
        assert!(
            c.rows_per_sec > 0.0,
            "combo tp={} parallel={} must report non-zero throughput",
            c.target_partitions,
            c.parallel_final_aggregation
        );
        assert_eq!(
            c.result_rows, report.config.groups,
            "every combo returns the same group count"
        );
    }
}
