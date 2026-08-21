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
    let expected_parts = config.parts;
    let expected_runs = config.runs;
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
    // The dataset actually published as many distinct series as configured: the
    // observed group count (distinct series_id the query returned) equals the
    // requested series count, so a generation bug that collapsed cardinality
    // (label churn, a bad offset) would show here rather than being hidden.
    assert_eq!(
        report.config.groups, report.config.series,
        "the dataset must publish exactly as many distinct series as configured"
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

    // Every combination produced non-zero timing and the same group count, and
    // its OBSERVED facts prove the swept axes actually reached execution.
    // These assertions read fields recorded from the real plan and the real
    // timed loop, not the requested config echoed back, so a wiring break in
    // either axis fails the test rather than passing vacuously.
    for c in &report.combos {
        // Over an in-memory store with lifted budgets no combination fails; a
        // ResourcesExhausted here would be a real regression, not an expected
        // labeled failure.
        assert!(
            c.error.is_none(),
            "combo tp={} parallel={} unexpectedly failed: {:?}",
            c.target_partitions,
            c.parallel_final_aggregation,
            c.error
        );
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

        // Observed fan-out matches the axis. The ADR-0094 hash repartition is
        // inserted only when the flag is on AND target_partitions > 1 (at
        // target_partitions=1 EnforceDistribution needs no repartition even
        // with the flag on). A flag hardcoded off, or a fetch_concurrency
        // pinned to 1, breaks this equality.
        let expect_fanout = c.parallel_final_aggregation && c.target_partitions > 1;
        assert_eq!(
            c.fanned_out, expect_fanout,
            "combo tp={} parallel={} fan-out must be {expect_fanout} (observed {})",
            c.target_partitions, c.parallel_final_aggregation, c.fanned_out
        );

        // Observed scan partitioning is the real min(target_partitions, parts):
        // the requested target_partitions must reach the segment-granular scan.
        // A fetch_concurrency pinned to 1, or a dataset forced to one part,
        // breaks this equality.
        let expect_scan_partitions = c.target_partitions.min(expected_parts);
        assert_eq!(
            c.scan_partitions,
            expect_scan_partitions,
            "combo tp={} parallel={} scan partitions must be min(tp={}, parts={})={expect_scan_partitions} (observed {})",
            c.target_partitions,
            c.parallel_final_aggregation,
            c.target_partitions,
            expected_parts,
            c.scan_partitions
        );

        // Observed timed-iteration count equals the requested runs: a timed
        // loop forced to a single pass breaks this equality.
        assert_eq!(
            c.runs_taken, expected_runs,
            "combo tp={} parallel={} must perform exactly {expected_runs} timed runs (observed {})",
            c.target_partitions, c.parallel_final_aggregation, c.runs_taken
        );
    }
}
