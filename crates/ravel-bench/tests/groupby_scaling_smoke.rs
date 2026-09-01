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

use ravel_bench::groupby_scaling::{
    DISTINCT_QUERIES, DistinctScalingConfig, GroupbyScalingConfig, run, run_distinct,
};
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

/// A combination that exceeds its memory budget fails as a labeled per-combo
/// error, not a panic that discards every combination already measured. Both
/// axes still run to completion: `run()` must return one entry per
/// `target_partitions x flag` combination even though every one of them
/// fails, proving the sweep survives an over-budget combo rather than
/// aborting mid-run.
///
/// Prove-the-test: this exercises the exact failure class the checkpoint
/// review's B1 finding named -- before the fix, an over-budget execute()
/// result reached `.expect("warm query")`/`.expect("timed query")` and
/// panicked the whole async task, so `run()` never returned at all and this
/// test would hang/panic instead of observing a completed `Report`.
#[tokio::test(flavor = "multi_thread")]
async fn groupby_scaling_over_budget_combo_fails_labeled_not_panicked() {
    let mut config = GroupbyScalingConfig::smoke(Arc::new(MemoryStore::new()), "memory");
    config.max_tenant_bytes = 1024;

    let report = run(&config).await;

    assert_eq!(
        report.combos.len(),
        config.target_partitions.len() * 2,
        "every combination must still be present even though all are over budget"
    );
    for c in &report.combos {
        assert!(
            c.error.is_some(),
            "combo tp={} parallel={} over a 1024-byte tenant budget must be a labeled failure",
            c.target_partitions,
            c.parallel_final_aggregation
        );
    }
}

// No deadline-specific sibling of the budget test above, deliberately: a
// `DeadlineExceeded` error is a distinct `SqlError` variant from
// `ResourcesExhausted`, but `run_combo`'s fix (checkpoint review finding B1)
// changed every `Err(SqlError::ResourcesExhausted(msg))` match arm to a
// generic `Err(e)`, so there is exactly ONE code path handling every error
// variant, not one per variant -- the budget test above already proves that
// arm degrades to a labeled failure instead of panicking, and a second test
// hitting the identical line of code for a different variant adds no new
// coverage. A dedicated `DeadlineExceeded` test was tried and dropped: it
// wraps `tokio::time::timeout` (executor.rs) around the query future, and a
// `MemoryStore`-backed query's future resolves entirely on a single poll with
// no intermediate `.await` yield point, so the timeout combinator never gets
// a chance to race the timer in against it regardless of how long that one
// poll takes in wall-clock time (confirmed empirically: a deadline of 1ns
// against a 1M-row in-process dataset still completed successfully after 78s,
// never tripping). Forcing a real yield point to make this deterministically
// testable is out of this benchmark's scope.

/// Acceptance test for the distinct-key memory sweep (issue #680).
///
/// The sweep's whole value is the peak-pool-bytes figure and the dataset shape
/// that makes it interpretable, so this asserts both rather than just "a report
/// came back":
///
/// - Every `(query, D, target_partitions)` cell is present and succeeded.
/// - `total_records == objects * D * repeats_per_value`, so every object really
///   does carry the full distinct set. A generator that spread the values
///   across objects instead of repeating them would leave each partition seeing
///   `D / objects` values, and the partition axis would measure nothing.
/// - `scan_partitions == min(target_partitions, objects)`: the requested
///   partition count actually reached `LogsScanExec`. A fan-out pinned to 1
///   would make every cell identical and the fit meaningless.
/// - `peak_pool_bytes > 0` on every cell, and a fit per query shape.
/// - The result row counts are the ones the two query shapes must produce:
///   one row for `COUNT(DISTINCT ...)`, `low_cardinality` rows for the grouped
///   shape (the low key is a function of the high key, so every one of the
///   `low_cardinality` buckets is non-empty at this `D`).
#[tokio::test(flavor = "multi_thread")]
async fn distinct_sweep_measures_peak_pool_bytes_across_the_partition_axis() {
    let config = DistinctScalingConfig::smoke(Arc::new(MemoryStore::new()), "memory");
    let expected_partitions = config.target_partitions.clone();
    let expected_distinct = config.distinct_values.clone();
    let objects = config.objects;
    let low_cardinality = config.low_cardinality;
    let repeats = config.repeats_per_value;

    let report = run_distinct(&config).await;

    assert_eq!(
        report.results.len(),
        expected_distinct.len() * expected_partitions.len() * DISTINCT_QUERIES.len(),
        "one result per (D x target_partitions x query shape)"
    );

    for r in &report.results {
        assert!(
            r.error.is_none(),
            "cell query={} D={} tp={} failed: {:?}",
            r.query_label,
            r.distinct_values,
            r.target_partitions,
            r.error
        );
        assert_eq!(
            r.total_records,
            objects * r.distinct_values * repeats,
            "every object must carry the full distinct set: cell query={} D={} tp={}",
            r.query_label,
            r.distinct_values,
            r.target_partitions
        );
        assert_eq!(
            r.scan_partitions,
            r.target_partitions.min(objects),
            "cell query={} D={} tp={}: the requested target_partitions must reach \
             LogsScanExec (observed {})",
            r.query_label,
            r.distinct_values,
            r.target_partitions,
            r.scan_partitions
        );
        assert!(
            r.peak_pool_bytes > 0,
            "cell query={} D={} tp={} must reach a non-zero pool high-water mark",
            r.query_label,
            r.distinct_values,
            r.target_partitions
        );
        let expected_rows = match r.query_label.as_str() {
            "distinct_only" => 1,
            "groupby_distinct" => low_cardinality.min(r.distinct_values),
            other => panic!("unexpected query label {other}"),
        };
        assert_eq!(
            r.result_rows, expected_rows,
            "cell query={} D={} tp={} returned the wrong row count",
            r.query_label, r.distinct_values, r.target_partitions
        );
    }

    // One fit per (query shape, D), each over two real partition counts.
    assert_eq!(
        report.fits.len(),
        expected_distinct.len() * DISTINCT_QUERIES.len(),
        "one fitted partition axis per (query shape, D)"
    );
    for f in &report.fits {
        assert_eq!(f.min_partitions, expected_partitions[0]);
        assert_eq!(
            f.max_partitions,
            *expected_partitions.last().expect("a partition value")
        );
        assert!(
            f.peak_at_min > 0 && f.peak_at_max > 0 && f.peak_ratio > 0.0,
            "fit query={} D={} must be over real peaks",
            f.query_label,
            f.distinct_values
        );
        assert!(
            f.partition_exponent.is_finite(),
            "fit query={} D={} exponent must be finite",
            f.query_label,
            f.distinct_values
        );
    }
}
