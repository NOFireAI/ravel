//! Acceptance test for the `logs_scan_scaling_bench` path (ADR-0102 decision 1,
//! epic #361 item 1).
//!
//! Drives `logs_scan_scaling::run` in its smoke configuration end to end against
//! an in-process `MemoryStore` and asserts the report proves the two facts this
//! item exists to demonstrate: the undersubscribed `logs` scan fans out to
//! `target_partitions` (more than the segment count) block-level partitions, and
//! ADR-0046's read cache keeps the object-store GET count flat across the sweep
//! while the un-cached path's climbs.
//!
//! Gated on `sql-latency` (the module and bin's gate), so a default
//! `cargo test -p ravel-bench` sees an empty crate rather than a link error. Run
//! with `cargo test -p ravel-bench --features sql-latency`.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_bench::logs_scan_scaling::{LogsScanScalingConfig, run};
use ravel_object_store::memory::MemoryStore;

#[tokio::test(flavor = "multi_thread")]
async fn logs_scan_scaling_smoke_proves_fanout_and_flat_request_count() {
    let config = LogsScanScalingConfig::smoke(Arc::new(MemoryStore::new()), "memory");
    let segments = config.segments;
    let expected_partitions = config.target_partitions.clone();
    let report = run(&config).await;

    assert!(
        report.config.total_records > 0,
        "smoke run must ingest a non-zero record count"
    );
    assert_eq!(
        report.combos.len(),
        expected_partitions.len() * 2,
        "one entry per (target_partitions x cache) combination"
    );

    let mut get_by_combo: HashMap<(usize, bool), u64> = HashMap::new();
    for c in &report.combos {
        assert!(
            c.error.is_none(),
            "combo tp={} cache={} unexpectedly failed: {:?}",
            c.target_partitions,
            c.cache_wired,
            c.error
        );
        assert!(
            c.median_ms > 0.0,
            "combo tp={} cache={} must report non-zero timing",
            c.target_partitions,
            c.cache_wired
        );
        assert_eq!(
            c.result_rows, report.config.total_records,
            "a bare `SELECT ts, body FROM logs` returns every record"
        );

        // The declared scan fan-out is target_partitions regardless of the
        // segment count -- the capability block-level striping adds. Under the
        // old segment-granular rule this would have been min(tp, segments).
        assert_eq!(
            c.scan_partitions, c.target_partitions,
            "block-level striping declares target_partitions scan partitions, not \
             min(tp, segments): tp={}, observed {}",
            c.target_partitions, c.scan_partitions
        );

        // The undersubscribed capability: more non-empty partitions than
        // segments once the partition count exceeds the segment count. The
        // dataset has many blocks per segment, so every partition up to tp gets
        // at least one block.
        if c.target_partitions > segments {
            assert!(
                c.non_empty_partitions > segments,
                "tp={} (> {segments} segments) must fan out to more than {segments} \
                 non-empty partitions; got {}",
                c.target_partitions,
                c.non_empty_partitions
            );
        }
        assert_eq!(
            c.non_empty_partitions, c.target_partitions,
            "with many blocks per segment every partition up to tp={} decodes \
             blocks; got {}",
            c.target_partitions, c.non_empty_partitions
        );

        get_by_combo.insert(
            (c.target_partitions, c.cache_wired),
            c.object_store_get_requests,
        );
    }

    // Request-count story (ADR-0046): with the cache wired, the whole-object
    // reads the striping issues coalesce (single-flight) so the GET count is
    // FLAT across the whole sweep -- one whole-object GET per segment plus the
    // constant catalog-resolve traffic, independent of the partition count.
    // Without the cache each partition issues its own whole-object GET, so the
    // raw count is at least the cached count and climbs with the partition
    // count. (`segments` is unused directly here because the absolute count
    // includes constant catalog GETs; the invariant is flatness, not a literal.)
    let _ = segments;
    let cached_at_1 = get_by_combo[&(1, true)];
    for &tp in &expected_partitions {
        let cached = get_by_combo[&(tp, true)];
        let uncached = get_by_combo[&(tp, false)];
        assert_eq!(
            cached, cached_at_1,
            "cache-wired GET count must be flat across the sweep: tp={tp} issued \
             {cached}, tp=1 issued {cached_at_1}"
        );
        assert!(
            uncached >= cached,
            "un-cached tp={tp} GET count ({uncached}) must be at least the cached count ({cached})"
        );
    }

    // The un-cached raw GET count actually grows with the partition count: the
    // largest swept tp issues strictly more GETs than tp=1. This is the
    // regression the cache neutralizes, measured directly.
    let &max_tp = expected_partitions.iter().max().unwrap();
    if max_tp > 1 {
        assert!(
            get_by_combo[&(max_tp, false)] > get_by_combo[&(1, false)],
            "un-cached GET count must climb with target_partitions: tp={max_tp} \
             issued {}, tp=1 issued {}",
            get_by_combo[&(max_tp, false)],
            get_by_combo[&(1, false)]
        );
    }
}
