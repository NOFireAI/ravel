//! Acceptance test for the `logs_scan_scaling_bench` path (ADR-0102 decision 1,
//! epic #361 item 1).
//!
//! Drives `logs_scan_scaling::run` in its smoke configuration end to end against
//! an in-process `MemoryStore` and asserts the report proves what this item
//! actually ships: with ADR-0046's read cache wired, an undersubscribed `logs`
//! scan fans out to `target_partitions` block-level partitions (more than the
//! segment count); without it the partition count is capped at the segment count
//! and the object-store GET count therefore stops responding to
//! `target_partitions` once the cap binds.
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
async fn logs_scan_scaling_smoke_proves_cache_gated_fanout_and_request_count() {
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

    // The planning prune's per-segment serialized await is only visible with
    // enough segments to serialize; two made it a rounding error. The report
    // carries the real figure rather than prose about it.
    assert_eq!(
        report.planning.segments, segments,
        "the planning measurement must cover every segment in the fixture"
    );
    assert!(
        segments >= 32,
        "the fixture needs enough segments for `compute_plan_counts`'s \
         per-segment serialized await to be measurable; got {segments}"
    );
    assert!(
        report.planning.serial_ms > 0.0 && report.planning.total_blocks > 0,
        "planning measurement must report real work: {} ms over {} blocks",
        report.planning.serial_ms,
        report.planning.total_blocks
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

        // The declared fan-out is `target_partitions` only when the read cache is
        // wired (ADR-0102 decision 1's precondition: the fetch unit is the whole
        // object, so the extra partitions' repeated reads need the cache to
        // coalesce). Un-cached it falls back to the pre-item-1 bound.
        let expected = if c.cache_wired {
            c.target_partitions
        } else {
            c.target_partitions.min(segments)
        };
        assert_eq!(
            c.scan_partitions, expected,
            "tp={} cache={}: expected {expected} scan partitions, got {}",
            c.target_partitions, c.cache_wired, c.scan_partitions
        );

        // Every declared partition owns blocks on this fixture (192 blocks across
        // at most 64 partitions), so the non-empty count matches the declared
        // one -- and in the cached, undersubscribed case that exceeds the segment
        // count, which is the capability this item adds.
        assert_eq!(
            c.non_empty_partitions, expected,
            "tp={} cache={}: every declared partition should decode blocks; got {}",
            c.target_partitions, c.cache_wired, c.non_empty_partitions
        );
        if c.cache_wired && c.target_partitions > segments {
            assert!(
                c.non_empty_partitions > segments,
                "cached tp={} (> {segments} segments) must fan out past the \
                 segment count; got {}",
                c.target_partitions,
                c.non_empty_partitions
            );
        }

        get_by_combo.insert(
            (c.target_partitions, c.cache_wired),
            c.object_store_get_requests,
        );
    }

    // Request count, un-cached: once `target_partitions` reaches the segment
    // count the partition count stops growing, so the GET count stops growing
    // with it. This is the property the has_cache gate buys -- without it, every
    // partition past the segment count added whole-object reads that nothing
    // absorbed.
    let at_cap = expected_partitions
        .iter()
        .copied()
        .filter(|&tp| tp >= segments)
        .collect::<Vec<_>>();
    assert!(
        at_cap.len() >= 2,
        "the sweep must contain at least two `target_partitions` values at or \
         above the segment count to show the un-cached count going flat; got {at_cap:?}"
    );
    let baseline = get_by_combo[&(at_cap[0], false)];
    for &tp in &at_cap[1..] {
        assert_eq!(
            get_by_combo[&(tp, false)],
            baseline,
            "un-cached GET count must be flat once the segment-count cap binds: \
             tp={tp} issued {}, tp={} issued {baseline}",
            get_by_combo[&(tp, false)],
            at_cap[0]
        );
    }

    // Request count, cache-wired: this is the side that keeps adding partitions
    // past the segment count, so it is the side where repeated whole-object reads
    // at one key actually happen. On this fixture -- a cache sized to hold every
    // object, no eviction -- they coalesce completely and the GET count is flat
    // across the sweep, and below the un-cached count at every partition value.
    // "Flat" is a property of THIS fixture, not of striping: a cache too small to
    // hold the working set would evict between partitions and issue GETs again.
    let &min_tp = expected_partitions.iter().min().unwrap();
    let cached_baseline = get_by_combo[&(min_tp, true)];
    for &tp in &expected_partitions {
        assert_eq!(
            get_by_combo[&(tp, true)],
            cached_baseline,
            "cache-wired GET count is flat across the sweep on this fixture: \
             tp={tp} issued {}, tp={min_tp} issued {cached_baseline}",
            get_by_combo[&(tp, true)]
        );
        assert!(
            get_by_combo[&(tp, true)] <= get_by_combo[&(tp, false)],
            "cache-wired tp={tp} GET count ({}) must not exceed the un-cached \
             count ({})",
            get_by_combo[&(tp, true)],
            get_by_combo[&(tp, false)]
        );
    }

    // Un-cached, the unit of assignment is the whole segment (ADR-0102, the
    // un-cached amendment): each segment is opened by exactly one partition, so
    // raising `target_partitions` below the cap adds no whole-object reads
    // either. The count is one plan read plus one scan read per segment at every
    // partition value, which the single-partition run measures directly.
    let uncached_baseline = get_by_combo[&(min_tp, false)];
    for &tp in &expected_partitions {
        assert_eq!(
            get_by_combo[&(tp, false)],
            uncached_baseline,
            "un-cached GET count is flat across the whole sweep once segments \
             are assigned whole: tp={tp} issued {}, tp={min_tp} issued \
             {uncached_baseline}",
            get_by_combo[&(tp, false)]
        );
    }
}
