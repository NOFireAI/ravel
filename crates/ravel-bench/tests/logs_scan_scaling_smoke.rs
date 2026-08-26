//! Acceptance test for the `logs_scan_scaling_bench` path (ADR-0102 decision 1,
//! epic #361 item 1; issue #693).
//!
//! Drives `logs_scan_scaling::run` in its smoke configuration end to end against
//! an in-process `MemoryStore` and asserts the report proves what this item
//! actually ships: with ADR-0046's read cache wired, an undersubscribed `logs`
//! scan fans out to `target_partitions` block-level partitions (more than the
//! segment count); without it the partition count is capped at the segment count
//! and the object-store GET count therefore stops responding to
//! `target_partitions` once the cap binds.
//!
//! It also pins the figure issue #693 exists for: on the un-cached,
//! over-threshold, full-window rows the report's `bytes_amplification` is the
//! amplification factor itself, and it must match one plan read plus one scan
//! read per partition. The cached rows cannot show it -- they sit at ~1.0 -- so
//! the pin names the row it reads explicitly.
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

/// Slack allowed on the pinned `bytes_amplification`: one further read of every
/// segment, i.e. one whole extra pass over the dataset. Wide enough to absorb
/// ADR-0107's fixed per-sequence overhead (the 64 KiB suffix probe and the
/// directory sections, a few percent of a 1.2 MiB object each), narrow enough
/// that a single extra read sequence per partition would break it.
const AMPLIFICATION_TOLERANCE: f64 = 1.0;

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
        expected_partitions.len() * 4,
        "one entry per (target_partitions x cache x over_threshold) combination"
    );

    // The `over_threshold` rows are only what they claim if the fixture's
    // objects genuinely clear ADR-0107's threshold. Forcing the ranged path onto
    // sub-threshold objects instead would make the 64 KiB suffix probe re-read
    // each whole object and roughly double every bytes figure below.
    assert!(
        report.config.min_object_bytes > report.config.over_threshold_block_range_threshold,
        "the smallest published object ({} bytes) must exceed the block-range \
         threshold ({} bytes) for the over_threshold rows to measure ADR-0107's \
         ranged path",
        report.config.min_object_bytes,
        report.config.over_threshold_block_range_threshold
    );
    assert!(
        report.config.dataset_bytes > 0,
        "bytes_amplification needs a non-zero dataset to divide by"
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

    let mut get_by_combo: HashMap<(usize, bool, bool), u64> = HashMap::new();
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
            (c.target_partitions, c.cache_wired, c.over_threshold),
            c.object_store_get_requests,
        );
    }

    // Request count, un-cached: once `target_partitions` reaches the segment
    // count the partition count stops growing, so the GET count stops growing
    // with it. This is the property the has_cache gate buys -- without it, every
    // partition past the segment count added whole-object reads that nothing
    // absorbed. Checked on both read shapes: the shape changes how many GETs one
    // read sequence costs, not how many sequences the plan issues.
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
    for over_threshold in [false, true] {
        let baseline = get_by_combo[&(at_cap[0], false, over_threshold)];
        for &tp in &at_cap[1..] {
            assert_eq!(
                get_by_combo[&(tp, false, over_threshold)],
                baseline,
                "un-cached GET count must be flat once the segment-count cap binds \
                 (over_threshold={over_threshold}): tp={tp} issued {}, tp={} issued {baseline}",
                get_by_combo[&(tp, false, over_threshold)],
                at_cap[0]
            );
        }
    }

    // Request count, cache-wired: this is the side that keeps adding partitions
    // past the segment count, so it is the side where repeated reads at one key
    // actually happen. On this fixture -- a cache sized to hold every object, no
    // eviction -- they coalesce completely and the GET count is flat across the
    // sweep, and below the un-cached count at every partition value. "Flat" is a
    // property of THIS fixture, not of striping: a cache too small to hold the
    // working set would evict between partitions and issue GETs again.
    let &min_tp = expected_partitions.iter().min().unwrap();
    for over_threshold in [false, true] {
        let cached_baseline = get_by_combo[&(min_tp, true, over_threshold)];
        for &tp in &expected_partitions {
            assert_eq!(
                get_by_combo[&(tp, true, over_threshold)],
                cached_baseline,
                "cache-wired GET count is flat across the sweep on this fixture \
                 (over_threshold={over_threshold}): tp={tp} issued {}, tp={min_tp} \
                 issued {cached_baseline}",
                get_by_combo[&(tp, true, over_threshold)]
            );
            assert!(
                get_by_combo[&(tp, true, over_threshold)]
                    <= get_by_combo[&(tp, false, over_threshold)],
                "cache-wired tp={tp} GET count ({}) must not exceed the un-cached \
                 count ({}) at over_threshold={over_threshold}",
                get_by_combo[&(tp, true, over_threshold)],
                get_by_combo[&(tp, false, over_threshold)]
            );
        }
    }

    // The multiplier the segment-count cap bounds but does not remove: below the
    // cap, raising `target_partitions` still adds reads, because every partition
    // owning blocks in a segment opens that segment itself. Only a
    // single-partition plan matches the pre-item-1 whole-segment request count.
    let &max_tp = expected_partitions.iter().max().unwrap();
    if min_tp == 1 && max_tp > 1 {
        for over_threshold in [false, true] {
            assert!(
                get_by_combo[&(max_tp, false, over_threshold)]
                    > get_by_combo[&(1, false, over_threshold)],
                "un-cached GET count still climbs from a single partition to the \
                 cap (over_threshold={over_threshold}): tp={max_tp} issued {}, \
                 tp=1 issued {}",
                get_by_combo[&(max_tp, false, over_threshold)],
                get_by_combo[&(1, false, over_threshold)]
            );
        }
    }

    // Issue #693, the figure this report exists to carry. On the un-cached,
    // over-threshold, full-window row the plan phase reads every object once and
    // then every partition owning blocks in a segment fetches that segment's
    // candidates again, so the bytes fetched come to
    // `1 + min(partitions, blocks_per_segment)` passes over the dataset. Read
    // from the un-cached row by name: the cached row is ~1.0 whatever the
    // partition count, which is exactly why it could never show this.
    //
    // When #693 part 1 lands (fleet task f343d69b changes the un-cached block
    // assignment in ravel-sql/ravel-query so a partition fetches only its own
    // blocks), the same row drops to ~2 -- one plan read plus one scan read
    // shared across partitions -- and this expectation is what will flip.
    for &tp in &expected_partitions {
        let row = report
            .combo(tp, false, true)
            .unwrap_or_else(|| panic!("no un-cached over-threshold row at tp={tp}"));
        let expected = 1.0 + (row.scan_partitions as f64).min(row.blocks_per_segment());
        assert!(
            (row.bytes_amplification - expected).abs() <= AMPLIFICATION_TOLERANCE,
            "un-cached over-threshold tp={tp}: bytes_amplification {:.3} is not \
             within {AMPLIFICATION_TOLERANCE} of the expected {expected:.3} \
             (1 plan read + min({} partitions, {:.1} blocks/segment) scan reads); \
             {} bytes fetched over a {} byte dataset",
            row.bytes_amplification,
            row.scan_partitions,
            row.blocks_per_segment(),
            row.object_store_bytes,
            report.config.dataset_bytes
        );
    }

    // The request-side contrast, in its own pass so the amplification pin above
    // is the assertion that speaks first when a row is misread: at one partition
    // the two rows are only one plan read apart, so it is the multi-partition
    // values that separate them.
    for &tp in &expected_partitions {
        let uncached = report.combo(tp, false, true).expect("un-cached row");
        let cached = report.combo(tp, true, true).expect("cached row");
        assert!(
            uncached.reads_per_segment > cached.reads_per_segment,
            "un-cached over-threshold tp={tp} must issue more reads per segment \
             ({:.2}) than the cached row ({:.2})",
            uncached.reads_per_segment,
            cached.reads_per_segment
        );
    }
}
