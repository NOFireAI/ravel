//! Acceptance test for the `logs_scan_scaling_bench` path (ADR-0102 decision 1,
//! epic #361 item 1; issue #693).
//!
//! Drives `logs_scan_scaling::run` in its smoke configuration end to end against
//! an in-process `MemoryStore` and asserts the report proves what this item
//! actually ships: with ADR-0046's read cache wired, an undersubscribed `logs`
//! scan fans out to `target_partitions` block-level partitions (more than the
//! segment count); without it the partition count is capped at the segment count
//! and each segment is assigned whole to one partition, so the object-store GET
//! count does not respond to `target_partitions` at all.
//!
//! It also pins the read-amplification figures issue #693 exists for. The
//! report's statement is `SELECT ts, body FROM logs` over the full window, which
//! is exactly the shape #693 part 3's whole-segment fast path serves, so on the
//! `over_threshold` rows whose partition count fits inside the segment count the
//! plan phase is skipped and each segment is read whole exactly once: one read
//! per segment (the same statement as "the plan phase issued zero probes") and
//! bytes amplification of about 1.0, cached and un-cached alike. The row whose
//! partition count exceeds the segment count fails the fast path's fourth
//! conjunct and takes the plan-then-stripe path instead; it is pinned
//! separately, because it is the only combo in the sweep that still pays a plan
//! phase on an above-threshold object.
//!
//! Gated on `sql-latency` (the module and bin's gate), so a default
//! `cargo test -p ravel-bench` sees an empty crate rather than a link error. Run
//! with `cargo test -p ravel-bench --features sql-latency`.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_bench::logs_scan_scaling::{ComboResult, LogsScanScalingConfig, run};
use ravel_object_store::memory::MemoryStore;

/// The resolved snapshot's own catalog GET. `QueryAccounting` counts it in the
/// same `AccountedOp::Get` bucket as the scan's reads, so every per-segment
/// request law below is stated with this much slack rather than pretending the
/// scan is the only reader. It is one GET per query on this fixture, and it
/// carries no bytes (the key it probes is absent), which is why the byte
/// amplifications land on exact integers.
const CATALOG_GET_SLACK: u64 = 1;

/// Upper end of the band the `over_threshold` full-window rows must land in:
/// one pass over the dataset, plus at most one further read of every segment.
/// A per-partition read sequence, the shape block striping produced, would blow
/// straight past it at 32 partitions.
const MAX_FAST_PATH_AMPLIFICATION: f64 = 2.0;

/// Float slack on a ratio that is an exact integer quotient of two byte counts.
const EPS: f64 = 1e-9;

/// Whether this row's scan could take #693 part 3's whole-segment fast path:
/// above the block-range threshold, and with at least as many relevant segments
/// as the plan declared partitions (the fast path's fourth conjunct, evaluated
/// against the DECLARED count, which is what `LogsScanExec` compares). The
/// statement itself is predicate-free and its window contains every segment, so
/// those two conjuncts hold for every row in this sweep.
fn takes_fast_path(c: &ComboResult, segments: usize) -> bool {
    c.over_threshold && c.scan_partitions <= segments
}

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
    // each whole object, and would put the fast path out of reach entirely (its
    // `object_size > block_range_threshold` conjunct), so every figure below
    // would describe the plan-then-stripe path under an over-threshold label.
    assert!(
        report.config.min_object_bytes > report.config.over_threshold_block_range_threshold,
        "the smallest published object ({} bytes) must exceed the block-range \
         threshold ({} bytes) for the over_threshold rows to measure ADR-0107's \
         above-threshold path",
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
        let label = format!(
            "tp={} cache={} over_threshold={}",
            c.target_partitions, c.cache_wired, c.over_threshold
        );
        assert!(
            c.error.is_none(),
            "combo {label} unexpectedly failed: {:?}",
            c.error
        );
        assert!(
            c.median_ms > 0.0,
            "combo {label} must report non-zero timing"
        );
        assert_eq!(
            c.result_rows, report.config.total_records,
            "a bare `SELECT ts, body FROM logs` returns every record ({label})"
        );
        assert_eq!(
            c.segments_scanned, segments,
            "every combo scans the whole fixture ({label})"
        );

        // The declared fan-out is `target_partitions` only when the read cache is
        // wired (ADR-0102 decision 1's precondition: the extra partitions'
        // repeated reads need the cache to coalesce). Un-cached it falls back to
        // the pre-item-1 bound. This is decided before execution, so the
        // whole-segment fast path does not move it.
        let expected = if c.cache_wired {
            c.target_partitions
        } else {
            c.target_partitions.min(segments)
        };
        assert_eq!(
            c.scan_partitions, expected,
            "{label}: expected {expected} scan partitions, got {}",
            c.scan_partitions
        );

        // Every declared partition owns work on this fixture (32 segments and 192
        // blocks across at most 64 partitions), on both the whole-segment and the
        // block-striped assignment -- and in the cached, undersubscribed case
        // that exceeds the segment count, which is the capability this item adds.
        assert_eq!(
            c.non_empty_partitions, expected,
            "{label}: every declared partition should decode blocks; got {}",
            c.non_empty_partitions
        );
        if c.cache_wired && c.target_partitions > segments {
            assert!(
                c.non_empty_partitions > segments,
                "cached {label} (> {segments} segments) must fan out past the \
                 segment count; got {}",
                c.non_empty_partitions
            );
        }

        get_by_combo.insert(
            (c.target_partitions, c.cache_wired, c.over_threshold),
            c.object_store_get_requests,
        );
    }

    // Request count, un-cached: each segment is assigned whole to one partition
    // (ADR-0102's un-cached amendment, #693 part 1), so raising
    // `target_partitions` adds no reads at any value, below or above the cap.
    // Checked on both read shapes: the shape changes what one read of a segment
    // costs, not how many of them the plan issues.
    let &min_tp = expected_partitions.iter().min().unwrap();
    for over_threshold in [false, true] {
        let baseline = get_by_combo[&(min_tp, false, over_threshold)];
        for &tp in &expected_partitions {
            assert_eq!(
                get_by_combo[&(tp, false, over_threshold)],
                baseline,
                "un-cached GET count is flat across the whole sweep once segments \
                 are assigned whole (over_threshold={over_threshold}): tp={tp} \
                 issued {}, tp={min_tp} issued {baseline}",
                get_by_combo[&(tp, false, over_threshold)]
            );
        }
    }

    // Request count, cache-wired: this is the side that keeps adding partitions
    // past the segment count, so it is the side where repeated reads at one key
    // actually happen. On this fixture -- a cache sized to hold every object, no
    // eviction -- they coalesce completely and the GET count is flat, and never
    // above the un-cached count. "Flat" is a property of THIS fixture, not of
    // striping: a cache too small to hold the working set would evict between
    // partitions and issue GETs again.
    //
    // Both claims are made only over rows that take the same path as their
    // un-cached neighbour. The over-threshold row above the segment count is the
    // exception in both directions: it drops out of the whole-segment fast path
    // (fourth conjunct) while its un-cached neighbour stays in, so it pays a plan
    // phase the other does not, and it is pinned on its own below.
    for over_threshold in [false, true] {
        let comparable = |tp: usize| {
            let c = report.combo(tp, true, over_threshold).expect("cached row");
            !over_threshold || takes_fast_path(c, segments)
        };
        let flat: Vec<usize> = expected_partitions
            .iter()
            .copied()
            .filter(|&tp| comparable(tp))
            .collect();
        assert!(
            flat.len() >= 2,
            "the sweep needs at least two cache-wired rows on the same path to \
             show the GET count going flat (over_threshold={over_threshold}); \
             got {flat:?}"
        );
        let cached_baseline = get_by_combo[&(flat[0], true, over_threshold)];
        for &tp in &flat {
            assert_eq!(
                get_by_combo[&(tp, true, over_threshold)],
                cached_baseline,
                "cache-wired GET count is flat across the sweep on this fixture \
                 (over_threshold={over_threshold}): tp={tp} issued {}, tp={} \
                 issued {cached_baseline}",
                get_by_combo[&(tp, true, over_threshold)],
                flat[0]
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

    // Issue #693 part 3, the figure this report exists to carry. A
    // block-predicate-free statement whose window contains every relevant
    // segment, over segments above the block-range threshold, with at least as
    // many relevant segments as declared partitions, skips the plan phase and
    // reads each segment whole exactly once -- cached or not. So on every
    // over-threshold row at or below the segment count the GET count is the
    // segment count (plus the snapshot's own catalog read) and the bytes come to
    // one pass over the dataset.
    //
    // The GET bound is what pins "zero plan probes": a plan phase costs one
    // suffix probe per segment, which would put the count at 2x the segment
    // count, far outside the slack.
    let mut fast_path_rows = 0usize;
    for c in report
        .combos
        .iter()
        .filter(|c| takes_fast_path(c, segments))
    {
        fast_path_rows += 1;
        let label = format!(
            "tp={} cache={} over_threshold=true",
            c.target_partitions, c.cache_wired
        );
        let segs = c.segments_scanned as u64;
        assert!(
            c.object_store_get_requests >= segs
                && c.object_store_get_requests <= segs + CATALOG_GET_SLACK,
            "{label}: the whole-segment fast path reads each of {segs} segments \
             once and issues zero plan probes, so the GET count must be {segs} \
             (plus at most {CATALOG_GET_SLACK} catalog read); got {} \
             ({:.4} reads per segment)",
            c.object_store_get_requests,
            c.reads_per_segment
        );
        assert!(
            c.bytes_amplification >= 1.0 - EPS
                && c.bytes_amplification <= MAX_FAST_PATH_AMPLIFICATION,
            "{label}: bytes_amplification {:.4} is outside the one-pass band \
             [1.0, {MAX_FAST_PATH_AMPLIFICATION}]; {} bytes fetched over a {} \
             byte dataset",
            c.bytes_amplification,
            c.object_store_bytes,
            report.config.dataset_bytes
        );
    }
    assert!(
        fast_path_rows >= 4,
        "the sweep must pin the fast path on both cache settings at more than one \
         partition value; got {fast_path_rows} rows"
    );
    for cache_wired in [false, true] {
        for &tp in expected_partitions.iter().filter(|&&tp| tp <= segments) {
            let c = report
                .combo(tp, cache_wired, true)
                .expect("over-threshold row");
            assert!(
                takes_fast_path(c, segments),
                "tp={tp} cache={cache_wired}: a partition value at or below the \
                 segment count must stay on the fast path, but the plan declared \
                 {} partitions",
                c.scan_partitions
            );
        }
    }

    // The other side of the fourth conjunct: a row with MORE declared partitions
    // than relevant segments cannot fill every partition from whole segments, so
    // it falls back to plan-then-stripe. It pays a suffix probe per segment that
    // the fast path skips, plus at least one open of every segment, so it reads
    // strictly more than the same-shape row that keeps the fast path -- while
    // staying under one extra pass over the dataset, because above the threshold
    // each partition fetches only its own candidate blocks rather than the whole
    // object.
    let striped: Vec<&ComboResult> = report
        .combos
        .iter()
        .filter(|c| c.over_threshold && c.scan_partitions > segments)
        .collect();
    assert!(
        !striped.is_empty(),
        "the sweep must contain a `target_partitions` value above the segment \
         count ({segments}) so the fast path's relevant_segments >= \
         target_partitions conjunct is exercised failing"
    );
    for c in striped {
        let label = format!(
            "tp={} cache={} over_threshold=true",
            c.target_partitions, c.cache_wired
        );
        let segs = c.segments_scanned as u64;
        assert!(
            c.object_store_get_requests >= 2 * segs,
            "{label}: off the fast path the plan phase probes each of {segs} \
             segments and the scan opens each of them at least once, so the GET \
             count must be at least {}; got {} over {:.1} blocks per segment",
            2 * segs,
            c.object_store_get_requests,
            c.blocks_per_segment()
        );
        let fast = report
            .combo(c.target_partitions, !c.cache_wired, true)
            .expect("the same partition value on the other cache setting");
        assert!(
            takes_fast_path(fast, segments)
                && c.object_store_get_requests > fast.object_store_get_requests,
            "{label}: leaving the fast path must cost strictly more GETs than the \
             tp={} row that keeps it; got {} against {}",
            fast.target_partitions,
            c.object_store_get_requests,
            fast.object_store_get_requests
        );
        assert!(
            c.bytes_amplification > 1.0 && c.bytes_amplification < 2.0,
            "{label}: the striped path re-reads the probe and directory bytes the \
             fast path never fetches, but fetches only candidate blocks per \
             partition, so bytes_amplification must sit in (1.0, 2.0); got {:.4}",
            c.bytes_amplification
        );
    }

    // The whole-object rows, which read the SAME objects with the block-range
    // threshold moved out of reach: below the threshold the fast path does not
    // apply, so the plan-then-stripe path runs and the plan read is a real
    // whole-object read rather than a probe. Un-cached that is one plan read plus
    // one scan read per segment; cache-wired the two share the one
    // `(0, object_size)` key, so only the plan read reaches the store.
    for c in report.combos.iter().filter(|c| !c.over_threshold) {
        let label = format!(
            "tp={} cache={} over_threshold=false",
            c.target_partitions, c.cache_wired
        );
        let segs = c.segments_scanned as u64;
        let (reads, amplification) = if c.cache_wired { (1, 1.0) } else { (2, 2.0) };
        assert!(
            c.object_store_get_requests >= reads * segs
                && c.object_store_get_requests <= reads * segs + CATALOG_GET_SLACK,
            "{label}: the whole-object path costs {reads} store read(s) per \
             segment over {segs} segments, so the GET count must be {} (plus at \
             most {CATALOG_GET_SLACK} catalog read); got {}",
            reads * segs,
            c.object_store_get_requests
        );
        assert!(
            (c.bytes_amplification - amplification).abs() <= EPS,
            "{label}: bytes_amplification must be {amplification:.1}; got {:.4} \
             ({} bytes over a {} byte dataset)",
            c.bytes_amplification,
            c.object_store_bytes,
            report.config.dataset_bytes
        );
    }
}
