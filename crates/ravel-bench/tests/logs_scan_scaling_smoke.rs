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
//! is exactly the shape #693 part 3's whole-segment fast path serves. Issue #739
//! dropped the fast path's block-range-threshold conjunct, so object size no
//! longer decides ELIGIBILITY: on every row whose declared partition count fits
//! inside the segment count -- both `over_threshold` values, cached and
//! un-cached alike -- the plan phase is skipped and no segment is planned or
//! probed for pruning.
//!
//! Since #887 object size decides the READ SHAPE inside that fast path instead.
//! `PartitionCtx::open_by_column_chunk` routes each segment as it opens it, and
//! it holds only when the segment declares RLOG v4 AND
//! `LogSegmentFetcher::ranged_projection_pays` judges the bytes a narrow
//! projection skips worth the round trips the probe-and-range protocol adds.
//! The fixture writes v4 objects, so on this sweep the `over_threshold` axis is
//! exactly that second conjunct: the whole-object rows put the block-range
//! threshold out of reach, `ranged_projection_pays` answers false at or below
//! it, and they keep the one whole-object GET per segment; the over-threshold
//! rows clear it with the projection's saving to spare and take the ranged
//! entry. Both are pinned below, each against the request and byte law of its
//! own route.
//!
//! Only the cache-wired row whose partition count exceeds the segment count
//! fails the fast path's fourth conjunct and stripes; it is pinned separately as
//! the one combo in the sweep that declines the fast path. On its over-threshold
//! read shape the striping is visible as extra probe and directory GETs; on its
//! whole-object read shape the plan pass and every partition's open share one
//! `(0, object_size)` cache key, so single-flight coalesces them and only the
//! plan pass, not a GET, is what the fast path removes there.
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
use ravel_query::{DEFAULT_LOG_SUFFIX_LEN, LOG_SUFFIX_FLOOR_BYTES};

/// The resolved snapshot's own catalog GET. `QueryAccounting` counts it in the
/// same `AccountedOp::Get` bucket as the scan's reads, so every per-segment
/// request law below is stated with this much slack rather than pretending the
/// scan is the only reader. It is one GET per query on this fixture, and it
/// carries no bytes (the key it probes is absent), which is why the byte
/// amplifications land on exact integers.
const CATALOG_GET_SLACK: u64 = 1;

/// Upper end of the band the whole-object fast-path rows must land in: one pass
/// over the dataset, plus at most one further read of every segment. A
/// per-partition read sequence, the shape block striping produced, would blow
/// straight past it at 32 partitions.
const MAX_FAST_PATH_AMPLIFICATION: f64 = 2.0;

/// Float slack on a ratio that is an exact integer quotient of two byte counts.
const EPS: f64 = 1e-9;

/// The share of an object's columns this report's statement reads, as
/// `ResolvedColumns::fraction_of` computes it: `SELECT ts, body` names two
/// columns and `ColumnSelection` always adds `stream_ref`, so three distinct
/// object columns out of the ten fixed ones `ravel_logseg::record` stores (the
/// fixture declares no typed attribute columns, so there is nothing else in the
/// denominator). This is the `projected_fraction` `LogsScanExec` hands
/// `ranged_projection_pays`, and the precondition assertion below re-derives the
/// routing decision from it rather than trusting the label on the row.
const PROJECTED_FRACTION: f64 = 3.0 / 10.0;

/// Requests per segment the ranged fast-path route issues, as a band derived
/// from the shape of the read rather than from an observed number.
///
/// The protocol (`ravel_query::log_fetcher`, ADR-0107) is: one suffix probe for
/// the tail sections (footer, PAGE_DIR, SKIP_IDX); one coalesced GET over the
/// two front directory sections (STREAM_DIR/FIELD_DIR), which no suffix probe
/// can reach; then the data. The data is ONE more GET on this fixture whichever
/// way the fetcher decides it: the effective coalescing gap is one request cost
/// (`DEFAULT_LOG_REQUEST_COST_BYTES`, ~1.8 MiB) and every object here is smaller
/// than that, so all projected column-chunk extents of an object fuse into a
/// single range -- and if the projected pages instead cover the BLOCKS section
/// past `DEFAULT_LOG_COVERAGE_THRESHOLD`, the crossover replaces them with one
/// whole-object GET, which is also one. Hence 3 per segment, and 2 if a probe
/// long enough to reach the front sections ever makes that GET redundant.
const RANGED_MIN_GETS_PER_SEGMENT: u64 = 2;
const RANGED_MAX_GETS_PER_SEGMENT: u64 = 3;

/// Whether this row's scan takes #693 part 3's whole-segment fast path: at least
/// as many relevant segments as the plan declared partitions (the fast path's
/// fourth conjunct, evaluated against the DECLARED count, which is what
/// `LogsScanExec` compares). Since #739 dropped the block-range-threshold
/// conjunct, object size no longer enters into ELIGIBILITY, so this is the only
/// conjunct that varies across the sweep: the statement is predicate-free and
/// its window contains every segment, so the other three hold for every row.
///
/// What the fast path then COSTS depends on the route it opens each segment
/// with; see [`reads_by_column_chunk`].
fn takes_fast_path(c: &ComboResult, segments: usize) -> bool {
    c.scan_partitions <= segments
}

/// Whether this fast-path row opens its segments by column chunk (#887's ranged
/// route, `PartitionCtx::open_by_column_chunk`) rather than with one
/// whole-object GET each.
///
/// The routing predicate is `segment_format_version >=
/// ravel_logseg::footer::VERSION && ranged_projection_pays(object_size,
/// projected_fraction)`. The fixture writes v4 objects through `RlogWriter` and
/// declares that version, so the first conjunct holds for every row. The second
/// is false at or below the fetcher's block-range threshold and true once the
/// object clears it by more than the projection's saving -- which is exactly the
/// sweep's `over_threshold` axis, since the whole-object rows build their
/// fetcher with the threshold at `u64::MAX`. The caller asserts the saving
/// really does clear the break-even on this fixture rather than assuming it.
fn reads_by_column_chunk(c: &ComboResult) -> bool {
    c.over_threshold
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
    // objects genuinely clear ADR-0107's threshold, and since #887 that
    // threshold is what routes them: `ranged_projection_pays` answers false
    // outright at or below it, so a sub-threshold object cannot reach the ranged
    // route at all and every figure below would describe a whole-object read
    // under an over-threshold label. Forcing the ranged path onto sub-threshold
    // objects instead (a threshold of 0) is the other route, and it is rejected
    // in the bench itself: the suffix probe has a 128 KiB floor, so on a small
    // object it re-reads the object and the byte figure measures object size
    // rather than the path.
    assert!(
        report.config.min_object_bytes > report.config.over_threshold_block_range_threshold,
        "the smallest published object ({} bytes) must exceed the block-range \
         threshold ({} bytes) for the over_threshold rows to measure ADR-0107's \
         above-threshold path",
        report.config.min_object_bytes,
        report.config.over_threshold_block_range_threshold
    );
    // The second conjunct of the same routing decision, re-derived here rather
    // than assumed: `ranged_projection_pays` takes the ranged route only when
    // the bytes the projection is expected to skip, `object_size * (1 -
    // projected_fraction)`, beat the fetcher's request-cost break-even -- which
    // `with_block_range_threshold` pins to the same threshold value. Stated on
    // the SMALLEST object, so it holds for every segment in the fixture.
    let projected_saving = report.config.min_object_bytes as f64 * (1.0 - PROJECTED_FRACTION);
    assert!(
        projected_saving > report.config.over_threshold_block_range_threshold as f64,
        "the projection skips {projected_saving:.0} bytes of the smallest object \
         ({} bytes at a projected fraction of {PROJECTED_FRACTION}), which must \
         beat the ranged path's break-even ({} bytes) for `ranged_projection_pays` \
         to route the over_threshold rows by column chunk",
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
    // Both claims are made only over the cache-wired rows that stay on the fast
    // path. The row above the segment count is the exception on both axes: it
    // drops out of the whole-segment fast path (fourth conjunct) while its
    // un-cached neighbour, capped at the segment count, stays in. It is pinned on
    // its own below, where the two read shapes diverge (the over-threshold one
    // pays visible probe GETs, the whole-object one coalesces onto the same key).
    for over_threshold in [false, true] {
        let comparable = |tp: usize| {
            let c = report.combo(tp, true, over_threshold).expect("cached row");
            takes_fast_path(c, segments)
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

    // Issue #693 part 3, the figures this report exists to carry, split by the
    // route #887 opens each segment with. A block-predicate-free statement whose
    // window contains every relevant segment, with at least as many relevant
    // segments as declared partitions, skips the plan phase on every row at or
    // below the segment count -- cached or not, and since #739 whatever the
    // object size. What one read of a segment then COSTS is the routing decision
    // (`reads_by_column_chunk`), and the two routes have different laws:
    //
    // - whole-object rows: one full GET per segment, and one pass over the
    //   dataset. The GET bound is what pins "zero plan probes" there: a plan
    //   phase costs one suffix probe per segment, which would put the count at 2x
    //   the segment count, far outside the slack.
    // - ranged rows: the probe-and-range protocol, whose per-segment request
    //   band is `RANGED_MIN_GETS_PER_SEGMENT..=RANGED_MAX_GETS_PER_SEGMENT`, and
    //   whose bytes are the data the read brings PLUS one suffix probe per
    //   segment. On this statement the probe is pure overhead: the projection is
    //   narrow by column count (`PROJECTED_FRACTION`), which is what routes it,
    //   but the `body` column carries nearly every stored byte, so the projected
    //   pages cover the BLOCKS section past `DEFAULT_LOG_COVERAGE_THRESHOLD` and
    //   the fetcher's byte-exact crossover falls back to one whole-object GET
    //   after the probe. The band below states that: one pass over the dataset
    //   plus a probe per segment.
    let dataset_bytes = report.config.dataset_bytes as f64;
    let segs_all = segments as u64;
    // `derive_suffix_len` clamps the probe to `[LOG_SUFFIX_FLOOR_BYTES,
    // DEFAULT_LOG_SUFFIX_LEN]` per object (#883), so the probe traffic over the
    // fixture is between one floor and one cap per segment. The upper bound
    // carries a second cap's worth on top for the front-section GETs and any
    // tail range, which are directory bytes and orders of magnitude smaller than
    // a probe. Neither end is an observed number: the floor is the constant, the
    // dataset size is the report's own reading.
    let ranged_amp_min = 1.0 + (segs_all * LOG_SUFFIX_FLOOR_BYTES) as f64 / dataset_bytes;
    let ranged_amp_max = 1.0 + 2.0 * (segs_all * DEFAULT_LOG_SUFFIX_LEN) as f64 / dataset_bytes;
    let mut whole_object_rows = 0usize;
    let mut ranged_rows = 0usize;
    for c in report
        .combos
        .iter()
        .filter(|c| takes_fast_path(c, segments))
    {
        let label = format!(
            "tp={} cache={} over_threshold={}",
            c.target_partitions, c.cache_wired, c.over_threshold
        );
        let segs = c.segments_scanned as u64;
        if reads_by_column_chunk(c) {
            ranged_rows += 1;
            assert!(
                c.object_store_get_requests >= RANGED_MIN_GETS_PER_SEGMENT * segs
                    && c.object_store_get_requests
                        <= RANGED_MAX_GETS_PER_SEGMENT * segs + CATALOG_GET_SLACK,
                "{label}: the ranged fast-path route issues a suffix probe, a \
                 front-section GET, and one coalesced data read per segment, so \
                 the GET count over {segs} segments must be in [{}, {}] (the \
                 upper end plus at most {CATALOG_GET_SLACK} catalog read); got {} \
                 ({:.4} reads per segment). One read per segment means the route \
                 stopped engaging -- the segments must declare RLOG \
                 `ravel_logseg::footer::VERSION` for `open_by_column_chunk` to \
                 reach `ranged_projection_pays` at all.",
                RANGED_MIN_GETS_PER_SEGMENT * segs,
                RANGED_MAX_GETS_PER_SEGMENT * segs,
                c.object_store_get_requests,
                c.reads_per_segment
            );
            assert!(
                c.bytes_amplification >= ranged_amp_min - EPS
                    && c.bytes_amplification <= ranged_amp_max,
                "{label}: the ranged route moves one pass over the dataset plus \
                 one suffix probe per segment, so bytes_amplification must be in \
                 [{ranged_amp_min:.4}, {ranged_amp_max:.4}]; got {:.4} ({} bytes \
                 over a {} byte dataset). Exactly 1.0 means the segments were read \
                 whole and the ranged route never engaged; below the band means \
                 the coverage crossover stopped falling back to the whole object, \
                 which is a routing change, not noise.",
                c.bytes_amplification,
                c.object_store_bytes,
                report.config.dataset_bytes
            );
            // The same partition value and cache setting on the other read shape:
            // the same plan, the same rows, off the ranged route. Comparing the
            // two is what makes the figures above a statement about the ROUTE
            // rather than about this fixture's request count.
            let whole = report
                .combo(c.target_partitions, c.cache_wired, false)
                .expect("the same partition value on the whole-object read shape");
            assert!(
                c.object_store_get_requests > whole.object_store_get_requests,
                "{label}: the ranged route pays a probe and a front-section GET \
                 the whole-object route never issues, so it must read strictly \
                 more than its whole-object neighbour ({}); got {}",
                whole.object_store_get_requests,
                c.object_store_get_requests
            );
        } else {
            whole_object_rows += 1;
            assert!(
                c.object_store_get_requests >= segs
                    && c.object_store_get_requests <= segs + CATALOG_GET_SLACK,
                "{label}: the whole-object fast-path route reads each of {segs} \
                 segments once and issues zero plan probes, so the GET count must \
                 be {segs} (plus at most {CATALOG_GET_SLACK} catalog read); got {} \
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
    }
    let fast_path_rows = whole_object_rows + ranged_rows;
    assert!(
        fast_path_rows >= 8,
        "the sweep must pin the fast path on both cache settings and both \
         `over_threshold` values at more than one partition value; got \
         {fast_path_rows} rows"
    );
    assert_eq!(
        whole_object_rows, ranged_rows,
        "the `over_threshold` axis is the routing axis, so the fast-path rows \
         must split evenly between the two routes; got {whole_object_rows} \
         whole-object and {ranged_rows} ranged"
    );
    assert!(
        ranged_rows >= 4,
        "the ranged route must be pinned on both cache settings at more than one \
         partition value; got {ranged_rows} rows"
    );
    for over_threshold in [false, true] {
        for cache_wired in [false, true] {
            for &tp in expected_partitions.iter().filter(|&&tp| tp <= segments) {
                let c = report
                    .combo(tp, cache_wired, over_threshold)
                    .expect("swept row");
                assert!(
                    takes_fast_path(c, segments),
                    "tp={tp} cache={cache_wired} over_threshold={over_threshold}: \
                     a partition value at or below the segment count must stay on \
                     the fast path since #739 -- #887 routes what one read of a \
                     segment costs, not whether the fast path applies -- but the \
                     plan declared {} partitions",
                    c.scan_partitions
                );
            }
        }
    }

    // The other side of the fourth conjunct: a row with MORE declared partitions
    // than relevant segments cannot fill every partition from whole segments, so
    // it declines the fast path and falls back to plan-then-stripe. Only the
    // cache-wired row above the segment count reaches this -- the un-cached count
    // is capped at the segment count -- and since #739 it reaches it on BOTH
    // `over_threshold` values, so both are pinned off the fast path here. What
    // the striping COSTS then depends on the read shape, and the two diverge:
    //
    // - Above the threshold each partition opens its owned segments with a suffix
    //   probe, one GET per directory section, and coalesced candidate blocks. The
    //   probe and section GETs are real work the fast path skips, so the row
    //   reads strictly more than its fast-path neighbour -- at least one probe per
    //   segment on top of one open each (>= 2x the segment count) -- while staying
    //   under one extra pass over the dataset, because each partition fetches only
    //   its own candidate blocks rather than the whole object.
    // - At or below the threshold every partition's open, and the plan pass, is a
    //   whole-object `GetRange::Full` on the one `(0, object_size)` cache key, so
    //   single-flight coalesces them all. The striped row then issues the SAME GET
    //   count and reads the SAME single pass as its fast-path neighbour: leaving
    //   the fast path costs the plan pass, not a GET (ADR-0102's #739 amendment).
    let striped: Vec<&ComboResult> = report
        .combos
        .iter()
        .filter(|c| c.scan_partitions > segments)
        .collect();
    assert!(
        !striped.is_empty(),
        "the sweep must contain a `target_partitions` value above the segment \
         count ({segments}) so the fast path's relevant_segments >= \
         target_partitions conjunct is exercised failing"
    );
    let mut striped_over_threshold = 0usize;
    let mut striped_whole_object = 0usize;
    for c in striped {
        let label = format!(
            "tp={} cache={} over_threshold={}",
            c.target_partitions, c.cache_wired, c.over_threshold
        );
        let segs = c.segments_scanned as u64;
        assert!(
            !takes_fast_path(c, segments) && c.scan_partitions > segments,
            "{label}: a row above the segment count must decline the fast path; \
             the plan declared {} partitions",
            c.scan_partitions
        );
        // The fast-path neighbour: the same partition value and read shape on the
        // other cache setting, capped at the segment count and so still on the
        // fast path.
        let fast = report
            .combo(c.target_partitions, !c.cache_wired, c.over_threshold)
            .expect("the same partition value on the other cache setting");
        assert!(
            takes_fast_path(fast, segments),
            "{label}: the un-cached neighbour is capped at the segment count and \
             must stay on the fast path; it declared {} partitions",
            fast.scan_partitions
        );
        if c.over_threshold {
            striped_over_threshold += 1;
            assert!(
                c.object_store_get_requests >= 2 * segs,
                "{label}: off the fast path the plan phase probes each of {segs} \
                 segments and the scan opens each of them at least once, so the \
                 GET count must be at least {}; got {} over {:.1} blocks per \
                 segment",
                2 * segs,
                c.object_store_get_requests,
                c.blocks_per_segment()
            );
            assert!(
                c.object_store_get_requests > fast.object_store_get_requests,
                "{label}: leaving the fast path on the ranged read shape must cost \
                 strictly more GETs than the tp={} row that keeps it; got {} \
                 against {}",
                fast.target_partitions,
                c.object_store_get_requests,
                fast.object_store_get_requests
            );
            assert!(
                c.bytes_amplification > 1.0 && c.bytes_amplification < 2.0,
                "{label}: the striped path re-reads the probe and directory bytes \
                 the fast path never fetches, but fetches only candidate blocks \
                 per partition, so bytes_amplification must sit in (1.0, 2.0); got \
                 {:.4}",
                c.bytes_amplification
            );
        } else {
            striped_whole_object += 1;
            assert_eq!(
                c.object_store_get_requests, fast.object_store_get_requests,
                "{label}: on the whole-object read shape the plan pass and every \
                 partition's open share the one `(0, object_size)` cache key, so \
                 single-flight coalesces them and the striped row issues the same \
                 GET count as its fast-path neighbour ({}); got {}",
                fast.object_store_get_requests, c.object_store_get_requests
            );
            assert!(
                (c.bytes_amplification - 1.0).abs() <= EPS,
                "{label}: coalesced whole-object reads move one pass over the \
                 dataset, so bytes_amplification must be 1.0; got {:.4} ({} bytes \
                 over a {} byte dataset)",
                c.bytes_amplification,
                c.object_store_bytes,
                report.config.dataset_bytes
            );
        }
    }
    assert_eq!(
        (striped_over_threshold, striped_whole_object),
        (1, 1),
        "the sweep must stripe exactly the cache-wired row above the segment \
         count on each read shape; got {striped_over_threshold} over-threshold \
         and {striped_whole_object} whole-object striped rows"
    );
}
