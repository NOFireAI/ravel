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
//! It also pins the request SHAPE of that scan, split by read shape (issue
//! #862). The report's statement is `SELECT ts, body FROM logs` over the full
//! window: predicate-free, contains every segment, and NARROW -- it resolves to
//! three of the object's columns, a column-count fraction the ranged arbiter
//! (`LogSegmentFetcher::ranged_projection_pays`) judges worth the column-chunk
//! protocol, but only on a v4 object above the block-range threshold. The
//! `over_threshold` axis is what makes that condition true or false for the same
//! statement, so the two rows of each `(target_partitions, cache)` cell take
//! DIFFERENT read shapes:
//!
//! - `over_threshold = false` puts the threshold out of reach, so every segment
//!   keeps the whole-object read: one GET per segment (plus the resolve's own
//!   catalog probe), moving exactly the dataset bytes.
//! - `over_threshold = true` uses ADR-0107's production threshold, which every
//!   object here clears, so on these v4 segments the narrow projection routes by
//!   column chunk: a fixed per-segment request sequence (a suffix probe and two
//!   coalesced column-chunk ranges on this fixture), and -- because `body`
//!   dominates the object -- slightly MORE wire bytes than the whole-object read,
//!   the suffix-probe overhead the whole-object read never pays.
//!
//! Both counts are asserted exactly, not banded: this is the test that catches a
//! request-shape regression, so a change that reroutes either shape (the version
//! gate dropped, or the ranged arbiter's break-even moved) moves one of these
//! equalities. It mirrors `ravel-sql`'s `logs_fast_path_projection_routing.rs`,
//! which pins the same narrow/wide split at the `(full, suffix, range)` tuple
//! level; here `QueryAccounting` reports a single GET bucket, so the tuple
//! collapses to its total. The counts hold on every row of the sweep: with a
//! cache sized to hold the dataset, a striped row that fans out past the segment
//! count coalesces its repeats at one key onto single-flight GETs and issues the
//! same requests as the fast-path row it fans out from, and un-cached the
//! partition count is capped at the segment count and the fast path fires
//! directly.
//!
//! Gated on `sql-latency` (the module and bin's gate), so a default
//! `cargo test -p ravel-bench` sees an empty crate rather than a link error. Run
//! with `cargo test -p ravel-bench --features sql-latency`.
#![cfg(feature = "sql-latency")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_bench::logs_scan_scaling::{ComboResult, LogsScanScalingConfig, run};
use ravel_object_store::memory::MemoryStore;

/// The resolved snapshot's own catalog GET. `QueryAccounting` counts it in the
/// same `AccountedOp::Get` bucket as the scan's reads, so every per-segment
/// request law below is stated with this much slack rather than pretending the
/// scan is the only reader. It is one GET per query on this fixture, and it
/// carries no bytes (the key it probes is absent), which is why the whole-object
/// rows move exactly the dataset bytes.
const CATALOG_GET_SLACK: u64 = 1;

/// Whether this row's scan takes #693 part 3's whole-segment fast path: at least
/// as many relevant segments as the plan declared partitions (the fast path's
/// fourth conjunct, evaluated against the DECLARED count, which is what
/// `LogsScanExec` compares). Since #739 dropped the block-range-threshold
/// conjunct, object size no longer enters into it, so this is the only conjunct
/// that varies across the sweep: the statement is predicate-free and its window
/// contains every segment, so the other three hold for every row.
fn takes_fast_path(c: &ComboResult, segments: usize) -> bool {
    c.scan_partitions <= segments
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
    }

    // The request-shape invariant (issue #862), asserted exactly per read shape.
    // For the SAME statement, the two `over_threshold` rows take different routes
    // on these v4 segments, and each route has an exact per-segment request count:
    //
    // - whole-object read (`over_threshold = false`, threshold out of reach):
    //   one GET per segment, moving exactly the dataset bytes.
    // - column-chunk ranged read (`over_threshold = true`, threshold cleared):
    //   the narrow `SELECT ts, body` projection (three of the object's columns)
    //   routes by column chunk, a fixed sequence per segment.
    //
    // Both are pinned as EQUALITIES, not bands: this is the test that catches a
    // request-shape regression, so a reroute in either direction moves one of
    // them. Dropping the version gate would route the whole-object row ranged
    // too; a broken ranged arbiter would collapse the ranged row back to the
    // whole-object read. The mirror of `ravel-sql`'s
    // `logs_fast_path_projection_routing.rs`, which pins the same split at the
    // `(full, suffix, range)` tuple; `QueryAccounting` exposes one GET bucket, so
    // here the tuple collapses to its total.
    //
    // The exact counts are asserted only on the WHOLE-SEGMENT FAST PATH rows
    // (`scan_partitions <= segments`), where each segment is assigned to exactly
    // one partition and opened exactly once, so the count is deterministic. A
    // striped, cache-wired row above the segment count fans many partitions onto
    // one segment key: single-flight coalescing there is timing-dependent (it
    // saved 30 of 32 repeats one run, 31 the next), so its total is racy by one
    // or two GETs and is not a fixture invariant. Those rows are pinned only for
    // their fan-out (`non_empty_partitions > segments`, above) and their
    // existence (below), not for an exact count.

    /// The whole-object read opens each segment with one GET.
    const WHOLE_OBJECT_GETS_PER_SEGMENT: u64 = 1;
    /// The column-chunk ranged read opens each segment with one suffix probe and
    /// two coalesced column-chunk range GETs, on this fixture's `SELECT ts, body`
    /// projection. The exact `(full, suffix, range)` split is pinned in
    /// `crates/ravel-sql/tests/logs_fast_path_projection_routing.rs`; here only
    /// the total is observable.
    const RANGED_GETS_PER_SEGMENT: u64 = 3;

    let segs = segments as u64;
    let expected_gets = |over_threshold: bool| -> u64 {
        let per_seg = if over_threshold {
            RANGED_GETS_PER_SEGMENT
        } else {
            WHOLE_OBJECT_GETS_PER_SEGMENT
        };
        per_seg * segs + CATALOG_GET_SLACK
    };

    let mut fast_whole_rows = 0usize;
    let mut fast_ranged_rows = 0usize;
    for c in report
        .combos
        .iter()
        .filter(|c| takes_fast_path(c, segments))
    {
        let label = format!(
            "tp={} cache={} over_threshold={}",
            c.target_partitions, c.cache_wired, c.over_threshold
        );
        assert_eq!(
            c.object_store_get_requests,
            expected_gets(c.over_threshold),
            "{label}: on the fast path a {} read opens each of {segs} segments \
             {} (plus {CATALOG_GET_SLACK} catalog read), so the GET count must be \
             {}; got {} ({:.4} reads per segment)",
            if c.over_threshold {
                "column-chunk ranged"
            } else {
                "whole-object"
            },
            if c.over_threshold {
                "with 1 suffix probe and 2 coalesced column-chunk ranges"
            } else {
                "once"
            },
            expected_gets(c.over_threshold),
            c.object_store_get_requests,
            c.reads_per_segment,
        );

        if c.over_threshold {
            fast_ranged_rows += 1;
            // Column-chunk ranged read: because `body` dominates this object, the
            // narrow projection still moves MORE wire bytes than the whole-object
            // read it replaced -- the suffix-probe overhead the whole-object read
            // never pays -- not fewer. The routing decision here is a column-count
            // judgement (3 of 10 columns), not a byte one.
            assert!(
                c.object_store_bytes > report.config.dataset_bytes,
                "{label}: the ranged route re-reads the suffix probe the \
                 whole-object route never fetches, so on this body-dominated \
                 projection it moves more than the dataset's {} bytes; got {}",
                report.config.dataset_bytes,
                c.object_store_bytes,
            );
        } else {
            fast_whole_rows += 1;
            // Whole-object read: one pass over the dataset, exactly.
            assert_eq!(
                c.object_store_bytes, report.config.dataset_bytes,
                "{label}: a whole-object read moves exactly the dataset's {} \
                 bytes; got {}",
                report.config.dataset_bytes, c.object_store_bytes,
            );
        }
    }
    // Neither shape may go unasserted: the fixture straddles the segment count on
    // both cache settings, so each read shape has several fast-path rows.
    assert!(
        fast_whole_rows >= 2 && fast_ranged_rows >= 2,
        "the sweep must pin both read shapes on the fast path at more than one \
         partition value; got {fast_whole_rows} whole-object and \
         {fast_ranged_rows} ranged fast-path rows"
    );

    // The undersubscribed fast path still fires: every partition value at or
    // below the segment count declares no more partitions than there are
    // segments, so it stays on the whole-segment fast path (issue #739 dropped
    // the object-size conjunct, so this holds on both read shapes). The exact
    // per-segment counts above are what that fast path costs on each shape.
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
                     the fast path since #739, but the plan declared {} partitions",
                    c.scan_partitions
                );
            }
        }
    }

    // The sweep must actually exercise the fan-out past the segment count (the
    // capability item 1 adds): at least one cache-wired row declares more scan
    // partitions than there are segments. Its request counts are still exactly
    // the per-segment counts asserted above -- with a cache sized to hold the
    // dataset, the striped repeats coalesce at one key onto single-flight GETs --
    // and its > segment-count fan-out is pinned per row in the loop above.
    let fanned_out = report
        .combos
        .iter()
        .filter(|c| c.scan_partitions > segments)
        .count();
    assert!(
        fanned_out >= 1,
        "the sweep must contain a `target_partitions` value above the segment \
         count ({segments}) so the striped fan-out is exercised; got {fanned_out} \
         such rows"
    );
}
