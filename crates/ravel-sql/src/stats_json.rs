//! JSON rendering of a query's per-phase accounting and I/O shape (issue
//! #1367), for the `/api/v1/sql` response's `stats` object.
//!
//! Field names and phase ordering match `ravel_query::http::json`'s PromQL
//! `stats.phases`/`stats.io` rendering exactly, so a client reading both
//! surfaces sees one convention. That module's own phase-cost builder is
//! private to it, so the shape is rebuilt here from the same public
//! `QueryPhase`/`PhaseAccountingSnapshot`/`QueryIoShape` types rather than
//! reused; see this crate's `Refs: #1367` commit for that trade-off.
//!
//! # Which bytes each byte figure counts
//!
//! Every byte field is a WIRE byte figure -- the byte length of each
//! successful GET response body, retries excluded (a retried GET's failed
//! attempts never reach this counter; `object_store` retries beneath
//! `InstrumentedStore`, so a GET that retried counts once here) and range
//! reads included (a range GET's counter is the bytes the range itself
//! returned) -- with two named exceptions: `cacheServedBytes` never crossed
//! the wire (served from the in-process cache) and `decompressedOutputBytes`
//! is a decode-time output size no request paid for. The three are never
//! comparable or summable with each other.

use std::time::Duration;

use ravel_query::io_shape::QueryIoShape;
use ravel_query::{PhaseAccountingSnapshot, QueryPhase};
use ravel_types::accounting::{AccessIntent, AccountedOp};
use serde_json::{Value as Json, json};

use crate::{IntentCacheCounts, SignalStats, SqlStats, StatementCacheStats};

/// One entry per [`QueryPhase`], in [`QueryPhase::ALL`] order, each phase
/// exactly once: a phase added to the enum cannot be silently dropped from
/// this response.
pub fn phase_costs_json(snapshot: &PhaseAccountingSnapshot) -> Vec<Json> {
    QueryPhase::ALL
        .iter()
        .map(|phase| {
            let s = snapshot.phase(*phase);
            json!({
                "phase": phase.name(),
                "s3GetRequests": s.s3_requests(AccountedOp::Get),
                "s3GetWireBytes": s.s3_bytes(AccountedOp::Get),
                "s3ListRequests": s.s3_requests(AccountedOp::List),
                "cacheHits": s.cache_hits,
                "cacheMisses": s.cache_misses,
                "cacheServedBytes": s.cache_bytes,
                "decompressedOutputBytes": s.decompressed_bytes,
                "reusedRegionBytes": s.bytes_reused,
                "segmentsOpened": s.segments_opened,
                "seriesMatched": s.series_matched,
            })
        })
        .collect()
}

/// This query's I/O dependency shape, rendered under `stats.io`. See
/// `ravel_query::io_shape`'s module docs for what each figure means.
pub fn io_shape_json(shape: &QueryIoShape) -> Json {
    json!({
        "dependencyDepth": shape.dependency_depth,
        "listPageDepth": shape.list_page_depth,
        "serviceBatches": shape.service_batches,
        "unfoldedSegmentsResolved": shape.unfolded_segments_resolved,
        "unfoldedRecordsServedFromCache": shape.unfolded_records_served_from_cache,
        "planClass": shape.plan_class.name(),
    })
}

/// Nanoseconds as fractional milliseconds.
fn ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

/// The statement's wall time per stage, rendered under `stats.timings`
/// (ADR-2677 decision 4). These are wall-clock stages, not the request
/// accounting buckets `stats.phases` names. The five executor stages run one
/// after another; the scan figures overlap them, measured inside a scan
/// created during `startMs` and drained during `drainMs`. `audit` is the caller's wait on its audit submission,
/// which runs outside the executor.
///
/// Only the scan figures that hold across partitions are rendered: the
/// per-partition extremes and the barrier cost, never a sum over partitions
/// (their intervals overlap) and never the per-segment rows. `scans` is the
/// number of logs scans in the plan. `planInitMs`, `firstBatchMinMs` and
/// `streamMaxMs` are rendered only when it is 1: each scan times from its own
/// creation and pays its own barrier, so with two scans the offsets mix
/// origins and the barrier is a sum, and with none there is nothing to time.
pub fn timings_json(stats: &SqlStats, audit: Duration) -> Json {
    let wall = &stats.wall;
    let scan = &stats.scan_timing;
    let mut rendered = json!({
        "attempts": stats.attempts,
        "resolveMs": ms(wall.resolve_ns),
        "planMs": ms(wall.plan_ns),
        "startMs": ms(wall.start_ns),
        "firstBatchMs": ms(wall.first_batch_ns),
        "drainMs": ms(wall.drain_ns),
        "auditMs": audit.as_secs_f64() * 1_000.0,
        "scans": scan.scans,
        "planningWaitMaxMs": ms(scan.planning_wait_elapsed_max_ns),
        "openMaxMs": ms(scan.open_elapsed_max_ns),
        "decodeBuildMaxMs": ms(scan.decode_build_elapsed_max_ns),
    });
    if scan.scans == 1
        && let Some(fields) = rendered.as_object_mut()
    {
        fields.insert("planInitMs".into(), ms(scan.plan_init_elapsed_ns).into());
        fields.insert(
            "firstBatchMinMs".into(),
            ms(scan.first_batch_elapsed_min_ns).into(),
        );
        fields.insert("streamMaxMs".into(), ms(scan.stream_elapsed_max_ns).into());
    }
    rendered
}

/// The successful attempt's object- and block-level pruning counts, rendered
/// under `stats.pruning`. See [`SqlStats`] for what each counter means.
pub fn pruning_json(stats: &SqlStats) -> Json {
    json!({
        "segments": stats.segments,
        "segmentsPrunedByStats": stats.segments_pruned_by_stats,
        "segmentsPrunedByKeyIndex": stats.segments_pruned_by_key_index,
        "blocksPrunedByKeyIndex": stats.blocks_pruned_by_key_index,
        "blocksTotal": stats.blocks_total,
        "blocksScanned": stats.blocks_scanned,
        "blocksPrunedByPostings": stats.blocks_pruned_by_postings,
        "blocksSkippedByThreshold": stats.blocks_skipped_by_threshold,
        "segmentsSkippedByThreshold": stats.segments_skipped_by_threshold,
        "blocksPredicateDropped": stats.blocks_predicate_dropped,
        "rowsPredicateEvaluated": stats.rows_predicate_evaluated,
    })
}

/// One entry per signal table the statement read, in the order given
/// (ADR-2904 decision 7), rendered under `stats.signals`. Each entry's
/// `phases` has the shape of `stats.phases`, so for every phase and field the
/// entries plus the Parquet tables' share sum to `stats.phases`.
pub fn signals_json(signals: &[SignalStats]) -> Vec<Json> {
    signals
        .iter()
        .map(|signal| {
            json!({
                "table": signal.table,
                "segments": signal.segments,
                "segmentsPruned": signal.segments_pruned,
                "phases": phase_costs_json(&signal.phases),
            })
        })
        .collect()
}

/// The segment cache's movement over the statement's run, rendered under
/// `stats.cache`: the intent the statement read under, hits and served bytes
/// per intent, and cache-wide misses and admitted bytes. Every figure is the
/// difference of the process-global RAM-tier counters before and after the
/// statement, so statements running at the same time move each other's
/// figures (see [`StatementCacheStats`]).
pub fn cache_json(cache: &StatementCacheStats) -> Json {
    let counts = |counts: &IntentCacheCounts| {
        json!({
            "hits": counts.hits,
            "servedBytes": counts.served_bytes,
        })
    };
    json!({
        "intent": intent_name(cache.intent),
        "interactive": counts(&cache.interactive),
        "bulkScan": counts(&cache.bulk_scan),
        "maintenance": counts(&cache.maintenance),
        "misses": cache.misses,
        "admittedBytes": cache.admitted_bytes,
    })
}

/// An [`AccessIntent`]'s name in `stats.cache`, the same spelling as its
/// per-intent key there.
fn intent_name(intent: AccessIntent) -> &'static str {
    match intent {
        AccessIntent::Interactive => "interactive",
        AccessIntent::BulkScan => "bulkScan",
        AccessIntent::Maintenance => "maintenance",
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_query::PhaseAccounting;
    use ravel_query::io_shape::PlanClass;

    use super::*;

    /// Every phase renders exactly once, in
    /// `resolve`/`plan`/`probe`/`scan`/`keyIndex` order, with no phase missing
    /// and none duplicated: a phase emitted twice or missing here fails a
    /// client's per-phase sum the same way a wrong number does.
    #[test]
    fn phase_costs_json_names_every_phase_exactly_once() {
        let phases = PhaseAccounting::new();
        let snapshot = phases.snapshot();
        let entries = phase_costs_json(&snapshot);
        let names: Vec<&str> = entries
            .iter()
            .map(|e| e["phase"].as_str().expect("phase is a string"))
            .collect();
        assert_eq!(names, vec!["resolve", "plan", "probe", "scan", "keyIndex"]);
    }

    /// A query's per-phase requests and bytes sum back to the pooled total
    /// `QueryAccounting` (and thus `stats.accounting`) reports, by exact
    /// value: this is the assertion the summing-demonstration test in
    /// `executor.rs` also pins.
    #[test]
    fn phase_costs_json_sums_to_the_pooled_total() {
        let phases = PhaseAccounting::new();
        for (phase, bytes) in [
            (phases.resolve(), 100u64),
            (phases.plan(), 10),
            (phases.probe(), 1),
            (phases.scan(), 1000),
            (phases.key_index(), 10_000),
        ] {
            phase.record_s3_request(AccountedOp::Get);
            phase.add_s3_bytes(AccountedOp::Get, bytes);
        }

        let snapshot = phases.snapshot();
        let entries = phase_costs_json(&snapshot);
        let total_bytes: u64 = entries
            .iter()
            .map(|e| e["s3GetWireBytes"].as_u64().expect("u64"))
            .sum();
        let total_requests: u64 = entries
            .iter()
            .map(|e| e["s3GetRequests"].as_u64().expect("u64"))
            .sum();
        assert_eq!(
            total_bytes, 11_111,
            "100 + 10 + 1 + 1000 + 10000 resolve/plan/probe/scan/keyIndex"
        );
        assert_eq!(total_requests, 5, "one GET recorded per phase");
        assert_eq!(total_bytes, snapshot.pooled().total_s3_bytes());
        assert_eq!(total_requests, snapshot.pooled().total_s3_requests());
    }

    /// `cache_json`'s key set is exactly the six keys below, each per-intent
    /// object exactly `hits` and `servedBytes`, and every figure lands under
    /// its own key: exact set equality, so an added or dropped key fails.
    #[test]
    fn cache_json_names_every_field_exactly_once() {
        let cache = StatementCacheStats {
            intent: AccessIntent::BulkScan,
            interactive: IntentCacheCounts {
                hits: 1,
                served_bytes: 2,
            },
            bulk_scan: IntentCacheCounts {
                hits: 3,
                served_bytes: 4,
            },
            maintenance: IntentCacheCounts {
                hits: 5,
                served_bytes: 6,
            },
            misses: 7,
            admitted_bytes: 8,
        };
        let rendered = cache_json(&cache);
        let object = rendered.as_object().expect("cache renders an object");
        let keys: std::collections::BTreeSet<&str> = object.keys().map(String::as_str).collect();
        let expected: std::collections::BTreeSet<&str> = [
            "intent",
            "interactive",
            "bulkScan",
            "maintenance",
            "misses",
            "admittedBytes",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
        for (key, hits, served) in [
            ("interactive", 1, 2),
            ("bulkScan", 3, 4),
            ("maintenance", 5, 6),
        ] {
            let counts = rendered[key].as_object().expect("per-intent object");
            let counts_keys: std::collections::BTreeSet<&str> =
                counts.keys().map(String::as_str).collect();
            assert_eq!(
                counts_keys,
                ["hits", "servedBytes"].into_iter().collect(),
                "{key}"
            );
            assert_eq!(rendered[key]["hits"], hits, "{key}");
            assert_eq!(rendered[key]["servedBytes"], served, "{key}");
        }
        assert_eq!(rendered["intent"], "bulkScan");
        assert_eq!(rendered["misses"], 7);
        assert_eq!(rendered["admittedBytes"], 8);
    }

    /// `phase_costs_json`'s per-phase key set is exactly the eleven fields
    /// below, no more and no fewer, matching `ravel_query::http::json`'s
    /// PromQL `stats.phases` rendering (`http/json.rs`'s `PhaseCostJson`)
    /// exactly: a renamed, added, or dropped key here would otherwise ship
    /// silently, since the two renderings are independently maintained.
    /// Exact set equality, not a per-key `contains`, so a stray extra key
    /// fails this the same way a missing one does.
    #[test]
    fn phase_costs_json_names_every_field_exactly_once() {
        let phases = PhaseAccounting::new();
        let snapshot = phases.snapshot();
        let entries = phase_costs_json(&snapshot);
        let expected: std::collections::BTreeSet<&str> = [
            "phase",
            "s3GetRequests",
            "s3GetWireBytes",
            "s3ListRequests",
            "cacheHits",
            "cacheMisses",
            "cacheServedBytes",
            "decompressedOutputBytes",
            "reusedRegionBytes",
            "segmentsOpened",
            "seriesMatched",
        ]
        .into_iter()
        .collect();
        for entry in &entries {
            let object = entry.as_object().expect("phase entry renders an object");
            let keys: std::collections::BTreeSet<&str> =
                object.keys().map(String::as_str).collect();
            assert_eq!(keys, expected);
        }
    }

    /// `signals_json` renders one entry per record, in the given order, each
    /// with exactly the four keys below, and its `phases` exactly as
    /// `phase_costs_json` renders the same snapshot.
    #[test]
    fn signals_json_names_every_field_exactly_once() {
        let samples = PhaseAccounting::new();
        samples.resolve().record_s3_request(AccountedOp::List);
        let logs = PhaseAccounting::new();
        logs.scan().record_s3_request(AccountedOp::Get);
        let signals = [
            SignalStats {
                table: "samples",
                segments: 3,
                segments_pruned: 2,
                phases: samples.snapshot(),
            },
            SignalStats {
                table: "logs",
                segments: 1,
                segments_pruned: 0,
                phases: logs.snapshot(),
            },
        ];
        let entries = signals_json(&signals);
        let expected: std::collections::BTreeSet<&str> =
            ["table", "segments", "segmentsPruned", "phases"]
                .into_iter()
                .collect();
        assert_eq!(entries.len(), 2);
        for (entry, signal) in entries.iter().zip(&signals) {
            let object = entry.as_object().expect("signal entry renders an object");
            let keys: std::collections::BTreeSet<&str> =
                object.keys().map(String::as_str).collect();
            assert_eq!(keys, expected);
            assert_eq!(entry["table"], signal.table);
            assert_eq!(entry["segments"], signal.segments);
            assert_eq!(entry["segmentsPruned"], signal.segments_pruned);
            assert_eq!(
                entry["phases"],
                Json::Array(phase_costs_json(&signal.phases))
            );
        }
        assert_eq!(entries[0]["phases"][0]["s3ListRequests"], 1);
        assert_eq!(entries[1]["phases"][3]["s3GetRequests"], 1);
    }

    /// `io_shape_json`'s key set is exactly the six fields below, no more and
    /// no fewer: a renamed or newly-added `QueryIoShape` field that isn't
    /// wired into this renderer would otherwise ship silently, since nothing
    /// else in this crate reads `stats.io` back. Exact set equality, not a
    /// per-key `contains`, so a stray extra key fails this the same way a
    /// missing one does.
    #[test]
    fn io_shape_json_names_every_field_exactly_once() {
        let shape = QueryIoShape {
            dependency_depth: 1,
            list_page_depth: 2,
            service_batches: 3,
            unfolded_segments_resolved: 4,
            unfolded_records_served_from_cache: 5,
            plan_class: PlanClass::SelectiveIndexed,
        };
        let rendered = io_shape_json(&shape);
        let object = rendered
            .as_object()
            .expect("io_shape_json renders an object");
        let keys: std::collections::BTreeSet<&str> = object.keys().map(String::as_str).collect();
        let expected: std::collections::BTreeSet<&str> = [
            "dependencyDepth",
            "listPageDepth",
            "serviceBatches",
            "unfoldedSegmentsResolved",
            "unfoldedRecordsServedFromCache",
            "planClass",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
    }

    fn keys(rendered: &Json) -> std::collections::BTreeSet<&str> {
        rendered
            .as_object()
            .expect("renders an object")
            .keys()
            .map(String::as_str)
            .collect()
    }

    /// A stats value whose every rendered source field holds a distinct
    /// value, so a field wired to the wrong source shows up as a wrong
    /// number below.
    fn distinct_stats() -> SqlStats {
        let mut stats = SqlStats {
            attempts: 2,
            segments: 7,
            segments_pruned_by_stats: 3,
            segments_pruned_by_key_index: 23,
            blocks_pruned_by_key_index: 29,
            blocks_total: 40,
            blocks_scanned: 11,
            blocks_pruned_by_postings: 5,
            blocks_skipped_by_threshold: 13,
            segments_skipped_by_threshold: 2,
            blocks_predicate_dropped: 17,
            rows_predicate_evaluated: 19,
            ..SqlStats::default()
        };
        stats.wall = crate::PhaseWallTiming {
            resolve_ns: 1_000_000,
            plan_ns: 2_000_000,
            start_ns: 3_000_000,
            first_batch_ns: 4_000_000,
            drain_ns: 5_000_000,
        };
        let scan = &mut stats.scan_timing;
        scan.plan_init_elapsed_ns = 6_000_000;
        scan.planning_wait_elapsed_max_ns = 7_000_000;
        scan.open_elapsed_ns = 900_000_000;
        scan.open_elapsed_max_ns = 8_000_000;
        scan.decode_build_elapsed_ns = 910_000_000;
        scan.decode_build_elapsed_max_ns = 9_000_000;
        scan.first_batch_elapsed_min_ns = 10_000_000;
        scan.stream_elapsed_max_ns = 11_000_000;
        scan.scans = 1;
        stats
    }

    /// The three fields that have one origin only under a single logs scan.
    const SINGLE_SCAN_KEYS: [&str; 3] = ["planInitMs", "firstBatchMinMs", "streamMaxMs"];

    /// `timings_json`'s key set is exactly the fields below for each scan
    /// count, and each reads its own source: the scan figures are the
    /// per-partition maxima and minimum, never the overlapping sums the
    /// fixture sets far larger. `planInitMs`, `firstBatchMinMs` and
    /// `streamMaxMs` appear only at `scans == 1`. Exact set equality, so a
    /// stray key fails like a missing one.
    #[test]
    fn timings_json_names_every_field_exactly_once() {
        let always: std::collections::BTreeSet<&str> = [
            "attempts",
            "resolveMs",
            "planMs",
            "startMs",
            "firstBatchMs",
            "drainMs",
            "auditMs",
            "scans",
            "planningWaitMaxMs",
            "openMaxMs",
            "decodeBuildMaxMs",
        ]
        .into_iter()
        .collect();
        for scans in [0u64, 1, 2] {
            let mut stats = distinct_stats();
            stats.scan_timing.scans = scans;
            let rendered = timings_json(&stats, Duration::from_millis(12));
            let mut expected = always.clone();
            if scans == 1 {
                expected.extend(SINGLE_SCAN_KEYS);
            }
            assert_eq!(keys(&rendered), expected, "scans = {scans}");
            assert_eq!(rendered["attempts"], 2);
            assert_eq!(rendered["scans"].as_u64(), Some(scans));
            for (key, value) in [
                ("resolveMs", 1.0),
                ("planMs", 2.0),
                ("startMs", 3.0),
                ("firstBatchMs", 4.0),
                ("drainMs", 5.0),
                ("planningWaitMaxMs", 7.0),
                ("openMaxMs", 8.0),
                ("decodeBuildMaxMs", 9.0),
                ("auditMs", 12.0),
            ] {
                assert_eq!(rendered[key].as_f64(), Some(value), "{key}: {rendered}");
            }
            if scans == 1 {
                for (key, value) in [
                    ("planInitMs", 6.0),
                    ("firstBatchMinMs", 10.0),
                    ("streamMaxMs", 11.0),
                ] {
                    assert_eq!(rendered[key].as_f64(), Some(value), "{key}: {rendered}");
                }
            }
        }
    }

    /// `pruning_json`'s key set is exactly the eleven counters below, each
    /// carrying its own `SqlStats` field.
    #[test]
    fn pruning_json_names_every_field_exactly_once() {
        let rendered = pruning_json(&distinct_stats());
        let expected: std::collections::BTreeSet<&str> = [
            "segments",
            "segmentsPrunedByStats",
            "segmentsPrunedByKeyIndex",
            "blocksPrunedByKeyIndex",
            "blocksTotal",
            "blocksScanned",
            "blocksPrunedByPostings",
            "blocksSkippedByThreshold",
            "segmentsSkippedByThreshold",
            "blocksPredicateDropped",
            "rowsPredicateEvaluated",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys(&rendered), expected);
        for (key, value) in [
            ("segments", 7),
            ("segmentsPrunedByStats", 3),
            ("segmentsPrunedByKeyIndex", 23),
            ("blocksPrunedByKeyIndex", 29),
            ("blocksTotal", 40),
            ("blocksScanned", 11),
            ("blocksPrunedByPostings", 5),
            ("blocksSkippedByThreshold", 13),
            ("segmentsSkippedByThreshold", 2),
            ("blocksPredicateDropped", 17),
            ("rowsPredicateEvaluated", 19),
        ] {
            assert_eq!(rendered[key].as_u64(), Some(value), "{key}: {rendered}");
        }
    }
}
