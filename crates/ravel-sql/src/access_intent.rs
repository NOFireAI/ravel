//! The read-cache intent one SQL statement's reads carry (ADR-3072 decision
//! 3a). The executor classifies once per statement attempt, after resolve and
//! before the first segment read, and tags every accounting handle with the
//! result, so every cache funnel the statement reaches reads and admits under
//! it.
//!
//! A statement is BulkScan only when it names one signal table and no Parquet
//! table, its plan is predicate-free with no `Join` or `Limit`, and its window
//! runs from the epoch to now; for `samples` also no segment is pruned and
//! every segment lies inside the window, and for `logs` also there is no
//! pending erasure and every resolved segment lies inside the window. Every
//! other statement is Interactive ([`classify_access_intent`]).

use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::LogicalPlan;
use ravel_cache::{CacheMetrics, CacheMetricsSnapshot, IntentCounters, IntentMetricsSnapshot};
use ravel_catalog::Snapshot;
use ravel_types::TimeRange;
use ravel_types::accounting::AccessIntent;

use crate::executor::TargetSignal;

/// The intent a resolved statement's reads carry.
///
/// [`AccessIntent::BulkScan`] only when all of these hold:
///
/// - the statement names exactly one signal table, `samples` or `logs`, and
///   no Parquet table;
/// - its plan is predicate-free (`predicate_free`, see
///   [`plan_is_predicate_free`]): no `WHERE`, no `HAVING`, no predicate in a
///   CTE or subquery, no `Join` and no `Limit`;
/// - its window reaches from the Unix epoch to at least `now_ns`
///   ([`window_covers_all_history`]), so no segment can sit wholly outside it
///   unseen by the resolve;
/// - for `samples`, the resolve pruned no segment (`segments_pruned == 0`)
///   and every segment lies inside the window
///   ([`window_contains_every_segment`]);
/// - for `logs`, the snapshot has no pending erasure and every resolved
///   segment lies inside the window.
///
/// [`AccessIntent::Interactive`] otherwise: several signal tables, a Parquet
/// table, a `spans`, `alerts` or `audit` lane, any predicate or row limit, a
/// window that starts after the epoch or ends before `now_ns`, and a segment
/// pruned or cut by the window. A window that starts after the tenant's
/// first data is Interactive even when it happens to hold most of the
/// tenant: resolve lists only the window's hours, so it cannot tell such a
/// window from a one-hour dashboard query. A distributed fragment never
/// reaches this function and stays Interactive.
///
/// `names_parquet` is whether the statement names a Parquet table: those
/// resolve outside `lanes`, so a `samples` join with one would otherwise
/// read as a single lane.
pub(crate) fn classify_access_intent(
    lanes: &[(TargetSignal, &Snapshot)],
    names_parquet: bool,
    window: TimeRange,
    now_ns: i64,
    predicate_free: bool,
) -> AccessIntent {
    if names_parquet {
        return AccessIntent::Interactive;
    }
    classify_signal_lanes(lanes, window, now_ns, predicate_free)
}

/// [`classify_access_intent`] for a statement that names no Parquet table.
fn classify_signal_lanes(
    lanes: &[(TargetSignal, &Snapshot)],
    window: TimeRange,
    now_ns: i64,
    predicate_free: bool,
) -> AccessIntent {
    let [(signal, snapshot)] = lanes else {
        return AccessIntent::Interactive;
    };
    if reads_whole_lane(*signal, snapshot, window, now_ns) && predicate_free {
        AccessIntent::BulkScan
    } else {
        AccessIntent::Interactive
    }
}

/// The snapshot and window conjuncts of [`classify_access_intent`] for one
/// `signal` lane, without the plan's: whether a predicate-free statement
/// over `snapshot` reads every segment of the tenant whole.
pub(crate) fn reads_whole_lane(
    signal: TargetSignal,
    snapshot: &Snapshot,
    window: TimeRange,
    now_ns: i64,
) -> bool {
    let lane = match signal {
        TargetSignal::Metrics => snapshot.segments_pruned == 0,
        TargetSignal::Logs => snapshot.pending_erasure.is_empty(),
        TargetSignal::Spans
        | TargetSignal::Alerts
        | TargetSignal::Audit
        | TargetSignal::Parquet => false,
    };
    lane && window_covers_all_history(window, now_ns)
        && window_contains_every_segment(snapshot, window)
}

/// Whether the closed `window` reaches from the Unix epoch to at least
/// `now_ns`. Ingest refuses an event time more than its ingest lag behind
/// ingest time, so no segment holds an event before the epoch, and a segment
/// committed by `now_ns` holds none past it beyond the ingest future-skew
/// allowance, which [`window_contains_every_segment`] checks per segment.
pub(crate) fn window_covers_all_history(window: TimeRange, now_ns: i64) -> bool {
    window.start_ns <= 0 && window.end_ns >= now_ns
}

/// Whether every segment in `snapshot` has a well-formed event span lying
/// inside the closed `window`: the containment conjunct of the logs scan's
/// whole-segment test, so every block of every segment is read.
pub(crate) fn window_contains_every_segment(snapshot: &Snapshot, window: TimeRange) -> bool {
    snapshot.segments.iter().all(|seg| {
        seg.min_event_ts_ns <= seg.max_event_ts_ns
            && window.start_ns <= seg.min_event_ts_ns
            && seg.max_event_ts_ns <= window.end_ns
    })
}

/// Whether `plan` reads every row of its scans, subqueries included: no
/// `Filter` node, no filter pushed into a `TableScan`, no `Join` and no
/// `Limit`. A join's `ON` condition can be pushed into one side's scan by the
/// optimizer, and a limit stops a scan early, so a plan with either reads
/// less than the whole table even with no `WHERE`.
pub(crate) fn plan_is_predicate_free(plan: &LogicalPlan) -> bool {
    let mut free = true;
    // Never errors: the closure only inspects and returns a recursion verb.
    let _ = plan.apply_with_subqueries(|node| {
        let narrowed = match node {
            LogicalPlan::Filter(_) | LogicalPlan::Join(_) | LogicalPlan::Limit(_) => true,
            LogicalPlan::TableScan(scan) => !scan.filters.is_empty(),
            _ => false,
        };
        if narrowed {
            free = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    free
}

/// One intent's read-cache hits and bytes served over a statement's run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IntentCacheCounts {
    pub hits: u64,
    pub served_bytes: u64,
}

/// How far the read cache's RAM-tier counters moved while one statement ran,
/// with the intent the statement read under.
///
/// The counters are process-global: every statement running at the same time
/// moves them, so under concurrency a delta includes other statements' reads,
/// including reads under an intent this statement never used. Hits and bytes
/// served are split by intent; misses and admitted bytes are cache-wide,
/// because the cache does not split them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatementCacheStats {
    /// The intent [`classify_access_intent`] gave the statement.
    pub intent: AccessIntent,
    pub interactive: IntentCacheCounts,
    pub bulk_scan: IntentCacheCounts,
    pub maintenance: IntentCacheCounts,
    pub misses: u64,
    pub admitted_bytes: u64,
}

/// The read cache's counters at one instant, to subtract from a later read.
pub(crate) struct CacheCounterMark {
    metrics: Arc<CacheMetrics>,
    totals: CacheMetricsSnapshot,
    intents: IntentMetricsSnapshot,
}

impl CacheCounterMark {
    pub(crate) fn take(metrics: Arc<CacheMetrics>) -> Self {
        let totals = metrics.snapshot();
        let intents = metrics.intent_snapshot();
        CacheCounterMark {
            metrics,
            totals,
            intents,
        }
    }

    /// The counters' movement since [`Self::take`], for a statement read under
    /// `intent`.
    pub(crate) fn since(&self, intent: AccessIntent) -> StatementCacheStats {
        let totals = self.metrics.snapshot();
        let intents = self.metrics.intent_snapshot();
        let counts = |after: IntentCounters, before: IntentCounters| IntentCacheCounts {
            hits: after.hits.saturating_sub(before.hits),
            served_bytes: after.bytes_served.saturating_sub(before.bytes_served),
        };
        StatementCacheStats {
            intent,
            interactive: counts(intents.interactive, self.intents.interactive),
            bulk_scan: counts(intents.bulk_scan, self.intents.bulk_scan),
            maintenance: counts(intents.maintenance, self.intents.maintenance),
            misses: totals.misses.saturating_sub(self.totals.misses),
            admitted_bytes: totals
                .bytes_admitted
                .saturating_sub(self.totals.bytes_admitted),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_catalog::{SegmentLevel, SegmentRef};

    const HOUR: i64 = 3_600_000_000_000;
    const NOW: i64 = 100 * HOUR;

    fn segment(min: i64, max: i64) -> SegmentRef {
        SegmentRef {
            data_object_key: format!("seg-{min}"),
            object_size: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: 1,
            series_count: 1,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: uuid::Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: 0,
            declared_column_stats: Default::default(),
        }
    }

    fn snapshot(segments: Vec<SegmentRef>, segments_pruned: u64) -> Snapshot {
        Snapshot {
            segments,
            segments_pruned,
            pending_erasure: Vec::new(),
        }
    }

    fn whole_tenant() -> TimeRange {
        TimeRange {
            start_ns: 0,
            end_ns: NOW,
        }
    }

    fn two_hours() -> Snapshot {
        snapshot(
            vec![
                segment(97 * HOUR, 98 * HOUR - 1),
                segment(98 * HOUR, 99 * HOUR - 1),
            ],
            0,
        )
    }

    #[test]
    fn an_unpruned_predicate_free_metrics_lane_over_all_history_is_bulk() {
        let snap = two_hours();
        let metrics = [(TargetSignal::Metrics, &snap)];
        assert_eq!(
            classify_signal_lanes(&metrics, whole_tenant(), NOW, true),
            AccessIntent::BulkScan
        );
        // A predicate over the same window and segments (stats pruning would
        // then read only the newest ones) is Interactive.
        assert_eq!(
            classify_signal_lanes(&metrics, whole_tenant(), NOW, false),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn a_pruned_metrics_lane_is_interactive() {
        let snap = snapshot(two_hours().segments, 1);
        assert_eq!(
            classify_signal_lanes(&[(TargetSignal::Metrics, &snap)], whole_tenant(), NOW, true),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn an_unpruned_metrics_lane_over_a_window_after_the_epoch_is_interactive() {
        // Every resolved segment lies inside the window and nothing was pruned,
        // the one-hour dashboard shape: still Interactive.
        let snap = snapshot(vec![segment(98 * HOUR, 99 * HOUR - 1)], 0);
        let window = TimeRange {
            start_ns: 98 * HOUR,
            end_ns: NOW,
        };
        assert_eq!(
            classify_signal_lanes(&[(TargetSignal::Metrics, &snap)], window, NOW, true),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn a_window_ending_before_now_or_cutting_a_segment_is_interactive() {
        let snap = two_hours();
        let short = TimeRange {
            start_ns: 0,
            end_ns: NOW - 1,
        };
        assert_eq!(
            classify_signal_lanes(&[(TargetSignal::Metrics, &snap)], short, NOW, true),
            AccessIntent::Interactive
        );
        let future = snapshot(vec![segment(99 * HOUR, NOW + 1)], 0);
        assert_eq!(
            classify_signal_lanes(
                &[(TargetSignal::Metrics, &future)],
                whole_tenant(),
                NOW,
                true
            ),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn several_lanes_are_interactive() {
        let snap = two_hours();
        assert_eq!(
            classify_signal_lanes(
                &[(TargetSignal::Metrics, &snap), (TargetSignal::Logs, &snap)],
                whole_tenant(),
                NOW,
                true
            ),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn a_logs_lane_is_bulk_only_when_predicate_free_over_all_history() {
        let snap = snapshot(vec![segment(98 * HOUR, 99 * HOUR - 1)], 0);
        let logs = [(TargetSignal::Logs, &snap)];
        assert_eq!(
            classify_signal_lanes(&logs, whole_tenant(), NOW, true),
            AccessIntent::BulkScan
        );
        assert_eq!(
            classify_signal_lanes(&logs, whole_tenant(), NOW, false),
            AccessIntent::Interactive
        );
        // Every resolved segment inside a window that starts after the epoch:
        // the one-hour dashboard and alert-rule shape.
        let hour = TimeRange {
            start_ns: 98 * HOUR,
            end_ns: NOW,
        };
        assert_eq!(
            classify_signal_lanes(&logs, hour, NOW, true),
            AccessIntent::Interactive
        );
        let short = TimeRange {
            start_ns: 0,
            end_ns: 99 * HOUR,
        };
        assert_eq!(
            classify_signal_lanes(&logs, short, NOW, true),
            AccessIntent::Interactive
        );
        let cut = snapshot(vec![segment(98 * HOUR, NOW + 1)], 0);
        assert_eq!(
            classify_signal_lanes(&[(TargetSignal::Logs, &cut)], whole_tenant(), NOW, true),
            AccessIntent::Interactive
        );
    }

    #[test]
    fn a_cache_mark_reports_only_what_moved_after_it_per_intent() {
        let cache = ravel_cache::Cache::<String>::new(ravel_cache::CacheLimits::new(
            1 << 20,
            1024,
            1 << 16,
        ));
        let key = |n: u64| ravel_cache::CacheKey::new([0u8; 16], [1u8; 32], n * 100, 100);
        let value = bytes::Bytes::from(vec![7u8; 100]);
        cache.insert_with_intent(
            key(0),
            value.clone(),
            ravel_cache::AccessIntent::Interactive,
        );
        // Moved before the mark: not in the delta.
        assert!(
            cache
                .get_with_intent(&key(0), ravel_cache::AccessIntent::Interactive)
                .is_some()
        );
        let mark = CacheCounterMark::take(cache.metrics());
        cache.insert_with_intent(key(1), value.clone(), ravel_cache::AccessIntent::BulkScan);
        assert!(
            cache
                .get_with_intent(&key(1), ravel_cache::AccessIntent::BulkScan)
                .is_some()
        );
        assert!(
            cache
                .get_with_intent(&key(9), ravel_cache::AccessIntent::BulkScan)
                .is_none()
        );
        let delta = mark.since(AccessIntent::BulkScan);
        assert_eq!(
            delta,
            StatementCacheStats {
                intent: AccessIntent::BulkScan,
                interactive: IntentCacheCounts::default(),
                bulk_scan: IntentCacheCounts {
                    hits: 1,
                    served_bytes: 100,
                },
                maintenance: IntentCacheCounts::default(),
                misses: 1,
                admitted_bytes: 100,
            }
        );
    }

    #[test]
    fn other_lanes_are_interactive() {
        let snap = two_hours();
        for signal in [
            TargetSignal::Spans,
            TargetSignal::Alerts,
            TargetSignal::Audit,
            TargetSignal::Parquet,
        ] {
            assert_eq!(
                classify_signal_lanes(&[(signal, &snap)], whole_tenant(), NOW, true),
                AccessIntent::Interactive,
                "{signal:?}"
            );
        }
    }

    #[test]
    fn a_statement_naming_a_parquet_table_is_interactive() {
        let snap = two_hours();
        let lanes = [(TargetSignal::Metrics, &snap)];
        assert_eq!(
            classify_access_intent(&lanes, false, whole_tenant(), NOW, true),
            AccessIntent::BulkScan
        );
        assert_eq!(
            classify_access_intent(&lanes, true, whole_tenant(), NOW, true),
            AccessIntent::Interactive
        );
    }
}
