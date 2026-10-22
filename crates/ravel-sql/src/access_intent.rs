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
//! pending erasure and every resolved segment lies inside the window
//! ([`classify_access_intent`]).
//!
//! With a bulk-scan byte threshold set (the server's
//! `--cache-bulk-scan-min-bytes`, ADR-3072's size trigger amendment and its
//! read-bytes amendment), a statement sent as SQL text is also BulkScan when it
//! names one `logs` table and no Parquet table, the same tenant's same
//! statement text read at least the threshold on its last successful
//! execution, and the plan the KEY_IDX probe takes its filters from has no
//! equality on a field the tenant indexes in KEY_IDX ([`size_trigger_applies`],
//! [`StatementReadBytes`], [`plan_has_key_equality`]). A statement with no
//! record of its own, its first execution included, is Interactive. Every
//! other statement is Interactive. Unset, the first rule decides alone.
//!
//! The threshold the server applies is the flag raised to the in-memory read
//! cache's `max_bytes` ([`effective_bulk_scan_min_bytes`]): the bulk region
//! keeps a stable subset of a loop larger than the cache, and a loop that fits
//! the cache is better served by plain recency.

use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use ravel_cache::{CacheMetrics, CacheMetricsSnapshot, IntentCounters, IntentMetricsSnapshot};
use ravel_catalog::Snapshot;
use ravel_logseg::key_index::TRACE_ID_FIELD;
use ravel_types::accounting::{AccessIntent, AccountedOp, QueryAccountingSnapshot};
use ravel_types::{TenantHash, TimeRange};

use crate::declared::KeyIndexDeclaration;
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
/// This is the rule without the size trigger; the executor reads an
/// Interactive result as BulkScan when the trigger applies
/// ([`size_trigger_applies`], [`plan_has_key_equality`]).
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

/// Whether a statement's lanes are ones the bulk-scan size trigger reads:
/// one `logs` table and no Parquet table. A `samples` statement never is, so
/// a dashboard query over a large metrics resolve stays Interactive.
pub(crate) fn size_trigger_lane(lanes: &[(TargetSignal, &Snapshot)], names_parquet: bool) -> bool {
    !names_parquet && matches!(lanes, [(TargetSignal::Logs, _)])
}

/// Whether the bulk-scan size trigger applies to a statement: `min_bytes` is
/// set, its lanes pass [`size_trigger_lane`], and `last_read_bytes`, the bytes
/// the same statement read on its last successful execution
/// ([`StatementReadBytes`]), is at least `min_bytes`. A statement with no
/// record (`None`) never does. The caller still requires SQL text and rules
/// out an exact-key lookup ([`plan_has_key_equality`]) before reading the
/// statement as BulkScan.
pub(crate) fn size_trigger_applies(
    lanes: &[(TargetSignal, &Snapshot)],
    names_parquet: bool,
    min_bytes: Option<u64>,
    last_read_bytes: Option<u64>,
) -> bool {
    let (Some(min_bytes), Some(read_bytes)) = (min_bytes, last_read_bytes) else {
        return false;
    };
    size_trigger_lane(lanes, names_parquet) && read_bytes >= min_bytes
}

/// The bulk-scan threshold a process applies: the configured `flag` raised to
/// the in-memory read cache's `max_bytes` when a cache is configured, the
/// flag as given when none is, and `None` when the flag is unset. The bulk
/// region only beats recency on a loop larger than the cache, so a threshold
/// below the cache would read as bulk the statements that fit it.
pub fn effective_bulk_scan_min_bytes(
    flag: Option<u64>,
    cache_max_bytes: Option<u64>,
) -> Option<u64> {
    flag.map(|flag| cache_max_bytes.map_or(flag, |cache| flag.max(cache)))
}

/// The bytes a statement's execution read, as the size trigger records them:
/// the bytes resident cache entries served, the bytes another caller's
/// in-flight read handed it, and its own GET bytes, every phase. However the
/// read cache served it, the same statement over the same data records the
/// same figure.
pub(crate) fn trigger_read_bytes(accounting: &QueryAccountingSnapshot) -> u64 {
    accounting
        .cache_bytes
        .saturating_add(accounting.late_served_bytes)
        .saturating_add(accounting.s3_bytes(AccountedOp::Get))
}

/// How many statements [`StatementReadBytes`] holds a record for.
pub(crate) const STATEMENT_READ_BYTES_CAPACITY: usize = 4096;

/// What bounds a statement's reads besides its text, as the read-bytes record
/// keys it: the window's duration (its end minus its start, so a relative
/// window re-sent with fresh bounds keeps its record), the row window's
/// duration when the window is also applied as a row filter, and the row cap.
/// The same text over a much shorter window, or under a row cap, reads far
/// less, so it gets a record of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ReadShape {
    window_ns: i64,
    row_window_ns: Option<i64>,
    max_rows: Option<usize>,
}

impl ReadShape {
    pub(crate) fn new(window: TimeRange, row_window: bool, max_rows: Option<usize>) -> Self {
        let window_ns = window.end_ns.saturating_sub(window.start_ns);
        ReadShape {
            window_ns,
            row_window_ns: row_window.then_some(window_ns),
            max_rows,
        }
    }
}

/// The bytes each statement read on its last successful execution, keyed by
/// [`StatementReadBytes::key`] over the tenant, the exact SQL text and the
/// request's [`ReadShape`]: cache served, late served and store GET bytes,
/// every phase. Held in process memory, bounded
/// at [`STATEMENT_READ_BYTES_CAPACITY`] entries with the least recently
/// recorded dropped first, and charged to no cache. A restart forgets it, so
/// every statement's first execution after one is Interactive.
pub(crate) struct StatementReadBytes {
    inner: Mutex<ReadBytesEntries>,
}

#[derive(Default)]
struct ReadBytesEntries {
    /// Key to (bytes read, recording sequence).
    by_key: HashMap<u64, (u64, u64)>,
    /// Recording sequence to key, oldest first.
    by_age: BTreeMap<u64, u64>,
    next_seq: u64,
}

impl StatementReadBytes {
    pub(crate) fn new() -> Self {
        StatementReadBytes {
            inner: Mutex::new(ReadBytesEntries::default()),
        }
    }

    /// The record key of `sql` sent by `tenant` with `shape`. Two statements
    /// whose keys collide share a record, which only moves a cache intent.
    pub(crate) fn key(tenant: TenantHash, sql: &str, shape: ReadShape) -> u64 {
        let mut hasher = DefaultHasher::new();
        tenant.0.hash(&mut hasher);
        sql.hash(&mut hasher);
        shape.hash(&mut hasher);
        hasher.finish()
    }

    /// The bytes the statement under `key` read on its last recorded
    /// execution.
    pub(crate) fn get(&self, key: u64) -> Option<u64> {
        let entries = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        entries.by_key.get(&key).map(|&(bytes, _)| bytes)
    }

    /// Record that the statement under `key` read `bytes`, replacing its
    /// previous record and dropping the least recently recorded statement
    /// when the record is full.
    pub(crate) fn record(&self, key: u64, bytes: u64) {
        let mut entries = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = entries.next_seq;
        entries.next_seq += 1;
        if let Some((_, old_seq)) = entries.by_key.insert(key, (bytes, seq)) {
            entries.by_age.remove(&old_seq);
        }
        entries.by_age.insert(seq, key);
        while entries.by_key.len() > STATEMENT_READ_BYTES_CAPACITY {
            let Some((_, oldest)) = entries.by_age.pop_first() else {
                break;
            };
            entries.by_key.remove(&oldest);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let entries = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        entries.by_key.len()
    }
}

/// Whether `plan`, subqueries included, holds an `=` between a field
/// `key_index` names (a declared column in its `columns`, or `trace_id` when
/// its `trace_id` is set) and anything that is not a column, in a `Filter`
/// predicate, a filter pushed into a `TableScan`, or a `Join`'s filter. A
/// `Join::on` pair has a column on both sides, so it is never a key equality.
/// This is wider than the KEY_IDX probe's own test (any
/// position in the predicate, a cast column, a literal of any type), so a
/// statement the probe could answer is never read as a bulk scan, provided
/// `plan` is the optimized plan whose `TableScan` filters the probe reads.
pub(crate) fn plan_has_key_equality(plan: &LogicalPlan, key_index: &KeyIndexDeclaration) -> bool {
    if key_index.columns.is_empty() && !key_index.trace_id {
        return false;
    }
    let indexed = |name: &str| {
        if name == TRACE_ID_FIELD {
            key_index.trace_id
        } else {
            key_index.columns.iter().any(|c| c == name)
        }
    };
    let mut found = false;
    // Never errors: the closures only inspect and return a recursion verb.
    let _ = plan.apply_with_subqueries(|node| {
        let predicates: Vec<Expr> = match node {
            LogicalPlan::Filter(filter) => vec![filter.predicate.clone()],
            LogicalPlan::TableScan(scan) => scan.filters.clone(),
            LogicalPlan::Join(join) => join.filter.iter().cloned().collect(),
            _ => Vec::new(),
        };
        for predicate in &predicates {
            let _ = predicate.apply(|expr| {
                if let Expr::BinaryExpr(be) = expr
                    && key_equality_field(be).is_some_and(&indexed)
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// The field name of an `=` with a column, bare or cast, on one side and a
/// non-column on the other.
fn key_equality_field(be: &BinaryExpr) -> Option<&str> {
    if be.op != Operator::Eq {
        return None;
    }
    fn column(e: &Expr) -> Option<&str> {
        match e {
            Expr::Column(c) => Some(&c.name),
            Expr::Cast(cast) => column(&cast.expr),
            Expr::TryCast(cast) => column(&cast.expr),
            _ => None,
        }
    }
    match (column(&be.left), column(&be.right)) {
        (Some(name), None) | (None, Some(name)) => Some(name),
        _ => None,
    }
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

    /// The trigger compares the statement's own last read bytes, never the
    /// lane's resolve: over a 2-byte resolve a 3-byte read clears a 3-byte
    /// threshold, and a 2-byte read under a 3-byte threshold does not.
    #[test]
    fn the_size_trigger_applies_to_one_logs_lane_whose_last_read_reached_the_threshold() {
        // Two segments of one byte each.
        let snap = two_hours();
        let lane = [(TargetSignal::Logs, &snap)];
        assert!(size_trigger_applies(&lane, false, Some(3), Some(3)));
        assert!(size_trigger_applies(&lane, false, Some(2), Some(5)));
        assert!(!size_trigger_applies(&lane, false, Some(3), Some(2)));
        assert!(!size_trigger_applies(&lane, false, Some(1), None));
        assert!(!size_trigger_applies(&lane, false, None, Some(5)));
        assert!(!size_trigger_applies(&lane, true, Some(1), Some(5)));
        assert!(size_trigger_lane(&lane, false));
        assert!(!size_trigger_lane(&lane, true));
        for signal in [
            TargetSignal::Metrics,
            TargetSignal::Spans,
            TargetSignal::Alerts,
            TargetSignal::Audit,
            TargetSignal::Parquet,
        ] {
            assert!(
                !size_trigger_applies(&[(signal, &snap)], false, Some(1), Some(5)),
                "{signal:?}"
            );
            assert!(!size_trigger_lane(&[(signal, &snap)], false), "{signal:?}");
        }
        let two_lanes = [(TargetSignal::Metrics, &snap), (TargetSignal::Logs, &snap)];
        assert!(!size_trigger_applies(&two_lanes, false, Some(1), Some(5)));
        assert!(!size_trigger_lane(&two_lanes, false));
    }

    /// The flag raised to the cache's size when a cache is configured, as given
    /// when none is, and unset when the flag is.
    #[test]
    fn the_effective_bulk_scan_threshold_is_the_flag_floored_at_the_cache_size() {
        let cache = 6_184_752_906;
        assert_eq!(
            effective_bulk_scan_min_bytes(Some(1_000), Some(cache)),
            Some(cache),
            "a flag below the cache"
        );
        assert_eq!(
            effective_bulk_scan_min_bytes(Some(cache + 1), Some(cache)),
            Some(cache + 1),
            "a flag above the cache"
        );
        assert_eq!(
            effective_bulk_scan_min_bytes(Some(cache), Some(cache)),
            Some(cache),
            "a flag equal to the cache"
        );
        assert_eq!(
            effective_bulk_scan_min_bytes(Some(1_000), None),
            Some(1_000),
            "no cache"
        );
        assert_eq!(effective_bulk_scan_min_bytes(None, Some(cache)), None);
        assert_eq!(effective_bulk_scan_min_bytes(None, None), None);
    }

    /// The record holds 4,096 statements: the 4,097th distinct one drops the
    /// least recently recorded, and re-recording a statement makes it the most
    /// recent, so the next overflow drops another. A key names the tenant and
    /// the read shape as well as the text.
    #[test]
    fn the_read_bytes_record_drops_the_oldest_statement_past_its_bound() {
        let tenant = TenantHash([7u8; 16]);
        let shape = ReadShape::new(whole_tenant(), false, None);
        let key =
            |i: usize| StatementReadBytes::key(tenant, &format!("SELECT {i} FROM logs"), shape);
        let record = StatementReadBytes::new();
        for i in 0..STATEMENT_READ_BYTES_CAPACITY {
            record.record(key(i), i as u64);
        }
        assert_eq!(record.len(), 4_096);
        assert_eq!(record.get(key(0)), Some(0));

        record.record(key(4_096), 4_096);
        assert_eq!(record.len(), 4_096, "the 4,097th statement");
        assert_eq!(record.get(key(0)), None, "the oldest is dropped");
        assert_eq!(record.get(key(1)), Some(1));
        assert_eq!(record.get(key(4_096)), Some(4_096));

        record.record(key(1), 11);
        record.record(key(4_097), 4_097);
        assert_eq!(record.len(), 4_096);
        assert_eq!(record.get(key(1)), Some(11), "re-recorded, so kept");
        assert_eq!(record.get(key(2)), None, "now the oldest, so dropped");

        let other = StatementReadBytes::key(TenantHash([8u8; 16]), "SELECT 1 FROM logs", shape);
        assert_ne!(other, key(1));
        assert_eq!(record.get(other), None);
    }

    /// The read shape keys a record by the window's duration, not its bounds:
    /// the same window moved later shares a key, while a shorter window, the
    /// same window applied as a row filter, and a row cap each get their own.
    #[test]
    fn the_read_bytes_key_names_the_window_duration_row_window_and_row_cap() {
        let tenant = TenantHash([7u8; 16]);
        let sql = "SELECT * FROM logs";
        let hour = TimeRange {
            start_ns: 0,
            end_ns: 3_600_000_000_000,
        };
        let key = |window: TimeRange, row_window: bool, max_rows: Option<usize>| {
            StatementReadBytes::key(tenant, sql, ReadShape::new(window, row_window, max_rows))
        };
        let base = key(hour, false, None);
        let later_hour = TimeRange {
            start_ns: hour.start_ns + 7_200_000_000_000,
            end_ns: hour.end_ns + 7_200_000_000_000,
        };
        assert_eq!(
            key(later_hour, false, None),
            base,
            "same duration, later bounds"
        );
        let minute = TimeRange {
            start_ns: hour.end_ns - 60_000_000_000,
            end_ns: hour.end_ns,
        };
        assert_ne!(key(minute, false, None), base, "a shorter window");
        assert_ne!(key(hour, true, None), base, "the window as a row filter");
        assert_ne!(key(hour, false, Some(100)), base, "a row cap");
        assert_ne!(key(hour, false, Some(100)), key(hour, false, Some(1_000)));
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
