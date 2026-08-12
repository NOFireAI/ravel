//! Scan-time selective-erasure filtering (ADR-0064 decision 2, "Immediate
//! logical exclusion"; EJ-T3, issue #753).
//!
//! ADR-0064's visibility guarantee is that a query whose snapshot resolves
//! after an erasure request is durable can never return matching records, from
//! the store or from any cache tier. The resolver attaches every pending
//! erasure predicate to the resolved snapshot (EJ-T2); this module is the
//! downstream consumer: it filters matching series, samples, rows, and spans
//! out of already-materialized results, run **after fetch, after any cache
//! layer, before any result reaches the caller**. Because it operates on
//! decoded records rather than on the fetch, stale cached bytes of a
//! since-erased subject are excluded exactly as freshly-fetched bytes are --
//! that post-cache ordering is the whole point of doing exclusion here rather
//! than at fetch time.
//!
//! # Predicate semantics (ADR-0064 decision 1)
//!
//! An [`ErasurePredicate`] is a conjunction (logical AND) of exact-match
//! `key = value` matchers, optionally intersected with a half-open event-time
//! window `[window_start_ns, window_end_ns)`. A record matches when **every**
//! matcher is satisfied by the record's labels/attributes and, if a window is
//! present, the record's event time falls inside it. v1 is equality-only (no
//! regex): erasure is irreversible, so over-matching is unrecoverable.
//!
//! - **Metrics**: matchers test series labels; a windowless predicate erases
//!   every sample of a matching series, a windowed one erases only the
//!   in-window samples of a matching series (ADR-0064 decision 1: "every
//!   sample of every series whose labels satisfy the conjunction, intersected
//!   with the time range if present").
//! - **Logs**: matchers test a row's per-record attributes; the row's `ts_ns`
//!   is the event time.
//! - **Spans**: matchers test a span's merged attributes; the span's start
//!   timestamp is the event time.
//!
//! # Zero-as-unset window bounds
//!
//! Mirroring `ravel_commit::erasure`'s `ErasureRequest` validation, a window
//! bound of `0` means "unset on that side": `window_start_ns == 0` imposes no
//! lower bound, `window_end_ns == 0` imposes no upper bound, and both zero
//! means no event-time restriction at all. The retained interval is half-open
//! `[start, end)`.
//!
//! # Fail-safe on an empty matcher set
//!
//! A conjunction of zero matchers is logically "true" and would match every
//! record -- catastrophic for an irreversible erasure. `ravel_commit`'s
//! decoder already rejects an empty predicate, so a well-formed predicate
//! always carries at least one matcher; as defense in depth every match method
//! here treats an empty matcher set as matching **nothing**, never everything.

use ravel_catalog::Snapshot;
use ravel_logseg::{AttrValue, LogRecord};
use ravel_segment::SeriesEntry;
use ravel_types::{LabelSet, Sample};

use crate::fetcher::{FetchedHistogramSeries, FetchedSeries, FetchedSeriesSoa};

/// One pending erasure predicate to exclude at scan time (ADR-0064 decision
/// 1). A conjunction of exact `key = value` matchers plus an optional
/// half-open event-time window.
///
/// This is the scan-layer view of a resolved snapshot's pending erasure
/// requests. The resolver (EJ-T2) attaches the predicate set to every resolved
/// snapshot; the query engine and log fetcher convert those into this shape
/// and hand them to the `retain_*` functions below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasurePredicate {
    /// Conjunction of exact `key = value` matchers. Never empty for a
    /// well-formed predicate (see the module-level fail-safe note).
    matchers: Vec<(String, String)>,
    /// Half-open event-time window `[window_start_ns, window_end_ns)`, with
    /// `0` meaning "unset" on either side (see the module docs). Both zero
    /// means no time restriction.
    window_start_ns: i64,
    window_end_ns: i64,
}

impl ErasurePredicate {
    /// Builds a predicate from its matchers and optional event-time window.
    /// Pass `window_start_ns = 0` and/or `window_end_ns = 0` for an unset
    /// bound (see the module docs); both zero means no time restriction.
    pub fn new(matchers: Vec<(String, String)>, window_start_ns: i64, window_end_ns: i64) -> Self {
        ErasurePredicate {
            matchers,
            window_start_ns,
            window_end_ns,
        }
    }

    /// A windowless predicate: matches on labels/attributes alone, with no
    /// event-time restriction.
    pub fn windowless(matchers: Vec<(String, String)>) -> Self {
        Self::new(matchers, 0, 0)
    }

    /// Whether this predicate restricts by event time at all. `false` means it
    /// erases every matching record regardless of timestamp.
    pub fn has_window(&self) -> bool {
        self.window_start_ns != 0 || self.window_end_ns != 0
    }

    /// The conjunction's `(key, value)` matchers, in construction order. Read
    /// by the ADR-0071 distributed codec (`distrib::codec`) to carry a resolved
    /// snapshot's pending predicates in a slice's `FetchRequest`, so a worker
    /// applies the identical exclusion the local path would.
    pub fn matchers(&self) -> &[(String, String)] {
        &self.matchers
    }

    /// The half-open window's lower bound (`0` means unset; see the module
    /// docs). Read by the ADR-0071 distributed codec.
    pub fn window_start_ns(&self) -> i64 {
        self.window_start_ns
    }

    /// The half-open window's upper bound (`0` means unset; see the module
    /// docs). Read by the ADR-0071 distributed codec.
    pub fn window_end_ns(&self) -> i64 {
        self.window_end_ns
    }

    /// Whether `ts_ns` falls inside the half-open window `[start, end)`, with
    /// zero-as-unset bounds. Always `true` when the predicate is windowless.
    pub fn ts_in_window(&self, ts_ns: i64) -> bool {
        (self.window_start_ns == 0 || ts_ns >= self.window_start_ns)
            && (self.window_end_ns == 0 || ts_ns < self.window_end_ns)
    }

    /// Whether every matcher is satisfied by a caller-supplied label/attribute
    /// lookup. Returns `false` for an empty matcher set (fail-safe): a
    /// zero-matcher conjunction must never be treated as matching everything.
    fn matchers_satisfied<'a>(&self, mut lookup: impl FnMut(&str) -> Option<&'a str>) -> bool {
        if self.matchers.is_empty() {
            return false;
        }
        self.matchers
            .iter()
            .all(|(k, v)| lookup(k).is_some_and(|found| found == v.as_str()))
    }

    /// Whether this predicate's conjunction matches a metric series' labels.
    /// Time is not considered here (a windowed predicate still "matches" the
    /// series for label purposes; the caller decides per sample).
    pub fn matches_labels(&self, labels: &LabelSet) -> bool {
        self.matchers_satisfied(|k| labels.get(k))
    }

    /// Whether this predicate's conjunction matches a log row's per-record
    /// attributes. Only string-valued attributes can satisfy an (equality)
    /// matcher; a non-string attribute never matches a v1 predicate value.
    pub fn matches_log_attrs(&self, attrs: &[(String, AttrValue)]) -> bool {
        self.matchers_satisfied(|k| {
            attrs.iter().find_map(|(ak, av)| {
                if ak == k {
                    match av {
                        AttrValue::Str(s) => Some(s.as_str()),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        })
    }

    /// Whether this predicate's conjunction matches a set of decoded
    /// string-valued attributes (the shape a span's merged attributes take).
    /// The span query surface lives in `ravel-sql`; this method plus
    /// [`is_erased_span`] are the reusable core it calls per row.
    pub fn matches_str_attrs(&self, attrs: &[(String, String)]) -> bool {
        self.matchers_satisfied(|k| {
            attrs
                .iter()
                .find_map(|(ak, av)| (ak == k).then_some(av.as_str()))
        })
    }
}

/// Whether a span (or any string-attributed record) is erased by any predicate
/// in `predicates`, given its merged attributes and event timestamp. A span
/// matches when a predicate's conjunction matches its attributes and, if the
/// predicate is windowed, `event_ts_ns` falls in the window.
///
/// The span query surface is in `ravel-sql` (out of `ravel-query`'s reach);
/// that fetcher calls this per materialized span, after its store fetch and
/// before the row reaches the caller, exactly as the metric and log paths do
/// via the `retain_*` helpers below.
pub fn is_erased_span(
    attrs: &[(String, String)],
    event_ts_ns: i64,
    predicates: &[ErasurePredicate],
) -> bool {
    predicates
        .iter()
        .any(|p| p.matches_str_attrs(attrs) && (!p.has_window() || p.ts_in_window(event_ts_ns)))
}

/// The predicates whose conjunction matches `labels`. Split out so the three
/// metric result shapes share one label-match pass before deciding, per
/// shape, how to drop samples.
fn matching_predicates<'a>(
    labels: &LabelSet,
    predicates: &'a [ErasurePredicate],
) -> Vec<&'a ErasurePredicate> {
    predicates
        .iter()
        .filter(|p| p.matches_labels(labels))
        .collect()
}

/// Whether a sample at `ts_ns` survives the already-label-matched windowed
/// predicates in `matching`. A sample is erased if it falls in any matching
/// predicate's window.
fn sample_survives(ts_ns: i64, matching: &[&ErasurePredicate]) -> bool {
    !matching.iter().any(|p| p.ts_in_window(ts_ns))
}

/// Removes erased metric samples from SoA series in place (ADR-0064 decision
/// 1). A series whose labels match a **windowless** predicate is dropped
/// whole; a series matching only **windowed** predicates keeps its
/// out-of-window samples and is dropped only if that leaves it empty. Series
/// matching no predicate are untouched. A no-op when `predicates` is empty, so
/// a query with no pending erasure behaves exactly as before.
pub fn retain_series_soa(series: &mut Vec<FetchedSeriesSoa>, predicates: &[ErasurePredicate]) {
    if predicates.is_empty() {
        return;
    }
    series.retain_mut(|s| {
        let matching = matching_predicates(&s.labels, predicates);
        if matching.is_empty() {
            return true;
        }
        if matching.iter().any(|p| !p.has_window()) {
            return false;
        }
        compact_parallel(&mut s.timestamps, &mut s.values, &matching);
        !s.timestamps.is_empty()
    });
}

/// AoS counterpart of [`retain_series_soa`]: same rule, over
/// [`FetchedSeries`]'s `samples` vec.
pub fn retain_series_aos(series: &mut Vec<FetchedSeries>, predicates: &[ErasurePredicate]) {
    if predicates.is_empty() {
        return;
    }
    series.retain_mut(|s| {
        let matching = matching_predicates(&s.labels, predicates);
        if matching.is_empty() {
            return true;
        }
        if matching.iter().any(|p| !p.has_window()) {
            return false;
        }
        s.samples
            .retain(|smp: &Sample| sample_survives(smp.ts_ns, &matching));
        !s.samples.is_empty()
    });
}

/// Histogram counterpart of [`retain_series_soa`]: same rule, over
/// [`FetchedHistogramSeries`]'s parallel timestamp/value vecs.
pub fn retain_histogram_series(
    series: &mut Vec<FetchedHistogramSeries>,
    predicates: &[ErasurePredicate],
) {
    if predicates.is_empty() {
        return;
    }
    series.retain_mut(|s| {
        let matching = matching_predicates(&s.labels, predicates);
        if matching.is_empty() {
            return true;
        }
        if matching.iter().any(|p| !p.has_window()) {
            return false;
        }
        compact_parallel(&mut s.timestamps, &mut s.values, &matching);
        !s.timestamps.is_empty()
    });
}

/// Removes series whose labels match a **windowless** predicate from a
/// labels/series-metadata result (the `labels`, `label-values`, and `series`
/// endpoints, via `fetch_series`). A **windowed** predicate erases only some
/// samples of a series, so the series legitimately still exists for label
/// enumeration and is not removed here (its erased samples are excluded on the
/// sample path). A no-op when `predicates` is empty.
pub fn retain_series_entries(entries: &mut Vec<SeriesEntry>, predicates: &[ErasurePredicate]) {
    if predicates.is_empty() {
        return;
    }
    entries.retain(|e| {
        !predicates
            .iter()
            .any(|p| !p.has_window() && p.matches_labels(&e.labels))
    });
}

/// Removes erased log rows in place (ADR-0064 decision 1): a row is dropped if
/// any predicate's conjunction matches its per-record attributes and, when the
/// predicate is windowed, its `ts_ns` falls in the window. A no-op when
/// `predicates` is empty.
pub fn retain_log_records(records: &mut Vec<LogRecord>, predicates: &[ErasurePredicate]) {
    if predicates.is_empty() {
        return;
    }
    records.retain(|r| {
        !predicates
            .iter()
            .any(|p| p.matches_log_attrs(&r.attrs) && (!p.has_window() || p.ts_in_window(r.ts_ns)))
    });
}

/// The pending selective-erasure predicates attached to a resolved snapshot
/// (ADR-0064 decision 2, EJ-T3, issue #753). Every SQL and PromQL query
/// surface excludes series/samples/rows/spans matching any of these after
/// fetch and after cache; this is the single connection point between a
/// resolved [`Snapshot`] and that filter machinery, so `ravel-query`'s engine
/// and `ravel-sql`'s scan surfaces derive the same predicates from the same
/// snapshot field rather than each duplicating the mapping.
///
/// EJ-T2 (#752) is the resolver task that lists `t/<th>/<sig>/del/` per resolve
/// and attaches the decoded pending requests to [`Snapshot::pending_erasure`],
/// already scoped to this resolve's (tenant, signal). Each `ErasureRequest`
/// becomes one [`ErasurePredicate`], mapping its repeated `predicate` matchers
/// into `(key, value)` pairs and carrying the half-open
/// `[window_start_ns, window_end_ns)` event-time bounds through unchanged
/// (`0` on a bound is "unset" per `ErasurePredicate`). The request's other
/// fields (`format_version`, `tenant_hash`, `signal`, `request_id`,
/// `created_unix_ns`, `reason`) play no part in filtering.
///
/// Returns an empty vec when the snapshot carries no pending erasure, which is
/// the common case and makes every call site below a no-op.
pub fn snapshot_pending_erasure_predicates(snapshot: &Snapshot) -> Vec<ErasurePredicate> {
    snapshot
        .pending_erasure
        .iter()
        .map(|request| {
            let matchers = request
                .predicate
                .iter()
                .map(|matcher| (matcher.key.clone(), matcher.value.clone()))
                .collect();
            ErasurePredicate::new(matchers, request.window_start_ns, request.window_end_ns)
        })
        .collect()
}

/// Compacts two positionally-aligned vecs (timestamps and values) in place,
/// keeping only the indices whose timestamp survives the matching windowed
/// predicates. `timestamps` and `values` must have equal length; both are
/// truncated to the surviving prefix, preserving on-disk order.
fn compact_parallel<V>(
    timestamps: &mut Vec<i64>,
    values: &mut Vec<V>,
    matching: &[&ErasurePredicate],
) {
    debug_assert_eq!(timestamps.len(), values.len());
    let mut write = 0usize;
    for read in 0..timestamps.len() {
        if sample_survives(timestamps[read], matching) {
            timestamps.swap(write, read);
            values.swap(write, read);
            write += 1;
        }
    }
    timestamps.truncate(write);
    values.truncate(write);
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_logseg::LogStreamId;
    use ravel_segment::HistogramValue;
    use ravel_types::{Label, SeriesId};

    /// One SoA series projected to comparable owned data: its label pairs and
    /// its parallel timestamp/value vecs.
    type SoaProjection = (Vec<(String, String)>, Vec<i64>, Vec<f64>);

    /// A comparable projection of SoA series ([`FetchedSeriesSoa`] derives no
    /// `PartialEq`): sorted label pairs plus the parallel sample vecs.
    fn proj(series: &[FetchedSeriesSoa]) -> Vec<SoaProjection> {
        series
            .iter()
            .map(|s| {
                (
                    s.labels
                        .iter()
                        .map(|l| (l.name.clone(), l.value.clone()))
                        .collect(),
                    s.timestamps.clone(),
                    s.values.clone(),
                )
            })
            .collect()
    }

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
        .expect("unique label names")
    }

    fn pred(matchers: &[(&str, &str)], start: i64, end: i64) -> ErasurePredicate {
        ErasurePredicate::new(
            matchers
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            start,
            end,
        )
    }

    fn empty_histogram_value() -> HistogramValue {
        HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: None,
            custom_values: None,
            positive_spans: Vec::new(),
            negative_spans: Vec::new(),
            counts: ravel_segment::HistogramCounts::Int {
                zero_count: 0,
                count: 0,
                positive: Vec::new(),
                negative: Vec::new(),
            },
            reset_hint: ravel_segment::ResetHint::Unknown,
        }
    }

    fn soa(lbls: LabelSet, samples: &[(i64, f64)]) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([0; 16]),
            labels: lbls,
            timestamps: samples.iter().map(|(t, _)| *t).collect(),
            values: samples.iter().map(|(_, v)| *v).collect(),
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
        }
    }

    // ---- conjunction / label matching ----

    #[test]
    fn conjunction_requires_every_matcher() {
        let p = pred(&[("user_id", "u1"), ("env", "prod")], 0, 0);
        assert!(p.matches_labels(&labels(&[("user_id", "u1"), ("env", "prod"), ("x", "y")])));
        // Missing one conjunct: no match.
        assert!(!p.matches_labels(&labels(&[("user_id", "u1"), ("env", "staging")])));
        assert!(!p.matches_labels(&labels(&[("env", "prod")])));
    }

    #[test]
    fn empty_matcher_set_matches_nothing() {
        // Fail-safe: a zero-matcher conjunction must never erase everything.
        let p = ErasurePredicate::new(vec![], 0, 0);
        assert!(!p.matches_labels(&labels(&[("anything", "at-all")])));
        assert!(!p.matches_log_attrs(&[("k".to_string(), AttrValue::Str("v".to_string()))]));
        assert!(!p.matches_str_attrs(&[("k".to_string(), "v".to_string())]));
    }

    #[test]
    fn window_is_half_open_with_zero_as_unset() {
        let p = pred(&[("a", "b")], 100, 200);
        assert!(p.has_window());
        assert!(!p.ts_in_window(99));
        assert!(p.ts_in_window(100)); // inclusive lower
        assert!(p.ts_in_window(199));
        assert!(!p.ts_in_window(200)); // exclusive upper
        // Unset lower bound.
        let lower_open = pred(&[("a", "b")], 0, 200);
        assert!(lower_open.ts_in_window(i64::MIN));
        assert!(!lower_open.ts_in_window(200));
        // Unset upper bound.
        let upper_open = pred(&[("a", "b")], 100, 0);
        assert!(!upper_open.ts_in_window(99));
        assert!(upper_open.ts_in_window(i64::MAX));
        // Windowless matches all timestamps.
        let none = pred(&[("a", "b")], 0, 0);
        assert!(!none.has_window());
        assert!(none.ts_in_window(0));
        assert!(none.ts_in_window(i64::MIN));
    }

    // ---- metrics ----

    #[test]
    fn metrics_windowless_predicate_drops_whole_series() {
        let mut series = vec![
            soa(
                labels(&[("__name__", "m"), ("user_id", "u1")]),
                &[(1, 1.0), (2, 2.0)],
            ),
            soa(labels(&[("__name__", "m"), ("user_id", "u2")]), &[(1, 3.0)]),
        ];
        retain_series_soa(&mut series, &[pred(&[("user_id", "u1")], 0, 0)]);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels.get("user_id"), Some("u2"));
    }

    #[test]
    fn metrics_windowed_predicate_drops_only_in_window_samples() {
        let mut series = vec![soa(
            labels(&[("__name__", "m"), ("user_id", "u1")]),
            &[(50, 1.0), (100, 2.0), (150, 3.0), (200, 4.0)],
        )];
        // Erase [100, 200): drops ts 100 and 150, keeps 50 and 200.
        retain_series_soa(&mut series, &[pred(&[("user_id", "u1")], 100, 200)]);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].timestamps, vec![50, 200]);
        assert_eq!(series[0].values, vec![1.0, 4.0]);
    }

    #[test]
    fn metrics_windowed_predicate_dropping_all_samples_drops_series() {
        let mut series = vec![soa(labels(&[("user_id", "u1")]), &[(100, 1.0), (150, 2.0)])];
        retain_series_soa(&mut series, &[pred(&[("user_id", "u1")], 0, 1000)]);
        assert!(series.is_empty());
    }

    #[test]
    fn metrics_no_predicate_is_identity() {
        let original = vec![
            soa(labels(&[("user_id", "u1")]), &[(1, 1.0), (2, 2.0)]),
            soa(labels(&[("user_id", "u2")]), &[(3, 3.0)]),
        ];
        let mut series = original.clone();
        retain_series_soa(&mut series, &[]);
        assert_eq!(proj(&series), proj(&original));
    }

    #[test]
    fn metrics_aos_and_histograms_follow_the_same_rule() {
        // AoS: windowed drop.
        let mut aos = vec![FetchedSeries {
            series_id: SeriesId([0; 16]),
            labels: labels(&[("user_id", "u1")]),
            samples: vec![
                Sample {
                    ts_ns: 50,
                    value: 1.0,
                },
                Sample {
                    ts_ns: 120,
                    value: 2.0,
                },
            ],
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
        }];
        retain_series_aos(&mut aos, &[pred(&[("user_id", "u1")], 100, 200)]);
        assert_eq!(aos.len(), 1);
        assert_eq!(aos[0].samples.len(), 1);
        assert_eq!(aos[0].samples[0].ts_ns, 50);

        // Histograms: windowless whole-series drop.
        let mut hist = vec![FetchedHistogramSeries {
            series_id: SeriesId([0; 16]),
            labels: labels(&[("user_id", "u1")]),
            timestamps: vec![1, 2],
            values: vec![empty_histogram_value(), empty_histogram_value()],
            created_unix_ns: 0,
            writer_epoch: 0,
            writer_seq: 0,
        }];
        retain_histogram_series(&mut hist, &[pred(&[("user_id", "u1")], 0, 0)]);
        assert!(hist.is_empty());
    }

    #[test]
    fn series_entries_drop_only_on_windowless_match() {
        let entry = |name: &str, uid: &str| SeriesEntry {
            series_id: SeriesId([0; 16]),
            labels: labels(&[("__name__", name), ("user_id", uid)]),
            sample_count: 1,
            min_ts_ns: 0,
            max_ts_ns: 0,
            ts_page: (0, 0),
            val_page: (0, 0),
            value_kind: ravel_segment::ValueKind::Scalar,
            hist_page: (0, 0),
        };
        let mut entries = vec![entry("m", "u1"), entry("m", "u2")];
        // Windowless: series u1 removed from label enumeration.
        retain_series_entries(&mut entries, &[pred(&[("user_id", "u1")], 0, 0)]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].labels.get("user_id"), Some("u2"));
        // Windowed: series still exists for label enumeration.
        let mut entries = vec![entry("m", "u2")];
        retain_series_entries(&mut entries, &[pred(&[("user_id", "u2")], 100, 200)]);
        assert_eq!(entries.len(), 1);
    }

    // ---- logs ----

    fn log_row(ts: i64, attrs: &[(&str, &str)]) -> LogRecord {
        LogRecord {
            stream_id: LogStreamId([0; 16]),
            stream_attrs: Vec::new(),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs
                .iter()
                .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
                .collect(),
        }
    }

    #[test]
    fn logs_windowless_drops_matching_rows() {
        let mut rows = vec![
            log_row(1, &[("user_id", "u1")]),
            log_row(2, &[("user_id", "u2")]),
            log_row(3, &[("user_id", "u1"), ("k", "v")]),
        ];
        retain_log_records(&mut rows, &[pred(&[("user_id", "u1")], 0, 0)]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_ns, 2);
    }

    #[test]
    fn logs_windowed_drops_only_in_window_rows() {
        let mut rows = vec![
            log_row(50, &[("user_id", "u1")]),
            log_row(150, &[("user_id", "u1")]),
            log_row(250, &[("user_id", "u1")]),
        ];
        retain_log_records(&mut rows, &[pred(&[("user_id", "u1")], 100, 200)]);
        assert_eq!(
            rows.iter().map(|r| r.ts_ns).collect::<Vec<_>>(),
            vec![50, 250]
        );
    }

    #[test]
    fn logs_non_string_attr_never_matches() {
        let mut rows = vec![LogRecord {
            attrs: vec![("user_id".to_string(), AttrValue::I64(123))],
            ..log_row(1, &[])
        }];
        retain_log_records(&mut rows, &[pred(&[("user_id", "123")], 0, 0)]);
        assert_eq!(rows.len(), 1); // an I64 attr does not satisfy a string matcher
    }

    #[test]
    fn logs_no_predicate_is_identity() {
        let original = vec![
            log_row(1, &[("user_id", "u1")]),
            log_row(2, &[("user_id", "u2")]),
        ];
        let mut rows = original.clone();
        retain_log_records(&mut rows, &[]);
        assert_eq!(rows.len(), original.len());
    }

    // ---- spans (reusable core; wired in ravel-sql) ----

    #[test]
    fn spans_windowless_and_windowed() {
        let attrs = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };
        let preds = [pred(&[("user_id", "u1")], 0, 0)];
        assert!(is_erased_span(&attrs(&[("user_id", "u1")]), 999, &preds));
        assert!(!is_erased_span(&attrs(&[("user_id", "u2")]), 999, &preds));
        // Windowed: only spans whose start ts is in-window are erased.
        let windowed = [pred(&[("user_id", "u1")], 100, 200)];
        assert!(is_erased_span(&attrs(&[("user_id", "u1")]), 150, &windowed));
        assert!(!is_erased_span(&attrs(&[("user_id", "u1")]), 50, &windowed));
    }

    // ---- properties ----

    proptest! {
        // A windowless predicate on a present label erases every matching
        // series; no non-matching series is ever removed.
        #[test]
        fn prop_windowless_removes_exactly_matching_series(
            uids in prop::collection::vec(0u8..4, 0..12),
        ) {
            let mut series: Vec<FetchedSeriesSoa> = uids
                .iter()
                .map(|u| soa(labels(&[("user_id", &format!("u{u}"))]), &[(1, 1.0)]))
                .collect();
            let before_other = uids.iter().filter(|u| **u != 1).count();
            retain_series_soa(&mut series, &[pred(&[("user_id", "u1")], 0, 0)]);
            // No u1 survives; every non-u1 survives.
            prop_assert!(series.iter().all(|s| s.labels.get("user_id") != Some("u1")));
            prop_assert_eq!(series.len(), before_other);
        }

        // A windowed predicate never removes a sample outside its window and
        // always removes one inside it, for a label-matching series.
        #[test]
        fn prop_windowed_sample_partition(
            tss in prop::collection::vec(-1000i64..1000, 0..30),
            start in -500i64..500,
            len in 1i64..500,
        ) {
            let end = start + len;
            // Both bounds must be genuinely set for the strict `[start, end)`
            // oracle below to describe the predicate: `0` on a bound is the
            // zero-as-unset sentinel (see the module docs), which opens that
            // side of the window and legitimately erases samples the strict
            // reading would keep -- `start == 0` erases every negative
            // timestamp, `end == 0` erases every timestamp at or above
            // `start`. The sentinel's own semantics are covered exactly by
            // `window_is_half_open_with_zero_as_unset`; this property covers
            // fully-specified windows.
            prop_assume!(start != 0 && end != 0);
            let samples: Vec<(i64, f64)> = tss.iter().map(|t| (*t, *t as f64)).collect();
            let mut series = vec![soa(labels(&[("user_id", "u1")]), &samples)];
            retain_series_soa(&mut series, &[pred(&[("user_id", "u1")], start, end)]);
            let survivors: Vec<i64> = series.first().map(|s| s.timestamps.clone()).unwrap_or_default();
            // Every survivor is outside [start, end).
            prop_assert!(survivors.iter().all(|t| *t < start || *t >= end));
            // Every original ts outside the window survives (order preserved).
            let expected: Vec<i64> = tss.iter().copied().filter(|t| *t < start || *t >= end).collect();
            prop_assert_eq!(survivors, expected);
        }

        // Empty predicate list is always the identity.
        #[test]
        fn prop_no_predicate_identity(
            uids in prop::collection::vec(0u8..4, 0..12),
        ) {
            let original: Vec<FetchedSeriesSoa> = uids
                .iter()
                .map(|u| soa(labels(&[("user_id", &format!("u{u}"))]), &[(1, 1.0)]))
                .collect();
            let mut series = original.clone();
            retain_series_soa(&mut series, &[]);
            prop_assert_eq!(proj(&series), proj(&original));
        }
    }
}
