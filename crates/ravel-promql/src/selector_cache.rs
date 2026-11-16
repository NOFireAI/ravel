//! Per-query selector cache for range-query evaluation (ADR-3072 decision 1b).
//!
//! A range query whose top-level expression is an aggregate or a binary
//! expression is evaluated once per grid step, as are the per-step function
//! shapes (`abs(m)`, `histogram_quantile`, a range function over a subquery)
//! and every subquery's inner expression. Without this cache each of those
//! evaluations reads every
//! selector it reaches from the [`SeriesSource`](crate::source::SeriesSource)
//! again. With it, the first read of a selector fetches a window wide enough
//! to answer the selector at every later step, and each step is answered by
//! slicing that one fetch.
//!
//! A selector with a resolved `@` asks for the same window at every step, so
//! its fetch is that window, widened only by any enclosing subquery grids'
//! reach, and is not translated across the top-level grid.
//!
//! A miss on a key that already has an entry fetches the union of the entry's
//! window and the newly computed one, and that read replaces the entry. The
//! old entry's samples are dropped, not merged into the new read: the union
//! fetch already contains them, by the source property below. A key's window
//! therefore only grows within a query, and two selectors sharing a key whose
//! windows do not contain each other stop evicting each other once one read
//! covers both.
//!
//! Correctness rests on one source property: the `SeriesSource` contract
//! clips samples to the requested window (inclusive on both ends) and sorts
//! them by `ts_ns`, so the samples a fetch over a window `W` returns inside a
//! narrower window `R` are exactly the samples a fetch over `R` returns. The
//! window choice only decides how many fetches happen, never which samples a
//! step sees. A step can see a series whose slice is empty where a fetch over
//! `R` would return no series at all; every consumer drops an empty series,
//! so the output is the same.
//!
//! Every entry is charged, before it is stored, against two bounds: a
//! per-query byte cap on the cache's running total, and the query's
//! [`MemoryBudget`], one [`Reservation`] per entry, released when the entry
//! is replaced, when its key falls back, or when the cache is dropped with
//! its query (issue #3105). A
//! replacement is charged its own bytes in place of the old entry's on both
//! bounds, not the two together. An entry that does not fit is not stored, and its key falls back: every later step
//! reads that selector over its own request window, as evaluation without the
//! cache does. A fallback changes how many reads a query makes, not which
//! samples a step sees. The reservation is taken with
//! [`MemoryBudget::reserve_best_effort`], so a budget refusal here moves none
//! of the budget's refusal counters; the query's info annotation reports it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use ravel_memory::{MemoryBudget, MemoryExhausted, Reservation};
use ravel_types::{Label, LabelSet, Sample, TimeRange};

use crate::histogram::{FloatHistogram, Span};
use crate::source::{HistogramSample, HistogramSeriesData, LabelMatcher, MatchOp, SeriesData};

/// Which of the two `SeriesSource` reads an entry came from. A series is
/// float or native histogram in storage, and the two reads are separate
/// calls, so the same matcher set has one entry per channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SelectorChannel {
    Float,
    Histogram,
}

/// The operator of one matcher in a [`SelectorKey`]. `LabelMatcher` has no
/// `Hash` (its regex operators carry a compiled `Regex`); the key keeps the
/// operator kind here and the pattern text beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum MatcherKind {
    Eq,
    Ne,
    Re,
    Nre,
}

/// Identifies one cache entry: the selector's matcher set in canonical form
/// (label name, operator kind, compiled pattern text, value text; sorted, so
/// matcher order in the query does not matter), the channel, and the
/// selector's own time shift (signed `offset` and resolved `@` instant).
///
/// The shift is part of the key so that `m - m offset 1d` keeps two entries,
/// each covering its own selector's whole grid.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SelectorKey {
    matchers: Vec<(String, MatcherKind, String, String)>,
    channel: SelectorChannel,
    offset_ns: i64,
    at_ns: Option<i64>,
}

impl SelectorKey {
    pub(crate) fn new(
        matchers: &[LabelMatcher],
        channel: SelectorChannel,
        offset_ns: i64,
        at_ns: Option<i64>,
    ) -> Self {
        let mut canonical: Vec<(String, MatcherKind, String, String)> = matchers
            .iter()
            .map(|m| {
                let (kind, pattern) = match &m.op {
                    MatchOp::Eq => (MatcherKind::Eq, String::new()),
                    MatchOp::Ne => (MatcherKind::Ne, String::new()),
                    MatchOp::Re(re) => (MatcherKind::Re, re.as_str().to_string()),
                    MatchOp::Nre(re) => (MatcherKind::Nre, re.as_str().to_string()),
                };
                (m.name.clone(), kind, pattern, m.value.clone())
            })
            .collect();
        canonical.sort();
        SelectorKey {
            matchers: canonical,
            channel,
            offset_ns,
            at_ns,
        }
    }

    /// Whether the selector carries a resolved `@`, which fixes its window
    /// for every step of the query.
    pub(crate) fn pinned(&self) -> bool {
        self.at_ns.is_some()
    }
}

/// One series type a selector read returns, abstracted so the float and
/// native-histogram channels share the cache and slicing code.
pub(crate) trait CachedSeries: Sized {
    type Point;
    const CHANNEL: SelectorChannel;
    fn labels(&self) -> &LabelSet;
    fn points(&self) -> &[Self::Point];
    /// The heap bytes this series holds outside its own struct: its labels
    /// and its points, by allocated capacity.
    fn heap_bytes(&self) -> u64;
    fn ts_ns(point: &Self::Point) -> i64;
    fn into_entry(cached: Rc<CachedSelector<Self>>) -> CacheEntry;
    fn from_entry(entry: &CacheEntry) -> Option<&Rc<CachedSelector<Self>>>;
}

impl CachedSeries for SeriesData {
    type Point = Sample;
    const CHANNEL: SelectorChannel = SelectorChannel::Float;
    fn labels(&self) -> &LabelSet {
        &self.labels
    }
    fn points(&self) -> &[Sample] {
        &self.samples
    }
    fn heap_bytes(&self) -> u64 {
        label_bytes(&self.labels).saturating_add(vec_bytes(&self.samples))
    }
    fn ts_ns(point: &Sample) -> i64 {
        point.ts_ns
    }
    fn into_entry(cached: Rc<CachedSelector<Self>>) -> CacheEntry {
        CacheEntry::Float(cached)
    }
    fn from_entry(entry: &CacheEntry) -> Option<&Rc<CachedSelector<Self>>> {
        match entry {
            CacheEntry::Float(cached) => Some(cached),
            CacheEntry::Histogram(_) => None,
        }
    }
}

impl CachedSeries for HistogramSeriesData {
    type Point = HistogramSample;
    const CHANNEL: SelectorChannel = SelectorChannel::Histogram;
    fn labels(&self) -> &LabelSet {
        &self.labels
    }
    fn points(&self) -> &[HistogramSample] {
        &self.samples
    }
    fn heap_bytes(&self) -> u64 {
        self.samples
            .iter()
            .fold(label_bytes(&self.labels), |sum, point| {
                sum.saturating_add(histogram_heap_bytes(&point.value))
            })
            .saturating_add(vec_bytes(&self.samples))
    }
    fn ts_ns(point: &HistogramSample) -> i64 {
        point.ts_ns
    }
    fn into_entry(cached: Rc<CachedSelector<Self>>) -> CacheEntry {
        CacheEntry::Histogram(cached)
    }
    fn from_entry(entry: &CacheEntry) -> Option<&Rc<CachedSelector<Self>>> {
        match entry {
            CacheEntry::Histogram(cached) => Some(cached),
            CacheEntry::Float(_) => None,
        }
    }
}

/// `capacity` elements of `T`, saturating.
fn capacity_bytes<T>(capacity: usize) -> u64 {
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(std::mem::size_of::<T>()).unwrap_or(u64::MAX))
}

fn vec_bytes<T>(v: &Vec<T>) -> u64 {
    capacity_bytes::<T>(v.capacity())
}

/// A label set's heap bytes: one [`Label`] per label (the set's own vector is
/// private, so its length stands in for its capacity) plus each name's and
/// value's allocated capacity.
fn label_bytes(labels: &LabelSet) -> u64 {
    labels
        .iter()
        .fold(capacity_bytes::<Label>(labels.len()), |sum, label| {
            sum.saturating_add(capacity_bytes::<u8>(label.name.capacity()))
                .saturating_add(capacity_bytes::<u8>(label.value.capacity()))
        })
}

/// A native histogram's span and bucket vectors, by allocated capacity.
fn histogram_heap_bytes(h: &FloatHistogram) -> u64 {
    capacity_bytes::<Span>(h.positive_spans.capacity())
        .saturating_add(capacity_bytes::<Span>(h.negative_spans.capacity()))
        .saturating_add(vec_bytes(&h.positive_buckets))
        .saturating_add(vec_bytes(&h.negative_buckets))
        .saturating_add(vec_bytes(&h.custom_values))
}

/// How far one bound's cursor walks forward through a series' points before
/// the move counts as a jump and is found by binary search instead.
const MAX_CURSOR_WALK: usize = 64;

/// A cursor pair no request has seated yet. Past every series' end, so the
/// first request searches.
const UNSEATED: (usize, usize) = (usize::MAX, usize::MAX);

/// `points.partition_point(pred)`, found from `hint`, a previous answer for a
/// predicate of the same shape. When the answer lies at most
/// [`MAX_CURSOR_WALK`] points ahead of `hint` it is reached by walking;
/// otherwise (behind `hint`, further ahead, or `hint` past the end) it is
/// found by binary search over the side of `hint` that holds it. The `bool`
/// says whether a search ran. `pred` must be true on a prefix of `points` and
/// false on the rest, which ascending `ts_ns` gives both bound predicates.
fn seek<P>(points: &[P], hint: usize, pred: impl Fn(&P) -> bool) -> (usize, bool) {
    if hint > points.len() || (hint > 0 && !pred(&points[hint - 1])) {
        let end = hint.min(points.len());
        return (points[..end].partition_point(pred), true);
    }
    let stop = hint.saturating_add(MAX_CURSOR_WALK).min(points.len());
    let mut at = hint;
    while at < stop && pred(&points[at]) {
        at += 1;
    }
    if at == stop && at < points.len() && pred(&points[at]) {
        return (at + points[at..].partition_point(pred), true);
    }
    (at, false)
}

/// The series one selector read returned, in source order, and the window
/// that read covered, with one lower and upper slice cursor per series.
#[derive(Debug)]
pub(crate) struct CachedSelector<S> {
    window: TimeRange,
    series: Vec<S>,
    /// Each series' `[lo, hi)` from its last [`Self::slices`] request, the
    /// starting point for the next one.
    cursors: Vec<Cell<(usize, usize)>>,
    /// Series slices whose cursors were re-seated by binary search.
    #[cfg(test)]
    reseats: Cell<u64>,
}

impl<S: CachedSeries> CachedSelector<S> {
    pub(crate) fn new(window: TimeRange, series: Vec<S>) -> Self {
        CachedSelector {
            window,
            cursors: series.iter().map(|_| Cell::new(UNSEATED)).collect(),
            series,
            #[cfg(test)]
            reseats: Cell::new(0),
        }
    }

    /// The window the read covered.
    pub(crate) fn window(&self) -> TimeRange {
        self.window
    }

    /// How many series the read returned.
    pub(crate) fn len(&self) -> usize {
        self.series.len()
    }

    /// The bytes this read holds: its series vector by capacity, plus each
    /// series' [`CachedSeries::heap_bytes`]. This is what an entry is
    /// charged. The cursors, two `usize` per series, are not counted.
    pub(crate) fn bytes(&self) -> u64 {
        self.series.iter().fold(vec_bytes(&self.series), |sum, s| {
            sum.saturating_add(s.heap_bytes())
        })
    }

    /// Every series' points inside `request`, inclusive on both ends like
    /// `TimeRange`, so equal timestamps at either bound are kept. Series
    /// order is the source's. `request` must lie inside [`Self::window`]. A
    /// read over exactly `request` is returned whole, since the source
    /// already clipped it.
    ///
    /// Each bound is found from where the series' previous request left it
    /// ([`seek`]): a range query's steps move both bounds forward a few
    /// points at a time, which a short walk answers, and a request that moves
    /// a bound backwards or far ahead re-seats it by binary search. The slice
    /// is the same whichever way a bound was found.
    pub(crate) fn slices(
        &self,
        request: TimeRange,
    ) -> impl Iterator<Item = (&LabelSet, &[S::Point])> + '_ {
        let whole = request == self.window;
        self.series
            .iter()
            .zip(&self.cursors)
            .map(move |(s, cursor)| {
                let points = s.points();
                if whole {
                    return (s.labels(), points);
                }
                let (lo_hint, hi_hint) = cursor.get();
                let (lo, lo_searched) = seek(points, lo_hint, |p| S::ts_ns(p) < request.start_ns);
                let (hi, hi_searched) = seek(points, hi_hint, |p| S::ts_ns(p) <= request.end_ns);
                cursor.set((lo, hi));
                self.note_reseat(lo_searched || hi_searched);
                (s.labels(), &points[lo..hi.max(lo)])
            })
    }

    #[cfg(test)]
    fn note_reseat(&self, searched: bool) {
        if searched {
            self.reseats.set(self.reseats.get() + 1);
        }
    }

    #[cfg(not(test))]
    fn note_reseat(&self, _searched: bool) {}
}

pub(crate) enum CacheEntry {
    Float(Rc<CachedSelector<SeriesData>>),
    Histogram(Rc<CachedSelector<HistogramSeriesData>>),
}

impl CacheEntry {
    fn window(&self) -> TimeRange {
        match self {
            CacheEntry::Float(cached) => cached.window(),
            CacheEntry::Histogram(cached) => cached.window(),
        }
    }
}

/// A stored entry with the bytes it was charged and the budget reservation
/// that holds them; dropping the slot releases the reservation.
struct Slot {
    entry: CacheEntry,
    bytes: u64,
    _reservation: Option<Reservation>,
}

/// Why a key's entry was not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FallbackReason {
    /// The entry would have taken the cache's running total over its cap.
    Cap,
    /// The query's memory budget refused the entry's reservation.
    Budget,
}

/// Source reads the cache issued over one query, and the bytes it held.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectorCacheStats {
    /// Every `SeriesSource` read, either channel, including the per-step
    /// reads of a key that fell back.
    pub(crate) fetches: u64,
    /// The reads that replaced an existing entry because a request fell
    /// outside its window.
    pub(crate) refetches: u64,
    /// The bytes the stored entries hold now.
    pub(crate) bytes: u64,
    /// The largest value `bytes` reached over the query.
    pub(crate) peak_bytes: u64,
    /// The keys whose entry did not fit, for either reason.
    pub(crate) fallback_keys: u64,
}

pub(crate) struct SelectorCache {
    entries: RefCell<HashMap<SelectorKey, Slot>>,
    /// Keys whose entry did not fit, with which bound refused it; the refusal
    /// itself is kept only for the query's first, in `first_refusal`. A key
    /// here is not offered to [`Self::insert`] again and has no entry: the
    /// refused insert dropped any entry it would have replaced.
    fallen_back: RefCell<HashMap<SelectorKey, FallbackReason>>,
    /// The first budget refusal of the query, in the order [`Self::insert`]
    /// met them.
    first_refusal: Cell<Option<MemoryExhausted>>,
    max_bytes: u64,
    budget: Option<Arc<MemoryBudget>>,
    bytes: Cell<u64>,
    peak_bytes: Cell<u64>,
    fetches: Cell<u64>,
    refetches: Cell<u64>,
}

impl Default for SelectorCache {
    fn default() -> Self {
        SelectorCache::new(u64::MAX, None)
    }
}

impl SelectorCache {
    /// A cache whose stored entries may hold at most `max_bytes` in total
    /// (`u64::MAX` is uncapped), each also reserved against `budget` when one
    /// is given.
    pub(crate) fn new(max_bytes: u64, budget: Option<Arc<MemoryBudget>>) -> Self {
        SelectorCache {
            entries: RefCell::default(),
            fallen_back: RefCell::default(),
            first_refusal: Cell::new(None),
            max_bytes,
            budget,
            bytes: Cell::new(0),
            peak_bytes: Cell::new(0),
            fetches: Cell::new(0),
            refetches: Cell::new(0),
        }
    }

    /// The entry for `key` when its window covers `request`.
    pub(crate) fn lookup<S: CachedSeries>(
        &self,
        key: &SelectorKey,
        request: TimeRange,
    ) -> Option<Rc<CachedSelector<S>>> {
        let entries = self.entries.borrow();
        let cached = S::from_entry(&entries.get(key)?.entry)?;
        (cached.window.start_ns <= request.start_ns && request.end_ns <= cached.window.end_ns)
            .then(|| Rc::clone(cached))
    }

    /// Whether `key`'s entry did not fit earlier in this query, so a request
    /// [`Self::lookup`] does not answer is read over the request alone.
    pub(crate) fn fell_back(&self, key: &SelectorKey) -> bool {
        self.fallen_back.borrow().contains_key(key)
    }

    /// Count a read made outside the cache for a key that fell back.
    pub(crate) fn note_uncached_read(&self) {
        self.fetches.set(self.fetches.get().saturating_add(1));
    }

    /// The bytes the stored entries hold.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.get()
    }

    /// How many keys fell back for each reason, `(cap, budget)`, and the
    /// first budget refusal the query met.
    pub(crate) fn fallbacks(&self) -> (u64, u64, Option<MemoryExhausted>) {
        let mut cap = 0u64;
        let mut budget = 0u64;
        for reason in self.fallen_back.borrow().values() {
            match reason {
                FallbackReason::Cap => cap = cap.saturating_add(1),
                FallbackReason::Budget => budget = budget.saturating_add(1),
            }
        }
        (cap, budget, self.first_refusal.get())
    }

    /// The per-query cap this cache was built with.
    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// The window a miss on `key` fetches: `computed` when `key` has no
    /// entry, otherwise the smallest window containing both `computed` and
    /// the entry's window, so the read that replaces the entry still answers
    /// every request the entry did.
    pub(crate) fn miss_window(&self, key: &SelectorKey, computed: TimeRange) -> TimeRange {
        match self.entries.borrow().get(key) {
            Some(slot) => {
                let old = slot.entry.window();
                TimeRange {
                    start_ns: old.start_ns.min(computed.start_ns),
                    end_ns: old.end_ns.max(computed.end_ns),
                }
            }
            None => computed,
        }
    }

    /// Count a fresh read of `key` and store it, replacing any previous
    /// entry, when it fits: the running total with this entry in place of the
    /// one it replaces must stay within the cap, and the query's budget must
    /// admit a reservation of the entry's bytes. A replacement costs each bound
    /// the new entry's bytes in place of the old entry's, never both: the cap
    /// check subtracts the old entry's bytes, and the old entry, with its
    /// reservation, is dropped before the new one is reserved. A previous
    /// entry is dropped, with its bytes and reservation, whether or not the new
    /// one fits: when either bound refuses, `key` falls back and holds
    /// nothing. A miss reads over [`Self::miss_window`], so a replacement covers
    /// the window of the entry it replaces. This is the only way an entry
    /// enters the cache.
    pub(crate) fn insert<S: CachedSeries>(&self, key: SelectorKey, cached: Rc<CachedSelector<S>>) {
        self.fetches.set(self.fetches.get().saturating_add(1));
        let bytes = cached.bytes();
        let mut entries = self.entries.borrow_mut();
        let replaced_bytes = entries.get(&key).map_or(0, |slot| slot.bytes);
        let total = self
            .bytes()
            .saturating_sub(replaced_bytes)
            .saturating_add(bytes);
        let replaced = entries.remove(&key).is_some();
        if replaced {
            self.bytes
                .set(self.bytes.get().saturating_sub(replaced_bytes));
        }
        if total > self.max_bytes {
            self.fall_back(key, FallbackReason::Cap);
            return;
        }
        let reservation = match &self.budget {
            Some(budget) => match budget.reserve_best_effort(bytes) {
                Ok(reservation) => Some(reservation),
                Err(refusal) => {
                    self.first_refusal
                        .set(self.first_refusal.get().or(Some(refusal)));
                    self.fall_back(key, FallbackReason::Budget);
                    return;
                }
            },
            None => None,
        };
        let slot = Slot {
            entry: S::into_entry(cached),
            bytes,
            _reservation: reservation,
        };
        entries.insert(key, slot);
        if replaced {
            self.refetches.set(self.refetches.get().saturating_add(1));
        }
        self.bytes.set(total);
        self.peak_bytes.set(self.peak_bytes.get().max(total));
    }

    /// Record that `key` falls back for `reason`, keeping the first reason a
    /// key gets.
    fn fall_back(&self, key: SelectorKey, reason: FallbackReason) {
        self.fallen_back.borrow_mut().entry(key).or_insert(reason);
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> SelectorCacheStats {
        SelectorCacheStats {
            fetches: self.fetches.get(),
            refetches: self.refetches.get(),
            bytes: self.bytes(),
            peak_bytes: self.peak_bytes.get(),
            fallback_keys: u64::try_from(self.fallen_back.borrow().len()).unwrap_or(u64::MAX),
        }
    }
}

/// The window a cache miss computes for `request`, before
/// [`SelectorCache::miss_window`] joins it with an existing entry: the
/// request translated across the rest of the top-level grid,
/// `[request.start - (t_outer - grid_start), request.end + (grid_end -
/// t_outer)]`, widened further by `inner_pad` (the enclosing subquery grids'
/// extent before and after the current inner instant) so one fetch covers
/// the whole inner grid as it moves with the outer step. A selector with a resolved `@` (`pinned`)
/// asks for the same window at every top-level step, so it gets no grid
/// translation, only the `inner_pad` widening. Saturating, and never narrower
/// than `request`.
pub(crate) fn fetch_window(
    request: TimeRange,
    grid_start_ns: i64,
    grid_end_ns: i64,
    t_outer: i64,
    inner_pad: (i64, i64),
    pinned: bool,
) -> TimeRange {
    let (grid_before, grid_after) = if pinned {
        (0, 0)
    } else {
        (
            t_outer.saturating_sub(grid_start_ns).max(0),
            grid_end_ns.saturating_sub(t_outer).max(0),
        )
    };
    let before = grid_before.saturating_add(inner_pad.0.max(0));
    let after = grid_after.saturating_add(inner_pad.1.max(0));
    TimeRange {
        start_ns: request.start_ns.saturating_sub(before),
        end_ns: request.end_ns.saturating_add(after),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn samples(ts: &[i64]) -> Vec<Sample> {
        ts.iter()
            .map(|&ts_ns| Sample {
                ts_ns,
                value: ts_ns as f64,
            })
            .collect()
    }

    fn float_entry(points: usize) -> CachedSelector<SeriesData> {
        let ts: Vec<i64> = (0..points as i64).collect();
        CachedSelector::new(
            TimeRange {
                start_ns: 0,
                end_ns: points as i64,
            },
            vec![SeriesData {
                labels: LabelSet::default(),
                samples: samples(&ts),
            }],
        )
    }

    #[test]
    fn entry_bytes_cover_every_point_and_grow_with_the_sample_count() {
        let point = std::mem::size_of::<Sample>() as u64;
        let small = float_entry(1_000).bytes();
        let large = float_entry(4_000).bytes();
        assert!(small >= 1_000 * point, "{small}");
        assert!(large >= 4_000 * point, "{large}");
        // Everything beyond the points is per-series overhead, which the two
        // entries share.
        assert_eq!(large - small, 3_000 * point);

        let histogram = |points: usize| {
            CachedSelector::new(
                TimeRange {
                    start_ns: 0,
                    end_ns: points as i64,
                },
                vec![HistogramSeriesData {
                    labels: LabelSet::default(),
                    samples: (0..points as i64)
                        .map(|ts_ns| HistogramSample {
                            ts_ns,
                            value: FloatHistogram {
                                counter_reset_hint: crate::histogram::ResetHint::Unknown,
                                scale: 0,
                                zero_threshold: 0.0,
                                zero_count: 0.0,
                                count: 1.0,
                                sum: 1.0,
                                positive_spans: vec![Span {
                                    offset: 0,
                                    length: 2,
                                }],
                                negative_spans: Vec::new(),
                                positive_buckets: vec![0.5, 0.5],
                                negative_buckets: Vec::new(),
                                custom_values: Vec::new(),
                            },
                        })
                        .collect(),
                }],
            )
        };
        let per_histogram_point = std::mem::size_of::<HistogramSample>() as u64
            + std::mem::size_of::<Span>() as u64
            + 2 * std::mem::size_of::<f64>() as u64;
        let small = histogram(100).bytes();
        let large = histogram(400).bytes();
        assert!(small >= 100 * per_histogram_point, "{small}");
        assert_eq!(large - small, 300 * per_histogram_point);
    }

    #[test]
    fn slices_are_inclusive_on_both_ends_and_keep_equal_timestamps() {
        let cached = CachedSelector::new(
            TimeRange {
                start_ns: 0,
                end_ns: 100,
            },
            vec![SeriesData {
                labels: LabelSet::default(),
                samples: samples(&[5, 10, 10, 20, 30, 30, 40]),
            }],
        );
        let got: Vec<Vec<i64>> = cached
            .slices(TimeRange {
                start_ns: 10,
                end_ns: 30,
            })
            .map(|(_, points)| points.iter().map(|p| p.ts_ns).collect())
            .collect();
        assert_eq!(got, vec![vec![10, 10, 20, 30, 30]]);
    }

    #[test]
    fn key_is_independent_of_matcher_order_and_separates_channels_and_shifts() {
        let a = LabelMatcher::equal("a", "1");
        let b = LabelMatcher::regex("b", "x.*").expect("valid regex");
        let ab = SelectorKey::new(&[a.clone(), b.clone()], SelectorChannel::Float, 0, None);
        let ba = SelectorKey::new(&[b.clone(), a.clone()], SelectorChannel::Float, 0, None);
        assert_eq!(ab, ba);
        let hist = SelectorKey::new(&[a.clone(), b.clone()], SelectorChannel::Histogram, 0, None);
        assert_ne!(ab, hist);
        let shifted = SelectorKey::new(&[a.clone(), b.clone()], SelectorChannel::Float, 60, None);
        assert_ne!(ab, shifted);
        let pinned = SelectorKey::new(&[a.clone(), b.clone()], SelectorChannel::Float, 0, Some(0));
        assert_ne!(ab, pinned);
        let not_regex = SelectorKey::new(
            &[a, LabelMatcher::not_regex("b", "x.*").expect("valid regex")],
            SelectorChannel::Float,
            0,
            None,
        );
        assert_ne!(ab, not_regex);
    }

    #[test]
    fn fetch_window_translates_the_request_across_the_grid() {
        let request = TimeRange {
            start_ns: 95,
            end_ns: 100,
        };
        // First step: the window runs forward over the whole grid.
        assert_eq!(
            fetch_window(request, 100, 200, 100, (0, 0), false),
            TimeRange {
                start_ns: 95,
                end_ns: 200
            }
        );
        // A later step: backward to the grid start too.
        assert_eq!(
            fetch_window(request, 0, 200, 100, (0, 0), false),
            TimeRange {
                start_ns: -5,
                end_ns: 200
            }
        );
        // Inside a subquery the inner grid's extent is added on both sides.
        assert_eq!(
            fetch_window(request, 100, 200, 100, (3, 7), false),
            TimeRange {
                start_ns: 92,
                end_ns: 207
            }
        );
        // Saturates instead of overflowing.
        assert_eq!(
            fetch_window(
                TimeRange {
                    start_ns: i64::MIN + 1,
                    end_ns: i64::MAX - 1
                },
                i64::MIN,
                i64::MAX,
                0,
                (i64::MAX, i64::MAX),
                false
            ),
            TimeRange {
                start_ns: i64::MIN,
                end_ns: i64::MAX
            }
        );
    }

    #[test]
    fn fetch_window_does_not_translate_a_pinned_request() {
        let request = TimeRange {
            start_ns: 95,
            end_ns: 100,
        };
        // An `@` selector asks for the same window at every step: the grid
        // around `t_outer` adds nothing, at the first step or a later one.
        for (grid_start, t_outer) in [(100, 100), (0, 100), (0, 150)] {
            assert_eq!(
                fetch_window(request, grid_start, 200, t_outer, (0, 0), true),
                request
            );
        }
        // Inside a subquery only the inner grid's extent is added.
        assert_eq!(
            fetch_window(request, 0, 200, 100, (3, 7), true),
            TimeRange {
                start_ns: 92,
                end_ns: 107
            }
        );
    }

    fn one_series_read(window: TimeRange) -> Rc<CachedSelector<SeriesData>> {
        Rc::new(CachedSelector::new(
            window,
            vec![SeriesData {
                labels: LabelSet::default(),
                samples: samples(&[window.start_ns, window.end_ns]),
            }],
        ))
    }

    #[test]
    fn a_miss_on_an_existing_key_reads_and_stores_the_union_window() {
        let cache = SelectorCache::default();
        let key = SelectorKey::new(
            &[LabelMatcher::equal("__name__", "m")],
            SelectorChannel::Float,
            0,
            None,
        );
        let first = TimeRange {
            start_ns: 100,
            end_ns: 200,
        };
        // No entry yet: the computed window is fetched as is.
        assert_eq!(cache.miss_window(&key, first), first);
        cache.insert(key.clone(), one_series_read(first));

        // Neither window contains the other.
        let second = TimeRange {
            start_ns: 0,
            end_ns: 150,
        };
        assert!(cache.lookup::<SeriesData>(&key, second).is_none());
        let union = cache.miss_window(&key, second);
        assert_eq!(
            union,
            TimeRange {
                start_ns: 0,
                end_ns: 200
            }
        );
        cache.insert(key.clone(), one_series_read(union));

        // The replacement answers both requests, so the two stop evicting
        // each other.
        for request in [first, second] {
            let cached = cache
                .lookup::<SeriesData>(&key, request)
                .expect("the union entry covers the request");
            assert_eq!(cached.window, union);
        }
        // A channel's entry does not widen the other channel's window.
        let hist_key = SelectorKey::new(
            &[LabelMatcher::equal("__name__", "m")],
            SelectorChannel::Histogram,
            0,
            None,
        );
        assert_eq!(cache.miss_window(&hist_key, second), second);
        let stats = cache.stats();
        assert_eq!((stats.fetches, stats.refetches), (2, 1));
        // The replacement's bytes stand in for the replaced entry's.
        assert_eq!(stats.bytes, one_series_read(union).bytes());
        assert_eq!(stats.fallback_keys, 0);
    }

    /// `cached.slices(request)` as `(ts_ns, value bits)` per series, so NaN
    /// payloads and -0.0 compare exactly.
    fn sliced_bits(
        cached: &CachedSelector<SeriesData>,
        request: TimeRange,
        take: usize,
    ) -> Vec<Vec<(i64, u64)>> {
        cached
            .slices(request)
            .take(take)
            .map(|(_, points)| {
                points
                    .iter()
                    .map(|p| (p.ts_ns, p.value.to_bits()))
                    .collect()
            })
            .collect()
    }

    /// The slice by two binary searches, as before the cursors.
    fn searched_bits(
        series: &[SeriesData],
        request: TimeRange,
        take: usize,
    ) -> Vec<Vec<(i64, u64)>> {
        series
            .iter()
            .take(take)
            .map(|s| {
                let lo = s.samples.partition_point(|p| p.ts_ns < request.start_ns);
                let hi = s
                    .samples
                    .partition_point(|p| p.ts_ns <= request.end_ns)
                    .max(lo);
                s.samples[lo..hi]
                    .iter()
                    .map(|p| (p.ts_ns, p.value.to_bits()))
                    .collect()
            })
            .collect()
    }

    fn series_strategy() -> impl proptest::strategy::Strategy<Value = SeriesData> {
        use proptest::prelude::*;
        // Gaps of 0 give equal timestamps; up to 400 points with gaps up to
        // 3 put a jump past the cursor walk inside the data.
        (
            -50i64..50,
            proptest::collection::vec((0i64..4, any::<u64>()), 0..400),
        )
            .prop_map(|(first, points)| {
                let mut ts = first;
                SeriesData {
                    labels: LabelSet::default(),
                    samples: points
                        .into_iter()
                        .map(|(gap, bits)| {
                            ts += gap;
                            Sample {
                                ts_ns: ts,
                                value: f64::from_bits(bits),
                            }
                        })
                        .collect(),
                }
            })
    }

    fn move_strategy() -> impl proptest::strategy::Strategy<Value = i64> {
        use proptest::prelude::*;
        prop_oneof![
            6 => 0i64..6,   // a grid step
            2 => Just(0),   // the same window again
            2 => -120i64..0, // backwards
            1 => 64i64..1_200, // a jump
        ]
    }

    proptest::proptest! {
        /// Whatever order requests arrive in (monotone, repeated, backwards,
        /// jumping, empty, outside the data, the whole window, or a request
        /// whose iterator is dropped part way), every series' slice is the
        /// two-binary-search slice, bit for bit.
        #[test]
        fn cursor_slices_equal_searched_slices_for_any_request_sequence(
            series in proptest::collection::vec(series_strategy(), 1..4),
            first_start in -200i64..200,
            requests in proptest::collection::vec(
                (move_strategy(), -5i64..80, 0usize..5, 0u8..10),
                1..80,
            ),
        ) {
            let window = TimeRange { start_ns: -1_000, end_ns: 5_000 };
            let cached = CachedSelector::new(window, series.clone());
            let mut start = first_start;
            for (delta, width, take, whole) in requests {
                start += delta;
                let request = if whole == 0 {
                    window
                } else {
                    TimeRange { start_ns: start, end_ns: start + width }
                };
                proptest::prop_assert_eq!(
                    sliced_bits(&cached, request, take),
                    searched_bits(&series, request, take),
                    "request {:?}", request
                );
            }
        }
    }

    #[test]
    fn a_monotone_range_searches_once_and_a_backwards_request_reseats_once() {
        // One series sampled every 15 s, a 5 m window stepped by 60 s.
        let second = 1_000_000_000i64;
        let ts: Vec<i64> = (0..2_000).map(|i| i * 15 * second).collect();
        let series = vec![SeriesData {
            labels: LabelSet::default(),
            samples: samples(&ts),
        }];
        let cached = CachedSelector::new(
            TimeRange {
                start_ns: 0,
                end_ns: 30_000 * second,
            },
            series.clone(),
        );
        let request = |t: i64| TimeRange {
            start_ns: (t - 300) * second,
            end_ns: t * second,
        };
        let check = |t: i64| {
            assert_eq!(
                sliced_bits(&cached, request(t), usize::MAX),
                searched_bits(&series, request(t), usize::MAX)
            );
        };
        check(600);
        assert_eq!(cached.reseats.get(), 1, "the first step seats the cursors");
        for step in 1..400 {
            check(600 + step * 60);
        }
        assert_eq!(cached.reseats.get(), 1, "no search after the first step");
        check(600 + 100 * 60);
        assert_eq!(cached.reseats.get(), 2, "a backwards request re-seats once");
        check(600 + 101 * 60);
        assert_eq!(
            cached.reseats.get(),
            2,
            "and the cursors walk on from there"
        );
    }

    #[test]
    fn a_cap_refused_replacement_releases_the_entry_it_supersedes() {
        let narrow = float_entry(1_000);
        let narrow_bytes = narrow.bytes();
        let union = float_entry(4_000);
        assert!(union.bytes() > narrow_bytes);
        // Holds the narrow entry, or the second key's, not two of them and
        // not the union.
        let cap = narrow_bytes + narrow_bytes / 2;
        assert!(union.bytes() > cap);
        let budget = Arc::new(MemoryBudget::new(u64::MAX));
        let cache = SelectorCache::new(cap, Some(Arc::clone(&budget)));
        let key = |name: &str| {
            SelectorKey::new(
                &[LabelMatcher::equal("__name__", name)],
                SelectorChannel::Float,
                0,
                None,
            )
        };

        cache.insert(key("a"), Rc::new(narrow));
        assert_eq!(cache.bytes(), narrow_bytes);
        assert_eq!(budget.reserved(), narrow_bytes);

        cache.insert(key("a"), Rc::new(union));
        assert!(cache.fell_back(&key("a")));
        assert_eq!(cache.bytes(), 0, "the superseded entry's bytes are gone");
        assert_eq!(budget.reserved(), 0, "and so is its reservation");
        let full = TimeRange {
            start_ns: 0,
            end_ns: 1_000,
        };
        assert!(cache.lookup::<SeriesData>(&key("a"), full).is_none());

        let second = float_entry(1_000);
        let second_bytes = second.bytes();
        assert!(
            narrow_bytes + second_bytes > cap,
            "fits only in the freed room"
        );
        cache.insert(key("b"), Rc::new(second));
        assert!(cache.lookup::<SeriesData>(&key("b"), full).is_some());
        assert_eq!(cache.bytes(), second_bytes);
        assert_eq!(budget.reserved(), second_bytes);

        let (cap_fallbacks, budget_fallbacks, refusal) = cache.fallbacks();
        assert_eq!((cap_fallbacks, budget_fallbacks), (1, 0));
        assert!(refusal.is_none());
        let stats = cache.stats();
        assert_eq!((stats.fetches, stats.refetches), (3, 0));
        assert_eq!(stats.fallback_keys, 1);
    }
}
