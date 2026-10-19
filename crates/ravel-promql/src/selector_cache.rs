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

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use ravel_types::{LabelSet, Sample, TimeRange};

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

/// The series one selector read returned, in source order, and the window
/// that read covered.
#[derive(Debug)]
pub(crate) struct CachedSelector<S> {
    pub(crate) window: TimeRange,
    pub(crate) series: Vec<S>,
}

impl<S: CachedSeries> CachedSelector<S> {
    /// Every series' points inside `request`, inclusive on both ends like
    /// `TimeRange`, so equal timestamps at either bound are kept. Series
    /// order is the source's. `request` must lie inside [`Self::window`]. A
    /// read over exactly `request` is returned whole, since the source
    /// already clipped it.
    pub(crate) fn slices(
        &self,
        request: TimeRange,
    ) -> impl Iterator<Item = (&LabelSet, &[S::Point])> + '_ {
        let whole = request == self.window;
        self.series.iter().map(move |s| {
            let points = s.points();
            if whole {
                return (s.labels(), points);
            }
            let lo = points.partition_point(|p| S::ts_ns(p) < request.start_ns);
            let hi = points
                .partition_point(|p| S::ts_ns(p) <= request.end_ns)
                .max(lo);
            (s.labels(), &points[lo..hi])
        })
    }
}

pub(crate) enum CacheEntry {
    Float(Rc<CachedSelector<SeriesData>>),
    Histogram(Rc<CachedSelector<HistogramSeriesData>>),
}

impl CacheEntry {
    fn window(&self) -> TimeRange {
        match self {
            CacheEntry::Float(cached) => cached.window,
            CacheEntry::Histogram(cached) => cached.window,
        }
    }
}

/// Source reads the cache issued over one query.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectorCacheStats {
    /// Every `SeriesSource` read, either channel.
    pub(crate) fetches: u64,
    /// The reads that replaced an existing entry because a request fell
    /// outside its window.
    pub(crate) refetches: u64,
}

#[derive(Default)]
pub(crate) struct SelectorCache {
    entries: RefCell<HashMap<SelectorKey, CacheEntry>>,
    fetches: Cell<u64>,
    refetches: Cell<u64>,
}

impl SelectorCache {
    /// The entry for `key` when its window covers `request`.
    pub(crate) fn lookup<S: CachedSeries>(
        &self,
        key: &SelectorKey,
        request: TimeRange,
    ) -> Option<Rc<CachedSelector<S>>> {
        let entries = self.entries.borrow();
        let cached = S::from_entry(entries.get(key)?)?;
        (cached.window.start_ns <= request.start_ns && request.end_ns <= cached.window.end_ns)
            .then(|| Rc::clone(cached))
    }

    /// The window a miss on `key` fetches: `computed` when `key` has no
    /// entry, otherwise the smallest window containing both `computed` and
    /// the entry's window, so the read that replaces the entry still answers
    /// every request the entry did.
    pub(crate) fn miss_window(&self, key: &SelectorKey, computed: TimeRange) -> TimeRange {
        match self.entries.borrow().get(key) {
            Some(entry) => {
                let old = entry.window();
                TimeRange {
                    start_ns: old.start_ns.min(computed.start_ns),
                    end_ns: old.end_ns.max(computed.end_ns),
                }
            }
            None => computed,
        }
    }

    /// Store a fresh read under `key`, replacing any previous entry. A miss
    /// reads over [`Self::miss_window`], so a replacement covers the window
    /// of the entry it replaces.
    pub(crate) fn insert<S: CachedSeries>(&self, key: SelectorKey, cached: Rc<CachedSelector<S>>) {
        self.fetches.set(self.fetches.get().saturating_add(1));
        if self
            .entries
            .borrow_mut()
            .insert(key, S::into_entry(cached))
            .is_some()
        {
            self.refetches.set(self.refetches.get().saturating_add(1));
        }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> SelectorCacheStats {
        SelectorCacheStats {
            fetches: self.fetches.get(),
            refetches: self.refetches.get(),
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

    #[test]
    fn slices_are_inclusive_on_both_ends_and_keep_equal_timestamps() {
        let cached = CachedSelector {
            window: TimeRange {
                start_ns: 0,
                end_ns: 100,
            },
            series: vec![SeriesData {
                labels: LabelSet::default(),
                samples: samples(&[5, 10, 10, 20, 30, 30, 40]),
            }],
        };
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
        Rc::new(CachedSelector {
            window,
            series: vec![SeriesData {
                labels: LabelSet::default(),
                samples: samples(&[window.start_ns, window.end_ns]),
            }],
        })
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
        assert_eq!(
            cache.stats(),
            SelectorCacheStats {
                fetches: 2,
                refetches: 1
            }
        );
    }
}
