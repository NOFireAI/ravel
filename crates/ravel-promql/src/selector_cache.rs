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
/// each covering its own selector's whole grid, instead of one entry the two
/// selectors keep replacing.
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

    /// Store a fresh read under `key`, replacing any previous entry.
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

/// The window a cache miss fetches for `request`: the request translated
/// across the rest of the top-level grid, `[request.start - (t_outer -
/// grid_start), request.end + (grid_end - t_outer)]`, widened further by
/// `inner_pad` (the enclosing subquery grids' extent before and after the
/// current inner instant) so one fetch covers the whole inner grid as it
/// moves with the outer step. Saturating, and never narrower than `request`.
pub(crate) fn fetch_window(
    request: TimeRange,
    grid_start_ns: i64,
    grid_end_ns: i64,
    t_outer: i64,
    inner_pad: (i64, i64),
) -> TimeRange {
    let before = t_outer
        .saturating_sub(grid_start_ns)
        .max(0)
        .saturating_add(inner_pad.0.max(0));
    let after = grid_end_ns
        .saturating_sub(t_outer)
        .max(0)
        .saturating_add(inner_pad.1.max(0));
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
            fetch_window(request, 100, 200, 100, (0, 0)),
            TimeRange {
                start_ns: 95,
                end_ns: 200
            }
        );
        // A later step: backward to the grid start too.
        assert_eq!(
            fetch_window(request, 0, 200, 100, (0, 0)),
            TimeRange {
                start_ns: -5,
                end_ns: 200
            }
        );
        // Inside a subquery the inner grid's extent is added on both sides.
        assert_eq!(
            fetch_window(request, 100, 200, 100, (3, 7)),
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
                (i64::MAX, i64::MAX)
            ),
            TimeRange {
                start_ns: i64::MIN,
                end_ns: i64::MAX
            }
        );
    }
}
