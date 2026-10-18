//! Per-query selector fetch cache (ADR-3072 decision 1b).
//!
//! A compound range query (an aggregate or binary expression at the top of a
//! range query, or any expression inside a subquery) is evaluated once per
//! grid step, and each step reaches its selectors through
//! `eval_vector_selector`, `eval_matrix_selector` and
//! `eval_histogram_matrix_selector`. Without this cache every one of those
//! calls is a fresh `SeriesSource::query` over that step's own window. With
//! it, the first call for a selector fetches the request window widened by
//! how far the enclosing grids can still move it, and every later step whose
//! window lies inside that fetch is answered by slicing the cached series.
//!
//! Slicing is exact: a source answers a window with every matching sample
//! whose timestamp is inside it, inclusive on both ends (`TimeRange`'s
//! contract), so the inclusive `partition_point` slice of a superset fetch is
//! the same sample sequence the source would have returned for the narrower
//! window. Every helper then applies its own filters (lookback pick,
//! left-open matrix bound, staleness) to that slice unchanged.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use ravel_types::{Sample, TimeRange};

use crate::source::{
    HistogramSample, HistogramSeriesData, LabelMatcher, MatchOp, SeriesData, SourceError,
};

/// Which read channel a cached fetch answers: `SeriesSource::query` or
/// `SeriesSource::query_histograms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SelectorChannel {
    Float,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MatcherKind {
    Eq,
    Ne,
    Re,
    Nre,
}

/// Canonical identity of one selector fetch: the matcher set (label name,
/// operator kind, value or regex pattern text; sorted, so matcher order in
/// the query text does not split entries), the selector's own `offset` and
/// resolved `@` instant, and the read channel. `LabelMatcher` has no `Hash`
/// (it carries a compiled `Regex`), so the key holds the text instead.
///
/// The offset and `@` are part of the identity because they shift every
/// request window of the selector by a constant: `up + up offset 5m` asks
/// for two windows that slide side by side across the grid, and one entry
/// per shift keeps each covered after its single fetch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelectorKey {
    matchers: Vec<(String, MatcherKind, String)>,
    offset_ns: i64,
    at_ns: Option<i64>,
    channel: SelectorChannel,
}

impl SelectorKey {
    fn new(matchers: &[LabelMatcher], shift: SelectorShift, channel: SelectorChannel) -> Self {
        let mut canonical: Vec<(String, MatcherKind, String)> = matchers
            .iter()
            .map(|m| {
                let (kind, text) = match &m.op {
                    MatchOp::Eq => (MatcherKind::Eq, m.value.clone()),
                    MatchOp::Ne => (MatcherKind::Ne, m.value.clone()),
                    MatchOp::Re(re) => (MatcherKind::Re, re.as_str().to_string()),
                    MatchOp::Nre(re) => (MatcherKind::Nre, re.as_str().to_string()),
                };
                (m.name.clone(), kind, text)
            })
            .collect();
        canonical.sort_unstable();
        canonical.dedup();
        SelectorKey {
            matchers: canonical,
            offset_ns: shift.offset_ns,
            at_ns: shift.at_ns,
            channel,
        }
    }
}

/// A selector's time modifiers, resolved: the signed `offset` in
/// nanoseconds and the absolute `@` instant, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectorShift {
    pub(crate) offset_ns: i64,
    pub(crate) at_ns: Option<i64>,
}

/// One fetch of a selector: the window it covered and the series the source
/// returned for it, in the source's order.
#[derive(Debug)]
pub(crate) struct CachedSelector<S> {
    window: TimeRange,
    pub(crate) series: Vec<S>,
}

impl<S> CachedSelector<S> {
    /// The samples of one cached series inside `request`, inclusive on both
    /// ends. A fetch made for exactly `request` is returned whole, so a
    /// helper reading outside any grid sees the source's answer untouched.
    pub(crate) fn samples_in<'a, T: Timestamped>(
        &self,
        samples: &'a [T],
        request: TimeRange,
    ) -> &'a [T] {
        if self.window == request {
            return samples;
        }
        let lo = samples.partition_point(|s| s.ts_ns() < request.start_ns);
        let hi = samples.partition_point(|s| s.ts_ns() <= request.end_ns);
        samples.get(lo..hi).unwrap_or(&[])
    }
}

/// A sample type the cache can slice by timestamp.
pub(crate) trait Timestamped {
    fn ts_ns(&self) -> i64;
}

impl Timestamped for Sample {
    fn ts_ns(&self) -> i64 {
        self.ts_ns
    }
}

impl Timestamped for HistogramSample {
    fn ts_ns(&self) -> i64 {
        self.ts_ns
    }
}

#[derive(Debug)]
pub(crate) enum CachedEntry {
    Float(Rc<CachedSelector<SeriesData>>),
    Histogram(Rc<CachedSelector<HistogramSeriesData>>),
}

/// Ties a series type to its channel and its variant of [`CachedEntry`].
pub(crate) trait ChannelSeries: Sized {
    const CHANNEL: SelectorChannel;
    fn wrap(entry: Rc<CachedSelector<Self>>) -> CachedEntry;
    fn project(entry: &CachedEntry) -> Option<&Rc<CachedSelector<Self>>>;
}

impl ChannelSeries for SeriesData {
    const CHANNEL: SelectorChannel = SelectorChannel::Float;
    fn wrap(entry: Rc<CachedSelector<Self>>) -> CachedEntry {
        CachedEntry::Float(entry)
    }
    fn project(entry: &CachedEntry) -> Option<&Rc<CachedSelector<Self>>> {
        match entry {
            CachedEntry::Float(e) => Some(e),
            CachedEntry::Histogram(_) => None,
        }
    }
}

impl ChannelSeries for HistogramSeriesData {
    const CHANNEL: SelectorChannel = SelectorChannel::Histogram;
    fn wrap(entry: Rc<CachedSelector<Self>>) -> CachedEntry {
        CachedEntry::Histogram(entry)
    }
    fn project(entry: &CachedEntry) -> Option<&Rc<CachedSelector<Self>>> {
        match entry {
            CachedEntry::Histogram(e) => Some(e),
            CachedEntry::Float(_) => None,
        }
    }
}

/// How far the request window of the selector being evaluated can still move
/// across the grids enclosing this evaluation: `back_ns` towards earlier
/// steps, `fwd_ns` towards later ones. Each enclosing grid adds the distance
/// from its current step to its own first and last step. For a selector
/// directly under a range query's grid that is `(t - start, end - t)`; inside
/// a subquery it is the inner grid's distances plus the outer grid's, since
/// the outer step moves the whole inner grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GridReach {
    back_ns: i64,
    fwd_ns: i64,
}

/// The cache itself, one per query (it lives on `QueryWindow`).
#[derive(Debug)]
pub(crate) struct SelectorCache {
    enabled: bool,
    /// `None` outside every grid: an instant query's own top level reads
    /// straight from the source, as it did before this cache existed.
    reach: Cell<Option<GridReach>>,
    entries: RefCell<HashMap<SelectorKey, CachedEntry>>,
    fetches: Cell<u64>,
    refetches: Cell<u64>,
}

impl SelectorCache {
    pub(crate) fn new(enabled: bool) -> Self {
        SelectorCache {
            enabled,
            reach: Cell::new(None),
            entries: RefCell::new(HashMap::new()),
            fetches: Cell::new(0),
            refetches: Cell::new(0),
        }
    }

    /// Run `step` as the evaluation at `t_ns` of a grid spanning
    /// `[grid_start_ns, grid_end_ns]`, nested inside whatever grids are
    /// already active.
    pub(crate) fn at_grid_step<T>(
        &self,
        grid_start_ns: i64,
        grid_end_ns: i64,
        t_ns: i64,
        step: impl FnOnce() -> T,
    ) -> T {
        let outer = self.reach.get();
        let base = outer.unwrap_or(GridReach {
            back_ns: 0,
            fwd_ns: 0,
        });
        self.reach.set(Some(GridReach {
            back_ns: base
                .back_ns
                .saturating_add(t_ns.saturating_sub(grid_start_ns).max(0)),
            fwd_ns: base
                .fwd_ns
                .saturating_add(grid_end_ns.saturating_sub(t_ns).max(0)),
        }));
        let out = step();
        self.reach.set(outer);
        out
    }

    /// `(fetches, refetches)` this query has issued through the cache: a
    /// fetch is any source call made for a cached selector, a refetch is one
    /// that replaced an entry whose window did not cover the request.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> (u64, u64) {
        (self.fetches.get(), self.refetches.get())
    }

    /// The series `fetch` returns for `request`, read through the cache.
    ///
    /// Outside every grid (or with the cache disabled) this is one direct
    /// call for exactly `request`. Inside a grid an entry whose window covers
    /// `request` is returned without a source call; otherwise the request is
    /// widened by the current grid reach (and by the previous entry's window,
    /// if one existed, which counts as a refetch), fetched once, and stored.
    pub(crate) fn read<S: ChannelSeries>(
        &self,
        matchers: &[LabelMatcher],
        shift: SelectorShift,
        request: TimeRange,
        fetch: impl FnOnce(&[LabelMatcher], TimeRange) -> Result<Vec<S>, SourceError>,
    ) -> Result<Rc<CachedSelector<S>>, SourceError> {
        let reach = match self.reach.get() {
            Some(reach) if self.enabled => reach,
            _ => {
                return Ok(Rc::new(CachedSelector {
                    window: request,
                    series: fetch(matchers, request)?,
                }));
            }
        };
        let key = SelectorKey::new(matchers, shift, S::CHANNEL);
        let previous = {
            let entries = self.entries.borrow();
            entries.get(&key).and_then(S::project).map(Rc::clone)
        };
        let mut window = TimeRange {
            start_ns: request.start_ns.saturating_sub(reach.back_ns),
            end_ns: request.end_ns.saturating_add(reach.fwd_ns),
        };
        if let Some(entry) = &previous {
            if entry.window.start_ns <= request.start_ns && request.end_ns <= entry.window.end_ns {
                return Ok(Rc::clone(entry));
            }
            // Keep what the old entry covered so two request shapes sharing a
            // key (one selector under two subqueries with different offsets)
            // cannot evict each other on alternate steps.
            window.start_ns = window.start_ns.min(entry.window.start_ns);
            window.end_ns = window.end_ns.max(entry.window.end_ns);
            self.refetches.set(self.refetches.get().saturating_add(1));
        }
        self.fetches.set(self.fetches.get().saturating_add(1));
        let entry = Rc::new(CachedSelector {
            window,
            series: fetch(matchers, window)?,
        });
        self.entries
            .borrow_mut()
            .insert(key, S::wrap(Rc::clone(&entry)));
        Ok(entry)
    }
}

