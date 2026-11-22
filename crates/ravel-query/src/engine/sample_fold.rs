//! The sample read's per-series fold (issue #3747).
//!
//! A metrics sample read decodes one run per matched series from every object
//! the series wrote to. [`RunFold`] folds each object's runs into one
//! deduplicated, timestamp-sorted sample set per series as that object's fetch
//! completes, so the read holds one label set per series and one sample per
//! `(series, ts)` slot instead of every object's decoded copy until the last
//! fetch returns. What the fold holds is charged to the engine's memory
//! budget as it grows.
//!
//! The winner at a duplicate timestamp is the greatest candidate under the
//! same total order [`merge_soa_runs`](super::merge_soa_runs) and
//! [`merge_histogram_soa_runs`](super::merge_histogram_soa_runs) use. The
//! greatest element of a set under a total order does not depend on the order
//! the set's elements are compared in, and two candidates the order ranks
//! equal carry the same priority and the same value bits, so the fold's
//! output is the same whatever order objects complete in, and the same as
//! the two merges produce over the whole pool.

use std::collections::HashMap;
use std::sync::Arc;

use ravel_promql::{HistogramSeriesData, SeriesData};
use ravel_segment::{HistogramCounts, HistogramValue};
use ravel_types::{LabelSet, Sample, SeriesId};

use super::{
    RunPriorities, histogram_sort_key, histogram_value_to_float, is_greater, retained_series_len,
};
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchedHistogramSeries, FetchedSeriesSoa};

/// `(created_unix_ns, writer_epoch, writer_seq)`: the part of a sample's
/// dedup key its object shares. The fold stores it once per distinct value.
type ProvenancePrefix = (i64, u64, u64);

/// The bytes one distinct provenance prefix is charged: its table entry and
/// its index entry.
const PREFIX_LEN: u64 = (std::mem::size_of::<ProvenancePrefix>()
    + std::mem::size_of::<(ProvenancePrefix, u32)>()) as u64;

/// A sample value the fold can rank and charge.
pub(super) trait FoldValue: Clone {
    /// Whether `(a_priority, a)` is served over `(b_priority, b)` at one
    /// `(series, ts)` slot.
    fn serves_over(
        a_priority: (i64, u64, u64, u32),
        a: &Self,
        b_priority: (i64, u64, u64, u32),
        b: &Self,
    ) -> bool;

    /// Heap bytes the value holds beyond its inline size.
    fn heap_len(&self) -> u64;
}

impl FoldValue for f64 {
    fn serves_over(
        a_priority: (i64, u64, u64, u32),
        a: &Self,
        b_priority: (i64, u64, u64, u32),
        b: &Self,
    ) -> bool {
        is_greater(
            &super::Candidate {
                value: *a,
                priority: a_priority,
            },
            &super::Candidate {
                value: *b,
                priority: b_priority,
            },
        )
    }

    fn heap_len(&self) -> u64 {
        0
    }
}

impl FoldValue for HistogramValue {
    /// [`histogram_is_greater`](super::histogram_is_greater)'s order, which
    /// compares the priority first and the structural key only on a priority
    /// tie, so the key is built only then.
    fn serves_over(
        a_priority: (i64, u64, u64, u32),
        a: &Self,
        b_priority: (i64, u64, u64, u32),
        b: &Self,
    ) -> bool {
        match a_priority.cmp(&b_priority) {
            std::cmp::Ordering::Equal => histogram_sort_key(a) > histogram_sort_key(b),
            ordering => ordering == std::cmp::Ordering::Greater,
        }
    }

    fn heap_len(&self) -> u64 {
        let spans = self.positive_spans.len() + self.negative_spans.len();
        let buckets = match &self.counts {
            HistogramCounts::Int {
                positive, negative, ..
            } => positive.len() + negative.len(),
            HistogramCounts::Float {
                positive, negative, ..
            } => positive.len() + negative.len(),
        };
        let custom = self.custom_values.as_ref().map_or(0, Vec::len);
        (spans * std::mem::size_of::<ravel_segment::HistogramSpan>()
            + (buckets + custom) * std::mem::size_of::<u64>()) as u64
    }
}

/// One held sample: its timestamp, value, and dedup priority, the priority's
/// shared prefix stored as an index into the fold's prefix table.
#[derive(Debug, Clone)]
struct FoldedSample<V> {
    ts: i64,
    prefix: u32,
    in_page_index: u32,
    value: V,
}

/// One series' held samples, as a few sorted runs, each holding at most one
/// sample per timestamp. Each run holds more than twice as many samples as the
/// run after it, so a series holds at most about `log2(samples)` runs and each
/// sample is re-merged a logarithmic number of times, whatever order its
/// objects complete in.
struct FoldedSeries<V> {
    labels: LabelSet,
    levels: Vec<Vec<FoldedSample<V>>>,
    /// The bytes `levels`' samples take, under [`sample_len`].
    sample_bytes: u64,
}

/// One decoded run, in the shape both fetched kinds share.
pub(super) struct FoldRun<V> {
    series_id: SeriesId,
    labels: LabelSet,
    timestamps: Vec<i64>,
    values: Vec<V>,
    priorities: RunPriorities,
}

fn run_priorities(
    per_sample: Option<Vec<crate::fetcher::SamplePriority>>,
    prefix: ProvenancePrefix,
) -> RunPriorities {
    match per_sample {
        Some(column) => RunPriorities::PerSample(column),
        None => RunPriorities::RunWide(prefix),
    }
}

/// A fetched run the fold bounds by reference, so [`RunFold::absorb`]
/// reserves before it converts or holds anything of its own.
pub(super) trait FoldInput<V> {
    fn series_id(&self) -> SeriesId;
    fn labels(&self) -> &LabelSet;
    fn values(&self) -> &[V];
    /// The run-wide provenance prefix, used when there is no per-sample column.
    fn prefix(&self) -> ProvenancePrefix;
    fn per_sample(&self) -> Option<&[crate::fetcher::SamplePriority]>;
    fn into_run(self) -> FoldRun<V>;
}

macro_rules! fetched_fold_input {
    ($fetched:ty, $value:ty) => {
        impl FoldInput<$value> for $fetched {
            fn series_id(&self) -> SeriesId {
                self.series_id
            }

            fn labels(&self) -> &LabelSet {
                &self.labels
            }

            fn values(&self) -> &[$value] {
                &self.values
            }

            fn prefix(&self) -> ProvenancePrefix {
                (self.created_unix_ns, self.writer_epoch, self.writer_seq)
            }

            fn per_sample(&self) -> Option<&[crate::fetcher::SamplePriority]> {
                self.per_sample_priorities.as_deref()
            }

            fn into_run(self) -> FoldRun<$value> {
                FoldRun {
                    series_id: self.series_id,
                    labels: self.labels,
                    timestamps: self.timestamps,
                    values: self.values,
                    priorities: run_priorities(
                        self.per_sample_priorities,
                        (self.created_unix_ns, self.writer_epoch, self.writer_seq),
                    ),
                }
            }
        }
    };
}

fetched_fold_input!(FetchedSeriesSoa, f64);
fetched_fold_input!(FetchedHistogramSeries, HistogramValue);

/// A sample read's matched series, folded in as each object's fetch
/// completes. See the module docs.
pub(super) struct RunFold<V> {
    by_series: HashMap<SeriesId, FoldedSeries<V>>,
    prefixes: Vec<ProvenancePrefix>,
    prefix_ids: HashMap<ProvenancePrefix, u32>,
    max_series: usize,
    /// The bytes the fold holds now, under [`RunFold::charged`]'s figures.
    retained: u64,
    budget: Arc<ravel_memory::MemoryBudget>,
    reservations: Vec<ravel_memory::Reservation>,
}

/// The scalar fold.
pub(super) type ScalarFold = RunFold<f64>;
/// The native-histogram fold.
pub(super) type HistogramFold = RunFold<HistogramValue>;

impl<V: FoldValue> RunFold<V> {
    pub(super) fn new(budget: Arc<ravel_memory::MemoryBudget>, max_series: usize) -> Self {
        Self {
            by_series: HashMap::new(),
            prefixes: Vec::new(),
            prefix_ids: HashMap::new(),
            max_series,
            retained: 0,
            budget,
            reservations: Vec::new(),
        }
    }

    /// Reserves the most folding `runs` in can make the fold hold, then
    /// folds them in, so the fold never holds more than it has reserved
    /// (issue #4118). A series past `max_series` is refused with
    /// [`QueryError::TooManySeries`] (ADR-0051 S7) the moment it would be
    /// added; a run whose priority column is not parallel to its samples, or
    /// whose timestamps descend, with the error the merge returns for it; and
    /// a refused reservation with [`FetchError::FetchMemoryExhausted`], the
    /// error a fetch reservation returns, before any of `runs` is folded.
    ///
    /// The bound is computed over `runs` in place, sorted by series so each
    /// series' incoming samples sum without a side table (issue #4198); the
    /// fold's output does not depend on the order runs are folded in.
    pub(super) fn absorb<R: FoldInput<V>>(&mut self, mut runs: Vec<R>) -> Result<(), QueryError> {
        runs.sort_unstable_by_key(FoldInput::series_id);
        let mut growth = 0u64;
        // One merge runs at a time, and the level it builds holds at most the
        // samples its series held before this call plus every sample this
        // call brings it.
        let mut merge = 0u64;
        for series in runs.chunk_by(|a, b| a.series_id() == b.series_id()) {
            let mut incoming = 0u64;
            for run in series {
                let (added, samples) = self.growth_bound(run);
                growth = growth.saturating_add(added);
                incoming = incoming.saturating_add(samples);
            }
            if let Some(run) = series.first() {
                let held = self.held_sample_len(&run.series_id());
                merge = merge.max(held.saturating_add(incoming));
            }
        }
        self.reserve_up_to(self.retained.saturating_add(growth).saturating_add(merge))?;
        for run in runs {
            self.absorb_run(run.into_run())?;
        }
        support::observe_absorb(|| self.charged());
        Ok(())
    }

    /// The most folding `run` in adds to [`RunFold::retained`], and the
    /// bytes its samples alone take. The growth counts every sample as kept,
    /// the series' labels when it is new, and one prefix entry for each change
    /// of provenance prefix along the run that the fold has not interned.
    fn growth_bound(&self, run: &impl FoldInput<V>) -> (u64, u64) {
        let inline = std::mem::size_of::<FoldedSample<V>>() as u64;
        let samples = run
            .values()
            .iter()
            .map(|value| inline.saturating_add(value.heap_len()))
            .fold(0u64, u64::saturating_add);
        let held = self.by_series.get(&run.series_id());
        let labels = if held.is_some() {
            0
        } else {
            retained_series_len(run.labels())
        };
        let prefixes = match run.per_sample() {
            None => u64::from(!self.prefix_ids.contains_key(&run.prefix())),
            Some(column) => {
                let mut last = None;
                let mut new = 0u64;
                for p in column {
                    let key = (p.created_unix_ns, p.writer_epoch, p.writer_seq);
                    if last != Some(key) && !self.prefix_ids.contains_key(&key) {
                        new += 1;
                    }
                    last = Some(key);
                }
                new
            }
        };
        (
            samples
                .saturating_add(labels)
                .saturating_add(prefixes.saturating_mul(PREFIX_LEN)),
            samples,
        )
    }

    /// The bytes the samples the fold holds for `series_id` take.
    fn held_sample_len(&self, series_id: &SeriesId) -> u64 {
        self.by_series
            .get(series_id)
            .map_or(0, |series| series.sample_bytes)
    }

    fn absorb_run(&mut self, run: FoldRun<V>) -> Result<(), QueryError> {
        if !self.by_series.contains_key(&run.series_id) && self.by_series.len() >= self.max_series {
            return Err(QueryError::TooManySeries {
                count: self.by_series.len() + 1,
                max: self.max_series,
            });
        }
        run.priorities.check_len(run.timestamps.len())?;
        let mut level: Vec<FoldedSample<V>> = Vec::with_capacity(run.timestamps.len());
        let mut last_prefix: Option<(ProvenancePrefix, u32)> = None;
        for (pos, (&ts, value)) in run.timestamps.iter().zip(run.values).enumerate() {
            let Some((created, epoch, seq, in_page_index)) = run.priorities.at(pos) else {
                return Err(QueryError::PrioritySampleCountMismatch {
                    priorities: run.priorities.column_len().unwrap_or(0),
                    samples: run.timestamps.len(),
                });
            };
            let key = (created, epoch, seq);
            let prefix = match last_prefix {
                Some((last, id)) if last == key => id,
                _ => {
                    let id = self.intern(key)?;
                    last_prefix = Some((key, id));
                    id
                }
            };
            let sample = FoldedSample {
                ts,
                prefix,
                in_page_index,
                value,
            };
            match level.last_mut() {
                Some(last) if ts < last.ts => {
                    return Err(QueryError::NonMonotonicSamples {
                        prev: last.ts,
                        next: ts,
                    });
                }
                Some(last) if ts == last.ts => {
                    if self.sample_serves_over(&sample, last) {
                        *last = sample;
                    }
                }
                _ => level.push(sample),
            }
        }
        if level.len() < level.capacity() {
            level.shrink_to_fit();
        }
        let added: u64 = level.iter().map(sample_len).sum();
        self.retained = self.retained.saturating_add(added);

        let series = match self.by_series.entry(run.series_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.retained = self
                    .retained
                    .saturating_add(retained_series_len(&run.labels));
                entry.insert(FoldedSeries {
                    labels: run.labels,
                    levels: Vec::new(),
                    sample_bytes: 0,
                })
            }
        };
        let prefixes = &self.prefixes;
        let mut dropped = 0u64;
        while let Some(last) = series.levels.last() {
            if last.len() > level.len().saturating_mul(2) {
                break;
            }
            let Some(last) = series.levels.pop() else {
                break;
            };
            level = merge_levels(prefixes, last, level, &mut dropped);
        }
        if !level.is_empty() {
            series.levels.push(level);
        }
        series.sample_bytes = series
            .sample_bytes
            .saturating_add(added)
            .saturating_sub(dropped);
        self.retained = self.retained.saturating_sub(dropped);
        Ok(())
    }

    /// The prefix table index of `prefix`, adding it if new. Each distinct
    /// prefix came from at least one sample, so a table past `u32::MAX`
    /// entries is a read past `u32::MAX` samples, refused as one.
    fn intern(&mut self, prefix: ProvenancePrefix) -> Result<u32, QueryError> {
        if let Some(&id) = self.prefix_ids.get(&prefix) {
            return Ok(id);
        }
        let id = u32::try_from(self.prefixes.len()).map_err(|_| QueryError::TooManySamples {
            count: self.prefixes.len().saturating_add(1),
            max: u32::MAX as usize,
        })?;
        self.prefixes.push(prefix);
        self.prefix_ids.insert(prefix, id);
        self.retained = self.retained.saturating_add(PREFIX_LEN);
        Ok(id)
    }

    fn sample_serves_over(&self, a: &FoldedSample<V>, b: &FoldedSample<V>) -> bool {
        serves_over(&self.prefixes, a, b)
    }

    /// Grows the fold's reservation to at least `bytes`. The reservation only
    /// grows: it is the most any absorb could have made the fold hold.
    fn reserve_up_to(&mut self, bytes: u64) -> Result<(), QueryError> {
        let charged = self.charged();
        if bytes <= charged {
            return Ok(());
        }
        let reservation =
            self.budget
                .reserve(bytes - charged)
                .map_err(|e| FetchError::FetchMemoryExhausted {
                    requested: e.requested,
                    reserved: e.reserved,
                    limit: e.limit,
                    cause: e.cause,
                })?;
        self.reservations.push(reservation);
        Ok(())
    }

    /// The bytes this fold holds reserved: the most it has held after any
    /// object was folded in. Each series is charged its id and label set
    /// ([`retained_series_len`]), each held sample its inline size plus its
    /// value's heap bytes, and each distinct provenance prefix
    /// [`PREFIX_LEN`].
    pub(super) fn charged(&self) -> u64 {
        self.reservations
            .iter()
            .map(ravel_memory::Reservation::size)
            .sum()
    }

    /// Hands the fold's reservations to the caller, which holds them for as
    /// long as it holds the series [`into_series`](RunFold::into_series)
    /// returns: those take no more than the fold reserved, since a merged
    /// sample is smaller than a held one (issue #4118).
    pub(super) fn take_reservations(&mut self) -> Vec<ravel_memory::Reservation> {
        std::mem::take(&mut self.reservations)
    }

    /// Collapses each series' runs into one, converts each held sample with
    /// `convert`, and checks `max_samples` over the whole result, counting one
    /// sample per `(series, ts)` slot as the merge does. Reservations not
    /// taken with [`take_reservations`](RunFold::take_reservations) are
    /// released when this returns.
    fn finish<T>(
        self,
        max_samples: usize,
        convert: impl Fn(FoldedSample<V>) -> T,
    ) -> Result<Vec<(LabelSet, Vec<T>)>, QueryError> {
        let Self {
            by_series,
            prefixes,
            reservations,
            ..
        } = self;
        let mut total = 0usize;
        let mut out = Vec::with_capacity(by_series.len());
        for series in by_series.into_values() {
            let mut levels = series.levels;
            let mut merged = levels.pop().unwrap_or_default();
            let mut dropped = 0u64;
            while let Some(level) = levels.pop() {
                merged = merge_levels(&prefixes, level, merged, &mut dropped);
            }
            total = total.saturating_add(merged.len());
            if total > max_samples {
                return Err(QueryError::TooManySamples {
                    count: max_samples.saturating_add(1),
                    max: max_samples,
                });
            }
            out.push((series.labels, merged.into_iter().map(&convert).collect()));
        }
        drop(reservations);
        Ok(out)
    }
}

impl RunFold<f64> {
    /// The merged series, as [`merge_soa_runs`](super::merge_soa_runs)
    /// returns them.
    pub(super) fn into_series(self, max_samples: usize) -> Result<Vec<SeriesData>, QueryError> {
        Ok(self
            .finish(max_samples, |s| Sample {
                ts_ns: s.ts,
                value: s.value,
            })?
            .into_iter()
            .map(|(labels, samples)| SeriesData { labels, samples })
            .collect())
    }
}

impl RunFold<HistogramValue> {
    /// The merged series, as
    /// [`merge_histogram_soa_runs`](super::merge_histogram_soa_runs) returns
    /// them.
    pub(super) fn into_series(
        self,
        max_samples: usize,
    ) -> Result<Vec<HistogramSeriesData>, QueryError> {
        Ok(self
            .finish(max_samples, |s| ravel_promql::HistogramSample {
                ts_ns: s.ts,
                value: histogram_value_to_float(&s.value),
            })?
            .into_iter()
            .map(|(labels, samples)| HistogramSeriesData { labels, samples })
            .collect())
    }
}

fn sample_len<V: FoldValue>(sample: &FoldedSample<V>) -> u64 {
    (std::mem::size_of::<FoldedSample<V>>() as u64).saturating_add(sample.value.heap_len())
}

fn priority<V>(prefixes: &[ProvenancePrefix], s: &FoldedSample<V>) -> (i64, u64, u64, u32) {
    let (created, epoch, seq) =
        prefixes
            .get(s.prefix as usize)
            .copied()
            .unwrap_or((i64::MIN, 0, 0));
    (created, epoch, seq, s.in_page_index)
}

fn serves_over<V: FoldValue>(
    prefixes: &[ProvenancePrefix],
    a: &FoldedSample<V>,
    b: &FoldedSample<V>,
) -> bool {
    V::serves_over(
        priority(prefixes, a),
        &a.value,
        priority(prefixes, b),
        &b.value,
    )
}

/// Merges two sorted runs of one sample per timestamp into one, keeping the
/// sample served over the other where both hold a timestamp, and adds the
/// bytes of every sample it drops to `dropped`.
fn merge_levels<V: FoldValue>(
    prefixes: &[ProvenancePrefix],
    a: Vec<FoldedSample<V>>,
    b: Vec<FoldedSample<V>>,
    dropped: &mut u64,
) -> Vec<FoldedSample<V>> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
    loop {
        let take_a = match (a.peek(), b.peek()) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(x), Some(y)) if x.ts != y.ts => x.ts < y.ts,
            (Some(x), Some(y)) => {
                let a_wins = serves_over(prefixes, x, y);
                let loser = if a_wins { b.next() } else { a.next() };
                if let Some(loser) = loser {
                    *dropped = dropped.saturating_add(sample_len(&loser));
                }
                a_wins
            }
        };
        let next = if take_a { a.next() } else { b.next() };
        if let Some(sample) = next {
            out.push(sample);
        }
    }
    // The emptied inputs still hold their buffers, which the fold charges as
    // held levels. Freed first, the shrunk copy takes bytes they released, so
    // the merge holds at most the inputs plus `out`, as `absorb` reserved.
    drop((a, b));
    if out.len() < out.capacity() {
        out.shrink_to_fit();
    }
    out
}

/// Test and bench support only (issue #4530, ADR-4523 decision 6, R5): the
/// scalar fold driven from outside the crate, the pre-registered fold shapes,
/// the hash-table size formula, and an observer of the fold's charge after
/// each absorb. Only tests, `tests/run_fold_accounting.rs` and
/// `benches/run_fold.rs` call into it; the crate itself reaches it only
/// through [`RunFold::absorb`]'s observer check, which does nothing unless an
/// observer was installed with [`set_absorb_observer`](support::set_absorb_observer).
#[doc(hidden)]
pub mod support {
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};

    use ravel_promql::SeriesData;
    use ravel_types::{Label, LabelSet, SeriesId, TypeError};

    use super::{FoldedSeries, ProvenancePrefix, ScalarFold};
    use crate::error::QueryError;
    use crate::fetcher::FetchedSeriesSoa;

    static ABSORB_OBSERVER: OnceLock<fn(u64)> = OnceLock::new();

    /// Installs `observer`, called with [`RunFold::charged`](super::RunFold::charged)
    /// after every successful absorb of every fold in the process. Returns
    /// false when an observer was already installed.
    pub fn set_absorb_observer(observer: fn(u64)) -> bool {
        ABSORB_OBSERVER.set(observer).is_ok()
    }

    pub(super) fn observe_absorb(charged: impl FnOnce() -> u64) {
        if let Some(observer) = ABSORB_OBSERVER.get() {
            observer(charged());
        }
    }

    /// A scalar fold over an unlimited budget with no series cap.
    pub struct ScalarRunFold(ScalarFold);

    impl Default for ScalarRunFold {
        fn default() -> Self {
            Self(ScalarFold::new(
                Arc::new(ravel_memory::MemoryBudget::unlimited()),
                usize::MAX,
            ))
        }
    }

    impl ScalarRunFold {
        pub fn absorb(&mut self, runs: Vec<FetchedSeriesSoa>) -> Result<(), QueryError> {
            self.0.absorb(runs)
        }

        pub fn charged(&self) -> u64 {
            self.0.charged()
        }

        pub fn into_series(self) -> Result<Vec<SeriesData>, QueryError> {
            self.0.into_series(usize::MAX)
        }
    }

    const BASE_NS: i64 = 1_700_000_000_000_000_000;
    const STEP_NS: i64 = 15_000_000_000;

    fn series_run(
        series: usize,
        slots: std::ops::Range<i64>,
        prefix: ProvenancePrefix,
    ) -> Result<FetchedSeriesSoa, TypeError> {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&(series as u64).to_le_bytes());
        let labels = LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            },
            Label {
                name: "id".to_string(),
                value: format!("s{series}"),
            },
        ])?;
        Ok(FetchedSeriesSoa {
            series_id: SeriesId(id),
            labels,
            timestamps: slots.clone().map(|k| BASE_NS + k * STEP_NS).collect(),
            values: slots
                .map(|k| (series as f64) + (k as f64) * 0.5 + (prefix.0 as f64) * 0.001)
                .collect(),
            created_unix_ns: prefix.0,
            writer_epoch: prefix.1,
            writer_seq: prefix.2,
            per_sample_priorities: None,
        })
    }

    /// Shape A: 20,000 series of 4 samples, one run per series, in one
    /// absorb. Each element of the result is one absorb call's runs.
    pub fn shape_a() -> Result<Vec<Vec<FetchedSeriesSoa>>, TypeError> {
        let runs = (0..20_000)
            .map(|s| series_run(s, 0..4, (1, 1, 1)))
            .collect::<Result<_, _>>()?;
        Ok(vec![runs])
    }

    /// Shape B: 8 series of 200,000 samples, one run per series, in one
    /// absorb.
    pub fn shape_b() -> Result<Vec<Vec<FetchedSeriesSoa>>, TypeError> {
        let runs = (0..8)
            .map(|s| series_run(s, 0..200_000, (1, 1, 1)))
            .collect::<Result<_, _>>()?;
        Ok(vec![runs])
    }

    /// Shape C: 2,000 series, 64 runs of 16 samples each, absorbed in 64
    /// calls. Call `k` holds one run per series over slots `8k..8k+16` under
    /// its own provenance prefix, so each run shares 8 timestamps with the
    /// run before it.
    pub fn shape_c() -> Result<Vec<Vec<FetchedSeriesSoa>>, TypeError> {
        (0..64i64)
            .map(|k| {
                (0..2_000)
                    .map(|s| series_run(s, 8 * k..8 * k + 16, (k + 1, 1, 1)))
                    .collect()
            })
            .collect()
    }

    /// hashbrown's group width: the SSE2 group on x86, 8 bytes elsewhere.
    const GROUP_WIDTH: u64 = if cfg!(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_feature = "sse2"
    )) {
        16
    } else {
        8
    };

    /// The bytes a `HashMap` whose [`HashMap::capacity`] is `capacity` and
    /// whose `(key, value)` slot is `slot` bytes allocates, as the R5
    /// pre-registration defines it: buckets x slot, plus one control byte
    /// per bucket and the group width. HashMap has no allocation accessor,
    /// so the bucket count is recovered from the capacity the way hashbrown
    /// computes it: the next power of two of `capacity * 8 / 7`, and 4 below
    /// a capacity of 4. A capacity of 0 is a table that never allocated.
    /// hashbrown's padding of the slot array to the control bytes' alignment
    /// is not counted; `tests/run_fold_accounting.rs` reports the formula's
    /// error against jemalloc.
    pub fn table_bytes(capacity: usize, slot: usize) -> u64 {
        if capacity == 0 {
            return 0;
        }
        let buckets = if capacity < 4 {
            4
        } else {
            (capacity.saturating_mul(8) / 7).next_power_of_two()
        } as u64;
        buckets
            .saturating_mul(slot as u64)
            .saturating_add(buckets)
            .saturating_add(GROUP_WIDTH)
    }

    /// A table of the fold's key and value types, sized for its entries up
    /// front so it allocates once, held so that allocation can be measured.
    pub struct HeldTable {
        _by_series: HashMap<SeriesId, FoldedSeries<f64>>,
        _prefix_ids: HashMap<ProvenancePrefix, u32>,
        capacity: usize,
        formula: u64,
    }

    impl HeldTable {
        /// [`HashMap::capacity`] of the held table.
        pub fn capacity(&self) -> usize {
            self.capacity
        }

        /// [`table_bytes`] for the held table.
        pub fn formula_bytes(&self) -> u64 {
            self.formula
        }
    }

    /// The fold's `by_series` table with `entries` entries whose values own
    /// no heap memory.
    pub fn by_series_table(entries: usize) -> HeldTable {
        let mut table = HashMap::with_capacity(entries);
        for i in 0..entries {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&(i as u64).to_le_bytes());
            table.insert(
                SeriesId(id),
                FoldedSeries::<f64> {
                    labels: LabelSet::default(),
                    levels: Vec::new(),
                    sample_bytes: 0,
                },
            );
        }
        let capacity = table.capacity();
        HeldTable {
            _by_series: table,
            _prefix_ids: HashMap::new(),
            capacity,
            formula: table_bytes(
                capacity,
                std::mem::size_of::<(SeriesId, FoldedSeries<f64>)>(),
            ),
        }
    }

    /// The fold's `prefix_ids` table with `entries` entries.
    pub fn prefix_ids_table(entries: usize) -> HeldTable {
        let mut table = HashMap::with_capacity(entries);
        for i in 0..entries {
            table.insert((i as i64, 1u64, 1u64), i as u32);
        }
        let capacity = table.capacity();
        HeldTable {
            _by_series: HashMap::new(),
            _prefix_ids: table,
            capacity,
            formula: table_bytes(capacity, std::mem::size_of::<(ProvenancePrefix, u32)>()),
        }
    }

    /// One R5 figure line: `R5 shape=<shape> charged=.. live=.. ratio=..
    /// gap_bytes=..`, the ratio to 4 decimals and the gap signed.
    pub fn r5_line(shape: &str, charged: u64, live: u64) -> String {
        let ratio = if live == 0 {
            f64::INFINITY
        } else {
            charged as f64 / live as f64
        };
        let gap = i128::from(charged) - i128::from(live);
        format!("R5 shape={shape} charged={charged} live={live} ratio={ratio:.4} gap_bytes={gap}")
    }

    /// The R5 presence rule over `lines`: each of `shapes` has exactly one
    /// figure line, with a nonzero charged and live figure. Returns one
    /// message per violation.
    pub fn presence_violations(lines: &[String], shapes: &[&str]) -> Vec<String> {
        let mut violations = Vec::new();
        for shape in shapes {
            let prefix = format!("R5 shape={shape} ");
            let found: Vec<&String> = lines.iter().filter(|l| l.starts_with(&prefix)).collect();
            if found.len() != 1 {
                violations.push(format!(
                    "shape {shape}: {} figure lines, want exactly 1",
                    found.len()
                ));
                continue;
            }
            for field in ["charged", "live"] {
                let value = found
                    .iter()
                    .flat_map(|l| l.split(' '))
                    .find_map(|kv| kv.strip_prefix(&format!("{field}=")).map(str::to_string))
                    .and_then(|v| v.parse::<u64>().ok());
                if !matches!(value, Some(v) if v > 0) {
                    violations.push(format!("shape {shape}: {field} is {value:?}, want > 0"));
                }
            }
        }
        violations
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;
    use ravel_promql::FloatHistogram;
    use ravel_segment::{HistogramSpan, ResetHint};
    use ravel_types::Label;

    use super::super::{merge_histogram_soa_runs, merge_soa_runs};
    use super::*;
    use crate::fetcher::SamplePriority;

    fn labels(series: u8) -> LabelSet {
        LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            },
            Label {
                name: "id".to_string(),
                value: format!("s{series}"),
            },
        ])
        .expect("valid labels")
    }

    fn scalar_run(
        series: u8,
        samples: &[(i64, f64)],
        prefix: ProvenancePrefix,
        per_sample: Option<Vec<SamplePriority>>,
    ) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([series; 16]),
            labels: labels(series),
            timestamps: samples.iter().map(|&(ts, _)| ts).collect(),
            values: samples.iter().map(|&(_, v)| v).collect(),
            created_unix_ns: prefix.0,
            writer_epoch: prefix.1,
            writer_seq: prefix.2,
            per_sample_priorities: per_sample,
        }
    }

    fn unlimited() -> Arc<ravel_memory::MemoryBudget> {
        Arc::new(ravel_memory::MemoryBudget::unlimited())
    }

    /// `(labels, [(ts, value bits)])` per series, sorted, so two results
    /// compare bit for bit whatever order their series came out in.
    fn project_scalar(series: Vec<SeriesData>) -> Vec<(LabelSet, Vec<(i64, u64)>)> {
        let mut out: Vec<_> = series
            .into_iter()
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .map(|x| (x.ts_ns, x.value.to_bits()))
                    .collect();
                (s.labels, samples)
            })
            .collect();
        out.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
        out
    }

    fn histogram_bits(h: &FloatHistogram) -> Vec<u64> {
        let mut bits = vec![
            h.scale as u64,
            h.zero_threshold.to_bits(),
            h.zero_count.to_bits(),
            h.count.to_bits(),
            h.sum.to_bits(),
        ];
        bits.extend(h.positive_buckets.iter().map(|f| f.to_bits()));
        bits.extend(h.negative_buckets.iter().map(|f| f.to_bits()));
        bits.extend(h.custom_values.iter().map(|f| f.to_bits()));
        bits
    }

    /// `(labels, [(ts, histogram field bits)])` per series.
    type HistogramProjection = Vec<(LabelSet, Vec<(i64, Vec<u64>)>)>;

    fn project_histograms(series: Vec<HistogramSeriesData>) -> HistogramProjection {
        let mut out: Vec<_> = series
            .into_iter()
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .map(|x| (x.ts_ns, histogram_bits(&x.value)))
                    .collect();
                (s.labels, samples)
            })
            .collect();
        out.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
        out
    }

    /// Values with frequent ties and every bit pattern the dedup order treats
    /// as distinct: NaN payloads, -0.0, infinities.
    fn value_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            8 => (-3i8..=3).prop_map(f64::from),
            1 => Just(-0.0f64),
            1 => Just(f64::NAN),
            1 => Just(-f64::NAN),
            1 => Just(f64::INFINITY),
        ]
    }

    /// One object's scalar runs. Small domains for series, provenance and
    /// timestamps force duplicate `(series, ts)` slots within a run, across
    /// runs of one object and across objects, with tied and untied
    /// provenance. A third of the runs carry a per-sample priority column.
    fn object_strategy() -> impl Strategy<Value = Vec<FetchedSeriesSoa>> {
        let run = (
            0u8..4,
            (0i64..3, 0u64..2, 0u64..2),
            prop::collection::vec((0i64..6, value_strategy(), 0i64..3, 0u32..3), 0..7),
            any::<bool>(),
        )
            .prop_map(|(series, prefix, mut samples, per_sample)| {
                samples.sort_by_key(|s| s.0);
                let pairs: Vec<(i64, f64)> = samples.iter().map(|s| (s.0, s.1)).collect();
                let column = per_sample.then(|| {
                    samples
                        .iter()
                        .map(|s| SamplePriority {
                            created_unix_ns: s.2,
                            writer_epoch: prefix.1,
                            writer_seq: prefix.2,
                            in_page_index: s.3,
                        })
                        .collect()
                });
                scalar_run(series, &pairs, prefix, column)
            });
        prop::collection::vec(run, 0..4)
    }

    fn histogram_value(sum: f64, count: u64) -> HistogramValue {
        HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: Some(sum),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count,
                positive: vec![count],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    fn histogram_object_strategy() -> impl Strategy<Value = Vec<FetchedHistogramSeries>> {
        let run = (
            0u8..3,
            (0i64..3, 0u64..2, 0u64..2),
            prop::collection::vec((0i64..5, value_strategy(), 0u64..3), 0..5),
        )
            .prop_map(|(series, prefix, mut samples)| {
                samples.sort_by_key(|s| s.0);
                FetchedHistogramSeries {
                    series_id: SeriesId([series; 16]),
                    labels: labels(series),
                    timestamps: samples.iter().map(|s| s.0).collect(),
                    values: samples.iter().map(|s| histogram_value(s.1, s.2)).collect(),
                    created_unix_ns: prefix.0,
                    writer_epoch: prefix.1,
                    writer_seq: prefix.2,
                    per_sample_priorities: None,
                }
            });
        prop::collection::vec(run, 0..3)
    }

    /// A permutation of `0..len` drawn from `seed`.
    fn completion_order(len: usize, seed: u64) -> Vec<usize> {
        let mut order: Vec<usize> = (0..len).collect();
        let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
        for i in (1..len).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            order.swap(i, ((state >> 33) as usize) % (i + 1));
        }
        order
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Issue #3747: folding each object's runs as it completes, in any
        /// completion order, gives exactly what `merge_soa_runs` gives over
        /// every object's runs at once, bit for bit.
        #[test]
        fn scalar_fold_equals_the_merge_in_any_completion_order(
            objects in prop::collection::vec(object_strategy(), 0..8),
            seed in any::<u64>(),
        ) {
            let want = project_scalar(
                merge_soa_runs(objects.clone(), usize::MAX, usize::MAX).expect("merge"),
            );
            let mut fold = ScalarFold::new(unlimited(), usize::MAX);
            for idx in completion_order(objects.len(), seed) {
                fold.absorb(objects[idx].clone()).expect("absorb");
            }
            let got = project_scalar(fold.into_series(usize::MAX).expect("finish"));
            prop_assert_eq!(got, want);
        }

        /// The histogram fold against `merge_histogram_soa_runs`, the same way.
        #[test]
        fn histogram_fold_equals_the_merge_in_any_completion_order(
            objects in prop::collection::vec(histogram_object_strategy(), 0..6),
            seed in any::<u64>(),
        ) {
            let want = project_histograms(
                merge_histogram_soa_runs(objects.clone(), usize::MAX, usize::MAX)
                    .expect("merge"),
            );
            let mut fold = HistogramFold::new(unlimited(), usize::MAX);
            for idx in completion_order(objects.len(), seed) {
                fold.absorb(objects[idx].clone()).expect("absorb");
            }
            let got = project_histograms(fold.into_series(usize::MAX).expect("finish"));
            prop_assert_eq!(got, want);
        }
    }

    /// A duplicate timestamp is won by the greatest candidate under the dedup
    /// order whichever object arrives first: here the later-arriving object
    /// carries the newer provenance.
    #[test]
    fn a_duplicate_timestamp_is_won_by_the_newer_write_arriving_last() {
        let old = vec![scalar_run(1, &[(10, 1.0)], (1, 1, 1), None)];
        let new = vec![scalar_run(1, &[(10, 2.0)], (2, 1, 1), None)];
        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        fold.absorb(old).expect("absorb");
        fold.absorb(new).expect("absorb");
        let series = fold.into_series(usize::MAX).expect("finish");
        assert_eq!(series[0].samples[0].value.to_bits(), 2.0f64.to_bits());
    }

    /// `max_series` and `max_samples` refuse with the merge's errors and
    /// figures.
    #[test]
    fn the_caps_refuse_as_the_merge_does() {
        let objects = vec![
            vec![scalar_run(1, &[(1, 1.0), (2, 1.0)], (1, 1, 1), None)],
            vec![scalar_run(2, &[(1, 1.0)], (1, 1, 2), None)],
        ];
        let mut fold = ScalarFold::new(unlimited(), 1);
        fold.absorb(objects[0].clone()).expect("first series fits");
        let err = fold.absorb(objects[1].clone()).expect_err("second series");
        let want = merge_soa_runs(objects.clone(), 1, usize::MAX).expect_err("merge");
        assert_eq!(format!("{err:?}"), format!("{want:?}"));

        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        for object in &objects {
            fold.absorb(object.clone()).expect("absorb");
        }
        let err = fold.into_series(2).expect_err("three samples");
        let want = merge_soa_runs(objects, usize::MAX, 2).expect_err("merge");
        assert_eq!(format!("{err:?}"), format!("{want:?}"));
    }

    /// Issue #3747: the bytes held for a known result are its series' ids
    /// and label sets, its held samples and its distinct provenance prefixes.
    /// Two objects carry the same series at the same three timestamps under
    /// two provenance prefixes; the merged result is one series of three
    /// samples, and the second object's copies are dropped, so what is held
    /// is one series, three samples and two prefixes, not one copy per object.
    ///
    /// Issue #4118: each absorb reserves before it folds: every incoming
    /// sample, the labels of a new series, one prefix per unseen provenance
    /// prefix, and the level the merge builds beside the levels it replaces.
    #[test]
    fn the_fold_reserves_what_an_absorb_can_hold_before_holding_it() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let samples = [(1, 1.0), (2, 2.0), (3, 3.0)];
        fold.absorb(vec![scalar_run(7, &samples, (1, 1, 1), None)])
            .expect("absorb");

        let label = std::mem::size_of::<ravel_types::Label>() as u64;
        let series_len = std::mem::size_of::<SeriesId>() as u64
            + std::mem::size_of::<LabelSet>() as u64
            + (label + "__name__".len() as u64 + "m".len() as u64)
            + (label + "id".len() as u64 + "s7".len() as u64);
        // ts (8) + prefix index (4) + in-page index (4) + value (8).
        let sample_len = 24u64;
        assert_eq!(std::mem::size_of::<FoldedSample<f64>>() as u64, sample_len);
        // The prefix table entry (24) and its index entry (32).
        let prefix_len = 56u64;
        assert_eq!(PREFIX_LEN, prefix_len);

        let one_object = series_len + 3 * sample_len + prefix_len;
        assert_eq!(fold.retained, one_object);
        // The merge level of the first absorb is the three incoming samples.
        let first_bound = one_object + 3 * sample_len;
        assert_eq!(fold.charged(), first_bound);
        assert_eq!(budget.reserved(), first_bound);

        fold.absorb(vec![scalar_run(7, &samples, (2, 1, 1), None)])
            .expect("absorb");
        let merged = series_len + 3 * sample_len + 2 * prefix_len;
        assert_eq!(fold.retained, merged);
        // Before the second absorb the fold held `one_object`; it can add
        // three samples and one prefix, and its merge builds a level of the
        // three held samples and the three incoming ones.
        let second_bound = one_object + (3 * sample_len + prefix_len) + 6 * sample_len;
        assert_eq!(fold.charged(), second_bound);
        assert_eq!(budget.reserved(), second_bound);

        let series = fold.into_series(usize::MAX).expect("finish");
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 3);
        assert_eq!(budget.reserved(), 0, "released once the result is built");
    }

    /// A run that records the budget's reservation when the fold converts it.
    struct Observed {
        run: FetchedSeriesSoa,
        budget: Arc<ravel_memory::MemoryBudget>,
        seen: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl FoldInput<f64> for Observed {
        fn series_id(&self) -> SeriesId {
            self.run.series_id()
        }
        fn labels(&self) -> &LabelSet {
            self.run.labels()
        }
        fn values(&self) -> &[f64] {
            self.run.values()
        }
        fn prefix(&self) -> ProvenancePrefix {
            self.run.prefix()
        }
        fn per_sample(&self) -> Option<&[SamplePriority]> {
            self.run.per_sample()
        }
        fn into_run(self) -> FoldRun<f64> {
            self.seen.lock().expect("lock").push(self.budget.reserved());
            self.run.into_run()
        }
    }

    /// Issue #4198: an absorb bounds its runs where they lie and reserves
    /// before it converts the first of them, so every conversion sees the
    /// whole call's reservation already held.
    #[test]
    fn an_absorb_reserves_before_it_converts_a_run() {
        let budget = unlimited();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = |series, prefix| Observed {
            run: scalar_run(series, &[(1, 1.0), (2, 2.0)], prefix, None),
            budget: Arc::clone(&budget),
            seen: Arc::clone(&seen),
        };
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(vec![
            observed(2, (1, 1, 1)),
            observed(1, (1, 1, 1)),
            observed(2, (2, 1, 1)),
        ])
        .expect("absorb");
        let charged = fold.charged();
        assert!(charged > 0);
        assert_eq!(*seen.lock().expect("lock"), vec![charged; 3]);
    }

    /// Reservations taken from the fold stay held after its result is built,
    /// until the caller drops them (issue #4118).
    #[test]
    fn taken_reservations_outlive_the_result() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(vec![scalar_run(7, &[(1, 1.0)], (1, 1, 1), None)])
            .expect("absorb");
        let charged = fold.charged();
        assert!(charged > 0);
        let held = fold.take_reservations();
        let series = fold.into_series(usize::MAX).expect("finish");
        assert_eq!(series.len(), 1);
        assert_eq!(budget.reserved(), charged, "held with the series");
        drop(held);
        assert_eq!(budget.reserved(), 0);
    }

    /// An absorb whose bound does not fit is refused before it folds a run.
    #[test]
    fn an_absorb_over_the_budget_is_refused_before_folding() {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(64));
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let err = fold
            .absorb(vec![scalar_run(7, &[(1, 1.0), (2, 2.0)], (1, 1, 1), None)])
            .expect_err("the bound exceeds 64 B");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted { limit: 64, .. })
            ),
            "{err:?}"
        );
        assert_eq!(fold.retained, 0, "nothing was folded");
        assert!(fold.by_series.is_empty());
        assert_eq!(budget.reserved(), 0);
    }

    /// A fold dropped before its result is built, as on a failed query,
    /// releases what it reserved.
    #[test]
    fn a_dropped_fold_releases_its_reservation() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(vec![scalar_run(1, &[(1, 1.0)], (1, 1, 1), None)])
            .expect("absorb");
        assert!(budget.reserved() > 0);
        drop(fold);
        assert_eq!(budget.reserved(), 0);
    }

    /// What a scalar fold's containers hold allocated, by part, under the R5
    /// pre-registration's definition (issue #4530): each Vec's
    /// `capacity() x size_of::<T>()`, and each hash table as
    /// [`support::table_bytes`] computes it from `HashMap::capacity()` (buckets
    /// x slot plus one control byte per bucket and the group width, the
    /// bucket count derived as hashbrown derives it). A label set's outer Vec
    /// is counted at its length, since `LabelSet` exposes no capacity; the
    /// fixtures build it with `vec!`, whose capacity is its length.
    struct LiveBytes {
        levels: u64,
        spines: u64,
        by_series: u64,
        prefix_ids: u64,
        prefixes: u64,
        labels: u64,
    }

    impl LiveBytes {
        fn total(&self) -> u64 {
            self.levels + self.spines + self.by_series + self.prefix_ids + self.prefixes + self.labels
        }
    }

    fn live_bytes(fold: &ScalarFold) -> LiveBytes {
        let sample = std::mem::size_of::<FoldedSample<f64>>() as u64;
        let level = std::mem::size_of::<Vec<FoldedSample<f64>>>() as u64;
        let label = std::mem::size_of::<Label>() as u64;
        let mut live = LiveBytes {
            levels: 0,
            spines: 0,
            by_series: support::table_bytes(
                fold.by_series.capacity(),
                std::mem::size_of::<(SeriesId, FoldedSeries<f64>)>(),
            ),
            prefix_ids: support::table_bytes(
                fold.prefix_ids.capacity(),
                std::mem::size_of::<(ProvenancePrefix, u32)>(),
            ),
            prefixes: fold.prefixes.capacity() as u64
                * std::mem::size_of::<ProvenancePrefix>() as u64,
            labels: 0,
        };
        for series in fold.by_series.values() {
            live.spines += series.levels.capacity() as u64 * level;
            for held in &series.levels {
                live.levels += held.capacity() as u64 * sample
                    + held.iter().map(|s| s.value.heap_len()).sum::<u64>();
            }
            for l in series.labels.iter() {
                live.labels += label + l.name.capacity() as u64 + l.value.capacity() as u64;
            }
        }
        live
    }

    /// Folds `calls` one absorb each and returns the result with, at the
    /// fold's peak charge, the charged and live figures and the live split.
    fn probed_fold(calls: Vec<Vec<FetchedSeriesSoa>>) -> (Vec<SeriesData>, u64, LiveBytes) {
        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        let mut peak: Option<(u64, LiveBytes)> = None;
        for call in calls {
            fold.absorb(call).expect("absorb");
            let charged = fold.charged();
            if peak.as_ref().is_none_or(|(c, _)| charged >= *c) {
                peak = Some((charged, live_bytes(&fold)));
            }
        }
        let (charged, live) = peak.expect("at least one absorb");
        (fold.into_series(usize::MAX).expect("finish"), charged, live)
    }

    fn plain_fold(calls: Vec<Vec<FetchedSeriesSoa>>) -> Vec<SeriesData> {
        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        for call in calls {
            fold.absorb(call).expect("absorb");
        }
        fold.into_series(usize::MAX).expect("finish")
    }

    fn emit(lines: &mut Vec<String>, line: String) {
        println!("{line}");
        lines.push(line);
    }

    /// Issue #4530 (ADR-4523 R5, step 1): the fold's charged bytes against
    /// the bytes its containers hold, at its peak charge, for pre-registered
    /// shapes A, B and C. The predicted bands are what the figures are
    /// measured against, not a pass condition: this asserts only that each
    /// shape reports exactly one nonzero figure line and that probing the
    /// fold leaves its result unchanged.
    #[test]
    fn r5_run_fold_charged_against_live_bytes() {
        let shapes = [
            ("A", support::shape_a().expect("shape A")),
            ("B", support::shape_b().expect("shape B")),
            ("C", support::shape_c().expect("shape C")),
        ];
        let mut lines = Vec::new();
        for (shape, calls) in shapes {
            let (probed, charged, live) = probed_fold(calls.clone());
            emit(&mut lines, support::r5_line(shape, charged, live.total()));
            println!(
                "R5 breakdown shape={shape} levels={} spines={} by_series={} prefix_ids={} \
                 prefixes={} labels={}",
                live.levels, live.spines, live.by_series, live.prefix_ids, live.prefixes, live.labels
            );
            assert_eq!(
                project_scalar(probed),
                project_scalar(plain_fold(calls)),
                "shape {shape}: probing changed the fold's result"
            );
        }
        let violations = support::presence_violations(&lines, &["A", "B", "C"]);
        assert!(violations.is_empty(), "R5 presence rule: {violations:?}");
    }
}
