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

impl From<FetchedSeriesSoa> for FoldRun<f64> {
    fn from(fs: FetchedSeriesSoa) -> Self {
        Self {
            series_id: fs.series_id,
            labels: fs.labels,
            timestamps: fs.timestamps,
            values: fs.values,
            priorities: run_priorities(
                fs.per_sample_priorities,
                (fs.created_unix_ns, fs.writer_epoch, fs.writer_seq),
            ),
        }
    }
}

impl From<FetchedHistogramSeries> for FoldRun<HistogramValue> {
    fn from(fs: FetchedHistogramSeries) -> Self {
        Self {
            series_id: fs.series_id,
            labels: fs.labels,
            timestamps: fs.timestamps,
            values: fs.values,
            priorities: run_priorities(
                fs.per_sample_priorities,
                (fs.created_unix_ns, fs.writer_epoch, fs.writer_seq),
            ),
        }
    }
}

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

    /// Folds one object's runs in, then reserves whatever the fold now holds
    /// beyond what it has already reserved. A series past `max_series` is
    /// refused with [`QueryError::TooManySeries`] (ADR-0051 S7) the moment it
    /// would be added; a run whose priority column is not parallel to its
    /// samples, or whose timestamps descend, with the error the merge returns
    /// for it; and a refused reservation with
    /// [`FetchError::FetchMemoryExhausted`], the error a fetch reservation
    /// returns.
    pub(super) fn absorb<R: Into<FoldRun<V>>>(
        &mut self,
        runs: impl IntoIterator<Item = R>,
    ) -> Result<(), QueryError> {
        for run in runs {
            self.absorb_run(run.into())?;
        }
        self.reserve_retained()
    }

    fn absorb_run(&mut self, run: FoldRun<V>) -> Result<(), QueryError> {
        if !self.by_series.contains_key(&run.series_id) && self.by_series.len() >= self.max_series
        {
            return Err(QueryError::TooManySeries {
                count: self.by_series.len() + 1,
                max: self.max_series,
            });
        }
        run.priorities.check_len(run.timestamps.len())?;
        let mut level: Vec<FoldedSample<V>> = Vec::with_capacity(run.timestamps.len());
        for (pos, (&ts, value)) in run.timestamps.iter().zip(run.values).enumerate() {
            let Some((created, epoch, seq, in_page_index)) = run.priorities.at(pos) else {
                return Err(QueryError::PrioritySampleCountMismatch {
                    priorities: run.priorities.column_len().unwrap_or(0),
                    samples: run.timestamps.len(),
                });
            };
            let prefix = self.intern((created, epoch, seq));
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
        self.retained = self.retained.saturating_sub(dropped);
        Ok(())
    }

    fn intern(&mut self, prefix: ProvenancePrefix) -> u32 {
        if let Some(&id) = self.prefix_ids.get(&prefix) {
            return id;
        }
        let id = u32::try_from(self.prefixes.len()).unwrap_or(u32::MAX);
        self.prefixes.push(prefix);
        self.prefix_ids.insert(prefix, id);
        self.retained = self.retained.saturating_add(PREFIX_LEN);
        id
    }

    fn sample_serves_over(&self, a: &FoldedSample<V>, b: &FoldedSample<V>) -> bool {
        serves_over(&self.prefixes, a, b)
    }

    fn reserve_retained(&mut self) -> Result<(), QueryError> {
        let charged = self.charged();
        if self.retained <= charged {
            return Ok(());
        }
        let reservation = self
            .budget
            .reserve(self.retained - charged)
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

    /// Collapses each series' runs into one and checks `max_samples` over the
    /// whole result, counting one sample per `(series, ts)` slot as the merge
    /// does. The reservations are released when this returns: the caller owns
    /// the samples from here.
    fn finish(self, max_samples: usize) -> Result<Vec<(LabelSet, Vec<FoldedSample<V>>)>, QueryError> {
        let prefixes = self.prefixes;
        let mut total = 0usize;
        let mut out = Vec::with_capacity(self.by_series.len());
        for series in self.by_series.into_values() {
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
            out.push((series.labels, merged));
        }
        Ok(out)
    }
}

impl RunFold<f64> {
    /// The merged series, as [`merge_soa_runs`](super::merge_soa_runs)
    /// returns them.
    pub(super) fn into_series(self, max_samples: usize) -> Result<Vec<SeriesData>, QueryError> {
        Ok(self
            .finish(max_samples)?
            .into_iter()
            .map(|(labels, samples)| SeriesData {
                labels,
                samples: samples
                    .into_iter()
                    .map(|s| Sample {
                        ts_ns: s.ts,
                        value: s.value,
                    })
                    .collect(),
            })
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
            .finish(max_samples)?
            .into_iter()
            .map(|(labels, samples)| HistogramSeriesData {
                labels,
                samples: samples
                    .into_iter()
                    .map(|s| ravel_promql::HistogramSample {
                        ts_ns: s.ts,
                        value: histogram_value_to_float(&s.value),
                    })
                    .collect(),
            })
            .collect())
    }
}

fn sample_len<V: FoldValue>(sample: &FoldedSample<V>) -> u64 {
    (std::mem::size_of::<FoldedSample<V>>() as u64).saturating_add(sample.value.heap_len())
}

fn priority<V>(prefixes: &[ProvenancePrefix], s: &FoldedSample<V>) -> (i64, u64, u64, u32) {
    let (created, epoch, seq) = prefixes
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
    V::serves_over(priority(prefixes, a), &a.value, priority(prefixes, b), &b.value)
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
    out
}
