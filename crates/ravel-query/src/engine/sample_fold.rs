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

    fn reserve_retained(&mut self) -> Result<(), QueryError> {
        let charged = self.charged();
        if self.retained <= charged {
            return Ok(());
        }
        let reservation = self.budget.reserve(self.retained - charged).map_err(|e| {
            FetchError::FetchMemoryExhausted {
                requested: e.requested,
                reserved: e.reserved,
                limit: e.limit,
                cause: e.cause,
            }
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

    /// Collapses each series' runs into one, converts each held sample with
    /// `convert`, and checks `max_samples` over the whole result, counting one
    /// sample per `(series, ts)` slot as the merge does. The reservations are
    /// released when this returns: the caller owns the samples from here.
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
    if out.len() < out.capacity() {
        out.shrink_to_fit();
    }
    out
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

    /// Issue #3747: the bytes reserved for a known result are its series'
    /// ids and label sets, its held samples and its distinct provenance
    /// prefixes. Two objects carry the same series at the same three
    /// timestamps under two provenance prefixes; the merged result is one
    /// series of three samples, and the second object's copies are dropped
    /// before the charge is taken, so the charge is one series, three
    /// samples and two prefixes, not one copy per object.
    #[test]
    fn the_fold_reserves_exactly_what_it_held() {
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
        assert_eq!(fold.charged(), one_object);
        assert_eq!(budget.reserved(), one_object);

        fold.absorb(vec![scalar_run(7, &samples, (2, 1, 1), None)])
            .expect("absorb");
        let merged = series_len + 3 * sample_len + 2 * prefix_len;
        assert_eq!(fold.charged(), merged);
        assert_eq!(budget.reserved(), merged);

        let series = fold.into_series(usize::MAX).expect("finish");
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 3);
        assert_eq!(budget.reserved(), 0, "released once the result is built");
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
}
