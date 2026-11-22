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
//!
//! The fold charges its containers by capacity (issue #4530, ADR-4523
//! decision 6): each level's and each spine's buffer, the `by_series` and
//! `prefix_ids` tables, the prefix Vec and each label set's strings. It
//! reserves an allocation before it makes it, holding the old and the new
//! buffer at once while a reallocation copies, and an allocation the
//! allocator refuses releases that charge and fails the absorb with
//! [`QueryError::AllocationRefused`].

use std::collections::{HashMap, TryReserveError};
use std::hash::Hash;
use std::sync::Arc;

use ravel_promql::{HistogramSeriesData, SeriesData};
use ravel_segment::{HistogramCounts, HistogramValue};
use ravel_types::{Label, LabelSet, Sample, SeriesId};

use super::{RunPriorities, histogram_sort_key, histogram_value_to_float, is_greater};
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchedHistogramSeries, FetchedSeriesSoa};

/// `(created_unix_ns, writer_epoch, writer_seq)`: the part of a sample's
/// dedup key its object shares. The fold stores it once per distinct value.
type ProvenancePrefix = (i64, u64, u64);

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
    ledger: Ledger,
    /// The most bytes [`RunFold::finish`]'s merges allocate beyond what the
    /// fold holds: the level capacity of the largest series held as more
    /// than one level, at any point so far.
    finish_headroom: u64,
}

/// The scalar fold.
pub(super) type ScalarFold = RunFold<f64>;
/// The native-histogram fold.
pub(super) type HistogramFold = RunFold<HistogramValue>;

/// A fold's reservations against its budget and the bytes it holds.
struct Ledger {
    budget: Arc<ravel_memory::MemoryBudget>,
    reservations: Vec<ravel_memory::Reservation>,
    /// The sum of `reservations`' sizes.
    charged: u64,
    /// The bytes the fold's containers hold allocated, by capacity.
    held: u64,
}

impl Ledger {
    /// Grows the reservations to at least `bytes`.
    fn cover(&mut self, bytes: u64) -> Result<(), QueryError> {
        if bytes <= self.charged {
            return Ok(());
        }
        let reservation = self
            .budget
            .reserve(bytes - self.charged)
            .map_err(budget_refused)?;
        self.charged = self.charged.saturating_add(reservation.size());
        self.reservations.push(reservation);
        Ok(())
    }

    /// Charges `bytes` already allocated that the fold takes ownership of.
    fn take(&mut self, bytes: u64) -> Result<(), QueryError> {
        self.cover(self.held.saturating_add(bytes))?;
        self.held = self.held.saturating_add(bytes);
        Ok(())
    }

    /// Stops charging `bytes` the fold has freed or dropped.
    fn release(&mut self, bytes: u64) {
        self.held = self.held.saturating_sub(bytes);
    }

    /// Charges a `new`-byte allocation that replaces an `old`-byte buffer,
    /// then makes it with `alloc`. Both buffers are charged while both are
    /// live, so the reservation covers `held + new` before `alloc` runs; once
    /// `alloc` succeeds the old buffer is the caller's to free. When `alloc`
    /// fails, the `new` bytes charged for it are released, once, and the
    /// failure is [`QueryError::AllocationRefused`] for them.
    fn grow(
        &mut self,
        old: u64,
        new: u64,
        alloc: impl FnOnce() -> Result<(), TryReserveError>,
    ) -> Result<(), QueryError> {
        self.cover(self.held.saturating_add(new))?;
        self.held = self.held.saturating_add(new);
        if !allocate(self.charged, alloc) {
            self.held = self.held.saturating_sub(new);
            return Err(allocation_failed(new));
        }
        self.held = self.held.saturating_sub(old);
        Ok(())
    }

    /// Replaces a `predicted` allocation's charge with the `actual` bytes
    /// the allocation took.
    fn settle(&mut self, predicted: u64, actual: u64) {
        self.held = self.held.saturating_sub(predicted).saturating_add(actual);
    }

    /// Shrinks the reservations to `bytes`: keeps the leading reservations
    /// that fit in `bytes`, reserves the rest before it releases the ones it
    /// replaces, so the fold is never charged less than it holds. That new
    /// reservation is asked for while the old ones are still held, so the
    /// budget refuses it whenever its room is short of it: a budget the
    /// call's own reservation filled, other holders on a tight budget, or a
    /// closed host gate. A refusal gives up the trim and leaves the old
    /// reservations in place, a [`Reservation`] being unable to shrink: the
    /// fold stays over-charged, never under-charged, until a later trim
    /// succeeds. Under that memory pressure the fold keeps the call's merge
    /// room as it did before issue #4530.
    fn trim_to(&mut self, bytes: u64) {
        if bytes >= self.charged {
            return;
        }
        let mut kept = 0;
        let mut sum = 0u64;
        for reservation in &self.reservations {
            let next = sum.saturating_add(reservation.size());
            if next > bytes {
                break;
            }
            sum = next;
            kept += 1;
        }
        let rest = bytes - sum;
        if rest > 0 {
            let Ok(reservation) = self.budget.reserve_best_effort(rest) else {
                return;
            };
            self.reservations.truncate(kept);
            self.reservations.push(reservation);
        } else {
            self.reservations.truncate(kept);
        }
        self.charged = bytes;
    }
}

/// Runs `alloc`, unless a test armed an allocation failure with
/// [`support::fail_allocation_after`]. `charged` is what the fold had
/// reserved when it asked.
fn allocate(charged: u64, alloc: impl FnOnce() -> Result<(), TryReserveError>) -> bool {
    #[cfg(any(test, feature = "test-support"))]
    if support::take_injected_failure(charged) {
        return false;
    }
    #[cfg(not(any(test, feature = "test-support")))]
    let _ = charged;
    alloc().is_ok()
}

/// The error for a `requested`-byte allocation the allocator refused after
/// the budget admitted its charge. It is not a budget refusal, so it is not
/// [`FetchError::FetchMemoryExhausted`], whose causes are the budget's.
fn allocation_failed(requested: u64) -> QueryError {
    QueryError::AllocationRefused { requested }
}

/// A reservation the budget refused, as the error a fetch reservation
/// returns.
fn budget_refused(e: ravel_memory::MemoryExhausted) -> QueryError {
    FetchError::FetchMemoryExhausted {
        requested: e.requested,
        reserved: e.reserved,
        limit: e.limit,
        cause: e.cause,
    }
    .into()
}

/// The bytes [`RunFold::finish`]'s output Vec of `S` allocates for
/// `series` series.
fn result_len<S>(series: usize) -> u64 {
    (series as u64).saturating_mul(std::mem::size_of::<S>() as u64)
}

impl<V: FoldValue> RunFold<V> {
    pub(super) fn new(budget: Arc<ravel_memory::MemoryBudget>, max_series: usize) -> Self {
        Self {
            by_series: HashMap::new(),
            prefixes: Vec::new(),
            prefix_ids: HashMap::new(),
            max_series,
            ledger: Ledger {
                budget,
                reservations: Vec::new(),
                charged: 0,
                held: 0,
            },
            finish_headroom: 0,
        }
    }

    /// Reserves the call's bound ([`call_bound`](RunFold::call_bound)), then
    /// folds `runs` in, charging each allocation before it is made, so the
    /// fold never holds more than it has reserved (issue #4118). A series
    /// past `max_series` is refused with [`QueryError::TooManySeries`]
    /// (ADR-0051 S7) the moment it would be added; a run whose priority
    /// column is not parallel to its samples, or whose timestamps descend,
    /// with the error the merge returns for it; a reservation the budget
    /// refuses with [`FetchError::FetchMemoryExhausted`], the error a fetch
    /// reservation returns; and an allocation the allocator refuses with
    /// [`QueryError::AllocationRefused`].
    ///
    /// A bound the budget refuses fails the call before any of `runs` is
    /// folded. Two charges lie outside the bound and can be refused after
    /// some of `runs` were folded, which then stay folded: the exact-size
    /// copy of a run's level that a duplicate timestamp shortened, and the
    /// finish headroom raised at the call's end for a series the bound did
    /// not predict to merge that ends the call as more than one level.
    ///
    /// The reservation is then shrunk, whether the call succeeded or not, to
    /// what the fold holds plus the headroom [`RunFold::finish`]'s merges
    /// need, so a call's merge room is not held past it, unless the budget
    /// refuses the smaller reservation ([`Ledger::trim_to`]).
    ///
    /// The bound is computed over `runs` in place, sorted by series so each
    /// series' incoming samples sum without a side table (issue #4198); the
    /// fold's output does not depend on the order runs are folded in.
    pub(super) fn absorb<R: FoldInput<V>>(&mut self, mut runs: Vec<R>) -> Result<(), QueryError> {
        runs.sort_unstable_by_key(FoldInput::series_id);
        let (bound, new_series) = self.call_bound(&runs);
        let folded = match self.ledger.cover(bound) {
            Ok(()) => self.fold_runs(runs, new_series),
            Err(e) => Err(e),
        };
        let keep = self.ledger.held.saturating_add(self.finish_headroom);
        let covered = self.ledger.cover(keep);
        self.ledger.trim_to(keep);
        folded?;
        covered?;
        #[cfg(any(test, feature = "test-support"))]
        support::observe_absorb(|| self.charged());
        Ok(())
    }

    fn fold_runs<R: FoldInput<V>>(
        &mut self,
        runs: Vec<R>,
        new_series: usize,
    ) -> Result<(), QueryError> {
        let len = self.by_series.len();
        let need = len.saturating_add(new_series.min(self.max_series.saturating_sub(len)));
        grow_table(&mut self.ledger, &mut self.by_series, need)?;
        for run in runs {
            self.absorb_run(run.into_run())?;
        }
        Ok(())
    }

    /// The reservation [`absorb`](RunFold::absorb) takes before it folds
    /// `runs`, sorted by series, in, and how many of their series the fold
    /// does not hold yet. It is not the most the fold can hold during the
    /// call: the exact-size copy of a level that a duplicate timestamp
    /// shortened is not counted, nor the finish headroom raised for a series
    /// not predicted to merge; each is charged when it arises.
    ///
    /// The bound is what the fold holds, plus its growth, plus the finish
    /// headroom or one transient buffer, whichever is larger. The growth counts every incoming sample as kept in a level of its
    /// run's length, the labels of each new series, each spine's growth to
    /// one more level per run, the `by_series` table's growth to the new
    /// series, and the prefix Vec's and `prefix_ids`' growth to one entry per
    /// change of provenance prefix along the call that the fold has not
    /// interned. While the fold grows, one buffer at a time is live beside
    /// what it holds: the buffer a container's last growth replaces, or the
    /// level a merge builds, which holds at most the samples its series held
    /// plus those the call brings it. A merge is counted only for a series
    /// that can merge: one with more than one run in the call, or whose last
    /// held level is at most twice its incoming run.
    fn call_bound<R: FoldInput<V>>(&self, runs: &[R]) -> (u64, usize) {
        let inline = inline_len::<V>();
        let spine = std::mem::size_of::<Vec<FoldedSample<V>>>();
        let mut growth = 0u64;
        let mut transient = self.finish_headroom;
        let mut new_series = 0usize;
        let mut new_prefixes = 0usize;
        let mut last_prefix = None;
        let mut note_prefix = |key: ProvenancePrefix| {
            if last_prefix != Some(key) {
                last_prefix = Some(key);
                if !self.prefix_ids.contains_key(&key) {
                    new_prefixes += 1;
                }
            }
        };
        for series in runs.chunk_by(|a, b| a.series_id() == b.series_id()) {
            let Some(first) = series.first() else {
                continue;
            };
            let mut incoming = 0usize;
            for run in series {
                incoming = incoming.saturating_add(run.values().len());
                growth = run
                    .values()
                    .iter()
                    .map(FoldValue::heap_len)
                    .fold(growth, u64::saturating_add);
                match run.per_sample() {
                    None => note_prefix(run.prefix()),
                    Some(column) => {
                        for p in column {
                            note_prefix((p.created_unix_ns, p.writer_epoch, p.writer_seq));
                        }
                    }
                }
            }
            growth = growth.saturating_add((incoming as u64).saturating_mul(inline));
            let held = self.by_series.get(&first.series_id());
            let (levels, held_samples) = match held {
                Some(held) => (&held.levels[..], held.levels.iter().map(Vec::len).sum()),
                None => {
                    new_series += 1;
                    growth = growth.saturating_add(label_len(first.labels()));
                    (&[][..], 0usize)
                }
            };
            let (spine_growth, spine_old) = vec_growth(
                held.map_or(0, |held| held.levels.capacity()),
                levels.len().saturating_add(series.len()),
                spine,
            );
            growth = growth.saturating_add(spine_growth);
            transient = transient.max(spine_old);
            let merges = series.len() > 1
                || levels
                    .last()
                    .is_some_and(|last| last.len() <= incoming.saturating_mul(2));
            if merges {
                let merged = (held_samples.saturating_add(incoming) as u64).saturating_mul(inline);
                transient = transient.max(merged);
            }
        }
        let len = self.by_series.len();
        let need = len.saturating_add(new_series.min(self.max_series.saturating_sub(len)));
        if need > self.by_series.capacity() {
            let slot = std::mem::size_of::<(SeriesId, FoldedSeries<V>)>();
            let old = table_bytes(self.by_series.capacity(), slot);
            let new = table_alloc(table_buckets(need), slot);
            growth = growth.saturating_add(new.saturating_sub(old));
            transient = transient.max(old);
        }
        let need = self.prefixes.len().saturating_add(new_prefixes);
        let (prefixes, prefixes_old) = vec_growth(
            self.prefixes.capacity(),
            need,
            std::mem::size_of::<ProvenancePrefix>(),
        );
        let (ids, ids_old) = table_growth(
            self.prefix_ids.capacity(),
            need,
            std::mem::size_of::<(ProvenancePrefix, u32)>(),
        );
        growth = growth.saturating_add(prefixes).saturating_add(ids);
        transient = transient.max(prefixes_old).max(ids_old);
        let bound = self
            .ledger
            .held
            .saturating_add(growth)
            .saturating_add(transient);
        (bound, new_series)
    }

    fn absorb_run(&mut self, run: FoldRun<V>) -> Result<(), QueryError> {
        let FoldRun {
            series_id,
            labels,
            timestamps,
            values,
            priorities,
        } = run;
        if !self.by_series.contains_key(&series_id) && self.by_series.len() >= self.max_series {
            return Err(QueryError::TooManySeries {
                count: self.by_series.len() + 1,
                max: self.max_series,
            });
        }
        priorities.check_len(timestamps.len())?;
        let inline = inline_len::<V>();
        let n = timestamps.len();
        let mut level: Vec<FoldedSample<V>> = Vec::new();
        self.ledger.grow(0, (n as u64).saturating_mul(inline), || {
            level.try_reserve_exact(n)
        })?;
        let filled = self.fill_level(&mut level, &timestamps, values, &priorities);
        let capacity = (level.capacity() as u64).saturating_mul(inline);
        let heap = heap_len(&level);
        if let Err(e) = filled.and_then(|()| self.ledger.take(heap)) {
            self.ledger.release(capacity);
            return Err(e);
        }
        if level.len() < level.capacity() {
            let len = level.len();
            let mut exact = Vec::new();
            if let Err(e) = self
                .ledger
                .grow(capacity, (len as u64).saturating_mul(inline), || {
                    exact.try_reserve_exact(len)
                })
            {
                self.ledger.release(capacity.saturating_add(heap));
                return Err(e);
            }
            exact.append(&mut level);
            level = exact;
        }
        self.place(series_id, labels, level, heap)
    }

    /// Fills `level` with `values` in timestamp order, one sample per
    /// timestamp.
    #[inline(always)]
    fn fill_level(
        &mut self,
        level: &mut Vec<FoldedSample<V>>,
        timestamps: &[i64],
        values: Vec<V>,
        priorities: &RunPriorities,
    ) -> Result<(), QueryError> {
        let mut last_prefix: Option<(ProvenancePrefix, u32)> = None;
        for (pos, (&ts, value)) in timestamps.iter().zip(values).enumerate() {
            let Some((created, epoch, seq, in_page_index)) = priorities.at(pos) else {
                return Err(QueryError::PrioritySampleCountMismatch {
                    priorities: priorities.column_len().unwrap_or(0),
                    samples: timestamps.len(),
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
        Ok(())
    }

    /// Adds `level`, whose samples hold `heap` heap bytes and which the fold
    /// already charges, to its series, merging it with the series' smaller
    /// levels. On an error the level is dropped and its charge released.
    #[inline(always)]
    fn place(
        &mut self,
        series_id: SeriesId,
        labels: LabelSet,
        mut level: Vec<FoldedSample<V>>,
        heap: u64,
    ) -> Result<(), QueryError> {
        let inline = inline_len::<V>();
        let spine = std::mem::size_of::<Vec<FoldedSample<V>>>() as u64;
        let level_bytes = (level.capacity() as u64)
            .saturating_mul(inline)
            .saturating_add(heap);
        // `entry` reserves room for a vacant key, so a full table grows here,
        // charged, before it would grow there.
        if self.by_series.len() == self.by_series.capacity()
            && !self.by_series.contains_key(&series_id)
        {
            let need = self.by_series.len().saturating_add(1);
            if let Err(e) = grow_table(&mut self.ledger, &mut self.by_series, need) {
                self.ledger.release(level_bytes);
                return Err(e);
            }
        }
        let series = match self.by_series.entry(series_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let Err(e) = self.ledger.take(label_len(&labels)) {
                    self.ledger.release(level_bytes);
                    return Err(e);
                }
                entry.insert(FoldedSeries {
                    labels,
                    levels: Vec::new(),
                })
            }
        };
        let prefixes = &self.prefixes;
        let ledger = &mut self.ledger;
        while let Some(last) = series.levels.last() {
            if last.len() > level.len().saturating_mul(2) {
                break;
            }
            let Some(last) = series.levels.pop() else {
                break;
            };
            let len = merged_len(&last, &level);
            let mut out = Vec::new();
            if let Err(e) = ledger.grow(0, (len as u64).saturating_mul(inline), || {
                out.try_reserve_exact(len)
            }) {
                // The pop left room for `last`, so this push does not allocate.
                series.levels.push(last);
                ledger.release(
                    (level.capacity() as u64)
                        .saturating_mul(inline)
                        .saturating_add(heap_len(&level)),
                );
                return Err(e);
            }
            let freed = ((last.capacity() + level.capacity()) as u64).saturating_mul(inline);
            let mut dropped = 0u64;
            merge_into(prefixes, last, level, &mut out, &mut dropped);
            ledger.release(freed.saturating_add(dropped));
            level = out;
        }
        if level.is_empty() {
            return Ok(());
        }
        let len = series.levels.len();
        let capacity = series.levels.capacity();
        if len == capacity {
            let next = next_capacity(capacity);
            let levels = &mut series.levels;
            if let Err(e) = ledger.grow(
                (capacity as u64).saturating_mul(spine),
                (next as u64).saturating_mul(spine),
                || levels.try_reserve_exact(next - len),
            ) {
                ledger.release(
                    (level.capacity() as u64)
                        .saturating_mul(inline)
                        .saturating_add(heap_len(&level)),
                );
                return Err(e);
            }
        }
        series.levels.push(level);
        if series.levels.len() > 1 {
            let held = series
                .levels
                .iter()
                .map(|level| (level.capacity() as u64).saturating_mul(inline))
                .fold(0u64, u64::saturating_add);
            self.finish_headroom = self.finish_headroom.max(held);
        }
        Ok(())
    }

    /// The prefix table index of `prefix`, adding it if new. Each distinct
    /// prefix came from at least one sample, so a table past `u32::MAX`
    /// entries is a read past `u32::MAX` samples, refused as one.
    #[inline(never)]
    fn intern(&mut self, prefix: ProvenancePrefix) -> Result<u32, QueryError> {
        if let Some(&id) = self.prefix_ids.get(&prefix) {
            return Ok(id);
        }
        let len = self.prefixes.len();
        let id = u32::try_from(len).map_err(|_| QueryError::TooManySamples {
            count: len.saturating_add(1),
            max: u32::MAX as usize,
        })?;
        let capacity = self.prefixes.capacity();
        if len == capacity {
            let entry = std::mem::size_of::<ProvenancePrefix>() as u64;
            let next = next_capacity(capacity);
            let prefixes = &mut self.prefixes;
            self.ledger.grow(
                (capacity as u64).saturating_mul(entry),
                (next as u64).saturating_mul(entry),
                || prefixes.try_reserve_exact(next - len),
            )?;
        }
        let need = self.prefix_ids.len().saturating_add(1);
        grow_table(&mut self.ledger, &mut self.prefix_ids, need)?;
        self.prefixes.push(prefix);
        self.prefix_ids.insert(prefix, id);
        Ok(id)
    }

    fn sample_serves_over(&self, a: &FoldedSample<V>, b: &FoldedSample<V>) -> bool {
        serves_over(&self.prefixes, a, b)
    }

    /// The bytes this fold holds reserved. It is never less than the bytes
    /// its containers hold, by capacity: each level's buffer at its inline
    /// sample size plus its samples' heap bytes, each series' spine of
    /// levels, the `by_series` and `prefix_ids` tables at hashbrown's
    /// allocation for their capacity, the prefix Vec, and each label's inline
    /// size plus its strings' capacity. After an absorb that succeeded and
    /// whose trim the budget granted, it is exactly those bytes plus the
    /// finish headroom: the level capacity of the largest series the fold has
    /// held as more than one level, 0 while every series is one level. A
    /// trim the budget refused leaves it above that ([`Ledger::trim_to`]).
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn charged(&self) -> u64 {
        self.ledger.charged
    }

    /// Hands the fold's reservations to the caller, which holds them for as
    /// long as it holds the series [`into_series`](RunFold::into_series)
    /// returns, and zeroes the fold's charged figure, so each reserved byte
    /// is held once.
    pub(super) fn take_reservations(&mut self) -> Vec<ravel_memory::Reservation> {
        self.ledger.charged = 0;
        std::mem::take(&mut self.ledger.reservations)
    }

    /// Collapses each series' runs into one, converts each held sample with
    /// `convert` and each series with `wrap`, and checks `max_samples` over
    /// the whole result, counting one sample per `(series, ts)` slot as the
    /// merge does. Reservations not taken with
    /// [`take_reservations`](RunFold::take_reservations) are released when
    /// this returns.
    ///
    /// Before it allocates the result Vec it reserves that Vec's bytes,
    /// [`result_len`], as a reservation of its own, and pushes it to `held`
    /// once the result is built, so the caller holds it with the result; a
    /// budget that refuses it is [`FetchError::FetchMemoryExhausted`]. Each
    /// merge's level is sized exactly before it is built, within the fold's
    /// headroom; an allocation the allocator refuses is
    /// [`QueryError::AllocationRefused`]. So the fold's charge and the result
    /// reservation together cover the containers, the result Vec and the
    /// merge buffer being built at every point (issue #4118).
    ///
    /// Two kinds of bytes stay uncharged, because the one reservation taken
    /// here is sized for the result Vec and is not grown per series. Each
    /// conversion maps a held sample to a smaller output sample of the same
    /// alignment ([`Sample`] or [`ravel_promql::HistogramSample`]), which
    /// `Vec`'s in-place collect writes into the merged level's own buffer;
    /// when that buffer is not a whole number of output samples the collect
    /// shrinks it with a reallocation, and an allocator that serves the
    /// shrink by moving the buffer holds an uncharged copy while it moves.
    /// And the histogram conversion allocates each converted value's span,
    /// bucket and custom-value Vecs before the held value it replaces is
    /// dropped, so it holds one value's heap bytes beyond the charge at a
    /// time; in total the converted values' heap bytes equal the held
    /// values', which the fold charged.
    fn finish<T, S>(
        self,
        max_samples: usize,
        convert: impl Fn(FoldedSample<V>) -> T,
        wrap: impl Fn(LabelSet, Vec<T>) -> S,
        held: &mut Vec<ravel_memory::Reservation>,
    ) -> Result<Vec<S>, QueryError> {
        let Self {
            by_series,
            prefixes,
            ledger,
            ..
        } = self;
        let inline = inline_len::<V>();
        let spine = std::mem::size_of::<Vec<FoldedSample<V>>>() as u64;
        let count = by_series.len();
        let result = ledger
            .budget
            .reserve(result_len::<S>(count))
            .map_err(budget_refused)?;
        let mut probe = FinishProbe::new(ledger.held, ledger.charged.saturating_add(result.size()));
        let mut total = 0usize;
        let mut out = Vec::new();
        if !allocate(ledger.charged, || out.try_reserve_exact(count)) {
            return Err(allocation_failed(result_len::<S>(count)));
        }
        probe.alloc(result_len::<S>(out.capacity()));
        for series in by_series.into_values() {
            let mut levels = series.levels;
            let mut merged = levels.pop().unwrap_or_default();
            while let Some(level) = levels.pop() {
                let len = merged_len(&level, &merged);
                let mut next = Vec::new();
                if !allocate(ledger.charged, || next.try_reserve_exact(len)) {
                    return Err(allocation_failed((len as u64).saturating_mul(inline)));
                }
                probe.alloc((next.capacity() as u64).saturating_mul(inline));
                let freed = ((level.capacity() + merged.capacity()) as u64).saturating_mul(inline);
                let mut dropped = 0u64;
                merge_into(&prefixes, level, merged, &mut next, &mut dropped);
                probe.free(freed.saturating_add(dropped));
                merged = next;
            }
            probe.free((levels.capacity() as u64).saturating_mul(spine));
            drop(levels);
            total = total.saturating_add(merged.len());
            if total > max_samples {
                return Err(QueryError::TooManySamples {
                    count: max_samples.saturating_add(1),
                    max: max_samples,
                });
            }
            let source = (
                merged.as_ptr() as usize,
                (merged.capacity() as u64).saturating_mul(inline),
            );
            let samples: Vec<T> = merged.into_iter().map(&convert).collect();
            probe.converted(
                source,
                samples.as_ptr() as usize,
                result_len::<T>(samples.capacity()),
            );
            out.push(wrap(series.labels, samples));
        }
        probe.finish();
        drop(ledger);
        held.push(result);
        Ok(out)
    }
}

impl RunFold<f64> {
    /// The merged series, as [`merge_soa_runs`](super::merge_soa_runs)
    /// returns them. The reservation for the result Vec is pushed to `held`
    /// ([`finish`](RunFold::finish)).
    pub(super) fn into_series(
        self,
        max_samples: usize,
        held: &mut Vec<ravel_memory::Reservation>,
    ) -> Result<Vec<SeriesData>, QueryError> {
        self.finish(
            max_samples,
            |s| Sample {
                ts_ns: s.ts,
                value: s.value,
            },
            |labels, samples| SeriesData { labels, samples },
            held,
        )
    }
}

impl RunFold<HistogramValue> {
    /// The merged series, as
    /// [`merge_histogram_soa_runs`](super::merge_histogram_soa_runs) returns
    /// them. The reservation for the result Vec is pushed to `held`
    /// ([`finish`](RunFold::finish)).
    pub(super) fn into_series(
        self,
        max_samples: usize,
        held: &mut Vec<ravel_memory::Reservation>,
    ) -> Result<Vec<HistogramSeriesData>, QueryError> {
        self.finish(
            max_samples,
            |s| ravel_promql::HistogramSample {
                ts_ns: s.ts,
                value: histogram_value_to_float(&s.value),
            },
            |labels, samples| HistogramSeriesData { labels, samples },
            held,
        )
    }
}

/// What [`RunFold::finish`] holds live as it runs, by step 1's definition
/// plus the result Vec, recorded for the #4118 check across finish
/// ([`support::take_finish_peak`]). It starts from the fold's running count
/// of what it holds, which the tests check against the containers after
/// every absorb, and counts the labels, the tables and the prefix Vec as
/// live until finish returns.
#[cfg(any(test, feature = "test-support"))]
struct FinishProbe {
    live: u64,
    peak: u64,
    charged: u64,
}

#[cfg(any(test, feature = "test-support"))]
impl FinishProbe {
    fn new(live: u64, charged: u64) -> Self {
        Self {
            live,
            peak: live,
            charged,
        }
    }

    fn alloc(&mut self, bytes: u64) {
        self.live = self.live.saturating_add(bytes);
        self.peak = self.peak.max(self.live);
    }

    fn free(&mut self, bytes: u64) {
        self.live = self.live.saturating_sub(bytes);
    }

    /// A level of `source` (its address and bytes) converted into a buffer
    /// at `address` of `bytes`. A conversion that moved to another buffer
    /// held both while it ran.
    fn converted(&mut self, source: (usize, u64), address: usize, bytes: u64) {
        if source.0 == address {
            self.free(source.1);
            self.live = self.live.saturating_add(bytes);
        } else {
            self.alloc(bytes);
            self.free(source.1);
        }
    }

    fn finish(self) {
        support::record_finish_peak(self.peak, self.charged);
    }
}

#[cfg(not(any(test, feature = "test-support")))]
struct FinishProbe;

#[cfg(not(any(test, feature = "test-support")))]
impl FinishProbe {
    #[inline(always)]
    fn new(_live: u64, _charged: u64) -> Self {
        Self
    }

    #[inline(always)]
    fn alloc(&mut self, _bytes: u64) {}

    #[inline(always)]
    fn free(&mut self, _bytes: u64) {}

    #[inline(always)]
    fn converted(&mut self, _source: (usize, u64), _address: usize, _bytes: u64) {}

    #[inline(always)]
    fn finish(self) {}
}

/// The bytes one held sample takes in a level's buffer.
fn inline_len<V>() -> u64 {
    std::mem::size_of::<FoldedSample<V>>() as u64
}

/// The heap bytes `level`'s samples hold beyond the level's buffer.
fn heap_len<V: FoldValue>(level: &[FoldedSample<V>]) -> u64 {
    level
        .iter()
        .map(|s| s.value.heap_len())
        .fold(0u64, u64::saturating_add)
}

/// The heap bytes `labels` holds: each label's slot in the set's Vec and
/// its two strings' capacity.
fn label_len(labels: &LabelSet) -> u64 {
    labels
        .iter()
        .map(|l| (std::mem::size_of::<Label>() + l.name.capacity() + l.value.capacity()) as u64)
        .fold(0u64, u64::saturating_add)
}

/// The capacity a full Vec of `capacity` grows to: double, at least 4,
/// as `Vec::push` grows a Vec of elements this small.
fn next_capacity(capacity: usize) -> usize {
    capacity.saturating_mul(2).max(4)
}

/// The bytes a Vec of `entry`-byte elements grows by, one
/// [`next_capacity`] step at a time from `capacity`, to hold `need`
/// elements, and the bytes of the buffer its last step replaces.
fn vec_growth(capacity: usize, need: usize, entry: usize) -> (u64, u64) {
    let mut grown = capacity;
    let mut replaced = 0usize;
    while grown < need {
        replaced = grown;
        grown = next_capacity(grown);
    }
    let entry = entry as u64;
    (
        ((grown - capacity) as u64).saturating_mul(entry),
        (replaced as u64).saturating_mul(entry),
    )
}

/// hashbrown's group width: the SSE2 group on x86, 8 bytes elsewhere.
pub(super) const GROUP_WIDTH: u64 = if cfg!(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "sse2"
)) {
    16
} else {
    8
};

/// The entries hashbrown lets a table of `buckets` buckets hold.
fn bucket_capacity(buckets: usize) -> usize {
    if buckets <= 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    }
}

/// The fewest buckets, a power of two of at least 4, whose
/// [`bucket_capacity`] covers `capacity`: the bucket count hashbrown
/// allocates for a table asked to hold `capacity` entries.
fn table_buckets(capacity: usize) -> usize {
    let mut buckets = 4usize;
    while bucket_capacity(buckets) < capacity {
        match buckets.checked_mul(2) {
            Some(next) => buckets = next,
            None => break,
        }
    }
    buckets
}

/// The bytes a table of `buckets` buckets of `slot`-byte entries allocates:
/// buckets x slot, plus one control byte per bucket and the group width. A
/// table of no buckets allocates nothing.
fn table_alloc(buckets: usize, slot: usize) -> u64 {
    if buckets == 0 {
        return 0;
    }
    let buckets = buckets as u64;
    buckets
        .saturating_mul(slot as u64)
        .saturating_add(buckets)
        .saturating_add(GROUP_WIDTH)
}

/// The bytes a `HashMap` whose [`HashMap::capacity`] is `capacity` and
/// whose `(key, value)` slot is `slot` bytes allocates, as the R5
/// pre-registration defines it: [`table_alloc`] for its bucket count.
/// HashMap has no allocation accessor, so the bucket count is recovered
/// from the capacity: the fewest buckets, a power of two of at least 4,
/// whose hashbrown capacity covers `capacity`, where `buckets` hold
/// `buckets - 1` entries up to 8 buckets and `buckets / 8 * 7` above. That
/// is exact for every capacity a table that never removed an entry reports,
/// which the fold's tables never do. A capacity of 0 is a table that never
/// allocated. hashbrown's padding of the slot array to the control bytes'
/// alignment is not counted; `tests/run_fold_accounting.rs` asserts the
/// formula against jemalloc.
fn table_bytes(capacity: usize, slot: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    table_alloc(table_buckets(capacity), slot)
}

/// The bytes a table of `slot`-byte entries grows by, one doubling at a
/// time from `capacity`, to hold `need` entries, and the bytes of the table
/// its last doubling replaces.
fn table_growth(capacity: usize, need: usize, slot: usize) -> (u64, u64) {
    if need <= capacity {
        return (0, 0);
    }
    let from = if capacity == 0 {
        0
    } else {
        table_buckets(capacity)
    };
    let to = table_buckets(need);
    let replaced = if to / 2 >= from.max(4) { to / 2 } else { from };
    (
        table_alloc(to, slot).saturating_sub(table_alloc(from, slot)),
        table_alloc(replaced, slot),
    )
}

/// Grows `table` to hold `need` entries, in one allocation charged to
/// `ledger` before it is made.
fn grow_table<K: Eq + Hash, T>(
    ledger: &mut Ledger,
    table: &mut HashMap<K, T>,
    need: usize,
) -> Result<(), QueryError> {
    let capacity = table.capacity();
    if need <= capacity {
        return Ok(());
    }
    let slot = std::mem::size_of::<(K, T)>();
    let predicted = table_alloc(table_buckets(need), slot);
    let additional = need - table.len();
    ledger.grow(table_bytes(capacity, slot), predicted, || {
        table.try_reserve(additional)
    })?;
    ledger.settle(predicted, table_bytes(table.capacity(), slot));
    Ok(())
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

/// The samples [`merge_into`] keeps from `a` and `b`, two sorted runs of
/// one sample per timestamp: both lengths less the timestamps they share,
/// which can only lie where their spans overlap.
fn merged_len<V>(a: &[FoldedSample<V>], b: &[FoldedSample<V>]) -> usize {
    let total = a.len() + b.len();
    let (Some(a_first), Some(a_last), Some(b_first), Some(b_last)) =
        (a.first(), a.last(), b.first(), b.last())
    else {
        return total;
    };
    let lo = a_first.ts.max(b_first.ts);
    let hi = a_last.ts.min(b_last.ts);
    if lo > hi {
        return total;
    }
    let window = |run: &[FoldedSample<V>]| -> (usize, usize) {
        (first_at_least(run, lo), first_after(run, hi))
    };
    let ((mut i, a_end), (mut j, b_end)) = (window(a), window(b));
    let mut shared = 0usize;
    while i < a_end && j < b_end {
        let (Some(x), Some(y)) = (a.get(i), b.get(j)) else {
            break;
        };
        match x.ts.cmp(&y.ts) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                shared += 1;
                i += 1;
                j += 1;
            }
        }
    }
    total - shared
}

/// The first index of sorted `run` whose timestamp is at least `lo`. A
/// level's overlap with the next lies at its end, so the search gallops back
/// from there rather than bisecting the whole level, which touches cold
/// cache lines across it.
fn first_at_least<V>(run: &[FoldedSample<V>], lo: i64) -> usize {
    if run.first().is_none_or(|s| s.ts >= lo) {
        return 0;
    }
    // Every sample in `run[found..]` is at or after `lo`.
    let mut found = run.len();
    let mut step = 1usize;
    loop {
        let probe = found.saturating_sub(step);
        match run.get(probe) {
            Some(s) if s.ts >= lo => {
                found = probe;
                step = step.saturating_mul(2);
            }
            _ => {
                let span = run.get(probe..found).unwrap_or_default();
                return probe + span.partition_point(|s| s.ts < lo);
            }
        }
    }
}

/// The first index of sorted `run` whose timestamp is after `hi`, galloping
/// forward from its start, where a level's overlap with the one before lies.
fn first_after<V>(run: &[FoldedSample<V>], hi: i64) -> usize {
    if run.last().is_none_or(|s| s.ts <= hi) {
        return run.len();
    }
    // Every sample in `run[..found]` is at or before `hi`.
    let mut found = 0usize;
    let mut step = 1usize;
    loop {
        let bound = found.saturating_add(step).min(run.len());
        match run.get(bound - 1) {
            Some(s) if s.ts <= hi => {
                found = bound;
                step = step.saturating_mul(2);
            }
            _ => {
                let span = run.get(found..bound).unwrap_or_default();
                return found + span.partition_point(|s| s.ts <= hi);
            }
        }
    }
}

/// Merges two sorted runs of one sample per timestamp into `out`, sized by
/// the caller for [`merged_len`] samples, keeping the sample served over
/// the other where both hold a timestamp, and adds the heap bytes of every
/// sample it drops to `dropped`.
fn merge_into<V: FoldValue>(
    prefixes: &[ProvenancePrefix],
    a: Vec<FoldedSample<V>>,
    b: Vec<FoldedSample<V>>,
    out: &mut Vec<FoldedSample<V>>,
    dropped: &mut u64,
) {
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
                    *dropped = dropped.saturating_add(loser.value.heap_len());
                }
                a_wins
            }
        };
        let next = if take_a { a.next() } else { b.next() };
        if let Some(sample) = next {
            out.push(sample);
        }
    }
}

/// Test and bench support only (issue #4530, ADR-4523 decision 6, R5): the
/// scalar fold driven from outside the crate, the pre-registered fold shapes,
/// the hash-table size formula, an observer of the fold's charge after each
/// absorb, and an injected allocation failure. Only tests,
/// `tests/run_fold_accounting.rs` and `benches/run_fold.rs` call into it; the
/// crate itself reaches it only through [`RunFold::absorb`]'s observer check
/// and the fold's allocation check, which do nothing unless an observer was
/// installed with [`set_absorb_observer`](support::set_absorb_observer) or a
/// failure armed with [`fail_allocation_after`](support::fail_allocation_after).
/// Built only under `cfg(test)` or the `test-support` feature, so a
/// production build carries neither the module nor either check.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod support {
    use std::cell::Cell;
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
            self.0.into_series(usize::MAX, &mut Vec::new())
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

    /// The fold's own table size formula, which charges its `by_series` and
    /// `prefix_ids` tables: the bytes a `HashMap` whose
    /// [`HashMap::capacity`] is `capacity` and whose `(key, value)` slot is
    /// `slot` bytes allocates, buckets x slot plus one control byte per
    /// bucket and the group width.
    pub fn table_bytes(capacity: usize, slot: usize) -> u64 {
        super::table_bytes(capacity, slot)
    }

    thread_local! {
        static FAIL_AFTER: Cell<Option<usize>> = const { Cell::new(None) };
        static INJECTED_CHARGE: Cell<Option<u64>> = const { Cell::new(None) };
        static FINISH_PEAK: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
    }

    pub(super) fn record_finish_peak(peak: u64, charged: u64) {
        FINISH_PEAK.with(|p| p.set(Some((peak, charged))));
    }

    /// The last `into_series` that returned a result on this thread: the
    /// most it held live at once, by step 1's definition plus its result
    /// Vec, and what the fold had charged plus the result reservation.
    /// Taking it clears it.
    pub fn take_finish_peak() -> Option<(u64, u64)> {
        FINISH_PEAK.with(|p| p.take())
    }

    /// Makes the fold's allocation after the next `skip` ones on this
    /// thread fail as an allocator refusal would, without allocating. Each
    /// level, spine, table and prefix Vec growth in an absorb, and each
    /// buffer `into_series` sizes, is one allocation.
    pub fn fail_allocation_after(skip: usize) {
        FAIL_AFTER.with(|f| f.set(Some(skip)));
        INJECTED_CHARGE.with(|c| c.set(None));
    }

    /// Disarms a failure [`fail_allocation_after`] armed on this thread.
    pub fn clear_allocation_failure() {
        FAIL_AFTER.with(|f| f.set(None));
        INJECTED_CHARGE.with(|c| c.set(None));
    }

    /// The fold's [`charged`](super::RunFold::charged) bytes when the armed
    /// failure fired on this thread, `None` while it has not.
    pub fn injected_failure_charge() -> Option<u64> {
        INJECTED_CHARGE.with(Cell::get)
    }

    /// Whether the allocation about to be made is the one armed to fail,
    /// recording `charged` when it is.
    pub(super) fn take_injected_failure(charged: u64) -> bool {
        FAIL_AFTER.with(|f| match f.get() {
            None => false,
            Some(0) => {
                f.set(None);
                INJECTED_CHARGE.with(|c| c.set(Some(charged)));
                true
            }
            Some(skip) => {
                f.set(Some(skip - 1));
                false
            }
        })
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

    /// The field naming an R5 figure line's second figure for `shape`.
    /// Shapes A, B and C report `live`, the fold's own container bytes.
    /// Shape D reports `process_delta`, the whole process's
    /// `stats.allocated` delta across the engine call, which counts
    /// different bytes, so D's ratio is not comparable with A to C's.
    pub fn r5_figure_field(shape: &str) -> &'static str {
        if shape == "D" {
            "process_delta"
        } else {
            "live"
        }
    }

    /// One R5 figure line: `R5 shape=<shape> charged=.. <field>=.. ratio=..
    /// gap_bytes=..`, `<field>` being [`r5_figure_field`] for the shape, the
    /// ratio to 4 decimals and the gap signed.
    pub fn r5_line(shape: &str, charged: u64, figure: u64) -> String {
        let ratio = if figure == 0 {
            f64::INFINITY
        } else {
            charged as f64 / figure as f64
        };
        let gap = i128::from(charged) - i128::from(figure);
        let field = r5_figure_field(shape);
        format!(
            "R5 shape={shape} charged={charged} {field}={figure} ratio={ratio:.4} gap_bytes={gap}"
        )
    }

    /// The R5 presence rule over `lines`: each of `shapes` has exactly one
    /// figure line, whose fields are `charged`, the shape's
    /// [`r5_figure_field`], `ratio` and `gap_bytes` in that order, with a
    /// nonzero charged and second figure. Returns one message per violation.
    pub fn presence_violations(lines: &[String], shapes: &[&str]) -> Vec<String> {
        let mut violations = Vec::new();
        for shape in shapes {
            let prefix = format!("R5 shape={shape} ");
            let found: Vec<&String> = lines.iter().filter(|l| l.starts_with(&prefix)).collect();
            let [line] = found.as_slice() else {
                violations.push(format!(
                    "shape {shape}: {} figure lines, want exactly 1",
                    found.len()
                ));
                continue;
            };
            let field = r5_figure_field(shape);
            let fields: Vec<(&str, &str)> = line
                .strip_prefix(&prefix)
                .unwrap_or_default()
                .split(' ')
                .map(|kv| kv.split_once('=').unwrap_or((kv, "")))
                .collect();
            let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
            let want = ["charged", field, "ratio", "gap_bytes"];
            if names != want {
                violations.push(format!("shape {shape}: fields {names:?}, want {want:?}"));
                continue;
            }
            for name in ["charged", field] {
                let value = fields
                    .iter()
                    .find(|(k, _)| *k == name)
                    .and_then(|(_, v)| v.parse::<u64>().ok());
                if !matches!(value, Some(v) if v > 0) {
                    violations.push(format!("shape {shape}: {name} is {value:?}, want > 0"));
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

    /// A sorted level of one sample per timestamp at `timestamps`, deduped.
    fn level_at(
        timestamps: &std::collections::BTreeSet<i64>,
        prefix: u32,
    ) -> Vec<FoldedSample<f64>> {
        timestamps
            .iter()
            .map(|&ts| FoldedSample {
                ts,
                prefix,
                in_page_index: 0,
                value: ts as f64,
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Issue #4530: `merged_len`, which sizes each merge's buffer
        /// exactly, is the length `merge_into` produces, and the galloping
        /// window searches agree with a bisection of the whole level.
        #[test]
        fn merged_len_is_the_merge_output_length(
            a in prop::collection::btree_set(-40i64..40, 0..40),
            b in prop::collection::btree_set(-40i64..40, 0..40),
            bound in -45i64..45,
        ) {
            let (a, b) = (level_at(&a, 0), level_at(&b, 1));
            prop_assert_eq!(first_at_least(&a, bound), a.partition_point(|s| s.ts < bound));
            prop_assert_eq!(first_after(&a, bound), a.partition_point(|s| s.ts <= bound));
            let prefixes = vec![(1, 1, 1), (2, 1, 1)];
            let len = merged_len(&a, &b);
            let mut out = Vec::new();
            let mut dropped = 0u64;
            merge_into(&prefixes, a, b, &mut out, &mut dropped);
            prop_assert_eq!(out.len(), len);
        }
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
            let got = project_scalar(fold.into_series(usize::MAX, &mut Vec::new()).expect("finish"));
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
            let got = project_histograms(fold.into_series(usize::MAX, &mut Vec::new()).expect("finish"));
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
        let series = fold.into_series(usize::MAX, &mut Vec::new()).expect("finish");
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
        let err = fold
            .into_series(2, &mut Vec::new())
            .expect_err("three samples");
        let want = merge_soa_runs(objects, usize::MAX, 2).expect_err("merge");
        assert_eq!(format!("{err:?}"), format!("{want:?}"));
    }

    /// Issue #4530: after an absorb the fold charges exactly the buffers it
    /// holds, by capacity. Two objects carry the same series at the same
    /// three timestamps under two provenance prefixes; the merged result is
    /// one series of three samples, and the second object's copies are
    /// dropped, so after either absorb the fold holds one `by_series` table
    /// of 4 buckets, one spine of 4 levels, one level of 3 samples, the
    /// series' labels, a prefix Vec of 4 and a `prefix_ids` table of 4
    /// buckets, and no headroom, since the series is one level.
    #[test]
    fn the_fold_charges_exactly_the_buffers_it_holds_after_an_absorb() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let samples = [(1, 1.0), (2, 2.0), (3, 3.0)];
        fold.absorb(vec![scalar_run(7, &samples, (1, 1, 1), None)])
            .expect("absorb");

        // ts (8) + prefix index (4) + in-page index (4) + value (8).
        let sample = 24u64;
        assert_eq!(std::mem::size_of::<FoldedSample<f64>>() as u64, sample);
        let series_slot = std::mem::size_of::<(SeriesId, FoldedSeries<f64>)>() as u64;
        let id_slot = std::mem::size_of::<(ProvenancePrefix, u32)>() as u64;
        assert_eq!(id_slot, 32);
        let labels: u64 = labels(7)
            .iter()
            .map(|l| {
                std::mem::size_of::<Label>() as u64
                    + l.name.capacity() as u64
                    + l.value.capacity() as u64
            })
            .sum();
        let held = (4 * series_slot + 4 + GROUP_WIDTH)
            + 4 * std::mem::size_of::<Vec<FoldedSample<f64>>>() as u64
            + 3 * sample
            + labels
            + 4 * 24
            + (4 * id_slot + 4 + GROUP_WIDTH);
        assert_eq!(fold.charged(), held);
        assert_eq!(fold.charged(), live_bytes(&fold).total());
        assert_eq!(budget.reserved(), held);

        fold.absorb(vec![scalar_run(7, &samples, (2, 1, 1), None)])
            .expect("absorb");
        assert_eq!(fold.charged(), held, "the merged level replaced both");
        assert_eq!(budget.reserved(), held);

        let series = fold.into_series(usize::MAX, &mut Vec::new()).expect("finish");
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
        let seen = seen.lock().expect("lock").clone();
        let bound = seen[0];
        assert_eq!(seen, vec![bound; 3]);
        // Series 2's two runs merge, so the call reserved room for that
        // merge's level, which the trim after the call released.
        assert!(bound > charged, "bound {bound} B, charged {charged} B");
        assert_eq!(budget.reserved(), charged);
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
        let series = fold.into_series(usize::MAX, &mut Vec::new()).expect("finish");
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
        assert_eq!(fold.ledger.held, 0, "nothing was folded");
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

    /// What a fold's containers hold allocated, by part, under the R5
    /// pre-registration's definition (issue #4530): each Vec's
    /// `capacity() x size_of::<T>()`, plus each held histogram value's
    /// [`FoldValue::heap_len`], and each hash table as
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
            self.levels
                + self.spines
                + self.by_series
                + self.prefix_ids
                + self.prefixes
                + self.labels
        }
    }

    fn live_bytes<V: FoldValue>(fold: &RunFold<V>) -> LiveBytes {
        let sample = std::mem::size_of::<FoldedSample<V>>() as u64;
        let level = std::mem::size_of::<Vec<FoldedSample<V>>>() as u64;
        let label = std::mem::size_of::<Label>() as u64;
        let mut live = LiveBytes {
            levels: 0,
            spines: 0,
            by_series: support::table_bytes(
                fold.by_series.capacity(),
                std::mem::size_of::<(SeriesId, FoldedSeries<V>)>(),
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
        (fold.into_series(usize::MAX, &mut Vec::new()).expect("finish"), charged, live)
    }

    fn plain_fold(calls: Vec<Vec<FetchedSeriesSoa>>) -> Vec<SeriesData> {
        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        for call in calls {
            fold.absorb(call).expect("absorb");
        }
        fold.into_series(usize::MAX, &mut Vec::new()).expect("finish")
    }

    /// Folds one shape, prints its R5 figure line, asserts charged/live sits
    /// in `band` and that probing left the fold's result unchanged.
    fn r5_shape(shape: &str, calls: Vec<Vec<FetchedSeriesSoa>>, band: (f64, f64)) {
        let (probed, charged, live) = probed_fold(calls.clone());
        println!("{}", support::r5_line(shape, charged, live.total()));
        let ratio = charged as f64 / live.total() as f64;
        assert!(
            (band.0..=band.1).contains(&ratio),
            "shape {shape}: charged {charged} B / live {} B = {ratio:.4}, outside {band:?}",
            live.total()
        );
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

    /// Issue #4530 (ADR-4523 R5, step 1): the fold's charged bytes against
    /// the bytes its containers hold, at its peak charge, for pre-registered
    /// shapes A, B and C.
    ///
    /// Asserts step 2's adopt bands for charged/live: A and B in 1.00 to
    /// 1.05, C in 1.00 to 1.10, and that probing the fold leaves its result
    /// unchanged. Each shape prints its one figure line from one `r5_shape`
    /// call, so a presence check over those lines could not fail here;
    /// `r5_presence_rule_fails_missing_duplicate_and_zero_lines` covers the
    /// rule itself.
    #[test]
    fn r5_run_fold_charged_against_live_bytes() {
        let shapes = [
            ("A", support::shape_a().expect("shape A"), (1.00, 1.05)),
            ("B", support::shape_b().expect("shape B"), (1.00, 1.05)),
            ("C", support::shape_c().expect("shape C"), (1.00, 1.10)),
        ];
        for (shape, calls, band) in shapes {
            r5_shape(shape, calls, band);
        }
    }

    /// The presence rule fails a shape with no figure line, two figure
    /// lines, a zero figure, or a second figure named for another shape's
    /// kind: `live` for D, `process_delta` for A to C.
    #[test]
    fn r5_presence_rule_fails_missing_duplicate_and_zero_lines() {
        let a = support::r5_line("A", 10, 20);
        let b = support::r5_line("B", 30, 40);
        let shapes = ["A", "B"];
        assert!(support::presence_violations(&[a.clone(), b.clone()], &shapes).is_empty());
        assert_eq!(
            support::presence_violations(std::slice::from_ref(&a), &shapes),
            vec!["shape B: 0 figure lines, want exactly 1".to_string()]
        );
        assert_eq!(
            support::presence_violations(&[a.clone(), b.clone(), b], &shapes),
            vec!["shape B: 2 figure lines, want exactly 1".to_string()]
        );
        assert_eq!(
            support::presence_violations(&[a.clone(), support::r5_line("B", 30, 0)], &shapes),
            vec!["shape B: live is Some(0), want > 0".to_string()]
        );

        let d = support::r5_line("D", 50, 60);
        assert_eq!(
            d,
            "R5 shape=D charged=50 process_delta=60 ratio=0.8333 gap_bytes=-10"
        );
        assert!(support::presence_violations(&[a.clone(), d], &["A", "D"]).is_empty());
        let d_as_live = "R5 shape=D charged=50 live=60 ratio=0.8333 gap_bytes=-10".to_string();
        assert_eq!(
            support::presence_violations(&[a, d_as_live], &["A", "D"]),
            vec![
                "shape D: fields [\"charged\", \"live\", \"ratio\", \"gap_bytes\"], want \
                 [\"charged\", \"process_delta\", \"ratio\", \"gap_bytes\"]"
                    .to_string()
            ]
        );
        let a_as_delta =
            "R5 shape=A charged=10 process_delta=20 ratio=0.5000 gap_bytes=-10".to_string();
        assert_eq!(
            support::presence_violations(&[a_as_delta], &["A"]),
            vec![
                "shape A: fields [\"charged\", \"process_delta\", \"ratio\", \"gap_bytes\"], \
                 want [\"charged\", \"live\", \"ratio\", \"gap_bytes\"]"
                    .to_string()
            ]
        );
    }

    /// FNV-1a over `text`: a digest of a result's projection that is stable
    /// across builds, so an expected value captured from one build pins the
    /// result of another.
    fn fnv(text: &str) -> u64 {
        text.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    }

    fn scalar_digest(series: Vec<SeriesData>) -> u64 {
        fnv(&format!("{:?}", project_scalar(series)))
    }

    /// A histogram fold shape: 50 series, 12 calls, call `k` holding one run
    /// per series over slots `3k..3k+6`, every sample of it carrying the
    /// per-sample priority `(k / 2, 1, 1, 0)`. A slot is written by two
    /// calls whose values differ: calls `2j` and `2j + 1` carry the same
    /// priority, so the structural key breaks their tie, and calls `2j + 1`
    /// and `2j + 2` do not.
    fn histogram_shape() -> Vec<Vec<FetchedHistogramSeries>> {
        (0..12i64)
            .map(|k| {
                (0..50u8)
                    .map(|s| {
                        let slots = 3 * k..3 * k + 6;
                        FetchedHistogramSeries {
                            series_id: SeriesId([s; 16]),
                            labels: labels(s),
                            timestamps: slots.clone().collect(),
                            values: slots
                                .clone()
                                .map(|t| {
                                    histogram_value(
                                        f64::from(s) + t as f64 * 0.25 + k as f64,
                                        (t + k) as u64,
                                    )
                                })
                                .collect(),
                            created_unix_ns: k / 2,
                            writer_epoch: 1,
                            writer_seq: 1,
                            per_sample_priorities: Some(
                                slots
                                    .map(|_| SamplePriority {
                                        created_unix_ns: k / 2,
                                        writer_epoch: 1,
                                        writer_seq: 1,
                                        in_page_index: 0,
                                    })
                                    .collect(),
                            ),
                        }
                    })
                    .collect()
            })
            .collect()
    }

    /// The histogram shape's priorities tie where the doc says, so the
    /// digest below covers the structural tie-break.
    #[test]
    fn the_histogram_shape_ties_overlapping_calls_two_by_two() {
        let shape = histogram_shape();
        let priority = |k: usize, slot: i64| {
            let run = &shape[k][0];
            let pos = run.timestamps.iter().position(|&t| t == slot).expect("slot");
            run.per_sample_priorities.as_ref().expect("column")[pos].as_tuple()
        };
        // Slot 3 is written by calls 0 and 1, slot 6 by calls 1 and 2.
        assert_eq!(priority(0, 3), priority(1, 3));
        assert_ne!(priority(1, 6), priority(2, 6));
        assert_ne!(shape[0][0].values[3].sum, shape[1][0].values[0].sum);
    }

    /// Digests of the scalar fold's output on shapes A, B and C and of the
    /// histogram fold's on [`histogram_shape`], captured from main's code
    /// (c9ef663) before the R5 step 2 change; on main the histogram digest
    /// is the same from the fold and from `merge_histogram_soa_runs`. The
    /// fold must return exactly these results.
    #[test]
    fn fold_outputs_are_bit_identical_to_main() {
        let want = vec![
            ("A", 0x6d85_8021_ceff_5993u64),
            ("B", 0xcda8_6cf8_badf_d242),
            ("C", 0x4974_8c5d_63fc_83d6),
            ("histogram", 0x8ed6_3c00_9ee8_0418),
        ];
        let mut got = Vec::new();
        for (shape, calls) in [
            ("A", support::shape_a().expect("shape A")),
            ("B", support::shape_b().expect("shape B")),
            ("C", support::shape_c().expect("shape C")),
        ] {
            got.push((shape, scalar_digest(plain_fold(calls))));
        }
        let mut fold = HistogramFold::new(unlimited(), usize::MAX);
        for call in histogram_shape() {
            fold.absorb(call).expect("absorb");
        }
        let histograms = project_histograms(fold.into_series(usize::MAX, &mut Vec::new()).expect("finish"));
        got.push(("histogram", fnv(&format!("{histograms:?}"))));
        for (shape, digest) in &got {
            println!("R5 digest shape={shape} {digest:#018x}");
        }
        assert_eq!(got, want);
    }

    /// Issue #4118: the fold never holds more than it has reserved. After
    /// every absorb of shapes A, B and C, the bytes its containers hold, by
    /// step 1's definition ([`live_bytes`]), are at most
    /// [`RunFold::charged`].
    #[test]
    fn the_fold_holds_no_more_than_it_charged_after_every_absorb() {
        let shapes = [
            ("A", support::shape_a().expect("shape A")),
            ("B", support::shape_b().expect("shape B")),
            ("C", support::shape_c().expect("shape C")),
        ];
        for (shape, calls) in shapes {
            let mut fold = ScalarFold::new(unlimited(), usize::MAX);
            for (call, runs) in calls.into_iter().enumerate() {
                fold.absorb(runs).expect("absorb");
                let live = live_bytes(&fold).total();
                assert!(
                    live <= fold.charged(),
                    "shape {shape}, call {call}: live {live} B > charged {} B",
                    fold.charged()
                );
            }
        }
    }

    /// Issue #4530: every absorb of shapes A, B and C succeeds, after each
    /// one the fold's running count of what it holds is exactly
    /// [`live_bytes`] recomputed from its containers, and it charges exactly
    /// that plus its finish headroom, which is 0 for A and B, whose series
    /// are each one level; the budget holds exactly the charge. The result
    /// equals `merge_soa_runs` over every call's runs at once.
    #[test]
    fn every_absorb_succeeds_charging_what_the_fold_holds_plus_headroom() {
        let shapes = [
            ("A", support::shape_a().expect("shape A")),
            ("B", support::shape_b().expect("shape B")),
            ("C", support::shape_c().expect("shape C")),
        ];
        for (shape, calls) in shapes {
            let want = project_scalar(
                merge_soa_runs(calls.clone(), usize::MAX, usize::MAX).expect("merge"),
            );
            let budget = unlimited();
            let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
            for (call, runs) in calls.into_iter().enumerate() {
                fold.absorb(runs).expect("absorb");
                let live = live_bytes(&fold).total();
                assert_eq!(fold.ledger.held, live, "shape {shape}, call {call}");
                assert_eq!(
                    fold.charged(),
                    live + fold.finish_headroom,
                    "shape {shape}, call {call}"
                );
                assert_eq!(budget.reserved(), fold.charged(), "shape {shape}");
            }
            if shape != "C" {
                assert_eq!(fold.finish_headroom, 0, "shape {shape}");
            }
            let got = project_scalar(fold.into_series(usize::MAX, &mut Vec::new()).expect("finish"));
            assert_eq!(got, want, "shape {shape}");
            assert_eq!(budget.reserved(), 0, "shape {shape}");
        }
    }

    /// Three series that fill a `by_series` table of 4 buckets.
    fn three_series() -> Vec<FetchedSeriesSoa> {
        (1..=3)
            .map(|s| scalar_run(s, &[(1, 1.0), (2, 2.0)], (1, 1, 1), None))
            .collect()
    }

    /// Issue #4530: a budget too small for the `by_series` table's growth
    /// to its next bucket count refuses the absorb that needs it with
    /// [`FetchError::FetchMemoryExhausted`], before anything is folded,
    /// leaves the fold charging exactly what it holds, and returns the
    /// budget's reservation to its value before the call.
    #[test]
    fn a_budget_too_small_for_one_table_growth_refuses_and_keeps_the_charge_exact() {
        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        fold.absorb(three_series()).expect("absorb");
        let before = fold.charged();
        assert_eq!(fold.by_series.capacity(), 3, "the table is full");

        let slot = std::mem::size_of::<(SeriesId, FoldedSeries<f64>)>();
        let table_growth = table_alloc(8, slot) - table_alloc(4, slot);
        let budget = Arc::new(ravel_memory::MemoryBudget::new(before + table_growth - 1));
        // Move the fold's charge onto the small budget, as if it had been
        // folding against it all along.
        fold.ledger.budget = Arc::clone(&budget);
        fold.ledger.reservations.clear();
        fold.ledger.charged = 0;
        fold.ledger.cover(before).expect("the held bytes fit");

        let err = fold
            .absorb(vec![scalar_run(4, &[(1, 1.0)], (1, 1, 1), None)])
            .expect_err("the table growth does not fit");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted {
                    cause: ravel_memory::ExhaustionCause::Accounted,
                    ..
                })
            ),
            "{err:?}"
        );
        assert_eq!(fold.by_series.len(), 3, "nothing was folded");
        assert_eq!(fold.by_series.capacity(), 3, "the table did not grow");
        assert_eq!(fold.charged(), live_bytes(&fold).total());
        assert_eq!(fold.charged(), before);
        assert_eq!(budget.reserved(), before);
    }

    /// A run of series 1 at `ts` in `start..start + n` with one duplicate of
    /// the first timestamp, which the fold drops, so its level is copied to
    /// an exact buffer, an allocation the call's bound does not count.
    fn duplicated_run(start: i64, n: i64) -> FetchedSeriesSoa {
        let mut samples: Vec<(i64, f64)> = (start..start + n).map(|t| (t, t as f64)).collect();
        samples.insert(1, (start, -1.0));
        scalar_run(1, &samples, (1, 1, 1), None)
    }

    /// Issue #4530: an absorb whose bound fits and whose exact copy of a
    /// level does not is refused mid-absorb with
    /// [`FetchError::FetchMemoryExhausted`]; the fold then charges exactly
    /// what it holds and the budget's reservation returns to its value
    /// before the call.
    #[test]
    fn a_refusal_mid_absorb_returns_the_budget_to_its_value_before_the_call() {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1 << 20));
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let first: Vec<(i64, f64)> = (0..60).map(|t| (t, t as f64)).collect();
        fold.absorb(vec![scalar_run(1, &first, (1, 1, 1), None)])
            .expect("absorb");
        // 60 held samples against 21 incoming do not merge, so the call's
        // bound carries no merge transient that would cover the copy.
        let runs = vec![duplicated_run(100, 20)];
        let (bound, _) = fold.call_bound(&runs);
        let filler = budget
            .reserve(budget.limit() - budget.reserved() - (bound - fold.charged()))
            .expect("filler");
        let before = budget.reserved();
        let held = fold.ledger.held;

        let err = fold.absorb(runs).expect_err("the copy does not fit");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted { limit, .. })
                    if limit == 1 << 20
            ),
            "{err:?}"
        );
        assert_eq!(fold.by_series[&SeriesId([1; 16])].levels.len(), 1);
        assert_eq!(fold.ledger.held, held);
        assert_eq!(fold.ledger.held, live_bytes(&fold).total());
        assert_eq!(fold.charged(), fold.ledger.held);
        assert_eq!(budget.reserved(), before);
        drop(fold);
        drop(filler);
        assert_eq!(budget.reserved(), 0);
    }

    /// A trim reserves its smaller reservation before it releases the call's
    /// larger one, so a budget the call filled refuses it and the trim gives
    /// up: the fold stays charged the call's bound, never less than it
    /// holds, until a later trim succeeds. That happens after a refused
    /// absorb and after one that succeeded; a later call's trim succeeds
    /// once the budget has room for the smaller reservation beside the
    /// larger.
    #[test]
    fn a_trim_the_budget_refuses_keeps_the_fold_over_charged_until_a_later_trim() {
        let runs = vec![duplicated_run(0, 20)];
        let (bound, _) = ScalarFold::new(unlimited(), usize::MAX).call_bound(&runs);
        let budget = Arc::new(ravel_memory::MemoryBudget::new(bound));
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let err = fold.absorb(runs).expect_err("the copy does not fit");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted { limit, .. }) if limit == bound
            ),
            "{err:?}"
        );
        assert!(fold.by_series.is_empty(), "the series was not added");
        assert_eq!(fold.ledger.held, live_bytes(&fold).total());
        assert!(fold.ledger.held > 0, "the tables grew before the copy");
        assert_eq!(fold.charged(), bound);
        assert_eq!(budget.reserved(), bound);
        drop(fold);
        assert_eq!(budget.reserved(), 0);

        // A call that succeeds and fills the budget: series 1's two runs
        // merge, so the bound carries the merge's level.
        let runs = vec![
            scalar_run(1, &[(1, 1.0), (2, 2.0), (3, 3.0)], (1, 1, 1), None),
            scalar_run(1, &[(2, 5.0), (3, 6.0), (4, 7.0)], (2, 1, 1), None),
        ];
        let (bound, _) = ScalarFold::new(unlimited(), usize::MAX).call_bound(&runs);
        let budget = Arc::new(ravel_memory::MemoryBudget::new(2 * bound));
        let filler = budget.reserve(bound).expect("filler");
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(runs).expect("the bound fits");
        let keep = fold.ledger.held + fold.finish_headroom;
        assert!(keep < bound, "the merge room is idle after the call");
        assert_eq!(fold.charged(), bound, "the trim gave up");
        assert_eq!(budget.reserved(), 2 * bound);

        drop(filler);
        fold.absorb(Vec::<FetchedSeriesSoa>::new())
            .expect("an empty call");
        assert_eq!(fold.charged(), keep, "the next trim succeeded");
        assert_eq!(budget.reserved(), keep);
        drop(fold);
        assert_eq!(budget.reserved(), 0);
    }

    /// Two absorb calls whose second one allocates at every kind of site:
    /// the `by_series` table's growth, a level per run, a merge with a held
    /// level and between two runs of one call, a spine's growth, an exact
    /// copy of a level with a duplicate timestamp, and the prefix Vec's and
    /// `prefix_ids`' growth. Series 13 ends as two levels, so `into_series`
    /// merges.
    fn injection_calls() -> [Vec<FetchedSeriesSoa>; 2] {
        let eight: Vec<(i64, f64)> = (0..8).map(|t| (t, t as f64)).collect();
        let sixteen: Vec<(i64, f64)> = (0..16).map(|t| (t, t as f64)).collect();
        let mut first: Vec<_> = (1..=3)
            .map(|s| scalar_run(s, &eight, (1, 1, 1), None))
            .collect();
        first.push(scalar_run(13, &sixteen, (1, 1, 1), None));
        let later: Vec<(i64, f64)> = (4..12).map(|t| (t, t as f64 + 0.5)).collect();
        let mut second = vec![
            scalar_run(1, &later, (2, 1, 1), None),
            scalar_run(2, &later, (2, 1, 1), None),
            scalar_run(2, &eight, (3, 1, 1), None),
            scalar_run(11, &[(1, 1.0), (1, 2.0), (2, 1.0)], (2, 1, 1), None),
            scalar_run(13, &[(20, 1.0), (21, 1.0)], (2, 1, 1), None),
        ];
        second.extend((4..=10).map(|s| scalar_run(s, &eight, (2, 1, 1), None)));
        let column = (0..8)
            .map(|t| SamplePriority {
                created_unix_ns: 10 + t,
                writer_epoch: 1,
                writer_seq: 1,
                in_page_index: 0,
            })
            .collect();
        second.push(scalar_run(12, &eight, (0, 1, 1), Some(column)));
        [first, second]
    }

    /// [`injection_calls`] as histogram runs: each value `v` becomes a
    /// histogram of sum `v` and one bucket, so every held sample carries 16
    /// heap bytes, its span and its bucket.
    fn histogram_injection_calls() -> [Vec<FetchedHistogramSeries>; 2] {
        injection_calls().map(|call| call.into_iter().map(as_histogram).collect())
    }

    /// `run` with each value `v` as a histogram of sum `v` and one bucket.
    fn as_histogram(run: FetchedSeriesSoa) -> FetchedHistogramSeries {
        FetchedHistogramSeries {
            series_id: run.series_id,
            labels: run.labels,
            timestamps: run.timestamps,
            values: run
                .values
                .iter()
                .map(|&v| histogram_value(v, v.abs() as u64))
                .collect(),
            created_unix_ns: run.created_unix_ns,
            writer_epoch: run.writer_epoch,
            writer_seq: run.writer_seq,
            per_sample_priorities: run.per_sample_priorities,
        }
    }

    /// Issue #4530: a budget refusal of a histogram absorb, swept one byte of
    /// limit at a time from 256 bytes under the second call's bound to the
    /// smallest limit the call fits, leaves the fold's running count equal to
    /// [`live_bytes`] recomputed from its containers, heap bytes included,
    /// the fold charged at least that, and the budget holding exactly the
    /// charge until the fold drops. The second call ends with series 255,
    /// whose 100-sample run carries a duplicate timestamp: its exact copy lies
    /// outside the call's bound and is larger than the merge room the bound
    /// keeps, so some limits admit the bound and refuse the copy, after the
    /// call's other series were folded: the series count grew before the
    /// refusal.
    #[test]
    fn a_budget_refusal_of_a_histogram_absorb_keeps_the_count_exact() {
        let [first, mut second] = histogram_injection_calls();
        let mut samples: Vec<(i64, f64)> = (0..100).map(|t| (t, t as f64)).collect();
        samples.insert(1, (0, -1.0));
        second.push(as_histogram(scalar_run(255, &samples, (1, 1, 1), None)));
        let mut probe = HistogramFold::new(unlimited(), usize::MAX);
        probe.absorb(first.clone()).expect("absorb");
        let mut sorted = second.clone();
        sorted.sort_unstable_by_key(FoldInput::series_id);
        let (bound, _) = probe.call_bound(&sorted);
        let mut refused = 0usize;
        let mut mid_call = 0usize;
        for limit in bound - 256.. {
            let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
            let mut fold = HistogramFold::new(Arc::clone(&budget), usize::MAX);
            if fold.absorb(first.clone()).is_err() {
                continue;
            }
            let series_before = fold.by_series.len();
            let Err(err) = fold.absorb(second.clone()) else {
                break;
            };
            refused += 1;
            assert!(
                matches!(
                    err,
                    QueryError::Fetch(FetchError::FetchMemoryExhausted { .. })
                ),
                "limit {limit}: {err:?}"
            );
            if fold.by_series.len() > series_before {
                mid_call += 1;
            }
            let live = live_bytes(&fold).total();
            assert!(live > 0, "limit {limit}");
            assert_eq!(fold.ledger.held, live, "limit {limit}");
            assert!(fold.charged() >= live, "limit {limit}");
            assert_eq!(budget.reserved(), fold.charged(), "limit {limit}");
            drop(fold);
            assert_eq!(budget.reserved(), 0, "limit {limit}");
        }
        assert!(refused > 256, "only {refused} limits refused the call");
        assert!(mid_call > 0, "no refusal came after part of the call was folded");
        println!("histogram budget sweep: {refused} refused, {mid_call} part way through");
    }

    /// Folds `calls` into a fold on an unlimited budget and checks the #4118
    /// invariant across [`RunFold::into_series`]: the most it holds live at
    /// once while it merges, converts and builds the result, by
    /// [`live_bytes`]' definition plus the result Vec, is at most what the
    /// fold charged plus the result reservation. Returns the peak and that
    /// charge.
    fn finish_peak(calls: Vec<Vec<FetchedSeriesSoa>>) -> (u64, u64) {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        for call in calls {
            fold.absorb(call).expect("absorb");
        }
        assert_eq!(fold.ledger.held, live_bytes(&fold).total(), "probe start");
        let charged = fold.charged();
        let result = result_len::<SeriesData>(fold.by_series.len());
        let mut held = Vec::new();
        let series = fold.into_series(usize::MAX, &mut held).expect("finish");
        let (peak, covered) = support::take_finish_peak().expect("finish recorded");
        assert_eq!(covered, charged + result);
        let sizes: u64 = held.iter().map(ravel_memory::Reservation::size).sum();
        assert_eq!(sizes, result, "the result reservation is handed over");
        assert_eq!(budget.reserved(), result, "the fold's own are released");
        drop(series);
        drop(held);
        assert_eq!(budget.reserved(), 0);
        (peak, covered)
    }

    /// Issue #4118 across finish: for shapes A, B and C, `into_series`
    /// never holds more than the fold charged plus the result reservation.
    /// Shape A's 20,000 one-level series hold nothing past their level but
    /// the result Vec, so without that reservation its peak exceeds the
    /// charge by exactly the result Vec, 20,000 x 48 bytes.
    #[test]
    fn the_fold_holds_no_more_than_it_charged_across_finish() {
        for (shape, calls) in [
            ("A", support::shape_a().expect("shape A")),
            ("B", support::shape_b().expect("shape B")),
            ("C", support::shape_c().expect("shape C")),
        ] {
            let (peak, covered) = finish_peak(calls);
            println!("R5 finish shape={shape} peak_live={peak} charged={covered}");
            assert!(
                peak <= covered,
                "shape {shape}: peak live {peak} B > charged {covered} B"
            );
        }
        assert_eq!(result_len::<SeriesData>(20_000), 960_000);
    }

    /// Issue #4530: `into_series` reserves its result Vec before allocating
    /// it and pushes that reservation to `held`, which holds it with the
    /// result after the taken reservations; a budget that refuses it fails
    /// with [`FetchError::FetchMemoryExhausted`] for exactly the result Vec
    /// and releases everything the fold held.
    #[test]
    fn into_series_reserves_its_result_vec_and_hands_it_over() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(three_series()).expect("absorb");
        let charged = fold.charged();
        let result = result_len::<SeriesData>(3);
        assert_eq!(result, 3 * 48);
        let mut held = fold.take_reservations();
        let series = fold.into_series(usize::MAX, &mut held).expect("finish");
        assert_eq!(series.len(), 3);
        assert_eq!(budget.reserved(), charged + result);
        drop(series);
        assert_eq!(budget.reserved(), charged + result, "held keeps both");
        drop(held);
        assert_eq!(budget.reserved(), 0);

        let mut fold = ScalarFold::new(unlimited(), usize::MAX);
        fold.absorb(three_series()).expect("absorb");
        let before = fold.charged();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(before + result - 1));
        fold.ledger.budget = Arc::clone(&budget);
        fold.ledger.reservations.clear();
        fold.ledger.charged = 0;
        fold.ledger.cover(before).expect("the held bytes fit");
        let mut held = Vec::new();
        let err = fold
            .into_series(usize::MAX, &mut held)
            .expect_err("the result Vec does not fit");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted { requested, .. })
                    if requested == result
            ),
            "{err:?}"
        );
        assert!(held.is_empty());
        assert_eq!(budget.reserved(), 0);
    }

    /// The conversion claims in [`RunFold::finish`]'s docs: a [`Sample`]
    /// and a [`ravel_promql::HistogramSample`] are each smaller than the held
    /// sample they convert and equally aligned, so the collect can reuse the
    /// held level's buffer; and a converted histogram value's heap bytes
    /// equal the held value's [`FoldValue::heap_len`].
    #[test]
    fn each_conversion_shrinks_a_sample_and_keeps_its_heap_bytes() {
        use std::mem::{align_of, size_of};
        type HistogramOut = ravel_promql::HistogramSample;
        assert!(size_of::<Sample>() < size_of::<FoldedSample<f64>>());
        assert_eq!(align_of::<Sample>(), align_of::<FoldedSample<f64>>());
        assert!(size_of::<HistogramOut>() < size_of::<FoldedSample<HistogramValue>>());
        assert_eq!(
            align_of::<HistogramOut>(),
            align_of::<FoldedSample<HistogramValue>>()
        );
        let mut value = histogram_value(2.0, 3);
        value.custom_values = Some(vec![1.0, 2.0]);
        value.negative_spans = vec![HistogramSpan {
            offset: 1,
            length: 2,
        }];
        let float = histogram_value_to_float(&value);
        let heap = (float.positive_spans.capacity() + float.negative_spans.capacity())
            * size_of::<ravel_promql::Span>()
            + (float.positive_buckets.capacity()
                + float.negative_buckets.capacity()
                + float.custom_values.capacity())
                * size_of::<f64>();
        assert_eq!(heap as u64, value.heap_len());
    }

    /// Issue #4530: the finish headroom a series raises when it ends a call
    /// as more than one level lies outside the call's bound when the bound
    /// did not predict the series to merge, so a budget that admits the
    /// bound can refuse it after the run was folded. The run stays folded,
    /// and the fold is charged at least what it holds.
    #[test]
    fn finish_headroom_for_a_series_not_predicted_to_merge_is_refused_after_folding() {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1 << 20));
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        let first: Vec<(i64, f64)> = (0..60).map(|t| (t, t as f64)).collect();
        fold.absorb(vec![scalar_run(1, &first, (1, 1, 1), None)])
            .expect("absorb");
        // 60 held samples against 21 incoming do not merge.
        let later: Vec<(i64, f64)> = (100..121).map(|t| (t, t as f64)).collect();
        let runs = vec![scalar_run(1, &later, (1, 1, 1), None)];
        let (bound, _) = fold.call_bound(&runs);
        let filler = budget
            .reserve(budget.limit() - budget.reserved() - (bound - fold.charged()))
            .expect("filler");

        let err = fold.absorb(runs).expect_err("the headroom does not fit");
        assert!(
            matches!(
                err,
                QueryError::Fetch(FetchError::FetchMemoryExhausted { requested, .. })
                    if requested == fold.finish_headroom
            ),
            "{err:?}"
        );
        assert_eq!(fold.by_series[&SeriesId([1; 16])].levels.len(), 2, "folded");
        assert_eq!(fold.finish_headroom, 81 * 24);
        assert_eq!(fold.ledger.held, live_bytes(&fold).total());
        assert_eq!(fold.charged(), bound);
        assert_eq!(fold.charged(), fold.ledger.held, "the bound held no headroom");
        drop(fold);
        drop(filler);
        assert_eq!(budget.reserved(), 0);
    }

    /// Folds `first`, arms an allocation failure after `skip` allocations,
    /// folds `second`, and checks what an injected failure leaves: the absorb
    /// fails with [`QueryError::AllocationRefused`] and the charge taken for
    /// it is released exactly once, so afterwards the fold's running count
    /// equals [`live_bytes`] recomputed from its containers (a charge kept or
    /// released twice shows as a difference of the failed allocation's
    /// size), it charges exactly that plus its headroom, and the budget holds
    /// exactly the charge and returns to 0 when the fold drops. Returns
    /// whether a failure was injected.
    fn inject_once<V: FoldValue, R: FoldInput<V>>(
        first: Vec<R>,
        second: Vec<R>,
        skip: usize,
    ) -> bool {
        let budget = unlimited();
        let mut fold = RunFold::<V>::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(first).expect("absorb");
        support::fail_allocation_after(skip);
        let result = fold.absorb(second);
        let Some(charge) = support::injected_failure_charge() else {
            support::clear_allocation_failure();
            result.expect("no failure was injected");
            return false;
        };
        let err = result.expect_err("the injected failure fails the absorb");
        assert!(
            matches!(err, QueryError::AllocationRefused { .. }),
            "allocation {skip}: {err:?}"
        );
        let live = live_bytes(&fold).total();
        assert_eq!(fold.ledger.held, live, "allocation {skip}");
        assert_eq!(
            fold.charged(),
            live + fold.finish_headroom,
            "allocation {skip}"
        );
        assert!(charge >= fold.charged(), "allocation {skip}: charged first");
        assert_eq!(budget.reserved(), fold.charged(), "allocation {skip}");
        assert_eq!(budget.accounted_refusals(), 0, "allocation {skip}");
        drop(fold);
        assert_eq!(budget.reserved(), 0, "allocation {skip}");
        true
    }

    /// Issue #4530: an allocation the allocator refuses, injected at each of
    /// the second absorb's allocation sites in turn, is released exactly
    /// once ([`inject_once`]).
    #[test]
    fn an_injected_allocation_failure_releases_its_charge_exactly_once() {
        let mut fired = 0usize;
        for skip in 0.. {
            let [first, second] = injection_calls();
            if !inject_once(first, second, skip) {
                break;
            }
            fired += 1;
        }
        assert!(fired >= 20, "only {fired} allocation sites failed");
    }

    /// The same sweep over the histogram fold, whose held samples carry
    /// heap bytes: a failure at a merge, a spine growth, an exact copy or a
    /// table growth releases the heap bytes of the level it drops, and a
    /// merge's dropped samples' heap bytes, exactly once.
    #[test]
    fn an_injected_allocation_failure_in_the_histogram_fold_releases_its_charge_exactly_once() {
        let mut fired = 0usize;
        for skip in 0.. {
            let [first, second] = histogram_injection_calls();
            if !inject_once(first, second, skip) {
                break;
            }
            fired += 1;
        }
        assert!(fired >= 20, "only {fired} allocation sites failed");
    }

    /// An allocation refused while `into_series` merges is
    /// [`QueryError::AllocationRefused`], and the fold's reservation and the
    /// result reservation are released with it.
    #[test]
    fn an_injected_allocation_failure_in_into_series_is_typed_and_releases() {
        for skip in [0, 1] {
            let budget = unlimited();
            let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
            for call in injection_calls() {
                fold.absorb(call).expect("absorb");
            }
            assert!(fold.finish_headroom > 0, "series 13 is two levels");
            support::fail_allocation_after(skip);
            let mut held = Vec::new();
            let err = fold
                .into_series(usize::MAX, &mut held)
                .expect_err("refused");
            assert!(support::injected_failure_charge().is_some(), "fired");
            assert!(
                matches!(err, QueryError::AllocationRefused { .. }),
                "allocation {skip}: {err:?}"
            );
            assert!(held.is_empty(), "allocation {skip}");
            assert_eq!(budget.reserved(), 0, "allocation {skip}");
        }
    }

    /// Issue #4530: an allocator refusal is its own typed error, not the
    /// budget's: its message says the allocator refused, and it moves no
    /// budget refusal counter.
    #[test]
    fn an_allocator_refusal_says_so_and_counts_no_budget_refusal() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        support::fail_allocation_after(0);
        let err = fold
            .absorb(vec![scalar_run(1, &[(1, 1.0)], (1, 1, 1), None)])
            .expect_err("refused");
        assert!(support::injected_failure_charge().is_some(), "fired");
        let QueryError::AllocationRefused { requested } = err else {
            panic!("{err:?}");
        };
        assert_eq!(
            err.to_string(),
            format!("query memory exhausted: the allocator refused a {requested}-byte allocation")
        );
        assert_eq!(budget.accounted_refusals(), 0);
        assert_eq!(budget.gate_refusals(), 0);
    }

    /// Issue #4530: a fold dropped between absorbs, as a cancelled query
    /// drops it, returns the budget to what it held before the fold started,
    /// leaving another holder's reservation in place.
    #[test]
    fn dropping_the_fold_mid_run_returns_the_budget_to_its_start() {
        let budget = unlimited();
        let other = budget.reserve(1000).expect("reserve");
        let start = budget.reserved();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        for call in support::shape_c().expect("shape C").into_iter().take(10) {
            fold.absorb(call).expect("absorb");
        }
        assert!(budget.reserved() > start);
        drop(fold);
        assert_eq!(budget.reserved(), start);
        drop(other);
        assert_eq!(budget.reserved(), 0);
    }

    /// Issue #4530: the reservations `take_reservations` hands over hold
    /// exactly what the fold charged, and the fold charges nothing after, so
    /// nothing is charged twice while the result is built and held.
    #[test]
    fn taken_reservations_hold_exactly_the_charge_and_nothing_twice() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        for call in injection_calls() {
            fold.absorb(call).expect("absorb");
        }
        let charged = fold.charged();
        let taken = fold.take_reservations();
        let sizes: u64 = taken.iter().map(ravel_memory::Reservation::size).sum();
        assert_eq!(sizes, charged);
        assert_eq!(fold.charged(), 0);
        assert_eq!(budget.reserved(), charged);
        let series = fold.into_series(usize::MAX, &mut Vec::new()).expect("finish");
        assert_eq!(budget.reserved(), charged, "held with the series");
        drop(series);
        drop(taken);
        assert_eq!(budget.reserved(), 0);
    }

    /// Issue #4530: the charge is released when its last holder drops,
    /// whichever of the fold and the taken reservations that is.
    #[test]
    fn the_charge_is_released_on_its_last_holders_drop() {
        let budget = unlimited();
        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(three_series()).expect("absorb");
        let charged = fold.charged();
        let taken = fold.take_reservations();
        drop(fold);
        assert_eq!(budget.reserved(), charged, "the taken reservations hold it");
        drop(taken);
        assert_eq!(budget.reserved(), 0);

        let mut fold = ScalarFold::new(Arc::clone(&budget), usize::MAX);
        fold.absorb(three_series()).expect("absorb");
        let taken = fold.take_reservations();
        drop(taken);
        assert_eq!(budget.reserved(), 0, "the fold charges nothing once taken");
        drop(fold);
        assert_eq!(budget.reserved(), 0);
    }

    /// `table_bytes`' bucket count inverts hashbrown's capacity exactly: for
    /// every table size up to 10,000 entries, the capacity a `HashMap`
    /// reports is exactly the capacity of the bucket count recovered from
    /// it, so no reported capacity falls between two bucket counts.
    #[test]
    fn r5_table_buckets_invert_hashmap_capacity() {
        for entries in 1..=10_000usize {
            let table: HashMap<u64, u64> = HashMap::with_capacity(entries);
            let capacity = table.capacity();
            let buckets = table_buckets(capacity);
            assert!(buckets.is_power_of_two() && buckets >= 4);
            assert_eq!(
                bucket_capacity(buckets),
                capacity,
                "with_capacity({entries}) reports capacity {capacity}, which no bucket count holds"
            );
        }
    }
}
