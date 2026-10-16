//! Before/after for ticket A1b: the old materialized merged window (a
//! per-series `HashMap<ts, Candidate>` filled from every segment run, then
//! drained and sorted) versus the new lazy k-way merge over per-segment SoA
//! runs (a min-heap keyed by ts, emitting ascending deduplicated samples
//! with no per-ts map and no final sort).
//!
//! The engine's real `merge_soa_runs` is private, so this bench carries a
//! faithful standalone copy of both algorithms and drives them over the
//! same synthetic per-segment runs. They are independent implementations of
//! the same normative order (docs/catalog-and-mvcc.md), and the bench
//! asserts they agree before timing, so the measurement is a like-for-like
//! delta of the merge step on one host, not a claim about any other code.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// One per-segment run of a single series: on-disk order (ascending ts,
/// duplicates preserved), plus the shared provenance prefix.
#[derive(Clone)]
struct Run {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    prefix: (i64, u64, u64),
}

#[derive(Clone, Copy)]
struct Candidate {
    value: f64,
    priority: (i64, u64, u64, u32),
}

fn is_greater(a: &Candidate, b: &Candidate) -> bool {
    (a.priority, a.value.to_bits()) > (b.priority, b.value.to_bits())
}

/// OLD: materialize a per-ts map for each series, then drain and sort.
fn merge_materialized(series: &[Vec<Run>]) -> Vec<Vec<(i64, f64)>> {
    let mut out = Vec::with_capacity(series.len());
    for runs in series {
        let mut map: HashMap<i64, Candidate> = HashMap::new();
        for run in runs {
            for (idx, (&ts, &value)) in run.timestamps.iter().zip(&run.values).enumerate() {
                let candidate = Candidate {
                    value,
                    priority: (run.prefix.0, run.prefix.1, run.prefix.2, idx as u32),
                };
                map.entry(ts)
                    .and_modify(|existing| {
                        if is_greater(&candidate, existing) {
                            *existing = candidate;
                        }
                    })
                    .or_insert(candidate);
            }
        }
        let mut samples: Vec<(i64, f64)> = map.into_iter().map(|(ts, c)| (ts, c.value)).collect();
        samples.sort_by_key(|s| s.0);
        out.push(samples);
    }
    out
}

/// NEW: lazy k-way merge over the already-sorted runs, no per-ts map, no
/// final sort. Mirrors `engine::merge_series_runs`.
fn merge_kway(series: &[Vec<Run>]) -> Vec<Vec<(i64, f64)>> {
    let mut out = Vec::with_capacity(series.len());
    for runs in series {
        let mut samples: Vec<(i64, f64)> = Vec::new();
        let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();
        let mut cursors = vec![0usize; runs.len()];
        for (idx, run) in runs.iter().enumerate() {
            if let Some(&ts) = run.timestamps.first() {
                heap.push(Reverse((ts, idx)));
            }
        }
        while let Some(&Reverse((ts, _))) = heap.peek() {
            let mut best: Option<Candidate> = None;
            while let Some(Reverse((head_ts, idx))) = heap.pop() {
                if head_ts != ts {
                    heap.push(Reverse((head_ts, idx)));
                    break;
                }
                let run = &runs[idx];
                let mut pos = cursors[idx];
                while pos < run.timestamps.len() && run.timestamps[pos] == ts {
                    let candidate = Candidate {
                        value: run.values[pos],
                        priority: (run.prefix.0, run.prefix.1, run.prefix.2, pos as u32),
                    };
                    best = match best {
                        Some(current) if is_greater(&current, &candidate) => Some(current),
                        _ => Some(candidate),
                    };
                    pos += 1;
                }
                cursors[idx] = pos;
                if let Some(&next_ts) = run.timestamps.get(pos) {
                    heap.push(Reverse((next_ts, idx)));
                }
            }
            if let Some(candidate) = best {
                samples.push((ts, candidate.value));
            }
        }
        out.push(samples);
    }
    out
}

const SERIES: usize = 500;
const SEGMENTS: usize = 4;
const SAMPLES_PER_RUN: usize = 60;

/// `SEGMENTS` runs per series over the *same* `SAMPLES_PER_RUN` timestamps,
/// so every timestamp has `SEGMENTS` cross-segment duplicates to resolve:
/// the dedup path (the part A1b rewrote) does real work.
fn build() -> Vec<Vec<Run>> {
    let mut series = Vec::with_capacity(SERIES);
    for s in 0..SERIES {
        let mut runs = Vec::with_capacity(SEGMENTS);
        for seg in 0..SEGMENTS {
            let timestamps: Vec<i64> = (0..SAMPLES_PER_RUN)
                .map(|j| 1_000_000_000 * j as i64)
                .collect();
            let values: Vec<f64> = (0..SAMPLES_PER_RUN)
                .map(|j| (s as f64) + (seg as f64) * 0.25 + (j as f64) * 0.5)
                .collect();
            runs.push(Run {
                timestamps,
                values,
                // Distinct created_unix_ns per segment: a real winner each ts.
                prefix: (seg as i64, 1, 1),
            });
        }
        series.push(runs);
    }
    series
}

fn bench_merge(c: &mut Criterion) {
    let series = build();
    let deduped: usize = SERIES * SAMPLES_PER_RUN;

    // Correctness guard: both algorithms must agree exactly before timing.
    let a = merge_materialized(&series);
    let b = merge_kway(&series);
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(&b) {
        assert_eq!(x.len(), y.len());
        for ((tx, vx), (ty, vy)) in x.iter().zip(y) {
            assert_eq!(tx, ty);
            assert_eq!(vx.to_bits(), vy.to_bits());
        }
    }

    let mut group = c.benchmark_group("merge_kway_vs_materialized");
    group.throughput(Throughput::Elements(deduped as u64));
    group.sample_size(50);

    group.bench_function("materialized_window", |bencher| {
        bencher.iter(|| {
            let out = merge_materialized(&series);
            let n: usize = out.iter().map(Vec::len).sum();
            assert_eq!(n, deduped);
        });
    });

    group.bench_function("kway_merge", |bencher| {
        bencher.iter(|| {
            let out = merge_kway(&series);
            let n: usize = out.iter().map(Vec::len).sum();
            assert_eq!(n, deduped);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
