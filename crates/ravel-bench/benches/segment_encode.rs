//! RSEG encode throughput by series cardinality (docs/benchmarking.md Phase
//! 1: "RSEG metric segment encode: samples/s, bytes/sample, by cardinality
//! (100 / 10k / 1M series)"). 1M series is scaled down to 200k for `--quick`
//! CI runtime; pass a higher `RAVEL_BENCH_MAX_SERIES` to widen the sweep.
//!
//! Covers both RSEG v1 (`SegmentWriter::write`) and v2
//! (`SegmentWriter::write_v2`, ADR-0014) as separate bench ids within the
//! same group (`{series_count}_series_v1` / `_v2`) so a later comparison
//! run has matched, directly comparable numbers for both. This crate makes
//! v2 runnable; it does not measure or claim any v1/v2 delta itself.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{bench_bounds, bench_identity};
use ravel_segment::{SegmentWriter, SeriesInput};

const TOTAL_SAMPLES: usize = 200_000;

/// Axis-sweep cells (issue #98): samples-per-series x labels-per-series at a
/// fixed ~200k total sample budget. Existing `segment_encode` cases above are
/// frozen (BENCHMARKS.md); these are new ids in their own group.
const AXIS_SAMPLES_PER_SERIES: [usize; 3] = [2, 60, 500];
const AXIS_LABELS_PER_SERIES: [usize; 2] = [5, 15];

fn series_inputs(series_count: usize) -> Vec<SeriesInput> {
    let samples_per_series = (TOTAL_SAMPLES / series_count).max(1);
    let config = WorkloadConfig {
        series_count,
        samples_per_series,
        cardinality: CardinalityProfile::many_small(series_count),
        ..Default::default()
    };
    generate_raw(&config)
        .expect("generate workload")
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect()
}

fn clone_inputs(inputs: &[SeriesInput]) -> Vec<SeriesInput> {
    inputs
        .iter()
        .map(|s| SeriesInput {
            series_id: s.series_id,
            labels: s.labels.clone(),
            samples: s.samples.clone(),
        })
        .collect()
}

fn bench_segment_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_encode");
    let max_series: usize = std::env::var("RAVEL_BENCH_MAX_SERIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    for &series_count in &[100usize, 10_000, max_series] {
        let inputs = series_inputs(series_count);
        let actual_samples: usize = inputs.iter().map(|s| s.samples.len()).sum();
        group.throughput(Throughput::Elements(actual_samples as u64));
        // The fixture clone is setup, not part of encode: `SegmentWriter::write`
        // consumes its input, so each iteration needs a fresh copy, but the deep
        // clone of every LabelSet and Vec<Sample> must not be charged to encode
        // time (a12-F01). iter_batched runs the clone in the untimed setup
        // closure and times only the write, matching the pattern in
        // src/bin/segment_alloc_profile.rs.
        group.bench_function(format!("{series_count}_series_v1"), |b| {
            b.iter_batched(
                || clone_inputs(&inputs),
                |series| {
                    SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode")
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function(format!("{series_count}_series_v2"), |b| {
            b.iter_batched(
                || clone_inputs(&inputs),
                |series| {
                    SegmentWriter::write_v2(series, bench_identity(), bench_bounds())
                        .expect("encode v2")
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn axis_inputs(samples_per_series: usize, labels_per_series: usize) -> Vec<SeriesInput> {
    let config = WorkloadConfig::axis_sweep(samples_per_series, labels_per_series);
    generate_raw(&config)
        .expect("generate axis workload")
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect()
}

/// samples-per-series x labels-per-series sweep for encode, v1 and v2 each
/// (issue #98). Case ids are `s{samples}_l{labels}_v1` / `_v2`. Same untimed
/// clone-in-setup pattern as `bench_segment_encode` (a12-F01): the deep
/// fixture clone is charged to setup, only the write is timed.
fn bench_segment_encode_axis_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_encode_axis_sweep");
    for &samples_per_series in &AXIS_SAMPLES_PER_SERIES {
        for &labels_per_series in &AXIS_LABELS_PER_SERIES {
            let inputs = axis_inputs(samples_per_series, labels_per_series);
            let actual_samples: usize = inputs.iter().map(|s| s.samples.len()).sum();
            group.throughput(Throughput::Elements(actual_samples as u64));
            let stem = format!("s{samples_per_series}_l{labels_per_series}");
            group.bench_function(format!("{stem}_v1"), |b| {
                b.iter_batched(
                    || clone_inputs(&inputs),
                    |series| {
                        SegmentWriter::write(series, bench_identity(), bench_bounds())
                            .expect("encode")
                    },
                    BatchSize::SmallInput,
                );
            });
            group.bench_function(format!("{stem}_v2"), |b| {
                b.iter_batched(
                    || clone_inputs(&inputs),
                    |series| {
                        SegmentWriter::write_v2(series, bench_identity(), bench_bounds())
                            .expect("encode v2")
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_segment_encode,
    bench_segment_encode_axis_sweep
);
criterion_main!(benches);
