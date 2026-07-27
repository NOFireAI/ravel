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

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{bench_bounds, bench_identity};
use ravel_segment::{SegmentWriter, SeriesInput};

const TOTAL_SAMPLES: usize = 200_000;

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
        group.bench_function(format!("{series_count}_series_v1"), |b| {
            b.iter(|| {
                let series = clone_inputs(&inputs);
                SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode")
            });
        });
        group.bench_function(format!("{series_count}_series_v2"), |b| {
            b.iter(|| {
                let series = clone_inputs(&inputs);
                SegmentWriter::write_v2(series, bench_identity(), bench_bounds())
                    .expect("encode v2")
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_segment_encode);
criterion_main!(benches);
