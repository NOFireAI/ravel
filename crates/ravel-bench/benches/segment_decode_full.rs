//! RSEG full decode throughput (docs/benchmarking.md Phase 1: "RSEG decode +
//! selector scan: samples/s, per matched fraction"). This bench covers the
//! 100% matched fraction: open, decode the catalog, then decode every
//! series' TS+VAL pages.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, decode_entries};
use ravel_segment::{ReaderLimits, decode_pages, plan_ranges, select};

const SERIES_COUNT: usize = 5_000;
const SAMPLES_PER_SERIES: usize = 60;

fn bench_segment_decode_full(c: &mut Criterion) {
    let config = WorkloadConfig {
        series_count: SERIES_COUNT,
        samples_per_series: SAMPLES_PER_SERIES,
        cardinality: CardinalityProfile::many_small(SERIES_COUNT),
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate workload");
    let total_samples: usize = raw.iter().map(|(_, _, samples)| samples.len()).sum();
    let written = build_segment(raw);
    let bytes = written.bytes;

    let mut group = c.benchmark_group("segment_decode_full");
    group.throughput(Throughput::Elements(total_samples as u64));
    group.bench_function("all_series", |b| {
        b.iter(|| {
            let limits = ReaderLimits::default();
            let (footer, entries) = decode_entries(&bytes);
            let selected: Vec<_> = select(&entries, &[], None);
            let ranges = plan_ranges(&footer, &selected).expect("plan ranges");
            let mut decoded_samples = 0usize;
            for (entry, range) in entries.iter().zip(ranges.iter()) {
                let ts_bytes = ravel_bench::segment_support::slice_range(&bytes, range.ts_range);
                let val_bytes = ravel_bench::segment_support::slice_range(&bytes, range.val_range);
                let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
                decoded_samples += samples.len();
            }
            assert_eq!(decoded_samples, total_samples);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_segment_decode_full);
criterion_main!(benches);
