//! RSEG selective decode: a matcher selecting 1% of series, measuring the
//! footer+dict+selected-pages-only path (docs/benchmarking.md Phase 1: "RSEG
//! decode + selector scan: samples/s, per matched fraction"). Catalog decode
//! (footer, LABEL_DICT, SERIES_TABLE) is unavoidable for any selector query;
//! only the selected series' TS/VAL pages are then decoded.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{build_segment, decode_entries, slice_range};
use ravel_segment::{ReaderLimits, decode_pages, plan_ranges, select};

const SERIES_COUNT: usize = 10_000;
const SAMPLES_PER_SERIES: usize = 60;
/// 100 distinct label-value groups over 10k series: matching one group
/// selects exactly 1% of series.
const DISTINCT_GROUPS: usize = 100;

fn bench_segment_decode_selective(c: &mut Criterion) {
    let config = WorkloadConfig {
        series_count: SERIES_COUNT,
        samples_per_series: SAMPLES_PER_SERIES,
        cardinality: CardinalityProfile {
            distinct_sets: DISTINCT_GROUPS,
            labels_per_set: 6,
        },
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate workload");
    let written = build_segment(raw);
    let bytes = written.bytes;

    let expected_series = SERIES_COUNT / DISTINCT_GROUPS;
    let mut group = c.benchmark_group("segment_decode_selective");
    group.throughput(Throughput::Elements(
        (expected_series * SAMPLES_PER_SERIES) as u64,
    ));
    group.bench_function("select_1pct", |b| {
        b.iter(|| {
            let limits = ReaderLimits::default();
            let (footer, entries) = decode_entries(&bytes);
            let selected = select(&entries, &[("label_000", "v0_0")], None);
            let ranges = plan_ranges(&footer, &selected).expect("plan ranges");
            let mut decoded_samples = 0usize;
            for (entry, range) in selected.iter().zip(ranges.iter()) {
                let ts_bytes = slice_range(&bytes, range.ts_range);
                let val_bytes = slice_range(&bytes, range.val_range);
                let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
                decoded_samples += samples.len();
            }
            assert_eq!(selected.len(), SERIES_COUNT / DISTINCT_GROUPS);
            decoded_samples
        });
    });
    group.finish();
}

criterion_group!(benches, bench_segment_decode_selective);
criterion_main!(benches);
