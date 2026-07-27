//! RSEG selective decode: a matcher selecting 1% of series, measuring the
//! footer+dict+selected-pages-only path (docs/benchmarking.md Phase 1: "RSEG
//! decode + selector scan: samples/s, per matched fraction"). Catalog decode
//! (footer, LABEL_DICT, SERIES_TABLE) is unavoidable for any selector query;
//! only the selected series' TS/VAL pages are then decoded.
//!
//! Covers both RSEG v1 (`decode_catalog`/`decode_catalog_matching`) and v2
//! (`decode_catalog_v2`/`decode_catalog_matching_v2`, ADR-0014) as separate
//! bench ids (`select_1pct_v1` / `_v2`, `select_1pct_lazy_v1` / `_v2`)
//! within the same group.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    build_segment, build_segment_v2, decode_entries, decode_entries_v2, decode_matching_entries,
    decode_matching_entries_v2, slice_range,
};
use ravel_segment::{Footer, ReaderLimits, SeriesEntry, decode_pages, plan_ranges, select};

const SERIES_COUNT: usize = 10_000;
const SAMPLES_PER_SERIES: usize = 60;
/// 100 distinct label-value groups over 10k series: matching one group
/// selects exactly 1% of series.
const DISTINCT_GROUPS: usize = 100;

fn decode_selected(bytes: &[u8], footer: &Footer, selected: &[&SeriesEntry]) -> usize {
    let limits = ReaderLimits::default();
    let ranges = plan_ranges(footer, selected).expect("plan ranges");
    let mut decoded_samples = 0usize;
    for (entry, range) in selected.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
        decoded_samples += samples.len();
    }
    assert_eq!(selected.len(), SERIES_COUNT / DISTINCT_GROUPS);
    decoded_samples
}

/// Axis-sweep cells (issue #98): samples-per-series x labels-per-series at a
/// fixed ~200k total sample budget, with the same 100-group 1%-selectivity
/// shape as the frozen cases above (so matched samples stay near
/// 200k/100 = ~2k per cell). Existing cases are untouched; these are new ids
/// in their own group.
const AXIS_SAMPLES_PER_SERIES: [usize; 3] = [2, 60, 500];
const AXIS_LABELS_PER_SERIES: [usize; 2] = [5, 15];

/// Decodes the selected series' pages and returns the decoded sample count,
/// asserting the matcher hit `expected` series. Group-agnostic twin of
/// [`decode_selected`] for the axis sweep, where the matched count varies
/// per cell instead of being the fixed `SERIES_COUNT / DISTINCT_GROUPS`.
fn decode_selected_axis(
    bytes: &[u8],
    footer: &Footer,
    selected: &[&SeriesEntry],
    expected: usize,
) -> usize {
    let limits = ReaderLimits::default();
    let ranges = plan_ranges(footer, selected).expect("plan ranges");
    let mut decoded_samples = 0usize;
    for (entry, range) in selected.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
        decoded_samples += samples.len();
    }
    assert_eq!(selected.len(), expected);
    decoded_samples
}

fn bench_segment_decode_selective_axis_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_decode_selective_axis_sweep");
    for &samples_per_series in &AXIS_SAMPLES_PER_SERIES {
        for &labels_per_series in &AXIS_LABELS_PER_SERIES {
            // Start from the axis cell (fixed total, derived series count,
            // labels_per_series labels each), then regroup into DISTINCT_GROUPS
            // shared label-value groups so a single equality matcher selects
            // ~1% of series. The regroup changes label *values*, not the label
            // count, so the labels axis is preserved.
            let mut config = WorkloadConfig::axis_sweep(samples_per_series, labels_per_series);
            config.cardinality.distinct_sets = DISTINCT_GROUPS;
            let series_count = config.series_count;
            // Matcher hits base group 0: series indices i with i % 100 == 0.
            let expected = series_count.div_ceil(DISTINCT_GROUPS);

            let raw = generate_raw(&config).expect("generate workload");
            let written_v1 = build_segment(raw.clone());
            let bytes_v1 = written_v1.bytes;
            let written_v2 = build_segment_v2(raw);
            let bytes_v2 = written_v2.bytes;

            group.throughput(Throughput::Elements((expected * samples_per_series) as u64));
            let stem = format!("s{samples_per_series}_l{labels_per_series}");
            group.bench_function(format!("{stem}_eager_v1"), |b| {
                b.iter(|| {
                    let (footer, entries) = decode_entries(&bytes_v1);
                    let selected = select(&entries, &[("label_000", "v0_0")], None);
                    decode_selected_axis(&bytes_v1, &footer, &selected, expected)
                });
            });
            group.bench_function(format!("{stem}_eager_v2"), |b| {
                b.iter(|| {
                    let (footer, entries) = decode_entries_v2(&bytes_v2);
                    let selected = select(&entries, &[("label_000", "v0_0")], None);
                    decode_selected_axis(&bytes_v2, &footer, &selected, expected)
                });
            });
            group.bench_function(format!("{stem}_lazy_v1"), |b| {
                b.iter(|| {
                    let (footer, entries) =
                        decode_matching_entries(&bytes_v1, &[("label_000", "v0_0")]);
                    let selected: Vec<_> = entries.iter().collect();
                    decode_selected_axis(&bytes_v1, &footer, &selected, expected)
                });
            });
            group.bench_function(format!("{stem}_lazy_v2"), |b| {
                b.iter(|| {
                    let (footer, entries) =
                        decode_matching_entries_v2(&bytes_v2, &[("label_000", "v0_0")]);
                    let selected: Vec<_> = entries.iter().collect();
                    decode_selected_axis(&bytes_v2, &footer, &selected, expected)
                });
            });
        }
    }
    group.finish();
}

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
    let written_v1 = build_segment(raw.clone());
    let bytes_v1 = written_v1.bytes;
    let written_v2 = build_segment_v2(raw);
    let bytes_v2 = written_v2.bytes;

    let expected_series = SERIES_COUNT / DISTINCT_GROUPS;
    let mut group = c.benchmark_group("segment_decode_selective");
    group.throughput(Throughput::Elements(
        (expected_series * SAMPLES_PER_SERIES) as u64,
    ));
    group.bench_function("select_1pct_v1", |b| {
        b.iter(|| {
            let (footer, entries) = decode_entries(&bytes_v1);
            let selected = select(&entries, &[("label_000", "v0_0")], None);
            decode_selected(&bytes_v1, &footer, &selected)
        });
    });
    group.bench_function("select_1pct_v2", |b| {
        b.iter(|| {
            let (footer, entries) = decode_entries_v2(&bytes_v2);
            let selected = select(&entries, &[("label_000", "v0_0")], None);
            decode_selected(&bytes_v2, &footer, &selected)
        });
    });
    // Same segment and matcher through the lazy ordinal-matching path
    // (decode_catalog_matching[_v2]): only matched series materialize labels.
    group.bench_function("select_1pct_lazy_v1", |b| {
        b.iter(|| {
            let (footer, entries) = decode_matching_entries(&bytes_v1, &[("label_000", "v0_0")]);
            let selected: Vec<_> = entries.iter().collect();
            decode_selected(&bytes_v1, &footer, &selected)
        });
    });
    group.bench_function("select_1pct_lazy_v2", |b| {
        b.iter(|| {
            let (footer, entries) = decode_matching_entries_v2(&bytes_v2, &[("label_000", "v0_0")]);
            let selected: Vec<_> = entries.iter().collect();
            decode_selected(&bytes_v2, &footer, &selected)
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_segment_decode_selective,
    bench_segment_decode_selective_axis_sweep
);
criterion_main!(benches);
