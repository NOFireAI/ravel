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

criterion_group!(benches, bench_segment_decode_selective);
criterion_main!(benches);
