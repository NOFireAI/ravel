//! RSEG full decode throughput (docs/benchmarking.md Phase 1: "RSEG decode +
//! selector scan: samples/s, per matched fraction"). This bench covers the
//! 100% matched fraction: open, decode the catalog, then decode every
//! series' TS+VAL pages.
//!
//! Covers both RSEG v1 (`decode_catalog`) and v2 (`decode_catalog_v2`,
//! ADR-0014) as separate bench ids (`all_series_v1` / `all_series_v2`)
//! within the same group. TS/VAL page decode (`decode_pages`) is identical
//! bytes-on-the-wire for both versions (only the catalog section set
//! differs), so only the catalog decode call differs between the two.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_bench::segment_support::{
    build_segment, build_segment_v2, decode_entries, decode_entries_v2,
};
use ravel_segment::{Footer, ReaderLimits, SeriesEntry, decode_pages, plan_ranges, select};

const SERIES_COUNT: usize = 5_000;
const SAMPLES_PER_SERIES: usize = 60;

fn decode_all(bytes: &[u8], footer: &Footer, entries: &[SeriesEntry], total_samples: usize) {
    let limits = ReaderLimits::default();
    let selected: Vec<_> = select(entries, &[], None);
    let ranges = plan_ranges(footer, &selected).expect("plan ranges");
    let mut decoded_samples = 0usize;
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = ravel_bench::segment_support::slice_range(bytes, range.ts_range);
        let val_bytes = ravel_bench::segment_support::slice_range(bytes, range.val_range);
        let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
        decoded_samples += samples.len();
    }
    assert_eq!(decoded_samples, total_samples);
}

fn bench_segment_decode_full(c: &mut Criterion) {
    let config = WorkloadConfig {
        series_count: SERIES_COUNT,
        samples_per_series: SAMPLES_PER_SERIES,
        cardinality: CardinalityProfile::many_small(SERIES_COUNT),
        ..Default::default()
    };
    let raw = generate_raw(&config).expect("generate workload");
    let total_samples: usize = raw.iter().map(|(_, _, samples)| samples.len()).sum();
    let written_v1 = build_segment(raw.clone());
    let bytes_v1 = written_v1.bytes;
    let written_v2 = build_segment_v2(raw);
    let bytes_v2 = written_v2.bytes;

    let mut group = c.benchmark_group("segment_decode_full");
    group.throughput(Throughput::Elements(total_samples as u64));
    group.bench_function("all_series_v1", |b| {
        b.iter(|| {
            let (footer, entries) = decode_entries(&bytes_v1);
            decode_all(&bytes_v1, &footer, &entries, total_samples);
        });
    });
    group.bench_function("all_series_v2", |b| {
        b.iter(|| {
            let (footer, entries) = decode_entries_v2(&bytes_v2);
            decode_all(&bytes_v2, &footer, &entries, total_samples);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_segment_decode_full);
criterion_main!(benches);
