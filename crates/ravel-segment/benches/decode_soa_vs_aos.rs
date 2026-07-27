//! Compares the AoS decode path (`decode_pages`, allocates a fresh
//! `Vec<Sample>` per series per call) against the SoA path
//! (`decode_pages_soa`) under two allocation regimes: fresh scratch/output
//! buffers every series (a fair baseline against AoS) and buffers reused
//! across every series in the segment (the intended fetch-loop usage,
//! docs/arrow-datafusion-plan.md ticket A1a). Self-contained: builds its own
//! segment with `SegmentWriter` rather than depending on `ravel-bench`
//! (out of scope for this ticket).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput,
    decode_catalog, decode_pages, decode_pages_soa, open_from_full, plan_ranges,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

const SERIES_COUNT: usize = 2_000;
const SAMPLES_PER_SERIES: usize = 60;

fn build_test_segment() -> bytes::Bytes {
    let tenant_id = TenantId::new("bench-tenant".to_string());
    let mut series = Vec::with_capacity(SERIES_COUNT);
    for i in 0..SERIES_COUNT {
        let metric = format!("bench_metric_{i}");
        let labels = LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: metric.clone(),
            },
            Label {
                name: "shard".to_string(),
                value: (i % 16).to_string(),
            },
        ])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, &metric, &labels).expect("series id");
        let samples = (0..SAMPLES_PER_SERIES)
            .map(|j| Sample {
                ts_ns: 1_000_000_000 * (j as i64),
                // Gentle drift so Gorilla has real work to do, matching a
                // realistic metric stream rather than an all-identical one.
                value: 100.0 + (j as f64) * 0.5 + (i as f64) * 0.001,
            })
            .collect();
        series.push(SeriesInput {
            series_id,
            labels,
            samples,
        });
    }
    let identity = SegmentIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: "bench-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    SegmentWriter::write(series, identity, bounds)
        .expect("write segment")
        .bytes
}

fn decode_entries(bytes: &bytes::Bytes) -> (ravel_segment::Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let footer = loc.footer;
    let label_dict = &bytes[section_bytes(&footer, ravel_segment_section::LABEL_DICT)];
    let series_table = &bytes[section_bytes(&footer, ravel_segment_section::SERIES_TABLE)];
    let entries = decode_catalog(&footer, label_dict, series_table, limits).expect("catalog");
    (footer, entries)
}

/// Section kinds from docs/segment-format.md (not exported by
/// `ravel-segment`; see `ravel-query/src/fetcher.rs` for the same local
/// mirror).
mod ravel_segment_section {
    pub const LABEL_DICT: u32 = 1;
    pub const SERIES_TABLE: u32 = 2;
}

fn section_bytes(footer: &ravel_segment::Footer, kind: u32) -> std::ops::Range<usize> {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    s.offset as usize..(s.offset + s.len) as usize
}

fn bench_decode(c: &mut Criterion) {
    let bytes = build_test_segment();
    let (footer, entries) = decode_entries(&bytes);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let planned = plan_ranges(&footer, &selected).expect("plan ranges");
    let total_samples = (SERIES_COUNT * SAMPLES_PER_SERIES) as u64;
    let limits = ReaderLimits::default();

    let mut group = c.benchmark_group("decode_soa_vs_aos");
    group.throughput(Throughput::Elements(total_samples));

    group.bench_function("aos_decode_pages", |b| {
        b.iter(|| {
            let mut decoded = 0usize;
            for (entry, plan) in entries.iter().zip(planned.iter()) {
                let ts_bytes =
                    &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
                let val_bytes = &bytes
                    [plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
                let samples = decode_pages(entry, ts_bytes, val_bytes, limits).expect("decode");
                decoded += samples.len();
            }
            assert_eq!(decoded, total_samples as usize);
        });
    });

    group.bench_function("soa_decode_pages_fresh_buffers", |b| {
        b.iter(|| {
            let mut decoded = 0usize;
            for (entry, plan) in entries.iter().zip(planned.iter()) {
                let ts_bytes =
                    &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
                let val_bytes = &bytes
                    [plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                decode_pages_soa(
                    entry,
                    ts_bytes,
                    val_bytes,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decode");
                decoded += timestamps.len();
            }
            assert_eq!(decoded, total_samples as usize);
        });
    });

    group.bench_function("soa_decode_pages_reused_buffers", |b| {
        let mut scratch = Vec::new();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        b.iter(|| {
            let mut decoded = 0usize;
            for (entry, plan) in entries.iter().zip(planned.iter()) {
                let ts_bytes =
                    &bytes[plan.ts_range.0 as usize..(plan.ts_range.0 + plan.ts_range.1) as usize];
                let val_bytes = &bytes
                    [plan.val_range.0 as usize..(plan.val_range.0 + plan.val_range.1) as usize];
                decode_pages_soa(
                    entry,
                    ts_bytes,
                    val_bytes,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decode");
                decoded += timestamps.len();
            }
            assert_eq!(decoded, total_samples as usize);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
