//! OTAP decode CPU and allocation bench (issue #18, A2). No otap bench
//! workload existed in this repo before this ticket (see the final report
//! for that gap against the task's "existing bench workloads" premise); this
//! is the first one, so before/after is reported here rather than as a diff
//! against a prior run.
//!
//! Reports two things per batch:
//! - wall-clock decode throughput, via the criterion group below.
//! - allocation count and bytes for one untimed decode, via `stats_alloc`'s
//!   instrumented global allocator (the workspace denies `unsafe_code`
//!   everywhere including benches, so allocation counting must go through a
//!   dependency that owns the `unsafe impl GlobalAlloc` itself), plus the
//!   observed zero-copy vs copy-fallback frame split from
//!   `StreamState::decode_stats` (docs/arrow-datafusion-plan.md ticket A2:
//!   the win is producer-alignment-dependent and must be measured, never
//!   assumed).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords;
use ravel_otap::stream::{StreamConfig, StreamState};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// The root table's `name` column is `Dictionary<UInt8, Utf8>` (encode.rs),
// so SERIES_COUNT must stay under 256 distinct metric names or the encoder
// raises `DictionaryKeyOverflowError`.
const SERIES_COUNT: usize = 200;
const POINTS_PER_SERIES: usize = 20;
const ATTRS_PER_POINT: usize = 4;

/// One `BatchArrowRecords` covering `SERIES_COUNT` metrics, each with
/// `POINTS_PER_SERIES` data points carrying `ATTRS_PER_POINT` string
/// attributes: a mixed numeric/dictionary/string workload representative of
/// the root/data-points/attrs payload triple this crate decodes.
fn build_batch() -> BatchArrowRecords {
    let mut encoder = MetricsStreamEncoder::new("bench-v1").expect("encoder construction");
    let metrics: Vec<MetricRow> = (0..SERIES_COUNT)
        .map(|i| MetricRow {
            name: format!("metric.{i}"),
            kind: MetricKind::Gauge,
            data_points: (0..POINTS_PER_SERIES)
                .map(|p| DataPointRow {
                    exemplars: vec![],
                    time_unix_nano: p as i64 * 1_000_000,
                    value: p as f64 * 0.5,
                    flags: 0,
                    attrs: (0..ATTRS_PER_POINT)
                        .map(|a| AttrRow {
                            key: format!("k{a}"),
                            value: AttrValue::Str(format!("v{a}-{i}")),
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect();
    encoder.encode_batch(1, &metrics).expect("encode batch")
}

fn bench_decode(c: &mut Criterion) {
    let batch = build_batch();
    let total_points = SERIES_COUNT * POINTS_PER_SERIES;

    // One untimed decode outside criterion's measured loop, to report
    // allocations/batch and the zero-copy fallback rate without criterion's
    // own bookkeeping polluting the counts.
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state
        .decode(batch.clone())
        .expect("decode for allocation count");
    let stats_alloc = region.change();
    let stats = state.decode_stats();
    eprintln!(
        "otap_decode: {total_points} points, {} payload frames, \
         {} allocations ({} bytes) per batch, \
         zero_copy_frames={} copy_fallback_frames={}",
        decoded.payloads.len(),
        stats_alloc.allocations,
        stats_alloc.bytes_allocated,
        stats.zero_copy_frames,
        stats.copy_fallback_frames,
    );
    drop(decoded);

    let mut group = c.benchmark_group("otap_decode");
    group.throughput(Throughput::Elements(total_points as u64));
    group.bench_function("decode_batch", |b| {
        b.iter(|| {
            let mut state = StreamState::new(StreamConfig::default());
            let decoded = state.decode(batch.clone()).expect("decode");
            std::hint::black_box(&decoded);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
