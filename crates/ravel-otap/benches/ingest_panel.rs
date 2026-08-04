//! OTLP-vs-OTAP ingest panel (issue #12, docs/otap-ingest.md phases 1 and 4).
//!
//! Decodes identical logical content two ways and carries both all the way
//! to normalized samples, so the comparison is wire-decode + normalize on
//! each side, not decode-only vs decode+normalize:
//!
//! - OTLP: protobuf `ExportMetricsServiceRequest` bytes -> `prost` decode ->
//!   `ravel_otlp::normalize_metrics`.
//! - OTAP: protobuf `BatchArrowRecords` envelope bytes -> `prost` decode ->
//!   `StreamState::decode` (Arrow IPC, zstd, columnar) ->
//!   `ravel_otap::normalize::normalize_decoded`.
//!
//! Both start from serialized wire bytes and end at `Vec<NormalizedPoint>`,
//! and both feed `SeriesId::compute` the same label sets (the differential
//! test proves the outputs are identical), so the only thing that differs is
//! the per-format decode/normalize cost this bench measures.
//!
//! Two attribute-cardinality settings, since docs/otap-ingest.md names high
//! attribute cardinality as OTAP's claimed sweet spot ("Why it fits Ravel
//! unusually well"):
//!
//! - `low`: every point of a metric carries an identical attribute set, so
//!   the batch has one distinct series per metric (heavy value repetition).
//! - `high`: one attribute per point carries a value unique to that point,
//!   so every point is its own distinct series (no value repetition).
//!
//! Reports, per (format, cardinality):
//! - wall-clock throughput (points/s) via the criterion group, from which
//!   CPU/point (ns/point) follows directly, and
//! - allocations/point and bytes/point for one untimed wire->normalized run,
//!   via `stats_alloc`'s instrumented global allocator (the workspace denies
//!   `unsafe_code` everywhere including benches, so allocation counting must
//!   go through a dependency that owns the `unsafe impl GlobalAlloc`), the
//!   same measurement approach BENCHMARKS.md's `otap_decode` row uses.
//!
//! Ack latency (the third phase-4 metric) is deliberately not here: it needs
//! the strict-commit ingest path (IngestRouter + object store + BatchStatus),
//! which for OTAP is the unbuilt phase-3 gateway wiring
//! (docs/otap-ingest.md: "services/ has no ravel-otap dependency"). It cannot
//! be measured from this decode/normalize crate and is reported as pending in
//! BENCHMARKS.md, not fabricated.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data as MetricData,
};
use prost::Message;
use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::normalize::normalize_decoded;
use ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords;
use ravel_otap::stream::{StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_types::TenantId;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// Root `id` and data-point `parent_id` are UInt16 in the encoder, so METRICS
// must stay under 65_536; `name` is Dictionary<UInt8, Utf8>, so it must also
// stay under 256 distinct names. 100 satisfies both.
const METRICS: usize = 100;
const POINTS_PER_METRIC: usize = 50;
const ATTRS_PER_POINT: usize = 6;

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;

#[derive(Clone, Copy)]
enum Cardinality {
    /// Every point of a metric shares one attribute set: one series per metric.
    Low,
    /// One attribute per point is unique: one series per point.
    High,
}

/// The attribute set for point `global_idx` of metric `metric`. All six
/// attributes depend on the metric; under `High`, attribute `k0` additionally
/// carries a per-point-unique value, so the point's series is unique.
fn attrs_for(metric: usize, global_idx: usize, card: Cardinality) -> Vec<(String, String)> {
    (0..ATTRS_PER_POINT)
        .map(|a| {
            let key = format!("k{a}");
            let value = match (a, card) {
                (0, Cardinality::High) => format!("u{global_idx}"),
                _ => format!("v{a}_m{metric}"),
            };
            (key, value)
        })
        .collect()
}

fn build_otlp_request(card: Cardinality) -> ExportMetricsServiceRequest {
    let mut global_idx = 0usize;
    let metrics: Vec<Metric> = (0..METRICS)
        .map(|i| {
            let data_points: Vec<NumberDataPoint> = (0..POINTS_PER_METRIC)
                .map(|p| {
                    let attributes = attrs_for(i, global_idx, card)
                        .into_iter()
                        .map(|(key, value)| KeyValue {
                            key,
                            value: Some(AnyValue {
                                value: Some(AnyValueVariant::StringValue(value)),
                            }),
                            ..Default::default()
                        })
                        .collect();
                    global_idx += 1;
                    NumberDataPoint {
                        attributes,
                        time_unix_nano: (INGEST_TS_NS + p as i64 * 1_000_000) as u64,
                        value: Some(NumberValue::AsDouble(p as f64 * 0.5)),
                        ..Default::default()
                    }
                })
                .collect();
            Metric {
                name: format!("metric_{i}"),
                data: Some(MetricData::Gauge(Gauge { data_points })),
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn build_otap_batch(card: Cardinality) -> BatchArrowRecords {
    let mut global_idx = 0usize;
    let metrics: Vec<MetricRow> = (0..METRICS)
        .map(|i| {
            let data_points: Vec<DataPointRow> = (0..POINTS_PER_METRIC)
                .map(|p| {
                    let attrs = attrs_for(i, global_idx, card)
                        .into_iter()
                        .map(|(key, value)| AttrRow {
                            key,
                            value: AttrValue::Str(value),
                        })
                        .collect();
                    global_idx += 1;
                    DataPointRow {
                        exemplars: vec![],
                        time_unix_nano: INGEST_TS_NS + p as i64 * 1_000_000,
                        value: p as f64 * 0.5,
                        flags: 0,
                        attrs,
                    }
                })
                .collect();
            MetricRow {
                name: format!("metric_{i}"),
                kind: MetricKind::Gauge,
                data_points,
            }
        })
        .collect();
    let mut encoder = MetricsStreamEncoder::new("ingest-panel").expect("encoder construction");
    encoder.encode_batch(1, &metrics).expect("encode batch")
}

/// OTLP wire bytes -> prost decode -> normalize. Returns the normalized point
/// count (the denominator for per-point figures and a cross-path check).
fn otlp_wire_to_normalized(bytes: &[u8], tenant: &TenantId, limits: &IngestLimits) -> usize {
    let req = ExportMetricsServiceRequest::decode(bytes).expect("decode OTLP request");
    let out = normalize_metrics(tenant, req, limits, INGEST_TS_NS);
    out.points.len()
}

/// OTAP wire bytes -> prost decode of the envelope -> Arrow IPC decode ->
/// normalize. Returns the normalized point count.
fn otap_wire_to_normalized(bytes: &[u8], tenant: &TenantId, limits: &IngestLimits) -> usize {
    let batch = BatchArrowRecords::decode(bytes).expect("decode OTAP envelope");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(batch).expect("Arrow IPC decode");
    let out = normalize_decoded(tenant, &decoded, limits, INGEST_TS_NS);
    out.points.len()
}

fn bench_ingest_panel(c: &mut Criterion) {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let total_points = METRICS * POINTS_PER_METRIC;

    let cases = [(Cardinality::Low, "low"), (Cardinality::High, "high")];

    // Pre-serialize each workload to wire bytes once, outside the timed loop:
    // the panel measures decode+normalize, not encode.
    let wire: Vec<(&str, Vec<u8>, Vec<u8>)> = cases
        .iter()
        .map(|(card, label)| {
            let otlp = build_otlp_request(*card).encode_to_vec();
            let otap = build_otap_batch(*card).encode_to_vec();
            (*label, otlp, otap)
        })
        .collect();

    // One untimed run per (format, cardinality) for allocations/point and
    // bytes/point, plus a cross-path point-count check, printed outside
    // criterion's measured region so its own bookkeeping does not pollute the
    // allocation counts.
    for (label, otlp_bytes, otap_bytes) in &wire {
        let region = Region::new(&INSTRUMENTED_SYSTEM);
        let otlp_pts = otlp_wire_to_normalized(otlp_bytes, &tenant, &limits);
        let otlp_alloc = region.change();

        let region = Region::new(&INSTRUMENTED_SYSTEM);
        let otap_pts = otap_wire_to_normalized(otap_bytes, &tenant, &limits);
        let otap_alloc = region.change();

        assert_eq!(
            otlp_pts, otap_pts,
            "{label}: OTLP and OTAP must normalize to the same point count"
        );
        assert_eq!(otlp_pts, total_points, "{label}: unexpected point count");

        eprintln!(
            "ingest_panel[{label}]: {otlp_pts} points | \
             OTLP wire {} B, {} allocs ({:.2}/pt), {} B alloc ({:.1} B/pt) | \
             OTAP wire {} B, {} allocs ({:.2}/pt), {} B alloc ({:.1} B/pt)",
            otlp_bytes.len(),
            otlp_alloc.allocations,
            otlp_alloc.allocations as f64 / otlp_pts as f64,
            otlp_alloc.bytes_allocated,
            otlp_alloc.bytes_allocated as f64 / otlp_pts as f64,
            otap_bytes.len(),
            otap_alloc.allocations,
            otap_alloc.allocations as f64 / otap_pts as f64,
            otap_alloc.bytes_allocated,
            otap_alloc.bytes_allocated as f64 / otap_pts as f64,
        );
    }

    let mut group = c.benchmark_group("ingest_panel");
    group.throughput(Throughput::Elements(total_points as u64));
    for (label, otlp_bytes, otap_bytes) in &wire {
        group.bench_function(format!("otlp/{label}"), |b| {
            b.iter(|| {
                let n = otlp_wire_to_normalized(otlp_bytes, &tenant, &limits);
                std::hint::black_box(n);
            });
        });
        group.bench_function(format!("otap/{label}"), |b| {
            b.iter(|| {
                let n = otap_wire_to_normalized(otap_bytes, &tenant, &limits);
                std::hint::black_box(n);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ingest_panel);
criterion_main!(benches);
