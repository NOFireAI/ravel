//! Decode+normalize throughput measurement: OTAP columnar path
//! (`StreamState::decode` + `normalize_decoded`) vs the OTLP protobuf path
//! (`prost` decode + `normalize_metrics`) on identical logical data.
//!
//! These are `#[ignore]`d: they are a bench, not a correctness gate, and
//! results depend on the machine they run on. Run with:
//!
//!   cargo test -p ravel-otap --test measurement -- --ignored --nocapture
//!
//! Results are reported in the task's final report only, per instructions
//! (no BENCHMARKS.md edits).

use std::time::Instant;

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
use ravel_otap::stream::{StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_types::TenantId;

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
const ITERATIONS: usize = 20;

/// One generated data point, independent of wire format.
struct GenPoint {
    metric_name: String,
    ts_offset_ns: i64,
    value: f64,
    attrs: Vec<(String, String)>,
}

/// 100 distinct series, 2 low-cardinality attrs, 200 points/series (20,000
/// points total).
fn workload_low_attr_100_series() -> Vec<GenPoint> {
    let series = 100;
    let points_per_series = 200;
    let mut out = Vec::with_capacity(series * points_per_series);
    for s in 0..series {
        let metric_name = format!("metric_{s}");
        let region = format!("region_{}", s % 4);
        let shard = format!("shard_{}", s % 8);
        for p in 0..points_per_series {
            out.push(GenPoint {
                metric_name: metric_name.clone(),
                ts_offset_ns: p as i64 * 1_000_000,
                value: (s * 1000 + p) as f64,
                attrs: vec![
                    ("region".to_string(), region.clone()),
                    ("shard".to_string(), shard.clone()),
                ],
            });
        }
    }
    out
}

/// 10,000 distinct series (one metric, attrs vary), 10 stable attrs drawn
/// from a small pool so distinct attribute-set groups are heavily reused
/// (dictionary-heavy: low distinct values per column, high row reuse), 1
/// point/series (10,000 points, 100,000 attr rows total -- stays under the
/// stream's 16 MiB decompressed-payload cap).
fn workload_dictionary_heavy_10k_series() -> Vec<GenPoint> {
    let series = 10_000;
    let points_per_series = 1;
    let mut out = Vec::with_capacity(series * points_per_series);
    for s in 0..series {
        let attrs: Vec<(String, String)> = (0..10)
            .map(|a| (format!("attr_{a}"), format!("val_{}", (s + a) % 20)))
            .collect();
        for p in 0..points_per_series {
            out.push(GenPoint {
                metric_name: "requests_total".to_string(),
                ts_offset_ns: p as i64 * 1_000_000,
                value: (s * 10 + p) as f64,
                attrs: attrs.clone(),
            });
        }
    }
    out
}

/// 10,000 distinct series, 1 point/series, every point carrying a
/// globally unique attribute value: no attribute-set group is ever
/// reused, so the series-grouping memoization cache has a 0% hit rate
/// (worst case for the grouping step).
fn workload_high_churn_10k_series() -> Vec<GenPoint> {
    let series = 10_000;
    let points_per_series = 1;
    let mut out = Vec::with_capacity(series * points_per_series);
    let mut unique = 0u64;
    for s in 0..series {
        for p in 0..points_per_series {
            out.push(GenPoint {
                metric_name: "requests_total".to_string(),
                ts_offset_ns: p as i64 * 1_000_000,
                value: (s * 10 + p) as f64,
                attrs: vec![
                    ("churn".to_string(), format!("v{unique}")),
                    ("shard".to_string(), format!("shard_{}", s % 32)),
                ],
            });
            unique += 1;
        }
    }
    out
}

fn otlp_request(points: &[GenPoint]) -> ExportMetricsServiceRequest {
    let mut metrics: Vec<Metric> = Vec::new();
    let mut current_name: Option<&str> = None;
    let mut current_dps: Vec<NumberDataPoint> = Vec::new();
    for point in points {
        if current_name != Some(point.metric_name.as_str()) {
            if let Some(name) = current_name.take() {
                metrics.push(Metric {
                    name: name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: std::mem::take(&mut current_dps),
                    })),
                    ..Default::default()
                });
            }
            current_name = Some(point.metric_name.as_str());
        }
        let attributes = point
            .attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(v.clone())),
                }),
                ..Default::default()
            })
            .collect();
        current_dps.push(NumberDataPoint {
            attributes,
            time_unix_nano: (INGEST_TS_NS + point.ts_offset_ns) as u64,
            value: Some(NumberValue::AsDouble(point.value)),
            ..Default::default()
        });
    }
    if let Some(name) = current_name {
        metrics.push(Metric {
            name: name.to_string(),
            data: Some(MetricData::Gauge(Gauge {
                data_points: current_dps,
            })),
            ..Default::default()
        });
    }
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

#[allow(clippy::expect_used)]
fn otap_batch(
    points: &[GenPoint],
) -> ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords {
    let mut metrics: Vec<MetricRow> = Vec::new();
    let mut current_name: Option<&str> = None;
    let mut current_dps: Vec<DataPointRow> = Vec::new();
    for point in points {
        if current_name != Some(point.metric_name.as_str()) {
            if let Some(name) = current_name.take() {
                metrics.push(MetricRow {
                    name: name.to_string(),
                    kind: MetricKind::Gauge,
                    data_points: std::mem::take(&mut current_dps),
                });
            }
            current_name = Some(point.metric_name.as_str());
        }
        let attrs = point
            .attrs
            .iter()
            .map(|(k, v)| AttrRow {
                key: k.clone(),
                value: AttrValue::Str(v.clone()),
            })
            .collect();
        current_dps.push(DataPointRow {
            time_unix_nano: INGEST_TS_NS + point.ts_offset_ns,
            value: point.value,
            attrs,
        });
    }
    if let Some(name) = current_name {
        metrics.push(MetricRow {
            name: name.to_string(),
            kind: MetricKind::Gauge,
            data_points: current_dps,
        });
    }
    let mut encoder = MetricsStreamEncoder::new("measurement").expect("new encoder");
    encoder.encode_batch(0, &metrics).expect("encode workload")
}

#[allow(clippy::expect_used)]
fn measure_otlp(points: &[GenPoint]) -> f64 {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let request = otlp_request(points);
    let bytes = request.encode_to_vec();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let decoded = ExportMetricsServiceRequest::decode(bytes.as_slice()).expect("decode proto");
        let out = normalize_metrics(&tenant, decoded, &limits, INGEST_TS_NS);
        std::hint::black_box(&out);
    }
    let elapsed = start.elapsed();
    (points.len() * ITERATIONS) as f64 / elapsed.as_secs_f64()
}

#[allow(clippy::expect_used)]
fn measure_otap(points: &[GenPoint]) -> f64 {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let raw_batch = otap_batch(points);

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut state = StreamState::new(StreamConfig::default());
        let decoded = state.decode(raw_batch.clone()).expect("decode otap batch");
        let out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);
        std::hint::black_box(&out);
    }
    let elapsed = start.elapsed();
    (points.len() * ITERATIONS) as f64 / elapsed.as_secs_f64()
}

fn report(label: &str, points: &[GenPoint]) {
    let otlp_pps = measure_otlp(points);
    let otap_pps = measure_otap(points);
    println!(
        "{label:<40} points={:>7}  otlp={:>12.0} pts/s  otap={:>12.0} pts/s  otap/otlp={:.2}x",
        points.len(),
        otlp_pps,
        otap_pps,
        otap_pps / otlp_pps
    );
}

#[test]
#[ignore]
fn decode_normalize_throughput() {
    report(
        "100 series, low attr count",
        &workload_low_attr_100_series(),
    );
    report(
        "10k series, 10 stable attrs (dict-heavy)",
        &workload_dictionary_heavy_10k_series(),
    );
    report(
        "10k series, high-churn attrs",
        &workload_high_churn_10k_series(),
    );
}
