//! Differential gate: the OTLP protobuf path and the OTAP columnar path
//! must agree on every gauge/sum workload. `normalize_decoded` mirrors
//! `ravel_otlp::normalize_metrics`'s admission rules exactly (see
//! src/normalize.rs's module docs), so feeding the same logical data
//! through `encode -> StreamState::decode -> normalize_decoded` and through
//! `normalize_metrics` on a hand-built `ExportMetricsServiceRequest` must
//! produce identical SeriesId sets, identical `(series, ts, value)` samples
//! (bit-pattern value comparison, per the repo's float-in-dedup-paths
//! convention), and identical rejection classes.
#![allow(clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, Exemplar, Gauge, Histogram, HistogramDataPoint, Metric,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    metric::Data as MetricData, summary_data_point::ValueAtQuantile,
};
use proptest::prelude::*;

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, HistogramMetricRow, HistogramPointRow, MetricKind, MetricRow,
    MetricsStreamEncoder, SummaryMetricRow, SummaryPointRow,
};
use ravel_otap::normalize::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, AGGREGATION_TEMPORALITY_DELTA,
    AGGREGATION_TEMPORALITY_UNSPECIFIED, normalize_decoded,
};
use ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords;
use ravel_otap::stream::{StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, NormalizeOutput, Rejection, normalize_metrics};
use ravel_types::{SeriesId, TenantId};

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;

#[derive(Debug, Clone)]
enum WorkloadValue {
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

#[derive(Debug, Clone)]
struct WorkloadAttr {
    key: String,
    value: WorkloadValue,
}

#[derive(Debug, Clone)]
struct WorkloadPoint {
    ts_offset_ns: i64,
    value: f64,
    attrs: Vec<WorkloadAttr>,
}

#[derive(Debug, Clone)]
enum WorkloadKind {
    Gauge,
    Sum {
        temporality: i32,
        is_monotonic: bool,
    },
}

#[derive(Debug, Clone)]
struct WorkloadMetric {
    name: String,
    kind: WorkloadKind,
    points: Vec<WorkloadPoint>,
}

fn attr_key_strategy() -> impl Strategy<Value = String> {
    // The over-long key (> max_label_name_len = 256) drives the per-label
    // LabelNameTooLong rejection. Combined with the over-long value below and
    // the arbitrary attribute order proptest already produces, this reaches
    // a9-F01 mechanism b: a point with a name violator and a value violator
    // whose input order differs from key-sorted order. Its sort key ("k"...)
    // orders it against the short keys so input vs sorted order can diverge.
    prop_oneof![
        8 => prop_oneof![
            Just("region".to_string()),
            Just("host".to_string()),
            Just("shard".to_string()),
            Just("az".to_string()),
        ],
        1 => Just("k".repeat(257)),
    ]
}

fn attr_value_strategy() -> impl Strategy<Value = WorkloadValue> {
    // The over-long string value (> max_label_value_len = 4096) drives the
    // per-label LabelValueTooLong rejection (a9-F01 mechanism b).
    prop_oneof![
        8 => "[a-z]{1,8}".prop_map(WorkloadValue::Str),
        1 => any::<bool>().prop_map(WorkloadValue::Bool),
        1 => (-1000i64..1000).prop_map(WorkloadValue::Int),
        1 => (-1000.0f64..1000.0).prop_map(WorkloadValue::Double),
        1 => Just(WorkloadValue::Str("v".repeat(4097))),
    ]
}

fn metric_name_strategy() -> impl Strategy<Value = String> {
    // Beyond valid names, reach the two metric-name rejection classes: the
    // empty name (EmptyMetricName) and the over-long name (> the 512-byte
    // max_metric_name_len, MetricNameTooLong). Paired with the Sum temporality
    // strategy below, an empty or over-long name on a delta Sum is exactly
    // a9-F01 mechanism a: the name must be classed before temporality.
    prop_oneof![
        6 => "[a-z]{3,10}".prop_map(String::from),
        1 => Just(String::new()),
        1 => "[a-z]{513,540}".prop_map(String::from),
    ]
}

fn sum_temporality_strategy() -> impl Strategy<Value = i32> {
    // Only cumulative sums are supported; delta and unspecified must be
    // classed UnsupportedTemporality on both paths.
    prop_oneof![
        4 => Just(AGGREGATION_TEMPORALITY_CUMULATIVE),
        1 => Just(AGGREGATION_TEMPORALITY_DELTA),
        1 => Just(AGGREGATION_TEMPORALITY_UNSPECIFIED),
    ]
}

fn attr_strategy() -> impl Strategy<Value = WorkloadAttr> {
    (attr_key_strategy(), attr_value_strategy())
        .prop_map(|(key, value)| WorkloadAttr { key, value })
}

fn point_strategy() -> impl Strategy<Value = WorkloadPoint> {
    (
        -1_000_000_000i64..1_000_000_000i64,
        -1000.0f64..1000.0f64,
        prop::collection::vec(attr_strategy(), 0..=4),
    )
        .prop_map(|(ts_offset_ns, value, attrs)| WorkloadPoint {
            ts_offset_ns,
            value,
            attrs,
        })
}

fn metric_kind_strategy() -> impl Strategy<Value = WorkloadKind> {
    prop_oneof![
        Just(WorkloadKind::Gauge),
        (sum_temporality_strategy(), any::<bool>()).prop_map(|(temporality, is_monotonic)| {
            WorkloadKind::Sum {
                temporality,
                is_monotonic,
            }
        }),
    ]
}

fn metric_strategy() -> impl Strategy<Value = WorkloadMetric> {
    (
        metric_name_strategy(),
        metric_kind_strategy(),
        prop::collection::vec(point_strategy(), 1..=6),
    )
        .prop_map(|(name, kind, points)| WorkloadMetric { name, kind, points })
}

fn workload_strategy() -> impl Strategy<Value = Vec<WorkloadMetric>> {
    prop::collection::vec(metric_strategy(), 1..=4)
}

fn otlp_kv(attr: &WorkloadAttr) -> KeyValue {
    let value = match &attr.value {
        WorkloadValue::Str(s) => AnyValueVariant::StringValue(s.clone()),
        WorkloadValue::Bool(b) => AnyValueVariant::BoolValue(*b),
        WorkloadValue::Int(i) => AnyValueVariant::IntValue(*i),
        WorkloadValue::Double(d) => AnyValueVariant::DoubleValue(*d),
    };
    KeyValue {
        key: attr.key.clone(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn build_otlp_request(workload: &[WorkloadMetric]) -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = workload
        .iter()
        .map(|m| {
            let data_points: Vec<NumberDataPoint> = m
                .points
                .iter()
                .map(|p| NumberDataPoint {
                    attributes: p.attrs.iter().map(otlp_kv).collect(),
                    time_unix_nano: (INGEST_TS_NS + p.ts_offset_ns) as u64,
                    value: Some(NumberValue::AsDouble(p.value)),
                    ..Default::default()
                })
                .collect();
            match &m.kind {
                WorkloadKind::Gauge => Metric {
                    name: m.name.clone(),
                    data: Some(MetricData::Gauge(Gauge { data_points })),
                    ..Default::default()
                },
                WorkloadKind::Sum {
                    temporality,
                    is_monotonic,
                } => Metric {
                    name: m.name.clone(),
                    data: Some(MetricData::Sum(Sum {
                        data_points,
                        aggregation_temporality: *temporality,
                        is_monotonic: *is_monotonic,
                    })),
                    ..Default::default()
                },
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

fn otap_attr(attr: &WorkloadAttr) -> AttrRow {
    let value = match &attr.value {
        WorkloadValue::Str(s) => AttrValue::Str(s.clone()),
        WorkloadValue::Bool(b) => AttrValue::Bool(*b),
        WorkloadValue::Int(i) => AttrValue::Int(*i),
        WorkloadValue::Double(d) => AttrValue::Double(*d),
    };
    AttrRow {
        key: attr.key.clone(),
        value,
    }
}

fn build_otap_batch(
    workload: &[WorkloadMetric],
    encoder: &mut MetricsStreamEncoder,
    batch_id: i64,
) -> BatchArrowRecords {
    let metrics: Vec<MetricRow> = workload
        .iter()
        .map(|m| {
            let kind = match &m.kind {
                WorkloadKind::Gauge => MetricKind::Gauge,
                WorkloadKind::Sum {
                    temporality,
                    is_monotonic,
                } => MetricKind::Sum {
                    temporality: *temporality,
                    is_monotonic: *is_monotonic,
                },
            };
            let data_points: Vec<DataPointRow> = m
                .points
                .iter()
                .map(|p| DataPointRow {
                    time_unix_nano: INGEST_TS_NS + p.ts_offset_ns,
                    value: p.value,
                    attrs: p.attrs.iter().map(otap_attr).collect(),
                })
                .collect();
            MetricRow {
                name: m.name.clone(),
                kind,
                data_points,
            }
        })
        .collect();
    encoder
        .encode_batch(batch_id, &metrics)
        .expect("encode workload")
}

fn series_ids(out: &NormalizeOutput) -> BTreeSet<SeriesId> {
    out.points.iter().map(|p| p.series_id).collect()
}

fn samples(out: &NormalizeOutput) -> BTreeSet<(SeriesId, i64, u64)> {
    out.points
        .iter()
        .map(|p| (p.series_id, p.sample.ts_ns, p.sample.value.to_bits()))
        .collect()
}

fn rejection_multiset(rejected: &[Rejection]) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for r in rejected {
        *m.entry(format!("{r:?}")).or_insert(0) += 1;
    }
    m
}

/// Run `workload` through both paths and assert they agree.
#[allow(clippy::expect_used)]
fn assert_paths_agree(workload: &[WorkloadMetric]) {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    let otlp_out = normalize_metrics(&tenant, build_otlp_request(workload), &limits, INGEST_TS_NS);

    let mut encoder = MetricsStreamEncoder::new("differential").expect("new encoder");
    let raw_batch = build_otap_batch(workload, &mut encoder, 0);
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode otap batch");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);

    assert_eq!(
        series_ids(&otlp_out),
        series_ids(&otap_out),
        "series id sets differ"
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

proptest! {
    #[test]
    fn otlp_and_otap_agree_on_random_gauge_sum_workloads(workload in workload_strategy()) {
        assert_paths_agree(&workload);
    }
}

// --- edge tests -------------------------------------------------------

#[test]
fn empty_batch_agrees() {
    assert_paths_agree(&[]);
}

#[test]
fn future_skew_boundary_agrees() {
    let limits = IngestLimits::default();
    let at_bound = WorkloadPoint {
        ts_offset_ns: limits.max_future_skew_ns,
        value: 1.0,
        attrs: vec![],
    };
    let past_bound = WorkloadPoint {
        ts_offset_ns: limits.max_future_skew_ns + 1,
        value: 1.0,
        attrs: vec![],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "widgets".to_string(),
        kind: WorkloadKind::Gauge,
        points: vec![at_bound, past_bound],
    }]);
}

#[test]
fn too_old_boundary_agrees() {
    let limits = IngestLimits::default();
    let at_bound = WorkloadPoint {
        ts_offset_ns: -limits.max_ingest_lag_ns,
        value: 1.0,
        attrs: vec![],
    };
    let past_bound = WorkloadPoint {
        ts_offset_ns: -(limits.max_ingest_lag_ns + 1),
        value: 1.0,
        attrs: vec![],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "widgets".to_string(),
        kind: WorkloadKind::Gauge,
        points: vec![at_bound, past_bound],
    }]);
}

#[test]
fn duplicate_label_names_after_sanitization_agree() {
    // "foo.bar" and "foo-bar" both sanitize to "foo_bar": a collision on
    // both paths, not just OTAP's.
    let point = WorkloadPoint {
        ts_offset_ns: 0,
        value: 1.0,
        attrs: vec![
            WorkloadAttr {
                key: "foo.bar".to_string(),
                value: WorkloadValue::Str("1".to_string()),
            },
            WorkloadAttr {
                key: "foo-bar".to_string(),
                value: WorkloadValue::Str("2".to_string()),
            },
        ],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "requests".to_string(),
        kind: WorkloadKind::Gauge,
        points: vec![point],
    }]);
}

#[test]
fn complex_attribute_value_rejected_on_both_paths() {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    let otlp_req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "widgets".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: "blob".to_string(),
                                value: Some(AnyValue {
                                    value: Some(AnyValueVariant::BytesValue(vec![1, 2, 3])),
                                }),
                                ..Default::default()
                            }],
                            time_unix_nano: INGEST_TS_NS as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let otlp_out = normalize_metrics(&tenant, otlp_req, &limits, INGEST_TS_NS);
    assert!(otlp_out.points.is_empty());
    assert_eq!(otlp_out.rejected, vec![Rejection::ComplexAttributeValue]);

    let metrics = vec![MetricRow {
        name: "widgets".to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            attrs: vec![AttrRow {
                key: "blob".to_string(),
                value: AttrValue::Complex,
            }],
        }],
    }];
    let mut encoder = MetricsStreamEncoder::new("complex-attr").expect("new encoder");
    let raw_batch = encoder.encode_batch(0, &metrics).expect("encode");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);
    assert!(otap_out.points.is_empty());
    assert_eq!(otap_out.rejected, vec![Rejection::ComplexAttributeValue]);
}

#[test]
fn delta_sum_rejected_on_both_paths() {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    let otlp_req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests_total".to_string(),
                    data: Some(MetricData::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: INGEST_TS_NS as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let otlp_out = normalize_metrics(&tenant, otlp_req, &limits, INGEST_TS_NS);
    assert!(otlp_out.points.is_empty());
    assert_eq!(
        otlp_out.rejected,
        vec![Rejection::UnsupportedTemporality { count: 1 }]
    );

    let metrics = vec![MetricRow {
        name: "requests_total".to_string(),
        kind: MetricKind::Sum {
            temporality: ravel_otap::normalize::AGGREGATION_TEMPORALITY_DELTA,
            is_monotonic: true,
        },
        data_points: vec![DataPointRow {
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            attrs: vec![],
        }],
    }];
    let mut encoder = MetricsStreamEncoder::new("delta-sum").expect("new encoder");
    let raw_batch = encoder.encode_batch(0, &metrics).expect("encode");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);
    assert!(otap_out.points.is_empty());
    assert_eq!(
        otap_out.rejected,
        vec![Rejection::UnsupportedTemporality { count: 1 }]
    );
}

// --- histogram/summary workloads (ADR-0016 phase B2) -------------------

#[derive(Debug, Clone)]
struct WorkloadHistogramPoint {
    ts_offset_ns: i64,
    count: u64,
    sum: Option<f64>,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    flags: u32,
    min: Option<f64>,
    max: Option<f64>,
    exemplar_count: usize,
    attrs: Vec<WorkloadAttr>,
}

#[derive(Debug, Clone)]
struct WorkloadHistogramMetric {
    name: String,
    temporality: i32,
    points: Vec<WorkloadHistogramPoint>,
}

#[derive(Debug, Clone)]
struct WorkloadQuantile {
    quantile: f64,
    value: f64,
}

#[derive(Debug, Clone)]
struct WorkloadSummaryPoint {
    ts_offset_ns: i64,
    count: u64,
    sum: f64,
    quantiles: Vec<WorkloadQuantile>,
    flags: u32,
    attrs: Vec<WorkloadAttr>,
}

#[derive(Debug, Clone)]
struct WorkloadSummaryMetric {
    name: String,
    points: Vec<WorkloadSummaryPoint>,
}

// Strictly increasing, finite bounds: the shape `explode_histogram` accepts
// without hitting NonFiniteHistogramBound/HistogramBoundsNotIncreasing, so
// the random-workload proptests below exercise the accepted path densely;
// those two rejections get their own dedicated edge tests instead.
fn histogram_bounds_strategy() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(1.0f64..1_000.0, 0..=4).prop_map(|mut v| {
        v.sort_by(f64::total_cmp);
        v.dedup_by(|a, b| a == b);
        v
    })
}

fn histogram_point_strategy() -> impl Strategy<Value = WorkloadHistogramPoint> {
    histogram_bounds_strategy().prop_flat_map(|bounds| {
        let n = bounds.len() + 1;
        let bounds_for_map = bounds.clone();
        (
            -1_000_000_000i64..1_000_000_000i64,
            prop::collection::vec(0u64..1000, n),
            proptest::option::of(-1000.0f64..1000.0f64),
            any::<bool>(),
            prop::collection::vec(attr_strategy(), 0..=3),
            0usize..=3,
            proptest::option::of(-1000.0f64..1000.0f64),
            proptest::option::of(-1000.0f64..1000.0f64),
        )
            .prop_map(
                move |(
                    ts_offset_ns,
                    bucket_counts,
                    sum,
                    stale,
                    attrs,
                    exemplar_count,
                    min,
                    max,
                )| {
                    let count: u64 = bucket_counts.iter().sum();
                    WorkloadHistogramPoint {
                        ts_offset_ns,
                        count,
                        sum,
                        bucket_counts,
                        explicit_bounds: bounds_for_map.clone(),
                        flags: if stale {
                            DataPointFlags::NoRecordedValueMask as u32
                        } else {
                            0
                        },
                        min,
                        max,
                        exemplar_count,
                        attrs,
                    }
                },
            )
    })
}

fn histogram_metric_strategy() -> impl Strategy<Value = WorkloadHistogramMetric> {
    (
        metric_name_strategy(),
        sum_temporality_strategy(),
        prop::collection::vec(histogram_point_strategy(), 1..=4),
    )
        .prop_map(|(name, temporality, points)| WorkloadHistogramMetric {
            name,
            temporality,
            points,
        })
}

fn histogram_workload_strategy() -> impl Strategy<Value = Vec<WorkloadHistogramMetric>> {
    prop::collection::vec(histogram_metric_strategy(), 1..=3)
}

// Quantiles in [0, 1], strictly increasing (so distinct after sorting),
// finite: the shape `explode_summary` accepts. NonFiniteQuantile and
// DuplicateQuantile get their own dedicated edge tests.
fn summary_quantiles_strategy() -> impl Strategy<Value = Vec<WorkloadQuantile>> {
    prop::collection::vec((0.0f64..=1.0, -1000.0f64..1000.0f64), 0..=4).prop_map(|mut v| {
        v.sort_by(|a, b| f64::total_cmp(&a.0, &b.0));
        v.dedup_by(|a, b| a.0 == b.0);
        v.into_iter()
            .map(|(quantile, value)| WorkloadQuantile { quantile, value })
            .collect()
    })
}

fn summary_point_strategy() -> impl Strategy<Value = WorkloadSummaryPoint> {
    (
        -1_000_000_000i64..1_000_000_000i64,
        0u64..10_000,
        -1000.0f64..1000.0f64,
        summary_quantiles_strategy(),
        any::<bool>(),
        prop::collection::vec(attr_strategy(), 0..=3),
    )
        .prop_map(
            |(ts_offset_ns, count, sum, quantiles, stale, attrs)| WorkloadSummaryPoint {
                ts_offset_ns,
                count,
                sum,
                quantiles,
                flags: if stale {
                    DataPointFlags::NoRecordedValueMask as u32
                } else {
                    0
                },
                attrs,
            },
        )
}

fn summary_metric_strategy() -> impl Strategy<Value = WorkloadSummaryMetric> {
    (
        metric_name_strategy(),
        prop::collection::vec(summary_point_strategy(), 1..=4),
    )
        .prop_map(|(name, points)| WorkloadSummaryMetric { name, points })
}

fn summary_workload_strategy() -> impl Strategy<Value = Vec<WorkloadSummaryMetric>> {
    prop::collection::vec(summary_metric_strategy(), 1..=3)
}

fn build_otlp_histogram_request(
    workload: &[WorkloadHistogramMetric],
) -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = workload
        .iter()
        .map(|m| {
            let data_points: Vec<HistogramDataPoint> = m
                .points
                .iter()
                .map(|p| HistogramDataPoint {
                    attributes: p.attrs.iter().map(otlp_kv).collect(),
                    time_unix_nano: (INGEST_TS_NS + p.ts_offset_ns) as u64,
                    count: p.count,
                    sum: p.sum,
                    bucket_counts: p.bucket_counts.clone(),
                    explicit_bounds: p.explicit_bounds.clone(),
                    exemplars: (0..p.exemplar_count).map(|_| Exemplar::default()).collect(),
                    flags: p.flags,
                    min: p.min,
                    max: p.max,
                    ..Default::default()
                })
                .collect();
            Metric {
                name: m.name.clone(),
                data: Some(MetricData::Histogram(Histogram {
                    data_points,
                    aggregation_temporality: m.temporality,
                })),
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

fn build_otlp_summary_request(workload: &[WorkloadSummaryMetric]) -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = workload
        .iter()
        .map(|m| {
            let data_points: Vec<SummaryDataPoint> = m
                .points
                .iter()
                .map(|p| SummaryDataPoint {
                    attributes: p.attrs.iter().map(otlp_kv).collect(),
                    time_unix_nano: (INGEST_TS_NS + p.ts_offset_ns) as u64,
                    count: p.count,
                    sum: p.sum,
                    quantile_values: p
                        .quantiles
                        .iter()
                        .map(|q| ValueAtQuantile {
                            quantile: q.quantile,
                            value: q.value,
                        })
                        .collect(),
                    flags: p.flags,
                    ..Default::default()
                })
                .collect();
            Metric {
                name: m.name.clone(),
                data: Some(MetricData::Summary(Summary { data_points })),
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

fn build_otap_histogram_batch(
    workload: &[WorkloadHistogramMetric],
    encoder: &mut MetricsStreamEncoder,
    batch_id: i64,
) -> BatchArrowRecords {
    let histograms: Vec<HistogramMetricRow> = workload
        .iter()
        .map(|m| {
            let data_points: Vec<HistogramPointRow> = m
                .points
                .iter()
                .map(|p| HistogramPointRow {
                    time_unix_nano: INGEST_TS_NS + p.ts_offset_ns,
                    count: p.count,
                    sum: p.sum,
                    bucket_counts: p.bucket_counts.clone(),
                    explicit_bounds: p.explicit_bounds.clone(),
                    flags: p.flags,
                    min: p.min,
                    max: p.max,
                    exemplar_count: p.exemplar_count,
                    attrs: p.attrs.iter().map(otap_attr).collect(),
                })
                .collect();
            HistogramMetricRow {
                name: m.name.clone(),
                temporality: m.temporality,
                data_points,
            }
        })
        .collect();
    encoder
        .encode_batch_ext(batch_id, &[], &histograms, &[])
        .expect("encode histogram workload")
}

fn build_otap_summary_batch(
    workload: &[WorkloadSummaryMetric],
    encoder: &mut MetricsStreamEncoder,
    batch_id: i64,
) -> BatchArrowRecords {
    let summaries: Vec<SummaryMetricRow> = workload
        .iter()
        .map(|m| {
            let data_points: Vec<SummaryPointRow> = m
                .points
                .iter()
                .map(|p| SummaryPointRow {
                    time_unix_nano: INGEST_TS_NS + p.ts_offset_ns,
                    count: p.count,
                    sum: p.sum,
                    quantiles: p.quantiles.iter().map(|q| (q.quantile, q.value)).collect(),
                    flags: p.flags,
                    attrs: p.attrs.iter().map(otap_attr).collect(),
                })
                .collect();
            SummaryMetricRow {
                name: m.name.clone(),
                data_points,
            }
        })
        .collect();
    encoder
        .encode_batch_ext(batch_id, &[], &[], &summaries)
        .expect("encode summary workload")
}

#[allow(clippy::expect_used)]
fn assert_histogram_paths_agree(workload: &[WorkloadHistogramMetric]) {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    let otlp_out = normalize_metrics(
        &tenant,
        build_otlp_histogram_request(workload),
        &limits,
        INGEST_TS_NS,
    );

    let mut encoder = MetricsStreamEncoder::new("differential-histogram").expect("new encoder");
    let raw_batch = build_otap_histogram_batch(workload, &mut encoder, 0);
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode otap batch");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);

    assert_eq!(
        series_ids(&otlp_out),
        series_ids(&otap_out),
        "series id sets differ"
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

#[allow(clippy::expect_used)]
fn assert_summary_paths_agree(workload: &[WorkloadSummaryMetric]) {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    let otlp_out = normalize_metrics(
        &tenant,
        build_otlp_summary_request(workload),
        &limits,
        INGEST_TS_NS,
    );

    let mut encoder = MetricsStreamEncoder::new("differential-summary").expect("new encoder");
    let raw_batch = build_otap_summary_batch(workload, &mut encoder, 0);
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode otap batch");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);

    assert_eq!(
        series_ids(&otlp_out),
        series_ids(&otap_out),
        "series id sets differ"
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

proptest! {
    #[test]
    fn otlp_and_otap_agree_on_random_histogram_workloads(workload in histogram_workload_strategy()) {
        assert_histogram_paths_agree(&workload);
    }

    #[test]
    fn otlp_and_otap_agree_on_random_summary_workloads(workload in summary_workload_strategy()) {
        assert_summary_paths_agree(&workload);
    }
}

// --- histogram/summary edge tests ---------------------------------------

#[test]
fn histogram_bucket_count_mismatch_agrees() {
    // 2 bounds require 3 bucket_counts; only 2 are given.
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 3,
        sum: Some(3.0),
        bucket_counts: vec![1, 2],
        explicit_bounds: vec![1.0, 2.0],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_non_finite_bound_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 2,
        sum: Some(2.0),
        bucket_counts: vec![1, 1],
        explicit_bounds: vec![f64::NAN],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_bounds_not_increasing_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 3,
        sum: Some(3.0),
        bucket_counts: vec![1, 1, 1],
        explicit_bounds: vec![5.0, 1.0],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_count_overflow_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 0,
        sum: None,
        bucket_counts: vec![u64::MAX, 1],
        explicit_bounds: vec![1.0],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_delta_temporality_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 1,
        sum: Some(1.0),
        bucket_counts: vec![1],
        explicit_bounds: vec![],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_DELTA,
        points: vec![point],
    }]);
}

#[test]
fn histogram_min_max_dropped_informational_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 2,
        sum: Some(2.0),
        bucket_counts: vec![1, 1],
        explicit_bounds: vec![1.0],
        flags: 0,
        min: Some(0.1),
        max: Some(1.9),
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_exemplars_dropped_informational_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 2,
        sum: Some(2.0),
        bucket_counts: vec![1, 1],
        explicit_bounds: vec![1.0],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 2,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn histogram_stale_marker_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 2,
        sum: Some(2.0),
        bucket_counts: vec![1, 1],
        explicit_bounds: vec![1.0],
        flags: DataPointFlags::NoRecordedValueMask as u32,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![],
    };
    assert_histogram_paths_agree(&[WorkloadHistogramMetric {
        name: "req_duration".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }]);
}

#[test]
fn summary_non_finite_quantile_agrees() {
    let point = WorkloadSummaryPoint {
        ts_offset_ns: 0,
        count: 1,
        sum: 1.0,
        quantiles: vec![WorkloadQuantile {
            quantile: f64::NAN,
            value: 1.0,
        }],
        flags: 0,
        attrs: vec![],
    };
    assert_summary_paths_agree(&[WorkloadSummaryMetric {
        name: "req_latency".to_string(),
        points: vec![point],
    }]);
}

#[test]
fn summary_duplicate_quantile_agrees() {
    let point = WorkloadSummaryPoint {
        ts_offset_ns: 0,
        count: 2,
        sum: 2.0,
        quantiles: vec![
            WorkloadQuantile {
                quantile: 0.5,
                value: 1.0,
            },
            WorkloadQuantile {
                quantile: 0.5,
                value: 2.0,
            },
        ],
        flags: 0,
        attrs: vec![],
    };
    assert_summary_paths_agree(&[WorkloadSummaryMetric {
        name: "req_latency".to_string(),
        points: vec![point],
    }]);
}

#[test]
fn summary_stale_marker_agrees() {
    let point = WorkloadSummaryPoint {
        ts_offset_ns: 0,
        count: 1,
        sum: 1.0,
        quantiles: vec![WorkloadQuantile {
            quantile: 0.5,
            value: 1.0,
        }],
        flags: DataPointFlags::NoRecordedValueMask as u32,
        attrs: vec![],
    };
    assert_summary_paths_agree(&[WorkloadSummaryMetric {
        name: "req_latency".to_string(),
        points: vec![point],
    }]);
}

/// docs/ingest-breadth-plan.md section 4.1's CH-1 cross-protocol identity
/// vector, adapted to a shape both paths can actually produce: the plan's
/// vector maps `job`/`instance` through resource attributes, but the OTAP
/// encoder never emits `RESOURCE_ATTRS` (documented scope gap, see
/// src/normalize.rs's module docs) and its `MetricRow` shape has no
/// resource field to carry them. Passing `job`/`instance` as plain
/// data-point attributes instead is a valid input on both paths and still
/// exercises everything CH-1 is actually checking here: bucket
/// accumulation, `le`/`quantile` float formatting, and SeriesId agreement
/// for the exact bounds/counts/sum/count the plan specifies.
#[test]
fn ch1_histogram_bucket_shape_identity_vector_agrees() {
    let point = WorkloadHistogramPoint {
        ts_offset_ns: 0,
        count: 10,
        sum: Some(42.5),
        bucket_counts: vec![1, 2, 3, 4],
        explicit_bounds: vec![0.1, 1.0, 10.0],
        flags: 0,
        min: None,
        max: None,
        exemplar_count: 0,
        attrs: vec![
            WorkloadAttr {
                key: "job".to_string(),
                value: WorkloadValue::Str("svc".to_string()),
            },
            WorkloadAttr {
                key: "instance".to_string(),
                value: WorkloadValue::Str("i-1".to_string()),
            },
        ],
    };
    let workload = [WorkloadHistogramMetric {
        name: "http_request_duration_seconds".to_string(),
        temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        points: vec![point],
    }];

    assert_histogram_paths_agree(&workload);

    let tenant = TenantId::new("t-fixture");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        build_otlp_histogram_request(&workload),
        &limits,
        INGEST_TS_NS,
    );
    assert!(otlp_out.rejected.is_empty());

    let expected: BTreeMap<(&str, &str), f64> = [
        (("http_request_duration_seconds_bucket", "0.1"), 1.0),
        (("http_request_duration_seconds_bucket", "1"), 3.0),
        (("http_request_duration_seconds_bucket", "10"), 6.0),
        (("http_request_duration_seconds_bucket", "+Inf"), 10.0),
    ]
    .into_iter()
    .collect();

    let mut bucket_seen = 0;
    for p in &otlp_out.points {
        let name = p.labels.get("__name__").expect("__name__ label");
        if name == "http_request_duration_seconds_bucket" {
            let le = p.labels.get("le").expect("le label");
            let want = expected[&(name, le)];
            assert_eq!(
                p.sample.value.to_bits(),
                want.to_bits(),
                "le={le} value mismatch"
            );
            bucket_seen += 1;
        } else if name == "http_request_duration_seconds_sum" {
            assert_eq!(p.sample.value.to_bits(), 42.5f64.to_bits());
        } else if name == "http_request_duration_seconds_count" {
            assert_eq!(p.sample.value.to_bits(), 10.0f64.to_bits());
        } else {
            panic!("unexpected series name {name}");
        }
    }
    assert_eq!(bucket_seen, 4, "expected all 4 buckets present");
    assert_eq!(otlp_out.points.len(), 6, "expected 4 buckets + sum + count");
}
