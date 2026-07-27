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
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    metric::Data as MetricData,
};
use proptest::prelude::*;

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::normalize::{AGGREGATION_TEMPORALITY_CUMULATIVE, normalize_decoded};
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
    Sum { is_monotonic: bool },
}

#[derive(Debug, Clone)]
struct WorkloadMetric {
    name: String,
    kind: WorkloadKind,
    points: Vec<WorkloadPoint>,
}

fn attr_key_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("region".to_string()),
        Just("host".to_string()),
        Just("shard".to_string()),
        Just("az".to_string()),
    ]
}

fn attr_value_strategy() -> impl Strategy<Value = WorkloadValue> {
    prop_oneof![
        "[a-z]{1,8}".prop_map(WorkloadValue::Str),
        any::<bool>().prop_map(WorkloadValue::Bool),
        (-1000i64..1000).prop_map(WorkloadValue::Int),
        (-1000.0f64..1000.0).prop_map(WorkloadValue::Double),
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
        any::<bool>().prop_map(|is_monotonic| WorkloadKind::Sum { is_monotonic }),
    ]
}

fn metric_strategy() -> impl Strategy<Value = WorkloadMetric> {
    (
        "[a-z]{3,10}",
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
                WorkloadKind::Sum { is_monotonic } => Metric {
                    name: m.name.clone(),
                    data: Some(MetricData::Sum(Sum {
                        data_points,
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
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
                WorkloadKind::Sum { is_monotonic } => MetricKind::Sum {
                    temporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
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
