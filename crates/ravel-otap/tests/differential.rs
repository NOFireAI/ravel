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
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value as OtlpExemplarValue;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, DataPointFlags, Exemplar, Gauge, Histogram, HistogramDataPoint, Metric,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    metric::Data as MetricData, summary_data_point::ValueAtQuantile,
};
use proptest::prelude::*;

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, ExemplarRow, ExemplarValue, HistogramMetricRow,
    HistogramPointRow, MetricKind, MetricRow, MetricsStreamEncoder, SummaryMetricRow,
    SummaryPointRow,
};
use ravel_otap::normalize::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, AGGREGATION_TEMPORALITY_DELTA,
    AGGREGATION_TEMPORALITY_UNSPECIFIED, ANY_VALUE_TYPE_BYTES, ANY_VALUE_TYPE_MAP,
    ANY_VALUE_TYPE_SLICE, ANY_VALUE_TYPE_STRING, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM,
    normalize_decoded, normalize_decoded_with_exemplars,
};
use ravel_otap::proto::experimental::arrow::v1::{ArrowPayloadType, BatchArrowRecords};
use ravel_otap::stream::{DecodedBatch, StreamConfig, StreamState};
use ravel_otlp::normalize::normalize_metrics_with_exemplars;
use ravel_otlp::{IngestLimits, NormalizeOutput, Rejection, normalize_metrics};
use ravel_types::{ExemplarCap, SeriesId, TenantId};

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
    flags: u32,
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
    unit: String,
    kind: WorkloadKind,
    points: Vec<WorkloadPoint>,
}

fn attr_key_strategy() -> impl Strategy<Value = String> {
    // The over-long key (> max_label_name_len = 256) drives the per-label
    // LabelNameTooLong rejection. Combined with the over-long value below and
    // the arbitrary attribute order proptest already produces, this reaches
    // the ordering-sensitive rejection case: a point with a name violator and
    // a value violator whose input order differs from key-sorted order. Its
    // sort key ("k"...)
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
    // per-label LabelValueTooLong rejection.
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
    // the name-before-temporality rule: the name must be classed before
    // temporality.
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
        any::<bool>(),
        prop::collection::vec(attr_strategy(), 0..=4),
    )
        .prop_map(|(ts_offset_ns, value, stale, attrs)| WorkloadPoint {
            ts_offset_ns,
            value,
            flags: if stale {
                DataPointFlags::NoRecordedValueMask as u32
            } else {
                0
            },
            attrs,
        })
}

/// A spread of units that exercise the ADR-0085 Decision 2 pass on both
/// paths: mapped simple units (`By`, `s`, `ms`), the gauge-only ratio (`1`),
/// compound and annotation forms (`By/s`, `{packet}/s`), an unknown unit
/// (`furlong`, no suffix), and empty (no suffix). Both paths apply the same
/// `prometheus_family_name`, so any divergence in how each feeds it a unit or
/// monotonic flag shows up as a different series-name set.
fn unit_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("By".to_string()),
        Just("s".to_string()),
        Just("ms".to_string()),
        Just("1".to_string()),
        Just("By/s".to_string()),
        Just("{packet}/s".to_string()),
        Just("furlong".to_string()),
    ]
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
        unit_strategy(),
        metric_kind_strategy(),
        prop::collection::vec(point_strategy(), 1..=6),
    )
        .prop_map(|(name, unit, kind, points)| WorkloadMetric {
            name,
            unit,
            kind,
            points,
        })
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
                    flags: p.flags,
                    ..Default::default()
                })
                .collect();
            match &m.kind {
                WorkloadKind::Gauge => Metric {
                    name: m.name.clone(),
                    unit: m.unit.clone(),
                    data: Some(MetricData::Gauge(Gauge { data_points })),
                    ..Default::default()
                },
                WorkloadKind::Sum {
                    temporality,
                    is_monotonic,
                } => Metric {
                    name: m.name.clone(),
                    unit: m.unit.clone(),
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
                    exemplars: vec![],
                    time_unix_nano: INGEST_TS_NS + p.ts_offset_ns,
                    value: p.value,
                    flags: p.flags,
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
    let units: Vec<&str> = workload.iter().map(|m| m.unit.as_str()).collect();
    encoder
        .encode_batch_units(batch_id, &metrics, &units)
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
        flags: 0,
        attrs: vec![],
    };
    let past_bound = WorkloadPoint {
        ts_offset_ns: limits.max_future_skew_ns + 1,
        value: 1.0,
        flags: 0,
        attrs: vec![],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "widgets".to_string(),
        unit: String::new(),
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
        flags: 0,
        attrs: vec![],
    };
    let past_bound = WorkloadPoint {
        ts_offset_ns: -(limits.max_ingest_lag_ns + 1),
        value: 1.0,
        flags: 0,
        attrs: vec![],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "widgets".to_string(),
        unit: String::new(),
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
        flags: 0,
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
        unit: String::new(),
        kind: WorkloadKind::Gauge,
        points: vec![point],
    }]);
}

/// A gauge point flagged `NoRecordedValue` is stored as the Prometheus stale
/// marker on both paths, matching `histogram_stale_marker_agrees`/
/// `summary_stale_marker_agrees`: `ravel_otlp::build_point` never consults
/// the value oneof for a stale point, and `flatten_dp`/`normalize_decoded`
/// now mirror that exactly (see src/normalize.rs's `has_no_recorded_value`
/// use just before attribute admission).
#[test]
fn number_stale_marker_agrees() {
    let point = WorkloadPoint {
        ts_offset_ns: 0,
        value: 1.0,
        flags: DataPointFlags::NoRecordedValueMask as u32,
        attrs: vec![],
    };
    assert_paths_agree(&[WorkloadMetric {
        name: "requests".to_string(),
        unit: String::new(),
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
            exemplars: vec![],
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            flags: 0,
            attrs: vec![AttrRow {
                key: "blob".to_string(),
                value: AttrValue::Complex(ANY_VALUE_TYPE_BYTES),
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

/// All three non-scalar AnyValue `type` slots -- map (5), slice
/// (6), and bytes (7) per otap-spec.md section 5.5.1 -- must survive
/// encode-then-decode and land on the normalizer's `ComplexAttributeValue`
/// rejection path. This pins the spec-aligned discriminant values: an
/// earlier revision numbered these 7=map/6=array/5=bytes (after OTLP's oneof
/// field numbers), so a regression that reverted them would still reject
/// here (every slot 5-7 is non-scalar) but would misread the scalar slots,
/// which the differential proptests catch. This test's job is to prove the
/// three complex slots are all reachable and all rejected together.
#[test]
fn complex_attribute_type_slots_round_trip_rejected() {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();

    // One gauge metric per complex type slot, each with a single point
    // carrying one attribute of that type. Each point must be rejected.
    let metrics: Vec<MetricRow> = [
        ("map_attr", ANY_VALUE_TYPE_MAP),
        ("slice_attr", ANY_VALUE_TYPE_SLICE),
        ("bytes_attr", ANY_VALUE_TYPE_BYTES),
    ]
    .into_iter()
    .map(|(name, ty)| MetricRow {
        name: name.to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            exemplars: vec![],
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            flags: 0,
            attrs: vec![AttrRow {
                key: "blob".to_string(),
                value: AttrValue::Complex(ty),
            }],
        }],
    })
    .collect();

    let mut encoder = MetricsStreamEncoder::new("complex-slots").expect("new encoder");
    let raw_batch = encoder.encode_batch(0, &metrics).expect("encode");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw_batch).expect("decode");
    let otap_out = normalize_decoded(&tenant, &decoded, &limits, INGEST_TS_NS);

    assert!(otap_out.points.is_empty());
    assert_eq!(
        otap_out.rejected,
        vec![
            Rejection::ComplexAttributeValue,
            Rejection::ComplexAttributeValue,
            Rejection::ComplexAttributeValue,
        ]
    );
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
            exemplars: vec![],
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            flags: 0,
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

/// The logical exemplars a workload point carries, as `(ts_ns, value,
/// trace_id)` triples both paths build their wire form from. Timestamps are
/// distinct per exemplar and values ascend from 0, so the admission cap and the
/// bucket-attachment rule (an exemplar joins the bucket series whose `le` bound
/// is the smallest at or above its value) are exercised with no ties to break:
/// any disagreement in either rule shows up as a different
/// `HistogramExemplarsDropped` count on one side.
fn workload_exemplars(
    point: &WorkloadHistogramPoint,
) -> impl Iterator<Item = (i64, f64, [u8; 16])> + '_ {
    (0..point.exemplar_count).map(move |i| {
        (
            INGEST_TS_NS + point.ts_offset_ns + i as i64,
            i as f64,
            [i as u8 + 1; 16],
        )
    })
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
                    exemplars: workload_exemplars(p)
                        .map(|(ts_ns, value, trace_id)| Exemplar {
                            time_unix_nano: ts_ns as u64,
                            value: Some(OtlpExemplarValue::AsDouble(value)),
                            trace_id: trace_id.to_vec(),
                            ..Default::default()
                        })
                        .collect(),
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
                    exemplars: workload_exemplars(p)
                        .map(|(ts_ns, value, trace_id)| ExemplarRow {
                            time_unix_nano: ts_ns,
                            value: ExemplarValue::Double(value),
                            trace_id: Some(trace_id),
                            span_id: None,
                            attrs: vec![],
                        })
                        .collect(),
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

    // Compare the rejection classes at the shared admission layer
    // (`*_with_exemplars`), not the wrapper outputs. All three wrappers discard
    // the exemplars the cap admits (nothing stores them yet) and count that
    // discard into `HistogramExemplarsDropped`. They still disagree at the
    // too-many-points return, where OTLP and Remote Write count the request's
    // exemplars and OTAP cannot, having decoded none. That wrapper bookkeeping
    // is not an admission decision;
    // the ADR-0011 parity that matters is that the two paths make the SAME cap
    // decisions, which is what the `_with_exemplars` outputs expose. Comparing
    // here is future-proof: it stays correct whether or not either wrapper
    // counts its discards, and it still asserts the cap-level
    // `HistogramExemplarsDropped` counts agree byte for byte.
    let otlp_full = normalize_metrics_with_exemplars(
        &tenant,
        build_otlp_histogram_request(workload),
        &limits,
        INGEST_TS_NS,
        &mut ExemplarCap::new(limits.exemplar_cap_window_ns),
    );
    let otap_full = normalize_decoded_with_exemplars(
        &tenant,
        &decoded,
        &limits,
        INGEST_TS_NS,
        &mut ExemplarCap::new(limits.exemplar_cap_window_ns),
    );
    assert_eq!(
        rejection_multiset(&otlp_full.output.rejected),
        rejection_multiset(&otap_full.output.rejected),
        "rejection classes differ"
    );
    // The two paths admit the same exemplars through the cap.
    assert_eq!(
        otlp_full.exemplars.len(),
        otap_full.exemplars.len(),
        "admitted exemplar counts differ"
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

/// Cross-protocol identity vector for histogram bucket shape, in a form both
/// paths can actually produce: `job`/`instance` pass as plain data-point
/// attributes rather than resource attributes, because the OTAP encoder never
/// emits `RESOURCE_ATTRS` (documented scope gap, see src/normalize.rs's module
/// docs) and its `MetricRow` shape has no resource field to carry them. This
/// still exercises everything the vector checks: bucket accumulation,
/// `le`/`quantile` float formatting, and SeriesId agreement for the exact
/// bounds/counts/sum/count.
#[test]
fn histogram_bucket_shape_identity_vector_agrees() {
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

// --- NUMBER_DATA_POINTS int_value fallback ---------------------------------
//
// `MetricsStreamEncoder` never emits `int_value` (see encode.rs's module
// docs), so exercising it takes a hand-built `RecordBatch` fed straight into
// `normalize_decoded`, bypassing the encoder's fixed schema.

/// Build a root-only `DecodedBatch` (one Gauge metric at id 0, zero data
/// points so `NUMBER_DATA_POINTS` is omitted per otap-spec.md section 3.1),
/// then splice in `dp_batch` as its `NUMBER_DATA_POINTS` payload.
#[allow(clippy::expect_used)]
fn decode_with_custom_number_dp(metric_name: &str, dp_batch: RecordBatch) -> NormalizeOutput {
    let metrics = vec![MetricRow {
        name: metric_name.to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![],
    }];
    let mut encoder = MetricsStreamEncoder::new("hand-built-number-dp").expect("new encoder");
    let raw_batch = encoder
        .encode_batch(0, &metrics)
        .expect("encode root-only batch");
    let mut state = StreamState::new(StreamConfig::default());
    let mut decoded = state.decode(raw_batch).expect("decode root-only batch");
    decoded
        .payloads
        .push((ArrowPayloadType::NumberDataPoints, dp_batch));
    normalize_decoded(
        &TenantId::new("acme"),
        &decoded,
        &IngestLimits::default(),
        INGEST_TS_NS,
    )
}

fn gauge_request(name: &str, data_points: Vec<NumberDataPoint>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.to_string(),
                    data: Some(MetricData::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A `NUMBER_DATA_POINTS` batch with only an `int_value` column (no
/// `double_value` column at all) must decode instead of being silently
/// dropped: the two value columns are alternatives, neither individually
/// required (otap-spec.md section 5.3.2). Uses a small int (well under
/// 2^53) so the value survives the `f64` round trip exactly and this does
/// not exercise `IntegerValuePrecisionLoss`, which the OTAP int_value
/// fallback does not yet track (see the crate report).
#[test]
fn number_data_point_int_value_only_column_decodes() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("parent_id", DataType::UInt16, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("int_value", DataType::Int64, false),
    ]);
    let dp_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32])),
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(TimestampNanosecondArray::from(vec![INGEST_TS_NS])),
            Arc::new(Int64Array::from(vec![42i64])),
        ],
    )
    .expect("build int-only number dp batch");

    let otap_out = decode_with_custom_number_dp("widgets", dp_batch);
    assert_eq!(
        otap_out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        otap_out.rejected
    );
    assert_eq!(otap_out.points.len(), 1);
    assert_eq!(otap_out.points[0].sample.value.to_bits(), 42.0f64.to_bits());

    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        gauge_request(
            "widgets",
            vec![NumberDataPoint {
                time_unix_nano: INGEST_TS_NS as u64,
                value: Some(NumberValue::AsInt(42)),
                ..Default::default()
            }],
        ),
        &limits,
        INGEST_TS_NS,
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

/// A `NUMBER_DATA_POINTS` batch where `double_value` is a nullable column
/// that is null on some rows must fall back to `int_value` for exactly
/// those rows, not treat the null as a decode error or a silent 0.0.
#[test]
fn number_data_point_null_double_value_falls_back_to_int_value() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("parent_id", DataType::UInt16, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, true),
        Field::new("int_value", DataType::Int64, true),
    ]);
    let dp_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32, 1u32])),
            Arc::new(UInt16Array::from(vec![0u16, 0u16])),
            Arc::new(TimestampNanosecondArray::from(vec![
                INGEST_TS_NS,
                INGEST_TS_NS + 1_000_000,
            ])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(Int64Array::from(vec![None, Some(7)])),
        ],
    )
    .expect("build mixed double/int number dp batch");

    let otap_out = decode_with_custom_number_dp("widgets", dp_batch);
    assert_eq!(
        otap_out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        otap_out.rejected
    );

    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        gauge_request(
            "widgets",
            vec![
                NumberDataPoint {
                    time_unix_nano: INGEST_TS_NS as u64,
                    value: Some(NumberValue::AsDouble(1.5)),
                    ..Default::default()
                },
                NumberDataPoint {
                    time_unix_nano: (INGEST_TS_NS + 1_000_000) as u64,
                    value: Some(NumberValue::AsInt(7)),
                    ..Default::default()
                },
            ],
        ),
        &limits,
        INGEST_TS_NS,
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

// --- DELTA-decode id/parent_id columns (otap-spec.md section
// 6.4.2). These hand-build every payload `RecordBatch` (rather than going
// through `MetricsStreamEncoder`, which always tags its own `id`/
// `parent_id` columns `encoding=plain`) so the encoded column and the
// decoded value can differ, which is the only way to observe whether
// decoding actually happened. ---

/// Attach an `encoding` field-metadata declaration (otap-spec.md section
/// 6.4), or leave it absent (`None`) to exercise the no-declaration-means-
/// DELTA default.
fn id_field(name: &str, data_type: DataType, encoding: Option<&str>) -> Field {
    let field = Field::new(name, data_type, false);
    match encoding {
        Some(value) => field.with_metadata(
            [("encoding".to_string(), value.to_string())]
                .into_iter()
                .collect(),
        ),
        None => field,
    }
}

/// Build a `DecodedBatch` directly from hand-built payloads, bypassing the
/// encoder entirely.
fn decode_custom_batch(payloads: Vec<(ArrowPayloadType, RecordBatch)>) -> NormalizeOutput {
    let decoded = DecodedBatch {
        batch_id: 0,
        payloads,
    };
    normalize_decoded(
        &TenantId::new("acme"),
        &decoded,
        &IngestLimits::default(),
        INGEST_TS_NS,
    )
}

/// A two-metric `UNIVARIATE_METRICS` batch whose `id` column carries
/// `encoded` (the still-encoded values); `id_encoding` controls that
/// column's field-metadata declaration.
fn root_batch(encoded: [u16; 2], names: [&str; 2], id_encoding: Option<&str>) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt16, id_encoding),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt16Array::from(encoded.to_vec())),
            Arc::new(UInt8Array::from(vec![METRIC_TYPE_GAUGE, METRIC_TYPE_GAUGE])),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("build root batch")
}

/// A two-row `NUMBER_DATA_POINTS` batch whose `id` and `parent_id` columns
/// carry `encoded_ids`/`encoded_parents` (the still-encoded values);
/// `id_encoding`/`parent_encoding` control those columns' field-metadata
/// declarations.
fn number_dp_batch(
    encoded_ids: [u32; 2],
    encoded_parents: [u16; 2],
    values: [f64; 2],
    id_encoding: Option<&str>,
    parent_encoding: Option<&str>,
) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt32, id_encoding),
        id_field("parent_id", DataType::UInt16, parent_encoding),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(encoded_ids.to_vec())),
            Arc::new(UInt16Array::from(encoded_parents.to_vec())),
            Arc::new(TimestampNanosecondArray::from(vec![
                INGEST_TS_NS,
                INGEST_TS_NS + 1_000_000,
            ])),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .expect("build number dp batch")
}

/// A `NUMBER_DP_ATTRS` batch with one string attribute per row. `parent_ids`
/// must be the *decoded* `NUMBER_DATA_POINTS.id` values. This batch declares
/// no `encoding` on its `parent_id` column, so it takes the QUASI-DELTA
/// default (otap-spec.md section 6.4.3); because the two
/// rows carry different `str` values they never match as equality columns and
/// each decodes to its own absolute value, so the literal `parent_ids` here
/// are exactly what QUASI-DELTA yields. See the dedicated QUASI-DELTA tests
/// below for a run of matching rows that actually accumulates.
fn number_dp_attrs_batch(parent_ids: [u32; 2], values: [&str; 2]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("parent_id", DataType::UInt32, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, false),
        Field::new("str", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(parent_ids.to_vec())),
            Arc::new(StringArray::from(vec!["region", "region"])),
            Arc::new(UInt8Array::from(vec![
                ANY_VALUE_TYPE_STRING,
                ANY_VALUE_TYPE_STRING,
            ])),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .expect("build attrs batch")
}

fn gauge_request_with_region(
    name: &str,
    points: [(i64, f64, &str); 2],
) -> ExportMetricsServiceRequest {
    gauge_request(
        name,
        points
            .into_iter()
            .map(|(ts, value, region)| NumberDataPoint {
                attributes: vec![KeyValue {
                    key: "region".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(region.to_string())),
                    }),
                    ..Default::default()
                }],
                time_unix_nano: ts as u64,
                value: Some(NumberValue::AsDouble(value)),
                ..Default::default()
            })
            .collect(),
    )
}

/// `NUMBER_DATA_POINTS.id` and `.parent_id` both default to DELTA
/// (otap-spec.md section 5.3.2). This scenario only produces the expected
/// two `widgets` points, correctly labeled, if both columns are decoded:
/// undecoded, the second point's `parent_id` (encoded `0`, decoded `1`)
/// would misattribute it to `metric_zero`, and the second point's `id`
/// (encoded `5`, decoded `15`) would fail to join `NUMBER_DP_ATTRS`, losing
/// its `region` label.
fn assert_number_dp_id_and_parent_id_scenario_agrees(
    id_encoding: Option<&str>,
    parent_encoding: Option<&str>,
) {
    let root = root_batch([0, 1], ["metric_zero", "widgets"], Some("plain"));
    // dp id: encoded [10, 5] -> decoded [10, 15] (DELTA).
    // dp parent_id: encoded [1, 0] -> decoded [1, 1] (DELTA); both rows
    // belong to metric id 1, "widgets".
    let dp = number_dp_batch([10, 5], [1, 0], [1.0, 2.0], id_encoding, parent_encoding);
    let attrs = number_dp_attrs_batch([10, 15], ["us", "eu"]);

    let otap_out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::NumberDataPoints, dp),
        (ArrowPayloadType::NumberDpAttrs, attrs),
    ]);
    assert_eq!(
        otap_out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        otap_out.rejected
    );
    assert_eq!(otap_out.points.len(), 2);

    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        gauge_request_with_region(
            "widgets",
            [
                (INGEST_TS_NS, 1.0, "us"),
                (INGEST_TS_NS + 1_000_000, 2.0, "eu"),
            ],
        ),
        &limits,
        INGEST_TS_NS,
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

#[test]
fn number_data_points_id_and_parent_id_delta_declared_decodes() {
    assert_number_dp_id_and_parent_id_scenario_agrees(Some("delta"), Some("delta"));
}

#[test]
fn number_data_points_id_and_parent_id_default_encoding_decodes_as_delta() {
    assert_number_dp_id_and_parent_id_scenario_agrees(None, None);
}

/// `UNIVARIATE_METRICS.id` defaults to DELTA (otap-spec.md section 5.3.1).
/// This scenario's single data point only lands on `widgets` (root-encoded
/// id `2`, decoded `5`) if the root `id` column is decoded; undecoded, its
/// literal `parent_id` of `5` matches no root row and the point is
/// classified an unknown metric type instead.
fn assert_univariate_metrics_id_scenario_agrees(root_id_encoding: Option<&str>) {
    // root id: encoded [3, 2] -> decoded [3, 5]; "widgets" is metric id 5.
    let root = root_batch([3, 2], ["metric_zero", "widgets"], root_id_encoding);
    let dp = number_dp_batch([0, 0], [5, 5], [1.0, 2.0], Some("plain"), Some("plain"));

    let otap_out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::NumberDataPoints, dp),
    ]);
    assert_eq!(
        otap_out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        otap_out.rejected
    );
    assert_eq!(otap_out.points.len(), 2);

    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        gauge_request(
            "widgets",
            vec![
                NumberDataPoint {
                    time_unix_nano: INGEST_TS_NS as u64,
                    value: Some(NumberValue::AsDouble(1.0)),
                    ..Default::default()
                },
                NumberDataPoint {
                    time_unix_nano: (INGEST_TS_NS + 1_000_000) as u64,
                    value: Some(NumberValue::AsDouble(2.0)),
                    ..Default::default()
                },
            ],
        ),
        &limits,
        INGEST_TS_NS,
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}

#[test]
fn univariate_metrics_id_delta_declared_decodes() {
    assert_univariate_metrics_id_scenario_agrees(Some("delta"));
}

#[test]
fn univariate_metrics_id_default_encoding_decodes_as_delta() {
    assert_univariate_metrics_id_scenario_agrees(None);
}

/// An `id`/`parent_id` column whose `encoding` field metadata is neither
/// `"plain"` nor `"delta"` must be rejected as a typed error, not guessed at
/// as one or the other.
#[test]
fn number_data_points_unrecognized_id_encoding_declaration_is_rejected() {
    let dp_batch = number_dp_batch([0, 0], [0, 0], [1.0, 2.0], Some("lz4"), Some("plain"));

    let otap_out = decode_with_custom_number_dp("widgets", dp_batch);
    assert_eq!(
        otap_out.points.len(),
        0,
        "no points should decode from a batch with an unrecognized id encoding"
    );
    assert_eq!(
        otap_out.rejected,
        vec![Rejection::UnsupportedMetricType {
            metric_type: "unrecognized_id_encoding",
            count: 2,
        }]
    );
}

// --- QUASI-DELTA-decode the `parent_id` column of the `*Attrs`
// and `*DpExemplars` tables (otap-spec.md section 6.4.3), which the core
// id/parent_id DELTA decoding does not cover. These, like the DELTA tests
// above, hand-build every payload `RecordBatch` so the encoded column and the
// decoded value can differ, the only way to observe that decoding actually
// happened. ---

/// A single-metric `UNIVARIATE_METRICS` batch (`id` plain) for one gauge.
fn single_gauge_root(name: &str) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt16, Some("plain")),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(UInt8Array::from(vec![METRIC_TYPE_GAUGE])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("build single gauge root batch")
}

/// A single-metric `UNIVARIATE_METRICS` batch (`id` plain) for one cumulative
/// histogram.
fn single_histogram_root(name: &str) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt16, Some("plain")),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("aggregation_temporality", DataType::Int32, true),
        Field::new("is_monotonic", DataType::Boolean, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(UInt8Array::from(vec![METRIC_TYPE_HISTOGRAM])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(Int32Array::from(vec![Some(
                AGGREGATION_TEMPORALITY_CUMULATIVE,
            )])),
            Arc::new(BooleanArray::from(vec![None::<bool>])),
        ],
    )
    .expect("build single histogram root batch")
}

/// A plain-encoded `NUMBER_DATA_POINTS` batch, all rows on one `parent`
/// metric, with the given decoded `ids`.
fn plain_number_dp_batch(ids: &[u32], parent: u16, values: &[f64], times: &[i64]) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt32, Some("plain")),
        id_field("parent_id", DataType::UInt16, Some("plain")),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("double_value", DataType::Float64, false),
    ]);
    let n = ids.len();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(ids.to_vec())),
            Arc::new(UInt16Array::from(vec![parent; n])),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .expect("build plain number dp batch")
}

/// A `*Attrs` batch of string attributes whose `parent_id` column carries the
/// still-encoded values; `encoding` controls its field-metadata declaration
/// (absent = the QUASI-DELTA default). Equality columns for QUASI-DELTA are
/// `type`, `key`, and (here always the `str`) Active Field.
fn quasi_delta_str_attrs_batch(
    encoded_parents: &[u32],
    keys: &[&str],
    strs: &[&str],
    encoding: Option<&str>,
) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("parent_id", DataType::UInt32, encoding),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, false),
        Field::new("str", DataType::Utf8, true),
    ]);
    let types: Vec<u8> = vec![ANY_VALUE_TYPE_STRING; keys.len()];
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(encoded_parents.to_vec())),
            Arc::new(StringArray::from(keys.to_vec())),
            Arc::new(UInt8Array::from(types)),
            Arc::new(StringArray::from(
                strs.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build quasi-delta attrs batch")
}

fn u64_list(rows: &[&[u64]]) -> ListArray {
    let item = Arc::new(Field::new("item", DataType::UInt64, true));
    let offsets = OffsetBuffer::from_lengths(rows.iter().map(|r| r.len()));
    let values: Vec<u64> = rows.iter().flat_map(|r| r.iter().copied()).collect();
    ListArray::try_new(item, offsets, Arc::new(UInt64Array::from(values)), None)
        .expect("build u64 list array")
}

fn f64_list(rows: &[&[f64]]) -> ListArray {
    let item = Arc::new(Field::new("item", DataType::Float64, true));
    let offsets = OffsetBuffer::from_lengths(rows.iter().map(|r| r.len()));
    let values: Vec<f64> = rows.iter().flat_map(|r| r.iter().copied()).collect();
    ListArray::try_new(item, offsets, Arc::new(Float64Array::from(values)), None)
        .expect("build f64 list array")
}

/// A plain-encoded `HISTOGRAM_DATA_POINTS` batch, all rows on one `parent`
/// metric, with a single bucket (`explicit_bounds` empty) per row so each
/// point admits and its exemplar count is observable.
fn histogram_dp_batch(ids: &[u32], parent: u16, counts: &[u64], times: &[i64]) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("id", DataType::UInt32, Some("plain")),
        id_field("parent_id", DataType::UInt16, Some("plain")),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new(
            "explicit_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
    ]);
    let n = ids.len();
    let bucket_rows: Vec<&[u64]> = counts.iter().map(std::slice::from_ref).collect();
    let empty: [f64; 0] = [];
    let bound_rows: Vec<&[f64]> = vec![&empty[..]; n];
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(ids.to_vec())),
            Arc::new(UInt16Array::from(vec![parent; n])),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(UInt64Array::from(counts.to_vec())),
            Arc::new(u64_list(&bucket_rows)),
            Arc::new(f64_list(&bound_rows)),
        ],
    )
    .expect("build histogram dp batch")
}

/// A `*DpExemplars` batch whose `parent_id` column carries the still-encoded
/// values; `encoding` controls its field-metadata declaration (absent = the
/// QUASI-DELTA default). Equality columns for QUASI-DELTA are `int_value` and
/// `double_value`; this helper populates only `double_value`.
fn quasi_delta_exemplars_batch(
    encoded_parents: &[u32],
    doubles: &[Option<f64>],
    encoding: Option<&str>,
) -> RecordBatch {
    let schema = Schema::new(vec![
        id_field("parent_id", DataType::UInt32, encoding),
        Field::new("int_value", DataType::Int64, true),
        Field::new("double_value", DataType::Float64, true),
    ]);
    let n = encoded_parents.len();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(encoded_parents.to_vec())),
            Arc::new(Int64Array::from(vec![None::<i64>; n])),
            Arc::new(Float64Array::from(doubles.to_vec())),
        ],
    )
    .expect("build quasi-delta exemplars batch")
}

fn region_of(point: &ravel_otlp::NormalizedPoint) -> Option<String> {
    point
        .labels
        .iter()
        .find(|l| l.name == "region")
        .map(|l| l.value.clone())
}

/// A `NUMBER_DP_ATTRS.parent_id` column defaults to QUASI-DELTA (otap-spec.md
/// section 6.4.3). Encoded `[0, 1, 3, 1]` with a run of matching `region`
/// values that breaks when the value changes decodes to `[0, 1, 3, 4]`: the
/// first row is absolute, the second deltas onto it (`0 + 1`), the third
/// resets to absolute because `str` changed (`us` -> `eu`), and the fourth
/// deltas again (`3 + 1`). Read literally the column would be `[0, 1, 3, 1]`,
/// which would give data point id 1 two `region` labels (a duplicate-label
/// rejection) and strand data point id 4 without one.
#[test]
fn attrs_quasi_delta_parent_id_decodes_to_literal_values() {
    let root = single_gauge_root("widgets");
    let ids = [0u32, 1, 3, 4];
    let values = [10.0, 20.0, 30.0, 40.0];
    let times = [
        INGEST_TS_NS,
        INGEST_TS_NS + 1,
        INGEST_TS_NS + 2,
        INGEST_TS_NS + 3,
    ];
    let dp = plain_number_dp_batch(&ids, 0, &values, &times);
    let attrs = quasi_delta_str_attrs_batch(
        &[0, 1, 3, 1],
        &["region", "region", "region", "region"],
        &["us", "us", "eu", "eu"],
        Some("quasidelta"),
    );

    let out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::NumberDataPoints, dp),
        (ArrowPayloadType::NumberDpAttrs, attrs),
    ]);
    assert_eq!(
        out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        out.rejected
    );
    assert_eq!(out.points.len(), 4);

    let mut by_value: BTreeMap<u64, Option<String>> = BTreeMap::new();
    for p in &out.points {
        by_value.insert(p.sample.value.to_bits(), region_of(p));
    }
    assert_eq!(by_value[&10.0f64.to_bits()].as_deref(), Some("us"));
    assert_eq!(by_value[&20.0f64.to_bits()].as_deref(), Some("us"));
    assert_eq!(by_value[&30.0f64.to_bits()].as_deref(), Some("eu"));
    assert_eq!(by_value[&40.0f64.to_bits()].as_deref(), Some("eu"));
}

/// A null `type` discriminant in a `*Attrs` batch (validity bit
/// unset) must be treated as out of contract, never read from whatever byte the
/// buffer still holds. An adversarial producer can mark a row's `type` null
/// while leaving a scalar-type byte (here `ANY_VALUE_TYPE_STRING`) in the
/// buffer; the QUASI-DELTA run-continuation decision in `decode_quasi_delta_attrs`
/// must take the documented "no scalar Active Field -> always absolute"
/// fallback for that row instead of matching the previous row and continuing
/// the delta run.
///
/// Three rows, all key=region str=us, the middle one's `type` marked null with
/// a `STRING` byte still in its buffer, encoded QUASI-DELTA parent_ids
/// `[0, 50, 2]`:
/// - With the fix: row 0 absolute -> 0, row 1 (null type) absolute -> 50 and
///   resets `prev`, row 2 absolute -> 2. So the third attr joins data point 2.
/// - Without the fix: the null row is read as a normal `STRING` row that
///   matches the run, so row 1 deltas to `0 + 50 = 50` and row 2 deltas to
///   `50 + 2 = 52`, stranding the third attr on non-existent data point 52 and
///   leaving data point 2 with no `region` label.
///
/// The middle attr's own decoded parent (50) matches no data point, so its
/// out-of-contract cell is never consulted; the observable signal is whether
/// data point 2 carries `region=us`.
#[test]
fn attrs_null_type_discriminant_reads_absolute_not_run_continuation() {
    use arrow::buffer::{NullBuffer, ScalarBuffer};

    let root = single_gauge_root("widgets");
    let dp = plain_number_dp_batch(&[0, 2], 0, &[10.0, 20.0], &[INGEST_TS_NS, INGEST_TS_NS + 1]);

    // The `type` column: buffer holds a scalar `STRING` byte on every row, but
    // row 1's validity bit is unset (null), the adversarial shape this guards.
    let type_values = ScalarBuffer::<u8>::from(vec![
        ANY_VALUE_TYPE_STRING,
        ANY_VALUE_TYPE_STRING,
        ANY_VALUE_TYPE_STRING,
    ]);
    let type_nulls = NullBuffer::from(vec![true, false, true]);
    let types = UInt8Array::new(type_values, Some(type_nulls));

    let schema = Schema::new(vec![
        id_field("parent_id", DataType::UInt32, Some("quasidelta")),
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::UInt8, true),
        Field::new("str", DataType::Utf8, true),
    ]);
    let attrs = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(vec![0u32, 50, 2])),
            Arc::new(StringArray::from(vec!["region", "region", "region"])),
            Arc::new(types),
            Arc::new(StringArray::from(vec![Some("us"), Some("us"), Some("us")])),
        ],
    )
    .expect("build null-type attrs batch");

    let out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::NumberDataPoints, dp),
        (ArrowPayloadType::NumberDpAttrs, attrs),
    ]);

    assert_eq!(
        out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        out.rejected
    );
    assert_eq!(out.points.len(), 2);

    let mut by_value: BTreeMap<u64, Option<String>> = BTreeMap::new();
    for p in &out.points {
        by_value.insert(p.sample.value.to_bits(), region_of(p));
    }
    // Data point id 0 (value 10.0) joins the first attr as before.
    assert_eq!(by_value[&10.0f64.to_bits()].as_deref(), Some("us"));
    // Data point id 2 (value 20.0) only carries `region=us` if the null-type
    // row read absolute (breaking the run) rather than continuing the delta.
    assert_eq!(
        by_value[&20.0f64.to_bits()].as_deref(),
        Some("us"),
        "null-type row must reset the QUASI-DELTA run so the third attr \
         decodes to parent 2, not continue it to parent 52"
    );
}

/// A `HISTOGRAM_DP_EXEMPLARS.parent_id` column defaults to QUASI-DELTA with
/// `int_value`/`double_value` as its equality columns (otap-spec.md section
/// 6.4.3). Encoded `[0, 3, 0, 3, 0]` with `double_value` `[1, 1, 7, 7, 7]`
/// decodes to `[0, 3, 0, 3, 3]`: the third row resets to absolute because the
/// value changed (`1` -> `7`), so two exemplars join histogram data point 0
/// and three join data point 3. Read literally the column would be
/// `[0, 3, 0, 3, 0]`, giving data point 0 three exemplars and data point 3
/// two, so the per-point `HistogramExemplarsDropped` counts observe the
/// decode.
#[test]
fn exemplars_quasi_delta_parent_id_decodes_to_literal_values() {
    let root = single_histogram_root("latency");
    let hist_dp = histogram_dp_batch(&[0, 3], 0, &[5, 5], &[INGEST_TS_NS, INGEST_TS_NS + 1]);
    let exemplars = quasi_delta_exemplars_batch(
        &[0, 3, 0, 3, 0],
        &[Some(1.0), Some(1.0), Some(7.0), Some(7.0), Some(7.0)],
        Some("quasidelta"),
    );

    let out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::HistogramDataPoints, hist_dp),
        (ArrowPayloadType::HistogramDpExemplars, exemplars),
    ]);

    // Each histogram data point (bounds empty) explodes into `_bucket{le=+Inf}`
    // and `_count`, so two points admit -> four points total.
    assert_eq!(out.points.len(), 4);
    // The informational HistogramExemplarsDropped rejections are emitted in
    // data-point row order (id 0 then id 3): 2 exemplars, then 3.
    assert_eq!(
        out.rejected,
        vec![
            Rejection::HistogramExemplarsDropped { count: 2 },
            Rejection::HistogramExemplarsDropped { count: 3 },
        ]
    );
}

/// A single batch mixing all three encodings proves QUASI-DELTA attrs
/// decoding does not regress the DELTA id/parent decoding: the
/// root ids and the data-point id/parent_id ride DELTA while the attrs
/// `parent_id` rides QUASI-DELTA (a matching `region` run that accumulates
/// `[5, 1]` -> `[5, 6]`), and the whole thing must still agree with the OTLP
/// oracle. If the attrs column were read literally, the second point's
/// `region` label would be stranded on non-existent data point id 1.
#[test]
fn quasi_delta_attrs_alongside_delta_ids_in_one_batch() {
    // root id DELTA: encoded [0, 1] -> decoded [0, 1]; "widgets" is metric 0.
    let root = root_batch([0, 1], ["widgets", "spare"], Some("delta"));
    // dp id DELTA: encoded [5, 1] -> [5, 6]; parent_id DELTA [0, 0] -> [0, 0].
    let dp = number_dp_batch([5, 1], [0, 0], [1.0, 2.0], Some("delta"), Some("delta"));
    // attrs parent_id QUASI-DELTA: encoded [5, 1], both region=us (equality
    // columns match) -> decoded [5, 6], joining both data points.
    let attrs = quasi_delta_str_attrs_batch(
        &[5, 1],
        &["region", "region"],
        &["us", "us"],
        Some("quasidelta"),
    );

    let otap_out = decode_custom_batch(vec![
        (ArrowPayloadType::UnivariateMetrics, root),
        (ArrowPayloadType::NumberDataPoints, dp),
        (ArrowPayloadType::NumberDpAttrs, attrs),
    ]);
    assert_eq!(
        otap_out.rejected,
        Vec::new(),
        "unexpected rejections: {:?}",
        otap_out.rejected
    );
    assert_eq!(otap_out.points.len(), 2);

    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let otlp_out = normalize_metrics(
        &tenant,
        gauge_request_with_region(
            "widgets",
            [
                (INGEST_TS_NS, 1.0, "us"),
                (INGEST_TS_NS + 1_000_000, 2.0, "us"),
            ],
        ),
        &limits,
        INGEST_TS_NS,
    );
    assert_eq!(samples(&otlp_out), samples(&otap_out), "samples differ");
    assert_eq!(
        rejection_multiset(&otlp_out.rejected),
        rejection_multiset(&otap_out.rejected),
        "rejection classes differ"
    );
}
