//! Regression tests for finding a9-F01 (issue #73, QF-a9-F01) from
//! docs/reviews/2026-07-27-storage-engine-quality-audit/a9-otap-arrow.md.
//!
//! ADR-0011 and docs/otap-ingest.md require that the OTLP protobuf path and
//! the OTAP columnar path reject logically identical input with identical
//! rejection classes. Two documented mechanisms made the OTAP normalizer
//! diverge from the OTLP oracle:
//!
//!   * mechanism a: `build_metric_decisions` checked Sum temporality before
//!     the metric name, so a delta Sum with an empty name was classed as
//!     `UnsupportedTemporality` instead of the oracle's `EmptyMetricName`.
//!   * mechanism b: the columnar normalizer sorted a point's attributes by
//!     key before the per-label length checks, so a point carrying an
//!     over-long value (first in input order) and an over-long name (first in
//!     sorted order) was classed as `LabelNameTooLong` instead of the
//!     oracle's `LabelValueTooLong`.
//!
//! These tests were `#[ignore]`d reproducers on branch audit/repro-a9; the
//! fix in src/normalize.rs aligns the check order, so they are now enabled
//! regression tests. (The a9-F02 dictionary-budget test is tracked separately
//! as issue #74 and is not included here.)
#![allow(clippy::expect_used, clippy::unwrap_used)]

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    metric::Data as MetricData,
};

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::normalize::{AGGREGATION_TEMPORALITY_DELTA, normalize_decoded};
use ravel_otap::stream::{StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, NormalizeOutput, normalize_metrics};
use ravel_types::TenantId;

const TS: i64 = 1_700_000_000_000_000_000;

fn otap_reject(metrics: Vec<MetricRow>) -> NormalizeOutput {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    let mut enc = MetricsStreamEncoder::new("a9").expect("encoder");
    let batch = enc.encode_batch(0, &metrics).expect("encode");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(batch).expect("decode");
    normalize_decoded(&tenant, &decoded, &limits, TS)
}

fn otlp_reject(req: ExportMetricsServiceRequest) -> NormalizeOutput {
    let tenant = TenantId::new("acme");
    let limits = IngestLimits::default();
    normalize_metrics(&tenant, req, &limits, TS)
}

/// a9-F01 mechanism a: a delta Sum with an empty metric name. OTLP checks the
/// name before temporality (EmptyMetricName); before the fix OTAP checked
/// temporality first (UnsupportedTemporality). Both paths must now class it
/// as EmptyMetricName.
#[test]
fn a9_f01_delta_sum_empty_name_rejection_classes_agree() {
    let otlp = otlp_reject(ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: String::new(),
                    data: Some(MetricData::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: TS as u64,
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
    });
    let otap = otap_reject(vec![MetricRow {
        name: String::new(),
        kind: MetricKind::Sum {
            temporality: AGGREGATION_TEMPORALITY_DELTA,
            is_monotonic: true,
        },
        data_points: vec![DataPointRow {
            time_unix_nano: TS,
            value: 1.0,
            flags: 0,
            attrs: vec![],
        }],
    }]);
    assert!(otlp.points.is_empty() && otap.points.is_empty());
    // Contract (ADR-0011): identical rejection classes.
    assert_eq!(
        format!("{:?}", otlp.rejected),
        format!("{:?}", otap.rejected),
        "rejection classes must match across ingest paths"
    );
}

/// a9-F01 mechanism b: a point with two length-violating attributes whose
/// input order differs from key-sorted order. OTLP checks attributes in input
/// order (first violation wins: LabelValueTooLong); before the fix OTAP sorted
/// the attribute set by key first, so a different attribute was checked first
/// (LabelNameTooLong). Both paths must now class it as LabelValueTooLong.
#[test]
fn a9_f01_attr_order_rejection_classes_agree() {
    let long_val = "v".repeat(4097); // > max_label_value_len (4096)
    let long_name = "a".repeat(257); // > max_label_name_len (256)
    let otlp = otlp_reject(ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "m".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![
                                KeyValue {
                                    key: "zzz".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(AnyValueVariant::StringValue(long_val.clone())),
                                    }),
                                    ..Default::default()
                                },
                                KeyValue {
                                    key: long_name.clone(),
                                    value: Some(AnyValue {
                                        value: Some(AnyValueVariant::StringValue("v".to_string())),
                                    }),
                                    ..Default::default()
                                },
                            ],
                            time_unix_nano: TS as u64,
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
    });
    let otap = otap_reject(vec![MetricRow {
        name: "m".to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: TS,
            value: 1.0,
            flags: 0,
            attrs: vec![
                AttrRow {
                    key: "zzz".to_string(),
                    value: AttrValue::Str(long_val.clone()),
                },
                AttrRow {
                    key: long_name.clone(),
                    value: AttrValue::Str("v".to_string()),
                },
            ],
        }],
    }]);
    assert!(otlp.points.is_empty() && otap.points.is_empty());
    // Contract (ADR-0011): identical rejection classes.
    assert_eq!(
        format!("{:?}", otlp.rejected),
        format!("{:?}", otap.rejected),
        "rejection classes must match across ingest paths"
    );
}
