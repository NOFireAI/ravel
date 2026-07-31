//! ADR-0038 cross-path gate: a logically identical series ingested via OTLP,
//! OTAP, and remote-write must yield one `SeriesId`, even when one wire form
//! carries a label with an empty value and another omits the label entirely.
//!
//! Before ADR-0038, OTLP and OTAP admitted an empty-valued label into series
//! identity while remote-write dropped it (Prometheus convention), so the same
//! logical series split into two ids depending on ingest path. This test pins
//! that they now converge, and that the converged id is the empty-label-dropped
//! id (not the id that includes an empty-valued `region`).
#![allow(clippy::expect_used)]

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data as MetricData,
};

use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
};
use ravel_otap::normalize::normalize_decoded;
use ravel_otap::stream::{StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_remote_write::{ResolvedRequest, ResolvedSample, ResolvedSeries, normalize_resolved};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, SeriesId, TenantId};

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
const INGEST_TS_MS: i64 = INGEST_TS_NS / 1_000_000;
const TENANT: &str = "t";
const METRIC: &str = "up";

/// OTLP: one gauge `up`, one point, with `attrs` as data-point attributes.
fn otlp_series_id(attrs: Vec<KeyValue>) -> SeriesId {
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: METRIC.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: attrs,
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
    let out = normalize_metrics(
        &TenantId::new(TENANT),
        req,
        &IngestLimits::default(),
        INGEST_TS_NS,
    );
    assert!(out.rejected.is_empty(), "otlp rejected: {:?}", out.rejected);
    assert_eq!(out.points.len(), 1, "otlp expected one point");
    out.points[0].series_id
}

/// OTAP: same gauge `up` and single point through encode -> decode ->
/// normalize.
fn otap_series_id(attrs: Vec<AttrRow>) -> SeriesId {
    let metrics = vec![MetricRow {
        name: METRIC.to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: INGEST_TS_NS,
            value: 1.0,
            flags: 0,
            attrs,
        }],
    }];
    let mut encoder = MetricsStreamEncoder::new("cross-path").expect("new encoder");
    let raw = encoder.encode_batch(0, &metrics).expect("encode");
    let mut state = StreamState::new(StreamConfig::default());
    let decoded = state.decode(raw).expect("decode");
    let out = normalize_decoded(
        &TenantId::new(TENANT),
        &decoded,
        &IngestLimits::default(),
        INGEST_TS_NS,
    );
    assert!(out.rejected.is_empty(), "otap rejected: {:?}", out.rejected);
    assert_eq!(out.points.len(), 1, "otap expected one point");
    out.points[0].series_id
}

/// remote-write: `__name__=up` plus `labels`, one sample, through the resolved
/// normalizer (the reference behavior this ADR aligns OTLP/OTAP onto).
fn rw_series_id(mut labels: Vec<Label>) -> SeriesId {
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    });
    let req = ResolvedRequest {
        series: vec![ResolvedSeries {
            labels,
            samples: vec![ResolvedSample {
                ts_ms: INGEST_TS_MS,
                value: 1.0,
            }],
            histograms: vec![],
            exemplar_count: 0,
        }],
        metadata_count: 0,
        created_timestamps_count: 0,
    };
    let out = normalize_resolved(
        &TenantId::new(TENANT),
        req,
        &IngestLimits::default(),
        INGEST_TS_NS,
    );
    assert!(out.rejected.is_empty(), "rw rejected: {:?}", out.rejected);
    assert_eq!(out.points.len(), 1, "rw expected one point");
    out.points[0].series_id
}

fn otlp_kv_empty(key: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(String::new())),
        }),
        ..Default::default()
    }
}

#[test]
fn empty_valued_label_yields_one_series_id_across_all_paths() {
    // The reference: the id every path should converge on is the one with no
    // `region` label at all, computed straight from ravel-types.
    let expected = SeriesId::compute(
        &TenantId::new(TENANT),
        METRIC,
        &LabelSet::new(vec![]).expect("empty label set"),
    )
    .expect("compute reference id");

    // The pre-ADR-0038 id: `region=""` admitted into identity. All three paths
    // must now differ from this, proving the empty label was dropped.
    let with_empty = SeriesId::compute(
        &TenantId::new(TENANT),
        METRIC,
        &LabelSet::new(vec![Label {
            name: "region".to_string(),
            value: String::new(),
        }])
        .expect("region label set"),
    )
    .expect("compute region-included id");
    assert_ne!(
        expected, with_empty,
        "fixture precondition: the two ids differ"
    );

    // OTLP with an explicit empty-string attribute, and with the attribute
    // absent entirely: both drop to the reference id.
    assert_eq!(otlp_series_id(vec![otlp_kv_empty("region")]), expected);
    assert_eq!(otlp_series_id(vec![]), expected);

    // OTAP with an empty-string cell, and with no attributes: both drop too.
    assert_eq!(
        otap_series_id(vec![AttrRow {
            key: "region".to_string(),
            value: AttrValue::Str(String::new()),
        }]),
        expected
    );
    assert_eq!(otap_series_id(vec![]), expected);

    // remote-write with `region=""` (its existing drop) and with `region`
    // absent: the reference behavior, still at the reference id.
    assert_eq!(
        rw_series_id(vec![Label {
            name: "region".to_string(),
            value: String::new(),
        }]),
        expected
    );
    assert_eq!(rw_series_id(vec![]), expected);
}
