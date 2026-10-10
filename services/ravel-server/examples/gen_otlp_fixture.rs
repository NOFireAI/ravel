//! Emits one `ExportMetricsServiceRequest` protobuf message to stdout, with
//! `time_unix_nano` set to the current wall clock.
//!
//! OTLP ingest rejects points more than `max_ingest_lag_ns` (2 hours) old, so
//! a fixture checked into git with a fixed timestamp would eventually go
//! stale; `scripts/demo.sh` runs this example to regenerate the bytes fresh
//! on every invocation instead.
//!
//! Usage: `gen_otlp_fixture [SERIES POINTS]`.
//!
//! With no arguments the message carries one `demo_requests_total` gauge
//! point and no point attributes. With `SERIES POINTS` it carries `SERIES`
//! series of `demo_requests_total`, told apart by a `series` point attribute,
//! each with `POINTS` points. The newest point of every series is at the
//! current wall clock and the oldest at most `MAX_SPREAD_NS` before it, so
//! every point stays far inside the ingest lag limit.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

/// Spacing between consecutive points of one series, before the spread cap.
const POINT_STEP_NS: u64 = 1_000_000_000;
/// Widest span between a series' oldest and newest point: two minutes.
const MAX_SPREAD_NS: u64 = 120_000_000_000;

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn service_name_attribute(job: &str) -> KeyValue {
    string_attribute("service.name", job)
}

/// Series and points-per-series counts; `None` is the single-point default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Shape {
    series: u64,
    points: u64,
}

fn parse_args(args: &[String]) -> anyhow::Result<Option<Shape>> {
    match args {
        [] => Ok(None),
        [series, points] => {
            let series: u64 = series
                .parse()
                .with_context(|| format!("SERIES must be a positive integer, got {series:?}"))?;
            let points: u64 = points
                .parse()
                .with_context(|| format!("POINTS must be a positive integer, got {points:?}"))?;
            if series == 0 || points == 0 {
                bail!("SERIES and POINTS must both be at least 1");
            }
            Ok(Some(Shape { series, points }))
        }
        _ => bail!("usage: gen_otlp_fixture [SERIES POINTS]"),
    }
}

fn data_points(ts_ns: u64, shape: Option<Shape>) -> Vec<NumberDataPoint> {
    let Some(Shape { series, points }) = shape else {
        return vec![NumberDataPoint {
            time_unix_nano: ts_ns,
            value: Some(NumberValue::AsDouble(7.0)),
            ..Default::default()
        }];
    };
    let step_ns = if points > 1 {
        POINT_STEP_NS.min(MAX_SPREAD_NS / (points - 1))
    } else {
        0
    };
    let mut out = Vec::new();
    for s in 0..series {
        let attribute = string_attribute("series", &format!("s{s}"));
        for p in 0..points {
            let age_ns = (points - 1 - p) * step_ns;
            out.push(NumberDataPoint {
                attributes: vec![attribute.clone()],
                time_unix_nano: ts_ns.saturating_sub(age_ns),
                value: Some(NumberValue::AsDouble(p as f64)),
                ..Default::default()
            });
        }
    }
    out
}

fn build_request(ts_ns: u64, shape: Option<Shape>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![service_name_attribute("demo")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "demo_requests_total".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: data_points(ts_ns, shape),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let shape = parse_args(&args)?;

    let ts_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let ts_ns = u64::try_from(ts_ns)?;

    let request = build_request(ts_ns, shape);
    std::io::stdout().write_all(&request.encode_to_vec())?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const TS_NS: u64 = 1_760_000_000_000_000_000;

    fn decoded_points(bytes: &[u8]) -> Vec<NumberDataPoint> {
        let request = ExportMetricsServiceRequest::decode(bytes).expect("decode");
        let mut out = Vec::new();
        for rm in request.resource_metrics {
            for sm in rm.scope_metrics {
                for m in sm.metrics {
                    assert_eq!(m.name, "demo_requests_total");
                    match m.data {
                        Some(MetricData::Gauge(g)) => out.extend(g.data_points),
                        other => panic!("expected a gauge, got {other:?}"),
                    }
                }
            }
        }
        out
    }

    #[test]
    fn default_is_one_point_without_attributes() {
        let points = decoded_points(&build_request(TS_NS, None).encode_to_vec());
        assert_eq!(points.len(), 1);
        assert!(points[0].attributes.is_empty());
        assert_eq!(points[0].time_unix_nano, TS_NS);
        assert_eq!(points[0].value, Some(NumberValue::AsDouble(7.0)));
    }

    #[test]
    fn shaped_request_carries_series_times_points() {
        let shape = Shape {
            series: 7,
            points: 300,
        };
        let points = decoded_points(&build_request(TS_NS, Some(shape)).encode_to_vec());
        assert_eq!(points.len(), 7 * 300);
        let mut per_series: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for p in &points {
            assert_eq!(p.attributes.len(), 1);
            assert_eq!(p.attributes[0].key, "series");
            let Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(v)),
            }) = &p.attributes[0].value
            else {
                panic!("series attribute is not a string");
            };
            per_series
                .entry(v.clone())
                .or_default()
                .push(p.time_unix_nano);
        }
        assert_eq!(per_series.len(), 7);
        for times in per_series.values() {
            assert_eq!(times.len(), 300);
            let mut distinct = times.clone();
            distinct.dedup();
            assert_eq!(
                distinct.len(),
                300,
                "timestamps within a series are distinct"
            );
            assert_eq!(times.iter().max().copied(), Some(TS_NS));
            let oldest = times.iter().min().copied().expect("non-empty");
            assert!(TS_NS - oldest <= MAX_SPREAD_NS);
        }
    }

    #[test]
    fn args_parse() {
        let s = |v: &[&str]| v.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        assert_eq!(parse_args(&s(&[])).expect("empty"), None);
        assert_eq!(
            parse_args(&s(&["3", "4"])).expect("two"),
            Some(Shape {
                series: 3,
                points: 4
            })
        );
        assert!(parse_args(&s(&["3"])).is_err());
        assert!(parse_args(&s(&["0", "4"])).is_err());
        assert!(parse_args(&s(&["x", "4"])).is_err());
    }
}
