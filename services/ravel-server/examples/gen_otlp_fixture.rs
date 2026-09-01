//! Emits one `ExportMetricsServiceRequest` protobuf message to stdout, with
//! `time_unix_nano` set to the current wall clock.
//!
//! OTLP ingest rejects points more than `max_ingest_lag_ns` (2 hours) old, so
//! a fixture checked into git with a fixed timestamp would eventually go
//! stale; `scripts/demo.sh` runs this example to regenerate the bytes fresh
//! on every invocation instead.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn service_name_attribute(job: &str) -> KeyValue {
    KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(job.to_string())),
        }),
        ..Default::default()
    }
}

fn main() -> anyhow::Result<()> {
    let ts_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let ts_ns = u64::try_from(ts_ns)?;

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![service_name_attribute("demo")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "demo_requests_total".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns,
                            value: Some(NumberValue::AsDouble(7.0)),
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

    std::io::stdout().write_all(&request.encode_to_vec())?;
    Ok(())
}
