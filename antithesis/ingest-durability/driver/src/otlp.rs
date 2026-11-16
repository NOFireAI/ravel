//! OTLP request bodies for the workload's writes. Every record carries the
//! write id and its index inside the write, so a query result attributes each
//! stored row to exactly one record the workload sent.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

/// Log record attribute holding the write id.
pub const LOG_WRITE_ATTR: &str = "wl.write";
/// Log record attribute holding the record's index inside its write.
pub const LOG_REC_ATTR: &str = "wl.rec";
/// The gauge every metrics write appends to.
pub const METRIC_NAME: &str = "wl_gauge";
/// Data point attribute naming the sample's series; becomes a label.
pub const METRIC_SERIES_LABEL: &str = "wl_series";
/// Number of distinct metric series the workload writes.
pub const METRIC_SERIES: u64 = 8;
/// A sample's value is `write_id * VALUE_STRIDE + index`.
pub const VALUE_STRIDE: u64 = 100;

fn kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
        ..Default::default()
    }
}

fn resource(service: String) -> Option<Resource> {
    Some(Resource {
        attributes: vec![kv("service.name", service)],
        ..Default::default()
    })
}

/// One log export: `records` records of write `write_id`, spread over
/// `streams` services so a request can fan out to several shards.
pub fn logs_request(write_id: u64, records: u32, streams: u32, ts_ns: u64) -> Vec<u8> {
    let streams = streams.max(1);
    let mut resource_logs = Vec::new();
    for stream in 0..streams {
        let log_records: Vec<LogRecord> = (0..records)
            .filter(|rec| rec % streams == stream)
            .map(|rec| LogRecord {
                time_unix_nano: ts_ns,
                observed_time_unix_nano: ts_ns,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!(
                        "wl write={write_id} rec={rec}"
                    ))),
                }),
                attributes: vec![
                    kv(LOG_WRITE_ATTR, write_id.to_string()),
                    kv(LOG_REC_ATTR, rec.to_string()),
                ],
                ..Default::default()
            })
            .collect();
        if log_records.is_empty() {
            continue;
        }
        resource_logs.push(ResourceLogs {
            resource: resource(format!("wl-svc-{stream}")),
            scope_logs: vec![ScopeLogs {
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    ExportLogsServiceRequest { resource_logs }.encode_to_vec()
}

/// One sample of a metrics write: its series index, timestamp and value.
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    pub series: u64,
    pub ts_ns: u64,
    pub value: f64,
}

/// One metrics export carrying `samples` as gauge points of `wl_gauge`.
pub fn metrics_request(samples: &[Sample]) -> Vec<u8> {
    let data_points = samples
        .iter()
        .map(|s| NumberDataPoint {
            attributes: vec![kv(METRIC_SERIES_LABEL, format!("s{}", s.series))],
            time_unix_nano: s.ts_ns,
            value: Some(number_data_point::Value::AsDouble(s.value)),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: resource("wl-metrics".to_string()),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: METRIC_NAME.to_string(),
                    data: Some(metric::Data::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// What an OTLP export response body says about partial success.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialSuccess {
    /// No partial-success message: every record was accepted.
    None,
    /// A partial-success message with this rejected count. A count of 0 is
    /// still a partial success (a zero-count rejection), so it is not clean.
    Rejected(i64),
    /// The body did not decode as the signal's export response.
    Undecodable,
}

pub fn partial_success(signal_logs: bool, body: &[u8]) -> PartialSuccess {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
    let rejected = if signal_logs {
        ExportLogsServiceResponse::decode(body)
            .map(|r| r.partial_success.map(|p| p.rejected_log_records))
    } else {
        ExportMetricsServiceResponse::decode(body)
            .map(|r| r.partial_success.map(|p| p.rejected_data_points))
    };
    match rejected {
        Ok(None) => PartialSuccess::None,
        Ok(Some(n)) => PartialSuccess::Rejected(n),
        Err(_) => PartialSuccess::Undecodable,
    }
}
