//! Timed loop for `normalize_logs_projected` over 2,000 records in 200
//! `ResourceLogs` (each with `host.name` and `service.name`), 30 string
//! attributes per record (issue #3644). Prints the median and minimum
//! nanoseconds per record over `ITERATIONS` runs. Not run by `cargo test`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Instant;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_ipcrypt::rules::TenantRules;
use ravel_otlp::{LogIngestLimits, normalize_logs_projected, project_log_resolved_label_bytes};

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
const RESOURCES: usize = 200;
const RECORDS_PER_RESOURCE: usize = 10;
const ATTRS: usize = 30;
const ITERATIONS: usize = 40;

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

fn request() -> ExportLogsServiceRequest {
    let built = ExportLogsServiceRequest {
        resource_logs: (0..RESOURCES)
            .map(|h| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        string_kv("host.name".into(), format!("host-{h:04}")),
                        string_kv("service.name".into(), format!("app-{:03}", h % 20)),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope.name".to_string(),
                        version: "1.2.3".to_string(),
                        ..Default::default()
                    }),
                    log_records: (0..RECORDS_PER_RESOURCE)
                        .map(|r| LogRecord {
                            time_unix_nano: INGEST_TS_NS as u64,
                            observed_time_unix_nano: INGEST_TS_NS as u64,
                            severity_number: 9,
                            severity_text: "INFORMATION".to_string(),
                            body: Some(AnyValue {
                                value: Some(AnyValueVariant::StringValue(format!(
                                    "log line {h:04} {r:04}"
                                ))),
                            }),
                            attributes: (0..ATTRS)
                                .map(|a| {
                                    string_kv(
                                        format!("attribute.key.{a:03}"),
                                        format!("attribute-value-{h:04}-{r:04}-{a:03}"),
                                    )
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
    };
    ExportLogsServiceRequest::decode(built.encode_to_vec().as_slice()).expect("round trip")
}

fn main() {
    let limits = LogIngestLimits::default();
    let rules = TenantRules::none();
    let records = RESOURCES * RECORDS_PER_RESOURCE;
    let mut ns_per_record = Vec::new();
    for _ in 0..ITERATIONS {
        let req = request();
        let projected = project_log_resolved_label_bytes(&req, &limits, &rules);
        let start = Instant::now();
        let out = normalize_logs_projected(req, &limits, &rules, INGEST_TS_NS, projected);
        let elapsed = start.elapsed();
        assert_eq!(out.records.len(), records, "rejected: {:?}", out.rejected);
        drop(out);
        ns_per_record.push(elapsed.as_nanos() as f64 / records as f64);
    }
    ns_per_record.sort_by(f64::total_cmp);
    println!(
        "median {:.1} ns/record, min {:.1} ns/record ({ITERATIONS} runs, {records} records)",
        ns_per_record[ITERATIONS / 2],
        ns_per_record[0]
    );
}
