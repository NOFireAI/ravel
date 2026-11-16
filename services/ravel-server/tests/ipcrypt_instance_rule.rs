//! A rule on `service.instance.id` and a rule on `instance` cover different
//! things: OTLP metrics store that resource attribute as the `instance` label
//! and match rules by stored label name, while logs and traces match the key
//! as sent. An operator who wants the address out of both writes both rules,
//! as docs/ingest.md says; these cases pin each half of that sentence through
//! the policy the server loads.
#![allow(clippy::expect_used)]

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_otlp::{IngestLimits, LogIngestLimits};
use ravel_server::ipcrypt::IpcryptPolicy;
use ravel_types::TenantId;

const ADDR: &str = "198.51.100.73";
const TS_NS: i64 = 1_700_000_000_000_000_000;

fn rules_on(key: &str) -> std::sync::Arc<ravel_ipcrypt::rules::TenantRules> {
    let text = format!("[[defaults.rules]]\nkey = \"{key}\"\nvariant = \"deterministic\"\n");
    let file =
        ravel_ipcrypt::policy::parse_policy_file(&text, ravel_server::ipcrypt::sanitize_rule_key)
            .expect("policy parses");
    IpcryptPolicy::new(file, Some(&[0x5a; 32]))
        .expect("policy loads")
        .rules_for(&TenantId::new("acme"))
        .expect("rules resolve")
}

fn resource() -> Option<Resource> {
    Some(Resource {
        attributes: vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue("checkout".to_string())),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "service.instance.id".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(ADDR.to_string())),
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    })
}

fn stored_instance_label(key: &str) -> String {
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: resource(),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "up".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: TS_NS as u64,
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
    let out = ravel_otlp::normalize::normalize_metrics(
        &TenantId::new("acme"),
        req,
        &IngestLimits::default(),
        &rules_on(key),
        TS_NS,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(out.points.len(), 1);
    out.points[0]
        .labels
        .get("instance")
        .expect("service.instance.id is stored as the instance label")
        .to_string()
}

fn log_stream_holds_the_address(key: &str) -> bool {
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: resource(),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: TS_NS as u64,
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("served".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let out = ravel_otlp::logs_normalize::normalize_logs(
        req,
        &LogIngestLimits::default(),
        &rules_on(key),
        TS_NS,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(out.records.len(), 1);
    out.records[0]
        .stream_attrs
        .windows(ADDR.len())
        .any(|w| w == ADDR.as_bytes())
}

#[test]
fn a_rule_on_service_instance_id_covers_logs_but_not_the_metric_instance_label() {
    assert_eq!(stored_instance_label("service.instance.id"), ADDR);
    assert!(!log_stream_holds_the_address("service.instance.id"));
}

#[test]
fn a_rule_on_instance_covers_the_metric_label_but_not_logs() {
    assert_ne!(stored_instance_label("instance"), ADDR);
    assert!(log_stream_holds_the_address("instance"));
}
