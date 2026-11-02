//! Tests for the vendored, bytes-retyped OTLP logs protos (ADR-3492 D3b,
//! `proto/README.md`).
//!
//! - `retype_pin`: the vendored `.proto` text itself differs from the
//!   upstream copy in exactly the six named fields, nowhere else.
//! - `equivalence`: for arbitrary valid requests, decoding the same wire
//!   bytes with the vendored types and validating them produces the same
//!   field values as decoding with upstream's `String`-typed types.
//! - `refusal_*`: invalid UTF-8 in each retyped field is refused by
//!   upstream's decode and by the vendored decode plus [`validate_utf8`],
//!   alike.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::path::Path;

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as UpstreamRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as UpstreamAnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue as UpstreamAnyValue, InstrumentationScope as UpstreamScope,
    KeyValue as UpstreamKeyValue,
};
use opentelemetry_proto::tonic::logs::v1::{
    LogRecord as UpstreamLogRecord, ResourceLogs as UpstreamResourceLogs,
    ScopeLogs as UpstreamScopeLogs,
};
use opentelemetry_proto::tonic::resource::v1::Resource as UpstreamResource;
use proptest::prelude::*;
use prost::Message;
use ravel_otlp::vendored_logs::any_value::Value as VendoredAnyValueVariant;
use ravel_otlp::vendored_logs::{
    self, AnyValue as VendoredAnyValue, ExportLogsServiceRequest as VendoredRequest,
    InstrumentationScope as VendoredScope, KeyValue as VendoredKeyValue,
    LogRecord as VendoredLogRecord, Resource as VendoredResource,
    ResourceLogs as VendoredResourceLogs, ScopeLogs as VendoredScopeLogs,
};

// ---------------------------------------------------------------------
// retype_pin
// ---------------------------------------------------------------------

const PROTO_ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// One `(file, expected retyped field names)` pair per vendored file. A
/// field name must appear verbatim as `bytes <name> = <n>;` in `retyped/`
/// where `upstream/` has `string <name> = <n>;`, and every other line of the
/// file must be byte-identical between the two copies.
const RETYPE_CASES: &[(&str, &[&str])] = &[
    (
        "opentelemetry/proto/collector/logs/v1/logs_service.proto",
        &[],
    ),
    (
        "opentelemetry/proto/logs/v1/logs.proto",
        &["severity_text", "event_name"],
    ),
    (
        "opentelemetry/proto/common/v1/common.proto",
        &["string_value", "key", "name", "version"],
    ),
    ("opentelemetry/proto/resource/v1/resource.proto", &[]),
];

/// Asserts that `retyped` differs from `upstream` only by changing `string`
/// to `bytes` on exactly the lines declaring `expected_fields`, with every
/// other line (including indentation) identical. A mutation that retypes an
/// extra field, or leaves one of `expected_fields` untouched, fails this.
fn assert_retype_only(upstream_path: &Path, retyped_path: &Path, expected_fields: &[&str]) {
    let upstream = std::fs::read_to_string(upstream_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", upstream_path.display()));
    let retyped = std::fs::read_to_string(retyped_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", retyped_path.display()));

    let upstream_lines: Vec<&str> = upstream.lines().collect();
    let retyped_lines: Vec<&str> = retyped.lines().collect();
    assert_eq!(
        upstream_lines.len(),
        retyped_lines.len(),
        "{} and {} have different line counts",
        upstream_path.display(),
        retyped_path.display()
    );

    let mut remaining: HashSet<&str> = expected_fields.iter().copied().collect();
    for (lineno, (u, r)) in upstream_lines.iter().zip(retyped_lines.iter()).enumerate() {
        if u == r {
            continue;
        }
        let u_trim = u.trim_start();
        let r_trim = r.trim_start();
        let u_indent = u.len() - u_trim.len();
        let r_indent = r.len() - r_trim.len();
        assert_eq!(
            u_indent,
            r_indent,
            "{}:{}: indentation changed: upstream={u:?} retyped={r:?}",
            retyped_path.display(),
            lineno + 1
        );
        let Some(rest_u) = u_trim.strip_prefix("string ") else {
            panic!(
                "{}:{}: unexpected diff (not a string->bytes retype): upstream={u:?} retyped={r:?}",
                retyped_path.display(),
                lineno + 1
            );
        };
        let Some(rest_r) = r_trim.strip_prefix("bytes ") else {
            panic!(
                "{}:{}: unexpected diff (not a string->bytes retype): upstream={u:?} retyped={r:?}",
                retyped_path.display(),
                lineno + 1
            );
        };
        assert_eq!(
            rest_u,
            rest_r,
            "{}:{}: retype changed more than the type keyword: upstream={u:?} retyped={r:?}",
            retyped_path.display(),
            lineno + 1
        );
        let field_name = rest_u
            .split_whitespace()
            .next()
            .expect("field declaration has a name");
        assert!(
            remaining.remove(field_name),
            "{}:{}: retyped field {field_name:?} is not in the expected set {expected_fields:?} \
             (already seen, or genuinely unexpected)",
            retyped_path.display(),
            lineno + 1
        );
    }
    assert!(
        remaining.is_empty(),
        "{}: expected fields never retyped: {remaining:?}",
        retyped_path.display()
    );
}

#[test]
fn retype_pin() {
    for (relative, expected_fields) in RETYPE_CASES {
        let upstream_path = Path::new(PROTO_ROOT).join("proto/upstream").join(relative);
        let retyped_path = Path::new(PROTO_ROOT).join("proto/retyped").join(relative);
        assert_retype_only(&upstream_path, &retyped_path, expected_fields);
    }
}

// ---------------------------------------------------------------------
// equivalence
// ---------------------------------------------------------------------

fn bytes_str(b: &Bytes) -> &str {
    std::str::from_utf8(b).expect("validate_utf8 already accepted this request")
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => Just(String::new()),
        2 => Just("plain ascii text".to_string()),
        2 => Just("h\u{e9}llo w\u{f6}rld".to_string()),
        2 => Just("\u{65e5}\u{672c}\u{8a9e}\u{306e}\u{30ed}\u{30b0}".to_string()),
        1 => Just("emoji \u{1f525}\u{1f600}".to_string()),
        4 => any::<String>(),
    ]
}

/// Biased toward a small pool so attribute keys repeat across a record and
/// across resources/scopes, as the OTLP data model allows.
fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => Just("dup.key".to_string()),
        1 => Just(String::new()),
        1 => Just("h\u{e9}y".to_string()),
        3 => any::<String>(),
    ]
}

fn leaf() -> impl Strategy<Value = Option<UpstreamAnyValueVariant>> {
    prop_oneof![
        1 => Just(None),
        6 => text().prop_map(|s| Some(UpstreamAnyValueVariant::StringValue(s))),
        1 => any::<bool>().prop_map(|b| Some(UpstreamAnyValueVariant::BoolValue(b))),
        1 => any::<i64>().prop_map(|i| Some(UpstreamAnyValueVariant::IntValue(i))),
        1 => prop_oneof![any::<f64>(), Just(f64::NAN), Just(-0.0), Just(f64::INFINITY)]
            .prop_map(|d| Some(UpstreamAnyValueVariant::DoubleValue(d))),
        1 => proptest::collection::vec(any::<u8>(), 0..6)
            .prop_map(|b| Some(UpstreamAnyValueVariant::BytesValue(b))),
        1 => any::<i32>().prop_map(|i| Some(UpstreamAnyValueVariant::StringValueStrindex(i))),
    ]
}

/// Every `AnyValue` kind, an unset one included, nested up to three deep
/// through `ArrayValue` and `KeyValueList`.
fn variant() -> impl Strategy<Value = Option<UpstreamAnyValueVariant>> {
    leaf().prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..3).prop_map(|values| {
                Some(UpstreamAnyValueVariant::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue {
                        values: values
                            .into_iter()
                            .map(|value| UpstreamAnyValue { value })
                            .collect(),
                    },
                ))
            }),
            proptest::collection::vec((key(), inner), 0..3).prop_map(|entries| {
                Some(UpstreamAnyValueVariant::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList {
                        values: entries
                            .into_iter()
                            .map(|(key, value)| UpstreamKeyValue {
                                key,
                                value: Some(UpstreamAnyValue { value }),
                                ..Default::default()
                            })
                            .collect(),
                    },
                ))
            }),
        ]
    })
}

fn any_value() -> impl Strategy<Value = Option<UpstreamAnyValue>> {
    prop_oneof![
        1 => Just(None),
        6 => variant().prop_map(|value| Some(UpstreamAnyValue { value })),
    ]
}

fn attributes(max: usize) -> impl Strategy<Value = Vec<UpstreamKeyValue>> {
    proptest::collection::vec(
        (key(), any_value()).prop_map(|(key, value)| UpstreamKeyValue {
            key,
            value,
            ..Default::default()
        }),
        0..max,
    )
}

fn record() -> impl Strategy<Value = UpstreamLogRecord> {
    (
        any_value(),
        attributes(6),
        text(),
        text(),
        any::<i32>(),
        any::<u64>(),
        any::<u64>(),
        proptest::collection::vec(any::<u8>(), 0..17),
        proptest::collection::vec(any::<u8>(), 0..9),
        any::<u32>(),
    )
        .prop_map(
            |(
                body,
                attributes,
                severity_text,
                event_name,
                severity_number,
                ts,
                observed,
                trace_id,
                span_id,
                flags,
            )| {
                UpstreamLogRecord {
                    time_unix_nano: ts,
                    observed_time_unix_nano: observed,
                    severity_number,
                    severity_text,
                    body,
                    attributes,
                    flags,
                    trace_id,
                    span_id,
                    event_name,
                    ..Default::default()
                }
            },
        )
}

fn scope_logs() -> impl Strategy<Value = UpstreamScopeLogs> {
    (
        proptest::option::weighted(0.8, (text(), text(), attributes(4))),
        proptest::collection::vec(record(), 0..5),
    )
        .prop_map(|(scope, log_records)| UpstreamScopeLogs {
            scope: scope.map(|(name, version, attributes)| UpstreamScope {
                name,
                version,
                attributes,
                ..Default::default()
            }),
            log_records,
            ..Default::default()
        })
}

fn request() -> impl Strategy<Value = UpstreamRequest> {
    proptest::collection::vec(
        (
            proptest::option::weighted(0.8, attributes(5)),
            proptest::collection::vec(scope_logs(), 0..4),
        )
            .prop_map(|(resource, scope_logs)| UpstreamResourceLogs {
                resource: resource.map(|attributes| UpstreamResource {
                    attributes,
                    ..Default::default()
                }),
                scope_logs,
                ..Default::default()
            }),
        0..4,
    )
    .prop_map(|resource_logs| UpstreamRequest { resource_logs })
}

fn assert_any_value_eq(up: &UpstreamAnyValue, down: &VendoredAnyValue) {
    match (&up.value, &down.value) {
        (None, None) => {}
        (
            Some(UpstreamAnyValueVariant::StringValue(s)),
            Some(VendoredAnyValueVariant::StringValue(b)),
        ) => {
            assert_eq!(s.as_str(), bytes_str(b));
        }
        (
            Some(UpstreamAnyValueVariant::BoolValue(a)),
            Some(VendoredAnyValueVariant::BoolValue(b)),
        ) => {
            assert_eq!(a, b);
        }
        (
            Some(UpstreamAnyValueVariant::IntValue(a)),
            Some(VendoredAnyValueVariant::IntValue(b)),
        ) => {
            assert_eq!(a, b);
        }
        (
            Some(UpstreamAnyValueVariant::DoubleValue(a)),
            Some(VendoredAnyValueVariant::DoubleValue(b)),
        ) => {
            assert_eq!(a.to_bits(), b.to_bits(), "double_value bit pattern");
        }
        (
            Some(UpstreamAnyValueVariant::ArrayValue(a)),
            Some(VendoredAnyValueVariant::ArrayValue(b)),
        ) => {
            assert_eq!(a.values.len(), b.values.len());
            for (u, d) in a.values.iter().zip(b.values.iter()) {
                assert_any_value_eq(u, d);
            }
        }
        (
            Some(UpstreamAnyValueVariant::KvlistValue(a)),
            Some(VendoredAnyValueVariant::KvlistValue(b)),
        ) => {
            assert_eq!(a.values.len(), b.values.len());
            for (u, d) in a.values.iter().zip(b.values.iter()) {
                assert_kv_eq(u, d);
            }
        }
        (
            Some(UpstreamAnyValueVariant::BytesValue(a)),
            Some(VendoredAnyValueVariant::BytesValue(b)),
        ) => {
            assert_eq!(a.as_slice(), b.as_ref());
        }
        (
            Some(UpstreamAnyValueVariant::StringValueStrindex(a)),
            Some(VendoredAnyValueVariant::StringValueStrindex(b)),
        ) => {
            assert_eq!(a, b);
        }
        (u, _) => panic!("AnyValue variant mismatch: upstream={u:?}"),
    }
}

fn assert_kv_eq(up: &UpstreamKeyValue, down: &VendoredKeyValue) {
    assert_eq!(up.key.as_str(), bytes_str(&down.key));
    assert_eq!(up.key_strindex, down.key_strindex);
    match (&up.value, &down.value) {
        (None, None) => {}
        (Some(u), Some(d)) => assert_any_value_eq(u, d),
        _ => panic!("KeyValue.value presence mismatch"),
    }
}

fn assert_scope_eq(up: &Option<UpstreamScope>, down: &Option<VendoredScope>) {
    match (up, down) {
        (None, None) => {}
        (Some(u), Some(d)) => {
            assert_eq!(u.name.as_str(), bytes_str(&d.name));
            assert_eq!(u.version.as_str(), bytes_str(&d.version));
            assert_eq!(u.attributes.len(), d.attributes.len());
            for (uk, dk) in u.attributes.iter().zip(d.attributes.iter()) {
                assert_kv_eq(uk, dk);
            }
            assert_eq!(u.dropped_attributes_count, d.dropped_attributes_count);
        }
        _ => panic!("InstrumentationScope presence mismatch"),
    }
}

fn assert_resource_eq(up: &Option<UpstreamResource>, down: &Option<VendoredResource>) {
    match (up, down) {
        (None, None) => {}
        (Some(u), Some(d)) => {
            assert_eq!(u.attributes.len(), d.attributes.len());
            for (uk, dk) in u.attributes.iter().zip(d.attributes.iter()) {
                assert_kv_eq(uk, dk);
            }
            assert_eq!(u.dropped_attributes_count, d.dropped_attributes_count);
        }
        _ => panic!("Resource presence mismatch"),
    }
}

fn assert_record_eq(up: &UpstreamLogRecord, down: &VendoredLogRecord) {
    assert_eq!(up.time_unix_nano, down.time_unix_nano);
    assert_eq!(up.observed_time_unix_nano, down.observed_time_unix_nano);
    assert_eq!(up.severity_number, down.severity_number);
    assert_eq!(up.severity_text.as_str(), bytes_str(&down.severity_text));
    assert_eq!(up.flags, down.flags);
    assert_eq!(up.trace_id.as_slice(), down.trace_id.as_ref());
    assert_eq!(up.span_id.as_slice(), down.span_id.as_ref());
    assert_eq!(up.event_name.as_str(), bytes_str(&down.event_name));
    assert_eq!(up.dropped_attributes_count, down.dropped_attributes_count);
    assert_eq!(up.attributes.len(), down.attributes.len());
    for (u, d) in up.attributes.iter().zip(down.attributes.iter()) {
        assert_kv_eq(u, d);
    }
    match (&up.body, &down.body) {
        (None, None) => {}
        (Some(u), Some(d)) => assert_any_value_eq(u, d),
        _ => panic!("LogRecord.body presence mismatch"),
    }
}

fn assert_scope_logs_eq(up: &UpstreamScopeLogs, down: &VendoredScopeLogs) {
    assert_scope_eq(&up.scope, &down.scope);
    assert_eq!(up.log_records.len(), down.log_records.len());
    for (u, d) in up.log_records.iter().zip(down.log_records.iter()) {
        assert_record_eq(u, d);
    }
    assert_eq!(up.schema_url, down.schema_url);
}

fn assert_resource_logs_eq(up: &UpstreamResourceLogs, down: &VendoredResourceLogs) {
    assert_resource_eq(&up.resource, &down.resource);
    assert_eq!(up.scope_logs.len(), down.scope_logs.len());
    for (u, d) in up.scope_logs.iter().zip(down.scope_logs.iter()) {
        assert_scope_logs_eq(u, d);
    }
    assert_eq!(up.schema_url, down.schema_url);
}

fn assert_requests_eq(up: &UpstreamRequest, down: &VendoredRequest) {
    assert_eq!(up.resource_logs.len(), down.resource_logs.len());
    for (u, d) in up.resource_logs.iter().zip(down.resource_logs.iter()) {
        assert_resource_logs_eq(u, d);
    }
}

proptest! {
    #[test]
    fn equivalence(req in request()) {
        let body = Bytes::from(req.encode_to_vec());

        let upstream_decoded = UpstreamRequest::decode(body.clone())
            .expect("upstream decode of generated (valid-UTF-8) bytes");
        let vendored_decoded = VendoredRequest::decode(body)
            .expect("vendored decode of the same bytes");
        vendored_logs::validate_utf8(&vendored_decoded)
            .expect("request was generated with valid UTF-8 strings throughout");

        assert_requests_eq(&upstream_decoded, &vendored_decoded);
    }
}

// ---------------------------------------------------------------------
// refusal
// ---------------------------------------------------------------------

/// Never valid as a UTF-8 continuation of anything: 0xFF is not a legal
/// leading byte in any UTF-8 sequence.
fn invalid_utf8() -> Bytes {
    Bytes::from_static(&[0xFF, 0xFE])
}

fn ok_str() -> Bytes {
    Bytes::from_static(b"ok")
}

/// Builds a one-record vendored request and asserts: upstream's `String`
/// decode of the same bytes refuses it, the vendored decode accepts it
/// (bytes fields take anything), and [`validate_utf8`] refuses it naming
/// exactly `field`.
fn assert_refused(field: &'static str, req: VendoredRequest) {
    let body = Bytes::from(req.encode_to_vec());

    let upstream_result = UpstreamRequest::decode(body.clone());
    assert!(
        upstream_result.is_err(),
        "upstream String decode accepted invalid UTF-8 meant for {field}"
    );

    let vendored_decoded =
        VendoredRequest::decode(body).expect("vendored bytes decode does not validate UTF-8");
    let err = vendored_logs::validate_utf8(&vendored_decoded)
        .expect_err("validate_utf8 must refuse invalid UTF-8");
    assert_eq!(err.field, field);
}

fn base_record() -> VendoredLogRecord {
    VendoredLogRecord {
        severity_text: ok_str(),
        event_name: ok_str(),
        ..Default::default()
    }
}

fn base_scope() -> VendoredScope {
    VendoredScope {
        name: ok_str(),
        version: ok_str(),
        ..Default::default()
    }
}

fn wrap(
    resource: Option<VendoredResource>,
    scope: VendoredScope,
    record: VendoredLogRecord,
) -> VendoredRequest {
    VendoredRequest {
        resource_logs: vec![VendoredResourceLogs {
            resource,
            scope_logs: vec![VendoredScopeLogs {
                scope: Some(scope),
                log_records: vec![record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn refusal_key_value_key() {
    let mut record = base_record();
    record.attributes.push(VendoredKeyValue {
        key: invalid_utf8(),
        value: Some(VendoredAnyValue {
            value: Some(VendoredAnyValueVariant::StringValue(ok_str())),
        }),
        ..Default::default()
    });
    assert_refused("KeyValue.key", wrap(None, base_scope(), record));
}

#[test]
fn refusal_any_value_string_value() {
    let mut record = base_record();
    record.attributes.push(VendoredKeyValue {
        key: ok_str(),
        value: Some(VendoredAnyValue {
            value: Some(VendoredAnyValueVariant::StringValue(invalid_utf8())),
        }),
        ..Default::default()
    });
    assert_refused("AnyValue.string_value", wrap(None, base_scope(), record));
}

#[test]
fn refusal_log_record_severity_text() {
    let mut record = base_record();
    record.severity_text = invalid_utf8();
    assert_refused("LogRecord.severity_text", wrap(None, base_scope(), record));
}

#[test]
fn refusal_log_record_event_name() {
    let mut record = base_record();
    record.event_name = invalid_utf8();
    assert_refused("LogRecord.event_name", wrap(None, base_scope(), record));
}

#[test]
fn refusal_instrumentation_scope_name() {
    let mut scope = base_scope();
    scope.name = invalid_utf8();
    assert_refused(
        "InstrumentationScope.name",
        wrap(None, scope, base_record()),
    );
}

#[test]
fn refusal_instrumentation_scope_version() {
    let mut scope = base_scope();
    scope.version = invalid_utf8();
    assert_refused(
        "InstrumentationScope.version",
        wrap(None, scope, base_record()),
    );
}
