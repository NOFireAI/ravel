//! IPCrypt rule sets applied inside OTLP normalization (ADR-2993 decision 3,
//! issue #3047): metrics, logs and traces store the ciphertext of every value
//! a rule matches, series identity is computed over the stored labels, limits
//! and projections see stored bytes, and `TenantRules::none()` behaves exactly
//! like a resolved rule set that happens to be empty.
#![allow(clippy::expect_used)]

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value as ExemplarValue;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use ravel_ipcrypt::policy::{Variant, parse_policy_file};
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::{EpochKey, TenantRules};
use ravel_otlp::normalize::sanitize_label_name;
use ravel_otlp::traces_normalize::{ATTR_EVENTS_RAW, ATTR_LINKS_RAW};
use ravel_otlp::{
    IngestLimits, LogIngestLimits, LogRejection, Rejection, SpanIngestLimits, normalize_logs,
    normalize_metrics_with_exemplars, normalize_traces, project_log_resolved_label_bytes,
    project_resolved_label_bytes, project_span_resolved_label_bytes,
};
use ravel_types::logstream::AttrValue;
use ravel_types::{ExemplarCap, Label, LabelSet, SeriesId, TenantHash, TenantId};

const DEPLOYMENT_KEY: [u8; 32] = [7u8; 32];
const TENANT_HASH: TenantHash = TenantHash([9u8; 16]);
const RULE_KEY: &str = "client.ip";
const RULE_LABEL: &str = "client_ip";
const HOSTNAME: &str = "db.example.internal";

fn sanitize(key: &str) -> String {
    sanitize_label_name(key.to_string())
}

/// One deterministic rule on `client.ip` (stored as the `client_ip` label on
/// the metrics path), resolved for tenant `acme`.
fn rules() -> TenantRules {
    let policy = parse_policy_file(
        "[defaults]\nrules = [{ key = \"client.ip\", variant = \"deterministic\" }]\n",
        sanitize,
    )
    .expect("policy parses");
    let rules = TenantRules::resolve(&policy, "acme", &TENANT_HASH, &DEPLOYMENT_KEY, sanitize)
        .expect("rules resolve");
    assert!(!rules.is_empty());
    rules
}

/// A policy whose only rule belongs to another tenant: resolved for `acme`,
/// the rule set is empty without being `TenantRules::none()`.
fn resolved_empty_rules() -> TenantRules {
    let policy = parse_policy_file(
        "[tenants.other]\nrules = [{ key = \"client.ip\", variant = \"deterministic\" }]\n",
        sanitize,
    )
    .expect("policy parses");
    let rules = TenantRules::resolve(&policy, "acme", &TENANT_HASH, &DEPLOYMENT_KEY, sanitize)
        .expect("rules resolve");
    assert!(rules.is_empty());
    rules
}

/// The ciphertext `raw` must be stored as, derived independently of any rule.
fn expected(raw: &str) -> String {
    let key = EpochKey::new(&DEPLOYMENT_KEY, &TENANT_HASH, Variant::Deterministic, 1)
        .expect("epoch key derives");
    let (rendered, outcome) = key.encrypt(raw);
    assert_eq!(outcome, RenderOutcome::Encrypted);
    assert_ne!(rendered, raw);
    rendered
}

fn contains_bytes(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

// --- metrics ---

fn gauge_point(value: &str, exemplar_value: Option<&str>) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![string_kv(RULE_KEY, value)],
        time_unix_nano: 1_000,
        value: Some(NumberValue::AsDouble(1.0)),
        exemplars: exemplar_value
            .map(|v| Exemplar {
                time_unix_nano: 1_000,
                value: Some(ExemplarValue::AsDouble(0.5)),
                filtered_attributes: vec![string_kv(RULE_KEY, v)],
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

fn metrics_request(points: Vec<NumberDataPoint>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource::default()),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "request_latency".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: points,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn normalize_metrics(
    req: ExportMetricsServiceRequest,
    limits: &IngestLimits,
    rules: &TenantRules,
) -> ravel_otlp::MetricsNormalizeResult {
    let mut cap = ExemplarCap::new(10_000_000_000);
    normalize_metrics_with_exemplars(&tenant(), req, limits, rules, 1_000, &mut cap)
}

fn expected_series_id(stored: &str) -> SeriesId {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "request_latency".to_string(),
        },
        Label {
            name: RULE_LABEL.to_string(),
            value: stored.to_string(),
        },
    ])
    .expect("label set");
    SeriesId::compute(&tenant(), "request_latency", &labels).expect("series id")
}

#[test]
fn metrics_store_ciphertext_for_point_and_exemplar_labels() {
    let (point_ip, exemplar_ip) = ("192.0.2.1", "198.51.100.7");
    let result = normalize_metrics(
        metrics_request(vec![gauge_point(point_ip, Some(exemplar_ip))]),
        &IngestLimits::default(),
        &rules(),
    );
    assert!(
        result.output.rejected.is_empty(),
        "{:?}",
        result.output.rejected
    );
    assert_eq!(result.output.points.len(), 1);
    assert_eq!(result.exemplars.len(), 1);

    let debug = format!("{result:?}");
    assert!(!debug.contains(point_ip), "raw point IP leaked: {debug}");
    assert!(
        !debug.contains(exemplar_ip),
        "raw exemplar IP leaked: {debug}"
    );

    let point = &result.output.points[0];
    let point_label = point.labels.get(RULE_LABEL).expect("label stored");
    assert_eq!(point_label, expected(point_ip));
    assert!(!format!("{:?}", point.labels).contains(point_ip));

    let exemplar_labels = &result.exemplars[0].exemplar.filtered_attributes;
    assert_eq!(
        exemplar_labels,
        &vec![Label {
            name: RULE_LABEL.to_string(),
            value: expected(exemplar_ip),
        }]
    );

    let tally = &result.output.ipcrypt_tally;
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
        2
    );
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Unparsed),
        0
    );
    assert_eq!(tally.count(Variant::Prefix, RenderOutcome::Encrypted), 0);
}

#[test]
fn metrics_series_identity_is_computed_over_the_ciphertext() {
    let (a, b) = ("192.0.2.1", "192.0.2.2");
    let result = normalize_metrics(
        metrics_request(vec![gauge_point(a, None), gauge_point(b, None)]),
        &IngestLimits::default(),
        &rules(),
    );
    assert!(result.output.rejected.is_empty());
    let points = &result.output.points;
    assert_eq!(points.len(), 2);
    assert_ne!(points[0].series_id, points[1].series_id);
    assert_eq!(points[0].series_id, expected_series_id(&expected(a)));
    assert_eq!(points[1].series_id, expected_series_id(&expected(b)));
    // Identity over the raw value would be this id instead.
    assert_ne!(points[0].series_id, expected_series_id(a));
}

/// The tally counts every stored value: two points carrying the same ruled
/// attribute count twice, though the second one reuses the first one's
/// built labels.
#[test]
fn metrics_tally_counts_each_stored_point_with_identical_attributes() {
    let ip = "192.0.2.1";
    let result = normalize_metrics(
        metrics_request(vec![gauge_point(ip, None), gauge_point(ip, None)]),
        &IngestLimits::default(),
        &rules(),
    );
    assert!(result.output.rejected.is_empty());
    assert_eq!(result.output.points.len(), 2);
    assert_eq!(
        result.output.points[0].series_id,
        result.output.points[1].series_id
    );
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        2
    );
}

/// A rejected point stores nothing, so its ruled values are not counted:
/// `client.ip` and `client_ip` both sanitize to `client_ip`, which rejects
/// the second point after its first label was already encrypted.
#[test]
fn metrics_tally_skips_a_rejected_point() {
    let mut rejected = gauge_point("192.0.2.2", None);
    rejected.attributes.push(string_kv(RULE_LABEL, "192.0.2.3"));
    let result = normalize_metrics(
        metrics_request(vec![gauge_point("192.0.2.1", None), rejected]),
        &IngestLimits::default(),
        &rules(),
    );
    assert_eq!(result.output.points.len(), 1);
    assert_eq!(
        result.output.rejected,
        vec![Rejection::DuplicateLabelName(RULE_LABEL.to_string())]
    );
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        1
    );
}

/// Direction: a value whose raw length is within `max_label_value_len` but
/// whose stored ciphertext is longer than it is rejected, and reports the
/// stored length.
#[test]
fn metrics_label_value_limit_sees_the_stored_length() {
    let raw = "::1";
    let stored = expected(raw);
    assert!(stored.len() > raw.len(), "{stored}");
    let limits = IngestLimits {
        max_label_value_len: stored.len() - 1,
        ..IngestLimits::default()
    };

    let without = normalize_metrics(
        metrics_request(vec![gauge_point(raw, None)]),
        &limits,
        &TenantRules::none(),
    );
    assert_eq!(without.output.points.len(), 1, "raw value fits the limit");

    let with = normalize_metrics(
        metrics_request(vec![gauge_point(raw, None)]),
        &limits,
        &rules(),
    );
    assert!(with.output.points.is_empty());
    assert_eq!(
        with.output.rejected,
        vec![Rejection::LabelValueTooLong {
            len: stored.len(),
            max: stored.len() - 1,
        }]
    );
}

/// A hostname under a rule is stored unchanged (unparsed), but the
/// projection has no way to know that before rendering and charges
/// `max(47, 19) = 47` bytes for it, 28 more than the raw value.
#[test]
fn metrics_projection_charges_max_stored_len_for_a_ruled_value() {
    let req = metrics_request(vec![gauge_point(HOSTNAME, None)]);
    let limits = IngestLimits::default();
    let without = project_resolved_label_bytes(&req, &limits, &TenantRules::none()).get();
    let with = project_resolved_label_bytes(&req, &limits, &rules()).get();
    assert_eq!(with, without + (47 - HOSTNAME.len()));
    assert_eq!(with, PINNED_METRICS_PROJECTION);

    let result = normalize_metrics(req, &limits, &rules());
    assert_eq!(
        result.output.points[0].labels.get(RULE_LABEL),
        Some(HOSTNAME)
    );
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Unparsed),
        1
    );
}

const PINNED_METRICS_PROJECTION: usize = 207;

#[test]
fn metrics_none_matches_a_resolved_empty_rule_set() {
    let req = || metrics_request(vec![gauge_point("192.0.2.1", Some("198.51.100.7"))]);
    let limits = IngestLimits::default();
    let none = normalize_metrics(req(), &limits, &TenantRules::none());
    let empty = normalize_metrics(req(), &limits, &resolved_empty_rules());
    assert_eq!(none, empty);
    assert!(none.output.ipcrypt_tally.is_empty());
    assert_eq!(
        project_resolved_label_bytes(&req(), &limits, &TenantRules::none()).get(),
        project_resolved_label_bytes(&req(), &limits, &resolved_empty_rules()).get()
    );
}

/// The rule set is resolved with the same sanitizer metrics store label names
/// under, so a rule on a dotted key matches its sanitized label.
#[test]
fn rule_lookup_uses_the_metrics_label_sanitizer() {
    let rules = rules();
    let (raw, label) = ("client.ip", "client_ip");
    assert_eq!(sanitize_label_name(raw.to_string()), label);
    assert_eq!(
        rules.lookup_label(label).map(|info| info.raw_key),
        Some(raw)
    );
    assert_eq!(rules.lookup_raw(raw).map(|info| info.raw_key), Some(raw));
    assert!(rules.lookup_label("client.ip").is_none());
}

// --- logs ---

fn logs_request(
    resource_ip: &str,
    scope_ip: &str,
    record_attrs: Vec<KeyValue>,
) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv(RULE_KEY, resource_ip)],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "lib".to_string(),
                    version: "1".to_string(),
                    attributes: vec![string_kv(RULE_KEY, scope_ip)],
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000,
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("hello".to_string())),
                    }),
                    attributes: record_attrs,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn logs_store_ciphertext_for_resource_scope_and_record_attributes() {
    let (resource_ip, scope_ip, record_ip) = ("192.0.2.1", "192.0.2.2", "192.0.2.3");
    let out = normalize_logs(
        logs_request(resource_ip, scope_ip, vec![string_kv(RULE_KEY, record_ip)]),
        &LogIngestLimits::default(),
        &rules(),
        1_000,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(out.records.len(), 1);
    let record = &out.records[0];

    let debug = format!("{out:?}");
    for raw in [resource_ip, scope_ip, record_ip] {
        assert!(!debug.contains(raw), "raw {raw} leaked: {debug}");
        assert!(
            !contains_bytes(&record.stream_attrs, raw),
            "raw {raw} in stream_attrs"
        );
    }
    assert!(contains_bytes(&record.stream_attrs, &expected(resource_ip)));
    assert!(contains_bytes(&record.stream_attrs, &expected(scope_ip)));
    assert_eq!(
        record.attrs,
        vec![(RULE_KEY.to_string(), AttrValue::Str(expected(record_ip)))]
    );

    assert_eq!(
        out.ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        3
    );
    assert_eq!(
        out.ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Unparsed),
        0
    );
}

/// A non-string value under a rule is left as sent and counted unparsed.
#[test]
fn logs_non_string_value_under_a_rule_is_counted_unparsed_and_kept() {
    let int_kv = KeyValue {
        key: RULE_KEY.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::IntValue(42)),
        }),
        ..Default::default()
    };
    let out = normalize_logs(
        logs_request("192.0.2.1", "192.0.2.2", vec![int_kv]),
        &LogIngestLimits::default(),
        &rules(),
        1_000,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(
        out.records[0].attrs,
        vec![(RULE_KEY.to_string(), AttrValue::I64(42))]
    );
    assert_eq!(
        out.ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        2
    );
    assert_eq!(
        out.ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Unparsed),
        1
    );
}

/// Same direction as the metrics case: raw within the attribute value limit,
/// stored ciphertext over it, so the attribute is dropped (logs keep the
/// record) and the rejection reports the stored length.
#[test]
fn logs_attribute_value_limit_sees_the_stored_length() {
    let raw = "::1";
    let stored = expected(raw);
    assert!(stored.len() > raw.len(), "{stored}");
    let limits = LogIngestLimits {
        max_attribute_value_len: stored.len() - 1,
        ..LogIngestLimits::default()
    };
    let req = || logs_request("", "", vec![string_kv(RULE_KEY, raw)]);

    let without = normalize_logs(req(), &limits, &TenantRules::none(), 1_000);
    assert!(without.rejected.is_empty(), "raw value fits the limit");
    assert_eq!(without.records[0].attrs.len(), 1);

    let with = normalize_logs(req(), &limits, &rules(), 1_000);
    assert_eq!(with.records.len(), 1);
    assert!(with.records[0].attrs.is_empty());
    assert_eq!(
        with.rejected,
        vec![LogRejection::AttributeValueTooLong {
            len: stored.len(),
            max: stored.len() - 1,
        }]
    );
}

#[test]
fn logs_projection_charges_max_stored_len_for_a_ruled_value() {
    let req = logs_request("", "", vec![string_kv(RULE_KEY, HOSTNAME)]);
    let limits = LogIngestLimits::default();
    let without = project_log_resolved_label_bytes(&req, &limits, &TenantRules::none()).get();
    let with = project_log_resolved_label_bytes(&req, &limits, &rules()).get();
    assert_eq!(with, without + (47 - HOSTNAME.len()));
    assert_eq!(with, PINNED_LOGS_PROJECTION);
}

const PINNED_LOGS_PROJECTION: usize = 330;

#[test]
fn logs_none_matches_a_resolved_empty_rule_set() {
    let req = || {
        logs_request(
            "192.0.2.1",
            "192.0.2.2",
            vec![string_kv(RULE_KEY, "192.0.2.3")],
        )
    };
    let limits = LogIngestLimits::default();
    let none = normalize_logs(req(), &limits, &TenantRules::none(), 1_000);
    let empty = normalize_logs(req(), &limits, &resolved_empty_rules(), 1_000);
    assert_eq!(none, empty);
    assert!(none.ipcrypt_tally.is_empty());
    assert_eq!(
        project_log_resolved_label_bytes(&req(), &limits, &TenantRules::none()).get(),
        project_log_resolved_label_bytes(&req(), &limits, &resolved_empty_rules()).get()
    );
}

// --- traces ---

const TRACE: [u8; 16] = [0xab; 16];
const SPAN: [u8; 8] = [0xcd; 8];

fn traces_request(span_ip: &str, event_ip: &str, link_ip: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource::default()),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "lib".to_string(),
                    version: "1".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: TRACE.to_vec(),
                    span_id: SPAN.to_vec(),
                    name: "op".to_string(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    attributes: vec![string_kv(RULE_KEY, span_ip)],
                    events: vec![Event {
                        time_unix_nano: 1,
                        name: "connect".to_string(),
                        attributes: vec![string_kv(RULE_KEY, event_ip)],
                        dropped_attributes_count: 0,
                    }],
                    links: vec![Link {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        attributes: vec![string_kv(RULE_KEY, link_ip)],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn attr<'a>(attrs: &'a [(String, String)], key: &str) -> &'a str {
    attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .expect("attribute stored")
}

fn string_value(kv: &KeyValue) -> Option<&str> {
    match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(AnyValueVariant::StringValue(s)) => Some(s),
        _ => None,
    }
}

#[test]
fn traces_store_ciphertext_for_span_event_and_link_attributes() {
    let (span_ip, event_ip, link_ip) = ("192.0.2.1", "192.0.2.2", "192.0.2.3");
    let out = normalize_traces(
        traces_request(span_ip, event_ip, link_ip),
        &SpanIngestLimits::default(),
        &rules(),
        5_000,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(out.spans.len(), 1);
    let attrs = &out.spans[0].attrs;

    let debug = format!("{out:?}");
    for raw in [span_ip, event_ip, link_ip] {
        assert!(!debug.contains(raw), "raw {raw} leaked: {debug}");
        // Event and link attributes are stored inside hex-encoded blobs.
        assert!(
            !debug.contains(&hex::encode(raw)),
            "raw {raw} leaked hex-encoded: {debug}"
        );
    }
    assert_eq!(attr(attrs, RULE_KEY), expected(span_ip));

    let events = hex::decode(attr(attrs, ATTR_EVENTS_RAW)).expect("events blob is hex");
    let event = Event::decode_length_delimited(events.as_slice()).expect("decodes as an Event");
    assert_eq!(event.attributes.len(), 1);
    assert_eq!(
        string_value(&event.attributes[0]),
        Some(expected(event_ip).as_str())
    );

    let links = hex::decode(attr(attrs, ATTR_LINKS_RAW)).expect("links blob is hex");
    let link = Link::decode_length_delimited(links.as_slice()).expect("decodes as a Link");
    assert_eq!(link.attributes.len(), 1);
    assert_eq!(
        string_value(&link.attributes[0]),
        Some(expected(link_ip).as_str())
    );

    assert_eq!(
        out.ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        3
    );
}

#[test]
fn traces_projection_charges_max_stored_len_for_a_ruled_value() {
    let mut req = traces_request(HOSTNAME, "", "");
    let span = &mut req.resource_spans[0].scope_spans[0].spans[0];
    span.events.clear();
    span.links.clear();
    let limits = SpanIngestLimits::default();
    let without = project_span_resolved_label_bytes(&req, &limits, &TenantRules::none()).get();
    let with = project_span_resolved_label_bytes(&req, &limits, &rules()).get();
    assert_eq!(with, without + (47 - HOSTNAME.len()));
    assert_eq!(with, PINNED_SPANS_PROJECTION);
}

const PINNED_SPANS_PROJECTION: usize = 450;

#[test]
fn traces_none_matches_a_resolved_empty_rule_set() {
    let req = || traces_request("192.0.2.1", "192.0.2.2", "192.0.2.3");
    let limits = SpanIngestLimits::default();
    let none = normalize_traces(req(), &limits, &TenantRules::none(), 5_000);
    let empty = normalize_traces(req(), &limits, &resolved_empty_rules(), 5_000);
    assert_eq!(none, empty);
    assert!(none.ipcrypt_tally.is_empty());
    assert_eq!(
        project_span_resolved_label_bytes(&req(), &limits, &TenantRules::none()).get(),
        project_span_resolved_label_bytes(&req(), &limits, &resolved_empty_rules()).get()
    );
}

// --- prefix variant ---

/// One prefix-preserving rule on `client.ip`, resolved for tenant `acme`.
fn prefix_rules() -> TenantRules {
    let policy = parse_policy_file(
        "[defaults]\nrules = [{ key = \"client.ip\", variant = \"prefix\" }]\n",
        sanitize,
    )
    .expect("policy parses");
    TenantRules::resolve(&policy, "acme", &TENANT_HASH, &DEPLOYMENT_KEY, sanitize)
        .expect("rules resolve")
}

/// The prefix-preserving ciphertext of the IPv4 address `raw`, derived
/// independently of any rule. It is itself an IPv4 address.
fn expected_prefix(raw: &str) -> String {
    let key = EpochKey::new(&DEPLOYMENT_KEY, &TENANT_HASH, Variant::Prefix, 1)
        .expect("epoch key derives");
    let (rendered, outcome) = key.encrypt(raw);
    assert_eq!(outcome, RenderOutcome::Encrypted);
    assert_ne!(rendered, raw);
    assert!(rendered.parse::<std::net::Ipv4Addr>().is_ok(), "{rendered}");
    rendered
}

fn assert_prefix_tally(tally: &ravel_ipcrypt::tally::IpcryptTally, encrypted: u64) {
    assert_eq!(
        tally.count(Variant::Prefix, RenderOutcome::Encrypted),
        encrypted
    );
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
        0
    );
}

#[test]
fn metrics_prefix_rule_keeps_the_ipv4_family() {
    let (point_ip, exemplar_ip) = ("192.0.2.1", "198.51.100.7");
    let result = normalize_metrics(
        metrics_request(vec![gauge_point(point_ip, Some(exemplar_ip))]),
        &IngestLimits::default(),
        &prefix_rules(),
    );
    assert!(result.output.rejected.is_empty());
    assert_eq!(
        result.output.points[0].labels.get(RULE_LABEL),
        Some(expected_prefix(point_ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![Label {
            name: RULE_LABEL.to_string(),
            value: expected_prefix(exemplar_ip),
        }]
    );
    assert_prefix_tally(&result.output.ipcrypt_tally, 2);
}

#[test]
fn logs_prefix_rule_keeps_the_ipv4_family() {
    let record_ip = "192.0.2.3";
    let out = normalize_logs(
        logs_request("", "", vec![string_kv(RULE_KEY, record_ip)]),
        &LogIngestLimits::default(),
        &prefix_rules(),
        1_000,
    );
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(
        out.records[0].attrs,
        vec![(
            RULE_KEY.to_string(),
            AttrValue::Str(expected_prefix(record_ip))
        )]
    );
    assert_prefix_tally(&out.ipcrypt_tally, 1);
}

#[test]
fn traces_prefix_rule_keeps_the_ipv4_family() {
    let span_ip = "192.0.2.1";
    let mut req = traces_request(span_ip, "", "");
    let span = &mut req.resource_spans[0].scope_spans[0].spans[0];
    span.events.clear();
    span.links.clear();
    let out = normalize_traces(req, &SpanIngestLimits::default(), &prefix_rules(), 5_000);
    assert!(out.rejected.is_empty(), "{:?}", out.rejected);
    assert_eq!(
        attr(&out.spans[0].attrs, RULE_KEY),
        expected_prefix(span_ip)
    );
    assert_prefix_tally(&out.ipcrypt_tally, 1);
}
