//! IPCrypt rule sets applied inside Remote Write normalization (ADR-2993
//! decision 3, issue #3047): sample and exemplar labels store the ciphertext
//! of every value a rule matches, matched by the label name sanitized the
//! way OTLP sanitizes an attribute key and stored under the name as sent,
//! series identity is computed over the stored labels, and
//! `TenantRules::none()` behaves exactly like a resolved empty rule set.
#![allow(clippy::expect_used)]

use ravel_ipcrypt::policy::{Variant, parse_policy_file};
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::{EpochKey, TenantRules};
use ravel_otlp::normalize::sanitize_label_name;
use ravel_otlp::{IngestLimits, Rejection};
use ravel_remote_write::{
    ResolvedExemplar, ResolvedRequest, ResolvedSample, ResolvedSeries, RwMetricsNormalizeResult,
    RwRejection, normalize_with_exemplars,
};
use ravel_types::{ExemplarCap, Label, LabelSet, SeriesId, TenantHash, TenantId};

const DEPLOYMENT_KEY: [u8; 32] = [7u8; 32];
const TENANT_HASH: TenantHash = TenantHash([9u8; 16]);
const RULE_LABEL: &str = "client_ip";
const TS_MS: i64 = 1_700_000_000_000;
const INGEST_TS_NS: i64 = 1_700_000_001_000_000_000;

fn sanitize(key: &str) -> String {
    sanitize_label_name(key.to_string())
}

/// One deterministic rule on the OTLP key `client.ip`, whose sanitized label
/// name `client_ip` is what a Remote Write sender sends.
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

fn expected(raw: &str) -> String {
    let key = EpochKey::new(&DEPLOYMENT_KEY, &TENANT_HASH, Variant::Deterministic, 1)
        .expect("epoch key derives");
    let (rendered, outcome) = key.encrypt(raw);
    assert_eq!(outcome, RenderOutcome::Encrypted);
    assert_ne!(rendered, raw);
    rendered
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn series(ip: &str, exemplar_ip: Option<&str>) -> ResolvedSeries {
    ResolvedSeries {
        labels: vec![label("__name__", "requests_total"), label(RULE_LABEL, ip)],
        samples: vec![ResolvedSample {
            ts_ms: TS_MS,
            value: 1.0,
        }],
        histograms: vec![],
        exemplars: exemplar_ip
            .map(|v| ResolvedExemplar {
                ts_ms: TS_MS,
                value: 0.5,
                labels: vec![label(RULE_LABEL, v)],
            })
            .into_iter()
            .collect(),
    }
}

fn request(series: Vec<ResolvedSeries>) -> ResolvedRequest {
    ResolvedRequest {
        series,
        ..ResolvedRequest::default()
    }
}

fn normalize(
    req: ResolvedRequest,
    limits: &IngestLimits,
    rules: &TenantRules,
) -> RwMetricsNormalizeResult {
    let mut cap = ExemplarCap::new(10_000_000_000);
    normalize_with_exemplars(&tenant(), req, limits, rules, INGEST_TS_NS, &mut cap)
}

fn expected_series_id(stored: &str) -> SeriesId {
    let labels = LabelSet::new(vec![
        label("__name__", "requests_total"),
        label(RULE_LABEL, stored),
    ])
    .expect("label set");
    SeriesId::compute(&tenant(), "requests_total", &labels).expect("series id")
}

#[test]
fn remote_write_stores_ciphertext_for_sample_and_exemplar_labels() {
    let (sample_ip, exemplar_ip) = ("192.0.2.1", "198.51.100.7");
    let result = normalize(
        request(vec![series(sample_ip, Some(exemplar_ip))]),
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
    assert!(!debug.contains(sample_ip), "raw sample IP leaked: {debug}");
    assert!(
        !debug.contains(exemplar_ip),
        "raw exemplar IP leaked: {debug}"
    );

    let point = &result.output.points[0];
    assert_eq!(
        point.labels.get(RULE_LABEL),
        Some(expected(sample_ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![label(RULE_LABEL, &expected(exemplar_ip))]
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
}

/// One deterministic rule on `client.address`.
fn address_rules() -> TenantRules {
    let policy = parse_policy_file(
        "[defaults]\nrules = [{ key = \"client.address\", variant = \"deterministic\" }]\n",
        sanitize,
    )
    .expect("policy parses");
    TenantRules::resolve(&policy, "acme", &TENANT_HASH, &DEPLOYMENT_KEY, sanitize)
        .expect("rules resolve")
}

/// A series and exemplar both labelled `name = ip`, normalized under
/// [`address_rules`]; asserts the label is stored under `name` unchanged
/// with the ciphertext of `ip`, on the sample and on the exemplar.
fn assert_address_label_encrypted_as_sent(name: &str) {
    let ip = "192.0.2.1";
    let mut s = series(ip, Some(ip));
    s.labels[1] = label(name, ip);
    s.exemplars[0].labels = vec![label(name, ip)];
    let result = normalize(request(vec![s]), &IngestLimits::default(), &address_rules());
    assert!(result.output.rejected.is_empty());
    assert_eq!(
        result.output.points[0].labels.get(name),
        Some(expected(ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![label(name, &expected(ip))]
    );
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        2
    );
}

/// A sender's dotted `client.address` label matches the rule on
/// `client.address` through its sanitized form, the way an OTLP attribute
/// would, and is stored under `client.address` as sent.
#[test]
fn remote_write_matches_a_dotted_label_name_and_stores_it_as_sent() {
    assert_address_label_encrypted_as_sent("client.address");
}

#[test]
fn remote_write_matches_the_sanitized_label_name() {
    assert_address_label_encrypted_as_sent("client_address");
}

/// A prefix-preserving rule stores an IPv4 address as the IPv4 ciphertext
/// the prefix epoch key gives it, on the sample and the exemplar labels.
#[test]
fn remote_write_prefix_rule_keeps_the_ipv4_family() {
    let policy = parse_policy_file(
        "[defaults]\nrules = [{ key = \"client.ip\", variant = \"prefix\" }]\n",
        sanitize,
    )
    .expect("policy parses");
    let rules = TenantRules::resolve(&policy, "acme", &TENANT_HASH, &DEPLOYMENT_KEY, sanitize)
        .expect("rules resolve");
    let key = EpochKey::new(&DEPLOYMENT_KEY, &TENANT_HASH, Variant::Prefix, 1)
        .expect("epoch key derives");
    let expected_prefix = |raw: &str| {
        let (rendered, outcome) = key.encrypt(raw);
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert_ne!(rendered, raw);
        assert!(rendered.parse::<std::net::Ipv4Addr>().is_ok(), "{rendered}");
        rendered
    };

    let (sample_ip, exemplar_ip) = ("192.0.2.1", "198.51.100.7");
    let result = normalize(
        request(vec![series(sample_ip, Some(exemplar_ip))]),
        &IngestLimits::default(),
        &rules,
    );
    assert!(result.output.rejected.is_empty());
    assert_eq!(
        result.output.points[0].labels.get(RULE_LABEL),
        Some(expected_prefix(sample_ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![label(RULE_LABEL, &expected_prefix(exemplar_ip))]
    );
    let tally = &result.output.ipcrypt_tally;
    assert_eq!(tally.count(Variant::Prefix, RenderOutcome::Encrypted), 2);
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
        0
    );
}

/// A rejected series stores nothing, so its ruled value is not counted,
/// though it was encrypted before a later label rejected the series.
#[test]
fn remote_write_tally_skips_a_rejected_series() {
    let mut rejected = series("192.0.2.2", None);
    rejected.labels.push(label("zone", &"z".repeat(100)));
    let limits = IngestLimits {
        max_label_value_len: 99,
        ..IngestLimits::default()
    };
    let result = normalize(
        request(vec![series("192.0.2.1", None), rejected]),
        &limits,
        &rules(),
    );
    assert_eq!(result.output.points.len(), 1);
    assert_eq!(result.output.rejected.len(), 1);
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        1
    );
}

#[test]
fn remote_write_series_identity_is_computed_over_the_ciphertext() {
    let (a, b) = ("192.0.2.1", "192.0.2.2");
    let result = normalize(
        request(vec![series(a, None), series(b, None)]),
        &IngestLimits::default(),
        &rules(),
    );
    assert!(result.output.rejected.is_empty());
    let points = &result.output.points;
    assert_eq!(points.len(), 2);
    assert_ne!(points[0].series_id, points[1].series_id);
    assert_eq!(points[0].series_id, expected_series_id(&expected(a)));
    assert_eq!(points[1].series_id, expected_series_id(&expected(b)));
    assert_ne!(points[0].series_id, expected_series_id(a));
}

/// Direction: raw value within `max_label_value_len`, stored ciphertext over
/// it, so the series is rejected with the stored length.
#[test]
fn remote_write_label_value_limit_sees_the_stored_length() {
    let raw = "::1";
    let stored = expected(raw);
    assert!(stored.len() > raw.len(), "{stored}");
    let limits = IngestLimits {
        max_label_value_len: stored.len() - 1,
        ..IngestLimits::default()
    };

    let without = normalize(
        request(vec![series(raw, None)]),
        &limits,
        &TenantRules::none(),
    );
    assert_eq!(without.output.points.len(), 1, "raw value fits the limit");

    let with = normalize(request(vec![series(raw, None)]), &limits, &rules());
    assert!(with.output.points.is_empty());
    assert_eq!(
        with.output.rejected,
        vec![RwRejection::Otlp {
            reason: Rejection::LabelValueTooLong {
                len: stored.len(),
                max: stored.len() - 1,
            },
            count: 1,
        }]
    );
}

#[test]
fn remote_write_none_matches_a_resolved_empty_rule_set() {
    let req = || request(vec![series("192.0.2.1", Some("198.51.100.7"))]);
    let limits = IngestLimits::default();
    let none = normalize(req(), &limits, &TenantRules::none());
    let empty = normalize(req(), &limits, &resolved_empty_rules());
    assert_eq!(none, empty);
    assert!(none.output.ipcrypt_tally.is_empty());
}
