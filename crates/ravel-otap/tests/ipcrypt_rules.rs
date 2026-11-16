//! IPCrypt rule sets applied inside OTAP normalization (ADR-2993 decision 3,
//! issue #3047): data-point, exemplar and promoted resource labels store the
//! ciphertext of every value a rule matches, series identity is computed over
//! the stored labels, the raw-length checks and the projection charge
//! `max(47, raw)` for a ruled value, and `TenantRules::none()` behaves exactly
//! like a resolved empty rule set.
#![allow(clippy::expect_used)]

use ravel_ipcrypt::policy::{Variant, parse_policy_file};
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::{EpochKey, TenantRules};
use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, ExemplarRow, ExemplarValue, MetricKind, MetricRow,
    MetricsStreamEncoder, ResourceAssignment,
};
use ravel_otap::normalize::{normalize_decoded_with_exemplars, project_resolved_label_bytes};
use ravel_otap::stream::{DecodedBatch, StreamConfig, StreamState};
use ravel_otlp::normalize::{MetricsNormalizeResult, sanitize_label_name};
use ravel_otlp::{IngestLimits, Rejection};
use ravel_types::{ExemplarCap, Label, LabelSet, SeriesId, TenantHash, TenantId};

const DEPLOYMENT_KEY: [u8; 32] = [7u8; 32];
const TENANT_HASH: TenantHash = TenantHash([9u8; 16]);
const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
const POINT_KEY: &str = "client.ip";
const POINT_LABEL: &str = "client_ip";
const RESOURCE_KEY: &str = "host.ip";
const RESOURCE_LABEL: &str = "host_ip";
const HOSTNAME: &str = "db.example.internal";

fn sanitize(key: &str) -> String {
    sanitize_label_name(key.to_string())
}

/// Deterministic rules on `client.ip` (a data-point and exemplar attribute)
/// and `host.ip` (a resource attribute promoted through the allowlist).
fn rules() -> TenantRules {
    let policy = parse_policy_file(
        "[defaults]\nrules = [\n  { key = \"client.ip\", variant = \"deterministic\" },\n  { key = \"host.ip\", variant = \"deterministic\" },\n]\n",
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

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn attr(key: &str, value: &str) -> AttrRow {
    AttrRow {
        key: key.to_string(),
        value: AttrValue::Str(value.to_string()),
    }
}

fn point(ip: &str, exemplar_ip: Option<&str>) -> DataPointRow {
    DataPointRow {
        time_unix_nano: INGEST_TS_NS,
        value: 1.0,
        flags: 0,
        exemplars: exemplar_ip
            .map(|v| ExemplarRow {
                time_unix_nano: INGEST_TS_NS,
                value: ExemplarValue::Double(0.5),
                trace_id: None,
                span_id: None,
                attrs: vec![attr(POINT_KEY, v)],
            })
            .into_iter()
            .collect(),
        attrs: vec![attr(POINT_KEY, ip)],
    }
}

/// One gauge `request_latency` holding `points`, under a resource carrying
/// `host.ip = resource_ip` when given, through the real IPC encode/decode.
fn batch(points: Vec<DataPointRow>, resource_ip: Option<&str>) -> DecodedBatch {
    let metrics = vec![MetricRow {
        name: "request_latency".to_string(),
        kind: MetricKind::Gauge,
        data_points: points,
    }];
    let resources: Vec<Vec<AttrRow>> = resource_ip
        .map(|ip| vec![attr(RESOURCE_KEY, ip)])
        .into_iter()
        .collect();
    let slots = [resource_ip.map(|_| 0usize)];
    let mut encoder = MetricsStreamEncoder::new("ipcrypt-rules").expect("encoder");
    let raw = encoder
        .encode_batch_resources(
            0,
            &metrics,
            &[""],
            &[],
            &[],
            &[],
            &[],
            &ResourceAssignment {
                resources: &resources,
                metrics: &slots,
                histograms: &[],
                summaries: &[],
            },
        )
        .expect("encode");
    StreamState::new(StreamConfig::default())
        .decode(raw)
        .expect("decode")
}

fn limits() -> IngestLimits {
    IngestLimits {
        resource_attribute_allowlist: vec![RESOURCE_KEY.to_string()],
        ..IngestLimits::default()
    }
}

fn normalize(
    batch: &DecodedBatch,
    limits: &IngestLimits,
    rules: &TenantRules,
) -> MetricsNormalizeResult {
    let mut cap = ExemplarCap::new(10_000_000_000);
    normalize_decoded_with_exemplars(&tenant(), batch, limits, rules, INGEST_TS_NS, &mut cap)
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn expected_series_id(stored: &str) -> SeriesId {
    let labels = LabelSet::new(vec![
        label("__name__", "request_latency"),
        label(POINT_LABEL, stored),
    ])
    .expect("label set");
    SeriesId::compute(&tenant(), "request_latency", &labels).expect("series id")
}

#[test]
fn otap_stores_ciphertext_for_point_exemplar_and_resource_labels() {
    let (point_ip, exemplar_ip, resource_ip) = ("192.0.2.1", "198.51.100.7", "203.0.113.9");
    let result = normalize(
        &batch(vec![point(point_ip, Some(exemplar_ip))], Some(resource_ip)),
        &limits(),
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
    for raw in [point_ip, exemplar_ip, resource_ip] {
        assert!(!debug.contains(raw), "raw {raw} leaked: {debug}");
    }

    let labels = &result.output.points[0].labels;
    assert_eq!(labels.get(POINT_LABEL), Some(expected(point_ip).as_str()));
    assert_eq!(
        labels.get(RESOURCE_LABEL),
        Some(expected(resource_ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![label(POINT_LABEL, &expected(exemplar_ip))]
    );

    let tally = &result.output.ipcrypt_tally;
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
        3
    );
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Unparsed),
        0
    );
}

#[test]
fn otap_series_identity_is_computed_over_the_ciphertext() {
    let (a, b) = ("192.0.2.1", "192.0.2.2");
    let result = normalize(
        &batch(vec![point(a, None), point(b, None)], None),
        &limits(),
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

/// The tally counts every stored value: two points carrying the same ruled
/// attribute count twice, though the second one reuses the first one's
/// group.
#[test]
fn otap_tally_counts_each_stored_point_with_identical_attributes() {
    let ip = "192.0.2.1";
    let result = normalize(
        &batch(vec![point(ip, None), point(ip, None)], None),
        &limits(),
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
fn otap_tally_skips_a_rejected_point() {
    let mut rejected = point("192.0.2.2", None);
    rejected.attrs.push(attr(POINT_LABEL, "192.0.2.3"));
    let result = normalize(
        &batch(vec![point("192.0.2.1", None), rejected], None),
        &limits(),
        &rules(),
    );
    assert_eq!(result.output.points.len(), 1);
    assert_eq!(
        result.output.rejected,
        vec![Rejection::DuplicateLabelName(POINT_LABEL.to_string())]
    );
    assert_eq!(
        result
            .output
            .ipcrypt_tally
            .count(Variant::Deterministic, RenderOutcome::Encrypted),
        1
    );
}

/// Direction: a value whose raw length is within `max_label_value_len` is
/// rejected once a rule matches it, because its stored length can exceed the
/// limit. OTAP's raw-length check runs before any label is built and charges
/// `max(47, raw)`, so the rejection reports 47 rather than the ciphertext's
/// own length.
#[test]
fn otap_label_value_limit_sees_the_stored_length() {
    let raw = "::1";
    let stored = expected(raw);
    assert!(stored.len() > raw.len(), "{stored}");
    let limits = IngestLimits {
        max_label_value_len: stored.len() - 1,
        ..limits()
    };

    let without = normalize(
        &batch(vec![point(raw, None)], None),
        &limits,
        &TenantRules::none(),
    );
    assert_eq!(without.output.points.len(), 1, "raw value fits the limit");

    let with = normalize(&batch(vec![point(raw, None)], None), &limits, &rules());
    assert!(with.output.points.is_empty());
    assert_eq!(
        with.output.rejected,
        vec![Rejection::LabelValueTooLong {
            len: 47,
            max: stored.len() - 1,
        }]
    );
}

/// A hostname under a rule is stored unchanged (unparsed), but the
/// projection charges `max(47, 19) = 47` for it. OTAP charges a point's
/// labels twice, once for the group's built set and once for the copy the
/// point carries, so the projection grows by `2 * (47 - 19)`.
#[test]
fn otap_projection_charges_max_stored_len_for_a_ruled_value() {
    let decoded = batch(vec![point(HOSTNAME, None)], None);
    let limits = limits();
    let without = project_resolved_label_bytes(&decoded, &limits, &TenantRules::none()).get();
    let with = project_resolved_label_bytes(&decoded, &limits, &rules()).get();
    assert_eq!(with, without + 2 * (47 - HOSTNAME.len()));
    assert_eq!(with, PINNED_OTAP_PROJECTION);

    let result = normalize(&decoded, &limits, &rules());
    assert_eq!(
        result.output.points[0].labels.get(POINT_LABEL),
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

const PINNED_OTAP_PROJECTION: usize = 506;

/// The resource raw-length check charges a ruled hostname at
/// `max(47, 19) = 47`, so a limit of 46 rejects the points under it even
/// though the value is stored unchanged at 19 bytes.
#[test]
fn otap_resource_label_check_charges_max_stored_len() {
    let limits = IngestLimits {
        max_label_value_len: 46,
        ..limits()
    };
    let decoded = batch(vec![point("192.0.2.1", None)], Some(HOSTNAME));
    let without = normalize(&decoded, &limits, &TenantRules::none());
    assert_eq!(without.output.points.len(), 1);
    assert_eq!(
        without.output.points[0].labels.get(RESOURCE_LABEL),
        Some(HOSTNAME)
    );

    let with = normalize(&decoded, &limits, &rules());
    assert!(with.output.points.is_empty());
    assert_eq!(
        with.output.rejected,
        vec![Rejection::Grouped {
            reason: Box::new(Rejection::LabelValueTooLong { len: 47, max: 46 }),
            count: 1,
        }]
    );
}

/// A prefix-preserving rule stores an IPv4 address as the IPv4 ciphertext
/// the prefix epoch key gives it, on the point, the exemplar and the
/// promoted resource label.
#[test]
fn otap_prefix_rule_keeps_the_ipv4_family() {
    let policy = parse_policy_file(
        "[defaults]\nrules = [\n  { key = \"client.ip\", variant = \"prefix\" },\n  { key = \"host.ip\", variant = \"prefix\" },\n]\n",
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

    let (point_ip, exemplar_ip, resource_ip) = ("192.0.2.1", "198.51.100.7", "203.0.113.9");
    let result = normalize(
        &batch(vec![point(point_ip, Some(exemplar_ip))], Some(resource_ip)),
        &limits(),
        &rules,
    );
    assert!(result.output.rejected.is_empty());
    let labels = &result.output.points[0].labels;
    assert_eq!(
        labels.get(POINT_LABEL),
        Some(expected_prefix(point_ip).as_str())
    );
    assert_eq!(
        labels.get(RESOURCE_LABEL),
        Some(expected_prefix(resource_ip).as_str())
    );
    assert_eq!(
        result.exemplars[0].exemplar.filtered_attributes,
        vec![label(POINT_LABEL, &expected_prefix(exemplar_ip))]
    );
    let tally = &result.output.ipcrypt_tally;
    assert_eq!(tally.count(Variant::Prefix, RenderOutcome::Encrypted), 3);
    assert_eq!(
        tally.count(Variant::Deterministic, RenderOutcome::Encrypted),
        0
    );
}

#[test]
fn otap_none_matches_a_resolved_empty_rule_set() {
    let decoded = batch(
        vec![point("192.0.2.1", Some("198.51.100.7"))],
        Some("203.0.113.9"),
    );
    let limits = limits();
    let none = normalize(&decoded, &limits, &TenantRules::none());
    let empty = normalize(&decoded, &limits, &resolved_empty_rules());
    assert_eq!(none, empty);
    assert!(none.output.ipcrypt_tally.is_empty());
    assert_eq!(
        project_resolved_label_bytes(&decoded, &limits, &TenantRules::none()).get(),
        project_resolved_label_bytes(&decoded, &limits, &resolved_empty_rules()).get()
    );
}
