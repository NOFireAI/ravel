use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use axum::routing::get;
use parking_lot::Mutex;

use super::*;

/// A fixed clock so signatures and retention expiry are reproducible.
struct FixedClock(i64);
impl SigningClock for FixedClock {
    fn now_unix_secs(&self) -> i64 {
        self.0
    }
}

// --- SigV4 known-answer test (AWS's published GET Object example) ---
//
// https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
// GET /test.txt from examplebucket, region us-east-1, service s3, with a
// Range header, credentials AKIAIOSFODNN7EXAMPLE /
// wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY, date 20130524T000000Z. AWS
// publishes the signature; recomputing it here pins the signer independently
// of any endpoint.

const KAT_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const KAT_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const KAT_REGION: &str = "us-east-1";
const KAT_AMZ_DATE: &str = "20130524T000000Z";
const KAT_DATE_STAMP: &str = "20130524";
const KAT_UNIX_SECS: i64 = 1_369_353_600;
const KAT_PUBLISHED_SIGNATURE: &str =
    "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

/// The exact canonical request AWS documents for the example, so the byte-flip
/// test below flips a real one.
fn kat_canonical_request() -> (String, String) {
    let headers = vec![
        SignedHeader {
            name: "host".to_string(),
            value: "examplebucket.s3.amazonaws.com".to_string(),
        },
        SignedHeader {
            name: "range".to_string(),
            value: "bytes=0-9".to_string(),
        },
        SignedHeader {
            name: "x-amz-content-sha256".to_string(),
            value: EMPTY_SHA256_HEX.to_string(),
        },
        SignedHeader {
            name: "x-amz-date".to_string(),
            value: KAT_AMZ_DATE.to_string(),
        },
    ];
    canonical_request("GET", "/test.txt", "", &headers, EMPTY_SHA256_HEX)
}

#[test]
fn sigv4_known_answer_matches_aws_published_signature() {
    let (request, signed_headers) = kat_canonical_request();
    assert_eq!(signed_headers, "host;range;x-amz-content-sha256;x-amz-date");
    let scope = format!("{KAT_DATE_STAMP}/{KAT_REGION}/{SERVICE}/aws4_request");
    let sts = string_to_sign(KAT_AMZ_DATE, &scope, &request);
    let sig = signature(KAT_SECRET_KEY, KAT_DATE_STAMP, KAT_REGION, SERVICE, &sts);
    assert_eq!(
        sig, KAT_PUBLISHED_SIGNATURE,
        "signer must reproduce AWS's published SigV4 signature"
    );
}

/// Flipping a single byte of the canonical request changes the signature away
/// from the published value: the signer is not accidentally constant.
#[test]
fn sigv4_known_answer_fails_with_one_byte_flipped() {
    let (request, _) = kat_canonical_request();
    // Flip the last byte of the canonical URI line: "/test.txt" -> "/test.txu".
    let line = "\n/test.txt\n";
    let at = request.find(line).expect("the canonical URI line") + line.len() - 2;
    let mut bytes = request.into_bytes();
    assert_eq!(bytes[at], b't');
    bytes[at] = b'u';
    let tampered = String::from_utf8(bytes).expect("still utf8");
    assert!(tampered.contains("\n/test.txu\n"));

    let scope = format!("{KAT_DATE_STAMP}/{KAT_REGION}/{SERVICE}/aws4_request");
    let sts = string_to_sign(KAT_AMZ_DATE, &scope, &tampered);
    let sig = signature(KAT_SECRET_KEY, KAT_DATE_STAMP, KAT_REGION, SERVICE, &sts);
    assert_ne!(
        sig, KAT_PUBLISHED_SIGNATURE,
        "a flipped canonical-request byte must change the signature"
    );
}

#[test]
fn amz_time_formats_the_kat_instant() {
    let (amz, stamp) = format_amz_time(KAT_UNIX_SECS);
    assert_eq!(amz, "20130524T000000Z");
    assert_eq!(stamp, "20130524");
}

#[test]
fn canonical_query_sorts_and_encodes() {
    let pairs = vec![
        ("versionId".to_string(), "a+b/c".to_string()),
        ("retention".to_string(), String::new()),
    ];
    assert_eq!(canonical_query(&pairs), "retention=&versionId=a%2Bb%2Fc");
}

#[test]
fn iso8601_parses_the_forms_s3_sends() {
    assert_eq!(
        parse_iso8601("2013-05-24T00:00:00Z"),
        Some((KAT_UNIX_SECS, 0))
    );
    assert_eq!(
        parse_iso8601("2013-05-24T00:00:00.250Z"),
        Some((KAT_UNIX_SECS, 250_000_000))
    );
    assert_eq!(
        parse_iso8601("2013-05-24T02:00:00+02:00"),
        Some((KAT_UNIX_SECS, 0))
    );
    for bad in [
        "",
        "2013-05-24",
        "2013-13-01T00:00:00Z",
        "2013-02-30T00:00:00Z",
        "2013-05-24T00:00:00",
        "2013-05-24T00:00:00.Z",
        "not a date at all!!",
    ] {
        assert_eq!(parse_iso8601(bad), None, "{bad:?} must not parse");
    }
}

/// The excerpt is cut at a byte offset in text the endpoint controls, so a
/// multi-byte character straddling that offset must not panic.
#[test]
fn short_excerpt_cuts_on_a_character_boundary() {
    // 'é' is two bytes, so one leading ASCII byte puts a character across
    // byte 200: the cut walks back to 199 and keeps 1 + 99 characters.
    let body = format!("x{}", "é".repeat(150));
    assert!(!body.is_char_boundary(200), "the test body must straddle");
    let excerpt = short_excerpt(&body);
    assert_eq!(excerpt.trim_end_matches('.').len(), 199);
    assert!(excerpt.ends_with("..."));
    assert_eq!(excerpt.trim_end_matches('.').chars().count(), 100);
    assert_eq!(short_excerpt("AccessDenied"), "AccessDenied");
}

// --- Request addressing (object_store's styles) ---

fn target(endpoint: Option<&str>, path_style: bool, key: Option<&str>) -> RequestTarget {
    let query: Vec<(String, String)> = match key {
        Some(_) => vec![
            ("retention".to_string(), String::new()),
            ("versionId".to_string(), "v1".to_string()),
        ],
        None => vec![("versioning".to_string(), String::new())],
    };
    request_target("bkt", "eu-west-1", endpoint, path_style, key, &query)
}

/// Each addressing style produces the URL `object_store` would use for the
/// same configuration. A custom endpoint with virtual-hosted style already
/// names the bucket, so the bucket is not prepended to it.
#[test]
fn request_target_matches_object_store_addressing() {
    let t = target(Some("https://bkt.minio.example:9000"), false, None);
    assert_eq!(t.url, "https://bkt.minio.example:9000/?versioning=");
    assert_eq!(t.host, "bkt.minio.example:9000");
    assert_eq!(t.canonical_uri, "/");

    let t = target(
        Some("https://bkt.minio.example:9000/"),
        false,
        Some("t/a b"),
    );
    assert_eq!(
        t.url,
        "https://bkt.minio.example:9000/t/a%20b?retention=&versionId=v1"
    );
    assert_eq!(t.canonical_uri, "/t/a%20b");

    let t = target(Some("http://minio:9000"), true, Some("t/a"));
    assert_eq!(t.url, "http://minio:9000/bkt/t/a?retention=&versionId=v1");
    assert_eq!(t.host, "minio:9000");
    assert_eq!(t.canonical_uri, "/bkt/t/a");

    let t = target(Some("http://gateway:9000/s3"), true, None);
    assert_eq!(t.url, "http://gateway:9000/s3/bkt?versioning=");
    assert_eq!(t.host, "gateway:9000");
    assert_eq!(t.canonical_uri, "/s3/bkt");

    let t = target(None, false, None);
    assert_eq!(t.url, "https://bkt.s3.eu-west-1.amazonaws.com/?versioning=");
    assert_eq!(t.host, "bkt.s3.eu-west-1.amazonaws.com");

    let t = target(None, true, Some("t/a"));
    assert_eq!(
        t.url,
        "https://s3.eu-west-1.amazonaws.com/bkt/t/a?retention=&versionId=v1"
    );
    assert_eq!(t.host, "s3.eu-west-1.amazonaws.com");
}

// --- XML reader tests (each response shape -> parsed value) ---

#[test]
fn parses_versioning_enabled() {
    let body = br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    assert_eq!(
        parse_versioning(body).expect("parse").status.as_deref(),
        Some("Enabled")
    );
}

#[test]
fn parses_versioning_empty() {
    let body = br#"<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration/>"#;
    assert_eq!(parse_versioning(body).expect("parse").status, None);
}

#[test]
fn parses_lifecycle_rule_fields() {
    let body = br#"<LifecycleConfiguration>
      <Rule><ID>r1</ID><Status>Enabled</Status>
        <Filter><Prefix>t/</Prefix></Filter>
        <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>
        <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
        <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
      </Rule>
    </LifecycleConfiguration>"#;
    let config = parse_lifecycle(body).expect("parse");
    assert_eq!(config.rules.len(), 1);
    let rule = &config.rules[0];
    assert_eq!(rule.id.as_deref(), Some("r1"));
    assert_eq!(rule.status, RuleStatus::Enabled);
    assert_eq!(rule.scope, RuleScope::Prefix("t/".to_string()));
    assert_eq!(rule.noncurrent_days, Some(Days::Value(30)));
    assert_eq!(rule.expired_object_delete_marker, Some(Flag::Value(true)));
    assert_eq!(rule.abort_incomplete_days, Some(Days::Value(7)));
    assert!(!rule.has_transition);
    assert_eq!(rule.expiration_days, None);
    assert_eq!(rule.expiration_date, None);
}

#[test]
fn parses_lifecycle_foreign_transition() {
    let body = br#"<LifecycleConfiguration>
      <Rule><Status>Enabled</Status><Prefix>t/</Prefix>
        <Transition><Days>10</Days><StorageClass>GLACIER</StorageClass></Transition>
      </Rule>
    </LifecycleConfiguration>"#;
    let config = parse_lifecycle(body).expect("parse");
    assert!(config.rules[0].has_transition);
    assert_eq!(config.rules[0].scope, RuleScope::Prefix("t/".to_string()));
}

/// Every lifecycle filter form reads to the scope it means: a plain prefix
/// covers, a tag or object-size bound narrows, and a shape outside the S3
/// grammar is unrecognised rather than read as the whole bucket.
#[test]
fn every_lifecycle_filter_form_is_read() {
    let scope_of = |filter: &str| {
        let body = format!(
            "<LifecycleConfiguration><Rule><Status>Enabled</Status>{filter}\
             <AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation>\
             </AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
        );
        parse_lifecycle(body.as_bytes()).expect("parse").rules[0]
            .scope
            .clone()
    };
    let prefix = |p: &str| RuleScope::Prefix(p.to_string());
    let narrowed = |p: &str, by: &str| RuleScope::Narrowed {
        prefix: p.to_string(),
        by: by.to_string(),
    };
    assert_eq!(scope_of("<Prefix>t/</Prefix>"), prefix("t/"));
    assert_eq!(scope_of("<Prefix></Prefix>"), prefix(""));
    assert_eq!(scope_of("<Filter/>"), prefix(""));
    assert_eq!(scope_of("<Filter></Filter>"), prefix(""));
    assert_eq!(
        scope_of("<Filter><Prefix>sys/</Prefix></Filter>"),
        prefix("sys/")
    );
    assert_eq!(
        scope_of("<Filter><Prefix>a&amp;b</Prefix></Filter>"),
        prefix("a&b")
    );
    assert_eq!(
        scope_of("<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>"),
        narrowed("", "a tag")
    );
    assert_eq!(
        scope_of("<Filter><ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan></Filter>"),
        narrowed("", "ObjectSizeGreaterThan")
    );
    assert_eq!(
        scope_of("<Filter><ObjectSizeLessThan>1024</ObjectSizeLessThan></Filter>"),
        narrowed("", "ObjectSizeLessThan")
    );
    assert_eq!(
        scope_of(
            "<Filter><And><Prefix>t/</Prefix><Tag><Key>a</Key><Value>1</Value></Tag>\
             <Tag><Key>b</Key><Value>2</Value></Tag>\
             <ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></And></Filter>"
        ),
        narrowed("t/", "a tag and ObjectSizeGreaterThan")
    );
    assert_eq!(
        scope_of(
            "<Filter><And><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan>\
             <ObjectSizeLessThan>9</ObjectSizeLessThan></And></Filter>"
        ),
        narrowed("", "ObjectSizeGreaterThan and ObjectSizeLessThan")
    );
    assert_eq!(
        scope_of("<Filter><And><Prefix>t/</Prefix></And></Filter>"),
        prefix("t/")
    );
    for shape in [
        "",
        "<Filter><Frobnicate/></Filter>",
        "<Filter><Prefix>t/</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></Filter>",
        "<Filter><And><Prefix>t/</Prefix><Prefix>sys/</Prefix></And></Filter>",
        "<Filter><And><Frobnicate/></And></Filter>",
        "<Prefix>t/</Prefix><Filter/>",
        "<Filter>text</Filter>",
    ] {
        assert!(
            matches!(scope_of(shape), RuleScope::Unrecognized(_)),
            "{shape:?} must be unrecognised, got {:?}",
            scope_of(shape)
        );
    }
}

#[test]
fn parses_replication_rules() {
    let body =
        br#"<ReplicationConfiguration><Role>arn</Role><Rule><ID>r</ID><Status>Enabled</Status>
      <Priority>1</Priority><Filter><Prefix></Prefix></Filter>
      <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
      <Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination>
    </Rule></ReplicationConfiguration>"#;
    let config = parse_replication(body).expect("parse");
    assert_eq!(
        config.rules,
        vec![ReplicationRule {
            id: Some("r".to_string()),
            status: RuleStatus::Enabled,
            scope: RuleScope::Prefix(String::new()),
            delete_marker_replication: Some(RuleStatus::Enabled),
        }]
    );
}

#[test]
fn parses_object_lock_enabled() {
    let body = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"#;
    assert!(parse_object_lock(body).expect("parse").enabled);
}

#[test]
fn parses_retention_mode_and_date() {
    let body = br#"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"#;
    let config = parse_retention(body).expect("parse");
    assert_eq!(config.mode.as_deref(), Some("COMPLIANCE"));
    assert_eq!(config.retain_until.as_deref(), Some("2030-01-01T00:00:00Z"));
}

#[test]
fn parses_object_versions() {
    let body = br#"<ListVersionsResult>
      <IsTruncated>true</IsTruncated><NextKeyMarker>t/a</NextKeyMarker><NextVersionIdMarker>v0</NextVersionIdMarker>
      <Version><Key>t/a</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest><LastModified>2013-05-01T00:00:00.000Z</LastModified></Version>
      <DeleteMarker><Key>t/b</Key><VersionId>d1</VersionId><IsLatest>true</IsLatest></DeleteMarker>
      <Version><Key>t/a</Key><VersionId>v0</VersionId><IsLatest>false</IsLatest></Version>
    </ListVersionsResult>"#;
    let listing = parse_object_versions(body).expect("parse");
    assert_eq!(
        listing.versions.len(),
        2,
        "a delete marker is not a version"
    );
    assert!(listing.versions[0].is_latest);
    assert_eq!(
        listing.versions[0].last_modified.as_deref(),
        Some("2013-05-01T00:00:00.000Z")
    );
    assert_eq!(listing.versions[1].version_id, "v0");
    assert!(listing.is_truncated);
    assert_eq!(listing.next_key_marker.as_deref(), Some("t/a"));
    assert_eq!(listing.next_version_id_marker.as_deref(), Some("v0"));
    assert!(
        parse_object_versions(
            br#"<ListVersionsResult><Version><IsLatest>maybe</IsLatest></Version></ListVersionsResult>"#
        )
        .is_err()
    );
}

#[test]
fn error_code_is_read_from_the_error_element_only() {
    assert_eq!(
        parse_error_code(
            br#"<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>m</Message></Error>"#
        )
        .as_deref(),
        Some("NoSuchBucket")
    );
    // A code mentioned anywhere but <Error><Code> is not the error code.
    assert_eq!(
        parse_error_code(br#"<Error><Message>NoSuchLifecycleConfiguration</Message></Error>"#),
        None
    );
    assert_eq!(parse_error_code(b"NoSuchLifecycleConfiguration"), None);
    assert_eq!(parse_error_code(b""), None);
}

#[test]
fn malformed_xml_is_a_parse_error() {
    // Unterminated: the body stops inside <Status>, so the truncation is
    // reported rather than the partial "Enabled" being believed.
    let body = br#"<VersioningConfiguration><Status>Enabled"#;
    let detail = parse_versioning(body)
        .expect_err("truncated body must not parse")
        .into_unknown_detail();
    assert!(
        detail.contains("ended inside <Status>"),
        "unexpected detail: {detail}"
    );
    let broken = b"not xml at all";
    let detail = parse_versioning(broken)
        .expect_err("non-XML body must not parse")
        .into_unknown_detail();
    assert!(
        detail.contains("not <VersioningConfiguration> XML"),
        "unexpected detail: {detail}"
    );
    // Wrong root: an <Error> body where a config was expected.
    let wrong = br#"<Error><Code>AccessDenied</Code></Error>"#;
    let detail = parse_versioning(wrong)
        .expect_err("an <Error> body must not parse as a configuration")
        .into_unknown_detail();
    assert!(
        detail.contains("not <VersioningConfiguration> XML"),
        "unexpected detail: {detail}"
    );
    // The expected root nested inside another root is not the expected root.
    assert!(
        parse_versioning(br#"<X><VersioningConfiguration/></X>"#).is_err(),
        "the configuration must be the document root"
    );
    assert!(
        parse_versioning(br#"<VersioningConfiguration>&bogus;</VersioningConfiguration>"#).is_err()
    );
}

/// The truncation check covers every reader, not just the one above.
#[test]
fn every_parser_rejects_a_truncated_body() {
    assert!(parse_lifecycle(br#"<LifecycleConfiguration><Rule><Status>Enabled"#).is_err());
    assert!(
        parse_replication(
            br#"<ReplicationConfiguration><Rule><DeleteMarkerReplication><Status>Enabled"#
        )
        .is_err()
    );
    assert!(parse_object_lock(br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled"#).is_err());
    assert!(parse_retention(br#"<Retention><Mode>COMPLIANCE"#).is_err());
    assert!(parse_object_versions(br#"<ListVersionsResult><Version><Key>a"#).is_err());
}

// --- Lifecycle evaluation ---

const ABORT_7: &str = "<AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>";
const MARKER: &str =
    "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>";

fn noncurrent(days: &str) -> String {
    format!(
        "<NoncurrentVersionExpiration><NoncurrentDays>{days}</NoncurrentDays></NoncurrentVersionExpiration>"
    )
}

fn rule(id: &str, filter: &str, actions: &str) -> String {
    format!("<Rule><ID>{id}</ID><Status>Enabled</Status>{filter}{actions}</Rule>")
}

/// Evaluate `rules` (the inner XML of a lifecycle document) with an expected
/// `E_v` of 30 days.
fn evaluate(rules: &str) -> LifecycleVerdicts {
    let body = format!("<LifecycleConfiguration>{rules}</LifecycleConfiguration>");
    let config = parse_lifecycle(body.as_bytes()).expect("parse");
    lifecycle_conditions(&FetchOutcome::Present(config), Some(30))
}

#[test]
fn compliant_single_rule_passes_every_lifecycle_condition() {
    let v = evaluate(&rule(
        "ravel",
        "<Filter/>",
        &format!("{}{MARKER}{ABORT_7}", noncurrent("30")),
    ));
    for (name, state) in [
        ("noncurrent", &v.noncurrent),
        ("marker", &v.expired_marker),
        ("abort", &v.abort),
        ("rule-scope", &v.rule_scope),
        ("no-foreign", &v.no_foreign),
    ] {
        assert!(state.is_pass(), "{name}: {state:?}");
    }
    assert!(v.notes.abort_rule_covers_data && v.notes.noncurrent_rule_covers_data);
}

/// The reviewer's pin: a noncurrent rule scoped to sys/ does not cover t/,
/// and an abort-only rule on the whole bucket carries neither noncurrent
/// expiration nor delete-marker cleanup, so both conditions fail.
#[test]
fn noncurrent_rule_on_sys_with_whole_bucket_abort_rule_fails() {
    let v = evaluate(&format!(
        "{}{}",
        rule(
            "sys-only",
            "<Filter><Prefix>sys/</Prefix></Filter>",
            &format!("{}{MARKER}", noncurrent("30"))
        ),
        rule("abort-only", "<Filter></Filter>", ABORT_7),
    ));
    assert!(v.noncurrent.is_fail(), "noncurrent: {:?}", v.noncurrent);
    assert!(v.rule_scope.is_fail(), "rule-scope: {:?}", v.rule_scope);
    let detail = v.rule_scope.detail();
    assert!(
        detail.contains("NoncurrentVersionExpiration")
            && detail.contains("ExpiredObjectDeleteMarker")
            && !detail.contains("AbortIncompleteMultipartUpload"),
        "rule-scope must name exactly the uncovered actions: {detail}"
    );
    assert!(v.abort.is_pass(), "abort: {:?}", v.abort);
}

/// Covering rules with different values fail whichever comes first.
#[test]
fn covering_rules_that_disagree_fail_in_either_order() {
    let right = rule(
        "right",
        "<Filter/>",
        &format!("{}{MARKER}{ABORT_7}", noncurrent("30")),
    );
    let wrong = rule("wrong", "<Prefix>t/</Prefix>", &noncurrent("14"));
    for rules in [format!("{right}{wrong}"), format!("{wrong}{right}")] {
        let v = evaluate(&rules);
        let detail = v.noncurrent.detail().to_string();
        assert!(v.noncurrent.is_fail(), "{rules}: {:?}", v.noncurrent);
        assert!(
            detail.contains("NoncurrentDays is 14, expected 30"),
            "detail: {detail}"
        );
    }
    // With no expected value (the server path) disagreement alone still fails.
    let body = format!(
        "<LifecycleConfiguration>{}{}</LifecycleConfiguration>",
        rule("a", "<Filter/>", &noncurrent("30")),
        rule("b", "<Filter/>", &noncurrent("14")),
    );
    let config = parse_lifecycle(body.as_bytes()).expect("parse");
    let v = lifecycle_conditions(&FetchOutcome::Present(config), None);
    assert!(
        v.noncurrent.is_fail() && v.noncurrent.detail().contains("disagree"),
        "{:?}",
        v.noncurrent
    );
}

/// A tag-narrowed rule applies to a subset of t/, so it never proves scope.
#[test]
fn tag_only_filter_does_not_make_rule_scope_pass() {
    let all = format!("{}{MARKER}{ABORT_7}", noncurrent("30"));
    let v = evaluate(&rule(
        "tagged",
        "<Filter><Tag><Key>tier</Key><Value>hot</Value></Tag></Filter>",
        &all,
    ));
    assert!(v.rule_scope.is_fail(), "rule-scope: {:?}", v.rule_scope);
    assert!(v.noncurrent.is_fail(), "noncurrent: {:?}", v.noncurrent);
    assert!(v.abort.is_fail(), "abort: {:?}", v.abort);

    let v = evaluate(&rule(
        "sized",
        "<Filter><And><Prefix>t/</Prefix><ObjectSizeGreaterThan>0</ObjectSizeGreaterThan></And></Filter>",
        &all,
    ));
    assert!(v.rule_scope.is_fail(), "rule-scope: {:?}", v.rule_scope);
}

/// A filter shape the reader does not recognise can make a condition Unknown,
/// never Pass, even beside a rule that would otherwise prove it.
#[test]
fn unrecognised_filter_is_unknown_never_pass() {
    let all = format!("{}{MARKER}{ABORT_7}", noncurrent("30"));
    let v = evaluate(&rule("odd", "<Filter><Frobnicate/></Filter>", &all));
    for state in [&v.noncurrent, &v.expired_marker, &v.abort, &v.rule_scope] {
        assert!(state.is_unknown(), "{state:?}");
    }
    let v = evaluate(&format!(
        "{}{}",
        rule("good", "<Filter/>", &all),
        rule("odd", "<Filter><Frobnicate/></Filter>", &noncurrent("30"))
    ));
    assert!(v.noncurrent.is_unknown(), "{:?}", v.noncurrent);
    assert!(
        v.abort.is_pass(),
        "the odd rule carries no abort: {:?}",
        v.abort
    );
}

/// Rules on narrower t/ prefixes may cover t/ as a union (ADR-1727 decision
/// 3), which the reader does not evaluate: Unknown rather than a false Fail.
#[test]
fn narrower_prefix_rules_are_unknown_not_fail() {
    let all = format!("{}{MARKER}{ABORT_7}", noncurrent("30"));
    let v = evaluate(&format!(
        "{}{}",
        rule("half-a", "<Filter><Prefix>t/0</Prefix></Filter>", &all),
        rule("half-b", "<Filter><Prefix>t/1</Prefix></Filter>", &all),
    ));
    assert!(v.noncurrent.is_unknown(), "{:?}", v.noncurrent);
    assert!(v.rule_scope.is_unknown(), "{:?}", v.rule_scope);
}

#[test]
fn disabled_rules_do_not_count() {
    let body = format!(
        "<LifecycleConfiguration><Rule><Status>Disabled</Status><Filter/>{}{MARKER}{ABORT_7}</Rule>\
         <Rule><Status>Disabled</Status><Filter/><Expiration><Days>1</Days></Expiration></Rule>\
         </LifecycleConfiguration>",
        noncurrent("30")
    );
    let config = parse_lifecycle(body.as_bytes()).expect("parse");
    let v = lifecycle_conditions(&FetchOutcome::Present(config), Some(30));
    assert!(v.noncurrent.is_fail() && v.rule_scope.is_fail());
    assert!(v.no_foreign.is_pass(), "{:?}", v.no_foreign);
}

#[test]
fn abort_longer_than_seven_days_fails_but_is_carried() {
    let v = evaluate(&rule(
        "slow-abort",
        "<Filter/>",
        "<AbortIncompleteMultipartUpload><DaysAfterInitiation>30</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
    ));
    assert!(v.abort.is_fail(), "{:?}", v.abort);
    assert!(v.notes.abort_rule_covers_data);
    assert!(!v.notes.noncurrent_rule_covers_data);
}

/// A Date expiration on a rule over t/ is a foreign rule.
#[test]
fn date_expiration_is_a_foreign_rule() {
    let v = evaluate(&rule(
        "dated",
        "<Filter/>",
        "<Expiration><Date>2030-01-01T00:00:00.000Z</Date></Expiration>",
    ));
    assert!(v.no_foreign.is_fail(), "{:?}", v.no_foreign);
    assert!(v.no_foreign.detail().contains("expiration on date"));

    let v = evaluate(&rule(
        "days",
        "<Filter><Prefix>sys/</Prefix></Filter>",
        "<Expiration><Days>90</Days></Expiration>",
    ));
    assert!(v.no_foreign.is_fail(), "{:?}", v.no_foreign);

    // Scoped entirely elsewhere: not foreign to Ravel.
    let v = evaluate(&rule(
        "elsewhere",
        "<Filter><Prefix>logs/</Prefix></Filter>",
        "<Expiration><Days>1</Days></Expiration>",
    ));
    assert!(v.no_foreign.is_pass(), "{:?}", v.no_foreign);

    // An expiration or action the reader cannot classify is Unknown.
    for actions in [
        "<Expiration><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>",
        "<DelMarkerExpiration><Days>1</Days></DelMarkerExpiration>",
    ] {
        let v = evaluate(&rule("vendor", "<Filter/>", actions));
        assert!(v.no_foreign.is_unknown(), "{actions}: {:?}", v.no_foreign);
    }
}

/// A day count that does not parse makes the condition it feeds Unknown: not
/// Fail, and not read as a missing rule.
#[test]
fn unparseable_numbers_are_unknown() {
    let v = evaluate(&rule(
        "bad",
        "<Filter/>",
        &format!(
            "{}{MARKER}<AbortIncompleteMultipartUpload><DaysAfterInitiation>seven</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
            noncurrent("thirty")
        ),
    ));
    assert!(v.noncurrent.is_unknown(), "{:?}", v.noncurrent);
    assert!(v.abort.is_unknown(), "{:?}", v.abort);
    assert!(v.expired_marker.is_pass(), "{:?}", v.expired_marker);
    // The actions are carried by a covering rule, whatever their values.
    assert!(v.rule_scope.is_pass(), "{:?}", v.rule_scope);

    let v = evaluate(&rule(
        "bad-expiry",
        "<Filter/>",
        "<Expiration><Days>-1</Days></Expiration>",
    ));
    assert!(v.no_foreign.is_unknown(), "{:?}", v.no_foreign);

    let v = evaluate(&rule(
        "bad-flag",
        "<Filter/>",
        "<Expiration><ExpiredObjectDeleteMarker>yes</ExpiredObjectDeleteMarker></Expiration>",
    ));
    assert!(v.expired_marker.is_unknown(), "{:?}", v.expired_marker);
}

#[test]
fn newer_noncurrent_versions_fail_noncurrent_expiration() {
    let v = evaluate(&rule(
        "keep-newer",
        "<Filter/>",
        "<NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays><NewerNoncurrentVersions>5</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
    ));
    assert!(v.noncurrent.is_fail(), "{:?}", v.noncurrent);
}

#[test]
fn absent_lifecycle_fails_sanctioned_rules_and_passes_no_foreign() {
    let v = lifecycle_conditions(
        &FetchOutcome::Absent("no lifecycle (NoSuchLifecycleConfiguration)".to_string()),
        Some(30),
    );
    assert!(v.noncurrent.is_fail() && v.expired_marker.is_fail() && v.abort.is_fail());
    assert!(v.rule_scope.is_fail());
    assert!(v.no_foreign.is_pass());
}

// --- Replication evaluation ---

fn dmr_state(rules: &str) -> ConditionState {
    let body = format!("<ReplicationConfiguration>{rules}</ReplicationConfiguration>");
    delete_marker_replication_state(&parse_replication(body.as_bytes()).expect("parse"))
}

fn replication_rule(status: &str, filter: &str, dmr: &str) -> String {
    format!(
        "<Rule><Status>{status}</Status>{filter}<DeleteMarkerReplication><Status>{dmr}</Status>\
         </DeleteMarkerReplication><Destination><Bucket>b</Bucket></Destination></Rule>"
    )
}

/// The reviewer's pin: a Disabled rule carrying DeleteMarkerReplication
/// Enabled replicates nothing.
#[test]
fn disabled_replication_rule_does_not_pass() {
    let state = dmr_state(&replication_rule("Disabled", "<Filter/>", "Enabled"));
    assert!(state.is_fail(), "{state:?}");
}

#[test]
fn replication_rule_scoped_elsewhere_does_not_pass() {
    let state = dmr_state(&replication_rule(
        "Enabled",
        "<Filter><Prefix>sys/</Prefix></Filter>",
        "Enabled",
    ));
    assert!(state.is_fail(), "{state:?}");
    let state = dmr_state(&replication_rule(
        "Enabled",
        "<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>",
        "Enabled",
    ));
    assert!(state.is_fail(), "{state:?}");
}

#[test]
fn conflicting_covering_replication_rules_fail() {
    let enabled = replication_rule("Enabled", "<Filter/>", "Enabled");
    let disabled = replication_rule(
        "Enabled",
        "<Filter><Prefix>t/</Prefix></Filter>",
        "Disabled",
    );
    for rules in [
        format!("{enabled}{disabled}"),
        format!("{disabled}{enabled}"),
    ] {
        let state = dmr_state(&rules);
        assert!(
            state.is_fail() && state.detail().contains("disagree"),
            "{state:?}"
        );
    }
    assert!(dmr_state(&enabled).is_pass());
    // A rule with an unrecognised filter leaves it Unknown, never Pass.
    let odd = replication_rule("Enabled", "<Filter><Frobnicate/></Filter>", "Disabled");
    assert!(dmr_state(&format!("{enabled}{odd}")).is_unknown());
}

// --- Retention verdicts ---

fn retention(mode: &str, until: &str) -> FetchOutcome<RetentionConfig> {
    FetchOutcome::Present(RetentionConfig {
        mode: Some(mode.to_string()),
        retain_until: Some(until.to_string()),
    })
}

#[test]
fn lapsed_retention_does_not_protect() {
    let now = KAT_UNIX_SECS;
    assert_eq!(
        retention_verdict(&retention("COMPLIANCE", "2030-01-01T00:00:00Z"), now, false),
        SampleVerdict::Protects
    );
    assert!(matches!(
        retention_verdict(&retention("COMPLIANCE", "2012-01-01T00:00:00Z"), now, false),
        SampleVerdict::NotProtecting(_)
    ));
    assert!(matches!(
        retention_verdict(&retention("GOVERNANCE", "2030-01-01T00:00:00Z"), now, false),
        SampleVerdict::NotProtecting(_)
    ));
    assert!(matches!(
        retention_verdict(&retention("COMPLIANCE", "soon"), now, false),
        SampleVerdict::Unknown(_)
    ));
    // Drawn from a listing cut off at the page cap, a lapsed lock is not proof.
    assert!(matches!(
        retention_verdict(&retention("COMPLIANCE", "2012-01-01T00:00:00Z"), now, true),
        SampleVerdict::Unknown(_)
    ));
}

// --- Fake endpoint: signer and reader over real HTTP ---

/// What the fake endpoint answers, given the first key of the query, the raw
/// path, and the raw query.
type Responder = Arc<dyn Fn(&str, &str, &str) -> (StatusCode, String) + Send + Sync>;

#[derive(Default)]
struct SeenRequest {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
}

#[derive(Clone)]
struct FakeState {
    seen: Arc<Mutex<Vec<SeenRequest>>>,
    respond: Responder,
}

async fn fake_handler(
    State(state): State<FakeState>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let query = uri.query().unwrap_or("").to_string();
    let path = uri.path().to_string();
    state.seen.lock().push(SeenRequest {
        method: method.to_string(),
        path: path.clone(),
        query: query.clone(),
        headers: headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or("").to_string(),
                )
            })
            .collect(),
    });
    let subresource = query
        .split('&')
        .next()
        .and_then(|p| p.split('=').next())
        .unwrap_or("");
    let (status, body) = (state.respond)(subresource, &path, &query);
    let mut response = Response::builder().status(status);
    if status.is_redirection() {
        response = response.header("location", "/redirected?versioning=");
    }
    response
        .body(axum::body::Body::from(body))
        .expect("response")
}

/// Stand up the fake, returning its base URL and the recorded-requests handle.
async fn spawn_fake(respond: Responder) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let state = FakeState {
        seen: Arc::clone(&seen),
        respond,
    };
    let app = Router::new()
        .route("/", get(fake_handler))
        .route("/{*rest}", get(fake_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), seen)
}

fn http_client(allow_http: bool) -> reqwest::Client {
    control_plane_http_client(
        Duration::from_secs(5),
        Duration::from_secs(10),
        Duration::from_secs(10),
        allow_http,
    )
    .expect("client")
}

fn test_client_with(
    endpoint: &str,
    token: Option<&str>,
    allow_http: bool,
) -> BucketControlPlaneClient {
    BucketControlPlaneClient::new(
        http_client(allow_http),
        static_credential_provider(KAT_ACCESS_KEY, KAT_SECRET_KEY, token),
        "ravel-test-bucket".to_string(),
        KAT_REGION.to_string(),
        Some(endpoint.to_string()),
        true,
    )
    .with_clock(Arc::new(FixedClock(KAT_UNIX_SECS)))
}

fn test_client(endpoint: &str) -> BucketControlPlaneClient {
    test_client_with(endpoint, None, true)
}

// --- A test-local SigV4 verifier, independent of the module's signer ---

fn local_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn local_sha256_hex(data: &[u8]) -> String {
    local_hex(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

fn local_hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn local_signature(secret: &str, date: &str, region: &str, string_to_sign: &str) -> String {
    let k_date = local_hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = local_hmac(&k_date, region.as_bytes());
    let k_service = local_hmac(&k_region, b"s3");
    let k_signing = local_hmac(&k_service, b"aws4_request");
    local_hex(&local_hmac(&k_signing, string_to_sign.as_bytes()))
}

/// RFC 3986 unreserved characters pass; everything else is `%XX` uppercase.
fn local_encode(raw: &[u8]) -> String {
    raw.iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn local_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Ok(byte) = u8::from_str_radix(input.get(i + 1..i + 3).unwrap_or(""), 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// The test verifier's own signing reproduces AWS's published example, so a
/// match in [`verify_authorization`] is evidence about the client.
#[test]
fn local_verifier_reproduces_the_aws_example() {
    let canonical = format!(
        "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n\
         x-amz-content-sha256:{}\nx-amz-date:{KAT_AMZ_DATE}\n\n\
         host;range;x-amz-content-sha256;x-amz-date\n{}",
        local_sha256_hex(b""),
        local_sha256_hex(b"")
    );
    let sts = format!(
        "AWS4-HMAC-SHA256\n{KAT_AMZ_DATE}\n{KAT_DATE_STAMP}/{KAT_REGION}/s3/aws4_request\n{}",
        local_sha256_hex(canonical.as_bytes())
    );
    assert_eq!(
        local_signature(KAT_SECRET_KEY, KAT_DATE_STAMP, KAT_REGION, &sts),
        KAT_PUBLISHED_SIGNATURE
    );
}

/// What a request must have been signed with, from the test's own values.
struct ExpectedSigning<'a> {
    token: Option<&'a str>,
}

/// Check one received request's SigV4 `Authorization` against the test's own
/// credentials and a canonical request rebuilt with test-local code from what
/// actually arrived on the wire.
fn verify_authorization(req: &SeenRequest, expected: &ExpectedSigning<'_>) {
    assert_eq!(req.method, "GET", "every control-plane request is a GET");
    let header = |name: &str| {
        req.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let auth = header("authorization").expect("authorization header present");
    let fields = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .expect("AWS4-HMAC-SHA256 algorithm");
    let field = |name: &str| {
        fields
            .split(',')
            .map(str::trim)
            .find_map(|part| part.strip_prefix(name))
            .map(str::to_string)
            .unwrap_or_else(|| panic!("{name} missing from Authorization"))
    };
    let credential = field("Credential=");
    let signed_headers = field("SignedHeaders=");
    let carried_signature = field("Signature=");

    assert_eq!(
        credential.split('/').collect::<Vec<_>>(),
        vec![
            KAT_ACCESS_KEY,
            KAT_DATE_STAMP,
            KAT_REGION,
            "s3",
            "aws4_request"
        ],
        "credential scope"
    );

    let names: Vec<&str> = signed_headers.split(';').collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "SignedHeaders must be sorted");
    for required in ["host", "x-amz-content-sha256", "x-amz-date"] {
        assert!(names.contains(&required), "{required} must be signed");
    }
    match expected.token {
        Some(token) => {
            assert_eq!(header("x-amz-security-token").as_deref(), Some(token));
            assert!(
                names.contains(&"x-amz-security-token"),
                "the session token must be signed"
            );
        }
        None => assert!(header("x-amz-security-token").is_none()),
    }

    let payload_hash = local_sha256_hex(b"");
    assert_eq!(
        header("x-amz-content-sha256").as_deref(),
        Some(payload_hash.as_str())
    );
    let amz_date = header("x-amz-date").expect("x-amz-date header");
    assert_eq!(amz_date, KAT_AMZ_DATE);

    // The path on the wire must already be in canonical (single) encoding.
    let reencoded_path: String = req
        .path
        .split('/')
        .map(|segment| local_encode(&local_decode(segment)))
        .collect::<Vec<_>>()
        .join("/");
    assert_eq!(req.path, reencoded_path, "path is canonically encoded");

    let mut pairs: Vec<(String, String)> = req
        .query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (
                local_encode(&local_decode(k)),
                local_encode(&local_decode(v)),
            )
        })
        .collect();
    pairs.sort();
    let canonical_query = pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    assert_eq!(
        req.query, canonical_query,
        "query is sent in canonical form"
    );

    let canonical_headers: String = names
        .iter()
        .map(|name| {
            let value = header(name).unwrap_or_else(|| panic!("signed header {name} not sent"));
            format!(
                "{name}:{}\n",
                value.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        })
        .collect();
    let canonical = format!(
        "GET\n{}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        req.path
    );
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{KAT_DATE_STAMP}/{KAT_REGION}/s3/aws4_request\n{}",
        local_sha256_hex(canonical.as_bytes())
    );
    assert_eq!(
        local_signature(KAT_SECRET_KEY, KAT_DATE_STAMP, KAT_REGION, &sts),
        carried_signature,
        "independently recomputed signature must match the Authorization header"
    );
}

const UNSIGNED_TOKEN: ExpectedSigning<'static> = ExpectedSigning { token: None };

#[tokio::test]
async fn versioning_get_signs_and_parses_over_http() {
    let respond: Responder = Arc::new(|sub, _path, _query| {
        assert_eq!(sub, "versioning");
        (
            StatusCode::OK,
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                .to_string(),
        )
    });
    let (base, seen) = spawn_fake(respond).await;
    let outcome = test_client(&base).fetch_versioning().await;
    match outcome {
        FetchOutcome::Present(config) => assert_eq!(config.status.as_deref(), Some("Enabled")),
        other => panic!("expected Present, got {other:?}"),
    }
    let requests = seen.lock();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/ravel-test-bucket");
    assert_eq!(requests[0].query, "versioning=");
    verify_authorization(&requests[0], &UNSIGNED_TOKEN);
}

/// A session credential's token rides on the request and is covered by the
/// signature.
#[tokio::test]
async fn session_token_is_sent_and_signed() {
    const TOKEN: &str = "FwoGZXIvYXdzEXAMPLE/session+token==";
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::OK,
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                .to_string(),
        )
    });
    let (base, seen) = spawn_fake(respond).await;
    let client = test_client_with(&base, Some(TOKEN), true);
    assert!(matches!(
        client.fetch_versioning().await,
        FetchOutcome::Present(_)
    ));
    let requests = seen.lock();
    assert_eq!(requests.len(), 1);
    verify_authorization(&requests[0], &ExpectedSigning { token: Some(TOKEN) });
}

#[tokio::test]
async fn retention_get_with_version_id_over_http() {
    let respond: Responder = Arc::new(|sub, path, _query| {
        assert_eq!(sub, "retention");
        assert_eq!(path, "/ravel-test-bucket/t/a");
        (
            StatusCode::OK,
            "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"
                .to_string(),
        )
    });
    let (base, seen) = spawn_fake(respond).await;
    match test_client(&base).fetch_retention("t/a", "v1+/x").await {
        FetchOutcome::Present(config) => assert_eq!(config.mode.as_deref(), Some("COMPLIANCE")),
        other => panic!("expected Present, got {other:?}"),
    }
    let requests = seen.lock();
    assert_eq!(requests[0].query, "retention=&versionId=v1%2B%2Fx");
    verify_authorization(&requests[0], &UNSIGNED_TOKEN);
}

#[tokio::test]
async fn access_denied_is_unknown_not_fail() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>".to_string(),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    match test_client(&base).fetch_lifecycle().await {
        FetchOutcome::Unknown(detail) => assert!(detail.contains("AccessDenied"), "{detail}"),
        other => panic!("expected Unknown, got {other:?}"),
    }
}

/// An S3 `SignatureDoesNotMatch` body echoes the canonical request and the
/// string to sign; no part of it but the error code may reach a detail.
#[tokio::test]
async fn error_details_carry_only_the_error_code() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::FORBIDDEN,
            "<Error><Code>SignatureDoesNotMatch</Code><AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId>\
             <CanonicalRequest>x-amz-security-token:SECRET-TOKEN</CanonicalRequest></Error>"
                .to_string(),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    for entry in &report.conditions {
        let detail = entry.state.detail();
        assert!(
            !detail.contains("SECRET-TOKEN") && !detail.contains(KAT_ACCESS_KEY),
            "{}: {detail}",
            entry.id.id()
        );
    }
}

fn lifecycle_404(code_body: &'static str) -> Responder {
    Arc::new(move |sub, _path, _query| match sub {
        "lifecycle" => (StatusCode::NOT_FOUND, code_body.to_string()),
        _ => (
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code></Error>".to_string(),
        ),
    })
}

const LIFECYCLE_IDS: [ProtectionConditionId; 5] = [
    ProtectionConditionId::NoncurrentExpiration,
    ProtectionConditionId::ExpiredDeleteMarker,
    ProtectionConditionId::AbortMultipart,
    ProtectionConditionId::RuleScope,
    ProtectionConditionId::NoForeignRule,
];

/// A 404 carrying `NoSuchLifecycleConfiguration` is proof of absence: the
/// sanctioned-rule conditions fail and `no-foreign-rule` passes. A bare 404
/// proves nothing, so every lifecycle condition stays Unknown.
#[tokio::test]
async fn not_configured_code_is_absent_then_fail_and_bare_404_is_unknown() {
    let (base, _seen) = spawn_fake(lifecycle_404(
        "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>",
    ))
    .await;
    let report = test_client(&base).report(&full_params()).await;
    for id in &LIFECYCLE_IDS[..4] {
        let state = report.state(*id).expect("present");
        assert!(state.is_fail(), "{}: {state:?}", id.id());
        assert!(
            state.detail().contains("NoSuchLifecycleConfiguration"),
            "{}: {state:?}",
            id.id()
        );
    }
    assert!(
        report
            .state(ProtectionConditionId::NoForeignRule)
            .expect("present")
            .is_pass()
    );

    for bare in ["", "not found", "<Error><Code>NoSuchKey</Code></Error>"] {
        let respond: Responder = Arc::new(move |sub, _path, _query| match sub {
            "lifecycle" => (StatusCode::NOT_FOUND, bare.to_string()),
            _ => (StatusCode::FORBIDDEN, String::new()),
        });
        let (base, _seen) = spawn_fake(respond).await;
        let report = test_client(&base).report(&full_params()).await;
        for id in LIFECYCLE_IDS {
            let state = report.state(id).expect("present");
            assert!(state.is_unknown(), "{bare:?} {}: {state:?}", id.id());
        }
    }
}

/// A not-configured code on any status but 404 is not proof either.
#[tokio::test]
async fn not_configured_code_on_another_status_is_unknown() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::BAD_REQUEST,
            "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>".to_string(),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    for id in LIFECYCLE_IDS {
        assert!(report.state(id).expect("present").is_unknown());
    }
}

/// `NoSuchBucket` on every call proves nothing about any condition.
#[tokio::test]
async fn no_such_bucket_is_unknown_on_every_condition_over_http() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchBucket</Code><BucketName>ravel-test-bucket</BucketName></Error>"
                .to_string(),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    for id in ProtectionConditionId::ALL {
        let state = report.state(id).expect("every id present");
        assert!(state.is_unknown(), "{} is {state:?}", id.id());
    }
    assert_eq!(report.failed_count(), 0);
}

#[tokio::test]
async fn malformed_body_is_unknown() {
    let respond: Responder =
        Arc::new(|_sub, _path, _query| (StatusCode::OK, "this is not xml".to_string()));
    let (base, _seen) = spawn_fake(respond).await;
    assert!(matches!(
        test_client(&base).fetch_lifecycle().await,
        FetchOutcome::Unknown(_)
    ));
}

/// A redirect is never followed: the Authorization header and session token
/// stay with the configured endpoint, and the result is Unknown.
#[tokio::test]
async fn redirect_is_unknown_and_not_followed() {
    let respond: Responder = Arc::new(|_sub, path, _query| {
        if path == "/redirected" {
            (
                StatusCode::OK,
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                    .to_string(),
            )
        } else {
            (StatusCode::FOUND, String::new())
        }
    });
    let (base, seen) = spawn_fake(respond).await;
    match test_client(&base).fetch_versioning().await {
        FetchOutcome::Unknown(detail) => assert!(detail.contains("redirect"), "{detail}"),
        other => panic!("expected Unknown, got {other:?}"),
    }
    assert_eq!(
        seen.lock().len(),
        1,
        "the redirect target must not be requested"
    );
}

/// Without `allow_http` the client refuses a plain-HTTP endpoint before sending.
#[tokio::test]
async fn plain_http_is_refused_unless_allowed() {
    let respond: Responder = Arc::new(|_sub, _path, _query| (StatusCode::OK, String::new()));
    let (base, seen) = spawn_fake(respond).await;
    let client = test_client_with(&base, None, false);
    assert!(matches!(
        client.fetch_versioning().await,
        FetchOutcome::Unknown(_)
    ));
    assert!(
        seen.lock().is_empty(),
        "no request may leave over plain HTTP"
    );
}

#[tokio::test]
async fn oversized_body_is_unknown() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        let filler = "x".repeat(MAX_BODY_BYTES);
        (
            StatusCode::OK,
            format!(
                "<VersioningConfiguration><Status>Enabled</Status>{filler}</VersioningConfiguration>"
            ),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    match test_client(&base).fetch_versioning().await {
        FetchOutcome::Unknown(detail) => assert!(detail.contains("exceeds"), "{detail}"),
        other => panic!("expected Unknown, got {other:?}"),
    }
}

// --- Whole-report tests over HTTP ---

/// Params that put every condition in play, including the retention sampling
/// the server path leaves off (ADR-1727 decision 5).
fn full_params() -> BucketProtectionParams {
    BucketProtectionParams {
        expected_noncurrent_days: Some(30),
        sample_object_retention: true,
        protected_retention_prefixes: vec!["t/".to_string()],
    }
}

const COMPLIANT_LIFECYCLE: &str = r#"<LifecycleConfiguration><Rule><Status>Enabled</Status>
      <Filter><Prefix></Prefix></Filter>
      <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>
      <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>
      <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
    </Rule></LifecycleConfiguration>"#;
const COMPLIANT_RETENTION: &str = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";

/// A responder for a compliant bucket whose `?versions` listing and
/// `?retention` answers come from the two closures.
fn bucket(
    listing: impl Fn(&str) -> (StatusCode, String) + Send + Sync + 'static,
    retention: impl Fn(&str) -> (StatusCode, String) + Send + Sync + 'static,
) -> Responder {
    Arc::new(move |sub, path, query| {
        let ok = |body: &str| (StatusCode::OK, body.to_string());
        // `sub` is the first key of the canonical (sorted) query, so the
        // `?versions` listing arrives keyed by `max-keys` (or `key-marker` on a
        // later page).
        match sub {
            "versioning" => {
                ok("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
            }
            "lifecycle" => ok(COMPLIANT_LIFECYCLE),
            "replication" => ok(
                "<ReplicationConfiguration><Rule><Status>Enabled</Status><Filter/><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication></Rule></ReplicationConfiguration>",
            ),
            "object-lock" => ok(
                "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
            ),
            "max-keys" | "key-marker" => listing(query),
            "retention" => retention(path),
            other => panic!("unexpected subresource {other}"),
        }
    })
}

fn version(key: &str, id: &str, latest: bool, modified: &str) -> String {
    format!(
        "<Version><Key>{key}</Key><VersionId>{id}</VersionId><IsLatest>{latest}</IsLatest>\
         <LastModified>{modified}</LastModified></Version>"
    )
}

fn listing_body(versions: &[String]) -> (StatusCode, String) {
    (
        StatusCode::OK,
        format!(
            "<ListVersionsResult>{}</ListVersionsResult>",
            versions.concat()
        ),
    )
}

fn retention_paths(seen: &Mutex<Vec<SeenRequest>>) -> Vec<String> {
    seen.lock()
        .iter()
        .filter(|r| r.query.starts_with("retention="))
        .map(|r| format!("{}?{}", r.path, r.query))
        .collect()
}

/// The whole report over HTTP: a compliant bucket yields `Pass` on all nine
/// conditions, and every request is a signed GET.
#[tokio::test]
async fn compliant_bucket_reports_every_condition_pass_over_http() {
    let respond = bucket(
        |_query| {
            listing_body(&[
                version("t/a/1.rseg", "v1", true, "2013-05-01T00:00:00.000Z"),
                version("t/a/1.rseg", "v0", false, "2013-04-01T00:00:00.000Z"),
            ])
        },
        |_path| (StatusCode::OK, COMPLIANT_RETENTION.to_string()),
    );
    let (base, seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;

    for id in ProtectionConditionId::ALL {
        let state = report.state(id).expect("every id present");
        assert!(state.is_pass(), "{} is {state:?}", id.id());
    }
    let requests = seen.lock();
    // versioning, lifecycle, replication, object-lock, the versions listing,
    // and one retention read per sampled version.
    assert_eq!(requests.len(), 7);
    for req in requests.iter() {
        verify_authorization(req, &UNSIGNED_TOKEN);
    }
}

/// The newest current version per family is sampled, not the first current
/// key in listing order.
#[tokio::test]
async fn newest_current_object_is_sampled() {
    let respond = bucket(
        |_query| {
            listing_body(&[
                version("t/a", "a1", true, "2013-01-01T00:00:00Z"),
                version("t/b", "b1", true, "2013-05-01T00:00:00Z"),
                version("t/b", "b0", false, "2013-04-01T00:00:00Z"),
            ])
        },
        |path| {
            if path.ends_with("/t/a") {
                (
                    StatusCode::OK,
                    "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2012-01-01T00:00:00Z</RetainUntilDate></Retention>"
                        .to_string(),
                )
            } else {
                (StatusCode::OK, COMPLIANT_RETENTION.to_string())
            }
        },
    );
    let (base, seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    let state = report
        .state(ProtectionConditionId::ObjectRetention)
        .expect("present");
    assert!(state.is_pass(), "{state:?}");
    assert_eq!(
        retention_paths(&seen),
        vec![
            "/ravel-test-bucket/t/b?retention=&versionId=b1".to_string(),
            "/ravel-test-bucket/t/b?retention=&versionId=b0".to_string(),
        ]
    );
}

/// The listing is followed past its first page, so the newest version on a
/// later page is the one sampled.
#[tokio::test]
async fn newest_object_on_a_later_page_is_sampled() {
    let respond = bucket(
        |query| {
            if query.starts_with("key-marker=t%2Fa") {
                listing_body(&[
                    version("t/z", "z1", true, "2013-05-20T00:00:00Z"),
                    version("t/z", "z0", false, "2013-05-19T00:00:00Z"),
                ])
            } else {
                (
                    StatusCode::OK,
                    format!(
                        "<ListVersionsResult><IsTruncated>true</IsTruncated>\
                         <NextKeyMarker>t/a</NextKeyMarker><NextVersionIdMarker>a1</NextVersionIdMarker>{}\
                         </ListVersionsResult>",
                        version("t/a", "a1", true, "2013-01-01T00:00:00Z")
                    ),
                )
            }
        },
        |_path| (StatusCode::OK, COMPLIANT_RETENTION.to_string()),
    );
    let (base, seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    assert!(
        report
            .state(ProtectionConditionId::ObjectRetention)
            .expect("present")
            .is_pass()
    );
    assert_eq!(
        retention_paths(&seen),
        vec![
            "/ravel-test-bucket/t/z?retention=&versionId=z1".to_string(),
            "/ravel-test-bucket/t/z?retention=&versionId=z0".to_string(),
        ]
    );
}

async fn retention_state(
    respond: Responder,
    prefixes: &[&str],
) -> (ConditionState, Arc<Mutex<Vec<SeenRequest>>>) {
    let (base, seen) = spawn_fake(respond).await;
    let params = BucketProtectionParams {
        protected_retention_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
        ..full_params()
    };
    let report = test_client(&base).report(&params).await;
    let state = report
        .state(ProtectionConditionId::ObjectRetention)
        .expect("present")
        .clone();
    (state, seen)
}

/// A compliance lock whose RetainUntilDate has passed no longer protects.
#[tokio::test]
async fn lapsed_retention_fails_object_retention_over_http() {
    let respond = bucket(
        |_query| {
            listing_body(&[
                version("t/a", "a1", true, "2013-05-01T00:00:00Z"),
                version("t/a", "a0", false, "2013-04-01T00:00:00Z"),
            ])
        },
        |_path| {
            (
                StatusCode::OK,
                "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2013-05-23T00:00:00Z</RetainUntilDate></Retention>"
                    .to_string(),
            )
        },
    );
    let (state, _seen) = retention_state(respond, &["t/"]).await;
    assert!(state.is_fail(), "{state:?}");
    assert!(state.detail().contains("lapsed"), "{state:?}");
}

/// A family with no current version, or whose listing fails, is Unknown for
/// that family rather than skipped.
#[tokio::test]
async fn family_without_a_current_version_or_listing_is_unknown() {
    let respond = bucket(
        |query| {
            if query.contains("prefix=sys%2F") {
                (
                    StatusCode::NOT_FOUND,
                    "<Error><Code>NoSuchBucket</Code></Error>".to_string(),
                )
            } else if query.contains("prefix=t%2Fonly-old%2F") {
                listing_body(&[version("t/only-old/x", "x0", false, "2013-04-01T00:00:00Z")])
            } else {
                listing_body(&[
                    version("t/a", "a1", true, "2013-05-01T00:00:00Z"),
                    version("t/a", "a0", false, "2013-04-01T00:00:00Z"),
                ])
            }
        },
        |_path| (StatusCode::OK, COMPLIANT_RETENTION.to_string()),
    );
    let (state, _seen) = retention_state(Arc::clone(&respond), &["t/", "sys/"]).await;
    assert!(state.is_unknown(), "listing absent: {state:?}");
    assert!(state.detail().contains("sys/"), "{state:?}");

    let (state, _seen) = retention_state(respond, &["t/", "t/only-old/"]).await;
    assert!(state.is_unknown(), "no current version: {state:?}");
    assert!(
        state.detail().contains("no current object version"),
        "{state:?}"
    );
}

/// A version that vanished between the listing and the retention GET is
/// Unknown for that sample, not Fail; only the call's own not-configured code
/// means "no retention".
#[tokio::test]
async fn vanished_version_is_unknown_and_missing_lock_is_fail() {
    let listing = |_query: &str| {
        listing_body(&[
            version("t/a", "a1", true, "2013-05-01T00:00:00Z"),
            version("t/a", "a0", false, "2013-04-01T00:00:00Z"),
        ])
    };
    for code in ["NoSuchVersion", "NoSuchKey"] {
        let respond = bucket(listing, move |_path| {
            (
                StatusCode::NOT_FOUND,
                format!("<Error><Code>{code}</Code></Error>"),
            )
        });
        let (state, _seen) = retention_state(respond, &["t/"]).await;
        assert!(state.is_unknown(), "{code}: {state:?}");
    }
    let respond = bucket(listing, |_path| {
        (
            StatusCode::NOT_FOUND,
            "<Error><Code>NoSuchObjectLockConfiguration</Code></Error>".to_string(),
        )
    });
    let (state, _seen) = retention_state(respond, &["t/"]).await;
    assert!(state.is_fail(), "{state:?}");
}

/// A credential that cannot read the control plane reports `Unknown` on every
/// condition, never `Fail` (ADR-1727 decision 3).
#[tokio::test]
async fn access_denied_reports_every_condition_unknown_over_http() {
    let respond: Responder = Arc::new(|_sub, _path, _query| {
        (
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code></Error>".to_string(),
        )
    });
    let (base, _seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    for id in ProtectionConditionId::ALL {
        let state = report.state(id).expect("every id present");
        assert!(state.is_unknown(), "{} is {state:?}", id.id());
    }
    assert_eq!(report.failed_count(), 0);
    assert_eq!(report.unknown_count(), ProtectionConditionId::ALL.len());
}

/// A 200 whose body the reader cannot parse is the other `Unknown` source,
/// and it must not reach `Fail` through the assembled report either.
#[tokio::test]
async fn malformed_bodies_report_every_condition_unknown_over_http() {
    let respond: Responder =
        Arc::new(|_sub, _path, _query| (StatusCode::OK, "<not-the-expected-root/>".to_string()));
    let (base, _seen) = spawn_fake(respond).await;
    let report = test_client(&base).report(&full_params()).await;
    for id in ProtectionConditionId::ALL {
        let state = report.state(id).expect("every id present");
        assert!(state.is_unknown(), "{} is {state:?}", id.id());
    }
    assert_eq!(report.failed_count(), 0);
}
