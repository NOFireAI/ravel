//! Read-only bucket protection control plane (ADR-1727 decision 1).
//!
//! Ravel's deletion, retention, and erasure bounds rest on bucket configuration
//! Ravel does not own (docs/object-store-contract.md, "Required bucket
//! configuration"). `object_store` 0.14 exposes no query for any of it, so this
//! module signs its own read-only SigV4 `GET`s over the `reqwest` client the
//! crate already depends on: `GET ?versioning`, `?lifecycle`, `?replication`,
//! `?object-lock`, and `?retention&versionId=` on sampled keys (plus a
//! `?versions` listing to sample from). Signing uses `ring`'s HMAC-SHA256 and
//! SHA-256; responses are read with `quick-xml` 0.41.
//!
//! This is **not** the "second, direct-SDK side channel" ADR-0042 rejected: that
//! rejection is about a second *write* path that would set Object Lock retention
//! outside `object_store`'s retry and error mapping. Every request here is a
//! `GET`; the module has no write and no way to add one without a further ADR.
//! Credentials come from the same provider `S3Store` already holds (static,
//! session, file, or instance role), so there is no second credential path and
//! `S3Config`'s "no credential-chain magic" rule still holds.
//!
//! No credential or signature bytes are ever logged: errors carry HTTP status
//! and a short body excerpt, never a header value.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::aws::{AwsCredential, AwsCredentialProvider};
use quick_xml::Reader;
use quick_xml::events::Event;

use crate::conformance::{
    BucketProtectionParams, BucketProtectionReport, ConditionState, ProtectionConditionId,
};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";
/// SHA-256 of the empty body: every request here is a bodyless `GET`.
const EMPTY_SHA256_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Wall-clock seam for SigV4's request timestamp, mirroring
/// [`super::instance_role`]'s `WallClock`: SigV4 requires a timestamp within a
/// few minutes of the endpoint's clock, which is inherently "now", but a fixed
/// clock makes the signer's tests deterministic (the repo's no-`SystemTime::now`
/// rule).
pub(crate) trait SigningClock: Send + Sync {
    fn now_unix_secs(&self) -> i64;
}

/// The one sanctioned `SystemTime::now()` for this module, isolated behind the
/// seam exactly as `super::instance_role::SystemTimeClock` isolates it there.
#[derive(Debug, Default)]
pub(crate) struct SystemSigningClock;

impl SigningClock for SystemSigningClock {
    fn now_unix_secs(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            Err(_) => 0,
        }
    }
}

// --- Hashing and hex (ring) ---

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    to_hex(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> ring::hmac::Tag {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data)
}

fn signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> ring::hmac::Tag {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

// --- AWS URI encoding (SigV4 canonical form) ---

/// AWS SigV4 percent-encoding: unreserved (`A-Za-z0-9-._~`) pass through, `/`
/// passes only when `encode_slash` is false (canonical URI path), every other
/// byte becomes `%XX` with uppercase hex.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'.' | b'_' | b'~')
            || (byte == b'/' && !encode_slash);
        if keep {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

// --- SigV4 canonicalization (pure, independently tested against AWS's KAT) ---

/// A header to sign: lowercase name plus value (already the value that will ride
/// on the wire).
#[derive(Debug, Clone)]
pub(crate) struct SignedHeader {
    pub name: String,
    pub value: String,
}

/// Build the canonical query string from key/value pairs (ADR-1727 decision 1's
/// subresource queries). Sorted by encoded key, each `enc(k)=enc(v)`, empty
/// value rendered as `k=`. Encoding matches what the request URL carries, so the
/// signature covers exactly the bytes sent.
pub(crate) fn canonical_query(pairs: &[(String, String)]) -> String {
    let mut encoded: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The canonical request and its `SignedHeaders` list (AWS SigV4). `headers`
/// need not be sorted; this sorts by lowercase name and normalizes each value's
/// internal whitespace.
pub(crate) fn canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    headers: &[SignedHeader],
    payload_hash: &str,
) -> (String, String) {
    let mut sorted: Vec<(&str, String)> = headers
        .iter()
        .map(|h| (h.name.as_str(), normalize_ws(&h.value)))
        .collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let signed_headers = sorted
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = sorted
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();

    let request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    (request, signed_headers)
}

/// Collapse runs of ASCII whitespace to a single space and trim the ends (AWS
/// canonical header-value rule).
fn normalize_ws(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hex(sha256(canonical_request))}`.
pub(crate) fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// The hex signature for a string-to-sign, given the credential's secret and the
/// scope's date/region.
pub(crate) fn signature(
    secret: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let key = signing_key(secret, date_stamp, region, service);
    to_hex(hmac_sha256(key.as_ref(), string_to_sign.as_bytes()).as_ref())
}

/// The two SigV4 timestamps: `X-Amz-Date` (`YYYYMMDDTHHMMSSZ`) and the scope
/// datestamp (`YYYYMMDD`), from Unix seconds.
pub(crate) fn format_amz_time(unix_secs: i64) -> (String, String) {
    let days = unix_secs.div_euclid(86_400);
    let secs = unix_secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    let date_stamp = format!("{year:04}{month:02}{day:02}");
    let amz_date = format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z");
    (amz_date, date_stamp)
}

/// Civil date from days since the Unix epoch (Howard Hinnant's algorithm, the
/// inverse of `super::http_date`'s `days_from_civil`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

// --- Request target (URL, host, canonical path) ---

struct RequestTarget {
    /// Full URL for reqwest; every character is URL-safe (unreserved or `%XX`),
    /// so `reqwest`/`url` parse it without re-encoding the query we signed.
    url: String,
    /// Authority (host, and port when non-default) for the `Host` header and for
    /// the signed `host` value.
    host: String,
    /// Canonical URI (single-encoded path) for the canonical request.
    canonical_uri: String,
}

/// Compute the request target for a bucket-subresource or object-subresource
/// `GET`. `object_key` `None` targets the bucket; `Some(key)` targets an object.
fn request_target(
    bucket: &str,
    region: &str,
    endpoint: Option<&str>,
    force_path_style: bool,
    object_key: Option<&str>,
    query_pairs: &[(String, String)],
) -> RequestTarget {
    let key_path: String = object_key
        .map(|key| {
            let encoded = key
                .split('/')
                .map(|segment| uri_encode(segment, true))
                .collect::<Vec<_>>()
                .join("/");
            format!("/{encoded}")
        })
        .unwrap_or_default();

    let (scheme, host, path) = match endpoint {
        Some(endpoint) => {
            let trimmed = endpoint.trim_end_matches('/');
            let (scheme, authority) = split_scheme(trimmed);
            if force_path_style {
                (
                    scheme,
                    authority.to_string(),
                    format!("/{bucket}{key_path}"),
                )
            } else {
                (scheme, format!("{bucket}.{authority}"), key_path.clone())
            }
        }
        None => {
            let base = format!("s3.{region}.amazonaws.com");
            if force_path_style {
                ("https", base, format!("/{bucket}{key_path}"))
            } else {
                ("https", format!("{bucket}.{base}"), key_path.clone())
            }
        }
    };

    // The canonical/URL path is always non-empty (at least "/").
    let path = if path.is_empty() {
        "/".to_string()
    } else {
        path
    };
    let query = canonical_query(query_pairs);
    let url = if query.is_empty() {
        format!("{scheme}://{host}{path}")
    } else {
        format!("{scheme}://{host}{path}?{query}")
    };
    RequestTarget {
        url,
        host,
        canonical_uri: path,
    }
}

/// Split `scheme://authority...` into (`"http"`/`"https"`, authority-and-rest).
/// A value with no scheme is treated as an `https` authority.
fn split_scheme(endpoint: &str) -> (&'static str, &str) {
    if let Some(rest) = endpoint.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        ("http", rest)
    } else {
        ("https", endpoint)
    }
}

// --- Fetch outcomes and errors ---

/// The outcome of fetching one bucket subresource.
#[derive(Debug, Clone)]
pub(crate) enum FetchOutcome<T> {
    /// The configuration was read and parsed.
    Present(T),
    /// The endpoint reported no such configuration (a 404 `NoSuch*`): the
    /// configuration is affirmatively absent, so the derived condition is a
    /// `Fail`, not `Unknown`. Carries a human-readable reason.
    Absent(String),
    /// The configuration could not be determined (access denied, transport
    /// failure, an unparseable body, or an unexpected status): the derived
    /// condition is `Unknown`, never `Fail`.
    Unknown(String),
}

/// A control-plane request failure, classified so the caller can tell an
/// affirmative "not configured" (a `Fail`) from a "could not tell" (an
/// `Unknown`).
#[derive(Debug)]
pub(crate) enum ControlPlaneError {
    /// 403: the credential cannot read this configuration -> `Unknown`.
    AccessDenied(String),
    /// 404 `NoSuch*`/`*NotFoundError`: affirmatively absent -> `Fail`.
    NotConfigured(String),
    /// Transport failure (connect, timeout, dropped connection) -> `Unknown`.
    Transport(String),
    /// The response body could not be parsed -> `Unknown`.
    Parse(String),
    /// Any other non-success status -> `Unknown`.
    UnexpectedStatus(u16),
}

impl ControlPlaneError {
    fn into_unknown_detail(self) -> String {
        match self {
            ControlPlaneError::AccessDenied(msg) => format!("access denied: {msg}"),
            ControlPlaneError::NotConfigured(msg) => msg,
            ControlPlaneError::Transport(msg) => format!("transport error: {msg}"),
            ControlPlaneError::Parse(msg) => format!("could not parse response: {msg}"),
            ControlPlaneError::UnexpectedStatus(status) => {
                format!("unexpected HTTP status {status}")
            }
        }
    }
}

// --- Parsed configurations ---

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VersioningConfig {
    /// `Some("Enabled")`, `Some("Suspended")`, or `None` (never enabled).
    pub status: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LifecycleRule {
    pub status: String,
    pub prefix: Option<String>,
    pub noncurrent_days: Option<u32>,
    pub expired_object_delete_marker: Option<bool>,
    pub abort_incomplete_days: Option<u32>,
    pub expiration_days: Option<u32>,
    pub has_transition: bool,
}

impl LifecycleRule {
    fn enabled(&self) -> bool {
        self.status.eq_ignore_ascii_case("Enabled")
    }

    fn carries_sanctioned(&self) -> bool {
        self.noncurrent_days.is_some()
            || self.abort_incomplete_days.is_some()
            || self.expired_object_delete_marker == Some(true)
    }

    fn carries_foreign(&self) -> bool {
        self.has_transition || self.expiration_days.is_some()
    }

    fn effective_prefix(&self) -> &str {
        self.prefix.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplicationConfig {
    pub delete_marker_replication_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectLockConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RetentionConfig {
    /// `Some("COMPLIANCE")`, `Some("GOVERNANCE")`, or `None`.
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectVersionListing {
    pub versions: Vec<ObjectVersion>,
}

// --- XML parsers (quick-xml 0.41, event-based, no serde) ---

fn xml_reader(body: &[u8]) -> Reader<&[u8]> {
    let mut reader = Reader::from_reader(body);
    // Default well-formedness checks stay on (check_end_names): an unterminated
    // or mismatched body errors rather than parsing to a misleading partial, so
    // a malformed response maps to Unknown, never Fail (ADR-1727 decision 3).
    reader.config_mut().trim_text(true);
    reader
}

/// Confirm the parse reached the expected root element and closed every element
/// it opened. A body that is not the S3 XML this call returns (garbage, an
/// unexpected shape) and a body that stops mid-element are both parse errors, so
/// the derived condition is Unknown rather than a wrong Fail. quick-xml's
/// `check_end_names` rejects a *mismatched* end tag but reports plain EOF for a
/// truncated body, which would otherwise yield a misleading partial parse, so
/// `open` (the elements still unclosed at EOF) is checked here too.
fn require_well_formed(
    saw_root: bool,
    open: &[Vec<u8>],
    expected: &str,
) -> Result<(), ControlPlaneError> {
    if let Some(name) = open.last() {
        return Err(ControlPlaneError::Parse(format!(
            "response ended inside <{}>",
            String::from_utf8_lossy(name)
        )));
    }
    if saw_root {
        Ok(())
    } else {
        Err(ControlPlaneError::Parse(format!(
            "response is not <{expected}> XML"
        )))
    }
}

fn local(name: &[u8]) -> Vec<u8> {
    match name.iter().rposition(|&b| b == b':') {
        Some(idx) => name[idx + 1..].to_vec(),
        None => name.to_vec(),
    }
}

fn decode_text(text: &quick_xml::events::BytesText) -> Result<String, ControlPlaneError> {
    text.decode()
        .map(|cow| cow.into_owned())
        .map_err(|e| ControlPlaneError::Parse(e.to_string()))
}

pub(crate) fn parse_versioning(body: &[u8]) -> Result<VersioningConfig, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut config = VersioningConfig::default();
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"VersioningConfiguration";
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                saw_root |= local(e.name().as_ref()) == b"VersioningConfiguration";
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if stack.last().map(Vec::as_slice) == Some(b"Status".as_slice()) {
                    config.status = Some(decode_text(&t)?);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "VersioningConfiguration")?;
    Ok(config)
}

pub(crate) fn parse_lifecycle(body: &[u8]) -> Result<LifecycleConfig, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut config = LifecycleConfig::default();
    let mut rule: Option<LifecycleRule> = None;
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"LifecycleConfiguration";
                if name == b"Rule" {
                    rule = Some(LifecycleRule::default());
                } else if let Some(rule) = rule.as_mut()
                    && (name == b"Transition" || name == b"NoncurrentVersionTransition")
                {
                    rule.has_transition = true;
                }
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"LifecycleConfiguration";
                if let Some(rule) = rule.as_mut()
                    && (name == b"Transition" || name == b"NoncurrentVersionTransition")
                {
                    rule.has_transition = true;
                }
            }
            Ok(Event::End(e)) => {
                let name = local(e.name().as_ref());
                if name == b"Rule"
                    && let Some(rule) = rule.take()
                {
                    config.rules.push(rule);
                }
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if let Some(rule) = rule.as_mut() {
                    let top = stack.last().map(Vec::as_slice);
                    let parent = if stack.len() >= 2 {
                        Some(stack[stack.len() - 2].as_slice())
                    } else {
                        None
                    };
                    let text = decode_text(&t)?;
                    match (top, parent) {
                        (Some(b"Status"), Some(b"Rule")) => rule.status = text,
                        (Some(b"NoncurrentDays"), Some(b"NoncurrentVersionExpiration")) => {
                            rule.noncurrent_days = text.trim().parse().ok();
                        }
                        (Some(b"ExpiredObjectDeleteMarker"), Some(b"Expiration")) => {
                            rule.expired_object_delete_marker =
                                Some(text.trim().eq_ignore_ascii_case("true"));
                        }
                        (Some(b"Days"), Some(b"Expiration")) => {
                            rule.expiration_days = text.trim().parse().ok();
                        }
                        (Some(b"DaysAfterInitiation"), Some(b"AbortIncompleteMultipartUpload")) => {
                            rule.abort_incomplete_days = text.trim().parse().ok();
                        }
                        (Some(b"Prefix"), _) => rule.prefix = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "LifecycleConfiguration")?;
    Ok(config)
}

pub(crate) fn parse_replication(body: &[u8]) -> Result<ReplicationConfig, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut config = ReplicationConfig::default();
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"ReplicationConfiguration";
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                saw_root |= local(e.name().as_ref()) == b"ReplicationConfiguration";
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                let top = stack.last().map(Vec::as_slice);
                let parent = if stack.len() >= 2 {
                    Some(stack[stack.len() - 2].as_slice())
                } else {
                    None
                };
                if top == Some(b"Status") && parent == Some(b"DeleteMarkerReplication") {
                    config.delete_marker_replication_enabled =
                        decode_text(&t)?.trim().eq_ignore_ascii_case("Enabled");
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "ReplicationConfiguration")?;
    Ok(config)
}

pub(crate) fn parse_object_lock(body: &[u8]) -> Result<ObjectLockConfig, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut config = ObjectLockConfig::default();
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"ObjectLockConfiguration";
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                saw_root |= local(e.name().as_ref()) == b"ObjectLockConfiguration";
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if stack.last().map(Vec::as_slice) == Some(b"ObjectLockEnabled".as_slice()) {
                    config.enabled = decode_text(&t)?.trim().eq_ignore_ascii_case("Enabled");
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "ObjectLockConfiguration")?;
    Ok(config)
}

pub(crate) fn parse_retention(body: &[u8]) -> Result<RetentionConfig, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut config = RetentionConfig::default();
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"Retention";
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                saw_root |= local(e.name().as_ref()) == b"Retention";
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if stack.last().map(Vec::as_slice) == Some(b"Mode".as_slice()) {
                    config.mode = Some(decode_text(&t)?.trim().to_string());
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "Retention")?;
    Ok(config)
}

pub(crate) fn parse_object_versions(
    body: &[u8],
) -> Result<ObjectVersionListing, ControlPlaneError> {
    let mut reader = xml_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut listing = ObjectVersionListing::default();
    let mut current: Option<ObjectVersion> = None;
    let mut saw_root = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local(e.name().as_ref());
                saw_root |= name == b"ListVersionsResult";
                if name == b"Version" {
                    current = Some(ObjectVersion::default());
                }
                stack.push(name);
            }
            Ok(Event::Empty(e)) => {
                saw_root |= local(e.name().as_ref()) == b"ListVersionsResult";
            }
            Ok(Event::End(e)) => {
                let name = local(e.name().as_ref());
                if name == b"Version"
                    && let Some(version) = current.take()
                {
                    listing.versions.push(version);
                }
                stack.pop();
            }
            Ok(Event::Text(t)) => {
                if let Some(version) = current.as_mut() {
                    let text = decode_text(&t)?;
                    match stack.last().map(Vec::as_slice) {
                        Some(b"Key") => version.key = text,
                        Some(b"VersionId") => version.version_id = text,
                        Some(b"IsLatest") => {
                            version.is_latest = text.trim().eq_ignore_ascii_case("true")
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(ControlPlaneError::Parse(e.to_string())),
        }
        buf.clear();
    }
    require_well_formed(saw_root, &stack, "ListVersionsResult")?;
    Ok(listing)
}

// --- The control plane ---

/// A read-only bucket-configuration client for one S3 bucket (ADR-1727 decision
/// 1). Holds the `reqwest` client, the credential provider `S3Store` already
/// owns, and enough of `S3Config` to address and sign requests.
pub(crate) struct BucketControlPlaneClient {
    client: reqwest::Client,
    credentials: AwsCredentialProvider,
    clock: Arc<dyn SigningClock>,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    force_path_style: bool,
}

impl BucketControlPlaneClient {
    pub(crate) fn new(
        client: reqwest::Client,
        credentials: AwsCredentialProvider,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        force_path_style: bool,
    ) -> Self {
        BucketControlPlaneClient {
            client,
            credentials,
            clock: Arc::new(SystemSigningClock),
            bucket,
            region,
            endpoint,
            force_path_style,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn SigningClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sign and send one read-only `GET`, returning the 200 body or a classified
    /// error. `not_configured_markers` are the response markers (S3 error codes)
    /// that mean "affirmatively absent" for this call, so a 404 carrying one maps
    /// to [`ControlPlaneError::NotConfigured`] rather than an opaque status.
    async fn send_get(
        &self,
        object_key: Option<&str>,
        query_pairs: &[(String, String)],
    ) -> Result<String, ControlPlaneError> {
        let credential =
            self.credentials.get_credential().await.map_err(|e| {
                ControlPlaneError::Transport(format!("credential fetch failed: {e}"))
            })?;

        let target = request_target(
            &self.bucket,
            &self.region,
            self.endpoint.as_deref(),
            self.force_path_style,
            object_key,
            query_pairs,
        );
        let (amz_date, date_stamp) = format_amz_time(self.clock.now_unix_secs());

        let mut headers = vec![
            SignedHeader {
                name: "host".to_string(),
                value: target.host.clone(),
            },
            SignedHeader {
                name: "x-amz-content-sha256".to_string(),
                value: EMPTY_SHA256_HEX.to_string(),
            },
            SignedHeader {
                name: "x-amz-date".to_string(),
                value: amz_date.clone(),
            },
        ];
        if let Some(token) = credential.token.as_ref() {
            headers.push(SignedHeader {
                name: "x-amz-security-token".to_string(),
                value: token.clone(),
            });
        }

        let query = canonical_query(query_pairs);
        let (request, signed_headers) = canonical_request(
            "GET",
            &target.canonical_uri,
            &query,
            &headers,
            EMPTY_SHA256_HEX,
        );
        let scope = format!("{date_stamp}/{}/{SERVICE}/aws4_request", self.region);
        let sts = string_to_sign(&amz_date, &scope, &request);
        let sig = signature(
            &credential.secret_key,
            &date_stamp,
            &self.region,
            SERVICE,
            &sts,
        );
        let authorization = format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={sig}",
            credential.key_id
        );

        let mut builder = self
            .client
            .get(&target.url)
            .header("x-amz-content-sha256", EMPTY_SHA256_HEX)
            .header("x-amz-date", &amz_date)
            .header(reqwest::header::AUTHORIZATION, &authorization);
        if let Some(token) = credential.token.as_ref() {
            builder = builder.header("x-amz-security-token", token);
        }

        let response = builder
            .send()
            .await
            .map_err(|e| ControlPlaneError::Transport(e.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| ControlPlaneError::Transport(e.to_string()))?;

        if status.is_success() {
            return Ok(body);
        }
        match status.as_u16() {
            403 => Err(ControlPlaneError::AccessDenied(short_excerpt(&body))),
            404 => Err(ControlPlaneError::NotConfigured(short_excerpt(&body))),
            other => {
                // A 404 marker can also arrive with a 200/400 in some S3-compatible
                // endpoints; treat an explicit NoSuch* / NotFound body as absent.
                if body.contains("NoSuch") || body.contains("NotFoundError") {
                    Err(ControlPlaneError::NotConfigured(short_excerpt(&body)))
                } else {
                    Err(ControlPlaneError::UnexpectedStatus(other))
                }
            }
        }
    }

    async fn fetch_versioning(&self) -> FetchOutcome<VersioningConfig> {
        match self
            .send_get(None, &[("versioning".to_string(), String::new())])
            .await
        {
            Ok(body) => match parse_versioning(body.as_bytes()) {
                Ok(config) => FetchOutcome::Present(config),
                Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
            },
            Err(ControlPlaneError::NotConfigured(_)) => {
                // ?versioning always answers 200 on real S3; a 404 here means the
                // endpoint has no versioning API at all -> Unknown, not a Fail.
                FetchOutcome::Unknown("endpoint has no ?versioning API".to_string())
            }
            Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
        }
    }

    async fn fetch_lifecycle(&self) -> FetchOutcome<LifecycleConfig> {
        classify_fetch(
            self.send_get(None, &[("lifecycle".to_string(), String::new())])
                .await,
            parse_lifecycle,
            "no lifecycle configuration on the bucket",
        )
    }

    async fn fetch_replication(&self) -> FetchOutcome<ReplicationConfig> {
        classify_fetch(
            self.send_get(None, &[("replication".to_string(), String::new())])
                .await,
            parse_replication,
            "no replication configuration on the bucket",
        )
    }

    async fn fetch_object_lock(&self) -> FetchOutcome<ObjectLockConfig> {
        classify_fetch(
            self.send_get(None, &[("object-lock".to_string(), String::new())])
                .await,
            parse_object_lock,
            "Object Lock is not enabled on the bucket",
        )
    }

    async fn fetch_object_versions(&self, prefix: &str) -> FetchOutcome<ObjectVersionListing> {
        classify_fetch(
            self.send_get(
                None,
                &[
                    ("versions".to_string(), String::new()),
                    ("prefix".to_string(), prefix.to_string()),
                    ("max-keys".to_string(), "100".to_string()),
                ],
            )
            .await,
            parse_object_versions,
            "no versions under the prefix",
        )
    }

    async fn fetch_retention(&self, key: &str, version_id: &str) -> FetchOutcome<RetentionConfig> {
        classify_fetch(
            self.send_get(
                Some(key),
                &[
                    ("retention".to_string(), String::new()),
                    ("versionId".to_string(), version_id.to_string()),
                ],
            )
            .await,
            parse_retention,
            "the object carries no retention",
        )
    }

    /// Fetch every subresource and assemble the report (ADR-1727 decision 3).
    pub(crate) async fn report(&self, params: &BucketProtectionParams) -> BucketProtectionReport {
        let versioning = self.fetch_versioning().await;
        let lifecycle = self.fetch_lifecycle().await;
        let replication = self.fetch_replication().await;
        let object_lock = self.fetch_object_lock().await;
        let retention = if params.sample_object_retention {
            self.sample_retention(&params.protected_retention_prefixes)
                .await
        } else {
            RetentionSample::NotSampled
        };

        assemble_report(
            &versioning,
            &lifecycle,
            &replication,
            &object_lock,
            &retention,
            params,
        )
    }

    /// Sample one current and one noncurrent object under each protected prefix
    /// and read their retention (ADR-1727 decision 3, reusing a versioned
    /// listing). Returns the worst outcome across all samples.
    async fn sample_retention(&self, prefixes: &[String]) -> RetentionSample {
        if prefixes.is_empty() {
            return RetentionSample::Unknown(
                "no protected prefixes configured to sample".to_string(),
            );
        }
        let mut sampled_any = false;
        let mut worst_fail: Option<String> = None;
        let mut worst_unknown: Option<String> = None;

        for prefix in prefixes {
            let listing = match self.fetch_object_versions(prefix).await {
                FetchOutcome::Present(listing) => listing,
                FetchOutcome::Absent(_) => continue,
                FetchOutcome::Unknown(detail) => {
                    worst_unknown.get_or_insert(format!("{prefix}: {detail}"));
                    continue;
                }
            };
            let current = listing.versions.iter().find(|v| v.is_latest);
            let noncurrent = listing.versions.iter().find(|v| !v.is_latest);
            for sample in [current, noncurrent].into_iter().flatten() {
                sampled_any = true;
                match self.fetch_retention(&sample.key, &sample.version_id).await {
                    FetchOutcome::Present(config) => {
                        if !config
                            .mode
                            .as_deref()
                            .is_some_and(|m| m.eq_ignore_ascii_case("COMPLIANCE"))
                        {
                            worst_fail.get_or_insert(format!(
                                "{}: retention mode {:?} is not COMPLIANCE",
                                sample.key, config.mode
                            ));
                        }
                    }
                    FetchOutcome::Absent(detail) => {
                        worst_fail
                            .get_or_insert(format!("{}: no retention ({detail})", sample.key));
                    }
                    FetchOutcome::Unknown(detail) => {
                        worst_unknown.get_or_insert(format!("{}: {detail}", sample.key));
                    }
                }
            }
        }

        if let Some(fail) = worst_fail {
            RetentionSample::Fail(fail)
        } else if let Some(unknown) = worst_unknown {
            RetentionSample::Unknown(unknown)
        } else if sampled_any {
            RetentionSample::Pass
        } else {
            RetentionSample::Unknown(
                "no objects under the protected prefixes to sample".to_string(),
            )
        }
    }
}

/// Classify a raw fetch result into a [`FetchOutcome`] with the given parser and
/// absent-reason.
fn classify_fetch<T>(
    result: Result<String, ControlPlaneError>,
    parse: impl Fn(&[u8]) -> Result<T, ControlPlaneError>,
    absent_reason: &str,
) -> FetchOutcome<T> {
    match result {
        Ok(body) => match parse(body.as_bytes()) {
            Ok(config) => FetchOutcome::Present(config),
            Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
        },
        Err(ControlPlaneError::NotConfigured(_)) => FetchOutcome::Absent(absent_reason.to_string()),
        Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
    }
}

/// The object-retention sampling outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RetentionSample {
    /// Retention was not sampled (server path, ADR-1727 decision 5).
    NotSampled,
    /// Every sampled object carried compliance-mode retention.
    Pass,
    /// At least one sampled object lacked compliance-mode retention.
    Fail(String),
    /// Sampling could not determine the state.
    Unknown(String),
}

/// The first 200 bytes of a body, for an error detail. Never a header or
/// credential value.
fn short_excerpt(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= 200 {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..200])
    }
}

/// Turn the fetched subresources into a [`BucketProtectionReport`] (ADR-1727
/// decision 3). Pure over the parsed inputs, so the mapping is unit-testable
/// without HTTP.
pub(crate) fn assemble_report(
    versioning: &FetchOutcome<VersioningConfig>,
    lifecycle: &FetchOutcome<LifecycleConfig>,
    replication: &FetchOutcome<ReplicationConfig>,
    object_lock: &FetchOutcome<ObjectLockConfig>,
    retention: &RetentionSample,
    params: &BucketProtectionParams,
) -> BucketProtectionReport {
    use ProtectionConditionId as Id;

    let mut states: Vec<(Id, ConditionState)> = Vec::new();

    // versioning
    states.push((
        Id::Versioning,
        match versioning {
            FetchOutcome::Present(config) => {
                if config
                    .status
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case("Enabled"))
                {
                    ConditionState::Pass
                } else {
                    ConditionState::Fail(format!(
                        "object versioning is not Enabled (status: {})",
                        config.status.as_deref().unwrap_or("none")
                    ))
                }
            }
            FetchOutcome::Absent(detail) => ConditionState::Fail(detail.clone()),
            FetchOutcome::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

    // The five lifecycle-derived conditions share one fetched config.
    let (noncurrent_state, expired_marker_state, abort_state, rule_scope_state, no_foreign_state) =
        lifecycle_conditions(lifecycle, params.expected_noncurrent_days);
    states.push((Id::NoncurrentExpiration, noncurrent_state));
    states.push((Id::ExpiredDeleteMarker, expired_marker_state));
    states.push((Id::AbortMultipart, abort_state));
    states.push((Id::RuleScope, rule_scope_state));
    states.push((Id::NoForeignRule, no_foreign_state));

    // delete-marker-replication
    states.push((
        Id::DeleteMarkerReplication,
        match replication {
            FetchOutcome::Present(config) => {
                if config.delete_marker_replication_enabled {
                    ConditionState::Pass
                } else {
                    ConditionState::Fail("DeleteMarkerReplication is not Enabled".to_string())
                }
            }
            FetchOutcome::Absent(detail) => ConditionState::Fail(detail.clone()),
            FetchOutcome::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

    // object-lock
    states.push((
        Id::ObjectLock,
        match object_lock {
            FetchOutcome::Present(config) => {
                if config.enabled {
                    ConditionState::Pass
                } else {
                    ConditionState::Fail("Object Lock is not enabled".to_string())
                }
            }
            FetchOutcome::Absent(detail) => ConditionState::Fail(detail.clone()),
            FetchOutcome::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

    // object-retention
    states.push((
        Id::ObjectRetention,
        match retention {
            RetentionSample::NotSampled => {
                ConditionState::Unknown("object retention not sampled".to_string())
            }
            RetentionSample::Pass => ConditionState::Pass,
            RetentionSample::Fail(detail) => ConditionState::Fail(detail.clone()),
            RetentionSample::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

    BucketProtectionReport::from_states(states)
}

/// Derive the five lifecycle conditions from the fetched lifecycle config.
fn lifecycle_conditions(
    lifecycle: &FetchOutcome<LifecycleConfig>,
    expected_noncurrent_days: Option<u32>,
) -> (
    ConditionState,
    ConditionState,
    ConditionState,
    ConditionState,
    ConditionState,
) {
    let config = match lifecycle {
        FetchOutcome::Present(config) => config,
        FetchOutcome::Absent(detail) => {
            // No lifecycle configuration: every lifecycle condition affirmatively
            // fails except no-foreign-rule, which is satisfied by there being no
            // rules at all.
            let absent = ConditionState::Fail(detail.clone());
            return (
                absent.clone(),
                absent.clone(),
                absent,
                ConditionState::Fail("no lifecycle rules to scope t/".to_string()),
                ConditionState::Pass,
            );
        }
        FetchOutcome::Unknown(detail) => {
            let unknown = ConditionState::Unknown(detail.clone());
            return (
                unknown.clone(),
                unknown.clone(),
                unknown.clone(),
                unknown.clone(),
                unknown,
            );
        }
    };

    let enabled: Vec<&LifecycleRule> = config.rules.iter().filter(|r| r.enabled()).collect();

    // noncurrent-expiration
    let noncurrent = enabled
        .iter()
        .find(|r| r.noncurrent_days.is_some())
        .copied();
    let noncurrent_state = match noncurrent {
        None => ConditionState::Fail("no enabled noncurrent-version expiration rule".to_string()),
        Some(rule) => {
            let days = rule.noncurrent_days.unwrap_or_default();
            match expected_noncurrent_days {
                Some(expected) if days != expected => ConditionState::Fail(format!(
                    "noncurrent-version expiration is {days} days, expected {expected}"
                )),
                _ => ConditionState::Pass,
            }
        }
    };

    // expired-delete-marker
    let expired_marker_state = if enabled
        .iter()
        .any(|r| r.expired_object_delete_marker == Some(true))
    {
        ConditionState::Pass
    } else {
        ConditionState::Fail("no enabled expired-object-delete-marker rule".to_string())
    };

    // abort-multipart
    let abort_state = if enabled
        .iter()
        .any(|r| r.abort_incomplete_days.is_some_and(|d| d <= 7))
    {
        ConditionState::Pass
    } else {
        ConditionState::Fail(
            "no enabled AbortIncompleteMultipartUpload rule of 7 days or less".to_string(),
        )
    };

    // rule-scope: some enabled sanctioned rule covers all of t/ (empty filter or
    // a prefix that t/ starts with).
    let rule_scope_state = if enabled
        .iter()
        .filter(|r| r.carries_sanctioned())
        .any(|r| "t/".starts_with(r.effective_prefix()))
    {
        ConditionState::Pass
    } else {
        ConditionState::Fail("no enabled sanctioned rule covers every t/ prefix".to_string())
    };

    // no-foreign-rule: no enabled rule with a current-version expiration or a
    // transition targets t/ or sys/.
    let foreign = enabled.iter().find(|r| {
        r.carries_foreign()
            && (prefix_intersects(r.effective_prefix(), "t/")
                || prefix_intersects(r.effective_prefix(), "sys/"))
    });
    let no_foreign_state = match foreign {
        None => ConditionState::Pass,
        Some(rule) => ConditionState::Fail(format!(
            "a foreign expiration/transition rule targets a Ravel prefix (prefix: {:?})",
            rule.prefix
        )),
    };

    (
        noncurrent_state,
        expired_marker_state,
        abort_state,
        rule_scope_state,
        no_foreign_state,
    )
}

/// Whether two prefixes name overlapping key ranges (either is a prefix of the
/// other). An empty prefix intersects everything.
fn prefix_intersects(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// Build the unified credential provider `S3Store` hands this module: the same
/// provider the client signs with, for every auth mode, so there is no second
/// credential path (ADR-1727 decision 1).
pub(crate) fn static_credential_provider(
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
) -> AwsCredentialProvider {
    Arc::new(object_store::StaticCredentialProvider::new(AwsCredential {
        key_id: access_key_id.to_string(),
        secret_key: secret_access_key.to_string(),
        token: session_token.map(str::to_string),
    }))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::Router;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::Response;
    use axum::routing::get;
    use parking_lot::Mutex;

    use super::*;

    /// A fixed clock so signatures are reproducible.
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
        // The credential scope is what the Authorization header carries.
        assert!(scope.starts_with(KAT_DATE_STAMP));
    }

    /// Flipping a single byte of the canonical request changes the signature away
    /// from the published value: the signer is not accidentally constant.
    #[test]
    fn sigv4_known_answer_fails_with_one_byte_flipped() {
        let (request, _) = kat_canonical_request();
        // Flip the last byte of the path: "/test.txt" -> "/test.txu".
        let mut bytes = request.into_bytes();
        let last = bytes
            .iter()
            .rposition(|&b| b == b't')
            .expect("a 't' to flip");
        bytes[last] = b'u';
        let tampered = String::from_utf8(bytes).expect("still utf8");

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
        // 20130524T000000Z is 1_369_353_600 unix seconds.
        let (amz, stamp) = format_amz_time(1_369_353_600);
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

    // --- XML reader tests (each response shape -> parsed value) ---

    #[test]
    fn parses_versioning_enabled() {
        let body =
            br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
        assert_eq!(
            parse_versioning(body).expect("parse").status.as_deref(),
            Some("Enabled")
        );
    }

    #[test]
    fn parses_versioning_empty() {
        let body = br#"<VersioningConfiguration/>"#;
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
        assert_eq!(rule.status, "Enabled");
        assert_eq!(rule.prefix.as_deref(), Some("t/"));
        assert_eq!(rule.noncurrent_days, Some(30));
        assert_eq!(rule.expired_object_delete_marker, Some(true));
        assert_eq!(rule.abort_incomplete_days, Some(7));
        assert!(!rule.has_transition);
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
    }

    #[test]
    fn parses_replication_delete_marker_enabled() {
        let body = br#"<ReplicationConfiguration><Rule><Status>Enabled</Status>
          <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
        </Rule></ReplicationConfiguration>"#;
        assert!(
            parse_replication(body)
                .expect("parse")
                .delete_marker_replication_enabled
        );
    }

    #[test]
    fn parses_object_lock_enabled() {
        let body = br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"#;
        assert!(parse_object_lock(body).expect("parse").enabled);
    }

    #[test]
    fn parses_retention_compliance() {
        let body = br#"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>"#;
        assert_eq!(
            parse_retention(body).expect("parse").mode.as_deref(),
            Some("COMPLIANCE")
        );
    }

    #[test]
    fn parses_object_versions() {
        let body = br#"<ListVersionsResult>
          <Version><Key>t/a</Key><VersionId>v1</VersionId><IsLatest>true</IsLatest></Version>
          <Version><Key>t/a</Key><VersionId>v0</VersionId><IsLatest>false</IsLatest></Version>
        </ListVersionsResult>"#;
        let listing = parse_object_versions(body).expect("parse");
        assert_eq!(listing.versions.len(), 2);
        assert!(listing.versions[0].is_latest);
        assert_eq!(listing.versions[1].version_id, "v0");
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
        // Not XML at all: no root element reached.
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
    }

    /// The truncation check is per parser, not just the one the test above
    /// drives: each of the six reads its own element stack.
    #[test]
    fn every_parser_rejects_a_truncated_body() {
        assert!(parse_lifecycle(br#"<LifecycleConfiguration><Rule><Status>Enabled"#).is_err());
        assert!(
            parse_replication(
                br#"<ReplicationConfiguration><Rule><DeleteMarkerReplication><Status>Enabled"#
            )
            .is_err()
        );
        assert!(
            parse_object_lock(br#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled"#).is_err()
        );
        assert!(parse_retention(br#"<Retention><Mode>COMPLIANCE"#).is_err());
        assert!(parse_object_versions(br#"<ListVersionsResult><Version><Key>a"#).is_err());
    }

    // --- assemble_report unit tests ---

    #[test]
    fn assemble_maps_unknown_never_to_fail() {
        let report = assemble_report(
            &FetchOutcome::Unknown("denied".to_string()),
            &FetchOutcome::Unknown("denied".to_string()),
            &FetchOutcome::Unknown("denied".to_string()),
            &FetchOutcome::Unknown("denied".to_string()),
            &RetentionSample::Unknown("denied".to_string()),
            &BucketProtectionParams::default(),
        );
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.unknown_count(), ProtectionConditionId::ALL.len());
    }

    #[test]
    fn assemble_compliant_bucket_passes_expected_conditions() {
        let lifecycle = LifecycleConfig {
            rules: vec![LifecycleRule {
                status: "Enabled".to_string(),
                prefix: Some("t/".to_string()),
                noncurrent_days: Some(30),
                expired_object_delete_marker: Some(true),
                abort_incomplete_days: Some(7),
                ..Default::default()
            }],
        };
        let params = BucketProtectionParams {
            expected_noncurrent_days: Some(30),
            ..Default::default()
        };
        let report = assemble_report(
            &FetchOutcome::Present(VersioningConfig {
                status: Some("Enabled".to_string()),
            }),
            &FetchOutcome::Present(lifecycle),
            &FetchOutcome::Present(ReplicationConfig {
                delete_marker_replication_enabled: true,
            }),
            &FetchOutcome::Present(ObjectLockConfig { enabled: true }),
            &RetentionSample::NotSampled,
            &params,
        );
        for id in [
            ProtectionConditionId::Versioning,
            ProtectionConditionId::NoncurrentExpiration,
            ProtectionConditionId::ExpiredDeleteMarker,
            ProtectionConditionId::AbortMultipart,
            ProtectionConditionId::RuleScope,
            ProtectionConditionId::NoForeignRule,
            ProtectionConditionId::DeleteMarkerReplication,
            ProtectionConditionId::ObjectLock,
        ] {
            assert!(
                report.state(id).expect("present").is_pass(),
                "{} should pass, got {:?}",
                id.id(),
                report.state(id)
            );
        }
        // Retention was not sampled -> Unknown, never Fail.
        assert!(
            report
                .state(ProtectionConditionId::ObjectRetention)
                .expect("present")
                .is_unknown()
        );
    }

    #[test]
    fn assemble_wrong_noncurrent_days_fails() {
        let lifecycle = LifecycleConfig {
            rules: vec![LifecycleRule {
                status: "Enabled".to_string(),
                prefix: Some("t/".to_string()),
                noncurrent_days: Some(14),
                ..Default::default()
            }],
        };
        let params = BucketProtectionParams {
            expected_noncurrent_days: Some(30),
            ..Default::default()
        };
        let (noncurrent, ..) = lifecycle_conditions(
            &FetchOutcome::Present(lifecycle),
            params.expected_noncurrent_days,
        );
        assert!(noncurrent.is_fail());
    }

    #[test]
    fn assemble_foreign_expiration_fails_no_foreign_rule() {
        let lifecycle = LifecycleConfig {
            rules: vec![LifecycleRule {
                status: "Enabled".to_string(),
                prefix: Some("t/".to_string()),
                expiration_days: Some(90),
                ..Default::default()
            }],
        };
        let (.., no_foreign) = lifecycle_conditions(&FetchOutcome::Present(lifecycle), None);
        assert!(no_foreign.is_fail());
    }

    // --- Fake-endpoint test: signer + XML reader over real HTTP ---

    /// What the fake endpoint answers, keyed by (subresource, path).
    type Responder = Arc<dyn Fn(&str, &str) -> (StatusCode, String) + Send + Sync>;

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
        /// Response body keyed by the subresource query's first key.
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
        let recorded = SeenRequest {
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
        };
        state.seen.lock().push(recorded);
        let subresource = query
            .split('&')
            .next()
            .and_then(|p| p.split('=').next())
            .unwrap_or("");
        let (status, body) = (state.respond)(subresource, &path);
        Response::builder()
            .status(status)
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

    fn test_client(endpoint: &str) -> BucketControlPlaneClient {
        BucketControlPlaneClient::new(
            reqwest::Client::new(),
            static_credential_provider(KAT_ACCESS_KEY, KAT_SECRET_KEY, None),
            "ravel-test-bucket".to_string(),
            KAT_REGION.to_string(),
            Some(endpoint.to_string()),
            true,
        )
        .with_clock(Arc::new(FixedClock(1_369_353_600)))
    }

    /// Recompute the signature the server received and compare it to the
    /// Authorization header's, from the same credentials and canonical request.
    /// Proves the client signed what it sent.
    fn verify_authorization(req: &SeenRequest) {
        let auth = req
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone())
            .expect("authorization header present");
        // Parse SignedHeaders and Signature out of the header.
        let signed_headers = auth
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .expect("SignedHeaders")
            .to_string();
        let carried_sig = auth
            .split("Signature=")
            .nth(1)
            .expect("Signature")
            .trim()
            .to_string();

        let host = req
            .headers
            .iter()
            .find(|(k, _)| k == "host")
            .map(|(_, v)| v.clone())
            .expect("host header");
        let amz_date = req
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-date")
            .map(|(_, v)| v.clone())
            .expect("x-amz-date header");

        // Rebuild the SignedHeader set from the request's own headers.
        let mut headers = Vec::new();
        for name in signed_headers.split(';') {
            let value = if name == "host" {
                host.clone()
            } else {
                req.headers
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            };
            headers.push(SignedHeader {
                name: name.to_string(),
                value,
            });
        }
        let canonical_uri: String = req
            .path
            .split('/')
            .map(|segment| uri_encode(segment, true))
            .collect::<Vec<_>>()
            .join("/");
        // The query pairs, decoded from the wire and re-canonicalized.
        let pairs: Vec<(String, String)> = if req.query.is_empty() {
            Vec::new()
        } else {
            req.query
                .split('&')
                .map(|p| {
                    let mut it = p.splitn(2, '=');
                    let k = percent_decode(it.next().unwrap_or(""));
                    let v = percent_decode(it.next().unwrap_or(""));
                    (k, v)
                })
                .collect()
        };
        let (request, _) = canonical_request(
            &req.method,
            &canonical_uri,
            &canonical_query(&pairs),
            &headers,
            EMPTY_SHA256_HEX,
        );
        let scope = format!("{KAT_DATE_STAMP}/{KAT_REGION}/{SERVICE}/aws4_request");
        let sts = string_to_sign(&amz_date, &scope, &request);
        let recomputed = signature(KAT_SECRET_KEY, KAT_DATE_STAMP, KAT_REGION, SERVICE, &sts);
        assert_eq!(
            recomputed, carried_sig,
            "server-side recomputed signature must match the Authorization header"
        );
        assert_eq!(req.method, "GET", "every control-plane request is a GET");
    }

    fn percent_decode(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn versioning_get_signs_and_parses_over_http() {
        let respond: Responder = Arc::new(|sub, _path| {
            assert_eq!(sub, "versioning");
            (
                StatusCode::OK,
                "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                    .to_string(),
            )
        });
        let (base, seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        let outcome = client.fetch_versioning().await;
        match outcome {
            FetchOutcome::Present(config) => assert_eq!(config.status.as_deref(), Some("Enabled")),
            other => panic!("expected Present, got {other:?}"),
        }
        let requests = seen.lock();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].query, "versioning=");
        verify_authorization(&requests[0]);
    }

    #[tokio::test]
    async fn lifecycle_get_signs_and_parses_over_http() {
        let respond: Responder = Arc::new(|_sub, _path| {
            (
                    StatusCode::OK,
                    r#"<LifecycleConfiguration><Rule><Status>Enabled</Status>
                      <Filter><Prefix>t/</Prefix></Filter>
                      <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>
                    </Rule></LifecycleConfiguration>"#
                        .to_string(),
                )
        });
        let (base, seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        match client.fetch_lifecycle().await {
            FetchOutcome::Present(config) => {
                assert_eq!(config.rules[0].noncurrent_days, Some(30))
            }
            other => panic!("expected Present, got {other:?}"),
        }
        verify_authorization(&seen.lock()[0]);
    }

    #[tokio::test]
    async fn replication_and_object_lock_get_over_http() {
        let respond: Responder = Arc::new(|sub, _path| {
            match sub {
                "replication" => (
                    StatusCode::OK,
                    "<ReplicationConfiguration><Rule><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication></Rule></ReplicationConfiguration>".to_string(),
                ),
                "object-lock" => (
                    StatusCode::OK,
                    "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>".to_string(),
                ),
                other => panic!("unexpected subresource {other}"),
            }
        });
        let (base, seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        assert!(matches!(
            client.fetch_replication().await,
            FetchOutcome::Present(ReplicationConfig {
                delete_marker_replication_enabled: true
            })
        ));
        assert!(matches!(
            client.fetch_object_lock().await,
            FetchOutcome::Present(ObjectLockConfig { enabled: true })
        ));
        let requests = seen.lock();
        assert_eq!(requests.len(), 2);
        for req in requests.iter() {
            verify_authorization(req);
        }
    }

    #[tokio::test]
    async fn retention_get_with_version_id_over_http() {
        let respond: Responder = Arc::new(|sub, path| {
            assert_eq!(sub, "retention");
            assert!(path.contains("t/a"), "object key in path: {path}");
            (
                StatusCode::OK,
                "<Retention><Mode>COMPLIANCE</Mode></Retention>".to_string(),
            )
        });
        let (base, seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        match client.fetch_retention("t/a", "v1+/x").await {
            FetchOutcome::Present(config) => assert_eq!(config.mode.as_deref(), Some("COMPLIANCE")),
            other => panic!("expected Present, got {other:?}"),
        }
        let requests = seen.lock();
        assert!(requests[0].query.contains("retention="));
        assert!(requests[0].query.contains("versionId="));
        verify_authorization(&requests[0]);
    }

    #[tokio::test]
    async fn access_denied_is_unknown_not_fail() {
        let respond: Responder = Arc::new(|_sub, _path| {
            (
                StatusCode::FORBIDDEN,
                "<Error><Code>AccessDenied</Code></Error>".to_string(),
            )
        });
        let (base, _seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        assert!(matches!(
            client.fetch_lifecycle().await,
            FetchOutcome::Unknown(_)
        ));
    }

    #[tokio::test]
    async fn not_configured_is_absent_then_fail() {
        let respond: Responder = Arc::new(|_sub, _path| {
            (
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchLifecycleConfiguration</Code></Error>".to_string(),
            )
        });
        let (base, _seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        assert!(matches!(
            client.fetch_lifecycle().await,
            FetchOutcome::Absent(_)
        ));
    }

    #[tokio::test]
    async fn malformed_body_is_unknown() {
        let respond: Responder =
            Arc::new(|_sub, _path| (StatusCode::OK, "this is not xml".to_string()));
        let (base, _seen) = spawn_fake(respond).await;
        let client = test_client(&base);
        // A 200 body that is not the expected XML must map to Unknown, never a
        // Fail (ADR-1727 decision 3): "could not parse" is not "not configured".
        assert!(matches!(
            client.fetch_lifecycle().await,
            FetchOutcome::Unknown(_)
        ));
    }
}
