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
//! No credential or signature bytes are ever logged: errors carry the HTTP
//! status and the S3 error code parsed from the body, never a header value and
//! never the rest of the body (an S3 `SignatureDoesNotMatch` body echoes the
//! canonical request, session token included).
//!
//! A condition is `Pass` only when a response proves it and `Fail` only when a
//! response proves the opposite; everything else is `Unknown` (ADR-1727
//! decision 3).

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

/// Largest response body read. Every configuration document and listing page
/// this module asks for is far smaller; a larger body is `Unknown`.
pub(crate) const MAX_BODY_BYTES: usize = 1 << 20;

/// `?versions` page size and page cap for retention sampling.
const LISTING_PAGE_KEYS: u32 = 1000;
const LISTING_MAX_PAGES: usize = 10;

/// The data root the sanctioned lifecycle rules must cover (ADR-1727 decision
/// 3, `rule-scope`), and the two roots no foreign rule may target.
const DATA_ROOT: &str = "t/";
const RAVEL_ROOTS: [&str; 2] = ["t/", "sys/"];

/// Clock seam for SigV4's request timestamp and for judging whether a sampled
/// object's retention has lapsed, mirroring [`super::instance_role`]'s
/// `WallClock`: a fixed clock makes both deterministic in tests (the repo's
/// no-`SystemTime::now` rule).
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

/// Unix seconds and nanoseconds for the ISO 8601 timestamps S3 returns in
/// `LastModified` and `RetainUntilDate` (`2030-01-01T00:00:00Z`, optional
/// fractional seconds, `Z` or a `+HH:MM`/`-HH:MM` offset). `None` for anything
/// else, so a value the reader cannot place in time is never compared.
pub(crate) fn parse_iso8601(value: &str) -> Option<(i64, u32)> {
    let v = value.trim();
    let bytes = v.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> {
        let digits = v.get(range)?;
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    };
    let year = num(0..4)?;
    let month = u32::try_from(num(5..7)?).ok()?;
    let day = u32::try_from(num(8..10)?).ok()?;
    let (hour, minute, second) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&month)
        || day == 0
        || day > super::http_date::days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut rest = &v[19..];
    let mut nanos = 0u32;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || digits > 9 {
            return None;
        }
        let scale = 10u32.pow(u32::try_from(9 - digits).ok()?);
        nanos = fraction[..digits].parse::<u32>().ok()? * scale;
        rest = &fraction[digits..];
    }
    let offset_secs = match rest {
        "Z" | "z" => 0,
        _ => {
            let sign = match rest.as_bytes().first() {
                Some(b'+') => 1,
                Some(b'-') => -1,
                _ => return None,
            };
            let offset = &rest[1..];
            if offset.len() != 5 || offset.as_bytes()[2] != b':' {
                return None;
            }
            let hours: i64 = offset[..2].parse().ok()?;
            let minutes: i64 = offset[3..].parse().ok()?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            sign * (hours * 3600 + minutes * 60)
        }
    };
    let days = super::http_date::days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3600 + minute * 60 + second - offset_secs;
    Some((secs, nanos))
}

// --- Request target (URL, host, canonical path) ---

#[derive(Debug)]
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
/// `GET`, addressed the way `object_store`'s `AmazonS3Builder` addresses the
/// data plane: path style is `{endpoint}/{bucket}`; virtual-hosted style with a
/// custom endpoint uses the endpoint as given, since `object_store` expects the
/// bucket to be in it already; with no endpoint, AWS's regional host.
/// `object_key` `None` targets the bucket; `Some(key)` targets an object.
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

    let (scheme, host, base_path) = match endpoint {
        Some(endpoint) => {
            let (scheme, rest) = split_scheme(endpoint.trim_end_matches('/'));
            let (authority, base_path) = match rest.find('/') {
                Some(index) => (&rest[..index], &rest[index..]),
                None => (rest, ""),
            };
            (scheme, authority.to_string(), base_path.to_string())
        }
        None if force_path_style => ("https", format!("s3.{region}.amazonaws.com"), String::new()),
        None => (
            "https",
            format!("{bucket}.s3.{region}.amazonaws.com"),
            String::new(),
        ),
    };
    let path = if force_path_style {
        format!("{base_path}/{bucket}{key_path}")
    } else {
        format!("{base_path}{key_path}")
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

/// The `reqwest` client the control plane sends through: the data plane's
/// timeouts, plain HTTP only where the store allows it (the data plane's
/// `allow_http` rule), and no redirects. A followed redirect would carry
/// `Authorization` and the session token to another URL, and the target's
/// unsigned body is not the bucket's answer, so a 3xx comes back as-is and reads
/// as `Unknown`.
pub(crate) fn control_plane_http_client(
    connect_timeout: Duration,
    request_timeout: Duration,
    pool_idle_timeout: Duration,
    allow_http: bool,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .pool_idle_timeout(pool_idle_timeout)
        .https_only(!allow_http)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

// --- Fetch outcomes and errors ---

/// The outcome of fetching one bucket subresource.
#[derive(Debug, Clone)]
pub(crate) enum FetchOutcome<T> {
    /// The configuration was read and parsed.
    Present(T),
    /// The endpoint answered 404 with this call's own not-configured S3 error
    /// code (for example `NoSuchLifecycleConfiguration` on `?lifecycle`), read
    /// from the body's `<Error><Code>` element: the configuration is
    /// affirmatively absent. Carries a human-readable reason.
    Absent(String),
    /// The configuration could not be determined (access denied, any other 404,
    /// transport failure, an unparseable or oversized body, a redirect, or an
    /// unexpected status): the derived condition is `Unknown`, never `Fail`.
    Unknown(String),
}

/// A control-plane request failure, classified so the caller can tell an
/// affirmative "not configured" from a "could not tell" (an `Unknown`).
#[derive(Debug)]
pub(crate) enum ControlPlaneError {
    /// 403: the credential cannot read this configuration -> `Unknown`.
    AccessDenied(String),
    /// 404 whose `<Error><Code>` is one of the not-configured codes the call
    /// named -> `Absent`. Carries the code.
    NotConfigured(String),
    /// Any other 404 (`NoSuchBucket`, `NoSuchKey`, `NoSuchVersion`, an empty or
    /// non-XML body, an endpoint without the API) -> `Unknown`.
    NotFound(String),
    /// Transport failure (connect, timeout, dropped connection) -> `Unknown`.
    Transport(String),
    /// The response body could not be parsed -> `Unknown`.
    Parse(String),
    /// A 3xx, never followed -> `Unknown`.
    Redirect(u16),
    /// The body passed [`MAX_BODY_BYTES`] -> `Unknown`.
    BodyTooLarge,
    /// Any other non-success status -> `Unknown`.
    UnexpectedStatus(u16, String),
}

impl ControlPlaneError {
    fn into_unknown_detail(self) -> String {
        match self {
            ControlPlaneError::AccessDenied(msg) => format!("access denied: {msg}"),
            ControlPlaneError::NotConfigured(code) => format!("not configured ({code})"),
            ControlPlaneError::NotFound(msg) => format!("HTTP 404: {msg}"),
            ControlPlaneError::Transport(msg) => format!("transport error: {msg}"),
            ControlPlaneError::Parse(msg) => format!("could not parse response: {msg}"),
            ControlPlaneError::Redirect(status) => {
                format!("HTTP {status} redirect, not followed")
            }
            ControlPlaneError::BodyTooLarge => {
                format!("response body exceeds {MAX_BODY_BYTES} bytes")
            }
            ControlPlaneError::UnexpectedStatus(status, msg) => {
                format!("unexpected HTTP status {status}: {msg}")
            }
        }
    }
}

// --- A small XML tree over quick-xml ---

/// One XML element with its local name, its own text, and its child elements.
/// Every body read here is capped at [`MAX_BODY_BYTES`], so building a tree and
/// walking it is simpler and no more costly than streaming.
#[derive(Debug, Default)]
struct XmlElement {
    name: String,
    text: String,
    children: Vec<XmlElement>,
}

/// A child element that appeared more than once where one value is expected.
/// It is never resolved to either copy: the value it carries is unparseable.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Repeated(String);

impl fmt::Display for Repeated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a repeated <{}>", self.0)
    }
}

impl From<Repeated> for ControlPlaneError {
    fn from(repeated: Repeated) -> Self {
        ControlPlaneError::Parse(repeated.to_string())
    }
}

impl XmlElement {
    /// The single child named `name`: `Ok(None)` when absent, `Err` when it
    /// appears more than once.
    fn single(&self, name: &str) -> Result<Option<&XmlElement>, Repeated> {
        let mut named = self.children.iter().filter(|child| child.name == name);
        match (named.next(), named.next()) {
            (None, _) => Ok(None),
            (Some(one), None) => Ok(Some(one)),
            (Some(_), Some(_)) => Err(Repeated(name.to_string())),
        }
    }

    fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a XmlElement> {
        self.children.iter().filter(move |child| child.name == name)
    }

    /// The element's text with surrounding whitespace removed.
    fn value(&self) -> &str {
        self.text.trim()
    }
}

fn local_name(name: &[u8]) -> Result<String, ControlPlaneError> {
    let local = match name.iter().rposition(|&b| b == b':') {
        Some(idx) => &name[idx + 1..],
        None => name,
    };
    String::from_utf8(local.to_vec())
        .map_err(|_| ControlPlaneError::Parse("element name is not UTF-8".to_string()))
}

fn attach(stack: &mut [XmlElement], root: &mut Option<XmlElement>, element: XmlElement) -> bool {
    match stack.last_mut() {
        Some(parent) => {
            parent.children.push(element);
            true
        }
        None if root.is_none() => {
            *root = Some(element);
            true
        }
        None => false,
    }
}

/// Parse `body` into a tree whose root must be `<expected_root>`. A body that is
/// not that XML (garbage, an `<Error>` document, another shape), a body that
/// stops mid-element, and an entity the reader cannot resolve are all parse
/// errors, so the derived condition is `Unknown` rather than a misleading
/// partial read. quick-xml's `check_end_names` rejects a mismatched end tag but
/// reports plain EOF for a truncated body, so the open stack is checked at EOF.
fn parse_document(body: &[u8], expected_root: &str) -> Result<XmlElement, ControlPlaneError> {
    let parse_err = |e: &dyn fmt::Display| ControlPlaneError::Parse(e.to_string());
    let mut reader = Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<XmlElement> = Vec::new();
    let mut root: Option<XmlElement> = None;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => stack.push(XmlElement {
                name: local_name(e.name().as_ref())?,
                ..XmlElement::default()
            }),
            Ok(Event::Empty(e)) => {
                let element = XmlElement {
                    name: local_name(e.name().as_ref())?,
                    ..XmlElement::default()
                };
                if !attach(&mut stack, &mut root, element) {
                    return Err(ControlPlaneError::Parse(
                        "more than one root element".to_string(),
                    ));
                }
            }
            Ok(Event::End(_)) => {
                let Some(element) = stack.pop() else {
                    return Err(ControlPlaneError::Parse("unbalanced end tag".to_string()));
                };
                if !attach(&mut stack, &mut root, element) {
                    return Err(ControlPlaneError::Parse(
                        "more than one root element".to_string(),
                    ));
                }
            }
            Ok(Event::Text(t)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| parse_err(&e))?);
                }
            }
            Ok(Event::CData(t)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| parse_err(&e))?);
                }
            }
            Ok(Event::GeneralRef(r)) => {
                let resolved = if r.is_char_ref() {
                    r.resolve_char_ref()
                        .map_err(|e| parse_err(&e))?
                        .map(String::from)
                } else {
                    let name = r.decode().map_err(|e| parse_err(&e))?;
                    quick_xml::escape::resolve_predefined_entity(&name).map(str::to_string)
                };
                let Some(resolved) = resolved else {
                    return Err(ControlPlaneError::Parse(
                        "unresolvable entity reference".to_string(),
                    ));
                };
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&resolved);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(parse_err(&e)),
        }
        buf.clear();
    }
    if let Some(open) = stack.last() {
        return Err(ControlPlaneError::Parse(format!(
            "response ended inside <{}>",
            open.name
        )));
    }
    match root {
        Some(root) if root.name == expected_root => Ok(root),
        _ => Err(ControlPlaneError::Parse(format!(
            "response is not <{expected_root}> XML"
        ))),
    }
}

/// The `<Error><Code>` of an S3 error body, or `None` when the body is not one.
pub(crate) fn parse_error_code(body: &[u8]) -> Option<String> {
    let root = parse_document(body, "Error").ok()?;
    let code = root.single("Code").ok()??.value();
    (!code.is_empty()).then(|| code.to_string())
}

/// The S3 error code of a failure body, for an error detail. Only the code: the
/// rest of an S3 error body can echo the canonical request, session token
/// included.
fn error_code_detail(body: &[u8]) -> String {
    match parse_error_code(body) {
        Some(code) => format!("S3 error code {}", short_excerpt(&code)),
        None => "no S3 error code in the body".to_string(),
    }
}

// --- Parsed configurations ---

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VersioningConfig {
    /// `Some("Enabled")`, `Some("Suspended")`, or `None` (never enabled).
    pub status: Option<String>,
}

/// A rule's `Status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuleStatus {
    Enabled,
    Disabled,
    /// Neither `Enabled` nor `Disabled`, missing, or repeated.
    Other(String),
}

impl RuleStatus {
    fn parse(element: Result<Option<&XmlElement>, Repeated>) -> RuleStatus {
        match element.map(|e| e.map(XmlElement::value)) {
            Ok(Some(value)) if value.eq_ignore_ascii_case("Enabled") => RuleStatus::Enabled,
            Ok(Some(value)) if value.eq_ignore_ascii_case("Disabled") => RuleStatus::Disabled,
            Ok(Some(value)) => RuleStatus::Other(value.to_string()),
            Ok(None) => RuleStatus::Other("<missing>".to_string()),
            Err(repeated) => RuleStatus::Other(repeated.to_string()),
        }
    }

    fn active(&self) -> Tri {
        match self {
            RuleStatus::Enabled => Tri::Yes,
            RuleStatus::Disabled => Tri::No,
            RuleStatus::Other(_) => Tri::Maybe,
        }
    }
}

/// A day count from a lifecycle rule. A value that does not parse is kept
/// distinct from absence, so it reads as `Unknown`, never as a missing rule or
/// a wrong value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Days {
    Value(u32),
    Invalid(String),
}

impl Days {
    fn parse(element: Result<Option<&XmlElement>, Repeated>) -> Days {
        match element.map(|e| e.map(XmlElement::value)) {
            Ok(Some(value)) => value
                .parse()
                .map(Days::Value)
                .unwrap_or_else(|_| Days::Invalid(value.to_string())),
            Ok(None) => Days::Invalid("<missing>".to_string()),
            Err(repeated) => Days::Invalid(repeated.to_string()),
        }
    }

    fn as_result(&self) -> Result<u32, String> {
        match self {
            Days::Value(days) => Ok(*days),
            Days::Invalid(raw) => Err(raw.clone()),
        }
    }
}

/// A boolean from a lifecycle rule, with the same absent/invalid split as
/// [`Days`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Flag {
    Value(bool),
    Invalid(String),
}

impl Flag {
    fn parse(element: &XmlElement) -> Flag {
        match element.value() {
            value if value.eq_ignore_ascii_case("true") => Flag::Value(true),
            value if value.eq_ignore_ascii_case("false") => Flag::Value(false),
            value => Flag::Invalid(value.to_string()),
        }
    }
}

/// Which objects a lifecycle or replication rule applies to, read from its
/// `Filter` (or legacy rule-level `Prefix`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuleScope {
    /// Every object whose key starts with the prefix (empty: the whole bucket).
    Prefix(String),
    /// Only the objects under the prefix that also match a tag or an object-size
    /// bound: a subset, so the rule never covers a whole prefix.
    Narrowed { prefix: String, by: String },
    /// A filter shape the reader does not recognise.
    Unrecognized(String),
}

/// How much of `t/` a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coverage {
    /// Every key under `t/`.
    Full,
    /// A plain prefix strictly under `t/`. Several such rules could cover all
    /// of `t/` as a union (ADR-1727 decision 3), which this reader does not
    /// evaluate, so it is neither proof of coverage nor of its absence.
    UnionMember,
    /// No key under `t/`, or only a tag- or size-narrowed subset.
    None,
    /// An unrecognised filter.
    Unknown,
}

/// Three-valued answer for a rule property that depends on something the reader
/// may not recognise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    Yes,
    No,
    Maybe,
}

impl RuleScope {
    fn coverage_of_data_root(&self) -> Coverage {
        match self {
            RuleScope::Prefix(prefix) if DATA_ROOT.starts_with(prefix.as_str()) => Coverage::Full,
            RuleScope::Prefix(prefix) if prefix.starts_with(DATA_ROOT) => Coverage::UnionMember,
            RuleScope::Prefix(_) | RuleScope::Narrowed { .. } => Coverage::None,
            RuleScope::Unrecognized(_) => Coverage::Unknown,
        }
    }

    /// Whether the rule can apply to any key under `t/` (`false` for an
    /// unrecognised filter, which [`Coverage::Unknown`] already reports).
    fn intersects_data_root(&self) -> bool {
        match self {
            RuleScope::Prefix(prefix) | RuleScope::Narrowed { prefix, .. } => {
                prefix_intersects(prefix, DATA_ROOT)
            }
            RuleScope::Unrecognized(_) => false,
        }
    }

    /// Whether the rule can apply to a key under any of `roots`.
    fn targets(&self, roots: &[&str]) -> Tri {
        match self {
            RuleScope::Prefix(prefix) | RuleScope::Narrowed { prefix, .. } => {
                if roots.iter().any(|root| prefix_intersects(prefix, root)) {
                    Tri::Yes
                } else {
                    Tri::No
                }
            }
            RuleScope::Unrecognized(_) => Tri::Maybe,
        }
    }

    fn describe(&self) -> String {
        match self {
            RuleScope::Prefix(prefix) if prefix.is_empty() => "the whole bucket".to_string(),
            RuleScope::Prefix(prefix) => format!("prefix {prefix:?}"),
            RuleScope::Narrowed { prefix, by } => format!("prefix {prefix:?} narrowed by {by}"),
            RuleScope::Unrecognized(detail) => format!("an unrecognised filter ({detail})"),
        }
    }
}

/// Read a rule's scope. Accepted shapes: a legacy rule-level `<Prefix>`, or a
/// `<Filter>` that is empty or holds exactly one of `<Prefix>`, `<Tag>`,
/// `<ObjectSizeGreaterThan>`, `<ObjectSizeLessThan>`, or `<And>` (whose children
/// are at most one `<Prefix>` plus any `<Tag>` and size bounds). Anything else
/// is [`RuleScope::Unrecognized`].
fn rule_scope(rule: &XmlElement) -> RuleScope {
    let unrecognized = |detail: &str| RuleScope::Unrecognized(detail.to_string());
    let (Ok(prefix), Ok(filter)) = (rule.single("Prefix"), rule.single("Filter")) else {
        return unrecognized("repeated Prefix or Filter");
    };
    match (prefix, filter) {
        (Some(prefix), None) => RuleScope::Prefix(prefix.text.clone()),
        (None, Some(filter)) => filter_scope(filter),
        (Some(_), Some(_)) => unrecognized("both a rule-level Prefix and a Filter"),
        (None, None) => unrecognized("neither a Filter nor a Prefix"),
    }
}

fn filter_scope(filter: &XmlElement) -> RuleScope {
    let [condition] = filter.children.as_slice() else {
        return if filter.children.is_empty() && filter.value().is_empty() {
            RuleScope::Prefix(String::new())
        } else {
            RuleScope::Unrecognized(
                "a Filter with more than one condition outside <And>".to_string(),
            )
        };
    };
    match condition.name.as_str() {
        "Prefix" => RuleScope::Prefix(condition.text.clone()),
        "Tag" => RuleScope::Narrowed {
            prefix: String::new(),
            by: "a tag".to_string(),
        },
        "ObjectSizeGreaterThan" | "ObjectSizeLessThan" => RuleScope::Narrowed {
            prefix: String::new(),
            by: condition.name.clone(),
        },
        "And" => and_scope(condition),
        other => RuleScope::Unrecognized(format!("<Filter><{other}>")),
    }
}

fn and_scope(and: &XmlElement) -> RuleScope {
    if and.children.is_empty() {
        return RuleScope::Unrecognized("an empty <And>".to_string());
    }
    let mut prefix: Option<String> = None;
    let mut narrowing: Vec<String> = Vec::new();
    for child in &and.children {
        match child.name.as_str() {
            "Prefix" if prefix.is_none() => prefix = Some(child.text.clone()),
            "Prefix" => return RuleScope::Unrecognized("<And> with two prefixes".to_string()),
            "Tag" => narrowing.push("a tag".to_string()),
            "ObjectSizeGreaterThan" | "ObjectSizeLessThan" => narrowing.push(child.name.clone()),
            other => return RuleScope::Unrecognized(format!("<And><{other}>")),
        }
    }
    let prefix = prefix.unwrap_or_default();
    if narrowing.is_empty() {
        RuleScope::Prefix(prefix)
    } else {
        narrowing.dedup();
        RuleScope::Narrowed {
            prefix,
            by: narrowing.join(" and "),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleRule {
    pub id: Option<String>,
    pub status: RuleStatus,
    pub scope: RuleScope,
    pub noncurrent_days: Option<Days>,
    /// `NewerNoncurrentVersions`: that many noncurrent versions outlive
    /// `NoncurrentDays`.
    pub newer_noncurrent_versions: Option<String>,
    pub expired_object_delete_marker: Option<Flag>,
    pub expiration_days: Option<Days>,
    pub expiration_date: Option<String>,
    /// An `<Expiration>` shape the reader does not classify.
    pub expiration_unrecognized: Option<String>,
    pub abort_incomplete_days: Option<Days>,
    pub has_transition: bool,
    /// A rule-level action element the reader does not classify (for example a
    /// vendor extension that deletes versions).
    pub unrecognized_action: Option<String>,
}

impl LifecycleRule {
    fn label(&self, index: usize) -> String {
        match &self.id {
            Some(id) => format!("rule {id:?}"),
            None => format!("rule #{}", index + 1),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationRule {
    pub id: Option<String>,
    pub status: RuleStatus,
    pub scope: RuleScope,
    /// `DeleteMarkerReplication/Status`, `None` when the element is missing.
    pub delete_marker_replication: Option<RuleStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplicationConfig {
    pub rules: Vec<ReplicationRule>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectLockConfig {
    /// `ObjectLockEnabled` read as `Enabled` or not, `None` when the element is
    /// missing or empty (a document that states nothing either way).
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RetentionConfig {
    /// `Some("COMPLIANCE")`, `Some("GOVERNANCE")`, or `None`.
    pub mode: Option<String>,
    /// `RetainUntilDate` as sent.
    pub retain_until: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectVersion {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObjectVersionListing {
    pub versions: Vec<ObjectVersion>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

// --- XML readers ---

pub(crate) fn parse_versioning(body: &[u8]) -> Result<VersioningConfig, ControlPlaneError> {
    let root = parse_document(body, "VersioningConfiguration")?;
    Ok(VersioningConfig {
        status: root.single("Status")?.map(|s| s.value().to_string()),
    })
}

pub(crate) fn parse_lifecycle(body: &[u8]) -> Result<LifecycleConfig, ControlPlaneError> {
    let root = parse_document(body, "LifecycleConfiguration")?;
    Ok(LifecycleConfig {
        rules: root.children_named("Rule").map(lifecycle_rule).collect(),
    })
}

fn lifecycle_rule(rule: &XmlElement) -> LifecycleRule {
    let mut out = LifecycleRule {
        id: rule_id(rule),
        status: RuleStatus::parse(rule.single("Status")),
        scope: rule_scope(rule),
        noncurrent_days: None,
        newer_noncurrent_versions: None,
        expired_object_delete_marker: None,
        expiration_days: None,
        expiration_date: None,
        expiration_unrecognized: None,
        abort_incomplete_days: None,
        has_transition: false,
        unrecognized_action: None,
    };
    for child in &rule.children {
        match child.name.as_str() {
            "ID" | "Status" | "Filter" | "Prefix" | "NoncurrentVersionExpiration"
            | "Expiration" | "AbortIncompleteMultipartUpload" => {}
            "Transition" | "NoncurrentVersionTransition" => out.has_transition = true,
            other => out.unrecognized_action = Some(format!("<{other}>")),
        }
    }
    match rule.single("NoncurrentVersionExpiration") {
        Ok(None) => {}
        Ok(Some(action)) => {
            out.noncurrent_days = Some(match action.single("NewerNoncurrentVersions") {
                Ok(newer) => {
                    out.newer_noncurrent_versions = newer.map(|n| n.value().to_string());
                    Days::parse(action.single("NoncurrentDays"))
                }
                Err(repeated) => Days::Invalid(repeated.to_string()),
            });
        }
        Err(repeated) => out.noncurrent_days = Some(Days::Invalid(repeated.to_string())),
    }
    match rule.single("Expiration") {
        Ok(None) => {}
        Ok(Some(expiration)) => read_expiration(expiration, &mut out),
        Err(repeated) => {
            out.expiration_unrecognized = Some(repeated.to_string());
            out.expired_object_delete_marker = Some(Flag::Invalid(repeated.to_string()));
        }
    }
    match rule.single("AbortIncompleteMultipartUpload") {
        Ok(None) => {}
        Ok(Some(action)) => {
            out.abort_incomplete_days = Some(Days::parse(action.single("DaysAfterInitiation")));
        }
        Err(repeated) => out.abort_incomplete_days = Some(Days::Invalid(repeated.to_string())),
    }
    out
}

/// A rule's `ID`, for its label only. A repeated `ID` labels the rule by its
/// position instead; no condition reads the ID.
fn rule_id(rule: &XmlElement) -> Option<String> {
    match rule.single("ID") {
        Ok(id) => id.map(|id| id.value().to_string()),
        Err(_) => None,
    }
}

fn read_expiration(expiration: &XmlElement, out: &mut LifecycleRule) {
    if expiration.children.is_empty() {
        out.expiration_unrecognized = Some("an empty <Expiration>".to_string());
    }
    for part in &expiration.children {
        match part.name.as_str() {
            "Days" | "Date" | "ExpiredObjectDeleteMarker" => {}
            other => out.expiration_unrecognized = Some(format!("<Expiration><{other}>")),
        }
    }
    match expiration.single("Days") {
        Ok(None) => {}
        days => out.expiration_days = Some(Days::parse(days)),
    }
    match expiration.single("Date") {
        Ok(None) => {}
        Ok(Some(date)) => out.expiration_date = Some(date.value().to_string()),
        Err(repeated) => out.expiration_unrecognized = Some(format!("<Expiration> {repeated}")),
    }
    match expiration.single("ExpiredObjectDeleteMarker") {
        Ok(None) => {}
        Ok(Some(marker)) => out.expired_object_delete_marker = Some(Flag::parse(marker)),
        Err(repeated) => {
            out.expired_object_delete_marker = Some(Flag::Invalid(repeated.to_string()));
        }
    }
}

pub(crate) fn parse_replication(body: &[u8]) -> Result<ReplicationConfig, ControlPlaneError> {
    let root = parse_document(body, "ReplicationConfiguration")?;
    Ok(ReplicationConfig {
        rules: root
            .children_named("Rule")
            .map(|rule| ReplicationRule {
                id: rule_id(rule),
                status: RuleStatus::parse(rule.single("Status")),
                scope: rule_scope(rule),
                delete_marker_replication: match rule.single("DeleteMarkerReplication") {
                    Ok(dmr) => dmr.map(|dmr| RuleStatus::parse(dmr.single("Status"))),
                    Err(repeated) => Some(RuleStatus::Other(repeated.to_string())),
                },
            })
            .collect(),
    })
}

pub(crate) fn parse_object_lock(body: &[u8]) -> Result<ObjectLockConfig, ControlPlaneError> {
    let root = parse_document(body, "ObjectLockConfiguration")?;
    Ok(ObjectLockConfig {
        enabled: root
            .single("ObjectLockEnabled")?
            .map(XmlElement::value)
            .filter(|value| !value.is_empty())
            .map(|value| value.eq_ignore_ascii_case("Enabled")),
    })
}

pub(crate) fn parse_retention(body: &[u8]) -> Result<RetentionConfig, ControlPlaneError> {
    let root = parse_document(body, "Retention")?;
    Ok(RetentionConfig {
        mode: root.single("Mode")?.map(|m| m.value().to_string()),
        retain_until: root
            .single("RetainUntilDate")?
            .map(|d| d.value().to_string()),
    })
}

fn parse_bool(element: &XmlElement) -> Result<bool, ControlPlaneError> {
    match Flag::parse(element) {
        Flag::Value(value) => Ok(value),
        Flag::Invalid(raw) => Err(ControlPlaneError::Parse(format!(
            "<{}> is {raw:?}, not true or false",
            element.name
        ))),
    }
}

pub(crate) fn parse_object_versions(
    body: &[u8],
) -> Result<ObjectVersionListing, ControlPlaneError> {
    let root = parse_document(body, "ListVersionsResult")?;
    let mut listing = ObjectVersionListing {
        is_truncated: match root.single("IsTruncated")? {
            Some(element) => parse_bool(element)?,
            None => {
                return Err(ControlPlaneError::Parse(
                    "versions listing carries no IsTruncated element".to_string(),
                ));
            }
        },
        next_key_marker: root.single("NextKeyMarker")?.map(|m| m.text.clone()),
        next_version_id_marker: root
            .single("NextVersionIdMarker")?
            .map(|m| m.value().to_string()),
        versions: Vec::new(),
    };
    for version in root.children_named("Version") {
        listing.versions.push(ObjectVersion {
            key: version
                .single("Key")?
                .map(|k| k.text.clone())
                .unwrap_or_default(),
            version_id: match version.single("VersionId")?.map(XmlElement::value) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => {
                    return Err(ControlPlaneError::Parse(
                        "a listed version carries no VersionId".to_string(),
                    ));
                }
            },
            is_latest: match version.single("IsLatest")? {
                Some(element) => parse_bool(element)?,
                None => false,
            },
            last_modified: version
                .single("LastModified")?
                .map(|m| m.value().to_string()),
        });
    }
    Ok(listing)
}

// --- The control plane ---

/// Not-configured error codes per call: a 404 carrying one of these is an
/// affirmatively absent configuration; every other 404 is `Unknown`.
const LIFECYCLE_NOT_CONFIGURED: &[&str] = &["NoSuchLifecycleConfiguration"];
const REPLICATION_NOT_CONFIGURED: &[&str] = &["ReplicationConfigurationNotFoundError"];
const OBJECT_LOCK_NOT_CONFIGURED: &[&str] = &["ObjectLockConfigurationNotFoundError"];
const RETENTION_NOT_CONFIGURED: &[&str] = &["NoSuchObjectLockConfiguration"];

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

    /// Sign and send one read-only `GET`, returning the 2xx body or a classified
    /// error. A 404 is [`ControlPlaneError::NotConfigured`] only when the body's
    /// `<Error><Code>` is one of `not_configured`; any other 404 is
    /// [`ControlPlaneError::NotFound`].
    async fn send_get(
        &self,
        object_key: Option<&str>,
        query_pairs: &[(String, String)],
        not_configured: &[&str],
    ) -> Result<Vec<u8>, ControlPlaneError> {
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

        let mut response = builder
            .send()
            .await
            .map_err(|e| ControlPlaneError::Transport(e.to_string()))?;
        let status = response.status();
        if status.is_redirection() {
            return Err(ControlPlaneError::Redirect(status.as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|len| len > MAX_BODY_BYTES as u64)
        {
            return Err(ControlPlaneError::BodyTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| ControlPlaneError::Transport(e.to_string()))?
        {
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(ControlPlaneError::BodyTooLarge);
            }
            body.extend_from_slice(&chunk);
        }

        if status.is_success() {
            return Ok(body);
        }
        match status.as_u16() {
            403 => Err(ControlPlaneError::AccessDenied(error_code_detail(&body))),
            404 => match parse_error_code(&body) {
                Some(code) if not_configured.contains(&code.as_str()) => {
                    Err(ControlPlaneError::NotConfigured(code))
                }
                _ => Err(ControlPlaneError::NotFound(error_code_detail(&body))),
            },
            other => Err(ControlPlaneError::UnexpectedStatus(
                other,
                error_code_detail(&body),
            )),
        }
    }

    async fn fetch_versioning(&self) -> FetchOutcome<VersioningConfig> {
        classify_fetch(
            self.send_get(None, &[("versioning".to_string(), String::new())], &[])
                .await,
            parse_versioning,
            "",
        )
    }

    async fn fetch_lifecycle(&self) -> FetchOutcome<LifecycleConfig> {
        classify_fetch(
            self.send_get(
                None,
                &[("lifecycle".to_string(), String::new())],
                LIFECYCLE_NOT_CONFIGURED,
            )
            .await,
            parse_lifecycle,
            "no lifecycle configuration on the bucket",
        )
    }

    async fn fetch_replication(&self) -> FetchOutcome<ReplicationConfig> {
        classify_fetch(
            self.send_get(
                None,
                &[("replication".to_string(), String::new())],
                REPLICATION_NOT_CONFIGURED,
            )
            .await,
            parse_replication,
            "no replication configuration on the bucket",
        )
    }

    async fn fetch_object_lock(&self) -> FetchOutcome<ObjectLockConfig> {
        classify_fetch(
            self.send_get(
                None,
                &[("object-lock".to_string(), String::new())],
                OBJECT_LOCK_NOT_CONFIGURED,
            )
            .await,
            parse_object_lock,
            "Object Lock is not enabled on the bucket",
        )
    }

    async fn fetch_object_versions(
        &self,
        prefix: &str,
        marker: Option<(&str, &str)>,
    ) -> FetchOutcome<ObjectVersionListing> {
        let mut pairs = vec![
            ("versions".to_string(), String::new()),
            ("prefix".to_string(), prefix.to_string()),
            ("max-keys".to_string(), LISTING_PAGE_KEYS.to_string()),
        ];
        if let Some((key_marker, version_id_marker)) = marker {
            pairs.push(("key-marker".to_string(), key_marker.to_string()));
            pairs.push((
                "version-id-marker".to_string(),
                version_id_marker.to_string(),
            ));
        }
        classify_fetch(
            self.send_get(None, &pairs, &[]).await,
            parse_object_versions,
            "",
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
                RETENTION_NOT_CONFIGURED,
            )
            .await,
            parse_retention,
            "the object version carries no retention",
        )
    }

    /// Fetch every subresource and assemble the report (ADR-1727 decision 3).
    pub(crate) async fn report(&self, params: &BucketProtectionParams) -> BucketProtectionReport {
        self.report_with_notes(params).await.0
    }

    /// [`Self::report`] plus the [`ReportNotes`] the `BucketConfigProbe`
    /// mapping needs.
    pub(crate) async fn report_with_notes(
        &self,
        params: &BucketProtectionParams,
    ) -> (BucketProtectionReport, ReportNotes) {
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

    /// Every version under `prefix`, following the listing for at most
    /// [`LISTING_MAX_PAGES`] pages. `Err` carries an `Unknown` detail.
    async fn list_versions(&self, prefix: &str) -> Result<VersionPages, String> {
        let mut pages = VersionPages::default();
        let mut marker: Option<(String, String)> = None;
        for page_number in 1..=LISTING_MAX_PAGES {
            let page = match self
                .fetch_object_versions(
                    prefix,
                    marker.as_ref().map(|(k, v)| (k.as_str(), v.as_str())),
                )
                .await
            {
                FetchOutcome::Present(page) => page,
                FetchOutcome::Absent(detail) | FetchOutcome::Unknown(detail) => {
                    return Err(format!("versions listing: {detail}"));
                }
            };
            pages.versions.extend(page.versions);
            if !page.is_truncated {
                return Ok(pages);
            }
            if page_number == LISTING_MAX_PAGES {
                break;
            }
            match (page.next_key_marker, page.next_version_id_marker) {
                (Some(key), Some(version)) => marker = Some((key, version)),
                _ => {
                    return Err(
                        "versions listing is truncated but carries no next markers".to_string()
                    );
                }
            }
        }
        pages.truncated = true;
        Ok(pages)
    }

    /// Sample, under each protected prefix family, the newest current object
    /// version and (when there is one) the newest noncurrent version, and read
    /// their retention (ADR-1727 decision 3). Newest is by `LastModified`: the
    /// retention a deployment applies is finite, so only a recent object is
    /// expected to still be locked, and the first key in listing order is the
    /// oldest shard and hour, not the newest write.
    async fn sample_retention(&self, prefixes: &[String]) -> RetentionSample {
        if prefixes.is_empty() {
            return RetentionSample::Unknown(
                "no protected prefixes configured to sample".to_string(),
            );
        }
        let now = self.clock.now_unix_secs();
        let mut fails: Vec<String> = Vec::new();
        let mut unknowns: Vec<String> = Vec::new();
        let mut noncurrent_sampled = false;

        for prefix in prefixes {
            let pages = match self.list_versions(prefix).await {
                Ok(pages) => pages,
                Err(detail) => {
                    unknowns.push(format!("{prefix}: {detail}"));
                    continue;
                }
            };
            let current = newest(pages.versions.iter().filter(|v| v.is_latest));
            let noncurrent = newest(pages.versions.iter().filter(|v| !v.is_latest));
            let mut samples: Vec<(&ObjectVersion, &str)> = Vec::new();
            match current {
                Ok(Some(version)) => samples.push((version, "newest current version")),
                Ok(None) => unknowns.push(format!("{prefix}: no current object version to sample")),
                Err(detail) => unknowns.push(format!("{prefix}: {detail}")),
            }
            match noncurrent {
                Ok(Some(version)) => samples.push((version, "newest noncurrent version")),
                Ok(None) => {}
                Err(detail) => unknowns.push(format!("{prefix}: {detail}")),
            }
            for (version, role) in samples {
                if role == "newest noncurrent version" {
                    noncurrent_sampled = true;
                }
                let outcome = self
                    .fetch_retention(&version.key, &version.version_id)
                    .await;
                let label = format!("{} ({role})", version.key);
                match retention_verdict(&outcome, now, pages.truncated) {
                    SampleVerdict::Protects => {}
                    SampleVerdict::NotProtecting(detail) => {
                        fails.push(format!("{label}: {detail}"))
                    }
                    SampleVerdict::Unknown(detail) => unknowns.push(format!("{label}: {detail}")),
                }
            }
        }

        if !fails.is_empty() {
            RetentionSample::Fail(fails.join("; "))
        } else if !unknowns.is_empty() {
            RetentionSample::Unknown(unknowns.join("; "))
        } else if !noncurrent_sampled {
            RetentionSample::Unknown(
                "no noncurrent version under any protected prefix to sample".to_string(),
            )
        } else {
            RetentionSample::Pass
        }
    }
}

#[derive(Debug, Default)]
struct VersionPages {
    versions: Vec<ObjectVersion>,
    /// The page cap was reached with more versions left unlisted.
    truncated: bool,
}

/// The version with the latest `LastModified` (ties broken by key), `Ok(None)`
/// for none, and `Err` when a candidate's `LastModified` is missing or does not
/// parse, since the newest then cannot be told.
fn newest<'a>(
    versions: impl Iterator<Item = &'a ObjectVersion>,
) -> Result<Option<&'a ObjectVersion>, String> {
    let mut best: Option<((i64, u32), &ObjectVersion)> = None;
    for version in versions {
        let Some(raw) = version.last_modified.as_deref() else {
            return Err(format!("{}: listing carries no LastModified", version.key));
        };
        let Some(at) = parse_iso8601(raw) else {
            return Err(format!(
                "{}: LastModified {raw:?} does not parse",
                version.key
            ));
        };
        if best.as_ref().is_none_or(|(best_at, best_version)| {
            (at, &version.key) > (*best_at, &best_version.key)
        }) {
            best = Some((at, version));
        }
    }
    Ok(best.map(|(_, version)| version))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SampleVerdict {
    Protects,
    NotProtecting(String),
    Unknown(String),
}

/// Whether one sampled version's retention protects it at `now`: compliance
/// mode with a `RetainUntilDate` still in the future. A sample drawn from a
/// listing cut off at the page cap is `Unknown` whichever way its lock reads,
/// since that sample may not be the family's newest object.
fn retention_verdict(
    outcome: &FetchOutcome<RetentionConfig>,
    now_unix_secs: i64,
    listing_truncated: bool,
) -> SampleVerdict {
    let capped = |detail: &str| {
        SampleVerdict::Unknown(format!(
            "{detail}, but the versions listing stopped at {LISTING_MAX_PAGES} pages, so this \
             may not be the newest object"
        ))
    };
    let not_protecting = |detail: String| {
        if listing_truncated {
            capped(&detail)
        } else {
            SampleVerdict::NotProtecting(detail)
        }
    };
    let config = match outcome {
        FetchOutcome::Present(config) => config,
        FetchOutcome::Absent(detail) => return not_protecting(detail.clone()),
        FetchOutcome::Unknown(detail) => return SampleVerdict::Unknown(detail.clone()),
    };
    if !config
        .mode
        .as_deref()
        .is_some_and(|mode| mode.eq_ignore_ascii_case("COMPLIANCE"))
    {
        return not_protecting(format!(
            "retention mode {:?} is not COMPLIANCE",
            config.mode
        ));
    }
    let Some(raw) = config.retain_until.as_deref() else {
        return SampleVerdict::Unknown("retention carries no RetainUntilDate".to_string());
    };
    let Some((until, _)) = parse_iso8601(raw) else {
        return SampleVerdict::Unknown(format!("RetainUntilDate {raw:?} does not parse"));
    };
    if until > now_unix_secs {
        if listing_truncated {
            capped(&format!("compliance retention holds until {raw}"))
        } else {
            SampleVerdict::Protects
        }
    } else {
        not_protecting(format!("compliance retention lapsed at {raw}"))
    }
}

/// Classify a raw fetch result into a [`FetchOutcome`] with the given parser and
/// absent-reason.
fn classify_fetch<T>(
    result: Result<Vec<u8>, ControlPlaneError>,
    parse: impl Fn(&[u8]) -> Result<T, ControlPlaneError>,
    absent_reason: &str,
) -> FetchOutcome<T> {
    match result {
        Ok(body) => match parse(&body) {
            Ok(config) => FetchOutcome::Present(config),
            Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
        },
        Err(ControlPlaneError::NotConfigured(code)) => {
            FetchOutcome::Absent(format!("{absent_reason} ({code})"))
        }
        Err(e) => FetchOutcome::Unknown(e.into_unknown_detail()),
    }
}

/// The object-retention sampling outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RetentionSample {
    /// Retention was not sampled (server path, ADR-1727 decision 5).
    NotSampled,
    /// Every sampled object carried unexpired compliance-mode retention.
    Pass,
    /// At least one sampled object was not protected.
    Fail(String),
    /// Sampling could not determine the state.
    Unknown(String),
}

/// At most the first 200 bytes of `text`, for an error detail.
fn short_excerpt(text: &str) -> String {
    const LIMIT: usize = 200;
    let trimmed = text.trim();
    if trimmed.len() <= LIMIT {
        return trimmed.to_string();
    }
    // The text is whatever the endpoint sent, so the 200th byte can land inside
    // a multi-byte character, where slicing panics.
    let mut end = LIMIT;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}

/// Facts the `BucketConfigProbe` mapping needs that a condition state cannot
/// carry: whether an enabled rule covering every key under `t/` carries the
/// action at all, so a rule that is present but out of range is not reported
/// as absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReportNotes {
    pub abort_rule_covers_data: bool,
    pub noncurrent_rule_covers_data: bool,
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
) -> (BucketProtectionReport, ReportNotes) {
    use ProtectionConditionId as Id;

    let mut states: Vec<(Id, ConditionState)> = Vec::new();

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
            // `?versioning` has no not-configured code, so this cannot occur;
            // a not-configured answer is still not proof of anything.
            FetchOutcome::Absent(detail) | FetchOutcome::Unknown(detail) => {
                ConditionState::Unknown(detail.clone())
            }
        },
    ));

    let verdicts = lifecycle_conditions(lifecycle, params.expected_noncurrent_days);
    states.push((Id::NoncurrentExpiration, verdicts.noncurrent));
    states.push((Id::ExpiredDeleteMarker, verdicts.expired_marker));
    states.push((Id::AbortMultipart, verdicts.abort));
    states.push((Id::RuleScope, verdicts.rule_scope));
    states.push((Id::NoForeignRule, verdicts.no_foreign));

    states.push((
        Id::DeleteMarkerReplication,
        match replication {
            FetchOutcome::Present(config) => delete_marker_replication_state(config),
            FetchOutcome::Absent(detail) => ConditionState::Fail(detail.clone()),
            FetchOutcome::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

    states.push((
        Id::ObjectLock,
        match object_lock {
            FetchOutcome::Present(config) => match config.enabled {
                Some(true) => ConditionState::Pass,
                Some(false) => ConditionState::Fail("Object Lock is not enabled".to_string()),
                None => ConditionState::Unknown(
                    "ObjectLockConfiguration carries no ObjectLockEnabled value".to_string(),
                ),
            },
            FetchOutcome::Absent(detail) => ConditionState::Fail(detail.clone()),
            FetchOutcome::Unknown(detail) => ConditionState::Unknown(detail.clone()),
        },
    ));

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

    (BucketProtectionReport::from_states(states), verdicts.notes)
}

/// The five lifecycle-derived condition states plus their [`ReportNotes`].
#[derive(Debug, Clone)]
pub(crate) struct LifecycleVerdicts {
    pub noncurrent: ConditionState,
    pub expired_marker: ConditionState,
    pub abort: ConditionState,
    pub rule_scope: ConditionState,
    pub no_foreign: ConditionState,
    pub notes: ReportNotes,
}

/// Derive the five lifecycle conditions from the fetched lifecycle config.
pub(crate) fn lifecycle_conditions(
    lifecycle: &FetchOutcome<LifecycleConfig>,
    expected_noncurrent_days: Option<u32>,
) -> LifecycleVerdicts {
    let config = match lifecycle {
        FetchOutcome::Present(config) => config,
        FetchOutcome::Absent(detail) => {
            // The endpoint said NoSuchLifecycleConfiguration: every sanctioned
            // rule is affirmatively missing, and with no rules at all no rule
            // can be foreign.
            let absent = ConditionState::Fail(detail.clone());
            return LifecycleVerdicts {
                noncurrent: absent.clone(),
                expired_marker: absent.clone(),
                abort: absent,
                rule_scope: ConditionState::Fail(format!(
                    "no lifecycle rules to cover t/: {detail}"
                )),
                no_foreign: ConditionState::Pass,
                notes: ReportNotes::default(),
            };
        }
        FetchOutcome::Unknown(detail) => {
            let unknown = ConditionState::Unknown(detail.clone());
            return LifecycleVerdicts {
                noncurrent: unknown.clone(),
                expired_marker: unknown.clone(),
                abort: unknown.clone(),
                rule_scope: unknown.clone(),
                no_foreign: unknown,
                notes: ReportNotes::default(),
            };
        }
    };
    let rules = config.rules.as_slice();

    let noncurrent = evaluate_action(
        rules,
        "NoncurrentVersionExpiration",
        |rule| rule.noncurrent_days.as_ref().map(Days::as_result),
        |rule, days| {
            if let Some(newer) = &rule.newer_noncurrent_versions {
                return Err(format!(
                    "NoncurrentVersionExpiration also keeps NewerNoncurrentVersions = {newer}, so \
                     versions can outlive NoncurrentDays"
                ));
            }
            match expected_noncurrent_days {
                Some(expected) if days != expected => {
                    Err(format!("NoncurrentDays is {days}, expected {expected}"))
                }
                _ => Ok(()),
            }
        },
        true,
    );
    let reference_days = expected_noncurrent_days.or_else(|| covering_noncurrent_days(rules));
    let noncurrent_state = fold_early_expiry(
        noncurrent.state.clone(),
        early_noncurrent_rules(rules, reference_days),
    );
    let expired_marker = evaluate_action(
        rules,
        "ExpiredObjectDeleteMarker",
        |rule| {
            rule.expired_object_delete_marker
                .as_ref()
                .map(|flag| match flag {
                    Flag::Value(value) => Ok(*value),
                    Flag::Invalid(raw) => Err(raw.clone()),
                })
        },
        |_, value| {
            if value {
                Ok(())
            } else {
                Err("ExpiredObjectDeleteMarker is false".to_string())
            }
        },
        false,
    );
    let abort = evaluate_action(
        rules,
        "AbortIncompleteMultipartUpload",
        |rule| rule.abort_incomplete_days.as_ref().map(Days::as_result),
        |_, days| {
            if days <= 7 {
                Ok(())
            } else {
                Err(format!(
                    "AbortIncompleteMultipartUpload is {days} days, more than 7"
                ))
            }
        },
        false,
    );

    let actions = [
        ("NoncurrentVersionExpiration", &noncurrent),
        ("ExpiredObjectDeleteMarker", &expired_marker),
        ("AbortIncompleteMultipartUpload", &abort),
    ];
    let missing: Vec<&str> = actions
        .iter()
        .filter(|(_, eval)| !eval.carried && eval.possibly_carried.is_empty())
        .map(|(name, _)| *name)
        .collect();
    let uncertain: Vec<String> = actions
        .iter()
        .filter(|(_, eval)| !eval.carried && !eval.possibly_carried.is_empty())
        .map(|(name, eval)| format!("{name} ({})", eval.possibly_carried.join(", ")))
        .collect();
    let rule_scope = if !missing.is_empty() {
        ConditionState::Fail(format!(
            "no enabled rule covering every key under t/ carries {}",
            missing.join(", ")
        ))
    } else if !uncertain.is_empty() {
        ConditionState::Unknown(format!(
            "coverage of t/ not provable for {}",
            uncertain.join("; ")
        ))
    } else {
        ConditionState::Pass
    };

    let notes = ReportNotes {
        abort_rule_covers_data: abort.carried,
        noncurrent_rule_covers_data: noncurrent.carried,
    };
    LifecycleVerdicts {
        noncurrent: noncurrent_state,
        expired_marker: expired_marker.state,
        abort: abort.state,
        rule_scope,
        no_foreign: no_foreign_rule_state(rules, reference_days),
        notes,
    }
}

/// The `NoncurrentDays` every enabled rule covering all of `t/` agrees on, the
/// reference an early expiry is measured against when no `E_v` was given.
fn covering_noncurrent_days(rules: &[LifecycleRule]) -> Option<u32> {
    let values: BTreeSet<u32> = rules
        .iter()
        .filter(|rule| {
            rule.status == RuleStatus::Enabled
                && rule.scope.coverage_of_data_root() == Coverage::Full
        })
        .filter_map(|rule| match rule.noncurrent_days {
            Some(Days::Value(days)) => Some(days),
            _ => None,
        })
        .collect();
    match values.len() {
        1 => values.first().copied(),
        _ => None,
    }
}

/// A rule's `NoncurrentVersionExpiration` when it deletes noncurrent versions
/// sooner than `reference` days (`Ok`), or when it cannot be judged (`Err`): a
/// day count that does not parse, or no reference to compare it against. Each
/// is a description; `None` otherwise. S3 applies the shortest of overlapping
/// expirations, so a longer one is harmless.
fn early_noncurrent(
    rule: &LifecycleRule,
    reference: Option<u32>,
) -> Option<Result<String, String>> {
    match rule.noncurrent_days.as_ref()? {
        Days::Value(days) => match reference {
            Some(reference) => (*days < reference).then(|| {
                Ok(format!(
                    "noncurrent-version expiration after {days} days, sooner than {reference}"
                ))
            }),
            None => Some(Err(format!(
                "noncurrent-version expiration after {days} days, with no reference to compare \
                 it against (no expected E_v, and no single NoncurrentDays on the rules covering \
                 t/)"
            ))),
        },
        Days::Invalid(raw) => Some(Err(format!("NoncurrentDays {raw:?} that does not parse"))),
    }
}

/// Rules that can delete noncurrent versions under `t/` early and that
/// [`evaluate_action`] does not already judge by value: every rule not
/// `Disabled` whose scope can reach `t/`, except an enabled rule covering all
/// of `t/`. A rule on `sys/` alone is `no-foreign-rule`'s to judge. Returns the
/// definite findings and the uncertain ones.
fn early_noncurrent_rules(
    rules: &[LifecycleRule],
    reference: Option<u32>,
) -> (Vec<String>, Vec<String>) {
    let mut fails: Vec<String> = Vec::new();
    let mut unknowns: Vec<String> = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        let active = rule.status.active();
        let targets = rule.scope.targets(&[DATA_ROOT]);
        if active == Tri::No || targets == Tri::No {
            continue;
        }
        if active == Tri::Yes && rule.scope.coverage_of_data_root() == Coverage::Full {
            continue;
        }
        let Some(found) = early_noncurrent(rule, reference) else {
            continue;
        };
        let on = format!("{} on {}", rule.label(index), rule.scope.describe());
        match found {
            Ok(detail) if active == Tri::Yes && targets == Tri::Yes => {
                fails.push(format!("{on} carries {detail}"))
            }
            Ok(detail) | Err(detail) => {
                unknowns.push(format!("{on} (Status {:?}) carries {detail}", rule.status))
            }
        }
    }
    (fails, unknowns)
}

/// Fold [`early_noncurrent_rules`]' findings into a condition state: a definite
/// one is `Fail`, and an uncertain one keeps a `Pass` from standing.
fn fold_early_expiry(
    state: ConditionState,
    (fails, unknowns): (Vec<String>, Vec<String>),
) -> ConditionState {
    let join = |base: &str, extra: &[String]| {
        if base.is_empty() {
            extra.join("; ")
        } else {
            format!("{base}; {}", extra.join("; "))
        }
    };
    if !fails.is_empty() {
        let base = if state.is_fail() { state.detail() } else { "" };
        return ConditionState::Fail(join(base, &fails));
    }
    if state.is_fail() || unknowns.is_empty() {
        return state;
    }
    ConditionState::Unknown(join(state.detail(), &unknowns))
}

/// One sanctioned lifecycle action evaluated over every rule.
struct ActionEval {
    state: ConditionState,
    /// An enabled rule covering every key under `t/` carries the action.
    carried: bool,
    /// Rules that might carry it over `t/`: an unrecognised filter or status,
    /// or a plain prefix strictly under `t/` (a possible union member).
    possibly_carried: Vec<String>,
}

/// Evaluate one sanctioned action against the enabled rules whose filter covers
/// every key under `t/`, independent of rule order. Any such rule whose value
/// `check` rejects, or (with `require_agreement`) such rules that carry
/// different values, is `Fail`. A value that does not parse, or a rule whose
/// filter or status is unrecognised, is `Unknown`. `Pass` needs at least one
/// covering rule and nothing uncertain. With no covering rule, rules on
/// narrower `t/` prefixes make it `Unknown` (they may form a union) and
/// otherwise it is `Fail`.
fn evaluate_action<T: Copy + fmt::Display>(
    rules: &[LifecycleRule],
    action: &str,
    value_of: impl Fn(&LifecycleRule) -> Option<Result<T, String>>,
    check: impl Fn(&LifecycleRule, T) -> Result<(), String>,
    require_agreement: bool,
) -> ActionEval {
    let mut fails: Vec<String> = Vec::new();
    let mut unknowns: Vec<String> = Vec::new();
    let mut union_members: Vec<String> = Vec::new();
    let mut possibly_carried: Vec<String> = Vec::new();
    let mut accepted: Vec<(String, T)> = Vec::new();
    let mut carried = false;

    for (index, rule) in rules.iter().enumerate() {
        let Some(value) = value_of(rule) else {
            continue;
        };
        let label = rule.label(index);
        let active = rule.status.active();
        if active == Tri::No {
            continue;
        }
        match (active, rule.scope.coverage_of_data_root()) {
            (_, Coverage::None) => {}
            (Tri::Yes, Coverage::Full) => {
                carried = true;
                match value {
                    Err(raw) => {
                        unknowns.push(format!("{label}: {action} value {raw:?} does not parse"))
                    }
                    Ok(value) => match check(rule, value) {
                        Ok(()) => accepted.push((label, value)),
                        Err(detail) => fails.push(format!("{label}: {detail}")),
                    },
                }
            }
            (Tri::Yes, Coverage::UnionMember) => {
                possibly_carried.push(label.clone());
                union_members.push(format!("{label} on {}", rule.scope.describe()));
            }
            (_, coverage) => {
                let why = match (&rule.status, coverage) {
                    (RuleStatus::Other(status), _) => {
                        format!("its Status {status:?} is neither Enabled nor Disabled")
                    }
                    _ => format!("it applies to {}", rule.scope.describe()),
                };
                possibly_carried.push(label.clone());
                unknowns.push(format!("{label} carries {action} but {why}"));
            }
        }
    }

    if require_agreement {
        let distinct: BTreeSet<String> = accepted.iter().map(|(_, v)| v.to_string()).collect();
        if distinct.len() > 1 {
            fails.push(format!(
                "rules covering t/ disagree on {action}: {}",
                accepted
                    .iter()
                    .map(|(label, value)| format!("{label} = {value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }

    let state = if !fails.is_empty() {
        ConditionState::Fail(fails.join("; "))
    } else if !unknowns.is_empty() {
        ConditionState::Unknown(unknowns.join("; "))
    } else if !accepted.is_empty() {
        ConditionState::Pass
    } else if !union_members.is_empty() {
        ConditionState::Unknown(format!(
            "no enabled rule covering every key under t/ carries {action}; rules on narrower \
             prefixes ({}) might cover t/ as a union, which is not evaluated",
            union_members.join(", ")
        ))
    } else {
        ConditionState::Fail(format!(
            "no enabled rule covering every key under t/ carries {action}"
        ))
    };
    ActionEval {
        state,
        carried,
        possibly_carried,
    }
}

/// `no-foreign-rule`: no enabled rule that targets `t/` or `sys/` carries a
/// transition, a current-version expiration (by days or by date), or a
/// noncurrent-version expiration sooner than `reference_noncurrent_days`. An
/// expiration or action the reader cannot classify, a day count that does not
/// parse, a `NoncurrentDays` with no reference to compare it against, or a rule
/// whose filter or status is unrecognised makes it `Unknown`.
fn no_foreign_rule_state(
    rules: &[LifecycleRule],
    reference_noncurrent_days: Option<u32>,
) -> ConditionState {
    let mut fails: Vec<String> = Vec::new();
    let mut unknowns: Vec<String> = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        let active = rule.status.active();
        let targets = rule.scope.targets(&RAVEL_ROOTS);
        if active == Tri::No || targets == Tri::No {
            continue;
        }
        let label = rule.label(index);
        let mut definite: Vec<String> = Vec::new();
        let mut unclassified: Vec<String> = Vec::new();
        if rule.has_transition {
            definite.push("a transition".to_string());
        }
        match &rule.expiration_days {
            Some(Days::Value(days)) => definite.push(format!("expiration after {days} days")),
            Some(Days::Invalid(raw)) => {
                unclassified.push(format!("Expiration Days {raw:?} that does not parse"))
            }
            None => {}
        }
        if let Some(date) = &rule.expiration_date {
            definite.push(format!("expiration on date {date}"));
        }
        match early_noncurrent(rule, reference_noncurrent_days) {
            Some(Ok(detail)) => definite.push(detail),
            Some(Err(detail)) => unclassified.push(detail),
            None => {}
        }
        if let Some(shape) = &rule.expiration_unrecognized {
            unclassified.push(shape.clone());
        }
        if let Some(action) = &rule.unrecognized_action {
            unclassified.push(format!("the unclassified action {action}"));
        }
        if definite.is_empty() && unclassified.is_empty() {
            continue;
        }
        let scope = rule.scope.describe();
        if active == Tri::Yes && targets == Tri::Yes && !definite.is_empty() {
            fails.push(format!(
                "{label} on {scope} carries {}",
                definite.join(" and ")
            ));
        } else {
            let carried: Vec<String> = definite.into_iter().chain(unclassified).collect();
            unknowns.push(format!(
                "{label} on {scope} (Status {:?}) carries {}",
                rule.status,
                carried.join(" and ")
            ));
        }
    }
    if !fails.is_empty() {
        ConditionState::Fail(format!(
            "foreign expiration or transition rule targets a Ravel prefix: {}",
            fails.join("; ")
        ))
    } else if !unknowns.is_empty() {
        ConditionState::Unknown(format!(
            "rule on a Ravel prefix not classifiable: {}",
            unknowns.join("; ")
        ))
    } else {
        ConditionState::Pass
    }
}

/// `delete-marker-replication`: only enabled replication rules whose filter
/// covers every key under `t/` can prove it. All of them must replicate delete
/// markers; a covering rule that does not, or covering rules that disagree, is
/// `Fail`. An enabled rule on part of `t/` (a narrower prefix, or a tag- or
/// size-narrowed filter) may take priority over the covering rule for the keys
/// it matches, so one with `DeleteMarkerReplication` `Disabled` is `Fail` and
/// one with a missing or unrecognised status is `Unknown`.
fn delete_marker_replication_state(config: &ReplicationConfig) -> ConditionState {
    let mut enabled: Vec<String> = Vec::new();
    let mut disabled: Vec<String> = Vec::new();
    let mut partial_disabled: Vec<String> = Vec::new();
    let mut unknowns: Vec<String> = Vec::new();
    let mut union_members: Vec<String> = Vec::new();
    for (index, rule) in config.rules.iter().enumerate() {
        let label = match &rule.id {
            Some(id) => format!("rule {id:?}"),
            None => format!("rule #{}", index + 1),
        };
        let active = rule.status.active();
        if active == Tri::No {
            continue;
        }
        let coverage = rule.scope.coverage_of_data_root();
        let partial = match coverage {
            Coverage::UnionMember => true,
            Coverage::None => rule.scope.intersects_data_root(),
            Coverage::Full | Coverage::Unknown => false,
        };
        if active == Tri::Yes && partial {
            let on = format!("{label} on {}", rule.scope.describe());
            match &rule.delete_marker_replication {
                Some(RuleStatus::Enabled) => {}
                Some(RuleStatus::Disabled) => partial_disabled.push(on.clone()),
                Some(RuleStatus::Other(status)) => {
                    unknowns.push(format!("{on}: DeleteMarkerReplication Status {status:?}"))
                }
                None => unknowns.push(format!("{on}: no DeleteMarkerReplication element")),
            }
            if coverage == Coverage::UnionMember {
                union_members.push(on);
            }
            continue;
        }
        match (active, coverage) {
            (_, Coverage::None) if !partial => {}
            (Tri::Yes, Coverage::Full) => match &rule.delete_marker_replication {
                Some(RuleStatus::Enabled) => enabled.push(label),
                Some(RuleStatus::Disabled) => disabled.push(label),
                Some(RuleStatus::Other(status)) => unknowns.push(format!(
                    "{label}: DeleteMarkerReplication Status {status:?}"
                )),
                None => unknowns.push(format!("{label}: no DeleteMarkerReplication element")),
            },
            (_, _) => unknowns.push(format!(
                "{label} (Status {:?}) applies to {}",
                rule.status,
                rule.scope.describe()
            )),
        }
    }
    if !partial_disabled.is_empty() {
        ConditionState::Fail(format!(
            "DeleteMarkerReplication is Disabled on {}, which applies to part of t/",
            partial_disabled.join(", ")
        ))
    } else if !disabled.is_empty() && !enabled.is_empty() {
        ConditionState::Fail(format!(
            "enabled replication rules covering t/ disagree on DeleteMarkerReplication: Enabled \
             on {}, Disabled on {}",
            enabled.join(", "),
            disabled.join(", ")
        ))
    } else if !disabled.is_empty() {
        ConditionState::Fail(format!(
            "DeleteMarkerReplication is Disabled on {} covering t/",
            disabled.join(", ")
        ))
    } else if !unknowns.is_empty() {
        ConditionState::Unknown(unknowns.join("; "))
    } else if !enabled.is_empty() {
        ConditionState::Pass
    } else if !union_members.is_empty() {
        ConditionState::Unknown(format!(
            "no enabled replication rule covers every key under t/; rules on narrower prefixes \
             ({}) might as a union, which is not evaluated",
            union_members.join(", ")
        ))
    } else {
        ConditionState::Fail(
            "no enabled replication rule covering every key under t/ replicates delete markers"
                .to_string(),
        )
    }
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
mod tests;
