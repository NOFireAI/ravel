//! An in-process fake of the GCS XML API's object requests and the JSON API's
//! `objects.list`, enough of both for [`GcsStore`]
//! (ADR-3456): one bucket, objects held in memory, served over loopback HTTP
//! by axum. Built for this crate's tests and, behind the `test-support`
//! feature, for other crates' tests.
//!
//! [`GcsStore`]: super::GcsStore
//!
//! # Requests served
//!
//! Every response, errors included, carries a `Date` header from the fake's
//! settable clock ([`FakeGcs::set_date_unix_secs`]). A request for any bucket
//! other than the fake's own is a 404: code `NoSuchBucket` (no body on a HEAD)
//! on an object request, a JSON error on a listing. On an object request the
//! bucket and the key are the first path segment and the rest, each
//! percent-decoded, so a key's `/` may arrive as `%2F`.
//!
//! - `PUT /{bucket}/{key}`: stores the body under a new generation, one greater
//!   than the last one this fake assigned to any write, so a rewrite with
//!   identical bytes gets a new generation and the same ETag (the quoted MD5
//!   of the content). `x-goog-if-generation-match: 0` requires the object to
//!   be absent and `x-goog-if-generation-match: N` requires its current
//!   generation to be `N`; otherwise 412 `PreconditionFailed`. A value that is
//!   not a decimal integer is 400 `InvalidArgument`. `If-Match` and
//!   `If-None-Match` are ignored on a PUT, as GCS ignores them. Answers 200
//!   with `ETag`, `x-goog-generation` and `x-goog-metageneration`.
//! - `GET /{bucket}/{key}`: the object, 200, or 206 for a `Range` of
//!   `bytes=a-b`, `bytes=a-` or `bytes=-n`, clamped to the object, with
//!   `Content-Range`. A range starting at or past the end, or any range on an
//!   empty object, is 416 `InvalidRange`; a `Range` header it cannot parse is
//!   ignored. `?generation=N` other than the current generation is 404
//!   `NoSuchKey`; the fake keeps no noncurrent generations. An `If-Match`
//!   other than the current ETag is 412 `PreconditionFailed`. Headers: `ETag`,
//!   `x-goog-generation`, `x-goog-metageneration`, `Last-Modified`,
//!   `Content-Length`.
//! - `HEAD /{bucket}/{key}`: the GET headers for the whole object, no body; a
//!   missing object is a bodiless 404.
//! - `DELETE /{bucket}/{key}`: 204, or 404 `NoSuchKey` for a missing object.
//! - `GET /storage/v1/b/{bucket}/o`: the JSON API's `objects.list`, in
//!   lexicographic order of the names' bytes, honouring `prefix`,
//!   `delimiter` (rolled up into `prefixes`, each counted as one result),
//!   `startOffset` (inclusive), `maxResults` (default and ceiling 1000; 0 or
//!   a non-number is 400) and `pageToken`. Each item carries `name`,
//!   `generation` and `size` as decimal strings, `updated` (RFC 3339), a
//!   JSON-API-style `etag` that is not the object's XML ETag, and `md5Hash`
//!   (base64). `fields` is accepted and ignored. A response with more results
//!   left carries an opaque `nextPageToken`; one that does not decode is 400.
//!
//! Anything else is 501 `NotImplemented`.
//!
//! [`FakeGcs::fail_next`] queues error statuses that the next requests get
//! instead of their normal answer, whatever they are, with an XML error code;
//! [`FakeGcs::fail_next_without_code`] queues them with an empty body.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use parking_lot::Mutex;

use super::GcsConfig;
use crate::StoreError;
use crate::conformance::ForeignKeySeeder;

/// The first generation the fake assigns. Real generations are microsecond
/// timestamps; this is one, so a token that would fit in a smaller integer
/// type is not mistaken for a realistic one.
const FIRST_GENERATION: u64 = 1_790_000_000_000_000;

/// The fake's `Date` until [`FakeGcs::set_date_unix_secs`] moves it:
/// 2026-09-21T14:13:20Z.
const DEFAULT_DATE_UNIX_SECS: i64 = 1_790_000_000;

const MAX_RESULTS: usize = 1000;

/// One request the fake received.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    /// The raw path, still percent-encoded.
    pub path: String,
    /// Decoded query pairs, in order.
    pub query: Vec<(String, String)>,
    /// Header names lowercased.
    pub headers: Vec<(String, String)>,
}

impl RecordedRequest {
    /// The first value of header `name`, matched case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The first value of query parameter `name`.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Whether this is a request for the object `key` in `bucket`.
    pub fn is_object(&self, bucket: &str, key: &str) -> bool {
        percent_decode(&self.path, false).as_deref() == Some(format!("/{bucket}/{key}").as_str())
    }
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    generation: u64,
    etag: String,
    last_modified_secs: i64,
}

#[derive(Debug)]
struct Inner {
    bucket: String,
    objects: Mutex<BTreeMap<String, StoredObject>>,
    next_generation: AtomicU64,
    requests: Mutex<Vec<RecordedRequest>>,
    /// Each with whether its body carries an XML error code.
    failures: Mutex<VecDeque<(u16, bool)>>,
    date_secs: AtomicI64,
}

/// A running fake GCS endpoint. Dropping it stops the server.
#[derive(Debug)]
pub struct FakeGcs {
    inner: Arc<Inner>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for FakeGcs {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeGcs {
    /// Bind a loopback port and serve `bucket` from it.
    pub async fn start(bucket: &str) -> std::io::Result<FakeGcs> {
        let inner = Arc::new(Inner {
            bucket: bucket.to_string(),
            objects: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(FIRST_GENERATION),
            requests: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::new()),
            date_secs: AtomicI64::new(DEFAULT_DATE_UNIX_SECS),
        });
        let state = Arc::clone(&inner);
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let state = Arc::clone(&state);
            async move { state.handle(request).await }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(FakeGcs {
            inner,
            endpoint: format!("http://{addr}"),
            server,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn bucket(&self) -> &str {
        &self.inner.bucket
    }

    /// A [`GcsConfig`] for this endpoint: plain HTTP, unsigned, and at most
    /// two retries with a 1 ms backoff so a scripted failure costs no time.
    pub fn config(&self) -> GcsConfig {
        self.config_for_bucket(&self.inner.bucket)
    }

    /// As [`Self::config`], for a bucket this fake does not serve, to reach
    /// its `NoSuchBucket` answers.
    pub fn config_for_bucket(&self, bucket: &str) -> GcsConfig {
        let mut config = GcsConfig::new(bucket);
        config.base_url = Some(self.endpoint.clone());
        config.allow_http = true;
        config.skip_signature = true;
        config.retry = Some(object_store::RetryConfig {
            backoff: object_store::BackoffConfig {
                init_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                base: 2.0,
            },
            max_retries: 2,
            retry_timeout: Duration::from_secs(30),
        });
        config
    }

    /// Every request received so far, in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.inner.requests.lock().clone()
    }

    pub fn clear_requests(&self) {
        self.inner.requests.lock().clear();
    }

    /// Answer the next `times` requests with `status` and an XML error body
    /// instead of serving them.
    pub fn fail_next(&self, status: u16, times: usize) {
        let mut failures = self.inner.failures.lock();
        failures.extend(std::iter::repeat_n((status, true), times));
    }

    /// As [`Self::fail_next`], with an empty body: the status is all the
    /// client gets.
    pub fn fail_next_without_code(&self, status: u16, times: usize) {
        let mut failures = self.inner.failures.lock();
        failures.extend(std::iter::repeat_n((status, false), times));
    }

    /// Set the clock every later response's `Date` reports.
    pub fn set_date_unix_secs(&self, secs: i64) {
        self.inner.date_secs.store(secs, Ordering::Relaxed);
    }

    /// The fake's clock in unix nanoseconds, as a `Date` header carries it
    /// (whole seconds).
    pub fn date_unix_ns(&self) -> i64 {
        self.inner
            .date_secs
            .load(Ordering::Relaxed)
            .saturating_mul(1_000_000_000)
    }

    /// The current generation of `key`, or `None` when it is absent.
    pub fn generation_of(&self, key: &str) -> Option<u64> {
        self.inner.objects.lock().get(key).map(|o| o.generation)
    }

    /// The current ETag of `key`, quotes included.
    pub fn etag_of(&self, key: &str) -> Option<String> {
        self.inner.objects.lock().get(key).map(|o| o.etag.clone())
    }

    /// The stored bytes of `key`.
    pub fn data_of(&self, key: &str) -> Option<Bytes> {
        self.inner.objects.lock().get(key).map(|o| o.data.clone())
    }

    /// Store `data` under `key` without a request, as another writer would.
    /// `key` is taken as is, so a key `GcsStore` refuses to address (a
    /// foreign writer's) can be planted for a listing to report.
    pub fn insert_object(&self, key: &str, data: impl Into<Bytes>) -> u64 {
        let object = self.inner.new_object(data.into());
        let generation = object.generation;
        self.inner.objects.lock().insert(key.to_string(), object);
        generation
    }

    /// Remove `key` without a request. Whether it was present.
    pub fn remove_object(&self, key: &str) -> bool {
        self.inner.objects.lock().remove(key).is_some()
    }

    /// The `nextPageToken` an objects.list response carries when its page
    /// ends at `last`, an object name or a common prefix.
    pub fn page_token_for(&self, last: &str) -> String {
        encode_token(last)
    }
}

/// Seeds the unaddressable keys the conformance suite plants, as another
/// writer would.
#[async_trait::async_trait]
impl ForeignKeySeeder for FakeGcs {
    async fn seed(&self, key: &str, data: Bytes) -> Result<(), StoreError> {
        self.insert_object(key, data);
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<(), StoreError> {
        self.remove_object(key);
        Ok(())
    }
}

impl Inner {
    fn new_object(&self, data: Bytes) -> StoredObject {
        StoredObject {
            etag: format!("\"{}\"", hex(&md5(&data))),
            generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            last_modified_secs: self.date_secs.load(Ordering::Relaxed),
            data,
        }
    }

    async fn handle(&self, request: axum::extract::Request) -> Response {
        let (parts, body) = request.into_parts();
        let query = parts.uri.query().map(parse_query).unwrap_or_default();
        let path = parts.uri.path().to_string();
        self.requests.lock().push(RecordedRequest {
            method: parts.method.to_string(),
            path: path.clone(),
            query: query.clone(),
            headers: parts
                .headers
                .iter()
                .map(|(n, v)| {
                    (
                        n.as_str().to_ascii_lowercase(),
                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                    )
                })
                .collect(),
        });
        let mut response = match axum::body::to_bytes(body, usize::MAX).await {
            Ok(body) => self.route(&parts.method, &path, &query, &parts.headers, body),
            Err(_) => error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "unreadable body",
            ),
        };
        if parts.method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        set_header(
            response.headers_mut(),
            "date",
            imf_fixdate(self.date_secs.load(Ordering::Relaxed)),
        );
        response
    }

    fn route(
        &self,
        method: &Method,
        path: &str,
        query: &[(String, String)],
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response {
        if let Some((status, with_code)) = self.failures.lock().pop_front() {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            if !with_code {
                let mut response = Response::new(Body::empty());
                *response.status_mut() = status;
                return response;
            }
            return error(status, failure_code(status), "scripted failure");
        }
        if let Some(bucket) = path
            .strip_prefix("/storage/v1/b/")
            .and_then(|rest| rest.strip_suffix("/o"))
        {
            if *method != Method::GET {
                return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", "method");
            }
            if percent_decode(bucket, false).as_deref() != Some(self.bucket.as_str()) {
                return json_error(
                    StatusCode::NOT_FOUND,
                    "The specified bucket does not exist.",
                );
            }
            return self.list(query);
        }
        let rest = path.strip_prefix('/').unwrap_or(path);
        let (bucket, key) = match rest.split_once('/') {
            Some((bucket, key)) => (bucket, Some(key)),
            None => (rest, None),
        };
        if percent_decode(bucket, false).as_deref() != Some(self.bucket.as_str()) {
            return error(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            );
        }
        match key {
            None | Some("") => error(
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "bucket request",
            ),
            Some(raw) => {
                let Some(key) = percent_decode(raw, false) else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "bad object name",
                    );
                };
                match *method {
                    Method::PUT => self.put(&key, headers, body),
                    Method::GET => self.get(&key, query, headers, true),
                    Method::HEAD => self.get(&key, query, headers, false),
                    Method::DELETE => self.delete(&key),
                    _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", "method"),
                }
            }
        }
    }

    fn put(&self, key: &str, headers: &HeaderMap, body: Bytes) -> Response {
        let expected = match headers.get("x-goog-if-generation-match") {
            None => None,
            Some(value) => match value.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
                Some(generation) => Some(generation),
                None => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "x-goog-if-generation-match is not a generation",
                    );
                }
            },
        };
        let mut objects = self.objects.lock();
        if let Some(expected) = expected {
            let current = objects.get(key).map_or(0, |o| o.generation);
            if current != expected {
                return error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold.",
                );
            }
        }
        let object = self.new_object(body);
        let mut response = Response::new(Body::empty());
        object_headers(response.headers_mut(), &object, false);
        objects.insert(key.to_string(), object);
        response
    }

    fn get(
        &self,
        key: &str,
        query: &[(String, String)],
        headers: &HeaderMap,
        with_body: bool,
    ) -> Response {
        let Some(object) = self.objects.lock().get(key).cloned() else {
            return no_such_key();
        };
        if let Some(generation) = query_param(query, "generation")
            && generation.parse::<u64>().ok() != Some(object.generation)
        {
            return no_such_key();
        }
        if let Some(if_match) = headers.get("if-match")
            && if_match.as_bytes() != object.etag.as_bytes()
        {
            return error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold.",
            );
        }
        let size = object.data.len() as u64;
        let range = if with_body {
            headers
                .get("range")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_range)
        } else {
            None
        };
        let Some(range) = range else {
            let mut response = Response::new(Body::from(object.data.clone()));
            object_headers(response.headers_mut(), &object, true);
            return response;
        };
        let (first, last) = match range {
            ByteRange::From(start, end) if start < size && end.is_none_or(|end| end >= start) => {
                (start, end.map_or(size - 1, |end| end.min(size - 1)))
            }
            ByteRange::Suffix(n) if size > 0 && n > 0 => (size.saturating_sub(n), size - 1),
            _ => {
                return error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "The requested range cannot be satisfied.",
                );
            }
        };
        // `first <= last < size`, so both fit in usize: the object is in memory.
        let piece = object.data.slice(first as usize..=last as usize);
        let mut response = Response::new(Body::from(piece.clone()));
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        object_headers(response.headers_mut(), &object, false);
        let headers = response.headers_mut();
        set_header(headers, "content-length", piece.len().to_string());
        set_header(
            headers,
            "content-range",
            format!("bytes {first}-{last}/{size}"),
        );
        response
    }

    fn delete(&self, key: &str) -> Response {
        if self.objects.lock().remove(key).is_none() {
            return no_such_key();
        }
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        response
    }

    fn list(&self, query: &[(String, String)]) -> Response {
        let prefix = query_param(query, "prefix").unwrap_or("");
        let delimiter = query_param(query, "delimiter").filter(|d| !d.is_empty());
        let start_offset = query_param(query, "startOffset");
        let max_results = match query_param(query, "maxResults").map(str::parse::<usize>) {
            None => MAX_RESULTS,
            Some(Ok(n)) if n > 0 => n.min(MAX_RESULTS),
            Some(_) => return json_error(StatusCode::BAD_REQUEST, "bad maxResults"),
        };
        let resume = match query_param(query, "pageToken") {
            Some(token) => match decode_token(token) {
                Some(resume) => Some(resume),
                None => return json_error(StatusCode::BAD_REQUEST, "bad pageToken"),
            },
            None => None,
        };

        enum Entry {
            Object(String, StoredObject),
            Prefix(String),
        }
        let mut entries: Vec<Entry> = Vec::new();
        {
            let objects = self.objects.lock();
            for (key, object) in objects.range(prefix.to_string()..) {
                if !key.starts_with(prefix) {
                    break;
                }
                if start_offset.is_some_and(|offset| key.as_str() < offset) {
                    continue;
                }
                if let Some(resume) = &resume {
                    // A token that is a rolled-up prefix skips the names under it.
                    let under_resume = delimiter.is_some_and(|d| resume.ends_with(d))
                        && key.starts_with(resume.as_str());
                    if key.as_str() <= resume.as_str() || under_resume {
                        continue;
                    }
                }
                let rolled_up = delimiter.and_then(|d| {
                    let rest = &key[prefix.len()..];
                    rest.find(d)
                        .map(|at| format!("{prefix}{}", &rest[..at + d.len()]))
                });
                match rolled_up {
                    Some(common) => {
                        if !matches!(entries.last(), Some(Entry::Prefix(last)) if *last == common) {
                            entries.push(Entry::Prefix(common));
                        }
                    }
                    None => entries.push(Entry::Object(key.clone(), object.clone())),
                }
            }
        }
        let more = entries.len() > max_results;
        entries.truncate(max_results);
        let mut items = Vec::new();
        let mut prefixes = Vec::new();
        for entry in &entries {
            match entry {
                Entry::Object(name, object) => items.push(serde_json::json!({
                    "kind": "storage#object",
                    "name": name,
                    "bucket": self.bucket,
                    "generation": object.generation.to_string(),
                    "metageneration": "1",
                    "size": object.data.len().to_string(),
                    "updated": iso8601(object.last_modified_secs),
                    "etag": json_etag(object.generation),
                    "md5Hash": STANDARD.encode(md5(&object.data)),
                })),
                Entry::Prefix(common) => prefixes.push(common.clone()),
            }
        }
        let mut body = serde_json::json!({ "kind": "storage#objects" });
        if !items.is_empty() {
            body["items"] = serde_json::Value::Array(items);
        }
        if !prefixes.is_empty() {
            body["prefixes"] = serde_json::json!(prefixes);
        }
        if more && let Some(last) = entries.last() {
            let resume = match last {
                Entry::Object(name, _) => name,
                Entry::Prefix(common) => common,
            };
            body["nextPageToken"] = serde_json::json!(encode_token(resume));
        }
        json_response(StatusCode::OK, &body)
    }
}

/// An `etag` in the JSON API's style, which is not the object's XML ETag.
fn json_etag(generation: u64) -> String {
    STANDARD.encode(format!("generation {generation}"))
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response {
    let mut response = Response::new(Body::from(body.to_string()));
    *response.status_mut() = status;
    set_header(
        response.headers_mut(),
        "content-type",
        "application/json; charset=UTF-8".into(),
    );
    response
}

fn json_error(status: StatusCode, message: &str) -> Response {
    json_response(
        status,
        &serde_json::json!({ "error": { "code": status.as_u16(), "message": message } }),
    )
}

fn object_headers(headers: &mut HeaderMap, object: &StoredObject, with_length: bool) {
    set_header(headers, "etag", object.etag.clone());
    set_header(headers, "x-goog-generation", object.generation.to_string());
    set_header(headers, "x-goog-metageneration", "1".into());
    set_header(
        headers,
        "last-modified",
        imf_fixdate(object.last_modified_secs),
    );
    set_header(headers, "content-type", "application/octet-stream".into());
    if with_length {
        set_header(headers, "content-length", object.data.len().to_string());
    }
}

fn set_header(headers: &mut HeaderMap, name: &'static str, value: String) {
    if let Ok(value) = HeaderValue::from_str(&value) {
        headers.insert(name, value);
    }
}

fn no_such_key() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The specified key does not exist.",
    )
}

fn failure_code(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "AuthenticationRequired",
        403 => "AccessDenied",
        404 => "NoSuchKey",
        412 => "PreconditionFailed",
        429 => "SlowDown",
        503 => "ServiceUnavailable",
        _ => "InternalError",
    }
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    let mut xml = String::from("<?xml version='1.0' encoding='UTF-8'?><Error>");
    push_element(&mut xml, "Code", code);
    push_element(&mut xml, "Message", message);
    xml.push_str("</Error>");
    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = status;
    set_header(
        response.headers_mut(),
        "content-type",
        "application/xml".into(),
    );
    response
}

fn push_element(xml: &mut String, name: &str, text: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    for c in text.chars() {
        match c {
            '&' => xml.push_str("&amp;"),
            '<' => xml.push_str("&lt;"),
            '>' => xml.push_str("&gt;"),
            '"' => xml.push_str("&quot;"),
            '\'' => xml.push_str("&apos;"),
            c => xml.push(c),
        }
    }
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}

fn query_param<'a>(query: &'a [(String, String)], name: &str) -> Option<&'a str> {
    query
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode(name, true).unwrap_or_else(|| name.to_string()),
                percent_decode(value, true).unwrap_or_else(|| value.to_string()),
            )
        })
        .collect()
}

/// `%XX` decoded to its byte, and `+` to a space when `plus_is_space`. `None`
/// for a malformed escape or a result that is not UTF-8.
fn percent_decode(text: &str, plus_is_space: bool) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let high = bytes.get(i + 1).copied().and_then(hex_digit)?;
                let low = bytes.get(i + 2).copied().and_then(hex_digit)?;
                out.push(high << 4 | low);
                i += 3;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn encode_token(marker: &str) -> String {
    format!("t{}", hex(marker.as_bytes()))
}

fn decode_token(token: &str) -> Option<String> {
    let digits = token.strip_prefix('t')?.as_bytes();
    if digits.len() % 2 != 0 {
        return None;
    }
    let bytes = digits
        .chunks_exact(2)
        .map(|pair| Some(hex_digit(pair[0])? << 4 | hex_digit(pair[1])?))
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ByteRange {
    /// `bytes=a-b` (inclusive) or `bytes=a-`.
    From(u64, Option<u64>),
    /// `bytes=-n`.
    Suffix(u64),
}

fn parse_range(value: &str) -> Option<ByteRange> {
    let spec = value.trim().strip_prefix("bytes=")?;
    let (first, last) = spec.split_once('-')?;
    if first.is_empty() {
        return Some(ByteRange::Suffix(last.parse().ok()?));
    }
    let first = first.parse().ok()?;
    if last.is_empty() {
        return Some(ByteRange::From(first, None));
    }
    Some(ByteRange::From(first, Some(last.parse().ok()?)))
}

/// Year, month and day of the civil date `days` after 1970-01-01 (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `secs` as an HTTP IMF-fixdate, `Sun, 06 Nov 1994 08:49:37 GMT`.
fn imf_fixdate(secs: i64) -> String {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = secs.div_euclid(86_400);
    let of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    // 1970-01-01 was a Thursday.
    let weekday = WEEKDAYS[(days + 4).rem_euclid(7) as usize];
    let month = MONTHS[(month - 1) as usize];
    format!(
        "{weekday}, {day:02} {month} {year:04} {:02}:{:02}:{:02} GMT",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60
    )
}

/// `secs` as the JSON API's `updated`, `2026-09-21T14:13:20.000Z`.
fn iso8601(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.000Z",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60
    )
}

/// RFC 1321 MD5, for the ETag GCS gives a single-request upload.
fn md5(data: &[u8]) -> [u8; 16] {
    const SHIFTS: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76a_a478,
        0xe8c7_b756,
        0x2420_70db,
        0xc1bd_ceee,
        0xf57c_0faf,
        0x4787_c62a,
        0xa830_4613,
        0xfd46_9501,
        0x6980_98d8,
        0x8b44_f7af,
        0xffff_5bb1,
        0x895c_d7be,
        0x6b90_1122,
        0xfd98_7193,
        0xa679_438e,
        0x49b4_0821,
        0xf61e_2562,
        0xc040_b340,
        0x265e_5a51,
        0xe9b6_c7aa,
        0xd62f_105d,
        0x0244_1453,
        0xd8a1_e681,
        0xe7d3_fbc8,
        0x21e1_cde6,
        0xc337_07d6,
        0xf4d5_0d87,
        0x455a_14ed,
        0xa9e3_e905,
        0xfcef_a3f8,
        0x676f_02d9,
        0x8d2a_4c8a,
        0xfffa_3942,
        0x8771_f681,
        0x6d9d_6122,
        0xfde5_380c,
        0xa4be_ea44,
        0x4bde_cfa9,
        0xf6bb_4b60,
        0xbebf_bc70,
        0x289b_7ec6,
        0xeaa1_27fa,
        0xd4ef_3085,
        0x0488_1d05,
        0xd9d4_d039,
        0xe6db_99e5,
        0x1fa2_7cf8,
        0xc4ac_5665,
        0xf429_2244,
        0x432a_ff97,
        0xab94_23a7,
        0xfc93_a039,
        0x655b_59c3,
        0x8f0c_cc92,
        0xffef_f47d,
        0x8584_5dd1,
        0x6fa8_7e4f,
        0xfe2c_e6e0,
        0xa301_4314,
        0x4e08_11a1,
        0xf753_7e82,
        0xbd3a_f235,
        0x2ad7_d2bb,
        0xeb86_d391,
    ];
    let mut state: [u32; 4] = [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_le_bytes());
    for block in message.chunks_exact(64) {
        let mut words = [0u32; 16];
        for (word, bytes) in words.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        let [mut a, mut b, mut c, mut d] = state;
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(K[i]).wrapping_add(words[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(SHIFTS[i]));
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }
    let mut digest = [0u8; 16];
    for (out, word) in digest.chunks_exact_mut(4).zip(state) {
        out.copy_from_slice(&word.to_le_bytes());
    }
    digest
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn md5_matches_rfc_1321_vectors() {
        assert_eq!(hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            hex(&md5(
                b"12345678901234567890123456789012345678901234567890123456789012345678901234567890"
            )),
            "57edf4a22be3c955ac49da2e2107b67a"
        );
    }

    #[test]
    fn dates_format_as_http_and_xml_api_text() {
        assert_eq!(imf_fixdate(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(imf_fixdate(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(iso8601(1_790_000_000), "2026-09-21T14:13:20.000Z");
        assert_eq!(iso8601(951_782_400), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn page_tokens_round_trip() {
        for marker in ["", "a/b", "dir/", "ünï"] {
            assert_eq!(decode_token(&encode_token(marker)).as_deref(), Some(marker));
        }
        assert_eq!(decode_token("x00"), None);
        assert_eq!(decode_token("t0"), None);
    }

    #[test]
    fn ranges_parse() {
        assert_eq!(parse_range("bytes=2-5"), Some(ByteRange::From(2, Some(5))));
        assert_eq!(parse_range("bytes=2-"), Some(ByteRange::From(2, None)));
        assert_eq!(parse_range("bytes=-3"), Some(ByteRange::Suffix(3)));
        assert_eq!(parse_range("items=1-2"), None);
    }
}
