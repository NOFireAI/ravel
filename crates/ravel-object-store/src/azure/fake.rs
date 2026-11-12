//! An in-process fake of the Azure Blob service, enough of it for
//! [`AzureStore`] (ADR-4158 D8): one account and one container, blobs held in
//! memory, served path-style (`/{account}/{container}/{blob}`, as the
//! emulator is) over loopback HTTP by axum. Built for this crate's tests and,
//! behind the `test-support` feature, for other crates' tests.
//!
//! [`AzureStore`]: super::AzureStore
//!
//! # Requests served
//!
//! Every response, errors included, carries `Date` from the fake's settable
//! clock ([`FakeAzure::set_date_unix_secs`]), `x-ms-version` and
//! `x-ms-request-id`. An error carries its code in `x-ms-error-code` and, but
//! for a HEAD, in an XML `<Error><Code>` body. A request for another account
//! is 400 `InvalidUri`; one for another container is 404
//! `ContainerNotFound`. The blob name is the path after the container,
//! percent-decoded, `+` taken literally.
//!
//! - `PUT` a blob: Put Blob, storing the body under a new ETag
//!   (`"0x8DE..."`, unique per write). `If-None-Match: *` on a present blob is
//!   409 `BlobAlreadyExists`; an `If-Match` on an absent blob or naming another
//!   ETag is 412 `ConditionNotMet`. Answers 201 with `ETag` and
//!   `Last-Modified`.
//! - `PUT ?comp=block&blockid=B`: Put Block, holding the body as an
//!   uncommitted block of that blob, 201. `PUT ?comp=blocklist`: Put Block
//!   List, committing the listed blocks (`Latest`, `Uncommitted` or
//!   `Committed`, all looked up among the uncommitted ones) in order as the
//!   blob, 201 with `ETag`; a block it does not hold is 400
//!   `InvalidBlockList`, a body it cannot read 400 `InvalidXmlDocument`.
//! - `GET` a blob: 200, or 206 for `Range` (or `x-ms-range`) `bytes=a-b` or
//!   `bytes=a-`, clamped, with `Content-Range`. A suffix range `bytes=-n` is
//!   answered as Azure answers it: 200 and the whole blob. A range starting at
//!   or past the end, or any range on an empty blob, is 416 `InvalidRange`.
//!   `If-Match` naming another ETag is 412 `ConditionNotMet`. Any `versionid`
//!   is 404 `BlobNotFound`: the fake keeps no versions. A missing blob is 404
//!   `BlobNotFound`.
//! - `HEAD` a blob: the GET headers, no body; its errors keep only
//!   `x-ms-error-code`.
//! - `DELETE` a blob: 202, or 404 `BlobNotFound`.
//! - `GET /{account}/{container}?restype=container&comp=list`: List Blobs in
//!   byte order of the names, honouring `prefix`, `delimiter` (each
//!   `BlobPrefix` counts as one result), `startFrom` (inclusive; ignored when
//!   [`FakeAzure::set_honour_start_from`] says so), `marker` (an opaque token
//!   the fake issued as `NextMarker`) and `maxresults` (1 to 5000, default
//!   5000; anything else 400 `OutOfRangeQueryParameterValue`). A name holding
//!   a character XML cannot carry, or every name after
//!   [`FakeAzure::set_encode_all_names`], is sent `Encoded="true"` with every
//!   byte but ASCII letters and digits percent-encoded. The listed `Etag` is
//!   unquoted, as Azure lists it.
//! - `restype=account&comp=properties` on the account or the container path:
//!   Get Account Information, 200 with `x-ms-sku-name`, `x-ms-account-kind`
//!   and `x-ms-is-hns-enabled` as [`FakeAzure::set_hns`] sets it.
//!
//! Anything else is 501 `NotImplemented`. [`FakeAzure::fail_next`] and its
//! variants queue error answers for the next requests, whatever they are.
//! [`FakeAzure::require_shared_key`] makes every request prove a `SharedKey`
//! signature, else 403 `AuthenticationFailed`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use parking_lot::Mutex;

use super::list::{AccountKey, X_MS_VERSION, imf_fixdate, parse_xml, string_to_sign};
use super::{AzureConfig, TestAccountKey};
use crate::StoreError;
use crate::conformance::ForeignKeySeeder;

/// The fake's `Date` until [`FakeAzure::set_date_unix_secs`] moves it:
/// 2026-09-21T14:13:20Z.
const DEFAULT_DATE_UNIX_SECS: i64 = 1_790_000_000;

const MAX_RESULTS: usize = 5000;

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

    /// Whether this request addresses the blob `key` (any `comp`).
    pub fn is_blob(&self, account: &str, container: &str, key: &str) -> bool {
        percent_decode(&self.path, false).as_deref()
            == Some(format!("/{account}/{container}/{key}").as_str())
    }

    /// Whether this is a List Blobs request.
    pub fn is_list(&self) -> bool {
        self.query_param("comp") == Some("list")
    }
}

/// What Get Account Information says about hierarchical namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HnsAnswer {
    /// `x-ms-is-hns-enabled: false`, a flat account.
    False,
    /// `x-ms-is-hns-enabled: true`.
    True,
    /// No `x-ms-is-hns-enabled` header.
    Absent,
    /// `x-ms-is-hns-enabled: maybe`.
    Unreadable,
}

#[derive(Debug, Clone)]
struct StoredBlob {
    data: Bytes,
    etag: String,
    last_modified_secs: i64,
}

#[derive(Debug, Clone)]
struct Failure {
    status: u16,
    /// `None` sends neither `x-ms-error-code` nor a body.
    code: Option<String>,
}

#[derive(Debug)]
struct Inner {
    account: String,
    container: String,
    blobs: Mutex<BTreeMap<String, StoredBlob>>,
    /// Uncommitted blocks by blob name and block id.
    blocks: Mutex<HashMap<String, HashMap<String, Bytes>>>,
    next_etag: AtomicU64,
    next_request_id: AtomicU64,
    requests: Mutex<Vec<RecordedRequest>>,
    failures: Mutex<VecDeque<Failure>>,
    /// Blobs to overwrite, one per successful HEAD of that name.
    replace_after_head: Mutex<VecDeque<(String, Bytes)>>,
    date_secs: AtomicI64,
    honour_start_from: AtomicBool,
    encode_all_names: AtomicBool,
    hns: Mutex<HnsAnswer>,
    shared_key: Mutex<Option<AccountKey>>,
}

/// A running fake Azure Blob endpoint. Dropping it stops the server.
#[derive(Debug)]
pub struct FakeAzure {
    inner: Arc<Inner>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for FakeAzure {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeAzure {
    /// Bind a loopback port and serve `container` of `account` from it.
    pub async fn start(account: &str, container: &str) -> std::io::Result<FakeAzure> {
        let inner = Arc::new(Inner {
            account: account.to_string(),
            container: container.to_string(),
            blobs: Mutex::new(BTreeMap::new()),
            blocks: Mutex::new(HashMap::new()),
            next_etag: AtomicU64::new(1),
            next_request_id: AtomicU64::new(1),
            requests: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::new()),
            replace_after_head: Mutex::new(VecDeque::new()),
            date_secs: AtomicI64::new(DEFAULT_DATE_UNIX_SECS),
            honour_start_from: AtomicBool::new(true),
            encode_all_names: AtomicBool::new(false),
            hns: Mutex::new(HnsAnswer::False),
            shared_key: Mutex::new(None),
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
        Ok(FakeAzure {
            inner,
            endpoint: format!("http://{addr}/{account}"),
            server,
        })
    }

    /// The account URL, `http://127.0.0.1:<port>/<account>`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn account(&self) -> &str {
        &self.inner.account
    }

    pub fn container(&self) -> &str {
        &self.inner.container
    }

    /// An [`AzureConfig`] for this endpoint: plain HTTP, unsigned, and at most
    /// two retries with a 1 ms backoff so a scripted failure costs no time.
    pub fn config(&self) -> AzureConfig {
        self.config_for_container(&self.inner.container)
    }

    /// As [`Self::config`], for a container this fake does not serve, to
    /// reach its `ContainerNotFound` answers.
    pub fn config_for_container(&self, container: &str) -> AzureConfig {
        let mut config = AzureConfig::new(self.inner.account.clone(), container);
        config.endpoint = Some(self.endpoint.clone());
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

    /// As [`Self::config`], signing every request with the base64 shared key
    /// `key` instead of skipping the signature.
    pub fn config_with_key(&self, key: &str) -> AzureConfig {
        let mut config = self.config();
        config.skip_signature = false;
        config.test_account_key = Some(TestAccountKey(key.to_string()));
        config
    }

    /// Every request received so far, in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.inner.requests.lock().clone()
    }

    pub fn clear_requests(&self) {
        self.inner.requests.lock().clear();
    }

    /// Answer the next `times` requests with `status` and a code chosen for
    /// it instead of serving them.
    pub fn fail_next(&self, status: u16, times: usize) {
        self.fail_next_with_code(status, failure_code(status), times);
    }

    /// As [`Self::fail_next`], with the error code `code`.
    pub fn fail_next_with_code(&self, status: u16, code: &str, times: usize) {
        let failure = Failure {
            status,
            code: Some(code.to_string()),
        };
        self.inner
            .failures
            .lock()
            .extend(std::iter::repeat_n(failure, times));
    }

    /// As [`Self::fail_next`], with no code and no body: the status is all
    /// the client gets.
    pub fn fail_next_without_code(&self, status: u16, times: usize) {
        let failure = Failure { status, code: None };
        self.inner
            .failures
            .lock()
            .extend(std::iter::repeat_n(failure, times));
    }

    /// After the next successful HEAD of `key`, overwrite it with `data`, as
    /// another writer racing a suffix read would. Each call queues one.
    pub fn replace_after_head(&self, key: &str, data: impl Into<Bytes>) {
        self.inner
            .replace_after_head
            .lock()
            .push_back((key.to_string(), data.into()));
    }

    /// Whether List Blobs honours `startFrom` (the default) or ignores it.
    pub fn set_honour_start_from(&self, honour: bool) {
        self.inner
            .honour_start_from
            .store(honour, Ordering::Relaxed);
    }

    /// Whether List Blobs sends every name `Encoded="true"`.
    pub fn set_encode_all_names(&self, encode: bool) {
        self.inner.encode_all_names.store(encode, Ordering::Relaxed);
    }

    /// What Get Account Information reports for hierarchical namespace.
    pub fn set_hns(&self, answer: HnsAnswer) {
        *self.inner.hns.lock() = answer;
    }

    /// Require a valid `SharedKey` signature with the base64 key `key` on
    /// every later request.
    pub fn require_shared_key(&self, key: &str) {
        *self.inner.shared_key.lock() = AccountKey::decode(key);
    }

    /// Set the clock every later response's `Date` reports.
    pub fn set_date_unix_secs(&self, secs: i64) {
        self.inner.date_secs.store(secs, Ordering::Relaxed);
    }

    /// The fake's clock in unix nanoseconds, as a `Date` header carries it.
    pub fn date_unix_ns(&self) -> i64 {
        self.inner
            .date_secs
            .load(Ordering::Relaxed)
            .saturating_mul(1_000_000_000)
    }

    /// The current ETag of `key`, quotes included.
    pub fn etag_of(&self, key: &str) -> Option<String> {
        self.inner.blobs.lock().get(key).map(|b| b.etag.clone())
    }

    /// The stored bytes of `key`.
    pub fn data_of(&self, key: &str) -> Option<Bytes> {
        self.inner.blobs.lock().get(key).map(|b| b.data.clone())
    }

    /// How many uncommitted blocks the fake holds, over every blob.
    pub fn uncommitted_blocks(&self) -> usize {
        self.inner.blocks.lock().values().map(HashMap::len).sum()
    }

    /// Store `data` under `key` without a request, as another writer would.
    /// `key` is taken as is, so a name `AzureStore` cannot address can be
    /// planted for a listing to report. The new ETag.
    pub fn insert_blob(&self, key: &str, data: impl Into<Bytes>) -> String {
        let blob = self.inner.new_blob(data.into());
        let etag = blob.etag.clone();
        self.inner.blobs.lock().insert(key.to_string(), blob);
        etag
    }

    /// Remove `key` without a request. Whether it was present.
    pub fn remove_blob(&self, key: &str) -> bool {
        self.inner.blobs.lock().remove(key).is_some()
    }
}

/// Seeds the unaddressable keys the conformance suite plants, as another
/// writer would.
#[async_trait::async_trait]
impl ForeignKeySeeder for FakeAzure {
    async fn seed(&self, key: &str, data: Bytes) -> Result<(), StoreError> {
        self.insert_blob(key, data);
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<(), StoreError> {
        self.remove_blob(key);
        Ok(())
    }
}

/// What a request path addresses.
enum Target {
    Account,
    Container(String),
    Blob(String, String),
}

impl Inner {
    fn new_blob(&self, data: Bytes) -> StoredBlob {
        StoredBlob {
            data,
            etag: format!(
                "\"0x8DE{:013X}\"",
                self.next_etag.fetch_add(1, Ordering::Relaxed)
            ),
            last_modified_secs: self.date_secs.load(Ordering::Relaxed),
        }
    }

    async fn handle(&self, request: axum::extract::Request) -> Response {
        let (parts, body) = request.into_parts();
        let query = parts.uri.query().map(parse_query).unwrap_or_default();
        let path = parts.uri.path().to_string();
        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .map(|(n, v)| {
                (
                    n.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        self.requests.lock().push(RecordedRequest {
            method: parts.method.to_string(),
            path: path.clone(),
            query: query.clone(),
            headers: headers.clone(),
        });
        let mut response = if self.signature_ok(parts.method.as_str(), &headers, &path, &query) {
            match axum::body::to_bytes(body, usize::MAX).await {
                Ok(body) => self.route(&parts.method, &path, &query, &parts.headers, body),
                Err(_) => error(StatusCode::BAD_REQUEST, "InvalidInput"),
            }
        } else {
            error(StatusCode::FORBIDDEN, "AuthenticationFailed")
        };
        if parts.method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let headers = response.headers_mut();
        set_header(
            headers,
            "date",
            imf_fixdate(self.date_secs.load(Ordering::Relaxed)),
        );
        set_header(headers, "x-ms-version", X_MS_VERSION.to_string());
        set_header(
            headers,
            "x-ms-request-id",
            format!("00000000-0000-0000-0000-{request_id:012x}"),
        );
        response
    }

    fn signature_ok(
        &self,
        method: &str,
        headers: &[(String, String)],
        path: &str,
        query: &[(String, String)],
    ) -> bool {
        let Some(key) = self.shared_key.lock().clone() else {
            return true;
        };
        let Some(authorization) = headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.as_str())
        else {
            return false;
        };
        let to_sign = string_to_sign(method, headers, &self.account, path, query);
        let expected = format!("SharedKey {}:{}", self.account, key.sign(&to_sign));
        authorization == expected
    }

    fn route(
        &self,
        method: &Method,
        path: &str,
        query: &[(String, String)],
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response {
        if let Some(failure) = self.failures.lock().pop_front() {
            let status =
                StatusCode::from_u16(failure.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            return match failure.code {
                Some(code) => error(status, &code),
                None => {
                    let mut response = Response::new(Body::empty());
                    *response.status_mut() = status;
                    response
                }
            };
        }
        let Some(target) = self.target(path) else {
            return error(StatusCode::BAD_REQUEST, "InvalidUri");
        };
        let restype = query_param(query, "restype");
        let comp = query_param(query, "comp");
        if restype == Some("account") && comp == Some("properties") {
            return match (method, &target) {
                (&Method::GET | &Method::HEAD, Target::Account | Target::Container(_)) => {
                    self.account_information()
                }
                _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
            };
        }
        let (container, blob) = match target {
            Target::Account => return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
            Target::Container(container) => (container, None),
            Target::Blob(container, blob) => (container, Some(blob)),
        };
        if container != self.container {
            return error(StatusCode::NOT_FOUND, "ContainerNotFound");
        }
        let Some(key) = blob else {
            return match (method, restype, comp) {
                (&Method::GET, Some("container"), Some("list")) => self.list(query),
                _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
            };
        };
        let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
        match (method, comp) {
            (&Method::PUT, None) => {
                self.put_blob(&key, header("if-match"), header("if-none-match"), body)
            }
            (&Method::PUT, Some("block")) => match query_param(query, "blockid") {
                Some(id) => {
                    self.blocks
                        .lock()
                        .entry(key)
                        .or_default()
                        .insert(id.to_string(), body);
                    created(None)
                }
                None => error(StatusCode::BAD_REQUEST, "MissingRequiredQueryParameter"),
            },
            (&Method::PUT, Some("blocklist")) => self.put_block_list(&key, &body),
            (&Method::GET | &Method::HEAD, None) => {
                if query_param(query, "versionid").is_some() {
                    return error(StatusCode::NOT_FOUND, "BlobNotFound");
                }
                let range = header("x-ms-range").or_else(|| header("range"));
                let response = self.get_blob(&key, range, header("if-match"));
                if method == Method::HEAD && response.status() == StatusCode::OK {
                    self.after_head(&key);
                }
                response
            }
            (&Method::DELETE, None) => match self.blobs.lock().remove(&key) {
                Some(_) => {
                    let mut response = Response::new(Body::empty());
                    *response.status_mut() = StatusCode::ACCEPTED;
                    response
                }
                None => error(StatusCode::NOT_FOUND, "BlobNotFound"),
            },
            _ => error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
        }
    }

    /// What `path` addresses, or `None` when it names another account or does
    /// not percent-decode (the service's 400 `InvalidUri`).
    fn target(&self, path: &str) -> Option<Target> {
        let rest = path.strip_prefix('/').unwrap_or(path);
        let (account, rest) = match rest.split_once('/') {
            Some((account, rest)) => (account, Some(rest)),
            None => (rest, None),
        };
        if percent_decode(account, false).as_deref() != Some(self.account.as_str()) {
            return None;
        }
        let Some(rest) = rest.filter(|rest| !rest.is_empty()) else {
            return Some(Target::Account);
        };
        let decode = |text: &str| percent_decode(text, false);
        match rest.split_once('/') {
            Some((container, blob)) if !blob.is_empty() => {
                Some(Target::Blob(decode(container)?, decode(blob)?))
            }
            Some((container, _)) => Some(Target::Container(decode(container)?)),
            None => Some(Target::Container(decode(rest)?)),
        }
    }

    fn account_information(&self) -> Response {
        let mut response = Response::new(Body::empty());
        let headers = response.headers_mut();
        set_header(headers, "x-ms-sku-name", "Standard_LRS".into());
        set_header(headers, "x-ms-account-kind", "StorageV2".into());
        match *self.hns.lock() {
            HnsAnswer::False => set_header(headers, "x-ms-is-hns-enabled", "false".into()),
            HnsAnswer::True => set_header(headers, "x-ms-is-hns-enabled", "true".into()),
            HnsAnswer::Unreadable => set_header(headers, "x-ms-is-hns-enabled", "maybe".into()),
            HnsAnswer::Absent => {}
        }
        response
    }

    fn put_blob(
        &self,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        body: Bytes,
    ) -> Response {
        let mut blobs = self.blobs.lock();
        let current = blobs.get(key);
        if if_none_match == Some("*") && current.is_some() {
            return error(StatusCode::CONFLICT, "BlobAlreadyExists");
        }
        if let Some(expected) = if_match
            && current.is_none_or(|blob| blob.etag != expected)
        {
            return error(StatusCode::PRECONDITION_FAILED, "ConditionNotMet");
        }
        let blob = self.new_blob(body);
        let response = created(Some(&blob));
        blobs.insert(key.to_string(), blob);
        response
    }

    fn put_block_list(&self, key: &str, body: &[u8]) -> Response {
        let Ok(root) = parse_xml(body) else {
            return error(StatusCode::BAD_REQUEST, "InvalidXmlDocument");
        };
        if root.name != "BlockList" {
            return error(StatusCode::BAD_REQUEST, "InvalidXmlDocument");
        }
        let mut blocks = self.blocks.lock();
        let held = blocks.get(key);
        let mut data = Vec::new();
        for entry in &root.children {
            if !matches!(entry.name.as_str(), "Latest" | "Uncommitted" | "Committed") {
                return error(StatusCode::BAD_REQUEST, "InvalidXmlDocument");
            }
            match held.and_then(|held| held.get(entry.text.trim())) {
                Some(block) => data.extend_from_slice(block),
                None => return error(StatusCode::BAD_REQUEST, "InvalidBlockList"),
            }
        }
        blocks.remove(key);
        drop(blocks);
        let blob = self.new_blob(Bytes::from(data));
        let response = created(Some(&blob));
        self.blobs.lock().insert(key.to_string(), blob);
        response
    }

    fn get_blob(&self, key: &str, range: Option<&str>, if_match: Option<&str>) -> Response {
        let Some(blob) = self.blobs.lock().get(key).cloned() else {
            return error(StatusCode::NOT_FOUND, "BlobNotFound");
        };
        if let Some(expected) = if_match
            && expected != blob.etag
            && expected != "*"
        {
            return error(StatusCode::PRECONDITION_FAILED, "ConditionNotMet");
        }
        let size = blob.data.len() as u64;
        let bounds = match range.and_then(parse_range) {
            // Azure answers a suffix range with the whole blob.
            None | Some(ByteRange::Suffix(_)) => None,
            Some(ByteRange::From(first, last)) => {
                if first >= size {
                    return error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange");
                }
                let last = last.map_or(size - 1, |last| last.min(size - 1));
                if last < first {
                    return error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange");
                }
                Some((first, last))
            }
        };
        let (status, data) = match bounds {
            None => (StatusCode::OK, blob.data.clone()),
            Some((first, last)) => (
                StatusCode::PARTIAL_CONTENT,
                blob.data.slice(first as usize..=last as usize),
            ),
        };
        let length = data.len();
        let mut response = Response::new(Body::from(data));
        *response.status_mut() = status;
        let headers = response.headers_mut();
        blob_headers(headers, &blob);
        set_header(headers, "content-length", length.to_string());
        set_header(headers, "x-ms-blob-type", "BlockBlob".into());
        set_header(headers, "accept-ranges", "bytes".into());
        if let Some((first, last)) = bounds {
            set_header(
                headers,
                "content-range",
                format!("bytes {first}-{last}/{size}"),
            );
        }
        response
    }

    fn after_head(&self, key: &str) {
        let mut queue = self.replace_after_head.lock();
        if queue.front().is_some_and(|(name, _)| name == key)
            && let Some((name, data)) = queue.pop_front()
        {
            drop(queue);
            let blob = self.new_blob(data);
            self.blobs.lock().insert(name, blob);
        }
    }

    fn list(&self, query: &[(String, String)]) -> Response {
        let prefix = query_param(query, "prefix").unwrap_or("");
        let delimiter = query_param(query, "delimiter").filter(|d| !d.is_empty());
        let max_results = match query_param(query, "maxresults") {
            None => MAX_RESULTS,
            Some(text) => match text.parse::<usize>() {
                Ok(n) if (1..=MAX_RESULTS).contains(&n) => n,
                _ => return error(StatusCode::BAD_REQUEST, "OutOfRangeQueryParameterValue"),
            },
        };
        let marker = match query_param(query, "marker").filter(|m| !m.is_empty()) {
            None => None,
            Some(token) => match decode_token(token) {
                Some(after) => Some(after),
                None => return error(StatusCode::BAD_REQUEST, "OutOfRangeInput"),
            },
        };
        let start_from = query_param(query, "startFrom")
            .filter(|_| self.honour_start_from.load(Ordering::Relaxed));
        let blobs = self.blobs.lock().clone();
        let mut entries: Vec<(String, Option<StoredBlob>)> = Vec::new();
        for (name, blob) in blobs.range(prefix.to_string()..) {
            if !name.starts_with(prefix) {
                break;
            }
            if start_from.is_some_and(|start| name.as_str() < start) {
                continue;
            }
            if let Some(after) = &marker
                && (name <= after
                    || (delimiter.is_some()
                        && name.starts_with(after.as_str())
                        && after.ends_with('/')))
            {
                continue;
            }
            let rolled = delimiter.and_then(|d| {
                name[prefix.len()..]
                    .find(d)
                    .map(|at| name[..prefix.len() + at + d.len()].to_string())
            });
            match rolled {
                Some(common) => {
                    if entries
                        .last()
                        .is_none_or(|(last, blob)| blob.is_some() || *last != common)
                    {
                        entries.push((common, None));
                    }
                }
                None => entries.push((name.clone(), Some(blob.clone()))),
            }
        }
        let more = entries.len() > max_results;
        entries.truncate(max_results);
        let encode_all = self.encode_all_names.load(Ordering::Relaxed);
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
        xml.push_str(&format!(
            "<EnumerationResults ServiceEndpoint=\"http://127.0.0.1/{}\" ContainerName=\"{}\">",
            self.account, self.container
        ));
        push_element(&mut xml, "Prefix", prefix);
        if let Some(marker) = query_param(query, "marker") {
            push_element(&mut xml, "Marker", marker);
        }
        push_element(&mut xml, "MaxResults", &max_results.to_string());
        if let Some(delimiter) = delimiter {
            push_element(&mut xml, "Delimiter", delimiter);
        }
        xml.push_str("<Blobs>");
        for (name, blob) in &entries {
            match blob {
                Some(blob) => {
                    xml.push_str("<Blob>");
                    push_name(&mut xml, name, encode_all);
                    xml.push_str("<Properties>");
                    push_element(
                        &mut xml,
                        "Last-Modified",
                        &imf_fixdate(blob.last_modified_secs),
                    );
                    push_element(&mut xml, "Etag", blob.etag.trim_matches('"'));
                    push_element(&mut xml, "Content-Length", &blob.data.len().to_string());
                    push_element(&mut xml, "BlobType", "BlockBlob");
                    xml.push_str("</Properties></Blob>");
                }
                None => {
                    xml.push_str("<BlobPrefix>");
                    push_name(&mut xml, name, encode_all);
                    xml.push_str("</BlobPrefix>");
                }
            }
        }
        xml.push_str("</Blobs>");
        match entries.last().filter(|_| more) {
            Some((last, _)) => push_element(&mut xml, "NextMarker", &encode_token(last)),
            None => xml.push_str("<NextMarker />"),
        }
        xml.push_str("</EnumerationResults>");
        let mut response = Response::new(Body::from(xml));
        set_header(
            response.headers_mut(),
            "content-type",
            "application/xml".into(),
        );
        response
    }
}

fn blob_headers(headers: &mut HeaderMap, blob: &StoredBlob) {
    set_header(headers, "etag", blob.etag.clone());
    set_header(
        headers,
        "last-modified",
        imf_fixdate(blob.last_modified_secs),
    );
}

/// 201, with the written blob's `ETag` and `Last-Modified` when there is one.
fn created(blob: Option<&StoredBlob>) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::CREATED;
    if let Some(blob) = blob {
        blob_headers(response.headers_mut(), blob);
    }
    response
}

/// A `Name` element, `Encoded="true"` when XML cannot carry the name or
/// `encode_all` asks for it.
fn push_name(xml: &mut String, name: &str, encode_all: bool) {
    if encode_all || name.chars().any(xml_invalid) {
        xml.push_str("<Name Encoded=\"true\">");
        for byte in name.bytes() {
            if byte.is_ascii_alphanumeric() {
                xml.push(char::from(byte));
            } else {
                xml.push_str(&format!("%{byte:02X}"));
            }
        }
        xml.push_str("</Name>");
    } else {
        push_element(xml, "Name", name);
    }
}

/// Whether XML 1.0 cannot carry `c` in text.
fn xml_invalid(c: char) -> bool {
    matches!(c, '\u{0}'..='\u{8}' | '\u{B}' | '\u{C}' | '\u{E}'..='\u{1F}' | '\u{FFFE}' | '\u{FFFF}')
}

fn failure_code(status: u16) -> &'static str {
    match status {
        401 => "NoAuthenticationInformation",
        403 => "AuthorizationFailure",
        404 => "BlobNotFound",
        409 => "SnapshotsPresent",
        412 => "ConditionNotMet",
        416 => "InvalidRange",
        429 => "TooManyRequests",
        500 => "OperationTimedOut",
        503 => "ServerBusy",
        _ => "InternalError",
    }
}

fn error(status: StatusCode, code: &str) -> Response {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error>");
    push_element(&mut xml, "Code", code);
    push_element(&mut xml, "Message", "fake Azure error");
    xml.push_str("</Error>");
    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    set_header(headers, "content-type", "application/xml".into());
    set_header(headers, "x-ms-error-code", code.to_string());
    response
}

fn set_header(headers: &mut HeaderMap, name: &'static str, value: String) {
    if let Ok(value) = HeaderValue::from_str(&value) {
        headers.insert(name, value);
    }
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

/// A `NextMarker`: opaque, so a client that sends a name in its place gets a
/// 400 rather than a plausible page.
fn encode_token(name: &str) -> String {
    let hex: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    format!("m{hex}")
}

fn decode_token(token: &str) -> Option<String> {
    let digits = token.strip_prefix('m')?.as_bytes();
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
