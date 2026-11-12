//! `AzureStore`'s own Blob service REST client (ADR-4158 D1, D2, D6): List
//! Blobs, Get Blob Properties (the HEAD a `head`, a `pin_of` and a suffix
//! read send) and Get Account Information.
//!
//! A List Blobs request is one `GET <account>/<container>?restype=container&
//! comp=list` carrying `prefix`, `maxresults`, `delimiter=/` for a delimited
//! listing, `startFrom` to resume after a key, and the service's opaque
//! `marker` to continue within one call. `maxresults` is the store's
//! configured page size for `list` and `list_after` and the wire maximum the
//! other listers use ([`WIRE_PAGE_KEYS`]) for `list_delimited`. A `Name` with
//! `Encoded="true"` is percent-decoded as a whole string. Names go to
//! [`crate::classify_objects`] and [`crate::classify_prefixes`] as decoded, so
//! a name `object_store`'s `Path` would rewrite is reported as unaddressable
//! rather than failing the page.
//!
//! Every request carries `x-ms-version` ([`X_MS_VERSION`]) and goes through the
//! data plane's [`S3HttpConnector`] recording around a reqwest client with
//! redirects off, inside the caller's [`crate::s3::connector::scope`]. It is
//! authorized with the credential the data-plane client resolved: a bearer
//! token as `Authorization: Bearer`, an account key as `SharedKey` signed
//! here, since `object_store` keeps its decoded key private. No error carries
//! a response body.
//!
//! [`S3HttpConnector`]: crate::s3::connector::S3HttpConnector

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use object_store::RetryConfig;
use object_store::azure::{AzureCredential, AzureCredentialProvider};
use object_store::client::{
    HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse,
};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, IF_MATCH};

use crate::s3::SigningClock;
use crate::s3::http_date::parse_imf_fixdate_ns;
use crate::s3::list::{Backoff, read_body, status_retryable, transport_retryable};
use crate::{DelimitedList, Etag, ListPage, ObjectMeta, PageToken, Pin, StoreError, Version};

/// The Blob service version every request asks for. `startFrom` and the
/// `Encoded` name attribute need 2023-05-03 or later; this is the version
/// `object_store` 0.14 sends on its own requests.
pub(crate) const X_MS_VERSION: &str = "2023-11-03";

/// Largest List Blobs response body read.
const LIST_MAX_BODY_BYTES: usize = crate::s3::list::LIST_MAX_BODY_BYTES;

/// Largest error body read for its `<Code>` when `x-ms-error-code` is absent.
const ERROR_BODY_BYTES: usize = 64 << 10;

/// The `maxresults` a `list_delimited` request sends, as the S3 and GCS
/// listers do for a delimited drain.
const WIRE_PAGE_KEYS: usize = 1000;

/// Consecutive responses listing nothing at all one `list` call accepts
/// before it fails with [`StoreError::ListPageCeiling`], as on S3 and GCS.
/// A response whose names the `start_after` filter removed does not count:
/// that is a service ignoring `startFrom`, which costs requests, not
/// correctness (ADR-4158 D1).
const EMPTY_RESPONSE_ALLOWANCE: usize = 16;

const X_MS_VERSION_HEADER: HeaderName = HeaderName::from_static("x-ms-version");
const X_MS_DATE: HeaderName = HeaderName::from_static("x-ms-date");

/// What a blob's properties say about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobHead {
    pub(crate) size: u64,
    /// The `ETag` header exactly as sent, quotes included.
    pub(crate) etag: String,
    /// `x-ms-version-id`, present when the account has blob versioning on.
    pub(crate) version_id: Option<String>,
    pub(crate) last_modified_unix_ms: i64,
}

/// The account key, decoded, for `SharedKey` signing. Never printed.
#[derive(Clone)]
pub(crate) struct AccountKey(Vec<u8>);

impl AccountKey {
    /// The key from its base64 text, or `None` when it is not base64.
    pub(crate) fn decode(text: &str) -> Option<Self> {
        STANDARD.decode(text.trim()).ok().map(AccountKey)
    }

    pub(crate) fn sign(&self, string_to_sign: &str) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &self.0);
        STANDARD.encode(ring::hmac::sign(&key, string_to_sign.as_bytes()).as_ref())
    }
}

impl fmt::Debug for AccountKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AccountKey(<redacted>)")
    }
}

/// Sends Blob service requests for one container.
pub(crate) struct AzureRestClient {
    http: HttpClient,
    /// `None` sends no `Authorization` header (a test endpoint only).
    credentials: Option<AzureCredentialProvider>,
    /// The key an `AccessKey` credential signs with.
    account_key: Option<AccountKey>,
    clock: Arc<dyn SigningClock>,
    retry: RetryConfig,
    account: String,
    /// `<account url>/<container>`, no trailing slash.
    container_url: String,
    /// The path part of `container_url`, which `SharedKey` signs.
    container_path: String,
    page_size: usize,
}

impl fmt::Debug for AzureRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureRestClient")
            .field("account", &self.account)
            .field("container_url", &self.container_url)
            .field("signed", &self.credentials.is_some())
            .field("account_key", &self.account_key)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive()
    }
}

/// How to build an [`AzureRestClient`].
pub(crate) struct RestClientParts {
    pub(crate) http: HttpClient,
    pub(crate) credentials: Option<AzureCredentialProvider>,
    pub(crate) account_key: Option<AccountKey>,
    pub(crate) clock: Arc<dyn SigningClock>,
    pub(crate) retry: RetryConfig,
    pub(crate) account: String,
    /// The service URL: the endpoint, or `https://<account>.blob.core.windows.net`.
    pub(crate) account_url: String,
    pub(crate) container: String,
    pub(crate) page_size: usize,
}

/// One List Blobs request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListRequest<'a> {
    pub(crate) prefix: &'a str,
    pub(crate) start_from: Option<&'a str>,
    pub(crate) marker: Option<&'a str>,
    pub(crate) delimited: bool,
}

/// One parsed List Blobs response, names decoded.
#[derive(Debug, Default)]
pub(crate) struct ListResponse {
    pub(crate) items: Vec<ObjectMeta>,
    pub(crate) prefixes: Vec<String>,
    pub(crate) next_marker: Option<String>,
}

/// Where the paging loops get their responses: [`AzureRestClient`] in
/// production, a script in the `azure` module's tests.
pub(crate) trait ListFetch {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError>;
}

/// One [`ListPage`]: up to `page_size` names under `prefix` strictly after
/// `start_after`, following `NextMarker` within the call. `startFrom` is
/// inclusive and may be ignored, so every name not greater than
/// `start_after` is dropped here. When the filter dropped a name and names
/// remain, the call returns them with a page token rather than sending
/// another request. When names remain, the page token is the raw last name.
pub(crate) async fn list_page<F: ListFetch>(
    fetch: &F,
    prefix: &str,
    start_after: Option<&str>,
    page_size: usize,
) -> Result<ListPage, StoreError> {
    let page_size = page_size.max(1);
    let mut listed: Vec<ObjectMeta> = Vec::new();
    let mut marker: Option<String> = None;
    let mut empty_responses = 0usize;
    let more = loop {
        if empty_responses == EMPTY_RESPONSE_ALLOWANCE {
            return Err(StoreError::ListPageCeiling {
                prefix: prefix.to_string(),
                ceiling: EMPTY_RESPONSE_ALLOWANCE,
            });
        }
        let request = ListRequest {
            prefix,
            start_from: start_after,
            marker: marker.as_deref(),
            delimited: false,
        };
        let response = fetch.fetch(&request).await?;
        let mut items = response.items;
        let received = items.len();
        if let Some(start_after) = start_after {
            items.retain(|meta| meta.key.as_str() > start_after);
        }
        let dropped = items.len() < received;
        if received == 0 {
            empty_responses += 1;
        } else {
            empty_responses = 0;
        }
        listed.extend(items);
        let Some(next) = response.next_marker else {
            break listed.len() > page_size;
        };
        if listed.len() >= page_size || (dropped && !listed.is_empty()) {
            break true;
        }
        marker = Some(next_marker(prefix, marker.as_deref(), next)?);
    };
    listed.truncate(page_size);
    let next = if more {
        listed.last().map(|meta| PageToken(meta.key.clone()))
    } else {
        None
    };
    let (objects, unaddressable) = crate::classify_objects(prefix, listed);
    Ok(ListPage {
        objects,
        next,
        unaddressable,
    })
}

/// The whole delimited listing under `prefix`, following `NextMarker` until a
/// response carries none, in at most `ceiling` responses. A prefix two
/// responses both report is listed once.
pub(crate) async fn list_delimited<F: ListFetch>(
    fetch: &F,
    prefix: &str,
    ceiling: usize,
) -> Result<DelimitedList, StoreError> {
    let mut listed: Vec<ObjectMeta> = Vec::new();
    let mut listed_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();
    let mut marker: Option<String> = None;
    let mut responses = 0usize;
    loop {
        if responses == ceiling {
            return Err(StoreError::ListPageCeiling {
                prefix: prefix.to_string(),
                ceiling,
            });
        }
        let request = ListRequest {
            prefix,
            start_from: None,
            marker: marker.as_deref(),
            delimited: true,
        };
        let response = fetch.fetch(&request).await?;
        responses += 1;
        listed.extend(response.items);
        for common in response.prefixes {
            if seen_prefixes.insert(common.clone()) {
                listed_prefixes.push(common);
            }
        }
        let Some(next) = response.next_marker else {
            break;
        };
        marker = Some(next_marker(prefix, marker.as_deref(), next)?);
    }
    let (objects, unaddressable) = crate::classify_objects(prefix, listed);
    let (common_prefixes, unaddressable_prefixes) =
        crate::classify_prefixes(prefix, listed_prefixes);
    Ok(DelimitedList {
        objects,
        common_prefixes,
        unaddressable,
        unaddressable_prefixes,
    })
}

/// The marker to follow a page with: refused when it repeats the one just
/// sent.
fn next_marker(prefix: &str, sent: Option<&str>, received: String) -> Result<String, StoreError> {
    if sent == Some(received.as_str()) {
        return Err(StoreError::ListRepeatedToken {
            prefix: prefix.to_string(),
        });
    }
    Ok(received)
}

impl ListFetch for AzureRestClient {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
        let mut query: Vec<(&str, String)> = vec![
            ("restype", "container".to_string()),
            ("comp", "list".to_string()),
        ];
        if !request.prefix.is_empty() {
            query.push(("prefix", request.prefix.to_string()));
        }
        if request.delimited {
            query.push(("delimiter", "/".to_string()));
        }
        if let Some(start_from) = request.start_from {
            query.push(("startFrom", start_from.to_string()));
        }
        if let Some(marker) = request.marker {
            query.push(("marker", marker.to_string()));
        }
        let max_results = if request.delimited {
            WIRE_PAGE_KEYS
        } else {
            self.page_size.max(1)
        };
        query.push(("maxresults", max_results.to_string()));
        let context = format!("listing {:?}", request.prefix);
        let (_, body) = self
            .send(
                &context,
                Method::GET,
                &self.container_path,
                &query,
                None,
                Subject::Container,
            )
            .await?;
        parse_list(request.prefix, &body)
    }
}

/// What a 404 means for one request (ADR-4158 D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Subject {
    /// A blob request: a 404 is [`StoreError::NotFound`] unless its code is
    /// `ContainerNotFound`.
    Blob,
    /// A listing or an account request: any 404 is the container missing.
    Container,
}

impl AzureRestClient {
    pub(crate) fn new(parts: RestClientParts) -> Self {
        let account_url = parts.account_url.trim_end_matches('/');
        let container = encode_component(&parts.container);
        let container_url = format!("{account_url}/{container}");
        let container_path = url_path(&container_url);
        AzureRestClient {
            http: parts.http,
            credentials: parts.credentials,
            account_key: parts.account_key,
            clock: parts.clock,
            retry: parts.retry,
            account: parts.account,
            container_url,
            container_path,
            page_size: parts.page_size,
        }
    }

    pub(crate) async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        list_page(self, prefix, start_after, self.page_size).await
    }

    pub(crate) async fn list_delimited(
        &self,
        prefix: &str,
        ceiling: usize,
    ) -> Result<DelimitedList, StoreError> {
        list_delimited(self, prefix, ceiling).await
    }

    /// Get Blob Properties for `key`. With a pin, the pin's ETag is sent as
    /// `If-Match` and its version as `versionid`, so a 412 is
    /// [`StoreError::PreconditionFailed`]: the key no longer holds the pinned
    /// blob.
    pub(crate) async fn head_blob(
        &self,
        key: &str,
        pin: Option<&Pin>,
    ) -> Result<BlobHead, StoreError> {
        let path = format!("{}/{}", self.container_path, encode_key(key));
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(version) = pin.and_then(|pin| pin.version.as_deref()) {
            query.push(("versionid", version.to_string()));
        }
        let context = format!("head {key:?}");
        let (headers, _) = self
            .send(
                &context,
                Method::HEAD,
                &path,
                &query,
                pin.map(|pin| pin.etag.as_str()),
                Subject::Blob,
            )
            .await?;
        blob_head(key, &headers)
    }

    /// Delete Blob for `key`. `object_store` deletes through Blob Batch, which
    /// the emulator does not serve and which buries a per-blob status in a
    /// multipart body, so a delete is sent here. A missing blob is
    /// [`StoreError::NotFound`].
    pub(crate) async fn delete_blob(&self, key: &str) -> Result<(), StoreError> {
        let path = format!("{}/{}", self.container_path, encode_key(key));
        let context = format!("delete {key:?}");
        self.send(&context, Method::DELETE, &path, &[], None, Subject::Blob)
            .await
            .map(|_| ())
    }

    /// Get Account Information through the container URL, and refuse a
    /// hierarchical-namespace account (ADR-4158 D6). A `true`, an absent
    /// header and any other value are all refusals.
    pub(crate) async fn refuse_hierarchical_namespace(&self) -> Result<(), StoreError> {
        let query = [
            ("restype", "account".to_string()),
            ("comp", "properties".to_string()),
        ];
        let context = format!("account information for {:?}", self.account);
        let (headers, _) = self
            .send(
                &context,
                Method::GET,
                &self.container_path,
                &query,
                None,
                Subject::Container,
            )
            .await?;
        hns_answer(&self.account, &headers)
    }

    /// Send one request under the retry rules and return the success
    /// response's headers and body. `context` opens every error message.
    async fn send(
        &self,
        context: &str,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        if_match: Option<&str>,
        subject: Subject,
    ) -> Result<(HeaderMap, Vec<u8>), StoreError> {
        let started = tokio::time::Instant::now();
        let mut backoff = Backoff::new(&self.retry.backoff);
        let mut retries = 0usize;
        loop {
            let exhausted =
                retries >= self.retry.max_retries || started.elapsed() > self.retry.retry_timeout;
            let outcome = match self
                .attempt(context, method.clone(), path, query, if_match)
                .await?
            {
                Ok(response) => read_response(context, response).await,
                Err(failure) => Err(failure),
            };
            let retry = match &outcome {
                Ok(_) => false,
                Err(Failure::Status { status, .. }) => status_retryable(*status),
                Err(Failure::Transport(error)) => transport_retryable(error.kind()),
                Err(Failure::Credential(_)) => true,
                Err(Failure::Refused(_)) => false,
            };
            if !retry || exhausted {
                return outcome.map_err(|failure| failure.into_store_error(context, subject));
            }
            tokio::time::sleep(backoff.next()).await;
            retries += 1;
        }
    }

    /// Send one attempt. The outer error is a URL or header that cannot be
    /// built, or a credential this client cannot use, which no retry changes;
    /// a failed credential fetch or send is the inner failure, for the retry
    /// loop.
    async fn attempt(
        &self,
        context: &str,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        if_match: Option<&str>,
    ) -> Result<Result<HttpResponse, Failure>, StoreError> {
        let origin = &self.container_url[..self.container_url.len() - self.container_path.len()];
        let encoded_query: Vec<String> = query
            .iter()
            .map(|(name, value)| format!("{name}={}", encode_component(value)))
            .collect();
        let url = if encoded_query.is_empty() {
            format!("{origin}{path}")
        } else {
            format!("{origin}{path}?{}", encoded_query.join("&"))
        };
        let mut request = HttpRequest::new(HttpRequestBody::from(Bytes::new()));
        *request.method_mut() = method.clone();
        *request.uri_mut() = url
            .parse()
            .map_err(|e| StoreError::Permanent(format!("{context}: invalid request URL: {e}")))?;
        let header = |value: &str, what: &str| {
            HeaderValue::from_str(value).map_err(|_| {
                StoreError::Permanent(format!("{context}: the {what} is not a valid header value"))
            })
        };
        let headers = request.headers_mut();
        headers.insert(X_MS_VERSION_HEADER, HeaderValue::from_static(X_MS_VERSION));
        if let Some(etag) = if_match {
            headers.insert(IF_MATCH, header(etag, "If-Match ETag")?);
        }
        let Some(credentials) = &self.credentials else {
            return Ok(self.http.execute(request).await.map_err(Failure::Transport));
        };
        let credential = match credentials.get_credential().await {
            Ok(credential) => credential,
            Err(e) => {
                return Ok(Err(Failure::Credential(format!(
                    "{context}: credential fetch failed: {e}"
                ))));
            }
        };
        let date = imf_fixdate(self.clock.now_unix_secs());
        request
            .headers_mut()
            .insert(X_MS_DATE, header(&date, "date")?);
        let authorization = match credential.as_ref() {
            AzureCredential::BearerToken(token) => format!("Bearer {token}"),
            AzureCredential::AccessKey(_) => {
                let Some(key) = &self.account_key else {
                    return Err(StoreError::Permanent(format!(
                        "{context}: the credential is an account key this client was not given"
                    )));
                };
                let mut signed: Vec<(String, String)> = vec![
                    ("x-ms-date".to_string(), date.clone()),
                    ("x-ms-version".to_string(), X_MS_VERSION.to_string()),
                ];
                if let Some(etag) = if_match {
                    signed.push(("if-match".to_string(), etag.to_string()));
                }
                let decoded: Vec<(String, String)> = query
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), value.clone()))
                    .collect();
                let to_sign =
                    string_to_sign(method.as_str(), &signed, &self.account, path, &decoded);
                format!("SharedKey {}:{}", self.account, key.sign(&to_sign))
            }
            AzureCredential::SASToken(_) => {
                return Err(StoreError::Permanent(format!(
                    "{context}: a SAS token is not accepted for Ravel's own store (ADR-4158 D3)"
                )));
            }
        };
        let mut value = header(&authorization, "authorization")?;
        value.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, value);
        Ok(self.http.execute(request).await.map_err(Failure::Transport))
    }
}

/// The `SharedKey` string-to-sign for a Blob service request: the verb, the
/// twelve standard headers in their fixed order (`Content-Length` empty when
/// zero), the `x-ms-*` headers lowercased and sorted, and the canonicalized
/// resource. `headers` are lowercased names with their values; `path` is the
/// URL path as sent, still percent-encoded; `query` holds decoded pairs.
pub(crate) fn string_to_sign(
    method: &str,
    headers: &[(String, String)],
    account: &str,
    path: &str,
    query: &[(String, String)],
) -> String {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map_or("", |(_, v)| v.as_str())
    };
    let content_length = match get("content-length") {
        "0" => "",
        other => other,
    };
    let mut out = String::new();
    for field in [
        method,
        get("content-encoding"),
        get("content-language"),
        content_length,
        get("content-md5"),
        get("content-type"),
        get("date"),
        get("if-modified-since"),
        get("if-match"),
        get("if-none-match"),
        get("if-unmodified-since"),
        get("range"),
    ] {
        out.push_str(field);
        out.push('\n');
    }
    let mut ms: Vec<(&str, &str)> = headers
        .iter()
        .filter(|(name, _)| name.starts_with("x-ms-"))
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    ms.sort_unstable();
    for (name, value) in ms {
        out.push_str(name);
        out.push(':');
        out.push_str(value);
        out.push('\n');
    }
    out.push('/');
    out.push_str(account);
    out.push_str(path);
    let mut params: Vec<(String, Vec<&str>)> = Vec::new();
    for (name, value) in query {
        let name = name.to_ascii_lowercase();
        match params.iter_mut().find(|(n, _)| *n == name) {
            Some((_, values)) => values.push(value),
            None => params.push((name, vec![value.as_str()])),
        }
    }
    params.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, mut values) in params {
        values.sort_unstable();
        out.push('\n');
        out.push_str(&name);
        out.push(':');
        out.push_str(&values.join(","));
    }
    out
}

/// The success response's headers and body, or the failure it stands for.
async fn read_response(
    context: &str,
    response: HttpResponse,
) -> Result<(HeaderMap, Vec<u8>), Failure> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        let headers = response.headers().clone();
        return match read_body(response, LIST_MAX_BODY_BYTES).await {
            Ok(Some(body)) => Ok((headers, body)),
            Ok(None) => Err(Failure::Refused(StoreError::Permanent(format!(
                "{context}: the response body exceeds {LIST_MAX_BODY_BYTES} bytes"
            )))),
            Err(error) => Err(Failure::Transport(error)),
        };
    }
    if response.status().is_redirection() {
        return Err(Failure::Refused(StoreError::Permanent(format!(
            "{context}: HTTP {status} redirect, not followed"
        ))));
    }
    let header_code = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .filter(|code| !code.is_empty())
        .map(str::to_string);
    let code = match header_code {
        Some(code) => Some(code),
        None => match read_body(response, ERROR_BODY_BYTES).await {
            Ok(Some(body)) => error_code(&body),
            Ok(None) | Err(_) => None,
        },
    };
    Err(Failure::Status { status, code })
}

/// Why one attempt did not succeed.
enum Failure {
    /// A non-success, non-redirect status, with its `x-ms-error-code`.
    Status { status: u16, code: Option<String> },
    /// The request or the body read failed in transport.
    Transport(HttpError),
    /// The credential provider failed; retried, then `Transient`.
    Credential(String),
    /// Already classified, and never retried.
    Refused(StoreError),
}

impl Failure {
    fn into_store_error(self, context: &str, subject: Subject) -> StoreError {
        match self {
            Failure::Refused(error) => error,
            Failure::Credential(message) => StoreError::Transient(message),
            Failure::Transport(error) => match error.kind() {
                HttpErrorKind::Timeout => StoreError::Timeout,
                _ => StoreError::Transient(format!("{context}: {error}")),
            },
            Failure::Status { status, code } => {
                status_error(context, status, code.as_deref(), subject)
            }
        }
    }
}

/// A final status, classified by the status, the `x-ms-error-code` and, for a
/// 404, what the request addressed (ADR-4158 D5).
fn status_error(context: &str, status: u16, code: Option<&str>, subject: Subject) -> StoreError {
    let detail = match code {
        Some(code) => format!("{context}: HTTP {status} {code}"),
        None => format!("{context}: HTTP {status}"),
    };
    match status {
        401 | 403 => StoreError::AccessDenied(detail),
        404 if code == Some("ContainerNotFound") || subject == Subject::Container => {
            StoreError::Permanent(format!("{detail}: the container does not exist"))
        }
        404 => StoreError::NotFound,
        409 | 412 if super::is_lease_code(code) => {
            StoreError::Permanent(format!("{detail}: the blob is leased"))
        }
        412 => StoreError::PreconditionFailed,
        416 => StoreError::InvalidRange(detail),
        429 | 503 => StoreError::Throttled {
            retry_after_ms: 1000,
        },
        408 | 500..=599 => StoreError::Transient(detail),
        _ => StoreError::Permanent(detail),
    }
}

fn blob_head(key: &str, headers: &HeaderMap) -> Result<BlobHead, StoreError> {
    let text = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let refuse = |what: &str| StoreError::Permanent(format!("head {key:?}: Azure returned {what}"));
    let etag = text("etag")
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| refuse("no ETag"))?
        .to_string();
    let size = text("content-length")
        .and_then(|length| length.parse::<u64>().ok())
        .ok_or_else(|| refuse("no readable Content-Length"))?;
    let last_modified_ns = text("last-modified")
        .and_then(parse_imf_fixdate_ns)
        .ok_or_else(|| refuse("no readable Last-Modified"))?;
    Ok(BlobHead {
        size,
        etag,
        version_id: text("x-ms-version-id")
            .filter(|version| !version.is_empty())
            .map(str::to_string),
        last_modified_unix_ms: last_modified_ns.div_euclid(1_000_000),
    })
}

/// `Ok` only for an `x-ms-is-hns-enabled: false`.
fn hns_answer(account: &str, headers: &HeaderMap) -> Result<(), StoreError> {
    match headers
        .get("x-ms-is-hns-enabled")
        .map(|value| value.to_str().map(str::trim))
    {
        Some(Ok(value)) if value.eq_ignore_ascii_case("false") => Ok(()),
        Some(Ok(value)) if value.eq_ignore_ascii_case("true") => {
            Err(StoreError::Permanent(format!(
                "Azure storage account {account:?} has hierarchical namespace enabled \
                 (x-ms-is-hns-enabled: true); Ravel needs a flat-namespace account, and the \
                 setting is fixed when the account is created"
            )))
        }
        Some(_) => Err(StoreError::Permanent(format!(
            "Azure storage account {account:?}: the hierarchical namespace setting \
             (x-ms-is-hns-enabled) is unreadable, so the account is refused"
        ))),
        None => Err(StoreError::Permanent(format!(
            "Azure storage account {account:?}: Get Account Information carried no \
             x-ms-is-hns-enabled header, so whether hierarchical namespace is enabled is \
             unknown and the account is refused"
        ))),
    }
}

/// An XML element with its attributes, entities resolved.
#[derive(Debug, Default)]
pub(crate) struct XmlNode {
    pub(crate) name: String,
    pub(crate) attributes: Vec<(String, String)>,
    pub(crate) text: String,
    pub(crate) children: Vec<XmlNode>,
}

impl XmlNode {
    pub(crate) fn child(&self, name: &str) -> Option<&XmlNode> {
        self.children.iter().find(|child| child.name == name)
    }

    fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Parse `body` into a tree. A body that is not well-formed XML, that stops
/// mid-element or that holds more than one root is an error.
pub(crate) fn parse_xml(body: &[u8]) -> Result<XmlNode, String> {
    let mut reader = Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut stack: Vec<XmlNode> = Vec::new();
    let mut root: Option<XmlNode> = None;
    let attach = |stack: &mut Vec<XmlNode>, root: &mut Option<XmlNode>, node: XmlNode| match stack
        .last_mut()
    {
        Some(parent) => {
            parent.children.push(node);
            Ok(())
        }
        None if root.is_none() => {
            *root = Some(node);
            Ok(())
        }
        None => Err("more than one root element".to_string()),
    };
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => stack.push(open_node(&e)?),
            Ok(Event::Empty(e)) => attach(&mut stack, &mut root, open_node(&e)?)?,
            Ok(Event::End(_)) => {
                let node = stack.pop().ok_or("unbalanced end tag")?;
                attach(&mut stack, &mut root, node)?;
            }
            Ok(Event::Text(t)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            Ok(Event::CData(t)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            Ok(Event::GeneralRef(r)) => {
                let resolved = if r.is_char_ref() {
                    r.resolve_char_ref()
                        .map_err(|e| e.to_string())?
                        .map(String::from)
                } else {
                    let name = r.decode().map_err(|e| e.to_string())?;
                    quick_xml::escape::resolve_predefined_entity(&name).map(str::to_string)
                };
                let resolved = resolved.ok_or("unresolvable entity reference")?;
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&resolved);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
        buf.clear();
    }
    if let Some(open) = stack.last() {
        return Err(format!("response ended inside <{}>", open.name));
    }
    root.ok_or_else(|| "no root element".to_string())
}

fn open_node(e: &BytesStart<'_>) -> Result<XmlNode, String> {
    let name = String::from_utf8(e.local_name().as_ref().to_vec())
        .map_err(|_| "an element name is not UTF-8".to_string())?;
    let mut attributes = Vec::new();
    for attribute in e.attributes() {
        let attribute = attribute.map_err(|e| e.to_string())?;
        let key = String::from_utf8_lossy(attribute.key.local_name().as_ref()).into_owned();
        let value = String::from_utf8_lossy(attribute.value.as_ref()).into_owned();
        attributes.push((key, value));
    }
    Ok(XmlNode {
        name,
        attributes,
        ..XmlNode::default()
    })
}

/// The `<Error><Code>` of an Azure error body, or `None` when it is not one.
fn error_code(body: &[u8]) -> Option<String> {
    let root = parse_xml(body).ok()?;
    if root.name != "Error" {
        return None;
    }
    let code = root.child("Code")?.text.trim();
    (!code.is_empty()).then(|| code.to_string())
}

/// One List Blobs response body, names decoded. Anything but an
/// `<EnumerationResults>` whose every `Blob` carries a name, a length, a
/// `Last-Modified` and an `Etag` is [`StoreError::Permanent`].
pub(crate) fn parse_list(prefix: &str, body: &[u8]) -> Result<ListResponse, StoreError> {
    let refuse = |detail: String| StoreError::Permanent(format!("listing {prefix:?}: {detail}"));
    let root = parse_xml(body)
        .map_err(|e| refuse(format!("the response is not a List Blobs result: {e}")))?;
    if root.name != "EnumerationResults" {
        return Err(refuse(format!(
            "the response is <{}>, not <EnumerationResults>",
            root.name
        )));
    }
    let mut response = ListResponse::default();
    if let Some(blobs) = root.child("Blobs") {
        for entry in &blobs.children {
            match entry.name.as_str() {
                "Blob" => response.items.push(listed_blob(entry).map_err(refuse)?),
                "BlobPrefix" => response.prefixes.push(
                    entry
                        .child("Name")
                        .map(listed_name)
                        .ok_or_else(|| refuse("a BlobPrefix carries no Name".to_string()))?,
                ),
                _ => {}
            }
        }
    }
    response.next_marker = root
        .child("NextMarker")
        .map(|marker| marker.text.clone())
        .filter(|marker| !marker.is_empty());
    Ok(response)
}

fn listed_blob(entry: &XmlNode) -> Result<ObjectMeta, String> {
    let key = entry
        .child("Name")
        .map(listed_name)
        .ok_or("a listed blob carries no Name")?;
    let properties = entry
        .child("Properties")
        .ok_or_else(|| format!("listed blob {key:?} carries no Properties"))?;
    let property = |name: &str| properties.child(name).map(|node| node.text.trim());
    let size = property("Content-Length")
        .and_then(|size| size.parse::<u64>().ok())
        .ok_or_else(|| format!("listed blob {key:?} has no readable Content-Length"))?;
    let last_modified_ns = property("Last-Modified")
        .and_then(parse_imf_fixdate_ns)
        .ok_or_else(|| format!("listed blob {key:?} has no readable Last-Modified"))?;
    let etag = property("Etag")
        .filter(|etag| !etag.is_empty())
        .map(quoted_etag)
        .ok_or_else(|| format!("listed blob {key:?} carries no Etag"))?;
    Ok(ObjectMeta {
        key,
        size,
        etag: Etag(etag.clone()),
        version: Version(etag),
        last_modified_unix_ms: last_modified_ns.div_euclid(1_000_000),
    })
}

/// The listed `Etag` as HEAD reports it: Azure lists it without the quotes
/// the `ETag` header carries (ADR-4158, Measured P11). One already quoted is
/// kept as is.
pub(crate) fn quoted_etag(listed: &str) -> String {
    if listed.starts_with('"') {
        listed.to_string()
    } else {
        format!("\"{listed}\"")
    }
}

/// A `Name` as the container holds it: decoded when `Encoded="true"`.
fn listed_name(node: &XmlNode) -> String {
    if node.attribute("Encoded") == Some("true") {
        decode_encoded_name(&node.text)
    } else {
        node.text.clone()
    }
}

/// An `Encoded="true"` name, percent-decoded as a whole string: Azure encodes
/// every character of such a name that is not unreserved, `/` included. A
/// name that does not decode (a `%` not followed by two hex digits, or bytes
/// that are not UTF-8) is kept as listed, which no request can address, so
/// the listing reports it as unaddressable.
pub(crate) fn decode_encoded_name(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let digits = bytes.get(index + 1..index + 3);
            let Some(byte) = digits.and_then(|d| Some(hex_value(d[0])? << 4 | hex_value(d[1])?))
            else {
                return text.to_string();
            };
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Percent-encode everything but RFC 3986's unreserved characters.
pub(crate) fn encode_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// A key as a URL path: each `/`-separated segment encoded on its own.
fn encode_key(key: &str) -> String {
    key.split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

/// The path part of an absolute URL without a query: from the first `/`
/// after the authority, or `/` when there is none.
fn url_path(url: &str) -> String {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    match after_scheme.find('/') {
        Some(at) => after_scheme[at..].to_string(),
        None => "/".to_string(),
    }
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
pub(crate) fn imf_fixdate(secs: i64) -> String {
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
