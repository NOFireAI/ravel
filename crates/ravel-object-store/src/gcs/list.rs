//! `GcsStore`'s own JSON API `objects.list` client (ADR-3456 decision 1).
//!
//! Each request is one `GET <base>/storage/v1/b/<bucket>/o` carrying `prefix`,
//! `delimiter` and `startOffset` as raw names, and the service's opaque
//! `pageToken` when following a page. `maxResults` is the store's configured
//! page size for `list` and `list_after`, since those stop as soon as one page
//! is filled, and the wire maximum ([`WIRE_PAGE_KEYS`]) for `list_delimited`,
//! which drains every response regardless of page size, the same as the S3
//! lister's own delimited listing. Names come back as JSON strings and go to
//! [`crate::classify_objects`] and [`crate::classify_prefixes`] as received,
//! so a name `object_store`'s `Path` would rewrite is reported as
//! unaddressable rather than failing the page.
//!
//! Requests go through the data plane's [`S3HttpConnector`] recording around
//! a reqwest client with redirects off, inside the caller's
//! [`crate::s3::connector::scope`], so every attempt, retries included, is a
//! store attempt for the `list` or `list_delimited` operation. The bearer
//! token comes from the credential provider the data-plane client holds.
//! No error carries a response body.
//!
//! [`S3HttpConnector`]: crate::s3::connector::S3HttpConnector

use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use object_store::RetryConfig;
use object_store::client::{
    HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse,
};
use object_store::gcp::GcpCredentialProvider;
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::Deserialize;

use crate::s3::list::{Backoff, read_body, status_retryable, transport_retryable};
use crate::s3::parse_iso8601;
use crate::{DelimitedList, Etag, ListPage, ObjectMeta, PageToken, StoreError, Version};

/// Largest `objects.list` response body read.
const LIST_MAX_BODY_BYTES: usize = crate::s3::list::LIST_MAX_BODY_BYTES;

/// The `maxResults` a `list_delimited` request sends, matching the S3
/// lister's own delimited-listing `max-keys` (`s3/list.rs`'s
/// `WIRE_PAGE_KEYS`): a delimited drain reads every response regardless of
/// the store's configured page size, so it asks for the most the wire
/// allows rather than paying one round trip per configured-size page.
const WIRE_PAGE_KEYS: usize = 1000;

/// The response fields asked for: everything [`ObjectMeta`] needs, and no
/// more. `md5Hash` and `componentCount` decide the listed ETag.
const FIELDS: &str =
    "items(name,generation,size,updated,etag,md5Hash,componentCount),prefixes,nextPageToken";

/// Consecutive responses carrying no listed object one `list` call accepts
/// before it fails with [`StoreError::ListPageCeiling`], as on S3.
const EMPTY_RESPONSE_ALLOWANCE: usize = 16;

/// Sends `objects.list` requests for one bucket.
#[derive(Debug)]
pub(crate) struct GcsListClient {
    http: HttpClient,
    /// `None` sends no `Authorization` header (a test endpoint only).
    credentials: Option<GcpCredentialProvider>,
    retry: RetryConfig,
    /// `<base>/storage/v1/b/<bucket>/o`, the bucket percent-encoded.
    url: String,
    /// `<base>/upload/storage/v1/b/<bucket>/o`, for the test-only foreign
    /// writer.
    #[cfg(any(test, feature = "test-support"))]
    upload_url: String,
    page_size: usize,
}

/// One `objects.list` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListRequest<'a> {
    prefix: &'a str,
    start_offset: Option<&'a str>,
    page_token: Option<&'a str>,
    delimited: bool,
}

/// One parsed `objects.list` response, with raw names.
#[derive(Debug, Default)]
pub(crate) struct ListResponse {
    items: Vec<ObjectMeta>,
    prefixes: Vec<String>,
    next_page_token: Option<String>,
}

/// Where the paging loops get their responses: [`GcsListClient`] in
/// production, a script in this module's tests.
pub(crate) trait ListFetch {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError>;
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireList {
    #[serde(default)]
    items: Vec<WireObject>,
    #[serde(default)]
    prefixes: Vec<String>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireObject {
    name: Option<String>,
    generation: Option<String>,
    size: Option<String>,
    updated: Option<String>,
    etag: Option<String>,
    md5_hash: Option<String>,
    /// Present on a composite object only; its value is not used.
    component_count: Option<serde_json::Value>,
}

/// One [`ListPage`]: up to `page_size` raw names under `prefix` after
/// `start_after`, following `nextPageToken` within the call. `startOffset` is
/// inclusive, so the name equal to `start_after` is dropped. When it was the
/// only name a response dropped and names remain, the call returns them with
/// a page token rather than sending another request. When names remain, the
/// page token is the raw last name kept.
pub(crate) async fn list_page<F: ListFetch>(
    fetch: &F,
    prefix: &str,
    start_after: Option<&str>,
    page_size: usize,
) -> Result<ListPage, StoreError> {
    let page_size = page_size.max(1);
    let mut listed: Vec<ObjectMeta> = Vec::new();
    let mut token: Option<String> = None;
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
            start_offset: start_after,
            page_token: token.as_deref(),
            delimited: false,
        };
        let response = fetch.fetch(&request).await?;
        let mut items = response.items;
        let received = items.len();
        items.retain(|meta| Some(meta.key.as_str()) != start_after);
        let dropped_start = items.len() < received;
        if items.is_empty() {
            empty_responses += 1;
        } else {
            empty_responses = 0;
        }
        listed.extend(items);
        let Some(next) = response.next_page_token else {
            break listed.len() > page_size;
        };
        if listed.len() >= page_size || (dropped_start && !listed.is_empty()) {
            break true;
        }
        token = Some(next_token(prefix, token.as_deref(), next)?);
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

/// The whole delimited listing under `prefix`, following `nextPageToken`
/// until a response carries none, in at most `ceiling` responses. A prefix
/// two responses both report is listed once.
pub(crate) async fn list_delimited<F: ListFetch>(
    fetch: &F,
    prefix: &str,
    ceiling: usize,
) -> Result<DelimitedList, StoreError> {
    let mut listed: Vec<ObjectMeta> = Vec::new();
    let mut listed_prefixes: Vec<String> = Vec::new();
    let mut seen_prefixes: HashSet<String> = HashSet::new();
    let mut token: Option<String> = None;
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
            start_offset: None,
            page_token: token.as_deref(),
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
        let Some(next) = response.next_page_token else {
            break;
        };
        token = Some(next_token(prefix, token.as_deref(), next)?);
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

impl ListFetch for GcsListClient {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
        let url = self.request_url(request);
        let context = format!("listing {:?}", request.prefix);
        let body = self
            .send(
                &context,
                Method::GET,
                &url,
                Bytes::new(),
                NotFoundSubject::Bucket,
            )
            .await?;
        parse_list(request.prefix, &body)
    }
}

impl GcsListClient {
    pub(crate) fn new(
        http: HttpClient,
        credentials: Option<GcpCredentialProvider>,
        retry: RetryConfig,
        base_url: &str,
        bucket: &str,
        page_size: usize,
    ) -> Self {
        let base_url = base_url.trim_end_matches('/');
        let bucket = encode_component(bucket);
        GcsListClient {
            http,
            credentials,
            retry,
            url: format!("{base_url}/storage/v1/b/{bucket}/o"),
            #[cfg(any(test, feature = "test-support"))]
            upload_url: format!("{base_url}/upload/storage/v1/b/{bucket}/o"),
            page_size,
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

    /// Store `data` under exactly `name` through the JSON API's media upload.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn upload_media(&self, name: &str, data: Bytes) -> Result<(), StoreError> {
        let url = format!(
            "{}?uploadType=media&name={}",
            self.upload_url,
            encode_component(name)
        );
        let context = format!("uploading {name:?}");
        self.send(&context, Method::POST, &url, data, NotFoundSubject::Object)
            .await?;
        Ok(())
    }

    /// Delete exactly `name` through the JSON API.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn delete_object(&self, name: &str) -> Result<(), StoreError> {
        let url = format!("{}/{}", self.url, encode_component(name));
        let context = format!("deleting {name:?}");
        self.send(
            &context,
            Method::DELETE,
            &url,
            Bytes::new(),
            NotFoundSubject::Object,
        )
        .await?;
        Ok(())
    }

    fn request_url(&self, request: &ListRequest<'_>) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();
        if !request.prefix.is_empty() {
            pairs.push(("prefix", request.prefix.to_string()));
        }
        if request.delimited {
            pairs.push(("delimiter", "/".to_string()));
        }
        if let Some(offset) = request.start_offset {
            pairs.push(("startOffset", offset.to_string()));
        }
        let max_results = if request.delimited {
            WIRE_PAGE_KEYS
        } else {
            self.page_size.max(1)
        };
        pairs.push(("maxResults", max_results.to_string()));
        if let Some(token) = request.page_token {
            pairs.push(("pageToken", token.to_string()));
        }
        pairs.push(("fields", FIELDS.to_string()));
        let query: Vec<String> = pairs
            .iter()
            .map(|(name, value)| format!("{name}={}", encode_component(value)))
            .collect();
        format!("{}?{}", self.url, query.join("&"))
    }

    /// Send one request under the retry rules and return the success body.
    /// `context` opens every error message; `not_found` says what a 404
    /// means for this request (ADR-3456 decision 4).
    async fn send(
        &self,
        context: &str,
        method: Method,
        url: &str,
        body: Bytes,
        not_found: NotFoundSubject,
    ) -> Result<Vec<u8>, StoreError> {
        let started = tokio::time::Instant::now();
        let mut backoff = Backoff::new(&self.retry.backoff);
        let mut retries = 0usize;
        loop {
            let exhausted =
                retries >= self.retry.max_retries || started.elapsed() > self.retry.retry_timeout;
            let outcome = match self
                .attempt(context, method.clone(), url, body.clone())
                .await?
            {
                Ok(response) => read_response(context, response).await,
                Err(failure) => Err(failure),
            };
            let retry = match &outcome {
                Ok(_) => false,
                Err(Failure::Status(status)) => status_retryable(*status),
                Err(Failure::Transport(error)) => transport_retryable(error.kind()),
                Err(Failure::Credential(_)) => true,
                Err(Failure::Refused(_)) => false,
            };
            if !retry || exhausted {
                return outcome.map_err(|failure| failure.into_store_error(context, not_found));
            }
            tokio::time::sleep(backoff.next()).await;
            retries += 1;
        }
    }

    /// Send one attempt. The outer error is a URL or header that cannot be
    /// built, which no retry changes; a failed credential fetch or send is the
    /// inner failure, for the retry loop.
    async fn attempt(
        &self,
        context: &str,
        method: Method,
        url: &str,
        body: Bytes,
    ) -> Result<Result<HttpResponse, Failure>, StoreError> {
        let mut request = HttpRequest::new(HttpRequestBody::from(body));
        *request.method_mut() = method;
        *request.uri_mut() = url
            .parse()
            .map_err(|e| StoreError::Permanent(format!("{context}: invalid request URL: {e}")))?;
        if let Some(credentials) = &self.credentials {
            let credential = match credentials.get_credential().await {
                Ok(credential) => credential,
                Err(e) => {
                    return Ok(Err(Failure::Credential(format!(
                        "{context}: credential fetch failed: {e}"
                    ))));
                }
            };
            let mut value = HeaderValue::from_str(&format!("Bearer {}", credential.bearer))
                .map_err(|_| {
                    StoreError::Permanent(format!(
                        "{context}: the bearer token is not a valid header value"
                    ))
                })?;
            value.set_sensitive(true);
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        Ok(self.http.execute(request).await.map_err(Failure::Transport))
    }
}

/// The token to follow a page with: refused when it repeats the one just sent.
fn next_token(prefix: &str, sent: Option<&str>, received: String) -> Result<String, StoreError> {
    if sent == Some(received.as_str()) {
        return Err(StoreError::ListRepeatedToken {
            prefix: prefix.to_string(),
        });
    }
    Ok(received)
}

/// The success body, or the failure a response stands for.
async fn read_response(context: &str, response: HttpResponse) -> Result<Vec<u8>, Failure> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        return match read_body(response, LIST_MAX_BODY_BYTES).await {
            Ok(Some(body)) => Ok(body),
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
    Err(Failure::Status(status))
}

/// Why one attempt did not return a listing.
enum Failure {
    /// A non-success, non-redirect status.
    Status(u16),
    /// The request or the body read failed in transport.
    Transport(HttpError),
    /// The credential provider failed; retried, then `Transient`.
    Credential(String),
    /// Already classified, and never retried.
    Refused(StoreError),
}

impl Failure {
    fn into_store_error(self, context: &str, not_found: NotFoundSubject) -> StoreError {
        match self {
            Failure::Refused(error) => error,
            Failure::Credential(message) => StoreError::Transient(message),
            Failure::Transport(error) => match error.kind() {
                HttpErrorKind::Timeout => StoreError::Timeout,
                _ => StoreError::Transient(format!("{context}: {error}")),
            },
            Failure::Status(status) => status_error(context, status, not_found),
        }
    }
}

/// What a 404 means for one request (ADR-3456 decision 4): `objects.list`
/// reports a missing *bucket* this way, while the object-scoped JSON API
/// calls (`delete`, media upload) report a missing *object* the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotFoundSubject {
    Bucket,
    #[cfg(any(test, feature = "test-support"))]
    Object,
}

/// A final status, classified by the status and, for 404, by `not_found`.
fn status_error(context: &str, status: u16, not_found: NotFoundSubject) -> StoreError {
    match status {
        401 | 403 => StoreError::AccessDenied(format!("{context}: HTTP {status}")),
        404 => match not_found {
            NotFoundSubject::Bucket => {
                StoreError::Permanent(format!("{context}: bucket does not exist (HTTP 404)"))
            }
            #[cfg(any(test, feature = "test-support"))]
            NotFoundSubject::Object => StoreError::NotFound,
        },
        429 | 503 => StoreError::Throttled {
            retry_after_ms: 1000,
        },
        408 | 500..=599 => StoreError::Transient(format!("{context}: HTTP {status}")),
        _ => StoreError::Permanent(format!("{context}: HTTP {status}")),
    }
}

fn parse_list(prefix: &str, body: &[u8]) -> Result<ListResponse, StoreError> {
    let wire: WireList = serde_json::from_slice(body).map_err(|e| {
        StoreError::Permanent(format!(
            "listing {prefix:?}: the response is not an objects.list result: {e}"
        ))
    })?;
    let items = wire
        .items
        .into_iter()
        .map(|object| listed_object(prefix, object))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ListResponse {
        items,
        prefixes: wire.prefixes,
        next_page_token: wire.next_page_token.filter(|token| !token.is_empty()),
    })
}

fn listed_object(prefix: &str, object: WireObject) -> Result<ObjectMeta, StoreError> {
    let refuse = |detail: String| StoreError::Permanent(format!("listing {prefix:?}: {detail}"));
    let Some(key) = object.name else {
        return Err(refuse("a listed object carries no name".to_string()));
    };
    let generation = match object.generation {
        Some(generation) if super::is_generation(&generation) => generation,
        Some(generation) => {
            return Err(refuse(format!(
                "listed object {key:?} has generation {generation:?}, not a positive decimal"
            )));
        }
        None => {
            return Err(refuse(format!(
                "listed object {key:?} carries no generation"
            )));
        }
    };
    let size: u64 = object
        .size
        .as_deref()
        .and_then(|size| size.parse().ok())
        .ok_or_else(|| refuse(format!("listed object {key:?} has no readable size")))?;
    let (secs, nanos) = object
        .updated
        .as_deref()
        .and_then(parse_iso8601)
        .ok_or_else(|| {
            refuse(format!(
                "listed object {key:?} has no readable updated time"
            ))
        })?;
    let etag = match (object.md5_hash, object.component_count, object.etag) {
        (Some(md5), _, _) => md5_etag(&md5)
            .ok_or_else(|| refuse(format!("listed object {key:?} has an unreadable md5Hash")))?,
        (None, Some(_), Some(etag)) if !etag.is_empty() => composite_etag(&etag),
        (None, Some(_), _) => {
            return Err(refuse(format!(
                "listed composite object {key:?} carries no etag"
            )));
        }
        (None, None, _) => {
            return Err(refuse(format!(
                "listed object {key:?} carries neither md5Hash nor componentCount, so the \
                 ETag head reports for it is unknown"
            )));
        }
    };
    Ok(ObjectMeta {
        key,
        size,
        etag: Etag(etag),
        version: Version(generation),
        last_modified_unix_ms: secs
            .saturating_mul(1000)
            .saturating_add(i64::from(nanos / 1_000_000)),
    })
}

/// The ETag the XML API's `GET` and `HEAD` report for an object with an MD5,
/// the quoted lowercase hex of the digest, from the JSON API's base64
/// `md5Hash`. The JSON API's own `etag` field is a different value, so a
/// listing that reported it would disagree with `head` on the same object.
fn md5_etag(md5_base64: &str) -> Option<String> {
    let digest = STANDARD.decode(md5_base64).ok()?;
    if digest.len() != 16 {
        return None;
    }
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!("\"{hex}\""))
}

/// The ETag the XML API reports for a composite object: a dash followed by
/// the JSON API's `etag`, quoted.
fn composite_etag(json_etag: &str) -> String {
    format!("\"-{json_etag}\"")
}

/// Percent-encode everything but RFC 3986's unreserved characters, so a
/// name's bytes reach the service as given (a space is `%20`, never `+`).
fn encode_component(text: &str) -> String {
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::VecDeque;

    use parking_lot::Mutex;

    use super::*;

    #[test]
    fn md5_hash_becomes_the_quoted_hex_etag() {
        // md5("") = d41d8cd98f00b204e9800998ecf8427e.
        assert_eq!(
            md5_etag("1B2M2Y8AsgTpgAmY7PhCfg==").as_deref(),
            Some("\"d41d8cd98f00b204e9800998ecf8427e\"")
        );
        assert_eq!(md5_etag("not base64!"), None);
        assert_eq!(md5_etag("AAAA"), None);
    }

    fn wire(
        generation: Option<&str>,
        md5_hash: Option<&str>,
        component_count: Option<u32>,
    ) -> WireObject {
        WireObject {
            name: Some("p/k".to_string()),
            generation: generation.map(str::to_string),
            size: Some("3".to_string()),
            updated: Some("2026-10-29T10:00:00.000Z".to_string()),
            etag: Some("CJbYtNGqvYkDEAE=".to_string()),
            md5_hash: md5_hash.map(str::to_string),
            component_count: component_count.map(serde_json::Value::from),
        }
    }

    const EMPTY_MD5: &str = "1B2M2Y8AsgTpgAmY7PhCfg==";

    #[test]
    fn a_listed_object_without_a_generation_fails_the_call() {
        for generation in [None, Some(""), Some("0"), Some("012"), Some("abc")] {
            let listed = listed_object("p/", wire(generation, Some(EMPTY_MD5), None));
            assert!(
                matches!(listed, Err(StoreError::Permanent(_))),
                "{generation:?}: {listed:?}"
            );
        }
        let listed = listed_object("p/", wire(Some("1790000000000001"), Some(EMPTY_MD5), None))
            .expect("listed");
        assert_eq!(listed.version.0, "1790000000000001");
    }

    #[test]
    fn the_listed_etag_is_the_md5_else_the_dashed_composite_etag() {
        let with_md5 = listed_object("p/", wire(Some("7"), Some(EMPTY_MD5), None)).expect("listed");
        assert_eq!(with_md5.etag.0, "\"d41d8cd98f00b204e9800998ecf8427e\"");
        let composite = listed_object("p/", wire(Some("7"), None, Some(2))).expect("listed");
        assert_eq!(composite.etag.0, "\"-CJbYtNGqvYkDEAE=\"");
        let mut no_etag = wire(Some("7"), None, Some(2));
        no_etag.etag = None;
        match listed_object("p/", no_etag) {
            Err(StoreError::Permanent(message)) => {
                assert!(message.contains("\"p/k\""), "{message}")
            }
            other => panic!("{other:?}"),
        }
        match listed_object("p/", wire(Some("7"), None, None)) {
            Err(StoreError::Permanent(message)) => {
                assert!(message.contains("\"p/k\""), "{message}");
                assert!(message.contains("componentCount"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_fields_mask_names_what_object_meta_and_the_etag_need() {
        assert_eq!(
            FIELDS,
            "items(name,generation,size,updated,etag,md5Hash,componentCount),prefixes,nextPageToken"
        );
    }

    #[test]
    fn components_keep_only_unreserved_bytes() {
        assert_eq!(encode_component("a b/c+d~e"), "a%20b%2Fc%2Bd~e");
        assert_eq!(encode_component("\u{1}é"), "%01%C3%A9");
    }

    #[test]
    fn a_404_on_objects_list_means_the_bucket_does_not_exist() {
        match status_error("listing \"p/\"", 404, NotFoundSubject::Bucket) {
            StoreError::Permanent(message) => {
                assert!(message.contains("bucket does not exist"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_404_on_an_object_path_means_the_object_was_not_found() {
        assert!(matches!(
            status_error("deleting \"p/k\"", 404, NotFoundSubject::Object),
            StoreError::NotFound
        ));
    }

    /// `startOffset`, `pageToken` and whether the request is delimited.
    type Asked = (Option<String>, Option<String>, bool);

    /// Answers each request from a script and records what was asked.
    #[derive(Default)]
    struct Script {
        responses: Mutex<VecDeque<ListResponse>>,
        seen: Mutex<Vec<Asked>>,
    }

    impl Script {
        fn new(responses: impl IntoIterator<Item = ListResponse>) -> Self {
            Script {
                responses: Mutex::new(responses.into_iter().collect()),
                seen: Mutex::default(),
            }
        }

        fn seen(&self) -> Vec<Asked> {
            self.seen.lock().clone()
        }
    }

    impl ListFetch for Script {
        async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
            self.seen.lock().push((
                request.start_offset.map(str::to_string),
                request.page_token.map(str::to_string),
                request.delimited,
            ));
            Ok(self.responses.lock().pop_front().unwrap_or_default())
        }
    }

    fn meta(key: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size: 1,
            etag: Etag("\"e\"".to_string()),
            version: Version("1".to_string()),
            last_modified_unix_ms: 0,
        }
    }

    fn page(keys: &[&str], token: Option<&str>) -> ListResponse {
        ListResponse {
            items: keys.iter().map(|k| meta(k)).collect(),
            prefixes: Vec::new(),
            next_page_token: token.map(str::to_string),
        }
    }

    fn keys(page: &ListPage) -> Vec<&str> {
        page.objects.iter().map(|meta| meta.key.as_str()).collect()
    }

    fn token(text: &str) -> Option<String> {
        Some(text.to_string())
    }

    #[tokio::test]
    async fn an_empty_page_carrying_a_token_is_followed() {
        let script = Script::new([page(&[], Some("t1")), page(&["p/a", "p/b"], None)]);
        let listed = list_page(&script, "p/", None, 2).await.expect("listed");
        assert_eq!(keys(&listed), ["p/a", "p/b"]);
        assert_eq!(listed.next, None);
        assert_eq!(
            script.seen(),
            [(None, None, false), (None, token("t1"), false)]
        );
    }

    #[tokio::test]
    async fn the_empty_page_ceiling_is_a_typed_error() {
        let script = Script::new((0..100).map(|i| page(&[], Some(&format!("t{i}")))));
        match list_page(&script, "p/", None, 2).await {
            Err(StoreError::ListPageCeiling { prefix, ceiling }) => {
                assert_eq!(prefix, "p/");
                assert_eq!(ceiling, EMPTY_RESPONSE_ALLOWANCE);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(script.seen().len(), EMPTY_RESPONSE_ALLOWANCE);
    }

    #[tokio::test]
    async fn a_repeated_page_token_fails_the_call() {
        let script = Script::new([page(&[], Some("t1")), page(&[], Some("t1"))]);
        match list_page(&script, "p/", None, 2).await {
            Err(StoreError::ListRepeatedToken { prefix }) => assert_eq!(prefix, "p/"),
            other => panic!("{other:?}"),
        }
        assert_eq!(script.seen().len(), 2);
        let script = Script::new([page(&[], Some("t1")), page(&[], Some("t1"))]);
        match list_delimited(&script, "p/", 8).await {
            Err(StoreError::ListRepeatedToken { prefix }) => assert_eq!(prefix, "p/"),
            other => panic!("{other:?}"),
        }
        assert_eq!(script.seen().len(), 2);
    }

    #[tokio::test]
    async fn a_resumed_page_that_drops_only_the_start_key_returns_short() {
        // startOffset is inclusive: the service returns the start key first.
        let script = Script::new([page(&["p/b", "p/c"], Some("t1")), page(&["p/d"], None)]);
        let listed = list_page(&script, "p/", Some("p/b"), 2)
            .await
            .expect("listed");
        assert_eq!(keys(&listed), ["p/c"]);
        assert_eq!(listed.next, Some(PageToken("p/c".to_string())));
        assert_eq!(script.seen(), [(token("p/b"), None, false)]);
    }

    #[tokio::test]
    async fn a_resumed_response_holding_only_the_start_key_is_followed() {
        let script = Script::new([page(&["p/b"], Some("t1")), page(&["p/c"], Some("t2"))]);
        let listed = list_page(&script, "p/", Some("p/b"), 1)
            .await
            .expect("listed");
        assert_eq!(keys(&listed), ["p/c"]);
        assert_eq!(listed.next, Some(PageToken("p/c".to_string())));
        assert_eq!(
            script.seen(),
            [
                (token("p/b"), None, false),
                (token("p/b"), token("t1"), false)
            ]
        );
    }

    #[tokio::test]
    async fn the_last_response_dropping_the_start_key_ends_the_listing() {
        let script = Script::new([page(&["p/b", "p/c"], None)]);
        let listed = list_page(&script, "p/", Some("p/b"), 2)
            .await
            .expect("listed");
        assert_eq!(keys(&listed), ["p/c"]);
        assert_eq!(listed.next, None);
        assert_eq!(script.seen().len(), 1);
    }

    #[tokio::test]
    async fn delimited_prefixes_repeated_across_responses_are_listed_once() {
        let mut first = page(&["p/x"], Some("t1"));
        first.prefixes = vec!["p/a/".to_string(), "p/b/".to_string()];
        let mut second = page(&[], None);
        second.prefixes = vec!["p/b/".to_string(), "p/c/".to_string()];
        let script = Script::new([first, second]);
        let listed = list_delimited(&script, "p/", 8).await.expect("listed");
        assert_eq!(listed.common_prefixes, ["p/a/", "p/b/", "p/c/"]);
        assert_eq!(
            listed
                .objects
                .iter()
                .map(|meta| meta.key.as_str())
                .collect::<Vec<_>>(),
            ["p/x"]
        );
        assert_eq!(
            script.seen(),
            [(None, None, true), (None, token("t1"), true)]
        );
    }
}
