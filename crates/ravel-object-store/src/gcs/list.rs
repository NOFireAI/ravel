//! `GcsStore`'s own JSON API `objects.list` client (ADR-3456 decision 1).
//!
//! Each request is one `GET <base>/storage/v1/b/<bucket>/o` carrying `prefix`,
//! `delimiter` and `startOffset` as raw names, `maxResults` set to the store's
//! page size, and the service's opaque `pageToken` when following a page.
//! Names come back as JSON strings and go to [`crate::classify_objects`] and
//! [`crate::classify_prefixes`] as received, so a name `object_store`'s `Path`
//! would rewrite is reported as unaddressable rather than failing the page.
//!
//! Requests go through the data plane's [`S3HttpConnector`] recording around
//! a reqwest client with redirects off, inside the caller's
//! [`crate::s3::connector::scope`], so every attempt, retries included, is a
//! store attempt for the `list` or `list_delimited` operation. The bearer
//! token comes from the credential provider the data-plane client holds.
//! No error carries a response body.
//!
//! [`S3HttpConnector`]: crate::s3::connector::S3HttpConnector

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use object_store::RetryConfig;
use object_store::client::{
    HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse,
};
use object_store::gcp::GcpCredentialProvider;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::Deserialize;

use crate::s3::list::{Backoff, read_body, status_retryable, transport_retryable};
use crate::s3::parse_iso8601;
use crate::{DelimitedList, Etag, ListPage, ObjectMeta, PageToken, StoreError, Version};

/// Largest `objects.list` response body read.
const LIST_MAX_BODY_BYTES: usize = crate::s3::list::LIST_MAX_BODY_BYTES;

/// The response fields asked for: everything [`ObjectMeta`] needs, and no
/// more.
const FIELDS: &str = "items(name,generation,size,updated,etag,md5Hash),prefixes,nextPageToken";

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
    page_size: usize,
}

/// One `objects.list` request.
struct ListRequest<'a> {
    prefix: &'a str,
    start_offset: Option<&'a str>,
    page_token: Option<&'a str>,
    delimited: bool,
}

/// One parsed `objects.list` response, with raw names.
struct ListResponse {
    items: Vec<ObjectMeta>,
    prefixes: Vec<String>,
    next_page_token: Option<String>,
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
        GcsListClient {
            http,
            credentials,
            retry,
            url: format!(
                "{}/storage/v1/b/{}/o",
                base_url.trim_end_matches('/'),
                encode_component(bucket)
            ),
            page_size,
        }
    }

    /// One [`ListPage`]: up to `page_size` raw names under `prefix` after
    /// `start_after`. `startOffset` is inclusive, so the name equal to
    /// `start_after` is dropped from the responses. Follows `nextPageToken`
    /// within the call; when names remain, the page token is the raw last
    /// name kept.
    pub(crate) async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let page_size = self.page_size.max(1);
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
            let response = self.fetch(&request).await?;
            let mut items = response.items;
            items.retain(|meta| Some(meta.key.as_str()) != start_after);
            if items.is_empty() {
                empty_responses += 1;
            } else {
                empty_responses = 0;
            }
            listed.extend(items);
            let Some(next) = response.next_page_token else {
                break listed.len() > page_size;
            };
            if listed.len() >= page_size {
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
    /// until a response carries none, in at most `ceiling` responses.
    pub(crate) async fn list_delimited(
        &self,
        prefix: &str,
        ceiling: usize,
    ) -> Result<DelimitedList, StoreError> {
        let mut listed: Vec<ObjectMeta> = Vec::new();
        let mut listed_prefixes: Vec<String> = Vec::new();
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
            let response = self.fetch(&request).await?;
            responses += 1;
            listed.extend(response.items);
            listed_prefixes.extend(response.prefixes);
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

    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
        let url = self.request_url(request);
        let body = self.send(request.prefix, &url).await?;
        parse_list(request.prefix, &body)
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
        pairs.push(("maxResults", self.page_size.max(1).to_string()));
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

    /// Send one GET under the retry rules and return the success body.
    async fn send(&self, prefix: &str, url: &str) -> Result<Vec<u8>, StoreError> {
        let started = tokio::time::Instant::now();
        let mut backoff = Backoff::new(&self.retry.backoff);
        let mut retries = 0usize;
        loop {
            let exhausted =
                retries >= self.retry.max_retries || started.elapsed() > self.retry.retry_timeout;
            let outcome = match self.attempt(prefix, url).await? {
                Ok(response) => read_response(prefix, response).await,
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
                return outcome.map_err(|failure| failure.into_store_error(prefix));
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
        prefix: &str,
        url: &str,
    ) -> Result<Result<HttpResponse, Failure>, StoreError> {
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.uri_mut() = url.parse().map_err(|e| {
            StoreError::Permanent(format!("listing {prefix:?}: invalid request URL: {e}"))
        })?;
        if let Some(credentials) = &self.credentials {
            let credential = match credentials.get_credential().await {
                Ok(credential) => credential,
                Err(e) => {
                    return Ok(Err(Failure::Credential(format!(
                        "listing {prefix:?}: credential fetch failed: {e}"
                    ))));
                }
            };
            let mut value = HeaderValue::from_str(&format!("Bearer {}", credential.bearer))
                .map_err(|_| {
                    StoreError::Permanent(format!(
                        "listing {prefix:?}: the bearer token is not a valid header value"
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
async fn read_response(prefix: &str, response: HttpResponse) -> Result<Vec<u8>, Failure> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        return match read_body(response, LIST_MAX_BODY_BYTES).await {
            Ok(Some(body)) => Ok(body),
            Ok(None) => Err(Failure::Refused(StoreError::Permanent(format!(
                "listing {prefix:?}: the response body exceeds {LIST_MAX_BODY_BYTES} bytes"
            )))),
            Err(error) => Err(Failure::Transport(error)),
        };
    }
    if response.status().is_redirection() {
        return Err(Failure::Refused(StoreError::Permanent(format!(
            "listing {prefix:?}: HTTP {status} redirect, not followed"
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
    fn into_store_error(self, prefix: &str) -> StoreError {
        match self {
            Failure::Refused(error) => error,
            Failure::Credential(message) => StoreError::Transient(message),
            Failure::Transport(error) => match error.kind() {
                HttpErrorKind::Timeout => StoreError::Timeout,
                _ => StoreError::Transient(format!("listing {prefix:?}: {error}")),
            },
            Failure::Status(status) => status_error(prefix, status),
        }
    }
}

/// A final status, classified by the status alone (ADR-3456 decision 4).
fn status_error(prefix: &str, status: u16) -> StoreError {
    let context = format!("listing {prefix:?}");
    match status {
        401 | 403 => StoreError::AccessDenied(format!("{context}: HTTP {status}")),
        404 => StoreError::Permanent(format!("{context}: bucket does not exist (HTTP 404)")),
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
            return Err(refuse(format!("listed object {key:?} carries no generation")));
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
        .ok_or_else(|| refuse(format!("listed object {key:?} has no readable updated time")))?;
    let etag = match (object.md5_hash, object.etag) {
        (Some(md5), _) => md5_etag(&md5)
            .ok_or_else(|| refuse(format!("listed object {key:?} has an unreadable md5Hash")))?,
        (None, Some(etag)) if !etag.is_empty() => etag,
        (None, _) => {
            return Err(refuse(format!(
                "listed object {key:?} carries neither md5Hash nor etag"
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

    #[test]
    fn components_keep_only_unreserved_bytes() {
        assert_eq!(encode_component("a b/c+d~e"), "a%20b%2Fc%2Bd~e");
        assert_eq!(encode_component("\u{1}é"), "%01%C3%A9");
    }
}
