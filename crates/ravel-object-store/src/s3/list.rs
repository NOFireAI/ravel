//! `S3Store`'s own SigV4 ListObjectsV2 client (ADR-2637 decision 1).
//!
//! `object_store` lists through `Path`, which re-encodes a listed key and
//! resumes from the re-encoded form, so a key that `Path` rewrites is skipped
//! or listed twice at a page boundary. This client sends the prefix and
//! `start-after` as raw keys and returns raw keys, which
//! [`crate::classify_objects`] and [`crate::classify_prefixes`] then split
//! into addressable and unaddressable ones.
//!
//! Requests go through the `HttpClient` that [`super::S3HttpConnector`] builds
//! from the data plane's `ClientOptions`, inside the caller's
//! [`super::connector::scope`], so every attempt is billed to the `list` or
//! `list_delimited` block exactly as before. Signing reuses
//! [`super::bucket_config`]'s SigV4 pieces with the credential provider the
//! store already holds. No error carries a response body: an S3 error body can
//! echo the signed request.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use object_store::aws::AwsCredentialProvider;
use object_store::client::{
    HttpClient, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody, HttpResponse,
};
use object_store::{BackoffConfig, RetryConfig};
use rand::RngExt;
use reqwest::header::{HeaderName, HeaderValue};

use super::bucket_config::{
    self, ControlPlaneError, RequestTarget, SigningClock, SystemSigningClock, XmlElement,
};
use crate::{DelimitedList, Etag, ListPage, ObjectMeta, PageToken, StoreError, Version};

/// Largest ListObjectsV2 response body read. A 1000-key response of 1024-byte
/// keys, URL-encoded to up to three times that, with its per-object metadata,
/// needs about 3.5 MB.
pub(crate) const LIST_MAX_BODY_BYTES: usize = 8 << 20;

/// Most keys one ListObjectsV2 response carries, and the most `max-keys` asks
/// for.
const WIRE_PAGE_KEYS: usize = 1000;

/// Responses a `ListPage` call may receive beyond the ones a full page needs.
const EMPTY_RESPONSE_ALLOWANCE: usize = 16;

/// Largest error body read for its `<Error><Code>`.
const ERROR_BODY_BYTES: usize = 64 << 10;

/// Most ListObjectsV2 responses one [`crate::ObjectStoreBackend::list`] or
/// `list_after` call on `S3Store` receives before it fails with
/// [`StoreError::ListPageCeiling`]: the responses a full page of `page_size`
/// keys needs, plus 16 that may come back empty. A drain is capped at
/// [`crate::MAX_LIST_PAGES`] pages, so a drained listing sends at most
/// `MAX_LIST_PAGES × max_responses_per_page(page_size)` requests.
pub const fn max_responses_per_page(page_size: usize) -> usize {
    page_size.div_ceil(WIRE_PAGE_KEYS) + EMPTY_RESPONSE_ALLOWANCE
}

/// One ListObjectsV2 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListRequest<'a> {
    pub(crate) prefix: &'a str,
    pub(crate) start_after: Option<&'a str>,
    pub(crate) continuation_token: Option<&'a str>,
    pub(crate) max_keys: usize,
    pub(crate) delimited: bool,
}

impl ListRequest<'_> {
    fn query_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = vec![
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), self.prefix.to_string()),
            ("max-keys".to_string(), self.max_keys.to_string()),
            ("encoding-type".to_string(), "url".to_string()),
        ];
        if let Some(after) = self.start_after {
            pairs.push(("start-after".to_string(), after.to_string()));
        }
        if let Some(token) = self.continuation_token {
            pairs.push(("continuation-token".to_string(), token.to_string()));
        }
        if self.delimited {
            pairs.push(("delimiter".to_string(), "/".to_string()));
        }
        pairs
    }
}

/// One parsed ListObjectsV2 response, with raw (decoded) keys.
#[derive(Debug, Clone, Default)]
pub(crate) struct ListResponse {
    pub(crate) contents: Vec<ObjectMeta>,
    /// Each with its trailing `/`.
    pub(crate) common_prefixes: Vec<String>,
    pub(crate) is_truncated: bool,
    pub(crate) next_token: Option<String>,
}

/// Where the paging loops get their responses: [`ListClient`] in production,
/// a script in this module's tests.
pub(crate) trait ListFetch {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError>;
}

/// One `ListPage`: up to `page_size` raw keys under `prefix`, after
/// `start_after`. Follows `NextContinuationToken` within the call, and hands
/// back the raw last key as the page token when the last response was
/// truncated.
pub(crate) async fn list_page<F: ListFetch>(
    fetch: &F,
    prefix: &str,
    start_after: Option<&str>,
    page_size: usize,
) -> Result<ListPage, StoreError> {
    let page_size = page_size.max(1);
    let ceiling = max_responses_per_page(page_size);
    let mut listed: Vec<ObjectMeta> = Vec::new();
    let mut token: Option<String> = None;
    let mut responses = 0usize;
    let truncated = loop {
        if responses == ceiling {
            return Err(StoreError::ListPageCeiling {
                prefix: prefix.to_string(),
                ceiling,
            });
        }
        let remaining = page_size.saturating_sub(listed.len());
        let request = ListRequest {
            prefix,
            start_after: if token.is_none() { start_after } else { None },
            continuation_token: token.as_deref(),
            max_keys: remaining.min(WIRE_PAGE_KEYS),
            delimited: false,
        };
        let response = fetch.fetch(&request).await?;
        responses += 1;
        listed.extend(response.contents);
        if !response.is_truncated {
            break false;
        }
        token = Some(next_token(prefix, token.as_deref(), response.next_token)?);
        if listed.len() >= page_size {
            break true;
        }
    };
    let next = if truncated {
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

/// The whole delimited listing under `prefix`, following
/// `NextContinuationToken` until a response is not truncated. Never resumes
/// from a key: a common prefix can sort after a response's last key.
pub(crate) async fn list_delimited<F: ListFetch>(
    fetch: &F,
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
            start_after: None,
            continuation_token: token.as_deref(),
            max_keys: WIRE_PAGE_KEYS,
            delimited: true,
        };
        let response = fetch.fetch(&request).await?;
        responses += 1;
        listed.extend(response.contents);
        listed_prefixes.extend(response.common_prefixes);
        if !response.is_truncated {
            break;
        }
        token = Some(next_token(prefix, token.as_deref(), response.next_token)?);
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

/// The token to follow a truncated response with: refused when it is absent
/// or repeats the token just sent.
fn next_token(
    prefix: &str,
    sent: Option<&str>,
    received: Option<String>,
) -> Result<String, StoreError> {
    let Some(received) = received else {
        return Err(StoreError::Permanent(format!(
            "listing {prefix:?}: a truncated ListObjectsV2 response carried no \
             NextContinuationToken"
        )));
    };
    if sent == Some(received.as_str()) {
        return Err(StoreError::ListRepeatedToken {
            prefix: prefix.to_string(),
        });
    }
    Ok(received)
}

// --- Response parsing ---

/// Parse a ListObjectsV2 body. When it echoes `<EncodingType>url</EncodingType>`,
/// every `Key` and `CommonPrefixes/Prefix` is URL-decoded with `+` read as a
/// space and `%XX` as its byte; without it the text is literal. A key that is
/// not UTF-8 once decoded keeps its still-encoded text, which carries a `%` and
/// so is never addressable.
pub(crate) fn parse_list_bucket_result(body: &[u8]) -> Result<ListResponse, ControlPlaneError> {
    let root = bucket_config::parse_document(body, "ListBucketResult")?;
    let encoded = match root.single("EncodingType")? {
        None => false,
        Some(element) if element.value().eq_ignore_ascii_case("url") => true,
        Some(element) => {
            return Err(ControlPlaneError::Parse(format!(
                "unknown EncodingType {:?}",
                element.value()
            )));
        }
    };
    let is_truncated = match root.single("IsTruncated")? {
        Some(element) => bucket_config::parse_bool(element)?,
        None => {
            return Err(ControlPlaneError::Parse(
                "the listing carries no IsTruncated element".to_string(),
            ));
        }
    };
    let next_token = root
        .single("NextContinuationToken")?
        .map(|element| element.value().to_string())
        .filter(|token| !token.is_empty());

    let mut contents = Vec::new();
    for entry in root.children_named("Contents") {
        contents.push(parse_contents(entry, encoded)?);
    }
    let mut common_prefixes = Vec::new();
    for entry in root.children_named("CommonPrefixes") {
        let Some(element) = entry.single("Prefix")? else {
            return Err(ControlPlaneError::Parse(
                "a CommonPrefixes entry carries no Prefix".to_string(),
            ));
        };
        common_prefixes.push(listed_text(
            &element.text,
            encoded,
            "CommonPrefixes/Prefix",
        )?);
    }
    Ok(ListResponse {
        contents,
        common_prefixes,
        is_truncated,
        next_token,
    })
}

fn parse_contents(entry: &XmlElement, encoded: bool) -> Result<ObjectMeta, ControlPlaneError> {
    let key = match entry.single("Key")? {
        Some(element) => listed_text(&element.text, encoded, "Key")?,
        None => {
            return Err(ControlPlaneError::Parse(
                "a listed object carries no Key".to_string(),
            ));
        }
    };
    let required = |name: &str| -> Result<&XmlElement, ControlPlaneError> {
        entry.single(name)?.ok_or_else(|| {
            ControlPlaneError::Parse(format!("listed object {key:?} carries no {name}"))
        })
    };
    let size: u64 = required("Size")?.value().parse().map_err(|_| {
        ControlPlaneError::Parse(format!("listed object {key:?} has an unreadable Size"))
    })?;
    let etag = required("ETag")?.value().to_string();
    if etag.is_empty() {
        return Err(ControlPlaneError::Parse(format!(
            "listed object {key:?} has an empty ETag"
        )));
    }
    let (secs, nanos) = bucket_config::parse_iso8601(required("LastModified")?.value())
        .ok_or_else(|| {
            ControlPlaneError::Parse(format!(
                "listed object {key:?} has an unreadable LastModified"
            ))
        })?;
    Ok(ObjectMeta {
        key,
        size,
        etag: Etag(etag.clone()),
        version: Version(etag),
        last_modified_unix_ms: secs
            .saturating_mul(1000)
            .saturating_add(i64::from(nanos / 1_000_000)),
    })
}

/// A listed key or common prefix as the store holds it.
fn listed_text(text: &str, encoded: bool, element: &str) -> Result<String, ControlPlaneError> {
    if !encoded {
        return Ok(text.to_string());
    }
    match url_decode(text) {
        Some(Ok(decoded)) => Ok(decoded),
        Some(Err(_not_utf8)) => Ok(text.to_string()),
        None => Err(ControlPlaneError::Parse(format!(
            "a {element} is not valid URL encoding"
        ))),
    }
}

/// `encoding-type=url` decoding: `+` is a space, `%XX` is the byte `XX`, any
/// other character is itself. `None` for a `%` not followed by two hex digits;
/// `Some(Err(bytes))` when the decoded bytes are not UTF-8.
pub(crate) fn url_decode(text: &str) -> Option<Result<String, Vec<u8>>> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                let high = hex_value(*bytes.get(index + 1)?)?;
                let low = hex_value(*bytes.get(index + 2)?)?;
                out.push(high << 4 | low);
                index += 3;
            }
            other => {
                out.push(other);
                index += 1;
            }
        }
    }
    Some(String::from_utf8(out).map_err(|e| e.into_bytes()))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// --- Retry ---

/// `object_store`'s decorrelated-jitter backoff, which it keeps crate-private:
/// each pause is the previous draw, and the next is drawn from
/// `[init, previous × base)` and capped at `max`. The first pause is `init`
/// exactly.
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    init_secs: f64,
    next_secs: f64,
    max_secs: f64,
    base: f64,
}

impl Backoff {
    pub(crate) fn new(config: &BackoffConfig) -> Self {
        let init_secs = config.init_backoff.as_secs_f64();
        Backoff {
            init_secs,
            next_secs: init_secs,
            max_secs: config.max_backoff.as_secs_f64(),
            base: config.base,
        }
    }

    /// The next pause, drawing the one after it with `draw(low, high)`, which
    /// returns a value in `[low, high)`.
    pub(crate) fn next_with(&mut self, draw: impl FnOnce(f64, f64) -> f64) -> Duration {
        let low = self.init_secs;
        let high = self.next_secs * self.base;
        let drawn = if high > low { draw(low, high) } else { low };
        let next = self.max_secs.min(drawn);
        Duration::from_secs_f64(std::mem::replace(&mut self.next_secs, next))
    }

    fn next(&mut self) -> Duration {
        self.next_with(|low, high| rand::rng().random_range(low..high))
    }
}

/// Whether a transport failure is retried. A list GET is idempotent, so a
/// timeout or an interrupted request is retried along with a connect or
/// request failure; a decode or unknown failure is not.
fn transport_retryable(kind: HttpErrorKind) -> bool {
    matches!(
        kind,
        HttpErrorKind::Connect
            | HttpErrorKind::Request
            | HttpErrorKind::Timeout
            | HttpErrorKind::Interrupted
    )
}

fn status_retryable(status: u16) -> bool {
    (500..600).contains(&status) || status == 429 || status == 408
}

// --- The client ---

/// Signs and sends ListObjectsV2 requests for one bucket.
pub(crate) struct ListClient {
    http: HttpClient,
    credentials: AwsCredentialProvider,
    clock: Arc<dyn SigningClock>,
    retry: RetryConfig,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    force_path_style: bool,
}

/// One request on behalf of the listing of `prefix`.
struct Bound<'a> {
    client: &'a ListClient,
    prefix: &'a str,
}

impl ListFetch for Bound<'_> {
    async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
        self.client.fetch(self.prefix, request).await
    }
}

impl ListClient {
    pub(crate) fn new(
        http: HttpClient,
        credentials: AwsCredentialProvider,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        force_path_style: bool,
    ) -> Self {
        ListClient {
            http,
            credentials,
            clock: Arc::new(SystemSigningClock),
            retry: RetryConfig::default(),
            bucket,
            region,
            endpoint,
            force_path_style,
        }
    }

    pub(crate) async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page_size: usize,
    ) -> Result<ListPage, StoreError> {
        list_page(
            &Bound {
                client: self,
                prefix,
            },
            prefix,
            start_after,
            page_size,
        )
        .await
    }

    pub(crate) async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        list_delimited(
            &Bound {
                client: self,
                prefix,
            },
            prefix,
            crate::MAX_LIST_PAGES,
        )
        .await
    }

    async fn fetch(
        &self,
        prefix: &str,
        request: &ListRequest<'_>,
    ) -> Result<ListResponse, StoreError> {
        let body = self.send(prefix, &request.query_pairs()).await?;
        parse_list_bucket_result(&body).map_err(|e| control_plane_error(prefix, e))
    }

    /// Send one GET under the retry rules and return the success body.
    async fn send(
        &self,
        prefix: &str,
        query_pairs: &[(String, String)],
    ) -> Result<Vec<u8>, StoreError> {
        let target = bucket_config::request_target(
            &self.bucket,
            &self.region,
            self.endpoint.as_deref(),
            self.force_path_style,
            None,
            query_pairs,
        )
        .map_err(|e| control_plane_error(prefix, e))?;
        let started = tokio::time::Instant::now();
        let mut backoff = Backoff::new(&self.retry.backoff);
        let mut retries = 0usize;
        loop {
            let exhausted =
                retries >= self.retry.max_retries || started.elapsed() > self.retry.retry_timeout;
            let outcome = match self.attempt(prefix, &target, query_pairs).await? {
                Ok(response) => self.read_response(prefix, response).await,
                Err(error) => Err(Failure::Transport(error)),
            };
            let retry = match &outcome {
                Ok(_) => false,
                Err(Failure::Status { status, .. }) => status_retryable(*status),
                Err(Failure::Transport(error)) => transport_retryable(error.kind()),
                Err(Failure::Refused(_)) => false,
            };
            if !retry || exhausted {
                return outcome.map_err(|failure| failure.into_store_error(prefix));
            }
            tokio::time::sleep(backoff.next()).await;
            retries += 1;
        }
    }

    /// Sign and send one attempt. The outer error is one no retry can change.
    async fn attempt(
        &self,
        prefix: &str,
        target: &RequestTarget,
        query_pairs: &[(String, String)],
    ) -> Result<Result<HttpResponse, HttpError>, StoreError> {
        let credential = self.credentials.get_credential().await.map_err(|e| {
            StoreError::Transient(format!("listing {prefix:?}: credential fetch failed: {e}"))
        })?;
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.uri_mut() = target.url.parse().map_err(|e| {
            StoreError::Permanent(format!("listing {prefix:?}: invalid request URL: {e}"))
        })?;
        for (name, value) in bucket_config::signed_get_headers(
            target,
            query_pairs,
            &credential,
            &self.region,
            self.clock.now_unix_secs(),
        ) {
            let value = HeaderValue::from_str(&value).map_err(|_| {
                StoreError::Permanent(format!(
                    "listing {prefix:?}: the {name} header is not a valid header value"
                ))
            })?;
            request
                .headers_mut()
                .insert(HeaderName::from_static(name), value);
        }
        Ok(self.http.execute(request).await)
    }

    /// The success body, or the failure a response stands for.
    async fn read_response(
        &self,
        prefix: &str,
        response: HttpResponse,
    ) -> Result<Vec<u8>, Failure> {
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
                "listing {prefix:?}: HTTP {status} redirect, not followed; check the \
                 configured region and endpoint"
            ))));
        }
        let code = match read_body(response, ERROR_BODY_BYTES).await {
            Ok(Some(body)) => bucket_config::parse_error_code(&body),
            Ok(None) | Err(_) => None,
        };
        Err(Failure::Status { status, code })
    }
}

/// Read a body of at most `limit` bytes; `Ok(None)` when it is longer.
async fn read_body(response: HttpResponse, limit: usize) -> Result<Option<Vec<u8>>, HttpError> {
    let mut stream = response.into_body().bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len() + chunk.len() > limit {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// Why one attempt did not return a listing.
enum Failure {
    /// A non-success, non-redirect status, with the body's S3 error code.
    Status { status: u16, code: Option<String> },
    /// The request or the body read failed in transport.
    Transport(HttpError),
    /// Already classified, and never retried.
    Refused(StoreError),
}

impl Failure {
    fn into_store_error(self, prefix: &str) -> StoreError {
        match self {
            Failure::Refused(error) => error,
            Failure::Transport(error) => match error.kind() {
                HttpErrorKind::Timeout => StoreError::Timeout,
                _ => StoreError::Transient(format!("listing {prefix:?}: {error}")),
            },
            Failure::Status { status, code } => status_error(prefix, status, code.as_deref()),
        }
    }
}

/// The error table of ADR-2637 decision 1, for a status that is final.
pub(crate) fn status_error(prefix: &str, status: u16, code: Option<&str>) -> StoreError {
    let code_detail = match code {
        Some(code) => format!("S3 error code {code}"),
        None => "no S3 error code".to_string(),
    };
    let context = format!("listing {prefix:?}");
    match status {
        403 => StoreError::AccessDenied(format!("{context}: HTTP 403, {code_detail}")),
        429 | 503 => StoreError::Throttled {
            retry_after_ms: 1000,
        },
        _ if code.is_some_and(super::is_throttle_code) => StoreError::Throttled {
            retry_after_ms: 1000,
        },
        404 if code == Some("NoSuchBucket") => super::no_such_bucket(&context),
        408 | 500..=599 => {
            StoreError::Transient(format!("{context}: HTTP {status}, {code_detail}"))
        }
        _ => StoreError::Permanent(format!("{context}: HTTP {status}, {code_detail}")),
    }
}

/// A request-building or parse failure from [`bucket_config`]. Only
/// `request_target` (a configuration it cannot address) and `parse_document`
/// (a body that is not a `ListBucketResult`) reach here, and neither changes
/// on a retry.
fn control_plane_error(prefix: &str, error: ControlPlaneError) -> StoreError {
    let context = format!("listing {prefix:?}");
    match error {
        ControlPlaneError::AccessDenied(detail) => {
            StoreError::AccessDenied(format!("{context}: {detail}"))
        }
        ControlPlaneError::Parse(detail) => StoreError::Permanent(format!(
            "{context}: the response is not a ListBucketResult: {detail}"
        )),
        ControlPlaneError::Transport(detail)
        | ControlPlaneError::NotConfigured(detail)
        | ControlPlaneError::NotFound(detail) => {
            StoreError::Permanent(format!("{context}: {detail}"))
        }
        ControlPlaneError::Redirect(status) => {
            StoreError::Permanent(format!("{context}: HTTP {status} redirect, not followed"))
        }
        ControlPlaneError::BodyTooLarge => {
            StoreError::Permanent(format!("{context}: the response body is too large"))
        }
        ControlPlaneError::UnexpectedStatus(status, detail) => {
            StoreError::Permanent(format!("{context}: HTTP {status}, {detail}"))
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::VecDeque;

    use parking_lot::Mutex;

    use super::*;

    /// The bounds of every pause, pinned against `RetryConfig::default()`: the
    /// first pause is `init_backoff` exactly, each later draw's range is
    /// `[init, previous × base)`, and a draw is capped at `max_backoff`. Drawn
    /// at the top of each range, the pauses double from 100 ms to the 15 s cap;
    /// drawn at the bottom, every pause is 100 ms.
    #[test]
    fn backoff_bounds_follow_the_default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.retry_timeout, Duration::from_secs(180));
        assert_eq!(config.backoff.init_backoff, Duration::from_millis(100));
        assert_eq!(config.backoff.max_backoff, Duration::from_secs(15));
        assert_eq!(config.backoff.base, 2.0);

        let mut upper = Backoff::new(&config.backoff);
        let mut ranges = Vec::new();
        let pauses: Vec<Duration> = (0..config.max_retries)
            .map(|_| {
                upper.next_with(|low, high| {
                    ranges.push((low, high));
                    high
                })
            })
            .collect();
        let millis: Vec<u128> = pauses.iter().map(Duration::as_millis).collect();
        assert_eq!(
            millis,
            [100, 200, 400, 800, 1600, 3200, 6400, 12800, 15000, 15000]
        );
        let expected_ranges = [
            (0.1, 0.2),
            (0.1, 0.4),
            (0.1, 0.8),
            (0.1, 1.6),
            (0.1, 3.2),
            (0.1, 6.4),
            (0.1, 12.8),
            (0.1, 25.6),
            (0.1, 30.0),
            (0.1, 30.0),
        ];
        assert_eq!(ranges.len(), expected_ranges.len());
        for ((low, high), (want_low, want_high)) in ranges.iter().zip(expected_ranges) {
            assert!((low - want_low).abs() < 1e-9, "{ranges:?}");
            assert!((high - want_high).abs() < 1e-9, "{ranges:?}");
        }

        let mut lower = Backoff::new(&config.backoff);
        for _ in 0..config.max_retries {
            assert_eq!(lower.next_with(|low, _| low), Duration::from_millis(100));
        }

        let mut drawn = Backoff::new(&config.backoff);
        for _ in 0..config.max_retries {
            let pause = drawn.next();
            assert!(
                pause >= Duration::from_millis(100) && pause <= Duration::from_secs(15),
                "{pause:?}"
            );
        }
    }

    #[test]
    fn url_decoding_reads_plus_as_space_and_percent_as_a_byte() {
        assert_eq!(url_decode("a%2Bb"), Some(Ok("a+b".to_string())));
        assert_eq!(url_decode("a+b"), Some(Ok("a b".to_string())));
        assert_eq!(url_decode("%0A"), Some(Ok("\n".to_string())));
        assert_eq!(url_decode("p/%C3%A9"), Some(Ok("p/é".to_string())));
        assert_eq!(url_decode("p/%ff"), Some(Err(vec![b'p', b'/', 0xff])));
        assert_eq!(url_decode("100%"), None);
        assert_eq!(url_decode("%G1"), None);
    }

    fn body(encoding: Option<&str>, keys: &[&str], prefixes: &[&str], tail: &str) -> String {
        let mut xml = String::from("<ListBucketResult>");
        if let Some(encoding) = encoding {
            xml.push_str(&format!("<EncodingType>{encoding}</EncodingType>"));
        }
        for key in keys {
            xml.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>3</Size><ETag>&quot;e&quot;</ETag>\
                 <LastModified>2026-01-02T03:04:05.678Z</LastModified></Contents>"
            ));
        }
        for prefix in prefixes {
            xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"
            ));
        }
        xml.push_str(tail);
        xml.push_str("</ListBucketResult>");
        xml
    }

    #[test]
    fn an_encoded_listing_is_decoded_and_a_literal_one_is_not() {
        let encoded = parse_list_bucket_result(
            body(
                Some("url"),
                &["a%2Bb", "a+b", "%0A", "p/%FF"],
                &["t/x+y/"],
                "<IsTruncated>false</IsTruncated>",
            )
            .as_bytes(),
        )
        .expect("parses");
        let keys: Vec<&str> = encoded.contents.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, ["a+b", "a b", "\n", "p/%FF"]);
        assert!(!crate::is_addressable_key("p/%FF"));
        assert_eq!(encoded.common_prefixes, ["t/x y/"]);
        let meta = &encoded.contents[0];
        assert_eq!(meta.size, 3);
        assert_eq!(meta.etag.0, "\"e\"");
        assert_eq!(meta.version.0, "\"e\"");
        assert_eq!(meta.last_modified_unix_ms, 1_767_323_045_678);

        let literal = parse_list_bucket_result(
            body(
                None,
                &["a%2Bb", "a+b"],
                &[],
                "<IsTruncated>true</IsTruncated>\
                 <NextContinuationToken>tok</NextContinuationToken>",
            )
            .as_bytes(),
        )
        .expect("parses");
        let keys: Vec<&str> = literal.contents.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, ["a%2Bb", "a+b"]);
        assert!(literal.is_truncated);
        assert_eq!(literal.next_token.as_deref(), Some("tok"));
    }

    #[test]
    fn a_body_that_is_not_a_listing_is_refused() {
        for bad in [
            "<Error><Code>InternalError</Code></Error>".to_string(),
            body(Some("url"), &[], &[], ""),
            body(
                Some("url"),
                &["100%"],
                &[],
                "<IsTruncated>false</IsTruncated>",
            ),
            body(Some("base64"), &[], &[], "<IsTruncated>false</IsTruncated>"),
            "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Key>\
             </Contents></ListBucketResult>"
                .to_string(),
        ] {
            assert!(
                matches!(
                    parse_list_bucket_result(bad.as_bytes()),
                    Err(ControlPlaneError::Parse(_))
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_status_table_matches_decision_one() {
        assert!(matches!(
            status_error("p/", 403, Some("AccessDenied")),
            StoreError::AccessDenied(_)
        ));
        for (status, code) in [
            (429, None),
            (503, Some("SlowDown")),
            (400, Some("SlowDownRead")),
        ] {
            assert!(matches!(
                status_error("p/", status, code),
                StoreError::Throttled {
                    retry_after_ms: 1000
                }
            ));
        }
        assert!(matches!(
            status_error("p/", 404, Some("NoSuchBucket")),
            StoreError::Permanent(m) if m.contains("NoSuchBucket")
        ));
        for status in [500, 502, 408] {
            assert!(matches!(
                status_error("p/", status, None),
                StoreError::Transient(_)
            ));
        }
        for (status, code) in [
            (400, Some("InvalidArgument")),
            (404, Some("NoSuchKey")),
            (301, None),
        ] {
            match status_error("p/", status, code) {
                StoreError::Permanent(message) => {
                    assert!(message.contains(&status.to_string()), "{message}");
                    if let Some(code) = code {
                        assert!(message.contains(code), "{message}");
                    }
                }
                other => panic!("{status}: {other:?}"),
            }
        }
    }

    /// `start-after`, `continuation-token` and `max-keys` of one request.
    type Asked = (Option<String>, Option<String>, usize);

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
    }

    impl ListFetch for Script {
        async fn fetch(&self, request: &ListRequest<'_>) -> Result<ListResponse, StoreError> {
            self.seen.lock().push((
                request.start_after.map(str::to_string),
                request.continuation_token.map(str::to_string),
                request.max_keys,
            ));
            Ok(self.responses.lock().pop_front().unwrap_or_default())
        }
    }

    fn meta(key: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size: 1,
            etag: Etag("e".to_string()),
            version: Version("e".to_string()),
            last_modified_unix_ms: 0,
        }
    }

    fn truncated(keys: &[&str], token: &str) -> ListResponse {
        ListResponse {
            contents: keys.iter().map(|k| meta(k)).collect(),
            common_prefixes: Vec::new(),
            is_truncated: true,
            next_token: Some(token.to_string()),
        }
    }

    fn last(keys: &[&str]) -> ListResponse {
        ListResponse {
            contents: keys.iter().map(|k| meta(k)).collect(),
            ..ListResponse::default()
        }
    }

    #[tokio::test]
    async fn a_page_follows_tokens_and_resumes_from_its_raw_last_key() {
        let script = Script::new([truncated(&[], "t1"), truncated(&["p/a", "p/b*"], "t2")]);
        let page = list_page(&script, "p/", Some("p/0"), 2)
            .await
            .expect("page");
        assert_eq!(page.next, Some(PageToken("p/b*".to_string())));
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.unaddressable.len(), 1);
        assert_eq!(
            *script.seen.lock(),
            [
                (Some("p/0".to_string()), None, 2),
                (None, Some("t1".to_string()), 2),
            ]
        );

        let script = Script::new([truncated(&["p/a"], "t1"), last(&["p/b"])]);
        let page = list_page(&script, "p/", None, 5).await.expect("page");
        assert_eq!(page.next, None);
        assert_eq!(page.objects.len(), 2);
        assert_eq!(
            *script.seen.lock(),
            [(None, None, 5), (None, Some("t1".to_string()), 4)]
        );
    }

    #[tokio::test]
    async fn a_page_refuses_a_repeated_token_a_missing_token_and_too_many_responses() {
        let script = Script::new([truncated(&[], "t1"), truncated(&[], "t1")]);
        assert!(matches!(
            list_page(&script, "p/", None, 2).await,
            Err(StoreError::ListRepeatedToken { prefix }) if prefix == "p/"
        ));

        let script = Script::new([ListResponse {
            is_truncated: true,
            ..ListResponse::default()
        }]);
        assert!(matches!(
            list_page(&script, "p/", None, 2).await,
            Err(StoreError::Permanent(m)) if m.contains("\"p/\"") && m.contains("NextContinuationToken")
        ));

        let fresh: Vec<ListResponse> = (0..100).map(|i| truncated(&[], &format!("t{i}"))).collect();
        let script = Script::new(fresh);
        assert!(matches!(
            list_page(&script, "p/", None, 1).await,
            Err(StoreError::ListPageCeiling { ceiling: 17, .. })
        ));
        assert_eq!(script.seen.lock().len(), 17);
        assert_eq!(max_responses_per_page(1000), 17);
        assert_eq!(max_responses_per_page(1001), 18);
    }

    #[tokio::test]
    async fn a_delimited_listing_follows_tokens_to_the_end_and_stops_at_its_ceiling() {
        let prefixes = |names: &[&str], token: Option<&str>| ListResponse {
            common_prefixes: names.iter().map(|n| n.to_string()).collect(),
            is_truncated: token.is_some(),
            next_token: token.map(str::to_string),
            ..ListResponse::default()
        };
        let script = Script::new([
            prefixes(&["t/a/"], Some("t1")),
            prefixes(&["t/b/", "t/\u{1}/"], Some("t2")),
            prefixes(&["t/c/"], None),
        ]);
        let listing = list_delimited(&script, "t/", 10).await.expect("listing");
        assert_eq!(listing.common_prefixes, ["t/a/", "t/b/", "t/c/"]);
        assert_eq!(listing.unaddressable_prefixes, ["t/\u{1}/"]);
        assert_eq!(
            *script.seen.lock(),
            [
                (None, None, 1000),
                (None, Some("t1".to_string()), 1000),
                (None, Some("t2".to_string()), 1000),
            ]
        );

        let fresh: Vec<ListResponse> = (0..10)
            .map(|i| prefixes(&[], Some(&format!("t{i}"))))
            .collect();
        let script = Script::new(fresh);
        assert!(matches!(
            list_delimited(&script, "t/", 3).await,
            Err(StoreError::ListPageCeiling { ceiling: 3, .. })
        ));
        assert_eq!(script.seen.lock().len(), 3);
    }

    #[test]
    fn the_query_carries_raw_keys_and_only_the_parameters_asked_for() {
        let request = ListRequest {
            prefix: "p/",
            start_after: Some("p/a#b"),
            continuation_token: None,
            max_keys: 7,
            delimited: false,
        };
        let pairs = request.query_pairs();
        let get = |name: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("list-type"), Some("2"));
        assert_eq!(get("prefix"), Some("p/"));
        assert_eq!(get("start-after"), Some("p/a#b"));
        assert_eq!(get("max-keys"), Some("7"));
        assert_eq!(get("encoding-type"), Some("url"));
        assert_eq!(get("continuation-token"), None);
        assert_eq!(get("delimiter"), None);

        let delimited = ListRequest {
            prefix: "t/",
            start_after: None,
            continuation_token: Some("tok"),
            max_keys: 1000,
            delimited: true,
        }
        .query_pairs();
        assert!(delimited.contains(&("delimiter".to_string(), "/".to_string())));
        assert!(delimited.contains(&("continuation-token".to_string(), "tok".to_string())));
        assert!(!delimited.iter().any(|(k, _)| k == "start-after"));
    }
}
