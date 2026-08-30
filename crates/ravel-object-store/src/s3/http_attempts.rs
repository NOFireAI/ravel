//! Per-attempt HTTP request counting for the S3 backend (#928).
//!
//! `object_store` runs its own internal retry loop per logical operation
//! (`RetryConfig`: default `max_retries = 10`, `retry_timeout = 180 s`,
//! jittered exponential backoff). That loop lives *inside* the crate: an
//! [`InstrumentedStore`](crate::instrument::InstrumentedStore) counter wraps
//! the logical trait call and by construction sees only its final completion,
//! which is exactly why the instrument module's docs say "`calls` is
//! completions, not attempts". So one `get()` that retried nine times records
//! one `StoreOp::Get` and one request in every counter above this layer, while
//! S3 was billed for ten.
//!
//! The only place object_store's retries are observable is *beneath*
//! object_store, where every attempt is a distinct HTTP request. object_store
//! 0.14 exposes that seam: [`HttpConnector`] builds the [`HttpClient`] the
//! `AmazonS3` client sends every request through, and its retry loop calls
//! [`HttpClient::execute`] once per attempt. [`CountingHttpConnector`] wraps the
//! real transport ([`object_store::client::ReqwestConnector`]) and hands back a
//! client whose [`HttpService::call`] increments a counter before delegating,
//! so it counts the initial attempt and every retry, including the successful
//! final one.
//!
//! # What the count means
//!
//! - `attempts >= calls` always holds, and `attempts - calls` is the number of
//!   retries object_store performed, per operation kind. On a clean path with
//!   no retries the two are equal.
//! - Attribution is derived from the wire request itself (HTTP method plus the
//!   query string), not from a logical call the way `StoreOp` counters are, so
//!   it reflects the requests S3 actually saw and billed:
//!   `HEAD` -> [`StoreOp::Head`], `DELETE` -> [`StoreOp::Delete`],
//!   `PUT`/`POST` -> [`StoreOp::Put`] (a single PUT, a multipart part upload,
//!   and CreateMultipartUpload/CompleteMultipartUpload all fold into `Put`,
//!   since no other `StoreOp` describes multipart traffic), a `GET` carrying an
//!   S3 `list-type` parameter -> [`StoreOp::List`] (or [`StoreOp::ListDelimited`]
//!   when it also carries a `delimiter`), and any other `GET` -> [`StoreOp::Get`].
//!   A request whose method this crate never issues is counted under
//!   [`HttpAttemptSnapshot::unclassified`] rather than misattributed.
//! - Bytes are deliberately *not* counted here: a failed attempt returns no
//!   payload, and the byte accounting above this layer already counts only the
//!   final returned data, so bytes stay honest while this makes the request
//!   count honest too.
//!
//! This is observability only, exactly like `InstrumentedStore`: the wrapped
//! transport's `HttpClient` is returned and every request and response is
//! forwarded verbatim, so wrapping it is a zero-behavior-change decorator.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
};

use crate::instrument::{STORE_OP_COUNT, StoreOp};

/// Classify one HTTP request into the [`StoreOp`] it counts under, from the
/// wire request alone. `None` means a method this crate never issues, counted
/// as unclassified rather than folded into some op it did not cause.
fn classify(req: &HttpRequest) -> Option<StoreOp> {
    classify_parts(req.method().as_str(), req.uri().query().unwrap_or(""))
}

/// The classification, split out from [`HttpRequest`] so it is unit-testable
/// without constructing one (which would pull the `http` crate into this
/// crate's dependencies for a test alone).
fn classify_parts(method: &str, query: &str) -> Option<StoreOp> {
    match method {
        "HEAD" => Some(StoreOp::Head),
        "DELETE" => Some(StoreOp::Delete),
        // A single PUT, a multipart part upload, and the multipart
        // create/complete POSTs all fold into `Put`: no other `StoreOp`
        // describes multipart traffic, and every one of them is a write S3
        // bills as such.
        "PUT" | "POST" => Some(StoreOp::Put),
        "GET" => {
            if has_query_param(query, "list-type") {
                if has_query_param(query, "delimiter") {
                    Some(StoreOp::ListDelimited)
                } else {
                    Some(StoreOp::List)
                }
            } else {
                Some(StoreOp::Get)
            }
        }
        _ => None,
    }
}

/// Whether `query` carries a parameter named exactly `key` (value irrelevant).
/// S3's `ListObjectsV2` uses `list-type=2` and delimited listing adds
/// `delimiter=%2F`; a plain object `GET` carries neither.
fn has_query_param(query: &str, key: &str) -> bool {
    query.split('&').any(|pair| {
        let name = pair.split('=').next().unwrap_or(pair);
        name == key
    })
}

/// HTTP attempt counters, one per [`StoreOp`], plus an unclassified bucket.
/// Flat `AtomicU64`s behind an `Arc` shared with the [`CountingHttpService`],
/// mirroring [`crate::instrument::StoreMetrics`]' process-global monotonic
/// totals with no per-key or per-tenant dimension.
#[derive(Debug, Default)]
pub(crate) struct HttpAttemptMetrics {
    attempts: [AtomicU64; STORE_OP_COUNT],
    unclassified: AtomicU64,
}

impl HttpAttemptMetrics {
    /// Record one HTTP attempt for the request's classification.
    fn record(&self, op: Option<StoreOp>) {
        match op {
            Some(op) => {
                self.attempts[op.index()].fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.unclassified.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Point-in-time copy of every counter. Not a consistent cut across ops
    /// (a concurrent request may land between two fields); it is a scrape, and
    /// nothing correctness-bearing reads it.
    pub(crate) fn snapshot(&self) -> HttpAttemptSnapshot {
        let mut attempts = [0u64; STORE_OP_COUNT];
        for (slot, counter) in attempts.iter_mut().zip(self.attempts.iter()) {
            *slot = counter.load(Ordering::Relaxed);
        }
        HttpAttemptSnapshot {
            attempts,
            unclassified: self.unclassified.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of [`HttpAttemptMetrics`]. `Copy`, no allocation, the
/// shape a scrape path reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpAttemptSnapshot {
    /// HTTP attempts by [`StoreOp::index`]. Includes every retry and the
    /// successful final attempt.
    attempts: [u64; STORE_OP_COUNT],
    /// Attempts whose method this crate never issues, so they were not
    /// attributed to any [`StoreOp`].
    pub unclassified: u64,
}

impl HttpAttemptSnapshot {
    /// HTTP attempts recorded for one operation kind. Compare against the
    /// matching `StoreMetrics` `calls` figure: the difference is the retry
    /// count object_store performed for that kind.
    pub fn attempts(&self, op: StoreOp) -> u64 {
        self.attempts[op.index()]
    }

    /// Every HTTP attempt across all operation kinds, unclassified included.
    pub fn total(&self) -> u64 {
        self.attempts.iter().sum::<u64>() + self.unclassified
    }
}

/// [`HttpConnector`] decorator that counts every HTTP attempt the built client
/// makes. Wraps any inner connector (the real
/// [`object_store::client::ReqwestConnector`] in production) and shares one
/// [`HttpAttemptMetrics`] across every client it builds.
#[derive(Debug)]
pub(crate) struct CountingHttpConnector<C> {
    inner: C,
    metrics: Arc<HttpAttemptMetrics>,
}

impl<C> CountingHttpConnector<C> {
    pub(crate) fn new(inner: C, metrics: Arc<HttpAttemptMetrics>) -> Self {
        CountingHttpConnector { inner, metrics }
    }
}

impl<C: HttpConnector> HttpConnector for CountingHttpConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.inner.connect(options)?;
        Ok(HttpClient::new(CountingHttpService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }))
    }
}

/// The [`HttpService`] wrapped around the real transport's client. object_store's
/// retry loop calls this once per attempt, so counting here counts attempts.
#[derive(Debug)]
struct CountingHttpService {
    inner: HttpClient,
    metrics: Arc<HttpAttemptMetrics>,
}

#[async_trait::async_trait]
impl HttpService for CountingHttpService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Count before delegating so an attempt is recorded even if the inner
        // call's future is dropped before it resolves: the request left this
        // process, which is what S3 bills.
        self.metrics.record(classify(&req));
        self.inner.execute(req).await
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn classify_maps_each_method_to_its_op() {
        assert_eq!(classify_parts("GET", ""), Some(StoreOp::Get));
        assert_eq!(classify_parts("HEAD", ""), Some(StoreOp::Head));
        assert_eq!(classify_parts("DELETE", ""), Some(StoreOp::Delete));
        assert_eq!(classify_parts("PUT", ""), Some(StoreOp::Put));
        assert_eq!(classify_parts("POST", "uploads"), Some(StoreOp::Put));
    }

    #[test]
    fn classify_splits_list_from_get_and_delimited_from_list() {
        assert_eq!(
            classify_parts("GET", "list-type=2&prefix=a/"),
            Some(StoreOp::List)
        );
        assert_eq!(
            classify_parts("GET", "list-type=2&delimiter=%2F&prefix=a/"),
            Some(StoreOp::ListDelimited)
        );
        // A plain object GET whose key contains the substring "list-type" has
        // no query string, so it is a Get: matching is by query-parameter
        // name, never by a substring of the path.
        assert_eq!(classify_parts("GET", ""), Some(StoreOp::Get));
    }

    #[test]
    fn classify_returns_none_for_unissued_methods() {
        assert_eq!(classify_parts("PATCH", ""), None);
    }

    #[test]
    fn record_accumulates_per_op_and_snapshot_reads_it_back() {
        let metrics = HttpAttemptMetrics::default();
        metrics.record(Some(StoreOp::Get));
        metrics.record(Some(StoreOp::Get));
        metrics.record(Some(StoreOp::Put));
        metrics.record(None);

        let snap = metrics.snapshot();
        assert_eq!(snap.attempts(StoreOp::Get), 2);
        assert_eq!(snap.attempts(StoreOp::Put), 1);
        assert_eq!(snap.attempts(StoreOp::Head), 0);
        assert_eq!(snap.unclassified, 1);
        assert_eq!(snap.total(), 4);
    }
}
