//! Attempt counting beneath `object_store`'s retry loop (#928).
//!
//! [`crate::instrument::InstrumentedStore`] counts completions of a logical
//! call. `object_store` retries inside one, up to `RetryConfig::max_retries`
//! (10) within `retry_timeout` (180 s), so a `get()` that succeeded on its
//! tenth try is one `calls`, one `QueryAccounting` GET, and ten requests S3
//! bills. Bytes are unaffected (a failed attempt returns no payload), which is
//! what makes the divergence one-directional: every request figure Ravel
//! reports is a lower bound on the real one, by an amount nothing recorded.
//!
//! The retry loop is private to `object_store`, so no wrapper *above* the
//! logical call can see an attempt. It can be seen from below:
//! `AmazonS3Builder::with_http_connector` installs a factory for the HTTP
//! client the loop drives, and the loop calls that client once per attempt
//! (`RetryableRequest::send` calls `HttpClient::execute` inside its `loop`).
//! [`AttemptCountingConnector`] therefore delegates to `object_store`'s own
//! [`ReqwestConnector`] for the transport, unchanged, and only counts what
//! passes through it.
//!
//! Attribution is by S3 request shape, not by a caller-threaded tag: the
//! `HttpRequest` is all this layer gets. [`classify`] maps method plus query
//! onto [`StoreOp`] and is total over the requests [`crate::s3::S3Store`]
//! issues; anything it does not recognize is counted as unclassified rather
//! than dropped, so a mapping that goes stale is visible instead of silently
//! shrinking every other slot.

use std::sync::Arc;

use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};

use crate::instrument::{StoreMetrics, StoreOp};

/// [`HttpConnector`] that counts every request the client it builds sends,
/// then delegates to `object_store`'s own reqwest transport.
///
/// Zero behavior change: the wrapped [`ReqwestConnector`] receives the same
/// [`ClientOptions`] the builder would have handed it, so the deliberate
/// timeout tuning in [`crate::s3::S3HttpConfig`] still reaches the client.
#[derive(Debug)]
pub(crate) struct AttemptCountingConnector {
    metrics: Arc<StoreMetrics>,
    inner: ReqwestConnector,
}

impl AttemptCountingConnector {
    /// Count into `metrics`, and mark it as attempt-observing so a snapshot
    /// reports the counts instead of [`None`]. Declaring here rather than on
    /// the first request means a store that has issued nothing reports
    /// `Some(0)`, which is a measurement, rather than `None`, which is not.
    pub(crate) fn new(metrics: Arc<StoreMetrics>) -> Self {
        metrics.declare_attempts_observed();
        AttemptCountingConnector {
            metrics,
            inner: ReqwestConnector::default(),
        }
    }
}

impl HttpConnector for AttemptCountingConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.inner.connect(options)?;
        Ok(HttpClient::new(AttemptCountingService {
            metrics: Arc::clone(&self.metrics),
            inner,
        }))
    }
}

/// The counting seam itself: one `call` is one request on the wire.
#[derive(Debug)]
struct AttemptCountingService {
    metrics: Arc<StoreMetrics>,
    inner: HttpClient,
}

#[async_trait::async_trait]
impl HttpService for AttemptCountingService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Counted before the request, so an attempt that never returns (a
        // dropped future, a hung connection the caller's deadline cancels) is
        // still an attempt the endpoint may have seen and billed. `calls` uses
        // the opposite rule by design: it counts completions, because a
        // cancelled call has no outcome to classify.
        match classify(req.method().as_str(), req.uri().query()) {
            Some(op) => self.metrics.record_attempt(op),
            None => self.metrics.record_unclassified_attempt(),
        }
        self.inner.execute(req).await
    }
}

/// Value of query parameter `name`, or `None` when absent. Values here
/// (`list-type`, `uploadId`, `delimiter`) are plain or percent-encoded ASCII
/// and only presence and emptiness are tested, so no decoding is needed.
fn query_value<'a>(query: Option<&'a str>, name: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then_some(value)
    })
}

/// The [`StoreOp`] an S3 request belongs to, from its method and query.
///
/// Multipart requests (`?uploads`, `?uploadId=...`) all count as [`StoreOp::Put`]
/// including the `DELETE` that aborts an upload: they are the write path's
/// requests, and attributing the abort to [`StoreOp::Delete`] would inflate a
/// counter callers read as "objects I asked to delete".
pub(crate) fn classify(method: &str, query: Option<&str>) -> Option<StoreOp> {
    if query_value(query, "uploads").is_some() || query_value(query, "uploadId").is_some() {
        return Some(StoreOp::Put);
    }
    match method {
        "GET" if query_value(query, "list-type").is_some() => {
            // `list_with_delimiter` is the only caller that sets a delimiter,
            // and it always sets a non-empty one.
            match query_value(query, "delimiter") {
                Some(delimiter) if !delimiter.is_empty() => Some(StoreOp::ListDelimited),
                _ => Some(StoreOp::List),
            }
        }
        "GET" => Some(StoreOp::Get),
        "HEAD" => Some(StoreOp::Head),
        "PUT" => Some(StoreOp::Put),
        // `POST ?delete` is `object_store`'s bulk delete. Nothing in this
        // adapter issues one today; classifying it keeps a future switch to
        // `delete_stream` from landing in the unclassified slot.
        "POST" if query_value(query, "delete").is_some() => Some(StoreOp::Delete),
        "DELETE" => Some(StoreOp::Delete),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The mapping, request shape by request shape, against the query strings
    /// `object_store`'s S3 client actually builds (`aws/client.rs`: `uploads`
    /// with an empty value, `uploadId`, `list-type=2`, `delimiter=%2F`).
    #[test]
    fn every_request_shape_the_adapter_issues_maps_to_its_op() {
        let cases = [
            ("GET", None, Some(StoreOp::Get)),
            ("GET", Some("x-id=GetObject"), Some(StoreOp::Get)),
            ("HEAD", None, Some(StoreOp::Head)),
            ("PUT", None, Some(StoreOp::Put)),
            ("DELETE", None, Some(StoreOp::Delete)),
            (
                "GET",
                Some("list-type=2&prefix=tenant%2F"),
                Some(StoreOp::List),
            ),
            (
                "GET",
                Some("continuation-token=abc&list-type=2"),
                Some(StoreOp::List),
            ),
            (
                "GET",
                Some("prefix=tenant%2F&delimiter=%2F&list-type=2"),
                Some(StoreOp::ListDelimited),
            ),
            ("POST", Some("uploads="), Some(StoreOp::Put)),
            ("PUT", Some("partNumber=3&uploadId=u1"), Some(StoreOp::Put)),
            ("POST", Some("uploadId=u1"), Some(StoreOp::Put)),
            ("DELETE", Some("uploadId=u1"), Some(StoreOp::Put)),
            ("POST", Some("delete="), Some(StoreOp::Delete)),
        ];
        for (method, query, expected) in cases {
            assert_eq!(
                classify(method, query),
                expected,
                "{method} {query:?} misclassified"
            );
        }
    }

    /// An empty `delimiter=` must not read as delimited: the split between
    /// `list` and `list_delimited` is what makes their `calls` blocks
    /// comparable with their attempt counts.
    #[test]
    fn an_empty_delimiter_is_a_plain_list() {
        assert_eq!(
            classify("GET", Some("list-type=2&delimiter=")),
            Some(StoreOp::List)
        );
    }

    /// A prefix or suffix of a parameter name is not that parameter. Without
    /// exact-name matching, `?x-uploads=1` would count as a multipart put.
    #[test]
    fn parameter_names_match_exactly() {
        assert_eq!(classify("GET", Some("not-uploads=1")), Some(StoreOp::Get));
        assert_eq!(classify("GET", Some("uploadIdentity=1")), Some(StoreOp::Get));
        assert_eq!(query_value(Some("a=1&ab=2"), "ab"), Some("2"));
        assert_eq!(query_value(Some("a=1"), "b"), None);
        assert_eq!(query_value(None, "a"), None);
    }

    /// A method this adapter never issues is not silently attributed to some
    /// operation it resembles; the caller counts it as unclassified instead.
    #[test]
    fn an_unknown_method_is_unclassified() {
        assert_eq!(classify("PATCH", None), None);
        assert_eq!(classify("POST", None), None);
    }
}
