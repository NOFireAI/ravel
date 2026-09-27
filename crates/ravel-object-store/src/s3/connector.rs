//! The HTTP connector the S3 adapter installs below `object_store`'s retry
//! loop, and the two things it observes there: billed requests (issue #928,
//! ADR-0927 decision 8) and the stored checksum a GET response carries
//! (ADR-1696 decisions 2 to 4).
//!
//! `object_store` runs its own retry loop *inside* each logical S3 operation
//! (`RetryConfig`, default `max_retries = 10`), so one `get()` that retried
//! nine times is a single completed call at the [`ObjectStoreBackend`] boundary
//! while the provider bills ten HTTP requests. The
//! [`InstrumentedStore`](crate::InstrumentedStore) decorator sits above that
//! boundary and counts completions (`calls`), so it cannot see the retries: the
//! divergence is one-directional and, under throttling, unbounded.
//!
//! The retries *are* observable, without forking the dependency, from the one
//! layer they all pass through: `object_store`'s retry loop dispatches every
//! attempt through its [`HttpClient`], whose backing [`HttpService`] is
//! swappable via `AmazonS3Builder::with_http_connector`. Each `HttpService`
//! `call()` is exactly one HTTP request on the wire --- one billed request ---
//! so wrapping the connector counts attempts including every retry, with zero
//! change to retry behaviour: [`S3HttpService`] records, then delegates the
//! request unchanged.
//!
//! [`ObjectStoreBackend`]: crate::ObjectStoreBackend
//!
//! # Attributing an attempt to its operation
//!
//! An `HttpService::call` sees an [`HttpRequest`] (method, URI), not the
//! [`StoreOp`] that issued it. Classifying by HTTP verb is lossy (an S3 `LIST`
//! is a `GET`, an `UploadPart` is a `PUT`) and would re-implement request
//! shapes `object_store` owns. Instead the S3 adapter names the operation at
//! its own call site with [`scope`], a `tokio` task-local set around each
//! logical op; the connector reads it. `object_store`'s retry loop and the
//! default [`ReqwestConnector`] both run the request inline on the task that
//! awaited the op (no [`SpawnedReqwestConnector`]), so the task-local is in
//! scope for every attempt, including a whole-object read's concurrent ranged
//! GETs. A request issued with no scope in effect (a credential-provider
//! refresh, say) simply records nothing --- it is not an S3 data request.
//!
//! [`SpawnedReqwestConnector`]: object_store::client::SpawnedReqwestConnector
//!
//! # Observing a GET's stored checksum
//!
//! `object_store` 0.14 exposes no response headers: `GetResult` carries
//! `payload`, `meta`, `range`, and `attributes`, none of which can hold
//! `x-amz-checksum-crc64nvme`. This connector is the only layer that sees the
//! header and the body's identity together, which is why ADR-1696 puts the
//! read-side check here. [`observe_get`] installs a per-request slot the
//! connector writes the response's checksum header and whole-object status
//! into; the adapter's `get_one` reads the slot back and, for a full-object
//! read, recomputes the digest over the bytes it assembled. The slot is scoped
//! to one `get_one` future, so the concurrently-polled ranged GETs of a split
//! whole-object read each observe their own response rather than sharing one
//! cell.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use parking_lot::Mutex;

use crate::instrument::{StoreMetrics, StoreOp};
use crate::s3::checksum::{self, ObservedChecksum};

tokio::task_local! {
    /// The [`StoreOp`] the S3 adapter is currently executing, read by
    /// [`S3HttpService`] to attribute each billed HTTP request. Unset outside a
    /// [`scope`].
    static CURRENT_OP: StoreOp;

    /// Where [`S3HttpService`] leaves what it saw on the current GET's
    /// response. Unset outside an [`observe_get`], which is every request that
    /// is not a single object read.
    static GET_OBSERVATION: Arc<Mutex<Option<GetObservation>>>;
}

/// What one GET response said about the object's stored checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GetObservation {
    /// The `x-amz-checksum-*` header the response carried, if any.
    pub(crate) checksum: ObservedChecksum,
    /// Whether this response's body is the *entire* object: a 200, or a 206
    /// whose `Content-Range` spans byte 0 to the last byte. Only then can a
    /// whole-object checksum be recomputed from it (ADR-1696 decision 4).
    pub(crate) whole_object: bool,
}

/// Run `fut` with `op` installed as the current operation, so every HTTP
/// request `object_store` issues while polling it is counted under `op`.
pub(crate) async fn scope<F: Future>(op: StoreOp, fut: F) -> F::Output {
    CURRENT_OP.scope(op, fut).await
}

/// Run `fut` with a fresh observation slot installed, returning its output
/// alongside whatever the connector saw on the last HTTP response inside it.
///
/// "Last" is deliberate: `object_store`'s retry loop can issue several attempts
/// inside one `fut`, and the bytes the caller ends up reading come from the
/// attempt that succeeded, which is the last one to write the slot.
pub(crate) async fn observe_get<F: Future>(fut: F) -> (F::Output, Option<GetObservation>) {
    let slot: Arc<Mutex<Option<GetObservation>>> = Arc::new(Mutex::new(None));
    let output = GET_OBSERVATION.scope(Arc::clone(&slot), fut).await;
    let observed = slot.lock().take();
    (output, observed)
}

/// An [`HttpConnector`] that wraps the default [`ReqwestConnector`], counts
/// every HTTP request the client it builds issues into a shared
/// [`StoreMetrics`] via [`StoreMetrics::record_attempt`], and records each GET
/// response's stored checksum into the [`observe_get`] slot in scope.
#[derive(Debug)]
pub(crate) struct S3HttpConnector {
    metrics: Arc<StoreMetrics>,
    inner: ReqwestConnector,
}

impl S3HttpConnector {
    pub(crate) fn new(metrics: Arc<StoreMetrics>) -> Self {
        S3HttpConnector {
            metrics,
            inner: ReqwestConnector::default(),
        }
    }
}

impl HttpConnector for S3HttpConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.inner.connect(options)?;
        Ok(HttpClient::new(S3HttpService {
            metrics: Arc::clone(&self.metrics),
            inner,
        }))
    }
}

/// The [`HttpService`] [`S3HttpConnector`] builds: record one attempt for the
/// scoped [`StoreOp`], delegate the request unchanged, then record what the
/// response said about the object's stored checksum. Delegation is
/// byte-for-byte the default reqwest path, so neither observation adds
/// behaviour to the request itself.
#[derive(Debug)]
struct S3HttpService {
    metrics: Arc<StoreMetrics>,
    inner: HttpClient,
}

#[async_trait]
impl HttpService for S3HttpService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Count before dispatch: the request is about to go on the wire and be
        // billed whether or not the response ever arrives. A request issued
        // outside any `scope` (e.g. a credential refresh) records nothing.
        if let Ok(op) = CURRENT_OP.try_with(|op| *op) {
            self.metrics.record_attempt(op);
        }
        let response = self.inner.execute(req).await;
        if let Ok(response) = &response
            && let Ok(slot) = GET_OBSERVATION.try_with(Arc::clone)
            && let Some(observation) = observation_of(response)
        {
            *slot.lock() = Some(observation);
        }
        response
    }
}

/// What a response says about the object's stored checksum, or `None` for a
/// response that carries no body worth checking (an error status, or a
/// redirect). A non-2xx response is left alone so a scripted 503 does not
/// overwrite the observation of the retry that succeeded.
fn observation_of(response: &HttpResponse) -> Option<GetObservation> {
    if !response.status().is_success() {
        return None;
    }
    let headers = response.headers();
    let whole_object = match headers.get("content-range") {
        // No `Content-Range`: a 200, whose body is the whole object.
        None => true,
        Some(value) => value
            .to_str()
            .ok()
            .and_then(content_range_covers_object)
            .unwrap_or(false),
    };
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .filter_map(|(name, value)| Some((name.as_str(), value.to_str().ok()?)))
        .collect();
    Some(GetObservation {
        checksum: checksum::observe(pairs),
        whole_object,
    })
}

/// Whether `bytes {first}-{last}/{total}` spans the whole object. An
/// unparseable or `*`-sized `Content-Range` answers `false`: unknown coverage
/// is not whole-object coverage.
fn content_range_covers_object(value: &str) -> Option<bool> {
    let spec = value.trim().strip_prefix("bytes ")?;
    let (range, total) = spec.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    let first: u64 = first.trim().parse().ok()?;
    let last: u64 = last.trim().parse().ok()?;
    let total: u64 = total.trim().parse().ok()?;
    Some(first == 0 && last.checked_add(1) == Some(total))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_content_range_spanning_the_object_is_whole_object() {
        assert_eq!(content_range_covers_object("bytes 0-99/100"), Some(true));
    }

    #[test]
    fn a_partial_content_range_is_not_whole_object() {
        assert_eq!(content_range_covers_object("bytes 0-49/100"), Some(false));
        assert_eq!(content_range_covers_object("bytes 50-99/100"), Some(false));
    }

    /// An unknown total (`*`) cannot be shown to cover the object, so it does
    /// not: the whole point of the flag is that a verified read really did
    /// receive every byte the checksum was computed over.
    #[test]
    fn an_unknown_total_is_not_whole_object() {
        assert_eq!(content_range_covers_object("bytes 0-99/*"), None);
        assert_eq!(content_range_covers_object("not a range"), None);
    }
}
