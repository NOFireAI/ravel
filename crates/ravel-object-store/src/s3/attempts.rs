//! Billed-request (attempt) counting at the HTTP layer, below
//! `object_store`'s retry loop (issue #928, ADR-0927 decision 8).
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
//! change to retry behaviour: [`AttemptCountingService`] records, then
//! delegates the request unchanged.
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
//! logical op; the counting service reads it. `object_store`'s retry loop and
//! the default [`ReqwestConnector`] both run the request inline on the task
//! that awaited the op (no [`SpawnedReqwestConnector`]), so the task-local is in
//! scope for every attempt, including a whole-object read's concurrent ranged
//! GETs. A request issued with no scope in effect (a credential-provider
//! refresh, say) simply records nothing --- it is not an S3 data request.
//!
//! [`SpawnedReqwestConnector`]: object_store::client::SpawnedReqwestConnector

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};

use crate::instrument::{StoreMetrics, StoreOp};

tokio::task_local! {
    /// The [`StoreOp`] the S3 adapter is currently executing, read by
    /// [`AttemptCountingService`] to attribute each billed HTTP request. Unset
    /// outside a [`scope`].
    static CURRENT_OP: StoreOp;
}

/// Run `fut` with `op` installed as the current operation, so every HTTP
/// request `object_store` issues while polling it is counted under `op`. A
/// no-op on attribution when no [`AttemptCountingConnector`] is installed (the
/// task-local is set either way, but nothing reads it), so the S3 adapter can
/// scope unconditionally.
pub(crate) async fn scope<F: Future>(op: StoreOp, fut: F) -> F::Output {
    CURRENT_OP.scope(op, fut).await
}

/// An [`HttpConnector`] that wraps the default [`ReqwestConnector`] and counts
/// every HTTP request the client it builds issues, into a shared
/// [`StoreMetrics`] via [`StoreMetrics::record_attempt`].
#[derive(Debug)]
pub(crate) struct AttemptCountingConnector {
    metrics: Arc<StoreMetrics>,
    inner: ReqwestConnector,
}

impl AttemptCountingConnector {
    pub(crate) fn new(metrics: Arc<StoreMetrics>) -> Self {
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

/// The [`HttpService`] [`AttemptCountingConnector`] builds: record one attempt
/// for the scoped [`StoreOp`], then delegate the request unchanged. Delegation
/// is byte-for-byte the default reqwest path, so counting adds no behaviour.
#[derive(Debug)]
struct AttemptCountingService {
    metrics: Arc<StoreMetrics>,
    inner: HttpClient,
}

#[async_trait]
impl HttpService for AttemptCountingService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Count before dispatch: the request is about to go on the wire and be
        // billed whether or not the response ever arrives. A request issued
        // outside any `scope` (e.g. a credential refresh) records nothing.
        if let Ok(op) = CURRENT_OP.try_with(|op| *op) {
            self.metrics.record_attempt(op);
        }
        self.inner.execute(req).await
    }
}
