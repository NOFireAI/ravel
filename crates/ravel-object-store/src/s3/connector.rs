//! The HTTP connector the S3 adapter installs below `object_store`'s retry
//! loop, and the three things it observes there: billed requests (issue #928,
//! ADR-0927 decision 8), the stored checksum a GET response carries
//! (ADR-1696 decisions 2 to 4), and the store's own clock from each response's
//! `Date` header (ADR-1685 decision 1).
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
//!
//! # Observing the store's clock
//!
//! A writer stamps its ingest-hour bucket from its own clock and has no second
//! time source to check it against (ADR-1685 context). Every S3 response
//! carries a `Date` header, and this connector is the layer that sees it:
//! `object_store` 0.14's `GetResult`/`PutResult` expose no response headers.
//! [`S3HttpService`] parses the header of every response it receives and stores
//! it in an [`ObservedStoreTime`] shared with the [`S3Store`] above it, which
//! reports it through `ObjectStoreBackend::observed_store_time_ns`.
//!
//! [`S3Store`]: crate::s3::S3Store

use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::HeaderMap;
use tokio::task::JoinSet;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

use crate::instrument::{StoreMetrics, StoreOp};
use crate::s3::checksum::{self, ObservedChecksum};
use crate::s3::http_date::parse_imf_fixdate_ns;

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

/// The store's own clock as the last response from it reported, in unix
/// nanoseconds (ADR-1685 decision 1).
///
/// Shared between the connector, which writes it below `object_store`'s retry
/// loop, and the [`S3Store`](crate::s3::S3Store) that reports it: one instance
/// per store, so two stores built against different endpoints observe
/// different clocks and a test's fake endpoint cannot be overwritten by an
/// unrelated store in the same process.
///
/// **The latest response wins, never a running maximum.** A maximum would
/// latch one bad header from a proxy for the life of the process; the latest
/// reading is wrong only until the next response arrives.
#[derive(Debug)]
pub(crate) struct ObservedStoreTime {
    /// The most recent parseable `Date`, or [`Self::UNSET`] before the first
    /// one. `Relaxed` throughout: this is a single standalone value that
    /// orders no other memory, and a reader that sees the previous
    /// observation for a moment reads a slightly staler lower bound, which is
    /// what a lower bound already tolerates.
    ns: AtomicI64,
}

impl ObservedStoreTime {
    /// No response has carried a parseable `Date` yet. A sentinel rather than
    /// `0`, which is a representable (if implausible) instant, so "unset" and
    /// "the epoch" stay distinguishable.
    const UNSET: i64 = i64::MIN;

    /// The latest observation, or `None` before the first response.
    pub(crate) fn latest(&self) -> Option<i64> {
        match self.ns.load(Ordering::Relaxed) {
            Self::UNSET => None,
            ns => Some(ns),
        }
    }

    /// Record what this response's `Date` said. Unconditional: the newest
    /// reading replaces whatever was there, older or newer.
    fn observe(&self, ns: i64) {
        self.ns.store(ns, Ordering::Relaxed);
    }
}

impl Default for ObservedStoreTime {
    fn default() -> Self {
        ObservedStoreTime {
            ns: AtomicI64::new(Self::UNSET),
        }
    }
}

/// An [`HttpConnector`] that wraps the default [`ReqwestConnector`], counts
/// every HTTP request the client it builds issues into a shared
/// [`StoreMetrics`] via [`StoreMetrics::record_attempt`], records each GET
/// response's stored checksum into the [`observe_get`] slot in scope, and
/// records every response's `Date` into the shared [`ObservedStoreTime`].
#[derive(Debug)]
pub(crate) struct S3HttpConnector {
    metrics: Arc<StoreMetrics>,
    store_time: Arc<ObservedStoreTime>,
    inner: ReqwestConnector,
}

impl S3HttpConnector {
    pub(crate) fn new(metrics: Arc<StoreMetrics>, store_time: Arc<ObservedStoreTime>) -> Self {
        S3HttpConnector {
            metrics,
            store_time,
            inner: ReqwestConnector::default(),
        }
    }

    /// An [`HttpClient`] that sends through `client` rather than through one
    /// built from `ClientOptions`, with the same per-attempt recording as the
    /// one [`HttpConnector::connect`] builds.
    pub(crate) fn wrap(&self, client: reqwest::Client) -> HttpClient {
        HttpClient::new(S3HttpService {
            metrics: Arc::clone(&self.metrics),
            store_time: Arc::clone(&self.store_time),
            inner: HttpClient::new(client),
        })
    }
}

/// Resolves a host to all of its addresses in random order, as the client
/// `object_store` builds does by default (`randomize_addresses`), so requests
/// spread over the addresses an S3 endpoint publishes instead of all trying
/// the first.
#[derive(Debug)]
pub(crate) struct ShuffleResolver;

impl Resolve for ShuffleResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            // A `JoinSet` drops the lookup if this future is dropped before it
            // starts.
            let mut tasks = JoinSet::new();
            tasks.spawn_blocking(move || -> Result<Addrs, BoxError> {
                let mut addrs: Vec<SocketAddr> = (name.as_str(), 0).to_socket_addrs()?.collect();
                addrs.shuffle(&mut rand::rng());
                Ok(Box::new(addrs.into_iter()))
            });
            match tasks.join_next().await {
                Some(joined) => joined.map_err(|e| Box::new(e) as BoxError)?,
                None => Err("the address lookup task was not spawned".into()),
            }
        })
    }
}

impl HttpConnector for S3HttpConnector {
    /// Builds the reqwest client with no default headers. `object_store` puts
    /// `ClientOptions`' default headers on a request itself, before SigV4 signs
    /// it, on every path except LIST; a reqwest client built from the same
    /// options would add them again after signing, which leaves
    /// `x-amz-checksum-mode` unsigned on every LIST, and an S3 endpoint refuses
    /// a request carrying an unsigned `x-amz-*` header. Dropping them here
    /// leaves only the signed copies.
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let unsigned_defaults_removed = options.clone().with_default_headers(HeaderMap::new());
        let inner = self.inner.connect(&unsigned_defaults_removed)?;
        Ok(HttpClient::new(S3HttpService {
            metrics: Arc::clone(&self.metrics),
            store_time: Arc::clone(&self.store_time),
            inner,
        }))
    }
}

/// The [`HttpService`] [`S3HttpConnector`] builds: record one attempt for the
/// scoped [`StoreOp`], delegate the request unchanged, then record what the
/// response said about the object's stored checksum and about the store's
/// clock. Delegation is byte-for-byte the default reqwest path, so no
/// observation adds behaviour to the request itself.
#[derive(Debug)]
struct S3HttpService {
    metrics: Arc<StoreMetrics>,
    store_time: Arc<ObservedStoreTime>,
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
        if let Ok(response) = &response {
            if let Ok(slot) = GET_OBSERVATION.try_with(Arc::clone)
                && let Some(observation) = observation_of(response)
            {
                *slot.lock() = Some(observation);
            }
            // Every response, whatever its status: a 503 comes from the store
            // and its `Date` is as good a reading of the store's clock as a
            // 200's. A missing or unparseable header changes nothing.
            if let Some(ns) = response_date_ns(response.headers()) {
                self.store_time.observe(ns);
            }
        }
        response
    }
}

/// Unix nanoseconds from a response's `Date` header, or `None` when there is
/// no such header or it is not an IMF-fixdate this can read.
fn response_date_ns(headers: &HeaderMap) -> Option<i64> {
    parse_imf_fixdate_ns(headers.get("date")?.to_str().ok()?)
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

    /// The `Date` of one response, as the connector reads it off the header
    /// map. `headers` are `(name, value)` pairs.
    fn date_of(headers: &[(&str, &str)]) -> Option<i64> {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .expect("test header names are valid");
            let value = reqwest::header::HeaderValue::from_str(value)
                .expect("test header values are valid");
            map.insert(name, value);
        }
        response_date_ns(&map)
    }

    #[test]
    fn a_responses_date_header_is_read_as_unix_nanoseconds() {
        assert_eq!(
            date_of(&[("date", "Sun, 06 Nov 1994 08:49:37 GMT")]),
            Some(784_111_777_000_000_000)
        );
    }

    /// Header names are case-insensitive on the wire; S3 sends `Date`.
    #[test]
    fn the_date_header_is_matched_case_insensitively() {
        assert_eq!(
            date_of(&[("Date", "Sun, 06 Nov 1994 08:49:37 GMT")]),
            Some(784_111_777_000_000_000)
        );
    }

    #[test]
    fn a_missing_or_unparseable_date_yields_no_observation() {
        assert_eq!(date_of(&[("etag", "\"abc\"")]), None);
        assert_eq!(date_of(&[("date", "not-a-valid-date")]), None);
    }

    #[test]
    fn an_unset_observation_reads_none() {
        assert_eq!(ObservedStoreTime::default().latest(), None);
    }

    /// The latest response wins. An older `Date` after a newer one replaces
    /// it, because a maximum would latch one bad header from a proxy for the
    /// life of the process (ADR-1685 decision 1).
    #[test]
    fn a_later_observation_replaces_an_earlier_one_in_both_directions() {
        let observed = ObservedStoreTime::default();
        observed.observe(2_000);
        assert_eq!(observed.latest(), Some(2_000));
        observed.observe(1_000);
        assert_eq!(observed.latest(), Some(1_000));
        observed.observe(3_000);
        assert_eq!(observed.latest(), Some(3_000));
    }

    #[tokio::test]
    async fn the_shuffle_resolver_returns_every_address_of_a_host() {
        let Ok(name) = "localhost".parse::<Name>() else {
            panic!("localhost is a valid host name");
        };
        let addrs: Vec<SocketAddr> = ShuffleResolver
            .resolve(name)
            .await
            .expect("localhost resolves")
            .collect();
        let mut expected: Vec<SocketAddr> = ("localhost", 0)
            .to_socket_addrs()
            .expect("localhost resolves")
            .collect();
        let mut sorted = addrs.clone();
        sorted.sort();
        expected.sort();
        assert!(!sorted.is_empty());
        assert_eq!(sorted, expected);
    }

    /// The epoch is a real reading, not the "no observation yet" sentinel.
    #[test]
    fn an_observation_of_zero_is_an_observation() {
        let observed = ObservedStoreTime::default();
        observed.observe(0);
        assert_eq!(observed.latest(), Some(0));
    }
}
