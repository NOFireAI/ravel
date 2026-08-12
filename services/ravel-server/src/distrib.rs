//! ADR-0071 distributed read fan-out server wiring (issue #865).
//!
//! This module turns the `ravel-query` distribution primitives (issue #864)
//! into a running cluster surface. It has three parts:
//!
//! * [`FragmentService`] -- the worker side. It implements the generated
//!   `SeriesFetch` gRPC service, guarding every call with a shared
//!   cluster-internal bearer token and admitting it against a distinct
//!   [`FragmentAdmission`] class (never the client-query cap). Per request it
//!   resolves a full-window snapshot for the request's tenant, builds an interim
//!   content-hash [`SnapshotSegmentResolver`], and delegates to the in-crate
//!   [`SeriesFetchService`] so a fragment fetch is byte-identical to what the
//!   local path would read.
//! * [`RoutingSliceFetcher`] -- the coordinator side. It implements the
//!   [`SliceFetcher`] seam the engine dispatches each slice through. It
//!   rendezvous-maps a slice's `(tenant_hash, signal, shard)` unit onto the live
//!   query-worker set: a slice this process owns runs locally with no network
//!   hop (through the same [`FragmentService`] the gRPC surface exposes), and a
//!   slice another worker owns is dispatched over an authed `tonic` channel.
//!
//!   The ADR-0071 failure matrix is enforced here (deliverable 1, 3, 4). A
//!   version-skewed worker is dropped at routing time, so a protocol mismatch
//!   costs no round trip (subsumes issue #885 item 3). A first remote attempt
//!   lost at transport or answered `Unavailable` is re-dispatched exactly once
//!   to the next rendezvous worker, then executed coordinator-local; a typed
//!   failure surfaces only if local execution also fails. A worker-reported
//!   corruption, or any decode/framing fault, is terminal and typed straight
//!   through: it is never retried and never masked by a local fallback.
//! * [`spawn_heartbeat`] -- the membership loop. It writes this process's
//!   `sys/query/workers/<uuid>` heartbeat every interval and refreshes the
//!   shared live-worker set the router reads.
//!
//! # Observability
//!
//! ADR-0071 defines two observability surfaces, both delivered here. The
//! `ravel_distrib_*` metric family ([`FragmentMetrics`]) carries the
//! process-global, cardinality-safe counters (rendered under the closed
//! `{mode}` label alone). The per-slice `stats.fragments[]` detail
//! ([`FragmentStatEntry`], collected through a task-local [`FragmentStatsSink`])
//! is attached to a distributed query's stats JSON by the query handler: one
//! entry per dispatched slice, carrying its worker endpoint, segment count,
//! reported bytes, and outcome. It is absent when distribution is off, since no
//! fan-out records anything. The metric family and the stats field are
//! independent: per-slice cardinality lives only in the response body's
//! `fragments[]`, never as a metric label.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use parking_lot::{Mutex, RwLock};
use ravel_cache::Cache;
use ravel_catalog::Catalog;
use ravel_fleet::query_workers::{QueryWorkerRecord, QueryWorkers};
use ravel_fleet::worker_set;
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT};
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::CacheFetchError;
use ravel_query::SegmentFetcher;
use ravel_query::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use ravel_query::distrib::codec::{self, CodecError};
use ravel_query::distrib::proto::series_fetch_client::SeriesFetchClient;
use ravel_query::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use ravel_query::distrib::service::{SeriesFetchService, SnapshotSegmentResolver};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantHash, TimeRange};
use tonic::transport::Channel;
use uuid::Uuid;

/// The frozen full-time-window a worker resolves a snapshot over (see
/// [`FragmentService::build_resolver`]): the whole possible timestamp domain, so
/// the resolved snapshot is a superset of whatever pinned window the coordinator
/// dispatched from, and every shipped content hash resolves.
const FULL_WINDOW: TimeRange = TimeRange {
    start_ns: i64::MIN,
    end_ns: i64::MAX,
};

/// Bound on establishing a channel to a remote worker. A dead or unreachable
/// worker must never stall a query: the coordinator times out here and falls
/// back to local execution (ADR-0071 failure semantics), since it can always
/// read any slice itself. Without this bound a black-holed endpoint blocks on
/// the kernel's TCP SYN timeout (often over two minutes).
const REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The `ravel_distrib_*` metric family (ADR-0071, issue #865). Process-global
/// atomics, read at `/metrics` scrape time. Carries only the closed `mode`
/// label at render time; never a per-shard, per-worker, or per-tenant label
/// (ADR-0044 section 4).
#[derive(Debug)]
pub struct FragmentMetrics {
    /// Inbound fragment requests served after passing token auth and admission.
    fragment_requests_total: AtomicU64,
    /// Inbound fragment requests refused for a missing or invalid bearer token.
    fragment_auth_failures_total: AtomicU64,
    /// Fragment requests currently holding an admission permit (gauge).
    fragment_inflight: AtomicU64,
    /// Slices this coordinator executed locally (self-mapped, no network hop).
    slices_local_total: AtomicU64,
    /// Slices this coordinator dispatched to a remote worker successfully.
    slices_remote_total: AtomicU64,
    /// Slices whose first remote attempt was lost or `Unavailable` and were
    /// re-dispatched once to the next rendezvous worker (ADR-0071 deliverable
    /// 1). Counted once per slice that entered re-dispatch, whether the next
    /// worker then succeeded or the slice went on to fall back local.
    slices_redispatched_total: AtomicU64,
    /// Slices whose remote dispatch failed at transport and fell back to local.
    slices_fallback_total: AtomicU64,
    /// Per-slice fetch latency, bucketed like the store-latency histogram.
    slice_fetch_micros_buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    /// Sum of per-slice fetch latencies, in nanoseconds, for the `_sum` series.
    slice_fetch_nanos_total: AtomicU64,
}

impl Default for FragmentMetrics {
    fn default() -> Self {
        FragmentMetrics {
            fragment_requests_total: AtomicU64::new(0),
            fragment_auth_failures_total: AtomicU64::new(0),
            fragment_inflight: AtomicU64::new(0),
            slices_local_total: AtomicU64::new(0),
            slices_remote_total: AtomicU64::new(0),
            slices_redispatched_total: AtomicU64::new(0),
            slices_fallback_total: AtomicU64::new(0),
            slice_fetch_micros_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            slice_fetch_nanos_total: AtomicU64::new(0),
        }
    }
}

impl FragmentMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn record_fragment_request(&self) {
        self.fragment_requests_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_fragment_auth_failure(&self) {
        self.fragment_auth_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn inc_inflight(&self) {
        self.fragment_inflight.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_inflight(&self) {
        // Saturating: an underflow would only happen on a double-drop, which the
        // permit guard's ownership prevents, but clamp rather than wrap.
        let _ = self
            .fragment_inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    fn record_slice_local(&self) {
        self.slices_local_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_remote(&self) {
        self.slices_remote_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_redispatched(&self) {
        self.slices_redispatched_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_slice_fallback(&self) {
        self.slices_fallback_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one completed slice fetch's latency into the histogram.
    pub fn observe_slice_fetch(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.slice_fetch_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        let index = latency_bucket(nanos);
        self.slice_fetch_micros_buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn fragment_requests_total(&self) -> u64 {
        self.fragment_requests_total.load(Ordering::Relaxed)
    }

    pub fn fragment_auth_failures_total(&self) -> u64 {
        self.fragment_auth_failures_total.load(Ordering::Relaxed)
    }

    pub fn fragment_inflight(&self) -> u64 {
        self.fragment_inflight.load(Ordering::Relaxed)
    }

    pub fn slices_local_total(&self) -> u64 {
        self.slices_local_total.load(Ordering::Relaxed)
    }

    pub fn slices_remote_total(&self) -> u64 {
        self.slices_remote_total.load(Ordering::Relaxed)
    }

    pub fn slices_redispatched_total(&self) -> u64 {
        self.slices_redispatched_total.load(Ordering::Relaxed)
    }

    pub fn slices_fallback_total(&self) -> u64 {
        self.slices_fallback_total.load(Ordering::Relaxed)
    }

    /// The per-bucket (non-cumulative) slice-fetch latency counts, for the
    /// renderer to turn into cumulative Prometheus buckets.
    pub fn slice_fetch_buckets(&self) -> [u64; LATENCY_BUCKET_COUNT] {
        std::array::from_fn(|i| self.slice_fetch_micros_buckets[i].load(Ordering::Relaxed))
    }

    pub fn slice_fetch_nanos_total(&self) -> u64 {
        self.slice_fetch_nanos_total.load(Ordering::Relaxed)
    }
}

/// The bucket a duration falls in, replicating
/// `ravel_object_store::instrument`'s private bucketing over the same public
/// bounds so the `ravel_distrib_slice_fetch_seconds` histogram shares the store
/// histogram's bucket layout.
fn latency_bucket(elapsed_nanos: u64) -> usize {
    let micros = elapsed_nanos / 1_000;
    LATENCY_BUCKET_BOUNDS_MICROS
        .iter()
        .position(|bound| micros <= *bound)
        .unwrap_or(LATENCY_BUCKET_COUNT - 1)
}

/// Constant-time byte-slice equality for the bearer-token check, so a rejected
/// request cannot learn how many leading bytes of the token it guessed from the
/// comparison's timing. Lengths are compared first (a token's length is not the
/// secret); equal-length inputs are compared without an early exit.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The distinct internal-workload admission class for inbound fragment fetches
/// (ADR-0071 deliverable 2, issue #865). A plain counting semaphore, separate
/// from the client-query admission controller: a coordinator that holds a
/// client-query permit while it waits on its dispatched fragments can never
/// deadlock behind client queries queued on the client cap, because the workers
/// serving those fragments admit them here, against this independent bound.
#[derive(Clone)]
pub struct FragmentAdmission {
    sem: Arc<tokio::sync::Semaphore>,
    metrics: Arc<FragmentMetrics>,
}

impl FragmentAdmission {
    /// A fragment admission class bounded by `max` concurrent fetches (clamped
    /// to at least 1).
    pub fn new(max: usize, metrics: Arc<FragmentMetrics>) -> Self {
        FragmentAdmission {
            sem: Arc::new(tokio::sync::Semaphore::new(max.max(1))),
            metrics,
        }
    }

    /// Acquire a fragment permit, queueing (never rejecting) when the class is
    /// saturated. `None` only if the semaphore was closed, which this process
    /// never does; the caller then maps it to an `Unavailable` status.
    async fn acquire(&self) -> Option<FragmentPermit> {
        let permit = self.sem.clone().acquire_owned().await.ok()?;
        self.metrics.inc_inflight();
        Some(FragmentPermit {
            _permit: permit,
            metrics: Arc::clone(&self.metrics),
        })
    }
}

/// Held for the duration of one admitted fragment fetch; releases the permit and
/// decrements the in-flight gauge on drop.
struct FragmentPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    metrics: Arc<FragmentMetrics>,
}

impl Drop for FragmentPermit {
    fn drop(&mut self) {
        self.metrics.dec_inflight();
    }
}

/// The worker-side fragment surface (ADR-0071 deliverables 1 and 2). Cheap to
/// clone (every field is an `Arc` or a small value): the gRPC server owns one
/// clone and the coordinator's [`RoutingSliceFetcher`] holds another for
/// no-hop local execution.
#[derive(Clone)]
pub struct FragmentService {
    inner: Arc<FragmentServiceInner>,
}

struct FragmentServiceInner {
    auth_token: String,
    /// The deployment's ordinary tenant resolver chain. Used only for
    /// cross-cluster federation (resolve-scope) requests, where a federating
    /// coordinator's credential is an ordinary tenant credential and the tenant
    /// is derived from it, never from the wire (ADR-0071 security, #868). The
    /// intra-cluster pinned path never consults it.
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: FragmentAdmission,
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    cache: Option<Arc<Cache<CacheFetchError>>>,
    clock: Arc<dyn Clock>,
    metrics: Arc<FragmentMetrics>,
}

/// The boxed frame stream the generated `SeriesFetch` server trait requires.
type FragmentStream =
    Pin<Box<dyn Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

impl FragmentService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        auth_token: String,
        tenant_resolver: Arc<dyn TenantResolver>,
        admission: FragmentAdmission,
        catalog: Arc<Catalog>,
        store: Arc<dyn ObjectStoreBackend>,
        cache: Option<Arc<Cache<CacheFetchError>>>,
        clock: Arc<dyn Clock>,
        metrics: Arc<FragmentMetrics>,
    ) -> Self {
        FragmentService {
            inner: Arc::new(FragmentServiceInner {
                auth_token,
                tenant_resolver,
                admission,
                catalog,
                store,
                cache,
                clock,
                metrics,
            }),
        }
    }

    /// Wrap this service in the generated gRPC server, ready to add to the
    /// cluster-internal `tonic` router. Only ever mounted on the gRPC listener,
    /// never the public HTTP or mTLS client listeners.
    pub fn into_server(&self) -> SeriesFetchServer<FragmentService> {
        SeriesFetchServer::new(self.clone())
    }

    /// Constant-time bearer-token check against the shared cluster-internal
    /// token. A missing header, a non-`Bearer` scheme, or an unequal token all
    /// refuse the request; the comparison is constant-time to avoid leaking the
    /// token through timing.
    fn check_auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> {
        let presented = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(str::trim);
        let ok = match presented {
            Some(token) => constant_time_eq(token.as_bytes(), self.inner.auth_token.as_bytes()),
            None => false,
        };
        if ok {
            Ok(())
        } else {
            self.inner.metrics.record_fragment_auth_failure();
            Err(tonic::Status::unauthenticated(
                "fragment request rejected: missing or invalid bearer token",
            ))
        }
    }

    /// Authenticate a cross-cluster federation (resolve-scope) request and
    /// derive its tenant from the presented credential, never from the wire.
    ///
    /// ADR-0071 (#868) requires the remote to treat a federating coordinator's
    /// credential as an ORDINARY TENANT CREDENTIAL: it runs the deployment's
    /// normal [`TenantResolver`] chain over the request metadata and takes the
    /// tenant from whatever that credential maps to. The cluster-internal
    /// fragment token is never accepted here (it is not in the tenant
    /// registry), and any `tenant_hash` a coordinator set on the wire is
    /// ignored. A federating cluster therefore reads only the tenant its own
    /// credential authorizes, never an arbitrary tenant it names.
    fn resolve_federation_tenant(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<TenantHash, tonic::Status> {
        crate::flight_auth::resolve_tenant(self.inner.tenant_resolver.as_ref(), metadata)
            .inspect_err(|_| self.inner.metrics.record_fragment_auth_failure())
    }

    /// Build the interim content-hash resolver for one request by resolving a
    /// full-window snapshot for the request's tenant and metrics signal. The
    /// full window is a superset of the coordinator's pinned window, so every
    /// pinned content hash resolves and the fetch reads exactly the pinned
    /// segments (byte-identical to the local path). A tenant we cannot decode,
    /// or a signal other than metrics, yields an empty resolver: the delegate
    /// service then returns the same typed status (`BadData`/`Unsupported`) it
    /// would for any such request, which the coordinator handles.
    async fn build_resolver(&self, request: &pb::FetchRequest) -> Arc<SnapshotSegmentResolver> {
        let Some(tenant_hash) = decode_tenant_hash(&request.tenant_hash) else {
            return Arc::new(SnapshotSegmentResolver::new(std::iter::empty()));
        };
        // Only metrics are distributed; for any other signal the delegate
        // returns Unsupported regardless of the resolver, so skip the resolve.
        if codec::signal_from_u32(request.signal) != Ok(Signal::Metrics) {
            return Arc::new(SnapshotSegmentResolver::new(std::iter::empty()));
        }
        let now_ns = self.inner.clock.now_ns();
        match self
            .inner
            .catalog
            .resolve(&tenant_hash, Signal::Metrics, FULL_WINDOW, &[], now_ns)
            .await
        {
            Ok(snapshot) => Arc::new(SnapshotSegmentResolver::new(snapshot.segments)),
            // A resolve failure leaves an empty resolver: the delegate maps the
            // unknown pinned segments to SnapshotInvalidated, and the
            // coordinator re-resolves and retries once, the same recovery a
            // genuinely vanished segment takes.
            Err(err) => {
                tracing::warn!(error = %err, "fragment snapshot resolve failed; returning empty resolver");
                Arc::new(SnapshotSegmentResolver::new(std::iter::empty()))
            }
        }
    }

    /// Rewrite a cross-cluster resolve-scope request into a pinned one over this
    /// cluster's own snapshot (ADR-0071 federation, issue #868).
    ///
    /// A federated coordinator ships matchers and a time window with an empty
    /// [`pb::fetch_request::Scope::Resolve`] scope; the remote resolves its OWN
    /// snapshot over that window, applies its OWN erasure, and enforces its own
    /// budgets. This is not a trust shortcut: by the time this runs, the gRPC
    /// handler has already overwritten `request.tenant_hash` with the tenant its
    /// own [`TenantResolver`] chain derived from the presented credential (see
    /// [`FragmentService::resolve_federation_tenant`]), never the value the
    /// coordinator put on the wire. Resolution then takes the same catalog path a
    /// local query on this cluster takes, so a federated fetch reads exactly what
    /// a local query for that credential's tenant would. We turn the resolve scope
    /// into the pinned scope the in-crate
    /// [`SeriesFetchService`] already fetches from, pinning every segment of the
    /// resolved snapshot and attaching the snapshot's own erasure predicates
    /// (the coordinator never sends erasure for a resolve scope, and never
    /// re-applies it: the remote is authoritative for its own erasure).
    ///
    /// Returns the rewritten request paired with a resolver over the same
    /// snapshot. On any decode or resolve failure the request is returned
    /// unchanged (still resolve scope) with an empty resolver, so the delegate
    /// yields `Unsupported` and the coordinator fails the query typed, never a
    /// silent empty result. A snapshot with no segments is a valid empty-OK
    /// answer (this cluster holds no data for the tenant in that window).
    async fn resolve_scope(
        &self,
        mut request: pb::FetchRequest,
    ) -> (pb::FetchRequest, Arc<SnapshotSegmentResolver>) {
        let empty = || Arc::new(SnapshotSegmentResolver::new(std::iter::empty()));
        let Some(tenant_hash) = decode_tenant_hash(&request.tenant_hash) else {
            return (request, empty());
        };
        // Only metrics are distributed; any other signal is left as resolve
        // scope, which the delegate maps to Unsupported.
        if codec::signal_from_u32(request.signal) != Ok(Signal::Metrics) {
            return (request, empty());
        }
        let window = TimeRange {
            start_ns: request.window_start_ns,
            end_ns: request.window_end_ns,
        };
        let now_ns = self.inner.clock.now_ns();
        let snapshot = match self
            .inner
            .catalog
            .resolve(&tenant_hash, Signal::Metrics, window, &[], now_ns)
            .await
        {
            Ok(snapshot) => snapshot,
            // Leave the scope as Resolve: the delegate returns Unsupported and
            // the coordinator fails the federated query typed rather than
            // treating this cluster as having contributed an empty result.
            Err(err) => {
                tracing::warn!(error = %err, "federated resolve-scope snapshot resolve failed; leaving resolve scope for typed fallback");
                return (request, empty());
            }
        };
        // The remote is authoritative for its own erasure: derive it from this
        // cluster's snapshot, not from anything the coordinator sent.
        let erasure = ravel_query::snapshot_erasure_predicates(&snapshot);
        let identities = snapshot
            .segments
            .iter()
            .map(codec::encode_segment_identity)
            .collect();
        request.scope = Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
            segments: identities,
        }));
        request.erasure = codec::encode_erasure(&erasure);
        let resolver = Arc::new(SnapshotSegmentResolver::new(snapshot.segments));
        (request, resolver)
    }

    /// Resolve the request's snapshot and run the slice through the in-crate
    /// [`SeriesFetchService`], collecting its frames. Shared by the gRPC handler
    /// (after auth and admission) and the coordinator's no-hop local path.
    async fn resolve_and_run(&self, request: pb::FetchRequest) -> Vec<pb::FetchResponse> {
        // A cross-cluster resolve scope is rewritten to a pinned scope over this
        // cluster's own snapshot; a pinned scope (intra-cluster) uses the
        // full-window content-hash resolver unchanged.
        let (request, resolver) = match &request.scope {
            Some(pb::fetch_request::Scope::Resolve(_)) => self.resolve_scope(request).await,
            _ => {
                let resolver = self.build_resolver(&request).await;
                (request, resolver)
            }
        };
        let mut fetcher = SegmentFetcher::new(self.inner.store.clone());
        if let Some(cache) = &self.inner.cache {
            fetcher = fetcher.with_cache(Arc::clone(cache));
        }
        let service = SeriesFetchService::new(fetcher, resolver);
        match service.fetch(tonic::Request::new(request)).await {
            Ok(response) => {
                let mut frames = Vec::new();
                let mut stream = response.into_inner();
                while let Some(frame) = stream.next().await {
                    // The in-crate service builds every frame eagerly and never
                    // yields a stream error; a defensive match keeps a future
                    // change from silently dropping data.
                    match frame {
                        Ok(frame) => frames.push(frame),
                        Err(status) => {
                            tracing::warn!(status = %status, "local fragment stream yielded an error frame");
                        }
                    }
                }
                frames
            }
            // The in-crate service's `fetch` is infallible (it maps every typed
            // failure into a summary frame), so this arm is unreachable in
            // practice; surface it as an empty result rather than panicking.
            Err(status) => {
                tracing::warn!(status = %status, "local fragment fetch returned a status");
                Vec::new()
            }
        }
    }

    /// Execute one slice in-process with no network hop, returning the same
    /// [`SliceResponse`] a remote fetch would. Skips token auth and fragment
    /// admission: this is the coordinator's own work under its client-query
    /// permit, not an inbound request from another coordinator.
    async fn run_local(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let frames = self.resolve_and_run(request).await;
        decode_frames(frames)
    }
}

#[tonic::async_trait]
impl SeriesFetch for FragmentService {
    type FetchStream = FragmentStream;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        let (metadata, _extensions, mut inner) = request.into_parts();
        // Two distinct trust models share this surface (ADR-0071 security):
        //  - Pinned scope (intra-cluster fan-out): the shared cluster-internal
        //    fragment token, with the tenant taken from the wire. Coordinator
        //    and worker are one trust domain, and the token conveys no
        //    privilege over the bucket's S3 credentials.
        //  - Resolve scope (cross-cluster federation): the presented credential
        //    is an ordinary tenant credential. The tenant is resolved from it
        //    through the normal resolver chain and OVERRIDES any wire
        //    tenant_hash, so a federating cluster reads only the tenant its
        //    credential authorizes. The fragment token is never accepted here.
        match &inner.scope {
            Some(pb::fetch_request::Scope::Resolve(_)) => {
                let tenant = self.resolve_federation_tenant(&metadata)?;
                inner.tenant_hash = tenant.0.to_vec();
            }
            _ => self.check_auth(&metadata)?,
        }
        let Some(_permit) = self.inner.admission.acquire().await else {
            return Err(tonic::Status::unavailable("fragment admission unavailable"));
        };
        self.inner.metrics.record_fragment_request();
        let frames = self.resolve_and_run(inner).await;
        // The permit (and its in-flight gauge decrement) is held across the
        // eager fetch above, the whole admission window, then released here
        // before the already-built frames replay as a stream.
        drop(_permit);
        let stream = futures::stream::iter(frames.into_iter().map(Ok));
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// One per-slice entry in a distributed query's `stats.fragments[]` (ADR-0071
/// observability deliverable, issue #865). [`RoutingSliceFetcher`] records one
/// for every slice a distributed query dispatches; the query handler renders
/// the collected entries into the stats JSON via [`crate::query::fragments_json`].
/// The family carries per-slice cardinality here, in the response body, never as
/// a metric label (ADR-0044 section 4).
#[derive(Debug, Clone)]
pub struct FragmentStatEntry {
    /// The worker the slice ran on: a remote worker's `host:port` fragment
    /// endpoint, or `"local"` for a self-mapped slice or one that fell back to
    /// local execution after a transport failure.
    pub worker_endpoint: String,
    /// Pinned segments the slice covered.
    pub segment_count: u64,
    /// Store bytes the slice's worker reported reading (its per-slice accounting
    /// `total_s3_bytes`); `0` when the slice ended in an error.
    pub bytes_reported: u64,
    /// The slice's outcome: `"ok"` (ran to completion, local or remote),
    /// `"fallback"` (remote dispatch failed at transport and the coordinator
    /// re-ran it locally), or `"error"` (the fetch returned a hard error).
    pub status: &'static str,
}

tokio::task_local! {
    /// Per-query fragment-stats sink, installed by a query handler around its
    /// engine call (see [`with_fragment_stats`]). Every [`RoutingSliceFetcher`]
    /// slice driven on the same task records into it; unset on any task no
    /// handler scoped (an inbound gRPC fetch, or a distribution-off path), where
    /// recording is a silent no-op.
    static FRAGMENT_STATS: FragmentStatsSink;
}

/// A per-query collector the coordinator installs in task-local storage so each
/// dispatched slice records a [`FragmentStatEntry`]. Cheap to clone (an `Arc`):
/// the handler keeps one clone to [`take`](Self::take) the entries after the
/// query resolves while the engine's fan-out records into another.
#[derive(Clone, Default)]
pub struct FragmentStatsSink {
    entries: Arc<Mutex<Vec<FragmentStatEntry>>>,
}

impl FragmentStatsSink {
    pub fn new() -> Self {
        Self::default()
    }

    fn record(&self, entry: FragmentStatEntry) {
        self.entries.lock().push(entry);
    }

    /// Drain the collected per-slice entries.
    pub fn take(&self) -> Vec<FragmentStatEntry> {
        std::mem::take(&mut self.entries.lock())
    }
}

/// Run `future` with `sink` installed as the task-local fragment-stats sink, so
/// every [`RoutingSliceFetcher`] slice the future drives on this task records
/// into `sink`. The caller keeps its own clone of `sink` to read the entries
/// once the future resolves. The engine's fan-out polls its slice futures inline
/// (`buffer_unordered`, never a detached `spawn`), so they share this task's
/// local and every slice is captured.
pub async fn with_fragment_stats<F>(sink: FragmentStatsSink, future: F) -> F::Output
where
    F: std::future::Future,
{
    FRAGMENT_STATS.scope(sink, future).await
}

/// Record one completed slice into the task-local [`FragmentStatsSink`], if a
/// query handler scoped one on this task; a no-op otherwise.
fn record_fragment_stat(
    result: &Result<SliceResponse, DistribError>,
    worker_endpoint: String,
    segment_count: u64,
    fell_back: bool,
) {
    let (bytes_reported, status) = match result {
        Ok(response) => (
            response.accounting.total_s3_bytes(),
            if fell_back { "fallback" } else { "ok" },
        ),
        Err(_) => (0, "error"),
    };
    let entry = FragmentStatEntry {
        worker_endpoint,
        segment_count,
        bytes_reported,
        status,
    };
    let _ = FRAGMENT_STATS.try_with(|sink| sink.record(entry));
}

/// The count of pinned segments a slice request carries, `0` for a request with
/// no pinned scope.
fn pinned_segment_count(request: &pb::FetchRequest) -> u64 {
    match &request.scope {
        Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments.len() as u64,
        _ => 0,
    }
}

/// One rendezvous-ranked owner of a slice's unit: either this coordinator
/// (execute locally, no hop) or a remote worker at a `host:port` fragment
/// endpoint. Produced in descending rendezvous rank by
/// [`RoutingSliceFetcher::ranked_owners`], version-skewed workers already
/// removed (ADR-0071: a protocol-version mismatch costs no round trip).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Owner {
    /// This process owns (or is the failover for) the slice: run it locally.
    SelfLocal,
    /// A remote worker owns the slice; dispatch to this fragment endpoint.
    Remote(String),
}

/// The classification of one remote dispatch attempt (ADR-0071 deliverable 1).
enum Attempt {
    /// A terminal outcome: a decoded response with a non-`Unavailable` status,
    /// or a non-transport error (a decode/framing/corruption fault). Return it
    /// as the slice's result; do not re-dispatch or fall back around it. Boxed
    /// because a `SliceResponse` is large relative to the `Retry` variant.
    Keep(Box<Result<SliceResponse, DistribError>>),
    /// Transport loss or an `Unavailable` summary. Re-dispatch may skip past
    /// this attempt to the next rendezvous worker, then coordinator-local.
    Retry,
}

/// The coordinator's [`SliceFetcher`] (ADR-0071 deliverable 3). Rendezvous-maps
/// each slice onto the live query-worker set and either runs it locally (no
/// hop) or dispatches it to the owning worker over an authed channel, with a
/// local fallback on transport failure.
pub struct RoutingSliceFetcher {
    /// This process's own worker id, set once the heartbeat handle is built.
    /// Until set, every slice routes local (safe: the coordinator can read any
    /// slice), so a query dispatched before the first heartbeat still succeeds.
    self_id: Arc<OnceLock<Uuid>>,
    /// The live worker set, refreshed by [`spawn_heartbeat`]. Starts empty.
    live_workers: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>,
    /// The shared cluster-internal bearer token presented on each dispatch.
    auth_token: String,
    /// The local fragment surface, used for self-mapped and fallback slices.
    local: FragmentService,
    /// Cached `tonic` channels, keyed by endpoint (a channel is cheap to clone
    /// but expensive to reconnect).
    channels: Mutex<HashMap<String, Channel>>,
    metrics: Arc<FragmentMetrics>,
}

impl RoutingSliceFetcher {
    pub fn new(
        self_id: Arc<OnceLock<Uuid>>,
        live_workers: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>,
        auth_token: String,
        local: FragmentService,
        metrics: Arc<FragmentMetrics>,
    ) -> Self {
        RoutingSliceFetcher {
            self_id,
            live_workers,
            auth_token,
            local,
            channels: Mutex::new(HashMap::new()),
            metrics,
        }
    }

    /// Rendezvous-rank the live worker set for a slice, top owner first.
    ///
    /// Only workers whose `protocol_version` equals the coordinator's are
    /// considered: a version-skewed worker is dropped here, at routing time, so
    /// the mismatch never costs a dispatch round trip (ADR-0071 failure
    /// semantics; subsumes issue #885 item 3). The ranking is produced by
    /// repeatedly asking [`worker_set::owner`] for the top owner of the
    /// remaining candidate set, so it matches the single-owner mapping the rest
    /// of the cluster computes, extended to a deterministic failover order.
    ///
    /// An empty result (no pinned scope, an undecodable unit, or no
    /// version-matched worker) means "run local": the caller executes the slice
    /// on the coordinator with no hop.
    fn ranked_owners(&self, request: &pb::FetchRequest) -> Vec<Owner> {
        let Some((tenant_hash, signal, shard)) = rendezvous_unit(request) else {
            return Vec::new();
        };
        let live = Arc::clone(&self.live_workers.read());
        // Version-matched records with a parseable process id, paired with the
        // id so ranking and endpoint lookup share one filtered view.
        let candidates: Vec<(Uuid, &QueryWorkerRecord)> = live
            .iter()
            .filter(|record| record.protocol_version == codec::PROTOCOL_VERSION)
            .filter_map(|record| {
                Uuid::parse_str(&record.process_id)
                    .ok()
                    .map(|id| (id, record))
            })
            .collect();
        let unit_key = worker_set::unit_key(&tenant_hash, signal, shard);
        let self_id = self.self_id.get().copied();
        let mut ids: Vec<Uuid> = candidates.iter().map(|(id, _)| *id).collect();
        let mut ranked = Vec::new();
        // Peel the top owner off the remaining candidate set until it empties,
        // giving every version-matched worker in descending rendezvous rank.
        while let Some(owner) = worker_set::owner(&unit_key, &ids) {
            if Some(owner) == self_id {
                ranked.push(Owner::SelfLocal);
            } else if let Some((_, record)) = candidates.iter().find(|(id, _)| *id == owner) {
                ranked.push(Owner::Remote(record.fragment_endpoint.clone()));
            }
            ids.retain(|id| *id != owner);
        }
        ranked
    }

    /// Attempt one remote dispatch and classify the outcome for re-dispatch
    /// (ADR-0071 deliverable 1). Transport loss and an `Unavailable` summary are
    /// [`Attempt::Retry`] (re-dispatchable); every other outcome, success or a
    /// hard decode/framing error, is [`Attempt::Keep`] and terminal.
    async fn try_remote(&self, endpoint: &str, request: &pb::FetchRequest) -> Attempt {
        match self.remote_fetch(endpoint, request.clone()).await {
            Ok(response) if response.status == pb::status::Code::Unavailable => {
                tracing::warn!(
                    %endpoint,
                    "remote slice reported Unavailable; re-dispatching to next worker"
                );
                Attempt::Retry
            }
            Ok(response) => Attempt::Keep(Box::new(Ok(response))),
            Err(DistribError::Transport(message)) => {
                tracing::warn!(
                    %endpoint,
                    error = %message,
                    "remote slice fetch failed at transport; re-dispatching to next worker"
                );
                Attempt::Retry
            }
            // A decode, framing, or worker-reported corruption error is a real
            // defect, not a routing miss: propagate it typed rather than mask it
            // with a retry or a local fallback (ADR-0071 deliverable 3).
            Err(other) => Attempt::Keep(Box::new(Err(other))),
        }
    }

    /// Get (or open and cache) a channel to a remote worker's fragment endpoint.
    async fn channel(&self, endpoint: &str) -> Result<Channel, DistribError> {
        if let Some(channel) = self.channels.lock().get(endpoint).cloned() {
            return Ok(channel);
        }
        let uri = format!("http://{endpoint}");
        let channel = Channel::from_shared(uri)
            .map_err(|e| {
                DistribError::Transport(format!("invalid worker endpoint {endpoint}: {e}"))
            })?
            .connect_timeout(REMOTE_CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|e| DistribError::Transport(format!("connect to {endpoint} failed: {e}")))?;
        self.channels
            .lock()
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Dispatch one slice to a remote worker over an authed channel and decode
    /// its frames.
    async fn remote_fetch(
        &self,
        endpoint: &str,
        request: pb::FetchRequest,
    ) -> Result<SliceResponse, DistribError> {
        let channel = self.channel(endpoint).await?;
        let mut tonic_request = tonic::Request::new(request);
        let value = format!("Bearer {}", self.auth_token)
            .parse()
            .map_err(|_| DistribError::Transport("invalid bearer token metadata".to_string()))?;
        tonic_request.metadata_mut().insert("authorization", value);
        let mut client = SeriesFetchClient::new(channel);
        let response = client
            .fetch(tonic_request)
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?;
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?
        {
            frames.push(frame);
        }
        decode_frames(frames)
    }
}

#[async_trait]
impl SliceFetcher for RoutingSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let start = Instant::now();
        let segment_count = pinned_segment_count(&request);
        let ranked = self.ranked_owners(&request);
        let (result, worker_endpoint, fell_back) = self.dispatch(ranked, request).await;
        self.metrics.observe_slice_fetch(start.elapsed());
        record_fragment_stat(&result, worker_endpoint, segment_count, fell_back);
        result
    }
}

impl RoutingSliceFetcher {
    /// Execute one slice against its rendezvous-ranked owners, applying the
    /// ADR-0071 failure sequence exactly (deliverable 1):
    ///
    /// * The top owner is this coordinator (or the unit is unroutable / has no
    ///   version-matched worker): run local, no hop, no fallback needed.
    /// * The top owner is remote: dispatch to it. On a terminal outcome
    ///   (success, or a hard decode/corruption error) return it. On transport
    ///   loss or an `Unavailable` summary, re-dispatch EXACTLY once to the next
    ///   rendezvous worker (skipping the failed one). If that next worker is
    ///   this coordinator, or is absent, or also fails re-dispatchably, execute
    ///   the slice coordinator-local. A typed failure surfaces only if local
    ///   execution fails too.
    ///
    /// Slice atomicity holds by construction: each attempt is decoded whole
    /// before it is returned, so partial frames from a failed attempt are
    /// discarded and never merged.
    ///
    /// Returns the slice result, the endpoint label for its `fragments[]` stats
    /// entry, and whether it fell back to local after a failed remote attempt.
    async fn dispatch(
        &self,
        ranked: Vec<Owner>,
        request: pb::FetchRequest,
    ) -> (Result<SliceResponse, DistribError>, String, bool) {
        let primary = match ranked.first() {
            // Self-mapped or unroutable: local, the normal no-hop path.
            None | Some(Owner::SelfLocal) => {
                self.metrics.record_slice_local();
                return (
                    self.local.run_local(request).await,
                    "local".to_string(),
                    false,
                );
            }
            Some(Owner::Remote(endpoint)) => endpoint.clone(),
        };

        // First remote attempt against the top owner.
        match self.try_remote(&primary, &request).await {
            Attempt::Keep(result) => {
                self.metrics.record_slice_remote();
                return (*result, primary, false);
            }
            Attempt::Retry => {}
        }

        // The primary was lost or Unavailable. Re-dispatch EXACTLY once to the
        // next rendezvous worker, skipping the failed primary. If the next
        // owner is this coordinator (ranked second), fall straight to local:
        // that is the coordinator-local step, not an extra remote hop.
        // The counter increments only when a second remote dispatch is
        // actually sent, so a 2-node cluster whose failover is the coordinator
        // itself records a fallback, not a phantom re-dispatch.
        if let Some(Owner::Remote(next)) = ranked.get(1)
            && *next != primary
        {
            self.metrics.record_slice_redispatched();
            if let Attempt::Keep(result) = self.try_remote(next, &request).await {
                self.metrics.record_slice_remote();
                return (*result, next.clone(), false);
            }
        }

        // Primary and its one re-dispatch both failed (or there was no next
        // remote worker): the coordinator reads the slice itself. Its store
        // access is the same, so a successful local read is byte-identical to
        // the remote result; only if local also fails does the slice fail typed.
        self.metrics.record_slice_fallback();
        (self.local.run_local(request).await, primary, true)
    }
}

/// The rendezvous unit `(tenant_hash, signal, shard)` for a slice: the tenant
/// and signal from the request, and the minimum shard across the slice's pinned
/// segments (deterministic when a slice spans several shards). `None` when the
/// request carries no pinned scope or an undecodable tenant/signal, in which
/// case the caller routes local.
fn rendezvous_unit(request: &pb::FetchRequest) -> Option<(TenantHash, Signal, u32)> {
    let tenant_hash = decode_tenant_hash(&request.tenant_hash)?;
    let signal = codec::signal_from_u32(request.signal).ok()?;
    let segments = match &request.scope {
        Some(pb::fetch_request::Scope::Pinned(pinned)) => &pinned.segments,
        _ => return None,
    };
    let shard = segments.iter().map(|s| s.shard).min()?;
    Some((tenant_hash, signal, shard))
}

/// Decode a 16-byte tenant hash, or `None` if the wire bytes are the wrong
/// length.
fn decode_tenant_hash(bytes: &[u8]) -> Option<TenantHash> {
    let arr: [u8; 16] = bytes.try_into().ok()?;
    Some(TenantHash(arr))
}

/// Decode a slice's frame sequence into a [`SliceResponse`], the same decode
/// [`ravel_query::distrib::client::RemoteSliceFetcher`] applies to a live gRPC
/// stream, shared here so the local and remote paths produce identical shapes.
fn decode_frames(frames: Vec<pb::FetchResponse>) -> Result<SliceResponse, DistribError> {
    let mut scalar = Vec::new();
    let mut summary: Option<pb::Summary> = None;
    for frame in frames {
        match frame.frame {
            Some(pb::fetch_response::Frame::Series(series)) => {
                scalar.extend(codec::decode_series_frame(series)?);
            }
            Some(pb::fetch_response::Frame::Hist(_)) => {
                return Err(DistribError::Codec(CodecError::EmptyFrame));
            }
            Some(pb::fetch_response::Frame::Summary(s)) => {
                if summary.is_some() {
                    return Err(DistribError::MultipleSummaries);
                }
                summary = Some(s);
            }
            None => return Err(DistribError::EmptyFrame),
        }
    }
    let summary = summary.ok_or(DistribError::NoSummary)?;
    let status = summary
        .status
        .ok_or(DistribError::Codec(CodecError::MissingStatus))?;
    let code = codec::decode_status_code(status.code)?;
    let accounting = summary
        .accounting
        .map(codec::decode_accounting)
        .unwrap_or_default();
    Ok(SliceResponse {
        scalar,
        accounting,
        stats: ravel_query::FetchStats {
            raw_f64_pages: summary.raw_f64_pages,
            raw_f64_bytes: summary.raw_f64_bytes,
        },
        series_returned: summary.series_returned,
        samples_returned: summary.samples_returned,
        status: code,
        status_message: status.message,
    })
}

/// Spawn the query-worker heartbeat loop (ADR-0071 deliverable 3). Writes this
/// process's `sys/query/workers/<uuid>` record immediately and then every
/// interval, and refreshes `live_workers` from the store on the same cadence so
/// the [`RoutingSliceFetcher`] always reads a recent membership view. The first
/// write/read happens before the first sleep, so membership converges promptly
/// after startup.
pub fn spawn_heartbeat(
    workers: Arc<QueryWorkers>,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    live_workers: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = workers.heartbeat_interval();
        loop {
            let now_ns = clock.now_ns();
            if let Err(err) = workers.write_heartbeat(store.as_ref(), now_ns).await {
                tracing::warn!(error = %err, "query worker heartbeat write failed");
            }
            match workers.live_set(store.as_ref(), now_ns).await {
                Ok(live) => *live_workers.write() = Arc::new(live),
                Err(err) => {
                    tracing::warn!(error = %err, "query worker live_set read failed; keeping prior membership")
                }
            }
            tokio::time::sleep(interval).await;
        }
    })
}

/// A [`SliceFetcher`] over one remote cluster's fragment `SeriesFetch` surface
/// (ADR-0071 cross-cluster federation, issue #868).
///
/// Unlike [`RoutingSliceFetcher`], which rendezvous-maps intra-cluster slices
/// across the local worker set, this always dials one fixed remote endpoint and
/// presents the OPERATOR credential configured for that remote. The calling
/// client's credential is never forwarded: it never crosses a cluster boundary,
/// so the remote only ever authenticates the operator principal. The remote
/// resolves its own snapshot over the request's window, and enforces its own
/// admission, limits, and erasure through its ordinary tenant-auth path; this
/// coordinator only decodes and folds the frames it returns, counting them
/// against its own budgets. The request carries a resolve scope (the coordinator
/// never pins another cluster's segments), which the remote rewrites to a pinned
/// fetch over its own snapshot (see [`FragmentService::resolve_scope`]).
pub struct FederationSliceFetcher {
    /// The remote's stable name, for error and warning context.
    cluster: String,
    /// A lazily-connecting channel to the remote's fragment endpoint. Lazy so a
    /// remote that is down at startup does not fail process start; it surfaces
    /// as an unavailable remote at query time (handled per its skip_unavailable).
    channel: Channel,
    /// The operator bearer token presented to the remote. This is the only
    /// principal the remote sees for a federated fetch.
    credential: String,
}

impl FederationSliceFetcher {
    /// Build a federation client for one resolved `--remote-cluster`. The
    /// channel connects lazily (see [`FederationSliceFetcher::channel`]).
    pub fn connect(config: &crate::config::RemoteClusterConfig) -> anyhow::Result<Self> {
        let scheme = if config.tls { "https" } else { "http" };
        let uri = format!("{scheme}://{}", config.endpoint);
        let mut endpoint = Channel::from_shared(uri)
            .map_err(|e| {
                anyhow::anyhow!(
                    "invalid --remote-cluster {} endpoint {}: {e}",
                    config.name,
                    config.endpoint
                )
            })?
            .connect_timeout(REMOTE_CONNECT_TIMEOUT);
        if config.tls {
            let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
            if let Some(ca_file) = &config.tls_ca_file {
                let pem = std::fs::read(ca_file).map_err(|e| {
                    anyhow::anyhow!(
                        "failed to read --remote-cluster {} tls-ca-file {}: {e}",
                        config.name,
                        ca_file.display()
                    )
                })?;
                tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem));
            }
            endpoint = endpoint.tls_config(tls).map_err(|e| {
                anyhow::anyhow!(
                    "failed to configure TLS for --remote-cluster {}: {e}",
                    config.name
                )
            })?;
        }
        Ok(FederationSliceFetcher {
            cluster: config.name.clone(),
            channel: endpoint.connect_lazy(),
            credential: config.credential.clone(),
        })
    }
}

#[async_trait]
impl SliceFetcher for FederationSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let mut tonic_request = tonic::Request::new(request);
        // Present the operator credential, never the calling client's: the
        // client credential never crosses a cluster boundary (ADR-0071 trust
        // boundary, issue #868).
        let value = format!("Bearer {}", self.credential).parse().map_err(|_| {
            DistribError::Transport("invalid federation bearer token metadata".to_string())
        })?;
        tonic_request.metadata_mut().insert("authorization", value);
        let mut client = SeriesFetchClient::new(self.channel.clone());
        let response = client.fetch(tonic_request).await.map_err(|s| {
            DistribError::Transport(format!("federated fetch to cluster {}: {s}", self.cluster))
        })?;
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream.message().await.map_err(|s| {
            DistribError::Transport(format!("federated fetch to cluster {}: {s}", self.cluster))
        })? {
            frames.push(frame);
        }
        // A remote's frames get the same decode validation as an intra-cluster
        // slice's (ADR-0071 trust boundary): a malformed frame is a typed
        // DistribError, never a panic and never silently dropped data.
        decode_frames(frames)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_catalog::CatalogConfig;
    use ravel_ingest::SystemClock;
    use ravel_object_store::memory::MemoryStore;

    fn metrics_signal() -> u32 {
        codec::signal_to_u32(Signal::Metrics)
    }

    /// A minimal pinned `FetchRequest` for `tenant_hash`, with one segment per
    /// shard in `shards` (each with a distinct content hash), enough to drive
    /// routing and the local no-hop fetch.
    fn pinned_request(tenant_hash: [u8; 16], shards: &[u32]) -> pb::FetchRequest {
        let segments = shards
            .iter()
            .enumerate()
            .map(|(i, &shard)| pb::SegmentIdentity {
                shard,
                content_hash: vec![i as u8; 32],
                ..Default::default()
            })
            .collect();
        pb::FetchRequest {
            protocol_version: codec::PROTOCOL_VERSION,
            tenant_hash: tenant_hash.to_vec(),
            signal: metrics_signal(),
            scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                segments,
            })),
            ..Default::default()
        }
    }

    /// A resolver that never authenticates any credential: the intra-cluster
    /// pinned-scope tests only exercise the shared fragment token, never the
    /// tenant resolver, so an empty one keeps them focused.
    fn empty_resolver() -> Arc<dyn TenantResolver> {
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        ))
    }

    fn test_service(metrics: Arc<FragmentMetrics>) -> FragmentService {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        let admission = FragmentAdmission::new(8, metrics.clone());
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        FragmentService::new(
            "cluster-token".to_string(),
            empty_resolver(),
            admission,
            catalog,
            store,
            None,
            clock,
            metrics,
        )
    }

    #[test]
    fn constant_time_eq_matches_only_equal_slices() {
        assert!(constant_time_eq(b"abc123", b"abc123"));
        assert!(!constant_time_eq(b"abc123", b"abc124"));
        assert!(!constant_time_eq(b"abc123", b"abc12"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn rendezvous_unit_uses_min_shard() {
        let tenant = [7u8; 16];
        let request = pinned_request(tenant, &[3, 1, 2]);
        let (got_tenant, signal, shard) =
            rendezvous_unit(&request).expect("pinned request yields a unit");
        assert_eq!(got_tenant, TenantHash(tenant));
        assert_eq!(signal, Signal::Metrics);
        assert_eq!(shard, 1, "the minimum shard across the slice's segments");
    }

    #[test]
    fn rendezvous_unit_none_without_pinned_scope() {
        let request = pb::FetchRequest {
            tenant_hash: [1u8; 16].to_vec(),
            signal: metrics_signal(),
            scope: None,
            ..Default::default()
        };
        assert!(rendezvous_unit(&request).is_none());
    }

    #[test]
    fn rendezvous_unit_none_on_bad_tenant_hash() {
        let request = pinned_request([0u8; 16], &[0]);
        let bad = pb::FetchRequest {
            tenant_hash: vec![1, 2, 3],
            ..request
        };
        assert!(rendezvous_unit(&bad).is_none());
    }

    /// The fragment admission class bounds concurrency and, crucially, keeps
    /// making progress: a queued acquire completes as soon as an outstanding
    /// permit drops. This is the no-deadlock property (ADR-0071 deliverable 2)
    /// at the class level; the client-query admission cap is a separate
    /// semaphore, so a saturated client cap can never starve fragment fetches.
    #[tokio::test]
    async fn fragment_admission_bounds_and_releases() {
        let metrics = Arc::new(FragmentMetrics::new());
        let admission = FragmentAdmission::new(1, metrics.clone());

        let first = admission.acquire().await.expect("first permit");
        assert_eq!(metrics.fragment_inflight(), 1);

        let waiter = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.acquire().await.map(|_permit| ()) })
        };
        // The class is full, so the waiter cannot have acquired yet.
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "second acquire must queue while full"
        );

        // Releasing the outstanding permit lets the waiter through: progress,
        // no deadlock.
        drop(first);
        waiter
            .await
            .expect("waiter task joins")
            .expect("second permit granted after release");
        assert_eq!(metrics.fragment_inflight(), 0);
    }

    #[tokio::test]
    async fn missing_or_wrong_token_is_rejected_valid_token_passes() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());

        let mut missing = tonic::Request::new(pinned_request([1u8; 16], &[0]));
        let _ = &mut missing;
        let err = service
            .check_auth(missing.metadata())
            .expect_err("missing token rejected");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        let mut wrong = tonic::Request::new(pinned_request([1u8; 16], &[0]));
        wrong
            .metadata_mut()
            .insert("authorization", "Bearer nope".parse().unwrap());
        let err = service
            .check_auth(wrong.metadata())
            .expect_err("wrong token rejected");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);

        let mut ok = tonic::Request::new(pinned_request([1u8; 16], &[0]));
        ok.metadata_mut()
            .insert("authorization", "Bearer cluster-token".parse().unwrap());
        service
            .check_auth(ok.metadata())
            .expect("valid token accepted");

        assert_eq!(
            metrics.fragment_auth_failures_total(),
            2,
            "both rejections counted, the acceptance not"
        );
    }

    #[tokio::test]
    async fn routing_runs_local_when_live_set_empty() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let fetcher = RoutingSliceFetcher::new(
            Arc::new(OnceLock::new()),
            Arc::new(RwLock::new(Arc::new(Vec::new()))),
            "cluster-token".to_string(),
            service,
            metrics.clone(),
        );

        // No live workers: the router owns nothing remotely, so the slice runs
        // locally with no network hop and still returns a decoded response.
        let response = fetcher
            .fetch(pinned_request([9u8; 16], &[0]))
            .await
            .expect("local fetch returns a decoded slice response");
        // The tenant has no data, so the pinned segment does not resolve and the
        // worker reports a non-Ok typed status; routing still succeeded.
        let _ = response.status;
        assert_eq!(metrics.slices_local_total(), 1);
        assert_eq!(metrics.slices_remote_total(), 0);
        assert_eq!(metrics.slices_fallback_total(), 0);
    }

    #[tokio::test]
    async fn routing_falls_back_local_when_owner_endpoint_unreachable() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        // A live set with self and one unreachable remote worker.
        let live = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: self_id.to_string(),
                fragment_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                // Reserved-for-docs TEST-NET address that never accepts a
                // connection, so any slice mapped here fails at transport.
                fragment_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live,
            "cluster-token".to_string(),
            service,
            metrics.clone(),
        );

        // Drive distinct rendezvous units until one maps to the unreachable
        // remote worker and one maps to self. A remote-mapped slice attempts
        // the unreachable endpoint, times out at transport (bounded by
        // REMOTE_CONNECT_TIMEOUT, not the kernel SYN timeout), and falls back
        // to local execution rather than failing the query. We break as soon as
        // both paths are observed so the test pays at most a couple of connect
        // timeouts rather than one per iteration.
        let mut saw_fallback = false;
        let mut saw_local = false;
        let mut fetched = 0u64;
        for i in 0..256u128 {
            let tenant = uuid::Uuid::from_u128(i).into_bytes();
            let fallback_before = metrics.slices_fallback_total();
            let local_before = metrics.slices_local_total();
            fetcher
                .fetch(pinned_request(tenant, &[0]))
                .await
                .expect("fetch always resolves, remote or via local fallback");
            fetched += 1;
            if metrics.slices_fallback_total() > fallback_before {
                saw_fallback = true;
            }
            if metrics.slices_local_total() > local_before {
                saw_local = true;
            }
            if saw_fallback && saw_local {
                break;
            }
        }
        assert!(
            saw_fallback,
            "at least one unit mapped to the unreachable remote and fell back"
        );
        assert!(
            saw_local,
            "at least one unit mapped to self and ran locally"
        );
        // A fallback is executed locally, not counted as a successful remote.
        assert_eq!(metrics.slices_remote_total(), 0);
        // Every fetch resolved either locally or via local fallback; none failed.
        assert_eq!(
            metrics.slices_local_total() + metrics.slices_fallback_total(),
            fetched
        );
    }

    /// A worker that drops out of the live set stops receiving slices: the same
    /// rendezvous unit that mapped to the now-absent worker maps to a live one
    /// (here, self, run locally). This is the "skip a worker missing from the
    /// live set" property (ADR-0071 deliverable 3).
    #[tokio::test]
    async fn routing_skips_worker_absent_from_live_set() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        let live: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>> = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: self_id.to_string(),
                fragment_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live.clone(),
            "cluster-token".to_string(),
            service,
            metrics.clone(),
        );

        // Find a tenant whose unit maps to the remote worker (proven by a
        // transport fallback), then drop that worker from the live set.
        let mut mapped_to_other: Option<[u8; 16]> = None;
        for i in 0..256u128 {
            let tenant = uuid::Uuid::from_u128(i).into_bytes();
            let before = metrics.slices_fallback_total();
            fetcher
                .fetch(pinned_request(tenant, &[0]))
                .await
                .expect("fetch");
            if metrics.slices_fallback_total() > before {
                mapped_to_other = Some(tenant);
                break;
            }
        }
        let tenant = mapped_to_other.expect("some unit maps to the remote worker");

        // Remove the remote worker: it is now absent from the live set.
        *live.write() = Arc::new(vec![QueryWorkerRecord {
            process_id: self_id.to_string(),
            fragment_endpoint: "127.0.0.1:1".to_string(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        }]);

        let fallback_before = metrics.slices_fallback_total();
        let local_before = metrics.slices_local_total();
        fetcher
            .fetch(pinned_request(tenant, &[0]))
            .await
            .expect("fetch after the remote worker left");
        assert_eq!(
            metrics.slices_fallback_total(),
            fallback_before,
            "no remote attempt is made once the owner is gone"
        );
        assert_eq!(
            metrics.slices_local_total(),
            local_before + 1,
            "the slice now runs locally"
        );
    }

    /// The heartbeat registration writes a live `sys/query/workers/<uuid>`
    /// record that a fresh reader sees, and that ages out of the live set once
    /// it is older than the staleness window. Exercises the same
    /// [`QueryWorkers`] the server's heartbeat loop drives, with an injected
    /// clock instead of wall time.
    #[tokio::test]
    async fn worker_registration_appears_then_ages_out() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let workers = QueryWorkers::with_defaults("127.0.0.1:7000", codec::PROTOCOL_VERSION);
        let interval_ns =
            i64::try_from(workers.heartbeat_interval().as_nanos()).expect("interval fits i64");

        workers
            .write_heartbeat(store.as_ref(), 1_000)
            .await
            .expect("heartbeat write");
        let live = workers
            .live_set(store.as_ref(), 1_000)
            .await
            .expect("live set read");
        assert!(
            live.iter()
                .any(|r| r.process_id == workers.process_id().to_string()),
            "the just-written worker is in the live set"
        );

        // Far past the staleness window (3x the interval by default): the record
        // is no longer live. `self` is always included by `live_set`, so read
        // from a different identity to observe the aged-out record's absence.
        let observer = QueryWorkers::with_defaults("127.0.0.1:7001", codec::PROTOCOL_VERSION);
        let stale_now = 1_000 + interval_ns * 10;
        let live = observer
            .live_set(store.as_ref(), stale_now)
            .await
            .expect("live set read");
        assert!(
            !live
                .iter()
                .any(|r| r.process_id == workers.process_id().to_string()),
            "the stale worker has aged out of the live set"
        );
    }

    // The no-deadlock property itself (ADR-0071 deliverable 2) is proven
    // end to end in tests/distributed_query_e2e.rs
    // (`fragment_admits_while_client_cap_saturated_no_deadlock`), against the
    // server's real `QueryAdmissionController` and `FragmentAdmission` wiring.
    // A unit test here could only re-assert that two semaphores this module
    // constructed itself are independent, which is true by construction of
    // the test and can never fail for a production-code reason.

    /// A distributed query's per-slice fragment stats (ADR-0071
    /// `stats.fragments[]`, finding 4) are collected through the task-local
    /// sink: a self-mapped local slice records a `local`/`ok` entry, and a slice
    /// whose remote owner is unreachable records a `fallback` entry naming the
    /// endpoint it tried. Outside a scope, recording is a silent no-op.
    #[tokio::test]
    async fn fragment_stats_sink_collects_per_slice_entries() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        let live = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: self_id.to_string(),
                fragment_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live,
            "cluster-token".to_string(),
            service,
            metrics.clone(),
        );

        // Outside a scope: recording is a no-op (must not panic, records
        // nothing).
        fetcher
            .fetch(pinned_request([5u8; 16], &[0]))
            .await
            .expect("fetch outside a scope");

        // Inside a scope: drive distinct units until both a local and a fallback
        // slice are observed.
        let sink = FragmentStatsSink::new();
        with_fragment_stats(sink.clone(), async {
            let mut saw_local = false;
            let mut saw_fallback = false;
            for i in 0..256u128 {
                let tenant = uuid::Uuid::from_u128(i).into_bytes();
                let local_before = metrics.slices_local_total();
                let fallback_before = metrics.slices_fallback_total();
                fetcher
                    .fetch(pinned_request(tenant, &[0]))
                    .await
                    .expect("fetch inside the scope");
                if metrics.slices_local_total() > local_before {
                    saw_local = true;
                }
                if metrics.slices_fallback_total() > fallback_before {
                    saw_fallback = true;
                }
                if saw_local && saw_fallback {
                    break;
                }
            }
            assert!(
                saw_local && saw_fallback,
                "need both a local and a fallback slice to inspect"
            );
        })
        .await;

        let recorded = sink.take();
        assert!(
            !recorded.is_empty(),
            "the scope collected per-slice entries"
        );
        let local = recorded
            .iter()
            .find(|e| e.status == "ok" && e.worker_endpoint == "local")
            .expect("a self-mapped local slice records a local/ok entry");
        assert_eq!(
            local.segment_count, 1,
            "the slice covered one pinned segment"
        );
        let fallback = recorded
            .iter()
            .find(|e| e.status == "fallback")
            .expect("a slice whose remote owner is unreachable records a fallback entry");
        assert_eq!(
            fallback.worker_endpoint, "192.0.2.1:9",
            "the fallback entry names the unreachable remote endpoint it tried"
        );
        assert_eq!(fallback.segment_count, 1);
    }

    // --- Cross-cluster federation auth (ADR-0071 security, #868) ------------

    /// A fixed clock so `Catalog::resolve` over a bounded window is
    /// deterministic (the same reasoning as `tests::FixedClock`).
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    /// Publish one real RSEG segment plus its commit record for `tenant`, so a
    /// resolve-scope fetch for that tenant reads real data. Mirrors
    /// `crate::tests::publish_segment`.
    async fn publish_metric(
        store: &dyn ObjectStoreBackend,
        tenant: &ravel_types::TenantId,
        ts_ns: i64,
    ) {
        use ravel_commit::{keys, publish, record};
        use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
        use ravel_types::{Label, LabelSet, Sample, SeriesId};

        let tenant_hash = tenant.hash();
        let metric = "m";
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
            labels: label_set,
            samples: vec![Sample { ts_ns, value: 1.0 }],
        }];
        let writer_id = uuid::Uuid::from_u128(2_000);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");
        let rec = record::build(record::NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(
                &data_key,
                written.bytes,
                ravel_object_store::PutOptions::default(),
            )
            .await
            .expect("put data object");
        publish::publish(store, &rec, &ravel_commit::publish::RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// A `FragmentService` over `store` whose tenant resolver maps each
    /// `(bearer, TenantId)` in `creds`. The fragment token is `cluster-token`
    /// (never a valid tenant credential unless also listed in `creds`).
    fn federation_service(
        store: Arc<dyn ObjectStoreBackend>,
        creds: &[(&str, &str)],
        now_ns: i64,
    ) -> FragmentService {
        let metrics = Arc::new(FragmentMetrics::new());
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        let tokens: std::collections::HashMap<String, ravel_types::TenantId> = creds
            .iter()
            .map(|(tok, tenant)| {
                (
                    (*tok).to_string(),
                    ravel_types::TenantId::new((*tenant).to_string()),
                )
            })
            .collect();
        let resolver: Arc<dyn TenantResolver> =
            Arc::new(ravel_query::http::StaticBearerTokenResolver::new(tokens));
        FragmentService::new(
            "cluster-token".to_string(),
            resolver,
            FragmentAdmission::new(8, metrics.clone()),
            catalog,
            store,
            None,
            Arc::new(FixedClock(now_ns)),
            metrics,
        )
    }

    /// A resolve-scope request naming `wire_tenant` on the wire, over a window
    /// that covers `ts_ns`, for the metrics signal.
    fn resolve_request(wire_tenant: TenantHash, window_end_ns: i64) -> pb::FetchRequest {
        pb::FetchRequest {
            protocol_version: codec::PROTOCOL_VERSION,
            tenant_hash: wire_tenant.0.to_vec(),
            signal: metrics_signal(),
            window_start_ns: 0,
            window_end_ns,
            scope: Some(pb::fetch_request::Scope::Resolve(pb::ResolveScope {
                min_commit_token: Vec::new(),
            })),
            ..Default::default()
        }
    }

    /// Drive `service.fetch` with `bearer` and decode the returned frames.
    async fn fetch_decoded(
        service: &FragmentService,
        request: pb::FetchRequest,
        bearer: &str,
    ) -> Result<SliceResponse, tonic::Status> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut()
            .insert("authorization", format!("Bearer {bearer}").parse().unwrap());
        let response = service.fetch(req).await?;
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream.next().await {
            frames.push(frame.expect("in-crate stream never errors"));
        }
        Ok(decode_frames(frames).expect("frames decode to a slice response"))
    }

    const HOUR_NS: i64 = 3_600_000_000_000;

    /// A federated resolve-scope request reads the tenant its CREDENTIAL maps to,
    /// never the tenant it names on the wire. The credential maps to the
    /// data-bearing tenant while the wire names a different, empty tenant: the
    /// remote must serve the credential's data.
    ///
    /// Flip-line proof: delete the `inner.tenant_hash = tenant.0.to_vec()`
    /// override in `FragmentService::fetch` and this returns zero series
    /// (the wire tenant has no data), so the assertion fails.
    #[tokio::test]
    async fn federation_resolves_tenant_from_credential_not_wire() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let has_data = ravel_types::TenantId::new("remote-tenant".to_string());
        publish_metric(store.as_ref(), &has_data, HOUR_NS).await;

        // Credential -> the data-bearing tenant; wire names a DIFFERENT tenant
        // that holds no data.
        let service = federation_service(store, &[("operator-cred", "remote-tenant")], 4 * HOUR_NS);
        let other = ravel_types::TenantId::new("wire-named-other".to_string()).hash();
        let response = fetch_decoded(
            &service,
            resolve_request(other, 4 * HOUR_NS),
            "operator-cred",
        )
        .await
        .expect("valid tenant credential is accepted");

        assert!(
            response.series_returned > 0 && response.samples_returned > 0,
            "the remote served the credential's tenant data, not the wire tenant's \
             (got {} series)",
            response.series_returned
        );
    }

    /// Naming a data-bearing tenant on the wire does NOT read it when the
    /// credential maps to a different, empty tenant: the coordinator cannot
    /// reach across tenants by setting `tenant_hash`.
    #[tokio::test]
    async fn federation_wire_tenant_cannot_cross_to_another_tenant() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let victim = ravel_types::TenantId::new("victim-tenant".to_string());
        publish_metric(store.as_ref(), &victim, HOUR_NS).await;

        // Credential -> an empty tenant; wire names the victim tenant that DOES
        // hold data. The remote must serve the credential's (empty) tenant.
        let service = federation_service(store, &[("cred-empty", "empty-tenant")], 4 * HOUR_NS);
        let response = fetch_decoded(
            &service,
            resolve_request(victim.hash(), 4 * HOUR_NS),
            "cred-empty",
        )
        .await
        .expect("valid tenant credential is accepted");

        assert_eq!(
            response.series_returned, 0,
            "the wire tenant_hash must not let a credential read another tenant's data"
        );
    }

    /// The cluster-internal fragment token is NOT a tenant credential: a
    /// resolve-scope (federation) request presenting it is rejected, because the
    /// tenant resolver does not map it. This is the trust-boundary property that
    /// federation authenticates through ordinary tenant auth, never the shared
    /// fragment token.
    #[tokio::test]
    async fn federation_rejects_the_fragment_token() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let service = federation_service(store, &[("operator-cred", "remote-tenant")], 4 * HOUR_NS);
        let tenant = ravel_types::TenantId::new("remote-tenant".to_string()).hash();

        // `cluster-token` is the fragment token; it is not in the tenant map.
        let mut req = tonic::Request::new(resolve_request(tenant, 4 * HOUR_NS));
        req.metadata_mut().insert(
            "authorization",
            "Bearer cluster-token".parse().expect("valid header value"),
        );
        let err = service
            .fetch(req)
            .await
            .err()
            .expect("the fragment token is not a tenant credential");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
