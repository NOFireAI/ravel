//! ADR-0071 distributed read fan-out server wiring.
//!
//! This module turns the `ravel-query` distribution primitives
//! into a running cluster surface. It has three parts:
//!
//! * [`FragmentService`] -- the worker side. It implements the generated
//!   `SeriesFetch` gRPC service, guarding every `Pinned` call with a per-tenant,
//!   per-query fragment capability (ADR-0071 amendment, decision 2) and
//!   admitting it against one of two independent
//!   [`AdmissionClasses`] (`Pinned` or `Resolve`, selected by request scope;
//!   never the client-query cap), so a federation-heavy peer cluster queuing
//!   on the `Resolve` class can never delay this cluster's own intra-cluster
//!   `Pinned` slices. Per request it
//!   resolves a snapshot for the request's tenant over the request's event-time
//!   window, builds an interim content-hash
//!   [`SnapshotSegmentResolver`], and delegates to the in-crate
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
//!   costs no round trip. A first remote attempt
//!   lost at transport or answered `Unavailable` is re-dispatched exactly once
//!   to the next rendezvous worker, then executed coordinator-local; a typed
//!   failure surfaces only if local execution also fails. A worker-reported
//!   corruption, or any decode/framing fault, is terminal and typed straight
//!   through: it is never retried and never masked by a local fallback.
//! * [`spawn_heartbeat`] -- the membership loop. It writes this process's
//!   `sys/query/workers/<uuid>` heartbeat every interval and refreshes the
//!   shared live-worker set the router reads. On graceful shutdown it deletes
//!   its own record so a draining process drops out of every coordinator's live
//!   set immediately.
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
use ravel_catalog::Catalog;
use ravel_fleet::query_workers::{QueryWorkerRecord, QueryWorkers};
use ravel_fleet::worker_set;
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT};
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::FetchStats;
use ravel_query::ReadCache;
use ravel_query::SegmentFetcher;
use ravel_query::distrib::SliceStreamDecoder;
use ravel_query::distrib::client::{
    DistribError, SliceFetcher, SliceResponse, decode_slice_frames,
};
use ravel_query::distrib::codec;
use ravel_query::distrib::proto::series_fetch_client::SeriesFetchClient;
use ravel_query::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use ravel_query::distrib::service::{SeriesFetchService, SnapshotSegmentResolver};
use ravel_query::http::TenantResolver;
use ravel_types::accounting::QueryAccountingSnapshot;
use ravel_types::{Signal, TenantHash, TimeRange};
use tonic::transport::Channel;
use uuid::Uuid;

/// Bound on establishing a channel to a remote worker. A dead or unreachable
/// worker must never stall a query: the coordinator times out here and falls
/// back to local execution (ADR-0071 failure semantics), since it can always
/// read any slice itself. Without this bound a black-holed endpoint blocks on
/// the kernel's TCP SYN timeout (often over two minutes).
const REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The per-message ceiling on a fragment response frame this coordinator will
/// decode, set on every outbound `SeriesFetchClient` (issue #1687).
///
/// It is the cap tonic already applies by default today, made explicit for the
/// same reason the OTLP and OTAP services state theirs (`lib.rs`): the
/// coordinator's own per-slice caps bound an aggregate, and the size of ONE
/// frame is bounded only here, so leaving it to a dependency's default means a
/// tonic release could raise it without any change in this repository. A
/// conforming worker emits one frame per returned run, orders of magnitude
/// below this.
const MAX_FRAGMENT_DECODING_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// The closed reason label for a rejected fragment capability (ADR-0071
/// amendment, decision 2). Every `Pinned` fetch's capability check that fails
/// increments exactly one of these; the set is fixed and cardinality-safe, so it
/// is a legitimate metric label (ADR-0044 section 4). A signal mismatch counts
/// as [`CapabilityReject::QueryMismatch`]: the signal is part of a query's
/// identity, so a capability whose signal differs from the request's was not
/// minted for this query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityReject {
    /// The request carried no fragment capability at all.
    Missing,
    /// The capability was malformed (wrong length) or its MAC did not verify
    /// under any configured key.
    BadMac,
    /// The capability's `expires_unix_ns` is at or before the injected clock.
    Expired,
    /// The request's `tenant_hash` did not equal the capability's claimed tenant.
    TenantMismatch,
    /// The request's `query_id` or `signal` did not equal the capability's
    /// claims.
    QueryMismatch,
}

impl CapabilityReject {
    /// Every reason, in the fixed label order the counters are indexed by.
    const ALL: [CapabilityReject; 5] = [
        CapabilityReject::Missing,
        CapabilityReject::BadMac,
        CapabilityReject::Expired,
        CapabilityReject::TenantMismatch,
        CapabilityReject::QueryMismatch,
    ];

    /// The stable `reason` metric label value.
    pub fn reason(self) -> &'static str {
        match self {
            CapabilityReject::Missing => "missing",
            CapabilityReject::BadMac => "bad_mac",
            CapabilityReject::Expired => "expired",
            CapabilityReject::TenantMismatch => "tenant_mismatch",
            CapabilityReject::QueryMismatch => "query_mismatch",
        }
    }

    fn index(self) -> usize {
        match self {
            CapabilityReject::Missing => 0,
            CapabilityReject::BadMac => 1,
            CapabilityReject::Expired => 2,
            CapabilityReject::TenantMismatch => 3,
            CapabilityReject::QueryMismatch => 4,
        }
    }
}

/// The two independent admission classes for inbound fragment fetches
/// (issue #1722). `Pinned` fetches serve
/// this cluster's own intra-cluster fan-out; `Resolve` fetches serve
/// cross-cluster federation reads. Each class queues (never rejects) against
/// its own cap, so a class saturated with one kind of traffic never delays
/// the other: a peer cluster driving the `Resolve` class to its limit cannot
/// starve this cluster's `Pinned` slices, and vice versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClass {
    /// Intra-cluster fan-out: a capability-authorized fetch from this
    /// cluster's own coordinator.
    Pinned,
    /// Cross-cluster federation: a fetch from a peer cluster's coordinator,
    /// authorized by an ordinary tenant credential.
    Resolve,
}

impl AdmissionClass {
    /// Every class, in the fixed label order the per-class arrays are indexed
    /// by.
    const ALL: [AdmissionClass; 2] = [AdmissionClass::Pinned, AdmissionClass::Resolve];

    /// The stable `class` metric label value.
    pub fn label(self) -> &'static str {
        match self {
            AdmissionClass::Pinned => "pinned",
            AdmissionClass::Resolve => "resolve",
        }
    }

    fn index(self) -> usize {
        match self {
            AdmissionClass::Pinned => 0,
            AdmissionClass::Resolve => 1,
        }
    }
}

/// The `ravel_distrib_*` metric family (ADR-0071). Process-global
/// atomics, read at `/metrics` scrape time. Carries only the closed `mode`
/// label (and, for the admission series, the closed `class` label) at render
/// time; never a per-shard, per-worker, or per-tenant label (ADR-0044 section
/// 4).
#[derive(Debug)]
pub struct FragmentMetrics {
    /// Inbound fragment requests served after passing capability auth and
    /// admission.
    fragment_requests_total: AtomicU64,
    /// Inbound cross-cluster federation (resolve-scope) requests refused because
    /// the presented credential is not a valid tenant credential.
    fragment_auth_failures_total: AtomicU64,
    /// Inbound `Pinned` fetches refused by capability verification, indexed by
    /// [`CapabilityReject`] (ADR-0071 amendment, decision 2). Rendered under the
    /// closed `reason` label.
    fragment_capability_rejects: [AtomicU64; 5],
    /// Fragment requests currently holding an admission permit (gauge),
    /// indexed by [`AdmissionClass`]. Rendered under the closed `class` label.
    fragment_inflight: [AtomicU64; 2],
    /// Admission acquires that found their class's semaphore saturated and had
    /// to queue, indexed by [`AdmissionClass`]. Rendered under the closed
    /// `class` label; a class queuing does not mean it rejected anything (this
    /// admission never rejects), only that a caller waited for a permit.
    fragment_admission_waits_total: [AtomicU64; 2],
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
    /// Dead-endpoint quarantine events (ADR-0071 amendment, decision 3): a
    /// fragment endpoint whose dispatch was classified re-dispatchable
    /// (transport loss or an `Unavailable` summary) was recorded in the
    /// coordinator's quarantine map. Counted once per marking dispatch, so a
    /// re-mark after a readmitted-but-still-dead endpoint fails again counts
    /// again.
    quarantine_marks_total: AtomicU64,
    /// Quarantine readmit events (ADR-0071 amendment, decision 3): a
    /// quarantined endpoint published a strictly newer heartbeat stamp than the
    /// one recorded at mark time, so its entry was cleared and it is ranked
    /// again on this dispatch. The worker's own heartbeat is the half-open
    /// probe.
    quarantine_readmits_total: AtomicU64,
    /// Endpoints currently held in the coordinator's quarantine map (gauge).
    /// Kept in step with the map's length after every mark, readmit, and prune.
    quarantine_current: AtomicU64,
}

impl Default for FragmentMetrics {
    fn default() -> Self {
        FragmentMetrics {
            fragment_requests_total: AtomicU64::new(0),
            fragment_auth_failures_total: AtomicU64::new(0),
            fragment_capability_rejects: std::array::from_fn(|_| AtomicU64::new(0)),
            fragment_inflight: std::array::from_fn(|_| AtomicU64::new(0)),
            fragment_admission_waits_total: std::array::from_fn(|_| AtomicU64::new(0)),
            slices_local_total: AtomicU64::new(0),
            slices_remote_total: AtomicU64::new(0),
            slices_redispatched_total: AtomicU64::new(0),
            slices_fallback_total: AtomicU64::new(0),
            slice_fetch_micros_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            slice_fetch_nanos_total: AtomicU64::new(0),
            quarantine_marks_total: AtomicU64::new(0),
            quarantine_readmits_total: AtomicU64::new(0),
            quarantine_current: AtomicU64::new(0),
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

    fn record_capability_reject(&self, reason: CapabilityReject) {
        self.fragment_capability_rejects[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn inc_inflight(&self, class: AdmissionClass) {
        self.fragment_inflight[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn dec_inflight(&self, class: AdmissionClass) {
        // Saturating: an underflow would only happen on a double-drop, which the
        // permit guard's ownership prevents, but clamp rather than wrap.
        let _ = self.fragment_inflight[class.index()].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |v| Some(v.saturating_sub(1)),
        );
    }

    /// Record that an admission acquire found `class`'s semaphore saturated
    /// and had to queue.
    fn record_admission_wait(&self, class: AdmissionClass) {
        self.fragment_admission_waits_total[class.index()].fetch_add(1, Ordering::Relaxed);
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

    /// Record one dead-endpoint quarantine mark (ADR-0071 amendment, decision
    /// 3): a marking dispatch just recorded an endpoint into the quarantine map.
    fn record_quarantine_mark(&self) {
        self.quarantine_marks_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one quarantine readmit (ADR-0071 amendment, decision 3): a
    /// strictly newer heartbeat stamp cleared an endpoint's quarantine entry.
    fn record_quarantine_readmit(&self) {
        self.quarantine_readmits_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the current quarantine-map size as the gauge, called by the
    /// coordinator under its map lock after every mark, readmit, and prune.
    fn set_quarantine_current(&self, current: u64) {
        self.quarantine_current.store(current, Ordering::Relaxed);
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

    /// The count of `Pinned` fetches rejected for `reason` (ADR-0071 amendment,
    /// decision 2).
    pub fn capability_rejects(&self, reason: CapabilityReject) -> u64 {
        self.fragment_capability_rejects[reason.index()].load(Ordering::Relaxed)
    }

    /// The per-reason capability-reject counts paired with their stable `reason`
    /// label, for the `/metrics` renderer to emit one series per reason.
    pub fn capability_rejects_by_reason(&self) -> [(&'static str, u64); 5] {
        CapabilityReject::ALL.map(|reason| (reason.reason(), self.capability_rejects(reason)))
    }

    pub fn fragment_inflight(&self, class: AdmissionClass) -> u64 {
        self.fragment_inflight[class.index()].load(Ordering::Relaxed)
    }

    /// The per-class in-flight gauge values paired with their
    /// [`AdmissionClass`], for the `/metrics` renderer to emit one series
    /// per class under the `class` label.
    pub fn fragment_inflight_by_class(&self) -> [(AdmissionClass, u64); 2] {
        AdmissionClass::ALL.map(|class| (class, self.fragment_inflight(class)))
    }

    pub fn fragment_admission_waits_total(&self, class: AdmissionClass) -> u64 {
        self.fragment_admission_waits_total[class.index()].load(Ordering::Relaxed)
    }

    /// The per-class admission-wait counts paired with their
    /// [`AdmissionClass`], for the `/metrics` renderer to emit one series
    /// per class under the `class` label.
    pub fn fragment_admission_waits_by_class(&self) -> [(AdmissionClass, u64); 2] {
        AdmissionClass::ALL.map(|class| (class, self.fragment_admission_waits_total(class)))
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

    /// Total dead-endpoint quarantine marks (ADR-0071 amendment, decision 3).
    pub fn quarantine_marks_total(&self) -> u64 {
        self.quarantine_marks_total.load(Ordering::Relaxed)
    }

    /// Total quarantine readmits (ADR-0071 amendment, decision 3).
    pub fn quarantine_readmits_total(&self) -> u64 {
        self.quarantine_readmits_total.load(Ordering::Relaxed)
    }

    /// Endpoints currently quarantined (gauge; ADR-0071 amendment, decision 3).
    pub fn quarantine_current(&self) -> u64 {
        self.quarantine_current.load(Ordering::Relaxed)
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

/// One internal-workload admission class for inbound fragment fetches
/// (ADR-0071 deliverable 2). A plain counting semaphore, separate from the
/// client-query admission controller: a coordinator that holds a client-query
/// permit while it waits on its dispatched fragments can never deadlock
/// behind client queries queued on the client cap, because the workers
/// serving those fragments admit them here, against this independent bound.
///
/// One class alone does not protect a `Pinned` fetch from a `Resolve` fetch,
/// or vice versa: see [`AdmissionClasses`], which pairs two of these under
/// disjoint caps.
#[derive(Clone)]
pub struct FragmentAdmission {
    sem: Arc<tokio::sync::Semaphore>,
    metrics: Arc<FragmentMetrics>,
    class: AdmissionClass,
}

impl FragmentAdmission {
    /// A fragment admission class bounded by `max` concurrent fetches (clamped
    /// to at least 1).
    fn new(max: usize, metrics: Arc<FragmentMetrics>, class: AdmissionClass) -> Self {
        FragmentAdmission {
            sem: Arc::new(tokio::sync::Semaphore::new(max.max(1))),
            metrics,
            class,
        }
    }

    /// Acquire a fragment permit, queueing (never rejecting) when the class is
    /// saturated. `None` only if the semaphore was closed, which this process
    /// never does; the caller then maps it to an `Unavailable` status.
    async fn acquire(&self) -> Option<FragmentPermit> {
        let permit = match self.sem.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.metrics.record_admission_wait(self.class);
                self.sem.clone().acquire_owned().await.ok()?
            }
        };
        self.metrics.inc_inflight(self.class);
        Some(FragmentPermit {
            _permit: permit,
            metrics: Arc::clone(&self.metrics),
            class: self.class,
        })
    }
}

/// Held for the duration of one admitted fragment fetch; releases the permit and
/// decrements the in-flight gauge on drop.
struct FragmentPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    metrics: Arc<FragmentMetrics>,
    class: AdmissionClass,
}

impl Drop for FragmentPermit {
    fn drop(&mut self) {
        self.metrics.dec_inflight(self.class);
    }
}

/// The two independent admission classes a [`FragmentService`] admits fetches
/// against (issue #1722). `Pinned` and
/// `Resolve` each carry their own semaphore and their own cap, so a peer
/// cluster's federation reads queuing on `Resolve` never delays this
/// cluster's own `Pinned` slices, and a `Pinned` backlog never delays
/// `Resolve`. Both classes queue (never reject) when saturated, same as the
/// single class this replaces.
#[derive(Clone)]
pub struct AdmissionClasses {
    pinned: FragmentAdmission,
    resolve: FragmentAdmission,
}

impl AdmissionClasses {
    /// `max_pinned` and `max_resolve` are each clamped to at least 1
    /// concurrent fetch.
    pub fn new(max_pinned: usize, max_resolve: usize, metrics: Arc<FragmentMetrics>) -> Self {
        AdmissionClasses {
            pinned: FragmentAdmission::new(max_pinned, metrics.clone(), AdmissionClass::Pinned),
            resolve: FragmentAdmission::new(max_resolve, metrics, AdmissionClass::Resolve),
        }
    }

    fn for_class(&self, class: AdmissionClass) -> &FragmentAdmission {
        match class {
            AdmissionClass::Pinned => &self.pinned,
            AdmissionClass::Resolve => &self.resolve,
        }
    }
}

/// The fixed dNSName SAN every fragment worker certificate carries, and the one
/// server name a coordinator's outbound fragment dial verifies against (ADR-0071
/// amendment decision 1). The CA is dedicated to this surface, so any
/// certificate it signed for this name means "a fragment worker of this
/// cluster"; per-process certificate identity is deliberately not required (the
/// capability, not the certificate, is the authorization).
pub const FRAGMENT_TLS_SERVER_NAME: &str = "ravel-fragment";

/// Which listener a [`FragmentService`] instance is mounted on, and therefore
/// which request scopes it serves (ADR-0071 amendment decision 1). One
/// `FragmentServiceInner` is shared by every mounted clone; the role is the only
/// per-listener difference, so it lives outside the `Arc`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentListenerRole {
    /// The pre-amendment layout: one listener serves both `Pinned` (intra-cluster
    /// fan-out) and `Resolve` (cross-cluster federation). Used on the public gRPC
    /// listener when no dedicated `--fragment-listener` is configured, so
    /// distribution keeps working before an operator stands the dedicated
    /// listener up (and across a rolling deploy).
    Combined,
    /// The public gRPC listener under the amendment: serves `Resolve`/federation
    /// with ordinary tenant credentials only, and rejects `Pinned` outright. The
    /// dedicated fragment listener carries all `Pinned` traffic instead.
    PublicFederation,
    /// The dedicated TLS fragment listener under the amendment: serves `Pinned`
    /// capability-authorized fetches only, and rejects `Resolve` outright.
    DedicatedFragment,
}

/// The worker-side fragment surface (ADR-0071 deliverables 1 and 2). Cheap to
/// clone (every field is an `Arc` or a small value): the gRPC server owns one
/// clone and the coordinator's [`RoutingSliceFetcher`] holds another for
/// no-hop local execution.
#[derive(Clone)]
pub struct FragmentService {
    inner: Arc<FragmentServiceInner>,
    /// The listener this clone is mounted on, gating which scopes `fetch`
    /// accepts (ADR-0071 amendment decision 1). Defaults to [`Combined`]; the
    /// listener assembly in `lib.rs` overrides it per listener with
    /// [`FragmentService::with_role`]. Irrelevant for the coordinator's
    /// no-hop local execution, which bypasses `fetch` entirely.
    ///
    /// [`Combined`]: FragmentListenerRole::Combined
    role: FragmentListenerRole,
    /// This worker's own query limits, wired from the resolved process
    /// configuration by `lib.rs` through
    /// [`with_engine_config`](FragmentService::with_engine_config). Every
    /// slice this service runs clamps the coordinator's wire budget to these
    /// (issue #1687 part A), so a request from another cluster or another
    /// coordinator can never authorize more work here than this process's
    /// operator configured. `EngineConfig` is `Copy` and small, so it lives
    /// beside `role` outside the `Arc` rather than in the shared inner: the
    /// service is built before the process resolves its engine config, and a
    /// builder that rebuilt the inner would silently unshare the admission
    /// counters every mounted clone must agree on.
    ///
    /// Defaults to [`ravel_query::EngineConfig::default`], which is
    /// `Unlimited` bytes: a directly-constructed service (tests, benches)
    /// honours the wire budget verbatim, as before.
    engine: ravel_query::EngineConfig,
}

struct FragmentServiceInner {
    /// The cluster fragment keys (ADR-0071 amendment, decision 2). The worker
    /// verifies a `Pinned` fetch's capability MAC against ALL of them, so key
    /// rotation needs no flag day: a capability minted under any current key
    /// verifies. Only the first mints, but the mint side lives on the
    /// coordinator ([`RoutingSliceFetcher`]); a pure worker never reads index 0.
    fragment_keys: Arc<Vec<[u8; 32]>>,
    /// The deployment's ordinary tenant resolver chain. Used only for
    /// cross-cluster federation (resolve-scope) requests, where a federating
    /// coordinator's credential is an ordinary tenant credential and the tenant
    /// is derived from it, never from the wire (ADR-0071 security). The
    /// intra-cluster pinned path never consults it.
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: AdmissionClasses,
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    cache: Option<ReadCache>,
    clock: Arc<dyn Clock>,
    metrics: Arc<FragmentMetrics>,
    /// ADR-1195: the process-wide GET concurrency limiter, shared with every
    /// other fetcher this process builds. The distributed fragment path's
    /// `SegmentFetcher` (`resolve_and_run`) is wired to this same `Arc`, not a
    /// private pool sized from `--fetch-concurrency`.
    get_limiter: Arc<ravel_query::GetLimiter>,
}

/// The boxed frame stream the generated `SeriesFetch` server trait requires.
type FragmentStream =
    Pin<Box<dyn Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

impl FragmentService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fragment_keys: Arc<Vec<[u8; 32]>>,
        tenant_resolver: Arc<dyn TenantResolver>,
        admission: AdmissionClasses,
        catalog: Arc<Catalog>,
        store: Arc<dyn ObjectStoreBackend>,
        cache: Option<ReadCache>,
        clock: Arc<dyn Clock>,
        metrics: Arc<FragmentMetrics>,
        get_limiter: Arc<ravel_query::GetLimiter>,
    ) -> Self {
        FragmentService {
            inner: Arc::new(FragmentServiceInner {
                fragment_keys,
                tenant_resolver,
                admission,
                catalog,
                store,
                cache,
                clock,
                metrics,
                get_limiter,
            }),
            // Backward-compatible default: a directly-constructed service serves
            // both scopes. `lib.rs` sets an explicit role per listener via
            // `with_role`.
            role: FragmentListenerRole::Combined,
            engine: ravel_query::EngineConfig::default(),
        }
    }

    /// Return a clone of this service carrying this worker's own resolved
    /// [`ravel_query::EngineConfig`], sharing the same `FragmentServiceInner`.
    /// Called once by `lib.rs` on the service every listener and the
    /// coordinator's no-hop path clone from, so every slice run in this
    /// process clamps its wire budget to the local configuration (issue
    /// #1687 part A).
    #[must_use]
    pub fn with_engine_config(&self, engine: ravel_query::EngineConfig) -> Self {
        FragmentService {
            inner: self.inner.clone(),
            role: self.role,
            engine,
        }
    }

    /// Return a clone of this service bound to `role`, sharing the same
    /// `FragmentServiceInner` (ADR-0071 amendment decision 1). The listener
    /// assembly builds one service and mounts a role-specialized clone on each
    /// listener: [`PublicFederation`] on the public gRPC listener and
    /// [`DedicatedFragment`] on the dedicated TLS listener.
    ///
    /// [`PublicFederation`]: FragmentListenerRole::PublicFederation
    /// [`DedicatedFragment`]: FragmentListenerRole::DedicatedFragment
    pub fn with_role(&self, role: FragmentListenerRole) -> Self {
        FragmentService {
            inner: self.inner.clone(),
            role,
            engine: self.engine,
        }
    }

    /// Wrap this service in the generated gRPC server, ready to add to a
    /// cluster-internal `tonic` router. Mounted on the public gRPC listener
    /// (as [`PublicFederation`] or [`Combined`]) and, when configured, the
    /// dedicated TLS fragment listener (as [`DedicatedFragment`]); never the
    /// public HTTP or mTLS client listeners.
    ///
    /// [`PublicFederation`]: FragmentListenerRole::PublicFederation
    /// [`DedicatedFragment`]: FragmentListenerRole::DedicatedFragment
    /// [`Combined`]: FragmentListenerRole::Combined
    pub fn into_server(&self) -> SeriesFetchServer<FragmentService> {
        SeriesFetchServer::new(self.clone())
    }

    /// Stateless verification of a `Pinned` fetch's fragment capability
    /// (ADR-0071 amendment, decision 2). Replaces the shared bearer-token check.
    ///
    /// The capability travels in `request.fragment_capability`, never in gRPC
    /// metadata. Verification recomputes the keyed-BLAKE3 MAC over the claims and
    /// compares it in constant time against ALL configured fragment keys (so a
    /// capability minted under any current key verifies, and rotation needs no
    /// flag day), checks `expires_unix_ns` against the injected clock, and
    /// requires the request's `tenant_hash`, `signal`, and `query_id` to equal
    /// the claims. No store read, no cache, no coordination, no durable state.
    ///
    /// Every failure is a typed [`CapabilityReject`], counted under its closed
    /// `reason` label, and mapped to `Unauthenticated`. The checks run in a fixed
    /// order (present, MAC, expiry, tenant, query) so a capability that fails
    /// multiple ways is attributed to the first, most-fundamental reason.
    fn verify_capability(&self, request: &pb::FetchRequest) -> Result<(), tonic::Status> {
        let reject = |reason: CapabilityReject, message: &'static str| {
            self.inner.metrics.record_capability_reject(reason);
            Err(tonic::Status::unauthenticated(message))
        };

        if request.fragment_capability.is_empty() {
            return reject(
                CapabilityReject::Missing,
                "fragment request rejected: missing capability",
            );
        }
        let (claims, presented_mac) = match codec::decode_capability(&request.fragment_capability) {
            Ok(parts) => parts,
            Err(_) => {
                return reject(
                    CapabilityReject::BadMac,
                    "fragment request rejected: malformed capability",
                );
            }
        };
        // Constant-time MAC compare against every configured key; a match under
        // any current key accepts. `constant_time_eq` already runs without an
        // early exit for equal-length inputs.
        let mac_ok = self
            .inner
            .fragment_keys
            .iter()
            .any(|key| constant_time_eq(&codec::capability_mac(key, &claims), &presented_mac));
        if !mac_ok {
            return reject(
                CapabilityReject::BadMac,
                "fragment request rejected: capability MAC did not verify",
            );
        }
        // Expiry reuses the deadline the protocol already enforces cluster-wide.
        // A capability whose expiry is at or before now is dead.
        if claims.expires_unix_ns <= self.inner.clock.now_ns() {
            return reject(
                CapabilityReject::Expired,
                "fragment request rejected: capability expired",
            );
        }
        // The wire tenant must equal the authorized tenant: a capability minted
        // for one tenant cannot authorize a fetch that names another. This is
        // the F-1 cross-tenant boundary.
        if request.tenant_hash.as_slice() != claims.tenant_hash.as_slice() {
            return reject(
                CapabilityReject::TenantMismatch,
                "fragment request rejected: capability tenant does not match request",
            );
        }
        // The query id and signal must equal the claims: a capability minted for
        // one query cannot authorize another (the signal is part of a query's
        // identity, so a signal mismatch is a query mismatch).
        if request.query_id.as_slice() != claims.query_id.as_slice()
            || request.signal != claims.signal
        {
            return reject(
                CapabilityReject::QueryMismatch,
                "fragment request rejected: capability query does not match request",
            );
        }
        Ok(())
    }

    /// Authenticate a cross-cluster federation (resolve-scope) request and
    /// derive its tenant from the presented credential, never from the wire.
    ///
    /// ADR-0071 requires the remote to treat a federating coordinator's
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
    /// snapshot for the request's tenant and metrics signal over the request's
    /// event-time window.
    ///
    /// The coordinator carries the event-time envelope of the slice's pinned
    /// segments in `window_start_ns`/`window_end_ns` (see
    /// [`ravel_query::distrib`]'s `slice_event_window`). That envelope contains
    /// every pinned segment's own event range, so a resolve bounded to it still
    /// returns every pinned segment (a `Catalog::resolve` over a window returns
    /// every segment whose events overlap it): the resolved snapshot stays a
    /// superset of the dispatched pins, and the fetch reads exactly the pinned
    /// segments (byte-identical to the local path), while no longer paying a
    /// whole-history catalog resolve on the query critical path. A tenant we
    /// cannot decode, or a signal other than metrics, yields an empty resolver:
    /// the delegate service then returns the same typed status
    /// (`BadData`/`Unsupported`) it would for any such request, which the
    /// coordinator handles.
    async fn build_resolver(&self, request: &pb::FetchRequest) -> Arc<SnapshotSegmentResolver> {
        let Some(tenant_hash) = decode_tenant_hash(&request.tenant_hash) else {
            return Arc::new(SnapshotSegmentResolver::new(std::iter::empty()));
        };
        // Only metrics are distributed; for any other signal the delegate
        // returns Unsupported regardless of the resolver, so skip the resolve.
        if codec::signal_from_u32(request.signal) != Ok(Signal::Metrics) {
            return Arc::new(SnapshotSegmentResolver::new(std::iter::empty()));
        }
        let window = TimeRange {
            start_ns: request.window_start_ns,
            end_ns: request.window_end_ns,
        };
        let now_ns = self.inner.clock.now_ns();
        match self
            .inner
            .catalog
            .resolve(&tenant_hash, Signal::Metrics, window, &[], now_ns)
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
    /// cluster's own snapshot (ADR-0071 federation).
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
        let federated = matches!(request.scope, Some(pb::fetch_request::Scope::Resolve(_)));
        let (request, resolver) = match &request.scope {
            Some(pb::fetch_request::Scope::Resolve(_)) => self.resolve_scope(request).await,
            _ => {
                let resolver = self.build_resolver(&request).await;
                (request, resolver)
            }
        };
        let mut fetcher = SegmentFetcher::new(self.inner.store.clone())
            .with_get_limiter(self.inner.get_limiter.clone());
        if let Some(cache) = &self.inner.cache {
            fetcher = fetcher.with_cache(cache.clone());
        }
        // The worker's own limits clamp the coordinator's wire budget on every
        // slice (issue #1687 part A). A federated slice additionally enforces
        // this cluster's `max_series`/`max_samples`: the rewrite above turned
        // its scope into a pinned one over the LOCAL snapshot, so the service
        // can no longer tell where the request came from, and the requesting
        // coordinator folds this cluster's whole answer as one lump.
        let mut service =
            SeriesFetchService::new(fetcher, resolver).with_engine_config(self.engine);
        if federated {
            service = service.with_resolve_scope();
        }
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
        decode_slice_frames(frames)
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
        //  - Pinned scope (intra-cluster fan-out): a per-tenant, per-query
        //    fragment capability carried in the request, with the tenant taken
        //    from the wire. Coordinator and worker are one trust domain, and the
        //    capability conveys no privilege over the bucket's S3 credentials
        //    (ADR-0071 amendment, decision 2).
        //  - Resolve scope (cross-cluster federation): the presented credential
        //    is an ordinary tenant credential. The tenant is resolved from it
        //    through the normal resolver chain and OVERRIDES any wire
        //    tenant_hash, so a federating cluster reads only the tenant its
        //    credential authorizes. A fragment capability is never accepted here.
        // The listener this clone is mounted on decides which scopes it serves
        // (ADR-0071 amendment decision 1). The scope check runs BEFORE any
        // credential is inspected: a Pinned request on the public listener is
        // refused whether or not it carries a valid capability, and a Resolve
        // request on the dedicated fragment listener is refused before the
        // resolver chain runs. The rejection is a typed `tonic::Status`; because
        // the SeriesFetch service is still registered on both listeners, the
        // caller sees this handler-level status, not a gRPC "unimplemented".
        match &inner.scope {
            Some(pb::fetch_request::Scope::Resolve(_)) => {
                if self.role == FragmentListenerRole::DedicatedFragment {
                    return Err(tonic::Status::permission_denied(
                        "resolve/federation scope is not served on the dedicated fragment \
                         listener; it carries Pinned intra-cluster fetches only. Federate to the \
                         cluster's public gRPC listener instead (ADR-0071 amendment decision 1).",
                    ));
                }
                let tenant = self.resolve_federation_tenant(&metadata)?;
                inner.tenant_hash = tenant.0.to_vec();
            }
            _ => {
                if self.role == FragmentListenerRole::PublicFederation {
                    return Err(tonic::Status::permission_denied(
                        "pinned fragment scope is not served on the public gRPC listener; it \
                         serves Resolve/federation only. Dispatch Pinned fetches to the dedicated \
                         --fragment-listener over TLS (ADR-0071 amendment decision 1).",
                    ));
                }
                self.verify_capability(&inner)?;
            }
        }
        // Pinned and Resolve admit against disjoint classes (issue #1722), so a peer cluster's
        // federation reads queuing on Resolve can never delay this cluster's
        // own Pinned slices.
        let class = match &inner.scope {
            Some(pb::fetch_request::Scope::Resolve(_)) => AdmissionClass::Resolve,
            _ => AdmissionClass::Pinned,
        };
        let Some(_permit) = self.inner.admission.for_class(class).acquire().await else {
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
/// observability deliverable). [`RoutingSliceFetcher`] records one
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
    /// Response frame bytes this coordinator accepted off the wire for the
    /// slice, summed over its remote attempts (issue #1687 part B). A BYTE-cap
    /// refusal includes the frame that tripped it, because
    /// `SliceStreamDecoder::push` adds the frame's length before it compares
    /// against the cap; a FRAME-cap refusal does not, because that check
    /// returns before the length is measured. `0` for a slice that ran
    /// coordinator-local, which moves no frames. Distinct from
    /// `bytes_reported`, which is store bytes the worker says it read.
    pub wire_bytes_consumed: u64,
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
///
/// `bytes_reported` is the slice's whole cost either way (issue #1723): the
/// summed accounting of every attempt on success, and the spend carried on the
/// error when the slice ended failed. It reads zero for a failure only when no
/// attempt got far enough to report a summary.
fn record_fragment_stat(
    result: &Result<SliceResponse, DistribError>,
    worker_endpoint: String,
    segment_count: u64,
    fell_back: bool,
    wire_bytes_consumed: u64,
) {
    let (bytes_reported, status) = match result {
        Ok(response) => (
            response.accounting.total_s3_bytes(),
            if fell_back { "fallback" } else { "ok" },
        ),
        Err(err) => (
            err.spend().map_or(0, |spend| spend.total_s3_bytes()),
            "error",
        ),
    };
    let entry = FragmentStatEntry {
        worker_endpoint,
        segment_count,
        bytes_reported,
        wire_bytes_consumed,
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

/// What one attempt of a slice cost before it was abandoned (issue #1723).
///
/// A re-dispatched attempt is not a free attempt: the worker that reported
/// `Unavailable` had already GET the segments it managed to read, and the store
/// served them. `dispatch` carries this onto whichever attempt finally answers,
/// so the coordinator folds the sum of every attempt into the query's live
/// accounting handle rather than the survivor's share of it.
#[derive(Debug, Default, Clone)]
struct AttemptSpend {
    accounting: QueryAccountingSnapshot,
    stats: FetchStats,
}

impl AttemptSpend {
    /// Field-wise saturating sum, so a worker reporting near `u64::MAX` clamps
    /// instead of wrapping under the byte budget the coordinator re-enforces.
    fn merge(&mut self, other: &AttemptSpend) {
        self.accounting = self.accounting.saturating_merge(&other.accounting);
        self.stats.raw_f64_pages = self
            .stats
            .raw_f64_pages
            .saturating_add(other.stats.raw_f64_pages);
        self.stats.raw_f64_bytes = self
            .stats
            .raw_f64_bytes
            .saturating_add(other.stats.raw_f64_bytes);
        self.stats.histogram_series_skipped = self
            .stats
            .histogram_series_skipped
            .saturating_add(other.stats.histogram_series_skipped);
    }

    /// Whether this attempt spent anything at all: a pure-transport loss with
    /// no summary reports nothing, and merging it must stay a no-op.
    fn is_zero(&self) -> bool {
        self.accounting == QueryAccountingSnapshot::default() && self.stats == FetchStats::default()
    }

    /// Fold this carried spend into a slice result, so the coordinator sees one
    /// outcome whose accounting is the sum over every attempt.
    ///
    /// Both arms carry. On `Ok` the spend merges into the response's
    /// accounting and `FetchStats`. On `Err` there is no response to merge
    /// into, so the accounting rides on the error itself as
    /// [`DistribError::Spent`] and the coordinator folds it before it maps the
    /// error (issue #1723); that is the path a byte-cap refusal, a decode fault
    /// and a failed local fallback all take. What travels on that arm is
    /// `self`, the spend of the attempts already abandoned; the failing attempt
    /// adds its own only if its terminal summary was decoded first, which a
    /// decode-cap refusal never manages. The `FetchStats` page counters do not
    /// survive the `Err` arm: the coordinator returns no stats for a query that
    /// fails, so they would have no reader.
    fn fold_into(
        &self,
        result: Result<SliceResponse, DistribError>,
    ) -> Result<SliceResponse, DistribError> {
        if self.is_zero() {
            return result;
        }
        let carried = self.accounting;
        result
            .map_err(|err| err.with_spend(&carried))
            .map(|mut response| {
                response.accounting = response.accounting.saturating_merge(&self.accounting);
                response.stats.raw_f64_pages = response
                    .stats
                    .raw_f64_pages
                    .saturating_add(self.stats.raw_f64_pages);
                response.stats.raw_f64_bytes = response
                    .stats
                    .raw_f64_bytes
                    .saturating_add(self.stats.raw_f64_bytes);
                response.stats.histogram_series_skipped = response
                    .stats
                    .histogram_series_skipped
                    .saturating_add(self.stats.histogram_series_skipped);
                response
            })
    }
}

/// The classification of one remote dispatch attempt (ADR-0071 deliverable 1).
enum Attempt {
    /// A terminal outcome: a decoded response with a non-`Unavailable` status,
    /// or a non-transport error (a decode/framing/corruption fault). Return it
    /// as the slice's result; do not re-dispatch or fall back around it. Boxed
    /// because a `SliceResponse` is large relative to the `Retry` variant.
    Keep(Box<Result<SliceResponse, DistribError>>),
    /// Transport loss or an `Unavailable` summary. Re-dispatch may skip past
    /// this attempt to the next rendezvous worker, then coordinator-local. The
    /// payload is what this abandoned attempt already spent (issue #1723): the
    /// response's own accounting for an `Unavailable` summary, whatever a
    /// terminal summary reported before a stream broke, and zero for a
    /// transport loss with no summary, where the worker's spend cannot be
    /// observed from here at all. Boxed to keep the variant small beside
    /// `Keep`.
    Retry(Box<AttemptSpend>),
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
    /// The cluster fragment keys (ADR-0071 amendment, decision 2). The first key
    /// mints; every slice of a query gets a capability under it, including
    /// re-dispatches. Held as an `Arc` shared with the local [`FragmentService`],
    /// which verifies against the whole list.
    fragment_keys: Arc<Vec<[u8; 32]>>,
    /// The local fragment surface, used for self-mapped and fallback slices.
    local: FragmentService,
    /// Cached `tonic` channels, keyed by endpoint (a channel is cheap to clone
    /// but expensive to reconnect).
    channels: Mutex<HashMap<String, Channel>>,
    /// Dead-endpoint quarantine (ADR-0071 amendment, decision 3): a map from a
    /// fragment endpoint to the worker's `started_unix_ns` heartbeat stamp as
    /// seen in the live view at the moment its dispatch was classified
    /// re-dispatchable. Coordinator-local soft state, in memory only, no durable
    /// backing. `ranked_owners` drops a candidate that is present here and whose
    /// current live-view stamp is no newer than the recorded one, so the
    /// coordinator stops re-paying `REMOTE_CONNECT_TIMEOUT` to a dead worker for
    /// the up-to-4-heartbeat-interval window it stays ranked after death. A
    /// strictly newer stamp (the worker's own next heartbeat, the half-open
    /// probe) readmits it; an endpoint absent from the live view is pruned.
    quarantine: Mutex<HashMap<String, i64>>,
    /// The client TLS configuration for dialing a remote worker's dedicated
    /// fragment listener (ADR-0071 amendment decision 1). `Some` when this
    /// process runs a `--fragment-listener` (workers advertise that TLS address
    /// as their `fragment_endpoint`), pinning the operator CA and verifying the
    /// fixed [`FRAGMENT_TLS_SERVER_NAME`] server name. `None` under the
    /// pre-amendment layout, where fragment endpoints are the plaintext public
    /// gRPC listener; the dial then stays plaintext, byte-identical to before.
    client_tls: Option<tonic::transport::ClientTlsConfig>,
    /// The per-slice response frame cap this coordinator decodes under (issue
    /// #1687 part B). Always [`codec::MAX_SLICE_RESPONSE_FRAMES`] in a real
    /// process; only the tests lower it, so a test can drive a real stream
    /// across the cap without producing a million frames.
    max_slice_frames: usize,
    /// The per-slice response BYTE cap this coordinator decodes under (issue
    /// #1687 part B). Always [`codec::MAX_SLICE_RESPONSE_BYTES`] in a real
    /// process; only the tests lower it, so a test can drive a real stream
    /// across the cap without moving hundreds of megabytes over the wire.
    max_slice_bytes: u64,
    metrics: Arc<FragmentMetrics>,
}

impl RoutingSliceFetcher {
    pub fn new(
        self_id: Arc<OnceLock<Uuid>>,
        live_workers: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>,
        fragment_keys: Arc<Vec<[u8; 32]>>,
        local: FragmentService,
        metrics: Arc<FragmentMetrics>,
    ) -> Self {
        RoutingSliceFetcher {
            self_id,
            live_workers,
            fragment_keys,
            local,
            channels: Mutex::new(HashMap::new()),
            quarantine: Mutex::new(HashMap::new()),
            // Plaintext dial by default (the pre-amendment layout). A coordinator
            // running a dedicated fragment listener enables TLS via
            // `with_client_tls`.
            client_tls: None,
            max_slice_frames: codec::MAX_SLICE_RESPONSE_FRAMES,
            max_slice_bytes: codec::MAX_SLICE_RESPONSE_BYTES,
            metrics,
        }
    }

    /// Lower this coordinator's per-slice frame cap. Test-only: a real process
    /// decodes under [`codec::MAX_SLICE_RESPONSE_FRAMES`], and there is no
    /// operator flag for this.
    #[cfg(test)]
    fn with_max_slice_frames(mut self, max_slice_frames: usize) -> Self {
        self.max_slice_frames = max_slice_frames;
        self
    }

    /// Lower this coordinator's per-slice byte cap. Test-only, for the same
    /// reason as [`with_max_slice_frames`](Self::with_max_slice_frames): a real
    /// process decodes under [`codec::MAX_SLICE_RESPONSE_BYTES`], and there is
    /// no operator flag for this.
    #[cfg(test)]
    fn with_max_slice_bytes(mut self, max_slice_bytes: u64) -> Self {
        self.max_slice_bytes = max_slice_bytes;
        self
    }

    /// Enable TLS on this coordinator's outbound fragment dials (ADR-0071
    /// amendment decision 1), pinning the operator CA and the fixed
    /// [`FRAGMENT_TLS_SERVER_NAME`] server name. Set when the process runs a
    /// `--fragment-listener`, so remote workers advertise their TLS fragment
    /// endpoint. A `None` argument leaves the dial plaintext, so this is a no-op
    /// under the pre-amendment layout. Added as a post-construction builder (not
    /// a `new` parameter) so existing call sites that dial plaintext are
    /// unchanged.
    pub fn with_client_tls(
        mut self,
        client_tls: Option<tonic::transport::ClientTlsConfig>,
    ) -> Self {
        self.client_tls = client_tls;
        self
    }

    /// Mint the `Pinned` fragment capability for a slice under the first
    /// (minting) fragment key (ADR-0071 amendment, decision 2). The claims come
    /// from the request the coordinator already threaded: its `tenant_hash`,
    /// `signal`, `query_id`, and absolute `deadline_unix_ns` as the expiry. A
    /// query has exactly one tenant/signal/query, so every slice of a query mints
    /// a byte-identical capability, including re-dispatches. Returns `None` when
    /// no key is configured or the wire tenant/query id is not 16 bytes; the
    /// worker then rejects the missing/short capability and the coordinator falls
    /// back to local execution, never a wrong answer.
    fn mint_capability(&self, request: &pb::FetchRequest) -> Option<Vec<u8>> {
        let key = self.fragment_keys.first()?;
        let tenant_hash: [u8; 16] = request.tenant_hash.as_slice().try_into().ok()?;
        let query_id: [u8; 16] = request.query_id.as_slice().try_into().ok()?;
        let claims = codec::FragmentClaims {
            capability_version: codec::CAPABILITY_VERSION,
            tenant_hash,
            signal: request.signal,
            query_id,
            expires_unix_ns: request.deadline_unix_ns,
        };
        Some(codec::mint_capability(key, &claims))
    }

    /// The `started_unix_ns` heartbeat stamp `endpoint` currently carries in the
    /// live view, or `None` if it is absent (aged out or never seen). Used at
    /// mark time to record the stamp a later heartbeat must strictly exceed to
    /// readmit the endpoint (ADR-0071 amendment, decision 3).
    fn live_stamp(&self, endpoint: &str) -> Option<i64> {
        let live = Arc::clone(&self.live_workers.read());
        live.iter()
            .find(|record| record.fragment_endpoint == endpoint)
            .map(|record| record.started_unix_ns)
    }

    /// Record `endpoint` into the quarantine map at its current live-view stamp
    /// (ADR-0071 amendment, decision 3, Mark). Called at the point `dispatch`
    /// classifies a remote attempt as [`Attempt::Retry`]; it invents no new
    /// failure detection, it reuses that classification. An endpoint already
    /// absent from the live view is never ranked again, so there is nothing to
    /// quarantine and the mark is skipped.
    fn mark_quarantine(&self, endpoint: &str) {
        let Some(stamp) = self.live_stamp(endpoint) else {
            return;
        };
        let mut quarantine = self.quarantine.lock();
        quarantine.insert(endpoint.to_string(), stamp);
        self.metrics.set_quarantine_current(quarantine.len() as u64);
        self.metrics.record_quarantine_mark();
    }

    /// Whether a quarantined `endpoint` at its current live-view `stamp` must be
    /// skipped by `ranked_owners` (ADR-0071 amendment, decision 3, Skip and
    /// Readmit). It is skipped while its stamp is no newer than the one recorded
    /// at mark time. A strictly newer stamp is first-hand evidence of life (only
    /// the worker writes its own record), so it clears the entry, counts a
    /// readmit, and the endpoint is ranked again. An endpoint not in the map is
    /// never skipped.
    fn quarantine_skips(&self, endpoint: &str, stamp: i64) -> bool {
        let mut quarantine = self.quarantine.lock();
        match quarantine.get(endpoint).copied() {
            Some(marked) if stamp > marked => {
                quarantine.remove(endpoint);
                self.metrics.set_quarantine_current(quarantine.len() as u64);
                self.metrics.record_quarantine_readmit();
                false
            }
            Some(_) => true,
            None => false,
        }
    }

    /// Drop quarantine entries whose endpoint is absent from `live` (ADR-0071
    /// amendment, decision 3, Prune). Such endpoints are never ranked anyway, so
    /// this only bounds the map by the historical worker-set size; it is not a
    /// readmit and is not counted as one.
    fn prune_quarantine(&self, live: &[QueryWorkerRecord]) {
        let mut quarantine = self.quarantine.lock();
        if quarantine.is_empty() {
            return;
        }
        let before = quarantine.len();
        quarantine.retain(|endpoint, _| {
            live.iter()
                .any(|record| &record.fragment_endpoint == endpoint)
        });
        if quarantine.len() != before {
            self.metrics.set_quarantine_current(quarantine.len() as u64);
        }
    }

    /// Rendezvous-rank the live worker set for a slice, top owner first.
    ///
    /// Only workers whose `protocol_version` equals the coordinator's are
    /// considered: a version-skewed worker is dropped here, at routing time, so
    /// the mismatch never costs a dispatch round trip (ADR-0071 failure
    /// semantics). The ranking is produced by
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
        // Opportunistic prune: drop quarantine entries for endpoints no longer
        // in the live view, bounding the map by the historical worker set
        // (ADR-0071 amendment, decision 3, Prune).
        self.prune_quarantine(&live);
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
                // Skip a quarantined dead endpoint whose heartbeat stamp has not
                // advanced past the one recorded at its failure, so routing falls
                // through to the next rendezvous owner or SelfLocal without
                // re-paying its connect timeout (ADR-0071 amendment, decision 3,
                // Skip). A strictly newer stamp readmits it inside
                // `quarantine_skips`.
                if !self.quarantine_skips(&record.fragment_endpoint, record.started_unix_ns) {
                    ranked.push(Owner::Remote(record.fragment_endpoint.clone()));
                }
            }
            ids.retain(|id| *id != owner);
        }
        ranked
    }

    /// Attempt one remote dispatch and classify the outcome for re-dispatch
    /// (ADR-0071 deliverable 1). Transport loss and an `Unavailable` summary are
    /// [`Attempt::Retry`] (re-dispatchable); every other outcome, success or a
    /// hard decode/framing error, is [`Attempt::Keep`] and terminal.
    ///
    /// Retrying drops the attempt's RESULT, never its COST (issue #1723): an
    /// `Unavailable` summary carries what that worker spent before it gave up,
    /// and [`Attempt::Retry`] carries it forward so `dispatch` can fold it into
    /// whichever attempt finally answers.
    async fn try_remote(
        &self,
        endpoint: &str,
        request: &pb::FetchRequest,
        wire_bytes: &AtomicU64,
    ) -> Attempt {
        let mut salvaged = None;
        match self
            .remote_fetch(endpoint, request.clone(), wire_bytes, &mut salvaged)
            .await
        {
            Ok(response) if response.status == pb::status::Code::Unavailable => {
                tracing::warn!(
                    %endpoint,
                    "remote slice reported Unavailable; re-dispatching to next worker"
                );
                Attempt::Retry(Box::new(AttemptSpend {
                    accounting: response.accounting,
                    stats: response.stats,
                }))
            }
            Ok(response) => Attempt::Keep(Box::new(Ok(response))),
            Err(DistribError::Transport(message)) => {
                tracing::warn!(
                    %endpoint,
                    error = %message,
                    "remote slice fetch failed at transport; re-dispatching to next worker"
                );
                // A stream that broke after its terminal summary still told us
                // what the worker spent; one that broke before it did not, and
                // reports zero rather than a guess.
                Attempt::Retry(Box::new(salvaged.unwrap_or_default()))
            }
            // A decode, framing, or worker-reported corruption error is a real
            // defect, not a routing miss: propagate it typed rather than mask it
            // with a retry or a local fallback (ADR-0071 deliverable 3).
            //
            // Terminal does not mean free (issue #1723). This arm ends the
            // slice, so a terminal summary decoded before the fault has no
            // later attempt to ride on and must ride on the error. `salvaged`
            // is that summary's spend. It is `None` for every attempt that
            // ended before its summary was decoded, where the wrap is a no-op,
            // and that includes both decode-cap refusals: a worker streams its
            // summary last and `push` checks the caps before it stores a
            // frame, so a refusal here reports none of what it made the worker
            // pay.
            Err(other) => Attempt::Keep(Box::new(Err(match salvaged {
                Some(spend) => other.with_spend(&spend.accounting),
                None => other,
            }))),
        }
    }

    /// Get (or open and cache) a channel to a remote worker's fragment endpoint.
    ///
    /// When this coordinator runs a dedicated fragment listener (`client_tls` is
    /// `Some`), the dial terminates TLS against the pinned operator CA with the
    /// fixed [`FRAGMENT_TLS_SERVER_NAME`] server name (ADR-0071 amendment
    /// decision 1): the coordinator authenticates that it reached a real cluster
    /// worker before any capability crosses the wire. Under the pre-amendment
    /// layout (`client_tls` is `None`) the dial stays plaintext `http://`,
    /// byte-identical to before. The channel cache is unchanged either way.
    async fn channel(&self, endpoint: &str) -> Result<Channel, DistribError> {
        if let Some(channel) = self.channels.lock().get(endpoint).cloned() {
            return Ok(channel);
        }
        let scheme = if self.client_tls.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{endpoint}");
        let mut endpoint_builder = Channel::from_shared(uri)
            .map_err(|e| {
                DistribError::Transport(format!("invalid worker endpoint {endpoint}: {e}"))
            })?
            .connect_timeout(REMOTE_CONNECT_TIMEOUT);
        if let Some(tls) = &self.client_tls {
            endpoint_builder = endpoint_builder.tls_config(tls.clone()).map_err(|e| {
                DistribError::Transport(format!(
                    "failed to configure fragment TLS for {endpoint}: {e}"
                ))
            })?;
        }
        let channel = endpoint_builder
            .connect()
            .await
            .map_err(|e| DistribError::Transport(format!("connect to {endpoint} failed: {e}")))?;
        self.channels
            .lock()
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Dispatch one slice to a remote worker over an authed channel and decode
    /// its frames as they arrive.
    ///
    /// The decode is incremental and bounded (issue #1687 part B): each frame is
    /// counted and measured before it is decoded, and the first breach of either
    /// the frame cap or the fixed per-slice wire-byte ceiling returns without
    /// pulling another message. Dropping the stream at that point cancels the
    /// RPC, so the remote stops producing too.
    ///
    /// `wire_bytes` accumulates the frame bytes this attempt accepted, including
    /// the frame that tripped a cap, so a refused slice still reports what it
    /// made the coordinator hold.
    ///
    /// `salvaged` receives the spend of a terminal summary that arrived before
    /// the attempt failed, and is left `None` otherwise (issue #1723). It is an
    /// out-parameter because it is meaningful only on the `Err` return, where
    /// there is no `SliceResponse` to put it on.
    async fn remote_fetch(
        &self,
        endpoint: &str,
        mut request: pb::FetchRequest,
        wire_bytes: &AtomicU64,
        salvaged: &mut Option<AttemptSpend>,
    ) -> Result<SliceResponse, DistribError> {
        let channel = self.channel(endpoint).await?;
        // Mint and attach the per-query capability (ADR-0071 amendment, decision
        // 2): it travels in the request body, not in gRPC metadata. Every slice
        // of a query, including this re-dispatch, mints a byte-identical
        // capability under the first fragment key.
        if let Some(capability) = self.mint_capability(&request) {
            request.fragment_capability = capability;
        }
        let tonic_request = tonic::Request::new(request);
        let mut client = SeriesFetchClient::new(channel)
            .max_decoding_message_size(MAX_FRAGMENT_DECODING_MESSAGE_BYTES);
        let response = client
            .fetch(tonic_request)
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?;
        let mut decoder = SliceStreamDecoder::new(&self.local.engine)
            .with_max_frames(self.max_slice_frames)
            .with_max_bytes(self.max_slice_bytes);
        let mut stream = response.into_inner();
        let outcome = loop {
            let next = stream
                .message()
                .await
                .map_err(|s| DistribError::Transport(s.to_string()));
            match next {
                Ok(Some(frame)) => {
                    if let Err(err) = decoder.push(frame) {
                        break Err(err);
                    }
                }
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };
        wire_bytes.fetch_add(decoder.bytes_consumed(), Ordering::Relaxed);
        match outcome {
            // A stream can close cleanly and still fail to finish: a summary
            // carrying no status, or a status code only a newer worker emits.
            // That summary already said what the attempt spent and this arm
            // ends the slice, so its spend is salvaged here too (issue #1723).
            // Reading it before `finish` consumes the decoder costs one
            // snapshot decode on the success path.
            Ok(()) => {
                let reported = decoder.summary_spend();
                decoder.finish().inspect_err(|_| {
                    *salvaged =
                        reported.map(|(accounting, stats)| AttemptSpend { accounting, stats });
                })
            }
            Err(err) => {
                *salvaged = decoder
                    .summary_spend()
                    .map(|(accounting, stats)| AttemptSpend { accounting, stats });
                if matches!(
                    err,
                    DistribError::Codec(codec::CodecError::SliceFrameCapExceeded { .. })
                        | DistribError::Codec(codec::CodecError::SliceByteCapExceeded { .. })
                ) {
                    tracing::warn!(
                        %endpoint,
                        frames = decoder.frames_consumed(),
                        wire_bytes = decoder.bytes_consumed(),
                        error = %err,
                        "refused a slice that exceeded a coordinator decode cap",
                    );
                }
                Err(err)
            }
        }
    }
}

#[async_trait]
impl SliceFetcher for RoutingSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let start = Instant::now();
        let segment_count = pinned_segment_count(&request);
        let ranked = self.ranked_owners(&request);
        // Wire frame bytes this slice made the coordinator hold, summed over
        // every remote attempt it made (issue #1687 part B). A refused attempt
        // contributes what it accepted before the cap tripped, so the accounting
        // does not read as zero for the case the caps exist for.
        let wire_bytes = AtomicU64::new(0);
        let (result, worker_endpoint, fell_back) =
            self.dispatch(ranked, request, &wire_bytes).await;
        self.metrics.observe_slice_fetch(start.elapsed());
        record_fragment_stat(
            &result,
            worker_endpoint,
            segment_count,
            fell_back,
            wire_bytes.load(Ordering::Relaxed),
        );
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
    /// discarded and never merged. A failed attempt's DATA is discarded; its
    /// COST is not (issue #1723). Each abandoned attempt's spend accumulates in
    /// `carried` and is folded into whichever attempt answers, so the one
    /// `SliceResponse` the coordinator folds carries the sum of what the store
    /// actually served for this slice, not the survivor's share of it. Without
    /// that, a store answering 503 lets one slice be fetched three times and be
    /// charged once, and the byte budget is enforced against the third of it
    /// that got reported.
    ///
    /// Returns the slice result, the endpoint label for its `fragments[]` stats
    /// entry, and whether it fell back to local after a failed remote attempt.
    async fn dispatch(
        &self,
        ranked: Vec<Owner>,
        request: pb::FetchRequest,
        wire_bytes: &AtomicU64,
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

        // What the attempts that were abandoned already spent (issue #1723),
        // folded into the attempt that answers.
        let mut carried = AttemptSpend::default();

        // First remote attempt against the top owner.
        match self.try_remote(&primary, &request, wire_bytes).await {
            Attempt::Keep(result) => {
                self.metrics.record_slice_remote();
                return (*result, primary, false);
            }
            // The primary failed re-dispatchably: quarantine it at its current
            // live-view stamp so later queries route past it without re-paying
            // the connect timeout (ADR-0071 amendment, decision 3, Mark). This
            // reuses the existing classification, adding no new failure signal.
            Attempt::Retry(spend) => {
                carried.merge(&spend);
                self.mark_quarantine(&primary);
            }
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
            match self.try_remote(next, &request, wire_bytes).await {
                Attempt::Keep(result) => {
                    self.metrics.record_slice_remote();
                    return (carried.fold_into(*result), next.clone(), false);
                }
                // The re-dispatch target also failed re-dispatchably: quarantine
                // it too, so a subsequent query skips both corpses.
                Attempt::Retry(spend) => {
                    carried.merge(&spend);
                    self.mark_quarantine(next);
                }
            }
        }

        // Primary and its one re-dispatch both failed (or there was no next
        // remote worker): the coordinator reads the slice itself. Its store
        // access is the same, so a successful local read is byte-identical to
        // the remote result; only if local also fails does the slice fail typed.
        self.metrics.record_slice_fallback();
        (
            carried.fold_into(self.local.run_local(request).await),
            primary,
            true,
        )
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

/// Spawn the query-worker heartbeat loop (ADR-0071 deliverable 3). Writes this
/// process's `sys/query/workers/<uuid>` record immediately and then every
/// interval, and refreshes `live_workers` from the store on the same cadence so
/// the [`RoutingSliceFetcher`] always reads a recent membership view. The first
/// write/read happens before the first sleep, so membership converges promptly
/// after startup.
///
/// The loop stops when `shutdown` fires (graceful shutdown holds the sender on
/// `Running`). On stop it DELETES its own `sys/query/workers/<uuid>` record
/// before returning, so a draining process drops out of every sibling
/// coordinator's live set at once rather than lingering until its stamp ages
/// past the `3 * H` staleness window. Without this a coordinator keeps dialing
/// a worker that has already stopped serving for up to the staleness window.
///
/// That delete runs on a graceful drain alone, so a process lost to a panic, a
/// kill or a node loss leaves its key behind. The same tick therefore reaps
/// every key past the reap horizon, taken from the listing the membership read
/// already made (issue #1761), which bounds the prefix to the live fleet
/// instead of to every query worker that ever ran.
pub fn spawn_heartbeat(
    workers: Arc<QueryWorkers>,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    live_workers: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = workers.heartbeat_interval();
        loop {
            let now_ns = clock.now_ns();
            if let Err(err) = workers.write_heartbeat(store.as_ref(), now_ns).await {
                tracing::warn!(error = %err, "query worker heartbeat write failed");
            }
            match workers.live_set_read(store.as_ref(), now_ns).await {
                Ok(read) => {
                    // One listing serves both: the membership view the
                    // routing fetcher reads, and the keys past the reap
                    // horizon. Reaping from that same read is what keeps the
                    // prefix bounded without a second LIST, which is how the
                    // maintain tier does it too.
                    let reaped = workers.reap_keys(store.as_ref(), &read.reapable).await;
                    if reaped > 0 {
                        tracing::info!(reaped, "reaped dead query worker heartbeat keys");
                    }
                    *live_workers.write() = Arc::new(read.live);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "query worker live_set read failed; keeping prior membership")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = &mut shutdown => {
                    // Draining: remove our own record so coordinators stop
                    // dialing us immediately. A failed delete self-corrects as
                    // the stamp ages out, so it is a warning, not fatal.
                    if let Err(err) = workers.delete_heartbeat(store.as_ref()).await {
                        tracing::warn!(error = %err, "query worker heartbeat delete on shutdown failed");
                    }
                    return;
                }
            }
        }
    })
}

/// A [`SliceFetcher`] over one remote cluster's fragment `SeriesFetch` surface
/// (ADR-0071 cross-cluster federation).
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
    /// This coordinator's own query limits, the config its slice decoder is
    /// built from. The decoder's caps are fixed constants and do not vary with
    /// it (issue #1687 part B); the process still wires its resolved config in
    /// through [`with_engine_config`](FederationSliceFetcher::with_engine_config)
    /// so a federated slice decodes under the same configuration object an
    /// intra-cluster one does.
    engine: ravel_query::EngineConfig,
    /// The per-slice response byte cap this coordinator decodes a federated
    /// slice under. Always [`codec::MAX_SLICE_RESPONSE_BYTES`] in a real
    /// process; only the tests lower it, so a test can drive a real stream
    /// across the cap without moving hundreds of megabytes over the wire.
    max_slice_bytes: u64,
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
            engine: ravel_query::EngineConfig::default(),
            max_slice_bytes: codec::MAX_SLICE_RESPONSE_BYTES,
        })
    }

    /// Wire this process's resolved query limits into the fetcher, so a
    /// federated slice decodes under the same configuration an intra-cluster
    /// slice does. A post-construction builder, so an existing call site that
    /// does not set one keeps the `EngineConfig::default` it had. Both decode
    /// caps are fixed constants (issue #1687 part B) and apply either way.
    pub fn with_engine_config(mut self, engine: ravel_query::EngineConfig) -> Self {
        self.engine = engine;
        self
    }

    /// Lower the per-slice byte cap for a federated slice. Test-only: a real
    /// process decodes under [`codec::MAX_SLICE_RESPONSE_BYTES`], and there is
    /// no operator flag for this.
    #[cfg(test)]
    fn with_max_slice_bytes(mut self, max_slice_bytes: u64) -> Self {
        self.max_slice_bytes = max_slice_bytes;
        self
    }
}

#[async_trait]
impl SliceFetcher for FederationSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let mut tonic_request = tonic::Request::new(request);
        // Present the operator credential, never the calling client's: the
        // client credential never crosses a cluster boundary (ADR-0071 trust
        // boundary).
        let value = format!("Bearer {}", self.credential).parse().map_err(|_| {
            DistribError::Transport("invalid federation bearer token metadata".to_string())
        })?;
        tonic_request.metadata_mut().insert("authorization", value);
        let mut client = SeriesFetchClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_FRAGMENT_DECODING_MESSAGE_BYTES);
        let response = client.fetch(tonic_request).await.map_err(|s| {
            DistribError::Transport(format!("federated fetch to cluster {}: {s}", self.cluster))
        })?;
        // A remote's frames get the same decode validation as an intra-cluster
        // slice's (ADR-0071 trust boundary): a malformed frame is a typed
        // DistribError, never a panic and never silently dropped data. They get
        // the same two decode caps too (issue #1687 part B), checked before each
        // frame is decoded, and the first breach returns without pulling
        // another message.
        let mut decoder =
            SliceStreamDecoder::new(&self.engine).with_max_bytes(self.max_slice_bytes);
        let mut stream = response.into_inner();
        let outcome = loop {
            let next = stream.message().await.map_err(|s| {
                DistribError::Transport(format!("federated fetch to cluster {}: {s}", self.cluster))
            });
            match next {
                Ok(Some(frame)) => {
                    if let Err(err) = decoder.push(frame) {
                        break Err(err);
                    }
                }
                Ok(None) => break Ok(()),
                Err(err) => break Err(err),
            }
        };
        match outcome {
            Ok(()) => decoder.finish(),
            Err(err) => {
                // A federated slice has no `fragments[]` entry to record into
                // (that array is the intra-cluster fan-out's), so the bytes this
                // coordinator was made to hold are reported here and in the
                // typed error's own counts.
                if matches!(
                    err,
                    DistribError::Codec(codec::CodecError::SliceFrameCapExceeded { .. })
                        | DistribError::Codec(codec::CodecError::SliceByteCapExceeded { .. })
                ) {
                    tracing::warn!(
                        cluster = %self.cluster,
                        frames = decoder.frames_consumed(),
                        wire_bytes = decoder.bytes_consumed(),
                        error = %err,
                        "refused a federated slice that exceeded a coordinator decode cap",
                    );
                }
                Err(err)
            }
        }
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

    /// The minting/verify key every test that does not exercise rotation uses.
    const TEST_KEY: [u8; 32] = [0x11; 32];

    /// The single-key configuration for a test service or coordinator.
    fn test_keys() -> Arc<Vec<[u8; 32]>> {
        Arc::new(vec![TEST_KEY])
    }

    /// Mint a capability for the request's own `(tenant, signal, query_id)` with
    /// `expires` under `key`, exactly as a coordinator does. Used to hand a
    /// worker-side `fetch` a valid or a deliberately-mismatched capability.
    fn mint(
        key: &[u8; 32],
        tenant_hash: [u8; 16],
        signal: u32,
        query_id: [u8; 16],
        expires_unix_ns: i64,
    ) -> Vec<u8> {
        codec::mint_capability(
            key,
            &codec::FragmentClaims {
                capability_version: codec::CAPABILITY_VERSION,
                tenant_hash,
                signal,
                query_id,
                expires_unix_ns,
            },
        )
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
    /// pinned-scope tests only exercise capability verification, never the
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
        let admission = AdmissionClasses::new(8, 8, metrics.clone());
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        FragmentService::new(
            test_keys(),
            empty_resolver(),
            admission,
            catalog,
            store,
            None,
            clock,
            metrics,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
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
        let classes = AdmissionClasses::new(1, 8, metrics.clone());
        let admission = classes.for_class(AdmissionClass::Pinned).clone();

        let first = admission.acquire().await.expect("first permit");
        assert_eq!(metrics.fragment_inflight(AdmissionClass::Pinned), 1);

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
        assert_eq!(metrics.fragment_inflight(AdmissionClass::Pinned), 0);
        assert_eq!(
            metrics.fragment_admission_waits_total(AdmissionClass::Pinned),
            1,
            "the queued waiter recorded exactly one admission wait"
        );
    }

    /// A worker-side `FragmentService` with a fixed clock and the given keys, for
    /// deterministic capability-verification tests (ADR-0071 amendment,
    /// decision 2).
    fn capability_service(
        now_ns: i64,
        keys: Arc<Vec<[u8; 32]>>,
        metrics: Arc<FragmentMetrics>,
    ) -> FragmentService {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        FragmentService::new(
            keys,
            empty_resolver(),
            AdmissionClasses::new(8, 8, metrics.clone()),
            catalog,
            store,
            None,
            Arc::new(FixedClock(now_ns)),
            metrics,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        )
    }

    /// A pinned single-shard request carrying `query_id` and `capability`.
    fn pinned_with_cap(
        tenant_hash: [u8; 16],
        query_id: [u8; 16],
        capability: Vec<u8>,
    ) -> pb::FetchRequest {
        let mut req = pinned_request(tenant_hash, &[0]);
        req.query_id = query_id.to_vec();
        req.fragment_capability = capability;
        req
    }

    /// Drive the worker's `fetch` on a `Pinned` request and decode its frames.
    /// Returns the transport-level `Status` on a rejected capability.
    async fn pinned_fetch(
        service: &FragmentService,
        request: pb::FetchRequest,
    ) -> Result<SliceResponse, tonic::Status> {
        let response = service.fetch(tonic::Request::new(request)).await?;
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream.next().await {
            frames.push(frame.expect("in-crate stream never errors"));
        }
        Ok(decode_slice_frames(frames).expect("frames decode"))
    }

    /// A worker-side `FragmentService` sharing the caller's `metrics` and
    /// `admission` classes, so a test can hold a permit on one class directly
    /// while driving `fetch` through the service.
    fn service_with_admission(
        metrics: Arc<FragmentMetrics>,
        admission: AdmissionClasses,
        resolver: Arc<dyn TenantResolver>,
        now_ns: i64,
    ) -> FragmentService {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        FragmentService::new(
            test_keys(),
            resolver,
            admission,
            catalog,
            store,
            None,
            Arc::new(FixedClock(now_ns)),
            metrics,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        )
    }

    /// issue #1722. A cross-cluster
    /// federation coordinator saturating the `Resolve` class (here bounded to
    /// 1) must never delay this cluster's own `Pinned` fetches: the two
    /// classes are independent semaphores, so a `Pinned` fetch is admitted
    /// and completes well inside the bounded timeout even while a `Resolve`
    /// permit is held forever.
    #[tokio::test]
    async fn pinned_fetch_admitted_while_resolve_class_is_saturated() {
        let now = 1_000;
        let metrics = Arc::new(FragmentMetrics::new());
        let admission = AdmissionClasses::new(8, 1, metrics.clone());
        let service =
            service_with_admission(metrics.clone(), admission.clone(), empty_resolver(), now);

        // Saturate and permanently hold the Resolve class: a peer cluster
        // driving federation reads at the cap, forever.
        let _resolve_permit = admission
            .for_class(AdmissionClass::Resolve)
            .acquire()
            .await
            .expect("resolve permit acquired");

        let tenant = [3u8; 16];
        let query = [4u8; 16];
        let cap = mint(&TEST_KEY, tenant, metrics_signal(), query, now + 1_000);

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            pinned_fetch(&service, pinned_with_cap(tenant, query, cap)),
        )
        .await
        .expect("a saturated Resolve class must not starve a Pinned fetch");
        outcome.expect("the Pinned fetch is admitted and served");
    }

    /// The inverse of the disjointness property above: a `Pinned` backlog
    /// saturating its own class must never delay a `Resolve` (federation)
    /// fetch.
    #[tokio::test]
    async fn resolve_fetch_admitted_while_pinned_class_is_saturated() {
        let now = 1_000;
        let metrics = Arc::new(FragmentMetrics::new());
        let admission = AdmissionClasses::new(1, 8, metrics.clone());
        let tokens: std::collections::HashMap<String, ravel_types::TenantId> =
            std::collections::HashMap::from([(
                "federation-token".to_string(),
                ravel_types::TenantId::new("tenant-a".to_string()),
            )]);
        let resolver: Arc<dyn TenantResolver> =
            Arc::new(ravel_query::http::StaticBearerTokenResolver::new(tokens));
        let service = service_with_admission(metrics.clone(), admission.clone(), resolver, now);

        // Saturate and permanently hold the Pinned class: an intra-cluster
        // fan-out backlog at the cap, forever.
        let _pinned_permit = admission
            .for_class(AdmissionClass::Pinned)
            .acquire()
            .await
            .expect("pinned permit acquired");

        let request = resolve_request(TenantHash([9u8; 16]), now);
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            fetch_decoded(&service, request, "federation-token"),
        )
        .await
        .expect("a saturated Pinned class must not starve a Resolve fetch");
        outcome.expect("the Resolve fetch is admitted and served");
    }

    /// A valid capability naming the request's own tenant, signal, and query is
    /// accepted: the fetch is admitted (no `Unauthenticated`), the request is
    /// counted, and no reject counter fires. (An empty store yields a non-Ok
    /// slice status, which is orthogonal to authorization.)
    #[tokio::test]
    async fn valid_capability_is_accepted() {
        let now = 1_000;
        let metrics = Arc::new(FragmentMetrics::new());
        let service = capability_service(now, test_keys(), metrics.clone());
        let tenant = [1u8; 16];
        let query = [2u8; 16];
        let cap = mint(&TEST_KEY, tenant, metrics_signal(), query, now + 1_000);

        pinned_fetch(&service, pinned_with_cap(tenant, query, cap))
            .await
            .expect("valid capability is accepted, not an Unauthenticated status");
        assert_eq!(
            metrics.fragment_requests_total(),
            1,
            "the request was served"
        );
        for reason in CapabilityReject::ALL {
            assert_eq!(
                metrics.capability_rejects(reason),
                0,
                "no reject fires on a valid capability"
            );
        }
    }

    /// Each reject reason fires independently, increments only its own labeled
    /// counter, refuses the request with `Unauthenticated`, and short-circuits
    /// before the request is served (so `fragment_requests_total` stays 0).
    #[tokio::test]
    async fn each_capability_reject_reason_is_labeled_and_counted() {
        let now = 1_000;
        let tenant = [1u8; 16];
        let query = [2u8; 16];
        let signal = metrics_signal();

        // A distinct (service, request) per reason so counters do not interleave.
        struct Case {
            reason: CapabilityReject,
            request: pb::FetchRequest,
        }
        // missing: no capability at all.
        let missing = pinned_with_cap(tenant, query, Vec::new());
        // bad MAC: a valid capability with one flipped byte.
        let mut tampered = mint(&TEST_KEY, tenant, signal, query, now + 1_000);
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        let bad_mac = pinned_with_cap(tenant, query, tampered);
        // expired: expiry at the clock (rejected as `<=`).
        let expired = pinned_with_cap(tenant, query, mint(&TEST_KEY, tenant, signal, query, now));
        // tenant mismatch: capability names a different tenant.
        let tenant_mismatch = pinned_with_cap(
            [9u8; 16],
            query,
            mint(&TEST_KEY, tenant, signal, query, now + 1_000),
        );
        // query mismatch: capability names a different query id.
        let query_mismatch = pinned_with_cap(
            tenant,
            [8u8; 16],
            mint(&TEST_KEY, tenant, signal, query, now + 1_000),
        );

        let cases = [
            Case {
                reason: CapabilityReject::Missing,
                request: missing,
            },
            Case {
                reason: CapabilityReject::BadMac,
                request: bad_mac,
            },
            Case {
                reason: CapabilityReject::Expired,
                request: expired,
            },
            Case {
                reason: CapabilityReject::TenantMismatch,
                request: tenant_mismatch,
            },
            Case {
                reason: CapabilityReject::QueryMismatch,
                request: query_mismatch,
            },
        ];

        for case in cases {
            let metrics = Arc::new(FragmentMetrics::new());
            let service = capability_service(now, test_keys(), metrics.clone());
            let err = pinned_fetch(&service, case.request)
                .await
                .expect_err("a mismatched capability is rejected");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
            assert_eq!(
                metrics.capability_rejects(case.reason),
                1,
                "{} reject fires exactly once",
                case.reason.reason()
            );
            // Only the expected reason fired.
            for other in CapabilityReject::ALL {
                if other != case.reason {
                    assert_eq!(
                        metrics.capability_rejects(other),
                        0,
                        "{} must not fire for a {} case",
                        other.reason(),
                        case.reason.reason()
                    );
                }
            }
            assert_eq!(
                metrics.fragment_requests_total(),
                0,
                "a rejected capability is refused before the request is served"
            );
        }
    }

    /// The epic's F-1 reproduction, now a permanent test: a capability MINTED for
    /// tenant A, presented on a `Pinned` fetch that NAMES tenant B, is rejected
    /// as a tenant mismatch before any snapshot resolve or admission
    /// (`fragment_requests_total` stays 0). This is the concrete cross-tenant
    /// proof: a held capability for one tenant grants no read of another.
    #[tokio::test]
    async fn capability_for_tenant_a_cannot_fetch_tenant_b() {
        let now = 1_000;
        let tenant_a = [0xAAu8; 16];
        let tenant_b = [0xBBu8; 16];
        let query = [7u8; 16];
        let metrics = Arc::new(FragmentMetrics::new());
        let service = capability_service(now, test_keys(), metrics.clone());

        // A genuine, MAC-valid, unexpired capability for tenant A.
        let cap_for_a = mint(&TEST_KEY, tenant_a, metrics_signal(), query, now + 1_000);
        // Present it on a fetch that names tenant B on the wire.
        let request = pinned_with_cap(tenant_b, query, cap_for_a);

        let err = pinned_fetch(&service, request)
            .await
            .expect_err("a tenant-A capability cannot authorize a tenant-B fetch");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert_eq!(
            metrics.capability_rejects(CapabilityReject::TenantMismatch),
            1,
            "the cross-tenant attempt is a tenant mismatch"
        );
        assert_eq!(
            metrics.fragment_requests_total(),
            0,
            "rejected before any snapshot resolve or admission"
        );
    }

    /// Key rotation: a file with two keys mints under the first but verifies a
    /// capability minted under EITHER, so a rolling rotation needs no flag day.
    #[tokio::test]
    async fn rotation_verifies_capability_under_any_configured_key() {
        let now = 1_000;
        let key_new = [0x22u8; 32];
        let key_old = [0x33u8; 32];
        let keys = Arc::new(vec![key_new, key_old]);
        let tenant = [4u8; 16];
        let query = [5u8; 16];
        let signal = metrics_signal();

        // Minted under the new (first) key: accepted.
        let metrics = Arc::new(FragmentMetrics::new());
        let service = capability_service(now, keys.clone(), metrics.clone());
        pinned_fetch(
            &service,
            pinned_with_cap(
                tenant,
                query,
                mint(&key_new, tenant, signal, query, now + 1_000),
            ),
        )
        .await
        .expect("capability under the minting key verifies");
        assert_eq!(metrics.capability_rejects(CapabilityReject::BadMac), 0);

        // Minted under the old (still-listed) key: also accepted, so an in-flight
        // capability survives the roll that prepends the new key.
        let metrics = Arc::new(FragmentMetrics::new());
        let service = capability_service(now, keys, metrics.clone());
        pinned_fetch(
            &service,
            pinned_with_cap(
                tenant,
                query,
                mint(&key_old, tenant, signal, query, now + 1_000),
            ),
        )
        .await
        .expect("capability under a retained rotation key verifies");
        assert_eq!(metrics.capability_rejects(CapabilityReject::BadMac), 0);
    }

    /// The coordinator mints under the FIRST key: a `RoutingSliceFetcher` holding
    /// `[key_new, key_old]` produces a capability that verifies under `key_new`,
    /// never `key_old`. Pairs with the verify-any-key rotation test above.
    #[test]
    fn coordinator_mints_under_the_first_key() {
        let key_new = [0x22u8; 32];
        let key_old = [0x33u8; 32];
        let metrics = Arc::new(FragmentMetrics::new());
        let fetcher = RoutingSliceFetcher::new(
            Arc::new(OnceLock::new()),
            Arc::new(RwLock::new(Arc::new(Vec::new()))),
            Arc::new(vec![key_new, key_old]),
            test_service(metrics.clone()),
            metrics,
        );
        let tenant = [4u8; 16];
        let query = [5u8; 16];
        let mut request = pinned_with_cap(tenant, query, Vec::new());
        request.deadline_unix_ns = 9_000;
        let minted = fetcher
            .mint_capability(&request)
            .expect("coordinator mints for a well-formed request");
        let (claims, mac) = codec::decode_capability(&minted).expect("decode");
        assert_eq!(claims.tenant_hash, tenant);
        assert_eq!(claims.query_id, query);
        assert_eq!(
            claims.expires_unix_ns, 9_000,
            "expiry is the query deadline"
        );
        assert_eq!(
            codec::capability_mac(&key_new, &claims),
            mac,
            "minted under the first key"
        );
        assert_ne!(
            codec::capability_mac(&key_old, &claims),
            mac,
            "not under the second key"
        );
    }

    /// A worker reporting the version below the coordinator's is excluded
    /// from `ranked_owners`'s candidate set: the EXISTING version-skew filter
    /// (`record.protocol_version == codec::PROTOCOL_VERSION`) drops it at
    /// routing time, so its slices run coordinator-local with no round trip.
    /// A version-matched worker at the same endpoint is kept, proving the
    /// exclusion is the version filter, not an unrelated routing miss.
    #[test]
    fn version_skewed_worker_is_dropped_from_ranked_owners() {
        let metrics = Arc::new(FragmentMetrics::new());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");

        let make = |version: u32| {
            let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: "192.0.2.1:9".to_string(),
                flight_sql_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: version,
                started_unix_ns: 0,
            }])));
            RoutingSliceFetcher::new(
                self_cell.clone(),
                live,
                test_keys(),
                test_service(metrics.clone()),
                metrics.clone(),
            )
        };

        // Only a skewed (one-below-current) worker in the set: it is filtered
        // out, no owner remains, so the slice routes local (empty ranked list).
        let skewed = make(codec::PROTOCOL_VERSION - 1);
        assert!(
            skewed
                .ranked_owners(&pinned_request([9u8; 16], &[0]))
                .is_empty(),
            "a version-skewed worker is excluded, leaving no remote owner"
        );

        // The same worker at the current version is kept as a remote owner:
        // the exclusion above is the version filter, not the worker being
        // unroutable for another reason.
        let matched = make(codec::PROTOCOL_VERSION);
        assert_eq!(
            matched.ranked_owners(&pinned_request([9u8; 16], &[0])),
            vec![Owner::Remote("192.0.2.1:9".to_string())],
            "a version-matched worker is a remote owner"
        );
    }

    #[tokio::test]
    async fn routing_runs_local_when_live_set_empty() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let fetcher = RoutingSliceFetcher::new(
            Arc::new(OnceLock::new()),
            Arc::new(RwLock::new(Arc::new(Vec::new()))),
            test_keys(),
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
                flight_sql_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                // Reserved-for-docs TEST-NET address that never accepts a
                // connection, so any slice mapped here fails at transport.
                fragment_endpoint: "192.0.2.1:9".to_string(),
                flight_sql_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher =
            RoutingSliceFetcher::new(self_cell, live, test_keys(), service, metrics.clone());

        // Pinned BEFORE the loop runs, on purpose. Once the loop's first
        // dead-mapped tenant quarantines the endpoint, the remote drops out of
        // every ranking and `ranked_owners` answers `SelfLocal` for every
        // tenant, so a self-mapped tenant found afterwards proves nothing. This
        // one ranks self ahead of a live, un-quarantined remote.
        let self_mapped = (0..1024u128)
            .map(|i| uuid::Uuid::from_u128(i).into_bytes())
            .find(|tenant| {
                matches!(
                    fetcher
                        .ranked_owners(&pinned_request(*tenant, &[0]))
                        .first(),
                    Some(Owner::SelfLocal)
                )
            })
            .expect("some tenant in 1024 ranks self first under rendezvous hashing");

        // Drive the self-mapped unit BEFORE the loop, while the remote is still
        // live and un-quarantined. That ordering is the whole point: once the
        // loop quarantines the dead endpoint, a quarantine-skipped unit also
        // runs locally without a fallback, so neither `saw_local` nor these
        // assertions could tell a self-mapped unit from a skipped one. Run
        // first, and "ran locally, never attempted a remote" means self-mapped.
        let local_before = metrics.slices_local_total();
        let fallback_before = metrics.slices_fallback_total();
        fetcher
            .fetch(pinned_request(self_mapped, &[0]))
            .await
            .expect("a self-mapped unit runs locally");
        assert_eq!(
            metrics.slices_local_total(),
            local_before + 1,
            "the pinned self-mapped unit ran locally"
        );
        assert_eq!(
            metrics.slices_fallback_total(),
            fallback_before,
            "a self-mapped unit is not a fallback: it never attempted a remote"
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
        let mut fetched = 1u64; // the pinned self-mapped fetch above
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
            "at least one unit ran locally (self-mapped or quarantine-skipped)"
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
                flight_sql_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: "192.0.2.1:9".to_string(),
                flight_sql_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live.clone(),
            test_keys(),
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
            flight_sql_endpoint: "127.0.0.1:1".to_string(),
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

    /// Dead-endpoint quarantine (ADR-0071 amendment, decision 3), driven end to
    /// end through the production routing path (`fetch` -> `ranked_owners` ->
    /// `dispatch`), not through a unit call on the map:
    ///
    /// 1. The first query to a slice that rendezvous-maps to an unreachable
    ///    endpoint dispatches, fails at transport, falls back local, and marks
    ///    the endpoint quarantined at its live-view stamp.
    /// 2. The second query to the same slice routes local with ZERO dial to that
    ///    endpoint (asserted as the absence of a remote attempt / fallback, the
    ///    whole point of the feature), because `ranked_owners` skips the
    ///    quarantined endpoint.
    /// 3. A strictly newer heartbeat stamp readmits the endpoint: the next query
    ///    ranks it again and dials it (falling back once more, since it is still
    ///    unreachable in this test).
    /// 4. The mark/readmit metrics move exactly as those steps imply.
    ///
    /// To watch step 2 fail against the pre-change behavior, flip
    /// `quarantine_skips`'s `Some(_) => true` arm to `Some(_) => false`: the
    /// endpoint is then never skipped, so the second query re-dials and pays the
    /// connect timeout again (`slices_fallback_total` increments, no local run),
    /// and the step-2 assertions below fail.
    #[tokio::test]
    async fn quarantine_skips_dead_endpoint_then_readmits_on_fresh_stamp() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let dead_endpoint = "192.0.2.1:9";
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        // Self plus one unreachable remote worker, both version-matched. The
        // remote's fragment endpoint is a reserved-for-docs TEST-NET address that
        // never accepts a connection, so any slice mapped to it fails at
        // transport (bounded by REMOTE_CONNECT_TIMEOUT).
        let self_record = QueryWorkerRecord {
            process_id: self_id.to_string(),
            fragment_endpoint: "127.0.0.1:1".to_string(),
            flight_sql_endpoint: "127.0.0.1:1".to_string(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        };
        let live: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>> = Arc::new(RwLock::new(Arc::new(vec![
            self_record.clone(),
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: dead_endpoint.to_string(),
                flight_sql_endpoint: dead_endpoint.to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live.clone(),
            test_keys(),
            service,
            metrics.clone(),
        );

        // Step 1: find a tenant whose unit maps to the dead remote (proven by a
        // transport fallback) and, in the same query, quarantine it. We break on
        // the first fallback, so exactly one endpoint is marked.
        let mut mapped_to_dead: Option<[u8; 16]> = None;
        for i in 0..256u128 {
            let tenant = uuid::Uuid::from_u128(i).into_bytes();
            let before = metrics.slices_fallback_total();
            fetcher
                .fetch(pinned_request(tenant, &[0]))
                .await
                .expect("fetch resolves via local fallback");
            if metrics.slices_fallback_total() > before {
                mapped_to_dead = Some(tenant);
                break;
            }
        }
        let tenant = mapped_to_dead.expect("some unit maps to the dead remote worker");
        assert_eq!(
            metrics.quarantine_marks_total(),
            1,
            "the failed dispatch marked exactly one endpoint"
        );
        assert_eq!(
            metrics.quarantine_current(),
            1,
            "one endpoint is currently quarantined"
        );
        assert_eq!(metrics.quarantine_readmits_total(), 0);

        // Step 2: a second query to the SAME slice must route local with no dial
        // to the dead endpoint. Absence of a dial is the property under test, so
        // assert no remote attempt and no fallback occurred, only a local run.
        let fallback_before = metrics.slices_fallback_total();
        let remote_before = metrics.slices_remote_total();
        let local_before = metrics.slices_local_total();
        fetcher
            .fetch(pinned_request(tenant, &[0]))
            .await
            .expect("second query resolves");
        assert_eq!(
            metrics.slices_fallback_total(),
            fallback_before,
            "the quarantined endpoint is not dialed, so no fallback is paid"
        );
        assert_eq!(
            metrics.slices_remote_total(),
            remote_before,
            "no remote attempt is made to the quarantined endpoint"
        );
        assert_eq!(
            metrics.slices_local_total(),
            local_before + 1,
            "the slice routes local instead"
        );
        assert_eq!(
            metrics.quarantine_marks_total(),
            1,
            "no re-mark: the skip happens before any dispatch"
        );
        assert_eq!(metrics.quarantine_readmits_total(), 0);
        // The skip must not mutate the quarantine map: a mere skip is neither a
        // readmit nor a prune, so the exported `ravel_distrib_quarantine_current`
        // gauge stays at 1. Without this the gauge could silently drift from the
        // map (e.g. a skip that erroneously cleared the entry) and every later
        // step would still pass, since routing-local is the same either way.
        assert_eq!(
            metrics.quarantine_current(),
            1,
            "a skip leaves the endpoint quarantined; the gauge is unchanged"
        );

        // Step 3: advance the live view with a strictly newer started_unix_ns for
        // the dead endpoint. The next query readmits and ranks it again, so it is
        // dialed once more (and falls back, since it is still unreachable here).
        *live.write() = Arc::new(vec![
            self_record,
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: dead_endpoint.to_string(),
                flight_sql_endpoint: dead_endpoint.to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 1_000,
            },
        ]);
        let fallback_before = metrics.slices_fallback_total();
        fetcher
            .fetch(pinned_request(tenant, &[0]))
            .await
            .expect("query after readmission resolves");
        assert_eq!(
            metrics.quarantine_readmits_total(),
            1,
            "the strictly newer heartbeat stamp readmitted the endpoint"
        );
        assert_eq!(
            metrics.slices_fallback_total(),
            fallback_before + 1,
            "the readmitted endpoint is ranked again and dialed (then falls back)"
        );
        assert_eq!(
            metrics.quarantine_marks_total(),
            2,
            "still unreachable, so the readmitted endpoint is re-quarantined"
        );
        assert_eq!(
            metrics.quarantine_current(),
            1,
            "re-quarantined at the newer stamp"
        );
    }

    /// The quarantine counters reach the `/metrics` exposition from their real
    /// source (#269, ADR-0071 amendment decision 3). A genuine transport-failure
    /// mark on a real [`RoutingSliceFetcher`] drives [`FragmentMetrics`], and the
    /// same [`crate::metrics::DistribSnapshot::from_metrics`] mapping the scrape
    /// handler uses then feeds [`crate::metrics::render`], whose output must carry
    /// the three quarantine series. A hand-built snapshot could pass even if the
    /// exporter never read `FragmentMetrics`; this drives that read end to end.
    #[tokio::test]
    async fn quarantine_counters_reach_the_metrics_renderer() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let dead_endpoint = "192.0.2.1:9";
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        let live: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>> = Arc::new(RwLock::new(Arc::new(vec![
            QueryWorkerRecord {
                process_id: self_id.to_string(),
                fragment_endpoint: "127.0.0.1:1".to_string(),
                flight_sql_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: dead_endpoint.to_string(),
                flight_sql_endpoint: dead_endpoint.to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live.clone(),
            test_keys(),
            service,
            metrics.clone(),
        );

        // Drive a real transport-failure quarantine mark: find a tenant whose
        // unit maps to the unreachable remote and stop on the first fallback.
        for i in 0..256u128 {
            let tenant = uuid::Uuid::from_u128(i).into_bytes();
            let before = metrics.slices_fallback_total();
            fetcher
                .fetch(pinned_request(tenant, &[0]))
                .await
                .expect("fetch resolves via local fallback");
            if metrics.slices_fallback_total() > before {
                break;
            }
        }
        assert_eq!(
            metrics.quarantine_marks_total(),
            1,
            "the transport failure marked exactly one endpoint"
        );
        assert_eq!(metrics.quarantine_current(), 1);

        // The handler's real source-to-snapshot mapping picks up the quarantine
        // counters.
        let snapshot = crate::metrics::DistribSnapshot::from_metrics(&metrics);
        assert_eq!(snapshot.quarantine_marks_total, 1);
        assert_eq!(snapshot.quarantine_current, 1);

        // And the full render entry point emits them.
        let body = crate::metrics::render(
            crate::config::Mode::Query,
            &Default::default(),
            &[],
            &crate::metrics::CatalogCountersSnapshot::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &crate::metrics::AdmissionCountersSnapshot::default(),
            &[],
            0,
            crate::metrics::IngestBufferBudgetSnapshot::default(),
            Some(&snapshot),
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            crate::metrics::MemoryBudgetSnapshot::default(),
            true,
        );
        assert!(
            body.contains("ravel_distrib_quarantine_marks_total{mode=\"query\"} 1"),
            "the real quarantine mark must reach the exposition:\n{body}"
        );
        assert!(
            body.contains("ravel_distrib_quarantine_current{mode=\"query\"} 1"),
            "the real currently-quarantined gauge must reach the exposition:\n{body}"
        );
    }

    /// Prune (ADR-0071 amendment, decision 3): a quarantined endpoint that
    /// leaves the live view entirely is dropped from the map opportunistically
    /// on the next ranking pass, bounding the map by the historical worker set.
    /// Such an endpoint is never ranked anyway, so this is not counted as a
    /// readmit.
    #[tokio::test]
    async fn quarantine_prunes_endpoint_absent_from_live_view() {
        let metrics = Arc::new(FragmentMetrics::new());
        let service = test_service(metrics.clone());
        let self_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let dead_endpoint = "192.0.2.1:9";
        let self_cell = Arc::new(OnceLock::new());
        self_cell.set(self_id).expect("set self id");
        let self_record = QueryWorkerRecord {
            process_id: self_id.to_string(),
            fragment_endpoint: "127.0.0.1:1".to_string(),
            flight_sql_endpoint: "127.0.0.1:1".to_string(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        };
        let live: Arc<RwLock<Arc<Vec<QueryWorkerRecord>>>> = Arc::new(RwLock::new(Arc::new(vec![
            self_record.clone(),
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: dead_endpoint.to_string(),
                flight_sql_endpoint: dead_endpoint.to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher = RoutingSliceFetcher::new(
            self_cell,
            live.clone(),
            test_keys(),
            service,
            metrics.clone(),
        );

        // Quarantine the dead endpoint (a query whose unit maps to it falls back).
        for i in 0..256u128 {
            let tenant = uuid::Uuid::from_u128(i).into_bytes();
            let before = metrics.slices_fallback_total();
            fetcher
                .fetch(pinned_request(tenant, &[0]))
                .await
                .expect("fetch");
            if metrics.slices_fallback_total() > before {
                break;
            }
        }
        assert_eq!(metrics.quarantine_current(), 1, "endpoint quarantined");

        // The worker leaves the live set entirely (aged out). The next ranking
        // pass prunes its stale quarantine entry, and no readmit is counted.
        *live.write() = Arc::new(vec![self_record]);
        let _ = fetcher.ranked_owners(&pinned_request([0u8; 16], &[0]));
        assert_eq!(
            metrics.quarantine_current(),
            0,
            "the absent endpoint's quarantine entry is pruned"
        );
        assert_eq!(
            metrics.quarantine_readmits_total(),
            0,
            "a prune is not a readmit"
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
        let workers = QueryWorkers::with_defaults(
            "127.0.0.1:7000",
            "127.0.0.1:7100",
            codec::PROTOCOL_VERSION,
        );
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
        let observer = QueryWorkers::with_defaults(
            "127.0.0.1:7001",
            "127.0.0.1:7101",
            codec::PROTOCOL_VERSION,
        );
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
                flight_sql_endpoint: "127.0.0.1:1".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
            QueryWorkerRecord {
                process_id: other_id.to_string(),
                fragment_endpoint: "192.0.2.1:9".to_string(),
                flight_sql_endpoint: "192.0.2.1:9".to_string(),
                protocol_version: codec::PROTOCOL_VERSION,
                started_unix_ns: 0,
            },
        ])));
        let fetcher =
            RoutingSliceFetcher::new(self_cell, live, test_keys(), service, metrics.clone());

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

    // --- Cross-cluster federation auth (ADR-0071 security) ------------

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
            test_keys(),
            resolver,
            AdmissionClasses::new(8, 8, metrics.clone()),
            catalog,
            store,
            None,
            Arc::new(FixedClock(now_ns)),
            metrics,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
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
        Ok(decode_slice_frames(frames).expect("frames decode to a slice response"))
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

    /// A worker clamps the wire budget to its own `EngineConfig` on the resolve
    /// path: the wire sends `max_bytes_scanned: 0`, the "no cap" sentinel, and
    /// the worker still refuses at its own 1-byte limit. The refusal is the
    /// same typed `TooManyBytesScanned` string the local path renders, and the
    /// summary carries the bytes actually spent so the coordinator folds the
    /// real cost.
    ///
    /// Flip-line proof: in `slice_byte_limit` return `ByteLimit::Unlimited`
    /// instead of `worker` for the `Some(0) | None` arm and the status is `Ok`,
    /// not `BudgetExceeded`.
    #[tokio::test]
    async fn resolve_scope_clamps_wire_budget_to_local_engine_config() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("budget-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;

        let service = federation_service(store, &[("operator-cred", "budget-tenant")], 4 * HOUR_NS)
            .with_engine_config(ravel_query::EngineConfig {
                max_bytes_scanned: ravel_query::ByteLimit::Bounded(1),
                ..ravel_query::EngineConfig::default()
            });

        // The wire carries the unbounded sentinel on every cap.
        let mut request = resolve_request(tenant.hash(), 4 * HOUR_NS);
        request.budgets = Some(pb::Budgets {
            max_series: 0,
            max_samples: 0,
            max_bytes_scanned: 0,
            max_segments: 0,
        });

        let response = fetch_decoded(&service, request, "operator-cred")
            .await
            .expect("valid tenant credential is accepted");

        assert_eq!(
            response.status,
            pb::status::Code::BudgetExceeded,
            "the worker's own 1-byte limit refuses the slice even though the wire \
             budget is the unbounded sentinel (got {:?}: {})",
            response.status,
            response.status_message
        );
        let spent = response.accounting.total_s3_bytes();
        assert!(
            spent > 1,
            "the refusal must report the bytes actually scanned, got {spent}"
        );
        assert_eq!(
            response.status_message,
            format!("query scanned {spent} bytes, exceeding the budget of 1"),
            "the worker renders the same typed TooManyBytesScanned string the \
             local path does, against its OWN limit"
        );
        assert_eq!(response.series_returned, 0);
    }

    /// The same clamp covers the count caps on the resolve path: the worker
    /// enforces its own `EngineConfig::max_series` over what the slice is about
    /// to return, with the wire budget unset entirely. One published series
    /// against a zero-series worker limit refuses with the local path's typed
    /// `TooManySeries` string.
    ///
    /// Flip-line proof: delete the `resolve_scope_count_refusal` call in
    /// `run_slice_metrics` and the status is `Ok` with one series returned.
    #[tokio::test]
    async fn resolve_scope_enforces_local_series_cap() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("series-cap-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;

        let service = federation_service(
            store,
            &[("operator-cred", "series-cap-tenant")],
            4 * HOUR_NS,
        )
        .with_engine_config(ravel_query::EngineConfig {
            max_series: 0,
            ..ravel_query::EngineConfig::default()
        });

        // `budgets` is None here: the worker's own config is the only cap.
        let response = fetch_decoded(
            &service,
            resolve_request(tenant.hash(), 4 * HOUR_NS),
            "operator-cred",
        )
        .await
        .expect("valid tenant credential is accepted");

        assert_eq!(
            response.status,
            pb::status::Code::BudgetExceeded,
            "the worker's own max_series refuses the slice (got {:?}: {})",
            response.status,
            response.status_message
        );
        assert_eq!(
            response.status_message, "query matched 1 series, exceeding the limit of 0",
            "the exact series count and the worker's own limit are both reported"
        );
        assert_eq!(response.series_returned, 0);
    }

    /// The count enforcement is scoped to the resolve path only. An
    /// intra-cluster pinned slice is one shard-major piece of a query whose
    /// counts the coordinator folds and re-checks, so the same zero-series
    /// worker limit must NOT refuse it: enforcing per-slice there would fail a
    /// query that is under its own total.
    ///
    /// Flip-line proof: drop the `if !self.resolve_scope { return None; }` guard
    /// at the top of `resolve_scope_count_refusal` and this pinned slice returns
    /// `BudgetExceeded` instead of its one series.
    #[tokio::test]
    async fn pinned_scope_does_not_enforce_the_local_series_cap() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("pinned-cap-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;

        let service = pinned_service(store, now).with_engine_config(ravel_query::EngineConfig {
            max_series: 0,
            ..ravel_query::EngineConfig::default()
        });
        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let response = service
            .run_local(pinned_over_window(tenant.hash(), &seg, envelope))
            .await
            .expect("local run");

        assert_eq!(
            response.status,
            pb::status::Code::Ok,
            "a pinned slice is not a whole query: its counts fold at the \
             coordinator (got {:?}: {})",
            response.status,
            response.status_message
        );
        assert_eq!(response.series_returned, 1);
    }

    // --- Windowed fragment resolve ------------------------------------------

    /// A `FragmentService` over `store` on the intra-cluster pinned path: a
    /// fixed clock and no tenant credentials (the pinned path only verifies the
    /// fragment capability).
    fn pinned_service(store: Arc<dyn ObjectStoreBackend>, now_ns: i64) -> FragmentService {
        let metrics = Arc::new(FragmentMetrics::new());
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        FragmentService::new(
            test_keys(),
            empty_resolver(),
            AdmissionClasses::new(8, 8, metrics.clone()),
            catalog,
            store,
            None,
            Arc::new(FixedClock(now_ns)),
            metrics,
            Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        )
    }

    /// Resolve the tenant's single published segment (over the full window) so a
    /// test can pin it by its real content-hash identity, exactly as the
    /// coordinator would.
    async fn only_segment(
        store: &Arc<dyn ObjectStoreBackend>,
        tenant_hash: TenantHash,
        now_ns: i64,
    ) -> ravel_catalog::SegmentRef {
        let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
        let snapshot = catalog
            .resolve(&tenant_hash, Signal::Metrics, FULL, &[], now_ns)
            .await
            .expect("resolve");
        snapshot
            .segments
            .into_iter()
            .next()
            .expect("one published segment")
    }

    /// The whole timestamp domain, wider than the event-time window a worker
    /// resolves over. Used here only as the test oracle
    /// (a full-window resolve) and to name a disjoint window.
    const FULL: TimeRange = TimeRange {
        start_ns: i64::MIN,
        end_ns: i64::MAX,
    };

    /// A pinned single-segment request carrying `window`, mirroring what
    /// [`ravel_query::distrib`]'s coordinator dispatches.
    fn pinned_over_window(
        tenant_hash: TenantHash,
        seg: &ravel_catalog::SegmentRef,
        window: TimeRange,
    ) -> pb::FetchRequest {
        pb::FetchRequest {
            protocol_version: codec::PROTOCOL_VERSION,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: metrics_signal(),
            scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                segments: vec![codec::encode_segment_identity(seg)],
            })),
            window_start_ns: window.start_ns,
            window_end_ns: window.end_ns,
            ..Default::default()
        }
    }

    /// The worker resolves its content-hash resolver over the request's window,
    /// not the whole history: a window disjoint from a pinned segment's event
    /// range bounds the resolve away from it, so the pin no longer resolves and
    /// the slice reports `SnapshotInvalidated`. A covering window (the segment's
    /// own event envelope, what the coordinator ships) still finds it. Under the
    /// old whole-history resolve the disjoint window would have found the segment
    /// too, so this proves the resolve is bounded to the query window.
    #[tokio::test]
    async fn windowed_resolve_is_bounded_to_the_request_window() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("windowed-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let service = pinned_service(store, now);

        // Covering window = the segment's own event envelope: the pin resolves.
        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let covering = service
            .run_local(pinned_over_window(tenant.hash(), &seg, envelope))
            .await
            .expect("local run");
        assert_eq!(covering.status, pb::status::Code::Ok);
        assert_eq!(covering.series_returned, 1);

        // A window entirely after the segment's event range: the resolve is
        // bounded away from it, so the pin is not found.
        let disjoint = TimeRange {
            start_ns: 3 * HOUR_NS,
            end_ns: 4 * HOUR_NS,
        };
        let missed = service
            .run_local(pinned_over_window(tenant.hash(), &seg, disjoint))
            .await
            .expect("local run");
        assert_eq!(
            missed.status,
            pb::status::Code::SnapshotInvalidated,
            "a window disjoint from the pin bounds the resolve away from it"
        );
    }

    /// The narrowed windowed resolve loses no pinned segment: a slice over the
    /// segment's event envelope returns byte-identical rows to the same slice
    /// resolved over the whole history.
    ///
    /// Flip-line proof: in [`FragmentService::build_resolver`], narrow the
    /// `window` passed to `catalog.resolve(..)` so it drops the pin -- e.g.
    /// change `end_ns: request.window_end_ns` to
    /// `end_ns: request.window_start_ns.saturating_sub(1)`. The windowed run then
    /// resolves to `SnapshotInvalidated` with zero rows while the full-window
    /// oracle still returns the sample, so the row assertions below fail.
    #[tokio::test]
    async fn windowed_fragment_returns_same_rows_as_full_window() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("same-rows-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let service = pinned_service(store, now);

        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let windowed = service
            .run_local(pinned_over_window(tenant.hash(), &seg, envelope))
            .await
            .expect("windowed local run");
        let full = service
            .run_local(pinned_over_window(tenant.hash(), &seg, FULL))
            .await
            .expect("full-window local run");

        assert_eq!(windowed.status, pb::status::Code::Ok);
        assert_eq!(full.status, pb::status::Code::Ok);
        assert_eq!(windowed.series_returned, full.series_returned);
        assert_eq!(windowed.samples_returned, full.samples_returned);
        assert_eq!(windowed.scalar.len(), full.scalar.len());
        for (w, f) in windowed.scalar.iter().zip(full.scalar.iter()) {
            assert_eq!(w.series_id, f.series_id, "series id differs");
            assert_eq!(w.timestamps, f.timestamps, "timestamps differ");
            let wb: Vec<u64> = w.values.iter().map(|v| v.to_bits()).collect();
            let fb: Vec<u64> = f.values.iter().map(|v| v.to_bits()).collect();
            assert_eq!(wb, fb, "value bit patterns differ");
        }
    }

    // ---- ADR-0071 amendment decision 1: the dedicated TLS fragment listener ----
    //
    // Operator-provisioned test PEM material, generated offline (EC P-256). The
    // fragment certificate carries a `ravel-fragment` dNSName SAN, the one fixed
    // name a coordinator verifies against, and both the serverAuth and clientAuth
    // extended key usages: the listener now requires a client certificate from
    // the same pinned CA (issue #1690), and one process is both the worker that
    // serves fragments and the coordinator that dials them, so a single key pair
    // has to satisfy both roles. `TEST_FRAGMENT_CA_PEM` signed it, and
    // `TEST_FRAGMENT_OTHER_CA_PEM` is an unrelated CA used to prove a cert that
    // does not chain to the pinned CA is refused at the TLS layer. Ravel mints no
    // certificates; these stand in for what an operator provisions.
    const TEST_FRAGMENT_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBiDCCAS6gAwIBAgIURqm7z2RSSm9YMcJrjixkBTS6iSAwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQt
dGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABOynQpfkkGc1dJv+181e
8I9uvBML0AvXJo95Z4dxje72IOA/Hhh4cpQ0EQfogGW4LtnbWS7NgilX1+RpC6gG
CVajQjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQW
BBT7C6f70yaChMoXGTIBoZSw+8p3TTAKBggqhkjOPQQDAgNIADBFAiBXCp1E6E6i
I3VH8wGfxQxywXkuQ86dVH5Z7FpTA9udVAIhAJT+wKFJo9hWpeKKbEmbtuuwfaok
5axPjJ9kiO1C6ZIu
-----END CERTIFICATE-----
";
    const TEST_FRAGMENT_SERVER_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIB2jCCAYCgAwIBAgIUX3yTIiYvkWMVICeYkAoWQ/cpCEYwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWcmF2ZWwtZnJhZ21lbnQtdGVzdC1jYTAgFw0yNjA5MTkyMTIx
MzVaGA8yMTI2MDgyNjIxMjEzNVowGTEXMBUGA1UEAwwOcmF2ZWwtZnJhZ21lbnQw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFEo4GbMIGY
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUF
BwMBBggrBgEFBQcDAjAZBgNVHREEEjAQgg5yYXZlbC1mcmFnbWVudDAdBgNVHQ4E
FgQUfJC6GQoihnxgaXOnWiJBAfwInPwwHwYDVR0jBBgwFoAU+wun+9MmgoTKFxky
AaGUsPvKd00wCgYIKoZIzj0EAwIDSAAwRQIgbEMg/jES94eo3dxOwEiM1FiHhY1v
hzdk6C9qmCCckI4CIQC/2tvVzC1VvE9eO0Y9eN2GDp63hSc+5YvKnvFm8P6I6Q==
-----END CERTIFICATE-----
";
    const TEST_FRAGMENT_SERVER_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgonxTB6rEt10ZCoQ+
L1ACQVzux8AvoQAB2A9c890hWPmhRANCAAQHF0p+BdFVa6wOH4/e9vBYV2a3We8/
+XoQmKdUGN8vOrlnREuOj4pqI54CjnYZ1OLRQF3JRynJ5y/yWL+i3rFE
-----END PRIVATE KEY-----
";
    const TEST_FRAGMENT_OTHER_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBrDCCAVGgAwIBAgIULqSB2f/Xm0w0CMNBVGjjXWDp0fowCgYIKoZIzj0EAwIw
IjEgMB4GA1UEAwwXcmF2ZWwtZnJhZ21lbnQtb3RoZXItY2EwIBcNMjYwODE1MDQ0
MzA5WhgPMjEyNjA3MjIwNDQzMDlaMCIxIDAeBgNVBAMMF3JhdmVsLWZyYWdtZW50
LW90aGVyLWNhMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEEWMJQ2Erg2dYe+8q
3g2VkkzeTjAU5MCI4lIvrxTnk2JJK0EnF7UyrFtaNc8WY60+8QXNZMtOdZGUKtqG
C7xceaNjMGEwHQYDVR0OBBYEFEwWujPqBV9auGmo0WLYbhyZ1qqVMB8GA1UdIwQY
MBaAFEwWujPqBV9auGmo0WLYbhyZ1qqVMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0P
AQH/BAQDAgEGMAoGCCqGSM49BAMCA0kAMEYCIQDzu2Xnqcrx1lAxDXuPSTlKQVvV
iFSzkVWOOnkdu5oasgIhAJFMWNwX8xQfZBeOpm6+wokjn/GMaPeQCes2yQ3Zcyir
-----END CERTIFICATE-----
";

    /// Stand up a real TLS-terminating fragment listener over `service` (mounted
    /// with the `DedicatedFragment` role, exactly as `lib.rs` mounts it) on an
    /// ephemeral loopback port. Returns the bound address and a shutdown handle;
    /// dropping the handle without sending stops the task at test end.
    async fn spawn_tls_fragment_listener(
        service: FragmentService,
    ) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
        // The unit tests do not call `start()`, which installs the provider in a
        // real process; install it here too (idempotent, `Err` when already set).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let identity = tonic::transport::Identity::from_pem(
            TEST_FRAGMENT_SERVER_CERT_PEM,
            TEST_FRAGMENT_SERVER_KEY_PEM,
        );
        // Mutual TLS, exactly as `lib.rs` configures it (issue #1690): the
        // pinned CA is both what this listener proves itself with and the
        // roster of clients it will complete a handshake with.
        let tls = tonic::transport::ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(tonic::transport::Certificate::from_pem(
                TEST_FRAGMENT_CA_PEM,
            ));
        let server = tonic::transport::Server::builder()
            .tls_config(tls)
            .expect("server TLS config")
            .add_service(
                service
                    .with_role(FragmentListenerRole::DedicatedFragment)
                    .into_server(),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            server
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await
                .expect("fragment listener serves");
        });
        (addr, tx)
    }

    /// Dial `addr` over TLS with `ca_pem` pinned and `server_name` expected,
    /// presenting the process's own fragment certificate as client identity,
    /// mirroring the coordinator's outbound fragment dial. Eagerly connects, so
    /// a CA or server-name mismatch surfaces here as the TLS handshake failure.
    async fn dial_fragment_tls(
        addr: std::net::SocketAddr,
        ca_pem: &str,
        server_name: &str,
    ) -> Result<Channel, tonic::transport::Error> {
        let tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
            .identity(tonic::transport::Identity::from_pem(
                TEST_FRAGMENT_SERVER_CERT_PEM,
                TEST_FRAGMENT_SERVER_KEY_PEM,
            ))
            .domain_name(server_name);
        dial_fragment_tls_with(addr, tls).await
    }

    /// The same dial with no client identity: the pinned CA is trusted for the
    /// server direction, and nothing is offered in the client direction. This
    /// is what a peer that verified the worker but holds no certificate of its
    /// own looks like on the wire.
    async fn dial_fragment_tls_without_client_cert(
        addr: std::net::SocketAddr,
        ca_pem: &str,
        server_name: &str,
    ) -> Result<Channel, tonic::transport::Error> {
        let tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
            .domain_name(server_name);
        dial_fragment_tls_with(addr, tls).await
    }

    async fn dial_fragment_tls_with(
        addr: std::net::SocketAddr,
        tls: tonic::transport::ClientTlsConfig,
    ) -> Result<Channel, tonic::transport::Error> {
        Channel::from_shared(format!("https://{addr}"))
            .expect("valid uri")
            .connect_timeout(REMOTE_CONNECT_TIMEOUT)
            .tls_config(tls)
            .expect("client TLS config")
            .connect()
            .await
    }

    /// Collect and decode a `SeriesFetch` response stream into a `SliceResponse`.
    async fn collect_fetch(
        response: tonic::Response<tonic::Streaming<pb::FetchResponse>>,
    ) -> SliceResponse {
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream.message().await.expect("stream frame") {
            frames.push(frame);
        }
        decode_slice_frames(frames).expect("frames decode")
    }

    /// Drive `service.fetch` in-process (no metadata) and decode the frames.
    async fn fetch_pinned_inproc(
        service: &FragmentService,
        request: pb::FetchRequest,
    ) -> Result<SliceResponse, tonic::Status> {
        let response = service.fetch(tonic::Request::new(request)).await?;
        let mut frames = Vec::new();
        let mut stream = response.into_inner();
        while let Some(frame) = stream.next().await {
            frames.push(frame.expect("in-crate stream never errors"));
        }
        Ok(decode_slice_frames(frames).expect("frames decode"))
    }

    /// End to end over a REAL TLS channel: a dedicated fragment listener with
    /// operator test certificates, dialed with the correct CA and the fixed
    /// `ravel-fragment` server name, serves a `Pinned` fetch carrying a REAL
    /// minted capability and returns the pinned segment's data. This is the
    /// happy path of ADR-0071 amendment decision 1 tests deliverable 2.
    #[tokio::test]
    async fn dedicated_tls_listener_serves_pinned_with_real_capability() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("tls-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let service = pinned_service(store, now);
        let (addr, _shutdown) = spawn_tls_fragment_listener(service).await;

        let channel = dial_fragment_tls(addr, TEST_FRAGMENT_CA_PEM, FRAGMENT_TLS_SERVER_NAME)
            .await
            .expect("TLS dial with the correct CA and server name connects");

        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let mut request = pinned_over_window(tenant.hash(), &seg, envelope);
        let query_id = [0x5A; 16];
        request.query_id = query_id.to_vec();
        // A real capability, minted exactly as `RoutingSliceFetcher` would.
        request.fragment_capability = mint(
            &TEST_KEY,
            tenant.hash().0,
            metrics_signal(),
            query_id,
            now + HOUR_NS,
        );

        let mut client = SeriesFetchClient::new(channel);
        let response = client
            .fetch(request)
            .await
            .expect("a valid Pinned fetch over the TLS channel succeeds");
        let decoded = collect_fetch(response).await;
        assert_eq!(decoded.status, pb::status::Code::Ok);
        assert_eq!(
            decoded.series_returned, 1,
            "the pinned segment was served through the real TLS channel"
        );
    }

    /// The TLS layer refuses a coordinator that pins the WRONG CA (the worker's
    /// certificate does not chain to it) and one that expects the WRONG server
    /// name (the certificate's SAN is `ravel-fragment`). Both fail at the
    /// handshake, before any capability is inspected (ADR-0071 amendment decision
    /// 1 tests deliverable 3).
    #[tokio::test]
    async fn dedicated_tls_listener_rejects_wrong_ca_and_wrong_server_name() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let service = pinned_service(store, 4 * HOUR_NS);
        let (addr, _shutdown) = spawn_tls_fragment_listener(service).await;

        let wrong_ca =
            dial_fragment_tls(addr, TEST_FRAGMENT_OTHER_CA_PEM, FRAGMENT_TLS_SERVER_NAME)
                .await
                .map(|_| ());
        assert!(
            wrong_ca.is_err(),
            "a worker certificate that does not chain to the pinned CA must be refused at TLS"
        );

        let wrong_name = dial_fragment_tls(addr, TEST_FRAGMENT_CA_PEM, "not-ravel-fragment")
            .await
            .map(|_| ());
        assert!(
            wrong_name.is_err(),
            "a server-name mismatch must be refused at the TLS layer"
        );
    }

    /// Issue #1690: the dedicated fragment listener requires a client
    /// certificate from the pinned `--fragment-tls-ca`. A peer that presents
    /// none is refused by the transport even when it carries a REAL, currently
    /// valid capability for the segment it asks for, and the SAME request
    /// succeeds over a channel presenting the coordinator identity. The
    /// capability is held constant across the two halves, so what differs is
    /// the client certificate and nothing else.
    ///
    /// The refusal may land on `connect()` or on the first RPC: under TLS 1.3
    /// the client finishes its side of the handshake before the server has
    /// verified the certificate it did not receive, so the alert arrives on the
    /// next flight. Either point is the transport refusing; what the assertion
    /// pins is that the request never reaches the capability check, which the
    /// succeeding half proves would otherwise have admitted it.
    #[tokio::test]
    async fn dedicated_tls_listener_refuses_a_client_with_no_certificate() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("mtls-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let service = pinned_service(store, now);
        let (addr, _shutdown) = spawn_tls_fragment_listener(service).await;

        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let query_id = [0x7C; 16];
        let capability = mint(
            &TEST_KEY,
            tenant.hash().0,
            metrics_signal(),
            query_id,
            now + HOUR_NS,
        );
        let request = || {
            let mut request = pinned_over_window(tenant.hash(), &seg, envelope);
            request.query_id = query_id.to_vec();
            request.fragment_capability = capability.clone();
            request
        };

        let anonymous = dial_fragment_tls_without_client_cert(
            addr,
            TEST_FRAGMENT_CA_PEM,
            FRAGMENT_TLS_SERVER_NAME,
        )
        .await;
        match anonymous {
            Err(_) => {}
            Ok(channel) => {
                let status = SeriesFetchClient::new(channel)
                    .fetch(request())
                    .await
                    .expect_err(
                        "a client presenting no certificate must not be served, even with a \
                         valid capability",
                    );
                let detail = format!("{status:?}");
                assert!(
                    detail.contains("CertificateRequired"),
                    "the refusal is the TLS layer's own alert for a missing client \
                     certificate, not a capability rejection (which would be a typed \
                     PermissionDenied status): {detail}"
                );
                assert_ne!(
                    status.code(),
                    tonic::Code::PermissionDenied,
                    "the request must not reach the capability check at all: {detail}"
                );
            }
        }

        // The control: the same capability, over a channel that presents the
        // coordinator's certificate, is served.
        let authenticated = dial_fragment_tls(addr, TEST_FRAGMENT_CA_PEM, FRAGMENT_TLS_SERVER_NAME)
            .await
            .expect("a client presenting the coordinator identity completes the handshake");
        let response = SeriesFetchClient::new(authenticated)
            .fetch(request())
            .await
            .expect("the same request with a client certificate succeeds");
        let decoded = collect_fetch(response).await;
        assert_eq!(decoded.status, pb::status::Code::Ok);
        assert_eq!(
            decoded.series_returned, 1,
            "the capability was accepted once the client certificate was present"
        );
    }

    /// A `Resolve` (federation) request reaching the dedicated fragment listener
    /// is rejected with a typed `PermissionDenied`, before the resolver runs: the
    /// dedicated listener carries `Pinned` traffic only (ADR-0071 amendment
    /// decision 1 tests deliverable 4). Exercised in-process against the same
    /// role the TLS listener mounts, so the assertion is deterministic.
    #[tokio::test]
    async fn dedicated_listener_rejects_resolve_with_typed_error() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let service = federation_service(store, &[("operator-cred", "some-tenant")], 4 * HOUR_NS)
            .with_role(FragmentListenerRole::DedicatedFragment);
        let mut req = tonic::Request::new(resolve_request(TenantHash([9u8; 16]), 4 * HOUR_NS));
        req.metadata_mut()
            .insert("authorization", "Bearer operator-cred".parse().unwrap());
        let err = service
            .fetch(req)
            .await
            .err()
            .expect("Resolve is rejected on the dedicated fragment listener");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("dedicated fragment listener"),
            "the rejection names the surface: {}",
            err.message()
        );
    }

    /// The public gRPC listener (mounted `PublicFederation` when a dedicated
    /// fragment listener exists) refuses a `Pinned` fetch with a typed
    /// `PermissionDenied`, whether or not it carries a valid capability: the scope
    /// is rejected before the capability is inspected (ADR-0071 amendment decision
    /// 1 tests deliverable 5).
    ///
    /// The `SeriesFetch` service is still registered on the public listener (it
    /// serves `Resolve`), so a `Pinned` request reaches this handler and receives
    /// a handler-level status; it is NOT a gRPC "unimplemented service".
    #[tokio::test]
    async fn public_listener_rejects_pinned_even_with_valid_capability() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("pub-tenant".to_string());
        let now = 4 * HOUR_NS;
        let service = pinned_service(store, now).with_role(FragmentListenerRole::PublicFederation);
        let query_id = [0x33; 16];
        let mut request = pinned_request(tenant.hash().0, &[0]);
        request.query_id = query_id.to_vec();
        // A VALID capability: the point is it is refused on scope grounds anyway.
        request.fragment_capability = mint(
            &TEST_KEY,
            tenant.hash().0,
            metrics_signal(),
            query_id,
            now + HOUR_NS,
        );
        let err = service
            .fetch(tonic::Request::new(request))
            .await
            .err()
            .expect("Pinned is rejected on the public listener");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("public gRPC listener"),
            "the rejection names the surface: {}",
            err.message()
        );
    }

    /// The capability mechanism and the transport are genuinely decoupled: one
    /// capability, minted with no knowledge of which listener will serve it
    /// (the mint side is `RoutingSliceFetcher`, transport-agnostic), verifies
    /// byte-for-byte the same on the new dedicated `Pinned` listener as on the
    /// legacy combined surface (ADR-0071 amendment decision 1 tests deliverable
    /// 6). This proves the two listeners are not incompatible islands.
    #[tokio::test]
    async fn capability_is_decoupled_from_listener_transport() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("decoupled-tenant".to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let envelope = TimeRange {
            start_ns: seg.min_event_ts_ns,
            end_ns: seg.max_event_ts_ns,
        };
        let query_id = [0xC0; 16];
        let capability = mint(
            &TEST_KEY,
            tenant.hash().0,
            metrics_signal(),
            query_id,
            now + HOUR_NS,
        );
        let mut request = pinned_over_window(tenant.hash(), &seg, envelope);
        request.query_id = query_id.to_vec();
        request.fragment_capability = capability;

        let dedicated =
            pinned_service(store.clone(), now).with_role(FragmentListenerRole::DedicatedFragment);
        let on_dedicated = fetch_pinned_inproc(&dedicated, request.clone())
            .await
            .expect("the dedicated Pinned listener accepts the capability");
        assert_eq!(on_dedicated.status, pb::status::Code::Ok);

        let combined = pinned_service(store, now).with_role(FragmentListenerRole::Combined);
        let on_combined = fetch_pinned_inproc(&combined, request)
            .await
            .expect("the legacy combined surface accepts the identical capability");
        assert_eq!(on_combined.status, pb::status::Code::Ok);

        assert_eq!(
            on_dedicated.series_returned, on_combined.series_returned,
            "the same capability yields the same result regardless of transport"
        );
    }

    // ---- issue #1687 part B: bounded slice decoding ------------------------

    /// Roughly nine wire KiB per flooded frame: enough that a handful of them
    /// fill the HTTP/2 stream window, so a coordinator that stops reading
    /// stops the producer within a small, assertable overrun rather than
    /// letting it run to completion inside the window.
    const FLOOD_SAMPLES_PER_FRAME: usize = 1024;

    /// A `SeriesFetch` server that streams `total` large series frames and then
    /// a terminal OK summary, counting every message it actually produces.
    ///
    /// The count is the whole point: a coordinator that stops pulling at its cap
    /// leaves this well below `total + 1`, and one that drains the stream before
    /// checking anything drives it to exactly `total + 1`. The stream is built
    /// with `unfold`, so a message is constructed only when the transport asks
    /// for one.
    struct FrameFlood {
        total: usize,
        produced: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn flood_series_frame(index: usize) -> pb::FetchResponse {
        let mut series_id = [0u8; 16];
        series_id[..8].copy_from_slice(&(index as u64).to_be_bytes());
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Series(pb::SeriesFrame {
                series_id: series_id.to_vec(),
                labels: vec![pb::Label {
                    name: "__name__".to_string(),
                    value: "flood".to_string(),
                }],
                runs: vec![pb::Run {
                    ts_delta: vec![1i64; FLOOD_SAMPLES_PER_FRAME],
                    value_bits: vec![1u64; FLOOD_SAMPLES_PER_FRAME],
                    ..Default::default()
                }],
            })),
        }
    }

    fn flood_summary() -> pb::FetchResponse {
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                status: Some(pb::Status {
                    code: pb::status::Code::Ok as i32,
                    message: String::new(),
                }),
                ..Default::default()
            })),
        }
    }

    #[tonic::async_trait]
    impl SeriesFetch for FrameFlood {
        type FetchStream = FragmentStream;

        async fn fetch(
            &self,
            _request: tonic::Request<pb::FetchRequest>,
        ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
            let total = self.total;
            let produced = Arc::clone(&self.produced);
            let stream = futures::stream::unfold(0usize, move |i| {
                let produced = Arc::clone(&produced);
                async move {
                    if i > total {
                        return None;
                    }
                    produced.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let frame = if i == total {
                        flood_summary()
                    } else {
                        flood_series_frame(i)
                    };
                    Some((Ok(frame), i + 1))
                }
            });
            Ok(tonic::Response::new(Box::pin(stream)))
        }
    }

    /// Stand up a [`FrameFlood`] on an ephemeral plaintext loopback port.
    /// Returns its `host:port`, the produced-message counter, and a shutdown
    /// handle whose drop stops the task at test end.
    async fn spawn_frame_flood(
        total: usize,
    ) -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let produced = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server =
            tonic::transport::Server::builder().add_service(SeriesFetchServer::new(FrameFlood {
                total,
                produced: Arc::clone(&produced),
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = server
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        (addr.to_string(), produced, tx)
    }

    /// A coordinator whose only live worker is `endpoint`, so every slice ranks
    /// that endpoint first and has no second remote owner to re-dispatch to: a
    /// terminal error from the first attempt is the fetch's result.
    fn fetcher_against(
        endpoint: &str,
        engine: ravel_query::EngineConfig,
        metrics: Arc<FragmentMetrics>,
    ) -> RoutingSliceFetcher {
        let self_cell = Arc::new(OnceLock::new());
        self_cell
            .set(uuid::Uuid::from_u128(1))
            .expect("set self id");
        let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
            process_id: uuid::Uuid::from_u128(2).to_string(),
            fragment_endpoint: endpoint.to_string(),
            flight_sql_endpoint: endpoint.to_string(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        }])));
        RoutingSliceFetcher::new(
            self_cell,
            live,
            test_keys(),
            test_service(metrics.clone()).with_engine_config(engine),
            metrics,
        )
    }

    /// Issue #1687 part B, the acceptance test: a remote that streams far more
    /// frames than the coordinator's per-slice frame cap is refused AT the cap,
    /// and the coordinator never holds the slice.
    ///
    /// `max_bytes_scanned` is deliberately left at the `EngineConfig` default,
    /// so the byte cap in force is the absolute derived ceiling. The frame cap
    /// is lowered to 64 frames, about 576 wire KiB, so it is the cap this
    /// stream reaches. Two wrong implementations are ruled out, each by a
    /// different assertion:
    ///
    /// * A decoder that enforces the byte cap but NOT the frame cap runs on
    ///   past 64 frames and reaches the terminal summary: the whole flood is
    ///   `TOTAL` frames of 1024 narrowest samples, about 185 MB of wire bytes,
    ///   which is under [`codec::MAX_SLICE_RESPONSE_BYTES`], so the byte
    ///   ceiling never fires on this stream at all. The typed-variant
    ///   assertion and the produced-count assertion fail.
    /// * A decoder that enforces BOTH caps but only after draining the stream
    ///   returns the same error, so the error assertions pass, but it pulls
    ///   every message: `produced` reaches `TOTAL + 1`. The produced-count
    ///   assertion fails.
    ///
    /// The overrun allowance is HTTP/2 flow control, not slack in the claim: a
    /// producer may fill the stream window ahead of the reader, and the window
    /// holds only a few frames of this size. What is asserted is that the
    /// producer stopped near the cap, which is what "without buffering the
    /// slice" means on a real stream.
    #[tokio::test]
    async fn remote_fetch_stops_pulling_at_the_frame_cap_without_buffering_the_slice() {
        const CAP: usize = 64;
        // Far more than the transport can buffer ahead, and 300x the cap: the
        // produced count then distinguishes "stopped near the cap" from
        // "stopped when the stream ended" by two orders of magnitude, not by a
        // margin that could be flow-control luck. Frames are generated lazily,
        // so a large TOTAL costs nothing unless something actually pulls them.
        const TOTAL: usize = 20_000;
        /// The measured overrun past the cap is about 170 frames (the HTTP/2
        /// windows); this is that with room to spare, and it is a constant, not
        /// a fraction of TOTAL.
        const MAX_OVERRUN: usize = 1024;

        assert_eq!(
            RoutingSliceFetcher::new(
                Arc::new(OnceLock::new()),
                Arc::new(RwLock::new(Arc::new(Vec::new()))),
                test_keys(),
                test_service(Arc::new(FragmentMetrics::new())),
                Arc::new(FragmentMetrics::new()),
            )
            .max_slice_frames,
            codec::MAX_SLICE_RESPONSE_FRAMES,
            "a coordinator built the production way decodes under the constant; \
             the lowered cap below is a test seam, not a different rule"
        );

        let (endpoint, produced, _shutdown) = spawn_frame_flood(TOTAL).await;
        let metrics = Arc::new(FragmentMetrics::new());
        let fetcher = fetcher_against(
            &endpoint,
            ravel_query::EngineConfig::default(),
            metrics.clone(),
        )
        .with_max_slice_frames(CAP);

        let sink = FragmentStatsSink::new();
        let err = with_fragment_stats(sink.clone(), fetcher.fetch(pinned_request([9u8; 16], &[0])))
            .await
            .expect_err("a slice past the frame cap is refused, not decoded");

        // Rules out the byte-cap-only decoder: with an Unlimited byte cap it has
        // nothing to trip on and returns the whole slice.
        match err {
            DistribError::Codec(codec::CodecError::SliceFrameCapExceeded { frames, max }) => {
                assert_eq!(
                    (frames, max),
                    (CAP + 1, CAP),
                    "the refusal names the exact counts and trips on the first \
                     frame past the cap, not later"
                );
            }
            other => panic!("expected a frame-cap refusal, got {other:?}"),
        }

        // Rules out the drain-then-check decoder: that one pulls every message.
        let produced = produced.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            produced < TOTAL + 1,
            "the coordinator stopped pulling: the remote produced {produced} of \
             {} messages",
            TOTAL + 1
        );
        assert!(
            produced > CAP,
            "the coordinator did read up to its cap before refusing, so the \
             refusal is the cap and not an earlier transport failure \
             (produced {produced})"
        );
        assert!(
            produced <= CAP + 1 + MAX_OVERRUN,
            "the overrun past the cap is the HTTP/2 window, a constant, not a \
             share of the stream: produced {produced} for a cap of {CAP} out of \
             {} available",
            TOTAL + 1
        );

        // The bytes the refused slice made the coordinator hold are reported,
        // not written off as zero because the slice errored.
        let entries = sink.take();
        assert_eq!(entries.len(), 1, "one slice, one fragment entry");
        assert_eq!(entries[0].status, "error");
        assert!(
            entries[0].wire_bytes_consumed > 0,
            "a refused slice reports the wire bytes it consumed"
        );
    }

    /// The byte cap is the other half, and it is the one that binds when a
    /// remote sends few but enormous frames: a slice whose frames outrun the
    /// per-slice wire ceiling is refused naming both byte counts, well before
    /// the frame cap could apply.
    ///
    /// The cap is lowered through the `with_max_slice_bytes` test seam. A real
    /// coordinator enforces [`codec::MAX_SLICE_RESPONSE_BYTES`] whatever it
    /// configured, which the assertion below pins on a production-built
    /// fetcher; driving the whole derived ceiling over a loopback stream would
    /// test the same `push` branch at thousands of times the cost, and
    /// `ravel_query::distrib::codec::slice_cap_tests` drives the real constant
    /// directly.
    #[tokio::test]
    async fn remote_fetch_refuses_a_slice_past_the_byte_cap() {
        const TOTAL: usize = 20_000;
        // Under ~9 KiB per frame, this trips after a handful of frames, so the
        // frame cap (left at the production constant) cannot be what fires.
        const MAX_BYTES: u64 = 32 * 1024;

        let (endpoint, produced, _shutdown) = spawn_frame_flood(TOTAL).await;
        let metrics = Arc::new(FragmentMetrics::new());
        assert_eq!(
            fetcher_against(
                &endpoint,
                ravel_query::EngineConfig {
                    // A store-byte budget far below the wire ceiling does not
                    // become the wire cap: the two count different things.
                    max_bytes_scanned: ravel_query::ByteLimit::Bounded(MAX_BYTES),
                    ..ravel_query::EngineConfig::default()
                },
                metrics.clone(),
            )
            .max_slice_bytes,
            codec::MAX_SLICE_RESPONSE_BYTES,
            "a coordinator built the production way decodes under the constant; \
             the lowered cap below is a test seam, not a different rule"
        );
        let fetcher = fetcher_against(
            &endpoint,
            ravel_query::EngineConfig::default(),
            metrics.clone(),
        )
        .with_max_slice_bytes(MAX_BYTES);

        let err = fetcher
            .fetch(pinned_request([9u8; 16], &[0]))
            .await
            .expect_err("a slice past the byte cap is refused");
        match err {
            DistribError::Codec(codec::CodecError::SliceByteCapExceeded { bytes, max }) => {
                assert_eq!(max, MAX_BYTES, "the cap named is the one in force");
                assert!(
                    bytes > MAX_BYTES,
                    "the refusal names the bytes actually accepted ({bytes})"
                );
            }
            other => panic!("expected a byte-cap refusal, got {other:?}"),
        }
        let produced = produced.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            produced < TOTAL + 1,
            "the coordinator stopped pulling at the byte cap: the remote \
             produced {produced} of {} messages",
            TOTAL + 1
        );
    }

    /// The cap a STOCK process decodes under: the case nobody configures, which
    /// is where the cap has to hold on its own.
    ///
    /// The config is built the way `lib.rs` builds the process-wide
    /// `EngineConfig`: `max_bytes_scanned` from `LimitsConfig`'s resolved
    /// `query_defaults`, which with no `--limits-file` is
    /// `shipped_query_defaults()`. That resolves to an explicit
    /// `ByteLimit::Unlimited`, so a cap that applied only to an ABSENT setting
    /// would be off here, on every default deployment. It resolves instead to
    /// the fixed ceiling, and `push` refuses on `bytes > byte_cap()` (the
    /// same field, proven firing end to end by the 32 KiB test above).
    #[test]
    fn a_stock_resolved_config_decodes_under_the_absolute_byte_cap() {
        let limits = crate::config::limits::LimitsConfig::default();
        assert_eq!(
            limits.query_defaults.max_bytes_scanned,
            ravel_query::ByteLimit::Unlimited,
            "the shipped query defaults opt out of the store-bytes budget"
        );
        let engine = ravel_query::EngineConfig {
            max_bytes_scanned: limits.query_defaults.max_bytes_scanned,
            ..ravel_query::EngineConfig::default()
        };
        assert_eq!(
            SliceStreamDecoder::new(&engine).byte_cap(),
            codec::MAX_SLICE_RESPONSE_BYTES
        );
        assert!(
            codec::MAX_SLICE_RESPONSE_BYTES
                >= ravel_query::DEFAULT_MAX_SAMPLES as u64 * codec::WIRE_BYTES_PER_SAMPLE_WIDEST,
            "the stock ceiling admits a slice at the stock sample budget"
        );
    }

    /// A slice that stays inside both caps decodes exactly as before: the caps
    /// refuse a flood, they do not change an ordinary slice's result.
    #[tokio::test]
    async fn remote_fetch_below_the_caps_decodes_the_whole_slice() {
        const TOTAL: usize = 32;

        let (endpoint, produced, _shutdown) = spawn_frame_flood(TOTAL).await;
        let metrics = Arc::new(FragmentMetrics::new());
        let fetcher = fetcher_against(
            &endpoint,
            ravel_query::EngineConfig::default(),
            metrics.clone(),
        )
        .with_max_slice_frames(TOTAL + 1);

        let sink = FragmentStatsSink::new();
        let response =
            with_fragment_stats(sink.clone(), fetcher.fetch(pinned_request([9u8; 16], &[0])))
                .await
                .expect("a slice inside both caps decodes");
        assert_eq!(response.status, pb::status::Code::Ok);
        assert_eq!(
            response.scalar.len(),
            TOTAL,
            "every frame below the cap is decoded and kept"
        );
        assert_eq!(
            produced.load(std::sync::atomic::Ordering::Relaxed),
            TOTAL + 1,
            "the whole stream, summary included, was consumed"
        );
        let entries = sink.take();
        assert_eq!(entries[0].status, "ok");
        assert!(
            entries[0].wire_bytes_consumed > 0,
            "wire bytes are reported for a successful slice too"
        );
    }

    /// The federation client is bounded by the same byte cap, taken from the
    /// federating coordinator's own `EngineConfig`. A remote cluster is outside
    /// this operator's control, so this is the only place that bound exists.
    #[tokio::test]
    async fn federation_fetch_refuses_a_slice_past_the_byte_cap() {
        const TOTAL: usize = 20_000;
        const MAX_BYTES: u64 = 32 * 1024;

        let (endpoint, produced, _shutdown) = spawn_frame_flood(TOTAL).await;
        let channel = Channel::from_shared(format!("http://{endpoint}"))
            .expect("valid uri")
            .connect_timeout(REMOTE_CONNECT_TIMEOUT)
            .connect()
            .await
            .expect("connect to the flood server");
        let fetcher = FederationSliceFetcher {
            cluster: "remote-a".to_string(),
            channel,
            credential: "token".to_string(),
            engine: ravel_query::EngineConfig::default(),
            max_slice_bytes: codec::MAX_SLICE_RESPONSE_BYTES,
        }
        // A store-byte budget far below the wire ceiling does not become the
        // wire cap; the seam below is what lowers it for this test.
        .with_engine_config(ravel_query::EngineConfig {
            max_bytes_scanned: ravel_query::ByteLimit::Bounded(MAX_BYTES),
            ..ravel_query::EngineConfig::default()
        })
        .with_max_slice_bytes(MAX_BYTES);

        let err = fetcher
            .fetch(pinned_request([9u8; 16], &[0]))
            .await
            .expect_err("a federated slice past the byte cap is refused");
        assert!(
            matches!(
                err,
                DistribError::Codec(codec::CodecError::SliceByteCapExceeded { max, .. })
                    if max == MAX_BYTES
            ),
            "expected a byte-cap refusal naming this coordinator's cap, got {err:?}"
        );
        let produced = produced.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            produced < TOTAL + 1,
            "the federation client stopped pulling too: the remote produced \
             {produced} of {} messages",
            TOTAL + 1
        );
    }

    // ---- issue #1723: a failed attempt's cost is carried, not discarded ----

    /// What one scripted attempt reports having spent before it gave up. These
    /// are real GET counters on a real accounting snapshot, not a marker value:
    /// the assertions below are exact sums over them.
    #[derive(Clone, Copy)]
    struct Spend {
        get_requests: u64,
        get_bytes: u64,
        raw_f64_pages: u64,
        raw_f64_bytes: u64,
    }

    impl Spend {
        fn snapshot(&self) -> QueryAccountingSnapshot {
            let accounting = ravel_types::accounting::QueryAccounting::new();
            for _ in 0..self.get_requests {
                accounting.record_s3_request(ravel_types::accounting::AccountedOp::Get);
            }
            accounting.add_s3_bytes(ravel_types::accounting::AccountedOp::Get, self.get_bytes);
            accounting.snapshot()
        }

        /// The terminal summary frame a worker emits when it gives up after
        /// spending this much, exactly as `SeriesFetchService` builds one.
        fn summary(&self, code: pb::status::Code) -> pb::FetchResponse {
            pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                    accounting: Some(codec::encode_accounting(&self.snapshot())),
                    series_returned: 0,
                    samples_returned: 0,
                    status: Some(pb::Status {
                        code: code as i32,
                        message: if code == pb::status::Code::Ok {
                            String::new()
                        } else {
                            "store returned 503 on the next segment".to_string()
                        },
                    }),
                    raw_f64_pages: self.raw_f64_pages,
                    raw_f64_bytes: self.raw_f64_bytes,
                })),
            }
        }

        /// The same terminal summary, carrying a status code this build does
        /// not know: what a worker from a newer version sends. The decoder
        /// accepts the frame (and its accounting) and `finish` refuses it
        /// afterwards, which is the shape issue #1723's `finish` salvage
        /// exists for.
        fn summary_with_raw_status(&self, raw_code: i32) -> pb::FetchResponse {
            let mut frame = self.summary(pb::status::Code::Ok);
            if let Some(pb::fetch_response::Frame::Summary(summary)) = frame.frame.as_mut()
                && let Some(status) = summary.status.as_mut()
            {
                status.code = raw_code;
            }
            frame
        }
    }

    /// A `Status.code` past the highest this build's proto defines
    /// (`INTERNAL = 8`), so `codec::decode_status_code` refuses it as
    /// `UnknownStatusCode`.
    const UNKNOWN_STATUS_CODE: i32 = 9;

    /// How a scripted worker ends one attempt: the three ways a re-dispatchable
    /// attempt reaches `try_remote`, and the three that end it terminally.
    #[derive(Clone, Copy, PartialEq)]
    enum Ending {
        /// Terminal summary, clean stream close: the `Unavailable`-summary case.
        Summary,
        /// Terminal summary, then the stream breaks: a transport loss whose
        /// spend is still recoverable from the summary already decoded.
        SummaryThenBreak,
        /// The stream breaks before anything is sent: a transport loss that
        /// reveals nothing about what the worker spent.
        BreakOnly,
        /// Terminal summary, then a frame the decoder refuses: a typed decode
        /// fault that ends the slice after the attempt reported its cost.
        SummaryThenEmptyFrame,
        /// Terminal summary, then enough series frames to pass a lowered byte
        /// cap: a refusal whose attempt already said what it spent.
        SummaryThenFlood,
        /// The wire order a real worker uses, series frames then the terminal
        /// summary, with enough of them to pass a lowered byte cap before the
        /// summary is reached: a refusal that knows nothing about the spend.
        FloodThenSummary,
        /// A terminal summary whose status code this build does not know, then
        /// a clean close. The stream never breaks, so the fault surfaces out of
        /// `SliceStreamDecoder::finish` rather than out of `push`, after the
        /// summary's accounting was already accepted.
        UnknownStatusSummary,
    }

    /// The per-slice wire cap the two scripted flood endings are refused under,
    /// lowered through the `with_max_slice_bytes` test seam. A scripted summary
    /// frame is a couple of hundred bytes, so this accepts one comfortably and
    /// refuses on the first series frame after it.
    const CAP_UNDER_TEST: u64 = 4 * 1024;

    /// Enough [`flood_series_frame`]s to pass the cap. One frame is already
    /// several times [`CAP_UNDER_TEST`]; the rest only prove the decoder stops
    /// rather than drains.
    const FLOOD_FRAMES: usize = 4;

    /// A `SeriesFetch` worker that answers every request with one scripted
    /// [`Spend`] and [`Ending`], counting the attempts it served so a test can
    /// prove each worker was dispatched to exactly once.
    struct ScriptedWorker {
        spend: Spend,
        code: pb::status::Code,
        ending: Ending,
        attempts: Arc<AtomicU64>,
    }

    #[tonic::async_trait]
    impl SeriesFetch for ScriptedWorker {
        type FetchStream = FragmentStream;

        async fn fetch(
            &self,
            _request: tonic::Request<pb::FetchRequest>,
        ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            let summary = || Ok(self.spend.summary(self.code));
            let broke = || Err(tonic::Status::unavailable("worker went away mid-stream"));
            let flood = || (0..FLOOD_FRAMES).map(|i| Ok(flood_series_frame(i)));
            let items: Vec<Result<pb::FetchResponse, tonic::Status>> = match self.ending {
                Ending::Summary => vec![summary()],
                Ending::SummaryThenBreak => vec![summary(), broke()],
                Ending::BreakOnly => vec![broke()],
                Ending::SummaryThenEmptyFrame => {
                    vec![summary(), Ok(pb::FetchResponse { frame: None })]
                }
                Ending::SummaryThenFlood => std::iter::once(summary()).chain(flood()).collect(),
                Ending::FloodThenSummary => flood().chain(std::iter::once(summary())).collect(),
                Ending::UnknownStatusSummary => {
                    vec![Ok(self.spend.summary_with_raw_status(UNKNOWN_STATUS_CODE))]
                }
            };
            Ok(tonic::Response::new(Box::pin(futures::stream::iter(items))))
        }
    }

    /// Stand up a [`ScriptedWorker`] on an ephemeral plaintext loopback port.
    /// Returns its `host:port`, its attempt counter, and a shutdown handle whose
    /// drop stops the task at test end.
    async fn spawn_scripted(
        spend: Spend,
        code: pb::status::Code,
        ending: Ending,
    ) -> (String, Arc<AtomicU64>, tokio::sync::oneshot::Sender<()>) {
        let attempts = Arc::new(AtomicU64::new(0));
        let server = tonic::transport::Server::builder().add_service(SeriesFetchServer::new(
            ScriptedWorker {
                spend,
                code,
                ending,
                attempts: Arc::clone(&attempts),
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = server
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await;
        });
        (addr.to_string(), attempts, tx)
    }

    /// A coordinator whose local execution reads `store` for real and whose live
    /// worker set is empty: these tests hand `dispatch` the ranked owners
    /// directly, so the slice's route is scripted rather than rendezvous-derived.
    fn coordinator_over(store: Arc<dyn ObjectStoreBackend>, now_ns: i64) -> RoutingSliceFetcher {
        RoutingSliceFetcher::new(
            Arc::new(OnceLock::new()),
            Arc::new(RwLock::new(Arc::new(Vec::new()))),
            test_keys(),
            pinned_service(store, now_ns),
            Arc::new(FragmentMetrics::new()),
        )
    }

    /// One tenant with one published segment, the pinned request for it, and
    /// what ONE cold coordinator-local attempt at that slice costs.
    ///
    /// The local figure is measured from the same code path the fallback runs,
    /// not hardcoded: the sums asserted below are exact against it, so they
    /// stay exact if the fetch path's request count changes.
    async fn one_slice_corpus(
        tenant_name: &str,
    ) -> (
        Arc<dyn ObjectStoreBackend>,
        i64,
        pb::FetchRequest,
        SliceResponse,
    ) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new(tenant_name.to_string());
        publish_metric(store.as_ref(), &tenant, HOUR_NS).await;
        let now = 4 * HOUR_NS;
        let seg = only_segment(&store, tenant.hash(), now).await;
        let request = pinned_over_window(
            tenant.hash(),
            &seg,
            TimeRange {
                start_ns: seg.min_event_ts_ns,
                end_ns: seg.max_event_ts_ns,
            },
        );
        let local_only = pinned_service(Arc::clone(&store), now)
            .run_local(request.clone())
            .await
            .expect("the local attempt succeeds");
        assert_eq!(local_only.status, pb::status::Code::Ok);
        assert!(
            local_only.accounting.total_s3_bytes() > 0
                && local_only.accounting.total_s3_requests() > 0,
            "the fixture's local attempt must cost real requests for the sums \
             below to mean anything"
        );
        (store, now, request, local_only)
    }

    /// Issue #1723, the acceptance proof: a slice the store serves THREE times
    /// is charged three times.
    ///
    /// Both remote owners answer `Unavailable` after spending real GETs, so the
    /// slice runs primary -> re-dispatch -> coordinator-local, and the store
    /// served every one of those attempts. The recorded spend is asserted as the
    /// exact sum `local + A + B`, per counter, not as "more than one attempt's
    /// worth".
    ///
    /// Mutation proof: the answering attempt here is the coordinator-local one,
    /// so the line this pins is the `carried.fold_into(..)` around
    /// `self.local.run_local(request)` at the end of `dispatch`. Reverting it to
    /// return `run_local`'s result unfolded reports the local attempt's spend
    /// alone: the request assertion then reads `local` against the expected
    /// `local + A + B`. The re-dispatch fold is a different line and a
    /// different test
    /// (`redispatch_success_folds_the_abandoned_primarys_spend`).
    #[tokio::test]
    async fn three_attempt_slice_records_the_sum_of_every_attempt() {
        const A: Spend = Spend {
            get_requests: 3,
            get_bytes: 4_096,
            raw_f64_pages: 2,
            raw_f64_bytes: 16_384,
        };
        const B: Spend = Spend {
            get_requests: 5,
            get_bytes: 8_192,
            raw_f64_pages: 7,
            raw_f64_bytes: 57_344,
        };
        // Both abandoned attempts must report raw bytes, or the raw-byte sum
        // below holds for a fixture that never exercised that counter.
        const { assert!(A.raw_f64_bytes > 0 && B.raw_f64_bytes > 0) };

        let (store, now, request, local_only) = one_slice_corpus("three-attempt-tenant").await;
        let (endpoint_a, tries_a, _keep_a) =
            spawn_scripted(A, pb::status::Code::Unavailable, Ending::Summary).await;
        let (endpoint_b, tries_b, _keep_b) =
            spawn_scripted(B, pb::status::Code::Unavailable, Ending::Summary).await;

        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, endpoint, fell_back) = fetcher
            .dispatch(
                vec![
                    Owner::Remote(endpoint_a.clone()),
                    Owner::Remote(endpoint_b.clone()),
                ],
                request,
                &wire_bytes,
            )
            .await;
        let response = result.expect("the coordinator-local third attempt answers");

        // Exactly three attempts were made, one per owner plus local.
        assert_eq!(
            tries_a.load(Ordering::Relaxed),
            1,
            "primary dispatched once"
        );
        assert_eq!(
            tries_b.load(Ordering::Relaxed),
            1,
            "re-dispatched to the next owner once"
        );
        assert!(fell_back, "the third attempt ran coordinator-local");
        assert_eq!(endpoint, endpoint_a, "the stats entry names the primary");
        assert_eq!(response.status, pb::status::Code::Ok);

        assert_eq!(
            response.accounting.total_s3_requests(),
            local_only.accounting.total_s3_requests() + A.get_requests + B.get_requests,
            "recorded requests must be the sum over all three attempts \
             (local {} + A {} + B {})",
            local_only.accounting.total_s3_requests(),
            A.get_requests,
            B.get_requests
        );
        assert_eq!(
            response.accounting.total_s3_bytes(),
            local_only.accounting.total_s3_bytes() + A.get_bytes + B.get_bytes,
            "recorded bytes must be the sum over all three attempts \
             (local {} + A {} + B {})",
            local_only.accounting.total_s3_bytes(),
            A.get_bytes,
            B.get_bytes
        );
        assert_eq!(
            response.stats.raw_f64_pages,
            local_only.stats.raw_f64_pages + A.raw_f64_pages + B.raw_f64_pages,
            "the page counters sum over the attempts too"
        );
        assert_eq!(
            response.stats.raw_f64_bytes,
            local_only.stats.raw_f64_bytes + A.raw_f64_bytes + B.raw_f64_bytes,
            "and so do the raw f64 byte counters (local {} + A {} + B {})",
            local_only.stats.raw_f64_bytes,
            A.raw_f64_bytes,
            B.raw_f64_bytes
        );
    }

    /// The `Unavailable`-summary retry on its own: one remote owner reports
    /// `Unavailable` after spending, the coordinator runs the slice locally, and
    /// the answer carries `local + remote` exactly.
    ///
    /// This is the classification `try_remote` makes from a DECODED response, as
    /// opposed to the transport-loss classification covered separately below.
    ///
    /// Mutation proof: RED when the `carried.fold_into(..)` around the local
    /// fallback in `dispatch` is reverted, and equally when `try_remote`'s
    /// `Unavailable` arm goes back to carrying nothing, which is what
    /// discarding that response whole, result and cost together, amounts to.
    /// Either way the local fallback answers with its own spend and the request
    /// assertion reads `local` against the expected `local + REMOTE`.
    #[tokio::test]
    async fn unavailable_summary_retry_folds_the_abandoned_attempts_spend() {
        const REMOTE: Spend = Spend {
            get_requests: 3,
            get_bytes: 4_096,
            raw_f64_pages: 2,
            raw_f64_bytes: 16_384,
        };

        let (store, now, request, local_only) = one_slice_corpus("unavailable-retry-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(REMOTE, pb::status::Code::Unavailable, Ending::Summary).await;

        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, _endpoint, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint)], request, &wire_bytes)
            .await;
        let response = result.expect("the local fallback answers");

        assert_eq!(tries.load(Ordering::Relaxed), 1);
        assert!(fell_back);
        assert_eq!(
            response.accounting.total_s3_requests(),
            local_only.accounting.total_s3_requests() + REMOTE.get_requests
        );
        assert_eq!(
            response.accounting.total_s3_bytes(),
            local_only.accounting.total_s3_bytes() + REMOTE.get_bytes,
            "the Unavailable attempt's GETs are charged alongside the local ones"
        );
    }

    /// The transport-loss retry, both of its shapes.
    ///
    /// A stream that breaks AFTER its terminal summary still told the
    /// coordinator what that worker spent, and `SliceStreamDecoder::summary_spend`
    /// salvages it: the answer carries `local + remote` exactly. A stream that
    /// breaks before any summary reveals nothing, and carries zero rather than a
    /// guess: the answer is the local attempt's spend exactly. Both are exact
    /// equalities, so a wrong value in either direction fails.
    ///
    /// Mutation proof: the salvage half is RED when `remote_fetch` stops
    /// writing `decoder.summary_spend()` into its `salvaged` out-parameter, when
    /// `try_remote`'s transport arm stops reading it
    /// (`salvaged.unwrap_or_default()` back to `AttemptSpend::default()`), and
    /// when the local fallback's `carried.fold_into(..)` is reverted: any of the
    /// three turns a transport error into a free attempt, and the byte
    /// assertion reads `local` against the expected `local + REMOTE`, the
    /// figure the second half pins as the no-summary case. The no-summary half
    /// passes before
    /// and after: it pins the deliberate under-report, so a later change that
    /// starts inventing a figure there fails here.
    #[tokio::test]
    async fn transport_loss_folds_a_salvaged_summary_and_nothing_else() {
        const REMOTE: Spend = Spend {
            get_requests: 3,
            get_bytes: 4_096,
            raw_f64_pages: 2,
            raw_f64_bytes: 16_384,
        };

        // Half 1: the summary arrived, then the stream broke.
        let (store, now, request, local_only) = one_slice_corpus("salvage-tenant").await;
        let (endpoint, tries, _keep) = spawn_scripted(
            REMOTE,
            pb::status::Code::Unavailable,
            Ending::SummaryThenBreak,
        )
        .await;
        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, _endpoint, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint)], request.clone(), &wire_bytes)
            .await;
        let response = result.expect("the local fallback answers");
        assert_eq!(tries.load(Ordering::Relaxed), 1);
        assert!(fell_back);
        assert_eq!(
            response.accounting.total_s3_bytes(),
            local_only.accounting.total_s3_bytes() + REMOTE.get_bytes,
            "a summary decoded before the break is salvaged, not lost"
        );
        assert_eq!(
            response.accounting.total_s3_requests(),
            local_only.accounting.total_s3_requests() + REMOTE.get_requests
        );
        assert_eq!(
            response.stats.raw_f64_bytes,
            local_only.stats.raw_f64_bytes + REMOTE.raw_f64_bytes,
            "the salvaged summary's raw f64 bytes are folded too, not just its \
             requests"
        );

        // Half 2: nothing arrived before the break.
        let (store, now, request, local_only) = one_slice_corpus("silent-loss-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(REMOTE, pb::status::Code::Unavailable, Ending::BreakOnly).await;
        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, _endpoint, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint)], request, &wire_bytes)
            .await;
        let response = result.expect("the local fallback answers");
        assert_eq!(tries.load(Ordering::Relaxed), 1);
        assert!(fell_back);
        assert_eq!(
            response.accounting.total_s3_bytes(),
            local_only.accounting.total_s3_bytes(),
            "a transport loss with no summary cannot observe the worker's spend \
             from here, so it carries zero rather than a guess"
        );
        assert_eq!(
            response.accounting.total_s3_requests(),
            local_only.accounting.total_s3_requests()
        );
    }

    /// The other fold in `dispatch`: the re-dispatch target answers, so the
    /// abandoned primary's spend rides on a REMOTE result rather than on the
    /// coordinator-local one.
    ///
    /// No local attempt runs at all here, so both sides of the sum come from
    /// the scripted workers and the expected figures are exact constants: the
    /// slice is charged `LOST + ANSWERED` per counter, and for the page and raw
    /// byte counters too.
    ///
    /// Mutation proof: RED when the successful re-dispatch returns
    /// `(*result, next.clone(), false)` instead of folding `carried` into it.
    /// The slice then reports only what the worker that answered spent
    /// (`ANSWERED` against the expected `LOST + ANSWERED`) and the primary's
    /// attempt is free.
    #[tokio::test]
    async fn redispatch_success_folds_the_abandoned_primarys_spend() {
        const LOST: Spend = Spend {
            get_requests: 3,
            get_bytes: 4_096,
            raw_f64_pages: 2,
            raw_f64_bytes: 16_384,
        };
        const ANSWERED: Spend = Spend {
            get_requests: 5,
            get_bytes: 8_192,
            raw_f64_pages: 7,
            raw_f64_bytes: 57_344,
        };

        let (store, now, request, _local_only) = one_slice_corpus("redispatch-fold-tenant").await;
        let (endpoint_a, tries_a, _keep_a) =
            spawn_scripted(LOST, pb::status::Code::Unavailable, Ending::Summary).await;
        let (endpoint_b, tries_b, _keep_b) =
            spawn_scripted(ANSWERED, pb::status::Code::Ok, Ending::Summary).await;

        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, endpoint, fell_back) = fetcher
            .dispatch(
                vec![Owner::Remote(endpoint_a), Owner::Remote(endpoint_b.clone())],
                request,
                &wire_bytes,
            )
            .await;
        let response = result.expect("the re-dispatch target answers");

        assert_eq!(
            tries_a.load(Ordering::Relaxed),
            1,
            "primary dispatched once"
        );
        assert_eq!(
            tries_b.load(Ordering::Relaxed),
            1,
            "re-dispatched to the next owner once"
        );
        assert!(
            !fell_back,
            "the second remote answered, so no coordinator-local attempt ran"
        );
        assert_eq!(
            endpoint, endpoint_b,
            "the stats entry names the worker that answered"
        );
        assert_eq!(response.status, pb::status::Code::Ok);

        assert_eq!(
            response.accounting.total_s3_requests(),
            LOST.get_requests + ANSWERED.get_requests,
            "recorded requests must be the sum over both remote attempts \
             (lost {} + answered {})",
            LOST.get_requests,
            ANSWERED.get_requests
        );
        assert_eq!(
            response.accounting.total_s3_bytes(),
            LOST.get_bytes + ANSWERED.get_bytes,
            "recorded bytes must be the sum over both remote attempts \
             (lost {} + answered {})",
            LOST.get_bytes,
            ANSWERED.get_bytes
        );
        assert_eq!(
            response.stats.raw_f64_pages,
            LOST.raw_f64_pages + ANSWERED.raw_f64_pages,
            "the page counters sum over both attempts too"
        );
        assert_eq!(
            response.stats.raw_f64_bytes,
            LOST.raw_f64_bytes + ANSWERED.raw_f64_bytes,
            "and so do the raw f64 byte counters"
        );
    }

    /// A slice whose final attempt ends in a typed decode fault AFTER its
    /// terminal summary: the worker read its segments, said what that cost, and
    /// then sent a frame the coordinator refuses.
    ///
    /// The error is terminal, so nothing re-dispatches and no later attempt
    /// exists for the spend to ride on: it rides on the error itself. The two
    /// assertions are the two halves of that claim, since a wrapper that ate
    /// the typed class would be as wrong as one that lost the figure.
    ///
    /// Mutation proof: RED when `try_remote`'s terminal arm stops reading
    /// `salvaged` (`Err(other) => Attempt::Keep(Box::new(Err(other)))`). The
    /// error then carries no spend at all and `spend()` is `None`.
    #[tokio::test]
    async fn terminal_error_after_a_summary_carries_what_that_attempt_paid() {
        const PAID: Spend = Spend {
            get_requests: 4,
            get_bytes: 12_288,
            raw_f64_pages: 3,
            raw_f64_bytes: 24_576,
        };

        let (store, now, request, _local_only) = one_slice_corpus("salvaged-terminal-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(PAID, pb::status::Code::Ok, Ending::SummaryThenEmptyFrame).await;

        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, named, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint.clone())], request, &wire_bytes)
            .await;
        let err = result.expect_err("an empty frame ends the slice typed");

        assert_eq!(
            tries.load(Ordering::Relaxed),
            1,
            "a decode fault is not re-dispatched"
        );
        assert!(
            !fell_back,
            "and does not fall back to the coordinator either"
        );
        assert_eq!(named, endpoint);
        assert!(
            matches!(err.unspent(), DistribError::EmptyFrame),
            "the spend wrapper keeps the typed class underneath it: {err:?}"
        );
        let spend = err.spend().expect("the failed slice reports what it paid");
        assert_eq!(
            spend.total_s3_requests(),
            PAID.get_requests,
            "exactly the summary's requests, counted once"
        );
        assert_eq!(
            spend.total_s3_bytes(),
            PAID.get_bytes,
            "exactly the summary's bytes, counted once"
        );
    }

    /// The byte-cap refusal as a slice's final attempt, in both wire orders.
    ///
    /// What a refusal reports is decided by whether the attempt's terminal
    /// summary was decoded before the cap tripped, not by what the worker
    /// really read. With the summary first the refusal reports that figure
    /// exactly; with the frames first, which is the order a real worker streams
    /// in, nothing was decoded and the refusal carries no spend rather than a
    /// guess. The second half is therefore the shape every production refusal
    /// takes: the coordinator declines to hold a result the worker did pay for,
    /// and reports none of that attempt's own cost. Both halves keep the typed
    /// 422 class, which is what `cap_refusal_error` matches on.
    ///
    /// Mutation proof: the first half is RED when `try_remote`'s terminal arm
    /// stops reading `salvaged`, the same line as
    /// `terminal_error_after_a_summary_carries_what_that_attempt_paid`. The
    /// second half is RED if that arm ever starts inventing a figure where the
    /// decoder reported none.
    #[tokio::test]
    async fn a_byte_cap_refusal_reports_only_the_spend_it_decoded() {
        const PAID: Spend = Spend {
            get_requests: 6,
            get_bytes: 65_536,
            raw_f64_pages: 9,
            raw_f64_bytes: 131_072,
        };

        // Half 1: the summary landed, then the flood tripped the cap.
        let (store, now, request, _local_only) = one_slice_corpus("cap-after-summary-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(PAID, pb::status::Code::Ok, Ending::SummaryThenFlood).await;
        let fetcher = coordinator_over(store, now).with_max_slice_bytes(CAP_UNDER_TEST);
        let wire_bytes = AtomicU64::new(0);
        let (result, _named, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint)], request, &wire_bytes)
            .await;
        let err = result.expect_err("a slice past the byte cap is refused");
        assert_eq!(tries.load(Ordering::Relaxed), 1);
        assert!(!fell_back, "a refusal is terminal, not a routing miss");
        assert!(
            matches!(
                err.unspent(),
                DistribError::Codec(codec::CodecError::SliceByteCapExceeded { .. })
            ),
            "the refusal keeps its typed class under the spend wrapper: {err:?}"
        );
        let spend = err.spend().expect("the refused slice reports what it paid");
        assert_eq!(spend.total_s3_requests(), PAID.get_requests);
        assert_eq!(
            spend.total_s3_bytes(),
            PAID.get_bytes,
            "the worker read these before the coordinator declined to hold the \
             result"
        );
        assert!(
            wire_bytes.load(Ordering::Relaxed) > 0,
            "the wire bytes the refusal made the coordinator hold are reported \
             alongside the store spend"
        );

        // Half 2: the flood came first, so no summary was ever decoded.
        let (store, now, request, _local_only) =
            one_slice_corpus("cap-before-summary-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(PAID, pb::status::Code::Ok, Ending::FloodThenSummary).await;
        let fetcher = coordinator_over(store, now).with_max_slice_bytes(CAP_UNDER_TEST);
        let wire_bytes = AtomicU64::new(0);
        let (result, _named, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint)], request, &wire_bytes)
            .await;
        let err = result.expect_err("a slice past the byte cap is refused");
        assert_eq!(tries.load(Ordering::Relaxed), 1);
        assert!(!fell_back);
        assert!(
            matches!(
                err,
                DistribError::Codec(codec::CodecError::SliceByteCapExceeded { .. })
            ),
            "with nothing decoded there is no spend to carry, so the error is \
             the bare refusal with no wrapper: {err:?}"
        );
        assert!(
            err.spend().is_none(),
            "a refusal before any summary cannot observe the worker's spend"
        );
    }

    /// The two attempts of a slice that ends refused, each with its own spend:
    /// the primary reports `Unavailable` after spending, and the re-dispatch
    /// target is refused by the coordinator's byte cap after its summary
    /// landed. The error the slice ends with must carry BOTH figures, summed.
    ///
    /// No local attempt runs (the refusal is terminal), so both sides of the
    /// sum are exact constants and each counter is asserted against
    /// `PRIMARY + REFUSED`.
    ///
    /// Mutation proof, two lines, one revert each:
    ///
    /// * `AttemptSpend::fold_into`'s `.map_err(|err| err.with_spend(&carried))`
    ///   reverted to returning `result` unchanged on the `Err` arm. The carried
    ///   primary is then dropped and the error reports `REFUSED` alone.
    /// * `DistribError::with_spend`'s `DistribError::Spent { .. }` arm removed,
    ///   so an already-carrying error is wrapped a second time instead of
    ///   merged. `spend()` then reads the outer wrapper only and reports
    ///   `PRIMARY` alone, with `REFUSED` buried under it.
    #[tokio::test]
    async fn a_refused_redispatch_carries_the_primarys_spend_and_its_own() {
        const PRIMARY: Spend = Spend {
            get_requests: 3,
            get_bytes: 4_096,
            raw_f64_pages: 2,
            raw_f64_bytes: 16_384,
        };
        const REFUSED: Spend = Spend {
            get_requests: 5,
            get_bytes: 8_192,
            raw_f64_pages: 7,
            raw_f64_bytes: 57_344,
        };
        // The two attempts must differ per counter, or a sum cannot be told
        // from one attempt's share reported twice.
        const {
            assert!(PRIMARY.get_requests != REFUSED.get_requests);
            assert!(PRIMARY.get_bytes != REFUSED.get_bytes);
        };

        let (store, now, request, _local_only) =
            one_slice_corpus("refused-redispatch-tenant").await;
        let (endpoint_a, tries_a, _keep_a) =
            spawn_scripted(PRIMARY, pb::status::Code::Unavailable, Ending::Summary).await;
        let (endpoint_b, tries_b, _keep_b) =
            spawn_scripted(REFUSED, pb::status::Code::Ok, Ending::SummaryThenFlood).await;

        let fetcher = coordinator_over(store, now).with_max_slice_bytes(CAP_UNDER_TEST);
        let wire_bytes = AtomicU64::new(0);
        let (result, named, fell_back) = fetcher
            .dispatch(
                vec![Owner::Remote(endpoint_a), Owner::Remote(endpoint_b.clone())],
                request,
                &wire_bytes,
            )
            .await;
        let err = result.expect_err("the re-dispatched slice is refused by the byte cap");

        assert_eq!(
            tries_a.load(Ordering::Relaxed),
            1,
            "primary dispatched once"
        );
        assert_eq!(
            tries_b.load(Ordering::Relaxed),
            1,
            "re-dispatched to the next owner once"
        );
        assert!(
            !fell_back,
            "a refusal is terminal, so no coordinator-local attempt ran"
        );
        assert_eq!(
            named, endpoint_b,
            "the stats entry names the worker that was refused"
        );
        assert!(
            matches!(
                err.unspent(),
                DistribError::Codec(codec::CodecError::SliceByteCapExceeded { .. })
            ),
            "one merged wrapper, with the typed refusal still underneath it: \
             {err:?}"
        );

        let spend = err.spend().expect("the failed slice reports what it paid");
        assert_eq!(
            spend.total_s3_requests(),
            PRIMARY.get_requests + REFUSED.get_requests,
            "both attempts' requests, summed (primary {} + refused {})",
            PRIMARY.get_requests,
            REFUSED.get_requests
        );
        assert_eq!(
            spend.total_s3_bytes(),
            PRIMARY.get_bytes + REFUSED.get_bytes,
            "both attempts' bytes, summed (primary {} + refused {})",
            PRIMARY.get_bytes,
            REFUSED.get_bytes
        );
    }

    /// A stream that closes cleanly and still fails to finish: the summary
    /// decodes, carrying its accounting, and its status code is one this build
    /// does not know (what a newer worker sends).
    ///
    /// The fault surfaces out of `SliceStreamDecoder::finish`, not out of
    /// `push`, so it takes the one path in `remote_fetch` where the decoder is
    /// not consulted for a salvage. The attempt is terminal, so that summary's
    /// spend has no later attempt to ride on and must reach accounting on the
    /// error.
    ///
    /// Mutation proof: RED when `remote_fetch`'s `Ok(())` arm goes back to a
    /// bare `decoder.finish()`. `salvaged` is then never set on this path, the
    /// error carries no spend at all, and `spend()` is `None`.
    #[tokio::test]
    async fn an_unknown_status_summary_carries_what_that_attempt_paid() {
        const PAID: Spend = Spend {
            get_requests: 4,
            get_bytes: 12_288,
            raw_f64_pages: 3,
            raw_f64_bytes: 24_576,
        };

        let (store, now, request, _local_only) = one_slice_corpus("unknown-status-tenant").await;
        let (endpoint, tries, _keep) =
            spawn_scripted(PAID, pb::status::Code::Ok, Ending::UnknownStatusSummary).await;

        let fetcher = coordinator_over(store, now);
        let wire_bytes = AtomicU64::new(0);
        let (result, named, fell_back) = fetcher
            .dispatch(vec![Owner::Remote(endpoint.clone())], request, &wire_bytes)
            .await;
        let err = result.expect_err("an unknown status code ends the slice typed");

        assert_eq!(
            tries.load(Ordering::Relaxed),
            1,
            "a decode fault is not re-dispatched"
        );
        assert!(
            !fell_back,
            "and does not fall back to the coordinator either"
        );
        assert_eq!(named, endpoint);
        assert!(
            matches!(
                err.unspent(),
                DistribError::Codec(codec::CodecError::UnknownStatusCode(UNKNOWN_STATUS_CODE))
            ),
            "the spend wrapper keeps the typed class underneath it: {err:?}"
        );

        let spend = err
            .spend()
            .expect("the summary said what the attempt paid before finish refused it");
        assert_eq!(spend.total_s3_requests(), PAID.get_requests);
        assert_eq!(spend.total_s3_bytes(), PAID.get_bytes);
    }

    /// A failed slice's `fragments[]` entry reports the spend its error
    /// carried, not a flat zero (issue #1723).
    ///
    /// This drives `RoutingSliceFetcher::fetch` rather than `dispatch`, because
    /// `record_fragment_stat` sits in `fetch`: the live worker set holds one
    /// scripted worker, and with no self id set the slice routes to it. That
    /// worker sends its summary and then a frame the decoder refuses, so the
    /// slice ends in a terminal error carrying `PAID`.
    ///
    /// Mutation proof: RED when `record_fragment_stat`'s `Err` arm goes back to
    /// reporting `0` instead of `err.spend().map_or(0, ..)`.
    #[tokio::test]
    async fn a_failed_slices_fragment_entry_reports_the_spend_it_carried() {
        const PAID: Spend = Spend {
            get_requests: 4,
            get_bytes: 12_288,
            raw_f64_pages: 3,
            raw_f64_bytes: 24_576,
        };

        let (store, now, request, _local_only) =
            one_slice_corpus("fragment-stat-error-tenant").await;
        let (endpoint, _tries, _keep) =
            spawn_scripted(PAID, pb::status::Code::Ok, Ending::SummaryThenEmptyFrame).await;

        // One live worker and no self id: every slice ranks that worker top, so
        // `fetch` dispatches to it rather than running local.
        let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
            process_id: uuid::Uuid::from_u128(7).to_string(),
            fragment_endpoint: endpoint.clone(),
            flight_sql_endpoint: endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        }])));
        let fetcher = RoutingSliceFetcher::new(
            Arc::new(OnceLock::new()),
            live,
            test_keys(),
            pinned_service(store, now),
            Arc::new(FragmentMetrics::new()),
        );

        let sink = FragmentStatsSink::new();
        with_fragment_stats(sink.clone(), async {
            let err = fetcher
                .fetch(request)
                .await
                .expect_err("an empty frame ends the slice typed");
            assert!(matches!(err.unspent(), DistribError::EmptyFrame), "{err:?}");
        })
        .await;

        let recorded = sink.take();
        assert_eq!(recorded.len(), 1, "one slice, one entry: {recorded:?}");
        let entry = &recorded[0];
        assert_eq!(entry.status, "error");
        assert_eq!(entry.worker_endpoint, endpoint);
        assert_eq!(
            entry.bytes_reported, PAID.get_bytes,
            "a failed fragment reports the bytes its error carried"
        );
    }
}
