//! Observability-only instrumentation decorator for [`ObjectStoreBackend`]
//! (docs/object-store-contract.md, "Instrumentation decorator").
//!
//! [`InstrumentedStore`] wraps any backend, counts what happened, and returns
//! the wrapped backend's result unchanged. It is **never
//! correctness-bearing**: no counter is read by any durability, visibility, or
//! query-correctness path, and none may become an input to one. Wrapping a
//! backend is a **zero behavior change**: every method delegates, every
//! `Ok`/`Err` value is forwarded verbatim (same bytes, same etag, same
//! `StoreError` variant and message), and [`Capabilities`] passes through
//! untouched, so the server's startup capability gate sees exactly what the
//! wrapped backend reports. If a counter and a caller ever disagree, the
//! caller is right.
//!
//! # What is recorded
//!
//! One [`OpMetrics`] block per [`StoreOp`] (`put`, `get`, `head`, `list`,
//! `list_delimited`, `delete`), all flat `AtomicU64`s in a single
//! [`StoreMetrics`] shared through an `Arc`, snapshot-able with
//! [`StoreMetrics::snapshot`]. This mirrors `ravel_ingest::metrics`:
//! process-global monotonic totals, with no per-tenant and no per-key
//! dimension (a key is unbounded cardinality, and tenant identity is not
//! visible at this layer).
//!
//! Counting conventions, because the totals are easy to misread:
//!
//! - `calls` counts every completed call; `ok` counts the ones that returned
//!   `Ok`. `calls - ok` equals the sum over `errors`, so an error class is
//!   never double counted and no call is missed. A call cancelled by drop
//!   before the inner future resolves records nothing at all, so `calls` is
//!   completions, not attempts.
//! - `attempts` counts billed HTTP requests, retries included, and is the one
//!   figure this decorator does **not** record itself: the retry loop it would
//!   need to see runs *below* the [`ObjectStoreBackend`] boundary, inside
//!   `object_store`'s S3 client (`RetryConfig`, default `max_retries = 10`), so
//!   one `get()` that retried nine times is a single completed `calls`/`get`
//!   here while the provider bills ten requests (issue #928, ADR-0927
//!   decision 8). The gap is one-directional: the real bill is never below
//!   `calls` and, under throttling, is strictly above it. `attempts` is filled
//!   in from the layer that *can* see each retry --- the S3 adapter's counting
//!   HTTP connector ([`crate::s3`]) records one attempt per HTTP request it
//!   issues via [`StoreMetrics::record_attempt`], into the same per-op block, so
//!   `attempts - calls` is the retry (billed) overhead. `attempts >= calls`
//!   holds exactly when every store this decorator counts a `calls` on records
//!   its attempts into the same handle: an [`InstrumentedStore::with_metrics`]
//!   wrapping an [`S3Store`](crate::s3::S3Store) built with
//!   [`S3Store::new`](crate::s3::S3Store::new) (no handle) would count
//!   `calls` while recording no `attempts`, so the relation is a property of the
//!   wiring, not a guarantee of this type. The server wires the whole S3 chain
//!   --- the base store and, under `--tenant-kms-config`, every per-tenant
//!   KMS-routed store (see [`crate::KmsRoutingStore::new`]) --- onto one handle,
//!   so it holds there (issue #928). A backend that issues no HTTP requests (for example
//!   [`crate::memory::MemoryStore`]) leaves `attempts` at zero: there is no bill
//!   and nothing retried. Because a single logical read may fan a whole-object
//!   `GetRange::Full` into an unranged GET cut at the per-request bound plus
//!   ranged GETs for the rest, `attempts` can exceed `calls` for `get` even
//!   with no retry at all; each of those requests is a real billed request. `attempts` for `put` likewise counts every request a
//!   multipart upload issues (create, each part, complete), not one per logical
//!   `put`.
//! - `get_unverified` (`ravel_store_get_unverified_total`) is a store-wide total,
//!   not a per-op block: it counts full-object reads the S3 adapter served
//!   without checking the body against a stored checksum, because the response
//!   carried no `x-amz-checksum-*` header, carried one this adapter cannot
//!   recompute, or arrived as several responses none of which is the
//!   whole object (ADR-1696 decision 3). Like `attempts`, it is recorded by the
//!   adapter's HTTP connector's owner rather than by this decorator, through
//!   [`StoreMetrics::record_get_unverified`]. Zero for a backend that is not the
//!   S3 adapter. A non-zero and *growing* value against an endpoint that is
//!   supposed to store checksums is the signal that it is dropping them.
//! - The bucket-protection control plane (ADR-1727 decision 1) has its own
//!   block, [`ControlPlaneMetricsSnapshot`], read with
//!   [`StoreMetrics::control_plane`] and never through [`StoreOp`] or
//!   [`StoreMetrics::snapshot`]. Its read-only GETs are not data-plane calls,
//!   so counting them under `get` or `list` would add attempts with no
//!   matching `calls` and break `attempts - calls` as the retry overhead.
//!   `requests` counts every GET it sends, before dispatch; `calls` counts
//!   those that got an HTTP response back, whatever its status, so
//!   `requests - calls` is the GETs that got no response at all.
//!   `response_bytes` counts the wire bytes of each response body as received,
//!   error bodies included, before any size check refuses the body; a response
//!   refused on its `Content-Length` reads no body and adds nothing.
//! - `errors[class]` is indexed by [`StoreErrorClass`], one slot per
//!   [`StoreError`] variant. `AlreadyExists` under `CreateIfAbsent` is a
//!   protocol signal rather than a failure (ADR-0002), so a healthy commit
//!   path grows that slot; do not alert on `errors` in aggregate.
//! - `bytes` for `get` is the length of the data actually returned (a ranged
//!   read counts the range, not the object), so a failed `get` adds zero. For
//!   `put` it is the payload length of every attempt, failures included: the
//!   payload is known before the call and those bytes were offered to the
//!   backend whether or not it accepted them. `head`, `list`,
//!   `list_delimited`, and `delete` never move `bytes`.
//! - `latency_micros_buckets` and `latency_nanos_total` cover successes and
//!   failures alike (a slow timeout is exactly the latency an operator wants
//!   to see), measured around the inner call only, so decorator bookkeeping is
//!   outside the measurement.
//!
//! # Time
//!
//! Time is injected, per the repo's no-`SystemTime::now()` rule.
//! [`MonotonicClock`] is the seam: [`InstrumentedStore::new`] installs
//! [`InstantClock`], which is `std::time::Instant`-based (monotonic, immune to
//! wall-clock jumps), and [`InstrumentedStore::with_clock`] takes a fake for
//! deterministic histogram tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;

use crate::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};

/// The operation kinds counted separately. One [`OpMetrics`] block each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreOp {
    Put,
    Get,
    Head,
    List,
    ListDelimited,
    Delete,
}

/// Number of [`StoreOp`] variants; the width of [`StoreMetrics`]'s array.
pub const STORE_OP_COUNT: usize = 6;

impl StoreOp {
    /// Every variant, in `index()` order.
    pub const ALL: [StoreOp; STORE_OP_COUNT] = [
        StoreOp::Put,
        StoreOp::Get,
        StoreOp::Head,
        StoreOp::List,
        StoreOp::ListDelimited,
        StoreOp::Delete,
    ];

    /// Dense array index, stable and `< STORE_OP_COUNT` by construction.
    pub fn index(self) -> usize {
        match self {
            StoreOp::Put => 0,
            StoreOp::Get => 1,
            StoreOp::Head => 2,
            StoreOp::List => 3,
            StoreOp::ListDelimited => 4,
            StoreOp::Delete => 5,
        }
    }

    /// Trait-method name, for labelling an export built on a snapshot.
    pub fn name(self) -> &'static str {
        match self {
            StoreOp::Put => "put",
            StoreOp::Get => "get",
            StoreOp::Head => "head",
            StoreOp::List => "list",
            StoreOp::ListDelimited => "list_delimited",
            StoreOp::Delete => "delete",
        }
    }
}

/// One slot per [`StoreError`] variant, so an error counter never loses the
/// distinction a caller's retry decision turns on (`StoreError::is_retryable`
/// covers `Throttled`, `Timeout`, `Transient`; everything else is terminal).
/// Payloads (`retry_after_ms`, messages) are deliberately dropped: they are
/// unbounded-cardinality label values, and the tracing output already carries
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreErrorClass {
    NotFound,
    AlreadyExists,
    PreconditionFailed,
    AccessDenied,
    Throttled,
    Timeout,
    Corrupted,
    InvalidRange,
    Transient,
    Permanent,
}

/// Number of [`StoreErrorClass`] variants; the width of the `errors` arrays.
pub const STORE_ERROR_CLASS_COUNT: usize = 10;

impl StoreErrorClass {
    /// Every variant, in `index()` order.
    pub const ALL: [StoreErrorClass; STORE_ERROR_CLASS_COUNT] = [
        StoreErrorClass::NotFound,
        StoreErrorClass::AlreadyExists,
        StoreErrorClass::PreconditionFailed,
        StoreErrorClass::AccessDenied,
        StoreErrorClass::Throttled,
        StoreErrorClass::Timeout,
        StoreErrorClass::Corrupted,
        StoreErrorClass::InvalidRange,
        StoreErrorClass::Transient,
        StoreErrorClass::Permanent,
    ];

    /// Classify an error. Total over [`StoreError`]: adding a variant there
    /// fails to compile here rather than silently landing in a catch-all.
    pub fn of(err: &StoreError) -> Self {
        match err {
            StoreError::NotFound => StoreErrorClass::NotFound,
            StoreError::AlreadyExists => StoreErrorClass::AlreadyExists,
            StoreError::PreconditionFailed => StoreErrorClass::PreconditionFailed,
            StoreError::AccessDenied(_) => StoreErrorClass::AccessDenied,
            StoreError::Throttled { .. } => StoreErrorClass::Throttled,
            StoreError::Timeout => StoreErrorClass::Timeout,
            StoreError::Corrupted(_) => StoreErrorClass::Corrupted,
            StoreError::InvalidRange(_) => StoreErrorClass::InvalidRange,
            StoreError::Transient(_) => StoreErrorClass::Transient,
            StoreError::Permanent(_) => StoreErrorClass::Permanent,
            // The drain helpers ([`crate::list_all`]) synthesize these when a
            // backend violates the listing contract; they never flow through
            // this decorator, which records raw `list` results, not drain
            // outcomes. They are permanent, non-retryable client-side failures,
            // so they classify as `Permanent`. Named explicitly, not via a
            // wildcard, so a future variant still fails to compile here.
            StoreError::ListRepeatedToken { .. }
            | StoreError::ListPageCeiling { .. }
            | StoreError::ListOrderViolation { .. } => StoreErrorClass::Permanent,
            // A capability the backend does not have, and a mutation of a
            // store opened read-only: both are client-side configuration
            // failures that no retry and no other argument can fix, so they
            // class as `Permanent` rather than widening the class enum (and
            // with it every exported metrics array). Named explicitly, not via
            // a wildcard, for the same reason as the variants above.
            StoreError::Unsupported { .. } | StoreError::ReadOnly { .. } => {
                StoreErrorClass::Permanent
            }
        }
    }

    /// Dense array index, stable and `< STORE_ERROR_CLASS_COUNT`.
    pub fn index(self) -> usize {
        match self {
            StoreErrorClass::NotFound => 0,
            StoreErrorClass::AlreadyExists => 1,
            StoreErrorClass::PreconditionFailed => 2,
            StoreErrorClass::AccessDenied => 3,
            StoreErrorClass::Throttled => 4,
            StoreErrorClass::Timeout => 5,
            StoreErrorClass::Corrupted => 6,
            StoreErrorClass::InvalidRange => 7,
            StoreErrorClass::Transient => 8,
            StoreErrorClass::Permanent => 9,
        }
    }

    /// Variant name, for labelling an export built on a snapshot.
    pub fn name(self) -> &'static str {
        match self {
            StoreErrorClass::NotFound => "not_found",
            StoreErrorClass::AlreadyExists => "already_exists",
            StoreErrorClass::PreconditionFailed => "precondition_failed",
            StoreErrorClass::AccessDenied => "access_denied",
            StoreErrorClass::Throttled => "throttled",
            StoreErrorClass::Timeout => "timeout",
            StoreErrorClass::Corrupted => "corrupted",
            StoreErrorClass::InvalidRange => "invalid_range",
            StoreErrorClass::Transient => "transient",
            StoreErrorClass::Permanent => "permanent",
        }
    }
}

/// Inclusive upper bounds, in microseconds, of the fixed latency histogram.
/// Bucket `i` counts observations with `bounds[i-1] < d <= bounds[i]`; bucket
/// 0 counts `d <= 100us`. Fixed rather than configurable so a snapshot is
/// comparable across processes without carrying its own schema.
pub const LATENCY_BUCKET_BOUNDS_MICROS: [u64; 12] = [
    100, 500, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000,
];

/// Histogram width: one bucket per bound plus a final overflow bucket for
/// anything slower than the largest bound (over 5s).
pub const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_BOUNDS_MICROS.len() + 1;

/// The bucket a duration falls in. Sub-microsecond durations truncate to 0us
/// and land in bucket 0; anything past the last bound lands in the overflow
/// bucket (`LATENCY_BUCKET_COUNT - 1`).
fn latency_bucket(elapsed_nanos: u64) -> usize {
    let micros = elapsed_nanos / 1_000;
    LATENCY_BUCKET_BOUNDS_MICROS
        .iter()
        .position(|bound| micros <= *bound)
        .unwrap_or(LATENCY_BUCKET_COUNT - 1)
}

/// Monotonic time seam. Implementations must be non-decreasing across calls
/// from any thread; the decorator subtracts two readings with
/// `saturating_sub`, so a violation costs a mis-bucketed observation and
/// nothing more.
pub trait MonotonicClock: Send + Sync + 'static {
    /// Nanoseconds since an arbitrary, implementation-chosen origin. Only
    /// differences are meaningful; this is never a wall-clock timestamp.
    fn now_nanos(&self) -> u64;
}

/// Default clock: `std::time::Instant` elapsed since construction. Monotonic
/// by the standard library's contract and unaffected by wall-clock jumps, and
/// never `SystemTime::now()`.
#[derive(Debug)]
pub struct InstantClock {
    origin: Instant,
}

impl InstantClock {
    pub fn new() -> Self {
        InstantClock {
            origin: Instant::now(),
        }
    }
}

impl Default for InstantClock {
    fn default() -> Self {
        InstantClock::new()
    }
}

impl MonotonicClock for InstantClock {
    fn now_nanos(&self) -> u64 {
        // Saturating: 2^64 ns is ~584 years of uptime, so this cannot be
        // reached in practice, and clamping beats a panic in a counter path.
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Counters for one operation kind. See the [module docs](self) for what each
/// one counts and, more importantly, what it does not.
#[derive(Debug, Default)]
pub struct OpMetrics {
    calls: AtomicU64,
    ok: AtomicU64,
    errors: [AtomicU64; STORE_ERROR_CLASS_COUNT],
    bytes: AtomicU64,
    /// Billed HTTP requests, retries included. Recorded by the S3 adapter's
    /// counting connector, never by this decorator; see the [module docs](self).
    attempts: AtomicU64,
    latency_micros_buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    latency_nanos_total: AtomicU64,
}

impl OpMetrics {
    /// One billed HTTP request (an attempt, retries included). Separate from
    /// [`record`](Self::record), which counts one completed logical call.
    fn record_attempt(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record(&self, elapsed_nanos: u64, bytes: u64, err: Option<&StoreError>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match err {
            None => {
                self.ok.fetch_add(1, Ordering::Relaxed);
            }
            Some(err) => {
                self.errors[StoreErrorClass::of(err).index()].fetch_add(1, Ordering::Relaxed);
            }
        }
        if bytes > 0 {
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.latency_micros_buckets[latency_bucket(elapsed_nanos)].fetch_add(1, Ordering::Relaxed);
        self.latency_nanos_total
            .fetch_add(elapsed_nanos, Ordering::Relaxed);
    }

    fn snapshot(&self) -> OpMetricsSnapshot {
        let mut errors = [0u64; STORE_ERROR_CLASS_COUNT];
        for (slot, counter) in errors.iter_mut().zip(self.errors.iter()) {
            *slot = counter.load(Ordering::Relaxed);
        }
        let mut latency_micros_buckets = [0u64; LATENCY_BUCKET_COUNT];
        for (slot, counter) in latency_micros_buckets
            .iter_mut()
            .zip(self.latency_micros_buckets.iter())
        {
            *slot = counter.load(Ordering::Relaxed);
        }
        OpMetricsSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            ok: self.ok.load(Ordering::Relaxed),
            errors,
            bytes: self.bytes.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            latency_micros_buckets,
            latency_nanos_total: self.latency_nanos_total.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of one [`OpMetrics`] block. Plain `u64`s and fixed-size
/// arrays, so a snapshot is `Copy` and needs no allocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpMetricsSnapshot {
    /// Completed calls (a call cancelled by drop is not counted).
    pub calls: u64,
    /// Calls that returned `Ok`.
    pub ok: u64,
    /// Failures by [`StoreErrorClass::index`]. Sums to `calls - ok`.
    pub errors: [u64; STORE_ERROR_CLASS_COUNT],
    /// Bytes returned (`get`) or offered (`put`); zero for every other op.
    pub bytes: u64,
    /// Billed HTTP requests, retries included (issue #928). `>= calls` when
    /// every store the decorator counts shares this handle (the wiring the
    /// module docs describe); `attempts - calls` is then the retry (billed)
    /// overhead `calls` alone hides. Zero for a non-HTTP backend. See the
    /// [module docs](self) for why this decorator does not record it itself.
    pub attempts: u64,
    /// Fixed histogram over [`LATENCY_BUCKET_BOUNDS_MICROS`] plus overflow.
    pub latency_micros_buckets: [u64; LATENCY_BUCKET_COUNT],
    /// Summed latency of every completed call, for an exact mean the buckets
    /// alone cannot give.
    pub latency_nanos_total: u64,
}

impl OpMetricsSnapshot {
    /// Failures in one class.
    pub fn error_count(&self, class: StoreErrorClass) -> u64 {
        self.errors[class.index()]
    }

    /// Failures across every class. Equals `calls - ok`.
    pub fn errors_total(&self) -> u64 {
        self.errors.iter().sum()
    }
}

/// Shared metrics handle: one instance per [`InstrumentedStore`], held behind
/// an `Arc` and clonable out of the decorator with
/// [`InstrumentedStore::metrics`] so a scrape path can read counters without
/// touching the store.
#[derive(Debug, Default)]
pub struct StoreMetrics {
    ops: [OpMetrics; STORE_OP_COUNT],
    /// `ravel_store_get_unverified_total`: full-object reads served without a
    /// checksum check (ADR-1696 decision 3). Store-wide rather than per-op:
    /// only `get` can move it, so a per-op block would be five permanent zeros.
    get_unverified: AtomicU64,
    /// The bucket-protection control plane's own block; see the
    /// [module docs](self).
    control_plane: ControlPlaneMetrics,
}

/// Counters for the bucket-protection control plane's read-only GETs
/// (ADR-1727 decision 1). See the [module docs](self) for what each counts.
#[derive(Debug, Default)]
struct ControlPlaneMetrics {
    requests: AtomicU64,
    calls: AtomicU64,
    response_bytes: AtomicU64,
}

/// Point-in-time copy of the control plane's block, read with
/// [`StoreMetrics::control_plane`]. Not part of [`StoreMetricsSnapshot`]: the
/// block is no [`StoreOp`], so an exporter iterating [`StoreOp::ALL`] never
/// sees it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControlPlaneMetricsSnapshot {
    /// GETs sent, counted before dispatch.
    pub requests: u64,
    /// GETs that got an HTTP response back, whatever its status.
    pub calls: u64,
    /// Wire bytes of the response bodies, as received.
    pub response_bytes: u64,
}

impl StoreMetrics {
    fn op(&self, op: StoreOp) -> &OpMetrics {
        &self.ops[op.index()]
    }

    /// Record one completed call. `bytes` is the payload length for `put`, the
    /// returned data length for a successful `get`, and 0 otherwise.
    fn record(&self, op: StoreOp, elapsed_nanos: u64, bytes: u64, err: Option<&StoreError>) {
        self.op(op).record(elapsed_nanos, bytes, err);
    }

    /// Record one billed HTTP request (an attempt, retries included) for `op`,
    /// into the same per-op block [`snapshot`](Self::snapshot) reads as
    /// `attempts`. This is the seam the S3 adapter's counting HTTP connector
    /// ([`crate::s3`]) records through: it fires once per HTTP request it issues,
    /// so `attempts` sees `object_store`'s internal retries that a completed
    /// `calls` never does (issue #928). It touches no other counter, so a caller
    /// that records attempts and a decorator that records completions never
    /// contend on the same field. See the [module docs](self).
    pub fn record_attempt(&self, op: StoreOp) {
        self.op(op).record_attempt();
    }

    /// Record one full-object read served without verifying it against a
    /// stored checksum (`ravel_store_get_unverified_total`, ADR-1696
    /// decision 3). The S3 adapter records this once per logical full-object
    /// `get`, not once per HTTP request, so a large object split into several
    /// bounded requests counts one unverified read rather than one per chunk. It touches no other counter.
    pub fn record_get_unverified(&self) {
        self.get_unverified.fetch_add(1, Ordering::Relaxed);
    }

    /// Current value of `ravel_store_get_unverified_total`, for a caller that
    /// wants the one counter without taking a whole [`snapshot`](Self::snapshot).
    pub fn get_unverified(&self) -> u64 {
        self.get_unverified.load(Ordering::Relaxed)
    }

    /// One control-plane GET about to be sent. Touches no per-op block.
    pub(crate) fn record_control_plane_request(&self) {
        self.control_plane.requests.fetch_add(1, Ordering::Relaxed);
    }

    /// One control-plane GET that got an HTTP response back.
    pub(crate) fn record_control_plane_call(&self) {
        self.control_plane.calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Response-body bytes of a control-plane GET, as received.
    pub(crate) fn record_control_plane_response_bytes(&self, bytes: u64) {
        self.control_plane
            .response_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Point-in-time copy of the control plane's block. Separate from
    /// [`snapshot`](Self::snapshot), which covers the data plane only.
    pub fn control_plane(&self) -> ControlPlaneMetricsSnapshot {
        ControlPlaneMetricsSnapshot {
            requests: self.control_plane.requests.load(Ordering::Relaxed),
            calls: self.control_plane.calls.load(Ordering::Relaxed),
            response_bytes: self.control_plane.response_bytes.load(Ordering::Relaxed),
        }
    }

    /// Record one completed call from outside this module, using the same
    /// accounting [`InstrumentedStore`] applies internally.
    ///
    /// This is the seam the per-class request scheduler
    /// ([`crate::scheduling::ClassedStore`], ADR-0070 decision 1) records
    /// through: it holds one [`StoreMetrics`] per request class and calls this
    /// with the class's block, so per-class metrics reuse this exact metric
    /// family with a `{class}` dimension rather than a parallel one. `bytes`
    /// follows the module conventions (payload length for `put`, returned data
    /// length for a successful `get`, 0 otherwise); `elapsed_nanos` is measured
    /// around the inner call only.
    pub fn record_op(&self, op: StoreOp, elapsed_nanos: u64, bytes: u64, err: Option<&StoreError>) {
        self.record(op, elapsed_nanos, bytes, err);
    }

    /// Point-in-time copy of every counter. Not atomic across operation
    /// kinds: concurrent calls may land between two fields, so a snapshot can
    /// show `put` from a hair later than `get`. It is a scrape, not a
    /// consistent cut, and nothing correctness-bearing reads it.
    pub fn snapshot(&self) -> StoreMetricsSnapshot {
        StoreMetricsSnapshot {
            put: self.op(StoreOp::Put).snapshot(),
            get: self.op(StoreOp::Get).snapshot(),
            head: self.op(StoreOp::Head).snapshot(),
            list: self.op(StoreOp::List).snapshot(),
            list_delimited: self.op(StoreOp::ListDelimited).snapshot(),
            delete: self.op(StoreOp::Delete).snapshot(),
            get_unverified: self.get_unverified.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of a whole [`StoreMetrics`], one sub-struct per
/// operation kind (the shape `IngestMetricsSnapshot` uses: plain fields, no
/// interior mutability, no allocation).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreMetricsSnapshot {
    pub put: OpMetricsSnapshot,
    pub get: OpMetricsSnapshot,
    pub head: OpMetricsSnapshot,
    pub list: OpMetricsSnapshot,
    pub list_delimited: OpMetricsSnapshot,
    pub delete: OpMetricsSnapshot,
    /// `ravel_store_get_unverified_total`: full-object reads the S3 adapter
    /// served without checking the body against a stored checksum (ADR-1696
    /// decision 3). Store-wide, not per-op; see the [module docs](self).
    pub get_unverified: u64,
}

impl StoreMetricsSnapshot {
    /// Total LIST-family calls: paged [`list`](ObjectStoreBackend::list) plus
    /// [`list_delimited`](ObjectStoreBackend::list_delimited).
    ///
    /// Both are one S3 `LIST` request, billed identically, so a request-cost
    /// model wants them summed. The two `OpMetrics` blocks stay separate for
    /// diagnostics; this is the seam a caller that only cares about billable
    /// request count reads instead of forgetting `list_delimited` and
    /// undercounting.
    pub fn list_calls(&self) -> u64 {
        self.list.calls + self.list_delimited.calls
    }

    /// One operation's block, for iterating [`StoreOp::ALL`] in an exporter.
    pub fn op(&self, op: StoreOp) -> &OpMetricsSnapshot {
        match op {
            StoreOp::Put => &self.put,
            StoreOp::Get => &self.get,
            StoreOp::Head => &self.head,
            StoreOp::List => &self.list,
            StoreOp::ListDelimited => &self.list_delimited,
            StoreOp::Delete => &self.delete,
        }
    }
}

/// Counting decorator over any [`ObjectStoreBackend`]. Observability only:
/// every method delegates to the wrapped backend and forwards its result
/// unchanged, `capabilities()` passes through verbatim, and no counter feeds
/// any correctness decision. See the [module docs](self).
pub struct InstrumentedStore<S: ObjectStoreBackend> {
    inner: S,
    metrics: Arc<StoreMetrics>,
    clock: Arc<dyn MonotonicClock>,
}

impl<S: ObjectStoreBackend> InstrumentedStore<S> {
    /// Wrap `inner`, timing with the process's monotonic clock.
    pub fn new(inner: S) -> Self {
        InstrumentedStore::with_clock(inner, Arc::new(InstantClock::new()))
    }

    /// Wrap `inner` with an injected clock, for deterministic latency tests.
    pub fn with_clock(inner: S, clock: Arc<dyn MonotonicClock>) -> Self {
        Self::with_clock_and_metrics(inner, clock, Arc::new(StoreMetrics::default()))
    }

    /// Wrap `inner` recording into a caller-supplied [`StoreMetrics`], so the
    /// `attempts` counter the S3 adapter's counting connector fills in
    /// ([`StoreMetrics::record_attempt`], issue #928) and the `calls` counter
    /// this decorator fills in land in one shared block, read by a single
    /// [`snapshot`](StoreMetrics::snapshot). Build the `Arc` first, hand a clone
    /// to `S3Store` so its connector records attempts into it, and pass the same
    /// `Arc` here; [`metrics`](Self::metrics) then returns it. `new`/`with_clock`
    /// keep their own private block (no attempts source), unchanged.
    pub fn with_metrics(inner: S, metrics: Arc<StoreMetrics>) -> Self {
        Self::with_clock_and_metrics(inner, Arc::new(InstantClock::new()), metrics)
    }

    /// Wrap `inner` with both an injected clock and a shared [`StoreMetrics`].
    pub fn with_clock_and_metrics(
        inner: S,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<StoreMetrics>,
    ) -> Self {
        InstrumentedStore {
            inner,
            metrics,
            clock,
        }
    }

    /// Clone the shared metrics handle out, e.g. to hand to a scrape path.
    pub fn metrics(&self) -> Arc<StoreMetrics> {
        self.metrics.clone()
    }

    /// Borrow the wrapped backend, e.g. to assert on its state in tests.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn record<T>(&self, op: StoreOp, start_nanos: u64, bytes: u64, result: &Result<T, StoreError>) {
        let elapsed = self.clock.now_nanos().saturating_sub(start_nanos);
        self.metrics
            .record(op, elapsed, bytes, result.as_ref().err());
    }
}

#[async_trait::async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for InstrumentedStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        // Payload length is read before the move into the inner call, and is
        // counted whether or not the backend accepts the write.
        let bytes = data.len() as u64;
        let start = self.clock.now_nanos();
        let result = self.inner.put(key, data, opts).await;
        self.record(StoreOp::Put, start, bytes, &result);
        result
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.get(key, range).await;
        // Bytes actually handed back: a ranged read counts the range, not
        // `total_size`, and a failed read counts nothing.
        let bytes = result
            .as_ref()
            .map_or(0, |outcome| outcome.data.len() as u64);
        self.record(StoreOp::Get, start, bytes, &result);
        result
    }

    /// Counted as a [`StoreOp::Get`], identically to [`Self::get`]: a pinned
    /// read is one GET on the wire and costs the same, so splitting it into
    /// its own op would make a caller's GET count depend on which read path it
    /// took. A refused precondition lands in the `PreconditionFailed` error
    /// class and counts zero bytes.
    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &crate::Pin,
    ) -> Result<crate::PinnedRead, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.get_pinned(key, range, pin).await;
        let bytes = result
            .as_ref()
            .map_or(0, |read| read.outcome.data.len() as u64);
        self.record(StoreOp::Get, start, bytes, &result);
        result
    }

    /// Counted as a [`StoreOp::Get`] for the same reason as
    /// [`Self::get_pinned`]: it is the same GET, with the object's version
    /// reported alongside the bytes.
    async fn get_with_pin(
        &self,
        key: &str,
        range: GetRange,
    ) -> Result<crate::PinnedRead, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.get_with_pin(key, range).await;
        let bytes = result
            .as_ref()
            .map_or(0, |read| read.outcome.data.len() as u64);
        self.record(StoreOp::Get, start, bytes, &result);
        result
    }

    /// One HEAD on the wire, counted as [`StoreOp::Head`] like [`Self::head`].
    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, crate::Pin), StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.pin_of(key).await;
        self.record(StoreOp::Head, start, 0, &result);
        result
    }

    /// Passthrough, uncounted. A multipart upload is a handle, not a call:
    /// counting it here would mean wrapping the returned [`MultipartUpload`]
    /// and attributing its parts to some [`StoreOp`], and folding part bytes
    /// into `put` would make this decorator's `put.calls` disagree with the
    /// number of `put()` calls a caller made. Explicit multipart traffic is
    /// therefore invisible to these counters. `put()`'s own above-threshold
    /// multipart path *is* counted, as one `put`, because that is what the
    /// caller invoked.
    ///
    /// The decision is local to this decorator. The per-class counters of
    /// [`crate::scheduling::ClassedStore`] count each explicit `put_part` as
    /// one `put` (see `ScheduledMultipartUpload`), so for the same explicit
    /// multipart traffic the per-class `put` block reads one call per part
    /// while this block reads none. The two count it differently on purpose.
    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.head(key).await;
        self.record(StoreOp::Head, start, 0, &result);
        result
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.list(prefix, page).await;
        // One call is one page, so a full drain of N pages is N calls.
        self.record(StoreOp::List, start, 0, &result);
        result
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.list_after(prefix, start_after, page).await;
        // A start-after page is still one LIST, counted the same as `list`.
        self.record(StoreOp::List, start, 0, &result);
        result
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.list_delimited(prefix).await;
        self.record(StoreOp::ListDelimited, start, 0, &result);
        result
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let start = self.clock.now_nanos();
        let result = self.inner.delete(key).await;
        self.record(StoreOp::Delete, start, 0, &result);
        result
    }

    /// Passthrough, unchanged. The server's startup capability gate must see
    /// the wrapped backend's declaration, never a decorator's opinion of it.
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    /// Passthrough (ADR-1685 decision 1). Answering `None` here would leave a
    /// wrapped S3 store's observation invisible to the writer's clock-lag
    /// check, silently disabling it for every production process, since
    /// `ravel-server` wraps its backend in this decorator unconditionally.
    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The decorator reports the wrapped backend's store-clock observation
    /// rather than the trait default (ADR-1685 decision 1). `ravel-server`
    /// wraps every backend in this, so a `None` here would disable the
    /// writer's clock-lag check in every production process.
    #[test]
    fn observed_store_time_delegates_to_the_inner_store() {
        let inner = crate::memory::MemoryStore::new();
        inner.set_observed_store_time_ns(Some(1_700_000_000_123_456_789));
        let store = InstrumentedStore::new(inner);
        assert_eq!(
            store.observed_store_time_ns(),
            Some(1_700_000_000_123_456_789)
        );
    }

    #[test]
    fn op_and_error_class_indices_are_dense_and_stable() {
        for (expected, op) in StoreOp::ALL.iter().enumerate() {
            assert_eq!(op.index(), expected, "{} index moved", op.name());
        }
        for (expected, class) in StoreErrorClass::ALL.iter().enumerate() {
            assert_eq!(class.index(), expected, "{} index moved", class.name());
        }
    }

    #[test]
    fn every_error_class_is_reachable_and_the_shared_variants_are_pinned() {
        let errors = [
            (StoreError::NotFound, StoreErrorClass::NotFound),
            (StoreError::AlreadyExists, StoreErrorClass::AlreadyExists),
            (
                StoreError::PreconditionFailed,
                StoreErrorClass::PreconditionFailed,
            ),
            (
                StoreError::AccessDenied("nope".into()),
                StoreErrorClass::AccessDenied,
            ),
            (
                StoreError::Throttled { retry_after_ms: 5 },
                StoreErrorClass::Throttled,
            ),
            (StoreError::Timeout, StoreErrorClass::Timeout),
            (
                StoreError::Corrupted("crc".into()),
                StoreErrorClass::Corrupted,
            ),
            (
                StoreError::InvalidRange("empty".into()),
                StoreErrorClass::InvalidRange,
            ),
            (
                StoreError::Transient("blip".into()),
                StoreErrorClass::Transient,
            ),
            (
                StoreError::Permanent("gone".into()),
                StoreErrorClass::Permanent,
            ),
        ];
        assert_eq!(
            errors.len(),
            STORE_ERROR_CLASS_COUNT,
            "every error class must be covered"
        );
        for (err, expected) in &errors {
            assert_eq!(StoreErrorClass::of(err), *expected, "misclassified {err:?}");
        }

        // The two variants that share a class rather than owning one. They are
        // both caller-side facts about the backend's shape, not about the
        // request, so they read as permanent and widening the class enum would
        // renumber every exported metrics array.
        for err in [
            StoreError::Unsupported {
                operation: "conditional get".into(),
            },
            StoreError::ReadOnly {
                operation: "put".into(),
                store: "external".into(),
            },
        ] {
            assert_eq!(
                StoreErrorClass::of(&err),
                StoreErrorClass::Permanent,
                "misclassified {err:?}"
            );
        }
    }

    #[test]
    fn latency_buckets_are_upper_bound_inclusive() {
        // Sub-microsecond and exact-boundary observations both belong to the
        // first bucket; a value one microsecond past a bound moves up one.
        assert_eq!(latency_bucket(0), 0);
        assert_eq!(latency_bucket(999), 0);
        assert_eq!(latency_bucket(100_000), 0, "100us is bucket 0's bound");
        assert_eq!(latency_bucket(101_000), 1);
        assert_eq!(latency_bucket(500_000), 1, "500us is bucket 1's bound");
        assert_eq!(latency_bucket(501_000), 2);
        assert_eq!(latency_bucket(5_000_000_000), LATENCY_BUCKET_COUNT - 2);
        assert_eq!(
            latency_bucket(5_000_001_000),
            LATENCY_BUCKET_COUNT - 1,
            "past the last bound is the overflow bucket"
        );
        assert_eq!(latency_bucket(u64::MAX), LATENCY_BUCKET_COUNT - 1);
    }

    #[test]
    fn recorded_calls_split_into_ok_and_error_classes() {
        let metrics = StoreMetrics::default();
        metrics.record(StoreOp::Get, 50_000, 7, None);
        metrics.record(StoreOp::Get, 50_000, 0, Some(&StoreError::NotFound));
        metrics.record(
            StoreOp::Get,
            50_000,
            0,
            Some(&StoreError::Throttled { retry_after_ms: 1 }),
        );

        let snap = metrics.snapshot();
        assert_eq!(snap.get.calls, 3);
        assert_eq!(snap.get.ok, 1);
        assert_eq!(snap.get.bytes, 7);
        assert_eq!(snap.get.error_count(StoreErrorClass::NotFound), 1);
        assert_eq!(snap.get.error_count(StoreErrorClass::Throttled), 1);
        assert_eq!(snap.get.errors_total(), snap.get.calls - snap.get.ok);
        assert_eq!(snap.get.latency_nanos_total, 150_000);
        assert_eq!(snap.put, OpMetricsSnapshot::default(), "no put recorded");
    }

    #[test]
    fn attempts_are_separate_from_calls_and_do_not_touch_other_counters() {
        // A get() that the backend retried twice below the trait boundary: the
        // connector records three billed attempts, the decorator one completion.
        // `attempts` must read 3 and `calls` 1, so `attempts - calls` is exactly
        // the retry overhead #928 exists to expose, and no other counter moves.
        let metrics = StoreMetrics::default();
        metrics.record_attempt(StoreOp::Get);
        metrics.record_attempt(StoreOp::Get);
        metrics.record_attempt(StoreOp::Get);
        metrics.record(StoreOp::Get, 10_000, 42, None);

        let snap = metrics.snapshot();
        assert_eq!(snap.get.attempts, 3, "three billed HTTP requests");
        assert_eq!(snap.get.calls, 1, "one completed logical call");
        assert_eq!(snap.get.ok, 1);
        assert_eq!(snap.get.bytes, 42);
        assert_eq!(
            snap.get.errors_total(),
            0,
            "recording attempts must not move any error class"
        );
        // A quiet op with no attempts recorded reads exactly zero, so the figure
        // never drifts on a backend that issues no HTTP (e.g. MemoryStore).
        assert_eq!(snap.put.attempts, 0);
        assert_eq!(snap.head.attempts, 0);
    }

    /// `ravel_store_get_unverified_total` is store-wide and independent of
    /// every per-op counter (ADR-1696 decision 3): a read that was served
    /// without a checksum check is still an ordinary successful `get`, so
    /// recording one must not touch `calls`, `ok`, `errors` or `attempts`, and
    /// the two accessors must agree.
    #[test]
    fn get_unverified_is_store_wide_and_touches_no_op_counter() {
        let metrics = StoreMetrics::default();
        metrics.record(StoreOp::Get, 10_000, 42, None);
        metrics.record_get_unverified();
        metrics.record_get_unverified();

        let snap = metrics.snapshot();
        assert_eq!(snap.get_unverified, 2, "two unverified full-object reads");
        assert_eq!(
            metrics.get_unverified(),
            snap.get_unverified,
            "the direct accessor and the snapshot must read one counter"
        );
        assert_eq!(snap.get.calls, 1, "the read itself is one ordinary call");
        assert_eq!(snap.get.ok, 1);
        assert_eq!(snap.get.attempts, 0);
        assert_eq!(snap.get.errors_total(), 0);
        assert_eq!(
            StoreMetrics::default().snapshot().get_unverified,
            0,
            "a store that recorded nothing reads exactly zero"
        );
    }

    /// A pinned read is a GET on the wire and is billed as one: it lands in the
    /// same `StoreOp::Get` block as an unpinned read, with its bytes, and a
    /// refusal lands in that block's `PreconditionFailed` class. Anything else
    /// would make the cost of a pinned read invisible to the per-phase
    /// accounting every read path reports.
    #[tokio::test]
    async fn get_pinned_is_billed_as_a_get_with_its_bytes_and_its_refusals() {
        use crate::memory::MemoryStore;
        use crate::{GetRange, ObjectStoreBackend, Pin, PutOptions};
        use bytes::Bytes;

        let store = InstrumentedStore::new(MemoryStore::new());
        store
            .put(
                "pinned/k",
                Bytes::from_static(b"0123456789"),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let meta = store.head("pinned/k").await.expect("head");
        let pin = Pin::etag(meta.etag.0.clone());

        let got = store
            .get_pinned("pinned/k", GetRange::Range(0, 4), &pin)
            .await
            .expect("a matching pin is served");
        assert_eq!(&got.outcome.data[..], b"0123");

        let err = store
            .get_pinned("pinned/k", GetRange::Range(0, 4), &Pin::etag("\"0\""))
            .await
            .expect_err("a wrong pin is refused");
        assert!(matches!(err, StoreError::PreconditionFailed), "got {err:?}");

        let snap = store.metrics().snapshot();
        assert_eq!(snap.get.calls, 2, "both pinned reads are GET calls");
        assert_eq!(snap.get.ok, 1);
        assert_eq!(snap.get.bytes, 4, "only the served range is charged");
        assert_eq!(snap.get.error_count(StoreErrorClass::PreconditionFailed), 1);
        assert_eq!(snap.head.calls, 1, "the head is billed separately");
        assert_eq!(snap.put.calls, 1);
    }

    #[test]
    fn snapshot_op_accessor_matches_recorded_op() {
        let metrics = StoreMetrics::default();
        for op in StoreOp::ALL {
            for _ in 0..=op.index() {
                metrics.record(op, 1_000, 0, None);
            }
        }
        let snap = metrics.snapshot();
        for op in StoreOp::ALL {
            assert_eq!(
                snap.op(op).calls,
                op.index() as u64 + 1,
                "{} block mismatched",
                op.name()
            );
        }
    }
}
