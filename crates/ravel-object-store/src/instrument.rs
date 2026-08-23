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
    latency_micros_buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    latency_nanos_total: AtomicU64,
}

impl OpMetrics {
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
        InstrumentedStore {
            inner,
            metrics: Arc::new(StoreMetrics::default()),
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

    /// Passthrough, uncounted. A multipart upload is a handle, not a call:
    /// counting it would mean wrapping the returned [`MultipartUpload`] and
    /// attributing its parts to some [`StoreOp`], and no `StoreOp` describes
    /// them (folding part bytes into `put` would make `put.calls` disagree with
    /// the number of `put()` calls a caller made). Multipart traffic is
    /// therefore invisible to these counters; only compaction writes it, and
    /// its own metrics cover part counts and bytes. `put()`'s own
    /// above-threshold multipart path *is* counted, as one `put`, because that
    /// is what the caller invoked.
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
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

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
    fn every_store_error_variant_has_its_own_class() {
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
