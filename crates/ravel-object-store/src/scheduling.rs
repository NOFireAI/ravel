//! Two-class request scheduling for object-store traffic (ADR-0070 decision 1).
//!
//! Every process shares one `Arc<dyn ObjectStoreBackend>`. Foreground,
//! ack-bearing traffic (ingest and commit PUTs, query segment GETs, resolve
//! LISTs) and background traffic (compaction, fold, sweep, scrub, audit
//! retention) meet in the same connection pool with no prioritization. Request
//! class is *not decidable from the key* -- foreground query fetch and
//! background compaction GET the same `t/<hash>/<sig>/l0/...` objects -- so the
//! [`KmsRoutingStore`](crate::kms_routing) "one seam, key decides" shape does
//! not apply. Class is a property of the *caller*, so it attaches where callers
//! get their store handle.
//!
//! [`ClassedStore`] wraps an inner store and hands out two
//! [`ObjectStoreBackend`] handles, [`ClassedStore::foreground`] and
//! [`ClassedStore::background`], that share one [`RequestScheduler`]. Each
//! handle, on every op, acquires a permit of its class from the shared
//! scheduler, then delegates to the inner store. There is no trait change, no
//! per-request parameter, and no key sniffing.
//!
//! # The scheduler: a weighted pair of semaphores
//!
//! Foreground requests are admitted up to `fg_permits` (the global in-flight
//! cap; foreground may use all of it). Background requests are admitted up to
//! `bg_permits` *and additionally yield when foreground waiters exist*. The
//! rule is strict-priority-with-floor:
//!
//! - **Foreground priority.** While any foreground acquire is waiting, a
//!   background acquire that already holds at least `bg_floor` in-flight
//!   permits stops taking new ones. It does not sit in the global queue ahead
//!   of foreground, so a released permit reaches the waiting foreground rather
//!   than a fresh background request. A foreground acquire is therefore never
//!   delayed by more than one in-flight background request (the one already
//!   holding the permit it needs).
//! - **Background floor.** Background never starves completely. Its first
//!   `bg_floor` (>= 1) concurrent requests compete for the global permit
//!   *fairly* (a FIFO queue slot), so they make progress even against a
//!   saturating foreground stream. Only requests beyond the floor yield.
//!
//! The fair queue slot is not preemptible: once a background request is queued
//! on the global semaphore, a later foreground acquire cannot displace it,
//! because tokio hands a released permit straight to the queue head. Entry into
//! that wait is therefore *reserved* before the wait begins (`bg_committed`),
//! not counted after the permit is taken; counting after would let N background
//! requests all read a below-floor count, all commit to the un-preemptible
//! wait, and turn a floor of one into a floor of N. Every counter the admission
//! decision reads (`fg_waiters`, `bg_committed`) is held by an RAII guard, so a
//! cancelled acquire -- a `select!` timeout, a client disconnect, shutdown --
//! releases it on the way out instead of pinning the decision forever.
//!
//! # Off by default (ADR-0070 decision 2)
//!
//! [`ClassedStore::passthrough`] is the default construction: both handles are
//! the inner store itself (`Arc::clone`), so every op delegates directly with
//! no permit acquire and no added latency -- byte-for-byte today's behavior.
//! [`ClassedStore::scheduled`] is opt-in and installs the scheduler. The flag
//! that selects between them (`--store-scheduling`) and the wiring into
//! `build_store` are a separate task (E5-T2); this module ships the mechanism
//! and its tests only, constructed but not yet adopted.
//!
//! # Metrics
//!
//! In scheduled mode each class records into its own [`StoreMetrics`], reusing
//! the [`InstrumentedStore`](crate::instrument::InstrumentedStore) op-label
//! family extended with a `{class}` dimension (`foreground`/`background`).
//! Passthrough mode records nothing new, matching today's behavior exactly.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::instrument::{InstantClock, MonotonicClock, StoreMetrics, StoreOp};
use crate::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};

/// The class a store handle belongs to. Attached to the handle at construction,
/// never threaded through a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestClass {
    /// Ack-bearing traffic: ingest/commit writes, query reads, catalog resolve.
    Foreground,
    /// Deferred traffic: compaction, fold, sweep, scrub, audit retention.
    Background,
}

impl RequestClass {
    /// Label for the `{class}` metric dimension.
    pub fn name(self) -> &'static str {
        match self {
            RequestClass::Foreground => "foreground",
            RequestClass::Background => "background",
        }
    }
}

/// Sizing for a [`RequestScheduler`]. `fg_permits` is the global in-flight cap
/// (foreground may use all of it); `bg_permits` caps concurrent background
/// requests handled by the scheduler; `bg_floor` is the number of background
/// requests guaranteed to make progress even while foreground waits.
///
/// Background is additionally bounded by the global cap: if `bg_permits`
/// exceeds `fg_permits`, background can still never hold more than `fg_permits`
/// permits at once. Defaults are deliberately not frozen here (ADR-0070
/// decision 2: they change only on panel evidence); callers pass explicit
/// values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub fg_permits: usize,
    pub bg_permits: usize,
    pub bg_floor: usize,
}

impl SchedulerConfig {
    /// Build a config, clamping to the invariants the scheduler relies on:
    /// every permit count is at least 1, and the background floor lands in
    /// `1..=bg_permits`.
    pub fn new(fg_permits: usize, bg_permits: usize, bg_floor: usize) -> Self {
        let fg_permits = fg_permits.max(1);
        let bg_permits = bg_permits.max(1);
        let bg_floor = bg_floor.clamp(1, bg_permits);
        SchedulerConfig {
            fg_permits,
            bg_permits,
            bg_floor,
        }
    }
}

/// A held admission. Kept alive for the duration of the request and dropped
/// once the inner call returns, releasing the permit(s) back to the scheduler.
/// The type is deliberately opaque: a caller holds it, it does not read it.
#[must_use = "the permit must be held for the duration of the request"]
pub struct Permit {
    /// The global in-flight permit. `None` only in the degraded case where the
    /// scheduler's semaphore was closed (never in normal operation); the op
    /// then proceeds unscheduled rather than failing.
    global: Option<OwnedSemaphorePermit>,
    /// The background sub-cap permit; `None` for foreground.
    bg: Option<OwnedSemaphorePermit>,
    /// For a background request that took a global permit: its slot in the
    /// `bg_committed` count, released when the request ends. `None` for
    /// foreground and for the degraded case.
    bg_committed: Option<CountGuard>,
    /// Woken on release so a yielding or queued acquirer re-evaluates.
    wake: Arc<Notify>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Release the global permit first so a woken acquirer observes it free,
        // then the background sub-cap permit, then the committed-count slot,
        // then wake. Doing the wake last means the woken task sees the permit
        // already available rather than racing the release.
        drop(self.global.take());
        drop(self.bg.take());
        drop(self.bg_committed.take());
        self.wake.notify_waiters();
    }
}

/// Holds one unit of a `usize` counter for as long as it lives, and releases it
/// in `Drop`.
///
/// Both counters the admission decision reads are held through one of these,
/// because both are read by *other* tasks to decide whether to yield or to
/// commit to an un-preemptible wait. A plain increment/decrement pair around an
/// `.await` releases only on the path where the await completes: drop the
/// future instead (a `select!` timeout, a client disconnect, a shutdown, a
/// panic unwinding through the acquire) and the counter stays elevated for the
/// life of the process, permanently skewing every later decision. `Drop` runs
/// on all of those paths.
struct CountGuard {
    count: Arc<AtomicUsize>,
}

impl CountGuard {
    /// Take one unit unconditionally.
    fn acquire(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        CountGuard {
            count: Arc::clone(count),
        }
    }

    /// Take one unit only while the counter is below `limit`, atomically, so
    /// two concurrent claimants cannot both read the same below-limit value and
    /// both succeed. `None` means the limit is already reached.
    fn try_claim(count: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        let mut current = count.load(Ordering::SeqCst);
        loop {
            if current >= limit {
                return None;
            }
            match count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(CountGuard {
                        count: Arc::clone(count),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for CountGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A weighted pair of semaphores implementing strict-priority-with-floor
/// admission for two request classes. See the [module docs](self).
pub struct RequestScheduler {
    /// Global in-flight cap (`fg_permits`). Both classes draw from it.
    global: Arc<Semaphore>,
    /// Background sub-cap (`bg_permits`): bounds concurrent background requests
    /// in the scheduler, independent of the global cap.
    bg_sem: Arc<Semaphore>,
    /// Count of foreground acquires currently blocked, so background can yield
    /// while any foreground waits.
    fg_waiters: Arc<AtomicUsize>,
    /// Count of background requests that hold a global permit *or* have
    /// committed to the un-preemptible below-floor wait for one. The floor
    /// decision reads this, so a request must be counted before it enters that
    /// wait, not after it wins the permit.
    bg_committed: Arc<AtomicUsize>,
    /// Background requests below this count compete fairly for the global
    /// permit (floor progress); at or above it they yield to foreground.
    bg_floor: usize,
    /// Woken on every permit release and whenever a foreground waiter clears,
    /// so yielding/queued acquirers re-check their gate.
    wake: Arc<Notify>,
}

impl RequestScheduler {
    /// Build a scheduler sized by `config`.
    pub fn new(config: SchedulerConfig) -> Self {
        RequestScheduler {
            global: Arc::new(Semaphore::new(config.fg_permits)),
            bg_sem: Arc::new(Semaphore::new(config.bg_permits)),
            fg_waiters: Arc::new(AtomicUsize::new(0)),
            bg_committed: Arc::new(AtomicUsize::new(0)),
            bg_floor: config.bg_floor,
            wake: Arc::new(Notify::new()),
        }
    }

    /// Acquire a permit of `class`, awaiting admission per the class's rule.
    pub async fn acquire(&self, class: RequestClass) -> Permit {
        match class {
            RequestClass::Foreground => self.acquire_foreground().await,
            RequestClass::Background => self.acquire_background().await,
        }
    }

    /// Foreground admission. Registers as a waiter (so background yields to it),
    /// then takes a global permit. Uses a non-blocking probe plus the release
    /// notification rather than the semaphore's own queue: priority for
    /// foreground comes from background stepping aside, and the floor comes from
    /// background's fair queue slot, so foreground itself must not sit in that
    /// queue ahead of a floor-guaranteed background request.
    ///
    /// The waiter registration is an RAII guard, so dropping this future while
    /// it is parked (timeout, disconnect, shutdown) releases it. A leaked
    /// registration would hold `fg_waiters` above zero forever, and background
    /// yields whenever `fg_waiters > 0`, so one cancelled foreground request
    /// would pin background at its floor for the life of the process.
    async fn acquire_foreground(&self) -> Permit {
        let waiter = CountGuard::acquire(&self.fg_waiters);
        let global = loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            // Register for the wake-up before probing, so a release landing
            // between the probe and the await is not lost.
            notified.as_mut().enable();
            match Arc::clone(&self.global).try_acquire_owned() {
                Ok(permit) => break Some(permit),
                Err(TryAcquireError::NoPermits) => notified.await,
                Err(TryAcquireError::Closed) => break None,
            }
        };
        // Release the registration before waking, so a woken background task
        // reads the cleared count rather than racing the decrement.
        drop(waiter);
        // A foreground waiter cleared: let background re-evaluate its gate.
        self.wake.notify_waiters();
        Permit {
            global,
            bg: None,
            bg_committed: None,
            wake: Arc::clone(&self.wake),
        }
    }

    /// Background admission. Holds a background sub-cap permit for the whole
    /// attempt (bounding concurrent background requests to `bg_permits`), then:
    ///
    /// - Below the floor: competes fairly for the global permit (a FIFO queue
    ///   slot), guaranteeing progress even against a saturating foreground
    ///   stream.
    /// - At or above the floor, with a foreground waiter present: yields, so
    ///   the release reaches foreground.
    /// - At or above the floor, with no foreground waiter: takes a global
    ///   permit if one is free, else waits for a release.
    ///
    /// "Below the floor" is decided by *claiming* a slot in `bg_committed`
    /// before entering the fair wait, not by reading a count of already-admitted
    /// requests. The fair wait is un-preemptible (tokio hands a released permit
    /// to the queue head, so a later foreground acquire cannot take it), so a
    /// request that has entered it counts against the floor from that moment.
    /// Deciding from the admitted count instead lets every concurrent
    /// background request read zero, all enter the fair wait, and delay
    /// foreground behind up to `bg_permits` of them.
    async fn acquire_background(&self) -> Permit {
        // Closed: degrade to unscheduled rather than fail the op (the semaphore
        // is never closed in normal operation).
        let bg_permit = Arc::clone(&self.bg_sem).acquire_owned().await.ok();
        // This request's slot in `bg_committed`, once it has one. Held from
        // before the wait it authorizes until the request ends, and released by
        // the guard on every exit path including cancellation.
        let mut committed: Option<CountGuard> = None;
        let global = loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if committed.is_none() {
                committed = CountGuard::try_claim(&self.bg_committed, self.bg_floor);
            }
            if committed.is_some() {
                // Floor progress: take a free permit immediately, otherwise
                // join the fair queue so a release eventually reaches us even
                // while foreground is saturating the store.
                match Arc::clone(&self.global).try_acquire_owned() {
                    Ok(permit) => break Some(permit),
                    Err(TryAcquireError::NoPermits) => {
                        match Arc::clone(&self.global).acquire_owned().await {
                            Ok(permit) => break Some(permit),
                            Err(_) => break None,
                        }
                    }
                    Err(TryAcquireError::Closed) => break None,
                }
            }

            // At or above the floor: yield to any waiting foreground. Removing
            // this `fg_waiters` guard is the flip that lets background starve
            // foreground (see the `foreground_priority_*` test).
            if self.fg_waiters.load(Ordering::SeqCst) > 0 {
                notified.await;
                continue;
            }

            // No foreground waiting: take a free permit or wait for a release.
            // The slot is claimed before the probe and dropped again if no
            // permit was free, so a concurrent background request never reads a
            // count that omits a request already on its way in.
            let slot = CountGuard::acquire(&self.bg_committed);
            match Arc::clone(&self.global).try_acquire_owned() {
                Ok(permit) => {
                    committed = Some(slot);
                    break Some(permit);
                }
                Err(TryAcquireError::NoPermits) => {
                    drop(slot);
                    notified.await;
                }
                Err(TryAcquireError::Closed) => break None,
            }
        };
        let committed = if global.is_some() {
            committed
        } else {
            // Degraded (semaphore closed): nothing was admitted, so release the
            // slot rather than counting an unscheduled op against the floor.
            drop(committed);
            None
        };
        Permit {
            global,
            bg: bg_permit,
            bg_committed: committed,
            wake: Arc::clone(&self.wake),
        }
    }
}

/// How a [`ClassedStore`] hands out its two handles.
enum Mode {
    /// Both handles are the inner store itself: no permit, no metrics, byte-for
    /// byte today's behavior (ADR-0070 decision 2, the default).
    Passthrough,
    /// Both handles wrap the inner store with a shared scheduler and per-class
    /// metrics.
    Scheduled(Scheduled),
}

/// The scheduled mode's shared state: one scheduler, one metrics block per
/// class, and the clock the per-class recording times against.
struct Scheduled {
    scheduler: Arc<RequestScheduler>,
    fg_metrics: Arc<StoreMetrics>,
    bg_metrics: Arc<StoreMetrics>,
    clock: Arc<dyn MonotonicClock>,
}

/// Wraps an inner store and hands out a [`RequestClass::Foreground`] and a
/// [`RequestClass::Background`] handle that share one [`RequestScheduler`]
/// (scheduled mode) or delegate straight through (passthrough mode). See the
/// [module docs](self).
pub struct ClassedStore {
    inner: Arc<dyn ObjectStoreBackend>,
    mode: Mode,
}

impl ClassedStore {
    /// Passthrough (the ADR-0070 decision 2 default): both handles are the
    /// inner store itself, so every op delegates directly with no permit
    /// acquire and no metrics -- byte-for-byte today's behavior.
    pub fn passthrough(inner: Arc<dyn ObjectStoreBackend>) -> Self {
        ClassedStore {
            inner,
            mode: Mode::Passthrough,
        }
    }

    /// Scheduled mode: install a shared [`RequestScheduler`] sized by `config`
    /// and per-class metrics. Times per-class latency against the process
    /// monotonic clock.
    pub fn scheduled(inner: Arc<dyn ObjectStoreBackend>, config: SchedulerConfig) -> Self {
        Self::scheduled_with_clock(inner, config, Arc::new(InstantClock::new()))
    }

    /// [`Self::scheduled`] with an injected clock, for deterministic per-class
    /// latency tests.
    pub fn scheduled_with_clock(
        inner: Arc<dyn ObjectStoreBackend>,
        config: SchedulerConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Self {
        ClassedStore {
            inner,
            mode: Mode::Scheduled(Scheduled {
                scheduler: Arc::new(RequestScheduler::new(config)),
                fg_metrics: Arc::new(StoreMetrics::default()),
                bg_metrics: Arc::new(StoreMetrics::default()),
                clock,
            }),
        }
    }

    /// The foreground handle: ack-bearing callers use this.
    pub fn foreground(&self) -> Arc<dyn ObjectStoreBackend> {
        self.handle(RequestClass::Foreground)
    }

    /// The background handle: compaction, fold, sweep, scrub, and audit
    /// retention use this.
    pub fn background(&self) -> Arc<dyn ObjectStoreBackend> {
        self.handle(RequestClass::Background)
    }

    fn handle(&self, class: RequestClass) -> Arc<dyn ObjectStoreBackend> {
        match &self.mode {
            // Passthrough returns the inner store verbatim: no wrapper, no
            // scheduler, no metrics. `Arc::ptr_eq` against the inner store
            // holds, which is the byte-for-byte-identical guarantee made
            // literal.
            Mode::Passthrough => Arc::clone(&self.inner),
            Mode::Scheduled(s) => Arc::new(ScheduledHandle {
                inner: Arc::clone(&self.inner),
                scheduler: Arc::clone(&s.scheduler),
                class,
                metrics: match class {
                    RequestClass::Foreground => Arc::clone(&s.fg_metrics),
                    RequestClass::Background => Arc::clone(&s.bg_metrics),
                },
                clock: Arc::clone(&s.clock),
            }),
        }
    }

    /// The metrics block for `class`, or `None` in passthrough mode (which adds
    /// no metrics). The `{class}` dimension is realized by one block per class.
    pub fn metrics(&self, class: RequestClass) -> Option<Arc<StoreMetrics>> {
        match &self.mode {
            Mode::Passthrough => None,
            Mode::Scheduled(s) => Some(match class {
                RequestClass::Foreground => Arc::clone(&s.fg_metrics),
                RequestClass::Background => Arc::clone(&s.bg_metrics),
            }),
        }
    }
}

/// One class's store handle: acquires a permit of its class, delegates to the
/// inner store, and records into its class's metrics block.
struct ScheduledHandle {
    inner: Arc<dyn ObjectStoreBackend>,
    scheduler: Arc<RequestScheduler>,
    class: RequestClass,
    metrics: Arc<StoreMetrics>,
    clock: Arc<dyn MonotonicClock>,
}

impl ScheduledHandle {
    fn record<T>(&self, op: StoreOp, start_nanos: u64, bytes: u64, result: &Result<T, StoreError>) {
        let elapsed = self.clock.now_nanos().saturating_sub(start_nanos);
        self.metrics
            .record_op(op, elapsed, bytes, result.as_ref().err());
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for ScheduledHandle {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        // Payload length is read before the move, and counted whether or not
        // the backend accepts the write (mirrors `InstrumentedStore`).
        let bytes = data.len() as u64;
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.put(key, data, opts).await;
        self.record(StoreOp::Put, start, bytes, &result);
        result
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.get(key, range).await;
        let bytes = result
            .as_ref()
            .map_or(0, |outcome| outcome.data.len() as u64);
        self.record(StoreOp::Get, start, bytes, &result);
        result
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        // The initiation is one scheduled request; the returned handle's parts
        // are not scheduled (no production caller uses multipart yet, per
        // `Capabilities::mandatory`). Uncounted, like `InstrumentedStore`: a
        // multipart upload is a handle, not a call any single `StoreOp`
        // describes. The permit releases when this call returns the handle.
        let _permit = self.scheduler.acquire(self.class).await;
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.head(key).await;
        self.record(StoreOp::Head, start, 0, &result);
        result
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.list(prefix, page).await;
        self.record(StoreOp::List, start, 0, &result);
        result
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.list_delimited(prefix).await;
        self.record(StoreOp::ListDelimited, start, 0, &result);
        result
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let _permit = self.scheduler.acquire(self.class).await;
        let start = self.clock.now_nanos();
        let result = self.inner.delete(key).await;
        self.record(StoreOp::Delete, start, 0, &result);
        result
    }

    /// Passthrough, unchanged and unscheduled: the server's startup capability
    /// gate must see the inner backend's declaration, never a decorator's.
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use crate::memory::MemoryStore;
    use std::sync::atomic::AtomicU64;

    /// Yield to the current-thread scheduler until `cond` holds, bounded so a
    /// broken scheduler makes the test fail rather than hang. Under the correct
    /// code every wait here settles in a handful of turns.
    async fn spin_until(mut cond: impl FnMut() -> bool) -> bool {
        for _ in 0..2000 {
            if cond() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        cond()
    }

    /// The scheduler behind a scheduled [`ClassedStore`] (tests reach into the
    /// private mode; passthrough has none).
    fn scheduler(cs: &ClassedStore) -> &Arc<RequestScheduler> {
        match &cs.mode {
            Mode::Scheduled(s) => &s.scheduler,
            Mode::Passthrough => panic!("passthrough store has no scheduler"),
        }
    }

    /// Held-call keys currently parked in the inner store's gate, i.e. the ops
    /// that have been admitted through the scheduler and reached the backend.
    fn held_keys(gate: &GateHandle) -> Vec<String> {
        gate.held_details().into_iter().map(|(_, _, k)| k).collect()
    }

    /// Release the held call matching `key`, returning true if one was found.
    fn release_key(gate: &GateHandle, key: &str) -> bool {
        match gate.held_details().into_iter().find(|(_, _, k)| k == key) {
            Some((id, _, _)) => gate.release(id),
            None => false,
        }
    }

    /// An inner store that counts each op and returns fakes, for proving the
    /// passthrough path issues exactly one inner op per handle op with no
    /// scheduler involvement.
    #[derive(Default)]
    struct CountingStore {
        gets: AtomicU64,
        puts: AtomicU64,
        deletes: AtomicU64,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for CountingStore {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(PutOutcome {
                etag: crate::Etag("fake".into()),
                version: crate::Version("fake".into()),
            })
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Ok(GetOutcome {
                data: Bytes::new(),
                etag: crate::Etag("fake".into()),
                version: crate::Version("fake".into()),
                total_size: 0,
            })
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            Err(StoreError::NotFound)
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            Ok(ListPage {
                objects: Vec::new(),
                next: None,
            })
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            Ok(DelimitedList {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
            })
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// Build a scheduled `ClassedStore` over a `FaultStore` whose every `get`
    /// is held, so a test can freeze admitted ops as "in flight" and control
    /// their release order. Returns the store and the gate.
    fn scheduled_rig(config: SchedulerConfig) -> (ClassedStore, GateHandle) {
        let inner = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let gate = inner.hold(Op::Get, None, Occurrence::Always);
        let cs = ClassedStore::scheduled(inner as Arc<dyn ObjectStoreBackend>, config);
        (cs, gate)
    }

    /// Passthrough (ADR-0070 decision 2, the default) hands out the inner store
    /// itself: `Arc::ptr_eq` holds, no metrics exist, and N handle ops issue
    /// exactly N inner ops with no scheduler between them.
    #[tokio::test]
    async fn passthrough_is_the_inner_store_verbatim() {
        let counting = Arc::new(CountingStore::default());
        let inner: Arc<dyn ObjectStoreBackend> = counting.clone();
        let cs = ClassedStore::passthrough(Arc::clone(&inner));

        let fg = cs.foreground();
        let bg = cs.background();

        // Byte-for-byte identical: both handles ARE the inner store, not a
        // wrapper. This is the "no added latency, no permit" guarantee made
        // literal -- there is no other object in the call path.
        assert!(
            Arc::ptr_eq(&fg, &inner),
            "foreground must be the inner store"
        );
        assert!(
            Arc::ptr_eq(&bg, &inner),
            "background must be the inner store"
        );

        // No metrics in passthrough: nothing new is recorded.
        assert!(cs.metrics(RequestClass::Foreground).is_none());
        assert!(cs.metrics(RequestClass::Background).is_none());

        // Exactly one inner op per handle op.
        for _ in 0..3 {
            fg.get("k", GetRange::Full).await.expect("get");
        }
        for _ in 0..2 {
            bg.get("k", GetRange::Full).await.expect("get");
        }
        bg.delete("k").await.expect("delete");

        assert_eq!(counting.gets.load(Ordering::SeqCst), 5);
        assert_eq!(counting.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(counting.puts.load(Ordering::SeqCst), 0);
    }

    /// Scheduled mode records each class into its own metrics block, so the
    /// `{class}` dimension is realized as one `StoreMetrics` per class over the
    /// existing op-label family.
    #[tokio::test]
    async fn scheduled_metrics_are_labelled_per_class() {
        let inner = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let cs = ClassedStore::scheduled(
            inner as Arc<dyn ObjectStoreBackend>,
            SchedulerConfig::new(4, 4, 1),
        );
        let fg = cs.foreground();
        let bg = cs.background();

        fg.put("a", Bytes::from_static(b"xy"), PutOptions::default())
            .await
            .expect("put");
        fg.put("b", Bytes::from_static(b"z"), PutOptions::default())
            .await
            .expect("put");
        fg.get("a", GetRange::Full).await.expect("get");
        bg.delete("a").await.expect("delete");

        let fg_m = cs
            .metrics(RequestClass::Foreground)
            .expect("scheduled has metrics")
            .snapshot();
        let bg_m = cs
            .metrics(RequestClass::Background)
            .expect("scheduled has metrics")
            .snapshot();

        assert_eq!(fg_m.put.calls, 2, "two foreground puts");
        assert_eq!(fg_m.put.bytes, 3, "put bytes counted (2 + 1)");
        assert_eq!(fg_m.get.calls, 1, "one foreground get");
        assert_eq!(
            fg_m.delete.calls, 0,
            "delete was background, not foreground"
        );
        assert_eq!(bg_m.delete.calls, 1, "one background delete");
        assert_eq!(bg_m.put.calls, 0, "no background put");
    }

    /// Weighted admission, foreground cap: with `fg_permits` foreground ops in
    /// flight, the `fg_permits + 1`-th foreground op waits until one releases.
    #[tokio::test]
    async fn foreground_cap_is_respected() {
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 2, 1));
        let fg = cs.foreground();

        let f0 = fg.clone();
        let f1 = fg.clone();
        tokio::spawn(async move { f0.get("f0", GetRange::Full).await });
        tokio::spawn(async move { f1.get("f1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        // The third foreground op cannot be admitted: the cap is 2.
        let f2 = fg.clone();
        tokio::spawn(async move { f2.get("f2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "third foreground op must register as a waiter"
        );
        assert_eq!(gate.held_count(), 2, "cap holds the third op out");
        assert!(!held_keys(&gate).contains(&"f2".to_string()));

        // Releasing one admits the third.
        assert!(release_key(&gate, "f0"));
        assert!(
            spin_until(|| held_keys(&gate).contains(&"f2".to_string())).await,
            "third op admitted once a permit freed"
        );
    }

    /// Weighted admission, background cap: `bg_permits` bounds concurrent
    /// background ops independently of the (larger) global cap.
    #[tokio::test]
    async fn background_cap_is_respected() {
        // Global cap 4 so the global semaphore is not the limit; bg cap 1.
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(4, 1, 1));
        let bg = cs.background();

        let b0 = bg.clone();
        tokio::spawn(async move { b0.get("b0", GetRange::Full).await });
        gate.wait_until_held(1).await;

        let b1 = bg.clone();
        tokio::spawn(async move { b1.get("b1", GetRange::Full).await });
        // b1 is stuck at the background sub-cap, not admitted, even though the
        // global cap has room.
        assert!(spin_until(|| held_keys(&gate).len() == 1).await);
        assert!(!held_keys(&gate).contains(&"b1".to_string()));

        assert!(release_key(&gate, "b0"));
        assert!(
            spin_until(|| held_keys(&gate).contains(&"b1".to_string())).await,
            "second background op admitted once the first released"
        );
    }

    /// Foreground priority: with background saturating the global cap and a
    /// further background request already parked in the scheduler competing for
    /// the next permit, a foreground acquire is admitted after exactly one
    /// in-flight background release -- the parked background request yields
    /// rather than taking the freed permit.
    ///
    /// The load-bearing line is the `fg_waiters` yield in
    /// `acquire_background`: with `bg2` registered on the wake queue *before*
    /// `fg0`, removing that guard lets `bg2` take the freed permit first and
    /// `fg0` starves. The parked `bg2`, not the just-released `bg0`, is what
    /// makes this bite: without the yield, `bg2` wins the release it is already
    /// waiting on.
    #[tokio::test]
    async fn foreground_preempts_a_parked_background_request() {
        // Global cap 2, background sub-cap 3 (so a third bg can park in the
        // scheduler past the sub-cap while two hold the global permits), floor
        // 1.
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 3, 1));
        let bg = cs.background();
        let fg = cs.foreground();

        // Saturate the global cap with two background ops.
        let b0 = bg.clone();
        let b1 = bg.clone();
        tokio::spawn(async move { b0.get("bg0", GetRange::Full).await });
        tokio::spawn(async move { b1.get("bg1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        // A third background op enters the scheduler and parks waiting for a
        // permit, registering on the wake queue while no foreground waits yet.
        let b2 = bg.clone();
        tokio::spawn(async move { b2.get("bg2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).bg_committed.load(Ordering::SeqCst) == 2).await,
            "exactly two background ops hold global permits"
        );
        // Let bg2 reach its parked wait before the foreground arrives.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        // Now a foreground op arrives and waits, registered after bg2.
        let f0 = fg.clone();
        tokio::spawn(async move { f0.get("fg0", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "foreground registers as a waiter"
        );

        // Release exactly one in-flight background op.
        assert!(release_key(&gate, "bg0"));

        // Foreground is admitted; the parked background op yielded and did not
        // take the freed permit.
        assert!(
            spin_until(|| held_keys(&gate).contains(&"fg0".to_string())).await,
            "foreground admitted after one background release"
        );
        let held = held_keys(&gate);
        assert!(
            !held.contains(&"bg2".to_string()),
            "the parked background op must yield, not take the freed permit: {held:?}"
        );
        assert!(
            held.contains(&"bg1".to_string()),
            "the other bg is still in flight"
        );
    }

    /// Background floor: under sustained foreground load with a foreground op
    /// also waiting, a background op below the floor still makes progress. Its
    /// fair queue slot beats the waiting foreground for one permit, guaranteeing
    /// the floor.
    ///
    /// The load-bearing line is the below-floor fair `acquire_owned` in
    /// `acquire_background`: replace it with a yield (as the above-floor path
    /// does) and the background op starves behind the waiting foreground
    /// instead of being admitted.
    #[tokio::test]
    async fn background_floor_makes_progress_under_foreground_load() {
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 2, 1));
        let fg = cs.foreground();
        let bg = cs.background();

        // Saturate the global cap with foreground ops.
        let f0 = fg.clone();
        let f1 = fg.clone();
        tokio::spawn(async move { f0.get("fg0", GetRange::Full).await });
        tokio::spawn(async move { f1.get("fg1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        // A third foreground op waits.
        let f2 = fg.clone();
        tokio::spawn(async move { f2.get("fg2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "a foreground op is waiting"
        );

        // A background op below the floor joins the fair queue for a permit.
        let b0 = bg.clone();
        tokio::spawn(async move { b0.get("bg0", GetRange::Full).await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        // Release one foreground op. The below-floor background op wins the
        // freed permit over the waiting foreground: the floor is guaranteed.
        assert!(release_key(&gate, "fg0"));
        assert!(
            spin_until(|| held_keys(&gate).contains(&"bg0".to_string())).await,
            "background floor makes progress even against a waiting foreground"
        );
    }

    /// Regression: a foreground acquire cancelled while parked must release its
    /// waiter registration.
    ///
    /// Dropping the acquire future is routine (a `select!` timeout, a client
    /// disconnect, shutdown). With the decrement on the completing path only,
    /// `fg_waiters` stayed elevated for the life of the process, and since
    /// background yields whenever `fg_waiters > 0`, one cancelled foreground
    /// request pinned background at its floor forever. Both halves are asserted:
    /// the count returns to zero, and background above the floor is admitted
    /// again afterwards (the count alone could be zeroed by a decrement that
    /// still left the scheduler wedged).
    #[tokio::test]
    async fn review_fg_waiters_leaks_on_cancellation() {
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 2, 1));
        let fg = cs.foreground();
        let bg = cs.background();

        // Saturate the global cap so the next foreground op must park.
        let f0 = fg.clone();
        let f1 = fg.clone();
        tokio::spawn(async move { f0.get("fg0", GetRange::Full).await });
        tokio::spawn(async move { f1.get("fg1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        let f2 = fg.clone();
        let parked = tokio::spawn(async move { f2.get("fg2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "the third foreground op parks as a waiter"
        );

        // Cancel it while parked, as a timeout or a disconnect would.
        parked.abort();
        let _ = parked.await;
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 0).await,
            "a cancelled foreground acquire must release its waiter registration"
        );

        // Consequence: with no foreground waiting, background above the floor
        // is admitted again. A leaked registration wedges it out forever.
        assert!(release_key(&gate, "fg0"));
        assert!(release_key(&gate, "fg1"));
        let b0 = bg.clone();
        let b1 = bg.clone();
        tokio::spawn(async move { b0.get("bg0", GetRange::Full).await });
        tokio::spawn(async move { b1.get("bg1", GetRange::Full).await });
        assert!(
            spin_until(|| {
                let held = held_keys(&gate);
                held.contains(&"bg0".to_string()) && held.contains(&"bg1".to_string())
            })
            .await,
            "background above the floor must run again once the cancelled \
             foreground waiter is gone: {:?}",
            held_keys(&gate)
        );
    }

    /// Regression: a background acquire cancelled while parked below the floor
    /// must release its `bg_committed` reservation.
    ///
    /// A below-floor background request claims a `bg_committed` slot *before* it
    /// enters the un-preemptible `acquire_owned().await` for a global permit.
    /// Dropping that future is routine (a `select!` timeout, a client
    /// disconnect, shutdown). With the release on the completing path only,
    /// `bg_committed` stayed elevated for the life of the process, so the floor
    /// reservation was consumed forever: a later below-floor background request
    /// could no longer claim the slot, lost its floor guarantee, and starved
    /// behind any waiting foreground. Both halves are asserted: the count
    /// returns to zero, and a fresh below-floor background op still reclaims its
    /// floor slot and beats a waiting foreground afterwards (the count alone
    /// could be zeroed while the floor stayed wedged).
    #[tokio::test]
    async fn review_bg_committed_leaks_on_cancellation() {
        // Global cap 2, background sub-cap 4 (so the sub-cap never holds a
        // background op back), floor 1.
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 4, 1));
        let fg = cs.foreground();
        let bg = cs.background();

        // Saturate the global cap so a below-floor background op must park in
        // the un-preemptible fair wait after claiming its floor slot.
        let f0 = fg.clone();
        let f1 = fg.clone();
        tokio::spawn(async move { f0.get("fg0", GetRange::Full).await });
        tokio::spawn(async move { f1.get("fg1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        // A below-floor background op claims its floor reservation and parks
        // waiting for a global permit. No foreground waits yet, so it takes the
        // floor path (try_claim + acquire_owned().await), not the yield path.
        let bcancel = bg.clone();
        let parked = tokio::spawn(async move { bcancel.get("bgcancel", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).bg_committed.load(Ordering::SeqCst) == 1).await,
            "the below-floor background op claims its floor reservation and parks"
        );

        // Cancel it while parked, as a timeout or a disconnect would.
        parked.abort();
        let _ = parked.await;
        assert!(
            spin_until(|| scheduler(&cs).bg_committed.load(Ordering::SeqCst) == 0).await,
            "a cancelled below-floor background acquire must release its \
             bg_committed reservation"
        );

        // Consequence: the floor slot is reclaimable. A foreground op waits,
        // then a fresh below-floor background op must still win a freed permit
        // over that waiting foreground -- the floor guarantee. A leaked
        // reservation keeps the slot consumed, so the fresh op could not claim
        // it and would starve behind the foreground waiter (the count alone
        // could read zero via a decrement that still left the floor wedged).
        let f2 = fg.clone();
        tokio::spawn(async move { f2.get("fg2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "a foreground op is waiting"
        );

        let b0 = bg.clone();
        tokio::spawn(async move { b0.get("bg0", GetRange::Full).await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        // Free one permit. The below-floor background op wins it over the
        // waiting foreground: the floor is reclaimable after the cancellation.
        assert!(release_key(&gate, "fg0"));
        assert!(
            spin_until(|| held_keys(&gate).contains(&"bg0".to_string())).await,
            "a fresh below-floor background op must reclaim the floor slot and \
             beat the waiting foreground: {:?}",
            held_keys(&gate)
        );
    }

    /// Regression: a foreground acquire must not be delayed by more than one
    /// in-flight background request, however many background requests entered
    /// the scheduler first.
    ///
    /// The below-floor wait on the global semaphore is un-preemptible: tokio
    /// hands a released permit straight to the FIFO head, before
    /// `Permit::drop` reaches `notify_waiters`. Deciding "below the floor" from
    /// the count of *already admitted* background requests let every concurrent
    /// background request read zero and all enter that wait, so a floor of one
    /// became a floor of up to `bg_permits`. Here two background requests park
    /// while both global permits are held; releasing both must admit the
    /// waiting foreground, not a second background request.
    #[tokio::test]
    async fn review_floor_overshoot_delays_foreground_past_one_request() {
        // Global cap 2, background sub-cap 4 (so the sub-cap is not what holds
        // the second background request back), floor 1.
        let (cs, gate) = scheduled_rig(SchedulerConfig::new(2, 4, 1));
        let fg = cs.foreground();
        let bg = cs.background();

        // Two foreground ops hold both global permits.
        let f0 = fg.clone();
        let f1 = fg.clone();
        tokio::spawn(async move { f0.get("fg0", GetRange::Full).await });
        tokio::spawn(async move { f1.get("fg1", GetRange::Full).await });
        gate.wait_until_held(2).await;

        // Two background ops enter the scheduler and park, one after the other,
        // both before any foreground waits. Only the counters distinguish which
        // path each took, so the test asserts nothing about them: what matters
        // is which requests are admitted at the end.
        let b0 = bg.clone();
        tokio::spawn(async move { b0.get("bg0", GetRange::Full).await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let b1 = bg.clone();
        tokio::spawn(async move { b1.get("bg1", GetRange::Full).await });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gate.held_count(),
            2,
            "neither background op is admitted while foreground holds both permits"
        );
        // Pin the reservation during the below-floor window: exactly one
        // background op holds the floor slot (the other read the floor as full
        // and parked above it). A floor of one that had become a floor of N
        // would show a higher count here.
        assert!(
            spin_until(|| scheduler(&cs).bg_committed.load(Ordering::SeqCst) == 1).await,
            "exactly one background op holds the floor reservation: {}",
            scheduler(&cs).bg_committed.load(Ordering::SeqCst)
        );

        // A foreground op arrives and waits, behind both parked background ops.
        let f2 = fg.clone();
        tokio::spawn(async move { f2.get("fg2", GetRange::Full).await });
        assert!(
            spin_until(|| scheduler(&cs).fg_waiters.load(Ordering::SeqCst) == 1).await,
            "the third foreground op parks as a waiter"
        );

        // Free both permits. One goes to the floor-slot background op; the
        // other must reach the waiting foreground.
        assert!(release_key(&gate, "fg0"));
        assert!(release_key(&gate, "fg1"));
        assert!(
            spin_until(|| held_keys(&gate).contains(&"fg2".to_string())).await,
            "foreground must be admitted, delayed by at most one background \
             request: {:?}",
            held_keys(&gate)
        );
        let held = held_keys(&gate);
        let bg_held = held.iter().filter(|k| k.starts_with("bg")).count();
        assert!(
            bg_held <= 1,
            "at most one background request may hold a permit ahead of \
             foreground: {held:?}"
        );
    }
}
