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
    /// For a background permit that took a global permit: the in-flight counter
    /// to decrement on release. `None` for foreground and for the degraded case.
    bg_inflight: Option<Arc<AtomicUsize>>,
    /// Woken on release so a yielding or queued acquirer re-evaluates.
    wake: Arc<Notify>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Release the global permit first so a woken acquirer observes it free,
        // then the background sub-cap permit, then update the in-flight count,
        // then wake. Doing the wake last means the woken task sees the permit
        // already available rather than racing the release.
        drop(self.global.take());
        drop(self.bg.take());
        if let Some(inflight) = self.bg_inflight.take() {
            inflight.fetch_sub(1, Ordering::SeqCst);
        }
        self.wake.notify_waiters();
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
    /// Count of background requests currently holding a global permit, so the
    /// floor decision knows how many background requests are already admitted.
    bg_inflight: Arc<AtomicUsize>,
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
            bg_inflight: Arc::new(AtomicUsize::new(0)),
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
    async fn acquire_foreground(&self) -> Permit {
        self.fg_waiters.fetch_add(1, Ordering::SeqCst);
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
        self.fg_waiters.fetch_sub(1, Ordering::SeqCst);
        // A foreground waiter cleared: let background re-evaluate its gate.
        self.wake.notify_waiters();
        Permit {
            global,
            bg: None,
            bg_inflight: None,
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
    async fn acquire_background(&self) -> Permit {
        let bg_permit = match Arc::clone(&self.bg_sem).acquire_owned().await {
            Ok(permit) => Some(permit),
            // Closed: degrade to unscheduled rather than fail the op.
            Err(_) => None,
        };
        let global = loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let below_floor = self.bg_inflight.load(Ordering::SeqCst) < self.bg_floor;
            if below_floor {
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
            match Arc::clone(&self.global).try_acquire_owned() {
                Ok(permit) => break Some(permit),
                Err(TryAcquireError::NoPermits) => notified.await,
                Err(TryAcquireError::Closed) => break None,
            }
        };
        let bg_inflight = if global.is_some() {
            self.bg_inflight.fetch_add(1, Ordering::SeqCst);
            Some(Arc::clone(&self.bg_inflight))
        } else {
            None
        };
        Permit {
            global,
            bg: bg_permit,
            bg_inflight,
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
            spin_until(|| scheduler(&cs).bg_inflight.load(Ordering::SeqCst) == 2).await,
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
}
