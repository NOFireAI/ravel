use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::clock::{Clock, SystemClock};
use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;
use crate::s3fifo::S3Fifo;
use crate::single_flight::{Role, SingleFlight, SingleFlightError};

/// XOR mask applied to every byte on a hit in corruption mode. Never
/// 0x00: XORing with 0x00 would leave a zero-valued byte unchanged, and a
/// real segment page containing at least one 0x00 byte is common enough
/// that "unchanged" would not reliably mean "not corrupted".
const CORRUPTION_XOR: u8 = 0xA5;

/// One RAM-tier entry: the cached bytes plus the wall-clock time they were
/// admitted, in nanoseconds since the Unix epoch, read from the injected
/// [`Clock`]. The stamp is what the per-`get` age check and the background
/// sweep compare against `max_entry_age_ns` so an erased subject's bytes do
/// not linger in a query node's RAM past the bound (ADR-0064).
/// Cloned on every hit, but a [`Bytes`] clone is a refcount bump, not a copy.
#[derive(Clone)]
struct CacheEntry {
    written_at_ns: u64,
    bytes: Bytes,
}

/// The shared, `Arc`-held state of a [`Cache`]: the eviction structure, the
/// counters, the limits, and the injected clock -- everything the cache and
/// its background age-sweep task both touch. Split out from [`Cache`] (and
/// kept non-generic, independent of the caller's fetch-error type `E`) so the
/// sweeper can hold a [`Weak`] to it rather than a strong reference: the
/// sweeper never keeps the tier alive, so dropping the [`Cache`] frees this
/// state and the next tick sees a dead `Weak` and exits (see [`spawn_sweeper`]).
struct Inner {
    fifo: Mutex<S3Fifo<CacheEntry>>,
    metrics: Arc<CacheMetrics>,
    limits: CacheLimits,
    /// Injected wall clock. Stamped into each entry at `insert`, read on every
    /// `get`, and read by the periodic sweep to age entries out past
    /// `limits.max_entry_age_ns` (ADR-0064). Injected so tests
    /// drive ageing deterministically instead of sleeping.
    clock: Arc<dyn Clock>,
}

/// The RAM tier of ADR-0046's read cache: a bounded, content-addressed,
/// single-flighted `CacheKey -> Bytes` map. `E` is the caller's upstream
/// fetch error type; this crate defines no error type of its own and has
/// no opinion on what a miss's upstream call looks like.
///
/// Like the disk tier, each entry carries a stamped write time and a
/// configured per-entry max-age (`limits.max_entry_age_ns`). An entry older
/// than the max-age is treated as a miss on `get` and dropped; a background
/// sweep (ADR-0064) drops every over-age entry on a fixed
/// interval regardless of access, so an entry that is never re-read and sees
/// no eviction pressure still ages out within one `limits.sweep_interval_ns`
/// past the max-age. This mirrors the disk tier on a query
/// node's separate, in-RAM copy: the erasure sweep runs on the maintain node
/// and cannot reach a query node's RAM, so the bound is enforced locally.
pub struct Cache<E> {
    inner: Arc<Inner>,
    single_flight: SingleFlight<CacheKey, Bytes, E>,
    corrupt_hits: bool,
    /// Handle to the background age-sweep task, present only when the cache was
    /// constructed inside a Tokio runtime (production always is). Aborted on
    /// drop so no sweeper task outlives its cache.
    sweeper: Option<tokio::task::JoinHandle<()>>,
}

impl<E> Cache<E>
where
    E: Clone + Send + Sync + 'static,
{
    pub fn new(limits: CacheLimits) -> Self {
        Self::new_with_clock(limits, Arc::new(SystemClock))
    }

    /// Like [`Cache::new`], but with an explicit [`Clock`] for the per-entry
    /// max-age and the background sweep (ADR-0064). Production uses
    /// [`Cache::new`], which injects a [`SystemClock`]; tests inject a clock
    /// they advance across the max-age boundary by hand. `new` keeps its
    /// original signature so `ravel-query`/`ravel-server` callers compile
    /// unchanged.
    pub fn new_with_clock(limits: CacheLimits, clock: Arc<dyn Clock>) -> Self {
        let inner = Arc::new(Inner {
            fifo: Mutex::new(S3Fifo::new(limits)),
            metrics: Arc::new(CacheMetrics::default()),
            limits,
            clock,
        });
        let sweeper = spawn_sweeper(&inner);
        Cache {
            inner,
            single_flight: SingleFlight::new(),
            corrupt_hits: false,
            sweeper,
        }
    }

    /// A cache that returns deliberately corrupted bytes on every hit.
    ///
    /// This is not a test fixture: it is a supported mode, and the
    /// acceptance gate for ADR-0046's whole read-cache epic. A later task
    /// runs the entire query test suite against a cache built this way,
    /// and every test must either return the identical result it returns
    /// against an uncached store, or fail with a typed error -- proving
    /// that query correctness never depends on what the cache happens to
    /// hold. Do not use this constructor for anything other than that
    /// suite.
    pub fn with_corruption(limits: CacheLimits) -> Self {
        // Not `..Cache::new(limits)`: `Cache` implements `Drop` (to abort the
        // sweeper), so struct-update syntax cannot move its fields out.
        let mut cache = Cache::new(limits);
        cache.corrupt_hits = true;
        cache
    }

    /// A cloneable handle to this cache's counters, independent of the
    /// cache's own lifetime.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        self.inner.metrics.clone()
    }

    /// Current number of resident entries.
    pub fn len(&self) -> usize {
        self.inner.fifo.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current total bytes across every resident entry.
    pub fn total_bytes(&self) -> u64 {
        self.inner.fifo.lock().total_bytes()
    }

    /// Look up `key` without fetching. Records a hit or a miss either
    /// way. An entry older than `max_entry_age_ns` is dropped and reported
    /// as a miss rather than served (ADR-0064). In corruption
    /// mode, a hit returns deliberately corrupted bytes rather than what was
    /// inserted.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner.get(key).map(|bytes| self.maybe_corrupt(bytes))
    }

    /// Admit `value` under `key`. Not an error, and a no-op on the
    /// eviction state, if `value` is larger than the configured maximum
    /// single-entry size: the caller still has its own copy of the bytes.
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        self.inner.insert(key, value);
    }

    fn maybe_corrupt(&self, bytes: Bytes) -> Bytes {
        if !self.corrupt_hits {
            return bytes;
        }
        Bytes::from(
            bytes
                .iter()
                .map(|b| b ^ CORRUPTION_XOR)
                .collect::<Vec<u8>>(),
        )
    }

    /// Collapses concurrent misses on the same key into one call to `fetch`
    /// (ADR-0046 decision 5) and, on a leader miss that succeeds, admits the
    /// result before returning it.
    ///
    /// Does not consult the cache itself first: both call sites in
    /// `ravel-query` already call `get` to decide their own hit/miss
    /// accounting (ADR-0044's `QueryAccounting` needs that branch either
    /// way) and call this only on that miss. An earlier version re-checked
    /// here too, so the *same* logical miss recorded two `CacheMetrics`
    /// misses -- one from the caller's `get`, one from this method's own --
    /// corrupting the request-hit-rate SLI ADR-0046 lists. Call `get`
    /// yourself first if you need the hit path; this is the miss-only half.
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<Bytes, SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Bytes, E>> + Send,
    {
        let (result, role) = self.single_flight.run(key, fetch).await;
        match (&result, role) {
            (Ok(bytes), Role::Leader) => self.insert(key, bytes.clone()),
            (Ok(_), Role::Follower) => self.inner.metrics.record_collapse(),
            (Err(_), _) => {}
        }
        result
    }

    /// Runs one age sweep synchronously: every entry older than
    /// `max_entry_age_ns` is dropped, exactly as the background task does on
    /// each tick. The background task is the production driver; this is the
    /// deterministic hook a test uses to trigger a single sweep without waiting
    /// on a timer.
    #[cfg(test)]
    fn sweep_expired_now(&self) {
        self.inner.sweep_expired();
    }

    /// A `Weak` to the shared state, for tests that assert the sweeper leaks no
    /// strong reference: after the cache is dropped this must not upgrade.
    #[cfg(test)]
    fn inner_weak(&self) -> Weak<Inner> {
        Arc::downgrade(&self.inner)
    }
}

impl<E> Drop for Cache<E> {
    /// Aborts the background sweep task so it never outlives the cache. Even
    /// without this, the sweeper holds only a [`Weak`] and would exit on its
    /// next tick once this drop frees the last strong reference; the abort just
    /// makes shutdown prompt rather than up to one interval late.
    fn drop(&mut self) {
        if let Some(handle) = self.sweeper.take() {
            handle.abort();
        }
    }
}

impl Inner {
    /// Look up `key`, aging it out if it is past `max_entry_age_ns`. Records a
    /// hit, or a miss (plus an expiry, when the miss is because the entry aged
    /// out), matching the disk tier's `get`/expiry accounting.
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let mut fifo = self.fifo.lock();
        let Some(entry) = fifo.get(key) else {
            drop(fifo);
            self.metrics.record_miss();
            return None;
        };
        if self.clock.now_ns().saturating_sub(entry.written_at_ns) > self.limits.max_entry_age_ns {
            // Older than the configured max-age: an erased subject's bytes must
            // not outlive the sweep in a query node's RAM by more than this
            // (ADR-0064). Dropped and re-fetched, not served.
            // `saturating_sub` keeps a clock that jumped backward (a stamp
            // newer than "now") from wrapping into a huge age: such an entry is
            // simply treated as not yet expired, mirroring the disk tier.
            fifo.remove(key);
            drop(fifo);
            self.metrics.record_miss();
            return None;
        }
        drop(fifo);
        self.metrics.record_hit(entry.bytes.len() as u64);
        Some(entry.bytes)
    }

    fn insert(&self, key: CacheKey, value: Bytes) {
        let size = value.len() as u64;
        let entry = CacheEntry {
            written_at_ns: self.clock.now_ns(),
            bytes: value,
        };
        self.fifo.lock().insert(key, entry, size, &self.metrics);
    }

    /// Drops every entry whose stamped write time is older than
    /// `max_entry_age_ns`, regardless of whether it was ever re-read. This is
    /// the periodic driver the background task calls on each tick
    /// (ADR-0064): the per-`get` age check only reaches entries that are read,
    /// so an idle entry under no eviction pressure needs this walk to have its
    /// bytes physically dropped from RAM within one sweep interval past the
    /// max-age. The sweep body is fully synchronous and holds the fifo lock
    /// only while it mutates the accounting -- never across an `await`.
    fn sweep_expired(&self) {
        let now_ns = self.clock.now_ns();
        let max_age = self.limits.max_entry_age_ns;
        let mut fifo = self.fifo.lock();
        // Discards the returned keys: unlike the disk tier there is no backing
        // file to unlink, and no metric is exposed for RAM expiry, so dropping
        // them from the eviction accounting is the whole of the work.
        let _ = fifo.drain_where(|entry| now_ns.saturating_sub(entry.written_at_ns) > max_age);
    }
}

/// Spawns the periodic age-sweep task for `inner`, returning its join handle,
/// or `None` when no Tokio runtime is available (a synchronous construction).
/// The task holds only a [`Weak`], so it never keeps the tier alive: once the
/// owning [`Cache`] drops, the next `upgrade` fails and the task exits. It
/// also holds no strong reference across its `await`, so a drop that lands
/// while the task is parked between ticks frees the state immediately.
fn spawn_sweeper(inner: &Arc<Inner>) -> Option<tokio::task::JoinHandle<()>> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let weak: Weak<Inner> = Arc::downgrade(inner);
    // Zero would panic `interval`; a sane floor keeps a misconfigured or
    // test-shrunk value from doing so while staying deterministic.
    let period = Duration::from_nanos(inner.limits.sweep_interval_ns.max(1));
    Some(handle.spawn(async move {
        let mut ticker = tokio::time::interval(period);
        // The first tick fires immediately; consume it so the first real sweep
        // is one interval out, not at construction time.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match weak.upgrade() {
                // Scoped so the strong reference is dropped at the end of this
                // statement, before the next `tick().await`.
                Some(inner) => inner.sweep_expired(),
                None => break,
            }
        }
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn test_key_with_len(n: u64, len: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([7u8; 16], content_hash, 0, len)
    }

    fn generous_limits() -> CacheLimits {
        CacheLimits::new(64 * 1024 * 1024, 10_000, 16 * 1024 * 1024)
    }

    /// A hand-advanced clock so the max-age boundary is crossed
    /// deterministically, without sleeping. `now_ns` reads whatever the test
    /// last set; `set` moves wall time forward (or back).
    struct TestClock(AtomicU64);

    impl TestClock {
        fn new(start_ns: u64) -> Arc<Self> {
            Arc::new(TestClock(AtomicU64::new(start_ns)))
        }
        fn set(&self, now_ns: u64) {
            self.0.store(now_ns, Ordering::Relaxed);
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// ADR-0064: a RAM entry younger than the max-age is served;
    /// once wall time crosses `written_at + max_age`, the same key is a miss on
    /// `get` and the stale bytes are dropped rather than served.
    ///
    /// FLIP (non-vacuity): in `Inner::get`, change
    /// `self.clock.now_ns().saturating_sub(entry.written_at_ns) > self.limits.max_entry_age_ns`
    /// to `... > u64::MAX` (never true). The post-boundary `assert!(... is_none)`
    /// then fails because the aged-out bytes are served.
    #[test]
    fn ram_entry_older_than_max_age_is_not_served() {
        let max_age = 24 * 60 * 60 * 1_000_000_000u64; // 24h in ns
        let limits = generous_limits().with_max_entry_age_ns(max_age);
        let clock = TestClock::new(1_000_000);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, Bytes::from_static(b"hello"));

        // Younger than max-age: served.
        clock.set(1_000_000 + max_age / 2);
        assert_eq!(cache.get(&key).as_deref(), Some(b"hello".as_slice()));

        // Exactly at the boundary (age == max_age): still served, the check is
        // strictly-greater-than.
        clock.set(1_000_000 + max_age);
        assert_eq!(cache.get(&key).as_deref(), Some(b"hello".as_slice()));

        // One nanosecond past the boundary: a miss, and the entry is dropped
        // from RAM rather than served.
        clock.set(1_000_000 + max_age + 1);
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.len(), 0, "the aged-out entry is dropped from RAM");
        assert_eq!(cache.total_bytes(), 0);
        // The dropped read still records a plain miss, exactly as a cold miss
        // would (three gets so far: two served hits, one aged-out miss).
        assert_eq!(cache.metrics().snapshot().misses, 1);
    }

    /// Non-vacuity for the periodic sweep: an entry that is never `get`-touched
    /// (so the per-`get` check never reaches it) and ages past
    /// `max_entry_age_ns` must have its bytes dropped from RAM by one sweep,
    /// not linger until eviction pressure or an access happens to touch it.
    ///
    /// FLIP (non-vacuity): in `Inner::sweep_expired`, change
    /// `now_ns.saturating_sub(entry.written_at_ns) > max_age`
    /// to `... > u64::MAX` (never true). The sweep then drops nothing, and the
    /// `cache.len() == 0` / `get is_none` assertions below fail.
    #[test]
    fn idle_ram_entry_past_max_age_is_swept() {
        let max_age = 24 * 60 * 60 * 1_000_000_000u64; // 24h in ns
        let limits = generous_limits().with_max_entry_age_ns(max_age);
        let clock = TestClock::new(1_000_000);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, Bytes::from_static(b"hello"));
        assert_eq!(cache.len(), 1, "precondition: the entry is resident");
        assert_eq!(cache.total_bytes(), 5);

        // Never `get`-touched. Advance the injected clock past the max-age and
        // run exactly one sweep.
        clock.set(1_000_000 + max_age + 1);
        cache.sweep_expired_now();

        assert_eq!(
            cache.len(),
            0,
            "an idle aged-out entry must be swept from RAM"
        );
        assert_eq!(cache.total_bytes(), 0);
        assert!(
            cache.get(&key).is_none(),
            "a swept entry is a subsequent miss"
        );

        // An entry still within the max-age is left alone by the sweep: it is
        // not an indiscriminate purge.
        let fresh = test_key_with_len(2, 5);
        cache.insert(fresh, Bytes::from_static(b"world"));
        cache.sweep_expired_now();
        assert_eq!(
            cache.get(&fresh).as_deref(),
            Some(b"world".as_slice()),
            "a within-age entry must survive the sweep"
        );
    }

    /// Wiring proof: the background task spawned at construction drives
    /// `sweep_expired` on its interval, with no manual trigger. An idle entry
    /// aged past the max-age is gone within a bounded number of sweep intervals.
    #[tokio::test]
    async fn spawned_sweeper_evicts_idle_ram_entry_within_interval() {
        let max_age = 1_000_000_000u64; // 1s in ns
        // A short sweep interval so the test does not wait on the 1 h default.
        let limits = generous_limits()
            .with_max_entry_age_ns(max_age)
            .with_sweep_interval_ns(5_000_000); // 5 ms
        let clock = TestClock::new(10_000_000_000);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, Bytes::from_static(b"hello"));
        assert_eq!(cache.len(), 1, "precondition: the entry is resident");

        // Age it out; the periodic task must notice on its own.
        clock.set(10_000_000_000 + max_age + 1);

        let mut gone = false;
        for _ in 0..400 {
            if cache.is_empty() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            gone,
            "the periodic sweeper must drop an idle aged-out entry on its own"
        );
        assert!(cache.get(&key).is_none());
    }

    /// Clean shutdown / no leak: dropping the cache must leave no sweeper task
    /// holding its state alive. The sweeper holds only a `Weak`, so once the
    /// sole strong `Arc` (inside `Cache`) drops, the shared state is freed and a
    /// later `upgrade` returns `None`; `Drop` also aborts the task so it never
    /// wakes again.
    #[tokio::test]
    async fn sweeper_task_stops_when_cache_dropped() {
        let limits = generous_limits().with_sweep_interval_ns(5_000_000);
        let clock = TestClock::new(1);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock);

        assert!(
            cache.sweeper.is_some(),
            "constructed inside a runtime, the sweeper task must exist"
        );
        let weak = cache.inner_weak();
        assert!(weak.upgrade().is_some(), "precondition: state is live");

        drop(cache);

        // Yield so the aborted task is reaped; the state is freed regardless,
        // since the task never held a strong reference across its await.
        tokio::task::yield_now().await;
        assert!(
            weak.upgrade().is_none(),
            "dropping the cache must leave no live sweeper holding its state"
        );
    }

    /// `max_entry_age_ns == 0` must expire promptly with no u64 underflow or
    /// panic in either age-comparison site (the per-`get` check and the sweep).
    #[test]
    fn max_entry_age_zero_expires_promptly_without_underflow() {
        let limits = generous_limits().with_max_entry_age_ns(0);
        let clock = TestClock::new(5_000_000);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, Bytes::from_static(b"hello"));

        // Same instant: age is 0, and the boundary is strictly-greater-than, so
        // the entry is not yet expired. No underflow computing `now - written`.
        assert_eq!(
            cache.get(&key).as_deref(),
            Some(b"hello".as_slice()),
            "at age 0 the strictly-greater-than boundary still serves"
        );

        // One ns past write: expires via the `get` path, no underflow.
        clock.set(5_000_001);
        assert!(
            cache.get(&key).is_none(),
            "one ns past write, age 0 expires"
        );
        assert_eq!(cache.len(), 0, "the age-0 expiry drops the entry");

        // The sweep path handles `max_entry_age_ns == 0` identically.
        cache.insert(key, Bytes::from_static(b"hello")); // stamped at now = 5_000_001
        clock.set(5_000_002);
        cache.sweep_expired_now();
        assert_eq!(
            cache.len(),
            0,
            "the sweep must drop a one-ns-old entry when max-age is 0"
        );
    }

    /// A backward clock jump (a stamp newer than "now") must not wrap into a
    /// huge age and expire a fresh entry: `saturating_sub` clamps it to zero,
    /// so the entry is treated as not yet expired on both the `get` and sweep
    /// paths.
    #[test]
    fn backward_clock_jump_does_not_expire_a_fresh_entry() {
        let max_age = 1_000_000_000u64;
        let limits = generous_limits().with_max_entry_age_ns(max_age);
        let clock = TestClock::new(10_000_000_000);
        let cache: Cache<&'static str> = Cache::new_with_clock(limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, Bytes::from_static(b"hello"));

        // Clock jumps backward, so `now < written_at`.
        clock.set(1_000);
        assert_eq!(
            cache.get(&key).as_deref(),
            Some(b"hello".as_slice()),
            "a backward clock jump must not expire a fresh entry"
        );
        cache.sweep_expired_now();
        assert_eq!(cache.len(), 1, "the sweep must not drop it either");
    }
}
