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
use crate::tiered::ReadOutcome;

/// XOR mask applied to every byte on a hit in corruption mode. Never
/// 0x00: XORing with 0x00 would leave a zero-valued byte unchanged, and a
/// real segment page containing at least one 0x00 byte is common enough
/// that "unchanged" would not reliably mean "not corrupted".
const CORRUPTION_XOR: u8 = 0xA5;

/// The exact byte transformation a corruption-mode hit applies. Shared with
/// the tiered handle ([`crate::tiered`]) so a disk-served hit read through it
/// is corrupted by the identical operation as a RAM hit, rather than by a
/// second copy that could drift: ADR-0046 decision 4's acceptance gate must
/// reach every tier the same way. Never mutates in place; a fresh [`Bytes`]
/// is returned so the tier's own clean copy is untouched.
pub(crate) fn corrupt_bytes(bytes: &Bytes) -> Bytes {
    Bytes::from(
        bytes
            .iter()
            .map(|b| b ^ CORRUPTION_XOR)
            .collect::<Vec<u8>>(),
    )
}

/// `value` as the sole handle to an allocation of exactly its length: kept
/// as is when it already is one, otherwise copied once into a fresh
/// exact-size allocation and the copy counted. Shared, static, and
/// owner-backed `Bytes` are not unique, so they are copied.
fn owned_exact(value: Bytes, metrics: &CacheMetrics) -> Bytes {
    if value.is_empty() {
        return Bytes::new();
    }
    let unique = match value.try_into_mut() {
        Ok(unique) if unique.capacity() == unique.len() => unique,
        Ok(oversized) => return copy_counted(&oversized, metrics),
        Err(shared) => return copy_counted(&shared, metrics),
    };
    // `BytesMut::capacity` does not count bytes before the view's start; the
    // `Vec` it converts into does, so its capacity is the whole allocation.
    let vec = Vec::from(unique);
    if vec.capacity() == vec.len() {
        Bytes::from(vec)
    } else {
        copy_counted(&vec, metrics)
    }
}

fn copy_counted(bytes: &[u8], metrics: &CacheMetrics) -> Bytes {
    metrics.record_admission_copy(bytes.len() as u64);
    Bytes::copy_from_slice(bytes)
}

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
    /// The stored value is `(clean_bytes, from_cache)`: `true` when the
    /// leader's RAM recheck in [`get_or_fetch`](Self::get_or_fetch) served the
    /// bytes, so they are corruption-gated like any other hit.
    single_flight: SingleFlight<CacheKey, (Bytes, bool), E>,
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
    ///
    /// **Double-counting warning (issue #653), the same one
    /// [`get_or_fetch`](Self::get_or_fetch) carries.** This method records one
    /// miss on a miss. A caller that peeks a key with `get`, sees a miss, and
    /// then resolves it by calling [`get_or_fetch`](Self::get_or_fetch) on the
    /// same key must not be counted twice for the one logical request: this
    /// `get`'s miss is the single accounted miss for that key, and the
    /// request-hit-rate SLI ADR-0046 depends on it staying that way.
    /// `get_or_fetch` itself runs `single_flight` and records no miss of its
    /// own, so it remains the correct way to resolve a peeked miss; what the
    /// caller must avoid is counting a second miss around it, not the call.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner.get(key).map(|bytes| self.maybe_corrupt(bytes))
    }

    /// The resident bytes for `key`, exactly as inserted, recording neither a
    /// hit nor a miss and applying no corruption: for a caller whose earlier
    /// [`get`](Self::get) already accounted this request. Unlike
    /// [`peek_uncounted`](Self::peek_uncounted) it skips the corruption gate.
    pub(crate) fn get_uncounted(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner.lookup(key)
    }

    /// Look up `key` without fetching and without recording a hit or a miss,
    /// for a caller whose earlier [`get`](Self::get) already accounted this
    /// request and that looks again because the entry may have been admitted
    /// since. Ages entries out like `get` and, in corruption mode, serves a hit
    /// corrupted like any other hit.
    pub fn peek_uncounted(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner
            .lookup(key)
            .map(|bytes| self.maybe_corrupt(bytes))
    }

    /// Admit `value` under `key`. Not an error, and a no-op on the
    /// eviction state, if `value` is larger than the configured maximum
    /// single-entry size: the caller still has its own copy of the bytes.
    ///
    /// The entry is charged `value.len()`, so it must not keep a larger
    /// allocation alive: unless `value` is the only handle to an allocation of
    /// exactly its length, an exact-size copy is stored instead (counted by
    /// [`CacheMetrics::admission_copies`]). A slice of a larger buffer, such as
    /// a response body that is a view into an HTTP read buffer, therefore
    /// stops pinning that buffer once the caller's handles drop. A caller that
    /// keeps a clone of `value` makes it shared, so it is always copied; pass
    /// it by value and use [`admit`](Self::admit) to get the stored bytes back.
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        self.inner.insert(key, value);
    }

    /// [`insert`](Self::insert), returning a handle to the bytes the tier now
    /// stores (or `value` itself if it was over the size limit), so an
    /// admitting caller that also serves the bytes need not hold a second
    /// handle to `value` across the admission and force a copy.
    pub(crate) fn admit(&self, key: CacheKey, value: Bytes) -> Bytes {
        self.inner.insert(key, value)
    }

    /// Whether this cache is in the ADR-0046 acceptance-gate corruption mode
    /// (built with [`Cache::with_corruption`]). The tiered handle reads this
    /// to decide whether a disk-served hit it serves must be corrupted too,
    /// so the gate covers both tiers rather than only the RAM one.
    pub fn corrupts_hits(&self) -> bool {
        self.corrupt_hits
    }

    fn maybe_corrupt(&self, bytes: Bytes) -> Bytes {
        if !self.corrupt_hits {
            return bytes;
        }
        crate::cache::corrupt_bytes(&bytes)
    }

    /// Collapses concurrent misses on the same key into one call to `fetch`
    /// (ADR-0046 decision 5) and, on a leader miss that succeeds, admits the
    /// result before returning it.
    ///
    /// Does not record a lookup of its own: both call sites in
    /// `ravel-query` already call `get` to decide their own hit/miss
    /// accounting (ADR-0044's `QueryAccounting` needs that branch either
    /// way) and call this only on that miss. An earlier version re-checked
    /// here too, so the *same* logical miss recorded two `CacheMetrics`
    /// misses -- one from the caller's `get`, one from this method's own --
    /// corrupting the request-hit-rate SLI ADR-0046 lists. Call `get`
    /// yourself first if you need the hit path; this is the miss-only half.
    ///
    /// A caller's `get` can miss while a flight for `key` is running and the
    /// caller reach here after that flight has left the single-flight map. Two
    /// things keep it from fetching the range a second time: the leader admits
    /// its bytes to RAM inside the flight, before the slot is removed, and a
    /// new leader rechecks RAM, uncounted, before it fetches. Such a caller is
    /// served the finished flight's bytes and records nothing beyond its own
    /// `get`'s miss. An entry over the size limit is not admitted, so that
    /// recheck misses and the caller fetches again; a failed fetch admits
    /// nothing.
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<Bytes, SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Bytes, E>> + Send,
    {
        self.get_or_fetch_outcome(key, fetch)
            .await
            .map(|(bytes, _)| bytes)
    }

    /// [`get_or_fetch`](Self::get_or_fetch), also reporting whether this call
    /// ran `fetch`. [`ReadOutcome::Fetched`] only when it did: this call led
    /// the flight and its RAM recheck missed. [`ReadOutcome::LateServe`]
    /// otherwise: the leader's RAM recheck served the bytes, or this call
    /// followed another caller's flight. Never [`ReadOutcome::Hit`]: this is
    /// the miss-only half, and the caller's own `get` already counted the
    /// miss a late serve stays. Corruption is unchanged from `get_or_fetch`:
    /// a recheck serve is corrupted in corruption mode, and a follower of a
    /// flight that fetched gets the clean fetched bytes.
    pub async fn get_or_fetch_outcome<F, Fut>(
        &self,
        key: CacheKey,
        fetch: F,
    ) -> Result<(Bytes, ReadOutcome), SingleFlightError<E>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<Bytes, E>> + Send,
    {
        let (result, role) = self
            .single_flight
            .run(key, move || async move {
                if let Some(bytes) = self.inner.lookup(&key) {
                    return Ok((bytes, true));
                }
                let bytes = self.admit(key, fetch().await?);
                Ok((bytes, false))
            })
            .await;
        let (bytes, from_cache) = result?;
        if role == Role::Follower {
            self.inner.metrics.record_collapse();
        }
        let outcome = if role == Role::Leader && !from_cache {
            ReadOutcome::Fetched
        } else {
            ReadOutcome::LateServe
        };
        let bytes = if from_cache {
            self.maybe_corrupt(bytes)
        } else {
            bytes
        };
        Ok((bytes, outcome))
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
    /// hit on a live entry, or a miss otherwise. An over-age entry is dropped
    /// and reported as an ordinary miss: unlike the disk tier, the RAM tier has
    /// no max-age expiry counter, so an aged-out read moves the same `misses`
    /// counter a cold miss does and nothing else.
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        match self.lookup(key) {
            Some(bytes) => {
                self.metrics.record_hit(bytes.len() as u64);
                Some(bytes)
            }
            None => {
                self.metrics.record_miss();
                None
            }
        }
    }

    /// [`Self::get`] without recording a hit or a miss.
    fn lookup(&self, key: &CacheKey) -> Option<Bytes> {
        let mut fifo = self.fifo.lock();
        let entry = fifo.get(key)?;
        if self.clock.now_ns().saturating_sub(entry.written_at_ns) > self.limits.max_entry_age_ns {
            // Older than the configured max-age: an erased subject's bytes must
            // not outlive the sweep in a query node's RAM by more than this
            // (ADR-0064). Dropped and re-fetched, not served.
            // `saturating_sub` keeps a clock that jumped backward (a stamp
            // newer than "now") from wrapping into a huge age: such an entry is
            // simply treated as not yet expired, mirroring the disk tier.
            fifo.remove(key);
            return None;
        }
        Some(entry.bytes)
    }

    /// Admits `value` as an allocation the entry owns outright (see
    /// [`owned_exact`]) and returns a handle to what was stored. An over-size
    /// value is declined by the eviction structure, so it is passed through
    /// uncopied.
    fn insert(&self, key: CacheKey, value: Bytes) -> Bytes {
        let size = value.len() as u64;
        let value = if size > self.limits.max_entry_bytes {
            value
        } else {
            owned_exact(value, &self.metrics)
        };
        let entry = CacheEntry {
            written_at_ns: self.clock.now_ns(),
            bytes: value.clone(),
        };
        self.fifo.lock().insert(key, entry, size, &self.metrics);
        value
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use tokio::sync::oneshot;

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

    /// `get_or_fetch_outcome` reports [`ReadOutcome::Fetched`] only to the call
    /// that ran its fetch: a follower of that flight and a later caller served
    /// by the RAM recheck are both [`ReadOutcome::LateServe`].
    ///
    /// FLIP: labelling the result from `from_cache` alone, ignoring the
    /// single-flight role, reports the follower as `ReadOutcome::Fetched`.
    #[tokio::test]
    async fn get_or_fetch_outcome_reports_fetched_only_for_the_fetching_call() {
        let cache: Arc<Cache<&'static str>> = Arc::new(Cache::new(generous_limits()));
        let payload = Bytes::from_static(b"fetched once");
        let key = test_key_with_len(1, payload.len() as u64);
        let fetches = Arc::new(AtomicUsize::new(0));

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let cache = cache.clone();
            let payload = payload.clone();
            let fetches = fetches.clone();
            tokio::spawn(async move {
                cache
                    .get_or_fetch_outcome(key, move || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<Bytes, &'static str>(payload)
                    })
                    .await
            })
        };
        entered_rx.await.expect("the leader reaches its fetch");
        let follower = {
            let cache = cache.clone();
            let fetches = fetches.clone();
            tokio::spawn(async move {
                cache
                    .get_or_fetch_outcome(key, move || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        Ok::<Bytes, &'static str>(Bytes::from_static(b"follower fetch"))
                    })
                    .await
            })
        };
        while cache.single_flight.waiters(&key) < 1 {
            tokio::task::yield_now().await;
        }
        release_tx.send(()).expect("the leader is still parked");
        let leader_result = leader.await.unwrap().unwrap();
        let follower_result = follower.await.unwrap().unwrap();
        assert_eq!(leader_result, (payload.clone(), ReadOutcome::Fetched));
        assert_eq!(follower_result, (payload.clone(), ReadOutcome::LateServe));

        let late = cache
            .get_or_fetch_outcome(key, || async {
                Ok::<Bytes, &'static str>(Bytes::from_static(b"second fetch"))
            })
            .await
            .unwrap();
        assert_eq!(
            late,
            (payload, ReadOutcome::LateServe),
            "the RAM recheck served it"
        );
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(cache.metrics().snapshot().single_flight_collapses, 1);
    }

    /// `peek_uncounted` serves a resident entry, corrupted in corruption mode
    /// like a `get` hit, and records neither a hit nor a miss.
    #[test]
    fn peek_uncounted_records_no_lookup_and_corrupts_like_get() {
        let cache: Cache<&'static str> = Cache::with_corruption(generous_limits());
        let payload = Bytes::from_static(b"resident");
        let key = test_key_with_len(1, payload.len() as u64);
        let absent = test_key_with_len(2, payload.len() as u64);
        cache.insert(key, payload.clone());
        let before = cache.metrics().snapshot();

        assert_eq!(cache.peek_uncounted(&key), Some(corrupt_bytes(&payload)));
        assert_eq!(cache.peek_uncounted(&absent), None);

        let after = cache.metrics().snapshot();
        assert_eq!(after.hits, before.hits);
        assert_eq!(after.misses, before.misses);
        assert_eq!(after.bytes_served, before.bytes_served);
    }

    /// A caller whose `get` missed while a fetch for the key was in flight, and
    /// that reaches `get_or_fetch` only after that flight has finished and left
    /// the single-flight map, is served the finished flight's bytes: the fetch
    /// closure runs once in total, and nothing is recorded beyond the late
    /// caller's own `get` miss.
    ///
    /// FLIP: removing the leader's `self.inner.lookup(&key)` recheck in
    /// `get_or_fetch` makes the late caller lead a second flight, so
    /// `fetches` reads 2.
    #[tokio::test]
    async fn get_or_fetch_after_the_flight_finished_reuses_its_bytes() {
        let cache: Arc<Cache<&'static str>> = Arc::new(Cache::new(generous_limits()));
        let payload = Bytes::from_static(b"fetched once");
        let key = test_key_with_len(1, payload.len() as u64);
        let fetches = Arc::new(AtomicUsize::new(0));

        assert!(cache.get(&key).is_none(), "the leader's own get misses");
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let leader = {
            let cache = cache.clone();
            let payload = payload.clone();
            let fetches = fetches.clone();
            tokio::spawn(async move {
                cache
                    .get_or_fetch(key, move || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(());
                        let _ = release_rx.await;
                        Ok::<Bytes, &'static str>(payload)
                    })
                    .await
            })
        };
        entered_rx.await.expect("the leader reaches its fetch");

        assert!(
            cache.get(&key).is_none(),
            "the late caller's get misses while the leader's fetch is in flight"
        );
        let after_gets = cache.metrics().snapshot();
        release_tx.send(()).expect("the leader is still parked");
        let leader_bytes = leader.await.unwrap().unwrap();
        assert!(
            !cache.single_flight.is_in_flight(&key),
            "the flight has finished and left the map"
        );

        let ran = fetches.clone();
        let late_bytes = cache
            .get_or_fetch(key, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok::<Bytes, &'static str>(Bytes::from_static(b"second fetch"))
            })
            .await
            .unwrap();

        assert_eq!(
            fetches.load(Ordering::SeqCst),
            1,
            "a miss peeked during the flight must not fetch again"
        );
        assert_eq!(late_bytes, payload);
        assert_eq!(leader_bytes, late_bytes);
        let after = cache.metrics().snapshot();
        assert_eq!(
            after.misses, after_gets.misses,
            "the recheck records no miss"
        );
        assert_eq!(after.hits, after_gets.hits, "the recheck records no hit");
        assert_eq!(after.bytes_served, after_gets.bytes_served);
        assert_eq!(after.bytes_admitted, payload.len() as u64, "admitted once");
        assert_eq!(after.single_flight_collapses, 0);
    }

    /// A clock that, on every read, records whether `key`'s flight is still in
    /// `cache`'s single-flight map. A fresh cache reads the clock only to stamp
    /// an admission, so each record says whether that admission ran inside the
    /// flight.
    struct FlightProbeClock {
        cache: std::sync::OnceLock<Weak<Cache<&'static str>>>,
        key: CacheKey,
        in_flight_at_read: Mutex<Vec<bool>>,
    }

    impl Clock for FlightProbeClock {
        fn now_ns(&self) -> u64 {
            if let Some(cache) = self.cache.get().and_then(Weak::upgrade) {
                let in_flight = cache.single_flight.is_in_flight(&self.key);
                self.in_flight_at_read.lock().push(in_flight);
            }
            1
        }
    }

    /// The leader admits its bytes to RAM before its slot leaves the
    /// single-flight map, so a caller that finds no slot finds the bytes: there
    /// is no window between the slot's removal and the admission in which a
    /// new caller would lead a second fetch.
    ///
    /// FLIP: moving `self.insert(key, bytes.clone())` out of the flight
    /// closure to after `single_flight.run` returns records the admission with
    /// the flight already gone, so `in_flight_at_read` reads `[false]`.
    #[tokio::test]
    async fn get_or_fetch_admits_to_ram_before_the_flight_leaves_the_map() {
        let key = test_key_with_len(1, 5);
        let clock = Arc::new(FlightProbeClock {
            cache: std::sync::OnceLock::new(),
            key,
            in_flight_at_read: Mutex::new(Vec::new()),
        });
        let cache: Arc<Cache<&'static str>> =
            Arc::new(Cache::new_with_clock(generous_limits(), clock.clone()));
        assert!(clock.cache.set(Arc::downgrade(&cache)).is_ok());

        let bytes = cache
            .get_or_fetch(key, || async {
                Ok::<Bytes, &'static str>(Bytes::from_static(b"hello"))
            })
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"hello");
        assert_eq!(
            *clock.in_flight_at_read.lock(),
            vec![true],
            "one admission, made while the flight still held its slot"
        );
        assert_eq!(cache.len(), 1);
    }

    /// The recheck changes nothing for an entry the RAM tier refuses or a fetch
    /// that fails: neither is admitted, so the next leader fetches again.
    #[tokio::test]
    async fn get_or_fetch_admits_no_oversized_entry_and_no_failed_fetch() {
        let limits = CacheLimits::new(64 * 1024 * 1024, 10_000, 4);
        let cache: Cache<&'static str> = Cache::new(limits);
        let fetches = AtomicUsize::new(0);
        let big = test_key_with_len(1, 5);
        for _ in 0..2 {
            let bytes = cache
                .get_or_fetch(big, || async {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<Bytes, &'static str>(Bytes::from_static(b"hello"))
                })
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), b"hello");
        }
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            2,
            "an entry over the size limit is never served by the recheck"
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 2);

        let failing = test_key_with_len(2, 3);
        let result = cache
            .get_or_fetch(failing, || async { Err::<Bytes, &'static str>("boom") })
            .await;
        assert!(matches!(result, Err(SingleFlightError::Upstream("boom"))));
        assert_eq!(cache.len(), 0, "a failed fetch admits nothing");
        assert_eq!(cache.metrics().snapshot().bytes_admitted, 0);
    }

    /// In corruption mode a late caller served by the recheck gets corrupted
    /// bytes, like any other RAM hit, while the fetching leader gets the clean
    /// upstream bytes.
    #[tokio::test]
    async fn get_or_fetch_recheck_serve_is_corrupted_in_corruption_mode() {
        let cache: Cache<&'static str> = Cache::with_corruption(generous_limits());
        let key = test_key_with_len(1, 5);
        let clean = Bytes::from_static(b"hello");
        let fresh = cache
            .get_or_fetch(key, || async { Ok::<Bytes, &'static str>(clean.clone()) })
            .await
            .unwrap();
        assert_eq!(
            fresh, clean,
            "the fetching leader's bytes are not corrupted"
        );
        let late = cache
            .get_or_fetch(key, || async {
                Ok::<Bytes, &'static str>(Bytes::from_static(b"never"))
            })
            .await
            .unwrap();
        assert_eq!(late, corrupt_bytes(&clean));
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

    const BIG: usize = 1024 * 1024;
    const SLICE: usize = 64 * 1024;

    /// A 1 MiB buffer with no repeating 64 KiB window, so a hit that returned
    /// the wrong range would not compare equal.
    fn big_buffer() -> Bytes {
        Bytes::from((0..BIG).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
    }

    /// Issue #2633: an entry admitted as a 64 KiB slice of a 1 MiB buffer is
    /// charged 64 KiB, so it must not keep the 1 MiB buffer alive. Once the
    /// cache's returned handles drop, the caller's original handle is the only
    /// one left on that buffer.
    ///
    /// FLIP (non-vacuity): store `value` itself in `Inner::insert` instead of
    /// `owned_exact(value, ..)`. The entry then holds the slice, and
    /// `big.is_unique()` fails.
    #[test]
    fn insert_of_a_slice_stores_an_owned_copy_and_frees_the_larger_buffer() {
        let cache: Cache<&'static str> = Cache::new(generous_limits());
        let big = big_buffer();
        let expected = big.slice(0..SLICE).to_vec();
        let key = test_key_with_len(1, SLICE as u64);

        cache.insert(key, big.slice(0..SLICE));
        let hit = cache.get(&key).unwrap();
        assert_eq!(hit.as_ref(), expected.as_slice());
        drop(hit);

        assert!(
            big.is_unique(),
            "the cache entry must not hold a handle on the 1 MiB buffer"
        );
        assert_eq!(cache.total_bytes(), SLICE as u64, "charged 64 KiB");
        let metrics = cache.metrics();
        assert_eq!(metrics.admission_copies(), 1);
        assert_eq!(metrics.admission_copied_bytes(), SLICE as u64);

        // A hit serves the stored copy; it copies nothing more.
        let again = cache.get(&key).unwrap();
        assert_eq!(again.as_ref(), expected.as_slice());
        assert_eq!(metrics.admission_copies(), 1, "a hit is never a copy");
    }

    /// The other side of the rule: a value that is already the sole handle to
    /// an allocation of exactly its length is stored as is. The served bytes
    /// are the inserted allocation (same pointer), and the counter stays 0.
    #[test]
    fn exact_size_unique_value_is_stored_without_a_copy() {
        let cache: Cache<&'static str> = Cache::new(generous_limits());
        let value = Bytes::from(vec![0x5Au8; SLICE]);
        let ptr = value.as_ptr();
        let key = test_key_with_len(1, SLICE as u64);

        cache.insert(key, value);
        let hit = cache.get(&key).unwrap();
        assert_eq!(hit.as_ptr(), ptr, "the original allocation is stored");
        assert_eq!(hit.as_ref(), vec![0x5Au8; SLICE].as_slice());
        assert_eq!(cache.metrics().admission_copies(), 0);
        assert_eq!(cache.metrics().admission_copied_bytes(), 0);
    }

    /// A sole handle can still pin more than its length: the tail of a buffer
    /// whose head was dropped (`BytesMut::capacity` reads exactly its length,
    /// but the allocation starts before it), and a `Vec` with spare capacity.
    /// Both are copied.
    ///
    /// FLIP (non-vacuity): in `owned_exact`, return `Bytes::from(vec)` without
    /// the `vec.capacity() == vec.len()` check. The tail case is then stored
    /// uncopied and the first count reads 0.
    #[test]
    fn unique_value_on_a_larger_allocation_is_copied() {
        let cache: Cache<&'static str> = Cache::new(generous_limits());
        let metrics = cache.metrics();

        let mut head = big_buffer();
        let tail = head.split_off(BIG - SLICE);
        drop(head);
        assert!(tail.is_unique(), "precondition: the tail is a sole handle");
        cache.insert(test_key_with_len(1, SLICE as u64), tail);
        assert_eq!(metrics.admission_copies(), 1, "the tail is copied");

        let mut spare = Vec::with_capacity(BIG);
        spare.extend_from_slice(&[7u8; SLICE]);
        let spare = Bytes::from(spare);
        assert!(spare.is_unique(), "precondition: a sole handle");
        cache.insert(test_key_with_len(2, SLICE as u64), spare);
        assert_eq!(metrics.admission_copies(), 2, "spare capacity is copied");
        assert_eq!(metrics.admission_copied_bytes(), 2 * SLICE as u64);
        assert_eq!(cache.total_bytes(), 2 * SLICE as u64);
    }

    /// The `get_or_fetch` admission path, which admits inside the flight, gets
    /// the same treatment as `insert`: a fetch that returns a slice of a 1 MiB
    /// buffer leaves that buffer uniquely owned once the returned handle drops.
    ///
    /// FLIP (non-vacuity): in `get_or_fetch_outcome`, admit with
    /// `self.inner.fifo.lock().insert(..)` on the raw fetched value (or any
    /// admission that bypasses `owned_exact`). `big.is_unique()` then fails.
    #[tokio::test]
    async fn get_or_fetch_of_a_slice_stores_an_owned_copy() {
        let cache: Cache<&'static str> = Cache::new(generous_limits());
        let big = big_buffer();
        let expected = big.slice(0..SLICE).to_vec();
        let key = test_key_with_len(1, SLICE as u64);

        let fetched = big.slice(0..SLICE);
        let got = cache
            .get_or_fetch(key, move || async move { Ok::<Bytes, &'static str>(fetched) })
            .await
            .unwrap();
        assert_eq!(got.as_ref(), expected.as_slice());
        drop(got);

        assert!(
            big.is_unique(),
            "the cache entry must not hold a handle on the 1 MiB buffer"
        );
        assert_eq!(cache.total_bytes(), SLICE as u64, "charged 64 KiB");
        let metrics = cache.metrics();
        assert_eq!(metrics.admission_copies(), 1);
        assert_eq!(metrics.admission_copied_bytes(), SLICE as u64);
        let hit = cache.get(&key).unwrap();
        assert_eq!(hit.as_ref(), expected.as_slice());
        assert_eq!(metrics.admission_copies(), 1, "a hit is never a copy");
    }

    /// `get_or_fetch` admits an exact-size fetched value without a copy and
    /// serves the caller that same allocation.
    #[tokio::test]
    async fn get_or_fetch_of_an_exact_size_value_is_not_copied() {
        let cache: Cache<&'static str> = Cache::new(generous_limits());
        let value = Bytes::from(vec![0x5Au8; SLICE]);
        let ptr = value.as_ptr();
        let key = test_key_with_len(1, SLICE as u64);

        let got = cache
            .get_or_fetch(key, move || async move { Ok::<Bytes, &'static str>(value) })
            .await
            .unwrap();
        assert_eq!(got.as_ptr(), ptr);
        assert_eq!(cache.get(&key).unwrap().as_ptr(), ptr);
        assert_eq!(cache.metrics().admission_copies(), 0);
    }
}
