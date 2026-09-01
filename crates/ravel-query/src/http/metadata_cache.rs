//! Per-process, per-tenant, on-demand cache over the catalog's metric metadata
//! record (`t/<tenant_hash>/m/meta`), the read path of ADR-0085 decision 1.
//!
//! `/api/v1/metadata` is on Grafana's dashboard-load and datasource-probe
//! critical path: an uncached design would turn every one of those into an S3
//! round trip that is both user-visible latency and an unbudgeted request-count
//! line item (ADR-0075). This cache serves the same data at one GET per
//! (queried tenant, refresh horizon, process), independent of request rate.
//!
//! Shape (ADR-0085 "Read path."):
//!
//! - **On demand.** A tenant nobody asks about costs zero requests. The first
//!   request for a tenant does one [`read_metrics_meta`] GET, keeps the parsed
//!   record (an absent record caches as an empty snapshot), and serves it.
//! - **Bounded staleness.** A request within the refresh horizon (default 60 s,
//!   the same bounded-staleness horizon `config` and the lifecycle gate use) is
//!   served from memory with no I/O. A request past the horizon is served from
//!   the cached record *immediately* while at most one background refresh GET
//!   runs (stale-while-revalidate), so no client ever waits on S3 for metadata
//!   after the first call.
//! - **Single-flight refresh.** A per-tenant `refreshing` flag under the state
//!   lock means concurrent past-horizon requests trigger one refresh, not N. A
//!   refresh error keeps serving the stale record, is counted and logged, and
//!   is never surfaced to the client.
//! - **Bounded memory.** The cached-tenant count is LRU-bounded (default 256)
//!   and an entry idle longer than `idle_evict_after` (default 5 horizons) is
//!   dropped on the next access, so the process holds records only for tenants
//!   it recently answered for.
//!
//! This cache is constructed only in ravel-query's own tests for now; the
//! shipping-binary wiring (`ravel-server` building it from its store and
//! calling [`AppState::with_metadata_cache`](crate::http::AppState::with_metadata_cache))
//! and the ingest-to-query end-to-end test are issue #237.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_cache::Clock;
use ravel_catalog::{MetricMetadataEntry, read_metrics_meta};
use ravel_object_store::{ObjectStoreBackend, Version};
use ravel_types::TenantHash;

/// A cheap-to-clone snapshot of one tenant's metric metadata: the decoded
/// entries behind an `Arc`, so a handler clones a pointer, never the record.
pub type MetadataSnapshot = Arc<Vec<MetricMetadataEntry>>;

/// Tuning for [`MetadataCache`]. Every default is the ADR-0085 value.
#[derive(Debug, Clone, Copy)]
pub struct MetadataCacheConfig {
    /// How long a cached record is served without a refresh. A request past
    /// this horizon still returns the cached value immediately and triggers one
    /// background refresh (stale-while-revalidate). Default 60 s.
    pub refresh_horizon: Duration,
    /// LRU bound on the number of distinct tenants held at once. A many-tenant
    /// deployment stays predictable: touching an (N+1)th tenant evicts the
    /// least-recently-used one. Default 256.
    pub max_tenants: usize,
    /// A tenant entry untouched for longer than this is dropped on the next
    /// access, so the process does not hold records for tenants it has stopped
    /// answering for. Default 5 refresh horizons.
    pub idle_evict_after: Duration,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        let refresh_horizon = Duration::from_secs(60);
        MetadataCacheConfig {
            refresh_horizon,
            max_tenants: 256,
            idle_evict_after: refresh_horizon * 5,
        }
    }
}

/// A point-in-time read of the cache's counters, following this crate's
/// plain-`AtomicU64`-behind-`Arc` accounting shape (see
/// [`ravel_types::accounting`]). A `/metrics` exporter renders these as the
/// ADR-0044-label-allowlisted process counters
/// `query_metadata_cache_hits_total`, `query_metadata_cache_misses_total`,
/// `query_metadata_cache_refreshes_total`, and
/// `query_metadata_cache_refresh_errors_total`. Like every scrape in this
/// crate it is a read of independently-updated atomics, not a consistent cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetadataCacheCounters {
    /// Requests served from an already-cached tenant record (fresh or stale).
    pub hits: u64,
    /// Requests that found no cached record and did an inline fill GET.
    pub misses: u64,
    /// Background refreshes started (a past-horizon request that won the
    /// single-flight). Includes refreshes that later errored.
    pub refreshes: u64,
    /// Background refreshes that failed their GET or decode. The stale record
    /// keeps being served; the client never sees the error.
    pub refresh_errors: u64,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    refreshes: AtomicU64,
    refresh_errors: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> MetadataCacheCounters {
        MetadataCacheCounters {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            refreshes: self.refreshes.load(Ordering::Relaxed),
            refresh_errors: self.refresh_errors.load(Ordering::Relaxed),
        }
    }
}

/// One tenant's cached record and the bookkeeping the horizon, single-flight,
/// and eviction rules need.
struct TenantEntry {
    snapshot: MetadataSnapshot,
    /// The store version the snapshot was read at, carried so a later refresh
    /// could CAS if it needed to (it only GETs today). `None` for an absent
    /// record cached as empty.
    #[allow(dead_code)]
    version: Option<Version>,
    /// Clock reading when `snapshot` was last filled or refreshed. Horizon is
    /// `now - fetched_at_ns`.
    fetched_at_ns: u64,
    /// Clock reading of the last request that touched this entry. Drives LRU
    /// eviction and idle eviction.
    last_access_ns: u64,
    /// A background refresh is in flight for this tenant. The single-flight
    /// guard: a second past-horizon request observing `true` skips its own
    /// refresh rather than firing a duplicate GET.
    refreshing: bool,
}

struct Inner {
    tenants: HashMap<TenantHash, TenantEntry>,
}

/// The shared, `Arc`-held state a background refresh task captures. Holding it
/// behind one `Arc` lets a spawned refresh write its result back into the same
/// map the foreground `get` reads, without borrowing the `MetadataCache`.
struct Shared {
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    config: MetadataCacheConfig,
    inner: std::sync::Mutex<Inner>,
    counters: Counters,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock means a prior holder panicked mid-update. The cached
        // metadata is best-effort observability, never correctness-bearing, so
        // recovering the guard and continuing is strictly better than
        // propagating a panic onto every future metadata request.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn horizon_ns(&self) -> u64 {
        u64::try_from(self.config.refresh_horizon.as_nanos()).unwrap_or(u64::MAX)
    }

    fn idle_evict_ns(&self) -> u64 {
        u64::try_from(self.config.idle_evict_after.as_nanos()).unwrap_or(u64::MAX)
    }
}

/// A per-process, per-tenant, on-demand cache over the metric metadata record.
///
/// Cheap to clone: every clone shares the one backing state and counter set, so
/// the router can hold it behind an `Arc` and every request reads the same
/// cache.
#[derive(Clone)]
pub struct MetadataCache {
    shared: Arc<Shared>,
}

impl MetadataCache {
    /// Build a cache over `store`, reading the wall clock through `clock` so the
    /// horizon and idle-eviction decisions are deterministic under test (repo
    /// rule: no `SystemTime::now()` in library logic).
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        config: MetadataCacheConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        MetadataCache {
            shared: Arc::new(Shared {
                store,
                clock,
                config,
                inner: std::sync::Mutex::new(Inner {
                    tenants: HashMap::new(),
                }),
                counters: Counters::default(),
            }),
        }
    }

    /// A point-in-time read of the cache counters.
    pub fn counters(&self) -> MetadataCacheCounters {
        self.shared.counters.snapshot()
    }

    /// Get the cached metadata snapshot for `tenant_hash`, filling on a miss.
    ///
    /// - Miss: one [`read_metrics_meta`] GET inline; the parsed record (or an
    ///   empty snapshot for an absent record) is stored and returned.
    /// - Hit within the horizon: the cached snapshot, no I/O.
    /// - Hit past the horizon: the cached snapshot returned immediately, plus at
    ///   most one background refresh GET (single-flight per tenant).
    ///
    /// Never returns an error: a refresh failure keeps serving the stale record
    /// and is counted, and a miss whose GET fails caches an empty snapshot for
    /// the horizon rather than turning a best-effort probe into a fault.
    pub async fn get(&self, tenant_hash: TenantHash) -> MetadataSnapshot {
        let now = self.shared.clock.now_ns();

        // Fast path under the lock: sweep idle entries, then serve a hit (and
        // arm a single-flight refresh if it is past the horizon).
        {
            let mut inner = self.shared.lock();
            self.sweep_idle(&mut inner, now);
            if let Some(entry) = inner.tenants.get_mut(&tenant_hash) {
                entry.last_access_ns = now;
                let snapshot = Arc::clone(&entry.snapshot);
                let stale = now.saturating_sub(entry.fetched_at_ns) > self.shared.horizon_ns();
                if stale && !entry.refreshing {
                    entry.refreshing = true;
                    self.shared
                        .counters
                        .refreshes
                        .fetch_add(1, Ordering::Relaxed);
                    spawn_refresh(Arc::clone(&self.shared), tenant_hash);
                }
                self.shared.counters.hits.fetch_add(1, Ordering::Relaxed);
                return snapshot;
            }
        }

        // Miss: fill inline with one GET, off the lock so a slow store never
        // blocks another tenant's request.
        self.shared.counters.misses.fetch_add(1, Ordering::Relaxed);
        let (snapshot, version) = fetch_snapshot(self.shared.as_ref(), tenant_hash).await;

        let mut inner = self.shared.lock();
        // Re-check under the lock: a concurrent miss for the same tenant may
        // have filled it while this GET was in flight. Keep whichever is
        // already there rather than clobber it, and never grow past the bound.
        if let Some(entry) = inner.tenants.get_mut(&tenant_hash) {
            entry.last_access_ns = now;
            return Arc::clone(&entry.snapshot);
        }
        self.evict_if_full(&mut inner, tenant_hash);
        inner.tenants.insert(
            tenant_hash,
            TenantEntry {
                snapshot: Arc::clone(&snapshot),
                version,
                fetched_at_ns: now,
                last_access_ns: now,
                refreshing: false,
            },
        );
        snapshot
    }

    /// Drop entries untouched for longer than `idle_evict_after`. Runs on every
    /// access, so an idle tenant's record is reclaimed by the next unrelated
    /// request without a background sweeper task.
    fn sweep_idle(&self, inner: &mut Inner, now: u64) {
        let idle = self.shared.idle_evict_ns();
        inner
            .tenants
            .retain(|_, e| now.saturating_sub(e.last_access_ns) <= idle);
    }

    /// If the map is at `max_tenants`, evict the least-recently-used tenant to
    /// make room for `incoming`. `incoming` is never itself in the map here
    /// (the caller only evicts on a confirmed miss), so it can never evict the
    /// entry it is about to insert.
    fn evict_if_full(&self, inner: &mut Inner, incoming: TenantHash) {
        let max = self.shared.config.max_tenants.max(1);
        while inner.tenants.len() >= max {
            let victim = inner
                .tenants
                .iter()
                .filter(|(k, _)| **k != incoming)
                .min_by_key(|(_, e)| e.last_access_ns)
                .map(|(k, _)| *k);
            match victim {
                Some(k) => {
                    inner.tenants.remove(&k);
                }
                None => break,
            }
        }
    }
}

/// Read a tenant's record into a snapshot. An absent record and any read error
/// both become an empty snapshot: metadata is best-effort, so its absence or a
/// transient store failure is not a fault to surface. A read error on the miss
/// path is logged; the horizon then bounds how long the empty answer is served.
async fn fetch_snapshot(
    shared: &Shared,
    tenant_hash: TenantHash,
) -> (MetadataSnapshot, Option<Version>) {
    match read_metrics_meta(shared.store.as_ref(), &tenant_hash).await {
        Ok(Some((entries, version))) => (Arc::new(entries), Some(version)),
        Ok(None) => (Arc::new(Vec::new()), None),
        Err(err) => {
            tracing::warn!(
                target: "ravel_query::metadata_cache",
                error = %err,
                "metric metadata read failed; serving an empty record for this horizon"
            );
            (Arc::new(Vec::new()), None)
        }
    }
}

/// Spawn the single background refresh for `tenant_hash`. The caller has already
/// set `refreshing = true` under the lock and counted the refresh, so this is
/// the one GET the single-flight guard admits. On success the entry's snapshot,
/// version, and `fetched_at_ns` advance; on error the stale snapshot stays and
/// `refresh_errors` is incremented. Either way `refreshing` clears so a later
/// past-horizon request can refresh again.
fn spawn_refresh(shared: Arc<Shared>, tenant_hash: TenantHash) {
    tokio::spawn(async move {
        let result = read_metrics_meta(shared.store.as_ref(), &tenant_hash).await;
        let now = shared.clock.now_ns();
        let mut inner = shared.lock();
        let Some(entry) = inner.tenants.get_mut(&tenant_hash) else {
            // The tenant was evicted while its refresh ran. Nothing to write
            // back; the next request for it will miss and fill fresh.
            return;
        };
        match result {
            Ok(Some((entries, version))) => {
                entry.snapshot = Arc::new(entries);
                entry.version = Some(version);
                entry.fetched_at_ns = now;
            }
            Ok(None) => {
                // The record was deleted out from under us (no role holds
                // delete on this key today, so this is not expected, but it is
                // representable): serve empty going forward.
                entry.snapshot = Arc::new(Vec::new());
                entry.version = None;
                entry.fetched_at_ns = now;
            }
            Err(err) => {
                shared
                    .counters
                    .refresh_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    target: "ravel_query::metadata_cache",
                    error = %err,
                    "metric metadata refresh failed; keeping the stale record"
                );
            }
        }
        entry.refreshing = false;
    });
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_catalog::{MetricKind, write_metrics_meta};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, PageToken,
        PutOptions, PutOutcome, StoreError,
    };
    use std::sync::atomic::AtomicI64;

    /// A test clock the test advances by hand, so horizon and idle-eviction
    /// crossings are exercised with no real sleep.
    struct TestClock {
        now: AtomicI64,
    }

    impl TestClock {
        fn new(now: i64) -> Arc<Self> {
            Arc::new(TestClock {
                now: AtomicI64::new(now),
            })
        }
        fn advance(&self, by: Duration) {
            self.now.fetch_add(
                i64::try_from(by.as_nanos()).expect("fits"),
                Ordering::SeqCst,
            );
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> u64 {
            u64::try_from(self.now.load(Ordering::SeqCst)).expect("non-negative test time")
        }
    }

    /// Wraps a store and counts GETs, so a test can assert the exact request
    /// count the ADR budgets ("one GET per queried tenant per horizon").
    struct CountingStore {
        inner: Arc<dyn ObjectStoreBackend>,
        gets: AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
            Arc::new(CountingStore {
                inner,
                gets: AtomicU64::new(0),
            })
        }
        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for CountingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    fn entry(name: &str, kind: MetricKind, help: &str, unit: &str) -> MetricMetadataEntry {
        MetricMetadataEntry {
            family_name: name.to_string(),
            kind,
            help: help.to_string(),
            unit: unit.to_string(),
            updated_unix_ns: 1,
        }
    }

    async fn seed(store: &dyn ObjectStoreBackend, th: TenantHash, entries: &[MetricMetadataEntry]) {
        // Read the current version so a re-seed (a changed record) is a CAS,
        // not a losing CreateIfAbsent.
        let expected = read_metrics_meta(store, &th)
            .await
            .expect("read")
            .map(|(_, v)| v);
        write_metrics_meta(store, &th, entries, expected)
            .await
            .expect("seed record");
    }

    /// Yield until the store's GET count reaches `target`, so a spawned refresh
    /// is observed to have run without any real sleep. Bounded so a bug fails
    /// the test instead of hanging.
    async fn wait_for_gets(store: &CountingStore, target: u64) {
        for _ in 0..1000 {
            if store.get_count() >= target {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "store never reached {target} GETs (saw {})",
            store.get_count()
        );
    }

    async fn wait_for_refresh_errors(cache: &MetadataCache, target: u64) {
        for _ in 0..1000 {
            if cache.counters().refresh_errors >= target {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "refresh_errors never reached {target} (saw {})",
            cache.counters().refresh_errors
        );
    }

    /// Acceptance test (ADR-0085 read path): within the horizon the store is hit
    /// exactly once however many requests arrive, and every response carries the
    /// seeded record.
    ///
    /// prove-the-test: flipping the cache to fetch per request (replacing the
    /// hit branch of `get` with an unconditional `fetch_snapshot`) makes this
    /// fail with `expected 1 GET, saw 10` on the `get_count()` assertion.
    #[tokio::test]
    async fn metadata_served_from_cache_after_single_get() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let th = tenant(0xA1);
        seed(
            store.as_ref(),
            th,
            &[
                entry("http_requests_total", MetricKind::Counter, "reqs", ""),
                entry("mem_bytes", MetricKind::Gauge, "resident", "bytes"),
            ],
        )
        .await;
        // seed() itself read once; count GETs against the handler only.
        let base = store.get_count();

        let clock = TestClock::new(1_000);
        let cache =
            MetadataCache::new(store.clone(), MetadataCacheConfig::default(), clock.clone());

        for _ in 0..10 {
            let snap = cache.get(th).await;
            assert_eq!(snap.len(), 2, "every response carries the seeded entries");
            assert!(snap.iter().any(|e| e.family_name == "http_requests_total"));
            assert!(snap.iter().any(|e| e.family_name == "mem_bytes"));
        }
        assert_eq!(
            store.get_count() - base,
            1,
            "10 requests within the horizon cost exactly one GET"
        );
        let c = cache.counters();
        assert_eq!(c.misses, 1);
        assert_eq!(c.hits, 9);
    }

    #[tokio::test]
    async fn past_horizon_serves_stale_and_refreshes_once() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let th = tenant(0xB2);
        seed(
            store.as_ref(),
            th,
            &[entry("old_metric", MetricKind::Counter, "old", "")],
        )
        .await;
        let base = store.get_count();

        let clock = TestClock::new(10_000);
        let cache =
            MetadataCache::new(store.clone(), MetadataCacheConfig::default(), clock.clone());

        // Fill the cache (one GET).
        let first = cache.get(th).await;
        assert_eq!(first.len(), 1);
        assert_eq!(store.get_count() - base, 1);

        // The record changes durably, and the clock crosses the horizon.
        seed(
            store.as_ref(),
            th,
            &[
                entry("old_metric", MetricKind::Counter, "old", ""),
                entry("new_metric", MetricKind::Gauge, "new", "bytes"),
            ],
        )
        .await;
        let base = store.get_count();
        clock.advance(Duration::from_secs(61));

        // Five concurrent past-horizon requests: all return immediately with a
        // valid (old or new) record, and together trigger one refresh GET.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move { cache.get(th).await }));
        }
        for h in handles {
            let snap = h.await.expect("no panic");
            assert!(!snap.is_empty(), "never an error/empty record");
        }
        wait_for_gets(&store, base + 1).await;
        assert_eq!(
            store.get_count() - base,
            1,
            "concurrent past-horizon requests single-flight to one GET"
        );

        // A later request sees the refreshed record.
        let after = cache.get(th).await;
        assert_eq!(after.len(), 2, "the refresh brought in the new metric");
        assert!(after.iter().any(|e| e.family_name == "new_metric"));
    }

    #[tokio::test]
    async fn refresh_error_keeps_serving_stale() {
        let inner = MemoryStore::new();
        let th = tenant(0xC3);
        seed(
            &inner,
            th,
            &[entry("stable_metric", MetricKind::Counter, "help", "")],
        )
        .await;
        // Fault only the 2nd matching GET (the refresh): the 1st (the miss
        // fill) passes so the cache holds a record to serve stale from. seed()
        // read the raw MemoryStore before it was wrapped, so it does not count.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
                .with_key_contains("/m/meta")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let fault = Arc::new(FaultStore::new(inner, plan));
        let store = CountingStore::new(fault.clone());

        let clock = TestClock::new(5_000);
        let cache =
            MetadataCache::new(store.clone(), MetadataCacheConfig::default(), clock.clone());

        // First GET (the 1st occurrence) fills the cache and is not faulted.
        let first = cache.get(th).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].family_name, "stable_metric");

        clock.advance(Duration::from_secs(61));
        // Past-horizon request: returns stale immediately, triggers a refresh
        // whose GET (the 2nd occurrence) is faulted.
        let stale = cache.get(th).await;
        assert_eq!(stale.len(), 1, "the stale record is served, not an error");
        assert_eq!(stale[0].family_name, "stable_metric");

        wait_for_refresh_errors(&cache, 1).await;
        assert_eq!(
            fault.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected refresh fault fired",
        );
        assert_eq!(cache.counters().refresh_errors, 1);

        // The stale record is still served after the failed refresh.
        let again = cache.get(th).await;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].family_name, "stable_metric");
    }

    #[tokio::test]
    async fn absent_record_is_cached_as_empty() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let th = tenant(0xD4);

        let clock = TestClock::new(1);
        let cache = MetadataCache::new(store.clone(), MetadataCacheConfig::default(), clock);

        let first = cache.get(th).await;
        assert!(first.is_empty(), "no record yet -> empty snapshot");
        assert_eq!(store.get_count(), 1);

        let second = cache.get(th).await;
        assert!(second.is_empty());
        assert_eq!(
            store.get_count(),
            1,
            "the absent record is cached, not re-GET"
        );
    }

    #[tokio::test]
    async fn lru_bound_evicts_least_recently_used_tenant() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let (a, b, c) = (tenant(1), tenant(2), tenant(3));
        for th in [a, b, c] {
            seed(
                store.as_ref(),
                th,
                &[entry("m", MetricKind::Gauge, "h", "")],
            )
            .await;
        }
        let base = store.get_count();

        let clock = TestClock::new(1_000);
        let config = MetadataCacheConfig {
            max_tenants: 2,
            ..MetadataCacheConfig::default()
        };
        let cache = MetadataCache::new(store.clone(), config, clock.clone());

        // Touch A, then B, then C. Each touch advances the clock so the LRU
        // order is unambiguous. C's fill evicts A (the least-recently-used).
        cache.get(a).await;
        clock.advance(Duration::from_secs(1));
        cache.get(b).await;
        clock.advance(Duration::from_secs(1));
        cache.get(c).await;
        assert_eq!(store.get_count() - base, 3, "three misses, three GETs");

        // B and C are hits (no GET); A was evicted, so it misses and GETs again.
        let base = store.get_count();
        cache.get(b).await;
        cache.get(c).await;
        assert_eq!(store.get_count() - base, 0, "B and C are still cached");
        cache.get(a).await;
        assert_eq!(store.get_count() - base, 1, "A was evicted and re-fetched");
    }

    #[tokio::test]
    async fn idle_entry_is_evicted_on_next_access() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let (a, b) = (tenant(7), tenant(8));
        for th in [a, b] {
            seed(
                store.as_ref(),
                th,
                &[entry("m", MetricKind::Gauge, "h", "")],
            )
            .await;
        }
        let base = store.get_count();

        let clock = TestClock::new(1_000);
        // idle_evict_after defaults to 5 * 60s = 300s.
        let cache =
            MetadataCache::new(store.clone(), MetadataCacheConfig::default(), clock.clone());

        cache.get(a).await;
        assert_eq!(store.get_count() - base, 1);

        // Let A sit idle well past idle_evict_after, then touch B: A is swept.
        clock.advance(Duration::from_secs(301));
        cache.get(b).await;
        // A must miss now (it was evicted by the sweep on B's access).
        let base = store.get_count();
        cache.get(a).await;
        assert_eq!(store.get_count() - base, 1, "the idle A entry was evicted");
    }
}
