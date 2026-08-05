//! ADR-0046 cache warmup: populate the read cache with each tenant's most
//! recent parts before `/readyz` latches, so the first real query after a
//! restart is not the one that pays every cold-fetch cost.
//!
//! Bounded on two axes. Per (tenant, signal), at most
//! [`MAX_PARTS_PER_TENANT_PER_SIGNAL`] of the most recent parts are warmed:
//! this pass exists to take the edge off a cold restart, not to size a
//! working set only real query traffic can size correctly. The whole pass
//! is wrapped in `tokio::time::timeout(`[`WARM_DEADLINE`]`, ..)`, so a large
//! tenant count or a slow store can never hold `/readyz` back indefinitely.
//!
//! Every failure degrades to "warm less than planned," never to a startup
//! failure: tenant discovery erroring, a resolve erroring, a fetch erroring,
//! and the deadline itself are all handled identically, by logging and
//! moving on. This pass is an optimization, never a correctness or
//! availability dependency; a node with a cold cache answers every query
//! correctly and only more slowly (ADR-0046 consequences).
//!
//! Warming calls exactly the funnels the real query paths already call --
//! [`SegmentFetcher::fetch_series`] with no matchers (footer + catalog
//! sections only, no page data: see its own doc, "labels only, no
//! samples") and [`LogSegmentFetcher::fetch_accounted_with_tenant`] (RLOG's
//! only funnel, always a whole-object fetch) -- so warming a part costs
//! exactly what a real query resolving that part would cost, and nothing
//! here reimplements or changes either fetcher's cache behavior.

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, SegmentRef};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_query::{CacheFetchError, LogQuery, LogSegmentFetcher, SegmentFetcher};
use ravel_types::{Signal, TenantHash, TimeRange};

/// Per (tenant, signal), the number of most-recent parts warmed.
const MAX_PARTS_PER_TENANT_PER_SIGNAL: usize = 4;

/// Overall wall-clock budget for the whole pass, across every tenant and
/// signal. A single deadline over the whole loop, not a per-fetch one: a
/// store with many tenants must not warm zero of them just because the
/// first several were slow.
const WARM_DEADLINE: Duration = Duration::from_secs(10);

/// How far back "most recent" looks when resolving parts to warm. Wide
/// enough to find a recent part for a slow ingest cadence without resolving
/// a tenant's entire history.
const WARM_LOOKBACK_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// Discovers every tenant storage holds data for and warms the read cache
/// with each one's most recent metric and log parts. Returns nothing: this
/// is a best-effort side effect, called for its effect on `cache`, not for
/// any result a caller needs to branch on. `cache` must already be the
/// instance attached to the real query fetchers (`with_cache`); a cache
/// warmed here and never consulted elsewhere warms nothing that matters.
pub async fn warm_cache(
    store: Arc<dyn ObjectStoreBackend>,
    catalog: Arc<Catalog>,
    cache: Arc<ravel_cache::Cache<CacheFetchError>>,
    clock: &dyn Clock,
) {
    let now_ns = clock.now_ns();
    match tokio::time::timeout(
        WARM_DEADLINE,
        warm_cache_inner(store, catalog, cache, now_ns),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            tracing::warn!(
                deadline_secs = WARM_DEADLINE.as_secs(),
                "cache warmup did not finish before its deadline; continuing to readiness with \
                 whatever it warmed so far"
            );
        }
    }
}

async fn warm_cache_inner(
    store: Arc<dyn ObjectStoreBackend>,
    catalog: Arc<Catalog>,
    cache: Arc<ravel_cache::Cache<CacheFetchError>>,
    now_ns: i64,
) {
    let tenants = match ravel_maintain::discover_tenants(store.as_ref()).await {
        Ok(tenants) => tenants,
        Err(err) => {
            tracing::warn!(error = %err, "cache warmup: tenant discovery failed; skipping warmup");
            return;
        }
    };

    let range = TimeRange {
        start_ns: now_ns.saturating_sub(WARM_LOOKBACK_NS),
        end_ns: now_ns,
    };

    let metrics_fetcher = SegmentFetcher::new(store.clone()).with_cache(cache.clone());
    let logs_fetcher = LogSegmentFetcher::new(store).with_cache(cache);

    for tenant in tenants {
        warm_metrics(&catalog, &metrics_fetcher, tenant, range, now_ns).await;
        warm_logs(&catalog, &logs_fetcher, tenant, range, now_ns).await;
    }
}

/// Most-recent-first parts for one (tenant, signal), bounded to
/// [`MAX_PARTS_PER_TENANT_PER_SIGNAL`]. A resolve error warms nothing for
/// this (tenant, signal) and is otherwise silent: the same no-op-on-failure
/// rule as every other step of this pass.
async fn most_recent_parts(
    catalog: &Catalog,
    tenant: TenantHash,
    signal: Signal,
    range: TimeRange,
    now_ns: i64,
) -> Vec<SegmentRef> {
    let snapshot = match catalog.resolve(&tenant, signal, range, &[], now_ns).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::debug!(
                tenant_hash = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "cache warmup: resolve failed; skipping this tenant and signal"
            );
            return Vec::new();
        }
    };
    let mut segments = snapshot.segments;
    segments.sort_unstable_by_key(|s| std::cmp::Reverse(s.max_event_ts_ns));
    segments.truncate(MAX_PARTS_PER_TENANT_PER_SIGNAL);
    segments
}

async fn warm_metrics(
    catalog: &Catalog,
    fetcher: &SegmentFetcher,
    tenant: TenantHash,
    range: TimeRange,
    now_ns: i64,
) {
    let parts = most_recent_parts(catalog, tenant, Signal::Metrics, range, now_ns).await;
    for part in &parts {
        if let Err(err) = fetcher.fetch_series(tenant, part, &[]).await {
            tracing::debug!(
                tenant_hash = %tenant.to_hex(),
                key = %part.data_object_key,
                error = %err,
                "cache warmup: metric part fetch failed; skipping"
            );
        }
    }
}

async fn warm_logs(
    catalog: &Catalog,
    fetcher: &LogSegmentFetcher,
    tenant: TenantHash,
    range: TimeRange,
    now_ns: i64,
) {
    let parts = most_recent_parts(catalog, tenant, Signal::Logs, range, now_ns).await;
    for part in &parts {
        let query = LogQuery::new(part.min_event_ts_ns, part.max_event_ts_ns);
        if let Err(err) = fetcher
            .fetch_accounted_with_tenant(
                part,
                tenant,
                &query,
                &ravel_types::accounting::QueryAccounting::new(),
            )
            .await
        {
            tracing::debug!(
                tenant_hash = %tenant.to_hex(),
                key = %part.data_object_key,
                error = %err,
                "cache warmup: log part fetch failed; skipping"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use bytes::Bytes;
    use ravel_cache::{Cache, CacheLimits};
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, PageToken,
        PutOptions, PutOutcome, StoreError,
    };
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;

    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

    fn now_ns() -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch");
        let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
        (ns / NS_PER_SEC) * NS_PER_SEC
    }

    async fn publish_metric_segment(store: &MemoryStore, tenant_hash: TenantHash, now: i64) {
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "http_requests_total".to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(&TenantId::new("acme"), "http_requests_total", &label_set)
                .expect("series id"),
            labels: label_set,
            samples: vec![Sample {
                ts_ns: now - NS_PER_MIN,
                value: 42.0,
            }],
        }];

        let writer_id = Uuid::new_v4();
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

        let rec = record::build(NewCommitRecord {
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
            created_unix_ns: now,
            ingest_hour_bucket: u32::try_from(now / NS_PER_HOUR).expect("hour bucket"),
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    struct FixedClock(i64);

    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn catalog_for(store: Arc<dyn ObjectStoreBackend>) -> Arc<Catalog> {
        crate::query::build_catalog(store, 1, false, ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES)
            .expect("catalog")
    }

    #[tokio::test]
    async fn warms_a_published_tenants_most_recent_metric_part() {
        let memory = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let now = now_ns();
        publish_metric_segment(&memory, tenant, now).await;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));
        assert!(cache.is_empty(), "sanity: nothing fetched yet");

        warm_cache(store, catalog, cache.clone(), &FixedClock(now)).await;

        assert!(
            !cache.is_empty(),
            "warming a tenant with a real recent metric part must populate the cache"
        );
    }

    #[tokio::test]
    async fn empty_store_warms_nothing_and_does_not_error() {
        let memory = MemoryStore::new();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        warm_cache(store, catalog, cache.clone(), &FixedClock(now_ns())).await;

        assert!(cache.is_empty(), "no tenants means nothing to warm");
    }

    /// `discover_tenants` calls `list_delimited`; a fault there must degrade
    /// this whole pass to a no-op, not panic or propagate.
    #[tokio::test]
    async fn tenant_discovery_failure_is_a_silent_no_op() {
        let inner = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        publish_metric_segment(&inner, tenant, now_ns()).await;

        let plan = FaultPlan::empty().with_rule(Rule::new(Op::List, ScriptedFault::Timeout));
        let fault_store = Arc::new(FaultStore::new(inner, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        warm_cache(store.clone(), catalog, cache.clone(), &FixedClock(now_ns())).await;

        assert!(
            cache.is_empty(),
            "a discovery failure must warm nothing, not partially warm or panic"
        );
        assert_eq!(
            fault_store.fault_count(Op::List, FaultKind::Timeout),
            1,
            "the discovery failure must actually come from the injected fault, not some other cause"
        );
    }

    /// A never-resolving store: `list_delimited` and `get` both return a
    /// future that never completes. `FaultStore`'s `Timeout` fault fails
    /// instantly and cannot simulate a genuinely slow backend, so this pass's
    /// deadline can only be exercised with a hand-rolled double like this
    /// one, combined with `tokio::time::pause`/`advance` so the test does
    /// not actually wait ten seconds of real time.
    struct HangingStore;

    #[async_trait]
    impl ObjectStoreBackend for HangingStore {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            std::future::pending().await
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            std::future::pending().await
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            std::future::pending().await
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            std::future::pending().await
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            std::future::pending().await
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            std::future::pending().await
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn warm_pass_gives_up_at_its_deadline_instead_of_hanging_forever() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(HangingStore);
        let catalog = catalog_for(store.clone());
        let cache = Arc::new(Cache::new(CacheLimits::new(
            16 * 1024 * 1024,
            1024,
            1024 * 1024,
        )));

        let warm = tokio::spawn(async move {
            warm_cache(store, catalog, cache, &FixedClock(now_ns())).await;
        });

        // No manual `advance()`: the child's own `WARM_DEADLINE` sleep is only
        // constructed once it is first polled, so advancing before that would
        // race against a deadline measured from a later baseline. Paused-clock
        // auto-advance (both tasks below are blocked purely on timers) jumps
        // straight to the child's deadline once we await it; the outer guard
        // is comfortably larger than `WARM_DEADLINE` so it can only fire if
        // the child genuinely never returns.
        tokio::time::timeout(WARM_DEADLINE * 3, warm)
            .await
            .expect("warm_cache must return once its internal deadline elapses, not hang forever")
            .expect("warm_cache task must not panic");
    }
}
