//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`,
//! and, behind the `sql` feature, the `SqlState` for `/api/v1/sql`.

use std::sync::Arc;

use ravel_cache::Cache;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{CacheFetchError, EngineConfig, QueryEngine};
use ravel_types::accounting::{AccountedOp, CostEstimate, QueryAccountingSnapshot};

/// The per-query `stats` object attached beside a query response's data
/// (issue #425, ADR-0044 sections 1 and 3): this query's actual accounting
/// counters and its pre-execution cost estimate, rendered as camelCase JSON to
/// match the field names `ravel-query`'s PromQL `stats.accounting`/`stats.estimate`
/// already use (crates/ravel-query/src/http/json.rs).
///
/// It deliberately omits the `rawF64Pages`/`rawF64Bytes` and
/// `segmentsFetched`/`segmentsPruned` fields the PromQL shape carries: those
/// come from `ravel-query`'s internal per-segment `FetchStats`/`QueryStats`,
/// which the SQL executor's `SqlOutcome` and the analytics range call do not
/// surface. The accounting snapshot and the cost estimate are the shape every
/// query path can supply, so both server-owned handlers report exactly that,
/// and the divergence between the estimate and the actual stays computable from
/// the response as well as from `/metrics`.
pub fn accounting_stats_json(
    accounting: &QueryAccountingSnapshot,
    estimate: &CostEstimate,
) -> serde_json::Value {
    serde_json::json!({
        "accounting": {
            "s3GetRequests": accounting.s3_requests(AccountedOp::Get),
            "s3GetBytes": accounting.s3_bytes(AccountedOp::Get),
            "s3ListRequests": accounting.s3_requests(AccountedOp::List),
            "s3ListBytes": accounting.s3_bytes(AccountedOp::List),
            "s3HeadRequests": accounting.s3_requests(AccountedOp::Head),
            "s3HeadBytes": accounting.s3_bytes(AccountedOp::Head),
            "cacheHits": accounting.cache_hits,
            "cacheMisses": accounting.cache_misses,
            "cacheBytes": accounting.cache_bytes,
            "decompressedBytes": accounting.decompressed_bytes,
            "segmentsOpened": accounting.segments_opened,
            "seriesMatched": accounting.series_matched,
            "bytesReused": accounting.bytes_reused,
            "peakIntermediateBytes": accounting.peak_intermediate_bytes,
        },
        "estimate": {
            "estimatedRequests": estimate.estimated_requests,
            "estimatedStoreBytes": estimate.estimated_store_bytes,
            "estimatedDecompressedBytes": estimate.estimated_decompressed_bytes,
            "segments": estimate.segments,
            "series": estimate.series,
        },
    })
}

/// Builds the shared [`Catalog`] used both for query resolve and for the
/// background fold task (docs/metric-index-plan.md section 4): one instance
/// per process so its decoded HEAD/part caches serve both paths.
pub fn build_catalog(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
) -> anyhow::Result<Arc<Catalog>> {
    let catalog_config = CatalogConfig {
        shard_count,
        ..CatalogConfig::default()
    };
    // Durable shard_count enforcement on the read path (ADR-0050 section 5,
    // EC5): the first resolve for each (tenant, signal) validates this
    // catalog's configured shard_count against the tenant's provisioning
    // record, so a query never silently resolves over a subset of shards. The
    // check is read-only (it never writes a record), so a query-only node with
    // write-restricted credentials is unaffected.
    let catalog = Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    Ok(Arc::new(catalog))
}

/// Build the query `AppState`. `engine_config` carries the resolved query
/// deadline (ADR-0050 section 4, EC4): the caller passes the SAME
/// `EngineConfig` whose `deadline` was validated against `sys/gc` in `main`, so
/// the engine that actually enforces the deadline uses the validated value
/// rather than an independent `EngineConfig::default()`.
pub fn build_app_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<Arc<Cache<CacheFetchError>>>,
    engine_config: EngineConfig,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
) -> AppState {
    let mut engine = QueryEngine::new(catalog, store, engine_config);
    if let Some(cache) = cache {
        engine = engine.with_cache(cache);
    }
    // Fold every completed Prometheus-shaped query into the same process
    // aggregator the SQL and analytics paths use (ADR-0044 section 4, issue
    // #425), so `/metrics` covers PromQL read traffic too.
    AppState::new(Arc::new(engine), tenant_resolver).with_cost_recorder(query_accounting)
}

/// Default per-tenant SQL memory ceiling: 1 GiB across a tenant's concurrent
/// queries, four times the per-query default in `ravel_sql::SqlConfig`. A
/// placeholder pending the Phase B measurements
/// docs/arrow-datafusion-plan.md says will set both figures in BENCHMARKS.md;
/// documented as such so it is not mistaken for a tuned value.
#[cfg(feature = "sql")]
pub const DEFAULT_MAX_TENANT_BYTES: usize = 1024 * 1024 * 1024;

/// Build the state for `POST /api/v1/sql`.
///
/// Takes the same `Catalog` instance the PromQL engine and `/metrics` use
/// (ADR-0050 section 2): a second, independent `Catalog` here would carry
/// its own `isolation_breaches` counter, so a tenant_hash or LIST-prefix
/// breach hit only through the SQL path would never reach
/// `ravel_catalog_isolation_breach_total` and the alert rule built on it.
///
/// `engine_config` carries the resolved query deadline (ADR-0050 section 4,
/// EC4), the SAME value passed to [`build_app_state`]: SQL and Flight SQL
/// must enforce the deadline `main` validated against `sys/gc`, not an
/// independent `EngineConfig::default()` (the bug the fix-continuation for
/// issue #588 found: PromQL was wired, SQL/Flight SQL were not).
#[cfg(feature = "sql")]
pub fn build_sql_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
    cache: Option<Arc<Cache<CacheFetchError>>>,
    engine_config: EngineConfig,
    query_accounting: Arc<crate::metrics::QueryAccountingMetrics>,
) -> anyhow::Result<crate::sql::SqlState> {
    use ravel_query::{LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{SqlConfig, SqlExecutor};

    let config = SqlConfig {
        engine: engine_config,
        ..SqlConfig::default()
    };
    let max_deadline = config.engine.deadline;
    let mut metrics_fetcher = SegmentFetcher::new(store.clone());
    let mut logs_fetcher = LogSegmentFetcher::new(store.clone());
    if let Some(cache) = cache {
        metrics_fetcher = metrics_fetcher.with_cache(cache.clone());
        logs_fetcher = logs_fetcher.with_cache(cache);
    }
    // The metrics fetcher (RSEG) and the logs fetcher (RLOG) both read the
    // same object store; the executor uses whichever the query's target table
    // needs (ADR-0033).
    let executor = SqlExecutor::new(
        catalog,
        metrics_fetcher,
        logs_fetcher,
        config,
        DEFAULT_MAX_TENANT_BYTES,
    );
    Ok(crate::sql::SqlState {
        executor: Arc::new(executor),
        tenant_resolver,
        // The audit writer (ADR-0042 decision 4) writes to the same store the
        // executor reads from.
        store,
        clock: Arc::new(ravel_ingest::SystemClock),
        max_deadline,
        query_accounting,
    })
}

#[cfg(all(test, feature = "sql"))]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::http::StaticBearerTokenResolver;
    use std::collections::HashMap;
    use std::time::Duration;

    /// `build_sql_state` must enforce the `EngineConfig` it was given, not an
    /// independent `SqlConfig::default()` -- the gap the ADR-0050 section 4 /
    /// EC4 fix-continuation found: the PromQL path (`build_app_state`) took a
    /// resolved deadline, but SQL/Flight SQL silently kept a hardcoded 30s.
    #[test]
    fn build_sql_state_honors_the_passed_engine_config_deadline() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(store.clone(), 1).expect("catalog");
        let tenant_resolver: Arc<dyn TenantResolver> =
            Arc::new(StaticBearerTokenResolver::new(HashMap::new()));
        let non_default = EngineConfig {
            deadline: Duration::from_secs(10),
            ..EngineConfig::default()
        };
        assert_ne!(
            non_default.deadline,
            EngineConfig::default().deadline,
            "sanity: the test deadline must actually differ from the default"
        );

        let state = build_sql_state(
            catalog,
            store,
            tenant_resolver,
            None,
            non_default,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
        )
        .expect("sql state builds");

        assert_eq!(
            state.max_deadline, non_default.deadline,
            "build_sql_state's max_deadline must be the resolved value passed in, \
             not an independent EngineConfig::default()"
        );
    }
}
