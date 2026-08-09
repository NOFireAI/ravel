//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`,
//! and, behind the `sql` feature, the `SqlState` for `/api/v1/sql`.

use std::sync::Arc;

use ravel_cache::Cache;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{CacheFetchError, EngineConfig, QueryAdmissionController, QueryEngine};
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
///
/// `disable_cache` and `cache_max_bytes` are the CLI's `--disable-cache` and
/// `--cache-max-bytes`, the same flags that govern the fetcher cache in
/// [`crate::store::build_cache`]. They reach the catalog's ADR-0046 byte cache
/// too (issue #553): `--disable-cache` builds a catalog with no byte cache at
/// all (the `byte_cache_max_bytes: 0` sentinel), so a memory-constrained
/// `--disable-cache` deployment no longer silently keeps a 512 MiB catalog
/// byte cache; otherwise `cache_max_bytes` is the catalog byte cache's total
/// budget, sharing one number with the fetcher cache. The other two byte-cache
/// bounds keep their catalog defaults (the CLI has no flag for them).
pub fn build_catalog(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    disable_cache: bool,
    cache_max_bytes: u64,
) -> anyhow::Result<Arc<Catalog>> {
    // `0` is the byte cache's disabled sentinel (ravel_catalog::CatalogConfig):
    // Catalog::new then constructs no byte cache. Mirrors how build_cache turns
    // --disable-cache into a `None` fetcher cache.
    let byte_cache_max_bytes = if disable_cache { 0 } else { cache_max_bytes };
    let catalog_config = CatalogConfig {
        shard_count,
        byte_cache_max_bytes,
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
    query_admission: Arc<QueryAdmissionController>,
) -> AppState {
    let mut engine = QueryEngine::new(catalog, store, engine_config);
    if let Some(cache) = cache {
        engine = engine.with_cache(cache);
    }
    // Fold every completed Prometheus-shaped query into the same process
    // aggregator the SQL and analytics paths use (ADR-0044 section 4, issue
    // #425), so `/metrics` covers PromQL read traffic too. The shared query
    // concurrency controller (ADR-0061 decision 2) gates every handler before
    // it resolves or fetches.
    AppState::new(Arc::new(engine), tenant_resolver)
        .with_cost_recorder(query_accounting)
        .with_query_admission(query_admission)
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
    query_admission: Arc<QueryAdmissionController>,
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
        query_admission,
        // EL-5 routes the SQL HTTP audit through the QueryAuditSink seam;
        // installing the process-wide AuditPipeline is EL-7's server-wiring
        // task, so this defaults to the no-op today. The endpoint already
        // submits and awaits through the trait, so EL-7 is a one-line swap.
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod catalog_cache_tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::ByteLimit;
    use ravel_query::http::StaticBearerTokenResolver;
    use std::collections::HashMap;

    /// ADR-0061 decision 1: the bytes-scanned budget resolved from
    /// `--limits-file` must reach the PromQL/HTTP engine `build_app_state`
    /// builds, not be dropped to the `EngineConfig::default()` `Unlimited`.
    /// Asserts on the engine's own `config()`, the value the fetch fan-outs
    /// actually check.
    #[test]
    fn build_app_state_threads_the_byte_budget_into_the_engine() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        )
        .expect("catalog");
        let engine_config = EngineConfig {
            max_bytes_scanned: ByteLimit::Bounded(4096),
            ..EngineConfig::default()
        };
        let state = build_app_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            engine_config,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
        );
        assert_eq!(
            state.engine.config().max_bytes_scanned,
            ByteLimit::Bounded(4096),
            "the PromQL engine must enforce the configured byte budget, not the default Unlimited"
        );
    }

    /// Issue #553: `--disable-cache` (passed as `disable_cache: true`) must
    /// build a catalog with no byte cache constructed, the byte-cache analogue
    /// of the `None` fetcher cache `build_cache` returns. Asserts on the
    /// absence of the counters handle, not a zero hit count.
    #[test]
    fn build_catalog_disable_cache_constructs_no_byte_cache() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(store, 1, true, ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES)
            .expect("catalog builds");
        assert!(
            catalog.byte_cache_metrics().is_none(),
            "--disable-cache must leave the catalog with no byte cache constructed"
        );
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            0,
            "the disabled catalog config carries the byte-cache disable sentinel"
        );
    }

    /// Issue #553: with caching on, `--cache-max-bytes` must bound the catalog
    /// byte cache, not just the fetcher cache. The value reaches
    /// `CatalogConfig::byte_cache_max_bytes`, and the byte cache (with its
    /// counters handle) is constructed.
    #[test]
    fn build_catalog_wires_cache_max_bytes_through_to_the_byte_cache() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let budget = 7 * 1024 * 1024;
        let catalog = build_catalog(store, 1, false, budget).expect("catalog builds");
        assert_eq!(
            catalog.config().byte_cache_max_bytes,
            budget,
            "--cache-max-bytes must bound the catalog byte cache, not only the fetcher cache"
        );
        assert!(
            catalog.byte_cache_metrics().is_some(),
            "an enabled catalog byte cache must expose its counters handle for /metrics"
        );
    }
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
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        )
        .expect("catalog");
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
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
        )
        .expect("sql state builds");

        assert_eq!(
            state.max_deadline, non_default.deadline,
            "build_sql_state's max_deadline must be the resolved value passed in, \
             not an independent EngineConfig::default()"
        );
    }

    /// ADR-0061 decision 1: the SQL/HTTP surface must enforce the same
    /// bytes-scanned budget the PromQL surface does, so the value threaded into
    /// `build_sql_state`'s `EngineConfig` must survive into the executor's
    /// `SqlConfig.engine` (where `RsegScanExec::prepare_partition` checks it),
    /// not be dropped to `SqlConfig::default()`'s `Unlimited`.
    #[test]
    fn build_sql_state_threads_the_byte_budget_into_the_executor() {
        use ravel_query::ByteLimit;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = build_catalog(
            store.clone(),
            1,
            false,
            ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        )
        .expect("catalog");
        let engine_config = EngineConfig {
            max_bytes_scanned: ByteLimit::Bounded(4096),
            ..EngineConfig::default()
        };
        let state = build_sql_state(
            catalog,
            store,
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            None,
            engine_config,
            Arc::new(crate::metrics::QueryAccountingMetrics::new(
                std::collections::HashSet::new(),
            )),
            QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
        )
        .expect("sql state builds");
        assert_eq!(
            state.executor.config().engine.max_bytes_scanned,
            ByteLimit::Bounded(4096),
            "the SQL executor must enforce the configured byte budget, not the default Unlimited"
        );
    }
}
