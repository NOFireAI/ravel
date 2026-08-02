//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`,
//! and, behind the `sql` feature, the `SqlState` for `/api/v1/sql`.

use std::sync::Arc;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{EngineConfig, QueryEngine};

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
    let catalog = Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?;
    Ok(Arc::new(catalog))
}

pub fn build_app_state(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> AppState {
    let engine = QueryEngine::new(catalog, store, EngineConfig::default());
    AppState {
        engine: Arc::new(engine),
        tenant_resolver,
    }
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
/// The SQL path gets its own `Catalog` instance rather than sharing the
/// PromQL engine's: `QueryEngine` owns its catalog privately and exposes no
/// accessor, and a second listing catalog over the same store is
/// behaviourally identical (its only mutable state is a decoded-record
/// cache). Sharing one would be a small allocation win and no more.
#[cfg(feature = "sql")]
pub fn build_sql_state(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> anyhow::Result<crate::sql::SqlState> {
    use ravel_query::{LogSegmentFetcher, SegmentFetcher};
    use ravel_sql::{SqlConfig, SqlExecutor};

    let catalog = build_catalog(store.clone(), shard_count)?;
    let config = SqlConfig::default();
    let max_deadline = config.engine.deadline;
    // The metrics fetcher (RSEG) and the logs fetcher (RLOG) both read the
    // same object store; the executor uses whichever the query's target table
    // needs (ADR-0033).
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
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
    })
}
