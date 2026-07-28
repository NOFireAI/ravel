//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`.

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
