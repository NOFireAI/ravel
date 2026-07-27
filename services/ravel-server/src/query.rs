//! Builds the `ravel-query` `AppState` (catalog + engine) mounted at `/api/v1/*`.

use std::sync::Arc;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AppState, TenantResolver};
use ravel_query::{EngineConfig, QueryEngine};

pub fn build_app_state(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> anyhow::Result<AppState> {
    let catalog_config = CatalogConfig {
        shard_count,
        ..CatalogConfig::default()
    };
    let catalog = Catalog::new(store.clone(), catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?;
    let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());
    Ok(AppState {
        engine: Arc::new(engine),
        tenant_resolver,
    })
}
