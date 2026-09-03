//! Resolving a tenant's effective retention window from the same two sources
//! the catalog fold uses (ADR-0078), so the retention sweep and the fold never
//! disagree on which hours are expired.
//!
//! The fold overlays the durable per-tenant `TenantConfig.retention_ns` on the
//! deployment default it is handed (crates/ravel-catalog/src/fold.rs). The
//! sweep used to read only the CLI-derived [`RetentionConfig`], so a tenant
//! whose durable window was longer than the CLI window had its buckets
//! tombstoned by the sweep while the fold's frontier reconcile kept naming
//! them, and the physical delete stalled on the HEAD-reachability gate until
//! the hour aged past the CLI window. [`resolve_retention_window`] closes that
//! gap by resolving the sweep's window through the same durable record with the
//! same precedence the fold applies.

use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;

use crate::config::RetentionConfig;
use crate::error::{MaintainError, Result};

/// The retention window that applies to one tenant, resolved exactly as
/// `Catalog::fold` resolves it (ADR-0078): the durable
/// `TenantConfig.retention_ns` override when the record exists and carries
/// `Some`, otherwise the deployment default from the CLI-derived
/// [`RetentionConfig`] (the per-tenant `--retention-tenant` override, else the
/// `--retention-default` default, else `None` for no retention). The durable
/// record therefore wins over both CLI sources when it is present; the CLI
/// per-tenant override only applies when no durable record carries a window.
///
/// The durable record is read from the same store, at the same key, with the
/// same decode the fold uses ([`ravel_catalog::read_config_values`]). An absent
/// record (`Ok(None)`) is the no-override case and falls through to the
/// deployment default.
///
/// Fails CLOSED on a store or decode fault, unlike the fold, which falls back
/// to the deployment default because its index role is a pure optimization.
/// The sweep drives an irreversible tombstone, so a transient read fault must
/// not silently fall back to a shorter CLI window and tombstone an hour the
/// fold would keep. The caller aborts the pass and retries on the next tick.
pub async fn resolve_retention_window(
    store: &dyn ObjectStoreBackend,
    retention: &RetentionConfig,
    tenant: &TenantHash,
) -> Result<Option<i64>> {
    let durable = ravel_catalog::read_config_values(store, tenant)
        .await
        .map_err(|e| {
            MaintainError::Invariant(format!(
                "tenant config read failed while resolving the retention window: {e}"
            ))
        })?
        .and_then(|cfg| cfg.retention_ns);
    Ok(durable.or_else(|| retention.window_for(tenant)))
}
