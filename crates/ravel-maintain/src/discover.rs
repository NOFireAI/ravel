//! Storage-derived tenant discovery (ADR-0048 decision 3, issue #504).
//!
//! The maintenance and fold background tasks used to learn their tenant set
//! only from CLI flags (`--tenant-token`, `--maintain-tenant`), so a
//! deployment authenticating tenants through OIDC or mTLS -- which never
//! populates either flag -- silently ran neither task for any of them
//! (findings S2-17, S5-09). Storage already knows every tenant with data: one
//! delimited LIST of the `t/` prefix enumerates it directly, with no second
//! source of truth to drift from what is actually durable (ADR-0048 rejected
//! alternative 2).

use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;

use crate::error::{MaintainError, Result};

/// Enumerate every tenant storage holds data for: one `list_delimited("t/")`
/// call, parsing each returned common prefix `t/<32 hex>/` into a
/// [`TenantHash`]. A prefix that does not parse as exactly 32 lowercase-or-any
/// hex characters is a fail-loud [`MaintainError::InvalidTenantPrefix`], never
/// a silent skip: the same key-shape discipline every other object key in
/// this crate is held to (docs/catalog-and-mvcc.md key layout).
pub async fn discover_tenants(store: &dyn ObjectStoreBackend) -> Result<Vec<TenantHash>> {
    let listing = store.list_delimited("t/").await?;
    let mut tenants = Vec::with_capacity(listing.common_prefixes.len());
    for prefix in &listing.common_prefixes {
        let hex = prefix
            .strip_prefix("t/")
            .and_then(|rest| rest.strip_suffix('/'))
            .filter(|hex| hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit()));
        let Some(hex) = hex else {
            return Err(MaintainError::InvalidTenantPrefix(prefix.clone()));
        };
        let tenant = TenantHash::from_hex(hex)
            .map_err(|_| MaintainError::InvalidTenantPrefix(prefix.clone()))?;
        tenants.push(tenant);
    }
    Ok(tenants)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    use super::*;

    async fn put(store: &MemoryStore, key: &str) {
        store
            .put(key, Bytes::from_static(b"x"), PutOptions::default())
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn empty_store_discovers_no_tenants() {
        let store = MemoryStore::new();
        let tenants = discover_tenants(&store).await.expect("discover");
        assert!(tenants.is_empty());
    }

    #[tokio::test]
    async fn discovers_every_distinct_tenant_prefix() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        let globex = TenantId::new("globex").hash();
        put(&store, &format!("t/{}/catalog/m/HEAD", acme.to_hex())).await;
        put(
            &store,
            &format!("t/{}/m/l0/0/writer.1.1.abcd.rseg", acme.to_hex()),
        )
        .await;
        put(&store, &format!("t/{}/catalog/m/HEAD", globex.to_hex())).await;

        let mut tenants = discover_tenants(&store).await.expect("discover");
        tenants.sort_by_key(|t| t.to_hex());
        let mut expected = vec![acme, globex];
        expected.sort_by_key(|t| t.to_hex());
        assert_eq!(tenants, expected);
    }

    #[tokio::test]
    async fn non_hex_prefix_under_t_is_a_fail_loud_error() {
        let store = MemoryStore::new();
        put(&store, "t/not-a-valid-tenant-hash/whatever").await;

        let err = discover_tenants(&store).await.expect_err("must fail loud");
        assert!(matches!(err, MaintainError::InvalidTenantPrefix(_)));
    }

    #[tokio::test]
    async fn short_prefix_under_t_is_a_fail_loud_error() {
        let store = MemoryStore::new();
        put(&store, "t/abcd/whatever").await;

        let err = discover_tenants(&store).await.expect_err("must fail loud");
        assert!(matches!(err, MaintainError::InvalidTenantPrefix(_)));
    }
}
