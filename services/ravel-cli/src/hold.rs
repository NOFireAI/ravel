//! `ravel-cli hold` subcommands (ADR-0048 decision 2, issue #505): the only
//! production mechanism to place, clear, and list legal holds. ADR-0048
//! rejected an HTTP admin API for this: no authenticated admin plane exists,
//! and letting tenant credentials place their own holds would invert the
//! custody model. Set and clear write the existing immutable ADR-0040 audit
//! records through `ravel_maintain::write_hold_set` / `write_hold_clear`;
//! list is `LegalHoldCheck::refresh` plus printing `active_prefixes()`.

use std::sync::Arc;

use ravel_maintain::{LegalHoldCheck, shard_hold_scopes, write_hold_clear, write_hold_set};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{TenantHash, TenantId};
use uuid::Uuid;

use crate::maintain::SignalArg;

/// Resolve `--scope` or the `--signal`/`--shard` sugar to the scope(s) a
/// set/clear invocation writes, before anything is written. Exactly one form
/// must be given: `--scope` alone, or `--signal` with `--shard` together (the
/// sugar, which expands to all three `shard_hold_scopes` prefixes so a hold
/// placed through it covers what an operator believes it covers).
fn resolve_scopes(
    tenant_hash: &TenantHash,
    scope: Option<String>,
    signal: Option<SignalArg>,
    shard: Option<u32>,
) -> anyhow::Result<Vec<String>> {
    match (scope, signal, shard) {
        (Some(scope), None, None) => Ok(vec![scope]),
        (None, Some(signal), Some(shard)) => {
            let scopes = shard_hold_scopes(tenant_hash, signal.to_signal(), shard)
                .map_err(|err| anyhow::anyhow!("failed to build shard hold scopes: {err}"))?;
            Ok(scopes.to_vec())
        }
        (None, None, None) => Err(anyhow::anyhow!(
            "hold requires either --scope, or --signal together with --shard"
        )),
        _ => Err(anyhow::anyhow!(
            "hold takes either --scope alone or --signal with --shard, never a mix of the two forms"
        )),
    }
}

/// Reject a scope outside the named tenant's own `t/<tenant_hex>/` prefix: a
/// hold that can name another tenant's data is a cross-tenant write.
fn validate_scope(tenant_hash: &TenantHash, scope: &str) -> anyhow::Result<()> {
    let tenant_prefix = format!("t/{}/", tenant_hash.to_hex());
    if !scope.starts_with(&tenant_prefix) {
        anyhow::bail!(
            "scope {scope} is outside tenant prefix {tenant_prefix}; a hold cannot name another \
             tenant's data"
        );
    }
    Ok(())
}

/// `hold set`: place a legal hold on one or more scopes, each as a fresh
/// immutable ADR-0040 `set` record.
pub async fn set(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    scope: Option<String>,
    signal: Option<SignalArg>,
    shard: Option<u32>,
    reason: &str,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let scopes = resolve_scopes(&tenant_hash, scope, signal, shard)?;
    for scope in &scopes {
        validate_scope(&tenant_hash, scope)?;
    }
    let now_ns = crate::now_ns()?;
    for scope in &scopes {
        write_hold_set(
            store.as_ref(),
            &tenant_hash,
            Uuid::new_v4(),
            now_ns,
            scope,
            reason,
        )
        .await
        .map_err(|err| anyhow::anyhow!("failed to write hold set for {scope}: {err}"))?;
        println!("hold set: {scope}");
    }
    Ok(())
}

/// `hold clear`: release a legal hold on one or more scopes, each as a fresh
/// immutable ADR-0040 `clear` record.
pub async fn clear(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    scope: Option<String>,
    signal: Option<SignalArg>,
    shard: Option<u32>,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let scopes = resolve_scopes(&tenant_hash, scope, signal, shard)?;
    for scope in &scopes {
        validate_scope(&tenant_hash, scope)?;
    }
    let now_ns = crate::now_ns()?;
    for scope in &scopes {
        write_hold_clear(store.as_ref(), &tenant_hash, Uuid::new_v4(), now_ns, scope)
            .await
            .map_err(|err| anyhow::anyhow!("failed to write hold clear for {scope}: {err}"))?;
        println!("hold clear: {scope}");
    }
    Ok(())
}

/// `hold list`: the tenant's currently active held prefixes, derived by the
/// same fold-to-latest-record `LegalHoldCheck::refresh` the maintenance
/// drivers use.
pub async fn list(store: Arc<dyn ObjectStoreBackend>, tenant: &str) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
        .await
        .map_err(|err| anyhow::anyhow!("failed to refresh legal holds: {err}"))?;
    if snapshot.is_empty() {
        println!("no active holds for tenant {tenant}");
        return Ok(());
    }
    for scope in snapshot.active_prefixes() {
        println!("{scope}");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::Signal;

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    #[tokio::test]
    async fn set_then_list_round_trip() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let scope = format!("t/{}/m/l0/0000/", tenant_hash.to_hex());

        set(
            store.clone(),
            tenant,
            Some(scope.clone()),
            None,
            None,
            "litigation hold",
        )
        .await
        .expect("hold set succeeds");

        // Exercise the CLI print path too, not just the underlying refresh.
        list(store.clone(), tenant).await.expect("hold list runs");

        let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
            .await
            .expect("refresh succeeds");
        assert_eq!(snapshot.active_prefixes(), [scope]);
    }

    #[tokio::test]
    async fn set_rejects_scope_outside_tenant_prefix() {
        let store = store();
        let err = set(
            store,
            "acme",
            Some("t/deadbeefdeadbeefdeadbeefdeadbeef/m/l0/0000/".to_string()),
            None,
            None,
            "",
        )
        .await
        .expect_err("a scope outside the tenant's own prefix must be rejected");
        assert!(
            err.to_string().contains("outside tenant prefix"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn signal_shard_sugar_writes_all_three_prefixes() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();

        set(
            store.clone(),
            tenant,
            None,
            Some(SignalArg::Metrics),
            Some(7),
            "shard hold",
        )
        .await
        .expect("sugar hold set succeeds");

        let mut expected = shard_hold_scopes(&tenant_hash, Signal::Metrics, 7)
            .expect("shard scopes")
            .to_vec();
        expected.sort();

        let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
            .await
            .expect("refresh succeeds");
        let mut active = snapshot.active_prefixes().to_vec();
        active.sort();
        assert_eq!(active, expected);
    }

    #[tokio::test]
    async fn set_requires_exactly_one_of_scope_or_sugar() {
        let store = store();
        let err = set(store.clone(), "acme", None, None, None, "")
            .await
            .expect_err("neither --scope nor the sugar given must be rejected");
        assert!(err.to_string().contains("requires either"));

        let err = set(
            store,
            "acme",
            Some("t/x/".to_string()),
            Some(SignalArg::Metrics),
            Some(0),
            "",
        )
        .await
        .expect_err("mixing --scope with the sugar must be rejected");
        assert!(err.to_string().contains("never a mix"));
    }

    #[tokio::test]
    async fn clear_releases_a_prior_set() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let scope = format!("t/{}/m/l0/0000/", tenant_hash.to_hex());

        set(store.clone(), tenant, Some(scope.clone()), None, None, "")
            .await
            .expect("hold set succeeds");
        clear(store.clone(), tenant, Some(scope.clone()), None, None)
            .await
            .expect("hold clear succeeds");

        let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
            .await
            .expect("refresh succeeds");
        assert!(snapshot.is_empty(), "a cleared scope must not stay active");
    }
}
