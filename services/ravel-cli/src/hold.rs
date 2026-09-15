//! `ravel-cli hold` subcommands (ADR-0048 decision 2): the only
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

/// The three sibling directory segments the prefixes `shard_hold_scopes`
/// returns for one `(tenant, signal, shard)` live under: L0 data objects,
/// commit records, and L1 compacted parts. Pinned against `shard_hold_scopes`
/// itself by `the_three_shard_scopes_live_under_these_dirs`, so a change to
/// that layout fails a test here rather than silently narrowing this rule.
const SHARD_SCOPE_DIRS: [&str; 3] = ["l0", "c", "l1"];

/// Refuse a `--scope` that reaches into a shard's key space without covering
/// all of it.
///
/// One `(tenant, signal, shard)` spans three sibling prefixes, and the sweeps
/// check each independently, so a hold over one or two of them protects some of
/// a bucket's objects and not the rest. That is the partial hold this refuses.
/// Since issue #1697 the retention sweep treats such a hold as blocking the
/// whole bucket rather than deleting around it, so a partial hold no longer
/// destroys data, but it still parks every bucket in the shard in
/// `SweptPartial` indefinitely while reading, on `hold list`, exactly like a
/// hold that covers the shard. Refusing it at the point it is written is the
/// only place an operator finds out.
///
/// What is accepted is anything that covers all three, and anything that
/// touches none of them:
///
/// - `t/<hex>/` and `t/<hex>/<signal>/` cover all three prefixes of every shard
///   they span. They are strictly broader than the shard form, never narrower,
///   so accepting them cannot produce a partial hold.
/// - a scope under a sibling directory that is none of the three
///   (`t/<hex>/<signal>/maint/`, `.../del/`) protects nothing a shard sweep
///   deletes, so it is not a partial shard hold either.
///
/// A scope that stops mid-segment is judged by what it can reach: `.../l` is a
/// prefix of both `l0/` and `l1/` and of neither `c/`, so it is a partial hold
/// and is refused.
///
/// This applies only to an operator-supplied `--scope`. The `--signal`/`--shard`
/// sugar writes the three prefixes as a set, and each of them on its own is
/// exactly the shape refused here, so running them through this check would
/// refuse the one form that is guaranteed complete.
fn reject_partial_shard_scope(tenant_hash: &TenantHash, scope: &str) -> anyhow::Result<()> {
    let tenant_prefix = format!("t/{}/", tenant_hash.to_hex());
    // Not this tenant's keyspace at all: `validate_scope` owns that refusal.
    let Some(rest) = scope.strip_prefix(&tenant_prefix) else {
        return Ok(());
    };
    // Tenant-wide: covers every prefix of every shard.
    if rest.is_empty() {
        return Ok(());
    }
    // Stops at or inside the signal segment: covers all three dirs of every
    // signal it matches.
    let Some((_signal, tail)) = rest.split_once('/') else {
        return Ok(());
    };
    if tail.is_empty() {
        return Ok(());
    }
    let reaches_a_shard_dir = match tail.split_once('/') {
        // A complete directory segment: partial exactly when it is one of the
        // three.
        Some((dir, _)) => SHARD_SCOPE_DIRS.contains(&dir),
        // A partial directory segment: partial when it can reach any of the
        // three. None of the three is a prefix of another, so a non-empty
        // partial segment can never reach all three.
        None => SHARD_SCOPE_DIRS.iter().any(|dir| dir.starts_with(tail)),
    };
    if reaches_a_shard_dir {
        anyhow::bail!(
            "scope {scope} covers part of a shard but not all of it: L0 data objects, commit \
             records and L1 parts live under three sibling prefixes and each is swept \
             independently, so this hold would leave the rest of the shard unprotected. Use \
             --signal with --shard to hold one whole shard, or a broader --scope such as \
             t/<tenant_hex>/ or t/<tenant_hex>/<signal>/"
        );
    }
    Ok(())
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
    // Only the operator-supplied form is checked for a partial shard: the sugar
    // writes the three prefixes as a set, and each one alone is the shape the
    // check refuses.
    let operator_supplied_scope = scope.is_some();
    let scopes = resolve_scopes(&tenant_hash, scope, signal, shard)?;
    for scope in &scopes {
        validate_scope(&tenant_hash, scope)?;
        if operator_supplied_scope {
            reject_partial_shard_scope(&tenant_hash, scope)?;
        }
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
///
/// A clear is deliberately NOT checked against `reject_partial_shard_scope`.
/// The fold matches a clear to a set by the exact scope string, so a partial
/// scope written before that check existed, or by a direct library call, can
/// only be released by a clear naming that same partial scope. Refusing it here
/// would leave such a hold with no way to release it, and every bucket it
/// covers parked forever. Refusing to create a partial hold is the safe
/// direction; refusing to release one is not.
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

    /// The round trip runs through the `--signal`/`--shard` sugar: the scope
    /// this used to pass by hand, `t/<hex>/m/l0/0000/`, is exactly the partial
    /// shard hold `set` now refuses, and the sugar is the supported way to ask
    /// for what it was asking for.
    #[tokio::test]
    async fn set_then_list_round_trip() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();

        set(
            store.clone(),
            tenant,
            None,
            Some(SignalArg::Metrics),
            Some(0),
            "litigation hold",
        )
        .await
        .expect("hold set succeeds");

        // Exercise the CLI print path too, not just the underlying refresh.
        list(store.clone(), tenant).await.expect("hold list runs");

        let mut expected = shard_hold_scopes(&tenant_hash, Signal::Metrics, 0)
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

    /// The three prefixes `shard_hold_scopes` returns really do live under the
    /// three directory segments `SHARD_SCOPE_DIRS` names, in that order. If the
    /// key layout moves, this fails rather than letting the refusal below
    /// quietly stop matching anything.
    #[test]
    fn the_three_shard_scopes_live_under_these_dirs() {
        let tenant_hash = TenantId::new("acme").hash();
        let scopes = shard_hold_scopes(&tenant_hash, Signal::Metrics, 7).expect("shard scopes");
        let prefix = format!("t/{}/m/", tenant_hash.to_hex());
        let dirs: Vec<String> = scopes
            .iter()
            .map(|scope| {
                let rest = scope
                    .strip_prefix(&prefix)
                    .expect("scope under the tenant/signal prefix");
                rest.split('/')
                    .next()
                    .expect("a directory segment")
                    .to_string()
            })
            .collect();
        assert_eq!(dirs, SHARD_SCOPE_DIRS);
    }

    /// A `--scope` reaching into one of a shard's three prefixes without
    /// covering all three is refused before anything is written: it would
    /// protect part of every bucket in the shard and leave the rest sweepable,
    /// and after issue #1697 it parks those buckets indefinitely instead.
    #[tokio::test]
    async fn set_rejects_a_scope_covering_part_of_a_shard() {
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let hex = tenant_hash.to_hex();
        for scope in [
            // Each of the three prefixes on its own, the shape shard_hold_scopes
            // exists to stop a caller assembling by hand.
            format!("t/{hex}/m/l0/0000/"),
            format!("t/{hex}/m/c/0000/"),
            format!("t/{hex}/m/l1/0000/"),
            // Narrower still: one hour of one shard's commit records.
            format!("t/{hex}/m/c/0000/20260621T00/"),
            // A whole directory across every shard: still only one of three.
            format!("t/{hex}/m/l0/"),
            // Stops mid-segment: reaches l0/ and l1/, never c/.
            format!("t/{hex}/m/l"),
        ] {
            let store = store();
            let err = set(store.clone(), tenant, Some(scope.clone()), None, None, "")
                .await
                .expect_err("a partial shard scope must be refused");
            assert!(
                err.to_string().contains("covers part of a shard"),
                "unexpected error for {scope}: {err}"
            );

            let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
                .await
                .expect("refresh succeeds");
            assert!(
                snapshot.is_empty(),
                "a refused scope must leave no hold record behind: {scope}"
            );
        }
    }

    /// The scopes that stay accepted: broader than a shard (so they cover all
    /// three of its prefixes) or outside the three entirely (so they are not a
    /// shard hold at all).
    #[tokio::test]
    async fn set_accepts_scopes_broader_than_a_shard_and_scopes_outside_it() {
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let hex = tenant_hash.to_hex();
        for scope in [
            format!("t/{hex}/"),
            format!("t/{hex}/m/"),
            format!("t/{hex}/m/maint/"),
        ] {
            let store = store();
            set(store.clone(), tenant, Some(scope.clone()), None, None, "")
                .await
                .unwrap_or_else(|err| panic!("{scope} must stay accepted: {err}"));

            let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
                .await
                .expect("refresh succeeds");
            assert_eq!(snapshot.active_prefixes(), [scope]);
        }
    }

    /// A clear is not subject to the partial-shard refusal: a hold written
    /// before that rule existed can only be released by a clear naming the same
    /// partial scope.
    #[tokio::test]
    async fn clear_accepts_a_partial_shard_scope_a_set_would_refuse() {
        let store = store();
        let tenant = "acme";
        let tenant_hash = TenantId::new(tenant).hash();
        let scope = format!("t/{}/m/l0/0000/", tenant_hash.to_hex());

        // Written the way a pre-#1697 CLI would have, bypassing the CLI's set.
        write_hold_set(
            store.as_ref(),
            &tenant_hash,
            Uuid::new_v4(),
            1_000,
            &scope,
            "old partial hold",
        )
        .await
        .expect("seed a partial hold");

        clear(store.clone(), tenant, Some(scope.clone()), None, None)
            .await
            .expect("clearing a partial hold must stay possible");

        let snapshot = LegalHoldCheck::refresh(store.as_ref(), &tenant_hash)
            .await
            .expect("refresh succeeds");
        assert!(snapshot.is_empty(), "the partial hold was released");
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
        // Signal-wide: broader than a shard, so `set` accepts it.
        let scope = format!("t/{}/m/", tenant_hash.to_hex());

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
