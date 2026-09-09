//! Shared storage-derived tenant discovery for the maintenance and fold
//! background tasks (ADR-0048 decision 3).
//!
//! Both tasks used to learn their tenant set only from `--tenant-token` and
//! `--maintain-tenant`, so a deployment authenticating tenants through OIDC
//! or mTLS -- which populates neither flag -- silently ran neither task for
//! any of them (the `--maintain-tenant` startup warning only fires when the
//! merged list is entirely empty, so a
//! stale non-empty list is still silent for a newly onboarded tenant). This
//! module replaces the flag-derived set with [`ravel_maintain::discover_tenants`]
//! and narrows it to an optional flag *restriction* instead.
//!
//! `restrict: None` means no restriction is configured (every discovered
//! tenant is maintained); this is distinct from `Some(&[])`, which would mean
//! "restrict to nothing." Both `--tenant-token` and `--maintain-tenant` are
//! empty by default, so an unconfigured deployment gets `None` here, never an
//! empty restriction set.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_catalog::read_config_values;
use ravel_maintain::MaintainError;
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;

/// One discovery-and-restrict cycle's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryOutcome {
    /// Every tenant prefix storage reports under `t/`, this cycle.
    pub discovered: Vec<TenantHash>,
    /// `discovered`, narrowed to the flag restriction when one is
    /// configured; identical to `discovered` when `restrict` is `None`.
    /// This is the actual working set the caller runs its cycle over.
    pub maintained: Vec<TenantHash>,
    /// Discovered tenants the restriction excluded from `maintained`, so a
    /// deliberate flag scoping is visible rather than looking identical to
    /// "storage reports fewer tenants."
    pub excluded: usize,
}

/// Enumerate every tenant storage holds data for, then narrow to `restrict`
/// (the merged `--tenant-token`/`--maintain-tenant` set) when it names any
/// tenant (ADR-0048 decision 3). A discovery failure (the LIST errors)
/// propagates to the caller rather than being papered over here: the
/// contract this function exists to uphold is that a failure is never
/// indistinguishable from "storage has no tenants."
pub async fn discover_and_restrict(
    store: &dyn ObjectStoreBackend,
    restrict: Option<&[TenantHash]>,
) -> Result<DiscoveryOutcome, MaintainError> {
    let discovered = ravel_maintain::discover_tenants(store).await?;
    let (maintained, excluded) = match restrict {
        None => {
            let maintained = discovered.clone();
            (maintained, 0)
        }
        Some(allow) => {
            let allow: HashSet<TenantHash> = allow.iter().copied().collect();
            let mut maintained = Vec::with_capacity(discovered.len());
            let mut excluded = 0usize;
            for tenant in &discovered {
                if allow.contains(tenant) {
                    maintained.push(*tenant);
                } else {
                    excluded += 1;
                }
            }
            (maintained, excluded)
        }
    };
    Ok(DiscoveryOutcome {
        discovered,
        maintained,
        excluded,
    })
}

/// Lifecycle-aware discovery (ADR-0066 decision 6): the maintained set
/// is re-derived each cycle from the durable per-tenant config records, not
/// frozen at startup from the `--tenant-token`/`--maintain-tenant` union.
///
/// Once a tenant carries a durable `t/<hash>/config` record, that record is the
/// sole authority on whether it is maintained: it is maintained
/// UNCONDITIONALLY, and no CLI flag can exclude it. All three lifecycle states
/// keep maintenance running -- `active` fully, `suspended` refuses only
/// ingest/query, `offboarding` keeps retention running until the data is gone
/// ([`ravel_catalog::TenantLifecycleState`]) -- so a present record always
/// keeps the tenant maintained, regardless of state.
///
/// `fallback_allow` is the merged `--tenant-token`/`--maintain-tenant` set
/// (empty means `None`). It is a fallback source that governs ONLY tenants with
/// no config record yet: such a tenant is maintained when `fallback_allow` is
/// `None` or names it, and excluded (and counted) otherwise. This preserves the
/// flag-restriction behaviour for data that predates per-tenant config records
/// (legacy or
/// OIDC-only tenants). It is explicitly NOT a filter that can override a config
/// record: any flag-derived set that could exclude a config-recorded tenant
/// would reintroduce the removed-token-disables-retention hazard this whole
/// design closes, because such a set necessarily shrinks whenever an operator's
/// static token/`--maintain-tenant` list changes.
///
/// This closes the removed-token-disables-retention hazard named in ADR-0066's
/// Context: a token lives in `sys/auth`, the lifecycle lives in
/// `t/<hash>/config`, and removing a token (or dropping a `--tenant-token` /
/// `--maintain-tenant` entry on restart) never touches the config record, so a
/// deprovisioned-by-token tenant keeps its `active` record and stays maintained
/// (retention enforcement continues) until its data is actually gone. A tenant
/// otherwise only drops out by having its data (and therefore its `t/` prefix)
/// removed -- fully-completed offboarding through the erasure mechanism, not
/// lifecycle refresh.
///
/// A per-tenant config read error is NOT allowed to drop a tenant on its own:
/// that would reintroduce the very hazard (a transient fault silently disabling
/// retention). Such a tenant stays maintained this cycle and the error is
/// logged, the fail-safe direction, matching how a whole-cycle discovery
/// failure is never papered over as "no tenants".
pub async fn discover_and_restrict_by_lifecycle(
    store: &dyn ObjectStoreBackend,
    fallback_allow: Option<&[TenantHash]>,
) -> Result<DiscoveryOutcome, MaintainError> {
    let discovered = ravel_maintain::discover_tenants(store).await?;
    let fallback: Option<HashSet<TenantHash>> = fallback_allow.map(|a| a.iter().copied().collect());
    let mut maintained = Vec::with_capacity(discovered.len());
    let mut excluded = 0usize;
    for tenant in &discovered {
        let keep = match read_config_values(store, tenant).await {
            // A durable config record (any lifecycle state) is the sole
            // authority: the tenant is maintained unconditionally, no flag can
            // exclude it.
            Ok(Some(_config)) => true,
            // No record yet: honour the token-derived fallback (backward compat
            // for data that predates per-tenant config records).
            Ok(None) => fallback.as_ref().is_none_or(|a| a.contains(tenant)),
            // A transient config read fault must never drop a tenant from
            // maintenance on its own: keep it (retention keeps running) and log.
            Err(err) => {
                tracing::warn!(
                    tenant_hash = %tenant.to_hex(),
                    error = %err,
                    "lifecycle discovery: config record read failed; keeping the tenant \
                     maintained this cycle rather than disabling its retention, retried next cycle"
                );
                true
            }
        };
        if keep {
            maintained.push(*tenant);
        } else {
            excluded += 1;
        }
    }
    Ok(DiscoveryOutcome {
        discovered,
        maintained,
        excluded,
    })
}

/// Process-global gauges and failure counter for tenant discovery (ADR-0048
/// decision 3 "What alarms"), rendered on the existing `GET /metrics`
/// endpoint (ADR-0044 section 4) by [`crate::metrics`] -- no second registry.
/// Updated by the maintenance task's discovery-driven supervisor cycle.
#[derive(Debug, Default)]
pub struct TenantDiscoveryMetrics {
    tenants_discovered: AtomicU64,
    tenants_maintained: AtomicU64,
    discovery_failures: AtomicU64,
}

impl TenantDiscoveryMetrics {
    pub fn tenants_discovered(&self) -> u64 {
        self.tenants_discovered.load(Ordering::Relaxed)
    }

    pub fn tenants_maintained(&self) -> u64 {
        self.tenants_maintained.load(Ordering::Relaxed)
    }

    pub fn discovery_failures(&self) -> u64 {
        self.discovery_failures.load(Ordering::Relaxed)
    }

    /// A successful cycle overwrites both gauges with this cycle's counts:
    /// they describe the current state of the world, not an accumulation.
    pub fn record_discovery(&self, discovered: usize, maintained: usize) {
        self.tenants_discovered
            .store(discovered as u64, Ordering::Relaxed);
        self.tenants_maintained
            .store(maintained as u64, Ordering::Relaxed);
    }

    /// A failed cycle leaves both gauges at their last known-good value
    /// (never zeroed). Zeroing them on failure would make a discovery fault
    /// render identically to "storage reports no tenants" on the very
    /// dashboard meant to distinguish the two (ADR-0048 decision 3).
    pub fn record_discovery_failure(&self) {
        self.discovery_failures.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    use super::*;

    #[tokio::test]
    async fn no_restriction_maintains_every_discovered_tenant() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        store
            .put(
                &format!("t/{}/catalog/m/HEAD", acme.to_hex()),
                bytes::Bytes::from_static(b"x"),
                PutOptions::default(),
            )
            .await
            .expect("put");

        let outcome = discover_and_restrict(&store, None).await.expect("ok");
        assert_eq!(outcome.discovered, vec![acme]);
        assert_eq!(outcome.maintained, vec![acme]);
        assert_eq!(outcome.excluded, 0);
    }

    #[tokio::test]
    async fn restriction_excludes_a_discovered_tenant_not_named_by_it() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        let globex = TenantId::new("globex").hash();
        for tenant in [acme, globex] {
            store
                .put(
                    &format!("t/{}/catalog/m/HEAD", tenant.to_hex()),
                    bytes::Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }

        let outcome = discover_and_restrict(&store, Some(&[acme]))
            .await
            .expect("ok");
        let mut discovered = outcome.discovered.clone();
        discovered.sort_by_key(|t| t.to_hex());
        let mut expected = vec![acme, globex];
        expected.sort_by_key(|t| t.to_hex());
        assert_eq!(discovered, expected);
        assert_eq!(outcome.maintained, vec![acme]);
        assert_eq!(outcome.excluded, 1);
    }

    /// THE deliverable-3 regression test (ADR-0066 decision 6): removing a
    /// tenant's token from `sys/auth` must NOT drop it from the maintained set,
    /// so its retention keeps running. This is a genuine before/after: the OLD
    /// startup-frozen restriction (`discover_and_restrict`) DROPS a tenant once
    /// the restriction no longer names it (the bug), while the new
    /// lifecycle-aware restriction (`discover_and_restrict_by_lifecycle`) KEEPS
    /// it because its durable config record is untouched by the token removal.
    #[tokio::test]
    async fn removing_a_token_never_drops_a_tenant_with_a_config_record() {
        use ravel_catalog::{
            TenantConfig, TenantLifecycleState, read_auth_map, remove_token, set_tenant_config,
            tenant_for_token, upsert_token,
        };

        const KEY: &[u8; 32] = &[0x42u8; 32];
        let store = MemoryStore::new();
        let acme_id = TenantId::new("acme");
        let acme = acme_id.hash();

        // acme has data (so it is discovered), a durable config record (active),
        // and a bearer token in sys/auth.
        store
            .put(
                &format!("t/{}/catalog/m/HEAD", acme.to_hex()),
                bytes::Bytes::from_static(b"x"),
                PutOptions::default(),
            )
            .await
            .expect("put head");
        set_tenant_config(
            &store,
            &acme,
            &TenantConfig::new(TenantLifecycleState::Active),
            1_000,
        )
        .await
        .expect("write config");
        upsert_token(&store, KEY, b"acme-token", "acme", 1_000)
            .await
            .expect("provision token");

        // While the token is present, both the old token-derived restriction
        // and the new lifecycle restriction maintain acme.
        let token_set = vec![acme];
        assert_eq!(
            discover_and_restrict(&store, Some(&token_set))
                .await
                .expect("ok")
                .maintained,
            vec![acme],
            "with the token present, both restrictions maintain the tenant"
        );

        // The operator removes acme's token from sys/auth.
        remove_token(&store, KEY, b"acme-token", 2_000)
            .await
            .expect("revoke token");
        let (map, _v) = read_auth_map(&store, KEY)
            .await
            .expect("read auth")
            .expect("present");
        assert!(
            tenant_for_token(&map, KEY, b"acme-token").is_none(),
            "sanity: the token no longer resolves, so a token-derived maintained set would drop acme"
        );

        // OLD behaviour (the bug): with the restriction now empty (the token is
        // gone), acme is EXCLUDED -- its retention would stop even though its
        // data is still present.
        let after_removal: Vec<TenantHash> = Vec::new();
        let old = discover_and_restrict(&store, Some(&after_removal))
            .await
            .expect("ok");
        assert_eq!(
            old.maintained,
            Vec::<TenantHash>::new(),
            "the old frozen restriction drops the tenant once its token is gone (the bug)"
        );
        assert_eq!(old.excluded, 1);

        // NEW behaviour (the fix): the lifecycle restriction keeps acme, because
        // its durable config record is untouched by the token removal, so
        // maintenance (and therefore retention) continues. The token-derived set
        // is the `fallback_allow` (which shrank to empty), and there is no
        // explicit `--maintain-tenant` narrowing.
        let new = discover_and_restrict_by_lifecycle(&store, Some(&after_removal))
            .await
            .expect("ok");
        assert_eq!(
            new.maintained,
            vec![acme],
            "the lifecycle restriction keeps a config-recorded tenant after its token is removed"
        );
        assert_eq!(new.excluded, 0);
    }

    /// The acute regression this fix closes (ADR-0066 decision 6): a NO-CONFIG
    /// tenant named only via `--tenant-token` must stay maintained when a
    /// DIFFERENT tenant is separately named via `--maintain-tenant`. The two
    /// flags are merged into one `fallback_allow` set, so both `acme` and
    /// `globex` appear in it; with no config record for either, both are
    /// maintained. The pre-fix `explicit_restrict` mechanism (a `--maintain-
    /// tenant`-only final filter) silently dropped `acme` here, even though it
    /// is named by `--tenant-token`, which its flag's own contract forbids.
    #[tokio::test]
    async fn no_config_tenant_named_by_tenant_token_stays_maintained_with_a_separate_maintain_tenant()
     {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        let globex = TenantId::new("globex").hash();
        // Neither tenant carries a config record. This reproduces the reviewer's
        // repro: `--tenant-token acme=... --maintain-tenant globex`, merged into
        // one fallback allow-list of [acme, globex].
        for tenant in [acme, globex] {
            store
                .put(
                    &format!("t/{}/catalog/m/HEAD", tenant.to_hex()),
                    bytes::Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }

        let outcome = discover_and_restrict_by_lifecycle(&store, Some(&[acme, globex]))
            .await
            .expect("ok");
        let mut maintained = outcome.maintained.clone();
        maintained.sort_by_key(|t| t.to_hex());
        let mut expected = vec![acme, globex];
        expected.sort_by_key(|t| t.to_hex());
        assert_eq!(
            maintained, expected,
            "a no-config tenant named by --tenant-token stays maintained even when a \
             different tenant is named by --maintain-tenant"
        );
        assert_eq!(outcome.excluded, 0);
    }

    /// The property this whole redesign rests on: a tenant WITH a durable config
    /// record stays maintained even when `fallback_allow` is a non-empty set
    /// that does NOT name it. A config record is the sole authority; no CLI flag
    /// can exclude it. This is the direct replacement for the deleted
    /// `explicit_maintain_tenant_restriction_excludes_a_config_recorded_tenant`,
    /// asserting the opposite outcome now mandated by ADR-0066 decision 6.
    #[tokio::test]
    async fn config_recorded_tenant_stays_maintained_even_when_fallback_excludes_it() {
        use ravel_catalog::{TenantConfig, TenantLifecycleState, set_tenant_config};

        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        let globex = TenantId::new("globex").hash();
        // Both tenants hold data AND carry a durable config record.
        for tenant in [acme, globex] {
            store
                .put(
                    &format!("t/{}/catalog/m/HEAD", tenant.to_hex()),
                    bytes::Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
            set_tenant_config(
                &store,
                &tenant,
                &TenantConfig::new(TenantLifecycleState::Active),
                1_000,
            )
            .await
            .expect("write config");
        }

        // A non-empty fallback naming only globex: acme is NOT named by it. Its
        // config record must keep it maintained anyway.
        let outcome = discover_and_restrict_by_lifecycle(&store, Some(&[globex]))
            .await
            .expect("ok");
        let mut maintained = outcome.maintained.clone();
        maintained.sort_by_key(|t| t.to_hex());
        let mut expected = vec![acme, globex];
        expected.sort_by_key(|t| t.to_hex());
        assert_eq!(
            maintained, expected,
            "a config-recorded tenant stays maintained even when the fallback set does not name it"
        );
        assert_eq!(
            outcome.excluded, 0,
            "no config-recorded tenant is excluded by the fallback set"
        );
    }

    /// A discovered tenant with NO config record still honours the static flag
    /// restriction (backward compatibility for legacy/OIDC data that predates
    /// per-tenant config records): named by the restriction it is maintained,
    /// not named it is excluded.
    #[tokio::test]
    async fn lifecycle_discovery_falls_back_to_flag_restriction_without_a_config_record() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme").hash();
        let globex = TenantId::new("globex").hash();
        for tenant in [acme, globex] {
            store
                .put(
                    &format!("t/{}/catalog/m/HEAD", tenant.to_hex()),
                    bytes::Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .await
                .expect("put");
        }
        // Neither tenant has a config record. With a restriction naming only
        // acme, globex is excluded (the flag restriction still governs
        // no-config-record tenants); with no restriction, both are maintained.
        let restricted = discover_and_restrict_by_lifecycle(&store, Some(&[acme]))
            .await
            .expect("ok");
        assert_eq!(restricted.maintained, vec![acme]);
        assert_eq!(restricted.excluded, 1);

        let unrestricted = discover_and_restrict_by_lifecycle(&store, None)
            .await
            .expect("ok");
        assert_eq!(
            unrestricted.maintained.len(),
            2,
            "no restriction maintains both"
        );
        assert_eq!(unrestricted.excluded, 0);
    }

    #[test]
    fn metrics_failure_does_not_reset_gauges() {
        let metrics = TenantDiscoveryMetrics::default();
        metrics.record_discovery(3, 2);
        metrics.record_discovery_failure();
        assert_eq!(metrics.tenants_discovered(), 3);
        assert_eq!(metrics.tenants_maintained(), 2);
        assert_eq!(metrics.discovery_failures(), 1);
    }
}
