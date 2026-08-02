//! Shared storage-derived tenant discovery for the maintenance and fold
//! background tasks (ADR-0048 decision 3, issue #504).
//!
//! Both tasks used to learn their tenant set only from `--tenant-token` and
//! `--maintain-tenant`, so a deployment authenticating tenants through OIDC
//! or mTLS -- which populates neither flag -- silently ran neither task for
//! any of them (findings S2-17, S5-09; issue #398's `--maintain-tenant` plus
//! startup warning only fires when the merged list is entirely empty, so a
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
