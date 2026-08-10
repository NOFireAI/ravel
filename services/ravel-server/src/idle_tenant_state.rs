//! Background idle-tenant state eviction sweep (ADR-0069 decision 2, issue
//! #820).
//!
//! Ravel's per-tenant maps never shrank for idle tenants: generation-switch
//! views, the catalog's per-tenant decoded caches, and the SQL per-tenant
//! memory accountants all grew monotonically over process lifetime (ADR-0069
//! context). This is the periodic sweep that bounds them. Its shape is the
//! mechanism/lifecycle split every other background loop in this crate uses
//! ([`crate::store_probe`], [`crate::admission_reconcile`], [`crate::scrub`]):
//! the eviction *mechanism* (which entries are idle, and that eviction is
//! re-derivable) lives in each owning crate and is unit-tested there; the
//! *lifecycle* (interval, jitter, shutdown) lives here.
//!
//! # What is swept, and what is not
//!
//! Only re-derivable per-tenant state is evicted (ADR-0069 decision 2):
//!
//! - **Generation views** (`ravel-ingest`): re-read from the provisioning
//!   record on the tenant's next write.
//! - **Catalog per-tenant caches** (`ravel-catalog`): immutable,
//!   content-addressed or TTL-revalidated, so re-read on the next resolve.
//! - **SQL memory accountants with zero outstanding reservations**
//!   (`ravel-sql`): pure process-local byte counters, rebuilt on the next
//!   query.
//!
//! **Admission-controller state is explicitly excluded.** Its
//! active-series/stream counts are correctness-bearing caps; evicting them
//! would silently reset a tenant's cap consumption (ADR-0069 decision 2,
//! rejected alternative "LRU caps on the admission maps"). Nothing here holds
//! an admission handle, so the exclusion is structural: an admission map is not
//! an [`IdleTenantEvictor`] and can never be registered as one.
//!
//! # Determinism
//!
//! Each owning crate's `evict_idle*` takes the idle threshold and the clock
//! reading as arguments and reads no clock itself; only this loop reads the
//! wall clock (via [`SystemClock`], the one blessed clock, exactly as the sibling
//! loops do), so every eviction rule is deterministic under test.

use std::sync::Arc;
use std::time::Duration;

use ravel_ingest::{Clock, SystemClock};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::fold::jittered;

/// Default `--idle-tenant-state-ttl`: one hour (ADR-0069 decision 2). A tenant
/// whose re-derivable per-tenant state has gone untouched this long is a
/// candidate for eviction. Matches the humantime-duration flag convention of
/// the sibling `--store-probe-interval`/`--scrub-period` knobs.
pub const DEFAULT_IDLE_TENANT_STATE_TTL: Duration = Duration::from_secs(3600);

/// Anything holding re-derivable per-tenant state an idle sweep may evict.
///
/// The one method takes the injected clock reading and idle threshold so the
/// implementation reads no clock; the sweep loop supplies both. `label` names
/// the target for the debug log line, nothing more.
///
/// Admission-controller state is deliberately not an implementor: its caps are
/// correctness-bearing (ADR-0069 decision 2), so it can never be registered
/// with the sweep.
pub trait IdleTenantEvictor: Send + Sync {
    /// Evict per-tenant state last touched before `now_ns - ttl_ns`. Returns
    /// the number of per-tenant entries evicted. Must be safe to call
    /// concurrently with live traffic; every evicted entry is re-derived on the
    /// tenant's next access.
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize;

    /// A short static label for the sweep's debug log (e.g.
    /// `"generation-views-metrics"`).
    fn label(&self) -> &'static str;
}

impl IdleTenantEvictor for ravel_ingest::IngestRouter {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.evict_idle_generation_views(now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "generation-views-metrics"
    }
}

impl IdleTenantEvictor for ravel_ingest::LogIngestRouter {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.evict_idle_generation_views(now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "generation-views-logs"
    }
}

impl IdleTenantEvictor for ravel_ingest::SpanIngestRouter {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.evict_idle_generation_views(now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "generation-views-spans"
    }
}

impl IdleTenantEvictor for ravel_catalog::Catalog {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.evict_idle_tenants(now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "catalog-per-tenant-caches"
    }
}

#[cfg(feature = "sql")]
impl IdleTenantEvictor for ravel_sql::SqlExecutor {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        self.evict_idle_accountants(now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "sql-memory-accountants"
    }
}

/// One sweep over `evictors` at `now_ns` with idle threshold `ttl_ns`. Returns
/// the total number of per-tenant entries evicted across all targets. Split
/// from the loop so a test can drive one deterministic sweep without the timer.
pub fn run_sweep(evictors: &[Arc<dyn IdleTenantEvictor>], now_ns: i64, ttl_ns: i64) -> usize {
    let mut total = 0;
    for evictor in evictors {
        let evicted = evictor.evict_idle(now_ns, ttl_ns);
        if evicted > 0 {
            tracing::debug!(
                target = evictor.label(),
                evicted,
                "idle-tenant sweep evicted re-derivable per-tenant state"
            );
        }
        total += evicted;
    }
    total
}

/// Handle to the spawned idle-tenant sweep, so shutdown can stop it cleanly
/// (mirrors [`crate::admission_reconcile::AdmissionReconcileTask`]).
pub struct IdleTenantStateTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl IdleTenantStateTask {
    /// No task: the sweep is disabled (`--idle-tenant-state-ttl 0`) or there is
    /// no re-derivable per-tenant state to sweep in this mode.
    pub fn none() -> Self {
        IdleTenantStateTask {
            shutdown: None,
            handle: None,
        }
    }

    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle {
            let _ = handle.await;
        }
    }
}

/// Spawn the idle-tenant sweep over `evictors` on interval `ttl` (jittered by
/// the same helper the fold and maintenance loops use, so replicas do not tick
/// in lockstep). Every tick evicts per-tenant state last touched more than
/// `ttl` ago. Returns immediately; the task runs until
/// [`IdleTenantStateTask::shutdown`].
///
/// A zero `ttl` (the `--idle-tenant-state-ttl 0` disabled sentinel) or an empty
/// `evictors` list returns a no-op handle: nothing is spawned. The first cycle
/// sleeps a full (jittered) interval before the first sweep, so a fleet of
/// replicas started together do not sweep in lockstep.
pub fn spawn(evictors: Vec<Arc<dyn IdleTenantEvictor>>, ttl: Duration) -> IdleTenantStateTask {
    if ttl.is_zero() || evictors.is_empty() {
        return IdleTenantStateTask::none();
    }
    // The idle threshold in nanoseconds, saturating so an absurdly large TTL
    // cannot overflow into a negative i64 that would evict everything.
    let ttl_ns = i64::try_from(ttl.as_nanos()).unwrap_or(i64::MAX);

    let (tx, mut rx) = oneshot::channel();
    // Production OS-entropy jitter (ADR-0068 decision 2), the same default the
    // fold and maintenance loops use; the harness does not drive this loop.
    let rng: Arc<dyn ravel_commit::rng::RngSource> = Arc::new(ravel_commit::rng::SystemRng);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(jittered(ttl, rng.as_ref())) => {}
                _ = &mut rx => return,
            }
            let now_ns = SystemClock.now_ns();
            run_sweep(&evictors, now_ns, ttl_ns);
        }
    });
    IdleTenantStateTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use ravel_ingest::{
        AdmissionController, AdmissionLimits, IngestConfig, IngestRouter, SystemClock,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::{Signal, TenantId};

    use super::*;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// ADR-0069 decision 2 (issue #820): a sweep that evicts idle re-derivable
    /// per-tenant state leaves the admission controller's cap state for the same
    /// idle tenant untouched. Admission state is explicitly excluded from
    /// eviction (its active-series counts are correctness-bearing caps), which
    /// is structural here: the admission controller is not an
    /// [`IdleTenantEvictor`] and is never in the sweep's target list.
    ///
    /// Deterministic: the sweep is driven with an injected `now_ns` far past the
    /// TTL, so the router's idle generation view is unconditionally evicted,
    /// yet the admission usage recorded for the same tenant survives.
    #[tokio::test]
    async fn admission_state_never_evicted() {
        let store: Arc<dyn ravel_object_store::ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = Arc::new(SystemClock);

        // A router with a cached generation view for the idle tenant, and an
        // admission controller holding cap state for the same tenant.
        let router = Arc::new(IngestRouter::new(
            IngestConfig {
                shard_count: 2,
                ..IngestConfig::default()
            },
            store.clone(),
            Signal::Metrics,
            clock.clone(),
        ));
        let admission = AdmissionController::new(clock.clone(), AdmissionLimits::default());

        let tenant = TenantId::new("idle-tenant");
        let tenant_hash = tenant.hash();

        // Prime the router's generation view (as a first write would) at t0.
        let t0 = 1_000 * NS_PER_HOUR;
        router.refresh_generations(tenant_hash, vec![], t0);

        // Record admission usage for the tenant so its per-tenant cap state is
        // non-empty (a byte-rate charge admits and books the bytes).
        admission
            .check_byte_rate(&tenant, Signal::Metrics, 4_096, t0)
            .expect("byte-rate charge admitted under the default cap");
        let bytes_before = admission
            .usage_snapshot()
            .into_iter()
            .find(|u| u.tenant_hash == tenant_hash)
            .map(|u| u.bytes_admitted_total)
            .expect("tenant has admission usage before the sweep");
        assert_eq!(bytes_before, 4_096);

        // Sweep, far past the TTL. The router is the only registered evictor;
        // the admission controller is deliberately not one.
        let evictors: Vec<Arc<dyn IdleTenantEvictor>> = vec![router.clone()];
        let ttl_ns = 100 * NS_PER_HOUR;
        let sweep_ns = t0 + ttl_ns + 1;
        let evicted = run_sweep(&evictors, sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "the idle tenant's generation view is evicted");

        // Admission cap state for the same idle tenant is untouched.
        let bytes_after = admission
            .usage_snapshot()
            .into_iter()
            .find(|u| u.tenant_hash == tenant_hash)
            .map(|u| u.bytes_admitted_total)
            .expect("admission usage survives the sweep");
        assert_eq!(
            bytes_after, bytes_before,
            "admission cap state must not be evicted by the idle sweep"
        );

        // The router's shard actors are detached tokio tasks; the test runtime
        // reaps them on drop. `shutdown` is not called here because the router
        // is shared (an evictor holds an `Arc` clone), so it cannot be moved.
    }

    /// The disabled sentinel and the empty-target case both spawn no task.
    #[tokio::test]
    async fn disabled_or_empty_spawns_no_task() {
        let disabled = spawn(Vec::new(), Duration::ZERO);
        disabled.shutdown().await;
        let empty = spawn(Vec::new(), DEFAULT_IDLE_TENANT_STATE_TTL);
        empty.shutdown().await;
    }
}
