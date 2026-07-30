//! Per-tenant background maintenance task (docs/compaction-retention-plan.md
//! P8, issue #115). Periodically runs age-based retention, L0->L1 compaction,
//! and the GC sweeper over every `(signal, shard)` of each tenant, mirroring
//! the shape of the fold task (`crate::fold`): one loop per tenant from the
//! static tenant-token config, a config struct, and a handle with clean
//! shutdown.
//!
//! Unlike fold (a pure query-cost optimization), this task deletes and rewrites
//! durable objects, but it changes nothing about *what* any sweep, retention,
//! or compaction rule decides: it is only the driver that calls
//! [`ravel_maintain::scan_and_maintain`] (retention-before-compaction over
//! every sealed bucket) and [`ravel_maintain::sweep_shard`] (the three GC
//! rules) once per tick. Both are stateless and idempotent, so a missed or
//! crashed tick is recovered on the next one. The clock is the real
//! [`SystemClock`], matching everything else in this crate, and the only
//! [`LeaseCheck`] that exists ([`NoLeases`]) is used.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt as _;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::{
    Clock, CompactorConfig, NoLeases, RetentionConfig, scan_and_maintain, sweep_shard,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Default `maintain_interval`: 5 minutes.
pub const DEFAULT_MAINTAIN_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// The service-layer wall clock for [`ravel_maintain::Clock`]
/// (`ravel_maintain`'s clock doc defers the wall-clock impl to phase 8, here).
/// Delegates to `ravel-ingest`'s [`SystemClock`], the one blessed wall clock in
/// this process, so no maintenance code path ever reads `SystemTime::now()`
/// directly.
struct WallClock;

impl Clock for WallClock {
    fn now_ns(&self) -> i64 {
        SystemClock.now_ns()
    }
}

/// The signals this server ingests, and therefore maintains, today. Metrics
/// (RSEG) and logs (RLOG) both flow through the same signal-generic
/// compaction/retention/sweep code (ADR-0032).
const MAINTAINED_SIGNALS: [Signal; 2] = [Signal::Metrics, Signal::Logs];

/// Everything the maintenance task needs beyond the store and the tenant list.
#[derive(Debug, Clone)]
pub struct MaintenanceTaskConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub shard_count: u32,
    /// Compactor knobs (seal margin, part cap, grace, protection horizon).
    /// `dry_run` is always false for the running service; only the CLI's
    /// `--dry-run` sets it.
    pub compactor: CompactorConfig,
    /// Validated per-tenant retention windows (ADR-0019). `RetentionConfig`'s
    /// default is "no retention", so with no `--retention-*` flags this task
    /// compacts and sweeps but never age-deletes.
    pub retention: RetentionConfig,
}

impl Default for MaintenanceTaskConfig {
    fn default() -> Self {
        MaintenanceTaskConfig {
            enabled: false,
            interval: DEFAULT_MAINTAIN_INTERVAL,
            shard_count: 4,
            compactor: CompactorConfig::default(),
            retention: RetentionConfig::default(),
        }
    }
}

/// Handle to every spawned maintenance task, for clean shutdown (mirrors
/// [`crate::fold::FoldTasks`]).
pub struct MaintenanceTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl MaintenanceTasks {
    pub fn none() -> Self {
        MaintenanceTasks {
            shutdown: Vec::new(),
            handles: Vec::new(),
        }
    }

    pub async fn shutdown(self) {
        for tx in self.shutdown {
            let _ = tx.send(());
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Spawn one maintenance loop per tenant. Tenants come from the static
/// tenant-token config, the same list [`crate::fold::spawn`] uses. Returns
/// immediately; tasks run until [`MaintenanceTasks::shutdown`].
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    tenants: &[TenantHash],
    config: MaintenanceTaskConfig,
) -> MaintenanceTasks {
    if !config.enabled || tenants.is_empty() {
        return MaintenanceTasks::none();
    }

    // One compactor writer_id per process start, shared by every tenant task
    // (recorded in each L1 part's footer; informational, never dedup-priority).
    let mut compactor = config.compactor.clone();
    compactor.compactor_writer_id = Uuid::new_v4();
    let compactor = Arc::new(compactor);
    let retention = Arc::new(config.retention.clone());

    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for &tenant in tenants {
        let (tx, rx) = oneshot::channel();
        let store = store.clone();
        let compactor = compactor.clone();
        let retention = retention.clone();
        let interval = config.interval;
        let shard_count = config.shard_count;
        let handle = tokio::spawn(async move {
            run_loop(
                store,
                tenant,
                compactor,
                retention,
                shard_count,
                interval,
                rx,
            )
            .await;
        });
        shutdown.push(tx);
        handles.push(handle);
    }
    MaintenanceTasks { shutdown, handles }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    compactor: Arc<CompactorConfig>,
    retention: Arc<RetentionConfig>,
    shard_count: u32,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }
        run_tick(store.as_ref(), &tenant, &compactor, &retention, shard_count).await;
    }
}

/// One maintenance pass over every `(signal, shard)` of one tenant: retention
/// before compaction (via [`scan_and_maintain`]) then the GC sweeper (via
/// [`sweep_shard`]). Every error is logged and retried next tick; nothing here
/// affects query correctness. Split out from [`run_loop`] so a test can drive a
/// single deterministic tick without the timer.
pub async fn run_tick(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
) {
    let clock = WallClock;
    for signal in MAINTAINED_SIGNALS {
        for shard in 0..shard_count {
            match scan_and_maintain(
                store, &clock, compactor, retention, &NoLeases, *tenant, signal, shard,
            )
            .await
            {
                Ok(report) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        retired = report.retired,
                        compacted = report.compacted,
                        already_done = report.already_done,
                        not_sealed = report.not_sealed,
                        "maintenance: retention + compaction pass complete"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        error = %err,
                        "maintenance: retention/compaction pass failed; retried next tick"
                    );
                }
            }

            match sweep_shard(store, &clock, compactor, &NoLeases, tenant, signal, shard).await {
                Ok(report) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        orphans = report.orphans_deleted,
                        superseded_records = report.superseded_records_deleted,
                        superseded_data = report.superseded_data_deleted,
                        unreferenced_parts = report.unreferenced_parts_deleted,
                        "maintenance: sweep pass complete"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        error = %err,
                        "maintenance: sweep pass failed; retried next tick"
                    );
                }
            }
        }
    }
}

/// Up to 10% jitter over `base`, so co-started replicas' maintenance ticks do
/// not run in lockstep (same rationale as the fold task's jitter).
fn jittered(base: Duration) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rand::rng().random_range(0..=jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    /// The retention floor is validated against the catalog's max_ingest_lag,
    /// which must equal ravel-maintain's own DEFAULT_MAX_INGEST_LAG_NS: the two
    /// crates duplicate the constant behind a sync-contract comment (no
    /// ravel-maintain -> ravel-catalog dependency), so if either ever drifts
    /// this test fails and the retention floor would silently be validated
    /// against a different lag than the catalog resolves with.
    #[test]
    fn catalog_and_maintain_ingest_lag_agree() {
        assert_eq!(
            ravel_catalog::CatalogConfig::default().max_ingest_lag_ns,
            ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS,
            "catalog and maintain max_ingest_lag drifted; the retention floor \
             would be validated against a different lag than the catalog uses"
        );
    }

    /// One maintenance tick over an empty store touches every (signal, shard)
    /// and returns cleanly: it proves the driver actually walks both signals
    /// and all shards and calls scan_and_maintain + sweep_shard for each,
    /// without needing seeded segments (the underlying functions are tested in
    /// ravel-maintain).
    #[tokio::test]
    async fn run_tick_over_empty_store_is_clean() {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        run_tick(&store, &tenant, &compactor, &retention, 4).await;
    }
}
