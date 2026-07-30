//! Per-(tenant, signal) background catalog fold task (docs/metric-index-plan.md
//! section 4, ADR-0020). Periodically calls [`Catalog::fold`] so query
//! resolve can serve sealed history from snapshots instead of full listing.
//!
//! Never runs on the ingest or query path, and never affects correctness:
//! every failure here is logged and retried on the next tick. Disabling this
//! task (`--disable-fold`) only changes query cost, never query results.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt as _;
use ravel_catalog::Catalog;
use ravel_ingest::{Clock, SystemClock};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Default `fold_interval`: 5 minutes (docs/metric-index-plan.md section 4).
pub const DEFAULT_FOLD_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy)]
pub struct FoldTaskConfig {
    pub enabled: bool,
    pub fold_interval: Duration,
}

impl Default for FoldTaskConfig {
    fn default() -> Self {
        FoldTaskConfig {
            enabled: true,
            fold_interval: DEFAULT_FOLD_INTERVAL,
        }
    }
}

/// Handle to every spawned fold task, so shutdown can stop them cleanly
/// (mirrors [`crate::Running`]'s listener shutdown handles).
pub struct FoldTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl FoldTasks {
    pub fn none() -> Self {
        FoldTasks {
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

/// Every signal `ravel-server` folds. One fold loop is spawned per
/// (tenant, signal) pair, so adding a signal here doubles the task count per
/// tenant without changing any loop's shape. `FoldTaskConfig` (enabled,
/// fold_interval) is shared across signals for v1: both currently want the
/// same 5-minute cadence (ADR-0033); a per-signal interval is a config-shape
/// follow-up if that changes.
const FOLD_SIGNALS: [Signal; 2] = [Signal::Metrics, Signal::Logs];

/// Spawns one fold loop per (tenant, signal), for every signal in
/// [`FOLD_SIGNALS`]. [`run_loop`] is signal-generic; a new signal is added by
/// extending that array, not by restructuring this function (ADR-0033 gap 1;
/// docs/metric-index-plan.md is written per (tenant, signal) throughout).
///
/// [`Signal::Logs`] folds through the same [`Catalog::fold`] path as metrics
/// and produces `catalog/l/HEAD` plus snapshot parts, but no name-postings
/// object: `Catalog::fold` always attempts the RSEG-specific postings build
/// (`build_postings`/`fetch_entry_names`), which for a log entry issues one
/// full-object GET and then fails to decode the bytes as RSEG (an RLOG object
/// carries signal=2, RSEG expects signal=1), so `build_postings` returns
/// `None` and the fold skips writing a postings ref without failing. That
/// wasted GET-plus-failed-decode recurs every fold cycle for each log entry
/// newly covered since the last fold: real I/O and CPU proportional to new
/// log volume. It is accepted for v1 (ADR-0033), not a bug; fixing it would
/// mean a signal-aware short-circuit inside `ravel-catalog`, deliberately out
/// of scope here.
///
/// Tenants come from the static tenant-token config, per the plan's own note
/// (section 11) that no separate enumeration mechanism is needed. Returns
/// immediately; tasks run in the background until [`FoldTasks::shutdown`].
pub fn spawn(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenants: &[TenantHash],
    config: FoldTaskConfig,
) -> FoldTasks {
    if !config.enabled || tenants.is_empty() {
        return FoldTasks::none();
    }

    // One folder_id per process start (proto/ravel/catalog.proto,
    // `SnapshotHead.folder_id`), shared by every tenant task in this process.
    let folder_id = Uuid::new_v4();
    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for &tenant in tenants {
        for signal in FOLD_SIGNALS {
            let (tx, rx) = oneshot::channel();
            let catalog = catalog.clone();
            let store = store.clone();
            let interval = config.fold_interval;
            let handle = tokio::spawn(async move {
                run_loop(catalog, store, tenant, signal, folder_id, interval, rx).await;
            });
            shutdown.push(tx);
            handles.push(handle);
        }
    }
    FoldTasks { shutdown, handles }
}

async fn run_loop(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    signal: Signal,
    folder_id: Uuid,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }

        let now_ns = SystemClock.now_ns();
        if head_fresh_enough(store.as_ref(), &tenant, signal, interval, now_ns).await {
            tracing::debug!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                "catalog fold: HEAD already fresh, skipping this tick"
            );
            continue;
        }

        match catalog.fold(&tenant, signal, folder_id, now_ns, &[]).await {
            Ok(report) => {
                tracing::info!(
                    tenant = %tenant.to_hex(),
                    signal = ?signal,
                    no_op = report.no_op,
                    rebuilt = report.rebuilt,
                    watermark_hour = ?report.watermark_hour,
                    previous_watermark_hour = ?report.previous_watermark_hour,
                    buckets_folded = report.buckets_folded,
                    entry_count = report.entry_count,
                    part_bytes = report.part_bytes,
                    list_requests = report.list_requests,
                    get_requests = report.get_requests,
                    put_requests = report.put_requests,
                    "catalog fold complete"
                );
            }
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    signal = ?signal,
                    error = %err,
                    "catalog fold failed; the index degrades to listing until a later fold succeeds"
                );
            }
        }
    }
}

/// Adds up to 10% jitter on top of `base`, so multiple replicas' fold tasks
/// (started at roughly the same time) don't tick in lockstep forever.
fn jittered(base: Duration) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rand::rng().random_range(0..=jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

/// Cheap duplicate-work avoidance across replicas (docs/metric-index-plan.md
/// section 4 scheduling note): peeks at HEAD directly (bypassing the
/// catalog's own HEAD cache and fold logic) and skips this tick if another
/// replica folded within the last `interval`. Correctness never depends on
/// this: `catalog.fold` performs its own authoritative HEAD read and no-ops
/// safely if there is nothing new to fold, so a wrong (or racing) answer here
/// only costs a redundant fold attempt, never a missed one.
async fn head_fresh_enough(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    interval: Duration,
    now_ns: i64,
) -> bool {
    let key = head_key(tenant, signal);
    let Ok(got) = store.get(&key, GetRange::Full).await else {
        return false;
    };
    let Ok(head) = ravel_catalog::decode_head(&got.data) else {
        return false;
    };
    let age_ns = now_ns.saturating_sub(head.created_unix_ns);
    age_ns >= 0 && (age_ns as u128) < interval.as_nanos()
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format):
/// `t/<tenant_hash>/catalog/<signal>/HEAD`. Reconstructed here rather than
/// imported because it names a `pub(crate)` helper inside `ravel-catalog`;
/// this task only ever uses it for the freshness peek above, never to
/// mutate the object.
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}
