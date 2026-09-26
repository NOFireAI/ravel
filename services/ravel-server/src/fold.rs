//! Per-signal background catalog fold task (ADR-0020; storage-derived tenant
//! set is ADR-0048 decision 3). Periodically calls [`Catalog::fold`] so query resolve can
//! serve sealed history from snapshots instead of full listing.
//!
//! Never runs on the ingest or query path, and never affects correctness:
//! every failure here is logged and retried on the next tick. Disabling this
//! task (`--disable-fold`) only changes query cost, never query results.
//!
//! One loop per [`FOLD_SIGNALS`] entry, not one per tenant: each tick
//! re-enumerates tenants from storage ([`ravel_maintain::discover_tenants`],
//! one delimited listing of `t/`) and folds every tenant the cycle discovers,
//! narrowed by ownership and then by each tenant's durable lifecycle state and
//! the flag fallback (ADR-0066 decision 6: a config record keeps a tenant in
//! the set unconditionally regardless of its token, and no flag can exclude a
//! config-recorded tenant). A tenant onboarded mid-run is folded starting the
//! next tick with no restart.
//! A discovery failure skips that signal's whole cycle -- no tenant is folded
//! -- and is retried next tick; it never falls back to an empty set, since
//! fold is best-effort and a quiet failure here would look identical to
//! "nothing new to fold."
//!
//! The scheduled fold is partitioned across the maintain live set (ADR-1693
//! decision 1). Each tick gates a `(tenant, signal)` pair on
//! [`WorkerSet::owns_unit`] over shard [`FOLD_UNIT_SHARD`], the same unit key
//! the per-signal maintenance sweeps use, so the process that sweeps a pair's
//! catalog objects is the process that folds it. The gate is pure computation
//! over the discovery result and it runs before every per-tenant read, the
//! lifecycle config record included, so a pair this process does not own costs
//! zero requests. The per-tick cost of a pair nobody on this process owns is
//! therefore its share of the one `t/` listing and nothing else.
//! In `--mode all` the live set is the solo one (`{self}`), every pair is
//! owned, and the behavior is byte-for-byte the unpartitioned fold
//! (ADR-1693 decision 3).

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::Catalog;
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_maintain::{Clock, MaintainError, RetentionConfig, WorkerSet};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::restrict_by_lifecycle;

/// Default `fold_interval`: 5 minutes.
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
/// (tenant, signal) pair, so adding a signal here adds one loop per tenant
/// without changing any loop's shape. `FoldTaskConfig` (enabled,
/// fold_interval) is shared across signals for v1: all currently want the
/// same 5-minute cadence (ADR-0033); a per-signal interval is a config-shape
/// follow-up if that changes.
///
/// [`Signal::Spans`] is added once span compaction exists (ADR-0041 phase 3):
/// span buckets now produce L1 `.rspan` parts the resolver serves from
/// snapshots, exactly as logs do. Spans fold through the same
/// [`Catalog::fold`] path as logs and share the same accepted no-op-postings
/// cost described below (an `.rspan` object carries signal=3, so the
/// RSEG-specific postings build fails to decode it and skips writing a
/// postings ref, same as logs' signal=2).
///
/// `pub(crate)` so `crate::metrics` renders exactly one fold-liveness series
/// per entry: the series set at `/metrics` is then the set of loops that are
/// supposed to be alive, and a loop that dies leaves its own series standing
/// and going stale rather than hiding behind its siblings.
pub(crate) const FOLD_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// The shard whose rendezvous owner owns a whole `(tenant, signal)` pair's
/// fold (ADR-1693 decision 1). The fold is per pair, not per shard, so it
/// needs one shard to key the unit on; shard 0 is the convention the
/// per-signal maintenance sweeps already use
/// ([`crate::maintain`]), which is what makes the sweeper and the
/// folder for a pair the same process.
pub const FOLD_UNIT_SHARD: u32 = 0;

/// What one tick did, per tenant, for one signal. Returned by [`run_tick`] so
/// the partition is assertable (ADR-1693's acceptance test) rather than only
/// visible in logs.
///
/// `discovered` counts the whole discovery result: discovery is per process
/// and never gated on ownership (ADR-0065 decision 2), so both processes see
/// every tenant. The three narrowings then apply in cost order: `owned` is the
/// subset this process owns under the live set it was given, decided without
/// any request, and `maintained`/`excluded` split `owned` by the lifecycle
/// read that costs one request per tenant. `maintained` is exactly the set the
/// remaining fields partition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldTickReport {
    /// Every tenant storage reported under `t/` this tick.
    pub discovered: usize,
    /// Discovered tenants whose `(tenant, signal, 0)` unit this process owns.
    pub owned: Vec<TenantHash>,
    /// `owned`, narrowed by lifecycle state and the flag fallback.
    pub maintained: usize,
    /// Owned tenants the flag restriction excluded.
    pub excluded: usize,
    /// Maintained tenants whose fold ran and returned a report.
    pub folded: Vec<TenantHash>,
    /// Maintained tenants whose fold returned an error. Logged and retried
    /// next tick; never fails a query.
    pub failed: Vec<TenantHash>,
    /// Maintained tenants skipped because their HEAD was younger than the fold
    /// interval, the cheap duplicate-work peek.
    pub skipped_fresh: Vec<TenantHash>,
}

/// Spawns one fold loop per signal in [`FOLD_SIGNALS`], not one per tenant:
/// each tick re-derives the tenant set from storage. [`run_loop`] is
/// signal-generic; a new signal is added by extending that array, not by
/// restructuring this function (ADR-0033 gap 1; folding is per
/// (tenant, signal) throughout).
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
/// `fallback_allow` is the merged static tenant set (`--tenant-token` or
/// `--tenant-token-file`, plus `--maintain-tenant`):
/// empty means unconfigured, and it otherwise governs only tenants with no
/// durable config record (ADR-0048 decision 3, ADR-0066 decision 6). A tenant
/// carrying a config record is maintained unconditionally, so no flag can
/// exclude it. Returns immediately; tasks run in the background until
/// [`FoldTasks::shutdown`].
///
/// `retention` is the same CLI-derived [`RetentionConfig`] the Maintain-mode
/// physical sweep uses (`main.rs`, threaded into `MaintenanceTaskConfig`).
/// Each tick resolves `retention.window_for(tenant)` per tenant and passes it
/// into [`Catalog::fold`] as the deployment-default retention window, so the
/// fold's retention-frontier reconcile runs for a tenant configured only by
/// CLI flags with no durable `TenantConfig.retention_ns` record (ADR-0078).
/// An unconfigured `RetentionConfig` resolves to `None` for every tenant, so
/// this is inert when no retention flag is set.
///
/// `worker` and `live_set` are the process's one maintain [`WorkerSet`] and the
/// live-set `watch` the maintenance heartbeat task publishes on (ADR-1693
/// decision 1). Every tick reads the latest published set and folds only the
/// pairs this process owns. In `Mode::All` nothing ever publishes on that
/// channel, so the receiver holds the solo live set (`{self}`) for the life of
/// the process and every pair is owned (decision 3).
///
/// `clock` is the maintain context's injected clock (decision 6), so a test
/// that drives membership and folding advances one clock.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: &[TenantHash],
    config: FoldTaskConfig,
    retention: Arc<RetentionConfig>,
    worker: Arc<WorkerSet>,
    live_set: watch::Receiver<Vec<Uuid>>,
    clock: Arc<dyn Clock>,
) -> FoldTasks {
    if !config.enabled {
        return FoldTasks::none();
    }

    // Production OS-entropy randomness (ADR-0068 decision 2): the folder id
    // and the per-tick loop jitter both draw from this one source instead of
    // `Uuid::new_v4()` / `rand::rng()` directly. The server always uses the
    // OS-entropy default; only the simulation harness injects a seeded source,
    // and it does not drive this loop.
    let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
    // One folder_id per process start (proto/ravel/catalog.proto,
    // `SnapshotHead.folder_id`), shared by every signal loop in this process.
    let folder_id = rng.new_uuid();
    let fallback_allow = if fallback_allow.is_empty() {
        None
    } else {
        Some(fallback_allow.to_vec())
    };
    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for signal in FOLD_SIGNALS {
        let (tx, rx) = oneshot::channel();
        let catalog = catalog.clone();
        let store = store.clone();
        let fallback_allow = fallback_allow.clone();
        let interval = config.fold_interval;
        let rng = Arc::clone(&rng);
        let retention = Arc::clone(&retention);
        let worker = Arc::clone(&worker);
        let live_set = live_set.clone();
        let clock = Arc::clone(&clock);
        let handle = tokio::spawn(async move {
            run_loop(
                catalog,
                store,
                signal,
                fallback_allow,
                folder_id,
                interval,
                rng,
                retention,
                worker,
                live_set,
                clock,
                rx,
            )
            .await;
        });
        shutdown.push(tx);
        handles.push(handle);
    }
    FoldTasks { shutdown, handles }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    signal: Signal,
    fallback_allow: Option<Vec<TenantHash>>,
    folder_id: Uuid,
    interval: Duration,
    rng: Arc<dyn RngSource>,
    retention: Arc<RetentionConfig>,
    worker: Arc<WorkerSet>,
    live_set: watch::Receiver<Vec<Uuid>>,
    clock: Arc<dyn Clock>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }

        // The latest set the maintenance heartbeat task published. Read once
        // per tick so every pair in one tick is partitioned against one
        // membership view, the same way the maintenance discovery cycle reads
        // it (ADR-0065 decision 2).
        let live = live_set.borrow().clone();
        match run_tick(
            catalog.as_ref(),
            store.as_ref(),
            signal,
            fallback_allow.as_deref(),
            folder_id,
            interval,
            retention.as_ref(),
            worker.as_ref(),
            &live,
            clock.as_ref(),
        )
        .await
        {
            Ok(report) => {
                if report.excluded > 0 {
                    tracing::debug!(
                        signal = ?signal,
                        excluded = report.excluded,
                        "catalog fold: flag restriction excluded discovered tenants"
                    );
                }
                tracing::debug!(
                    signal = ?signal,
                    discovered = report.discovered,
                    maintained = report.maintained,
                    owned = report.owned.len(),
                    folded = report.folded.len(),
                    failed = report.failed.len(),
                    skipped_fresh = report.skipped_fresh.len(),
                    "catalog fold cycle complete"
                );
            }
            Err(err) => {
                tracing::error!(
                    signal = ?signal,
                    error = %err,
                    "catalog fold: tenant discovery failed; skipping this cycle entirely, retried next tick"
                );
            }
        }
    }
}

/// One fold cycle for one signal: re-enumerate tenants from storage, then fold
/// every pair this process owns under `live_set` (ADR-1693 decision 1).
///
/// The ownership gate comes BEFORE every per-tenant read, the lifecycle config
/// record included, so a pair this process does not own costs zero requests
/// naming it. The tick's whole cost for such a pair is its share of the one
/// delimited `t/` listing that discovery makes. A discovery failure returns the
/// error rather than an empty set: fold is best-effort and a quiet failure here
/// would look identical to "nothing new to fold."
///
/// Narrowing by ownership before the lifecycle read cannot change which pairs
/// fold: ownership and the lifecycle restriction are independent predicates
/// over the discovered set, so applying them in either order selects the same
/// intersection.
///
/// Public for the same reason [`crate::maintain::run_tick`] is: a test drives
/// one deterministic cycle with an injected clock and an explicit live set,
/// instead of racing the background loop's timer.
#[allow(clippy::too_many_arguments)]
pub async fn run_tick(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    fallback_allow: Option<&[TenantHash]>,
    folder_id: Uuid,
    interval: Duration,
    retention: &RetentionConfig,
    worker: &WorkerSet,
    live_set: &[Uuid],
    clock: &dyn Clock,
) -> Result<FoldTickReport, MaintainError> {
    let discovered = ravel_maintain::discover_tenants(store).await?;
    let mut owned = Vec::new();
    for tenant in &discovered {
        if worker.owns_unit(live_set, tenant, signal, FOLD_UNIT_SHARD) {
            owned.push(*tenant);
        } else {
            tracing::trace!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                "catalog fold: not this process's unit under the current live set"
            );
        }
    }
    let (maintained, excluded) = restrict_by_lifecycle(store, &owned, fallback_allow).await;
    let mut report = FoldTickReport {
        discovered: discovered.len(),
        owned,
        maintained: maintained.len(),
        excluded,
        ..FoldTickReport::default()
    };

    for tenant in maintained {
        // The deployment-default retention window for this tenant, resolved
        // per tick from the CLI-derived RetentionConfig (ADR-0078). The fold
        // overlays the durable TenantConfig.retention_ns on top of it.
        let default_retention_ns = retention.window_for(&tenant);
        match run_tenant_tick(
            catalog,
            store,
            &tenant,
            signal,
            folder_id,
            interval,
            default_retention_ns,
            clock,
        )
        .await
        {
            TenantTickOutcome::Folded => report.folded.push(tenant),
            TenantTickOutcome::Failed => report.failed.push(tenant),
            TenantTickOutcome::SkippedFresh => report.skipped_fresh.push(tenant),
        }
    }

    Ok(report)
}

/// What one tenant's fold attempt did, so [`run_tick`] can report the exact
/// partition instead of a bare count.
enum TenantTickOutcome {
    Folded,
    Failed,
    SkippedFresh,
}

/// One fold attempt for one tenant this process already owns: the HEAD
/// freshness peek, then [`Catalog::fold`] if it's stale. Split out from
/// [`run_tick`] so discovery, ownership and the per-tenant fold logic stay
/// independently readable.
#[allow(clippy::too_many_arguments)]
async fn run_tenant_tick(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    folder_id: Uuid,
    interval: Duration,
    default_retention_ns: Option<i64>,
    clock: &dyn Clock,
) -> TenantTickOutcome {
    let now_ns = clock.now_ns();
    if head_fresh_enough(store, tenant, signal, interval, now_ns).await {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            "catalog fold: HEAD already fresh, skipping this tick"
        );
        return TenantTickOutcome::SkippedFresh;
    }

    match catalog
        .fold(tenant, signal, folder_id, now_ns, &[], default_retention_ns)
        .await
    {
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
            TenantTickOutcome::Folded
        }
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold failed; the index degrades to listing until a later fold succeeds"
            );
            TenantTickOutcome::Failed
        }
    }
}

/// Adds up to 10% jitter on top of `base`, so multiple replicas' fold tasks
/// (started at roughly the same time) don't tick in lockstep forever. Shared
/// with the store-reachability probe ([`crate::store_probe`]), which reuses this
/// single helper rather than growing a second copy of the same jitter rule.
pub(crate) fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

/// Cheap duplicate-work avoidance across replicas: peeks at HEAD directly
/// (bypassing the
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
    head_is_fresh(head.created_unix_ns, now_ns, interval)
}

/// Whether a HEAD published at `created_unix_ns` is younger than `interval` as
/// of `now_ns`. The freshness predicate behind [`head_fresh_enough`], factored
/// out so the on-demand route ([`crate::fold_on_demand`]) applies the same rule
/// to a HEAD it has already read rather than growing a second freshness notion.
/// A zero `interval` is never fresh (age is always `>= 0`), which is how a
/// caller opts out of the gate.
pub(crate) fn head_is_fresh(created_unix_ns: i64, now_ns: i64, interval: Duration) -> bool {
    let age_ns = now_ns.saturating_sub(created_unix_ns);
    age_ns >= 0 && (age_ns as u128) < interval.as_nanos()
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format):
/// `t/<tenant_hash>/catalog/<signal>/HEAD`. Reconstructed here rather than
/// imported because it names a `pub(crate)` helper inside `ravel-catalog`;
/// this task only ever uses it for the freshness peek above, never to
/// mutate the object, and so does
/// [`crate::fold_on_demand`], which shares this one spelling of the key
/// rather than growing a second copy.
pub(crate) fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}
