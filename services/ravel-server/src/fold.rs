//! Per-signal background catalog fold task (ADR-0020; storage-derived tenant
//! set is ADR-0048 decision 3). Periodically calls [`Catalog::fold`] so query resolve can
//! serve sealed history from snapshots instead of full listing.
//!
//! Never runs on the ingest or query path, and never affects correctness:
//! every failure here is logged and retried on the next tick. Disabling this
//! task (`--disable-fold`) only changes query cost, never query results.
//!
//! One loop per [`FOLD_SIGNALS`] entry, not one per tenant: each tick
//! re-enumerates tenants from storage
//! ([`crate::tenant_discovery::discover_and_restrict_by_lifecycle`]) and folds
//! every tenant the cycle discovers, narrowed by each tenant's durable
//! lifecycle state and the flag fallback (ADR-0066 decision 6: a config record
//! keeps a tenant in the set unconditionally regardless of its token, and no
//! flag can exclude a config-recorded tenant). A tenant onboarded mid-run is
//! folded starting the next tick with no restart.
//! A discovery failure skips that signal's whole cycle -- no tenant is folded
//! -- and is retried next tick; it never falls back to an empty set, since
//! fold is best-effort and a quiet failure here would look identical to
//! "nothing new to fold."

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, FoldReport, RefoldRequest};
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_ingest::{Clock, SystemClock};
use ravel_maintain::{CompactorConfig, NoLeases, RetentionConfig};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::discover_and_restrict_by_lifecycle;

/// The tick's own instant, as the [`ravel_maintain::Clock`] the sweep-side
/// derivation takes. Every fold entry point already receives `now_ns` as a
/// parameter (CLAUDE.md testing patterns: time is injected), so the derivation
/// sees that instant rather than reading the wall clock a second time.
struct TickClock(i64);

impl ravel_maintain::Clock for TickClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

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
/// `compactor` is the same [`CompactorConfig`] the Maintain-mode sweep runs
/// under. Each tick derives this tenant's blocked ingest hours from the store
/// with it ([`derive_refold_request`]) and passes them to
/// [`Catalog::fold_with_refold_request`], so the targeted reconcile pass
/// re-reads hours that sit outside the fixed reconcile window.
pub fn spawn(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: &[TenantHash],
    config: FoldTaskConfig,
    retention: Arc<RetentionConfig>,
    compactor: Arc<CompactorConfig>,
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
        let compactor = Arc::clone(&compactor);
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
                compactor,
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
    compactor: Arc<CompactorConfig>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }

        let outcome = match discover_and_restrict_by_lifecycle(
            store.as_ref(),
            fallback_allow.as_deref(),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::error!(
                    signal = ?signal,
                    error = %err,
                    "catalog fold: tenant discovery failed; skipping this cycle entirely, retried next tick"
                );
                continue;
            }
        };
        if outcome.excluded > 0 {
            tracing::debug!(
                signal = ?signal,
                excluded = outcome.excluded,
                "catalog fold: flag restriction excluded discovered tenants"
            );
        }

        for tenant in outcome.maintained {
            // The deployment-default retention window for this tenant, resolved
            // per tick from the CLI-derived RetentionConfig (ADR-0078). The fold
            // overlays the durable TenantConfig.retention_ns on top of it.
            let default_retention_ns = retention.window_for(&tenant);
            run_tenant_tick(
                catalog.as_ref(),
                store.as_ref(),
                compactor.as_ref(),
                &tenant,
                signal,
                folder_id,
                interval,
                SystemClock.now_ns(),
                default_retention_ns,
            )
            .await;
        }
    }
}

/// The ingest hours this tenant's fold should re-list: the hours whose live
/// catalog HEAD snapshot still names inputs a published compaction or rewrite
/// record superseded, so the ADR-0020 delete blocker holds those inputs
/// ([`ravel_maintain::SnapshotBlock::Named`]).
///
/// Derived here, from the store, at the fold's own tick. Nothing hands this
/// set to the fold: in every shipped topology a process runs EITHER the
/// maintain loop or the fold loop (`Mode::Maintain` in `crate::lib` spawns one
/// and passes the other's `none()` handle), so an in-process hand-off from the
/// sweep would never fire.
///
/// [`ravel_maintain::blocked_named_hours`] is the derivation, and it is the
/// same pass and the same `SnapshotBlock::Named` gate arm that fills
/// `SweepReport::blocked_named_hours` on the maintain side. The two cannot
/// drift on what "blocked" means because there is one function.
///
/// Cost, per tenant per fold tick: one LIST of the signal's commit prefix to
/// enumerate shards, then one LIST plus the record GETs per shard, with a
/// single HEAD read shared across all of them. It is paid only after the HEAD
/// freshness peek has decided this tick folds at all.
///
/// Best-effort, like every other part of this task: a derivation failure logs
/// and yields an empty request rather than skipping the fold. The hours stay
/// blocked in the store and the next tick derives them again.
async fn derive_refold_request(
    store: &dyn ObjectStoreBackend,
    compactor: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
    now_ns: i64,
) -> RefoldRequest {
    // `NoLeases`: this process holds no reader lease or legal hold of its own.
    // A group a real hold protects can therefore be derived here and not by
    // the sweep, which only ever adds an hour to re-list; see
    // `ravel_maintain::blocked_named_hours`.
    match ravel_maintain::blocked_named_hours(
        store,
        &TickClock(now_ns),
        compactor,
        &NoLeases,
        tenant,
        signal,
    )
    .await
    {
        Ok(hours) => RefoldRequest::from_hours(hours),
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold: blocked-hour derivation failed; folding without a targeted \
                 re-fold pass this tick"
            );
            RefoldRequest::new()
        }
    }
}

/// One fold attempt for one tenant: the HEAD freshness peek, the blocked-hour
/// derivation, then [`Catalog::fold_with_refold_request`]. Split out from
/// [`run_loop`] so discovery and the per-tenant fold logic stay independently
/// readable.
///
/// The derivation runs after the freshness peek, never before: a HEAD another
/// replica folded within the last interval has already run its own targeted
/// pass over the same store state, and the LISTs would be spent on a fold that
/// does not happen.
///
/// `now_ns` is the caller's clock reading, taken once per tenant by
/// [`run_loop`] from [`SystemClock`]. Taking it as a parameter is what lets a
/// test fold at a determined instant (CLAUDE.md testing patterns: time is
/// injected) instead of at whatever the host's wall clock says, which for a
/// fixture built on low ingest hours decides whether anything is sealed at
/// all.
///
/// Returns the fold's own [`FoldReport`], or `None` when the freshness peek
/// skipped this tick or the fold itself failed. [`run_loop`] logs and
/// discards it; the re-fold tests assert
/// [`FoldReport::refold_hours_reconciled`] on it.
///
/// `pub(crate)` so those tests fold through this exact function, deriving
/// their own blocked hours, rather than calling the catalog directly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tenant_tick(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    compactor: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
    folder_id: Uuid,
    interval: Duration,
    now_ns: i64,
    default_retention_ns: Option<i64>,
) -> Option<FoldReport> {
    if head_fresh_enough(store, tenant, signal, interval, now_ns).await {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            "catalog fold: HEAD already fresh, skipping this tick"
        );
        return None;
    }

    let refold_request = derive_refold_request(store, compactor, tenant, signal, now_ns).await;

    match catalog
        .fold_with_refold_request(
            tenant,
            signal,
            folder_id,
            now_ns,
            &[],
            default_retention_ns,
            &refold_request,
        )
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
                refold_hours_requested = refold_request.len(),
                refold_hours_reconciled = report.refold_hours_reconciled,
                "catalog fold complete"
            );
            Some(report)
        }
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold failed; the index degrades to listing until a later fold succeeds"
            );
            None
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
