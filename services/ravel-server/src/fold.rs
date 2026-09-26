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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::FutureExt;
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

/// Supervisor restarts of the per-signal fold loops, one counter per
/// [`FOLD_SIGNALS`] entry, rendered as
/// `ravel_catalog_fold_loop_restarts_total{signal}`.
///
/// The fold is partitioned across the maintain live set (ADR-1693 decision 1),
/// and that is what makes this counter necessary rather than merely useful. A
/// replica whose loop for one signal dies keeps heartbeating, so it stays in
/// the live set, no peer takes over its pairs, and those pairs stay unfolded.
/// `ravel_catalog_fold_last_success_timestamp_seconds` only moves on a
/// successful [`Catalog::fold`], and the fold-stalled alert aggregates
/// `max by (signal)` across the fleet, so the peers' fresh gauges hold that
/// alert under its threshold while the stranded pairs go unsealed. This
/// counter is the only figure that moves in that state.
#[derive(Debug)]
pub struct FoldLoopMetrics {
    restarts: [AtomicU64; FOLD_SIGNALS.len()],
}

impl Default for FoldLoopMetrics {
    fn default() -> Self {
        FoldLoopMetrics {
            restarts: FOLD_SIGNALS.map(|_| AtomicU64::new(0)),
        }
    }
}

impl FoldLoopMetrics {
    /// Records one supervisor restart of `signal`'s loop. A signal outside
    /// [`FOLD_SIGNALS`] has no counter and is ignored; no loop runs for one.
    fn inc_restart(&self, signal: Signal) {
        if let Some(slot) = FOLD_SIGNALS.iter().position(|entry| *entry == signal) {
            self.restarts[slot].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// This signal's restart tally.
    pub fn restarts_for(&self, signal: Signal) -> u64 {
        FOLD_SIGNALS
            .iter()
            .position(|entry| *entry == signal)
            .map_or(0, |slot| self.restarts[slot].load(Ordering::Relaxed))
    }

    /// Every signal's tally, in [`FOLD_SIGNALS`] order, for the scrape
    /// handler to hand the renderer.
    pub fn restarts(&self) -> [u64; FOLD_SIGNALS.len()] {
        FOLD_SIGNALS.map(|signal| self.restarts_for(signal))
    }
}

/// Backoff before the first restart after a panic. Doubles up to
/// [`RESTART_BACKOFF_MAX`] across consecutive panics, and resets once an
/// attempt completes at least one tick before dying, so a loop that hits a
/// single transient panic restarts promptly while a crash-looping one is
/// bounded rather than spinning. The same pair of bounds the maintenance
/// supervisor uses ([`crate::maintain`]).
const RESTART_BACKOFF_INITIAL: Duration = Duration::from_secs(1);

/// Ceiling for the panic-restart backoff.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);

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
///
/// `loop_metrics` is the process's one [`FoldLoopMetrics`], shared with
/// `/metrics`. Each signal's loop runs under [`run_supervisor`], which catches
/// a panic in the tick body, counts a restart there, and respawns the loop
/// after a bounded backoff.
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
    loop_metrics: Arc<FoldLoopMetrics>,
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
        let ctx = LoopContext {
            catalog: catalog.clone(),
            store: store.clone(),
            signal,
            fallback_allow: fallback_allow.clone(),
            folder_id,
            interval: config.fold_interval,
            rng: Arc::clone(&rng),
            retention: Arc::clone(&retention),
            worker: Arc::clone(&worker),
            live_set: live_set.clone(),
            clock: Arc::clone(&clock),
            // Production has no test seam, so the per-tick hook is a no-op.
            // Tests pass a closure that panics to exercise the supervisor.
            tick_hook: Arc::new(|| {}),
        };
        let handle = tokio::spawn(run_supervisor(
            ctx,
            rx,
            Arc::clone(&loop_metrics),
            RESTART_BACKOFF_INITIAL,
            RESTART_BACKOFF_MAX,
        ));
        shutdown.push(tx);
        handles.push(handle);
    }
    FoldTasks { shutdown, handles }
}

/// Everything one fold-loop attempt needs, bundled so the supervisor can clone
/// it and respawn a fresh attempt after a panic. Every field is cheap to clone
/// (an `Arc`, a `Copy`, or a small owned value), and a fresh attempt carries no
/// state from the one that died: a tick derives the tenant set and the live set
/// from scratch.
#[derive(Clone)]
struct LoopContext {
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
    /// Called once at the top of every tick body, inside the `catch_unwind`
    /// boundary. A no-op in production; a test seam for driving a panic
    /// through the supervisor.
    tick_hook: Arc<dyn Fn() + Send + Sync>,
}

/// Why one supervised [`run_loop`] attempt returned.
enum LoopExit {
    /// The shutdown channel fired: the supervisor must stop, not restart.
    Shutdown,
    /// A tick body panicked and was caught. The supervisor restarts the loop.
    Panicked,
}

/// The outcome of one [`run_loop`] attempt: why it ended, and how many ticks it
/// completed first (so the supervisor can reset its backoff after a healthy
/// run).
struct LoopOutcome {
    exit: LoopExit,
    completed_ticks: u64,
}

/// Owns one signal's fold-loop `JoinHandle` and restarts a fresh attempt after
/// a caught panic, so a panic anywhere in the discovery or fold call graph no
/// longer leaves a Running/Ready maintain replica heartbeating with a dead fold
/// loop for that signal.
///
/// Each attempt is a spawned [`run_loop`] whose tick body is guarded by
/// `catch_unwind`, which returns [`LoopExit::Panicked`] rather than unwinding
/// the task. The supervisor counts the restart on
/// [`FoldLoopMetrics::inc_restart`], logs it at error level with the signal,
/// backs off (bounded, [`RESTART_BACKOFF_INITIAL`]..=[`RESTART_BACKOFF_MAX`])
/// and spawns the next attempt. A join error (a panic that escaped the guard,
/// or an aborted task) is counted and restarted the same way, so the total
/// never undercounts.
///
/// Shutdown stops the loop and never restarts it: on the oneshot the supervisor
/// signals the current attempt, joins it, and returns. The backoff wait races
/// the same receiver, so a drain arriving inside a 60 s backoff is observed at
/// once rather than held behind it.
async fn run_supervisor(
    ctx: LoopContext,
    mut shutdown: oneshot::Receiver<()>,
    metrics: Arc<FoldLoopMetrics>,
    initial_backoff: Duration,
    max_backoff: Duration,
) {
    let signal = ctx.signal;
    let mut backoff = initial_backoff;
    let mut pending_backoff: Option<Duration> = None;
    loop {
        if let Some(wait) = pending_backoff.take() {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }

        let (attempt_tx, attempt_rx) = oneshot::channel();
        let mut attempt = tokio::spawn(run_loop(ctx.clone(), attempt_rx));

        tokio::select! {
            _ = &mut shutdown => {
                let _ = attempt_tx.send(());
                let _ = attempt.await;
                return;
            }
            joined = &mut attempt => {
                match joined {
                    Ok(LoopOutcome { exit: LoopExit::Shutdown, .. }) => return,
                    Ok(LoopOutcome { exit: LoopExit::Panicked, completed_ticks }) => {
                        if completed_ticks > 0 {
                            backoff = initial_backoff;
                        }
                        metrics.inc_restart(signal);
                        tracing::error!(
                            signal = ?signal,
                            completed_ticks,
                            backoff_ms = backoff.as_millis(),
                            "catalog fold: loop task panicked; restarting after backoff \
                             (see ravel_catalog_fold_loop_restarts_total)"
                        );
                    }
                    Err(join_err) => {
                        metrics.inc_restart(signal);
                        tracing::error!(
                            signal = ?signal,
                            error = %join_err,
                            backoff_ms = backoff.as_millis(),
                            "catalog fold: loop task died outside the tick guard; restarting \
                             after backoff (see ravel_catalog_fold_loop_restarts_total)"
                        );
                    }
                }

                pending_backoff = Some(backoff);
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// One supervised attempt of one signal's fold loop. Ticks until either the
/// shutdown channel fires (returns [`LoopExit::Shutdown`]) or a tick body
/// panics and is caught (returns [`LoopExit::Panicked`]). The supervisor
/// ([`run_supervisor`]) owns this task's handle and restarts it on a panic.
async fn run_loop(ctx: LoopContext, mut shutdown: oneshot::Receiver<()>) -> LoopOutcome {
    let mut completed_ticks: u64 = 0;
    let exit = loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(ctx.interval, ctx.rng.as_ref())) => {}
            _ = &mut shutdown => break LoopExit::Shutdown,
        }

        // The whole tick body runs inside `catch_unwind` so a panic anywhere in
        // the discovery or fold call graph is caught here and turned into a
        // supervised restart rather than a silently dead loop on a replica that
        // keeps heartbeating and so keeps its pairs. `AssertUnwindSafe` is
        // honest: on a caught panic this attempt is discarded entirely and the
        // supervisor spawns a fresh one, which re-derives both the tenant set
        // and the live set from scratch.
        let tick = AssertUnwindSafe(async {
            // Test seam (a no-op in production).
            (ctx.tick_hook)();

            // The latest set the maintenance heartbeat task published. Read
            // once per tick so every pair in one tick is partitioned against
            // one membership view, the same way the maintenance discovery
            // cycle reads it (ADR-0065 decision 2).
            let live = ctx.live_set.borrow().clone();
            match run_tick(
                ctx.catalog.as_ref(),
                ctx.store.as_ref(),
                ctx.signal,
                ctx.fallback_allow.as_deref(),
                ctx.folder_id,
                ctx.interval,
                ctx.retention.as_ref(),
                ctx.worker.as_ref(),
                &live,
                ctx.clock.as_ref(),
            )
            .await
            {
                Ok(report) => {
                    if report.excluded > 0 {
                        tracing::debug!(
                            signal = ?ctx.signal,
                            excluded = report.excluded,
                            "catalog fold: flag restriction excluded discovered tenants"
                        );
                    }
                    tracing::debug!(
                        signal = ?ctx.signal,
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
                        signal = ?ctx.signal,
                        error = %err,
                        "catalog fold: tenant discovery failed; skipping this cycle entirely, retried next tick"
                    );
                }
            }
        });

        match tick.catch_unwind().await {
            Ok(()) => completed_ticks = completed_ticks.saturating_add(1),
            Err(_panic) => break LoopExit::Panicked,
        }
    };

    LoopOutcome {
        exit,
        completed_ticks,
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use bytes::Bytes;
    use ravel_catalog::CatalogConfig;
    use ravel_maintain::FixedClock;
    use ravel_maintain::worker_set::{DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::PutOptions;
    use ravel_types::TenantId;

    use super::*;

    /// The injected clock's one value. Every fold in these tests stamps this
    /// exact number, so a stamp taken from a second clock fails rather than
    /// landing in a band.
    const NOW_NS: i64 = 1_700_000_000_000_000_000;

    /// The heartbeat cadence the `WorkerSet` is built with. Irrelevant to the
    /// partition here (the solo live set owns every unit) and named only
    /// because the constructor takes one.
    const HEARTBEAT: Duration = Duration::from_secs(60);

    /// One tick's interval. Larger than the [`advance_until`] step below, so a
    /// single step can never carry the paused clock across two ticks and the
    /// exact cycle counts these tests assert stay exact.
    const TEST_INTERVAL: Duration = Duration::from_secs(1);

    /// Advances the paused clock in small steps until `pred` holds or the step
    /// budget runs out, returning whether it held. Small steps so the paused
    /// runtime actually wakes the spawned loop between each. Mirrors the
    /// maintenance supervisor's test helper: every wait in these tests is
    /// bounded and driven by state, never by a fixed sleep.
    async fn advance_until(steps: usize, step: Duration, pred: impl Fn() -> bool) -> bool {
        for _ in 0..steps {
            if pred() {
                return true;
            }
            tokio::time::advance(step).await;
            tokio::task::yield_now().await;
        }
        pred()
    }

    /// A store holding one tenant prefix, and a `Catalog` over it. The tenant
    /// carries no commit records: the fold over it is a no-op cycle, which is
    /// the healthy steady state and still records a successful fold, so it is
    /// enough to prove a restarted loop is folding again.
    async fn seeded_store() -> (Arc<dyn ObjectStoreBackend>, Arc<Catalog>, TenantHash) {
        let inner = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("fold-loop-supervision").hash();
        inner
            .put(
                &format!("t/{}/marker", tenant.to_hex()),
                Bytes::from_static(b"x"),
                PutOptions::default(),
            )
            .await
            .expect("seed the tenant prefix discovery lists");
        let store: Arc<dyn ObjectStoreBackend> = inner;
        let catalog = Arc::new(
            Catalog::new(
                store.clone(),
                CatalogConfig {
                    shard_count: 1,
                    ..CatalogConfig::default()
                },
            )
            .expect("catalog builds"),
        );
        (store, catalog, tenant)
    }

    /// One signal's loop context over [`seeded_store`], with the test's panic
    /// seam installed. The live set is this process's solo set, so the one
    /// seeded tenant is always owned and the partition is not what these tests
    /// are about.
    fn loop_context(
        catalog: Arc<Catalog>,
        store: Arc<dyn ObjectStoreBackend>,
        tenant: TenantHash,
        tick_hook: Arc<dyn Fn() + Send + Sync>,
    ) -> LoopContext {
        let worker = Arc::new(WorkerSet::new(
            NOW_NS,
            HEARTBEAT,
            DEFAULT_LIVENESS_FACTOR,
            DEFAULT_UNIT_CONCURRENCY,
        ));
        let live_set = watch::channel(worker.solo_live_set()).0.subscribe();
        LoopContext {
            catalog,
            store,
            signal: Signal::Metrics,
            fallback_allow: Some(vec![tenant]),
            folder_id: Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0011),
            interval: TEST_INTERVAL,
            rng: Arc::new(SystemRng),
            retention: Arc::new(RetentionConfig::default()),
            worker,
            live_set,
            clock: Arc::new(FixedClock::new(NOW_NS)),
            tick_hook,
        }
    }

    /// A panic in one signal's tick body is caught, counted exactly once on
    /// `ravel_catalog_fold_loop_restarts_total{signal="metrics"}`, and the
    /// supervisor restarts the loop so a later tick folds again.
    ///
    /// This is the blocker ADR-1693 opened. Before the fold was partitioned, a
    /// panic killed every replica's loop for that signal and the fold-stalled
    /// alert fired. Now the panicking replica keeps heartbeating, so it keeps
    /// its pairs while its peers hold `max by (signal)` fresh, and nothing
    /// reports it.
    ///
    /// Flip to watch it fail against pre-fix code: spawn `run_loop(ctx, rx)`
    /// here instead of `run_supervisor(...)` and drop the `catch_unwind` in
    /// `run_loop`'s tick arm. The injected panic then ends the task, the
    /// restart counter never leaves zero, and no tick ever folds.
    #[tokio::test(start_paused = true)]
    async fn a_caught_panic_restarts_the_loop_and_a_later_tick_folds() {
        let (store, catalog, tenant) = seeded_store().await;
        let metrics = Arc::new(FoldLoopMetrics::default());

        // Panic on the first tick only; every later tick runs normally. The
        // supervisor clones the context (and this shared flag) into each
        // attempt, so the second attempt sees the flag already consumed.
        let panic_armed = Arc::new(AtomicBool::new(true));
        let hook_flag = Arc::clone(&panic_armed);
        let tick_hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if hook_flag.swap(false, Ordering::SeqCst) {
                panic!("injected catalog fold loop panic (test)");
            }
        });

        let ctx = loop_context(catalog.clone(), store, tenant, tick_hook);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        // Small, fixed backoff so the paused-time advance drives the restart.
        let handle = tokio::spawn(run_supervisor(
            ctx,
            shutdown_rx,
            Arc::clone(&metrics),
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));

        let folded = advance_until(600, Duration::from_millis(100), || {
            catalog.fold_cycles(Signal::Metrics) >= 1
        })
        .await;
        assert!(
            folded,
            "the supervisor must restart the loop and a later tick must fold"
        );
        assert!(
            !panic_armed.load(Ordering::SeqCst),
            "the one-shot panic must have fired"
        );
        assert_eq!(
            metrics.restarts_for(Signal::Metrics),
            1,
            "exactly one restart was counted, not zero (an uncounted death) and not a \
             restart loop"
        );
        assert_eq!(
            metrics.restarts_for(Signal::Logs),
            0,
            "the counter is per signal: only the loop that panicked restarted"
        );
        assert_eq!(
            catalog.fold_cycles(Signal::Metrics),
            1,
            "the restarted loop folded on its next tick"
        );
        assert_eq!(
            catalog.fold_last_success_unix_ns(Signal::Metrics),
            NOW_NS,
            "the fold after the restart stamps the liveness gauge from the injected clock"
        );

        shutdown_tx.send(()).expect("send shutdown");
        let _ = handle.await;
    }

    /// Shutdown after a panic stops the loop and does not respawn it, and it is
    /// observed DURING the restart backoff rather than after it. Every tick
    /// panics here, so the supervisor is always inside its backoff when the
    /// drain arrives; the backoff is 60 s and the drain deadline is 5 s, both on
    /// the paused runtime's virtual clock, so a supervisor that waited the
    /// backoff out would miss the deadline rather than merely be slow.
    ///
    /// Flip to watch it fail against pre-fix code: spawn `run_loop(ctx, rx)`
    /// here instead of `run_supervisor(...)` and drop the `catch_unwind` in
    /// `run_loop`'s tick arm. The restart counter never reaches one, so the
    /// bounded wait below times out.
    #[tokio::test(start_paused = true)]
    async fn shutdown_after_a_panic_does_not_respawn_the_loop() {
        const BACKOFF: Duration = Duration::from_secs(60);

        let (store, catalog, tenant) = seeded_store().await;
        let metrics = Arc::new(FoldLoopMetrics::default());
        let tick_hook: Arc<dyn Fn() + Send + Sync> =
            Arc::new(|| panic!("injected catalog fold loop panic (test)"));

        let ctx = loop_context(catalog.clone(), store, tenant, tick_hook);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(run_supervisor(
            ctx,
            shutdown_rx,
            Arc::clone(&metrics),
            BACKOFF,
            BACKOFF,
        ));

        let panicked = advance_until(600, Duration::from_millis(100), || {
            metrics.restarts_for(Signal::Metrics) >= 1
        })
        .await;
        assert!(panicked, "the first attempt must panic and be counted");

        shutdown_tx.send(()).expect("send shutdown");
        let drained = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            drained.is_ok(),
            "a drain arriving during the {BACKOFF:?} backoff must be observed at once, not \
             held behind it"
        );
        drained.expect("deadline").expect("supervisor joins");

        assert_eq!(
            metrics.restarts_for(Signal::Metrics),
            1,
            "no further attempt is spawned once shutdown arrives, so the restart total stays \
             at the one attempt that ran"
        );
        assert_eq!(
            catalog.fold_cycles(Signal::Metrics),
            0,
            "every tick panicked, so nothing was ever folded"
        );
    }
}
