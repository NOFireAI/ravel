//! Per-signal background catalog fold task (ADR-0020; storage-derived tenant
//! set is ADR-0048 decision 3). Periodically calls
//! [`Catalog::fold_with_refold_request`] so query resolve can serve sealed
//! history from snapshots instead of full listing, passing each tenant the
//! hours the maintain loop's sweeps queued on the [`RefoldQueue`].
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
//! catalog objects, and every shard of the pair, is the process that folds it.
//! The gate is pure computation
//! over the discovery result and it runs before every per-tenant read, the
//! lifecycle config record included, so a pair this process does not own costs
//! zero requests. The per-tick cost of a pair nobody on this process owns is
//! therefore its share of the one `t/` listing and nothing else.
//! In `--mode all` the live set is the solo one (`{self}`), every pair is
//! owned, and the behavior is byte-for-byte the unpartitioned fold
//! (ADR-1693 decision 3).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::FutureExt;
use ravel_catalog::{Catalog, RefoldRequest};
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_maintain::{Clock, MaintainError, RetentionConfig, WorkerSet};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::config::Mode;
use crate::tenant_discovery::restrict_by_lifecycle;

/// Default `fold_interval`: 5 minutes.
pub const DEFAULT_FOLD_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy)]
pub struct FoldTaskConfig {
    pub enabled: bool,
    /// Pause between fold passes (`--fold-interval-secs`). A zero interval is
    /// refused at startup with [`SpawnError::ZeroFoldInterval`], whether or not
    /// the loop is enabled: `validate_loop_intervals` checks it regardless of
    /// `enabled` because the interval also feeds the on-demand fold rate gate
    /// and the fold-lag threshold. [`check_spawnable`](Self::check_spawnable)
    /// re-refuses it at the enabled loop's spawn site.
    pub fold_interval: Duration,
}

/// Why [`spawn`] refused to start the fold loops. Nothing is spawned.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// A zero `fold_interval` would run every fold pass back to back.
    #[error(
        "--fold-interval-secs must be non-zero: a zero fold interval runs every pass back to back"
    )]
    ZeroFoldInterval,
}

impl FoldTaskConfig {
    /// The refusal [`spawn`] applies before starting any loop. `start` refuses
    /// a zero interval earlier, before spawning anything, whether or not the
    /// loop is enabled.
    pub fn check_spawnable(&self) -> Result<(), SpawnError> {
        if self.enabled && self.fold_interval.is_zero() {
            return Err(SpawnError::ZeroFoldInterval);
        }
        Ok(())
    }
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
/// per-signal maintenance sweeps already use ([`crate::maintain`]). The owner
/// of this shard also runs the sweep pass of every shard of the pair, owned or
/// not, while each shard's retention and compaction stay with that shard's
/// owner (ADR-1693, the 2026-10-07 sweep ownership amendment). So the process
/// whose superseded-input sweep finds a named-snapshot hold on any shard of
/// the pair is the process that folds it, and the hold reaches the fold
/// through [`RefoldQueue`].
pub const FOLD_UNIT_SHARD: u32 = 0;

/// Default [`RefoldQueue`] capacity, in `(tenant, signal)` pairs.
pub const DEFAULT_REFOLD_QUEUE_CAPACITY: usize = 256;

/// The most hours one [`RefoldQueue`] entry holds. A merge past it keeps the
/// smallest hours, since the catalog reconciles a request oldest first.
pub const REFOLD_ENTRY_HOURS_MAX: usize = 1024;

/// One pair's pending hours. `seq` is the order the pair was inserted in; a
/// merge into an existing entry keeps it.
#[derive(Debug)]
struct RefoldEntry {
    seq: u64,
    hours: BTreeSet<u32>,
}

#[derive(Debug, Default)]
struct RefoldPending {
    next_seq: u64,
    entries: HashMap<(TenantHash, Signal), RefoldEntry>,
}

/// The in-process hand-off from the maintain loop's superseded-input sweep to
/// the scheduled fold of the same `(tenant, signal)` pair (ADR-0063 section
/// 4): the ingest hours a sweep held because the live catalog HEAD still names
/// their superseded inputs ([`ravel_maintain::SweepReport::blocked_named_hours`]).
///
/// The queue holds one entry per pair, and a send for a pair already queued
/// merges its hours into that entry. The fold tick reads at most the catalog's
/// per-fold cap (`frontier_reconcile_max_hours`) of a pair's oldest hours with
/// [`Self::peek`], passes them to [`Catalog::fold_with_refold_request`], and
/// removes those hours with [`Self::remove_hours`] only after a fold that
/// returned `Ok` and actually reconciled them: on `!no_op` (a first fold, a
/// rebuild, or an ordinary advancing fold) unconditionally, since all three
/// derive every hour they touch from the commit layout; on `no_op` only when
/// `report.refold_hours_reconciled > 0`. `no_op` alone is not the removal
/// signal: the held-open-only path runs the targeted re-fold pass before
/// deciding `no_op` and can still report `no_op: true` when nothing was newly
/// sealed, so a request can be fully reconciled on a no-op fold. A fresh
/// skip, a failed fold, and a no-op fold that never reached the targeted pass
/// for these hours (no held-open window covering them) all leave the request
/// queued, and the hours past the cap stay queued for the pair's next fold
/// that reconciles them.
///
/// A queued hour is a hint, never a durability dependency. The queue lives in
/// memory and is bounded at `capacity` pairs: a send for a new pair that finds
/// it full evicts the pair inserted earliest (a merge does not make a pair
/// younger) and counts it in [`Self::dropped_requests`]. One entry holds at
/// most [`REFOLD_ENTRY_HOURS_MAX`] hours, the smallest ones, and the hours cut
/// past that are not counted. An evicted pair, hours past the entry cap, and
/// an entry lost with the process all cost the same thing: the sweep
/// re-derives its blocked set on each pass, so a still-held hour is sent again
/// by the next sweep that finds it.
#[derive(Debug)]
pub struct RefoldQueue {
    capacity: usize,
    pending: Mutex<RefoldPending>,
    dropped: AtomicU64,
}

impl Default for RefoldQueue {
    fn default() -> Self {
        Self::new(DEFAULT_REFOLD_QUEUE_CAPACITY)
    }
}

impl RefoldQueue {
    /// A queue holding at most `capacity` pairs; a zero capacity holds one.
    pub fn new(capacity: usize) -> Self {
        RefoldQueue {
            capacity: capacity.max(1),
            pending: Mutex::new(RefoldPending::default()),
            dropped: AtomicU64::new(0),
        }
    }

    /// Queues `hours` for the next fold of `(tenant, signal)` that acts on
    /// them. An empty set is ignored. Hours for a pair already queued are
    /// merged into its entry. A new pair that finds the queue full evicts the
    /// pair inserted earliest and counts it in [`Self::dropped_requests`].
    pub fn send(&self, tenant: TenantHash, signal: Signal, hours: BTreeSet<u32>) {
        if hours.is_empty() {
            return;
        }
        let mut pending = self.lock();
        let RefoldPending { next_seq, entries } = &mut *pending;
        if let Some(entry) = entries.get_mut(&(tenant, signal)) {
            entry.hours.extend(hours);
            cap_entry_hours(&mut entry.hours);
            return;
        }
        while entries.len() >= self.capacity {
            let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.seq)
                .map(|(key, _)| *key)
            else {
                break;
            };
            entries.remove(&oldest);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        let mut hours = hours;
        cap_entry_hours(&mut hours);
        entries.insert(
            (tenant, signal),
            RefoldEntry {
                seq: *next_seq,
                hours,
            },
        );
        *next_seq += 1;
    }

    /// At most `limit` of the hours pending for `(tenant, signal)`, the
    /// smallest ones, copied out and left queued. Empty when nothing is
    /// pending.
    pub fn peek(&self, tenant: &TenantHash, signal: Signal, limit: usize) -> RefoldRequest {
        self.lock()
            .entries
            .get(&(*tenant, signal))
            .map(|entry| RefoldRequest::from_hours(entry.hours.iter().copied().take(limit)))
            .unwrap_or_default()
    }

    /// Removes `taken` from the pair's pending hours, and the entry once no
    /// hour is left. An hour sent after the [`Self::peek`] that produced
    /// `taken` stays queued.
    pub fn remove_hours(&self, tenant: &TenantHash, signal: Signal, taken: &RefoldRequest) {
        let mut pending = self.lock();
        let key = (*tenant, signal);
        let Some(entry) = pending.entries.get_mut(&key) else {
            return;
        };
        for hour in taken.hours() {
            entry.hours.remove(&hour);
        }
        if entry.hours.is_empty() {
            pending.entries.remove(&key);
        }
    }

    /// Removes every `signal` entry whose tenant fails `keep`, returning each
    /// removed tenant and its hour count. Not counted as dropped.
    pub fn remove_unless(
        &self,
        signal: Signal,
        keep: impl Fn(&TenantHash) -> bool,
    ) -> Vec<(TenantHash, usize)> {
        let mut removed = Vec::new();
        self.lock().entries.retain(|(tenant, entry_signal), entry| {
            if *entry_signal != signal || keep(tenant) {
                return true;
            }
            removed.push((*tenant, entry.hours.len()));
            false
        });
        removed
    }

    /// Pairs evicted because the queue was full, since the process started.
    pub fn dropped_requests(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Pairs currently queued, across every signal.
    pub fn pending_len(&self) -> usize {
        self.lock().entries.len()
    }

    /// A poisoned lock still guards a well-formed queue, at worst missing an
    /// update a panicking caller was making, and a lost entry is only a lost
    /// hint, so the contents are used as they are.
    fn lock(&self) -> MutexGuard<'_, RefoldPending> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Trims `hours` to its [`REFOLD_ENTRY_HOURS_MAX`] smallest.
fn cap_entry_hours(hours: &mut BTreeSet<u32>) {
    while hours.len() > REFOLD_ENTRY_HOURS_MAX {
        hours.pop_last();
    }
}

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
    /// The `folded` tenants whose fold was a no-op (the watermark did not
    /// advance), so their pending re-fold hours stayed queued.
    pub no_op: Vec<TenantHash>,
    /// Maintained tenants whose fold returned an error. Logged and retried
    /// next tick; never fails a query.
    pub failed: Vec<TenantHash>,
    /// Maintained tenants skipped because their HEAD was younger than the fold
    /// interval, the cheap duplicate-work peek.
    pub skipped_fresh: Vec<TenantHash>,
    /// Ingest hours the folds of this tick re-listed from a [`RefoldQueue`]
    /// request, summed over `folded` ([`ravel_catalog::FoldReport::refold_hours_reconciled`]).
    pub refold_hours_reconciled: usize,
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
/// [`FoldTasks::shutdown`]. An enabled config with a zero `fold_interval` is
/// refused with [`SpawnError::ZeroFoldInterval`] and nothing is spawned.
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
///
/// `refold` is the process's one [`RefoldQueue`], fed by the maintain loop's
/// sweeps ([`crate::maintain::spawn`]). Each signal's tick folds each
/// maintained tenant with at most the catalog's per-fold cap of that pair's
/// oldest pending hours ([`run_tick`]). In `Mode::All` no maintain loop runs,
/// nothing feeds the queue, and every request is empty; a maintain process
/// whose fold is disabled spawns nothing here and hands its sweeps no queue
/// ([`refold_queue_for_maintain`]).
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
    refold: Arc<RefoldQueue>,
) -> Result<FoldTasks, SpawnError> {
    config.check_spawnable()?;
    if !config.enabled {
        return Ok(FoldTasks::none());
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
            refold: Arc::clone(&refold),
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
    Ok(FoldTasks { shutdown, handles })
}

/// The queue [`crate::maintain::spawn`] sends its sweeps' held hours to:
/// `queue` when this process spawns the scheduled fold loops that take from
/// it (`mode` runs the scheduled fold and `config` enables it, the same
/// condition [`spawn`] starts them under), otherwise `None`, so a process
/// with no fold to take a request queues nothing.
pub fn refold_queue_for_maintain(
    mode: Mode,
    config: &FoldTaskConfig,
    queue: &Arc<RefoldQueue>,
) -> Option<Arc<RefoldQueue>> {
    (mode.runs_scheduled_fold() && config.enabled).then(|| Arc::clone(queue))
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
    refold: Arc<RefoldQueue>,
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
/// and spawns the next attempt, which ticks as soon as the backoff ends rather
/// than after a further interval. At the defaults a loop that panics on every
/// tick therefore restarts at 0, 1, 3, 7, 15, 31, 63 and 123 s after its first
/// panic and every 60 s after that, which is the rate
/// `RavelCatalogFoldLoopCrashLooping` is sized against. A join error (a panic that escaped the guard,
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
    let mut restarted = false;
    loop {
        if let Some(wait) = pending_backoff.take() {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }

        // Only a restarted attempt ticks at once: the first attempt waits one
        // interval as at startup, and a restart that waited the interval too
        // would bound the restart rate by the interval rather than the backoff.
        let first_tick_immediate = restarted;
        restarted = true;
        let (attempt_tx, attempt_rx) = oneshot::channel();
        let mut attempt = tokio::spawn(run_loop(ctx.clone(), attempt_rx, first_tick_immediate));

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
///
/// Each tick is preceded by a jittered `interval` sleep, except the first when
/// `first_tick_immediate` is set, which is how a restarted attempt runs: its
/// backoff already stood in for that wait.
async fn run_loop(
    ctx: LoopContext,
    mut shutdown: oneshot::Receiver<()>,
    first_tick_immediate: bool,
) -> LoopOutcome {
    let mut completed_ticks: u64 = 0;
    let mut skip_sleep = first_tick_immediate;
    let exit = loop {
        if !std::mem::take(&mut skip_sleep) {
            tokio::select! {
                _ = tokio::time::sleep(jittered(ctx.interval, ctx.rng.as_ref())) => {}
                _ = &mut shutdown => break LoopExit::Shutdown,
            }
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
                ctx.refold.as_ref(),
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
                        refold_hours_reconciled = report.refold_hours_reconciled,
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
/// Before folding any tenant the tick removes from `refold` every entry of
/// this signal whose tenant it does not maintain this tick: the process does
/// not own shard 0 of the pair under `live_set`, or the tenant is not among
/// the discovered, lifecycle-filtered tenants it is about to fold. Each
/// removal is logged at debug and not counted as dropped, so a pair whose
/// ownership moved away, or a tenant deleted after a send, does not pin a
/// queue slot. Each maintained tenant is then folded with at most the
/// catalog's `frontier_reconcile_max_hours` of its oldest pending hours, read
/// with [`RefoldQueue::peek`] and left queued (empty when none is pending),
/// since the catalog reconciles no more than that per fold. Those hours are
/// removed with [`RefoldQueue::remove_hours`] only once the fold result shows
/// they were actually reconciled: on a `!no_op` fold (a first fold, a
/// rebuild, or an ordinary advancing fold) unconditionally, since all three
/// derive every hour they touch from the commit layout; on a `no_op` fold
/// only when `refold_hours_reconciled > 0`, since the held-open-only path
/// runs the targeted re-fold pass before deciding `no_op` and can fully
/// reconcile a request while writing nothing new. A failed fold, a fresh
/// skip, or a no-op fold with `refold_hours_reconciled == 0` leaves the
/// request queued for the next tick, and the hours past the cap stay queued
/// for the pair's next fold that reconciles them.
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
    refold: &RefoldQueue,
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
    let maintained_set: HashSet<TenantHash> = maintained.iter().copied().collect();
    for (tenant, hours) in refold.remove_unless(signal, |tenant| maintained_set.contains(tenant)) {
        if worker.owns_unit(live_set, &tenant, signal, FOLD_UNIT_SHARD) {
            tracing::debug!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                hours,
                "catalog fold: removing a re-fold request for a tenant this tick does not maintain"
            );
        } else {
            tracing::debug!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                hours,
                "catalog fold: removing a re-fold request for a pair this process no longer folds"
            );
        }
    }
    let mut report = FoldTickReport {
        discovered: discovered.len(),
        owned,
        maintained: maintained.len(),
        excluded,
        ..FoldTickReport::default()
    };

    // The catalog reconciles at most this many requested hours per fold, oldest
    // first, so the tick takes no more than that and the rest stay queued.
    let refold_take =
        usize::try_from(catalog.config().frontier_reconcile_max_hours).unwrap_or(usize::MAX);
    for tenant in maintained {
        // The deployment-default retention window for this tenant, resolved
        // per tick from the CLI-derived RetentionConfig (ADR-0078). The fold
        // overlays the durable TenantConfig.retention_ns on top of it.
        let default_retention_ns = retention.window_for(&tenant);
        let refold_request = refold.peek(&tenant, signal, refold_take);
        match run_tenant_tick(
            catalog,
            store,
            &tenant,
            signal,
            folder_id,
            interval,
            default_retention_ns,
            clock,
            &refold_request,
        )
        .await
        {
            TenantTickOutcome::Folded {
                no_op,
                refold_hours_reconciled,
            } => {
                // `!no_op` (a first fold, a rebuild, or an ordinary advancing
                // fold) derives every hour it touches from the commit layout,
                // so the request is reconciled unconditionally. A `no_op`
                // fold only reconciled the request if the held-open-only
                // path's targeted re-fold pass actually ran over it
                // (`refold_hours_reconciled > 0`); otherwise this fold never
                // reached these hours and the request must stay queued.
                if no_op {
                    report.no_op.push(tenant);
                    if refold_hours_reconciled > 0 && !refold_request.is_empty() {
                        refold.remove_hours(&tenant, signal, &refold_request);
                    }
                } else if !refold_request.is_empty() {
                    refold.remove_hours(&tenant, signal, &refold_request);
                }
                report.folded.push(tenant);
                report.refold_hours_reconciled += refold_hours_reconciled;
            }
            TenantTickOutcome::Failed => report.failed.push(tenant),
            TenantTickOutcome::SkippedFresh => report.skipped_fresh.push(tenant),
        }
    }

    Ok(report)
}

/// What one tenant's fold attempt did, so [`run_tick`] can report the exact
/// partition instead of a bare count.
enum TenantTickOutcome {
    Folded {
        no_op: bool,
        refold_hours_reconciled: usize,
    },
    Failed,
    SkippedFresh,
}

/// One fold attempt for one tenant this process already owns: the HEAD
/// freshness peek, then [`Catalog::fold_with_refold_request`] if it's stale.
/// Split out from [`run_tick`] so discovery, ownership and the per-tenant fold
/// logic stay independently readable.
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
    refold_request: &RefoldRequest,
) -> TenantTickOutcome {
    let now_ns = clock.now_ns();
    if head_fresh_enough(store, tenant, signal, interval, now_ns).await {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            refold_hours_pending = refold_request.len(),
            "catalog fold: HEAD already fresh, skipping this tick"
        );
        return TenantTickOutcome::SkippedFresh;
    }

    match catalog
        .fold_with_refold_request(
            tenant,
            signal,
            folder_id,
            now_ns,
            &[],
            default_retention_ns,
            refold_request,
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
            TenantTickOutcome::Folded {
                no_op: report.no_op,
                refold_hours_reconciled: report.refold_hours_reconciled,
            }
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
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
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
            refold: Arc::new(RefoldQueue::default()),
            tick_hook,
        }
    }

    /// A jitter source that always draws zero, so every sleep the loop takes
    /// is exactly its base interval and the schedules below are exact.
    struct ZeroJitter;

    impl RngSource for ZeroJitter {
        fn jitter_ms(&self, _max_ms: u64) -> u64 {
            0
        }

        fn new_uuid(&self) -> Uuid {
            Uuid::nil()
        }
    }

    /// The virtual instant of every tick a [`recording_hook`] saw, in order.
    type TickLog = Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>;

    /// A [`LoopContext::tick_hook`].
    type TickHook = Arc<dyn Fn() + Send + Sync>;

    /// A tick hook that records the virtual instant of every tick it sees,
    /// then panics on the calls `panics_on` selects (1-based call numbers).
    fn recording_hook(
        panics_on: impl Fn(usize) -> bool + Send + Sync + 'static,
    ) -> (TickLog, TickHook) {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook_calls = Arc::clone(&calls);
        let hook: TickHook = Arc::new(move || {
            let call = {
                let mut calls = hook_calls.lock().expect("hook lock");
                calls.push(tokio::time::Instant::now());
                calls.len()
            };
            if panics_on(call) {
                panic!("injected catalog fold loop panic (test), call {call}");
            }
        });
        (calls, hook)
    }

    /// Each tick's offset from the first, in whole milliseconds.
    fn offsets_ms(calls: &[tokio::time::Instant]) -> Vec<u128> {
        calls
            .iter()
            .map(|at| (*at - calls[0]).as_millis())
            .collect()
    }

    /// The window `RavelCatalogFoldLoopCrashLooping` takes the increase over.
    const CRASH_LOOP_WINDOW: Duration = Duration::from_secs(15 * 60);

    /// The restart count that window must exceed for the rule to fire.
    const CRASH_LOOP_THRESHOLD: u64 = 5;

    /// The shipped rule's expression, spelled with the two constants above, so
    /// a rule edit that is not reflected here fails this test rather than
    /// leaving it asserting against a threshold nobody ships.
    #[test]
    fn the_shipped_crash_loop_rule_uses_the_window_and_threshold_tested_here() {
        let rules = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/prometheus/ravel.rules.yaml"
        ))
        .expect("read the shipped rules file");
        let expr = format!(
            "increase(ravel_catalog_fold_loop_restarts_total[{}m]) > {CRASH_LOOP_THRESHOLD}",
            CRASH_LOOP_WINDOW.as_secs() / 60
        );
        assert_eq!(
            rules.matches(&expr).count(),
            1,
            "the shipped rules file carries `{expr}` exactly once"
        );
    }

    /// At the DEFAULT fold interval and the default restart backoff, a loop
    /// that panics on every tick crosses `RavelCatalogFoldLoopCrashLooping`'s
    /// threshold well inside the rule's window, at the exact restart rate the
    /// rule's comment and the observability guide state.
    ///
    /// The first attempt ticks after one full interval (300 s), as at startup.
    /// Every restarted attempt then ticks as soon as its backoff ends, so the
    /// restarts land at offsets 0, 1, 3, 7, 15, 31, 63 and 123 s from the first
    /// panic, and every 60 s after that: 20 in the first 15 minutes, 15 in
    /// every 15 minutes after. The threshold is crossed at the sixth restart,
    /// 31 s after the first panic.
    #[tokio::test(start_paused = true)]
    async fn a_loop_panicking_every_tick_at_the_defaults_crosses_the_crash_loop_threshold() {
        let (store, catalog, tenant) = seeded_store().await;
        let metrics = Arc::new(FoldLoopMetrics::default());
        let (calls, tick_hook) = recording_hook(|_| true);
        let mut ctx = loop_context(catalog.clone(), store, tenant, tick_hook);
        ctx.interval = DEFAULT_FOLD_INTERVAL;
        ctx.rng = Arc::new(ZeroJitter);

        let started = tokio::time::Instant::now();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(run_supervisor(
            ctx,
            shutdown_rx,
            Arc::clone(&metrics),
            RESTART_BACKOFF_INITIAL,
            RESTART_BACKOFF_MAX,
        ));

        // Two full windows after the first tick, on the paused clock: the
        // runtime jumps from timer to timer, so every tick below lands at its
        // exact deadline.
        tokio::time::sleep_until(started + DEFAULT_FOLD_INTERVAL + 2 * CRASH_LOOP_WINDOW).await;
        let calls = calls.lock().expect("hook lock").clone();
        assert!(!calls.is_empty(), "the first attempt must tick and panic");
        assert_eq!(
            calls[0] - started,
            DEFAULT_FOLD_INTERVAL,
            "the first attempt ticks after one full interval, as at startup"
        );

        // The schedule the backoff bounds imply: each restart one backoff after
        // the last, the backoff doubling from 1 s to its 60 s cap.
        let mut expected: Vec<u128> = vec![0];
        let mut backoff = RESTART_BACKOFF_INITIAL;
        let mut at = Duration::ZERO;
        loop {
            at += backoff;
            if at >= 2 * CRASH_LOOP_WINDOW {
                break;
            }
            expected.push(at.as_millis());
            backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
        }
        let offsets = offsets_ms(&calls);
        assert_eq!(
            offsets, expected,
            "every restarted attempt ticks as soon as its backoff ends"
        );
        assert_eq!(
            &offsets[..9],
            &[
                0, 1_000, 3_000, 7_000, 15_000, 31_000, 63_000, 123_000, 183_000
            ],
            "the restart offsets the rule's comment and the guide state"
        );

        let window_ms = CRASH_LOOP_WINDOW.as_millis();
        let first_window = offsets.iter().filter(|at| **at < window_ms).count();
        let second_window = offsets
            .iter()
            .filter(|at| (window_ms..2 * window_ms).contains(*at))
            .count();
        assert_eq!(
            first_window, 20,
            "restarts in the first 15 minutes of panicking"
        );
        assert_eq!(
            second_window, 15,
            "restarts in each later 15 minutes, one per 60 s cap"
        );
        assert!(
            u64::try_from(second_window).expect("fits") > CRASH_LOOP_THRESHOLD,
            "the steady-state rate stays over the threshold, so the rule's `for` holds"
        );
        assert_eq!(
            offsets[usize::try_from(CRASH_LOOP_THRESHOLD).expect("fits")],
            31_000,
            "the restart that crosses the threshold lands 31 s after the first panic"
        );
        assert_eq!(
            metrics.restarts_for(Signal::Metrics),
            u64::try_from(calls.len()).expect("fits"),
            "every panicked tick is counted as exactly one restart"
        );

        shutdown_tx.send(()).expect("send shutdown");
        let _ = handle.await;
    }

    /// The restart backoff doubles from 1 s across consecutive panics, holds
    /// at its 60 s cap, and resets to 1 s once an attempt completes a tick.
    ///
    /// Calls 1 to 10 panic, call 11 completes (the restarted attempt then
    /// sleeps one interval), call 12 panics after that completed tick, and
    /// calls 13 onward complete.
    #[tokio::test(start_paused = true)]
    async fn the_restart_backoff_doubles_to_its_cap_and_resets_after_a_completed_tick() {
        let (store, catalog, tenant) = seeded_store().await;
        let metrics = Arc::new(FoldLoopMetrics::default());
        let (calls, tick_hook) = recording_hook(|call| call <= 10 || call == 12);
        let mut ctx = loop_context(catalog.clone(), store, tenant, tick_hook);
        ctx.rng = Arc::new(ZeroJitter);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(run_supervisor(
            ctx,
            shutdown_rx,
            Arc::clone(&metrics),
            RESTART_BACKOFF_INITIAL,
            RESTART_BACKOFF_MAX,
        ));

        let reached = async {
            for _ in 0..2_000 {
                if calls.lock().expect("hook lock").len() >= 14 {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            false
        }
        .await;
        assert!(reached, "the loop reaches its 14th tick");

        let calls = calls.lock().expect("hook lock").clone();
        let gaps_ms: Vec<u128> = calls[..14]
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_millis())
            .collect();
        let interval_ms = TEST_INTERVAL.as_millis();
        assert_eq!(
            gaps_ms,
            vec![
                1_000,
                2_000,
                4_000,
                8_000,
                16_000,
                32_000,
                60_000,
                60_000,
                60_000,
                60_000,
                // Call 11 completes, then the attempt sleeps one interval.
                interval_ms,
                // Call 12 panics after a completed tick: back to 1 s.
                1_000,
                // Call 13 completes; call 14 follows one interval later.
                interval_ms,
            ],
            "1, 2, 4 ... capped at 60 s, reset to 1 s after a completed tick"
        );
        assert_eq!(metrics.restarts_for(Signal::Metrics), 11);

        shutdown_tx.send(()).expect("send shutdown");
        let _ = handle.await;
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

        let ctx = loop_context(catalog.clone(), store.clone(), tenant, tick_hook);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        // Small, fixed backoff so the paused-time advance drives the restart.
        let handle = tokio::spawn(run_supervisor(
            ctx,
            shutdown_rx,
            Arc::clone(&metrics),
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));

        // The logs loop runs beside it on the same counters and never panics,
        // so the per-signal assertion below has a live loop to hold at zero.
        let mut logs_ctx = loop_context(catalog.clone(), store, tenant, Arc::new(|| {}));
        logs_ctx.signal = Signal::Logs;
        let (logs_shutdown_tx, logs_shutdown_rx) = oneshot::channel();
        let logs_handle = tokio::spawn(run_supervisor(
            logs_ctx,
            logs_shutdown_rx,
            Arc::clone(&metrics),
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));

        let folded = advance_until(600, Duration::from_millis(100), || {
            catalog.fold_cycles(Signal::Metrics) >= 1 && catalog.fold_cycles(Signal::Logs) >= 1
        })
        .await;
        assert!(
            folded,
            "the supervisor must restart the loop and a later tick must fold, and the logs \
             loop must fold beside it"
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
        logs_shutdown_tx.send(()).expect("send logs shutdown");
        let _ = handle.await;
        let _ = logs_handle.await;
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

    /// [`spawn`] refuses an enabled fold config with a zero `fold_interval`
    /// directly at its spawn site with [`SpawnError::ZeroFoldInterval`], before
    /// any loop is spawned. `fold` is a `pub` module, so an outside caller
    /// reaches this function without passing through `validate_loop_intervals`;
    /// this pins the guard that caller depends on.
    ///
    /// Flip to watch it fail: delete the `config.check_spawnable()?` call at the
    /// top of [`spawn`]. The function then builds the per-signal loop contexts
    /// on a zero interval and returns `Ok`.
    #[tokio::test]
    async fn spawn_refuses_a_zero_fold_interval() {
        let (store, catalog, tenant) = seeded_store().await;
        let worker = Arc::new(WorkerSet::new(
            NOW_NS,
            HEARTBEAT,
            DEFAULT_LIVENESS_FACTOR,
            DEFAULT_UNIT_CONCURRENCY,
        ));
        let live_set = watch::channel(worker.solo_live_set()).0.subscribe();
        let config = FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::ZERO,
        };
        match spawn(
            catalog,
            store,
            &[tenant],
            config,
            Arc::new(RetentionConfig::default()),
            worker,
            live_set,
            Arc::new(FixedClock::new(NOW_NS)),
            Arc::new(FoldLoopMetrics::default()),
            Arc::new(RefoldQueue::default()),
        ) {
            Err(SpawnError::ZeroFoldInterval) => {}
            Ok(tasks) => {
                tasks.shutdown().await;
                panic!("a zero fold_interval must be refused at spawn");
            }
        }
    }

    fn refold_tenant(n: u8) -> TenantHash {
        TenantHash([n; 16])
    }

    /// The hours pending for one pair, ascending, for exact comparison.
    fn pending_hours(queue: &RefoldQueue, tenant: TenantHash, signal: Signal) -> Vec<u32> {
        queue.peek(&tenant, signal, usize::MAX).hours().collect()
    }

    /// An empty hour set is not an entry: it neither occupies a slot nor
    /// pushes a real request out.
    #[test]
    fn an_empty_refold_send_queues_nothing() {
        let queue = RefoldQueue::new(1);
        queue.send(refold_tenant(1), Signal::Metrics, BTreeSet::from([7]));
        queue.send(refold_tenant(2), Signal::Metrics, BTreeSet::new());
        assert_eq!(queue.pending_len(), 1);
        assert_eq!(queue.dropped_requests(), 0);
    }

    /// Two sends for one pair merge into one entry carrying the union of their
    /// hours, and the same tenant under another signal is a separate pair.
    /// Past capacity the pair inserted earliest is evicted, exactly once, even
    /// when a merge into it is the send just before: age is insertion age, and
    /// a merge does not make a pair younger.
    ///
    /// Flip either line to watch it fail:
    /// - in [`RefoldQueue::send`], replace the merge branch's `entry.hours.extend(hours)`
    ///   with `entry.hours = hours` (overwrite): tenant 1's hours are `[3, 9]`;
    /// - in the same merge branch, add `entry.seq = *next_seq; *next_seq += 1;`
    ///   (a merge refreshes age): tenant 1 is evicted instead of tenant 0, so
    ///   tenant 0's metrics hours are `[0, 1000]`, not empty.
    #[test]
    fn sends_for_one_pair_merge_and_do_not_use_a_second_slot() {
        let queue = RefoldQueue::default();
        queue.send(refold_tenant(1), Signal::Metrics, BTreeSet::from([5, 9]));
        queue.send(refold_tenant(1), Signal::Logs, BTreeSet::from([11]));
        queue.send(refold_tenant(1), Signal::Metrics, BTreeSet::from([9, 3]));
        assert_eq!(queue.pending_len(), 2, "one entry per pair");
        assert_eq!(
            pending_hours(&queue, refold_tenant(1), Signal::Metrics),
            vec![3, 5, 9]
        );
        assert_eq!(
            pending_hours(&queue, refold_tenant(1), Signal::Logs),
            vec![11]
        );
        assert_eq!(queue.dropped_requests(), 0);

        let queue = RefoldQueue::new(DEFAULT_REFOLD_QUEUE_CAPACITY);
        for n in 0..DEFAULT_REFOLD_QUEUE_CAPACITY {
            let tenant = refold_tenant(u8::try_from(n).expect("fits"));
            let hour = u32::try_from(n).expect("fits");
            queue.send(tenant, Signal::Metrics, BTreeSet::from([hour]));
        }
        assert_eq!(queue.pending_len(), DEFAULT_REFOLD_QUEUE_CAPACITY);
        assert_eq!(queue.dropped_requests(), 0);

        // A merge into the oldest pair, then one new pair past capacity.
        queue.send(refold_tenant(0), Signal::Metrics, BTreeSet::from([1000]));
        assert_eq!(queue.dropped_requests(), 0, "a merge evicts nothing");
        queue.send(refold_tenant(0), Signal::Logs, BTreeSet::from([2000]));

        assert_eq!(queue.dropped_requests(), 1);
        assert_eq!(queue.pending_len(), DEFAULT_REFOLD_QUEUE_CAPACITY);
        assert_eq!(
            pending_hours(&queue, refold_tenant(0), Signal::Metrics),
            Vec::<u32>::new(),
            "the earliest-inserted pair is evicted although it was merged into last"
        );
        assert_eq!(
            pending_hours(&queue, refold_tenant(1), Signal::Metrics),
            vec![1]
        );
        assert_eq!(
            pending_hours(&queue, refold_tenant(255), Signal::Metrics),
            vec![255]
        );
        assert_eq!(
            pending_hours(&queue, refold_tenant(0), Signal::Logs),
            vec![2000]
        );
    }

    /// An entry holds at most [`REFOLD_ENTRY_HOURS_MAX`] hours and keeps the
    /// smallest, on the insert path and on the merge path alike, and the cut
    /// counts nothing as dropped.
    ///
    /// Flip to watch it fail: in `cap_entry_hours`, call `hours.pop_first()`
    /// instead of `hours.pop_last()` (keep the largest). The first entry then
    /// starts at hour 10, not 0.
    #[test]
    fn an_entry_keeps_only_its_oldest_hours_past_the_cap() {
        let max = u32::try_from(REFOLD_ENTRY_HOURS_MAX).expect("fits");
        let queue = RefoldQueue::default();

        queue.send(refold_tenant(1), Signal::Metrics, (0..max + 10).collect());
        assert_eq!(
            pending_hours(&queue, refold_tenant(1), Signal::Metrics),
            (0..max).collect::<Vec<u32>>()
        );

        queue.send(refold_tenant(2), Signal::Metrics, (10..max + 10).collect());
        queue.send(refold_tenant(2), Signal::Metrics, (0..10).collect());
        assert_eq!(
            pending_hours(&queue, refold_tenant(2), Signal::Metrics),
            (0..max).collect::<Vec<u32>>()
        );
        assert_eq!(queue.dropped_requests(), 0);
        assert_eq!(queue.pending_len(), 2);
    }

    /// Removing the hours a fold was given leaves an hour sent while that fold
    /// ran, and the entry goes only once its last hour does.
    ///
    /// Flip to watch it fail: in [`RefoldQueue::remove_hours`], replace the
    /// per-hour loop and the emptiness check with
    /// `pending.entries.remove(&key);` (remove the whole entry). Hour 12 is
    /// then gone and `pending_len` is 0.
    #[test]
    fn hours_sent_during_a_fold_survive_its_removal() {
        let queue = RefoldQueue::default();
        let tenant = refold_tenant(1);
        queue.send(tenant, Signal::Metrics, BTreeSet::from([5, 9]));

        let taken = queue.peek(&tenant, Signal::Metrics, usize::MAX);
        assert_eq!(taken.hours().collect::<Vec<u32>>(), vec![5, 9]);
        assert_eq!(queue.pending_len(), 1, "a peek removes nothing");

        queue.send(tenant, Signal::Metrics, BTreeSet::from([12]));
        queue.remove_hours(&tenant, Signal::Metrics, &taken);
        assert_eq!(queue.pending_len(), 1);
        assert_eq!(pending_hours(&queue, tenant, Signal::Metrics), vec![12]);

        queue.remove_hours(&tenant, Signal::Metrics, &RefoldRequest::from_hours([12]));
        assert_eq!(queue.pending_len(), 0);
        assert_eq!(queue.dropped_requests(), 0);
    }

    /// The maintain loop gets the queue only in a process whose scheduled fold
    /// loops are spawned: a mode that runs the scheduled fold, with the fold
    /// enabled. A maintain process with the fold disabled, and a mode with no
    /// scheduled fold, get `None`.
    ///
    /// Flip to watch it fail: in [`refold_queue_for_maintain`], return
    /// `Some(Arc::clone(queue))` unconditionally. The disabled maintain case
    /// is then `Some`, not `None`.
    #[test]
    fn the_maintain_loop_gets_the_queue_only_where_the_fold_runs() {
        let queue = Arc::new(RefoldQueue::default());
        let fold = |enabled| FoldTaskConfig {
            enabled,
            fold_interval: DEFAULT_FOLD_INTERVAL,
        };
        let given = |mode, enabled| {
            refold_queue_for_maintain(mode, &fold(enabled), &queue)
                .map(|given| Arc::ptr_eq(&given, &queue))
        };
        assert_eq!(given(Mode::Maintain, true), Some(true));
        assert_eq!(given(Mode::Maintain, false), None);
        assert_eq!(given(Mode::Query, true), None);
        assert_eq!(given(Mode::Query, false), None);
    }

    /// A limited peek returns the `limit` smallest hours and removes nothing;
    /// removing them leaves exactly the larger ones.
    ///
    /// Flip to watch it fail: in [`RefoldQueue::peek`], drop the `.take(limit)`.
    /// The peek then returns all five hours, not `[3, 5, 9]`.
    #[test]
    fn a_limited_peek_returns_the_smallest_hours() {
        let queue = RefoldQueue::default();
        let tenant = refold_tenant(1);
        queue.send(tenant, Signal::Metrics, BTreeSet::from([20, 9, 3, 14, 5]));

        let taken = queue.peek(&tenant, Signal::Metrics, 3);
        assert_eq!(taken.hours().collect::<Vec<u32>>(), vec![3, 5, 9]);
        assert_eq!(
            pending_hours(&queue, tenant, Signal::Metrics),
            vec![3, 5, 9, 14, 20],
            "a peek removes nothing"
        );
        assert_eq!(
            queue
                .peek(&tenant, Signal::Metrics, 0)
                .hours()
                .collect::<Vec<u32>>(),
            Vec::<u32>::new()
        );
        assert_eq!(
            queue
                .peek(&tenant, Signal::Metrics, 6)
                .hours()
                .collect::<Vec<u32>>(),
            vec![3, 5, 9, 14, 20]
        );

        queue.remove_hours(&tenant, Signal::Metrics, &taken);
        assert_eq!(pending_hours(&queue, tenant, Signal::Metrics), vec![14, 20]);
    }
}
