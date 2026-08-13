//! Background maintenance task (docs/compaction-retention-plan.md P8, issue
//! #115; storage-derived tenant set is ADR-0048 decision 3, issue #504).
//! Periodically runs age-based retention, L0->L1 compaction, and the GC
//! sweeper over every `(signal, shard)` of every tenant storage holds data
//! for. It also brings the query-audit shard (`Signal::Audit` /
//! `QUERY_AUDIT_SHARD`) into the maintained set (issue #763, EL-6): after the
//! data-signal loop it compacts that shard, cleans up the compacted L0 inputs,
//! and runs a dedicated age-based retention sweep on its own 90-day window,
//! separate from the ADR-0019 per-tenant data retention. The legal-hold shard
//! (`AUDIT_HOLD_SHARD` = 0) is never a delete target.
//!
//! Unlike fold (a pure query-cost optimization), this task deletes and rewrites
//! durable objects, but it changes nothing about *what* any sweep, retention,
//! or compaction rule decides: it is only the driver that calls
//! [`scan_and_maintain_with_memo`] (retention-before-compaction over every
//! sealed bucket), [`ravel_maintain::sweep_shard`] (the three per-shard GC
//! rules), [`ravel_maintain::sweep_idempotency_markers`] (the fourth GC
//! rule, run once per signal instead of per shard), and the ADR-0064
//! selective-erasure trio -- [`ravel_maintain::erasure_rewrite_bucket`] per
//! bucket, the request-completion `.done` write, and
//! [`ravel_maintain::sweep_erasure_requests`] -- once per tenant per tick.
//! All are idempotent, so a missed or crashed tick is recovered on the next
//! one. The clock is the real [`SystemClock`], matching everything else in
//! this crate.
//!
//! [`spawn`] runs one supervisor task, not one task per tenant: at the start
//! of every tick it re-enumerates tenants from storage
//! ([`ravel_maintain::discover_tenants`] via
//! [`crate::tenant_discovery::discover_and_restrict_by_lifecycle`]), narrows
//! that set by each tenant's durable lifecycle state and the flag fallback
//! (ADR-0066 decision 6: a config record keeps a tenant maintained
//! unconditionally regardless of its token, and no flag can exclude a
//! config-recorded tenant), then runs [`run_tick`] for each tenant in the
//! result. A
//! tenant that first writes data mid-run is picked up on the next cycle with
//! no restart, and a tenant removed from the maintained set (but still holding
//! data) is counted as excluded rather than silently dropped. Discovery
//! failure (the LIST errors) skips the whole cycle -- no tenant's tick runs
//! -- with a logged warning and a failure counter; it never falls back to an
//! empty set, because that would be indistinguishable from healthy idleness,
//! the exact silence findings S2-17/S5-09 describe.
//!
//! [`LegalHoldCheck::refresh`] is called once per tenant per tick, before
//! either pass, and its snapshot is the [`LeaseCheck`] threaded through every
//! `(signal, shard)` of that tick (ADR-0048 decision 1). A refresh failure
//! never falls back to [`NoLeases`]: the tenant's whole tick is skipped and
//! retried next tick, so a transient store fault can never turn into an
//! unprotected delete pass.
//!
//! One [`MaintainMemo`] is held across every tick and every tenant until
//! shutdown (issue #280, #330). The memo records buckets already known
//! terminal so a steady-state tick skips re-listing and re-reading them,
//! until a periodic full re-verify forces a fresh evaluation. It is ephemeral
//! and never correctness-bearing: a fresh (cold) memo on the first tick after
//! a worker start does exactly one full rescan identical to the pre-memo
//! behavior, and a wrong or lost entry only defers work by at most the
//! re-verify interval. The memo key is `(tenant, signal, shard, hour)`, so one
//! process-wide memo safely spans every tenant this supervisor discovers,
//! across ticks.
//!
//! That memo is also persisted durably (ADR-0065 decision 3, issue #747). On
//! its discovery cadence [`run_loop`] writes a compact per-unit summary of the
//! memo to `sys/maintain/memo/<process_id>`, debounced so an unchanged tick
//! writes nothing (the debounce compares the timestamp-free snapshot body, so
//! it piggybacks on the discovery tick and needs no dedicated timer). On
//! startup, and whenever a membership change moves ownership of a unit to this
//! process, the loop seeds the in-memory memo from every non-stale durable
//! snapshot (its own previous one and siblings') for the units it now owns, so a
//! restart or handoff warm-starts instead of rescanning the retention window
//! cold. Every read or write here is fail-open: a fault logs and degrades to a
//! cold start for the affected units, never blocking or crashing the loop
//! ([`crate::maintain::seed_memo_from_snapshots`],
//! [`crate::maintain::persist_memo_snapshot`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_commit::keys;
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::scan::{MaintainMemo, MaintainReport, scan_and_maintain_with_memo};
use ravel_maintain::worker_set::{DEFAULT_UNIT_CONCURRENCY, run_bounded};
use ravel_maintain::{
    Bucket, Clock, CompactorConfig, DEFAULT_MEMO_SNAPSHOT_STALENESS_NS, ErasureRewriteOutcome,
    LeaseCheck, LegalHoldCheck, MaintainError, PendingErasureRequest, QUERY_AUDIT_SHARD,
    RetentionConfig, WorkerSet, erasure_rewrite_bucket, pending_erasure_requests,
    read_all_memo_snapshots, scan_and_compact, sweep_audit_retention, sweep_erasure_requests,
    sweep_idempotency_markers, sweep_shard, sweep_shard_zoned, write_memo_snapshot,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::commit::v1::{ErasureCompletion, ErasureDeferralCause, ErasureRequest};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::{TenantDiscoveryMetrics, discover_and_restrict_by_lifecycle};

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

/// The data signals this server ingests, and therefore maintains, today.
/// Metrics (RSEG), logs (RLOG), and spans all flow through the same
/// signal-generic compaction/retention/sweep code (ADR-0032), carrying ADR-0019
/// per-tenant retention and per-signal shard counts. `Signal::Audit` is
/// deliberately absent: the query-audit shard is a fixed control-plane shard
/// maintained separately at the end of [`run_tick`] on its own window (issue
/// #763, EL-6), and the legal-hold shard is never a delete target at all.
pub(crate) const MAINTAINED_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Position of `signal` within [`MAINTAINED_SIGNALS`], and therefore within
/// [`MaintenanceSafetyMetrics`]'s per-signal arrays. Exhaustive over the
/// signals this driver actually loops over (`run_tick`'s `for signal in
/// MAINTAINED_SIGNALS`), so a signal from outside that set is a caller bug,
/// not a case to fold into an "other" bucket.
fn signal_index(signal: Signal) -> usize {
    match signal {
        Signal::Metrics => 0,
        Signal::Logs => 1,
        Signal::Spans => 2,
        other => {
            unreachable!("maintenance safety metrics only track MAINTAINED_SIGNALS, got {other:?}")
        }
    }
}

/// Process-global counters for the three maintenance safety controls that,
/// before issue #517, only reached an operator through a `tracing` line: a
/// legal-hold refresh failure (ADR-0048 decision 1), a compaction
/// conservation-gate abort (decision 6), and an orphan-GC circuit breaker
/// trip (decision 4). Rendered on the existing `GET /metrics` endpoint by
/// [`crate::metrics::render_maintain_safety_family`], no second registry.
///
/// Indexed by [`signal_index`] because each event is signal-scoped; there is
/// deliberately no `tenant_hash` dimension here. ADR-0048's decision 4 names
/// `tenant_hash` as a label for the breaker-trip counter, but ADR-0044
/// section 4 blocks any per-tenant `/metrics` series on this unauthenticated
/// route pending an authentication decision. ADR-0051's `--metrics-tenant-labels`
/// flag now exists, but it applies only to the admission usage family
/// (ADR-0051 section 6), not to this maintenance-safety family. Adding a
/// raw `tenant_hash` label here would violate ADR-0044's safety
/// precondition; see the issue #517 report for the full contradiction.
#[derive(Debug, Default)]
pub struct MaintenanceSafetyMetrics {
    legal_hold_refresh_failures: AtomicU64,
    conservation_aborts: [AtomicU64; MAINTAINED_SIGNALS.len()],
    orphan_breaker_trips: [AtomicU64; MAINTAINED_SIGNALS.len()],
    orphans_withheld: [AtomicU64; MAINTAINED_SIGNALS.len()],
    orphans_present: [AtomicU64; MAINTAINED_SIGNALS.len()],
}

impl MaintenanceSafetyMetrics {
    pub fn legal_hold_refresh_failures(&self) -> u64 {
        self.legal_hold_refresh_failures.load(Ordering::Relaxed)
    }

    pub fn conservation_aborts(&self, signal: Signal) -> u64 {
        self.conservation_aborts[signal_index(signal)].load(Ordering::Relaxed)
    }

    pub fn orphan_breaker_trips(&self, signal: Signal) -> u64 {
        self.orphan_breaker_trips[signal_index(signal)].load(Ordering::Relaxed)
    }

    /// Orphan candidates withheld by the most recent sweep pass for `signal`.
    /// Always `0` when that pass did not trip the breaker -- including a
    /// pass after a previous trip, when dilution or partial restoration let
    /// the breaker clear. That drop to `0` is the un-trip: `orphan_breaker_trips`
    /// above is the durable record that a trip (and its withheld data) ever
    /// happened; this gauge alone must never be read as "resolved."
    pub fn orphans_withheld(&self, signal: Signal) -> u64 {
        self.orphans_withheld[signal_index(signal)].load(Ordering::Relaxed)
    }

    /// Orphan candidates the most recent sweep pass for `signal` found,
    /// whether the breaker tripped or not: `orphans_deleted + orphans_withheld`
    /// (exactly one of those is nonzero per pass). This is the signal for
    /// small-scale commit-record loss the breaker's ratio/count thresholds are
    /// deliberately too coarse to catch (ADR-0058 decision 1): delete a handful
    /// of commit records for one shard and the breaker never trips, so
    /// `orphans_withheld` stays `0` even as the orphaned data objects march to
    /// the grace horizon and get deleted like ordinary abandoned flushes. This
    /// gauge is nonzero for exactly those passes.
    ///
    /// A gauge, not a counter, for the same reason as [`orphans_withheld`]:
    /// it reflects only the most recent pass. A drop to a lower value (or to
    /// `0`) is not "resolved" -- it is just this pass's candidate count, which
    /// falls as orphans are deleted or their records restored. The durable
    /// record that orphans were ever present is the operator's own
    /// investigation the alert triggered, not a later reading of this gauge.
    ///
    /// [`orphans_withheld`]: Self::orphans_withheld
    pub fn orphans_present(&self, signal: Signal) -> u64 {
        self.orphans_present[signal_index(signal)].load(Ordering::Relaxed)
    }

    pub fn record_legal_hold_refresh_failure(&self) {
        self.legal_hold_refresh_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_conservation_abort(&self, signal: Signal) {
        self.conservation_aborts[signal_index(signal)].fetch_add(1, Ordering::Relaxed);
    }

    /// One `sweep_shard` result for `signal`: increments the trip counter
    /// when `tripped`, and always overwrites the withheld and present gauges
    /// with this pass's counts (both `store`, never `fetch_add` -- these are
    /// gauges), matching [`orphans_withheld`]'s and [`orphans_present`]'s docs
    /// on why neither gauge alone can be read as "resolved". `present` is the
    /// pass's total orphan-candidate count (`orphans_deleted +
    /// orphans_withheld`, exactly one of which is nonzero); `withheld` is `0`
    /// unless the breaker tripped.
    ///
    /// [`orphans_withheld`]: Self::orphans_withheld
    /// [`orphans_present`]: Self::orphans_present
    pub fn record_sweep(&self, signal: Signal, tripped: bool, withheld: usize, present: usize) {
        let index = signal_index(signal);
        if tripped {
            self.orphan_breaker_trips[index].fetch_add(1, Ordering::Relaxed);
        }
        self.orphans_withheld[index].store(withheld as u64, Ordering::Relaxed);
        self.orphans_present[index].store(present as u64, Ordering::Relaxed);
    }
}

/// Default number of consecutive failed ticks a unit must accrue before
/// `MaintenanceOwnershipMetrics::observe_unit_tick` counts it as stalled
/// (ADR-0065 stuck-owner mitigation).
const DEFAULT_STALLED_AFTER_INTERVALS: u32 = 3;

/// Per-unit consecutive-failure counters backing `ravel_maintain_units_stalled`.
///
/// A unit (tenant, signal, shard) is "stalled" once it has failed
/// `stalled_after_intervals` ticks in a row without an intervening success;
/// a single success resets its counter to zero.
#[derive(Debug, Default)]
struct UnitStallTracker {
    threshold: u32,
    failures: parking_lot::Mutex<std::collections::HashMap<(TenantHash, Signal, u32), u32>>,
}

impl UnitStallTracker {
    fn new(threshold: u32) -> Self {
        UnitStallTracker {
            threshold,
            failures: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Records one tick outcome for `unit` and returns the current count of
    /// units at or past the stall threshold.
    fn observe(&self, unit: (TenantHash, Signal, u32), ok: bool) -> u64 {
        let mut failures = self.failures.lock();
        if ok {
            failures.remove(&unit);
        } else {
            let count = failures.entry(unit).or_insert(0);
            *count = count.saturating_add(1);
        }
        failures
            .values()
            .filter(|&&count| count >= self.threshold)
            .count() as u64
    }

    /// Drops every entry not present in `owned` and returns the recomputed
    /// stall count over what remains. A unit only accrues failures here while
    /// `observe` is called for it, i.e. while this process owns it; once it
    /// leaves the owned set (rendezvous re-partition, tenant deprovision, a
    /// shard-count reduction) `observe` is never called for it again, so
    /// without this its entry is stranded and keeps counting toward the
    /// gauge forever. If the unit returns to this process's ownership later
    /// and fails again, it re-accumulates from zero.
    fn retain_owned(&self, owned: &std::collections::HashSet<(TenantHash, Signal, u32)>) -> u64 {
        let mut failures = self.failures.lock();
        failures.retain(|unit, _| owned.contains(unit));
        failures
            .values()
            .filter(|&&count| count >= self.threshold)
            .count() as u64
    }
}

/// New metrics for ADR-0065's stuck-owner mitigation: how many owned units
/// this process is carrying, how many workers are live in-process, how many
/// units warm-started from a durable memo snapshot, and how many units are
/// stalled (consecutive failing ticks). Exported on `/metrics` with closed
/// label sets only (ADR-0044 section 4): no `tenant_hash`.
#[derive(Debug, Default)]
pub struct MaintenanceOwnershipMetrics {
    workers_live: AtomicU64,
    units_owned: AtomicU64,
    memo_warm_start_units: AtomicU64,
    full_sweep_passes_total: AtomicU64,
    units_stalled: AtomicU64,
    stalls: UnitStallTracker,
    /// This cycle's owned-unit accumulator, paired with `set_units_owned(0)`:
    /// cleared in `begin_cycle`, filled by `note_owned_unit` as `run_tick`
    /// walks each tenant, then consumed by `end_cycle` to prune `stalls`.
    owned_this_cycle: parking_lot::Mutex<std::collections::HashSet<(TenantHash, Signal, u32)>>,
}

impl MaintenanceOwnershipMetrics {
    pub fn new(stalled_after_intervals: u32) -> Self {
        MaintenanceOwnershipMetrics {
            workers_live: AtomicU64::new(0),
            units_owned: AtomicU64::new(0),
            memo_warm_start_units: AtomicU64::new(0),
            full_sweep_passes_total: AtomicU64::new(0),
            units_stalled: AtomicU64::new(0),
            stalls: UnitStallTracker::new(stalled_after_intervals),
            owned_this_cycle: parking_lot::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Clears the per-cycle owned-unit accumulator. Call once at the top of a
    /// discovery cycle, alongside `set_units_owned(0)`.
    fn begin_cycle(&self) {
        self.owned_this_cycle.lock().clear();
    }

    /// Records that `(tenant, signal, shard)` is owned by this process this
    /// cycle. Call alongside `add_units_owned` for every unit in an owned
    /// set, whether inside a discovery cycle or a standalone `run_tick`.
    fn note_owned_unit(&self, tenant: TenantHash, signal: Signal, shard: u32) {
        self.owned_this_cycle.lock().insert((tenant, signal, shard));
    }

    /// Prunes stall history to this cycle's owned set and recomputes
    /// `units_stalled` over what remains. Call once at the end of a
    /// discovery cycle, after every tenant's tick has run.
    fn end_cycle(&self) {
        let owned = self.owned_this_cycle.lock();
        let stalled = self.stalls.retain_owned(&owned);
        self.units_stalled.store(stalled, Ordering::Relaxed);
    }

    fn set_workers_live(&self, count: u64) {
        self.workers_live.store(count, Ordering::Relaxed);
    }

    fn set_units_owned(&self, count: u64) {
        self.units_owned.store(count, Ordering::Relaxed);
    }

    fn add_units_owned(&self, delta: u64) {
        self.units_owned.fetch_add(delta, Ordering::Relaxed);
    }

    fn add_memo_warm_start_units(&self, count: u64) {
        self.memo_warm_start_units
            .fetch_add(count, Ordering::Relaxed);
    }

    fn inc_full_sweep_passes(&self) {
        self.full_sweep_passes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one tick outcome for the given owned unit, updating the
    /// stalled-unit count if this tick crossed (or cleared) the threshold.
    fn observe_unit_tick(&self, tenant: TenantHash, signal: Signal, shard: u32, ok: bool) {
        let stalled = self.stalls.observe((tenant, signal, shard), ok);
        self.units_stalled.store(stalled, Ordering::Relaxed);
    }

    pub fn workers_live(&self) -> u64 {
        self.workers_live.load(Ordering::Relaxed)
    }

    pub fn units_owned(&self) -> u64 {
        self.units_owned.load(Ordering::Relaxed)
    }

    pub fn memo_warm_start_units(&self) -> u64 {
        self.memo_warm_start_units.load(Ordering::Relaxed)
    }

    pub fn full_sweep_passes_total(&self) -> u64 {
        self.full_sweep_passes_total.load(Ordering::Relaxed)
    }

    pub fn units_stalled(&self) -> u64 {
        self.units_stalled.load(Ordering::Relaxed)
    }
}

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
    /// Bounded intra-process unit concurrency (ADR-0065 decision 2's
    /// stuck-owner mitigation): the maximum number of owned `(signal, shard)`
    /// units this process maintains at once within a tenant's tick, replacing
    /// the pre-ADR-0065 strictly-sequential per-shard walk. Default 4.
    pub unit_concurrency: usize,
    /// Consecutive failed ticks a unit must accrue before
    /// `ravel_maintain_units_stalled` counts it (ADR-0065 stuck-owner
    /// mitigation). Default 3.
    pub stalled_after_intervals: u32,
}

impl Default for MaintenanceTaskConfig {
    fn default() -> Self {
        MaintenanceTaskConfig {
            enabled: false,
            interval: DEFAULT_MAINTAIN_INTERVAL,
            shard_count: 4,
            compactor: CompactorConfig::default(),
            retention: RetentionConfig::default(),
            unit_concurrency: DEFAULT_UNIT_CONCURRENCY,
            stalled_after_intervals: DEFAULT_STALLED_AFTER_INTERVALS,
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

/// Spawn one supervisor task that re-discovers the tenant set from storage
/// every tick (ADR-0048 decision 3, ADR-0066 decision 6). `fallback_allow` is
/// the merged `--tenant-token`/`--maintain-tenant` set: empty means unconfigured
/// and it otherwise governs only tenants with no durable config record. A tenant
/// carrying a config record is maintained unconditionally, so no flag can
/// exclude it. Returns immediately; the task runs until
/// [`MaintenanceTasks::shutdown`].
///
/// `stored_gc` is the durable `sys/gc` object `main` already bootstrapped and
/// read (ADR-0050 section 4). Before this fail-closed startup RE-ASSERT (issue
/// #993, closing the #904 gap): the write fence in `ravel-cli gc-config set`
/// validates a proposed horizon against the *CLI's* declared
/// `--clock-skew-allowance`, but that knob and THIS running sweeper's
/// [`CompactorConfig::clock_skew_allowance_ns`] are independent -- a deployment
/// could write `sys/gc` with a 5 min skew while running sweepers configured with
/// a larger one, leaving the durable horizon skew-uncovered for the sweeper that
/// actually deletes. [`ravel_server::gc_config::validate_maintain`] does not
/// catch it (it only checks that the configured horizon and grace EQUAL the
/// stored ones; the skew term is in neither). So before spawning the loop that
/// runs the delete/GC path, re-assert `protection_horizon >= max_query_duration
/// + grace + clock_skew_allowance` using the running sweeper's OWN skew. On a
/// violation this returns [`GcConfigError::MaintainSkewUncovered`] and spawns
/// nothing: FAIL CLOSED. A misconfigured, unsafe GC is an operator error to fix,
/// strictly better than silently deleting a live reader's snapshot. This is on
/// the shipping maintain binary's path (`ravel_server::start` -> `spawn` ->
/// `run_loop`), so no sweep loop is ever entered with a skew-uncovered horizon.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: Vec<TenantHash>,
    config: MaintenanceTaskConfig,
    stored_gc: ravel_maintain::GcConfigValues,
    metrics: Arc<TenantDiscoveryMetrics>,
    safety: Arc<MaintenanceSafetyMetrics>,
    ownership: Arc<MaintenanceOwnershipMetrics>,
    worker: Arc<WorkerSet>,
) -> Result<MaintenanceTasks, ravel_maintain::GcConfigError> {
    if !config.enabled {
        return Ok(MaintenanceTasks::none());
    }

    // Fail-closed skew re-assert (issue #993) BEFORE any delete path can run:
    // the stored `sys/gc` horizon must cover THIS running sweeper's own
    // `clock_skew_allowance_ns`, not just the write-time skew the horizon was
    // authored against. A violation refuses to spawn the sweep loop at all.
    ravel_maintain::validate_maintain_skew(&stored_gc, config.compactor.clock_skew_allowance_ns)?;

    // Production OS-entropy source (ADR-0068 decision 2) for the compactor
    // writer id and the per-tick loop jitter. The server always uses the
    // OS-entropy default; the simulation harness does not drive this loop.
    let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
    // One compactor writer_id per process start, shared across every tenant
    // this supervisor maintains (recorded in each L1 part's footer;
    // informational, never dedup-priority).
    let mut compactor = config.compactor.clone();
    compactor.compactor_writer_id = rng.new_uuid();
    let compactor = Arc::new(compactor);
    let retention = Arc::new(config.retention.clone());
    let fallback_allow = if fallback_allow.is_empty() {
        None
    } else {
        Some(fallback_allow)
    };

    let (tx, rx) = oneshot::channel();
    let interval = config.interval;
    let shard_count = config.shard_count;
    let handle = tokio::spawn(async move {
        run_loop(
            store,
            fallback_allow,
            compactor,
            retention,
            shard_count,
            interval,
            metrics,
            safety,
            ownership,
            worker,
            rng,
            rx,
        )
        .await;
    });
    Ok(MaintenanceTasks {
        shutdown: vec![tx],
        handles: vec![handle],
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: Option<Vec<TenantHash>>,
    compactor: Arc<CompactorConfig>,
    retention: Arc<RetentionConfig>,
    shard_count: u32,
    interval: Duration,
    metrics: Arc<TenantDiscoveryMetrics>,
    safety: Arc<MaintenanceSafetyMetrics>,
    ownership: Arc<MaintenanceOwnershipMetrics>,
    worker: Arc<WorkerSet>,
    rng: Arc<dyn RngSource>,
    mut shutdown: oneshot::Receiver<()>,
) {
    // One memo for the whole process, held across every tick and every
    // discovered tenant until shutdown (issue #280, #330). Its key includes
    // the tenant and signal, so this single instance safely spans every
    // tenant this supervisor discovers. Cold on the first tick, so that tick
    // is a full rescan identical to the pre-memo behavior -- unless warm start
    // (below) seeds it from durable snapshots first.
    //
    // The memo's re-verify interval now governs only the interior zone
    // (ADR-0065 decision 3): head and tail hours bypass the memo and
    // evaluate every tick regardless of this value
    // (`scan::scan_and_maintain_with_memo`), so this is
    // `compactor.interior_reverify_ns`, not the crate's flat 1 h default.
    let mut memo = MaintainMemo::new(compactor.interior_reverify_ns);

    // Durable memo snapshot state (ADR-0065 decision 3, issue #747).
    //
    // `reseed` requests a warm start: seed `memo` from every non-stale durable
    // snapshot (this process's own previous one and siblings') for the units
    // this process now owns, before the next discovery cycle runs cold. It is
    // set once at startup and again whenever a membership change moves ownership
    // (the live set below changes), which is exactly when a unit may have just
    // arrived from another worker and its terminal facts live only in that
    // worker's snapshot.
    //
    // `last_memo_body` debounces the durable write: it holds the last snapshot
    // *body* (the RLE unit/exception content, without the ever-changing
    // timestamp header) we successfully wrote, so a tick that changed nothing
    // writes nothing. The debounce piggybacks on the existing discovery cadence;
    // it needs no dedicated timer.
    let mut reseed = true;
    let mut last_memo_body: Option<Vec<u8>> = None;

    let clock = WallClock;

    // Worker membership (ADR-0065 decision 1) runs on its own heartbeat cadence
    // `H`, independent of the (coarser) discovery interval, and -- since issue
    // #796 -- in its OWN spawned task rather than as a `select!` arm sharing the
    // loop with discovery. A discovery cycle walks every tenant sequentially
    // with no wall-time bound; when the heartbeat was a `select!` arm the macro
    // did not re-enter while the discovery arm's future ran, so the heartbeat
    // `interval` arm was never polled (`MissedTickBehavior::Delay` merely defers
    // the tick). A cycle longer than the `3 * H` liveness window (180s at
    // defaults) then starved this process's own heartbeat past that window, and
    // siblings evicted it from their live sets mid-cycle and took over units it
    // was still working. Running the heartbeat concurrently fires it on cadence
    // `H` no matter how long a discovery cycle runs.
    //
    // The heartbeat task owns the write cadence, the `set_workers_live`
    // accounting, and the live-set computation; it publishes each freshly
    // computed live set over a `watch` channel that the discovery loop reads at
    // the top of every cycle. `interval`'s first tick fires immediately, so the
    // heartbeat is written and the live set computed before the first discovery
    // cycle runs. A live-set read failure publishes nothing, so the receiver
    // keeps the last-known set (fail-open, ADR-0065 decision 1): ownership stays
    // stable rather than collapsing.
    let (live_tx, live_rx) = tokio::sync::watch::channel(worker.solo_live_set());
    ownership.set_workers_live(live_rx.borrow().len() as u64);
    let (heartbeat_shutdown_tx, mut heartbeat_shutdown_rx) = oneshot::channel::<()>();
    let heartbeat_handle = {
        let store = Arc::clone(&store);
        let worker = Arc::clone(&worker);
        let ownership = Arc::clone(&ownership);
        tokio::spawn(async move {
            let clock = WallClock;
            let mut heartbeat = tokio::time::interval(worker.heartbeat_interval());
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        let now = clock.now_ns();
                        if let Err(err) = worker.write_heartbeat(store.as_ref(), now).await {
                            tracing::warn!(
                                error = %err,
                                "maintenance: worker heartbeat write failed; self-corrects next interval"
                            );
                        }
                        match worker.live_set(store.as_ref(), now).await {
                            Ok(computed) => {
                                ownership.set_workers_live(computed.len() as u64);
                                // Publish the latest live set for the discovery
                                // loop. `send` fails only once the receiver has
                                // dropped, i.e. the loop is shutting down; there
                                // is nothing to publish to then.
                                let _ = live_tx.send(computed);
                            }
                            Err(err) => tracing::warn!(
                                error = %err,
                                "maintenance: worker live-set read failed; keeping the last-known \
                                 live set (fail-open, ADR-0065 decision 1)"
                            ),
                        }
                    }
                    _ = &mut heartbeat_shutdown_rx => return,
                }
            }
        })
    };

    // Reseed trigger (ADR-0065 decision 3), evaluated in the discovery loop:
    // compare the live set this cycle uses against the previous cycle's and
    // request a warm start when a membership change moved a unit's ownership
    // onto this process. Seeded from the solo set so the startup `reseed = true`
    // still fires once in a single-replica deployment (where the computed set
    // equals the solo set and never changes).
    let mut prev_live_set = worker.solo_live_set();

    // A `tokio::time::sleep(...)` written directly as a `select!` branch is a
    // fresh expression re-evaluated every time the macro re-polls at the top of
    // the loop, so a spurious wakeup would silently restart this countdown from
    // zero before it can ever elapse -- discovery would then never run. Pin one
    // `Sleep` outside the loop so `select!` only ever polls the same underlying
    // timer across iterations, and reset it (with a fresh jittered duration)
    // only when it actually fires.
    let discovery_sleep = tokio::time::sleep(jittered(interval, rng.as_ref()));
    tokio::pin!(discovery_sleep);
    loop {
        tokio::select! {
            () = &mut discovery_sleep => {
                // Latest live set from the heartbeat task (fail-open: the
                // receiver holds the last-known set across a read failure). A
                // membership change since the previous cycle may have moved a
                // unit's ownership onto this process; request a warm start so its
                // terminal facts are seeded from the departing worker's snapshot
                // rather than rescanned cold (ADR-0065 decision 3). `borrow`
                // returns a guard, so clone out of it immediately and never hold
                // it across an await.
                let live_set = live_rx.borrow().clone();
                if membership_changed(&prev_live_set, &live_set) {
                    reseed = true;
                }
                prev_live_set = live_set.clone();

                // Warm start / handoff seeding (ADR-0065 decision 3): before a
                // cycle runs cold, seed the memo from durable snapshots for the
                // units this process now owns. Fail-open: a read fault logs and
                // degrades to a cold start, never blocks the loop.
                if reseed {
                    match read_all_memo_snapshots(store.as_ref()).await {
                        Ok(snapshots) => {
                            let now = clock.now_ns();
                            let (units, buckets) = seed_memo_from_snapshots(
                                &mut memo, &snapshots, now, &worker, &live_set,
                            );
                            ownership.add_memo_warm_start_units(units as u64);
                            if buckets > 0 {
                                tracing::info!(
                                    seeded_units = units,
                                    seeded_buckets = buckets,
                                    "maintenance: warm-started memo from durable snapshots"
                                );
                            }
                            // Clear the pending reseed only on a successful read.
                            // A single transient LIST/GET fault must leave reseed
                            // set so the next cycle retries: in a single-replica
                            // deployment the live set never changes (it is always
                            // solo_live_set()), so `membership_changed` is
                            // structurally never true and this first-cycle reseed
                            // is the only warm-start trigger the worker ever gets.
                            // Clearing it on the Err arm would leave that worker
                            // cold for its whole process life.
                            reseed = false;
                        }
                        Err(err) => tracing::warn!(
                            error = %err,
                            "maintenance: memo snapshot read failed; cold start for all units \
                             this cycle, reseed retried next cycle (fail-open, ADR-0065 decision 3)"
                        ),
                    }
                }

                run_discovery_cycle(
                    store.as_ref(),
                    fallback_allow.as_deref(),
                    &compactor,
                    &retention,
                    shard_count,
                    &mut memo,
                    metrics.as_ref(),
                    safety.as_ref(),
                    ownership.as_ref(),
                    &worker,
                    &live_set,
                )
                .await;

                // Persist the updated memo, debounced (ADR-0065 decision 3): a
                // tick whose terminal set and verify times are unchanged writes
                // nothing. Fail-open: a write fault logs and retries next cycle.
                persist_memo_snapshot(
                    store.as_ref(),
                    &worker,
                    &memo,
                    &mut last_memo_body,
                    clock.now_ns(),
                )
                .await;

                discovery_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + jittered(interval, rng.as_ref()));
            }
            _ = &mut shutdown => break,
        }
    }

    // Stop the heartbeat task so no spawned task outlives this loop (issue
    // #796): signal it and await its handle. On return the heartbeat task is
    // guaranteed finished rather than detached, matching
    // `MaintenanceTasks::shutdown`'s join of the supervisor task itself.
    let _ = heartbeat_shutdown_tx.send(());
    let _ = heartbeat_handle.await;
}

/// One discovery cycle: re-enumerate tenants from storage, narrow by lifecycle
/// state and the flag fallback, then run [`run_tick`] for each tenant in the
/// result (ADR-0048 decision 3, ADR-0066 decision 6, issue #504).
/// `fallback_allow` is the token-derived allow-list governing no-config tenants;
/// a tenant carrying a config record is maintained unconditionally, so no flag
/// can exclude it. `metrics` records the discovered and maintained gauges on
/// success; a discovery failure -- the LIST itself erroring -- skips the whole
/// cycle (no tenant's tick runs) and only bumps the failure counter, never
/// falling back to an empty set. Falling back would render identically to
/// "storage has no tenants," the exact silent failure findings S2-17/S5-09
/// describe.
#[allow(clippy::too_many_arguments)]
pub async fn run_discovery_cycle(
    store: &dyn ObjectStoreBackend,
    fallback_allow: Option<&[TenantHash]>,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    metrics: &TenantDiscoveryMetrics,
    safety: &MaintenanceSafetyMetrics,
    ownership: &MaintenanceOwnershipMetrics,
    worker: &WorkerSet,
    live_set: &[Uuid],
) -> MaintainReport {
    let outcome = match discover_and_restrict_by_lifecycle(store, fallback_allow).await {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::error!(
                error = %err,
                "maintenance: tenant discovery failed; skipping this cycle entirely, retried next cycle"
            );
            metrics.record_discovery_failure();
            return MaintainReport::default();
        }
    };

    metrics.record_discovery(outcome.discovered.len(), outcome.maintained.len());
    if outcome.excluded > 0 {
        tracing::info!(
            excluded = outcome.excluded,
            "maintenance: flag restriction excluded discovered tenants holding data"
        );
    }

    // Recomputed from scratch every cycle (ADR-0065 stuck-owner mitigation
    // metrics): each tenant's tick below adds its owned unit count, so this
    // gauge always reflects the current cycle's ownership, not a running
    // total across cycles.
    ownership.set_units_owned(0);
    ownership.begin_cycle();

    let mut total = MaintainReport::default();
    for tenant in &outcome.maintained {
        let report = run_tick(
            store,
            tenant,
            compactor,
            retention,
            shard_count,
            memo,
            safety,
            ownership,
            worker,
            live_set,
        )
        .await;
        total.retired += report.retired;
        total.compacted += report.compacted;
        total.already_done += report.already_done;
        total.not_sealed += report.not_sealed;
        total.skipped_terminal += report.skipped_terminal;
    }

    // Prune stall history to exactly this cycle's owned set (ADR-0065
    // stuck-owner mitigation): a unit dropped entirely out of
    // `outcome.maintained` (tenant deprovisioned) or reassigned to a peer
    // (rendezvous re-partition) never calls `observe_unit_tick` again, so
    // without this its failure entry -- and its contribution to
    // `units_stalled` -- would be stranded forever.
    ownership.end_cycle();
    total
}

/// One maintenance pass over every `(signal, shard)` of one tenant: a legal
/// hold refresh, then retention before compaction (via
/// [`scan_and_maintain_with_memo`]), then the GC sweeper (via
/// [`sweep_shard`]), then, once per signal after that signal's shard loop
/// completes, the idempotency-marker sweep (via [`sweep_idempotency_markers`])
/// for [`Signal::Logs`] and [`Signal::Spans`] only -- markers don't exist for
/// [`Signal::Metrics`] (ADR-0051 §5) and the marker sweep already covers every
/// shard of a signal in one LIST, so it does not belong in the per-shard
/// loop -- and finally the ADR-0064 selective-erasure pass
/// ([`run_erasure_pass`]) for every signal. Every scan/sweep error is logged
/// and retried next tick; nothing here affects query correctness. Split out
/// from [`run_discovery_cycle`] so a test can drive a single deterministic
/// tenant tick without discovery or the timer.
///
/// The legal hold refresh (ADR-0048 decision 1) runs once, before either
/// pass, and its snapshot gates every `(signal, shard)` of this tick. If the
/// refresh fails, the entire tenant tick is skipped -- no signal, no shard,
/// no pass runs -- and an empty [`MaintainReport`] is returned; the driver
/// never falls back to [`ravel_maintain::NoLeases`], because that would
/// convert a transient store fault into an unprotected delete pass. The
/// failure is logged at error level so it is visible to an operator, and the
/// tick is retried next interval.
///
/// `memo` is the caller's per-worker [`MaintainMemo`], threaded through every
/// `(signal, shard)` and mutated in place: buckets it already knows terminal
/// are skipped without a per-bucket LIST or GET (issue #280, #330). The
/// returned [`MaintainReport`] sums the per-`(signal, shard)` reports of the
/// retention-and-compaction passes (the sweep pass is logged, not summed);
/// `skipped_terminal` is the count of buckets the memo let this tick skip.
///
/// `worker`/`live_set` gate ownership (ADR-0065 decision 2): a `(signal,
/// shard)` unit this process does not own under the current live set is skipped
/// entirely -- neither its retention/compaction pass nor its `sweep_shard` is
/// attempted (a discovery-time skip, not a mid-work abort). The idempotency-
/// marker sweep is gated on ownership of shard 0 of the `(tenant, signal)` pair.
/// Owned units run with bounded intra-process concurrency
/// ([`WorkerSet::unit_concurrency`]) instead of a strictly sequential walk, so
/// one pathological unit cannot starve the rest of the process's ownership
/// (decision 2's stuck-owner mitigation). With a single-replica live set
/// (`{self}`) every unit is owned and the behavior is byte-for-byte the
/// pre-ADR-0065 unconditional walk. Tenant discovery and the legal-hold refresh
/// below stay per-process, never gated on ownership (ADR-0065 decision 2).
#[allow(clippy::too_many_arguments)]
pub async fn run_tick(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    safety: &MaintenanceSafetyMetrics,
    ownership: &MaintenanceOwnershipMetrics,
    worker: &WorkerSet,
    live_set: &[Uuid],
) -> MaintainReport {
    run_tick_with_clock(
        &WallClock,
        store,
        tenant,
        compactor,
        retention,
        shard_count,
        memo,
        safety,
        ownership,
        worker,
        live_set,
    )
    .await
}

/// [`run_tick`] with the clock injected instead of hardwired to [`WallClock`].
/// The running service always passes [`WallClock`]; tests pass a
/// [`ravel_maintain::FixedClock`] so a tick that must observe time *passing*
/// -- the ADR-0064 `.dreq` sweep waits out `protection_horizon` after the
/// `.done` write -- can advance it deterministically instead of sleeping or
/// shrinking the horizon to zero (CLAUDE.md testing patterns: time is
/// injected).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tick_with_clock(
    clock: &dyn Clock,
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    safety: &MaintenanceSafetyMetrics,
    ownership: &MaintenanceOwnershipMetrics,
    worker: &WorkerSet,
    live_set: &[Uuid],
) -> MaintainReport {
    let hold = match LegalHoldCheck::refresh(store, tenant).await {
        Ok(hold) => hold,
        Err(err) => {
            tracing::error!(
                tenant = %tenant.to_hex(),
                error = %err,
                "maintenance: legal hold refresh failed; skipping this tenant's tick entirely, retried next tick"
            );
            safety.record_legal_hold_refresh_failure();
            return MaintainReport::default();
        }
    };

    let mut total = MaintainReport::default();
    for signal in MAINTAINED_SIGNALS {
        // Durable shard_count validation before maintaining this (tenant,
        // signal) (ADR-0050 section 5, EC5). A statically-known tenant's
        // mismatch already refused startup; a dynamically-discovered tenant's
        // mismatch here means this maintain process is configured for a
        // different shard_count than the tenant's data was written under.
        // Maintaining over `0..shard_count` would compact or sweep only a
        // subset of shards, so skip this (tenant, signal)'s pass entirely and
        // log loudly, rather than silently maintaining a truncated shard range
        // or crashing the whole maintain loop for one tenant. Pre-ADR data with
        // all shard indices in range is adopted here (the ADR names the
        // maintenance touch as an adopter).
        if let Err(err) = ravel_catalog::validate_or_adopt(
            store,
            tenant,
            signal,
            shard_count,
            clock.now_ns(),
            ravel_catalog::AbsentPolicy::AdoptIfData,
        )
        .await
        {
            // Count a hard mismatch caught here too, so an alert keyed on
            // `ravel_provisioning_shard_count_mismatch_total` fires for a
            // maintain-only mismatch, not just an ingest-path one.
            crate::provisioning::note_provisioning_failure(&err);
            tracing::error!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "maintenance: shard_count provisioning check failed; skipping this \
                 (tenant, signal) this tick rather than maintaining a truncated shard range"
            );
            continue;
        }

        // Generation-aware scan range (ADR-0052 section 4): maintenance must
        // compact and sweep every shard any generation ever wrote, not just
        // `0..shard_count` (this process's static config value). The scan set
        // is the union of every generation's range, i.e. the largest
        // `shard_count` across the history: after an increase this covers the
        // new, wider shards; after a decrease it keeps covering the old, wider
        // shards until retention ages their hours out. Read fresh and uncached
        // each tick (this is a separate read from the `validate_or_adopt`
        // check above, which validates the scalar gen-0 count, not the scan
        // range). An empty high shard lists cheaply and is tolerated. Absent
        // record: the single implicit generation at the configured count.
        let scan_shards =
            match ravel_catalog::read_generations_from_store(store, tenant, signal).await {
                Ok(Some(generations)) => generations
                    .iter()
                    .map(|g| g.shard_count)
                    .max()
                    .unwrap_or(shard_count),
                Ok(None) => shard_count,
                Err(err) => {
                    crate::provisioning::note_provisioning_failure(&err);
                    tracing::error!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        error = %err,
                        "maintenance: shard-generation history read failed; skipping this \
                         (tenant, signal) this tick rather than maintaining a possibly-truncated \
                         shard range"
                    );
                    continue;
                }
            };
        // Ownership gate (ADR-0065 decision 2): keep only the shards this
        // process owns under the current live set. A unit it does not own is
        // not evaluated at all this tick (a discovery-time skip, not a mid-work
        // abort); whichever worker the rendezvous hash assigns it to runs it.
        let owned_shards: Vec<u32> = (0..scan_shards)
            .filter(|shard| worker.owns_unit(live_set, tenant, signal, *shard))
            .collect();
        ownership.add_units_owned(owned_shards.len() as u64);
        for &shard in &owned_shards {
            ownership.note_owned_unit(*tenant, signal, shard);
        }

        // Carve each owned unit's memo slice out of the shared memo so its
        // concurrent future can mutate it without aliasing another unit's
        // disjoint bucket space; the slices are merged back (in ascending shard
        // order) after the fan-out completes.
        let units: Vec<(u32, MaintainMemo)> = owned_shards
            .iter()
            .map(|&shard| (shard, memo.split_unit(*tenant, signal, shard)))
            .collect();

        // Maintain owned units with bounded intra-process concurrency
        // (decision 2's stuck-owner mitigation) instead of a strictly
        // sequential walk, so one pathological unit cannot starve the rest of
        // the process's ownership. `run_bounded` preserves input (ascending
        // shard) order in its results, so the serial accounting and logging
        // below is deterministic and, for a single-replica live set, produces
        // byte-for-byte the pre-ADR-0065 sequential behavior. Each future runs
        // one unit's retention/compaction pass and then its sweep, over a
        // keyspace disjoint from every other unit's.
        let clock_ref = clock;
        let hold_ref = &hold;
        let ownership_ref = ownership;
        let unit_results = run_bounded(
            worker.unit_concurrency(),
            units,
            move |(shard, mut unit_memo)| async move {
                let scan = scan_and_maintain_with_memo(
                    &mut unit_memo,
                    store,
                    clock_ref,
                    compactor,
                    retention,
                    hold_ref,
                    *tenant,
                    signal,
                    shard,
                )
                .await;
                // Zone-scoped sweep on most ticks (ADR-0065 decision 3): rules
                // 2 and 3 list only the head+tail hours this tick's scan just
                // classified, reusing that classification instead of a second
                // hour-discovery LIST. The slow safety-net cadence -- due on
                // this unit's first tick (cold memo) and every
                // `interior_reverify_ns` after -- runs the unscoped
                // `sweep_shard` instead, so every hour, including one an
                // interior-only invalidation gap or a bug in the zone split
                // itself left permanently unswept, still gets a full pass. A
                // failed scan carries no head+tail set to scope by, so it also
                // falls back to a full pass rather than sweeping nothing.
                let now = clock_ref.now_ns();
                let sweep = match &scan {
                    Ok(report)
                        if !unit_memo.full_sweep_due(
                            *tenant,
                            signal,
                            shard,
                            now,
                            compactor.interior_reverify_ns,
                        ) =>
                    {
                        sweep_shard_zoned(
                            store,
                            clock_ref,
                            compactor,
                            hold_ref,
                            tenant,
                            signal,
                            shard,
                            &report.head_tail_hours,
                        )
                        .await
                    }
                    _ => {
                        let outcome = sweep_shard(
                            store, clock_ref, compactor, hold_ref, tenant, signal, shard,
                        )
                        .await;
                        if outcome.is_ok() {
                            unit_memo.record_full_sweep(*tenant, signal, shard, now);
                            ownership_ref.inc_full_sweep_passes();
                        }
                        outcome
                    }
                };
                (shard, unit_memo, scan, sweep)
            },
        )
        .await;

        for (shard, unit_memo, scan_result, sweep_result) in unit_results {
            memo.merge_unit(unit_memo);
            ownership.observe_unit_tick(
                *tenant,
                signal,
                shard,
                scan_result.is_ok() && sweep_result.is_ok(),
            );
            match scan_result {
                Ok(report) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        retired = report.retired,
                        compacted = report.compacted,
                        already_done = report.already_done,
                        not_sealed = report.not_sealed,
                        skipped_terminal = report.skipped_terminal,
                        "maintenance: retention + compaction pass complete"
                    );
                    total.retired += report.retired;
                    total.compacted += report.compacted;
                    total.already_done += report.already_done;
                    total.not_sealed += report.not_sealed;
                    total.skipped_terminal += report.skipped_terminal;
                }
                Err(MaintainError::ConservationViolation {
                    input_sample_count,
                    part_sample_count,
                    ingest_hour_bucket,
                    ..
                }) => {
                    tracing::error!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        ingest_hour_bucket,
                        input_sample_count,
                        part_sample_count,
                        "maintenance: compaction conservation gate aborted a publish; \
                         inputs and built parts disagree on record count, nothing written, \
                         retried next tick"
                    );
                    safety.record_conservation_abort(signal);
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

            match sweep_result {
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
                    if report.orphan_breaker_tripped {
                        tracing::error!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            shard,
                            withheld = report.orphans_withheld,
                            "maintenance: orphan GC mass-orphan circuit breaker tripped; \
                             deletions withheld this pass, not self-clearing in the sense an \
                             operator expects, see the breaker runbook"
                        );
                    }
                    safety.record_sweep(
                        signal,
                        report.orphan_breaker_tripped,
                        report.orphans_withheld,
                        report.orphans_deleted + report.orphans_withheld,
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

        // Idempotency markers exist only for logs and spans (ADR-0051 SS5);
        // the sweep LISTs one coarse prefix covering every shard of the
        // signal, so it runs once per signal here, not inside the per-shard
        // loop above. Gated on ownership of shard 0 of this (tenant, signal)
        // pair (ADR-0065 decision 2): the single worker that owns shard 0 runs
        // the whole-signal marker sweep, so replicas do not double-pay it.
        // Logged like the GC sweep pass, not folded into `MaintainReport`'s
        // summed fields.
        if matches!(signal, Signal::Logs | Signal::Spans)
            && worker.owns_unit(live_set, tenant, signal, 0)
        {
            match sweep_idempotency_markers(store, clock, compactor, &hold, tenant, signal).await {
                Ok(outcome) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        deleted = outcome.deleted,
                        kept = outcome.kept,
                        skipped_malformed = outcome.skipped_malformed,
                        "maintenance: idempotency marker sweep pass complete"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        error = %err,
                        "maintenance: idempotency marker sweep pass failed; retried next tick"
                    );
                }
            }
        }

        // Selective subject erasure (ADR-0064, epic EJ, issue #997): the
        // rewrite pass over every bucket, the request-completion `.done`
        // write, and the `.dreq` sweep -- in that order, so a request's
        // `.dreq` is only ever removed after its erasure is durably complete.
        // This is the production caller for `erasure_rewrite_bucket` and
        // `sweep_erasure_requests`; without it a submitted request is
        // physically erased by nothing and its `.dreq` (which carries the
        // subject identifier) is removed by nothing.
        //
        // Runs after this signal's compaction pass above, which is ADR-0064
        // decision 3 point 5's per-bucket serialization of compaction and
        // rewrite inside the single per-tenant loop. Gated on ownership of
        // shard 0 like the marker sweep, but unlike it the pass then walks
        // EVERY shard in `scan_shards`, not just the ones this process owns:
        // an erasure request is scoped to a whole (tenant, signal), so its
        // completion condition (below) is only sound when one process has
        // observed every shard's buckets. Correctness does not depend on the
        // resulting cross-worker interleaving with another replica's
        // compaction of a non-owned shard: every publish is `CreateIfAbsent`
        // and races resolve exactly as compaction's do (ADR-0064 decision 3
        // point 5 -- the serialization is an efficiency measure).
        if worker.owns_unit(live_set, tenant, signal, 0) {
            run_erasure_pass(
                store,
                clock,
                compactor,
                &hold,
                tenant,
                signal,
                scan_shards,
                memo,
            )
            .await;
        }
    }

    // Query-audit shard maintenance (issue #763, EL-6). The query-audit shard
    // (Signal::Audit / QUERY_AUDIT_SHARD) is brought into the maintained set on
    // its own terms: a dedicated age-based retention sweep on
    // `compactor.audit_retention_window_ns` (default 90 days), then RLOG
    // compaction of its sealed buckets, then cleanup of the compacted L0 inputs.
    // Retention runs before compaction, matching the data-signal path's
    // efficiency-preferred ordering (retention.rs `maintain_bucket`): an expired
    // bucket's records are deleted first, so compaction never rewrites data that
    // is about to be swept. It is deliberately NOT part of the MAINTAINED_SIGNALS
    // data-shard loop above: those carry ADR-0019 per-tenant retention and
    // per-signal shard counts, while the query-audit shard is a fixed
    // control-plane shard with its own lifetime. The legal-hold shard
    // (AUDIT_HOLD_SHARD = 0) is never touched: `compact_bucket` guards on the
    // shard, and every sweep here is scoped to QUERY_AUDIT_SHARD. Gated on
    // ownership of this one unit (ADR-0065 decision 2); the same `hold` snapshot
    // from above gates every delete, so a legal hold covering the query-audit
    // shard blocks it. Logged, not folded into `MaintainReport` (whose fields
    // describe the data-signal passes), and it never touches
    // `MaintenanceSafetyMetrics`, whose per-signal arrays cover only
    // MAINTAINED_SIGNALS.
    if worker.owns_unit(live_set, tenant, Signal::Audit, QUERY_AUDIT_SHARD) {
        match sweep_audit_retention(store, clock, compactor, &hold, tenant).await {
            Ok(outcome) => tracing::info!(
                tenant = %tenant.to_hex(),
                records = outcome.records_deleted,
                data = outcome.data_deleted,
                parts = outcome.parts_deleted,
                kept = outcome.kept,
                "maintenance: query-audit retention sweep complete"
            ),
            Err(err) => tracing::warn!(
                tenant = %tenant.to_hex(),
                error = %err,
                "maintenance: query-audit retention sweep failed; retried next tick"
            ),
        }

        match scan_and_compact(
            store,
            clock,
            compactor,
            *tenant,
            Signal::Audit,
            QUERY_AUDIT_SHARD,
        )
        .await
        {
            Ok(report) => tracing::info!(
                tenant = %tenant.to_hex(),
                compacted = report.compacted,
                already_done = report.already_done,
                "maintenance: query-audit compaction pass complete"
            ),
            Err(err) => tracing::warn!(
                tenant = %tenant.to_hex(),
                error = %err,
                "maintenance: query-audit compaction pass failed; retried next tick"
            ),
        }

        match sweep_shard(
            store,
            clock,
            compactor,
            &hold,
            tenant,
            Signal::Audit,
            QUERY_AUDIT_SHARD,
        )
        .await
        {
            Ok(report) => tracing::info!(
                tenant = %tenant.to_hex(),
                superseded_records = report.superseded_records_deleted,
                superseded_data = report.superseded_data_deleted,
                unreferenced_parts = report.unreferenced_parts_deleted,
                "maintenance: query-audit input-cleanup sweep complete"
            ),
            Err(err) => tracing::warn!(
                tenant = %tenant.to_hex(),
                error = %err,
                "maintenance: query-audit input-cleanup sweep failed; retried next tick"
            ),
        }
    }

    total
}

/// Domain-separation prefix for [`erasure_predicate_hash`], following the same
/// convention as ravel-commit's rewrite `input_set_hash` domain: a digest from
/// this preimage can never collide with a digest over any other structure.
const ERASURE_PREDICATE_HASH_DOMAIN: &[u8] = b"ravel-erasure-predicate-v1\0";

/// blake3 over one erasure request's canonical predicate encoding, which is
/// what an `ErasureCompletion` carries in place of the plaintext predicate
/// (ADR-0064 decision 1: the `.done` is permanent, so it must hold no subject
/// identifier).
///
/// The preimage is the domain prefix, the matcher count, then every
/// `(key, value)` pair length-prefixed and **sorted**, then both window bounds.
/// Sorting is what makes this canonical: matcher order is not significant to a
/// conjunction, so two submissions of the same erasure with the matchers typed
/// in a different order must hash identically. That is exactly the identity
/// `ravel-cli erase submit`'s `same_erasure` uses to decide whether a
/// colliding `request_id` is an idempotent retry or a genuinely different
/// erasure (matchers as an unordered multiset, plus the window bounds), so the
/// two agree on what "the same predicate" means.
///
/// This encoding lives here only because no shared helper exists yet:
/// ADR-0064 names "a blake3 hash of the canonical predicate encoding" but
/// neither `ravel_commit::erasure` nor any normative doc defines the bytes,
/// and this task's scope is this file. Flagged for a follow-up that moves it
/// next to `compute_rewrite_input_set_hash` in ravel-commit, which is where a
/// frozen-contract preimage belongs.
fn erasure_predicate_hash(request: &ErasureRequest) -> [u8; 32] {
    let mut matchers: Vec<(&str, &str)> = request
        .predicate
        .iter()
        .map(|m| (m.key.as_str(), m.value.as_str()))
        .collect();
    matchers.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    hasher.update(ERASURE_PREDICATE_HASH_DOMAIN);
    hasher.update(&(matchers.len() as u64).to_le_bytes());
    for (key, value) in matchers {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(&request.window_start_ns.to_le_bytes());
    hasher.update(&request.window_end_ns.to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// List every ingest-hour bucket present under one `(tenant, signal, shard)`,
/// ascending -- the erasure pass's bucket discovery. One delimited LIST of the
/// shard's `c/` prefix, exactly what `ravel_maintain::scan`'s own (private)
/// hour listing does; a common prefix that is not an ingest hour is layout
/// drift and errors rather than being skipped.
async fn list_erasure_scan_hours(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<Vec<u32>, MaintainError> {
    let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
    let listed = store.list_delimited(&prefix).await?;
    let mut hours: Vec<u32> = Vec::with_capacity(listed.common_prefixes.len());
    for common in &listed.common_prefixes {
        // common == "<prefix><hour>/"; extract the hour segment.
        let rest = common
            .strip_prefix(&prefix)
            .and_then(|r| r.strip_suffix('/'))
            .unwrap_or("");
        hours.push(keys::parse_ingest_hour_string(rest)?);
    }
    hours.sort_unstable();
    Ok(hours)
}

/// What one `(tenant, signal)` rewrite sweep observed, for the completion
/// decision and the log line.
#[derive(Debug, Default, PartialEq, Eq)]
struct ErasureRewritePass {
    /// Buckets this tick actually rewrote (a `RewriteRecord` was published,
    /// converged, or abandoned -- see `deferred` for the abandoned case).
    rewritten: usize,
    /// Buckets whose live `RewriteRecord` already named every overlapping
    /// request, so nothing was republished (the EJ-T5 idempotence guard).
    already_applied: usize,
    /// Buckets that contribute nothing to any pending request: no pending
    /// request's event-time range overlaps them, or they are tombstoned.
    out_of_scope: usize,
    /// Buckets not yet sealed. ADR-0064 decision 3 point 1 defers these to a
    /// later pass and excludes them from a request's scope; their data is
    /// already unreturnable through the query-time filter meanwhile.
    not_sealed: usize,
    /// A bucket in scope was NOT brought up to date this tick: a legal hold,
    /// an abandoned publish, a listing failure, or a rewrite error. Any of
    /// these makes the completion verification unsound, so no `.done` is
    /// written this tick and every request stays pending.
    deferred: bool,
    /// `request_id`s the CATALOG resolver
    /// ([`ravel_maintain::bucket_erasure_completion`], built on
    /// `ravel_catalog::resolve_rewrite_supersession`) proves are still served
    /// out of at least one in-scope bucket -- an L0 input, a compaction part,
    /// or a sibling rewrite the query would resolve but the rewrite pass's
    /// one-hop `resolve_live_record` was blind to (ADR-0064 §4 F1). Their
    /// `.done` is withheld even when `deferred` is false: completion follows the
    /// resolver the query path trusts, not the pass's own live-record hop.
    catalog_blocked: std::collections::HashSet<String>,
}

/// Rewrite every bucket of one `(tenant, signal)` against `pending`
/// (ADR-0064 decision 3), and classify what happened for the completion
/// decision the caller then makes.
///
/// `pending` is passed whole to every bucket: `erasure_rewrite_bucket` does
/// its own per-bucket overlap prefilter and batches every overlapping request
/// into ONE `RewriteRecord`, so N pending requests cost one rewrite per
/// bucket, not N (ADR-0064 consequences, "N concurrent DSARs cost one rewrite,
/// not N").
///
/// The [`ErasureRewriteOutcome::AlreadyApplied`] arm is the EJ-T5 idempotence
/// guard and is deliberately counted, not re-driven: a bucket whose live
/// record already names every overlapping request must not republish, or every
/// tick would land a fresh no-op rewrite superseding the last one and churn
/// generations forever.
#[allow(clippy::too_many_arguments)]
async fn erasure_rewrite_pass(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    compactor: &CompactorConfig,
    hold: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    scan_shards: u32,
    pending: &[PendingErasureRequest],
    memo: &mut MaintainMemo,
) -> ErasureRewritePass {
    let mut pass = ErasureRewritePass::default();
    for shard in 0..scan_shards {
        let hours = match list_erasure_scan_hours(store, tenant, signal, shard).await {
            Ok(hours) => hours,
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    signal = ?signal,
                    shard,
                    error = %err,
                    "maintenance: erasure rewrite could not list a shard's buckets; \
                     no completion written this tick, retried next tick"
                );
                pass.deferred = true;
                continue;
            }
        };
        for hour in hours {
            let bucket = Bucket::new(*tenant, signal, shard, hour);
            match erasure_rewrite_bucket(store, clock, compactor, hold, &bucket, pending, memo)
                .await
            {
                Ok(ErasureRewriteOutcome::Rewritten { parts, publish }) => {
                    pass.rewritten += 1;
                    // An abandoned publish wrote no record, so this bucket
                    // does not yet name the pending requests.
                    if matches!(publish, ravel_maintain::PublishOutcome::Abandoned) {
                        pass.deferred = true;
                    }
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        hour,
                        parts,
                        publish = ?publish,
                        "maintenance: erasure rewrite published for a bucket"
                    );
                }
                Ok(ErasureRewriteOutcome::AlreadyApplied) => pass.already_applied += 1,
                Ok(
                    ErasureRewriteOutcome::NoApplicableRequests | ErasureRewriteOutcome::Tombstoned,
                ) => pass.out_of_scope += 1,
                Ok(ErasureRewriteOutcome::NotSealed) => pass.not_sealed += 1,
                Ok(ErasureRewriteOutcome::Held) => {
                    // ADR-0064 §6: a legal hold wins over erasure. The request
                    // stays pending, query-time exclusion keeps hiding the
                    // data, and the erasure clock is explicitly paused.
                    pass.deferred = true;
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        hour,
                        "maintenance: erasure rewrite skipped a bucket under legal hold; \
                         the request stays pending until the hold clears"
                    );
                }
                Err(err) => {
                    pass.deferred = true;
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        hour,
                        error = %err,
                        "maintenance: erasure rewrite of a bucket failed; no completion \
                         written this tick, retried next tick"
                    );
                }
            }

            // Completion gate (ADR-0064 §4, the 2026-08-08 F1 correction): the
            // outcome above classified the bucket off ravel-maintain's one-hop
            // `resolve_live_record`, which is blind to an L0 input a query still
            // resolves through the full supersession chain. Re-derive "is this
            // bucket's contribution to every pending request current" through
            // the SAME resolver the query path runs
            // (`ravel_catalog::resolve_rewrite_supersession`, via
            // `bucket_erasure_completion`), on a fresh listing that reflects any
            // rewrite just published, so a `.done` can never be written while a
            // resolvable snapshot still serves the subject. This runs for every
            // bucket regardless of the rewrite outcome: a bucket the pass called
            // `AlreadyApplied` or `NoApplicableRequests` off its one-hop view is
            // exactly where the divergence hides.
            match ravel_maintain::bucket_erasure_completion(
                store, clock, compactor, hold, &bucket, pending,
            )
            .await
            {
                Ok(completion) => {
                    if completion.unresolved {
                        pass.deferred = true;
                    }
                    pass.catalog_blocked.extend(completion.blocked);
                }
                Err(err) => {
                    // The resolver could not establish this bucket's served
                    // view (list/decode/supersession failure). Treat it exactly
                    // as a rewrite failure: defer every request this tick rather
                    // than complete on an unverified view.
                    pass.deferred = true;
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        hour,
                        error = %err,
                        "maintenance: erasure completion resolution failed; no completion \
                         written this tick, retried next tick"
                    );
                }
            }
        }
    }
    pass
}

/// Write one request's `.done` completion record (ADR-0064 decision 1 and 4).
/// `CreateIfAbsent`, like every other durable record here: an already-present
/// `.done` is a completed earlier tick, reported as `Ok(false)`, never an
/// error and never an overwrite (the `.done` is immutable permanent evidence).
///
/// The record carries no plaintext predicate -- only its blake3 hash
/// ([`erasure_predicate_hash`]), the request id, the two timestamps, and the
/// deferral cause -- so a permanent completion record holds no subject
/// identifier, which is the entire reason `.dreq` and `.done` are separate
/// objects.
///
/// `bucket_drops` is left empty: the authoritative per-bucket dropped counts
/// are durable in each bucket's own `RewriteRecord.drops`, and
/// `ErasureRewriteOutcome` does not surface them to a driver (the live-record
/// resolution that would read them back is private to ravel-maintain).
/// Fabricating zeroes here would put wrong counts in a permanent audit record,
/// so the field stays empty until ravel-maintain returns real ones; flagged as
/// a follow-up.
async fn write_erasure_completion(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
    tenant: &TenantHash,
    signal: Signal,
    request: &ErasureRequest,
) -> Result<bool, MaintainError> {
    let completion = ErasureCompletion {
        format_version: ravel_commit::erasure::FORMAT_VERSION,
        tenant_hash: tenant.0.to_vec(),
        signal: ravel_commit::signal::to_proto(signal) as i32,
        request_id: request.request_id.clone(),
        predicate_hash: erasure_predicate_hash(request).to_vec(),
        bucket_drops: Vec::new(),
        requested_unix_ns: request.created_unix_ns,
        // A completion can never precede its own request in the durable
        // record (the codec rejects that pair outright), so a backwards or
        // skewed clock clamps to the request time rather than writing a
        // record no decoder will accept and stalling the erasure forever.
        completed_unix_ns: now_ns.max(request.created_unix_ns),
        deferral_cause: ErasureDeferralCause::Unspecified as i32,
    };
    // Validate before writing, matching `erase submit`'s discipline: a record
    // that would not decode must never reach the store.
    ravel_commit::erasure::validate_completion(&completion)?;
    let key = keys::erasure_completion_key_for(&completion)?;
    let body = ravel_commit::erasure::encode_completion(&completion);
    match store.put(&key, body, PutOptions::create_if_absent()).await {
        Ok(_) => Ok(true),
        Err(StoreError::AlreadyExists) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// One `(tenant, signal)` selective-erasure pass (ADR-0064 decisions 3, 4,
/// and 5), in the order those decisions require:
///
/// 1. **Rewrite** every bucket against every pending `.dreq`
///    ([`erasure_rewrite_pass`]).
/// 2. **Complete**: write each pending request's `.done` when the pass proved
///    every bucket in its scope now carries a live `RewriteRecord` naming it.
/// 3. **Sweep** the `.dreq`s whose `.done` is older than
///    `protection_horizon` ([`sweep_erasure_requests`]).
///
/// The order is the safety property: a `.dreq` (and with it the query-time
/// exclusion filter) can only be removed after the `.done` exists and the
/// horizon has passed, so the filter never retires while a pre-rewrite input
/// could still be resolvable. Step 3 runs even when nothing is pending,
/// because a request completed by an earlier tick is by definition no longer
/// pending and its `.dreq` still has to be swept.
///
/// **How completion is computed.** Every bucket of the (tenant, signal) is
/// classified by the rewrite pass itself, not by a second independent listing:
/// `Rewritten` and `AlreadyApplied` both mean the bucket's live record now
/// names every pending request that overlaps it (`erasure_rewrite_bucket`
/// batches all overlapping requests into one record, and its `AlreadyApplied`
/// guard fires only when the live record already names all of them);
/// `NoApplicableRequests` and `Tombstoned` mean the bucket is in no request's
/// scope. If every bucket lands in those four arms, then for every pending
/// request, every bucket in that request's scope carries a live
/// `RewriteRecord` naming it -- ADR-0064 §4's sibling-safe condition ("every
/// live rewrite record in this bucket names R", not the weaker
/// "rewrite-output-only") -- and each request's `.done` is written. If any
/// bucket was held, errored, abandoned its publish, or could not be listed,
/// `deferred` is set and NO `.done` is written this tick: completion is
/// verified, never assumed.
///
/// Deriving the check from the pass's own outcomes is deliberate: ADR-0064 §4
/// (2026-08-08 correction) forbids deciding completion from a fresh bucket
/// LIST taken independently of the exclusion logic, and
/// `erasure_rewrite_bucket` resolves each bucket's live record through the
/// same supersession resolution the rewrite itself uses.
///
/// **Known gap, matching ADR-0064 decision 3 point 1:** an unsealed bucket is
/// deferred and excluded from scope, so a windowless request can complete
/// while the current (still-open) ingest hour holds matching records that will
/// only seal later -- by which time the request is no longer pending and no
/// rewrite will revisit it. The ADR defines scope as sealed buckets and defers
/// unsealed ones; blocking completion on them instead would mean a
/// continuously-ingesting tenant never completes any request, so its `.dreq`
/// (which holds the subject identifier) would be retained forever, which is
/// the failure ADR-0064 decision 5 exists to prevent. Reported rather than
/// silently resolved either way.
#[allow(clippy::too_many_arguments)]
async fn run_erasure_pass(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    compactor: &CompactorConfig,
    hold: &dyn LeaseCheck,
    tenant: &TenantHash,
    signal: Signal,
    scan_shards: u32,
    memo: &mut MaintainMemo,
) {
    match pending_erasure_requests(store, tenant, signal).await {
        Ok(pending) if !pending.is_empty() => {
            let pass = erasure_rewrite_pass(
                store,
                clock,
                compactor,
                hold,
                tenant,
                signal,
                scan_shards,
                &pending,
                memo,
            )
            .await;
            tracing::info!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                pending = pending.len(),
                rewritten = pass.rewritten,
                already_applied = pass.already_applied,
                out_of_scope = pass.out_of_scope,
                not_sealed = pass.not_sealed,
                deferred = pass.deferred,
                catalog_blocked = pass.catalog_blocked.len(),
                "maintenance: erasure rewrite pass complete"
            );

            if pass.deferred {
                tracing::info!(
                    tenant = %tenant.to_hex(),
                    signal = ?signal,
                    pending = pending.len(),
                    "maintenance: erasure completion deferred; at least one in-scope bucket \
                     was not brought up to date this tick, so no .done is written and every \
                     request stays pending"
                );
            } else {
                for entry in &pending {
                    // Completion follows the CATALOG resolver, not the rewrite
                    // pass's one-hop live-record classification (ADR-0064 §4
                    // F1): if `bucket_erasure_completion` proved any in-scope
                    // bucket still serves this request's subject, its `.done` is
                    // withheld this tick even though no bucket deferred. This is
                    // the exact gate the divergence test flips -- drop the
                    // `contains` guard and a `.done` lands while a snapshot
                    // still resolves the subject out of an L0 input the one-hop
                    // resolver never saw.
                    if pass.catalog_blocked.contains(&entry.request.request_id) {
                        tracing::info!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            request_id = %entry.request.request_id,
                            "maintenance: erasure completion withheld; the catalog resolver \
                             still serves the subject from an in-scope bucket, so no .done is \
                             written and the request stays pending"
                        );
                        continue;
                    }
                    match write_erasure_completion(
                        store,
                        clock.now_ns(),
                        tenant,
                        signal,
                        &entry.request,
                    )
                    .await
                    {
                        Ok(true) => tracing::info!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            request_id = %entry.request.request_id,
                            "maintenance: erasure request complete; .done written"
                        ),
                        Ok(false) => tracing::debug!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            request_id = %entry.request.request_id,
                            "maintenance: erasure .done already present; left untouched"
                        ),
                        Err(err) => tracing::warn!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            request_id = %entry.request.request_id,
                            error = %err,
                            "maintenance: erasure .done write failed; the request stays \
                             pending and its .dreq is kept, retried next tick"
                        ),
                    }
                }
            }
        }
        Ok(_) => {}
        Err(err) => tracing::warn!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            error = %err,
            "maintenance: pending erasure request listing failed; no rewrite or completion \
             this tick, retried next tick"
        ),
    }

    // Rule 6 (ADR-0064 decision 5), last: a `.dreq` is deleted only once its
    // `.done` exists, the protection horizon has passed, and the legal-hold
    // check allows it. Runs unconditionally, including when nothing is
    // pending: a request completed by an earlier tick no longer appears in
    // `pending_erasure_requests`, and its `.dreq` still needs sweeping.
    match sweep_erasure_requests(store, clock, compactor, hold, tenant, signal).await {
        Ok(outcome) => tracing::info!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            deleted = outcome.deleted,
            kept = outcome.kept,
            "maintenance: erasure request sweep pass complete"
        ),
        Err(err) => tracing::warn!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            error = %err,
            "maintenance: erasure request sweep pass failed; retried next tick"
        ),
    }
}

/// Whether a recomputed live set differs from the one last held, which is the
/// warm-start reseed trigger (ADR-0065 decision 3): a membership change may have
/// moved a unit's rendezvous ownership onto this process, so its terminal facts
/// should be seeded from the departing worker's snapshot rather than rescanned
/// cold. Extracted from the discovery loop so the trigger is unit-testable: it
/// must fire on a genuine handoff (live set changed) and stay quiet on stable
/// membership. Both sets are sorted-and-deduped by [`WorkerSet::live_set`], so a
/// plain slice comparison is order-stable.
fn membership_changed(previous: &[Uuid], computed: &[Uuid]) -> bool {
    previous != computed
}

/// Seed `memo` from durable memo snapshots (ADR-0065 decision 3's warm start
/// and handoff). Each snapshot is gated on ownership -- only units this process
/// owns under `live_set` are seeded, so a departing worker's units are picked up
/// by whichever survivor the rendezvous hash now assigns them to -- and on
/// staleness ([`DEFAULT_MEMO_SNAPSHOT_STALENESS_NS`]). Merging several snapshots
/// keeps the freshest verdict per bucket. An undecodable snapshot is skipped
/// with a debug line (fail-open: cold start for whatever it would have seeded),
/// never a panic. Returns `(seeded_units, seeded_buckets)` summed across the
/// snapshots, for the caller's log line.
fn seed_memo_from_snapshots(
    memo: &mut MaintainMemo,
    snapshots: &[Vec<u8>],
    now_ns: i64,
    worker: &WorkerSet,
    live_set: &[Uuid],
) -> (usize, usize) {
    let mut units = 0usize;
    let mut buckets = 0usize;
    for bytes in snapshots {
        match memo.seed_from_snapshot(
            bytes,
            now_ns,
            DEFAULT_MEMO_SNAPSHOT_STALENESS_NS,
            |tenant, signal, shard| worker.owns_unit(live_set, &tenant, signal, shard),
        ) {
            Ok(stats) => {
                units += stats.seeded_units;
                buckets += stats.seeded_buckets;
            }
            Err(err) => tracing::debug!(
                error = %err,
                "maintenance: skipping an undecodable memo snapshot (cold start for its units)"
            ),
        }
    }
    (units, buckets)
}

/// Write the memo snapshot only when its content changed since the last write
/// (ADR-0065 decision 3's debounce). Compares the timestamp-free snapshot body
/// against `last_body`: identical bodies mean this tick verified nothing new, so
/// the write is skipped and the durable object left as-is. On a real change the
/// body is framed with `now_ns` and written `Overwrite`; a write fault is logged
/// and `last_body` left unchanged so the next cycle retries (fail-open). Returns
/// whether a write happened (for tests and callers that count writes).
async fn persist_memo_snapshot(
    store: &dyn ObjectStoreBackend,
    worker: &WorkerSet,
    memo: &MaintainMemo,
    last_body: &mut Option<Vec<u8>>,
    now_ns: i64,
) -> bool {
    let body = memo.snapshot_body();
    if last_body.as_deref() == Some(body.as_slice()) {
        return false;
    }
    let bytes = MaintainMemo::snapshot_bytes_from_body(&body, now_ns);
    match write_memo_snapshot(store, &worker.process_id(), bytes).await {
        Ok(()) => {
            *last_body = Some(body);
            true
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "maintenance: memo snapshot write failed; retried next cycle (fail-open, \
                 ADR-0065 decision 3)"
            );
            false
        }
    }
}

/// Up to 10% jitter over `base`, so co-started replicas' maintenance ticks do
/// not run in lockstep (same rationale as the fold task's jitter).
fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_maintain::{AUDIT_HOLD_SHARD, RetentionPolicy, shard_hold_scopes, write_hold_set};
    use ravel_object_store::GetRange;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
    use ravel_object_store::list_all;
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

    use ravel_ingest::{IdempotencyReceipt, marker_key, write_marker};

    /// Real wall-clock nanoseconds per hour, matching the private constant
    /// every ingest-hour-bucket computation in this crate and ravel-ingest
    /// shares (`run_tick` always uses the real [`WallClock`], never an
    /// injected one, so tests that exercise the idempotency sweep must derive
    /// "now" the same way production does rather than fix a clock).
    const TEST_NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// A single-replica worker for the deterministic tenant-tick tests: its
    /// solo live set (`{self}`) owns every unit, so `run_tick` and
    /// `run_discovery_cycle` behave exactly as the pre-ADR-0065 unconditional
    /// walk. It keeps the default unit concurrency (4), so these tests also
    /// exercise the bounded-concurrent per-unit path, whose per-shard effects
    /// are over disjoint keyspaces and therefore identical to a sequential walk.
    fn solo_worker() -> WorkerSet {
        WorkerSet::with_defaults(0)
    }

    /// The acceptance test named by issue #746 / experiment S5-E6 (ADR-0065
    /// decisions 1 and 2): two maintain replicas sharing one store partition the
    /// unit set rather than both paying for it.
    ///
    /// Two `WorkerSet`s each heartbeat into one shared `MemoryStore`; both
    /// compute a live set, which must converge to include both processes and be
    /// identical. For every `(Metrics, shard)` unit both must compute the same
    /// rendezvous owner (the formula is deterministic given the same live set),
    /// and exactly one of the two must own it. Finally, running each process's
    /// `run_tick` over the same tenant with its own cold memo must process every
    /// unit exactly once across the two -- proven by the combined `already_done`
    /// equalling the unit count (not twice it: no double-pay), and by the two
    /// memos partitioning the owned shards disjointly.
    #[tokio::test]
    async fn two_replicas_partition_units_without_double_pay() {
        const SHARDS: u32 = 8;
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        // One below-threshold (terminal) bucket per shard: each is a real unit
        // of maintenance work, classified `already_done` when evaluated.
        for shard in 0..SHARDS {
            publish_terminal_bucket_at_shard(&store, &tenant_id, shard).await;
        }

        // Two replicas, each with the default membership/timing. A single clock
        // reading for both heartbeats keeps both fresh within the liveness
        // window.
        let now = 1_000 * TEST_NS_PER_HOUR;
        let a = WorkerSet::with_defaults(now);
        let b = WorkerSet::with_defaults(now);
        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");

        // Live sets converge: each sees both processes, and the sorted sets are
        // identical (so the rendezvous input is identical on both sides).
        let live_a = a.live_set(&store, now).await.expect("a live set");
        let live_b = b.live_set(&store, now).await.expect("b live set");
        assert_eq!(live_a, live_b, "both replicas compute the same live set");
        assert!(live_a.contains(&a.process_id()) && live_a.contains(&b.process_id()));
        assert_eq!(live_a.len(), 2);

        // Ownership is computed identically by both, and every unit is owned by
        // exactly one replica (disjoint and complete).
        let mut owned_by_a = 0u32;
        let mut owned_by_b = 0u32;
        for shard in 0..SHARDS {
            let a_owns = a.owns_unit(&live_a, &tenant, Signal::Metrics, shard);
            let b_owns = b.owns_unit(&live_b, &tenant, Signal::Metrics, shard);
            assert_ne!(
                a_owns, b_owns,
                "shard {shard} must be owned by exactly one of the two replicas"
            );
            if a_owns {
                owned_by_a += 1;
            } else {
                owned_by_b += 1;
            }
        }
        assert_eq!(owned_by_a + owned_by_b, SHARDS, "every unit is owned");

        // Run both replicas' ticks over the same tenant, each with its own cold
        // memo. Every unit is processed by exactly one: the combined
        // `already_done` is the unit count (not double it), and each memo holds
        // exactly that replica's owned shards.
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);

        let mut memo_a = MaintainMemo::with_default_interval();
        let report_a = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            SHARDS,
            &mut memo_a,
            &safety,
            &ownership,
            &a,
            &live_a,
        )
        .await;

        let mut memo_b = MaintainMemo::with_default_interval();
        let report_b = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            SHARDS,
            &mut memo_b,
            &safety,
            &ownership,
            &b,
            &live_b,
        )
        .await;

        assert_eq!(
            report_a.already_done + report_b.already_done,
            SHARDS as usize,
            "every unit processed exactly once across the two replicas (no double-pay)"
        );
        assert_eq!(
            report_a.already_done, owned_by_a as usize,
            "replica A processed exactly the units it owns"
        );
        assert_eq!(
            report_b.already_done, owned_by_b as usize,
            "replica B processed exactly the units it owns"
        );
        assert_eq!(
            memo_a.len() + memo_b.len(),
            SHARDS as usize,
            "the two memos partition the owned shards disjointly and completely"
        );
    }

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
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        let report = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            4,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        // Nothing to maintain, nothing memoized: a subsequent tick would still
        // find nothing to skip.
        assert_eq!(report, MaintainReport::default());
        assert!(memo.is_empty());
    }

    /// THE deliverable-3 acceptance test (ADR-0066 decision 6): retention keeps
    /// running for a tenant whose token was removed from `sys/auth`. This closes
    /// the named bug -- removing a token to "deprovision" a tenant must not
    /// silently stop compacting, sweeping, or retention-enforcing its still
    /// present data.
    ///
    /// The tenant has a durable config record (active) and one sealed bucket at
    /// ingest hour 0 (1970), decades past any retention window. The discovery
    /// cycle is run with the flag restriction EMPTY -- exactly what a
    /// token-derived restriction becomes once the tenant's token is removed --
    /// and retention must still retire the expired bucket, because the
    /// lifecycle-aware discovery keeps the tenant maintained on the strength of
    /// its config record, not its token. As a control, the pre-fix
    /// `discover_and_restrict` excludes the tenant under the same empty
    /// restriction, so its retention would NOT run.
    #[tokio::test]
    async fn retention_continues_after_token_removal_when_a_config_record_is_present() {
        use ravel_catalog::{TenantConfig, TenantLifecycleState, set_tenant_config};

        use crate::tenant_discovery::{
            TenantDiscoveryMetrics, discover_and_restrict, discover_and_restrict_by_lifecycle,
        };

        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        // One sealed bucket at ingest hour 0 (1970): decades older than any
        // retention window, so retention must retire it.
        publish_terminal_bucket(&store, &tenant_id).await;
        // A durable config record marks the tenant active. This is what keeps it
        // maintained after its token is gone.
        set_tenant_config(
            &store,
            &tenant,
            &TenantConfig::new(TenantLifecycleState::Active),
            1_000,
        )
        .await
        .expect("write config record");

        // A retention config that would retire the 1970 bucket: a one-year
        // window, comfortably above the ADR-0019 floor and far below the
        // bucket's age against the real wall clock.
        const YEAR_NS: i64 = 365 * 24 * 3_600 * 1_000_000_000;
        let compactor = CompactorConfig::default();
        let max_lag = ravel_catalog::CatalogConfig::default().max_ingest_lag_ns;
        let retention = RetentionConfig::from_policy(
            RetentionPolicy {
                default: Some(YEAR_NS),
                tenants: Vec::new(),
            },
            &compactor,
            max_lag,
        )
        .expect("valid retention config");

        // The token was removed, so the flag restriction is now empty. Control:
        // the pre-fix restriction drops the tenant entirely.
        let empty_restrict: Vec<TenantHash> = Vec::new();
        let old = discover_and_restrict(&store, Some(&empty_restrict))
            .await
            .expect("discover");
        assert!(
            old.maintained.is_empty(),
            "control: the pre-fix restriction drops the tenant once its token is gone"
        );

        // The fix: the lifecycle-aware discovery keeps the tenant maintained.
        // The empty set is the token-derived fallback; no explicit
        // `--maintain-tenant` narrowing is configured.
        let discovered = discover_and_restrict_by_lifecycle(&store, Some(&empty_restrict))
            .await
            .expect("lifecycle discover");
        assert_eq!(
            discovered.maintained,
            vec![tenant],
            "the tenant stays maintained on its config record despite the empty token restriction"
        );

        // Drive the real maintenance discovery cycle (which uses the
        // lifecycle-aware discovery) with the empty restriction and assert its
        // retention actually retired the expired bucket -- maintenance genuinely
        // continues for the deprovisioned-by-token tenant.
        let metrics = TenantDiscoveryMetrics::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        let mut memo = MaintainMemo::with_default_interval();
        let report = run_discovery_cycle(
            &store,
            Some(&empty_restrict),
            &compactor,
            &retention,
            1,
            &mut memo,
            &metrics,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert!(
            report.retired >= 1,
            "retention retired the expired bucket for the token-removed tenant \
             (retention enforcement continues), got {report:?}"
        );
        assert_eq!(
            metrics.tenants_maintained(),
            1,
            "the discovery cycle recorded the tenant as maintained"
        );
    }

    /// `run_tick` must actually call the idempotency-marker sweep for logs
    /// and spans, once per signal, using the real [`WallClock`] (issue #531's
    /// adversarial checkpoint: the sweep previously had no production
    /// caller). Seeds one marker per maintained signal at ingest hour 0
    /// (1970, far past any real dedup window) and one at the real current
    /// ingest hour (still within window), then asserts the past-window
    /// marker is gone and the in-window one survives after a single tick --
    /// for logs and spans. A metrics marker is never written in production
    /// (ADR-0051 §5), so this also proves the sweep is not mistakenly called
    /// for `Signal::Metrics`: a metrics marker seeded the same way must
    /// survive regardless of age, since nothing calls the marker sweep for
    /// that signal at all.
    #[tokio::test]
    async fn run_tick_sweeps_idempotency_markers_for_logs_and_spans_only() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        let now_hour = u32::try_from(SystemClock.now_ns().div_euclid(TEST_NS_PER_HOUR))
            .expect("real wall clock ingest hour bucket fits in u32");

        for signal in [Signal::Logs, Signal::Spans, Signal::Metrics] {
            write_marker(
                &store,
                &tenant_id,
                signal,
                b"past-window",
                0,
                &IdempotencyReceipt {
                    written_count: 1,
                    commit_token: "v2:token".to_string(),
                },
            )
            .await
            .expect("seed past-window marker");
            write_marker(
                &store,
                &tenant_id,
                signal,
                b"in-window",
                now_hour,
                &IdempotencyReceipt {
                    written_count: 1,
                    commit_token: "v2:token".to_string(),
                },
            )
            .await
            .expect("seed in-window marker");
        }

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        for signal in [Signal::Logs, Signal::Spans] {
            let past_key = marker_key(&tenant_id, signal, b"past-window", 0);
            assert!(
                store.get(&past_key, GetRange::Full).await.is_err(),
                "{signal:?}'s past-window marker must be swept by a real tick"
            );
            let in_window_key = marker_key(&tenant_id, signal, b"in-window", now_hour);
            assert!(
                store.get(&in_window_key, GetRange::Full).await.is_ok(),
                "{signal:?}'s in-window marker must survive a real tick"
            );
        }

        // Metrics markers are never produced in production and the sweep is
        // never called for that signal; both survive regardless of age.
        for hour in [0, now_hour] {
            let key = marker_key(
                &tenant_id,
                Signal::Metrics,
                if hour == 0 {
                    b"past-window"
                } else {
                    b"in-window"
                },
                hour,
            );
            assert!(
                store.get(&key, GetRange::Full).await.is_ok(),
                "a Metrics marker must never be touched: the sweep is not called for that signal"
            );
        }
    }

    // --- ADR-0064 selective erasure, driven through `run_tick` (issue #997) ---

    /// The injected "now" every erasure test ticks at: hour 10_000 (1971), far
    /// past the seal margin for the ingest-hour-0 bucket those tests publish,
    /// so the rewrite pass sees a sealed bucket without any wall-clock
    /// dependence.
    const TEST_ERASURE_NOW_NS: i64 = 10_000 * TEST_NS_PER_HOUR;

    /// The label whose value the erasure predicate names (the "subject").
    const TEST_SUBJECT_LABEL: &str = "user_id";
    /// The subject to erase, and the one that must survive untouched.
    const TEST_ERASED_SUBJECT: &str = "u123";
    const TEST_SURVIVING_SUBJECT: &str = "u999";

    fn subject_labels(subject: &str) -> LabelSet {
        LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: "http_requests".to_string(),
            },
            Label {
                name: TEST_SUBJECT_LABEL.to_string(),
                value: subject.to_string(),
            },
        ])
        .expect("valid labels")
    }

    /// Publish one sealed metrics bucket at ingest hour 0 holding two series:
    /// the erasure subject and one bystander, one sample each. A single L0
    /// input keeps the bucket below `min_compaction_inputs`, so the tick's
    /// compaction pass leaves the live record set as raw L0 and the erasure
    /// rewrite is what acts on it.
    async fn publish_erasable_bucket(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
        let tenant_hash = tenant.hash();
        let mut series: Vec<SeriesInput> = [TEST_ERASED_SUBJECT, TEST_SURVIVING_SUBJECT]
            .into_iter()
            .map(|subject| {
                let labels = subject_labels(subject);
                SeriesInput {
                    series_id: SeriesId::compute(tenant, "http_requests", &labels)
                        .expect("series id"),
                    labels,
                    samples: vec![Sample {
                        ts_ns: 1_000,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        series.sort_by_key(|s| s.series_id);

        let writer_id = Uuid::from_u128(9_100);
        let written = SegmentWriter::write(
            series,
            SegmentIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            },
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// Submit one erasure request exactly as `ravel-cli erase submit` does: a
    /// validated `ErasureRequest` written `CreateIfAbsent` to its `.dreq` key.
    /// Returns the key.
    async fn submit_erasure_request(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        request_id: Uuid,
        subject: &str,
        now_ns: i64,
    ) -> String {
        let request = ErasureRequest {
            format_version: ravel_commit::erasure::FORMAT_VERSION,
            tenant_hash: tenant.0.to_vec(),
            signal: ravel_commit::signal::to_proto(signal) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: now_ns,
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: TEST_SUBJECT_LABEL.to_string(),
                value: subject.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: "dsar".to_string(),
        };
        ravel_commit::erasure::validate_request(&request).expect("valid request");
        let key = keys::erasure_request_key(tenant, signal, request_id).expect("dreq key");
        store
            .put(
                &key,
                ravel_commit::erasure::encode_request(&request),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("submit .dreq");
        key
    }

    /// Every `RewriteRecord` present in `(tenant, Metrics, shard 0, hour 0)`,
    /// decoded, in key order.
    async fn rewrite_records(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
    ) -> Vec<ravel_proto::commit::v1::RewriteRecord> {
        let prefix =
            keys::commit_shard_hour_prefix(tenant, Signal::Metrics, 0, 0).expect("bucket prefix");
        let mut keys_found: Vec<String> = list_all(store, &prefix)
            .await
            .expect("list bucket")
            .into_iter()
            .map(|meta| meta.key)
            .filter(|key| key.contains("/rw."))
            .collect();
        keys_found.sort();
        let mut out = Vec::with_capacity(keys_found.len());
        for key in keys_found {
            let got = store.get(&key, GetRange::Full).await.expect("get rewrite");
            out.push(ravel_commit::erasure::decode_rewrite(&got.data).expect("decode rewrite"));
        }
        out
    }

    /// The label sets a rewrite record's output parts actually hold, read back
    /// out of the published segment objects. This is the "survivors are intact"
    /// oracle: it reads what a query would read, not what the record claims.
    async fn rewritten_label_sets(
        store: &dyn ObjectStoreBackend,
        record: &ravel_proto::commit::v1::RewriteRecord,
    ) -> Vec<LabelSet> {
        use ravel_segment::{ReaderLimits, decode_catalog_v5, open_from_full};

        let limits = ReaderLimits::default();
        let mut out = Vec::new();
        for part in &record.parts {
            let key = keys::reconstruct_rewrite_part_key(record, part).expect("part key");
            let got = store.get(&key, GetRange::Full).await.expect("get part");
            let object = got.data;
            let loc = open_from_full(&object, limits).expect("open part");
            for entry in decode_catalog_v5(&loc.footer, &object, limits).expect("decode catalog") {
                out.push(entry.entry.labels);
            }
        }
        out
    }

    /// THE reachability acceptance test for issue #997 (ADR-0064 decisions 3,
    /// 4, and 5): everything is driven through `run_tick` itself, never by
    /// calling `erasure_rewrite_bucket` or `sweep_erasure_requests` directly.
    ///
    /// Ingest one sealed bucket holding the erasure subject and one bystander,
    /// submit a `.dreq` naming the subject, and run a tick. The tick must
    /// publish a `RewriteRecord` that names the request, drops exactly the
    /// subject's one sample, and leaves the bystander's series intact in the
    /// output part -- and must write the request's `.done` in the same tick,
    /// because every bucket in the request's scope now carries a rewrite
    /// naming it. The `.dreq` must still be there: the protection horizon has
    /// not elapsed. Advance the injected clock past that horizon, tick again,
    /// and the `.dreq` must be gone while the (permanent, PII-free) `.done`
    /// survives.
    ///
    /// The flip that proves this test bites: comment out the
    /// `run_erasure_pass(...)` call in `run_tick_with_clock`'s per-signal loop
    /// and it fails at the first assertion (no rewrite record is ever
    /// published). Flipping only the `.dreq` sweep (dropping the
    /// `sweep_erasure_requests` call inside `run_erasure_pass`) fails at the
    /// final assertion instead.
    #[tokio::test]
    async fn run_tick_rewrites_for_an_erasure_request_then_sweeps_its_dreq() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_erasable_bucket(&store, &tenant_id).await;

        let clock = ravel_maintain::FixedClock::new(TEST_ERASURE_NOW_NS);
        let request_id = Uuid::from_u128(0xE7A5);
        let dreq_key = submit_erasure_request(
            &store,
            &tenant,
            Signal::Metrics,
            request_id,
            TEST_ERASED_SUBJECT,
            TEST_ERASURE_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        // Tick 1: the rewrite runs and the request completes.
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        let records = rewrite_records(&store, &tenant).await;
        assert_eq!(
            records.len(),
            1,
            "the tick must publish exactly one RewriteRecord for the bucket"
        );
        let record = &records[0];
        assert_eq!(
            record.drops.len(),
            1,
            "the rewrite applies exactly the one pending request"
        );
        assert_eq!(record.drops[0].request_id, request_id.to_string());
        assert_eq!(
            record.drops[0].dropped_count, 1,
            "exactly the subject's one sample is dropped"
        );

        let survivors = rewritten_label_sets(&store, record).await;
        assert_eq!(
            survivors,
            vec![subject_labels(TEST_SURVIVING_SUBJECT)],
            "the bystander series survives the rewrite intact and the subject is gone"
        );

        assert!(
            store.get(&done_key, GetRange::Full).await.is_ok(),
            "every bucket in scope carries the request, so the tick writes .done"
        );
        assert!(
            store.get(&dreq_key, GetRange::Full).await.is_ok(),
            ".dreq must survive until protection_horizon has elapsed past .done"
        );

        // Tick 2, past the horizon: the `.dreq` (which carries the subject
        // identifier) is swept; the PII-free `.done` is permanent.
        clock.set(TEST_ERASURE_NOW_NS + compactor.protection_horizon_ns + 1);
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert!(
            store.get(&dreq_key, GetRange::Full).await.is_err(),
            ".dreq must be swept once its .done is older than protection_horizon"
        );
        assert!(
            store.get(&done_key, GetRange::Full).await.is_ok(),
            ".done is permanent erasure evidence and must never be swept"
        );
    }

    /// The EJ-T5 idempotence guard, observed through the loop: a second tick
    /// over a bucket whose live `RewriteRecord` already names every pending
    /// request must publish nothing new. Without the `AlreadyApplied` skip the
    /// second tick would land a no-op rewrite superseding the first, every
    /// tick, forever.
    ///
    /// Driven by keeping the request pending across two ticks: the first tick
    /// writes `.done`, so the request would drop out of the pending set --
    /// deleting the `.done` between the ticks puts it back and re-runs the
    /// rewrite against an already-rewritten bucket, which is exactly the state
    /// the guard exists for.
    #[tokio::test]
    async fn second_tick_over_an_already_erased_bucket_publishes_no_new_rewrite() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_erasable_bucket(&store, &tenant_id).await;

        let clock = ravel_maintain::FixedClock::new(TEST_ERASURE_NOW_NS);
        let request_id = Uuid::from_u128(0xE7A6);
        submit_erasure_request(
            &store,
            &tenant,
            Signal::Metrics,
            request_id,
            TEST_ERASED_SUBJECT,
            TEST_ERASURE_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        let mut memo = MaintainMemo::with_default_interval();

        for _ in 0..2 {
            run_tick_with_clock(
                &clock,
                &store,
                &tenant,
                &compactor,
                &retention,
                1,
                &mut memo,
                &safety,
                &ownership,
                &worker,
                &worker.solo_live_set(),
            )
            .await;
            // Put the request back in the pending set for the next tick.
            store.delete(&done_key).await.expect("delete .done");
        }

        let records = rewrite_records(&store, &tenant).await;
        assert_eq!(
            records.len(),
            1,
            "a bucket already rewritten for every pending request must not be \
             republished: {records:?}"
        );
    }

    /// Failure path: the `.done` write faults. The ordering guarantee must
    /// hold anyway -- no `.done`, so the `.dreq` stays even once the
    /// protection horizon has passed, and the request stays pending. A later
    /// fault-free tick completes it, without redoing the rewrite (the bucket
    /// is `AlreadyApplied` by then).
    #[tokio::test]
    async fn a_failed_done_write_keeps_the_dreq_and_completes_on_a_later_tick() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_erasable_bucket(&inner, &tenant_id).await;

        let clock = ravel_maintain::FixedClock::new(TEST_ERASURE_NOW_NS);
        let request_id = Uuid::from_u128(0xE7A7);
        let dreq_key = submit_erasure_request(
            &inner,
            &tenant,
            Signal::Metrics,
            request_id,
            TEST_ERASED_SUBJECT,
            TEST_ERASURE_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

        // Fault the FIRST PUT of a `.done` object and nothing else, so the
        // same store serves the recovery ticks untouched.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Transient("completion write unavailable".into()),
            )
            .with_key_contains(".done")
            .with_occurrence(ravel_object_store::fault::Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        // Tick 1 under the fault, already past the horizon: the rewrite lands,
        // the `.done` write fails, and the `.dreq` must survive regardless.
        clock.set(TEST_ERASURE_NOW_NS + compactor.protection_horizon_ns + 1);
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected .done write fault must actually have fired"
        );
        assert!(
            store.get(&done_key, GetRange::Full).await.is_err(),
            "no .done was written"
        );
        assert!(
            store.get(&dreq_key, GetRange::Full).await.is_ok(),
            "a .dreq must never be swept while its .done is absent, horizon or not"
        );
        assert_eq!(
            rewrite_records(&store, &tenant).await.len(),
            1,
            "the rewrite itself landed before the completion write failed"
        );

        // Tick 2, past the one-shot fault: the request completes, and its
        // `.dreq` is swept on the tick after the horizon passes.
        clock.set(TEST_ERASURE_NOW_NS + 2 * compactor.protection_horizon_ns + 2);
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert!(
            store.get(&done_key, GetRange::Full).await.is_ok(),
            "the fault-free tick completes the request"
        );
        assert_eq!(
            rewrite_records(&store, &tenant).await.len(),
            1,
            "the completing tick must not republish a rewrite for a bucket that \
             already names the request"
        );

        clock.set(TEST_ERASURE_NOW_NS + 3 * compactor.protection_horizon_ns + 3);
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert!(
            store.get(&dreq_key, GetRange::Full).await.is_err(),
            "once the .done exists and its horizon passes, the .dreq is swept"
        );
    }

    /// A legal hold over the bucket pauses erasure (ADR-0064 §6): the rewrite
    /// is skipped, so no `.done` may be written and the `.dreq` must survive
    /// even past the protection horizon. Legal hold wins over erasure until an
    /// authorized human clears it.
    #[tokio::test]
    async fn a_held_bucket_defers_erasure_completion_and_keeps_the_dreq() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_erasable_bucket(&store, &tenant_id).await;

        let clock = ravel_maintain::FixedClock::new(TEST_ERASURE_NOW_NS);
        let request_id = Uuid::from_u128(0xE7A8);
        let dreq_key = submit_erasure_request(
            &store,
            &tenant,
            Signal::Metrics,
            request_id,
            TEST_ERASED_SUBJECT,
            TEST_ERASURE_NOW_NS,
        )
        .await;
        let done_key =
            keys::erasure_completion_key(&tenant, Signal::Metrics, request_id).expect("done key");

        // Hold every scope covering (Metrics, shard 0).
        for scope in shard_hold_scopes(&tenant, Signal::Metrics, 0).expect("valid hold scopes") {
            write_hold_set(
                &store,
                &tenant,
                Uuid::new_v4(),
                TEST_ERASURE_NOW_NS,
                &scope,
                "litigation hold over the erasure subject's bucket",
            )
            .await
            .expect("set hold");
        }

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        clock.set(TEST_ERASURE_NOW_NS + compactor.protection_horizon_ns + 1);
        run_tick_with_clock(
            &clock,
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert!(
            rewrite_records(&store, &tenant).await.is_empty(),
            "a held bucket must not be rewritten"
        );
        assert!(
            store.get(&done_key, GetRange::Full).await.is_err(),
            "a deferred request must not be marked complete"
        );
        assert!(
            store.get(&dreq_key, GetRange::Full).await.is_ok(),
            "the request stays pending while the hold is in force"
        );
    }

    /// The `.done` predicate hash is what makes a permanent completion record
    /// PII-free, so it must be stable across matcher orderings (two
    /// submissions of the same erasure hash the same) and must never contain
    /// the plaintext subject.
    #[test]
    fn erasure_predicate_hash_is_order_stable_and_carries_no_plaintext() {
        let matcher = |key: &str, value: &str| ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: key.to_string(),
            value: value.to_string(),
        };
        let base = ErasureRequest {
            format_version: ravel_commit::erasure::FORMAT_VERSION,
            tenant_hash: TenantId::new("acme").hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: Uuid::from_u128(1).to_string(),
            created_unix_ns: 10,
            predicate: vec![matcher("user_id", "u123"), matcher("region", "eu")],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: "dsar".to_string(),
        };
        let mut reordered = base.clone();
        reordered.predicate.reverse();
        // A different reason and submit time is the same erasure.
        reordered.reason = "second submit".to_string();
        reordered.created_unix_ns = 99;
        assert_eq!(
            erasure_predicate_hash(&base),
            erasure_predicate_hash(&reordered),
            "matcher order and non-predicate fields must not change the hash"
        );

        let mut different_value = base.clone();
        different_value.predicate[0] = matcher("user_id", "u124");
        assert_ne!(
            erasure_predicate_hash(&base),
            erasure_predicate_hash(&different_value),
            "a different subject must hash differently"
        );

        let mut windowed = base.clone();
        windowed.window_start_ns = 1;
        windowed.window_end_ns = 2;
        assert_ne!(
            erasure_predicate_hash(&base),
            erasure_predicate_hash(&windowed),
            "the event-time window is part of what is erased, so it is hashed"
        );

        let hash = erasure_predicate_hash(&base);
        assert!(
            !hash.windows(4).any(|w| w == b"u123"),
            "the digest must not contain the subject identifier"
        );
    }

    /// Publish one real sealed segment plus its commit record into a past ingest
    /// hour of `(tenant, Metrics, shard 0)`. One input is below the default
    /// `min_compaction_inputs`, and with no retention policy the bucket stays
    /// live, so maintenance classifies it terminal (below-threshold): exactly
    /// the steady state the memo is meant to skip. The ingest hour is 0 (1970)
    /// so the real [`WallClock`] `run_tick` uses always sees it as sealed.
    async fn publish_terminal_bucket(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
        let tenant_hash = tenant.hash();
        let metric = "up";
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
            labels: label_set,
            samples: vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        }];
        let writer_id = Uuid::from_u128(7_000);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");

        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// Publish one below-threshold (terminal) sealed bucket into a specific
    /// `(tenant, Metrics, shard)` at ingest hour 0 (1970, always sealed vs the
    /// real `WallClock`), for the resharding scan-range test. Mirrors
    /// [`publish_terminal_bucket`] but lets the caller place data in a shard
    /// index outside the process's static `shard_count`.
    async fn publish_terminal_bucket_at_shard(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        shard: u32,
    ) {
        let tenant_hash = tenant.hash();
        let metric = "up";
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
            labels: label_set,
            samples: vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        }];
        let writer_id = Uuid::from_u128(u128::from(7_000 + shard));
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// ADR-0052 section 4: `run_tick` must maintain the post-reshard shard
    /// range, not the pre-reshard one. With a provisioning record recording an
    /// increase (generation 0 count 2, generation 1 count 4) and a bucket
    /// placed in shard 3 -- outside the process's static `shard_count` of 2 --
    /// the tick must still discover and evaluate that bucket. Under the old
    /// static `0..shard_count` loop shard 3 was never scanned; the
    /// generation-aware union range (max count across generations = 4) reaches
    /// it.
    #[tokio::test]
    async fn run_tick_maintains_post_reshard_shard_range() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        // Record generation 0 (count 2) before any out-of-range data exists,
        // then append generation 1 (count 4) activating at hour 1.
        ravel_catalog::validate_or_adopt(
            &store,
            &tenant,
            Signal::Metrics,
            2,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        ravel_catalog::append_generation(&store, &tenant, Signal::Metrics, 4, 1, 0)
            .await
            .expect("append generation 1");

        // A terminal bucket in shard 3: reachable only under the widened count.
        publish_terminal_bucket_at_shard(&store, &tenant_id, 3).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        // Static shard_count is 2, matching generation 0's count; the scan
        // range must nonetheless cover shard 3 via the generation history.
        let report = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            2,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            report.already_done, 1,
            "the shard-3 bucket (outside the static count-2 range) must be evaluated"
        );
        assert_eq!(
            memo.len(),
            1,
            "exactly the shard-3 terminal bucket is memoized; the pre-reshard \
             0..2 loop would have found nothing"
        );
    }

    /// A second `run_tick` with the same memo (a second tick) skips the buckets
    /// the first tick proved terminal: `skipped_terminal` rises from 0 to the
    /// bucket count, and the second tick issues strictly fewer GETs because the
    /// skipped bucket's per-bucket LIST/GET reads are elided (issue #280, #330).
    #[tokio::test]
    async fn second_tick_with_shared_memo_skips_terminal_buckets() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        // Per-bucket object reads: the memo elides the per-bucket LIST and GET
        // reads, so this is what shrinks between the cold and warm ticks. The
        // shard-level `list_delimited` runs on every tick and is excluded.
        let per_bucket_reads = |s: &StoreMetricsSnapshot| -> u64 { s.list.calls + s.get.calls };

        // Tick 1 (cold memo): full evaluation, nothing skipped. The single-input
        // bucket is below the compaction threshold, so it is already-done and
        // gets memoized as terminal.
        let before_first = store.metrics().snapshot();
        let first = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        let first_reads =
            per_bucket_reads(&store.metrics().snapshot()) - per_bucket_reads(&before_first);
        assert_eq!(first.skipped_terminal, 0, "cold memo skips nothing");
        assert_eq!(first.already_done, 1, "the below-threshold bucket is done");
        assert_eq!(memo.len(), 1, "the terminal bucket is memoized");
        assert!(first_reads > 0, "cold tick did per-bucket reads");

        // Tick 2 (warm memo): the bucket is skipped straight from the memo.
        let before_second = store.metrics().snapshot();
        let second = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        let second_reads =
            per_bucket_reads(&store.metrics().snapshot()) - per_bucket_reads(&before_second);
        assert_eq!(second.skipped_terminal, 1, "warm memo skips the bucket");
        assert_eq!(second.already_done, 0, "no per-bucket work redone");
        assert!(
            second_reads < first_reads,
            "the skipped tick reads fewer objects (first={first_reads}, second={second_reads})"
        );
    }

    /// ADR-0048 decision 1 / ADR-0042: a legal hold covering a bucket stops
    /// the real driver's retention path from ever physically deleting it.
    ///
    /// Retention's physical delete is horizon-gated (`retention_sweep_bucket`
    /// only sweeps once `now >= tombstone.retired_at_ns + protection_horizon_ns`),
    /// and a tombstone write itself is not lease-gated (only the physical
    /// delete is), so reaching the delete path this driver actually guards
    /// takes two ticks even with a zero horizon: the first tombstones the
    /// already-expired bucket, the second attempts the now horizon-elapsed
    /// physical sweep. The hold must block every delete in that second tick.
    #[tokio::test]
    async fn held_bucket_survives_retention_tick() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        // Cover all three prefixes a shard-level hold must (L0 data, commit
        // records, L1 parts), exactly as the CLI's --signal/--shard
        // convenience form does, so nothing in this bucket is left
        // unprotected.
        for scope in shard_hold_scopes(&tenant, Signal::Metrics, 0).expect("valid hold scopes") {
            write_hold_set(
                &store,
                &tenant,
                Uuid::new_v4(),
                SystemClock.now_ns(),
                &scope,
                "held for held_bucket_survives_retention_tick",
            )
            .await
            .expect("write hold set");
        }

        let compactor = CompactorConfig {
            protection_horizon_ns: 0,
            ..CompactorConfig::default()
        };
        let max_ingest_lag_ns = ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
        let floor_ns = compactor.retention_floor_ns(max_ingest_lag_ns);
        let retention = RetentionConfig::from_policy(
            RetentionPolicy {
                default: Some(floor_ns),
                tenants: Vec::new(),
            },
            &compactor,
            max_ingest_lag_ns,
        )
        .expect("valid retention policy");
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        // Tick 1: the bucket's one sample is from ingest hour 0 (1970), so any
        // valid retention window is already expired against the real wall
        // clock. Not memoized terminal (Tombstoned isn't a terminal state),
        // so tick 2 re-evaluates it for real rather than skipping it.
        let first = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert_eq!(first.retired, 1, "the expired bucket is tombstoned");

        // Tick 2: the tombstone's horizon has elapsed (zero protection
        // horizon), so this tick attempts the physical sweep. The hold must
        // block it entirely.
        let second = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert_eq!(
            second.retired, 1,
            "still counted retired (tombstoned), never actually swept"
        );

        let l0_prefix = &shard_hold_scopes(&tenant, Signal::Metrics, 0).expect("scopes")[0];
        let surviving = list_all(&store, l0_prefix)
            .await
            .expect("list held l0 prefix");
        assert!(
            !surviving.is_empty(),
            "held bucket's L0 data object must still exist after a retention tick"
        );

        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid commit shard prefix");
        let surviving_commit = list_all(&store, &commit_prefix)
            .await
            .expect("list held commit prefix");
        assert!(
            surviving_commit
                .iter()
                .any(|meta| keys::partition_bucket_entry(&meta.key)
                    .is_ok_and(|entry| matches!(entry, keys::BucketEntry::CommitRecord(_)))),
            "held bucket's commit record must still exist after a retention tick"
        );
    }

    /// ADR-0048 decision 1: a legal-hold refresh failure must never fall back
    /// to `NoLeases`. It must skip the whole tenant tick -- no bucket
    /// touched, nothing deleted -- and the failure must be visible (not
    /// swallowed). Uses `FaultStore` to fail the one LIST `LegalHoldCheck::refresh`
    /// issues (the audit hold shard's commit prefix) and asserts its fault
    /// counter to prove the fault actually fired, not just that nothing
    /// happened to be due for deletion.
    #[tokio::test]
    async fn hold_refresh_failure_skips_tenant_tick_and_deletes_nothing() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&inner, &tenant_id).await;

        let hold_prefix = keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_HOLD_SHARD)
            .expect("valid audit hold shard prefix");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("hold shard unavailable".into()),
            )
            .with_key_contains(hold_prefix),
        );
        let store = FaultStore::new(inner, plan);

        let compactor = CompactorConfig {
            protection_horizon_ns: 0,
            ..CompactorConfig::default()
        };
        let max_ingest_lag_ns = ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
        let floor_ns = compactor.retention_floor_ns(max_ingest_lag_ns);
        let retention = RetentionConfig::from_policy(
            RetentionPolicy {
                default: Some(floor_ns),
                tenants: Vec::new(),
            },
            &compactor,
            max_ingest_lag_ns,
        )
        .expect("valid retention policy");
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        let report = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            report,
            MaintainReport::default(),
            "a refresh failure must skip the whole tick, not just gate deletes"
        );
        assert_eq!(
            store.fault_count(Op::List, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected refresh fault must actually have fired"
        );
        assert!(memo.is_empty(), "a skipped tick memoizes nothing");
        assert_eq!(
            safety.legal_hold_refresh_failures(),
            1,
            "the refresh failure must be visible on the metrics endpoint, not just as a log line"
        );

        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid commit shard prefix");
        let surviving = list_all(store.inner(), &commit_prefix)
            .await
            .expect("list commit prefix");
        assert!(
            !surviving.is_empty(),
            "the tenant's bucket must be untouched when the hold refresh fails"
        );
    }

    /// The test the task spec requires by name: a tenant known only to
    /// storage (no `--tenant-token`, no `--maintain-tenant`, i.e. `restrict =
    /// None`) is discovered and maintained by the real driver wiring
    /// (ADR-0048 decision 3, issue #504, experiment L2). This is exactly the
    /// OIDC/mTLS-authenticated-tenant scenario findings S2-17/S5-09
    /// describe: the flag-derived set used to be empty and nothing ran.
    #[tokio::test]
    async fn storage_discovered_tenant_is_maintained_without_flags() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        let report = run_discovery_cycle(
            &store,
            None,
            &compactor,
            &retention,
            1,
            &mut memo,
            &metrics,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            report.already_done, 1,
            "the storage-discovered tenant's bucket must actually be evaluated"
        );
        assert_eq!(metrics.tenants_discovered(), 1);
        assert_eq!(metrics.tenants_maintained(), 1);
        assert_eq!(metrics.discovery_failures(), 0);
    }

    /// A flag restriction narrows the discovered set: a discovered tenant not
    /// named by `--tenant-token`/`--maintain-tenant` is excluded from the
    /// cycle (never maintained) and counted, rather than either running
    /// unconditionally or being indistinguishable from "storage didn't report
    /// it."
    #[tokio::test]
    async fn flag_restriction_excludes_a_discovered_tenant_and_counts_it() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme");
        let globex = TenantId::new("globex");
        publish_terminal_bucket(&store, &acme).await;
        publish_terminal_bucket(&store, &globex).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        let restrict = [acme.hash()];

        let report = run_discovery_cycle(
            &store,
            Some(&restrict),
            &compactor,
            &retention,
            1,
            &mut memo,
            &metrics,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            report.already_done, 1,
            "only the restricted-in tenant's bucket is evaluated"
        );
        assert_eq!(metrics.tenants_discovered(), 2, "both tenants hold data");
        assert_eq!(
            metrics.tenants_maintained(),
            1,
            "only the restriction-named tenant is maintained"
        );
        assert_eq!(
            memo.len(),
            1,
            "only the maintained tenant's bucket is memoized"
        );
    }

    /// ADR-0048 decision 3: a tenant discovery failure (the `list_delimited("t/")`
    /// LIST erroring) must skip the entire cycle -- no tenant's tick runs --
    /// and must never fall back to an empty tenant set and report success.
    /// Uses `FaultStore` to fail the discovery LIST specifically and asserts
    /// its fault counter to prove the fault actually fired, not just that
    /// there happened to be nothing to do.
    #[tokio::test]
    async fn discovery_failure_skips_cycle_without_running_empty_set() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        publish_terminal_bucket(&inner, &tenant_id).await;

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("tenant discovery unavailable".into()),
            )
            .with_key_contains("t/"),
        );
        let store = FaultStore::new(inner, plan);

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();

        let report = run_discovery_cycle(
            &store,
            None,
            &compactor,
            &retention,
            1,
            &mut memo,
            &metrics,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;

        assert_eq!(
            report,
            MaintainReport::default(),
            "a discovery failure must skip the whole cycle, never run an empty set"
        );
        assert_eq!(
            store.fault_count(Op::List, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected discovery fault must actually have fired"
        );
        assert_eq!(metrics.discovery_failures(), 1);
        assert_eq!(
            metrics.tenants_discovered(),
            0,
            "gauges stay at their last known-good value, never reporting this failed cycle"
        );
        assert!(memo.is_empty(), "a skipped cycle memoizes nothing");
    }

    /// `run_loop`'s discovery arm must survive across `select!` iterations:
    /// with the default heartbeat cadence (60s) shorter than the discovery
    /// interval (300s), a discovery sleep written as a bare `select!` branch
    /// expression is silently rebuilt (and its countdown restarted) every
    /// time the heartbeat arm fires first, so discovery never elapses.
    /// Drives the real `run_loop` under a paused clock, injecting a
    /// persistent discovery-LIST fault so each actual discovery attempt is
    /// observable via `discovery_failures()` -- proving the discovery arm
    /// fired repeatedly over simulated time, not just that the loop didn't
    /// panic.
    #[tokio::test(start_paused = true)]
    async fn discovery_arm_fires_repeatedly_despite_a_shorter_heartbeat() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("discovery unavailable (test)".into()),
            )
            .with_key_contains("t/"),
        );
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(inner, plan));

        let compactor = Arc::new(CompactorConfig::default());
        let retention = Arc::new(RetentionConfig::default());
        let metrics = Arc::new(TenantDiscoveryMetrics::default());
        let safety = Arc::new(MaintenanceSafetyMetrics::default());
        let ownership = Arc::new(MaintenanceOwnershipMetrics::new(
            DEFAULT_STALLED_AFTER_INTERVALS,
        ));
        let worker = Arc::new(WorkerSet::with_defaults(0));
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let discovery_interval = Duration::from_secs(300);
        let handle = tokio::spawn(run_loop(
            store,
            None,
            compactor,
            retention,
            1,
            discovery_interval,
            metrics.clone(),
            safety,
            ownership,
            worker,
            Arc::new(SystemRng),
            shutdown_rx,
        ));

        // 30 simulated minutes: 30 heartbeats at the default 60s cadence
        // (which used to starve discovery outright), against roughly 6
        // discovery cycles at 300s (plus up to 10% jitter). Advance in small
        // steps so the paused-time runtime actually polls and wakes the
        // spawned task between each, rather than one giant jump.
        for _ in 0..180 {
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::task::yield_now().await;
        }

        assert!(
            metrics.discovery_failures() >= 4,
            "discovery must have fired repeatedly over 30 simulated minutes at a 300s interval, \
             got {} -- the discovery arm is starved again",
            metrics.discovery_failures()
        );

        handle.abort();
    }

    /// ADR-0065 decision 3 (issue #747): the durable memo write is debounced.
    /// A first persist writes the snapshot object; a second persist over an
    /// unchanged memo writes nothing (no PUT); and a persist after the memo
    /// gained a bucket writes again. Counts PUTs through an `InstrumentedStore`
    /// around each `persist_memo_snapshot` call so the assertion isolates the
    /// persist's own write from the publishing/tick writes.
    #[tokio::test]
    async fn memo_snapshot_write_is_debounced_on_unchanged_tick() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = solo_worker();
        let mut memo = MaintainMemo::with_default_interval();

        // A tick populates the memo with the one terminal bucket.
        run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &safety,
            &ownership,
            &worker,
            &worker.solo_live_set(),
        )
        .await;
        assert_eq!(memo.len(), 1, "the tick memoized the terminal bucket");

        let now = SystemClock.now_ns();
        let mut last_body: Option<Vec<u8>> = None;

        // First persist: writes the snapshot object (one PUT).
        let before = store.metrics().snapshot();
        assert!(
            persist_memo_snapshot(&store, &worker, &memo, &mut last_body, now).await,
            "first persist writes"
        );
        let after = store.metrics().snapshot();
        assert_eq!(after.put.calls - before.put.calls, 1, "one snapshot PUT");

        // Second persist over the unchanged memo: writes nothing, even though
        // the timestamp advanced (debounce compares the timestamp-free body).
        let before = store.metrics().snapshot();
        assert!(
            !persist_memo_snapshot(&store, &worker, &memo, &mut last_body, now + 1_000_000).await,
            "an unchanged memo persists nothing"
        );
        let after = store.metrics().snapshot();
        assert_eq!(
            after.put.calls - before.put.calls,
            0,
            "the debounced tick issues no PUT"
        );

        // A memo change writes again: here the terminal set emptied (as it
        // would after the bucket was swept), so the body differs from the last
        // written one and the debounce lets the write through.
        let emptied = MaintainMemo::with_default_interval();
        let before = store.metrics().snapshot();
        assert!(
            persist_memo_snapshot(&store, &worker, &emptied, &mut last_body, now + 2_000_000).await,
            "a changed memo persists again"
        );
        let after = store.metrics().snapshot();
        assert_eq!(
            after.put.calls - before.put.calls,
            1,
            "the changed memo re-writes"
        );
    }

    /// ADR-0065 decision 3 (issue #747): warm start and ownership handoff
    /// through real store objects. Worker A maintains a unit, memoizes its
    /// terminal bucket, and persists a durable snapshot. Worker B -- a distinct
    /// process that now owns the unit -- reads the snapshots back from the store
    /// and seeds its cold memo from A's, then skips the bucket A already proved
    /// terminal rather than cold-rescanning it.
    #[tokio::test]
    async fn warm_start_seeds_successor_from_predecessor_snapshot_through_store() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);

        // Worker A warms and persists its snapshot.
        let worker_a = solo_worker();
        let mut memo_a = MaintainMemo::with_default_interval();
        run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo_a,
            &safety,
            &ownership,
            &worker_a,
            &worker_a.solo_live_set(),
        )
        .await;
        assert_eq!(memo_a.len(), 1);
        let now = SystemClock.now_ns();
        let mut last_body = None;
        assert!(
            persist_memo_snapshot(&store, &worker_a, &memo_a, &mut last_body, now).await,
            "A persists its snapshot"
        );

        // Worker B (a different process id) reads the snapshots and warm-starts.
        let worker_b = WorkerSet::with_defaults(0);
        let live_b = worker_b.solo_live_set();
        let snapshots = read_all_memo_snapshots(&store)
            .await
            .expect("read snapshots");
        assert_eq!(snapshots.len(), 1, "A's one snapshot is on the store");

        let mut memo_b = MaintainMemo::with_default_interval();
        let (b_units, b_buckets) =
            seed_memo_from_snapshots(&mut memo_b, &snapshots, now, &worker_b, &live_b);
        assert_eq!(b_units, 1, "B seeds the one unit it owns from A's snapshot");
        assert_eq!(b_buckets, 1);
        assert_eq!(memo_b.len(), 1, "B's memo warm-started from the store");

        // B's tick skips the seeded terminal bucket (no cold rescan).
        let report = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo_b,
            &safety,
            &ownership,
            &worker_b,
            &live_b,
        )
        .await;
        assert_eq!(
            report.skipped_terminal, 1,
            "B skips A's terminal bucket after a store-backed warm start"
        );
        assert_eq!(report.already_done, 0, "no per-bucket work redone by B");
    }

    /// ADR-0065 decision 3 (issue #747): the reseed trigger and the ownership
    /// filter under a *genuine* handoff, with two independent live-set views
    /// rather than two `solo_live_set()`s (which make ownership unconditional and
    /// the `computed != live_set` filter vacuous). Two replicas A and B share a
    /// live set `{A, B}`; a shard the rendezvous assigns to A is warmed and
    /// snapshotted by A. When A departs, B's live set becomes `{B}` and that
    /// shard's ownership moves to B. This test asserts:
    ///
    /// 1. `membership_changed` fires on the real change (`{A,B}` -> `{B}`) and
    ///    stays quiet on stable membership (`{A,B}` == `{A,B}`), so a transient
    ///    single-replica worker is not stuck cold and a stable one does not
    ///    reseed every heartbeat.
    /// 2. Seeding from A's snapshot with B's *pre-handoff* view `{A,B}` seeds
    ///    nothing (B did not own the shard then) -- proving the `owns_unit`
    ///    filter is genuine, not vacuously true.
    /// 3. Seeding with B's *post-handoff* view `{B}` seeds the shard and B's tick
    ///    skips it rather than cold-rescanning.
    #[tokio::test]
    async fn reseed_and_seeding_track_genuine_ownership_handoff() {
        const SHARDS: u32 = 8;
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        // Two live replicas sharing one store; both compute the same live set.
        let now = SystemClock.now_ns();
        let a = WorkerSet::with_defaults(now);
        let b = WorkerSet::with_defaults(now);
        a.write_heartbeat(&store, now).await.expect("a heartbeat");
        b.write_heartbeat(&store, now).await.expect("b heartbeat");
        let live_ab = a.live_set(&store, now).await.expect("a live set");
        assert_eq!(live_ab, b.live_set(&store, now).await.expect("b live set"));
        assert_eq!(live_ab.len(), 2, "both replicas are live");

        // Pick a shard the rendezvous assigns to A under {A, B}: B does not own
        // it yet, so the pre-handoff filter must reject it.
        let a_shard = (0..SHARDS)
            .find(|shard| a.owns_unit(&live_ab, &tenant, Signal::Metrics, *shard))
            .expect("A owns at least one shard");
        assert!(
            !b.owns_unit(&live_ab, &tenant, Signal::Metrics, a_shard),
            "the chosen shard is A's, not B's, before the handoff"
        );
        publish_terminal_bucket_at_shard(&store, &tenant_id, a_shard).await;

        // A warms its memo over its owned shards and persists a durable snapshot.
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let mut memo_a = MaintainMemo::with_default_interval();
        run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            SHARDS,
            &mut memo_a,
            &safety,
            &ownership,
            &a,
            &live_ab,
        )
        .await;
        assert_eq!(memo_a.len(), 1, "A memoized its one terminal bucket");
        let snap_now = SystemClock.now_ns();
        let mut last_body = None;
        assert!(
            persist_memo_snapshot(&store, &a, &memo_a, &mut last_body, snap_now).await,
            "A persists its snapshot"
        );

        // Reseed trigger: stable membership is quiet; the real handoff fires it.
        assert!(
            !membership_changed(&live_ab, &live_ab),
            "stable membership must not trigger a reseed"
        );
        let live_b = b.solo_live_set();
        assert!(
            membership_changed(&live_ab, &live_b),
            "A's departure ({{A,B}} -> {{B}}) must trigger a reseed"
        );

        let snapshots = read_all_memo_snapshots(&store)
            .await
            .expect("read snapshots");

        // Counter-case: B's PRE-handoff view {A,B} does not own the shard, so
        // seeding filters it out. Proves the ownership filter is not vacuous.
        let mut memo_b_before = MaintainMemo::with_default_interval();
        let (units_before, buckets_before) =
            seed_memo_from_snapshots(&mut memo_b_before, &snapshots, snap_now, &b, &live_ab);
        assert_eq!(
            (units_before, buckets_before),
            (0, 0),
            "B seeds nothing while it does not yet own the shard"
        );
        assert_eq!(memo_b_before.len(), 0);

        // Post-handoff view {B}: B now owns the shard and seeds it from A's
        // snapshot, then skips it on its own tick (no cold rescan).
        let mut memo_b = MaintainMemo::with_default_interval();
        let (units, buckets) =
            seed_memo_from_snapshots(&mut memo_b, &snapshots, snap_now, &b, &live_b);
        assert_eq!(units, 1, "B seeds the one unit it took over");
        assert_eq!(buckets, 1);

        let report = run_tick(
            &store,
            &tenant,
            &compactor,
            &retention,
            SHARDS,
            &mut memo_b,
            &safety,
            &ownership,
            &b,
            &live_b,
        )
        .await;
        assert_eq!(
            report.skipped_terminal, 1,
            "B skips the handed-over terminal bucket instead of cold-rescanning"
        );
    }

    /// The un-trip an operator must not read as "resolved" (ADR-0048 decision
    /// 4, issue #500): a second, non-tripped sweep pass for the same signal
    /// drops `orphans_withheld` back to `0`, but `orphan_breaker_trips` -- the
    /// counter a first-trip alert fires on -- keeps the earlier trip on the
    /// record.
    #[test]
    fn orphan_breaker_withheld_gauge_drops_but_trip_counter_does_not() {
        let safety = MaintenanceSafetyMetrics::default();
        safety.record_sweep(Signal::Metrics, true, 42, 42);
        assert_eq!(safety.orphan_breaker_trips(Signal::Metrics), 1);
        assert_eq!(safety.orphans_withheld(Signal::Metrics), 42);

        safety.record_sweep(Signal::Metrics, false, 0, 0);
        assert_eq!(
            safety.orphan_breaker_trips(Signal::Metrics),
            1,
            "a cleared pass must never erase that a trip happened"
        );
        assert_eq!(
            safety.orphans_withheld(Signal::Metrics),
            0,
            "the withheld gauge reflects only the most recent pass"
        );

        // A different signal's counters are untouched.
        assert_eq!(safety.orphan_breaker_trips(Signal::Logs), 0);
        assert_eq!(safety.conservation_aborts(Signal::Logs), 0);
        assert_eq!(safety.legal_hold_refresh_failures(), 0);
    }

    /// ADR-0058 decision 1: `orphans_present` is a last-observed-value gauge
    /// that catches small-scale record loss the breaker never trips on. It
    /// carries the pass's total orphan-candidate count regardless of what
    /// happened to those candidates, and drops to whatever the latest pass
    /// found -- it is never sticky and never monotonic.
    #[test]
    fn orphans_present_gauge_tracks_latest_pass_and_is_not_sticky() {
        let safety = MaintenanceSafetyMetrics::default();

        // Breaker not tripped: candidates were deleted, so `present` is the
        // deleted count while `withheld` stays 0. This is exactly the
        // small-scale-loss case the breaker's thresholds are too coarse for.
        safety.record_sweep(Signal::Metrics, false, 0, 3);
        assert_eq!(safety.orphans_present(Signal::Metrics), 3);
        assert_eq!(safety.orphans_withheld(Signal::Metrics), 0);
        assert_eq!(
            safety.orphan_breaker_trips(Signal::Metrics),
            0,
            "a below-threshold pass with orphans present must not trip the breaker"
        );

        // Breaker tripped: candidates were withheld, so `present` equals the
        // withheld count (deleted is 0 on a tripped pass).
        safety.record_sweep(Signal::Metrics, true, 55, 55);
        assert_eq!(safety.orphans_present(Signal::Metrics), 55);
        assert_eq!(safety.orphans_withheld(Signal::Metrics), 55);

        // A subsequent clean pass with zero candidates resets the gauge to 0:
        // gauge semantics, last observed value, not a monotonic counter that
        // remembers the earlier 55.
        safety.record_sweep(Signal::Metrics, false, 0, 0);
        assert_eq!(
            safety.orphans_present(Signal::Metrics),
            0,
            "orphans_present reflects only the most recent pass, never sticky"
        );
        // The trip that happened is still on the durable counter, untouched by
        // the present gauge dropping to 0.
        assert_eq!(safety.orphan_breaker_trips(Signal::Metrics), 1);

        // A different signal is untouched throughout.
        assert_eq!(safety.orphans_present(Signal::Logs), 0);
    }

    /// A store wrapper that instruments `list_delimited` -- the call
    /// `scan_and_maintain_with_memo` makes first, via `list_shard_hours`,
    /// before touching anything else for a unit -- with a run of
    /// `tokio::task::yield_now` before delegating to `inner`. Widening this
    /// window gives every unit `run_bounded` has actually admitted a chance to
    /// reach the probe before any of them release it, so `peak` reports the
    /// true number of units `run_tick` runs concurrently rather than an
    /// artifact of one future finishing before the next starts.
    struct ConcurrencyProbeStore<S> {
        inner: S,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<S> ConcurrencyProbeStore<S> {
        fn new(inner: S) -> Self {
            ConcurrencyProbeStore {
                inner,
                in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for ConcurrencyProbeStore<S> {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            use std::sync::atomic::Ordering;
            let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(cur, Ordering::SeqCst);
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            let result = self.inner.list_delimited(prefix).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// `run_tick` never admits more than `worker.unit_concurrency()` units'
    /// scans at once, proven end to end through the real driver (not just
    /// through `run_bounded` in isolation, which ravel-fleet's
    /// `run_bounded_respects_the_concurrency_cap` already covers). Eight
    /// shards against a cap of 2, over both maintained signals, gives 16
    /// probed calls, far more than the cap, so a peak below or above 2 cannot
    /// be a scheduling coincidence.
    ///
    /// Proved to actually bind: with the `worker.unit_concurrency()` argument
    /// at the `run_bounded` call site in `run_tick` (this file) temporarily
    /// replaced by `units.len()`, this test's `peak == CAP` assertion failed
    /// with `peak == 8` (all of one signal's shards admitted at once); reverting
    /// that one-line substitution restored the pass.
    #[tokio::test]
    async fn run_tick_never_exceeds_configured_unit_concurrency() {
        use std::sync::atomic::Ordering;

        const SHARDS: u32 = 8;
        const CAP: usize = 2;

        let store = ConcurrencyProbeStore::new(MemoryStore::new());
        let in_flight = store.in_flight.clone();
        let peak = store.peak.clone();

        let tenant = TenantId::new("acme").hash();
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(DEFAULT_STALLED_AFTER_INTERVALS);
        let worker = WorkerSet::new(0, Duration::from_secs(60), 3, CAP);
        let live_set = worker.solo_live_set();

        let _report = run_tick(
            &store, &tenant, &compactor, &retention, SHARDS, &mut memo, &safety, &ownership,
            &worker, &live_set,
        )
        .await;

        assert_eq!(
            in_flight.load(Ordering::SeqCst),
            0,
            "every probed call released by the time the tick returns"
        );
        assert_eq!(
            peak.load(Ordering::SeqCst),
            CAP,
            "run_tick admits at most, and (with more units than the cap) exactly, \
             worker.unit_concurrency() unit scans concurrently"
        );
    }

    /// A unit that fails every tick, tenant lifecycle otherwise unchanged,
    /// accrues toward `ownership.units_stalled()` once it reaches
    /// `stalled_after_intervals` consecutive failures, and a single
    /// intervening success resets its counter to zero (no partial credit
    /// carried across a recovered tick). Failure is injected via `FaultStore`
    /// on `list_delimited`, the first call `scan_and_maintain_with_memo` makes
    /// for a unit, so the whole per-unit tick (scan and sweep both) reports an
    /// error for that unit on the faulted ticks.
    #[tokio::test]
    async fn units_stalled_fires_after_threshold_consecutive_failures_and_resets_on_success() {
        const THRESHOLD: u32 = 3;

        let tenant = TenantId::new("acme").hash();
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let worker = solo_worker();
        let live_set = worker.solo_live_set();
        let ownership = MaintenanceOwnershipMetrics::new(THRESHOLD);

        // Faults only (tenant, Metrics, shard 0)'s own listing: the legal-hold
        // refresh (a different shard, Signal::Audit) and the Logs/Spans shard-0
        // units stay clean, so only the one unit's tick fails.
        let shard_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid metrics shard prefix");

        // Three consecutive faulted ticks: the unit fails every time, so the
        // third crosses the threshold and the gauge goes to 1.
        for tick in 0..THRESHOLD {
            let plan = FaultPlan::empty().with_rule(
                Rule::new(
                    Op::List,
                    ScriptedFault::Transient("scan unavailable".into()),
                )
                .with_key_contains(shard_prefix.clone()),
            );
            let faulted = FaultStore::new(MemoryStore::new(), plan);
            let mut memo = MaintainMemo::with_default_interval();
            let _report = run_tick(
                &faulted,
                &tenant,
                &compactor,
                &retention,
                1,
                &mut memo,
                &MaintenanceSafetyMetrics::default(),
                &ownership,
                &worker,
                &live_set,
            )
            .await;
            let expected = u64::from(tick + 1 >= THRESHOLD);
            assert_eq!(
                ownership.units_stalled(),
                expected,
                "tick {tick}: stalled gauge must reflect exactly whether the \
                 threshold has been reached, not fire early or stay pinned"
            );
        }
        assert_eq!(ownership.units_stalled(), 1);

        // A clean tick (no fault) succeeds and resets this unit's streak.
        let clean = MemoryStore::new();
        let mut memo = MaintainMemo::with_default_interval();
        let _report = run_tick(
            &clean,
            &tenant,
            &compactor,
            &retention,
            1,
            &mut memo,
            &MaintenanceSafetyMetrics::default(),
            &ownership,
            &worker,
            &live_set,
        )
        .await;
        assert_eq!(
            ownership.units_stalled(),
            0,
            "a single success resets the unit's consecutive-failure streak"
        );
    }

    /// A unit that reaches the stall threshold and then LEAVES this process's
    /// owned set entirely -- a rendezvous re-partition when a peer joins,
    /// here -- must not keep pinning `units_stalled` (ADR-0065). Drives
    /// `(tenant, Metrics, shard 0)` to the threshold under a solo live set,
    /// then runs one more discovery cycle under a two-worker live set chosen
    /// so shard 0's rendezvous owner moves to the peer. This process's
    /// `run_tick` then never evaluates shard 0 at all, so
    /// `observe_unit_tick` is never called for it again; only
    /// `run_discovery_cycle`'s per-cycle `ownership.end_cycle()` (which
    /// prunes `UnitStallTracker.failures` to the current owned set) can
    /// clear its stranded entry. Reverting that call -- or reverting
    /// `UnitStallTracker::retain_owned`'s body to a no-op -- leaves the final
    /// assertion at 1 forever instead of 0.
    #[tokio::test]
    async fn units_stalled_drops_a_unit_that_leaves_the_owned_set() {
        const THRESHOLD: u32 = 3;

        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let discovery_metrics = TenantDiscoveryMetrics::default();
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(THRESHOLD);
        let worker = solo_worker();
        let live_solo = worker.solo_live_set();

        // A deterministic peer id whose presence in the live set flips shard
        // 0's rendezvous owner away from `worker` (the argmax is a function
        // of both ids, so some peer id must exist that outweighs `worker`'s
        // own weight for this specific unit key).
        let peer_id = (1u128..10_000)
            .map(Uuid::from_u128)
            .find(|candidate| {
                let live = vec![worker.process_id(), *candidate];
                !worker.owns_unit(&live, &tenant, Signal::Metrics, 0)
            })
            .expect("some peer id flips shard 0's rendezvous owner away from the solo worker");
        let live_ab = vec![worker.process_id(), peer_id];

        let inner = MemoryStore::new();
        publish_terminal_bucket(&inner, &tenant_id).await;

        // Faults only (tenant, Metrics, shard 0)'s own listing, exactly as
        // `units_stalled_fires_after_threshold_consecutive_failures_and_resets_on_success`
        // does: tenant discovery's `list_delimited("t/")` and the legal-hold
        // refresh (a different shard, Signal::Audit) stay clean.
        let shard_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid metrics shard prefix");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("scan unavailable".into()),
            )
            .with_key_contains(shard_prefix),
        );
        let store = FaultStore::new(inner, plan);
        let mut memo = MaintainMemo::with_default_interval();

        // Three consecutive faulted cycles while shard 0 is still owned:
        // drives its failure streak to the threshold.
        for _ in 0..THRESHOLD {
            let _report = run_discovery_cycle(
                &store,
                None,
                &compactor,
                &retention,
                1,
                &mut memo,
                &discovery_metrics,
                &safety,
                &ownership,
                &worker,
                &live_solo,
            )
            .await;
        }
        assert_eq!(
            ownership.units_stalled(),
            1,
            "shard 0 must cross the stall threshold while still owned"
        );

        // The peer joins: shard 0's rendezvous owner moves to it, so
        // `worker`'s owned set for this cycle no longer includes shard 0 at
        // all. Its failing entry is never observed again by this process.
        let _report = run_discovery_cycle(
            &store,
            None,
            &compactor,
            &retention,
            1,
            &mut memo,
            &discovery_metrics,
            &safety,
            &ownership,
            &worker,
            &live_ab,
        )
        .await;

        assert_eq!(
            ownership.units_stalled(),
            0,
            "a unit that leaves the owned set must not keep pinning units_stalled \
             forever -- without pruning stall history to the current cycle's \
             owned set, this stays at 1 even though the unit no longer ticks here"
        );
    }

    /// A store that (1) counts heartbeat PUTs (writes to `sys/maintain/workers/`)
    /// and (2) blocks tenant discovery: its `list_delimited("t/")` awaits a
    /// `tokio::time::sleep(block)` before delegating, so a discovery cycle stays
    /// stuck inside that await for `block` of simulated time. Every other
    /// operation delegates straight through -- crucially the heartbeat's own
    /// `live_set` read, which uses `list_all` (`.list`), not `list_delimited`,
    /// so it is never blocked by this wrapper.
    struct BlockingDiscoveryStore<S> {
        inner: S,
        heartbeat_writes: Arc<AtomicU64>,
        entered_block: Arc<std::sync::atomic::AtomicBool>,
        block: Duration,
    }

    impl<S> BlockingDiscoveryStore<S> {
        fn new(inner: S, block: Duration) -> Self {
            BlockingDiscoveryStore {
                inner,
                heartbeat_writes: Arc::new(AtomicU64::new(0)),
                entered_block: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                block,
            }
        }
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for BlockingDiscoveryStore<S> {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            if key.starts_with("sys/maintain/workers/") {
                self.heartbeat_writes.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            if prefix.starts_with("t/") {
                self.entered_block.store(true, Ordering::SeqCst);
                tokio::time::sleep(self.block).await;
            }
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Issue #796 (the real fix): the heartbeat must keep firing on cadence `H`
    /// even while a single discovery cycle runs far longer than the `3 * H`
    /// liveness window. Before the fix the heartbeat was a `select!` arm sharing
    /// `run_loop` with the discovery arm, so once `run_discovery_cycle(...).await`
    /// began the macro never re-entered and the heartbeat `interval` arm was
    /// never polled (`MissedTickBehavior::Delay` merely defers the tick). A cycle
    /// longer than `3 * H` therefore starved this process's own heartbeat, and
    /// siblings evicted it from their live sets mid-cycle.
    ///
    /// Drives the real `run_loop` under a paused clock with a store whose tenant-
    /// discovery `list_delimited("t/")` blocks for far longer than the whole test
    /// window, so every discovery cycle is stuck inside that await. It advances
    /// simulated time well past `3 * H` and asserts the heartbeat PUT count
    /// advanced during the blocked cycle.
    ///
    /// The flip that proves it bites: move the heartbeat back into `run_loop`'s
    /// `select!` as a `_ = heartbeat.tick()` arm alongside the (blocked)
    /// `discovery_sleep` arm. The heartbeat is then never polled while discovery
    /// awaits, so the write count does not advance during the block and the
    /// `>= 3` assertion fails at 0.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_continues_during_a_discovery_cycle_longer_than_the_liveness_window() {
        use std::sync::atomic::Ordering;

        // H = 1s, liveness factor 3 (so the window is 3 * H = 3s), default unit
        // concurrency. Discovery interval 1s so a cycle starts almost at once;
        // the blocking LIST then holds it for 3600s -- effectively forever for
        // this test.
        let worker = Arc::new(WorkerSet::new(0, Duration::from_secs(1), 3, 4));
        let store_impl = BlockingDiscoveryStore::new(MemoryStore::new(), Duration::from_secs(3600));
        let heartbeat_writes = store_impl.heartbeat_writes.clone();
        let entered_block = store_impl.entered_block.clone();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store_impl);

        let compactor = Arc::new(CompactorConfig::default());
        let retention = Arc::new(RetentionConfig::default());
        let metrics = Arc::new(TenantDiscoveryMetrics::default());
        let safety = Arc::new(MaintenanceSafetyMetrics::default());
        let ownership = Arc::new(MaintenanceOwnershipMetrics::new(
            DEFAULT_STALLED_AFTER_INTERVALS,
        ));
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_loop(
            store,
            None,
            compactor,
            retention,
            1,
            Duration::from_secs(1),
            metrics,
            safety,
            ownership,
            worker,
            Arc::new(SystemRng),
            shutdown_rx,
        ));

        // Advance until the discovery cycle has entered its (blocked) tenant
        // LIST. The immediate first heartbeat has already fired by now.
        for _ in 0..60 {
            if entered_block.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::advance(Duration::from_millis(200)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            entered_block.load(Ordering::SeqCst),
            "the discovery cycle must have entered its blocked tenant LIST"
        );
        let writes_before = heartbeat_writes.load(Ordering::SeqCst);

        // Advance ~5s = 5 * H, well past the 3s liveness window, while discovery
        // stays blocked. Small steps so the paused runtime actually wakes the
        // heartbeat task on each 1s boundary.
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        let writes_after = heartbeat_writes.load(Ordering::SeqCst);

        assert!(
            entered_block.load(Ordering::SeqCst),
            "the discovery cycle is still blocked (its LIST sleep dwarfs the test window)"
        );
        assert!(
            writes_after - writes_before >= 3,
            "the heartbeat must keep firing on cadence H (>= 3 writes across a > 3*H window) \
             while a discovery cycle is blocked, got {} (before={writes_before}, after={writes_after}) \
             -- the heartbeat is starved again",
            writes_after - writes_before
        );

        handle.abort();
    }

    /// Issue #796 clean shutdown: the heartbeat task `run_loop` spawns must stop
    /// when the loop ends -- no leaked task. `run_loop` signals and awaits the
    /// heartbeat handle before returning, so once the loop's own join handle
    /// resolves the heartbeat task is guaranteed finished, not detached.
    ///
    /// Runs `run_loop` (heartbeat H = 1s, discovery interval 300s so no discovery
    /// cycle fires during the test), lets a few heartbeats write, sends shutdown,
    /// and joins the loop. It then advances simulated time well past several `H`
    /// and asserts the heartbeat write count does not move: a detached (leaked)
    /// task would keep writing.
    #[tokio::test(start_paused = true)]
    async fn shutdown_joins_the_heartbeat_task_without_leaking_it() {
        use std::sync::atomic::Ordering;

        let worker = Arc::new(WorkerSet::new(0, Duration::from_secs(1), 3, 4));
        let store_impl = BlockingDiscoveryStore::new(MemoryStore::new(), Duration::from_secs(3600));
        let heartbeat_writes = store_impl.heartbeat_writes.clone();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store_impl);

        let compactor = Arc::new(CompactorConfig::default());
        let retention = Arc::new(RetentionConfig::default());
        let metrics = Arc::new(TenantDiscoveryMetrics::default());
        let safety = Arc::new(MaintenanceSafetyMetrics::default());
        let ownership = Arc::new(MaintenanceOwnershipMetrics::new(
            DEFAULT_STALLED_AFTER_INTERVALS,
        ));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_loop(
            store,
            None,
            compactor,
            retention,
            1,
            // Discovery interval far past the test window: only the heartbeat
            // task runs, so this test isolates its lifecycle.
            Duration::from_secs(300),
            metrics,
            safety,
            ownership,
            worker,
            Arc::new(SystemRng),
            shutdown_rx,
        ));

        // Let a few heartbeats fire.
        for _ in 0..40 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            heartbeat_writes.load(Ordering::SeqCst) >= 2,
            "the heartbeat task must fire while the loop runs"
        );

        // Shut down and join the loop; run_loop must join the heartbeat task
        // before returning (no time advance is needed: shutdown is a oneshot).
        shutdown_tx.send(()).expect("send shutdown");
        handle.await.expect("run_loop joins cleanly on shutdown");

        // No leaked task: past the loop's return, advancing several H produces
        // no further heartbeat writes.
        let writes_at_stop = heartbeat_writes.load(Ordering::SeqCst);
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            heartbeat_writes.load(Ordering::SeqCst),
            writes_at_stop,
            "the heartbeat task must stop when the loop ends -- no writes after shutdown, no leak"
        );
    }

    /// Issue #993 fail-closed re-assert (closing the #904 gap): a stored
    /// `sys/gc` that satisfies its OWN write-time skew is still refused when the
    /// RUNNING sweeper is configured with a LARGER `clock_skew_allowance`, so the
    /// sweep loop that actually deletes is never spawned with a skew-uncovered
    /// horizon. `spawn` returns [`ravel_maintain::GcConfigError::MaintainSkewUncovered`]
    /// and no supervisor task is started -- no delete/GC path can run.
    ///
    /// The mirror: a stored horizon that DOES cover the running sweeper's skew
    /// spawns normally.
    ///
    /// Flip line to watch the fail-closed check pass through (the sweep loop
    /// would then spawn and delete): in `spawn`, change the re-assert to pass
    /// `0` instead of `config.compactor.clock_skew_allowance_ns` (drop the
    /// sweeper-skew term). The stored default horizon then meets the reduced
    /// bound, `validate_maintain_skew` returns `Ok`, `spawn` returns `Ok`, and
    /// the `expect_err` below panics.
    #[tokio::test]
    async fn spawn_fails_closed_when_running_sweeper_skew_exceeds_stored_horizon() {
        // The stored durable object: the maintain defaults, whose horizon
        // covers exactly the default 5m skew (`max_query_duration + grace + 5m`).
        let stored_gc = ravel_maintain::GcConfigValues::maintain_defaults();
        let default_skew = CompactorConfig::default().clock_skew_allowance_ns;

        // A running sweeper configured with a LARGER skew than the stored
        // horizon budgets for: independent knob, never cross-checked before #993.
        let over_skew = default_skew + 60_000_000_000; // +1 min
        let fail_config = MaintenanceTaskConfig {
            enabled: true,
            compactor: CompactorConfig {
                clock_skew_allowance_ns: over_skew,
                ..CompactorConfig::default()
            },
            ..MaintenanceTaskConfig::default()
        };

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let make_args = || {
            (
                Arc::new(TenantDiscoveryMetrics::default()),
                Arc::new(MaintenanceSafetyMetrics::default()),
                Arc::new(MaintenanceOwnershipMetrics::new(
                    DEFAULT_STALLED_AFTER_INTERVALS,
                )),
                Arc::new(solo_worker()),
            )
        };

        let (metrics, safety, ownership, worker) = make_args();
        // `MaintenanceTasks` is not `Debug`, so match the Result rather than
        // `expect_err` (which would need the Ok payload to be printable).
        match spawn(
            store.clone(),
            Vec::new(),
            fail_config,
            stored_gc,
            metrics,
            safety,
            ownership,
            worker,
        ) {
            Err(ravel_maintain::GcConfigError::MaintainSkewUncovered {
                clock_skew_allowance_ns,
                stored_horizon_ns,
                ..
            }) => {
                assert_eq!(clock_skew_allowance_ns, over_skew);
                assert_eq!(stored_horizon_ns, stored_gc.protection_horizon_ns);
            }
            Err(other) => panic!("expected MaintainSkewUncovered, got: {other}"),
            Ok(_) => panic!(
                "a running sweeper skew the stored horizon does not cover must fail spawn, \
                 not enter the sweep loop"
            ),
        }

        // Mirror: the running sweeper's skew equals the default the stored
        // horizon covers, so spawn succeeds and returns a running supervisor.
        let ok_config = MaintenanceTaskConfig {
            enabled: true,
            compactor: CompactorConfig::default(),
            ..MaintenanceTaskConfig::default()
        };
        let (metrics, safety, ownership, worker) = make_args();
        let tasks = spawn(
            store,
            Vec::new(),
            ok_config,
            stored_gc,
            metrics,
            safety,
            ownership,
            worker,
        )
        .expect("a horizon that covers the running sweeper's skew spawns normally");
        tasks.shutdown().await;
    }
}
