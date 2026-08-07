//! Background maintenance task (docs/compaction-retention-plan.md P8, issue
//! #115; storage-derived tenant set is ADR-0048 decision 3, issue #504).
//! Periodically runs age-based retention, L0->L1 compaction, and the GC
//! sweeper over every `(signal, shard)` of every tenant storage holds data
//! for.
//!
//! Unlike fold (a pure query-cost optimization), this task deletes and rewrites
//! durable objects, but it changes nothing about *what* any sweep, retention,
//! or compaction rule decides: it is only the driver that calls
//! [`scan_and_maintain_with_memo`] (retention-before-compaction over every
//! sealed bucket), [`ravel_maintain::sweep_shard`] (the three per-shard GC
//! rules), and [`ravel_maintain::sweep_idempotency_markers`] (the fourth GC
//! rule, run once per signal instead of per shard) once per tenant per tick.
//! All are idempotent, so a missed or crashed tick is recovered on the next
//! one. The clock is the real [`SystemClock`], matching everything else in
//! this crate.
//!
//! [`spawn`] runs one supervisor task, not one task per tenant: at the start
//! of every tick it re-enumerates tenants from storage
//! ([`ravel_maintain::discover_tenants`] via
//! [`crate::tenant_discovery::discover_and_restrict`]), optionally narrows
//! that set to the configured `--tenant-token`/`--maintain-tenant`
//! restriction, then runs [`run_tick`] for each tenant in the result. A
//! tenant that first writes data mid-run is picked up on the next cycle with
//! no restart, and a tenant removed from the restriction (but still holding
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::RngExt as _;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::scan::{MaintainMemo, MaintainReport, scan_and_maintain_with_memo};
use ravel_maintain::{
    Clock, CompactorConfig, LegalHoldCheck, MaintainError, RetentionConfig,
    sweep_idempotency_markers, sweep_shard,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::{TenantDiscoveryMetrics, discover_and_restrict};

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

/// Spawn one supervisor task that re-discovers the tenant set from storage
/// every tick (ADR-0048 decision 3). `restrict` is the merged
/// `--tenant-token`/`--maintain-tenant` set: empty means unconfigured (every
/// discovered tenant is maintained), non-empty narrows the discovered set to
/// exactly those tenants. Returns immediately; the task runs until
/// [`MaintenanceTasks::shutdown`].
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Vec<TenantHash>,
    config: MaintenanceTaskConfig,
    metrics: Arc<TenantDiscoveryMetrics>,
    safety: Arc<MaintenanceSafetyMetrics>,
) -> MaintenanceTasks {
    if !config.enabled {
        return MaintenanceTasks::none();
    }

    // One compactor writer_id per process start, shared across every tenant
    // this supervisor maintains (recorded in each L1 part's footer;
    // informational, never dedup-priority).
    let mut compactor = config.compactor.clone();
    compactor.compactor_writer_id = Uuid::new_v4();
    let compactor = Arc::new(compactor);
    let retention = Arc::new(config.retention.clone());
    let restrict = if restrict.is_empty() {
        None
    } else {
        Some(restrict)
    };

    let (tx, rx) = oneshot::channel();
    let interval = config.interval;
    let shard_count = config.shard_count;
    let handle = tokio::spawn(async move {
        run_loop(
            store,
            restrict,
            compactor,
            retention,
            shard_count,
            interval,
            metrics,
            safety,
            rx,
        )
        .await;
    });
    MaintenanceTasks {
        shutdown: vec![tx],
        handles: vec![handle],
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Option<Vec<TenantHash>>,
    compactor: Arc<CompactorConfig>,
    retention: Arc<RetentionConfig>,
    shard_count: u32,
    interval: Duration,
    metrics: Arc<TenantDiscoveryMetrics>,
    safety: Arc<MaintenanceSafetyMetrics>,
    mut shutdown: oneshot::Receiver<()>,
) {
    // One memo for the whole process, held across every tick and every
    // discovered tenant until shutdown (issue #280, #330). Its key includes
    // the tenant and signal, so this single instance safely spans every
    // tenant this supervisor discovers. Cold on the first tick, so that tick
    // is a full rescan identical to the pre-memo behavior.
    let mut memo = MaintainMemo::with_default_interval();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }
        run_discovery_cycle(
            store.as_ref(),
            restrict.as_deref(),
            &compactor,
            &retention,
            shard_count,
            &mut memo,
            metrics.as_ref(),
            safety.as_ref(),
        )
        .await;
    }
}

/// One discovery cycle: re-enumerate tenants from storage, narrow to
/// `restrict` when configured, then run [`run_tick`] for each tenant in the
/// result (ADR-0048 decision 3, issue #504). `metrics` records the discovered
/// and maintained gauges on success; a discovery failure -- the LIST itself
/// erroring -- skips the whole cycle (no tenant's tick runs) and only bumps
/// the failure counter, never falling back to an empty set. Falling back
/// would render identically to "storage has no tenants," the exact silent
/// failure findings S2-17/S5-09 describe.
#[allow(clippy::too_many_arguments)]
pub async fn run_discovery_cycle(
    store: &dyn ObjectStoreBackend,
    restrict: Option<&[TenantHash]>,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    metrics: &TenantDiscoveryMetrics,
    safety: &MaintenanceSafetyMetrics,
) -> MaintainReport {
    let outcome = match discover_and_restrict(store, restrict).await {
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
        )
        .await;
        total.retired += report.retired;
        total.compacted += report.compacted;
        total.already_done += report.already_done;
        total.not_sealed += report.not_sealed;
        total.skipped_terminal += report.skipped_terminal;
    }
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
/// loop. Every scan/sweep error is logged and retried next tick;
/// nothing here affects query correctness. Split out from [`run_discovery_cycle`]
/// so a test can drive a single deterministic tenant tick without discovery or
/// the timer.
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
pub async fn run_tick(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    safety: &MaintenanceSafetyMetrics,
) -> MaintainReport {
    let clock = WallClock;

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
        for shard in 0..scan_shards {
            match scan_and_maintain_with_memo(
                memo, store, &clock, compactor, retention, &hold, *tenant, signal, shard,
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

            match sweep_shard(store, &clock, compactor, &hold, tenant, signal, shard).await {
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
        // loop above. Logged like the GC sweep pass, not folded into
        // `MaintainReport`'s summed fields.
        if matches!(signal, Signal::Logs | Signal::Spans) {
            match sweep_idempotency_markers(store, &clock, compactor, &hold, tenant, signal).await {
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
    }
    total
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
        let report = run_tick(
            &store, &tenant, &compactor, &retention, 4, &mut memo, &safety,
        )
        .await;
        // Nothing to maintain, nothing memoized: a subsequent tick would still
        // find nothing to skip.
        assert_eq!(report, MaintainReport::default());
        assert!(memo.is_empty());
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
        run_tick(
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
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

        // Static shard_count is 2, matching generation 0's count; the scan
        // range must nonetheless cover shard 3 via the generation history.
        let report = run_tick(
            &store, &tenant, &compactor, &retention, 2, &mut memo, &safety,
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

        // Per-bucket object reads: the memo elides the per-bucket LIST and GET
        // reads, so this is what shrinks between the cold and warm ticks. The
        // shard-level `list_delimited` runs on every tick and is excluded.
        let per_bucket_reads = |s: &StoreMetricsSnapshot| -> u64 { s.list.calls + s.get.calls };

        // Tick 1 (cold memo): full evaluation, nothing skipped. The single-input
        // bucket is below the compaction threshold, so it is already-done and
        // gets memoized as terminal.
        let before_first = store.metrics().snapshot();
        let first = run_tick(
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
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
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
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

        // Tick 1: the bucket's one sample is from ingest hour 0 (1970), so any
        // valid retention window is already expired against the real wall
        // clock. Not memoized terminal (Tombstoned isn't a terminal state),
        // so tick 2 re-evaluates it for real rather than skipping it.
        let first = run_tick(
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
        )
        .await;
        assert_eq!(first.retired, 1, "the expired bucket is tombstoned");

        // Tick 2: the tombstone's horizon has elapsed (zero protection
        // horizon), so this tick attempts the physical sweep. The hold must
        // block it entirely.
        let second = run_tick(
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
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

        let report = run_tick(
            &store, &tenant, &compactor, &retention, 1, &mut memo, &safety,
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

        let report = run_discovery_cycle(
            &store, None, &compactor, &retention, 1, &mut memo, &metrics, &safety,
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

        let report = run_discovery_cycle(
            &store, None, &compactor, &retention, 1, &mut memo, &metrics, &safety,
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
}
