//! The `sys/compat` maintenance units of `--mode maintain` (ADR-4233
//! decisions 6 and 7).
//!
//! One task, spawned beside the maintenance supervisor when the process has a
//! deployment key, ticks every [`CompatMaintainConfig::interval`] and runs:
//!
//! - **The late-write pass**, once a day per certified class-A family
//!   (`rseg`, `rlog`, `rspan`): [`ravel_maintain::late_write_pass`], owned by
//!   one live maintain process per family through the unit key
//!   `compat-late-write/<family>`. It is due when the family's watermark is
//!   [`LATE_WRITE_PASS_PERIOD_NS`] old, or not yet past the certificate's
//!   `certified_unix_ns` (a certificate no pass has verified). The pass's CAS
//!   makes a concurrent pass from another owner harmless.
//! - **Certify finalization**, after the pass on an uncontradicted family: a
//!   [`ravel_maintain::certify`] call at the certificate's own version. A
//!   certificate whose floor raises wait on a pass reports
//!   [`CertifyReport::FinalizationPending`] until the watermark passes
//!   `certified_unix_ns`, and the first call after that finishes the raises.
//!   With no pending raises it is refused as already certified before any
//!   write, and [`PROBE_TIMINGS`] keep its time gate shut, so it never starts a
//!   census.
//! - **Background migrate**, with `--background-migrate`:
//!   [`ravel_maintain::background_migrate_unit`] for every discovered tenant
//!   and provisioned signal this process owns the fold unit of, under
//!   [`BACKGROUND_MIGRATE_BUDGET`].
//!
//! Each tick's outcome is a [`CompatMaintainReport`], logged per family and
//! per walked bucket. The watermark, `contradicted` and finalization-pending
//! state also render on `/metrics` from the process's `sys/compat` view
//! ([`crate::metrics::CompatSnapshot`]).

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use ravel_catalog::compat::{
    CompatKeys, CompatView, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, read_compat,
};
use ravel_maintain::compat_policy::part_family;
use ravel_maintain::worker_set::owns;
use ravel_maintain::{
    BackgroundMigrateOutcome, CertifyRefusal, CertifyReport, CertifyTimings, Clock,
    CompactorConfig, LateWriteReport, MigrateBudget, WorkerSet, background_migrate_unit, certify,
    late_write_pass,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::fold::FOLD_UNIT_SHARD;
use crate::tenant_discovery::discover_and_restrict_by_lifecycle;

/// The class-A families the late-write pass covers.
pub const CERTIFIED_FAMILIES: [&str; 3] = [FAMILY_RSEG, FAMILY_RLOG, FAMILY_RSPAN];

/// How often a family's late-write pass is due: once a day.
pub const LATE_WRITE_PASS_PERIOD_NS: i64 = 24 * 3_600 * 1_000_000_000;

/// Default tick of the compat maintenance task.
pub const DEFAULT_COMPAT_MAINTAIN_INTERVAL: Duration = Duration::from_secs(3_600);

/// L0 records one background migrate unit rewrites per `(tenant, signal)` per
/// tick before it yields; the cursor resumes the walk next tick.
pub const BACKGROUND_MIGRATE_BUDGET: MigrateBudget = MigrateBudget { max_records: 10_000 };

/// The signals with a provisioning record, so a format floor the background
/// unit can rest on. Alerts and audit have none.
const MIGRATE_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Timings for the finalization call. Its pending-raises and already-certified
/// paths read none of them; a `t_req_ns` this large holds the time gate shut,
/// so a call that finds the certificate contradicted after the pass read it
/// is refused before writing a census cursor.
pub const PROBE_TIMINGS: CertifyTimings = CertifyTimings {
    t_req_ns: i64::MAX / 4,
    max_flush_lifetime_ns: 0,
    clock_skew_allowance_ns: 0,
};

/// The compat maintenance knobs inside
/// [`crate::maintain::MaintenanceTaskConfig`].
#[derive(Clone)]
pub struct CompatMaintainConfig {
    /// Pause between ticks.
    pub interval: Duration,
    /// `Some` with `--background-migrate`: the budget of each background
    /// migrate unit.
    pub background_migrate: Option<MigrateBudget>,
    /// The `sys/compat` MAC keys, derived from the deployment key. `None`
    /// spawns no task: an unkeyed process installs no compat policy.
    pub keys: Option<CompatKeys>,
}

impl std::fmt::Debug for CompatMaintainConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompatMaintainConfig")
            .field("interval", &self.interval)
            .field("background_migrate", &self.background_migrate)
            .field("keys", &self.keys.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Default for CompatMaintainConfig {
    fn default() -> Self {
        CompatMaintainConfig {
            interval: DEFAULT_COMPAT_MAINTAIN_INTERVAL,
            background_migrate: None,
            keys: None,
        }
    }
}

/// Everything one tick needs.
#[derive(Clone)]
pub struct CompatMaintainContext {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub keys: CompatKeys,
    /// Refreshed after a tick that wrote `sys/compat`, so `/metrics` and the
    /// maintenance writers see the new watermark without waiting a refresh
    /// interval.
    pub view: CompatView,
    /// The maintenance compactor configuration, compat policy included.
    pub compactor: Arc<CompactorConfig>,
    pub worker: Arc<WorkerSet>,
    pub clock: Arc<dyn Clock>,
    pub shard_count: u32,
    pub fallback_allow: Option<Vec<TenantHash>>,
    pub background_migrate: Option<MigrateBudget>,
    pub interval: Duration,
}

/// What one tick did.
#[derive(Debug, Default)]
pub struct CompatMaintainReport {
    /// Why the tick read no family, when it read none.
    pub compat_read_error: Option<String>,
    /// One entry per certified class-A family this process owns.
    pub families: Vec<FamilyUnitReport>,
    /// One entry per `(tenant, signal)` background migrate unit run.
    pub migrate: Vec<MigrateUnitReport>,
    /// Why background migrate ran no unit this tick, when tenant discovery
    /// or the walk's task failed.
    pub migrate_error: Option<String>,
}

/// One family's late-write pass and finalization this tick.
#[derive(Debug)]
pub struct FamilyUnitReport {
    pub family: &'static str,
    pub late_write: LateWriteStep,
    pub finalize: FinalizeStep,
    /// The family's state after both steps, as this tick last read or wrote
    /// it.
    pub state: CertificateState,
}

/// The late-write pass step.
#[derive(Debug)]
pub enum LateWriteStep {
    /// The watermark is past the certificate and younger than a day.
    NotDue,
    Ran(LateWriteReport),
    Failed(String),
}

/// The certify finalization step.
#[derive(Debug)]
pub enum FinalizeStep {
    /// The family is contradicted: nothing to finalize, and a certify call
    /// would start a census.
    Contradicted,
    Ran(CertifyReport),
    Failed(String),
}

/// A family's certificate state, from its `sys/compat` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertificateState {
    pub certificate_version: u32,
    pub certified_unix_ns: i64,
    pub watermark_unix_ns: i64,
    pub contradicted: bool,
    /// Uncontradicted, with the watermark not yet past `certified_unix_ns`:
    /// the state in which `certify` reports
    /// [`CertifyReport::FinalizationPending`] and the floor raises wait on a
    /// late-write pass.
    pub finalization_pending: bool,
}

impl CertificateState {
    /// The state of `entry`, or `None` when it carries no certificate.
    pub fn of(entry: &ravel_proto::sys::v1::CompatFamily) -> Option<CertificateState> {
        let certificate = entry.certificate.as_ref()?;
        let watermark_unix_ns = entry.late_writes_verified_through_unix_ns;
        Some(CertificateState {
            certificate_version: certificate.version,
            certified_unix_ns: certificate.certified_unix_ns,
            watermark_unix_ns,
            contradicted: entry.contradicted,
            finalization_pending: !entry.contradicted
                && watermark_unix_ns <= certificate.certified_unix_ns,
        })
    }

    fn late_write_pass_due(&self, now_unix_ns: i64) -> bool {
        self.watermark_unix_ns <= self.certified_unix_ns
            || now_unix_ns.saturating_sub(self.watermark_unix_ns) >= LATE_WRITE_PASS_PERIOD_NS
    }
}

/// One background migrate unit's outcome.
#[derive(Debug)]
pub struct MigrateUnitReport {
    pub tenant: TenantHash,
    pub signal: Signal,
    pub family: &'static str,
    pub outcome: Result<BackgroundMigrateOutcome, String>,
}

/// The unit key whose owner runs `family`'s late-write pass.
pub fn late_write_unit_key(family: &str) -> String {
    format!("compat-late-write/{family}")
}

/// Run one tick against `live_set`.
pub async fn run_tick(ctx: &CompatMaintainContext, live_set: &[Uuid]) -> CompatMaintainReport {
    let mut report = CompatMaintainReport::default();
    let process_id = ctx.worker.process_id();
    let mut wrote = false;

    match read_compat(ctx.store.as_ref(), &ctx.keys).await {
        Ok(Some((record, _))) => {
            for family in CERTIFIED_FAMILIES {
                let Some(entry) = record.families.iter().find(|entry| entry.family == family)
                else {
                    continue;
                };
                let Some(state) = CertificateState::of(entry) else {
                    continue;
                };
                if !owns(
                    late_write_unit_key(family).as_bytes(),
                    process_id,
                    live_set,
                ) {
                    continue;
                }
                let unit = run_family(ctx, family, state).await;
                wrote |= matches!(&unit.late_write, LateWriteStep::Ran(pass) if pass.certified)
                    || matches!(&unit.finalize, FinalizeStep::Ran(CertifyReport::Certified { .. }));
                report.families.push(unit);
            }
        }
        Ok(None) => {}
        Err(err) => report.compat_read_error = Some(err.to_string()),
    }
    if wrote {
        crate::compat::refresh_once(&ctx.view).await;
    }

    if let Some(budget) = ctx.background_migrate {
        run_background_migrate(ctx, live_set, budget, &mut report).await;
    }
    report
}

async fn run_family(
    ctx: &CompatMaintainContext,
    family: &'static str,
    before: CertificateState,
) -> FamilyUnitReport {
    let now = ctx.clock.now_ns();
    let mut state = before;
    let late_write = if before.late_write_pass_due(now) {
        match late_write_pass(
            ctx.store.as_ref(),
            &ctx.keys,
            now,
            family,
            ctx.compactor.clock_skew_allowance_ns,
        )
        .boxed()
        .await
        {
            Ok(pass) => {
                if pass.certified && !pass.certificate_replaced {
                    state.watermark_unix_ns = state.watermark_unix_ns.max(pass.watermark_unix_ns);
                    state.contradicted |= !pass.late_writes.is_empty();
                }
                LateWriteStep::Ran(pass)
            }
            Err(err) => LateWriteStep::Failed(err.to_string()),
        }
    } else {
        LateWriteStep::NotDue
    };

    let finalize = if state.contradicted {
        FinalizeStep::Contradicted
    } else {
        match certify(
            ctx.store.as_ref(),
            &ctx.keys,
            ctx.clock.now_ns(),
            family,
            state.certificate_version,
            PROBE_TIMINGS,
            Some(0),
        )
        .boxed()
        .await
        {
            Ok(outcome) => FinalizeStep::Ran(outcome),
            Err(err) => FinalizeStep::Failed(err.to_string()),
        }
    };
    state.finalization_pending = match &finalize {
        FinalizeStep::Contradicted => false,
        FinalizeStep::Ran(CertifyReport::FinalizationPending { .. }) => true,
        FinalizeStep::Ran(CertifyReport::Certified { .. }) => false,
        _ => !state.contradicted && state.watermark_unix_ns <= state.certified_unix_ns,
    };
    FamilyUnitReport {
        family,
        late_write,
        finalize,
        state,
    }
}

async fn run_background_migrate(
    ctx: &CompatMaintainContext,
    live_set: &[Uuid],
    budget: MigrateBudget,
    report: &mut CompatMaintainReport,
) {
    let tenants =
        match discover_and_restrict_by_lifecycle(ctx.store.as_ref(), ctx.fallback_allow.as_deref())
            .await
        {
            Ok(outcome) => outcome.maintained,
            Err(err) => {
                report.migrate_error = Some(format!("tenant discovery failed: {err}"));
                return;
            }
        };
    let mut units = Vec::new();
    for tenant in tenants {
        for signal in MIGRATE_SIGNALS {
            let Some(family) = part_family(signal) else {
                continue;
            };
            if ctx
                .worker
                .owns_unit(live_set, &tenant, signal, FOLD_UNIT_SHARD)
            {
                units.push((tenant, signal, family));
            }
        }
    }
    if units.is_empty() {
        return;
    }
    // The walk's re-encode path builds futures rustc cannot prove `Send` for
    // every lifetime, so it cannot run on a spawned task; it runs to
    // completion on a blocking thread driven by this runtime instead.
    let job = ctx.clone();
    let runtime = tokio::runtime::Handle::current();
    let walked = tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
            let mut reports = Vec::with_capacity(units.len());
            for (tenant, signal, family) in units {
                let outcome = background_migrate_unit(
                    job.store.as_ref(),
                    job.clock.as_ref(),
                    &job.compactor,
                    tenant,
                    signal,
                    family,
                    job.shard_count,
                    budget,
                )
                .await
                .map_err(|err| err.to_string());
                reports.push(MigrateUnitReport {
                    tenant,
                    signal,
                    family,
                    outcome,
                });
            }
            reports
        })
    })
    .await;
    match walked {
        Ok(reports) => report.migrate = reports,
        Err(err) => report.migrate_error = Some(format!("the walk task failed: {err}")),
    }
}

/// Log `report`: one line per family and per background migrate unit that did
/// something.
pub fn log_report(report: &CompatMaintainReport) {
    if let Some(err) = &report.compat_read_error {
        tracing::warn!(error = %err, "compat maintenance: could not read sys/compat; retried next tick");
    }
    for unit in &report.families {
        let family = unit.family;
        let state = unit.state;
        match &unit.late_write {
            LateWriteStep::NotDue => {}
            LateWriteStep::Ran(pass) if !pass.late_writes.is_empty() => {
                let first: Vec<&str> = pass
                    .late_writes
                    .iter()
                    .take(5)
                    .map(|late| late.record_key.as_str())
                    .collect();
                tracing::error!(
                    family,
                    late_writes = pass.late_writes.len(),
                    first = ?first,
                    certified_version = state.certificate_version,
                    "compat maintenance: the late-write pass found records below the certified \
                     version; the certificate is contradicted (ADR-4233 decision 7). Roll back \
                     to the release that reads both versions, migrate, and re-certify"
                );
            }
            LateWriteStep::Ran(pass) => tracing::info!(
                family,
                examined = pass.examined,
                watermark_unix_ns = pass.watermark_unix_ns,
                certificate_replaced = pass.certificate_replaced,
                "compat maintenance: late-write pass complete"
            ),
            LateWriteStep::Failed(err) => tracing::warn!(
                family,
                error = %err,
                "compat maintenance: late-write pass failed; retried next tick"
            ),
        }
        match &unit.finalize {
            FinalizeStep::Contradicted => {}
            FinalizeStep::Ran(CertifyReport::Certified { version, .. }) => tracing::info!(
                family,
                version,
                "compat maintenance: certify finalization finished the certificate's floor raises"
            ),
            FinalizeStep::Ran(CertifyReport::FinalizationPending {
                version,
                certified_unix_ns,
                watermark_unix_ns,
            }) => tracing::info!(
                family,
                version,
                certified_unix_ns,
                watermark_unix_ns,
                "compat maintenance: certify finalization pending a late-write pass past the \
                 certificate"
            ),
            FinalizeStep::Ran(CertifyReport::Refused(CertifyRefusal::AlreadyCertified {
                ..
            })) => {}
            FinalizeStep::Ran(other) => tracing::warn!(
                family,
                outcome = ?other,
                "compat maintenance: unexpected certify finalization outcome"
            ),
            FinalizeStep::Failed(err) => tracing::warn!(
                family,
                error = %err,
                "compat maintenance: certify finalization failed; retried next tick"
            ),
        }
        tracing::debug!(
            family,
            certificate_version = state.certificate_version,
            watermark_unix_ns = state.watermark_unix_ns,
            contradicted = state.contradicted,
            finalization_pending = state.finalization_pending,
            "compat maintenance: family state"
        );
    }
    if let Some(err) = &report.migrate_error {
        tracing::warn!(error = %err, "background migrate: no unit ran this tick; retried next tick");
    }
    for unit in &report.migrate {
        let tenant = unit.tenant.to_hex();
        let signal = unit.signal.key_prefix();
        match &unit.outcome {
            Ok(BackgroundMigrateOutcome::NoPolicy | BackgroundMigrateOutcome::UpToDate { .. }) => {}
            Ok(BackgroundMigrateOutcome::Walked {
                active_version,
                report,
                held,
            }) => {
                if held.is_empty() {
                    tracing::info!(
                        tenant,
                        signal,
                        family = unit.family,
                        active_version,
                        buckets_examined = report.buckets_examined,
                        buckets_migrated = report.buckets_migrated,
                        records_migrated = report.records_migrated,
                        "background migrate: walked toward the active version"
                    );
                } else {
                    tracing::warn!(
                        tenant,
                        signal,
                        family = unit.family,
                        active_version,
                        buckets_migrated = report.buckets_migrated,
                        held = held.len(),
                        holds = ?held,
                        "background migrate: walked with held buckets; the floor stays unraised"
                    );
                }
            }
            Ok(BackgroundMigrateOutcome::Refused { refusal }) => tracing::warn!(
                tenant,
                signal,
                family = unit.family,
                reason = refusal.reason(),
                "background migrate: refused by the compat policy"
            ),
            Err(err) => tracing::warn!(
                tenant,
                signal,
                family = unit.family,
                error = %err,
                "background migrate: unit failed; retried next tick"
            ),
        }
    }
}

/// Spawn the tick loop. It waits for the live set to admit this process, so
/// the first tick can own units, then ticks every `ctx.interval` until
/// `shutdown`.
pub fn spawn(
    ctx: CompatMaintainContext,
    mut live_rx: watch::Receiver<Vec<Uuid>>,
    mut shutdown: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let process_id = ctx.worker.process_id();
        tokio::select! {
            _ = &mut shutdown => return,
            admitted = live_rx.wait_for(|set| set.contains(&process_id)) => {
                if admitted.is_err() {
                    return;
                }
            }
        }
        let mut ticker = tokio::time::interval(ctx.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = ticker.tick() => {}
            }
            let live_set = live_rx.borrow_and_update().clone();
            let tick = AssertUnwindSafe(run_tick(&ctx, &live_set)).catch_unwind();
            tokio::select! {
                _ = &mut shutdown => return,
                outcome = tick => match outcome {
                    Ok(report) => log_report(&report),
                    Err(_) => tracing::error!(
                        "compat maintenance: a tick panicked; the next tick runs on schedule"
                    ),
                },
            }
        }
    })
}
