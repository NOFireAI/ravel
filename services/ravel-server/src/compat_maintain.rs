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
    CompatKeys, CompatView, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN,
    certificate_finalization_pending, finalization_pending, read_compat,
};
use ravel_catalog::provisioning_key;
use ravel_maintain::compat_policy::part_family;
use ravel_maintain::worker_set::owns;
use ravel_maintain::{
    BackgroundMigrateOutcome, CertifyRefusal, CertifyReport, CertifyTimings, Clock,
    CompactorConfig, LateWriteReport, MigrateBudget, WorkerSet, background_migrate_unit, certify,
    late_write_pass,
};
use ravel_object_store::{ObjectStoreBackend, StoreError};
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
pub const BACKGROUND_MIGRATE_BUDGET: MigrateBudget = MigrateBudget {
    max_records: 10_000,
};

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
            finalization_pending: finalization_pending(entry),
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
                if !owns(late_write_unit_key(family).as_bytes(), process_id, live_set) {
                    continue;
                }
                let unit = run_family(ctx, family, state).await;
                wrote |= matches!(&unit.late_write, LateWriteStep::Ran(pass) if pass.certified)
                    || matches!(
                        &unit.finalize,
                        FinalizeStep::Ran(CertifyReport::Certified { .. })
                    );
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
        _ => certificate_finalization_pending(
            state.contradicted,
            state.watermark_unix_ns,
            state.certified_unix_ns,
        ),
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
            if !ctx
                .worker
                .owns_unit(live_set, &tenant, signal, FOLD_UNIT_SHARD)
            {
                continue;
            }
            // A signal the tenant never provisioned has no floor to raise; any
            // other HEAD failure is left to the unit, which reports it.
            if let Err(StoreError::NotFound) =
                ctx.store.head(&provisioning_key(&tenant, signal)).await
            {
                continue;
            }
            units.push((tenant, signal, family));
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::compat::{COMPAT_FORMAT_VERSION, CompatClock, create_compat};
    use ravel_catalog::{
        AbsentPolicy, current_floor_from_store, provisioning_key, raise_compat_fence,
        validate_or_adopt,
    };
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_maintain::FixedClock;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions};
    use ravel_proto::sys::v1 as sysproto;

    use super::*;

    const HOUR: i64 = 3_600_000_000_000;
    const ACTIVATED: i64 = 1_000;
    /// Far enough past `ACTIVATED` to clear certify's activation and `G_pre`
    /// terms.
    const NOW1: i64 = 400_000 * HOUR;
    /// The RSEG writer's version.
    const V_SEG: u32 = 7;
    const CERTIFY_CURSOR: &str = "sys/maintain/certify/rseg/cursor";

    fn ckeys() -> CompatKeys {
        CompatKeys::from_file_keys(&[[7; 32]]).expect("one key")
    }

    fn tenant() -> TenantHash {
        TenantHash([5; 16])
    }

    struct ViewClock(FixedClock);

    impl CompatClock for ViewClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.now_ns()
        }
    }

    fn timings() -> CertifyTimings {
        CertifyTimings {
            t_req_ns: 30 * 1_000_000_000,
            max_flush_lifetime_ns: HOUR,
            clock_skew_allowance_ns: CompactorConfig::default().clock_skew_allowance_ns,
        }
    }

    fn context(store: Arc<MemoryStore>, clock: &FixedClock) -> CompatMaintainContext {
        let store: Arc<dyn ObjectStoreBackend> = store;
        CompatMaintainContext {
            store: store.clone(),
            keys: ckeys(),
            view: CompatView::new(store, ckeys(), Arc::new(ViewClock(clock.clone()))),
            compactor: Arc::new(CompactorConfig::default()),
            worker: Arc::new(WorkerSet::new(0, Duration::from_secs(60), 3, 1)),
            clock: Arc::new(clock.clone()),
            shard_count: 1,
            fallback_allow: None,
            background_migrate: None,
            interval: DEFAULT_COMPAT_MAINTAIN_INTERVAL,
        }
    }

    /// `sys/compat` with `rseg` active at `V_SEG` since `ACTIVATED`, its fence
    /// walk complete and `P` one hour, and a fenced one-shard metrics tenant.
    async fn seed_rseg(store: &MemoryStore) {
        create_compat(
            store,
            &ckeys(),
            sysproto::CompatRecord {
                format_version: COMPAT_FORMAT_VERSION,
                baseline_release_major: 0,
                baseline_release_minor: 34,
                families: vec![sysproto::CompatFamily {
                    family: FAMILY_RSEG.to_string(),
                    active_write_version: V_SEG,
                    activated_epoch: 1,
                    activated_unix_ns: ACTIVATED,
                    activated_by: "test".to_string(),
                    certificate: None,
                    fence_complete_unix_ns: ACTIVATED,
                    late_writes_verified_through_unix_ns: 0,
                    contradicted: false,
                }],
                compat_epoch: 1,
                max_publish_lifetime_ns: HOUR,
                adopted: Vec::new(),
                mac: Vec::new(),
            },
        )
        .await
        .expect("create sys/compat");
        validate_or_adopt(
            store,
            &tenant(),
            Signal::Metrics,
            1,
            ACTIVATED,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
        raise_compat_fence(store, &tenant(), Signal::Metrics, 1, ACTIVATED)
            .await
            .expect("fence");
    }

    /// Certify `rseg` at `NOW1` with the provisioning-record PUT faulted, so
    /// the certificate CAS lands and its floor raise does not: the state a
    /// crashed certify leaves, which only a late-write pass can finalize.
    async fn certify_unfinalized(store: &Arc<MemoryStore>) {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("put down".to_string()))
                .with_key_contains(provisioning_key(&tenant(), Signal::Metrics)),
        );
        let faulted = FaultStore::new(store.clone(), plan);
        let result = certify(
            &faulted,
            &ckeys(),
            NOW1,
            FAMILY_RSEG,
            V_SEG,
            timings(),
            None,
        )
        .await;
        assert!(result.is_err(), "{result:?}");
        assert_eq!(faulted.fault_count(Op::Put, FaultKind::Permanent), 1);
        let entry = rseg_entry(store.as_ref()).await;
        let certificate = entry.certificate.expect("the CAS landed");
        assert_eq!(certificate.certified_unix_ns, NOW1);
        assert!(entry.late_writes_verified_through_unix_ns <= NOW1);
        assert_eq!(floor(store.as_ref()).await, None);
        assert!(cursor_present(store.as_ref()).await);
    }

    async fn rseg_entry(store: &dyn ObjectStoreBackend) -> sysproto::CompatFamily {
        read_compat(store, &ckeys())
            .await
            .expect("read")
            .expect("present")
            .0
            .families
            .into_iter()
            .find(|f| f.family == FAMILY_RSEG)
            .expect("rseg entry")
    }

    async fn floor(store: &dyn ObjectStoreBackend) -> Option<u32> {
        current_floor_from_store(store, &tenant(), Signal::Metrics, FAMILY_RSEG)
            .await
            .expect("floor")
    }

    async fn cursor_present(store: &dyn ObjectStoreBackend) -> bool {
        store.get(CERTIFY_CURSOR, GetRange::Full).await.is_ok()
    }

    /// A metrics commit record stamped `stamp`, landed at `modified_ns`.
    async fn seed_commit(store: &MemoryStore, stamp: u32, modified_ns: i64) -> String {
        store.set_clock_ms(u64::try_from(modified_ns / 1_000_000).expect("ms"));
        let ts_ns = NOW1;
        let commit = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 1,
            content_hash: [1; 32],
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: ts_ns,
            max_event_ts_ns: ts_ns,
            min_ingest_ts_ns: ts_ns,
            max_ingest_ts_ns: ts_ns,
            segment_format_version: stamp,
            created_unix_ns: ts_ns,
            ingest_hour_bucket: u32::try_from(ts_ns.div_euclid(HOUR)).expect("hour"),
        })
        .expect("build commit");
        let key = ravel_commit::keys::commit_key_for_record(&commit).expect("commit key");
        store
            .put(&key, record::encode(&commit), PutOptions::default())
            .await
            .expect("put commit");
        key
    }

    fn only_family(report: &CompatMaintainReport) -> &FamilyUnitReport {
        assert_eq!(report.families.len(), 1, "{report:?}");
        &report.families[0]
    }

    /// ACCEPTANCE: one tick of the unit runs the late-write pass on a
    /// certificate whose floor raises a crashed certify left pending, and then
    /// finishes those raises, so the floor reaches the certified version
    /// without an operator re-running certify.
    ///
    /// DISTINGUISHING, wrong implementations this rules out:
    ///  1. a unit that runs the late-write pass and never calls certify leaves
    ///     the floor unraised: the `Some(V_SEG)` floor assertion fails.
    ///  2. a unit that calls certify before the pass gets `FinalizationPending`
    ///     (the watermark is not yet past the certificate) and raises nothing
    ///     this tick: the `Certified` and floor assertions fail.
    ///  3. a unit that does not refresh the process's view leaves `/metrics`
    ///     reporting the finalization as pending: the view assertion fails.
    #[tokio::test]
    async fn a_tick_runs_the_late_write_pass_then_finishes_pending_floor_raises() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        certify_unfinalized(&store).await;
        let clock = FixedClock::new(NOW1 + 2 * HOUR);
        let ctx = context(store.clone(), &clock);
        crate::compat::refresh_once(&ctx.view).await;
        let pending = CertificateState::of(&ctx.view.last_record().expect("read").families[0])
            .expect("certified");
        assert!(pending.finalization_pending, "{pending:?}");

        let report = run_tick(&ctx, &[ctx.worker.process_id()]).await;

        let unit = only_family(&report);
        assert_eq!(unit.family, FAMILY_RSEG);
        match &unit.late_write {
            LateWriteStep::Ran(pass) => {
                assert!(pass.late_writes.is_empty(), "{pass:?}");
                assert_eq!(pass.watermark_unix_ns, NOW1 + 2 * HOUR);
            }
            other => panic!("the pass is due on an unverified certificate: {other:?}"),
        }
        assert!(
            matches!(
                unit.finalize,
                FinalizeStep::Ran(CertifyReport::Certified { version: V_SEG, .. })
            ),
            "{:?}",
            unit.finalize
        );
        assert_eq!(
            unit.state,
            CertificateState {
                certificate_version: V_SEG,
                certified_unix_ns: NOW1,
                watermark_unix_ns: NOW1 + 2 * HOUR,
                contradicted: false,
                finalization_pending: false,
            }
        );
        assert_eq!(floor(store.as_ref()).await, Some(V_SEG));
        assert!(!cursor_present(store.as_ref()).await);
        let viewed = CertificateState::of(&ctx.view.last_record().expect("read").families[0])
            .expect("certified");
        assert_eq!(
            viewed, unit.state,
            "the view was refreshed after the writes"
        );
    }

    /// The pass runs once a day: a tick inside a day of the last pass leaves
    /// the watermark alone, and the first tick a day after it advances it.
    /// The finalization call that follows a finished certificate is refused
    /// as already certified and writes nothing.
    ///
    /// DISTINGUISHING: a unit that runs the pass every tick advances the
    /// watermark at `+1h`; a unit gated on `>` a day rather than `>=` skips
    /// the tick exactly a day later.
    #[tokio::test]
    async fn the_late_write_pass_runs_once_a_day() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        certify_unfinalized(&store).await;
        let first = NOW1 + 2 * HOUR;
        let clock = FixedClock::new(first);
        let ctx = context(store.clone(), &clock);
        let live = [ctx.worker.process_id()];
        run_tick(&ctx, &live).await;
        assert_eq!(
            rseg_entry(store.as_ref())
                .await
                .late_writes_verified_through_unix_ns,
            first
        );

        clock.set(first + HOUR);
        let report = run_tick(&ctx, &live).await;
        let unit = only_family(&report);
        assert!(matches!(unit.late_write, LateWriteStep::NotDue), "{unit:?}");
        assert!(
            matches!(
                unit.finalize,
                FinalizeStep::Ran(CertifyReport::Refused(
                    CertifyRefusal::AlreadyCertified { .. }
                ))
            ),
            "{unit:?}"
        );
        assert!(
            !cursor_present(store.as_ref()).await,
            "the probe started no census"
        );
        assert_eq!(
            rseg_entry(store.as_ref())
                .await
                .late_writes_verified_through_unix_ns,
            first
        );

        let day_later = first + LATE_WRITE_PASS_PERIOD_NS;
        clock.set(day_later);
        let report = run_tick(&ctx, &live).await;
        assert!(
            matches!(only_family(&report).late_write, LateWriteStep::Ran(_)),
            "{report:?}"
        );
        assert_eq!(
            rseg_entry(store.as_ref())
                .await
                .late_writes_verified_through_unix_ns,
            day_later
        );
    }

    /// A late write below the certified version contradicts the certificate,
    /// and the unit then skips finalization: the floor stays unraised so
    /// background migrate still rewrites the bucket holding it.
    ///
    /// DISTINGUISHING: a unit that finalizes regardless finishes the raises
    /// the crashed certify left (certify itself drops the finalizing cursor of
    /// a contradicted certificate, so the cursor assertion catches a unit that
    /// calls it at all).
    #[tokio::test]
    async fn a_late_write_contradicts_and_skips_finalization() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        certify_unfinalized(&store).await;
        let late = seed_commit(&store, V_SEG - 1, NOW1 + HOUR).await;
        let clock = FixedClock::new(NOW1 + 2 * HOUR);
        let ctx = context(store.clone(), &clock);

        let report = run_tick(&ctx, &[ctx.worker.process_id()]).await;

        let unit = only_family(&report);
        match &unit.late_write {
            LateWriteStep::Ran(pass) => assert_eq!(
                pass.late_writes,
                vec![ravel_maintain::LateWrite {
                    record_key: late,
                    version: V_SEG - 1,
                }]
            ),
            other => panic!("{other:?}"),
        }
        assert!(
            matches!(unit.finalize, FinalizeStep::Contradicted),
            "{unit:?}"
        );
        assert!(unit.state.contradicted && !unit.state.finalization_pending);
        assert!(rseg_entry(store.as_ref()).await.contradicted);
        assert_eq!(floor(store.as_ref()).await, None);
        assert!(cursor_present(store.as_ref()).await, "no certify call");
    }

    /// Only the live-set owner of `compat-late-write/rseg` runs the unit.
    #[tokio::test]
    async fn a_process_that_does_not_own_the_family_runs_nothing() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        certify_unfinalized(&store).await;
        let clock = FixedClock::new(NOW1 + 2 * HOUR);
        let ctx = context(store.clone(), &clock);
        let other = Uuid::from_u128(42);
        assert!(!owns(
            late_write_unit_key(FAMILY_RSEG).as_bytes(),
            ctx.worker.process_id(),
            &[other]
        ));

        let report = run_tick(&ctx, &[other]).await;

        assert!(report.families.is_empty(), "{report:?}");
        assert_eq!(
            rseg_entry(store.as_ref())
                .await
                .late_writes_verified_through_unix_ns,
            NOW1
        );
        assert_eq!(floor(store.as_ref()).await, None);
    }

    /// OBSERVABILITY: a maintain scrape renders the watermark in seconds and
    /// the contradicted and finalization-pending flags for each certified
    /// family, from the view, and no other mode renders them.
    #[tokio::test]
    async fn a_maintain_scrape_renders_the_certificate_gauges() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        certify_unfinalized(&store).await;
        let clock = FixedClock::new(NOW1 + 2 * HOUR);
        let ctx = context(store.clone(), &clock);
        crate::compat::refresh_once(&ctx.view).await;
        let render = |mode| {
            let mut body = String::new();
            crate::metrics::render_compat_family(
                &mut body,
                mode,
                &crate::metrics::CompatSnapshot::of(&ctx.view, clock.now_ns()),
            );
            body
        };
        let sample = |body: &str, name: &str| -> Vec<String> {
            body.lines()
                .filter(|line| line.starts_with(&format!("{name}{{")))
                .map(str::to_string)
                .collect()
        };
        let certified_seconds = NOW1 / 1_000_000_000;

        let body = render(crate::config::Mode::Maintain);
        assert_eq!(
            sample(&body, "ravel_compat_late_write_watermark_seconds"),
            vec![format!(
                "ravel_compat_late_write_watermark_seconds{{mode=\"maintain\",family=\"rseg\"}} {certified_seconds}"
            )]
        );
        assert_eq!(
            sample(&body, "ravel_compat_certificate_contradicted"),
            vec!["ravel_compat_certificate_contradicted{mode=\"maintain\",family=\"rseg\"} 0"]
        );
        assert_eq!(
            sample(&body, "ravel_compat_certify_finalization_pending"),
            vec!["ravel_compat_certify_finalization_pending{mode=\"maintain\",family=\"rseg\"} 1"]
        );

        seed_commit(&store, V_SEG - 1, NOW1 + HOUR).await;
        run_tick(&ctx, &[ctx.worker.process_id()]).await;
        let body = render(crate::config::Mode::Maintain);
        assert_eq!(
            sample(&body, "ravel_compat_late_write_watermark_seconds"),
            vec![format!(
                "ravel_compat_late_write_watermark_seconds{{mode=\"maintain\",family=\"rseg\"}} {}",
                certified_seconds + 2 * 3_600
            )]
        );
        assert_eq!(
            sample(&body, "ravel_compat_certificate_contradicted"),
            vec!["ravel_compat_certificate_contradicted{mode=\"maintain\",family=\"rseg\"} 1"]
        );
        assert_eq!(
            sample(&body, "ravel_compat_certify_finalization_pending"),
            vec!["ravel_compat_certify_finalization_pending{mode=\"maintain\",family=\"rseg\"} 0"]
        );

        let body = render(crate::config::Mode::All);
        for name in [
            "ravel_compat_late_write_watermark_seconds",
            "ravel_compat_certificate_contradicted",
            "ravel_compat_certify_finalization_pending",
        ] {
            assert!(!body.contains(name), "{name} rendered outside maintain");
        }
    }

    /// With `--background-migrate`, a tick runs the background migrate unit
    /// for each discovered tenant's provisioned signals whose fold unit this
    /// process owns: the metrics walk raises the unraised floor to the active
    /// version, and the unprovisioned logs and spans run no unit (one would
    /// fail on the missing provisioning record every tick). A process that
    /// owns no fold unit runs none.
    #[tokio::test]
    async fn a_tick_runs_background_migrate_for_owned_tenants() {
        let store = Arc::new(MemoryStore::with_page_size(2));
        seed_rseg(&store).await;
        let clock = FixedClock::new(NOW1);
        let mut ctx = context(store.clone(), &clock);
        crate::compat::refresh_once(&ctx.view).await;
        ctx.compactor = Arc::new(CompactorConfig {
            compat_policy: Some(ravel_maintain::MaintainCompatPolicy::new(
                ctx.view.clone(),
                Arc::new(ViewClock(clock.clone())),
            )),
            ..CompactorConfig::default()
        });
        ctx.background_migrate = Some(BACKGROUND_MIGRATE_BUDGET);
        assert_eq!(floor(store.as_ref()).await, None);

        let other = Uuid::from_u128(0xfeed);
        let report = run_tick(&ctx, &[other]).await;
        assert!(report.migrate.is_empty(), "{report:?}");
        assert_eq!(report.migrate_error, None);
        assert_eq!(floor(store.as_ref()).await, None);

        let report = run_tick(&ctx, &[ctx.worker.process_id()]).await;
        assert_eq!(report.migrate_error, None);
        let [unit] = report.migrate.as_slice() else {
            panic!("one provisioned signal, one unit: {report:?}");
        };
        assert_eq!(
            (unit.tenant, unit.signal, unit.family),
            (tenant(), Signal::Metrics, FAMILY_RSEG)
        );
        assert!(
            matches!(
                unit.outcome,
                Ok(BackgroundMigrateOutcome::Walked { ref held, .. }) if held.is_empty()
            ),
            "{:?}",
            unit.outcome
        );
        assert_eq!(floor(store.as_ref()).await, Some(V_SEG));
    }

    /// A contradicted certificate is never finalization-pending, even with its
    /// watermark at the certified time: certify starts a census on it rather
    /// than finishing floor raises. A state that dropped the `contradicted`
    /// term reports 1 on the gauge here.
    #[test]
    fn a_contradicted_certificate_is_not_finalization_pending() {
        let entry = |contradicted| sysproto::CompatFamily {
            family: FAMILY_RSEG.to_string(),
            active_write_version: V_SEG,
            certificate: Some(sysproto::Certificate {
                version: V_SEG,
                certified_unix_ns: NOW1,
                ..Default::default()
            }),
            late_writes_verified_through_unix_ns: NOW1,
            contradicted,
            ..Default::default()
        };
        let state = |contradicted| CertificateState::of(&entry(contradicted)).expect("certified");

        assert!(state(false).finalization_pending, "{:?}", state(false));
        assert_eq!(
            state(true),
            CertificateState {
                certificate_version: V_SEG,
                certified_unix_ns: NOW1,
                watermark_unix_ns: NOW1,
                contradicted: true,
                finalization_pending: false,
            }
        );
    }

    /// The configuration's `Debug` never prints the MAC keys.
    #[test]
    fn the_config_debug_redacts_the_keys() {
        let config = CompatMaintainConfig {
            keys: Some(ckeys()),
            ..Default::default()
        };
        let printed = format!("{config:?}");
        assert!(printed.contains("<redacted>"), "{printed}");
    }
}
