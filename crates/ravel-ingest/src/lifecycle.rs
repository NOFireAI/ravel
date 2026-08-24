//! Bounded-staleness lifecycle refresh (ADR-0066 decision 6):
//! the admission-limits and auth analogue of [`crate::GenerationSwitch`]'s
//! routing-generation refresh.
//!
//! `GenerationSwitch` (crate `generation`) already gives every router a durable
//! per-tenant fact (its shard-generation history) read on a bounded-staleness
//! horizon `C` and applied off the hot path, failing closed when it cannot be
//! trusted (ADR-0052 section 3). ADR-0066 decision 6 moves the rest of a
//! tenant's lifecycle -- admission-limit overrides and auth -- onto the same
//! pattern, so a `ravel-cli` config or token change takes effect without a
//! fleet restart.
//!
//! This module holds the two reusable pieces of that pattern that belong beside
//! the ingest hot path they feed:
//!
//! - [`StalenessGate`]: a last-successful-refresh timestamp plus a hard
//!   staleness bound `horizon * hard_multiple`. Past that bound a process can no
//!   longer trust its cached auth-affecting state and must fail closed for
//!   lifecycle-gated operations. This is the fail-closed direction of
//!   [`crate::GenerationSwitch`]'s ADR-0052 prov-staleness rule, sized by the
//!   `N * interval` shape ADR-0057's admission reconciliation (`2 * R`) and
//!   ADR-0065's worker liveness (`liveness_factor * H`) already use -- but
//!   inverted from their fail-open behaviour to fail closed.
//! - [`overlay_admission_limits`] / [`refresh_tenant_limits_once`]: read a
//!   tenant's durable [`ravel_catalog::TenantConfig`] and re-invoke the existing
//!   [`AdmissionController::set_tenant_limits`] with the record's overrides laid
//!   over the deployment base, exactly as ADR-0057's [`crate::reconcile_once`]
//!   re-invokes the controller off the hot path. The admission hot path is
//!   untouched; only the numbers change.
//!
//! The background *loop* that drives these on the horizon (with the on-miss
//! re-read and the durable auth resolver) lives in `services/ravel-server`,
//! the same mechanism-here / lifecycle-there split [`crate::reconcile_once`] and
//! its server-side spawner already use.

use std::sync::atomic::{AtomicI64, Ordering};

use ravel_catalog::{TenantConfig, TenantConfigError, read_config_values};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantId;

use crate::admission::{AdmissionController, AdmissionLimits, CountLimit, RateLimit};

/// Default lifecycle refresh horizon, mirroring [`crate::DEFAULT_REFRESH_INTERVAL_NS`]
/// (`GenerationSwitch`'s `C`): 60 seconds. A durable config or token change is
/// observed everywhere within one horizon, and revocation is horizon-bounded in
/// the same order (ADR-0066 decision 6's documented, accepted asymmetric
/// staleness). Cheap enough that one `sys/auth` GET plus one small config GET
/// per tenant per horizon per process is negligible, the same order as the
/// existing `GenerationSwitch` reads.
pub const DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS: i64 = 60 * 1_000_000_000;

/// The multiple of the horizon past which a process treats auth-affecting state
/// as stale and fails closed (ADR-0066 decision 6). Ten horizons (10 minutes at
/// the default `C`): far enough that a brief store blip never flips auth closed
/// (the intervening refreshes keep the gate fresh), but bounded, so a process
/// that has been unable to reach object storage for that long stops trusting a
/// possibly-revoked cached token map rather than honouring it indefinitely.
///
/// This is deliberately larger than ADR-0057's `2 * R` and ADR-0065's `3 * H`
/// staleness windows: those exclude a *silent peer* and fail open (assume it is
/// gone), where crossing the bound is the common, benign case. This bound gates
/// *this* process's own auth on *its own* inability to refresh and fails closed,
/// where crossing it is a genuine, rare fault, so it is set to tolerate ordinary
/// transient outages before biting.
pub const DEFAULT_HARD_STALENESS_MULTIPLE: i64 = 10;

/// A last-successful-refresh timestamp with a hard staleness bound
/// (ADR-0066 decision 6). A caller [`record_success`](Self::record_success)es
/// after each refresh that actually reached the durable object; a caller checks
/// [`hard_stale`](Self::hard_stale) before serving a lifecycle-gated operation,
/// and fails that operation closed when it returns true.
///
/// The gate is seeded with the construction time, so a freshly started process
/// is considered fresh for one full hard-bound window: it is given the chance to
/// complete its first background refresh before its cached (initially empty)
/// state could flip it closed. Once refreshes are succeeding, `record_success`
/// keeps the gate fresh; only a sustained inability to refresh (a store outage
/// longer than the hard bound) drives it stale.
#[derive(Debug)]
pub struct StalenessGate {
    horizon_ns: i64,
    hard_multiple: i64,
    last_success_ns: AtomicI64,
}

impl StalenessGate {
    /// Build a gate with refresh horizon `horizon_ns` and hard bound
    /// `horizon_ns * hard_multiple`, seeded fresh at `now_ns`. Both inputs are
    /// clamped to at least 1 so a zero (mis)configuration can never produce a
    /// zero hard bound that fails every operation closed instantly.
    pub fn new(horizon_ns: i64, hard_multiple: i64, now_ns: i64) -> Self {
        StalenessGate {
            horizon_ns: horizon_ns.max(1),
            hard_multiple: hard_multiple.max(1),
            last_success_ns: AtomicI64::new(now_ns),
        }
    }

    /// A gate with the default horizon and hard multiple, seeded at `now_ns`.
    pub fn with_defaults(now_ns: i64) -> Self {
        StalenessGate::new(
            DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS,
            DEFAULT_HARD_STALENESS_MULTIPLE,
            now_ns,
        )
    }

    /// Record that a refresh reached the durable object at `now_ns`, resetting
    /// the staleness clock. Call this only on a genuine success (including a
    /// genuine absence, which is a valid state, not a failure); never on a store
    /// error, or the hard bound would never bite.
    pub fn record_success(&self, now_ns: i64) {
        self.last_success_ns.store(now_ns, Ordering::Relaxed);
    }

    /// Unix-ns of the last recorded successful refresh (or the seed time).
    pub fn last_success_ns(&self) -> i64 {
        self.last_success_ns.load(Ordering::Relaxed)
    }

    /// The hard staleness bound in ns: `horizon * hard_multiple`, saturating.
    pub fn hard_bound_ns(&self) -> i64 {
        self.horizon_ns.saturating_mul(self.hard_multiple)
    }

    /// Age of the cached state at `now_ns`: `now - last_success`, saturating. A
    /// negative reading (clock skew, a refresh stamped in the future) saturates
    /// toward zero and reads as fresh, matching [`crate::reconcile_once`]'s
    /// treatment of a future snapshot.
    pub fn age_ns(&self, now_ns: i64) -> i64 {
        now_ns.saturating_sub(self.last_success_ns())
    }

    /// Whether the cached state is older than the hard bound and can no longer
    /// be trusted: the caller must fail lifecycle-gated operations closed
    /// (ADR-0066 decision 6). Exactly at the bound is still fresh; one ns past it
    /// is stale (the inclusive-fresh boundary [`crate::reconcile_once`]'s
    /// `is_stale` uses).
    pub fn hard_stale(&self, now_ns: i64) -> bool {
        self.age_ns(now_ns) > self.hard_bound_ns()
    }
}

/// Lay a durable [`TenantConfig`]'s admission-limit overrides over a base
/// [`AdmissionLimits`] (ADR-0066 decision 6). Each present (`Some`) override
/// replaces that one dimension; an absent (`None`) override leaves the base
/// value in place, so the deployment default (or `--limits-file` override) still
/// governs any dimension the durable record does not set.
///
/// A count override becomes [`CountLimit::Bounded`]. A rate override sets the
/// bucket's `per_sec`; it keeps the base's `burst` when the base was already
/// bounded, and otherwise (the base was `Unlimited`, so there is no base burst)
/// uses the override value as the burst too -- a single-value durable override
/// cannot express a separate burst, and a burst equal to the rate is the
/// conservative choice (no extra burst headroom beyond one second's worth).
pub fn overlay_admission_limits(base: AdmissionLimits, config: &TenantConfig) -> AdmissionLimits {
    AdmissionLimits {
        max_active_series: config
            .max_active_series
            .map_or(base.max_active_series, CountLimit::Bounded),
        max_active_streams: config
            .max_active_streams
            .map_or(base.max_active_streams, CountLimit::Bounded),
        ingest_byte_rate: overlay_rate(base.ingest_byte_rate, config.max_ingest_byte_rate),
        series_creation_rate: overlay_rate(
            base.series_creation_rate,
            config.max_series_creation_rate,
        ),
    }
}

/// Overlay a single optional rate override onto a base [`RateLimit`], per the
/// burst rule documented on [`overlay_admission_limits`].
fn overlay_rate(base: RateLimit, override_per_sec: Option<u64>) -> RateLimit {
    match override_per_sec {
        None => base,
        Some(per_sec) => {
            let burst = match base {
                RateLimit::Bounded { burst, .. } => burst,
                RateLimit::Unlimited => per_sec.max(1),
            };
            RateLimit::Bounded { per_sec, burst }
        }
    }
}

/// Read one tenant's durable config record and re-invoke
/// [`AdmissionController::set_tenant_limits`] with the resolved limits
/// (ADR-0066 decision 6). This is the whole admission-refresh mechanism; the
/// server wraps it in the horizon loop, exactly as it wraps
/// [`crate::reconcile_once`].
///
/// `base` is the tenant's startup-resolved limits (its `--limits-file` override
/// if it has one, else the deployment default): the durable record overlays on
/// top of it, so a tenant whose record clears an override falls back to its
/// startup value rather than to the global default. On `Ok(None)` (no durable
/// record) the base is re-applied unchanged, so a record that is later deleted
/// reverts the tenant to its startup limits within one horizon.
///
/// Returns the decoded config (`Some` when a record existed) so the caller can
/// also consult its lifecycle state. A store or decode error is propagated: the
/// caller keeps the last-applied limits (never resets to base on a transient
/// fault) and lets its [`StalenessGate`] account for the missed refresh, the
/// last-known-value discipline ADR-0057 established.
pub async fn refresh_tenant_limits_once(
    controller: &AdmissionController,
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    base: AdmissionLimits,
) -> Result<Option<TenantConfig>, TenantConfigError> {
    let hash = tenant.hash();
    match read_config_values(store, &hash).await? {
        Some(config) => {
            controller.set_tenant_limits(tenant.clone(), overlay_admission_limits(base, &config));
            Ok(Some(config))
        }
        None => {
            controller.set_tenant_limits(tenant.clone(), base);
            Ok(None)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use ravel_catalog::{TenantLifecycleState, set_tenant_config};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::SeriesId;

    use super::*;
    use crate::{Clock, SystemClock};

    fn base_limits(max_active_series: u64) -> AdmissionLimits {
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(max_active_series),
            max_active_streams: CountLimit::Unlimited,
            ingest_byte_rate: RateLimit::Unlimited,
            series_creation_rate: RateLimit::Unlimited,
        }
    }

    fn series(byte: u8) -> SeriesId {
        SeriesId([byte; 16])
    }

    /// A config that overrides only the active-series cap, leaving the lifecycle
    /// active.
    fn config_with_series_cap(cap: u64) -> TenantConfig {
        TenantConfig {
            max_active_series: Some(cap),
            ..TenantConfig::new(TenantLifecycleState::Active)
        }
    }

    /// The staleness gate is fresh at and within the hard bound and stale one ns
    /// past it; `record_success` resets it (ADR-0066 decision 6).
    #[test]
    fn staleness_gate_flips_closed_past_the_hard_bound() {
        let t0 = 1_000 * DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS;
        let gate = StalenessGate::new(DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS, 3, t0);
        let bound = gate.hard_bound_ns();
        assert_eq!(bound, 3 * DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS);

        assert!(!gate.hard_stale(t0), "just refreshed: fresh");
        assert!(
            !gate.hard_stale(t0 + bound),
            "exactly at the bound: still fresh"
        );
        assert!(
            gate.hard_stale(t0 + bound + 1),
            "one ns past the bound: stale, must fail closed"
        );

        // A clock reading before the last success (skew) reads as fresh, not a
        // spurious stale.
        assert!(
            !gate.hard_stale(t0 - 1),
            "a past reading is fresh, never stale"
        );

        // A later successful refresh clears the staleness.
        gate.record_success(t0 + bound + 1);
        assert!(
            !gate.hard_stale(t0 + bound + 1),
            "a fresh success clears staleness"
        );
    }

    /// `overlay_admission_limits`: a present override replaces its dimension, an
    /// absent one leaves the base, and a rate override keeps the base burst when
    /// the base was bounded.
    #[test]
    fn overlay_replaces_only_present_dimensions() {
        let base = AdmissionLimits {
            max_active_series: CountLimit::Bounded(100),
            max_active_streams: CountLimit::Bounded(200),
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: 1_000,
                burst: 5_000,
            },
            series_creation_rate: RateLimit::Unlimited,
        };
        let config = TenantConfig {
            max_active_series: Some(7),
            max_active_streams: None,
            max_ingest_byte_rate: Some(2_000),
            max_series_creation_rate: Some(9),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        let overlaid = overlay_admission_limits(base, &config);
        assert_eq!(
            overlaid.max_active_series,
            CountLimit::Bounded(7),
            "present count override applied"
        );
        assert_eq!(
            overlaid.max_active_streams,
            CountLimit::Bounded(200),
            "absent override leaves the base"
        );
        assert_eq!(
            overlaid.ingest_byte_rate,
            RateLimit::Bounded {
                per_sec: 2_000,
                burst: 5_000
            },
            "rate override sets per_sec, keeps the base burst"
        );
        assert_eq!(
            overlaid.series_creation_rate,
            RateLimit::Bounded {
                per_sec: 9,
                burst: 9
            },
            "rate override over an unlimited base uses the rate as burst"
        );
    }

    /// THE deliverable-1 test: a durable config-record change genuinely changes
    /// admission behaviour, not merely that the refresh ran. The controller
    /// starts with a base cap of 2; a durable record raising it to 5 is applied
    /// by `refresh_tenant_limits_once`, and admission then admits series it would
    /// previously have rejected; deleting the override (via a record that clears
    /// it) reverts admission to the base cap on the next refresh.
    #[tokio::test]
    async fn refresh_reinvokes_set_tenant_limits_and_admission_behaviour_changes() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        let base = base_limits(2);
        let controller = AdmissionController::new(Arc::new(SystemClock), base);
        // Seed the tenant at the base cap, as startup does.
        controller.set_tenant_limits(tenant.clone(), base);

        let now = SystemClock.now_ns();
        // At the base cap of 2, a fresh tenant admits 2 of 3 new series.
        let admitted = controller.admit_series(&tenant, &[series(1), series(2), series(3)], now);
        assert_eq!(admitted.admitted.len(), 2, "base cap of 2 admits two");
        assert_eq!(
            admitted.rejected.len(),
            1,
            "the third is rejected at the base cap"
        );

        // A durable record raises the cap to 5. The refresh must re-invoke
        // set_tenant_limits so admission actually changes.
        set_tenant_config(
            store.as_ref(),
            &tenant.hash(),
            &config_with_series_cap(5),
            now,
        )
        .await
        .expect("write raised-cap config");
        let got = refresh_tenant_limits_once(&controller, store.as_ref(), &tenant, base)
            .await
            .expect("refresh");
        assert!(got.is_some(), "a config record was found and applied");
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(5)),
            "the raised cap is now the effective cap"
        );
        // series 1 and 2 are still active (2 used); with the cap now 5 the next
        // three new series are all admitted, where before they would have been
        // rejected.
        let admitted = controller.admit_series(&tenant, &[series(4), series(5), series(6)], now);
        assert_eq!(
            admitted.admitted.len(),
            3,
            "raising the durable cap admits series the base cap rejected"
        );
        assert!(admitted.rejected.is_empty());

        // Clearing the override (a record with no cap) reverts to the base cap
        // on the next refresh: the numbers change back with no restart.
        set_tenant_config(
            store.as_ref(),
            &tenant.hash(),
            &TenantConfig::new(TenantLifecycleState::Active),
            now,
        )
        .await
        .expect("write cleared config");
        refresh_tenant_limits_once(&controller, store.as_ref(), &tenant, base)
            .await
            .expect("refresh");
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(2)),
            "clearing the durable override reverts to the base cap"
        );
    }

    /// BLOCK 2 regression (ADR-0066 decision 6 x ADR-0057): a refresh cycle
    /// whose durable config record is UNCHANGED must not clobber the per-process
    /// fleet share that reconciliation narrowed the caps to. The lifecycle loop
    /// re-invokes `set_tenant_limits` every horizon for every known tenant even
    /// when nothing changed; before the per-dimension guard in
    /// `TenantState::set_limits`, that unconditionally overwrote
    /// `fleet_max_active_series` back to the raw configured cap and rebuilt the
    /// rate bucket at full burst under the un-divided `per_sec`, re-admitting the
    /// full un-divided fleet cap for a reconcile interval out of every horizon.
    /// This seeds a base, narrows both the count cap and the byte rate via
    /// `apply_reconciliation`, runs an unchanged-config refresh, and asserts the
    /// narrowed values survive.
    ///
    /// Reverting the guard (restoring the unconditional overwrite in
    /// `set_limits`) makes this fail exactly as the reviewer's proof did: the
    /// effective series cap snaps back from the narrowed 25 to the raw 100 and
    /// the byte rate from 250 back to 1000.
    #[tokio::test]
    async fn refresh_with_unchanged_config_preserves_the_reconciled_fleet_share() {
        use crate::admission::TenantReconcileApply;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        // Raw configured (un-divided) limits: a count cap of 100 and a bounded
        // byte rate of 1000/s, both of which reconciliation will narrow.
        let base = AdmissionLimits {
            max_active_series: CountLimit::Bounded(100),
            max_active_streams: CountLimit::Unlimited,
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: 1_000,
                burst: 5_000,
            },
            series_creation_rate: RateLimit::Unlimited,
        };
        let controller = AdmissionController::new(Arc::new(SystemClock), base);
        // Startup seeds the tenant at the raw configured caps.
        controller.set_tenant_limits(tenant.clone(), base);
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(100)),
            "startup installs the raw configured count cap"
        );
        assert_eq!(
            controller.effective_byte_rate_per_sec(&tenant),
            Some(1_000),
            "startup installs the raw configured byte rate"
        );

        // A reconciliation cycle narrows this process's share to a quarter of
        // the fleet cap (a 4-process fleet splitting 100 -> 25, 1000/s -> 250/s).
        controller.apply_reconciliation(&TenantReconcileApply {
            tenant: tenant.clone(),
            series_cap: Some(CountLimit::Bounded(25)),
            streams_cap: None,
            byte_rate_per_sec: Some(250),
            creation_rate_per_sec: None,
            baseline_bytes: 0,
            baseline_created: 0,
        });
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(25)),
            "reconciliation narrowed the count cap to this process's share"
        );
        assert_eq!(
            controller.effective_byte_rate_per_sec(&tenant),
            Some(250),
            "reconciliation narrowed the byte rate to this process's share"
        );

        // A lifecycle refresh with the durable record UNCHANGED (here: no record
        // at all, so the identical base is re-applied). Nothing about the
        // tenant's configured limits changed, so the reconciled share must
        // survive rather than being clobbered back to the raw configured values.
        let got = refresh_tenant_limits_once(&controller, store.as_ref(), &tenant, base)
            .await
            .expect("refresh");
        assert!(got.is_none(), "no durable record: the base is re-applied");
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(25)),
            "an unchanged-config refresh must NOT clobber the reconciled count \
             share back to the raw configured cap of 100"
        );
        assert_eq!(
            controller.effective_byte_rate_per_sec(&tenant),
            Some(250),
            "an unchanged-config refresh must NOT rebuild the bucket back to the \
             raw configured byte rate of 1000 (nor hand out a fresh full burst)"
        );
    }

    /// No durable record re-applies the base limits (the tenant runs on its
    /// startup value), and a store fault is propagated so the caller can keep
    /// the last-known limits rather than resetting to base on a transient fault.
    #[tokio::test]
    async fn refresh_handles_absent_record_and_propagates_store_faults() {
        let base = base_limits(3);
        let tenant = TenantId::new("acme");

        // Absent record: base re-applied, Ok(None).
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let controller = AdmissionController::new(Arc::new(SystemClock), base);
        let got = refresh_tenant_limits_once(&controller, store.as_ref(), &tenant, base)
            .await
            .expect("absent record is not an error");
        assert!(got.is_none(), "no record found");
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(3)),
            "base limits applied for a tenant with no durable record"
        );

        // A store fault on the config GET is a typed error the caller propagates.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("config store down".into()),
            )
            .with_key_contains("/config"),
        );
        let faulty = FaultStore::new(MemoryStore::new(), plan);
        let err = refresh_tenant_limits_once(&controller, &faulty, &tenant, base)
            .await
            .expect_err("a store fault on the config read must propagate");
        assert!(matches!(err, TenantConfigError::Store { .. }), "got: {err}");
    }
}
