//! Fleet-global admission reconciliation (ADR-0057).
//!
//! [`AdmissionController`] (ADR-0051) enforces its caps entirely per process,
//! so a tenant routed across N replicas gets N times its configured budget.
//! This module closes that gap **off the hot path**: every reconciliation
//! interval `R`, each process writes its own current usage to a key it alone
//! ever writes and reads every sibling's key to compute the fleet-wide picture,
//! then updates the value the existing hot-path checks compare against. No key
//! is ever contended (each writer owns its key exclusively), so there is no
//! CAS, no retry-on-conflict, and no hot single object (ADR-0057 Decision).
//!
//! The hot path is unchanged: [`AdmissionController::admit_series`] and friends
//! read a field this task updates from outside the lock. The only new cost is
//! this background task, off the request path, on interval `R`.
//!
//! ## Snapshot key (ADR-0057 section 1)
//!
//! ```text
//! t/<tenant_hash>/<sig>/admission/<process_id>.snapshot
//! ```
//!
//! Written with [`PutMode::Overwrite`] — not `CreateIfAbsent`, not
//! `CasVersion` — because only the owning process ever writes it, so there is
//! no concurrent writer to race and a failed or stale write is self-correcting
//! on the next interval.
//!
//! ## The local soft thresholds (ADR-0057 section 2)
//!
//! Count and rate caps use different formulas, because one is a stock and the
//! other is a flow.
//!
//! Count caps (`active_series`/`active_streams`) use additive headroom:
//!
//! ```text
//! fleet_used(cap)          = own_current_usage(cap) + sum(non-stale sibling usage(cap))
//! local_soft_threshold(cap) = own_current_usage(cap) + max(0, configured_fleet_cap(cap) - fleet_used(cap))
//! ```
//!
//! This is correct for a stock: `own_current_usage` only grows through
//! admissions this same threshold gates, so once the fleet is at or over cap
//! the threshold collapses to `own_current_usage` and this process admits no
//! more — a stable fixed point. Counts sum as an upper bound: the same series
//! routed to two replicas is counted twice, a deliberate, documented
//! overestimate (ADR-0057 Context) that can only reject sooner than the letter
//! of the cap, never admit more.
//!
//! Rate caps (`ingest_byte_rate`/`series_creation_rate`) use an equal
//! fleet-share of the configured cap instead:
//!
//! ```text
//! N(cap)                         = 1 (self) + count of non-stale siblings reporting a snapshot
//! local_soft_rate_threshold(cap) = configured_fleet_cap(cap) / N(cap)   // integer division, floor
//! ```
//!
//! The additive-headroom formula does not converge for a flow: a rate's
//! `own_current_usage` is a *measured* rate, not a stock the threshold
//! controls, so once the fleet crosses the cap every process reads "whatever I
//! am already sending" as `own`, additive headroom returns exactly that, and
//! the fleet sustains the sum of everyone's rate indefinitely (a bug the
//! checkpoint reviewing this mechanism caught before it landed, 2026-08-06).
//! Dividing the cap into equal shares floors the fleet-wide sum at
//! `configured_fleet_cap` (floor-rounding can only lose up to `N - 1` total,
//! never exceed the cap). `N` is computed from the same non-stale sibling set
//! the count formula reads, so count and rate reconciliation never disagree
//! about who is live.
//!
//! ## Staleness (ADR-0057 section 3)
//!
//! A sibling whose `snapshot_unix_ns` is older than `2 * R` at read time is
//! excluded and treated as contributing **zero** — a silent/dead process is
//! assumed not to be consuming budget, so one slow write never freezes another
//! process's admission. If a process's own reconciliation read (the LIST or a
//! GET) fails, it keeps its last-computed threshold and increments
//! `ravel_admission_reconciliation_failures_total`; it never fails closed to
//! zero admission on a transient store error.

use std::collections::HashSet;
use std::time::Duration;

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use ravel_proto::sys::v1::AdmissionUsageSnapshot;
use ravel_types::{Signal, TenantHash};

use crate::admission::{AdmissionController, TenantReconcileApply};

/// Default reconciliation interval `R` (ADR-0057 section 4): 10 seconds. Keeps
/// the LIST-then-N-GETs read cheap while bounding the overshoot window (at
/// most one interval's admission per process) short enough that an operator
/// sizing a hard cap need not pad it materially. An operator wanting a tighter
/// bound configures a shorter `R`, trading request volume for a smaller window.
pub const DEFAULT_ADMISSION_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// Format floor for [`AdmissionUsageSnapshot`] (matches the guard every other
/// `sys/` message uses). A sibling snapshot advertising a higher floor is one
/// this reader does not understand, so it is skipped (treated as absent) rather
/// than misread.
const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// The immutable infix between a (tenant, signal) prefix and the per-process
/// snapshot key.
const ADMISSION_INFIX: &str = "admission";

/// The snapshot key one process owns for a (tenant, signal) (ADR-0057 section
/// 1). The frozen object-key layout gains this one additive prefix; no existing
/// key is reshaped.
pub fn snapshot_key(tenant_hash: &TenantHash, signal: Signal, process_id: &str) -> String {
    format!(
        "t/{}/{}/{}/{}.snapshot",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        ADMISSION_INFIX,
        process_id,
    )
}

/// The listing prefix covering every process's snapshot for a (tenant, signal).
fn admission_prefix(tenant_hash: &TenantHash, signal: Signal) -> String {
    format!(
        "t/{}/{}/{}/",
        tenant_hash.to_hex(),
        signal.key_prefix(),
        ADMISSION_INFIX,
    )
}

/// A sibling process's decoded snapshot for one (tenant, signal). `process_id`
/// comes from the object key (the snapshot body carries no id), and is how a
/// reader excludes its own key and counts each distinct sibling once across a
/// process's per-signal snapshots. The rate caps divide by the number of live
/// siblings rather than summing their flow, so the snapshot's flow-delta fields
/// are not decoded here.
pub(crate) struct SiblingSnapshot {
    pub(crate) process_id: String,
    pub(crate) snapshot_unix_ns: i64,
    pub(crate) active_series_count: u64,
    pub(crate) active_streams_count: u64,
}

/// The local soft threshold for one **count** cap (ADR-0057 section 2):
/// `own + max(0, configured_cap - (own + siblings))`. Saturating throughout, so
/// a fleet already over its cap yields exactly `own` (this process admits no
/// more but keeps what it has), never underflow. This is the value the hot path
/// compares against, replacing the static configured cap.
///
/// This is the **stock** formula and is correct only for a stock (a count that
/// only grows through admissions this same threshold gates). Rate caps are a
/// flow and use [`local_soft_rate_threshold`] instead; the two are deliberately
/// kept as separate functions, since applying this one to a flow does not
/// converge (see [`local_soft_rate_threshold`] for the failure mode).
pub(crate) fn local_soft_threshold(own: u64, siblings_sum: u64, configured_cap: u64) -> u64 {
    let fleet_used = own.saturating_add(siblings_sum);
    let remaining = configured_cap.saturating_sub(fleet_used);
    own.saturating_add(remaining)
}

/// The local soft threshold for one **rate** cap (ADR-0057 section 2): an equal
/// fleet-share of the configured cap, `configured_cap / n`, where `n` is this
/// process plus every non-stale sibling that reported a snapshot for this
/// (tenant, signal), regardless of whether its reported delta was itself zero.
/// Integer division floors, so the sum of every process's share is at most
/// `configured_cap` (floor-rounding loses at most `n - 1` total, never exceeds
/// the cap).
///
/// A rate is a flow, not a stock, so [`local_soft_threshold`]'s additive-
/// headroom formula does not converge for it: once the fleet crosses the cap
/// every process reads its own current (over-cap) rate as `own`, additive
/// headroom returns exactly that, and the fleet sustains the sum of everyone's
/// rate indefinitely rather than for one interval. Equal-share division has no
/// such fixed point above the cap. `n == 0` cannot occur (a process always
/// counts itself), but is handled as the lone-process case rather than
/// dividing by zero.
pub(crate) fn local_soft_rate_threshold(configured_cap: u64, n: u64) -> u64 {
    if n == 0 {
        return configured_cap;
    }
    configured_cap / n
}

/// Whether a sibling snapshot is stale at read time (ADR-0057 section 3): older
/// than `2 * R`. Exactly `2 * R` old is **not** stale (the boundary is
/// inclusive on the fresh side); a snapshot from the future (negative age,
/// e.g. clock skew) is treated as fresh.
pub(crate) fn is_stale(now_ns: i64, snapshot_unix_ns: i64, two_r_ns: i64) -> bool {
    now_ns.saturating_sub(snapshot_unix_ns) > two_r_ns
}

/// Extract the `<process_id>` from a snapshot key
/// (`.../admission/<process_id>.snapshot`). Returns `None` for a key that does
/// not match the shape (a defensive guard; every key this task writes matches).
fn process_id_of(key: &str) -> Option<&str> {
    key.rsplit('/').next()?.strip_suffix(".snapshot")
}

/// Read and decode every sibling snapshot under a (tenant, signal)'s admission
/// prefix, excluding this process's own key. A corrupt or future-version
/// sibling is skipped (treated as absent, self-correcting next interval), not
/// an error: only a failed LIST or GET is an [`Err`] the caller counts as a
/// reconciliation failure (ADR-0057 section 3).
async fn read_siblings(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    own_process_id: &str,
) -> Result<Vec<SiblingSnapshot>, StoreError> {
    let prefix = admission_prefix(tenant_hash, signal);
    let objects = list_all(store, &prefix).await?;
    let mut out = Vec::new();
    for meta in objects {
        let Some(pid) = process_id_of(&meta.key) else {
            continue;
        };
        if pid == own_process_id {
            continue;
        }
        let pid = pid.to_string();
        let got = store.get(&meta.key, GetRange::Full).await?;
        let Ok(snap) = AdmissionUsageSnapshot::decode(got.data.as_ref()) else {
            tracing::debug!(
                key = %meta.key,
                "admission reconciliation: skipping an undecodable sibling snapshot"
            );
            continue;
        };
        if snap.format_version > SNAPSHOT_FORMAT_VERSION {
            tracing::debug!(
                key = %meta.key,
                version = snap.format_version,
                "admission reconciliation: skipping a future-version sibling snapshot"
            );
            continue;
        }
        out.push(SiblingSnapshot {
            process_id: pid,
            snapshot_unix_ns: snap.snapshot_unix_ns,
            active_series_count: snap.active_series_count,
            active_streams_count: snap.active_streams_count,
        });
    }
    Ok(out)
}

/// Reconcile one count cap: `None` when the configured cap is unlimited (no
/// fleet cap to enforce, leave the effective cap untouched), otherwise
/// `Some(Bounded(local_soft_threshold))`.
fn reconcile_count(
    configured: crate::admission::CountLimit,
    own: u64,
    siblings_sum: u64,
) -> Option<crate::admission::CountLimit> {
    match configured {
        crate::admission::CountLimit::Unlimited => None,
        crate::admission::CountLimit::Bounded(cap) => Some(crate::admission::CountLimit::Bounded(
            local_soft_threshold(own, siblings_sum, cap),
        )),
    }
}

/// Reconcile one rate cap into an effective `rate_per_sec`: `None` when the
/// configured rate is unlimited (no bucket to reconcile). A bounded rate cap
/// is divided into an equal fleet-share, `per_sec / n`, where `n` is this
/// process plus every non-stale sibling (ADR-0057 section 2). Unlike the count
/// caps, this does not read any flow delta: the equal-share split depends only
/// on how many live processes there are, not on how much each is currently
/// sending, which is exactly the property that makes it converge for a flow.
fn reconcile_rate(configured: crate::admission::RateLimit, n: u64) -> Option<u64> {
    match configured {
        crate::admission::RateLimit::Unlimited => None,
        crate::admission::RateLimit::Bounded { per_sec, .. } => {
            Some(local_soft_rate_threshold(per_sec, n))
        }
    }
}

/// Run exactly one reconciliation cycle for every (tenant, signal) the process
/// currently tracks (ADR-0057 sections 1-3). This is the whole mechanism; the
/// server wraps it in a periodic loop with clean shutdown (like the fold and
/// maintenance tasks), so the interval/lifecycle lives there and the cycle
/// itself stays unit-testable.
///
/// Never blocks or delays the hot path: the controller's mutex is taken only by
/// the two lock-free-I/O hooks ([`AdmissionController::snapshot_for_reconciliation`]
/// and [`AdmissionController::apply_reconciliation`]), and every object-store
/// LIST/GET/PUT happens between them with no lock held.
pub async fn reconcile_once(
    controller: &AdmissionController,
    store: &dyn ObjectStoreBackend,
    interval: Duration,
    now_ns: i64,
) {
    let r_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
    let two_r_ns = r_ns.saturating_mul(2);
    let process_id = controller.process_id().to_string();

    let states = controller.snapshot_for_reconciliation(now_ns);
    for state in states {
        // 1. Write this process's own per-signal snapshots (Overwrite: single
        //    writer, no CAS). A write failure is logged and self-corrects next
        //    interval; it is NOT the counted read failure.
        for &signal in &state.signals {
            let snapshot = AdmissionUsageSnapshot {
                format_version: SNAPSHOT_FORMAT_VERSION,
                active_series_count: if signal == Signal::Metrics {
                    state.active_series
                } else {
                    0
                },
                active_streams_count: if signal == Signal::Logs {
                    state.active_streams
                } else {
                    0
                },
                byte_rate_consumed_since_last_snapshot: state.byte_delta,
                series_creation_consumed_since_last_snapshot: state.creation_delta,
                snapshot_unix_ns: now_ns,
            };
            let key = snapshot_key(&state.tenant_hash, signal, &process_id);
            let body = snapshot.encode_to_vec();
            if let Err(err) = store
                .put(
                    &key,
                    body.into(),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        checksum: None,
                    },
                )
                .await
            {
                tracing::warn!(
                    key = %key,
                    error = %err,
                    "admission reconciliation: snapshot write failed; self-corrects next interval"
                );
            }
        }

        // 2. Read every sibling snapshot for each signal. A failed read for a
        //    signal keeps that signal's last threshold and, to stay
        //    conservative, keeps the whole tenant's thresholds (the tenant-wide
        //    rate flow it would sum is incomplete), while incrementing the
        //    failure counter. Fresh siblings are summed per ADR-0057 section 2.
        let mut read_failed = false;
        let mut sibling_series: u64 = 0;
        let mut sibling_streams: u64 = 0;
        // The distinct non-stale sibling processes, deduplicated by process id:
        // a process writes into each of its per-signal snapshots, so the same
        // sibling appears once per signal it tracks. This one set is both the
        // count formula's sibling membership and the rate formula's denominator
        // `N` (= this set's size + 1), so count and rate reconciliation can
        // never disagree about who is live (ADR-0057 section 2).
        let mut nonstale_siblings: HashSet<String> = HashSet::new();

        for &signal in &state.signals {
            match read_siblings(store, &state.tenant_hash, signal, &process_id).await {
                Ok(siblings) => {
                    for sib in siblings {
                        if is_stale(now_ns, sib.snapshot_unix_ns, two_r_ns) {
                            continue;
                        }
                        sibling_series = sibling_series.saturating_add(sib.active_series_count);
                        sibling_streams = sibling_streams.saturating_add(sib.active_streams_count);
                        nonstale_siblings.insert(sib.process_id);
                    }
                }
                Err(err) => {
                    read_failed = true;
                    controller.record_reconciliation_failure(&state.tenant, signal);
                    tracing::warn!(
                        tenant_hash = %state.tenant_hash.to_hex(),
                        signal = ?signal,
                        error = %err,
                        "admission reconciliation: sibling read failed; keeping last-known \
                         threshold (never fails closed to zero, ADR-0057 section 3)"
                    );
                }
            }
        }

        if read_failed {
            // Keep every last-computed threshold for this tenant (ADR-0057
            // section 3): do not apply a partial view.
            continue;
        }

        // 3. Compute and apply the local soft thresholds. `N` for the rate caps
        //    is this process plus every distinct non-stale sibling (the same set
        //    the count formula summed over), so a fleet of `N` live processes
        //    each enforces `configured_cap / N` and the fleet-wide sum floors at
        //    the configured cap (ADR-0057 section 2).
        let n = (nonstale_siblings.len() as u64).saturating_add(1);

        controller.apply_reconciliation(&TenantReconcileApply {
            tenant: state.tenant.clone(),
            series_cap: reconcile_count(
                state.configured_series,
                state.active_series,
                sibling_series,
            ),
            streams_cap: reconcile_count(
                state.configured_streams,
                state.active_streams,
                sibling_streams,
            ),
            byte_rate_per_sec: reconcile_rate(state.configured_byte_rate, n),
            creation_rate_per_sec: reconcile_rate(state.configured_creation_rate, n),
            baseline_bytes: state.snapshot_bytes_total,
            baseline_created: state.snapshot_created_total,
        });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::{SeriesId, TenantId};

    use super::*;
    use crate::Clock;
    use crate::admission::{AdmissionController, AdmissionLimits, CountLimit, RateLimit};

    const R: Duration = Duration::from_secs(10);
    const R_NS: i64 = 10 * 1_000_000_000;

    struct TestClock(AtomicI64);
    impl TestClock {
        fn new(now_ns: i64) -> Arc<Self> {
            Arc::new(TestClock(AtomicI64::new(now_ns)))
        }
        fn set(&self, now_ns: i64) {
            self.0.store(now_ns, Ordering::SeqCst);
        }
    }
    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn series(byte: u8) -> SeriesId {
        SeriesId([byte; 16])
    }

    /// Limits with a bounded active-series cap and everything else unlimited, so
    /// only the active-series count cap gates.
    fn count_only_limits(max_active_series: u64) -> AdmissionLimits {
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(max_active_series),
            max_active_streams: CountLimit::Bounded(max_active_series),
            ingest_byte_rate: RateLimit::Unlimited,
            series_creation_rate: RateLimit::Unlimited,
        }
    }

    /// Limits with a bounded ingest byte-rate and everything else unlimited, so
    /// only the byte-rate cap reconciles. The burst is set far above any single
    /// request these tests make, so `check_byte_rate` always admits and the
    /// tests observe the reconciled `rate_per_sec`, not token exhaustion.
    fn rate_only_limits(per_sec: u64) -> AdmissionLimits {
        AdmissionLimits {
            max_active_series: CountLimit::Unlimited,
            max_active_streams: CountLimit::Unlimited,
            ingest_byte_rate: RateLimit::Bounded {
                per_sec,
                burst: per_sec.max(1).saturating_mul(1000),
            },
            series_creation_rate: RateLimit::Unlimited,
        }
    }

    /// A controller with a bounded byte-rate cap that already carries live
    /// byte-rate usage for `tenant` (so it is tracked for reconciliation), all
    /// on the shared `clock`.
    fn rate_process(clock: Arc<TestClock>, per_sec: u64, tenant: &TenantId) -> AdmissionController {
        let controller = AdmissionController::new(clock.clone(), rate_only_limits(per_sec));
        controller.set_tenant_limits(tenant.clone(), rate_only_limits(per_sec));
        // One request records byte-rate usage, so the tenant is tracked; the
        // equal-share formula does not read the amount, only that the process is
        // live.
        let _ = controller.check_byte_rate(tenant, Signal::Metrics, per_sec, clock.now_ns());
        controller
    }

    fn sibling(snapshot_unix_ns: i64, active_series_count: u64) -> SiblingSnapshot {
        SiblingSnapshot {
            process_id: format!("pid-{snapshot_unix_ns}-{active_series_count}"),
            snapshot_unix_ns,
            active_series_count,
            active_streams_count: 0,
        }
    }

    /// THE formula test (ADR-0057 section 2): `local_soft_threshold` matches the
    /// reconciliation formula, including the safe-overestimate direction for
    /// count caps (two siblings reporting overlapping series SUM, never dedupe)
    /// and the exact staleness boundary at `2 * R`.
    #[test]
    fn local_soft_threshold_matches_the_reconciliation_formula() {
        // Plain formula: own + max(0, cap - own - siblings).
        assert_eq!(local_soft_threshold(30, 40, 100), 30 + (100 - 30 - 40));
        // Fleet already at cap: no remaining, threshold pins to own (admit no
        // more, keep what we have) — never underflows.
        assert_eq!(local_soft_threshold(60, 60, 100), 60);
        assert_eq!(local_soft_threshold(200, 50, 100), 200);
        // No siblings: threshold is the full configured cap.
        assert_eq!(local_soft_threshold(10, 0, 100), 100);

        // Safe overestimate: two siblings each reporting 40 active series (which
        // may well be the SAME series routed to both) SUM to 80, they are not
        // deduplicated, so fleet_used is over-counted and the threshold is
        // driven down sooner than a true distinct count would.
        let now = 1_000 * R_NS;
        let two_r = 2 * R_NS;
        let siblings = [
            sibling(now, 40), // fresh
            sibling(now, 40), // fresh, overlapping — summed, not deduped
        ];
        let fresh_sum: u64 = siblings
            .iter()
            .filter(|s| !is_stale(now, s.snapshot_unix_ns, two_r))
            .map(|s| s.active_series_count)
            .sum();
        assert_eq!(fresh_sum, 80, "overlapping sibling counts sum, not dedupe");
        // own 30, cap 100: fleet_used = 30 + 80 = 110 > 100, so remaining 0 and
        // the threshold is own (30) — the overestimate rejects sooner, the safe
        // direction.
        assert_eq!(local_soft_threshold(30, fresh_sum, 100), 30);

        // Exact staleness boundary at 2 * R: a sibling exactly 2*R old is kept;
        // one a nanosecond older is excluded and contributes zero.
        let at_boundary = sibling(now - two_r, 25);
        let just_past = sibling(now - two_r - 1, 25);
        assert!(
            !is_stale(now, at_boundary.snapshot_unix_ns, two_r),
            "exactly 2*R old is still fresh"
        );
        assert!(
            is_stale(now, just_past.snapshot_unix_ns, two_r),
            "one ns past 2*R is stale and contributes zero"
        );
        let boundary_sum: u64 = [at_boundary, just_past]
            .iter()
            .filter(|s| !is_stale(now, s.snapshot_unix_ns, two_r))
            .map(|s| s.active_series_count)
            .sum();
        assert_eq!(boundary_sum, 25, "only the non-stale sibling counts");
    }

    /// A failed reconciliation read does not fail admission closed (ADR-0057
    /// section 3): the last-computed threshold keeps being used and the failure
    /// counter increments.
    #[tokio::test]
    async fn failed_reconciliation_read_keeps_last_threshold_and_counts() {
        let tenant = TenantId::new("acme");
        let tenant_hash = tenant.hash();
        let clock = TestClock::new(1_000 * R_NS);
        // Fleet cap 100; this process has admitted 10 series.
        let controller = AdmissionController::new(clock.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        for i in 0..10u8 {
            controller.admit_series(&tenant, [series(i)], clock.now_ns());
        }

        // The SECOND LIST under the admission prefix faults; the first passes.
        // So the first (clean) cycle installs a real threshold and the second
        // (faulting) cycle exercises the failure path.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("admission list down".into()),
            )
            .with_key_contains("/admission/")
            .with_occurrence(Occurrence::Nth(2)),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));

        // A busy sibling (60 active series) already sits in the store (PUT is not
        // faulted), so a successful cycle drives this process's effective cap
        // below the raw configured 100.
        let sibling_snapshot = AdmissionUsageSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            active_series_count: 60,
            active_streams_count: 0,
            byte_rate_consumed_since_last_snapshot: 0,
            series_creation_consumed_since_last_snapshot: 0,
            snapshot_unix_ns: clock.now_ns(),
        };
        let sibling_key = snapshot_key(&tenant_hash, Signal::Metrics, "sibling-process");
        store
            .put(
                &sibling_key,
                sibling_snapshot.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed sibling snapshot");

        // A clean cycle (first LIST): effective cap becomes
        // own(10) + max(0, 100 - 10 - 60) = 40.
        reconcile_once(&controller, store.as_ref(), R, clock.now_ns()).await;
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(40)),
            "a clean cycle installs the reconciled threshold"
        );

        // A second cycle whose LIST faults: the read cannot complete.
        reconcile_once(&controller, store.as_ref(), R, clock.now_ns()).await;

        // The last-known threshold (40) still holds — NOT reset to the configured
        // 100, and NOT failed closed to 0 — and the failure counter incremented.
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(40)),
            "a failed read keeps the last-computed threshold"
        );
        let failures: u64 = controller
            .usage_snapshot()
            .iter()
            .filter(|u| u.tenant_hash == tenant_hash && u.signal == Signal::Metrics)
            .map(|u| u.reconciliation_failures_total)
            .sum();
        assert_eq!(
            failures, 1,
            "the reconciliation failure counter incremented"
        );
    }

    /// A quiet fleet (no siblings) reconciles a bounded cap to exactly the
    /// configured value, so a lone process behaves identically to per-process
    /// ADR-0051 enforcement.
    #[tokio::test]
    async fn lone_process_reconciles_to_the_configured_cap() {
        let tenant = TenantId::new("solo");
        let clock = TestClock::new(500 * R_NS);
        let controller = AdmissionController::new(clock.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        controller.admit_series(&tenant, [series(1), series(2)], clock.now_ns());

        let store = Arc::new(MemoryStore::new());
        reconcile_once(&controller, store.as_ref(), R, clock.now_ns()).await;
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(100)),
            "with no siblings the effective cap equals the configured fleet cap"
        );

        // A prior process's snapshot that has gone stale (> 2*R old) contributes
        // zero, so the cap stays at the configured value rather than being
        // dragged down by a dead process (ADR-0057 section 3).
        let stale = AdmissionUsageSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            active_series_count: 90,
            active_streams_count: 0,
            byte_rate_consumed_since_last_snapshot: 0,
            series_creation_consumed_since_last_snapshot: 0,
            snapshot_unix_ns: clock.now_ns() - 3 * R_NS,
        };
        store
            .put(
                &snapshot_key(&tenant.hash(), Signal::Metrics, "dead-process"),
                stale.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed stale snapshot");
        clock.set(clock.now_ns()); // read time unchanged
        reconcile_once(&controller, store.as_ref(), R, clock.now_ns()).await;
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(100)),
            "a stale sibling is excluded and never drags the cap down"
        );
    }

    /// The rate-share formula (ADR-0057 section 2): `cap / N`, floored, so the
    /// sum of every process's share never exceeds the cap. This is the formula
    /// that replaced the buggy additive-headroom one for flows.
    #[test]
    fn local_soft_rate_threshold_divides_the_cap_into_equal_shares() {
        assert_eq!(
            local_soft_rate_threshold(100, 1),
            100,
            "lone process gets the whole cap"
        );
        assert_eq!(local_soft_rate_threshold(100, 2), 50);
        // Floor division: three shares of 33 sum to 99 <= 100, never over.
        assert_eq!(local_soft_rate_threshold(100, 3), 33);
        assert!(3 * local_soft_rate_threshold(100, 3) <= 100);
        // n == 0 cannot happen (a process always counts itself) but must not
        // divide by zero.
        assert_eq!(local_soft_rate_threshold(100, 0), 100);
    }

    /// Two processes each offered the full fleet byte-rate cap converge, through
    /// real `reconcile_once` cycles over a shared store, to an equal share whose
    /// sum is at most the cap. Under the old additive-headroom formula each
    /// would instead settle above `cap / 2` and the fleet would sustain more
    /// than the cap; the equal-share formula pins each to exactly `cap / N`.
    #[tokio::test]
    async fn rate_share_two_over_cap_processes_converge_to_at_most_cap() {
        let tenant = TenantId::new("acme");
        let clock = TestClock::new(1_000 * R_NS);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let cap = 100;
        let a = rate_process(clock.clone(), cap, &tenant);
        let b = rate_process(clock.clone(), cap, &tenant);

        // One reconciliation round: A writes (no sibling yet), B writes and
        // reads A, then A reads B. Both then see N = 2.
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&b, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;

        let ra = a.effective_byte_rate_per_sec(&tenant).unwrap();
        let rb = b.effective_byte_rate_per_sec(&tenant).unwrap();
        assert_eq!(ra, 50, "each process enforces cap / N = 100 / 2");
        assert_eq!(rb, 50);
        assert!(
            ra + rb <= cap,
            "combined fleet rate {ra}+{rb} stays within cap {cap}"
        );
    }

    /// A third process joining the fleet is picked up in `N` within one further
    /// reconciliation round: once each of the three has written and read once
    /// more, all three enforce `cap / 3`.
    #[tokio::test]
    async fn rate_share_picks_up_a_third_process_within_one_interval() {
        let tenant = TenantId::new("acme");
        let clock = TestClock::new(2_000 * R_NS);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let cap = 100;
        let a = rate_process(clock.clone(), cap, &tenant);
        let b = rate_process(clock.clone(), cap, &tenant);

        // Converge the two-process fleet first.
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&b, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        assert_eq!(a.effective_byte_rate_per_sec(&tenant).unwrap(), 50);
        assert_eq!(b.effective_byte_rate_per_sec(&tenant).unwrap(), 50);

        // A third process joins. One round each (C, then A, then B) is enough
        // for every process's N to reach 3, at the same non-stale instant.
        let c = rate_process(clock.clone(), cap, &tenant);
        reconcile_once(&c, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&b, store.as_ref(), R, clock.now_ns()).await;

        let ra = a.effective_byte_rate_per_sec(&tenant).unwrap();
        let rb = b.effective_byte_rate_per_sec(&tenant).unwrap();
        let rc = c.effective_byte_rate_per_sec(&tenant).unwrap();
        assert_eq!(
            (ra, rb, rc),
            (33, 33, 33),
            "each enforces cap / N = 100 / 3"
        );
        assert!(
            ra + rb + rc <= cap,
            "combined fleet rate {ra}+{rb}+{rc} stays within cap {cap}"
        );
    }

    /// A sibling that goes silent past the `2 * R` staleness window drops out of
    /// `N`, so a process reclaims its share of the now-smaller fleet: with the
    /// only sibling stale, `N` falls to 1 and the full cap is restored.
    #[tokio::test]
    async fn rate_share_drops_a_stale_sibling_within_the_staleness_window() {
        let tenant = TenantId::new("acme");
        let clock = TestClock::new(3_000 * R_NS);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let cap = 100;
        let a = rate_process(clock.clone(), cap, &tenant);
        let b = rate_process(clock.clone(), cap, &tenant);

        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&b, store.as_ref(), R, clock.now_ns()).await;
        reconcile_once(&a, store.as_ref(), R, clock.now_ns()).await;
        assert_eq!(a.effective_byte_rate_per_sec(&tenant).unwrap(), 50);

        // B goes silent. A reconciles one nanosecond past `2 * R` later: B's
        // last snapshot is now stale and excluded, so N drops to 1.
        let later = clock.now_ns() + 2 * R_NS + 1;
        reconcile_once(&a, store.as_ref(), R, later).await;
        assert_eq!(
            a.effective_byte_rate_per_sec(&tenant).unwrap(),
            100,
            "a stale sole sibling drops out of N, restoring the full fleet cap"
        );
    }
}
