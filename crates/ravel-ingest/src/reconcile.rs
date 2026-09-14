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
//! the fleet sustains the sum of everyone's rate indefinitely (a bug caught in review before it landed).
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
//!
//! ## Reading only the live fleet, and reaping the dead
//!
//! A snapshot key is written once per process per (tenant, signal) and the
//! process id is fresh per process, so the prefix accumulates one key for every
//! process that has ever served the tenant. Reading them all back would make a
//! cycle's cost grow without bound: with a GET per listed key, a year of daily
//! restarts turns one cycle into hundreds of thousands of serial round trips,
//! the cycle overruns `2 * R`, every sibling then reads as stale and each
//! process enforces the whole fleet cap alone (issue #1679).
//!
//! Two bounds keep the cost proportional to the *live* fleet instead:
//!
//! - The LIST result already carries each key's
//!   [`last_modified_unix_ms`](ravel_object_store::ObjectMeta::last_modified_unix_ms),
//!   so a key whose modification time is
//!   already past the same `2 * R` window is skipped without a GET
//!   ([`mtime_stale`]). Skipping never drops a sibling the decoded body would
//!   have counted as fresh **under the assumption that the writer's clock and
//!   the object store's agree**: `snapshot_unix_ns` is stamped by the
//!   writer's clock, the modification time by the store's, and only when
//!   those two agree is the body guaranteed no fresher than the mtime the
//!   LIST already showed. A writer clock running far enough ahead of the
//!   store's can in principle write a body that reads as fresh while the
//!   store's own modification time already looks past the window, making the
//!   skip drop it. In practice this needs skew large enough to also defeat
//!   the direct [`is_stale`] check on the body, since a live sibling
//!   refreshes its mtime every `R`; the skip is therefore no worse than that
//!   check, not a stronger guarantee than it, and either way it
//!   self-corrects the next interval.
//! - A key past the *reap horizon* ([`reap_horizon_ns`], `2 * R` widened by the
//!   same factor again) is deleted, which bounds the LIST itself. The extra
//!   width is the clock-skew margin: the modification time comes from the
//!   object store's clock while the horizon is measured against this reader's,
//!   so a writer whose key looks older than it is keeps a full staleness window
//!   of grace before its key is reaped. Reaping a live process's key costs
//!   nothing but one interval of invisibility anyway, since that process
//!   rewrites the key every `R`.
//!
//! A backend that reports no usable modification time (`<= 0`) gets neither
//! treatment: its keys are read, as before, and never reaped.

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

/// How much wider than the staleness window the reap horizon is: a key is
/// deleted only once it is `REAP_WINDOW_FACTOR * 2 * R` old. The factor, rather
/// than a second absolute duration, keeps one configured `R` the only knob and
/// leaves the reap horizon derived from the same window
/// [`is_stale`] judges against.
pub(crate) const REAP_WINDOW_FACTOR: i64 = 2;

/// The reap horizon for a staleness window (issue #1679): the window widened by
/// [`REAP_WINDOW_FACTOR`], which is the clock-skew margin. Staleness is judged
/// against a stamp the writer wrote with its own clock, while reaping is judged
/// against a modification time the object store wrote with *its* clock; a
/// writer whose apparent age runs ahead by up to a full staleness window (skew
/// plus write jitter plus the modification time's 1-second granularity on real
/// backends) is therefore still not reaped while it is alive.
pub(crate) fn reap_horizon_ns(window_ns: i64) -> i64 {
    window_ns.saturating_mul(REAP_WINDOW_FACTOR)
}

/// Whether a listed key is already past `window_ns` according to the LIST
/// result's modification time alone, so its GET can be skipped (or, at the reap
/// horizon, the key deleted).
///
/// `last_modified_unix_ms <= 0` means the backend reported no usable
/// modification time (the in-memory oracle's default clock does exactly this).
/// That is "unknown", never "ancient": the key is read normally and is never
/// reaped, so a backend without modification times keeps the pre-#1679
/// behaviour rather than silently losing every sibling.
pub(crate) fn mtime_stale(now_ns: i64, last_modified_unix_ms: i64, window_ns: i64) -> bool {
    if last_modified_unix_ms <= 0 {
        return false;
    }
    let last_modified_ns = last_modified_unix_ms.saturating_mul(1_000_000);
    is_stale(now_ns, last_modified_ns, window_ns)
}

/// What one [`read_siblings`] call observed under a (tenant, signal) prefix.
#[derive(Default)]
struct SiblingRead {
    /// Siblings whose body was read and decoded (still subject to the
    /// authoritative [`is_stale`] check on `snapshot_unix_ns`).
    siblings: Vec<SiblingSnapshot>,
    /// Keys past the reap horizon, for the caller to delete.
    reapable: Vec<String>,
    /// Keys the LIST returned whose GET was skipped as already stale.
    skipped_stale: u64,
}

/// Extract the `<process_id>` from a snapshot key
/// (`.../admission/<process_id>.snapshot`). Returns `None` for a key that does
/// not match the shape (a defensive guard; every key this task writes matches).
fn process_id_of(key: &str) -> Option<&str> {
    key.rsplit('/').next()?.strip_suffix(".snapshot")
}

/// Read and decode every *live* sibling snapshot under a (tenant, signal)'s
/// admission prefix, excluding this process's own key. A corrupt or
/// future-version sibling is skipped (treated as absent, self-correcting next
/// interval), not an error: only a failed LIST or GET is an [`Err`] the caller
/// counts as a reconciliation failure (ADR-0057 section 3).
///
/// A key the LIST already shows as older than `two_r_ns` costs no GET, and one
/// past [`reap_horizon_ns`] is returned for the caller to delete (issue #1679);
/// see the [module docs](self).
async fn read_siblings(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    own_process_id: &str,
    now_ns: i64,
    two_r_ns: i64,
) -> Result<SiblingRead, StoreError> {
    let prefix = admission_prefix(tenant_hash, signal);
    let objects = list_all(store, &prefix).await?;
    let reap_horizon = reap_horizon_ns(two_r_ns);
    let mut out = SiblingRead::default();
    for meta in objects {
        let Some(pid) = process_id_of(&meta.key) else {
            continue;
        };
        if pid == own_process_id {
            continue;
        }
        if mtime_stale(now_ns, meta.last_modified_unix_ms, two_r_ns) {
            // Already past the staleness window by the LIST's own metadata: it
            // would contribute zero, so its body is never fetched.
            out.skipped_stale = out.skipped_stale.saturating_add(1);
            if mtime_stale(now_ns, meta.last_modified_unix_ms, reap_horizon) {
                out.reapable.push(meta.key);
            }
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
        out.siblings.push(SiblingSnapshot {
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

/// What one [`reconcile_once`] cycle cost and saw (issue #1679). The cycle is
/// what silently degrades when the control-plane prefixes fill with dead
/// processes' keys, and nothing observable moved while it did: the LISTs and
/// GETs all succeed, so `ravel_admission_reconciliation_failures_total` stays
/// at zero while every sibling ages past `2 * R` before it is read.
///
/// Returned by [`reconcile_once`] and published on the controller, where
/// [`AdmissionController::last_reconcile_cycle_stats`] reads the most recent
/// cycle's copy. Each field names the `ravel_admission_*` series it belongs
/// under, beside the existing families.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileCycleStats {
    /// `ravel_admission_reconciliation_cycle_duration_seconds` (gauge, exported
    /// as seconds): how long this cycle took, measured on the controller's
    /// injected clock. A cycle approaching `2 * R` is the failure this figure
    /// exists to make visible; it saturates at zero rather than going negative
    /// if the clock steps backwards mid-cycle.
    pub cycle_duration_ns: i64,
    /// `ravel_admission_reconciliation_siblings_observed` (gauge): distinct
    /// non-stale sibling processes seen across every (tenant, signal) this
    /// cycle, which is the live fleet size this process reconciled against.
    pub siblings_observed: u64,
    /// `ravel_admission_reconciliation_stale_keys_skipped` (gauge): listed keys
    /// whose GET was skipped because the LIST already showed them past the
    /// staleness window.
    pub stale_keys_skipped: u64,
    /// `ravel_admission_reconciliation_keys_reaped_total` (counter): keys past
    /// the reap horizon this cycle deleted.
    pub keys_reaped: u64,
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
///
/// The cycle reads only the live fleet and reaps the dead from the prefixes it
/// already lists (issue #1679, see the [module docs](self)), and returns what it
/// cost and saw as [`ReconcileCycleStats`].
pub async fn reconcile_once(
    controller: &AdmissionController,
    store: &dyn ObjectStoreBackend,
    interval: Duration,
    now_ns: i64,
) -> ReconcileCycleStats {
    let r_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
    let two_r_ns = r_ns.saturating_mul(2);
    let process_id = controller.process_id().to_string();
    let mut stats = ReconcileCycleStats::default();
    // Every distinct non-stale sibling this cycle saw, across every tenant and
    // signal: the live fleet size, reported as one gauge for the process.
    let mut cycle_siblings: HashSet<String> = HashSet::new();

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
            match read_siblings(
                store,
                &state.tenant_hash,
                signal,
                &process_id,
                now_ns,
                two_r_ns,
            )
            .await
            {
                Ok(read) => {
                    stats.stale_keys_skipped =
                        stats.stale_keys_skipped.saturating_add(read.skipped_stale);
                    stats.keys_reaped = stats
                        .keys_reaped
                        .saturating_add(reap_keys(store, &read.reapable).await);
                    for sib in read.siblings {
                        if is_stale(now_ns, sib.snapshot_unix_ns, two_r_ns) {
                            continue;
                        }
                        sibling_series = sibling_series.saturating_add(sib.active_series_count);
                        sibling_streams = sibling_streams.saturating_add(sib.active_streams_count);
                        cycle_siblings.insert(sib.process_id.clone());
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

    stats.siblings_observed = cycle_siblings.len() as u64;
    // Measured on the controller's injected clock, the same one `now_ns` was
    // drawn from, so a test drives it deterministically and no library logic
    // reads wall time of its own.
    stats.cycle_duration_ns = controller.now_ns().saturating_sub(now_ns).max(0);
    controller.record_reconcile_cycle(stats);
    stats
}

/// Delete every key past the reap horizon, best effort (issue #1679). A delete
/// that fails is logged and retried by whichever process next lists the prefix;
/// it is never a reconciliation read failure, because the fleet picture this
/// cycle computed is complete either way. `delete` is idempotent, so several
/// processes reaping the same dead key concurrently is not a race.
async fn reap_keys(store: &dyn ObjectStoreBackend, keys: &[String]) -> u64 {
    let mut reaped = 0;
    for key in keys {
        match store.delete(key).await {
            Ok(()) => reaped += 1,
            Err(err) => tracing::warn!(
                key = %key,
                error = %err,
                "admission reconciliation: reaping a dead process's snapshot failed; \
                 retried next cycle"
            ),
        }
    }
    reaped
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
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

    /// A clock that advances by `step` on every read, so the duration a cycle
    /// measures between two reads is a deterministic function of the reads it
    /// makes rather than of how long the test machine took.
    struct SteppingClock {
        next: AtomicI64,
        step: i64,
    }
    impl SteppingClock {
        fn new(start_ns: i64, step: i64) -> Arc<Self> {
            Arc::new(SteppingClock {
                next: AtomicI64::new(start_ns),
                step,
            })
        }
    }
    impl Clock for SteppingClock {
        fn now_ns(&self) -> i64 {
            self.next.fetch_add(self.step, Ordering::SeqCst)
        }
    }

    /// Write one sibling snapshot whose *store* modification time is
    /// `mtime_ms`, which is what the LIST result carries and therefore what the
    /// read-side staleness skip and the reaper judge against. The oracle's
    /// clock is left at `restore_ms` so later writes (this process's own
    /// snapshot, above all) land with a current modification time.
    async fn seed_snapshot(
        store: &FaultStore<MemoryStore>,
        key: &str,
        mtime_ms: u64,
        restore_ms: u64,
        snapshot_unix_ns: i64,
        active_series_count: u64,
    ) {
        store.inner().set_clock_ms(mtime_ms);
        let snapshot = AdmissionUsageSnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            active_series_count,
            active_streams_count: 0,
            byte_rate_consumed_since_last_snapshot: 0,
            series_creation_consumed_since_last_snapshot: 0,
            snapshot_unix_ns,
        };
        store
            .put(
                key,
                snapshot.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed sibling snapshot");
        store.inner().set_clock_ms(restore_ms);
    }

    /// The keys currently under a (tenant, signal)'s admission prefix, sorted.
    async fn keys_under(
        store: &FaultStore<MemoryStore>,
        tenant_hash: &TenantHash,
        signal: Signal,
    ) -> Vec<String> {
        let mut keys: Vec<String> = list_all(store, &admission_prefix(tenant_hash, signal))
            .await
            .expect("list admission prefix")
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        keys.sort();
        keys
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

    /// Issue #1679's own case: a prefix filled with the snapshots of processes
    /// that have long since exited costs one LIST and a GET per LIVE sibling,
    /// not a GET per key, and the two live siblings are still both counted.
    ///
    /// The GET budget is asserted through the fault plan rather than by reading
    /// a counter: the 4th GET under the admission prefix is scripted to fail,
    /// so more than three of them shows up as a fired fault (and, since a
    /// failed read keeps the previous threshold, as the wrong cap). Against the
    /// pre-fix `read_siblings` the cycle issues 502 GETs and both assertions
    /// fail.
    async fn stale_prefix_case(memory: MemoryStore) {
        const STALE_SIBLINGS: usize = 500;
        let tenant = TenantId::new("acme");
        let tenant_hash = tenant.hash();
        let now_ns = 1_000 * R_NS;
        let now_ms = (now_ns / 1_000_000) as u64;
        // Every dead process's key is 10 * R old: past the 2 * R staleness
        // window and past the 4 * R reap horizon.
        let dead_ms = now_ms - 10 * (R_NS / 1_000_000) as u64;

        let clock = TestClock::new(now_ns);
        let controller = AdmissionController::new(clock.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        for i in 0..10u8 {
            controller.admit_series(&tenant, [series(i)], clock.now_ns());
        }

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("a 4th admission GET in one cycle".into()),
            )
            .with_key_contains("/admission/")
            .with_occurrence(Occurrence::Nth(4)),
        );
        let store = Arc::new(FaultStore::new(memory, plan));
        store.inner().set_clock_ms(now_ms);

        for i in 0..STALE_SIBLINGS {
            seed_snapshot(
                store.as_ref(),
                &snapshot_key(&tenant_hash, Signal::Metrics, &format!("dead-{i:04}")),
                dead_ms,
                now_ms,
                now_ns - 10 * R_NS,
                90,
            )
            .await;
        }
        let fresh_a = snapshot_key(&tenant_hash, Signal::Metrics, "live-a");
        let fresh_b = snapshot_key(&tenant_hash, Signal::Metrics, "live-b");
        seed_snapshot(store.as_ref(), &fresh_a, now_ms, now_ms, now_ns, 7).await;
        seed_snapshot(store.as_ref(), &fresh_b, now_ms, now_ms, now_ns, 13).await;

        let stats = reconcile_once(&controller, store.as_ref(), R, now_ns).await;

        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Transient),
            0,
            "the 4th GET under the admission prefix never happened: at most 3"
        );
        // own(10) + max(0, 100 - 10 - (7 + 13)) = 80: both live siblings counted,
        // and no dead one. Either live sibling missing would read 87 or 93.
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(80)),
            "both fresh siblings count, and no stale one does"
        );
        assert_eq!(stats.siblings_observed, 2, "two live siblings");
        assert_eq!(
            stats.stale_keys_skipped, STALE_SIBLINGS as u64,
            "every dead process's key was judged from the LIST result alone"
        );
        assert_eq!(
            stats.keys_reaped, STALE_SIBLINGS as u64,
            "and every one past the reap horizon was deleted"
        );

        // The prefix now holds exactly the live fleet: this process and the two
        // fresh siblings, by exact key set.
        let own = snapshot_key(
            &tenant_hash,
            Signal::Metrics,
            &controller.process_id().to_string(),
        );
        let mut expected = vec![own, fresh_a, fresh_b];
        expected.sort();
        assert_eq!(
            keys_under(store.as_ref(), &tenant_hash, Signal::Metrics).await,
            expected,
            "the dead keys are gone and every live one survived"
        );
    }

    #[tokio::test]
    async fn a_prefix_of_dead_processes_costs_no_gets_and_is_reaped() {
        stale_prefix_case(MemoryStore::new()).await;
    }

    /// The same case over a listing that pages: a reaper (or a skip) that works
    /// on one page and not on two would pass the test above and still leave
    /// most of the prefix behind.
    #[tokio::test]
    async fn a_prefix_of_dead_processes_is_reaped_across_list_pages() {
        stale_prefix_case(MemoryStore::with_page_size(2)).await;
    }

    /// The reap horizon is the staleness window widened by
    /// [`REAP_WINDOW_FACTOR`], and that extra width is the clock-skew margin: a
    /// key inside it is already excluded from the fleet sum (and costs no GET)
    /// but is NOT deleted, so a live process whose key looks older than it is
    /// keeps its key.
    #[tokio::test]
    async fn a_key_inside_the_skew_margin_is_skipped_but_not_reaped() {
        let tenant = TenantId::new("acme");
        let tenant_hash = tenant.hash();
        let now_ns = 1_000 * R_NS;
        let now_ms = (now_ns / 1_000_000) as u64;
        let r_ms = (R_NS / 1_000_000) as u64;

        let clock = TestClock::new(now_ns);
        let controller = AdmissionController::new(clock.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        for i in 0..10u8 {
            controller.admit_series(&tenant, [series(i)], clock.now_ns());
        }

        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        store.inner().set_clock_ms(now_ms);

        // 3 * R old: past the 2 * R staleness window, inside the 4 * R horizon.
        let inside = snapshot_key(&tenant_hash, Signal::Metrics, "skewed");
        // 5 * R old: past the horizon.
        let past = snapshot_key(&tenant_hash, Signal::Metrics, "long-dead");
        seed_snapshot(
            store.as_ref(),
            &inside,
            now_ms - 3 * r_ms,
            now_ms,
            now_ns - 3 * R_NS,
            40,
        )
        .await;
        seed_snapshot(
            store.as_ref(),
            &past,
            now_ms - 5 * r_ms,
            now_ms,
            now_ns - 5 * R_NS,
            40,
        )
        .await;

        let stats = reconcile_once(&controller, store.as_ref(), R, now_ns).await;

        assert_eq!(stats.stale_keys_skipped, 2, "both keys are past 2 * R");
        assert_eq!(
            stats.keys_reaped, 1,
            "only the key past the reap horizon is deleted"
        );
        assert_eq!(stats.siblings_observed, 0, "neither counts as live");
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(100)),
            "a stale sibling contributes zero whether or not it was reaped"
        );

        let own = snapshot_key(
            &tenant_hash,
            Signal::Metrics,
            &controller.process_id().to_string(),
        );
        let mut expected = vec![own, inside];
        expected.sort();
        assert_eq!(
            keys_under(store.as_ref(), &tenant_hash, Signal::Metrics).await,
            expected,
            "the key inside the skew margin survives; the one past the horizon does not"
        );
    }

    /// Pins the module doc's same-clock caveat on the read-side skip: the skip
    /// judges `mtime_stale` off the object store's clock alone, while the
    /// authoritative [`is_stale`] check the skip stands in for judges off the
    /// writer's clock (`snapshot_unix_ns`). The two only agree when both
    /// clocks agree. Here the sibling's *store* modification time is stamped
    /// 3 * R old (past the 2 * R staleness window) while its body's
    /// `snapshot_unix_ns` is stamped fresh (0 old) -- a writer clock reading
    /// ahead of the store's -- so the skip fires and drops a sibling that
    /// `is_stale` alone would have counted as live.
    ///
    /// This is deliberately not a bug fix: the skip is exactly as safe as the
    /// pre-existing `is_stale` check it sits beside (a sibling this far
    /// out of clock agreement already looks dead to `is_stale` too), and it
    /// self-corrects the next interval. The point of this test is to hold the
    /// doc's caveat to a concrete, checkable case rather than an unverified
    /// claim.
    #[tokio::test]
    async fn a_skip_can_drop_a_fresh_sibling_when_the_writer_clock_leads_the_store_clock() {
        let tenant = TenantId::new("acme");
        let tenant_hash = tenant.hash();
        let now_ns = 1_000 * R_NS;
        let now_ms = (now_ns / 1_000_000) as u64;
        let r_ms = (R_NS / 1_000_000) as u64;

        let clock = TestClock::new(now_ns);
        let controller = AdmissionController::new(clock.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        for i in 0..10u8 {
            controller.admit_series(&tenant, [series(i)], clock.now_ns());
        }

        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        store.inner().set_clock_ms(now_ms);

        // Store mtime 3 * R old (past the 2 * R window); body stamped fresh
        // (`snapshot_unix_ns == now_ns`), as a writer clock ahead of the
        // store's would produce.
        let skewed = snapshot_key(&tenant_hash, Signal::Metrics, "skewed-writer");
        seed_snapshot(
            store.as_ref(),
            &skewed,
            now_ms - 3 * r_ms,
            now_ms,
            now_ns,
            40,
        )
        .await;

        let stats = reconcile_once(&controller, store.as_ref(), R, now_ns).await;

        assert_eq!(
            stats.stale_keys_skipped, 1,
            "the mtime-only skip fires on the store clock's stamp alone"
        );
        assert_eq!(
            stats.siblings_observed, 0,
            "the fresh body is never read, so it is never counted live"
        );
        assert_eq!(
            controller.effective_max_active_series(&tenant),
            Some(CountLimit::Bounded(100)),
            "the skipped sibling's 40 series never entered fleet_used, so the \
             local threshold is not tightened for it -- the under-count this \
             test exists to pin"
        );
    }

    /// The read-side skip judges a listed key from the modification time the
    /// LIST result carries, and a backend that reports none (`<= 0`, which is
    /// the in-memory oracle's default) is read exactly as before rather than
    /// read as ancient. Every other reconciliation test in this module relies
    /// on that: they seed siblings without ever setting the oracle's clock.
    #[test]
    fn a_missing_modification_time_is_unknown_never_ancient() {
        let now = 1_000 * R_NS;
        let two_r = 2 * R_NS;
        let now_ms = now / 1_000_000;
        let r_ms = R_NS / 1_000_000;

        assert!(
            !mtime_stale(now, 0, two_r),
            "no modification time means read the key, not skip it"
        );
        assert!(!mtime_stale(now, -1, two_r), "nor does a negative one");
        assert!(
            !mtime_stale(now, now_ms - 2 * r_ms, two_r),
            "exactly 2 * R old is still fresh, exactly as is_stale judges it"
        );
        assert!(
            mtime_stale(now, now_ms - 2 * r_ms - 1, two_r),
            "one millisecond past 2 * R is stale"
        );
        // The reap horizon is the same window, widened by the factor: nothing
        // is reaped that the staleness window would not already exclude.
        assert_eq!(reap_horizon_ns(two_r), 4 * R_NS);
        assert!(!mtime_stale(now, now_ms - 4 * r_ms, reap_horizon_ns(two_r)));
        assert!(mtime_stale(
            now,
            now_ms - 4 * r_ms - 1,
            reap_horizon_ns(two_r)
        ));
    }

    /// The cycle duration is measured on the controller's injected clock: a
    /// clock that does not move reports exactly zero, and one that advances by
    /// a known step reports exactly that step (the cycle takes one end stamp).
    /// No wall-clock reading enters the figure.
    #[tokio::test]
    async fn cycle_duration_is_measured_on_the_injected_clock() {
        let tenant = TenantId::new("solo");
        let now_ns = 500 * R_NS;
        let store = Arc::new(MemoryStore::new());

        let fixed = TestClock::new(now_ns);
        let controller = AdmissionController::new(fixed.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        controller.admit_series(&tenant, [series(1)], now_ns);
        let stats = reconcile_once(&controller, store.as_ref(), R, now_ns).await;
        assert_eq!(
            stats.cycle_duration_ns, 0,
            "a clock that did not move measures a zero-length cycle"
        );
        assert_eq!(stats.siblings_observed, 0, "a lone process has no siblings");

        const STEP_NS: i64 = 250_000;
        let stepping = SteppingClock::new(now_ns, STEP_NS);
        let controller = AdmissionController::new(stepping.clone(), count_only_limits(100));
        controller.set_tenant_limits(tenant.clone(), count_only_limits(100));
        controller.admit_series(&tenant, [series(1)], now_ns);
        let start_ns = stepping.now_ns();
        let stats = reconcile_once(&controller, store.as_ref(), R, start_ns).await;
        assert_eq!(
            stats.cycle_duration_ns, STEP_NS,
            "the cycle's end stamp comes from the same injected clock as its start"
        );
        assert_eq!(
            controller.last_reconcile_cycle_stats(),
            stats,
            "the cycle publishes the same figures it returned, for the exporter to read"
        );
    }
}
