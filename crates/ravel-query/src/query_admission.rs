//! Fleet-global query concurrency ceiling (ADR-0061 decision 2).
//!
//! A single fleet-wide cap on the number of queries in flight at once, reusing
//! ADR-0057's count-cap reconciliation *pattern* (not its code:
//! `ravel_ingest::admission::AdmissionController` is ingest-scoped state with no
//! query-path caller, so this is an independent controller that only mirrors the
//! shape). Concurrent query count is a **stock** (how many queries are open right
//! now), the same shape as ADR-0057's `active_series`, so this reuses the
//! additive-headroom formula ADR-0057 shipped correct the first time, not the
//! equal-share rate formula.
//!
//! ## The decision, not a cancellation
//!
//! Unlike decision 1's per-tenant bytes-scanned budget (which cancels a query
//! mid-flight once it has scanned too much), this is an **admission** decision
//! made *before* a query is allowed to start: before any snapshot resolve, let
//! alone any object GET. A query that would push this process past its local
//! threshold is rejected outright; an admitted query holds a slot for the
//! lifetime of the work that slot covers and releases it on completion, error,
//! or client disconnect. The slot is a [`QueryPermit`] whose [`Drop`] releases
//! it, so no code path can forget to decrement the stock.
//!
//! On the single-shot HTTP and PromQL surfaces one permit covers a query end to
//! end. Flight SQL splits one query across two RPCs, so it admits at two points:
//! `get_flight_info_statement` holds a permit for resolve + logical plan
//! (released when that RPC returns), and `do_get_statement` takes a fresh permit
//! held for the whole streaming execution phase (released when the result stream
//! ends or the client abandons it). `get_flight_info`'s permit is already gone
//! by the time the ticket is redeemed, so the `do_get_statement` gate is the one
//! that actually bounds Flight's scan phase. The two RPCs are separate points in
//! time, so one Flight query never holds two slots at once. See
//! `ravel_sql::flight`.
//!
//! ## Fleet reconciliation (ADR-0057 pattern)
//!
//! Each process writes its own current in-flight count to a key it alone ever
//! writes, reads every sibling's non-stale snapshot, and recomputes a local
//! threshold from the reconciled fleet-wide total, exactly as ADR-0057's count
//! caps do. The keyspace is new and deliberately **not** tenant-scoped, because
//! this ceiling is fleet-global, not per-tenant (ADR-0061 decision 2):
//!
//! ```text
//! admission/query/<process_id>.snapshot
//! ```
//!
//! at the bucket root, rather than under a `t/<tenant_hash>/` prefix like every
//! other admission/catalog key. See docs/catalog-and-mvcc.md.
//!
//! Written with [`PutMode::Overwrite`] (single writer per key, no CAS, a failed
//! or stale write self-corrects next interval). The body is a small JSON object
//! rather than a protobuf message: this keyspace is owned entirely by this
//! module, single-writer per key and ephemeral (self-correcting every interval),
//! so its body is an internal contract of this module, not a cross-crate frozen
//! format that would need a proto schema and version bump.
//!
//! ## Staleness (ADR-0057 section 3, the `2R` window)
//!
//! A sibling whose snapshot is older than `2 * R` at read time is excluded and
//! treated as contributing **zero** — a silent or dead process is assumed not to
//! be holding any slots, so one slow write never freezes another process's
//! admission. Within the `2R` window a sibling is trusted at its last-known
//! count. If this process's own reconciliation read (the LIST or a GET) fails,
//! it keeps its last-computed threshold rather than failing closed to zero
//! admission, and increments a failure counter so a sustained fault is
//! observable.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, list_all};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// The query concurrency ceiling has no default-interval constant of its own by
// design: the server drives this reconciliation loop off the same
// `--admission-reconcile-interval` knob (defaulting to
// `ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL`) it drives the ingest one
// with, so there is one cadence to reason about, not two. A second 10s constant
// here would be dead weight that could silently drift from the value actually in
// force.

/// Format floor for the snapshot body. A sibling advertising a higher version is
/// one this reader does not understand, so it is skipped (treated as absent)
/// rather than misread, mirroring every `sys/`-message guard in ADR-0057.
const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// The bucket-root prefix every process's query-admission snapshot lives under
/// (ADR-0061 decision 2). Explicitly not under a `t/<tenant_hash>/` prefix: this
/// resource is fleet-global, with no tenant to scope it under.
const QUERY_ADMISSION_PREFIX: &str = "admission/query/";

/// The fleet-global cap on concurrently in-flight queries per process (before
/// reconciliation) / per fleet (after), or an explicit opt-in to no cap at all.
///
/// Mirrors [`crate::ByteLimit`] and `ravel_ingest::admission::CountLimit`'s shape
/// deliberately: the same enum operators already learned for the byte budget and
/// for ingest admission, applied to a concurrency stock. [`Self::Unlimited`] is
/// the default so a deployment that sets no ceiling behaves exactly as before
/// this mechanism existed — a bounded default would silently start rejecting an
/// existing deployment's legitimate query fan-out on upgrade with no config
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryConcurrencyLimit {
    Bounded(u64),
    #[default]
    Unlimited,
}

/// One process's query-admission snapshot body. `process_id` is not carried in
/// the body — it comes from the object key, exactly as ADR-0057's sibling reader
/// takes it from the key rather than the message.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct QueryConcurrencySnapshot {
    format_version: u32,
    /// The number of queries this process had in flight when it wrote the
    /// snapshot. A stock, summed across non-stale siblings by the reader.
    in_flight: u64,
    /// Wall-clock nanoseconds at write time, used for the `2R` staleness check.
    snapshot_unix_ns: i64,
}

/// The snapshot key one process owns (ADR-0061 decision 2). At the bucket root,
/// keyed only by `process_id`; no tenant dimension.
pub fn query_admission_snapshot_key(process_id: &str) -> String {
    format!("{QUERY_ADMISSION_PREFIX}{process_id}.snapshot")
}

/// Extract the `<process_id>` from a snapshot key
/// (`admission/query/<process_id>.snapshot`). Returns `None` for a key that does
/// not match the shape; every key this module writes matches.
fn process_id_of(key: &str) -> Option<&str> {
    key.strip_prefix(QUERY_ADMISSION_PREFIX)?
        .strip_suffix(".snapshot")
}

/// Whether a sibling snapshot is stale at read time (ADR-0057 section 3): older
/// than `2 * R`. Exactly `2 * R` old is still fresh (boundary inclusive on the
/// fresh side); a future-dated snapshot (negative age from clock skew) is fresh.
fn is_stale(now_ns: i64, snapshot_unix_ns: i64, two_r_ns: i64) -> bool {
    now_ns.saturating_sub(snapshot_unix_ns) > two_r_ns
}

/// The local soft threshold for the concurrency stock (ADR-0057 section 2's
/// additive-headroom formula): `own + max(0, cap - (own + siblings))`, saturating
/// throughout. A fleet already at or over its cap yields exactly `own` (this
/// process admits no more but keeps the queries it already has), never underflow.
///
/// Correct because the stock only grows through admissions this same threshold
/// gates: once the fleet is at cap the threshold collapses to `own`, a stable
/// fixed point. Sibling counts SUM rather than dedupe (there is nothing to
/// dedupe here — each process's in-flight queries are genuinely distinct), an
/// upper bound that can only reject sooner than the letter of the cap, never
/// admit past it.
fn local_soft_threshold(own: u64, siblings_sum: u64, configured_cap: u64) -> u64 {
    let fleet_used = own.saturating_add(siblings_sum);
    let remaining = configured_cap.saturating_sub(fleet_used);
    own.saturating_add(remaining)
}

#[derive(Debug)]
struct State {
    /// Queries in flight in this process right now (the stock).
    in_flight: u64,
    /// The effective fleet-reconciled ceiling the hot path compares `in_flight`
    /// against. Starts equal to the configured cap, so before the first
    /// reconciliation cycle a lone process enforces exactly the configured cap.
    /// A successful reconciliation overwrites it with `local_soft_threshold`; a
    /// failed one leaves it at its last-known value (never fails closed to zero,
    /// ADR-0057 section 3). Meaningless (and unread) when the limit is
    /// [`QueryConcurrencyLimit::Unlimited`].
    fleet_threshold: u64,
    /// Reconciliation read failures (ADR-0057 section 3): a LIST or GET of the
    /// sibling snapshots failed, so the last-computed threshold stayed in force.
    reconciliation_failures_total: u64,
}

/// Fleet-global query concurrency admission controller (ADR-0061 decision 2).
///
/// Tracks this process's concurrently-open query count as a stock and answers
/// ADMIT/REJECT before a query starts. Shared (via `Arc`) across every query
/// surface in the process — the PromQL HTTP handlers, the SQL HTTP endpoint,
/// and Flight SQL's `GetFlightInfo` (planning) and `DoGet` (execution) paths,
/// each held for its own phase's duration — so the process holds one honest
/// count across all transports, not one per transport.
///
/// A single [`Mutex`] guards the small counter state; every operation is O(1)
/// arithmetic with no `.await` under the lock, so there is no contention concern
/// at query rates.
pub struct QueryAdmissionController {
    limit: QueryConcurrencyLimit,
    /// Stable, fleet-unique id naming the one snapshot key this process owns and
    /// alone writes (ADR-0057 section 1). Generated once at construction.
    process_id: Uuid,
    state: Mutex<State>,
}

impl QueryAdmissionController {
    /// A controller enforcing `limit`. The effective threshold starts at the
    /// configured cap, so a lone process (no reconciliation yet) behaves exactly
    /// as a per-process cap of that value.
    pub fn new(limit: QueryConcurrencyLimit) -> Self {
        let fleet_threshold = match limit {
            QueryConcurrencyLimit::Bounded(cap) => cap,
            QueryConcurrencyLimit::Unlimited => u64::MAX,
        };
        QueryAdmissionController {
            limit,
            process_id: Uuid::new_v4(),
            state: Mutex::new(State {
                in_flight: 0,
                fleet_threshold,
                reconciliation_failures_total: 0,
            }),
        }
    }

    /// Convenience for the common case: an `Arc` around [`Self::new`], since the
    /// controller is always shared and [`Self::try_admit`] needs an `Arc`.
    pub fn shared(limit: QueryConcurrencyLimit) -> Arc<Self> {
        Arc::new(Self::new(limit))
    }

    /// This process's stable snapshot-key owner id (ADR-0057 section 1). Names
    /// its own key and excludes it from the sibling sum.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    /// Decide admission for one query and, on ADMIT, reserve a slot.
    ///
    /// Returns a [`QueryPermit`] that holds the slot until it is dropped — on
    /// normal completion, on an early `?` return, on a panic unwind, or on a
    /// dropped request future (client disconnect). Returns [`QueryRejected`] when
    /// admitting one more would exceed this process's effective fleet threshold;
    /// the caller must reject the query before doing any resolve or GET.
    ///
    /// [`QueryConcurrencyLimit::Unlimited`] always admits, but still tracks the
    /// stock: [`Self::in_flight`] reports a truthful live count under any config,
    /// so an observer that reads it (a test today; a `/metrics` gauge if one is
    /// later wired) sees the real number, not a zero the unlimited branch skipped.
    /// Reconciliation, by contrast, is a genuine no-op under `Unlimited`
    /// ([`reconcile_query_admission_once`] returns early with no cap to enforce),
    /// so under that config it neither reads nor writes any snapshot.
    pub fn try_admit(self: &Arc<Self>) -> Result<QueryPermit, QueryRejected> {
        let mut state = lock(&self.state);
        let admit = match self.limit {
            QueryConcurrencyLimit::Unlimited => true,
            QueryConcurrencyLimit::Bounded(_) => state.in_flight < state.fleet_threshold,
        };
        if !admit {
            return Err(QueryRejected);
        }
        state.in_flight = state.in_flight.saturating_add(1);
        Ok(QueryPermit {
            controller: Arc::clone(self),
        })
    }

    /// Release one held slot. Called only by [`QueryPermit`]'s [`Drop`]; not a
    /// public method, so a slot can never be released without a permit having
    /// held it.
    fn release(&self) {
        let mut state = lock(&self.state);
        state.in_flight = state.in_flight.saturating_sub(1);
    }

    /// Queries currently in flight in this process.
    pub fn in_flight(&self) -> u64 {
        lock(&self.state).in_flight
    }

    /// Overwrite the effective fleet threshold after a successful reconciliation.
    fn set_fleet_threshold(&self, threshold: u64) {
        lock(&self.state).fleet_threshold = threshold;
    }

    /// Record a reconciliation read failure (ADR-0057 section 3).
    fn record_reconciliation_failure(&self) {
        lock(&self.state).reconciliation_failures_total += 1;
    }

    /// The effective (fleet-reconciled) ceiling now in force. Test/observation
    /// hook; the hot path reads the same field.
    pub fn effective_threshold(&self) -> u64 {
        lock(&self.state).fleet_threshold
    }

    /// Reconciliation read failures observed so far (ADR-0057 section 3).
    pub fn reconciliation_failures_total(&self) -> u64 {
        lock(&self.state).reconciliation_failures_total
    }
}

/// An admitted query's held concurrency slot. Dropping it releases the slot back
/// to the controller — the single decrement path, so every admission (including
/// one that later errors, unwinds, or is cancelled by a client disconnect)
/// decrements exactly once.
pub struct QueryPermit {
    controller: Arc<QueryAdmissionController>,
}

impl Drop for QueryPermit {
    fn drop(&mut self) {
        self.controller.release();
    }
}

/// A query refused by the concurrency ceiling before it started (ADR-0061
/// decision 2). The caller maps this to a retryable rejection (HTTP 503 /
/// gRPC `RESOURCE_EXHAUSTED`); no resolve or GET has run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("fleet query concurrency ceiling reached")]
pub struct QueryRejected;

/// Read and decode every sibling snapshot under the query-admission prefix,
/// excluding this process's own key. A corrupt or future-version sibling is
/// skipped (treated as absent, self-correcting next interval), not an error:
/// only a failed LIST or GET is an [`Err`] the caller counts as a reconciliation
/// failure (ADR-0057 section 3). Returns the summed in-flight count of every
/// non-stale sibling.
async fn read_sibling_sum(
    store: &dyn ObjectStoreBackend,
    own_process_id: &str,
    now_ns: i64,
    two_r_ns: i64,
) -> Result<u64, StoreError> {
    let objects = list_all(store, QUERY_ADMISSION_PREFIX).await?;
    let mut sum: u64 = 0;
    for meta in objects {
        let Some(pid) = process_id_of(&meta.key) else {
            continue;
        };
        if pid == own_process_id {
            continue;
        }
        let got = store.get(&meta.key, GetRange::Full).await?;
        let Ok(snap) = serde_json::from_slice::<QueryConcurrencySnapshot>(got.data.as_ref()) else {
            tracing::debug!(
                key = %meta.key,
                "query admission reconciliation: skipping an undecodable sibling snapshot"
            );
            continue;
        };
        if snap.format_version > SNAPSHOT_FORMAT_VERSION {
            tracing::debug!(
                key = %meta.key,
                version = snap.format_version,
                "query admission reconciliation: skipping a future-version sibling snapshot"
            );
            continue;
        }
        if is_stale(now_ns, snap.snapshot_unix_ns, two_r_ns) {
            continue;
        }
        sum = sum.saturating_add(snap.in_flight);
    }
    Ok(sum)
}

/// Run exactly one fleet reconciliation cycle for the query concurrency ceiling
/// (ADR-0057 sections 1-3, ADR-0061 decision 2). The server wraps this in a
/// periodic loop with clean shutdown, so the interval/lifecycle lives there and
/// the cycle itself stays unit-testable — the same split the ingest
/// reconciliation uses.
///
/// Never touches the hot path: the controller's mutex is taken only for the
/// brief lock-free-I/O hooks (`in_flight`, `set_fleet_threshold`), and every
/// object-store LIST/GET/PUT happens between them with no lock held.
///
/// A no-op under [`QueryConcurrencyLimit::Unlimited`]: with no cap to enforce
/// there is nothing to write or read, so an unlimited deployment pays no
/// reconciliation I/O at all.
pub async fn reconcile_query_admission_once(
    controller: &QueryAdmissionController,
    store: &dyn ObjectStoreBackend,
    interval: Duration,
    now_ns: i64,
) {
    let cap = match controller.limit {
        QueryConcurrencyLimit::Unlimited => return,
        QueryConcurrencyLimit::Bounded(cap) => cap,
    };
    let r_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
    let two_r_ns = r_ns.saturating_mul(2);
    let process_id = controller.process_id().to_string();
    let own = controller.in_flight();

    // 1. Write this process's own snapshot (Overwrite: single writer, no CAS). A
    //    write failure is logged and self-corrects next interval; it is NOT the
    //    counted read failure.
    let snapshot = QueryConcurrencySnapshot {
        format_version: SNAPSHOT_FORMAT_VERSION,
        in_flight: own,
        snapshot_unix_ns: now_ns,
    };
    let key = query_admission_snapshot_key(&process_id);
    match serde_json::to_vec(&snapshot) {
        Ok(body) => {
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
                    "query admission reconciliation: snapshot write failed; self-corrects next \
                     interval"
                );
            }
        }
        Err(err) => {
            // A plain struct of integers cannot fail to serialize in practice;
            // handle it without unwrap rather than panic on a critical path.
            tracing::warn!(
                error = %err,
                "query admission reconciliation: snapshot encode failed; skipping this cycle's \
                 write"
            );
        }
    }

    // 2. Read every sibling snapshot and recompute the local threshold. A failed
    //    read keeps the last-computed threshold (never fails closed to zero) and
    //    increments the failure counter (ADR-0057 section 3).
    match read_sibling_sum(store, &process_id, now_ns, two_r_ns).await {
        Ok(sibling_sum) => {
            controller.set_fleet_threshold(local_soft_threshold(own, sibling_sum, cap));
        }
        Err(err) => {
            controller.record_reconciliation_failure();
            tracing::warn!(
                error = %err,
                "query admission reconciliation: sibling read failed; keeping last-known \
                 threshold (never fails closed to zero, ADR-0057 section 3)"
            );
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // Query-admission bookkeeping is in-memory and per-process; a panic while
    // holding this lock must not turn into a permanent admission outage, so a
    // poisoned lock is recovered rather than propagated (same discipline as
    // ravel_ingest::admission).
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const R: Duration = Duration::from_secs(10);
    const R_NS: i64 = 10 * 1_000_000_000;

    async fn put_sibling(
        store: &MemoryStore,
        process_id: &str,
        in_flight: u64,
        snapshot_unix_ns: i64,
    ) {
        let snap = QueryConcurrencySnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            in_flight,
            snapshot_unix_ns,
        };
        let body = serde_json::to_vec(&snap).expect("serialize sibling snapshot");
        store
            .put(
                &query_admission_snapshot_key(process_id),
                body.into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed sibling snapshot");
    }

    #[test]
    fn admits_under_the_ceiling_and_rejects_at_it() {
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(2));
        let p1 = controller.try_admit().expect("first admits");
        let p2 = controller.try_admit().expect("second admits");
        assert_eq!(controller.in_flight(), 2);
        // At the ceiling: the third is rejected before it starts.
        assert!(controller.try_admit().is_err(), "third must be rejected");
        assert_eq!(
            controller.in_flight(),
            2,
            "a rejected query reserves no slot"
        );
        drop(p1);
        drop(p2);
    }

    #[test]
    fn permit_drop_decrements_and_frees_a_slot() {
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(1));
        {
            let _p = controller.try_admit().expect("admits");
            assert_eq!(controller.in_flight(), 1);
            assert!(controller.try_admit().is_err(), "at ceiling while held");
        }
        // The permit dropped at the end of the block, freeing the one slot.
        assert_eq!(controller.in_flight(), 0);
        let _p = controller
            .try_admit()
            .expect("a freed slot admits the next query");
        assert_eq!(controller.in_flight(), 1);
    }

    #[test]
    fn error_path_still_decrements_via_the_guard() {
        // Simulate a query that acquires a permit then fails: the `?`-style early
        // return drops the permit on the way out, so the slot is released even
        // though the query never completed normally.
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(1));

        fn admitted_then_errors(
            controller: &Arc<QueryAdmissionController>,
        ) -> Result<(), &'static str> {
            let _permit = controller.try_admit().map_err(|_| "rejected")?;
            // The permit is live here; returning an error drops it.
            Err("simulated mid-query failure")
        }

        assert!(admitted_then_errors(&controller).is_err());
        assert_eq!(
            controller.in_flight(),
            0,
            "the guard released the slot on the error path"
        );
        // The freed slot is immediately reusable.
        assert!(controller.try_admit().is_ok());
    }

    #[test]
    fn unlimited_never_rejects_but_still_tracks_the_stock() {
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited);
        let mut permits = Vec::new();
        for _ in 0..1000 {
            permits.push(controller.try_admit().expect("unlimited always admits"));
        }
        assert_eq!(controller.in_flight(), 1000);
        permits.clear();
        assert_eq!(controller.in_flight(), 0);
    }

    #[tokio::test]
    async fn lone_process_reconciles_to_the_configured_cap() {
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(5));
        let _p = controller.try_admit().expect("admits");
        let store = MemoryStore::new();
        reconcile_query_admission_once(&controller, &store, R, 1_000 * R_NS).await;
        assert_eq!(
            controller.effective_threshold(),
            5,
            "with no siblings the effective threshold equals the configured cap"
        );
    }

    #[tokio::test]
    async fn a_fresh_sibling_reduces_this_process_headroom() {
        // Fleet cap 10; this process holds 2, a fresh sibling holds 5, so
        // headroom = 10 - (2 + 5) = 3 and the threshold pins to own + 3 = 5.
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(10));
        let _p1 = controller.try_admit().expect("admits");
        let _p2 = controller.try_admit().expect("admits");
        let store = MemoryStore::new();
        put_sibling(&store, "sibling-a", 5, 1_000 * R_NS).await;

        reconcile_query_admission_once(&controller, &store, R, 1_000 * R_NS).await;
        assert_eq!(
            controller.effective_threshold(),
            5,
            "own(2) + max(0, 10 - 2 - 5) = 5"
        );
    }

    #[tokio::test]
    async fn a_stale_sibling_is_treated_as_zero_within_the_2r_window() {
        // ADR-0057 section 3: a sibling older than 2*R is excluded and contributes
        // zero, so its slots do not silently freeze this process out; a sibling
        // inside the window is trusted at its last-known count.
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(10));
        let _p = controller.try_admit().expect("admits");
        let store = MemoryStore::new();

        let now = 1_000 * R_NS;
        // A busy sibling holding 8 slots, written exactly 3*R ago: stale (> 2R).
        put_sibling(&store, "dead-process", 8, now - 3 * R_NS).await;
        reconcile_query_admission_once(&controller, &store, R, now).await;
        assert_eq!(
            controller.effective_threshold(),
            10,
            "a stale sibling contributes zero, so the full cap is available: own(1) + (10 - 1)"
        );

        // The same sibling, now refreshed to a snapshot inside the 2R window, is
        // trusted at its count and drags the threshold down.
        put_sibling(&store, "dead-process", 8, now).await;
        reconcile_query_admission_once(&controller, &store, R, now).await;
        assert_eq!(
            controller.effective_threshold(),
            2,
            "a fresh sibling holding 8 leaves headroom 1: own(1) + max(0, 10 - 1 - 8) = 2"
        );

        // Exactly 2*R old is still fresh (boundary inclusive on the fresh side).
        assert!(!is_stale(now, now - 2 * R_NS, 2 * R_NS));
        assert!(is_stale(now, now - 2 * R_NS - 1, 2 * R_NS));
    }

    #[tokio::test]
    async fn a_failed_sibling_read_keeps_the_last_threshold_and_counts() {
        use ravel_object_store::fault::{
            FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };

        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(10));
        let _p = controller.try_admit().expect("admits");

        // The SECOND LIST under the query-admission prefix faults; the first
        // passes, so a clean cycle installs a threshold and the second exercises
        // the failure path.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("admission list down".into()),
            )
            .with_key_contains("admission/query/")
            .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);

        // A busy sibling sits in the store (PUT is not faulted), so a clean cycle
        // drives the effective threshold below the configured 10.
        put_sibling_faultstore(&store, "sibling-a", 6, 1_000 * R_NS).await;

        // Clean cycle: threshold = own(1) + max(0, 10 - 1 - 6) = 4.
        reconcile_query_admission_once(&controller, &store, R, 1_000 * R_NS).await;
        assert_eq!(controller.effective_threshold(), 4);
        assert_eq!(controller.reconciliation_failures_total(), 0);

        // Faulting cycle: the read cannot complete, so the last-known threshold
        // (4) holds — not reset to 10, not failed closed to 0 — and the counter
        // increments.
        reconcile_query_admission_once(&controller, &store, R, 1_000 * R_NS).await;
        assert_eq!(
            controller.effective_threshold(),
            4,
            "a failed read keeps the last-computed threshold"
        );
        assert_eq!(controller.reconciliation_failures_total(), 1);
    }

    async fn put_sibling_faultstore(
        store: &ravel_object_store::fault::FaultStore<MemoryStore>,
        process_id: &str,
        in_flight: u64,
        snapshot_unix_ns: i64,
    ) {
        let snap = QueryConcurrencySnapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            in_flight,
            snapshot_unix_ns,
        };
        let body = serde_json::to_vec(&snap).expect("serialize sibling snapshot");
        store
            .put(
                &query_admission_snapshot_key(process_id),
                body.into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("seed sibling snapshot");
    }

    #[test]
    fn formula_matches_additive_headroom_and_saturates() {
        assert_eq!(local_soft_threshold(2, 5, 10), 5);
        assert_eq!(local_soft_threshold(1, 0, 10), 10);
        // Fleet already over cap: pins to own, never underflows.
        assert_eq!(local_soft_threshold(6, 6, 10), 6);
        assert_eq!(local_soft_threshold(20, 5, 10), 20);
    }
}
