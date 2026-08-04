//! Background store-reachability probe with hysteresis (ADR-0050 section 7,
//! EC7).
//!
//! `/readyz` must reflect store reachability without doing a store call on
//! every kubelet probe (the two objections health.rs documents: kubelet-
//! frequency S3 cost, and single-blip mass ejection). This module is the
//! resolution ADR-0050 section 7 decides: one background task per process does
//! a small GET of the fixed `sys/tenancy` object every `--store-probe-interval`
//! (jittered), and readiness reads a cheap atomic the task maintains.
//!
//! # Asymmetric hysteresis
//!
//! Reachability flips with deliberate asymmetry (ADR-0050 section 7: "After K
//! consecutive failures ... the first success flips it back"): [`K`] consecutive
//! failures are required to go unhealthy, but a single success recovers
//! immediately. That is the two-minutes-of-hysteresis-at-defaults the ADR wants
//! before marking a fleet unready (a genuine outage that long means every data
//! path is failing and unreadiness is the truthful signal), while recovery is
//! instant so a fleet does not stay ejected one interval longer than the outage.
//!
//! # Why process-global atomics
//!
//! The reachability flag and the failure counter are process-global atomics,
//! not an instance handle, matching the established pattern for the other
//! single-source, no-label `/metrics` counters this crate renders directly in
//! `metrics::render` ([`crate::tenancy::v1_unkeyed_adoption_count`],
//! [`crate::provisioning::shard_count_mismatch_count`]). There is one probe per
//! process by design, so one global reachability truth is exactly right:
//! [`crate::health::Readiness`] reads it, `/metrics` reads it, and the single
//! probe task writes it, with no Arc plumbing threaded through every
//! `ServerConfig` construction.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::tenancy::TENANCY_MARKER_KEY;

/// Consecutive-failure threshold before readiness flips to unhealthy (ADR-0050
/// section 7 default). Not a flag: the ADR gives a default and does not ask for
/// it to be configurable, and a fixed value keeps the hysteresis window a
/// documented, fleet-wide constant (roughly `K * --store-probe-interval` of
/// outage before ejection) rather than a per-process knob an operator could set
/// to 1 and reintroduce the single-blip mass-ejection failure mode this exists
/// to prevent.
pub const K: u32 = 4;

/// Default `--store-probe-interval`: 30 seconds (ADR-0050 section 7), matching
/// the humantime-duration flag convention already used for the `--gc-*` knobs.
pub const DEFAULT_STORE_PROBE_INTERVAL: Duration = Duration::from_secs(30);

/// Whether the store is currently reachable, as maintained by the probe task.
/// Starts `true`: before the first probe cycle completes, a process is assumed
/// reachable, so `/readyz` depends only on the startup latch until the probe
/// has actually observed the store. Rendered at `/metrics` as the
/// `ravel_store_reachable` gauge (1 = healthy, 0 = unhealthy).
static STORE_REACHABLE: AtomicBool = AtomicBool::new(true);

/// Total store-probe failures observed by this process, monotonic. Rendered at
/// `/metrics` as `ravel_store_probe_failures_total`. Every failed probe cycle
/// increments it, whether or not that failure was the one that crossed [`K`],
/// so an operator sees intermittent store trouble even when hysteresis keeps
/// readiness green.
static PROBE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Whether the store is currently reachable (the `ravel_store_reachable` gauge
/// source and one half of [`crate::health::Readiness::is_ready`]).
pub fn store_reachable() -> bool {
    STORE_REACHABLE.load(Ordering::Relaxed)
}

/// Total probe failures observed this process (the
/// `ravel_store_probe_failures_total` counter source).
pub fn probe_failures_total() -> u64 {
    PROBE_FAILURES_TOTAL.load(Ordering::Relaxed)
}

fn set_store_reachable(reachable: bool) {
    STORE_REACHABLE.store(reachable, Ordering::Relaxed);
}

/// Tracks consecutive probe failures and applies the ADR-0050 section 7
/// asymmetric hysteresis. Kept as a plain struct, independent of the store and
/// the background task, so the counting rule (K consecutive to go unhealthy, 1
/// success to recover) has a fast, focused unit test.
#[derive(Debug, Default)]
pub struct ProbeHysteresis {
    consecutive_failures: u32,
}

impl ProbeHysteresis {
    pub fn new() -> Self {
        ProbeHysteresis::default()
    }

    /// Record one probe outcome and return the resulting reachability. A
    /// success resets the streak and is reachable immediately (recovery is 1,
    /// not K). A failure increments the streak; reachability stays `true` until
    /// the streak reaches [`K`], then is `false` and stays `false` for every
    /// further failure. This return value IS the reachability the caller sets,
    /// so the flag never diverges from the counting rule.
    pub fn observe(&mut self, ok: bool) -> bool {
        if ok {
            self.consecutive_failures = 0;
            true
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            self.consecutive_failures < K
        }
    }
}

/// One probe: GET the fixed `sys/tenancy` object, classify the outcome, update
/// the failure counter and the reachability atomic through `hysteresis`, and
/// return the resulting reachability. A read, never a write: the probe must not
/// create or mutate `sys/tenancy` (ADR-0050 section 7 reads one fixed object).
///
/// A `NotFound` counts as reachable: the store answered, which is what
/// reachability means. This also keeps a not-yet-bootstrapped bucket (no
/// `sys/tenancy` marker written yet) from spuriously flipping readiness, since
/// only a transport/backend error means the store is actually unreachable.
///
/// Public so an integration test can drive probe cycles deterministically
/// against a controlled store, rather than sleeping through real intervals.
pub async fn run_probe_cycle(
    store: &dyn ObjectStoreBackend,
    hysteresis: &mut ProbeHysteresis,
) -> bool {
    let ok = match store.get(TENANCY_MARKER_KEY, GetRange::Full).await {
        Ok(_) | Err(StoreError::NotFound) => true,
        Err(_) => false,
    };
    if !ok {
        PROBE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    let reachable = hysteresis.observe(ok);
    set_store_reachable(reachable);
    reachable
}

/// Handle to the spawned probe task, so shutdown can stop it cleanly (mirrors
/// [`crate::fold::FoldTasks`]).
pub struct StoreProbeTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl StoreProbeTask {
    /// A no-op handle for the (currently none) modes that would not run a probe.
    /// Kept for symmetry with the other task handles and so shutdown is
    /// uniform.
    pub fn none() -> Self {
        StoreProbeTask {
            shutdown: None,
            handle: None,
        }
    }

    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle {
            let _ = handle.await;
        }
    }
}

/// Spawn the single background store probe (ADR-0050 section 7). Every
/// `interval` (jittered by the same helper the fold and maintenance tasks use,
/// so replicas do not tick in lockstep) it runs one [`run_probe_cycle`],
/// maintaining the process-global reachability atomic [`crate::health::Readiness`]
/// reads. Returns immediately; the task runs until [`StoreProbeTask::shutdown`].
pub fn spawn(store: Arc<dyn ObjectStoreBackend>, interval: Duration) -> StoreProbeTask {
    let (tx, mut rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut hysteresis = ProbeHysteresis::new();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(crate::fold::jittered(interval)) => {}
                _ = &mut rx => return,
            }
            let reachable = run_probe_cycle(store.as_ref(), &mut hysteresis).await;
            if !reachable {
                tracing::warn!(
                    key = TENANCY_MARKER_KEY,
                    "store probe: readiness is unhealthy after {K} consecutive failed probes of \
                     {TENANCY_MARKER_KEY}; /readyz is now 503. A single successful probe recovers it."
                );
            }
        }
    });
    StoreProbeTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The core of ADR-0050 section 7, in isolation: K consecutive failures are
    /// required to go unhealthy, and a single success recovers immediately.
    #[test]
    fn hysteresis_needs_k_failures_and_one_success_recovers() {
        let mut h = ProbeHysteresis::new();
        assert!(h.observe(true), "starts reachable on a success");

        // K-1 failures: still reachable (flips only AT K, not before).
        for i in 1..K {
            assert!(
                h.observe(false),
                "must stay reachable after {i} consecutive failures (< K={K})"
            );
        }
        // The K-th consecutive failure flips to unhealthy.
        assert!(
            !h.observe(false),
            "must be unhealthy after K={K} consecutive failures"
        );
        // Further failures keep it unhealthy.
        assert!(!h.observe(false), "stays unhealthy on further failures");

        // A single success recovers immediately (asymmetric: 1, not K).
        assert!(h.observe(true), "a single success recovers immediately");

        // And the failure streak reset: it again takes a full K to go down.
        for _ in 1..K {
            assert!(h.observe(false), "streak reset, so < K stays reachable");
        }
        assert!(
            !h.observe(false),
            "unhealthy again only after another full K"
        );
    }
}
