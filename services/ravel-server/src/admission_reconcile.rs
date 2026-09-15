//! Background fleet-global admission reconciliation task (ADR-0057).
//!
//! Wraps [`ravel_ingest::reconcile_once`] in a periodic loop with clean
//! shutdown, mirroring the fold and maintenance background tasks
//! ([`crate::fold`]): the reconciliation *mechanism* lives in `ravel-ingest`
//! (and is unit-tested there); the *lifecycle* (interval `R`, jitter, shutdown)
//! lives here, the same split every other background loop in this crate uses.
//!
//! Spawned only in the ingest-serving modes ([`crate::config::Mode::All`] and
//! `Gateway`): a query- or maintain-only process runs no ingest admission, so
//! it has no usage to reconcile. Every cycle is best-effort and never on the
//! hot path (ADR-0057): a failed reconciliation read keeps the last-known
//! threshold and increments a counter rather than degrading admission, and the
//! hot-path checks never wait on this task.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_ingest::{AdmissionController, Clock, ReconcileCycleStats, SystemClock, reconcile_once};
use ravel_object_store::ObjectStoreBackend;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::fold::jittered;

/// The one reconciliation figure the exporter cannot read off the controller.
///
/// [`AdmissionController::last_reconcile_cycle_stats`] publishes the most
/// recent cycle only, so its three last-observed figures (duration, siblings,
/// stale keys skipped) render straight out of it as gauges. Reaped keys is the
/// exception: [`ReconcileCycleStats::keys_reaped`] is also per cycle, and
/// publishing a per-cycle count under a `_total` name gives a counter that
/// drops to zero on the first cycle that reaps nothing. So the running total
/// is kept here, on the process, and the loop adds each cycle's count to it as
/// the cycle completes.
#[derive(Debug, Default)]
pub struct ReconcileCycleMetrics {
    keys_reaped_total: AtomicU64,
}

impl ReconcileCycleMetrics {
    /// Snapshot keys reaped by every completed cycle since process start,
    /// rendered as `ravel_admission_reconciliation_keys_reaped_total`.
    pub fn keys_reaped_total(&self) -> u64 {
        self.keys_reaped_total.load(Ordering::Relaxed)
    }

    /// Fold one completed cycle's figures in. Called by the loop below once
    /// [`reconcile_once`] returns, the same point the controller's own
    /// last-cycle copy is replaced.
    pub fn record_cycle(&self, stats: &ReconcileCycleStats) {
        self.keys_reaped_total
            .fetch_add(stats.keys_reaped, Ordering::Relaxed);
    }
}

/// Handle to the spawned reconciliation task, so shutdown can stop it cleanly
/// (mirrors [`crate::fold::FoldTasks`]).
pub struct AdmissionReconcileTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl AdmissionReconcileTask {
    /// No task (query/maintain modes, which serve no ingest admission).
    pub fn none() -> Self {
        AdmissionReconcileTask {
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

/// Spawn the reconciliation loop for `controller` against `store` on interval
/// `R`. Returns immediately; the task runs in the background until
/// [`AdmissionReconcileTask::shutdown`]. The first cycle sleeps a full
/// (jittered) interval before its first write/read, so a fleet of replicas
/// started together do not reconcile in lockstep forever.
pub fn spawn(
    controller: Arc<AdmissionController>,
    store: Arc<dyn ObjectStoreBackend>,
    interval: Duration,
    cycle_metrics: Arc<ReconcileCycleMetrics>,
) -> AdmissionReconcileTask {
    let (tx, mut rx) = oneshot::channel();
    // Production OS-entropy jitter (ADR-0068 decision 2), the same default the
    // fold and maintenance loops use; the harness does not drive this loop.
    let rng: Arc<dyn ravel_commit::rng::RngSource> = Arc::new(ravel_commit::rng::SystemRng);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
                _ = &mut rx => return,
            }
            let now_ns = SystemClock.now_ns();
            let stats = reconcile_once(controller.as_ref(), store.as_ref(), interval, now_ns).await;
            cycle_metrics.record_cycle(&stats);
            // A cycle that approaches the `2 * R` staleness window makes every
            // sibling read as stale, so each process starts enforcing the whole
            // fleet cap alone (issue #1679). The figures move before that does.
            tracing::debug!(
                cycle_duration_ns = stats.cycle_duration_ns,
                siblings_observed = stats.siblings_observed,
                stale_keys_skipped = stats.stale_keys_skipped,
                keys_reaped = stats.keys_reaped,
                "admission reconciliation cycle"
            );
        }
    });
    AdmissionReconcileTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}
