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
use std::time::Duration;

use ravel_ingest::{AdmissionController, Clock, SystemClock, reconcile_once};
use ravel_object_store::ObjectStoreBackend;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::fold::jittered;

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
            reconcile_once(controller.as_ref(), store.as_ref(), interval, now_ns).await;
        }
    });
    AdmissionReconcileTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}
