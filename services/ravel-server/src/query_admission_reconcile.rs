//! Background fleet-global query-concurrency reconciliation task (ADR-0061
//! decision 2, reusing ADR-0057's pattern).
//!
//! Wraps [`ravel_query::reconcile_query_admission_once`] in a periodic loop with
//! clean shutdown, exactly like [`crate::admission_reconcile`] wraps the ingest
//! reconciliation: the *mechanism* (write own snapshot, read siblings, recompute
//! the local threshold) lives in `ravel-query` and is unit-tested there; the
//! *lifecycle* (interval `R`, jitter, shutdown) lives here.
//!
//! Spawned only in the query-serving modes ([`crate::config::Mode::All`] and
//! `Query`): a gateway- or maintain-only process serves no queries, so it holds
//! no concurrency stock to reconcile. Every cycle is best-effort and never on
//! the request path: a failed sibling read keeps the last-known threshold and
//! increments a counter rather than degrading admission, and the hot-path
//! admission check never waits on this task. Under an unlimited ceiling the
//! reconcile call is a no-op and issues no object-store I/O at all.

use std::sync::Arc;
use std::time::Duration;

use ravel_ingest::{Clock, SystemClock};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::{QueryAdmissionController, reconcile_query_admission_once};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::fold::jittered;

/// Handle to the spawned reconciliation task, so shutdown can stop it cleanly
/// (mirrors [`crate::admission_reconcile::AdmissionReconcileTask`]).
pub struct QueryAdmissionReconcileTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl QueryAdmissionReconcileTask {
    /// No task (gateway/maintain modes, which serve no queries).
    pub fn none() -> Self {
        QueryAdmissionReconcileTask {
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
/// `R`. Returns immediately; the task runs until
/// [`QueryAdmissionReconcileTask::shutdown`]. The first cycle sleeps a full
/// (jittered) interval before its first write/read, so a fleet of replicas
/// started together do not reconcile in lockstep.
pub fn spawn(
    controller: Arc<QueryAdmissionController>,
    store: Arc<dyn ObjectStoreBackend>,
    interval: Duration,
) -> QueryAdmissionReconcileTask {
    let (tx, mut rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(jittered(interval)) => {}
                _ = &mut rx => return,
            }
            let now_ns = SystemClock.now_ns();
            reconcile_query_admission_once(controller.as_ref(), store.as_ref(), interval, now_ns)
                .await;
        }
    });
    QueryAdmissionReconcileTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}
