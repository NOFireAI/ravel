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

use crate::admission_reconcile::{SpawnError, check_spawnable};
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
/// started together do not reconcile in lockstep. A zero `interval` is refused
/// with [`SpawnError::ZeroReconcileInterval`], the same refusal the ingest
/// reconciliation applies to the same `--admission-reconcile-interval`.
pub fn spawn(
    controller: Arc<QueryAdmissionController>,
    store: Arc<dyn ObjectStoreBackend>,
    interval: Duration,
) -> Result<QueryAdmissionReconcileTask, SpawnError> {
    check_spawnable(interval)?;
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
            reconcile_query_admission_once(controller.as_ref(), store.as_ref(), interval, now_ns)
                .await;
        }
    });
    Ok(QueryAdmissionReconcileTask {
        shutdown: Some(tx),
        handle: Some(handle),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::QueryConcurrencyLimit;

    use super::*;

    /// A zero interval is refused with the shared typed error naming the flag,
    /// and no task is spawned.
    ///
    /// Flip to watch it fail: delete `check_spawnable(interval)?;` in
    /// [`spawn`], and the refusal expectation fails.
    #[tokio::test]
    async fn spawn_refuses_a_zero_reconcile_interval() {
        let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let metrics = tokio::runtime::Handle::current().metrics();
        let alive_before = metrics.num_alive_tasks();
        let err = spawn(controller, store, Duration::ZERO)
            .err()
            .expect("a zero reconcile interval must be refused at spawn");
        assert!(
            matches!(err, SpawnError::ZeroReconcileInterval),
            "expected ZeroReconcileInterval, got: {err}"
        );
        assert!(
            err.to_string().contains("--admission-reconcile-interval"),
            "the refusal must name the flag, got: {err}"
        );
        assert_eq!(
            metrics.num_alive_tasks(),
            alive_before,
            "a refused spawn must leave no task running"
        );
    }
}
