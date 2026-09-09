//! Supervision for the process's metric metadata flush loop (ADR-0085 decision
//! 1, write side).
//!
//! The *mechanism* lives in `ravel-ingest` ([`ravel_ingest::MetadataSink`]): the
//! fingerprint map, the debounce window's one-GET-and-at-most-one-CAS-PUT per
//! tenant, the field-wise merge, the bounded CAS retry. This module owns only
//! the process lifecycle half, exactly the way [`crate::lifecycle_refresh`] and
//! [`crate::admission_reconcile`] split over their `ravel-ingest` mechanisms:
//! one background task, spawned when the process serves ingest, joined on
//! shutdown after a final flush.
//!
//! The sink itself is constructed once in [`crate::start`] next to the ingest
//! router (one per process, not one per protocol, so the process never races
//! itself on `t/<tenant_hash>/m/meta`) and shared by `Arc` with every ingest
//! surface, which calls [`ravel_ingest::MetadataSink::observe`] on the request
//! path. `observe` is synchronous and does no I/O, so no ingest acknowledgement
//! ever waits on this loop.

use std::sync::Arc;

use ravel_ingest::MetadataSink;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Handle to the spawned flush loop. Dropping it leaves the loop running;
/// [`shutdown`](Self::shutdown) stops it and waits for its final flush.
pub struct MetadataSinkTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl MetadataSinkTask {
    /// A task handle for a mode that spawned no flush loop (a query-only
    /// process, or a disabled sink). Shutting it down is a no-op.
    pub fn none() -> Self {
        MetadataSinkTask {
            shutdown: None,
            handle: None,
        }
    }

    /// Signal the loop to stop and wait for it to finish, including the final
    /// flush of whatever window was in progress.
    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle {
            let _ = handle.await;
        }
    }
}

/// Spawn the metadata flush loop. Returns immediately; the loop runs until
/// [`MetadataSinkTask::shutdown`], flushing once per debounce window
/// ([`ravel_ingest::MetadataSinkConfig::window`]) and once more on the way out.
///
/// Same supervision shape as [`crate::lifecycle_refresh::spawn`]: this module
/// owns the oneshot, the loop never returns an error (every metadata failure is
/// counted and logged inside the sink, never fatal to ingest), and a `None` sink
/// (a process that serves no ingest) spawns nothing.
pub fn spawn(sink: Option<Arc<MetadataSink>>) -> MetadataSinkTask {
    let Some(sink) = sink else {
        return MetadataSinkTask::none();
    };
    let (tx, rx) = oneshot::channel();
    let window = sink.config().window;
    let handle = tokio::spawn(async move { sink.run(rx).await });
    tracing::debug!(
        window_secs = window.as_secs(),
        "metric metadata flush loop started (ADR-0085 decision 1)"
    );
    MetadataSinkTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ingest::{IngestMetrics, MetadataSinkConfig};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    /// The spawned loop flushes what was observed before shutdown, so a graceful
    /// stop does not discard the window in progress. Uses a short window so the
    /// final-flush path is what the assertion depends on, not the cadence.
    #[tokio::test]
    async fn shutdown_joins_the_loop_after_a_final_flush() {
        let store = Arc::new(MemoryStore::new());
        let sink = Arc::new(MetadataSink::new(
            store.clone(),
            MetadataSinkConfig::default(),
            Arc::new(IngestMetrics::default()),
        ));
        let tenant = TenantId::new("tenant-a");
        sink.observe(
            &tenant,
            tenant.hash(),
            [ravel_otlp::metadata::MetricMetadata {
                family_name: "http_requests_total".to_string(),
                kind: ravel_otlp::metadata::MetricKind::Counter,
                help: "requests".to_string(),
                unit: String::new(),
            }],
        );

        let task = spawn(Some(sink));
        task.shutdown().await;

        let entries = ravel_catalog::read_metrics_meta(store.as_ref(), &tenant.hash())
            .await
            .expect("read metadata record")
            .expect("record written by the final flush")
            .0;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].family_name, "http_requests_total");
    }

    /// No sink (a process that serves no ingest) spawns nothing and shuts down
    /// cleanly.
    #[tokio::test]
    async fn no_sink_spawns_no_task() {
        spawn(None).shutdown().await;
    }
}
