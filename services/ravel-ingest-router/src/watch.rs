//! Drives the [`kube_runtime::watcher`] event stream for the gateway Service's
//! EndpointSlices into the shared [`EndpointStore`] (deliverable 2).
//!
//! This is the only I/O-bound part of the watcher; the state reduction it feeds
//! ([`EndpointStore::apply_event`]) is pure and unit-tested without a cluster.

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::Api;
use kube_runtime::watcher;

use crate::endpoints::EndpointStore;

/// The standard label the EndpointSlice controller stamps on every slice it
/// generates for a Service.
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// The `kube_runtime::watcher` config selecting only the gateway Service's
/// slices.
pub(crate) fn watch_config(service_name: &str) -> watcher::Config {
    watcher::Config::default().labels(&format!("{SERVICE_NAME_LABEL}={service_name}"))
}

/// Run the watch loop until the stream ends (it normally does not; the watcher
/// reconnects with backoff internally). Each event updates the store, which
/// swaps its snapshot; the first `InitDone` latches readiness.
pub(crate) async fn run(
    api: Api<EndpointSlice>,
    config: watcher::Config,
    store: Arc<EndpointStore>,
) {
    let stream = watcher(api, config);
    futures::pin_mut!(stream);
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => store.apply_event(event),
            // A watch error is transient (the underlying watcher retries with
            // backoff); log and keep consuming. Readiness only ever latches on a
            // successful `InitDone`, so a failing watch simply leaves the router
            // not-ready rather than serving a stale or empty set as ready.
            Err(error) => tracing::warn!(%error, "endpointslice watch error"),
        }
    }
    tracing::warn!("endpointslice watch stream ended");
}
