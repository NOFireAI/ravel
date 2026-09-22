//! The operator's own `/healthz`, `/readyz`, and `/metrics` listener
//! (ADR-1731 decisions 2 and 4).
//!
//! Three fixed paths need no router: a bare `hyper` 1 HTTP/1.1 server built
//! on `hyper-util`'s `TokioIo`, with no `axum`/`tower` in the accept loop
//! (ADR-1731 rejected alternatives). `crate::controller::run` starts this
//! listener before the controller and spawns the controller as its own task
//! so this module can observe the task's termination for liveness.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use kube_runtime::reflector::Store;
use tokio::net::TcpListener;
use tracing::warn;

use crate::crd::RavelCluster;
use crate::metrics::{self, ReconcileMetrics};

/// Default bind address for [`serve`] (ADR-1731 decision 2), and the
/// `--listen-health` flag's default in `main.rs`. Matches the `health`
/// container port named in `deploy/k8s/operator/operator.yaml`'s probes and
/// scrape annotation.
pub const DEFAULT_HEALTH_ADDR: &str = "0.0.0.0:8080";

/// Shared state the listener reads on every request. Liveness and readiness
/// are each a single `bool` flipped once by `controller::run`; the metrics
/// and reflector store are read fresh on every `/metrics` scrape.
pub struct HealthState {
    /// True until the spawned controller task terminates (decision 4:
    /// liveness is "the controller future is running").
    controller_alive: AtomicBool,
    /// True once the `RavelCluster` reflector has delivered its initial list
    /// (decision 4: readiness is "the initial watch list has arrived").
    ready: AtomicBool,
    metrics: Arc<ReconcileMetrics>,
    clusters: Store<RavelCluster>,
}

impl HealthState {
    /// `metrics` is the same `Arc` [`crate::controller::Context`] holds, so a
    /// count `controller::reconcile` records is the one `/metrics` renders
    /// on the very next scrape.
    pub fn new(clusters: Store<RavelCluster>, metrics: Arc<ReconcileMetrics>) -> Self {
        Self {
            controller_alive: AtomicBool::new(true),
            ready: AtomicBool::new(false),
            metrics,
            clusters,
        }
    }

    /// The reconcile counters `controller::reconcile` records into.
    pub fn metrics(&self) -> &ReconcileMetrics {
        &self.metrics
    }

    /// Mark the controller task terminated. `/healthz` answers 503 from this
    /// point on so the kubelet restarts the pod.
    pub fn mark_controller_stopped(&self) {
        self.controller_alive.store(false, Ordering::Relaxed);
    }

    /// Mark the initial `RavelCluster` list delivered. `/readyz` answers 200
    /// from this point on.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }

    fn is_alive(&self) -> bool {
        self.controller_alive.load(Ordering::Relaxed)
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

fn text_response(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .unwrap_or_else(|_| {
            let mut response = Response::new(Full::new(Bytes::from_static(b"")));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        })
}

fn metrics_response(state: &HealthState) -> Response<Full<Bytes>> {
    let body = metrics::render(state.metrics(), state.clusters.len());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| {
            let mut response = Response::new(Full::new(Bytes::from_static(b"")));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        })
}

async fn handle(
    state: Arc<HealthState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match req.uri().path() {
        "/healthz" => {
            if state.is_alive() {
                text_response(StatusCode::OK, "ok")
            } else {
                text_response(StatusCode::SERVICE_UNAVAILABLE, "controller task stopped")
            }
        }
        "/readyz" => {
            if state.is_ready() {
                text_response(StatusCode::OK, "ok")
            } else {
                text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "initial watch list not received",
                )
            }
        }
        "/metrics" => metrics_response(&state),
        _ => text_response(StatusCode::NOT_FOUND, "not found"),
    };
    Ok(response)
}

/// Bind `addr` and serve `/healthz`, `/readyz`, and `/metrics` until the
/// process exits. Runs for the process lifetime; a per-connection error is
/// logged and the listener keeps accepting (one bad connection must not take
/// down the probe surface).
pub async fn serve(addr: SocketAddr, state: Arc<HealthState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(error) => {
                warn!(%error, "health listener accept failed");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = service_fn(move |req| handle(Arc::clone(&state), req));
            if let Err(error) = http1::Builder::new().serve_connection(io, service).await {
                warn!(%error, "health connection error");
            }
        });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use kube_runtime::reflector;

    fn state() -> Arc<HealthState> {
        let (reader, _writer) = reflector::store::<RavelCluster>();
        Arc::new(HealthState::new(reader, Arc::new(ReconcileMetrics::new())))
    }

    // `hyper::body::Incoming` cannot be constructed outside hyper's own
    // server internals, so `handle` is exercised through the state machine
    // it reads (`is_alive`/`is_ready`) rather than with a built `Request`;
    // the accept-loop wiring itself is covered by the integration test named
    // in ADR-1731's follow-up.
    #[tokio::test]
    async fn healthz_is_ok_until_marked_stopped() {
        let state = state();
        assert!(state.is_alive());
        state.mark_controller_stopped();
        assert!(!state.is_alive());
    }

    #[tokio::test]
    async fn readyz_flips_once_marked_ready() {
        let state = state();
        assert!(!state.is_ready());
        state.mark_ready();
        assert!(state.is_ready());
    }
}
