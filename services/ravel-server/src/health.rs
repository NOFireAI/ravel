//! Kubernetes liveness (`/healthz`) and readiness (`/readyz`) routes
//! (ADR-0034 decision 4).
//!
//! Both routes are served on the HTTP listener in every mode, including
//! [`Mode::Maintain`](crate::Mode::Maintain), whose router is otherwise empty.
//! They are ordinary routed handlers, not special-cased bypasses: a request
//! reaching `/healthz` proves the axum server task is running and can route,
//! which is exactly what liveness means.
//!
//! - `/healthz` (liveness): always 200 once the handler is reachable. If the
//!   event loop is alive enough to route the request, the process is alive by
//!   definition, so this handler carries no state.
//! - `/readyz` (readiness): 503 until startup has fully completed (config
//!   parsed, the object-store capability gate passed, listeners bound), then
//!   200. It performs no object-store I/O per probe: a store call on every
//!   kubelet probe of every pod would add real S3 cost, and a transient S3
//!   blip would eject every pod from its Service at once. Continuous store
//!   health probing is a deliberate follow-up, not an omission here.
//!
//! `/-/healthy` and `/-/ready` are Prometheus' own spellings of the same two
//! probes, routed to the same handler functions (issue #336) so a
//! Prometheus-shaped client can probe the paths it already knows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

/// Shared readiness flag for the `/readyz` handler.
///
/// Starts `false` and is flipped to `true` exactly once, at the point in the
/// startup sequence where config is parsed, the capability gate has passed,
/// and both listeners are bound. It never flips back: readiness is a
/// startup-completion latch, not a live health signal (see the module docs on
/// why `/readyz` does no per-probe store call).
#[derive(Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// A new flag in the not-ready state.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Latch the flag to ready. Idempotent; calling it more than once is
    /// harmless and it can never move back to not-ready.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether startup has completed.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Router carrying `/healthz` and `/readyz`, with `readiness` baked in as
/// state so the returned `Router` merges into the main router like every other
/// mode's routes.
pub fn router(readiness: Readiness) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        // Prometheus' own health/readiness paths, pointed at the same two
        // handler functions rather than reimplemented: Grafana and other
        // Prometheus-shaped clients probe these, and any divergence between
        // the two spellings would be a bug by construction.
        .route("/-/healthy", get(healthz))
        .route("/-/ready", get(readyz))
        .with_state(readiness)
}

/// Liveness: reachable means alive.
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: 200 once startup has completed, 503 before that.
async fn readyz(State(readiness): State<Readiness>) -> StatusCode {
    if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn readiness_starts_not_ready_flips_once_and_never_reverts() {
        let readiness = Readiness::new();
        assert!(!readiness.is_ready(), "must start not ready");

        readiness.mark_ready();
        assert!(readiness.is_ready(), "must be ready after mark_ready");

        // Idempotent: a second mark_ready does not revert or change state.
        readiness.mark_ready();
        assert!(readiness.is_ready(), "must stay ready");

        // A clone observes the same latched state (the flag is shared).
        let clone = readiness.clone();
        assert!(clone.is_ready(), "clone shares the latched state");
    }
}
