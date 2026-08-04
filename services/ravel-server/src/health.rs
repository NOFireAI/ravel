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
//!   200 for as long as the store also stays reachable. It performs no
//!   object-store I/O per probe: a store call on every kubelet probe of every
//!   pod would add real S3 cost, and a transient S3 blip would eject every pod
//!   from its Service at once. Since ADR-0050 section 7 (EC7) readiness is the
//!   AND of the startup latch and a background store-reachability flag
//!   ([`crate::store_probe`]): the continuous store probing that the original
//!   comment deferred now runs on its own jittered cadence, with hysteresis, so
//!   `/readyz` reflects a real store outage while still reading only an atomic
//!   per probe.
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

/// Readiness for the `/readyz` handler: the AND of a one-way startup-completion
/// latch and the background store-reachability flag (ADR-0050 section 7, EC7).
///
/// The startup latch starts `false` and is flipped to `true` exactly once, at
/// the point in the startup sequence where config is parsed, the capability gate
/// has passed, and both listeners are bound. It never flips back. On its own it
/// is a startup-completion latch, not a live health signal; readiness combines
/// it with [`crate::store_probe::store_reachable`], which the background probe
/// task flips as the store becomes unreachable or recovers. `/readyz` is 200
/// only when startup has completed AND the store is currently reachable, so a
/// store outage correctly halts readiness-gated rollouts without any per-probe
/// store call on this path.
///
/// `/healthz` (liveness) is deliberately independent of the probe: a store
/// outage must never make liveness fail and get healthy processes killed and
/// restarted (see the module docs).
#[derive(Clone, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// A new flag in the not-ready state.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Latch the startup flag to ready. Idempotent; calling it more than once is
    /// harmless and the startup latch can never move back to not-ready. Store
    /// reachability is tracked separately by [`crate::store_probe`] and can flip
    /// both ways after this latches.
    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether the startup latch alone has fired, ignoring store reachability.
    /// Retained for tests and callers that need the latch state specifically;
    /// `/readyz` uses [`Readiness::is_ready`], which also requires the store to
    /// be reachable.
    pub fn startup_complete(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// Whether the process is ready to serve: startup has completed AND the
    /// background store probe currently reports the store reachable (ADR-0050
    /// section 7). Either condition false yields 503 at `/readyz`.
    pub fn is_ready(&self) -> bool {
        self.startup_complete() && crate::store_probe::store_reachable()
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
