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
//! probes, routed to the same handler functions so a
//! Prometheus-shaped client can probe the paths it already knows.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
///
/// A third input, the `draining` flag, is one-way in the opposite direction:
/// it starts `false` and graceful shutdown flips it to `true` exactly once,
/// before any listener closes, so a readiness probe observes 503 while the
/// process is still routing and draining. It never flips back: a process that
/// has begun draining is on its way out and must not re-advertise itself as a
/// rollout target. It is intentionally distinct from the startup latch (which
/// never reverts either, but in the other direction) so that beginning to drain
/// cannot be confused with startup never having completed.
#[derive(Clone, Default)]
pub struct Readiness {
    /// One-way startup-completion latch: `false` until startup finishes, then
    /// `true` forever.
    startup: Arc<AtomicBool>,
    /// One-way drain latch: `false` until graceful shutdown begins, then `true`
    /// forever.
    draining: Arc<AtomicBool>,
    /// In-process ingest health sources (issue #1299): each is a router that
    /// reports not-ready once one of its shard actors has been condemned after
    /// exhausting its respawn budget. `is_ready` ANDs `shards_ready` across all
    /// of them, so a condemned shard turns `/readyz` to 503 and the orchestrator
    /// replaces this replica. Registered once at startup; never mutated after.
    ingest: Arc<Mutex<Vec<Arc<dyn IngestHealth>>>>,
}

/// In-process ingest health consulted by the readiness probe (issue #1299): a
/// router reports not-ready once one of its shard actors has exhausted its
/// respawn budget and been condemned. Pull-based -- [`Readiness::is_ready`]
/// reads it on each probe (a cheap atomic load) -- so no code path has to
/// remember to set a flag, matching the store-probe design's one-truth,
/// read-on-demand shape.
pub trait IngestHealth: Send + Sync {
    /// False once this router has a condemned shard.
    fn shards_ready(&self) -> bool;
}

impl IngestHealth for ravel_ingest::IngestRouter {
    fn shards_ready(&self) -> bool {
        self.ready()
    }
}

impl Readiness {
    /// A new flag in the not-ready, not-draining state.
    pub fn new() -> Self {
        Self {
            startup: Arc::new(AtomicBool::new(false)),
            draining: Arc::new(AtomicBool::new(false)),
            ingest: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register an in-process ingest health source (issue #1299). Called once
    /// per router at startup, before `mark_ready`. Poison-recovering: this is
    /// self-observability plumbing, not a durability path.
    pub fn register_ingest_health(&self, source: Arc<dyn IngestHealth>) {
        self.ingest
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(source);
    }

    /// Whether every registered ingest source reports its shards ready. True
    /// when none is registered (the modes that run no ingest router).
    fn ingest_shards_ready(&self) -> bool {
        self.ingest
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .all(|source| source.shards_ready())
    }

    /// Latch the startup flag to ready. Idempotent; calling it more than once is
    /// harmless and the startup latch can never move back to not-ready. Store
    /// reachability is tracked separately by [`crate::store_probe`] and can flip
    /// both ways after this latches.
    pub fn mark_ready(&self) {
        self.startup.store(true, Ordering::SeqCst);
    }

    /// Latch the drain flag so `is_ready` returns false from here on. One-way:
    /// once draining begins the process is leaving and must never re-advertise
    /// as ready. `pub(crate)` so it is reachable only from graceful shutdown
    /// within this crate, matching the doc: nothing outside the crate may flip a
    /// process into draining.
    pub(crate) fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    /// Whether the startup latch alone has fired, ignoring store reachability
    /// and draining. Retained for tests and callers that need the latch state
    /// specifically; `/readyz` uses [`Readiness::is_ready`].
    pub fn startup_complete(&self) -> bool {
        self.startup.load(Ordering::SeqCst)
    }

    /// Whether graceful shutdown has begun draining this process.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Whether the process is ready to serve: startup has completed, the process
    /// is NOT draining, the background store probe currently reports the store
    /// reachable (ADR-0050 section 7), AND every registered ingest router still
    /// has all its shards (no shard condemned after exhausting its respawn
    /// budget, issue #1299). Any condition false yields 503 at `/readyz`.
    pub fn is_ready(&self) -> bool {
        self.startup_complete()
            && !self.is_draining()
            && crate::store_probe::store_reachable()
            && self.ingest_shards_ready()
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

/// Readiness: 200 only when all three of [`Readiness::is_ready`]'s conditions
/// hold (startup completed, the process is not draining, and the background
/// store probe reports the store reachable); 503 whenever any one is false, so
/// before startup completes, once graceful shutdown begins draining, or during a
/// store outage.
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

    #[test]
    fn draining_makes_is_ready_false_without_reverting_the_startup_latch() {
        let readiness = Readiness::new();
        readiness.mark_ready();
        assert!(readiness.is_ready(), "ready once startup completes");
        assert!(!readiness.is_draining(), "not draining before shutdown");

        // Begin draining: readiness drops to not-ready so a probe sees 503,
        // but the startup latch itself must NOT revert (a draining process has
        // still completed startup; it is leaving, not un-started).
        readiness.begin_drain();
        assert!(readiness.is_draining(), "draining after begin_drain");
        assert!(
            readiness.startup_complete(),
            "the one-way startup latch must not revert when draining begins"
        );
        assert!(
            !readiness.is_ready(),
            "a draining process is not ready even with startup complete and store reachable"
        );

        // One-way: draining stays latched, and the drain state is shared across
        // clones (shutdown flips one handle; the /readyz handler holds another).
        let clone = readiness.clone();
        assert!(clone.is_draining(), "clone observes the shared drain latch");
        assert!(!clone.is_ready(), "clone is not ready while draining");
    }

    #[test]
    fn a_condemned_ingest_shard_makes_readiness_not_ready() {
        struct Health(bool);
        impl IngestHealth for Health {
            fn shards_ready(&self) -> bool {
                self.0
            }
        }

        let readiness = Readiness::new();
        readiness.mark_ready();
        assert!(
            readiness.is_ready(),
            "ready with startup complete, store reachable, no ingest source condemned"
        );

        // A healthy ingest source leaves readiness ready.
        readiness.register_ingest_health(Arc::new(Health(true)));
        assert!(
            readiness.is_ready(),
            "a healthy ingest source keeps it ready"
        );

        // A second source with a condemned shard (shards_ready == false) turns
        // the AND false, so /readyz becomes 503 even though startup completed,
        // the store is reachable, and the process is not draining.
        readiness.register_ingest_health(Arc::new(Health(false)));
        assert!(
            !readiness.is_ready(),
            "a condemned shard in any registered ingest source turns readiness not-ready"
        );
        assert!(
            readiness.startup_complete() && !readiness.is_draining(),
            "the condemned-shard path, not draining or an unset startup latch, is what dropped readiness"
        );
    }
}
