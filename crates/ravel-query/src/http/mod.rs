//! Prometheus-compatible HTTP API, exported as a plain axum `Router`
//! (docs/query-engine.md "HTTP API"). Library only: binding a listener and
//! wiring this into a service is left to the caller.

mod compat;
mod error;
mod handlers;
mod json;
mod params;
pub mod tenant;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use ravel_maintain::{NoopQueryAuditSink, QueryAuditSink};
use ravel_types::accounting::{NoopQueryCostRecorder, QueryCostRecorder};

pub use error::{MSG_CORRUPT, MSG_UNAVAILABLE, MSG_UNSATISFIABLE, QueryErrorResponse};
pub use tenant::{
    AuthError, DevHeaderTenantResolver, MtlsResolver, OidcError, OidcJwksCache, OidcResolver,
    StaticBearerTokenResolver, TenantResolver,
};

use crate::{QueryAdmissionController, QueryConcurrencyLimit, QueryEngine};

const ONE_HOUR_NS: i64 = 60 * 60 * 1_000_000_000;

/// Shared state for every route: the query engine, the tenant resolution
/// strategy, and the per-query cost recorder. There is no `Default`: callers
/// must pick a `TenantResolver` explicitly (default-deny, docs/query-engine.md).
///
/// Construct with [`AppState::new`], which defaults the cost recorder to the
/// no-op, so a caller that mounts the router without a `/metrics` aggregator,
/// and every test, needs no recorder of its own. A deployment attaches a real
/// one with [`AppState::with_cost_recorder`].
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<QueryEngine>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// Records each completed query's cost (its accounting snapshot and its
    /// pre-execution estimate) into a process aggregate exported at `/metrics`
    /// (ADR-0044 section 4, issue #425). Defaults to
    /// [`NoopQueryCostRecorder`]; a deployment sets the real aggregator so the
    /// Prometheus-shaped read paths fold into `/metrics` like the SQL path
    /// does.
    pub cost_recorder: Arc<dyn QueryCostRecorder>,
    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2). Every
    /// Prometheus-shaped handler acquires a [`crate::QueryPermit`] from this
    /// before doing any resolve or GET, and is rejected if admitting one more
    /// query would exceed this process's reconciled fleet threshold. Defaults to
    /// an [`QueryConcurrencyLimit::Unlimited`] controller in [`AppState::new`],
    /// so a caller that mounts the router without a ceiling (and every test)
    /// behaves exactly as before this mechanism existed; a deployment attaches
    /// the one shared controller with [`AppState::with_query_admission`].
    pub query_admission: Arc<QueryAdmissionController>,
    /// The evidential audit sink every query surface submits one
    /// [`AuditEvent`](ravel_maintain::AuditEvent) through before releasing its
    /// response (ADR-0062 §2a, epic EL / issue #762). Submission awaits the
    /// event's durability, so a completed handler's response is released only
    /// after its audit record is durable (or, in best-effort mode, after the
    /// pipeline decided to release it anyway). Defaults to
    /// [`NoopQueryAuditSink`] in [`AppState::new`], so a caller that mounts the
    /// router without an audit pipeline (and every test) runs unaudited exactly
    /// as before this seam existed; a deployment attaches the one shared
    /// pipeline with [`AppState::with_audit_sink`].
    pub audit_sink: Arc<dyn QueryAuditSink>,
}

impl AppState {
    /// State with the given engine and resolver, a no-op cost recorder, and an
    /// unlimited (never-rejecting) query concurrency controller.
    pub fn new(engine: Arc<QueryEngine>, tenant_resolver: Arc<dyn TenantResolver>) -> Self {
        AppState {
            engine,
            tenant_resolver,
            cost_recorder: Arc::new(NoopQueryCostRecorder),
            query_admission: QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
            audit_sink: Arc::new(NoopQueryAuditSink),
        }
    }

    /// Set the recorder every completed query folds its cost into. Returns
    /// `self` so it chains off [`AppState::new`].
    pub fn with_cost_recorder(mut self, cost_recorder: Arc<dyn QueryCostRecorder>) -> Self {
        self.cost_recorder = cost_recorder;
        self
    }

    /// Set the shared fleet-global query concurrency controller (ADR-0061
    /// decision 2). Returns `self` so it chains off [`AppState::new`]. A
    /// deployment passes the one controller instance shared with the SQL and
    /// Flight SQL surfaces, so the process holds a single honest in-flight count
    /// across every query transport.
    pub fn with_query_admission(mut self, query_admission: Arc<QueryAdmissionController>) -> Self {
        self.query_admission = query_admission;
        self
    }

    /// Set the evidential audit sink every query surface submits one event
    /// through and awaits durability on before responding (ADR-0062 §2a).
    /// Returns `self` so it chains off [`AppState::new`]. A deployment passes
    /// the one shared [`AuditPipeline`](ravel_maintain::AuditPipeline) instance
    /// so every read surface's audit trail lands through one seam.
    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn QueryAuditSink>) -> Self {
        self.audit_sink = audit_sink;
        self
    }
}

/// Builds the Prometheus-compatible query API router. The caller is
/// responsible for binding a listener, adding middleware (tracing,
/// compression, timeouts), and nesting this under whatever path prefix
/// their service uses.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/query", get(handlers::query).post(handlers::query))
        .route(
            "/api/v1/query_range",
            get(handlers::query_range).post(handlers::query_range),
        )
        .route("/api/v1/labels", get(handlers::labels))
        .route("/api/v1/label/{name}/values", get(handlers::label_values))
        .route(
            "/api/v1/series",
            get(handlers::series).post(handlers::series),
        )
        .with_state(state)
        // Stateless Prometheus compatibility routes (buildinfo, metadata).
        // Merged here so every service mounting this router serves them
        // without any extra wiring of its own.
        .merge(compat::router())
}
