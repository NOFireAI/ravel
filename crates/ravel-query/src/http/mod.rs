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

pub use error::{MSG_CORRUPT, MSG_UNAVAILABLE, MSG_UNSATISFIABLE, QueryErrorResponse};
pub use tenant::{
    AuthError, DevHeaderTenantResolver, MtlsResolver, OidcError, OidcJwksCache, OidcResolver,
    StaticBearerTokenResolver, TenantResolver,
};

use crate::QueryEngine;

const ONE_HOUR_NS: i64 = 60 * 60 * 1_000_000_000;

/// Shared state for every route: the query engine and the tenant
/// resolution strategy. There is no `Default`: callers must pick a
/// `TenantResolver` explicitly (default-deny, docs/query-engine.md).
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<QueryEngine>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
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
