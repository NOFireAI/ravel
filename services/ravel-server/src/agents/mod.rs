//! The Ravel Agents server seam (ADR-3331 decision 9), compiled only with the
//! `agents` cargo feature.
//!
//! `crate::start` mounts [`router`] on the public and the mTLS router from
//! inside its query block when `--agents` is set, feeds the
//! [`AgentsRuntime`]'s counters to `/metrics`, and adds an
//! [`AgentsReadiness`] to `/readyz`. The router serves
//! `GET /api/v1/agents/health`, which reports the settings below, and the
//! client and worker routes of milestones 1 and 2 over one
//! [`ravel_agents_runtime::Coordinator`] per tenant. Timer, effect and
//! reconcile routes are not mounted.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use ravel_query::http::TenantResolver;
use serde::Serialize;

use crate::ServerConfig;
use crate::config::Mode;
use crate::health::IngestHealth;

mod routes;
mod runtime;

pub use routes::MAX_EVENTS_PAGE;
pub use runtime::{AgentsRuntime, LONG_POLL_CAP_MS, Lane, TenantAgents};

/// The unauthenticated settings route.
pub const HEALTH_PATH: &str = "/api/v1/agents/health";

/// Every authenticated route, as `(method, path)`, in mount order.
pub const ROUTES: &[(&str, &str)] = &[
    ("POST", "/api/v1/agents/create"),
    ("POST", "/api/v1/agents/send"),
    ("POST", "/api/v1/agents/call"),
    ("GET", "/api/v1/agents/operation"),
    ("GET", "/api/v1/agents/result"),
    ("POST", "/api/v1/agents/resolve"),
    ("POST", "/api/v1/agents/cancel"),
    ("GET", "/api/v1/agents/state"),
    ("POST", "/api/v1/agents/events"),
    ("POST", "/api/v1/agents/worker/register"),
    ("POST", "/api/v1/agents/worker/poll"),
    ("POST", "/api/v1/agents/worker/commit"),
    ("POST", "/api/v1/agents/worker/heartbeat"),
];

/// The agents settings a process runs with: its mode and the resolved
/// `--agents-*` values from [`crate::config::AgentsConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentsSettings {
    pub mode: Mode,
    pub activation_concurrency: usize,
    pub poll_concurrency: usize,
    pub memory_budget_bytes: u64,
    pub max_agents: u64,
    pub max_mailbox: u64,
    pub max_state_bytes: u64,
    pub max_receipts: u64,
    pub max_timers: u64,
    pub max_body_bytes: usize,
    pub lease_ms: u64,
    pub heartbeat_grace_ms: u64,
}

impl AgentsSettings {
    pub fn from_server_config(config: &ServerConfig) -> Self {
        let agents = &config.query_budgets.agents;
        AgentsSettings {
            mode: config.mode,
            activation_concurrency: agents.activation_concurrency,
            poll_concurrency: agents.poll_concurrency,
            memory_budget_bytes: agents.memory_budget_bytes,
            max_agents: agents.max_agents,
            max_mailbox: agents.max_mailbox,
            max_state_bytes: agents.max_state_bytes,
            max_receipts: agents.max_receipts,
            max_timers: agents.max_timers,
            max_body_bytes: agents.max_body_bytes,
            lease_ms: agents.lease_ms,
            heartbeat_grace_ms: agents.heartbeat_grace_ms,
        }
    }
}

/// Whether the agents router has been mounted. Reports not ready until
/// [`Self::mark_mounted`], so a process started with `--agents` does not pass
/// `/readyz` while its agents routes are absent.
#[derive(Debug, Default)]
pub struct AgentsReadiness {
    mounted: AtomicBool,
}

impl AgentsReadiness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_mounted(&self) {
        self.mounted.store(true, Ordering::Release);
    }

    pub fn is_mounted(&self) -> bool {
        self.mounted.load(Ordering::Acquire)
    }
}

impl IngestHealth for AgentsReadiness {
    fn shards_ready(&self) -> bool {
        self.is_mounted()
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct HealthBody {
    feature: &'static str,
    mode: &'static str,
    activation_concurrency: usize,
    poll_concurrency: usize,
    memory_budget_bytes: u64,
    quotas: QuotaBody,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct QuotaBody {
    max_agents: u64,
    max_mailbox: u64,
    max_state_bytes: u64,
    max_receipts: u64,
    max_timers: u64,
}

impl HealthBody {
    fn new(settings: &AgentsSettings) -> Self {
        HealthBody {
            feature: "agents",
            mode: crate::metrics::mode_name(settings.mode),
            activation_concurrency: settings.activation_concurrency,
            poll_concurrency: settings.poll_concurrency,
            memory_budget_bytes: settings.memory_budget_bytes,
            quotas: QuotaBody {
                max_agents: settings.max_agents,
                max_mailbox: settings.max_mailbox,
                max_state_bytes: settings.max_state_bytes,
                max_receipts: settings.max_receipts,
                max_timers: settings.max_timers,
            },
        }
    }
}

/// The agents router for one listener. `GET /api/v1/agents/health` reads no
/// store and no tenant data, only operator settings `/metrics` also exposes,
/// so it carries no authentication on either listener, including the mTLS
/// one, where `/healthz` itself is not served (ADR-3331 decision 9 mounts it
/// on both). Every other route resolves its tenant through `resolver`, the
/// listener's own.
pub fn router(runtime: Arc<AgentsRuntime>, resolver: Arc<dyn TenantResolver>) -> Router {
    let limit = runtime.settings.max_body_bytes;
    let api = routes::Api { runtime, resolver };
    Router::new()
        .route(HEALTH_PATH, get(health))
        .route("/api/v1/agents/create", post(routes::create))
        .route("/api/v1/agents/send", post(routes::send))
        .route("/api/v1/agents/call", post(routes::call))
        .route("/api/v1/agents/operation", get(routes::operation))
        .route("/api/v1/agents/result", get(routes::result))
        .route("/api/v1/agents/resolve", post(routes::resolve))
        .route("/api/v1/agents/cancel", post(routes::cancel))
        .route("/api/v1/agents/state", get(routes::state))
        .route("/api/v1/agents/events", post(routes::events))
        .route(
            "/api/v1/agents/worker/register",
            post(routes::worker_register),
        )
        .route("/api/v1/agents/worker/poll", post(routes::worker_poll))
        .route("/api/v1/agents/worker/commit", post(routes::worker_commit))
        .route(
            "/api/v1/agents/worker/heartbeat",
            post(routes::worker_heartbeat),
        )
        .layer(DefaultBodyLimit::max(limit))
        .with_state(api)
}

async fn health(State(api): State<routes::Api>) -> Json<HealthBody> {
    Json(HealthBody::new(&api.runtime.settings))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::AgentsConfig;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn settings() -> AgentsSettings {
        AgentsSettings {
            mode: Mode::Query,
            activation_concurrency: 3,
            poll_concurrency: 5,
            memory_budget_bytes: 7,
            max_agents: 11,
            max_mailbox: 13,
            max_state_bytes: 17,
            max_receipts: 19,
            max_timers: 23,
            max_body_bytes: 4096,
            lease_ms: 30_000,
            heartbeat_grace_ms: 10_000,
        }
    }

    const WORKER_TOKEN: &str = "worker-token";

    fn test_router(settings: AgentsSettings) -> Router {
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            WORKER_TOKEN.to_string(),
            ravel_tenant_resolve::Principal {
                agents_worker: true,
                ..ravel_tenant_resolve::Principal::tenant_only(ravel_types::TenantId::new("acme"))
            },
        );
        let resolver: Arc<dyn TenantResolver> =
            Arc::new(ravel_tenant_resolve::StaticBearerTokenResolver::with_principals(tokens));
        let runtime = Arc::new(AgentsRuntime::new(
            settings,
            Arc::new(AgentsReadiness::new()),
            Arc::new(ravel_object_store::memory::MemoryStore::new()),
        ));
        router(runtime, resolver)
    }

    /// `worker/poll` asking for a 60 s wait answers with no assignment once
    /// the 20 s cap elapses, on paused time, and not before it.
    #[tokio::test(start_paused = true)]
    async fn worker_poll_wait_is_capped_at_the_long_poll_cap() {
        let app = test_router(settings());
        let started = tokio::time::Instant::now();
        let response = app
            .oneshot(
                Request::post("/api/v1/agents/worker/poll")
                    .header("authorization", format!("Bearer {WORKER_TOKEN}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "worker_id": "w-unregistered",
                            "handlers": [],
                            "wait_ms": 60_000,
                        })
                        .to_string(),
                    ))
                    .expect("request builds"),
            )
            .await
            .expect("router answers");
        let waited = started.elapsed();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("body is JSON");
        assert_eq!(body["assignment"], serde_json::Value::Null);
        // hygiene-allow: wall-clock -- `started` is tokio's Instant on a
        // start_paused runtime, so `waited` is virtual time advanced only by
        // the poll's own timer, exact on any machine.
        assert_eq!(
            waited,
            std::time::Duration::from_millis(LONG_POLL_CAP_MS),
            "the wait ends at the cap, not at the 60 s asked for"
        );
    }

    /// The per-agent quota defaults equal the kernel's own limits, so the
    /// flags do not advertise a cap the kernel would refuse first.
    #[test]
    fn per_agent_quota_defaults_match_the_kernel_limits() {
        let limits = ravel_agents_core::kernel::Limits::default();
        let defaults = AgentsConfig::default();
        assert_eq!(defaults.max_mailbox, limits.max_mailbox_depth);
        assert_eq!(defaults.max_receipts, limits.max_receipts);
        assert_eq!(defaults.max_timers, limits.max_timers);
    }

    #[test]
    fn readiness_is_not_ready_until_mounted() {
        let readiness = AgentsReadiness::new();
        assert!(!readiness.shards_ready());
        readiness.mark_mounted();
        assert!(readiness.shards_ready());
    }

    #[test]
    fn runtime_enabled_follows_the_mount_flag() {
        let mounted = Arc::new(AgentsReadiness::new());
        let runtime = AgentsRuntime::new(
            settings(),
            mounted.clone(),
            Arc::new(ravel_object_store::memory::MemoryStore::new()),
        );
        assert!(!runtime.enabled());
        mounted.mark_mounted();
        assert!(runtime.enabled());
    }

    /// The health route answers 200 with every setting under its own key.
    #[tokio::test]
    async fn health_reports_feature_mode_and_every_setting() {
        let response = test_router(settings())
            .oneshot(
                Request::get(HEALTH_PATH)
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router answers");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("body is JSON");
        assert_eq!(
            body,
            serde_json::json!({
                "feature": "agents",
                "mode": "query",
                "activation_concurrency": 3,
                "poll_concurrency": 5,
                "memory_budget_bytes": 7,
                "quotas": {
                    "max_agents": 11,
                    "max_mailbox": 13,
                    "max_state_bytes": 17,
                    "max_receipts": 19,
                    "max_timers": 23,
                },
            })
        );
    }
}
