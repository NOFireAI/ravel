//! The Ravel Agents server seam (ADR-3331 decision 9), compiled only with the
//! `agents` cargo feature.
//!
//! `crate::start` mounts [`router`] on the public and the mTLS router from
//! inside its query block when `--agents` is set, feeds the
//! [`AgentsRuntime`]'s counters to `/metrics`, and adds an
//! [`AgentsReadiness`] to `/readyz`. The router serves
//! `GET /api/v1/agents/health`, which reports the settings below, and the
//! client, worker and admin routes, the timer and sweep routes and the
//! inspector page over one [`ravel_agents_runtime::Coordinator`] per tenant.
//! Each tenant's coordinator also runs one bounded sweep pass every
//! `sweep_interval_ms` once the tenant is first served in this process.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::State;
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use ravel_maintain::QueryAuditSink;
use ravel_query::http::TenantResolver;
use serde::Serialize;

use crate::ServerConfig;
use crate::config::Mode;
use crate::health::IngestHealth;

mod audit;
mod routes;
mod runtime;

pub use audit::{KIND_AGENTS_RECONCILE, ReconcileAction, agents_reconcile_audit_event};
pub use routes::{
    Capability, INSPECTOR_CONTENT_TYPE, INSPECTOR_CSP, INSPECTOR_HTML, MAX_EVENTS_PAGE,
    MAX_INSPECT_ATTEMPTS,
};
pub use runtime::{
    AgentsRuntime, EvidenceIndex, LONG_POLL_CAP_MS, Lane, MAX_EVIDENCE_EFFECTS,
    MAX_EVIDENCE_PER_EFFECT, TenantAgents, WorkerDispatcher,
};

/// The unauthenticated settings route.
pub const HEALTH_PATH: &str = "/api/v1/agents/health";

/// Every authenticated route, as `(method, path, capability)`, in mount
/// order. Each handler checks its capability before anything else
/// (`tests::every_route_refuses_a_token_lacking_its_capability`).
pub const ROUTES: &[(&str, &str, Capability)] = &[
    ("POST", "/api/v1/agents/create", Capability::Agents),
    ("POST", "/api/v1/agents/send", Capability::Agents),
    ("POST", "/api/v1/agents/call", Capability::Agents),
    ("GET", "/api/v1/agents/operation", Capability::Agents),
    ("GET", "/api/v1/agents/result", Capability::Agents),
    ("POST", "/api/v1/agents/resolve", Capability::Agents),
    ("POST", "/api/v1/agents/cancel", Capability::Agents),
    ("GET", "/api/v1/agents/state", Capability::Agents),
    ("POST", "/api/v1/agents/events", Capability::Agents),
    ("POST", "/api/v1/agents/timers", Capability::Agents),
    ("POST", "/api/v1/agents/timers/cancel", Capability::Agents),
    ("GET", "/api/v1/agents/timers", Capability::Agents),
    ("GET", "/api/v1/agents/inspector", Capability::Agents),
    (
        "POST",
        "/api/v1/agents/worker/register",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/poll",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/commit",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/heartbeat",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/step",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/effects/complete",
        Capability::AgentsWorker,
    ),
    (
        "POST",
        "/api/v1/agents/worker/effects/fail",
        Capability::AgentsWorker,
    ),
    ("GET", "/api/v1/agents/effects", Capability::Agents),
    ("GET", "/api/v1/agents/effects/{effect}", Capability::Agents),
    ("POST", "/api/v1/agents/reconcile", Capability::AgentsAdmin),
    ("POST", "/api/v1/agents/sweep", Capability::AgentsAdmin),
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
    pub sweep_interval_ms: u64,
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
            sweep_interval_ms: agents.sweep_interval_ms,
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
    sweep_interval_ms: u64,
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
            sweep_interval_ms: settings.sweep_interval_ms,
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
/// listener's own, and the reconcile route writes its audit record to
/// `audit`.
///
/// Handlers read their own body under `max_body_bytes` after admission, so
/// no body-limit layer is mounted: axum's applies only to its body
/// extractors, which no handler uses.
pub fn router(
    runtime: Arc<AgentsRuntime>,
    resolver: Arc<dyn TenantResolver>,
    audit: Arc<dyn QueryAuditSink>,
) -> Router {
    let api = routes::Api {
        runtime,
        resolver,
        audit,
    };
    mounted()
        .into_iter()
        .fold(
            Router::new().route(HEALTH_PATH, get(health)),
            |router, (_, path, handler)| router.route(path, handler),
        )
        .with_state(api)
}

/// Every authenticated route with its handler, in [`ROUTES`] order; the
/// router mounts these and [`HEALTH_PATH`], nothing else.
fn mounted() -> Vec<(&'static str, &'static str, MethodRouter<routes::Api>)> {
    vec![
        ("POST", "/api/v1/agents/create", post(routes::create)),
        ("POST", "/api/v1/agents/send", post(routes::send)),
        ("POST", "/api/v1/agents/call", post(routes::call)),
        ("GET", "/api/v1/agents/operation", get(routes::operation)),
        ("GET", "/api/v1/agents/result", get(routes::result)),
        ("POST", "/api/v1/agents/resolve", post(routes::resolve)),
        ("POST", "/api/v1/agents/cancel", post(routes::cancel)),
        ("GET", "/api/v1/agents/state", get(routes::state)),
        ("POST", "/api/v1/agents/events", post(routes::events)),
        (
            "POST",
            "/api/v1/agents/timers",
            post(routes::timer_schedule),
        ),
        (
            "POST",
            "/api/v1/agents/timers/cancel",
            post(routes::timer_cancel),
        ),
        ("GET", "/api/v1/agents/timers", get(routes::timers)),
        ("GET", "/api/v1/agents/inspector", get(routes::inspector)),
        (
            "POST",
            "/api/v1/agents/worker/register",
            post(routes::worker_register),
        ),
        (
            "POST",
            "/api/v1/agents/worker/poll",
            post(routes::worker_poll),
        ),
        (
            "POST",
            "/api/v1/agents/worker/commit",
            post(routes::worker_commit),
        ),
        (
            "POST",
            "/api/v1/agents/worker/heartbeat",
            post(routes::worker_heartbeat),
        ),
        (
            "POST",
            "/api/v1/agents/worker/step",
            post(routes::worker_step),
        ),
        (
            "POST",
            "/api/v1/agents/worker/effects/complete",
            post(routes::effect_complete),
        ),
        (
            "POST",
            "/api/v1/agents/worker/effects/fail",
            post(routes::effect_fail),
        ),
        ("GET", "/api/v1/agents/effects", get(routes::effects)),
        (
            "GET",
            "/api/v1/agents/effects/{effect}",
            get(routes::effect_inspect),
        ),
        ("POST", "/api/v1/agents/reconcile", post(routes::reconcile)),
        ("POST", "/api/v1/agents/sweep", post(routes::sweep)),
    ]
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
            sweep_interval_ms: 29,
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
        router(
            runtime,
            resolver,
            Arc::new(ravel_maintain::NoopQueryAuditSink),
        )
    }

    /// The router mounts exactly the [`ROUTES`] table: the handler list is
    /// the table, entry for entry, and every entry answers its method
    /// unauthenticated with 401, not 404 or 405.
    #[tokio::test]
    async fn every_route_in_the_table_is_mounted_and_nothing_else() {
        let listed: Vec<(&str, &str)> = mounted().iter().map(|(m, p, _)| (*m, *p)).collect();
        let table: Vec<(&str, &str)> = ROUTES.iter().map(|(m, p, _)| (*m, *p)).collect();
        assert_eq!(listed, table);
        for (method, path, _) in ROUTES {
            let response = test_router(settings())
                .oneshot(route_request(method, path, None))
                .await
                .expect("router answers");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path}"
            );
        }
        let unlisted = test_router(settings())
            .oneshot(
                Request::delete("/api/v1/agents/timers")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("router answers");
        assert_eq!(unlisted.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    fn route_request(method: &str, path: &str, token: Option<&str>) -> Request<Body> {
        let uri = path.replace("{effect}", "not-a-handle");
        let mut builder = Request::builder().method(method).uri(uri.as_str());
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("request builds")
    }

    /// Every row of [`ROUTES`] answers 403 naming its own capability to a
    /// token that holds every agents capability but that one. Each request
    /// carries no body and names no agent, so a handler that read its body
    /// or a store before the check would answer 422 or 404 instead.
    #[tokio::test]
    async fn every_route_refuses_a_token_lacking_its_capability() {
        let mut tokens = std::collections::HashMap::new();
        for (token, agents, agents_worker, agents_admin) in [
            ("no-agents", false, true, true),
            ("no-worker", true, false, true),
            ("no-admin", true, true, false),
        ] {
            tokens.insert(
                token.to_string(),
                ravel_tenant_resolve::Principal {
                    agents,
                    agents_worker,
                    agents_admin,
                    ..ravel_tenant_resolve::Principal::tenant_only(ravel_types::TenantId::new(
                        "acme",
                    ))
                },
            );
        }
        let resolver: Arc<dyn TenantResolver> =
            Arc::new(ravel_tenant_resolve::StaticBearerTokenResolver::with_principals(tokens));
        let app = router(
            Arc::new(AgentsRuntime::new(
                settings(),
                Arc::new(AgentsReadiness::new()),
                Arc::new(ravel_object_store::memory::MemoryStore::new()),
            )),
            resolver,
            Arc::new(ravel_maintain::NoopQueryAuditSink),
        );
        let mut seen = [0usize; 3];
        for (method, path, capability) in ROUTES {
            let (token, slot) = match capability {
                Capability::Agents => ("no-agents", 0),
                Capability::AgentsWorker => ("no-worker", 1),
                Capability::AgentsAdmin => ("no-admin", 2),
            };
            seen[slot] += 1;
            let response = app
                .clone()
                .oneshot(route_request(method, path, Some(token)))
                .await
                .expect("router answers");
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body reads");
            let text = String::from_utf8_lossy(&bytes);
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {text}");
            let needle = format!("requires the {} capability", capability.name());
            assert!(text.contains(&needle), "{method} {path}: {text}");
        }
        assert_eq!(seen, [15, 7, 2], "routes per capability");
    }

    /// The inspector page is the embedded file, served as HTML under a
    /// policy that allows no other origin.
    #[tokio::test]
    async fn inspector_serves_the_embedded_page_as_html() {
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            "client".to_string(),
            ravel_tenant_resolve::Principal {
                agents: true,
                ..ravel_tenant_resolve::Principal::tenant_only(ravel_types::TenantId::new("acme"))
            },
        );
        let resolver: Arc<dyn TenantResolver> =
            Arc::new(ravel_tenant_resolve::StaticBearerTokenResolver::with_principals(tokens));
        let app = router(
            Arc::new(AgentsRuntime::new(
                settings(),
                Arc::new(AgentsReadiness::new()),
                Arc::new(ravel_object_store::memory::MemoryStore::new()),
            )),
            resolver,
            Arc::new(ravel_maintain::NoopQueryAuditSink),
        );
        let response = app
            .oneshot(route_request(
                "GET",
                "/api/v1/agents/inspector",
                Some("client"),
            ))
            .await
            .expect("router answers");
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers().clone();
        assert_eq!(headers["content-type"], INSPECTOR_CONTENT_TYPE);
        assert_eq!(headers["content-security-policy"], INSPECTOR_CSP);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        assert_eq!(&bytes[..], include_str!("inspector.html").as_bytes());
        let page = std::str::from_utf8(&bytes).expect("utf-8 page");
        for id in INSPECTOR_MARKER_IDS {
            assert_eq!(
                page.matches(&format!("id=\"{id}\"")).count(),
                1,
                "marker {id}"
            );
        }
        for reason in [
            "EffectInDoubt",
            "NonDeterministicReplay",
            "IncompatibleHandler",
            "CancelPending",
        ] {
            assert!(
                page.contains(&format!("\"data-reason\": \"{reason}\"")),
                "{reason}"
            );
        }
        assert!(page.contains("/api/v1/agents/reconcile"));
        assert!(page.contains("[\"preview\"]"), "previews are labelled");
        assert!(
            page.contains("el(\"details\""),
            "raw payloads sit in <details>"
        );
        assert!(page.contains("\"Authorization\": \"Bearer \""));
        for external in [
            "<script src",
            "<link",
            "@import",
            "http://",
            "https://",
            "url(",
        ] {
            assert!(!page.contains(external), "external asset: {external}");
        }
    }

    /// Every section and input of the inspector page, by element id.
    const INSPECTOR_MARKER_IDS: [&str; 13] = [
        "inspector-form",
        "inspector-token",
        "inspector-kind",
        "inspector-key",
        "inspector-error",
        "inspector-blocked",
        "inspector-state",
        "inspector-active-turn",
        "inspector-mailbox",
        "inspector-timers",
        "inspector-steps",
        "inspector-effects",
        "inspector-events",
    ];

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
                "sweep_interval_ms": 29,
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
