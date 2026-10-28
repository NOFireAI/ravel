//! The Ravel Agents server seam (ADR-3331 decision 9), compiled only with the
//! `agents` cargo feature.
//!
//! `crate::start` mounts [`router`] on the public and the mTLS router from
//! inside its query block when `--agents` is set, feeds an
//! [`AgentsMetricsSnapshot`] to `/metrics`, and adds an [`AgentsReadiness`]
//! to `/readyz`. The only route is `GET /api/v1/agents/health`, which reports
//! the settings below; the coordinator, store and worker routes that act on
//! them arrive in later tasks of epic #3331.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::ServerConfig;
use crate::config::Mode;
use crate::health::IngestHealth;

/// The one route this release serves.
pub const HEALTH_PATH: &str = "/api/v1/agents/health";

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

/// What `/metrics` renders for the agents surface: the settings, and whether
/// the router is mounted, read live from the same flag `/readyz` reads.
#[derive(Debug, Clone)]
pub struct AgentsMetricsSnapshot {
    pub settings: AgentsSettings,
    mounted: Arc<AgentsReadiness>,
}

impl AgentsMetricsSnapshot {
    pub fn new(settings: AgentsSettings, mounted: Arc<AgentsReadiness>) -> Self {
        Self { settings, mounted }
    }

    /// True once the agents router is mounted in this process.
    pub fn enabled(&self) -> bool {
        self.mounted.is_mounted()
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

/// `GET /api/v1/agents/health`. It reads no store and no tenant data, only
/// operator settings `/metrics` also exposes, so it carries no authentication
/// on either listener, including the mTLS one, where `/healthz` itself is not
/// served (ADR-3331 decision 9 mounts it on both).
pub fn router(settings: AgentsSettings) -> Router {
    Router::new()
        .route(HEALTH_PATH, get(health))
        .with_state(settings)
}

async fn health(State(settings): State<AgentsSettings>) -> Json<HealthBody> {
    Json(HealthBody::new(&settings))
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
        }
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
    fn snapshot_enabled_follows_the_mount_flag() {
        let mounted = Arc::new(AgentsReadiness::new());
        let snapshot = AgentsMetricsSnapshot::new(settings(), mounted.clone());
        assert!(!snapshot.enabled());
        mounted.mark_mounted();
        assert!(snapshot.enabled());
    }

    /// The health route answers 200 with every setting under its own key.
    #[tokio::test]
    async fn health_reports_feature_mode_and_every_setting() {
        let response = router(settings())
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
