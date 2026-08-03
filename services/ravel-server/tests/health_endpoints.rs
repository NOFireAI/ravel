//! Liveness (`/healthz`) and readiness (`/readyz`) routes (ADR-0034 decision
//! 4). Covers: `/healthz` is 200 once serving; `/readyz` is 503 before the
//! readiness latch flips and 200 after full startup, in every mode; and the
//! regression guard that maintain mode (whose router was previously empty)
//! serves both routes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_object_store::memory::MemoryStore;
use ravel_server::health::{self, Readiness};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use tower::ServiceExt;

const TOKEN: &str = "testtoken";

/// Start an in-process server backed by `MemoryStore` in the given mode.
/// Mirrors `integration.rs`'s helper but is parameterized by mode so the
/// readiness assertion can run against each one. `start` binds the listeners
/// and latches readiness before returning, so `/readyz` is already 200 by the
/// time this returns; the pre-ready 503 window is exercised at router level in
/// `readyz_returns_503_before_ready_and_200_after_startup` instead.
async fn start_test_server(mode: Mode) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        limits: ravel_server::LimitsConfig::default(),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
    )
    .await
    .expect("server starts")
}

/// Drive one GET through a `Router` via `oneshot`, returning the status and
/// body bytes. No real socket: exercises the routed handlers directly.
async fn get(app: &Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec();
    (status, bytes)
}

#[tokio::test]
async fn healthz_returns_ok_once_serving() {
    let running = start_test_server(Mode::All).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("healthz request completes");

    assert_eq!(
        response.status(),
        200,
        "healthz must be 200 once the listener is serving"
    );
    let body = response.text().await.expect("healthz body is text");
    assert_eq!(body, "ok", "healthz returns a minimal ok body");

    running.shutdown().await.expect("graceful shutdown");
}

/// The readiness latch is flipped inside `start`, before it returns a
/// `Running`, so the pre-ready 503 window is not observable from a started
/// server without a startup hook that holds the server at a pre-ready point.
/// No such hook exists in this crate's test utilities, and building one is out
/// of scope for this task (issue #246). So this test uses approach (b) from
/// the task, strengthened: it drives the real `health::router` handlers
/// through the actual axum router (not merely the atomic in isolation) with
/// the readiness flag both false and true, then confirms a fully started
/// server answers `/readyz` with 200 in every mode.
#[tokio::test]
async fn readyz_returns_503_before_ready_and_200_after_startup() {
    // Pre-ready window, at router level: the exact handler and route the
    // server mounts, with the flag not yet latched.
    let readiness = Readiness::new();
    let app = health::router(readiness.clone());

    let (status, _) = get(&app, "/readyz").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "readyz must be 503 before startup completes"
    );

    // Latch readiness the same way `start` does, then re-probe the same
    // router: the shared flag makes the already-mounted handler flip.
    readiness.mark_ready();
    let (status, _) = get(&app, "/readyz").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "readyz must be 200 after readiness is latched"
    );

    // Full startup, over a real socket, in every mode: `start` latches
    // readiness before returning, so `/readyz` is 200 by the time the server
    // is reachable.
    for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
        let running = start_test_server(mode).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .expect("readyz request completes");
        assert_eq!(
            response.status(),
            200,
            "readyz must be 200 after full startup in mode {mode:?}"
        );
        running.shutdown().await.expect("graceful shutdown");
    }
}

/// Regression guard for the previously-empty maintain-mode router (ADR-0034
/// decision 4). Maintain mode used to serve no routes, so `--listen-http`
/// "liveness" meant only a TCP accept. Both health routes must now return real
/// (non-404) responses. If a future change reverts maintain to an empty
/// router, both assertions below fail (an empty router answers 404).
#[tokio::test]
async fn maintain_mode_serves_health_routes() {
    let running = start_test_server(Mode::Maintain).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let healthz = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("healthz request completes");
    assert_ne!(
        healthz.status(),
        404,
        "maintain mode must serve /healthz, not an empty router"
    );
    assert_eq!(healthz.status(), 200, "maintain /healthz is 200");

    let readyz = client
        .get(format!("{base}/readyz"))
        .send()
        .await
        .expect("readyz request completes");
    assert_ne!(
        readyz.status(),
        404,
        "maintain mode must serve /readyz, not an empty router"
    );
    assert_eq!(
        readyz.status(),
        200,
        "maintain /readyz is 200 after full startup"
    );

    running.shutdown().await.expect("graceful shutdown");
}
