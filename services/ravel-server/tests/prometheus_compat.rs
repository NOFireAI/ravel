//! Prometheus HTTP API compatibility routes (issue #336): the four routes
//! Grafana's built-in Prometheus datasource probes but that carry no query
//! semantics.
//!
//! `/api/v1/status/buildinfo` and `/api/v1/metadata` come from
//! `ravel_query::http::router`; `/-/healthy` and `/-/ready` from
//! `ravel_server::health::router`. Both are merged into the server's HTTP
//! router, so this drives all four against a real started server rather than
//! against a hand-built router, which also proves the merge wiring is there.

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

/// An in-process server on `MemoryStore` in `Mode::All`, the mode that mounts
/// both the query router and the health router.
async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::All,
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
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// Drive one GET through a `Router` via `oneshot`, returning status and body
/// bytes. No socket: the routed handlers run directly.
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
async fn buildinfo_reports_ravels_own_version_not_a_prometheus_one() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/status/buildinfo"))
        .send()
        .await
        .expect("buildinfo request completes");
    assert_eq!(response.status(), 200, "buildinfo must be 200");

    let body: serde_json::Value = response.json().await.expect("buildinfo body is JSON");
    assert_eq!(body["status"], "success");

    let data = &body["data"];
    // The version must be Ravel's own crate version. Every workspace crate,
    // ravel-server included, inherits the single `[workspace.package]
    // version`, so the server's own CARGO_PKG_VERSION is the right oracle
    // here even though the handler lives in ravel-query.
    assert_eq!(
        data["version"],
        serde_json::json!(env!("CARGO_PKG_VERSION")),
        "buildinfo.version must be ravel-server's own crate version"
    );
    assert!(
        !env!("CARGO_PKG_VERSION").is_empty(),
        "the version reported must be a real, non-empty version"
    );

    // Fields Ravel has no value for are present and empty, not absent: a
    // Prometheus client reads them unconditionally.
    for field in ["branch", "buildUser", "buildDate", "goVersion"] {
        assert_eq!(
            data[field],
            serde_json::json!(""),
            "{field} must be present and empty, never invented"
        );
    }
    assert!(
        data["revision"].is_string(),
        "revision is a string (the git SHA when the build supplied one, else empty)"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn metadata_returns_a_success_envelope_with_an_empty_data_object() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/metadata"))
        .send()
        .await
        .expect("metadata request completes");
    assert_eq!(response.status(), 200, "metadata must be 200, not 404");

    let body: serde_json::Value = response.json().await.expect("metadata body is JSON");
    assert_eq!(body["status"], "success");
    // Empty, because Ravel captures no OTLP metric type/help/unit metadata.
    // An empty object is a valid Prometheus response; fabricated entries
    // would not be.
    assert_eq!(
        body["data"],
        serde_json::json!({}),
        "data must be an empty JSON object"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The Prometheus-spelled aliases must be the same handlers, not a second
/// implementation: byte-identical bodies and identical statuses for the same
/// readiness state, checked in both states through the real health router.
#[tokio::test]
async fn prometheus_health_aliases_match_healthz_and_readyz_exactly() {
    for mark_ready in [false, true] {
        let readiness = Readiness::new();
        if mark_ready {
            readiness.mark_ready();
        }
        let app = health::router(readiness);

        let (healthz_status, healthz_body) = get(&app, "/healthz").await;
        let (healthy_status, healthy_body) = get(&app, "/-/healthy").await;
        assert_eq!(
            healthy_status, healthz_status,
            "/-/healthy status must match /healthz (ready={mark_ready})"
        );
        assert_eq!(
            healthy_body, healthz_body,
            "/-/healthy body must be byte-identical to /healthz (ready={mark_ready})"
        );
        assert_eq!(healthz_status, StatusCode::OK, "liveness is always 200");

        let (readyz_status, readyz_body) = get(&app, "/readyz").await;
        let (ready_status, ready_body) = get(&app, "/-/ready").await;
        assert_eq!(
            ready_status, readyz_status,
            "/-/ready status must match /readyz (ready={mark_ready})"
        );
        assert_eq!(
            ready_body, readyz_body,
            "/-/ready body must be byte-identical to /readyz (ready={mark_ready})"
        );
        let expected = if mark_ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        assert_eq!(readyz_status, expected, "readiness state drives the status");
    }
}

/// The aliases are reachable on a real started server too, not only on a
/// hand-built health router: this is the check that the merge wiring in
/// `ravel_server::start` carries them.
#[tokio::test]
async fn health_aliases_are_served_by_a_started_server() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    for path in ["/-/healthy", "/-/ready"] {
        let response = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .expect("alias request completes");
        assert_eq!(response.status(), 200, "{path} must be 200 after startup");
    }

    running.shutdown().await.expect("graceful shutdown");
}
