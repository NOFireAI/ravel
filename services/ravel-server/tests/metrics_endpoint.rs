//! `GET /metrics` is served on the HTTP listener in every mode, including
//! `Mode::Maintain` (issue #423), the same regression shape as
//! `health_endpoints.rs`'s maintain-mode guard.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// Mirrors `health_endpoints.rs`'s helper: an in-process server backed by
/// `MemoryStore`, parameterized by mode.
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
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
    };
    ravel_server::start(config, store, Arc::new(StoreMetrics::default()))
        .await
        .expect("server starts")
}

/// Regression guard for issue #423: `/metrics` must be served in every mode,
/// maintain included, where before this change only `/healthz` and `/readyz`
/// existed.
#[tokio::test]
async fn metrics_served_in_every_mode() {
    for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
        let running = start_test_server(mode).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics request completes");
        assert_eq!(
            response.status(),
            200,
            "metrics must be 200 in mode {mode:?}"
        );

        let content_type = response
            .headers()
            .get("content-type")
            .expect("content-type header present")
            .to_str()
            .expect("content-type is ASCII");
        assert!(
            content_type.starts_with("text/plain"),
            "metrics content-type should be text/plain, got {content_type}"
        );

        let body = response.text().await.expect("metrics body is text");
        assert!(
            body.contains("ravel_store_calls_total"),
            "metrics body missing store family in mode {mode:?}:\n{body}"
        );
        assert!(
            body.contains("ravel_catalog_interlock_violations_total"),
            "metrics body missing catalog family in mode {mode:?}:\n{body}"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}

/// `Mode::All` and `Mode::Gateway` build ingest routers; `Mode::Query` and
/// `Mode::Maintain` do not. The ingest metric families must appear exactly
/// where the routers exist, not be zero-padded in modes that build none.
#[tokio::test]
async fn metrics_ingest_family_present_only_in_ingest_modes() {
    for (mode, expect_ingest) in [
        (Mode::All, true),
        (Mode::Gateway, true),
        (Mode::Query, false),
        (Mode::Maintain, false),
    ] {
        let running = start_test_server(mode).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();

        let body = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics request completes")
            .text()
            .await
            .expect("metrics body is text");

        assert_eq!(
            body.contains("ravel_ingest_flushes_by_size_total"),
            expect_ingest,
            "mode {mode:?} ingest family presence should be {expect_ingest}:\n{body}"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}
