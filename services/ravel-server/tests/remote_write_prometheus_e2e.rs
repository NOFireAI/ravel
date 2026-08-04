//! Real-Prometheus-in-Docker e2e for `POST /api/v1/write` (ADR-0015,
//! docs/ingest-breadth-plan.md Ticket A3).
//!
//! This is the "against a real sender" half of A3's acceptance criteria: a
//! pinned Prometheus binary, remote-writing both protocol versions, into an
//! in-process `ravel-server` backed by `MemoryStore`, with the samples read
//! back via `/api/v1/query` using the returned commit token.
//!
//! `#[ignore]`d by default because it needs a working Docker daemon (the
//! executing user in the docker group or root, a reachable
//! `/var/run/docker.sock`) and the ability to pull or already have
//! `prom/prometheus:v3.0.0` cached locally. Run explicitly with:
//!
//! ```text
//! cargo test -p ravel-server --test remote_write_prometheus_e2e -- --ignored
//! ```
//!
//! This was written but never executed in the environment this ticket was
//! implemented in: the fleet executor's user was not in the `docker` group
//! (permission denied on the socket, no passwordless sudo available) and
//! the host has no general network egress to pull the image. See the
//! implementation report for A3 for the full detail. The in-process
//! `tests/remote_write.rs` suite covers the same decode/normalize/strict
//! commit/read-your-write path with real wire-format (snappy + protobuf)
//! payloads and does run in this environment; this file is the piece that
//! substitutes a synthetic sender for a real one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Write;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const PROMETHEUS_IMAGE: &str = "prom/prometheus:v3.0.0";

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode: Mode::All,
        // Bound to loopback but reachable from the Prometheus container via
        // `--network host`, which puts the container on the host's network
        // namespace (Linux only; this test does not attempt a Docker
        // Desktop / host.docker.internal fallback).
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

/// A Prometheus config that scrapes Prometheus's own `/metrics` (so there is
/// at least one real series flowing through the pipeline without standing
/// up a separate exporter) and remote-writes every sample to `ravel-server`
/// using `protobuf_message` to pin the wire version under test.
fn prometheus_config(write_port: u16, protobuf_message: &str) -> String {
    format!(
        r#"
global:
  scrape_interval: 1s
  evaluation_interval: 1s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

remote_write:
  - url: 'http://127.0.0.1:{write_port}/api/v1/write'
    protobuf_message: {protobuf_message}
    authorization:
      credentials: '{TOKEN}'
    queue_config:
      max_shards: 1
      min_backoff: 100ms
      max_backoff: 200ms
"#
    )
}

/// Runs one Prometheus-in-Docker remote-write cycle against `ravel-server`
/// and asserts the scraped `up` series becomes queryable.
///
/// Kept as a single helper (rather than one test per protocol version) so
/// both versions share the same container lifecycle handling; the protocol
/// version is a parameter.
async fn run_against_real_prometheus(protobuf_message: &str, metric_query: &str) {
    let running = start_test_server().await;
    let write_port = running.http_addr.port();

    let config_dir = tempfile_dir();
    let config_path = config_dir.join("prometheus.yml");
    std::fs::File::create(&config_path)
        .expect("create prometheus.yml")
        .write_all(prometheus_config(write_port, protobuf_message).as_bytes())
        .expect("write prometheus.yml");

    let container_name = format!(
        "ravel-rw-e2e-{}",
        std::process::id() // unique enough for one test process; not a security identifier
    );

    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "--name",
            &container_name,
            "-v",
            &format!(
                "{}:/etc/prometheus/prometheus.yml:ro",
                config_path.display()
            ),
            PROMETHEUS_IMAGE,
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.retention.time=1h",
        ])
        .status()
        .expect("docker must be runnable in an environment that executes this ignored test");
    assert!(status.success(), "docker run failed to start Prometheus");

    // Give Prometheus a few scrape+remote-write cycles to land a sample.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = reqwest::Client::new();
    let query_response = client
        .get(format!("http://{}/api/v1/query", running.http_addr))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", metric_query)])
        .send()
        .await
        .expect("query request succeeds");

    let _ = Command::new("docker")
        .args(["stop", &container_name])
        .status();

    assert_eq!(
        query_response.status(),
        200,
        "query for a real-Prometheus-remote-written series should succeed"
    );
    let body: serde_json::Value = query_response.json().await.expect("query response is JSON");
    let result = body["data"]["result"]
        .as_array()
        .expect("result is an array");
    assert!(
        !result.is_empty(),
        "expected at least one series remote-written by real Prometheus: {body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ravel-rw-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    dir
}

#[tokio::test]
#[ignore = "requires a Docker daemon reachable by this user and the prom/prometheus:v3.0.0 image"]
async fn real_prometheus_rw1_remote_write_is_queryable() {
    run_against_real_prometheus("prometheus.WriteRequest", "up").await;
}

#[tokio::test]
#[ignore = "requires a Docker daemon reachable by this user and the prom/prometheus:v3.0.0 image"]
async fn real_prometheus_rw2_remote_write_is_queryable() {
    run_against_real_prometheus("io.prometheus.write.v2.Request", "up").await;
}
