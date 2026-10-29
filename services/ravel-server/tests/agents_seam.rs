//! The Ravel Agents server seam (ADR-3331 decision 9): `--agents` mounts the
//! agents router on both the public and the mTLS listener of a query-serving
//! process, `/metrics` reports the agents settings, and the token suffix grants
//! the agents capabilities through the resolver chain.

#![cfg(feature = "agents")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue};
use clap::Parser;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{AgentsConfig, AuthResolverSettings};
use ravel_server::tenant::OidcCapabilityClaims;
use ravel_server::{Cli, FoldTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_types::TenantId;

const HEALTH_PATH: &str = "/api/v1/agents/health";

fn agents_config() -> AgentsConfig {
    AgentsConfig {
        enabled: true,
        activation_concurrency: 3,
        poll_concurrency: 5,
        memory_budget_bytes: 7_000,
        max_agents: 11,
        max_mailbox: 13,
        max_state_bytes: 17_000,
        max_receipts: 19,
        max_timers: 23,
        ..AgentsConfig::default()
    }
}

/// A `--mode all` process with both listeners bound, the agents surface set
/// to `agents`.
async fn start_server(agents: AgentsConfig) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert("public-token".to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let mut mtls_tokens = HashMap::new();
    mtls_tokens.insert("mtls-token".to_string(), TenantId::new("beta"));
    let mtls_resolver = ravel_server::tenant::build_resolver(mtls_tokens, false);

    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            agents,
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: Some(ravel_server::MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver: mtls_resolver,
        }),
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// `http://` URLs for the agents health route on the public and the mTLS
/// listener (the mTLS listener speaks plain HTTP behind its terminator).
fn health_urls(running: &ravel_server::Running) -> [String; 2] {
    let mtls = running
        .mtls_addr
        .expect("an mTLS listener was configured on a query-serving mode");
    [
        format!("http://{}{HEALTH_PATH}", running.http_addr),
        format!("http://{mtls}{HEALTH_PATH}"),
    ]
}

/// ADR-3331 decision 9: with the feature and `--agents`, the agents router is
/// mounted on both the public and the mTLS router; without `--agents` it is
/// mounted on neither. A mount on the public router alone fails the mTLS
/// assertion, and a mount that ignores the flag fails the 404 half.
#[tokio::test]
async fn agents_router_mounts_on_both_listeners_only_with_feature_and_flag() {
    let client = reqwest::Client::new();

    let running = start_server(agents_config()).await;
    for url in health_urls(&running) {
        let response = client.get(&url).send().await.expect("request completes");
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{url}");
        let body: serde_json::Value = response.json().await.expect("JSON body");
        assert_eq!(
            body,
            serde_json::json!({
                "feature": "agents",
                "mode": "all",
                "activation_concurrency": 3,
                "poll_concurrency": 5,
                "memory_budget_bytes": 7000,
                "sweep_interval_ms": 5000,
                "quotas": {
                    "max_agents": 11,
                    "max_mailbox": 13,
                    "max_state_bytes": 17000,
                    "max_receipts": 19,
                    "max_timers": 23,
                },
                "projection": {
                    "mode": "in_process",
                    "endpoint": null,
                    "backlog_bound": 4096,
                    "lag_max": 0,
                    "backlog": 0,
                    "events_projected": 0,
                    "spans_projected": 0,
                    "sink_failures": 0,
                    "backlog_full": 0,
                    "read_failures": 0,
                    "records_rejected": 0,
                    "last_sink_error": null,
                    "last_rejection": null,
                },
            }),
            "{url}"
        );
    }
    running.shutdown().await.expect("clean shutdown");

    let running = start_server(AgentsConfig::default()).await;
    for url in health_urls(&running) {
        let response = client.get(&url).send().await.expect("request completes");
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{url}");
    }
    running.shutdown().await.expect("clean shutdown");
}

/// Every `ravel_agents_*` family the scrape carries, with the value the
/// settings above produce.
const AGENTS_FAMILIES: [(&str, u64); 10] = [
    ("ravel_agents_enabled", 1),
    ("ravel_agents_activation_concurrency_limit", 3),
    ("ravel_agents_poll_concurrency_limit", 5),
    ("ravel_agents_memory_budget_bytes", 7_000),
    ("ravel_agents_max_agents", 11),
    ("ravel_agents_max_mailbox", 13),
    ("ravel_agents_max_state_bytes", 17_000),
    ("ravel_agents_max_receipts", 19),
    ("ravel_agents_max_timers", 23),
    ("ravel_agents_sweep_interval_ms", 5_000),
];

/// A real `/metrics` scrape carries each agents family exactly once: one
/// HELP, one TYPE gauge, and one sample labelled with the process mode and
/// carrying the configured value. Without `--agents` the same families are
/// present and `ravel_agents_enabled` reads 0.
#[tokio::test]
async fn metrics_scrape_reports_every_agents_family_once_with_the_mode_label() {
    let client = reqwest::Client::new();
    for (agents, enabled) in [(agents_config(), 1), (AgentsConfig::default(), 0)] {
        let defaults = !agents.enabled;
        let running = start_server(agents).await;
        let body = client
            .get(format!("http://{}/metrics", running.http_addr))
            .send()
            .await
            .expect("scrape completes")
            .text()
            .await
            .expect("scrape body");
        for (name, value) in AGENTS_FAMILIES {
            assert_eq!(
                body.matches(&format!("# HELP {name} ")).count(),
                1,
                "{name} HELP"
            );
            assert_eq!(
                body.matches(&format!("# TYPE {name} gauge\n")).count(),
                1,
                "{name} TYPE"
            );
            let samples: Vec<&str> = body
                .lines()
                .filter(|line| line.starts_with(&format!("{name}{{")) || *line == name)
                .collect();
            assert_eq!(samples.len(), 1, "{name} samples: {samples:?}");
            if name == "ravel_agents_enabled" {
                assert_eq!(samples[0], format!("{name}{{mode=\"all\"}} {enabled}"));
            } else if !defaults {
                assert_eq!(samples[0], format!("{name}{{mode=\"all\"}} {value}"));
            } else {
                assert!(
                    samples[0].starts_with(&format!("{name}{{mode=\"all\"}} ")),
                    "{name}: {}",
                    samples[0]
                );
            }
        }
        running.shutdown().await.expect("clean shutdown");
    }
}

fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("header value"),
    );
    headers
}

/// A `;agents_worker` token suffix, parsed by `--tenant-token` and resolved
/// through the same resolver chain `main` builds, yields a principal with
/// `agents_worker` and no other capability.
#[test]
fn agents_worker_token_suffix_resolves_only_the_worker_capability() {
    let cli = Cli::try_parse_from(["ravel-server", "--tenant-token", "dev=acme;agents_worker"])
        .expect("flags parse");
    let principals = cli
        .parse_tenant_principals()
        .expect("an agents_worker suffix parses");
    let bundle = ravel_server::tenant::build_auth_resolver_with_principals(
        principals,
        false,
        AuthResolverSettings {
            oidc: None,
            mtls_header: None,
        },
        OidcCapabilityClaims::default(),
    )
    .expect("resolver chain builds");
    let principal = bundle
        .resolver
        .resolve_principal(&bearer("dev"))
        .expect("the token resolves");
    assert_eq!(principal.tenant, TenantId::new("acme"));
    assert!(principal.agents_worker);
    assert!(!principal.agents);
    assert!(!principal.agents_admin);
    assert!(!principal.ddl);
    assert!(!principal.ipcrypt);
}
