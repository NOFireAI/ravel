//! EF-3 (issue #723), ADR-0061 decision 1: the per-query bytes-scanned budget
//! is reachable end-to-end from an operator's `--limits-file` through to a real
//! HTTP query response, not merely present in the engine.
//!
//! This drives the full wiring the way an operator and a client actually would:
//! a real limits file on disk is parsed through the real `--limits-file` flag
//! (`Cli::parse_limits_file`), fed into a real `ravel_server::start`ed server
//! bound to a real loopback port, and queried over real HTTP with `reqwest`.
//! A real RSEG segment and commit record are published to a `MemoryStore`, so
//! the query issues genuine object-store GETs whose bytes trip the tiny budget.
//! The assertion is on the exact HTTP status (422) and JSON body
//! (`errorType: "execution"`, the `TooManyBytesScanned` message) that
//! `ravel_query::http::error` maps `QueryError::TooManyBytesScanned` to, so the
//! test fails if any layer of the CLI -> config -> EngineConfig -> HTTP chain
//! drops the budget or mismaps the error.
//!
//! EF-1 already proved the engine cancels mid-scan in isolation; this proves an
//! operator can actually turn that enforcement on and observe it from a client.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Cli, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "m";

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// Publish one real RSEG segment plus its commit record for `tenant`, anchored
/// at `base_ns`, so a query over a window covering it resolves, opens the
/// segment, and issues a real GET whose bytes are charged against the budget.
/// Mirrors `query_cost_surfaces.rs`'s helper (the same shape `tests/e2e.rs`
/// uses): data a few minutes in the past, queried over a recent window, keeps
/// the catalog listing to a handful of hour buckets (issue #635).
async fn publish_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, base_ns: i64) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![
            Sample {
                ts_ns: base_ns + 100_000_000,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + 200_000_000,
                value: 2.5,
            },
        ],
    }];

    let writer_id = uuid::Uuid::from_u128(4_000);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base_ns + 300_000_000,
        ingest_hour_bucket: u32::try_from(base_ns / NS_PER_HOUR).expect("hour bucket fits u32"),
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// A `LimitsConfig` produced by the real `--limits-file` flag over a file on
/// disk holding a tiny `[defaults] max_bytes_scanned`. Going through `Cli`
/// exercises the exact CLI -> file -> parse path an operator uses, not a
/// hand-built config, so the test proves the flag is wired, not just the
/// in-memory struct.
fn limits_from_file(toml_body: &str) -> ravel_server::LimitsConfig {
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "ravel_query_byte_budget_{}.toml",
        std::process::id()
    ));
    std::fs::write(&path, toml_body).expect("write limits file");
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--limits-file",
        path.to_str().expect("utf8 path"),
    ])
    .expect("flags parse");
    let limits = cli.parse_limits_file().expect("limits file parses");
    let _ = std::fs::remove_file(&path);
    limits
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    limits: ravel_server::LimitsConfig,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
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
        limits,
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

/// A tiny `[defaults] max_bytes_scanned` configured via the limits file trips
/// on a real PromQL range query over published data and surfaces as HTTP 422
/// with the `TooManyBytesScanned` execution error. The budget of 1 byte is
/// smaller than any real segment, so the first completed segment fetch already
/// exceeds it and the engine cancels the query.
#[tokio::test]
async fn a_tiny_configured_budget_cancels_a_real_promql_query_over_http() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (start, end) = ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC);

    // The budget lives in [defaults]: the process-wide engine both query
    // surfaces share enforces the default budget for every tenant, so a tiny
    // default is what actually trips a query for the test tenant.
    let limits = limits_from_file("[defaults]\nmax_bytes_scanned = 1\n");
    assert_eq!(
        limits.query_defaults.max_bytes_scanned,
        ravel_query::ByteLimit::Bounded(1),
        "sanity: the file's tiny budget parsed"
    );

    let running = start_server(Arc::clone(&store), limits).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", METRIC.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query request completes");

    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("response body is JSON");

    // The exact mapping ravel_query/src/http/error.rs applies:
    // QueryError::TooManyBytesScanned -> ApiError::Unsupported ->
    // 422 UNPROCESSABLE_ENTITY, errorType "execution", the error's own Display.
    assert_eq!(
        status, 422,
        "a tripped byte budget must surface as HTTP 422, got {status}: {body}"
    );
    assert_eq!(
        body["status"], "error",
        "Prometheus-shaped error envelope: {body}"
    );
    assert_eq!(
        body["errorType"], "execution",
        "TooManyBytesScanned maps to the 422 execution error type, not a generic error: {body}"
    );
    let msg = body["error"].as_str().expect("error message is a string");
    assert!(
        msg.contains("exceeding the budget"),
        "the body must carry the TooManyBytesScanned message, got: {msg}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
