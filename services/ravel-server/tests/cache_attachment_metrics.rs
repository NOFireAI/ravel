//! Deliverable 4 (issues #445, #502): proves a server built from a
//! cache-enabled `Cli` config actually has the ADR-0046 read cache attached
//! on the metric (PromQL engine) path, not just that construction succeeds.
//!
//! The cache is asserted empty before any query and non-empty after a real
//! instant query over a published RSEG segment: only `SegmentFetcher`'s
//! `guarded_get`, reached through `QueryEngine::with_cache`
//! (`ravel_server::query::build_app_state`), can have populated it.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::{StaticBearerTokenResolver, router};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::Cli;
use ravel_server::query::{build_app_state, build_catalog};
use ravel_server::store::build_cache;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

/// Default-flag `Cli`: the cache is enabled unless `--disable-cache` is
/// passed, so this is already a "cache-enabled config".
fn cache_enabled_cli() -> Cli {
    Cli::try_parse_from(["ravel-server"]).expect("default flags parse")
}

async fn publish_segment(store: &MemoryStore, tenant_hash: TenantHash, now: i64) {
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: "http_requests_total".to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&TenantId::new("acme"), "http_requests_total", &label_set)
            .expect("series id"),
        labels: label_set,
        samples: vec![Sample {
            ts_ns: now - NS_PER_MIN,
            value: 42.0,
        }],
    }];

    let writer_id = Uuid::new_v4();
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
        created_unix_ns: now,
        ingest_hour_bucket: u32::try_from(now / NS_PER_HOUR).expect("hour bucket"),
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

fn encode_query_param(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn call(app: &Router, uri: &str, auth: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {auth}"))
        .body(Body::empty())
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("parse response json");
    (status, json)
}

#[tokio::test]
async fn cache_enabled_config_attaches_cache_to_the_metric_path() {
    let store = MemoryStore::new();
    let tenant = TenantId::new("acme");
    let now = now_ns();
    publish_segment(&store, tenant.hash(), now).await;

    let cli = cache_enabled_cli();
    let cache = build_cache(&cli).expect("cache enabled by default in a cache-enabled config");
    assert!(
        cache.is_empty(),
        "sanity: nothing has been fetched through this cache yet"
    );

    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    // Pass the same cache flags build_cache read above, so build_catalog wires
    // the catalog byte cache from the CLI too (issue #553), not just the
    // fetcher cache. A cache-enabled Cli means the catalog byte cache exists.
    let catalog =
        build_catalog(backend.clone(), 1, cli.disable_cache, cli.cache_max_bytes).expect("catalog");
    assert!(
        catalog.byte_cache_metrics().is_some(),
        "a cache-enabled config must attach the catalog byte cache too (issue #553)"
    );
    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let state = build_app_state(
        catalog,
        backend,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache.clone()),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        None,
    );
    let app: Router = router(state);

    let query = encode_query_param("http_requests_total");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, "acme-token").await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    assert_eq!(body["status"], "success");

    assert!(
        !cache.is_empty(),
        "an instant query over a real segment must populate the cache attached \
         via build_app_state's with_cache call -- an empty cache after a \
         successful query means the metric path never actually consulted it"
    );
}
