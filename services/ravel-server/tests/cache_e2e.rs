//! ADR-0046 / #97 acceptance: `--cache-dir` attaches the local-disk cache tier
//! end to end, and a query is actually served from a disk-tier hit.
//!
//! Two things are proven here, together closing the exact gap #97 names ("a
//! merged, crate-tested cache that no caller constructs"):
//!
//! 1. **Wiring.** `store::build_cache` on a `Cli` carrying `--cache-dir` returns
//!    a [`ReadCache::Tiered`] whose disk-tier metrics handle exists. This is the
//!    RAM-only-until-now constructor `main.rs` calls at startup, so the disk tier
//!    now reaches production automatically.
//!
//! 2. **Serving.** A PromQL instant query, run twice against an engine whose
//!    fetcher holds a RAM-over-disk cache with a RAM tier too small to admit
//!    anything (the `CacheLimits::new(1, 1, 1)` refusal-to-admit technique #95's
//!    `cache_correctness.rs` uses to force reads past RAM), is served from the
//!    disk tier on the second pass: the result is bit-correct AND the disk tier's
//!    `CacheMetrics` records the hit. Asserting only the query result would not
//!    prove the disk tier specifically served it.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use ravel_cache::{Cache, CacheLimits, DiskCache, TieredCache};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::{StaticBearerTokenResolver, router};
use ravel_query::{CacheFetchError, ReadCache};
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
const METRIC_NAME: &str = "http_requests_total";
const SAMPLE_VALUE: f64 = 42.0;

fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

/// Publish one RSEG segment carrying a single `http_requests_total` sample.
async fn publish_segment(store: &MemoryStore, tenant_hash: TenantHash, now: i64) {
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC_NAME.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&TenantId::new("acme"), METRIC_NAME, &label_set)
            .expect("series id"),
        labels: label_set,
        samples: vec![Sample {
            ts_ns: now - NS_PER_MIN,
            value: SAMPLE_VALUE,
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

/// Run the instant query for [`METRIC_NAME`] and return the scalar value string
/// the response carries, asserting the query itself succeeded.
async fn query_metric_value(app: &Router, at_ts_s: i64) -> String {
    let query = encode_query_param(METRIC_NAME);
    let uri = format!("/api/v1/query?query={query}&time={at_ts_s}");
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", "Bearer acme-token")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("parse response json");
    assert_eq!(status, StatusCode::OK, "query failed: {json}");
    assert_eq!(json["status"], "success", "query status: {json}");
    let result = &json["data"]["result"];
    let series = result
        .as_array()
        .and_then(|arr| arr.first())
        .unwrap_or_else(|| panic!("query returned no series: {json}"));
    series["value"][1]
        .as_str()
        .unwrap_or_else(|| panic!("value is not a string: {json}"))
        .to_string()
}

#[tokio::test]
async fn cache_dir_attaches_disk_tier_and_a_query_is_served_from_a_disk_hit() {
    // --- Part 1: the CLI wiring `main.rs` calls at startup attaches a disk tier.
    // `store::build_cache` on a Cli carrying `--cache-dir` must return a tiered
    // cache exposing disk-tier metrics; without #97's wiring this either failed
    // startup (the deleted `--cache-dir` bail) or returned a RAM-only cache.
    let wiring_dir = tempfile::tempdir().expect("temp cache dir");
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--cache-dir",
        wiring_dir.path().to_str().expect("temp path is utf-8"),
    ])
    .expect("flags parse");
    let wired = build_cache(&cli).expect("a cache-enabled config with --cache-dir builds a cache");
    assert!(
        matches!(wired, ReadCache::Tiered(_)),
        "--cache-dir must make store::build_cache return the RAM-over-disk ReadCache::Tiered, \
         not the RAM-only variant"
    );
    assert!(
        wired.disk_metrics().is_some(),
        "the tiered fetcher cache built from --cache-dir must expose a disk-tier metrics handle"
    );

    // --- Part 2: a query is actually served from the disk tier.
    let store = MemoryStore::new();
    let tenant = TenantId::new("acme");
    let now = now_ns();
    publish_segment(&store, tenant.hash(), now).await;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // A RAM tier too small to admit anything (the cache_correctness.rs technique),
    // over a generous disk tier at a real temp `--cache-dir`: every read misses
    // RAM, so once the first query has admitted the byte ranges to disk, the
    // second query is served from the disk tier and nothing else.
    let serve_dir = tempfile::tempdir().expect("temp cache dir");
    let ram = Cache::<CacheFetchError>::new(CacheLimits::new(1, 1, 1));
    let disk = DiskCache::new(
        serve_dir.path().to_path_buf(),
        CacheLimits::new(64 << 20, 1024, 64 << 20),
    );
    let tiered = TieredCache::new(ram, disk);
    let disk_metrics = tiered.disk_metrics();
    let cache = ReadCache::Tiered(Arc::new(tiered));

    let catalog = build_catalog(
        backend.clone(),
        1,
        cli.disable_cache,
        cli.cache_max_bytes,
        cli.cache_dir.clone(),
    )
    .expect("catalog");
    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let state = build_app_state(
        catalog,
        backend,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        None,
        None,
    );
    let app: Router = router(state);
    let at_ts_s = (now - NS_PER_MIN) / NS_PER_SEC;

    // First query: cold cache. Misses RAM (too small) and disk, fetches from the
    // store, admits the byte ranges to the disk tier. No disk hit yet.
    let first = query_metric_value(&app, at_ts_s).await;
    assert_eq!(
        first, "42",
        "the cold query must return the published sample value"
    );
    assert_eq!(
        disk_metrics.snapshot().hits,
        0,
        "the first (cold) query fetches from the store and only admits to disk; it must record no \
         disk hit"
    );

    // Second query, identical: the RAM tier still holds nothing, so the same byte
    // ranges are served from the disk tier. The result must be correct AND the
    // disk tier's own hit counter must have advanced, proving the disk tier
    // served it rather than a re-fetch from the store.
    let second = query_metric_value(&app, at_ts_s).await;
    assert_eq!(
        second, "42",
        "the disk-served query must return the identical correct value"
    );
    assert!(
        disk_metrics.snapshot().hits > 0,
        "the second query must be served FROM the disk tier (a recorded disk hit): a correct \
         result with zero disk hits would not prove the disk tier was reachable at all"
    );
}
