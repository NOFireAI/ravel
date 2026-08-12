//! Item 2 (issue #913): the HTTP query handler merges a federated fan-out's
//! partial-coverage warnings into the Prometheus response envelope's top-level
//! `warnings` array.
//!
//! Every other `ravel-server` handler test builds its `AppState` with
//! `remote_clusters: Vec::new()` (no federation), so the warning-merge loops in
//! `crates/ravel-query/src/http/handlers.rs` (`handle_query` and
//! `handle_query_range`) have no end-to-end coverage. This test is the first to
//! drive the real router with a non-empty federation: a real
//! `RemoteClusterConfig` -> `FederationSliceFetcher` -> `Federation`, attached
//! to the engine through the same `build_app_state` seam `ravel_server::start`
//! uses, over an unavailable remote with `skip_unavailable = true`. The remote
//! is skipped (a partial-coverage warning naming the cluster), and the assertion
//! is that the warning reaches the client through the response envelope's
//! `warnings` array -- the surface the handler's merge loop populates.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::EngineConfig;
use ravel_query::distrib::{Federation, RemoteCluster};
use ravel_query::http::{StaticBearerTokenResolver, router as promql_router};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::RemoteClusterConfig;
use ravel_server::metrics::QueryAccountingMetrics;
use ravel_server::query::{build_app_state, build_catalog};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tower::ServiceExt;
use uuid::Uuid;

const METRIC: &str = "http_requests_total";
const OPERATOR_CRED: &str = "operator-east-credential";
const TOKEN: &str = "acme-token";

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

/// Publish one real RSEG segment (one series, one sample) for `tenant`, anchored
/// at `base_ns`, so a recent instant query resolves and opens the segment.
async fn publish_series(store: &dyn ObjectStoreBackend, tenant: &TenantId, base_ns: i64) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![Sample {
            ts_ns: base_ns,
            value: 1.0,
        }],
    }];

    let writer_id = Uuid::from_u128(1);
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
        created_unix_ns: base_ns + NS_PER_MIN,
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

/// An address bound then immediately released: a connection to it is refused, so
/// a remote pointed here is unavailable at query time.
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind dead");
    let addr = listener.local_addr().expect("dead local addr");
    drop(listener);
    addr.to_string()
}

/// A `Federation` over one unavailable remote cluster with `skip_unavailable =
/// true`, dialed by the production `FederationSliceFetcher` exactly as
/// `--remote-cluster` wires it.
fn dead_federation(name: &str, endpoint: &str) -> Federation {
    let config = RemoteClusterConfig {
        name: name.to_string(),
        endpoint: endpoint.to_string(),
        credential: OPERATOR_CRED.to_string(),
        tls: false,
        tls_ca_file: None,
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    };
    let fetcher = ravel_server::distrib::FederationSliceFetcher::connect(&config)
        .expect("federation client connects lazily");
    Federation::new(vec![RemoteCluster {
        name: name.to_string(),
        fetcher: Arc::new(fetcher),
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    }])
}

/// The PromQL router wired with a federation over one dead, skippable remote,
/// exactly as `ravel_server::start` assembles it (through `build_app_state`).
async fn promql_with_federation(store: Arc<dyn ObjectStoreBackend>, tenant: &TenantId) -> Router {
    let catalog = build_catalog(store.clone(), 1, true, 0).expect("catalog");
    let mut tokens = std::collections::HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let federation = Arc::new(dead_federation("west", &dead_endpoint().await));

    let app_state = build_app_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        EngineConfig::default(),
        Arc::new(QueryAccountingMetrics::new(HashSet::new())),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Some(federation),
    );
    promql_router(app_state)
}

async fn get(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("body not JSON ({e}): {}", String::from_utf8_lossy(&bytes)));
    (status, value)
}

/// The response envelope's top-level `warnings` array carries the federation
/// partial-coverage warning naming the skipped cluster, for both `/query` and
/// `/query_range` -- the two handler paths whose merge loops
/// (`handlers.rs::handle_query` / `handle_query_range`) fold `stats.warnings`
/// into the envelope.
///
/// Flip-line proof (the prove-the-test evidence): delete the
/// `for w in &stats.warnings { ... warnings.push(w.clone()); }` loop in
/// `handle_query` (and, for the range assertion, the matching loop in
/// `handle_query_range`) in `crates/ravel-query/src/http/handlers.rs`. With the
/// loop gone the response's `warnings` array no longer carries the federation
/// warning and the `contains("west")` assertions below fail; restoring the loop
/// makes them pass again. The query still succeeds (skip_unavailable=true), so
/// only the client-facing partial-coverage signal is lost -- exactly the
/// property this test pins.
#[tokio::test]
async fn federation_warning_reaches_the_query_envelope_warnings() {
    let base = now_ns() - 2 * NS_PER_MIN;
    let tenant = TenantId::new("acme");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(store.as_ref(), &tenant, base).await;
    let app = promql_with_federation(Arc::clone(&store), &tenant).await;

    let now_s = now_ns() / NS_PER_SEC;

    // Instant query.
    let (status, body) = get(&app, &format!("/api/v1/query?query={METRIC}&time={now_s}")).await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    let warnings = body["warnings"]
        .as_array()
        .expect("the envelope must carry a warnings array when a remote is skipped");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().map(|s| s.contains("west")).unwrap_or(false)),
        "the skipped federation cluster must be named in the response warnings: {warnings:?}"
    );

    // Range query: same merge loop in handle_query_range.
    let start_s = (base - NS_PER_MIN) / NS_PER_SEC;
    let (status, body) = get(
        &app,
        &format!("/api/v1/query_range?query={METRIC}&start={start_s}&end={now_s}&step=60s"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "range query failed: {body}");
    let warnings = body["warnings"]
        .as_array()
        .expect("the range envelope must carry a warnings array when a remote is skipped");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().map(|s| s.contains("west")).unwrap_or(false)),
        "the skipped federation cluster must be named in the range response warnings: {warnings:?}"
    );
}
