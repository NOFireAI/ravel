//! Acceptance proof for ADR-0071's "partial results are consent-gated and
//! envelope-visible" amendment (decisions 1-3): the read HTTP surface refuses
//! partial coverage unless the client opted in with `allow_partial`, and when
//! it does return partial data it flags it with a required top-level `partial`
//! field.
//!
//! The amendment's acceptance property: **a client which ignores warnings
//! cannot mistake partial for complete.** This is proven two ways here:
//!
//! 1. Without `allow_partial`, a partial-coverage request fails with the typed
//!    HTTP 503 `unavailable` error (decision 1), so a status-only client sees
//!    an error, never a `success` envelope it would treat as complete.
//! 2. With `allow_partial=true`, the response is HTTP 200 whose ONLY top-level
//!    discriminator from a complete response is the new `partial` field
//!    (decision 2). `partial_field_is_the_sole_top_level_discriminator` proves
//!    that reading only `status`+`data` (what a naive client does) leaves a
//!    partial and a complete response indistinguishable -- so the `partial`
//!    field is genuinely load-bearing, and its removal would reintroduce the
//!    defect.
//!
//! Uniformity (decision 3) is proven across `/api/v1/query` (value-bearing) and
//! `/api/v1/series` (metadata), the two envelope shapes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use ravel_query::distrib::{Federation, RemoteCluster};
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::TenantId;
use serde_json::Value;
use tower::ServiceExt;

const TOKEN: &str = "tok-1116";
const CLUSTER: &str = "eu-west";

/// A remote that always fails transport. With `skip_unavailable = true` the
/// coordinator records it as a coverage gap (a skipped cluster), which is
/// exactly the partial-coverage condition decision 1 gates on -- no local data
/// or real remote is needed to drive the gate.
struct AlwaysUnavailable;

#[async_trait]
impl SliceFetcher for AlwaysUnavailable {
    async fn fetch(&self, _request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        Err(DistribError::Transport("remote down (test)".to_string()))
    }
}

fn tenant() -> TenantId {
    TenantId::new("tenant-1116".to_string())
}

/// A router over an empty tenant. `federated` attaches one `skip_unavailable`
/// remote that always fails (so every query has partial coverage); otherwise
/// the engine is fully cluster-local (so every query has complete coverage).
fn app(federated: bool) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let mut engine = QueryEngine::new(catalog, store, EngineConfig::default());
    if federated {
        let federation = Federation::new(vec![RemoteCluster {
            name: CLUSTER.to_string(),
            fetcher: Arc::new(AlwaysUnavailable),
            skip_unavailable: true,
            soft_timeout: Duration::from_secs(5),
        }]);
        engine = engine.with_federation(Arc::new(federation));
    }
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let state = AppState::new(
        Arc::new(engine),
        Arc::new(StaticBearerTokenResolver::new(tokens)),
    );
    router(state)
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
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

// ---- /api/v1/query (value-bearing endpoint) ------------------------------

#[tokio::test]
async fn query_partial_without_opt_in_is_503_unavailable() {
    let (status, json) = get(&app(true), "/api/v1/query?query=up").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "partial coverage without allow_partial must fail with 503, not a 200 a \
         status-only client would accept: {json}"
    );
    assert_eq!(json["status"], "error");
    assert_eq!(
        json["errorType"], "unavailable",
        "reuses the existing typed unavailable error (decision 1)"
    );
    let msg = json["error"].as_str().expect("error message");
    assert!(
        msg.contains(CLUSTER),
        "the failure must name the degraded cluster: {msg}"
    );
    assert!(
        msg.contains("allow_partial"),
        "the failure must name the remedy so the fix is in the error itself: {msg}"
    );
}

#[tokio::test]
async fn query_partial_with_opt_in_is_200_partial_true() {
    let (status, json) = get(&app(true), "/api/v1/query?query=up&allow_partial=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(
        json["partial"],
        Value::Bool(true),
        "an opted-in partial response carries the required top-level partial:true"
    );
    let warnings = json["warnings"].as_array().expect("warnings array");
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap_or("").contains(CLUSTER)),
        "the skipped cluster is named in warnings too: {json}"
    );
}

#[tokio::test]
async fn query_complete_is_200_partial_false_present() {
    let (status, json) = get(&app(false), "/api/v1/query?query=up").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    // Decision 2: present, not omitted -- `false`, never absent.
    assert!(
        json.get("partial").is_some(),
        "partial must be present on a complete response, never omitted: {json}"
    );
    assert_eq!(json["partial"], Value::Bool(false));
}

// ---- /api/v1/series (metadata endpoint) ----------------------------------

#[tokio::test]
async fn series_partial_without_opt_in_is_503_unavailable() {
    let (status, json) = get(&app(true), "/api/v1/series?match%5B%5D=up").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the metadata surface gates identically to the query surface (decision 3): {json}"
    );
    assert_eq!(json["status"], "error");
    assert_eq!(json["errorType"], "unavailable");
    let msg = json["error"].as_str().expect("error message");
    assert!(
        msg.contains(CLUSTER) && msg.contains("allow_partial"),
        "{msg}"
    );
}

#[tokio::test]
async fn series_partial_with_opt_in_is_200_partial_true() {
    let (status, json) = get(
        &app(true),
        "/api/v1/series?match%5B%5D=up&allow_partial=true",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(
        json["partial"],
        Value::Bool(true),
        "the metadata envelope carries the same top-level partial marker: {json}"
    );
}

#[tokio::test]
async fn series_complete_is_200_partial_false_present() {
    let (status, json) = get(&app(false), "/api/v1/series?match%5B%5D=up").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "success");
    assert_eq!(
        json["partial"],
        Value::Bool(false),
        "a complete metadata response carries partial:false, present not omitted: {json}"
    );
}

// ---- the load-bearing proof ----------------------------------------------

/// The amendment's acceptance property, made executable: a naive client that
/// reads only `status` and `data` sees an IDENTICAL shape for an opted-in
/// partial response and a complete response. The `partial` field is therefore
/// the sole top-level signal distinguishing them; without it (the
/// pre-amendment wire, or any regression that drops/omits it) the two are
/// indistinguishable and the client silently accepts partial as complete.
#[tokio::test]
async fn partial_field_is_the_sole_top_level_discriminator() {
    let (_, partial_json) = get(&app(true), "/api/v1/query?query=up&allow_partial=true").await;
    let (_, complete_json) = get(&app(false), "/api/v1/query?query=up").await;

    // What a warnings-ignoring client that consumes status + the query result
    // observes: identical. (The buried `data.stats.partial` flag does differ,
    // but that nested marker is exactly the pre-amendment signal the ADR's
    // context section deemed insufficient -- a programmatic consumer reads the
    // result payload, not the stats block, and no metadata endpoint even has a
    // stats block to bury it in.)
    assert_eq!(
        partial_json["status"], complete_json["status"],
        "status is 'success' for both -- a status check cannot tell them apart"
    );
    assert_eq!(
        partial_json["data"]["resultType"], complete_json["data"]["resultType"],
        "resultType is identical for both"
    );
    assert_eq!(
        partial_json["data"]["result"], complete_json["data"]["result"],
        "the query result payload is byte-identical for both (both empty \
         vectors) -- consuming the result cannot tell them apart"
    );

    // The ONLY top-level field that differs is `partial`. This is the field the
    // amendment adds; deleting it collapses the two responses into one a naive
    // client cannot distinguish, which is exactly the defect the amendment fixes.
    assert_ne!(
        partial_json["partial"], complete_json["partial"],
        "partial must be the discriminator: {partial_json} vs {complete_json}"
    );
    assert_eq!(partial_json["partial"], Value::Bool(true));
    assert_eq!(complete_json["partial"], Value::Bool(false));
}
