//! Every Prometheus-shaped query surface submits exactly one evidential audit
//! event through the [`QueryAuditSink`] seam and awaits its durability before
//! releasing the response (ADR-0062 §2a, epic EL / issue #762).
//!
//! These drive real requests through `ravel_query::http::router` with an
//! injected recording sink over an empty store: a query over no data still
//! reaches execution for a resolved tenant, so it is audited exactly once, and
//! the recorded `query.language`/`query.status`/`query.text`/`query.tenant`
//! attrs are asserted per surface. A separate test proves the fail-closed
//! posture: when the sink's submission fails (`audit_mode=required` surfacing a
//! flush error), the request returns 503 rather than releasing unaudited.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_logseg::AttrValue;
use ravel_maintain::{AuditEvent, MaintainError, QueryAuditSink};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::TenantId;
use tower::ServiceExt;

const TOKEN: &str = "secret";
const TENANT: &str = "acme";

/// Captures every submitted [`AuditEvent`] and reports durability success, so a
/// test can assert exactly which events a request produced.
struct RecordingSink {
    events: Arc<Mutex<Vec<AuditEvent>>>,
}

#[async_trait]
impl QueryAuditSink for RecordingSink {
    async fn submit(&self, event: AuditEvent) -> Result<(), MaintainError> {
        self.events.lock().expect("lock").push(event);
        Ok(())
    }
}

/// Always fails its submission, standing in for an `audit_mode=required`
/// pipeline whose flush failed: every surface must fail the request closed.
struct FailingSink;

#[async_trait]
impl QueryAuditSink for FailingSink {
    async fn submit(&self, _event: AuditEvent) -> Result<(), MaintainError> {
        Err(MaintainError::AuditFlush("audit store down".to_string()))
    }
}

fn app_with_sink(sink: Arc<dyn QueryAuditSink>) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, store, EngineConfig::default()));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT.to_string()));
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)))
        .with_audit_sink(sink);
    router(state)
}

fn recording() -> (Router, Arc<Mutex<Vec<AuditEvent>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::new(RecordingSink {
        events: Arc::clone(&events),
    });
    (app_with_sink(sink), events)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request")
}

/// The value of a string `attrs` entry, or `None` if absent or non-string.
fn attr<'a>(event: &'a AuditEvent, key: &str) -> Option<&'a str> {
    event
        .attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

/// Assert exactly one event was recorded and return it.
fn one(events: &Arc<Mutex<Vec<AuditEvent>>>) -> AuditEvent {
    let guard = events.lock().expect("lock");
    assert_eq!(guard.len(), 1, "exactly one audit event per request");
    guard[0].clone()
}

fn assert_common(event: &AuditEvent, language: &str, status: &str) {
    assert_eq!(attr(event, "kind"), Some("query"));
    assert_eq!(attr(event, "query.language"), Some(language));
    assert_eq!(attr(event, "query.status"), Some(status));
    assert_eq!(
        attr(event, "query.tenant"),
        Some(TenantId::new(TENANT.to_string()).hash().to_hex().as_str())
    );
}

#[tokio::test]
async fn instant_query_submits_one_promql_ok_event() {
    let (app, events) = recording();
    let response = app
        .oneshot(get("/api/v1/query?query=up&time=1000"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let event = one(&events);
    assert_common(&event, "promql", "ok");
    assert_eq!(attr(&event, "query.text"), Some("up"));
}

#[tokio::test]
async fn range_query_submits_one_promql_ok_event_with_the_resolved_window() {
    let (app, events) = recording();
    let response = app
        .oneshot(get("/api/v1/query_range?query=up&start=1&end=2&step=1"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let event = one(&events);
    assert_common(&event, "promql", "ok");
    assert_eq!(attr(&event, "query.text"), Some("up"));
    // start=1s, end=2s expressed in nanoseconds.
    assert_eq!(attr(&event, "query.window_start_ns"), Some("1000000000"));
    assert_eq!(attr(&event, "query.window_end_ns"), Some("2000000000"));
}

#[tokio::test]
async fn labels_submits_one_labels_event() {
    let (app, events) = recording();
    let response = app
        .oneshot(get("/api/v1/labels"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    assert_common(&one(&events), "labels", "ok");
}

#[tokio::test]
async fn label_values_submits_one_labels_event() {
    let (app, events) = recording();
    let response = app
        .oneshot(get("/api/v1/label/job/values"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    assert_common(&one(&events), "labels", "ok");
}

#[tokio::test]
async fn series_submits_one_series_event_with_the_selectors() {
    let (app, events) = recording();
    let response = app
        .oneshot(get("/api/v1/series?match%5B%5D=up"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let event = one(&events);
    assert_common(&event, "series", "ok");
    assert_eq!(attr(&event, "query.text"), Some("up"));
}

/// A request rejected before it reaches execution for a resolved tenant is not
/// audited: an unauthenticated request produces no audit event.
#[tokio::test]
async fn an_unauthenticated_request_is_not_audited() {
    let (app, events) = recording();
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/query?query=up&time=1000")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        events.lock().expect("lock").is_empty(),
        "a request rejected before execution writes no audit event"
    );
}

/// Fail-closed: when the audit submission fails (a `required`-mode flush
/// failure), the query surface returns 503 rather than releasing an unaudited
/// response. This is the deliberate inversion of "queries outlive the trail".
#[tokio::test]
async fn a_required_audit_failure_fails_the_query_closed() {
    let app = app_with_sink(Arc::new(FailingSink));
    let response = app
        .oneshot(get("/api/v1/query?query=up&time=1000"))
        .await
        .expect("infallible");
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "an audit submission failure must fail the query closed"
    );
}
