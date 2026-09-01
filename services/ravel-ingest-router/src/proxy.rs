//! HTTP reverse proxy (deliverable 5) and the axum app wiring (deliverable 6).
//!
//! The proxy forwards an incoming request's method, headers, and body straight
//! to the selected pod's dial address and streams the response back. It dials
//! the pod address from the EndpointSlice directly; it never resolves or dials
//! the gateway Service's cluster IP or DNS name, so kube-proxy's own load
//! balancing cannot re-spread the request and undo the subset selection.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};

use crate::router::RouterState;
use crate::select::RouteError;

/// Build the axum app: health probes plus the catch-all proxy.
///
/// `/readyz` and `/healthz` (with Prometheus' `/-/ready` and `/-/healthy`
/// spellings) are the router's own; every other path is proxied.
pub(crate) fn build_app(state: Arc<RouterState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/-/healthy", get(healthz))
        .route("/readyz", get(readyz))
        .route("/-/ready", get(readyz))
        .fallback(any(proxy))
        .with_state(state)
}

/// Liveness: reachable means alive.
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: 200 once the watcher has completed its first sync, 503 before
/// that, so a Deployment readiness probe holds traffic off a router that would
/// otherwise select from an empty subset (deliverable 2 cold start).
async fn readyz(State(state): State<Arc<RouterState>>) -> StatusCode {
    if state.endpoints.is_synced() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The catch-all proxy handler: select, then forward.
async fn proxy(State(state): State<Arc<RouterState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let selection = match state.resolve_and_select(&parts.headers) {
        Ok(sel) => sel,
        // The error maps to a status only; no key bytes ever reach the body.
        Err(err) => return err.status().into_response(),
    };

    match forward(&state.http, selection.addr, parts, body).await {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(addr = %selection.addr, error = %err, "upstream forward failed");
            RouteError::Upstream.status().into_response()
        }
    }
}

/// Forward the request to `addr` and stream the response back.
async fn forward(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    parts: axum::http::request::Parts,
    body: Body,
) -> Result<Response, reqwest::Error> {
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    // Dial the pod address directly. The authority is the endpoint IP:port from
    // the EndpointSlice, never a Service name, so there is no VIP indirection.
    let url = format!("http://{addr}{path_and_query}");

    let mut headers = parts.headers.clone();
    strip_hop_by_hop(&mut headers);
    // reqwest sets Host from the URL authority (the pod address); a forwarded
    // client Host would conflict with it.
    headers.remove(axum::http::header::HOST);

    let upstream = client
        .request(parts.method, url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await?;

    let mut builder = Response::builder().status(upstream.status());
    for (name, value) in upstream.headers() {
        if !is_hop_by_hop(name) {
            builder = builder.header(name, value);
        }
    }
    // `Body::from_stream` streams the upstream response without buffering it.
    // A builder error here means an invalid status/header from the upstream;
    // surface it as a 502 rather than panicking.
    Ok(builder
        .body(Body::from_stream(upstream.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response()))
}

/// Hop-by-hop headers (RFC 9110 sec 7.6.1) that a proxy must not forward.
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let drop: Vec<HeaderName> = headers
        .keys()
        .filter(|n| is_hop_by_hop(n))
        .cloned()
        .collect();
    for name in drop {
        headers.remove(name);
    }
}
