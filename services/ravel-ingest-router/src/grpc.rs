//! gRPC (HTTP/2 cleartext) transparent proxy (#184, deliverable 3).
//!
//! This is the gRPC-transport analogue of [`crate::proxy`], not a second
//! selection implementation: it calls the exact same
//! [`RouterState::resolve_and_select`](crate::router::RouterState::resolve_and_select)
//! boundary the HTTP proxy uses, then forwards the request as raw HTTP/2 frames
//! to the selected pod's dial address without decoding the gRPC message envelope
//! (ADR-0080 decision 3: a transparent HTTP/2-level proxy that preserves gRPC
//! framing is sufficient; the router never parses OTLP proto here).
//!
//! # Framing of a router-rejected request
//!
//! When selection itself fails (cold start, exhausted subset, canonical-tenant
//! resolution failure, or a forwarding failure to the chosen backend), the
//! router has not read a single gRPC frame, so there is nothing to stream back.
//! It answers with a **trailers-only** gRPC response: HTTP 200,
//! `content-type: application/grpc`, and the `grpc-status`/`grpc-message`
//! carried in the initial HEADERS frame (an empty body, so that frame also ends
//! the stream). This is the canonical gRPC error encoding for an immediate
//! error, and a gRPC client reads `grpc-status` straight from the response
//! headers in this case (see `tonic::Status::from_header_map`, exercised by the
//! `grpc_canonical_tenant_resolution_failure_rejects_with_unauthenticated`
//! test). [`tonic::Status::into_http`] builds exactly this shape from the
//! [`RouteError::grpc_code`](crate::select::RouteError) mapping, so the reject
//! path reuses the same status projection deliverable 2 defines rather than
//! hand-assembling headers.
//!
//! # Trailer transparency on the success path
//!
//! A real gRPC response carries its outcome in HTTP/2 *trailers*
//! (`grpc-status`/`grpc-message` after the message body). `reqwest`'s
//! `bytes_stream()` yields only data frames and would silently drop those
//! trailers, so this module instead converts the upstream response into
//! `http::Response<reqwest::Body>` (whose body forwards trailer frames untouched)
//! and re-wraps it as an [`axum::body::Body`], leaving the backend's trailers to
//! reach the client unmodified whenever this router did not itself reject the
//! request.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName};
use axum::response::Response;
use axum::routing::any;

use crate::router::RouterState;
use crate::select::RouteError;

/// Build the gRPC proxy app: a single catch-all handler over every path, since a
/// transparent proxy routes by tenant key, not by gRPC method path.
pub(crate) fn build_app(state: Arc<RouterState>) -> Router {
    Router::new().fallback(any(proxy)).with_state(state)
}

/// The catch-all gRPC handler: select, then forward, mirroring
/// [`crate::proxy::proxy`] but over HTTP/2 with gRPC-aware framing.
async fn proxy(State(state): State<Arc<RouterState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let selection = match state.resolve_and_select(&parts.headers) {
        Ok(sel) => sel,
        // The router rejected before reading any gRPC frame: answer with a
        // trailers-only gRPC error carrying the mapped status. No key bytes ever
        // reach the response (RouteError's Display strings carry none).
        Err(err) => return reject(&err),
    };

    match forward(&state.grpc_http, selection.addr, parts, body).await {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(addr = %selection.addr, error = %err, "grpc upstream forward failed");
            reject(&RouteError::Upstream)
        }
    }
}

/// Encode a [`RouteError`] as a trailers-only gRPC error response the client
/// parses as the mapped [`tonic::Code`].
fn reject(err: &RouteError) -> Response {
    // `into_http` sets HTTP 200, `content-type: application/grpc`, and the
    // `grpc-status`/`grpc-message` headers over an empty (`Body::default`) body.
    tonic::Status::new(err.grpc_code(), err.to_string()).into_http::<Body>()
}

/// Forward the request to `addr` over cleartext HTTP/2 and stream the response
/// back, preserving gRPC response trailers.
async fn forward(
    client: &reqwest::Client,
    addr: SocketAddr,
    parts: axum::http::request::Parts,
    body: Body,
) -> Result<Response, reqwest::Error> {
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    // Dial the pod address directly, exactly like the HTTP proxy: the authority
    // is the endpoint IP:port from the EndpointSlice, never a Service name, so
    // kube-proxy cannot re-spread the connection and undo the subset selection.
    let url = format!("http://{addr}{path_and_query}");

    let mut headers = parts.headers.clone();
    strip_hop_by_hop(&mut headers);
    // reqwest sets Host from the URL authority (the pod address); a forwarded
    // client Host would conflict with it.
    headers.remove(axum::http::header::HOST);

    // gRPC requests carry their payload (and end-of-stream) in DATA frames, not
    // trailers, so streaming the data frames through is fully transparent for the
    // request direction. The `http2_prior_knowledge()` client sends this over
    // h2c to the tonic backend.
    let upstream = client
        .request(parts.method, url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await?;

    // Convert into `http::Response<reqwest::Body>` so the response body still
    // yields trailer frames (`grpc-status`/`grpc-message`); `bytes_stream()`
    // would drop them. `Body::new` re-wraps that body for axum, preserving the
    // frames end to end.
    let http_resp: axum::http::Response<reqwest::Body> = axum::http::Response::from(upstream);
    let (mut resp_parts, resp_body) = http_resp.into_parts();
    // Strip hop-by-hop response headers. gRPC trailers live in body frames, not
    // here, so they are untouched.
    strip_hop_by_hop(&mut resp_parts.headers);
    Ok(Response::from_parts(resp_parts, Body::new(resp_body)))
}

/// Connection-management headers that must not be forwarded across a proxy hop.
///
/// Unlike the HTTP proxy's list ([`crate::proxy`]), this deliberately keeps `te`
/// and `trailer`: gRPC-over-HTTP/2 relies on `te: trailers`, and stripping it
/// would break trailer negotiation with a strict backend. The remaining
/// connection-specific headers are illegal in HTTP/2 anyway, so removing them
/// keeps the forwarded request well-formed.
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
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
