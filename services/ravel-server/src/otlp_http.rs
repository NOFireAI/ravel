//! `POST /v1/metrics`, `POST /v1/logs`, and `POST /v1/traces`: OTLP
//! HTTP-protobuf ingest. All three endpoints share this file's tenant
//! resolution, write-mode header, and commit-token header handling; the
//! signal-specific logic lives in [`crate::ingest`], [`crate::logs_ingest`],
//! and [`crate::traces_ingest`].

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use ravel_ingest::WriteMode;
use ravel_query::http::TenantResolver;

use crate::ingest::IngestState;
use crate::logs_ingest::LogIngestState;
use crate::traces_ingest::SpanIngestState;

pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";

pub struct GatewayState {
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub ingest: IngestState,
    /// The log pipeline's counterpart of `ingest`. Separate router, separate
    /// limits: logs flush RLOG objects under the `l` keyspace, metrics flush
    /// RSEG under `m`, and nothing is shared between them but this struct.
    pub logs_ingest: LogIngestState,
    /// The span pipeline's counterpart, on the same terms: RSPAN objects under
    /// the `s` keyspace, its own router and its own limits (ADR-0041).
    pub traces_ingest: SpanIngestState,
}

pub fn router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .route("/v1/traces", post(export_traces))
        .with_state(state)
}

/// Attaches the encoded protobuf `body` as an OTLP response, plus the
/// commit-token header when the write produced tokens. Shared by both
/// endpoints: the header name is the same for either signal, and a client
/// distinguishes them by which endpoint it called.
fn otlp_response(body: Vec<u8>, tokens: &[ravel_types::CommitToken]) -> Response {
    let mut response = Bytes::from(body).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-protobuf"),
    );
    if !tokens.is_empty() {
        let encoded = tokens
            .iter()
            .map(|token| token.encode())
            .collect::<Vec<_>>()
            .join(",");
        if let Ok(value) = HeaderValue::from_str(&encoded) {
            response.headers_mut().insert(COMMIT_TOKEN_HEADER, value);
        }
    }
    response
}

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub(crate) fn write_mode_from_headers(headers: &HeaderMap) -> WriteMode {
    let buffered = headers
        .get(INGEST_MODE_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some("buffered");
    if buffered {
        WriteMode::Buffered
    } else {
        WriteMode::Strict
    }
}

async fn export_metrics(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    let request = match ExportMetricsServiceRequest::decode(body.as_ref()) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };

    match crate::ingest::handle_export(&state.ingest, tenant, mode, request, now_ns()).await {
        Ok(outcome) => otlp_response(outcome.response.encode_to_vec(), &outcome.tokens),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}

/// `POST /v1/logs`. Same shape as [`export_metrics`], down to the status
/// codes: 401 for an unresolvable tenant, 400 for an undecodable body, 503
/// for a write the log pipeline could not accept.
async fn export_logs(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    let request = match ExportLogsServiceRequest::decode(body.as_ref()) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };

    match crate::logs_ingest::handle_export_logs(
        &state.logs_ingest,
        tenant,
        mode,
        request,
        now_ns(),
    )
    .await
    {
        Ok(outcome) => otlp_response(outcome.response.encode_to_vec(), &outcome.tokens),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}

/// `POST /v1/traces`. Same shape as [`export_metrics`] and [`export_logs`],
/// down to the status codes: 401 for an unresolvable tenant, 400 for an
/// undecodable body, 503 for a write the span pipeline could not accept.
async fn export_traces(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    let request = match ExportTraceServiceRequest::decode(body.as_ref()) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };

    match crate::traces_ingest::handle_export_traces(
        &state.traces_ingest,
        tenant,
        mode,
        request,
        now_ns(),
    )
    .await
    {
        Ok(outcome) => otlp_response(outcome.response.encode_to_vec(), &outcome.tokens),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}
