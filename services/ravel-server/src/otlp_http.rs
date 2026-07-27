//! `POST /v1/metrics`: OTLP HTTP-protobuf metrics ingest.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use ravel_ingest::WriteMode;
use ravel_query::http::TenantResolver;

use crate::ingest::IngestState;

pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";

pub struct GatewayState {
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub ingest: IngestState,
}

pub fn router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(export_metrics))
        .with_state(state)
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
        Ok(outcome) => {
            let mut response = Bytes::from(outcome.response.encode_to_vec()).into_response();
            response.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("application/x-protobuf"),
            );
            if !outcome.tokens.is_empty() {
                let encoded = outcome
                    .tokens
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
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}
