//! `POST /v1/metrics`, `POST /v1/logs`, and `POST /v1/traces`: OTLP
//! HTTP-protobuf ingest. All three endpoints share this file's tenant
//! resolution, write-mode header, and commit-token header handling; the
//! signal-specific logic lives in [`crate::ingest`], [`crate::logs_ingest`],
//! and [`crate::traces_ingest`].

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use ravel_ingest::{AdmissionController, RequestRejection, WriteMode};
use ravel_query::http::TenantResolver;
use ravel_types::Signal;

use crate::ingest::{IngestRequestError, IngestState};
use crate::logs_ingest::{LogIngestRequestError, LogIngestState};
use crate::traces_ingest::{SpanIngestRequestError, SpanIngestState};

pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";
/// Opaque, caller-supplied idempotency key for logs and spans (ADR-0051
/// section 5). The same string is the HTTP header name and the gRPC metadata
/// key, the way `authorization` is reused verbatim across both transports.
/// A keyed request that a client retries after a lost ack replays the stored
/// receipt instead of re-ingesting (docs/consistency-model.md).
pub const IDEMPOTENCY_KEY_HEADER: &str = "x-ravel-idempotency-key";
/// Cap on the idempotency key length (ravel-ingest's `idempotency` module
/// doc: the key is `≤128 bytes`). A longer key is rejected (HTTP 400 / gRPC
/// `InvalidArgument`), never truncated or hashed anyway: silent truncation
/// would collapse two distinct keys into one dedup identity.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

/// Nanoseconds per hour, the unit an ingest-hour bucket counts in. Mirrors
/// `ravel_ingest`'s private `config::NS_PER_HOUR`; it and the shard actors'
/// `checked_ingest_hour_bucket` are `pub(crate)` to that crate and so
/// unreachable here. See [`request_ingest_hour_bucket`].
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Layer 1 (ADR-0051 section 2): the wire-body cap on every OTLP HTTP
/// endpoint, ahead of protobuf decode.
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

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
    /// Tenant admission (ADR-0051): shared by all three signals for the
    /// layer-2 byte-rate check, done here on wire bytes before decode.
    pub admission: Arc<AdmissionController>,
}

pub fn router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .route("/v1/traces", post(export_traces))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Turn a layer-2/layer-4 whole-request rejection into the ADR-0051 HTTP
/// response: 429 with `Retry-After` in whole seconds (rounded up, minimum
/// 1), the reason as the body.
fn admission_rejection_response(rejection: RequestRejection) -> Response {
    let mut response =
        (StatusCode::TOO_MANY_REQUESTS, rejection.reason.to_string()).into_response();
    let retry_after_secs = retry_after_seconds(rejection.retry_after_ns);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

fn retry_after_seconds(retry_after_ns: i64) -> u64 {
    let ns = retry_after_ns.max(0) as u64;
    ns.div_ceil(1_000_000_000).max(1)
}

/// Attaches the encoded protobuf `body` as an OTLP response, plus the
/// commit-token header `commit_token` when present. Shared by all three
/// endpoints: the header name is the same for either signal, and a client
/// distinguishes them by which endpoint it called.
///
/// `commit_token` is the already-built header value, not the token list: a
/// normal write passes [`encode_commit_tokens`] of its receipt, and an
/// idempotency replay passes the receipt's stored value verbatim, so a
/// replayed response carries the byte-identical header the original did.
fn otlp_response(body: Vec<u8>, commit_token: Option<&str>) -> Response {
    let mut response = Bytes::from(body).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-protobuf"),
    );
    if let Some(encoded) = commit_token
        && let Ok(value) = HeaderValue::from_str(encoded)
    {
        response.headers_mut().insert(COMMIT_TOKEN_HEADER, value);
    }
    response
}

/// Build the `x-ravel-commit-token` header value from a write receipt's
/// tokens: one `CommitToken::encode()` per shard the request flushed through,
/// comma-joined (docs/consistency-model.md). `None` when the write produced
/// no tokens (buffered mode, or nothing admitted), so no header is emitted.
///
/// This is the exact string an idempotency marker stores, so a keyed replay
/// round-trips it back out unchanged; it lives here as the single definition
/// both transports and the marker-write path share.
pub(crate) fn encode_commit_tokens(tokens: &[ravel_types::CommitToken]) -> Option<String> {
    if tokens.is_empty() {
        return None;
    }
    Some(
        tokens
            .iter()
            .map(|token| token.encode())
            .collect::<Vec<_>>()
            .join(","),
    )
}

/// Extract the opaque idempotency key from request headers/metadata (both
/// reach here as a [`HeaderMap`]; the gRPC handlers convert metadata first).
/// An absent or empty key is `None` (plain at-least-once); length validation
/// against [`MAX_IDEMPOTENCY_KEY_BYTES`] happens inside the signal handler so
/// the rejection is a typed error the transport maps to its own status code.
pub(crate) fn idempotency_key_from_headers(headers: &HeaderMap) -> Option<Vec<u8>> {
    headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .map(|value| value.as_bytes().to_vec())
        .filter(|key| !key.is_empty())
}

/// The request's ingest-hour bucket, computed once from `ingest_ts_ns` and
/// used for both the marker lookup (`read_marker`'s `now` bucket) and the
/// marker write, so the two can never drift within one request.
///
/// It mirrors `ravel_ingest`'s `checked_ingest_hour_bucket` formula
/// (`div_euclid` by [`NS_PER_HOUR`]); that function and its `NS_PER_HOUR` are
/// `pub(crate)` to `ravel-ingest` and unreachable from this crate, and the
/// commit token's `ingest_hour_bucket` field (the other in-tree source) only
/// exists *after* a write, so it cannot serve the pre-write lookup. `None`
/// for a non-positive or non-representable reading: idempotency then fails
/// open to the normal at-least-once path rather than failing a request whose
/// data is (or will be) durably committed regardless.
pub(crate) fn request_ingest_hour_bucket(ingest_ts_ns: i64) -> Option<u32> {
    if ingest_ts_ns <= 0 {
        return None;
    }
    u32::try_from(ingest_ts_ns.div_euclid(NS_PER_HOUR)).ok()
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

    // Layer 2 (ADR-0051 section 2): byte rate on the wire body, before
    // decode, whole-request rejection with no tokens consumed.
    if let Err(rejection) =
        state
            .admission
            .check_byte_rate(&tenant, Signal::Metrics, body.len() as u64, now_ns())
    {
        return admission_rejection_response(rejection);
    }

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
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            encode_commit_tokens(&outcome.tokens).as_deref(),
        ),
        Err(IngestRequestError::Admission(rejection)) => admission_rejection_response(rejection),
        Err(err @ IngestRequestError::Write(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
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

    // Layer 2 (ADR-0051 section 2): byte rate on the wire body, before
    // decode, whole-request rejection with no tokens consumed.
    if let Err(rejection) =
        state
            .admission
            .check_byte_rate(&tenant, Signal::Logs, body.len() as u64, now_ns())
    {
        return admission_rejection_response(rejection);
    }

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

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::logs_ingest::handle_export_logs(
        &state.logs_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
    )
    .await
    {
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            outcome.commit_token_header().as_deref(),
        ),
        Err(LogIngestRequestError::Admission(rejection)) => admission_rejection_response(rejection),
        Err(err @ LogIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ LogIngestRequestError::Write(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
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

    // Layer 2 (ADR-0051 section 2): byte rate applies uniformly to every
    // signal including spans, even though spans get no layer-4 admission
    // (ADR-0051 excludes spans from series/stream admission).
    if let Err(rejection) =
        state
            .admission
            .check_byte_rate(&tenant, Signal::Spans, body.len() as u64, now_ns())
    {
        return admission_rejection_response(rejection);
    }

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

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::traces_ingest::handle_export_traces(
        &state.traces_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
    )
    .await
    {
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            outcome.commit_token_header().as_deref(),
        ),
        Err(err @ SpanIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ SpanIngestRequestError::Write(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
    }
}
