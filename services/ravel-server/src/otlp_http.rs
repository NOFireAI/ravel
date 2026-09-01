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
use ravel_ingest::{
    AdmissionController, LogWriteError, RequestRejection, SpanWriteError, WriteError, WriteMode,
};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantId};

use crate::ingest::{IngestRequestError, IngestState};
use crate::ingest_concurrency::IngestConcurrencyController;
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
/// endpoint, ahead of protobuf decode. This bounds the *compressed* body when a
/// client sends gzip; the decompressed size is bounded independently by
/// [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`], exactly as Remote Write pairs its two
/// caps (ADR-0084 decision 3).
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Cap on the decompressed size of a gzip OTLP body (ADR-0084 decision 3),
/// matching Remote Write's post-Snappy cap. Enforced *while* expanding, not
/// after, by reading the decoder through `take(cap + 1)`: a decompression bomb
/// is refused as it is being inflated rather than after Ravel has already
/// allocated it (the same discipline as `ravel-otap`'s `decompress_capped`).
const MAX_DECOMPRESSED_OTLP_BODY_BYTES: usize = 64 * 1024 * 1024;

/// The `Content-Encoding` a request declared, after RFC 9110 parsing. Only the
/// codings Ravel actually decodes are named; everything else is
/// [`ContentCoding::Unsupported`] and answered with 415 rather than being
/// guessed at, so an unknown coding never reaches prost as a compressed body
/// and produces the misleading `invalid OTLP payload` 400 ADR-0084 exists to
/// remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentCoding {
    /// Absent header, empty value, or the literal `identity`: the body is the
    /// payload as-is.
    Identity,
    /// `gzip` or its RFC 9110 alias `x-gzip`, compared case-insensitively.
    Gzip,
    /// Any other single coding, or any multi-coding list (`gzip, gzip`,
    /// `deflate, gzip`): Ravel does not chain decoders, so this is a 415.
    Unsupported,
}

/// Parses `Content-Encoding` per RFC 9110: case-insensitive, `x-gzip` an alias
/// for `gzip`, a single coding only. A comma-separated list is unsupported
/// because Ravel implements no chained decoding and guessing at one member
/// would be a silent approximation.
fn parse_content_encoding(headers: &HeaderMap) -> ContentCoding {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return ContentCoding::Identity;
    };
    let Ok(value) = value.to_str() else {
        return ContentCoding::Unsupported;
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return ContentCoding::Identity;
    }
    // A list is always unsupported, even `gzip, gzip`: no chained decoding.
    if trimmed.contains(',') {
        return ContentCoding::Unsupported;
    }
    let coding = trimmed.to_ascii_lowercase();
    match coding.as_str() {
        "identity" => ContentCoding::Identity,
        "gzip" | "x-gzip" => ContentCoding::Gzip,
        _ => ContentCoding::Unsupported,
    }
}

/// Why a gzip body could not be turned into OTLP bytes.
#[derive(Debug)]
enum GzipDecodeError {
    /// The decompressed stream exceeded [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`]:
    /// HTTP 413. Detected while expanding, before the full expansion is
    /// allocated.
    TooLarge,
    /// The bytes were not a well-formed gzip stream, or carried trailing bytes
    /// after a well-formed stream ended: HTTP 400. Truncating silently is not
    /// an option at any size.
    Invalid(String),
}

/// Decompresses a gzip `body` under a single hard cap across all members,
/// copying `ravel-otap`'s `decompress_capped` discipline: the decoder is read
/// through `take(cap + 1)` so the output buffer can never grow past `cap + 1`
/// before the check runs, which refuses a bomb as it inflates rather than after
/// the allocation the attack targets has already happened.
///
/// Uses [`MultiGzDecoder`], not `GzDecoder`: a concatenated multi-member stream
/// is legal gzip that ordinary tooling produces, and a plain `GzDecoder` would
/// decode member one, acknowledge it, and silently drop the rest. Trailing
/// bytes after the final member surface as [`GzipDecodeError::Invalid`] (400).
fn decompress_gzip_capped(body: &[u8], cap: usize) -> Result<Vec<u8>, GzipDecodeError> {
    use std::io::Read;

    use flate2::read::MultiGzDecoder;

    let cap_u64 = cap as u64;
    let mut limited = MultiGzDecoder::new(body).take(cap_u64 + 1);
    let mut out = Vec::new();
    limited
        .read_to_end(&mut out)
        .map_err(|err| GzipDecodeError::Invalid(err.to_string()))?;
    if out.len() as u64 > cap_u64 {
        return Err(GzipDecodeError::TooLarge);
    }
    Ok(out)
}

/// HTTP 415 for an unsupported `Content-Encoding`, naming what is supported so
/// the gap is legible rather than a mystery (ADR-0084 decision 1).
fn unsupported_encoding_response() -> Response {
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported Content-Encoding; this endpoint accepts only an absent or identity encoding, \
         or a single gzip (x-gzip) coding",
    )
        .into_response()
}

/// Applies the layer-2 byte-rate charge and returns the OTLP protobuf bytes to
/// decode, dispatching on `Content-Encoding` (ADR-0084 decisions 1, 3, 4).
///
/// - Identity: the current path byte for byte. `check_byte_rate` charges the
///   wire body length exactly as before, and the returned `Bytes` is the input
///   with no copy.
/// - gzip/x-gzip: the compressed length is pre-checked against the tenant's
///   available tokens without consuming any (`peek_byte_rate`); if even that
///   lower bound is over rate the request is rejected 429 before anything is
///   inflated. Otherwise the body is decompressed under the 64 MiB cap and the
///   real charge is made on the decompressed size.
/// - anything else: 415.
///
/// The wire (compressed) length is recorded per tenant on the admitted path so
/// `/metrics` can report it alongside the charged (decompressed) size.
fn admit_and_decode_body(
    state: &GatewayState,
    headers: &HeaderMap,
    tenant: &TenantId,
    signal: Signal,
    body: Bytes,
) -> Result<Bytes, Box<Response>> {
    match parse_content_encoding(headers) {
        ContentCoding::Unsupported => Err(Box::new(unsupported_encoding_response())),
        ContentCoding::Identity => {
            let wire_len = body.len() as u64;
            // Layer 2 (ADR-0051 section 2): byte rate on the wire body, before
            // decode, unchanged from today for an uncompressed client.
            if let Err(rejection) =
                state
                    .admission
                    .check_byte_rate(tenant, signal, wire_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            state
                .ingest_byte_metrics
                .record_wire_bytes(tenant, signal, wire_len);
            Ok(body)
        }
        ContentCoding::Gzip => {
            let wire_len = body.len() as u64;
            // Compressed-size pre-check (ADR-0084 decision 4): the compressed
            // length is a strict lower bound on the decompressed length, so if
            // even it exceeds the available tokens the request is rejected 429
            // without inflating anything and without consuming tokens. This
            // keeps an already-over-rate tenant from billing the gateway up to
            // 64 MiB of inflate per rejected request.
            if let Err(rejection) =
                state
                    .admission
                    .peek_byte_rate(tenant, signal, wire_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            let decompressed = match decompress_gzip_capped(&body, MAX_DECOMPRESSED_OTLP_BODY_BYTES)
            {
                Ok(bytes) => bytes,
                Err(GzipDecodeError::TooLarge) => {
                    return Err(Box::new(
                        (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            format!(
                                "decompressed OTLP body exceeds {} bytes",
                                MAX_DECOMPRESSED_OTLP_BODY_BYTES
                            ),
                        )
                            .into_response(),
                    ));
                }
                Err(GzipDecodeError::Invalid(detail)) => {
                    return Err(Box::new(
                        (
                            StatusCode::BAD_REQUEST,
                            format!("invalid gzip body: {detail}"),
                        )
                            .into_response(),
                    ));
                }
            };
            let decompressed_len = decompressed.len() as u64;
            // The real charge: the decompressed size (ADR-0084 decision 4), so
            // a compressing tenant and an uncompressing one sending the same
            // telemetry are charged the same.
            if let Err(rejection) =
                state
                    .admission
                    .check_byte_rate(tenant, signal, decompressed_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            state
                .ingest_byte_metrics
                .record_wire_bytes(tenant, signal, wire_len);
            Ok(Bytes::from(decompressed))
        }
    }
}

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
    /// layer-2 byte-rate check. On the identity path this is done on wire bytes
    /// before decode as before; on the gzip path (ADR-0084) it is done on the
    /// decompressed size after decompression, behind a compressed-size
    /// pre-check.
    pub admission: Arc<AdmissionController>,
    /// Per-tenant wire (compressed) request-body byte counter (ADR-0084
    /// decision 5). Shared with the gRPC ingest services and the `/metrics`
    /// renderer, so an operator can compare wire bytes against the charged
    /// (decompressed) bytes admission reports and tell a tenant that increased
    /// telemetry from one that turned compression off.
    pub ingest_byte_metrics: Arc<crate::ingest_byte_metrics::IngestByteMetrics>,
    /// The process-wide in-flight ingest-request ceiling, shared
    /// with every OTLP HTTP/gRPC service and Remote Write on this listener
    /// and the mTLS listener. Checked first in every handler below, ahead of
    /// tenant resolution and the layer-2 byte-rate check, so a shed request
    /// does none of that work.
    pub ingest_concurrency: Arc<IngestConcurrencyController>,
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

/// A fixed `Retry-After` for the process-wide in-flight shed: the
/// controller tracks no per-caller refill time the way `RequestRejection`
/// does, and a slot can free up as soon as any in-flight request completes,
/// so a short fixed wait is the right shape here (no per-error estimate is
/// available, the same situation `remote_write::RETRY_AFTER_SECONDS`
/// documents).
const INGEST_CONCURRENCY_RETRY_AFTER_SECONDS: u64 = 1;

/// 429 for a request shed by the process-wide in-flight ceiling, before
/// tenant resolution or any per-signal admission check: no shard is touched
/// and no commit token is issued.
fn ingest_concurrency_shed_response() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "process in-flight ingest-request limit reached",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// A fixed `Retry-After` for the process-wide ingest buffer byte budget shed
/// (ADR-0069): like the in-flight shed above, the budget tracks no per-caller
/// refill time -- a buffer slot frees as soon as any in-flight flush completes
/// -- so a short fixed wait is the right shape.
const INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS: u64 = 1;

/// 429 for a request shed by the process-wide ingest buffer byte budget
/// (ADR-0069 decision 1): the write was rejected before any buffering, so no
/// shard was touched and no commit token issued. Same 429 + `Retry-After`
/// shape as the byte-rate rejection and the in-flight shed.
fn ingest_buffer_budget_shed_response() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "ingest buffer byte budget reached",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS.to_string())
    {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
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
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before.
    let body = match admit_and_decode_body(&state, &headers, &tenant, Signal::Metrics, body) {
        Ok(body) => body,
        Err(response) => return *response,
    };

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
        // Receiver-clock floor (ADR-0051 amendment): 503, the fault is
        // the replica's and a retry against a healthy one succeeds.
        Err(err @ IngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ IngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        // The buffer-budget shed (ADR-0069) is a 429, not the 503 the other
        // write failures take: it is an admission backpressure signal, not a
        // durability failure, and the client should retry with backoff.
        Err(IngestRequestError::Write(WriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
        }
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
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before.
    let body = match admit_and_decode_body(&state, &headers, &tenant, Signal::Logs, body) {
        Ok(body) => body,
        Err(response) => return *response,
    };

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
        Err(err @ LogIngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ LogIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ LogIngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        Err(LogIngestRequestError::Write(LogWriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
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
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): byte rate
    // applies uniformly to every signal including spans (even though spans get
    // no layer-4 admission), charged after decompression on the gzip path.
    let body = match admit_and_decode_body(&state, &headers, &tenant, Signal::Spans, body) {
        Ok(body) => body,
        Err(response) => return *response,
    };

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
        Err(err @ SpanIngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ SpanIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ SpanIngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        Err(SpanIngestRequestError::Write(SpanWriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
        }
        Err(err @ SpanIngestRequestError::Write(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::io::Write as _;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use ravel_ingest::{
        AdmissionController, AdmissionLimits, IngestConfig, IngestRouter, LogIngestRouter,
        RateLimit, SpanIngestRouter, SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
    use ravel_query::http::AuthError;

    use super::*;
    use crate::ingest_byte_metrics::IngestByteMetrics;
    use crate::ingest_concurrency::{IngestConcurrencyController, IngestConcurrencyLimit};

    const TENANT: &str = "acme";

    /// Resolves every request to the same fixed tenant, so a handler test can
    /// find that tenant's admission usage row without threading a token.
    struct FixedTenantResolver(TenantId);

    impl TenantResolver for FixedTenantResolver {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(self.0.clone())
        }
    }

    /// A `GatewayState` over `MemoryStore`, whose admission controller carries
    /// `limits` as its per-tenant defaults (so the fixed tenant gets them). The
    /// metrics, log, and span routers are all real, so a handler runs the full
    /// ingest path and returns the same status a client would see.
    fn state_with_limits(limits: AdmissionLimits) -> Arc<GatewayState> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let metrics_router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store.clone(),
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        let log_router = Arc::new(LogIngestRouter::new(
            IngestConfig::default(),
            store.clone(),
            Arc::new(SystemClock),
        ));
        let span_router = Arc::new(SpanIngestRouter::new(
            IngestConfig::default(),
            store.clone(),
            Arc::new(SystemClock),
        ));
        let admission = Arc::new(AdmissionController::new(Arc::new(SystemClock), limits));
        Arc::new(GatewayState {
            tenant_resolver: Arc::new(FixedTenantResolver(TenantId::new(TENANT))),
            ingest: crate::ingest::IngestState {
                router: metrics_router,
                limits: IngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                recovery: None,
                provisioning: None,
                metadata_sink: None,
            },
            logs_ingest: crate::logs_ingest::LogIngestState {
                router: log_router,
                limits: LogIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
            },
            traces_ingest: crate::traces_ingest::SpanIngestState {
                router: span_router,
                limits: SpanIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
            },
            admission,
            ingest_concurrency: IngestConcurrencyController::shared(
                IngestConcurrencyLimit::Unlimited,
            ),
            ingest_byte_metrics: Arc::new(IngestByteMetrics::new()),
        })
    }

    /// A metrics export that compresses well: `points` copies of one gauge data
    /// point, so the decompressed protobuf is many times the gzip size. The
    /// distinction matters for the charging tests, which assert the byte rate is
    /// charged the decompressed length, not the compressed one.
    fn compressible_request(points: usize) -> ExportMetricsServiceRequest {
        let data_points = (0..points)
            .map(|_| NumberDataPoint {
                time_unix_nano: now_ns() as u64,
                value: Some(NumberValue::AsDouble(1.0)),
                ..Default::default()
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests_total".to_string(),
                        data: Some(MetricData::Gauge(Gauge { data_points })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One-member gzip of `data`.
    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    /// The fixed tenant's metrics admission usage row, or `None` if untouched.
    fn metrics_usage(state: &GatewayState) -> Option<ravel_ingest::TenantUsage> {
        let want = TenantId::new(TENANT).hash();
        state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|row| row.tenant_hash == want && row.signal == Signal::Metrics)
    }

    fn gzip_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        headers
    }

    /// ADR-0084 decision 1/4: a gzip body is decoded and ingested (200), and the
    /// byte rate is charged the DECOMPRESSED length, not the compressed one.
    ///
    /// Non-vacuity: the request is chosen so `compressed_len` is far below
    /// `decompressed_len`, and the test asserts the charge equals the latter and
    /// differs from the former. Change `admit_and_decode_body`'s gzip
    /// `check_byte_rate(decompressed_len)` to charge `wire_len` and this fails.
    #[tokio::test]
    async fn gzip_body_is_accepted_and_charged_decompressed() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(2000).encode_to_vec();
        let compressed = gzip(&encoded);
        assert!(
            compressed.len() < encoded.len(),
            "fixture must compress: compressed={} decompressed={}",
            compressed.len(),
            encoded.len()
        );

        let response = export_metrics(
            State(state.clone()),
            gzip_headers(),
            Bytes::from(compressed.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "gzip body must be accepted"
        );

        let usage = metrics_usage(&state).expect("a metrics usage row after admitting the request");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "the byte rate must charge the decompressed length"
        );
        assert_ne!(
            usage.bytes_admitted_total,
            compressed.len() as u64,
            "the charge must not be the compressed length"
        );

        // The wire (compressed) size is recorded separately for /metrics.
        let wire = state.ingest_byte_metrics.snapshot();
        let row = wire
            .iter()
            .find(|r| r.tenant_hash == TenantId::new(TENANT).hash() && r.signal == Signal::Metrics)
            .expect("a wire-bytes row");
        assert_eq!(row.wire_bytes_total, compressed.len() as u64);
    }

    /// ADR-0084 decision 3: a body inflating past 64 MiB is rejected 413 while
    /// expanding, without the process allocating the full expansion (the decoder
    /// is read through `take(cap + 1)`, so at most 64 MiB + 1 is ever buffered).
    ///
    /// Non-vacuity: revert `decompress_gzip_capped`'s `if out.len() as u64 >
    /// cap_u64 { return Err(TooLarge) }` and the oversized body flows on to
    /// prost as a truncated buffer, turning the 413 into a 400.
    #[tokio::test]
    async fn gzip_bomb_over_cap_is_rejected_413() {
        let state = state_with_limits(AdmissionLimits::default());
        // Zeros compress ~1000:1, so this is a small body that inflates past the
        // 64 MiB cap. It need not be valid OTLP: the cap trips before decode.
        let bomb_plain = vec![0u8; MAX_DECOMPRESSED_OTLP_BODY_BYTES + 1024];
        let compressed = gzip(&bomb_plain);
        assert!(
            compressed.len() < 1024 * 1024,
            "the compressed bomb must stay small: {}",
            compressed.len()
        );

        let response = export_metrics(State(state), gzip_headers(), Bytes::from(compressed)).await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an over-cap decompression must be 413"
        );
    }

    /// ADR-0084 decision 1: a concatenated multi-member gzip stream ingests ALL
    /// members. The fixture splits one encoded request across two members, so a
    /// decoder that stops after member one (`GzDecoder`) yields a truncated,
    /// undecodable protobuf.
    ///
    /// Non-vacuity: swap `decompress_gzip_capped`'s `MultiGzDecoder` for
    /// `GzDecoder` and only the first half decompresses, so prost sees a
    /// truncated message and the handler returns 400 instead of 200.
    #[tokio::test]
    async fn gzip_multi_member_stream_ingests_all_members() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(500).encode_to_vec();
        let mid = encoded.len() / 2;
        assert!(mid > 0 && mid < encoded.len(), "need a real two-way split");
        // Two independently-framed gzip members, concatenated. Each is a
        // complete gzip stream; MultiGzDecoder decodes both and yields the full
        // `encoded`, GzDecoder would yield only the first half.
        let mut two_member = gzip(&encoded[..mid]);
        two_member.extend_from_slice(&gzip(&encoded[mid..]));

        // Sanity: our own capped decoder reconstructs the whole thing.
        let round_trip =
            decompress_gzip_capped(&two_member, MAX_DECOMPRESSED_OTLP_BODY_BYTES).expect("decodes");
        assert_eq!(round_trip, encoded, "both members must decompress");

        let response = export_metrics(
            State(state.clone()),
            gzip_headers(),
            Bytes::from(two_member),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "both members must ingest as one request"
        );
        let usage = metrics_usage(&state).expect("a metrics usage row");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "the charge covers the whole decompressed stream, not one member"
        );
    }

    /// ADR-0084 decision 1: trailing bytes after a well-formed gzip stream are a
    /// 400, never silently truncated.
    #[tokio::test]
    async fn gzip_trailing_garbage_is_rejected_400() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(10).encode_to_vec();
        let mut body = gzip(&encoded);
        body.extend_from_slice(b"trailing junk not a gzip header");

        let response = export_metrics(State(state), gzip_headers(), Bytes::from(body)).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "trailing bytes after the stream must be 400"
        );
    }

    /// ADR-0084 decision 1: an unsupported single coding, and any multi-coding
    /// list, are 415 (never treated as identity, which would hand prost a
    /// compressed body and produce the misleading 400 this ADR removes).
    #[tokio::test]
    async fn unsupported_content_encoding_is_415() {
        for value in ["deflate", "br", "gzip, gzip", "deflate, gzip"] {
            let state = state_with_limits(AdmissionLimits::default());
            let encoded = compressible_request(10).encode_to_vec();
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(value).unwrap(),
            );
            let response = export_metrics(State(state), headers, Bytes::from(gzip(&encoded))).await;
            assert_eq!(
                response.status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Encoding {value:?} must be 415"
            );
        }
    }

    /// ADR-0084 decision 1/4: the identity path is byte-for-byte unchanged. An
    /// uncompressed body (absent Content-Encoding) is charged its wire length,
    /// exactly as today, and `x-gzip` is honored as a gzip alias.
    #[tokio::test]
    async fn identity_path_charge_is_wire_length() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(50).encode_to_vec();

        let response = export_metrics(
            State(state.clone()),
            HeaderMap::new(),
            Bytes::from(encoded.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let usage = metrics_usage(&state).expect("a metrics usage row");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "identity charge must equal the wire body length, as before this change"
        );
    }

    /// ADR-0084 decision 1: `x-gzip` is an accepted alias for `gzip`.
    #[tokio::test]
    async fn x_gzip_alias_is_accepted() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(50).encode_to_vec();
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("X-Gzip"));
        let response = export_metrics(State(state), headers, Bytes::from(gzip(&encoded))).await;
        assert_eq!(response.status(), StatusCode::OK, "x-gzip must be accepted");
    }

    /// ADR-0084 decision 4: an already-over-rate tenant sending a compressed body
    /// is rejected 429 by the compressed-size pre-check, WITHOUT the decompressor
    /// running.
    ///
    /// The body is deliberately not valid gzip. If the pre-check runs first (as
    /// it must), the request is 429 and the bytes are never fed to the decoder;
    /// if decompression ran before the check, the invalid body would surface as
    /// a 400. Asserting 429 therefore proves the decompressor did not run.
    ///
    /// Non-vacuity: delete the `peek_byte_rate` pre-check in
    /// `admit_and_decode_body`'s gzip arm and this returns 400 (the invalid body
    /// reaches the decoder).
    #[tokio::test]
    async fn over_rate_compressed_body_pre_check_rejects_429_without_decompressing() {
        // A tight byte-rate bucket: burst 16 bytes, so any real body is over.
        let limits = AdmissionLimits {
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: 1,
                burst: 16,
            },
            ..AdmissionLimits::default()
        };
        let state = state_with_limits(limits);
        // Not a gzip stream at all, and larger than the 16-byte burst.
        let not_gzip = Bytes::from(vec![0x42u8; 1024]);

        let response = export_metrics(State(state.clone()), gzip_headers(), not_gzip).await;
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the compressed-size pre-check must reject before decompressing"
        );
        let usage = metrics_usage(&state).expect("a metrics usage row after the rejection");
        assert_eq!(
            usage.requests_rejected_byte_rate_total, 1,
            "the pre-check rejection is counted as a byte-rate rejection"
        );
        assert_eq!(
            usage.bytes_admitted_total, 0,
            "nothing is admitted and no tokens are consumed on the pre-check rejection"
        );
    }
}
