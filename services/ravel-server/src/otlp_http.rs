//! `POST /v1/metrics`, `POST /v1/logs`, and `POST /v1/traces`: OTLP
//! HTTP-protobuf ingest. All three endpoints share this file's tenant
//! resolution, write-mode header, and commit-token header handling; the
//! signal-specific logic lives in [`crate::ingest`], [`crate::logs_ingest`],
//! and [`crate::traces_ingest`].

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use ravel_cpu_gate::WriteGate;
use ravel_ingest::{
    AdmissionController, IngestByteBudget, LogWriteError, SpanWriteError, WriteError,
};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantId};

use crate::ingest::{IngestRequestError, IngestState};
use crate::ingest_admission::{
    IngestPermitHeld, PendingAckHeld, admission_rejection_response,
    ingest_buffer_budget_shed_response, ingest_deferral_cap_response, pending_ack_shed_response,
};
use crate::ingest_concurrency::IngestConcurrencyController;
use crate::ingest_decode::{MAX_REQUEST_BODY_BYTES, admit_and_decode_body};
use crate::ingest_mode::resolve_write_mode;
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
    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1, amended
    /// by issue #1297). The same `Arc` the ingest routers hold via
    /// `with_budget`. The gzip inflate path (`admit_and_decode_body`) charges
    /// the decompressed bytes into it *while* it inflates, one charge per chunk
    /// taken before that chunk is retained, and holds the charge through decode;
    /// it releases the charge once prost has copied the chunks into owned
    /// structs, before the router takes its own buffered charge, so the inflate
    /// charge and the buffered charge never coexist and transient decode memory
    /// is bounded by `--max-ingest-buffer-bytes` the same way buffered memory
    /// is.
    pub budget: Arc<IngestByteBudget>,
    /// The process-wide in-flight ingest-request ceiling, shared
    /// with every OTLP HTTP/gRPC service and Remote Write on this listener
    /// and the mTLS listener. Checked on the request head by
    /// `crate::ingest_admission`, ahead of the body read, tenant resolution
    /// and the layer-2 byte-rate check, so a shed request does none of that
    /// work.
    pub ingest_concurrency: Arc<IngestConcurrencyController>,
    /// The ADR-1702 write gate. A gzip body's inflate runs on it under the
    /// `otlp_http_gzip` site, and an OTAP stream's payload decompression
    /// under `otap_decode`, each sized by a bound on its inflated bytes. `None`
    /// runs both inline on the request task.
    pub write_gate: Option<Arc<WriteGate>>,
    /// The process default strict flush policy (ADR-2793 decision 1),
    /// resolved from `--strict-flush-policy`. [`resolve_write_mode`] applies
    /// it to a request whose `x-ravel-ingest-mode` header does not name a
    /// mode explicitly.
    pub strict_flush_policy: crate::config::StrictFlushPolicy,
}

impl crate::ingest_admission::IngestAdmissionState for GatewayState {
    fn ingest_concurrency(&self) -> &Arc<IngestConcurrencyController> {
        &self.ingest_concurrency
    }

    fn tenant_resolver(&self) -> &Arc<dyn TenantResolver> {
        &self.tenant_resolver
    }

    /// The metrics router's budget. The server builds all three routers from
    /// one `Arc<PendingAckBudget>`, so this is the budget the log and span
    /// routers hold too.
    fn pending_ack_budget(&self) -> &Arc<ravel_ingest::PendingAckBudget> {
        self.ingest.router.pending_ack_budget()
    }

    /// The handlers resolve the mode the same way from the same headers.
    fn strict_write(&self, headers: &HeaderMap) -> bool {
        resolve_write_mode(headers, self.strict_flush_policy) != ravel_ingest::WriteMode::Buffered
    }
}

pub fn router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .route("/v1/traces", post(export_traces))
        // Admission and the credential check run on the request head: see
        // `crate::ingest_admission`. `route_layer` so an unrouted path never
        // takes a permit.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::ingest_admission::admit_ingest_request::<GatewayState>,
        ))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
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

/// The in-flight permit and the tenant both come from
/// [`crate::ingest_admission::admit_ingest_request`], which ran on this
/// request's head before `body` was read (issue #1705). `Extension<TenantId>`
/// is present on every request that reaches here: the middleware is wired
/// onto these routes in [`router`] and returns 401 itself when it cannot
/// resolve one.
async fn export_metrics(
    State(state): State<Arc<GatewayState>>,
    Extension(tenant): Extension<TenantId>,
    Extension(IngestPermitHeld(permit)): Extension<IngestPermitHeld>,
    Extension(PendingAckHeld(pending_ack)): Extension<PendingAckHeld>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mode = resolve_write_mode(&headers, state.strict_flush_policy);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before. On the gzip path
    // `decode_charge` holds the process-wide budget charge for the retained
    // inflate chunks (empty on the identity path); it is released once prost has
    // decoded and freed them, just below (ADR-0069 as amended by issue #1297).
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Metrics, body).await {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    // `decode` consumes the chunked body, so prost's own return frees the
    // retained inflate chunks: it copies them into owned protobuf structs and
    // nothing borrows them afterwards.
    let request = match ExportMetricsServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // Release the inflate charge here, with the chunks already freed, before the
    // router takes its own charge for the normalized batch: the inflate charge
    // and the router's buffered charge never coexist, so a request whose inflate
    // and batch each fit the budget is not shed for their sum (issue #1297).
    drop(decode_charge);

    match crate::ingest::handle_export_releasing(
        &state.ingest,
        tenant,
        mode,
        request,
        now_ns(),
        &permit,
        &pending_ack,
    )
    .await
    {
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
        // The pending strict-ack bound (ADR-2793 decision 4) refuses a strict
        // write before it buffers anything: the same 429 as the in-flight
        // shed, with its own body text.
        Err(IngestRequestError::Write(WriteError::PendingAckLimitReached)) => {
            pending_ack_shed_response()
        }
        Err(IngestRequestError::Write(err @ WriteError::DeferralCapReached)) => {
            ingest_deferral_cap_response(err.to_string())
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(IngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted (e.g. a
        // series value-kind mismatch), so 400 rather than the 503 that would
        // tell a well-behaved exporter to retry forever.
        Err(IngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

/// `POST /v1/logs`. Same shape as [`export_metrics`], down to the status
/// codes: 401 for an unresolvable tenant, 400 for an undecodable body or a
/// non-retryable write the log pipeline rejected, 503 for a retryable write
/// failure.
async fn export_logs(
    State(state): State<Arc<GatewayState>>,
    Extension(tenant): Extension<TenantId>,
    Extension(IngestPermitHeld(permit)): Extension<IngestPermitHeld>,
    Extension(PendingAckHeld(pending_ack)): Extension<PendingAckHeld>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mode = resolve_write_mode(&headers, state.strict_flush_policy);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before.
    // `decode_charge`: see `export_metrics`. Released once decode has consumed
    // and freed the inflate chunks, before the log write below.
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Logs, body).await {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    let request = match ExportLogsServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // See `export_metrics`: decode consumed the chunks and prost owns the
    // decoded structs, so release the charge before the router charges the
    // normalized batch, so the two never coexist (issue #1297).
    drop(decode_charge);

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::logs_ingest::handle_export_logs_releasing(
        &state.logs_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
        &permit,
        &pending_ack,
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
        Err(LogIngestRequestError::Write(LogWriteError::PendingAckLimitReached)) => {
            pending_ack_shed_response()
        }
        Err(LogIngestRequestError::Write(err @ LogWriteError::DeferralCapReached)) => {
            ingest_deferral_cap_response(err.to_string())
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(LogIngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted, so 400 rather
        // than the 503 that would tell a well-behaved exporter to retry
        // forever.
        Err(LogIngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

/// `POST /v1/traces`. Same shape as [`export_metrics`] and [`export_logs`],
/// down to the status codes: 401 for an unresolvable tenant, 400 for an
/// undecodable body or a non-retryable write the span pipeline rejected, 503
/// for a retryable write failure.
async fn export_traces(
    State(state): State<Arc<GatewayState>>,
    Extension(tenant): Extension<TenantId>,
    Extension(IngestPermitHeld(permit)): Extension<IngestPermitHeld>,
    Extension(PendingAckHeld(pending_ack)): Extension<PendingAckHeld>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mode = resolve_write_mode(&headers, state.strict_flush_policy);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): byte rate
    // applies uniformly to every signal including spans (even though spans get
    // no layer-4 admission), charged after decompression on the gzip path.
    // `decode_charge`: see `export_metrics`. Released once decode has consumed
    // and freed the inflate chunks, before the span write below.
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Spans, body).await {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    let request = match ExportTraceServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // See `export_metrics`: decode consumed the chunks and prost owns the
    // decoded structs, so release the charge before the router charges the
    // normalized batch, so the two never coexist (issue #1297).
    drop(decode_charge);

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::traces_ingest::handle_export_traces_releasing(
        &state.traces_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
        &permit,
        &pending_ack,
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
        Err(SpanIngestRequestError::Write(SpanWriteError::PendingAckLimitReached)) => {
            pending_ack_shed_response()
        }
        Err(SpanIngestRequestError::Write(err @ SpanWriteError::DeferralCapReached)) => {
            ingest_deferral_cap_response(err.to_string())
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(SpanIngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted, so 400 rather
        // than the 503 that would tell a well-behaved exporter to retry
        // forever.
        Err(SpanIngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
pub(crate) mod tests {
    use std::io::Write as _;

    use axum::http::header;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use ravel_ingest::{
        AdmissionController, AdmissionLimits, IngestByteBudget, IngestByteBudgetLimit,
        IngestConfig, IngestRouter, LogIngestRouter, PendingAckBudget, RateLimit, SpanIngestRouter,
        SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
    use ravel_query::http::AuthError;

    use super::*;
    use crate::ingest_byte_metrics::IngestByteMetrics;
    use crate::ingest_concurrency::{IngestConcurrencyController, IngestConcurrencyLimit};
    use crate::ingest_decode::{MAX_DECOMPRESSED_OTLP_BODY_BYTES, decompress_gzip_capped_charged};
    use crate::normalize_reject_metrics::NormalizeRejectMetrics;

    pub(crate) const TENANT: &str = "acme";

    /// Resolves every request to the same fixed tenant, so a handler test can
    /// find that tenant's admission usage row without threading a token.
    struct FixedTenantResolver(TenantId);

    impl TenantResolver for FixedTenantResolver {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(self.0.clone())
        }
    }

    /// A `GatewayState` over `store`, whose admission controller carries
    /// `limits` as its per-tenant defaults (so the fixed tenant gets them), and
    /// whose metrics, log, and span routers all share `budget` as their ingest
    /// buffer byte budget. The routers are all real, so a handler runs the full
    /// ingest path and returns the same status a client would see.
    pub(crate) fn state_with_store_and_budget(
        store: Arc<dyn ObjectStoreBackend>,
        limits: AdmissionLimits,
        budget: Arc<IngestByteBudget>,
    ) -> Arc<GatewayState> {
        state_with_pending_ack(store, limits, budget, Arc::new(PendingAckBudget::new(0)))
    }

    /// [`state_with_store_and_budget`] whose three routers share `pending_ack`
    /// as their pending strict-ack budget, as the server builds them, so the
    /// admission layer's `pending_ack_budget` is each router's.
    pub(crate) fn state_with_pending_ack(
        store: Arc<dyn ObjectStoreBackend>,
        limits: AdmissionLimits,
        budget: Arc<IngestByteBudget>,
        pending_ack: Arc<PendingAckBudget>,
    ) -> Arc<GatewayState> {
        let metrics_router = Arc::new(
            IngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Signal::Metrics,
                Arc::new(SystemClock),
                pending_ack.clone(),
            )
            .with_budget(budget.clone()),
        );
        let log_router = Arc::new(
            LogIngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Arc::new(SystemClock),
                pending_ack.clone(),
            )
            .with_budget(budget.clone()),
        );
        let span_router = Arc::new(
            SpanIngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Arc::new(SystemClock),
                pending_ack.clone(),
            )
            .with_budget(budget.clone()),
        );
        let admission = Arc::new(AdmissionController::new(Arc::new(SystemClock), limits));
        // One instance across all three signals, as in `gateway_state`: a test
        // that reads it sees what a scrape would.
        let normalize_metrics = Arc::new(NormalizeRejectMetrics::new());
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
                normalize_metrics: normalize_metrics.clone(),
                budget: budget.clone(),
            },
            logs_ingest: crate::logs_ingest::LogIngestState {
                router: log_router,
                limits: LogIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
                normalize_metrics: normalize_metrics.clone(),
                budget: budget.clone(),
            },
            traces_ingest: crate::traces_ingest::SpanIngestState {
                router: span_router,
                limits: SpanIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
                normalize_metrics: normalize_metrics.clone(),
                budget: budget.clone(),
            },
            admission,
            budget,
            ingest_concurrency: IngestConcurrencyController::shared(
                IngestConcurrencyLimit::Unlimited,
            ),
            ingest_byte_metrics: Arc::new(IngestByteMetrics::new()),
            write_gate: None,
            strict_flush_policy: crate::config::StrictFlushPolicy::Aged,
        })
    }

    /// A `GatewayState` over `MemoryStore`, whose admission controller carries
    /// `limits` as its per-tenant defaults and whose routers share an
    /// unlimited ingest buffer budget (today's default).
    ///
    /// `pub(crate)`: reused by `otlp_grpc`'s test module, so the gRPC
    /// permanent-write-error status test exercises the same fixture as the
    /// HTTP one instead of a second hand-built `GatewayState`.
    pub(crate) fn state_with_limits(limits: AdmissionLimits) -> Arc<GatewayState> {
        state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            limits,
            IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        )
    }

    /// A `GatewayState` over `store`, with default admission limits and an
    /// unlimited ingest buffer budget: for tests that fault-inject the store
    /// rather than the admission or budget layers.
    fn state_with_store(store: Arc<dyn ObjectStoreBackend>) -> Arc<GatewayState> {
        state_with_store_and_budget(
            store,
            AdmissionLimits::default(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        )
    }

    /// A `GatewayState` over `MemoryStore`, with default admission limits and
    /// an ingest buffer budget of zero: any write charges it and is shed
    /// before touching a shard.
    fn state_with_zero_budget() -> Arc<GatewayState> {
        state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(0)),
        )
    }

    /// A metrics export that compresses well: `points` copies of one gauge data
    /// point, so the decompressed protobuf is many times the gzip size. The
    /// distinction matters for the charging tests, which assert the byte rate is
    /// charged the decompressed length, not the compressed one.
    pub(crate) fn compressible_request(points: usize) -> ExportMetricsServiceRequest {
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

    /// A metrics export carrying two metrics of the same name and empty
    /// attributes, so they share one `series_id`: one a scalar `Gauge` point,
    /// the other a native (`ExponentialHistogram`) point. `ingest.rs`'s
    /// `handle_export` routes both points through one
    /// `write_values_with_exemplars` call, so `shard.rs`'s `merge` sees both
    /// claims for the same series in one batch and returns
    /// `WriteError::SeriesValueKindMismatch` -- a non-retryable, permanent
    /// rejection of the input itself.
    ///
    /// `pub(crate)`: reused by `otlp_grpc`'s test module (see
    /// `state_with_limits`).
    pub(crate) fn value_kind_mismatch_request() -> ExportMetricsServiceRequest {
        let ts = now_ns() as u64;
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "conflicting_series".to_string(),
                            data: Some(MetricData::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: ts,
                                    value: Some(NumberValue::AsDouble(1.0)),
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "conflicting_series".to_string(),
                            data: Some(MetricData::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    time_unix_nano: ts,
                                    ..Default::default()
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One minimal, valid log export: a single record, so a handler exercises
    /// the full decode-and-write path without tripping any admission or
    /// decode rejection ahead of the write itself.
    fn minimal_log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: now_ns() as u64,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One minimal, valid trace export: a single span, so a handler exercises
    /// the full decode-and-write path without tripping any admission or
    /// decode rejection ahead of the write itself.
    fn minimal_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![7u8; 16],
                        span_id: vec![3u8; 8],
                        name: "span".to_string(),
                        start_time_unix_nano: now_ns() as u64,
                        end_time_unix_nano: now_ns() as u64,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One-member gzip of `data`.
    pub(crate) fn gzip(data: &[u8]) -> Vec<u8> {
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

    /// The tenant the admission middleware resolves from the request headers
    /// before the body extractor runs (issue #1705). A test that calls a
    /// handler directly stands in for that middleware, since the handler no
    /// longer resolves a tenant of its own.
    fn admitted_tenant() -> Extension<TenantId> {
        Extension(TenantId::new(TENANT))
    }

    /// The marker a handler called directly sees: no layer admitted it, so
    /// the handle holds nothing and its release is a no-op.
    fn no_permit() -> Extension<IngestPermitHeld> {
        Extension(IngestPermitHeld(
            crate::ingest_concurrency::IngestPermitHandle::default(),
        ))
    }

    /// The pending-ack marker a handler called directly sees: an empty
    /// handle, so the router takes the permit itself for a strict write.
    fn no_pending_ack() -> Extension<PendingAckHeld> {
        Extension(PendingAckHeld(
            crate::ingest_admission::PendingAckHandle::default(),
        ))
    }

    pub(crate) fn gzip_headers() -> HeaderMap {
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
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
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
    /// Non-vacuity: delete `decompress_gzip_capped_charged`'s projected-size cap
    /// check (`if produced + read as u64 > cap_u64 { return Err(TooLarge) }`) and
    /// the oversized body flows on to prost as a truncated buffer, turning the
    /// 413 into a 400.
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

        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            gzip_headers(),
            Bytes::from(compressed),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an over-cap decompression must be 413"
        );
    }

    /// Issue #1297 review finding 1: the decompressed cap is checked BEFORE the
    /// budget charge, so a body inflating one byte past the cap is answered with
    /// the documented 413 even when the budget has exactly the cap of headroom
    /// and the crossing byte would not fit. The shed counter stays at zero (the
    /// request was refused by the cap, not by backpressure) and every partial
    /// charge taken while inflating is refunded, so the in-flight gauge returns
    /// to zero.
    ///
    /// The ceiling is deliberately the cap exactly: that is the configuration in
    /// which charging the crossing byte first is observable, because the byte
    /// past the cap is also the byte past the ceiling.
    ///
    /// Non-vacuity: move the cap check back after the loop (charge and retain
    /// every chunk, then `if produced > cap_u64 { return Err(TooLarge) }`) and
    /// the crossing byte is charged before the cap is consulted, so the budget
    /// rejects it and the handler answers 429 where 413 is asserted.
    #[tokio::test]
    async fn over_cap_gzip_body_is_413_even_when_the_ceiling_equals_the_cap() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(
            MAX_DECOMPRESSED_OTLP_BODY_BYTES as u64,
        ));
        let state = state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            budget.clone(),
        );
        // Exactly one byte past the cap: zeros, so the compressed body stays
        // small and the cap (not the wire-body limit) is what trips.
        let one_past_cap = vec![0u8; MAX_DECOMPRESSED_OTLP_BODY_BYTES + 1];
        let compressed = gzip(&one_past_cap);

        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            gzip_headers(),
            Bytes::from(compressed),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an over-cap inflate is refused by the cap, so 413 and never the budget's 429"
        );
        assert_eq!(
            budget.shed_total(),
            0,
            "the cap rejection is not a budget shed, so the shed counter is untouched"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "every chunk charged while inflating is refunded on the cap rejection"
        );
    }

    /// ADR-0084 decision 1: a concatenated multi-member gzip stream ingests ALL
    /// members. The fixture splits one encoded request across two members, so a
    /// decoder that stops after member one (`GzDecoder`) yields a truncated,
    /// undecodable protobuf.
    ///
    /// Non-vacuity: swap `decompress_gzip_capped_charged`'s `MultiGzDecoder` for
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

        // Sanity: our own capped decoder reconstructs the whole thing. An
        // unlimited budget never sheds, so this exercises decode alone.
        let unlimited = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let (round_trip, _charges) = decompress_gzip_capped_charged(
            &two_member,
            MAX_DECOMPRESSED_OTLP_BODY_BYTES,
            &unlimited,
        )
        .expect("decodes");
        assert_eq!(round_trip.concat(), encoded, "both members must decompress");

        let response = export_metrics(
            State(state.clone()),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
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

        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            gzip_headers(),
            Bytes::from(body),
        )
        .await;
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
            let response = export_metrics(
                State(state),
                admitted_tenant(),
                no_permit(),
                no_pending_ack(),
                headers,
                Bytes::from(gzip(&encoded)),
            )
            .await;
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
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
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
        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            headers,
            Bytes::from(gzip(&encoded)),
        )
        .await;
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

        let response = export_metrics(
            State(state.clone()),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            gzip_headers(),
            not_gzip,
        )
        .await;
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

    /// A permanent, non-retryable write rejection (`WriteError::SeriesValueKindMismatch`)
    /// must be 400, not the 503 a retryable failure gets: a well-behaved
    /// exporter that retries only on 503/429 would otherwise retry a request
    /// that can never succeed.
    ///
    /// Non-vacuity: change `export_metrics`'s final `Err(IngestRequestError::Write(write_err))`
    /// arm back to `(StatusCode::SERVICE_UNAVAILABLE, ...)` and this fails,
    /// observing 503 where 400 is asserted.
    #[tokio::test]
    async fn series_value_kind_mismatch_returns_400_not_503() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = value_kind_mismatch_request().encode_to_vec();

        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            HeaderMap::new(),
            Bytes::from(encoded),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "a series value-kind mismatch is a permanent, non-retryable rejection"
        );
    }

    /// A retryable write failure (`WriteError::Abandoned`, from a store that
    /// permanently fails every `Put`) must still be 503: the reorder that
    /// added the 400 arm must not swallow the retryable arm ahead of it.
    ///
    /// Non-vacuity: delete the `if write_err.is_retryable()` guard's arm (or
    /// reorder it after the bare `Write(write_err)` arm) and this fails,
    /// observing 400 where 503 is asserted.
    #[tokio::test]
    async fn a_retryable_write_error_still_returns_503() {
        let plan = FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("boom".to_string()),
        ));
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let state = state_with_store(store);
        let encoded = compressible_request(10).encode_to_vec();

        let response = export_metrics(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            HeaderMap::new(),
            Bytes::from(encoded),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "an abandoned flush is retryable at the client"
        );
    }

    /// The dedicated buffer-budget-shed 429 arm must survive the reorder that
    /// added the retryable-vs-permanent split below it, for all three
    /// signals: it is easy to accidentally fold into the new
    /// `is_retryable()` guard (`BufferBudgetExceeded` is itself retryable),
    /// which would still return 503 instead of the 429 backpressure signal
    /// clients are meant to see.
    ///
    /// Non-vacuity: delete a handler's dedicated
    /// `Write(*WriteError::BufferBudgetExceeded)` arm (letting it fall
    /// through to the `is_retryable()` arm) and that handler's assertion
    /// fails, observing 503 where 429 is asserted.
    #[tokio::test]
    async fn buffer_budget_shed_still_returns_429() {
        let state = state_with_zero_budget();

        let metrics_response = export_metrics(
            State(state.clone()),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            HeaderMap::new(),
            Bytes::from(compressible_request(1).encode_to_vec()),
        )
        .await;
        assert_eq!(
            metrics_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero metrics buffer budget must shed as 429, not 503"
        );

        let logs_response = export_logs(
            State(state.clone()),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            HeaderMap::new(),
            Bytes::from(minimal_log_request().encode_to_vec()),
        )
        .await;
        assert_eq!(
            logs_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero log buffer budget must shed as 429, not 503"
        );

        let traces_response = export_traces(
            State(state),
            admitted_tenant(),
            no_permit(),
            no_pending_ack(),
            HeaderMap::new(),
            Bytes::from(minimal_trace_request().encode_to_vec()),
        )
        .await;
        assert_eq!(
            traces_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero span buffer budget must shed as 429, not 503"
        );
    }
}
