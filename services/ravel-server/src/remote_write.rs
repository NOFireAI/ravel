//! `POST /api/v1/write`: Prometheus Remote Write 1.0 and 2.0 ingest
//! (ADR-0015).
//!
//! This surface is strict-mode only regardless of the OTLP ingest mode
//! header: a Remote Write sender expects a 2xx to mean the samples are
//! durable, so the buffered-mode override honored by `otlp_http`/`otlp_grpc`
//! is never read here.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ingest_concurrency::IngestConcurrencyController;
use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use ravel_ingest::{
    AdmissionController, Clock, IngestByteBudget, IngestByteCharge, IngestPoint, IngestRouter,
    IngestValue, RequestRejection, WriteError, WriteMode, plausible_ingest_clock,
};
use ravel_otlp::IngestLimits;
use ravel_query::http::TenantResolver;
use ravel_remote_write::{
    ResolvedRequest, Rw1DecodeError, Rw2DecodeError, RwNormalizeOutput, normalize_resolved,
};
use ravel_types::{SeriesId, Signal, TenantId};

/// Layer 1 (ADR-0051 section 2): the compressed-wire-body cap, ahead of
/// [`MAX_DECOMPRESSED_PAYLOAD_BYTES`]'s existing post-Snappy cap.
const MAX_COMPRESSED_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

const SAMPLES_WRITTEN_HEADER: &str = "x-prometheus-remote-write-samples-written";
const HISTOGRAMS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-histograms-written";
const EXEMPLARS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-exemplars-written";
const RETRY_AFTER_HEADER: &str = "retry-after";

/// Snappy decompression cap for a single Remote Write request body, applied
/// before allocation (same discipline as `ravel-otap`'s
/// `max_decompressed_payload_bytes`).
const MAX_DECOMPRESSED_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Why a Remote Write body's snappy inflate could not be charged against the
/// process-wide ingest byte budget.
#[derive(Debug, PartialEq, Eq)]
enum SnappyChargeError {
    /// Charging the decompressed bytes would push the budget past its ceiling
    /// (ADR-0069, amended by issue #1419): HTTP 429, the same shed response the
    /// buffered charge takes.
    Shed,
}

/// Charges the process-wide ingest byte budget for the bytes this snappy body
/// will inflate to, **before** [`ravel_remote_write`] allocates the output
/// buffer (ADR-0069 as amended by issue #1419).
///
/// Snappy block format declares its decompressed length in a varint header, so
/// unlike gzip the exact figure is known without inflating anything:
/// `snap::raw::decompress_len` reads only that header. `ravel-remote-write`'s
/// `snappy::decompress` then allocates `vec![0u8; len]` and truncates to the
/// bytes actually written, which for a well-formed body is `len` exactly. The
/// charge is therefore the actual inflated length and the actual size of the
/// allocation it pays for, taken one step ahead of it: no over-charge of a
/// well-compressing sender, no undercount, and no growing buffer whose spare
/// capacity would escape the count (the failure mode `otlp_http`'s
/// [`crate::otlp_http`] `ChunkedBody` exists to avoid on the gzip path, where no
/// declared length is available and the charge has to be taken chunk by chunk).
///
/// The order is cap, then budget, matching the gzip path: a body whose header
/// claims more than `cap` takes no charge and is left to the decoder, which
/// rejects it with the existing typed snappy error (HTTP 400). A tight budget
/// therefore cannot turn an over-cap body into a 429, and the charge for such a
/// body is never taken at all.
///
/// A malformed header is likewise left to the decoder rather than guessed at:
/// nothing is charged and the decode call that follows returns its own typed
/// error. An empty body decompresses to nothing and takes no charge, mirroring
/// `snappy::decompress`'s own empty-input short circuit.
///
/// The caller holds the returned guard through protobuf decode and
/// normalization and drops it immediately before the router takes its own
/// buffered charge, so the two never coexist (issue #1297 finding 3); on any
/// error path the guard drops and refunds the budget exactly.
fn charge_snappy_inflate(
    body: &[u8],
    cap: usize,
    budget: &Arc<IngestByteBudget>,
) -> Result<Option<IngestByteCharge>, SnappyChargeError> {
    if body.is_empty() {
        return Ok(None);
    }
    let Ok(len) = snap::raw::decompress_len(body) else {
        return Ok(None);
    };
    if len > cap {
        return Ok(None);
    }
    match budget.try_charge(len as u64) {
        Ok(charge) => Ok(Some(charge)),
        Err(_) => Err(SnappyChargeError::Shed),
    }
}

/// Snappy-decompresses and protobuf-decodes `body` for `version`, holding a
/// process-wide ingest byte budget charge for the inflated bytes across the
/// whole call (ADR-0069 as amended by issue #1419).
///
/// Returns the decoded request alongside the charge guard, the same shape
/// `otlp_http::admit_and_decode_body` returns for the gzip path. The caller
/// keeps the guard alive through normalization and drops it before the router
/// charges the normalized batch.
///
/// `Err` says which response to return: 429 for a budget shed, 400 for a body
/// the decoder rejected (unchanged from before this charge existed).
fn decode_body_charged(
    body: &Bytes,
    version: RemoteWriteVersion,
    budget: &Arc<IngestByteBudget>,
) -> Result<(ResolvedRequest, Option<IngestByteCharge>), DecodeChargeError> {
    // Charge before the decoder allocates: a body whose inflate would cross the
    // ceiling is shed here, so the process never grows by the expansion.
    let charge = match charge_snappy_inflate(body, MAX_DECOMPRESSED_PAYLOAD_BYTES, budget) {
        Ok(charge) => charge,
        Err(SnappyChargeError::Shed) => return Err(DecodeChargeError::Shed),
    };
    let resolved = match version {
        RemoteWriteVersion::V1 => {
            ravel_remote_write::decode_write_request(body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                .map_err(|err: Rw1DecodeError| err.to_string())
        }
        RemoteWriteVersion::V2 => {
            ravel_remote_write::decode_request(body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                .map_err(|err: Rw2DecodeError| err.to_string())
        }
    };
    match resolved {
        // On this path `charge` drops here, refunding the budget exactly.
        Err(message) => Err(DecodeChargeError::Decode(message)),
        Ok(resolved) => Ok((resolved, charge)),
    }
}

/// Why a Remote Write body did not become a decoded request. Carried as a small
/// value rather than a built [`Response`], which the `result_large_err` lint
/// rejects in an `Err` variant.
#[derive(Debug)]
enum DecodeChargeError {
    /// The declared inflate would push the ingest byte budget past its ceiling.
    /// Taken before the expansion is allocated.
    Shed,
    /// The decoder rejected the body, with its own message.
    Decode(String),
}

impl IntoResponse for DecodeChargeError {
    fn into_response(self) -> Response {
        match self {
            DecodeChargeError::Shed => ingest_buffer_budget_shed_response(),
            DecodeChargeError::Decode(message) => {
                (StatusCode::BAD_REQUEST, message).into_response()
            }
        }
    }
}

/// 429 for a request shed by the process-wide ingest buffer byte budget
/// (ADR-0069 decision 1, amended by issue #1419): the body was refused before
/// its snappy expansion was allocated, so no shard was touched and no commit
/// token issued. Same 429 + `Retry-After` shape as the byte-rate rejection and
/// the in-flight shed, and the same shape [`write_error_response`] gives the
/// router's own `BufferBudgetExceeded`.
fn ingest_buffer_budget_shed_response() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "ingest buffer byte budget reached",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(RETRY_AFTER_HEADER, value);
    }
    response
}

/// Retry-After seconds advertised on retryable failures. No per-error
/// estimate is available from `WriteError` today, so this is a fixed,
/// conservative value rather than a computed one.
const RETRY_AFTER_SECONDS: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteWriteVersion {
    V1,
    V2,
}

/// Accepted/rejected/dropped counters for the Remote Write surface
/// (docs/ingest.md "Metrics"). Scoped to this module: no other ingest
/// surface in `ravel-server` has a metrics struct yet, so this does not
/// attempt to generalize beyond what Remote Write itself needs.
#[derive(Default)]
pub struct RemoteWriteMetrics {
    requests_accepted: AtomicU64,
    requests_rejected: AtomicU64,
    points_accepted: AtomicU64,
    points_dropped: AtomicU64,
    metadata_dropped: AtomicU64,
    created_timestamps_dropped: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteWriteMetricsSnapshot {
    pub requests_accepted: u64,
    pub requests_rejected: u64,
    pub points_accepted: u64,
    pub points_dropped: u64,
    /// Metric metadata entries accepted-and-dropped (no metadata store yet,
    /// ADR-0015). Not a data point; kept separate from `points_dropped`
    /// rather than folded into it.
    pub metadata_dropped: u64,
    /// Created/start timestamps accepted-and-dropped (no storage for them
    /// yet, ADR-0017). Not a data point; kept separate from `points_dropped`
    /// rather than folded into it.
    pub created_timestamps_dropped: u64,
}

impl RemoteWriteMetrics {
    fn record_request_rejected(&self) {
        self.requests_rejected.fetch_add(1, Ordering::Relaxed);
    }

    fn record_request_accepted(
        &self,
        points_accepted: u64,
        points_dropped: u64,
        metadata_dropped: u64,
        created_timestamps_dropped: u64,
    ) {
        self.requests_accepted.fetch_add(1, Ordering::Relaxed);
        self.points_accepted
            .fetch_add(points_accepted, Ordering::Relaxed);
        self.points_dropped
            .fetch_add(points_dropped, Ordering::Relaxed);
        self.metadata_dropped
            .fetch_add(metadata_dropped, Ordering::Relaxed);
        self.created_timestamps_dropped
            .fetch_add(created_timestamps_dropped, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RemoteWriteMetricsSnapshot {
        RemoteWriteMetricsSnapshot {
            requests_accepted: self.requests_accepted.load(Ordering::Relaxed),
            requests_rejected: self.requests_rejected.load(Ordering::Relaxed),
            points_accepted: self.points_accepted.load(Ordering::Relaxed),
            points_dropped: self.points_dropped.load(Ordering::Relaxed),
            metadata_dropped: self.metadata_dropped.load(Ordering::Relaxed),
            created_timestamps_dropped: self.created_timestamps_dropped.load(Ordering::Relaxed),
        }
    }
}

pub struct RemoteWriteState {
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub router: Arc<IngestRouter>,
    pub limits: IngestLimits,
    pub ack_deadline: std::time::Duration,
    pub metrics: RemoteWriteMetrics,
    /// Tenant admission (ADR-0051): byte-rate (layer 2) and
    /// series-creation-rate/active-series-cap (layer 4). Remote Write's
    /// partial-admission semantics are pinned: 429 is reserved for rate
    /// limits (byte rate, series-creation rate), never for an active-series
    /// cap breach, which instead reduces the written count in a 2xx (no
    /// partial-success message on this surface).
    pub admission: Arc<AdmissionController>,
    /// Recovery-manifest writer (ADR-0050 section 3), `Some` only on a keyed
    /// bucket. Ensured before the write; `None` (unkeyed) is a no-op.
    pub recovery: Option<Arc<crate::tenancy::RecoveryManifestWriter>>,
    /// Durable shard_count provisioning-record writer (ADR-0050 section 5),
    /// pins the (tenant, Metrics) record on the tenant's first write.
    pub provisioning: Option<Arc<crate::provisioning::ProvisioningRecordWriter>>,
    /// The process-wide in-flight ingest-request ceiling, the
    /// same shared controller `otlp_http::GatewayState` carries. Checked
    /// first in [`remote_write`], ahead of tenant resolution.
    pub ingest_concurrency: Arc<IngestConcurrencyController>,
    /// Injected receiver clock read at admission time (CLAUDE.md time
    /// injection; ADR-0051 amendment). In production this is
    /// `SystemClock`, so behavior is identical to the previous internal
    /// `SystemTime::now()`; tests supply a fixed sub-floor clock to exercise
    /// the receiver-clock plausibility floor deterministically.
    pub clock: Arc<dyn Clock>,
    /// The one-per-process metric metadata sink (ADR-0085 decision 1), the same
    /// `Arc` the OTLP and OTAP surfaces hold. RW1 and RW2 both decode
    /// `(family, type, help, unit)` already; this is where the decoded tuples
    /// stop being discarded. `None` captures nothing.
    pub metadata_sink: Option<Arc<ravel_ingest::MetadataSink>>,
    /// The process-wide ingest byte budget (ADR-0069 decision 1), the same
    /// shared budget `otlp_http::GatewayState` and the router carry. The snappy
    /// inflate of the request body is charged against it before the
    /// decompressed buffer is allocated and held through decode and
    /// normalization, so the expansion is inside the declared bound rather than
    /// a transient outside it (issue #1419).
    pub budget: Arc<IngestByteBudget>,
}

pub fn router(state: Arc<RemoteWriteState>) -> Router {
    Router::new()
        .route("/api/v1/write", post(remote_write))
        .layer(DefaultBodyLimit::max(MAX_COMPRESSED_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Data points dropped for the Remote Write "points accepted/dropped"
/// counters: the rejected-count sum already includes native histogram
/// rejections (`RwRejection::NativeHistogramUnsupported`), so it must not be
/// added again. Metadata entries and created/start timestamps are not data
/// points and are reported through their own counters instead.
fn compute_points_dropped(normalized: &RwNormalizeOutput) -> u64 {
    normalized
        .rejected
        .iter()
        .map(|r| r.rejected_count() as u64)
        .sum::<u64>()
        + normalized.exemplars_dropped as u64
}

/// Negotiates the Remote Write protocol version: content-type first (the
/// `proto=` parameter Prometheus sends), then the
/// `X-Prometheus-Remote-Write-Version` header, per ADR-0015. Returns `None`
/// for anything else, which the caller maps to 415.
fn negotiate_version(headers: &HeaderMap) -> Option<RemoteWriteVersion> {
    if let Some(content_type) = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        let lower = content_type.to_ascii_lowercase();
        if lower.contains("io.prometheus.write.v2.request") {
            return Some(RemoteWriteVersion::V2);
        }
        if lower.contains("prometheus.writerequest") {
            return Some(RemoteWriteVersion::V1);
        }
    }

    match headers
        .get("x-prometheus-remote-write-version")
        .and_then(|v| v.to_str().ok())
    {
        Some(v) if v.starts_with("2.") => Some(RemoteWriteVersion::V2),
        Some(v) if v.starts_with("0.1") => Some(RemoteWriteVersion::V1),
        _ => None,
    }
}

fn write_error_response(err: WriteError) -> Response {
    // The buffer-budget shed (ADR-0069) is admission backpressure, not a
    // durability failure: 429 + `Retry-After`, matching the byte-rate
    // rejection and the in-flight shed, rather than the 503 the other
    // retryable write failures take.
    if matches!(err, WriteError::BufferBudgetExceeded) {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, err.to_string()).into_response();
        if let Ok(value) =
            HeaderValue::from_str(&INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string())
        {
            response.headers_mut().insert(RETRY_AFTER_HEADER, value);
        }
        return response;
    }
    if err.is_retryable() {
        let mut response = (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response();
        if let Ok(value) = HeaderValue::from_str(&RETRY_AFTER_SECONDS.to_string()) {
            response.headers_mut().insert(RETRY_AFTER_HEADER, value);
        }
        response
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
    }
}

/// Layer 2/layer 4 rate-limit rejection (ADR-0051 section 1): 429 with
/// `Retry-After` in whole seconds (rounded up, minimum 1). Never used for an
/// active-series-cap breach, which is a per-series filter, not a whole-request
/// rejection.
fn admission_rejection_response(rejection: RequestRejection) -> Response {
    let mut response =
        (StatusCode::TOO_MANY_REQUESTS, rejection.reason.to_string()).into_response();
    let retry_after_secs = retry_after_seconds(rejection.retry_after_ns);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(RETRY_AFTER_HEADER, value);
    }
    response
}

fn retry_after_seconds(retry_after_ns: i64) -> u64 {
    let ns = retry_after_ns.max(0) as u64;
    ns.div_ceil(1_000_000_000).max(1)
}

/// A fixed `Retry-After` for the process-wide in-flight shed,
/// the same rationale as [`RETRY_AFTER_SECONDS`] above: no per-caller refill
/// time is tracked, and a slot can free up as soon as any in-flight request
/// completes, so a short fixed wait is the right shape.
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
        response.headers_mut().insert(RETRY_AFTER_HEADER, value);
    }
    response
}

async fn remote_write(
    State(state): State<Arc<RemoteWriteState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant: TenantId = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => {
            state.metrics.record_request_rejected();
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Receiver-clock plausibility (ADR-0051 amendment): the injected
    // clock is read once and reused for every admission decision in this
    // handler, checked before any per-record work. Whole-request 503, counted
    // reason="clock"; the fault is the replica's, and a retry against a
    // healthy one succeeds.
    let now_ns = state.clock.now_ns();
    if let Err(msg) = plausible_ingest_clock(now_ns) {
        state
            .admission
            .record_clock_rejection(&tenant, Signal::Metrics);
        state.metrics.record_request_rejected();
        let mut response = (StatusCode::SERVICE_UNAVAILABLE, msg).into_response();
        if let Ok(value) = HeaderValue::from_str(&RETRY_AFTER_SECONDS.to_string()) {
            response.headers_mut().insert(RETRY_AFTER_HEADER, value);
        }
        return response;
    }

    // Record the tenant's recovery manifest on its first write (ADR-0050
    // section 3), best-effort and off the durability path.
    crate::tenancy::ensure_recovery_manifest(&state.recovery, &tenant, now_ns).await;

    // Pin/validate the (tenant, Metrics) shard_count provisioning record on
    // first write (ADR-0050 section 5). A hard mismatch fails this request with
    // a 500; a store blip or corrupt record is logged and ingest proceeds.
    if let Err(err) = crate::provisioning::ensure_provisioning_record(
        &state.provisioning,
        &tenant,
        Signal::Metrics,
        now_ns,
    )
    .await
    {
        state.metrics.record_request_rejected();
        return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
    }

    // Layer 2 (ADR-0051 section 2): byte rate on the compressed wire body,
    // before decode, whole-request rejection with no tokens consumed.
    // Remote Write carries only metrics.
    if let Err(rejection) =
        state
            .admission
            .check_byte_rate(&tenant, Signal::Metrics, body.len() as u64, now_ns)
    {
        state.metrics.record_request_rejected();
        return admission_rejection_response(rejection);
    }

    let Some(version) = negotiate_version(&headers) else {
        state.metrics.record_request_rejected();
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unknown or missing Remote Write content type / version header",
        )
            .into_response();
    };

    // The snappy expansion is charged against the process-wide ingest byte
    // budget before it is allocated (ADR-0069 as amended by issue #1419). The
    // guard lives until just before the router takes its own charge below.
    let (resolved, inflate_charge) = match decode_body_charged(&body, version, &state.budget) {
        Ok(decoded) => decoded,
        Err(error) => {
            state.metrics.record_request_rejected();
            return error.into_response();
        }
    };

    // Metric metadata (ADR-0085 decision 1), captured before `resolved` is
    // consumed by normalization. Cloned rather than moved out: the decoded
    // metadata is part of the request `normalize_resolved` accounts for
    // (`metadata_dropped` below), so taking it would change what this surface
    // reports. `observe` is synchronous, does no I/O, and cannot fail, so the
    // 2xx this handler eventually returns depends only on the data write.
    let metadata = resolved.metadata.clone();
    if let Some(sink) = &state.metadata_sink {
        sink.observe(&tenant, tenant.hash(), metadata);
    }

    // Strict mode only: a Remote Write 2xx must mean durable, so the
    // buffered-mode header override is never consulted on this surface.
    let normalized = normalize_resolved(&tenant, resolved, &state.limits, now_ns);
    let mut points_dropped = compute_points_dropped(&normalized);
    let metadata_dropped = normalized.metadata_dropped as u64;
    let created_timestamps_dropped = normalized.created_timestamps_dropped as u64;

    let mut ingest_points: Vec<IngestPoint> =
        Vec::with_capacity(normalized.points.len() + normalized.histogram_points.len());
    ingest_points.extend(normalized.points.into_iter().map(IngestPoint::from));
    ingest_points.extend(
        normalized
            .histogram_points
            .into_iter()
            .map(IngestPoint::from),
    );

    // Layer 4 (ADR-0051 section 1), pinned semantics: series-creation-rate
    // is the only whole-request rejection (429, no tokens consumed); the
    // active-series cap that follows only ever reduces the written count in
    // the eventual 2xx response, never producing a 4xx of its own.
    let now = now_ns;
    let candidate_series: Vec<SeriesId> = ingest_points.iter().map(|p| p.series_id).collect();
    if let Err(rejection) =
        state
            .admission
            .check_series_creation_rate(&tenant, &candidate_series, now)
    {
        state.metrics.record_request_rejected();
        return admission_rejection_response(rejection);
    }
    let admission = state.admission.admit_series(&tenant, candidate_series, now);
    if !admission.rejected.is_empty() {
        let admitted: HashSet<SeriesId> = admission.admitted.into_iter().collect();
        ingest_points.retain(|p| admitted.contains(&p.series_id));
        points_dropped += admission.rejected.len() as u64;
    }

    // The RW2 stats headers count the two admitted point kinds separately: a
    // native histogram is one
    // written histogram, not one written sample. Computed from the
    // admission-filtered points, so a series-cap rejection above is reflected
    // in the written counts. Histograms became writable with RSEG v5, so this
    // is where the histograms-written
    // header stops being a constant zero.
    let samples_written = ingest_points
        .iter()
        .filter(|p| matches!(p.value, IngestValue::Scalar(_)))
        .count() as u64;
    let histograms_written = ingest_points
        .iter()
        .filter(|p| matches!(p.value, IngestValue::Histogram(_)))
        .count() as u64;
    let points_accepted = samples_written + histograms_written;

    let tenant_hash = tenant.hash();
    // Released before the router charges the normalized batch, so the inflate
    // charge and the router's buffered charge never coexist for the same
    // request (issue #1297 finding 3).
    drop(inflate_charge);
    let receipt = match state
        .router
        .write_values(tenant, ingest_points, WriteMode::Strict, state.ack_deadline)
        .await
    {
        Ok(receipt) => receipt,
        Err(err) => {
            // A partial multi-shard commit: the durable siblings are real
            // data, and remote write has no channel to hand their tokens
            // back, so the count is logged the way the OTLP paths log it.
            let durable_shard_count = err.durable_tokens().len();
            if durable_shard_count > 0 {
                tracing::warn!(
                    tenant_hash = %tenant_hash.to_hex(),
                    durable_shard_count,
                    "metric write partially committed before a sibling shard \
                     failed"
                );
            }
            state.metrics.record_request_rejected();
            return write_error_response(err);
        }
    };

    state.metrics.record_request_accepted(
        points_accepted,
        points_dropped,
        metadata_dropped,
        created_timestamps_dropped,
    );

    let mut response = StatusCode::NO_CONTENT.into_response();
    if !receipt.tokens.is_empty() {
        let encoded = receipt
            .tokens
            .iter()
            .map(|token| token.encode())
            .collect::<Vec<_>>()
            .join(",");
        if let Ok(value) = HeaderValue::from_str(&encoded) {
            response
                .headers_mut()
                .insert(crate::otlp_http::COMMIT_TOKEN_HEADER, value);
        }
    }

    if version == RemoteWriteVersion::V2 {
        let headers_mut = response.headers_mut();
        if let Ok(value) = HeaderValue::from_str(&samples_written.to_string()) {
            headers_mut.insert(SAMPLES_WRITTEN_HEADER, value);
        }
        if let Ok(value) = HeaderValue::from_str(&histograms_written.to_string()) {
            headers_mut.insert(HISTOGRAMS_WRITTEN_HEADER, value);
        }
        // Still a constant zero: exemplars are accepted and dropped, with no
        // storage of their own (ADR-0017 defers it).
        headers_mut.insert(EXEMPLARS_WRITTEN_HEADER, HeaderValue::from_static("0"));
    }

    response
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use ravel_ingest::{
        AdmissionLimits, IngestByteBudgetLimit, IngestConfig, MIN_PLAUSIBLE_INGEST_CLOCK_NS,
        SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::Rejection;
    use ravel_query::http::AuthError;
    use ravel_remote_write::RwRejection;

    use crate::ingest_concurrency::IngestConcurrencyLimit;

    use super::*;

    #[test]
    fn points_dropped_does_not_double_count_native_histograms() {
        let normalized = RwNormalizeOutput {
            points: Vec::new(),
            histogram_points: Vec::new(),
            rejected: vec![
                RwRejection::NativeHistogramSpanLengthZero,
                RwRejection::NativeHistogramSpanLengthZero,
                RwRejection::NativeHistogramSpanLengthZero,
                RwRejection::Otlp {
                    reason: Rejection::EmptyMetricName { count: 2 },
                    count: 2,
                },
            ],
            histograms_written: 0,
            histograms_dropped: 3,
            exemplars_dropped: 1,
            metadata_dropped: 5,
            created_timestamps_dropped: 7,
        };

        // 3 (one point per histogram rejection) + 2 (otlp rejection) + 1
        // (exemplar), not +3 again for histograms_dropped and not +5/+7 for
        // metadata/created timestamps, which are not data points.
        assert_eq!(compute_points_dropped(&normalized), 6);
    }

    #[test]
    fn points_dropped_ignores_metadata_and_created_timestamps() {
        let normalized = RwNormalizeOutput {
            points: Vec::new(),
            histogram_points: Vec::new(),
            rejected: Vec::new(),
            histograms_written: 0,
            histograms_dropped: 0,
            exemplars_dropped: 0,
            metadata_dropped: 4,
            created_timestamps_dropped: 9,
        };

        assert_eq!(compute_points_dropped(&normalized), 0);
    }

    /// Resolves every request to the same fixed tenant, so the handler reaches
    /// its receiver-clock plausibility check with a known tenant to attribute
    /// the `reason="clock"` rejection to.
    struct FixedTenantResolver(TenantId);

    impl TenantResolver for FixedTenantResolver {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(self.0.clone())
        }
    }

    /// A deterministic receiver clock (CLAUDE.md time injection): returns a
    /// fixed timestamp so the plausibility floor is exercised without ever
    /// reading `SystemTime::now()`.
    struct FixedClock(i64);

    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    fn state_with_clock(tenant: &TenantId, clock: Arc<dyn Clock>) -> Arc<RemoteWriteState> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        Arc::new(RemoteWriteState {
            tenant_resolver: Arc::new(FixedTenantResolver(tenant.clone())),
            router,
            limits: IngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            metrics: RemoteWriteMetrics::default(),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            recovery: None,
            provisioning: None,
            ingest_concurrency: IngestConcurrencyController::shared(
                IngestConcurrencyLimit::Unlimited,
            ),
            clock,
            metadata_sink: None,
            budget: IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        })
    }

    /// A Remote Write request whose
    /// injected receiver clock is below the 2020 floor must be rejected as a
    /// whole-request 503 / UNAVAILABLE, and the `reason="clock"` admission
    /// counter for the tenant's Metrics signal must increment. The clock is a
    /// fixed sub-floor timestamp, never `SystemTime::now()`, so the test is
    /// deterministic.
    ///
    /// Non-vacuity: delete the `if let Err(msg) = plausible_ingest_clock(now_ns)`
    /// guard block near the top of [`remote_write`] and this test fails --- the
    /// sub-floor clock then flows into the rest of the handler, which returns a
    /// non-503 status (415, no version header) and never touches the
    /// `reason="clock"` counter.
    #[tokio::test]
    async fn receiver_clock_below_floor_rejects_unavailable_with_reason_clock() {
        let tenant = TenantId::new("acme");
        // One nanosecond below the 2020 floor: an implausible receiver clock.
        let sub_floor = MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1;
        let state = state_with_clock(&tenant, Arc::new(FixedClock(sub_floor)));

        // No content-type / version header and an empty body: the clock check
        // runs before version negotiation and decode, so those never matter.
        let response = remote_write(State(state.clone()), HeaderMap::new(), Bytes::new()).await;

        // (a) The transport response is 503 / UNAVAILABLE with a Retry-After,
        // as this surface returns for an implausible receiver clock.
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a sub-floor receiver clock must reject the whole request as 503"
        );
        assert!(
            response.headers().contains_key(RETRY_AFTER_HEADER),
            "the 503 carries a Retry-After header"
        );

        // (b) The admission rejected counter increments under reason="clock"
        // for this tenant's Metrics signal.
        let row = state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|r| r.tenant_hash == tenant.hash() && r.signal == Signal::Metrics)
            .expect("a metrics usage row exists after the clock rejection");
        assert_eq!(
            row.requests_rejected_clock_total, 1,
            "the reason=\"clock\" rejected counter incremented exactly once"
        );
    }

    /// A fixed in-window receiver instant, so normalization accepts the fixture
    /// samples without reading a wall clock.
    const FIXTURE_NOW_NS: i64 = 1_750_000_000_000_000_000;

    /// A snappy-compressed RW2 body of `points` samples on one series, and the
    /// exact length it inflates to. Repeated identical samples compress well, so
    /// the inflated length is many times the compressed length: that gap is what
    /// makes charging the wrong quantity visible.
    fn rw2_snappy_body(points: usize) -> (Bytes, u64) {
        use prost::Message as _;
        use ravel_remote_write::proto::write_v2::{
            Request as ProtoRequestV2, Sample as ProtoSampleV2, TimeSeries as ProtoTimeSeriesV2,
        };

        let ts_ms = FIXTURE_NOW_NS / 1_000_000;
        let request = ProtoRequestV2 {
            symbols: vec![
                String::new(),
                "__name__".to_string(),
                "requests_total".to_string(),
                "job".to_string(),
                "bench".to_string(),
            ],
            timeseries: (0..points)
                .map(|i| ProtoTimeSeriesV2 {
                    labels_refs: vec![1, 2, 3, 4],
                    samples: vec![ProtoSampleV2 {
                        value: 1.0,
                        timestamp: ts_ms - i as i64,
                        start_timestamp: 0,
                    }],
                    histograms: vec![],
                    exemplars: vec![],
                    metadata: None,
                })
                .collect(),
        };
        let plain = request.encode_to_vec();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&plain)
            .expect("fixture compresses");
        assert!(
            compressed.len() < plain.len(),
            "fixture must compress: compressed={} inflated={}",
            compressed.len(),
            plain.len()
        );
        (Bytes::from(compressed), plain.len() as u64)
    }

    /// Issue #1419 requirement 3: the settled charge is the ACTUAL inflated
    /// length, not the 64 MiB cap and not the compressed wire length. Snappy
    /// declares its decompressed size in a varint header, so the figure is
    /// exact and is taken before `ravel-remote-write` allocates the buffer it
    /// pays for.
    ///
    /// Non-vacuity: charge `body.len()` (the compressed length) or
    /// `MAX_DECOMPRESSED_PAYLOAD_BYTES` (the cap) in `charge_snappy_inflate`
    /// and the first assertion fails, because the fixture pins all three
    /// quantities as distinct.
    #[test]
    fn snappy_inflate_charge_equals_the_exact_inflated_length() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(64 * 1024 * 1024));

        let charge = charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("a body inside the budget is charged, not shed")
            .expect("a non-empty body takes a charge");

        assert_eq!(
            charge.bytes(),
            inflated_len,
            "the charge is the exact inflated length"
        );
        assert_ne!(
            inflated_len,
            body.len() as u64,
            "the fixture must distinguish the inflated length from the compressed length"
        );
        assert_ne!(
            inflated_len, MAX_DECOMPRESSED_PAYLOAD_BYTES as u64,
            "the fixture must distinguish the inflated length from the cap"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the budget holds exactly the inflated length while the charge lives"
        );

        drop(charge);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "dropping the guard refunds the budget exactly"
        );
    }

    /// Issue #1419 requirement 2: the charge is held through decode AND
    /// normalization, not released the moment the inflate finishes. Reading
    /// `in_flight_bytes` after `decode_body_charged` returns and again after
    /// `normalize_resolved` has run pins both instants.
    ///
    /// Non-vacuity: drop the charge inside `decode_body_charged` (return only
    /// the request) and the first two assertions read 0 instead of the inflated
    /// length.
    #[test]
    fn inflate_charge_is_held_through_decode_and_normalize() {
        let tenant = TenantId::new("acme");
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(64 * 1024 * 1024));

        let Ok((resolved, charge)) = decode_body_charged(&body, RemoteWriteVersion::V2, &budget)
        else {
            panic!("a well-formed body inside the budget decodes");
        };
        let charge = charge.expect("a non-empty body carries its inflate charge past decode");

        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the inflate charge is still held after decode"
        );

        let normalized =
            normalize_resolved(&tenant, resolved, &IngestLimits::default(), FIXTURE_NOW_NS);
        assert!(
            !normalized.points.is_empty(),
            "the fixture normalizes to real points, so normalization did run"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the inflate charge is still held after normalization, before the router charge"
        );

        drop(charge);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "the charge is released before the router takes its own"
        );
    }

    /// A body whose inflate would cross the ceiling is shed before the
    /// decompressed buffer is allocated: `charge_snappy_inflate` reports
    /// [`SnappyChargeError::Shed`], the budget is left untouched, and
    /// `decode_body_charged` turns that into 429 + `Retry-After` rather than
    /// decoding the body.
    ///
    /// Non-vacuity: remove the `budget.try_charge` call and the body inflates
    /// and decodes fine, so both the error and the 429 disappear.
    #[test]
    fn inflate_over_the_ceiling_is_shed_without_allocating() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(inflated_len - 1));

        assert!(
            matches!(
                charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget),
                Err(SnappyChargeError::Shed)
            ),
            "one byte short of the inflated length sheds"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "a shed request holds no bytes at all"
        );
        assert_eq!(budget.shed_total(), 1, "the shed is counted exactly once");

        let Err(error) = decode_body_charged(&body, RemoteWriteVersion::V2, &budget) else {
            panic!("a shed body produces a response, not a decoded request");
        };
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER_HEADER)
                .expect("the shed response carries Retry-After")
                .to_str()
                .expect("ascii header value"),
            INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string(),
        );

        // The exact same body fits once the ceiling covers the inflated length,
        // so the shed above was the budget and nothing else.
        let roomy = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(inflated_len));
        assert!(
            charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &roomy)
                .expect("a ceiling equal to the inflated length admits the body")
                .is_some()
        );
    }

    /// Cap before budget, the same order the gzip path uses: a body whose
    /// declared inflate exceeds the cap takes no charge and is left to the
    /// decoder's existing typed error (HTTP 400), so a tight budget cannot turn
    /// an over-cap body into a 429. An empty or malformed body likewise charges
    /// nothing.
    #[test]
    fn over_cap_empty_and_malformed_bodies_take_no_charge() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1));

        let over_cap = charge_snappy_inflate(&body, (inflated_len - 1) as usize, &budget)
            .expect("an over-cap body is not shed");
        assert!(
            over_cap.is_none(),
            "an over-cap body takes no charge; the decoder rejects it"
        );

        let empty = charge_snappy_inflate(&Bytes::new(), MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("an empty body is not shed");
        assert!(empty.is_none(), "an empty body inflates to nothing");

        // A truncated snappy header has no readable decompressed length.
        let malformed = Bytes::from_static(&[0xff, 0xff, 0xff]);
        let malformed = charge_snappy_inflate(&malformed, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("a malformed body is not shed");
        assert!(
            malformed.is_none(),
            "a malformed header charges nothing and is left to the decoder"
        );

        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "none of these paths charged the budget"
        );
        assert_eq!(
            budget.shed_total(),
            0,
            "none of these paths reached the budget at all"
        );
    }
}
