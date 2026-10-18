//! `POST /api/v1/write`: Prometheus Remote Write 1.0 and 2.0 ingest
//! (ADR-0015).
//!
//! This surface is strict-mode only regardless of the OTLP ingest mode
//! header: a Remote Write sender expects a 2xx to mean the samples are
//! durable, so the buffered-mode override honored by `otlp_http`/`otlp_grpc`
//! is never read here. Which strict write mode that is comes from the
//! process `--strict-flush-policy` default alone (ADR-2793 decision 1):
//! this surface has no per-request override, since there is no header read
//! here to carry one.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ingest_admission::{IngestPermitHeld, PendingAckHeld, retry_after_seconds};
use crate::ingest_concurrency::IngestConcurrencyController;
use crate::ingest_decode::decode_body_charged;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use ravel_cpu_gate::{CpuGateError, WriteGate};
use ravel_ingest::{
    AdmissionController, Clock, IngestByteBudget, IngestPoint, IngestRouter, IngestValue,
    RequestRejection, WriteError, plausible_ingest_clock,
};
use ravel_otlp::IngestLimits;
use ravel_query::http::TenantResolver;
use ravel_remote_write::{RwNormalizeOutput, normalize_resolved};
use ravel_types::{SeriesId, Signal, TenantId};

/// Layer 1 (ADR-0051 section 2): the compressed-wire-body cap, ahead of
/// [`crate::ingest_decode::MAX_DECOMPRESSED_PAYLOAD_BYTES`]'s existing post-Snappy cap.
const MAX_COMPRESSED_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

const SAMPLES_WRITTEN_HEADER: &str = "x-prometheus-remote-write-samples-written";
const HISTOGRAMS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-histograms-written";
const EXEMPLARS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-exemplars-written";
pub(crate) const RETRY_AFTER_HEADER: &str = "retry-after";

/// Why a Remote Write body did not become a decoded request. Carried as a small
/// value rather than a built [`Response`], which the `result_large_err` lint
/// rejects in an `Err` variant.
#[derive(Debug)]
pub(crate) enum DecodeChargeError {
    /// The declared inflate would push the ingest byte budget past its ceiling.
    /// Taken before the expansion is allocated.
    Shed,
    /// The decoder rejected the body, with its own message.
    Decode(String),
    /// The write gate returned no result: the decode job panicked, or the
    /// runtime dropped it while shutting down. Nothing was written.
    Gate(CpuGateError),
}

impl IntoResponse for DecodeChargeError {
    fn into_response(self) -> Response {
        match self {
            DecodeChargeError::Shed => ingest_buffer_budget_shed_response(),
            DecodeChargeError::Decode(message) => {
                (StatusCode::BAD_REQUEST, message).into_response()
            }
            DecodeChargeError::Gate(err) => {
                let status = match err {
                    CpuGateError::Panicked => StatusCode::INTERNAL_SERVER_ERROR,
                    CpuGateError::Cancelled | CpuGateError::Closed => {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                };
                (status, format!("remote write decode failed: {err}")).into_response()
            }
        }
    }
}

/// A fixed `Retry-After` for the admission sheds that carry no per-caller
/// refill estimate: no refill time is tracked, and a slot or budget can free
/// up as soon as any in-flight request completes, so a short fixed wait is
/// the right shape.
pub(crate) const INGEST_CONCURRENCY_RETRY_AFTER_SECONDS: u64 = 1;

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
pub(crate) enum RemoteWriteVersion {
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
    /// The ADR-1702 write gate the body's snappy inflate and decode run on,
    /// sized by the declared inflated length capped at the decompression cap.
    /// `None` runs them inline on the request task.
    pub write_gate: Option<Arc<WriteGate>>,
    /// The process default strict flush policy (ADR-2793 decision 1),
    /// resolved from `--strict-flush-policy`. This surface reads no header
    /// (see the module doc comment above), so every write resolves through
    /// this value alone.
    pub strict_flush_policy: crate::config::StrictFlushPolicy,
    /// The process's IPCrypt policy, as [`crate::ingest::IngestState::ipcrypt`].
    pub ipcrypt: Arc<crate::ipcrypt::IpcryptPolicy>,
    /// The normalize-layer counters the OTLP and OTAP surfaces share; this
    /// surface feeds only `ravel_ingest_ipcrypt_values_total` into it.
    pub normalize_metrics: Arc<crate::normalize_reject_metrics::NormalizeRejectMetrics>,
}

impl crate::ingest_admission::IngestAdmissionState for RemoteWriteState {
    fn ingest_concurrency(&self) -> &Arc<IngestConcurrencyController> {
        &self.ingest_concurrency
    }

    fn tenant_resolver(&self) -> &Arc<dyn TenantResolver> {
        &self.tenant_resolver
    }

    /// This surface counts every whole-request rejection, the 401 included,
    /// so the middleware records it where the handler used to.
    fn on_unauthorized(&self) {
        self.metrics.record_request_rejected();
    }

    fn pending_ack_budget(&self) -> &Arc<ravel_ingest::PendingAckBudget> {
        self.router.pending_ack_budget()
    }

    /// Every Remote Write is strict: this surface reads no mode header, so a
    /// client cannot opt out of the pending-ack bound.
    fn strict_write(&self, _headers: &HeaderMap) -> bool {
        true
    }
}

pub fn router(state: Arc<RemoteWriteState>) -> Router {
    Router::new()
        .route("/api/v1/write", post(remote_write))
        // Issue #1705: admission and authentication decide on the request
        // head, ahead of the `body: Bytes` extractor. See `otlp_http::router`.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::ingest_admission::admit_ingest_request::<RemoteWriteState>,
        ))
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
    // The pending strict-ack bound (ADR-2793 decision 4): the in-flight
    // shed's 429 with its own body text, shared with the OTLP surfaces.
    if matches!(err, WriteError::PendingAckLimitReached) {
        return crate::ingest_admission::pending_ack_shed_response();
    }
    // The buffer-budget shed (ADR-0069) is admission backpressure, not a
    // durability failure: 429 + `Retry-After`, matching the byte-rate
    // rejection and the in-flight shed, rather than the 503 the other
    // retryable write failures take. A shard at the flush deferral cap refuses
    // before buffering too, so it takes the same answer.
    if matches!(
        err,
        WriteError::BufferBudgetExceeded | WriteError::DeferralCapReached
    ) {
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

/// The in-flight permit and the tenant both come from
/// [`crate::ingest_admission::admit_ingest_request`], which ran on this
/// request's head before `body` was read (issue #1705).
async fn remote_write(
    State(state): State<Arc<RemoteWriteState>>,
    Extension(tenant): Extension<TenantId>,
    Extension(IngestPermitHeld(permit)): Extension<IngestPermitHeld>,
    Extension(PendingAckHeld(pending_ack)): Extension<PendingAckHeld>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
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
    let (resolved, inflate_charge) =
        match decode_body_charged(&body, version, &state.budget, state.write_gate.as_deref()).await
        {
            Ok(decoded) => decoded,
            Err(error) => {
                state.metrics.record_request_rejected();
                return error.into_response();
            }
        };
    // The compressed body is counted under the in-flight permit, which the
    // handler releases at enqueue; it must not outlive that release.
    drop(body);

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
    // The tenant's IPCrypt rules (ADR-2993). A resolve failure stores
    // nothing: writing the ruled labels in the clear is what the policy
    // exists to prevent, and the cause (logged with the tenant hash and rule
    // key) is the operator's to fix, so a 500 rather than a retryable 5xx.
    let rules = match state.ipcrypt.rules_for(&tenant) {
        Ok(rules) => rules,
        Err(_) => {
            state.metrics.record_request_rejected();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                crate::ipcrypt::TENANT_RULES_UNAVAILABLE,
            )
                .into_response();
        }
    };
    let normalized = normalize_resolved(&tenant, resolved, &state.limits, &rules, now_ns);
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
    let fully_admitted = admission.rejected.is_empty();
    if !fully_admitted {
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
    // The in-flight permit goes back once the write is enqueued, before the
    // strict ack is awaited (ADR-2793 decision 4): every Remote Write is
    // strict, so this is the surface whose ack wait the permit covered most.
    let written = match state
        .router
        .submit_admitted(
            tenant,
            ingest_points,
            Vec::new(),
            state.strict_flush_policy.write_mode(),
            state.ack_deadline,
            pending_ack.take(),
        )
        .await
    {
        Ok(submitted) => {
            permit.release();
            submitted.acks().await
        }
        Err(err) => Err(err),
    };
    let receipt = match written {
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
    // Counted only once the write is acknowledged, and only when the
    // active-series cap dropped nothing: the tally is per request, not per
    // series, so a partly admitted request counts nothing.
    if fully_admitted {
        state
            .normalize_metrics
            .record_ipcrypt(Signal::Metrics, &normalized.ipcrypt_tally);
    }

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
pub(crate) mod tests {
    use std::time::Duration;

    use ravel_ingest::{
        AdmissionLimits, IngestByteBudgetLimit, IngestConfig, MIN_PLAUSIBLE_INGEST_CLOCK_NS,
        PendingAckBudget, SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::Rejection;
    use ravel_query::http::AuthError;
    use ravel_remote_write::RwRejection;

    use crate::ingest_concurrency::IngestConcurrencyLimit;

    use super::*;

    /// A write refused at the flush deferral cap takes the budget shed's 429
    /// with `Retry-After`, not the 503 the other retryable failures take,
    /// while an `Abandoned` (the outcome-unknown answer a stripped strict
    /// waiter gets) stays 503.
    #[test]
    fn a_deferral_cap_refusal_is_429_and_an_abandoned_flush_503() {
        let refused = write_error_response(WriteError::DeferralCapReached);
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(refused.headers().contains_key(RETRY_AFTER_HEADER));
        let abandoned = write_error_response(WriteError::Abandoned("x".into()));
        assert_eq!(abandoned.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A write refused at the pending strict-ack bound (ADR-2793 decision 4)
    /// takes the in-flight shed's answer on this surface too: 429, a one
    /// second `Retry-After`, and the body text the OTLP surfaces share.
    #[tokio::test]
    async fn a_pending_ack_refusal_is_429_with_the_shared_body() {
        let refused = write_error_response(WriteError::PendingAckLimitReached);
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = refused
            .headers()
            .get(RETRY_AFTER_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        assert_eq!(retry_after.as_deref(), Some("1"));
        let body = axum::body::to_bytes(refused.into_body(), 1 << 20)
            .await
            .expect("read body");
        assert_eq!(
            std::str::from_utf8(&body).expect("utf-8 body"),
            crate::ingest_admission::PENDING_ACK_SHED_MESSAGE
        );
    }

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
            ipcrypt_tally: ravel_ipcrypt::tally::IpcryptTally::new(),
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
            ipcrypt_tally: ravel_ipcrypt::tally::IpcryptTally::new(),
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
        state_with_resolver(Arc::new(FixedTenantResolver(tenant.clone())), clock)
    }

    fn state_with_resolver(
        tenant_resolver: Arc<dyn TenantResolver>,
        clock: Arc<dyn Clock>,
    ) -> Arc<RemoteWriteState> {
        state_with_ceiling(tenant_resolver, clock, IngestConcurrencyLimit::Unlimited)
    }

    fn state_with_ceiling(
        tenant_resolver: Arc<dyn TenantResolver>,
        clock: Arc<dyn Clock>,
        ceiling: IngestConcurrencyLimit,
    ) -> Arc<RemoteWriteState> {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
            Arc::new(PendingAckBudget::new(0)),
        ));
        Arc::new(RemoteWriteState {
            tenant_resolver,
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
            ingest_concurrency: IngestConcurrencyController::shared(ceiling),
            clock,
            metadata_sink: None,
            ipcrypt: crate::ipcrypt::IpcryptPolicy::disabled(),
            normalize_metrics: Arc::new(
                crate::normalize_reject_metrics::NormalizeRejectMetrics::new(),
            ),
            budget: IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
            write_gate: None,
            strict_flush_policy: crate::config::StrictFlushPolicy::Aged,
        })
    }

    /// An unauthorized Remote Write is refused 401 by the admission middleware
    /// on the request head, and still counts in `requests_rejected` through
    /// `on_unauthorized`, as it did when the handler refused it.
    ///
    /// Non-vacuity: empty `on_unauthorized` and the counter stays at 0.
    #[tokio::test]
    async fn unauthorized_write_is_counted_as_rejected() {
        use tower::ServiceExt;

        let state = state_with_resolver(
            Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
                std::collections::HashMap::new(),
            )),
            Arc::new(FixedClock(FIXTURE_NOW_NS)),
        );
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/api/v1/write")
            .header("authorization", "Bearer not-a-token")
            .body(axum::body::Body::from("unread"))
            .expect("request builds");

        let response = router(state.clone())
            .oneshot(request)
            .await
            .expect("infallible router");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(state.metrics.snapshot().requests_rejected, 1);
    }

    /// A Remote Write shed by the in-flight ceiling whose credential does not
    /// resolve is answered 429 with its body unread and counted once under
    /// `unauthenticated`, never under `over_cap` although its body is above
    /// the drain cap. The credential check on the shed path has no side
    /// effect: `requests_rejected`, which counts the 401, stays at 0.
    #[tokio::test]
    async fn unauthenticated_shed_is_not_counted_as_rejected() {
        use crate::ingest_admission::{HEAD_REFUSAL_DRAIN_MAX_BYTES, UndrainedReason};
        use tower::ServiceExt;

        let state = state_with_ceiling(
            Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
                std::collections::HashMap::new(),
            )),
            Arc::new(FixedClock(FIXTURE_NOW_NS)),
            IngestConcurrencyLimit::Bounded(0),
        );
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/api/v1/write")
            .header("authorization", "Bearer not-a-token")
            .body(axum::body::Body::from(vec![
                0u8;
                2 * HEAD_REFUSAL_DRAIN_MAX_BYTES
                    as usize
            ]))
            .expect("request builds");

        let response = router(state.clone())
            .oneshot(request)
            .await
            .expect("infallible router");

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(axum::http::header::CONNECTION),
            Some(&axum::http::HeaderValue::from_static("close"))
        );
        assert_eq!(state.metrics.snapshot().requests_rejected, 0);
        let drain = state.ingest_concurrency.head_refusal_drain();
        assert_eq!(drain.undrained_total(UndrainedReason::Unauthenticated), 1);
        assert_eq!(drain.undrained_total(UndrainedReason::OverCap), 0);
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
        // The tenant stands in for `admit_ingest_request`, which resolves it
        // from the request head before the body extractor runs.
        let response = remote_write(
            State(state.clone()),
            Extension(tenant.clone()),
            Extension(IngestPermitHeld(
                crate::ingest_concurrency::IngestPermitHandle::default(),
            )),
            Extension(PendingAckHeld(
                crate::ingest_admission::PendingAckHandle::default(),
            )),
            HeaderMap::new(),
            Bytes::new(),
        )
        .await;

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
    pub(crate) const FIXTURE_NOW_NS: i64 = 1_750_000_000_000_000_000;

    /// The gate's failures answer 500 for a panicked decode job and 503 for a
    /// cancelled one, and a flush whose encode the gate failed answers the
    /// retryable 503 its `ShardUnavailable` carries.
    #[test]
    fn gate_failures_answer_500_for_a_panic_and_503_for_a_cancel() {
        let status = |err| DecodeChargeError::Gate(err).into_response().status();
        assert_eq!(
            status(CpuGateError::Panicked),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(CpuGateError::Cancelled),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(CpuGateError::Closed),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let flush = write_error_response(WriteError::ShardUnavailable);
        assert_eq!(flush.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(flush.headers().contains_key(RETRY_AFTER_HEADER));
    }
}
