//! `POST /api/v1/write`: Prometheus Remote Write 1.0 and 2.0 ingest
//! (ADR-0015, docs/ingest-breadth-plan.md Track A).
//!
//! This surface is strict-mode only regardless of the OTLP ingest mode
//! header: a Remote Write sender expects a 2xx to mean the samples are
//! durable, so the buffered-mode override honored by `otlp_http`/`otlp_grpc`
//! is never read here.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use ravel_ingest::{IngestRouter, WriteError, WriteMode};
use ravel_otlp::IngestLimits;
use ravel_query::http::TenantResolver;
use ravel_remote_write::{Rw1DecodeError, Rw2DecodeError, normalize_resolved};
use ravel_types::TenantId;

const SAMPLES_WRITTEN_HEADER: &str = "x-prometheus-remote-write-samples-written";
const HISTOGRAMS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-histograms-written";
const EXEMPLARS_WRITTEN_HEADER: &str = "x-prometheus-remote-write-exemplars-written";
const RETRY_AFTER_HEADER: &str = "retry-after";

/// Snappy decompression cap for a single Remote Write request body, applied
/// before allocation (same discipline as `ravel-otap`'s
/// `max_decompressed_payload_bytes`).
const MAX_DECOMPRESSED_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteWriteMetricsSnapshot {
    pub requests_accepted: u64,
    pub requests_rejected: u64,
    pub points_accepted: u64,
    pub points_dropped: u64,
}

impl RemoteWriteMetrics {
    fn record_request_rejected(&self) {
        self.requests_rejected.fetch_add(1, Ordering::Relaxed);
    }

    fn record_request_accepted(&self, points_accepted: u64, points_dropped: u64) {
        self.requests_accepted.fetch_add(1, Ordering::Relaxed);
        self.points_accepted
            .fetch_add(points_accepted, Ordering::Relaxed);
        self.points_dropped
            .fetch_add(points_dropped, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RemoteWriteMetricsSnapshot {
        RemoteWriteMetricsSnapshot {
            requests_accepted: self.requests_accepted.load(Ordering::Relaxed),
            requests_rejected: self.requests_rejected.load(Ordering::Relaxed),
            points_accepted: self.points_accepted.load(Ordering::Relaxed),
            points_dropped: self.points_dropped.load(Ordering::Relaxed),
        }
    }
}

pub struct RemoteWriteState {
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub router: Arc<IngestRouter>,
    pub limits: IngestLimits,
    pub ack_deadline: std::time::Duration,
    pub metrics: RemoteWriteMetrics,
}

pub fn router(state: Arc<RemoteWriteState>) -> Router {
    Router::new()
        .route("/api/v1/write", post(remote_write))
        .with_state(state)
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
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

async fn remote_write(
    State(state): State<Arc<RemoteWriteState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant: TenantId = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => {
            state.metrics.record_request_rejected();
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let Some(version) = negotiate_version(&headers) else {
        state.metrics.record_request_rejected();
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unknown or missing Remote Write content type / version header",
        )
            .into_response();
    };

    let resolved = match version {
        RemoteWriteVersion::V1 => {
            ravel_remote_write::decode_write_request(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                .map_err(|err: Rw1DecodeError| err.to_string())
        }
        RemoteWriteVersion::V2 => {
            ravel_remote_write::decode_request(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                .map_err(|err: Rw2DecodeError| err.to_string())
        }
    };
    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(message) => {
            state.metrics.record_request_rejected();
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
    };

    // Strict mode only: a Remote Write 2xx must mean durable, so the
    // buffered-mode header override is never consulted on this surface.
    let normalized = normalize_resolved(&tenant, resolved, &state.limits, now_ns());
    let points_dropped: u64 = normalized
        .rejected
        .iter()
        .map(|r| r.rejected_count() as u64)
        .sum::<u64>()
        + normalized.histograms_dropped as u64
        + normalized.exemplars_dropped as u64
        + normalized.metadata_dropped as u64
        + normalized.created_timestamps_dropped as u64;
    let points_accepted = normalized.points.len() as u64;

    let receipt = match state
        .router
        .write(
            tenant,
            normalized.points,
            WriteMode::Strict,
            state.ack_deadline,
        )
        .await
    {
        Ok(receipt) => receipt,
        Err(err) => {
            state.metrics.record_request_rejected();
            return write_error_response(err);
        }
    };

    state
        .metrics
        .record_request_accepted(points_accepted, points_dropped);

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
        if let Ok(value) = HeaderValue::from_str(&points_accepted.to_string()) {
            headers_mut.insert(SAMPLES_WRITTEN_HEADER, value);
        }
        headers_mut.insert(HISTOGRAMS_WRITTEN_HEADER, HeaderValue::from_static("0"));
        headers_mut.insert(EXEMPLARS_WRITTEN_HEADER, HeaderValue::from_static("0"));
    }

    response
}
