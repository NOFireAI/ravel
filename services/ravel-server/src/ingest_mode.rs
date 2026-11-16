//! Per-request write-mode resolution, shared by every OTLP and OTAP ingest
//! handler.

use axum::http::HeaderMap;
use ravel_ingest::WriteMode;

use crate::otlp_http::INGEST_MODE_HEADER;

/// Resolves `headers`'s `x-ravel-ingest-mode` value and the process
/// `--strict-flush-policy` default into a [`WriteMode`] (ADR-2793
/// decision 1). `buffered` is unconditional; `strict-arrival` and
/// `strict-aged` override `policy` explicitly for this request; `strict`, an
/// absent header, and any other value all mean "strict under `policy`".
pub(crate) fn resolve_write_mode(
    headers: &HeaderMap,
    policy: crate::config::StrictFlushPolicy,
) -> WriteMode {
    match headers
        .get(INGEST_MODE_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some("buffered") => WriteMode::Buffered,
        Some("strict-arrival") => WriteMode::StrictArrival,
        Some("strict-aged") => WriteMode::Strict,
        _ => policy.write_mode(),
    }
}
