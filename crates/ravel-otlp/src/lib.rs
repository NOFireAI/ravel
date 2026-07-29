//! OTLP decode and normalization into Ravel canonical metric and log
//! batches.
//!
//! Gauge and Sum `NumberDataPoint`s, plus cumulative Histogram and Summary
//! data points exploded into Prometheus-convention scalar series (ADR-0016),
//! and cumulative `ExponentialHistogram` data points admitted as native
//! histogram samples (ADR-0017). Resource attributes flatten into labels per
//! the standard Prometheus mapping; see ADR-0005 note.
//!
//! Logs take a parallel path: `logs_normalize` maps an
//! `ExportLogsServiceRequest` to canonical log records carrying a log stream
//! identity (ADR-0029), computed once per `ScopeLogs` from the resource and
//! scope attributes.

pub mod limits;
pub mod logs_limits;
pub mod logs_normalize;
pub mod normalize;
pub mod promcompat;

pub use limits::{IngestLimits, Rejection};
pub use logs_limits::{LogIngestLimits, LogRejection};
pub use logs_normalize::{LogNormalizeOutput, NormalizedLogRecord, normalize_logs};
pub use normalize::{
    NormalizeOutput, NormalizedHistogramPoint, NormalizedPoint, normalize_metrics,
};
