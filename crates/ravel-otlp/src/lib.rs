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
//!
//! Traces take a third path: `traces_normalize` maps an
//! `ExportTraceServiceRequest` to canonical spans (ADR-0041). Spans have no
//! stream identity at all, so resource and scope attributes are merged into
//! each span's single `attrs` map instead of feeding an identity hash.

pub mod label_projection;
pub mod limits;
pub mod logs_limits;
pub mod logs_normalize;
pub mod metadata;
pub mod normalize;
pub mod promcompat;
pub mod traces_limits;
pub mod traces_normalize;

pub use label_projection::{
    RESOLVED_LABEL_OVERHEAD_BYTES, project_log_resolved_label_bytes, project_resolved_label_bytes,
    project_span_resolved_label_bytes,
};
pub use limits::{
    AdmissionClass, DEFAULT_MAX_RESOLVED_LABEL_BYTES_PER_REQUEST, IngestLimits,
    NormalizeRejectCounts, Rejection, resource_attrs_dropped_from_rejections,
};
pub use logs_limits::{LogIngestLimits, LogRejection};
pub use logs_normalize::{LogNormalizeOutput, NormalizedLogRecord, normalize_logs};
pub use metadata::{MetricKind, MetricMetadata};
pub use normalize::{
    MetricsNormalizeResult, NormalizeOutput, NormalizedHistogramPoint, NormalizedPoint,
    normalize_metrics, normalize_metrics_with_exemplars, normalize_metrics_with_metadata,
};
pub use traces_limits::{SpanIngestLimits, SpanRejection};
pub use traces_normalize::{NormalizedSpan, SpanNormalizeOutput, normalize_traces};
