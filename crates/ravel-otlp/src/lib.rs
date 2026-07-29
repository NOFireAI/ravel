//! OTLP decode and normalization into Ravel canonical metric batches.
//!
//! Gauge and Sum `NumberDataPoint`s, plus cumulative Histogram and Summary
//! data points exploded into Prometheus-convention scalar series (ADR-0016),
//! and cumulative `ExponentialHistogram` data points admitted as native
//! histogram samples (ADR-0017). Resource attributes flatten into labels per
//! the standard Prometheus mapping; see ADR-0005 note.

pub mod limits;
pub mod normalize;
pub mod promcompat;

pub use limits::{IngestLimits, Rejection};
pub use normalize::{
    NormalizeOutput, NormalizedHistogramPoint, NormalizedPoint, normalize_metrics,
};
