//! OTLP decode and normalization into Ravel canonical metric batches.
//!
//! Gauge and Sum `NumberDataPoint`s, plus cumulative Histogram and Summary
//! data points exploded into Prometheus-convention scalar series (ADR-0016).
//! `ExponentialHistogram` is the only metric type still unsupported, pending
//! ADR-0017. Resource attributes flatten into labels per the standard
//! Prometheus mapping; see ADR-0005 note.

pub mod limits;
pub mod normalize;
pub mod promcompat;

pub use limits::{IngestLimits, Rejection};
pub use normalize::{NormalizeOutput, NormalizedPoint, normalize_metrics};
