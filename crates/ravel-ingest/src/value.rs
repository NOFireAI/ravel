//! Generalized ingest point/value shapes (docs/rseg-v3-plan.md section 7): a
//! point carries either a scalar sample or a native-histogram sample. Wire
//! admission produces both: `ravel_otlp::NormalizedPoint` (scalar) and
//! `ravel_otlp::NormalizedHistogramPoint` (native histogram) each convert
//! into an [`IngestPoint`], so the shard buffer and segment-write plumbing
//! reach the same RSEG v5 writer regardless of which wire path decoded the
//! point.

use ravel_segment::HistogramSample;
use ravel_types::{LabelSet, Sample, SeriesId};

/// One point's value: scalar or native histogram.
#[derive(Debug, Clone)]
pub enum IngestValue {
    Scalar(Sample),
    Histogram(HistogramSample),
}

/// One series' identity, labels, and value for one point, independent of
/// which wire protocol (or, for histograms today, direct construction)
/// produced it.
#[derive(Debug, Clone)]
pub struct IngestPoint {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub value: IngestValue,
}

impl From<ravel_otlp::NormalizedPoint> for IngestPoint {
    fn from(p: ravel_otlp::NormalizedPoint) -> Self {
        IngestPoint {
            series_id: p.series_id,
            labels: p.labels,
            value: IngestValue::Scalar(p.sample),
        }
    }
}

impl From<ravel_otlp::NormalizedHistogramPoint> for IngestPoint {
    fn from(p: ravel_otlp::NormalizedHistogramPoint) -> Self {
        IngestPoint {
            series_id: p.series_id,
            labels: p.labels,
            value: IngestValue::Histogram(p.sample),
        }
    }
}

/// Which shape a series' points carry (`value_kind`,
/// docs/rseg-v3-plan.md section 3.4): homogeneous per series for its
/// whole life in a segment, so a shard buffer rejects a series that
/// receives both kinds within one flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueKind {
    Scalar,
    Histogram,
}

impl IngestValue {
    pub(crate) fn kind(&self) -> ValueKind {
        match self {
            IngestValue::Scalar(_) => ValueKind::Scalar,
            IngestValue::Histogram(_) => ValueKind::Histogram,
        }
    }
}
