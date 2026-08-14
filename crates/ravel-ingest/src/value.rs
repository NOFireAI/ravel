//! Generalized ingest point/value shapes: a
//! point carries either a scalar sample or a native-histogram sample. Wire
//! admission produces both: `ravel_otlp::NormalizedPoint` (scalar) and
//! `ravel_otlp::NormalizedHistogramPoint` (native histogram) each convert
//! into an [`IngestPoint`], so the shard buffer and segment-write plumbing
//! reach the same RSEG v5 writer regardless of which wire path decoded the
//! point.

use ravel_segment::{ExemplarInput, HistogramSample};
use ravel_types::{Exemplar, Label, LabelSet, Sample, SeriesId};

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

/// One exemplar and the series whose sample it illustrates (ADR-0047
/// decision 1), as a wire surface hands it to ingest. Already through the
/// caller's [`ravel_types::ExemplarCap`] on the normalize path
/// (`ravel_otlp::normalize::NormalizedExemplar`); the shard applies its own
/// flush-scoped cap again on top, since a flush is the unit that has to fit
/// in one object.
#[derive(Debug, Clone)]
pub struct IngestExemplar {
    pub series_id: SeriesId,
    pub exemplar: Exemplar,
}

impl From<ravel_otlp::normalize::NormalizedExemplar> for IngestExemplar {
    fn from(e: ravel_otlp::normalize::NormalizedExemplar) -> Self {
        IngestExemplar {
            series_id: e.series_id,
            exemplar: e.exemplar,
        }
    }
}

impl IngestExemplar {
    /// Estimated buffered byte cost, for the shard's `est_bytes` flush
    /// trigger.
    ///
    /// This measures what the exemplar occupies in memory while it waits, not
    /// what it will occupy in the object. Those differ by more than an order
    /// of magnitude and the trigger bounds the former: a `Label` is two
    /// `String` headers (24 bytes each) whatever the strings hold, and
    /// `ravel-otlp` admits up to 64 filtered attributes per exemplar. Counting
    /// only the stored form (ADR-0047's "roughly 40 bytes plus attributes")
    /// undercounts an exemplar with 64 one-character attributes by more than
    /// 10x, and exemplars are buffered before the cap runs, so the excess is
    /// client-controlled.
    pub(crate) fn est_bytes(&self) -> usize {
        let attrs: usize = self
            .exemplar
            .filtered_attributes
            .iter()
            .map(|l| size_of::<Label>() + l.name.len() + l.value.len())
            .sum();
        size_of::<Self>() + attrs
    }

    /// The writer-facing shape: the sample value comes back from its stored
    /// bit pattern (never a decimal round trip, so a NaN payload and -0.0
    /// survive), and attributes flatten to the `(name, value)` pairs the
    /// writer interns into LABEL_DICT.
    pub(crate) fn into_exemplar_input(self) -> ExemplarInput {
        ExemplarInput {
            series_id: self.series_id,
            ts_ns: self.exemplar.ts_ns,
            value: f64::from_bits(self.exemplar.value_bits),
            trace_id: self.exemplar.trace_id,
            span_id: self.exemplar.span_id,
            attrs: self
                .exemplar
                .filtered_attributes
                .into_iter()
                .map(|l| (l.name, l.value))
                .collect(),
        }
    }
}

/// Which shape a series' points carry (`value_kind`): homogeneous per series for its
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

impl IngestPoint {
    /// Estimated buffered byte cost of this one point, for the process-wide
    /// ingest byte budget's admission charge (ADR-0069, [`crate::IngestByteBudget`]).
    ///
    /// Mirrors `TenantBuf::merge`'s `est_bytes` rule -- 16 bytes per sample plus
    /// the label name/value bytes -- but counts label bytes for *every* point,
    /// not only the first sighting of a series in a buffer: the charge happens
    /// before routing, without the shard's buffer state, so it cannot know
    /// which series are already present. That makes the charge a deliberate
    /// slight over-estimate of what finally lands in the buffer, which is the
    /// safe direction for a memory ceiling (it sheds a touch early rather than
    /// a touch late).
    pub(crate) fn est_charge_bytes(&self) -> u64 {
        let label_bytes: u64 = self
            .labels
            .iter()
            .map(|l| (l.name.len() + l.value.len()) as u64)
            .sum();
        16 + label_bytes
    }
}
