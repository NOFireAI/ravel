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
    /// each label's `Label` struct header and its name/value bytes, the same
    /// per-label rule [`IngestExemplar::est_bytes`] applies -- but counts label
    /// bytes for *every* point, not only the first sighting of a series in a
    /// buffer: the charge happens before routing, without the shard's buffer
    /// state, so it cannot know which series are already present. That makes
    /// the charge a deliberate slight over-estimate of what finally lands in
    /// the buffer, which is the safe direction for a memory ceiling (it sheds a
    /// touch early rather than a touch late).
    ///
    /// The header term is what the buffer actually holds: a `Label` is two
    /// `String` headers (24 bytes each) whatever the strings contain, so
    /// counting only `name.len() + value.len()` undercharges a ten-label series
    /// with short values by about 480 bytes against the roughly 200 it counts,
    /// and the error grows with label count. Undercharging the ceiling is the
    /// unsafe direction: the process passes a limit it believes it is under.
    pub(crate) fn est_charge_bytes(&self) -> u64 {
        let label_bytes: u64 = self
            .labels
            .iter()
            .map(|l| (size_of::<Label>() + l.name.len() + l.value.len()) as u64)
            .sum();
        16 + label_bytes
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::Exemplar;

    use crate::log_shard::est_record_bytes;
    use crate::span_shard::est_span_bytes;
    use ravel_otlp::logs_normalize::NormalizedLogRecord;
    use ravel_otlp::traces_normalize::NormalizedSpan;
    use ravel_rspan::StatusCode;
    use ravel_types::logstream::{AttrValue, LogStreamId};

    fn labels_of(count: usize) -> LabelSet {
        let labels = (0..count)
            .map(|i| Label {
                name: format!("k{i}"),
                value: "v".to_string(),
            })
            .collect();
        LabelSet::new(labels).expect("distinct label names")
    }

    fn point_with(labels: LabelSet) -> IngestPoint {
        IngestPoint {
            series_id: SeriesId([0u8; 16]),
            labels,
            value: IngestValue::Scalar(Sample {
                ts_ns: 1_000,
                value: 1.0,
            }),
        }
    }

    fn exemplar_with(labels: LabelSet) -> IngestExemplar {
        IngestExemplar {
            series_id: SeriesId([0u8; 16]),
            exemplar: Exemplar {
                ts_ns: 1_000,
                value_bits: 1.0f64.to_bits(),
                trace_id: [0u8; 16],
                span_id: [0u8; 8],
                filtered_attributes: labels.iter().cloned().collect(),
            },
        }
    }

    /// A log record carrying `attr_count` attributes, each the identical
    /// `("attr", "v")` pair, and every other field empty/zero so that
    /// differencing two attribute widths cancels every fixed term and isolates
    /// the per-attribute charge.
    fn log_record_with(attr_count: usize) -> NormalizedLogRecord {
        NormalizedLogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: Vec::new(),
            ts_ns: 1_000,
            observed_ts_ns: 1_000,
            severity_num: 9,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: (0..attr_count)
                .map(|_| ("attr".to_string(), AttrValue::Str("v".to_string())))
                .collect(),
        }
    }

    /// A span carrying `attr_count` attributes, each the identical
    /// `("attr", "v")` pair, everything else empty/zero. Same differencing
    /// trick as [`log_record_with`].
    fn span_with(attr_count: usize) -> NormalizedSpan {
        NormalizedSpan {
            trace_id: [0u8; 16],
            span_id: [0u8; 8],
            parent_span_id: None,
            name: String::new(),
            start_ts_ns: 1_000,
            end_ts_ns: 1_100,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: (0..attr_count)
                .map(|_| ("attr".to_string(), "v".to_string()))
                .collect(),
        }
    }

    /// Every buffered-byte estimator feeding the one process-wide ingest byte
    /// budget (ADR-0069) must charge a per-attribute struct header, not only the
    /// attribute's string bytes. Metrics points and exemplars, log records, and
    /// spans all charge that single shared ceiling by `Arc` (docs/ingest.md), so
    /// if any one estimator drops the header term it silently undercharges the
    /// ceiling on its own signal while the others charge honestly -- exactly the
    /// skew this pin exists to catch.
    ///
    /// Each estimator's header term is isolated by differencing two attribute
    /// widths with identical per-attribute content, which cancels every fixed
    /// term (sample bytes, timestamps, body/name). The isolated term must equal
    /// that estimator's own attribute-pair `size_of`: `Label` for metrics,
    /// `(String, String)` for spans (byte-identical to `Label`), and the wider
    /// `(String, AttrValue)` for logs. This is the pin that stops the four rules
    /// drifting apart again.
    #[test]
    fn every_estimator_charges_a_per_attribute_header() {
        const W: u64 = 8;
        let label_sz = size_of::<Label>() as u64;

        // Metrics point: header per label is `size_of::<Label>()`.
        let labels = labels_of(W as usize);
        let label_content: u64 = labels
            .iter()
            .map(|l| (l.name.len() + l.value.len()) as u64)
            .sum();
        let point = point_with(labels.clone());
        let point_hdr = point.est_charge_bytes() - 16 - label_content;
        assert_eq!(
            point_hdr,
            W * label_sz,
            "IngestPoint::est_charge_bytes dropped the per-label header: \
             {point_hdr} != {}",
            W * label_sz
        );

        // Metrics exemplar: same `Label` header per filtered attribute.
        let exemplar = exemplar_with(labels);
        let exemplar_hdr =
            (exemplar.est_bytes() - size_of::<IngestExemplar>()) as u64 - label_content;
        assert_eq!(
            exemplar_hdr,
            W * label_sz,
            "IngestExemplar::est_bytes dropped the per-attribute header: \
             {exemplar_hdr} != {}",
            W * label_sz
        );

        // Log record: pair is `(String, AttrValue)`, wider than a `Label`.
        let log_pair = size_of::<(String, AttrValue)>() as u64;
        let attr_content = W * ("attr".len() + "v".len()) as u64;
        let log_hdr = (est_record_bytes(&log_record_with(W as usize))
            - est_record_bytes(&log_record_with(0))) as u64
            - attr_content;
        assert_eq!(
            log_hdr,
            W * log_pair,
            "est_record_bytes dropped the per-attribute header: {log_hdr} != {}",
            W * log_pair
        );

        // Span: pair is `(String, String)`, byte-identical to a `Label`.
        let span_pair = size_of::<(String, String)>() as u64;
        let span_hdr = (est_span_bytes(&span_with(W as usize)) - est_span_bytes(&span_with(0)))
            as u64
            - attr_content;
        assert_eq!(
            span_hdr,
            W * span_pair,
            "est_span_bytes dropped the per-attribute header: {span_hdr} != {}",
            W * span_pair
        );

        // Consistency across all four: the two-`String`-pair signals (point,
        // exemplar, span) charge the identical per-attribute header, and the
        // log record's `(String, AttrValue)` header is never smaller (a smaller
        // one would undercharge the shared ceiling on logs against the rest).
        assert_eq!(
            span_pair, label_sz,
            "span attr pair must match `Label` width"
        );
        assert!(
            log_pair >= label_sz,
            "log attr pair must be at least `Label` width"
        );
        assert_eq!(
            point_hdr, exemplar_hdr,
            "point and exemplar headers must agree"
        );
        assert_eq!(point_hdr, span_hdr, "point and span headers must agree");
    }

    /// The two buffered-byte estimators must charge one label the same way, or
    /// the process-wide ceiling and the exemplar accounting drift apart again.
    ///
    /// Two bounds, both stated here so a future edit has to break one of them:
    /// the per-label part of each estimate is byte-identical at every width,
    /// and once labels dominate the fixed struct overheads (10 labels and up)
    /// the totals stay within 25% of each other. At one label the fixed
    /// overheads still dominate -- an `IngestExemplar` carries a trace id, a
    /// span id, and a `Vec` header where a point carries 16 bytes of sample --
    /// so only the per-label bound is meaningful there.
    #[test]
    fn both_estimators_charge_a_label_the_same_way() {
        for width in [1usize, 10, 64] {
            let labels = labels_of(width);
            let point = point_with(labels.clone());
            let exemplar = exemplar_with(labels);

            let point_labels = point.est_charge_bytes() - 16;
            let exemplar_attrs = (exemplar.est_bytes() - size_of::<IngestExemplar>()) as u64;
            assert_eq!(
                point_labels, exemplar_attrs,
                "{width} labels: point charges {point_labels} label bytes, \
                 exemplar charges {exemplar_attrs}"
            );

            // Every label costs at least a `Label` header, whatever its strings
            // hold. This is the specific undercount the pin exists to catch:
            // dropping the header term leaves 3 to 4 bytes per label here.
            assert!(
                point_labels >= (width * size_of::<Label>()) as u64,
                "{width} labels: {point_labels} bytes charged is below the \
                 {} bytes of `Label` headers the buffer holds",
                width * size_of::<Label>()
            );

            if width >= 10 {
                let ratio = point.est_charge_bytes() as f64 / exemplar.est_bytes() as f64;
                assert!(
                    (0.75..=1.25).contains(&ratio),
                    "{width} labels: estimator ratio {ratio} outside [0.75, 1.25] \
                     (point {}, exemplar {})",
                    point.est_charge_bytes(),
                    exemplar.est_bytes()
                );
            }
        }
    }
}
