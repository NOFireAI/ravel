//! Generalized ingest point/value shapes: a
//! point carries either a scalar sample or a native-histogram sample. Wire
//! admission produces both: `ravel_otlp::NormalizedPoint` (scalar) and
//! `ravel_otlp::NormalizedHistogramPoint` (native histogram) each convert
//! into an [`IngestPoint`], so the shard buffer and segment-write plumbing
//! reach the same RSEG v5 writer regardless of which wire path decoded the
//! point.

use std::sync::Arc;

use ravel_segment::{ExemplarInput, HistogramCounts, HistogramSample, HistogramValue};
use ravel_types::{Exemplar, Label, LabelSet, Sample, SeriesId};

/// Bytes one buffered scalar sample contributes to the object a flush writes,
/// before any codec runs: the `(i64, f64)` pair ADR-0092 measures as "Raw
/// `(i64, f64)` is 16 bytes per sample".
pub(crate) const SCALAR_SAMPLE_OBJECT_BYTES: usize = 16;

/// Bytes one series contributes to the object a flush writes, on top of its
/// samples and its label bytes: the 16-byte `SERIES_IDS` row and the series'
/// `SERIES_META` row. ADR-0092 measures the marginal catalog cost at 11.04
/// bytes per run after zstd, so 32 stays above what the object holds.
pub(crate) const SERIES_OBJECT_OVERHEAD_BYTES: usize = 32;

/// Bytes one admitted exemplar contributes to the object a flush writes,
/// before its attributes: ADR-0047's "roughly 40 bytes plus attributes" (the
/// trace id, the span id, the timestamp, and the value).
const EXEMPLAR_OBJECT_BYTES: usize = 40;

/// Fixed per-sample cost of a native histogram in the object, before its
/// buckets: timestamp, scale, zero threshold, sum, count, and zero count.
const HISTOGRAM_SAMPLE_FIXED_BYTES: usize = 32;

/// Widest stored form of one histogram bucket count, span, or custom boundary.
/// The writer varint-encodes counts and delta-encodes boundaries, so a flat 8
/// per element stays above what the object holds.
const HISTOGRAM_ELEMENT_BYTES: usize = 8;

/// The object-side size of one native-histogram sample, counting each bucket
/// count, span, and custom boundary at its widest stored width.
fn histogram_object_bytes(value: &HistogramValue) -> usize {
    let buckets = match &value.counts {
        HistogramCounts::Int {
            positive, negative, ..
        } => positive.len() + negative.len(),
        HistogramCounts::Float {
            positive, negative, ..
        } => positive.len() + negative.len(),
    };
    let spans = value.positive_spans.len() + value.negative_spans.len();
    let custom = value
        .custom_values
        .as_ref()
        .map(Vec::len)
        .unwrap_or_default();
    HISTOGRAM_SAMPLE_FIXED_BYTES + HISTOGRAM_ELEMENT_BYTES * (buckets + spans + custom)
}

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
    /// One label set shared across the points of a series run (ADR-0098).
    /// The points of one run clone the same `Arc` rather than each owning a
    /// copy, so the shard buffer's collision pre-pass can short-circuit on
    /// [`Arc::ptr_eq`] and the flush moves a single allocation.
    pub labels: Arc<LabelSet>,
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

    /// Estimated bytes this exemplar contributes to the object a flush writes,
    /// for the size trigger only (issue #1305). The memory-side figure is
    /// [`IngestExemplar::est_bytes`] and stays what the byte budget charges:
    /// this one drops the `Label` and `Vec` struct headers, which the buffer
    /// holds and the object never stores, and keeps only the attribute bytes
    /// that reach LABEL_DICT.
    ///
    /// An over-estimate on two counts, both in the chosen direction (see
    /// [`IngestPoint::est_object_sample_bytes`]): the flush-scoped cap
    /// (`FlushCtx::admit_exemplars`) drops exemplars this figure already
    /// counted, and the stored attributes are interned and compressed.
    pub(crate) fn est_object_bytes(&self) -> usize {
        let attrs: usize = self
            .exemplar
            .filtered_attributes
            .iter()
            .map(|l| l.name.len() + l.value.len())
            .sum();
        EXEMPLAR_OBJECT_BYTES + attrs
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

    /// Estimated bytes this point's sample contributes to the object a flush
    /// writes, for the size trigger only (`target_bytes` and `min_flush_bytes`,
    /// issue #1305). Never the memory ceiling: that is
    /// [`IngestPoint::est_charge_bytes`], which charges what the buffer holds in
    /// RAM, and the two differ by more than an order of magnitude on a
    /// label-heavy series. A ten-label series with short values charges about
    /// 766 bytes per sample to the ceiling and contributes 16 here, so a size
    /// trigger reading the ceiling figure fires at a few hundred KB of data on a
    /// nominal 8 MiB `target_bytes` and the mean object size (which sets the
    /// request cost per stored terabyte) collapses.
    ///
    /// Model: the unencoded payload the RSEG writer is handed. One `(i64, f64)`
    /// pair per scalar sample; for a native histogram, its fixed fields plus its
    /// bucket counts, spans, and custom boundaries at their widest stored width.
    /// No `String`, `Vec`, or `Label` struct header appears, because none of
    /// them reach the object; the series' label bytes and its per-series rows
    /// are counted once per series by
    /// [`IngestPoint::est_object_series_bytes`], not per sample.
    ///
    /// Direction: this is an UPPER bound on the bytes the flush writes. Every
    /// codec in the write path moves in one direction from the unencoded
    /// payload -- delta-plus-zigzag varint timestamps, the integer-model and
    /// Gorilla value codecs, zstd over the catalog sections, LZ4 over the pages
    /// -- so the object lands at or under `target_bytes`, never over. That is
    /// the chosen failure: an object below target costs some request overhead,
    /// while an object over target costs buffered memory the operator did not
    /// ask for, on a compressibility ratio the client controls. The other side
    /// is bounded by the codecs' own reach: ADR-0092's amendment measures 2.50
    /// to 3.00 bytes per sample on representative value shapes, so the
    /// overshoot against the 16 counted here is bounded by about 6x rather than
    /// being open-ended, and the age triggers still bound how long a partly
    /// filled buffer waits.
    pub(crate) fn est_object_sample_bytes(&self) -> usize {
        match &self.value {
            IngestValue::Scalar(_) => SCALAR_SAMPLE_OBJECT_BYTES,
            IngestValue::Histogram(sample) => histogram_object_bytes(&sample.value),
        }
    }

    /// Estimated bytes this point's series contributes to the object a flush
    /// writes, charged once per series at its first sighting in a buffer (the
    /// object holds one label-dictionary entry and one series row per series,
    /// however many samples that series carries). Label bytes without their
    /// `Label` headers, for the reason
    /// [`IngestPoint::est_object_sample_bytes`] gives.
    pub(crate) fn est_object_series_bytes(&self) -> usize {
        let label_bytes: usize = self
            .labels
            .iter()
            .map(|l| l.name.len() + l.value.len())
            .sum();
        SERIES_OBJECT_OVERHEAD_BYTES + label_bytes
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::Exemplar;

    use crate::log_shard::{est_record_bytes, est_record_object_bytes};
    use crate::span_shard::{est_span_bytes, est_span_object_bytes};
    use ravel_otlp::logs_normalize::NormalizedLogRecord;
    use ravel_otlp::traces_normalize::NormalizedSpan;
    use ravel_rspan::StatusCode;
    use ravel_segment::{HistogramSpan, ResetHint};
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
            labels: Arc::new(labels),
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

    /// A log record whose single top-level attribute is a `Map` of `entry_count`
    /// identical `("", Bool)` entries, isolating the nested-level per-entry
    /// header. The top-level attribute is present at every `entry_count`, so
    /// differencing two counts cancels it and leaves only the nested cost.
    fn log_record_with_nested_map(entry_count: usize) -> NormalizedLogRecord {
        let entries = (0..entry_count)
            .map(|_| (String::new(), AttrValue::Bool(false)))
            .collect();
        let mut rec = log_record_with(0);
        rec.attrs = vec![("m".to_string(), AttrValue::Map(entries))];
        rec
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
    /// budget (ADR-0069) must charge a per-attribute struct header for its
    /// TOP-LEVEL attributes, not only the attribute's string bytes. Metrics
    /// points and exemplars, log records, and spans all charge that single
    /// shared ceiling by `Arc` (docs/ingest.md), so if any one estimator drops
    /// the header term it silently undercharges the ceiling on its own signal
    /// while the others charge honestly -- exactly the skew this pin exists to
    /// catch.
    ///
    /// Scope: the four-way top-level check below uses `AttrValue::Str` values at
    /// depth 0. The final block extends the log estimator one level deeper, to a
    /// nested `Map` value, pinning that `attr_value_len` charges the
    /// `(String, AttrValue)` header for every nested entry, not only for the
    /// top-level attribute. It does not exercise the metrics/span estimators
    /// below depth 0, which have no nested attribute values to charge.
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

        // Nested level: a log attribute whose value is a `Map` charges the
        // `(String, AttrValue)` header for each of its entries too, not only for
        // the top-level attribute. Differencing a W-entry nested `Map` against an
        // empty one cancels the (identical) top-level attribute and isolates the
        // nested-entry cost, W * (`(String, AttrValue)` header + one Bool byte).
        let nested_delta = (est_record_bytes(&log_record_with_nested_map(W as usize))
            - est_record_bytes(&log_record_with_nested_map(0))) as u64;
        let nested_hdr = nested_delta - W; // subtract each entry's Bool payload byte
        assert_eq!(
            nested_hdr,
            W * log_pair,
            "attr_value_len dropped the per-entry header inside a nested Map: \
             {nested_hdr} != {}",
            W * log_pair
        );
    }

    /// The size trigger's estimators count payload only. Where the ceiling
    /// charges a struct header per label, attribute, or span attribute, the
    /// object-side figure charges the bytes that reach the object and a fixed
    /// per-record term, on every signal. Exact figures, so widening either
    /// model has to come here and restate them.
    #[test]
    fn object_estimators_count_payload_not_struct_headers() {
        const W: usize = 8;
        // `labels_of(8)`: names `k0..k7` (2 bytes) with value "v" (1 byte),
        // so 24 bytes of label text and 8 * 48 bytes of `Label` headers.
        let point = point_with(labels_of(W));
        assert_eq!(point.est_charge_bytes(), 16 + 8 * 48 + 24);
        assert_eq!(
            point.est_object_series_bytes(),
            32 + 24,
            "series overhead plus label text, no `Label` headers"
        );
        assert_eq!(
            point.est_object_sample_bytes(),
            16,
            "one scalar sample is a timestamp and a value"
        );

        // A ten-label series is the shape issue #1305 measured: the ceiling
        // charges 480 bytes of headers per point that no object ever holds.
        let wide = point_with(labels_of(10));
        let charged = wide.est_charge_bytes() as usize;
        let object = wide.est_object_series_bytes() + wide.est_object_sample_bytes();
        assert_eq!(charged, 526);
        assert_eq!(object, 78);

        // Log record: 8 attributes of ("attr", Str("v")), everything else
        // empty, so 8 * 5 payload bytes plus the fixed per-record term.
        let record = log_record_with(W);
        assert_eq!(est_record_object_bytes(&record), 8 * 5 + 48);
        assert!(
            est_record_bytes(&record) > est_record_object_bytes(&record),
            "the ceiling stays above the object-side figure"
        );

        // Nested attribute values drop their headers too: one "m" attribute
        // holding a Map of 8 empty-keyed Bools is 1 key byte and 8 value
        // bytes over the fixed term.
        let nested = log_record_with_nested_map(W);
        assert_eq!(est_record_object_bytes(&nested), 1 + 8 + 48);

        // Span: same 8 attributes, empty name and status message.
        let span = span_with(W);
        assert_eq!(est_span_object_bytes(&span), 8 * 5 + 64);
        assert!(
            est_span_bytes(&span) > est_span_object_bytes(&span),
            "the ceiling stays above the object-side figure"
        );
    }

    /// A native histogram's object cost grows with its buckets, spans, and
    /// custom boundaries. The flat 16 bytes the ceiling charges a histogram
    /// point would make a bucket-heavy tenant's objects arbitrarily larger
    /// than `target_bytes` if the trigger reused it.
    #[test]
    fn histogram_object_bytes_counts_every_element() {
        let mut value = HistogramValue {
            scale: 2,
            zero_threshold: 1e-9,
            sum: Some(42.5),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 3,
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 1,
                count: 7,
                positive: vec![2, 3, 1],
                negative: vec![],
            },
            reset_hint: ResetHint::Unknown,
        };
        // 32 fixed, plus 8 per element over 3 bucket counts and 1 span.
        assert_eq!(histogram_object_bytes(&value), 32 + 8 * 4);

        value.counts = HistogramCounts::Int {
            zero_count: 1,
            count: 7,
            positive: vec![2; 100],
            negative: vec![1; 20],
        };
        assert_eq!(
            histogram_object_bytes(&value),
            32 + 8 * (100 + 20 + 1),
            "a wide histogram costs more than a narrow one"
        );
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
