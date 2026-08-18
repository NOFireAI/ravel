//! RW 2.0 decode: snappy-decompress and protobuf-decode an
//! `io.prometheus.write.v2.Request`, resolve every symbol-table reference,
//! then converge on the same [`ResolvedRequest`] shape the RW1 decoder
//! produces (ADR-0015).
//!
//! Every `labels_refs` index (on a series or an exemplar) and every
//! metadata `help_ref`/`unit_ref` is validated in range of `symbols` before
//! any string is used. A label name resolving to the empty string is also
//! rejected here, at decode time: `symbols[0]` is conventionally the empty
//! string per the RW2 spec (used by senders for unset optional refs like
//! `unit_ref`), so a labels_refs *name* position pointing at it is a
//! malformed reference, not a Prometheus label the normalizer should see
//! (unlike an empty label *value*, which is a normal "absent" convention
//! handled downstream in [`crate::normalize`]). Every failure here is a
//! typed, non-retryable [`Rw2DecodeError`]; this module never panics on
//! malformed input.

use std::collections::HashSet;

use prost::Message;
use ravel_otlp::metadata::{MetricKind, MetricMetadata};
use ravel_types::{Label, METRIC_NAME_LABEL};

use crate::proto::write_v2::histogram::{Count, ZeroCount};
use crate::proto::write_v2::{
    BucketSpan, Histogram as ProtoHistogramV2, Request, TimeSeries as ProtoTimeSeriesV2,
};
use crate::resolved::{
    ResolvedCount, ResolvedExemplar, ResolvedHistogram, ResolvedRequest, ResolvedSample,
    ResolvedSeries, ResolvedSpan,
};
use crate::snappy::{self, SnappyError};

#[derive(Debug, thiserror::Error)]
pub enum Rw2DecodeError {
    #[error("snappy decompression failed: {0}")]
    Snappy(#[from] SnappyError),
    #[error("malformed Request protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),

    #[error(
        "timeseries {series_index}: labels_refs has odd length {len}, must be (name, value) index pairs"
    )]
    OddLabelRefsLength { series_index: usize, len: usize },

    #[error(
        "timeseries {series_index}: labels_refs[{position}] = {symbol_ref} is out of range for a symbols table of length {symbols_len}"
    )]
    LabelRefOutOfRange {
        series_index: usize,
        position: usize,
        symbol_ref: u32,
        symbols_len: usize,
    },

    #[error(
        "timeseries {series_index}: labels_refs[{position}] names a label with an empty resolved name (symbol_ref {symbol_ref})"
    )]
    EmptyLabelName {
        series_index: usize,
        position: usize,
        symbol_ref: u32,
    },

    #[error(
        "timeseries {series_index} exemplar {exemplar_index}: labels_refs has odd length {len}"
    )]
    OddExemplarLabelRefsLength {
        series_index: usize,
        exemplar_index: usize,
        len: usize,
    },

    #[error(
        "timeseries {series_index} exemplar {exemplar_index}: labels_refs[{position}] = {symbol_ref} is out of range for a symbols table of length {symbols_len}"
    )]
    ExemplarLabelRefOutOfRange {
        series_index: usize,
        exemplar_index: usize,
        position: usize,
        symbol_ref: u32,
        symbols_len: usize,
    },

    #[error(
        "timeseries {series_index}: metadata help_ref {symbol_ref} is out of range for a symbols table of length {symbols_len}"
    )]
    MetadataHelpRefOutOfRange {
        series_index: usize,
        symbol_ref: u32,
        symbols_len: usize,
    },

    #[error(
        "timeseries {series_index}: metadata unit_ref {symbol_ref} is out of range for a symbols table of length {symbols_len}"
    )]
    MetadataUnitRefOutOfRange {
        series_index: usize,
        symbol_ref: u32,
        symbols_len: usize,
    },

    #[error(
        "resolved label bytes {resolved_bytes} exceed the budget of {budget_bytes} bytes ({RESOLVED_LABEL_BUDGET_MULTIPLIER}x max_decompressed_bytes)"
    )]
    ResolvedLabelBudgetExceeded {
        resolved_bytes: usize,
        budget_bytes: usize,
    },
}

/// `labels_refs` indexes are cheap on the wire (a handful of varint bytes
/// each) but every occurrence clones a full symbol string on resolution.
/// `symbols` itself already costs one byte of the decompressed body per
/// byte of string, so legitimate reuse of a small label vocabulary across
/// many series is what the decompressed-size cap is already sized for;
/// this multiplier gives that reuse generous headroom while still bounding
/// how many multiples of the wire size an adversarial `labels_refs` list
/// (many refs, few but large symbols) can blow up into.
const RESOLVED_LABEL_BUDGET_MULTIPLIER: usize = 16;

/// Decompress and decode `body` as an RW2 `Request`.
///
/// `max_decompressed_bytes` bounds the snappy output size, checked before
/// the output buffer is allocated (see [`crate::snappy::decompress`]), and
/// also derives the budget for cumulative resolved label bytes (see
/// [`Rw2DecodeError::ResolvedLabelBudgetExceeded`]).
pub fn decode_request(
    body: &[u8],
    max_decompressed_bytes: usize,
) -> Result<ResolvedRequest, Rw2DecodeError> {
    let raw = snappy::decompress(body, max_decompressed_bytes)?;
    let req = Request::decode(raw.as_slice())?;
    let resolved_label_budget =
        max_decompressed_bytes.saturating_mul(RESOLVED_LABEL_BUDGET_MULTIPLIER);
    resolve(req, resolved_label_budget)
}

fn resolve(req: Request, resolved_label_budget: usize) -> Result<ResolvedRequest, Rw2DecodeError> {
    let symbols = req.symbols;
    let mut series = Vec::with_capacity(req.timeseries.len());
    let mut metadata: Vec<MetricMetadata> = Vec::new();
    let mut metadata_seen: HashSet<String> = HashSet::new();
    let mut created_timestamps_count = 0usize;
    let mut exemplars_dropped = 0usize;
    let mut resolved_label_bytes = 0usize;
    // Exemplar labels charge a pool of their own, not the series pool. Series
    // overflow rejects the request and exemplar overflow drops one exemplar, so
    // a shared pool lets exemplar bytes decide whether an in-budget series
    // survives, which loses a whole batch of samples over an illustrative
    // signal.
    let mut resolved_exemplar_label_bytes = 0usize;
    for (series_index, ts) in req.timeseries.into_iter().enumerate() {
        series.push(resolve_series(
            series_index,
            ts,
            &symbols,
            &mut metadata,
            &mut metadata_seen,
            &mut created_timestamps_count,
            &mut exemplars_dropped,
            &mut resolved_label_bytes,
            &mut resolved_exemplar_label_bytes,
            resolved_label_budget,
        )?);
    }
    Ok(ResolvedRequest {
        series,
        metadata,
        created_timestamps_count,
        exemplars_dropped,
    })
}

/// Map one RW2 `io.prometheus.write.v2.Metadata` type to the protocol-neutral
/// kind. Types with no canonical Ravel kind (`GAUGEHISTOGRAM`, `INFO`,
/// `STATESET`, `UNSPECIFIED`) map to [`MetricKind::Unknown`].
fn rw2_kind(ty: i32) -> MetricKind {
    use crate::proto::write_v2::metadata::MetricType;
    match MetricType::try_from(ty) {
        Ok(MetricType::Counter) => MetricKind::Counter,
        Ok(MetricType::Gauge) => MetricKind::Gauge,
        Ok(MetricType::Histogram) => MetricKind::Histogram,
        Ok(MetricType::Summary) => MetricKind::Summary,
        _ => MetricKind::Unknown,
    }
}

/// Strip one structural suffix from an RW2 series `__name__` to recover the
/// metric family name (ADR-0085 Decision 1): `_bucket`/`_sum`/`_count` always,
/// and `_total` only when the metric type is `Counter` (so a gauge legitimately
/// named `foo_total` keeps its name). At most one suffix is removed.
fn strip_structural_suffix(name: &str, kind: MetricKind) -> &str {
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(stripped) = name.strip_suffix(suffix) {
            return stripped;
        }
    }
    if matches!(kind, MetricKind::Counter)
        && let Some(stripped) = name.strip_suffix("_total")
    {
        return stripped;
    }
    name
}

#[allow(clippy::too_many_arguments)]
fn resolve_series(
    series_index: usize,
    ts: ProtoTimeSeriesV2,
    symbols: &[String],
    metadata: &mut Vec<MetricMetadata>,
    metadata_seen: &mut HashSet<String>,
    created_timestamps_count: &mut usize,
    exemplars_dropped: &mut usize,
    resolved_label_bytes: &mut usize,
    resolved_exemplar_label_bytes: &mut usize,
    resolved_label_budget: usize,
) -> Result<ResolvedSeries, Rw2DecodeError> {
    let labels = resolve_label_refs(
        series_index,
        &ts.labels_refs,
        symbols,
        resolved_label_bytes,
        resolved_label_budget,
    )?;

    let mut exemplars = Vec::with_capacity(ts.exemplars.len());
    for (exemplar_index, exemplar) in ts.exemplars.iter().enumerate() {
        // An exemplar whose labels would carry the request past the budget is
        // dropped and counted, never a reason to reject the series that carries
        // it or the request as a whole (the `?` here still propagates a
        // genuinely malformed reference: an odd length or an out-of-range
        // symbol index, which is protocol corruption rather than size).
        match resolve_exemplar_label_refs(
            series_index,
            exemplar_index,
            &exemplar.labels_refs,
            symbols,
            resolved_exemplar_label_bytes,
            resolved_label_budget,
        )? {
            Some(labels) => exemplars.push(ResolvedExemplar {
                ts_ms: exemplar.timestamp,
                value: exemplar.value,
                labels,
            }),
            None => *exemplars_dropped += 1,
        }
    }

    if let Some(meta) = &ts.metadata {
        if meta.help_ref as usize >= symbols.len() {
            return Err(Rw2DecodeError::MetadataHelpRefOutOfRange {
                series_index,
                symbol_ref: meta.help_ref,
                symbols_len: symbols.len(),
            });
        }
        if meta.unit_ref as usize >= symbols.len() {
            return Err(Rw2DecodeError::MetadataUnitRefOutOfRange {
                series_index,
                symbol_ref: meta.unit_ref,
                symbols_len: symbols.len(),
            });
        }
        // One tuple per metric family, keyed by the series `__name__` stripped
        // of a structural suffix and deduplicated first-wins, so a classic
        // histogram's `_bucket`/`_sum`/`_count` series collapse to one entry
        // (ADR-0085 Decision 1). A series carrying metadata but no `__name__`
        // has no family to key on and is skipped. help_ref/unit_ref are already
        // range-checked above, so indexing `symbols` is in bounds.
        let kind = rw2_kind(meta.r#type);
        if let Some(name) = labels
            .iter()
            .find(|l| l.name == METRIC_NAME_LABEL)
            .map(|l| l.value.as_str())
            .filter(|v| !v.is_empty())
        {
            let family = strip_structural_suffix(name, kind).to_string();
            if metadata_seen.insert(family.clone()) {
                metadata.push(MetricMetadata {
                    family_name: family,
                    kind,
                    help: symbols[meta.help_ref as usize].clone(),
                    unit: symbols[meta.unit_ref as usize].clone(),
                });
            }
        }
    }

    let mut samples = Vec::with_capacity(ts.samples.len());
    for s in &ts.samples {
        if s.start_timestamp != 0 {
            *created_timestamps_count += 1;
        }
        samples.push(ResolvedSample {
            ts_ms: s.timestamp,
            value: s.value,
        });
    }
    let mut histograms = Vec::with_capacity(ts.histograms.len());
    for h in ts.histograms {
        if h.start_timestamp != 0 {
            *created_timestamps_count += 1;
        }
        histograms.push(resolve_histogram(h));
    }

    Ok(ResolvedSeries {
        labels,
        samples,
        histograms,
        exemplars,
    })
}

/// Field-for-field mapping of one wire `io.prometheus.write.v2.Histogram`
/// into the version-blind [`ResolvedHistogram`]. RW2's message is
/// field-compatible with RW1's apart from the extra `start_timestamp`
/// (tallied by the caller as a dropped created timestamp), so this is the
/// twin of `rw1::resolve_histogram` and validates nothing for the same
/// reason: [`crate::normalize`] owns every admission decision.
fn resolve_histogram(h: ProtoHistogramV2) -> ResolvedHistogram {
    ResolvedHistogram {
        ts_ms: h.timestamp,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        sum: h.sum,
        count: h.count.map(|c| match c {
            Count::CountInt(v) => ResolvedCount::Int(v),
            Count::CountFloat(v) => ResolvedCount::Float(v),
        }),
        zero_count: h.zero_count.map(|z| match z {
            ZeroCount::ZeroCountInt(v) => ResolvedCount::Int(v),
            ZeroCount::ZeroCountFloat(v) => ResolvedCount::Float(v),
        }),
        positive_spans: h.positive_spans.iter().map(resolve_span).collect(),
        negative_spans: h.negative_spans.iter().map(resolve_span).collect(),
        positive_deltas: h.positive_deltas,
        negative_deltas: h.negative_deltas,
        positive_counts: h.positive_counts,
        negative_counts: h.negative_counts,
        reset_hint: h.reset_hint,
        custom_values: h.custom_values,
    }
}

fn resolve_span(span: &BucketSpan) -> ResolvedSpan {
    ResolvedSpan {
        offset: span.offset,
        length: span.length,
    }
}

/// Resolve one series' `labels_refs` into owned [`Label`]s: length must be
/// even (name, value index pairs), every index must be in range of
/// `symbols`, and a name resolving to the empty string is rejected (see
/// module docs).
fn resolve_label_refs(
    series_index: usize,
    labels_refs: &[u32],
    symbols: &[String],
    resolved_label_bytes: &mut usize,
    resolved_label_budget: usize,
) -> Result<Vec<Label>, Rw2DecodeError> {
    if !labels_refs.len().is_multiple_of(2) {
        return Err(Rw2DecodeError::OddLabelRefsLength {
            series_index,
            len: labels_refs.len(),
        });
    }
    let mut labels = Vec::with_capacity(labels_refs.len() / 2);
    for (pair_index, pair) in labels_refs.chunks_exact(2).enumerate() {
        let name_position = pair_index * 2;
        let name_ref = pair[0];
        let value_ref = pair[1];
        let name = symbols
            .get(name_ref as usize)
            .ok_or(Rw2DecodeError::LabelRefOutOfRange {
                series_index,
                position: name_position,
                symbol_ref: name_ref,
                symbols_len: symbols.len(),
            })?;
        if name.is_empty() {
            return Err(Rw2DecodeError::EmptyLabelName {
                series_index,
                position: name_position,
                symbol_ref: name_ref,
            });
        }
        let value = symbols
            .get(value_ref as usize)
            .ok_or(Rw2DecodeError::LabelRefOutOfRange {
                series_index,
                position: name_position + 1,
                symbol_ref: value_ref,
                symbols_len: symbols.len(),
            })?;
        *resolved_label_bytes += name.len() + value.len();
        if *resolved_label_bytes > resolved_label_budget {
            return Err(Rw2DecodeError::ResolvedLabelBudgetExceeded {
                resolved_bytes: *resolved_label_bytes,
                budget_bytes: resolved_label_budget,
            });
        }
        labels.push(Label {
            name: name.clone(),
            value: value.clone(),
        });
    }
    Ok(labels)
}

/// Resolve one exemplar's `labels_refs` into labels, enforcing the same
/// reference-shape rules as before ADR-0047 gave exemplars storage (even
/// length, every index in range of `symbols`). Now that these strings are kept
/// rather than discarded, an adversarial exemplar `labels_refs` list amplifies
/// wire bytes into resident bytes exactly like a series one, so exemplar labels
/// carry a budget too.
///
/// The counter is a pool of the exemplars' own, sized by the same budget but
/// separate from the series pool. The two pools cannot share, because they
/// answer overflow differently: a series overflow rejects the whole request and
/// an exemplar overflow drops one exemplar. On a shared pool, bytes charged by
/// a kept exemplar decide whether a later, in-budget series survives, so one
/// exemplar costs a whole batch of samples.
///
/// Returns `Ok(Some(labels))` when the exemplar fits the pool (and only then
/// commits its bytes to `resolved_label_bytes`), and `Ok(None)` when it does
/// not: an over-budget exemplar is dropped and counted by the caller, never a
/// reason to reject the series or the request. An exemplar is a decoration on
/// the series data (ADR-0047 decision 1 carries its labels as filtered
/// attributes), so losing the metric payload over it would trade the data for
/// the decoration. A dropped exemplar charges nothing: its bytes are not
/// resident, so a later exemplar must not see the pool as if they were.
///
/// `Err` is still returned for a genuinely malformed reference (odd length or
/// an out-of-range symbol index): that is protocol corruption, not size, and
/// keeps the pre-existing shape validation intact.
///
/// Unlike [`resolve_label_refs`], an empty label *name* is not rejected here.
/// Exemplar labels are not series identity (ADR-0047 decision 1 carries them
/// as an exemplar's filtered attributes), so an odd name cannot alias one
/// series onto another, and rejecting a whole request over an informational
/// attribute would be stricter than the sender's own model.
fn resolve_exemplar_label_refs(
    series_index: usize,
    exemplar_index: usize,
    labels_refs: &[u32],
    symbols: &[String],
    resolved_label_bytes: &mut usize,
    resolved_label_budget: usize,
) -> Result<Option<Vec<Label>>, Rw2DecodeError> {
    if !labels_refs.len().is_multiple_of(2) {
        return Err(Rw2DecodeError::OddExemplarLabelRefsLength {
            series_index,
            exemplar_index,
            len: labels_refs.len(),
        });
    }
    let resolve = |position: usize, symbol_ref: u32| {
        symbols
            .get(symbol_ref as usize)
            .ok_or(Rw2DecodeError::ExemplarLabelRefOutOfRange {
                series_index,
                exemplar_index,
                position,
                symbol_ref,
                symbols_len: symbols.len(),
            })
    };

    // Measure first, clone second. Resolving a `&String` and reading its length
    // allocates nothing, so an exemplar that does not fit costs work
    // proportional to its reference count, which the decompressed-body cap
    // already bounds. Cloning as it went instead would let one request repeat a
    // whole budget's worth of allocation for every dropped exemplar, and the
    // wire cost of another dropped exemplar is a handful of varints.
    let mut tentative_bytes = *resolved_label_bytes;
    for (pair_index, pair) in labels_refs.chunks_exact(2).enumerate() {
        let name_position = pair_index * 2;
        let name = resolve(name_position, pair[0])?;
        let value = resolve(name_position + 1, pair[1])?;
        tentative_bytes = tentative_bytes.saturating_add(name.len() + value.len());
        if tentative_bytes > resolved_label_budget {
            return Ok(None);
        }
    }

    let mut labels = Vec::with_capacity(labels_refs.len() / 2);
    for (pair_index, pair) in labels_refs.chunks_exact(2).enumerate() {
        let name_position = pair_index * 2;
        labels.push(Label {
            name: resolve(name_position, pair[0])?.clone(),
            value: resolve(name_position + 1, pair[1])?.clone(),
        });
    }
    *resolved_label_bytes = tentative_bytes;
    Ok(Some(labels))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::proto::write_v2::{
        Exemplar as ProtoExemplarV2, Metadata as ProtoMetadataV2, Sample as ProtoSampleV2,
    };

    fn compress(bytes: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new()
            .compress_vec(bytes)
            .expect("compress")
    }

    /// `symbols[0]` is conventionally the empty string per the RW2 spec.
    fn symbols(rest: &[&str]) -> Vec<String> {
        std::iter::once(String::new())
            .chain(rest.iter().map(|s| s.to_string()))
            .collect()
    }

    fn request(symbols: Vec<String>, timeseries: Vec<ProtoTimeSeriesV2>) -> Request {
        Request {
            symbols,
            timeseries,
        }
    }

    fn decode(req: &Request) -> Result<ResolvedRequest, Rw2DecodeError> {
        let body = compress(&req.encode_to_vec());
        decode_request(&body, 1_000_000)
    }

    #[test]
    fn decodes_labels_and_samples_via_symbol_table() {
        // symbols: [ "", "__name__", "up", "job", "svc" ]
        let syms = symbols(&["__name__", "up", "job", "svc"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![ProtoSampleV2 {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                    start_timestamp: 0,
                }],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        assert_eq!(resolved.series.len(), 1);
        let series = &resolved.series[0];
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0].name, "__name__");
        assert_eq!(series.labels[0].value, "up");
        assert_eq!(series.labels[1].name, "job");
        assert_eq!(series.labels[1].value, "svc");
        assert_eq!(series.samples.len(), 1);
        assert_eq!(series.samples[0].ts_ms, 1_700_000_000_000);
        assert_eq!(series.samples[0].value, 1.0);
        assert_eq!(resolved.created_timestamps_count, 0);
    }

    #[test]
    /// Native histograms are materialized (durable storage since RSEG v5), and
    /// so are exemplars, whose `labels_refs` now resolve against `symbols`
    /// into real labels rather than being validated and discarded (ADR-0047).
    fn materializes_histograms_and_exemplars() {
        let syms = symbols(&["__name__", "latency", "trace_id", "abcd"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![Default::default(), Default::default()],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: vec![3, 4],
                    value: 1.0,
                    timestamp: 1_000,
                }],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        assert_eq!(resolved.series[0].histograms.len(), 2);
        assert_eq!(
            resolved.series[0].exemplars,
            vec![ResolvedExemplar {
                ts_ms: 1_000,
                value: 1.0,
                labels: vec![Label {
                    name: "trace_id".to_string(),
                    value: "abcd".to_string(),
                }],
            }]
        );
    }

    /// Every histogram field reaches [`ResolvedHistogram`] unreshaped, and
    /// `start_timestamp` is still tallied as a dropped created timestamp
    /// rather than becoming part of the resolved value.
    #[test]
    fn resolves_every_histogram_field_verbatim() {
        use crate::proto::write_v2::histogram::ResetHint;
        let syms = symbols(&["__name__", "latency"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![ProtoHistogramV2 {
                    count: Some(Count::CountInt(14)),
                    zero_count: Some(ZeroCount::ZeroCountInt(1)),
                    sum: 42.5,
                    schema: -53,
                    zero_threshold: 1e-9,
                    positive_spans: vec![BucketSpan {
                        offset: -2,
                        length: 3,
                    }],
                    positive_deltas: vec![2, 3, 1],
                    negative_spans: vec![],
                    negative_deltas: vec![],
                    positive_counts: vec![],
                    negative_counts: vec![],
                    reset_hint: ResetHint::Gauge as i32,
                    timestamp: 1_700_000_000_000,
                    start_timestamp: 1_600_000_000_000,
                    custom_values: vec![0.5, 1.0, 2.5],
                }],
                exemplars: vec![],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        let h = &resolved.series[0].histograms[0];
        assert_eq!(h.ts_ms, 1_700_000_000_000);
        assert_eq!(h.schema, -53);
        assert_eq!(h.zero_threshold.to_bits(), 1e-9f64.to_bits());
        assert_eq!(h.sum.to_bits(), 42.5f64.to_bits());
        assert_eq!(h.count, Some(ResolvedCount::Int(14)));
        assert_eq!(h.zero_count, Some(ResolvedCount::Int(1)));
        assert_eq!(
            h.positive_spans,
            vec![ResolvedSpan {
                offset: -2,
                length: 3
            }]
        );
        assert_eq!(h.positive_deltas, vec![2, 3, 1]);
        assert_eq!(h.reset_hint, ResetHint::Gauge as i32);
        assert_eq!(h.custom_values, vec![0.5, 1.0, 2.5]);
        assert_eq!(resolved.created_timestamps_count, 1);
    }

    #[test]
    fn resolves_float_histogram_oneofs_as_float() {
        let syms = symbols(&["__name__", "latency"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![ProtoHistogramV2 {
                    count: Some(Count::CountFloat(14.5)),
                    zero_count: Some(ZeroCount::ZeroCountFloat(1.5)),
                    positive_spans: vec![BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_counts: vec![3.0, 4.0],
                    ..ProtoHistogramV2::default()
                }],
                exemplars: vec![],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        let h = &resolved.series[0].histograms[0];
        assert_eq!(h.count, Some(ResolvedCount::Float(14.5)));
        assert_eq!(h.zero_count, Some(ResolvedCount::Float(1.5)));
        assert_eq!(h.positive_counts, vec![3.0, 4.0]);
        assert!(h.positive_deltas.is_empty());
    }

    #[test]
    fn metadata_surfaces_as_a_family_tuple() {
        use ravel_otlp::metadata::MetricKind;
        let syms = symbols(&["__name__", "up", "help text", "seconds"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: Some(ProtoMetadataV2 {
                    r#type: 0,
                    help_ref: 3,
                    unit_ref: 4,
                }),
            }],
        );

        let resolved = decode(&req).expect("decode");
        assert_eq!(resolved.metadata.len(), 1);
        assert_eq!(resolved.metadata[0].family_name, "up");
        // Type 0 is METRIC_TYPE_UNSPECIFIED: no canonical kind, not guessed.
        assert_eq!(resolved.metadata[0].kind, MetricKind::Unknown);
        assert_eq!(resolved.metadata[0].help, "help text");
        assert_eq!(resolved.metadata[0].unit, "seconds");
    }

    #[test]
    fn metadata_tuples_use_family_name_stripped_of_structural_suffixes() {
        use crate::proto::write_v2::metadata::MetricType;
        use ravel_otlp::metadata::MetricKind;
        // 0="",1="__name__",2..6=series names,7="help text",8="s"
        let syms = symbols(&[
            "__name__",
            "foo_bucket",
            "foo_sum",
            "foo_count",
            "bar_total",
            "baz_total",
            "help text",
            "s",
        ]);
        let hist_meta = |name_ref: u32| ProtoTimeSeriesV2 {
            labels_refs: vec![1, name_ref],
            samples: vec![],
            histograms: vec![],
            exemplars: vec![],
            metadata: Some(ProtoMetadataV2 {
                r#type: MetricType::Histogram as i32,
                help_ref: 7,
                unit_ref: 8,
            }),
        };
        let typed = |name_ref: u32, ty: MetricType| ProtoTimeSeriesV2 {
            labels_refs: vec![1, name_ref],
            samples: vec![],
            histograms: vec![],
            exemplars: vec![],
            metadata: Some(ProtoMetadataV2 {
                r#type: ty as i32,
                help_ref: 7,
                unit_ref: 8,
            }),
        };
        let req = request(
            syms,
            vec![
                hist_meta(2),                  // foo_bucket -> foo
                hist_meta(3),                  // foo_sum    -> foo (deduped)
                hist_meta(4),                  // foo_count  -> foo (deduped)
                typed(5, MetricType::Counter), // bar_total -> bar
                typed(6, MetricType::Gauge),   // baz_total stays (not a counter)
            ],
        );

        let resolved = decode(&req).expect("decode");
        let families: Vec<(&str, MetricKind)> = resolved
            .metadata
            .iter()
            .map(|m| (m.family_name.as_str(), m.kind))
            .collect();
        assert_eq!(
            families,
            vec![
                ("foo", MetricKind::Histogram),
                ("bar", MetricKind::Counter),
                ("baz_total", MetricKind::Gauge),
            ]
        );
    }

    #[test]
    fn start_timestamp_on_sample_and_histogram_counts_as_created_timestamp() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![ProtoSampleV2 {
                    value: 1.0,
                    timestamp: 1_000,
                    start_timestamp: 500,
                }],
                histograms: vec![crate::proto::write_v2::Histogram {
                    start_timestamp: 500,
                    ..Default::default()
                }],
                exemplars: vec![],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        assert_eq!(resolved.created_timestamps_count, 2);
    }

    #[test]
    fn empty_request_decodes_to_empty_resolved() {
        let req = request(symbols(&[]), vec![]);
        let resolved = decode(&req).expect("decode");
        assert!(resolved.series.is_empty());
        assert!(resolved.metadata.is_empty());
        assert_eq!(resolved.created_timestamps_count, 0);
    }

    // --- symbol-table corruption: typed, non-panicking errors ---

    #[test]
    fn odd_labels_refs_length_is_typed_error() {
        let syms = symbols(&["__name__"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }],
        );
        let err = decode(&req).expect_err("odd length must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::OddLabelRefsLength {
                series_index: 0,
                len: 1
            }
        ));
    }

    #[test]
    fn out_of_range_label_ref_is_typed_error() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 99],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }],
        );
        let err = decode(&req).expect_err("out-of-range ref must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::LabelRefOutOfRange {
                series_index: 0,
                position: 1,
                symbol_ref: 99,
                ..
            }
        ));
    }

    #[test]
    fn empty_resolved_label_name_is_typed_error() {
        // symbols[0] is the conventional empty string; pointing a name ref
        // at it is a malformed reference, not a legitimately empty name.
        let syms = symbols(&["up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![0, 1],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }],
        );
        let err = decode(&req).expect_err("empty name must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::EmptyLabelName {
                series_index: 0,
                position: 0,
                symbol_ref: 0,
            }
        ));
    }

    #[test]
    fn odd_exemplar_labels_refs_length_is_typed_error() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: vec![1],
                    value: 1.0,
                    timestamp: 1_000,
                }],
                metadata: None,
            }],
        );
        let err = decode(&req).expect_err("odd exemplar length must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::OddExemplarLabelRefsLength {
                series_index: 0,
                exemplar_index: 0,
                len: 1
            }
        ));
    }

    #[test]
    fn out_of_range_exemplar_label_ref_is_typed_error() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: vec![1, 99],
                    value: 1.0,
                    timestamp: 1_000,
                }],
                metadata: None,
            }],
        );
        let err = decode(&req).expect_err("out-of-range exemplar ref must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::ExemplarLabelRefOutOfRange {
                series_index: 0,
                exemplar_index: 0,
                position: 1,
                symbol_ref: 99,
                ..
            }
        ));
    }

    #[test]
    fn out_of_range_metadata_help_ref_is_typed_error() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: Some(ProtoMetadataV2 {
                    r#type: 0,
                    help_ref: 99,
                    unit_ref: 0,
                }),
            }],
        );
        let err = decode(&req).expect_err("out-of-range help_ref must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::MetadataHelpRefOutOfRange {
                series_index: 0,
                symbol_ref: 99,
                ..
            }
        ));
    }

    #[test]
    fn out_of_range_metadata_unit_ref_is_typed_error() {
        let syms = symbols(&["__name__", "up"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: Some(ProtoMetadataV2 {
                    r#type: 0,
                    help_ref: 0,
                    unit_ref: 99,
                }),
            }],
        );
        let err = decode(&req).expect_err("out-of-range unit_ref must fail");
        assert!(matches!(
            err,
            Rw2DecodeError::MetadataUnitRefOutOfRange {
                series_index: 0,
                symbol_ref: 99,
                ..
            }
        ));
    }

    // --- resolved-label byte budget: bounds clones, not just wire bytes ---

    #[test]
    fn resolved_label_budget_exceeded_before_full_resolution() {
        // A small symbol table (one 10-byte name, one 100-byte value) with
        // labels_refs repeating the same pair 500 times: the wire and
        // decompressed body stay tiny (cheap u32 indices), but resolution
        // would clone 500 * (10 + 100) = 55_000 bytes, far past the budget
        // derived from a 2_000-byte decompressed cap.
        let name = "n".repeat(10);
        let value = "v".repeat(100);
        let syms = symbols(&[&name, &value]);
        let mut labels_refs = Vec::with_capacity(1_000);
        for _ in 0..500 {
            labels_refs.push(1u32);
            labels_refs.push(2u32);
        }
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs,
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }],
        );
        let body = compress(&req.encode_to_vec());
        assert!(
            body.len() < 2_000,
            "test body must stay small to prove the rejection isn't just the decompression cap: {}",
            body.len()
        );

        let err = decode_request(&body, 2_000).expect_err("resolved label budget must reject");
        assert!(matches!(
            err,
            Rw2DecodeError::ResolvedLabelBudgetExceeded { .. }
        ));
    }

    /// FINDING 2: an exemplar whose labels alone blow the budget is dropped and
    /// counted, leaving the in-budget series payload (its samples) fully
    /// intact. Before the fix the shared cumulative counter carried the
    /// exemplar bytes past the budget and the `?` propagated a
    /// `ResolvedLabelBudgetExceeded` out through decode, losing every sample in
    /// the batch.
    #[test]
    fn exemplar_only_overflow_drops_the_exemplar_and_keeps_the_series() {
        // A tiny series (refs 1,2) plus one exemplar whose 200 (name,value)
        // pairs resolve to 200 * (1 + 200) = 40_200 bytes, past the 32_000-byte
        // budget derived from a 2_000-byte decompressed cap. The series bytes
        // (a 8-byte name plus a 2-byte value) are well within budget.
        let big_value = "v".repeat(200);
        let syms = symbols(&["__name__", "up", "trace_id", &big_value]);
        let mut exemplar_refs = Vec::with_capacity(400);
        for _ in 0..200 {
            exemplar_refs.push(3u32); // "trace_id"
            exemplar_refs.push(4u32); // 200-byte value
        }
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![
                    ProtoSampleV2 {
                        value: 1.0,
                        timestamp: 1_700_000_000_000,
                        start_timestamp: 0,
                    },
                    ProtoSampleV2 {
                        value: 2.0,
                        timestamp: 1_700_000_001_000,
                        start_timestamp: 0,
                    },
                ],
                histograms: vec![],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: exemplar_refs,
                    value: 3.0,
                    timestamp: 1_700_000_000_500,
                }],
                metadata: None,
            }],
        );
        let body = compress(&req.encode_to_vec());

        let resolved = decode_request(&body, 2_000).expect("request must decode, not reject");
        // The series survives whole: both samples present, the metric name
        // resolved.
        assert_eq!(resolved.series.len(), 1);
        let series = &resolved.series[0];
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[0].value, 1.0);
        assert_eq!(series.samples[1].value, 2.0);
        // The over-budget exemplar is dropped, not carried, and counted.
        assert!(series.exemplars.is_empty());
        assert_eq!(resolved.exemplars_dropped, 1);
    }

    /// A kept exemplar's label bytes must not reject a later in-budget series.
    /// Exemplar labels charge a pool of their own: on a shared pool the first
    /// series' exemplar fills the budget, the second series overflows on its
    /// own small label set, and `decode_request` returns
    /// `ResolvedLabelBudgetExceeded`, losing every sample in the request over
    /// one illustrative exemplar.
    #[test]
    fn a_kept_exemplar_does_not_reject_a_later_series() {
        // Budget is 16 * 2_000 = 32_000 bytes. The exemplar resolves to
        // 153 * ("trace_id" + 200 bytes) = 31_824, which fits and is kept. A
        // shared pool would leave 176 bytes for every later series; series 1
        // needs 208.
        let big_value = "v".repeat(200);
        let syms = symbols(&["__name__", "up", "trace_id", &big_value, "slow"]);
        let mut exemplar_refs = Vec::with_capacity(306);
        for _ in 0..153 {
            exemplar_refs.push(3u32); // "trace_id"
            exemplar_refs.push(4u32); // 200-byte value
        }
        let sample = |value: f64| ProtoSampleV2 {
            value,
            timestamp: 1_700_000_000_000,
            start_timestamp: 0,
        };
        let req = request(
            syms,
            vec![
                ProtoTimeSeriesV2 {
                    labels_refs: vec![1, 2],
                    samples: vec![sample(1.0)],
                    histograms: vec![],
                    exemplars: vec![ProtoExemplarV2 {
                        labels_refs: exemplar_refs,
                        value: 3.0,
                        timestamp: 1_700_000_000_500,
                    }],
                    metadata: None,
                },
                ProtoTimeSeriesV2 {
                    // "__name__" plus the 200-byte value: 208 bytes.
                    labels_refs: vec![1, 4],
                    samples: vec![sample(2.0)],
                    histograms: vec![],
                    exemplars: vec![],
                    metadata: None,
                },
            ],
        );
        let body = compress(&req.encode_to_vec());

        let resolved = decode_request(&body, 2_000).expect("request must decode, not reject");
        assert_eq!(resolved.series.len(), 2, "both series must survive");
        assert_eq!(resolved.series[1].samples.len(), 1);
        assert_eq!(resolved.series[1].samples[0].value, 2.0);
        // The exemplar fit, so it is kept and nothing is counted as dropped.
        assert_eq!(resolved.series[0].exemplars.len(), 1);
        assert_eq!(resolved.exemplars_dropped, 0);
    }

    /// Many over-budget exemplars in one request stay cheap. Each one is
    /// measured against the budget before a single symbol is cloned, so the
    /// work per dropped exemplar is proportional to its reference count, not to
    /// the budget. Charging as it cloned instead let a few hundred wire bytes
    /// buy a budget's worth of allocation, once per exemplar.
    #[test]
    fn many_dropped_exemplars_charge_nothing_and_keep_every_series() {
        // One 8_000-byte symbol under a 16_000-byte decompressed cap, so the
        // budget is 256_000. Each exemplar names that symbol 40 times for
        // 40 * (8 + 8_000) = 320_320 resolved bytes, well past the budget, at a
        // wire cost of about 80 bytes. That ratio is the shape the fix is
        // about: a handful of wire bytes per exemplar, a budget's worth of
        // cloning per exemplar if the code clones before it measures.
        let big_value = "v".repeat(8_000);
        let syms = symbols(&["__name__", "up", "trace_id", &big_value]);
        let mut exemplar_refs = Vec::with_capacity(80);
        for _ in 0..40 {
            exemplar_refs.push(3u32);
            exemplar_refs.push(4u32);
        }
        let exemplars: Vec<ProtoExemplarV2> = (0..64)
            .map(|i| ProtoExemplarV2 {
                labels_refs: exemplar_refs.clone(),
                value: f64::from(i),
                timestamp: 1_700_000_000_500,
            })
            .collect();
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![ProtoSampleV2 {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                    start_timestamp: 0,
                }],
                histograms: vec![],
                exemplars,
                metadata: None,
            }],
        );
        let body = compress(&req.encode_to_vec());

        let resolved = decode_request(&body, 16_000).expect("request must decode, not reject");
        assert_eq!(resolved.series.len(), 1);
        assert_eq!(resolved.series[0].samples.len(), 1);
        assert!(resolved.series[0].exemplars.is_empty());
        assert_eq!(resolved.exemplars_dropped, 64);
    }

    /// FINDING 2, other half: a series whose OWN labels blow the budget is
    /// still rejected outright, even when it also carries an exemplar. The
    /// exemplar-drop path must not weaken the series-label control.
    #[test]
    fn series_label_overflow_still_rejects_the_request() {
        let big_value = "v".repeat(200);
        let syms = symbols(&["__name__", &big_value, "trace_id"]);
        // The series labels_refs alone resolve to 200 * (8 + 200) far past the
        // 32_000-byte budget: (name "__name__" = 8 bytes, value = 200 bytes).
        let mut labels_refs = Vec::with_capacity(400);
        for _ in 0..200 {
            labels_refs.push(1u32); // "__name__"
            labels_refs.push(2u32); // 200-byte value
        }
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs,
                samples: vec![ProtoSampleV2 {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                    start_timestamp: 0,
                }],
                histograms: vec![],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: vec![3, 1],
                    value: 3.0,
                    timestamp: 1_700_000_000_500,
                }],
                metadata: None,
            }],
        );
        let body = compress(&req.encode_to_vec());

        let err = decode_request(&body, 2_000)
            .expect_err("series label budget overflow must still reject");
        assert!(matches!(
            err,
            Rw2DecodeError::ResolvedLabelBudgetExceeded { .. }
        ));
    }

    #[test]
    fn corrupt_snappy_body_is_typed_error() {
        let err = decode_request(&[0xff, 0xff, 0xff], 1_000).expect_err("must fail");
        assert!(matches!(err, Rw2DecodeError::Snappy(_)));
    }

    #[test]
    fn valid_snappy_but_invalid_protobuf_is_typed_error() {
        let body = compress(&[0xff, 0x02, 0x00, 0x00]);
        let err = decode_request(&body, 1_000).expect_err("must fail");
        assert!(matches!(err, Rw2DecodeError::Protobuf(_)));
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_snappy_bodies_never_panic(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)) {
            let _ = decode_request(&bytes, 10_000_000);
        }

        #[test]
        fn mutated_valid_request_never_panics(
            metric_name in "[a-zA-Z_][a-zA-Z0-9_]{0,20}",
            value in proptest::prelude::any::<f64>(),
            ts in proptest::prelude::any::<i64>(),
            flip_index in proptest::prelude::any::<usize>(),
            flip_byte in proptest::prelude::any::<u8>(),
        ) {
            let syms = symbols(&["__name__", &metric_name]);
            let req = request(syms, vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![ProtoSampleV2 { value, timestamp: ts, start_timestamp: 0 }],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }]);
            let mut body = compress(&req.encode_to_vec());
            if !body.is_empty() {
                let idx = flip_index % body.len();
                body[idx] ^= flip_byte;
            }
            let _ = decode_request(&body, 10_000_000);
        }

        /// Symbol-table corruption (out-of-range label ref): whatever the
        /// underlying series looks like, an out-of-range labels_refs index
        /// always rejects typed, never panics, and never partially
        /// normalizes.
        #[test]
        fn out_of_range_label_ref_always_typed_error(
            metric_name in "[a-zA-Z_][a-zA-Z0-9_]{0,20}",
            out_of_range_offset in 1u32..1000,
        ) {
            let syms = symbols(&["__name__", &metric_name]);
            let bad_ref = syms.len() as u32 + out_of_range_offset;
            let req = request(syms, vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, bad_ref],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }]);
            let err = decode(&req).expect_err("must reject out-of-range ref");
            let is_out_of_range = matches!(err, Rw2DecodeError::LabelRefOutOfRange { .. });
            proptest::prop_assert!(is_out_of_range);
        }

        /// Symbol-table corruption (odd-length labels_refs): an unpaired
        /// trailing index always rejects typed, regardless of how many
        /// well-formed pairs precede it.
        #[test]
        fn odd_length_label_refs_always_typed_error(
            pair_count in 0usize..5,
        ) {
            let syms = symbols(&["a", "b"]);
            let mut labels_refs: Vec<u32> = (0..pair_count * 2).map(|i| 1 + (i as u32 % 2)).collect();
            labels_refs.push(1);
            let req = request(syms, vec![ProtoTimeSeriesV2 {
                labels_refs,
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }]);
            let err = decode(&req).expect_err("must reject odd length");
            let is_odd_length = matches!(err, Rw2DecodeError::OddLabelRefsLength { .. });
            proptest::prop_assert!(is_odd_length);
        }

        /// Symbol-table corruption (empty-name reference): a label name
        /// index pointing at the conventional empty-string symbol always
        /// rejects typed, whatever value it is paired with.
        #[test]
        fn empty_label_name_always_typed_error(
            value_symbol in "[a-zA-Z_][a-zA-Z0-9_]{0,20}",
        ) {
            let syms = symbols(&[&value_symbol]);
            let req = request(syms, vec![ProtoTimeSeriesV2 {
                labels_refs: vec![0, 1],
                samples: vec![],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }]);
            let err = decode(&req).expect_err("must reject empty label name");
            let is_empty_name = matches!(err, Rw2DecodeError::EmptyLabelName { .. });
            proptest::prop_assert!(is_empty_name);
        }
    }
}
