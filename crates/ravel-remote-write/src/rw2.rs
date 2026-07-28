//! RW 2.0 decode: snappy-decompress and protobuf-decode an
//! `io.prometheus.write.v2.Request`, resolve every symbol-table reference,
//! then converge on the same [`ResolvedRequest`] shape the RW1 decoder
//! produces (ADR-0015, docs/ingest-breadth-plan.md section 2.1).
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

use prost::Message;
use ravel_types::Label;

use crate::proto::write_v2::{Request, TimeSeries as ProtoTimeSeriesV2};
use crate::resolved::{ResolvedRequest, ResolvedSample, ResolvedSeries};
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
}

/// Decompress and decode `body` as an RW2 `Request`.
///
/// `max_decompressed_bytes` bounds the snappy output size, checked before
/// the output buffer is allocated (see [`crate::snappy::decompress`]).
pub fn decode_request(
    body: &[u8],
    max_decompressed_bytes: usize,
) -> Result<ResolvedRequest, Rw2DecodeError> {
    let raw = snappy::decompress(body, max_decompressed_bytes)?;
    let req = Request::decode(raw.as_slice())?;
    resolve(req)
}

fn resolve(req: Request) -> Result<ResolvedRequest, Rw2DecodeError> {
    let symbols = req.symbols;
    let mut series = Vec::with_capacity(req.timeseries.len());
    let mut metadata_count = 0usize;
    let mut created_timestamps_count = 0usize;
    for (series_index, ts) in req.timeseries.into_iter().enumerate() {
        series.push(resolve_series(
            series_index,
            ts,
            &symbols,
            &mut metadata_count,
            &mut created_timestamps_count,
        )?);
    }
    Ok(ResolvedRequest {
        series,
        metadata_count,
        created_timestamps_count,
    })
}

fn resolve_series(
    series_index: usize,
    ts: ProtoTimeSeriesV2,
    symbols: &[String],
    metadata_count: &mut usize,
    created_timestamps_count: &mut usize,
) -> Result<ResolvedSeries, Rw2DecodeError> {
    let labels = resolve_label_refs(series_index, &ts.labels_refs, symbols)?;

    for (exemplar_index, exemplar) in ts.exemplars.iter().enumerate() {
        validate_exemplar_label_refs(
            series_index,
            exemplar_index,
            &exemplar.labels_refs,
            symbols.len(),
        )?;
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
        *metadata_count += 1;
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
    for h in &ts.histograms {
        if h.start_timestamp != 0 {
            *created_timestamps_count += 1;
        }
    }

    Ok(ResolvedSeries {
        labels,
        samples,
        histogram_count: ts.histograms.len(),
        exemplar_count: ts.exemplars.len(),
    })
}

/// Resolve one series' `labels_refs` into owned [`Label`]s: length must be
/// even (name, value index pairs), every index must be in range of
/// `symbols`, and a name resolving to the empty string is rejected (see
/// module docs).
fn resolve_label_refs(
    series_index: usize,
    labels_refs: &[u32],
    symbols: &[String],
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
        labels.push(Label {
            name: name.clone(),
            value: value.clone(),
        });
    }
    Ok(labels)
}

/// Validate one exemplar's `labels_refs` without materializing the labels:
/// exemplars are accepted-and-dropped (ADR-0015), so only the reference
/// shape needs checking, never the resolved strings.
fn validate_exemplar_label_refs(
    series_index: usize,
    exemplar_index: usize,
    labels_refs: &[u32],
    symbols_len: usize,
) -> Result<(), Rw2DecodeError> {
    if !labels_refs.len().is_multiple_of(2) {
        return Err(Rw2DecodeError::OddExemplarLabelRefsLength {
            series_index,
            exemplar_index,
            len: labels_refs.len(),
        });
    }
    for (position, &symbol_ref) in labels_refs.iter().enumerate() {
        if symbol_ref as usize >= symbols_len {
            return Err(Rw2DecodeError::ExemplarLabelRefOutOfRange {
                series_index,
                exemplar_index,
                position,
                symbol_ref,
                symbols_len,
            });
        }
    }
    Ok(())
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
    fn tallies_histograms_and_exemplars_without_materializing_them() {
        let syms = symbols(&["__name__", "latency"]);
        let req = request(
            syms,
            vec![ProtoTimeSeriesV2 {
                labels_refs: vec![1, 2],
                samples: vec![],
                histograms: vec![Default::default(), Default::default()],
                exemplars: vec![ProtoExemplarV2 {
                    labels_refs: vec![],
                    value: 1.0,
                    timestamp: 1_000,
                }],
                metadata: None,
            }],
        );

        let resolved = decode(&req).expect("decode");
        assert_eq!(resolved.series[0].histogram_count, 2);
        assert_eq!(resolved.series[0].exemplar_count, 1);
    }

    #[test]
    fn metadata_help_and_unit_refs_are_a_per_series_tally() {
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
        assert_eq!(resolved.metadata_count, 1);
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
        assert_eq!(resolved.metadata_count, 0);
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
