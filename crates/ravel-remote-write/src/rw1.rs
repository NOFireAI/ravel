//! RW 1.0 decode: snappy-decompress and protobuf-decode a
//! `prometheus.WriteRequest`, then resolve it into the version-blind
//! [`ResolvedRequest`] shape (ADR-0015, docs/ingest-breadth-plan.md section
//! 2.1).
//!
//! This module never panics on malformed input: every decode failure is a
//! typed, non-retryable [`Rw1DecodeError`].

use prost::Message;
use ravel_types::Label;

use crate::proto::prometheus::histogram::{Count, ZeroCount};
use crate::proto::prometheus::{
    BucketSpan, Histogram as ProtoHistogram, TimeSeries as ProtoTimeSeries, WriteRequest,
};
use crate::resolved::{
    ResolvedCount, ResolvedExemplar, ResolvedHistogram, ResolvedRequest, ResolvedSample,
    ResolvedSeries, ResolvedSpan,
};
use crate::snappy::{self, SnappyError};

#[derive(Debug, thiserror::Error)]
pub enum Rw1DecodeError {
    #[error("snappy decompression failed: {0}")]
    Snappy(#[from] SnappyError),
    #[error("malformed WriteRequest protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
}

/// Decompress and decode `body` as an RW1 `WriteRequest`.
///
/// `max_decompressed_bytes` bounds the snappy output size, checked before
/// the output buffer is allocated (see [`crate::snappy::decompress`]).
pub fn decode_write_request(
    body: &[u8],
    max_decompressed_bytes: usize,
) -> Result<ResolvedRequest, Rw1DecodeError> {
    let raw = snappy::decompress(body, max_decompressed_bytes)?;
    let req = WriteRequest::decode(raw.as_slice())?;
    Ok(resolve(req))
}

fn resolve(req: WriteRequest) -> ResolvedRequest {
    ResolvedRequest {
        series: req.timeseries.into_iter().map(resolve_series).collect(),
        metadata_count: req.metadata.len(),
        // prometheus.Sample carries no start_timestamp field on the wire.
        created_timestamps_count: 0,
    }
}

fn resolve_series(series: ProtoTimeSeries) -> ResolvedSeries {
    ResolvedSeries {
        labels: series
            .labels
            .into_iter()
            .map(|l| Label {
                name: l.name,
                value: l.value,
            })
            .collect(),
        samples: series
            .samples
            .into_iter()
            .map(|s| ResolvedSample {
                ts_ms: s.timestamp,
                value: s.value,
            })
            .collect(),
        histograms: series
            .histograms
            .into_iter()
            .map(resolve_histogram)
            .collect(),
        exemplars: series
            .exemplars
            .into_iter()
            .map(|e| ResolvedExemplar {
                ts_ms: e.timestamp,
                value: e.value,
                labels: e
                    .labels
                    .into_iter()
                    .map(|l| Label {
                        name: l.name,
                        value: l.value,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Field-for-field mapping of one wire `prometheus.Histogram` into the
/// version-blind [`ResolvedHistogram`]. Nothing is validated or reshaped
/// here (bucket deltas stay deltas, an unset `count` oneof stays absent):
/// [`crate::normalize`] owns every admission decision, so RW1 and RW2 cannot
/// drift on what they accept. Its RW2 twin is `rw2::resolve_histogram`.
fn resolve_histogram(h: ProtoHistogram) -> ResolvedHistogram {
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::proto::prometheus::{Label as ProtoLabel, MetricMetadata, Sample as ProtoSample};

    fn compress(bytes: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new()
            .compress_vec(bytes)
            .expect("compress")
    }

    fn write_request(series: Vec<ProtoTimeSeries>) -> WriteRequest {
        WriteRequest {
            timeseries: series,
            metadata: vec![],
        }
    }

    fn proto_label(name: &str, value: &str) -> ProtoLabel {
        ProtoLabel {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn decodes_labels_and_samples() {
        let req = write_request(vec![ProtoTimeSeries {
            labels: vec![proto_label("__name__", "up"), proto_label("job", "svc")],
            samples: vec![ProtoSample {
                value: 1.0,
                timestamp: 1_700_000_000_000,
            }],
            exemplars: vec![],
            histograms: vec![],
        }]);
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        assert_eq!(resolved.series.len(), 1);
        let series = &resolved.series[0];
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.samples.len(), 1);
        assert_eq!(series.samples[0].ts_ms, 1_700_000_000_000);
        assert_eq!(series.samples[0].value, 1.0);
        assert!(series.histograms.is_empty());
        assert!(series.exemplars.is_empty());
    }

    /// Native histograms are materialized (durable storage since RSEG v5), and
    /// so are exemplars, body and all, since ADR-0047 gave them an RSEG
    /// section: the wire labels arrive unsplit and undecoded, for
    /// [`crate::normalize`] to map.
    #[test]
    fn materializes_histograms_and_exemplars() {
        use crate::proto::prometheus::Exemplar as ProtoExemplar;
        let req = write_request(vec![ProtoTimeSeries {
            labels: vec![proto_label("__name__", "latency")],
            samples: vec![],
            exemplars: vec![ProtoExemplar {
                labels: vec![proto_label("trace_id", "0123456789abcdef0123456789abcdef")],
                value: 1.0,
                timestamp: 1_000,
            }],
            histograms: vec![ProtoHistogram::default(), ProtoHistogram::default()],
        }]);
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        assert_eq!(resolved.series[0].histograms.len(), 2);
        assert_eq!(
            resolved.series[0].exemplars,
            vec![ResolvedExemplar {
                ts_ms: 1_000,
                value: 1.0,
                labels: vec![Label {
                    name: "trace_id".to_string(),
                    value: "0123456789abcdef0123456789abcdef".to_string(),
                }],
            }]
        );
    }

    /// Every histogram field reaches [`ResolvedHistogram`] unreshaped: the
    /// oneofs keep their kind, integer bucket sides stay deltas, and nothing
    /// is validated at this layer.
    #[test]
    fn resolves_every_histogram_field_verbatim() {
        use crate::proto::prometheus::histogram::ResetHint;
        let req = write_request(vec![ProtoTimeSeries {
            labels: vec![proto_label("__name__", "latency")],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![ProtoHistogram {
                count: Some(Count::CountInt(14)),
                zero_count: Some(ZeroCount::ZeroCountInt(1)),
                sum: 42.5,
                schema: 2,
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
                reset_hint: ResetHint::Yes as i32,
                timestamp: 1_700_000_000_000,
                custom_values: vec![],
            }],
        }]);
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        let h = &resolved.series[0].histograms[0];
        assert_eq!(h.ts_ms, 1_700_000_000_000);
        assert_eq!(h.schema, 2);
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
        assert!(h.negative_spans.is_empty());
        assert!(h.positive_counts.is_empty());
        assert_eq!(h.reset_hint, ResetHint::Yes as i32);
        assert!(h.custom_values.is_empty());
    }

    /// A float histogram's oneofs resolve to the float variant, which is
    /// what tells the normalizer to read `positive_counts` instead of
    /// `positive_deltas`.
    #[test]
    fn resolves_float_histogram_oneofs_as_float() {
        let req = write_request(vec![ProtoTimeSeries {
            labels: vec![proto_label("__name__", "latency")],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![ProtoHistogram {
                count: Some(Count::CountFloat(14.5)),
                zero_count: Some(ZeroCount::ZeroCountFloat(1.5)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_counts: vec![3.0, 4.0],
                ..ProtoHistogram::default()
            }],
        }]);
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        let h = &resolved.series[0].histograms[0];
        assert_eq!(h.count, Some(ResolvedCount::Float(14.5)));
        assert_eq!(h.zero_count, Some(ResolvedCount::Float(1.5)));
        assert_eq!(h.positive_counts, vec![3.0, 4.0]);
        assert!(h.positive_deltas.is_empty());
    }

    #[test]
    fn metadata_is_a_request_level_tally() {
        use crate::proto::prometheus::metric_metadata::MetricType;
        let mut req = write_request(vec![]);
        req.metadata = vec![
            MetricMetadata {
                r#type: MetricType::Counter as i32,
                metric_family_name: "a".to_string(),
                help: String::new(),
                unit: String::new(),
            },
            MetricMetadata {
                r#type: MetricType::Gauge as i32,
                metric_family_name: "b".to_string(),
                help: String::new(),
                unit: String::new(),
            },
        ];
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        assert_eq!(resolved.metadata_count, 2);
    }

    #[test]
    fn empty_request_decodes_to_empty_resolved() {
        let req = write_request(vec![]);
        let body = compress(&req.encode_to_vec());
        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        assert!(resolved.series.is_empty());
        assert_eq!(resolved.metadata_count, 0);
    }

    #[test]
    fn corrupt_snappy_body_is_typed_error() {
        let err = decode_write_request(&[0xff, 0xff, 0xff], 1_000).expect_err("must fail");
        assert!(matches!(err, Rw1DecodeError::Snappy(_)));
    }

    #[test]
    fn valid_snappy_but_invalid_protobuf_is_typed_error() {
        let body = compress(&[0xff, 0x02, 0x00, 0x00]);
        let err = decode_write_request(&body, 1_000).expect_err("must fail");
        assert!(matches!(err, Rw1DecodeError::Protobuf(_)));
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_snappy_bodies_never_panic(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)) {
            let _ = decode_write_request(&bytes, 10_000_000);
        }

        #[test]
        fn mutated_valid_request_never_panics(
            metric_name in "[a-zA-Z_][a-zA-Z0-9_]{0,20}",
            value in proptest::prelude::any::<f64>(),
            ts in proptest::prelude::any::<i64>(),
            flip_index in proptest::prelude::any::<usize>(),
            flip_byte in proptest::prelude::any::<u8>(),
        ) {
            let req = write_request(vec![ProtoTimeSeries {
                labels: vec![proto_label("__name__", &metric_name)],
                samples: vec![ProtoSample { value, timestamp: ts }],
                exemplars: vec![],
                histograms: vec![],
            }]);
            let mut body = compress(&req.encode_to_vec());
            if !body.is_empty() {
                let idx = flip_index % body.len();
                body[idx] ^= flip_byte;
            }
            let _ = decode_write_request(&body, 10_000_000);
        }
    }
}
