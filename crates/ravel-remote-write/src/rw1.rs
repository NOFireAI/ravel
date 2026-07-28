//! RW 1.0 decode: snappy-decompress and protobuf-decode a
//! `prometheus.WriteRequest`, then resolve it into the version-blind
//! [`ResolvedRequest`] shape (ADR-0015, docs/ingest-breadth-plan.md section
//! 2.1).
//!
//! This module never panics on malformed input: every decode failure is a
//! typed, non-retryable [`Rw1DecodeError`].

use prost::Message;
use ravel_types::Label;

use crate::proto::prometheus::{TimeSeries as ProtoTimeSeries, WriteRequest};
use crate::resolved::{ResolvedRequest, ResolvedSample, ResolvedSeries};
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
        histogram_count: series.histograms.len(),
        exemplar_count: series.exemplars.len(),
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
        assert_eq!(series.histogram_count, 0);
        assert_eq!(series.exemplar_count, 0);
    }

    #[test]
    fn tallies_histograms_and_exemplars_without_materializing_them() {
        use crate::proto::prometheus::{Exemplar as ProtoExemplar, Histogram as ProtoHistogram};
        let req = write_request(vec![ProtoTimeSeries {
            labels: vec![proto_label("__name__", "latency")],
            samples: vec![],
            exemplars: vec![ProtoExemplar {
                labels: vec![],
                value: 1.0,
                timestamp: 1_000,
            }],
            histograms: vec![ProtoHistogram::default(), ProtoHistogram::default()],
        }]);
        let body = compress(&req.encode_to_vec());

        let resolved = decode_write_request(&body, 1_000_000).expect("decode");
        assert_eq!(resolved.series[0].histogram_count, 2);
        assert_eq!(resolved.series[0].exemplar_count, 1);
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
