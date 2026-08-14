//! Encodes a generated [`Dataset`](crate::generator::Dataset) as a
//! Remote Write 1.0 payload: `prometheus.WriteRequest` protobuf, snappy
//! block-compressed.
//!
//! This is the inverse of `ravel-remote-write`'s decode path
//! (`crates/ravel-remote-write/src/rw1.rs`), kept crate-local (the
//! remote-write proto is vendored crate-locally and is not a Ravel persistent
//! format) since it only ever produces fixtures for the pinned Prometheus
//! instance, never decodes production traffic.

use crate::generator::{Dataset, GeneratedHistogramSeries};
use crate::proto::prometheus::{
    BucketSpan, Histogram, Label as ProtoLabel, Sample as ProtoSample, TimeSeries, WriteRequest,
    histogram,
};
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("snappy compression failed: {0}")]
    Snappy(snap::Error),
}

/// Builds the wire-format RW1 body: protobuf-encode, then snappy
/// block-compress.
pub fn encode_write_request(dataset: &Dataset) -> Result<Vec<u8>, EncodeError> {
    let req = to_write_request(dataset);
    let encoded = req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .map_err(EncodeError::Snappy)
}

fn to_write_request(dataset: &Dataset) -> WriteRequest {
    let mut timeseries: Vec<TimeSeries> = dataset.series.iter().map(to_timeseries).collect();
    timeseries.extend(dataset.histogram_series.iter().map(to_histogram_timeseries));
    WriteRequest {
        timeseries,
        metadata: Vec::new(),
    }
}

/// Encode a native-histogram series as a `TimeSeries` carrying `Histogram`
/// messages (RW1 native-histogram support), the counterpart of
/// [`to_timeseries`] for the native-histogram corpus. Integer histograms are delta-encoded
/// per the remote-write contract (`positive_deltas`, not absolute counts).
fn to_histogram_timeseries(series: &GeneratedHistogramSeries) -> TimeSeries {
    TimeSeries {
        labels: series
            .labels
            .iter()
            .map(|(name, value)| ProtoLabel {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
        samples: Vec::new(),
        exemplars: Vec::new(),
        histograms: series.points.iter().map(to_proto_histogram).collect(),
    }
}

fn to_proto_histogram(point: &crate::generator::GeneratedHistogramPoint) -> Histogram {
    Histogram {
        count: Some(histogram::Count::CountInt(point.count)),
        sum: point.sum,
        schema: point.schema,
        zero_threshold: point.zero_threshold,
        zero_count: Some(histogram::ZeroCount::ZeroCountInt(point.zero_count)),
        negative_spans: Vec::new(),
        negative_deltas: Vec::new(),
        negative_counts: Vec::new(),
        positive_spans: point
            .positive_spans
            .iter()
            .map(|(offset, length)| BucketSpan {
                offset: *offset,
                length: *length,
            })
            .collect(),
        positive_deltas: absolute_to_deltas(&point.positive_buckets),
        positive_counts: Vec::new(),
        reset_hint: point.reset_hint,
        timestamp: point.ts_ms,
        custom_values: Vec::new(),
    }
}

/// Convert absolute bucket counts to the delta encoding remote write uses for
/// integer histograms: the first entry is the absolute count, each subsequent
/// entry is the difference from the previous bucket.
fn absolute_to_deltas(absolute: &[u64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(absolute.len());
    let mut prev = 0i64;
    for &c in absolute {
        let c = c as i64;
        out.push(c - prev);
        prev = c;
    }
    out
}

fn to_timeseries(series: &crate::generator::GeneratedSeries) -> TimeSeries {
    TimeSeries {
        labels: series
            .labels
            .iter()
            .map(|(name, value)| ProtoLabel {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
        samples: series
            .samples
            .iter()
            .map(|(ts_ms, value)| ProtoSample {
                value: *value,
                timestamp: *ts_ms,
            })
            .collect(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::generator::{DatasetConfig, generate};

    #[test]
    fn encodes_and_decodes_back_to_the_same_samples() {
        let dataset = generate(&DatasetConfig::default());
        let body = encode_write_request(&dataset).expect("encode");

        let decompressed = {
            let len = snap::raw::decompress_len(&body).expect("header");
            let mut out = vec![0u8; len];
            let written = snap::raw::Decoder::new()
                .decompress(&body, &mut out)
                .expect("decompress");
            out.truncate(written);
            out
        };
        let decoded = WriteRequest::decode(decompressed.as_slice()).expect("decode protobuf");

        assert_eq!(
            decoded.timeseries.len(),
            dataset.series.len() + dataset.histogram_series.len()
        );
        let first_wire = &decoded.timeseries[0];
        let first_series = &dataset.series[0];
        assert_eq!(first_wire.samples.len(), first_series.samples.len());
        for (wire_sample, (ts_ms, value)) in
            first_wire.samples.iter().zip(first_series.samples.iter())
        {
            assert_eq!(wire_sample.timestamp, *ts_ms);
            assert_eq!(wire_sample.value.to_bits(), value.to_bits());
        }
    }

    #[test]
    fn native_histograms_encode_and_decode_back() {
        let dataset = generate(&DatasetConfig::default());
        assert!(
            !dataset.histogram_series.is_empty(),
            "generator must produce native-histogram series"
        );
        let body = encode_write_request(&dataset).expect("encode");
        let decompressed = {
            let len = snap::raw::decompress_len(&body).expect("header");
            let mut out = vec![0u8; len];
            let written = snap::raw::Decoder::new()
                .decompress(&body, &mut out)
                .expect("decompress");
            out.truncate(written);
            out
        };
        let decoded = WriteRequest::decode(decompressed.as_slice()).expect("decode protobuf");

        // The histogram series sit after the float series in wire order.
        let first_hist_series = &dataset.histogram_series[0];
        let wire = decoded
            .timeseries
            .iter()
            .find(|ts| {
                ts.labels
                    .iter()
                    .any(|l| l.name == "__name__" && l.value == first_hist_series.metric_name())
            })
            .expect("histogram series present on the wire");
        assert!(
            wire.samples.is_empty(),
            "histogram series carries no floats"
        );
        assert_eq!(wire.histograms.len(), first_hist_series.points.len());

        let first_point = &first_hist_series.points[0];
        let wire_h = &wire.histograms[0];
        assert_eq!(wire_h.timestamp, first_point.ts_ms);
        assert_eq!(wire_h.schema, first_point.schema);
        assert_eq!(
            wire_h.count,
            Some(super::histogram::Count::CountInt(first_point.count))
        );
        // The absolute counts round-trip through the delta encoding.
        let recovered = deltas_to_absolute(&wire_h.positive_deltas);
        assert_eq!(recovered, first_point.positive_buckets);
    }

    fn deltas_to_absolute(deltas: &[i64]) -> Vec<u64> {
        let mut out = Vec::with_capacity(deltas.len());
        let mut acc = 0i64;
        for &d in deltas {
            acc += d;
            out.push(acc as u64);
        }
        out
    }

    #[test]
    fn golden_bytes_for_a_single_fixed_sample() {
        use crate::generator::GeneratedSeries;
        let dataset = Dataset {
            series: vec![GeneratedSeries {
                labels: vec![
                    ("__name__".to_string(), "diff_golden".to_string()),
                    ("job".to_string(), "test".to_string()),
                ],
                samples: vec![(1_700_000_000_000, 42.5)],
            }],
            histogram_series: Vec::new(),
        };
        let req = to_write_request(&dataset);
        let encoded = req.encode_to_vec();
        // Golden bytes captured from a known-good run of this same
        // encoder: one series, two labels (`__name__`, `job`), one sample.
        // Pins the exact wire bytes so any change to field encoding,
        // ordering, or proto shape is caught here rather than only
        // downstream in a live Prometheus roundtrip.
        let expected: Vec<u8> = vec![
            10, 56, // timeseries[0], len 56
            10, 23, // labels[0] (__name__), len 23
            10, 8, b'_', b'_', b'n', b'a', b'm', b'e', b'_', b'_', // name
            18, 11, b'd', b'i', b'f', b'f', b'_', b'g', b'o', b'l', b'd', b'e', b'n', // value
            10, 11, // labels[1] (job), len 11
            10, 3, b'j', b'o', b'b', // name
            18, 4, b't', b'e', b's', b't', // value
            18, 16, // samples[0], len 16
            9, 0, 0, 0, 0, 0, 64, 69, 64, // value: f64 42.5 (little-endian bits)
            16, 128, 208, 149, 255, 188, 49, // timestamp varint: 1_700_000_000_000
        ];
        assert_eq!(encoded, expected);
    }
}
