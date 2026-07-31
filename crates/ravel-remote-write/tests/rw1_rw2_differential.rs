//! RW1-vs-RW2 differential gate (docs/ingest-breadth-plan.md section 3,
//! ADR-0015): the same logical payload, encoded once as an RW1
//! `WriteRequest` (inline string labels) and once as an RW2 `Request`
//! (symbol-table references), must decode and normalize to identical
//! `RwNormalizeOutput`s: same admitted points, same rejection classes.
//! Neither decoder is treated as ground truth here; agreement between them
//! is what this test enforces.
#![allow(clippy::expect_used)]

use std::collections::HashMap;

use prost::Message;
use ravel_otlp::IngestLimits;
use ravel_remote_write::proto::prometheus::{
    Label as ProtoLabelV1, Sample as ProtoSampleV1, TimeSeries as ProtoTimeSeriesV1, WriteRequest,
};
use ravel_remote_write::proto::write_v2::{
    Request as RequestV2, Sample as ProtoSampleV2, TimeSeries as ProtoTimeSeriesV2,
};
use ravel_remote_write::{
    RwNormalizeOutput, decode_request, decode_write_request, normalize_resolved,
};
use ravel_types::TenantId;

/// A protocol-agnostic description of one series: label pairs in wire
/// order, plus (timestamp_ms, value) samples. Both encoders below build
/// their wire message directly from this, so any divergence in the output
/// can only come from the decoders themselves.
type LogicalSeries = (Vec<(String, String)>, Vec<(i64, f64)>);

fn compress(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(bytes)
        .expect("compress fixture")
}

fn logical_series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> LogicalSeries {
    (
        labels
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect(),
        samples.to_vec(),
    )
}

fn build_rw1(series: &[LogicalSeries]) -> Vec<u8> {
    let req = WriteRequest {
        timeseries: series
            .iter()
            .map(|(labels, samples)| ProtoTimeSeriesV1 {
                labels: labels
                    .iter()
                    .map(|(n, v)| ProtoLabelV1 {
                        name: n.clone(),
                        value: v.clone(),
                    })
                    .collect(),
                samples: samples
                    .iter()
                    .map(|(ts, v)| ProtoSampleV1 {
                        value: *v,
                        timestamp: *ts,
                    })
                    .collect(),
                exemplars: vec![],
                histograms: vec![],
            })
            .collect(),
        metadata: vec![],
    };
    compress(&req.encode_to_vec())
}

/// Interns strings into an RW2 symbol table, keeping `symbols[0]` the
/// conventional empty string.
struct Interner {
    symbols: Vec<String>,
    index: HashMap<String, u32>,
}

impl Interner {
    fn new() -> Self {
        let mut index = HashMap::new();
        index.insert(String::new(), 0);
        Interner {
            symbols: vec![String::new()],
            index,
        }
    }

    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = self.symbols.len() as u32;
        self.symbols.push(s.to_string());
        self.index.insert(s.to_string(), i);
        i
    }
}

fn build_rw2(series: &[LogicalSeries]) -> Vec<u8> {
    let mut interner = Interner::new();
    let timeseries = series
        .iter()
        .map(|(labels, samples)| {
            let mut labels_refs = Vec::with_capacity(labels.len() * 2);
            for (n, v) in labels {
                labels_refs.push(interner.intern(n));
                labels_refs.push(interner.intern(v));
            }
            ProtoTimeSeriesV2 {
                labels_refs,
                samples: samples
                    .iter()
                    .map(|(ts, v)| ProtoSampleV2 {
                        value: *v,
                        timestamp: *ts,
                        start_timestamp: 0,
                    })
                    .collect(),
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }
        })
        .collect();
    let req = RequestV2 {
        symbols: interner.symbols,
        timeseries,
    };
    compress(&req.encode_to_vec())
}

fn diff_normalize(series: &[LogicalSeries]) -> (RwNormalizeOutput, RwNormalizeOutput) {
    let tenant = TenantId::new("t-differential");
    let limits = IngestLimits::default();
    let ingest_ts_ns = 1_700_000_100_000_000_000;

    let resolved1 =
        decode_write_request(&build_rw1(series), 10_000_000).expect("rw1 decode fixture");
    let resolved2 = decode_request(&build_rw2(series), 10_000_000).expect("rw2 decode fixture");

    let out1 = normalize_resolved(&tenant, resolved1, &limits, ingest_ts_ns);
    let out2 = normalize_resolved(&tenant, resolved2, &limits, ingest_ts_ns);
    (out1, out2)
}

#[test]
fn identical_logical_payload_normalizes_identically() {
    let series = vec![
        logical_series(
            &[
                ("__name__", "process_cpu_seconds_total"),
                ("job", "svc"),
                ("instance", "10.0.0.1:9090"),
            ],
            &[(1_700_000_000_000, 12.5), (1_700_000_015_000, 13.1)],
        ),
        logical_series(
            &[
                ("__name__", "up"),
                ("job", "svc"),
                ("instance", "10.0.0.1:9090"),
            ],
            &[(1_700_000_000_000, 1.0)],
        ),
    ];

    let (out1, out2) = diff_normalize(&series);
    assert!(out1.rejected.is_empty(), "{:?}", out1.rejected);
    assert_eq!(out1, out2);
}

#[test]
fn missing_name_label_rejects_identically_across_decoders() {
    let series = vec![logical_series(&[("job", "svc")], &[(1_000, 1.0)])];
    let (out1, out2) = diff_normalize(&series);
    assert!(!out1.rejected.is_empty());
    assert_eq!(out1, out2);
}

#[test]
fn duplicate_label_name_rejects_identically_across_decoders() {
    let series = vec![logical_series(
        &[("__name__", "up"), ("job", "a"), ("job", "b")],
        &[(1_000, 1.0)],
    )];
    let (out1, out2) = diff_normalize(&series);
    assert!(!out1.rejected.is_empty());
    assert_eq!(out1, out2);
}

#[test]
fn empty_value_label_dropped_identically_across_decoders() {
    let series = vec![logical_series(
        &[("__name__", "up"), ("optional", "")],
        &[(1_700_000_000_000, 1.0)],
    )];
    let (out1, out2) = diff_normalize(&series);
    assert!(out1.rejected.is_empty(), "{:?}", out1.rejected);
    assert_eq!(out1, out2);
}

#[test]
fn empty_label_name_rejects_identically_across_decoders() {
    // The same logical payload carrying a label with an empty name must be
    // rejected by both wire formats (ADR-0015 protocol parity, ADR-0031,
    // issue #204). RW2 rejects it at decode (`Rw2DecodeError::EmptyLabelName`,
    // because an empty label name resolves through `symbols[0]`); RW1 copies
    // the empty name verbatim and the shared normalizer rejects it. Different
    // layers, but the series is admitted by neither, so the empty-named label
    // never reaches `SeriesId::compute` from either path.
    let series = vec![logical_series(
        &[("__name__", "up"), ("", "surprise")],
        &[(1_700_000_000_000, 1.0)],
    )];
    let tenant = TenantId::new("t-differential");
    let limits = IngestLimits::default();
    let ingest_ts_ns = 1_700_000_100_000_000_000;

    // RW1: decode succeeds, normalization rejects the whole series.
    let resolved1 = decode_write_request(&build_rw1(&series), 10_000_000).expect("rw1 decode");
    let out1 = normalize_resolved(&tenant, resolved1, &limits, ingest_ts_ns);
    assert!(out1.points.is_empty(), "{:?}", out1.points);
    assert!(!out1.rejected.is_empty());

    // RW2: rejected earlier, at decode, before normalization is reached.
    let rw2 = decode_request(&build_rw2(&series), 10_000_000);
    assert!(
        rw2.is_err(),
        "rw2 must reject an empty label name at decode"
    );
}

#[test]
fn zero_timestamp_rejects_identically_across_decoders() {
    let series = vec![logical_series(&[("__name__", "up")], &[(0, 1.0)])];
    let (out1, out2) = diff_normalize(&series);
    assert!(!out1.rejected.is_empty());
    assert_eq!(out1, out2);
}

proptest::proptest! {
    #[test]
    fn arbitrary_well_formed_payloads_normalize_identically(
        metric_name in "[a-zA-Z_][a-zA-Z0-9_]{0,20}",
        job in "[a-zA-Z0-9_-]{1,10}",
        sample_values in proptest::collection::vec(
            (proptest::prelude::any::<i64>(), proptest::prelude::any::<f64>()),
            1..5,
        ),
    ) {
        let series = vec![logical_series(
            &[("__name__", &metric_name), ("job", &job)],
            &sample_values,
        )];
        let (out1, out2) = diff_normalize(&series);
        proptest::prop_assert_eq!(out1, out2);
    }
}
