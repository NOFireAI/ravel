//! Golden-bytes regressions for the RSEG v3 writer (ADR-0017,
//! docs/rseg-v3-plan.md phase C3): the writer's output for each fixed
//! representative input must stay byte-for-byte identical across internal
//! refactors of the v3 encode path. RSEG v3 is a frozen persistent contract
//! (docs/segment-format.md "RSEG v3 amendment"); these tests are the
//! tripwire for an accidental format change.
//!
//! Seven cases per docs/rseg-v3-plan.md ticket C3's test list: integer
//! histogram, float histogram, custom boundaries, sparse span shapes, dense
//! span shapes, mixed scalar+histogram series in one segment, 1-sample
//! histogram series. To regenerate a fixture after a deliberate, versioned
//! format change (never for an internal refactor), run:
//!   cargo test -p ravel-segment --test golden_bytes_v3 -- --ignored --nocapture
#![allow(clippy::expect_used)]

use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
    SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

fn labels(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::new(
        pairs
            .iter()
            .map(|(n, v)| Label {
                name: (*n).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
    )
    .expect("valid labels")
}

fn fixed_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0xEF; 16],
        shard: 7,
        writer_id: "golden-v3-writer".to_string(),
        writer_epoch: 3,
        writer_seq: 90,
    }
}

fn fixed_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: -5_000,
        max_ingest_ts_ns: 20_000,
    }
}

fn span(offset: i32, length: u32) -> HistogramSpan {
    HistogramSpan { offset, length }
}

/// Two integer-count series: one with `sum`, `Yes` reset hint, and mixed
/// positive/negative spans; the other with no `sum`, `Gauge` reset hint,
/// and positive-side buckets only.
fn integer_histogram_series() -> Vec<SeriesInputV3> {
    vec![
        SeriesInputV3 {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "request_latency_seconds")]),
            values: SeriesValues::Histogram(vec![
                HistogramSample {
                    ts_ns: 0,
                    value: HistogramValue {
                        scale: 3,
                        zero_threshold: 1e-9,
                        sum: Some(123.5),
                        custom_values: None,
                        positive_spans: vec![span(0, 3), span(2, 2)],
                        negative_spans: vec![span(-1, 1)],
                        counts: HistogramCounts::Int {
                            zero_count: 2,
                            count: 19,
                            positive: vec![1, 4, 2, 3, 1],
                            negative: vec![5],
                        },
                        reset_hint: ResetHint::Yes,
                    },
                },
                HistogramSample {
                    ts_ns: 10_000,
                    value: HistogramValue {
                        scale: 3,
                        zero_threshold: 1e-9,
                        sum: Some(200.25),
                        custom_values: None,
                        positive_spans: vec![span(0, 4)],
                        negative_spans: vec![span(-1, 1)],
                        counts: HistogramCounts::Int {
                            zero_count: 1,
                            count: 15,
                            positive: vec![1, 2, 3, 2],
                            negative: vec![6],
                        },
                        reset_hint: ResetHint::No,
                    },
                },
            ]),
        },
        SeriesInputV3 {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "queue_depth")]),
            values: SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: 5_000,
                value: HistogramValue {
                    scale: 0,
                    zero_threshold: 0.0,
                    sum: None,
                    custom_values: None,
                    positive_spans: vec![span(0, 6)],
                    negative_spans: vec![],
                    counts: HistogramCounts::Int {
                        zero_count: 0,
                        count: 21,
                        positive: vec![1, 2, 3, 4, 5, 6],
                        negative: vec![],
                    },
                    reset_hint: ResetHint::Gauge,
                },
            }]),
        },
    ]
}

/// Same shape as `integer_histogram_series` but with float-kind counts
/// (`count_kind` flag bit set, `f64` bucket vectors instead of varints).
fn float_histogram_series() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x03; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "cpu_utilization_ratio")]),
        values: SeriesValues::Histogram(vec![
            HistogramSample {
                ts_ns: 0,
                value: HistogramValue {
                    scale: 2,
                    zero_threshold: 1e-6,
                    sum: Some(88.75),
                    custom_values: None,
                    positive_spans: vec![span(0, 3)],
                    negative_spans: vec![span(0, 2)],
                    counts: HistogramCounts::Float {
                        zero_count: 0.5,
                        count: 12.25,
                        positive: vec![1.5, 2.25, 3.0],
                        negative: vec![f64::INFINITY, -0.0],
                    },
                    reset_hint: ResetHint::Unknown,
                },
            },
            HistogramSample {
                ts_ns: 15_000,
                value: HistogramValue {
                    scale: 2,
                    zero_threshold: 1e-6,
                    sum: None,
                    custom_values: None,
                    positive_spans: vec![],
                    negative_spans: vec![],
                    counts: HistogramCounts::Float {
                        zero_count: f64::NAN,
                        count: 0.0,
                        positive: vec![],
                        negative: vec![],
                    },
                    reset_hint: ResetHint::No,
                },
            },
        ]),
    }]
}

/// `scale == -53`: custom bucket boundaries instead of a base-2 exponential
/// scale. Boundaries must be present and strictly ascending.
fn custom_boundaries_series() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x04; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "response_size_bytes")]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 0,
            value: HistogramValue {
                scale: -53,
                zero_threshold: 0.0,
                sum: Some(4_096.0),
                custom_values: Some(vec![0.0, 64.0, 256.0, 1_024.0, 4_096.0, 16_384.0]),
                positive_spans: vec![span(0, 5)],
                negative_spans: vec![],
                counts: HistogramCounts::Int {
                    zero_count: 0,
                    count: 30,
                    positive: vec![10, 8, 6, 4, 2],
                    negative: vec![],
                },
                reset_hint: ResetHint::Yes,
            },
        }]),
    }]
}

/// Many single-bucket spans separated by wide gaps -- the sparse shape
/// native to Prometheus-style exemplar histograms.
fn sparse_spans_series() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x05; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "sparse_metric")]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 0,
            value: HistogramValue {
                scale: 5,
                zero_threshold: 0.0,
                sum: Some(9.0),
                custom_values: None,
                positive_spans: vec![span(100, 1), span(50, 1), span(75, 1), span(200, 1)],
                negative_spans: vec![span(-30, 1), span(-90, 1)],
                counts: HistogramCounts::Int {
                    zero_count: 0,
                    count: 6,
                    positive: vec![1, 1, 1, 1],
                    negative: vec![1, 1],
                },
                reset_hint: ResetHint::Unknown,
            },
        }]),
    }]
}

/// One long contiguous run per side -- the dense shape native to OTLP's
/// contiguous-bucket exponential histograms.
fn dense_spans_series() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x06; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "dense_metric")]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 0,
            value: HistogramValue {
                scale: 4,
                zero_threshold: 0.0,
                sum: Some(500.0),
                custom_values: None,
                positive_spans: vec![span(-10, 40)],
                negative_spans: vec![span(-5, 20)],
                counts: HistogramCounts::Int {
                    zero_count: 3,
                    count: 700,
                    positive: (0..40).map(|i| i + 1).collect(),
                    negative: (0..20).map(|i| i + 1).collect(),
                },
                reset_hint: ResetHint::No,
            },
        }]),
    }]
}

/// A mix of scalar-kind and histogram-kind series in one segment, so both
/// VAL_PAGES and HIST_PAGES sections are present alongside each other.
fn mixed_scalar_and_histogram_series() -> Vec<SeriesInputV3> {
    vec![
        SeriesInputV3 {
            series_id: SeriesId([0x07; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "up")]),
            values: SeriesValues::Scalar(
                (0..5)
                    .map(|i| Sample {
                        ts_ns: i * 1_000,
                        value: 1.0,
                    })
                    .collect(),
            ),
        },
        SeriesInputV3 {
            series_id: SeriesId([0x08; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "latency_histogram")]),
            values: SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: 0,
                value: HistogramValue {
                    scale: 1,
                    zero_threshold: 1e-9,
                    sum: Some(4.5),
                    custom_values: None,
                    positive_spans: vec![span(0, 2)],
                    negative_spans: vec![],
                    counts: HistogramCounts::Int {
                        zero_count: 1,
                        count: 5,
                        positive: vec![2, 2],
                        negative: vec![],
                    },
                    reset_hint: ResetHint::Unknown,
                },
            }]),
        },
        SeriesInputV3 {
            series_id: SeriesId([0x09; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "cpu_seconds_total")]),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: 42,
                value: 3.5,
            }]),
        },
    ]
}

fn one_sample_histogram_series() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x7F; 16]),
        labels: labels(&[
            (METRIC_NAME_LABEL, "lonely_histogram"),
            ("instance", "only"),
        ]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 123,
            value: HistogramValue {
                scale: 0,
                zero_threshold: 0.0,
                sum: None,
                custom_values: None,
                positive_spans: vec![span(0, 1)],
                negative_spans: vec![],
                counts: HistogramCounts::Int {
                    zero_count: 0,
                    count: 1,
                    positive: vec![1],
                    negative: vec![],
                },
                reset_hint: ResetHint::Unknown,
            },
        }]),
    }]
}

macro_rules! golden_case {
    ($test_name:ident, $capture_name:ident, $series_fn:ident, $fixture:literal) => {
        #[test]
        fn $test_name() {
            let written = SegmentWriter::write_v3($series_fn(), fixed_identity(), fixed_bounds())
                .expect("writes");
            let fixture: &[u8] = include_bytes!(concat!("fixtures/", $fixture));
            assert_eq!(
                written.bytes.as_ref(),
                fixture,
                "v3 writer output for {} diverged from the captured golden fixture; \
                 RSEG v3 is frozen (docs/segment-format.md) -- this must never change \
                 without a version bump and ADR",
                stringify!($series_fn)
            );
        }

        /// Regenerates the fixture. Not run by default; see the module doc
        /// for when this is legitimate to run.
        #[test]
        #[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
        fn $capture_name() {
            let written = SegmentWriter::write_v3($series_fn(), fixed_identity(), fixed_bounds())
                .expect("writes");
            std::fs::write(
                concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/", $fixture),
                written.bytes.as_ref(),
            )
            .expect("writes fixture");
        }
    };
}

golden_case!(
    integer_histogram_matches_golden_fixture,
    capture_golden_v3_integer_histogram,
    integer_histogram_series,
    "golden_v3_integer_histogram.bin"
);
golden_case!(
    float_histogram_matches_golden_fixture,
    capture_golden_v3_float_histogram,
    float_histogram_series,
    "golden_v3_float_histogram.bin"
);
golden_case!(
    custom_boundaries_matches_golden_fixture,
    capture_golden_v3_custom_boundaries,
    custom_boundaries_series,
    "golden_v3_custom_boundaries.bin"
);
golden_case!(
    sparse_spans_matches_golden_fixture,
    capture_golden_v3_sparse_spans,
    sparse_spans_series,
    "golden_v3_sparse_spans.bin"
);
golden_case!(
    dense_spans_matches_golden_fixture,
    capture_golden_v3_dense_spans,
    dense_spans_series,
    "golden_v3_dense_spans.bin"
);
golden_case!(
    mixed_scalar_and_histogram_matches_golden_fixture,
    capture_golden_v3_mixed_scalar_and_histogram,
    mixed_scalar_and_histogram_series,
    "golden_v3_mixed_scalar_and_histogram.bin"
);
golden_case!(
    one_sample_histogram_matches_golden_fixture,
    capture_golden_v3_one_sample_histogram,
    one_sample_histogram_series,
    "golden_v3_one_sample_histogram.bin"
);

/// Encode determinism: the same logical input encoded twice produces
/// byte-identical output, mirroring `write_v2_is_deterministic_across_repeated_calls`.
#[test]
fn write_v3_is_deterministic_across_repeated_calls() {
    let first =
        SegmentWriter::write_v3(integer_histogram_series(), fixed_identity(), fixed_bounds())
            .expect("writes");
    let second =
        SegmentWriter::write_v3(integer_histogram_series(), fixed_identity(), fixed_bounds())
            .expect("writes");
    assert_eq!(first.bytes.as_ref(), second.bytes.as_ref());
    assert_eq!(first.summary, second.summary);

    let first_mixed = SegmentWriter::write_v3(
        mixed_scalar_and_histogram_series(),
        fixed_identity(),
        fixed_bounds(),
    )
    .expect("writes");
    let second_mixed = SegmentWriter::write_v3(
        mixed_scalar_and_histogram_series(),
        fixed_identity(),
        fixed_bounds(),
    )
    .expect("writes");
    assert_eq!(first_mixed.bytes.as_ref(), second_mixed.bytes.as_ref());
}
