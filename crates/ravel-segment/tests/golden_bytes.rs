//! Golden-bytes regression for the RSEG v1 writer (issue #19 / A3): the
//! writer's output for a fixed representative input must stay byte-for-byte
//! identical across internal refactors of the encode path. RSEG v1 is a
//! frozen persistent contract (docs/segment-format.md); this test is the
//! tripwire for an accidental format change.
//!
//! `fixtures/golden_v1_a3.bin` was captured from the writer before the A3
//! copy-reduction refactor. To regenerate it after a deliberate, versioned
//! format change (never for an internal refactor), run:
//!   cargo test -p ravel-segment --test golden_bytes -- --ignored --nocapture
#![allow(clippy::expect_used)]

use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const FIXTURE: &[u8] = include_bytes!("fixtures/golden_v1_a3.bin");

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

/// A fixed, varied multi-series input: enough samples per series to exercise
/// both the Gorilla and raw-f64 VAL fallback paths, irregular/duplicate/
/// negative timestamp deltas (lz4-compressed TS_DELTA_VARINT), NaN/-0.0/Inf
/// payloads, and more than one distinct label set (a non-trivial LABEL_DICT
/// and SERIES_TABLE).
fn representative_series() -> Vec<SeriesInput> {
    vec![
        SeriesInput {
            series_id: SeriesId([0x11; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("instance", "a"),
                ("method", "GET"),
            ]),
            samples: (0..40)
                .map(|i| Sample {
                    ts_ns: 1_000 + i * 17,
                    value: (i as f64) * 0.5,
                })
                .collect(),
        },
        SeriesInput {
            series_id: SeriesId([0x22; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("instance", "b"),
                ("method", "POST"),
            ]),
            samples: vec![
                Sample {
                    ts_ns: 500,
                    value: 1.0,
                },
                Sample {
                    ts_ns: 500,
                    value: 2.0,
                },
                Sample {
                    ts_ns: 300,
                    value: 3.0,
                },
                Sample {
                    ts_ns: -200,
                    value: f64::NAN,
                },
                Sample {
                    ts_ns: 900,
                    value: -0.0,
                },
                Sample {
                    ts_ns: 900,
                    value: f64::INFINITY,
                },
            ],
        },
        SeriesInput {
            series_id: SeriesId([0x33; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "cpu_seconds")]),
            samples: vec![Sample {
                ts_ns: 42,
                value: 3.5,
            }],
        },
        SeriesInput {
            series_id: SeriesId([0x44; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "memory_bytes"),
                ("instance", "a"),
                ("region", "us-east"),
            ]),
            samples: (0..20)
                .map(|i| Sample {
                    ts_ns: i * 100,
                    // Alternating-sign, high-entropy deltas: forces the
                    // "new block" Gorilla control path repeatedly, likely
                    // to land close to the raw-fallback size threshold.
                    value: if i % 2 == 0 {
                        f64::from_bits(0x4010_0000_0000_0000 ^ (i as u64))
                    } else {
                        f64::from_bits(0xC020_0000_0000_0000 ^ ((i as u64) << 3))
                    },
                })
                .collect(),
        },
    ]
}

fn fixed_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0xAB; 16],
        shard: 7,
        writer_id: "golden-writer".to_string(),
        writer_epoch: 3,
        writer_seq: 99,
    }
}

fn fixed_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: -1_000,
        max_ingest_ts_ns: 5_000,
    }
}

#[test]
fn writer_output_matches_golden_fixture() {
    let written = SegmentWriter::write(representative_series(), fixed_identity(), fixed_bounds())
        .expect("writes");
    assert_eq!(
        written.bytes.as_ref(),
        FIXTURE,
        "writer output diverged from the captured golden fixture; RSEG v1 is \
         frozen (docs/segment-format.md) -- this must never change without a \
         version bump and ADR"
    );
}

/// Regenerates `fixtures/golden_v1_a3.bin`. Not run by default; see the
/// module doc for when this is legitimate to run.
#[test]
#[ignore = "regenerates the golden fixture; run explicitly, never in CI"]
fn capture_golden_fixture() {
    let written = SegmentWriter::write(representative_series(), fixed_identity(), fixed_bounds())
        .expect("writes");
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_v1_a3.bin"
        ),
        written.bytes.as_ref(),
    )
    .expect("writes fixture");
}
