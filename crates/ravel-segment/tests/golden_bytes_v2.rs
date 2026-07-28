//! Golden-bytes regressions for the RSEG v2 writer (ADR-0014,
//! docs/rseg-v2-plan.md phase P2, issue #30): the writer's output for each
//! fixed representative input must stay byte-for-byte identical across
//! internal refactors of the v2 encode path. RSEG v2 is a frozen persistent
//! contract (docs/segment-format.md "RSEG v2 amendment"); these tests are
//! the tripwire for an accidental format change.
//!
//! Six cases per docs/rseg-v2-plan.md phase P2's test list: multi-schema,
//! single-schema, raw-f64 pages needing alignment padding, Gorilla-only,
//! empty segment, 1-sample series. To regenerate a fixture after a
//! deliberate, versioned format change (never for an internal refactor),
//! run:
//!   cargo test -p ravel-segment --test golden_bytes_v2 -- --ignored --nocapture
#![allow(clippy::expect_used)]

use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
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
        tenant_hash: [0xCD; 16],
        shard: 11,
        writer_id: "golden-v2-writer".to_string(),
        writer_epoch: 5,
        writer_seq: 200,
    }
}

fn fixed_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: -2_000,
        max_ingest_ts_ns: 9_000,
    }
}

/// Multiple distinct label-name sets (three schemas across four series),
/// NaN/-0.0/Inf payloads, and both Gorilla and raw-f64 VAL fallback --
/// deliberately similar in shape to golden_bytes.rs's v1 fixture, so the
/// v1/v2 fixtures are easy to compare by eye.
fn multi_schema_series() -> Vec<SeriesInput> {
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

/// Every series shares the exact same label-name set (only values differ),
/// so SERIES_META's schema dictionary has exactly one entry.
fn single_schema_series() -> Vec<SeriesInput> {
    (0..5)
        .map(|i| SeriesInput {
            series_id: SeriesId([i as u8 + 1; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "requests_total"),
                ("instance", if i % 2 == 0 { "a" } else { "b" }),
                ("method", "GET"),
            ]),
            samples: (0..10)
                .map(|j| Sample {
                    ts_ns: j * 10,
                    value: (i * 10 + j) as f64,
                })
                .collect(),
        })
        .collect()
}

/// Series id `0x01` is processed first (multi-sample, smoothly increasing
/// values -- Gorilla-compressible) and leaves the running VAL_PAGES length
/// at a value not congruent to 0 mod 8 once its header and payload are
/// accounted for; ids `0x02` and `0x03` each carry exactly one sample,
/// which the raw-fallback rule always stores as VAL_RAW_F64 (Gorilla's
/// first value alone is exactly 8 bytes, never smaller than raw), so each
/// needs alignment padding recorded in its `val_page_gap`.
fn raw_f64_with_padding_series() -> Vec<SeriesInput> {
    vec![
        SeriesInput {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "smooth_metric")]),
            samples: (0..7)
                .map(|i| Sample {
                    ts_ns: i * 10,
                    value: 100.0 + i as f64,
                })
                .collect(),
        },
        SeriesInput {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "single_a")]),
            samples: vec![Sample {
                ts_ns: 5,
                value: 42.5,
            }],
        },
        SeriesInput {
            series_id: SeriesId([0x03; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "single_b")]),
            samples: vec![Sample {
                ts_ns: 9,
                value: -7.25,
            }],
        },
    ]
}

/// Every series has enough samples with small, regular deltas that Gorilla
/// always beats the raw-fallback threshold, so no VAL_RAW_F64 page (and
/// therefore no alignment padding) appears anywhere in this fixture.
fn gorilla_only_series() -> Vec<SeriesInput> {
    (0..3)
        .map(|i| SeriesInput {
            series_id: SeriesId([0xA0 + i as u8; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "steady_metric")]),
            samples: (0..30)
                .map(|j| Sample {
                    ts_ns: j * 15,
                    value: 1_000.0 + (i * 30 + j) as f64 * 0.25,
                })
                .collect(),
        })
        .collect()
}

fn one_sample_series() -> Vec<SeriesInput> {
    vec![SeriesInput {
        series_id: SeriesId([0x7F; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "lonely_metric"), ("instance", "only")]),
        samples: vec![Sample {
            ts_ns: 123,
            value: 9.75,
        }],
    }]
}

macro_rules! golden_case {
    ($test_name:ident, $capture_name:ident, $series_fn:ident, $fixture:literal) => {
        #[test]
        fn $test_name() {
            let written = SegmentWriter::write_v2($series_fn(), fixed_identity(), fixed_bounds())
                .expect("writes");
            let fixture: &[u8] = include_bytes!(concat!("fixtures/", $fixture));
            assert_eq!(
                written.bytes.as_ref(),
                fixture,
                "v2 writer output for {} diverged from the captured golden fixture; \
                 RSEG v2 is frozen (docs/segment-format.md) -- this must never change \
                 without a version bump and ADR",
                stringify!($series_fn)
            );
        }

        /// Regenerates the fixture. Not run by default; see the module doc
        /// for when this is legitimate to run.
        #[test]
        #[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
        fn $capture_name() {
            let written = SegmentWriter::write_v2($series_fn(), fixed_identity(), fixed_bounds())
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
    multi_schema_matches_golden_fixture,
    capture_golden_v2_multi_schema,
    multi_schema_series,
    "golden_v2_multi_schema.bin"
);
golden_case!(
    single_schema_matches_golden_fixture,
    capture_golden_v2_single_schema,
    single_schema_series,
    "golden_v2_single_schema.bin"
);
golden_case!(
    raw_f64_with_padding_matches_golden_fixture,
    capture_golden_v2_raw_f64_padding,
    raw_f64_with_padding_series,
    "golden_v2_raw_f64_padding.bin"
);
golden_case!(
    gorilla_only_matches_golden_fixture,
    capture_golden_v2_gorilla_only,
    gorilla_only_series,
    "golden_v2_gorilla_only.bin"
);
golden_case!(
    one_sample_matches_golden_fixture,
    capture_golden_v2_one_sample,
    one_sample_series,
    "golden_v2_one_sample.bin"
);

fn empty_series() -> Vec<SeriesInput> {
    Vec::new()
}
golden_case!(
    empty_segment_matches_golden_fixture,
    capture_golden_v2_empty,
    empty_series,
    "golden_v2_empty.bin"
);

/// Encode determinism: the same logical input encoded twice produces
/// byte-identical output (no nondeterministic iteration order leaking into
/// LABEL_DICT's sorted order, HashMap-based schema dedup, or anything else
/// on the v2 path).
#[test]
fn write_v2_is_deterministic_across_repeated_calls() {
    let first = SegmentWriter::write_v2(multi_schema_series(), fixed_identity(), fixed_bounds())
        .expect("writes");
    let second = SegmentWriter::write_v2(multi_schema_series(), fixed_identity(), fixed_bounds())
        .expect("writes");
    assert_eq!(first.bytes.as_ref(), second.bytes.as_ref());
    assert_eq!(first.summary, second.summary);

    let first_single =
        SegmentWriter::write_v2(single_schema_series(), fixed_identity(), fixed_bounds())
            .expect("writes");
    let second_single =
        SegmentWriter::write_v2(single_schema_series(), fixed_identity(), fixed_bounds())
            .expect("writes");
    assert_eq!(first_single.bytes.as_ref(), second_single.bytes.as_ref());
}

/// Minimal, standalone re-implementation of unsigned LEB128 read/write,
/// used only to prove the "raw-f64 with padding" fixture actually contains
/// non-zero `val_page_gap` entries -- this file has no access to
/// `ravel_segment::varint` (crate-private, not re-exported), so it cannot
/// reuse the writer's own decoder. Pinned against hand vectors immediately
/// below so a bug here can't silently launder a real encoder bug.
mod local_varint {
    pub fn read_uvarint(bytes: &[u8], pos: &mut usize) -> u64 {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = bytes[*pos];
            *pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        result
    }
}

#[test]
fn local_varint_reader_matches_known_leb128_vectors() {
    // Same hand vectors as crates/ravel-segment/src/varint.rs's own test,
    // confirming this file's standalone re-implementation is correct
    // before trusting it to inspect the padding fixture below.
    let mut pos = 0;
    assert_eq!(local_varint::read_uvarint(&[0x00], &mut pos), 0);
    pos = 0;
    assert_eq!(local_varint::read_uvarint(&[0x7f], &mut pos), 127);
    pos = 0;
    assert_eq!(local_varint::read_uvarint(&[0xac, 0x02], &mut pos), 300);
}

/// Confirms the "raw-f64 with padding" fixture is not accidentally
/// padding-free: parses just enough of the real SERIES_META bytes (footer
/// via `prost`, section bytes via the footer's own offsets, zstd via the
/// `zstd` crate -- all ordinary dependencies of this crate, so no
/// crate-internal access is needed beyond the varint reader above) to
/// check that at least one `val_page_gap` entry is non-zero.
#[test]
fn raw_f64_with_padding_fixture_actually_has_padding() {
    let written = SegmentWriter::write_v2(
        raw_f64_with_padding_series(),
        fixed_identity(),
        fixed_bounds(),
    )
    .expect("writes");
    let object = written.bytes.as_ref();

    let n = object.len();
    let trailer = &object[n - 16..];
    let footer_len = u32::from_le_bytes(trailer[0..4].try_into().expect("4 bytes")) as usize;
    let footer_bytes = &object[n - 16 - footer_len..n - 16];
    let footer =
        <ravel_segment::Footer as prost::Message>::decode(footer_bytes).expect("footer decodes");

    let meta_section = footer
        .sections
        .iter()
        .find(|s| s.kind == 6) // SERIES_META
        .expect("SERIES_META section present");
    let stored =
        &object[meta_section.offset as usize..(meta_section.offset + meta_section.len) as usize];
    let raw = zstd::bulk::decompress(stored, meta_section.uncompressed_len as usize)
        .expect("zstd decompresses");

    let count = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes")) as usize;
    let schema_count = u32::from_le_bytes(raw[4..8].try_into().expect("4 bytes"));
    let mut pos = 8usize;
    for _ in 0..schema_count {
        let name_count = local_varint::read_uvarint(&raw, &mut pos);
        for _ in 0..name_count {
            local_varint::read_uvarint(&raw, &mut pos);
        }
    }
    // Skip schema_ref (block 1) and value_ord (block 2): read block_len,
    // advance past it without decoding (this test only needs val_page_gap,
    // block 8 of 9).
    for _ in 0..7 {
        let block_len = local_varint::read_uvarint(&raw, &mut pos) as usize;
        pos += block_len;
    }
    // Block 8: val_page_gap, `count` varints.
    let val_page_gap_len = local_varint::read_uvarint(&raw, &mut pos) as usize;
    let block_end = pos + val_page_gap_len;
    let mut any_nonzero_gap = false;
    for _ in 0..count {
        if local_varint::read_uvarint(&raw, &mut pos) != 0 {
            any_nonzero_gap = true;
        }
    }
    assert_eq!(
        pos, block_end,
        "val_page_gap block framing is self-consistent"
    );
    assert!(
        any_nonzero_gap,
        "the raw-f64-with-padding fixture must contain at least one non-zero \
         val_page_gap; otherwise it isn't exercising the alignment rule it's named for"
    );
}
