//! Golden-bytes regressions for the RSEG v5 writer (ADR-0026,
//! docs/segment-format.md "RSEG v5 amendment"): the writer's output for each
//! fixed representative input must stay byte-for-byte identical across
//! internal refactors of the v5 encode path. RSEG v5 is a frozen persistent
//! contract; these tests are the tripwire for an accidental format change.
//!
//! Two fixtures, one per emission mode:
//!
//! - below the sparse-emission threshold ("no sparse"): a small mixed
//!   scalar+histogram batch. The whole object is byte-pinned to
//!   `golden_v5_no_sparse.bin`, exactly like the v2/v3 golden fixtures.
//! - at/above the threshold ("sparse"): a 4096-series batch that emits
//!   SERIES_IDX + chunked SERIES_META. At ~300 KB the object is far larger
//!   than the sub-kilobyte v1-v3 fixtures, so instead of checking in the
//!   binary it is pinned by its BLAKE3 (a 32-byte frozen-format tripwire that
//!   moves iff any stored byte moves) plus structural assertions that the
//!   sparse sections are present and the whole-object decode round-trips.
//!
//! To regenerate `golden_v5_no_sparse.bin` or reprint the sparse BLAKE3 after
//! a deliberate, versioned format change (never for an internal refactor),
//! run:
//!   cargo test -p ravel-segment --test golden_bytes_v5 -- --ignored --nocapture
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue,
    IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunValuePageV4, SegmentIdentity,
    SegmentWriter, SeriesInputV3, SeriesInputV4, SeriesValues, ValueKind, decode_catalog_v3,
    decode_catalog_v5, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

fn fixed_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0xC5; 16],
        shard: 5,
        writer_id: "golden-v5-writer".to_string(),
        writer_epoch: 5,
        writer_seq: 50,
    }
}

fn fixed_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: -2_000,
        max_ingest_ts_ns: 30_000,
    }
}

fn fixed_meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 9,
        input_set_hash: [0x44; 32],
        part_index: 2,
        level: 1,
    }
}

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

fn hist_value(seed: i32) -> HistogramValue {
    let zero_count = 1u64;
    let positive = vec![2u64, seed.unsigned_abs() as u64 + 1];
    let count = zero_count + positive.iter().sum::<u64>();
    HistogramValue {
        scale: 2,
        zero_threshold: 1e-6,
        sum: Some(f64::from(seed) + 0.125),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 2,
        }],
        negative_spans: vec![],
        counts: HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative: vec![],
        },
        reset_hint: ResetHint::Unknown,
    }
}

fn section_bytes<'a>(bytes: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .unwrap_or_else(|| panic!("section kind {kind} present"));
    &bytes[s.offset as usize..(s.offset + s.len) as usize]
}

/// Fully deterministic single-run v4 inputs, built by writing one v3 object
/// over the whole batch and slicing each series' verbatim page bytes (page
/// crc32c is bound to series_id, preserved). Deterministic input in, so the
/// v5 output is deterministic and golden-pinnable.
fn v5_golden_inputs(n: usize, hist_every: usize) -> Vec<SeriesInputV4> {
    let base = 1_650_000_000_000_000_000i64;
    let mut v3: Vec<SeriesInputV3> = Vec::with_capacity(n);
    for i in 0..n {
        let mut id = [0u8; 16];
        id[0] = (i % 17) as u8;
        id[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x100_0000_01b3).to_be_bytes());
        id[5] = (i >> 8) as u8;
        id[6] = i as u8;
        let ls = labels(&[
            (METRIC_NAME_LABEL, "golden_v5"),
            ("job", &format!("job{}", i % 6)),
            ("inst", &format!("i{i}")),
        ]);
        let values = if hist_every > 0 && i % hist_every == 0 {
            SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: base + i as i64,
                value: hist_value(i as i32),
            }])
        } else {
            SeriesValues::Scalar(vec![
                Sample {
                    ts_ns: base + i as i64,
                    value: (i as f64) * 1.5,
                },
                Sample {
                    ts_ns: base + i as i64 + 750,
                    value: (i as f64) * 1.5 + 0.5,
                },
            ])
        };
        v3.push(SeriesInputV3 {
            series_id: SeriesId(id),
            labels: ls,
            values,
        });
    }

    let written = SegmentWriter::write_v3(v3, fixed_identity(), fixed_bounds())
        .expect("write v3 source for v5 golden");
    let obj = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v3 source");
    let footer = &loc.footer;
    let entries = decode_catalog_v3(
        footer,
        section_bytes(obj, footer, 1),
        section_bytes(obj, footer, 5),
        section_bytes(obj, footer, 6),
        limits,
    )
    .expect("decode v3 source");
    let ts_sec = footer.sections.iter().find(|s| s.kind == 3).unwrap();
    let val_sec = footer.sections.iter().find(|s| s.kind == 4);
    let hist_sec = footer.sections.iter().find(|s| s.kind == 7);

    entries
        .iter()
        .map(|e| {
            let (o, l) = e.ts_page;
            let a = (ts_sec.offset + o) as usize;
            let ts_page = obj[a..a + l as usize].to_vec();
            let value_page = match e.value_kind {
                ValueKind::Scalar => {
                    let s = val_sec.expect("val");
                    let (o, l) = e.val_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Scalar(obj[a..a + l as usize].to_vec())
                }
                ValueKind::Histogram => {
                    let s = hist_sec.expect("hist");
                    let (o, l) = e.hist_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Histogram(obj[a..a + l as usize].to_vec())
                }
            };
            SeriesInputV4 {
                series_id: e.series_id,
                labels: e.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 200 + (e.min_ts_ns % 89),
                    writer_epoch: 2,
                    writer_seq: 3,
                    min_ts_ns: e.min_ts_ns,
                    max_ts_ns: e.max_ts_ns,
                    sample_count: e.sample_count,
                    ts_page,
                    value_page,
                }],
            }
        })
        .collect()
}

/// A handful of series: below the 4096 threshold, so no sparse sections.
fn no_sparse_inputs() -> Vec<SeriesInputV4> {
    v5_golden_inputs(9, 3)
}

/// 4096 series: exactly the sparse-emission threshold.
fn sparse_inputs() -> Vec<SeriesInputV4> {
    v5_golden_inputs(4096, 8)
}

fn write_no_sparse() -> Vec<u8> {
    SegmentWriter::write_v5(
        no_sparse_inputs(),
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
    )
    .expect("write v5 no-sparse")
    .bytes
    .to_vec()
}

fn write_sparse() -> ravel_segment::SegmentSummary {
    SegmentWriter::write_v5(
        sparse_inputs(),
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
    )
    .expect("write v5 sparse")
    .summary
}

/// BLAKE3 of the golden sparse v5 object. Regenerate with the ignored
/// `capture_golden_v5_sparse_blake3` test after a deliberate format change.
const SPARSE_BLAKE3: [u8; 32] = [
    0x7f, 0x92, 0xdf, 0x5f, 0x71, 0xc0, 0x29, 0xa8, 0xee, 0xd2, 0xc6, 0x49, 0x07, 0xfd, 0x54, 0xf1,
    0x4f, 0xac, 0xa7, 0xc0, 0x1a, 0x34, 0x42, 0xb0, 0xf8, 0xf3, 0x63, 0x88, 0x13, 0x43, 0x27, 0x1c,
];

#[test]
fn no_sparse_matches_golden_fixture() {
    let written = write_no_sparse();
    let fixture: &[u8] = include_bytes!("fixtures/golden_v5_no_sparse.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "v5 no-sparse writer output diverged from the captured golden fixture; \
         RSEG v5 is frozen (docs/segment-format.md) -- this must never change \
         without a version bump and ADR"
    );
    // Below threshold: version 5, whole SERIES_META, no sparse sections.
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open");
    assert_eq!(loc.version, 5);
    assert!(loc.footer.sections.iter().any(|s| s.kind == 6));
    assert!(
        !loc.footer
            .sections
            .iter()
            .any(|s| s.kind == 8 || s.kind == 9)
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_v5_no_sparse() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_v5_no_sparse.bin"
        ),
        write_no_sparse(),
    )
    .expect("write fixture");
}

#[test]
fn sparse_blake3_is_pinned() {
    let summary = write_sparse();
    assert_eq!(
        summary.blake3, SPARSE_BLAKE3,
        "v5 sparse writer output BLAKE3 diverged from the pinned golden hash; \
         RSEG v5 is frozen -- this must never change without a version bump and ADR. \
         If this change is deliberate and versioned, reprint via \
         `capture_golden_v5_sparse_blake3`."
    );
}

#[test]
#[ignore = "reprints the golden sparse BLAKE3; run explicitly after a versioned change"]
fn capture_golden_v5_sparse_blake3() {
    let summary = write_sparse();
    let hex: String = summary
        .blake3
        .iter()
        .map(|b| format!("0x{b:02x}, "))
        .collect();
    println!("SPARSE_BLAKE3 = [{hex}]");
}

#[test]
fn sparse_object_structure_and_roundtrip() {
    let written = SegmentWriter::write_v5(
        sparse_inputs(),
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
    )
    .expect("write v5 sparse")
    .bytes
    .to_vec();
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open sparse");
    assert_eq!(loc.version, 5);
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == 8),
        "SERIES_IDX"
    );
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == 9),
        "SERIES_META_CHUNKS"
    );
    assert!(
        !loc.footer.sections.iter().any(|s| s.kind == 6),
        "no whole SERIES_META"
    );
    let entries = decode_catalog_v5(&loc.footer, &written, ReaderLimits::default())
        .expect("decode sparse v5 catalog");
    assert_eq!(entries.len(), 4096);
}

#[test]
fn write_v5_is_deterministic_across_repeated_calls() {
    let a = write_no_sparse();
    let b = write_no_sparse();
    assert_eq!(a, b, "no-sparse v5 output must be deterministic");
    assert_eq!(
        write_sparse().blake3,
        write_sparse().blake3,
        "sparse v5 output must be deterministic"
    );
}
