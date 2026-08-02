//! Golden-bytes regressions for the RSEG v6 writer (ADR-0047,
//! docs/segment-format.md): the writer's output for each fixed
//! representative input must stay byte-for-byte identical across internal
//! refactors of the v6 encode path. RSEG v6 is a frozen persistent contract;
//! these tests are the tripwire for an accidental format change.
//!
//! Two fixtures, one per emission mode:
//!
//! - below the sparse-emission threshold ("no sparse"): a small mixed
//!   scalar+histogram batch that also carries one EXEMPLARS record, so the
//!   v6 corpus has at least one object exercising the new section. The whole
//!   object is byte-pinned to `golden_v6_with_exemplars.bin`, exactly like
//!   the v2/v3/v5 golden fixtures.
//! - at/above the threshold ("sparse"): a 4096-series batch that emits
//!   SERIES_IDX + chunked SERIES_META, no exemplars. At ~300 KB the object is
//!   far larger than the sub-kilobyte v1-v3 fixtures, so instead of checking
//!   in the binary it is pinned by its BLAKE3 (a 32-byte frozen-format
//!   tripwire that moves iff any stored byte moves) plus structural
//!   assertions that the sparse sections are present and the whole-object
//!   decode round-trips.
//!
//! To regenerate `golden_v6_with_exemplars.bin` or reprint the sparse BLAKE3
//! after a deliberate, versioned format change (never for an internal
//! refactor), run:
//!   cargo test -p ravel-segment --test golden_bytes_v6 -- --ignored --nocapture
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, HistogramCounts, HistogramSample, HistogramSpan,
    HistogramValue, IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunValuePageV4,
    SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesInputV4, SeriesValues, ValueKind,
    decode_catalog_v5, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

/// Section kinds (docs/segment-format.md; crate-internal `format::section_kind`
/// is not public, so the wire values are named locally here).
const SERIES_META: u32 = 6;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;
const EXEMPLARS: u32 = 10;

fn fixed_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0xC6; 16],
        shard: 6,
        writer_id: "golden-v6-writer".to_string(),
        writer_epoch: 6,
        writer_seq: 60,
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
        input_set_hash: [0x46; 32],
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

/// Fully deterministic single-run v4 inputs, built by writing one v6 object
/// over the whole batch (via the raw-sample adapter, which frames pages the
/// same way the old v3 writer did) and slicing each series' verbatim page
/// bytes (page crc32c is bound to series_id, preserved). Deterministic input
/// in, so the v6 output under the fixed run provenance below is deterministic
/// and golden-pinnable.
fn v6_golden_inputs(n: usize, hist_every: usize) -> Vec<SeriesInputV4> {
    let base = 1_650_000_000_000_000_000i64;
    let mut v3: Vec<SeriesInputV3> = Vec::with_capacity(n);
    for i in 0..n {
        let mut id = [0u8; 16];
        id[0] = (i % 17) as u8;
        id[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x100_0000_01b3).to_be_bytes());
        id[5] = (i >> 8) as u8;
        id[6] = i as u8;
        let ls = labels(&[
            (METRIC_NAME_LABEL, "golden_v6"),
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

    let written = SegmentWriter::write_histograms(v3, fixed_identity(), fixed_bounds())
        .expect("write v6 source for v6 golden");
    let obj = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v6 source");
    let footer = &loc.footer;
    let _ = section_bytes(obj, footer, 1);
    let entries = decode_catalog_v5(footer, obj, limits).expect("decode v6 source");
    let ts_sec = footer.sections.iter().find(|s| s.kind == 3).unwrap();
    let val_sec = footer.sections.iter().find(|s| s.kind == 4);
    let hist_sec = footer.sections.iter().find(|s| s.kind == 7);

    entries
        .iter()
        .map(|e| {
            let run = &e.runs[0];
            let (o, l) = run.ts_page;
            let a = (ts_sec.offset + o) as usize;
            let ts_page = obj[a..a + l as usize].to_vec();
            let value_page = match e.entry.value_kind {
                ValueKind::Scalar => {
                    let s = val_sec.expect("val");
                    let (o, l) = run.val_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Scalar(obj[a..a + l as usize].to_vec())
                }
                ValueKind::Histogram => {
                    let s = hist_sec.expect("hist");
                    let (o, l) = run.hist_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Histogram(obj[a..a + l as usize].to_vec())
                }
            };
            SeriesInputV4 {
                series_id: e.entry.series_id,
                labels: e.entry.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 200 + (e.entry.min_ts_ns % 89),
                    writer_epoch: 2,
                    writer_seq: 3,
                    min_ts_ns: e.entry.min_ts_ns,
                    max_ts_ns: e.entry.max_ts_ns,
                    sample_count: e.entry.sample_count,
                    ts_page,
                    value_page,
                }],
            }
        })
        .collect()
}

/// A handful of series: below the 4096 threshold, so no sparse sections.
fn no_sparse_inputs() -> Vec<SeriesInputV4> {
    v6_golden_inputs(9, 3)
}

/// 4096 series: exactly the sparse-emission threshold.
fn sparse_inputs() -> Vec<SeriesInputV4> {
    v6_golden_inputs(4096, 8)
}

/// One exemplar attached to the first no-sparse series, so the golden v6
/// fixture exercises EXEMPLARS (ADR-0047) instead of only the unchanged v5
/// grammar. Trace/span id are non-zero (all-zero means "absent") and one
/// attribute pair exercises interning into LABEL_DICT alongside series labels.
fn no_sparse_exemplars(series: &[SeriesInputV4]) -> Vec<ExemplarInput> {
    let target = series.first().expect("at least one series");
    vec![ExemplarInput {
        series_id: target.series_id,
        ts_ns: target.runs[0].min_ts_ns,
        value: 42.5,
        trace_id: [0xAB; 16],
        span_id: [0xCD; 8],
        attrs: vec![("trace_state".to_string(), "sampled=1".to_string())],
    }]
}

fn write_no_sparse() -> Vec<u8> {
    let series = no_sparse_inputs();
    let exemplars = no_sparse_exemplars(&series);
    SegmentWriter::write_v5_with_exemplars(
        series,
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
        exemplars,
    )
    .expect("write v6 no-sparse")
    .bytes
    .to_vec()
}

fn write_sparse() -> ravel_segment::SegmentSummary {
    SegmentWriter::write_v5_with_exemplars(
        sparse_inputs(),
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
        Vec::new(),
    )
    .expect("write v6 sparse")
    .summary
}

/// BLAKE3 of the golden sparse v6 object. Regenerate with the ignored
/// `capture_golden_v6_sparse_blake3` test after a deliberate format change.
const SPARSE_BLAKE3: [u8; 32] = [
    0x2c, 0x4d, 0x00, 0xe1, 0xe5, 0x5a, 0x2a, 0x73, 0xe1, 0xea, 0x71, 0x56, 0xea, 0x7f, 0x0a, 0x82,
    0x39, 0xe9, 0x66, 0x23, 0xb6, 0x26, 0x89, 0x07, 0xe2, 0x7c, 0x29, 0xfb, 0x54, 0x71, 0x2c, 0xe7,
];

#[test]
fn no_sparse_matches_golden_fixture() {
    let written = write_no_sparse();
    let fixture: &[u8] = include_bytes!("fixtures/golden_v6_with_exemplars.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "v6 no-sparse writer output diverged from the captured golden fixture; \
         RSEG v6 is frozen (docs/segment-format.md) -- this must never change \
         without a version bump and ADR"
    );
    // Below threshold: version 6, whole SERIES_META, no sparse sections,
    // EXEMPLARS present (this fixture is the corpus's exemplar coverage).
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open");
    assert_eq!(loc.version, 6);
    assert!(loc.footer.sections.iter().any(|s| s.kind == SERIES_META));
    assert!(
        !loc
            .footer
            .sections
            .iter()
            .any(|s| s.kind == SERIES_IDX || s.kind == SERIES_META_CHUNKS)
    );
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == EXEMPLARS),
        "golden v6 no-sparse fixture must carry an EXEMPLARS section"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_v6_with_exemplars() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_v6_with_exemplars.bin"
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
        "v6 sparse writer output BLAKE3 diverged from the pinned golden hash; \
         RSEG v6 is frozen -- this must never change without a version bump and ADR. \
         If this change is deliberate and versioned, reprint via \
         `capture_golden_v6_sparse_blake3`."
    );
}

#[test]
#[ignore = "reprints the golden sparse BLAKE3; run explicitly after a versioned change"]
fn capture_golden_v6_sparse_blake3() {
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
    let written = SegmentWriter::write_v5_with_exemplars(
        sparse_inputs(),
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
        Vec::new(),
    )
    .expect("write v6 sparse")
    .bytes
    .to_vec();
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open sparse");
    assert_eq!(loc.version, 6);
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == SERIES_IDX),
        "SERIES_IDX"
    );
    assert!(
        loc
            .footer
            .sections
            .iter()
            .any(|s| s.kind == SERIES_META_CHUNKS),
        "SERIES_META_CHUNKS"
    );
    assert!(
        !loc.footer.sections.iter().any(|s| s.kind == SERIES_META),
        "no whole SERIES_META"
    );
    let entries = decode_catalog_v5(&loc.footer, &written, ReaderLimits::default())
        .expect("decode sparse v6 catalog");
    assert_eq!(entries.len(), 4096);
}

#[test]
fn write_v6_is_deterministic_across_repeated_calls() {
    let a = write_no_sparse();
    let b = write_no_sparse();
    assert_eq!(a, b, "no-sparse v6 output must be deterministic");
    assert_eq!(
        write_sparse().blake3,
        write_sparse().blake3,
        "sparse v6 output must be deterministic"
    );
}
