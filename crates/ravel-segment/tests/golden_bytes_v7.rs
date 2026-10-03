//! Golden-bytes regressions for the RSEG v7 writer (ADR-0092,
//! docs/segment-format.md): the writer's output for each fixed
//! representative input must stay byte-for-byte identical across internal
//! refactors of the v7 encode path. RSEG v7 is a frozen persistent contract;
//! these tests are the tripwire for an accidental format change.
//!
//! Three fixtures, one per emission mode:
//!
//! - below the sparse-emission threshold ("no sparse"): a small mixed
//!   scalar+histogram batch that also carries one EXEMPLARS record, so the
//!   v7 corpus has at least one object exercising that section. The whole
//!   object is byte-pinned to `golden_v7_with_exemplars.bin`, exactly like
//!   the v2/v3/v5 golden fixtures.
//! - at/above the threshold ("sparse"): a 4096-series batch that emits
//!   SERIES_IDX + chunked SERIES_META, no exemplars. At ~300 KB the object is
//!   far larger than the sub-kilobyte v1-v3 fixtures, so instead of checking
//!   in the binary it is pinned by its BLAKE3 (a 32-byte frozen-format
//!   tripwire that moves iff any stored byte moves) plus structural
//!   assertions that the sparse sections are present and the whole-object
//!   decode round-trips.
//! - a run-merged object carrying the optional per-sample dedup provenance
//!   columns (ADR-0092 decision 1): one series whose single run merged four
//!   samples from three different writes, so all four provenance columns
//!   (`created_unix_ns` delta, `writer_epoch`, `writer_seq`, `in_page_index`)
//!   carry real, non-constant content, plus a second series with no columns so
//!   the golden exercises the mixed "extension present, some runs None" shape.
//!   The whole object is byte-pinned to `golden_v7_merged_provenance.bin`.
//!
//! To regenerate `golden_v7_with_exemplars.bin`,
//! `golden_v7_merged_provenance.bin`, or reprint the sparse BLAKE3 after a
//! deliberate, versioned format change (never for an internal refactor), run:
//!   cargo test -p ravel-segment --test golden_bytes_v7 -- --ignored --nocapture
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, ExemplarInput, HistogramCounts, HistogramSample, HistogramSpan,
    HistogramValue, IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunInputV7, RunValuePageV4,
    SampleProvenance, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesInputV4, SeriesInputV7,
    SeriesValues, ValueKind, decode_catalog_v5, encode_run_v4, open_from_full,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

/// Section kinds (docs/segment-format.md; crate-internal `format::section_kind`
/// is not public, so the wire values are named locally here).
const LABEL_DICT: u32 = 1;
const SERIES_META: u32 = 6;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;
const EXEMPLARS: u32 = 10;

fn fixed_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0xC7; 16],
        shard: 7,
        writer_id: "golden-v7-writer".to_string(),
        writer_epoch: 7,
        writer_seq: 70,
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
        input_set_hash: [0x47; 32],
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

/// Fully deterministic single-run v4 inputs, built by writing one v7 object
/// over the whole batch (via the raw-sample adapter, which frames pages the
/// same way the old v3 writer did) and slicing each series' verbatim page
/// bytes (page crc32c is bound to series_id, preserved). Deterministic input
/// in, so the v7 output under the fixed run provenance below is deterministic
/// and golden-pinnable.
fn v7_golden_inputs(n: usize, hist_every: usize) -> Vec<SeriesInputV4> {
    let base = 1_650_000_000_000_000_000i64;
    let mut v3: Vec<SeriesInputV3> = Vec::with_capacity(n);
    for i in 0..n {
        let mut id = [0u8; 16];
        id[0] = (i % 17) as u8;
        id[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x100_0000_01b3).to_be_bytes());
        id[5] = (i >> 8) as u8;
        id[6] = i as u8;
        let ls = labels(&[
            (METRIC_NAME_LABEL, "golden_v7"),
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
        .expect("write v7 source for v7 golden");
    let obj = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v7 source");
    let footer = &loc.footer;
    let _ = section_bytes(obj, footer, 1);
    let entries = decode_catalog_v5(footer, obj, limits).expect("decode v7 source");
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
    v7_golden_inputs(9, 3)
}

/// 4096 series: exactly the sparse-emission threshold.
fn sparse_inputs() -> Vec<SeriesInputV4> {
    v7_golden_inputs(4096, 8)
}

/// One exemplar attached to the first no-sparse series, so the golden v7
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
    .expect("write v7 no-sparse")
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
    .expect("write v7 sparse")
    .summary
}

/// Scalar samples helper for the merged-provenance fixture.
fn scalar_samples(vals: &[(i64, f64)]) -> SeriesValues {
    SeriesValues::Scalar(
        vals.iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    )
}

/// A run-merged v7 object with the optional per-sample dedup provenance columns
/// (ADR-0092 decision 1). Two series:
///
/// - `merged`: one run of four samples merged from three different writes, so
///   every one of the four provenance columns carries real, non-constant
///   content -- `created_unix_ns` deltas (1000/1000/2000/3000), `writer_epoch`
///   (7/7/8/9), `writer_seq` (100/100/101/102), and `in_page_index`
///   (0/1/0/0) all vary, exercising the whole extension rather than a
///   single-writer constant/RLE degenerate case.
/// - `plain`: one run with no columns, so the golden also pins the mixed
///   "extension present, this run None" encoding.
fn write_merged_provenance() -> Vec<u8> {
    let merged_id = SeriesId([0x71; 16]);
    let plain_id = SeriesId([0x72; 16]);

    let merged_run = encode_run_v4(
        &merged_id,
        1_000,
        7,
        100,
        &scalar_samples(&[
            (1_650_000_000_000_000_000, 1.5),
            (1_650_000_000_000_000_750, 2.5),
            (1_650_000_000_000_001_500, 3.5),
            (1_650_000_000_000_002_250, 4.5),
        ]),
    )
    .expect("frame merged run");
    let merged_prov = vec![
        SampleProvenance {
            created_unix_ns: 1_000,
            writer_epoch: 7,
            writer_seq: 100,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 1_000,
            writer_epoch: 7,
            writer_seq: 100,
            in_page_index: 1,
        },
        SampleProvenance {
            created_unix_ns: 2_000,
            writer_epoch: 8,
            writer_seq: 101,
            in_page_index: 0,
        },
        SampleProvenance {
            created_unix_ns: 3_000,
            writer_epoch: 9,
            writer_seq: 102,
            in_page_index: 0,
        },
    ];

    let plain_run = encode_run_v4(
        &plain_id,
        5_000,
        3,
        200,
        &scalar_samples(&[(1_650_000_000_000_000_000, 9.0)]),
    )
    .expect("frame plain run");

    let series = vec![
        SeriesInputV7 {
            series_id: merged_id,
            labels: labels(&[(METRIC_NAME_LABEL, "merged_series")]),
            runs: vec![RunInputV7 {
                run: merged_run,
                provenance: Some(merged_prov),
            }],
        },
        SeriesInputV7 {
            series_id: plain_id,
            labels: labels(&[(METRIC_NAME_LABEL, "plain_series")]),
            runs: vec![RunInputV7 {
                run: plain_run,
                provenance: None,
            }],
        },
    ];

    SegmentWriter::write_v7_with_provenance(
        series,
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
        Vec::new(),
    )
    .expect("write v7 merged provenance")
    .bytes
    .to_vec()
}

/// BLAKE3 of the golden sparse v7 object. Regenerate with the ignored
/// `capture_golden_v7_sparse_blake3` test after a deliberate format change.
const SPARSE_BLAKE3: [u8; 32] = [
    0xbd, 0xfd, 0x81, 0x88, 0xd0, 0x2c, 0x39, 0x0b, 0xbf, 0x78, 0x06, 0x6d, 0xb2, 0x5b, 0x7c, 0x0d,
    0x30, 0x75, 0x7d, 0x61, 0x7a, 0xc7, 0x6f, 0x86, 0x6c, 0x47, 0x20, 0x23, 0x90, 0xd2, 0x50, 0xc7,
];

#[test]
fn no_sparse_matches_golden_fixture() {
    let written = write_no_sparse();
    let fixture: &[u8] = include_bytes!("fixtures/golden_v7_with_exemplars.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "v7 no-sparse writer output diverged from the captured golden fixture; \
         RSEG v7 is frozen (docs/segment-format.md) -- this must never change \
         without a version bump and ADR"
    );
    // Below threshold: version 7, whole SERIES_META, no sparse sections,
    // EXEMPLARS present (this fixture is the corpus's exemplar coverage).
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open");
    assert_eq!(loc.version, 7);
    assert!(loc.footer.sections.iter().any(|s| s.kind == SERIES_META));
    assert!(
        !loc.footer
            .sections
            .iter()
            .any(|s| s.kind == SERIES_IDX || s.kind == SERIES_META_CHUNKS)
    );
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == EXEMPLARS),
        "golden v7 no-sparse fixture must carry an EXEMPLARS section"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_v7_with_exemplars() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_v7_with_exemplars.bin"
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
        "v7 sparse writer output BLAKE3 diverged from the pinned golden hash; \
         RSEG v7 is frozen -- this must never change without a version bump and ADR. \
         If this change is deliberate and versioned, reprint via \
         `capture_golden_v7_sparse_blake3`."
    );
}

#[test]
#[ignore = "reprints the golden sparse BLAKE3; run explicitly after a versioned change"]
fn capture_golden_v7_sparse_blake3() {
    let summary = write_sparse();
    let hex: String = summary
        .blake3
        .iter()
        .map(|b| format!("0x{b:02x}, "))
        .collect();
    println!("SPARSE_BLAKE3 = [{hex}]");
}

#[test]
fn merged_run_with_per_sample_provenance_matches_golden_fixture() {
    let written = write_merged_provenance();
    let fixture: &[u8] = include_bytes!("fixtures/golden_v7_merged_provenance.bin");
    assert_eq!(
        written.as_slice(),
        fixture,
        "v7 merged-provenance writer output diverged from the captured golden \
         fixture; RSEG v7 and its per-sample provenance extension are frozen \
         (docs/segment-format.md, ADR-0092) -- this must never change without a \
         version bump and ADR"
    );

    // The fixture is a v7 object below the sparse threshold, so it carries the
    // whole SERIES_META (which holds the provenance extension), no sparse
    // sections, and no exemplars.
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open");
    assert_eq!(loc.version, 7);
    assert!(loc.footer.sections.iter().any(|s| s.kind == SERIES_META));
    assert!(
        !loc.footer
            .sections
            .iter()
            .any(|s| s.kind == SERIES_IDX || s.kind == SERIES_META_CHUNKS)
    );

    // The provenance columns survive decode: the merged series carries one
    // Some(column) of four keys, the plain series a None.
    let entries = decode_catalog_v5(&loc.footer, &written, ReaderLimits::default())
        .expect("decode merged-provenance catalog");
    let merged = entries
        .iter()
        .find(|e| e.entry.series_id == SeriesId([0x71; 16]))
        .expect("merged series present");
    assert_eq!(merged.per_sample_provenance.len(), 1);
    let column = merged.per_sample_provenance[0]
        .as_ref()
        .expect("merged run carries a provenance column");
    assert_eq!(column.len(), 4);
    assert_eq!(
        column
            .iter()
            .map(|p| (p.writer_epoch, p.writer_seq, p.in_page_index))
            .collect::<Vec<_>>(),
        vec![(7, 100, 0), (7, 100, 1), (8, 101, 0), (9, 102, 0)]
    );
    let plain = entries
        .iter()
        .find(|e| e.entry.series_id == SeriesId([0x72; 16]))
        .expect("plain series present");
    assert!(
        plain.per_sample_provenance[0].is_none(),
        "the plain series' run carries no provenance column"
    );
}

#[test]
#[ignore = "regenerates a golden fixture; run explicitly, never in CI"]
fn capture_golden_v7_merged_provenance() {
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/golden_v7_merged_provenance.bin"
        ),
        write_merged_provenance(),
    )
    .expect("write fixture");
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
    .expect("write v7 sparse")
    .bytes
    .to_vec();
    let loc = open_from_full(&written, ReaderLimits::default()).expect("open sparse");
    assert_eq!(loc.version, 7);
    assert!(
        loc.footer.sections.iter().any(|s| s.kind == SERIES_IDX),
        "SERIES_IDX"
    );
    assert!(
        loc.footer
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
        .expect("decode sparse v7 catalog");
    assert_eq!(entries.len(), 4096);
}

#[test]
fn write_v7_is_deterministic_across_repeated_calls() {
    let a = write_no_sparse();
    let b = write_no_sparse();
    assert_eq!(a, b, "no-sparse v7 output must be deterministic");
    assert_eq!(
        write_sparse().blake3,
        write_sparse().blake3,
        "sparse v7 output must be deterministic"
    );
    assert_eq!(
        write_merged_provenance(),
        write_merged_provenance(),
        "merged-provenance v7 output must be deterministic"
    );
}

/// Two exemplars for the same series at the same timestamp both survive a
/// write-read round trip. Compaction copies exemplars verbatim and never drops
/// a record, so a retried write that lands in two L0 objects must produce an L1
/// object holding both (ADR-0047 amendment 2026-08-03). An earlier revision of
/// the writer collapsed a run of equal keys to its last record, which made that
/// impossible.
#[test]
fn two_exemplars_sharing_a_key_both_survive_a_round_trip() {
    let series = no_sparse_inputs();
    let target = series.first().expect("at least one series");
    let ts_ns = target.runs[0].min_ts_ns;
    let both = vec![
        ExemplarInput {
            series_id: target.series_id,
            ts_ns,
            value: 1.0,
            trace_id: [0x11; 16],
            span_id: [0x22; 8],
            attrs: Vec::new(),
        },
        ExemplarInput {
            series_id: target.series_id,
            ts_ns,
            value: 2.0,
            trace_id: [0x33; 16],
            span_id: [0x44; 8],
            attrs: Vec::new(),
        },
    ];
    let written = SegmentWriter::write_v5_with_exemplars(
        series,
        fixed_identity(),
        fixed_bounds(),
        fixed_meta(),
        both,
    )
    .expect("write v7 with duplicate exemplar keys")
    .bytes;

    let loc = open_from_full(&written, ReaderLimits::default()).expect("open");
    let dict_bytes = section_bytes(&written, &loc.footer, LABEL_DICT);
    let exemplars_bytes = section_bytes(&written, &loc.footer, EXEMPLARS);

    let got = ravel_segment::decode_exemplars_section(
        &loc.footer,
        dict_bytes,
        exemplars_bytes,
        ReaderLimits::default(),
    )
    .expect("decode exemplars");

    assert_eq!(got.len(), 2, "both records must survive, not just the last");
    assert_eq!(got[0].series_index, got[1].series_index);
    assert_eq!(got[0].ts_ns, got[1].ts_ns);
    // Bit patterns, never `==`: an exemplar value can be NaN or -0.0.
    assert_eq!(got[0].value.to_bits(), 1.0f64.to_bits());
    assert_eq!(got[1].value.to_bits(), 2.0f64.to_bits());
    assert_eq!(got[0].trace_id, [0x11; 16]);
    assert_eq!(got[1].trace_id, [0x33; 16]);

    // The per-series probe must return both too, not stop at the first.
    let probed = ravel_segment::probe_exemplars_by_series(
        &loc.footer,
        dict_bytes,
        exemplars_bytes,
        ReaderLimits::default(),
        got[0].series_index,
    )
    .expect("probe exemplars");
    assert_eq!(probed.len(), 2);
    assert_eq!(probed[1].trace_id, [0x33; 16]);
}
