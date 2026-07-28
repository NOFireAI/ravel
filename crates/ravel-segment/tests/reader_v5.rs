//! Integration tests for RSEG v5 (ADR-0026, docs/segment-format.md "RSEG v5
//! amendment"): the sparse SERIES_IDX + chunked SERIES_META selective-read
//! path and the whole-catalog v5 decode.
//!
//! These migrate the two properties the #167 prototype's tests proved, onto
//! the production paths:
//!
//! - below the sparse-emission threshold a v5 object is the v4 object save the
//!   trailer version bytes (`below_threshold_v5_is_v4_plus_version_bump`),
//! - the sparse point-lookup path reads bit-identically to the whole-catalog
//!   path on the same object (`sparse_reads_match_whole_catalog`,
//!   proptest-driven over every series).
//!
//! Plus the v5-specific coverage ADR-0026 point 6 and the task require:
//! corrupt id-window and corrupt meta-chunk range-GETs must produce typed
//! errors, and v5 inherits v4's histogram runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue,
    IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunValuePageV4, SegmentError,
    SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesInputV4, SeriesValues, ValueKind,
    decode_catalog_v4, decode_catalog_v5, decode_chunk_runs, find_index_in_window, open_from_full,
    parse_series_idx, verify_and_decompress_chunk_frame, verify_id_window,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const SERIES_IDS: u32 = 5;
const HIST_PAGES: u32 = 7;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;

/// Above the 4096 sparse-emission threshold, so the sparse sections appear.
const SPARSE_N: usize = 5000;

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x2A; 16],
        shard: 2,
        writer_id: "reader-v5-test".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 100_000,
    }
}

fn meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 5,
        input_set_hash: [0x33; 32],
        part_index: 0,
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

fn section_bytes<'a>(bytes: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .unwrap_or_else(|| panic!("section kind {kind} present"));
    &bytes[s.offset as usize..(s.offset + s.len) as usize]
}

fn section_present(footer: &Footer, kind: u32) -> bool {
    footer.sections.iter().any(|s| s.kind == kind)
}

fn hist_value(seed: i32) -> HistogramValue {
    let zero_count = 1u64;
    let positive = vec![2u64, seed.unsigned_abs() as u64 + 1];
    let count = zero_count + positive.iter().sum::<u64>();
    HistogramValue {
        scale: 3,
        zero_threshold: 0.001,
        sum: Some(f64::from(seed) + 0.5),
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

/// Deterministic id for series `i`: distinct, and NOT in `i` order, so both
/// the writer sort and the reader binary search are exercised.
fn series_id(i: usize) -> SeriesId {
    let mut id = [0u8; 16];
    id[0] = (i % 13) as u8;
    id[3] = (i % 251) as u8;
    id[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9).to_be_bytes());
    // Fold i into the low bytes too so ids stay distinct even when the mul
    // collides in the high word.
    id[6] = (i >> 8) as u8;
    id[7] = i as u8;
    SeriesId(id)
}

/// Builds the same logical batch as single-run v4 inputs, sourcing each run's
/// verbatim page bytes from one v3 object built over the whole batch (page
/// crc32c is bound to series_id, preserved here). `hist_every > 0` makes every
/// `hist_every`-th series a histogram series, so v5 exercises HIST runs too.
fn v4_inputs(n: usize, hist_every: usize) -> Vec<SeriesInputV4> {
    let base = 1_700_000_000_000_000_000i64;
    let mut v3: Vec<SeriesInputV3> = Vec::with_capacity(n);
    for i in 0..n {
        let sid = series_id(i);
        let ls = labels(&[
            (
                METRIC_NAME_LABEL,
                if i % 2 == 0 {
                    "http_requests"
                } else {
                    "cpu_seconds"
                },
            ),
            ("job", &format!("j{}", i % 7)),
            ("inst", &format!("i{i}")),
        ]);
        let is_hist = hist_every > 0 && i % hist_every == 0;
        let values = if is_hist {
            SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: base + i as i64,
                value: hist_value(i as i32),
            }])
        } else {
            SeriesValues::Scalar(vec![
                Sample {
                    ts_ns: base + i as i64,
                    value: i as f64,
                },
                Sample {
                    ts_ns: base + i as i64 + 1000,
                    value: i as f64 + 0.5,
                },
            ])
        };
        v3.push(SeriesInputV3 {
            series_id: sid,
            labels: ls,
            values,
        });
    }

    let written = SegmentWriter::write_v3(v3, identity(), bounds()).expect("write v3 source");
    let obj = written.bytes.as_ref();
    let loc = open_from_full(obj, ReaderLimits::default()).expect("open v3 source");
    let footer = &loc.footer;
    let dict = section_bytes(obj, footer, 1);
    let ids = section_bytes(obj, footer, SERIES_IDS);
    let smeta = section_bytes(obj, footer, 6);
    let entries =
        ravel_segment::decode_catalog_v3(footer, dict, ids, smeta, ReaderLimits::default())
            .expect("decode v3 source catalog");

    let ts_section = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_section = footer.sections.iter().find(|s| s.kind == VAL_PAGES);
    let hist_section = footer.sections.iter().find(|s| s.kind == HIST_PAGES);

    entries
        .iter()
        .map(|e| {
            let (ts_off, ts_len) = e.ts_page;
            let ts_abs = (ts_section.offset + ts_off) as usize;
            let ts_page = obj[ts_abs..ts_abs + ts_len as usize].to_vec();
            let value_page = match e.value_kind {
                ValueKind::Scalar => {
                    let vs = val_section.expect("val section for scalar");
                    let (o, l) = e.val_page;
                    let a = (vs.offset + o) as usize;
                    RunValuePageV4::Scalar(obj[a..a + l as usize].to_vec())
                }
                ValueKind::Histogram => {
                    let hs = hist_section.expect("hist section for histogram");
                    let (o, l) = e.hist_page;
                    let a = (hs.offset + o) as usize;
                    RunValuePageV4::Histogram(obj[a..a + l as usize].to_vec())
                }
            };
            SeriesInputV4 {
                series_id: e.series_id,
                labels: e.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 1_000 + (e.min_ts_ns % 97),
                    writer_epoch: 1,
                    writer_seq: 1,
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

fn build_v4(n: usize, hist_every: usize) -> Vec<u8> {
    SegmentWriter::write_v4(v4_inputs(n, hist_every), identity(), bounds(), meta())
        .expect("write v4")
        .bytes
        .to_vec()
}

fn build_v5(n: usize, hist_every: usize) -> Vec<u8> {
    SegmentWriter::write_v5(v4_inputs(n, hist_every), identity(), bounds(), meta())
        .expect("write v5")
        .bytes
        .to_vec()
}

#[test]
fn below_threshold_v5_is_v4_plus_version_bump() {
    // n well below 4096: no sparse sections, so the v5 object is the v4 object
    // save the trailer version field (and the footer_crc it feeds).
    let n = 200;
    let v4 = build_v4(n, 0);
    let v5 = build_v5(n, 0);
    assert_eq!(v4.len(), v5.len(), "same length below threshold");

    let total = v4.len();
    // Trailer: [footer_len(4) crc(4) version(2) signal(1) reserved(1) magic(4)].
    // Only version (total-8..total-6) and the crc it feeds (total-12..total-8)
    // may differ.
    let allow_start = total - 12;
    let allow_end = total - 6;
    for (i, (a, b)) in v4.iter().zip(&v5).enumerate() {
        if (allow_start..allow_end).contains(&i) {
            continue;
        }
        assert_eq!(a, b, "byte {i} differs outside the version/crc window");
    }
    // The version field is exactly 4 -> 5.
    assert_eq!(u16::from_le_bytes([v4[total - 8], v4[total - 7]]), 4);
    assert_eq!(u16::from_le_bytes([v5[total - 8], v5[total - 7]]), 5);

    // And it reads back as a valid v5 object with the v4-shaped catalog.
    let loc = open_from_full(&v5, ReaderLimits::default()).expect("open below-threshold v5");
    assert_eq!(loc.version, 5);
    assert!(!section_present(&loc.footer, SERIES_IDX));
    assert!(!section_present(&loc.footer, SERIES_META_CHUNKS));
    assert!(section_present(&loc.footer, 6), "whole SERIES_META present");
}

#[test]
fn sparse_object_has_the_sparse_sections() {
    let v5 = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).expect("open sparse v5");
    assert_eq!(loc.version, 5);
    assert!(section_present(&loc.footer, SERIES_IDX));
    assert!(section_present(&loc.footer, SERIES_META_CHUNKS));
    assert!(!section_present(&loc.footer, 6), "no whole SERIES_META");
}

#[test]
fn v5_catalog_matches_v4_catalog() {
    // The chunked v5 catalog decodes to exactly the same folded entries and
    // per-run views as the v4 object of the same batch: page ranges are
    // relative to their (verbatim-copied) sections, so they match despite the
    // sparse sections shifting absolute offsets.
    for hist_every in [0usize, 5] {
        let v4 = build_v4(SPARSE_N, hist_every);
        let v5 = build_v5(SPARSE_N, hist_every);
        let l4 = open_from_full(&v4, ReaderLimits::default()).unwrap();
        let l5 = open_from_full(&v5, ReaderLimits::default()).unwrap();
        let e4 = decode_catalog_v4(
            &l4.footer,
            section_bytes(&v4, &l4.footer, 1),
            section_bytes(&v4, &l4.footer, SERIES_IDS),
            section_bytes(&v4, &l4.footer, 6),
            ReaderLimits::default(),
        )
        .unwrap();
        let e5 = decode_catalog_v5(&l5.footer, &v5, ReaderLimits::default()).unwrap();
        assert_eq!(
            e4, e5,
            "v5 catalog must equal v4 catalog (hist_every={hist_every})"
        );
    }
}

/// Runs of every series read via the sparse point-probe path.
fn sparse_probe_runs(obj: &[u8]) -> Vec<(SeriesId, Vec<ravel_segment::RunEntry>)> {
    let loc = open_from_full(obj, ReaderLimits::default()).unwrap();
    let footer = &loc.footer;
    let idx = parse_series_idx(section_bytes(obj, footer, SERIES_IDX)).unwrap();
    let ids_stored = section_bytes(obj, footer, SERIES_IDS);
    let chunks_stored = section_bytes(obj, footer, SERIES_META_CHUNKS);
    let count = (ids_stored.len() - 4) / 16;

    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let target: [u8; 16] = ids_stored[4 + i * 16..4 + i * 16 + 16].try_into().unwrap();
        let window = idx.locate(&target).expect("window for present id");
        let ws = window.section_offset as usize;
        let win = &ids_stored[ws..ws + window.len as usize];
        verify_id_window(win, &window).expect("window crc");
        let abs = find_index_in_window(win, window.first_index, &target)
            .unwrap()
            .expect("present id found");
        let cl = idx.chunk_for(abs).expect("chunk for index");
        let fo = cl.frame_offset as usize;
        let stored = &chunks_stored[fo..fo + cl.frame_stored_len as usize];
        let frame = verify_and_decompress_chunk_frame(stored, &cl, ReaderLimits::default())
            .expect("frame crc + decompress");
        let runs = decode_chunk_runs(&frame, cl.row_in_chunk, footer).expect("decode chunk runs");
        out.push((SeriesId(target), runs));
    }
    out
}

#[test]
fn sparse_probe_matches_whole_catalog() {
    for hist_every in [0usize, 5] {
        let v5 = build_v5(SPARSE_N, hist_every);
        let loc = open_from_full(&v5, ReaderLimits::default()).unwrap();
        let whole = decode_catalog_v5(&loc.footer, &v5, ReaderLimits::default()).unwrap();
        // Index the whole-catalog runs by series id.
        let mut by_id: std::collections::HashMap<SeriesId, &Vec<ravel_segment::RunEntry>> =
            std::collections::HashMap::new();
        for e in &whole {
            by_id.insert(e.entry.series_id, &e.runs);
        }
        for (sid, runs) in sparse_probe_runs(&v5) {
            let expected = by_id.get(&sid).expect("series present in whole catalog");
            assert_eq!(&runs, *expected, "sparse runs differ for {sid:?}");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    /// Differential: for an arbitrary series index, the sparse point-probe
    /// yields the exact runs the whole-catalog decode does. The full sweep
    /// lives in `sparse_probe_matches_whole_catalog`; this randomizes the
    /// target to catch any index-dependent boundary bug (chunk edges, window
    /// edges).
    #[test]
    fn sparse_probe_matches_for_random_index(pick in 0usize..SPARSE_N) {
        static OBJ: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
        let v5 = OBJ.get_or_init(|| build_v5(SPARSE_N, 5));
        let loc = open_from_full(v5, ReaderLimits::default()).unwrap();
        let footer = &loc.footer;
        let ids_stored = section_bytes(v5, footer, SERIES_IDS);
        let idx = parse_series_idx(section_bytes(v5, footer, SERIES_IDX)).unwrap();
        let chunks_stored = section_bytes(v5, footer, SERIES_META_CHUNKS);
        let target: [u8; 16] = ids_stored[4 + pick * 16..4 + pick * 16 + 16].try_into().unwrap();

        let window = idx.locate(&target).unwrap();
        let ws = window.section_offset as usize;
        let win = &ids_stored[ws..ws + window.len as usize];
        verify_id_window(win, &window).unwrap();
        let abs = find_index_in_window(win, window.first_index, &target).unwrap().unwrap();
        prop_assert_eq!(abs, pick as u64);
        let cl = idx.chunk_for(abs).unwrap();
        let fo = cl.frame_offset as usize;
        let frame = verify_and_decompress_chunk_frame(
            &chunks_stored[fo..fo + cl.frame_stored_len as usize],
            &cl,
            ReaderLimits::default(),
        )
        .unwrap();
        let runs = decode_chunk_runs(&frame, cl.row_in_chunk, footer).unwrap();

        let whole = decode_catalog_v5(footer, v5, ReaderLimits::default()).unwrap();
        let expected = whole
            .iter()
            .find(|e| e.entry.series_id == SeriesId(target))
            .unwrap();
        prop_assert_eq!(&runs, &expected.runs);
    }
}

#[test]
fn corrupt_id_window_is_typed_error() {
    let v5 = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).unwrap();
    let footer = &loc.footer;
    let idx = parse_series_idx(section_bytes(&v5, footer, SERIES_IDX)).unwrap();
    let ids_stored = section_bytes(&v5, footer, SERIES_IDS).to_vec();
    let target: [u8; 16] = ids_stored[4..20].try_into().unwrap();
    let window = idx.locate(&target).unwrap();
    let ws = window.section_offset as usize;
    let mut win = ids_stored[ws..ws + window.len as usize].to_vec();
    win[0] ^= 0xFF; // bit rot in the fetched window
    assert_eq!(
        verify_id_window(&win, &window),
        Err(SegmentError::IdWindowCrcMismatch),
    );
}

#[test]
fn corrupt_chunk_frame_is_typed_error() {
    let v5 = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).unwrap();
    let footer = &loc.footer;
    let idx = parse_series_idx(section_bytes(&v5, footer, SERIES_IDX)).unwrap();
    let chunks_stored = section_bytes(&v5, footer, SERIES_META_CHUNKS).to_vec();
    let cl = idx.chunk_for(0).unwrap();
    let fo = cl.frame_offset as usize;
    let mut stored = chunks_stored[fo..fo + cl.frame_stored_len as usize].to_vec();
    stored[0] ^= 0xFF; // bit rot in the fetched stored frame
    assert_eq!(
        verify_and_decompress_chunk_frame(&stored, &cl, ReaderLimits::default()).err(),
        Some(SegmentError::ChunkCrcMismatch),
    );
}

#[test]
fn unknown_version_still_fails_closed() {
    // A v5 object with its version field hand-set to an unknown 6 must fail
    // closed, the same pattern v1-v4 readers use when they meet a v5 object.
    let mut v5 = build_v5(200, 0);
    let total = v5.len();
    v5[total - 8] = 6;
    v5[total - 7] = 0;
    assert!(matches!(
        open_from_full(&v5, ReaderLimits::default()),
        Err(SegmentError::UnsupportedVersion(_)) | Err(SegmentError::FooterCrcMismatch)
    ));
}
