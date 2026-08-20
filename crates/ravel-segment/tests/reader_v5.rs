//! Integration tests for RSEG v5 (ADR-0026, docs/segment-format.md "RSEG v5
//! amendment"): the sparse SERIES_IDX + chunked SERIES_META selective-read
//! path and the whole-catalog v5 decode.
//!
//! The central property: the sparse point-lookup path reads bit-identically
//! to the whole-catalog path on the same object
//! (`sparse_probe_matches_whole_catalog`, plus the proptest-driven
//! `sparse_probe_matches_for_random_index`).
//!
//! Plus the v5-specific coverage the task requires: corrupt id-window and
//! corrupt meta-chunk range-GETs must produce typed errors, and v5 exercises
//! histogram runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use prost::Message;
use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue,
    IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunValuePageV4, SegmentError,
    SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesInputV4, SeriesValues, V5_STRIDE,
    ValueKind, decode_catalog_v5, decode_chunk_runs, find_index_in_window, open_from_full,
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
/// verbatim page bytes from one v5 object built over the whole batch via the
/// raw-sample adapter (which frames pages exactly as the old v3 writer did;
/// page crc32c is bound to series_id, preserved here). `hist_every > 0` makes
/// every `hist_every`-th series a histogram series, so v5 exercises HIST runs.
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

    let written =
        SegmentWriter::write_histograms(v3, identity(), bounds()).expect("write v5 source");
    let obj = written.bytes.as_ref();
    let loc = open_from_full(obj, ReaderLimits::default()).expect("open v5 source");
    let footer = &loc.footer;
    let entries =
        decode_catalog_v5(footer, obj, ReaderLimits::default()).expect("decode v5 source catalog");

    let ts_section = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_section = footer.sections.iter().find(|s| s.kind == VAL_PAGES);
    let hist_section = footer.sections.iter().find(|s| s.kind == HIST_PAGES);

    entries
        .iter()
        .map(|e| {
            let run = &e.runs[0];
            let (ts_off, ts_len) = run.ts_page;
            let ts_abs = (ts_section.offset + ts_off) as usize;
            let ts_page = obj[ts_abs..ts_abs + ts_len as usize].to_vec();
            let value_page = match e.entry.value_kind {
                ValueKind::Scalar => {
                    let vs = val_section.expect("val section for scalar");
                    let (o, l) = run.val_page;
                    let a = (vs.offset + o) as usize;
                    RunValuePageV4::Scalar(obj[a..a + l as usize].to_vec())
                }
                ValueKind::Histogram => {
                    let hs = hist_section.expect("hist section for histogram");
                    let (o, l) = run.hist_page;
                    let a = (hs.offset + o) as usize;
                    RunValuePageV4::Histogram(obj[a..a + l as usize].to_vec())
                }
            };
            SeriesInputV4 {
                series_id: e.entry.series_id,
                labels: e.entry.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 1_000 + (e.entry.min_ts_ns % 97),
                    writer_epoch: 1,
                    writer_seq: 1,
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

fn build_v5(n: usize, hist_every: usize) -> Vec<u8> {
    SegmentWriter::write_v5(v4_inputs(n, hist_every), identity(), bounds(), meta())
        .expect("write v5")
        .bytes
        .to_vec()
}

#[test]
fn sparse_object_has_the_sparse_sections() {
    let v5 = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).expect("open sparse v5");
    assert_eq!(loc.version, 7);
    assert!(section_present(&loc.footer, SERIES_IDX));
    assert!(section_present(&loc.footer, SERIES_META_CHUNKS));
    assert!(!section_present(&loc.footer, 6), "no whole SERIES_META");
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
        let runs = decode_chunk_runs(&frame, cl.row_in_chunk, footer, ReaderLimits::default())
            .expect("decode chunk runs");
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
        let runs = decode_chunk_runs(&frame, cl.row_in_chunk, footer, ReaderLimits::default()).unwrap();

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

/// Below the sparse threshold, v5 uses the whole-section v4 grammar
/// (`decode_catalog_v4` via `decode_catalog_v5`'s delegation), so a tiny
/// object still exercises `parse_series_meta_tail_v4`'s run-major columns.
/// This reproduces the amplification the new live-byte budget check exists to
/// close: the twelve run-major columns' live-decoded footprint for `n` runs
/// (11 u64/i64 columns + 1 u32 column + 3 (u64,u64) range vectors + one
/// 96-byte `RunEntry`, 236 bytes/run, see `RUN_LIVE_BYTES_DENSE` in
/// reader.rs) vastly exceeds this tiny object's actual on-disk uncompressed
/// SERIES_META byte count. Tightening the cap to exactly that real,
/// already-passing byte count means the pre-existing input-byte check
/// (`SectionTooLarge`) is satisfied, so only the new budget check --
/// `check_run_total_live_budget`, which runs immediately after `run_total` is
/// read and before the first run-major column `Vec::with_capacity` -- can
/// reject this decode. That the typed error comes back at all, with the
/// exact `run_total`/`live_bytes`/`cap` the check derives, is what proves the
/// rejection happens at header-check time rather than after allocating.
#[test]
fn dense_run_total_live_budget_rejects_disproportionate_allocation() {
    let n = 5;
    let v5 = build_v5(n, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).expect("open dense v5");
    let footer = loc.footer.clone();
    let meta_section = footer
        .sections
        .iter()
        .find(|s| s.kind == 6)
        .expect("whole SERIES_META present below threshold");

    let live_bytes_per_run = 236u64;
    let live_bytes = n as u64 * live_bytes_per_run;
    assert!(
        meta_section.uncompressed_len < live_bytes,
        "test assumption broken: section is already as large as the live footprint"
    );

    let tight = ReaderLimits {
        max_section_uncompressed_bytes: meta_section.uncompressed_len,
        ..ReaderLimits::default()
    };
    let result = decode_catalog_v5(&footer, &v5, tight);
    assert_eq!(
        result.err(),
        Some(SegmentError::RunTotalLiveBudgetExceeded {
            run_total: n as u64,
            live_bytes,
            cap: meta_section.uncompressed_len,
        })
    );
}

/// Sparse-chunk counterpart of `dense_run_total_live_budget_rejects_
/// disproportionate_allocation`: the first SERIES_META_CHUNKS frame (stride
/// `V5_STRIDE` = 512, one run per series from `v4_inputs`, so
/// `frame_run_total` == 512) decodes fine at the default cap, but tightening
/// the cap to exactly that frame's real on-disk uncompressed byte count (so
/// the existing `verify_and_decompress_chunk_frame` input-byte check still
/// passes, since that call keeps the default limits) makes the twelve
/// run-major columns' live-decoded footprint (240 bytes/run, see
/// `RUN_LIVE_BYTES_CHUNK` in reader.rs) exceed it. `decode_chunk_runs` is
/// where `limits` is actually threaded to `check_run_total_live_budget`
/// (`decode_chunk_frame` is private), and that check runs immediately after
/// `frame_run_total` is read, before schema_ref/value_ord/value_kind/
/// run_count/run-major column parsing -- so the typed error alone proves the
/// budget is enforced before any of that allocation.
#[test]
fn chunk_run_total_live_budget_rejects_disproportionate_allocation() {
    let v5 = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5, ReaderLimits::default()).unwrap();
    let footer = &loc.footer;
    let idx = parse_series_idx(section_bytes(&v5, footer, SERIES_IDX)).unwrap();
    let chunks_stored = section_bytes(&v5, footer, SERIES_META_CHUNKS);
    let cl = idx.chunk_for(0).unwrap();
    let fo = cl.frame_offset as usize;
    let stored = &chunks_stored[fo..fo + cl.frame_stored_len as usize];
    let frame = verify_and_decompress_chunk_frame(stored, &cl, ReaderLimits::default())
        .expect("frame crc + decompress at default limits");

    let frame_run_total = V5_STRIDE as u64;
    let live_bytes_per_run = 240u64;
    let live_bytes = frame_run_total * live_bytes_per_run;
    assert!(
        cl.frame_uncompressed_len < live_bytes,
        "test assumption broken: frame is already as large as the live footprint"
    );

    let tight = ReaderLimits {
        max_section_uncompressed_bytes: cl.frame_uncompressed_len,
        ..ReaderLimits::default()
    };
    let result = decode_chunk_runs(&frame, cl.row_in_chunk, footer, tight);
    assert_eq!(
        result.err(),
        Some(SegmentError::RunTotalLiveBudgetExceeded {
            run_total: frame_run_total,
            live_bytes,
            cap: cl.frame_uncompressed_len,
        })
    );
}

#[test]
fn unknown_version_still_fails_closed() {
    // A v7 object with its version field hand-set to an unknown 8 must fail
    // closed, the same way the reader rejects a retired v1-v6 object.
    let mut v5 = build_v5(200, 0);
    let total = v5.len();
    v5[total - 8] = 8;
    v5[total - 7] = 0;
    assert!(matches!(
        open_from_full(&v5, ReaderLimits::default()),
        Err(SegmentError::UnsupportedVersion(_)) | Err(SegmentError::FooterCrcMismatch)
    ));
}

// ===========================================================================
// Structural-tamper regression tests: each hand-tampers one on-disk
// invariant that a corrupt-but-crc-consistent object could otherwise violate
// silently, then proves the reader fails closed with the specific typed
// error rather than the field-level crc32c catching it (crc32c is
// recomputed after every tamper below, so only the new structural/semantic
// check under test can fire).
// ===========================================================================

/// Recomputes every checksum layer above a section after an in-place tamper
/// that does not change the object's total length (a same-width field
/// overwrite): the section's own `Section.crc32c`, the footer protobuf
/// bytes, and `footer_crc32c`. This isolates the check under test: without
/// this repair, a tamper would be caught by `SectionCrcMismatch` or
/// `FooterCrcMismatch` instead of the new structural/semantic error.
fn refix_after_inplace_tamper(v5: &mut [u8], tampered_kind: u32) {
    let loc = open_from_full(v5, ReaderLimits::default()).expect("open before refix");
    let mut footer = loc.footer.clone();
    let section = *footer
        .sections
        .iter()
        .find(|s| s.kind == tampered_kind)
        .unwrap_or_else(|| panic!("section kind {tampered_kind} present"));
    let start = section.offset as usize;
    let end = start + section.len as usize;
    let new_crc = crc32c::crc32c(&v5[start..end]);
    for s in &mut footer.sections {
        if s.kind == tampered_kind {
            s.crc32c = new_crc;
        }
    }

    let footer_bytes = footer.encode_to_vec();
    let old_footer_len = (loc.trailer_offset - loc.footer_offset) as usize;
    assert_eq!(
        footer_bytes.len(),
        old_footer_len,
        "in-place tamper must not change the footer protobuf's encoded length"
    );
    let footer_start = loc.footer_offset as usize;
    v5[footer_start..footer_start + old_footer_len].copy_from_slice(&footer_bytes);

    let trailer_start = loc.trailer_offset as usize;
    let footer_len = old_footer_len as u32;
    let version = loc.version;
    let signal = v5[trailer_start + 10];
    let reserved = v5[trailer_start + 11];
    let magic = [
        v5[trailer_start + 12],
        v5[trailer_start + 13],
        v5[trailer_start + 14],
        v5[trailer_start + 15],
    ];
    let mut crc = crc32c::crc32c(&footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[signal, reserved]);
    crc = crc32c::crc32c_append(crc, &magic);
    v5[trailer_start + 4..trailer_start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// Byte offset of chunk directory entry `chunk_index` within a SERIES_IDX
/// section body, mirroring the layout `parse_series_idx`/`build_series_idx`
/// use: 16-byte header, `sparse_count` 36-byte sparse entries, 8-byte
/// chunk_stride/chunk_count, then 36-byte chunk entries
/// (frame_offset:8, frame_stored_len:8, frame_uncompressed_len:8,
/// first_index:4, n:4, frame_crc32c:4).
fn series_idx_chunk_entry_offset(series_count: u32, chunk_index: u32) -> usize {
    let sparse_count = series_count.div_ceil(V5_STRIDE);
    16 + sparse_count as usize * 36 + 8 + chunk_index as usize * 36
}

#[test]
fn tampered_run_total_fails_with_run_count_sum_mismatch() {
    // decode_catalog_v5 parsed the v5 chunked SERIES_META
    // header's run_total but never cross-checked it against the sum of
    // per-chunk run counts, unlike the dense v4 path and the per-chunk-frame
    // check. Overwrite run_total (the last 4 bytes of the section header,
    // which chunk 0's frame_offset -- always relative to the section start
    // -- pins exactly) with a wrong value and confirm the whole-catalog
    // decode now rejects it.
    let mut v5 = build_v5(SPARSE_N, 5);
    let loc = open_from_full(&v5, ReaderLimits::default()).unwrap();
    let footer = loc.footer.clone();
    let idx = parse_series_idx(section_bytes(&v5, &footer, SERIES_IDX)).unwrap();
    let chunk0 = idx.chunk_for(0).unwrap();
    let chunks_section = footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_META_CHUNKS)
        .unwrap();
    let run_total_abs = chunks_section.offset as usize + chunk0.frame_offset as usize - 4;

    let mut run_total_bytes: [u8; 4] = v5[run_total_abs..run_total_abs + 4].try_into().unwrap();
    let original = u32::from_le_bytes(run_total_bytes);
    run_total_bytes = (original.wrapping_add(1)).to_le_bytes();
    v5[run_total_abs..run_total_abs + 4].copy_from_slice(&run_total_bytes);

    refix_after_inplace_tamper(&mut v5, SERIES_META_CHUNKS);

    let loc2 = open_from_full(&v5, ReaderLimits::default()).expect("footer still valid");
    let err = decode_catalog_v5(&loc2.footer, &v5, ReaderLimits::default())
        .expect_err("tampered run_total must be rejected");
    assert!(
        matches!(err, SegmentError::RunCountSumMismatch { .. }),
        "expected RunCountSumMismatch, got {err:?}"
    );
}

#[test]
fn corrupt_series_idx_chunk_chain_fails_to_parse() {
    // parse_series_idx validated per-field truncation and id
    // sortedness but never the chunk directory's dense first_index chain
    // (0, K, 2K, ...), which only decode_catalog_v5's now-removed ad hoc
    // check enforced -- and locate/chunk_for never called that. A
    // corrupt-but-crc-consistent directory used to let chunk_for silently
    // answer "absent" for a present id instead of failing to parse. Break
    // chunk 1's first_index (still crc-consistent afterward) and confirm
    // parse_series_idx now rejects the whole index up front.
    let v5_base = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5_base, ReaderLimits::default()).unwrap();
    let series_count = loc.footer.series_count as u32;
    assert!(
        series_count.div_ceil(V5_STRIDE) >= 3,
        "fixture must have at least 3 chunks to tamper chunk index 1 safely"
    );

    let mut v5 = v5_base;
    let idx_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDX)
        .unwrap();
    let entry_off = idx_section.offset as usize
        + series_idx_chunk_entry_offset(series_count, 1 /* chunk index */);
    let first_index_off = entry_off + 24;

    let mut first_index_bytes: [u8; 4] =
        v5[first_index_off..first_index_off + 4].try_into().unwrap();
    let original = u32::from_le_bytes(first_index_bytes);
    assert_eq!(original, V5_STRIDE, "chunk 1's dense first_index");
    first_index_bytes = (original + 1).to_le_bytes(); // breaks the dense chain
    v5[first_index_off..first_index_off + 4].copy_from_slice(&first_index_bytes);

    refix_after_inplace_tamper(&mut v5, SERIES_IDX);

    let loc2 = open_from_full(&v5, ReaderLimits::default()).expect("footer still valid");
    let idx_bytes = section_bytes(&v5, &loc2.footer, SERIES_IDX);
    let err = parse_series_idx(idx_bytes)
        .expect_err("corrupt chunk chain must fail to parse, not decode");
    assert!(
        matches!(err, SegmentError::BadSparseIndex(_)),
        "expected BadSparseIndex, got {err:?}"
    );

    // The whole-catalog decode path goes through the same parse and must
    // fail the same way, not silently drop series or answer any id absent.
    let err2 = decode_catalog_v5(&loc2.footer, &v5, ReaderLimits::default())
        .expect_err("whole-catalog decode must also fail closed");
    assert!(matches!(err2, SegmentError::BadSparseIndex(_)));
}

#[test]
fn chunk_frame_trailing_value_ord_bytes_fail_closed() {
    // The v5 frame's value_ord block decode never asserted the
    // per-series materialization consumed the whole block, unlike
    // parse_value_ord_block_all_v2 (v2/v3/v4), which returns BadBlockLen on
    // a mismatch. Every fixture series here carries a 3-label schema, so the
    // last chunk's value_ord block is consumed exactly by materialization;
    // append one extra ordinal past what any series' schema needs and
    // confirm decode_catalog_v5 now rejects it instead of silently ignoring
    // the trailing varint.
    let v5_base = build_v5(SPARSE_N, 0);
    let loc = open_from_full(&v5_base, ReaderLimits::default()).unwrap();
    let footer = loc.footer.clone();
    let series_count = footer.series_count as u32;
    let idx = parse_series_idx(section_bytes(&v5_base, &footer, SERIES_IDX)).unwrap();
    let last_index = u64::from(series_count - 1);
    let cl = idx.chunk_for(last_index).unwrap();

    let chunks_section = *footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_META_CHUNKS)
        .unwrap();
    let abs_start = chunks_section.offset as usize + cl.frame_offset as usize;
    let old_len = cl.frame_stored_len as usize;
    let stored = v5_base[abs_start..abs_start + old_len].to_vec();
    let frame_raw =
        verify_and_decompress_chunk_frame(&stored, &cl, ReaderLimits::default()).unwrap();

    // Walk the frame header (n:u32, frame_run_total:u32, 3 base uvarints,
    // then the length-prefixed schema_ref block) to find where the
    // length-prefixed value_ord block starts.
    fn read_uvarint(bytes: &[u8], pos: &mut usize) -> u64 {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = bytes[*pos];
            *pos += 1;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return result;
            }
            shift += 7;
        }
    }
    fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    let mut pos = 8usize; // n:u32 + frame_run_total:u32
    read_uvarint(&frame_raw, &mut pos); // ts_base
    read_uvarint(&frame_raw, &mut pos); // val_base
    read_uvarint(&frame_raw, &mut pos); // hist_base
    let schema_ref_len = read_uvarint(&frame_raw, &mut pos) as usize;
    pos += schema_ref_len; // skip the whole schema_ref block

    let value_ord_block_start = pos;
    let value_ord_len = read_uvarint(&frame_raw, &mut pos) as usize;
    let value_ord_payload_start = pos;
    let value_ord_payload_end = value_ord_payload_start + value_ord_len;

    let mut new_frame_raw = Vec::with_capacity(frame_raw.len() + 2);
    new_frame_raw.extend_from_slice(&frame_raw[..value_ord_block_start]);
    write_uvarint(&mut new_frame_raw, (value_ord_len + 1) as u64);
    new_frame_raw.extend_from_slice(&frame_raw[value_ord_payload_start..value_ord_payload_end]);
    new_frame_raw.push(0x00); // one extra ordinal no series' schema will consume
    new_frame_raw.extend_from_slice(&frame_raw[value_ord_payload_end..]);

    let new_stored = zstd::bulk::compress(&new_frame_raw, 3).unwrap();
    let new_crc = crc32c::crc32c(&new_stored);
    let delta = new_stored.len() as i64 - old_len as i64;

    let body_len = loc.footer_offset as usize;
    let mut body = v5_base[..body_len].to_vec();
    body.splice(abs_start..abs_start + old_len, new_stored.iter().copied());

    let mut new_footer = footer.clone();
    for s in &mut new_footer.sections {
        if s.kind == SERIES_META_CHUNKS {
            s.len = (s.len as i64 + delta) as u64;
        } else if s.offset as usize >= abs_start + old_len {
            s.offset = (s.offset as i64 + delta) as u64;
        }
    }

    // Patch the tampered chunk's own directory entry (frame_stored_len,
    // frame_uncompressed_len, frame_crc32c) inside SERIES_IDX's content,
    // which physically shifted with the rest of the tail but did not change
    // size, so its field offsets within the section are unchanged.
    let idx_section_new = *new_footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDX)
        .unwrap();
    let idx_abs = idx_section_new.offset as usize;
    let last_chunk_index = series_count.div_ceil(V5_STRIDE) - 1;
    let entry_off = idx_abs + series_idx_chunk_entry_offset(series_count, last_chunk_index);
    body[entry_off + 8..entry_off + 16].copy_from_slice(&(new_stored.len() as u64).to_le_bytes());
    body[entry_off + 16..entry_off + 24]
        .copy_from_slice(&(new_frame_raw.len() as u64).to_le_bytes());
    body[entry_off + 32..entry_off + 36].copy_from_slice(&new_crc.to_le_bytes());

    let idx_len = idx_section_new.len as usize;
    let idx_crc = crc32c::crc32c(&body[idx_abs..idx_abs + idx_len]);
    let mc_abs = chunks_section.offset as usize;
    let mc_len = new_footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_META_CHUNKS)
        .unwrap()
        .len as usize;
    let mc_crc = crc32c::crc32c(&body[mc_abs..mc_abs + mc_len]);
    for s in &mut new_footer.sections {
        if s.kind == SERIES_IDX {
            s.crc32c = idx_crc;
        } else if s.kind == SERIES_META_CHUNKS {
            s.crc32c = mc_crc;
        }
    }

    let footer_bytes = new_footer.encode_to_vec();
    let footer_len = footer_bytes.len() as u32;
    body.extend_from_slice(&footer_bytes);

    let old_trailer_start = loc.trailer_offset as usize;
    let version = loc.version;
    let signal = v5_base[old_trailer_start + 10];
    let reserved = v5_base[old_trailer_start + 11];
    let magic = [
        v5_base[old_trailer_start + 12],
        v5_base[old_trailer_start + 13],
        v5_base[old_trailer_start + 14],
        v5_base[old_trailer_start + 15],
    ];
    let mut crc = crc32c::crc32c(&footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[signal, reserved]);
    crc = crc32c::crc32c_append(crc, &magic);
    body.extend_from_slice(&footer_len.to_le_bytes());
    body.extend_from_slice(&crc.to_le_bytes());
    body.extend_from_slice(&version.to_le_bytes());
    body.push(signal);
    body.push(reserved);
    body.extend_from_slice(&magic);

    let tampered = body;
    let loc2 = open_from_full(&tampered, ReaderLimits::default()).expect("footer still valid");
    let err = decode_catalog_v5(&loc2.footer, &tampered, ReaderLimits::default())
        .expect_err("trailing value_ord bytes must be rejected");
    assert!(
        matches!(err, SegmentError::BadBlockLen),
        "expected BadBlockLen, got {err:?}"
    );
}
