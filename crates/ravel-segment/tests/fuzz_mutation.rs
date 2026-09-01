//! Corrupt-input mutation harness for the RSEG byte parsers. Object-storage
//! bytes are subject to bit rot and
//! partial writes; every decoder that reads them must return a typed error
//! or a valid decode, never panic and never yield wrong data
//! (docs/segment-format.md, CLAUDE.md testing patterns).
//!
//! ADR-0092 leaves v7 the only readable version. The live seed corpus is a
//! pair of real objects (below and above the sparse-emission threshold,
//! built via the current writer and so v7-trailered); proptest-driven
//! single-bit and truncation mutations, plus fully arbitrary byte vectors,
//! run through the public read pipeline and must never panic.
//!
//! Retired-version rejection: five checked-in pre-v7 goldens (v1, v2, v3,
//! v5, v6) are kept only as rejection seeds. `parse_footer` rejects a non-7
//! trailer version before it ever touches a section, so each decodes to a
//! typed `UnsupportedVersion`, never a parse attempt or panic. This is why
//! the two historically inconsistent v3 histogram fixtures (dense_spans,
//! float_histogram) are gone: with v3 no longer decoded, their
//! `HistogramCountInconsistent` outcome is unreachable, exactly as ADR-0027
//! predicted.
//!
//! Runs on the pinned stable toolchain (proptest only, no cargo-fuzz).
//! Case count honours `PROPTEST_CASES`; the CI fuzz job raises it.
//!
//! Point-probe coverage: `decode_catalog_v5` walks every series
//! unconditionally, but a live point lookup ("does series id X exist, and
//! where are its chunks") instead calls `SparseIdIndex::locate`,
//! `find_index_in_window`, `chunk_for`, and `decode_chunk_runs`, which parse
//! the same SERIES_IDX / SERIES_META_CHUNKS bytes through a different code
//! path. `point_probe` below drives that chain directly over mutated object
//! bytes so corruption mishandled only on the point-probe path (a panic, or
//! a wrong presence/absence answer) can't hide behind
//! `decode_catalog_v5`-only mutation coverage.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_segment::{
    Footer, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds,
    ReaderLimits, ResetHint, RunEntry, SegmentError, SegmentIdentity, SegmentWriter, SeriesEntryV4,
    SeriesInputV3, SeriesValues, ValueKind, decode_catalog_v5, decode_chunk_runs,
    decode_run_histogram_pages, decode_run_pages_soa, find_index_in_window, open_from_full,
    parse_footer, parse_series_idx, plan_ranges_v4, verify_and_decompress_chunk_frame,
    verify_id_window,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};
use std::sync::OnceLock;

/// v5 section kind numbers the point-probe path reads directly (frozen
/// format, docs/segment-format.md `section_kind`; not exported by
/// `ravel_segment` itself, so pinned here the same way the retired trailer
/// versions are pinned elsewhere in this file).
const SECTION_KIND_SERIES_IDS: u32 = 5;
const SECTION_KIND_SERIES_IDX: u32 = 8;
const SECTION_KIND_SERIES_META_CHUNKS: u32 = 9;

/// Retired-version goldens kept only as rejection seeds (one per retired
/// layout that has a checked-in fixture): v1-v3 predate v5 (ADR-0026), v5
/// itself was retired in turn by the v6 EXEMPLARS bump (ADR-0047), and v6 by
/// the v7 run-merge / provenance bump (ADR-0092). This
/// is the corpus convention (crates/ravel-segment/tests/fixtures/): once a
/// version is superseded, its golden is never deleted and never updated --
/// it stays exactly as the old writer produced it, and its only remaining
/// job is proving the reader still fails closed on it with a typed
/// `UnsupportedVersion` rather than silently misparsing a stale object.
/// Each must fail closed with `UnsupportedVersion`.
const REJECTION_SEEDS: &[&[u8]] = &[
    include_bytes!("fixtures/golden_v1_a3.bin"),
    include_bytes!("fixtures/golden_v2_single_schema.bin"),
    include_bytes!("fixtures/golden_v3_integer_histogram.bin"),
    include_bytes!("fixtures/golden_v5_no_sparse.bin"),
    include_bytes!("fixtures/golden_v6_with_exemplars.bin"),
];

fn range_bytes(bytes: &[u8], range: (u64, u64)) -> Result<&[u8], SegmentError> {
    let start = usize::try_from(range.0).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let len = usize::try_from(range.1).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let end = start
        .checked_add(len)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    bytes
        .get(start..end)
        .ok_or(SegmentError::SectionOutOfBounds)
}

/// Finds a section's `(offset, len)` by kind in a (possibly mutated) footer.
/// `None` means the section is absent -- either a legitimately below-
/// threshold v5 object, or a mutation that dropped the section from the
/// footer's own section list.
fn find_section_range(footer: &Footer, kind: u32) -> Option<(u64, u64)> {
    footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .map(|s| (s.offset, s.len))
}

/// Drives the whole public v5 read pipeline (footer -> validate -> catalog ->
/// plan -> per-run page decode) over complete object bytes. Any failure is a
/// typed [`SegmentError`]; the contract this harness checks is that the
/// function returns (Ok or Err) instead of panicking, for every input.
fn full_decode(bytes: &[u8]) -> Result<usize, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits)?;
    let entries = decode_catalog_v5(&loc.footer, bytes, limits)?;
    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = plan_ranges_v4(&loc.footer, &selected)?;
    let mut total = 0usize;
    for entry in &entries {
        for (run_index, run) in entry.runs.iter().enumerate() {
            let range = ranges
                .iter()
                .find(|r| r.series_id == entry.entry.series_id && r.run_index == run_index)
                .ok_or(SegmentError::SectionOutOfBounds)?;
            let ts_bytes = range_bytes(bytes, range.ts_range)?;
            total += match entry.entry.value_kind {
                ValueKind::Scalar => {
                    let val_bytes = range_bytes(bytes, range.val_range)?;
                    let mut scratch = Vec::new();
                    let mut timestamps = Vec::new();
                    let mut values = Vec::new();
                    decode_run_pages_soa(
                        &entry.entry.series_id,
                        run,
                        ts_bytes,
                        val_bytes,
                        limits,
                        &mut scratch,
                        &mut timestamps,
                        &mut values,
                    )?;
                    timestamps.len()
                }
                ValueKind::Histogram => {
                    let hist_bytes = range_bytes(bytes, range.hist_range)?;
                    decode_run_histogram_pages(
                        &entry.entry.series_id,
                        run,
                        ts_bytes,
                        hist_bytes,
                        limits,
                    )?
                    .len()
                }
            };
        }
    }
    Ok(total)
}

/// Outcome of [`point_probe`], distinguishing "no sparse index to probe"
/// from "index says this id is absent" from "id resolved to runs" -- unlike
/// `full_decode`'s single success/error split, the point-probe chain has a
/// legitimate not-found result that is not an error.
#[derive(Debug)]
enum ProbeOutcome {
    /// The object carries no SERIES_IDX/SERIES_IDS/SERIES_META_CHUNKS triple
    /// (a below-threshold v5 object, or a mutation that removed one of the
    /// sections from the footer).
    NoSparseIndex,
    /// `locate` or `find_index_in_window` report the target id is not
    /// present.
    Absent,
    /// The full chain resolved the target id's runs.
    Found(Vec<RunEntry>),
}

/// Drives the v5 sparse point-probe surface end to end: `SparseIdIndex::
/// locate` picks the SERIES_IDS window that must hold `target` if present,
/// `find_index_in_window` searches that crc-verified window, `chunk_for`
/// maps the resolved absolute index to its meta chunk, and
/// `decode_chunk_runs` decodes just that series' runs from the crc-verified,
/// decompressed frame. Every step returns a typed [`SegmentError`] on bad
/// bytes; the contract this checks, mirroring [`full_decode`], is that the
/// whole chain returns (a `ProbeOutcome` or an `Err`) instead of panicking.
fn point_probe(bytes: &[u8], target: &[u8; 16]) -> Result<ProbeOutcome, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits)?;
    let footer = &loc.footer;
    let (Some(idx_range), Some(ids_range), Some(chunks_range)) = (
        find_section_range(footer, SECTION_KIND_SERIES_IDX),
        find_section_range(footer, SECTION_KIND_SERIES_IDS),
        find_section_range(footer, SECTION_KIND_SERIES_META_CHUNKS),
    ) else {
        return Ok(ProbeOutcome::NoSparseIndex);
    };

    let idx_bytes = range_bytes(bytes, idx_range)?;
    let index = parse_series_idx(idx_bytes)?;
    let Some(window) = index.locate(target) else {
        return Ok(ProbeOutcome::Absent);
    };

    let abs_window_offset = ids_range
        .0
        .checked_add(window.section_offset)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    let window_bytes = range_bytes(bytes, (abs_window_offset, window.len))?;
    verify_id_window(window_bytes, &window)?;
    let Some(abs_index) = find_index_in_window(window_bytes, window.first_index, target)? else {
        return Ok(ProbeOutcome::Absent);
    };

    let Some(chunk) = index.chunk_for(abs_index) else {
        return Ok(ProbeOutcome::Absent);
    };
    let abs_frame_offset = chunks_range
        .0
        .checked_add(chunk.frame_offset)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    let frame_stored = range_bytes(bytes, (abs_frame_offset, chunk.frame_stored_len))?;
    let frame_raw = verify_and_decompress_chunk_frame(frame_stored, &chunk, limits)?;
    let runs = decode_chunk_runs(&frame_raw, chunk.row_in_chunk, footer, limits)?;
    Ok(ProbeOutcome::Found(runs))
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

fn identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x5A; 16],
        shard: 9,
        writer_id: "fuzz-seed-writer".to_string(),
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

fn sample_histogram_value(seed: i32) -> HistogramValue {
    let zero_count = 1u64;
    let positive = vec![2u64, (seed.unsigned_abs()) as u64 + 1];
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

/// A mixed scalar+histogram batch, emitted straight through the raw-sample v5
/// adapter. `n` picks the emission mode: small stays below the sparse
/// threshold, large crosses it.
fn v5_object(n: usize, hist_every: usize) -> Vec<u8> {
    let base = 1_600_000_000_000_000_000i64;
    let mut series = Vec::with_capacity(n);
    for i in 0..n {
        let mut id = [0u8; 16];
        id[0] = (i % 11) as u8;
        id[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9).to_be_bytes());
        id[6] = (i >> 8) as u8;
        id[7] = i as u8;
        let ls = labels(&[
            (METRIC_NAME_LABEL, "v5_seed"),
            ("job", &format!("j{}", i % 5)),
            ("inst", &format!("i{i}")),
        ]);
        let values = if hist_every > 0 && i % hist_every == 0 {
            SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: base + i as i64,
                value: sample_histogram_value(i as i32),
            }])
        } else {
            SeriesValues::Scalar(vec![
                Sample {
                    ts_ns: base + i as i64,
                    value: i as f64,
                },
                Sample {
                    ts_ns: base + i as i64 + 500,
                    value: i as f64 + 0.25,
                },
            ])
        };
        series.push(SeriesInputV3 {
            series_id: SeriesId(id),
            labels: ls,
            values,
        });
    }
    SegmentWriter::write_histograms(series, identity(), bounds())
        .expect("write v5 seed")
        .bytes
        .to_vec()
}

/// Two real v5 objects: one below the 4096 sparse-emission threshold (the
/// v4-shaped whole catalog under a version-5 trailer) and one above it (the
/// SERIES_IDX + chunked SERIES_META sparse form), each with histogram runs
/// mixed in. Built once and cached.
fn v5_seeds() -> &'static [Vec<u8>] {
    static SEEDS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    SEEDS.get_or_init(|| vec![v5_object(64, 3), v5_object(4096, 7)])
}

fn seed_count() -> usize {
    v5_seeds().len()
}

fn seed(index: usize) -> &'static [u8] {
    &v5_seeds()[index % seed_count()]
}

/// The above-sparse-threshold seed (`v5_object(4096, ..)`, `v5_seeds()[1]`):
/// the only cached seed that carries SERIES_IDX + SERIES_META_CHUNKS, the
/// sections the point-probe path reads.
fn sparse_seed() -> &'static [u8] {
    &v5_seeds()[1]
}

/// A real series id from the unmutated sparse seed's own catalog, so
/// `locate` is given a genuine target rather than an id that would trivially
/// read `Absent` even on clean bytes. Built once and cached.
fn sparse_seed_present_id() -> [u8; 16] {
    static ID: OnceLock<[u8; 16]> = OnceLock::new();
    *ID.get_or_init(|| {
        let limits = ReaderLimits::default();
        let loc = open_from_full(sparse_seed(), limits).expect("sparse seed opens");
        let entries =
            decode_catalog_v5(&loc.footer, sparse_seed(), limits).expect("sparse seed decodes");
        entries[entries.len() / 2].entry.series_id.0
    })
}

/// Single-bit flip at an in-bounds offset, then truncate to an in-bounds
/// length: the seed-corpus mutation operators the finding names.
fn mutate(bytes: &[u8], bit: usize, do_truncate: bool, truncate_to: usize) -> Vec<u8> {
    let mut out = bytes.to_vec();
    if !out.is_empty() {
        let byte = (bit / 8) % out.len();
        out[byte] ^= 1u8 << (bit % 8);
    }
    if do_truncate && !out.is_empty() {
        out.truncate(truncate_to % out.len());
    }
    out
}

#[test]
fn v5_seeds_decode_cleanly_unmutated() {
    for i in 0..seed_count() {
        assert!(
            full_decode(seed(i)).is_ok(),
            "unmutated v5 seed {i} must decode cleanly"
        );
    }
}

#[test]
fn sparse_seed_point_probe_finds_known_id_unmutated() {
    // Wiring sanity check before fuzzing it: a real id must resolve Found
    // with at least one run, and a fabricated id outside the generated id
    // space (every real id's first byte is `i % 11`, so 0xFF never occurs)
    // must resolve Absent, never Found and never an error.
    let target = sparse_seed_present_id();
    match point_probe(sparse_seed(), &target).expect("point probe on clean bytes") {
        ProbeOutcome::Found(runs) => {
            assert!(!runs.is_empty(), "known-present id must resolve runs");
        }
        other => panic!("known-present id must resolve Found, got {other:?}"),
    }

    let absent = [0xFFu8; 16];
    match point_probe(sparse_seed(), &absent).expect("point probe on clean bytes") {
        ProbeOutcome::Absent => {}
        other => panic!("fabricated id must resolve Absent, got {other:?}"),
    }
}

#[test]
fn retired_version_objects_are_rejected_with_typed_error() {
    // ADR-0027/ADR-0092: a stray pre-v7 object must stay detectably foreign.
    // Each of these is a real v1/v2/v3/v5/v6 golden; `parse_footer` rejects the
    // non-7 trailer version before touching a section, so the decode never
    // gets far enough to observe the historical v3 count inconsistency -- it
    // is now unreachable, as the removal predicted. The v6 golden proves the
    // just-retired version is rejected rather than half-parsed, exactly the way
    // every earlier retired version is.
    for (i, bytes) in REJECTION_SEEDS.iter().enumerate() {
        match open_from_full(bytes, ReaderLimits::default()) {
            Err(SegmentError::UnsupportedVersion(v)) => {
                assert!(
                    [1, 2, 3, 5, 6].contains(&v),
                    "rejection seed {i} version {v}"
                );
            }
            other => panic!("rejection seed {i} must be UnsupportedVersion, got {other:?}"),
        }
        // And the whole pipeline surfaces the same typed error, never a panic
        // or a wrong decode.
        assert!(matches!(
            full_decode(bytes),
            Err(SegmentError::UnsupportedVersion(_))
        ));
    }
}

#[test]
fn a_v4_trailer_object_fails_closed() {
    // v4 has no checked-in golden and can no longer be written, so synthesize
    // the retired-version case by stamping a v4 version onto a real v5 object.
    // The version field feeds the footer_crc, so this fails closed as either
    // UnsupportedVersion or FooterCrcMismatch -- a typed error either way,
    // never a parse attempt.
    let mut obj = v5_object(32, 0);
    let total = obj.len();
    obj[total - 8] = 4;
    obj[total - 7] = 0;
    assert!(matches!(
        full_decode(&obj),
        Err(SegmentError::UnsupportedVersion(_)) | Err(SegmentError::FooterCrcMismatch)
    ));
}

proptest! {
    /// A v5 seed with a single bit flipped and/or truncated must yield a typed
    /// error or a valid decode through the whole pipeline, never a panic.
    #[test]
    fn seed_single_bit_and_truncation_never_panics(
        index in 0usize..seed_count(),
        bit in any::<usize>(),
        do_truncate in any::<bool>(),
        truncate_to in any::<usize>(),
    ) {
        let corrupt = mutate(seed(index), bit, do_truncate, truncate_to);
        let _ = full_decode(&corrupt);
    }

    /// Fully arbitrary bytes fed to the whole pipeline must never panic.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = full_decode(&bytes);
    }

    /// The sparse point-probe chain (`locate` -> `find_index_in_window` ->
    /// `chunk_for` -> `decode_chunk_runs`) parses SERIES_IDX and the
    /// SERIES_META_CHUNKS frames through APIs `decode_catalog_v5`'s
    /// mutation coverage above never calls. A single bit flip and/or
    /// truncation of the sparse seed, probed for a real id that seed
    /// carries, must yield a `ProbeOutcome` or a typed error, never a panic
    /// -- the same criterion `seed_single_bit_and_truncation_never_panics`
    /// checks for the whole-catalog path.
    #[test]
    fn point_probe_single_bit_and_truncation_never_panics(
        bit in any::<usize>(),
        do_truncate in any::<bool>(),
        truncate_to in any::<usize>(),
    ) {
        let corrupt = mutate(sparse_seed(), bit, do_truncate, truncate_to);
        let _ = point_probe(&corrupt, &sparse_seed_present_id());
    }

    /// Fully arbitrary bytes fed through the point-probe chain (mirroring
    /// `arbitrary_bytes_never_panic` for the whole-catalog path) must never
    /// panic.
    #[test]
    fn point_probe_arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = point_probe(&bytes, &sparse_seed_present_id());
    }

    /// `parse_footer` reads the trailer from an arbitrary suffix with an
    /// arbitrary declared total size; it must never panic regardless of how
    /// the two disagree.
    #[test]
    fn parse_footer_arbitrary_tail_never_panics(
        total_size in any::<u64>(),
        tail in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let _ = parse_footer(total_size, &tail);
    }

    /// `open_from_full` on a prefix of a real object (a common partial-read
    /// shape) must never panic; it returns `Truncated`/typed errors.
    #[test]
    fn open_from_full_on_prefix_of_seed_never_panics(
        index in 0usize..seed_count(),
        take in any::<usize>(),
    ) {
        let s = seed(index);
        let n = if s.is_empty() { 0 } else { take % (s.len() + 1) };
        let _ = open_from_full(&s[..n], ReaderLimits::default());
    }
}
