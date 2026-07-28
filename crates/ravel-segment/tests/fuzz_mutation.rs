//! Corrupt-input mutation harness for the RSEG byte parsers (issue #82,
//! audit finding a11-F05). Object-storage bytes are subject to bit rot and
//! partial writes; every decoder that reads them must return a typed error
//! or a valid decode, never panic and never yield wrong data
//! (docs/segment-format.md, CLAUDE.md testing patterns).
//!
//! Scope and non-overlap: tests/reader_v2.rs already runs *exhaustive
//! deterministic* single-byte-flip and truncation sweeps over freshly
//! written v1/v2 objects (its sections 9-11), and tests/roundtrip.rs covers
//! v1 byte flips. This file is the *randomized, seed-corpus* counterpart the
//! finding asks for: it seeds from the checked-in golden fixtures (the
//! frozen-format tripwires) and applies proptest-driven single-bit and
//! truncation mutations, plus fully arbitrary byte vectors, through the
//! public reader entry points. The per-codec bit-stream fuzzing (varint,
//! gorilla, delta-varint) lives in those modules' own `fuzz_mutation` test
//! modules, because the section/page CRC gates on this whole-object path
//! shield the bit codecs from most mutated input.
//!
//! Runs on the pinned stable toolchain (proptest only, no cargo-fuzz).
//! Case count honours `PROPTEST_CASES`; the CI fuzz job raises it.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_segment::{
    ReaderLimits, SegmentError, SeriesEntry, ValueKind, decode_catalog, decode_catalog_v2,
    decode_catalog_v3, decode_histogram_pages, decode_pages, open_from_full, parse_footer,
    plan_ranges, plan_ranges_v3,
};

// Golden fixtures reused as fuzz seeds: the same frozen-format objects the
// golden-bytes regression tests pin. One v1 object, the full v2 set, and
// (C5, RSEG v3 phase 4, issue #137) the full v3 set -- scalar-only,
// histogram-only, and mixed segments, so the seed corpus exercises
// `full_decode`'s new version-3 arm the same way it already does 1 and 2.
//
// Two of the v3 fixtures (indices 8 and 10, see `EXPECTED_UNMUTATED_ERROR`
// below) are frozen-valid byte streams whose histogram *values* are
// internally inconsistent (count less than the sum of the buckets it
// claims to hold), inherited as-is from golden_bytes_v3.rs's fixture data
// (phase C3, already merged). golden_bytes_v3.rs only ever compares writer
// output bytes against these fixtures; nothing before this ticket ran them
// through the validating read path, so this is the first time that data
// gets decoded end to end. `decode_histogram_record`'s count-consistency
// check correctly rejects both. That looks like a latent bug in the C3
// fixture-generating series data (an arithmetic slip in dense_spans_series,
// and a stray `f64::INFINITY` bucket in float_histogram_series), not a
// reader bug -- out of this ticket's scope to change, so it is reported
// rather than fixed. This harness pins the current, correct reader
// behaviour on the frozen bytes instead of asserting a clean decode that
// would not reflect reality.
const SEEDS: &[&[u8]] = &[
    include_bytes!("fixtures/golden_v1_a3.bin"),
    include_bytes!("fixtures/golden_v2_empty.bin"),
    include_bytes!("fixtures/golden_v2_one_sample.bin"),
    include_bytes!("fixtures/golden_v2_single_schema.bin"),
    include_bytes!("fixtures/golden_v2_multi_schema.bin"),
    include_bytes!("fixtures/golden_v2_gorilla_only.bin"),
    include_bytes!("fixtures/golden_v2_raw_f64_padding.bin"),
    include_bytes!("fixtures/golden_v3_integer_histogram.bin"),
    include_bytes!("fixtures/golden_v3_float_histogram.bin"),
    include_bytes!("fixtures/golden_v3_custom_boundaries.bin"),
    include_bytes!("fixtures/golden_v3_dense_spans.bin"),
    include_bytes!("fixtures/golden_v3_sparse_spans.bin"),
    include_bytes!("fixtures/golden_v3_one_sample_histogram.bin"),
    include_bytes!("fixtures/golden_v3_mixed_scalar_and_histogram.bin"),
];

const LABEL_DICT: u32 = 1;
const SERIES_TABLE: u32 = 2;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;

/// Bounds-checked section slice: returns a typed error rather than panicking
/// on an out-of-range range, so the harness itself can never panic even if a
/// mutation slips past validation with an inconsistent footer.
fn section_bytes<'a>(
    bytes: &'a [u8],
    footer: &ravel_segment::Footer,
    kind: u32,
) -> Result<&'a [u8], SegmentError> {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    let start = usize::try_from(section.offset).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let len = usize::try_from(section.len).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let end = start
        .checked_add(len)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    bytes
        .get(start..end)
        .ok_or(SegmentError::SectionOutOfBounds)
}

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

/// Drive the whole public read pipeline (footer -> validate -> catalog ->
/// plan -> page decode) over complete object bytes, dispatching on the
/// trailer version the object reports. Any failure is a typed
/// [`SegmentError`]; the contract this harness checks is that the function
/// returns (Ok or Err) instead of panicking, for every input.
fn full_decode(bytes: &[u8]) -> Result<usize, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits)?;
    if loc.version == 3 {
        return full_decode_v3(bytes, limits);
    }
    let entries = match loc.version {
        1 => {
            let dict = section_bytes(bytes, &loc.footer, LABEL_DICT)?;
            let table = section_bytes(bytes, &loc.footer, SERIES_TABLE)?;
            decode_catalog(&loc.footer, dict, table, limits)?
        }
        2 => {
            let dict = section_bytes(bytes, &loc.footer, LABEL_DICT)?;
            let ids = section_bytes(bytes, &loc.footer, SERIES_IDS)?;
            let meta = section_bytes(bytes, &loc.footer, SERIES_META)?;
            decode_catalog_v2(&loc.footer, dict, ids, meta, limits)?
        }
        // open_from_full already rejects any other version.
        _ => return Err(SegmentError::UnsupportedVersion(loc.version)),
    };

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = plan_ranges(&loc.footer, &selected)?;
    let mut total = 0usize;
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = range_bytes(bytes, range.ts_range)?;
        let val_bytes = range_bytes(bytes, range.val_range)?;
        let samples = decode_pages(entry, ts_bytes, val_bytes, limits)?;
        total += samples.len();
    }
    Ok(total)
}

/// v3's leg of `full_decode` (C5, RSEG v3 phase 4, issue #137): same whole-
/// pipeline shape as the v1/v2 arms above, but each entry's `value_kind`
/// picks scalar (`decode_pages`) or histogram (`decode_histogram_pages`)
/// decode, per docs/rseg-v3-plan.md section 3.4/3.5.
fn full_decode_v3(bytes: &[u8], limits: ReaderLimits) -> Result<usize, SegmentError> {
    let loc = open_from_full(bytes, limits)?;
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT)?;
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS)?;
    let meta = section_bytes(bytes, &loc.footer, SERIES_META)?;
    let entries = decode_catalog_v3(&loc.footer, dict, ids, meta, limits)?;

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = plan_ranges_v3(&loc.footer, &selected)?;
    let mut total = 0usize;
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = range_bytes(bytes, range.ts_range)?;
        total += match entry.value_kind {
            ValueKind::Scalar => {
                let val_bytes = range_bytes(bytes, range.val_range)?;
                decode_pages(entry, ts_bytes, val_bytes, limits)?.len()
            }
            ValueKind::Histogram => {
                let hist_bytes = range_bytes(bytes, range.hist_range)?;
                decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)?.len()
            }
        };
    }
    Ok(total)
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

fn seed(index: usize) -> &'static [u8] {
    SEEDS[index % SEEDS.len()]
}

/// The two known-inconsistent v3 fixtures (see the `SEEDS` doc comment) and
/// the specific typed error their unmutated bytes must decode to. Every
/// other index must decode cleanly.
fn expected_unmutated_error(index: usize) -> Option<SegmentError> {
    match index {
        8 | 10 => Some(SegmentError::HistogramCountInconsistent),
        _ => None,
    }
}

proptest! {
    /// Every golden fixture must decode to a deterministic, known outcome
    /// unmutated: proves the seeds are live and the pipeline helper
    /// actually exercises the decoders (a harness that errored on its own
    /// seeds in an unexpected way would prove nothing). Almost every seed
    /// must decode cleanly; the two pre-existing inconsistent v3 fixtures
    /// (see `expected_unmutated_error`) must decode to the specific typed
    /// error the reader's validation is documented to raise for them.
    #[test]
    fn seeds_decode_unmutated(index in 0usize..SEEDS.len()) {
        let result = full_decode(seed(index));
        match expected_unmutated_error(index) {
            Some(expected) => prop_assert_eq!(result, Err(expected)),
            None => prop_assert!(result.is_ok()),
        }
    }

    /// A golden fixture with a single bit flipped and/or truncated must
    /// yield a typed error or a valid decode through the whole pipeline,
    /// never a panic.
    #[test]
    fn seed_single_bit_and_truncation_never_panics(
        index in 0usize..SEEDS.len(),
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

    /// `open_from_full` on a suffix of a real object (a common partial-read
    /// shape) must never panic; it returns `Truncated`/typed errors.
    #[test]
    fn open_from_full_on_prefix_of_seed_never_panics(
        index in 0usize..SEEDS.len(),
        take in any::<usize>(),
    ) {
        let s = seed(index);
        let n = if s.is_empty() { 0 } else { take % (s.len() + 1) };
        let _ = open_from_full(&s[..n], ReaderLimits::default());
    }
}
