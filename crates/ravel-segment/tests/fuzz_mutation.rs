//! Corrupt-input mutation harness for the RSEG byte parsers (issue #82,
//! audit finding a11-F05). Object-storage bytes are subject to bit rot and
//! partial writes; every decoder that reads them must return a typed error
//! or a valid decode, never panic and never yield wrong data
//! (docs/segment-format.md, CLAUDE.md testing patterns).
//!
//! ADR-0027 leaves v5 the only readable version. The live seed corpus is a
//! pair of real v5 objects (below and above the sparse-emission threshold);
//! proptest-driven single-bit and truncation mutations, plus fully arbitrary
//! byte vectors, run through the public v5 read pipeline and must never
//! panic.
//!
//! Retired-version rejection: three checked-in pre-v5 goldens (v1, v2, v3)
//! are kept only as rejection seeds. `parse_footer` rejects a non-5 trailer
//! version before it ever touches a section, so each decodes to a typed
//! `UnsupportedVersion`, never a parse attempt or panic. This is why the two
//! historically inconsistent v3 histogram fixtures (dense_spans,
//! float_histogram) are gone: with v3 no longer decoded, their
//! `HistogramCountInconsistent` outcome is unreachable, exactly as ADR-0027
//! predicted.
//!
//! Runs on the pinned stable toolchain (proptest only, no cargo-fuzz).
//! Case count honours `PROPTEST_CASES`; the CI fuzz job raises it.
#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ReaderLimits,
    ResetHint, SegmentError, SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInputV3,
    SeriesValues, ValueKind, decode_catalog_v5, decode_run_histogram_pages, decode_run_pages_soa,
    open_from_full, parse_footer, plan_ranges_v4,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};
use std::sync::OnceLock;

/// Pre-v5 goldens kept only as retired-version rejection seeds (one per
/// retired layout that has a checked-in fixture). Each must fail closed with
/// `UnsupportedVersion`.
const REJECTION_SEEDS: &[&[u8]] = &[
    include_bytes!("fixtures/golden_v1_a3.bin"),
    include_bytes!("fixtures/golden_v2_single_schema.bin"),
    include_bytes!("fixtures/golden_v3_integer_histogram.bin"),
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
fn retired_version_objects_are_rejected_with_typed_error() {
    // ADR-0027: a stray pre-v5 object must stay detectably foreign. Each of
    // these is a real v1/v2/v3 golden; `parse_footer` rejects the non-5
    // trailer version before touching a section, so the decode never gets far
    // enough to observe the historical v3 count inconsistency -- it is now
    // unreachable, as the removal predicted.
    for (i, bytes) in REJECTION_SEEDS.iter().enumerate() {
        match open_from_full(bytes, ReaderLimits::default()) {
            Err(SegmentError::UnsupportedVersion(v)) => {
                assert!((1..=3).contains(&v), "rejection seed {i} version {v}");
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
