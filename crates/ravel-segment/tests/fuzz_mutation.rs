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
    CompactionMetaV4, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue,
    IngestBounds, ReaderLimits, ResetHint, RunInputV4, RunValuePageV4, SegmentError,
    SegmentIdentity, SegmentWriter, SeriesEntry, SeriesEntryV4, SeriesInput, SeriesInputV3,
    SeriesInputV4, SeriesValues, ValueKind, decode_catalog, decode_catalog_v2, decode_catalog_v3,
    decode_catalog_v4, decode_catalog_v5, decode_histogram_pages, decode_pages,
    decode_run_histogram_pages, decode_run_pages_soa, open_from_full, parse_footer, plan_ranges,
    plan_ranges_v3, plan_ranges_v4,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};
use std::sync::OnceLock;

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
//
// v4 (ADR-0018, docs/compaction-retention-plan.md section 4) has no golden
// fixture file: unlike v1-v3, its section bytes cannot be hand-assembled
// byte-for-byte independent of the writer, since every run's TS/VAL/HIST
// page must be a verbatim, page-crc-valid copy from a real source segment
// (writer.rs's own precedent, `histogram_run_reuses_real_v3_hist_page_
// bytes_verbatim`). `v4_seeds` builds a handful of real v4 objects at
// runtime instead and appends them after the static seeds (see `seed`).
const STATIC_SEEDS: &[&[u8]] = &[
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
    if loc.version == 4 {
        return full_decode_v4(bytes, limits);
    }
    if loc.version == 5 {
        return full_decode_v5(bytes, limits);
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

/// v4's leg of `full_decode`: same whole-pipeline shape as the v3 arm
/// above, but iterated per run instead of per series (docs/compaction-
/// retention-plan.md section 4), since a v4 series folds an arbitrary
/// number of runs and each has its own TS/VAL-or-HIST page range.
fn full_decode_v4(bytes: &[u8], limits: ReaderLimits) -> Result<usize, SegmentError> {
    let loc = open_from_full(bytes, limits)?;
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT)?;
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS)?;
    let meta = section_bytes(bytes, &loc.footer, SERIES_META)?;
    let entries = decode_catalog_v4(&loc.footer, dict, ids, meta, limits)?;
    decode_all_runs_v4(bytes, &loc.footer, &entries, limits)
}

/// v5's leg of `full_decode` (ADR-0026): the catalog comes from
/// `decode_catalog_v5` (which reassembles the chunked SERIES_META, or
/// delegates to v4 below the sparse threshold), then the per-run page decode
/// is identical to v4 -- v5 changes only the catalog layout, not the pages.
fn full_decode_v5(bytes: &[u8], limits: ReaderLimits) -> Result<usize, SegmentError> {
    let loc = open_from_full(bytes, limits)?;
    let entries = decode_catalog_v5(&loc.footer, bytes, limits)?;
    decode_all_runs_v4(bytes, &loc.footer, &entries, limits)
}

/// Plans and decodes every run of every entry through the v4 page path,
/// shared by `full_decode_v4` and `full_decode_v5` (v5 pages are v4 pages).
fn decode_all_runs_v4(
    bytes: &[u8],
    footer: &ravel_segment::Footer,
    entries: &[SeriesEntryV4],
    limits: ReaderLimits,
) -> Result<usize, SegmentError> {
    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = plan_ranges_v4(footer, &selected)?;
    let mut total = 0usize;
    for entry in entries {
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

fn v4_fixture_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x5A; 16],
        shard: 9,
        writer_id: "fuzz-v4-seed-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn v4_fixture_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 100_000,
    }
}

fn v4_fixture_meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 3,
        input_set_hash: [0x22; 32],
        part_index: 1,
        level: 2,
    }
}

/// Wraps one v1-written scalar series' own TS_PAGES/VAL_PAGES bytes,
/// verbatim, as a single run -- the same technique tests/reader_v4.rs's
/// `scalar_run_from_v1` uses, since page crc32c is bound to
/// series_id+enc+comp+payload and must stay under the same series_id.
fn v4_scalar_run_from_v1(
    created_unix_ns: i64,
    series_id: SeriesId,
    samples: Vec<Sample>,
) -> RunInputV4 {
    let min_ts_ns = samples.iter().map(|s| s.ts_ns).min().expect("non-empty");
    let max_ts_ns = samples.iter().map(|s| s.ts_ns).max().expect("non-empty");
    let sample_count = samples.len() as u32;
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id,
            labels: labels(&[(METRIC_NAME_LABEL, "fuzz_seed_scalar")]),
            samples,
        }],
        v4_fixture_identity(),
        v4_fixture_bounds(),
    )
    .expect("writes v1 fixture for v4 seed");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("parses v1 fixture footer");
    let ts_page = section_bytes(bytes, &loc.footer, 3)
        .expect("ts section")
        .to_vec();
    let val_page = section_bytes(bytes, &loc.footer, 4)
        .expect("val section")
        .to_vec();
    RunInputV4 {
        created_unix_ns,
        writer_epoch: 1,
        writer_seq: 1,
        min_ts_ns,
        max_ts_ns,
        sample_count,
        ts_page,
        value_page: RunValuePageV4::Scalar(val_page),
    }
}

/// Histogram counterpart of `v4_scalar_run_from_v1`, sourcing its run's
/// verbatim TS_PAGES/HIST_PAGES bytes from a real v3 write.
fn v4_histogram_run_from_v3(
    created_unix_ns: i64,
    series_id: SeriesId,
    samples: Vec<HistogramSample>,
) -> RunInputV4 {
    let min_ts_ns = samples.iter().map(|s| s.ts_ns).min().expect("non-empty");
    let max_ts_ns = samples.iter().map(|s| s.ts_ns).max().expect("non-empty");
    let sample_count = samples.len() as u32;
    let written = SegmentWriter::write_v3(
        vec![SeriesInputV3 {
            series_id,
            labels: labels(&[(METRIC_NAME_LABEL, "fuzz_seed_hist")]),
            values: SeriesValues::Histogram(samples),
        }],
        v4_fixture_identity(),
        v4_fixture_bounds(),
    )
    .expect("writes v3 fixture for v4 seed");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("parses v3 fixture footer");
    let ts_page = section_bytes(bytes, &loc.footer, 3)
        .expect("ts section")
        .to_vec();
    let hist_page = section_bytes(bytes, &loc.footer, 7)
        .expect("hist section")
        .to_vec();
    RunInputV4 {
        created_unix_ns,
        writer_epoch: 1,
        writer_seq: 1,
        min_ts_ns,
        max_ts_ns,
        sample_count,
        ts_page,
        value_page: RunValuePageV4::Histogram(hist_page),
    }
}

fn v4_sample_histogram_value(seed: i32) -> HistogramValue {
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

/// Three real v4 objects covering the shapes v1-v3 can't express: a
/// single-run scalar series, a single-run histogram series, and a series
/// with two runs of differing provenance -- built once and cached, since
/// each build performs two real segment writes (the source fixture plus
/// the v4 object itself).
fn v4_seeds() -> &'static [Vec<u8>] {
    static SEEDS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    SEEDS.get_or_init(|| {
        let scalar_single = SegmentWriter::write_v4(
            vec![SeriesInputV4 {
                series_id: SeriesId([0x51; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "v4_scalar_single"), ("zone", "a")]),
                runs: vec![v4_scalar_run_from_v1(
                    100,
                    SeriesId([0x51; 16]),
                    vec![
                        Sample {
                            ts_ns: 0,
                            value: 1.0,
                        },
                        Sample {
                            ts_ns: 1_000,
                            value: 1.5,
                        },
                    ],
                )],
            }],
            v4_fixture_identity(),
            v4_fixture_bounds(),
            v4_fixture_meta(),
        )
        .expect("writes v4 scalar single-run seed");

        let histogram_single = SegmentWriter::write_v4(
            vec![SeriesInputV4 {
                series_id: SeriesId([0x52; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "v4_hist_single"), ("zone", "b")]),
                runs: vec![v4_histogram_run_from_v3(
                    100,
                    SeriesId([0x52; 16]),
                    vec![HistogramSample {
                        ts_ns: 0,
                        value: v4_sample_histogram_value(1),
                    }],
                )],
            }],
            v4_fixture_identity(),
            v4_fixture_bounds(),
            v4_fixture_meta(),
        )
        .expect("writes v4 histogram single-run seed");

        let scalar_id = SeriesId([0x53; 16]);
        let hist_id = SeriesId([0x54; 16]);
        let mixed_multi_run = SegmentWriter::write_v4(
            vec![
                SeriesInputV4 {
                    series_id: scalar_id,
                    labels: labels(&[(METRIC_NAME_LABEL, "v4_scalar_multi"), ("zone", "c")]),
                    runs: vec![
                        v4_scalar_run_from_v1(
                            200,
                            scalar_id,
                            vec![Sample {
                                ts_ns: 2_000,
                                value: 2.0,
                            }],
                        ),
                        v4_scalar_run_from_v1(
                            100,
                            scalar_id,
                            vec![Sample {
                                ts_ns: 0,
                                value: 1.0,
                            }],
                        ),
                    ],
                },
                SeriesInputV4 {
                    series_id: hist_id,
                    labels: labels(&[(METRIC_NAME_LABEL, "v4_hist_multi"), ("zone", "d")]),
                    runs: vec![v4_histogram_run_from_v3(
                        150,
                        hist_id,
                        vec![HistogramSample {
                            ts_ns: 0,
                            value: v4_sample_histogram_value(2),
                        }],
                    )],
                },
            ],
            v4_fixture_identity(),
            v4_fixture_bounds(),
            v4_fixture_meta(),
        )
        .expect("writes v4 mixed multi-run seed");

        vec![
            scalar_single.bytes.as_ref().to_vec(),
            histogram_single.bytes.as_ref().to_vec(),
            mixed_multi_run.bytes.as_ref().to_vec(),
        ]
    })
}

/// Builds a batch of single-run v4 inputs by writing one v3 object over the
/// whole batch and slicing each series' verbatim TS/VAL-or-HIST page bytes,
/// the scalable counterpart of `v4_scalar_run_from_v1` (which writes one
/// object per series). `hist_every > 0` makes every `hist_every`-th series a
/// histogram series, so the v5 seed exercises HIST runs.
fn v5_inputs(n: usize, hist_every: usize) -> Vec<SeriesInputV4> {
    let base = 1_600_000_000_000_000_000i64;
    let mut v3: Vec<SeriesInputV3> = Vec::with_capacity(n);
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
                value: v4_sample_histogram_value(i as i32),
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
        v3.push(SeriesInputV3 {
            series_id: SeriesId(id),
            labels: ls,
            values,
        });
    }

    let written = SegmentWriter::write_v3(v3, v4_fixture_identity(), v4_fixture_bounds())
        .expect("write v3 source for v5 seed");
    let obj = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v3 source");
    let footer = &loc.footer;
    let dict = section_bytes(obj, footer, 1).expect("dict");
    let ids = section_bytes(obj, footer, 5).expect("ids");
    let meta = section_bytes(obj, footer, 6).expect("meta");
    let entries = decode_catalog_v3(footer, dict, ids, meta, limits).expect("decode v3 source");

    let ts_sec = footer.sections.iter().find(|s| s.kind == 3).expect("ts");
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
                    let s = val_sec.expect("val section");
                    let (o, l) = e.val_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Scalar(obj[a..a + l as usize].to_vec())
                }
                ValueKind::Histogram => {
                    let s = hist_sec.expect("hist section");
                    let (o, l) = e.hist_page;
                    let a = (s.offset + o) as usize;
                    RunValuePageV4::Histogram(obj[a..a + l as usize].to_vec())
                }
            };
            SeriesInputV4 {
                series_id: e.series_id,
                labels: e.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 100 + (e.min_ts_ns % 61),
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

/// Two real v5 objects: one below the 4096 sparse-emission threshold (the
/// v4-shaped whole catalog under a version-5 trailer) and one above it (the
/// SERIES_IDX + chunked SERIES_META sparse form), the latter with histogram
/// runs mixed in. Built once and cached.
fn v5_seeds() -> &'static [Vec<u8>] {
    static SEEDS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    SEEDS.get_or_init(|| {
        let below = SegmentWriter::write_v5(
            v5_inputs(64, 3),
            v4_fixture_identity(),
            v4_fixture_bounds(),
            v4_fixture_meta(),
        )
        .expect("writes below-threshold v5 seed");
        let sparse = SegmentWriter::write_v5(
            v5_inputs(4096, 7),
            v4_fixture_identity(),
            v4_fixture_bounds(),
            v4_fixture_meta(),
        )
        .expect("writes sparse v5 seed");
        vec![
            below.bytes.as_ref().to_vec(),
            sparse.bytes.as_ref().to_vec(),
        ]
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

fn seed_count() -> usize {
    STATIC_SEEDS.len() + v4_seeds().len() + v5_seeds().len()
}

fn seed(index: usize) -> &'static [u8] {
    let index = index % seed_count();
    if index < STATIC_SEEDS.len() {
        return STATIC_SEEDS[index];
    }
    let index = index - STATIC_SEEDS.len();
    if index < v4_seeds().len() {
        &v4_seeds()[index]
    } else {
        &v5_seeds()[index - v4_seeds().len()]
    }
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
    fn seeds_decode_unmutated(index in 0usize..seed_count()) {
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

    /// `open_from_full` on a suffix of a real object (a common partial-read
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
