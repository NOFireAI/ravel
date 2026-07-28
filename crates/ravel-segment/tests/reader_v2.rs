//! Integration tests for RSEG v2 decode (docs/segment-format.md "RSEG v2
//! amendment", docs/rseg-v2-plan.md phase P3, issue #31): the
//! `decode_catalog_v2`/`decode_catalog_matching_v2` reader path, the
//! version-aware trailer/`validate_sections` dispatch, and a typed-error
//! corpus over hand-crafted corrupt/truncated v2 inputs.
//!
//! v1 fixtures and v1-focused tests live in tests/roundtrip.rs and
//! tests/golden_bytes.rs, both completely unmodified by this ticket -- see
//! those files for the proof v1 decode behavior wasn't touched.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::array::uniform16;
use proptest::prelude::*;
use ravel_proto::segment::v1::{Footer, Section};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentError, SegmentIdentity, SegmentWriter, SeriesEntry,
    SeriesInput,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

// --- shared helpers -------------------------------------------------------

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

fn slice_range(bytes: &[u8], range: (u64, u64)) -> &[u8] {
    let start = range.0 as usize;
    let end = start + range.1 as usize;
    &bytes[start..end]
}

fn section_bytes<'a>(bytes: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    slice_range(bytes, (section.offset, section.len))
}

const LABEL_DICT: u32 = 1;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;

/// One decoded series, reduced to exact bit-pattern comparison keys
/// (`normalize`'s output shape), matching tests/roundtrip.rs's
/// `NormalizedSeries` convention.
type NormalizedSeries = ([u8; 16], LabelSet, Vec<(i64, u64)>);
/// Expected per-series outcome keyed by raw series_id bytes, for the
/// roundtrip proptest below.
type ExpectedById = std::collections::HashMap<[u8; 16], (LabelSet, Vec<(i64, u64)>)>;

/// Full v2 pipeline (footer -> catalog -> plan -> decode), eager path.
/// `plan_ranges`/`decode_pages` are version-blind (they only consult
/// footer section offsets and the common `SeriesEntry` shape), so this
/// reuses the very same functions the v1 pipeline uses.
fn full_decode_v2(bytes: &[u8]) -> Result<Vec<(SeriesId, LabelSet, Vec<Sample>)>, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits)?;
    assert_eq!(loc.version, 2, "test helper only decodes v2 objects");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v2(&loc.footer, dict, ids, meta, limits)?;

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected)?;

    let mut out = Vec::with_capacity(entries.len());
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let samples = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)?;
        out.push((entry.series_id, entry.labels.clone(), samples));
    }
    Ok(out)
}

fn full_decode_v1(bytes: &[u8]) -> Result<Vec<(SeriesId, LabelSet, Vec<Sample>)>, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits)?;
    assert_eq!(loc.version, 1, "test helper only decodes v1 objects");
    let dict = section_bytes(bytes, &loc.footer, 1);
    let table = section_bytes(bytes, &loc.footer, 2);
    let entries = ravel_segment::decode_catalog(&loc.footer, dict, table, limits)?;

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected)?;

    let mut out = Vec::with_capacity(entries.len());
    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let samples = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)?;
        out.push((entry.series_id, entry.labels.clone(), samples));
    }
    Ok(out)
}

/// Dispatches to [`full_decode_v1`]/[`full_decode_v2`] based on whichever
/// version the trailer actually reports, for the P4 mutation/truncation
/// tests below that must not assume up front which version (if any
/// well-formed one at all) survives a given mutation. Never panics on the
/// dispatch itself: `open_from_full`'s own version check already restricts
/// `loc.version` to 1 or 2 before either helper's internal `assert_eq!` is
/// reached, so the `unreachable!` arm is genuinely unreachable, not a
/// disguised `expect`.
fn full_decode_dispatch(
    bytes: &[u8],
) -> Result<Vec<(SeriesId, LabelSet, Vec<Sample>)>, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits)?;
    match loc.version {
        1 => full_decode_v1(bytes),
        2 => full_decode_v2(bytes),
        other => unreachable!("open_from_full only ever returns version 1 or 2, got {other}"),
    }
}

fn normalize(entries: &[(SeriesId, LabelSet, Vec<Sample>)]) -> Vec<NormalizedSeries> {
    let mut out: Vec<_> = entries
        .iter()
        .map(|(id, labels, samples)| {
            (
                id.0,
                labels.clone(),
                samples
                    .iter()
                    .map(|s| (s.ts_ns, s.value.to_bits()))
                    .collect(),
            )
        })
        .collect();
    out.sort_by_key(|(id, _, _)| *id);
    out
}

fn test_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x5A; 16],
        shard: 4,
        writer_id: "reader-v2-test-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn test_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 1_000,
    }
}

// --- 1. Differential test: one fixture, both writers, decoded output   ---
// --- compared field by field (except val_page offsets: v2 alignment    ---
// --- padding legitimately shifts them, see the comment below).         ---

/// One logical input, used for both `write` (v1) and `write_v2` (v2).
/// Deliberately not alphabetically ordered by label name across series
/// (the same non-lexicographic pattern P2's
/// `schema_name_ord_follows_name_byte_order_not_ordinal_order` test uses):
/// series `0x01` interns "zone" first (a name that sorts last among
/// {app, region, zone}), then series `0x04` uses all three together, so a
/// schema-ordering bug in the v2 decode path would show up here, not just
/// in P2's dedicated writer-side test. Includes NaN, -0.0, and duplicate
/// timestamps.
fn differential_fixture() -> Vec<SeriesInput> {
    vec![
        SeriesInput {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "requests"), ("zone", "z1")]),
            samples: (0..12)
                .map(|i| Sample {
                    ts_ns: 1_000 + i * 13,
                    value: (i as f64) * 0.75,
                })
                .collect(),
        },
        SeriesInput {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "requests"), ("zone", "z2")]),
            samples: vec![
                Sample {
                    ts_ns: 500,
                    value: f64::NAN,
                },
                Sample {
                    ts_ns: 500,
                    value: -f64::NAN,
                },
                Sample {
                    ts_ns: 500,
                    value: -0.0,
                },
                Sample {
                    ts_ns: 900,
                    value: f64::INFINITY,
                },
            ],
        },
        SeriesInput {
            series_id: SeriesId([0x03; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "single")]),
            samples: vec![Sample {
                ts_ns: 42,
                value: 3.5,
            }],
        },
        SeriesInput {
            series_id: SeriesId([0x04; 16]),
            labels: labels(&[
                (METRIC_NAME_LABEL, "requests"),
                ("zone", "z3"),
                ("app", "a1"),
                ("region", "r1"),
            ]),
            samples: (0..5)
                .map(|i| Sample {
                    ts_ns: i * 100,
                    value: f64::from_bits(0x4010_0000_0000_0000 ^ (i as u64)),
                })
                .collect(),
        },
    ]
}

#[test]
fn v1_and_v2_decode_the_same_logical_content() {
    let v1 = SegmentWriter::write(differential_fixture(), test_identity(), test_bounds())
        .expect("v1 writes");
    let v2 = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("v2 writes");

    let limits = ReaderLimits::default();
    let loc1 = ravel_segment::open_from_full(&v1.bytes, limits).expect("v1 opens");
    let loc2 = ravel_segment::open_from_full(&v2.bytes, limits).expect("v2 opens");
    assert_eq!(loc1.version, 1);
    assert_eq!(loc2.version, 2);

    let dict1 = section_bytes(&v1.bytes, &loc1.footer, 1);
    let table1 = section_bytes(&v1.bytes, &loc1.footer, 2);
    let mut entries1 =
        ravel_segment::decode_catalog(&loc1.footer, dict1, table1, limits).expect("v1 catalog");
    entries1.sort_by_key(|e| e.series_id.0);

    let dict2 = section_bytes(&v2.bytes, &loc2.footer, LABEL_DICT);
    let ids2 = section_bytes(&v2.bytes, &loc2.footer, SERIES_IDS);
    let meta2 = section_bytes(&v2.bytes, &loc2.footer, SERIES_META);
    let mut entries2 = ravel_segment::decode_catalog_v2(&loc2.footer, dict2, ids2, meta2, limits)
        .expect("v2 catalog");
    entries2.sort_by_key(|e| e.series_id.0);

    assert_eq!(entries1.len(), entries2.len());
    for (a, b) in entries1.iter().zip(entries2.iter()) {
        assert_eq!(a.series_id, b.series_id);
        assert_eq!(a.labels, b.labels, "label set for {:?}", a.series_id);
        assert_eq!(a.sample_count, b.sample_count);
        assert_eq!(a.min_ts_ns, b.min_ts_ns);
        assert_eq!(a.max_ts_ns, b.max_ts_ns);
        // ts pages are never padding-shifted in either version.
        assert_eq!(a.ts_page, b.ts_page);
        // val_page OFFSETS may legitimately differ: v2 inserts 8-byte
        // alignment padding before VAL_RAW_F64 pages (recorded in
        // val_page_gap) that v1 never does. Lengths must still agree.
        assert_eq!(
            a.val_page.1, b.val_page.1,
            "val page length for {:?}",
            a.series_id
        );
    }

    // The real differential: decode actual samples through both pipelines
    // and compare bit-for-bit (NaN payload and -0.0 significant).
    let decoded1 = full_decode_v1(&v1.bytes).expect("v1 full decode");
    let decoded2 = full_decode_v2(&v2.bytes).expect("v2 full decode");
    assert_eq!(normalize(&decoded1), normalize(&decoded2));
}

// --- 2. Pinned-bytes test: version dispatch --------------------------

#[test]
fn parse_footer_reports_v2_version_and_rejects_unknown_versions() {
    let written = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("writes");
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&written.bytes, limits).expect("opens");
    assert_eq!(loc.version, 2, "a v2 object must report trailer version 2");

    // A v1-only reader dispatching purely on `!= 1` would reject this
    // object; the actual frozen-field behavior a v1-only reader relies on
    // is UnsupportedVersion(2), pinned here directly against parse_footer.
    let mut corrupted = written.bytes.to_vec();
    let n = corrupted.len();
    // version is trailer bytes [8..10] from the end (see
    // docs/segment-format.md trailer layout); bump it to 5, an accepted-
    // nowhere version (1/2/3/4 are all real trailer versions as of the
    // compaction-retention plan P3), and recompute nothing else --
    // parse_footer must reject on the version check before ever reaching
    // footer_crc32c.
    corrupted[n - 16 + 8] = 5;
    corrupted[n - 16 + 9] = 0;
    let err = ravel_segment::parse_footer(corrupted.len() as u64, &corrupted).unwrap_err();
    assert_eq!(err, SegmentError::UnsupportedVersion(5));
}

#[test]
fn validate_sections_rejects_unsupported_version_directly() {
    let written = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("writes");
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&written.bytes, limits).expect("opens");
    let err =
        ravel_segment::validate_sections(&loc.footer, 5, loc.footer_offset, limits).unwrap_err();
    assert_eq!(err, SegmentError::UnsupportedVersion(5));
}

// --- 3. decode_catalog_matching_v2 vs decode_catalog_v2 + select ------

/// Asserts two entry lists are field-for-field identical (`SeriesEntry`
/// has no `PartialEq`).
fn assert_entries_eq(a: &[&SeriesEntry], b: &[SeriesEntry]) {
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.series_id, y.series_id);
        assert_eq!(x.labels, y.labels);
        assert_eq!(x.sample_count, y.sample_count);
        assert_eq!(x.min_ts_ns, y.min_ts_ns);
        assert_eq!(x.max_ts_ns, y.max_ts_ns);
        assert_eq!(x.ts_page, y.ts_page);
        assert_eq!(x.val_page, y.val_page);
    }
}

/// The gap/len running sum must advance over *every* series regardless of
/// match, not just matching ones -- a lazy implementation that only
/// accumulates the running sum for matching series would still pass a
/// test where the first series matches or all gaps are zero. This fixture
/// specifically has a non-matching first series, and places a matching
/// series after one with a non-zero `val_page_gap` (guaranteed by mixing
/// multi-sample Gorilla-friendly series with 1-sample raw-fallback series,
/// mirroring golden_bytes_v2.rs's `raw_f64_with_padding_series` shape).
fn lazy_running_sum_fixture() -> Vec<SeriesInput> {
    vec![
        // Does not match ("target", "yes"); multi-sample, Gorilla-coded,
        // leaves VAL_PAGES at a running length not congruent to 0 mod 8.
        SeriesInput {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "smooth"), ("target", "no")]),
            samples: (0..7)
                .map(|i| Sample {
                    ts_ns: i * 10,
                    value: 100.0 + i as f64,
                })
                .collect(),
        },
        // Does not match; 1 sample -> raw-fallback -> needs alignment
        // padding, so the *next* series' val_page_gap is nonzero.
        SeriesInput {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "single_a"), ("target", "no")]),
            samples: vec![Sample {
                ts_ns: 5,
                value: 42.5,
            }],
        },
        // Matches; sits right after the padded series above, so decoding
        // its page range correctly requires the running sum to have
        // advanced correctly over BOTH preceding non-matching series.
        SeriesInput {
            series_id: SeriesId([0x03; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "single_b"), ("target", "yes")]),
            samples: vec![Sample {
                ts_ns: 9,
                value: -7.25,
            }],
        },
    ]
}

#[test]
fn decode_catalog_matching_v2_agrees_with_eager_select() {
    let written =
        SegmentWriter::write_v2(lazy_running_sum_fixture(), test_identity(), test_bounds())
            .expect("writes");
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&written.bytes, limits).expect("opens");
    let dict = section_bytes(&written.bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(&written.bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(&written.bytes, &loc.footer, SERIES_META);
    let entries =
        ravel_segment::decode_catalog_v2(&loc.footer, dict, ids, meta, limits).expect("catalog");

    for equals in [
        vec![],
        vec![("target", "yes")],
        vec![("target", "no")],
        vec![(METRIC_NAME_LABEL, "single_b")],
        vec![("target", "does-not-exist")],
        vec![("no-such-name", "yes")],
    ] {
        let eager = ravel_segment::select(&entries, &equals, None);
        let lazy = ravel_segment::decode_catalog_matching_v2(
            &loc.footer,
            dict,
            ids,
            meta,
            &equals,
            limits,
        )
        .expect("matching decode");
        assert_entries_eq(&eager, &lazy);
    }

    // Directly confirm the matching series' page range decoded correctly
    // (proves the running sum advanced over the two preceding series,
    // including the padded one).
    let lazy = ravel_segment::decode_catalog_matching_v2(
        &loc.footer,
        dict,
        ids,
        meta,
        &[("target", "yes")],
        limits,
    )
    .expect("matching decode");
    assert_eq!(lazy.len(), 1);
    assert_eq!(lazy[0].series_id, SeriesId([0x03; 16]));
    let selected: Vec<&SeriesEntry> = lazy.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected).expect("ranges");
    let ts_bytes = slice_range(&written.bytes, ranges[0].ts_range);
    let val_bytes = slice_range(&written.bytes, ranges[0].val_range);
    let samples =
        ravel_segment::decode_pages(&lazy[0], ts_bytes, val_bytes, limits).expect("decodes");
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].ts_ns, 9);
    assert_eq!(samples[0].value.to_bits(), (-7.25f64).to_bits());
}

// --- 4. Extreme timestamp bounds: min_ts_delta/ts_span reconstruction  ---
// --- must use i128 intermediates, not an i64-only path.                ---

#[test]
fn extreme_timestamp_bounds_reconstruct_without_false_overflow() {
    // footer.min_event_ts_ns becomes i64::MIN (series A's only sample);
    // series B's sample sits at i64::MAX, so min_ts_delta for series B is
    // u64::MAX (== i64::MAX - i64::MIN as an unsigned difference), which
    // does not fit in an i64 -- exactly the case a naive
    // `i64::try_from(delta)` (instead of reconstructing via i128) would
    // wrongly reject.
    let series = vec![
        SeriesInput {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "extreme")]),
            samples: vec![Sample {
                ts_ns: i64::MIN,
                value: 1.0,
            }],
        },
        SeriesInput {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "extreme")]),
            samples: vec![Sample {
                ts_ns: i64::MAX,
                value: 2.0,
            }],
        },
    ];
    let written = SegmentWriter::write_v2(series, test_identity(), test_bounds()).expect("writes");
    let decoded = full_decode_v2(&written.bytes).expect("decodes without false overflow");
    let mut by_id: Vec<_> = decoded.into_iter().collect();
    by_id.sort_by_key(|(id, _, _)| id.0);
    assert_eq!(by_id[0].0, SeriesId([0x01; 16]));
    assert_eq!(by_id[0].2[0].ts_ns, i64::MIN);
    assert_eq!(by_id[1].0, SeriesId([0x02; 16]));
    assert_eq!(by_id[1].2[0].ts_ns, i64::MAX);
}

// --- 5. Roundtrip proptest: write_v2 -> decode_catalog_v2 -------------

fn label_name_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,6}"
}

fn label_value_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_]{0,8}"
}

fn labelset_strategy() -> impl Strategy<Value = LabelSet> {
    (
        "[a-z_]{1,10}",
        prop::collection::vec((label_name_strategy(), label_value_strategy()), 0..5),
    )
        .prop_map(|(metric_name, extra)| {
            let mut ls = vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: metric_name,
            }];
            let mut seen = std::collections::HashSet::new();
            seen.insert(METRIC_NAME_LABEL.to_string());
            for (name, value) in extra {
                if seen.insert(name.clone()) {
                    ls.push(Label { name, value });
                }
            }
            LabelSet::new(ls).expect("no duplicate names by construction")
        })
}

// Bounded, like tests/roundtrip.rs's v1 `ts_strategy`: adjacent samples of
// the same series feed TS_DELTA_VARINT (unchanged in v2), whose delta
// accumulation is only checked to fit i64 -- two samples of the same
// series at i64::MIN and i64::MAX legitimately overflow that (a writer
// limitation shared with v1, not a v2 reader concern). The extreme-range
// reconstruction this ticket cares about (footer.min_event_ts_ns +
// min_ts_delta, potentially spanning the full i64 range *across
// series*) is covered directly and deterministically by
// `extreme_timestamp_bounds_reconstruct_without_false_overflow` above,
// with one sample per series so no intra-series delta is involved.
fn ts_strategy() -> impl Strategy<Value = i64> {
    prop_oneof![
        3 => 0i64..300,
        1 => -1_000_000_000_000i64..=1_000_000_000_000i64,
    ]
}

fn sample_value_strategy() -> impl Strategy<Value = f64> {
    prop_oneof![
        10 => any::<f64>(),
        1 => Just(f64::NAN),
        1 => Just(-f64::NAN),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        1 => Just(0.0f64),
        1 => Just(-0.0f64),
        1 => Just(f64::MIN_POSITIVE),
        1 => Just(5e-324f64),
        1 => Just(-5e-324f64),
    ]
}

fn samples_strategy() -> impl Strategy<Value = Vec<(i64, f64)>> {
    prop_oneof![
        3 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..2),
        5 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..12),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn write_v2_read_roundtrip_is_identity(
        series_data in prop::collection::vec((labelset_strategy(), samples_strategy()), 0..8),
        tenant_hash in uniform16(any::<u8>()),
        shard in any::<u32>(),
        writer_id in "[a-zA-Z0-9-]{1,20}",
        epoch in any::<u64>(),
        seq in any::<u64>(),
    ) {
        let mut series_inputs = Vec::new();
        let mut expected: ExpectedById = std::collections::HashMap::new();

        for (idx, (series_labels, samples)) in series_data.into_iter().enumerate() {
            let mut id_bytes = [0u8; 16];
            id_bytes[..8].copy_from_slice(&(idx as u64).to_be_bytes());

            let mut indexed: Vec<(usize, (i64, f64))> =
                samples.iter().copied().enumerate().collect();
            indexed.sort_by_key(|(i, (ts, _))| (*ts, *i));
            let expected_samples: Vec<(i64, u64)> = indexed
                .into_iter()
                .map(|(_, (ts, v))| (ts, v.to_bits()))
                .collect();

            if !samples.is_empty() {
                expected.insert(id_bytes, (series_labels.clone(), expected_samples));
            }

            series_inputs.push(SeriesInput {
                series_id: SeriesId(id_bytes),
                labels: series_labels,
                samples: samples
                    .into_iter()
                    .map(|(ts_ns, value)| Sample { ts_ns, value })
                    .collect(),
            });
        }

        let identity = SegmentIdentity {
            tenant_hash,
            shard,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: -1_000_000_000,
            max_ingest_ts_ns: 1_000_000_000,
        };

        let expected_series_count = expected.len();
        let written = SegmentWriter::write_v2(series_inputs, identity, bounds).expect("write succeeds");
        prop_assert_eq!(written.summary.series_count as usize, expected_series_count);

        let decoded = full_decode_v2(&written.bytes).expect("decode succeeds");
        prop_assert_eq!(decoded.len(), expected_series_count);

        for (series_id, series_labels, samples) in &decoded {
            let (exp_labels, exp_samples) = expected
                .get(&series_id.0)
                .expect("decoded series must be one we wrote");
            prop_assert_eq!(series_labels, exp_labels);
            let got: Vec<(i64, u64)> =
                samples.iter().map(|s| (s.ts_ns, s.value.to_bits())).collect();
            prop_assert_eq!(&got, exp_samples);
        }
    }
}

// --- 6. Typed-error corpus over hand-crafted corrupt/truncated v2      ---
// --- inputs. Built directly (comp = NONE, `Section`/`Footer` literals) ---
// --- rather than round-tripped through `write_v2` + zstd, per          ---
// --- docs/segment-format.md: "comp" is writer policy, so a hand-built  ---
// --- footer with `comp: 0` is a valid v2 shape the reader must accept. ---

fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn build_label_dict_bytes(strings: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in strings {
        write_uvarint(&mut buf, s.len() as u64);
        buf.extend_from_slice(s.as_bytes());
    }
    buf
}

fn build_series_ids_bytes(ids: &[[u8; 16]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(id);
    }
    buf
}

/// A structurally-valid-by-default SERIES_META specification: field-level
/// mutation surface for corpus tests, serialized by `build()` following
/// the grammar exactly (docs/segment-format.md v2 amendment). Baseline: 2
/// series, 1 schema (`name_ords = [0]`, i.e. dictionary ordinal 0, "app"),
/// each series 1 sample, zero gaps, tiny page lengths well within the
/// generous TS_PAGES/VAL_PAGES section lengths the footer builder uses.
#[derive(Clone)]
struct MetaSpec {
    count: u32,
    schemas: Vec<Vec<u64>>,
    schema_ref: Vec<u64>,
    value_ord: Vec<Vec<u64>>,
    sample_count: Vec<u64>,
    min_ts_delta: Vec<u64>,
    ts_span: Vec<u64>,
    ts_page_gap: Vec<u64>,
    ts_page_len: Vec<u64>,
    val_page_gap: Vec<u64>,
    val_page_len: Vec<u64>,
}

impl MetaSpec {
    fn baseline() -> Self {
        MetaSpec {
            count: 2,
            schemas: vec![vec![0]], // schema 0: just dict ordinal 0 ("app")
            schema_ref: vec![0, 0],
            value_ord: vec![vec![1], vec![2]], // "va", "vb"
            sample_count: vec![1, 1],
            min_ts_delta: vec![0, 0],
            ts_span: vec![0, 0],
            ts_page_gap: vec![0, 0],
            ts_page_len: vec![5, 5],
            val_page_gap: vec![0, 0],
            val_page_len: vec![8, 8],
        }
    }

    fn build(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&(self.schemas.len() as u32).to_le_bytes());
        for schema in &self.schemas {
            write_uvarint(&mut buf, schema.len() as u64);
            for n in schema {
                write_uvarint(&mut buf, *n);
            }
        }
        push_block(&mut buf, &self.schema_ref);
        let mut vo_block = Vec::new();
        for vals in &self.value_ord {
            for v in vals {
                write_uvarint(&mut vo_block, *v);
            }
        }
        write_uvarint(&mut buf, vo_block.len() as u64);
        buf.extend_from_slice(&vo_block);
        push_block(&mut buf, &self.sample_count);
        push_block(&mut buf, &self.min_ts_delta);
        push_block(&mut buf, &self.ts_span);
        push_block(&mut buf, &self.ts_page_gap);
        push_block(&mut buf, &self.ts_page_len);
        push_block(&mut buf, &self.val_page_gap);
        push_block(&mut buf, &self.val_page_len);
        buf
    }

    /// Same output as [`MetaSpec::build`], but also returns each of the 9
    /// column blocks' CONTENT byte range (i.e. excluding its `block_len`
    /// varint prefix) within the buffer, keyed by the block's name from
    /// docs/segment-format.md's v2 amendment ("then 9 column blocks..."),
    /// for the per-block-boundary mutation/truncation test below (P4,
    /// issue #32). Callers should `assert_eq!` the returned bytes against
    /// `build()`'s output so this can never silently drift out of sync
    /// with it.
    fn build_with_block_ranges(&self) -> (Vec<u8>, Vec<(&'static str, std::ops::Range<usize>)>) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&(self.schemas.len() as u32).to_le_bytes());
        for schema in &self.schemas {
            write_uvarint(&mut buf, schema.len() as u64);
            for n in schema {
                write_uvarint(&mut buf, *n);
            }
        }

        let mut ranges: Vec<(&'static str, std::ops::Range<usize>)> = Vec::new();
        push_block_tracked(&mut buf, &self.schema_ref, "schema_ref", &mut ranges);

        // value_ord's prefix is the serialized varint stream's byte length
        // (not a value count like the other 8 blocks), but the boundary
        // tracking is otherwise identical.
        let mut vo_block = Vec::new();
        for vals in &self.value_ord {
            for v in vals {
                write_uvarint(&mut vo_block, *v);
            }
        }
        write_uvarint(&mut buf, vo_block.len() as u64);
        let vo_start = buf.len();
        buf.extend_from_slice(&vo_block);
        ranges.push(("value_ord", vo_start..buf.len()));

        push_block_tracked(&mut buf, &self.sample_count, "sample_count", &mut ranges);
        push_block_tracked(&mut buf, &self.min_ts_delta, "min_ts_delta", &mut ranges);
        push_block_tracked(&mut buf, &self.ts_span, "ts_span", &mut ranges);
        push_block_tracked(&mut buf, &self.ts_page_gap, "ts_page_gap", &mut ranges);
        push_block_tracked(&mut buf, &self.ts_page_len, "ts_page_len", &mut ranges);
        push_block_tracked(&mut buf, &self.val_page_gap, "val_page_gap", &mut ranges);
        push_block_tracked(&mut buf, &self.val_page_len, "val_page_len", &mut ranges);

        (buf, ranges)
    }
}

fn push_block(buf: &mut Vec<u8>, values: &[u64]) {
    let mut block = Vec::new();
    for v in values {
        write_uvarint(&mut block, *v);
    }
    write_uvarint(buf, block.len() as u64);
    buf.extend_from_slice(&block);
}

/// Same as [`push_block`], but also records the pushed block's CONTENT
/// byte range (post length-prefix) under `name` in `ranges`. Additive
/// sibling used only by [`MetaSpec::build_with_block_ranges`]; `push_block`
/// itself (used by every existing P3 corpus test) is untouched.
fn push_block_tracked(
    buf: &mut Vec<u8>,
    values: &[u64],
    name: &'static str,
    ranges: &mut Vec<(&'static str, std::ops::Range<usize>)>,
) {
    let mut block = Vec::new();
    for v in values {
        write_uvarint(&mut block, *v);
    }
    write_uvarint(buf, block.len() as u64);
    let start = buf.len();
    buf.extend_from_slice(&block);
    ranges.push((name, start..buf.len()));
}

/// Builds a complete, self-consistent (footer, LABEL_DICT bytes,
/// SERIES_IDS bytes, SERIES_META bytes) tuple around a `MetaSpec`, all
/// sections stored uncompressed (`comp = 0`) with a correct section
/// crc32c, footer.series_count matching the spec's `count`, and generous
/// TS_PAGES/VAL_PAGES section lengths (200 bytes each) so gap/len values
/// up to that bound decode without tripping SectionOutOfBounds
/// incidentally. `ids` must be sorted ascending and `ids.len()` must equal
/// `spec.count`.
fn build_scenario(
    dict_strings: &[&str],
    ids: &[[u8; 16]],
    spec: &MetaSpec,
) -> (Footer, Vec<u8>, Vec<u8>, Vec<u8>) {
    let dict_bytes = build_label_dict_bytes(dict_strings);
    let ids_bytes = build_series_ids_bytes(ids);
    let meta_bytes = spec.build();

    let section = |kind: u32, bytes: &[u8]| Section {
        kind,
        offset: 0, // decode_catalog_v2 never reads section.offset; only .len (TS/VAL) or the passed-in bytes (LABEL_DICT/SERIES_IDS/SERIES_META) matter.
        len: bytes.len() as u64,
        crc32c: crc32c::crc32c(bytes),
        comp: 0,
        uncompressed_len: bytes.len() as u64,
    };

    let footer = Footer {
        tenant_hash: vec![0u8; 16],
        shard: 0,
        writer_id: "corpus".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
        min_event_ts_ns: 0,
        max_event_ts_ns: 0,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        sample_count: spec.sample_count.iter().sum(),
        series_count: u64::from(spec.count),
        sections: vec![
            section(LABEL_DICT, &dict_bytes),
            section(SERIES_IDS, &ids_bytes),
            section(SERIES_META, &meta_bytes),
            Section {
                kind: 3, // TS_PAGES
                offset: 0,
                len: 200,
                crc32c: 0,
                comp: 0,
                uncompressed_len: 200,
            },
            Section {
                kind: 4, // VAL_PAGES
                offset: 0,
                len: 200,
                crc32c: 0,
                comp: 0,
                uncompressed_len: 200,
            },
        ],
        ..Default::default()
    };
    (footer, dict_bytes, ids_bytes, meta_bytes)
}

fn decode(
    footer: &Footer,
    dict: &[u8],
    ids: &[u8],
    meta: &[u8],
) -> Result<Vec<SeriesEntry>, SegmentError> {
    ravel_segment::decode_catalog_v2(footer, dict, ids, meta, ReaderLimits::default())
}

#[test]
fn corpus_baseline_decodes_successfully() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let entries = decode(&footer, &dict, &ids_bytes, &meta).expect("baseline decodes");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].labels.get("app"), Some("va"));
    assert_eq!(entries[1].labels.get("app"), Some("vb"));
}

#[test]
fn corpus_non_ascending_series_ids_is_rejected() {
    let ids = [[0x02; 16], [0x01; 16]]; // descending
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SeriesIdsUnsorted)
    );
}

#[test]
fn corpus_series_ids_series_meta_count_mismatch_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.count = 3; // SERIES_META now claims 3, SERIES_IDS still has 2
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert!(matches!(err, SegmentError::SeriesCountMismatch { .. }));
}

#[test]
fn corpus_footer_series_count_mismatch_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (mut footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    footer.series_count = 99; // disagrees with both SERIES_IDS and SERIES_META (2)
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert!(matches!(err, SegmentError::SeriesCountMismatch { .. }));
}

#[test]
fn corpus_schema_ref_out_of_range_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.schema_ref = vec![0, 1]; // schema 1 doesn't exist (schema_count == 1)
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert!(matches!(err, SegmentError::SchemaRefOutOfRange(1)));
}

#[test]
fn corpus_zero_sample_count_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.sample_count = vec![1, 0];
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ZeroSampleCount)
    );
}

#[test]
fn corpus_bad_label_ordinal_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.value_ord = vec![vec![99], vec![2]]; // ordinal 99 doesn't exist in a 3-entry dict
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert!(matches!(err, SegmentError::BadOrdinal(99)));
}

#[test]
fn corpus_schema_name_ord_out_of_dict_range_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.schemas = vec![vec![99]]; // dict only has 3 entries (ordinals 0..2)
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert!(matches!(err, SegmentError::BadOrdinal(99)));
}

#[test]
fn corpus_schema_names_not_ascending_by_byte_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    // dict = ["app", "bbb", "va", "vb"]; a schema listing bbb(1) then
    // app(0) is descending by referenced name bytes ("bbb" > "app").
    spec.schemas = vec![vec![1, 0]];
    spec.value_ord = vec![vec![2, 2], vec![3, 3]];
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "bbb", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SchemaNamesUnsorted)
    );
}

#[test]
fn corpus_oversized_schema_name_count_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpec::baseline();
    spec.count = 1;
    spec.schema_ref = vec![0];
    spec.value_ord = vec![vec![1]];
    spec.sample_count = vec![1];
    spec.min_ts_delta = vec![0];
    spec.ts_span = vec![0];
    spec.ts_page_gap = vec![0];
    spec.ts_page_len = vec![5];
    spec.val_page_gap = vec![0];
    spec.val_page_len = vec![8];
    // Hand-build meta bytes with a corrupt name_count (> 65535) directly,
    // since MetaSpec::build() always derives name_count from a real Vec.
    let mut meta = Vec::new();
    meta.extend_from_slice(&1u32.to_le_bytes()); // count
    meta.extend_from_slice(&1u32.to_le_bytes()); // schema_count
    write_uvarint(&mut meta, 70_000); // name_count, oversized
    let (mut footer, dict, ids_bytes, _unused) = build_scenario(&["app", "va"], &ids, &spec);
    let series_meta_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section");
    footer.sections[series_meta_idx].len = meta.len() as u64;
    footer.sections[series_meta_idx].uncompressed_len = meta.len() as u64;
    footer.sections[series_meta_idx].crc32c = crc32c::crc32c(&meta);
    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert_eq!(err, SegmentError::SchemaNameCountTooLarge(70_000));
}

#[test]
fn corpus_val_page_gap_overflow_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.val_page_gap = vec![0, u64::MAX];
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SectionOutOfBounds)
    );
}

#[test]
fn corpus_val_page_range_exceeds_section_length_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.val_page_len = vec![8, 10_000]; // VAL_PAGES section is 200 bytes
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SectionOutOfBounds)
    );
}

#[test]
fn corpus_ts_page_range_exceeds_section_length_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpec::baseline();
    spec.ts_page_len = vec![5, 10_000]; // TS_PAGES section is 200 bytes
    let (footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SectionOutOfBounds)
    );
}

#[test]
fn corpus_block_with_leftover_bytes_is_rejected() {
    // Splice a valid baseline's SERIES_META bytes so the schema_ref
    // block's declared block_len is one byte larger than what the block's
    // own content (count=2 single-byte varints) actually needs. Since
    // block_len is itself a 1-byte varint here (schema_ref's real block
    // content is 2 bytes: two single-byte varints [0, 0]), incrementing
    // that one length-prefix byte in place keeps every later byte offset
    // valid (there are plenty of subsequent bytes in the section for the
    // now-larger slice to still fit), and produces exactly "block declares
    // more bytes than its parsed content consumed".
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, mut meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);

    // meta layout: count(4) + schema_count(4) + schema list + block1...
    // schema list: name_count varint (1 byte, value 1) + 1 name_ord varint
    // (1 byte, value 0) = 2 bytes. Block 1 (schema_ref) starts right after.
    let block1_len_pos = 4 + 4 + 2;
    assert_eq!(
        meta[block1_len_pos], 2,
        "test assumption: schema_ref block_len is a 1-byte varint of value 2"
    );
    meta[block1_len_pos] = 3; // claim 3 bytes; only 2 are real content
    // Recompute the section crc32c for the mutated bytes and rebuild the
    // footer's SERIES_META section descriptor (len/uncompressed_len grow
    // by 0 -- we didn't insert bytes, only changed the length-prefix
    // value -- but crc32c must still match the new byte pattern).
    let mut footer = footer;
    let series_meta_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section");
    footer.sections[series_meta_idx].crc32c = crc32c::crc32c(&meta);

    let err = decode(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert_eq!(err, SegmentError::BadBlockLen);
}

/// The other direction of "corrupt block_len": a declared length that
/// overruns the section (there aren't that many bytes left at all, as
/// opposed to `corpus_block_with_leftover_bytes_is_rejected`'s "there are
/// enough bytes overall, but this block didn't consume all of what it
/// claimed"). `take_block` cannot even slice out the claimed byte range,
/// so this fails as `Truncated`, not `BadBlockLen`.
#[test]
fn corpus_block_len_overruns_section_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, mut meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);

    let block1_len_pos = 4 + 4 + 2;
    assert_eq!(
        meta[block1_len_pos], 2,
        "test assumption: schema_ref block_len is a 1-byte varint of value 2"
    );
    // Claim far more bytes than remain in the whole section (baseline
    // SERIES_META is well under 100 bytes total). 127 is the largest value
    // that still encodes as a single-byte varint (MSB unset), so this
    // in-place byte replacement keeps every earlier offset in the buffer
    // valid.
    meta[block1_len_pos] = 127;
    let mut footer = footer;
    let series_meta_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section");
    footer.sections[series_meta_idx].crc32c = crc32c::crc32c(&meta);

    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::Truncated)
    );
}

#[test]
fn corpus_series_meta_trailing_bytes_after_last_block_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, mut meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    meta.push(0x00); // one extra byte past the end of block 9
    let mut footer = footer;
    let series_meta_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section");
    footer.sections[series_meta_idx].len = meta.len() as u64;
    footer.sections[series_meta_idx].uncompressed_len = meta.len() as u64;
    footer.sections[series_meta_idx].crc32c = crc32c::crc32c(&meta);

    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::TrailingBytes)
    );
}

#[test]
fn corpus_series_ids_trailing_bytes_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, mut ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    ids_bytes.push(0xFF);
    let mut footer = footer;
    let series_ids_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_IDS)
        .expect("series ids section");
    footer.sections[series_ids_idx].len = ids_bytes.len() as u64;
    footer.sections[series_ids_idx].uncompressed_len = ids_bytes.len() as u64;
    footer.sections[series_ids_idx].crc32c = crc32c::crc32c(&ids_bytes);

    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::TrailingBytes)
    );
}

#[test]
fn corpus_series_ids_truncated_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, mut ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    ids_bytes.truncate(ids_bytes.len() - 1);
    let mut footer = footer;
    let series_ids_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_IDS)
        .expect("series ids section");
    footer.sections[series_ids_idx].len = ids_bytes.len() as u64;
    footer.sections[series_ids_idx].uncompressed_len = ids_bytes.len() as u64;
    footer.sections[series_ids_idx].crc32c = crc32c::crc32c(&ids_bytes);

    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::Truncated)
    );
}

#[test]
fn corpus_missing_ts_pages_section_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (mut footer, dict, ids_bytes, meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    footer.sections.retain(|s| s.kind != 3); // drop TS_PAGES
    assert_eq!(
        decode(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::MissingSection("TS_PAGES"))
    );
}

// --- 7. Direct `validate_sections` (v2 mandatory-kind set) coverage.   ---
// --- Every corpus test above reaches decode_catalog_v2 through a       ---
// --- footer that already satisfies validate_sections's requirements   ---
// --- (validate_sections is never called by decode_catalog_v2 itself,  ---
// --- only by open_from_full/open_from_suffix), so none of them        ---
// --- exercise validate_sections_v2's own mandatory-kind/duplicate      ---
// --- logic. These call it directly. ---

const PAGE_REGION_END: u64 = 10_000;

#[test]
fn validate_sections_v2_rejects_missing_series_meta() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (mut footer, _dict, _ids_bytes, _meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    footer.sections.retain(|s| s.kind != SERIES_META);
    assert_eq!(
        ravel_segment::validate_sections(&footer, 2, PAGE_REGION_END, ReaderLimits::default()),
        Err(SegmentError::MissingSection("SERIES_META"))
    );
}

#[test]
fn validate_sections_v2_rejects_missing_series_ids() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (mut footer, _dict, _ids_bytes, _meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    footer.sections.retain(|s| s.kind != SERIES_IDS);
    assert_eq!(
        ravel_segment::validate_sections(&footer, 2, PAGE_REGION_END, ReaderLimits::default()),
        Err(SegmentError::MissingSection("SERIES_IDS"))
    );
}

#[test]
fn validate_sections_v2_rejects_duplicate_series_ids_section() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (mut footer, _dict, _ids_bytes, _meta) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let dup = *footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDS)
        .expect("series ids section");
    footer.sections.push(dup);
    assert_eq!(
        ravel_segment::validate_sections(&footer, 2, PAGE_REGION_END, ReaderLimits::default()),
        Err(SegmentError::DuplicateSection(SERIES_IDS))
    );
}

/// Cross-version pin: the v1 and v2 mandatory-kind sets are genuinely
/// distinct, and `validate_sections` actually dispatches on the `version`
/// argument rather than, say, always checking the v1 set (or the union of
/// both). A real v1 footer validated as version 2 is missing SERIES_IDS (v1
/// never emits it); a real v2 footer validated as version 1 is missing
/// SERIES_TABLE (v2 never emits it). Either direction failing differently,
/// or not failing at all, would mean the dispatch is wired wrong.
#[test]
fn validate_sections_v2_and_v1_mandatory_kind_sets_are_distinct() {
    let v1_written = SegmentWriter::write(differential_fixture(), test_identity(), test_bounds())
        .expect("v1 writes");
    let v1_loc = ravel_segment::open_from_full(&v1_written.bytes, ReaderLimits::default())
        .expect("v1 opens");
    assert_eq!(
        ravel_segment::validate_sections(
            &v1_loc.footer,
            2,
            v1_loc.footer_offset,
            ReaderLimits::default()
        ),
        Err(SegmentError::MissingSection("SERIES_IDS"))
    );

    let v2_written =
        SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
            .expect("v2 writes");
    let v2_loc = ravel_segment::open_from_full(&v2_written.bytes, ReaderLimits::default())
        .expect("v2 opens");
    assert_eq!(
        ravel_segment::validate_sections(
            &v2_loc.footer,
            1,
            v2_loc.footer_offset,
            ReaderLimits::default()
        ),
        Err(SegmentError::MissingSection("SERIES_TABLE"))
    );
}

/// One real end-to-end case (through `write_v2` + zstd + section crc, not
/// hand-crafted): flipping a byte inside a real object's stored
/// SERIES_META must still fail closed on the production path, proving the
/// crc gate that guards the hand-crafted corpus above also fires for real
/// compressed/zstd sections, not only for the corpus's `comp = 0` shape.
#[test]
fn real_object_series_meta_byte_flip_is_rejected_by_section_crc() {
    let written = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("writes");
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&written.bytes, limits).expect("opens");
    let dict = section_bytes(&written.bytes, &loc.footer, LABEL_DICT).to_vec();
    let ids = section_bytes(&written.bytes, &loc.footer, SERIES_IDS).to_vec();
    let mut meta = section_bytes(&written.bytes, &loc.footer, SERIES_META).to_vec();
    meta[0] ^= 0xFF;
    assert_eq!(
        decode(&loc.footer, &dict, &ids, &meta),
        Err(SegmentError::SectionCrcMismatch)
    );
}

/// A deterministic (not merely probable) check that decoding an empty v2
/// segment succeeds with zero entries. The roundtrip proptest above can
/// generate a `0..8`-length batch that happens to include zero series, but
/// that path is probabilistic; P2 added an equivalent dedicated writer-side
/// test (`empty_segment_has_well_formed_sections`) for the same reason
/// ("neither property test's loop body ever executes on that path"). This
/// exercises `count = 0`, `schema_count = 0`, all nine `block_len = 0`
/// blocks, and the final `pos == meta_bytes.len()` check together.
#[test]
fn empty_v2_segment_decodes_to_zero_entries() {
    let written = SegmentWriter::write_v2(Vec::new(), test_identity(), test_bounds())
        .expect("writes empty v2 segment");
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&written.bytes, limits).expect("opens");
    assert_eq!(loc.version, 2);
    let dict = section_bytes(&written.bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(&written.bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(&written.bytes, &loc.footer, SERIES_META);
    let entries =
        ravel_segment::decode_catalog_v2(&loc.footer, dict, ids, meta, limits).expect("decodes");
    assert!(entries.is_empty());

    let lazy = ravel_segment::decode_catalog_matching_v2(&loc.footer, dict, ids, meta, &[], limits)
        .expect("decodes");
    assert!(lazy.is_empty());
}

// ===========================================================================
// P4: fuzz/property hardening over both versions (docs/rseg-v2-plan.md
// phase P4, issue #32). Sections 1-7 above (P3, issue #31) are unmodified;
// everything below is additive. Reuses this file's existing helpers
// (`differential_fixture`, `lazy_running_sum_fixture`, `full_decode_v1`/
// `full_decode_v2`/`normalize`, `labelset_strategy`/`ts_strategy`/
// `sample_value_strategy`/`samples_strategy`, `MetaSpec`/`build_scenario`)
// rather than reinventing them, per the ticket.
// ===========================================================================

// --- 8. Generalized v1-vs-v2 differential property ----------------------
//
// Section 1 above is one hand-picked fixture. This extends the same
// comparison -- catalog entries field-by-field (except val_page's OFFSET,
// which legitimately differs under v2 alignment padding, exactly as
// section 1 documents), then full sample decode compared bit-for-bit via
// `normalize` -- to arbitrary generated input, reusing the label/
// timestamp/value strategies already defined in section 5 above instead of
// defining new ones. Exhaustive mutation loops deliberately do NOT belong
// inside a proptest body (each of the ~200 cases would multiply by however
// many mutations it ran); this test is pure differential comparison, no
// mutation, so it belongs here.

fn build_series_inputs_for_differential(
    series_data: &[(LabelSet, Vec<(i64, f64)>)],
) -> Vec<SeriesInput> {
    series_data
        .iter()
        .enumerate()
        .map(|(idx, (series_labels, samples))| {
            let mut id_bytes = [0u8; 16];
            id_bytes[..8].copy_from_slice(&(idx as u64).to_be_bytes());
            SeriesInput {
                series_id: SeriesId(id_bytes),
                labels: series_labels.clone(),
                samples: samples
                    .iter()
                    .map(|&(ts_ns, value)| Sample { ts_ns, value })
                    .collect(),
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn v1_v2_differential_over_arbitrary_input(
        series_data in prop::collection::vec((labelset_strategy(), samples_strategy()), 0..8),
        tenant_hash in uniform16(any::<u8>()),
        shard in any::<u32>(),
        writer_id in "[a-zA-Z0-9-]{1,20}",
        epoch in any::<u64>(),
        seq in any::<u64>(),
    ) {
        // write() and write_v2() each consume their input by value and
        // neither SeriesInput nor SegmentIdentity/IngestBounds is Clone, so
        // build two independent (but logically identical) batches/structs
        // rather than adding Clone derives to production types for test
        // convenience.
        let inputs1 = build_series_inputs_for_differential(&series_data);
        let inputs2 = build_series_inputs_for_differential(&series_data);
        let identity1 = SegmentIdentity {
            tenant_hash,
            shard,
            writer_id: writer_id.clone(),
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let identity2 = SegmentIdentity {
            tenant_hash,
            shard,
            writer_id,
            writer_epoch: epoch,
            writer_seq: seq,
        };
        let bounds1 = IngestBounds {
            min_ingest_ts_ns: -1_000_000_000,
            max_ingest_ts_ns: 1_000_000_000,
        };
        let bounds2 = IngestBounds {
            min_ingest_ts_ns: -1_000_000_000,
            max_ingest_ts_ns: 1_000_000_000,
        };

        let v1 = SegmentWriter::write(inputs1, identity1, bounds1).expect("v1 writes");
        let v2 = SegmentWriter::write_v2(inputs2, identity2, bounds2).expect("v2 writes");

        let limits = ReaderLimits::default();
        let loc1 = ravel_segment::open_from_full(&v1.bytes, limits).expect("v1 opens");
        let loc2 = ravel_segment::open_from_full(&v2.bytes, limits).expect("v2 opens");
        prop_assert_eq!(loc1.version, 1);
        prop_assert_eq!(loc2.version, 2);

        let dict1 = section_bytes(&v1.bytes, &loc1.footer, 1);
        let table1 = section_bytes(&v1.bytes, &loc1.footer, 2);
        let mut entries1 =
            ravel_segment::decode_catalog(&loc1.footer, dict1, table1, limits).expect("v1 catalog");
        entries1.sort_by_key(|e| e.series_id.0);

        let dict2 = section_bytes(&v2.bytes, &loc2.footer, LABEL_DICT);
        let ids2 = section_bytes(&v2.bytes, &loc2.footer, SERIES_IDS);
        let meta2 = section_bytes(&v2.bytes, &loc2.footer, SERIES_META);
        let mut entries2 = ravel_segment::decode_catalog_v2(&loc2.footer, dict2, ids2, meta2, limits)
            .expect("v2 catalog");
        entries2.sort_by_key(|e| e.series_id.0);

        prop_assert_eq!(entries1.len(), entries2.len());
        for (a, b) in entries1.iter().zip(entries2.iter()) {
            prop_assert_eq!(a.series_id, b.series_id);
            prop_assert_eq!(&a.labels, &b.labels);
            prop_assert_eq!(a.sample_count, b.sample_count);
            prop_assert_eq!(a.min_ts_ns, b.min_ts_ns);
            prop_assert_eq!(a.max_ts_ns, b.max_ts_ns);
            prop_assert_eq!(a.ts_page, b.ts_page);
            // val_page OFFSET may legitimately differ (v2 8-byte alignment
            // padding, see section 1's comment above); length must agree.
            prop_assert_eq!(a.val_page.1, b.val_page.1);
        }

        // The real differential: full sample decode, bit-for-bit (NaN
        // payload and -0.0 significant, per `sample_value_strategy`).
        let decoded1 = full_decode_v1(&v1.bytes).expect("v1 full decode");
        let decoded2 = full_decode_v2(&v2.bytes).expect("v2 full decode");
        prop_assert_eq!(normalize(&decoded1), normalize(&decoded2));
    }
}

// --- 9. Byte-level mutation fuzzing, v2: exhaustive single-byte flips --
//
// tests/roundtrip.rs's `corruption_every_byte_flip_errors_or_matches`
// already does this exhaustively for v1 and finds zero uncovered bytes
// (v1 has no padding); that test is unmodified by this ticket. v2 objects
// legitimately contain bytes no checksum on the selective decode path
// covers: inter-section alignment padding before VAL_PAGES, and
// intra-VAL_PAGES padding before individual VAL_RAW_F64 pages
// (docs/segment-format.md "Checksum coverage, v2": both listed "n/a on the
// selective path"). So instead of requiring zero silent passes, this
// computes the exact set of checksum-covered byte offsets for the object
// under test (footer+trailer, the three crc-verified catalog sections, and
// every entry's actual TS/VAL page byte range, each page fully covered by
// its own crc per `split_page_header`) and asserts every silent pass falls
// OUTSIDE that set -- a positive pin on the v2 checksum-coverage claim, not
// just a tolerance for it.

#[test]
fn v2_object_byte_flip_uncovered_offsets_are_exactly_padding() {
    // Uses `lazy_running_sum_fixture` (not `differential_fixture`): it's
    // documented (section 3 above) to force a raw-fallback VAL_RAW_F64
    // page, which forces nonzero alignment padding, so this sweep actually
    // exercises the pad-byte path instead of only vacuously passing.
    let written =
        SegmentWriter::write_v2(lazy_running_sum_fixture(), test_identity(), test_bounds())
            .expect("v2 writes");
    let bytes = written.bytes.to_vec();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(&bytes, limits).expect("opens");
    assert_eq!(loc.version, 2);

    let dict = section_bytes(&bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(&bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(&bytes, &loc.footer, SERIES_META);
    let entries =
        ravel_segment::decode_catalog_v2(&loc.footer, dict, ids, meta, limits).expect("catalog");
    let ts_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == 3)
        .expect("ts section");
    let val_section = loc
        .footer
        .sections
        .iter()
        .find(|s| s.kind == 4)
        .expect("val section");

    let n = bytes.len();
    let mut covered = vec![false; n];
    for b in covered.iter_mut().skip(loc.footer_offset as usize) {
        *b = true; // footer + trailer, entirely under footer_crc32c.
    }
    for section in &loc.footer.sections {
        if matches!(section.kind, LABEL_DICT | SERIES_IDS | SERIES_META) {
            let start = section.offset as usize;
            let end = start + section.len as usize;
            covered[start..end].fill(true);
        }
    }
    let mut had_padding = false;
    for entry in &entries {
        let ts_start = (ts_section.offset + entry.ts_page.0) as usize;
        let ts_end = ts_start + entry.ts_page.1 as usize;
        covered[ts_start..ts_end].fill(true);
        let val_start = (val_section.offset + entry.val_page.0) as usize;
        let val_end = val_start + entry.val_page.1 as usize;
        covered[val_start..val_end].fill(true);
    }
    if !covered[..val_section.offset as usize + val_section.len as usize]
        .iter()
        .all(|&c| c)
    {
        had_padding = true;
    }
    assert!(
        had_padding,
        "test fixture must actually produce alignment padding, or this sweep \
         is vacuous -- see lazy_running_sum_fixture's doc comment"
    );

    let original = normalize(&full_decode_v2(&bytes).expect("baseline decodes"));
    let mut silent_passes = 0usize;
    for offset in 0..n {
        let mut flipped = bytes.clone();
        flipped[offset] ^= 0xFF;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            full_decode_dispatch(&flipped)
        }));
        match result {
            Err(_) => panic!("byte flip at offset {offset} caused a panic"),
            Ok(Err(_)) => {} // fails closed: fine regardless of coverage
            Ok(Ok(decoded)) => {
                assert_eq!(
                    normalize(&decoded),
                    original,
                    "byte flip at offset {offset} produced a DIFFERENT decode \
                     without erroring -- silent corruption"
                );
                assert!(
                    !covered[offset],
                    "byte flip at offset {offset} passed silently but IS \
                     checksum-covered (footer/trailer, a crc-verified section, \
                     or a page range) -- this is exactly the gap the v2 \
                     checksum coverage table (docs/segment-format.md) rules out"
                );
                silent_passes += 1;
            }
        }
    }
    eprintln!(
        "v2 byte-flip sweep: {n} offsets, {silent_passes} passed silently \
         (all confirmed to be alignment padding, never a checksum-covered byte)"
    );
}

// --- 10. Byte-level mutation fuzzing: exhaustive truncation, both       ---
// --- versions -------------------------------------------------------- ---
//
// tests/roundtrip.rs covers v1 byte flips but not truncation; this closes
// that gap for v1 too, deliberately here (not in roundtrip.rs, which this
// ticket leaves completely unmodified, per that file's own header comment)
// since P4 explicitly hardens "both versions" together.

fn assert_truncation_always_fails_closed(bytes: &[u8], label: &str) {
    for len in 0..bytes.len() {
        let truncated = &bytes[..len];
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            full_decode_dispatch(truncated)
        }));
        match result {
            Err(_) => panic!("{label}: truncation to {len} bytes caused a panic"),
            Ok(Ok(_)) => panic!(
                "{label}: truncation to {len} bytes decoded successfully -- \
                 truncated data must fail closed, never decode"
            ),
            Ok(Err(_)) => {} // expected: typed error
        }
    }
}

#[test]
fn v1_object_truncation_always_fails_closed() {
    let written = SegmentWriter::write(differential_fixture(), test_identity(), test_bounds())
        .expect("v1 writes");
    assert_truncation_always_fails_closed(&written.bytes, "v1");
}

#[test]
fn v2_object_truncation_always_fails_closed() {
    let written = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("v2 writes");
    assert_truncation_always_fails_closed(&written.bytes, "v2");
}

// --- 11. Byte-level mutation fuzzing: every SERIES_META block boundary --
//
// Real v2 objects store SERIES_META zstd-compressed, so wire-level
// mutation of the compressed bytes mostly just breaks zstd decompression
// (a generic `Decompress` error already exercised by the whole-object
// byte-flip sweep in section 9 and by `real_object_series_meta_byte_flip_
// is_rejected_by_section_crc` in section 6). To actually test the block-
// boundary semantics docs/segment-format.md defines (each of the 9
// columns' `block_len` prefix and content), this reuses the hand-crafted,
// uncompressed (`comp = 0`) `MetaSpec`/`build_scenario` infrastructure
// section 6 already established as a valid v2 shape, plus the section-crc
// recompute idiom every P3 corpus test above already uses when mutating
// bytes after construction.

fn with_updated_meta_section(footer: &Footer, meta: &[u8]) -> Footer {
    let mut footer = footer.clone();
    let idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section present");
    footer.sections[idx].len = meta.len() as u64;
    footer.sections[idx].uncompressed_len = meta.len() as u64;
    footer.sections[idx].crc32c = crc32c::crc32c(meta);
    footer
}

#[test]
fn series_meta_block_boundary_mutations_never_panic_and_fail_closed() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpec::baseline();
    let (footer, dict, ids_bytes, _) = build_scenario(&["app", "va", "vb"], &ids, &spec);
    let (meta, block_ranges) = spec.build_with_block_ranges();
    assert_eq!(
        meta,
        spec.build(),
        "build_with_block_ranges must match build() byte-for-byte"
    );

    for (name, range) in &block_ranges {
        // (a) flip the first and last content byte of this block.
        for &pos in &[range.start, range.end.saturating_sub(1)] {
            if range.is_empty() || pos >= meta.len() {
                continue;
            }
            let mut m = meta.clone();
            m[pos] ^= 0xFF;
            let f = with_updated_meta_section(&footer, &m);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode(&f, &dict, &ids_bytes, &m)
            }));
            match outcome {
                Err(_) => panic!("panic flipping byte {pos} in block {name}"),
                Ok(Err(_)) => {} // fails closed: expected
                Ok(Ok(entries)) => {
                    // Not every content mutation is guaranteed to be
                    // structurally invalid (a gap/len varint can flip to
                    // another in-range value) -- but it must never panic,
                    // and any successful decode must still be a
                    // self-consistent SeriesEntry list of the declared size,
                    // never garbage.
                    assert_eq!(entries.len(), spec.count as usize, "block {name}");
                }
            }
        }

        // (b) truncate the whole meta buffer to end exactly at this
        // block's content start: cuts off this block, and everything
        // after it, entirely.
        let m = meta[..range.start].to_vec();
        let f = with_updated_meta_section(&footer, &m);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode(&f, &dict, &ids_bytes, &m)
        }));
        match outcome {
            Err(_) => panic!("panic truncating meta before block {name}'s content"),
            Ok(Ok(_)) => panic!("truncating meta before block {name} decoded successfully"),
            Ok(Err(_)) => {} // expected: typed error
        }

        // (c) truncate mid-content, when the block has more than one byte.
        if range.len() > 1 {
            let mid = range.start + range.len() / 2;
            let m = meta[..mid].to_vec();
            let f = with_updated_meta_section(&footer, &m);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode(&f, &dict, &ids_bytes, &m)
            }));
            match outcome {
                Err(_) => panic!("panic truncating meta mid-block {name}"),
                Ok(Ok(_)) => panic!("truncating meta mid-block {name} decoded successfully"),
                Ok(Err(_)) => {} // expected: typed error
            }
        }
    }
}

// --- 12. Cross-version grammar confusion --------------------------------
//
// A real object's trailer version stamped to the OTHER version, with
// footer_crc32c recomputed so the object is internally consistent up to
// that point (never just a simple crc mismatch) -- the "presented under
// the wrong trailer" case docs/rseg-v2-plan.md phase P4 calls out. This
// goes through the real `open_from_full` entry point end to end (unlike
// `validate_sections_v2_and_v1_mandatory_kind_sets_are_distinct` in section
// 7 above, which calls `validate_sections` directly with a hand-picked
// version argument to test the dispatch logic in isolation -- this test
// instead proves an actual byte-level object with a self-consistent but
// wrong-version trailer fails the same way when opened for real).

/// Recomputes footer_crc32c the way the writer does (docs/segment-format.md:
/// "computed over: FooterProto bytes, then footer_len (u32 LE), version
/// (u16 LE), signal, reserved, magic"; mirrors src/crc.rs's private
/// `footer_crc`, which isn't exported outside the crate). The only way to
/// reach `validate_sections` -- rather than failing immediately on
/// `FooterCrcMismatch` -- after re-stamping a real object's trailer
/// version byte.
fn recompute_footer_crc(bytes: &mut [u8]) {
    let n = bytes.len();
    let trailer_start = n - 16;
    let footer_len =
        u32::from_le_bytes(bytes[trailer_start..trailer_start + 4].try_into().unwrap());
    let footer_start = trailer_start - footer_len as usize;
    let mut input = Vec::with_capacity(footer_len as usize + 12);
    input.extend_from_slice(&bytes[footer_start..trailer_start]); // FooterProto bytes
    input.extend_from_slice(&bytes[trailer_start..trailer_start + 4]); // footer_len
    input.extend_from_slice(&bytes[trailer_start + 8..trailer_start + 16]); // version, signal, reserved, magic
    let crc = crc32c::crc32c(&input);
    bytes[trailer_start + 4..trailer_start + 8].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn v1_sections_under_a_v2_trailer_fail_closed() {
    let written = SegmentWriter::write(differential_fixture(), test_identity(), test_bounds())
        .expect("v1 writes");
    let mut bytes = written.bytes.to_vec();
    let n = bytes.len();
    bytes[n - 16 + 8] = 2; // version = 2
    bytes[n - 16 + 9] = 0;
    recompute_footer_crc(&mut bytes);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ravel_segment::open_from_full(&bytes, ReaderLimits::default())
    }));
    let err = match outcome {
        Err(_) => panic!("cross-version stamp (v1 sections, v2 trailer) caused a panic"),
        Ok(Ok(loc)) => panic!(
            "v1 sections under a v2 trailer opened successfully (version {}) \
             -- must fail closed",
            loc.version
        ),
        Ok(Err(e)) => e,
    };
    // v1 never emits SERIES_IDS (kind 5); it's the first mandatory v2-only
    // kind validate_sections_v2 checks for (docs/segment-format.md v2
    // mandatory set: LABEL_DICT, SERIES_IDS, SERIES_META, TS_PAGES,
    // VAL_PAGES).
    assert_eq!(err, SegmentError::MissingSection("SERIES_IDS"));
}

#[test]
fn v2_sections_under_a_v1_trailer_fail_closed() {
    let written = SegmentWriter::write_v2(differential_fixture(), test_identity(), test_bounds())
        .expect("v2 writes");
    let mut bytes = written.bytes.to_vec();
    let n = bytes.len();
    bytes[n - 16 + 8] = 1; // version = 1
    bytes[n - 16 + 9] = 0;
    recompute_footer_crc(&mut bytes);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ravel_segment::open_from_full(&bytes, ReaderLimits::default())
    }));
    let err = match outcome {
        Err(_) => panic!("cross-version stamp (v2 sections, v1 trailer) caused a panic"),
        Ok(Ok(loc)) => panic!(
            "v2 sections under a v1 trailer opened successfully (version {}) \
             -- must fail closed",
            loc.version
        ),
        Ok(Err(e)) => e,
    };
    // v2 never emits SERIES_TABLE (kind 2); v1's mandatory set requires it.
    assert_eq!(err, SegmentError::MissingSection("SERIES_TABLE"));
}
