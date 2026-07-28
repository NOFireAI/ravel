//! Cross-version roundtrip property test (docs/compaction-retention-plan.md
//! section 4): the same logical scalar content, written by v1, v2, v3, and
//! a single-run v4 object, must decode to identical (series_id, labels,
//! sorted (ts_ns, value-bits)) content regardless of which version wrote
//! it. Value-bits comparison (not `==`) makes NaN payloads and -0.0
//! significant, matching this crate's float-comparison convention
//! (CLAUDE.md: "Float comparisons ... use bit patterns, never ==").
//!
//! This mirrors tests/reader_v3.rs's
//! `v1_v2_v3_scalar_differential_over_arbitrary_input` (itself following
//! rseg-v2-plan P4's v1-vs-v2 precedent), extended with a v4 arm. The v4
//! object is never built from arbitrary bytes: each series' single run
//! reuses the real TS_PAGES/VAL_PAGES bytes sliced verbatim out of the
//! v1 object already written for the v1 comparison arm (same series_id,
//! so page crc32c -- bound to series_id+enc+comp+payload -- stays valid),
//! the same technique tests/reader_v4.rs's fixture builders use and
//! writer.rs's own `histogram_run_reuses_real_v3_hist_page_bytes_verbatim`
//! established.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_proto::segment::v1::Footer;
use ravel_segment::{
    CompactionMetaV4, IngestBounds, ReaderLimits, RunInputV4, RunValuePageV4, SegmentIdentity,
    SegmentWriter, SeriesEntry, SeriesEntryV4, SeriesInputV3, SeriesInputV4, SeriesValues,
    ValueKind,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

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

fn ts_strategy() -> impl Strategy<Value = i64> {
    -1_000_000i64..1_000_000i64
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
    ]
}

fn samples_strategy() -> impl Strategy<Value = Vec<(i64, f64)>> {
    prop_oneof![
        3 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..2),
        5 => prop::collection::vec((ts_strategy(), sample_value_strategy()), 1..12),
    ]
}

fn build_series_inputs_for_differential(
    series_data: &[(LabelSet, Vec<(i64, f64)>)],
) -> Vec<ravel_segment::SeriesInput> {
    series_data
        .iter()
        .enumerate()
        .map(|(idx, (series_labels, samples))| {
            let mut id_bytes = [0u8; 16];
            id_bytes[..8].copy_from_slice(&(idx as u64).to_be_bytes());
            ravel_segment::SeriesInput {
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

fn build_series_inputs_v3_for_differential(
    series_data: &[(LabelSet, Vec<(i64, f64)>)],
) -> Vec<SeriesInputV3> {
    series_data
        .iter()
        .enumerate()
        .map(|(idx, (series_labels, samples))| {
            let mut id_bytes = [0u8; 16];
            id_bytes[..8].copy_from_slice(&(idx as u64).to_be_bytes());
            SeriesInputV3 {
                series_id: SeriesId(id_bytes),
                labels: series_labels.clone(),
                values: SeriesValues::Scalar(
                    samples
                        .iter()
                        .map(|&(ts_ns, value)| Sample { ts_ns, value })
                        .collect(),
                ),
            }
        })
        .collect()
}

type NormalizedSeries = ([u8; 16], LabelSet, Vec<(i64, u64)>);

fn decode_v1_scalar(bytes: &[u8]) -> Vec<NormalizedSeries> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v1 opens");
    assert_eq!(loc.version, 1);
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let table = section_bytes(bytes, &loc.footer, 2);
    let mut entries =
        ravel_segment::decode_catalog(&loc.footer, dict, table, limits).expect("v1 catalog");
    entries.sort_by_key(|e| e.series_id.0);

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected).expect("v1 plans");
    entries
        .iter()
        .zip(ranges.iter())
        .map(|(entry, range)| {
            let ts_bytes = slice_range(bytes, range.ts_range);
            let val_bytes = slice_range(bytes, range.val_range);
            let samples =
                ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits).expect("v1 pages");
            (
                entry.series_id.0,
                entry.labels.clone(),
                samples
                    .iter()
                    .map(|s| (s.ts_ns, s.value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

fn decode_v2_scalar(bytes: &[u8]) -> Vec<NormalizedSeries> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v2 opens");
    assert_eq!(loc.version, 2);
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let mut entries =
        ravel_segment::decode_catalog_v2(&loc.footer, dict, ids, meta, limits).expect("v2 catalog");
    entries.sort_by_key(|e| e.series_id.0);

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges(&loc.footer, &selected).expect("v2 plans");
    entries
        .iter()
        .zip(ranges.iter())
        .map(|(entry, range)| {
            let ts_bytes = slice_range(bytes, range.ts_range);
            let val_bytes = slice_range(bytes, range.val_range);
            let samples =
                ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits).expect("v2 pages");
            (
                entry.series_id.0,
                entry.labels.clone(),
                samples
                    .iter()
                    .map(|s| (s.ts_ns, s.value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

fn decode_v3_scalar(bytes: &[u8]) -> Vec<NormalizedSeries> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v3 opens");
    assert_eq!(loc.version, 3);
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let mut entries =
        ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits).expect("v3 catalog");
    entries.sort_by_key(|e| e.series_id.0);

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected).expect("v3 plans");
    entries
        .iter()
        .zip(ranges.iter())
        .map(|(entry, range)| {
            assert_eq!(
                entry.value_kind,
                ValueKind::Scalar,
                "this section writes scalar-only v3 input"
            );
            let ts_bytes = slice_range(bytes, range.ts_range);
            let val_bytes = slice_range(bytes, range.val_range);
            let samples =
                ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits).expect("v3 pages");
            (
                entry.series_id.0,
                entry.labels.clone(),
                samples
                    .iter()
                    .map(|s| (s.ts_ns, s.value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

/// Builds one single-run `SeriesInputV4` per series in a real v1 object,
/// reusing that object's own TS_PAGES/VAL_PAGES bytes verbatim (see the
/// module doc comment). `provenance` distinguishes runs across the
/// property test's cases only for readability in failure output; with one
/// run per series its value has no decode-visible effect.
fn build_v4_inputs_from_v1_bytes(bytes: &[u8]) -> Vec<SeriesInputV4> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v1 opens for v4 fixture");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let table = section_bytes(bytes, &loc.footer, 2);
    let entries =
        ravel_segment::decode_catalog(&loc.footer, dict, table, limits).expect("v1 catalog");

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges =
        ravel_segment::plan_ranges(&loc.footer, &selected).expect("v1 plans for v4 fixture");

    entries
        .iter()
        .zip(ranges.iter())
        .map(|(entry, range)| {
            let ts_page = slice_range(bytes, range.ts_range).to_vec();
            let val_page = slice_range(bytes, range.val_range).to_vec();
            SeriesInputV4 {
                series_id: entry.series_id,
                labels: entry.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 0,
                    writer_epoch: 0,
                    writer_seq: 0,
                    min_ts_ns: entry.min_ts_ns,
                    max_ts_ns: entry.max_ts_ns,
                    sample_count: entry.sample_count,
                    ts_page,
                    value_page: RunValuePageV4::Scalar(val_page),
                }],
            }
        })
        .collect()
}

fn decode_v4_scalar(bytes: &[u8]) -> Vec<NormalizedSeries> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v4 opens");
    assert_eq!(loc.version, 4);
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let mut entries =
        ravel_segment::decode_catalog_v4(&loc.footer, dict, ids, meta, limits).expect("v4 catalog");
    entries.sort_by_key(|e| e.entry.series_id.0);

    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("v4 plans");

    entries
        .iter()
        .map(|entry| {
            assert_eq!(
                entry.runs.len(),
                1,
                "this section writes exactly one run per series"
            );
            assert_eq!(
                entry.entry.value_kind,
                ValueKind::Scalar,
                "this section writes scalar-only v4 input"
            );
            let run = &entry.runs[0];
            let range = ranges
                .iter()
                .find(|r| r.series_id == entry.entry.series_id && r.run_index == 0)
                .expect("planned run range present");
            let ts_bytes = slice_range(bytes, range.ts_range);
            let val_bytes = slice_range(bytes, range.val_range);
            let mut scratch = Vec::new();
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            ravel_segment::decode_run_pages_soa(
                &entry.entry.series_id,
                run,
                ts_bytes,
                val_bytes,
                limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .expect("v4 run pages");
            let mut samples: Vec<(i64, u64)> = timestamps
                .into_iter()
                .zip(values.into_iter().map(f64::to_bits))
                .collect();
            samples.sort_by_key(|(ts, _)| *ts);
            (entry.entry.series_id.0, entry.entry.labels.clone(), samples)
        })
        .collect()
}

/// v5 decode of a below-threshold object (these cases have <8 series, so no
/// sparse sections): `decode_catalog_v5` delegates to the v4 whole-catalog
/// path, then the pages decode exactly as v4's. Confirms a version-5 object
/// carrying the v4-shaped catalog reads back identically.
fn decode_v5_scalar(bytes: &[u8]) -> Vec<NormalizedSeries> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("v5 opens");
    assert_eq!(loc.version, 5);
    let mut entries =
        ravel_segment::decode_catalog_v5(&loc.footer, bytes, limits).expect("v5 catalog");
    entries.sort_by_key(|e| e.entry.series_id.0);

    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("v5 plans");
    entries
        .iter()
        .map(|entry| {
            let run = &entry.runs[0];
            let range = ranges
                .iter()
                .find(|r| r.series_id == entry.entry.series_id && r.run_index == 0)
                .expect("planned run range present");
            let ts_bytes = slice_range(bytes, range.ts_range);
            let val_bytes = slice_range(bytes, range.val_range);
            let mut scratch = Vec::new();
            let mut timestamps = Vec::new();
            let mut values = Vec::new();
            ravel_segment::decode_run_pages_soa(
                &entry.entry.series_id,
                run,
                ts_bytes,
                val_bytes,
                limits,
                &mut scratch,
                &mut timestamps,
                &mut values,
            )
            .expect("v5 run pages");
            let mut samples: Vec<(i64, u64)> = timestamps
                .into_iter()
                .zip(values.into_iter().map(f64::to_bits))
                .collect();
            samples.sort_by_key(|(ts, _)| *ts);
            (entry.entry.series_id.0, entry.entry.labels.clone(), samples)
        })
        .collect()
}

fn test_compaction_meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 0,
        input_set_hash: [0u8; 32],
        part_index: 0,
        level: 0,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// The same logical scalar-only content, written by v1, v2, v3, and a
    /// single-run v4 object, must decode to identical (series_id, labels,
    /// sorted (ts_ns, value-bits)) content -- including NaN payloads and
    /// -0.0 -- regardless of which version wrote it.
    #[test]
    fn v1_v2_v3_v4_scalar_single_run_differential_over_arbitrary_input(
        series_data in prop::collection::vec((labelset_strategy(), samples_strategy()), 0..8),
        tenant_hash in proptest::array::uniform16(any::<u8>()),
        shard in any::<u32>(),
        writer_id in "[a-zA-Z0-9-]{1,20}",
        epoch in any::<u64>(),
        seq in any::<u64>(),
    ) {
        // Each writer consumes its input by value and neither SeriesInput
        // nor SeriesInputV3/SegmentIdentity/IngestBounds is Clone, so build
        // independent (but logically identical) batches/structs rather
        // than adding Clone derives to production types for test
        // convenience. The v4 batch is built afterward, from the real v1
        // object's own bytes, not from series_data directly.
        let inputs1 = build_series_inputs_for_differential(&series_data);
        let inputs2 = build_series_inputs_for_differential(&series_data);
        let inputs3 = build_series_inputs_v3_for_differential(&series_data);

        let identity1 = SegmentIdentity {
            tenant_hash, shard, writer_id: writer_id.clone(), writer_epoch: epoch, writer_seq: seq,
        };
        let identity2 = SegmentIdentity {
            tenant_hash, shard, writer_id: writer_id.clone(), writer_epoch: epoch, writer_seq: seq,
        };
        let identity3 = SegmentIdentity {
            tenant_hash, shard, writer_id: writer_id.clone(), writer_epoch: epoch, writer_seq: seq,
        };
        let writer_id_5 = writer_id.clone();
        let identity4 = SegmentIdentity {
            tenant_hash, shard, writer_id, writer_epoch: epoch, writer_seq: seq,
        };
        let bounds = || IngestBounds {
            min_ingest_ts_ns: -1_000_000_000,
            max_ingest_ts_ns: 1_000_000_000,
        };

        let v1 = SegmentWriter::write(inputs1, identity1, bounds()).expect("v1 writes");
        let v2 = SegmentWriter::write_v2(inputs2, identity2, bounds()).expect("v2 writes");
        let v3 = SegmentWriter::write_v3(inputs3, identity3, bounds()).expect("v3 writes");

        let got1 = decode_v1_scalar(&v1.bytes);
        let got2 = decode_v2_scalar(&v2.bytes);
        let got3 = decode_v3_scalar(&v3.bytes);

        prop_assert_eq!(&got1, &got2, "v1 and v2 diverged on the same logical scalar input");
        prop_assert_eq!(&got1, &got3, "v1 and v3 diverged on the same logical scalar input");

        // Zero-series inputs write an empty v4 object fine, but write_v4
        // drops empty series and there are none here to drop, so this
        // covers the 0..8 lower bound without a special case.
        let inputs4 = build_v4_inputs_from_v1_bytes(&v1.bytes);
        if !inputs4.is_empty() {
            let identity5 = SegmentIdentity {
                tenant_hash,
                shard,
                writer_id: writer_id_5,
                writer_epoch: epoch,
                writer_seq: seq,
            };
            let v4 = SegmentWriter::write_v4(inputs4, identity4, bounds(), test_compaction_meta())
                .expect("v4 writes");
            let got4 = decode_v4_scalar(&v4.bytes);
            prop_assert_eq!(&got1, &got4, "v1 and v4 diverged on the same logical scalar input");

            // v5 below threshold: same v4 grammar, version-5 trailer.
            let inputs5 = build_v4_inputs_from_v1_bytes(&v1.bytes);
            let v5 = SegmentWriter::write_v5(inputs5, identity5, bounds(), test_compaction_meta())
                .expect("v5 writes");
            let got5 = decode_v5_scalar(&v5.bytes);
            prop_assert_eq!(&got1, &got5, "v1 and v5 diverged on the same logical scalar input");
        }
    }
}
