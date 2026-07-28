//! Integration tests for RSEG v4 decode (ADR-0018,
//! docs/compaction-retention-plan.md section 4):
//! `decode_catalog_v4`/`decode_catalog_matching_v4`, `plan_ranges_v4`,
//! `decode_run_pages_soa`/`decode_run_histogram_pages`, quad-version
//! trailer dispatch, and a typed-error corpus over hand-crafted corrupt v4
//! SERIES_META inputs plus cross-version grammar confusion.
//!
//! Mirrors tests/reader_v3.rs's structure and corpus-building approach
//! (`MetaSpecV3`/`build_scenario_v3`/`push_block`), extended with the
//! v4-only `run_total` header and the ten run-major columns. v1/v2/v3
//! fixtures and tests are completely unmodified by this ticket.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_proto::segment::v1::{Footer, Section};
use ravel_segment::{
    CompactionMetaV4, HistogramValue, IngestBounds, ReaderLimits, RunInputV4, RunValuePageV4,
    SegmentError, SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInput, SeriesInputV3,
    SeriesInputV4, SeriesValues, ValueKind,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

// --- shared helpers, mirroring tests/reader_v3.rs --------------------------

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
const TS_PAGES: u32 = 3;
const VAL_PAGES: u32 = 4;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;
const HIST_PAGES: u32 = 7;

fn test_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0x5A; 16],
        shard: 4,
        writer_id: "reader-v4-test-writer".to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn test_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 100_000,
    }
}

fn test_compaction_meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 7,
        input_set_hash: [0x11; 32],
        part_index: 0,
        level: 1,
    }
}

// --- 1. Real-byte run fixtures: build a single-series v1/v3 object via   ---
// --- the public writer API and slice out its own TS_PAGES/VAL_PAGES/     ---
// --- HIST_PAGES bytes, exactly the technique write_v4's own unit tests    ---
// --- use (writer.rs::histogram_run_reuses_real_v3_hist_page_bytes_       ---
// --- verbatim) since page crc32c is bound to series_id+enc+comp+payload:  ---
// --- the fixture must reuse the SAME series_id the v4 run will carry.     ---

fn scalar_run_from_v1(
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series_id: SeriesId,
    mut samples: Vec<Sample>,
) -> RunInputV4 {
    samples.sort_by_key(|s| s.ts_ns);
    let min_ts_ns = samples.first().expect("non-empty").ts_ns;
    let max_ts_ns = samples.last().expect("non-empty").ts_ns;
    let sample_count = samples.len() as u32;
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id,
            labels: labels(&[(METRIC_NAME_LABEL, "fixture")]),
            samples,
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v1 fixture");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses v1 fixture footer");
    let ts_page = section_bytes(bytes, &loc.footer, TS_PAGES).to_vec();
    let val_page = section_bytes(bytes, &loc.footer, VAL_PAGES).to_vec();
    RunInputV4 {
        created_unix_ns,
        writer_epoch,
        writer_seq,
        min_ts_ns,
        max_ts_ns,
        sample_count,
        ts_page,
        value_page: RunValuePageV4::Scalar(val_page),
    }
}

fn histogram_run_from_v3(
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    series_id: SeriesId,
    samples: Vec<ravel_segment::HistogramSample>,
) -> RunInputV4 {
    let mut sorted = samples;
    sorted.sort_by_key(|s| s.ts_ns);
    let min_ts_ns = sorted.first().expect("non-empty").ts_ns;
    let max_ts_ns = sorted.last().expect("non-empty").ts_ns;
    let sample_count = sorted.len() as u32;
    let written = SegmentWriter::write_v3(
        vec![SeriesInputV3 {
            series_id,
            labels: labels(&[(METRIC_NAME_LABEL, "fixture")]),
            values: SeriesValues::Histogram(sorted),
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v3 fixture");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses v3 fixture footer");
    let ts_page = section_bytes(bytes, &loc.footer, TS_PAGES).to_vec();
    let hist_page = section_bytes(bytes, &loc.footer, HIST_PAGES).to_vec();
    RunInputV4 {
        created_unix_ns,
        writer_epoch,
        writer_seq,
        min_ts_ns,
        max_ts_ns,
        sample_count,
        ts_page,
        value_page: RunValuePageV4::Histogram(hist_page),
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
        positive_spans: vec![ravel_segment::HistogramSpan {
            offset: 0,
            length: 2,
        }],
        negative_spans: vec![],
        counts: ravel_segment::HistogramCounts::Int {
            zero_count,
            count,
            positive,
            negative: vec![],
        },
        reset_hint: ravel_segment::ResetHint::Unknown,
    }
}

// --- 2. Full pipeline roundtrip: write_v4 -> decode_catalog_v4 ->        ---
// --- plan_ranges_v4 -> decode_run_pages_soa / decode_run_histogram_pages, ---
// --- multi-run per series, verifying every run's content decodes exactly ---
// --- what the underlying real v1/v3 fixture held.                        ---

#[test]
fn write_v4_read_roundtrip_decodes_multi_run_scalar_and_histogram_series() {
    let scalar_id = SeriesId([0x01; 16]);
    let hist_id = SeriesId([0x02; 16]);

    let run1 = scalar_run_from_v1(
        100,
        1,
        1,
        scalar_id,
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
    );
    let run2 = scalar_run_from_v1(
        200,
        1,
        2,
        scalar_id,
        vec![
            Sample {
                ts_ns: 2_000,
                value: 2.0,
            },
            Sample {
                ts_ns: 3_000,
                value: 2.5,
            },
        ],
    );
    let hist_run = histogram_run_from_v3(
        150,
        1,
        1,
        hist_id,
        vec![
            ravel_segment::HistogramSample {
                ts_ns: 0,
                value: sample_histogram_value(1),
            },
            ravel_segment::HistogramSample {
                ts_ns: 500,
                value: sample_histogram_value(2),
            },
        ],
    );

    let series = vec![
        SeriesInputV4 {
            series_id: scalar_id,
            labels: labels(&[(METRIC_NAME_LABEL, "scalar_m"), ("zone", "z1")]),
            // Input order deliberately reversed vs. expected (created_unix_ns,
            // writer_epoch, writer_seq) sort order.
            runs: vec![run2, run1],
        },
        SeriesInputV4 {
            series_id: hist_id,
            labels: labels(&[(METRIC_NAME_LABEL, "hist_m"), ("zone", "z2")]),
            runs: vec![hist_run],
        },
    ];

    let written = SegmentWriter::write_v4(
        series,
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    assert_eq!(loc.version, 4);

    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v4(&loc.footer, dict, ids, meta, limits)
        .expect("decodes v4 catalog");
    assert_eq!(entries.len(), 2);

    let scalar_entry = entries
        .iter()
        .find(|e| e.entry.series_id == scalar_id)
        .expect("scalar series present");
    assert_eq!(scalar_entry.runs.len(), 2);
    assert_eq!(scalar_entry.entry.sample_count, 4);
    // Sorted by (created_unix_ns, writer_epoch, writer_seq): run1 (100) then run2 (200).
    assert_eq!(scalar_entry.runs[0].created_unix_ns, 100);
    assert_eq!(scalar_entry.runs[1].created_unix_ns, 200);

    let hist_entry = entries
        .iter()
        .find(|e| e.entry.series_id == hist_id)
        .expect("histogram series present");
    assert_eq!(hist_entry.runs.len(), 1);
    assert_eq!(hist_entry.entry.value_kind, ValueKind::Histogram);

    let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("plans v4 ranges");

    let find_range = |series_id: SeriesId, run_index: usize| {
        ranges
            .iter()
            .find(|r| r.series_id == series_id && r.run_index == run_index)
            .expect("planned range present")
    };

    // Sorted by (created_unix_ns, writer_epoch, writer_seq): run1 (100, ts
    // 0/1000) then run2 (200, ts 2000/3000).
    let expected_scalar: [(Vec<i64>, Vec<f64>); 2] = [
        (vec![0, 1_000], vec![1.0, 1.5]),
        (vec![2_000, 3_000], vec![2.0, 2.5]),
    ];
    for (run_index, run) in scalar_entry.runs.iter().enumerate() {
        let range = find_range(scalar_id, run_index);
        let ts_bytes = slice_range(bytes, range.ts_range);
        let val_bytes = slice_range(bytes, range.val_range);
        let mut scratch = Vec::new();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        ravel_segment::decode_run_pages_soa(
            &scalar_entry.entry.series_id,
            run,
            ts_bytes,
            val_bytes,
            limits,
            &mut scratch,
            &mut timestamps,
            &mut values,
        )
        .expect("decodes run scalar pages");
        assert_eq!(timestamps.len(), run.sample_count as usize);
        assert_eq!(timestamps, expected_scalar[run_index].0);
        let value_bits: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let expected_bits: Vec<u64> = expected_scalar[run_index]
            .1
            .iter()
            .map(|v| v.to_bits())
            .collect();
        assert_eq!(value_bits, expected_bits);
    }

    let range = find_range(hist_id, 0);
    let ts_bytes = slice_range(bytes, range.ts_range);
    let hist_bytes = slice_range(bytes, range.hist_range);
    let decoded = ravel_segment::decode_run_histogram_pages(
        &hist_entry.entry.series_id,
        &hist_entry.runs[0],
        ts_bytes,
        hist_bytes,
        limits,
    )
    .expect("decodes run histogram pages");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].ts_ns, 0);
    assert_eq!(decoded[1].ts_ns, 500);
    assert_eq!(decoded[0].value.sum, sample_histogram_value(1).sum);
    assert_eq!(decoded[1].value.sum, sample_histogram_value(2).sum);
}

#[test]
fn decode_catalog_matching_v4_agrees_with_eager_select() {
    let scalar_id = SeriesId([0x01; 16]);
    let hist_id = SeriesId([0x02; 16]);
    let run = scalar_run_from_v1(
        100,
        1,
        1,
        scalar_id,
        vec![Sample {
            ts_ns: 0,
            value: 1.0,
        }],
    );
    let hist_run = histogram_run_from_v3(
        100,
        1,
        1,
        hist_id,
        vec![ravel_segment::HistogramSample {
            ts_ns: 0,
            value: sample_histogram_value(1),
        }],
    );
    let series = vec![
        SeriesInputV4 {
            series_id: scalar_id,
            labels: labels(&[(METRIC_NAME_LABEL, "scalar_m"), ("zone", "z1")]),
            runs: vec![run],
        },
        SeriesInputV4 {
            series_id: hist_id,
            labels: labels(&[(METRIC_NAME_LABEL, "hist_m"), ("zone", "z2")]),
            runs: vec![hist_run],
        },
    ];
    let written = SegmentWriter::write_v4(
        series,
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);

    let eager = ravel_segment::decode_catalog_v4(&loc.footer, dict, ids, meta, limits)
        .expect("eager decode");
    let expected: Vec<&SeriesEntryV4> = eager
        .iter()
        .filter(|e| e.entry.labels.get("zone") == Some("z2"))
        .collect();
    assert_eq!(expected.len(), 1);

    let matched = ravel_segment::decode_catalog_matching_v4(
        &loc.footer,
        dict,
        ids,
        meta,
        &[("zone", "z2")],
        limits,
    )
    .expect("matching decode");
    assert_eq!(matched.len(), expected.len());
    assert_eq!(matched[0].entry.series_id, expected[0].entry.series_id);
    assert_eq!(matched[0].runs.len(), expected[0].runs.len());
}

// --- 3. Quad-version dispatch: v1, v2, v3, and v4 objects each report    ---
// --- their own trailer version; version 5 is rejected as unsupported.    ---

#[test]
fn quad_version_dispatch_reports_each_trailers_own_version() {
    let v1 = SegmentWriter::write(
        vec![SeriesInput {
            series_id: SeriesId([0x10; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v1_series")]),
            samples: vec![Sample {
                ts_ns: 0,
                value: 1.0,
            }],
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v1");
    let v2 = SegmentWriter::write_v2(
        vec![SeriesInput {
            series_id: SeriesId([0x20; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v2_series")]),
            samples: vec![Sample {
                ts_ns: 0,
                value: 2.0,
            }],
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v2");
    let v3 = SegmentWriter::write_v3(
        vec![SeriesInputV3 {
            series_id: SeriesId([0x30; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v3_series")]),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: 0,
                value: 3.0,
            }]),
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v3");
    let v4_run = scalar_run_from_v1(
        100,
        1,
        1,
        SeriesId([0x40; 16]),
        vec![Sample {
            ts_ns: 0,
            value: 4.0,
        }],
    );
    let v4 = SegmentWriter::write_v4(
        vec![SeriesInputV4 {
            series_id: SeriesId([0x40; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v4_series")]),
            runs: vec![v4_run],
        }],
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");

    let limits = ReaderLimits::default();
    assert_eq!(
        ravel_segment::open_from_full(v1.bytes.as_ref(), limits)
            .expect("v1 parses")
            .version,
        1
    );
    assert_eq!(
        ravel_segment::open_from_full(v2.bytes.as_ref(), limits)
            .expect("v2 parses")
            .version,
        2
    );
    assert_eq!(
        ravel_segment::open_from_full(v3.bytes.as_ref(), limits)
            .expect("v3 parses")
            .version,
        3
    );
    assert_eq!(
        ravel_segment::open_from_full(v4.bytes.as_ref(), limits)
            .expect("v4 parses")
            .version,
        4
    );
}

/// Patches a real object's trailer version field in place (last 16 bytes:
/// footer_len(4), footer_crc32c(4), version(2), signal(1), reserved(1),
/// magic(4) -- docs/segment-format.md), leaving the footer_crc32c
/// untouched: version isn't crc-covered, so this corrupts only the
/// dispatch decision, not the footer's own integrity.
fn patch_trailer_version(bytes: &[u8], version: u16) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let len = out.len();
    let version_offset = len - 8;
    out[version_offset..version_offset + 2].copy_from_slice(&version.to_le_bytes());
    out
}

#[test]
fn version_five_trailer_is_rejected_as_unsupported() {
    let v4_run = scalar_run_from_v1(
        100,
        1,
        1,
        SeriesId([0x40; 16]),
        vec![Sample {
            ts_ns: 0,
            value: 4.0,
        }],
    );
    let v4 = SegmentWriter::write_v4(
        vec![SeriesInputV4 {
            series_id: SeriesId([0x40; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v4_series")]),
            runs: vec![v4_run],
        }],
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");
    let patched = patch_trailer_version(v4.bytes.as_ref(), 5);
    let limits = ReaderLimits::default();
    assert_eq!(
        ravel_segment::open_from_full(&patched, limits).unwrap_err(),
        SegmentError::UnsupportedVersion(5)
    );
}

// --- 4. Cross-version grammar confusion: a v3-shaped SERIES_META body    ---
// --- read through the v4 grammar (v4 has an extra run_total header and   ---
// --- ten extra run-major columns v3 never wrote), and vice versa. This   ---
// --- feeds one version's real section bytes straight into the other      ---
// --- version's decode_catalog_vN, which is the actual grammar boundary:  ---
// --- decode_catalog_vN never inspects the trailer version itself, so     ---
// --- patching the trailer (and thus needing to recompute the version-    ---
// --- covered footer_crc32c) would only be testing dispatch, not grammar. ---
// --- Neither call may panic; both must fail closed with a typed error,   ---
// --- never silently misdecode.                                          ---

#[test]
fn v3_object_reread_as_v4_fails_closed_not_panics() {
    let v3 = SegmentWriter::write_v3(
        vec![SeriesInputV3 {
            series_id: SeriesId([0x30; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v3_series")]),
            values: SeriesValues::Scalar(vec![Sample {
                ts_ns: 0,
                value: 3.0,
            }]),
        }],
        test_identity(),
        test_bounds(),
    )
    .expect("writes v3");
    let bytes = v3.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses v3 footer");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT).to_vec();
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS).to_vec();
    let meta = section_bytes(bytes, &loc.footer, SERIES_META).to_vec();
    let footer = loc.footer.clone();
    let result = std::panic::catch_unwind(|| {
        ravel_segment::decode_catalog_v4(&footer, &dict, &ids, &meta, limits)
    });
    let decoded = result.expect("must not panic on cross-version-confused bytes");
    assert!(
        decoded.is_err(),
        "v3 grammar misread as v4 must fail closed, not silently decode"
    );
}

#[test]
fn v4_object_reread_as_v3_fails_closed_not_panics() {
    let v4_run = scalar_run_from_v1(
        100,
        1,
        1,
        SeriesId([0x40; 16]),
        vec![Sample {
            ts_ns: 0,
            value: 4.0,
        }],
    );
    let v4 = SegmentWriter::write_v4(
        vec![SeriesInputV4 {
            series_id: SeriesId([0x40; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "v4_series")]),
            runs: vec![v4_run],
        }],
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");
    let bytes = v4.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses v4 footer");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT).to_vec();
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS).to_vec();
    let meta = section_bytes(bytes, &loc.footer, SERIES_META).to_vec();
    let footer = loc.footer.clone();
    let result = std::panic::catch_unwind(|| {
        ravel_segment::decode_catalog_v3(&footer, &dict, &ids, &meta, limits)
    });
    let decoded = result.expect("must not panic on cross-version-confused bytes");
    assert!(
        decoded.is_err(),
        "v4 grammar misread as v3 must fail closed, not silently decode"
    );
}

// --- 5. Truncation fail-closed, mirroring reader_v3.rs's                ---
// --- v3_object_truncation_always_fails_closed.                          ---

#[test]
fn v4_object_truncation_always_fails_closed() {
    let run1 = scalar_run_from_v1(
        100,
        1,
        1,
        SeriesId([0x01; 16]),
        vec![Sample {
            ts_ns: 0,
            value: 1.0,
        }],
    );
    let hist_run = histogram_run_from_v3(
        100,
        1,
        1,
        SeriesId([0x02; 16]),
        vec![ravel_segment::HistogramSample {
            ts_ns: 0,
            value: sample_histogram_value(1),
        }],
    );
    let written = SegmentWriter::write_v4(
        vec![
            SeriesInputV4 {
                series_id: SeriesId([0x01; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "scalar_m")]),
                runs: vec![run1],
            },
            SeriesInputV4 {
                series_id: SeriesId([0x02; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "hist_m")]),
                runs: vec![hist_run],
            },
        ],
        test_identity(),
        test_bounds(),
        test_compaction_meta(),
    )
    .expect("writes v4");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();

    for cut in 1..bytes.len() {
        let truncated = &bytes[..cut];
        let result = std::panic::catch_unwind(|| ravel_segment::open_from_full(truncated, limits));
        assert!(result.is_ok(), "panicked at truncation length {cut}");
    }
}

// --- 6. Corpus: hand-crafted v4 SERIES_META scenarios, one per new       ---
// --- Corrupted rule in docs/compaction-retention-plan.md section 4.      ---
// --- Mirrors tests/reader_v3.rs's MetaSpecV3/build_scenario_v3/push_block ---
// --- approach, extended with the run_total header and the ten run-major  ---
// --- columns.                                                            ---

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

fn push_block(buf: &mut Vec<u8>, values: &[u64]) {
    push_block_padded(buf, values, 0);
}

/// Like `push_block`, but appends `pad` extra zero bytes inside the block
/// after its real varint content, with the declared block_len covering
/// them -- reproduces "block_len drift" (a declared length that outruns
/// what the column's own varints actually consume) deterministically,
/// without hand-computing byte offsets into the buffer.
fn push_block_padded(buf: &mut Vec<u8>, values: &[u64], pad: usize) {
    let mut block = Vec::new();
    for v in values {
        write_uvarint(&mut block, *v);
    }
    block.extend(std::iter::repeat_n(0u8, pad));
    write_uvarint(buf, block.len() as u64);
    buf.extend_from_slice(&block);
}

fn push_byte_block(buf: &mut Vec<u8>, values: &[u8]) {
    write_uvarint(buf, values.len() as u64);
    buf.extend_from_slice(values);
}

/// A structurally-valid-by-default v4 SERIES_META specification
/// (docs/compaction-retention-plan.md section 4): `run_total` is the
/// header field the writer emits right after the schema list; every
/// other field here is one of the fourteen tail blocks
/// `parse_series_meta_tail_v4` reads (`value_kind` and `run_count` are
/// series-major, the remaining ten are run-major, matching
/// `build_series_meta_v4`'s column order in writer.rs).
#[derive(Clone)]
struct MetaSpecV4 {
    count: u32,
    run_total: u32,
    schemas: Vec<Vec<u64>>,
    schema_ref: Vec<u64>,
    value_ord: Vec<Vec<u64>>,
    value_kind: Vec<u8>,
    run_count: Vec<u64>,
    run_created_delta: Vec<u64>,
    run_epoch: Vec<u64>,
    run_seq: Vec<u64>,
    run_sample_count: Vec<u64>,
    run_min_ts_delta: Vec<u64>,
    run_ts_span: Vec<u64>,
    ts_page_gap: Vec<u64>,
    ts_page_len: Vec<u64>,
    val_page_gap: Vec<u64>,
    val_page_len: Vec<u64>,
    hist_page_gap: Vec<u64>,
    hist_page_len: Vec<u64>,
    ts_page_len_drift_pad: usize,
}

impl MetaSpecV4 {
    fn baseline_scalar() -> Self {
        MetaSpecV4 {
            count: 1,
            run_total: 1,
            schemas: vec![vec![0]], // schema 0: dict ordinal 0 ("app")
            schema_ref: vec![0],
            value_ord: vec![vec![1]], // "va"
            value_kind: vec![0],      // Scalar
            run_count: vec![1],
            run_created_delta: vec![0],
            run_epoch: vec![1],
            run_seq: vec![1],
            run_sample_count: vec![1],
            run_min_ts_delta: vec![0],
            run_ts_span: vec![0],
            ts_page_gap: vec![0],
            ts_page_len: vec![5],
            val_page_gap: vec![0],
            val_page_len: vec![8],
            hist_page_gap: vec![0],
            hist_page_len: vec![0],
            ts_page_len_drift_pad: 0,
        }
    }

    fn baseline_histogram() -> Self {
        MetaSpecV4 {
            count: 1,
            run_total: 1,
            schemas: vec![vec![0]],
            schema_ref: vec![0],
            value_ord: vec![vec![1]],
            value_kind: vec![1], // Histogram
            run_count: vec![1],
            run_created_delta: vec![0],
            run_epoch: vec![1],
            run_seq: vec![1],
            run_sample_count: vec![1],
            run_min_ts_delta: vec![0],
            run_ts_span: vec![0],
            ts_page_gap: vec![0],
            ts_page_len: vec![5],
            val_page_gap: vec![0],
            val_page_len: vec![0],
            hist_page_gap: vec![0],
            hist_page_len: vec![10],
            ts_page_len_drift_pad: 0,
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
        buf.extend_from_slice(&self.run_total.to_le_bytes());
        push_block(&mut buf, &self.schema_ref);
        let mut vo_block = Vec::new();
        for vals in &self.value_ord {
            for v in vals {
                write_uvarint(&mut vo_block, *v);
            }
        }
        write_uvarint(&mut buf, vo_block.len() as u64);
        buf.extend_from_slice(&vo_block);
        push_byte_block(&mut buf, &self.value_kind);
        push_block(&mut buf, &self.run_count);
        push_block(&mut buf, &self.run_created_delta);
        push_block(&mut buf, &self.run_epoch);
        push_block(&mut buf, &self.run_seq);
        push_block(&mut buf, &self.run_sample_count);
        push_block(&mut buf, &self.run_min_ts_delta);
        push_block(&mut buf, &self.run_ts_span);
        push_block(&mut buf, &self.ts_page_gap);
        push_block_padded(&mut buf, &self.ts_page_len, self.ts_page_len_drift_pad);
        push_block(&mut buf, &self.val_page_gap);
        push_block(&mut buf, &self.val_page_len);
        push_block(&mut buf, &self.hist_page_gap);
        push_block(&mut buf, &self.hist_page_len);
        buf
    }
}

/// Builds a complete, self-consistent (footer, LABEL_DICT, SERIES_IDS,
/// SERIES_META) tuple around a `MetaSpecV4`, mirroring
/// tests/reader_v3.rs's `build_scenario_v3`.
fn build_scenario_v4(
    dict_strings: &[&str],
    ids: &[[u8; 16]],
    spec: &MetaSpecV4,
    include_val_pages: bool,
    include_hist_pages: bool,
) -> (Footer, Vec<u8>, Vec<u8>, Vec<u8>) {
    let dict_bytes = build_label_dict_bytes(dict_strings);
    let ids_bytes = build_series_ids_bytes(ids);
    let meta_bytes = spec.build();

    let section = |kind: u32, bytes: &[u8]| Section {
        kind,
        offset: 0,
        len: bytes.len() as u64,
        crc32c: crc32c::crc32c(bytes),
        comp: 0,
        uncompressed_len: bytes.len() as u64,
    };

    let mut sections = vec![
        section(LABEL_DICT, &dict_bytes),
        section(SERIES_IDS, &ids_bytes),
        section(SERIES_META, &meta_bytes),
        Section {
            kind: TS_PAGES,
            offset: 0,
            len: 200,
            crc32c: 0,
            comp: 0,
            uncompressed_len: 200,
        },
    ];
    if include_val_pages {
        sections.push(Section {
            kind: VAL_PAGES,
            offset: 0,
            len: 200,
            crc32c: 0,
            comp: 0,
            uncompressed_len: 200,
        });
    }
    if include_hist_pages {
        sections.push(Section {
            kind: HIST_PAGES,
            offset: 0,
            len: 200,
            crc32c: 0,
            comp: 0,
            uncompressed_len: 200,
        });
    }

    let footer = Footer {
        tenant_hash: vec![0u8; 16],
        shard: 0,
        writer_id: "corpus-v4".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
        min_event_ts_ns: 0,
        max_event_ts_ns: 0,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        sample_count: spec.run_sample_count.iter().sum(),
        series_count: u64::from(spec.count),
        sections,
        base_created_unix_ns: 0,
        ingest_hour_bucket: 0,
        input_set_hash: Vec::new(),
        part_index: 0,
        level: 0,
    };
    (footer, dict_bytes, ids_bytes, meta_bytes)
}

fn decode_v4(
    footer: &Footer,
    dict: &[u8],
    ids: &[u8],
    meta: &[u8],
) -> Result<Vec<SeriesEntryV4>, SegmentError> {
    ravel_segment::decode_catalog_v4(footer, dict, ids, meta, ReaderLimits::default())
}

#[test]
fn corpus_v4_baseline_scalar_decodes_successfully() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV4::baseline_scalar();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    let entries = decode_v4(&footer, &dict, &ids_bytes, &meta).expect("baseline scalar decodes");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry.value_kind, ValueKind::Scalar);
    assert_eq!(entries[0].runs.len(), 1);
}

#[test]
fn corpus_v4_baseline_histogram_decodes_successfully() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV4::baseline_histogram();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, false, true);
    let entries = decode_v4(&footer, &dict, &ids_bytes, &meta).expect("baseline histogram decodes");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry.value_kind, ValueKind::Histogram);
    assert_eq!(entries[0].runs[0].hist_page, (0, 10));
}

#[test]
fn corpus_v4_zero_run_count_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_count = vec![0];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ZeroRunCount)
    );
}

#[test]
fn corpus_v4_run_count_sum_mismatch_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_count = vec![2]; // header declares run_total = 1
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::RunCountSumMismatch {
            run_count_sum: 2,
            run_total: 1,
        })
    );
}

#[test]
fn corpus_v4_created_unix_ns_provenance_overflow_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_created_delta = vec![u64::MAX];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    let mut footer = footer;
    footer.base_created_unix_ns = i64::MAX;
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ProvenanceBoundsOverflow)
    );
}

#[test]
fn corpus_v4_min_ts_provenance_overflow_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_min_ts_delta = vec![u64::MAX];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    let mut footer = footer;
    footer.min_event_ts_ns = i64::MAX;
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ProvenanceBoundsOverflow)
    );
}

#[test]
fn corpus_v4_ts_span_provenance_overflow_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_min_ts_delta = vec![0];
    spec.run_ts_span = vec![u64::MAX];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ProvenanceBoundsOverflow)
    );
}

#[test]
fn corpus_v4_zero_sample_count_run_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.run_sample_count = vec![0];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, false);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ZeroSampleCount)
    );
}

#[test]
fn corpus_v4_scalar_series_with_hist_page_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.hist_page_len = vec![6]; // Scalar-kind but claims hist bytes
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ScalarSeriesHasHistPage)
    );
}

#[test]
fn corpus_v4_histogram_series_with_val_page_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_histogram();
    spec.val_page_len = vec![8]; // Histogram-kind but claims val bytes
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::HistogramSeriesHasValPage)
    );
}

#[test]
fn corpus_v4_histogram_series_with_zero_hist_page_len_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_histogram();
    spec.hist_page_len = vec![0];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ZeroHistPageLen)
    );
}

/// A run claims `value_kind = Scalar` but has `val_page_len == 0` and no
/// VAL_PAGES section at all: every per-run bounds/kind check in
/// `parse_series_meta_tail_v4` passes trivially (the "no bytes" (0, 0)
/// shape is legal for a run of the *other* kind), so only the aggregate,
/// run-weighted value_kind-count-vs-page-count check in
/// `check_value_kind_pages_v4` catches this.
#[test]
fn corpus_v4_scalar_run_count_page_count_mismatch_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_scalar();
    spec.val_page_len = vec![0];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, false, false);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ValueKindPageCountMismatch {
            kind: "VAL_SCALAR",
            value_kind_count: 1,
            page_count: 0,
        })
    );
}

#[test]
fn corpus_v4_unexpected_val_pages_section_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV4::baseline_histogram();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::UnexpectedSectionPresent("VAL_PAGES"))
    );
}

#[test]
fn corpus_v4_unexpected_hist_pages_section_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV4::baseline_scalar();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::UnexpectedSectionPresent("HIST_PAGES"))
    );
}

#[test]
fn corpus_v4_series_meta_trailing_bytes_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV4::baseline_histogram();
    let (footer, dict, ids_bytes, mut meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, false, true);
    meta.push(0xFF); // trailing byte past the last block
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
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::TrailingBytes)
    );
}

/// A declared block_len that outruns the block's real varint content
/// (docs/compaction-retention-plan.md section 4: every SERIES_META column
/// must consume exactly its declared block_len) -- here on `ts_page_len`,
/// picked arbitrarily among the ten run-major columns since the check is
/// identical for all of them.
#[test]
fn corpus_v4_series_meta_bad_block_len_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV4::baseline_histogram();
    spec.ts_page_len_drift_pad = 1;
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v4(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v4(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::BadBlockLen)
    );
}
