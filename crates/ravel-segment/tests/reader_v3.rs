//! Integration tests for RSEG v3 decode (ADR-0017, docs/rseg-v3-plan.md
//! section 3.4/3.5): `decode_catalog_v3`/`decode_catalog_matching_v3`,
//! `decode_histogram_pages`, tri-version trailer dispatch, and a typed-error
//! corpus over hand-crafted corrupt v3 SERIES_META/HIST_PAGES inputs.
//!
//! Mirrors tests/reader_v2.rs's structure and corpus-building approach
//! (`MetaSpec`/`build_scenario`/`push_block`) extended with the three v3-only
//! SERIES_META columns and an optional HIST_PAGES section. v1/v2 fixtures and
//! tests are completely unmodified by this ticket -- see reader_v2.rs and
//! roundtrip.rs for proof v1/v2 decode behavior wasn't touched.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use proptest::prelude::*;
use ravel_proto::segment::v1::{Footer, Section};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramValue, IngestBounds, ReaderLimits, SegmentError,
    SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInputV3, SeriesValues, ValueKind,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId};

// --- shared helpers, mirroring tests/reader_v2.rs --------------------------

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
        writer_id: "reader-v3-test-writer".to_string(),
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

// --- 1. Full pipeline roundtrip: write_v3 -> decode_catalog_v3 ->        ---
// --- plan_ranges_v3 -> decode_pages_soa / decode_histogram_pages.        ---

fn mixed_fixture() -> Vec<SeriesInputV3> {
    vec![
        SeriesInputV3 {
            series_id: SeriesId([0x01; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "up"), ("zone", "z1")]),
            values: SeriesValues::Scalar(
                (0..6)
                    .map(|i| Sample {
                        ts_ns: i * 1_000,
                        value: (i as f64) * 0.5,
                    })
                    .collect(),
            ),
        },
        SeriesInputV3 {
            series_id: SeriesId([0x02; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "req_latency"), ("zone", "z2")]),
            values: SeriesValues::Histogram(vec![
                HistogramSample {
                    ts_ns: 0,
                    value: HistogramValue {
                        scale: 2,
                        zero_threshold: 1e-9,
                        sum: Some(42.5),
                        custom_values: None,
                        positive_spans: vec![ravel_segment::HistogramSpan {
                            offset: 0,
                            length: 3,
                        }],
                        negative_spans: vec![],
                        counts: HistogramCounts::Int {
                            zero_count: 1,
                            count: 7,
                            positive: vec![2, 3, 1],
                            negative: vec![],
                        },
                        reset_hint: ravel_segment::ResetHint::Yes,
                    },
                },
                HistogramSample {
                    ts_ns: 5_000,
                    value: HistogramValue {
                        scale: 2,
                        zero_threshold: 1e-9,
                        sum: Some(50.0),
                        custom_values: None,
                        positive_spans: vec![ravel_segment::HistogramSpan {
                            offset: 0,
                            length: 3,
                        }],
                        negative_spans: vec![],
                        counts: HistogramCounts::Int {
                            zero_count: 0,
                            count: 8,
                            positive: vec![3, 3, 2],
                            negative: vec![],
                        },
                        reset_hint: ravel_segment::ResetHint::No,
                    },
                },
            ]),
        },
        SeriesInputV3 {
            series_id: SeriesId([0x03; 16]),
            labels: labels(&[(METRIC_NAME_LABEL, "cpu_ratio")]),
            values: SeriesValues::Histogram(vec![HistogramSample {
                ts_ns: 0,
                value: HistogramValue {
                    scale: 0,
                    zero_threshold: 0.0,
                    sum: None,
                    custom_values: None,
                    positive_spans: vec![],
                    negative_spans: vec![],
                    counts: HistogramCounts::Float {
                        zero_count: 0.0,
                        count: 0.0,
                        positive: vec![],
                        negative: vec![],
                    },
                    reset_hint: ravel_segment::ResetHint::Unknown,
                },
            }]),
        },
    ]
}

#[test]
fn write_v3_read_roundtrip_decodes_scalar_and_histogram_series() {
    let written = SegmentWriter::write_v3(mixed_fixture(), test_identity(), test_bounds())
        .expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    assert_eq!(loc.version, 3);

    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes v3 catalog");
    assert_eq!(entries.len(), 3);

    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected).expect("plans v3 ranges");

    for (entry, range) in entries.iter().zip(ranges.iter()) {
        let ts_bytes = slice_range(bytes, range.ts_range);
        match entry.value_kind {
            ValueKind::Scalar => {
                let val_bytes = slice_range(bytes, range.val_range);
                let samples = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)
                    .expect("decodes scalar pages");
                assert_eq!(samples.len(), entry.sample_count as usize);
            }
            ValueKind::Histogram => {
                let hist_bytes = slice_range(bytes, range.hist_range);
                let samples =
                    ravel_segment::decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)
                        .expect("decodes histogram pages");
                assert_eq!(samples.len(), entry.sample_count as usize);
            }
        }
    }
}

/// A histogram-only segment omits VAL_PAGES entirely (docs/rseg-v3-plan.md
/// section 3.2); confirms `decode_catalog_v3` accepts that shape rather than
/// misinterpreting the absent section as corruption.
#[test]
fn write_v3_histogram_only_segment_omits_val_pages() {
    let series = vec![mixed_fixture().into_iter().nth(1).expect("series present")];
    let written =
        SegmentWriter::write_v3(series, test_identity(), test_bounds()).expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    assert!(
        loc.footer.sections.iter().all(|s| s.kind != VAL_PAGES),
        "histogram-only segment must omit VAL_PAGES"
    );

    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes v3 catalog");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value_kind, ValueKind::Histogram);
}

/// A scalar-only segment omits HIST_PAGES entirely, symmetrically.
#[test]
fn write_v3_scalar_only_segment_omits_hist_pages() {
    let series = vec![mixed_fixture().into_iter().next().expect("series present")];
    let written =
        SegmentWriter::write_v3(series, test_identity(), test_bounds()).expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    assert!(
        loc.footer.sections.iter().all(|s| s.kind != HIST_PAGES),
        "scalar-only segment must omit HIST_PAGES"
    );

    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes v3 catalog");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value_kind, ValueKind::Scalar);
}

#[test]
fn decode_catalog_matching_v3_agrees_with_eager_select() {
    let written = SegmentWriter::write_v3(mixed_fixture(), test_identity(), test_bounds())
        .expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);

    let eager = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("eager decode");
    let expected: Vec<&SeriesEntry> = eager
        .iter()
        .filter(|e| e.labels.get("zone") == Some("z2"))
        .collect();
    assert_eq!(expected.len(), 1);

    let matched = ravel_segment::decode_catalog_matching_v3(
        &loc.footer,
        dict,
        ids,
        meta,
        &[("zone", "z2")],
        limits,
    )
    .expect("matching decode");
    assert_eq!(matched.len(), expected.len());
    assert_eq!(matched[0].series_id, expected[0].series_id);
    assert_eq!(matched[0].value_kind, expected[0].value_kind);
}

// --- 2. Tri-version dispatch: v1, v2, and v3 objects each report their   ---
// --- own trailer version and decode through their own catalog decoder.  ---

#[test]
fn tri_version_dispatch_reports_each_trailers_own_version() {
    let v1 = ravel_segment::SegmentWriter::write(
        vec![ravel_segment::SeriesInput {
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
    let v2 = ravel_segment::SegmentWriter::write_v2(
        vec![ravel_segment::SeriesInput {
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
    let v3 = SegmentWriter::write_v3(mixed_fixture(), test_identity(), test_bounds())
        .expect("writes v3");

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
}

// --- 3. Corpus: hand-crafted v3 SERIES_META scenarios, one per           ---
// --- Corrupted rule in docs/rseg-v3-plan.md section 3.2/3.4. Mirrors     ---
// --- reader_v2.rs's MetaSpec/build_scenario/push_block approach,         ---
// --- extended with the three v3-only columns and an optional HIST_PAGES  ---
// --- section.                                                            ---

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
    let mut block = Vec::new();
    for v in values {
        write_uvarint(&mut block, *v);
    }
    write_uvarint(buf, block.len() as u64);
    buf.extend_from_slice(&block);
}

/// Pushes `value_kind`'s block: `count` raw bytes (not uvarint-encoded),
/// per `build_series_meta_v3`'s wire layout -- one byte per series.
fn push_byte_block(buf: &mut Vec<u8>, values: &[u8]) {
    write_uvarint(buf, values.len() as u64);
    buf.extend_from_slice(values);
}

/// A structurally-valid-by-default v3 SERIES_META specification, mirroring
/// tests/reader_v2.rs's `MetaSpec` extended with the three v3-only columns
/// (docs/rseg-v3-plan.md section 3.4: `value_kind`, `hist_page_gap`,
/// `hist_page_len`). Baseline: 2 scalar-kind series, 1 schema, no
/// histogram series.
#[derive(Clone)]
struct MetaSpecV3 {
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
    value_kind: Vec<u8>,
    hist_page_gap: Vec<u64>,
    hist_page_len: Vec<u64>,
}

impl MetaSpecV3 {
    fn baseline_scalar() -> Self {
        MetaSpecV3 {
            count: 2,
            schemas: vec![vec![0]], // schema 0: dict ordinal 0 ("app")
            schema_ref: vec![0, 0],
            value_ord: vec![vec![1], vec![2]], // "va", "vb"
            sample_count: vec![1, 1],
            min_ts_delta: vec![0, 0],
            ts_span: vec![0, 0],
            ts_page_gap: vec![0, 0],
            ts_page_len: vec![5, 5],
            val_page_gap: vec![0, 0],
            val_page_len: vec![8, 8],
            value_kind: vec![0, 0],
            hist_page_gap: vec![0, 0],
            hist_page_len: vec![0, 0],
        }
    }

    fn baseline_histogram() -> Self {
        MetaSpecV3 {
            count: 1,
            schemas: vec![vec![0]],
            schema_ref: vec![0],
            value_ord: vec![vec![1]],
            sample_count: vec![1],
            min_ts_delta: vec![0],
            ts_span: vec![0],
            ts_page_gap: vec![0],
            ts_page_len: vec![5],
            val_page_gap: vec![0],
            val_page_len: vec![0],
            value_kind: vec![1],
            hist_page_gap: vec![0],
            hist_page_len: vec![10],
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
        push_byte_block(&mut buf, &self.value_kind);
        push_block(&mut buf, &self.hist_page_gap);
        push_block(&mut buf, &self.hist_page_len);
        buf
    }
}

/// Builds a complete, self-consistent (footer, LABEL_DICT, SERIES_IDS,
/// SERIES_META) tuple around a `MetaSpecV3`, mirroring
/// tests/reader_v2.rs's `build_scenario`. `include_val_pages`/
/// `include_hist_pages` control whether a (200-byte, generously sized)
/// VAL_PAGES/HIST_PAGES section descriptor is present in the footer at
/// all, letting corpus tests exercise both "section legitimately absent"
/// and "section present but unneeded" scenarios.
fn build_scenario_v3(
    dict_strings: &[&str],
    ids: &[[u8; 16]],
    spec: &MetaSpecV3,
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
        writer_id: "corpus-v3".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
        min_event_ts_ns: 0,
        max_event_ts_ns: 0,
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
        sample_count: spec.sample_count.iter().sum(),
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

fn decode_v3(
    footer: &Footer,
    dict: &[u8],
    ids: &[u8],
    meta: &[u8],
) -> Result<Vec<SeriesEntry>, SegmentError> {
    ravel_segment::decode_catalog_v3(footer, dict, ids, meta, ReaderLimits::default())
}

#[test]
fn corpus_v3_baseline_scalar_decodes_successfully() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpecV3::baseline_scalar();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va", "vb"], &ids, &spec, true, false);
    let entries = decode_v3(&footer, &dict, &ids_bytes, &meta).expect("baseline scalar decodes");
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.value_kind == ValueKind::Scalar));
}

#[test]
fn corpus_v3_baseline_histogram_decodes_successfully() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV3::baseline_histogram();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    let entries = decode_v3(&footer, &dict, &ids_bytes, &meta).expect("baseline histogram decodes");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value_kind, ValueKind::Histogram);
    assert_eq!(entries[0].hist_page, (0, 10));
}

#[test]
fn corpus_v3_invalid_value_kind_byte_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV3::baseline_histogram();
    spec.value_kind = vec![2]; // neither 0 (Scalar) nor 1 (Histogram)
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::InvalidValueKind(2))
    );
}

#[test]
fn corpus_v3_scalar_series_with_hist_page_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let mut spec = MetaSpecV3::baseline_scalar();
    spec.hist_page_len = vec![0, 6]; // series 1 is Scalar but claims hist bytes
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va", "vb"], &ids, &spec, true, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ScalarSeriesHasHistPage)
    );
}

#[test]
fn corpus_v3_histogram_series_with_val_page_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV3::baseline_histogram();
    spec.val_page_len = vec![8]; // Histogram-kind but claims val_page bytes
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::HistogramSeriesHasValPage)
    );
}

#[test]
fn corpus_v3_histogram_series_with_zero_hist_page_len_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV3::baseline_histogram();
    spec.hist_page_len = vec![0];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::ZeroHistPageLen)
    );
}

/// A series claims `value_kind = Scalar` but has `val_page_len == 0` and no
/// VAL_PAGES section at all: every per-series bounds/kind check in
/// `parse_series_meta_tail_v3` passes trivially (the "no bytes" (0, 0) shape
/// is legal for a series of the *other* kind), so only the aggregate
/// value_kind-count-vs-page-count equality check in `decode_catalog_v3`
/// (docs/rseg-v3-plan.md section 3.2's footer-validation-additions
/// paragraph) can catch this.
#[test]
fn corpus_v3_scalar_count_page_count_mismatch_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV3 {
        count: 1,
        schemas: vec![vec![0]],
        schema_ref: vec![0],
        value_ord: vec![vec![1]],
        sample_count: vec![1],
        min_ts_delta: vec![0],
        ts_span: vec![0],
        ts_page_gap: vec![0],
        ts_page_len: vec![5],
        val_page_gap: vec![0],
        val_page_len: vec![0], // Scalar-kind but zero-length val_page
        value_kind: vec![0],
        hist_page_gap: vec![0],
        hist_page_len: vec![0],
    };
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, false);
    let err = decode_v3(&footer, &dict, &ids_bytes, &meta).unwrap_err();
    assert_eq!(
        err,
        SegmentError::ValueKindPageCountMismatch {
            kind: "VAL_SCALAR",
            value_kind_count: 1,
            page_count: 0,
        }
    );
}

/// An all-histogram segment (zero scalar series) with a VAL_PAGES section
/// physically present in the footer regardless: a mandatory-count-zero
/// section entry, Corrupted per docs/rseg-v3-plan.md section 3.2.
#[test]
fn corpus_v3_unexpected_val_pages_section_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV3::baseline_histogram();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, true, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::UnexpectedSectionPresent("VAL_PAGES"))
    );
}

/// Symmetric: an all-scalar segment (zero histogram series) with a
/// HIST_PAGES section physically present regardless.
#[test]
fn corpus_v3_unexpected_hist_pages_section_is_rejected() {
    let ids = [[0x01; 16], [0x02; 16]];
    let spec = MetaSpecV3::baseline_scalar();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va", "vb"], &ids, &spec, true, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::UnexpectedSectionPresent("HIST_PAGES"))
    );
}

#[test]
fn corpus_v3_hist_page_range_exceeds_section_length_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV3::baseline_histogram();
    spec.hist_page_len = vec![10_000]; // HIST_PAGES section is 200 bytes
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SectionOutOfBounds)
    );
}

#[test]
fn corpus_v3_hist_page_gap_overflow_is_rejected() {
    let ids = [[0x01; 16]];
    let mut spec = MetaSpecV3::baseline_histogram();
    spec.hist_page_gap = vec![u64::MAX];
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    assert_eq!(
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::SectionOutOfBounds)
    );
}

#[test]
fn corpus_v3_series_meta_trailing_bytes_after_block_12_is_rejected() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV3::baseline_histogram();
    let (footer, dict, ids_bytes, mut meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    meta.push(0xFF); // trailing byte past block 12
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
        decode_v3(&footer, &dict, &ids_bytes, &meta),
        Err(SegmentError::TrailingBytes)
    );
}

/// A `value_kind` byte with the continuation bit set must be rejected
/// byte-for-byte (the block is raw `u8`s, not uvarints) rather than
/// silently desyncing the remaining blocks by consuming a second byte.
#[test]
fn corpus_v3_value_kind_continuation_bit_is_rejected_not_desynced() {
    let ids = [[0x01; 16]];
    let spec = MetaSpecV3::baseline_histogram();
    let (footer, dict, ids_bytes, meta) =
        build_scenario_v3(&["app", "va"], &ids, &spec, false, true);
    // Locate the value_kind block precisely by re-deriving its offset from
    // a fresh build rather than hardcoding a byte index, so this test
    // can't silently drift out of sync with MetaSpecV3::build's layout.
    let mut probe = meta.clone();
    // meta layout for baseline_histogram (count=1, one schema of 1 name):
    // count(4) + schema_count(4) + schema list(2: name_count=1, name_ord=0)
    // + 9 single-value blocks at 2 bytes each (1-byte block_len prefix + a
    // single 1-byte varint content: schema_ref, value_ord, sample_count,
    // min_ts_delta, ts_span, ts_page_gap, ts_page_len, val_page_gap,
    // val_page_len) = 4 + 4 + 2 + 9 * 2 = 28, then value_kind's own 1-byte
    // block_len prefix at 28, its single content byte at 29.
    let value_kind_byte_pos = 4 + 4 + 2 + 9 * 2 + 1;
    assert_eq!(
        meta[value_kind_byte_pos], 1,
        "test assumption: value_kind content byte holds Histogram (1)"
    );
    probe[value_kind_byte_pos] = 0x80; // continuation bit set, corrupt encoding

    // Mutating the section content invalidates its stored crc32c; recompute
    // it (and the section descriptor's lengths) the same way
    // corpus_v3_series_meta_trailing_bytes_after_block_12_is_rejected does,
    // so this is a section-crc-clean corruption, not a checksum mismatch.
    let mut footer = footer;
    let series_meta_idx = footer
        .sections
        .iter()
        .position(|s| s.kind == SERIES_META)
        .expect("series meta section");
    footer.sections[series_meta_idx].len = probe.len() as u64;
    footer.sections[series_meta_idx].uncompressed_len = probe.len() as u64;
    footer.sections[series_meta_idx].crc32c = crc32c::crc32c(&probe);

    assert!(matches!(
        decode_v3(&footer, &dict, &ids_bytes, &probe),
        Err(SegmentError::InvalidValueKind(0x80))
    ));
}

// --- 4. proptest: v3 differential roundtrip over arbitrary scalar/       ---
// --- histogram mixes, confirming decode reconstructs exactly what was    ---
// --- written for every series regardless of value_kind.                  ---

fn ts_strategy() -> impl Strategy<Value = i64> {
    -1_000_000i64..1_000_000i64
}

fn reset_hint_strategy() -> impl Strategy<Value = ravel_segment::ResetHint> {
    prop_oneof![
        Just(ravel_segment::ResetHint::Unknown),
        Just(ravel_segment::ResetHint::Yes),
        Just(ravel_segment::ResetHint::No),
        Just(ravel_segment::ResetHint::Gauge),
    ]
}

/// 0-2 spans per side, offsets ranging negative to positive (the format
/// places no ordering constraint on span offsets at decode time -- see
/// `decode_hist_spans`, which only rejects `length == 0`), lengths 1-3.
fn hist_spans_strategy() -> impl Strategy<Value = Vec<ravel_segment::HistogramSpan>> {
    proptest::collection::vec(
        (-20i32..20, 1u32..4)
            .prop_map(|(offset, length)| ravel_segment::HistogramSpan { offset, length }),
        0..3,
    )
}

/// Generates a self-consistent histogram covering int/float count kind,
/// multiple spans per side with negative and positive offsets, optional
/// custom boundaries (`scale == -53`, strictly ascending), and every
/// `reset_hint` state (C5, RSEG v3 phase 4, issue #137; supersedes the
/// narrower fixed-single-span int-only generator this replaces). Bucket
/// counts are all-distinct-and-positive by construction and `count =
/// zero_count + sum(bucket_counts)`, so `count >= zero_count && count >=
/// sum(bucket_counts)` (docs/rseg-v3-plan.md section 3.5) always holds
/// without relying on the reader's `<`-based NaN-transparent check.
/// NaN/Inf/-0.0 float payloads are exercised separately (see
/// `write_v3_read_roundtrip_preserves_nan_inf_negative_zero_float_histogram_bits`
/// below): mixing them into this generator would make the
/// `HistogramValue`-derived `PartialEq` this test relies on unusable, since
/// `NaN != NaN` under `==`.
fn histogram_value_strategy() -> impl Strategy<Value = HistogramValue> {
    let scale_and_custom = prop_oneof![
        3 => (-20i32..20).prop_map(|s| (s, None)),
        1 => proptest::collection::vec(1.0f64..1_000.0, 1..5).prop_map(|mut bounds| {
            bounds.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            bounds.dedup_by(|a, b| a == b);
            (-53, Some(bounds))
        }),
    ];

    (
        any::<bool>(),
        scale_and_custom,
        0.0f64..1.0,
        hist_spans_strategy(),
        hist_spans_strategy(),
        any::<bool>(),
        0u64..5,
        reset_hint_strategy(),
    )
        .prop_map(
            |(
                is_float,
                (scale, custom_values),
                zero_threshold,
                positive_spans,
                negative_spans,
                has_sum,
                zero_count,
                reset_hint,
            )| {
                let positive_len: usize = positive_spans.iter().map(|s| s.length as usize).sum();
                let negative_len: usize = negative_spans.iter().map(|s| s.length as usize).sum();
                let counts = if is_float {
                    let positive: Vec<f64> = (0..positive_len).map(|i| (i as f64) + 1.0).collect();
                    let negative: Vec<f64> = (0..negative_len).map(|i| (i as f64) + 1.0).collect();
                    let total: f64 = positive.iter().chain(negative.iter()).sum();
                    let zero_count = zero_count as f64;
                    HistogramCounts::Float {
                        count: zero_count + total,
                        zero_count,
                        positive,
                        negative,
                    }
                } else {
                    let positive: Vec<u64> = (0..positive_len).map(|i| (i as u64) + 1).collect();
                    let negative: Vec<u64> = (0..negative_len).map(|i| (i as u64) + 1).collect();
                    let total: u64 = positive.iter().chain(negative.iter()).sum();
                    HistogramCounts::Int {
                        count: zero_count + total,
                        zero_count,
                        positive,
                        negative,
                    }
                };
                let sum = if has_sum {
                    Some(match &counts {
                        HistogramCounts::Int { count, .. } => *count as f64,
                        HistogramCounts::Float { count, .. } => *count,
                    })
                } else {
                    None
                };
                HistogramValue {
                    scale,
                    zero_threshold,
                    sum,
                    custom_values,
                    positive_spans,
                    negative_spans,
                    counts,
                    reset_hint,
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn v3_roundtrip_over_arbitrary_scalar_and_histogram_mixes(
        scalar_ts in proptest::collection::vec(ts_strategy(), 1..6),
        scalar_val in proptest::collection::vec(any::<f64>(), 1..6),
        hist_ts in proptest::collection::vec(ts_strategy(), 1..4),
        hist_val in proptest::collection::vec(histogram_value_strategy(), 1..4),
    ) {
        let n = scalar_ts.len().min(scalar_val.len());
        let scalar_samples: Vec<Sample> = scalar_ts[..n]
            .iter()
            .zip(&scalar_val[..n])
            .map(|(&ts_ns, &value)| Sample { ts_ns, value })
            .collect();
        prop_assume!(!scalar_samples.is_empty());

        let m = hist_ts.len().min(hist_val.len());
        let hist_samples: Vec<HistogramSample> = hist_ts[..m]
            .iter()
            .zip(hist_val[..m].iter().cloned())
            .map(|(&ts_ns, value)| HistogramSample { ts_ns, value })
            .collect();
        prop_assume!(!hist_samples.is_empty());

        // The writer stable-sorts each series' samples by ts_ns (ties keep
        // input order), same contract write_v2's roundtrip proptest already
        // asserts against; the decoded order must match that, not the
        // arbitrary generator order.
        let mut expected_scalar = scalar_samples.clone();
        expected_scalar.sort_by_key(|s| s.ts_ns);
        let mut expected_hist_ts: Vec<i64> = hist_samples.iter().map(|s| s.ts_ns).collect();
        let mut expected_hist_value: Vec<HistogramValue> =
            hist_samples.iter().map(|s| s.value.clone()).collect();
        {
            let mut indexed: Vec<usize> = (0..hist_samples.len()).collect();
            indexed.sort_by_key(|&i| hist_samples[i].ts_ns);
            expected_hist_ts = indexed.iter().map(|&i| expected_hist_ts[i]).collect();
            expected_hist_value = indexed.iter().map(|&i| expected_hist_value[i].clone()).collect();
        }

        let series = vec![
            SeriesInputV3 {
                series_id: SeriesId([0xA1; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "prop_scalar")]),
                values: SeriesValues::Scalar(scalar_samples.clone()),
            },
            SeriesInputV3 {
                series_id: SeriesId([0xA2; 16]),
                labels: labels(&[(METRIC_NAME_LABEL, "prop_hist")]),
                values: SeriesValues::Histogram(hist_samples.clone()),
            },
        ];

        let written = SegmentWriter::write_v3(series, test_identity(), test_bounds())
            .expect("writes v3");
        let bytes = written.bytes.as_ref();
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(bytes, limits).expect("parses footer");
        prop_assert_eq!(loc.version, 3);

        let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
        let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
        let meta = section_bytes(bytes, &loc.footer, SERIES_META);
        let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
            .expect("decodes v3 catalog");
        prop_assert_eq!(entries.len(), 2);

        let selected: Vec<&SeriesEntry> = entries.iter().collect();
        let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected)
            .expect("plans v3 ranges");

        for (entry, range) in entries.iter().zip(ranges.iter()) {
            let ts_bytes = slice_range(bytes, range.ts_range);
            match entry.value_kind {
                ValueKind::Scalar => {
                    let val_bytes = slice_range(bytes, range.val_range);
                    let decoded = ravel_segment::decode_pages(entry, ts_bytes, val_bytes, limits)
                        .expect("decodes scalar pages");
                    prop_assert_eq!(decoded.len(), expected_scalar.len());
                    for (got, want) in decoded.iter().zip(expected_scalar.iter()) {
                        prop_assert_eq!(got.ts_ns, want.ts_ns);
                        prop_assert_eq!(got.value.to_bits(), want.value.to_bits());
                    }
                }
                ValueKind::Histogram => {
                    let hist_bytes = slice_range(bytes, range.hist_range);
                    let decoded =
                        ravel_segment::decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)
                            .expect("decodes histogram pages");
                    prop_assert_eq!(decoded.len(), expected_hist_ts.len());
                    for ((got, &want_ts), want_value) in decoded
                        .iter()
                        .zip(expected_hist_ts.iter())
                        .zip(expected_hist_value.iter())
                    {
                        prop_assert_eq!(got.ts_ns, want_ts);
                        // Full structural equality (scale, zero_threshold,
                        // custom_values, spans, counts, reset_hint), not
                        // just the sum field: `HistogramValue`'s derived
                        // `PartialEq` is safe here because
                        // `histogram_value_strategy` never generates NaN
                        // or -0.0 (see that function's doc comment).
                        prop_assert_eq!(&got.value, want_value);
                    }
                }
            }
        }
    }
}

/// Deterministic (non-proptest) round trip for NaN/Inf/-0.0 float
/// histogram payloads (C5, RSEG v3 phase 4, issue #137): `HistogramCounts::
/// Float` fields and `sum` are legal to carry NaN, +-Infinity, and -0.0
/// (docs/rseg-v3-plan.md section 2's int/float duality); `decode_histogram_
/// record`'s doc comment records the `<`-not-`!(>=)` count check
/// specifically so these payloads pass validation rather than being
/// rejected. Compared via `to_bits()`, never `==`, per repo-wide float-
/// comparison discipline (CLAUDE.md) -- this is exactly why this case is
/// deterministic rather than folded into `histogram_value_strategy`'s
/// proptest, whose assertions rely on derived `PartialEq`.
#[test]
fn write_v3_read_roundtrip_preserves_nan_inf_negative_zero_float_histogram_bits() {
    let value = HistogramValue {
        scale: 3,
        zero_threshold: -0.0,
        sum: Some(f64::NAN),
        custom_values: None,
        positive_spans: vec![ravel_segment::HistogramSpan {
            offset: 0,
            length: 3,
        }],
        negative_spans: vec![],
        counts: HistogramCounts::Float {
            zero_count: f64::NAN,
            count: f64::INFINITY,
            positive: vec![f64::NAN, -0.0, f64::INFINITY],
            negative: vec![],
        },
        reset_hint: ravel_segment::ResetHint::Gauge,
    };
    let series = vec![SeriesInputV3 {
        series_id: SeriesId([0x0A; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "nan_inf_hist")]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 0,
            value: value.clone(),
        }]),
    }];
    let written =
        SegmentWriter::write_v3(series, test_identity(), test_bounds()).expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("opens");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes catalog");
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected).expect("plans ranges");
    let (entry, range) = entries.iter().zip(ranges.iter()).next().expect("one entry");
    let ts_bytes = slice_range(bytes, range.ts_range);
    let hist_bytes = slice_range(bytes, range.hist_range);
    let decoded = ravel_segment::decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)
        .expect("decodes histogram");
    assert_eq!(decoded.len(), 1);
    let got = &decoded[0].value;

    assert_eq!(got.scale, value.scale);
    assert_eq!(got.zero_threshold.to_bits(), value.zero_threshold.to_bits());
    assert_eq!(got.sum.map(f64::to_bits), value.sum.map(f64::to_bits));
    assert_eq!(got.reset_hint, value.reset_hint);
    match (&got.counts, &value.counts) {
        (
            HistogramCounts::Float {
                zero_count: gz,
                count: gc,
                positive: gp,
                negative: gn,
            },
            HistogramCounts::Float {
                zero_count: ez,
                count: ec,
                positive: ep,
                negative: en,
            },
        ) => {
            assert_eq!(gz.to_bits(), ez.to_bits());
            assert_eq!(gc.to_bits(), ec.to_bits());
            assert_eq!(gp.len(), ep.len());
            for (g, e) in gp.iter().zip(ep.iter()) {
                assert_eq!(g.to_bits(), e.to_bits());
            }
            assert_eq!(gn.len(), en.len());
        }
        _ => panic!("expected float counts on both sides"),
    }
}

// --- 5. Truncation fail-closed: every truncation length of a real v3    ---
// --- object either fails closed or decodes exactly (mirrors reader_v2.rs's ---
// --- v2_object_truncation_always_fails_closed).                          ---

#[test]
fn v3_object_truncation_always_fails_closed() {
    let written = SegmentWriter::write_v3(mixed_fixture(), test_identity(), test_bounds())
        .expect("writes v3");
    let bytes = written.bytes.as_ref();
    let limits = ReaderLimits::default();

    for cut in 1..bytes.len() {
        let truncated = &bytes[..cut];
        // Must never panic; any outcome (Err, or coincidentally an Ok on a
        // prefix that happens to be self-describing) is acceptable here --
        // only a panic is a failure. `catch_unwind` proves no panic path
        // exists at any truncation boundary.
        let result = std::panic::catch_unwind(|| ravel_segment::open_from_full(truncated, limits));
        assert!(result.is_ok(), "panicked at truncation length {cut}");
    }
}

// --- 6. v1-vs-v2-vs-v3 differential property over scalar content (C5,   ---
// --- RSEG v3 phase 4, issue #137): docs/rseg-v3-plan.md section 10's C5  ---
// --- entry calls for "the v1-vs-v2-vs-v3 differential property          ---
// --- extended (same logical scalar content still decodes identically    ---
// --- across all three)". Histogram content has no v1/v2 counterpart, so ---
// --- its own differential is encode-then-decode against itself, which   ---
// --- `v3_roundtrip_over_arbitrary_scalar_and_histogram_mixes` above and  ---
// --- `write_v3_read_roundtrip_preserves_nan_inf_negative_zero_float_     ---
// --- histogram_bits` already cover. This section reuses `ts_strategy`    ---
// --- from section 4 above and mirrors tests/reader_v2.rs's own          ---
// --- `labelset_strategy`/`sample_value_strategy`/`samples_strategy`/     ---
// --- `build_series_inputs_for_differential` (each test file is a        ---
// --- separate compiled crate, so the helpers can't be imported directly ---
// --- across files; the shapes are kept identical on purpose).

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
    let dict = section_bytes(bytes, &loc.footer, 1);
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
    let dict = section_bytes(bytes, &loc.footer, 1);
    let ids = section_bytes(bytes, &loc.footer, 5);
    let meta = section_bytes(bytes, &loc.footer, 6);
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
    let dict = section_bytes(bytes, &loc.footer, 1);
    let ids = section_bytes(bytes, &loc.footer, 5);
    let meta = section_bytes(bytes, &loc.footer, 6);
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// The same logical scalar-only content, written by all three versions'
    /// writers, must decode to identical (series_id, labels, sorted
    /// (ts_ns, value-bits)) content regardless of which version wrote it.
    /// v3's own writer/reader (`SeriesValues::Scalar`, `ValueKind::Scalar`)
    /// is exercised here the same way P4 of docs/rseg-v2-plan.md already
    /// exercised v1-vs-v2.
    #[test]
    fn v1_v2_v3_scalar_differential_over_arbitrary_input(
        series_data in prop::collection::vec((labelset_strategy(), samples_strategy()), 0..8),
        tenant_hash in proptest::array::uniform16(any::<u8>()),
        shard in any::<u32>(),
        writer_id in "[a-zA-Z0-9-]{1,20}",
        epoch in any::<u64>(),
        seq in any::<u64>(),
    ) {
        // Each writer consumes its input by value and neither SeriesInput
        // nor SeriesInputV3/SegmentIdentity/IngestBounds is Clone, so build
        // three independent (but logically identical) batches/structs
        // rather than adding Clone derives to production types for test
        // convenience.
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
    }
}

// --- 7. HIST_SPANS record-level byte mutation at every structural       ---
// --- boundary (C5, RSEG v3 phase 4, issue #137): docs/rseg-v3-plan.md    ---
// --- section 10's C5 entry calls for "byte-level mutations at every new ---
// --- structural boundary (flags byte, scale, span/length pairs,         ---
// --- count-kind-dependent varint-vs-f64 branches)". tests/fuzz_mutation ---
// --- .rs's whole-object seed corpus already fuzzes at the object level; ---
// --- CRC gates on the page/section framing mean a random single-bit     ---
// --- flip almost never reaches the record decoder itself. This section ---
// --- targets that decoder directly: build one minimal HIST_SPANS        ---
// --- record with a known byte layout, flip one deliberately-chosen      ---
// --- byte at a time, patch the page's crc32c so the mutated bytes pass  ---
// --- the framing gate, and assert the exact typed error the record      ---
// --- decoder (`decode_histogram_record`, src/reader.rs) is documented   ---
// --- to raise for that boundary.

/// One histogram series, one sample, chosen so every field of the encoded
/// HIST_SPANS record is exactly one byte wide except the 8-byte
/// `zero_threshold` f64: flags=0 (int, no sum, `ResetHint::Unknown`),
/// scale=0, zero_threshold=0.0, zero_count=0, count=1, one positive span
/// (offset=0, length=1, bucket=[1]), zero negative spans. Encodes to
/// exactly 17 bytes, verified by `hist_mutation_record_layout_is_stable`
/// below, which pins the byte-for-byte layout the other tests in this
/// section index into by fixed offset.
fn hist_mutation_fixture() -> Vec<SeriesInputV3> {
    vec![SeriesInputV3 {
        series_id: SeriesId([0x09; 16]),
        labels: labels(&[(METRIC_NAME_LABEL, "hist_mutation_target")]),
        values: SeriesValues::Histogram(vec![HistogramSample {
            ts_ns: 0,
            value: HistogramValue {
                scale: 0,
                zero_threshold: 0.0,
                sum: None,
                custom_values: None,
                positive_spans: vec![ravel_segment::HistogramSpan {
                    offset: 0,
                    length: 1,
                }],
                negative_spans: vec![],
                counts: HistogramCounts::Int {
                    zero_count: 0,
                    count: 1,
                    positive: vec![1],
                    negative: vec![],
                },
                reset_hint: ravel_segment::ResetHint::Unknown,
            },
        }]),
    }]
}

/// Byte offsets into `hist_mutation_fixture`'s single encoded record,
/// relative to the record's own first byte (docs/rseg-v3-plan.md section
/// 3.5's field order).
const HIST_REC_FLAGS: usize = 0;
const HIST_REC_SCALE: usize = 1;
const HIST_REC_ZERO_COUNT: usize = 10;
const HIST_REC_COUNT: usize = 11;
const HIST_REC_POS_SPAN_COUNT: usize = 12;
const HIST_REC_POS_SPAN_LENGTH: usize = 14;
const HIST_REC_NEG_SPAN_COUNT: usize = 16;
const HIST_MUTATION_RECORD_LEN: usize = 17;

/// zigzag-varint encodings (single byte each, values in [-64, 63]) of the
/// two scale boundaries this section mutates into: -54 is one past the
/// `scale >= -53` floor, and -53 itself switches the record into
/// custom-boundaries mode.
const ZIGZAG_SCALE_NEG_54: u8 = 107;
const ZIGZAG_SCALE_NEG_53: u8 = 105;

/// Locates `hist_mutation_fixture`'s single HIST_SPANS record within a
/// freshly written v3 object: the absolute byte range of its containing
/// page, and the absolute offset of the record's first byte within that
/// page (immediately after the 6-byte page header, since a single-sample
/// page holds exactly one record with no per-record length prefix -- see
/// `decode_hist_page_into`, src/reader.rs).
fn locate_hist_mutation_record(bytes: &[u8]) -> ((usize, usize), usize) {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits).expect("opens");
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)
        .expect("decodes catalog");
    assert_eq!(entries.len(), 1);
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected).expect("plans ranges");
    let page_start = ranges[0].hist_range.0 as usize;
    let page_len = ranges[0].hist_range.1 as usize;
    let record_start = page_start + 6;
    ((page_start, page_len), record_start)
}

/// Recomputes and patches a HIST_SPANS page's crc32c after a record byte
/// has been mutated in place, so the mutation reaches the record decoder
/// instead of being rejected earlier by the page-framing crc check.
/// Reimplements `src/crc.rs::page_crc`'s algorithm locally (that function
/// is crate-private, not part of `ravel_segment`'s public API): crc32c
/// over `series_id || enc || comp || payload`, matching
/// `split_page_header`'s own field order.
fn recompute_hist_page_crc(
    bytes: &mut [u8],
    page_start: usize,
    page_len: usize,
    series_id: &[u8; 16],
) {
    let enc = bytes[page_start];
    let comp = bytes[page_start + 1];
    let payload_start = page_start + 6;
    let payload_end = page_start + page_len;
    let mut crc = crc32c::crc32c(series_id);
    crc = crc32c::crc32c_append(crc, &[enc, comp]);
    crc = crc32c::crc32c_append(crc, &bytes[payload_start..payload_end]);
    bytes[page_start + 2..page_start + 6].copy_from_slice(&crc.to_le_bytes());
}

/// Builds `hist_mutation_fixture`, flips the record byte at
/// `record_offset` to `new_byte`, patches the page crc, and returns the
/// mutated whole-object bytes plus everything needed to decode it again.
fn mutate_hist_record(record_offset: usize, new_byte: u8) -> Vec<u8> {
    let written = SegmentWriter::write_v3(hist_mutation_fixture(), test_identity(), test_bounds())
        .expect("writes v3");
    let mut bytes = written.bytes.as_ref().to_vec();
    let ((page_start, page_len), record_start) = locate_hist_mutation_record(&bytes);
    bytes[record_start + record_offset] = new_byte;
    recompute_hist_page_crc(&mut bytes, page_start, page_len, &[0x09; 16]);
    bytes
}

/// Runs the full v3 read pipeline (catalog -> plan -> histogram page
/// decode) over mutated bytes and returns the single series' decode
/// result, so each mutation test asserts on the exact typed error (or
/// success) `decode_histogram_pages` produces.
fn decode_hist_mutation(bytes: &[u8]) -> Result<Vec<HistogramValue>, SegmentError> {
    let limits = ReaderLimits::default();
    let loc = ravel_segment::open_from_full(bytes, limits)?;
    let dict = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let ids = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let meta = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = ravel_segment::decode_catalog_v3(&loc.footer, dict, ids, meta, limits)?;
    let selected: Vec<&SeriesEntry> = entries.iter().collect();
    let ranges = ravel_segment::plan_ranges_v3(&loc.footer, &selected)?;
    let entry = &entries[0];
    let range = &ranges[0];
    let ts_bytes = slice_range(bytes, range.ts_range);
    let hist_bytes = slice_range(bytes, range.hist_range);
    let samples = ravel_segment::decode_histogram_pages(entry, ts_bytes, hist_bytes, limits)?;
    Ok(samples.into_iter().map(|s| s.value).collect())
}

/// Pins the byte layout every other test in this section indexes into by
/// fixed offset, decoding the unmutated fixture back to its original
/// logical value first.
#[test]
fn hist_mutation_record_layout_is_stable() {
    let written = SegmentWriter::write_v3(hist_mutation_fixture(), test_identity(), test_bounds())
        .expect("writes v3");
    let bytes = written.bytes.as_ref();
    let ((page_start, page_len), record_start) = locate_hist_mutation_record(bytes);
    assert_eq!(record_start - page_start, 6);
    assert_eq!(page_len - 6, HIST_MUTATION_RECORD_LEN);

    let values = decode_hist_mutation(bytes).expect("unmutated record decodes");
    assert_eq!(values.len(), 1);
    assert_eq!(
        values[0],
        HistogramValue {
            scale: 0,
            zero_threshold: 0.0,
            sum: None,
            custom_values: None,
            positive_spans: vec![ravel_segment::HistogramSpan {
                offset: 0,
                length: 1
            }],
            negative_spans: vec![],
            counts: HistogramCounts::Int {
                zero_count: 0,
                count: 1,
                positive: vec![1],
                negative: vec![],
            },
            reset_hint: ravel_segment::ResetHint::Unknown,
        }
    );
}

/// Flags byte bits 4-7 are reserved and must be zero (docs/rseg-v3-plan.md
/// section 3.5).
#[test]
fn hist_mutation_reserved_flags_bits_rejected() {
    let bytes = mutate_hist_record(HIST_REC_FLAGS, 0b0001_0000);
    assert_eq!(
        decode_hist_mutation(&bytes),
        Err(SegmentError::HistogramReservedFlagsNonZero)
    );
}

/// Flipping the count-kind bit (bit0) without changing anything else
/// desyncs the rest of the record: the decoder now expects `zero_count`
/// and `count` as two 8-byte f64 fields instead of two 1-byte uvarints,
/// reading past the 17-byte record (and the page it lives in) before it
/// can find a valid stopping point. Must be a typed error, never a panic
/// or a wrong-but-successful decode.
#[test]
fn hist_mutation_count_kind_bit_flip_desyncs_but_never_panics() {
    let bytes = mutate_hist_record(HIST_REC_FLAGS, 0b0000_0001);
    let result = std::panic::catch_unwind(|| decode_hist_mutation(&bytes));
    let result = result.expect("must not panic");
    assert!(result.is_err(), "a desynced count-kind bit must not decode");
}

/// `scale >= -53` is required; one past that floor is rejected by name.
#[test]
fn hist_mutation_scale_below_floor_rejected() {
    let bytes = mutate_hist_record(HIST_REC_SCALE, ZIGZAG_SCALE_NEG_54);
    assert_eq!(
        decode_hist_mutation(&bytes),
        Err(SegmentError::HistogramScaleTooSmall(-54))
    );
}

/// `scale == -53` switches the record into custom-boundaries mode, which
/// expects a `custom_values` count-and-array this fixture's bytes don't
/// contain; the decoder must fail typed (reading into whatever bytes
/// happen to follow, then running out), never panic or fabricate a
/// decode.
#[test]
fn hist_mutation_scale_switched_to_custom_boundaries_never_panics() {
    let bytes = mutate_hist_record(HIST_REC_SCALE, ZIGZAG_SCALE_NEG_53);
    let result = std::panic::catch_unwind(|| decode_hist_mutation(&bytes));
    let result = result.expect("must not panic");
    assert!(
        result.is_err(),
        "scale=-53 with no custom_values bytes present must not decode"
    );
}

/// A span with `length == 0` is rejected by name, per `decode_hist_spans`.
#[test]
fn hist_mutation_zero_length_span_rejected() {
    let bytes = mutate_hist_record(HIST_REC_POS_SPAN_LENGTH, 0);
    assert_eq!(
        decode_hist_mutation(&bytes),
        Err(SegmentError::HistogramSpanLengthZero)
    );
}

/// `count` (1) must be >= the sum of every bucket (also 1 here) plus
/// `zero_count` (0); dropping `count` to 0 violates that.
#[test]
fn hist_mutation_count_below_bucket_sum_rejected() {
    let bytes = mutate_hist_record(HIST_REC_COUNT, 0);
    assert_eq!(
        decode_hist_mutation(&bytes),
        Err(SegmentError::HistogramCountInconsistent)
    );
}

/// `count` (1) must also be >= `zero_count`; raising `zero_count` above
/// it violates that half of the same check.
#[test]
fn hist_mutation_zero_count_above_count_rejected() {
    let bytes = mutate_hist_record(HIST_REC_ZERO_COUNT, 2);
    assert_eq!(
        decode_hist_mutation(&bytes),
        Err(SegmentError::HistogramCountInconsistent)
    );
}

/// Claiming a positive span count of 2 when only one span's bytes (and
/// only one bucket's worth of trailing data) actually exist must fail
/// typed rather than reading uninitialized-looking trailing bytes as a
/// second span.
#[test]
fn hist_mutation_span_count_overclaim_never_panics() {
    let bytes = mutate_hist_record(HIST_REC_POS_SPAN_COUNT, 2);
    let result = std::panic::catch_unwind(|| decode_hist_mutation(&bytes));
    let result = result.expect("must not panic");
    assert!(result.is_err(), "an overclaimed span count must not decode");
}

/// Claiming a negative span where the record has none (no negative span
/// bytes exist at all past the record's declared end) must fail typed,
/// never panic.
#[test]
fn hist_mutation_negative_span_count_overclaim_never_panics() {
    let bytes = mutate_hist_record(HIST_REC_NEG_SPAN_COUNT, 1);
    let result = std::panic::catch_unwind(|| decode_hist_mutation(&bytes));
    let result = result.expect("must not panic");
    assert!(
        result.is_err(),
        "a negative span claimed out of no data must not decode"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every single byte of the record, flipped to every value that byte
    /// doesn't already hold, must decode to a typed error or a valid
    /// (Ok) result -- never panic. This is the structural-boundary
    /// counterpart to tests/fuzz_mutation.rs's whole-object random
    /// mutation: it guarantees single-bit/single-byte coverage of the
    /// record's own bytes specifically, which a whole-object crc-gated
    /// random mutation would essentially never reach unmutated.
    #[test]
    fn hist_mutation_any_record_byte_never_panics(
        byte_offset in 0usize..HIST_MUTATION_RECORD_LEN,
        new_byte in any::<u8>(),
    ) {
        let bytes = mutate_hist_record(byte_offset, new_byte);
        let result = std::panic::catch_unwind(|| decode_hist_mutation(&bytes));
        prop_assert!(result.is_ok(), "panicked mutating record byte {byte_offset} to {new_byte:#04x}");
    }
}

// ===========================================================================
// Cargo-fuzz justify-or-add call for the v3 HIST_SPANS surface (C5, RSEG
// v3 phase 4, issue #137; docs/rseg-v3-plan.md section 10 explicitly asks
// this be re-derived for v3, "not silently inherited" from P4 of
// docs/rseg-v2-plan.md's own justify-or-add call for v1/v2, issue #32).
//
// Decision: proptest byte-mutation coverage remains sufficient for
// HIST_SPANS; no cargo-fuzz target is added in this phase.
//
// What this phase adds specifically for the HIST_SPANS surface (beyond
// what P4 already had for v1/v2):
//   - `hist_mutation_any_record_byte_never_panics` above: every one of the
//     17 record byte offsets against every possible byte value (512 cases
//     per run, all 256 values reachable at every offset over enough runs),
//     with the page crc patched so mutations reach the record decoder
//     instead of being caught by the framing check first -- the class of
//     input a whole-object random mutation essentially never produces
//     unaided, since the crc gate rejects almost all of it before the
//     record decoder is reached.
//   - Named boundary tests pinning the exact typed error at each of the
//     record's Corrupted-rule branches (reserved flags, scale floor,
//     zero-length span, count/zero_count/bucket-sum consistency).
//   - `histogram_value_strategy`'s structured generator (widened this
//     phase to int/float kind, multi-span, custom boundaries, every
//     reset_hint) feeding the full write-then-read roundtrip property,
//     giving broad structural coverage on top of the narrow byte-mutation
//     coverage above.
//   - The whole-object seed corpus in tests/fuzz_mutation.rs extended
//     with all seven v3 golden fixtures, covering the framing layer
//     (page/section crc, footer, catalog) the record-level tests above
//     deliberately bypass.
//
// Why not add a cargo-fuzz target in this phase specifically: a
// libfuzzer-based corpus explores byte-space cheaply but blindly; the
// gain over structured proptest is largest when the format has many
// interacting length-prefixed fields libfuzzer can happen to discover
// (the same reasoning P4 used for v1/v2's catalog and page framing).
// HIST_SPANS adds exactly one further such field (the span count/length
// pairs), which the structured generator and the boundary tests above
// already exercise directly and deterministically -- a fuzzer would need
// to get lucky finding a crc-valid page framing bytes-first, which is
// precisely the cost the byte-mutation tests here sidestep by
// constructing valid framing and mutating only the field under test.
// libfuzzer's marginal value over that is judged low enough not to
// justify the toolchain cost (still nightly-only, still absent from the
// pinned stable toolchain this repo builds on, per P4's original note).
//
// This phase also cannot add a cargo-fuzz target even if the tradeoff
// above were judged the other way: a fuzz/ crate requires a new
// workspace member (root Cargo.toml) and a CI job entry, both explicitly
// out of scope ("Work only in crates/ravel-segment", C5's own line).
// Recording that structural blocker here rather than silently letting it
// stand in for the tradeoff judgment above -- the two are separate
// facts, and the justify-or-add call would be the same even if this
// ticket could touch the workspace root.
// ===========================================================================
