//! Shared segment build/decode plumbing for the segment_* benches and the
//! gorilla_vs_raw size test. Section kind numbers (1..6) are the persistent
//! format contract from docs/segment-format.md, not re-exported by
//! `ravel_segment`, so they are named here once.
//!
//! `_v2` counterparts below write and decode RSEG v2 objects
//! (`SegmentWriter::write_v2` / `decode_catalog_v2`, ADR-0014,
//! docs/rseg-v2-plan.md), added alongside the v1 helpers rather than as a
//! parameter on them so every existing caller (the segment_* benches,
//! `gorilla_vs_raw`, `alignment_measurement`, `profile_hotspots`) keeps its
//! v1 behavior unchanged.
//!
//! `expect` is used freely here: this module only ever runs on segments this
//! same crate just built from generator output, so a failure is a bench bug,
//! not a runtime condition to recover from.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    CompactionMetaV4, Footer, IngestBounds, ReaderLimits, RunInputV4, RunValuePageV4,
    SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput, SeriesInputV3, SeriesInputV4,
    SeriesValues, WrittenSegment, decode_catalog, decode_catalog_matching,
    decode_catalog_matching_v2, decode_catalog_v2, open_from_full,
};
use ravel_types::{LabelSet, Sample, SeriesId};

pub const LABEL_DICT: u32 = 1;
pub const SERIES_TABLE: u32 = 2;
pub const TS_PAGES: u32 = 3;
pub const VAL_PAGES: u32 = 4;
/// v2+ sections (v1 objects never emit these; ADR-0014).
pub const SERIES_IDS: u32 = 5;
pub const SERIES_META: u32 = 6;
/// RSEG v5 sparse-catalog sections (ADR-0026, kinds frozen in
/// `ravel_segment::format::section_kind`). Named here for the selective-read
/// bench and byte gates.
pub const SERIES_IDX: u32 = 8;
pub const SERIES_META_CHUNKS: u32 = 9;

pub fn bench_identity() -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: [0u8; 16],
        shard: 0,
        writer_id: "bench-writer".to_string(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

pub fn bench_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    }
}

pub fn bench_meta() -> CompactionMetaV4 {
    CompactionMetaV4 {
        ingest_hour_bucket: 0,
        input_set_hash: [0u8; 32],
        part_index: 0,
        level: 1,
    }
}

/// Encodes `raw` series into one segment object via the public writer API.
pub fn build_segment(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    let series = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect();
    SegmentWriter::write(series, bench_identity(), bench_bounds()).expect("encode bench segment")
}

/// v2 counterpart of [`build_segment`]: encodes `raw` series via
/// `SegmentWriter::write_v2`.
pub fn build_segment_v2(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    let series = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect();
    SegmentWriter::write_v2(series, bench_identity(), bench_bounds())
        .expect("encode bench segment v2")
}

/// v3 counterpart of [`build_segment`]: encodes `raw` series via
/// `SegmentWriter::write_v3` on the scalar path (every series' payload is
/// `SeriesValues::Scalar`). v3's catalog layout (LABEL_DICT + SERIES_IDS +
/// SERIES_META) is unchanged from v2, so this exercises the same catalog
/// byte gates as [`build_segment_v2`] (docs/rseg-v3-plan.md section 3.4).
pub fn build_segment_v3(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    let series = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInputV3 {
            series_id,
            labels,
            values: SeriesValues::Scalar(samples),
        })
        .collect();
    SegmentWriter::write_v3(series, bench_identity(), bench_bounds())
        .expect("encode bench segment v3")
}

/// Converts a scalar workload into single-run RSEG v4 inputs by writing one
/// v2 object over the whole batch and slicing each series' verbatim
/// TS_PAGES/VAL_PAGES bytes (page crc32c is bound to series_id, preserved).
/// This is the bench counterpart of the compaction pipeline's real
/// verbatim-page copy: v4/v5 writers take pre-framed runs, never raw samples.
pub fn raw_to_v4_inputs(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> Vec<SeriesInputV4> {
    let v2 = build_segment_v2(raw);
    let obj = v2.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v2 for v4 inputs");
    let footer = &loc.footer;
    let entries = decode_catalog_v2(
        footer,
        section_bytes(obj, footer, LABEL_DICT),
        section_bytes(obj, footer, SERIES_IDS),
        section_bytes(obj, footer, SERIES_META),
        limits,
    )
    .expect("decode v2 catalog for v4 inputs");
    let ts_sec = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_sec = footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .unwrap();
    entries
        .iter()
        .map(|e| {
            let (o, l) = e.ts_page;
            let a = (ts_sec.offset + o) as usize;
            let ts_page = obj[a..a + l as usize].to_vec();
            let (o, l) = e.val_page;
            let a = (val_sec.offset + o) as usize;
            let val_page = obj[a..a + l as usize].to_vec();
            SeriesInputV4 {
                series_id: e.series_id,
                labels: e.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 0,
                    writer_epoch: 0,
                    writer_seq: 0,
                    min_ts_ns: e.min_ts_ns,
                    max_ts_ns: e.max_ts_ns,
                    sample_count: e.sample_count,
                    ts_page,
                    value_page: RunValuePageV4::Scalar(val_page),
                }],
            }
        })
        .collect()
}

/// v4 counterpart of [`build_segment_v2`]: encodes `raw` as single-run v4.
pub fn build_segment_v4(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    SegmentWriter::write_v4(
        raw_to_v4_inputs(raw),
        bench_identity(),
        bench_bounds(),
        bench_meta(),
    )
    .expect("encode bench segment v4")
}

/// v5 counterpart of [`build_segment_v2`]: encodes `raw` as single-run v5,
/// which emits the sparse SERIES_IDX + chunked SERIES_META sections when the
/// object carries at least the v5 threshold of series (ADR-0026).
pub fn build_segment_v5(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    SegmentWriter::write_v5(
        raw_to_v4_inputs(raw),
        bench_identity(),
        bench_bounds(),
        bench_meta(),
    )
    .expect("encode bench segment v5")
}

pub fn slice_range(bytes: &[u8], range: (u64, u64)) -> &[u8] {
    let start = range.0 as usize;
    let end = start + range.1 as usize;
    &bytes[start..end]
}

pub fn section_bytes<'a>(bytes: &'a [u8], footer: &Footer, kind: u32) -> &'a [u8] {
    let section = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .expect("section present");
    slice_range(bytes, (section.offset, section.len))
}

/// Opens the footer and decodes the full series catalog (LABEL_DICT +
/// SERIES_TABLE), without touching TS/VAL page bytes.
pub fn decode_entries(bytes: &[u8]) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let label_dict_bytes = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let series_table_bytes = section_bytes(bytes, &loc.footer, SERIES_TABLE);
    let entries = decode_catalog(&loc.footer, label_dict_bytes, series_table_bytes, limits)
        .expect("decode catalog");
    (loc.footer, entries)
}

/// Opens the footer and decodes only the series matching all `equals`
/// pairs, via the ordinal-matching lazy path
/// (`ravel_segment::decode_catalog_matching`).
pub fn decode_matching_entries(
    bytes: &[u8],
    equals: &[(&str, &str)],
) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let label_dict_bytes = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let series_table_bytes = section_bytes(bytes, &loc.footer, SERIES_TABLE);
    let entries = decode_catalog_matching(
        &loc.footer,
        label_dict_bytes,
        series_table_bytes,
        equals,
        limits,
    )
    .expect("decode matching catalog");
    (loc.footer, entries)
}

/// v2 counterpart of [`decode_entries`]: decodes the full series catalog
/// (LABEL_DICT + SERIES_IDS + SERIES_META) via `decode_catalog_v2`.
pub fn decode_entries_v2(bytes: &[u8]) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let label_dict_bytes = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let series_ids_bytes = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let series_meta_bytes = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = decode_catalog_v2(
        &loc.footer,
        label_dict_bytes,
        series_ids_bytes,
        series_meta_bytes,
        limits,
    )
    .expect("decode catalog v2");
    (loc.footer, entries)
}

/// v2 counterpart of [`decode_matching_entries`]: decodes only the series
/// matching all `equals` pairs via the v2 lazy ordinal-matching path
/// (`ravel_segment::decode_catalog_matching_v2`).
pub fn decode_matching_entries_v2(
    bytes: &[u8],
    equals: &[(&str, &str)],
) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let label_dict_bytes = section_bytes(bytes, &loc.footer, LABEL_DICT);
    let series_ids_bytes = section_bytes(bytes, &loc.footer, SERIES_IDS);
    let series_meta_bytes = section_bytes(bytes, &loc.footer, SERIES_META);
    let entries = decode_catalog_matching_v2(
        &loc.footer,
        label_dict_bytes,
        series_ids_bytes,
        series_meta_bytes,
        equals,
        limits,
    )
    .expect("decode matching catalog v2");
    (loc.footer, entries)
}

/// Raw VAL page bytes for one series entry (header + payload, undecoded):
/// byte 0 is the enc tag (16 Gorilla, 17 raw f64), bytes 6.. are the payload.
pub fn val_page_bytes<'a>(bytes: &'a [u8], footer: &Footer, entry: &SeriesEntry) -> &'a [u8] {
    let val_section = footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .expect("VAL_PAGES section present");
    let (off, len) = entry.val_page;
    let abs_off = (val_section.offset + off) as usize;
    &bytes[abs_off..abs_off + len as usize]
}
