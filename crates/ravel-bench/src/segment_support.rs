//! Shared segment build/decode plumbing for the segment_* benches and the
//! gorilla_vs_raw size test. Section kind numbers (1..4) are the persistent
//! format contract from docs/segment-format.md, not re-exported by
//! `ravel_segment`, so they are named here once.
//!
//! `expect` is used freely here: this module only ever runs on segments this
//! same crate just built from generator output, so a failure is a bench bug,
//! not a runtime condition to recover from.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    Footer, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesEntry, SeriesInput,
    WrittenSegment, decode_catalog, decode_catalog_matching, open_from_full,
};
use ravel_types::{LabelSet, Sample, SeriesId};

pub const LABEL_DICT: u32 = 1;
pub const SERIES_TABLE: u32 = 2;
pub const VAL_PAGES: u32 = 4;

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
