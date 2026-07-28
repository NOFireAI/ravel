//! Shared segment build/decode plumbing for the segment_* benches and the
//! gorilla_vs_raw size test. Section kind numbers are the persistent format
//! contract from docs/segment-format.md, not re-exported by `ravel_segment`,
//! so they are named here once.
//!
//! ADR-0027 leaves v5 the only version: `build_segment` writes a v5 object
//! through the raw-sample adapter, and `decode_entries` /
//! `decode_matching_entries` decode the run-major v5 catalog and fold each
//! (single-run) series to the flat [`SeriesEntry`] view the page-slicing
//! benches consume, so existing callers keep working unchanged.
//!
//! `expect` is used freely here: this module only ever runs on segments this
//! same crate just built from generator output, so a failure is a bench bug,
//! not a runtime condition to recover from.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_segment::{
    CompactionMetaV4, Footer, IngestBounds, ReaderLimits, RunInputV4, RunValuePageV4,
    SegmentIdentity, SegmentWriter, SeriesEntry, SeriesEntryV4, SeriesInputV3, SeriesInputV4,
    SeriesValues, WrittenSegment, decode_catalog_v5, open_from_full,
};
use ravel_types::{LabelSet, Sample, SeriesId};

pub const LABEL_DICT: u32 = 1;
pub const TS_PAGES: u32 = 3;
pub const VAL_PAGES: u32 = 4;
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

/// Encodes `raw` scalar series into one RSEG v5 object via the raw-sample
/// adapter (one run per series, below the sparse threshold for the small
/// workloads the general benches use).
pub fn build_segment(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    let series = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInputV3 {
            series_id,
            labels,
            values: SeriesValues::Scalar(samples),
        })
        .collect();
    SegmentWriter::write_histograms(series, bench_identity(), bench_bounds())
        .expect("encode bench segment")
}

/// Converts a scalar workload into single-run RSEG v4 inputs by writing one
/// v5 object over the whole batch and slicing each series' verbatim
/// TS_PAGES/VAL_PAGES bytes (page crc32c is bound to series_id, preserved).
/// Feeds `build_segment_v5`, whose sparse-emission path (>= threshold series)
/// the selective-read bench and byte gates measure.
pub fn raw_to_v4_inputs(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> Vec<SeriesInputV4> {
    let built = build_segment(raw);
    let obj = built.bytes.as_ref();
    let limits = ReaderLimits::default();
    let loc = open_from_full(obj, limits).expect("open v5 for v4 inputs");
    let footer = &loc.footer;
    let entries = decode_catalog_v5(footer, obj, limits).expect("decode v5 catalog for v4 inputs");
    let ts_sec = footer.sections.iter().find(|s| s.kind == TS_PAGES).unwrap();
    let val_sec = footer
        .sections
        .iter()
        .find(|s| s.kind == VAL_PAGES)
        .unwrap();
    entries
        .iter()
        .map(|e| {
            let run = &e.runs[0];
            let (o, l) = run.ts_page;
            let a = (ts_sec.offset + o) as usize;
            let ts_page = obj[a..a + l as usize].to_vec();
            let (o, l) = run.val_page;
            let a = (val_sec.offset + o) as usize;
            let val_page = obj[a..a + l as usize].to_vec();
            SeriesInputV4 {
                series_id: e.entry.series_id,
                labels: e.entry.labels.clone(),
                runs: vec![RunInputV4 {
                    created_unix_ns: 0,
                    writer_epoch: 0,
                    writer_seq: 0,
                    min_ts_ns: e.entry.min_ts_ns,
                    max_ts_ns: e.entry.max_ts_ns,
                    sample_count: e.entry.sample_count,
                    ts_page,
                    value_page: RunValuePageV4::Scalar(val_page),
                }],
            }
        })
        .collect()
}

/// Encodes `raw` as single-run v5, which emits the sparse SERIES_IDX +
/// chunked SERIES_META sections when the object carries at least the v5
/// threshold of series (docs/segment-format.md).
pub fn build_segment_v5(raw: Vec<(SeriesId, LabelSet, Vec<Sample>)>) -> WrittenSegment {
    SegmentWriter::write_v5(
        raw_to_v4_inputs(raw),
        bench_identity(),
        bench_bounds(),
        bench_meta(),
    )
    .expect("encode bench segment v5")
}

/// Folds a run-major v5 entry to the flat [`SeriesEntry`] view: bench objects
/// are single-run, so runs[0]'s section-relative page ranges become the
/// entry's, and the page-slicing helpers below keep working as they did for
/// v1/v2 entries.
fn fold_entry(e: SeriesEntryV4) -> SeriesEntry {
    let run = e.runs.into_iter().next().expect("bench series has one run");
    SeriesEntry {
        series_id: e.entry.series_id,
        labels: e.entry.labels,
        sample_count: e.entry.sample_count,
        min_ts_ns: e.entry.min_ts_ns,
        max_ts_ns: e.entry.max_ts_ns,
        ts_page: run.ts_page,
        val_page: run.val_page,
        value_kind: e.entry.value_kind,
        hist_page: run.hist_page,
    }
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

/// Opens the footer and decodes the full v5 series catalog (over the whole
/// object), folding each single-run series to a flat [`SeriesEntry`].
pub fn decode_entries(bytes: &[u8]) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let entries = decode_catalog_v5(&loc.footer, bytes, limits).expect("decode catalog");
    let folded = entries.into_iter().map(fold_entry).collect();
    (loc.footer, folded)
}

/// Opens the footer and decodes only the series matching all `equals` pairs
/// (exact name=value), folding each to a flat [`SeriesEntry`].
pub fn decode_matching_entries(
    bytes: &[u8],
    equals: &[(&str, &str)],
) -> (Footer, Vec<SeriesEntry>) {
    let limits = ReaderLimits::default();
    let loc = open_from_full(bytes, limits).expect("open segment");
    let entries = decode_catalog_v5(&loc.footer, bytes, limits).expect("decode catalog");
    let folded = entries
        .into_iter()
        .filter(|e| {
            equals
                .iter()
                .all(|(n, v)| e.entry.labels.get(n).is_some_and(|got| got == *v))
        })
        .map(fold_entry)
        .collect();
    (loc.footer, folded)
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
