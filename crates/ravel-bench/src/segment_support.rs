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
    CompactionMetaV4, Footer, IngestBounds, ReaderLimits, RunEntry, RunInputV4, RunValuePageV4,
    SegmentIdentity, SegmentWriter, SeriesEntry, SeriesEntryV4, SeriesInputV3, SeriesInputV4,
    SeriesValues, WrittenSegment, decode_catalog_v5, decode_run_pages_soa, encode_run_v4,
    open_from_full,
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

// --- production single-run page access for the codec bake-offs ----------
//
// The value and timestamp bake-offs (`src/bin/codec_bakeoff.rs`,
// `src/bin/ts_bakeoff.rs`) need the *production* encoding of one page, reached
// through the public API rather than reimplemented. `encode_run_v4` is the only
// public entry that frames a real RSEG run (a 6-byte-header TS page under the
// TS_DELTA_VARINT + 64-byte-floor-LZ4 rule, and a VAL page under the
// Gorilla/raw-fallback rule), and `decode_run_pages_soa` is its inverse. Both
// frame a whole run, so a value measurement carries a fixed TS-framing cost the
// payload-only challengers do not; the bins state this in their output.

/// Fixed synthetic timestamp base and 15 s step for a value-codec run. A value
/// codec only cares about the value payload, but the production writer frames a
/// whole run, so it is handed regular millisecond-epoch timestamps.
const BAKEOFF_TS_START_NS: i64 = 1_700_000_000_000_000_000;
const BAKEOFF_TS_STEP_NS: i64 = 15_000_000_000;

/// A fixed series id for bake-off runs. Any value works: the page crc binds
/// `series_id || enc || comp || payload`, and encode and decode here use the
/// same id, so verification passes.
fn bakeoff_series_id() -> SeriesId {
    SeriesId([7u8; 16])
}

/// One scalar run framed through the production writer ([`encode_run_v4`]) over
/// `values` with synthetic regular timestamps. The value bake-off reads
/// [`scalar_val_page`] off the result; the timestamps exist only because the
/// run frames a TS page too.
pub fn production_scalar_run(values: &[f64]) -> RunInputV4 {
    let samples: Vec<Sample> = values
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample {
            ts_ns: BAKEOFF_TS_START_NS + i as i64 * BAKEOFF_TS_STEP_NS,
            value,
        })
        .collect();
    encode_run_v4(
        &bakeoff_series_id(),
        0,
        0,
        0,
        &SeriesValues::Scalar(samples),
    )
    .expect("encode scalar run through production writer")
}

/// The framed VAL page of a scalar run: byte 0 is the enc tag (16 Gorilla,
/// 17 raw f64), byte 1 the comp tag, bytes 2..6 the crc, bytes 6.. the payload.
pub fn scalar_val_page(run: &RunInputV4) -> &[u8] {
    let RunValuePageV4::Scalar(bytes) = &run.value_page else {
        panic!("production_scalar_run always frames a scalar value page");
    };
    bytes
}

/// One scalar run framed through the production writer over `ts_ns` (values are
/// irrelevant to the TS page, so they are all zero). The timestamp bake-off
/// reads `ts_page` off the result: a real 6-byte-header TS_DELTA_VARINT page,
/// LZ4-compressed exactly when the writer's 64-byte floor rule fires.
pub fn production_ts_run(ts_ns: &[i64]) -> RunInputV4 {
    let samples: Vec<Sample> = ts_ns
        .iter()
        .map(|&ts_ns| Sample { ts_ns, value: 0.0 })
        .collect();
    encode_run_v4(
        &bakeoff_series_id(),
        0,
        0,
        0,
        &SeriesValues::Scalar(samples),
    )
    .expect("encode scalar run through production writer")
}

/// Decodes a production scalar run's TS and VAL pages back to `(timestamps,
/// values)` through [`decode_run_pages_soa`], the inverse of
/// [`production_scalar_run`]. Used to time the production decode path and to
/// assert the production round trip is bit-exact.
pub fn decode_scalar_run(run: &RunInputV4) -> (Vec<i64>, Vec<f64>) {
    let entry = RunEntry {
        created_unix_ns: run.created_unix_ns,
        writer_epoch: run.writer_epoch,
        writer_seq: run.writer_seq,
        sample_count: run.sample_count,
        min_ts_ns: run.min_ts_ns,
        max_ts_ns: run.max_ts_ns,
        ts_page: (0, 0),
        val_page: (0, 0),
        hist_page: (0, 0),
    };
    let RunValuePageV4::Scalar(val_bytes) = &run.value_page else {
        panic!("production_scalar_run always frames a scalar value page");
    };
    let mut scratch = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    decode_run_pages_soa(
        &bakeoff_series_id(),
        &entry,
        &run.ts_page,
        val_bytes,
        ReaderLimits::default(),
        &mut scratch,
        &mut timestamps,
        &mut values,
    )
    .expect("decode scalar run");
    (timestamps, values)
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
