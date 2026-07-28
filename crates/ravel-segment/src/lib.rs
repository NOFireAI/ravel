//! RSEG v5 segment format: writer and reader for metric segments.
//!
//! Implementer contract: docs/segment-format.md and ADR-0004. ADR-0027
//! leaves v5 the only readable and writable version. All offsets, lengths,
//! and counts from stored bytes are untrusted; bounds-check everything and
//! verify crc32c on every section/page/footer touched.

mod crc;
mod error;
mod format;
mod gorilla;
mod histogram;
mod identity;
mod reader;
mod sparse;
mod ts_delta;
mod varint;
mod writer;

pub use error::{SegmentError, WriteError};
pub use format::{ReaderLimits, V5_SPARSE_THRESHOLD, V5_STRIDE};

pub use histogram::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
pub use identity::{ExpectedIdentity, check_identity};
pub use reader::{
    FooterLocation, FooterOutcome, PlannedRunRange, RunEntry, SeriesEntry, SeriesEntryV4,
    ValPageKind, ValueKind, decode_catalog_matching_v4, decode_catalog_v4,
    decode_run_histogram_pages, decode_run_pages_soa, open_from_full, open_from_suffix,
    parse_footer, plan_ranges_v4, validate_sections,
};
/// RSEG v5 catalog (docs/segment-format.md): `decode_catalog_v5` is the
/// whole-catalog decode over the whole object (folding to the run-major
/// `decode_catalog_v4` grammar below the sparse threshold). The rest is the
/// point-lookup sparse probe: parse SERIES_IDX, locate a crc-verified
/// SERIES_IDS window, then a crc-verified meta chunk, then decode that
/// series' runs.
pub use sparse::{
    ChunkLocation, IdWindow, SparseIdIndex, decode_catalog_v5, decode_chunk_runs,
    find_index_in_window, parse_series_idx, verify_and_decompress_chunk_frame, verify_id_window,
};
pub use writer::{
    CompactionMetaV4, HistogramSample, IngestBounds, RunInputV4, RunValuePageV4, SegmentIdentity,
    SegmentSummary, SegmentWriter, SeriesInput, SeriesInputV3, SeriesInputV4, SeriesValues,
    WrittenSegment,
};

pub use ravel_proto::segment::v1::Footer;
