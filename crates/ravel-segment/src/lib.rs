//! RSEG v1 segment format: writer and reader for metric segments.
//!
//! Implementer contract: docs/segment-format.md and ADR-0004. All offsets,
//! lengths, and counts from stored bytes are untrusted; bounds-check
//! everything and verify crc32c on every section/page/footer touched.

mod crc;
mod error;
mod experiment;
mod format;
mod gorilla;
mod histogram;
mod identity;
mod reader;
mod ts_delta;
mod varint;
mod writer;

/// Within-segment selective-read experiment (GitHub issue #167). PROTOTYPE,
/// flag-gated, never on the default writer/reader path; see the module docs.
pub mod experiment_api {
    pub use crate::experiment::{
        ChunkLocation, DEFAULT_STRIDE, ExperimentError, ExperimentFlags, IdsWindow, PageLoc,
        SECTION_KIND_SERIES_IDX, SECTION_KIND_SERIES_META_CHUNKS, SparseIdIndex,
        decode_chunk_page_location, decode_page_locations_v2, decompress_chunk_frame,
        find_experimental_section, find_index_in_window, parse_footer_from_suffix_unvalidated,
        parse_footer_unvalidated, parse_series_idx, write_v2_experimental,
    };
}

pub use error::{SegmentError, WriteError};
pub use format::ReaderLimits;
pub use histogram::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
pub use identity::{ExpectedIdentity, check_identity};
pub use reader::{
    FooterLocation, FooterOutcome, PlannedRange, PlannedRunRange, RunEntry, SeriesEntry,
    SeriesEntryV4, ValPageKind, ValueKind, decode_catalog, decode_catalog_matching,
    decode_catalog_matching_v2, decode_catalog_matching_v3, decode_catalog_matching_v4,
    decode_catalog_v2, decode_catalog_v3, decode_catalog_v4, decode_histogram_pages, decode_pages,
    decode_pages_soa, decode_run_histogram_pages, decode_run_pages_soa, open_from_full,
    open_from_suffix, parse_footer, plan_ranges, plan_ranges_v3, plan_ranges_v4, select,
    validate_sections,
};
pub use writer::{
    CompactionMetaV4, HistogramSample, IngestBounds, RunInputV4, RunValuePageV4, SegmentIdentity,
    SegmentSummary, SegmentWriter, SeriesInput, SeriesInputV3, SeriesInputV4, SeriesValues,
    WrittenSegment,
};

pub use ravel_proto::segment::v1::Footer;
