//! RSEG v1 segment format: writer and reader for metric segments.
//!
//! Implementer contract: docs/segment-format.md and ADR-0004. All offsets,
//! lengths, and counts from stored bytes are untrusted; bounds-check
//! everything and verify crc32c on every section/page/footer touched.

mod crc;
mod error;
mod format;
mod gorilla;
mod histogram;
mod identity;
mod reader;
mod ts_delta;
mod varint;
mod writer;

pub use error::{SegmentError, WriteError};
pub use format::ReaderLimits;
pub use histogram::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};
pub use identity::{ExpectedIdentity, check_identity};
pub use reader::{
    FooterLocation, FooterOutcome, PlannedRange, SeriesEntry, ValPageKind, decode_catalog,
    decode_catalog_matching, decode_catalog_matching_v2, decode_catalog_v2, decode_pages,
    decode_pages_soa, open_from_full, open_from_suffix, parse_footer, plan_ranges, select,
    validate_sections,
};
pub use writer::{
    HistogramSample, IngestBounds, SegmentIdentity, SegmentSummary, SegmentWriter, SeriesInput,
    SeriesInputV3, SeriesValues, WrittenSegment,
};

pub use ravel_proto::segment::v1::Footer;
