//! RSEG v1 segment format: writer and reader for metric segments.
//!
//! Implementer contract: docs/segment-format.md and ADR-0004. All offsets,
//! lengths, and counts from stored bytes are untrusted; bounds-check
//! everything and verify crc32c on every section/page/footer touched.

mod crc;
mod error;
mod format;
mod gorilla;
mod identity;
mod reader;
mod ts_delta;
mod varint;
mod writer;

pub use error::{SegmentError, WriteError};
pub use format::ReaderLimits;
pub use identity::{ExpectedIdentity, check_identity};
pub use reader::{
    FooterLocation, FooterOutcome, PlannedRange, SeriesEntry, decode_catalog, decode_pages,
    open_from_full, open_from_suffix, parse_footer, plan_ranges, select, validate_sections,
};
pub use writer::{
    IngestBounds, SegmentIdentity, SegmentSummary, SegmentWriter, SeriesInput, WrittenSegment,
};

pub use ravel_proto::segment::v1::Footer;
