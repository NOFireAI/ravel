//! RSPAN v1: Ravel's columnar span segment format (ADR-0041).
//!
//! A self-describing immutable object holding span records in columnar row
//! blocks, with an interval-aware skip index. RSPAN is a sibling of RLOG (crate
//! `ravel-logseg`) and RSEG (crate `ravel-segment`): it shares their
//! conventions (16-byte trailer, protobuf footer, crc32c checksum discipline,
//! suffix-GET reader protocol, untrusted-input parsing) but none of the bytes,
//! and two deliberate departures (ADR-0041):
//!
//! 1. Records sort by `(trace_id, start_ts)`, not by a derived stream identity:
//!    a span has no stream, and trace_id is the primary lookup key.
//! 2. The skip index tracks a time *interval* `(min_start_ts, max_end_ts)` per
//!    block, pruned by overlap, plus each block's `(min_trace_id, max_trace_id)`
//!    range.
//!
//! The on-object layout is the frozen contract in
//! `docs/span-segment-format.md`. Every decode path treats stored bytes as
//! untrusted and returns [`SpanSegError`] on any violation, never panicking.

pub mod block;
pub mod error;
pub mod footer;
pub mod ranged;
pub mod reader;
pub mod record;
pub mod skip_index;
pub mod varint;
pub mod writer;

/// Generated protobuf types for the RSPAN footer. The
/// `proto/ravel/rspan.proto` file is the source of truth; field numbers are
/// frozen (see the file header).
pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/ravel.rspan.v1.rs"));
}

pub use error::SpanSegError;
pub use footer::{SpanFooter, SuffixOutcome, open, open_from_suffix, read_section};
pub use ranged::{RspanRangeReader, TraceBlockSpan};
pub use reader::{RspanReader, ScanStats, SpanQuery};
pub use record::{SpanRecord, StatusCode, merge_attrs};
pub use writer::{ObjectIdentity, RspanConfig, RspanWriter};
