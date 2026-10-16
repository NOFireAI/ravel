//! RSPAN: Ravel's columnar span segment format (ADR-0041). Trailer version 5
//! (ADR-2707 decision 2) adds the mandatory KEY_IDX section, an exact index
//! from `trace_id` to block ordinals ([`key_index`]), to v4's layout: per-key
//! string attribute columns (RLOG's design: a 1000-column budget with overflow
//! folded into an `attrs_raw` blob), span events in nested columns
//! (`event_ts`/`event_name`/`event_attrs_blob` with a per-row `event_count`),
//! the mandatory per-block BLOOM section over `service.name` and span-name
//! tokens, the lifted `service_name` column (id 9, ADR-0054), and the SKIP_IDX
//! duration/status bounds (ADR-0045). v5 retires v4 with no dual reader.
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
pub mod columnar;
pub mod error;
pub mod footer;
pub mod key_index;
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

pub use columnar::{BytesColumn, ColumnarBlockView, I64Column};
pub use error::SpanSegError;
pub use footer::{SpanFooter, SuffixOutcome, decode_section, open, open_from_suffix, read_section};
pub use key_index::{KeyIdxSummary, TRACE_ID_FIELD, TraceKeyProbe, encode_trace_key_idx};
pub use ranged::{BlockLoc, RspanRangeReader, TraceBlockSpan, TraceKeyBucket, TraceKeyStep};
pub use reader::{RspanReader, ScanStats, SpanQuery};
pub use record::{
    COL_NAME, COL_SERVICE_NAME, ResolvedSpanRow, SpanEvent, SpanRecord, StatusCode, merge_attrs,
};
pub use skip_index::{BloomPredicate, SkipIndex};
pub use writer::{ObjectIdentity, RspanConfig, RspanWriter};

/// Re-exported so a bloom-pruning caller (for example ravel-sql)
/// can parse the BLOOM section this crate hands it and call
/// [`SkipIndex::candidate_blocks_with_bloom`] without depending on
/// `ravel-codec` directly or constructing any RSPAN-internal type.
pub use ravel_codec::bloom_section::BloomSection;
