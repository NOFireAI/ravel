//! RLOG v1: Ravel's columnar log segment format.
//!
//! A self-describing immutable object holding log records in columnar row
//! blocks, with a multi-level skip index and per-block token blooms for
//! proof-based pruning. RLOG is a sibling of RSEG (crate `ravel-segment`):
//! it shares the conventions (16-byte trailer, protobuf footer, crc32c
//! checksum discipline, suffix-GET reader protocol, untrusted-input parsing)
//! but none of the bytes.
//!
//! The on-object layout is the frozen contract in
//! `docs/log-segment-format.md` (ADR-0029). Every decode path treats stored
//! bytes as untrusted and returns [`LogSegError`] on any violation, never
//! panicking.

pub mod block;
pub mod error;
pub mod field_dir;
pub mod footer;
pub mod page;
pub mod postings;
pub mod ranged;
pub mod reader;
pub mod record;
pub mod skip_index;
pub mod stream_dir;
pub mod varint;
pub mod writer;

// encoding, bloom, bloom_section, and tokenizer moved to `ravel-codec`
// (issue #429, docs/adrs/0045-rspan-v2-trace-investigation.md decision 1).
// Re-exported here so no other crate's imports or Cargo.toml need to change.
pub use ravel_codec::{bloom, bloom_section, encoding, tokenizer};

pub use error::LogSegError;
pub use footer::{SuffixOutcome, open_from_suffix};
pub use ranged::{RlogRangeReader, StreamBlockLoc, StreamBlockSpan};
pub use reader::{RlogReader, ScanStats, decode_section, read_section};
pub use record::{FieldSel, LogRecord, Predicate, stream_attrs_bytes};
pub use writer::{ObjectIdentity, RlogConfig, RlogWriter};

// Re-exported for callers building records; these are the ravel-types identity
// primitives that appear in the public [`LogRecord`] and [`Predicate`] surface.
pub use ravel_types::logstream::{AttrValue, LogStreamId};
