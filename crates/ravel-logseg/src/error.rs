//! Typed errors for RLOG v1 encode/decode (docs/log-segment-format.md).
//!
//! Every fallible decode path returns one of these instead of panicking.
//! Every offset, length, count, and tag read from stored bytes is untrusted:
//! bounds-checked, overflow-checked, and turned into a typed error, never a
//! panic and never wrong data.

/// Errors from writing or reading an RLOG segment.
///
/// `Corrupted` is the catch-all for every violation of the on-object
/// contract in docs/log-segment-format.md: a bad tag, an out-of-range
/// offset, an overflowing accumulation, a checksum mismatch, or trailing
/// bytes. `LimitExceeded` is a caller/config bound (too many columns, an
/// empty object). `InconsistentStreamAttrs` is writer-side input validation:
/// two records claiming one stream id but disagreeing on the resource+scope
/// bytes behind it. `Io` wraps compression backend failures.
#[derive(Debug, thiserror::Error)]
pub enum LogSegError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
    /// The trailer's format version is not the one this build supports
    /// (ADR-0066 decision 2: fail-closed-on-newer, everywhere, typed). This is
    /// distinct from `Corrupted`: the bytes are well-formed, they just carry a
    /// version this reader does not implement. Kept separate so a caller can
    /// tell a genuine corruption from a stray older/newer object. Mirrors
    /// `ravel_segment::SegmentError::UnsupportedVersion`.
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// Records handed to one writer share a `stream_id` but carry different
    /// `stream_attrs` bytes, so the object has no single truthful STREAM_DIR
    /// blob for that stream. Either a caller bug or a stream-id hash
    /// collision; the writer refuses the whole object instead of silently
    /// picking one blob. This is input validation, not object corruption:
    /// nothing has been decoded yet, so it is never `Corrupted`.
    #[error("inconsistent stream attrs: {0}")]
    InconsistentStreamAttrs(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// `ravel-codec`'s moved encoding/bloom/tokenizer code returns its own
/// `CodecError` (it must not depend on `ravel-logseg`, see the crate's
/// `lib.rs`). This conversion is the crate boundary: every existing `?`
/// call site in this crate that decodes a page or a bloom entry keeps
/// propagating the same `Corrupted` message unchanged.
impl From<ravel_codec::error::CodecError> for LogSegError {
    fn from(e: ravel_codec::error::CodecError) -> Self {
        match e {
            ravel_codec::error::CodecError::Corrupted(s) => LogSegError::Corrupted(s),
        }
    }
}
