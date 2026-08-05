//! Typed errors for RSPAN v1 encode/decode (docs/span-segment-format.md).
//!
//! Every fallible decode path returns one of these instead of panicking. Every
//! offset, length, count, and tag read from stored bytes is untrusted:
//! bounds-checked, overflow-checked, and turned into a typed error, never a
//! panic and never wrong data. This mirrors `ravel_logseg::LogSegError`, the
//! sibling RLOG format's error type; RSPAN is a separate format so it carries
//! its own error rather than importing that one.

/// Errors from writing or reading an RSPAN span segment.
///
/// `Corrupted` is the catch-all for every violation of the on-object contract
/// in docs/span-segment-format.md: a bad tag, an out-of-range offset, an
/// overflowing accumulation, a checksum mismatch, or trailing bytes.
/// `LimitExceeded` is a caller/config bound (an empty object, a decode past a
/// configured cap). `Io` wraps compression backend failures.
#[derive(Debug, thiserror::Error)]
pub enum SpanSegError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// The shared `ravel-codec` crate (the blocked bloom filter and its section
/// framing, ADR-0054) reports every violation as `CodecError::Corrupted`.
/// Convert it into this crate's `Corrupted` at the boundary so every
/// `?`-based call site in the BLOOM read path returns a typed `SpanSegError`,
/// exactly as `ravel-logseg` does for its own reuse of the same code.
impl From<ravel_codec::error::CodecError> for SpanSegError {
    fn from(e: ravel_codec::error::CodecError) -> Self {
        match e {
            ravel_codec::error::CodecError::Corrupted(s) => SpanSegError::Corrupted(s),
        }
    }
}
