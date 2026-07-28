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
/// empty object). `Io` wraps compression backend failures.
#[derive(Debug, thiserror::Error)]
pub enum LogSegError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
