//! Typed errors for the shared codec crate.
//!
//! `Corrupted` is the catch-all the moved encoding/bloom/tokenizer code
//! produces: an unknown tag, an out-of-range offset, an overflowing
//! accumulation, a checksum mismatch, or trailing bytes. `ravel-logseg` and
//! `ravel-rspan` convert this into their own `Corrupted` at the crate
//! boundary (`impl From<CodecError> for LogSegError` /
//! `impl From<CodecError> for SpanSegError`) so every existing `?`-based call
//! site keeps compiling and behaving unchanged.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
    /// A nested section's own grammar version (KEY_IDX, ADR-2707) is above
    /// every version this build knows (ADR-4233 decision 4, hazard H5).
    /// Distinct from `Corrupted`: the bytes are well-formed, they just carry
    /// a version this reader does not implement, so a caller can tell a
    /// genuine corruption from a stray newer object.
    #[error("unsupported {section} section version {version}")]
    UnsupportedSectionVersion { section: &'static str, version: u16 },
}
