//! Typed errors for the shared codec crate.
//!
//! `Corrupted` is the only variant the moved encoding/bloom/tokenizer code
//! ever produces: an unknown tag, an out-of-range offset, an overflowing
//! accumulation, a checksum mismatch, or trailing bytes. `ravel-logseg`
//! converts this into its own `LogSegError::Corrupted` at the crate
//! boundary (`impl From<CodecError> for LogSegError`) so every existing
//! `?`-based call site keeps compiling and behaving unchanged.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
}
