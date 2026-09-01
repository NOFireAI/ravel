//! Shared segment codecs, extracted out of `ravel-logseg`
//! (docs/adrs/0045-rspan-v2-trace-investigation.md decision 1): the encoding
//! tag registry, the blocked token bloom filter and its section framing, and
//! the normative word tokenizer. `ravel-logseg` re-exports all four so its
//! own callers see no change. `ravel-rspan` will depend on this crate
//! directly rather than growing a second copy of the tokenizer, which is
//! normative because it defines word semantics identically on the write and
//! read paths (docs/log-segment-format.md "Tokenizer"). That dependency
//! arrives with RSPAN v2; this crate does not have it yet.
//!
//! This crate deliberately has no `ravel-logseg`, `ravel-proto`, or
//! `ravel-types` dependency: it is a sibling to every segment format crate,
//! not a downstream of any one of them.

mod varint;

pub mod bloom;
pub mod bloom_section;
pub mod encoding;
pub mod error;
pub mod tokenizer;
