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

pub mod bloom;
pub mod encoding;
pub mod error;
pub mod tokenizer;
pub mod varint;

pub use error::LogSegError;
