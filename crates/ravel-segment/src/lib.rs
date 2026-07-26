//! RSEG v1 segment format: writer and reader for metric segments.
//!
//! Implementer contract: docs/segment-format.md and ADR-0004. All offsets,
//! lengths, and counts from stored bytes are untrusted; bounds-check
//! everything and verify crc32c on every section/page/footer touched.
