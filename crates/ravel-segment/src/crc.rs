//! Shared crc32c coverage helpers so the writer and reader can never
//! disagree about exactly which bytes a checksum covers
//! (docs/segment-format.md, ADR-0010 §4).

use crate::format::MAGIC;

/// Page crc32c: covers `series_id (16 bytes) || enc || comp || stored
/// payload`. The series_id is a seed prefix only; it is never itself stored
/// in the page bytes on disk.
pub fn page_crc(series_id: &[u8; 16], enc: u8, comp: u8, payload: &[u8]) -> u32 {
    let mut crc = crc32c::crc32c(series_id);
    crc = crc32c::crc32c_append(crc, &[enc, comp]);
    crc32c::crc32c_append(crc, payload)
}

/// footer_crc32c: covers the footer protobuf bytes, then footer_len (u32
/// LE), version (u16 LE), signal, reserved, magic -- i.e. every trailer byte
/// except the crc field itself (ADR-0010 §4).
pub fn footer_crc(
    footer_bytes: &[u8],
    footer_len: u32,
    version: u16,
    signal: u8,
    reserved: u8,
) -> u32 {
    let mut crc = crc32c::crc32c(footer_bytes);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &[signal, reserved]);
    crc32c::crc32c_append(crc, &MAGIC)
}
