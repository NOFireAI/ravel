//! LEB128 varints and zigzag encoding.
//!
//! This is a private, crate-internal copy of the same primitives defined
//! (and fully tested) in `ravel-logseg::varint`. `varint.rs` itself was not
//! named in issue #429's move list — only `encoding.rs`, `bloom.rs`,
//! `bloom_section.rs`, and `tokenizer.rs` were — but those modules call it,
//! and this crate must not depend on `ravel-logseg` (that would be
//! circular: `ravel-logseg` depends on `ravel-codec` for the re-export).
//! Not part of this crate's public API; see the final task report for the
//! duplication this creates and the follow-up it implies.

use crate::error::CodecError;

const MAX_VARINT_BYTES: usize = 10;

pub(crate) fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn get_uvarint(buf: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_VARINT_BYTES {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| CodecError::Corrupted("varint truncated".into()))?;
        *pos += 1;
        let low7 = u64::from(byte & 0x7f);
        if i == MAX_VARINT_BYTES - 1 {
            // The 10th byte only ever carries 1 meaningful bit (64 - 9*7 = 1).
            if low7 > 1 {
                return Err(CodecError::Corrupted("varint out of range".into()));
            }
        }
        let shifted = low7
            .checked_shl(shift)
            .ok_or_else(|| CodecError::Corrupted("varint out of range".into()))?;
        result |= shifted;
        if byte & 0x80 == 0 {
            // Canonical form: a multi-byte varint must not end in a redundant
            // zero group (e.g. [0x80, 0x00] is a non-canonical encoding of 0).
            if i > 0 && byte == 0 {
                return Err(CodecError::Corrupted("non-canonical varint".into()));
            }
            return Ok(result);
        }
        shift += 7;
    }
    Err(CodecError::Corrupted("varint too long".into()))
}

pub(crate) fn zigzag_encode(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

pub(crate) fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

pub(crate) fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, zigzag_encode(value));
}

pub(crate) fn get_ivarint(buf: &[u8], pos: &mut usize) -> Result<i64, CodecError> {
    Ok(zigzag_decode(get_uvarint(buf, pos)?))
}
