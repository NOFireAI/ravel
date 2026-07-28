//! Per-page columnar codecs and the encoding tag registry
//! (docs/log-segment-format.md "Encodings"). The writer measures every
//! applicable codec and keeps the smallest; the tag makes the choice
//! self-describing. Every decoder is untrusted: it validates against the
//! caller-supplied element count, consumes exactly its bytes, and returns a
//! typed [`LogSegError`] on any violation, never panicking.

use crate::error::LogSegError;
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

/// Encoding tag registry (frozen contract, docs/log-segment-format.md).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Enc {
    Plain = 1,
    Constant = 2,
    Rle = 3,
    DeltaZigzag = 4,
    DoubleDelta = 5,
    ForBitpack = 6,
    Dict = 7,
    Bitmap = 8,
    FixedWidth = 9,
}

impl Enc {
    /// Maps a stored tag byte to an [`Enc`]; an unknown byte is `Corrupted`.
    pub fn from_u8(v: u8) -> Result<Enc, LogSegError> {
        Ok(match v {
            1 => Enc::Plain,
            2 => Enc::Constant,
            3 => Enc::Rle,
            4 => Enc::DeltaZigzag,
            5 => Enc::DoubleDelta,
            6 => Enc::ForBitpack,
            7 => Enc::Dict,
            8 => Enc::Bitmap,
            9 => Enc::FixedWidth,
            other => return Err(LogSegError::Corrupted(format!("unknown enc tag {other}"))),
        })
    }

    /// The stored tag byte.
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Defensive capacity hint: trust the caller's count for the common case but
/// never pre-allocate an unbounded amount from a single field.
fn cap(count: usize) -> usize {
    count.min(1 << 16)
}

/// Asserts a decoder consumed exactly the bytes it was given.
fn expect_consumed(pos: usize, len: usize) -> Result<(), LogSegError> {
    if pos == len {
        Ok(())
    } else {
        Err(LogSegError::Corrupted(format!(
            "codec left {} of {len} bytes unconsumed",
            len.saturating_sub(pos)
        )))
    }
}

// ---------------------------------------------------------------------------
// Bit packing (LSB-first), shared by ForBitpack and the dictionary id codecs.
// ---------------------------------------------------------------------------

/// Packs `values` into `out`, `bit_width` bits each, LSB-first. `bit_width`
/// must be in `0..=64`; a width of 0 writes nothing (every value is 0).
pub(crate) fn pack_bits(out: &mut Vec<u8>, values: &[u64], bit_width: u32) {
    if bit_width == 0 {
        return;
    }
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    for &v in values {
        acc |= u128::from(v & mask) << nbits;
        nbits += bit_width;
        while nbits >= 8 {
            out.push((acc & 0xff) as u8);
            acc >>= 8;
            nbits -= 8;
        }
    }
    if nbits > 0 {
        out.push((acc & 0xff) as u8);
    }
}

/// Unpacks `count` values of `bit_width` bits each from `buf`, LSB-first.
/// `buf` MUST be exactly the packed length; a `bit_width > 64`, a length
/// mismatch, or a count/width product that overflows is `Corrupted`.
pub(crate) fn unpack_bits(
    buf: &[u8],
    count: usize,
    bit_width: u32,
) -> Result<Vec<u64>, LogSegError> {
    if bit_width > 64 {
        return Err(LogSegError::Corrupted(format!(
            "bit_width {bit_width} > 64"
        )));
    }
    if bit_width == 0 {
        if !buf.is_empty() {
            return Err(LogSegError::Corrupted("bit_width 0 with payload".into()));
        }
        return Ok(vec![0u64; count]);
    }
    let need_bits = (count as u64)
        .checked_mul(u64::from(bit_width))
        .ok_or_else(|| LogSegError::Corrupted("packed length overflow".into()))?;
    let need = need_bits.div_ceil(8);
    if buf.len() as u64 != need {
        return Err(LogSegError::Corrupted(format!(
            "packed length {} != {need}",
            buf.len()
        )));
    }
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut out = Vec::with_capacity(cap(count));
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    let mut idx = 0usize;
    for _ in 0..count {
        while nbits < bit_width {
            acc |= u128::from(buf[idx]) << nbits;
            idx += 1;
            nbits += 8;
        }
        out.push((acc & u128::from(mask)) as u64);
        acc >>= bit_width;
        nbits -= bit_width;
    }
    Ok(out)
}

/// Minimum bit width needed to hold `range` as an unsigned value.
pub(crate) fn width_for(range: u64) -> u32 {
    if range == 0 {
        0
    } else {
        64 - range.leading_zeros()
    }
}

// ---------------------------------------------------------------------------
// Integer codecs
// ---------------------------------------------------------------------------

fn enc_plain_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        put_ivarint(&mut out, v);
    }
    out
}

fn enc_constant_i64(value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    put_ivarint(&mut out, value);
    out
}

fn enc_rle_i64(values: &[i64]) -> Vec<u8> {
    let mut runs: Vec<(i64, u64)> = Vec::new();
    for &v in values {
        match runs.last_mut() {
            Some((rv, rc)) if *rv == v => *rc += 1,
            _ => runs.push((v, 1)),
        }
    }
    let mut out = Vec::new();
    put_uvarint(&mut out, runs.len() as u64);
    for (v, c) in runs {
        put_ivarint(&mut out, v);
        put_uvarint(&mut out, c);
    }
    out
}

/// Delta-zigzag. `None` if any delta overflows i64 (so the codec is skipped).
fn enc_delta_i64(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    if let Some(&first) = values.first() {
        put_ivarint(&mut out, first);
        for w in values.windows(2) {
            let delta = w[1].checked_sub(w[0])?;
            put_ivarint(&mut out, delta);
        }
    }
    Some(out)
}

/// Double-delta. `None` if any intermediate overflows i64.
fn enc_double_delta_i64(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    if values.is_empty() {
        return Some(out);
    }
    put_ivarint(&mut out, values[0]);
    if values.len() >= 2 {
        let mut prev_delta = values[1].checked_sub(values[0])?;
        put_ivarint(&mut out, prev_delta);
        for i in 2..values.len() {
            let delta = values[i].checked_sub(values[i - 1])?;
            let dod = delta.checked_sub(prev_delta)?;
            put_ivarint(&mut out, dod);
            prev_delta = delta;
        }
    }
    Some(out)
}

fn enc_for_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    let min = values.iter().copied().min().unwrap_or(0);
    let max = values.iter().copied().max().unwrap_or(0);
    let range = (max as u64).wrapping_sub(min as u64);
    let bit_width = width_for(range);
    put_ivarint(&mut out, min);
    out.push(bit_width as u8);
    let offsets: Vec<u64> = values
        .iter()
        .map(|&v| (v as u64).wrapping_sub(min as u64))
        .collect();
    pack_bits(&mut out, &offsets, bit_width);
    out
}

/// Encodes `values` with every applicable integer codec and returns the
/// smallest, biased so `Constant` then `Rle` win ties.
pub fn encode_i64(values: &[i64]) -> (Enc, Vec<u8>) {
    if values.is_empty() {
        return (Enc::Plain, Vec::new());
    }
    // Candidates in tie-break priority order; keep the first strictly-smaller.
    let mut candidates: Vec<(Enc, Vec<u8>)> = Vec::new();
    if values.iter().all(|&v| v == values[0]) {
        candidates.push((Enc::Constant, enc_constant_i64(values[0])));
    }
    candidates.push((Enc::Rle, enc_rle_i64(values)));
    candidates.push((Enc::Plain, enc_plain_i64(values)));
    if let Some(b) = enc_delta_i64(values) {
        candidates.push((Enc::DeltaZigzag, b));
    }
    if let Some(b) = enc_double_delta_i64(values) {
        candidates.push((Enc::DoubleDelta, b));
    }
    candidates.push((Enc::ForBitpack, enc_for_i64(values)));

    let mut best: Option<(Enc, Vec<u8>)> = None;
    for (enc, bytes) in candidates {
        match &best {
            Some((_, b)) if bytes.len() >= b.len() => {}
            _ => best = Some((enc, bytes)),
        }
    }
    best.unwrap_or((Enc::Plain, Vec::new()))
}

/// Decodes `count` i64 values encoded with `enc`.
pub fn decode_i64(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<i64>, LogSegError> {
    let mut pos = 0usize;
    let out = match enc {
        Enc::Plain => {
            let mut out = Vec::with_capacity(cap(count));
            for _ in 0..count {
                out.push(get_ivarint(bytes, &mut pos)?);
            }
            out
        }
        Enc::Constant => {
            let v = get_ivarint(bytes, &mut pos)?;
            vec![v; count]
        }
        Enc::Rle => {
            let run_count = get_uvarint(bytes, &mut pos)?;
            let mut out = Vec::with_capacity(cap(count));
            let mut total: u64 = 0;
            for _ in 0..run_count {
                let v = get_ivarint(bytes, &mut pos)?;
                let run = get_uvarint(bytes, &mut pos)?;
                total = total
                    .checked_add(run)
                    .ok_or_else(|| LogSegError::Corrupted("rle run overflow".into()))?;
                if total > count as u64 {
                    return Err(LogSegError::Corrupted("rle exceeds count".into()));
                }
                for _ in 0..run {
                    out.push(v);
                }
            }
            if total != count as u64 {
                return Err(LogSegError::Corrupted("rle short of count".into()));
            }
            out
        }
        Enc::DeltaZigzag => {
            let mut out = Vec::with_capacity(cap(count));
            if count > 0 {
                let mut acc = get_ivarint(bytes, &mut pos)?;
                out.push(acc);
                for _ in 1..count {
                    let delta = get_ivarint(bytes, &mut pos)?;
                    acc = acc
                        .checked_add(delta)
                        .ok_or_else(|| LogSegError::Corrupted("delta overflow".into()))?;
                    out.push(acc);
                }
            }
            out
        }
        Enc::DoubleDelta => {
            let mut out = Vec::with_capacity(cap(count));
            if count > 0 {
                let mut value = get_ivarint(bytes, &mut pos)?;
                out.push(value);
                if count > 1 {
                    let mut prev_delta = get_ivarint(bytes, &mut pos)?;
                    value = value
                        .checked_add(prev_delta)
                        .ok_or_else(|| LogSegError::Corrupted("double-delta overflow".into()))?;
                    out.push(value);
                    for _ in 2..count {
                        let dod = get_ivarint(bytes, &mut pos)?;
                        prev_delta = prev_delta.checked_add(dod).ok_or_else(|| {
                            LogSegError::Corrupted("double-delta overflow".into())
                        })?;
                        value = value.checked_add(prev_delta).ok_or_else(|| {
                            LogSegError::Corrupted("double-delta overflow".into())
                        })?;
                        out.push(value);
                    }
                }
            }
            out
        }
        Enc::ForBitpack => {
            let min = get_ivarint(bytes, &mut pos)?;
            let bit_width = u32::from(
                *bytes
                    .get(pos)
                    .ok_or_else(|| LogSegError::Corrupted("for truncated".into()))?,
            );
            pos += 1;
            let offsets = unpack_bits(&bytes[pos..], count, bit_width)?;
            pos = bytes.len();
            offsets
                .into_iter()
                .map(|o| (min as u64).wrapping_add(o) as i64)
                .collect()
        }
        other => {
            return Err(LogSegError::Corrupted(format!(
                "enc {other:?} is not an integer codec"
            )));
        }
    };
    expect_consumed(pos, bytes.len())?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Bitmap codec (bool columns and presence bitmaps)
// ---------------------------------------------------------------------------

/// Encodes `bits` LSB-first, zero-padded to a whole number of bytes.
pub fn encode_bitmap(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1u8 << ((i % 8) as u32);
        }
    }
    out
}

/// Decodes `count` bools from a bitmap page. The payload length must be
/// exactly `ceil(count / 8)`.
pub fn decode_bitmap(bytes: &[u8], count: usize) -> Result<Vec<bool>, LogSegError> {
    if bytes.len() != count.div_ceil(8) {
        return Err(LogSegError::Corrupted(format!(
            "bitmap length {} != {}",
            bytes.len(),
            count.div_ceil(8)
        )));
    }
    let mut out = Vec::with_capacity(cap(count));
    for i in 0..count {
        out.push(bytes[i / 8] & (1u8 << ((i % 8) as u32)) != 0);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn enc_tag_roundtrip() {
        for tag in 1u8..=9 {
            assert_eq!(Enc::from_u8(tag).expect("known").to_u8(), tag);
        }
        assert!(matches!(Enc::from_u8(0), Err(LogSegError::Corrupted(_))));
        assert!(matches!(Enc::from_u8(10), Err(LogSegError::Corrupted(_))));
    }

    #[test]
    fn rle_exact_bytes() {
        // [5,5,5,9] -> run_count 2, (5,3),(9,1).
        // 2=0x02; ivarint(5)=0x0a; 3=0x03; ivarint(9)=0x12; 1=0x01.
        let bytes = enc_rle_i64(&[5, 5, 5, 9]);
        assert_eq!(bytes, vec![0x02, 0x0a, 0x03, 0x12, 0x01]);
        assert_eq!(
            decode_i64(Enc::Rle, &bytes, 4).expect("decode"),
            vec![5, 5, 5, 9]
        );
    }

    #[test]
    fn constant_exact_bytes() {
        let bytes = enc_constant_i64(7);
        assert_eq!(bytes, vec![0x0e]); // ivarint(7) = zigzag 14 = 0x0e
        assert_eq!(
            decode_i64(Enc::Constant, &bytes, 3).expect("decode"),
            vec![7, 7, 7]
        );
    }

    #[test]
    fn plain_exact_bytes() {
        let bytes = enc_plain_i64(&[0, -1, 1]);
        // ivarint: 0->0x00, -1->0x01, 1->0x02
        assert_eq!(bytes, vec![0x00, 0x01, 0x02]);
        assert_eq!(
            decode_i64(Enc::Plain, &bytes, 3).expect("decode"),
            vec![0, -1, 1]
        );
    }

    #[test]
    fn delta_roundtrip() {
        let vals = [100, 101, 103, 106];
        let bytes = enc_delta_i64(&vals).expect("no overflow");
        assert_eq!(
            decode_i64(Enc::DeltaZigzag, &bytes, 4).expect("decode"),
            vals
        );
    }

    #[test]
    fn double_delta_roundtrip() {
        let vals = [10, 20, 30, 40, 51];
        let bytes = enc_double_delta_i64(&vals).expect("no overflow");
        assert_eq!(
            decode_i64(Enc::DoubleDelta, &bytes, 5).expect("decode"),
            vals
        );
    }

    #[test]
    fn for_bitpack_roundtrip_and_zero_width() {
        let vals = [1000, 1001, 1007, 1002];
        let bytes = enc_for_i64(&vals);
        assert_eq!(
            decode_i64(Enc::ForBitpack, &bytes, 4).expect("decode"),
            vals
        );
        // All-equal -> bit_width 0, no packed bytes.
        let flat = enc_for_i64(&[42, 42, 42]);
        assert_eq!(flat, vec![0x54, 0x00]); // ivarint(42)=84=0x54, width 0
        assert_eq!(
            decode_i64(Enc::ForBitpack, &flat, 3).expect("decode"),
            vec![42, 42, 42]
        );
    }

    #[test]
    fn picker_prefers_constant_then_rle() {
        let (enc, _) = encode_i64(&[9, 9, 9, 9]);
        assert_eq!(enc, Enc::Constant);
        // Long run, few distinct: RLE beats plain.
        let mut v = vec![7i64; 50];
        v.extend([8, 8, 8]);
        let (enc, bytes) = encode_i64(&v);
        assert_eq!(enc, Enc::Rle);
        assert_eq!(decode_i64(enc, &bytes, v.len()).expect("decode"), v);
    }

    #[test]
    fn empty_encodes_plain_empty() {
        let (enc, bytes) = encode_i64(&[]);
        assert_eq!(enc, Enc::Plain);
        assert!(bytes.is_empty());
        assert_eq!(
            decode_i64(Enc::Plain, &[], 0).expect("decode"),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn bitmap_exact_bytes() {
        let bits = [true, false, true, false, false, false, false, false, true];
        let bytes = encode_bitmap(&bits);
        // byte0: bits 0 and 2 -> 0b0000_0101 = 0x05; byte1: bit 8 -> 0x01
        assert_eq!(bytes, vec![0x05, 0x01]);
        assert_eq!(decode_bitmap(&bytes, 9).expect("decode"), bits);
    }

    #[test]
    fn decode_rejects_trailing_and_short() {
        // Plain with an extra trailing byte.
        assert!(matches!(
            decode_i64(Enc::Plain, &[0x00, 0x00], 1),
            Err(LogSegError::Corrupted(_))
        ));
        // Rle claiming more than count.
        let bytes = enc_rle_i64(&[5, 5, 5, 9]);
        assert!(matches!(
            decode_i64(Enc::Rle, &bytes, 2),
            Err(LogSegError::Corrupted(_))
        ));
        // FOR with bit_width > 64.
        let bad = vec![0x00, 65];
        assert!(matches!(
            decode_i64(Enc::ForBitpack, &bad, 1),
            Err(LogSegError::Corrupted(_))
        ));
    }
}

/// Round-trip over every picker path, and decode-never-panics over the whole
/// integer/bitmap decode surface.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn i64_roundtrip_all_pickers(vals in proptest::collection::vec(any::<i64>(), 0..2000)) {
            let (enc, bytes) = encode_i64(&vals);
            prop_assert_eq!(decode_i64(enc, &bytes, vals.len()).expect("decode"), vals);
        }

        #[test]
        fn i64_decode_never_panics(
            tag in 1u8..=9,
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..1000,
        ) {
            if let Ok(e) = Enc::from_u8(tag) {
                let _ = decode_i64(e, &bytes, count);
            }
        }

        #[test]
        fn bitmap_roundtrip(bits in proptest::collection::vec(any::<bool>(), 0..2000)) {
            let bytes = encode_bitmap(&bits);
            prop_assert_eq!(decode_bitmap(&bytes, bits.len()).expect("decode"), bits);
        }

        #[test]
        fn bitmap_decode_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..2000,
        ) {
            let _ = decode_bitmap(&bytes, count);
        }
    }
}
