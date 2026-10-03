//! Unsigned LEB128 varints and zigzag encoding for signed values
//! (docs/segment-format.md: "varint" means protobuf-style LEB128; signed
//! values use zigzag). Shared by the SERIES_TABLE grammar and the
//! TS_DELTA_VARINT page encoding.

use crate::error::SegmentError;

/// Maximum bytes a u64 LEB128 varint can occupy (ceil(64/7)).
const MAX_VARINT_BYTES: usize = 10;

/// Appends the unsigned LEB128 encoding of `value` to `out`.
pub fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
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

/// Reads an unsigned LEB128 varint starting at `*pos`, advancing `*pos` past
/// it. Bounds-checked and rejects overlong encodings; never panics.
pub fn read_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64, SegmentError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_VARINT_BYTES {
        let byte = *bytes.get(*pos).ok_or(SegmentError::Truncated)?;
        *pos += 1;
        let low7 = u64::from(byte & 0x7f);
        if i == MAX_VARINT_BYTES - 1 {
            // The 10th byte only ever contributes 1 meaningful bit (64 - 9*7 = 1).
            if low7 > 1 {
                return Err(SegmentError::BadVarint);
            }
        }
        let shifted = low7.checked_shl(shift).ok_or(SegmentError::BadVarint)?;
        result |= shifted;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(SegmentError::BadVarint)
}

/// Zigzag-encodes a signed value to an unsigned one (protobuf convention).
pub fn zigzag_encode(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

/// Decodes a zigzag-encoded unsigned value back to signed.
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Appends the zigzag-varint encoding of a signed value to `out`.
pub fn write_zigzag_varint(out: &mut Vec<u8>, value: i64) {
    write_uvarint(out, zigzag_encode(value));
}

/// Reads a zigzag-varint signed value starting at `*pos`.
pub fn read_zigzag_varint(bytes: &[u8], pos: &mut usize) -> Result<i64, SegmentError> {
    Ok(zigzag_decode(read_uvarint(bytes, pos)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn roundtrip_uvarint(value: u64) {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, value);
        let mut pos = 0;
        let decoded = read_uvarint(&buf, &mut pos).expect("decodes");
        assert_eq!(decoded, value);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn uvarint_hand_vectors() {
        // Values and their canonical LEB128 encodings (protobuf-style).
        assert_eq!(encode_vec_uvarint(0), vec![0x00]);
        assert_eq!(encode_vec_uvarint(1), vec![0x01]);
        assert_eq!(encode_vec_uvarint(127), vec![0x7f]);
        assert_eq!(encode_vec_uvarint(128), vec![0x80, 0x01]);
        assert_eq!(encode_vec_uvarint(300), vec![0xac, 0x02]);
        assert_eq!(encode_vec_uvarint(16384), vec![0x80, 0x80, 0x01]);
        assert_eq!(
            encode_vec_uvarint(u64::MAX),
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
        );
    }

    fn encode_vec_uvarint(v: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, v);
        buf
    }

    #[test]
    fn uvarint_roundtrips() {
        for v in [
            0u64,
            1,
            2,
            63,
            64,
            127,
            128,
            255,
            256,
            16383,
            16384,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX,
        ] {
            roundtrip_uvarint(v);
        }
    }

    #[test]
    fn uvarint_rejects_truncated_input() {
        let mut pos = 0;
        // continuation bit set, no following byte
        let err = read_uvarint(&[0x80], &mut pos);
        assert_eq!(err, Err(SegmentError::Truncated));
    }

    #[test]
    fn uvarint_rejects_overlong_encoding() {
        // 10 bytes, all continuation bits set except final byte has an
        // out-of-range high bit for a u64 (low7 > 1 in the 10th byte).
        let bytes = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        let mut pos = 0;
        assert_eq!(read_uvarint(&bytes, &mut pos), Err(SegmentError::BadVarint));
    }

    #[test]
    fn uvarint_rejects_11_byte_stream() {
        let bytes = [0xff; 11];
        let mut pos = 0;
        assert_eq!(read_uvarint(&bytes, &mut pos), Err(SegmentError::BadVarint));
    }

    #[test]
    fn zigzag_hand_vectors() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);
    }

    #[test]
    fn zigzag_roundtrips() {
        for v in [
            0i64,
            1,
            -1,
            2,
            -2,
            i64::MAX,
            i64::MIN,
            i64::MAX - 1,
            i64::MIN + 1,
            123_456_789,
            -123_456_789,
        ] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }
}

/// Corrupt-input mutation harness: the
/// fixed hand-vectors above pin a handful of chosen corruptions; these
/// proptest targets explore the arbitrary/mutated byte space. The LEB128
/// reader decodes untrusted stored bytes, so the contract is "typed error
/// or a valid decode, never a panic and never wrong data"
/// (docs/segment-format.md, CLAUDE.md testing patterns).
///
/// Runs on the pinned stable toolchain via proptest (no nightly/cargo-fuzz).
/// Case count honours the `PROPTEST_CASES` env var; the CI fuzz job raises
/// it for deeper exploration.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod fuzz_mutation {
    use super::*;
    use proptest::prelude::*;

    /// Single-bit and truncation mutations of a byte buffer, the seed-corpus
    /// mutation operators the finding calls for.
    fn mutate(bytes: &[u8], bit: usize, truncate_to: usize) -> Vec<u8> {
        let mut out = bytes.to_vec();
        if !out.is_empty() {
            let byte = (bit / 8) % out.len();
            out[byte] ^= 1u8 << (bit % 8);
        }
        let cut = if out.is_empty() {
            0
        } else {
            truncate_to % (out.len() + 1)
        };
        out.truncate(cut);
        out
    }

    proptest! {
        /// Reading a uvarint from arbitrary bytes at an arbitrary position
        /// must never panic; on success the position advances by 1..=10
        /// in-bounds bytes.
        #[test]
        fn read_uvarint_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
            start in 0usize..80,
        ) {
            let start = if bytes.is_empty() { 0 } else { start % (bytes.len() + 1) };
            let mut pos = start;
            match read_uvarint(&bytes, &mut pos) {
                Ok(_) => {
                    prop_assert!(pos > start && pos <= start + MAX_VARINT_BYTES);
                    prop_assert!(pos <= bytes.len());
                }
                Err(SegmentError::Truncated) | Err(SegmentError::BadVarint) => {}
                Err(other) => prop_assert!(false, "unexpected error variant: {other:?}"),
            }
        }

        /// Any u64 round-trips exactly and consumes exactly its encoding
        /// (no-wrong-data on the valid input space).
        #[test]
        fn uvarint_roundtrips_any_u64(value in any::<u64>()) {
            let mut buf = Vec::new();
            write_uvarint(&mut buf, value);
            let mut pos = 0;
            let decoded = read_uvarint(&buf, &mut pos).expect("valid encoding decodes");
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(pos, buf.len());
        }

        /// A valid encoding with a single bit flipped or truncated must
        /// still only ever yield a typed error or a (possibly different)
        /// valid value, never a panic.
        #[test]
        fn uvarint_seed_mutation_never_panics(
            value in any::<u64>(),
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
        ) {
            let mut buf = Vec::new();
            write_uvarint(&mut buf, value);
            let corrupt = mutate(&buf, bit, truncate_to);
            let mut pos = 0;
            let _ = read_uvarint(&corrupt, &mut pos);
        }

        /// Any i64 round-trips exactly through zigzag-varint.
        #[test]
        fn zigzag_varint_roundtrips_any_i64(value in any::<i64>()) {
            let mut buf = Vec::new();
            write_zigzag_varint(&mut buf, value);
            let mut pos = 0;
            let decoded = read_zigzag_varint(&buf, &mut pos).expect("valid encoding decodes");
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(pos, buf.len());
        }

        /// Zigzag decode over arbitrary bytes never panics: it is a total
        /// function of whatever `read_uvarint` returns.
        #[test]
        fn read_zigzag_varint_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let mut pos = 0;
            let _ = read_zigzag_varint(&bytes, &mut pos);
        }
    }
}
