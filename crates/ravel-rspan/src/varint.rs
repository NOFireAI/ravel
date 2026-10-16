//! LEB128 varints and zigzag encoding for RSPAN v1
//! (docs/span-segment-format.md: "varint" is protobuf-style LEB128, "ivarint"
//! is zigzag then LEB128). Every decode path is untrusted: bounds-checked,
//! rejecting overlong and non-canonical encodings, never panicking. This is the
//! same proven grammar RLOG's `varint` module uses; RSPAN carries its own copy
//! so the span format does not take a crate dependency on the log format.

use crate::error::SpanSegError;

/// Maximum bytes a u64 LEB128 varint can occupy (ceil(64/7)).
const MAX_VARINT_BYTES: usize = 10;

/// Appends the unsigned LEB128 encoding of `value` to `out`.
pub fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
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
/// it. Rejects a truncated buffer, an encoding longer than 10 bytes, an
/// out-of-range 10th byte, and a non-canonical (overlong) encoding whose
/// terminating byte is a redundant zero group. Never panics.
pub fn get_uvarint(buf: &[u8], pos: &mut usize) -> Result<u64, SpanSegError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_VARINT_BYTES {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| SpanSegError::Corrupted("varint truncated".into()))?;
        *pos += 1;
        let low7 = u64::from(byte & 0x7f);
        if i == MAX_VARINT_BYTES - 1 {
            // The 10th byte only ever carries 1 meaningful bit (64 - 9*7 = 1).
            if low7 > 1 {
                return Err(SpanSegError::Corrupted("varint out of range".into()));
            }
        }
        let shifted = low7
            .checked_shl(shift)
            .ok_or_else(|| SpanSegError::Corrupted("varint out of range".into()))?;
        result |= shifted;
        if byte & 0x80 == 0 {
            // Canonical form: a multi-byte varint must not end in a redundant
            // zero group (e.g. [0x80, 0x00] is a non-canonical encoding of 0).
            if i > 0 && byte == 0 {
                return Err(SpanSegError::Corrupted("non-canonical varint".into()));
            }
            return Ok(result);
        }
        shift += 7;
    }
    Err(SpanSegError::Corrupted("varint too long".into()))
}

/// Zigzag-encodes a signed value (protobuf convention).
pub fn zigzag_encode(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

/// Decodes a zigzag-encoded unsigned value back to signed.
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Appends the zigzag-varint (ivarint) encoding of a signed value to `out`.
pub fn put_ivarint(out: &mut Vec<u8>, value: i64) {
    put_uvarint(out, zigzag_encode(value));
}

/// Reads a zigzag-varint (ivarint) signed value starting at `*pos`.
pub fn get_ivarint(buf: &[u8], pos: &mut usize) -> Result<i64, SpanSegError> {
    Ok(zigzag_decode(get_uvarint(buf, pos)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn is_corrupted<T>(r: Result<T, SpanSegError>) -> bool {
        matches!(r, Err(SpanSegError::Corrupted(_)))
    }

    #[test]
    fn uvarint_hand_vectors() {
        let enc = |v| {
            let mut b = Vec::new();
            put_uvarint(&mut b, v);
            b
        };
        assert_eq!(enc(0), vec![0x00]);
        assert_eq!(enc(1), vec![0x01]);
        assert_eq!(enc(127), vec![0x7f]);
        assert_eq!(enc(128), vec![0x80, 0x01]);
        assert_eq!(enc(300), vec![0xac, 0x02]);
        assert_eq!(
            enc(u64::MAX),
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
        );
    }

    #[test]
    fn uvarint_rejects_truncated_overlong_and_noncanonical() {
        let mut pos = 0;
        assert!(is_corrupted(get_uvarint(&[0x80], &mut pos)));
        let mut pos = 0;
        assert!(is_corrupted(get_uvarint(&[0xff; 11], &mut pos)));
        let bytes = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        let mut pos = 0;
        assert!(is_corrupted(get_uvarint(&bytes, &mut pos)));
        // [0x80, 0x00] is a two-byte encoding of 0; canonical is [0x00].
        let mut pos = 0;
        assert!(is_corrupted(get_uvarint(&[0x80, 0x00], &mut pos)));
    }

    #[test]
    fn ivarint_hand_vectors() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
    }
}

/// Property tests over the untrusted decode surface: round-trips are exact and
/// consume exactly their bytes; arbitrary input never panics and yields only a
/// typed error or a valid decode.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn uvarint_roundtrips_any_u64(value in any::<u64>()) {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, value);
            let mut pos = 0;
            prop_assert_eq!(get_uvarint(&buf, &mut pos).expect("decode"), value);
            prop_assert_eq!(pos, buf.len());
        }

        #[test]
        fn ivarint_roundtrips_any_i64(value in any::<i64>()) {
            let mut buf = Vec::new();
            put_ivarint(&mut buf, value);
            let mut pos = 0;
            prop_assert_eq!(get_ivarint(&buf, &mut pos).expect("decode"), value);
            prop_assert_eq!(pos, buf.len());
        }

        #[test]
        fn uvarint_arbitrary_bytes_never_panic(
            bytes in proptest::collection::vec(any::<u8>(), 0..32),
        ) {
            let mut pos = 0;
            match get_uvarint(&bytes, &mut pos) {
                Ok(_) => prop_assert!(pos <= bytes.len()),
                Err(SpanSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }
    }
}
