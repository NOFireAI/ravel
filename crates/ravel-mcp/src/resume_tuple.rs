//! The bytes of a keyset cursor's resume tuple.
//!
//! [`crate::cursor::CursorPosition::Keyset`] carries the last row of a page
//! as `tuple: Vec<u8>`, opaque to the cursor codec, which MACs it with the
//! rest of the token. This module is the encoding a tool writes there for a
//! `ravel_sql::ResumeValue` tuple, and the decoding the redeeming call reads
//! it back with before handing the tuple to `ravel_sql::plan_page`.
//!
//! The layout, all integers little-endian:
//!
//! ```text
//! tuple  := count:u32 value{count}
//! value  := 1 i64                       Int
//!         | 2 u64                       UInt
//!         | 3 u8 (0 or 1)               Bool
//!         | 4 u64                       Float, the f64's to_bits
//!         | 5 len:u32 utf8{len}         Str
//!         | 6 i64                       TimestampNanos
//!         | 7 len:u32 bytes{len}        Binary
//!         | 8 width:u32 len:u32 bytes{len}  FixedSizeBinary
//! ```
//!
//! A float is stored as its bit pattern, not as text, so `-0.0` and `0.0`
//! stay two values: the keyset predicate places a zero boundary by its sign.
//! A fixed-width value carries its width beside its length, and decoding
//! refuses the two disagreeing, so a width the column never had cannot reach
//! the `decode` cast `plan_page` renders.
//!
//! Decoding refuses, with a [`ResumeTupleError`] and never a panic: a tag
//! outside 1 to 8, a buffer that ends inside a value, bytes left after the
//! last value, a float whose bits are NaN or an infinity (`plan_page` would
//! refuse it anyway, and a cursor this crate minted never holds one), a
//! width that disagrees with its length, a bool byte other than 0 or 1, and
//! a string that is not UTF-8. Encoding refuses a non-finite float and a
//! length or count past `u32::MAX`.

use ravel_sql::ResumeValue;

const TAG_INT: u8 = 1;
const TAG_UINT: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_STR: u8 = 5;
const TAG_TIMESTAMP_NANOS: u8 = 6;
const TAG_BINARY: u8 = 7;
const TAG_FIXED_SIZE_BINARY: u8 = 8;

/// Why a resume tuple could not be encoded or decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResumeTupleError {
    /// A value's tag byte names no variant.
    #[error("resume tuple value has unknown tag {tag}")]
    UnknownTag { tag: u8 },
    /// The buffer ends inside a count, tag, length or payload.
    #[error("resume tuple is truncated")]
    Truncated,
    /// Bytes remain after the last value the count announced.
    #[error("resume tuple has {len} trailing bytes")]
    TrailingBytes { len: usize },
    /// A float is NaN or an infinity, which no keyset boundary can be.
    #[error("resume tuple float is not finite")]
    NonFiniteFloat,
    /// A fixed-width value's declared width is not its length.
    #[error("resume tuple fixed-width value declares width {width} but holds {len} bytes")]
    WidthMismatch { width: u32, len: u32 },
    /// A bool's byte is neither 0 nor 1.
    #[error("resume tuple bool byte {byte} is neither 0 nor 1")]
    InvalidBool { byte: u8 },
    /// A string's bytes are not UTF-8.
    #[error("resume tuple string is not UTF-8")]
    InvalidUtf8,
    /// A count or length does not fit the `u32` it is written as.
    #[error("resume tuple length {len} does not fit in 32 bits")]
    LengthTooLarge { len: usize },
}

/// Encodes `tuple` in this module's layout.
pub fn encode_resume_tuple(tuple: &[ResumeValue]) -> Result<Vec<u8>, ResumeTupleError> {
    let mut out = Vec::new();
    out.extend_from_slice(&len_u32(tuple.len())?.to_le_bytes());
    for value in tuple {
        match value {
            ResumeValue::Int(v) => {
                out.push(TAG_INT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ResumeValue::UInt(v) => {
                out.push(TAG_UINT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ResumeValue::Bool(v) => {
                out.push(TAG_BOOL);
                out.push(u8::from(*v));
            }
            ResumeValue::Float(v) => {
                if !v.is_finite() {
                    return Err(ResumeTupleError::NonFiniteFloat);
                }
                out.push(TAG_FLOAT);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            ResumeValue::Str(s) => {
                out.push(TAG_STR);
                write_len_prefixed(&mut out, s.as_bytes())?;
            }
            ResumeValue::TimestampNanos(ns) => {
                out.push(TAG_TIMESTAMP_NANOS);
                out.extend_from_slice(&ns.to_le_bytes());
            }
            ResumeValue::Binary(bytes) => {
                out.push(TAG_BINARY);
                write_len_prefixed(&mut out, bytes)?;
            }
            ResumeValue::FixedSizeBinary(bytes) => {
                out.push(TAG_FIXED_SIZE_BINARY);
                out.extend_from_slice(&len_u32(bytes.len())?.to_le_bytes());
                write_len_prefixed(&mut out, bytes)?;
            }
        }
    }
    Ok(out)
}

/// Decodes a tuple [`encode_resume_tuple`] wrote, refusing any other bytes.
pub fn decode_resume_tuple(bytes: &[u8]) -> Result<Vec<ResumeValue>, ResumeTupleError> {
    let mut reader = Reader { buf: bytes, pos: 0 };
    let count = reader.read_u32()? as usize;
    // Every value takes at least two bytes, so a count the buffer cannot hold
    // never sizes the allocation.
    let mut tuple = Vec::with_capacity(count.min(reader.remaining() / 2));
    for _ in 0..count {
        let tag = reader.read_u8()?;
        tuple.push(match tag {
            TAG_INT => ResumeValue::Int(i64::from_le_bytes(reader.read_array()?)),
            TAG_UINT => ResumeValue::UInt(u64::from_le_bytes(reader.read_array()?)),
            TAG_BOOL => match reader.read_u8()? {
                0 => ResumeValue::Bool(false),
                1 => ResumeValue::Bool(true),
                byte => return Err(ResumeTupleError::InvalidBool { byte }),
            },
            TAG_FLOAT => {
                let value = f64::from_bits(u64::from_le_bytes(reader.read_array()?));
                if !value.is_finite() {
                    return Err(ResumeTupleError::NonFiniteFloat);
                }
                ResumeValue::Float(value)
            }
            TAG_STR => {
                let bytes = reader.read_len_prefixed()?;
                let s = std::str::from_utf8(bytes).map_err(|_| ResumeTupleError::InvalidUtf8)?;
                ResumeValue::Str(s.to_owned())
            }
            TAG_TIMESTAMP_NANOS => {
                ResumeValue::TimestampNanos(i64::from_le_bytes(reader.read_array()?))
            }
            TAG_BINARY => ResumeValue::Binary(reader.read_len_prefixed()?.to_vec()),
            TAG_FIXED_SIZE_BINARY => {
                let width = reader.read_u32()?;
                let bytes = reader.read_len_prefixed()?;
                let len = len_u32(bytes.len())?;
                if width != len {
                    return Err(ResumeTupleError::WidthMismatch { width, len });
                }
                ResumeValue::FixedSizeBinary(bytes.to_vec())
            }
            tag => return Err(ResumeTupleError::UnknownTag { tag }),
        });
    }
    match reader.remaining() {
        0 => Ok(tuple),
        len => Err(ResumeTupleError::TrailingBytes { len }),
    }
}

fn len_u32(len: usize) -> Result<u32, ResumeTupleError> {
    u32::try_from(len).map_err(|_| ResumeTupleError::LengthTooLarge { len })
}

fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ResumeTupleError> {
    out.extend_from_slice(&len_u32(bytes.len())?.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ResumeTupleError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ResumeTupleError::Truncated)?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or(ResumeTupleError::Truncated)?;
        self.pos = end;
        Ok(bytes)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ResumeTupleError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.read_bytes(N)?);
        Ok(out)
    }

    fn read_u8(&mut self) -> Result<u8, ResumeTupleError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, ResumeTupleError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_len_prefixed(&mut self) -> Result<&'a [u8], ResumeTupleError> {
        let len = self.read_u32()? as usize;
        self.read_bytes(len)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Whether two tuples hold the same values, floats compared by bits.
    fn same(left: &[ResumeValue], right: &[ResumeValue]) -> bool {
        left.len() == right.len()
            && left.iter().zip(right).all(|pair| match pair {
                (ResumeValue::Float(a), ResumeValue::Float(b)) => a.to_bits() == b.to_bits(),
                (a, b) => a == b,
            })
    }

    fn finite_float() -> impl Strategy<Value = f64> {
        prop_oneof![
            Just(-0.0),
            Just(0.0),
            Just(f64::MIN_POSITIVE),
            Just(-f64::from_bits(1)),
            any::<u64>()
                .prop_map(f64::from_bits)
                .prop_filter("finite", |v| v.is_finite()),
        ]
    }

    fn resume_value() -> impl Strategy<Value = ResumeValue> {
        prop_oneof![
            any::<i64>().prop_map(ResumeValue::Int),
            any::<u64>().prop_map(ResumeValue::UInt),
            any::<bool>().prop_map(ResumeValue::Bool),
            finite_float().prop_map(ResumeValue::Float),
            any::<String>().prop_map(ResumeValue::Str),
            any::<i64>().prop_map(ResumeValue::TimestampNanos),
            proptest::collection::vec(any::<u8>(), 0..24).prop_map(ResumeValue::Binary),
            proptest::collection::vec(any::<u8>(), 0..24).prop_map(ResumeValue::FixedSizeBinary),
        ]
    }

    fn encoded(tuple: &[ResumeValue]) -> Vec<u8> {
        encode_resume_tuple(tuple).expect("a finite tuple encodes")
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Encoding then decoding returns the tuple bit for bit.
        ///
        /// FLIP: storing a float as `format!("{v:.17}")` text and parsing it
        /// back fails here on any value below 1e-17, which prints as zero.
        #[test]
        fn decoding_an_encoded_tuple_returns_it_bit_for_bit(
            tuple in proptest::collection::vec(resume_value(), 0..8),
        ) {
            let decoded = decode_resume_tuple(&encoded(&tuple)).expect("an encoded tuple decodes");
            prop_assert!(same(&decoded, &tuple), "{tuple:?} decoded as {decoded:?}");
        }

        /// Arbitrary bytes decode to a tuple or a typed error, never a panic.
        ///
        /// FLIP: indexing the buffer (`buf[pos..pos + len]`) instead of
        /// `get` panics here on the first short buffer.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let _ = decode_resume_tuple(&bytes);
        }

        /// Every strict prefix of an encoded tuple is refused as truncated.
        ///
        /// FLIP: reading a short count, tag or payload as zero-padded instead
        /// of refusing it decodes the empty prefix as an empty tuple.
        #[test]
        fn every_strict_prefix_is_truncated(
            tuple in proptest::collection::vec(resume_value(), 1..4),
        ) {
            let bytes = encoded(&tuple);
            for cut in 0..bytes.len() {
                prop_assert_eq!(
                    decode_resume_tuple(&bytes[..cut]),
                    Err(ResumeTupleError::Truncated),
                    "cut at {} of {}", cut, bytes.len()
                );
            }
        }
    }

    /// Each variant is written in the layout the module docs give.
    ///
    /// FLIP: storing a float as its shortest decimal text (`{v:?}`,
    /// length-prefixed after tag 4) fails here: `-0.0` writes the four bytes
    /// of `-0.0`, not its eight bits. That text round-trips every finite
    /// f64, so this test, not the round trip, is what catches it.
    #[test]
    fn each_variant_is_written_in_the_documented_layout() {
        let bytes = encoded(&[
            ResumeValue::Int(-2),
            ResumeValue::UInt(3),
            ResumeValue::Bool(true),
            ResumeValue::Float(-0.0),
            ResumeValue::Str("é".to_owned()),
            ResumeValue::TimestampNanos(5),
            ResumeValue::Binary(vec![0xab]),
            ResumeValue::FixedSizeBinary(vec![1, 2]),
        ]);
        let mut expected = vec![8, 0, 0, 0];
        expected.push(1);
        expected.extend_from_slice(&(-2i64).to_le_bytes());
        expected.push(2);
        expected.extend_from_slice(&3u64.to_le_bytes());
        expected.extend_from_slice(&[3, 1]);
        expected.push(4);
        expected.extend_from_slice(&0x8000_0000_0000_0000u64.to_le_bytes());
        expected.extend_from_slice(&[5, 2, 0, 0, 0, 0xc3, 0xa9]);
        expected.push(6);
        expected.extend_from_slice(&5i64.to_le_bytes());
        expected.extend_from_slice(&[7, 1, 0, 0, 0, 0xab]);
        expected.extend_from_slice(&[8, 2, 0, 0, 0, 2, 0, 0, 0, 1, 2]);
        assert_eq!(bytes, expected);
    }

    /// The two zeros encode to different bytes and decode to their own sign.
    ///
    /// FLIP: writing `(v + 0.0).to_bits()` folds `-0.0` into `0.0` and
    /// fails here.
    #[test]
    fn the_two_zeros_keep_their_signs() {
        let negative = encoded(&[ResumeValue::Float(-0.0)]);
        let positive = encoded(&[ResumeValue::Float(0.0)]);
        assert_ne!(negative, positive);
        let decoded = decode_resume_tuple(&negative).expect("decodes");
        assert!(matches!(decoded.as_slice(), [ResumeValue::Float(v)] if v.is_sign_negative()));
        let decoded = decode_resume_tuple(&positive).expect("decodes");
        assert!(matches!(decoded.as_slice(), [ResumeValue::Float(v)] if v.is_sign_positive()));
    }

    /// The empty tuple is four zero bytes and decodes back to empty.
    ///
    /// FLIP: omitting the count when the tuple is empty writes no bytes and
    /// fails the first assertion; the decoder then refuses it as truncated.
    #[test]
    fn the_empty_tuple_is_a_zero_count() {
        assert_eq!(encoded(&[]), vec![0, 0, 0, 0]);
        assert_eq!(decode_resume_tuple(&[0, 0, 0, 0]), Ok(vec![]));
    }

    /// A tag outside 1 to 8, zero included, is refused by value.
    ///
    /// FLIP: a catch-all arm that decodes any unknown tag as `Int` fails here.
    #[test]
    fn an_unknown_tag_is_refused() {
        for tag in [0u8, 9, 0xff] {
            let mut bytes = vec![1, 0, 0, 0, tag];
            bytes.extend_from_slice(&[0; 8]);
            assert_eq!(
                decode_resume_tuple(&bytes),
                Err(ResumeTupleError::UnknownTag { tag })
            );
        }
    }

    /// A count larger than the values present is refused as truncated.
    ///
    /// FLIP: stopping at the end of the buffer instead of at the count
    /// decodes this as a one-value tuple.
    #[test]
    fn a_count_past_the_values_is_truncated() {
        let mut bytes = encoded(&[ResumeValue::Int(1)]);
        bytes[0] = 2;
        assert_eq!(
            decode_resume_tuple(&bytes),
            Err(ResumeTupleError::Truncated)
        );
        // A count no buffer could hold is refused without sizing anything
        // by it.
        assert_eq!(
            decode_resume_tuple(&[0xff, 0xff, 0xff, 0xff]),
            Err(ResumeTupleError::Truncated)
        );
    }

    /// A length prefix past the end of the buffer is refused as truncated.
    ///
    /// FLIP: taking `min(len, remaining)` bytes decodes this as `"a"`.
    #[test]
    fn a_length_past_the_end_is_truncated() {
        let bytes = [1, 0, 0, 0, TAG_STR, 2, 0, 0, 0, b'a'];
        assert_eq!(
            decode_resume_tuple(&bytes),
            Err(ResumeTupleError::Truncated)
        );
    }

    /// Bytes after the last value are refused with their count.
    ///
    /// FLIP: returning the tuple without checking `remaining()` fails here.
    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = encoded(&[ResumeValue::Bool(false)]);
        bytes.extend_from_slice(&[0, 0, 0]);
        assert_eq!(
            decode_resume_tuple(&bytes),
            Err(ResumeTupleError::TrailingBytes { len: 3 })
        );
    }

    /// NaN, either NaN sign and payload, and both infinities are refused on
    /// both sides.
    ///
    /// FLIP: decoding `from_bits` without the finiteness check returns the
    /// NaN as a `Float` and fails here.
    #[test]
    fn a_non_finite_float_is_refused() {
        for bits in [
            f64::NAN.to_bits(),
            (-f64::NAN).to_bits(),
            0x7ff0_0000_0000_0001,
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
        ] {
            let mut bytes = vec![1, 0, 0, 0, TAG_FLOAT];
            bytes.extend_from_slice(&bits.to_le_bytes());
            assert_eq!(
                decode_resume_tuple(&bytes),
                Err(ResumeTupleError::NonFiniteFloat),
                "bits {bits:#x}"
            );
            assert_eq!(
                encode_resume_tuple(&[ResumeValue::Float(f64::from_bits(bits))]),
                Err(ResumeTupleError::NonFiniteFloat),
                "bits {bits:#x}"
            );
        }
    }

    /// A fixed-width value whose width is not its length is refused.
    ///
    /// FLIP: reading the width and discarding it decodes this as a two-byte
    /// value.
    #[test]
    fn a_width_mismatch_is_refused() {
        let bytes = [
            1,
            0,
            0,
            0,
            TAG_FIXED_SIZE_BINARY,
            16,
            0,
            0,
            0,
            2,
            0,
            0,
            0,
            1,
            2,
        ];
        assert_eq!(
            decode_resume_tuple(&bytes),
            Err(ResumeTupleError::WidthMismatch { width: 16, len: 2 })
        );
    }

    /// A bool byte other than 0 or 1 is refused by value.
    ///
    /// FLIP: decoding `byte != 0` reads this as `true`.
    #[test]
    fn a_bool_byte_other_than_zero_or_one_is_refused() {
        assert_eq!(
            decode_resume_tuple(&[1, 0, 0, 0, TAG_BOOL, 2]),
            Err(ResumeTupleError::InvalidBool { byte: 2 })
        );
    }

    /// A string that is not UTF-8 is refused.
    ///
    /// FLIP: `String::from_utf8_lossy` decodes this as U+FFFD.
    #[test]
    fn a_string_that_is_not_utf8_is_refused() {
        assert_eq!(
            decode_resume_tuple(&[1, 0, 0, 0, TAG_STR, 1, 0, 0, 0, 0xff]),
            Err(ResumeTupleError::InvalidUtf8)
        );
    }

    /// A length past `u32::MAX` is refused rather than truncated.
    ///
    /// FLIP: `len as u32` writes the low 32 bits and fails here.
    #[test]
    fn a_length_past_u32_is_refused() {
        let len = u32::MAX as usize + 1;
        assert_eq!(len_u32(len), Err(ResumeTupleError::LengthTooLarge { len }));
        assert_eq!(len_u32(u32::MAX as usize), Ok(u32::MAX));
    }
}
