//! TS_DELTA_VARINT (enc 1) core codec: first timestamp as a zigzag-varint
//! delta from 0, then zigzag-varint deltas between consecutive timestamps
//! (docs/segment-format.md). Shared by the writer (encode) and reader
//! (decode) so both sides can never disagree about the byte grammar.

use crate::error::SegmentError;
use crate::varint::{read_zigzag_varint, write_zigzag_varint};

/// Encodes `timestamps` as delta-varint bytes. Test-only convenience;
/// production encoding goes through [`encode_ts_deltas_into`] with a
/// reused buffer.
#[cfg(test)]
pub fn encode_ts_deltas(timestamps: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(timestamps.len() * 2);
    encode_ts_deltas_into(&mut out, timestamps)?;
    Some(out)
}

/// Appends the delta-varint encoding of `timestamps` to `out` without
/// allocating, so the writer can reuse one scratch buffer across series.
pub fn encode_ts_deltas_into(out: &mut Vec<u8>, timestamps: &[i64]) -> Option<()> {
    let mut prev = 0i64;
    for (i, &ts) in timestamps.iter().enumerate() {
        let delta = if i == 0 { ts } else { ts.checked_sub(prev)? };
        write_zigzag_varint(out, delta);
        prev = ts;
    }
    Some(())
}

/// Decodes exactly `count` timestamps with overflow-checked accumulation;
/// each decoded value is validated against `[min_ts_ns, max_ts_ns]`
/// (docs/segment-format.md). Rejects trailing bytes past the declared
/// count.
pub fn decode_ts_deltas(
    bytes: &[u8],
    count: usize,
    min_ts_ns: i64,
    max_ts_ns: i64,
) -> Result<Vec<i64>, SegmentError> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    let mut accum = 0i64;
    for i in 0..count {
        let delta = read_zigzag_varint(bytes, &mut pos)?;
        accum = if i == 0 {
            delta
        } else {
            accum
                .checked_add(delta)
                .ok_or(SegmentError::TimestampOverflow)?
        };
        if accum < min_ts_ns || accum > max_ts_ns {
            return Err(SegmentError::TimestampOutOfBounds {
                ts: accum,
                min: min_ts_ns,
                max: max_ts_ns,
            });
        }
        out.push(accum);
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn hand_vector_ascending_with_duplicate_and_gap() {
        // ts = [10, 10, 25, 100]; deltas = [10, 0, 15, 75]
        // zigzag(10)=20=0x14; zigzag(0)=0=0x00; zigzag(15)=30=0x1e;
        // zigzag(75)=150 -> varint [0x96, 0x01] (150 = 0b1001_0110:
        // low7=0x16 with continuation -> 0x96, remaining=1 -> 0x01).
        let ts = [10i64, 10, 25, 100];
        let encoded = encode_ts_deltas(&ts).expect("no overflow");
        assert_eq!(encoded, vec![0x14, 0x00, 0x1e, 0x96, 0x01]);
        let decoded = decode_ts_deltas(&encoded, 4, 10, 100).expect("decodes");
        assert_eq!(decoded, ts);
    }

    #[test]
    fn hand_vector_decode_tolerates_decreasing_deltas() {
        // The decode side is generic: it accepts negative deltas (only the
        // writer always pre-sorts ascending). ts=[5,3]; delta0=5 (zigzag
        // 10=0x0a); delta1=3-5=-2 (zigzag 3=0x03).
        let encoded = [0x0a, 0x03];
        let decoded = decode_ts_deltas(&encoded, 2, 3, 5).expect("decodes");
        assert_eq!(decoded, vec![5, 3]);
    }

    #[test]
    fn empty_sequence_roundtrips() {
        let encoded = encode_ts_deltas(&[]).expect("no overflow");
        assert!(encoded.is_empty());
        assert_eq!(
            decode_ts_deltas(&encoded, 0, 0, 0).expect("decodes"),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn encode_rejects_delta_overflow() {
        // i64::MAX - i64::MIN overflows i64 subtraction.
        assert_eq!(encode_ts_deltas(&[i64::MIN, i64::MAX]), None);
    }

    #[test]
    fn decode_rejects_accumulation_overflow() {
        let mut bytes = Vec::new();
        write_zigzag_varint(&mut bytes, i64::MAX);
        write_zigzag_varint(&mut bytes, i64::MAX);
        assert_eq!(
            decode_ts_deltas(&bytes, 2, i64::MIN, i64::MAX),
            Err(SegmentError::TimestampOverflow)
        );
    }

    #[test]
    fn decode_rejects_out_of_bounds_timestamp() {
        let ts = [1i64, 2, 3];
        let encoded = encode_ts_deltas(&ts).expect("no overflow");
        // Declare bounds that exclude 3.
        assert_eq!(
            decode_ts_deltas(&encoded, 3, 1, 2),
            Err(SegmentError::TimestampOutOfBounds {
                ts: 3,
                min: 1,
                max: 2
            })
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let ts = [1i64, 2, 3];
        let mut encoded = encode_ts_deltas(&ts).expect("no overflow");
        encoded.push(0x01); // stray extra byte
        assert_eq!(
            decode_ts_deltas(&encoded, 3, 1, 3),
            Err(SegmentError::TrailingBytes)
        );
    }
}
