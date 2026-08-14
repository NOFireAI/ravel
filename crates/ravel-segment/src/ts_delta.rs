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

/// Decodes exactly `count` timestamps with overflow-checked accumulation,
/// *appending* them to a caller-supplied buffer (existing contents are
/// preserved; on error the appended tail is unspecified). Each decoded value
/// is validated against `[min_ts_ns, max_ts_ns]` (docs/segment-format.md).
/// Rejects trailing bytes past the declared count. Appending (rather than
/// clearing first) lets a caller concatenate several runs' pages into one
/// buffer, which the query fetcher's L0 branch relies on to emit one unit per
/// series with every run's samples in on-disk order; a caller that
/// wants a fresh decode clears the buffer itself.
pub fn decode_ts_deltas_into(
    bytes: &[u8],
    count: usize,
    min_ts_ns: i64,
    max_ts_ns: i64,
    out: &mut Vec<i64>,
) -> Result<(), SegmentError> {
    let mut pos = 0usize;
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
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn decode_ts_deltas(
        bytes: &[u8],
        count: usize,
        min_ts_ns: i64,
        max_ts_ns: i64,
    ) -> Result<Vec<i64>, SegmentError> {
        let mut out = Vec::new();
        decode_ts_deltas_into(bytes, count, min_ts_ns, max_ts_ns, &mut out)?;
        Ok(out)
    }

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

/// Corrupt-input mutation harness. The
/// TS_DELTA_VARINT decoder reads untrusted stored bytes; the contract is a
/// typed error or a valid decode, never a panic and never wrong data
/// (docs/segment-format.md, CLAUDE.md testing patterns). The hand-vectors
/// above pin chosen cases; these proptests explore the arbitrary/mutated
/// byte space on the pinned stable toolchain (proptest, no cargo-fuzz).
///
/// `count` is bounded to a realistic range here on purpose: it is a
/// separate decode input (the reader derives it from `sample_count`, itself
/// cross-checked against page bytes elsewhere), so these targets fuzz the
/// *byte grammar* rather than the pre-allocation behaviour of an absurd
/// count. Case count honours `PROPTEST_CASES`.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod fuzz_mutation {
    use super::*;
    use proptest::prelude::*;

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

    fn decode(bytes: &[u8], count: usize, min: i64, max: i64) -> Result<Vec<i64>, SegmentError> {
        let mut out = Vec::new();
        decode_ts_deltas_into(bytes, count, min, max, &mut out)?;
        Ok(out)
    }

    proptest! {
        /// Arbitrary bytes with an arbitrary (bounded) count and arbitrary
        /// bounds must yield a typed error or a valid decode, never a panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..1024,
            min in any::<i64>(),
            max in any::<i64>(),
        ) {
            let (min, max) = if min <= max { (min, max) } else { (max, min) };
            let _ = decode(&bytes, count, min, max);
        }

        /// Ascending timestamps within wide bounds round-trip exactly
        /// (no-wrong-data on the valid input space).
        #[test]
        fn roundtrips_ascending_timestamps(
            deltas in proptest::collection::vec(0i64..1_000_000, 0..64),
            base in -1_000_000_000i64..1_000_000_000,
        ) {
            // Build a strictly-derived ascending-ish series from a base and
            // non-negative deltas (stays well inside i64).
            let mut ts = Vec::with_capacity(deltas.len());
            let mut acc = base;
            for (i, d) in deltas.iter().enumerate() {
                acc = if i == 0 { base } else { acc + d };
                ts.push(acc);
            }
            let encoded = encode_ts_deltas(&ts).expect("no overflow for bounded deltas");
            let (min, max) = ts.iter().fold((i64::MAX, i64::MIN), |(lo, hi), &t| {
                (lo.min(t), hi.max(t))
            });
            let (min, max) = if ts.is_empty() { (0, 0) } else { (min, max) };
            let decoded = decode(&encoded, ts.len(), min, max).expect("valid stream decodes");
            prop_assert_eq!(decoded, ts);
        }

        /// A valid encoding with a single bit flipped or truncated must
        /// still only yield a typed error or a valid decode, never a panic.
        /// Deltas are bounded so the seed always encodes without overflow;
        /// the point is to fuzz the *decoder* over a mutated valid stream.
        #[test]
        fn seed_mutation_never_panics(
            deltas in proptest::collection::vec(-1_000_000i64..1_000_000, 0..48),
            base in -1_000_000_000i64..1_000_000_000,
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
            count in 0usize..1024,
        ) {
            let mut ts = Vec::with_capacity(deltas.len());
            let mut acc = base;
            for (i, d) in deltas.iter().enumerate() {
                acc = if i == 0 { base } else { acc + d };
                ts.push(acc);
            }
            let encoded = encode_ts_deltas(&ts).expect("bounded deltas never overflow");
            let corrupt = mutate(&encoded, bit, truncate_to);
            let _ = decode(&corrupt, count, i64::MIN, i64::MAX);
        }
    }
}
