//! TS_GCD_I64 (enc 2) core codec: divide every timestamp in the page by the
//! page's greatest common divisor, then `ravel_codec::encode_i64` the reduced
//! integers with a self-describing tag byte (ADR-0092 decision 6, measured in
//! issue #312). Shared by the writer (encode) and reader (decode).
//!
//! This is a *second* timestamp encoding, not a replacement for
//! [`crate::ts_delta`]: the writer builds both candidate pages and keeps the
//! smaller (`crate::writer::frame_ts_page`). The GCD stack wins on jittered and
//! irregular millisecond streams, where dividing out the millisecond factor
//! before delta coding sheds a constant per sample; it loses on the
//! exactly-regular stream, where the incumbent varint page collapses under LZ4
//! to about 0.16 bytes per sample and a bare `encode_i64` would regress it, and
//! it gains nothing on true-nanosecond OTLP, where the page GCD is 1. The
//! candidate is stored uncompressed, so per-page selection compares its raw
//! output against the incumbent's *post-LZ4* payload and never regresses the
//! best case.
//!
//! Payload grammar: `uvarint gcd`, then one [`ravel_codec::encoding::Enc`] tag
//! byte, then the `encode_i64` bytes for the `sample_count` reduced values.
//! The decoder treats these as untrusted stored bytes: corrupt or truncated
//! payloads return a typed [`SegmentError`], never a panic and never wrong
//! data.

use crate::error::SegmentError;
use crate::varint::{read_uvarint, write_uvarint};
use ravel_codec::encoding::{Enc, decode_i64, encode_i64};

fn gcd_i64(a: i64, b: i64) -> i64 {
    let (mut a, mut b) = (a.unsigned_abs(), b.unsigned_abs());
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a as i64
}

/// GCD of every timestamp in the page, floored to 1 (so it is a legal divisor
/// even when every value is 0). A stream at millisecond resolution has every
/// value a multiple of `1_000_000`, so the divisor sheds that factor before
/// delta coding.
pub fn page_gcd(ts: &[i64]) -> i64 {
    ts.iter().fold(0i64, |acc, &v| gcd_i64(acc, v)).max(1)
}

/// Encodes `timestamps` as a TS_GCD_I64 payload. Infallible: the page GCD
/// divides every value exactly by construction, and `encode_i64` never fails.
pub fn encode_ts_gcd(timestamps: &[i64]) -> Vec<u8> {
    let g = page_gcd(timestamps);
    let divided: Vec<i64> = timestamps.iter().map(|&v| v / g).collect();
    let (enc, bytes) = encode_i64(&divided);
    let mut out = Vec::with_capacity(bytes.len() + 11);
    write_uvarint(&mut out, g as u64);
    out.push(enc.to_u8());
    out.extend_from_slice(&bytes);
    out
}

/// Decodes exactly `count` timestamps from a TS_GCD_I64 payload, *appending*
/// them to `out` (existing contents preserved; on error the appended tail is
/// unspecified), matching [`crate::ts_delta::decode_ts_deltas_into`] so the
/// query fetcher can concatenate a series' runs. Each reconstructed timestamp
/// is validated against `[min_ts_ns, max_ts_ns]` (docs/segment-format.md).
pub fn decode_ts_gcd_into(
    bytes: &[u8],
    count: usize,
    min_ts_ns: i64,
    max_ts_ns: i64,
    out: &mut Vec<i64>,
) -> Result<(), SegmentError> {
    let mut pos = 0usize;
    let g = read_uvarint(bytes, &mut pos)?;
    if g == 0 {
        return Err(SegmentError::ValuePageCodec(
            "TS_GCD divisor is zero".into(),
        ));
    }
    let g = i64::try_from(g).map_err(|_| SegmentError::FieldOverflow)?;
    let enc_tag = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos += 1;
    let enc = Enc::from_u8(enc_tag)
        .map_err(|_| SegmentError::ValuePageCodec("TS_GCD integer encoding tag invalid".into()))?;
    let divided = decode_i64(enc, &bytes[pos..], count)
        .map_err(|e| SegmentError::ValuePageCodec(e.to_string()))?;
    if divided.len() != count {
        return Err(SegmentError::ValuePageCodec(
            "TS_GCD integer stream length mismatch".into(),
        ));
    }
    out.reserve(count);
    for d in divided {
        let ts = d.checked_mul(g).ok_or(SegmentError::TimestampOverflow)?;
        if ts < min_ts_ns || ts > max_ts_ns {
            return Err(SegmentError::TimestampOutOfBounds {
                ts,
                min: min_ts_ns,
                max: max_ts_ns,
            });
        }
        out.push(ts);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn decode(bytes: &[u8], count: usize, min: i64, max: i64) -> Result<Vec<i64>, SegmentError> {
        let mut out = Vec::new();
        decode_ts_gcd_into(bytes, count, min, max, &mut out)?;
        Ok(out)
    }

    #[test]
    fn page_gcd_sheds_the_millisecond_factor() {
        let ms = 1_000_000i64;
        let ts: Vec<i64> = (0..10)
            .map(|i| 1_700_000_000_000 * ms + i * 15_000 * ms)
            .collect();
        // Every value is a multiple of a millisecond, so the divisor is at
        // least a millisecond.
        assert_eq!(page_gcd(&ts) % ms, 0);
    }

    #[test]
    fn roundtrips_a_regular_ms_stream() {
        let ms = 1_000_000i64;
        let start = 1_700_000_000_000_000_000i64;
        let ts: Vec<i64> = (0..64).map(|i| start + i * 15_000 * ms).collect();
        let enc = encode_ts_gcd(&ts);
        let (min, max) = (ts[0], ts[ts.len() - 1]);
        assert_eq!(decode(&enc, ts.len(), min, max).expect("decodes"), ts);
    }

    #[test]
    fn appends_onto_an_existing_buffer() {
        let ts = [10i64, 20, 30];
        let enc = encode_ts_gcd(&ts);
        let mut out = vec![1i64, 2];
        decode_ts_gcd_into(&enc, ts.len(), 10, 30, &mut out).expect("decodes");
        assert_eq!(out, vec![1, 2, 10, 20, 30]);
    }

    #[test]
    fn out_of_bounds_timestamp_is_rejected() {
        let ts = [10i64, 20, 30];
        let enc = encode_ts_gcd(&ts);
        // Declare bounds that exclude 30.
        assert!(matches!(
            decode(&enc, 3, 10, 25),
            Err(SegmentError::TimestampOutOfBounds { .. })
        ));
    }

    #[test]
    fn truncated_payload_is_a_typed_error() {
        let ts: Vec<i64> = (0..16).map(|i| 1_000 + i * 7).collect();
        let enc = encode_ts_gcd(&ts);
        for cut in 0..enc.len() {
            let res = decode(&enc[..cut], ts.len(), i64::MIN, i64::MAX);
            assert!(
                res.is_err(),
                "truncation to {cut} bytes must be a typed error"
            );
        }
    }

    proptest! {
        /// Ascending streams (any resolution/divisor) round-trip exactly.
        #[test]
        fn roundtrips_ascending_streams(
            base in -1_000_000_000i64..1_000_000_000,
            steps in proptest::collection::vec(1i64..10_000_000, 1..48),
            scale in prop::sample::select(vec![1i64, 1_000, 1_000_000]),
        ) {
            let mut ts = vec![base];
            let mut acc = base;
            for s in &steps {
                acc = acc.saturating_add(s * scale);
                ts.push(acc);
            }
            let enc = encode_ts_gcd(&ts);
            let (min, max) = (
                *ts.iter().min().expect("non-empty"),
                *ts.iter().max().expect("non-empty"),
            );
            let decoded = decode(&enc, ts.len(), min, max).expect("valid stream decodes");
            prop_assert_eq!(decoded, ts);
        }

        /// A mutated or truncated valid stream yields a typed error or a valid
        /// decode, never a panic.
        #[test]
        fn mutation_never_panics(
            steps in proptest::collection::vec(1i64..1_000_000, 1..32),
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
            count in 0usize..256,
        ) {
            let mut ts = vec![0i64];
            let mut acc = 0i64;
            for s in &steps {
                acc += s * 1_000_000;
                ts.push(acc);
            }
            let mut enc = encode_ts_gcd(&ts);
            if !enc.is_empty() {
                let idx = (bit / 8) % enc.len();
                enc[idx] ^= 1u8 << (bit % 8);
                enc.truncate(truncate_to % (enc.len() + 1));
            }
            let _ = decode(&enc, count, i64::MIN, i64::MAX);
        }
    }
}
