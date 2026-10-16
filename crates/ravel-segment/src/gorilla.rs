//! VAL_GORILLA (enc 16): Gorilla XOR value compression, classic control-bit
//! scheme from the Gorilla paper section 4.1.2. First value stored raw (64
//! bits); each following value is XORed with the previous value and encoded
//! as:
//!
//!   - xor == 0: bit `0` (same as previous)
//!   - xor fits prior window: bits `10` + meaningful bits (reuse block)
//!   - otherwise: bits `11` + 5-bit leading-zero count (clamped to 31) +
//!     6-bit (meaningful_len - 1) + meaningful bits (new block)
//!
//! The whole stream is byte-padded with zero bits at the end. Pure bit
//! manipulation on `f64::to_bits`/`from_bits`, so NaN payloads, -0.0, and
//! denormals round-trip exactly.

use crate::error::SegmentError;

/// MSB-first bit packer appending to a borrowed buffer, zero-padded to a
/// byte boundary on `finish`.
struct BitWriter<'a> {
    buf: &'a mut Vec<u8>,
    cur: u8,
    nbits: u8,
}

impl<'a> BitWriter<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        BitWriter {
            buf,
            cur: 0,
            nbits: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        self.cur = (self.cur << 1) | u8::from(bit);
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.nbits = 0;
        }
    }

    /// Writes the low `nbits` bits of `value`, most-significant bit first.
    /// `nbits` must be `<= 64`.
    fn write_bits(&mut self, value: u64, nbits: u32) {
        for i in (0..nbits).rev() {
            self.write_bit(((value >> i) & 1) == 1);
        }
    }

    fn finish(mut self) {
        if self.nbits > 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
    }
}

/// MSB-first bit reader over a byte slice. Every read is bounds-checked;
/// running out of bits is a [`SegmentError`], never a panic.
struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: u64,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitReader { data, bit_pos: 0 }
    }

    fn read_bit(&mut self) -> Result<bool, SegmentError> {
        let byte_idx = usize::try_from(self.bit_pos / 8).map_err(|_| SegmentError::Truncated)?;
        let bit_idx = 7 - (self.bit_pos % 8) as u32;
        let byte = *self.data.get(byte_idx).ok_or(SegmentError::Truncated)?;
        self.bit_pos += 1;
        Ok((byte >> bit_idx) & 1 == 1)
    }

    /// Reads `nbits` (`<= 64`) bits into a right-aligned `u64`.
    fn read_bits(&mut self, nbits: u32) -> Result<u64, SegmentError> {
        if nbits > 64 {
            return Err(SegmentError::CorruptGorilla);
        }
        let mut result = 0u64;
        for _ in 0..nbits {
            result = (result << 1) | u64::from(self.read_bit()?);
        }
        Ok(result)
    }
}

fn mask64(nbits: u32) -> u64 {
    if nbits >= 64 {
        u64::MAX
    } else {
        (1u64 << nbits) - 1
    }
}

/// Encodes `values` as a Gorilla XOR bit stream, byte-padded with zeros.
/// Test-only convenience; production encoding goes through
/// [`encode_gorilla_into`] with a reused buffer.
#[cfg(test)]
pub fn encode_gorilla(values: &[f64]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_gorilla_into(values, &mut out);
    out
}

/// Appends the Gorilla encoding of `values` to `out` without allocating,
/// so the writer can reuse one scratch buffer across series.
pub fn encode_gorilla_into(values: &[f64], out: &mut Vec<u8>) {
    let mut w = BitWriter::new(out);
    let mut iter = values.iter();
    let Some(first) = iter.next() else {
        return w.finish();
    };
    let mut prev = first.to_bits();
    w.write_bits(prev, 64);
    // (leading_zeros, trailing_zeros) of the block window established by the
    // last "new block" control code, clamped leading zeros per the 5-bit field.
    let mut window: Option<(u32, u32)> = None;
    for value in iter {
        let cur = value.to_bits();
        let xor = cur ^ prev;
        if xor == 0 {
            w.write_bit(false);
        } else {
            let real_lead = xor.leading_zeros();
            let trail = xor.trailing_zeros();
            let reuses_window = matches!(window, Some((wl, wt)) if real_lead >= wl && trail >= wt);
            if reuses_window {
                // `window` is `Some` by construction of `reuses_window`.
                if let Some((wl, wt)) = window {
                    w.write_bit(true);
                    w.write_bit(false);
                    let meaningful_len = 64 - wl - wt;
                    let meaningful = (xor >> wt) & mask64(meaningful_len);
                    w.write_bits(meaningful, meaningful_len);
                }
            } else {
                let lead = real_lead.min(31);
                w.write_bit(true);
                w.write_bit(true);
                w.write_bits(u64::from(lead), 5);
                let meaningful_len = 64 - lead - trail;
                w.write_bits(u64::from(meaningful_len - 1), 6);
                let meaningful = (xor >> trail) & mask64(meaningful_len);
                w.write_bits(meaningful, meaningful_len);
                window = Some((lead, trail));
            }
        }
        prev = cur;
    }
    w.finish()
}

/// Decodes exactly `count` Gorilla-encoded values from `bytes`, *appending*
/// them to a caller-supplied buffer (existing contents are preserved; on
/// error the appended tail is unspecified). Rejects any malformed control
/// sequence (out-of-range window arithmetic, truncated stream) as
/// [`SegmentError::CorruptGorilla`] or [`SegmentError::Truncated`]; never
/// panics. Appending (rather than clearing first) lets a caller concatenate
/// several runs' pages into one buffer, which the query fetcher's L0 branch
/// relies on; a caller that wants a fresh decode clears the buffer
/// itself.
pub fn decode_gorilla_into(
    bytes: &[u8],
    count: usize,
    out: &mut Vec<f64>,
) -> Result<(), SegmentError> {
    if count == 0 {
        return Ok(());
    }
    out.reserve(count);
    let mut r = BitReader::new(bytes);
    let mut prev = r.read_bits(64)?;
    out.push(f64::from_bits(prev));
    let mut window: Option<(u32, u32)> = None;
    for _ in 1..count {
        if !r.read_bit()? {
            out.push(f64::from_bits(prev));
            continue;
        }
        if !r.read_bit()? {
            let (wl, wt) = window.ok_or(SegmentError::CorruptGorilla)?;
            let meaningful_len = 64u32
                .checked_sub(wl)
                .and_then(|v| v.checked_sub(wt))
                .filter(|&v| v > 0)
                .ok_or(SegmentError::CorruptGorilla)?;
            let bits = r.read_bits(meaningful_len)?;
            let xor = bits.checked_shl(wt).ok_or(SegmentError::CorruptGorilla)?;
            prev ^= xor;
            out.push(f64::from_bits(prev));
        } else {
            let lead = u32::try_from(r.read_bits(5)?).map_err(|_| SegmentError::CorruptGorilla)?;
            let mlen_minus1 =
                u32::try_from(r.read_bits(6)?).map_err(|_| SegmentError::CorruptGorilla)?;
            let meaningful_len = mlen_minus1 + 1; // 1..=64
            let trail = 64u32
                .checked_sub(lead)
                .and_then(|v| v.checked_sub(meaningful_len))
                .ok_or(SegmentError::CorruptGorilla)?;
            let bits = r.read_bits(meaningful_len)?;
            let xor = bits
                .checked_shl(trail)
                .ok_or(SegmentError::CorruptGorilla)?;
            prev ^= xor;
            out.push(f64::from_bits(prev));
            window = Some((lead, trail));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn decode_gorilla(bytes: &[u8], count: usize) -> Result<Vec<f64>, SegmentError> {
        let mut out = Vec::new();
        decode_gorilla_into(bytes, count, &mut out)?;
        Ok(out)
    }

    fn bits(values: &[f64]) -> Vec<u64> {
        values.iter().map(|v| v.to_bits()).collect()
    }

    #[test]
    fn constant_series_hand_vector() {
        // 1.0 = 0x3FF0000000000000 raw, then three "same as previous" bits
        // (xor == 0), padded with 5 zero bits.
        let encoded = encode_gorilla(&[1.0, 1.0, 1.0, 1.0]);
        assert_eq!(
            encoded,
            vec![0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        let decoded = decode_gorilla(&encoded, 4).expect("decodes");
        assert_eq!(bits(&decoded), bits(&[1.0, 1.0, 1.0, 1.0]));
    }

    #[test]
    fn two_value_hand_vector() {
        // 0.0 raw (8 zero bytes), then a "new block" transition to 1.0:
        // xor = 0x3FF0000000000000, leading_zeros=2, trailing_zeros=52,
        // meaningful_len=10, meaningful bits = 0b11_1111_1111 (1023).
        // Control stream: 11 00010 001001 1111111111, padded with one 0 bit.
        let encoded = encode_gorilla(&[0.0, 1.0]);
        assert_eq!(
            encoded,
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC4, 0x4F, 0xFE
            ]
        );
        let decoded = decode_gorilla(&encoded, 2).expect("decodes");
        assert_eq!(bits(&decoded), bits(&[0.0, 1.0]));
    }

    #[test]
    fn window_reuse_hand_vector() {
        // Independent known-answer vector for the "10" window-reuse control
        // path (gorilla.rs:133-141 encode, :184-194 decode). Bytes
        // hand-computed from enc 16 and re-derived here, independent of the
        // encoder.
        //
        //   values: [0.0, from_bits(0x0F00_0000_0000_0000),
        //                 from_bits(0x0800_0000_0000_0000)]
        //
        //   v0 = 0.0                      -> 64 raw zero bits (8 x 0x00)
        //   v1: xor = 0x0F00_0000_0000_0000, real_lead=4, trail=56, mlen=4.
        //       "new block" 11 | lead=4 (00100) | mlen-1=3 (000011)
        //       | meaningful=1111. window := (4, 56).
        //   v2: xor = 0x0700_0000_0000_0000, real_lead=5>=4, trail=56>=56, so
        //       the window is reused: "10" | meaningful=(xor>>56)&0xF = 0111.
        //   Control bits: 11 00100 000011 1111 10 0111, padded with one 0 bit
        //   -> 0xC8, 0x1F, 0xCE.
        let values = [
            0.0,
            f64::from_bits(0x0F00_0000_0000_0000),
            f64::from_bits(0x0800_0000_0000_0000),
        ];
        let expected = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC8, 0x1F, 0xCE,
        ];
        // Decode direction: bytes -> values, independent of the encoder.
        let decoded = decode_gorilla(&expected, values.len()).expect("decodes");
        assert_eq!(bits(&decoded), bits(&values));
        // Encode direction: values -> bytes, pinned against the same oracle so
        // both directions are checked against an external reference, not each
        // other.
        assert_eq!(encode_gorilla(&values), expected);
    }

    #[test]
    fn clamped_leading_hand_vector() {
        // Independent known-answer vector for the clamped leading-zero path
        // (real_lead > 31, clamped to 31 by gorilla.rs:143). Bytes
        // hand-computed from enc 16 and re-derived here, independent of the
        // encoder.
        //
        //   values: [0.0, from_bits(0x0000_0000_0010_0000)]
        //
        //   v0 = 0.0                      -> 64 raw zero bits (8 x 0x00)
        //   v1: xor = 0x0000_0000_0010_0000 (bit 20 only), real_lead=43 which
        //       clamps to lead=31, trail=20, meaningful_len = 64-31-20 = 13.
        //       "new block" 11 | lead=31 (11111) | mlen-1=12 (001100)
        //       | meaningful = (xor>>20)&0x1FFF = 1 (13 bits: ...0000001).
        //   Control bits: 11 11111 001100 0000000000001, padded with six 0
        //   bits -> 0xFE, 0x60, 0x00, 0x40.
        let values = [0.0, f64::from_bits(0x0000_0000_0010_0000)];
        let expected = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFE, 0x60, 0x00, 0x40,
        ];
        // Decode direction: bytes -> values, independent of the encoder.
        let decoded = decode_gorilla(&expected, values.len()).expect("decodes");
        assert_eq!(bits(&decoded), bits(&values));
        // Encode direction: values -> bytes, pinned against the same oracle.
        assert_eq!(encode_gorilla(&values), expected);
    }

    #[test]
    fn empty_series_encodes_to_empty_stream() {
        assert_eq!(encode_gorilla(&[]), Vec::<u8>::new());
        assert_eq!(decode_gorilla(&[], 0).expect("decodes"), Vec::<f64>::new());
    }

    #[test]
    fn single_value_roundtrips() {
        let encoded = encode_gorilla(&[42.5]);
        assert_eq!(encoded.len(), 8);
        let decoded = decode_gorilla(&encoded, 1).expect("decodes");
        assert_eq!(bits(&decoded), bits(&[42.5]));
    }

    #[test]
    fn alternating_values_roundtrip() {
        // Exercises the "new block" path on every transition (since the
        // window keeps changing) as well as same-value runs.
        let values = [1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0];
        let encoded = encode_gorilla(&values);
        let decoded = decode_gorilla(&encoded, values.len()).expect("decodes");
        assert_eq!(bits(&decoded), bits(&values));
    }

    #[test]
    fn reused_window_roundtrip() {
        // Values chosen so consecutive xors share the same leading/trailing
        // zero window, forcing the "reuse" (10) control path repeatedly.
        let values = [0.0, 1.0, 0.0, 1.0, 0.0, 2.0, 0.0];
        let encoded = encode_gorilla(&values);
        let decoded = decode_gorilla(&encoded, values.len()).expect("decodes");
        assert_eq!(bits(&decoded), bits(&values));
    }

    #[test]
    fn special_values_roundtrip_exactly() {
        let values = [
            0.0,
            -0.0,
            f64::NAN,
            -f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE, // smallest normal
            5e-324,            // smallest positive denormal
            -5e-324,
            f64::MAX,
            f64::MIN,
        ];
        let encoded = encode_gorilla(&values);
        let decoded = decode_gorilla(&encoded, values.len()).expect("decodes");
        assert_eq!(bits(&decoded), bits(&values));
    }

    #[test]
    fn decode_rejects_truncated_stream() {
        let encoded = encode_gorilla(&[0.0, 1.0, 2.0, 3.0]);
        for cut in 0..8 {
            let truncated = &encoded[..encoded.len().saturating_sub(cut).max(1)];
            // Either it errors or (rarely, if only padding was cut) matches;
            // above all it must never panic.
            let _ = decode_gorilla(truncated, 4);
        }
        // A stream of zero bytes cannot supply the mandatory first 64 bits.
        assert!(decode_gorilla(&[], 4).is_err());
    }

    #[test]
    fn decode_rejects_reuse_without_prior_window() {
        // First value raw (any 8 bytes), then bits `1 0` (reuse) with no
        // window ever established: must error, not panic or underflow.
        let mut bytes = Vec::new();
        let mut w = BitWriter::new(&mut bytes);
        w.write_bits(0u64, 64);
        w.write_bit(true);
        w.write_bit(false);
        w.write_bits(0, 10);
        w.finish();
        assert_eq!(decode_gorilla(&bytes, 2), Err(SegmentError::CorruptGorilla));
    }

    #[test]
    fn decode_rejects_new_block_overflowing_bit_budget() {
        // lead=31 (max 5-bit value), meaningful_len=64 (mlen_minus1=63, max
        // 6-bit value): lead + meaningful_len = 95 > 64, must error cleanly.
        let mut bytes = Vec::new();
        let mut w = BitWriter::new(&mut bytes);
        w.write_bits(0u64, 64);
        w.write_bit(true);
        w.write_bit(true);
        w.write_bits(31, 5);
        w.write_bits(63, 6);
        w.write_bits(0, 64);
        w.finish();
        assert_eq!(decode_gorilla(&bytes, 2), Err(SegmentError::CorruptGorilla));
    }

    #[test]
    fn bit_writer_reader_roundtrip_all_widths() {
        for nbits in 0..=64u32 {
            let value = mask64(nbits);
            let mut bytes = Vec::new();
            let mut w = BitWriter::new(&mut bytes);
            w.write_bits(value, nbits);
            w.finish();
            let mut r = BitReader::new(&bytes);
            let got = r.read_bits(nbits).expect("reads");
            assert_eq!(got, value, "nbits={nbits}");
        }
    }
}

/// Corrupt-input mutation harness. The
/// Gorilla decoder reads an untrusted XOR bit stream; the contract is a
/// typed error or a valid decode, never a panic and never wrong data
/// (docs/segment-format.md, CLAUDE.md testing patterns). The hand-vectors
/// above pin chosen malformed control sequences; these proptests explore
/// the arbitrary/mutated byte space on the pinned stable toolchain
/// (proptest, no cargo-fuzz).
///
/// `count` is bounded to a realistic range on purpose: it is a separate
/// decode input (the reader derives it from `sample_count`) and the
/// production decoder pre-reserves `count` slots, so these targets fuzz the
/// *bit-stream grammar* rather than a decoder's reaction to an absurd
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

    fn decode(bytes: &[u8], count: usize) -> Result<Vec<f64>, SegmentError> {
        let mut out = Vec::new();
        decode_gorilla_into(bytes, count, &mut out)?;
        Ok(out)
    }

    fn value_bits_eq(a: &[f64], b: &[f64]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
    }

    prop_compose! {
        /// f64 values biased toward the special payloads the codec must
        /// preserve bit-exactly, mixed with arbitrary bit patterns.
        fn any_f64()(choice in 0u8..7, raw in any::<u64>()) -> f64 {
            match choice {
                0 => 0.0,
                1 => -0.0,
                2 => f64::NAN,
                3 => f64::INFINITY,
                4 => f64::NEG_INFINITY,
                5 => f64::from_bits(raw),
                _ => (raw as i64 as f64) * 0.5,
            }
        }
    }

    proptest! {
        /// Decoding arbitrary bytes with an arbitrary (bounded) count must
        /// yield a typed error or a valid decode, never a panic.
        #[test]
        fn decode_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..1024,
        ) {
            let _ = decode(&bytes, count);
        }

        /// Any value sequence round-trips bit-exactly, including NaN
        /// payloads and signed zero (no-wrong-data on the valid space).
        #[test]
        fn roundtrips_arbitrary_values(values in proptest::collection::vec(any_f64(), 0..96)) {
            let encoded = encode_gorilla(&values);
            let decoded = decode(&encoded, values.len()).expect("valid stream decodes");
            prop_assert!(value_bits_eq(&decoded, &values));
        }

        /// A valid stream with a single bit flipped or truncated, decoded at
        /// the original count or a nearby wrong count, must only yield a
        /// typed error or a valid decode, never a panic.
        #[test]
        fn seed_mutation_never_panics(
            values in proptest::collection::vec(any_f64(), 1..48),
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
            count_delta in 0usize..8,
        ) {
            let encoded = encode_gorilla(&values);
            let corrupt = mutate(&encoded, bit, truncate_to);
            let _ = decode(&corrupt, values.len().saturating_add(count_delta));
            let _ = decode(&corrupt, values.len().saturating_sub(count_delta));
        }
    }
}
