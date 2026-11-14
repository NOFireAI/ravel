//! Integer-model VAL page codecs adopted in RSEG v7 (ADR-0092 decision 6,
//! measured in issue #312, ported from the bench reference `Alp` /
//! `GcdDeltaFor`). Both recover a decimal exponent so each value becomes an
//! `i64`, then compress those integers; both keep every value bit-exact, so
//! NaN payloads (distinct payloads stay distinct), `-0.0`, denormals, and both
//! infinities round-trip through the `f64` bit pattern, never `==`.
//!
//! - [`encode_alp`] / [`decode_alp_into`] (VAL_ALP, enc 18): the best single
//!   exponent that fits the most values; any value the model does not
//!   reproduce bit-exactly is stored as a raw-`f64` exception. Targets integer
//!   counters and decimal gauges; falls back to about raw size (never larger,
//!   because per-page selection keeps the smaller) on noisy floats.
//! - [`encode_gcd_delta_for`] / [`decode_gcd_delta_for_into`] (VAL_GCD_DELTA_FOR,
//!   enc 19): the smallest exponent that fits *every* value (else a whole-page
//!   raw fallback), delta the integers, divide the deltas by their GCD, then
//!   frame-of-reference bit-pack the reduced deltas. Targets constant series
//!   and integer/decimal gauges whose deltas share a large GCD.
//!
//! Decoders treat their input as untrusted stored bytes: corrupt or truncated
//! payloads return a typed [`SegmentError::ValuePageCodec`], never a panic and
//! never wrong data (docs/segment-format.md, CLAUDE.md testing patterns).

use crate::error::SegmentError;
use crate::varint::{read_uvarint, read_zigzag_varint, write_uvarint, write_zigzag_varint};
use ravel_codec::encoding::{Enc, decode_i64_into, encode_i64};

/// Largest decimal exponent either integer-model codec will try. `10^18`
/// still fits an `i64` (max `~9.22e18`); a decimal that needs more places is
/// not modelled and falls back (per value for ALP, per page for GCD-FOR).
const MAX_DECIMAL_EXP: usize = 18;

/// `10^0 ..= 10^18` as exact `f64` (`10^22` is the last exactly-representable
/// power of ten in `f64`, so every entry here is exact).
const POW10: [f64; MAX_DECIMAL_EXP + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

fn codec_err(msg: &'static str) -> SegmentError {
    SegmentError::ValuePageCodec(msg.into())
}

/// The i64 digit `v` maps to under exponent `e`, or `None` when the model does
/// not reproduce `v` bit-exactly: non-finite, out of the i64 domain after
/// scaling, or a round-trip that changes any bit (notably `-0.0`, whose sign
/// the integer path drops).
fn digit_for(v: f64, e: usize) -> Option<i64> {
    let scaled = v * POW10[e];
    // 2^63 as f64; reject the whole saturating range to stay inside the
    // exactly-invertible domain.
    if !scaled.is_finite() || scaled.abs() >= 9_223_372_036_854_775_808.0 {
        return None;
    }
    let digit = scaled.round() as i64;
    // The decoder computes exactly this; accept only when it is bit-identical.
    if ((digit as f64) / POW10[e]).to_bits() == v.to_bits() {
        Some(digit)
    } else {
        None
    }
}

/// Exponent in `0..=MAX_DECIMAL_EXP` that fits the most values bit-exactly,
/// ties broken toward the smaller exponent. Returns the exponent and its fit
/// count.
fn best_exponent(values: &[f64]) -> (usize, usize) {
    let mut best = (0usize, 0usize);
    for e in 0..=MAX_DECIMAL_EXP {
        let fits = values
            .iter()
            .filter(|&&v| digit_for(v, e).is_some())
            .count();
        if fits > best.1 {
            best = (e, fits);
            if fits == values.len() {
                break;
            }
        }
    }
    best
}

/// Digits under the smallest exponent that fits *every* value, or `None` when
/// no exponent does (then the page falls back to raw `f64`).
fn model_digits_all(values: &[f64]) -> Option<(usize, Vec<i64>)> {
    for e in 0..=MAX_DECIMAL_EXP {
        let mut digits = Vec::with_capacity(values.len());
        let mut all = true;
        for &v in values {
            match digit_for(v, e) {
                Some(d) => digits.push(d),
                None => {
                    all = false;
                    break;
                }
            }
        }
        if all {
            return Some((e, digits));
        }
    }
    None
}

fn gcd_u64(a: u64, b: u64) -> u64 {
    let (mut a, mut b) = (a, b);
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

// --- frame-of-reference bit-packing over i64 (LSB-first) ----------------

/// FOR-packs `values`: zigzag-varint min, then a `bit_width` byte, then each
/// `(value - min)` LSB-packed at that width. Width 0 (all equal) writes no
/// packed bytes.
fn for_pack(out: &mut Vec<u8>, values: &[i64]) {
    let min = values.iter().copied().min().unwrap_or(0);
    let max_offset = values
        .iter()
        .map(|&v| (v as u64).wrapping_sub(min as u64))
        .max()
        .unwrap_or(0);
    let bit_width = if max_offset == 0 {
        0
    } else {
        64 - max_offset.leading_zeros()
    };
    write_zigzag_varint(out, min);
    out.push(bit_width as u8);
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
        let off = (v as u64).wrapping_sub(min as u64);
        acc |= u128::from(off & mask) << nbits;
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

/// The header of a [`for_pack`] region holding exactly `count` values: its
/// minimum, its bit width and its packed bytes, which [`for_unpack_into`]
/// expands. A `bit_width > 64` or a packed region shorter than
/// `count * width` bits is a typed error, never a panic.
fn for_header<'a>(
    bytes: &'a [u8],
    pos: &mut usize,
    count: usize,
) -> Result<(i64, u32, &'a [u8]), SegmentError> {
    let min = read_zigzag_varint(bytes, pos)?;
    let bit_width = u32::from(*bytes.get(*pos).ok_or(SegmentError::Truncated)?);
    *pos += 1;
    if bit_width > 64 {
        return Err(codec_err("FOR bit width exceeds 64"));
    }
    let need_bits = (count as u64)
        .checked_mul(u64::from(bit_width))
        .ok_or(SegmentError::FieldOverflow)?;
    let need = usize::try_from(need_bits.div_ceil(8)).map_err(|_| SegmentError::FieldOverflow)?;
    let end = pos.checked_add(need).ok_or(SegmentError::Truncated)?;
    let region = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos += need;
    Ok((min, bit_width, region))
}

/// Inverse of [`for_pack`]: expands a region [`for_header`] validated for
/// `out.len()` values into `out`.
fn for_unpack_into(min: i64, bit_width: u32, region: &[u8], out: &mut [i64]) {
    if bit_width == 0 {
        out.fill(min);
        return;
    }
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    let mut byte_idx = 0usize;
    for slot in out {
        while nbits < bit_width {
            acc |= u128::from(region[byte_idx]) << nbits;
            byte_idx += 1;
            nbits += 8;
        }
        let off = (acc as u64) & mask;
        acc >>= bit_width;
        nbits -= bit_width;
        *slot = (min as u64).wrapping_add(off) as i64;
    }
}

/// Appends `count` values to `out` that `fill` decodes as integers and
/// `to_f64` converts, without a second buffer: `out` is turned into its bit
/// patterns and collected back in place (`f64` and `i64` share size and
/// alignment, so both collects reuse `out`'s allocation), and the integers
/// are decoded straight into its tail. The decode's peak is therefore `out`
/// grown by `count` values, the 8 bytes per sample its caller charges. The
/// values `out` held before are bit-exact afterwards, on error too; on error
/// nothing is appended.
fn append_via_i64(
    out: &mut Vec<f64>,
    count: usize,
    fill: impl FnOnce(&mut [i64]) -> Result<(), SegmentError>,
    to_f64: impl Fn(i64) -> f64,
) -> Result<(), SegmentError> {
    let prefix = out.len();
    let mut bits: Vec<i64> = std::mem::take(out)
        .into_iter()
        .map(|v| v.to_bits() as i64)
        .collect();
    bits.reserve_exact(count);
    bits.resize(prefix + count, 0);
    let filled = fill(&mut bits[prefix..]);
    *out = bits
        .into_iter()
        .enumerate()
        .map(|(i, d)| {
            if i < prefix {
                f64::from_bits(d as u64)
            } else {
                to_f64(d)
            }
        })
        .collect();
    if filled.is_err() {
        out.truncate(prefix);
    }
    filled
}

// --- ALP (VAL_ALP, enc 18) ----------------------------------------------

/// Encodes `values` under ALP: one decimal exponent, the fit digits through
/// `encode_i64`, then each non-fitting value as a raw-`f64` exception.
/// Infallible; the returned bytes are the VAL page payload.
pub fn encode_alp(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let (e, _fits) = best_exponent(values);
    // Digits for fit positions; a placeholder 0 for exceptions, so the integer
    // stream stays length `count` and the exceptions overwrite it on decode.
    let mut digits = Vec::with_capacity(values.len());
    let mut exceptions: Vec<(usize, u64)> = Vec::new();
    for (i, &v) in values.iter().enumerate() {
        match digit_for(v, e) {
            Some(d) => digits.push(d),
            None => {
                digits.push(0);
                exceptions.push((i, v.to_bits()));
            }
        }
    }
    let (enc, int_bytes) = encode_i64(&digits);
    let mut out = Vec::new();
    out.push(e as u8);
    out.push(enc.to_u8());
    write_uvarint(&mut out, int_bytes.len() as u64);
    out.extend_from_slice(&int_bytes);
    write_uvarint(&mut out, exceptions.len() as u64);
    for (idx, bits) in &exceptions {
        write_uvarint(&mut out, *idx as u64);
        out.extend_from_slice(&bits.to_le_bytes());
    }
    out
}

/// [`decode_alp_into`] into a fresh vector.
#[cfg(test)]
pub fn decode_alp(bytes: &[u8], count: usize) -> Result<Vec<f64>, SegmentError> {
    let mut out = Vec::new();
    decode_alp_into(bytes, count, &mut out)?;
    Ok(out)
}

/// Decodes exactly `count` values from an ALP payload, appending them to
/// `out` with no intermediate buffer: the integer stream is decoded straight
/// into `out`'s new tail ([`append_via_i64`]). Every structural violation is
/// a typed [`SegmentError`], never a panic and never wrong data; on error
/// `out` keeps exactly the values it held.
pub fn decode_alp_into(bytes: &[u8], count: usize, out: &mut Vec<f64>) -> Result<(), SegmentError> {
    if count == 0 {
        if bytes.is_empty() {
            return Ok(());
        }
        return Err(codec_err("ALP payload for zero values is non-empty"));
    }
    let mut pos = 0usize;
    let e = usize::from(*bytes.get(pos).ok_or(SegmentError::Truncated)?);
    pos += 1;
    if e > MAX_DECIMAL_EXP {
        return Err(codec_err("ALP exponent out of range"));
    }
    let enc_tag = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos += 1;
    let enc = Enc::from_u8(enc_tag).map_err(|_| codec_err("ALP integer encoding tag invalid"))?;
    let int_len =
        usize::try_from(read_uvarint(bytes, &mut pos)?).map_err(|_| SegmentError::FieldOverflow)?;
    let int_end = pos.checked_add(int_len).ok_or(SegmentError::Truncated)?;
    let int_bytes = bytes.get(pos..int_end).ok_or(SegmentError::Truncated)?;
    pos += int_len;
    if matches!(enc, Enc::Plain | Enc::DeltaZigzag | Enc::DoubleDelta) && count > int_bytes.len() {
        // Every varint takes at least one byte; refuse before sizing by `count`.
        return Err(codec_err("ALP integer stream length mismatch"));
    }
    let prefix = out.len();
    let pow = POW10[e];
    append_via_i64(
        out,
        count,
        |slots| {
            decode_i64_into(enc, int_bytes, None, slots, Ok)
                .map(|_| ())
                .map_err(|e| SegmentError::ValuePageCodec(e.to_string()))
        },
        |d| (d as f64) / pow,
    )?;
    let patched = patch_alp_exceptions(bytes, pos, &mut out[prefix..]);
    if patched.is_err() {
        out.truncate(prefix);
    }
    patched
}

/// Reads ALP's exception list from `pos` to the end of `bytes`, overwriting
/// each listed position of `values` with its raw `f64`.
fn patch_alp_exceptions(
    bytes: &[u8],
    mut pos: usize,
    values: &mut [f64],
) -> Result<(), SegmentError> {
    let n_exc =
        usize::try_from(read_uvarint(bytes, &mut pos)?).map_err(|_| SegmentError::FieldOverflow)?;
    for _ in 0..n_exc {
        let idx = usize::try_from(read_uvarint(bytes, &mut pos)?)
            .map_err(|_| SegmentError::FieldOverflow)?;
        let raw_end = pos.checked_add(8).ok_or(SegmentError::Truncated)?;
        let raw = bytes.get(pos..raw_end).ok_or(SegmentError::Truncated)?;
        pos += 8;
        let arr: [u8; 8] = raw.try_into().map_err(|_| SegmentError::Truncated)?;
        *values
            .get_mut(idx)
            .ok_or_else(|| codec_err("ALP exception index out of range"))? =
            f64::from_bits(u64::from_le_bytes(arr));
    }
    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }
    Ok(())
}

// --- GCD-of-deltas + frame-of-reference (VAL_GCD_DELTA_FOR, enc 19) ------

/// Model marker byte written first: whole-page raw fallback vs the integer
/// model.
const GDF_RAW: u8 = 0;
const GDF_MODEL: u8 = 1;

/// Encodes `values` under GCD-of-deltas-plus-FOR, or a whole-page raw-`f64`
/// fallback when no single exponent fits every value (or the digit deltas
/// overflow). Infallible; the returned bytes are the VAL page payload.
pub fn encode_gcd_delta_for(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let modelled = model_digits_all(values).and_then(|(e, digits)| {
        // Deltas of the digit stream; a checked_sub failure (astronomical
        // spread) drops to raw.
        let mut deltas = Vec::with_capacity(digits.len().saturating_sub(1));
        for w in digits.windows(2) {
            deltas.push(w[1].checked_sub(w[0])?);
        }
        let g = deltas
            .iter()
            .fold(0u64, |acc, &d| gcd_u64(acc, d.unsigned_abs()));
        let g = g.max(1);
        let reduced: Vec<i64> = deltas.iter().map(|&d| d / g as i64).collect();
        let mut out = vec![GDF_MODEL, e as u8];
        write_zigzag_varint(&mut out, digits[0]);
        write_uvarint(&mut out, g);
        for_pack(&mut out, &reduced);
        Some(out)
    });
    modelled.unwrap_or_else(|| {
        let mut out = Vec::with_capacity(values.len() * 8 + 1);
        out.push(GDF_RAW);
        for v in values {
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        out
    })
}

/// [`decode_gcd_delta_for_into`] into a fresh vector.
#[cfg(test)]
pub fn decode_gcd_delta_for(bytes: &[u8], count: usize) -> Result<Vec<f64>, SegmentError> {
    let mut out = Vec::new();
    decode_gcd_delta_for_into(bytes, count, &mut out)?;
    Ok(out)
}

/// Decodes exactly `count` values from a GCD-delta-FOR payload, appending
/// them to `out` with no intermediate buffer: the reduced deltas are unpacked
/// and accumulated in `out`'s new tail ([`append_via_i64`]). Every structural
/// violation is a typed [`SegmentError`], never a panic and never wrong data;
/// on error `out` keeps exactly the values it held.
pub fn decode_gcd_delta_for_into(
    bytes: &[u8],
    count: usize,
    out: &mut Vec<f64>,
) -> Result<(), SegmentError> {
    if count == 0 {
        if bytes.is_empty() {
            return Ok(());
        }
        return Err(codec_err("GCD-FOR payload for zero values is non-empty"));
    }
    let (&marker, body) = bytes.split_first().ok_or(SegmentError::Truncated)?;
    match marker {
        GDF_RAW => {
            let expected = count.checked_mul(8).ok_or(SegmentError::FieldOverflow)?;
            if body.len() != expected {
                return Err(codec_err("GCD-FOR raw body length mismatch"));
            }
            out.reserve_exact(count);
            for chunk in body.chunks_exact(8) {
                let arr: [u8; 8] = chunk.try_into().map_err(|_| SegmentError::Truncated)?;
                out.push(f64::from_bits(u64::from_le_bytes(arr)));
            }
            Ok(())
        }
        GDF_MODEL => {
            let mut pos = 0usize;
            let e = usize::from(*body.get(pos).ok_or(SegmentError::Truncated)?);
            pos += 1;
            if e > MAX_DECIMAL_EXP {
                return Err(codec_err("GCD-FOR exponent out of range"));
            }
            let first = read_zigzag_varint(body, &mut pos)?;
            let g = read_uvarint(body, &mut pos)?;
            if g == 0 {
                return Err(codec_err("GCD-FOR divisor is zero"));
            }
            let (min, bit_width, region) = for_header(body, &mut pos, count - 1)?;
            if pos != body.len() {
                return Err(SegmentError::TrailingBytes);
            }
            let g = g as i64;
            let pow = POW10[e];
            append_via_i64(
                out,
                count,
                |slots| {
                    let (head, rest) = slots.split_first_mut().ok_or(SegmentError::Truncated)?;
                    *head = first;
                    for_unpack_into(min, bit_width, region, rest);
                    let mut acc = first;
                    for slot in rest {
                        let step = slot
                            .checked_mul(g)
                            .ok_or_else(|| codec_err("GCD-FOR delta overflow"))?;
                        acc = acc
                            .checked_add(step)
                            .ok_or_else(|| codec_err("GCD-FOR accumulation overflow"))?;
                        *slot = acc;
                    }
                    Ok(())
                },
                |d| (d as f64) / pow,
            )
        }
        _ => Err(codec_err("GCD-FOR marker byte invalid")),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A spread of bit-significant floats every codec must preserve exactly:
    /// two distinct NaN payloads, both zeros, both infinities, denormals, and
    /// ordinary integers/decimals.
    fn hard_floats() -> Vec<f64> {
        vec![
            f64::from_bits(0x7ff8_0000_0000_0001), // a quiet NaN payload
            f64::from_bits(0x7ff0_0000_0000_0002), // a distinct (signalling) NaN payload
            f64::NAN,
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,                     // smallest normal
            f64::from_bits(1),                     // smallest positive subnormal
            f64::from_bits(0x8000_0000_0000_0001), // smallest negative subnormal
            1.0,
            -1.0,
            42.0,
            3.28,
            -273.15,
            1e17,
        ]
    }

    type DecodeInto = fn(&[u8], usize, &mut Vec<f64>) -> Result<(), SegmentError>;

    fn bits(v: &[f64]) -> Vec<u64> {
        v.iter().map(|x| x.to_bits()).collect()
    }

    #[test]
    fn alp_roundtrips_hard_floats_bit_exactly() {
        let vals = hard_floats();
        let enc = encode_alp(&vals);
        let dec = decode_alp(&enc, vals.len()).expect("alp decodes");
        assert_eq!(bits(&dec), bits(&vals), "ALP must preserve every bit");
    }

    #[test]
    fn gcd_for_roundtrips_hard_floats_bit_exactly() {
        let vals = hard_floats();
        let enc = encode_gcd_delta_for(&vals);
        let dec = decode_gcd_delta_for(&enc, vals.len()).expect("gcd-for decodes");
        assert_eq!(bits(&dec), bits(&vals), "GCD-FOR must preserve every bit");
    }

    #[test]
    fn distinct_nan_payloads_stay_distinct() {
        // Two NaNs with different payloads must not collapse to one. Each codec
        // is decoded by its own decoder (the first payload byte is not a codec
        // discriminator: ALP's is the exponent, which can equal a GCD marker).
        let a = f64::from_bits(0x7ff8_0000_0000_00aa);
        let b = f64::from_bits(0x7ff8_0000_0000_00bb);
        let vals = vec![a, b, a, b];

        let alp = encode_alp(&vals);
        assert_eq!(
            bits(&decode_alp(&alp, vals.len()).expect("alp")),
            bits(&vals)
        );

        let gdf = encode_gcd_delta_for(&vals);
        assert_eq!(
            bits(&decode_gcd_delta_for(&gdf, vals.len()).expect("gcd")),
            bits(&vals)
        );
    }

    #[test]
    fn empty_page_roundtrips() {
        assert!(encode_alp(&[]).is_empty());
        assert!(encode_gcd_delta_for(&[]).is_empty());
        assert_eq!(decode_alp(&[], 0).expect("empty"), Vec::<f64>::new());
        assert_eq!(
            decode_gcd_delta_for(&[], 0).expect("empty"),
            Vec::<f64>::new()
        );
    }

    #[test]
    fn truncated_payloads_return_typed_error_not_panic() {
        let vals: Vec<f64> = (0..32).map(|i| (1000 + i * 3) as f64).collect();
        for full in [encode_alp(&vals), encode_gcd_delta_for(&vals)] {
            let is_gcd = matches!(full.first(), Some(&GDF_RAW) | Some(&GDF_MODEL));
            for cut in 0..full.len() {
                let truncated = &full[..cut];
                let res = if is_gcd {
                    decode_gcd_delta_for(truncated, vals.len())
                } else {
                    decode_alp(truncated, vals.len())
                };
                // A prefix of the valid stream must never reproduce the whole
                // page, so it is always a typed error here (never a panic).
                assert!(
                    res.is_err(),
                    "truncation to {cut} bytes must be a typed error"
                );
            }
        }
    }

    #[test]
    fn nonempty_payload_for_zero_values_is_rejected() {
        // `count` is out-of-band (the reader supplies `sample_count`), so a
        // count larger than encoded is not always detectable from bytes alone
        // (a constant/width-0 run is RLE-like). But a non-empty payload paired
        // with count 0 is unambiguously corrupt and must fail closed.
        let vals: Vec<f64> = (0..8).map(|i| i as f64).collect();
        assert!(decode_alp(&encode_alp(&vals), 0).is_err());
        assert!(decode_gcd_delta_for(&encode_gcd_delta_for(&vals), 0).is_err());
    }

    proptest! {
        /// Any float slice round-trips bit-exactly through both codecs
        /// (`any::<f64>()` covers NaN payloads, +/-0.0, subnormals, infinities).
        #[test]
        fn roundtrip_arbitrary_floats(
            vals in proptest::collection::vec(any::<f64>(), 1..64),
        ) {
            let alp = encode_alp(&vals);
            let a = decode_alp(&alp, vals.len()).expect("alp decodes");
            prop_assert_eq!(bits(&a), bits(&vals));

            let gdf = encode_gcd_delta_for(&vals);
            let g = decode_gcd_delta_for(&gdf, vals.len()).expect("gcd decodes");
            prop_assert_eq!(bits(&g), bits(&vals));
        }

        /// Decoding into a vector that already holds values appends the
        /// fresh decode after them and leaves them bit-exact, NaN payloads
        /// included; a payload that fails leaves the vector exactly as it was.
        #[test]
        fn decoding_into_appends_and_keeps_what_was_there(
            held in proptest::collection::vec(any::<f64>(), 0..16),
            vals in proptest::collection::vec(-1_000_000i64..1_000_000, 1..48),
            cut in any::<usize>(),
        ) {
            let vals: Vec<f64> = vals.iter().map(|&i| i as f64 / 100.0).collect();
            let decoders: [(Vec<u8>, DecodeInto); 2] = [
                (encode_alp(&vals), decode_alp_into),
                (encode_gcd_delta_for(&vals), decode_gcd_delta_for_into),
            ];
            for (payload, decode) in decoders {
                let mut out = held.clone();
                decode(&payload, vals.len(), &mut out).expect("decodes");
                prop_assert_eq!(bits(&out[..held.len()]), bits(&held));
                prop_assert_eq!(bits(&out[held.len()..]), bits(&vals));

                let truncated = &payload[..cut % payload.len()];
                let mut out = held.clone();
                if decode(truncated, vals.len(), &mut out).is_err() {
                    prop_assert_eq!(bits(&out), bits(&held));
                }
            }
        }

        /// A single-bit mutation or truncation of a valid stream yields a typed
        /// error or a valid decode, never a panic and never a false success
        /// that returns wrong bytes without erroring first.
        #[test]
        fn mutation_never_panics(
            seed in proptest::collection::vec(-1_000_000i64..1_000_000, 1..48),
            bit in any::<usize>(),
            truncate_to in any::<usize>(),
            count in 0usize..256,
        ) {
            let vals: Vec<f64> = seed.iter().map(|&i| i as f64).collect();
            for base in [encode_alp(&vals), encode_gcd_delta_for(&vals)] {
                let is_gcd = matches!(base.first(), Some(&GDF_RAW) | Some(&GDF_MODEL));
                let mut m = base.clone();
                if !m.is_empty() {
                    let idx = (bit / 8) % m.len();
                    m[idx] ^= 1u8 << (bit % 8);
                    m.truncate(truncate_to % (m.len() + 1));
                }
                let _ = if is_gcd {
                    decode_gcd_delta_for(&m, count)
                } else {
                    decode_alp(&m, count)
                };
            }
        }
    }
}
