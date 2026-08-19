//! Self-contained float-value codecs for the codec bake-off
//! (`src/bin/codec_bakeoff.rs`). Report-only: these live in the bench crate
//! and never touch a production code path. They exist to be compared against
//! the production Gorilla codec (RSEG enc 16, docs/segment-format.md), which
//! is measured through its public segment API in the bin, not reimplemented
//! here.
//!
//! Four challengers plus a raw baseline:
//!
//! - [`Alp`]: Adaptive Lossless floating-Point compression (Afroozeh,
//!   Kuffo, Boncz, "ALP: Adaptive Lossless floating-Point Compression",
//!   SIGMOD 2024). Recover a decimal exponent `e` so each value becomes the
//!   integer `round(v * 10^e)`, compress those integers through
//!   `encode_i64` (the same integer path RSEG v7 uses for its
//!   provenance columns, ADR-0092), and store any value the model does not
//!   reproduce bit-exactly as a raw `f64` exception. Clean fallback: a value
//!   that overflows the integer domain, is non-finite, or is `-0.0` becomes an
//!   exception rather than corrupting the stream.
//! - [`GcdDeltaFor`]: recover the same decimal exponent (all values must fit,
//!   else a whole-page raw fallback), delta the resulting integers, divide the
//!   deltas by their greatest common divisor, then frame-of-reference
//!   bit-pack the reduced deltas. Targets integer-valued counters and
//!   fixed-precision decimal gauges, where the deltas share a large GCD.
//! - [`Chimp128`]: the 128-previous-values variant of the Chimp codec
//!   (Liakos, Papakonstantinopoulou, Kotidis, "Chimp: Efficient Lossless
//!   Floating Point Compression for Time Series Databases", VLDB 2022). Each
//!   value is XORed against the best of the last 128 values, found through a
//!   trailing-significant-bits index, with a 7-bit back-reference and Chimp's
//!   3-bit quantized leading-zero buckets.
//! - [`ByteSplitZstd`]: byte-stream-split. Transpose the eight byte planes of
//!   the f64 stream (all byte-0s, then all byte-1s, ...) so each plane is
//!   low-entropy, then zstd level 3 over the transposed buffer.
//! - [`RawF64`]: little-endian `f64` bytes, no compression (RSEG enc 17's
//!   payload, the 8-bytes-per-sample floor).
//!
//! Every codec round-trips by `f64` bit pattern, so NaN payloads, -0.0, and
//! denormals survive exactly (asserted by `tests/codec_roundtrip.rs`).
//! Decoders treat their input as untrusted: corrupt or truncated bytes return
//! a typed [`CodecError`], never a panic.

use ravel_codec::encoding::{Enc, decode_i64, encode_i64};

/// Typed decode failure. Encoding never fails, so only decoders return this.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The stream ended before the decoder had read every value it was told
    /// to expect.
    #[error("truncated encoded stream")]
    Truncated,
    /// A control sequence or field was structurally impossible (e.g. a
    /// leading/trailing-zero split that does not sum within 64 bits).
    #[error("corrupt encoded stream")]
    Corrupt,
    /// A fixed-width codec's byte count did not match `count` values.
    #[error("length mismatch: expected {expected} bytes for {count} values, got {actual}")]
    LengthMismatch {
        expected: usize,
        actual: usize,
        count: usize,
    },
    /// zstd rejected the frame (byte-split decode).
    #[error("decompression failed")]
    Decompress,
    /// `count * 8` (or a similar size) overflowed `usize`.
    #[error("size overflow")]
    Overflow,
}

/// A float-value page codec: encode a value slice to bytes, decode exactly
/// `count` values back. `count` is carried out of band, exactly as the RSEG
/// page format carries `sample_count` in SERIES_TABLE rather than in the page
/// payload (docs/segment-format.md), so every codec here matches that
/// contract and none self-delimits.
pub trait ValueCodec {
    /// Stable identifier used as the report row label.
    fn name(&self) -> &'static str;
    /// Encodes `values`; the returned length is what the bake-off charges as
    /// payload bytes.
    fn encode(&self, values: &[f64]) -> Vec<u8>;
    /// Decodes exactly `count` values, or returns a typed error. Never panics
    /// on adversarial input.
    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError>;
}

// --- shared MSB-first bit IO (Chimp) -------------------------------------

/// MSB-first bit packer appending to an owned buffer, zero-padded to a byte
/// boundary on `finish`. Mirrors the segment crate's private Gorilla
/// `BitWriter` so the two codecs are compared on the same packing discipline.
struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    nbits: u8,
}

impl BitWriter {
    fn new() -> Self {
        BitWriter {
            buf: Vec::new(),
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

    /// Writes the low `nbits` (`<= 64`) bits of `value`, most-significant
    /// first.
    fn write_bits(&mut self, value: u64, nbits: u32) {
        for i in (0..nbits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
        self.buf
    }
}

/// MSB-first bit reader over a byte slice; every read is bounds-checked and
/// running out of bits is [`CodecError::Truncated`], never a panic.
struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: u64,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitReader { data, bit_pos: 0 }
    }

    fn read_bit(&mut self) -> Result<bool, CodecError> {
        let byte_idx = usize::try_from(self.bit_pos / 8).map_err(|_| CodecError::Truncated)?;
        let bit_idx = 7 - (self.bit_pos % 8) as u32;
        let byte = *self.data.get(byte_idx).ok_or(CodecError::Truncated)?;
        self.bit_pos += 1;
        Ok((byte >> bit_idx) & 1 == 1)
    }

    /// Reads `nbits` (`<= 64`) bits into a right-aligned `u64`.
    fn read_bits(&mut self, nbits: u32) -> Result<u64, CodecError> {
        if nbits > 64 {
            return Err(CodecError::Corrupt);
        }
        let mut result = 0u64;
        for _ in 0..nbits {
            result = (result << 1) | u64::from(self.read_bit()?);
        }
        Ok(result)
    }
}

// --- Chimp128 ------------------------------------------------------------

/// Raw-leading-zeros (0..=63) to Chimp's 3-bit bucket code (0..=7).
const LEADING_REPR: [u8; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 0..7
    1, 1, 1, 1, // 8..11
    2, 2, 2, 2, // 12..15
    3, 3, // 16..17
    4, 4, // 18..19
    5, 5, // 20..21
    6, 6, // 22..23
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, // 24..63
];

/// Raw-leading-zeros (0..=63) to the rounded-down bucket value it encodes as.
const LEADING_ROUND: [u32; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 0..7 -> 0
    8, 8, 8, 8, // 8..11 -> 8
    12, 12, 12, 12, // 12..15 -> 12
    16, 16, // 16..17 -> 16
    18, 18, // 18..19 -> 18
    20, 20, // 20..21 -> 20
    22, 22, // 22..23 -> 22
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 24..63 -> 24
];

/// 3-bit bucket code (0..=7) back to its rounded leading-zero count.
const LEADING_DECODE: [u32; 8] = [0, 8, 12, 16, 18, 20, 22, 24];

const PREVIOUS_VALUES: usize = 128;
const PREVIOUS_VALUES_LOG2: u32 = 7;
const THRESHOLD: u32 = 6 + PREVIOUS_VALUES_LOG2; // 13
const SET_LSB: u64 = (1u64 << (THRESHOLD + 1)) - 1; // 2^14 - 1
const FLAG_ZERO_SIZE: u32 = PREVIOUS_VALUES_LOG2 + 2; // 9: [00][index:7]
const FLAG_ONE_SIZE: u32 = PREVIOUS_VALUES_LOG2 + 11; // 18: [01][index:7][lead:3][sig:6]
/// Sentinel "no reusable window": never equals a real rounded leading count
/// (max 24), so a stray flag-`10` reuse after a flag-`00`/`01` decodes as
/// corrupt rather than reading a stale window.
const NO_WINDOW: u32 = u32::MAX;

/// Chimp128 codec (see module docs). Stateless handle; all state is local to
/// each `encode`/`decode` call.
pub struct Chimp128;

impl ValueCodec for Chimp128 {
    fn name(&self) -> &'static str {
        "chimp128"
    }

    fn encode(&self, values: &[f64]) -> Vec<u8> {
        let mut w = BitWriter::new();
        let Some((first, rest)) = values.split_first() else {
            return w.finish();
        };

        let mut stored = [0u64; PREVIOUS_VALUES];
        // `indices[key]` = absolute index of the last value whose low-14-bit
        // key matched; used only by the encoder to find a back-reference.
        let mut indices = vec![0usize; (SET_LSB as usize) + 1];
        let mut stored_leading = NO_WINDOW;

        let first_bits = first.to_bits();
        w.write_bits(first_bits, 64);
        stored[0] = first_bits;
        indices[(first_bits & SET_LSB) as usize] = 0;
        let mut index: usize = 0; // absolute index of the most recent stored value

        for value in rest {
            let cur = value.to_bits();
            let key = (cur & SET_LSB) as usize;
            let curr_index = indices[key];

            // Prefer the indexed back-reference when it is still inside the
            // 128-value window and shares enough trailing zeros; otherwise XOR
            // against the immediately previous value.
            let in_window = index
                .checked_sub(curr_index)
                .is_some_and(|d| d < PREVIOUS_VALUES);
            let (previous_index, xor, trailing) = if in_window {
                let cand_slot = curr_index % PREVIOUS_VALUES;
                let temp_xor = cur ^ stored[cand_slot];
                let tz = temp_xor.trailing_zeros();
                if tz > THRESHOLD {
                    (cand_slot, temp_xor, tz)
                } else {
                    let slot = index % PREVIOUS_VALUES;
                    (slot, cur ^ stored[slot], 0)
                }
            } else {
                let slot = index % PREVIOUS_VALUES;
                (slot, cur ^ stored[slot], 0)
            };

            if xor == 0 {
                // flag 00 + 7-bit back-reference (top two of the 9 bits zero).
                w.write_bits(previous_index as u64, FLAG_ZERO_SIZE);
                stored_leading = NO_WINDOW;
            } else {
                let lead_raw = xor.leading_zeros() as usize;
                let lead_round = LEADING_ROUND[lead_raw];
                let lead_code = u64::from(LEADING_REPR[lead_raw]);
                if trailing > THRESHOLD {
                    // flag 01: transmit the back-reference, quantized leading
                    // count, and significant-bit width, then the significant
                    // bits (trailing zeros stripped and reconstructed).
                    let sig = 64 - lead_round - trailing;
                    let field = (1u64 << 16)
                        | ((previous_index as u64) << 9)
                        | (lead_code << 6)
                        | u64::from(sig);
                    w.write_bits(field, FLAG_ONE_SIZE);
                    w.write_bits(xor >> trailing, sig);
                    stored_leading = NO_WINDOW;
                } else if lead_round == stored_leading {
                    // flag 10: reuse the previous leading-zero window.
                    let sig = 64 - lead_round;
                    w.write_bits(0b10, 2);
                    w.write_bits(xor, sig);
                } else {
                    // flag 11: new leading-zero window, 3-bit bucket code.
                    stored_leading = lead_round;
                    let sig = 64 - lead_round;
                    w.write_bits(0b11_000 | lead_code, 5);
                    w.write_bits(xor, sig);
                }
            }

            index += 1;
            stored[index % PREVIOUS_VALUES] = cur;
            indices[key] = index;
        }
        w.finish()
    }

    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError> {
        let mut out = Vec::with_capacity(count);
        if count == 0 {
            return Ok(out);
        }
        let mut r = BitReader::new(bytes);
        let mut stored = [0u64; PREVIOUS_VALUES];
        let mut stored_leading = NO_WINDOW;

        let first = r.read_bits(64)?;
        stored[0] = first;
        out.push(f64::from_bits(first));
        let mut index: usize = 0;

        for _ in 1..count {
            let value = match r.read_bits(2)? {
                0b00 => {
                    let idx = r.read_bits(7)? as usize; // 7 bits: always < 128
                    stored_leading = NO_WINDOW;
                    stored[idx]
                }
                0b01 => {
                    let idx = r.read_bits(7)? as usize;
                    let lead = LEADING_DECODE[r.read_bits(3)? as usize];
                    let sig = r.read_bits(6)? as u32;
                    let trailing = 64u32
                        .checked_sub(lead)
                        .and_then(|v| v.checked_sub(sig))
                        .ok_or(CodecError::Corrupt)?;
                    let payload = r.read_bits(sig)?;
                    let xor = payload.checked_shl(trailing).ok_or(CodecError::Corrupt)?;
                    stored_leading = NO_WINDOW;
                    stored[idx] ^ xor
                }
                0b10 => {
                    if stored_leading > 63 {
                        return Err(CodecError::Corrupt);
                    }
                    let sig = 64 - stored_leading;
                    let xor = r.read_bits(sig)?;
                    stored[index % PREVIOUS_VALUES] ^ xor
                }
                _ => {
                    // 0b11
                    let lead = LEADING_DECODE[r.read_bits(3)? as usize];
                    stored_leading = lead;
                    let sig = 64 - lead;
                    let xor = r.read_bits(sig)?;
                    stored[index % PREVIOUS_VALUES] ^ xor
                }
            };
            index += 1;
            stored[index % PREVIOUS_VALUES] = value;
            out.push(f64::from_bits(value));
        }
        Ok(out)
    }
}

// --- byte-stream-split + zstd -------------------------------------------

/// Byte-stream-split baseline: transpose the eight byte planes of the f64
/// stream, then zstd level 3. See module docs.
pub struct ByteSplitZstd;

const ZSTD_LEVEL: i32 = 3;

impl ValueCodec for ByteSplitZstd {
    fn name(&self) -> &'static str {
        "bytesplit_zstd"
    }

    fn encode(&self, values: &[f64]) -> Vec<u8> {
        let n = values.len();
        // Plane-major: all byte-0s, then all byte-1s, ... byte-7s. Each plane
        // groups the same significance across values, which is what makes the
        // transposed buffer compressible.
        let mut planes = vec![0u8; n * 8];
        for (i, v) in values.iter().enumerate() {
            let b = v.to_bits().to_le_bytes();
            for (j, &byte) in b.iter().enumerate() {
                planes[j * n + i] = byte;
            }
        }
        // zstd::bulk::compress only errors on truly exceptional conditions
        // (allocation); on the realistic path it always succeeds. Fall back to
        // the uncompressed planes framed with a marker byte so encode stays
        // infallible without an unwrap on a library error.
        match zstd::bulk::compress(&planes, ZSTD_LEVEL) {
            Ok(mut compressed) => {
                compressed.insert(0, 1); // 1 = zstd frame follows
                compressed
            }
            Err(_) => {
                let mut out = Vec::with_capacity(planes.len() + 1);
                out.push(0); // 0 = raw planes follow
                out.extend_from_slice(&planes);
                out
            }
        }
    }

    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let plane_bytes = count.checked_mul(8).ok_or(CodecError::Overflow)?;
        let (&marker, body) = bytes.split_first().ok_or(CodecError::Truncated)?;
        let planes = match marker {
            1 => zstd::bulk::decompress(body, plane_bytes).map_err(|_| CodecError::Decompress)?,
            0 => body.to_vec(),
            _ => return Err(CodecError::Corrupt),
        };
        if planes.len() != plane_bytes {
            return Err(CodecError::LengthMismatch {
                expected: plane_bytes,
                actual: planes.len(),
                count,
            });
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let mut b = [0u8; 8];
            for (j, slot) in b.iter_mut().enumerate() {
                *slot = planes[j * count + i];
            }
            out.push(f64::from_bits(u64::from_le_bytes(b)));
        }
        Ok(out)
    }
}

// --- raw f64 baseline ----------------------------------------------------

/// Little-endian `f64` bytes, no compression: RSEG enc 17's payload and the
/// 8-bytes-per-sample floor every codec is measured against.
pub struct RawF64;

impl ValueCodec for RawF64 {
    fn name(&self) -> &'static str {
        "raw_f64"
    }

    fn encode(&self, values: &[f64]) -> Vec<u8> {
        let mut out = Vec::with_capacity(values.len() * 8);
        for v in values {
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        out
    }

    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError> {
        let expected = count.checked_mul(8).ok_or(CodecError::Overflow)?;
        if bytes.len() != expected {
            return Err(CodecError::LengthMismatch {
                expected,
                actual: bytes.len(),
                count,
            });
        }
        let mut out = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(8) {
            let arr: [u8; 8] = chunk.try_into().map_err(|_| CodecError::Truncated)?;
            out.push(f64::from_bits(u64::from_le_bytes(arr)));
        }
        Ok(out)
    }
}

// --- shared integer-model helpers (ALP, GCD-FOR) ------------------------

/// Largest decimal exponent either integer-model codec will try. `10^18`
/// still fits an `i64` (max `~9.22e18`), and larger exponents only lose
/// precision, so a decimal that needs more than 18 places is not modelled and
/// falls back (per value for ALP, per page for GCD-FOR).
const MAX_DECIMAL_EXP: usize = 18;

/// `10^0 ..= 10^18` as exact `f64` (every one is representable: `10^22` is the
/// last exactly-representable power of ten in `f64`).
const POW10: [f64; MAX_DECIMAL_EXP + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

/// The i64 digit `v` maps to under exponent `e`, or `None` when the model does
/// not reproduce `v` bit-exactly: non-finite, out of the i64 domain after
/// scaling, or a round-trip that changes any bit (notably `-0.0`, whose sign
/// the integer path drops).
fn digit_for(v: f64, e: usize) -> Option<i64> {
    let scaled = v * POW10[e];
    // 2^63 as f64; `as i64` saturates but we reject the whole range to stay
    // inside the exactly-invertible domain.
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
/// ties broken toward the smaller exponent (fewer significant digits, smaller
/// integers). Returns the exponent and how many values it fits.
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

fn gcd_u64(a: u64, b: u64) -> u64 {
    let (mut a, mut b) = (a, b);
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

// --- self-contained LEB128 varints + zigzag (no dependency on ravel_codec's
// crate-private varint module) -------------------------------------------

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

fn get_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos).ok_or(CodecError::Truncated)?;
        *pos += 1;
        if shift >= 64 {
            return Err(CodecError::Corrupt);
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

fn get_ivarint(bytes: &[u8], pos: &mut usize) -> Result<i64, CodecError> {
    let u = get_uvarint(bytes, pos)?;
    Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
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
    put_ivarint(out, min);
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

/// Inverse of [`for_pack`] for exactly `count` values, reading from `bytes` at
/// `pos`. A `bit_width > 64` or a packed region shorter than `count * width`
/// bits is a typed error, never a panic.
fn for_unpack(bytes: &[u8], pos: &mut usize, count: usize) -> Result<Vec<i64>, CodecError> {
    let min = get_ivarint(bytes, pos)?;
    let bit_width = u32::from(*bytes.get(*pos).ok_or(CodecError::Truncated)?);
    *pos += 1;
    if bit_width > 64 {
        return Err(CodecError::Corrupt);
    }
    if bit_width == 0 {
        return Ok(vec![min; count]);
    }
    let need_bits = (count as u64)
        .checked_mul(u64::from(bit_width))
        .ok_or(CodecError::Overflow)?;
    let need = usize::try_from(need_bits.div_ceil(8)).map_err(|_| CodecError::Overflow)?;
    let region = bytes.get(*pos..*pos + need).ok_or(CodecError::Truncated)?;
    *pos += need;
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut out = Vec::with_capacity(count.min(1 << 16));
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    let mut byte_idx = 0usize;
    for _ in 0..count {
        while nbits < bit_width {
            acc |= u128::from(region[byte_idx]) << nbits;
            byte_idx += 1;
            nbits += 8;
        }
        let off = (acc as u64) & mask;
        acc >>= bit_width;
        nbits -= bit_width;
        out.push((min as u64).wrapping_add(off) as i64);
    }
    Ok(out)
}

// --- ALP -----------------------------------------------------------------

/// ALP codec (see module docs). Stateless handle.
pub struct Alp;

impl ValueCodec for Alp {
    fn name(&self) -> &'static str {
        "alp"
    }

    fn encode(&self, values: &[f64]) -> Vec<u8> {
        if values.is_empty() {
            return Vec::new();
        }
        let (e, _fits) = best_exponent(values);
        // Digits for fit positions; a placeholder 0 for exceptions, so the
        // integer stream stays length `count` and the exceptions overwrite it.
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
        put_uvarint(&mut out, int_bytes.len() as u64);
        out.extend_from_slice(&int_bytes);
        put_uvarint(&mut out, exceptions.len() as u64);
        for (idx, bits) in &exceptions {
            put_uvarint(&mut out, *idx as u64);
            out.extend_from_slice(&bits.to_le_bytes());
        }
        out
    }

    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut pos = 0usize;
        let e = usize::from(*bytes.get(pos).ok_or(CodecError::Truncated)?);
        pos += 1;
        if e > MAX_DECIMAL_EXP {
            return Err(CodecError::Corrupt);
        }
        let enc_tag = *bytes.get(pos).ok_or(CodecError::Truncated)?;
        pos += 1;
        let enc = Enc::from_u8(enc_tag).map_err(|_| CodecError::Corrupt)?;
        let int_len =
            usize::try_from(get_uvarint(bytes, &mut pos)?).map_err(|_| CodecError::Overflow)?;
        let int_end = pos.checked_add(int_len).ok_or(CodecError::Truncated)?;
        let int_bytes = bytes.get(pos..int_end).ok_or(CodecError::Truncated)?;
        pos += int_len;
        let digits = decode_i64(enc, int_bytes, count).map_err(|_| CodecError::Corrupt)?;
        if digits.len() != count {
            return Err(CodecError::Corrupt);
        }
        let mut out: Vec<f64> = digits.iter().map(|&d| (d as f64) / POW10[e]).collect();
        let n_exc =
            usize::try_from(get_uvarint(bytes, &mut pos)?).map_err(|_| CodecError::Overflow)?;
        for _ in 0..n_exc {
            let idx =
                usize::try_from(get_uvarint(bytes, &mut pos)?).map_err(|_| CodecError::Overflow)?;
            let raw_end = pos.checked_add(8).ok_or(CodecError::Truncated)?;
            let raw = bytes.get(pos..raw_end).ok_or(CodecError::Truncated)?;
            pos += 8;
            let arr: [u8; 8] = raw.try_into().map_err(|_| CodecError::Truncated)?;
            *out.get_mut(idx).ok_or(CodecError::Corrupt)? = f64::from_bits(u64::from_le_bytes(arr));
        }
        Ok(out)
    }
}

// --- GCD-of-deltas + frame-of-reference ---------------------------------

/// GCD-of-deltas then FOR bit-packing (see module docs). Stateless handle.
pub struct GcdDeltaFor;

/// Model marker byte written first: whole-page raw fallback vs the integer
/// model.
const GDF_RAW: u8 = 0;
const GDF_MODEL: u8 = 1;

impl GcdDeltaFor {
    /// Digits under the smallest exponent that fits *every* value, or `None`
    /// when no exponent does (then the page falls back to raw f64).
    fn model_digits(values: &[f64]) -> Option<(usize, Vec<i64>)> {
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
}

impl ValueCodec for GcdDeltaFor {
    fn name(&self) -> &'static str {
        "gcd_delta_for"
    }

    fn encode(&self, values: &[f64]) -> Vec<u8> {
        if values.is_empty() {
            return Vec::new();
        }
        let modelled = GcdDeltaFor::model_digits(values).and_then(|(e, digits)| {
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
            put_ivarint(&mut out, digits[0]);
            put_uvarint(&mut out, g);
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

    fn decode(&self, bytes: &[u8], count: usize) -> Result<Vec<f64>, CodecError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let (&marker, body) = bytes.split_first().ok_or(CodecError::Truncated)?;
        match marker {
            GDF_RAW => {
                let expected = count.checked_mul(8).ok_or(CodecError::Overflow)?;
                if body.len() != expected {
                    return Err(CodecError::LengthMismatch {
                        expected,
                        actual: body.len(),
                        count,
                    });
                }
                let mut out = Vec::with_capacity(count);
                for chunk in body.chunks_exact(8) {
                    let arr: [u8; 8] = chunk.try_into().map_err(|_| CodecError::Truncated)?;
                    out.push(f64::from_bits(u64::from_le_bytes(arr)));
                }
                Ok(out)
            }
            GDF_MODEL => {
                let mut pos = 0usize;
                let e = usize::from(*body.get(pos).ok_or(CodecError::Truncated)?);
                pos += 1;
                if e > MAX_DECIMAL_EXP {
                    return Err(CodecError::Corrupt);
                }
                let first = get_ivarint(body, &mut pos)?;
                let g = get_uvarint(body, &mut pos)?;
                if g == 0 {
                    return Err(CodecError::Corrupt);
                }
                let reduced = for_unpack(body, &mut pos, count - 1)?;
                let g = g as i64;
                let mut digits = Vec::with_capacity(count);
                digits.push(first);
                let mut acc = first;
                for r in reduced {
                    let step = r.checked_mul(g).ok_or(CodecError::Corrupt)?;
                    acc = acc.checked_add(step).ok_or(CodecError::Corrupt)?;
                    digits.push(acc);
                }
                Ok(digits.iter().map(|&d| (d as f64) / POW10[e]).collect())
            }
            _ => Err(CodecError::Corrupt),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn bits(values: &[f64]) -> Vec<u64> {
        values.iter().map(|v| v.to_bits()).collect()
    }

    fn roundtrip<C: ValueCodec>(codec: &C, values: &[f64]) {
        let encoded = codec.encode(values);
        let decoded = codec.decode(&encoded, values.len()).expect("decode");
        assert_eq!(bits(&decoded), bits(values), "codec {}", codec.name());
    }

    fn special_values() -> Vec<f64> {
        vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            f64::NAN,
            -f64::NAN,
            f64::from_bits(0x7FF8_0000_0000_0001), // NaN with payload
            f64::from_bits(0xFFF0_0000_0000_0007), // signalling-ish NaN payload
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            5e-324,  // smallest positive denormal
            -5e-324, // smallest negative denormal
            f64::MAX,
            f64::MIN,
            42.0,
            42.0,
            42.0, // runs (xor == 0)
        ]
    }

    #[test]
    fn all_codecs_roundtrip_special_values() {
        let values = special_values();
        roundtrip(&Alp, &values);
        roundtrip(&GcdDeltaFor, &values);
        roundtrip(&Chimp128, &values);
        roundtrip(&ByteSplitZstd, &values);
        roundtrip(&RawF64, &values);
    }

    #[test]
    fn empty_and_single_roundtrip() {
        for values in [vec![], vec![7.25], vec![f64::NAN]] {
            roundtrip(&Alp, &values);
            roundtrip(&GcdDeltaFor, &values);
            roundtrip(&Chimp128, &values);
            roundtrip(&ByteSplitZstd, &values);
            roundtrip(&RawF64, &values);
        }
    }

    #[test]
    fn integer_and_decimal_streams_roundtrip() {
        // Integer counter, 2-dp and 3-dp decimals: the paths the two integer
        // codecs model rather than fall back on.
        let counter: Vec<f64> = (0..200).map(|i| (i * 3) as f64).collect();
        let dec2: Vec<f64> = (0..200).map(|i| (i as f64) * 0.01 + 3.57).collect();
        let dec3: Vec<f64> = (0..200).map(|i| (i as f64) * 0.001 - 2.501).collect();
        for values in [&counter, &dec2, &dec3] {
            roundtrip(&Alp, values);
            roundtrip(&GcdDeltaFor, values);
        }
    }

    #[test]
    fn alp_negative_zero_is_an_exception() {
        // -0.0 * 10^e rounds to integer 0, which decodes to +0.0; ALP must
        // route it through the exception list to preserve the sign bit.
        let values = vec![1.0, -0.0, 2.0, -0.0, 3.0];
        let encoded = Alp.encode(&values);
        let decoded = Alp.decode(&encoded, values.len()).expect("decode");
        assert_eq!(bits(&decoded), bits(&values));
        assert!(decoded[1].is_sign_negative());
    }

    #[test]
    fn gcd_delta_for_reduces_regular_counter() {
        // A counter stepping by a constant 250 has all deltas == 250; the GCD
        // is 250, reduced deltas are all 1, and the model beats raw f64.
        let values: Vec<f64> = (0..240).map(|i| (i * 250) as f64).collect();
        let encoded = GcdDeltaFor.encode(&values);
        assert!(
            encoded.len() < values.len() * 8,
            "gcd_delta_for must beat raw on a constant-stride counter"
        );
        let decoded = GcdDeltaFor.decode(&encoded, values.len()).expect("decode");
        assert_eq!(bits(&decoded), bits(&values));
    }

    #[test]
    fn chimp_window_reuse_and_backreference() {
        // A pattern that forces every Chimp control path: constant runs
        // (flag 00), a repeating value 128+ apart (indexed back-reference),
        // and small deltas that reuse and then reset the leading window.
        let mut values = Vec::new();
        for i in 0..300u64 {
            values.push(f64::from_bits(0x4000_0000_0000_0000 ^ (i % 7)));
        }
        values[150] = values[10]; // exercises the trailing-bits back-reference
        roundtrip(&Chimp128, &values);
    }

    #[test]
    fn truncation_is_typed_error_never_panic() {
        let values = special_values();
        for codec_encoded in [
            Chimp128.encode(&values),
            ByteSplitZstd.encode(&values),
            RawF64.encode(&values),
            Alp.encode(&values),
            GcdDeltaFor.encode(&values),
        ]
        .into_iter()
        .enumerate()
        {
            let (which, encoded) = codec_encoded;
            for cut in 1..encoded.len().min(24) {
                let truncated = &encoded[..encoded.len() - cut];
                let _ = match which {
                    0 => Chimp128.decode(truncated, values.len()),
                    1 => ByteSplitZstd.decode(truncated, values.len()),
                    2 => RawF64.decode(truncated, values.len()),
                    3 => Alp.decode(truncated, values.len()),
                    _ => GcdDeltaFor.decode(truncated, values.len()),
                };
                // No panic is the assertion; result may be Ok or Err.
            }
        }
    }
}
