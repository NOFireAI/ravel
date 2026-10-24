//! Per-page columnar codecs and the encoding tag registry
//! (docs/log-segment-format.md "Encodings"). The writer measures every
//! applicable codec and keeps the smallest; the tag makes the choice
//! self-describing. Every decoder is untrusted: it validates against the
//! caller-supplied element count, consumes exactly its bytes, and returns a
//! typed [`CodecError`] on any violation, never panicking.

use crate::error::CodecError;
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint, zigzag_decode};

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
    /// The GCD-scaled integer codec. Implemented in ravel-logseg (ADR-2135
    /// decision 3). No encoder in this crate emits it and every decoder
    /// refuses it.
    GcdI64 = 10,
    /// The column-reference codec. Implemented in ravel-logseg (ADR-2135
    /// decision 3). No encoder in this crate emits it and every decoder
    /// refuses it.
    ColumnRef = 11,
    /// The row-group dictionary page. Implemented in ravel-logseg (ADR-2135
    /// decision 6). No encoder in this crate emits it and every decoder
    /// refuses it.
    DictPage = 12,
    /// The ids page of a row-group dictionary. Implemented in ravel-logseg
    /// (ADR-2135 decision 6). No encoder in this crate emits it and every
    /// decoder refuses it.
    DictIds = 13,
}

impl Enc {
    /// Maps a stored tag byte to an [`Enc`]; an unknown byte is `Corrupted`.
    pub fn from_u8(v: u8) -> Result<Enc, CodecError> {
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
            10 => Enc::GcdI64,
            11 => Enc::ColumnRef,
            12 => Enc::DictPage,
            13 => Enc::DictIds,
            other => return Err(CodecError::Corrupted(format!("unknown enc tag {other}"))),
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
fn expect_consumed(pos: usize, len: usize) -> Result<(), CodecError> {
    if pos == len {
        Ok(())
    } else {
        Err(CodecError::Corrupted(format!(
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
) -> Result<Vec<u64>, CodecError> {
    let mut out = Vec::with_capacity(cap(count));
    unpack_bits_each(buf, count, bit_width, &mut |v| {
        out.push(v);
        Ok(())
    })?;
    Ok(out)
}

/// [`unpack_bits`], handing each value to `emit` in order instead of
/// collecting them, so a caller can store them wherever they belong.
fn unpack_bits_each(
    buf: &[u8],
    count: usize,
    bit_width: u32,
    emit: &mut impl FnMut(u64) -> Result<(), CodecError>,
) -> Result<(), CodecError> {
    if bit_width > 64 {
        return Err(CodecError::Corrupted(format!("bit_width {bit_width} > 64")));
    }
    if bit_width == 0 {
        if !buf.is_empty() {
            return Err(CodecError::Corrupted("bit_width 0 with payload".into()));
        }
        for _ in 0..count {
            emit(0)?;
        }
        return Ok(());
    }
    let need_bits = (count as u64)
        .checked_mul(u64::from(bit_width))
        .ok_or_else(|| CodecError::Corrupted("packed length overflow".into()))?;
    let need = need_bits.div_ceil(8);
    if buf.len() as u64 != need {
        return Err(CodecError::Corrupted(format!(
            "packed length {} != {need}",
            buf.len()
        )));
    }
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    let mut idx = 0usize;
    for _ in 0..count {
        while nbits < bit_width {
            acc |= u128::from(buf[idx]) << nbits;
            idx += 1;
            nbits += 8;
        }
        emit((acc & u128::from(mask)) as u64)?;
        acc >>= bit_width;
        nbits -= bit_width;
    }
    Ok(())
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
pub fn decode_i64(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<i64>, CodecError> {
    let mut identity = Ok;
    match enc {
        Enc::Constant => {
            let mut pos = 0usize;
            let v = get_ivarint(bytes, &mut pos)?;
            expect_consumed(pos, bytes.len())?;
            Ok(vec![v; count])
        }
        Enc::Rle => {
            let mut out = Vec::with_capacity(cap(count));
            let pos = decode_rle(bytes, count, |v, run| {
                out.resize(out.len() + run, v);
                Ok(())
            })?;
            expect_consumed(pos, bytes.len())?;
            Ok(out)
        }
        Enc::ForBitpack => {
            let (min, bit_width, payload) = for_header(bytes, count)?;
            let mut out = vec![0; count];
            unpack_for(payload, bit_width, min, &mut out);
            Ok(out)
        }
        Enc::Plain | Enc::DeltaZigzag | Enc::DoubleDelta if count > bytes.len() => {
            // Every value takes at least one byte, so decoding stops at a
            // varint error no later than the value after the last byte.
            let mut out = vec![0; bytes.len()];
            let mut pos = decode_varints(enc, bytes, &mut out, &mut identity)?;
            get_ivarint(bytes, &mut pos)?;
            Err(CodecError::Corrupted("varint truncated".into()))
        }
        Enc::Plain | Enc::DeltaZigzag | Enc::DoubleDelta => {
            let mut out = vec![0; count];
            let pos = decode_varints(enc, bytes, &mut out, &mut identity)?;
            expect_consumed(pos, bytes.len())?;
            Ok(out)
        }
        other => Err(not_integer(other)),
    }
}

/// Decodes the present values of an i64 page straight into a block-length
/// buffer.
///
/// `out` holds one slot per block row. `presence` is the column's LSB-first
/// presence bitmap (`None` when every row is present); it must be exactly
/// `ceil(out.len() / 8)` bytes. The page carries one value per present row;
/// each decoded value passes through `map` and is stored at the next present
/// row. Absent rows are written as 0. Returns the number of present rows.
///
/// `map` is applied once per decoded value, except on Constant and RLE pages,
/// where it is applied once per run, so it must be a pure function of its
/// input. When a decode error follows values that `map` rejects, the first
/// rejection is returned, as it would be value by value.
pub fn decode_i64_into(
    enc: Enc,
    bytes: &[u8],
    presence: Option<&[u8]>,
    out: &mut [i64],
    mut map: impl FnMut(i64) -> Result<i64, CodecError>,
) -> Result<usize, CodecError> {
    let rows = out.len();
    let Some(bits) = presence else {
        decode_dense(enc, bytes, out, &mut map)?;
        return Ok(rows);
    };
    let count = count_present(bits, rows)?;
    decode_dense(enc, bytes, &mut out[rows - count..], &mut map)?;
    scatter_present(bits, out, count);
    Ok(count)
}

/// Number of set bits among the first `rows` bits of an LSB-first bitmap,
/// which must be exactly `ceil(rows / 8)` bytes long.
pub fn count_present(bits: &[u8], rows: usize) -> Result<usize, CodecError> {
    if bits.len() != rows.div_ceil(8) {
        return Err(CodecError::Corrupted(format!(
            "bitmap length {} != {}",
            bits.len(),
            rows.div_ceil(8)
        )));
    }
    let full = rows / 8;
    let words = bits[..full].chunks_exact(8);
    let rest = words.remainder();
    let mut n: usize = words
        .map(|w| {
            let mut word = [0u8; 8];
            word.copy_from_slice(w);
            u64::from_le_bytes(word).count_ones() as usize
        })
        .sum();
    n += rest.iter().map(|b| b.count_ones() as usize).sum::<usize>();
    let tail = rows % 8;
    if tail > 0 {
        n += (bits[full] & ((1u8 << tail) - 1)).count_ones() as usize;
    }
    Ok(n)
}

fn not_integer(enc: Enc) -> CodecError {
    CodecError::Corrupted(format!("enc {enc:?} is not an integer codec"))
}

/// Moves the `count` dense values at the back of `out` to the rows whose
/// presence bit is set, in order, and writes 0 to every other row. `count`
/// must be the number of set bits among the first `out.len()` bits.
fn scatter_present(bits: &[u8], out: &mut [i64], count: usize) {
    let rows = out.len();
    // Walking front to back, the next value to place sits at or after the
    // row it goes to, and after every row of the current byte that is
    // absent, so nothing is overwritten before it is read.
    let mut src = rows - count;
    for (byte_idx, &stored) in bits.iter().enumerate() {
        let base = byte_idx * 8;
        if src == base {
            // No absent row is left, so every remaining value is in place.
            return;
        }
        let width = (rows - base).min(8);
        let byte = stored & (0xffu16 >> (8 - width)) as u8;
        if byte == 0 {
            out[base..base + width].fill(0);
            continue;
        }
        if width == 8 && src + 8 <= rows {
            // Take the byte's values, and the slots after them, before
            // writing any of its rows, then give each row the value of its
            // rank among the present rows.
            let mut vals = [0i64; 8];
            vals.copy_from_slice(&out[src..src + 8]);
            let mut rank = 0usize;
            for (bit, slot) in out[base..base + 8].iter_mut().enumerate() {
                let present = (byte >> bit) & 1;
                *slot = vals[rank] & i64::from(present).wrapping_neg();
                rank += usize::from(present);
            }
            src += rank;
            continue;
        }
        for bit in 0..width {
            if byte & (1 << bit) != 0 {
                out[base + bit] = out[src];
                src += 1;
            } else {
                out[base + bit] = 0;
            }
        }
    }
}

/// Decodes exactly `out.len()` values into `out`, each passed through `map`,
/// and checks the page was consumed exactly.
fn decode_dense(
    enc: Enc,
    bytes: &[u8],
    out: &mut [i64],
    map: &mut impl FnMut(i64) -> Result<i64, CodecError>,
) -> Result<(), CodecError> {
    let pos = match enc {
        Enc::Constant => {
            let mut pos = 0usize;
            let v = get_ivarint(bytes, &mut pos)?;
            if !out.is_empty() {
                out.fill(map(v)?);
            }
            pos
        }
        Enc::Rle => {
            let mut start = 0usize;
            decode_rle(bytes, out.len(), |v, run| {
                let m = map(v)?;
                out[start..start + run].fill(m);
                start += run;
                Ok(())
            })?
        }
        Enc::ForBitpack => {
            let (min, bit_width, payload) = for_header(bytes, out.len())?;
            unpack_for(payload, bit_width, min, out);
            map_all(out, map)?;
            bytes.len()
        }
        Enc::Plain | Enc::DeltaZigzag | Enc::DoubleDelta => decode_varints(enc, bytes, out, map)?,
        other => return Err(not_integer(other)),
    };
    expect_consumed(pos, bytes.len())
}

fn map_all(
    out: &mut [i64],
    map: &mut impl FnMut(i64) -> Result<i64, CodecError>,
) -> Result<(), CodecError> {
    for v in out.iter_mut() {
        *v = map(*v)?;
    }
    Ok(())
}

/// Reads the runs of an RLE page and hands each non-empty `(value, length)`
/// to `fill` in order; the lengths sum to `count`. Returns the position after
/// the last run.
fn decode_rle(
    bytes: &[u8],
    count: usize,
    mut fill: impl FnMut(i64, usize) -> Result<(), CodecError>,
) -> Result<usize, CodecError> {
    let mut pos = 0usize;
    let run_count = get_uvarint(bytes, &mut pos)?;
    let mut total: u64 = 0;
    for _ in 0..run_count {
        let v = get_ivarint(bytes, &mut pos)?;
        let run = get_uvarint(bytes, &mut pos)?;
        total = total
            .checked_add(run)
            .ok_or_else(|| CodecError::Corrupted("rle run overflow".into()))?;
        if total > count as u64 {
            return Err(CodecError::Corrupted("rle exceeds count".into()));
        }
        if run > 0 {
            // `run <= total <= count`, so it fits in usize.
            fill(v, run as usize)?;
        }
    }
    if total != count as u64 {
        return Err(CodecError::Corrupted("rle short of count".into()));
    }
    Ok(pos)
}

/// Reads one zigzag varint, taking the one-byte form inline and leaving
/// everything else, errors included, to [`get_ivarint`].
#[inline]
fn read_ivarint(bytes: &[u8], pos: &mut usize) -> Result<i64, CodecError> {
    match bytes.get(*pos) {
        Some(&b) if b < 0x80 => {
            *pos += 1;
            Ok(zigzag_decode(u64::from(b)))
        }
        _ => get_ivarint(bytes, pos),
    }
}

/// Decodes `out.len()` values of a Plain, DeltaZigzag or DoubleDelta page
/// into `out`, each passed through `map`, and returns the position after the
/// last one. On a decode error, the values before it are mapped first, so a
/// rejection by `map` wins as it would value by value.
fn decode_varints(
    enc: Enc,
    bytes: &[u8],
    out: &mut [i64],
    map: &mut impl FnMut(i64) -> Result<i64, CodecError>,
) -> Result<usize, CodecError> {
    // All three carry one varint per value; the deltas are summed in a second
    // pass. An overflow in the sums comes before the first unreadable varint,
    // since only the varints read before it are summed.
    let mut pos = 0usize;
    let read = varints_into(bytes, &mut pos, out);
    let done = match &read {
        Ok(()) => out.len(),
        Err((done, _)) => *done,
    };
    let summed = match enc {
        Enc::DeltaZigzag => delta_sums(&mut out[..done]),
        Enc::DoubleDelta => double_delta_sums(&mut out[..done]),
        _ => Ok(()),
    };
    if let Err((valid, e)) = summed.and(read) {
        map_all(&mut out[..valid], map)?;
        return Err(e);
    }
    map_all(out, map)?;
    Ok(pos)
}

/// The decoders below fill `out` from the front and report a failure as the
/// number of leading values that are final and the error.
type Partial = Result<(), (usize, CodecError)>;

const STOP_BITS: u64 = 0x8080_8080_8080_8080;

/// Reads `out.len()` zigzag varints into `out`. Eight bytes are loaded at a
/// time and every canonical varint that ends inside them is decoded from that
/// load, found through the bytes without a continuation bit; a varint that
/// does not fit in one load, a non-canonical one, and the last bytes of the
/// page go through [`get_ivarint`].
fn varints_into(bytes: &[u8], pos: &mut usize, out: &mut [i64]) -> Partial {
    let n = out.len();
    let mut i = 0usize;
    while i < n {
        let Some(Ok(window)) = bytes.get(*pos..*pos + 8).map(<[u8; 8]>::try_from) else {
            break;
        };
        let w = u64::from_le_bytes(window);
        if w & STOP_BITS == 0 && n - i >= 8 {
            for (slot, &b) in out[i..i + 8].iter_mut().zip(&window) {
                *slot = zigzag_decode(u64::from(b));
            }
            i += 8;
            *pos += 8;
            continue;
        }
        let mut stops = !w & STOP_BITS;
        // Bit offset in the window of the next varint.
        let mut start = 0u32;
        while stops != 0 && i < n {
            let end = stops.trailing_zeros() + 1;
            let bits = end - start;
            let x = (w >> start) & (u64::MAX >> (64 - bits));
            // A zero last byte after the first is non-canonical.
            if bits > 8 && x >> (bits - 8) == 0 {
                break;
            }
            let v = if bits <= 16 {
                (x & 0x7f) | ((x >> 1) & 0x3f80)
            } else {
                compact_varint(x)
            };
            out[i] = zigzag_decode(v);
            i += 1;
            start = end;
            stops &= stops - 1;
        }
        *pos += (start / 8) as usize;
        if start == 0 {
            out[i] = get_ivarint(bytes, pos).map_err(|e| (i, e))?;
            i += 1;
        }
    }
    while i < n {
        out[i] = read_ivarint(bytes, pos).map_err(|e| (i, e))?;
        i += 1;
    }
    Ok(())
}

/// Joins the 7-bit groups of a varint of up to eight bytes, LSB-first,
/// pairing neighbours: bytes into 14-bit, then 28-bit, then 56-bit groups.
#[inline]
fn compact_varint(x: u64) -> u64 {
    let x = (x & 0x007f_007f_007f_007f) | ((x & 0x7f00_7f00_7f00_7f00) >> 1);
    let x = (x & 0x0000_3fff_0000_3fff) | ((x & 0x3fff_0000_3fff_0000) >> 2);
    (x & 0x0000_0000_0fff_ffff) | ((x & 0x0fff_ffff_0000_0000) >> 4)
}

/// Turns a first value and its deltas into values, in place.
fn delta_sums(out: &mut [i64]) -> Partial {
    for i in 1..out.len() {
        out[i] = out[i - 1]
            .checked_add(out[i])
            .ok_or_else(|| (i, CodecError::Corrupted("delta overflow".into())))?;
    }
    Ok(())
}

/// Turns a first value, a first delta and the deltas of deltas into values,
/// in place.
fn double_delta_sums(out: &mut [i64]) -> Partial {
    let overflow = |i| (i, CodecError::Corrupted("double-delta overflow".into()));
    if out.len() < 2 {
        return Ok(());
    }
    let mut prev_delta = out[1];
    let mut value = out[0].checked_add(prev_delta).ok_or_else(|| overflow(1))?;
    out[1] = value;
    for (i, slot) in out.iter_mut().enumerate().skip(2) {
        prev_delta = prev_delta.checked_add(*slot).ok_or_else(|| overflow(i))?;
        value = value.checked_add(prev_delta).ok_or_else(|| overflow(i))?;
        *slot = value;
    }
    Ok(())
}

/// Parses and validates a ForBitpack page header for `count` values:
/// `(min, bit_width, packed payload)`. Every length check happens here, so
/// [`unpack_for`] cannot fail.
fn for_header(bytes: &[u8], count: usize) -> Result<(i64, u32, &[u8]), CodecError> {
    let mut pos = 0usize;
    let min = get_ivarint(bytes, &mut pos)?;
    let bit_width = u32::from(
        *bytes
            .get(pos)
            .ok_or_else(|| CodecError::Corrupted("for truncated".into()))?,
    );
    pos += 1;
    let payload = &bytes[pos..];
    check_packed_len(payload, count, bit_width)?;
    Ok((min, bit_width, payload))
}

/// The length checks of [`unpack_bits_each`], in its order and with its
/// messages.
fn check_packed_len(buf: &[u8], count: usize, bit_width: u32) -> Result<(), CodecError> {
    if bit_width > 64 {
        return Err(CodecError::Corrupted(format!("bit_width {bit_width} > 64")));
    }
    if bit_width == 0 {
        if !buf.is_empty() {
            return Err(CodecError::Corrupted("bit_width 0 with payload".into()));
        }
        return Ok(());
    }
    let need_bits = (count as u64)
        .checked_mul(u64::from(bit_width))
        .ok_or_else(|| CodecError::Corrupted("packed length overflow".into()))?;
    let need = need_bits.div_ceil(8);
    if buf.len() as u64 != need {
        return Err(CodecError::Corrupted(format!(
            "packed length {} != {need}",
            buf.len()
        )));
    }
    Ok(())
}

/// Unpacks `out.len()` offsets of `bit_width` bits from `buf` (already
/// length-checked by [`check_packed_len`]) and stores `min + offset` in each
/// slot, wrapping.
fn unpack_for(buf: &[u8], bit_width: u32, min: i64, out: &mut [i64]) {
    let base = min as u64;
    if bit_width == 0 {
        out.fill(min);
        return;
    }
    let w = u64::from(bit_width);
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut i = 0usize;
    if bit_width <= 56 {
        // A value starts at most 7 bits into its first byte, so one
        // little-endian 8-byte window holds all of it.
        while i < out.len() {
            let off = i as u64 * w;
            let byte = (off / 8) as usize;
            let Some(Ok(window)) = buf.get(byte..byte + 8).map(<[u8; 8]>::try_from) else {
                break;
            };
            let o = (u64::from_le_bytes(window) >> (off % 8)) & mask;
            out[i] = base.wrapping_add(o) as i64;
            i += 1;
        }
    }
    // The last few values, and every value wider than 56 bits, read up to
    // nine bytes into a u128.
    for (j, slot) in out.iter_mut().enumerate().skip(i) {
        let off = j as u64 * w;
        let byte = (off / 8) as usize;
        let mut acc: u128 = 0;
        for (k, &b) in buf[byte..].iter().take(9).enumerate() {
            acc |= u128::from(b) << (8 * k);
        }
        let o = ((acc >> (off % 8)) as u64) & mask;
        *slot = base.wrapping_add(o) as i64;
    }
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
pub fn decode_bitmap(bytes: &[u8], count: usize) -> Result<Vec<bool>, CodecError> {
    if bytes.len() != count.div_ceil(8) {
        return Err(CodecError::Corrupted(format!(
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

// ---------------------------------------------------------------------------
// Dictionary id bodies, shared by the string, f64-bits, and fixed codecs.
// A body is `bit_width: u8` then `count` ids FOR-bit-packed LSB-first.
// ---------------------------------------------------------------------------

fn encode_dict_ids(out: &mut Vec<u8>, ids: &[u64], dict_count: usize) {
    let bit_width = width_for((dict_count.saturating_sub(1)) as u64);
    out.push(bit_width as u8);
    pack_bits(out, ids, bit_width);
}

/// Reads a dict id body starting at `*pos` and running to the end of `buf`
/// (ids are always the trailing part of a dictionary payload). Validates
/// every id is `< dict_count`.
fn decode_dict_ids(
    buf: &[u8],
    pos: &mut usize,
    count: usize,
    dict_count: usize,
) -> Result<Vec<u64>, CodecError> {
    let bit_width = u32::from(
        *buf.get(*pos)
            .ok_or_else(|| CodecError::Corrupted("dict ids truncated".into()))?,
    );
    *pos += 1;
    let ids = unpack_bits(&buf[*pos..], count, bit_width)?;
    *pos = buf.len();
    for &id in &ids {
        if id >= dict_count as u64 {
            return Err(CodecError::Corrupted(format!(
                "dict id {id} >= dict_count {dict_count}"
            )));
        }
    }
    Ok(ids)
}

/// Distinct-ratio dictionary heuristic: dictionary-encode when at most half
/// the values are distinct.
fn dict_is_worth_it(distinct: usize, total: usize) -> bool {
    total > 0 && distinct.saturating_mul(2) <= total
}

// ---------------------------------------------------------------------------
// String codecs
// ---------------------------------------------------------------------------

fn enc_plain_strings(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        put_uvarint(&mut out, v.len() as u64);
    }
    for v in values {
        out.extend_from_slice(v);
    }
    out
}

fn enc_dict_strings(values: &[&[u8]], sorted: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, sorted.len() as u64);
    for entry in sorted {
        put_uvarint(&mut out, entry.len() as u64);
        out.extend_from_slice(entry);
    }
    let ids: Vec<u64> = values
        .iter()
        .map(|v| sorted.partition_point(|e| e < v) as u64)
        .collect();
    encode_dict_ids(&mut out, &ids, sorted.len());
    out
}

/// Encodes byte-string values. Uses a sorted-dictionary page when at most
/// half the values are distinct, else a plain lengths+blob page.
pub fn encode_strings(values: &[&[u8]]) -> (Enc, Vec<u8>) {
    if values.is_empty() {
        return (Enc::Plain, Vec::new());
    }
    let mut sorted: Vec<&[u8]> = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if dict_is_worth_it(sorted.len(), values.len()) {
        (Enc::Dict, enc_dict_strings(values, &sorted))
    } else {
        (Enc::Plain, enc_plain_strings(values))
    }
}

/// Encodes a string column already in dictionary shape. `present_ids[i]` is
/// the dictionary index of present row `i` into `dict`, the column's distinct
/// values (in any order); `dict` may hold entries no id references. Produces
/// bytes byte-identical to `encode_strings(&values)` where
/// `values[i] == dict[present_ids[i] as usize]`, but derives the
/// dictionary-versus-plain choice and the sorted entry set from the referenced
/// distinct values alone, never sorting or scanning the per-row values.
///
/// A dictionary-shaped columnar producer (ADR-0109 decision 3) calls this once
/// per block so string encoding costs one sort per distinct value, not the
/// `encode_strings` sort-plus-`partition_point` per row.
pub fn encode_strings_dict(dict: &[&[u8]], present_ids: &[u32]) -> (Enc, Vec<u8>) {
    if present_ids.is_empty() {
        return (Enc::Plain, Vec::new());
    }
    // One pass over the per-row ids (integers) to mark the referenced entries;
    // the sort below runs over those distinct values, never the per-row values.
    let mut referenced = vec![false; dict.len()];
    let mut distinct: Vec<&[u8]> = Vec::new();
    for &id in present_ids {
        let i = id as usize;
        if !referenced[i] {
            referenced[i] = true;
            distinct.push(dict[i]);
        }
    }
    distinct.sort_unstable();
    distinct.dedup();

    if dict_is_worth_it(distinct.len(), present_ids.len()) {
        // Map each referenced dictionary index to its position in the sorted
        // distinct set once (per distinct), so the per-row id is an integer
        // lookup rather than a `partition_point` string search.
        let mut pos_of = vec![0u32; dict.len()];
        for (i, seen) in referenced.iter().enumerate() {
            if *seen {
                pos_of[i] = distinct.partition_point(|e| *e < dict[i]) as u32;
            }
        }
        let mut out = Vec::new();
        put_uvarint(&mut out, distinct.len() as u64);
        for entry in &distinct {
            put_uvarint(&mut out, entry.len() as u64);
            out.extend_from_slice(entry);
        }
        let ids: Vec<u64> = present_ids
            .iter()
            .map(|&id| u64::from(pos_of[id as usize]))
            .collect();
        encode_dict_ids(&mut out, &ids, distinct.len());
        (Enc::Dict, out)
    } else {
        let values: Vec<&[u8]> = present_ids.iter().map(|&id| dict[id as usize]).collect();
        (Enc::Plain, enc_plain_strings(&values))
    }
}

/// The non-fusing decode of a string column: either the plain per-value form,
/// or the dictionary form kept intact (the distinct values plus one id per
/// value). A dictionary page decoded this way keeps its structure instead of
/// materializing one owned `Vec<u8>` per row, which a columnar consumer
/// (ADR-0099 decision 4) turns straight into an Arrow dictionary array.
///
/// The `ids` are per value in row order, before any presence bitmap is applied:
/// each indexes `dict`, and every id is validated `< dict.len()` at decode.
pub enum DecodedStrings {
    Plain(Vec<Vec<u8>>),
    Dict { dict: Vec<Vec<u8>>, ids: Vec<u32> },
}

impl DecodedStrings {
    /// Fuses the dictionary form back to one owned value per row; the plain form
    /// is returned as-is. This is exactly what [`decode_strings`] returns, and
    /// it is the reason `decode_strings` can be a thin wrapper: the fused output
    /// is byte-for-byte the pre-ADR-0099 result, repeated entries and all.
    pub fn fuse(self) -> Vec<Vec<u8>> {
        match self {
            DecodedStrings::Plain(v) => v,
            DecodedStrings::Dict { dict, ids } => ids
                .into_iter()
                .map(|id| dict[id as usize].clone())
                .collect(),
        }
    }
}

/// Decodes `count` byte-string values encoded with `enc` without fusing a
/// dictionary page: a [`Enc::Dict`] page returns its distinct values and ids
/// intact ([`DecodedStrings::Dict`]), a [`Enc::Plain`] page the per-value form
/// ([`DecodedStrings::Plain`]). Every corruption guard [`decode_strings`]
/// applies fires here too; both entry points share this body.
pub fn decode_strings_columnar(
    enc: Enc,
    bytes: &[u8],
    count: usize,
) -> Result<DecodedStrings, CodecError> {
    let mut pos = 0usize;
    let out = match enc {
        Enc::Plain => {
            let mut lens = Vec::with_capacity(cap(count));
            let mut total: u64 = 0;
            for _ in 0..count {
                let len = get_uvarint(bytes, &mut pos)?;
                total = total
                    .checked_add(len)
                    .ok_or_else(|| CodecError::Corrupted("string length overflow".into()))?;
                lens.push(len);
            }
            let blob = &bytes[pos..];
            if blob.len() as u64 != total {
                return Err(CodecError::Corrupted(format!(
                    "string blob {} != declared {total}",
                    blob.len()
                )));
            }
            let mut out = Vec::with_capacity(cap(count));
            let mut off = 0usize;
            for len in lens {
                let len = len as usize;
                out.push(blob[off..off + len].to_vec());
                off += len;
            }
            pos = bytes.len();
            DecodedStrings::Plain(out)
        }
        Enc::Dict => {
            let dict_count = get_uvarint(bytes, &mut pos)? as usize;
            if dict_count > cap(bytes.len() + 1) {
                return Err(CodecError::Corrupted("dict_count too large".into()));
            }
            let mut dict: Vec<Vec<u8>> = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let len = get_uvarint(bytes, &mut pos)? as usize;
                let entry = bytes
                    .get(pos..pos + len)
                    .ok_or_else(|| CodecError::Corrupted("dict entry truncated".into()))?;
                dict.push(entry.to_vec());
                pos += len;
            }
            // `decode_dict_ids` validates every id is `< dict_count`, and
            // `dict_count` is bounded by `cap` (<= 1 << 16), so each id fits a
            // u32 losslessly.
            let ids = decode_dict_ids(bytes, &mut pos, count, dict_count)?;
            let ids: Vec<u32> = ids.into_iter().map(|id| id as u32).collect();
            DecodedStrings::Dict { dict, ids }
        }
        other => {
            return Err(CodecError::Corrupted(format!(
                "enc {other:?} is not a string codec"
            )));
        }
    };
    expect_consumed(pos, bytes.len())?;
    Ok(out)
}

/// Decodes `count` byte-string values encoded with `enc`, fusing a dictionary
/// page to one owned value per row. A thin wrapper over
/// [`decode_strings_columnar`] kept byte-for-byte identical in signature and
/// output so every existing caller is untouched.
pub fn decode_strings(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<Vec<u8>>, CodecError> {
    Ok(decode_strings_columnar(enc, bytes, count)?.fuse())
}

// ---------------------------------------------------------------------------
// f64-bits codecs (callers pass f64::to_bits so NaN payloads and -0.0 are
// significant and survive round-trips exactly).
// ---------------------------------------------------------------------------

/// Encodes f64 values as their `u64` bit patterns. Constant when all equal,
/// else a sorted dictionary when at most half are distinct, else plain LE.
pub fn encode_f64(values: &[u64]) -> (Enc, Vec<u8>) {
    if values.is_empty() {
        return (Enc::Plain, Vec::new());
    }
    if values.iter().all(|&v| v == values[0]) {
        return (Enc::Constant, values[0].to_le_bytes().to_vec());
    }
    let mut sorted: Vec<u64> = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if dict_is_worth_it(sorted.len(), values.len()) {
        let mut out = Vec::new();
        put_uvarint(&mut out, sorted.len() as u64);
        for &v in &sorted {
            out.extend_from_slice(&v.to_le_bytes());
        }
        let ids: Vec<u64> = values
            .iter()
            .map(|v| sorted.partition_point(|e| e < v) as u64)
            .collect();
        encode_dict_ids(&mut out, &ids, sorted.len());
        (Enc::Dict, out)
    } else {
        let mut out = Vec::with_capacity(values.len() * 8);
        for &v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        (Enc::Plain, out)
    }
}

fn read_u64_le(bytes: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let slice = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| CodecError::Corrupted("f64 bits truncated".into()))?;
    let mut a = [0u8; 8];
    a.copy_from_slice(slice);
    *pos += 8;
    Ok(u64::from_le_bytes(a))
}

/// Decodes `count` f64 bit patterns encoded with `enc`.
pub fn decode_f64(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<u64>, CodecError> {
    let mut pos = 0usize;
    let out = match enc {
        Enc::Plain => {
            let mut out = Vec::with_capacity(cap(count));
            for _ in 0..count {
                out.push(read_u64_le(bytes, &mut pos)?);
            }
            out
        }
        Enc::Constant => {
            let v = read_u64_le(bytes, &mut pos)?;
            vec![v; count]
        }
        Enc::Dict => {
            let dict_count = get_uvarint(bytes, &mut pos)? as usize;
            if dict_count > cap(bytes.len() + 1) {
                return Err(CodecError::Corrupted("dict_count too large".into()));
            }
            let mut dict = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                dict.push(read_u64_le(bytes, &mut pos)?);
            }
            let ids = decode_dict_ids(bytes, &mut pos, count, dict_count)?;
            ids.into_iter().map(|id| dict[id as usize]).collect()
        }
        other => {
            return Err(CodecError::Corrupted(format!(
                "enc {other:?} is not an f64 codec"
            )));
        }
    };
    expect_consumed(pos, bytes.len())?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Fixed-width codecs (trace_id 16B, span_id 8B)
// ---------------------------------------------------------------------------

/// Encodes fixed-`width` values. Uses a sorted dictionary when at most half
/// are distinct, else a raw concatenation. Callers guarantee every value is
/// exactly `width` bytes.
pub fn encode_fixed(values: &[&[u8]], width: usize) -> (Enc, Vec<u8>) {
    if values.is_empty() {
        return (Enc::FixedWidth, Vec::new());
    }
    let mut sorted: Vec<&[u8]> = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if dict_is_worth_it(sorted.len(), values.len()) {
        let mut out = Vec::new();
        put_uvarint(&mut out, sorted.len() as u64);
        for entry in &sorted {
            out.extend_from_slice(entry);
        }
        let ids: Vec<u64> = values
            .iter()
            .map(|v| sorted.partition_point(|e| e < v) as u64)
            .collect();
        encode_dict_ids(&mut out, &ids, sorted.len());
        (Enc::Dict, out)
    } else {
        let mut out = Vec::with_capacity(values.len() * width);
        for v in values {
            out.extend_from_slice(v);
        }
        (Enc::FixedWidth, out)
    }
}

/// Decodes `count` fixed-`width` values encoded with `enc`.
pub fn decode_fixed(
    enc: Enc,
    bytes: &[u8],
    count: usize,
    width: usize,
) -> Result<Vec<Vec<u8>>, CodecError> {
    let mut pos = 0usize;
    let out = match enc {
        Enc::FixedWidth => {
            let need = count
                .checked_mul(width)
                .ok_or_else(|| CodecError::Corrupted("fixed length overflow".into()))?;
            if bytes.len() != need {
                return Err(CodecError::Corrupted(format!(
                    "fixed length {} != {need}",
                    bytes.len()
                )));
            }
            let mut out = Vec::with_capacity(cap(count));
            for i in 0..count {
                out.push(bytes[i * width..i * width + width].to_vec());
            }
            pos = bytes.len();
            out
        }
        Enc::Dict => {
            let dict_count = get_uvarint(bytes, &mut pos)? as usize;
            if dict_count > cap(bytes.len() + 1) {
                return Err(CodecError::Corrupted("dict_count too large".into()));
            }
            let mut dict: Vec<Vec<u8>> = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                let entry = bytes
                    .get(pos..pos + width)
                    .ok_or_else(|| CodecError::Corrupted("fixed dict entry truncated".into()))?;
                dict.push(entry.to_vec());
                pos += width;
            }
            let ids = decode_dict_ids(bytes, &mut pos, count, dict_count)?;
            ids.into_iter()
                .map(|id| dict[id as usize].clone())
                .collect()
        }
        other => {
            return Err(CodecError::Corrupted(format!(
                "enc {other:?} is not a fixed-width codec"
            )));
        }
    };
    expect_consumed(pos, bytes.len())?;
    Ok(out)
}

/// The value-at-a-time integer decode this module shipped before the bulk
/// decode (issue #3235), kept verbatim as the oracle the bulk decode is
/// tested against.
#[cfg(test)]
mod oracle {
    use super::*;

    /// Decodes `count` i64 values encoded with `enc`.
    pub fn decode_i64(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<i64>, CodecError> {
        let mut out = Vec::with_capacity(cap(count));
        decode_i64_each(enc, bytes, count, &mut |v| {
            out.push(v);
            Ok(())
        })?;
        Ok(out)
    }

    /// Decodes the present values of an i64 page straight into a block-length
    /// buffer, in the same pass that reads the varints.
    ///
    /// `out` holds one slot per block row. `presence` is the column's LSB-first
    /// presence bitmap (`None` when every row is present); it must be exactly
    /// `ceil(out.len() / 8)` bytes. The page carries one value per present row;
    /// each decoded value passes through `map` and is stored at the next present
    /// row. Absent rows are written as 0. Returns the number of present rows.
    pub fn decode_i64_into(
        enc: Enc,
        bytes: &[u8],
        presence: Option<&[u8]>,
        out: &mut [i64],
        mut map: impl FnMut(i64) -> Result<i64, CodecError>,
    ) -> Result<usize, CodecError> {
        let rows = out.len();
        let Some(bits) = presence else {
            let mut row = 0usize;
            decode_i64_each(enc, bytes, rows, &mut |v| {
                let slot = out
                    .get_mut(row)
                    .ok_or_else(|| CodecError::Corrupted("value page longer than block".into()))?;
                *slot = map(v)?;
                row += 1;
                Ok(())
            })?;
            return Ok(rows);
        };
        let count = count_present(bits, rows)?;
        let mut row = 0usize;
        decode_i64_each(enc, bytes, count, &mut |v| {
            while row < rows && !bit_is_set(bits, row) {
                out[row] = 0;
                row += 1;
            }
            let slot = out
                .get_mut(row)
                .ok_or_else(|| CodecError::Corrupted("value page longer than presence".into()))?;
            *slot = map(v)?;
            row += 1;
            Ok(())
        })?;
        for slot in out.iter_mut().skip(row) {
            *slot = 0;
        }
        Ok(count)
    }

    fn bit_is_set(bits: &[u8], i: usize) -> bool {
        bits[i / 8] & (1 << (i % 8)) != 0
    }

    /// Core of [`decode_i64`]: hands each of the `count` decoded values to `emit`
    /// in order, then checks the page was consumed exactly.
    fn decode_i64_each(
        enc: Enc,
        bytes: &[u8],
        count: usize,
        emit: &mut impl FnMut(i64) -> Result<(), CodecError>,
    ) -> Result<(), CodecError> {
        let mut pos = 0usize;
        match enc {
            Enc::Plain => {
                for _ in 0..count {
                    emit(get_ivarint(bytes, &mut pos)?)?;
                }
            }
            Enc::Constant => {
                let v = get_ivarint(bytes, &mut pos)?;
                for _ in 0..count {
                    emit(v)?;
                }
            }
            Enc::Rle => {
                let run_count = get_uvarint(bytes, &mut pos)?;
                let mut total: u64 = 0;
                for _ in 0..run_count {
                    let v = get_ivarint(bytes, &mut pos)?;
                    let run = get_uvarint(bytes, &mut pos)?;
                    total = total
                        .checked_add(run)
                        .ok_or_else(|| CodecError::Corrupted("rle run overflow".into()))?;
                    if total > count as u64 {
                        return Err(CodecError::Corrupted("rle exceeds count".into()));
                    }
                    for _ in 0..run {
                        emit(v)?;
                    }
                }
                if total != count as u64 {
                    return Err(CodecError::Corrupted("rle short of count".into()));
                }
            }
            Enc::DeltaZigzag => {
                if count > 0 {
                    let mut acc = get_ivarint(bytes, &mut pos)?;
                    emit(acc)?;
                    for _ in 1..count {
                        let delta = get_ivarint(bytes, &mut pos)?;
                        acc = acc
                            .checked_add(delta)
                            .ok_or_else(|| CodecError::Corrupted("delta overflow".into()))?;
                        emit(acc)?;
                    }
                }
            }
            Enc::DoubleDelta => {
                if count > 0 {
                    let mut value = get_ivarint(bytes, &mut pos)?;
                    emit(value)?;
                    if count > 1 {
                        let mut prev_delta = get_ivarint(bytes, &mut pos)?;
                        value = value
                            .checked_add(prev_delta)
                            .ok_or_else(|| CodecError::Corrupted("double-delta overflow".into()))?;
                        emit(value)?;
                        for _ in 2..count {
                            let dod = get_ivarint(bytes, &mut pos)?;
                            prev_delta = prev_delta.checked_add(dod).ok_or_else(|| {
                                CodecError::Corrupted("double-delta overflow".into())
                            })?;
                            value = value.checked_add(prev_delta).ok_or_else(|| {
                                CodecError::Corrupted("double-delta overflow".into())
                            })?;
                            emit(value)?;
                        }
                    }
                }
            }
            Enc::ForBitpack => {
                let min = get_ivarint(bytes, &mut pos)?;
                let bit_width = u32::from(
                    *bytes
                        .get(pos)
                        .ok_or_else(|| CodecError::Corrupted("for truncated".into()))?,
                );
                pos += 1;
                unpack_bits_each(&bytes[pos..], count, bit_width, &mut |o| {
                    emit((min as u64).wrapping_add(o) as i64)
                })?;
                pos = bytes.len();
            }
            other => {
                return Err(CodecError::Corrupted(format!(
                    "enc {other:?} is not an integer codec"
                )));
            }
        }
        expect_consumed(pos, bytes.len())
    }

    /// [`unpack_bits`], handing each value to `emit` in order instead of
    /// collecting them, so a caller can store them wherever they belong.
    fn unpack_bits_each(
        buf: &[u8],
        count: usize,
        bit_width: u32,
        emit: &mut impl FnMut(u64) -> Result<(), CodecError>,
    ) -> Result<(), CodecError> {
        if bit_width > 64 {
            return Err(CodecError::Corrupted(format!("bit_width {bit_width} > 64")));
        }
        if bit_width == 0 {
            if !buf.is_empty() {
                return Err(CodecError::Corrupted("bit_width 0 with payload".into()));
            }
            for _ in 0..count {
                emit(0)?;
            }
            return Ok(());
        }
        let need_bits = (count as u64)
            .checked_mul(u64::from(bit_width))
            .ok_or_else(|| CodecError::Corrupted("packed length overflow".into()))?;
        let need = need_bits.div_ceil(8);
        if buf.len() as u64 != need {
            return Err(CodecError::Corrupted(format!(
                "packed length {} != {need}",
                buf.len()
            )));
        }
        let mask = if bit_width == 64 {
            u64::MAX
        } else {
            (1u64 << bit_width) - 1
        };
        let mut acc: u128 = 0;
        let mut nbits: u32 = 0;
        let mut idx = 0usize;
        for _ in 0..count {
            while nbits < bit_width {
                acc |= u128::from(buf[idx]) << nbits;
                idx += 1;
                nbits += 8;
            }
            emit((acc & u128::from(mask)) as u64)?;
            acc >>= bit_width;
            nbits -= bit_width;
        }
        Ok(())
    }
}

/// The bulk integer decode against the value-at-a-time [`oracle`] it
/// replaced: the same values on every valid page, and on mutated, truncated
/// or mislabelled pages the same values or the same error.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod bulk_decode_oracle {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;

    fn values() -> impl Strategy<Value = Vec<i64>> {
        let runs = proptest::collection::vec(
            (prop_oneof![Just(0i64), -5i64..5, any::<i64>()], 1usize..40),
            0..20,
        )
        .prop_map(|runs| {
            runs.into_iter()
                .flat_map(|(v, n)| std::iter::repeat_n(v, n))
                .collect()
        });
        let rising = (any::<i64>(), proptest::collection::vec(-1i64..4, 0..300)).prop_map(
            |(start, deltas)| {
                let mut acc = start;
                deltas
                    .into_iter()
                    .map(|d| {
                        acc = acc.wrapping_add(d);
                        acc
                    })
                    .collect()
            },
        );
        prop_oneof![
            proptest::collection::vec(any::<i64>(), 0..300),
            proptest::collection::vec(-100i64..100, 0..300),
            proptest::collection::vec(0i64..(1 << 40), 0..300),
            runs,
            rising,
            proptest::collection::vec(
                prop_oneof![Just(i64::MIN), Just(i64::MAX), Just(0i64), Just(-1i64)],
                0..50
            ),
        ]
    }

    /// One page per integer encoding that can hold `vals`. The Constant page
    /// carries the first value, so it decodes to `vals` only when they are
    /// all equal; either way both decoders read the same page.
    fn pages(vals: &[i64]) -> Vec<(Enc, Vec<u8>)> {
        let mut out = vec![
            (Enc::Plain, enc_plain_i64(vals)),
            (
                Enc::Constant,
                enc_constant_i64(vals.first().copied().unwrap_or(0)),
            ),
            (Enc::Rle, enc_rle_i64(vals)),
            (Enc::ForBitpack, enc_for_i64(vals)),
        ];
        if let Some(b) = enc_delta_i64(vals) {
            out.push((Enc::DeltaZigzag, b));
        }
        if let Some(b) = enc_double_delta_i64(vals) {
            out.push((Enc::DoubleDelta, b));
        }
        out
    }

    /// A pure map that rejects some values, naming the one it rejected, so
    /// the comparison also pins which value is rejected first.
    fn picky(v: i64) -> Result<i64, CodecError> {
        if v.rem_euclid(5) == 2 {
            Err(CodecError::Corrupted(format!("rejected {v}")))
        } else {
            Ok(v.wrapping_mul(3))
        }
    }

    fn same<T: PartialEq + std::fmt::Debug>(
        what: &str,
        new: Result<T, CodecError>,
        old: Result<T, CodecError>,
    ) -> Result<(), TestCaseError> {
        match (new, old) {
            (Ok(a), Ok(b)) => prop_assert_eq!(a, b, "{}", what),
            (Err(a), Err(b)) => prop_assert_eq!(a.to_string(), b.to_string(), "{}", what),
            (a, b) => prop_assert!(false, "{}: bulk {:?}, oracle {:?}", what, a, b),
        }
        Ok(())
    }

    /// Runs `decode_i64` and `decode_i64_into` (identity map and [`picky`])
    /// through both implementations and requires identical results. Slots
    /// start as junk so a row either decoder leaves unwritten shows up.
    fn check(
        enc: Enc,
        bytes: &[u8],
        count: usize,
        presence: Option<(&[u8], usize)>,
    ) -> Result<(), TestCaseError> {
        same(
            "decode_i64",
            decode_i64(enc, bytes, count),
            oracle::decode_i64(enc, bytes, count),
        )?;
        let (bits, rows) = match presence {
            Some((bits, rows)) => (Some(bits), rows),
            None => (None, count),
        };
        for picky_map in [false, true] {
            let mut a = vec![0x5a5a_5a5a_i64; rows];
            let mut b = a.clone();
            let (ra, rb) = if picky_map {
                (
                    decode_i64_into(enc, bytes, bits, &mut a, picky),
                    oracle::decode_i64_into(enc, bytes, bits, &mut b, picky),
                )
            } else {
                (
                    decode_i64_into(enc, bytes, bits, &mut a, Ok),
                    oracle::decode_i64_into(enc, bytes, bits, &mut b, Ok),
                )
            };
            let ok = ra.is_ok() && rb.is_ok();
            same("decode_i64_into", ra, rb)?;
            if ok {
                prop_assert_eq!(a, b, "decode_i64_into slots, picky {}", picky_map);
            }
        }
        Ok(())
    }

    /// `count` present rows spread over `count + absent` rows, laid out by
    /// `layout`, with junk in the bitmap's padding bits.
    fn presence(count: usize, absent: usize, layout: &[bool], pad: u8) -> (Vec<u8>, usize) {
        let rows = count + absent;
        let mut bits = vec![0u8; rows.div_ceil(8)];
        let (mut present_left, mut absent_left) = (count, absent);
        for row in 0..rows {
            let take =
                absent_left == 0 || (present_left > 0 && layout.get(row).copied().unwrap_or(true));
            if take {
                bits[row / 8] |= 1 << (row % 8);
                present_left -= 1;
            } else {
                absent_left -= 1;
            }
        }
        if !rows.is_multiple_of(8) {
            bits[rows / 8] |= pad << (rows % 8);
        }
        (bits, rows)
    }

    #[derive(Debug, Clone)]
    enum Mutation {
        FlipBit { at: usize, bit: u8 },
        SetByte { at: usize, value: u8 },
        Overwrite { at: usize, bytes: Vec<u8> },
        Truncate { len: usize },
        Append { bytes: Vec<u8> },
    }

    fn mutation() -> impl Strategy<Value = Mutation> {
        prop_oneof![
            (any::<usize>(), 0u8..8).prop_map(|(at, bit)| Mutation::FlipBit { at, bit }),
            (any::<usize>(), any::<u8>()).prop_map(|(at, value)| Mutation::SetByte { at, value }),
            (
                any::<usize>(),
                proptest::collection::vec(any::<u8>(), 1..=4)
            )
                .prop_map(|(at, bytes)| Mutation::Overwrite { at, bytes }),
            any::<usize>().prop_map(|len| Mutation::Truncate { len }),
            proptest::collection::vec(any::<u8>(), 1..=3)
                .prop_map(|bytes| Mutation::Append { bytes }),
        ]
    }

    fn mutate(page: &[u8], m: &Mutation) -> Vec<u8> {
        let mut out = page.to_vec();
        if let Mutation::Append { bytes } = m {
            out.extend_from_slice(bytes);
            return out;
        }
        if out.is_empty() {
            return out;
        }
        let n = out.len();
        match m {
            Mutation::FlipBit { at, bit } => out[at % n] ^= 1 << bit,
            Mutation::SetByte { at, value } => out[at % n] = *value,
            Mutation::Overwrite { at, bytes } => {
                let start = at % n;
                for (slot, b) in out[start..].iter_mut().zip(bytes) {
                    *slot = *b;
                }
            }
            Mutation::Truncate { len } => out.truncate(len % n),
            Mutation::Append { .. } => {}
        }
        out
    }

    proptest! {
        #[test]
        fn bulk_matches_oracle_on_valid_pages(
            vals in values(),
            absent in 0usize..40,
            layout in proptest::collection::vec(any::<bool>(), 0..400),
            pad in any::<u8>(),
        ) {
            let (bits, rows) = presence(vals.len(), absent, &layout, pad);
            for (enc, bytes) in pages(&vals) {
                if enc != Enc::Constant || vals.iter().all(|&v| Some(&v) == vals.first()) {
                    prop_assert_eq!(&decode_i64(enc, &bytes, vals.len()).expect("valid page"), &vals);
                }
                check(enc, &bytes, vals.len(), None)?;
                check(enc, &bytes, vals.len(), Some((&bits, rows)))?;
            }
        }

        #[test]
        fn bulk_matches_oracle_on_corrupt_pages(
            vals in values(),
            m in mutation(),
            absent in 0usize..40,
            layout in proptest::collection::vec(any::<bool>(), 0..400),
            relabel in proptest::option::of(1u8..=13),
            count_shift in -3i64..=3,
        ) {
            let (bits, rows) = presence(vals.len(), absent, &layout, 0);
            for (enc, page) in pages(&vals) {
                let bytes = mutate(&page, &m);
                let enc = relabel.map_or(Ok(enc), Enc::from_u8).expect("tag in range");
                let count = vals.len().saturating_add_signed(count_shift as isize);
                check(enc, &bytes, count, None)?;
                check(enc, &bytes, vals.len(), Some((&bits, rows)))?;
            }
        }

        #[test]
        fn bulk_matches_oracle_on_arbitrary_bytes(
            tag in 1u8..=13,
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..400,
            absent in 0usize..40,
            layout in proptest::collection::vec(any::<bool>(), 0..440),
        ) {
            let enc = Enc::from_u8(tag).expect("tag in range");
            let (bits, rows) = presence(count, absent, &layout, 0);
            check(enc, &bytes, count, None)?;
            check(enc, &bytes, count, Some((&bits, rows)))?;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn enc_tag_roundtrip() {
        for tag in 1u8..=13 {
            assert_eq!(Enc::from_u8(tag).expect("known").to_u8(), tag);
        }
        assert!(matches!(Enc::from_u8(0), Err(CodecError::Corrupted(_))));
        assert!(matches!(Enc::from_u8(14), Err(CodecError::Corrupted(_))));
    }

    /// Tags 10-13 are registered but no codec here reads them: every decoder
    /// refuses each one with its own not-this-codec error, whatever the bytes.
    #[test]
    fn reserved_enc_tags_are_refused_by_every_decoder() {
        for tag in 10u8..=13 {
            let enc = Enc::from_u8(tag).expect("registered");
            for bytes in [&[][..], &[0u8, 0, 0, 0][..]] {
                let is_refusal = |r: Result<(), CodecError>, what: &str| match r {
                    Err(CodecError::Corrupted(msg)) => {
                        assert!(msg.contains(what), "tag {tag}: {msg}")
                    }
                    Ok(()) => panic!("tag {tag} decoded as {what}"),
                };
                is_refusal(
                    decode_i64(enc, bytes, 0).map(|_| ()),
                    "not an integer codec",
                );
                is_refusal(
                    decode_strings(enc, bytes, 0).map(|_| ()),
                    "not a string codec",
                );
                is_refusal(
                    decode_strings_columnar(enc, bytes, 0).map(|_| ()),
                    "not a string codec",
                );
                is_refusal(decode_f64(enc, bytes, 0).map(|_| ()), "not an f64 codec");
                is_refusal(
                    decode_fixed(enc, bytes, 0, 8).map(|_| ()),
                    "not a fixed-width codec",
                );
            }
        }
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
            Err(CodecError::Corrupted(_))
        ));
        // Rle claiming more than count.
        let bytes = enc_rle_i64(&[5, 5, 5, 9]);
        assert!(matches!(
            decode_i64(Enc::Rle, &bytes, 2),
            Err(CodecError::Corrupted(_))
        ));
        // FOR with bit_width > 64.
        let bad = vec![0x00, 65];
        assert!(matches!(
            decode_i64(Enc::ForBitpack, &bad, 1),
            Err(CodecError::Corrupted(_))
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
            tag in 1u8..=13,
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod string_dict_tests {
    use super::*;

    #[test]
    fn dict_strings_exact_bytes() {
        let vals: Vec<&[u8]> = vec![b"b", b"a", b"a", b"b"];
        let (enc, bytes) = encode_strings(&vals);
        assert_eq!(enc, Enc::Dict);
        // dict_count 2; "a"(1,0x61); "b"(1,0x62); ids width 1; packed [1,0,0,1]=0x09
        assert_eq!(bytes, vec![0x02, 0x01, 0x61, 0x01, 0x62, 0x01, 0x09]);
        let got = decode_strings(enc, &bytes, 4).expect("decode");
        let got: Vec<&[u8]> = got.iter().map(|v| v.as_slice()).collect();
        assert_eq!(got, vals);
    }

    #[test]
    fn plain_strings_roundtrip() {
        let vals: Vec<&[u8]> = vec![b"get", b"", b"timeout", b"x"];
        let (enc, bytes) = encode_strings(&vals);
        assert_eq!(enc, Enc::Plain); // 4 distinct of 4 -> plain
        let got = decode_strings(enc, &bytes, 4).expect("decode");
        let got: Vec<&[u8]> = got.iter().map(|v| v.as_slice()).collect();
        assert_eq!(got, vals);
    }

    /// Acceptance test (issue #416): over a corpus that triggers `Enc::Dict`,
    /// the non-fusing decode returns a dictionary and ids whose fusion equals
    /// `decode_strings`' output exactly -- repeated entries, an empty value, and
    /// a single-distinct-value column included -- and a plain page returns the
    /// plain form.
    #[test]
    fn dict_decode_preserves_dictionary_and_ids() {
        // Low cardinality (2 distinct of many) forces Enc::Dict, and the corpus
        // carries a repeated entry and an empty value.
        let corpus: Vec<&[u8]> = vec![b"get", b"", b"get", b"", b"get", b""];
        let (enc, bytes) = encode_strings(&corpus);
        assert_eq!(enc, Enc::Dict, "low-cardinality corpus must be a dict page");

        let decoded = decode_strings_columnar(enc, &bytes, corpus.len()).expect("columnar decode");
        match &decoded {
            DecodedStrings::Dict { dict, ids } => {
                // The distinct set survives, sorted (the encoder sorts): "", "get".
                assert_eq!(dict.len(), 2, "the distinct set is kept, not fused away");
                assert_eq!(dict[0], b"");
                assert_eq!(dict[1], b"get");
                assert_eq!(ids.len(), corpus.len(), "one id per value");
                // Every id indexes the dictionary in range.
                for &id in ids {
                    assert!((id as usize) < dict.len());
                }
            }
            DecodedStrings::Plain(_) => panic!("a dict page must not decode as plain"),
        }

        // Fusing the non-fusing form equals the fused entry point byte-for-byte.
        let fused_direct = decode_strings(enc, &bytes, corpus.len()).expect("fused decode");
        assert_eq!(decoded.fuse(), fused_direct);
        assert_eq!(fused_direct.len(), corpus.len());
        for (got, want) in fused_direct.iter().zip(&corpus) {
            assert_eq!(got.as_slice(), *want);
        }

        // A single-distinct-value column: still a dict page (1 distinct of 5).
        let single: Vec<&[u8]> = vec![b"only", b"only", b"only", b"only", b"only"];
        let (enc, bytes) = encode_strings(&single);
        assert_eq!(enc, Enc::Dict);
        match decode_strings_columnar(enc, &bytes, single.len()).expect("decode") {
            DecodedStrings::Dict { dict, ids } => {
                assert_eq!(dict, vec![b"only".to_vec()]);
                assert_eq!(ids, vec![0u32; single.len()]);
            }
            DecodedStrings::Plain(_) => panic!("single-distinct must be a dict page"),
        }

        // A plain page decodes to the plain form (all distinct -> Enc::Plain).
        let distinct: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let (enc, bytes) = encode_strings(&distinct);
        assert_eq!(enc, Enc::Plain);
        match decode_strings_columnar(enc, &bytes, distinct.len()).expect("decode") {
            DecodedStrings::Plain(v) => {
                let got: Vec<&[u8]> = v.iter().map(|x| x.as_slice()).collect();
                assert_eq!(got, distinct);
            }
            DecodedStrings::Dict { .. } => panic!("a plain page must not decode as dict"),
        }
    }

    /// Every corruption guard still fires through the non-fusing entry point,
    /// as a typed `CodecError`, never a panic and never wrong data.
    #[test]
    fn columnar_decode_rejects_corruption() {
        // Out-of-range id: dict_count 2, entries "a","b", one id = 3.
        let out_of_range = vec![0x02, 0x01, 0x61, 0x01, 0x62, 0x02, 0x03];
        assert!(matches!(
            decode_strings_columnar(Enc::Dict, &out_of_range, 1),
            Err(CodecError::Corrupted(_))
        ));
        // dict_count larger than the buffer.
        let lying_count = vec![0x05, 0x01, 0x61];
        assert!(matches!(
            decode_strings_columnar(Enc::Dict, &lying_count, 1),
            Err(CodecError::Corrupted(_))
        ));
        // Truncated dict entry: length says 4 but no bytes follow.
        let truncated = vec![0x01, 0x04];
        assert!(matches!(
            decode_strings_columnar(Enc::Dict, &truncated, 1),
            Err(CodecError::Corrupted(_))
        ));
        // Plain page with a length longer than its blob.
        let short_blob = vec![0x05, 0x61, 0x62];
        assert!(matches!(
            decode_strings_columnar(Enc::Plain, &short_blob, 1),
            Err(CodecError::Corrupted(_))
        ));
        // Trailing unconsumed bytes after a valid single-distinct dict page.
        let single: Vec<&[u8]> = vec![b"x", b"x"];
        let (enc, mut bytes) = encode_strings(&single);
        assert_eq!(enc, Enc::Dict);
        bytes.push(0x00);
        assert!(matches!(
            decode_strings_columnar(Enc::Dict, &bytes, single.len()),
            Err(CodecError::Corrupted(_))
        ));
    }

    #[test]
    fn f64_constant_and_nan_and_neg_zero() {
        // Constant.
        let same = vec![3.5f64.to_bits(); 4];
        let (enc, bytes) = encode_f64(&same);
        assert_eq!(enc, Enc::Constant);
        assert_eq!(decode_f64(enc, &bytes, 4).expect("decode"), same);

        // NaN payloads and -0.0 survive bit-exactly.
        let vals = vec![
            (-0.0f64).to_bits(),
            0.0f64.to_bits(),
            f64::NAN.to_bits() | 0x1,
            f64::NAN.to_bits() | 0x7,
        ];
        let (enc, bytes) = encode_f64(&vals);
        assert_eq!(decode_f64(enc, &bytes, 4).expect("decode"), vals);
    }

    #[test]
    fn f64_dict_roundtrip() {
        let a = 1.0f64.to_bits();
        let b = 2.0f64.to_bits();
        let vals = vec![a, b, a, b, a, b];
        let (enc, bytes) = encode_f64(&vals);
        assert_eq!(enc, Enc::Dict);
        assert_eq!(decode_f64(enc, &bytes, 6).expect("decode"), vals);
    }

    #[test]
    fn fixed_roundtrip_and_dict() {
        let id1 = [1u8; 16];
        let id2 = [2u8; 16];
        // All distinct -> FixedWidth.
        let a = [3u8; 16];
        let distinct: Vec<&[u8]> = vec![&id1, &id2, &a];
        let (enc, bytes) = encode_fixed(&distinct, 16);
        assert_eq!(enc, Enc::FixedWidth);
        let got = decode_fixed(enc, &bytes, 3, 16).expect("decode");
        let got: Vec<&[u8]> = got.iter().map(|v| v.as_slice()).collect();
        assert_eq!(got, distinct);
        // Repetitive -> Dict.
        let rep: Vec<&[u8]> = vec![&id1, &id2, &id1, &id2];
        let (enc, bytes) = encode_fixed(&rep, 16);
        assert_eq!(enc, Enc::Dict);
        let got = decode_fixed(enc, &bytes, 4, 16).expect("decode");
        let got: Vec<&[u8]> = got.iter().map(|v| v.as_slice()).collect();
        assert_eq!(got, rep);
    }

    #[test]
    fn dict_rejects_id_out_of_range() {
        // dict_count 2, entries "a","b", then width=2 with a single id = 3.
        let bytes = vec![0x02, 0x01, 0x61, 0x01, 0x62, 0x02, 0x03];
        assert!(matches!(
            decode_strings(Enc::Dict, &bytes, 1),
            Err(CodecError::Corrupted(_))
        ));
    }

    #[test]
    fn dict_rejects_lying_dict_count() {
        // dict_count says 5 but only one entry is present.
        let bytes = vec![0x05, 0x01, 0x61];
        assert!(matches!(
            decode_strings(Enc::Dict, &bytes, 1),
            Err(CodecError::Corrupted(_))
        ));
    }

    #[test]
    fn plain_rejects_short_blob() {
        // length says 5 but blob is 2 bytes.
        let bytes = vec![0x05, 0x61, 0x62];
        assert!(matches!(
            decode_strings(Enc::Plain, &bytes, 1),
            Err(CodecError::Corrupted(_))
        ));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod string_dict_proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn strings_roundtrip(
            vals in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..16), 0..400)
        ) {
            let refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();
            let (enc, bytes) = encode_strings(&refs);
            let got = decode_strings(enc, &bytes, refs.len()).expect("decode");
            prop_assert_eq!(got, vals);
        }

        /// `encode_strings_dict` fed a dictionary and per-row ids produces the
        /// exact bytes `encode_strings` produces over the fused per-row values,
        /// across both sides of the dict-versus-plain threshold, with an
        /// unreferenced dictionary entry and a dictionary carrying duplicate
        /// values both present.
        #[test]
        fn encode_strings_dict_matches_encode_strings(
            dict in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..6), 1..8),
            ids in proptest::collection::vec(0u32..8, 0..400),
        ) {
            // Keep ids in range of the generated dictionary.
            let ids: Vec<u32> = ids.iter().map(|&i| i % dict.len() as u32).collect();
            let dref: Vec<&[u8]> = dict.iter().map(|v| v.as_slice()).collect();
            let fused: Vec<&[u8]> = ids.iter().map(|&i| dref[i as usize]).collect();

            let (want_enc, want_bytes) = encode_strings(&fused);
            let (got_enc, got_bytes) = encode_strings_dict(&dref, &ids);
            prop_assert_eq!(got_enc, want_enc);
            prop_assert_eq!(got_bytes, want_bytes);
        }

        #[test]
        fn f64_roundtrip_with_nan_payloads(
            vals in proptest::collection::vec(
                prop_oneof![
                    any::<u64>(),
                    Just(f64::NAN.to_bits()),
                    (0u64..0x8).prop_map(|p| f64::NAN.to_bits() | p),
                    Just((-0.0f64).to_bits()),
                    Just(0.0f64.to_bits()),
                ], 0..400)
        ) {
            let (enc, bytes) = encode_f64(&vals);
            prop_assert_eq!(decode_f64(enc, &bytes, vals.len()).expect("decode"), vals);
        }

        #[test]
        fn fixed_roundtrip(
            vals in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 8..=8), 0..400)
        ) {
            let refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();
            let (enc, bytes) = encode_fixed(&refs, 8);
            let got = decode_fixed(enc, &bytes, refs.len(), 8).expect("decode");
            prop_assert_eq!(got, vals);
        }

        #[test]
        fn string_decode_never_panics(
            tag in 1u8..=13,
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..500,
        ) {
            if let Ok(e) = Enc::from_u8(tag) {
                let _ = decode_strings(e, &bytes, count);
                let _ = decode_f64(e, &bytes, count);
                let _ = decode_fixed(e, &bytes, count, 8);
            }
        }

        /// The non-fusing decode fused equals the fusing decode, and every id it
        /// returns indexes its dictionary in range. A low-cardinality generator
        /// (values drawn from a small alphabet) makes `Enc::Dict` the common
        /// case rather than a rarity.
        #[test]
        fn columnar_decode_fuses_to_decode_strings(
            idxs in proptest::collection::vec(0usize..6, 0..400)
        ) {
            let alphabet: [&[u8]; 6] = [b"", b"a", b"get", b"post", b"timeout", b"xyz"];
            let vals: Vec<&[u8]> = idxs.iter().map(|&i| alphabet[i]).collect();
            let (enc, bytes) = encode_strings(&vals);
            let columnar = decode_strings_columnar(enc, &bytes, vals.len()).expect("columnar");
            if let DecodedStrings::Dict { dict, ids } = &columnar {
                prop_assert_eq!(ids.len(), vals.len());
                for &id in ids {
                    prop_assert!((id as usize) < dict.len());
                }
            }
            let fused = decode_strings(enc, &bytes, vals.len()).expect("fused");
            prop_assert_eq!(columnar.fuse(), fused);
        }

        /// The non-fusing entry point never panics on arbitrary bytes.
        #[test]
        fn columnar_string_decode_never_panics(
            tag in 1u8..=13,
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
            count in 0usize..500,
        ) {
            if let Ok(e) = Enc::from_u8(tag) {
                let _ = decode_strings_columnar(e, &bytes, count);
            }
        }
    }
}
