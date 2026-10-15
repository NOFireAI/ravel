//! A scalar, FastLanes-style transposed bit-packed layout for i64 pages
//! (Afroozeh and Boncz, "The FastLanes Compression Layout", VLDB 2023), built
//! here only to compare its size and decode speed with the production integer
//! page encodings.
//!
//! Values are cut into vectors of [`VECTOR`] values; a short final vector is
//! padded to the full length and the padding is counted in the size. Value
//! `j * LANES + l` of a vector belongs to lane `l` at row `j`, so consecutive
//! values sit in different lanes. Each lane's 64 values are bit-packed
//! LSB-first at the vector's width into `width` 64-bit words, and the words
//! are stored interleaved: word `r` of lane `l` is stored at `r * LANES + l`.
//! The decode loop runs the lanes innermost over contiguous words. The
//! "04261537" row permutation FastLanes applies for its own delta kernels is
//! not applied.
//!
//! Two variants:
//!
//! - [`Variant::For`]: per vector, `base` (the minimum, i64 LE), `width` (u8),
//!   then the packed offsets `v - base` (wrapping, as u64).
//! - [`Variant::Delta`]: per vector, the [`LANES`] row-0 values (i64 LE each),
//!   then the frame of reference of the lane deltas `v[j][l] - v[j-1][l]`
//!   (row 0's delta is 0) exactly as `For` stores offsets.
//!
//! A page is a `count` (u32 LE) followed by its vectors.

/// Values per vector.
pub const VECTOR: usize = 1024;
/// Lanes per vector: a 1024-bit register over 64-bit values.
pub const LANES: usize = 16;
/// Rows per lane.
const ROWS: usize = VECTOR / LANES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    For,
    Delta,
}

impl Variant {
    pub fn name(self) -> &'static str {
        match self {
            Variant::For => "fastlanes_for",
            Variant::Delta => "fastlanes_delta",
        }
    }
}

fn width_of(max_offset: u64) -> u8 {
    (64 - max_offset.leading_zeros()) as u8
}

/// Packs 1024 offsets at `width` bits into `width * LANES` interleaved words.
fn pack(offsets: &[u64; VECTOR], width: u8, out: &mut Vec<u8>) {
    let w = usize::from(width);
    if w == 0 {
        return;
    }
    let mut words = vec![0u64; w * LANES];
    for l in 0..LANES {
        for j in 0..ROWS {
            let v = offsets[j * LANES + l];
            let bit = j * w;
            let (r, shift) = (bit / 64, bit % 64);
            words[r * LANES + l] |= v << shift;
            if shift + w > 64 {
                words[(r + 1) * LANES + l] |= v >> (64 - shift);
            }
        }
    }
    for word in words {
        out.extend_from_slice(&word.to_le_bytes());
    }
}

/// Inverse of [`pack`]: reads `width * LANES` words from `bytes`.
fn unpack(bytes: &[u8], width: u8, out: &mut [u64; VECTOR]) -> Option<()> {
    let w = usize::from(width);
    if w == 0 {
        out.fill(0);
        return Some(());
    }
    let need = w * LANES * 8;
    let src = bytes.get(..need)?;
    let mut words = vec![0u64; w * LANES];
    for (dst, chunk) in words.iter_mut().zip(src.chunks_exact(8)) {
        *dst = u64::from_le_bytes(chunk.try_into().ok()?);
    }
    let mask = if w == 64 { u64::MAX } else { (1u64 << w) - 1 };
    for j in 0..ROWS {
        let bit = j * w;
        let (r, shift) = (bit / 64, bit % 64);
        let lo = &words[r * LANES..(r + 1) * LANES];
        let row = &mut out[j * LANES..(j + 1) * LANES];
        if shift + w > 64 {
            let hi = &words[(r + 1) * LANES..(r + 2) * LANES];
            for l in 0..LANES {
                row[l] = ((lo[l] >> shift) | (hi[l] << (64 - shift))) & mask;
            }
        } else {
            for l in 0..LANES {
                row[l] = (lo[l] >> shift) & mask;
            }
        }
    }
    Some(())
}

fn frame(values: &[i64; VECTOR], out: &mut Vec<u8>) {
    let base = values.iter().copied().min().unwrap_or(0);
    let mut offsets = [0u64; VECTOR];
    for (o, &v) in offsets.iter_mut().zip(values) {
        *o = v.wrapping_sub(base) as u64;
    }
    let width = width_of(offsets.iter().copied().max().unwrap_or(0));
    out.extend_from_slice(&base.to_le_bytes());
    out.push(width);
    pack(&offsets, width, out);
}

/// Reads one frame written by [`frame`] into `out`; returns the bytes read.
fn unframe(bytes: &[u8], out: &mut [i64; VECTOR], scratch: &mut [u64; VECTOR]) -> Option<usize> {
    let base = i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
    let width = *bytes.get(8)?;
    if width > 64 {
        return None;
    }
    unpack(bytes.get(9..)?, width, scratch)?;
    for (o, &s) in out.iter_mut().zip(scratch.iter()) {
        *o = base.wrapping_add(s as i64);
    }
    Some(9 + usize::from(width) * LANES * 8)
}

/// Encodes `values` as one page of `variant`.
pub fn encode(values: &[i64], variant: Variant) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for chunk in values.chunks(VECTOR) {
        // Pad with the chunk's last value: offset or delta 0 past the end.
        let pad = chunk.last().copied().unwrap_or(0);
        let mut v = [pad; VECTOR];
        v[..chunk.len()].copy_from_slice(chunk);
        match variant {
            Variant::For => frame(&v, &mut out),
            Variant::Delta => {
                let mut d = [0i64; VECTOR];
                for i in LANES..VECTOR {
                    d[i] = v[i].wrapping_sub(v[i - LANES]);
                }
                for x in &v[..LANES] {
                    out.extend_from_slice(&x.to_le_bytes());
                }
                frame(&d, &mut out);
            }
        }
    }
    out
}

/// Decodes a page written by [`encode`]. `None` on malformed input.
pub fn decode(bytes: &[u8], variant: Variant) -> Option<Vec<i64>> {
    let count = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    // `count` comes from untrusted input; do not let a corrupted huge count
    // drive a multi-GB allocation before the payload length is checked.
    let mut out = Vec::new();
    let mut at = 4;
    let mut vec = [0i64; VECTOR];
    let mut scratch = [0u64; VECTOR];
    while out.len() < count {
        match variant {
            Variant::For => {
                at += unframe(bytes.get(at..)?, &mut vec, &mut scratch)?;
            }
            Variant::Delta => {
                let mut heads = [0i64; LANES];
                for (l, h) in heads.iter_mut().enumerate() {
                    let s = at + l * 8;
                    *h = i64::from_le_bytes(bytes.get(s..s + 8)?.try_into().ok()?);
                }
                at += LANES * 8;
                at += unframe(bytes.get(at..)?, &mut vec, &mut scratch)?;
                vec[..LANES].copy_from_slice(&heads);
                for i in LANES..VECTOR {
                    vec[i] = vec[i - LANES].wrapping_add(vec[i]);
                }
            }
        }
        let take = (count - out.len()).min(VECTOR);
        out.extend_from_slice(&vec[..take]);
    }
    (at == bytes.len()).then_some(out)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn sizes_are_the_stated_layout() {
        // 2048 values 0..2048 step 1: FOR width 10, Delta width 5 (delta 16).
        let v: Vec<i64> = (0..2048).collect();
        let f = encode(&v, Variant::For);
        assert_eq!(f.len(), 4 + 2 * (9 + 10 * LANES * 8));
        let d = encode(&v, Variant::Delta);
        assert_eq!(d.len(), 4 + 2 * (LANES * 8 + 9 + 5 * LANES * 8));
        assert_eq!(decode(&f, Variant::For).unwrap(), v);
        assert_eq!(decode(&d, Variant::Delta).unwrap(), v);
    }

    #[test]
    fn transposed_order_puts_neighbours_in_different_lanes() {
        // Width 1: value 1 at position 1 lands in lane 1, word 0, bit 0.
        let mut v = vec![0i64; VECTOR];
        v[1] = 1;
        let page = encode(&v, Variant::For);
        let words = &page[4 + 9..];
        assert_eq!(words.len(), LANES * 8);
        let lane1 = u64::from_le_bytes(words[8..16].try_into().unwrap());
        assert_eq!(lane1, 1);
        let lane0 = u64::from_le_bytes(words[..8].try_into().unwrap());
        assert_eq!(lane0, 0);
    }

    #[test]
    fn extremes_round_trip() {
        let v = vec![i64::MIN, i64::MAX, 0, -1, 1, i64::MIN, 7];
        for variant in [Variant::For, Variant::Delta] {
            assert_eq!(decode(&encode(&v, variant), variant).unwrap(), v);
        }
        assert_eq!(
            decode(&encode(&[], Variant::For), Variant::For).unwrap(),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn constant_values_pack_at_width_zero() {
        let v = vec![9i64; 100];
        let f = encode(&v, Variant::For);
        assert_eq!(f.len(), 4 + 9);
        assert_eq!(decode(&f, Variant::For).unwrap(), v);
        let d = encode(&v, Variant::Delta);
        assert_eq!(d.len(), 4 + LANES * 8 + 9);
        assert_eq!(decode(&d, Variant::Delta).unwrap(), v);
    }

    #[test]
    fn huge_count_with_short_payload_is_refused() {
        let mut page = u32::MAX.to_le_bytes().to_vec();
        page.extend_from_slice(&[0u8; 16]);
        assert!(decode(&page, Variant::For).is_none());
        assert!(decode(&page, Variant::Delta).is_none());
    }

    #[test]
    fn truncated_input_is_refused() {
        let v: Vec<i64> = (0..100).map(|i| i * 1_000_003).collect();
        let page = encode(&v, Variant::Delta);
        for cut in [0, 3, 10, page.len() - 1] {
            assert!(decode(&page[..cut], Variant::Delta).is_none(), "{cut}");
        }
    }

    proptest! {
        #[test]
        fn round_trip(v in proptest::collection::vec(any::<i64>(), 0..3000)) {
            for variant in [Variant::For, Variant::Delta] {
                prop_assert_eq!(decode(&encode(&v, variant), variant), Some(v.clone()));
            }
        }
    }
}
