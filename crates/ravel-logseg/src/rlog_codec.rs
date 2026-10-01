//! RLOG-only page codecs and the candidate sets the writer chooses among by
//! stored size (ADR-2135 decisions 3 and 4, docs/log-segment-format.md
//! "Encodings (tag registry)" and "Integer codec layouts").
//!
//! `ravel-codec`'s `encode_i64` and `encode_strings` return only their own
//! pick, chosen by encoded length, and they are shared with RSEG and RSPAN, so
//! they cannot change. Choosing by stored size needs every candidate, so the
//! candidate encoders are reproduced here byte for byte; the unit tests pin
//! them against `ravel-codec`'s own output. Decoding tags 1 to 9 stays in
//! `ravel-codec`.
//!
//! Tag 10 (GCD i64), tag 11 (column reference), and tags 12 and 13 (the row
//! group string dictionary page and its per-block id pages) exist only in
//! RLOG; RSEG and RSPAN keep refusing 12 and 13. Every
//! decoder here treats its input as untrusted and returns
//! [`LogSegError::Corrupted`] on any violation.

use crate::encoding::{Enc, decode_i64, encode_i64};
use crate::error::LogSegError;
use crate::record::{COL_OBSERVED_TS, COL_TS};
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

// ---------------------------------------------------------------------------
// Integer candidates (tags 1 to 6, as `ravel-codec` lays them out)
// ---------------------------------------------------------------------------

fn width_for(range: u64) -> u32 {
    if range == 0 {
        0
    } else {
        64 - range.leading_zeros()
    }
}

/// Packs `values`, `bit_width` bits each, LSB-first (`bit_width` in `0..=64`).
fn pack_bits(out: &mut Vec<u8>, values: &[u64], bit_width: u32) {
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

fn enc_plain_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        put_ivarint(&mut out, v);
    }
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

fn enc_delta_i64(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    if let Some(&first) = values.first() {
        put_ivarint(&mut out, first);
        for w in values.windows(2) {
            put_ivarint(&mut out, w[1].checked_sub(w[0])?);
        }
    }
    Some(out)
}

fn enc_double_delta_i64(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let Some(&first) = values.first() else {
        return Some(out);
    };
    put_ivarint(&mut out, first);
    if values.len() >= 2 {
        let mut prev_delta = values[1].checked_sub(values[0])?;
        put_ivarint(&mut out, prev_delta);
        for w in values.windows(2).skip(1) {
            let delta = w[1].checked_sub(w[0])?;
            put_ivarint(&mut out, delta.checked_sub(prev_delta)?);
            prev_delta = delta;
        }
    }
    Some(out)
}

fn enc_for_i64(values: &[i64], min: i64, max: i64) -> Vec<u8> {
    let bit_width = width_for((max as u64).wrapping_sub(min as u64));
    let mut out = Vec::new();
    put_ivarint(&mut out, min);
    out.push(bit_width as u8);
    let offsets: Vec<u64> = values
        .iter()
        .map(|&v| (v as u64).wrapping_sub(min as u64))
        .collect();
    pack_bits(&mut out, &offsets, bit_width);
    out
}

fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Encodes a tag-10 page, or `None` when the candidate does not apply.
///
/// The offsets are `v.wrapping_sub(base)` as u64 with `base` the page
/// minimum. The candidate applies only when the offsets share a divisor of at
/// least 2 and every quotient fits in i64. Layout: `gcd` varint, `base`
/// ivarint, the inner tag `encode_i64` picks for the quotients (one of 1 to
/// 6), then the inner encoding.
pub fn encode_gcd_i64(values: &[i64]) -> Option<Vec<u8>> {
    let base = values.iter().copied().min()?;
    let mut g = 0u64;
    for &v in values {
        g = gcd_u64(g, (v as u64).wrapping_sub(base as u64));
        if g == 1 {
            return None;
        }
    }
    if g < 2 {
        return None;
    }
    let quotients: Vec<i64> = values
        .iter()
        .map(|&v| i64::try_from((v as u64).wrapping_sub(base as u64) / g).ok())
        .collect::<Option<_>>()?;
    let (inner, inner_bytes) = encode_i64(&quotients);
    let mut out = Vec::with_capacity(inner_bytes.len() + 21);
    put_uvarint(&mut out, g);
    put_ivarint(&mut out, base);
    out.push(inner.to_u8());
    out.extend_from_slice(&inner_bytes);
    Some(out)
}

/// Decodes `count` values from a tag-10 page.
///
/// A `gcd` below 2, an inner tag outside 1 to 6, a negative quotient, a
/// quotient whose product with `gcd` overflows u64, and trailing or missing
/// bytes are all `Corrupted`. The product is added to `base` with a wrapping
/// add, which inverts the encoder's wrapping subtraction exactly.
pub fn decode_gcd_i64(bytes: &[u8], count: usize) -> Result<Vec<i64>, LogSegError> {
    let mut pos = 0usize;
    let g = get_uvarint(bytes, &mut pos)?;
    if g < 2 {
        return Err(LogSegError::Corrupted(format!("gcd page divisor {g} < 2")));
    }
    let base = get_ivarint(bytes, &mut pos)?;
    let tag = *bytes
        .get(pos)
        .ok_or_else(|| LogSegError::Corrupted("gcd page truncated before inner tag".into()))?;
    pos += 1;
    let inner = Enc::from_u8(tag)?;
    if !matches!(
        inner,
        Enc::Plain
            | Enc::Constant
            | Enc::Rle
            | Enc::DeltaZigzag
            | Enc::DoubleDelta
            | Enc::ForBitpack
    ) {
        return Err(LogSegError::Corrupted(format!(
            "gcd page inner tag {tag} is not an integer codec"
        )));
    }
    let quotients = decode_i64(inner, &bytes[pos..], count)?;
    let mut out = Vec::with_capacity(quotients.len());
    for q in quotients {
        let q = u64::try_from(q)
            .map_err(|_| LogSegError::Corrupted(format!("gcd page quotient {q} is negative")))?;
        let product = q.checked_mul(g).ok_or_else(|| {
            LogSegError::Corrupted(format!("gcd page quotient {q} times {g} overflows u64"))
        })?;
        out.push((base as u64).wrapping_add(product) as i64);
    }
    Ok(out)
}

/// Every integer candidate for one page, in tie-break priority order:
/// constant (when every value is equal), RLE, plain, delta-zigzag and
/// double-delta (each when no intermediate overflows), FOR bit-pack, then GCD
/// i64 (when [`encode_gcd_i64`] applies). At most six candidates at once:
/// constant needs every value equal, which makes every offset zero, so it
/// and GCD i64 never both apply.
///
/// The first six are exactly `ravel-codec`'s `encode_i64` candidates, in its
/// order, so the smallest of them by encoded length with ties to the earlier
/// one is what `encode_i64` returns.
pub fn i64_candidates(values: &[i64]) -> Vec<(Enc, Vec<u8>)> {
    let (Some(min), Some(max)) = (values.iter().copied().min(), values.iter().copied().max())
    else {
        return vec![(Enc::Plain, Vec::new())];
    };
    let mut candidates: Vec<(Enc, Vec<u8>)> = Vec::with_capacity(7);
    if min == max {
        let mut b = Vec::new();
        put_ivarint(&mut b, min);
        candidates.push((Enc::Constant, b));
    }
    candidates.push((Enc::Rle, enc_rle_i64(values)));
    candidates.push((Enc::Plain, enc_plain_i64(values)));
    if let Some(b) = enc_delta_i64(values) {
        candidates.push((Enc::DeltaZigzag, b));
    }
    if let Some(b) = enc_double_delta_i64(values) {
        candidates.push((Enc::DoubleDelta, b));
    }
    candidates.push((Enc::ForBitpack, enc_for_i64(values, min, max)));
    if let Some(b) = encode_gcd_i64(values) {
        candidates.push((Enc::GcdI64, b));
    }
    candidates
}

// ---------------------------------------------------------------------------
// Column reference (tag 11)
// ---------------------------------------------------------------------------

/// Encodes a tag-11 page naming `target`, the column this one equals row for
/// row with identical presence.
pub fn encode_column_ref(target: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(1);
    put_uvarint(&mut out, u64::from(target));
    out
}

/// Parses a tag-11 page found on `column_id` and returns the referenced
/// column. The only permitted pair is `observed_ts` referring to `ts`: tag 11
/// on any other column, any other target, and trailing or missing bytes are
/// `Corrupted`.
pub fn decode_column_ref(column_id: u32, bytes: &[u8]) -> Result<u32, LogSegError> {
    if column_id != COL_OBSERVED_TS {
        return Err(LogSegError::Corrupted(format!(
            "column reference on column {column_id}; only observed_ts may carry one"
        )));
    }
    let mut pos = 0usize;
    let target = get_uvarint(bytes, &mut pos)?;
    if pos != bytes.len() {
        return Err(LogSegError::Corrupted(format!(
            "column reference left {} bytes unconsumed",
            bytes.len() - pos
        )));
    }
    if target != u64::from(COL_TS) {
        return Err(LogSegError::Corrupted(format!(
            "observed_ts column reference names column {target}; only ts is permitted"
        )));
    }
    Ok(COL_TS)
}

// ---------------------------------------------------------------------------
// String candidates (tags 1 and 7, as `ravel-codec` lays them out)
// ---------------------------------------------------------------------------

/// `ravel-codec`'s dictionary heuristic, kept only to order the two string
/// candidates so a tie goes to the encoding the heuristic alone would pick.
fn dict_is_worth_it(distinct: usize, total: usize) -> bool {
    total > 0 && distinct.saturating_mul(2) <= total
}

fn enc_plain_strings<'a>(values: impl Iterator<Item = &'a [u8]> + Clone) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values.clone() {
        put_uvarint(&mut out, v.len() as u64);
    }
    for v in values {
        out.extend_from_slice(v);
    }
    out
}

/// Dictionary page body: the sorted distinct entries, then the per-value ids
/// as a FOR bit-pack body.
fn enc_dict_strings(sorted: &[&[u8]], ids: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, sorted.len() as u64);
    for entry in sorted {
        put_uvarint(&mut out, entry.len() as u64);
        out.extend_from_slice(entry);
    }
    let bit_width = width_for(sorted.len().saturating_sub(1) as u64);
    out.push(bit_width as u8);
    pack_bits(&mut out, ids, bit_width);
    out
}

fn ordered_string_candidates(
    distinct: usize,
    total: usize,
    dict: Vec<u8>,
    plain: Vec<u8>,
) -> Vec<(Enc, Vec<u8>)> {
    if dict_is_worth_it(distinct, total) {
        vec![(Enc::Dict, dict), (Enc::Plain, plain)]
    } else {
        vec![(Enc::Plain, plain), (Enc::Dict, dict)]
    }
}

/// One string page's values in dictionary shape: the sorted distinct values
/// and, per present value in row order, its index into them. Both writer
/// paths reduce a string column to this before encoding, so the candidates
/// and the row group dictionary see the same values however they arrived.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrShape<'a> {
    pub sorted: Vec<&'a [u8]>,
    pub ids: Vec<u64>,
}

/// The [`StrShape`] of one page's present values.
pub fn string_shape<'a>(values: &[&'a [u8]]) -> StrShape<'a> {
    let mut sorted: Vec<&[u8]> = values.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let ids: Vec<u64> = values
        .iter()
        .map(|v| sorted.partition_point(|e| e < v) as u64)
        .collect();
    StrShape { sorted, ids }
}

/// The [`StrShape`] of a column already in dictionary shape: `present_ids[i]`
/// indexes `dict` for present row `i`, and `dict` may hold entries no id
/// references. Equal to [`string_shape`] over the per-row values, but the sort
/// runs over the referenced distinct values only.
pub fn string_dict_shape<'a>(dict: &[&'a [u8]], present_ids: &[u32]) -> StrShape<'a> {
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
    let mut pos_of = vec![0u32; dict.len()];
    for (i, seen) in referenced.iter().enumerate() {
        if *seen {
            pos_of[i] = distinct.partition_point(|e| *e < dict[i]) as u32;
        }
    }
    let ids: Vec<u64> = present_ids
        .iter()
        .map(|&id| u64::from(pos_of[id as usize]))
        .collect();
    StrShape {
        sorted: distinct,
        ids,
    }
}

/// Both string candidates for a page of `shape`, dictionary and plain. The
/// one `ravel-codec`'s `encode_strings` would pick comes first, so it wins a
/// tie.
pub fn shape_candidates(shape: &StrShape<'_>) -> Vec<(Enc, Vec<u8>)> {
    if shape.ids.is_empty() {
        return vec![(Enc::Plain, Vec::new())];
    }
    let dict = enc_dict_strings(&shape.sorted, &shape.ids);
    let plain = enc_plain_strings(shape.ids.iter().map(|&i| shape.sorted[i as usize]));
    ordered_string_candidates(shape.sorted.len(), shape.ids.len(), dict, plain)
}

/// Both string candidates for one page, dictionary and plain. The one
/// `ravel-codec`'s `encode_strings` would pick comes first, so it wins a tie.
pub fn string_candidates(values: &[&[u8]]) -> Vec<(Enc, Vec<u8>)> {
    shape_candidates(&string_shape(values))
}

/// [`string_candidates`] for a column already in dictionary shape, as
/// [`string_dict_shape`] reads it. Byte-identical to `string_candidates` over
/// the per-row values.
pub fn string_dict_candidates(dict: &[&[u8]], present_ids: &[u32]) -> Vec<(Enc, Vec<u8>)> {
    shape_candidates(&string_dict_shape(dict, present_ids))
}

// ---------------------------------------------------------------------------
// Row group string dictionaries (tags 12 and 13, ADR-2135 decision 6)
// ---------------------------------------------------------------------------

/// Most entries a tag 12 page may carry: the same cap `ravel-codec` puts on a
/// tag 7 page's dictionary section.
pub const MAX_DICT_ENTRIES: u64 = 1 << 16;

/// A tag 12 page: `dict_count` varint, then each entry as `len` varint and its
/// bytes, strictly ascending. The layout of a tag 7 page's dictionary section.
pub fn encode_dict_page(sorted: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, sorted.len() as u64);
    for entry in sorted {
        put_uvarint(&mut out, entry.len() as u64);
        out.extend_from_slice(entry);
    }
    out
}

/// Decodes a tag 12 page.
///
/// An empty dictionary, a `dict_count` above [`MAX_DICT_ENTRIES`] or above
/// what the page's bytes could hold, entries not strictly ascending, and
/// trailing or missing bytes are all `Corrupted`.
pub fn decode_dict_page(bytes: &[u8]) -> Result<Vec<Vec<u8>>, LogSegError> {
    let mut pos = 0usize;
    let count = get_uvarint(bytes, &mut pos)?;
    // Every entry costs at least its length varint, so a count past the page's
    // bytes cannot be honest; the fixed cap bounds the allocation either way.
    let cap = MAX_DICT_ENTRIES.min(bytes.len() as u64 + 1);
    if count == 0 || count > cap {
        return Err(LogSegError::Corrupted(format!(
            "dictionary page count {count} outside 1..={cap}"
        )));
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = get_uvarint(bytes, &mut pos)?;
        let end = usize::try_from(len)
            .ok()
            .and_then(|l| pos.checked_add(l))
            .filter(|e| *e <= bytes.len())
            .ok_or_else(|| LogSegError::Corrupted("dictionary page entry truncated".into()))?;
        let entry = &bytes[pos..end];
        pos = end;
        if let Some(prev) = out.last()
            && prev.as_slice() >= entry
        {
            return Err(LogSegError::Corrupted(
                "dictionary page entries not strictly ascending".into(),
            ));
        }
        out.push(entry.to_vec());
    }
    if pos != bytes.len() {
        return Err(LogSegError::Corrupted(
            "dictionary page trailing bytes".into(),
        ));
    }
    Ok(out)
}

/// The id width of a tag 13 page over a dictionary of `dict_len` entries: the
/// width a tag 7 page over the same dictionary would use.
fn dict_id_width(dict_len: usize) -> u32 {
    width_for(dict_len.saturating_sub(1) as u64)
}

/// A tag 13 page: one id per present value packed LSB-first at
/// `width_for(dict_len - 1)` bits. The page stores no width; the reader
/// derives it from the chunk's tag 12 page, so a one-entry dictionary's id
/// pages are empty.
pub fn encode_dict_ids(ids: &[u64], dict_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    pack_bits(&mut out, ids, dict_id_width(dict_len));
    out
}

/// Decodes `count` ids from a tag 13 page over a dictionary of `dict_len`
/// entries.
///
/// An id at or past `dict_len`, and a page length other than `count` ids at
/// the derived width need, are `Corrupted`.
pub fn decode_dict_ids(
    body: &[u8],
    count: usize,
    dict_len: usize,
) -> Result<Vec<u32>, LogSegError> {
    let w = dict_id_width(dict_len);
    let need = (count as u64)
        .checked_mul(u64::from(w))
        .map(|bits| bits.div_ceil(8))
        .ok_or_else(|| LogSegError::Corrupted("dictionary id page length overflow".into()))?;
    if body.len() as u64 != need {
        return Err(LogSegError::Corrupted(format!(
            "dictionary id page holds {} bytes, {count} ids at width {w} need {need}",
            body.len()
        )));
    }
    let mask = if w == 0 { 0u64 } else { (1u64 << w) - 1 };
    let mut out = Vec::with_capacity(count);
    let mut acc: u64 = 0;
    let mut nbits: u32 = 0;
    let mut bytes_in = body.iter();
    for _ in 0..count {
        while nbits < w {
            let b = bytes_in
                .next()
                .ok_or_else(|| LogSegError::Corrupted("dictionary id page truncated".into()))?;
            acc |= u64::from(*b) << nbits;
            nbits += 8;
        }
        let id = acc & mask;
        acc >>= w;
        nbits -= w;
        if id >= dict_len as u64 {
            return Err(LogSegError::Corrupted(format!(
                "dictionary id {id} past a dictionary of {dict_len} entries"
            )));
        }
        out.push(id as u32);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::encoding::{encode_strings, encode_strings_dict};
    use proptest::prelude::*;

    /// The pre-envelope pick among the first six candidates, which must be
    /// exactly `encode_i64`'s output.
    fn smallest_encoded(candidates: Vec<(Enc, Vec<u8>)>) -> (Enc, Vec<u8>) {
        let mut best: Option<(Enc, Vec<u8>)> = None;
        for (enc, bytes) in candidates {
            match &best {
                Some((_, b)) if bytes.len() >= b.len() => {}
                _ => best = Some((enc, bytes)),
            }
        }
        best.expect("at least one candidate")
    }

    fn without_gcd(values: &[i64]) -> Vec<(Enc, Vec<u8>)> {
        i64_candidates(values)
            .into_iter()
            .filter(|(e, _)| *e != Enc::GcdI64)
            .collect()
    }

    #[test]
    fn gcd_exact_bytes() {
        // Offsets from base 10 are 0, 6, 12: gcd 6, quotients 0, 1, 2. Plain,
        // delta, double-delta and FOR all take three bytes for those, and
        // encode_i64 breaks the tie toward plain (zigzag 0, 2, 4). Base 10 is
        // zigzag 20.
        let got = encode_gcd_i64(&[10, 16, 22]).expect("applies");
        assert_eq!(got, vec![6, 20, Enc::Plain.to_u8(), 0, 2, 4]);
        assert_eq!(decode_gcd_i64(&got, 3).expect("decodes"), vec![10, 16, 22]);
    }

    #[test]
    fn gcd_does_not_apply_without_a_shared_divisor() {
        assert_eq!(encode_gcd_i64(&[]), None);
        assert_eq!(encode_gcd_i64(&[7, 7, 7]), None, "all offsets zero");
        assert_eq!(encode_gcd_i64(&[4, 6, 7]), None, "offsets 0, 2, 3");
        // Values that share no divisor but whose gaps do still qualify.
        assert!(encode_gcd_i64(&[3, 5, 9]).is_some());
    }

    #[test]
    fn gcd_rejects_bad_divisor_and_inner_tag() {
        let good = encode_gcd_i64(&[10, 16, 22]).expect("applies");
        for g in [0u8, 1] {
            let mut bad = good.clone();
            bad[0] = g;
            assert!(matches!(
                decode_gcd_i64(&bad, 3),
                Err(LogSegError::Corrupted(m)) if m.contains("divisor")
            ));
        }
        for tag in [7u8, 8, 9, 10, 11, 12, 13] {
            let mut bad = good.clone();
            bad[2] = tag;
            assert!(
                matches!(decode_gcd_i64(&bad, 3), Err(LogSegError::Corrupted(_))),
                "inner tag {tag}"
            );
        }
        let mut long = good.clone();
        long.push(0);
        assert!(matches!(
            decode_gcd_i64(&long, 3),
            Err(LogSegError::Corrupted(_))
        ));
        assert!(matches!(
            decode_gcd_i64(&good[..good.len() - 1], 3),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn gcd_rejects_a_negative_quotient() {
        // gcd 2, base 0, plain inner, one quotient of -1.
        let bytes = vec![2, 0, Enc::Plain.to_u8(), 1];
        assert!(matches!(
            decode_gcd_i64(&bytes, 1),
            Err(LogSegError::Corrupted(m)) if m.contains("negative")
        ));
    }

    #[test]
    fn column_ref_exact_bytes_and_rules() {
        assert_eq!(encode_column_ref(COL_TS), vec![0]);
        assert_eq!(
            decode_column_ref(COL_OBSERVED_TS, &[0]).expect("ok"),
            COL_TS
        );
        assert!(decode_column_ref(COL_TS, &[0]).is_err());
        assert!(decode_column_ref(COL_OBSERVED_TS, &[2]).is_err());
        assert!(decode_column_ref(COL_OBSERVED_TS, &[0, 0]).is_err());
        assert!(decode_column_ref(COL_OBSERVED_TS, &[]).is_err());
    }

    #[test]
    fn string_candidates_put_the_heuristic_pick_first() {
        let low: Vec<&[u8]> = vec![b"a", b"a", b"b", b"b"];
        assert_eq!(string_candidates(&low)[0].0, Enc::Dict);
        let high: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        assert_eq!(string_candidates(&high)[0].0, Enc::Plain);
    }

    proptest! {
        /// The stored-size winner never stores more bytes than the page
        /// `encode_i64` or `encode_strings` alone would have produced, because
        /// that page is one of the candidates.
        #[test]
        fn stored_choice_never_exceeds_the_codec_pick(
            ints in proptest::collection::vec(
                prop_oneof![any::<i64>(), 0i64..16, (0i64..50).prop_map(|s| s * 1_000_000_000)],
                1..1500,
            ),
            strs in proptest::collection::vec(proptest::collection::vec(0u8..3, 0..12), 1..600),
        ) {
            use crate::page::{seal_page, smallest_stored};
            let (enc, bytes) = encode_i64(&ints);
            let before = seal_page(enc, bytes, 3).stored.len();
            let after = smallest_stored(i64_candidates(&ints), 3).expect("candidate");
            prop_assert!(after.stored.len() <= before, "{} > {before}", after.stored.len());

            let refs: Vec<&[u8]> = strs.iter().map(Vec::as_slice).collect();
            let (enc, bytes) = encode_strings(&refs);
            let before = seal_page(enc, bytes, 3).stored.len();
            let after = smallest_stored(string_candidates(&refs), 3).expect("candidate");
            prop_assert!(after.stored.len() <= before, "{} > {before}", after.stored.len());
        }

        #[test]
        fn i64_candidates_reproduce_encode_i64(
            vals in proptest::collection::vec(
                prop_oneof![any::<i64>(), -40i64..40, Just(i64::MIN), Just(i64::MAX)],
                0..300,
            ),
        ) {
            prop_assert_eq!(smallest_encoded(without_gcd(&vals)), encode_i64(&vals));
            for (enc, bytes) in i64_candidates(&vals) {
                let back = if enc == Enc::GcdI64 {
                    decode_gcd_i64(&bytes, vals.len()).expect("gcd decodes")
                } else {
                    decode_i64(enc, &bytes, vals.len()).expect("decodes")
                };
                prop_assert_eq!(&back, &vals, "enc {:?}", enc);
            }
        }

        #[test]
        fn gcd_round_trips_scaled_pages(
            base in any::<i64>(),
            steps in proptest::collection::vec(0u64..1000, 1..200),
            scale in prop::sample::select(vec![2u64, 3, 1_000, 1_000_000_000, 1 << 62]),
        ) {
            let vals: Vec<i64> = steps
                .iter()
                .map(|s| (base as u64).wrapping_add(s.wrapping_mul(scale)) as i64)
                .collect();
            if let Some(bytes) = encode_gcd_i64(&vals) {
                prop_assert_eq!(decode_gcd_i64(&bytes, vals.len()).expect("decodes"), vals);
            }
        }

        #[test]
        fn string_candidates_reproduce_encode_strings(
            vals in proptest::collection::vec(
                proptest::collection::vec(0u8..4, 0..3),
                0..60,
            ),
        ) {
            let refs: Vec<&[u8]> = vals.iter().map(Vec::as_slice).collect();
            let cands = string_candidates(&refs);
            prop_assert_eq!(&cands[0], &encode_strings(&refs));
            // The dictionary-shaped producer yields the same two candidates.
            let mut dict: Vec<Vec<u8>> = Vec::new();
            let mut ids: Vec<u32> = Vec::new();
            for v in &vals {
                let id = match dict.iter().position(|d| d == v) {
                    Some(i) => i,
                    None => {
                        dict.push(v.clone());
                        dict.len() - 1
                    }
                };
                ids.push(id as u32);
            }
            let dref: Vec<&[u8]> = dict.iter().map(Vec::as_slice).collect();
            prop_assert_eq!(&string_dict_candidates(&dref, &ids), &cands);
            prop_assert_eq!(&cands[0], &encode_strings_dict(&dref, &ids));
        }
    }
}
