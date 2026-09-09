//! Blocked token bloom filter, one per row block
//! (docs/log-segment-format.md "BLOOM").
//!
//! A bloom answers "can this block contain this token in this column?" with
//! false positives but never false negatives. That asymmetry is what makes
//! bloom-based pruning sound (ADR-0013): a negative is proof of absence, so
//! skipping is safe; a positive is no information, so the block is scanned and
//! the predicate re-evaluated exactly.
//!
//! Filters are blocked on 512-bit blocks so a probe touches one cache line,
//! and field-scoped (the column id is hashed in) so a `body` match never
//! collides with an `attr.k` match.

use std::collections::HashSet;

use crate::error::CodecError;
use crate::varint::{get_uvarint, put_uvarint};

/// Hash count per probe, sized for a ~1% false-positive rate.
const K: u8 = 7;
/// Block size in bits; a probe stays inside one block.
const BLOCK_BITS: u64 = 512;
/// Bits per element for p = 0.01: `-ln(0.01) / (ln 2)^2`.
const BITS_PER_ELEM: f64 = 9.585;

/// The three 64-bit hashes for a `(column_id, token)` key under `seed`,
/// read from disjoint bytes of the BLAKE3 digest so block selection and the
/// within-block probes are independent.
struct KeyHash {
    /// Selects the 512-bit block.
    block: u64,
    /// Within-block double-hashing base.
    g1: u64,
    /// Within-block double-hashing stride, low bit forced set so it is never
    /// zero (mod 512).
    g2: u64,
}

/// `h = blake3(seed_le || column_id_le || token)`. `block` is bytes 0..8 LE,
/// `g1` is bytes 8..16 LE, `g2` is bytes 16..24 LE with the low bit forced
/// set. Using disjoint digest bytes for the block and the offsets keeps the
/// first probe from being congruent to the block index (which would collapse
/// most set bits onto two offsets and wreck the false-positive rate).
fn key_hash(seed: u64, column_id: u32, token: &[u8]) -> KeyHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&seed.to_le_bytes());
    hasher.update(&column_id.to_le_bytes());
    hasher.update(token);
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    let le = |range: std::ops::Range<usize>| {
        let mut a = [0u8; 8];
        a.copy_from_slice(&bytes[range]);
        u64::from_le_bytes(a)
    };
    KeyHash {
        block: le(0..8),
        g1: le(8..16),
        g2: le(16..24) | 1,
    }
}

/// The `k` bit indices a key sets or probes, given the filter geometry.
fn probe_bits(h: &KeyHash, k: u8, block_count: u64) -> impl Iterator<Item = u64> {
    let base = (h.block % block_count) * BLOCK_BITS;
    let g1 = h.g1;
    let g2 = h.g2;
    (0..u64::from(k)).map(move |i| base + (g1.wrapping_add(i.wrapping_mul(g2)) % BLOCK_BITS))
}

fn set_bit(bits: &mut [u8], bit: u64) {
    bits[(bit / 8) as usize] |= 1u8 << ((bit % 8) as u32);
}

fn get_bit(bits: &[u8], bit: u64) -> bool {
    bits[(bit / 8) as usize] & (1u8 << ((bit % 8) as u32)) != 0
}

/// Accumulates distinct `(column_id, token)` keys, then sizes and serializes a
/// blocked bloom filter for them.
pub struct BloomBuilder {
    seed: u64,
    staged: HashSet<(u64, u64, u64)>,
}

impl BloomBuilder {
    pub fn new(seed: u64) -> Self {
        BloomBuilder {
            seed,
            staged: HashSet::new(),
        }
    }

    /// Stages one field-scoped token. Duplicates collapse; the staged
    /// distinct count sizes the filter.
    pub fn insert(&mut self, column_id: u32, token: &[u8]) {
        let h = key_hash(self.seed, column_id, token);
        self.staged.insert((h.block, h.g1, h.g2));
    }

    /// Sizes the filter for a ~1% false-positive rate at the staged distinct
    /// count (`m_bits` rounded up to a power of two, at least 512 bits) and
    /// returns the serialized entry bytes: `m_bits` uvarint, `k` u8, `seed`
    /// u64 LE, then the bit array (`m_bits / 8` bytes).
    pub fn finish(self) -> Vec<u8> {
        let n = self.staged.len() as f64;
        let target = (n * BITS_PER_ELEM).ceil() as u64;
        let m_bits = target.max(BLOCK_BITS).next_power_of_two();
        let block_count = m_bits / BLOCK_BITS;
        let mut bits = vec![0u8; (m_bits / 8) as usize];
        for (block, g1, g2) in &self.staged {
            let h = KeyHash {
                block: *block,
                g1: *g1,
                g2: *g2,
            };
            for bit in probe_bits(&h, K, block_count) {
                set_bit(&mut bits, bit);
            }
        }
        let mut out = Vec::new();
        put_uvarint(&mut out, m_bits);
        out.push(K);
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&bits);
        out
    }
}

/// A parsed, read-only view over one serialized bloom entry.
pub struct BloomView<'a> {
    m_bits: u64,
    k: u8,
    seed: u64,
    bits: &'a [u8],
}

impl<'a> BloomView<'a> {
    /// Parses an entry. Rejects a truncated buffer, an `m_bits` that is not a
    /// power of two or is below 512, a `k` of 0, a bit array whose length is
    /// not `m_bits / 8`, and trailing bytes.
    pub fn parse(bytes: &'a [u8]) -> Result<Self, CodecError> {
        let mut pos = 0;
        let m_bits = get_uvarint(bytes, &mut pos)?;
        if m_bits < BLOCK_BITS || !m_bits.is_power_of_two() {
            return Err(CodecError::Corrupted(format!("bloom m_bits {m_bits}")));
        }
        let k = *bytes
            .get(pos)
            .ok_or_else(|| CodecError::Corrupted("bloom truncated at k".into()))?;
        pos += 1;
        if k == 0 {
            return Err(CodecError::Corrupted("bloom k is zero".into()));
        }
        let seed_bytes = bytes
            .get(pos..pos + 8)
            .ok_or_else(|| CodecError::Corrupted("bloom truncated at seed".into()))?;
        let mut seed_arr = [0u8; 8];
        seed_arr.copy_from_slice(seed_bytes);
        let seed = u64::from_le_bytes(seed_arr);
        pos += 8;
        let bits = bytes
            .get(pos..)
            .ok_or_else(|| CodecError::Corrupted("bloom truncated at bits".into()))?;
        if bits.len() as u64 != m_bits / 8 {
            return Err(CodecError::Corrupted(format!(
                "bloom bits length {} != {}",
                bits.len(),
                m_bits / 8
            )));
        }
        Ok(BloomView {
            m_bits,
            k,
            seed,
            bits,
        })
    }

    /// True if the block may contain `token` in `column_id`. A false result
    /// is proof of absence; a true result may be a false positive.
    pub fn may_contain(&self, column_id: u32, token: &[u8]) -> bool {
        let h = key_hash(self.seed, column_id, token);
        let block_count = self.m_bits / BLOCK_BITS;
        probe_bits(&h, self.k, block_count).all(|bit| get_bit(self.bits, bit))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn field_scoping_separates_columns() {
        let mut b = BloomBuilder::new(7);
        b.insert(1, b"timeout");
        let bytes = b.finish();
        let v = BloomView::parse(&bytes).expect("parse");
        assert!(v.may_contain(1, b"timeout"));
        assert!(!v.may_contain(2, b"timeout"));
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let bytes = BloomBuilder::new(0).finish();
        let v = BloomView::parse(&bytes).expect("parse");
        assert!(!v.may_contain(0, b"anything"));
    }

    #[test]
    fn measured_fpr_near_target() {
        let mut b = BloomBuilder::new(42);
        for i in 0..10_000u32 {
            b.insert(3, format!("token-{i}").as_bytes());
        }
        let bytes = b.finish();
        let v = BloomView::parse(&bytes).expect("parse");
        // Inserted tokens never report absent.
        for i in 0..10_000u32 {
            assert!(v.may_contain(3, format!("token-{i}").as_bytes()));
        }
        let mut fp = 0u32;
        let probes = 100_000u32;
        for i in 0..probes {
            if v.may_contain(3, format!("absent-{i}").as_bytes()) {
                fp += 1;
            }
        }
        let fpr = f64::from(fp) / f64::from(probes);
        assert!(fpr < 0.03, "fpr {fpr} too high");
    }

    #[test]
    fn parse_rejects_corrupt_entries() {
        // Truncated: empty buffer.
        assert!(matches!(
            BloomView::parse(&[]),
            Err(CodecError::Corrupted(_))
        ));
        // m_bits not a power of two.
        let mut bad = Vec::new();
        put_uvarint(&mut bad, 513);
        bad.push(7);
        bad.extend_from_slice(&0u64.to_le_bytes());
        assert!(matches!(
            BloomView::parse(&bad),
            Err(CodecError::Corrupted(_))
        ));
        // k = 0.
        let mut bad = Vec::new();
        put_uvarint(&mut bad, 512);
        bad.push(0);
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.extend_from_slice(&[0u8; 64]);
        assert!(matches!(
            BloomView::parse(&bad),
            Err(CodecError::Corrupted(_))
        ));
        // Wrong bit-array length (512 bits needs 64 bytes; give 10).
        let mut bad = Vec::new();
        put_uvarint(&mut bad, 512);
        bad.push(7);
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.extend_from_slice(&[0u8; 10]);
        assert!(matches!(
            BloomView::parse(&bad),
            Err(CodecError::Corrupted(_))
        ));
    }
}

/// The soundness property: for any corpus, every inserted token probes
/// positive. A false negative is impossible by construction.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn no_false_negatives(
            tokens in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 1..32), 1..500),
            col in 0u32..8,
            seed in any::<u64>(),
        ) {
            let mut b = BloomBuilder::new(seed);
            for t in &tokens {
                b.insert(col, t);
            }
            let bytes = b.finish();
            let v = BloomView::parse(&bytes).expect("parse");
            for t in &tokens {
                prop_assert!(v.may_contain(col, t));
            }
        }

        #[test]
        fn parse_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..256),
        ) {
            if let Ok(v) = BloomView::parse(&bytes) {
                let _ = v.may_contain(0, b"probe");
            }
        }
    }
}
