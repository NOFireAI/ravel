//! A per-block trigram Bloom filter, built here only to size it and to count
//! the blocks it would keep for a substring needle. ADR-0105's GRAM_IDX
//! (reserved, not implemented) is exact trigram postings instead; it would
//! keep the report's `trigram_set_blocks`. Nothing reads or writes this filter
//! outside the report.

/// A byte trigram packed as `b0 << 16 | b1 << 8 | b2`.
pub type Trigram = u32;

/// Every byte trigram of `text`, in order, duplicates kept. Empty for text
/// shorter than three bytes.
pub fn trigrams(text: &[u8]) -> impl Iterator<Item = Trigram> + '_ {
    text.windows(3)
        .map(|w| (u32::from(w[0]) << 16) | (u32::from(w[1]) << 8) | u32::from(w[2]))
}

/// The sorted distinct trigrams over every value in `values`.
pub fn distinct_trigrams<V: AsRef<[u8]>>(values: &[V]) -> Vec<Trigram> {
    let mut out: Vec<Trigram> = values.iter().flat_map(|v| trigrams(v.as_ref())).collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// Filter geometry for `n` distinct keys at false-positive target `p`: the
/// textbook optimum `m = ceil(-n ln p / (ln 2)^2)` rounded up to whole 64-bit
/// words (at least one), and `k = max(1, round(m / n * ln 2))`.
pub fn geometry(n: usize, p: f64) -> (usize, u32) {
    let ln2 = std::f64::consts::LN_2;
    let n_f = n.max(1) as f64;
    let raw = (-(n_f) * p.ln() / (ln2 * ln2)).ceil().max(64.0) as usize;
    let m_bits = raw.div_ceil(64) * 64;
    let k = ((m_bits as f64 / n_f) * ln2).round().max(1.0) as u32;
    (m_bits, k)
}

/// splitmix64's finalizer.
pub(crate) fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A classic Bloom filter over trigrams with double hashing: probe `i` sets
/// bit `(h1 + i * h2) mod m`, `h1 = mix64(t)`, `h2 = mix64(h1) | 1`.
#[derive(Clone, Debug)]
pub struct TrigramBloom {
    words: Vec<u64>,
    m_bits: u64,
    k: u32,
}

impl TrigramBloom {
    pub fn build(keys: &[Trigram], p: f64) -> Self {
        let (m_bits, k) = geometry(keys.len(), p);
        let mut bloom = TrigramBloom {
            words: vec![0; m_bits / 64],
            m_bits: m_bits as u64,
            k,
        };
        for &t in keys {
            for bit in bloom.bits(t) {
                bloom.words[(bit / 64) as usize] |= 1 << (bit % 64);
            }
        }
        bloom
    }

    fn bits(&self, t: Trigram) -> impl Iterator<Item = u64> + use<> {
        let h1 = mix64(u64::from(t));
        let h2 = mix64(h1) | 1;
        let m = self.m_bits;
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % m)
    }

    pub fn may_contain(&self, t: Trigram) -> bool {
        self.bits(t)
            .all(|bit| self.words[(bit / 64) as usize] & (1 << (bit % 64)) != 0)
    }

    /// Whether the block may hold a value containing every trigram of
    /// `needle`. A needle under three bytes has no trigram and is always kept.
    pub fn may_match(&self, needle: &[u8]) -> bool {
        trigrams(needle).all(|t| self.may_contain(t))
    }

    /// The filter's bit array size in bytes (no header counted).
    pub fn byte_len(&self) -> usize {
        self.words.len() * 8
    }

    pub fn hash_count(&self) -> u32 {
        self.k
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn geometry_matches_the_textbook_point() {
        // 9.585 bits per key and 7 hashes at 1%; 14.38 and 10 at 0.1%.
        assert_eq!(geometry(1000, 0.01), (9600, 7));
        assert_eq!(geometry(1000, 0.001), (14400, 10));
        assert_eq!(geometry(0, 0.01), (64, 44));
    }

    #[test]
    fn no_false_negative_and_needle_semantics() {
        let lines = [b"connection refused".as_slice(), b"ok"];
        let keys = distinct_trigrams(&lines);
        let bloom = TrigramBloom::build(&keys, 0.01);
        assert!(bloom.may_match(b"refuse"));
        assert!(bloom.may_match(b"ion r"));
        assert!(bloom.may_match(b"ok"));
        assert!(bloom.may_match(b""));
        assert!(!bloom.may_match(b"zzzzqqqq"));
    }

    #[test]
    fn measured_fpr_is_near_target() {
        let keys: Vec<Trigram> = (0..5000u32).map(|i| i * 3).collect();
        let bloom = TrigramBloom::build(&keys, 0.01);
        let fp = (0..100_000u32)
            .map(|i| i * 3 + 1)
            .filter(|&t| bloom.may_contain(t))
            .count();
        // 1% target over 100,000 absent keys: well inside [300, 2000].
        assert!((300..2000).contains(&fp), "{fp}");
    }
}
