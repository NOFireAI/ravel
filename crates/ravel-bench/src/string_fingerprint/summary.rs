//! The per-block string summary (issue #3710, ADR-3692 Plan row T12).
//!
//! Every value of a block is cut into byte grams of a fixed length
//! (`gram_len`), and every possible gram maps to exactly one bit of a block's
//! bitset:
//!
//! - a **learned** gram, one chosen from the training workload, owns a bit of
//!   its own (bits `0..learned`);
//! - every other gram hashes into a shared **fallback** region (bits
//!   `learned..learned + fallback`).
//!
//! A block's summary sets the bit of every gram of every value it holds. A
//! literal is pruned on a block when ANY of its grams maps to a bit that is
//! clear in that block's summary ([`survives`]). A block holding a value
//! equal to, or containing, the literal holds every gram of the literal, so
//! every one of those bits is set: no false negative, for any learned set and
//! any fallback size. Equality uses the same test, since a value equal to a
//! literal contains it. A literal shorter than `gram_len` has no gram and is
//! never pruned.
//!
//! Relation to arXiv:2507.10391 ("Instance-Optimized String Fingerprints"):
//! the paper keeps one fixed-width fingerprint per row, partitions the
//! alphabet of single characters into bins (one bit per bin), and picks the
//! partition by mixed-integer optimization over a seen workload; an unseen
//! pattern is tested against the same partition, and the subset test makes
//! that sound. This summary keeps the subset test and the
//! partition-of-the-universe shape (each learned gram is a bin of one, the
//! hash partitions every other gram into `fallback` bins), and departs from
//! the paper in three ways: one bitset per RLOG block instead of per row,
//! grams of `gram_len` bytes instead of single characters, and a greedy
//! benefit rank ([`build`]) instead of a solver.

use std::collections::{BTreeMap, HashMap};

/// The shortest gram this summary is built from.
pub const MIN_GRAM: usize = 3;
/// The longest gram: a gram is packed into a `u64` key.
pub const MAX_GRAM: usize = 8;

/// How [`Built::summary_bytes`] counts.
pub const LAYOUT_BYTES: &str = "summary_bytes = blocks * ceil(bits_per_block / 8) + \
learned_bits * gram_len (the shared table of learned grams, each stored as its raw bytes)";

/// The summary's own parameters, pre-registered per subject.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Params {
    pub gram_len: usize,
    /// The byte budget as a share of the column's compressed bytes: every
    /// block's bitset plus the shared learned-gram table.
    pub budget_share: f64,
    /// The fewest fallback bits a block's bitset may have. Grams are learned
    /// only while this many fallback bits still fit the budget; every bit the
    /// budget leaves over goes to the fallback region.
    pub fallback_min_bits: usize,
}

fn gram_mask(gram_len: usize) -> u64 {
    if gram_len >= 8 {
        u64::MAX
    } else {
        (1u64 << (8 * gram_len)) - 1
    }
}

/// Calls `f` with the packed key of every `gram_len`-byte window of `bytes`.
pub fn for_each_gram(bytes: &[u8], gram_len: usize, mut f: impl FnMut(u64)) {
    if gram_len == 0 || bytes.len() < gram_len {
        return;
    }
    let mask = gram_mask(gram_len);
    let mut key = 0u64;
    for (i, &b) in bytes.iter().enumerate() {
        key = ((key << 8) | u64::from(b)) & mask;
        if i + 1 >= gram_len {
            f(key);
        }
    }
}

/// The fallback bit, as an offset into the fallback region, a gram hashes to.
pub fn fallback_slot(key: u64, fallback_bits: usize) -> usize {
    let h = (key ^ 0x517c_c1b7_2722_0a95).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (((h >> 32) * fallback_bits as u64) >> 32) as usize
}

/// Which bit every gram maps to: the learned grams, sorted (bit `i` belongs
/// to `learned[i]`), then `fallback_bits` hashed bits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    pub gram_len: usize,
    pub learned: Vec<u64>,
    pub fallback_bits: usize,
}

impl Layout {
    pub fn bits_per_block(&self) -> usize {
        self.learned.len() + self.fallback_bits
    }

    pub fn bit_of(&self, key: u64) -> usize {
        match self.learned.binary_search(&key) {
            Ok(i) => i,
            Err(_) => self.learned.len() + fallback_slot(key, self.fallback_bits),
        }
    }

    /// The bits `literal`'s grams map to; empty when it is shorter than a gram.
    pub fn literal_bits(&self, literal: &[u8]) -> Vec<usize> {
        let mut out = Vec::new();
        for_each_gram(literal, self.gram_len, |k| out.push(self.bit_of(k)));
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Bytes shared by every block: the learned grams, `gram_len` bytes each.
    pub fn shared_bytes(&self) -> u64 {
        (self.learned.len() * self.gram_len) as u64
    }

    /// Bytes one block's bitset takes.
    pub fn block_bytes(&self) -> u64 {
        self.bits_per_block().div_ceil(8) as u64
    }
}

/// Membership and index lookup over a set of gram keys. 3-byte grams test a
/// dense 2^24-bit table first, so a gram outside the set costs one bit read.
struct KeySet {
    dense: Option<Vec<u64>>,
    index: HashMap<u64, u32>,
}

impl KeySet {
    fn new(keys: &[u64], gram_len: usize) -> KeySet {
        let dense = (gram_len == 3).then(|| {
            let mut bits = vec![0u64; (1 << 24) / 64];
            for &k in keys {
                if let Some(w) = bits.get_mut((k >> 6) as usize) {
                    *w |= 1 << (k & 63);
                }
            }
            bits
        });
        let index = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i as u32))
            .collect();
        KeySet { dense, index }
    }

    fn get(&self, key: u64) -> Option<usize> {
        if let Some(d) = &self.dense
            && d.get((key >> 6) as usize)
                .is_none_or(|w| w & (1 << (key & 63)) == 0)
        {
            return None;
        }
        self.index.get(&key).map(|&i| i as usize)
    }
}

/// One block's bitset over a [`Layout`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSummary {
    words: Vec<u64>,
}

impl BlockSummary {
    fn empty(bits: usize) -> BlockSummary {
        BlockSummary {
            words: vec![0u64; bits.div_ceil(64)],
        }
    }

    fn set(&mut self, i: usize) {
        if let Some(w) = self.words.get_mut(i / 64) {
            *w |= 1 << (i % 64);
        }
    }

    pub fn bit(&self, i: usize) -> bool {
        self.words
            .get(i / 64)
            .is_some_and(|w| w & (1 << (i % 64)) != 0)
    }

    /// Builds one block's bitset against an existing layout. [`build`] uses
    /// the same code for every block it learns from.
    pub fn build(values: &[Vec<u8>], layout: &Layout) -> BlockSummary {
        Builder::new(layout).block(values)
    }

    /// The share of the fallback region's bits that are set.
    pub fn fallback_density(&self, layout: &Layout) -> f64 {
        if layout.fallback_bits == 0 {
            return 0.0;
        }
        let k = layout.learned.len();
        let set = (k..k + layout.fallback_bits)
            .filter(|&i| self.bit(i))
            .count();
        set as f64 / layout.fallback_bits as f64
    }
}

/// Whether a block may hold the literal whose bits are `bits`
/// ([`Layout::literal_bits`]): `false` as soon as any bit is clear.
pub fn survives(summary: &BlockSummary, bits: &[usize]) -> bool {
    bits.iter().all(|&b| summary.bit(b))
}

/// Builds block bitsets against one layout, sharing the learned-gram lookup.
pub struct Builder<'a> {
    layout: &'a Layout,
    learned: KeySet,
}

impl<'a> Builder<'a> {
    pub fn new(layout: &'a Layout) -> Builder<'a> {
        Builder {
            layout,
            learned: KeySet::new(&layout.learned, layout.gram_len),
        }
    }

    pub fn block(&self, values: &[Vec<u8>]) -> BlockSummary {
        let l = self.layout;
        let k = l.learned.len();
        let mut s = BlockSummary::empty(l.bits_per_block());
        for v in values {
            for_each_gram(v, l.gram_len, |key| match self.learned.get(key) {
                Some(i) => s.set(i),
                None => s.set(k + fallback_slot(key, l.fallback_bits)),
            });
        }
        s
    }
}

/// A built summary over a set of blocks.
#[derive(Clone, Debug)]
pub struct Built {
    pub layout: Layout,
    pub summaries: Vec<BlockSummary>,
    /// Distinct grams of the training literals.
    pub candidates: usize,
    /// The byte budget the layout was sized to.
    pub budget_bytes: u64,
}

impl Built {
    pub fn summary_bytes(&self) -> u64 {
        self.summaries.len() as u64 * self.layout.block_bytes() + self.layout.shared_bytes()
    }

    /// The mean, over blocks, of [`BlockSummary::fallback_density`].
    pub fn fallback_density(&self) -> f64 {
        mean_density(&self.summaries, &self.layout)
    }
}

/// The mean, over `summaries`, of [`BlockSummary::fallback_density`].
pub fn mean_density(summaries: &[BlockSummary], layout: &Layout) -> f64 {
    if summaries.is_empty() {
        return 0.0;
    }
    summaries
        .iter()
        .map(|s| s.fallback_density(layout))
        .sum::<f64>()
        / summaries.len() as f64
}

fn cost(blocks: u64, bits: u64, learned: u64, gram_len: u64) -> u64 {
    blocks * bits.div_ceil(8) + learned * gram_len
}

/// Learns the layout from `training` (the training literals, both kinds) and
/// builds every block's bitset: one pass to count, per training gram, the
/// blocks holding it, then one pass to set the bits.
///
/// The candidates are the training literals' distinct grams. A candidate's
/// benefit is the number of training literals holding it times the number of
/// blocks lacking it: a learned gram's bit is exact, so it prunes every such
/// (literal, block) pair, where a hashed bit can be set by a collision.
/// Candidates with a positive benefit are learned in rank order (ties by key)
/// while `fallback_min_bits` fallback bits still fit the budget. What the
/// learned table leaves of the budget is spent on whole bytes of bitset per
/// block, and every bit past the learned ones is fallback.
pub fn build(
    training: &[Vec<u8>],
    blocks: &[Vec<Vec<u8>>],
    params: &Params,
    compressed_bytes: u64,
) -> Result<Built, String> {
    let g = params.gram_len;
    if !(MIN_GRAM..=MAX_GRAM).contains(&g) {
        return Err(format!("gram_len {g} is not in {MIN_GRAM}..={MAX_GRAM}"));
    }
    if params.fallback_min_bits == 0 {
        return Err("fallback_min_bits is 0".into());
    }
    if blocks.is_empty() {
        return Err("no blocks to summarize".into());
    }
    let n = blocks.len() as u64;
    let budget_bytes = (params.budget_share * compressed_bytes as f64).floor() as u64;
    let fmin = params.fallback_min_bits;
    let fits = |k: usize| cost(n, (k + fmin) as u64, k as u64, g as u64) <= budget_bytes;
    if !fits(0) {
        return Err(format!(
            "a budget of {budget_bytes} bytes cannot hold {fmin} fallback bits for {n} blocks"
        ));
    }

    let mut literal_count: BTreeMap<u64, u64> = BTreeMap::new();
    for lit in training {
        let mut grams = Vec::new();
        for_each_gram(lit, g, |k| grams.push(k));
        grams.sort_unstable();
        grams.dedup();
        for k in grams {
            *literal_count.entry(k).or_default() += 1;
        }
    }
    let keys: Vec<u64> = literal_count.keys().copied().collect();
    let candidates = KeySet::new(&keys, g);

    let mut df = vec![0u64; keys.len()];
    let mut in_block = vec![false; keys.len()];
    let mut touched = Vec::new();
    for values in blocks {
        for v in values {
            for_each_gram(v, g, |key| {
                if let Some(i) = candidates.get(key)
                    && let Some(seen) = in_block.get_mut(i)
                    && !*seen
                {
                    *seen = true;
                    touched.push(i);
                }
            });
        }
        for i in touched.drain(..) {
            if let (Some(d), Some(seen)) = (df.get_mut(i), in_block.get_mut(i)) {
                *d += 1;
                *seen = false;
            }
        }
    }

    let mut ranked: Vec<(u64, u64)> = literal_count
        .iter()
        .zip(&df)
        .filter_map(|((&key, &count), &d)| {
            let benefit = count * (n - d);
            (benefit > 0).then_some((benefit, key))
        })
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

    // `fits` is monotone in k: find the largest k that fits.
    let (mut lo, mut hi) = (0usize, ranked.len());
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    let k = lo;
    let per_block_bytes = (budget_bytes - (k * g) as u64) / n;
    let total_bits = (per_block_bytes * 8) as usize;
    let mut learned: Vec<u64> = ranked.iter().take(k).map(|&(_, key)| key).collect();
    learned.sort_unstable();
    let layout = Layout {
        gram_len: g,
        learned,
        fallback_bits: total_bits - k,
    };

    let builder = Builder::new(&layout);
    let summaries = blocks.iter().map(|b| builder.block(b)).collect();
    Ok(Built {
        layout,
        summaries,
        candidates: keys.len(),
        budget_bytes,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::*;

    fn grams(s: &str) -> Vec<u64> {
        let mut out = Vec::new();
        for_each_gram(s.as_bytes(), 3, |k| out.push(k));
        out.sort_unstable();
        out.dedup();
        out
    }

    fn values(vs: &[&str]) -> Vec<Vec<u8>> {
        vs.iter().map(|v| v.as_bytes().to_vec()).collect()
    }

    fn pruned(layout: &Layout, block: &[&str], literal: &str) -> bool {
        let s = BlockSummary::build(&values(block), layout);
        !survives(&s, &layout.literal_bits(literal.as_bytes()))
    }

    #[test]
    fn absent_literal_with_every_gram_clear_is_pruned() {
        let layout = Layout {
            gram_len: 3,
            learned: grams("needle"),
            fallback_bits: 64,
        };
        assert!(pruned(&layout, &["other words"], "needle"));
        assert!(!pruned(&layout, &["a needle here"], "needle"));
    }

    #[test]
    fn pruned_when_any_gram_is_clear_in_either_order() {
        // Only the grams of "needle" are learned: "zzn"/"zne" and "lez"/"ezz"
        // fall back to hashed bits, all of them clear in a block whose only
        // value is "needle", whose grams set only their own learned bits.
        let layout = Layout {
            gram_len: 3,
            learned: grams("needle"),
            fallback_bits: 64,
        };
        assert!(pruned(&layout, &["needle"], "zzneedle"));
        assert!(pruned(&layout, &["needle"], "needlezz"));
    }

    #[test]
    fn unseen_literal_is_pruned_through_the_fallback_region() {
        let built = build(
            &values(&["alpha"]),
            &[values(&["alpha beta"]), values(&["gamma delta"])],
            &Params {
                gram_len: 3,
                budget_share: 1.0,
                fallback_min_bits: 256,
            },
            1024,
        )
        .expect("build");
        for g in grams("omega") {
            assert!(!built.layout.learned.contains(&g), "omega is unseen");
        }
        let bits = built.layout.literal_bits(b"omega");
        assert!(!survives(&built.summaries[0], &bits));
        assert!(!survives(&built.summaries[1], &bits));
        assert!(survives(
            &built.summaries[1],
            &built.layout.literal_bits(b"delta")
        ));
    }

    #[test]
    fn short_literal_is_never_pruned() {
        let layout = Layout {
            gram_len: 3,
            learned: Vec::new(),
            fallback_bits: 64,
        };
        assert!(layout.literal_bits(b"ab").is_empty());
        assert!(!pruned(&layout, &["zzzz"], "ab"));
    }

    #[test]
    fn build_learns_only_training_grams_within_budget() {
        let blocks: Vec<Vec<Vec<u8>>> = (0..8)
            .map(|i| values(&[&format!("row {i} payload"), "common text"]))
            .collect();
        let training = values(&["row 3 payload", "zzzz"]);
        let built = build(
            &training,
            &blocks,
            &Params {
                gram_len: 3,
                budget_share: 0.5,
                fallback_min_bits: 16,
            },
            1000,
        )
        .expect("build");
        let allowed: HashSet<u64> = grams("row 3 payload")
            .into_iter()
            .chain(grams("zzzz"))
            .collect();
        assert!(!built.layout.learned.is_empty());
        for g in &built.layout.learned {
            assert!(allowed.contains(g));
        }
        // "row" is in every block: no benefit, never learned.
        assert!(!built.layout.learned.contains(&grams("row")[0]));
        assert!(built.summary_bytes() <= built.budget_bytes);
        assert!(built.layout.fallback_bits >= 16);
        for (b, s) in blocks.iter().zip(&built.summaries) {
            assert_eq!(s, &BlockSummary::build(b, &built.layout));
        }
    }

    #[test]
    fn build_refuses_a_budget_below_the_fallback_floor() {
        let err = build(
            &values(&["abc"]),
            &[values(&["abc"]), values(&["abc"]), values(&["abc"])],
            &Params {
                gram_len: 3,
                budget_share: 0.01,
                fallback_min_bits: 64,
            },
            100,
        )
        .unwrap_err();
        assert!(err.contains("cannot hold"), "{err}");
    }

    fn byte_string() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(prop::sample::select(b"abcde xy".to_vec()), 0..24)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// The pruning rule is exactly "some gram's bit is clear", with the
        /// block's bits computed independently of `BlockSummary::build`, and
        /// a block holding the literal is never pruned.
        #[test]
        fn survives_is_exactly_the_subset_test(
            block in prop::collection::vec(byte_string(), 0..6),
            literal in byte_string(),
            learn_from in prop::collection::vec(byte_string(), 0..4),
            fallback_bits in 1usize..48,
        ) {
            let mut learned: Vec<u64> = Vec::new();
            for l in &learn_from {
                for_each_gram(l, 3, |k| learned.push(k));
            }
            learned.sort_unstable();
            learned.dedup();
            let layout = Layout { gram_len: 3, learned, fallback_bits };
            let summary = BlockSummary::build(&block, &layout);

            let mut set = HashSet::new();
            for v in &block {
                for_each_gram(v, 3, |k| { set.insert(layout.bit_of(k)); });
            }
            let mut every_bit_set = true;
            for_each_gram(&literal, 3, |k| every_bit_set &= set.contains(&layout.bit_of(k)));

            let kept = survives(&summary, &layout.literal_bits(&literal));
            prop_assert_eq!(kept, every_bit_set);
            let held = block.iter().any(|v| {
                literal.is_empty() || v.windows(literal.len()).any(|w| w == literal.as_slice())
            });
            if held {
                prop_assert!(kept, "false negative");
            }
        }
    }
}
