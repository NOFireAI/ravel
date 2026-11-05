//! The per-block learned string summary (issue #3710, ADR-3692 Plan row T12):
//! a shared vocabulary of features selected from a *seen* predicate workload,
//! with each block storing its own fixed-size bitset over that vocabulary.
//!
//! This is a different shape than the REI arm's
//! [`crate::logbody::grams::GramIndex`] (issue #3709): `GramIndex` is one
//! shared postings list, gram to the blocks holding it, built once over a
//! corpus. Here every block's summary is a bitset over the shared vocabulary
//! and nothing else; a query is answered from one block's bits alone, with no
//! cross-block structure to consult. That is the "genuinely per-block
//! self-contained" shape issue #3710 asks for, as opposed to a shared index
//! wearing a per-block label.
//!
//! Two kinds of query, two kinds of feature:
//!
//! - **Equality** (`col = 'literal'`): the feature is the whole literal
//!   value. A block's bit is set iff the block holds a value byte-equal to
//!   it. The vocabulary only ever contains literals drawn from the seen
//!   workload (an equality fingerprint has no way to generalize to a value it
//!   has never been told about): a query literal outside the vocabulary
//!   always survives, unconditionally.
//! - **`LIKE '%literal%'`**: the literal is decomposed into fixed-length
//!   byte grams ([`decompose`]); the feature is one gram. A block's bit is
//!   set iff some value in the block contains that gram as a byte substring.
//!   A query literal survives unless every one of its grams is both in the
//!   vocabulary and set in the block's bits; a query whose grams happen to
//!   all appear in vocabulary entries built from *other*, seen literals can
//!   still be pruned, which is the generalization the hypothesis (and this
//!   whole report) is testing.
//!
//! Conservativeness does not depend on which features made it into the
//! vocabulary: [`BlockSummary::build`] only ever sets a bit when it finds the
//! feature truly present in that block's own values, and every query falls
//! back to "survives" the moment it meets a feature the vocabulary does not
//! have. So for *any* vocabulary (not just the one [`Vocabulary::build`]
//! would have chosen), a block that truly holds a matching value is never
//! pruned. [`tests::conservative_for_any_vocabulary`] states that property
//! directly over randomized blocks, vocabularies and literals.

use std::collections::BTreeSet;

/// The shortest gram a `LIKE` feature is built from. Below this a substring
/// index has no selectivity worth the bit it costs (mirrors
/// [`crate::logbody::grams::MIN_GRAM`]).
pub const MIN_GRAM: usize = 3;

/// One vocabulary entry: an equality literal or a `LIKE` gram. Order is the
/// bit index into every [`BlockSummary`] built against the enclosing
/// [`Vocabulary`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Feature {
    Equals(Vec<u8>),
    Gram(Vec<u8>),
}

/// The summary's own parameters: pre-registered per subject, never inferred
/// at run time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Params {
    /// The fixed gram length `LIKE` literals are decomposed into.
    pub gram_len: usize,
    /// A candidate gram is dropped when more than this share of blocks
    /// contain it: a near-universal gram buys no pruning and only spends
    /// budget.
    pub df_threshold: f64,
    /// The vocabulary's serialized byte cap, as a share of the column's
    /// compressed bytes. Candidates are dropped rarest-kept-first, i.e. the
    /// highest-document-frequency survivors are dropped first, until the
    /// vocabulary fits.
    pub budget_share: f64,
}

/// Splits `literal` into the grams a `LIKE` query or a vocabulary build tests:
/// non-overlapping information is not required, so this is the plain sliding
/// window of `gram_len` bytes; a literal shorter than `gram_len` is its own
/// single (short) gram, exactly like [`crate::logbody::grams::Pattern`]'s
/// literal runs are tested whole when they do not reach a gram length.
pub fn decompose(literal: &[u8], gram_len: usize) -> Vec<Vec<u8>> {
    if literal.is_empty() {
        return Vec::new();
    }
    if literal.len() <= gram_len {
        return vec![literal.to_vec()];
    }
    literal.windows(gram_len).map(|w| w.to_vec()).collect()
}

fn contains(value: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    value.windows(needle.len()).any(|w| w == needle)
}

/// The shared feature set, built once per subject from the seen workload and
/// the corpus's real per-block values.
#[derive(Clone, Debug, Default)]
pub struct Vocabulary {
    /// Bit order. Sorted so two builds over the same inputs agree byte for
    /// byte, which [`tests::vocabulary_build_is_deterministic`] pins.
    features: Vec<Feature>,
}

/// What a candidate gram cost to evaluate, for the budget cut.
struct Candidate {
    feature: Feature,
    df_blocks: usize,
}

impl Vocabulary {
    /// Builds the vocabulary from the seen equality literals and seen `LIKE`
    /// literals only: an unseen literal and every one of its grams are never
    /// candidates, by construction, which is what makes the unseen-predicate
    /// figures an honest test of generalization rather than a vocabulary
    /// that was quietly handed the answer.
    ///
    /// `blocks` are the corpus's real per-block column values, read back
    /// through the production block cuts (never a naive re-chunking): a
    /// candidate's document frequency and a block's bits must agree on what
    /// "the same block" means.
    pub fn build(
        seen_equals: &[Vec<u8>],
        seen_like: &[Vec<u8>],
        blocks: &[Vec<Vec<u8>>],
        params: &Params,
        compressed_bytes: u64,
    ) -> Vocabulary {
        let n_blocks = blocks.len().max(1);
        let mut equals: BTreeSet<Vec<u8>> = BTreeSet::new();
        for lit in seen_equals {
            equals.insert(lit.clone());
        }

        let mut grams: BTreeSet<Vec<u8>> = BTreeSet::new();
        for lit in seen_like {
            for g in decompose(lit, params.gram_len) {
                grams.insert(g);
            }
        }

        // Document frequency rule: drop a gram held by more than
        // `df_threshold` of blocks. Equality literals keep no such rule --
        // the workload named them explicitly and there are never enough of
        // them to need a cut on frequency alone.
        let mut candidates: Vec<Candidate> = Vec::new();
        for lit in &equals {
            candidates.push(Candidate {
                feature: Feature::Equals(lit.clone()),
                df_blocks: blocks.iter().filter(|b| b.iter().any(|v| v == lit)).count(),
            });
        }
        for g in &grams {
            let df = blocks
                .iter()
                .filter(|b| b.iter().any(|v| contains(v, g)))
                .count();
            if (df as f64) / (n_blocks as f64) <= params.df_threshold {
                candidates.push(Candidate {
                    feature: Feature::Gram(g.clone()),
                    df_blocks: df,
                });
            }
        }

        // Budget cut: rarest first (lowest document frequency buys the most
        // selectivity per bit); a feature whose own bytes alone would push
        // the vocabulary over the cap is dropped and the next cheaper one is
        // tried, mirroring GramIndex's rarest-first-under-budget rule
        // (crate::logbody::grams).
        candidates.sort_by(|a, b| {
            a.df_blocks
                .cmp(&b.df_blocks)
                .then_with(|| a.feature.cmp(&b.feature))
        });
        let cap_bytes = (params.budget_share * compressed_bytes as f64)
            .floor()
            .max(0.0) as u64;
        let mut features = Vec::new();
        let mut bytes_used: u64 = 0;
        for c in candidates {
            let cost = feature_bytes(&c.feature);
            if bytes_used + cost > cap_bytes {
                continue;
            }
            bytes_used += cost;
            features.push(c.feature);
        }
        features.sort();
        Vocabulary { features }
    }

    pub fn feature_index(&self, f: &Feature) -> Option<usize> {
        self.features.binary_search(f).ok()
    }

    pub fn len(&self) -> usize {
        self.features.len()
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    /// The vocabulary's own encoded size: a length-prefixed byte string per
    /// feature plus a one-byte kind tag, the layout [`FEATURE_LAYOUT`]
    /// states. This is reported alongside, never instead of, the per-block
    /// bitset bytes that [`BlockSummary::bytes`] sums: both make up
    /// `summary_bytes` in the report.
    pub fn bytes(&self) -> usize {
        self.features
            .iter()
            .map(|f| {
                let lit = match f {
                    Feature::Equals(l) | Feature::Gram(l) => l,
                };
                1 + 4 + lit.len()
            })
            .sum()
    }
}

fn feature_bytes(f: &Feature) -> u64 {
    let lit = match f {
        Feature::Equals(l) | Feature::Gram(l) => l,
    };
    (1 + 4 + lit.len()) as u64
}

pub const FEATURE_LAYOUT: &str = "per vocabulary feature, a one-byte kind tag (0 = equals literal, \
1 = like gram), a little-endian u32 byte length, then the literal bytes; summary_bytes is this plus, \
per block, ceil(vocabulary_len / 8) bitset bytes.";

/// One block's bitset over a [`Vocabulary`]: bit `i` is set iff this block's
/// own real values truly contain feature `i` (equals: byte-equal; gram: byte
/// substring). This is the only place a bit is ever set, which is what makes
/// the summary sound regardless of how the vocabulary was chosen.
#[derive(Clone, Debug)]
pub struct BlockSummary {
    bits: Vec<u8>,
}

impl BlockSummary {
    pub fn build(values: &[Vec<u8>], vocab: &Vocabulary) -> BlockSummary {
        let mut bits = vec![0u8; vocab.len().div_ceil(8)];
        for (i, f) in vocab.features.iter().enumerate() {
            let hit = match f {
                Feature::Equals(lit) => values.iter().any(|v| v == lit),
                Feature::Gram(g) => values.iter().any(|v| contains(v, g)),
            };
            if hit {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        BlockSummary { bits }
    }

    fn bit(&self, i: usize) -> bool {
        self.bits
            .get(i / 8)
            .is_some_and(|byte| byte & (1 << (i % 8)) != 0)
    }

    pub fn bytes(&self) -> usize {
        self.bits.len()
    }
}

/// Whether `block` (its summary, over `vocab`) may hold a value exactly equal
/// to `literal`: `false` only when `literal` is in the vocabulary and the
/// block's bit for it is clear, which [`BlockSummary::build`] only ever does
/// when the block's own values truly never equal it.
pub fn survives_equals(summary: &BlockSummary, vocab: &Vocabulary, literal: &[u8]) -> bool {
    match vocab.feature_index(&Feature::Equals(literal.to_vec())) {
        Some(i) => summary.bit(i),
        None => true,
    }
}

/// Whether `block` (its summary, over `vocab`) may hold a value containing
/// `literal` as a substring (`LIKE '%literal%'`). Decomposes `literal` into
/// grams ([`decompose`], at `gram_len`); survives unless every gram is both
/// in the vocabulary and clear in this block's bits. A gram the vocabulary
/// never saw always survives, which includes every gram of a literal whose
/// length is under `gram_len` and was never a seen `LIKE` literal itself.
pub fn survives_like(
    summary: &BlockSummary,
    vocab: &Vocabulary,
    literal: &[u8],
    gram_len: usize,
) -> bool {
    let grams = decompose(literal, gram_len);
    if grams.is_empty() {
        return true;
    }
    for g in &grams {
        match vocab.feature_index(&Feature::Gram(g.clone())) {
            Some(i) => {
                if !summary.bit(i) {
                    return false;
                }
            }
            None => return true,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_value() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(prop::sample::select(vec![b'a', b'b', b'c', b'd']), 0..12)
    }

    fn arb_block() -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop::collection::vec(arb_value(), 0..6)
    }

    fn arb_feature() -> impl Strategy<Value = Feature> {
        prop_oneof![
            arb_value().prop_map(Feature::Equals),
            arb_value().prop_map(Feature::Gram),
        ]
    }

    fn arb_vocab() -> impl Strategy<Value = Vocabulary> {
        prop::collection::vec(arb_feature(), 0..10).prop_map(|mut features| {
            features.sort();
            features.dedup();
            Vocabulary { features }
        })
    }

    proptest! {
        /// However the vocabulary was chosen (not just the one `build` would
        /// pick): a block whose real values truly hold a value equal to, or
        /// containing, a literal is never pruned by that block's own
        /// summary. This is the conservativeness guarantee issue #3710
        /// requires, stated over an adversarial vocabulary so it cannot pass
        /// by accident of `build`'s own selection rule.
        #[test]
        fn conservative_for_any_vocabulary(
            block in arb_block(),
            vocab in arb_vocab(),
            literal in arb_value(),
            gram_len in 1usize..6,
        ) {
            let summary = BlockSummary::build(&block, &vocab);
            if block.iter().any(|v| v == &literal) {
                prop_assert!(survives_equals(&summary, &vocab, &literal));
            }
            if block.iter().any(|v| contains(v, &literal)) {
                prop_assert!(survives_like(&summary, &vocab, &literal, gram_len));
            }
        }
    }

    #[test]
    fn vocabulary_build_is_deterministic() {
        let blocks = vec![
            vec![b"needle".to_vec(), b"other".to_vec()],
            vec![b"haystack".to_vec()],
        ];
        let params = Params {
            gram_len: 3,
            df_threshold: 1.0,
            budget_share: 1.0,
        };
        let a = Vocabulary::build(
            &[b"needle".to_vec()],
            &[b"stack".to_vec()],
            &blocks,
            &params,
            10_000,
        );
        let b = Vocabulary::build(
            &[b"needle".to_vec()],
            &[b"stack".to_vec()],
            &blocks,
            &params,
            10_000,
        );
        assert_eq!(a.features, b.features);
        assert!(
            a.feature_index(&Feature::Equals(b"needle".to_vec()))
                .is_some()
        );
    }

    #[test]
    fn equals_vocabulary_never_grows_from_corpus_values() {
        // A value that is common in the corpus but was never a seen equality
        // literal must never become an equality feature: equality has no
        // unsupervised signal to train on, only the named workload.
        let blocks = vec![vec![b"common".to_vec(); 20]];
        let params = Params {
            gram_len: 3,
            df_threshold: 1.0,
            budget_share: 1.0,
        };
        let vocab = Vocabulary::build(&[], &[], &blocks, &params, 10_000);
        assert!(vocab.is_empty());
    }

    #[test]
    fn df_threshold_drops_a_near_universal_gram() {
        // "aaa" occurs in every block (df share 1.0) and must be dropped as
        // non-discriminating even though it is a substring of the seen LIKE
        // literal; "XXX" occurs in only one of the two blocks (df share
        // 0.5) and clears the same bar.
        let blocks = vec![vec![b"aaaXXX".to_vec()], vec![b"aaaYYY".to_vec()]];
        let params = Params {
            gram_len: 3,
            df_threshold: 0.6,
            budget_share: 1.0,
        };
        let vocab = Vocabulary::build(&[], &[b"aaaXXX".to_vec()], &blocks, &params, 10_000);
        assert!(
            vocab
                .feature_index(&Feature::Gram(b"aaa".to_vec()))
                .is_none()
        );
        assert!(
            vocab
                .feature_index(&Feature::Gram(b"XXX".to_vec()))
                .is_some()
        );
    }

    #[test]
    fn budget_share_caps_vocabulary_bytes() {
        let blocks = vec![vec![b"abcdefghij".to_vec()]];
        let seen_like = vec![b"abcdefghij".to_vec()];
        let params = Params {
            gram_len: 3,
            df_threshold: 1.0,
            budget_share: 1.0,
        };
        let unlimited = Vocabulary::build(&[], &seen_like, &blocks, &params, 1_000_000);
        assert!(unlimited.len() > 1);
        let starved = Vocabulary::build(&[], &seen_like, &blocks, &params, 0);
        assert_eq!(starved.len(), 0);
    }

    #[test]
    fn unseen_literal_outside_vocabulary_always_survives() {
        let blocks = vec![vec![b"abcdef".to_vec()]];
        let params = Params {
            gram_len: 3,
            df_threshold: 1.0,
            budget_share: 1.0,
        };
        let vocab = Vocabulary::build(&[b"abcdef".to_vec()], &[], &blocks, &params, 10_000);
        let summary = BlockSummary::build(&blocks[0], &vocab);
        // "zzz" never occurs anywhere and was never seen: not in vocabulary,
        // so it must survive unconditionally (honest 100% FP, not a crash or
        // a false "pruned").
        assert!(survives_equals(&summary, &vocab, b"zzzzzz"));
    }
}
