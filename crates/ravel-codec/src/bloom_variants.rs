//! Test-only alternatives to [`BloomBuilder`], measured against it for issue
//! #3228. None of these is reachable from production code; they exist so the
//! adoption decision rests on numbers from one key stream.
//!
//! - A: [`BloomBuilder`] with `finish_exact`, the baseline.
//! - B: [`FoldBuilder`], no staging: hash every insert, size from the insert
//!   count, then fold block halves while the fill-rate FPP estimate stays at
//!   or below 1%.
//! - C: [`SortDedupBuilder`], hash every insert, sort and dedup the triples,
//!   then size exactly as `finish_exact` does.
//! - D: [`FxStagedBuilder`], today's full-key staging set with an FxHash-style
//!   hasher in place of SipHash.
//!
//! C and D must emit bytes identical to A; B emits a different (power of two)
//! geometry and must keep the no-false-negative guarantee.

#![allow(clippy::expect_used)]

use std::alloc::System;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hasher};

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

use crate::bloom::{
    BITS_PER_ELEM, BLOCK_BITS, BloomBuilder, BloomView, K, KeyHash, get_bit, key_hash, probe_bits,
    set_bit,
};
use crate::varint::{get_uvarint, put_uvarint};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Bytes in one 512-bit block.
const BLOCK_BYTES: usize = (BLOCK_BITS / 8) as usize;
/// The fill-rate FPP estimate B folds down to and no further.
const FOLD_TARGET_FPP: f64 = 0.01;

type Triple = (u64, u64, u64);

fn triple(h: &KeyHash) -> Triple {
    (h.block, h.g1, h.g2)
}

fn set_triple(bits: &mut [u8], t: Triple, block_count: u64) {
    let h = KeyHash {
        block: t.0,
        g1: t.1,
        g2: t.2,
    };
    for bit in probe_bits(&h, K, block_count) {
        set_bit(bits, bit);
    }
}

/// `finish_exact`'s sizing: `max(512, ceil(9.585 n))` rounded up to a
/// multiple of 512.
fn exact_m_bits(distinct: usize) -> u64 {
    let target = (distinct as f64 * BITS_PER_ELEM).ceil() as u64;
    target.max(BLOCK_BITS).div_ceil(BLOCK_BITS) * BLOCK_BITS
}

fn entry(seed: u64, m_bits: u64, bits: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, m_bits);
    out.push(K);
    out.extend_from_slice(&seed.to_le_bytes());
    out.extend_from_slice(bits);
    out
}

/// Serializes a set of distinct triples with `finish_exact`'s geometry.
fn exact_entry<'a>(
    seed: u64,
    triples: impl ExactSizeIterator<Item = &'a Triple> + Clone,
) -> Vec<u8> {
    let m_bits = exact_m_bits(triples.len());
    let block_count = m_bits / BLOCK_BITS;
    let mut bits = vec![0u8; (m_bits / 8) as usize];
    for t in triples {
        set_triple(&mut bits, *t, block_count);
    }
    entry(seed, m_bits, &bits)
}

/// `(m_bits, bit array)` of a serialized entry.
fn entry_bits(entry: &[u8]) -> (u64, &[u8]) {
    let mut pos = 0;
    let m_bits = get_uvarint(entry, &mut pos).expect("m_bits");
    (m_bits, &entry[pos + 1 + 8..])
}

/// The fill-rate FPP estimate: the mean over blocks of the block's fill
/// fraction to the power `k`.
fn fill_fpp(bits: &[u8]) -> f64 {
    let blocks = bits.len() / BLOCK_BYTES;
    let sum: f64 = bits
        .chunks_exact(BLOCK_BYTES)
        .map(|b| {
            let ones: u32 = b.iter().map(|x| x.count_ones()).sum();
            (f64::from(ones) / BLOCK_BITS as f64).powi(i32::from(K))
        })
        .sum();
    sum / blocks as f64
}

// ---------------------------------------------------------------- variant B

/// Halves a filter of `block_count` blocks. The probe selects block
/// `block % block_count`, so at half the count block `j + z/2` lands on `j`:
/// the fold ORs the upper half onto the lower half.
fn fold_half(bits: &[u8]) -> Vec<u8> {
    let half = bits.len() / 2;
    bits[..half]
        .iter()
        .zip(&bits[half..])
        .map(|(a, b)| a | b)
        .collect()
}

/// Variant B: no staging. The bit array is sized from the insert count, which
/// the caller states up front (as the arrow-rs and ClickHouse folding builders
/// take an expected NDV), then folded at `finish`.
pub(crate) struct FoldBuilder {
    seed: u64,
    block_count: u64,
    bits: Vec<u8>,
}

impl FoldBuilder {
    pub(crate) fn new(seed: u64, inserts: usize) -> Self {
        let target = (inserts as f64 * BITS_PER_ELEM).ceil() as u64;
        let blocks = target
            .max(BLOCK_BITS)
            .div_ceil(BLOCK_BITS)
            .next_power_of_two();
        Self::with_blocks(seed, blocks)
    }

    fn with_blocks(seed: u64, block_count: u64) -> Self {
        FoldBuilder {
            seed,
            block_count,
            bits: vec![0u8; block_count as usize * BLOCK_BYTES],
        }
    }

    pub(crate) fn insert(&mut self, column_id: u32, token: &[u8]) {
        let h = key_hash(self.seed, column_id, token);
        set_triple(&mut self.bits, triple(&h), self.block_count);
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        while self.block_count > 1 {
            let folded = fold_half(&self.bits);
            if fill_fpp(&folded) > FOLD_TARGET_FPP {
                break;
            }
            self.bits = folded;
            self.block_count /= 2;
        }
        entry(self.seed, self.block_count * BLOCK_BITS, &self.bits)
    }
}

// ---------------------------------------------------------------- variant C

/// Variant C: hash every insert into a vector, then sort and dedup.
pub(crate) struct SortDedupBuilder {
    seed: u64,
    hashes: Vec<Triple>,
}

impl SortDedupBuilder {
    pub(crate) fn new(seed: u64) -> Self {
        SortDedupBuilder {
            seed,
            hashes: Vec::new(),
        }
    }

    pub(crate) fn insert(&mut self, column_id: u32, token: &[u8]) {
        let h = key_hash(self.seed, column_id, token);
        self.hashes.push(triple(&h));
    }

    pub(crate) fn finish_exact(mut self) -> Vec<u8> {
        self.hashes.sort_unstable();
        self.hashes.dedup();
        exact_entry(self.seed, self.hashes.iter())
    }
}

// ---------------------------------------------------------------- variant D

/// FxHash-style hasher: rotate, xor a word, multiply. The final rotate moves
/// the multiply's well-mixed high bits down to where hashbrown takes the
/// bucket index from.
#[derive(Default)]
pub(crate) struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let mut w = [0u8; 8];
            w.copy_from_slice(c);
            self.add(u64::from_le_bytes(w));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut w = [0u8; 8];
            w[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(w));
        }
    }

    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }

    fn finish(&self) -> u64 {
        self.hash.rotate_left(26)
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct FxBuildHasher;

impl BuildHasher for FxBuildHasher {
    type Hasher = FxHasher;
    fn build_hasher(&self) -> FxHasher {
        FxHasher::default()
    }
}

/// Variant D: [`BloomBuilder`]'s staging, full key bytes and full-key
/// equality, with [`FxBuildHasher`] in place of SipHash.
pub(crate) struct FxStagedBuilder {
    seed: u64,
    staged: HashSet<Box<[u8]>, FxBuildHasher>,
    scratch: Vec<u8>,
}

impl FxStagedBuilder {
    pub(crate) fn new(seed: u64) -> Self {
        FxStagedBuilder {
            seed,
            staged: HashSet::with_hasher(FxBuildHasher),
            scratch: Vec::new(),
        }
    }

    pub(crate) fn insert(&mut self, column_id: u32, token: &[u8]) {
        self.scratch.clear();
        self.scratch.extend_from_slice(&column_id.to_le_bytes());
        self.scratch.extend_from_slice(token);
        if !self.staged.contains(self.scratch.as_slice()) {
            self.staged.insert(self.scratch.as_slice().into());
        }
    }

    pub(crate) fn finish_exact(mut self) -> Vec<u8> {
        let mut triples: HashSet<Triple, FxBuildHasher> =
            HashSet::with_capacity_and_hasher(self.staged.len(), FxBuildHasher);
        for key in self.staged.drain() {
            let mut col = [0u8; 4];
            col.copy_from_slice(&key[..4]);
            let h = key_hash(self.seed, u32::from_le_bytes(col), &key[4..]);
            triples.insert(triple(&h));
        }
        exact_entry(self.seed, triples.iter())
    }
}

// ------------------------------------------------------------ build helpers

type Inserts = [(u32, Vec<u8>)];

fn build_a(seed: u64, inserts: &Inserts) -> Vec<u8> {
    let mut b = BloomBuilder::new(seed);
    for (c, t) in inserts {
        b.insert(*c, t);
    }
    b.finish_exact()
}

fn build_b(seed: u64, inserts: &Inserts) -> Vec<u8> {
    let mut b = FoldBuilder::new(seed, inserts.len());
    for (c, t) in inserts {
        b.insert(*c, t);
    }
    b.finish()
}

fn build_c(seed: u64, inserts: &Inserts) -> Vec<u8> {
    let mut b = SortDedupBuilder::new(seed);
    for (c, t) in inserts {
        b.insert(*c, t);
    }
    b.finish_exact()
}

fn build_d(seed: u64, inserts: &Inserts) -> Vec<u8> {
    let mut b = FxStagedBuilder::new(seed);
    for (c, t) in inserts {
        b.insert(*c, t);
    }
    b.finish_exact()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    /// The byte-identity check the C and D proptests run.
    fn same_as_a(build: fn(u64, &Inserts) -> Vec<u8>, seed: u64, inserts: &Inserts) -> bool {
        build(seed, inserts) == build_a(seed, inserts)
    }

    /// Wrong C: sizes from the insert count instead of the distinct count.
    fn wrong_c_insert_sized(seed: u64, inserts: &Inserts) -> Vec<u8> {
        let mut hashes: Vec<Triple> = inserts
            .iter()
            .map(|(c, t)| triple(&key_hash(seed, *c, t)))
            .collect();
        let m_bits = exact_m_bits(hashes.len());
        hashes.sort_unstable();
        hashes.dedup();
        let block_count = m_bits / BLOCK_BITS;
        let mut bits = vec![0u8; (m_bits / 8) as usize];
        for t in &hashes {
            set_triple(&mut bits, *t, block_count);
        }
        entry(seed, m_bits, &bits)
    }

    /// Wrong D: dedups on a fingerprint, here deliberately weak (the key
    /// length), so two keys sharing it drop one.
    fn wrong_d_fingerprint(seed: u64, inserts: &Inserts) -> Vec<u8> {
        let mut staged: HashMap<u64, (u32, Vec<u8>)> = HashMap::new();
        for (c, t) in inserts {
            staged.entry(4 + t.len() as u64).or_insert((*c, t.clone()));
        }
        let mut triples: HashSet<Triple> = HashSet::new();
        for (c, t) in staged.values() {
            triples.insert(triple(&key_hash(seed, *c, t)));
        }
        exact_entry(seed, triples.iter())
    }

    /// Adjacent-pair fold (Parquet SBBF placement): block j = old 2j | 2j+1.
    fn fold_adjacent(bits: &[u8]) -> Vec<u8> {
        bits.chunks_exact(2 * BLOCK_BYTES)
            .flat_map(|pair| {
                let (a, b) = pair.split_at(BLOCK_BYTES);
                a.iter().zip(b).map(|(x, y)| x | y).collect::<Vec<u8>>()
            })
            .collect()
    }

    /// Truncation: halves the array without remapping any block.
    fn fold_truncate(bits: &[u8]) -> Vec<u8> {
        bits[..bits.len() / 2].to_vec()
    }

    /// Folding a filter built at `2z` blocks equals building at `z` directly.
    fn fold_matches_direct(fold: fn(&[u8]) -> Vec<u8>, seed: u64, keys: &Inserts, z: u64) -> bool {
        let mut wide = FoldBuilder::with_blocks(seed, 2 * z);
        let mut narrow = FoldBuilder::with_blocks(seed, z);
        for (c, t) in keys {
            wide.insert(*c, t);
            narrow.insert(*c, t);
        }
        fold(&wide.bits) == narrow.bits
    }

    fn fixed_keys(n: u32, len: usize) -> Vec<(u32, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let mut t = format!("k{i:08}").into_bytes();
                t.resize(len.max(t.len()), b'x');
                (i % 8, t)
            })
            .collect()
    }

    fn stream_with_dups(pool: &Inserts, picks: &[usize]) -> Vec<(u32, Vec<u8>)> {
        picks
            .iter()
            .map(|&i| pool[i % pool.len()].clone())
            .collect()
    }

    fn pool_strategy() -> impl Strategy<Value = Vec<(u32, Vec<u8>)>> {
        prop_oneof![
            proptest::collection::vec(
                (
                    0u32..8u32,
                    proptest::collection::vec(any::<u8>(), 0..80usize)
                ),
                1..54usize
            ),
            proptest::collection::vec(
                (
                    0u32..8u32,
                    proptest::collection::vec(any::<u8>(), 0..80usize)
                ),
                54..600usize
            ),
        ]
    }

    proptest! {
        #[test]
        fn c_and_d_are_byte_identical_to_a(
            pool in pool_strategy(),
            picks in proptest::collection::vec(any::<usize>(), 0..1600usize),
            seed in any::<u64>(),
        ) {
            let inserts = stream_with_dups(&pool, &picks);
            prop_assert!(same_as_a(build_c, seed, &inserts));
            prop_assert!(same_as_a(build_d, seed, &inserts));
        }

        #[test]
        fn b_has_no_false_negatives(
            pool in pool_strategy(),
            picks in proptest::collection::vec(any::<usize>(), 1..1600usize),
            seed in any::<u64>(),
        ) {
            let inserts = stream_with_dups(&pool, &picks);
            let bytes = build_b(seed, &inserts);
            let v = BloomView::parse(&bytes).expect("B emits a power-of-two filter");
            for (c, t) in &inserts {
                prop_assert!(v.may_contain(*c, t));
            }
        }

        #[test]
        fn folding_2z_equals_building_at_z(
            keys in proptest::collection::vec(
                (0u32..8u32, proptest::collection::vec(any::<u8>(), 0..80usize)),
                0..400usize),
            z_log in 0u32..7,
            seed in any::<u64>(),
        ) {
            prop_assert!(fold_matches_direct(fold_half, seed, &keys, 1u64 << z_log));
        }
    }

    #[test]
    fn fold_property_rejects_adjacent_pair_and_truncation() {
        let keys = fixed_keys(200, 12);
        assert!(fold_matches_direct(fold_half, 1, &keys, 4));
        assert!(!fold_matches_direct(fold_adjacent, 1, &keys, 4));
        assert!(!fold_matches_direct(fold_truncate, 1, &keys, 4));
    }

    #[test]
    fn identity_check_rejects_insert_sizing_and_fingerprint_dedup() {
        // 100 distinct keys, each inserted three times: insert-count sizing
        // reads 300 keys.
        let pool = fixed_keys(100, 12);
        let picks: Vec<usize> = (0..300).collect();
        let inserts = stream_with_dups(&pool, &picks);
        assert!(same_as_a(build_c, 5, &inserts));
        assert!(same_as_a(build_d, 5, &inserts));
        assert!(!same_as_a(wrong_c_insert_sized, 5, &inserts));
        // Every key has the same length, so the weak fingerprint collides.
        assert!(!same_as_a(wrong_d_fingerprint, 5, &inserts));
    }

    #[test]
    fn fold_stops_at_the_target_estimate() {
        // 30x duplicates: sized for 30000 keys, holding 1000.
        let pool = fixed_keys(1000, 16);
        let picks: Vec<usize> = (0..30_000).collect();
        let inserts = stream_with_dups(&pool, &picks);
        let bytes = build_b(9, &inserts);
        let (m_bits, bits) = entry_bits(&bytes);
        assert!(fill_fpp(bits) <= FOLD_TARGET_FPP);
        assert!(m_bits < FoldBuilder::new(9, inserts.len()).block_count * BLOCK_BITS);
        if m_bits > BLOCK_BITS {
            assert!(fill_fpp(&fold_half(bits)) > FOLD_TARGET_FPP);
        }
    }

    // ------------------------------------------------------- measurement

    struct SplitMix64(u64);

    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn token(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    const RNG_SEED: u64 = 0x3228_b100_f17e_5eed;
    const BLOOM_SEED: u64 = 0x5eed;
    const REPS: usize = 11;
    const PROBES: usize = 2_000_000;
    const NS: [usize; 3] = [1000, 16_000, 64_000];
    const DUPS: [usize; 3] = [1, 3, 30];
    const VARIANTS: [char; 4] = ['A', 'B', 'C', 'D'];

    /// `n` distinct keys, 4 to 64 bytes, column ids from 8, each inserted
    /// `dup` times, shuffled.
    fn key_stream(rng: &mut SplitMix64, n: usize, dup: usize) -> Vec<(u32, Vec<u8>)> {
        let mut seen: HashSet<(u32, Vec<u8>)> = HashSet::with_capacity(n);
        let mut keys = Vec::with_capacity(n);
        while keys.len() < n {
            let col = rng.below(8) as u32;
            let len = 4 + rng.below(61) as usize;
            let k = (col, rng.token(len));
            if seen.insert(k.clone()) {
                keys.push(k);
            }
        }
        let mut stream: Vec<(u32, Vec<u8>)> = Vec::with_capacity(n * dup);
        for _ in 0..dup {
            stream.extend(keys.iter().cloned());
        }
        for i in (1..stream.len()).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            stream.swap(i, j);
        }
        stream
    }

    fn live(region: &Region<'_, System>) -> i64 {
        let s = region.change();
        s.bytes_allocated as i64 - s.bytes_deallocated as i64
    }

    /// Live heap bytes after the last insert, and the finished entry.
    fn heap_and_entry(v: char, inserts: &Inserts) -> (i64, Vec<u8>) {
        macro_rules! measure {
            ($b:expr, $finish:ident) => {{
                let region = Region::new(GLOBAL);
                let mut b = $b;
                for (c, t) in inserts {
                    b.insert(*c, t);
                }
                let bytes = live(&region);
                (bytes, b.$finish())
            }};
        }
        match v {
            'A' => measure!(BloomBuilder::new(BLOOM_SEED), finish_exact),
            'B' => measure!(FoldBuilder::new(BLOOM_SEED, inserts.len()), finish),
            'C' => measure!(SortDedupBuilder::new(BLOOM_SEED), finish_exact),
            _ => measure!(FxStagedBuilder::new(BLOOM_SEED), finish_exact),
        }
    }

    fn build(v: char, inserts: &Inserts) -> Vec<u8> {
        match v {
            'A' => build_a(BLOOM_SEED, inserts),
            'B' => build_b(BLOOM_SEED, inserts),
            'C' => build_c(BLOOM_SEED, inserts),
            _ => build_d(BLOOM_SEED, inserts),
        }
    }

    /// Absent probe keys: tokens of 65 to 72 bytes, never inserted (inserted
    /// tokens are 4 to 64 bytes).
    fn probe_keys(rng: &mut SplitMix64) -> Vec<(u32, Vec<u8>)> {
        (0..PROBES)
            .map(|_| {
                let col = rng.below(8) as u32;
                let len = 65 + rng.below(8) as usize;
                (col, rng.token(len))
            })
            .collect()
    }

    /// Realised FPR over precomputed probe hashes. Equivalent to
    /// `BloomView::may_contain` per key; the report test pins that on a
    /// prefix of the probes.
    fn realised_fpr(entry: &[u8], probes: &[Triple]) -> f64 {
        let (m_bits, bits) = entry_bits(entry);
        let block_count = m_bits / BLOCK_BITS;
        let hits = probes
            .iter()
            .filter(|t| {
                let h = KeyHash {
                    block: t.0,
                    g1: t.1,
                    g2: t.2,
                };
                probe_bits(&h, K, block_count).all(|bit| get_bit(bits, bit))
            })
            .count();
        hits as f64 / probes.len() as f64
    }

    fn view(v: char, entry: &[u8]) -> BloomView<'_> {
        if v == 'B' {
            BloomView::parse(entry).expect("parse")
        } else {
            BloomView::parse_exact(entry).expect("parse_exact")
        }
    }

    struct Fig {
        n: usize,
        dup: usize,
        v: char,
        metric: &'static str,
        value: f64,
    }

    fn stats_alloc_cross_checks() {
        let region = Region::new(GLOBAL);
        let v = std::hint::black_box(Vec::<u64>::with_capacity(1000));
        let got = live(&region);
        drop(v);
        println!("cross-check: Vec::<u64>::with_capacity(1000) live bytes = {got} (want 8000)");
        assert_eq!(got, 8000, "stats_alloc cross-check 1");

        let region = Region::new(GLOBAL);
        let mut v = Vec::<u64>::new();
        for i in 0..1000u64 {
            v.push(std::hint::black_box(i));
        }
        let got = live(&region);
        let cap = v.capacity();
        drop(v);
        println!(
            "cross-check: Vec<u64> pushed to capacity {cap} live bytes = {got} (want {})",
            8 * cap
        );
        assert_eq!(got, 8 * cap as i64, "stats_alloc cross-check 2");
    }

    #[test]
    #[ignore = "measurement report; run explicitly, alone, with --test-threads=1"]
    fn bloom_variants_report() {
        stats_alloc_cross_checks();
        println!(
            "rng seed {RNG_SEED:#x}, bloom seed {BLOOM_SEED:#x}, reps {REPS}, probes {PROBES}"
        );

        let mut rng = SplitMix64(RNG_SEED);
        let probe_set = probe_keys(&mut rng);
        let probes: Vec<Triple> = probe_set
            .iter()
            .map(|(c, t)| triple(&key_hash(BLOOM_SEED, *c, t)))
            .collect();

        let mut figs: Vec<Fig> = Vec::new();
        for &n in &NS {
            for &dup in &DUPS {
                let inserts = key_stream(&mut rng, n, dup);
                let mut times: Vec<Vec<f64>> = vec![Vec::with_capacity(REPS); VARIANTS.len()];
                for _ in 0..REPS {
                    for (i, &v) in VARIANTS.iter().enumerate() {
                        let start = std::time::Instant::now();
                        let e = build(v, &inserts);
                        times[i].push(start.elapsed().as_secs_f64() * 1e6);
                        std::hint::black_box(e);
                    }
                }
                for (i, &v) in VARIANTS.iter().enumerate() {
                    let t = &mut times[i];
                    t.sort_by(f64::total_cmp);
                    let mut push = |metric, value| {
                        figs.push(Fig {
                            n,
                            dup,
                            v,
                            metric,
                            value,
                        })
                    };
                    push("cpu_us", t[REPS / 2]);
                    let (heap, e) = heap_and_entry(v, &inserts);
                    push("heap", heap as f64);
                    push("entry", e.len() as f64);
                    let bv = view(v, &e);
                    for (c, tok) in probe_set.iter().take(1000) {
                        let (m_bits, bits) = entry_bits(&e);
                        let h = key_hash(BLOOM_SEED, *c, tok);
                        let direct =
                            probe_bits(&h, K, m_bits / BLOCK_BITS).all(|b| get_bit(bits, b));
                        assert_eq!(bv.may_contain(*c, tok), direct, "probe shortcut");
                    }
                    push("fpr", realised_fpr(&e, &probes));
                    if v == 'A' || v == 'B' {
                        push("est", fill_fpp(entry_bits(&e).1));
                    }
                }
            }
        }

        let get = |n, dup, v, metric: &str, misses: &mut Vec<String>| -> f64 {
            let found: Vec<f64> = figs
                .iter()
                .filter(|f| f.n == n && f.dup == dup && f.v == v && f.metric == metric)
                .map(|f| f.value)
                .collect();
            if found.len() != 1 {
                misses.push(format!(
                    "n={n} dup={dup} {v} {metric}: present {} times",
                    found.len()
                ));
                return f64::NAN;
            }
            found[0]
        };

        let mut misses: Vec<String> = Vec::new();
        println!(
            "{:>6} {:>4} {:>3} {:>12} {:>7} {:>11} {:>7} {:>9} {:>7} {:>8} {:>8} {:>8}",
            "n",
            "dup",
            "var",
            "cpu_us",
            "cpu/A",
            "heap_B",
            "heap/A",
            "entry_B",
            "entry/A",
            "fpr%",
            "est%",
            "est/fpr"
        );
        for &n in &NS {
            for &dup in &DUPS {
                let mut m = Vec::new();
                let a_cpu = get(n, dup, 'A', "cpu_us", &mut m);
                let a_heap = get(n, dup, 'A', "heap", &mut m);
                let a_entry = get(n, dup, 'A', "entry", &mut m);
                for v in VARIANTS {
                    let cpu = get(n, dup, v, "cpu_us", &mut m);
                    let heap = get(n, dup, v, "heap", &mut m);
                    let ent = get(n, dup, v, "entry", &mut m);
                    let fpr = get(n, dup, v, "fpr", &mut m);
                    let (est, ratio) = if v == 'A' || v == 'B' {
                        let e = get(n, dup, v, "est", &mut m);
                        (format!("{:.3}", e * 100.0), format!("{:.3}", e / fpr))
                    } else {
                        ("-".into(), "-".into())
                    };
                    println!(
                        "{n:>6} {dup:>4} {v:>3} {cpu:>12.1} {:>7.3} {heap:>11} {:>7.3} {ent:>9} {:>7.3} {:>8.3} {est:>8} {ratio:>8}",
                        cpu / a_cpu,
                        heap / a_heap,
                        ent / a_entry,
                        fpr * 100.0
                    );
                }
            }
        }

        let band = |label: String, value: f64, lo: f64, hi: f64, misses: &mut Vec<String>| {
            if !(value >= lo && value <= hi) {
                misses.push(format!("{label} = {value:.4}, band [{lo}, {hi}]"));
            }
        };
        let fpr_b_max = 0.01 + 3.0 * (0.01f64 * 0.99 / PROBES as f64).sqrt();
        let mut entry_ratios_b = Vec::new();
        for &n in &NS {
            for &dup in &DUPS {
                let cell = format!("n={n} dup={dup}");
                let mut g = |v, metric| get(n, dup, v, metric, &mut misses);
                let a_cpu = g('A', "cpu_us");
                let a_heap = g('A', "heap");
                let a_entry = g('A', "entry");
                let a_fpr = g('A', "fpr");
                let a_est = g('A', "est");
                let b_cpu = g('B', "cpu_us");
                let b_heap = g('B', "heap");
                let b_entry = g('B', "entry");
                let b_fpr = g('B', "fpr");
                let b_est = g('B', "est");
                let c_cpu = g('C', "cpu_us");
                let c_heap = g('C', "heap");
                let c_entry = g('C', "entry");
                let _ = g('C', "fpr");
                let d_cpu = g('D', "cpu_us");
                let d_heap = g('D', "heap");
                let d_entry = g('D', "entry");
                let _ = g('D', "fpr");

                band(
                    format!("{cell} entry C/A"),
                    c_entry / a_entry,
                    1.0,
                    1.0,
                    &mut misses,
                );
                band(
                    format!("{cell} entry D/A"),
                    d_entry / a_entry,
                    1.0,
                    1.0,
                    &mut misses,
                );
                band(
                    format!("{cell} entry B/A"),
                    b_entry / a_entry,
                    0.95,
                    2.0,
                    &mut misses,
                );
                entry_ratios_b.push(b_entry / a_entry);
                band(format!("{cell} fpr A"), a_fpr, 0.009, 0.018, &mut misses);
                band(format!("{cell} fpr B"), b_fpr, 0.0, fpr_b_max, &mut misses);
                if n >= 16_000 {
                    band(
                        format!("{cell} est/fpr A"),
                        a_est / a_fpr,
                        0.75,
                        1.25,
                        &mut misses,
                    );
                    band(
                        format!("{cell} est/fpr B"),
                        b_est / b_fpr,
                        0.75,
                        1.25,
                        &mut misses,
                    );
                }
                match dup {
                    1 => {
                        band(
                            format!("{cell} cpu B/A"),
                            b_cpu / a_cpu,
                            0.6,
                            1.2,
                            &mut misses,
                        );
                        band(
                            format!("{cell} cpu C/A"),
                            c_cpu / a_cpu,
                            0.5,
                            1.0,
                            &mut misses,
                        );
                        band(
                            format!("{cell} heap C/A"),
                            c_heap / a_heap,
                            0.25,
                            0.7,
                            &mut misses,
                        );
                        band(
                            format!("{cell} heap B/A"),
                            b_heap / a_heap,
                            0.01,
                            0.1,
                            &mut misses,
                        );
                    }
                    30 => {
                        band(
                            format!("{cell} cpu B/A"),
                            b_cpu / a_cpu,
                            1.5,
                            8.0,
                            &mut misses,
                        );
                        band(
                            format!("{cell} cpu C/A"),
                            c_cpu / a_cpu,
                            1.5,
                            8.0,
                            &mut misses,
                        );
                    }
                    _ => {}
                }
                band(
                    format!("{cell} cpu D/A"),
                    d_cpu / a_cpu,
                    0.5,
                    0.95,
                    &mut misses,
                );
                band(
                    format!("{cell} heap D/A"),
                    d_heap / a_heap,
                    0.9,
                    1.1,
                    &mut misses,
                );
            }
        }
        entry_ratios_b.sort_by(f64::total_cmp);
        let median = entry_ratios_b[entry_ratios_b.len() / 2];
        println!("entry B/A median over cells = {median:.4}");
        band("median entry B/A".into(), median, 1.15, 1.5, &mut misses);

        println!("band misses: {}", misses.len());
        for m in &misses {
            println!("MISS {m}");
        }
        assert!(
            misses.is_empty(),
            "{} band misses:\n{}",
            misses.len(),
            misses.join("\n")
        );
    }
}
