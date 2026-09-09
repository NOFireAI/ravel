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

/// Test-only instrument: counts `key_hash` (BLAKE3) invocations on the current
/// thread. A `debug_assert!` cannot answer "how many": the tests below assert
/// an exact invocation count (zero while duplicates are only staged, one for
/// a single distinct key, `m` for `m` distinct keys), which needs a queryable
/// value, not an inline boolean check. Compiled only under `cfg(test)`, so a
/// production build never pays for it on the path this change optimizes;
/// verified to still increment correctly under an optimized test build
/// (`cargo test --release -p ravel-codec bloom::`). Thread-local, so parallel
/// tests do not race a shared counter.
#[cfg(test)]
mod hash_calls {
    use std::cell::Cell;

    thread_local! {
        static CALLS: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn bump() {
        CALLS.with(|c| c.set(c.get() + 1));
    }

    pub(super) fn reset() {
        CALLS.with(|c| c.set(0));
    }

    pub(super) fn count() -> u64 {
        CALLS.with(Cell::get)
    }
}

/// `h = blake3(seed_le || column_id_le || token)`. `block` is bytes 0..8 LE,
/// `g1` is bytes 8..16 LE, `g2` is bytes 16..24 LE with the low bit forced
/// set. Using disjoint digest bytes for the block and the offsets keeps the
/// first probe from being congruent to the block index (which would collapse
/// most set bits onto two offsets and wreck the false-positive rate).
fn key_hash(seed: u64, column_id: u32, token: &[u8]) -> KeyHash {
    #[cfg(test)]
    hash_calls::bump();
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
///
/// The staged key is the raw `column_id_le || token` bytes, deduplicated
/// before any BLAKE3 is computed. Ingest calls `insert` 20x-349x more often
/// than there are distinct keys (issue #1518), so hashing on insert spent most
/// of its work on tokens the staging set was about to discard. Staging the raw
/// key first defers `key_hash` to `finish`, which runs it exactly once per
/// distinct key: the `n` BLAKE3 invocations that used to be spread across
/// `insert` calls at ingest time now all land inside one `finish` call, so
/// ingest gets cheaper and `finish` gets correspondingly slower -- a
/// stage-timing split now attributes that cost to `finish`, not to `insert`.
/// The emitted bytes are unchanged: `finish` reconstructs the same
/// `(block, g1, g2)` triple set the old insert-time hashing produced (it even
/// re-deduplicates by triple, so a BLAKE3 collision between two distinct raw
/// keys still collapses to one, matching the old distinct-triple count that
/// sizes the filter).
///
/// `finish` takes `self` by value and drains `staged`, freeing each raw
/// entry's `Box<[u8]>` as it is hashed. That drain does not halve peak
/// memory: `triples` below is built with
/// `HashSet::with_capacity(self.staged.len())` before the drain loop runs,
/// and `with_capacity` reserves the full bucket array up front to satisfy
/// its no-reallocation guarantee, so the whole `triples` table exists
/// before a single `staged` entry is freed -- `staged` and `triples` are
/// both fully live at that point, and draining only lets `staged` shrink
/// back down while `triples` fills in alongside it. Measured on this host
/// (process RSS around one `BloomBuilder` loaded with a large distinct-key
/// corpus): 38460 KB immediately before the drain loop starts, 37380 KB
/// immediately after `finish` returns -- about 3%, not "roughly halved".
///
/// Per-key cost at that peak, on the same basis for both tables (hashbrown
/// adds one control byte per slot and keeps the load factor between about
/// 0.4375 just after a resize and 0.875 just before the next one, so a
/// slot's real cost is `(value_width + 1) / load_factor`, not the bare
/// value width):
/// - `staged: HashSet<Box<[u8]>>` -- each slot holds a `Box<[u8]>` fat
///   pointer (16 bytes: data pointer + length), so the slot itself costs
///   `(16 + 1) / 0.875` to `(16 + 1) / 0.4375`, roughly 19 to 39 bytes. That
///   pointer addresses a separate heap allocation for the raw
///   `column_id_le || token` bytes: up to 4 + 64 = 68 bytes of data (the
///   64-byte token bound `insert` documents below) plus a typical ~16-byte
///   allocator header, roughly 84 bytes. Staged cost: roughly 103 to 123
///   bytes per distinct key.
/// - `triples: HashSet<(u64, u64, u64)>` -- the 24-byte tuple lives inline
///   in the slot, no separate heap allocation, so the slot costs
///   `(24 + 1) / 0.875` to `(24 + 1) / 0.4375`, roughly 29 to 57 bytes per
///   distinct key.
///
/// Both tables are live at peak (above), so the combined peak cost is their
/// sum: roughly 132 to 180 bytes per distinct key.
pub struct BloomBuilder {
    seed: u64,
    /// Distinct `column_id_le || token` byte strings. Queried by `&[u8]` via
    /// `Box<[u8]>: Borrow<[u8]>`, so a duplicate insert probes without
    /// allocating.
    staged: HashSet<Box<[u8]>>,
    /// Reused `column_id_le || token` buffer for the membership probe, so a
    /// duplicate insert copies bytes but allocates nothing.
    scratch: Vec<u8>,
}

impl BloomBuilder {
    pub fn new(seed: u64) -> Self {
        BloomBuilder {
            seed,
            staged: HashSet::new(),
            scratch: Vec::new(),
        }
    }

    /// Stages one field-scoped token. Duplicates collapse; the filter is
    /// sized by the distinct-*triple* count `finish` computes after hashing
    /// and re-deduping the staged keys, not by the staged distinct-key
    /// count itself. No BLAKE3 here: only distinct keys reach `key_hash`,
    /// in `finish`.
    ///
    /// `token` is not length-checked or truncated here. The crate's
    /// per-key memory bound holds only because every current caller keeps
    /// `token` at or under 64 bytes: `ravel-logseg`'s writer module bounds
    /// whole-value inserts with a private 64-byte const (`EXACT_BLOOM_MAX`,
    /// crates/ravel-logseg/src/writer.rs), and the tokenizer bounds word
    /// tokens with a private 64-byte const scoped to its own function body
    /// (`TOKEN_MAX_BYTES` inside `tokenizer::tokens`,
    /// crates/ravel-codec/src/tokenizer.rs) -- neither const is importable
    /// from here. A caller outside those two paths can pass an
    /// arbitrary-length slice, and this function will stage it as-is.
    pub fn insert(&mut self, column_id: u32, token: &[u8]) {
        self.scratch.clear();
        self.scratch.extend_from_slice(&column_id.to_le_bytes());
        self.scratch.extend_from_slice(token);
        if !self.staged.contains(self.scratch.as_slice()) {
            self.staged.insert(self.scratch.as_slice().into());
        }
    }

    /// Sizes the filter for a ~1% false-positive rate at the staged distinct
    /// count (`m_bits` rounded up to a power of two, at least 512 bits) and
    /// returns the serialized entry bytes: `m_bits` uvarint, `k` u8, `seed`
    /// u64 LE, then the bit array (`m_bits / 8` bytes).
    pub fn finish(mut self) -> Vec<u8> {
        // Hash each distinct raw key once, then dedup by triple exactly as the
        // old insert-time path did (idempotent for bit-setting, but the
        // distinct-triple count is what sizes the filter). Draining (instead
        // of borrowing) frees each key's `Box<[u8]>` as it is hashed, but
        // `with_capacity` below reserves the full table up front, so `staged`
        // and `triples` are both fully live once the loop starts -- see the
        // struct doc for the measured peak and per-key arithmetic.
        let mut triples: HashSet<(u64, u64, u64)> = HashSet::with_capacity(self.staged.len());
        for key in self.staged.drain() {
            let mut col = [0u8; 4];
            col.copy_from_slice(&key[..4]);
            let column_id = u32::from_le_bytes(col);
            let h = key_hash(self.seed, column_id, &key[4..]);
            triples.insert((h.block, h.g1, h.g2));
        }
        let n = triples.len() as f64;
        let target = (n * BITS_PER_ELEM).ceil() as u64;
        let m_bits = target.max(BLOCK_BITS).next_power_of_two();
        let block_count = m_bits / BLOCK_BITS;
        let mut bits = vec![0u8; (m_bits / 8) as usize];
        for (block, g1, g2) in &triples {
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
    fn blake3_scales_with_distinct_keys_not_inserts() {
        // Many duplicates of one key: BLAKE3 runs exactly once, in finish.
        let mut b = BloomBuilder::new(9);
        hash_calls::reset();
        for _ in 0..10_000 {
            b.insert(5, b"same-token");
        }
        assert_eq!(hash_calls::count(), 0, "insert must not hash");
        let _ = b.finish();
        assert_eq!(
            hash_calls::count(),
            1,
            "one distinct key must hash exactly once"
        );

        // M distinct keys: exactly M invocations, still none on insert.
        let m: u32 = 500;
        let mut b = BloomBuilder::new(9);
        hash_calls::reset();
        for i in 0..m {
            b.insert(5, format!("token-{i}").as_bytes());
        }
        assert_eq!(hash_calls::count(), 0, "insert must not hash");
        let _ = b.finish();
        assert_eq!(
            u32::try_from(hash_calls::count()).expect("count fits u32"),
            m,
            "distinct-key count must equal BLAKE3 invocations"
        );
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

    /// The pre-#1518 algorithm: hash on every insert, stage the triple, size
    /// and serialize from the distinct-triple set. The new builder must emit
    /// bytes identical to this for every input and every insert order.
    fn reference_filter(seed: u64, inserts: &[(u32, Vec<u8>)]) -> Vec<u8> {
        let mut staged: HashSet<(u64, u64, u64)> = HashSet::new();
        for (col, tok) in inserts {
            let h = key_hash(seed, *col, tok);
            staged.insert((h.block, h.g1, h.g2));
        }
        let n = staged.len() as f64;
        let target = (n * BITS_PER_ELEM).ceil() as u64;
        let m_bits = target.max(BLOCK_BITS).next_power_of_two();
        let block_count = m_bits / BLOCK_BITS;
        let mut bits = vec![0u8; (m_bits / 8) as usize];
        for (block, g1, g2) in &staged {
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
        out.extend_from_slice(&seed.to_le_bytes());
        out.extend_from_slice(&bits);
        out
    }

    fn build(seed: u64, inserts: &[(u32, Vec<u8>)]) -> Vec<u8> {
        let mut b = BloomBuilder::new(seed);
        for (col, tok) in inserts {
            b.insert(*col, tok);
        }
        b.finish()
    }

    proptest! {
        // Byte-for-byte identical to the pre-#1518 output, over random column
        // ids, token lengths (including empty and >64 bytes), seeds, insert
        // orders, and high duplicate densities (a small key pool picked many
        // times). Insert order must not change the bytes.
        //
        // The pool is drawn from two size ranges, not one, so both filter
        // geometries stay covered every run: 1..=53 distinct keys keeps the
        // 512-bit floor reachable (`ceil(n * 9.585) <= 512` iff `n <= 53`, so
        // `block_count == 1`; a pool this small is also fully saturated by up
        // to 1600 picks, so `n` is exactly the pool size, never above it), and
        // 54..=599 routinely lands above the floor (`block_count > 1`). A
        // single 1..600 range measured 0 of 258 cases at `block_count == 1`:
        // once `picks` (0..1600) exceeds the pool size, the birthday effect
        // saturates the pool almost every run, so raising the top of one
        // range to reach `block_count > 1` silently pushed distinct counts
        // past the floor on every case instead of adding coverage next to it.
        #[test]
        fn byte_identical_to_reference(
            pool in prop_oneof![
                proptest::collection::vec(
                    (0u32..8u32, proptest::collection::vec(any::<u8>(), 0..80usize)),
                    1..54usize),
                proptest::collection::vec(
                    (0u32..8u32, proptest::collection::vec(any::<u8>(), 0..80usize)),
                    54..600usize),
            ],
            picks in proptest::collection::vec(any::<usize>(), 0..1600usize),
            seed in any::<u64>(),
        ) {
            let inserts: Vec<(u32, Vec<u8>)> = picks
                .iter()
                .map(|&i| pool[i % pool.len()].clone())
                .collect();
            let reference = reference_filter(seed, &inserts);
            let mine = build(seed, &inserts);
            prop_assert_eq!(&mine, &reference);

            // Insert order independence: reversed inserts, same bytes.
            let reversed: Vec<(u32, Vec<u8>)> = inserts.iter().rev().cloned().collect();
            prop_assert_eq!(build(seed, &reversed), mine);
        }
    }

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
