//! The two block indexes the REI arm compares, and the needle patterns they
//! answer. See the parent module for the selection rule and the figures.

use std::collections::HashMap;

use crate::logbody_report::trigram::{Trigram, distinct_trigrams, trigrams};

/// One block's bodies, joined with a 0x0A byte between lines. A body never
/// holds 0x0A (the report's line rule drops it), so no gram that a pattern
/// can ask for spans two lines.
#[derive(Clone, Debug)]
pub struct BlockText {
    bytes: Vec<u8>,
}

impl BlockText {
    /// `None` when a line holds 0x0A, which would make two lines one.
    pub fn from_lines<L: AsRef<[u8]>>(lines: &[L]) -> Option<Self> {
        let mut bytes = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            let line = line.as_ref();
            if line.contains(&b'\n') {
                return None;
            }
            if i > 0 {
                bytes.push(b'\n');
            }
            bytes.extend_from_slice(line);
        }
        Some(BlockText { bytes })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn lines(&self) -> impl Iterator<Item = &[u8]> {
        self.bytes.split(|&b| b == b'\n')
    }
}

/// The pattern operator: any bytes, within one line.
pub const GAP: &str = ".*";

/// A needle: literal runs separated by [`GAP`]. Every other byte is literal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pattern {
    pub runs: Vec<Vec<u8>>,
}

impl Pattern {
    /// Splits `text` on [`GAP`] and drops empty runs. `None` when no run is
    /// at least [`MIN_GRAM`] bytes, since neither index can answer it.
    pub fn parse(text: &str) -> Option<Self> {
        let runs: Vec<Vec<u8>> = text
            .split(GAP)
            .filter(|r| !r.is_empty())
            .map(|r| r.as_bytes().to_vec())
            .collect();
        runs.iter()
            .any(|r| r.len() >= MIN_GRAM)
            .then_some(Pattern { runs })
    }

    /// The longest literal run, in bytes.
    pub fn literal_run_bytes(&self) -> usize {
        self.runs.iter().map(Vec::len).max().unwrap_or(0)
    }

    /// Whether `line` holds every run, in order, without overlap.
    pub fn matches_line(&self, line: &[u8]) -> bool {
        let mut at = 0usize;
        for run in &self.runs {
            match line
                .get(at..)
                .and_then(|rest| rest.windows(run.len()).position(|w| w == run.as_slice()))
            {
                Some(p) => at += p + run.len(),
                None => return false,
            }
        }
        true
    }

    /// Whether any line of `block` matches: the exact residual.
    pub fn matches_block(&self, block: &BlockText) -> bool {
        block.lines().any(|l| self.matches_line(l))
    }
}

/// The shortest gram either index holds.
pub const MIN_GRAM: usize = 3;

/// The exact per-block trigram set (the control): every distinct byte
/// trigram of every line, stored as a sorted array of 3-byte keys.
pub struct TrigramSets {
    sets: Vec<Vec<Trigram>>,
}

impl TrigramSets {
    pub fn build(blocks: &[BlockText]) -> Self {
        let sets = blocks
            .iter()
            .map(|b| distinct_trigrams(&b.lines().collect::<Vec<_>>()))
            .collect();
        TrigramSets { sets }
    }

    /// Three bytes per distinct trigram per block.
    pub fn bytes(&self) -> u64 {
        self.sets.iter().map(|s| 3 * s.len() as u64).sum()
    }

    /// Per block: whether its set holds every trigram of every run.
    pub fn survivors(&self, p: &Pattern) -> Vec<bool> {
        let wanted: Vec<Trigram> = p.runs.iter().flat_map(|r| trigrams(r)).collect();
        self.sets
            .iter()
            .map(|s| wanted.iter().all(|t| s.binary_search(t).is_ok()))
            .collect()
    }
}

/// The selection parameters of the REI arm.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Params {
    /// Longest gram considered (the shortest is [`MIN_GRAM`]).
    pub max_gram: usize,
    /// A gram is kept only when it occurs in at most this share of blocks.
    pub df_threshold: f64,
    /// The index's byte cap, as a share of the corpus's compressed bytes.
    pub budget_share: f64,
}

pub const DEFAULT_MAX_GRAM: usize = 6;
pub const DEFAULT_DF_THRESHOLD: f64 = 0.10;
pub const DEFAULT_BUDGET_SHARE: f64 = 0.05;

impl Default for Params {
    fn default() -> Self {
        Params {
            max_gram: DEFAULT_MAX_GRAM,
            df_threshold: DEFAULT_DF_THRESHOLD,
            budget_share: DEFAULT_BUDGET_SHARE,
        }
    }
}

/// A gram of at most 7 bytes packed with its length, so a lookup hashes one
/// word.
fn pack(gram: &[u8]) -> u64 {
    gram.iter()
        .fold(gram.len() as u64, |acc, &b| (acc << 8) | u64::from(b))
}

/// An open-addressing map from packed grams to candidate ids, at most a
/// quarter full.
struct GramTable {
    slots: Vec<(u64, u32)>,
    mask: usize,
}

impl GramTable {
    fn new(entries: &[(u64, u32)]) -> Self {
        let cap = (entries.len() * 4).next_power_of_two().max(16);
        let mut t = GramTable {
            slots: vec![(0, 0); cap],
            mask: cap - 1,
        };
        for &(k, id) in entries {
            let mut slot = t.slot(k);
            while t.slots[slot].0 != 0 {
                slot = (slot + 1) & t.mask;
            }
            t.slots[slot] = (k, id);
        }
        t
    }

    fn slot(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) as usize & self.mask
    }

    /// A packed key is never 0: its length byte is at least 3.
    fn get(&self, k: u64) -> Option<u32> {
        let mut slot = self.slot(k);
        loop {
            match self.slots[slot] {
                (0, _) => return None,
                (x, id) if x == k => return Some(id),
                _ => slot = (slot + 1) & self.mask,
            }
        }
    }
}

/// Every distinct substring of `MIN_GRAM..=max_gram` bytes of every run of
/// `workload`: the candidate grams.
pub fn candidates(workload: &[Pattern], max_gram: usize) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = workload
        .iter()
        .flat_map(|p| p.runs.iter())
        .flat_map(|r| (MIN_GRAM..=max_gram).flat_map(move |n| r.windows(n).map(<[u8]>::to_vec)))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// For each candidate, the ascending blocks it occurs in. Candidates are
/// closed under prefixes of at least [`MIN_GRAM`] bytes (a prefix of a
/// substring of a run is a substring of that run), so the scan extends a
/// match one byte at a time and stops at the first length that is not a
/// candidate. A 2^16-bit filter over hashed trigrams, small enough for L1,
/// skips most positions before the table is probed.
pub fn postings(blocks: &[BlockText], cands: &[Vec<u8>]) -> Vec<Vec<u32>> {
    let filter_slot = |t: u32| (t.wrapping_mul(0x9e37_79b1) >> 16) as usize;
    let mut first = vec![0u64; 1 << 10];
    let mut all = Vec::with_capacity(cands.len());
    for (id, g) in cands.iter().enumerate() {
        if g.len() == MIN_GRAM {
            let t = (u32::from(g[0]) << 16) | (u32::from(g[1]) << 8) | u32::from(g[2]);
            let f = filter_slot(t);
            first[f >> 6] |= 1 << (f & 63);
        }
        all.push((pack(g), id as u32));
    }
    let table = GramTable::new(&all);
    let mut out = vec![Vec::new(); cands.len()];
    let mut last = vec![u32::MAX; cands.len()];
    let max_len = cands.iter().map(Vec::len).max().unwrap_or(0);
    for (bi, block) in blocks.iter().enumerate() {
        let bi = bi as u32;
        let bytes = block.bytes();
        for i in 0..bytes.len().saturating_sub(MIN_GRAM - 1) {
            let t = (u32::from(bytes[i]) << 16)
                | (u32::from(bytes[i + 1]) << 8)
                | u32::from(bytes[i + 2]);
            let f = filter_slot(t);
            if first[f >> 6] & (1 << (f & 63)) == 0 {
                continue;
            }
            // `pack` of the n bytes from i, extended one byte at a time.
            let mut val = u64::from(t);
            let mut n = MIN_GRAM;
            loop {
                let key = ((n as u64) << (8 * n)) | val;
                let Some(id) = table.get(key) else { break };
                if last[id as usize] != bi {
                    last[id as usize] = bi;
                    out[id as usize].push(bi);
                }
                if n == max_len || i + n >= bytes.len() {
                    break;
                }
                val = (val << 8) | u64::from(bytes[i + n]);
                n += 1;
            }
        }
    }
    out
}

/// Bytes of an unsigned LEB128 varint.
fn varint_len(mut v: u64) -> u64 {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Posting encodings in the index layout.
const POSTING_DELTAS: u8 = 0;
const POSTING_BITMAP: u8 = 1;

/// Bytes of the cheaper posting encoding for `list` over `blocks` blocks:
/// the count and gaps as varints, or a bitmap.
fn posting_len(list: &[u32], blocks: u32) -> (u8, u64) {
    let mut prev = 0u32;
    let deltas: u64 = varint_len(list.len() as u64)
        + list
            .iter()
            .map(|&b| {
                let d = b - prev;
                prev = b;
                varint_len(u64::from(d))
            })
            .sum::<u64>();
    let bitmap = u64::from(blocks).div_ceil(8);
    if bitmap < deltas {
        (POSTING_BITMAP, bitmap)
    } else {
        (POSTING_DELTAS, deltas)
    }
}

/// Encoded bytes of one gram entry: length byte, gram, posting tag, posting.
fn entry_len(gram: &[u8], list: &[u32], blocks: u32) -> u64 {
    2 + gram.len() as u64 + posting_len(list, blocks).1
}

/// The layout [`GramIndex::encode`] writes, as the report states it.
pub const INDEX_LAYOUT: &str = "varint block count, varint gram count, then per gram in \
byte order: u8 length, the gram bytes, u8 posting tag, and the posting: tag 0 a varint count and \
varint gaps between ascending block numbers (the first gap from 0), tag 1 a bitmap of \
ceil(blocks / 8) bytes, LSB first; each gram takes the smaller.";

/// The selected-gram index: the kept grams with their block postings.
pub struct GramIndex {
    pub blocks: u32,
    /// Sorted by gram bytes.
    pub grams: Vec<(Vec<u8>, Vec<u32>)>,
    pub candidates: usize,
    /// Grams that passed the frequency rule before the byte cap.
    pub eligible: usize,
    pub encoded: Vec<u8>,
}

impl GramIndex {
    /// Counts the candidates of `workload` over `blocks`, selects, budgets,
    /// and encodes. `compressed_bytes` sets the byte cap.
    pub fn build(
        blocks: &[BlockText],
        workload: &[Pattern],
        compressed_bytes: u64,
        params: &Params,
    ) -> Self {
        let block_count = blocks.len() as u32;
        let cands = candidates(workload, params.max_gram);
        let lists = postings(blocks, &cands);

        // Frequency rule and redundancy rule, shortest grams first.
        let max_df = (params.df_threshold * f64::from(block_count)).floor() as usize;
        let mut order: Vec<usize> = (0..cands.len()).collect();
        order.sort_by(|&a, &b| {
            (cands[a].len(), lists[a].len(), &cands[a]).cmp(&(
                cands[b].len(),
                lists[b].len(),
                &cands[b],
            ))
        });
        let mut kept: HashMap<&[u8], usize> = HashMap::new();
        let mut eligible = Vec::new();
        for &c in &order {
            let g = cands[c].as_slice();
            let df = lists[c].len();
            if df > max_df {
                continue;
            }
            let redundant = (MIN_GRAM..g.len())
                .flat_map(|n| g.windows(n))
                .any(|s| kept.get(s) == Some(&df));
            if redundant {
                continue;
            }
            kept.insert(g, df);
            eligible.push(c);
        }

        // Byte cap: rarest first, longer before shorter at equal frequency;
        // a gram that does not fit is skipped and the next one tried.
        eligible.sort_by(|&a, &b| {
            (lists[a].len(), std::cmp::Reverse(cands[a].len()), &cands[a]).cmp(&(
                lists[b].len(),
                std::cmp::Reverse(cands[b].len()),
                &cands[b],
            ))
        });
        let cap = (params.budget_share * compressed_bytes as f64).floor() as u64;
        let header_cap = varint_len(u64::from(block_count)) + varint_len(cands.len() as u64);
        let mut spent = header_cap;
        let mut chosen = Vec::new();
        for &c in &eligible {
            let e = entry_len(&cands[c], &lists[c], block_count);
            if spent + e <= cap {
                spent += e;
                chosen.push(c);
            }
        }
        let mut grams: Vec<(Vec<u8>, Vec<u32>)> = chosen
            .into_iter()
            .map(|c| (cands[c].clone(), lists[c].clone()))
            .collect();
        grams.sort_unstable();
        let mut index = GramIndex {
            blocks: block_count,
            grams,
            candidates: cands.len(),
            eligible: eligible.len(),
            encoded: Vec::new(),
        };
        index.encoded = index.encode();
        index
    }

    /// The index in [`INDEX_LAYOUT`].
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_varint(&mut out, u64::from(self.blocks));
        put_varint(&mut out, self.grams.len() as u64);
        for (g, list) in &self.grams {
            out.push(g.len() as u8);
            out.extend_from_slice(g);
            let (tag, _) = posting_len(list, self.blocks);
            out.push(tag);
            if tag == POSTING_BITMAP {
                let mut bits = vec![0u8; self.blocks.div_ceil(8) as usize];
                for &b in list {
                    bits[(b / 8) as usize] |= 1 << (b % 8);
                }
                out.extend_from_slice(&bits);
            } else {
                put_varint(&mut out, list.len() as u64);
                let mut prev = 0u32;
                for &b in list {
                    put_varint(&mut out, u64::from(b - prev));
                    prev = b;
                }
            }
        }
        out
    }

    pub fn bytes(&self) -> u64 {
        self.encoded.len() as u64
    }

    /// Per block: whether it holds every kept gram that is a substring of
    /// some run. A pattern with no kept gram keeps every block.
    pub fn survivors(&self, p: &Pattern) -> Vec<bool> {
        let mut keep = vec![true; self.blocks as usize];
        for (g, list) in &self.grams {
            if !p.runs.iter().any(|r| r.windows(g.len()).any(|w| w == g)) {
                continue;
            }
            let mut has = vec![false; self.blocks as usize];
            for &b in list {
                has[b as usize] = true;
            }
            for (k, h) in keep.iter_mut().zip(has) {
                *k &= h;
            }
        }
        keep
    }
}

/// Reads an index in [`INDEX_LAYOUT`] back into its grams and postings.
pub fn decode(bytes: &[u8]) -> Option<(u32, Vec<(Vec<u8>, Vec<u32>)>)> {
    fn varint(b: &[u8], at: &mut usize) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *b.get(*at)?;
            *at += 1;
            v |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Some(v);
            }
        }
        None
    }
    let mut at = 0usize;
    let blocks = u32::try_from(varint(bytes, &mut at)?).ok()?;
    let count = varint(bytes, &mut at)?;
    let mut grams = Vec::new();
    for _ in 0..count {
        let len = usize::from(*bytes.get(at)?);
        at += 1;
        let g = bytes.get(at..at + len)?.to_vec();
        at += len;
        let tag = *bytes.get(at)?;
        at += 1;
        let mut list = Vec::new();
        if tag == POSTING_BITMAP {
            let n = blocks.div_ceil(8) as usize;
            let bits = bytes.get(at..at + n)?;
            at += n;
            for b in 0..blocks {
                if bits[(b / 8) as usize] & (1 << (b % 8)) != 0 {
                    list.push(b);
                }
            }
        } else {
            let n = varint(bytes, &mut at)?;
            let mut prev = 0u32;
            for _ in 0..n {
                prev = prev.checked_add(u32::try_from(varint(bytes, &mut at)?).ok()?)?;
                list.push(prev);
            }
        }
        grams.push((g, list));
    }
    (at == bytes.len()).then_some((blocks, grams))
}
