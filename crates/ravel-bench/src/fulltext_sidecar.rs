//! T13 arm 2 (issue #3711): an optional compressed full-text sidecar for cold
//! logs, against the production bloom and a postings section, for rare-token
//! search.
//!
//! LogCloud (PVLDB vol. 18, p. 2362) keeps a compressed full-text index beside
//! each cold log object, so a search for a rare token reads a small index and
//! then only the rows that hold the token, instead of every object's bloom
//! filter and every block the bloom cannot rule out. LogCloud's index is an
//! FM-index over the text; this arm measures the simpler member of the same
//! family that RLOG would actually carry: a row-granular inverted index of the
//! body's tokens (the production tokenizer, so the same words the bloom and
//! `HasWord` see), zstd-compressed in blocks of `sidecar_stride` terms with
//! prefix-shared terms and delta-coded row ordinals, plus a block index and
//! the row map (rows per block) at the tail. A probe reads the tail, then the
//! one term block that can hold the token. It answers substring queries only
//! for whole tokens, which is what `HasWord` asks.
//!
//! Every object is written by the production `RlogWriter` from LogHub lines,
//! one line per row. The baselines are the production structures on the same
//! objects: the BLOOM section the writer wrote (its size, and its verdict
//! through the production `LogSegmentFetcher::scan_accounted_with_tenant`),
//! and a POSTINGS section built by the production `encode_postings_section`
//! over the body's tokens to block indices (production does not build body
//! postings today; it is what the sidecar would replace or stand beside).
//!
//! Production narrows by BLOOM only at decode, over a buffer that already
//! holds every block in the time window, so for a full-window `HasWord` its
//! scan moves the whole object whatever the bloom says. The bloom path the
//! decision is taken against is therefore the bloom-guided route production
//! does not take: real ranged GETs of the suffix probe, PAGE_DIR and BLOOM,
//! then only the pages of the blocks the production bloom keeps. The two
//! production scans (defaults, and the coverage crossover off) are measured
//! beside it for context. Every path runs against a `MemoryStore` behind the
//! production `InstrumentedStore`, which counts their GETs and bytes, and
//! every returned row is checked against the corpus.
//!
//! The 100 GB scale is extrapolated, not measured. With `S = scale_raw_bytes /
//! (raw corpus bytes per object measured)` objects and, per sampled token,
//! the measured cost of each object it occurs in, a token's cost at scale is
//! `sum(cost of its matching objects) + (S - matching objects) * (mean cost of
//! an object it does not occur in)`, for the bloom path and the sidecar path
//! alike; the reported ratio is the mean over tokens of the sidecar cost over
//! the mean over tokens of the bloom cost. A rare token occurs in at most
//! `rare_max_rows` rows of the measured corpus; at scale it is assumed to stay
//! that rare, so the miss term dominates.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::postings::{FieldTerms, PostingsSection, encode_postings_section};
use ravel_logseg::record::COL_BODY;
use ravel_logseg::tokenizer::for_each_token;
use ravel_logseg::varint::{get_uvarint, put_uvarint};
use ravel_logseg::{
    AttrValue, ColumnSelection, FieldSel, LogRecord, ObjectIdentity, Predicate, RlogConfig,
    RlogReader, RlogWriter, footer, stream_attrs_bytes,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_query::{
    BlockRangeFetcher, DEFAULT_LOG_REQUEST_COST_BYTES, DEFAULT_LOG_SUFFIX_LEN, LogQuery,
    LogSegmentFetcher,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;

use crate::t13::{self, Figure, Measured, Rule, SubjectSpec};

pub const PARAMS: &[&str] = &[
    "max_lines",
    "object_rows",
    "block_records",
    "group_blocks",
    "zstd_level",
    "sidecar_stride",
    "sidecar_suffix_bytes",
    "rare_max_rows",
    "rare_sample",
    "scale_raw_bytes",
    "seed",
];

pub const FIGURES: &[&str] = &[
    "lines",
    "objects",
    "blocks",
    "raw_bytes",
    "object_bytes",
    "bloom/section_bytes",
    "postings/bytes",
    "postings/terms",
    "postings/capped_objects",
    "postings/verdict_mismatches",
    "sidecar/bytes",
    "sidecar/terms",
    "sidecar/entries",
    "sidecar/over_postings",
    "sidecar/over_bloom_section",
    "build/sidecar_cpu_ms",
    "build/compaction_cpu_ms",
    "build/cpu_ratio",
    "build/rewrite_amplification",
    "rare/candidates",
    "rare/tokens",
    "rare/rows",
    "rare/bloom_gets",
    "rare/bloom_bytes",
    "rare/bloom_blocks_kept",
    "rare/false_positive_blocks",
    "rare/plan_route_bytes_per_object",
    "rare/default_path_gets",
    "rare/default_path_bytes",
    "rare/no_crossover_gets",
    "rare/no_crossover_bytes",
    "rare/sidecar_gets",
    "rare/sidecar_bytes",
    "rare/bytes_ratio_measured",
    "rare/scale_objects",
    "rare/bloom_bytes_100gb",
    "rare/sidecar_bytes_100gb",
    "rare/bytes_ratio_100gb",
    "rare/gets_ratio_100gb",
    "rare/default_path_bytes_100gb",
    "rare/bytes_ratio_100gb_default_path",
    "delete/erased_only_terms",
    "delete/residual_terms",
    "delete/rebuild_cpu_ms",
    "delete/rebuild_cpu_over_compaction",
    "delete/bytes_over_object",
];

pub const RULES: &[Rule] = &[
    Rule {
        key: "rare/bytes_ratio_100gb",
        max: 0.1,
    },
    Rule {
        key: "sidecar/over_postings",
        max: 1.2,
    },
    Rule {
        key: "build/cpu_ratio",
        max: 1.5,
    },
    Rule {
        key: "delete/rebuild_cpu_over_compaction",
        max: f64::INFINITY,
    },
    Rule {
        key: "delete/bytes_over_object",
        max: f64::INFINITY,
    },
    Rule {
        key: "delete/residual_terms",
        max: 0.0,
    },
];

/// The sidecar's trailing magic.
pub const MAGIC: &[u8; 4] = b"FTX1";
/// The fixed tail: the compressed index length (u32 LE), then [`MAGIC`].
pub const TRAILER: usize = 8;

/// Tokens the bloom and `HasWord` truncate are not exact words; a rare
/// candidate is shorter than the cap so the word it names is the token.
const TOKEN_CAP: usize = 64;

/// Maps a row ordinal in the object (0-based, in row order) to its block and
/// its position in the block, and back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMap {
    /// `starts[b]` is block `b`'s first ordinal; the last entry is the row
    /// count.
    starts: Vec<u64>,
}

impl RowMap {
    pub fn from_counts(per_block: &[u64]) -> Self {
        let mut starts = Vec::with_capacity(per_block.len() + 1);
        let mut at = 0u64;
        starts.push(0);
        for &n in per_block {
            at += n;
            starts.push(at);
        }
        RowMap { starts }
    }

    pub fn rows(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    pub fn blocks(&self) -> usize {
        self.starts.len().saturating_sub(1)
    }

    /// `(block, row in block)` of ordinal `ord`.
    pub fn locate(&self, ord: u64) -> Option<(u32, u64)> {
        if ord >= self.rows() {
            return None;
        }
        let b = self.starts.partition_point(|&s| s <= ord) - 1;
        Some((u32::try_from(b).ok()?, ord - self.starts[b]))
    }

    /// The ordinal of row `row` of block `block`.
    pub fn ordinal(&self, block: u32, row: u64) -> Option<u64> {
        let b = block as usize;
        let start = *self.starts.get(b)?;
        let end = *self.starts.get(b + 1)?;
        (start + row < end).then_some(start + row)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_uvarint(out, self.blocks() as u64);
        for w in self.starts.windows(2) {
            put_uvarint(out, w[1] - w[0]);
        }
    }

    fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, String> {
        let n = uvarint(buf, pos)?;
        if n > buf.len() as u64 {
            return Err(format!("row map: {n} blocks in a {}-byte index", buf.len()));
        }
        let mut counts = Vec::with_capacity(n as usize);
        for _ in 0..n {
            counts.push(uvarint(buf, pos)?);
        }
        Ok(RowMap::from_counts(&counts))
    }
}

fn uvarint(buf: &[u8], pos: &mut usize) -> Result<u64, String> {
    get_uvarint(buf, pos).map_err(|e| format!("varint at {pos}: {e}"))
}

fn take<'a>(buf: &'a [u8], pos: &mut usize, n: u64) -> Result<&'a [u8], String> {
    let end = usize::try_from(n)
        .ok()
        .and_then(|n| pos.checked_add(n))
        .filter(|&e| e <= buf.len())
        .ok_or_else(|| format!("{n} bytes at {pos} run past {}", buf.len()))?;
    let s = &buf[*pos..end];
    *pos = end;
    Ok(s)
}

/// Every distinct body token of `bodies` and the ordinals of the rows that
/// hold it, ascending.
pub fn term_rows(bodies: &[String]) -> BTreeMap<Vec<u8>, Vec<u64>> {
    let mut terms: BTreeMap<Vec<u8>, Vec<u64>> = BTreeMap::new();
    for (ord, body) in bodies.iter().enumerate() {
        let ord = ord as u64;
        for_each_token(body, |tok| match terms.get_mut(tok) {
            Some(rows) => {
                if rows.last() != Some(&ord) {
                    rows.push(ord);
                }
            }
            None => {
                terms.insert(tok.to_vec(), vec![ord]);
            }
        });
    }
    terms
}

/// One term block before compression: the term count, then per term the
/// bytes it shares with the previous term, its suffix length and suffix, its
/// row count, and its rows as the first ordinal then the gaps.
pub fn encode_term_block(terms: &[(&[u8], &[u64])]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, terms.len() as u64);
    let mut prev: &[u8] = &[];
    for (term, rows) in terms {
        let shared = prev
            .iter()
            .zip(term.iter())
            .take_while(|(a, b)| a == b)
            .count();
        put_uvarint(&mut out, shared as u64);
        put_uvarint(&mut out, (term.len() - shared) as u64);
        out.extend_from_slice(&term[shared..]);
        put_uvarint(&mut out, rows.len() as u64);
        let mut last = 0u64;
        for (i, &r) in rows.iter().enumerate() {
            put_uvarint(&mut out, if i == 0 { r } else { r - last });
            last = r;
        }
        prev = term;
    }
    out
}

/// The rows of `want` in a decompressed term block, or `None` when the block
/// does not hold it.
pub fn find_in_term_block(block: &[u8], want: &[u8]) -> Result<Option<Vec<u64>>, String> {
    let mut pos = 0usize;
    let n = uvarint(block, &mut pos)?;
    let mut term: Vec<u8> = Vec::new();
    for _ in 0..n {
        let shared = uvarint(block, &mut pos)? as usize;
        let suffix_len = uvarint(block, &mut pos)?;
        if shared > term.len() {
            return Err(format!(
                "term shares {shared} bytes of a {}-byte term",
                term.len()
            ));
        }
        term.truncate(shared);
        term.extend_from_slice(take(block, &mut pos, suffix_len)?);
        let count = uvarint(block, &mut pos)?;
        if count > block.len() as u64 {
            return Err(format!("{count} rows in a {}-byte block", block.len()));
        }
        let mut rows = Vec::with_capacity(count as usize);
        let mut last = 0u64;
        for i in 0..count {
            let v = uvarint(block, &mut pos)?;
            last = if i == 0 { v } else { last + v };
            rows.push(last);
        }
        match term.as_slice().cmp(want) {
            std::cmp::Ordering::Equal => return Ok(Some(rows)),
            std::cmp::Ordering::Greater => return Ok(None),
            std::cmp::Ordering::Less => {}
        }
    }
    Ok(None)
}

/// One term block's place in the sidecar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockEntry {
    pub first_term: Vec<u8>,
    pub offset: u64,
    pub len: u64,
}

/// The decoded tail of a sidecar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SidecarIndex {
    pub blocks: Vec<BlockEntry>,
    pub rows: RowMap,
}

impl SidecarIndex {
    /// The one term block that can hold `term`: the last whose first term is
    /// at or before it.
    pub fn block_for(&self, term: &[u8]) -> Option<&BlockEntry> {
        let i = self
            .blocks
            .partition_point(|b| b.first_term.as_slice() <= term);
        i.checked_sub(1).map(|i| &self.blocks[i])
    }
}

/// What a sidecar build made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built {
    pub bytes: Vec<u8>,
    pub terms: u64,
    pub entries: u64,
}

/// The sidecar for an object whose rows' bodies are `bodies`, in order, laid
/// out over blocks of `per_block` rows.
pub fn build_sidecar(
    bodies: &[String],
    per_block: &[u64],
    stride: usize,
    zstd_level: i32,
) -> Result<Built, String> {
    let rows = RowMap::from_counts(per_block);
    if rows.rows() != bodies.len() as u64 {
        return Err(format!(
            "{} bodies over blocks of {} rows",
            bodies.len(),
            rows.rows()
        ));
    }
    if stride == 0 {
        return Err("sidecar stride must be positive".into());
    }
    let terms = term_rows(bodies);
    let all: Vec<(&[u8], &[u64])> = terms
        .iter()
        .map(|(t, r)| (t.as_slice(), r.as_slice()))
        .collect();
    let mut out = Vec::new();
    let mut index = Vec::new();
    let mut entries = Vec::new();
    for chunk in all.chunks(stride) {
        let raw = encode_term_block(chunk);
        let z = zstd::bulk::compress(&raw, zstd_level).map_err(|e| format!("zstd: {e}"))?;
        entries.push(BlockEntry {
            first_term: chunk[0].0.to_vec(),
            offset: out.len() as u64,
            len: z.len() as u64,
        });
        out.extend_from_slice(&z);
    }
    put_uvarint(&mut index, entries.len() as u64);
    for e in &entries {
        put_uvarint(&mut index, e.first_term.len() as u64);
        index.extend_from_slice(&e.first_term);
        put_uvarint(&mut index, e.offset);
        put_uvarint(&mut index, e.len);
    }
    rows.encode(&mut index);
    let zi = zstd::bulk::compress(&index, zstd_level).map_err(|e| format!("zstd: {e}"))?;
    let zi_len = u32::try_from(zi.len()).map_err(|_| "sidecar index over 4 GiB")?;
    out.extend_from_slice(&zi);
    out.extend_from_slice(&zi_len.to_le_bytes());
    out.extend_from_slice(MAGIC);
    Ok(Built {
        bytes: out,
        terms: terms.len() as u64,
        entries: terms.values().map(|r| r.len() as u64).sum(),
    })
}

/// The compressed index length the trailer at the end of `tail` names.
pub fn index_len(tail: &[u8]) -> Result<u64, String> {
    let t = tail
        .len()
        .checked_sub(TRAILER)
        .map(|s| &tail[s..])
        .ok_or("sidecar tail shorter than its trailer")?;
    if &t[4..] != MAGIC {
        return Err("sidecar trailer magic mismatch".into());
    }
    Ok(u64::from(u32::from_le_bytes([t[0], t[1], t[2], t[3]])))
}

/// Decodes the compressed index.
pub fn parse_index(zindex: &[u8]) -> Result<SidecarIndex, String> {
    let buf = zstd::decode_all(zindex).map_err(|e| format!("sidecar index: {e}"))?;
    let mut pos = 0usize;
    let n = uvarint(&buf, &mut pos)?;
    if n > buf.len() as u64 {
        return Err(format!("{n} term blocks in a {}-byte index", buf.len()));
    }
    let mut blocks = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let tl = uvarint(&buf, &mut pos)?;
        let first_term = take(&buf, &mut pos, tl)?.to_vec();
        let offset = uvarint(&buf, &mut pos)?;
        let len = uvarint(&buf, &mut pos)?;
        blocks.push(BlockEntry {
            first_term,
            offset,
            len,
        });
    }
    let rows = RowMap::decode(&buf, &mut pos)?;
    if pos != buf.len() {
        return Err(format!("{} trailing index bytes", buf.len() - pos));
    }
    Ok(SidecarIndex { blocks, rows })
}

/// `(block, row in block)` of every row holding `term`, from a whole sidecar
/// in memory.
pub fn probe_local(sidecar: &[u8], term: &[u8]) -> Result<Vec<(u32, u64)>, String> {
    let n = index_len(sidecar)? as usize;
    let end = sidecar.len() - TRAILER;
    let start = end
        .checked_sub(n)
        .ok_or("sidecar index longer than the sidecar")?;
    let index = parse_index(&sidecar[start..end])?;
    let Some(b) = index.block_for(term) else {
        return Ok(Vec::new());
    };
    let z = sidecar
        .get(b.offset as usize..(b.offset + b.len) as usize)
        .ok_or("term block past the sidecar")?;
    let raw = zstd::decode_all(z).map_err(|e| format!("term block: {e}"))?;
    locate_all(
        &index.rows,
        find_in_term_block(&raw, term)?.unwrap_or_default(),
    )
}

fn locate_all(rows: &RowMap, ords: Vec<u64>) -> Result<Vec<(u32, u64)>, String> {
    ords.into_iter()
        .map(|o| {
            rows.locate(o)
                .ok_or_else(|| format!("ordinal {o} past {} rows", rows.rows()))
        })
        .collect()
}

/// A token's cost at scale: the objects it was measured to occur in, plus
/// every other object of the `scale_objects` at the mean cost of an object it
/// does not occur in.
pub fn extrapolate(match_costs: &[f64], mean_miss_cost: f64, scale_objects: f64) -> f64 {
    let m = match_costs.len() as f64;
    match_costs.iter().sum::<f64>() + (scale_objects - m).max(0.0) * mean_miss_cost
}

/// `sample` of `pool`, drawn without replacement by a seeded Fisher-Yates
/// prefix, then sorted.
pub fn sample<T: Ord + Clone>(pool: &[T], sample: usize, seed: u64) -> Vec<T> {
    let mut v = pool.to_vec();
    let mut st = seed;
    let k = sample.min(v.len());
    for i in 0..k {
        let j = i + (t13::splitmix64(&mut st) % (v.len() - i) as u64) as usize;
        v.swap(i, j);
    }
    v.truncate(k);
    v.sort();
    v
}

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: t13::TENANT,
        shard: 0,
        writer_id: [0x46; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

/// Writes `bodies` as one object, row `i` at timestamp `ts0 + i`.
fn write_object(
    bodies: &[&str],
    ts: &[i64],
    cfg: &RlogConfig,
    seq: u64,
) -> Result<Vec<u8>, String> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("t13-fulltext-sidecar".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "t13_fulltext_sidecar", "1", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "t13_fulltext_sidecar", "1", &[]);
    let mut writer = RlogWriter::new(cfg.clone(), identity(seq));
    for (body, &t) in bodies.iter().zip(ts) {
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: t,
                observed_ts_ns: t,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: (*body).to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .map_err(|e| format!("push: {e}"))?;
    }
    writer.finish().map_err(|e| format!("finish: {e}"))
}

/// Every record of an object, in row order, and each block's row count.
fn read_all(bytes: &[u8], cfg: &RlogConfig) -> Result<(Vec<LogRecord>, Vec<u64>), String> {
    let reader = RlogReader::new(bytes, cfg).map_err(|e| format!("open object: {e}"))?;
    let mut scan = reader
        .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
        .map_err(|e| format!("scan: {e}"))?;
    let mut out = Vec::new();
    let mut per_block = Vec::new();
    while let Some(index) = scan.next_block_index() {
        let Some(recs) = scan
            .next_block(bytes)
            .map_err(|e| format!("decode block {index}: {e}"))?
        else {
            break;
        };
        if index != per_block.len() {
            return Err(format!("block {index} read back out of order"));
        }
        per_block.push(recs.len() as u64);
        out.extend(recs);
    }
    Ok((out, per_block))
}

/// The compaction stand-in: decode every block and write the rows again with
/// the production writer. Returns the new object and the thread CPU seconds.
fn compact(bytes: &[u8], cfg: &RlogConfig, seq: u64) -> Result<(Vec<u8>, f64), String> {
    let cpu0 = t13::thread_cpu_s();
    let (recs, _) = read_all(bytes, cfg)?;
    let mut writer = RlogWriter::new(cfg.clone(), identity(seq));
    for r in recs {
        writer.push(r).map_err(|e| format!("push: {e}"))?;
    }
    let out = writer.finish().map_err(|e| format!("finish: {e}"))?;
    Ok((out, t13::thread_cpu_s() - cpu0))
}

/// The body-token postings section over `bodies` laid out over `per_block`,
/// as the production encoder writes it, and its distinct term count.
fn postings_section(
    bodies: &[String],
    per_block: &[u64],
    cfg: &RlogConfig,
) -> Result<(Vec<u8>, u64), String> {
    let rows = RowMap::from_counts(per_block);
    let mut terms: BTreeMap<Vec<u8>, BTreeSet<u32>> = BTreeMap::new();
    for (ord, body) in bodies.iter().enumerate() {
        let (block, _) = rows
            .locate(ord as u64)
            .ok_or_else(|| format!("row {ord} past the row map"))?;
        for_each_token(body, |tok| {
            terms.entry(tok.to_vec()).or_default().insert(block);
        });
    }
    let n = terms.len() as u64;
    let mut fields = BTreeMap::new();
    fields.insert(COL_BODY, FieldTerms::Terms(terms));
    let bytes = encode_postings_section(&fields, cfg.postings_stride, cfg.zstd_level)
        .map_err(|e| format!("postings: {e}"))?;
    Ok((bytes, n))
}

/// One object as the arm measured it.
struct Object {
    key: String,
    side_key: String,
    seg: SegmentRef,
    sidecar_len: u64,
    postings: Vec<u8>,
    guide: Guide,
    /// The first ordinal of the object in the corpus.
    first_line: usize,
    rows: usize,
}

/// Where the bloom-guided route reads on one object, as absolute
/// `[start, end)` byte ranges.
#[derive(Clone, Debug, Default, PartialEq)]
struct Guide {
    size: u64,
    /// PAGE_DIR and BLOOM: what the route needs beyond the footer.
    meta: Vec<(u64, u64)>,
    /// Per block, its pages in every column.
    blocks: Vec<Vec<(u64, u64)>>,
}

fn guide(object: &[u8], cfg: &RlogConfig) -> Result<Guide, String> {
    let foot = footer::open(object).map_err(|e| format!("footer: {e}"))?;
    let dir_desc = foot
        .section(footer::kind::PAGE_DIR)
        .ok_or("no PAGE_DIR section")?;
    let bloom = foot
        .section(footer::kind::BLOOM)
        .ok_or("no BLOOM section")?;
    let blocks_desc = foot
        .section(footer::kind::BLOCKS)
        .ok_or("no BLOCKS section")?;
    let base = blocks_desc.offset;
    let mut placed = 0u64;
    let dir_raw = ravel_logseg::reader::read_section(object, dir_desc, cfg)
        .map_err(|e| format!("PAGE_DIR: {e}"))?;
    let dir = PageDir::decode(&dir_raw).map_err(|e| format!("PAGE_DIR: {e}"))?;
    let mut blocks: Vec<Vec<(u64, u64)>> = Vec::new();
    for g in &dir.groups {
        let first = g.first_block as usize;
        let end = first + g.block_count as usize;
        if blocks.len() < end {
            blocks.resize(end, Vec::new());
        }
        for c in &g.chunks {
            let offsets = c
                .page_offsets()
                .ok_or("page offsets overflow in PAGE_DIR")?;
            for (p, rel) in c.pages.iter().zip(offsets) {
                if p.len == 0 {
                    continue;
                }
                let r = (base + rel, base + rel + p.len);
                placed += p.len;
                // A dictionary page serves every block of its group.
                if c.dict_page().is_some_and(|d| std::ptr::eq(d, p)) {
                    for b in &mut blocks[first..end] {
                        b.push(r);
                    }
                } else {
                    blocks
                        .get_mut(first + p.block as usize)
                        .ok_or("a page past its group's blocks")?
                        .push(r);
                }
            }
        }
    }
    if placed != blocks_desc.len {
        return Err(format!(
            "PAGE_DIR places {placed} bytes of a {}-byte BLOCKS section",
            blocks_desc.len
        ));
    }
    Ok(Guide {
        size: object.len() as u64,
        meta: vec![
            (dir_desc.offset, dir_desc.offset + dir_desc.len),
            (bloom.offset, bloom.offset + bloom.len),
        ],
        blocks,
    })
}

/// The GET rounds of the bloom-guided route: the suffix probe of `probe`
/// bytes, then the metadata the probe did not cover, then the pages of the
/// `kept` blocks, each later round clipped to what the probe did not bring
/// and coalesced at `gap`. An empty round is dropped.
fn guided_rounds(
    g: &Guide,
    probe: u64,
    kept: &[usize],
    gap: u64,
) -> Result<Vec<Vec<(u64, u64)>>, String> {
    let tail = g.size.saturating_sub(probe);
    let clip = |r: &[(u64, u64)]| -> Vec<(u64, u64)> {
        r.iter()
            .filter(|r| r.0 < tail)
            .map(|&(s, e)| (s, e.min(tail)))
            .collect()
    };
    let mut pages = Vec::new();
    for &b in kept {
        pages.extend(clip(g.blocks.get(b).ok_or("a kept block past the object")?));
    }
    let mut rounds = vec![vec![(tail, g.size)]];
    for r in [clip(&g.meta), pages] {
        let c = crate::structural_fetch::coalesce(r, gap);
        if !c.is_empty() {
            rounds.push(c);
        }
    }
    Ok(rounds)
}

/// The bloom-guided route on one object, which production does not take:
/// production narrows by BLOOM only at decode, after fetching every block
/// in the time window. Real ranged GETs of the production-placed bytes for
/// the production suffix probe, PAGE_DIR and BLOOM, and the pages of the
/// blocks the production bloom kept, coalesced at the production request
/// cost. The verdict (`kept`) is the production scan's.
async fn bloom_guided_cost(
    store: &Arc<dyn ObjectStoreBackend>,
    metrics: &StoreMetrics,
    obj: &Object,
    kept: &[usize],
) -> Result<Cost, String> {
    let rounds = guided_rounds(
        &obj.guide,
        DEFAULT_LOG_SUFFIX_LEN,
        kept,
        DEFAULT_LOG_REQUEST_COST_BYTES,
    )?;
    let (g0, b0, _) = t13::store_counts(metrics);
    for (s, e) in rounds.into_iter().flatten() {
        get_range(store, &obj.key, s, e).await?;
    }
    let (g1, b1, _) = t13::store_counts(metrics);
    Ok(Cost {
        gets: g1 - g0,
        bytes: b1 - b0,
    })
}

/// One search path's cost on one object.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Cost {
    gets: u64,
    bytes: u64,
}

/// One sampled token's measured costs on the objects that hold it.
#[derive(Default)]
struct Token {
    bloom_bytes: Vec<f64>,
    side_bytes: Vec<f64>,
    bloom_gets: Vec<f64>,
    side_gets: Vec<f64>,
    default_bytes: Vec<f64>,
}

struct BloomVerdict {
    cost: Cost,
    kept: Vec<usize>,
    /// The scan returns only the rows the predicate keeps, so a row is known
    /// by its block and body, not its position.
    rows: Vec<(u32, String)>,
}

fn fetcher(store: &Arc<dyn ObjectStoreBackend>, cfg: &RlogConfig) -> LogSegmentFetcher {
    LogSegmentFetcher::new(store.clone()).with_config(cfg.clone())
}

/// The production fetcher with the coverage crossover off, so a scan whose
/// time window covers every block still reads only the blocks the bloom keeps.
fn fetcher_ranged(store: &Arc<dyn ObjectStoreBackend>, cfg: &RlogConfig) -> LogSegmentFetcher {
    fetcher(store, cfg).with_block_range(
        BlockRangeFetcher::new(store.clone())
            .with_config(cfg.clone())
            .with_coverage_threshold(2.0),
    )
}

/// Scans `indices` of a planned object and returns every row the scan
/// yields, as `(block, row in block, body)`.
#[allow(clippy::too_many_arguments)]
async fn scan_rows(
    f: &LogSegmentFetcher,
    seg: &SegmentRef,
    query: &LogQuery,
    indices: &[usize],
    survivors: &[usize],
    footer: Option<&footer::LogFooter>,
    whole: Option<ravel_query::CarriedWholeObject>,
    dirs: &Arc<ravel_logseg::reader::SegmentDirectories>,
) -> Result<Vec<(u32, u64, String)>, String> {
    let Some(mut scan) = f
        .scan_accounted_with_tenant_subset_raw(
            seg,
            TenantHash(t13::TENANT),
            query,
            &ColumnSelection::all(),
            indices,
            survivors,
            footer,
            whole,
            Some(dirs),
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("scan: {e}"))?
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    loop {
        let Some(block) = scan.next_block_index() else {
            break;
        };
        let Some(recs) = scan.next_block().map_err(|e| format!("scan block: {e}"))? else {
            break;
        };
        for (i, r) in recs.into_iter().enumerate() {
            out.push((block as u32, i as u64, r.body));
        }
    }
    Ok(out)
}

fn has_word(obj: &Object, token: &str) -> LogQuery {
    LogQuery::new(obj.seg.min_event_ts_ns, obj.seg.max_event_ts_ns).with_content(
        Predicate::HasWord {
            field: FieldSel::Body,
            word: token.to_string(),
        },
    )
}

/// The bloom path for `token` on one object: the production scan funnel with
/// a `HasWord` arm on the body. `ranged` turns the coverage crossover off;
/// then, above the block-range threshold, it reads the footer probe, the
/// BLOOM section, and the ranges of the blocks the bloom keeps. With the
/// production defaults a full-window query covers every block and the scan
/// reads the whole object instead.
async fn bloom_path(
    store: &Arc<dyn ObjectStoreBackend>,
    metrics: &StoreMetrics,
    cfg: &RlogConfig,
    obj: &Object,
    token: &str,
    ranged: bool,
) -> Result<BloomVerdict, String> {
    let f = if ranged {
        fetcher_ranged(store, cfg)
    } else {
        fetcher(store, cfg)
    };
    let query = has_word(obj, token);
    let (g0, b0, _) = t13::store_counts(metrics);
    let mut kept = Vec::new();
    let mut rows = Vec::new();
    if let Some(mut scan) = f
        .scan_accounted_with_tenant(
            &obj.seg,
            TenantHash(t13::TENANT),
            &query,
            &ColumnSelection::all(),
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("bloom scan: {e}"))?
    {
        while let Some(block) = scan.next_block_index() {
            let Some(recs) = scan.next_block().map_err(|e| format!("scan block: {e}"))? else {
                break;
            };
            kept.push(block);
            for (i, r) in recs.into_iter().enumerate() {
                rows.push((block as u32, i as u64, r.body));
            }
        }
    }
    let (g1, b1, _) = t13::store_counts(metrics);
    Ok(BloomVerdict {
        cost: Cost {
            gets: g1 - g0,
            bytes: b1 - b0,
        },
        kept,
        rows: {
            let mut rows: Vec<(u32, String)> =
                rows.into_iter().map(|(b, _, body)| (b, body)).collect();
            rows.sort();
            rows
        },
    })
}

/// The production plan for the same `HasWord` query on one object, without
/// the scan: what a caller that plans each object first pays before reading
/// a block.
async fn plan_route_cost(
    store: &Arc<dyn ObjectStoreBackend>,
    metrics: &StoreMetrics,
    cfg: &RlogConfig,
    obj: &Object,
    token: &str,
) -> Result<Cost, String> {
    let (g0, b0, _) = t13::store_counts(metrics);
    fetcher(store, cfg)
        .plan_segment(
            &obj.seg,
            TenantHash(t13::TENANT),
            &has_word(obj, token),
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("plan: {e}"))?;
    let (g1, b1, _) = t13::store_counts(metrics);
    Ok(Cost {
        gets: g1 - g0,
        bytes: b1 - b0,
    })
}

async fn get_range(
    store: &Arc<dyn ObjectStoreBackend>,
    key: &str,
    s: u64,
    e: u64,
) -> Result<Bytes, String> {
    let got = store
        .get(key, GetRange::Range(s, e))
        .await
        .map_err(|err| format!("get {key} {s}..{e}: {err}"))?;
    if got.data.len() as u64 != e - s {
        return Err(format!(
            "get {key} {s}..{e} returned {} bytes",
            got.data.len()
        ));
    }
    Ok(got.data)
}

/// The sidecar probe over the store: the tail (index and trailer) in one GET
/// of up to `suffix` bytes, a second GET when the index is longer, then the
/// one term block that can hold the token. The sidecar's length is taken as
/// known, as the catalog entry that names it would carry it.
async fn remote_probe(
    store: &Arc<dyn ObjectStoreBackend>,
    obj: &Object,
    suffix: u64,
    token: &[u8],
) -> Result<Vec<(u32, u64)>, String> {
    let size = obj.sidecar_len;
    let tail = get_range(store, &obj.side_key, size - suffix.min(size), size).await?;
    let n = index_len(&tail)?;
    let need = n + TRAILER as u64;
    let zindex = if need <= tail.len() as u64 {
        tail.slice(tail.len() - need as usize..tail.len() - TRAILER)
    } else {
        let s = size
            .checked_sub(need)
            .ok_or("sidecar index longer than the sidecar")?;
        get_range(store, &obj.side_key, s, size - TRAILER as u64).await?
    };
    let index = parse_index(&zindex)?;
    let Some(b) = index.block_for(token) else {
        return Ok(Vec::new());
    };
    let z = get_range(store, &obj.side_key, b.offset, b.offset + b.len).await?;
    let raw = zstd::decode_all(&z[..]).map_err(|e| format!("term block: {e}"))?;
    locate_all(
        &index.rows,
        find_in_term_block(&raw, token)?.unwrap_or_default(),
    )
}

/// The sidecar path for `token` on one object: the probe, then, when it names
/// rows, the production routed plan over their blocks and a scan of them,
/// keeping the probed rows.
async fn sidecar_path(
    store: &Arc<dyn ObjectStoreBackend>,
    metrics: &StoreMetrics,
    cfg: &RlogConfig,
    obj: &Object,
    suffix: u64,
    token: &str,
) -> Result<(Cost, Vec<(u32, u64)>), String> {
    let (g0, b0, _) = t13::store_counts(metrics);
    let hits = remote_probe(store, obj, suffix, token.as_bytes()).await?;
    let mut rows = Vec::new();
    if !hits.is_empty() {
        let blocks: Vec<u32> = hits
            .iter()
            .map(|h| h.0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let f = fetcher(store, cfg);
        let query = LogQuery::new(obj.seg.min_event_ts_ns, obj.seg.max_event_ts_ns);
        let keyed = f
            .plan_segment_routed(
                &obj.seg,
                TenantHash(t13::TENANT),
                &query,
                &blocks,
                &QueryAccounting::new(),
                &QueryAccounting::new(),
            )
            .await
            .map_err(|e| format!("sidecar plan: {e}"))?;
        let (survivors, dirs, _, foot, whole) = keyed
            .plan
            .ok_or("the routed plan dropped an object the sidecar matched")?;
        let indices = keyed.key_blocks.unwrap_or_else(|| survivors.clone());
        let want: BTreeSet<(u32, u64)> = hits.iter().copied().collect();
        for (b, r, body) in scan_rows(
            &f,
            &obj.seg,
            &query,
            &indices,
            &survivors,
            foot.as_ref(),
            whole,
            &dirs,
        )
        .await?
        {
            if want.contains(&(b, r)) {
                if !has_token(&body, token.as_bytes()) {
                    return Err(format!("sidecar row ({b}, {r}) does not hold {token:?}"));
                }
                rows.push((b, r));
            }
        }
        if rows.len() != hits.len() {
            return Err(format!(
                "the scan returned {} of the {} rows the sidecar named",
                rows.len(),
                hits.len()
            ));
        }
    }
    let (g1, b1, _) = t13::store_counts(metrics);
    Ok((
        Cost {
            gets: g1 - g0,
            bytes: b1 - b0,
        },
        rows,
    ))
}

fn has_token(body: &str, token: &[u8]) -> bool {
    let mut found = false;
    for_each_token(body, |t| found |= t == token);
    found
}

/// Whether `token` is a word `HasWord` matches exactly: under the cap, and
/// tokenizing it gives back only itself.
fn exact_word(token: &[u8]) -> Option<String> {
    if token.len() >= TOKEN_CAP {
        return None;
    }
    let s = std::str::from_utf8(token).ok()?;
    let mut toks = Vec::new();
    for_each_token(s, |t| toks.push(t.to_vec()));
    (toks.len() == 1 && toks[0] == token).then(|| s.to_string())
}

/// The arm's measurement, for [`t13::exec`].
pub fn run(spec: &SubjectSpec, corpus: &Path) -> Result<Measured, String> {
    let max_lines = spec.param_usize("max_lines")?;
    let object_rows = spec.param_usize("object_rows")?;
    let cfg = RlogConfig {
        block_target_records: spec.param_usize("block_records")?,
        group_target_blocks: spec.param_usize("group_blocks")?,
        zstd_level: i32::try_from(spec.param_u64("zstd_level")?).map_err(|e| e.to_string())?,
        ..RlogConfig::default()
    };
    let stride = spec.param_usize("sidecar_stride")?;
    let suffix = spec.param_u64("sidecar_suffix_bytes")?;
    let rare_max = spec.param_u64("rare_max_rows")?;
    let rare_n = spec.param_usize("rare_sample")?;
    let scale_raw = spec.param("scale_raw_bytes")?;
    let seed = spec.param_u64("seed")?;
    if object_rows == 0 || rare_max == 0 || rare_n == 0 || suffix < TRAILER as u64 {
        return Err(
            "object_rows, rare_max_rows, rare_sample must be positive and \
                    sidecar_suffix_bytes at least the trailer"
                .into(),
        );
    }

    let lines = t13::read_lines(corpus, max_lines)?;
    if lines.len() < object_rows {
        return Err(format!(
            "the corpus has {} lines, fewer than one object of {object_rows}",
            lines.len()
        ));
    }
    // Whole objects only: a short tail object would weigh the per-object
    // means with a size no scaled object has.
    let n_objects = lines.len() / object_rows;
    let used = &lines[..n_objects * object_rows];
    let raw_bytes: u64 = used.iter().map(|l| l.len() as u64 + 1).sum();

    let (store, metrics) = t13::remote_store(Duration::ZERO);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;

    // Pass 1: write, read back, build, compact, baselines.
    let mut objects = Vec::with_capacity(n_objects);
    let mut global: HashMap<Vec<u8>, u64> = HashMap::new();
    let (mut blocks, mut object_bytes, mut bloom_bytes) = (0u64, 0u64, 0u64);
    let (mut post_bytes, mut post_terms, mut capped) = (0u64, 0u64, 0u64);
    let (mut side_bytes, mut side_terms, mut side_entries) = (0u64, 0u64, 0u64);
    let (mut side_cpu, mut comp_cpu) = (0f64, 0f64);
    let mut first_per_block = Vec::new();
    let mut first_comp_cpu = 0f64;
    for (k, chunk) in used.chunks(object_rows).enumerate() {
        let first_line = k * object_rows;
        let bodies: Vec<&str> = chunk.iter().map(String::as_str).collect();
        let ts: Vec<i64> = (first_line..first_line + chunk.len())
            .map(|i| i as i64)
            .collect();
        let object = write_object(&bodies, &ts, &cfg, k as u64 + 1)?;
        let (recs, per_block) = read_all(&object, &cfg)?;
        if recs.len() != chunk.len() || recs.iter().zip(chunk).any(|(r, l)| &r.body != l) {
            return Err(format!(
                "object {k} did not read back its {} lines",
                chunk.len()
            ));
        }
        let cpu0 = t13::thread_cpu_s();
        let built = build_sidecar(chunk, &per_block, stride, cfg.zstd_level)?;
        let s_cpu = t13::thread_cpu_s() - cpu0;
        let (_, c_cpu) = compact(&object, &cfg, k as u64 + 1)?;
        let (post, pterms) = postings_section(chunk, &per_block, &cfg)?;
        if pterms > cfg.postings_max_distinct as u64 {
            capped += 1;
        }
        let guide = guide(&object, &cfg)?;
        let foot = footer::open(&object).map_err(|e| format!("footer: {e}"))?;
        bloom_bytes += foot
            .section(footer::kind::BLOOM)
            .ok_or("no BLOOM section")?
            .len;
        for body in chunk {
            let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
            for_each_token(body, |t| {
                if !seen.contains(t) {
                    seen.insert(t.to_vec());
                }
            });
            for t in seen {
                *global.entry(t).or_default() += 1;
            }
        }
        blocks += per_block.len() as u64;
        object_bytes += object.len() as u64;
        post_bytes += post.len() as u64;
        post_terms += pterms;
        side_bytes += built.bytes.len() as u64;
        side_terms += built.terms;
        side_entries += built.entries;
        side_cpu += s_cpu;
        comp_cpu += c_cpu;
        if k == 0 {
            first_per_block = per_block.clone();
            first_comp_cpu = c_cpu;
        }
        let key = format!("t13/sidecar/{k:04}.rlog");
        let side_key = format!("t13/sidecar/{k:04}.ftx");
        let seg = t13::segment_ref(
            &key,
            &object,
            chunk.len() as u64,
            (first_line as i64, (first_line + chunk.len()) as i64 - 1),
        );
        let sidecar_len = built.bytes.len() as u64;
        rt.block_on(async {
            store
                .put(&key, Bytes::from(object), PutOptions::default())
                .await
                .map_err(|e| format!("put object: {e}"))?;
            store
                .put(&side_key, Bytes::from(built.bytes), PutOptions::default())
                .await
                .map_err(|e| format!("put sidecar: {e}"))
        })?;
        objects.push(Object {
            key,
            side_key,
            seg,
            sidecar_len,
            postings: post,
            guide,
            first_line,
            rows: chunk.len(),
        });
    }

    // Pass 2: the rare tokens and the two search paths.
    let mut pool: Vec<Vec<u8>> = global
        .iter()
        .filter(|(_, n)| **n <= rare_max)
        .filter_map(|(t, _)| exact_word(t).map(|_| t.clone()))
        .collect();
    pool.sort();
    let candidates = pool.len();
    let picked = sample(&pool, rare_n, seed);
    if picked.is_empty() {
        return Err("no rare token in the corpus".into());
    }
    let mut truth: BTreeMap<Vec<u8>, Vec<usize>> =
        picked.iter().map(|t| (t.clone(), Vec::new())).collect();
    for (i, line) in used.iter().enumerate() {
        for_each_token(line, |t| {
            if let Some(v) = truth.get_mut(t)
                && v.last() != Some(&i)
            {
                v.push(i);
            }
        });
    }
    let mut row_maps = Vec::with_capacity(objects.len());
    for obj in &objects {
        let side = rt.block_on(async {
            store
                .get(&obj.side_key, GetRange::Full)
                .await
                .map_err(|e| format!("get sidecar: {e}"))
        })?;
        let n = index_len(&side.data)? as usize;
        let end = side.data.len() - TRAILER;
        row_maps.push(parse_index(&side.data[end - n..end])?.rows);
    }

    let (mut b_gets, mut b_bytes, mut kept_blocks, mut fp_blocks) = (0u64, 0u64, 0u64, 0u64);
    let (mut s_gets, mut s_bytes, mut rare_rows, mut mismatches) = (0u64, 0u64, 0u64, 0u64);
    let mut bloom_miss = Vec::new();
    let mut side_miss = Vec::new();
    let mut bloom_miss_gets = Vec::new();
    let mut side_miss_gets = Vec::new();
    let (mut d_gets, mut d_bytes, mut default_miss) = (0u64, 0u64, Vec::new());
    let (mut x_gets, mut x_bytes) = (0u64, 0u64);
    let mut per_token: Vec<Token> = Vec::new();
    let first_word = picked
        .first()
        .and_then(|t| exact_word(t))
        .ok_or("a picked token is not an exact word")?;
    let mut plan_route_bytes = 0u64;
    for obj in &objects {
        plan_route_bytes += rt
            .block_on(plan_route_cost(&store, &metrics, &cfg, obj, &first_word))?
            .bytes;
    }
    for token in &picked {
        let word = exact_word(token).ok_or("a picked token is not an exact word")?;
        let rows_of = &truth[token];
        rare_rows += rows_of.len() as u64;
        let mut entry = Token::default();
        for (k, obj) in objects.iter().enumerate() {
            let want: Vec<(u32, u64)> = rows_of
                .iter()
                .filter(|&&i| i >= obj.first_line && i < obj.first_line + obj.rows)
                .map(|&i| row_maps[k].locate((i - obj.first_line) as u64))
                .collect::<Option<_>>()
                .ok_or("a corpus row past its object's row map")?;
            let want_blocks: BTreeSet<u32> = want.iter().map(|w| w.0).collect();
            let mut want_bodies: Vec<(u32, String)> = rows_of
                .iter()
                .filter(|&&i| i >= obj.first_line && i < obj.first_line + obj.rows)
                .zip(&want)
                .map(|(&i, &(b, _))| (b, used[i].clone()))
                .collect();
            want_bodies.sort();
            let default = rt.block_on(bloom_path(&store, &metrics, &cfg, obj, &word, false))?;
            let ranged = rt.block_on(bloom_path(&store, &metrics, &cfg, obj, &word, true))?;
            let (s_cost, s_rows) =
                rt.block_on(sidecar_path(&store, &metrics, &cfg, obj, suffix, &word))?;
            if default.rows != want_bodies
                || ranged.rows != want_bodies
                || ranged.kept != default.kept
                || s_rows != want
            {
                return Err(format!(
                    "{word:?} in object {k}: corpus {want:?}, default scan {:?} kept {:?}, \
                     no-crossover scan {:?} kept {:?}, sidecar path {s_rows:?}",
                    default.rows, default.kept, ranged.rows, ranged.kept
                ));
            }
            let guided = rt.block_on(bloom_guided_cost(&store, &metrics, obj, &default.kept))?;
            d_gets += default.cost.gets;
            d_bytes += default.cost.bytes;
            x_gets += ranged.cost.gets;
            x_bytes += ranged.cost.bytes;
            let post = PostingsSection::parse(&obj.postings)
                .map_err(|e| format!("postings: {e}"))?
                .probe(COL_BODY, token)
                .map_err(|e| format!("postings probe: {e}"))?
                .unwrap_or_default();
            if post.into_iter().collect::<BTreeSet<u32>>() != want_blocks {
                mismatches += 1;
            }
            kept_blocks += default.kept.len() as u64;
            fp_blocks += default
                .kept
                .iter()
                .filter(|&&b| !want_blocks.contains(&(b as u32)))
                .count() as u64;
            b_gets += guided.gets;
            b_bytes += guided.bytes;
            s_gets += s_cost.gets;
            s_bytes += s_cost.bytes;
            if want.is_empty() {
                bloom_miss.push(guided.bytes as f64);
                side_miss.push(s_cost.bytes as f64);
                bloom_miss_gets.push(guided.gets as f64);
                side_miss_gets.push(s_cost.gets as f64);
                default_miss.push(default.cost.bytes as f64);
            } else {
                entry.bloom_bytes.push(guided.bytes as f64);
                entry.side_bytes.push(s_cost.bytes as f64);
                entry.bloom_gets.push(guided.gets as f64);
                entry.side_gets.push(s_cost.gets as f64);
                entry.default_bytes.push(default.cost.bytes as f64);
            }
        }
        per_token.push(entry);
    }
    let mean = |v: &[f64]| {
        if v.is_empty() {
            Err("no object without the token was measured: the miss cost is unknown".to_string())
        } else {
            Ok(v.iter().sum::<f64>() / v.len() as f64)
        }
    };
    let scale_objects = scale_raw / (raw_bytes as f64 / n_objects as f64);
    let (bm, sm, bmg, smg, dm) = (
        mean(&bloom_miss)?,
        mean(&side_miss)?,
        mean(&bloom_miss_gets)?,
        mean(&side_miss_gets)?,
        mean(&default_miss)?,
    );
    let tokens = per_token.len() as f64;
    let at_scale = |pick: &dyn Fn(&Token) -> f64| per_token.iter().map(pick).sum::<f64>() / tokens;
    let bloom_100 = at_scale(&|t| extrapolate(&t.bloom_bytes, bm, scale_objects));
    let side_100 = at_scale(&|t| extrapolate(&t.side_bytes, sm, scale_objects));
    let bloom_g100 = at_scale(&|t| extrapolate(&t.bloom_gets, bmg, scale_objects));
    let side_g100 = at_scale(&|t| extrapolate(&t.side_gets, smg, scale_objects));
    let default_100 = at_scale(&|t| extrapolate(&t.default_bytes, dm, scale_objects));

    // Deletion: erase from object 0 a row holding a token no other row of
    // the corpus holds, rewrite the object without it, rebuild the sidecar.
    let first = &objects[0];
    let erased = used[..first.rows]
        .iter()
        .position(|l| {
            let mut unique = false;
            for_each_token(l, |t| unique |= global.get(t) == Some(&1));
            unique
        })
        .ok_or("no row of object 0 holds a token unique in the corpus")?;
    let mut erased_only = BTreeSet::new();
    for_each_token(&used[erased], |t| {
        if global.get(t) == Some(&1) {
            erased_only.insert(t.to_vec());
        }
    });
    let kept_bodies: Vec<&str> = used[..first.rows]
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != erased)
        .map(|(_, l)| l.as_str())
        .collect();
    let kept_ts: Vec<i64> = (0..first.rows as i64)
        .filter(|&i| i as usize != erased)
        .collect();
    let rewritten = write_object(&kept_bodies, &kept_ts, &cfg, 1_000_001)?;
    let (recs, per_block2) = read_all(&rewritten, &cfg)?;
    let bodies2: Vec<String> = recs.into_iter().map(|r| r.body).collect();
    let cpu0 = t13::thread_cpu_s();
    let rebuilt = build_sidecar(&bodies2, &per_block2, stride, cfg.zstd_level)?;
    let rebuild_cpu = t13::thread_cpu_s() - cpu0;
    let mut residual = 0u64;
    for t in &erased_only {
        if !probe_local(&rebuilt.bytes, t)?.is_empty() {
            residual += 1;
        }
    }
    // The row after the erased one moved up by one ordinal; the rebuilt
    // sidecar must name its new place.
    let next_ord = erased.min(bodies2.len().saturating_sub(1));
    let mut check_tok = Vec::new();
    for_each_token(&bodies2[next_ord], |t| {
        if check_tok.is_empty() && t.len() < TOKEN_CAP {
            check_tok = t.to_vec();
        }
    });
    if !check_tok.is_empty() {
        let map2 = RowMap::from_counts(&per_block2);
        let place = map2
            .locate(next_ord as u64)
            .ok_or("the shifted row is past the rebuilt row map")?;
        if !probe_local(&rebuilt.bytes, &check_tok)?.contains(&place) {
            return Err("the rebuilt sidecar lost the row after the erased one".into());
        }
    }
    if per_block2.iter().sum::<u64>() + 1 != first_per_block.iter().sum::<u64>() {
        return Err("the rewritten object does not hold exactly one row fewer".into());
    }

    let fig = |key: &str, value: f64| Figure {
        key: key.to_string(),
        value,
    };
    let figures = vec![
        fig("lines", used.len() as f64),
        fig("objects", n_objects as f64),
        fig("blocks", blocks as f64),
        fig("raw_bytes", raw_bytes as f64),
        fig("object_bytes", object_bytes as f64),
        fig("bloom/section_bytes", bloom_bytes as f64),
        fig("postings/bytes", post_bytes as f64),
        fig("postings/terms", post_terms as f64),
        fig("postings/capped_objects", capped as f64),
        fig("postings/verdict_mismatches", mismatches as f64),
        fig("sidecar/bytes", side_bytes as f64),
        fig("sidecar/terms", side_terms as f64),
        fig("sidecar/entries", side_entries as f64),
        fig(
            "sidecar/over_postings",
            side_bytes as f64 / post_bytes as f64,
        ),
        fig(
            "sidecar/over_bloom_section",
            side_bytes as f64 / bloom_bytes as f64,
        ),
        fig("build/sidecar_cpu_ms", side_cpu * 1e3),
        fig("build/compaction_cpu_ms", comp_cpu * 1e3),
        fig("build/cpu_ratio", side_cpu / comp_cpu),
        fig(
            "build/rewrite_amplification",
            (object_bytes + side_bytes) as f64 / object_bytes as f64,
        ),
        fig("rare/candidates", candidates as f64),
        fig("rare/tokens", picked.len() as f64),
        fig("rare/rows", rare_rows as f64),
        fig("rare/bloom_gets", b_gets as f64),
        fig("rare/bloom_bytes", b_bytes as f64),
        fig("rare/bloom_blocks_kept", kept_blocks as f64),
        fig("rare/false_positive_blocks", fp_blocks as f64),
        fig(
            "rare/plan_route_bytes_per_object",
            plan_route_bytes as f64 / objects.len() as f64,
        ),
        fig("rare/default_path_gets", d_gets as f64),
        fig("rare/default_path_bytes", d_bytes as f64),
        fig("rare/no_crossover_gets", x_gets as f64),
        fig("rare/no_crossover_bytes", x_bytes as f64),
        fig("rare/sidecar_gets", s_gets as f64),
        fig("rare/sidecar_bytes", s_bytes as f64),
        fig("rare/bytes_ratio_measured", s_bytes as f64 / b_bytes as f64),
        fig("rare/scale_objects", scale_objects),
        fig("rare/bloom_bytes_100gb", bloom_100),
        fig("rare/sidecar_bytes_100gb", side_100),
        fig("rare/bytes_ratio_100gb", side_100 / bloom_100),
        fig("rare/gets_ratio_100gb", side_g100 / bloom_g100),
        fig("rare/default_path_bytes_100gb", default_100),
        fig(
            "rare/bytes_ratio_100gb_default_path",
            side_100 / default_100,
        ),
        fig("delete/erased_only_terms", erased_only.len() as f64),
        fig("delete/residual_terms", residual as f64),
        fig("delete/rebuild_cpu_ms", rebuild_cpu * 1e3),
        fig(
            "delete/rebuild_cpu_over_compaction",
            rebuild_cpu / first_comp_cpu,
        ),
        fig(
            "delete/bytes_over_object",
            rebuilt.bytes.len() as f64 / rewritten.len() as f64,
        ),
    ];
    let notes = vec![
        format!(
            "objects: {n_objects} of {object_rows} rows from the first {} corpus lines, written by \
             the production RlogWriter (bloom over every field); keys {}..{}",
            used.len(),
            objects.first().map_or("", |o| o.key.as_str()),
            objects.last().map_or("", |o| o.key.as_str()),
        ),
        "sidecar: row-granular inverted index of the production body tokens, term blocks of \
         sidecar_stride prefix-shared terms with delta-coded row ordinals, each zstd-compressed, \
         then a zstd index (first term, offset, length per block; rows per RLOG block) and an \
         8-byte trailer. A real build and a real probe through ranged GETs, not a simulation"
            .to_string(),
        "baselines: the BLOOM section the production writer wrote, its verdict through the \
         production scan funnel scan_accounted_with_tenant with HasWord(body) over the \
         object's full time window, every returned row checked against the corpus. \
         rare/default_path_* is that scan with the production defaults: every block is a time \
         candidate, the 0.75 coverage crossover fires before the bloom is consulted, and the \
         whole object is read. rare/no_crossover_* is the same scan with the crossover off \
         (BlockRangeFetcher coverage threshold 2.0): the ranged read still fetches every time \
         candidate block, because production narrows by BLOOM only at decode over the fetched \
         buffer. rare/bloom_* is the bloom-guided route production does not take, the \
         baseline the decision is taken against: real ranged GETs of the production suffix \
         probe (DEFAULT_LOG_SUFFIX_LEN), PAGE_DIR and BLOOM where the probe missed them, and \
         the pages of the blocks the production bloom kept, each round coalesced at \
         DEFAULT_LOG_REQUEST_COST_BYTES; its block verdict is the production scan's. \
         rare/plan_route_bytes_per_object is what the production plan_segment for the same \
         query moves per object before any scan (the first sampled token, once per object); \
         the POSTINGS section the production \
         encode_postings_section builds over body tokens to block indices (uncapped, so its \
         size is what it would cost; postings/capped_objects counts the objects where the \
         production distinct cap would have dropped it)"
            .to_string(),
        "costs: GETs and the bytes they returned, counted by the production InstrumentedStore, \
         through a fresh LogSegmentFetcher per object and token (no cache); every returned row \
         on both paths equals the corpus rows holding the token"
            .to_string(),
        format!(
            "100 GB: scale_objects = scale_raw_bytes / (raw_bytes / objects) = {scale_raw} / \
             ({raw_bytes} / {n_objects}); per token cost = sum(measured costs of its matching \
             objects) + (scale_objects - matching objects) * mean miss cost; mean miss bytes \
             bloom {bm:.1}, sidecar {sm:.1}, default path {dm:.1}; mean miss GETs bloom {bmg:.3}, sidecar {smg:.3}; \
             ratio = mean sidecar cost / mean bloom cost over {} tokens",
            per_token.len()
        ),
        format!(
            "build: build/sidecar_cpu_ms is the thread CPU of tokenizing the decoded bodies and \
             encoding the sidecar; build/compaction_cpu_ms is the thread CPU of decoding every \
             block and re-encoding with the production writer; rewrite amplification is \
             (objects + sidecars) / objects in bytes written"
        ),
        format!(
            "deletion: erased row {erased} of object 0 ({} terms only it held); the object is \
             rewritten without it by the production writer and the sidecar rebuilt from the \
             rewritten rows (a sidecar cannot be edited in place: every later ordinal moves); \
             each erased-only term was probed in the rebuilt sidecar, and the row after the erased \
             one was found at its new place; rebuild CPU is over object 0's compaction CPU",
            erased_only.len()
        ),
    ];
    Ok(Measured { figures, notes })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn the_guided_route_probes_then_reads_missed_metadata_then_kept_pages() {
        let g = Guide {
            size: 1000,
            meta: vec![(600, 700), (700, 950)],
            blocks: vec![
                vec![(0, 10), (100, 120)],
                vec![(10, 30), (120, 130)],
                vec![(30, 40), (130, 200)],
            ],
        };
        // The probe brings 900..1000, so BLOOM is read only up to 900; block
        // 2's pages coalesce with block 0's at a gap of 20 (30 <= 10 + 20,
        // 130 <= 120 + 20) but 100 > 40 + 20 starts a second range.
        assert_eq!(
            guided_rounds(&g, 100, &[2, 0], 20).expect("rounds"),
            vec![
                vec![(900, 1000)],
                vec![(600, 900)],
                vec![(0, 40), (100, 200)],
            ]
        );
        // A miss reads no page.
        assert_eq!(
            guided_rounds(&g, 100, &[], 20).expect("rounds"),
            vec![vec![(900, 1000)], vec![(600, 900)]]
        );
        // A probe as long as the object reads it in one GET.
        assert_eq!(
            guided_rounds(&g, 4096, &[1], 20).expect("rounds"),
            vec![vec![(0, 1000)]]
        );
        assert!(guided_rounds(&g, 100, &[3], 20).is_err());
    }

    #[test]
    fn row_map_round_trips_ordinals_and_places() {
        let map = RowMap::from_counts(&[3, 0, 2]);
        assert_eq!(map.rows(), 5);
        let places: Vec<_> = (0..5).map(|o| map.locate(o).expect("place")).collect();
        assert_eq!(places, vec![(0, 0), (0, 1), (0, 2), (2, 0), (2, 1)]);
        for (o, &(b, r)) in places.iter().enumerate() {
            assert_eq!(map.ordinal(b, r), Some(o as u64));
        }
        assert_eq!(map.locate(5), None);
        assert_eq!(map.ordinal(1, 0), None);
        assert_eq!(map.ordinal(0, 3), None);
        let mut buf = Vec::new();
        map.encode(&mut buf);
        assert_eq!(buf, vec![3, 3, 0, 2]);
        let mut pos = 0;
        assert_eq!(RowMap::decode(&buf, &mut pos).expect("decode"), map);
        assert_eq!(pos, 4);
    }

    #[test]
    fn a_term_block_is_prefix_shared_and_delta_coded() {
        let raw =
            encode_term_block(&[(b"err", &[2, 5]), (b"error", &[9]), (b"fail", &[0, 1, 300])]);
        let want: Vec<u8> = vec![
            3, // terms
            0, 3, b'e', b'r', b'r', 2, 2, 3, // "err" rows 2, 5
            3, 2, b'o', b'r', 1, 9, // "error" shares 3, row 9
            0, 4, b'f', b'a', b'i', b'l', 3, 0, 1, 0xAB, 0x02, // rows 0, 1, 300 (gap 299)
        ];
        assert_eq!(raw, want);
        assert_eq!(
            find_in_term_block(&raw, b"error").expect("find"),
            Some(vec![9])
        );
        assert_eq!(
            find_in_term_block(&raw, b"fail").expect("find"),
            Some(vec![0, 1, 300])
        );
        assert_eq!(find_in_term_block(&raw, b"erro").expect("find"), None);
        assert_eq!(find_in_term_block(&raw, b"zzz").expect("find"), None);
    }

    #[test]
    fn the_sidecar_maps_each_token_to_its_rows_across_blocks() {
        // Blocks of 3, 2 and 2 rows; stride 2 puts the 7 terms in 4 blocks.
        let bodies: Vec<String> = [
            "alpha beta",
            "Beta gamma",
            "delta",
            "alpha alpha",
            "epsilon",
            "zeta beta",
            "eta",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let built = build_sidecar(&bodies, &[3, 2, 2], 2, 3).expect("build");
        assert_eq!(built.terms, 7);
        // alpha 2, beta 3, gamma 1, delta 1, epsilon 1, zeta 1, eta 1.
        assert_eq!(built.entries, 10);
        let p = |t: &str| probe_local(&built.bytes, t.as_bytes()).expect("probe");
        assert_eq!(p("alpha"), vec![(0, 0), (1, 0)]);
        assert_eq!(p("beta"), vec![(0, 0), (0, 1), (2, 0)]);
        assert_eq!(p("delta"), vec![(0, 2)]);
        assert_eq!(p("epsilon"), vec![(1, 1)]);
        assert_eq!(p("eta"), vec![(2, 1)]);
        assert_eq!(p("zeta"), vec![(2, 0)]);
        assert_eq!(p("aaa"), Vec::<(u32, u64)>::new());
        assert_eq!(p("omega"), Vec::<(u32, u64)>::new());
        let n = index_len(&built.bytes).expect("trailer") as usize;
        let end = built.bytes.len() - TRAILER;
        let index = parse_index(&built.bytes[end - n..end]).expect("index");
        let firsts: Vec<&[u8]> = index
            .blocks
            .iter()
            .map(|b| b.first_term.as_slice())
            .collect();
        assert_eq!(firsts, vec![&b"alpha"[..], b"delta", b"eta", b"zeta"]);
        assert_eq!(index.rows, RowMap::from_counts(&[3, 2, 2]));
        assert!(build_sidecar(&bodies, &[3, 2], 2, 3).is_err());
    }

    #[test]
    fn extrapolation_charges_every_unmeasured_object_the_miss_cost() {
        // Two matching objects at 100 and 300 bytes, 1000 objects at scale,
        // 10 bytes per miss: 400 + 998 * 10.
        assert_eq!(extrapolate(&[100.0, 300.0], 10.0, 1000.0), 10_380.0);
        assert_eq!(extrapolate(&[], 10.0, 1000.0), 10_000.0);
        assert_eq!(extrapolate(&[5.0, 5.0], 10.0, 1.0), 10.0);
    }

    #[test]
    fn the_sample_is_seeded_distinct_and_sorted() {
        let pool: Vec<u32> = (0..100).collect();
        let a = sample(&pool, 10, 7);
        assert_eq!(a, sample(&pool, 10, 7));
        assert_eq!(a.len(), 10);
        assert!(a.windows(2).all(|w| w[0] < w[1]));
        assert_ne!(a, sample(&pool, 10, 8));
        assert_eq!(sample(&pool[..3], 10, 7), vec![0, 1, 2]);
    }
}
