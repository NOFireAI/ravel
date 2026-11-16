//! T13 arm 1 (issue #3711): a Lance-style structural read for a journey row
//! fetch, against today's ranged RLOG plan.
//!
//! Lance (arXiv:2504.15247) stores small values in "mini-blocks" of a few KiB,
//! each compressed on its own, and keeps a per-page index of where each
//! mini-block starts, so a point lookup reads one mini-block per column
//! instead of the whole page. RLOG compresses one page per block per column
//! (8192 rows), so a journey fetch of ten rows in ten blocks today decodes ten
//! whole blocks.
//!
//! The question: for ten matched rows of a 10 MiB RLOG object whose block set
//! is already known (the tier-2 router's answer), would a structural read
//! cut wire bytes to a quarter or less, without more than 1.5x the GETs and
//! without a slower wall clock, at 30 ms per GET?
//!
//! Today's side is the production path, run for real: the object is written
//! by `RlogWriter`, stored in a `MemoryStore` behind the production
//! `InstrumentedStore` and a 30 ms per-GET delay, planned with
//! `LogSegmentFetcher::plan_segment_routed` given the known block set, and
//! scanned with `scan_accounted_with_tenant_subset_raw` over every column.
//! Its GETs, bytes, wall clock and decode CPU are measured.
//!
//! The structural side is a FAITHFUL SIMULATION, not a real decode: no
//! structural reader exists to call. Its byte and GET counts come from the
//! object's real layout. Every page of the real object is decompressed, cut
//! into `ceil(stored / mini_block_bytes)` equal slices of its decompressed
//! bytes, and each slice is recompressed on its own at the writer's zstd
//! level, so each mini-block's stored size is measured, not assumed. A row is
//! taken to sit in the mini-block its position in the block predicts
//! (`floor(row * chunks / rows)`): that is the one approximation, exact for
//! fixed-width columns and proportional for variable-width ones. Dictionary
//! pages are read whole. The ranges a fetch of the matched rows needs are
//! coalesced at the gap being measured and issued as real ranged GETs against
//! the simulated layout, stored beside the object in the same delayed store;
//! the plan is the production plan, run for real, plus the mini-block index
//! (`mini_block_meta_bytes` per mini-block, assumed carried in the page
//! directory GET the plan already makes). Decode CPU is modelled: today's
//! measured decode CPU scaled by the decompressed bytes the structural read
//! would decode over the ones today's read decodes. The structural wall is
//! its measured plan and data GET time plus that modelled CPU.
//!
//! Both sides are measured at two kinds of coalescing gap. At the production
//! default (`coalesce_gap`, which must equal
//! `ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES`) on an L0 segment, ten
//! targets about a megabyte apart coalesce into ranges spanning them on both
//! sides (or today's side reads the whole object through its coverage
//! crossover), so `ratio/*` there is a layout and recompression ratio and
//! decides nothing. The decision is taken at the [`SWEEP`] gaps on an L1
//! segment, where today's side runs the same production fetcher with
//! `BlockRangeFetcher::with_coalesce_gap` and the structural ranges are
//! coalesced at the same gap: `gap_64k/ratio/*` decides, the wider gaps are
//! reported beside it. L1, because the fetcher bridges an L0 read to at most
//! `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` runs before its coverage crossover,
//! so on L0 today's side spans the targets at any gap; [`L0_FLOOR`] measures
//! that case, with the structural ranges held to the same run cap by
//! [`bound_runs`], and is reported only. The delay is a fixed latency per GET
//! with no bandwidth term, so a larger range costs no more wall time than a
//! small one.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ravel_catalog::SegmentRef;
use ravel_logseg::page::{COMP_NONE, COMP_ZSTD};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader,
    RlogWriter, footer, stream_attrs_bytes,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_query::{BlockRangeFetcher, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;

use crate::t13::{self, Figure, Measured, Rule, SubjectSpec};

pub const PARAMS: &[&str] = &[
    "max_lines",
    "object_min_bytes",
    "object_max_bytes",
    "block_records",
    "group_blocks",
    "zstd_level",
    "trace_run_rows",
    "matched_rows",
    "latency_ms",
    "max_concurrent_gets",
    "mini_block_bytes",
    "mini_block_meta_bytes",
    "coalesce_gap",
    "runs",
    "seed",
];

pub const FIGURES: &[&str] = &[
    "rows",
    "blocks",
    "object_bytes",
    "target_blocks",
    "today/plan_gets",
    "today/plan_bytes",
    "today/gets",
    "today/bytes",
    "today/data_over_object",
    "today/uncomp_bytes",
    "today/wall_ms",
    "today/decode_cpu_ms",
    "structural/layout_bytes",
    "structural/raw_ranges",
    "structural/data_ranges",
    "structural/meta_bytes",
    "structural/gets",
    "structural/bytes",
    "structural/uncomp_bytes",
    "structural/wall_ms",
    "structural/decode_cpu_ms",
    "ratio/bytes",
    "ratio/gets",
    "ratio/wall",
    "gap_64k/today/plan_gets",
    "gap_64k/today/plan_bytes",
    "gap_64k/today/gets",
    "gap_64k/today/bytes",
    "gap_64k/today/data_over_object",
    "gap_64k/today/wall_ms",
    "gap_64k/today/decode_cpu_ms",
    "gap_64k/structural/data_ranges",
    "gap_64k/structural/gets",
    "gap_64k/structural/bytes",
    "gap_64k/structural/wall_ms",
    "gap_64k/structural/decode_cpu_ms",
    "gap_64k/ratio/bytes",
    "gap_64k/ratio/gets",
    "gap_64k/ratio/wall",
    "gap_256k/today/plan_gets",
    "gap_256k/today/plan_bytes",
    "gap_256k/today/gets",
    "gap_256k/today/bytes",
    "gap_256k/today/data_over_object",
    "gap_256k/today/wall_ms",
    "gap_256k/today/decode_cpu_ms",
    "gap_256k/structural/data_ranges",
    "gap_256k/structural/gets",
    "gap_256k/structural/bytes",
    "gap_256k/structural/wall_ms",
    "gap_256k/structural/decode_cpu_ms",
    "gap_256k/ratio/bytes",
    "gap_256k/ratio/gets",
    "gap_256k/ratio/wall",
    "gap_1m/today/plan_gets",
    "gap_1m/today/plan_bytes",
    "gap_1m/today/gets",
    "gap_1m/today/bytes",
    "gap_1m/today/data_over_object",
    "gap_1m/today/wall_ms",
    "gap_1m/today/decode_cpu_ms",
    "gap_1m/structural/data_ranges",
    "gap_1m/structural/gets",
    "gap_1m/structural/bytes",
    "gap_1m/structural/wall_ms",
    "gap_1m/structural/decode_cpu_ms",
    "gap_1m/ratio/bytes",
    "gap_1m/ratio/gets",
    "gap_1m/ratio/wall",
    "l0_64k/today/plan_gets",
    "l0_64k/today/plan_bytes",
    "l0_64k/today/gets",
    "l0_64k/today/bytes",
    "l0_64k/today/data_over_object",
    "l0_64k/today/wall_ms",
    "l0_64k/today/decode_cpu_ms",
    "l0_64k/structural/data_ranges",
    "l0_64k/structural/gets",
    "l0_64k/structural/bytes",
    "l0_64k/structural/wall_ms",
    "l0_64k/structural/decode_cpu_ms",
    "l0_64k/ratio/bytes",
    "l0_64k/ratio/gets",
    "l0_64k/ratio/wall",
];

/// The coalescing gaps both sides are measured at besides the production
/// default, each with the prefix its figures carry, on an L1 segment. The
/// first is the production floor and the one the decision is taken at.
pub const SWEEP: &[(&str, u64)] = &[
    ("gap_64k", 64 * 1024),
    ("gap_256k", 256 * 1024),
    ("gap_1m", 1024 * 1024),
];

/// The floor gap again on an L0 segment, where both sides are held to
/// `ravel_query::MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` data GETs.
pub const L0_FLOOR: (&str, u64) = ("l0_64k", 64 * 1024);

const _: () = assert!(SWEEP[0].1 == ravel_query::DEFAULT_LOG_COALESCE_GAP);
const _: () = assert!(L0_FLOOR.1 == ravel_query::DEFAULT_LOG_COALESCE_GAP);

pub const RULES: &[Rule] = &[
    Rule {
        key: "gap_64k/ratio/bytes",
        max: 0.25,
    },
    Rule {
        key: "gap_64k/ratio/gets",
        max: 1.5,
    },
    Rule {
        key: "gap_64k/ratio/wall",
        max: 1.0,
    },
];

/// The trace id every matched row carries.
pub const JOURNEY: [u8; 16] = [0xA5; 16];

const OBJECT_KEY: &str = "t13/structural/object.rlog";
const LAYOUT_KEY: &str = "t13/structural/layout.bin";

/// One mini-block: its stored (compressed) and decompressed sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub stored: u64,
    pub uncomp: u64,
}

/// What a page of the simulated layout belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageKind {
    /// A value or presence page of one block, cut into mini-blocks.
    Value { block: u32 },
    /// A row group dictionary page, read whole by any block of the group.
    Dict { first_block: u32, block_count: u32 },
}

/// One page of the simulated structural layout. Pages sit back to back in
/// the order of the real object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SPage {
    pub column: u32,
    pub kind: PageKind,
    pub chunks: Vec<Chunk>,
}

/// One matched row: its block, its position in the block, and the block's
/// row count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Target {
    pub block: u32,
    pub row: u64,
    pub rows: u64,
}

/// What a structural read of the targets would fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuralPlan {
    /// Distinct mini-block and dictionary ranges before coalescing.
    pub raw_ranges: usize,
    /// The ranges after coalescing: one GET each.
    pub ranges: Vec<(u64, u64)>,
    /// Bytes the coalesced ranges cover, gap bytes included.
    pub bytes: u64,
    /// Decompressed bytes of the distinct mini-blocks and dictionary pages.
    pub uncomp: u64,
    /// The mini-block index over every value page of the object.
    pub meta_bytes: u64,
}

/// How many mini-blocks a page of `stored` bytes is cut into.
pub fn mini_block_count(stored: u64, target: u64) -> u64 {
    stored.div_ceil(target.max(1)).max(1)
}

/// The mini-block row `row` of a `rows`-row block falls in, of `chunks`.
pub fn mini_block_of(row: u64, rows: u64, chunks: u64) -> u64 {
    if rows == 0 {
        return 0;
    }
    (u128::from(row) * u128::from(chunks) / u128::from(rows)) as u64
}

/// Sorts `ranges` and merges each into the previous one when it starts no
/// more than `gap` bytes after the previous one ends: one GET then reads
/// both and the bytes between. The production fetcher's rule for block
/// ranges; its default gap is the request cost,
/// `ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES`.
pub fn coalesce(mut ranges: Vec<(u64, u64)>, gap: u64) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match out.last_mut() {
            Some(last) if s <= last.1.saturating_add(gap) => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

/// Bridges the smallest gaps between sorted, disjoint `runs` (ties to the
/// earlier gap) until at most `max_runs` remain, `max_runs` 0 read as 1: the
/// production rule (`ravel_query`'s `bound_runs`, crate-private there) that
/// holds one read of an L0 object to
/// `ravel_query::MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` GETs.
pub fn bound_runs(runs: Vec<(u64, u64)>, max_runs: usize) -> Vec<(u64, u64)> {
    let max_runs = max_runs.max(1);
    if runs.len() <= max_runs {
        return runs;
    }
    let mut gaps: Vec<(u64, usize)> = runs
        .windows(2)
        .enumerate()
        .map(|(i, pair)| (pair[1].0.saturating_sub(pair[0].1), i))
        .collect();
    gaps.sort_unstable();
    let mut bridged = vec![false; gaps.len()];
    for (_, i) in gaps.into_iter().take(runs.len() - max_runs) {
        bridged[i] = true;
    }
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(max_runs);
    let mut bridge_previous = false;
    for (run, bridge_next) in runs.into_iter().zip(bridged.into_iter().chain([false])) {
        match out.last_mut() {
            Some(last) if bridge_previous => last.1 = last.1.max(run.1),
            _ => out.push(run),
        }
        bridge_previous = bridge_next;
    }
    out
}

/// The ranges a structural read of `targets` needs over `layout`: per target,
/// the one mini-block of each page of its block that holds the row, and the
/// whole dictionary page of each column of the block's group that has one;
/// coalesced at `gap`, then bounded to `max_runs` GETs by [`bound_runs`].
pub fn structural_plan(
    layout: &[SPage],
    targets: &[Target],
    gap: u64,
    max_runs: usize,
    meta_per_chunk: u64,
) -> Result<StructuralPlan, String> {
    let mut offsets = Vec::with_capacity(layout.len());
    let mut at = 0u64;
    for p in layout {
        offsets.push(at);
        at += p.chunks.iter().map(|c| c.stored).sum::<u64>();
    }
    let mut wanted: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for t in targets {
        if t.row >= t.rows {
            return Err(format!("target row {} of a {}-row block", t.row, t.rows));
        }
        let mut columns = BTreeSet::new();
        for (p, &off) in layout.iter().zip(&offsets) {
            if p.kind != (PageKind::Value { block: t.block }) {
                continue;
            }
            columns.insert(p.column);
            let k = mini_block_of(t.row, t.rows, p.chunks.len() as u64) as usize;
            let start = off + p.chunks[..k].iter().map(|c| c.stored).sum::<u64>();
            let c = p.chunks[k];
            if c.stored > 0 {
                wanted.insert((start, start + c.stored), c.uncomp);
            }
        }
        if columns.is_empty() {
            return Err(format!("block {} has no page in the layout", t.block));
        }
        for (p, &off) in layout.iter().zip(&offsets) {
            let PageKind::Dict {
                first_block,
                block_count,
            } = p.kind
            else {
                continue;
            };
            let covers = first_block <= t.block && t.block < first_block + block_count;
            if covers && columns.contains(&p.column) {
                let stored: u64 = p.chunks.iter().map(|c| c.stored).sum();
                let uncomp: u64 = p.chunks.iter().map(|c| c.uncomp).sum();
                wanted.insert((off, off + stored), uncomp);
            }
        }
    }
    let raw_ranges = wanted.len();
    let uncomp = wanted.values().sum();
    let ranges = bound_runs(coalesce(wanted.into_keys().collect(), gap), max_runs);
    let bytes = ranges.iter().map(|(s, e)| e - s).sum();
    let meta_bytes = layout
        .iter()
        .filter(|p| matches!(p.kind, PageKind::Value { .. }))
        .map(|p| p.chunks.len() as u64 * meta_per_chunk)
        .sum();
    Ok(StructuralPlan {
        raw_ranges,
        ranges,
        bytes,
        uncomp,
        meta_bytes,
    })
}

/// The simulated layout of a real object, and its bytes: every page's
/// decompressed bytes cut into `mini_block_count` equal slices, each
/// compressed on its own when the page was compressed (kept raw when zstd is
/// not smaller, as the writer does), dictionary pages copied whole.
pub fn build_layout(
    object: &[u8],
    dir: &PageDir,
    blocks_offset: u64,
    mini_block_bytes: u64,
    zstd_level: i32,
) -> Result<(Vec<SPage>, Vec<u8>), String> {
    // Keyed by offset then directory order: an empty page shares its
    // offset with the page after it.
    let mut pages: Vec<((u64, usize), SPage)> = Vec::new();
    let mut data_of: BTreeMap<(u64, usize), Vec<Vec<u8>>> = BTreeMap::new();
    for g in &dir.groups {
        for chunk in &g.chunks {
            let offsets = chunk
                .page_offsets()
                .ok_or("page offsets overflow in PAGE_DIR")?;
            for (p, rel) in chunk.pages.iter().zip(offsets) {
                let abs = blocks_offset + rel;
                let stored = object
                    .get(abs as usize..(abs + p.len) as usize)
                    .ok_or_else(|| format!("page at {abs} runs past the object"))?;
                let is_dict = chunk.dict_page().is_some_and(|d| std::ptr::eq(d, p));
                let (kind, parts) = if is_dict {
                    (
                        PageKind::Dict {
                            first_block: g.first_block,
                            block_count: g.block_count,
                        },
                        vec![(stored.to_vec(), p.uncomp_len)],
                    )
                } else {
                    let plain = match p.comp {
                        COMP_NONE => stored.to_vec(),
                        COMP_ZSTD => zstd::bulk::decompress(stored, p.uncomp_len as usize)
                            .map_err(|e| format!("decompress page at {abs}: {e}"))?,
                        other => return Err(format!("page at {abs}: compression {other}")),
                    };
                    let c = mini_block_count(p.len, mini_block_bytes)
                        .min((plain.len() as u64).max(1)) as usize;
                    let mut parts = Vec::with_capacity(c);
                    for i in 0..c {
                        let slice = &plain[i * plain.len() / c..(i + 1) * plain.len() / c];
                        let packed = if p.comp == COMP_ZSTD {
                            match zstd::bulk::compress(slice, zstd_level) {
                                Ok(z) if z.len() < slice.len() => z,
                                _ => slice.to_vec(),
                            }
                        } else {
                            slice.to_vec()
                        };
                        parts.push((packed, slice.len() as u64));
                    }
                    (
                        PageKind::Value {
                            block: g.first_block + p.block,
                        },
                        parts,
                    )
                };
                let page = SPage {
                    column: chunk.column_id,
                    kind,
                    chunks: parts
                        .iter()
                        .map(|(b, u)| Chunk {
                            stored: b.len() as u64,
                            uncomp: *u,
                        })
                        .collect(),
                };
                let key = (abs, pages.len());
                data_of.insert(key, parts.into_iter().map(|(b, _)| b).collect());
                pages.push((key, page));
            }
        }
    }
    pages.sort_by_key(|(key, _)| *key);
    let mut bytes = Vec::new();
    for parts in data_of.into_values() {
        for b in parts {
            bytes.extend_from_slice(&b);
        }
    }
    Ok((pages.into_iter().map(|(_, p)| p).collect(), bytes))
}

/// Rows that carry [`JOURNEY`]: `matched` rows spread evenly over `rows`.
pub fn target_rows(rows: usize, matched: usize) -> Vec<usize> {
    (0..matched)
        .map(|k| k * rows / matched + rows / (2 * matched))
        .collect()
}

fn trace_id(seed: u64, row: usize, run: usize) -> [u8; 16] {
    let mut st = seed ^ (row / run.max(1)) as u64;
    let a = t13::splitmix64(&mut st);
    let b = t13::splitmix64(&mut st);
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&a.to_le_bytes());
    id[8..].copy_from_slice(&b.to_le_bytes());
    id
}

fn write_object(
    lines: &[String],
    rows: usize,
    cfg: &RlogConfig,
    seed: u64,
    trace_run: usize,
    matched: usize,
) -> Result<Vec<u8>, String> {
    let identity = ObjectIdentity {
        tenant_hash: t13::TENANT,
        shard: 0,
        writer_id: [0x46; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("t13-structural-fetch".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "t13_structural_fetch", "1", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "t13_structural_fetch", "1", &[]);
    let targets: BTreeSet<usize> = target_rows(rows, matched).into_iter().collect();
    let mut writer = RlogWriter::new(*cfg, identity);
    for (i, line) in lines.iter().take(rows).enumerate() {
        let trace = if targets.contains(&i) {
            JOURNEY
        } else {
            trace_id(seed, i, trace_run)
        };
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns: i as i64,
                observed_ts_ns: i as i64,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: line.clone(),
                trace_id: Some(trace),
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .map_err(|e| format!("push: {e}"))?;
    }
    writer.finish().map_err(|e| format!("finish: {e}"))
}

/// The row count, in whole blocks, whose object lands in the byte window:
/// a proportional search from one row group.
fn sized_object(
    lines: &[String],
    cfg: &RlogConfig,
    window: (u64, u64),
    seed: u64,
    trace_run: usize,
    matched: usize,
) -> Result<(usize, Vec<u8>, usize), String> {
    let step = cfg.block_target_records.max(1);
    let mut rows = (step * cfg.group_target_blocks.max(1)).min(lines.len());
    let mid = (window.0 + window.1) / 2;
    for attempt in 1..=24 {
        let bytes = write_object(lines, rows, cfg, seed, trace_run, matched)?;
        let size = bytes.len() as u64;
        if window.0 <= size && size <= window.1 {
            return Ok((rows, bytes, attempt));
        }
        let scaled = (rows as f64 * mid as f64 / size as f64).round() as usize;
        let mut next = (scaled / step).max(1) * step;
        if next == rows {
            next = if size < window.0 {
                rows + step
            } else {
                rows.saturating_sub(step).max(step)
            };
        }
        if next > lines.len() {
            return Err(format!(
                "the corpus has {} lines; an object of {}..{} bytes needs about {next}",
                lines.len(),
                window.0,
                window.1
            ));
        }
        rows = next;
    }
    Err(format!(
        "no whole-block row count put the object in {}..{} bytes after 24 tries",
        window.0, window.1
    ))
}

/// Each block's row count, read back through the production reader, and
/// every matched row found, checked against where it was written.
fn read_back(
    bytes: &[u8],
    cfg: &RlogConfig,
    rows: usize,
    matched: usize,
) -> Result<(Vec<u64>, Vec<Target>), String> {
    let reader = RlogReader::new(bytes, cfg).map_err(|e| format!("open object: {e}"))?;
    let mut scan = reader
        .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
        .map_err(|e| format!("scan: {e}"))?;
    let mut per_block = Vec::new();
    let mut targets = Vec::new();
    let mut found_rows = Vec::new();
    let mut next = 0usize;
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
        for (i, r) in recs.iter().enumerate() {
            if r.trace_id == Some(JOURNEY) {
                targets.push((index as u32, i as u64));
                found_rows.push(next + i);
            }
        }
        next += recs.len();
        per_block.push(recs.len() as u64);
    }
    if next != rows || found_rows != target_rows(rows, matched) {
        return Err(format!(
            "read back {next} rows (wrote {rows}) with matched rows {found_rows:?}"
        ));
    }
    let targets = targets
        .into_iter()
        .map(|(block, row)| Target {
            block,
            row,
            rows: per_block[block as usize],
        })
        .collect();
    Ok((per_block, targets))
}

/// Decompressed bytes today's read of `blocks` decodes: every page of each
/// block and each dictionary page those blocks need, once.
fn today_uncomp(dir: &PageDir, blocks: &[u32]) -> Result<u64, String> {
    let mut total = 0u64;
    let mut dicts = BTreeSet::new();
    for &b in blocks {
        let pages = dir
            .block_pages(b)
            .ok_or_else(|| format!("block {b} not in PAGE_DIR"))?;
        total += pages.iter().map(|p| p.desc.uncomp_len).sum::<u64>();
        for d in dir
            .block_dict_pages(b)
            .ok_or_else(|| format!("block {b} not in PAGE_DIR"))?
        {
            if dicts.insert(d.offset) {
                total += d.desc.uncomp_len;
            }
        }
    }
    Ok(total)
}

#[derive(Clone)]
struct Ctx<'a> {
    store: Arc<dyn ObjectStoreBackend>,
    metrics: Arc<StoreMetrics>,
    cfg: &'a RlogConfig,
    max_gets: usize,
    seg: &'a SegmentRef,
    blocks: &'a [u32],
}

/// One routed plan through a fresh fetcher, as both sides make it.
struct Planned {
    fetcher: LogSegmentFetcher,
    query: LogQuery,
    key_blocks: Vec<usize>,
    plan: ravel_query::SegmentPlan,
    gets: u64,
    bytes: u64,
    elapsed: Duration,
}

/// A fresh production fetcher: the production defaults throughout, or with
/// its block-range coalescing gap set to `gap` and nothing else changed.
fn fetcher(ctx: &Ctx<'_>, gap: Option<u64>) -> LogSegmentFetcher {
    let f = LogSegmentFetcher::new(ctx.store.clone()).with_config(*ctx.cfg);
    let f = match gap {
        Some(g) => f.with_block_range(
            BlockRangeFetcher::new(ctx.store.clone())
                .with_config(*ctx.cfg)
                .with_coalesce_gap(g),
        ),
        None => f,
    };
    f.with_max_concurrent_gets(ctx.max_gets)
}

async fn plan(ctx: &Ctx<'_>, gap: Option<u64>) -> Result<Planned, String> {
    let fetcher = fetcher(ctx, gap);
    let query = LogQuery::new(ctx.seg.min_event_ts_ns, ctx.seg.max_event_ts_ns);
    let (g0, b0, _) = t13::store_counts(&ctx.metrics);
    let t0 = Instant::now();
    let keyed = fetcher
        .plan_segment_routed(
            ctx.seg,
            TenantHash(t13::TENANT),
            &query,
            ctx.blocks,
            &QueryAccounting::new(),
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("plan: {e}"))?;
    let elapsed = t0.elapsed();
    let (g1, b1, _) = t13::store_counts(&ctx.metrics);
    let key_blocks = keyed.key_blocks.ok_or(
        "the routed plan restricted no block: the object is not above the whole-object bound",
    )?;
    let want: Vec<usize> = ctx.blocks.iter().map(|&b| b as usize).collect();
    if key_blocks != want {
        return Err(format!(
            "the routed plan kept blocks {key_blocks:?}, not {want:?}"
        ));
    }
    Ok(Planned {
        fetcher,
        query,
        key_blocks,
        plan: keyed.plan,
        gets: g1 - g0,
        bytes: b1 - b0,
        elapsed,
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TodayRun {
    plan_gets: u64,
    plan_bytes: u64,
    gets: u64,
    bytes: u64,
    wall: f64,
    cpu: f64,
}

async fn today_once(ctx: &Ctx<'_>, matched: usize, gap: Option<u64>) -> Result<TodayRun, String> {
    let (g0, b0, h0) = t13::store_counts(&ctx.metrics);
    let t0 = Instant::now();
    let p = plan(ctx, gap).await?;
    let (indices, dirs, _, footer, whole) = p.plan.ok_or("the plan dropped the object")?;
    if whole.is_some() {
        return Err("the plan carried the whole object".into());
    }
    let cpu0 = t13::thread_cpu_s();
    let mut scan = p
        .fetcher
        .scan_accounted_with_tenant_subset_raw(
            ctx.seg,
            TenantHash(t13::TENANT),
            &p.query,
            &ColumnSelection::all(),
            &p.key_blocks,
            &indices,
            footer.as_ref(),
            whole,
            Some(&dirs),
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("scan: {e}"))?
        .ok_or("the scan opened nothing")?;
    let mut found = 0usize;
    while let Some(recs) = scan.next_block().map_err(|e| format!("scan block: {e}"))? {
        found += recs.iter().filter(|r| r.trace_id == Some(JOURNEY)).count();
    }
    let cpu = t13::thread_cpu_s() - cpu0;
    let wall = t0.elapsed().as_secs_f64();
    let (g1, b1, h1) = t13::store_counts(&ctx.metrics);
    if found != matched {
        return Err(format!(
            "today's fetch returned {found} matched rows, not {matched}"
        ));
    }
    if h1 != h0 {
        return Err(format!("today's fetch issued {} HEADs", h1 - h0));
    }
    Ok(TodayRun {
        plan_gets: p.gets,
        plan_bytes: p.bytes,
        gets: g1 - g0,
        bytes: b1 - b0,
        wall,
        cpu,
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct StructuralRun {
    plan_gets: u64,
    plan_bytes: u64,
    data_gets: u64,
    data_bytes: u64,
    plan_s: f64,
    data_s: f64,
}

async fn structural_once(
    ctx: &Ctx<'_>,
    ranges: &[(u64, u64)],
    gap: Option<u64>,
) -> Result<StructuralRun, String> {
    let p = plan(ctx, gap).await?;
    let (g0, b0, _) = t13::store_counts(&ctx.metrics);
    let t0 = Instant::now();
    let permits = Arc::new(tokio::sync::Semaphore::new(ctx.max_gets.max(1)));
    let mut set = tokio::task::JoinSet::new();
    for &(s, e) in ranges {
        let store = ctx.store.clone();
        let permits = permits.clone();
        set.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .map_err(|e| format!("semaphore: {e}"))?;
            let got = store
                .get(LAYOUT_KEY, GetRange::Range(s, e))
                .await
                .map_err(|err| format!("get {s}..{e}: {err}"))?;
            if got.data.len() as u64 == e - s {
                Ok(())
            } else {
                Err(format!("get {s}..{e} returned {} bytes", got.data.len()))
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.map_err(|e| format!("join: {e}"))??;
    }
    let data_s = t0.elapsed().as_secs_f64();
    let (g1, b1, _) = t13::store_counts(&ctx.metrics);
    Ok(StructuralRun {
        plan_gets: p.gets,
        plan_bytes: p.bytes,
        data_gets: g1 - g0,
        data_bytes: b1 - b0,
        plan_s: p.elapsed.as_secs_f64(),
        data_s,
    })
}

fn same<T: PartialEq + Copy + std::fmt::Debug>(what: &str, vs: &[T]) -> Result<T, String> {
    let first = *vs.first().ok_or_else(|| format!("{what}: no run"))?;
    if vs.iter().any(|v| *v != first) {
        return Err(format!("{what} varied across runs: {vs:?}"));
    }
    Ok(first)
}

/// One measured configuration: the prefix its figures carry, the gap today's
/// fetcher is given (`None`: the production default, unmodified), the gap
/// the structural ranges are coalesced at, and the segment level, which sets
/// the run cap on both sides.
struct Variant {
    prefix: &'static str,
    today_gap: Option<u64>,
    gap: u64,
    l1: bool,
}

impl Variant {
    fn label(&self) -> String {
        let level = if self.l1 { "L1" } else { "L0" };
        if self.prefix.is_empty() {
            format!("production gap {} on {level}", self.gap)
        } else {
            format!("{} ({} bytes on {level})", self.prefix, self.gap)
        }
    }
}

/// One gap's two sides, reduced over the runs.
struct Sided {
    plan: (u64, u64),
    today: (u64, u64),
    today_wall: f64,
    today_cpu: f64,
    s_gets: u64,
    s_bytes: u64,
    s_wall: f64,
    s_cpu: f64,
}

/// Every GET and byte count must be the same in each run, both sides must
/// have planned alike, and the structural data GETs must be the ones
/// `splan` says. Decode CPU of the structural side is today's at the same
/// gap, scaled by the decompressed bytes each side decodes.
fn reduce(
    what: &str,
    today: &[TodayRun],
    structural: &[StructuralRun],
    splan: &StructuralPlan,
    today_uncomp: u64,
) -> Result<Sided, String> {
    let t_plan = same(
        &format!("{what}: today plan GETs"),
        &today
            .iter()
            .map(|r| (r.plan_gets, r.plan_bytes))
            .collect::<Vec<_>>(),
    )?;
    let t_all = same(
        &format!("{what}: today GETs"),
        &today.iter().map(|r| (r.gets, r.bytes)).collect::<Vec<_>>(),
    )?;
    let s_plan = same(
        &format!("{what}: structural plan GETs"),
        &structural
            .iter()
            .map(|r| (r.plan_gets, r.plan_bytes))
            .collect::<Vec<_>>(),
    )?;
    let s_data = same(
        &format!("{what}: structural data GETs"),
        &structural
            .iter()
            .map(|r| (r.data_gets, r.data_bytes))
            .collect::<Vec<_>>(),
    )?;
    if s_plan != t_plan {
        return Err(format!(
            "{what}: the two sides planned differently: {t_plan:?} vs {s_plan:?}"
        ));
    }
    if s_data != (splan.ranges.len() as u64, splan.bytes) {
        return Err(format!(
            "{what}: the structural GETs moved {s_data:?}, the plan said {:?}",
            (splan.ranges.len(), splan.bytes)
        ));
    }
    let med = |v: Vec<f64>| t13::median(&v).ok_or_else(|| format!("{what}: no run"));
    let today_wall = med(today.iter().map(|r| r.wall).collect())?;
    let today_cpu = med(today.iter().map(|r| r.cpu).collect())?;
    let s_cpu = today_cpu * splan.uncomp as f64 / today_uncomp.max(1) as f64;
    let s_wall = med(structural.iter().map(|r| r.plan_s + r.data_s).collect())? + s_cpu;
    Ok(Sided {
        plan: t_plan,
        today: t_all,
        today_wall,
        today_cpu,
        s_gets: s_plan.0 + s_data.0,
        s_bytes: s_plan.1 + splan.meta_bytes + s_data.1,
        s_wall,
        s_cpu,
    })
}

/// The arm's measurement, for [`t13::exec`].
pub fn run(spec: &SubjectSpec, corpus: &Path) -> Result<Measured, String> {
    let max_lines = spec.param_usize("max_lines")?;
    let window = (
        spec.param_u64("object_min_bytes")?,
        spec.param_u64("object_max_bytes")?,
    );
    let cfg = RlogConfig {
        block_target_records: spec.param_usize("block_records")?,
        group_target_blocks: spec.param_usize("group_blocks")?,
        zstd_level: i32::try_from(spec.param_u64("zstd_level")?).map_err(|e| e.to_string())?,
        ..RlogConfig::default()
    };
    let trace_run = spec.param_usize("trace_run_rows")?;
    let matched = spec.param_usize("matched_rows")?;
    let latency = Duration::from_millis(spec.param_u64("latency_ms")?);
    let max_gets = spec.param_usize("max_concurrent_gets")?;
    let mini = spec.param_u64("mini_block_bytes")?;
    let meta = spec.param_u64("mini_block_meta_bytes")?;
    let gap = spec.param_u64("coalesce_gap")?;
    let runs = spec.param_usize("runs")?;
    let seed = spec.param_u64("seed")?;
    if matched == 0 || runs == 0 {
        return Err("matched_rows and runs must be positive".into());
    }
    if gap != ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES {
        return Err(format!(
            "coalesce_gap {gap} is not the production default gap, \
             DEFAULT_LOG_REQUEST_COST_BYTES = {}: the production-gap figures compare against \
             the unmodified production fetcher",
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES
        ));
    }

    let lines = t13::read_lines(corpus, max_lines)?;
    let (rows, object, attempts) = sized_object(&lines, &cfg, window, seed, trace_run, matched)?;
    let (per_block, targets) = read_back(&object, &cfg, rows, matched)?;
    let blocks: Vec<u32> = targets
        .iter()
        .map(|t| t.block)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let foot = footer::open(&object).map_err(|e| format!("footer: {e}"))?;
    let dir_desc = foot
        .section(footer::kind::PAGE_DIR)
        .ok_or("no PAGE_DIR section")?;
    let dir_raw = ravel_logseg::reader::read_section(&object, dir_desc, &cfg)
        .map_err(|e| format!("PAGE_DIR: {e}"))?;
    let dir = PageDir::decode(&dir_raw).map_err(|e| format!("PAGE_DIR: {e}"))?;
    let blocks_offset = foot
        .section(footer::kind::BLOCKS)
        .ok_or("no BLOCKS section")?
        .offset;
    let (layout, layout_bytes) = build_layout(&object, &dir, blocks_offset, mini, cfg.zstd_level)?;
    let l0_cap = ravel_query::MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT;
    // Index 0 is the production default gap on L0, as round 0 measured it;
    // then the SWEEP gaps on L1, then the floor gap on L0.
    let gaps: Vec<Variant> = std::iter::once(Variant {
        prefix: "",
        today_gap: None,
        gap,
        l1: false,
    })
    .chain(SWEEP.iter().map(|&(prefix, g)| Variant {
        prefix,
        today_gap: Some(g),
        gap: g,
        l1: true,
    }))
    .chain(std::iter::once(Variant {
        prefix: L0_FLOOR.0,
        today_gap: Some(L0_FLOOR.1),
        gap: L0_FLOOR.1,
        l1: false,
    }))
    .collect();
    let splans = gaps
        .iter()
        .map(|v| {
            let max_runs = if v.l1 { usize::MAX } else { l0_cap };
            structural_plan(&layout, &targets, v.gap, max_runs, meta)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let today_uncomp = today_uncomp(&dir, &blocks)?;

    let ts = (0, rows as i64 - 1);
    let seg_l0 = t13::segment_ref(OBJECT_KEY, &object, rows as u64, ts);
    let seg_l1 = t13::segment_ref_at(
        OBJECT_KEY,
        &object,
        rows as u64,
        ts,
        ravel_catalog::SegmentLevel::L1 {
            input_set_hash: [0x13; 32],
            part_index: 0,
        },
    );
    let (store, metrics) = t13::remote_store(latency);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    let ctx_l0 = Ctx {
        store: store.clone(),
        metrics,
        cfg: &cfg,
        max_gets,
        seg: &seg_l0,
        blocks: &blocks,
    };
    let ctx_l1 = Ctx {
        seg: &seg_l1,
        ..ctx_l0.clone()
    };
    let ctx_of = |v: &Variant| if v.l1 { &ctx_l1 } else { &ctx_l0 };
    let bound = LogSegmentFetcher::new(store.clone())
        .with_config(cfg)
        .plan_whole_object_bound();
    let sides = rt.block_on(async {
        store
            .put(
                OBJECT_KEY,
                Bytes::from(object.clone()),
                PutOptions::default(),
            )
            .await
            .map_err(|e| format!("put object: {e}"))?;
        store
            .put(
                LAYOUT_KEY,
                Bytes::from(layout_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .map_err(|e| format!("put layout: {e}"))?;
        // Interleaved: every run measures every gap, each side once.
        let mut out: Vec<(Vec<TodayRun>, Vec<StructuralRun>)> = gaps
            .iter()
            .map(|_| (Vec::with_capacity(runs), Vec::with_capacity(runs)))
            .collect();
        for _ in 0..runs {
            for ((v, sp), (today, structural)) in gaps.iter().zip(&splans).zip(out.iter_mut()) {
                let ctx = ctx_of(v);
                today.push(today_once(ctx, matched, v.today_gap).await?);
                structural.push(structural_once(ctx, &sp.ranges, v.today_gap).await?);
            }
        }
        Ok::<_, String>(out)
    })?;
    let reduced = gaps
        .iter()
        .zip(&splans)
        .zip(&sides)
        .map(|((v, sp), (today, structural))| {
            reduce(&v.label(), today, structural, sp, today_uncomp)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let object_len = object.len() as u64;
    // A covering read: after the plan, one GET that returned the object.
    let whole = |s: &Sided| s.today.0 - s.plan.0 == 1 && s.today.1 - s.plan.1 == object_len;

    let fig = |key: &str, value: f64| Figure {
        key: key.to_string(),
        value,
    };
    let splan = &splans[0];
    let mut figures = vec![
        fig("rows", rows as f64),
        fig("blocks", per_block.len() as f64),
        fig("object_bytes", object.len() as f64),
        fig("target_blocks", blocks.len() as f64),
        fig("today/uncomp_bytes", today_uncomp as f64),
        fig("structural/layout_bytes", layout_bytes.len() as f64),
        fig("structural/raw_ranges", splan.raw_ranges as f64),
        fig("structural/meta_bytes", splan.meta_bytes as f64),
        fig("structural/uncomp_bytes", splan.uncomp as f64),
    ];
    for ((v, sp), s) in gaps.iter().zip(&splans).zip(&reduced) {
        let prefix = v.prefix;
        let key = |k: &str| {
            if prefix.is_empty() {
                k.to_string()
            } else {
                format!("{prefix}/{k}")
            }
        };
        figures.extend([
            fig(&key("today/plan_gets"), s.plan.0 as f64),
            fig(&key("today/plan_bytes"), s.plan.1 as f64),
            fig(&key("today/gets"), s.today.0 as f64),
            fig(&key("today/bytes"), s.today.1 as f64),
            fig(
                &key("today/data_over_object"),
                (s.today.1 - s.plan.1) as f64 / object_len.max(1) as f64,
            ),
            fig(&key("today/wall_ms"), s.today_wall * 1e3),
            fig(&key("today/decode_cpu_ms"), s.today_cpu * 1e3),
            fig(&key("structural/data_ranges"), sp.ranges.len() as f64),
            fig(&key("structural/gets"), s.s_gets as f64),
            fig(&key("structural/bytes"), s.s_bytes as f64),
            fig(&key("structural/wall_ms"), s.s_wall * 1e3),
            fig(&key("structural/decode_cpu_ms"), s.s_cpu * 1e3),
            fig(&key("ratio/bytes"), s.s_bytes as f64 / s.today.1 as f64),
            fig(&key("ratio/gets"), s.s_gets as f64 / s.today.0 as f64),
            fig(&key("ratio/wall"), s.s_wall / s.today_wall),
        ]);
    }
    let today_reads: Vec<String> = gaps
        .iter()
        .zip(&reduced)
        .map(|(v, s)| {
            let at = v.label();
            if whole(s) {
                format!(
                    "{at}: the whole object in one GET after the plan (coverage crossover), \
                     the plan's {} bytes read again",
                    s.plan.1
                )
            } else {
                format!(
                    "{at}: {} ranged GETs of {} bytes after the plan",
                    s.today.0 - s.plan.0,
                    s.today.1 - s.plan.1
                )
            }
        })
        .collect();
    let notes = vec![
        format!(
            "object: {rows} rows from the first lines of the corpus, {} blocks, {} bytes, sized in \
             {attempts} writes; whole-object bound {bound} bytes, so the plan is the ranged plan \
             restricted to the known blocks. Whether today's scan then reads those blocks ranged \
             or the whole object is the fetcher's coverage crossover, decided per gap: {}",
            per_block.len(),
            object.len(),
            today_reads.join("; ")
        ),
        format!(
            "matched rows (block, row of block): {:?}",
            targets.iter().map(|t| (t.block, t.row)).collect::<Vec<_>>()
        ),
        "today: production plan_segment_routed + scan_accounted_with_tenant_subset_raw over every \
         column, real GETs through InstrumentedStore behind a per-GET delay; decode CPU is the \
         thread CPU of the scan open and drain"
            .to_string(),
        "structural: a faithful simulation, not a real decode. Byte and GET counts come from the \
         object's real pages, re-cut into independently compressed mini-blocks and fetched as real \
         ranged GETs; the plan is the production plan run for real plus the mini-block index; \
         decode CPU is modelled as today's CPU at the same gap times structural/uncomp_bytes over \
         today/uncomp_bytes; wall is measured plan and data GET time plus that modelled CPU"
            .to_string(),
        format!(
            "coalescing: unprefixed figures are both sides at the production default gap, \
             coalesce_gap = DEFAULT_LOG_REQUEST_COST_BYTES = {gap} bytes (checked), on an L0 \
             segment, the configuration round 0 measured. Targets {} blocks apart sit closer \
             than that, so both sides there tend to one range and ratio/* is a layout and \
             recompression ratio; it is not counted toward the decision. {} are both sides at \
             the named gap on an L1 segment, measured: today's side is the production fetcher \
             with BlockRangeFetcher::with_coalesce_gap and nothing else changed, the structural \
             ranges are coalesced at the same gap, and both sides' plans are made by that \
             fetcher. The decision is taken at {} ({} bytes, DEFAULT_LOG_COALESCE_GAP). L1 \
             because an L0 read is bridged to at most {l0_cap} \
             (MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT) runs before the coverage crossover is \
             checked, which spans the targets whatever the gap; {}/* is that L0 case at {} \
             bytes, with the structural ranges bounded to the same {l0_cap} runs by the same \
             rule, reported beside the decision and not counted",
            (per_block.len() / matched.max(1)).max(1),
            SWEEP
                .iter()
                .map(|(p, g)| format!("{p}/* ({g} bytes)"))
                .collect::<Vec<_>>()
                .join(", "),
            SWEEP[0].0,
            SWEEP[0].1,
            L0_FLOOR.0,
            L0_FLOOR.1
        ),
        "latency model: every GET waits latency_ms before it returns, whatever its size; there \
         is no bandwidth term, so wall time counts round trips and decode CPU, not bytes moved"
            .to_string(),
        format!(
            "medians of {runs} interleaved runs (each run measures every gap, today then \
             structural); every GET and byte count was identical across runs"
        ),
    ];
    Ok(Measured { figures, notes })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_merges_within_the_gap_inclusive_and_sorts_first() {
        let got = coalesce(
            vec![(100, 200), (0, 50), (250, 300), (50, 60), (400, 500)],
            50,
        );
        // (0,50)+(50,60): touching. (100,200): starts 40 after 60. (250,300):
        // starts exactly 50 after 200, which still merges. (400,500): 100
        // after 300, its own GET.
        assert_eq!(got, vec![(0, 300), (400, 500)]);
        assert_eq!(coalesce(vec![(10, 20), (12, 15)], 0), vec![(10, 20)]);
        assert_eq!(
            coalesce(vec![(0, 10), (11, 20)], 0),
            vec![(0, 10), (11, 20)]
        );
    }

    #[test]
    fn mini_block_mapping_is_proportional() {
        assert_eq!(mini_block_count(1, 8192), 1);
        assert_eq!(mini_block_count(8192, 8192), 1);
        assert_eq!(mini_block_count(8193, 8192), 2);
        assert_eq!(mini_block_count(0, 8192), 1);
        // 10 rows over 4 chunks: rows 0-2 in 0, 3-4 in 1, 5-7 in 2, 8-9 in 3.
        let got: Vec<u64> = (0..10).map(|r| mini_block_of(r, 10, 4)).collect();
        assert_eq!(got, vec![0, 0, 0, 1, 1, 2, 2, 2, 3, 3]);
    }

    fn page(column: u32, kind: PageKind, stored: &[u64]) -> SPage {
        SPage {
            column,
            kind,
            chunks: stored
                .iter()
                .map(|&s| Chunk {
                    stored: s,
                    uncomp: s * 4,
                })
                .collect(),
        }
    }

    #[test]
    fn structural_plan_reads_one_mini_block_per_page_and_the_dictionary() {
        // One group of two blocks: column 0 has a dictionary page and one
        // page per block; column 5 has a 4-chunk page per block.
        let layout = vec![
            page(
                0,
                PageKind::Dict {
                    first_block: 0,
                    block_count: 2,
                },
                &[30],
            ),
            page(0, PageKind::Value { block: 0 }, &[10]),
            page(0, PageKind::Value { block: 1 }, &[10]),
            page(5, PageKind::Value { block: 0 }, &[100, 100, 100, 100]),
            page(5, PageKind::Value { block: 1 }, &[100, 100, 100, 100]),
        ];
        // Offsets: dict 0..30, c0b0 30..40, c0b1 40..50, c5b0 50..450,
        // c5b1 450..850.
        let targets = [
            Target {
                block: 0,
                row: 7,
                rows: 8,
            },
            Target {
                block: 1,
                row: 0,
                rows: 8,
            },
        ];
        let plan = structural_plan(&layout, &targets, 0, usize::MAX, 2).expect("plan");
        // Block 0 row 7 -> c5 chunk 3: 350..450; block 1 row 0 -> chunk 0:
        // 450..550. Plus the dictionary 0..30 and both column-0 pages.
        assert_eq!(plan.raw_ranges, 5);
        assert_eq!(plan.ranges, vec![(0, 50), (350, 550)]);
        assert_eq!(plan.bytes, 250);
        assert_eq!(plan.uncomp, (30 + 10 + 10 + 100 + 100) * 4);
        // 1 + 1 + 4 + 4 value-page chunks, 2 bytes each.
        assert_eq!(plan.meta_bytes, 20);
        let gapped = structural_plan(&layout, &targets, 300, usize::MAX, 2).expect("plan");
        assert_eq!(gapped.ranges, vec![(0, 550)]);
        assert_eq!(gapped.bytes, 550);
        assert!(
            structural_plan(
                &layout,
                &[Target {
                    block: 2,
                    row: 0,
                    rows: 1
                }],
                0,
                usize::MAX,
                2
            )
            .is_err()
        );
        // An L0 cap of one run bridges the two coalesced ranges.
        let capped = structural_plan(&layout, &targets, 0, 1, 2).expect("plan");
        assert_eq!(capped.ranges, vec![(0, 550)]);
        assert_eq!(capped.raw_ranges, 5);
    }

    #[test]
    fn bound_runs_bridges_the_smallest_gaps_first() {
        let runs = vec![(0, 10), (30, 40), (45, 50), (100, 110), (115, 120)];
        // Gaps: 20, 5, 50, 5. Two bridges take the two 5s.
        assert_eq!(
            bound_runs(runs.clone(), 3),
            vec![(0, 10), (30, 50), (100, 120)]
        );
        // A third takes the 20.
        assert_eq!(bound_runs(runs.clone(), 2), vec![(0, 50), (100, 120)]);
        // Ties go to the earlier gap: of the two 5s only the first bridges.
        assert_eq!(
            bound_runs(runs.clone(), 4),
            vec![(0, 10), (30, 50), (100, 110), (115, 120)]
        );
        assert_eq!(bound_runs(runs.clone(), 0), vec![(0, 120)]);
        assert_eq!(bound_runs(runs.clone(), 5), runs);
        assert_eq!(bound_runs(runs.clone(), usize::MAX), runs);
    }

    #[test]
    fn targets_are_spread_and_centred() {
        assert_eq!(
            target_rows(100, 10),
            vec![5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
        );
    }
}
