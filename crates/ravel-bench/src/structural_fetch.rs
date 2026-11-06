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
//! coalesced with the production gap and issued as real ranged GETs against
//! the simulated layout, stored beside the object in the same delayed store;
//! the plan is the production plan, run for real, plus the mini-block index
//! (`mini_block_meta_bytes` per mini-block, assumed carried in the page
//! directory GET the plan already makes). Decode CPU is modelled: today's
//! measured decode CPU scaled by the decompressed bytes the structural read
//! would decode over the ones today's read decodes. The structural wall is
//! its measured plan and data GET time plus that modelled CPU.

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
use ravel_query::{LogQuery, LogSegmentFetcher};
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
    "floor_gap/data_ranges",
    "floor_gap/bytes",
    "ratio/bytes",
    "ratio/gets",
    "ratio/wall",
];

pub const RULES: &[Rule] = &[
    Rule {
        key: "ratio/bytes",
        max: 0.25,
    },
    Rule {
        key: "ratio/gets",
        max: 1.5,
    },
    Rule {
        key: "ratio/wall",
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

/// The ranges a structural read of `targets` needs over `layout`: per target,
/// the one mini-block of each page of its block that holds the row, and the
/// whole dictionary page of each column of the block's group that has one.
pub fn structural_plan(
    layout: &[SPage],
    targets: &[Target],
    gap: u64,
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
    let ranges = coalesce(wanted.into_keys().collect(), gap);
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
    let mut writer = RlogWriter::new(cfg.clone(), identity);
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

async fn plan(ctx: &Ctx<'_>) -> Result<Planned, String> {
    let fetcher = LogSegmentFetcher::new(ctx.store.clone())
        .with_config(ctx.cfg.clone())
        .with_max_concurrent_gets(ctx.max_gets);
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

async fn today_once(ctx: &Ctx<'_>, matched: usize) -> Result<TodayRun, String> {
    let (g0, b0, h0) = t13::store_counts(&ctx.metrics);
    let t0 = Instant::now();
    let p = plan(ctx).await?;
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

async fn structural_once(ctx: &Ctx<'_>, ranges: &[(u64, u64)]) -> Result<StructuralRun, String> {
    let p = plan(ctx).await?;
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
                .map_err(|e| format!("get {s}..{e}: {e}"))?;
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
    let splan = structural_plan(&layout, &targets, gap, meta)?;
    let floor = structural_plan(
        &layout,
        &targets,
        ravel_query::DEFAULT_LOG_COALESCE_GAP,
        meta,
    )?;
    let today_uncomp = today_uncomp(&dir, &blocks)?;

    let seg = t13::segment_ref(OBJECT_KEY, &object, rows as u64, (0, rows as i64 - 1));
    let (store, metrics) = t13::remote_store(latency);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    let ctx = Ctx {
        store: store.clone(),
        metrics,
        cfg: &cfg,
        max_gets,
        seg: &seg,
        blocks: &blocks,
    };
    let bound = LogSegmentFetcher::new(store.clone())
        .with_config(cfg.clone())
        .plan_whole_object_bound();
    let (today, structural) = rt.block_on(async {
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
        let mut today = Vec::with_capacity(runs);
        let mut structural = Vec::with_capacity(runs);
        for _ in 0..runs {
            today.push(today_once(&ctx, matched).await?);
            structural.push(structural_once(&ctx, &splan.ranges).await?);
        }
        Ok::<_, String>((today, structural))
    })?;

    let t_plan = same(
        "today plan GETs",
        &today
            .iter()
            .map(|r| (r.plan_gets, r.plan_bytes))
            .collect::<Vec<_>>(),
    )?;
    let t_all = same(
        "today GETs",
        &today.iter().map(|r| (r.gets, r.bytes)).collect::<Vec<_>>(),
    )?;
    let s_plan = same(
        "structural plan GETs",
        &structural
            .iter()
            .map(|r| (r.plan_gets, r.plan_bytes))
            .collect::<Vec<_>>(),
    )?;
    let s_data = same(
        "structural data GETs",
        &structural
            .iter()
            .map(|r| (r.data_gets, r.data_bytes))
            .collect::<Vec<_>>(),
    )?;
    if s_plan != t_plan {
        return Err(format!(
            "the two sides planned differently: {t_plan:?} vs {s_plan:?}"
        ));
    }
    if s_data != (splan.ranges.len() as u64, splan.bytes) {
        return Err(format!(
            "the structural GETs moved {s_data:?}, the plan said {:?}",
            (splan.ranges.len(), splan.bytes)
        ));
    }
    let med = |v: Vec<f64>| t13::median(&v).ok_or_else(|| "no run".to_string());
    let today_wall = med(today.iter().map(|r| r.wall).collect())?;
    let today_cpu = med(today.iter().map(|r| r.cpu).collect())?;
    let s_cpu = today_cpu * splan.uncomp as f64 / today_uncomp.max(1) as f64;
    let s_wall = med(structural.iter().map(|r| r.plan_s + r.data_s).collect())? + s_cpu;
    let s_gets = s_plan.0 + s_data.0;
    let s_bytes = s_plan.1 + splan.meta_bytes + s_data.1;

    let fig = |key: &str, value: f64| Figure {
        key: key.to_string(),
        value,
    };
    let figures = vec![
        fig("rows", rows as f64),
        fig("blocks", per_block.len() as f64),
        fig("object_bytes", object.len() as f64),
        fig("target_blocks", blocks.len() as f64),
        fig("today/plan_gets", t_plan.0 as f64),
        fig("today/plan_bytes", t_plan.1 as f64),
        fig("today/gets", t_all.0 as f64),
        fig("today/bytes", t_all.1 as f64),
        fig("today/uncomp_bytes", today_uncomp as f64),
        fig("today/wall_ms", today_wall * 1e3),
        fig("today/decode_cpu_ms", today_cpu * 1e3),
        fig("structural/layout_bytes", layout_bytes.len() as f64),
        fig("structural/raw_ranges", splan.raw_ranges as f64),
        fig("structural/data_ranges", splan.ranges.len() as f64),
        fig("structural/meta_bytes", splan.meta_bytes as f64),
        fig("structural/gets", s_gets as f64),
        fig("structural/bytes", s_bytes as f64),
        fig("structural/uncomp_bytes", splan.uncomp as f64),
        fig("structural/wall_ms", s_wall * 1e3),
        fig("structural/decode_cpu_ms", s_cpu * 1e3),
        fig("floor_gap/data_ranges", floor.ranges.len() as f64),
        fig(
            "floor_gap/bytes",
            (s_plan.1 + floor.meta_bytes + floor.bytes) as f64,
        ),
        fig("ratio/bytes", s_bytes as f64 / t_all.1 as f64),
        fig("ratio/gets", s_gets as f64 / t_all.0 as f64),
        fig("ratio/wall", s_wall / today_wall),
    ];
    let notes = vec![
        format!(
            "object: {rows} rows from the first lines of the corpus, {} blocks, {} bytes, sized in \
             {attempts} writes; whole-object bound {bound} bytes, so the routed plan restricts reads \
             to the known blocks",
            per_block.len(),
            object.len()
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
         decode CPU is modelled as today's CPU times structural/uncomp_bytes over \
         today/uncomp_bytes; wall is measured plan and data GET time plus that modelled CPU"
            .to_string(),
        format!(
            "coalescing: the structural ranges are coalesced at coalesce_gap = {gap} bytes; the \
             production fetcher's default effective gap is its request cost, {} bytes, so today's \
             ranges coalesce at that gap too. floor_gap/* is the structural plan at the {}-byte \
             floor instead, planned only (no GETs issued): its byte figure includes the plan and \
             mini-block index bytes",
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
            ravel_query::DEFAULT_LOG_COALESCE_GAP
        ),
        format!(
            "medians of {runs} interleaved runs; every GET and byte count was identical across runs"
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
        let plan = structural_plan(&layout, &targets, 0, 2).expect("plan");
        // Block 0 row 7 -> c5 chunk 3: 350..450; block 1 row 0 -> chunk 0:
        // 450..550. Plus the dictionary 0..30 and both column-0 pages.
        assert_eq!(plan.raw_ranges, 5);
        assert_eq!(plan.ranges, vec![(0, 50), (350, 550)]);
        assert_eq!(plan.bytes, 250);
        assert_eq!(plan.uncomp, (30 + 10 + 10 + 100 + 100) * 4);
        // 1 + 1 + 4 + 4 value-page chunks, 2 bytes each.
        assert_eq!(plan.meta_bytes, 20);
        let gapped = structural_plan(&layout, &targets, 300, 2).expect("plan");
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
                2
            )
            .is_err()
        );
    }

    #[test]
    fn targets_are_spread_and_centred() {
        assert_eq!(
            target_rows(100, 10),
            vec![5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
        );
    }
}
