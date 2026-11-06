//! The measurement behind `structural_read_report` (T13 arm 1, issue #3711).
//! [`crate::structural_read`] holds the spec, the pure layout rules and the
//! decision; this module writes the subject's object with the production
//! writer, publishes it, and runs the three plans against it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::footer::{self, SuffixOutcome, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    ColumnSelection, LogRecord, Predicate, RlogConfig, RlogReader, SegmentDirectories, SparseObject,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, InstrumentedStore, ObjectStoreBackend, PutOptions};
use ravel_query::{LogQuery, LogSegmentFetcher};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};

use crate::harness::DelayedGetStore;
use crate::structural_read::{
    MiniChunk, SHAPES, SubjectSpec, chunks_for_rows, mini_chunks, place_chunks, wall_ms,
};
use crate::t13::{
    Decision, Expect, Figure, Io, NOW_NS, RowMap, SavedDecision, SplitMix, coalesce, decode_all,
    identity, line_body, line_records, line_ts, median, thread_cpu_s, write_rlog,
};

/// The columns every plan decodes: what the journey's row view shows.
pub fn projection() -> ColumnSelection {
    ColumnSelection::fixed_only()
        .with_severity_text()
        .with_body()
}

/// The projected columns' ids, which [`ColumnSelection::resolve`] gives for
/// [`projection`] over any FIELD_DIR.
pub const PROJECTED: [u32; 4] = [
    ravel_logseg::record::COL_TS,
    ravel_logseg::record::COL_STREAM_REF,
    ravel_logseg::record::COL_SEVERITY_TEXT,
    ravel_logseg::record::COL_BODY,
];

pub use crate::t13::Outcome;

struct Store {
    inst: Arc<InstrumentedStore<DelayedGetStore<MemoryStore>>>,
    dyn_store: Arc<dyn ObjectStoreBackend>,
}

impl Store {
    fn gets(&self) -> (u64, u64) {
        let s = self.inst.metrics().snapshot();
        (s.get.calls, s.get.bytes)
    }
}

/// The written object and what the writer put in it.
struct Object {
    bytes: Vec<u8>,
    lines: usize,
    map: RowMap,
    /// Every block's projected records, for the mini-block simulation.
    blocks: Vec<Vec<LogRecord>>,
}

/// Writes the object from the first N lines, N calibrated in at most three
/// proportional passes so the object lands within the tolerance of the
/// target.
fn calibrated_object(
    spec: &SubjectSpec,
    lines: &[Vec<u8>],
    th: &TenantHash,
) -> Result<(Vec<u8>, usize), String> {
    let cfg = RlogConfig {
        block_target_records: spec.block_records,
        zstd_level: spec.zstd_level,
        ..RlogConfig::default()
    };
    let target = spec.object_target_bytes as f64;
    let raw: u64 = lines.iter().map(|l| l.len() as u64 + 1).sum();
    // A first guess at 8x compression; the passes correct it.
    let mut n = ((target * 8.0 / (raw.max(1) as f64 / lines.len().max(1) as f64)) as usize)
        .clamp(1, lines.len().max(1));
    let mut last = None;
    for _ in 0..4 {
        let bytes = write_rlog(
            line_records(&lines[..n], 0),
            cfg,
            crate::t13::identity(th, 1),
        )?;
        let size = bytes.len() as f64;
        if (size - target).abs() <= target * spec.object_tolerance {
            return Ok((bytes, n));
        }
        let next = ((n as f64) * target / size).round() as usize;
        let next = next.clamp(1, lines.len());
        last = Some((bytes, n));
        if next == n {
            break;
        }
        n = next;
    }
    let (bytes, n) = last.ok_or("no object written")?;
    Err(format!(
        "object of {n} lines is {} bytes, outside {} +/- {}; the corpus holds {} lines",
        bytes.len(),
        spec.object_target_bytes,
        spec.object_tolerance,
        lines.len()
    ))
}

fn build_object(spec: &SubjectSpec, lines: &[Vec<u8>], th: &TenantHash) -> Result<Object, String> {
    let (bytes, n) = calibrated_object(spec, lines, th)?;
    let blocks = decode_all(&bytes, &projection())?;
    let counts: Vec<u32> = blocks.iter().map(|b| b.len() as u32).collect();
    let map = RowMap::new(&counts);
    if map.rows() != n as u64 {
        return Err(format!("object holds {} rows, wrote {n}", map.rows()));
    }
    let mut ordinal = 0u64;
    for b in &blocks {
        for r in b {
            if r.ts_ns != line_ts(ordinal) {
                return Err(format!(
                    "row {ordinal} reads back at ts {}, written at {}: the writer reordered rows",
                    r.ts_ns,
                    line_ts(ordinal)
                ));
            }
            ordinal += 1;
        }
    }
    Ok(Object {
        bytes,
        lines: n,
        map,
        blocks,
    })
}

async fn publish_object(
    store: &Store,
    th: TenantHash,
    obj: &Object,
) -> Result<ravel_catalog::SegmentRef, String> {
    let rec = NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard: 0,
        writer_id: uuid::Uuid::from_bytes(identity(&th, 1).writer_id),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: obj.bytes.len() as u64,
        content_hash: *blake3::hash(&obj.bytes).as_bytes(),
        sample_count: obj.lines as u64,
        series_count: 1,
        min_event_ts_ns: line_ts(0),
        max_event_ts_ns: line_ts(obj.lines as u64 - 1),
        min_ingest_ts_ns: line_ts(0),
        max_ingest_ts_ns: line_ts(obj.lines as u64 - 1),
        segment_format_version: u32::from(footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(rec).map_err(|e| format!("commit record: {e}"))?;
    let key = keys::reconstruct_data_key(&rec).map_err(|e| format!("data key: {e}"))?;
    store
        .dyn_store
        .put(
            &key,
            bytes::Bytes::from(obj.bytes.clone()),
            PutOptions::default(),
        )
        .await
        .map_err(|e| format!("put: {e}"))?;
    publish::publish(&store.dyn_store, &rec, &RetryPolicy::default())
        .await
        .map_err(|e| format!("publish: {e}"))?;
    let catalog = Catalog::new(Arc::clone(&store.dyn_store), CatalogConfig::default())
        .map_err(|e| format!("catalog: {e}"))?;
    let snap = catalog
        .resolve(
            &th,
            Signal::Logs,
            TimeRange {
                start_ns: 0,
                end_ns: NOW_NS,
            },
            &[],
            NOW_NS,
        )
        .await
        .map_err(|e| format!("resolve: {e}"))?;
    match snap.segments.as_slice() {
        [s] => Ok(s.clone()),
        other => Err(format!(
            "resolve gave {} segments, published 1",
            other.len()
        )),
    }
}

/// One plan's measurement of one rep.
#[derive(Clone, Copy, Debug)]
struct Rep {
    gets: u64,
    bytes: u64,
    elapsed_s: f64,
    cpu_s: f64,
}

/// What a plan read back for the wanted ordinals: `ts -> body`.
type Rows = HashMap<i64, String>;

fn check_rows(
    plan: &str,
    shape: &str,
    wanted: &[u64],
    lines: &[Vec<u8>],
    got: &Rows,
) -> Vec<String> {
    let mut out = Vec::new();
    for o in wanted {
        let want = line_body(&lines[*o as usize]);
        match got.get(&line_ts(*o)) {
            Some(b) if *b == want => {}
            Some(_) => out.push(format!(
                "{shape}/{plan}: row {o} read back a different body"
            )),
            None => out.push(format!("{shape}/{plan}: row {o} not read back")),
        }
    }
    out
}

fn keep_rows(rows: &mut Rows, block: Vec<LogRecord>, ts: &HashSet<i64>) {
    for r in block {
        if ts.contains(&r.ts_ns) {
            rows.insert(r.ts_ns, r.body);
        }
    }
}

async fn run_today(
    store: &Store,
    seg: &ravel_catalog::SegmentRef,
    th: TenantHash,
    wanted_blocks: &[usize],
    all_blocks: &[usize],
    ts: &HashSet<i64>,
) -> Result<(Rep, Rows), String> {
    let (g0, b0) = store.gets();
    let cpu0 = thread_cpu_s();
    let t = Instant::now();
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store.dyn_store));
    let mut scan = fetcher
        .scan_accounted_with_tenant_subset_raw(
            seg,
            th,
            &LogQuery::new(0, NOW_NS),
            &projection(),
            wanted_blocks,
            all_blocks,
            None,
            None,
            None,
            &QueryAccounting::new(),
        )
        .await
        .map_err(|e| format!("today: {e}"))?
        .ok_or("today: the fetcher pruned the object")?;
    let mut rows = Rows::new();
    while let Some(b) = scan
        .next_block()
        .map_err(|e| format!("today decode: {e}"))?
    {
        keep_rows(&mut rows, b, ts);
    }
    let elapsed_s = t.elapsed().as_secs_f64();
    let cpu_s = thread_cpu_s() - cpu0;
    let (g1, b1) = store.gets();
    Ok((
        Rep {
            gets: g1 - g0,
            bytes: b1 - b0,
            elapsed_s,
            cpu_s,
        },
        rows,
    ))
}

/// The structural read's metadata rounds: one suffix GET (a second when the
/// footer is longer than the suffix), then one coalesced round for the
/// directory sections the suffix did not cover.
async fn structural_dirs(
    io: &Io,
    total: u64,
    spec: &SubjectSpec,
) -> Result<(SparseObject, SegmentDirectories, u64), String> {
    let mut sparse = SparseObject::new(total);
    let suffix_len = spec.structural_suffix_bytes.min(total);
    let suffix = io.get(GetRange::Suffix(suffix_len)).await?;
    let mut held_from = total - suffix.len() as u64;
    sparse
        .place(held_from, suffix.clone())
        .map_err(|e| format!("place suffix: {e}"))?;
    let foot = match footer::open_from_suffix(&suffix, total).map_err(|e| format!("footer: {e}"))? {
        SuffixOutcome::Ready(f) => f,
        SuffixOutcome::NeedRange { offset, len } => {
            let b = io.get(GetRange::Range(offset, offset + len)).await?;
            sparse
                .place(offset, b.clone())
                .map_err(|e| format!("place footer: {e}"))?;
            held_from = held_from.min(offset);
            match footer::open_from_suffix(&b, total).map_err(|e| format!("footer: {e}"))? {
                SuffixOutcome::Ready(f) => f,
                SuffixOutcome::NeedRange { .. } => return Err("footer needs a third range".into()),
            }
        }
    };
    let mut missing = Vec::new();
    for k in [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
        kind::KEY_IDX,
    ] {
        if let Some(s) = foot.section(k)
            && s.offset < held_from
        {
            missing.push((s.offset, s.len.min(held_from - s.offset)));
        }
    }
    for (start, b) in io
        .get_ranges(&coalesce(&missing, spec.coalesce_gap))
        .await?
    {
        sparse
            .place(start, b)
            .map_err(|e| format!("place dirs: {e}"))?;
    }
    let dirs = RlogReader::<SparseObject>::decode_directories(&sparse, &RlogConfig::default())
        .map_err(|e| format!("directories: {e}"))?;
    let blocks_off = foot
        .section(kind::BLOCKS)
        .map(|s| s.offset)
        .ok_or("no BLOCKS section")?;
    Ok((sparse, dirs, blocks_off))
}

async fn run_page_exact(
    store: &Store,
    io: &Io,
    total: u64,
    spec: &SubjectSpec,
    wanted_blocks: &[usize],
    all_blocks: &[usize],
    ts: &HashSet<i64>,
) -> Result<(Rep, Rows), String> {
    let (g0, b0) = store.gets();
    let cpu0 = thread_cpu_s();
    let t = Instant::now();
    let (mut sparse, dirs, blocks_off) = structural_dirs(io, total, spec).await?;
    let cols = projection().resolve(dirs.field_dir());
    let pd = Arc::clone(dirs.page_dir());
    let mut extents = Vec::new();
    for (gi, g) in pd.groups.iter().enumerate() {
        let within: Vec<u32> = wanted_blocks
            .iter()
            .filter_map(|b| (*b as u32).checked_sub(g.first_block))
            .filter(|w| *w < g.block_count)
            .collect();
        if within.is_empty() {
            continue;
        }
        let r = pd
            .projected_page_ranges(gi, &within, cols.as_ref())
            .ok_or("projected_page_ranges")?;
        extents.extend(r.into_iter().map(|(o, l)| (blocks_off + o, l)));
    }
    for (start, b) in io
        .get_ranges(&coalesce(&extents, spec.coalesce_gap))
        .await?
    {
        sparse
            .place(start, b)
            .map_err(|e| format!("place pages: {e}"))?;
    }
    let reader = RlogReader::from_decoded(&sparse, &dirs);
    let mut scan = reader
        .scan_blocks_raw_subset(
            &Predicate::And(vec![]),
            &[],
            &projection(),
            wanted_blocks,
            all_blocks,
        )
        .map_err(|e| format!("page_exact scan: {e}"))?;
    let mut rows = Rows::new();
    while let Some(b) = scan
        .next_block(&sparse)
        .map_err(|e| format!("page_exact decode: {e}"))?
    {
        keep_rows(&mut rows, b, ts);
    }
    let elapsed_s = t.elapsed().as_secs_f64();
    let cpu_s = thread_cpu_s() - cpu0;
    let (g1, b1) = store.gets();
    Ok((
        Rep {
            gets: g1 - g0,
            bytes: b1 - b0,
            elapsed_s,
            cpu_s,
        },
        rows,
    ))
}

/// One (block, column) page of the simulated mini-block layout.
struct MiniPage {
    chunks: Vec<MiniChunk>,
    /// Each chunk's compressed values.
    stored: Vec<Vec<u8>>,
    /// Each chunk's absolute extent inside the real page.
    extents: Vec<(u64, u64)>,
    /// The real column chunk's dictionary page, absolute, when it has one.
    dict: Option<(u64, u64)>,
}

/// The simulated layout: per block, per projected column.
pub struct MiniLayout {
    pages: HashMap<(u32, u32), MiniPage>,
    pub total_chunks: u64,
    /// (block, column) pairs the real object stores no page for.
    pub elided_pages: u64,
}

fn serialize(col: u32, r: &LogRecord) -> Vec<u8> {
    let text = |s: &str| {
        let mut v = Vec::with_capacity(s.len() + 2);
        ravel_logseg::varint::put_uvarint(&mut v, s.len() as u64);
        v.extend_from_slice(s.as_bytes());
        v
    };
    match col {
        ravel_logseg::record::COL_TS => r.ts_ns.to_le_bytes().to_vec(),
        // One stream per object: every row references stream 0.
        ravel_logseg::record::COL_STREAM_REF => 0u32.to_le_bytes().to_vec(),
        ravel_logseg::record::COL_SEVERITY_TEXT => text(&r.severity_text),
        _ => text(&r.body),
    }
}

fn zstd_len(level: i32) -> impl Fn(&[u8]) -> Result<usize, String> {
    move |b: &[u8]| {
        zstd::bulk::compress(b, level)
            .map(|c| c.len())
            .map_err(|e| format!("zstd: {e}"))
    }
}

/// The data pages' extent (dictionary page excluded) of `col` for `block`,
/// absolute, and the column chunk's dictionary page.
/// `None` when the block stores no page for the column (a column the writer
/// elides for the block, such as one constant over the whole group).
pub type RealPage = (Option<(u64, u64)>, Option<(u64, u64)>);

pub fn real_page(pd: &PageDir, blocks_off: u64, block: u32, col: u32) -> RealPage {
    let Some((g, within)) = pd.locate_block(block) else {
        return (None, None);
    };
    let Some(chunk) = g.chunks.iter().find(|c| c.column_id == col) else {
        return (None, None);
    };
    let Some(offs) = chunk.page_offsets() else {
        return (None, None);
    };
    let mut lo = u64::MAX;
    let mut hi = 0u64;
    let skip = usize::from(chunk.dict_page().is_some());
    for (p, o) in chunk.pages.iter().zip(offs).skip(skip) {
        if p.block == within {
            lo = lo.min(o);
            hi = hi.max(o + p.len);
        }
    }
    let dict = chunk
        .dict_page()
        .map(|d| (blocks_off + chunk.offset, d.len));
    ((lo < hi).then_some((blocks_off + lo, hi - lo)), dict)
}

pub fn mini_layout(
    bytes: &[u8],
    blocks: &[Vec<LogRecord>],
    spec: &SubjectSpec,
) -> Result<MiniLayout, String> {
    let foot = footer::open(bytes).map_err(|e| format!("footer: {e}"))?;
    let blocks_off = foot
        .section(kind::BLOCKS)
        .map(|s| s.offset)
        .ok_or("no BLOCKS section")?;
    let cfg = RlogConfig::default();
    let dirs = RlogReader::<[u8]>::decode_directories(bytes, &cfg)
        .map_err(|e| format!("directories: {e}"))?;
    let pd = Arc::clone(dirs.page_dir());
    let compress = zstd_len(spec.zstd_level);
    let mut pages = HashMap::new();
    let mut total_chunks = 0u64;
    let mut elided_pages = 0u64;
    for (b, recs) in blocks.iter().enumerate() {
        for col in PROJECTED {
            let values: Vec<Vec<u8>> = recs.iter().map(|r| serialize(col, r)).collect();
            let chunks = mini_chunks(
                &values,
                spec.mini_max_rows,
                spec.mini_chunk_bytes,
                &compress,
            )?;
            let (data, dict) = real_page(&pd, blocks_off, b as u32, col);
            let stored = chunks
                .iter()
                .map(|c| {
                    zstd::bulk::compress(
                        &values[c.first_row..c.first_row + c.rows].concat(),
                        spec.zstd_level,
                    )
                    .map_err(|e| format!("zstd: {e}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            total_chunks += chunks.len() as u64;
            // A column the block stores no page for costs no chunk bytes in
            // the simulation either.
            let extents = match data {
                Some((start, len)) => place_chunks(start, len, &chunks),
                None => {
                    elided_pages += 1;
                    Vec::new()
                }
            };
            pages.insert(
                (b as u32, col),
                MiniPage {
                    chunks,
                    stored,
                    extents,
                    dict,
                },
            );
        }
    }
    Ok(MiniLayout {
        pages,
        total_chunks,
        elided_pages,
    })
}

/// Parses `n` serialized values of `col` back out of a decompressed chunk.
fn parse_values(col: u32, data: &[u8], n: usize) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::with_capacity(n);
    let mut pos = 0usize;
    for _ in 0..n {
        let len = match col {
            ravel_logseg::record::COL_TS => 8,
            ravel_logseg::record::COL_STREAM_REF => 4,
            _ => ravel_logseg::varint::get_uvarint(data, &mut pos)
                .map_err(|e| format!("chunk: {e}"))? as usize,
        };
        let v = data.get(pos..pos + len).ok_or("chunk: short value")?;
        out.push(v.to_vec());
        pos += len;
    }
    if pos != data.len() {
        return Err("chunk: trailing bytes".into());
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn run_miniblock(
    store: &Store,
    io: &Io,
    total: u64,
    spec: &SubjectSpec,
    layout: &MiniLayout,
    map: &RowMap,
    wanted: &[u64],
) -> Result<(Rep, Rows), String> {
    let (g0, b0) = store.gets();
    let cpu0 = thread_cpu_s();
    let t = Instant::now();
    let (_sparse, _dirs, _) = structural_dirs(io, total, spec).await?;
    let mut by_block: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for o in wanted {
        let (b, r) = map.locate(*o).ok_or("ordinal past the object")?;
        by_block.entry(b).or_default().push(r);
    }
    let mut extents = Vec::new();
    let mut needed: Vec<(u32, u32, usize)> = Vec::new();
    for (b, rows) in &by_block {
        for col in PROJECTED {
            let page = layout.pages.get(&(*b, col)).ok_or("mini page missing")?;
            for c in chunks_for_rows(&page.chunks, rows) {
                if let Some(e) = page.extents.get(c) {
                    extents.push(*e);
                }
                needed.push((*b, col, c));
            }
            if let Some(d) = page.dict {
                extents.push(d);
            }
        }
    }
    let _fetched = io
        .get_ranges(&coalesce(&extents, spec.coalesce_gap))
        .await?;
    // Decode the needed chunks' simulated contents.
    let mut values: HashMap<(u32, u32), Vec<Vec<u8>>> = HashMap::new();
    for (b, col, c) in needed {
        let page = &layout.pages[&(b, col)];
        let chunk = page.chunks[c];
        let data = zstd::bulk::decompress(&page.stored[c], spec.mini_chunk_bytes.max(1 << 20) * 64)
            .map_err(|e| format!("unzstd: {e}"))?;
        let vals = parse_values(col, &data, chunk.rows)?;
        let slot = values.entry((b, col)).or_default();
        if slot.len() < chunk.first_row + chunk.rows {
            slot.resize(chunk.first_row + chunk.rows, Vec::new());
        }
        for (i, v) in vals.into_iter().enumerate() {
            slot[chunk.first_row + i] = v;
        }
    }
    let mut rows = Rows::new();
    for (b, rs) in &by_block {
        for r in rs {
            let ts = values
                .get(&(*b, ravel_logseg::record::COL_TS))
                .and_then(|v| v.get(*r as usize))
                .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
                .map(i64::from_le_bytes)
                .ok_or("miniblock: ts missing")?;
            let body = values
                .get(&(*b, ravel_logseg::record::COL_BODY))
                .and_then(|v| v.get(*r as usize))
                .ok_or("miniblock: body missing")?;
            rows.insert(ts, String::from_utf8_lossy(body).into_owned());
        }
    }
    let elapsed_s = t.elapsed().as_secs_f64();
    let cpu_s = thread_cpu_s() - cpu0;
    let (g1, b1) = store.gets();
    Ok((
        Rep {
            gets: g1 - g0,
            bytes: b1 - b0 + layout.total_chunks * spec.mini_meta_bytes_per_chunk,
            elapsed_s,
            cpu_s,
        },
        rows,
    ))
}

/// The wanted ordinals of a shape: `scattered` draws distinct rows,
/// `clustered` takes consecutive rows from a drawn start.
pub fn wanted_rows(shape: &str, rows: usize, total: u64, seed: u64) -> Vec<u64> {
    if shape == "scattered" {
        SplitMix::new(seed).distinct(rows, total)
    } else {
        let n = rows as u64;
        let start = SplitMix::new(seed.wrapping_add(1)).below(total.saturating_sub(n) + 1);
        (start..(start + n).min(total)).collect()
    }
}

/// Medians over reps of one plan; GETs and bytes must not vary.
fn settle(plan: &str, shape: &str, reps: &[Rep], problems: &mut Vec<String>) -> Rep {
    let first = reps[0];
    if reps
        .iter()
        .any(|r| r.gets != first.gets || r.bytes != first.bytes)
    {
        problems.push(format!("{shape}/{plan}: GETs or bytes varied across reps"));
    }
    let el: Vec<f64> = reps.iter().map(|r| r.elapsed_s).collect();
    let cpu: Vec<f64> = reps.iter().map(|r| r.cpu_s).collect();
    Rep {
        gets: first.gets,
        bytes: first.bytes,
        elapsed_s: median(&el).unwrap_or(0.0),
        cpu_s: median(&cpu).unwrap_or(0.0),
    }
}

/// Runs one subject on `lines` (already sha-checked) and reports its
/// figures, keyed `<subject>/<figure>`.
pub async fn run_subject(spec: &SubjectSpec, lines: &[Vec<u8>]) -> Result<Outcome, String> {
    let mut out = Outcome::default();
    let tenant = TenantId::new(format!("t13-structural-{}", spec.name));
    let th = tenant.hash();
    let obj = build_object(spec, lines, &th)?;
    let layout = mini_layout(&obj.bytes, &obj.blocks, spec)?;
    let inst = Arc::new(InstrumentedStore::new(DelayedGetStore::new(
        MemoryStore::new(),
        Duration::from_millis(spec.get_latency_ms),
    )));
    let store = Store {
        dyn_store: Arc::clone(&inst) as Arc<dyn ObjectStoreBackend>,
        inst,
    };
    let seg = publish_object(&store, th, &obj).await?;
    let total = obj.bytes.len() as u64;
    let io = Io {
        store: Arc::clone(&store.dyn_store),
        key: seg.data_object_key.clone(),
        concurrency: spec.concurrency,
    };
    let p = |k: &str, v: f64| Figure {
        key: format!("{}/{k}", spec.name),
        value: v,
    };
    out.figures.push(p("object/lines", obj.lines as f64));
    out.figures.push(p("object/bytes", total as f64));
    out.figures
        .push(p("object/blocks", obj.map.blocks() as f64));
    let meta = layout.total_chunks * spec.mini_meta_bytes_per_chunk;
    out.figures.push(p("miniblock/meta_bytes", meta as f64));
    out.notes.push(format!(
        "miniblock: {} chunks over {} (block, column) pages, {} of them stored with no page in the real object",
        layout.total_chunks,
        obj.map.blocks() * PROJECTED.len(),
        layout.elided_pages
    ));
    let all_blocks: Vec<usize> = (0..obj.map.blocks()).collect();
    for shape in SHAPES {
        let wanted = wanted_rows(shape, spec.rows, obj.map.rows(), spec.seed);
        let ts: HashSet<i64> = wanted.iter().map(|o| line_ts(*o)).collect();
        let mut wanted_blocks: Vec<usize> = wanted
            .iter()
            .filter_map(|o| obj.map.locate(*o).map(|(b, _)| b as usize))
            .collect();
        wanted_blocks.dedup();
        out.notes.push(format!(
            "{shape}: rows {wanted:?} in blocks {wanted_blocks:?}"
        ));
        out.figures.push(p(
            &format!("{shape}/blocks_touched"),
            wanted_blocks.len() as f64,
        ));
        let (mut today, mut exact, mut mini) = (Vec::new(), Vec::new(), Vec::new());
        for rep in 0..spec.reps {
            let (r, rows) = run_today(&store, &seg, th, &wanted_blocks, &all_blocks, &ts).await?;
            if rep == 0 {
                out.problems
                    .extend(check_rows("today", shape, &wanted, lines, &rows));
            }
            today.push(r);
            let (r, rows) =
                run_page_exact(&store, &io, total, spec, &wanted_blocks, &all_blocks, &ts).await?;
            if rep == 0 {
                out.problems
                    .extend(check_rows("page_exact", shape, &wanted, lines, &rows));
            }
            exact.push(r);
            let (r, rows) =
                run_miniblock(&store, &io, total, spec, &layout, &obj.map, &wanted).await?;
            if rep == 0 {
                out.problems
                    .extend(check_rows("miniblock", shape, &wanted, lines, &rows));
            }
            mini.push(r);
        }
        let today = settle("today", shape, &today, &mut out.problems);
        let today_wall = wall_ms(today.elapsed_s, today.bytes, spec.bandwidth_mib_s);
        out.figures
            .push(p(&format!("{shape}/today/gets"), today.gets as f64));
        out.figures
            .push(p(&format!("{shape}/today/bytes"), today.bytes as f64));
        out.figures
            .push(p(&format!("{shape}/today/wall_ms"), today_wall));
        out.figures.push(p(
            &format!("{shape}/today/decode_cpu_ms"),
            today.cpu_s * 1000.0,
        ));
        for (v, reps) in [("page_exact", exact), ("miniblock", mini)] {
            let r = settle(v, shape, &reps, &mut out.problems);
            let wall = wall_ms(r.elapsed_s, r.bytes, spec.bandwidth_mib_s);
            let ratio = |a: f64, b: f64| if b > 0.0 { a / b } else { f64::INFINITY };
            out.figures
                .push(p(&format!("{shape}/{v}/gets"), r.gets as f64));
            out.figures
                .push(p(&format!("{shape}/{v}/bytes"), r.bytes as f64));
            out.figures
                .push(p(&format!("{shape}/{v}/decode_cpu_ms"), r.cpu_s * 1000.0));
            out.figures.push(p(
                &format!("{shape}/{v}/bytes_ratio"),
                ratio(r.bytes as f64, today.bytes as f64),
            ));
            out.figures.push(p(
                &format!("{shape}/{v}/gets_ratio"),
                ratio(r.gets as f64, today.gets as f64),
            ));
            out.figures.push(p(
                &format!("{shape}/{v}/wall_ratio"),
                ratio(wall, today_wall),
            ));
        }
    }
    Ok(out)
}

/// Arm 1 as the report bin drives it.
pub struct StructuralRead;

impl crate::t13::Arm for StructuralRead {
    const NAME: &'static str = crate::structural_read::ARM;
    type Subject = SubjectSpec;

    fn subjects(spec_text: &str) -> Result<Vec<SubjectSpec>, String> {
        Ok(crate::structural_read::parse(spec_text)?.subject)
    }
    fn name(s: &SubjectSpec) -> &str {
        &s.name
    }
    fn corpus_sha256(s: &SubjectSpec) -> &str {
        &s.corpus_sha256
    }
    fn max_lines(s: &SubjectSpec) -> u64 {
        s.max_lines
    }
    fn expectations(s: &SubjectSpec) -> BTreeMap<String, Expect> {
        s.expectations()
    }
    fn measure(s: &SubjectSpec, lines: &[Vec<u8>]) -> Result<Outcome, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| format!("tokio runtime: {e}"))?;
        rt.block_on(run_subject(s, lines))
    }
    fn decide(subject: &str, figures: &[Figure]) -> Decision {
        crate::structural_read::decide(subject, figures)
    }
    fn overall(subjects: &[String], saved: &[SavedDecision]) -> Decision {
        crate::structural_read::overall(subjects, saved)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn wanted_rows_are_pinned_by_seed() {
        let s = wanted_rows("scattered", 3, 1000, 7);
        assert_eq!(s, SplitMix::new(7).distinct(3, 1000));
        let c = wanted_rows("clustered", 3, 1000, 7);
        assert_eq!(c.len(), 3);
        assert_eq!(c[1], c[0] + 1);
        assert_eq!(c[2], c[0] + 2);
        assert_eq!(wanted_rows("clustered", 5, 3, 7), vec![0, 1, 2]);
    }

    #[test]
    fn parse_values_reads_what_serialize_wrote() {
        let rec = line_records(&[b"hello world".to_vec()], 4).remove(0);
        let mut chunk = Vec::new();
        for _ in 0..3 {
            chunk.extend(serialize(ravel_logseg::record::COL_BODY, &rec));
        }
        let vals = parse_values(ravel_logseg::record::COL_BODY, &chunk, 3).expect("parse");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[2], b"hello world".to_vec());
        assert!(parse_values(ravel_logseg::record::COL_BODY, &chunk, 2).is_err());
        let ts = serialize(ravel_logseg::record::COL_TS, &rec);
        assert_eq!(ts, line_ts(4).to_le_bytes().to_vec());
    }

    /// The whole pipeline on a small synthetic corpus with no injected
    /// latency: every plan must read the wanted rows back, the structural
    /// plans never read more than today's, and the GET and byte figures are
    /// stable across reps.
    #[tokio::test(flavor = "current_thread")]
    async fn every_plan_reads_the_wanted_rows_back() {
        let lines: Vec<Vec<u8>> = (0..4000)
            .map(|i| {
                format!(
                    "{i:06} host-{} request handled in {} ms status ok",
                    i % 7,
                    (i * 37) % 1000
                )
                .into_bytes()
            })
            .collect();
        let spec = crate::structural_read::parse(
            &crate::structural_read::tests_support::spec_toml(16, 40_000, 0.9),
        )
        .expect("spec")
        .subject
        .remove(0);
        let out = run_subject(&spec, &lines).await.expect("run");
        assert!(out.problems.is_empty(), "{:?}", out.problems);
        let get = |k: &str| {
            out.figures
                .iter()
                .find(|f| f.key == format!("s1/{k}"))
                .map(|f| f.value)
                .unwrap_or_else(|| panic!("{k} missing"))
        };
        assert_eq!(
            out.figures.len(),
            crate::structural_read::figure_names().len()
        );
        for shape in SHAPES {
            assert!(
                get(&format!("{shape}/page_exact/bytes")) <= get(&format!("{shape}/today/bytes"))
            );
            assert!(get(&format!("{shape}/blocks_touched")) >= 1.0);
        }
    }
}
