//! ADR-2414 decision A1: the striped logs read route decodes a segment's footer
//! directories once per (query, segment), not once per open.
//!
//! The striped route runs when the SQL partition count exceeds the query's
//! segment count (the whole-segment fast path refuses with
//! `FewerSegmentsThanPartitions`). It used to open the same object once per
//! partition that owned one of its blocks, and every open decoded STREAM_DIR,
//! FIELD_DIR, SKIP_IDX and PAGE_DIR again (once on the fetch side, once in the
//! reader, and, with a read gate wired, PAGE_DIR a third time for the gate's job
//! size), charging each decode to `decompressed_bytes`. A 1-of-N projection
//! through that route then paid several times the whole-object path's
//! decompression for the same rows.
//!
//! Now the plan phase decodes the four directories once per segment and charges
//! them to the plan phase, and every per-partition open takes them from that
//! decode and decodes only the blocks it owns. For one projection, per segment:
//!
//! - the plan phase's `decompressed_bytes` is the four sections' `uncomp_len`,
//!   read off the object's own footer;
//! - the scan phase's `decompressed_bytes` is the projected block pages only,
//!   which a whole-object [`RlogReader`] reports independently;
//! - the whole-segment fast path, which opens each object once, charges its scan
//!   phase the same directories and pages plus its ranged fetch's SKIP_IDX +
//!   PAGE_DIR + FIELD_DIR and the read gate's PAGE_DIR, each once per object.
//!   Those do not grow with the partition count, and the striped route no
//!   longer pays them per open either.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate};
use ravel_logseg::footer::{COMP_ZSTD, kind};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, Predicate, RlogConfig, RlogReader, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    BlockRangeFetcher, CacheFetchError, LogSegmentFetcher, PhaseAccounting, PhaseAccountingSnapshot,
};
use ravel_sql::{DeclaredColumn, DeclaredType, LOG_COL_BODY, LogsTableProvider};
use ravel_types::TenantHash;
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

const SEGMENTS: usize = 2;
/// Row groups per object, and blocks per row group: 12 blocks, 3 groups.
const GROUPS: usize = 3;
const GROUP_BLOCKS: usize = 4;
const BLOCKS_PER_SEG: usize = GROUPS * GROUP_BLOCKS;
/// Declared attribute columns beside the nine fixed ones, so a 1-of-N
/// projection leaves most of an object's chunks unread.
const ATTR_COLS: usize = 8;
/// More partitions than segments, so the whole-segment fast path refuses and
/// the striped route runs; and as many as segments, so the fast path runs.
const STRIPED_PARTS: usize = 8;
const FAST_PARTS: usize = SEGMENTS;
const SUFFIX_LEN: u64 = 8192;

fn attr_name(i: usize) -> String {
    format!("c{i}")
}

fn declared() -> Vec<DeclaredColumn> {
    (0..ATTR_COLS)
        .map(|i| DeclaredColumn::new(attr_name(i), DeclaredType::I64))
        .collect()
}

/// Printable filler that zstd shrinks, so the projected `body` column's pages
/// are zstd pages and show up in `decompressed_bytes`.
fn filler(seed: u64, len: usize) -> String {
    const WORDS: [&str; 8] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    ];
    let mut state = seed;
    let mut out = String::with_capacity(len + 8);
    while out.len() < len {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push_str(WORDS[(state >> 60) as usize & 7]);
        out.push(' ');
    }
    out
}

fn record(seg: usize, blk: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    let ts = (seg * 1_000_000 + blk) as i64;
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: filler(((seg as u64) << 32) | blk as u64, 2048),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: (0..ATTR_COLS)
            .map(|c| (attr_name(c), AttrValue::I64((blk * 31 + c) as i64)))
            .collect(),
    }
}

fn writer_config() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        group_target_blocks: GROUP_BLOCKS,
        ..RlogConfig::default()
    }
}

/// One object per segment, and the raw bytes so a test can read each footer.
async fn write_segment(store: &dyn ObjectStoreBackend, seg: usize) -> (SegmentRef, Vec<u8>) {
    let recs: Vec<LogRecord> = (0..BLOCKS_PER_SEG).map(|b| record(seg, b)).collect();
    write_records(store, seg, recs).await
}

async fn write_records(
    store: &dyn ObjectStoreBackend,
    seg: usize,
    recs: Vec<LogRecord>,
) -> (SegmentRef, Vec<u8>) {
    let identity = ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: (seg + 1) as u64,
    };
    let mut writer = RlogWriter::new(writer_config(), identity);
    for r in &recs {
        writer.push(r.clone()).expect("push");
    }
    let bytes = writer.finish().expect("finish");
    let key = format!("logs/dir_seg{seg}.rlog");
    store
        .put(
            &key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let seg_ref = SegmentRef {
        data_object_key: key,
        object_size: bytes.len() as u64,
        min_event_ts_ns: recs.iter().map(|r| r.ts_ns).min().unwrap(),
        max_event_ts_ns: recs.iter().map(|r| r.ts_ns).max().unwrap(),
        ingest_hour_bucket: 0,
        sample_count: recs.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: (seg + 1) as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (seg_ref, bytes)
}

/// `uncomp_len` of one directory section of `obj`, read off its own footer.
fn section_bytes(obj: &[u8], k: u32) -> u64 {
    let footer = ravel_logseg::footer::open(obj).expect("footer");
    let desc = *footer.section(k).expect("directory section");
    assert_eq!(
        desc.comp, COMP_ZSTD,
        "fixture section {k} must be zstd so uncomp_len is what zstd produced"
    );
    desc.uncomp_len
}

/// What one decode of STREAM_DIR, FIELD_DIR, SKIP_IDX and PAGE_DIR produces.
fn directory_bytes(obj: &[u8]) -> u64 {
    [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
    ]
    .iter()
    .map(|k| section_bytes(obj, *k))
    .sum()
}

/// Bytes a whole-object [`RlogReader`] decompresses scanning `obj` under
/// `columns`, over the inclusive ts range `[lo, hi]`.
fn reader_decompressed(obj: &[u8], columns: &ColumnSelection, lo: i64, hi: i64) -> u64 {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(obj, &cfg).expect("open");
    let mut scan = reader
        .scan_blocks(
            &Predicate::TsRange {
                min_ns: lo,
                max_ns: hi,
            },
            &[],
            columns,
        )
        .expect("scan");
    while scan.next_block(obj).expect("next").is_some() {}
    scan.stats().decompressed_bytes
}

/// The projected block pages of `obj` alone: a whole-object reader's total for
/// the `body` projection, less what a scan over an empty ts range reports (the
/// directories the open decompressed).
fn page_bytes(obj: &[u8]) -> u64 {
    let columns = ColumnSelection::fixed_only().with_body();
    reader_decompressed(obj, &columns, i64::MIN, i64::MAX)
        - reader_decompressed(obj, &columns, i64::MAX - 1, i64::MAX)
}

/// A fetcher that ranges every object (threshold 0), with a read cache, so the
/// striped route stripes blocks, and a read gate wired the way the server wires
/// one, so the gate's job size is computed on every open.
fn fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        .with_coalesce_gap(0)
        .with_whole_object_threshold(0);
    let cache_bytes = 64u64 << 20;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        (cache_bytes / 4096) as usize,
        cache_bytes,
    )));
    let gate = Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 2,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ));
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(cache)
        .with_block_range_threshold(0)
        .with_read_gate(gate)
}

/// The same cache and read gate as [`fetcher`] with the fetcher's default
/// thresholds, so these small objects are read whole: the plan phase decodes
/// the directories from their ranged reads and each partition's open takes the
/// whole object's bytes with those directories carried, through
/// `open_scan_on_gate_from_decoded`.
fn whole_object_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    let cache_bytes = 64u64 << 20;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        (cache_bytes / 4096) as usize,
        cache_bytes,
    )));
    let gate = Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 2,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ));
    let block_range = BlockRangeFetcher::new(Arc::clone(&store)).with_suffix_len(SUFFIX_LEN);
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(cache)
        .with_read_gate(gate)
}

/// [`fetcher`] without the zero coalesce gap: the default gap bridges every
/// hole these objects have.
fn default_gap_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    // A probe far shorter than the object, so every block page is a separate
    // ranged read rather than part of the probe.
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(512)
        .with_whole_object_threshold(0);
    let cache_bytes = 64u64 << 20;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        (cache_bytes / 4096) as usize,
        cache_bytes,
    )));
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(cache)
        .with_block_range_threshold(0)
}

fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "LogsScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|c| find(c))
    }
    let scan = find(plan).expect("a LogsScanExec leaf");
    let set = scan.metrics().expect("metrics");
    set.iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

struct Run {
    phases: PhaseAccountingSnapshot,
    rows: usize,
    plan: Arc<dyn ExecutionPlan>,
}

/// Scan every segment through the SQL provider with `target_partitions`
/// partitions, projecting only `body` (1 of the nine fixed columns plus
/// [`ATTR_COLS`] declared ones), and return the per-phase accounting.
async fn run(
    store: &Arc<dyn ObjectStoreBackend>,
    snapshot: &Snapshot,
    target_partitions: usize,
) -> Run {
    run_with(
        store,
        snapshot,
        target_partitions,
        fetcher(Arc::clone(store)),
    )
    .await
}

async fn run_with(
    store: &Arc<dyn ObjectStoreBackend>,
    snapshot: &Snapshot,
    target_partitions: usize,
    fetcher: LogSegmentFetcher,
) -> Run {
    let _ = store;
    run_scan(
        snapshot,
        target_partitions,
        fetcher,
        Some(vec![LOG_COL_BODY]),
        &[],
    )
    .await
}

/// [`run_with`] with any projection (`None` is every column) and filters.
async fn run_scan(
    snapshot: &Snapshot,
    target_partitions: usize,
    fetcher: LogSegmentFetcher,
    projection: Option<Vec<usize>>,
    filters: &[Expr],
) -> Run {
    let phase = PhaseAccounting::new();
    let provider =
        LogsTableProvider::new(snapshot.clone(), TenantHash(TENANT), fetcher, phase.clone())
            .with_declared_columns(declared());
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    let plan = TableProvider::scan(&provider, &ctx.state(), projection.as_ref(), filters, None)
        .await
        .expect("scan");
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    Run {
        phases: phase.snapshot(),
        rows: batches.iter().map(|b| b.num_rows()).sum(),
        plan,
    }
}

/// The corpus, and the sums over its objects of what each decode costs.
struct Fixture {
    store: Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    /// STREAM_DIR + FIELD_DIR + SKIP_IDX + PAGE_DIR, per object, summed.
    directories: u64,
    /// What the fast path's ranged fetch decodes before it opens the reader
    /// (SKIP_IDX + PAGE_DIR + FIELD_DIR, the projection needing FIELD_DIR), per
    /// object, summed.
    fetch_side: u64,
    /// PAGE_DIR alone, per object, summed: the read gate's job size decodes it
    /// once more per open that does not take it from a carried decode.
    page_dir: u64,
    /// The `body` projection's block pages, per object, summed.
    pages: u64,
    /// The objects' lengths, summed.
    object_bytes: u64,
    /// Each segment's object bytes, in snapshot order.
    objects: Vec<Vec<u8>>,
}

async fn fixture() -> Fixture {
    let base = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    let (mut directories, mut fetch_side, mut page_dir, mut pages) = (0u64, 0u64, 0u64, 0u64);
    let mut object_bytes = 0u64;
    let mut objects = Vec::new();
    for s in 0..SEGMENTS {
        let (seg_ref, obj) = write_segment(base.as_ref(), s).await;
        directories += directory_bytes(&obj);
        fetch_side += [kind::SKIP_IDX, kind::PAGE_DIR, kind::FIELD_DIR]
            .iter()
            .map(|k| section_bytes(&obj, *k))
            .sum::<u64>();
        page_dir += section_bytes(&obj, kind::PAGE_DIR);
        pages += page_bytes(&obj);
        object_bytes += obj.len() as u64;
        // The read-gate assertions below catch a second PAGE_DIR decode only
        // when the suffix probe already covers PAGE_DIR: a probe that missed
        // it would fetch (and decode) it separately in every route.
        let page_dir_desc = *ravel_logseg::footer::open(&obj)
            .expect("footer")
            .section(kind::PAGE_DIR)
            .expect("PAGE_DIR");
        assert!(
            page_dir_desc.offset >= (obj.len() as u64).saturating_sub(SUFFIX_LEN),
            "the {SUFFIX_LEN}-byte suffix probe must cover PAGE_DIR in segment {s}"
        );
        segments.push(seg_ref);
        objects.push(obj);
    }
    let store: Arc<dyn ObjectStoreBackend> = base;
    Fixture {
        store,
        snapshot: Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        directories,
        fetch_side,
        page_dir,
        pages,
        object_bytes,
        objects,
    }
}

/// A 1-of-N projection through the striped route decompresses each segment's
/// directories once: the plan phase carries exactly that, the scan phase carries
/// exactly the projected pages, and the total is the whole-object path's figure
/// less the fast path's own per-object extras (its ranged fetch's SKIP_IDX,
/// PAGE_DIR and FIELD_DIR decodes and the read gate's PAGE_DIR decode, which
/// the assertions below subtract).
#[tokio::test]
async fn striped_projection_decompresses_once_per_segment() {
    let fx = fixture().await;
    assert!(
        fx.pages > 0,
        "the projected body pages are zstd, so the page figure is not vacuous"
    );

    let fast = run(&fx.store, &fx.snapshot, FAST_PARTS).await;
    assert_eq!(
        sum_metric(
            &fast.plan,
            "fast_path_rejected_fewer_segments_than_partitions"
        ),
        0,
        "the reference run must take the whole-segment fast path"
    );
    let striped = run(&fx.store, &fx.snapshot, STRIPED_PARTS).await;
    assert_eq!(
        sum_metric(
            &striped.plan,
            "fast_path_rejected_fewer_segments_than_partitions"
        ),
        STRIPED_PARTS,
        "the run under test must be the striped route, one refusal per partition"
    );
    assert_eq!(fast.rows, SEGMENTS * BLOCKS_PER_SEG);
    assert_eq!(striped.rows, fast.rows, "both routes return every row");

    // The reference: one open per object. The whole-object route's own
    // decode is the directories plus the projected pages; the fast path adds its
    // ranged fetch's SKIP_IDX + PAGE_DIR + FIELD_DIR and the read gate's
    // PAGE_DIR once per object, a constant that does not grow with partitions.
    let reader_total = fx.directories + fx.pages;
    assert_eq!(
        fast.phases.scan.decompressed_bytes,
        reader_total + fx.fetch_side + fx.page_dir,
        "fast path: directories and pages, plus its once-per-object fetch-side and gate decodes"
    );
    assert_eq!(fast.phases.plan.decompressed_bytes, 0);

    assert_eq!(
        striped.phases.plan.decompressed_bytes, fx.directories,
        "the plan phase decodes each segment's four directories exactly once"
    );
    assert_eq!(
        striped.phases.scan.decompressed_bytes, fx.pages,
        "the per-partition opens decode only their blocks' pages, no directory"
    );
    assert_eq!(
        striped.phases.plan.decompressed_bytes + striped.phases.scan.decompressed_bytes,
        reader_total,
        "striped total is exactly what one whole-object reader decompresses for the projection"
    );
    assert_eq!(
        fast.phases.scan.decompressed_bytes
            - (striped.phases.plan.decompressed_bytes + striped.phases.scan.decompressed_bytes),
        fx.fetch_side + fx.page_dir,
        "the striped route is below the fast path by the fast path's own per-object extras"
    );
}

/// The same decode-once pin on the route that reads whole objects: the plan
/// phase charges each segment's directories once, the scan phase charges the
/// projected pages only, and together they equal one whole-object decode.
///
/// Fails against a reader built from carried directories that still seeds its
/// open-time total (each partition's open charges the directories again, so the
/// scan phase exceeds the pages by one directory decode per open), and against a
/// read gate that decodes PAGE_DIR again to size its job (the scan phase exceeds
/// the pages by `page_dir`). Both are asserted by the exact scan-phase figure.
#[tokio::test]
async fn whole_object_route_decompresses_once_per_segment() {
    let fx = fixture().await;
    let run = run_with(
        &fx.store,
        &fx.snapshot,
        STRIPED_PARTS,
        whole_object_fetcher(Arc::clone(&fx.store)),
    )
    .await;
    assert_eq!(
        sum_metric(
            &run.plan,
            "fast_path_rejected_fewer_segments_than_partitions"
        ),
        STRIPED_PARTS,
        "the run under test must be the striped route, one refusal per partition"
    );
    assert_eq!(run.rows, SEGMENTS * BLOCKS_PER_SEG);
    // The route under test: the plan phase read each whole object once (the
    // suffix probe covers these small objects), and the opens took those bytes
    // with the directories carried, so the scan phase issued no GET.
    assert_eq!(
        run.phases.plan.s3_bytes[ravel_types::accounting::AccountedOp::Get.index()],
        fx.object_bytes,
        "the plan phase holds each whole object"
    );
    assert_eq!(
        run.phases.scan.s3_requests[ravel_types::accounting::AccountedOp::Get.index()],
        0,
        "the opens read the carried whole objects, not the store"
    );
    assert_eq!(
        run.phases.plan.decompressed_bytes, fx.directories,
        "the plan phase decodes each segment's four directories exactly once"
    );
    assert_eq!(
        run.phases.scan.decompressed_bytes, fx.pages,
        "the whole-object opens decode only their blocks' pages, no directory"
    );
    assert_eq!(
        run.phases.plan.decompressed_bytes + run.phases.scan.decompressed_bytes,
        fx.directories + fx.pages,
        "the total is one whole-object decode of the projection"
    );
}

/// One segment of three row groups scanned by two partitions: the round-robin
/// deal gives the first partition groups 0 and 2 and the second group 1. With
/// the fetcher's default coalesce gap, wider than the hole between groups 0 and
/// 2, each partition's ranged read must still stay inside its own groups, so
/// the scan phase issues one GET per owned group and moves exactly each group's
/// projected span: wire bytes across both partitions are the three spans, within
/// the object's length, with no span overlapping another.
///
/// Fails against a plan that bridges over the whole candidate set's holes: the
/// first partition's one run then spans group 1 (two GETs in all, one of them
/// moving group 1's bytes a second time), and against a plan that never bridges
/// for a partition (one GET per column chunk, not one per group).
#[tokio::test]
async fn interleaved_groups_move_each_groups_span_once() {
    let fx = fixture().await;
    let seg = fx.snapshot.segments[0].clone();
    let obj = fx.objects[0].clone();
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let run = run_with(
        &fx.store,
        &snapshot,
        2,
        default_gap_fetcher(Arc::clone(&fx.store)),
    )
    .await;
    assert_eq!(run.rows, BLOCKS_PER_SEG);

    let cfg = RlogConfig::default();
    let reader = RlogReader::new(&obj[..], &cfg).expect("open");
    assert_eq!(reader.row_group_count(), GROUPS);
    let dirs = RlogReader::decode_directories(&obj[..], &cfg).expect("directories");
    let selected = ColumnSelection::fixed_only()
        .with_body()
        .resolve(dirs.field_dir())
        .expect("a narrow projection resolves to a column set");
    let spans: Vec<u64> = (0..GROUPS)
        .map(|g| {
            let chunks: Vec<(u64, u64)> = selected
                .iter()
                .filter_map(|c| reader.column_chunk_range(g, *c))
                .collect();
            let start = chunks.iter().map(|c| c.0).min().expect("a chunk");
            let end = chunks.iter().map(|c| c.0 + c.1).max().expect("a chunk");
            end - start
        })
        .collect();
    let get = ravel_types::accounting::AccountedOp::Get.index();
    // Besides the group spans, each partition places BLOOM (the probe is too
    // short to cover it), one range both partitions ask for and the cache
    // serves with one GET.
    let bloom = ravel_logseg::footer::open(&obj[..])
        .expect("footer")
        .section(kind::BLOOM)
        .expect("BLOOM")
        .len;
    assert_eq!(
        run.phases.scan.s3_requests[get],
        GROUPS as u64 + 1,
        "one GET per owned group, and the shared BLOOM range"
    );
    assert_eq!(
        run.phases.scan.s3_bytes[get],
        spans.iter().sum::<u64>() + bloom,
        "each group's projected span is moved once"
    );
    assert!(run.phases.scan.s3_bytes[get] <= obj.len() as u64);
}

/// Blocks in the short segment: one group of [`GROUP_BLOCKS`] and one of one.
const SHORT_BLOCKS: usize = GROUP_BLOCKS + 1;
/// The value of `c0` that only block 1 of the short segment holds when its
/// middle block is to be pruned, outside [`C0_BELOW`].
const C0_OUTLIER: i64 = 1_000_000;
const C0_BELOW: i64 = 1_000;

/// One segment of [`SHORT_BLOCKS`] blocks. With `prune_middle`, block 1's `c0`
/// is [`C0_OUTLIER`], so `c0 < C0_BELOW` prunes it and leaves a hole inside
/// the first group.
async fn short_segment(prune_middle: bool) -> (Arc<dyn ObjectStoreBackend>, Snapshot, Vec<u8>) {
    let base = Arc::new(MemoryStore::new());
    let recs: Vec<LogRecord> = (0..SHORT_BLOCKS)
        .map(|b| {
            let mut r = record(0, b);
            if prune_middle && b == 1 {
                r.attrs[0].1 = AttrValue::I64(C0_OUTLIER);
            }
            r
        })
        .collect();
    let (seg, obj) = write_records(base.as_ref(), 0, recs).await;
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (base, snapshot, obj)
}

/// A ranged fetcher with a read cache and a probe far shorter than the
/// object, so every block page is a separate ranged read.
fn short_probe_fetcher(
    store: Arc<dyn ObjectStoreBackend>,
    coalesce_gap: Option<u64>,
) -> LogSegmentFetcher {
    let mut block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(512)
        .with_whole_object_threshold(0);
    if let Some(gap) = coalesce_gap {
        block_range = block_range.with_coalesce_gap(gap);
    }
    let cache_bytes = 64u64 << 20;
    let cache: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        (cache_bytes / 4096) as usize,
        cache_bytes,
    )));
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(cache)
        .with_block_range_threshold(0)
}

/// Each row group's span over every column: from its first chunk's start to
/// its last chunk's end.
fn all_column_group_spans(obj: &[u8]) -> Vec<u64> {
    let cfg = RlogConfig::default();
    let reader = RlogReader::new(obj, &cfg).expect("open");
    let dirs = RlogReader::decode_directories(obj, &cfg).expect("directories");
    (0..reader.row_group_count())
        .map(|g| {
            let chunks: Vec<(u64, u64)> = dirs.page_dir().groups[g]
                .chunks
                .iter()
                .filter_map(|c| reader.column_chunk_range(g, c.column_id))
                .collect();
            let start = chunks.iter().map(|c| c.0).min().expect("a chunk");
            let end = chunks.iter().map(|c| c.0 + c.1).max().expect("a chunk");
            end - start
        })
        .collect()
}

fn section_len(obj: &[u8], k: u32) -> u64 {
    ravel_logseg::footer::open(obj)
        .expect("footer")
        .section(k)
        .expect("section")
        .len
}

/// Five blocks in groups of four and one, every column projected, two
/// partitions: the first owns the four-block group, which alone covers more
/// than the coverage threshold of the BLOCKS section, and the second owns the
/// one-block group. Each partition's crossover weighs its own span, so the
/// first issues one covering range of exactly its group and the second one
/// range of its group: wire bytes are the two spans plus the BLOOM range both
/// ask for, within the object's length. One partition, routed through the
/// striped path by a predicate every block passes, owns every block and takes
/// the one whole-object GET.
///
/// Fails against the whole-object crossover on a partial share (the first
/// partition moves the object, and the sum passes its length) and against a
/// collapse onto the object's BLOCKS extent (the first partition moves the
/// section).
#[tokio::test]
async fn a_partial_share_crossover_moves_its_own_span() {
    let (store, snapshot, obj) = short_segment(false).await;
    let spans = all_column_group_spans(&obj);
    assert_eq!(spans.len(), 2, "groups of four and one");
    let blocks_len = section_len(&obj, kind::BLOCKS);
    assert!(
        spans[0] as f64 / blocks_len as f64 >= ravel_query::DEFAULT_LOG_COVERAGE_THRESHOLD,
        "the four-block group must cross the default threshold against the section"
    );
    let bloom = section_len(&obj, kind::BLOOM);
    let get = ravel_types::accounting::AccountedOp::Get.index();

    let run = run_scan(
        &snapshot,
        2,
        short_probe_fetcher(Arc::clone(&store), None),
        None,
        &[],
    )
    .await;
    assert_eq!(run.rows, SHORT_BLOCKS);
    assert_eq!(
        sum_metric(
            &run.plan,
            "fast_path_rejected_fewer_segments_than_partitions"
        ),
        2,
        "the striped route"
    );
    assert_eq!(
        run.phases.scan.s3_requests[get], 3,
        "one covering range per share, and the shared BLOOM range"
    );
    assert_eq!(
        run.phases.scan.s3_bytes[get],
        spans[0] + spans[1] + bloom,
        "each share moves exactly its own span"
    );
    assert!(run.phases.scan.s3_bytes[get] <= obj.len() as u64);

    // One partition with a predicate every block passes: the fast path
    // refuses the predicate, so the striped route runs with one share owning
    // every row group, the full-share crossover this half pins.
    let keep_all = vec![col(attr_name(0)).lt(lit(C0_BELOW))];
    let whole = run_scan(
        &snapshot,
        1,
        short_probe_fetcher(Arc::clone(&store), None),
        None,
        &keep_all,
    )
    .await;
    assert_eq!(whole.rows, SHORT_BLOCKS);
    assert_eq!(
        sum_metric(&whole.plan, "fast_path_rejected_block_predicate"),
        1,
        "the striped route"
    );
    assert_eq!(
        (
            whole.phases.scan.s3_requests[get],
            whole.phases.scan.s3_bytes[get]
        ),
        (1, obj.len() as u64),
        "a full share reads the object in one GET"
    );
}

/// The same segment with block 1 pruned by `c0 < 1000` and no coalesce gap:
/// the first partition's share is blocks 0, 2 and 3, whose pages leave a hole
/// in every column chunk. Without a crossover that share issues one GET per
/// chunk run (bridged down to the L0 cap); its runs cover more than the
/// threshold of its own span, so it issues one covering range of exactly that
/// span instead.
///
/// Fails against a partial share that never crosses over (the first share's
/// GETs are its bridged runs, not one), against the whole-object crossover (the
/// object is moved), and against a collapse onto the object's BLOCKS extent.
#[tokio::test]
async fn a_partial_share_crossover_collapses_a_pruned_hole_into_one_range() {
    let (store, snapshot, obj) = short_segment(true).await;
    let spans = all_column_group_spans(&obj);
    let bloom = section_len(&obj, kind::BLOOM);
    let get = ravel_types::accounting::AccountedOp::Get.index();
    let filters = vec![col(attr_name(0)).lt(lit(C0_BELOW))];
    let run = run_scan(
        &snapshot,
        2,
        short_probe_fetcher(Arc::clone(&store), Some(0)),
        None,
        &filters,
    )
    .await;
    assert_eq!(run.rows, SHORT_BLOCKS - 1, "block 1 is filtered out");
    assert_eq!(
        run.phases.scan.s3_requests[get], 3,
        "one covering range per share, and the shared BLOOM range"
    );
    assert_eq!(run.phases.scan.s3_bytes[get], spans[0] + spans[1] + bloom);
}
