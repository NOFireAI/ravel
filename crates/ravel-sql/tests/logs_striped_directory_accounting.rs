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
    let phase = PhaseAccounting::new();
    let provider =
        LogsTableProvider::new(snapshot.clone(), TenantHash(TENANT), fetcher, phase.clone())
            .with_declared_columns(declared());
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    let projection = vec![LOG_COL_BODY];
    let plan = TableProvider::scan(&provider, &ctx.state(), Some(&projection), &[], None)
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
}

async fn fixture() -> Fixture {
    let base = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    let (mut directories, mut fetch_side, mut page_dir, mut pages) = (0u64, 0u64, 0u64, 0u64);
    let mut object_bytes = 0u64;
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
