//! ADR-2677 decision 6 (issue #2694): an `ORDER BY ts LIMIT k` over `logs`
//! skips every block and segment whose stored minimum `ts` is strictly above
//! the TopK's current threshold, and returns exactly the rows the unskipped
//! scan returns.
//!
//! Every comparison runs the same statement twice through a real
//! [`ravel_sql::build_session`] over a `MemoryStore` fixture, differing in one
//! bit: DataFusion's `enable_topk_dynamic_filter_pushdown`. Off, the TopK never
//! hands its filter down, so the scan reads every block; that run is the
//! oracle the skipping run must match row for row.
//!
//! # The fixture
//!
//! Eight segments of three streams of eight records, blocks of four records,
//! so six blocks per segment and 48 in all. A segment's *rank* sets its `ts`
//! span: rank `r` holds offsets `8 * (5r + k) + r` for `k` in `0..24`, so
//! consecutive ranks overlap, and the `+ r` makes every `ts` unique. Snapshot
//! index `s` has rank `(s + 1) % 8`: the segment written last holds the
//! globally smallest `ts`.
//!
//! Within a segment, stream `j` holds `k = 3i + j` for `i` in `0..8`. The
//! writer sorts rows by stream, so a segment's blocks run stream by stream and
//! their minima are not monotone: `k` = 0, 12, 1, 13, 2, 14 for streams in
//! order 0, 1, 2. A skip that stopped at the first block above the threshold
//! would drop the next stream's smallest rows.
//!
//! Records with `k % 4 == 1` carry severity `DEBUG`, which the `<>` statements
//! filter out above the scan.
//!
//! # Batch size
//!
//! A `FilterExec` coalesces its output up to `batch_size` rows before the TopK
//! sees any of it. The writer's default block holds 8192 records, the same as
//! DataFusion's default `batch_size`, so in production a filtered TopK sees
//! rows about once per block. The fixture's blocks hold four records, so the
//! session's `batch_size` is set to four to keep that ratio; at 8192 the
//! filter would hold back the whole fixture and the TopK would publish its
//! threshold only after the last block.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision,
    SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: [u8; 16] = [9u8; 16];
const TS_BASE: i64 = 1_700_000_000_000_000_000;

const SEGMENTS: usize = 8;
const STREAMS: usize = 3;
const PER_STREAM: usize = 8;
const RECORDS_PER_BLOCK: usize = 4;
const PER_SEGMENT: usize = STREAMS * PER_STREAM;
const BLOCKS_PER_SEGMENT: usize = PER_SEGMENT / RECORDS_PER_BLOCK;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEGMENT;
const TOTAL_RECORDS: usize = SEGMENTS * PER_SEGMENT;

/// Declared `Str` attribute columns, so `SELECT *` projects enough surplus
/// columns for the late-materialization rewrite to fire.
const WIDE_COLUMNS: usize = 12;

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

fn declared_columns() -> Vec<DeclaredColumn> {
    (0..WIDE_COLUMNS)
        .map(|i| DeclaredColumn::new(format!("c{i:02}"), DeclaredType::Str))
        .collect()
}

fn rank(segment: usize) -> usize {
    (segment + 1) % SEGMENTS
}

fn ts_of(rank: usize, k: usize) -> i64 {
    TS_BASE + (8 * (5 * rank + k) + rank) as i64
}

fn log_record(stream: usize, ts: i64, body: String, debug: bool) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{stream}")),
    )];
    let attrs = (0..WIDE_COLUMNS)
        .map(|c| (format!("c{c:02}"), AttrValue::Str(format!("{body}-{c:02}"))))
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        // Same order as `ts`, so a statement led by `observed_ts` has the same
        // answer and must still skip nothing.
        observed_ts_ns: ts,
        severity_num: if debug { 5 } else { 9 },
        severity_text: if debug { "DEBUG" } else { "INFO" }.into(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn segment_records(segment: usize) -> Vec<LogRecord> {
    let r = rank(segment);
    let mut recs = Vec::with_capacity(PER_SEGMENT);
    for j in 0..STREAMS {
        for i in 0..PER_STREAM {
            let k = STREAMS * i + j;
            recs.push(log_record(j, ts_of(r, k), format!("r{r}k{k}"), k % 4 == 1));
        }
    }
    recs
}

async fn write_segment(
    store: &dyn ObjectStoreBackend,
    seq: usize,
    recs: &[LogRecord],
    cfg: RlogConfig,
) -> SegmentRef {
    let mut w = RlogWriter::new(cfg, identity((seq + 1) as u64));
    for r in recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = format!("logs/seg{seq}.rlog");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    SegmentRef {
        data_object_key: key,
        object_size: size,
        min_event_ts_ns: recs.iter().map(|r| r.ts_ns).min().unwrap(),
        max_event_ts_ns: recs.iter().map(|r| r.ts_ns).max().unwrap(),
        ingest_hour_bucket: 0,
        sample_count: recs.len() as u64,
        series_count: STREAMS as u64,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: (seq + 1) as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

fn block_config(group_blocks: Option<usize>) -> RlogConfig {
    let mut cfg = RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        ..RlogConfig::default()
    };
    if let Some(blocks) = group_blocks {
        cfg.group_target_blocks = blocks;
    }
    cfg
}

/// Which records a run is over.
#[derive(Clone, Copy)]
enum Fixture {
    /// The staggered eight-segment fixture the module header describes.
    Staggered,
    /// One segment, one stream, three blocks: `ts` offsets 0, 1, 2, 3 | 3, 4,
    /// 5, 6 | 7, 8, 9, 10. The two rows at offset 3 straddle the first block
    /// boundary, and their bodies differ, so a second sort key decides which
    /// one wins.
    Tied,
}

async fn build_snapshot(
    store: &dyn ObjectStoreBackend,
    fixture: Fixture,
    group_blocks: Option<usize>,
) -> Snapshot {
    let cfg = block_config(group_blocks);
    let mut segments = Vec::new();
    match fixture {
        Fixture::Staggered => {
            for s in 0..SEGMENTS {
                segments.push(write_segment(store, s, &segment_records(s), cfg).await);
            }
        }
        Fixture::Tied => {
            let offsets = [0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10];
            let recs: Vec<LogRecord> = offsets
                .iter()
                .enumerate()
                .map(|(n, &o)| {
                    let body = match n {
                        3 => "tie-m".to_string(),
                        4 => "tie-n".to_string(),
                        _ => format!("row{n}"),
                    };
                    log_record(0, TS_BASE + o, body, false)
                })
                .collect();
            segments.push(write_segment(store, 0, &recs, cfg).await);
        }
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

// ---- running one statement ------------------------------------------------

#[derive(Clone, Copy)]
struct Setup {
    skip: bool,
    fixture: Fixture,
    partitions: usize,
    group_blocks: Option<usize>,
}

impl Setup {
    fn new() -> Self {
        Setup {
            skip: true,
            fixture: Fixture::Staggered,
            partitions: 1,
            group_blocks: None,
        }
    }
    fn skip(mut self, skip: bool) -> Self {
        self.skip = skip;
        self
    }
    fn tied(mut self) -> Self {
        self.fixture = Fixture::Tied;
        self
    }
    fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
    fn group_blocks(mut self, blocks: usize) -> Self {
        self.group_blocks = Some(blocks);
        self
    }
}

struct Run {
    rows: String,
    explain: String,
    blocks_scanned: usize,
    blocks_total: usize,
    blocks_skipped: usize,
    segments_skipped: usize,
}

impl Run {
    fn has_fetch_node(&self) -> bool {
        self.explain.contains("LogsRowFetchExec")
    }
}

fn session(provider: LogsTableProvider, setup: Setup) -> SessionContext {
    let mut config = SqlConfig::default();
    config.engine.fetch_concurrency = setup.partitions;
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        tenant,
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    let ctx = build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
    )
    .expect("session builds");
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.optimizer.enable_topk_dynamic_filter_pushdown = setup.skip;
        options.execution.batch_size = RECORDS_PER_BLOCK;
    }
    ctx
}

fn metric(plan: &Arc<dyn ExecutionPlan>, node: &str, name: &str) -> usize {
    fn walk(plan: &Arc<dyn ExecutionPlan>, node: &str, name: &str, out: &mut usize) {
        if plan.name() == node
            && let Some(set) = plan.metrics()
        {
            *out += set
                .iter()
                .filter(|m| m.value().name() == name)
                .map(|m| m.value().as_usize())
                .sum::<usize>();
        }
        for child in plan.children() {
            walk(child, node, name, out);
        }
    }
    let mut out = 0;
    walk(plan, node, name, &mut out);
    out
}

fn render(rows: &[RecordBatch]) -> String {
    pretty_format_batches(rows)
        .expect("rows render")
        .to_string()
}

async fn run(sql: &str, setup: Setup) -> Run {
    let store = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(store.as_ref(), setup.fixture, setup.group_blocks).await;
    let accounting = QueryAccounting::new();
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        LogSegmentFetcher::new(store as Arc<dyn ObjectStoreBackend>),
        PhaseAccounting::pooled_over(&accounting),
    )
    .with_declared_columns(declared_columns());
    let ctx = session(provider, setup);
    let plan = ctx
        .sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let rows = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("statement runs");
    let run = Run {
        rows: render(&rows),
        explain: displayable(plan.as_ref()).indent(false).to_string(),
        blocks_scanned: metric(&plan, "LogsScanExec", "blocks_scanned"),
        blocks_total: metric(&plan, "LogsScanExec", "blocks_total"),
        blocks_skipped: metric(&plan, "LogsScanExec", "blocks_skipped_by_threshold"),
        segments_skipped: metric(&plan, "LogsScanExec", "segments_skipped_by_threshold"),
    };
    eprintln!(
        "[{}] blocks={}/{} skipped blocks={} segments={}\n{}",
        if setup.skip { "skip" } else { "oracle" },
        run.blocks_scanned,
        run.blocks_total,
        run.blocks_skipped,
        run.segments_skipped,
        run.explain,
    );
    run
}

/// Runs `sql` with the skip off and on, asserts the rows are identical, and
/// returns the skipping run.
async fn same_rows(sql: &str, setup: Setup) -> Run {
    let oracle = run(sql, setup.skip(false)).await;
    assert_eq!(
        (oracle.blocks_skipped, oracle.segments_skipped),
        (0, 0),
        "with the TopK filter not pushed down nothing may be skipped"
    );
    let skipping = run(sql, setup.skip(true)).await;
    assert_eq!(
        skipping.rows, oracle.rows,
        "the skipping scan must return exactly the unskipped rows for {sql}"
    );
    skipping
}

/// The rows `ORDER BY ts LIMIT k` over the staggered fixture must return,
/// computed from the fixture itself: a cross-check on the oracle run.
fn smallest_bodies(k: usize, drop_debug: bool) -> Vec<String> {
    let mut all: Vec<(i64, String, bool)> = (0..SEGMENTS)
        .flat_map(segment_records)
        .map(|r| (r.ts_ns, r.body, r.severity_text == "DEBUG"))
        .collect();
    all.sort();
    all.into_iter()
        .filter(|(_, _, debug)| !(drop_debug && *debug))
        .take(k)
        .map(|(_, body, _)| body)
        .collect()
}

fn assert_bodies_in_order(rows: &str, expected: &[String]) {
    let mut at = 0;
    for body in expected {
        let found = rows[at..]
            .find(&format!("| {body} "))
            .unwrap_or_else(|| panic!("{body} missing or out of order in\n{rows}"));
        at += found + body.len();
    }
}

// ---- the tests ------------------------------------------------------------

/// The ClickBench q25 shape: a `<>` filter above the scan and `ORDER BY ts
/// LIMIT 10`. The rows match the unskipped run and the fixture's own top
/// ten, and the scan reads well under half the blocks. On one partition every
/// block the scan does not read is either skipped inside an opened segment or
/// belongs to a segment it never opened.
#[tokio::test]
async fn filtered_order_by_ts_limit_matches_and_skips() {
    let sql = "SELECT ts, body FROM logs WHERE severity_text <> 'DEBUG' ORDER BY ts LIMIT 10";
    let run = same_rows(sql, Setup::new()).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(10, true));
    assert!(
        run.blocks_scanned * 4 < TOTAL_BLOCKS,
        "the skip must leave most blocks unread: {}/{TOTAL_BLOCKS}",
        run.blocks_scanned
    );
    assert!(run.blocks_skipped > 0 && run.segments_skipped > 0);
    assert_eq!(
        run.blocks_total - run.blocks_scanned,
        run.blocks_skipped,
        "inside opened segments every unread block is a threshold skip"
    );
    assert_eq!(
        run.blocks_total + run.segments_skipped * BLOCKS_PER_SEGMENT,
        TOTAL_BLOCKS,
        "every block is either in an opened segment or in a skipped one"
    );
}

/// The non-monotone and late-segment claims, with exact figures on one
/// partition and `LIMIT 4`.
///
/// The segment written last (snapshot index 7) holds the smallest minimum, so
/// it is opened first. Its first block (one stream's `k` = j, j+3, j+6, j+9)
/// fills the heap; that stream's second block (minimum `k` = 12 + j) is then
/// above the threshold and skipped, but the next stream's first block (minimum
/// `k` below 3) is not, and must be read: block minima are not monotone. Three
/// blocks read, three skipped, and the threshold is then `k` = 3 of rank 0, so
/// all seven other segments, whose minima start at rank 1's `k` = 0, are never
/// opened.
///
/// Visiting in snapshot order instead would open rank 1's segment first and
/// skip only six. Stopping a segment at its first block above the threshold
/// would lose `k` = 1 and 2.
#[tokio::test]
async fn non_monotone_blocks_and_late_segment_are_read_first() {
    let sql = "SELECT ts, body FROM logs ORDER BY ts LIMIT 4";
    let run = same_rows(sql, Setup::new()).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(4, false));
    assert_eq!(run.blocks_scanned, 3, "one first block per stream");
    assert_eq!(run.blocks_skipped, 3, "one second block per stream");
    assert_eq!(run.blocks_total, BLOCKS_PER_SEGMENT, "one segment opened");
    assert_eq!(
        run.segments_skipped,
        SEGMENTS - 1,
        "the late segment holds the smallest ts, so every other is skipped"
    );
}

/// The ClickBench q27 shape: a second sort key after `ts`. The TopK's filter
/// is then `ts < t OR (ts = t AND ...)`, and the scan reads only its `ts`
/// bound.
#[tokio::test]
async fn second_sort_key_matches_and_skips() {
    let sql = "SELECT ts, body FROM logs WHERE severity_text <> 'DEBUG' \
               ORDER BY ts, body LIMIT 10";
    let run = same_rows(sql, Setup::new()).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(10, true));
    assert!(run.blocks_skipped > 0 && run.segments_skipped > 0);
    assert!(run.blocks_scanned * 4 < TOTAL_BLOCKS);
}

/// Ties straddling a block boundary: the two rows at offset 3 sit in blocks 0
/// and 1. After block 0 the heap of four is full with threshold `ts` = 3, so
/// block 1's minimum equals the threshold and must be read: its tied row wins
/// on `body` under one of the two directions, whichever row the writer put
/// first. Block 2's minimum (7) is above the threshold and is skipped, which
/// proves the skip is live on this fixture. A skip on `>=` would drop block 1
/// and return the wrong tied row.
#[tokio::test]
async fn ties_on_the_threshold_are_read() {
    for direction in ["DESC", "ASC"] {
        let sql = format!("SELECT ts, body FROM logs ORDER BY ts, body {direction} LIMIT 4");
        let run = same_rows(&sql, Setup::new().tied()).await;
        let winner = if direction == "ASC" { "tie-m" } else { "tie-n" };
        let loser = if direction == "ASC" { "tie-n" } else { "tie-m" };
        assert!(run.rows.contains(winner), "{direction}: {}", run.rows);
        assert!(!run.rows.contains(loser), "{direction}: {}", run.rows);
        assert_eq!(
            (run.blocks_scanned, run.blocks_skipped, run.blocks_total),
            (2, 1, 3),
            "{direction}: the tie block is read, the block above is skipped"
        );
    }
}

/// The late-materialized shape: `SELECT *` over twelve declared columns is
/// rewritten into a narrow phase-1 TopK and a row fetch, and the phase-1 scan
/// gets the TopK's filter by hand. Row refs index blocks by position, which a
/// skip does not move, so the phase-1 scan skips blocks, not only segments.
#[tokio::test]
async fn late_materialized_select_star_matches_and_skips() {
    let sql = "SELECT * FROM logs WHERE severity_text <> 'DEBUG' ORDER BY ts LIMIT 10";
    let run = same_rows(sql, Setup::new()).await;
    assert!(
        run.has_fetch_node(),
        "the rewrite must fire:\n{}",
        run.explain
    );
    assert_bodies_in_order(&run.rows, &smallest_bodies(10, true));
    assert!(run.blocks_skipped > 0, "phase 1 skips blocks");
    assert!(run.segments_skipped > 0, "phase 1 skips segments");
    assert!(run.blocks_scanned * 4 < TOTAL_BLOCKS);
}

/// No skip until the heap is full: with `LIMIT` above the row count the TopK
/// never publishes a threshold, so every block is read and every row returned.
#[tokio::test]
async fn heap_never_full_skips_nothing() {
    let sql = format!(
        "SELECT ts, body FROM logs ORDER BY ts LIMIT {}",
        TOTAL_RECORDS + 1
    );
    let run = same_rows(&sql, Setup::new()).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(TOTAL_RECORDS, false));
    assert_eq!((run.blocks_skipped, run.segments_skipped), (0, 0));
    assert_eq!(run.blocks_scanned, TOTAL_BLOCKS);
}

/// The heap fills late: the filter drops three of every four records of the
/// first segment opened, so the threshold appears only after several blocks
/// and the answer still matches.
#[tokio::test]
async fn heap_filling_late_still_matches() {
    let sql = "SELECT ts, body FROM logs WHERE severity_text = 'DEBUG' ORDER BY ts LIMIT 6";
    let run = same_rows(sql, Setup::new()).await;
    let debug: Vec<String> = {
        let mut all: Vec<(i64, String)> = (0..SEGMENTS)
            .flat_map(segment_records)
            .filter(|r| r.severity_text == "DEBUG")
            .map(|r| (r.ts_ns, r.body))
            .collect();
        all.sort();
        all.into_iter().take(6).map(|(_, b)| b).collect()
    };
    assert_bodies_in_order(&run.rows, &debug);
}

/// Shapes the skip must leave alone: a descending `ts` (its filter is a lower
/// bound), a string leading key (ClickBench q26), a leading key that is
/// another column with the same order as `ts`, and an expression over `ts`.
#[tokio::test]
async fn other_leading_keys_skip_nothing() {
    for sql in [
        "SELECT ts, body FROM logs ORDER BY ts DESC LIMIT 10",
        "SELECT body FROM logs WHERE severity_text <> 'DEBUG' ORDER BY body LIMIT 10",
        "SELECT ts, body FROM logs ORDER BY observed_ts LIMIT 10",
        "SELECT ts, body FROM logs ORDER BY ts + INTERVAL '1' SECOND LIMIT 10",
    ] {
        let run = same_rows(sql, Setup::new()).await;
        assert_eq!(
            (run.blocks_skipped, run.segments_skipped, run.blocks_scanned),
            (0, 0, TOTAL_BLOCKS),
            "{sql}"
        );
    }
}

/// Several partitions each check the shared threshold on their own. With four
/// partitions each owns two whole segments, dealt in ascending minimum; by the
/// time a partition reaches its second segment its own rows have tightened the
/// threshold below that segment's minimum, so at least four are never opened.
#[tokio::test]
async fn partitions_check_the_threshold_on_their_own() {
    let sql = "SELECT ts, body FROM logs ORDER BY ts LIMIT 10";
    let run = same_rows(sql, Setup::new().partitions(4)).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(10, false));
    assert!(
        run.segments_skipped >= SEGMENTS / 2,
        "each partition's second segment is above its threshold: {}",
        run.segments_skipped
    );
}

/// The striped path: row groups of two blocks are dealt across partitions, a
/// segment's groups can land on different partitions, and each skips by the
/// same rule.
#[tokio::test]
async fn striped_row_groups_match() {
    let sql = "SELECT ts, body FROM logs WHERE severity_text <> 'DEBUG' ORDER BY ts LIMIT 10";
    let run = same_rows(sql, Setup::new().partitions(3).group_blocks(2)).await;
    assert_bodies_in_order(&run.rows, &smallest_bodies(10, true));
    assert!(run.blocks_skipped + run.segments_skipped > 0);
}
