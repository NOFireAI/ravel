//! ADR-2677 decision 6, pinned by order rather than by scheduling (issue
//! #2851): a threshold the TopK publishes before the logs scan opens a segment
//! skips exactly the blocks of that segment whose stored minimum `ts` is above
//! it and skips a segment whose every row is above it without opening it,
//! and a scan whose threshold is never published while it runs skips nothing.
//!
//! A statement run end to end decides when its TopK publishes by how the
//! runtime interleaves the scan with the sort, so its skip count varies from
//! run to run. Here the TopK never runs: the test plans `ORDER BY ts LIMIT k`,
//! takes the `SortExec`'s dynamic filter, drives the scan's one partition
//! itself, and publishes the threshold through that filter at a fixed point in
//! the scan.
//!
//! `logs_topk_threshold_skip.rs`, in
//! `non_monotone_blocks_and_late_segment_are_read_first`, already pins two of
//! this fixture's claims, block minima that are not monotone inside a segment
//! and a late segment skipped whole; this file adds the four fetch routes and
//! the exact keys read before and after the publish.
//!
//! # The routes
//!
//! Every run asserts the route counters of the fetch route it drives:
//!
//! - fast path, whole object: no block predicate and one partition, so the
//!   scan takes the whole-segment fast path and opens each segment with one
//!   whole-object GET;
//! - fast path, ranged: the same statement over a fetcher whose block-range
//!   threshold is 0, so the fast path opens each segment by column chunk;
//! - planned: `has_word(body, 'line')`, which every record matches, is a block
//!   predicate and rejects the fast path, and with a block-range threshold of
//!   0 every segment is above the plan bound, so the plan phase reads all
//!   three segments before the scan emits a row and the scan then opens each
//!   planned subset;
//! - unplanned (ADR-2677 decision 2): the same predicate over the default
//!   fetcher, where every segment is at or under the plan bound, so nothing is
//!   planned and the scan opens each segment with one whole-object read.
//!
//! On the planned route the plan has already read C when the threshold is
//! published, so what the skip saves there is C's open: no GET touches C after
//! the publish. On the other three routes C is never read at all.
//!
//! # The fixture
//!
//! Blocks of four records, two streams. `ts` offsets from `TS_BASE`:
//!
//! - segment A: stream 0 at 0, 2, ..., 14 and stream 1 at 1, 3, ..., 15, so
//!   four blocks, all below 100;
//! - segment B: stream 0 at 100, 102, 104, 106 | 200, 202, 204, 206 and
//!   stream 1 at 101, ..., 107 | 201, ..., 207. The writer lays blocks out
//!   stream by stream, so B's block minima are 100, 200, 101, 201 in some
//!   stream order: a skippable block sits between two that must be read, so a
//!   skip that stopped at the first block above the threshold would miss one;
//! - segment C: one stream at 300, ..., 307, two blocks.
//!
//! The snapshot lists them C, B, A, so the scan's visit order by minimum `ts`
//! is what puts A first. With `k = 24` the answer is all of A and B's eight
//! rows below 200, so its 24th `ts` is offset 107: the threshold published is
//! `ts < TS_BASE + 107`, which skips B's two blocks starting at 200 and 201
//! and all of C.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, TimestampNanosecondArray};
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, Literal};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream, displayable,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::reader::RlogReader;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, LogsScanExec, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];
const TS_BASE: i64 = 1_700_000_000_000_000_000;
const RECORDS_PER_BLOCK: usize = 4;

const KEY_A: &str = "logs/a.rlog";
const KEY_B: &str = "logs/b.rlog";
const KEY_C: &str = "logs/c.rlog";

/// `k` in the statement.
const K: usize = 24;

/// `(stream, ts offset)` of every record in a segment.
fn segment_offsets(key: &str) -> Vec<(usize, i64)> {
    match key {
        KEY_A => (0..16).map(|o| ((o % 2) as usize, o)).collect(),
        KEY_B => [100, 200]
            .into_iter()
            .flat_map(|base| (0..8).map(move |i| ((i % 2) as usize, base + i)))
            .collect(),
        KEY_C => (300..308).map(|o| (0, o)).collect(),
        _ => unreachable!("fixture key"),
    }
}

/// The offset of the answer's `k`-th `ts`, from the records the segments are
/// written from.
fn kth_offset() -> i64 {
    let mut all: Vec<i64> = [KEY_A, KEY_B, KEY_C]
        .iter()
        .flat_map(|key| segment_offsets(key))
        .map(|(_, o)| o)
        .collect();
    all.sort_unstable();
    all[K - 1]
}

fn log_record(stream: usize, offset: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{stream}")),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: TS_BASE + offset,
        observed_ts_ns: TS_BASE + offset,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("line o{offset}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Writes `key`'s segment and returns its ref and each block's stored
/// `(min_ts, max_ts)`, as offsets from `TS_BASE`, in block order.
async fn write_segment(
    store: &dyn ObjectStoreBackend,
    seq: u64,
    key: &str,
) -> (SegmentRef, Vec<(i64, i64)>) {
    let cfg = RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    };
    let offsets = segment_offsets(key);
    let mut writer = RlogWriter::new(cfg, identity);
    for &(stream, offset) in &offsets {
        writer.push(log_record(stream, offset)).expect("push");
    }
    let bytes = writer.finish().expect("finish");
    let blocks = RlogReader::new(&bytes, &cfg)
        .expect("read back")
        .directories()
        .skip_index()
        .l0
        .iter()
        .map(|block| (block.min_ts - TS_BASE, block.max_ts - TS_BASE))
        .collect();
    let size = bytes.len() as u64;
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let streams: BTreeSet<usize> = offsets.iter().map(|&(s, _)| s).collect();
    let segment = SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: TS_BASE + offsets.iter().map(|&(_, o)| o).min().unwrap(),
        max_event_ts_ns: TS_BASE + offsets.iter().map(|&(_, o)| o).max().unwrap(),
        ingest_hour_bucket: 0,
        sample_count: offsets.len() as u64,
        series_count: streams.len() as u64,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (segment, blocks)
}

/// Records the key of every `get`, in call order.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<String>>,
}

impl RecordingStore {
    fn get_count(&self) -> usize {
        self.gets.lock().unwrap().len()
    }

    /// The keys of the `get`s from the `from`-th on.
    fn keys_read_from(&self, from: usize) -> BTreeSet<String> {
        self.gets.lock().unwrap()[from..].iter().cloned().collect()
    }
}

#[async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.lock().unwrap().push(key.to_string());
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// A session from the production builder. The TopK dynamic filter the skip
/// reads is armed by DataFusion's default, which nothing in Ravel sets, so the
/// test asserts the built session has it on rather than turning it on itself.
fn session(provider: LogsTableProvider) -> SessionContext {
    let mut config = SqlConfig::default();
    config.engine.fetch_concurrency = 1;
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
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
        assert!(
            options.optimizer.enable_topk_dynamic_filter_pushdown,
            "the production session arms the TopK dynamic filter"
        );
        options.execution.batch_size =
            ConfigNonZeroUsize::try_new(RECORDS_PER_BLOCK).expect("the block size is nonzero");
    }
    ctx
}

/// The first node of type `T` in `plan`, depth first, if there is one.
fn find<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<T>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find::<T>)
}

fn metric(scan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    scan.metrics()
        .expect("scan metrics")
        .iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

/// The fetch route a run drives; see the module doc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    FastWhole,
    FastRanged,
    Planned,
    Unplanned,
}

const ROUTES: [Route; 4] = [
    Route::FastWhole,
    Route::FastRanged,
    Route::Planned,
    Route::Unplanned,
];

impl Route {
    fn sql(self) -> String {
        let predicate = match self {
            Route::FastWhole | Route::FastRanged => "",
            Route::Planned | Route::Unplanned => "WHERE has_word(body, 'line') ",
        };
        format!("SELECT ts, body FROM logs {predicate}ORDER BY ts LIMIT {K}")
    }

    fn fetcher(self, store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
        let fetcher = LogSegmentFetcher::new(store);
        match self {
            Route::FastWhole | Route::Unplanned => fetcher,
            Route::FastRanged | Route::Planned => fetcher.with_block_range_threshold(0),
        }
    }
}

/// When the test publishes the threshold relative to the scan.
#[derive(Clone, Copy, Debug)]
enum Publish {
    /// After A's last row, before the scan opens B.
    BeforeSecondSegment,
    /// Never while the scan runs.
    NotDuringScan,
}

/// The route counters a run published.
#[derive(Debug, PartialEq, Eq)]
struct RouteMetrics {
    fast_path_whole_object_segments: usize,
    fast_path_ranged_segments: usize,
    segments_unplanned: usize,
    fast_path_rejected_block_predicate: usize,
    plan_full_reads: usize,
    segments_opened: usize,
}

impl RouteMetrics {
    fn of(scan: &Arc<dyn ExecutionPlan>) -> Self {
        RouteMetrics {
            fast_path_whole_object_segments: metric(scan, "fast_path_whole_object_segments"),
            fast_path_ranged_segments: metric(scan, "fast_path_ranged_segments"),
            segments_unplanned: metric(scan, "segments_unplanned"),
            fast_path_rejected_block_predicate: metric(scan, "fast_path_rejected_block_predicate"),
            plan_full_reads: metric(scan, "plan_full_reads"),
            segments_opened: metric(scan, "segments_opened"),
        }
    }

    /// What `route` publishes when it opens `opened` segments.
    fn expected(route: Route, opened: usize) -> Self {
        let none = RouteMetrics {
            fast_path_whole_object_segments: 0,
            fast_path_ranged_segments: 0,
            segments_unplanned: 0,
            fast_path_rejected_block_predicate: 0,
            plan_full_reads: 0,
            segments_opened: opened,
        };
        match route {
            Route::FastWhole => RouteMetrics {
                fast_path_whole_object_segments: opened,
                ..none
            },
            Route::FastRanged => RouteMetrics {
                fast_path_ranged_segments: opened,
                ..none
            },
            // `has_word` is decided per row, not from the skip index, so the
            // plan reads each of the three segments whole.
            Route::Planned => RouteMetrics {
                fast_path_rejected_block_predicate: 1,
                plan_full_reads: 3,
                ..none
            },
            Route::Unplanned => RouteMetrics {
                fast_path_rejected_block_predicate: 1,
                segments_unplanned: opened,
                ..none
            },
        }
    }
}

struct Outcome {
    /// `ts` offsets the scan emitted.
    emitted: BTreeSet<i64>,
    blocks_scanned: usize,
    blocks_total: usize,
    blocks_skipped: usize,
    segments_skipped: usize,
    route: RouteMetrics,
    /// Keys read before the threshold was published, if it was.
    read_before_publish: Option<BTreeSet<String>>,
    /// Keys read after the threshold was published, if it was.
    read_after_publish: Option<BTreeSet<String>>,
    /// Keys read over the whole run, and how many `get`s read them.
    read: BTreeSet<String>,
    gets: usize,
    /// B's blocks as `(min, max)` offsets, in block order.
    b_blocks: Vec<(i64, i64)>,
}

fn keys(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|k| k.to_string()).collect()
}

/// Drives the scan of `route`'s statement by hand and publishes
/// `ts < TS_BASE + kth_offset()` through the sort's dynamic filter at the
/// point `publish` names.
async fn drive(route: Route, publish: Publish) -> Outcome {
    let store = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let (c, _) = write_segment(store.as_ref(), 1, KEY_C).await;
    let (b, b_blocks) = write_segment(store.as_ref(), 2, KEY_B).await;
    let (a, a_blocks) = write_segment(store.as_ref(), 3, KEY_A).await;
    assert_eq!(a_blocks.len(), 4, "A's blocks: {a_blocks:?}");
    assert_eq!(b_blocks.len(), 4, "B's blocks: {b_blocks:?}");
    let snapshot = Snapshot {
        segments: vec![c, b, a],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let accounting = QueryAccounting::new();
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        route.fetcher(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>),
        PhaseAccounting::pooled_over(&accounting),
    );
    let ctx = session(provider);
    let plan = ctx
        .sql(&route.sql())
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    let sort = find::<SortExec>(&plan)
        .unwrap_or_else(|| panic!("the plan sorts with a SortExec:\n{explain}"));
    let produced = sort.dynamic_expressions_produced();
    assert_eq!(
        produced.len(),
        1,
        "the TopK publishes one dynamic filter:\n{explain}"
    );
    let produced: Arc<dyn PhysicalExpr> = Arc::clone(&produced[0]);
    let filter = Arc::downcast::<DynamicFilterPhysicalExpr>(produced)
        .unwrap_or_else(|_| panic!("the TopK's filter is a DynamicFilterPhysicalExpr:\n{explain}"));
    let scan = find::<LogsScanExec>(&plan)
        .unwrap_or_else(|| panic!("the plan scans with a LogsScanExec:\n{explain}"));
    assert!(
        explain.contains("topk_threshold=ts"),
        "the scan confirmed the threshold:\n{explain}"
    );
    assert_eq!(scan.output_partitioning().partition_count(), 1, "{explain}");

    let mut stream: SendableRecordBatchStream =
        scan.execute(0, ctx.task_ctx()).expect("scan executes");
    let mut emitted = BTreeSet::new();
    let a_rows = segment_offsets(KEY_A).len();
    let mut read_before_publish = None;
    let mut published_at = None;

    // A run that never publishes while the scan runs is the discriminator: it
    // reads what a scan without the skip reads.
    if matches!(publish, Publish::BeforeSecondSegment) {
        while emitted.len() < a_rows {
            let batch = stream
                .next()
                .await
                .expect("A's rows arrive before the stream ends")
                .expect("batch");
            emitted.extend(ts_offsets(&batch));
        }
        assert_eq!(
            emitted,
            segment_offsets(KEY_A).iter().map(|&(_, o)| o).collect(),
            "{route:?}: the scan visits A first and emits all of it"
        );
        assert_eq!(metric(&scan, "blocks_skipped_by_threshold"), 0);
        read_before_publish = Some(store.keys_read_from(0));
        published_at = Some(store.get_count());
        let threshold = Arc::new(BinaryExpr::new(
            Arc::clone(filter.children()[0]),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
                Some(TS_BASE + kth_offset()),
                None,
            ))),
        ));
        filter.update(threshold).expect("publish threshold");
    }
    while let Some(batch) = stream.next().await {
        emitted.extend(ts_offsets(&batch.expect("batch")));
    }

    let outcome = Outcome {
        emitted,
        blocks_scanned: metric(&scan, "blocks_scanned"),
        blocks_total: metric(&scan, "blocks_total"),
        blocks_skipped: metric(&scan, "blocks_skipped_by_threshold"),
        segments_skipped: metric(&scan, "segments_skipped_by_threshold"),
        route: RouteMetrics::of(&scan),
        read_before_publish,
        read_after_publish: published_at.map(|at| store.keys_read_from(at)),
        read: store.keys_read_from(0),
        gets: store.get_count(),
        b_blocks,
    };
    eprintln!(
        "[{route:?} {publish:?}] blocks scanned {}/{} skipped {} segments skipped {} \
         route {:?} B blocks {:?} reads {:?}",
        outcome.blocks_scanned,
        outcome.blocks_total,
        outcome.blocks_skipped,
        outcome.segments_skipped,
        outcome.route,
        outcome.b_blocks,
        store.gets.lock().unwrap(),
    );
    outcome
}

fn ts_offsets(batch: &datafusion::arrow::record_batch::RecordBatch) -> Vec<i64> {
    let ts = batch
        .column_by_name("ts")
        .expect("ts column")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ts is nanosecond timestamps");
    assert_eq!(ts.null_count(), 0);
    ts.values().iter().map(|t| t - TS_BASE).collect()
}

fn all_offsets(keys: &[&str]) -> BTreeSet<i64> {
    keys.iter()
        .flat_map(|k| segment_offsets(k))
        .map(|(_, o)| o)
        .collect()
}

#[tokio::test]
async fn threshold_published_before_the_second_segment_skips_exactly_its_late_blocks() {
    let kth = kth_offset();
    assert_eq!(kth, 107, "the answer's {K}th ts offset");
    for route in ROUTES {
        let run = drive(route, Publish::BeforeSecondSegment).await;
        // Which of B's blocks lie above the threshold, read off its own skip
        // index: the two starting at 200 and 201, one per stream, and each
        // follows a block that must be read.
        let above: BTreeSet<(i64, i64)> = run
            .b_blocks
            .iter()
            .copied()
            .filter(|&(min, _)| min > kth)
            .collect();
        assert_eq!(
            above,
            BTreeSet::from([(200, 206), (201, 207)]),
            "{:?}",
            run.b_blocks
        );
        assert_eq!(
            run.b_blocks
                .iter()
                .map(|&(min, _)| min > kth)
                .collect::<Vec<_>>(),
            vec![false, true, false, true],
            "B's blocks run stream by stream: {:?}",
            run.b_blocks
        );

        assert_eq!(
            run.route,
            RouteMetrics::expected(route, 2),
            "{route:?}: A and B opened on this route, C not opened"
        );
        assert_eq!(run.blocks_skipped, 2, "{route:?}: B's 200 and 201 blocks");
        assert_eq!(run.segments_skipped, 1, "{route:?}: C skipped whole");
        // The plan phase counts every planned segment's blocks, C's included;
        // the other routes count the blocks of the segments they open.
        let total = if route == Route::Planned { 10 } else { 8 };
        assert_eq!(run.blocks_total, total, "{route:?}");
        assert_eq!(run.blocks_scanned, 6, "{route:?}");
        let mut read = all_offsets(&[KEY_A]);
        read.extend((100..108).collect::<Vec<_>>());
        assert_eq!(
            run.emitted, read,
            "{route:?}: A and B's 100 and 101 blocks are read, nothing else"
        );

        let (before, after) = match route {
            // The plan reads all three segments before the scan emits a row.
            Route::Planned => (keys(&[KEY_A, KEY_B, KEY_C]), keys(&[KEY_B])),
            _ => (keys(&[KEY_A]), keys(&[KEY_B])),
        };
        assert_eq!(
            run.read_before_publish,
            Some(before),
            "{route:?}: keys read before the threshold is published"
        );
        assert_eq!(
            run.read_after_publish,
            Some(after),
            "{route:?}: keys read after the threshold is published; C is not one"
        );
        let all = if route == Route::Planned {
            keys(&[KEY_A, KEY_B, KEY_C])
        } else {
            keys(&[KEY_A, KEY_B])
        };
        assert_eq!(run.read, all, "{route:?}: keys read over the run");
        if matches!(route, Route::FastWhole | Route::Unplanned) {
            assert_eq!(
                run.gets, 2,
                "{route:?}: one whole-object GET each for A and B"
            );
        }
    }
}

#[tokio::test]
async fn threshold_never_published_during_the_scan_skips_nothing() {
    for route in ROUTES {
        let run = drive(route, Publish::NotDuringScan).await;
        assert_eq!(run.route, RouteMetrics::expected(route, 3), "{route:?}");
        assert_eq!(run.blocks_skipped, 0, "{route:?}");
        assert_eq!(run.segments_skipped, 0, "{route:?}");
        assert_eq!(run.blocks_total, 10, "{route:?}");
        assert_eq!(run.blocks_scanned, 10, "{route:?}");
        assert_eq!(run.emitted, all_offsets(&[KEY_A, KEY_B, KEY_C]));
        assert_eq!(run.read, keys(&[KEY_A, KEY_B, KEY_C]), "{route:?}");
        if matches!(route, Route::FastWhole | Route::Unplanned) {
            assert_eq!(run.gets, 3, "{route:?}: one whole-object GET per segment");
        }
    }
}
