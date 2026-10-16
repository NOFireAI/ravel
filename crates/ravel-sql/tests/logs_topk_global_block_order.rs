//! ADR-2677 decision 6, issue #2694: on the striped planned path (a read cache
//! wired), an ascending `ORDER BY ts LIMIT k` deals row groups to partitions
//! in one global order of ascending block minimum `ts` across every segment,
//! so each partition reads the earliest blocks first, the TopK heap fills from
//! them, and the threshold skip passes over the rest. Ordering whole segments
//! by their minimum cannot do that when every segment spans the whole time
//! range: every segment's minimum is then near the start.
//!
//! Each test runs its statement two ways. End to end through
//! [`ravel_sql::build_session`], with the TopK filter pushed down and not,
//! the two must return the same rows. And driven by hand, as in
//! `topk_threshold_skip_order.rs`: the plan's single TopK sits above a
//! `CoalescePartitionsExec`, so end to end how many blocks a partition decodes
//! before the TopK publishes depends on how the runtime interleaves the
//! partitions. The hand-driven run polls the scan's partitions in lockstep,
//! one batch (one block) each per round, keeps the top `k` matching rows
//! itself, and publishes the TopK's threshold through the sort's own dynamic
//! filter after every batch, as a TopK that consumes each batch before the
//! partition that sent it decodes another. Its block counts are exact.
//!
//! # The fixture
//!
//! Eight segments, blocks of four records, row groups of two blocks. Segment
//! `s` holds two streams. The writer lays blocks out stream by stream, and the
//! stream whose id sorts first holds the late rows: 120 records at `ts`
//! offsets `LATE_BASE + 8j + s`, fifteen row groups that match nothing. The
//! other holds the early rows: eight records at `8i + s`, one row group, all
//! matching `url LIKE '%google%'`. So every segment spans the whole range with
//! its minimum at `s`, every block spans 24 ns of it, and the early group of
//! every segment is its last group in block order. Every `ts` is unique.
//!
//! The statement's `ts` bound sits inside every segment's last block, so it
//! prunes no block, but no segment is contained in the window, which keeps
//! the scan off the whole-segment fast path; a block-range threshold of 0 puts
//! every segment on the planned route.
//!
//! The answer is the early rows at offsets 0..10, so `T*`, its tenth `ts`, is
//! offset 9, and the blocks whose minimum is at or under it, which every plan
//! must read, are the eight early first blocks.
//!
//! # Why the bound holds under the global deal and fails under segment order
//!
//! Four partitions. Global deal: the eight early groups have the eight
//! smallest keys, so partition `p` gets the early groups of segments `p` and
//! `p + 4` first: sixteen matching rows in four blocks, enough to fill a heap
//! of ten. Every later unit is a late group whose blocks start at
//! `LATE_BASE`, above any `ts` a full heap of early rows can publish. With
//! each batch reaching the TopK before its partition decodes the next block,
//! and a threshold published by another partition only tightening it, each
//! partition decodes at most its four early blocks: at most 16.
//!
//! Segment order (main before this change): segments by minimum are 0..8,
//! each has sixteen groups, and group `g` of each goes to partition `g % 4`,
//! so partition 3 owns every early group, each behind three late groups of the
//! same segment, and the other partitions own no matching row. Partition 3
//! reaches its twelfth matching row at its fifteenth block (six late, two
//! early, six late, one early), and until then nothing can publish a
//! threshold. The drive polls partitions 0..3 in order each round and
//! publishes the threshold right after partition 3's batch fills the heap, so
//! partitions 0-2 only see it on their next poll, and whatever blocks were
//! already in flight for them before the skip takes hold are decoded too: the
//! lockstep drive decodes 67 blocks in all, confirmed by running this fixture
//! with `global_deal` disabled. No committed test pins that number (it would
//! need a production hook back to segment order), so it is not asserted here,
//! only explained.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, Literal};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::footer::kind;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_memory::MemoryBudget;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::PutOptions;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{CacheFetchError, LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsScanExec, LogsTableProvider, SessionTable,
    SpillDecision, SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: [u8; 16] = [5u8; 16];
/// 2023-11-14 22:13:20 UTC, so an offset `o` is the timestamp literal
/// `2023-11-14 22:13:20.000000000` plus `o` nanoseconds.
const TS_BASE: i64 = 1_700_000_000_000_000_000;

const PARTITIONS: usize = 4;
const RECORDS_PER_BLOCK: usize = 4;
const GROUP_BLOCKS: usize = 2;
const K: usize = 10;

const SEGMENTS: usize = 8;
const LATE_GROUPS: usize = 15;
const LATE_RECORDS: usize = LATE_GROUPS * GROUP_BLOCKS * RECORDS_PER_BLOCK;
const EARLY_RECORDS: usize = GROUP_BLOCKS * RECORDS_PER_BLOCK;
const LATE_BASE: i64 = 1000;
const BLOCKS_PER_SEGMENT: usize = (LATE_RECORDS + EARLY_RECORDS) / RECORDS_PER_BLOCK;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEGMENT;

/// The early first blocks: every block whose minimum is at or under `T*`,
/// which every plan must read (see the module doc). The lockstep drive is
/// deterministic, and under the global deal it decodes exactly this many
/// and no more: a full heap of ten publishes a threshold before any
/// partition reaches a second early block or a late one.
const FLOOR_BLOCKS: usize = SEGMENTS;

/// The bound inside every segment's last block: that block starts at
/// `LATE_BASE + 8 * 116 + s`, at most offset 1935, and the segment ends at
/// `LATE_BASE + 8 * 119 + s`, at least offset 1952.
const ACCEPTANCE_TS_BOUND: &str = "2023-11-14 22:13:20.000001936";

const TIED_SEGMENTS: usize = 5;
const TIED_GROUP_BLOCKS: usize = 3;
/// Inside every tied segment's trailing block, offsets 1000..1004.
const TIED_TS_BOUND: &str = "2023-11-14 22:13:20.000001002";

fn resource(name: &str) -> Vec<(String, AttrValue)> {
    vec![("service.name".to_string(), AttrValue::Str(name.to_string()))]
}

fn stream_id(name: &str) -> ravel_types::logstream::LogStreamId {
    ravel_types::logstream::log_stream_id(&resource(name), "scope", "1.0", &[])
}

/// The two stream names, the one whose id the writer orders first first.
fn stream_names() -> (&'static str, &'static str) {
    if stream_id("svc0") < stream_id("svc1") {
        ("svc0", "svc1")
    } else {
        ("svc1", "svc0")
    }
}

fn record(stream: &str, offset: i64, matching: bool, body: String) -> LogRecord {
    let url = if matching {
        format!("https://www.google.com/{body}")
    } else {
        format!("https://example.com/{body}")
    };
    LogRecord {
        stream_id: stream_id(stream),
        stream_attrs: stream_attrs_bytes(&resource(stream), "scope", "1.0", &[]),
        ts_ns: TS_BASE + offset,
        observed_ts_ns: TS_BASE + offset,
        severity_num: 9,
        severity_text: "INFO".into(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("url".to_string(), AttrValue::Str(url))],
    }
}

/// Segment `s` of the acceptance fixture; see the module doc.
fn acceptance_records(s: usize) -> Vec<LogRecord> {
    let (late, early) = stream_names();
    let s_off = s as i64;
    let mut recs: Vec<LogRecord> = (0..LATE_RECORDS)
        .map(|j| {
            record(
                late,
                LATE_BASE + 8 * j as i64 + s_off,
                false,
                format!("late-s{s}-j{j}"),
            )
        })
        .collect();
    recs.extend((0..EARLY_RECORDS).map(|i| {
        record(
            early,
            8 * i as i64 + s_off,
            true,
            format!("early-s{s}-i{i}"),
        )
    }));
    recs
}

/// The two-key fixture, by segment, in snapshot order. One stream, groups of
/// three blocks, and a trailing non-matching block at offsets 1000..1004 in
/// every segment, so the `ts < 1002` bound cuts every segment and prunes no
/// block.
///
/// - segment 0: twelve matching rows at 0..9, 20, 21, 22, the one at 20 with
///   body `tie-n`, in three blocks: one row group, key 0, and the trailing
///   block as a second;
/// - segments 1, 2, 3: one non-matching block from offset `s` on, key `s`;
/// - segment 4: matching rows at 20 (body `tie-m`), 23, 24, 25, key 20.
///
/// The global deal sorts the keys 0, 1, 2, 3, 20 first, so partition 0 owns
/// segment 0's first group and then segment 4's. After segment 0's three
/// blocks the top ten are 0..9 and `(20, tie-n)`, and the TopK publishes
/// `ts < 20 OR (ts = 20 AND body < 'tie-n')`, ceiling 20: segment 4's block
/// starts AT the ceiling, so it must be read, and its `(20, tie-m)` displaces
/// `tie-n`. No other partition owns a matching row. A skip on `>=` passes
/// over segment 4 and keeps `tie-n`.
fn tied_records(s: usize) -> Vec<LogRecord> {
    let (stream, _) = stream_names();
    let mut recs: Vec<LogRecord> = match s {
        0 => (0..9)
            .map(|o| (o, format!("row{o}")))
            .chain([
                (20, "tie-n".to_string()),
                (21, "row21".into()),
                (22, "row22".into()),
            ])
            .map(|(o, body)| record(stream, o, true, body))
            .collect(),
        1..=3 => (0..4)
            .map(|i| record(stream, s as i64 + 100 * i, false, format!("other-s{s}-{i}")))
            .collect(),
        4 => [(20, "tie-m"), (23, "row23"), (24, "row24"), (25, "row25")]
            .into_iter()
            .map(|(o, body)| record(stream, o, true, body.to_string()))
            .collect(),
        _ => unreachable!("five segments"),
    };
    recs.extend((1000..1004).map(|o| record(stream, o, false, format!("tail-s{s}-{o}"))));
    recs
}

#[derive(Clone, Copy, Debug)]
enum Fixture {
    Acceptance,
    Tied,
}

impl Fixture {
    fn segments(self) -> usize {
        match self {
            Fixture::Acceptance => SEGMENTS,
            Fixture::Tied => TIED_SEGMENTS,
        }
    }
    fn records(self, s: usize) -> Vec<LogRecord> {
        match self {
            Fixture::Acceptance => acceptance_records(s),
            Fixture::Tied => tied_records(s),
        }
    }
    fn config(self) -> RlogConfig {
        RlogConfig {
            block_target_records: RECORDS_PER_BLOCK,
            group_target_blocks: match self {
                Fixture::Acceptance => GROUP_BLOCKS,
                Fixture::Tied => TIED_GROUP_BLOCKS,
            },
            ..RlogConfig::default()
        }
    }
    /// Every record's `(ts offset, body)` matching the statement's filter,
    /// sorted: the first `K` are the answer.
    fn matching(self) -> Vec<(i64, String)> {
        let mut all: Vec<(i64, String)> = (0..self.segments())
            .flat_map(|s| self.records(s))
            .filter(|r| {
                r.attrs
                    .iter()
                    .any(|(_, v)| matches!(v, AttrValue::Str(u) if u.contains("google")))
            })
            .map(|r| (r.ts_ns - TS_BASE, r.body))
            .collect();
        all.sort();
        all
    }
}

/// Writes segment `s` and returns its ref and its directories' decoded bytes.
async fn write_segment(
    store: &dyn ObjectStoreBackend,
    fixture: Fixture,
    s: usize,
) -> (SegmentRef, u64) {
    let recs = fixture.records(s);
    let identity = ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: s as u64 + 1,
    };
    let mut w = RlogWriter::new(fixture.config(), identity);
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let footer = ravel_logseg::footer::open(&bytes).expect("footer");
    let directories = [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
    ]
    .iter()
    .map(|k| footer.section(*k).expect("section").uncomp_len)
    .sum();
    let key = format!("logs/seg{s}.rlog");
    let size = bytes.len() as u64;
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let segment = SegmentRef {
        data_object_key: key,
        object_size: size,
        min_event_ts_ns: recs.iter().map(|r| r.ts_ns).min().unwrap(),
        max_event_ts_ns: recs.iter().map(|r| r.ts_ns).max().unwrap(),
        ingest_hour_bucket: 0,
        sample_count: recs.len() as u64,
        series_count: 2,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: s as u64 + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    (segment, directories)
}

/// A session over `fixture` with a read cache, so the planned path stripes
/// row groups, and a fetch budget the test reads. The cache admits no entry
/// (none fits in one byte), so no cached read's bytes are in the budget.
struct Setup {
    ctx: SessionContext,
    budget: Arc<MemoryBudget>,
    /// Sum of every segment's decoded directory bytes.
    directories: u64,
}

async fn setup(fixture: Fixture, skip: bool) -> Setup {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    let mut directories = 0;
    for s in 0..fixture.segments() {
        let (segment, dirs) = write_segment(store.as_ref(), fixture, s).await;
        segments.push(segment);
        directories += dirs;
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let budget = Arc::new(MemoryBudget::unlimited());
    let cache: Arc<Cache<CacheFetchError>> =
        Arc::new(Cache::new(CacheLimits::new(64 << 20, 16_384, 1)));
    let fetcher = LogSegmentFetcher::new(store as Arc<dyn ObjectStoreBackend>)
        .with_memory_budget(Arc::clone(&budget))
        .with_block_range_threshold(0)
        .with_cache(cache);
    let accounting = QueryAccounting::new();
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        PhaseAccounting::pooled_over(&accounting),
    )
    .with_declared_columns(vec![DeclaredColumn::new("url", DeclaredType::Str)]);

    let mut config = SqlConfig::default();
    config.engine.fetch_concurrency = PARTITIONS;
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
        options.optimizer.enable_topk_dynamic_filter_pushdown = skip;
        // A filtered TopK sees rows once per block, as at production block and
        // batch sizes (see `logs_topk_threshold_skip.rs`, "Batch size").
        options.execution.batch_size =
            datafusion::common::config::ConfigNonZeroUsize::try_new(RECORDS_PER_BLOCK)
                .expect("the block size is nonzero");
    }
    Setup {
        ctx,
        budget,
        directories,
    }
}

/// The first node of type `T` in `plan`, depth first, if there is one.
fn find<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<T>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find::<T>)
}

fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    fn walk(plan: &Arc<dyn ExecutionPlan>, name: &str, out: &mut usize) {
        if plan.name() == "LogsScanExec"
            && let Some(set) = plan.metrics()
        {
            *out += set
                .iter()
                .filter(|m| m.value().name() == name)
                .map(|m| m.value().as_usize())
                .sum::<usize>();
        }
        for child in plan.children() {
            walk(child, name, out);
        }
    }
    let mut out = 0;
    walk(plan, name, &mut out);
    out
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan")
}

/// The statement's rows end to end, with the TopK filter pushed down or not.
async fn rows(sql: &str, fixture: Fixture, skip: bool) -> (String, Arc<dyn ExecutionPlan>) {
    let setup = setup(fixture, skip).await;
    let plan = physical_plan(&setup.ctx, sql).await;
    let batches: Vec<RecordBatch> = collect(Arc::clone(&plan), setup.ctx.task_ctx())
        .await
        .expect("statement runs");
    let rows = pretty_format_batches(&batches)
        .expect("rows render")
        .to_string();
    (rows, plan)
}

/// Asserts the statement returns the same rows with the TopK filter pushed
/// down as without, and returns the skipping run's rows.
async fn same_rows(sql: &str, fixture: Fixture) -> String {
    let (oracle, oracle_plan) = rows(sql, fixture, false).await;
    assert_eq!(
        metric(&oracle_plan, "blocks_skipped_by_threshold")
            + metric(&oracle_plan, "segments_skipped_by_threshold"),
        0,
        "with the TopK filter not pushed down nothing may be skipped"
    );
    let (skipping, plan) = rows(sql, fixture, true).await;
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    eprintln!(
        "[{fixture:?} end to end] blocks {}/{}\n{explain}",
        metric(&plan, "blocks_scanned"),
        metric(&plan, "blocks_total"),
    );
    assert_eq!(
        skipping, oracle,
        "the skipping scan must return exactly the unskipped rows for {sql}"
    );
    assert!(
        explain.contains("topk_threshold=ts"),
        "the threshold is confirmed:\n{explain}"
    );
    skipping
}

struct Driven {
    /// The top `K` matching `(ts offset, body)` the drive kept, sorted.
    top: Vec<(i64, String)>,
    blocks_scanned: usize,
    blocks_total: usize,
    /// The fetch budget's reservation after each batch, in drive order.
    reserved: Vec<u64>,
    directories: u64,
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Arc<dyn Array> {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column"))
}

/// The matching rows of `batch` as `(ts offset, body)`.
fn matching_rows(batch: &RecordBatch) -> Vec<(i64, String)> {
    let ts = column(batch, "ts")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ts is nanosecond timestamps")
        .clone();
    let text = |name: &str| {
        cast(column(batch, name), &DataType::Utf8)
            .expect("string column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .clone()
    };
    let (url, body) = (text("url"), text("body"));
    (0..batch.num_rows())
        .filter(|&i| url.value(i).contains("google"))
        .map(|i| (ts.value(i) - TS_BASE, body.value(i).to_string()))
        .collect()
}

/// The threshold DataFusion's TopK publishes for a full heap whose worst row
/// is `worst`, over its sort keys `keys` (`ts` then, with two keys, `body`).
fn threshold(keys: &[Arc<dyn PhysicalExpr>], worst: &(i64, String)) -> Arc<dyn PhysicalExpr> {
    let bin = |l: Arc<dyn PhysicalExpr>, op, r: Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    };
    let ts = || -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
            Some(TS_BASE + worst.0),
            None,
        )))
    };
    let below = bin(Arc::clone(&keys[0]), Operator::Lt, ts());
    match keys.get(1) {
        None => below,
        Some(body) => {
            let tie = bin(
                bin(Arc::clone(&keys[0]), Operator::Eq, ts()),
                Operator::And,
                bin(
                    Arc::clone(body),
                    Operator::Lt,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(worst.1.clone())))),
                ),
            );
            bin(below, Operator::Or, tie)
        }
    }
}

/// Plans `sql`, then drives the scan's partitions in lockstep and publishes
/// the threshold after every batch; see the module doc.
async fn drive(sql: &str, fixture: Fixture) -> Driven {
    let setup = setup(fixture, true).await;
    let plan = physical_plan(&setup.ctx, sql).await;
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    let sort = find::<SortExec>(&plan)
        .unwrap_or_else(|| panic!("the plan sorts with a SortExec:\n{explain}"));
    let produced = sort.dynamic_expressions_produced();
    assert_eq!(produced.len(), 1, "one TopK filter:\n{explain}");
    let produced: Arc<dyn PhysicalExpr> = Arc::clone(&produced[0]);
    let filter = Arc::downcast::<DynamicFilterPhysicalExpr>(produced)
        .unwrap_or_else(|_| panic!("a DynamicFilterPhysicalExpr:\n{explain}"));
    let keys: Vec<Arc<dyn PhysicalExpr>> = filter.children().into_iter().cloned().collect();
    let scan = find::<LogsScanExec>(&plan)
        .unwrap_or_else(|| panic!("the plan scans with a LogsScanExec:\n{explain}"));
    assert!(
        explain.contains("topk_threshold=ts"),
        "the threshold is confirmed:\n{explain}"
    );
    assert_eq!(
        scan.output_partitioning().partition_count(),
        PARTITIONS,
        "{explain}"
    );

    let mut streams: Vec<Option<SendableRecordBatchStream>> = (0..PARTITIONS)
        .map(|p| {
            Some(
                scan.execute(p, setup.ctx.task_ctx())
                    .expect("scan executes"),
            )
        })
        .collect();
    let mut top: Vec<(i64, String)> = Vec::new();
    let mut reserved = Vec::new();
    while streams.iter().any(Option::is_some) {
        for slot in &mut streams {
            let Some(stream) = slot else { continue };
            match stream.next().await {
                Some(batch) => {
                    top.extend(matching_rows(&batch.expect("batch")));
                    top.sort();
                    top.truncate(K);
                    if top.len() == K {
                        filter
                            .update(threshold(&keys, &top[K - 1]))
                            .expect("publish threshold");
                    }
                    reserved.push(setup.budget.reserved());
                }
                None => *slot = None,
            }
        }
    }
    let driven = Driven {
        top,
        blocks_scanned: metric(&scan, "blocks_scanned"),
        blocks_total: metric(&scan, "blocks_total"),
        reserved,
        directories: setup.directories,
    };
    assert!(
        metric(&scan, "fast_path_rejected_segment_not_contained") > 0,
        "the planned path ran:\n{explain}"
    );
    assert_eq!(
        metric(&scan, "segments_unplanned"),
        0,
        "every segment planned"
    );
    eprintln!(
        "[{fixture:?} lockstep] blocks {}/{} skipped blocks {} segments {} \
         reserved peak {} mean {} directories {} by batch {:?}",
        driven.blocks_scanned,
        driven.blocks_total,
        metric(&scan, "blocks_skipped_by_threshold"),
        metric(&scan, "segments_skipped_by_threshold"),
        driven.reserved.iter().max().copied().unwrap_or(0),
        driven.reserved.iter().sum::<u64>() / driven.reserved.len().max(1) as u64,
        driven.directories,
        driven.reserved,
    );
    driven
}

fn assert_in_order(rows: &str, expected: &[String]) {
    let mut at = 0;
    for item in expected {
        let found = rows[at..]
            .find(item.as_str())
            .unwrap_or_else(|| panic!("{item} missing or out of order in\n{rows}"));
        at += found + item.len();
    }
}

#[tokio::test]
async fn logs_topk_global_block_order() {
    let sql = format!(
        "SELECT * FROM logs WHERE url LIKE '%google%' \
         AND ts < TIMESTAMP '{ACCEPTANCE_TS_BOUND}' ORDER BY ts LIMIT {K}"
    );
    let answer: Vec<(i64, String)> = Fixture::Acceptance.matching().into_iter().take(K).collect();
    assert_eq!(
        answer.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        (0..K as i64).collect::<Vec<_>>(),
        "the answer is the early rows at offsets 0..10"
    );

    let rows = same_rows(&sql, Fixture::Acceptance).await;
    let bodies: Vec<String> = answer.iter().map(|(_, b)| format!("| {b} ")).collect();
    assert_in_order(&rows, &bodies);

    let run = drive(&sql, Fixture::Acceptance).await;
    assert_eq!(run.top, answer, "the drive's own top {K}");
    assert_eq!(
        run.blocks_total, TOTAL_BLOCKS,
        "the plan counts every block"
    );
    assert_eq!(
        run.blocks_scanned, FLOOR_BLOCKS,
        "the global deal's lockstep drive decodes exactly the early first \
         blocks, no more and no fewer"
    );
    // Every segment's directories are reserved from the end of the plan
    // phase, so the first batch already sees all of them.
    let peak = run.reserved.iter().max().copied().unwrap_or(0);
    assert!(
        peak >= run.directories,
        "carried directories are reserved while the scan runs: {peak} of {}",
        run.directories
    );
}

/// Two sort keys and a tie on the threshold across a block and a row-group
/// boundary (see [`tied_records`]): the block whose minimum equals the
/// ceiling is read and its tied row wins on `body`.
#[tokio::test]
async fn logs_topk_global_block_order_two_keys() {
    let sql = format!(
        "SELECT * FROM logs WHERE url LIKE '%google%' \
         AND ts < TIMESTAMP '{TIED_TS_BOUND}' ORDER BY ts, body LIMIT {K}"
    );
    let answer: Vec<(i64, String)> = Fixture::Tied.matching().into_iter().take(K).collect();
    assert_eq!(answer[K - 1], (20, "tie-m".to_string()), "the tie's winner");

    let rows = same_rows(&sql, Fixture::Tied).await;
    let bodies: Vec<String> = answer.iter().map(|(_, b)| format!("| {b} ")).collect();
    assert_in_order(&rows, &bodies);
    assert!(!rows.contains("tie-n"), "{rows}");

    let run = drive(&sql, Fixture::Tied).await;
    assert_eq!(
        run.top, answer,
        "the block starting at the ceiling is read and its tied row wins"
    );
    assert!(
        run.blocks_scanned < run.blocks_total,
        "the skip fired: {}/{}",
        run.blocks_scanned,
        run.blocks_total
    );
}
