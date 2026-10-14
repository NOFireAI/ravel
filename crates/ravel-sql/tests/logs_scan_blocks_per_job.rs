//! Issue #2795: `LogsTableProvider::with_scan_blocks_per_job` makes one
//! `log_block` read-gate job of the `logs` scan's columnar path decode up to
//! that many consecutive blocks of a segment. Every statement here runs at 1,
//! 2, 4 and 8 blocks per job through a real [`ravel_sql::build_session`] over
//! a `MemoryStore` fixture, and must return the same rows at each.
//!
//! # The fixture
//!
//! Two segments of three streams of twelve records, four records per block,
//! so nine blocks per segment. The writer orders rows by stream id, so each
//! segment drains one stream's three blocks, then the next stream's. Record
//! `i` of stream `j` has `k = j + 3i` and `ts = TS_BASE + 2k + s`; records with
//! `k % 4 == 1` are `DEBUG`, the rest `INFO`. Every record carries `filler`,
//! and in the `Fallback` variant the records of segment 0's block 5 in drain
//! order (the second stream's last block) also carry `tags`. Written with one
//! dynamic column, `filler` takes it and `tags` overflows into `attrs_raw`, in
//! that block only, so the columnar drain falls back to the row path at block
//! 5: in the middle of a run for 2, 4 and 8 blocks per job (runs `4-5`, `4-7`,
//! `0-7`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroU32;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite};
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
const SEGMENTS: usize = 2;
const STREAMS: usize = 3;
const PER_STREAM: usize = 12;
const RECORDS_PER_BLOCK: usize = 4;
const PER_SEGMENT: usize = STREAMS * PER_STREAM;
const BLOCKS_PER_SEGMENT: usize = PER_SEGMENT / RECORDS_PER_BLOCK;
/// Segment 0's block that spills `tags` to `attrs_raw` in the `Fallback`
/// fixture: stream 1's last block.
const SPILL_BLOCK: usize = 5;
const BLOCKS_PER_JOB: [u32; 4] = [1, 2, 4, 8];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fixture {
    Clean,
    Fallback,
}

fn resource(j: usize) -> Vec<(String, AttrValue)> {
    vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{j}")),
    )]
}

/// Stream `j`'s position in drain order: the writer orders streams by id.
fn drain_position(j: usize) -> usize {
    let id = |j: usize| ravel_types::logstream::log_stream_id(&resource(j), "scope", "1.0", &[]);
    (0..STREAMS).filter(|&other| id(other) < id(j)).count()
}

fn record(segment: usize, j: usize, i: usize, spill: bool) -> LogRecord {
    let resource = resource(j);
    let k = j + STREAMS * i;
    let ts = TS_BASE + (2 * k + segment) as i64;
    let mut attrs = vec![("filler".to_string(), AttrValue::Str(format!("f{}", k % 3)))];
    if spill {
        attrs.push(("tags".to_string(), AttrValue::Str(format!("t{k}"))));
    }
    let debug = k % 4 == 1;
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: if debug { 5 } else { 9 },
        severity_text: if debug { "DEBUG" } else { "INFO" }.into(),
        body: format!("s{segment}k{k}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

async fn write_segment(
    store: &dyn ObjectStoreBackend,
    segment: usize,
    fixture: Fixture,
) -> SegmentRef {
    let cfg = RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };
    let identity = ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: segment as u64 + 1,
    };
    let mut writer = RlogWriter::new(cfg, identity);
    let mut recs = Vec::new();
    for j in 0..STREAMS {
        for i in 0..PER_STREAM {
            let block = (drain_position(j) * PER_STREAM + i) / RECORDS_PER_BLOCK;
            let spill = fixture == Fixture::Fallback && segment == 0 && block == SPILL_BLOCK;
            recs.push(record(segment, j, i, spill));
        }
    }
    for r in &recs {
        writer.push(r.clone()).expect("push");
    }
    let bytes = writer.finish().expect("finish");
    let key = format!("logs/seg{segment}.rlog");
    let size = bytes.len() as u64;
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
        writer_seq: segment as u64 + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// The erasure the `erasure` statement runs under: every record whose
/// `filler` is `f1`, a third of the fixture.
fn erase_f1() -> ravel_proto::commit::v1::ErasureRequest {
    ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "filler".to_string(),
            value: "f1".to_string(),
        }],
        ..Default::default()
    }
}

#[derive(Clone, Copy)]
struct Setup {
    fixture: Fixture,
    erasure: bool,
    blocks: u32,
}

struct Run {
    rows: String,
    row_count: usize,
    jobs: u64,
    blocks_scanned: usize,
    blocks_skipped: usize,
    reopens: usize,
    columnar_batches: usize,
}

fn metric(plan: &Arc<dyn ExecutionPlan>, node: &str, name: &str) -> usize {
    fn walk(plan: &Arc<dyn ExecutionPlan>, node: &str, name: &str, out: &mut usize) {
        if plan.name() == node
            && let Some(set) = plan.metrics()
        {
            *out += set
                .iter()
                .filter(|m| m.value().name() == name && m.labels().is_empty())
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
        // A block's worth of rows per batch, so a TopK above the scan sees
        // rows, and publishes its threshold, about once per block.
        state.config_mut().options_mut().execution.batch_size =
            datafusion::common::config::ConfigNonZeroUsize::try_new(RECORDS_PER_BLOCK)
                .expect("the block size is nonzero");
    }
    ctx
}

async fn run(sql: &str, setup: Setup) -> Run {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for s in 0..SEGMENTS {
        segments.push(write_segment(store.as_ref(), s, setup.fixture).await);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: if setup.erasure {
            vec![erase_f1()]
        } else {
            Vec::new()
        },
    };
    let gate = Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 2,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ));
    let accounting = QueryAccounting::new();
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        LogSegmentFetcher::new(store).with_read_gate(Arc::clone(&gate)),
        PhaseAccounting::pooled_over(&accounting),
    )
    .with_declared_columns(vec![DeclaredColumn::new("tags", DeclaredType::Str)])
    .with_scan_blocks_per_job(NonZeroU32::new(setup.blocks).expect("nonzero"));
    let ctx = session(provider);
    let plan = ctx
        .sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let batches: Vec<RecordBatch> = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("statement runs");
    let (jobs, inline) = gate
        .snapshot()
        .sites
        .iter()
        .find(|c| c.site == ReadSite::LogBlock)
        .map_or((0, 0), |c| (c.jobs, c.inline));
    assert_eq!(inline, 0, "a floor-zero gate runs nothing inline");
    Run {
        rows: pretty_format_batches(&batches)
            .expect("rows render")
            .to_string(),
        row_count: batches.iter().map(RecordBatch::num_rows).sum(),
        jobs,
        blocks_scanned: metric(&plan, "LogsScanExec", "blocks_scanned"),
        blocks_skipped: metric(&plan, "LogsScanExec", "blocks_skipped_by_threshold"),
        reopens: metric(&plan, "LogsScanExec", "reopens"),
        columnar_batches: metric(&plan, "LogsScanExec", "columnar_batches"),
    }
}

/// Runs `sql` at every [`BLOCKS_PER_JOB`] and asserts the rows are identical,
/// row for row, to one block per job's. Returns the runs in that order.
async fn same_rows(sql: &str, fixture: Fixture, erasure: bool) -> Vec<Run> {
    let mut runs = Vec::new();
    for blocks in BLOCKS_PER_JOB {
        let ran = run(
            sql,
            Setup {
                fixture,
                erasure,
                blocks,
            },
        )
        .await;
        if let Some(first) = runs.first() {
            let first: &Run = first;
            assert_eq!(
                ran.rows, first.rows,
                "{sql} at {blocks} blocks per job returns one per job's rows"
            );
        }
        runs.push(ran);
    }
    runs
}

const PLAIN: &str = "SELECT ts, body, severity_text, tags FROM logs";
const GROUP_BY: &str = "SELECT severity_text, count(*) AS n, min(ts) AS lo, max(ts) AS hi, \
                        count(tags) AS tagged FROM logs GROUP BY severity_text \
                        ORDER BY severity_text";
const TOP_K: &str = "SELECT ts, body, tags FROM logs ORDER BY ts LIMIT 5";

/// A plain projection returns every fixture row in the same order at each
/// blocks-per-job setting, from the columnar path, in `ceil(9 / n)` jobs per
/// segment.
#[tokio::test]
async fn a_plain_projection_matches_and_takes_ceil_n_jobs() {
    let runs = same_rows(PLAIN, Fixture::Clean, false).await;
    for (ran, (blocks, per_segment)) in runs.iter().zip([(1, 9), (2, 5), (4, 3), (8, 2)]) {
        assert_eq!(ran.row_count, SEGMENTS * PER_SEGMENT, "{blocks}");
        assert!(ran.columnar_batches > 0, "{blocks}: the columnar path");
        assert_eq!(ran.blocks_scanned, SEGMENTS * BLOCKS_PER_SEGMENT);
        assert_eq!(
            ran.jobs,
            (SEGMENTS * per_segment) as u64,
            "{blocks} blocks per job"
        );
    }
}

/// A `GROUP BY` over every row aggregates to the same groups.
#[tokio::test]
async fn a_group_by_matches() {
    let runs = same_rows(GROUP_BY, Fixture::Clean, false).await;
    assert!(runs[0].rows.contains("DEBUG") && runs[0].rows.contains("INFO"));
}

/// `ORDER BY ts LIMIT k` returns the same top rows with the TopK threshold
/// skipping blocks at every setting.
#[tokio::test]
async fn an_order_by_ts_limit_matches_with_the_threshold_skipping() {
    let runs = same_rows(TOP_K, Fixture::Clean, false).await;
    assert_eq!(runs[0].row_count, 5);
    for (ran, blocks) in runs.iter().zip(BLOCKS_PER_JOB) {
        assert!(
            ran.blocks_skipped > 0,
            "{blocks}: the threshold skipped blocks"
        );
    }
}

/// With segment 0's block 5 spilling to `attrs_raw`, every statement returns
/// the same rows at each setting: the fallback lands mid-run at 2, 4 and 8
/// blocks per job, and the batches the run built before it are emitted
/// before the fallback's rows.
///
/// At 4 blocks per job segment 0 takes nine `log_block` jobs (the run `0-3`;
/// the run `4-5`, which stops at block 5's fallback; the row path's six
/// decodes, five re-drained and block 5; the run `6-8`) and segment 1 three,
/// against twenty-four at one per job (segment 0: blocks 0-5 columnar, the
/// six row-path decodes, blocks 6-8; segment 1: nine).
///
/// Fails with a run that drops the batches built before a fallback block
/// (block 4's rows go missing at 2, 4 and 8).
#[tokio::test]
async fn a_fallback_mid_run_matches() {
    for sql in [PLAIN, GROUP_BY] {
        let runs = same_rows(sql, Fixture::Fallback, false).await;
        for (ran, blocks) in runs.iter().zip(BLOCKS_PER_JOB) {
            assert_eq!(ran.reopens, 1, "{sql} at {blocks}: the fallback ran");
        }
    }
    // The TopK threshold passes over block 5 before any run reaches it.
    same_rows(TOP_K, Fixture::Fallback, false).await;
    let runs = same_rows(PLAIN, Fixture::Fallback, false).await;
    assert_eq!(runs[0].row_count, SEGMENTS * PER_SEGMENT);
    let spilled = (0..STREAMS)
        .find(|&j| drain_position(j) == 1)
        .expect("a second stream");
    for i in 8..PER_STREAM {
        let tag = format!("t{}", spilled + STREAMS * i);
        assert!(runs[0].rows.contains(&tag), "{tag} served");
    }
    assert_eq!(runs[0].jobs, 24, "one job per decode at 1");
    assert_eq!(runs[2].jobs, 12, "the pinned run count at 4");
}

/// With an erasure pending, the scan serves no erased row and returns the
/// same rows at each setting.
#[tokio::test]
async fn an_erasure_pending_matches() {
    for sql in [PLAIN, GROUP_BY, TOP_K] {
        same_rows(sql, Fixture::Clean, true).await;
    }
    let runs = same_rows(PLAIN, Fixture::Clean, true).await;
    let erased = (0..PER_SEGMENT).filter(|k| k % 3 == 1).count() * SEGMENTS;
    assert_eq!(runs[0].row_count, SEGMENTS * PER_SEGMENT - erased);
}
