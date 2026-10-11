//! ADR-2677 decision 6, pinned by order rather than by scheduling (issue
//! #2851): a threshold the TopK publishes before the logs scan opens a segment
//! skips exactly the blocks of that segment whose stored minimum `ts` is above
//! it, and a threshold published after the scan finished skips nothing.
//!
//! A statement run end to end decides when its TopK publishes by how the
//! runtime interleaves the scan with the sort, so its skip count varies from
//! run to run. Here the TopK never runs: the test plans `ORDER BY ts LIMIT k`,
//! takes the `SortExec`'s dynamic filter, drives the scan's one partition
//! itself, and publishes the threshold through that filter at a fixed point in
//! the scan.
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
    CeilingBreach, LogsTableProvider, SessionTable, SpillDecision, SqlConfig, TenantDelegatingPool,
    TenantMemoryAccountant, build_session,
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

/// `k` in the statement, and the offset of the answer's `k`-th `ts`.
const K: usize = 24;
const KTH_OFFSET: i64 = 107;

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
        body: format!("o{offset}"),
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
    fn keys_read(&self) -> BTreeSet<String> {
        self.gets.lock().unwrap().iter().cloned().collect()
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
        options.optimizer.enable_topk_dynamic_filter_pushdown = true;
        options.execution.batch_size = RECORDS_PER_BLOCK;
    }
    ctx
}

fn find(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Arc<dyn ExecutionPlan> {
    if plan.name() == name {
        return Arc::clone(plan);
    }
    plan.children()
        .into_iter()
        .find_map(|child| {
            let found = find(child, name);
            (found.name() == name).then_some(found)
        })
        .unwrap_or_else(|| Arc::clone(plan))
}

fn metric(scan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    scan.metrics()
        .expect("scan metrics")
        .iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

/// When the test publishes the threshold relative to the scan.
#[derive(Clone, Copy, Debug)]
enum Publish {
    /// After A's last row, before the scan opens B.
    BeforeSecondSegment,
    /// After the scan's stream has ended.
    AfterScan,
}

struct Outcome {
    /// `ts` offsets the scan emitted.
    emitted: BTreeSet<i64>,
    blocks_scanned: usize,
    blocks_total: usize,
    blocks_skipped: usize,
    segments_skipped: usize,
    /// B's blocks as `(min, max)` offsets, in block order.
    b_blocks: Vec<(i64, i64)>,
}

/// Drives the scan of `SELECT ts, body FROM logs ORDER BY ts LIMIT K` by
/// hand and publishes `ts < TS_BASE + KTH_OFFSET` through the sort's dynamic
/// filter at the point `publish` names.
async fn drive(planned: bool, publish: Publish) -> Outcome {
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
    let mut fetcher = LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
    if planned {
        fetcher = fetcher.with_block_range_threshold(0);
    }
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        PhaseAccounting::pooled_over(&accounting),
    );
    let ctx = session(provider);
    let plan = ctx
        .sql(&format!("SELECT ts, body FROM logs ORDER BY ts LIMIT {K}"))
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    let sort = find(&plan, "SortExec");
    let filter: Arc<DynamicFilterPhysicalExpr> = sort
        .downcast_ref::<SortExec>()
        .and_then(SortExec::dynamic_filter_expr)
        .unwrap_or_else(|| panic!("the TopK publishes a dynamic filter:\n{explain}"));
    let scan = find(&plan, "LogsScanExec");
    assert_eq!(scan.name(), "LogsScanExec", "{explain}");
    assert!(
        explain.contains("topk_threshold=ts"),
        "the scan confirmed the threshold:\n{explain}"
    );
    assert_eq!(scan.output_partitioning().partition_count(), 1, "{explain}");

    let threshold = Arc::new(BinaryExpr::new(
        Arc::clone(filter.children()[0]),
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
            Some(TS_BASE + KTH_OFFSET),
            None,
        ))),
    ));
    let mut stream: SendableRecordBatchStream =
        scan.execute(0, ctx.task_ctx()).expect("scan executes");
    let mut emitted = BTreeSet::new();
    let a_rows = segment_offsets(KEY_A).len();

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
            "the scan visits A first and emits all of it"
        );
        assert_eq!(
            store.keys_read(),
            BTreeSet::from([KEY_A.to_string()]),
            "nothing but A is read before the threshold is published"
        );
        assert_eq!(metric(&scan, "blocks_skipped_by_threshold"), 0);
        filter.update(threshold.clone()).expect("publish threshold");
    }
    while let Some(batch) = stream.next().await {
        emitted.extend(ts_offsets(&batch.expect("batch")));
    }
    if matches!(publish, Publish::AfterScan) {
        filter.update(threshold).expect("publish threshold");
    }

    let outcome = Outcome {
        emitted,
        blocks_scanned: metric(&scan, "blocks_scanned"),
        blocks_total: metric(&scan, "blocks_total"),
        blocks_skipped: metric(&scan, "blocks_skipped_by_threshold"),
        segments_skipped: metric(&scan, "segments_skipped_by_threshold"),
        b_blocks,
    };
    eprintln!(
        "[planned={planned} {publish:?}] blocks scanned {}/{} skipped {} segments skipped {} \
         B blocks {:?} reads {:?}",
        outcome.blocks_scanned,
        outcome.blocks_total,
        outcome.blocks_skipped,
        outcome.segments_skipped,
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
    for planned in [false, true] {
        let run = drive(planned, Publish::BeforeSecondSegment).await;
        // Which of B's blocks lie above the threshold, read off its own skip
        // index: the two starting at 200 and 201, one per stream, and each
        // follows a block that must be read.
        let above: BTreeSet<(i64, i64)> = run
            .b_blocks
            .iter()
            .copied()
            .filter(|&(min, _)| min > KTH_OFFSET)
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
                .map(|&(min, _)| min > KTH_OFFSET)
                .collect::<Vec<_>>(),
            vec![false, true, false, true],
            "B's blocks run stream by stream: {:?}",
            run.b_blocks
        );

        assert_eq!(
            run.blocks_skipped, 2,
            "planned={planned}: B's 200 and 201 blocks"
        );
        assert_eq!(
            run.segments_skipped, 1,
            "planned={planned}: C skipped whole"
        );
        assert_eq!(run.blocks_total, 8, "planned={planned}: A and B opened");
        assert_eq!(run.blocks_scanned, 6, "planned={planned}");
        let mut read = all_offsets(&[KEY_A]);
        read.extend((100..108).collect::<Vec<_>>());
        assert_eq!(
            run.emitted, read,
            "planned={planned}: A and B's 100 and 101 blocks are read, nothing else"
        );
    }
}

#[tokio::test]
async fn threshold_published_after_the_scan_skips_nothing() {
    for planned in [false, true] {
        let run = drive(planned, Publish::AfterScan).await;
        assert_eq!(run.blocks_skipped, 0, "planned={planned}");
        assert_eq!(run.segments_skipped, 0, "planned={planned}");
        assert_eq!(run.blocks_total, 10, "planned={planned}");
        assert_eq!(run.blocks_scanned, 10, "planned={planned}");
        assert_eq!(run.emitted, all_offsets(&[KEY_A, KEY_B, KEY_C]));
    }
}
