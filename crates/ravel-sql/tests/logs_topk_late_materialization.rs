//! Regression fixture for issue #774 / ADR-0774: a wide `ORDER BY ... LIMIT k`
//! over `logs` must decode the wide columns for the `k` winners only.
//!
//! Every test here runs the same statement twice through a real
//! [`ravel_sql::build_session`] over a `MemoryStore` fixture, differing in one
//! bit: `SqlConfig::late_materialization_extra_columns` is `Some(8)` (the
//! shipped default, so [`ravel_sql::TopKLateMaterialization`] is installed) or
//! `None` (the rule is not installed at all). The rewritten side must return
//! the identical rows -- by value, in every column, in the same order -- while
//! decoding far fewer pages.
//!
//! # The fixture
//!
//! Four segments of four blocks of four records, 64 records, over a schema of
//! nine fixed columns plus 32 declared `Str` attribute columns, so `SELECT *`
//! projects 41 columns. `ts` is `TS_BASE + index` over the whole fixture, so
//! `ORDER BY ts` is a total order with no tie (the tie case has its own
//! fixture). Exactly ten records carry `NEEDLE` in `body`, one every seventh
//! index, which puts each of them in a different block:
//!
//! | index | segment | block | row |
//! |---|---|---|---|
//! | 0 | 0 | 0 | 0 |
//! | 7 | 0 | 1 | 3 |
//! | 14 | 0 | 3 | 2 |
//! | 21 | 1 | 1 | 1 |
//! | 28 | 1 | 3 | 0 |
//! | 35 | 2 | 0 | 3 |
//! | 42 | 2 | 2 | 2 |
//! | 49 | 3 | 0 | 1 |
//! | 56 | 3 | 2 | 0 |
//! | 63 | 3 | 3 | 3 |
//!
//! `body LIKE '%NEEDLE%'` is deliberately the predicate: a substring `LIKE` is
//! neither a block prune nor a bloom probe, so it survives as a DataFusion
//! residual `FilterExec` above the scan and the scan reads every block. That is
//! the production shape this ADR exists for, and it is what makes "which
//! columns did phase 1 decode" the whole question.
//!
//! # One partition, on purpose
//!
//! `fetch_concurrency` is 1, so `target_partitions` is 1. The figures pinned
//! below are per-phase decode counts and object-store GET counts; with several
//! partitions feeding a `CoalescePartitionsExec` those become properties of the
//! tokio scheduler as well as of the rewrite, and a tie on the sort key would
//! resolve differently run to run. One partition makes every figure a property
//! of the rewrite alone. `logs_selective_scan_amplification.rs` covers the
//! multi-partition read shape.
//!
//! Both row-ref addressing branches are exercised: the `LIKE` statements push
//! no block-level predicate, so they take #693's whole-segment fast path where
//! every block survives and a block's whole-object index IS its surviving-block
//! index; `has_word_reaches_the_same_rows_through_the_striped_path` pushes a
//! content predicate, which forces the plan-then-stripe path where a row ref is
//! the block's position in the plan's survivor list, and
//! `split_row_groups_address_the_surviving_block_through_the_plan_list` splits
//! each segment's row groups across partitions.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::TimestampNanosecondArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, LogSegError, Predicate, RlogConfig, RlogReader,
    RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{
    BlockRangeFetcher, CacheFetchError, LogFetchError, LogSegmentFetcher, PhaseAccounting,
    PhaseWireByteCounts, QueryPhase,
};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision,
    SqlConfig, SqlError, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

const SEGMENTS: usize = 4;
const BLOCKS_PER_SEG: usize = 4;
const RECORDS_PER_BLOCK: usize = 4;
const RECORDS_PER_SEG: usize = BLOCKS_PER_SEG * RECORDS_PER_BLOCK;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEG;

/// Declared `Str` attribute columns. 32 of them plus the nine fixed columns
/// makes `SELECT *` a 41-column projection, of which a `ts`/`body` TopK needs
/// two: 39 surplus columns, well past the shipped threshold of 8.
const WIDE_COLUMNS: usize = 32;
const FIXED_COLUMNS: usize = 9;
const TOTAL_COLUMNS: usize = FIXED_COLUMNS + WIDE_COLUMNS;

/// The substring exactly ten of the 64 records carry, one every seventh index.
const NEEDLE: &str = "NEEDLE";
/// One every `NEEDLE_STRIDE` indices carries [`NEEDLE`].
const NEEDLE_STRIDE: usize = 7;
/// How many records carry it: indices 0, 7, ... 63.
const MATCHES: usize = 10;

const TS_BASE: i64 = 1_700_000_000_000_000_000;

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

fn declared_columns() -> Vec<DeclaredColumn> {
    (0..WIDE_COLUMNS)
        .map(|i| DeclaredColumn::new(format!("c{i:02}"), DeclaredType::Str))
        .collect()
}

/// Blocks of exactly [`RECORDS_PER_BLOCK`] records, so a record's index maps to
/// a `(segment, block, row)` address the header's table can state.
///
/// `group_blocks`, when `Some`, is the row group size in blocks; `None` keeps
/// the writer's default, which holds a whole segment's four blocks in one group.
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

/// The record at global index `index`. `ts` is strictly increasing in `index`,
/// each wide attribute is unique per record (so a wrong row is visible in every
/// column, not only in `body`), and every seventh record carries [`NEEDLE`].
fn record(index: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    let matches = index.is_multiple_of(NEEDLE_STRIDE);
    let body = if matches {
        format!("row {index} carries {NEEDLE} here")
    } else {
        format!("row {index} is filler")
    };
    let attrs = (0..WIDE_COLUMNS)
        .map(|c| {
            (
                format!("c{c:02}"),
                AttrValue::Str(format!("v{index}-{c:02}")),
            )
        })
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: TS_BASE + index as i64,
        observed_ts_ns: TS_BASE + index as i64,
        severity_num: 9,
        severity_text: "INFO".into(),
        body,
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// The same fixture with every record's `ts` collapsed onto one of two values,
/// so a TopK has to break ties. `ts` is the block ordinal within the segment,
/// which puts four records at each value inside a segment.
fn tied_record(index: usize) -> LogRecord {
    let mut r = record(index);
    let block = (index % RECORDS_PER_SEG) / RECORDS_PER_BLOCK;
    r.ts_ns = TS_BASE + block as i64;
    r.observed_ts_ns = r.ts_ns;
    r
}

async fn write_segment(
    store: &dyn ObjectStoreBackend,
    seg: usize,
    tied: bool,
    group_blocks: Option<usize>,
) -> SegmentRef {
    let recs: Vec<LogRecord> = (0..RECORDS_PER_SEG)
        .map(|r| {
            let index = seg * RECORDS_PER_SEG + r;
            if tied {
                tied_record(index)
            } else {
                record(index)
            }
        })
        .collect();
    let mut w = RlogWriter::new(block_config(group_blocks), identity((seg + 1) as u64));
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    let key = format!("logs/seg{seg}.rlog");
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
        series_count: 1,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: (seg + 1) as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

async fn build_snapshot(
    store: &dyn ObjectStoreBackend,
    tied: bool,
    group_blocks: Option<usize>,
) -> Snapshot {
    let mut segments = Vec::with_capacity(SEGMENTS);
    for s in 0..SEGMENTS {
        segments.push(write_segment(store, s, tied, group_blocks).await);
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

// ---- GET-counting store ---------------------------------------------------

/// Counts every `get` and the bytes it returned, so a test can read the exact
/// wire cost of a statement and cross-check it against `QueryAccounting`.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
    bytes: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }
    fn gets(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let got = self.inner.get(key, range).await?;
        self.bytes
            .fetch_add(got.data.len() as u64, Ordering::SeqCst);
        Ok(got)
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---- running one statement ------------------------------------------------

/// One measured run: the rows, the plan text, the per-phase counters, and the
/// object-store cost.
struct Run {
    rows: Vec<RecordBatch>,
    explain: String,
    /// Column pages the phase-1 (or, with the rule off, the only)
    /// `LogsScanExec` decoded and skipped.
    pages_decoded: usize,
    pages_skipped: usize,
    blocks_scanned: usize,
    blocks_total: usize,
    /// `LogsRowFetchExec`'s counters. All zero when the rule did not fire,
    /// because there is no such node.
    row_refs: usize,
    blocks_fetched: usize,
    segments_fetched: usize,
    gets: u64,
    bytes: u64,
    accounted_gets: u64,
}

impl Run {
    fn row_count(&self) -> usize {
        self.rows.iter().map(|b| b.num_rows()).sum()
    }
    fn has_fetch_node(&self) -> bool {
        self.explain.contains("LogsRowFetchExec")
    }
}

fn config(setup: Setup) -> SqlConfig {
    let mut config = SqlConfig::default();
    // `target_partitions` comes from `fetch_concurrency`. One, by default: see
    // the module header for why every figure below depends on it.
    config.engine.fetch_concurrency = setup.partitions;
    config.late_materialization_extra_columns = if setup.rule {
        SqlConfig::default().late_materialization_extra_columns
    } else {
        None
    };
    config
}

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    let bytes = 64 << 20;
    Arc::new(Cache::new(CacheLimits::new(bytes, 4096, bytes)))
}

fn session(provider: LogsTableProvider, config: &SqlConfig) -> SessionContext {
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        tenant,
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(ravel_types::TenantHash([0u8; 16])),
    )
    .expect("session builds")
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

/// How one measurement is set up. `cache` decides whether ADR-0046's read
/// cache is wired: it is what makes phase 2's re-reads free or real, and both
/// are worth pinning.
#[derive(Clone, Copy)]
struct Setup {
    rule: bool,
    tied: bool,
    cache: bool,
    partitions: usize,
    /// Row group size in blocks; `None` is the writer's default.
    group_blocks: Option<usize>,
    /// Zero the fetcher's plan bound so every segment is planned. Off, every
    /// fixture object is under the default bound and is opened once without a
    /// plan read (ADR-2677 decision 2).
    planned: bool,
}

impl Setup {
    /// The default: rewrite on, distinct `ts`, cache wired, one partition.
    fn new() -> Self {
        Setup {
            rule: true,
            tied: false,
            cache: true,
            partitions: 1,
            group_blocks: None,
            planned: false,
        }
    }
    fn planned(mut self) -> Self {
        self.planned = true;
        self
    }
    fn rule(mut self, rule: bool) -> Self {
        self.rule = rule;
        self
    }
    fn tied(mut self) -> Self {
        self.tied = true;
        self
    }
    fn uncached(mut self) -> Self {
        self.cache = false;
        self
    }
    /// Fan the scan out across `partitions`. Partitions take whole row groups
    /// (ADR-2414 decision A1), and with the default group size a
    /// segment's four blocks are one group, so each segment goes whole to one
    /// partition; [`Setup::group_blocks`] splits a segment into several groups
    /// that can land on different partitions. Only the result set and the
    /// phase-2 block count stay deterministic at this setting; see the module
    /// header.
    fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
    /// Write each segment with row groups of `blocks` blocks, so one segment
    /// holds several row groups that the striped deal can hand to different
    /// partitions.
    fn group_blocks(mut self, blocks: usize) -> Self {
        self.group_blocks = Some(blocks);
        self
    }
}

/// Plan and execute `sql` over a fresh copy of the fixture.
async fn run(sql: &str, setup: Setup) -> Run {
    let Setup {
        tied,
        cache,
        group_blocks,
        planned,
        ..
    } = setup;
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref(), tied, group_blocks).await;
    let counting = CountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let accounting = QueryAccounting::new();
    let fetcher = LogSegmentFetcher::new(store);
    let fetcher = if planned {
        fetcher.with_block_range_threshold(0)
    } else {
        fetcher
    };
    let fetcher = if cache {
        fetcher.with_cache(read_cache())
    } else {
        fetcher
    };
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        PhaseAccounting::pooled_over(&accounting),
    )
    .with_declared_columns(declared_columns());
    let config = config(setup);
    let ctx = session(provider, &config);

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
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    Run {
        pages_decoded: metric(&plan, "LogsScanExec", "pages_decoded"),
        pages_skipped: metric(&plan, "LogsScanExec", "pages_skipped"),
        blocks_scanned: metric(&plan, "LogsScanExec", "blocks_scanned"),
        blocks_total: metric(&plan, "LogsScanExec", "blocks_total"),
        row_refs: metric(&plan, "LogsRowFetchExec", "row_refs"),
        blocks_fetched: metric(&plan, "LogsRowFetchExec", "blocks_fetched"),
        segments_fetched: metric(&plan, "LogsRowFetchExec", "segments_fetched"),
        rows,
        explain,
        gets: counting.gets(),
        bytes: counting.bytes(),
        accounted_gets: accounting.snapshot().s3_requests(AccountedOp::Get),
    }
}

fn report(label: &str, r: &Run) {
    eprintln!(
        "[{label}] rows={} pages_decoded={} pages_skipped={} blocks={}/{} \
         row_refs={} blocks_fetched={} segments_fetched={} gets={} bytes={}",
        r.row_count(),
        r.pages_decoded,
        r.pages_skipped,
        r.blocks_scanned,
        r.blocks_total,
        r.row_refs,
        r.blocks_fetched,
        r.segments_fetched,
        r.gets,
        r.bytes,
    );
}

/// `SELECT *` over the fixture, ordered and limited.
fn wide_statement(k: usize) -> String {
    format!("SELECT * FROM logs WHERE body LIKE '%{NEEDLE}%' ORDER BY ts LIMIT {k}")
}

// ---- the tests ------------------------------------------------------------

/// The core claim: for `k` = 1, 10, and 20 (more than the ten matching rows),
/// the rewritten plan returns exactly the rows the single-phase plan returns,
/// by value in all 41 columns and in the same order, while phase 1 decodes only
/// the sort and predicate columns and phase 2 reads exactly the blocks holding
/// the winners.
///
/// The decode counters are the red side: with the rule off, `SELECT *` projects
/// `attrs`, which resolves to every dynamic column plus the overflow, so the
/// scan decodes 40 pages per block (39 columns plus `severity_text`'s
/// row-group dictionary page), 640 over the fixture, and skips none. With
/// the rule on, phase 1's scan projects `ts` and `body`, so it decodes three
/// pages per block (`ts` and `stream_ref` from
/// `ColumnSelection::fixed_only`, plus `body`) -- 48 over the fixture -- and
/// walks past the other 37.
#[tokio::test]
async fn a_wide_topk_returns_identical_rows_while_decoding_only_the_narrow_columns() {
    for k in [1usize, 10, 20] {
        let with = run(&wide_statement(k), Setup::new()).await;
        let without = run(&wide_statement(k), Setup::new().rule(false)).await;
        report(&format!("k={k} rule=on"), &with);
        report(&format!("k={k} rule=off"), &without);

        assert!(
            with.has_fetch_node(),
            "k={k}: the rule must fire on a 41-column projection needing 2 \
             columns:\n{}",
            with.explain
        );
        assert!(
            !without.has_fetch_node(),
            "k={k}: the rule is not installed on the baseline:\n{}",
            without.explain
        );

        // Identical results, column for column, row for row.
        assert_eq!(
            with.rows, without.rows,
            "k={k}: the rewrite changed the result"
        );
        assert_eq!(
            with.row_count(),
            k.min(MATCHES),
            "k={k}: the ten matching records bound the answer"
        );
        // ...and the answer really is the wide schema, not a narrowed one.
        assert_eq!(
            with.rows[0].num_columns(),
            TOTAL_COLUMNS,
            "k={k}: the restored schema carries every projected column"
        );

        // Both plans read every block: the substring `LIKE` prunes nothing, by
        // design. So the difference below is decode, not pruning.
        assert_eq!(with.blocks_total, TOTAL_BLOCKS, "k={k}");
        assert_eq!(without.blocks_total, TOTAL_BLOCKS, "k={k}");
        assert_eq!(with.blocks_scanned, TOTAL_BLOCKS, "k={k}");
        assert_eq!(without.blocks_scanned, TOTAL_BLOCKS, "k={k}");

        // Phase 1 decodes three pages per block (ts, stream_ref, body) and
        // walks past the other 37: the 32 declared columns, observed_ts,
        // severity_num, severity_text's id page and its row-group dictionary
        // page (every block reads its chunk's dictionary), and flags. The
        // baseline decodes all 40.
        assert_eq!(
            with.pages_decoded,
            3 * TOTAL_BLOCKS,
            "k={k}: phase 1 decodes three pages per block over {TOTAL_BLOCKS} blocks"
        );
        assert_eq!(
            with.pages_skipped,
            37 * TOTAL_BLOCKS,
            "k={k}: phase 1 walks past the wide columns' pages"
        );
        assert_eq!(
            without.pages_decoded,
            40 * TOTAL_BLOCKS,
            "k={k}: the single-phase scan decodes every column of every block"
        );
        assert_eq!(
            without.pages_skipped, 0,
            "k={k}: a `SELECT *` projection skips nothing"
        );
        assert_eq!(
            with.pages_decoded + with.pages_skipped,
            without.pages_decoded,
            "k={k}: both plans see the same pages; the rewrite decides which to decode"
        );

        // Phase 2 reads one block per winner: the ten matching records sit one
        // every seventh index, which lands each in a distinct block.
        assert_eq!(
            with.row_refs,
            k.min(MATCHES),
            "k={k}: one row ref per winner"
        );
        assert_eq!(
            with.blocks_fetched,
            k.min(MATCHES),
            "k={k}: the winners are in distinct blocks, so one fetch each"
        );

        // The accounting funnel sees phase 2's reads like any other scan read.
        assert_eq!(
            with.accounted_gets, with.gets,
            "k={k}: accounting GET count == store GETs, phase 2 included"
        );
        assert_eq!(without.accounted_gets, without.gets, "k={k}");
    }
}

/// The winners' segments, pinned separately from their blocks so the header's
/// address table is asserted and not merely documented: `k = 1` reaches one
/// segment, `k = 10` reaches all four.
#[tokio::test]
async fn phase_two_reads_only_the_segments_holding_the_winners() {
    let one = run(&wide_statement(1), Setup::new()).await;
    let all = run(&wide_statement(10), Setup::new()).await;
    report("k=1", &one);
    report("k=10", &all);

    assert_eq!(one.blocks_fetched, 1);
    assert_eq!(
        one.segments_fetched, 1,
        "the single lowest-ts match is record 0, in segment 0"
    );
    assert_eq!(all.blocks_fetched, MATCHES);
    assert_eq!(
        all.segments_fetched, SEGMENTS,
        "the ten matches are spread over every segment"
    );

    // With ADR-0046's read cache wired, phase 2's re-reads cost no request at
    // all: phase 1 already pulled each segment's object on the whole-segment
    // fast path and every one of these fixture objects is below the block-range
    // threshold, so the block read lands on the same `(0, object_size)` cache
    // key. One GET per segment, whatever `k` is.
    assert_eq!(
        one.gets, SEGMENTS as u64,
        "k=1, cached: one GET per segment"
    );
    assert_eq!(
        all.gets, SEGMENTS as u64,
        "k=10, cached: one GET per segment"
    );
}

/// The same statements with no read cache, where phase 2's re-reads ARE real
/// object-store requests: exactly one per segment holding a winner, on top of
/// phase 1's one per segment, however many of that segment's blocks won (issue
/// #797). This is the cost side of the trade the ADR states, pinned as a figure
/// rather than described -- in requests AND in bytes.
#[tokio::test]
async fn without_a_cache_phase_two_costs_exactly_one_get_per_segment_holding_a_winner() {
    let one = run(&wide_statement(1), Setup::new().uncached()).await;
    let all = run(&wide_statement(10), Setup::new().uncached()).await;
    let baseline = run(&wide_statement(10), Setup::new().rule(false).uncached()).await;
    report("uncached k=1", &one);
    report("uncached k=10", &all);
    report("uncached baseline", &baseline);

    assert_eq!(
        baseline.gets, SEGMENTS as u64,
        "the single-phase plan reads each segment once and decodes everything"
    );
    assert_eq!(
        one.gets,
        SEGMENTS as u64 + 1,
        "one winner: four segment reads plus one segment re-read"
    );
    // Ten winners in more blocks than segments, so one open per block and one
    // open per segment give different counts here.
    assert!(all.blocks_fetched > all.segments_fetched);
    assert_eq!(
        all.gets,
        2 * SEGMENTS as u64,
        "ten winners in four segments: four segment reads plus four re-reads"
    );
    assert_eq!(
        all.gets - baseline.gets,
        all.segments_fetched as u64,
        "the whole extra request cost is one GET per segment phase 2 opened"
    );

    // Bytes, not just requests. These fixture objects are below the
    // block-range threshold, so each segment re-read is a whole-object GET,
    // exactly as the baseline's segment reads are. 27,577 is the four objects
    // a single pass moves (the baseline); 34,235 is that plus 6,658 for the
    // one object holding the single winner, and 55,154 is the four objects
    // twice. Above the threshold the re-read narrows to the winners' own pages
    // instead (`phase_two_reads_only_the_winners_pages_and_opens_the_object_once`).
    assert_eq!(baseline.bytes, 27_577, "the four objects, once");
    assert_eq!(
        one.bytes, 34_235,
        "one winner adds one whole-object re-read"
    );
    assert_eq!(
        all.bytes,
        2 * baseline.bytes,
        "ten winners in four segments re-read each object once"
    );
    // And the rows are still identical, so the extra reads bought nothing but
    // the decode saving.
    assert_eq!(all.rows, baseline.rows);
    assert_eq!(all.accounted_gets, all.gets);
}

/// Ties on the sort key resolve identically. Phase 1's TopK is the same
/// operator over the same rows in the same order, so it picks the same rows in
/// the same order; phase 2 does not sort.
///
/// The fixture collapses `ts` onto the block ordinal, so each segment has four
/// records at each of four `ts` values and the ten matching records tie
/// heavily. Comparison is against the un-rewritten plan, so this fails if the
/// rewrite ever reorders a tied group rather than pinning one arbitrary order.
#[tokio::test]
async fn ties_on_the_sort_key_resolve_exactly_as_the_single_phase_plan() {
    let sql = format!("SELECT * FROM logs WHERE body LIKE '%{NEEDLE}%' ORDER BY ts LIMIT 6");
    let with = run(&sql, Setup::new().tied()).await;
    let without = run(&sql, Setup::new().rule(false).tied()).await;
    report("tied rule=on", &with);
    report("tied rule=off", &without);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    assert_eq!(with.row_count(), 6);
    // The fixture really does tie: six winners drawn from a `ts` domain of four
    // values cannot all be distinct.
    let tied_values: std::collections::BTreeSet<i64> = with
        .rows
        .iter()
        .flat_map(|b| {
            let ts = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                .expect("ts column");
            (0..ts.len()).map(|i| ts.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert!(
        tied_values.len() < 6,
        "the fixture must actually tie, saw {} distinct ts values",
        tied_values.len()
    );
    assert_eq!(
        with.rows, without.rows,
        "a tie resolved differently under the rewrite"
    );
}

/// A statement whose predicate matches nothing returns no rows and issues no
/// phase-2 read at all: there is no winner to re-read.
#[tokio::test]
async fn a_predicate_matching_nothing_fetches_no_block() {
    let with = run(
        "SELECT * FROM logs WHERE body LIKE '%NOTHINGMATCHESTHIS%' ORDER BY ts LIMIT 10",
        Setup::new(),
    )
    .await;
    report("empty", &with);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    assert_eq!(with.row_count(), 0);
    assert_eq!(with.row_refs, 0, "no winner, so no row ref");
    assert_eq!(with.blocks_fetched, 0, "phase 2 issues no read");
    assert_eq!(with.segments_fetched, 0);
    // Phase 1 still scans, so the statement is not trivially cheap for the
    // wrong reason.
    assert_eq!(with.blocks_scanned, TOTAL_BLOCKS);
}

/// A `has_word` predicate IS pushed into the fetch as an exact content arm, so
/// the scan leaves the whole-segment fast path and a segment's block list is
/// its pruned survivors rather than every block. The row refs must address the
/// same rows through it on both routes: planned, where the plan phase lists
/// the survivors, and unplanned (ADR-2677 decision 2), where the one open
/// prunes them and a row ref's position indexes that open's survivor list.
#[tokio::test]
async fn has_word_reaches_the_same_rows_through_the_striped_path() {
    let sql = format!("SELECT * FROM logs WHERE has_word(body, '{NEEDLE}') ORDER BY ts LIMIT 4");
    let without = run(&sql, Setup::new().rule(false)).await;
    report("has_word rule=off", &without);
    // The content arm is bloom-pruned at decode, so only the ten blocks
    // holding a match can be scanned. The first four of them fill the TopK
    // (indices 0, 7, 14, 21), and its threshold then skips the other six
    // (ADR-2677 decision 6): the remaining block of segment 1 starts at index
    // 28, and segments 2 and 3 start at 32 and 48. Unplanned, segments 2 and 3
    // are never opened, so their blocks are not in `blocks_total`.
    for (setup, route, blocks_total) in [
        (Setup::new().planned(), "planned", TOTAL_BLOCKS),
        (Setup::new(), "unplanned", 2 * BLOCKS_PER_SEG),
    ] {
        let with = run(&sql, setup).await;
        report(&format!("has_word rule=on {route}"), &with);
        assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
        assert_eq!(
            with.blocks_scanned, 4,
            "{route}: the four winners' blocks are scanned, the six later matches skipped"
        );
        assert_eq!(with.blocks_total, blocks_total, "{route}");
        assert_eq!(with.row_count(), 4, "{route}");
        assert_eq!(
            with.blocks_fetched, 4,
            "{route}: four winners in four distinct blocks"
        );
        assert_eq!(
            with.rows, without.rows,
            "{route}: the rewrite changed the result"
        );
    }
}

/// The same `has_word` statement fanned across four partitions. With the
/// default group size a segment's four blocks are one row group, so each
/// segment is taken whole by one partition; a segment is not split here, and the row refs of a partition's blocks are
/// positions in that segment's survivor list, which is not the identity either
/// (segment 0's survivors are blocks 0, 1 and 3). The split-group case is
/// `split_row_groups_address_the_surviving_block_through_the_plan_list`.
///
/// Only the result and the phase-2 block count are asserted: with four
/// partitions feeding a `CoalescePartitionsExec` the GET count and the decode
/// counters depend on the scheduler. The `ts` values are distinct, so the
/// answer itself does not.
#[tokio::test]
async fn whole_segments_dealt_to_partitions_return_the_same_rows() {
    let sql = format!("SELECT * FROM logs WHERE has_word(body, '{NEEDLE}') ORDER BY ts LIMIT 4");
    let with = run(&sql, Setup::new().partitions(4)).await;
    let without = run(&sql, Setup::new().rule(false).partitions(4)).await;
    report("striped rule=on", &with);
    report("striped rule=off", &without);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    // At most the ten surviving blocks, at least the four winners' blocks; how
    // many the TopK threshold skips in between depends on the scheduler.
    assert!(
        (4..=MATCHES).contains(&with.blocks_scanned),
        "the striped partitions between them decode at most the ten surviving blocks: {}",
        with.blocks_scanned
    );
    assert_eq!(with.row_count(), 4);
    assert_eq!(
        with.blocks_fetched, 4,
        "four winners in four distinct blocks"
    );
    assert_eq!(
        with.rows, without.rows,
        "the rewrite changed the result under striping"
    );
}

/// A segment whose row groups sit on different partitions. Groups of two blocks
/// give each segment two groups, and the deal numbers groups across segments, so
/// with four partitions segment 0's groups (blocks 0 and 1, then block 3 alone
/// after pruning) go to partitions 0 and 1, and every segment is split between
/// two partitions. Partition 1 owns segment 0's block 3, which is position 2 in
/// that segment's survivor list `[0, 1, 3]`; a row ref built from the block's
/// whole-object index (3) or from the cursor position in the partition's own
/// list (0) names a different block, or none, and phase 2 returns other rows.
///
/// Fails against a row ref carrying the whole-object block index instead of the
/// survivor position (segment 0's block 3 would address a block that does not
/// exist in the list), and against one carrying the position in the
/// partition's own share (partition 1's first block would address survivor 0).
#[tokio::test]
async fn split_row_groups_address_the_surviving_block_through_the_plan_list() {
    let sql = format!("SELECT * FROM logs WHERE has_word(body, '{NEEDLE}') ORDER BY ts LIMIT 10");
    let setup = Setup::new().partitions(4).group_blocks(2);
    let with = run(&sql, setup).await;
    let without = run(&sql, setup.rule(false)).await;
    report("split groups rule=on", &with);
    report("split groups rule=off", &without);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    assert_eq!(
        with.blocks_scanned, MATCHES,
        "the partitions between them decode the ten surviving blocks"
    );
    assert_eq!(with.row_count(), MATCHES);
    assert_eq!(
        with.blocks_fetched, MATCHES,
        "ten winners in ten distinct blocks"
    );
    assert_eq!(
        with.rows, without.rows,
        "the rewrite changed the result when a segment's groups are split"
    );
}

/// The three shapes the rule must decline, each pinned by the absence of
/// `LogsRowFetchExec` from the plan text.
///
/// - A narrow projection has nothing to late-materialize: `SELECT ts, body`
///   projects two columns and needs both, so the surplus is 0, under the
///   threshold of 8.
/// - An aggregate between the scan and the sort breaks the row identity a row
///   ref depends on: the sorted rows are groups, not scanned rows.
/// - A sort with no fetch materializes every row whatever the rule does.
#[tokio::test]
async fn the_rule_declines_narrow_aggregated_and_unlimited_plans() {
    let narrow = run(
        &format!("SELECT ts, body FROM logs WHERE body LIKE '%{NEEDLE}%' ORDER BY ts LIMIT 5"),
        Setup::new(),
    )
    .await;
    report("narrow", &narrow);
    assert!(
        !narrow.has_fetch_node(),
        "a two-column projection is not wide enough:\n{}",
        narrow.explain
    );
    assert!(
        narrow.explain.contains("SortExec"),
        "and it is still a TopK plan:\n{}",
        narrow.explain
    );
    assert_eq!(narrow.row_count(), 5);

    let aggregated = run(
        "SELECT severity_text, count(*) AS n FROM logs GROUP BY severity_text \
         ORDER BY n LIMIT 5",
        Setup::new(),
    )
    .await;
    report("aggregated", &aggregated);
    assert!(
        !aggregated.has_fetch_node(),
        "an aggregate sits between the scan and the sort:\n{}",
        aggregated.explain
    );
    assert!(
        aggregated.explain.contains("AggregateExec"),
        "and the fixture really did aggregate:\n{}",
        aggregated.explain
    );
    assert_eq!(aggregated.row_count(), 1);

    let unlimited = run(
        &format!("SELECT * FROM logs WHERE body LIKE '%{NEEDLE}%' ORDER BY ts"),
        Setup::new(),
    )
    .await;
    report("unlimited", &unlimited);
    assert!(
        !unlimited.has_fetch_node(),
        "a sort with no fetch is not a TopK:\n{}",
        unlimited.explain
    );
    assert_eq!(unlimited.row_count(), MATCHES);
}

/// The plan text a report reads: both phases and the row-ref column are visible
/// in one `EXPLAIN`, and the row-ref column does not reach the output schema.
#[tokio::test]
async fn explain_shows_both_phases_and_the_row_ref_column() {
    let with = run(&wide_statement(10), Setup::new()).await;
    eprintln!("{}", with.explain);

    assert!(
        with.explain
            .contains("LogsRowFetchExec: row_ref=__ravel_row_ref"),
        "phase 2 names the row-ref column:\n{}",
        with.explain
    );
    assert!(
        with.explain
            .contains(&format!("restored_columns={TOTAL_COLUMNS}")),
        "phase 2 says how wide the restored projection is:\n{}",
        with.explain
    );
    assert!(
        with.explain.contains(
            "LogsScanExec: partitions=1, content=0, prune=0, \
                      projection=[ts, body, __ravel_row_ref]"
        ),
        "phase 1 lists its narrow projection, row-ref column included:\n{}",
        with.explain
    );
    // Nothing above phase 2 has to drop the row ref: the fetch node's own
    // output schema is the restored one.
    for batch in &with.rows {
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|f| f.name() != "__ravel_row_ref"),
            "the row-ref column reached the result"
        );
    }
}

// ---- a whole-segment fast-path open that prunes a block -------------------

/// One-record blocks in the pruning fixture. Four, so that with block 1
/// pruned a whole-object index of 2 is still a valid position (it names block
/// 3), and an unguarded row-ref returns another block's row rather than an
/// out-of-range error.
const PRUNE_BLOCKS: usize = 4;
const PRUNE_TS_BASE: i64 = 100;
const PRUNE_OUTLIER_TS: i64 = 1_000_000_000;

/// `ts` as a SQL literal.
fn ts_literal(ns: i64) -> String {
    format!("TIMESTAMP '1970-01-01 00:00:00.{ns:09}'")
}

/// One segment of [`PRUNE_BLOCKS`] one-record blocks, block `b` at
/// `PRUNE_TS_BASE + b`. The catalog bounds are always that range; with
/// `outlier`, block 1 actually holds [`PRUNE_OUTLIER_TS`], so its skip-index
/// entry falls outside a window the catalog says contains the whole segment
/// and the fast path's open prunes it.
async fn prune_snapshot(store: &dyn ObjectStoreBackend, outlier: bool) -> Snapshot {
    // The writer orders records by stream, then ts. One stream per record,
    // ranked by stream id, puts each record in the block its rank names.
    let resource = |i: usize| {
        vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("svc{i}")),
        )]
    };
    let mut order: Vec<usize> = (0..PRUNE_BLOCKS).collect();
    order
        .sort_by_key(|&i| ravel_types::logstream::log_stream_id(&resource(i), "scope", "1.0", &[]));
    let recs: Vec<LogRecord> = order
        .iter()
        .enumerate()
        .map(|(block, &stream)| {
            let mut r = record(block);
            r.stream_id =
                ravel_types::logstream::log_stream_id(&resource(stream), "scope", "1.0", &[]);
            r.stream_attrs = stream_attrs_bytes(&resource(stream), "scope", "1.0", &[]);
            r.ts_ns = if outlier && block == 1 {
                PRUNE_OUTLIER_TS
            } else {
                PRUNE_TS_BASE + block as i64
            };
            r.observed_ts_ns = r.ts_ns;
            r
        })
        .collect();
    let cfg = RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity(1));
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");

    let reader = RlogReader::new(&bytes, &RlogConfig::default()).expect("open");
    let mut scan = reader
        .scan_blocks(
            &Predicate::TsRange {
                min_ns: i64::MIN,
                max_ns: i64::MAX,
            },
            &[],
            &ColumnSelection::fixed_only(),
        )
        .expect("scan");
    let mut block_ts = Vec::new();
    while let Some(block) = scan.next_block(&bytes).expect("block") {
        block_ts.push(block.iter().map(|r| r.ts_ns).collect::<Vec<_>>());
    }
    let mut want: Vec<Vec<i64>> = (0..PRUNE_BLOCKS as i64)
        .map(|b| vec![PRUNE_TS_BASE + b])
        .collect();
    if outlier {
        want[1] = vec![PRUNE_OUTLIER_TS];
    }
    assert_eq!(
        block_ts, want,
        "one record per block, in the intended order"
    );

    let key = "logs/pruned.rlog".to_string();
    let size = bytes.len() as u64;
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    Snapshot {
        segments: vec![SegmentRef {
            data_object_key: key,
            object_size: size,
            min_event_ts_ns: PRUNE_TS_BASE,
            max_event_ts_ns: PRUNE_TS_BASE + PRUNE_BLOCKS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: PRUNE_BLOCKS as u64,
            series_count: PRUNE_BLOCKS as u64,
            shard: 0,
            content_hash,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// `sql` over [`prune_snapshot`], one partition: the plan text and the rows,
/// or the error the statement failed with.
async fn run_pruned(
    outlier: bool,
    sql: &str,
    rule: bool,
) -> (String, Result<Vec<RecordBatch>, DataFusionError>) {
    let store = Arc::new(MemoryStore::new());
    let snapshot = prune_snapshot(store.as_ref(), outlier).await;
    let fetcher = LogSegmentFetcher::new(store).with_cache(read_cache());
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        PhaseAccounting::pooled_over(&QueryAccounting::new()),
    )
    .with_declared_columns(declared_columns());
    let ctx = session(provider, &config(Setup::new().rule(rule)));
    let plan = ctx
        .sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let explain = displayable(plan.as_ref()).indent(false).to_string();
    (explain, collect(plan, ctx.task_ctx()).await)
}

fn ts_of(rows: &[RecordBatch]) -> Vec<i64> {
    rows.iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("ts")
                .expect("ts column")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts type")
                .clone();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect()
}

fn pruned_statement(order: &str, k: usize) -> String {
    format!(
        "SELECT * FROM logs WHERE ts >= {} AND ts <= {} ORDER BY ts {order} LIMIT {k}",
        ts_literal(PRUNE_TS_BASE),
        ts_literal(PRUNE_TS_BASE + PRUNE_BLOCKS as i64 - 1),
    )
}

/// The whole-segment fast path stamps a row-ref with the decoded block's
/// whole-object index, which late materialization resolves as a position
/// among the blocks its reopen keeps. The two agree only while the open
/// prunes nothing. Here the catalog says the segment lies inside the window,
/// so the fast path runs, but block 1's skip-index entry lies outside it, so
/// the open drains blocks 0, 2 and 3. The winner in block 2 is at drain
/// position 1, and the scan fails with the typed survivor-mismatch error
/// instead of stamping a row-ref. Unpruned, the same statement shape returns
/// exactly the single-phase plan's rows, including the one in block 2.
///
/// Fails against a fast path that stamps the decoded index unguarded (the
/// reopen resolves position 2 to block 3, and the statement returns block 3's
/// row with no error) and against one that silently stamps the drain position
/// instead (it returns rows, not the typed error).
#[tokio::test]
async fn a_fast_path_open_that_prunes_a_block_refuses_to_stamp_a_row_ref() {
    let (explain, unpruned) = run_pruned(false, &pruned_statement("DESC", 2), true).await;
    assert!(
        explain.contains("LogsRowFetchExec"),
        "the rule fires:\n{explain}"
    );
    let unpruned = unpruned.expect("unpruned statement runs");
    let (_, single) = run_pruned(false, &pruned_statement("DESC", 2), false).await;
    let single = single.expect("single-phase statement runs");
    assert_eq!(ts_of(&unpruned), vec![PRUNE_TS_BASE + 3, PRUNE_TS_BASE + 2]);
    assert_eq!(
        unpruned, single,
        "a row-ref addressing block 2 returns the single-phase plan's rows"
    );

    let (_, single) = run_pruned(true, &pruned_statement("ASC", 2), false).await;
    assert_eq!(
        ts_of(&single.expect("without row refs the pruned open reads fine")),
        vec![PRUNE_TS_BASE, PRUNE_TS_BASE + 2]
    );
    let (_, pruned) = run_pruned(true, &pruned_statement("ASC", 2), true).await;
    let err = match pruned {
        Ok(rows) => panic!(
            "a pruned fast-path open stamped row refs: ts {:?}",
            ts_of(&rows)
        ),
        Err(e) => e,
    };
    let typed = match err.find_root() {
        DataFusionError::External(e) => e.downcast_ref::<SqlError>(),
        _ => None,
    };
    assert!(
        matches!(
            typed,
            Some(SqlError::LogFetch(LogFetchError::Corrupt {
                source: LogSegError::Corrupted(msg),
                ..
            })) if msg.contains("segment survivor mismatch")
                && msg.contains("whole-segment open decoded block 2 at drain position 1")
        ),
        "expected the typed survivor-mismatch error, got {err:?}"
    );
}

// ---- phase 2 over a ranged object -----------------------------------------

/// Blocks in the ranged fixture: one segment, one row group.
const RANGED_BLOCKS: usize = 6;
const RANGED_RECORDS: usize = RANGED_BLOCKS * RECORDS_PER_BLOCK;
const RANGED_TS_BASE: i64 = 100;
/// The statement's lower `ts` bound: the first record of block 1, so the skip
/// index prunes block 0 and every surviving block's position in the survivor
/// list is its whole-object index minus one.
const RANGED_TS_MIN: i64 = RANGED_TS_BASE + RECORDS_PER_BLOCK as i64;
/// The three winners of [`ranged_statement`]: indices 7, 14 and 21, the first
/// three [`NEEDLE`] records inside the window, in whole-object blocks 1, 3 and
/// 5, which are survivor positions 0, 2 and 4.
const RANGED_WINNER_BLOCKS: [u32; 3] = [1, 3, 5];

/// Records every GET's range and returned length, in issue order.
struct RecordingStore {
    inner: Arc<MemoryStore>,
    log: std::sync::Mutex<Vec<(GetRange, u64)>>,
}

impl RecordingStore {
    fn log(&self) -> Vec<(GetRange, u64)> {
        self.log.lock().expect("log lock").clone()
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
        let got = self.inner.get(key, range).await?;
        self.log
            .lock()
            .expect("log lock")
            .push((range, got.data.len() as u64));
        Ok(got)
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// One segment of [`RANGED_BLOCKS`] blocks of the main fixture's records, `ts`
/// rebased onto [`RANGED_TS_BASE`] so a window can be written as a literal. The
/// ref is level L1, whose version-4 chunk reads are not bridged down to the L0
/// run cap, so phase 2's wire bytes are exactly the pages it asks for.
async fn ranged_snapshot(store: &dyn ObjectStoreBackend) -> (Snapshot, Vec<u8>) {
    let recs: Vec<LogRecord> = (0..RANGED_RECORDS)
        .map(|i| {
            let mut r = record(i);
            r.ts_ns = RANGED_TS_BASE + i as i64;
            r.observed_ts_ns = r.ts_ns;
            r
        })
        .collect();
    let mut w = RlogWriter::new(block_config(None), identity(1));
    for r in &recs {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let key = "logs/ranged.rlog".to_string();
    let size = bytes.len() as u64;
    let content_hash = *blake3::hash(&bytes).as_bytes();
    store
        .put(
            &key,
            bytes::Bytes::from(bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put");
    let snapshot = Snapshot {
        segments: vec![SegmentRef {
            data_object_key: key,
            object_size: size,
            min_event_ts_ns: RANGED_TS_BASE,
            max_event_ts_ns: RANGED_TS_BASE + RANGED_RECORDS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: RANGED_RECORDS as u64,
            series_count: 1,
            shard: 0,
            content_hash,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L1 {
                input_set_hash: [0u8; 32],
                part_index: 0,
            },
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (snapshot, bytes)
}

fn ranged_statement() -> String {
    format!(
        "SELECT * FROM logs WHERE ts >= {} AND ts <= {} AND body LIKE '%{NEEDLE}%' \
         ORDER BY ts LIMIT {}",
        ts_literal(RANGED_TS_MIN),
        ts_literal(RANGED_TS_BASE + RANGED_RECORDS as i64 - 1),
        RANGED_WINNER_BLOCKS.len(),
    )
}

/// One run over [`ranged_snapshot`]: no read cache, every object read ranged
/// with no coalescing gap, so each GET is one the read asked for.
struct RangedRun {
    rows: Vec<RecordBatch>,
    explain: String,
    blocks_fetched: usize,
    segments_fetched: usize,
    log: Vec<(GetRange, u64)>,
    wire: PhaseWireByteCounts,
    object: Vec<u8>,
}

async fn run_ranged(rule: bool) -> RangedRun {
    run_ranged_gap(rule, Some(0), false).await
}

/// [`run_ranged`], with the block-range fetcher's coalescing gap chosen by the
/// caller: `Some(0)` fetches each winner-page run as its own GET (every GET is
/// one the read asked for, no coalesced holes); `None` leaves the production
/// default ([`BlockRangeFetcher`]'s request-cost gap), under which the winners'
/// non-adjacent pages coalesce and the fetch carries the holes between them.
///
/// `disable_crossover` turns off the coverage crossover
/// ([`BlockRangeFetcher::with_coverage_threshold`] raised past 1) so the ranged
/// path is taken even when the winners' coalesced span covers most of this tiny
/// fixture's object. It stands in for a production object large enough that
/// sparse winners never cross the whole-object threshold, which is the state
/// issue #797 measures; without it the production gap would read the whole
/// object and no coalesced range would exist to inspect.
async fn run_ranged_gap(rule: bool, coalesce_gap: Option<u64>, disable_crossover: bool) -> RangedRun {
    let base = Arc::new(MemoryStore::new());
    let (snapshot, object) = ranged_snapshot(base.as_ref()).await;
    let recording = Arc::new(RecordingStore {
        inner: base,
        log: std::sync::Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&recording) as Arc<dyn ObjectStoreBackend>;
    // The probe reaches back to the end of BLOCKS and no further, so it holds
    // every directory section and no page.
    let (_, blocks_end) = blocks_extent(&object);
    let mut block_range =
        BlockRangeFetcher::new(store.clone()).with_suffix_len(object.len() as u64 - blocks_end);
    if let Some(gap) = coalesce_gap {
        block_range = block_range.with_coalesce_gap(gap);
    }
    if disable_crossover {
        block_range = block_range
            .with_coverage_threshold(2.0)
            .with_whole_object_threshold(0);
    }
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range(block_range)
        .with_block_range_threshold(0);
    let wire = fetcher.phase_wire_byte_counter();
    let provider = LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        PhaseAccounting::pooled_over(&QueryAccounting::new()),
    )
    .with_declared_columns(declared_columns());
    let ctx = session(provider, &config(Setup::new().rule(rule)));
    let plan = ctx
        .sql(&ranged_statement())
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let rows = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("statement runs");
    RangedRun {
        explain: displayable(plan.as_ref()).indent(false).to_string(),
        blocks_fetched: metric(&plan, "LogsRowFetchExec", "blocks_fetched"),
        segments_fetched: metric(&plan, "LogsRowFetchExec", "segments_fetched"),
        rows,
        log: recording.log(),
        wire: wire.snapshot(),
        object,
    }
}

/// Phase 2's GETs: everything from the second footer probe on. Phase 1 probes
/// the object once, in its plan read, and hands the footer to its scan; every
/// phase-2 open probes for itself.
fn phase_two_gets(log: &[(GetRange, u64)]) -> &[(GetRange, u64)] {
    let probes: Vec<usize> = log
        .iter()
        .enumerate()
        .filter(|(_, (range, _))| matches!(range, GetRange::Suffix(_)))
        .map(|(i, _)| i)
        .collect();
    assert!(
        probes.len() >= 2,
        "phase 1's plan probe and at least one phase-2 open: {log:?}"
    );
    &log[probes[1]..]
}

/// Every page the winners' blocks hold in every column, which is the most a
/// `SELECT *` row fetch of them can need: the pages
/// `PageDir::projected_page_ranges` lists for them, dictionary pages included.
/// Returned as their total bytes and as the number of runs they form once
/// touching ranges are joined, which is the GET count a read with no
/// coalescing gap issues for them.
fn winner_pages(object: &[u8]) -> (u64, usize) {
    let reader = RlogReader::new(object, &RlogConfig::default()).expect("open");
    let dirs = reader.directories();
    let page_dir = dirs.page_dir();
    assert_eq!(page_dir.groups.len(), 1, "one row group");
    assert_eq!(page_dir.block_count(), RANGED_BLOCKS as u64);
    let mut ranges = page_dir
        .projected_page_ranges(0, &RANGED_WINNER_BLOCKS, None)
        .expect("ranges");
    ranges.sort_unstable();
    let bytes = ranges.iter().map(|(_, len)| len).sum();
    let mut runs = 0;
    let mut run_end = None;
    for (offset, len) in ranges {
        if run_end != Some(offset) {
            runs += 1;
        }
        run_end = Some(offset + len);
    }
    (bytes, runs)
}

/// The BLOCKS section's `[start, end)` in `object`.
fn blocks_extent(object: &[u8]) -> (u64, u64) {
    let footer = ravel_logseg::footer::open_source(object).expect("footer");
    let blocks = footer
        .section(ravel_logseg::footer::kind::BLOCKS)
        .copied()
        .expect("BLOCKS");
    (blocks.offset, blocks.offset + blocks.len)
}

/// Phase 2 over an object above the block-range threshold reads only the
/// winners' blocks' pages, opening the object once. The window prunes block 0,
/// so the winners' row refs carry survivor positions 0, 2 and 4 for
/// whole-object blocks 1, 3 and 5; phase 2 resolves those positions through the
/// same survivor list and fetches the three blocks' pages in every column, and
/// nothing of blocks 2 and 4 between them.
///
/// Fails against a fetch that takes the positions as whole-object indices (it
/// fetches block 2 instead of 1, 3 and 5 and the open refuses block 0 as no
/// survivor), against one that narrows the block set but keeps the coverage
/// crossover on the object's whole candidate set (blocks 1 to 5 of six cross
/// it, so the object is read whole), and against one that opens the object
/// once per winning block (three probes, not one).
#[tokio::test]
async fn phase_two_reads_only_the_winners_pages_and_opens_the_object_once() {
    let with = run_ranged(true).await;
    let without = run_ranged(false).await;
    assert!(
        with.explain.contains("LogsRowFetchExec"),
        "{}",
        with.explain
    );
    assert!(!without.explain.contains("LogsRowFetchExec"));
    assert_eq!(
        ts_of(&with.rows),
        vec![RANGED_TS_BASE + 7, RANGED_TS_BASE + 14, RANGED_TS_BASE + 21]
    );
    assert_eq!(with.rows, without.rows, "the rewrite changed the result");
    assert_eq!(with.blocks_fetched, RANGED_WINNER_BLOCKS.len());
    assert_eq!(with.segments_fetched, 1);

    let phase_two = phase_two_gets(&with.log);
    let (pages, page_runs) = winner_pages(&with.object);
    let (blocks_start, blocks_end) = blocks_extent(&with.object);
    let in_blocks = |range: &GetRange| match *range {
        GetRange::Range(start, end) => start >= blocks_start && end <= blocks_end,
        GetRange::Full | GetRange::Suffix(_) => false,
    };
    let block_bytes: u64 = phase_two
        .iter()
        .filter(|(range, _)| in_blocks(range))
        .map(|(_, len)| len)
        .sum();
    let total: u64 = phase_two.iter().map(|(_, len)| len).sum();
    eprintln!(
        "phase 2: gets={} bytes={total} block_bytes={block_bytes} winner_pages={pages} \
         object={} log={phase_two:?}",
        phase_two.len(),
        with.object.len(),
    );
    assert_eq!(
        phase_two
            .iter()
            .filter(|(range, _)| matches!(range, GetRange::Suffix(_)))
            .count(),
        1,
        "phase 2 opens the object once"
    );
    assert!(
        phase_two
            .iter()
            .all(|(range, _)| !matches!(range, GetRange::Full)),
        "phase 2 never reads the object whole"
    );
    assert_eq!(
        block_bytes, pages,
        "phase 2's BLOCKS bytes are the winners' pages, exactly"
    );
    // The whole of phase 2 is that one probe, which already holds every
    // directory and filter section, and one GET per run of winner pages.
    let probe = with.object.len() as u64 - blocks_end;
    assert_eq!(phase_two.len(), 1 + page_runs);
    assert_eq!(total, probe + pages);
    // Phase 2 is the fetch phase (issue #797): these are its exact GETs, its
    // WIRE bytes (probe metadata plus winner pages, as transferred), and its
    // BLOCKS-section bytes (the winner pages alone) on this fixture.
    assert_eq!(
        (phase_two.len(), total, pages),
        (115, 6_980, 2_820),
        "phase 2's exact request and wire-byte figures on this fixture"
    );
    // The fetch phase's own counter agrees with the GETs the store log slices
    // out as phase 2: every probe, section and page read phase 2 issued lands
    // on QueryPhase::Fetch, metadata and BLOCKS alike (ReadPhases::FETCH).
    assert_eq!(
        with.wire.phase_requests(QueryPhase::Fetch),
        phase_two.len() as u64,
        "fetch-phase GET count is phase 2's, exactly"
    );
    assert_eq!(
        with.wire.phase(QueryPhase::Fetch),
        total,
        "fetch-phase wire bytes are phase 2's whole cost, probe included"
    );
    // The scan phase now holds phase 1's BLOCKS bytes only: every BLOCKS byte
    // phase 2 moved is on the fetch phase instead. all_block_bytes is both
    // phases' BLOCKS bytes; phase 2's share (`block_bytes`) is on fetch.
    let all_block_bytes: u64 = with
        .log
        .iter()
        .filter(|(range, _)| in_blocks(range))
        .map(|(_, len)| len)
        .sum();
    assert_eq!(
        with.wire.phase(QueryPhase::Scan),
        all_block_bytes - block_bytes,
        "scan phase holds phase 1's BLOCKS bytes only"
    );
    // The six phases still sum to every wire byte the statement moved: moving
    // phase 2 to its own phase moved no byte out of the total.
    assert_eq!(
        with.wire.total(),
        with.log.iter().map(|(_, len)| len).sum::<u64>()
    );
}

/// A statement that runs no late materialization writes nothing to the fetch
/// phase. `run_ranged(false)` disables the rewrite, so there is no
/// `LogsRowFetchExec` and no phase-2 row fetch; phase 1's reads keep charging
/// the scan and probe phases.
///
/// Fails against distinguishing case (b) -- phase 1's own reads charged to the
/// fetch phase -- because there is no phase 2 here, so any nonzero fetch figure
/// is a phase-1 read misattributed. The flipped line is
/// `logs_scan/mod.rs`'s `ProbePhase::Fetch` moved onto phase 1's striped scan.
#[tokio::test]
async fn fetch_phase_is_zero_without_late_materialization() {
    let without = run_ranged(false).await;
    assert!(
        !without.explain.contains("LogsRowFetchExec"),
        "{}",
        without.explain
    );
    assert_eq!(
        without.wire.phase_requests(QueryPhase::Fetch),
        0,
        "no late materialization: zero fetch-phase GETs"
    );
    assert_eq!(
        without.wire.phase(QueryPhase::Fetch),
        0,
        "no late materialization: zero fetch-phase wire bytes"
    );
}

/// Phase 2 at the production coalescing gap (issue #797): the winners sit in
/// non-adjacent whole-object blocks 1, 3 and 5, and the default request-cost
/// gap ([`BlockRangeFetcher`]'s, not `with_coalesce_gap(0)`) coalesces their
/// pages into runs that also carry the holes between them (blocks 2 and 4's
/// pages). The fetch phase reports the whole cost; this pins its exact GETs and
/// wire bytes, and the ratio of fetch-phase BLOCKS bytes to the winners' own
/// page bytes, which is above 1 because of the coalesced holes.
#[tokio::test]
async fn phase_two_at_production_gap_coalesces_the_winners_blocks() {
    let prod = run_ranged_gap(true, None, true).await;
    assert!(
        prod.explain.contains("LogsRowFetchExec"),
        "{}",
        prod.explain
    );
    assert_eq!(
        ts_of(&prod.rows),
        vec![RANGED_TS_BASE + 7, RANGED_TS_BASE + 14, RANGED_TS_BASE + 21],
        "the production gap changes cost, not the winners"
    );
    assert_eq!(prod.blocks_fetched, RANGED_WINNER_BLOCKS.len());
    assert_eq!(prod.segments_fetched, 1);

    // Phase 2 is the fetch phase, so its GETs are exactly the fetch-phase
    // request count, and they are the tail of the store log (phase 1's TopK
    // scan runs to completion first). This is authoritative where the
    // suffix-probe slice is not: with the crossover off, phase 1 itself probes
    // more than once.
    let fetch_gets = prod.wire.phase_requests(QueryPhase::Fetch);
    let fetch_wire = prod.wire.phase(QueryPhase::Fetch);
    let phase_two = &prod.log[prod.log.len() - fetch_gets as usize..];
    let (pages, _gap0_runs) = winner_pages(&prod.object);
    let (blocks_start, blocks_end) = blocks_extent(&prod.object);
    let in_blocks = |range: &GetRange| match *range {
        GetRange::Range(start, end) => start >= blocks_start && end <= blocks_end,
        GetRange::Full | GetRange::Suffix(_) => false,
    };
    let fetch_block_bytes: u64 = phase_two
        .iter()
        .filter(|(range, _)| in_blocks(range))
        .map(|(_, len)| len)
        .sum();
    // Exact figures this fixture produces (issue #797). Phase 2 is one suffix
    // probe plus one coalesced BLOCKS range: the production gap is larger than
    // the whole BLOCKS section, so the winners' pages in blocks 1, 3 and 5
    // coalesce into a single range that spans the whole section, carrying
    // blocks 0, 2 and 4's pages as the holes between the winners.
    assert_eq!(fetch_gets, 2, "one probe plus one coalesced range");
    assert_eq!(fetch_wire, 9_704, "probe plus the coalesced range, as transferred");
    assert_eq!(
        fetch_block_bytes, 5_544,
        "the coalesced range is the whole BLOCKS section"
    );
    assert_eq!(fetch_wire, prod.wire.phase(QueryPhase::Fetch));
    // The one coalesced range is [blocks_start, blocks_end): the whole section.
    assert_eq!(
        phase_two
            .iter()
            .filter(|(r, _)| in_blocks(r))
            .collect::<Vec<_>>(),
        vec![&(GetRange::Range(blocks_start, blocks_end), 5_544u64)],
        "one coalesced range spans the whole BLOCKS section"
    );
    // Ratio of fetch-phase BLOCKS bytes to the winners' own pages, in basis
    // points to pin it without a float compare: 5544 / 2820 = 1.966, below 2.
    let ratio_bp = fetch_block_bytes * 10_000 / pages;
    assert_eq!(pages, 2_820, "the winners' own page bytes");
    assert_eq!(ratio_bp, 19_659, "fetch BLOCKS bytes / winner pages = 1.966");
    assert!(ratio_bp < 20_000, "ratio is below 2 on this fixture");
}
