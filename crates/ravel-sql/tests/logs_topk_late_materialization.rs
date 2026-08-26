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
//! a partition's block-index list is empty and the cursor position IS the
//! surviving-block index; `has_word_reaches_the_same_rows_through_the_striped_\
//! path` pushes a content predicate, which forces the plan-then-stripe path
//! where that list is explicit.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{CacheFetchError, LogSegmentFetcher};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SqlConfig,
    TenantDelegatingPool, TenantMemoryAccountant, build_session,
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
fn block_config() -> RlogConfig {
    RlogConfig {
        block_target_records: RECORDS_PER_BLOCK,
        ..RlogConfig::default()
    }
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

async fn write_segment(store: &dyn ObjectStoreBackend, seg: usize, tied: bool) -> SegmentRef {
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
    let mut w = RlogWriter::new(block_config(), identity((seg + 1) as u64));
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
    }
}

async fn build_snapshot(store: &dyn ObjectStoreBackend, tied: bool) -> Snapshot {
    let mut segments = Vec::with_capacity(SEGMENTS);
    for s in 0..SEGMENTS {
        segments.push(write_segment(store, s, tied).await);
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
    build_session(config, pool, SessionTable::Logs(Arc::new(provider)), false)
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
}

impl Setup {
    /// The default: rewrite on, distinct `ts`, cache wired, one partition.
    fn new() -> Self {
        Setup {
            rule: true,
            tied: false,
            cache: true,
            partitions: 1,
        }
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
    /// Fan the scan out, so a segment's surviving blocks are striped across
    /// partitions (ADR-0102) and a partition's block-index list stops being the
    /// identity. Only the result set and the phase-2 block count stay
    /// deterministic at this setting; see the module header.
    fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
}

/// Plan and execute `sql` over a fresh copy of the fixture.
async fn run(sql: &str, setup: Setup) -> Run {
    let Setup { tied, cache, .. } = setup;
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref(), tied).await;
    let counting = CountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let accounting = QueryAccounting::new();
    let fetcher = LogSegmentFetcher::new(store);
    let fetcher = if cache {
        fetcher.with_cache(read_cache())
    } else {
        fetcher
    };
    let provider =
        LogsTableProvider::new(snapshot, TenantHash(TENANT), fetcher, accounting.clone())
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
/// scan decodes 39 pages per block, 624 over the fixture, and skips none. With
/// the rule on, phase 1's scan projects `ts` and `body`, so it decodes three
/// pages per block (`ts` and `stream_ref` from
/// `ColumnSelection::fixed_only`, plus `body`) -- 48 over the fixture -- and
/// walks past the other 36.
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
        // walks past the other 36: the 32 declared columns, observed_ts,
        // severity_num, severity_text, and flags. The baseline decodes all 39.
        assert_eq!(
            with.pages_decoded,
            3 * TOTAL_BLOCKS,
            "k={k}: phase 1 decodes three pages per block over {TOTAL_BLOCKS} blocks"
        );
        assert_eq!(
            with.pages_skipped,
            36 * TOTAL_BLOCKS,
            "k={k}: phase 1 walks past the wide columns' pages"
        );
        assert_eq!(
            without.pages_decoded,
            39 * TOTAL_BLOCKS,
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
/// object-store requests: exactly one per block fetched, on top of phase 1's
/// one per segment. This is the cost side of the trade the ADR states, pinned
/// as a figure rather than described -- in requests AND in bytes, because the
/// two do not say the same thing here (see the byte assertions below).
#[tokio::test]
async fn without_a_cache_phase_two_costs_exactly_one_get_per_fetched_block() {
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
        "one winner: four segment reads plus one block read"
    );
    assert_eq!(
        all.gets,
        SEGMENTS as u64 + MATCHES as u64,
        "ten winners: four segment reads plus ten block reads"
    );
    assert_eq!(
        all.gets - baseline.gets,
        all.blocks_fetched as u64,
        "the whole extra request cost is one GET per block phase 2 fetched"
    );

    // Bytes, not just requests. Phase 2 restricts the DECODE to one block, but
    // its byte fetch is the query's normal fetch for that object: these fixture
    // objects are below the block-range threshold, so each block read is a
    // whole-object GET, exactly as the baseline's segment reads are. So the
    // cost is one object's bytes per winner, not one block's. 29,645 is the
    // four objects a single pass moves (the baseline); 103,606 is that plus
    // 73,961 for the ten whole-object block reads, and 36,818 is that plus
    // 7,173 for one. See ADR-0774's consequences: narrowing that fetch to the
    // named block indices is a ravel-query follow-up, and it is what would make
    // the byte cost per-block rather than per-object.
    assert_eq!(baseline.bytes, 29_645, "the four objects, once");
    assert_eq!(
        one.bytes, 36_818,
        "one winner adds one whole-object block read"
    );
    assert_eq!(
        all.bytes, 103_606,
        "ten winners add ten whole-object block reads"
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
/// records at each of four `ts` values and the twelve matching records tie
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
/// the scan takes the plan-then-stripe path and a partition's block-index list
/// is explicit rather than empty. The row refs must address the same rows
/// through it.
#[tokio::test]
async fn has_word_reaches_the_same_rows_through_the_striped_path() {
    let sql = format!("SELECT * FROM logs WHERE has_word(body, '{NEEDLE}') ORDER BY ts LIMIT 4");
    let with = run(&sql, Setup::new()).await;
    let without = run(&sql, Setup::new().rule(false)).await;
    report("has_word rule=on", &with);
    report("has_word rule=off", &without);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    // The content arm is bloom-pruned at decode: only the blocks holding a
    // matching record are scanned, one per matching record.
    assert_eq!(
        with.blocks_scanned, MATCHES,
        "bloom keeps the ten blocks holding a match"
    );
    assert_eq!(with.blocks_total, TOTAL_BLOCKS);
    assert_eq!(with.row_count(), 4);
    assert_eq!(
        with.blocks_fetched, 4,
        "four winners in four distinct blocks"
    );
    assert_eq!(with.rows, without.rows, "the rewrite changed the result");
}

/// The same `has_word` statement fanned across four partitions, where a
/// segment's surviving blocks really are striped (ADR-0102) and a partition's
/// block-index list is NOT the identity: with ten surviving blocks over four
/// segments, partition 0 owns segment 0's survivor 0, segment 1's survivor 1,
/// and segment 3's survivor 1. A row ref that recorded the cursor position
/// instead of the surviving-block index it names would address a different
/// block for two of those three and return the wrong rows.
///
/// Only the result and the phase-2 block count are asserted: with four
/// partitions feeding a `CoalescePartitionsExec` the GET count and the decode
/// counters depend on the scheduler. The `ts` values are distinct, so the
/// answer itself does not.
#[tokio::test]
async fn striped_partitions_address_the_surviving_block_not_the_cursor() {
    let sql = format!("SELECT * FROM logs WHERE has_word(body, '{NEEDLE}') ORDER BY ts LIMIT 4");
    let with = run(&sql, Setup::new().partitions(4)).await;
    let without = run(&sql, Setup::new().rule(false).partitions(4)).await;
    report("striped rule=on", &with);
    report("striped rule=off", &without);

    assert!(with.has_fetch_node(), "the rule fires:\n{}", with.explain);
    assert_eq!(
        with.blocks_scanned, MATCHES,
        "the striped partitions between them decode the ten surviving blocks"
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
