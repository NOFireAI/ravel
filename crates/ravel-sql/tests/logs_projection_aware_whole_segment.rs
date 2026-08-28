//! Tests for issue #862: the predicate-free full-window whole-segment fast path
//! (#693 part 3) must read the bytes the PROJECTION asks for, not the whole
//! object regardless of it.
//!
//! Before this, `LogsScanExec::whole_segment_fast_path`'s conjuncts decided that
//! every BLOCK of every relevant segment would be read, and the open then went
//! to `LogSegmentFetcher::scan_whole_accounted_with_tenant`, whose byte fetch
//! (`whole_object_bytes`) takes no column selection at all. So a statement
//! projecting one of a tenant's hundred declared columns moved one whole-object
//! GET per object, byte-identical to `SELECT *`: measured on the ClickBench
//! tenant, 12.031 GB and 3,471 GETs against 3,469 objects for a one-column
//! projection.
//!
//! The fix keeps every one of the fast path's conjuncts (pending erasure, block
//! predicates, segment containment, the partition-count floor) and keeps its
//! plan-phase elimination, and chooses the FETCH entry from the projection:
//!
//! - a projection covering every column of the resolved schema keeps the single
//!   `GetRange::Full` GET per object the fast path exists for;
//! - anything narrower takes `scan_accounted_with_tenant`, the already
//!   projection-aware ranged entry, which brings one coalesced range per
//!   surviving `(row group, projected column)` and keeps ADR-0107's 0.75
//!   coverage crossover untouched.
//!
//! Four properties are pinned here, all through the production
//! `TableProvider::scan` entry point (the one DataFusion calls, with the
//! projection it pushed down):
//!
//! - `narrow_projection_moves_only_the_projected_columns_bytes`: the EXACT GET
//!   count by range shape and the EXACT byte total for a one-declared-column
//!   projection, the EXACT marginal byte cost of one more projected column, and
//!   the `fast_path_ranged_opens` metric that attributes those GETs.
//! - `all_columns_projection_still_reads_one_whole_object_per_segment`:
//!   deliverable 3, so the narrow path cannot be widened into a regression of
//!   the shape the fast path was built for.
//! - `narrow_projection_rows_match_the_whole_object_path`: row-for-row equality
//!   between the ranged read and the same projection forced down the
//!   whole-object read, which is the constraint that matters: a plan that moves
//!   fewer bytes and returns fewer or different rows is a correctness bug.
//! - `narrow_projection_agrees_with_the_all_columns_scan`: the same equality
//!   against the widest read rather than an equally narrow one.
//!
//! # Fixture geometry, and why each figure is pinned rather than bounded
//!
//! `ATTR_COLS` declared string columns of `ATTR_BYTES` of incompressible filler
//! each, one record per block, `BLOCKS_PER_SEG` blocks per segment,
//! `SEGMENTS == PARTS` segments so the partition-count floor holds. The declared
//! columns dominate each object, so projecting one of `ATTR_COLS` of them is the
//! 1-of-many shape the ClickBench measurement had, at a size a test can hold.
//!
//! The objects come out above the 512 KiB `DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`,
//! the same side of it the production objects (3.47 MB) are on, so neither that
//! threshold nor the block-range threshold is overridden here. Two knobs are
//! pinned: `with_suffix_len(SUFFIX_LEN)`, because the default 256 KiB probe
//! window is 7% of a 3.47 MB production object but a third of a fixture object
//! and would swamp the page bytes the figure is about, and
//! `with_coalesce_gap(0)`, so a fetched run is exactly the projected pages and
//! not the gaps between them. The 0.75 coverage threshold is left at its default
//! by every test here; this fix does not change it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, CacheFetchError, LogSegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, FIRST_DECLARED_COL, LOG_COL_TS, LogsTableProvider};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

/// Segments in the fixture, and the partitions requested. Equal so
/// `relevant_segments >= target_partitions` holds and the fast path's
/// partition-count floor is satisfied.
const SEGMENTS: usize = 4;
const PARTS: usize = 4;
/// One record per block, so this is both the record count and the block count
/// per segment.
const BLOCKS_PER_SEG: usize = 6;
/// Declared string attribute columns every record carries. The 1-of-`ATTR_COLS`
/// projection is this fixture's stand-in for ClickBench's 1-of-105.
const ATTR_COLS: usize = 8;
/// Filler bytes per declared column value. Sized so each object clears the
/// 512 KiB whole-object threshold on its own (`BLOCKS_PER_SEG * ATTR_COLS *
/// ATTR_BYTES` = 768 KiB of incompressible column payload), which is what puts
/// the fixture on the production side of it with no knob override.
const ATTR_BYTES: usize = 16 * 1024;
/// Suffix probe window for the ranged entry, pinned so the byte figures below
/// decompose into probe bytes plus page bytes rather than depending on
/// `DEFAULT_LOG_SUFFIX_LEN`.
const SUFFIX_LEN: u64 = 8192;

/// Every row the fixture writes.
const TOTAL_ROWS: usize = SEGMENTS * BLOCKS_PER_SEG;

fn attr_key(i: usize) -> String {
    format!("c{i}")
}

/// The declared column set the provider advertises: every fixture attribute, as
/// a typed `Str` column, so a projection can name exactly one of them.
fn declared() -> Vec<DeclaredColumn> {
    (0..ATTR_COLS)
        .map(|i| DeclaredColumn::new(attr_key(i), DeclaredType::Str))
        .collect()
}

/// Schema index of declared column `i`.
fn declared_col(i: usize) -> usize {
    FIRST_DECLARED_COL + i
}

fn identity(seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

fn one_record_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

/// Pseudo-random printable filler, so the writer's page compression cannot
/// shrink a column back to nothing and distort the projected fraction (the same
/// generator `logs_selective_scan_amplification` uses).
fn filler(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push(ALPHABET[(z & 63) as usize] as char);
    }
    out
}

/// The value of declared column `col` in block `blk` of segment `seg`. Distinct
/// per `(seg, blk, col)`, so a row-for-row comparison cannot pass on a
/// coincidence and a mis-projected column reads as a different string, not as a
/// missing one.
fn attr_value(seg: usize, blk: usize, col: usize) -> String {
    let seed = (seg as u64) << 40 | (blk as u64) << 20 | col as u64;
    filler(seed, ATTR_BYTES)
}

fn ts_of(seg: usize, blk: usize) -> i64 {
    (seg * 1_000_000 + blk) as i64
}

fn record(seg: usize, blk: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    let ts = ts_of(seg, blk);
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        // Deliberately tiny: the declared columns are what the projection
        // narrows over, so `body` must not be a second large column that a
        // one-column projection would still be measured against.
        body: format!("s{seg}b{blk}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: (0..ATTR_COLS)
            .map(|c| (attr_key(c), AttrValue::Str(attr_value(seg, blk, c))))
            .collect(),
    }
}

async fn write_segment(store: &dyn ObjectStoreBackend, seg: usize) -> SegmentRef {
    let recs: Vec<LogRecord> = (0..BLOCKS_PER_SEG).map(|b| record(seg, b)).collect();
    let mut w = RlogWriter::new(one_record_blocks(), identity((seg + 1) as u64));
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
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: (seg + 1) as u64,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

async fn build_snapshot(store: &dyn ObjectStoreBackend) -> Snapshot {
    let mut segments = Vec::with_capacity(SEGMENTS);
    for s in 0..SEGMENTS {
        segments.push(write_segment(store, s).await);
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

// ---- byte- and shape-counting store --------------------------------------

/// Counts store `get` calls by `GetRange` shape and sums the bytes each
/// returned, so a test reads the exact wire cost independently of
/// `QueryAccounting` and can cross-check the two agree.
struct CountingStore {
    inner: Arc<MemoryStore>,
    full: AtomicU64,
    suffix: AtomicU64,
    range: AtomicU64,
    bytes: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            full: AtomicU64::new(0),
            suffix: AtomicU64::new(0),
            range: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
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
        match range {
            GetRange::Full => self.full.fetch_add(1, Ordering::SeqCst),
            GetRange::Suffix(_) => self.suffix.fetch_add(1, Ordering::SeqCst),
            GetRange::Range(_, _) => self.range.fetch_add(1, Ordering::SeqCst),
        };
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

fn read_cache() -> Arc<Cache<CacheFetchError>> {
    let bytes = 64u64 << 20;
    Arc::new(Cache::new(CacheLimits::new(
        bytes,
        (bytes / 4096) as usize,
        bytes,
    )))
}

/// A production-default fetcher, with ADR-0046's read cache wired: the fixture
/// objects clear the default block-range threshold on their own, so a narrow
/// projection reaches the ranged entry with neither threshold overridden. Only
/// the probe window and the coalescing slack are pinned (see this file's
/// header).
fn ranged_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        .with_coalesce_gap(0);
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(read_cache())
}

/// The same fetcher with every object BELOW the block-range threshold, so every
/// byte fetch is one `GetRange::Full` whole-object read whatever the projection
/// is. This is the "forced down the whole-object path" reference the row-for-row
/// comparison needs.
fn whole_object_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    LogSegmentFetcher::new(store)
        .with_cache(read_cache())
        .with_block_range_threshold(u64::MAX)
}

fn provider(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    acc: QueryAccounting,
) -> LogsTableProvider {
    LogsTableProvider::new(snapshot, TenantHash(TENANT), fetcher, acc)
        .with_declared_columns(declared())
}

fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    fn walk(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "LogsScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|c| walk(c))
    }
    walk(plan).expect("a LogsScanExec leaf")
}

fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let set = find_scan(plan).metrics().expect("metrics");
    set.iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

/// `(ts, value)` per emitted row, with `value` read from `value_col` and cast to
/// Utf8 so a declared `Str` column's `Dictionary(Int32, Utf8)` and a plain
/// string column compare the same way.
fn rows(batches: &[RecordBatch], value_col: usize) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts column at index 0");
        let vals = cast(batch.column(value_col), &DataType::Utf8).expect("cast to Utf8");
        let vals = vals
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 after cast");
        for i in 0..batch.num_rows() {
            assert!(!vals.is_null(i), "declared column value must not be NULL");
            out.push((ts.value(i), vals.value(i).to_string()));
        }
    }
    out.sort();
    out
}

/// What every row of the fixture should read as through declared column `col`.
fn want_rows(col: usize) -> Vec<(i64, String)> {
    let mut out: Vec<(i64, String)> = (0..SEGMENTS)
        .flat_map(|s| (0..BLOCKS_PER_SEG).map(move |b| (ts_of(s, b), attr_value(s, b, col))))
        .collect();
    out.sort();
    out
}

/// One measured execution: the plan (for its metrics), the batches, the store's
/// per-shape request and byte counters, and the query accounting.
struct Measured {
    plan: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    full_gets: u64,
    suffix_gets: u64,
    range_gets: u64,
    store_bytes: u64,
    object_bytes: u64,
    acc_gets: u64,
    acc_bytes: u64,
}

impl Measured {
    fn gets(&self) -> u64 {
        self.full_gets + self.suffix_gets + self.range_gets
    }
}

/// Run one projection through the production `TableProvider::scan` entry point
/// against a fresh fixture built for this call, at `PARTS` target partitions.
async fn measure(
    projection: Option<Vec<usize>>,
    build_fetcher: fn(Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher,
) -> Measured {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let object_bytes: u64 = snapshot.segments.iter().map(|s| s.object_size).sum();

    let counting = CountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let acc = QueryAccounting::new();
    let prov = provider(snapshot, build_fetcher(store), acc.clone());

    // The real DataFusion scan entry point, with the projection it would have
    // pushed down, and `target_partitions` read off the session config exactly
    // as `LogsTableProvider::scan` does.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(PARTS));
    let plan = prov
        .scan(&ctx.state(), projection.as_ref(), &[], None)
        .await
        .expect("scan");
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");

    let snap = acc.snapshot();
    Measured {
        plan,
        batches,
        full_gets: counting.full.load(Ordering::SeqCst),
        suffix_gets: counting.suffix.load(Ordering::SeqCst),
        range_gets: counting.range.load(Ordering::SeqCst),
        store_bytes: counting.bytes.load(Ordering::SeqCst),
        object_bytes,
        acc_gets: snap.s3_requests(AccountedOp::Get),
        acc_bytes: snap.total_s3_bytes(),
    }
}

// ---- deliverable 3: the all-columns shape the fast path exists for --------

/// A predicate-free full-window scan projecting EVERY column still takes exactly
/// one whole-object GET per object: no suffix probe, no byte-range GET, and the
/// bytes moved are the objects' bytes and not one more.
///
/// This is the read #693 part 3 introduced, and for this shape it is the right
/// plan: every byte of the object is wanted. It is pinned here so making the
/// narrow case ranged cannot silently cost the wide case a probe per object.
#[tokio::test]
async fn all_columns_projection_still_reads_one_whole_object_per_segment() {
    let m = measure(None, ranged_fetcher).await;

    assert_eq!(
        m.suffix_gets, 0,
        "all-columns fast path issues no suffix probe"
    );
    assert_eq!(
        m.range_gets, 0,
        "all-columns fast path issues no byte-range GET"
    );
    assert_eq!(
        m.full_gets, SEGMENTS as u64,
        "exactly one whole-object GET per object"
    );
    assert_eq!(
        m.store_bytes, m.object_bytes,
        "the whole-object path moves the objects' bytes exactly"
    );
    assert_eq!(m.acc_gets, m.gets(), "accounting GETs == store GETs");
    assert_eq!(
        m.acc_bytes, m.store_bytes,
        "accounting bytes == store bytes"
    );

    // The metric that attributes those GETs to the whole-object entry.
    assert_eq!(
        sum_metric(&m.plan, "fast_path_whole_object_opens"),
        SEGMENTS,
        "every fast-path open took the whole-object entry"
    );
    assert_eq!(
        sum_metric(&m.plan, "fast_path_ranged_opens"),
        0,
        "no fast-path open took the ranged entry"
    );
    // The fast path was taken, so no plan phase ran at all.
    assert_eq!(sum_metric(&m.plan, "plan_full_reads"), 0);

    let got: usize = m.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got, TOTAL_ROWS, "every written row is returned once");
    assert_eq!(
        rows(&m.batches, declared_col(0)),
        want_rows(0),
        "the all-columns scan reads every declared value"
    );
}

// ---- deliverable 1: the narrow shape -------------------------------------

/// A predicate-free full-window scan projecting `ts` plus ONE of `ATTR_COLS`
/// declared columns moves the projected columns' page bytes, not the corpus.
///
/// Every figure below is exact. The GET shape is structural: per object, one
/// suffix probe plus the coalesced ranges the projected `(row group, column)`
/// pages need, and ZERO whole-object GETs, because the coverage crossover's
/// numerator is the projected page bytes and a 1-of-8 projection sits far below
/// 0.75. The byte total is this fixture's measured figure, decomposed in the
/// assertion message; a writer layout change is expected to move it and to be
/// re-pinned deliberately, which is the point of pinning it.
#[tokio::test]
async fn narrow_projection_moves_only_the_projected_columns_bytes() {
    let m = measure(Some(vec![LOG_COL_TS, declared_col(0)]), ranged_fetcher).await;

    // Exact request shape: one probe per object, no whole-object GET.
    assert_eq!(
        m.suffix_gets, SEGMENTS as u64,
        "one suffix probe per object on the ranged entry"
    );
    assert_eq!(
        m.full_gets, 0,
        "a 1-of-{ATTR_COLS} projection never reaches the 0.75 coverage crossover"
    );
    assert_eq!(
        m.range_gets, 28,
        "exact byte-range GET count for the projected pages"
    );
    assert_eq!(m.gets(), 32, "exact total GET count");

    // Exact byte total for the whole run, against the exact corpus size.
    assert_eq!(
        m.object_bytes, 2_575_761,
        "fixture corpus size, so the byte figures below are read against a pinned \
         denominator and not a drifting one"
    );
    assert_eq!(
        m.store_bytes, 529_749,
        "exact bytes moved for a 1-of-{ATTR_COLS} projection over {SEGMENTS} objects: \
         the projected ts and c0 pages, plus the per-object fixed cost (the \
         {SUFFIX_LEN} B probe window, the front sections, and the non-BLOCKS \
         sections the reader re-verifies)"
    );
    assert_eq!(m.acc_gets, m.gets(), "accounting GETs == store GETs");
    assert_eq!(
        m.acc_bytes, m.store_bytes,
        "accounting bytes == store bytes"
    );

    // Proportional to the PROJECTED COLUMNS, shown as a marginal cost rather
    // than as a ratio: adding a second declared column adds one column's page
    // bytes and nothing else, because the fixed per-object cost above is paid
    // once either way. Both figures are exact, so the difference is exact.
    let two = measure(
        Some(vec![LOG_COL_TS, declared_col(0), declared_col(1)]),
        ranged_fetcher,
    )
    .await;
    assert_eq!(
        two.store_bytes, 826_553,
        "exact bytes moved for a 2-of-{ATTR_COLS} projection"
    );
    let per_column = two.store_bytes - m.store_bytes;
    assert_eq!(
        per_column, 296_804,
        "exact marginal cost of one more projected column"
    );
    // And that marginal cost really is one column's share of the corpus: the
    // fixture writes ATTR_COLS equal-sized column payloads, so a projected
    // column should cost about `object_bytes / ATTR_COLS`. Within 10%, which is
    // the per-column difference in how well the fixed sections compress.
    let ideal = m.object_bytes / ATTR_COLS as u64;
    assert!(
        per_column * 10 > ideal * 9 && per_column * 9 < ideal * 10,
        "one projected column must cost about one column's share of the corpus: \
         {per_column} vs {ideal}"
    );

    // Against the whole-object cost this replaces: the same statement on the
    // whole-object entry moves every object byte, and the narrow read moves
    // under a quarter of that.
    let whole = measure(
        Some(vec![LOG_COL_TS, declared_col(0)]),
        whole_object_fetcher,
    )
    .await;
    assert_eq!(
        whole.store_bytes, whole.object_bytes,
        "the whole-object entry moves the objects' bytes exactly"
    );
    assert!(
        m.store_bytes * 4 < whole.store_bytes,
        "a 1-of-{ATTR_COLS} projection must move under a quarter of the corpus: \
         {} vs {}",
        m.store_bytes,
        whole.store_bytes
    );

    // The metric that attributes the new path's GETs, so the change shows up in
    // a report and not only here.
    assert_eq!(
        sum_metric(&m.plan, "fast_path_ranged_opens"),
        SEGMENTS,
        "every fast-path open took the ranged entry"
    );
    assert_eq!(
        sum_metric(&m.plan, "fast_path_whole_object_opens"),
        0,
        "no fast-path open took the whole-object entry"
    );
    // The fast path's plan-phase elimination is preserved: narrowing the fetch
    // did not push this statement onto the plan-then-stripe path.
    assert_eq!(
        sum_metric(&m.plan, "plan_full_reads"),
        0,
        "the fast path still skips the plan phase"
    );
    for reason in [
        "fast_path_rejected_pending_erasure",
        "fast_path_rejected_block_predicate",
        "fast_path_rejected_segment_not_contained",
        "fast_path_rejected_fewer_segments_than_partitions",
    ] {
        assert_eq!(
            sum_metric(&m.plan, reason),
            0,
            "{reason} must not fire: the fast path's conjuncts are unchanged"
        );
    }

    // Every block is still read: narrowing the fetch narrows columns, never
    // blocks.
    assert_eq!(
        sum_metric(&m.plan, "blocks_total"),
        SEGMENTS * BLOCKS_PER_SEG
    );
    assert_eq!(
        sum_metric(&m.plan, "blocks_scanned"),
        SEGMENTS * BLOCKS_PER_SEG
    );
}

// ---- the correctness constraint ------------------------------------------

/// Row-for-row equality: the narrow projection read by range returns exactly the
/// rows the same projection returns when forced down the whole-object read, and
/// exactly the fixture's own written values.
///
/// This is the assertion the optimization has to survive. The ranged entry
/// fetches a strict subset of the object's bytes; if the block set, the
/// surviving-row set, or the column resolution differed by even one entry, the
/// two sides would disagree here even though both "work".
#[tokio::test]
async fn narrow_projection_rows_match_the_whole_object_path() {
    let projection = vec![LOG_COL_TS, declared_col(3)];
    let ranged = measure(Some(projection.clone()), ranged_fetcher).await;
    let whole = measure(Some(projection), whole_object_fetcher).await;

    // Proof the two sides really took different reads, so the comparison is
    // between two paths and not the same path twice.
    assert_eq!(
        ranged.full_gets, 0,
        "ranged side issues no whole-object GET"
    );
    assert_eq!(whole.suffix_gets, 0, "whole-object side issues no probe");
    assert_eq!(whole.range_gets, 0, "whole-object side issues no range GET");
    assert_eq!(whole.full_gets, SEGMENTS as u64);
    assert!(
        ranged.store_bytes < whole.store_bytes,
        "the ranged side must move fewer bytes ({} vs {}) or this test proves nothing",
        ranged.store_bytes,
        whole.store_bytes
    );

    let ranged_rows = rows(&ranged.batches, 1);
    let whole_rows = rows(&whole.batches, 1);
    assert_eq!(
        ranged_rows.len(),
        TOTAL_ROWS,
        "the ranged side returns every row"
    );
    assert_eq!(
        ranged_rows, whole_rows,
        "row-for-row equality between the ranged and whole-object reads"
    );
    assert_eq!(
        ranged_rows,
        want_rows(3),
        "and both equal the values the fixture wrote"
    );
}

/// The narrow projection's values are the same values the all-columns scan
/// reads for that column: a differential against the widest read, not only
/// against the same-width one.
#[tokio::test]
async fn narrow_projection_agrees_with_the_all_columns_scan() {
    let narrow = measure(Some(vec![LOG_COL_TS, declared_col(5)]), ranged_fetcher).await;
    let all = measure(None, ranged_fetcher).await;

    assert_eq!(
        rows(&narrow.batches, 1),
        rows(&all.batches, declared_col(5)),
        "narrow projection reads the same values the all-columns scan does"
    );
}
