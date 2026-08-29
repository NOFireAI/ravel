//! Regression fixture for issue #862: the predicate-free full-window
//! whole-segment fast path must route on PROJECTION WIDTH, not only on which
//! blocks survive.
//!
//! The mechanism (`ravel_sql::logs_scan`, `ravel_query::log_fetcher`):
//!
//! - `LogsScanExec::whole_segment_fast_path` proves, with zero I/O, that every
//!   block of every relevant segment survives, so the plan phase can go and each
//!   segment can be assigned whole to one partition. Every conjunct it tests is
//!   about BLOCKS; none is about COLUMNS.
//! - Before #862 that assignment implied the read: each segment went to
//!   `LogSegmentFetcher::scan_whole_accounted_with_tenant`, one whole-object GET,
//!   however narrow the projection. That was right under RLOG v3, where reading
//!   every block meant needing every byte. Under v4 a block's pages sit one per
//!   column chunk inside its row group, and the ranged entry already fetches one
//!   coalesced range per surviving `(row group, projected column)` from the same
//!   `ColumnSelection` (ADR-0699 decision 5), so reading every block no longer
//!   means reading every byte.
//! - After #862 the read shape is a per-segment decision at open time
//!   (`PartitionCtx::open_by_column_chunk`), arbitrated by
//!   `LogSegmentFetcher::ranged_projection_pays`: the ranged path is taken only
//!   when the bytes the projection skips outweigh the round trips the protocol
//!   adds, measured against the fetch layer's own request-cost break-even.
//!
//! The acceptance criterion is NOT fewer bytes. On this store a request is the
//! expensive unit, so each test below pins BOTH the exact request count and the
//! exact bytes for its shape, and the wide shapes are pinned as tightly as the
//! narrow one: a routing change that only ever moves one way is a change that
//! cannot be caught over-applying.
//!
//! Two fetcher settings this fixture pins deliberately, for the reasons
//! `logs_selective_scan_amplification.rs` states at greater length:
//!
//! - `SUFFIX_LEN` is sized to the object tail. At the production 256 KiB default
//!   one probe would swallow this fixture's whole object and there would be no
//!   byte figure left to read.
//! - `with_coalesce_gap(0)`. Under version 4 the hole between two wanted pages
//!   of one column is the other blocks' pages of that column, so a nonzero gap
//!   fuses the whole BLOCKS section into one range and puts the narrow shape
//!   back at full-object bytes.
//!
//! The block-range threshold is NOT pinned to zero here, unlike most fixtures in
//! this crate. Zero would make the arbiter degenerate (every saving beats a
//! break-even of nothing), and then the wide shapes would pass for the wrong
//! reason. `THRESHOLD_DIVISOR` sets it to half an object instead, so the cost
//! model has to actually decide.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
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
use ravel_sql::{
    DeclaredColumn, DeclaredType, FIRST_DECLARED_COL, LOG_COL_ATTRS, LOG_COL_TS, LogsTableProvider,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

/// Segments in the fixture, and the partitions requested. Equal so a
/// predicate-free full-window statement clears the fast path's
/// `relevant_segments >= target_partitions` conjunct.
const SEGMENTS: usize = 4;
const PARTS: usize = 4;
/// Blocks per segment (one record per block).
const BLOCKS_PER_SEG: usize = 6;

/// Declared typed attribute columns the tenant carries, `d00`..`d09`. Ten of
/// them so a single-column projection is genuinely narrow (three object columns
/// out of twenty: `ts` and `stream_ref` are always decoded), the ClickBench q07
/// shape at fixture scale.
const DECLARED_COLUMNS: usize = 10;

/// Filler bytes in each declared column's value, per record. This is where the
/// object's bytes are: a narrow projection that reads one declared column skips
/// the other nine, which is the saving the routing decision exists to capture.
const DECLARED_BYTES: usize = 2048;

/// Suffix probe length for the ranged path, sized to this fixture's object tail
/// rather than to production objects. See the header.
const SUFFIX_LEN: u64 = 8192;

/// The block-range threshold, as a divisor of the smallest object in the
/// fixture: an object is above the threshold (so it can be ranged at all), and
/// the ranged path must save more than half an object before the cost model
/// judges it worth the extra round trips. A projection reading nine tenths of
/// the columns therefore stays on the whole-object read, and one reading three
/// twentieths does not.
const THRESHOLD_DIVISOR: u64 = 2;

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

/// Pseudo-random printable filler so the writer's compression cannot shrink the
/// object back under the threshold this fixture sets (the same generator
/// `logs_selective_scan_amplification.rs` uses).
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

fn declared_key(k: usize) -> String {
    format!("d{k:02}")
}

fn declared_columns() -> Vec<DeclaredColumn> {
    (0..DECLARED_COLUMNS)
        .map(|k| DeclaredColumn::new(declared_key(k), DeclaredType::Str))
        .collect()
}

fn record(seg: usize, blk: usize) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    let ts = (seg * 1_000_000 + blk) as i64;
    let attrs = (0..DECLARED_COLUMNS)
        .map(|k| {
            let seed = (seg as u64) << 40 | (blk as u64) << 20 | k as u64;
            (
                declared_key(k),
                AttrValue::Str(filler(seed, DECLARED_BYTES)),
            )
        })
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("s{seg}-b{blk}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
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
    let min = recs.iter().map(|r| r.ts_ns).min().unwrap();
    let max = recs.iter().map(|r| r.ts_ns).max().unwrap();
    SegmentRef {
        data_object_key: key,
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
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

/// Total stored bytes of the fixture: what a whole-object read of every segment
/// moves, and the figure every narrow-shape byte assertion is measured against.
fn snapshot_bytes(snapshot: &Snapshot) -> u64 {
    snapshot.segments.iter().map(|s| s.object_size).sum()
}

fn smallest_object(snapshot: &Snapshot) -> u64 {
    snapshot
        .segments
        .iter()
        .map(|s| s.object_size)
        .min()
        .expect("a non-empty fixture")
}

// ---- byte- and shape-counting store --------------------------------------

/// Counts store `get` calls by `GetRange` shape and sums the bytes each
/// returned, so a test reads the exact wire cost of a shape.
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
    let bytes = 64 << 20;
    Arc::new(Cache::new(CacheLimits::new(
        bytes,
        (bytes / 4096) as usize,
        bytes,
    )))
}

/// The fixture's fetcher, with the block-range threshold set to `threshold`.
/// Both the funnel's routing threshold and the block-range fetcher's own
/// size crossover take that value (`with_block_range_threshold` sets the pair),
/// which is also the break-even the projection has to beat.
fn fetcher(store: Arc<dyn ObjectStoreBackend>, threshold: u64) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        .with_coalesce_gap(0);
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(read_cache())
        .with_block_range_threshold(threshold)
}

fn provider(snapshot: Snapshot, fetcher: LogSegmentFetcher) -> LogsTableProvider {
    LogsTableProvider::new(
        snapshot,
        TenantHash(TENANT),
        fetcher,
        QueryAccounting::new(),
    )
    .with_declared_columns(declared_columns())
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

/// Projection over the resolved full schema: `ts` plus the FIRST declared
/// column. Three object columns of twenty (`ts` and `stream_ref` come free), the
/// q07 shape.
fn narrow_projection() -> Vec<usize> {
    vec![LOG_COL_TS, FIRST_DECLARED_COL]
}

/// Every fixed column and every declared column, but NOT the merged `attrs`
/// map: nineteen object columns of twenty. Wide without being `SELECT *`, so
/// the wide case is pinned by the cost model's arithmetic rather than by the
/// `attrs`-means-everything shortcut.
fn wide_projection() -> Vec<usize> {
    let mut proj: Vec<usize> = (0..LOG_COL_ATTRS).collect();
    proj.extend((0..DECLARED_COLUMNS).map(|k| FIRST_DECLARED_COL + k));
    proj
}

/// What one executed shape cost and returned.
struct Shape {
    full_gets: u64,
    suffix_gets: u64,
    range_gets: u64,
    bytes: u64,
    rows: usize,
    ranged_segments: usize,
    whole_object_segments: usize,
    /// Every `(ts, d00)` pair the shape emitted, for the cross-path row
    /// comparison. Only populated for projections carrying both.
    pairs: BTreeSet<(i64, String)>,
}

/// Plan and run one projection against a fresh fixture at `threshold`, and
/// report its exact wire cost.
async fn measure(projection: Option<Vec<usize>>, threshold_divisor: Option<u64>) -> Shape {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let threshold = match threshold_divisor {
        Some(d) => smallest_object(&snapshot) / d,
        // No divisor: a break-even larger than any object in the fixture, so
        // nothing can beat it and every segment keeps the whole-object read.
        None => snapshot_bytes(&snapshot),
    };
    let counting = CountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let prov = Arc::new(provider(snapshot, fetcher(store, threshold)));

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(PARTS));
    let plan = TableProvider::scan(prov.as_ref(), &ctx.state(), projection.as_ref(), &[], None)
        .await
        .expect("scan");
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");

    Shape {
        full_gets: counting.full.load(Ordering::SeqCst),
        suffix_gets: counting.suffix.load(Ordering::SeqCst),
        range_gets: counting.range.load(Ordering::SeqCst),
        bytes: counting.bytes.load(Ordering::SeqCst),
        rows: batches.iter().map(RecordBatch::num_rows).sum(),
        ranged_segments: sum_metric(&plan, "fast_path_ranged_segments"),
        whole_object_segments: sum_metric(&plan, "fast_path_whole_object_segments"),
        pairs: ts_and_first_declared(&batches),
    }
}

/// The `(ts, d00)` pairs in a result whose first column is `ts` and whose LAST
/// column is `d00`, or is `d00` at the first declared index. Both projections
/// this file compares carry `ts` at output position 0; `d00` is at position 1
/// for the narrow projection and at `LOG_COL_ATTRS` for the wide one, so it is
/// located by name.
fn ts_and_first_declared(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let Some(d0) = batch.schema().index_of(&declared_key(0)).ok() else {
            continue;
        };
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts at output position 0");
        // A declared `Str` column is a `Dictionary(Int32, Utf8)`; flatten it so
        // the comparison is over values, not over dictionary layout (the two
        // paths decode the same pages but need not build the same dictionary).
        let flat = cast(batch.column(d0), &DataType::Utf8).expect("d00 casts to Utf8");
        let vals = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 yields a StringArray");
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                String::new()
            } else {
                vals.value(i).to_string()
            };
            out.insert((ts.value(i), v));
        }
    }
    out
}

const TOTAL_ROWS: usize = SEGMENTS * BLOCKS_PER_SEG;

/// A narrow projection over fast-path-eligible segments takes the RANGED path:
/// exact request count, exact bytes.
///
/// Before #862 this whole shape was `SEGMENTS` whole-object GETs moving every
/// stored byte, whatever the projection: `LogsScanExec::execute`'s fast-path arm
/// called `open_segment_whole` unconditionally. Flipping that one call site back
/// (`open_segment_fast(.., by_chunk)` -> `open_segment_whole(..)`) fails this
/// test on all four counters below.
#[tokio::test]
async fn narrow_projection_reads_column_chunks_not_whole_objects() {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let stored = snapshot_bytes(&snapshot);
    drop(snapshot);

    let shape = measure(Some(narrow_projection()), Some(THRESHOLD_DIVISOR)).await;

    // Exact request law: one suffix probe and five byte-range GETs per segment
    // (front sections, then one coalesced run per projected column chunk), and
    // NO whole-object GET.
    assert_eq!(
        (shape.full_gets, shape.suffix_gets, shape.range_gets),
        (0, SEGMENTS as u64, 5 * SEGMENTS as u64),
        "exact request shape per segment: 0 full, 1 suffix probe, 5 ranges"
    );

    // Exact bytes: the projection reads three object columns of twenty, so the
    // wire cost is a fraction of the stored bytes rather than all of them.
    assert_eq!(
        (shape.bytes, stored),
        (71_554, 411_444),
        "exact wire bytes for the narrow shape, against the fixture's stored bytes"
    );

    // Routing: every segment took the ranged entry, none the whole-object one.
    assert_eq!(
        (shape.ranged_segments, shape.whole_object_segments),
        (SEGMENTS, 0),
        "a narrow projection routes every fast-path segment by column chunk"
    );

    assert_eq!(shape.rows, TOTAL_ROWS, "every row is still returned");
}

/// The same fast-path-eligible segments under a WIDE projection keep the
/// whole-object GET: exact request count, exact bytes.
///
/// This is the direction that catches over-application. Nineteen object columns
/// of twenty leaves nothing worth ranging for, and the ranged path would pay a
/// probe and a section GET per segment to fetch the same bytes. Deleting the
/// `ranged_projection_pays` guard (returning `true` unconditionally from
/// `LogSegmentFetcher::ranged_projection_pays`) fails this test.
#[tokio::test]
async fn wide_projection_keeps_the_whole_object_read() {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let stored = snapshot_bytes(&snapshot);
    drop(snapshot);

    let shape = measure(Some(wide_projection()), Some(THRESHOLD_DIVISOR)).await;

    assert_eq!(
        (shape.full_gets, shape.suffix_gets, shape.range_gets),
        (SEGMENTS as u64, 0, 0),
        "exact request shape per segment: one whole-object GET, no probe, no ranges"
    );
    assert_eq!(
        (shape.bytes, stored),
        (411_444, 411_444),
        "a whole-object read moves exactly the stored bytes"
    );
    assert_eq!(
        (shape.ranged_segments, shape.whole_object_segments),
        (0, SEGMENTS),
        "a wide projection keeps every fast-path segment on the whole-object read"
    );
    assert_eq!(shape.rows, TOTAL_ROWS, "every row is still returned");
}

/// `SELECT *` (the q24 shape) is the widest projection there is and must be
/// untouched by #862: one whole-object GET per segment, every stored byte.
#[tokio::test]
async fn select_star_is_unchanged() {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let stored = snapshot_bytes(&snapshot);
    drop(snapshot);

    let shape = measure(None, Some(THRESHOLD_DIVISOR)).await;

    assert_eq!(
        (shape.full_gets, shape.suffix_gets, shape.range_gets),
        (SEGMENTS as u64, 0, 0),
        "SELECT * issues exactly one whole-object GET per segment"
    );
    assert_eq!(
        (shape.bytes, stored),
        (411_444, 411_444),
        "SELECT * moves exactly the stored bytes"
    );
    assert_eq!(
        (shape.ranged_segments, shape.whole_object_segments),
        (0, SEGMENTS),
        "SELECT * keeps every fast-path segment on the whole-object read"
    );
    assert_eq!(shape.rows, TOTAL_ROWS, "every row is still returned");
}

/// The property that matters most: the two paths return the SAME rows for the
/// same query. Only the arbiter differs between the two runs -- one fixture, one
/// projection, one break-even raised past any object in it -- so any difference
/// in the result is the routing change and nothing else.
#[tokio::test]
async fn both_paths_return_identical_rows() {
    let ranged = measure(Some(narrow_projection()), Some(THRESHOLD_DIVISOR)).await;
    let whole = measure(Some(narrow_projection()), None).await;

    // The two runs really did take different paths.
    assert_eq!(
        (ranged.ranged_segments, ranged.whole_object_segments),
        (SEGMENTS, 0),
        "the first run took the ranged path"
    );
    assert_eq!(
        (whole.ranged_segments, whole.whole_object_segments),
        (0, SEGMENTS),
        "the second run took the whole-object path"
    );
    assert_eq!(
        (whole.full_gets, whole.suffix_gets, whole.range_gets),
        (SEGMENTS as u64, 0, 0),
        "the whole-object run issues one full GET per segment"
    );

    assert_eq!(
        ranged.rows, whole.rows,
        "both paths return the same number of rows"
    );
    assert_eq!(
        ranged.pairs.len(),
        TOTAL_ROWS,
        "the compared row set is the whole fixture, not a subset that happens to match"
    );
    assert_eq!(
        ranged.pairs, whole.pairs,
        "both paths return identical (ts, d00) rows"
    );
}
