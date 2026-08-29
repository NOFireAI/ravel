//! Regression fixture for issue #761: a selective logs statement must move
//! object-store bytes proportional to the blocks it keeps, not the full-scan
//! bytes it once did.
//!
//! The mechanism (`ravel_query::log_fetcher` and `ravel_sql::logs_scan`):
//!
//! - A query carrying a block-level predicate (a declared-column `NumRange`
//!   prune arm for the ClickBench q20/q37 shapes, or a `has_word` content arm) is
//!   NOT `LogQuery::is_block_predicate_free`, so `LogsScanExec` takes the
//!   plan-then-stripe path, never #693's whole-segment fast path. The plan phase
//!   (`compute_plan_counts` -> `LogSegmentFetcher::plan_segment`) runs once per
//!   segment before any partition drains a block.
//! - Before #761 the plan slow branch read every segment WHOLE, and the scan's
//!   `BlockRangeFetcher::fetch_object_with_footer` resolved candidates by
//!   `SkipIndex::candidate_blocks(ts_min, ts_max, None, &[])` -- no numeric arms,
//!   so every block was a ts candidate, the coverage crossover fired, and one
//!   whole-object GET was issued. The selective predicate pruned blocks only at
//!   DECODE, shrinking `blocks_scanned` but not the bytes fetched. That was
//!   q37's "19,690 GETs / 11.7 GB to scan 144 of 17,731 blocks" against a full
//!   scan's "8,424 GETs / 11.1 GB".
//! - After #761 the NumRange arm is resolved against each object's FIELD_DIR and
//!   applied to `candidate_blocks` both at fetch (so the scan reads only
//!   surviving blocks) and in the plan phase (so it counts survivors from the
//!   skip index and fetches no block, carrying the footer forward). A text
//!   predicate the skip index cannot decide still falls back to a whole-object
//!   read, counted in the `plan_full_reads` metric.
//!
//! `selective_numeric_reads_only_surviving_blocks` pins the fixed numeric shapes
//! (byte cost proportional to the surviving fraction; the >= 75% coverage
//! crossover preserved). `text_predicate_falls_back_to_full_object_read` pins the
//! deliberately-kept fallback. `selective_third_no_partition_multiplication_\
//! under_cache_pressure` pins that eviction no longer re-reads whole objects.
//!
//! # Version 4 (ADR-0699 decision 5)
//!
//! The writer emits version-4 objects, so the surviving blocks these figures
//! are proportional to are no longer contiguous byte ranges: a block's pages sit
//! one per column chunk inside its row group, and the fetch is one coalesced
//! range per surviving `(row group, projected column)`. Every assertion below
//! keeps its meaning; the literals are measured on version-4 objects and each
//! says what it counts.
//!
//! Two fetcher settings this fixture pins deliberately, because the figures are
//! meaningless without them:
//!
//! - `SUFFIX_LEN` is sized to cover the object tail (footer, SKIP_IDX, PAGE_DIR,
//!   BLOOM), which is what `DEFAULT_LOG_SUFFIX_LEN` does at production object
//!   sizes and what issue #766 raised it for. At the default 256 KiB a probe
//!   would swallow this fixture's whole 81 KB object and there would be no byte
//!   figure left to read.
//! - `with_coalesce_gap(0)`. Under version 4 the hole between two wanted pages
//!   of one column is the OTHER blocks' pages of that column, so the gap
//!   threshold decides how many pruned blocks a range reads through. This
//!   fixture's geometry is one row group of six blocks whose `body` pages are
//!   ~12.4 KB each, so the hole between `body`'s surviving page and the next
//!   wanted chunk is ~61.8 KB -- just under the 64 KiB default, which therefore
//!   fuses the entire BLOCKS section into one range and puts the selective
//!   shapes back at full-scan bytes. At the production geometry (32-block row
//!   groups, ~6 KB pages) the same hole is ~186 KB and the default splits it.
//!   ADR-0699 left this threshold "not decided ... until measured"; the
//!   measurement is recorded in that ADR's as-built section, and this fixture
//!   pins the byte proportionality rather than the threshold.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_plan::{ExecutionPlan, collect};
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
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider, has_word_udf};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

/// Segments in the fixture, and the partitions requested. Equal so a
/// predicate-free full scan clears the fast path's
/// `relevant_segments >= target_partitions` conjunct.
const SEGMENTS: usize = 8;
const PARTS: usize = 8;
/// Blocks per segment (one record per block). 6 so a `code = 0` arm keeps one
/// block per segment (the very-selective q37 shape, 8/48), `code <= 1` keeps two
/// (the q20 shape, 16/48), and `code <= 4` keeps five (40/48, above the 0.75
/// coverage crossover).
const BLOCKS_PER_SEG: usize = 6;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEG;

/// Body filler bytes per record. Large enough that each object clears the small
/// block-range threshold this fixture sets, so every read takes ADR-0107's ranged
/// path, and large enough that `body` dominates each block: it is the column a
/// selective read still has to fetch a page of per surviving block, so it is
/// what makes the byte figures track the surviving fraction.
const BODY_BYTES: usize = 16 * 1024;
/// Suffix probe length for the ranged path: just over this fixture's 6,780-byte
/// object tail (footer + SKIP_IDX 139 + PAGE_DIR 199 + BLOOM 6,250), so one
/// probe per segment carries every plan section and the scan needs no second GET
/// for them. That is what `DEFAULT_LOG_SUFFIX_LEN` does at production object
/// sizes (issue #766); the production value would cover this whole 81 KB fixture
/// object and leave no byte figure to read.
const SUFFIX_LEN: u64 = 8192;

/// A marker word present in exactly the first block of every segment. A
/// `has_word` query for it survives one block per segment, but bloom is a
/// DECODE-only prune: the skip index cannot decide a text predicate, so this
/// shape exercises the plan-phase whole-object fallback (`plan_full_reads`) that
/// #761 could not remove.
const MARKER_FEW: &str = "MARKERFEW";

/// The declared numeric column every record carries: `code = <block index>`
/// (0..BLOCKS_PER_SEG). A `NumRange` prune arm on it (ClickBench q20/q37 shape)
/// is skip-index decidable, so #761's fetch-side pruning applies. `code = 0`
/// keeps one block per segment (q37); `code <= 1` keeps two (q20); `code <= 4`
/// keeps five of six, above the coverage crossover.
const CODE_COL: &str = "code";

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

/// Pseudo-random printable filler so the writer's body compression cannot shrink
/// the object back below the block-range threshold (same trick the
/// `logs_scan_scaling` bench uses).
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

/// Body for block `blk` of segment `seg`: the text marker (block 0 only) as a
/// leading word, then unique filler so bloom pruning can isolate the marked
/// block at decode.
fn body(seg: usize, blk: usize) -> String {
    let mut head = String::new();
    if blk == 0 {
        head.push_str(MARKER_FEW);
        head.push(' ');
    }
    let seed = (seg as u64) << 32 | blk as u64;
    format!("{head}{}", filler(seed, BODY_BYTES))
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
        body: body(seg, blk),
        trace_id: None,
        span_id: None,
        flags: 0,
        // The declared numeric column: one distinct value per block, so a
        // `NumRange` arm selects an exact block subset the skip index can prune.
        attrs: vec![(CODE_COL.to_string(), AttrValue::I64(blk as i64))],
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
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

/// Counts store `get` calls by `GetRange` shape and sums the bytes each returned,
/// so a test can read the exact wire cost independent of `QueryAccounting` (and
/// cross-check the two agree).
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
    fn gets(&self) -> u64 {
        self.full.load(Ordering::SeqCst)
            + self.suffix.load(Ordering::SeqCst)
            + self.range.load(Ordering::SeqCst)
    }
    fn full_gets(&self) -> u64 {
        self.full.load(Ordering::SeqCst)
    }
    fn suffix_gets(&self) -> u64 {
        self.suffix.load(Ordering::SeqCst)
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

fn read_cache(cache_bytes: u64) -> Arc<Cache<CacheFetchError>> {
    let max_entries = (cache_bytes / 4096).max(64) as usize;
    Arc::new(Cache::new(CacheLimits::new(
        cache_bytes,
        max_entries,
        cache_bytes,
    )))
}

/// A fetcher that treats every object as above the block-range threshold (ranged
/// path), with a tail-sized suffix probe, no coalescing slack, and ADR-0046's
/// read cache sized to `cache_bytes`. See this file's header for why those two
/// settings are pinned rather than left at their defaults.
fn fetcher(store: Arc<dyn ObjectStoreBackend>, cache_bytes: u64) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
        .with_coalesce_gap(0)
        .with_whole_object_threshold(0);
    LogSegmentFetcher::new(store)
        .with_block_range(block_range)
        .with_cache(read_cache(cache_bytes))
        .with_block_range_threshold(0)
}

fn provider(
    snapshot: Snapshot,
    fetcher: LogSegmentFetcher,
    acc: QueryAccounting,
) -> LogsTableProvider {
    LogsTableProvider::new(snapshot, TenantHash(TENANT), fetcher, acc)
        .with_declared_columns(vec![DeclaredColumn::new(CODE_COL, DeclaredType::I64)])
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

async fn drain(plan: Arc<dyn ExecutionPlan>) -> usize {
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

fn has_word(marker: &str) -> Expr {
    has_word_udf().call(vec![col("body"), lit(marker)])
}

/// `code = v`: a point `NumRange` arm keeping the one block whose value is `v`.
fn code_eq(v: i64) -> Expr {
    col(CODE_COL).eq(lit(v))
}

/// `code <= v`: an upper-bounded `NumRange` arm keeping blocks `0..=v`.
fn code_le(v: i64) -> Expr {
    col(CODE_COL).lt_eq(lit(v))
}

/// One measured shape.
struct Shape {
    label: &'static str,
    gets: u64,
    full_gets: u64,
    suffix_gets: u64,
    bytes: u64,
    blocks_scanned: usize,
    blocks_total: usize,
    rows: usize,
    plan_full_reads: usize,
    acc_gets: u64,
    acc_bytes: u64,
}

async fn measure(label: &'static str, filters: &[Expr], cache_bytes: u64) -> Shape {
    let base = Arc::new(MemoryStore::new());
    let snapshot = build_snapshot(base.as_ref()).await;
    let counting = CountingStore::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let acc = QueryAccounting::new();
    let prov = provider(snapshot, fetcher(store, cache_bytes), acc.clone());
    let plan = if filters.is_empty() {
        prov.plan(PARTS).expect("plan")
    } else {
        prov.plan_filters(PARTS, filters).expect("plan_filters")
    };
    let rows = drain(Arc::clone(&plan)).await;
    let snap = acc.snapshot();
    Shape {
        label,
        gets: counting.gets(),
        full_gets: counting.full_gets(),
        suffix_gets: counting.suffix_gets(),
        bytes: counting.bytes(),
        blocks_scanned: sum_metric(&plan, "blocks_scanned"),
        blocks_total: sum_metric(&plan, "blocks_total"),
        rows,
        plan_full_reads: sum_metric(&plan, "plan_full_reads"),
        acc_gets: snap.s3_requests(AccountedOp::Get),
        acc_bytes: snap.total_s3_bytes(),
    }
}

fn report(s: &Shape) {
    eprintln!(
        "[{}] gets={} (full={} suffix={}) bytes={} blocks_scanned={}/{} rows={} \
         plan_full_reads={} | accounting: gets={} bytes={}",
        s.label,
        s.gets,
        s.full_gets,
        s.suffix_gets,
        s.bytes,
        s.blocks_scanned,
        s.blocks_total,
        s.rows,
        s.plan_full_reads,
        s.acc_gets,
        s.acc_bytes,
    );
}

/// The #761 fix on the selective (q37/q20) shapes: a `NumRange` prune arm the
/// skip index can decide reads only the surviving blocks, not the whole object.
/// The plan phase counts survivors from the skip index (one suffix probe per
/// segment, no whole-object GET), the scan reads only the surviving blocks, and
/// the byte cost is proportional to the surviving fraction rather than equal to
/// a full scan. A shape whose survivors cover >= 75% of a segment still takes
/// the coverage crossover into a whole-object GET, so the crossover is pinned
/// too.
#[tokio::test]
async fn selective_numeric_reads_only_surviving_blocks() {
    // Cache large enough to hold the whole fixture with no eviction, so the
    // deterministic cost is exactly the cold plan + scan reads.
    let cache = 64 << 20;

    let full = measure("full_scan", &[], cache).await;
    // q37: one block per segment survives (8 of 48).
    let q37 = measure("selective_few (q37)", &[code_eq(0)], cache).await;
    // q20: two blocks per segment survive (16 of 48).
    let q20 = measure("selective_third (q20)", &[code_le(1)], cache).await;
    // Five of six blocks per segment survive (40 of 48): above the 0.75
    // coverage crossover, so each scanned segment is still read whole.
    let high = measure("high_coverage (>=75%)", &[code_le(4)], cache).await;
    report(&full);
    report(&q37);
    report(&q20);
    report(&high);

    // QueryAccounting agrees with the raw store counters on both axes.
    for s in [&full, &q37, &q20, &high] {
        assert_eq!(
            s.acc_gets, s.gets,
            "{}: accounting GET count == store GETs",
            s.label
        );
        assert_eq!(
            s.acc_bytes, s.bytes,
            "{}: accounting bytes == store bytes",
            s.label
        );
        assert_eq!(
            s.blocks_total, TOTAL_BLOCKS,
            "{}: blocks_total is the whole snapshot",
            s.label
        );
    }

    // Full scan: #693 whole-segment fast path. One whole-object GET per segment,
    // zero suffix probes, every block scanned. Unchanged by #761.
    assert_eq!(
        full.full_gets, SEGMENTS as u64,
        "full scan: one whole-object GET per segment"
    );
    assert_eq!(full.suffix_gets, 0, "full scan: no suffix probes");
    assert_eq!(
        full.gets, SEGMENTS as u64,
        "full scan: exactly one GET per segment and nothing else"
    );
    assert_eq!(
        full.blocks_scanned, TOTAL_BLOCKS,
        "full scan decodes every block"
    );
    assert_eq!(full.rows, TOTAL_BLOCKS, "full scan returns every record");
    // 649,900 = the eight version-4 objects' bytes exactly (81,144 B each plus
    // rounding across the writer's per-object framing). One whole-object GET per
    // segment and nothing else, so this is the object bytes and not a byte more.
    assert_eq!(
        full.bytes, 649_900,
        "full scan reads exactly the object bytes"
    );
    assert_eq!(full.plan_full_reads, 0, "full scan skips the plan phase");

    // q37: the plan phase decides every segment from the skip index (one suffix
    // probe per segment, no whole-object GET, no plan_full_reads), and the scan
    // reads only the one surviving block per segment. NumRange is skip-decidable,
    // so nothing falls back to a whole-object read.
    assert_eq!(
        q37.suffix_gets, SEGMENTS as u64,
        "q37: one plan-phase probe per segment, and the scan reuses the carried \
         footer (no scan-phase probe)"
    );
    assert_eq!(
        q37.full_gets, 0,
        "q37: no whole-object GET anywhere -- the numeric arm pruned the \
         candidate set below the coverage crossover"
    );
    assert_eq!(
        q37.plan_full_reads, 0,
        "q37: the plan phase counted survivors from the skip index, fetching no \
         block"
    );
    // Exact GET count: 8 plan probes plus 8 range GETs per segment. Under
    // version 4 the surviving block's bytes are one page per column chunk of its
    // row group, and this fixture's blocks carry pages in eight chunks (ts,
    // observed_ts, stream_ref, severity_num, severity_text, body, flags, code),
    // none of which coalesce at gap 0 because the pruned blocks' pages for the
    // same column sit between them. The two front sections (STREAM_DIR,
    // FIELD_DIR) and every tail section are absorbed by the probe and the 64 MiB
    // cache. No whole-object GET anywhere.
    assert_eq!(q37.gets, 72, "q37: exact GET count with no eviction");
    assert_eq!(
        q37.blocks_scanned, SEGMENTS,
        "q37 decodes exactly one block per segment"
    );
    assert_eq!(q37.rows, SEGMENTS, "q37 returns one record per segment");
    // Bytes proportional to the 8 of 48 surviving blocks (plus per-segment
    // directory reads and 8 suffix probes), a small fraction of a full scan --
    // NOT the full-scan bytes the pre-#761 whole-object read moved. The upper
    // bound fails if the fetch reads whole objects (the flip), the lower bound
    // fails if it somehow reads fewer than the surviving blocks.
    let q37_block_bytes = full.bytes * (SEGMENTS as u64) / (TOTAL_BLOCKS as u64);
    assert!(
        q37.bytes >= q37_block_bytes,
        "q37 reads at least its surviving-block bytes ({} vs {})",
        q37.bytes,
        q37_block_bytes
    );
    // 165,355 = 99,147 page bytes (the 8 surviving blocks' pages across their
    // eight column chunks; blocks differ slightly in encoded size, so the term
    // is the measured sum, not blocks x a constant) + 65,536 probe bytes
    // (8 x 8 KiB) + 672 front-section bytes (8 x (STREAM_DIR 62 + FIELD_DIR
    // 22)). The page term is 15% of the full scan; the probe term is the fixed
    // per-object directory cost, which on this deliberately tiny fixture is
    // 40% of the total and on a production 1.3 MB object is a rounding error.
    assert_eq!(
        q37.bytes, 165_355,
        "q37 moves the surviving blocks' page bytes plus the probe and the two \
         front sections, not the object"
    );
    assert!(
        q37.bytes <= full.bytes * 2 / 5,
        "q37 reads well under a full scan ({} vs {}), proportional to 8/48",
        q37.bytes,
        full.bytes
    );

    // q20: two surviving blocks per segment. Same plan shape as q37; the scan
    // reads twice as many blocks, so more bytes, still far under a full scan.
    assert_eq!(
        q20.suffix_gets, SEGMENTS as u64,
        "q20: one probe per segment"
    );
    assert_eq!(q20.full_gets, 0, "q20: no whole-object GET");
    assert_eq!(q20.plan_full_reads, 0, "q20: skip-index plan");
    // Same shape as q37: 8 probes plus 8 chunk ranges per segment. q20's second
    // surviving block per segment is ADJACENT to the first, so its page sits
    // next to the first's in every chunk and the pair coalesces even at gap 0 --
    // the range count is unchanged and only the bytes grow.
    assert_eq!(q20.gets, 72, "q20: exact GET count with no eviction");
    assert_eq!(
        q20.blocks_scanned,
        SEGMENTS * 2,
        "q20 decodes two blocks per segment"
    );
    assert_eq!(
        q20.rows,
        SEGMENTS * 2,
        "q20 returns two records per segment"
    );
    let q20_block_bytes = full.bytes * (2 * SEGMENTS as u64) / (TOTAL_BLOCKS as u64);
    assert!(
        q20.bytes >= q20_block_bytes,
        "q20 reads at least its surviving-block bytes ({} vs {})",
        q20.bytes,
        q20_block_bytes
    );
    // 264,405 = 198,197 page bytes (16 surviving blocks) + the same 65,536 probe
    // and 672 front-section bytes q37 pays: twice q37's page term, identical
    // fixed term.
    assert_eq!(
        q20.bytes, 264_405,
        "q20 moves twice q37's page bytes and the same fixed directory bytes"
    );
    assert!(
        q20.bytes <= full.bytes * 3 / 5,
        "q20 reads under a full scan ({} vs {}), proportional to 16/48",
        q20.bytes,
        full.bytes
    );
    assert!(
        q20.bytes > q37.bytes,
        "q20 reads more than q37 ({} vs {}): twice the surviving blocks",
        q20.bytes,
        q37.bytes
    );

    // High coverage (>= 75%): the numeric arm still prunes to five of six
    // blocks, but that clears the 0.75 coverage crossover, so each scanned
    // segment is read WHOLE -- one full GET per segment. The crossover is
    // preserved by #761, not bypassed.
    assert_eq!(
        high.full_gets, SEGMENTS as u64,
        "high coverage: the crossover still reads each scanned segment whole"
    );
    assert_eq!(
        high.plan_full_reads, 0,
        "high coverage: the plan phase is still skip-index only (the crossover \
         is a scan-phase decision)"
    );
    assert_eq!(
        high.blocks_scanned,
        SEGMENTS * 5,
        "high coverage decodes five blocks per segment"
    );
    assert_eq!(high.rows, SEGMENTS * 5, "high coverage returns 40 records");
}

/// A text predicate (`has_word`) is bloom-pruned only at DECODE: the skip index
/// cannot decide it, so #761's fetch-side pruning does not apply. The plan phase
/// falls back to a whole-object read per segment (`plan_full_reads`), the scan
/// still reads the whole object, and only the byte-decode is reduced (bloom
/// leaves one block per segment). This pins the fallback the fix deliberately
/// keeps for shapes the skip index cannot prune.
#[tokio::test]
async fn text_predicate_falls_back_to_full_object_read() {
    let cache = 64 << 20;
    let text = measure("text_fallback (has_word)", &[has_word(MARKER_FEW)], cache).await;
    report(&text);

    assert_eq!(
        text.plan_full_reads, SEGMENTS,
        "every relevant segment's plan phase read the whole object: the skip \
         index cannot decide a text predicate"
    );
    assert_eq!(
        text.full_gets, SEGMENTS as u64,
        "the whole-object read the plan fallback issues, one per segment"
    );
    assert_eq!(
        text.blocks_scanned, SEGMENTS,
        "bloom still prunes to one block per segment at decode"
    );
    assert_eq!(text.rows, SEGMENTS, "one matching record per segment");
    // The whole objects are read: bytes ~ a full scan plus the probes, the
    // amplification #761 cannot remove for a text predicate. 715,612 = 649,900
    // object bytes + 65,536 probe bytes (8 x 8 KiB) + 176 FIELD_DIR bytes
    // (8 x 22, read to resolve the arms before the fallback decides), the plan
    // phase's cost on top of the whole-object read its fallback then issues.
    let full = measure("full_scan", &[], cache).await;
    assert_eq!(
        text.bytes, 715_612,
        "text fallback: the whole objects plus one plan probe each"
    );
    assert!(
        text.bytes >= full.bytes,
        "text predicate still moves the full-object bytes ({} vs {})",
        text.bytes,
        full.bytes
    );
}

/// The q20 shape under a cache too small to hold the working set: eviction can
/// only add reads, but with #761 each surviving block is read at most once per
/// scan, so the GET count does NOT multiply by the partition count the way the
/// pre-fix whole-object re-reads did. Bounds the per-segment scan reads rather
/// than pinning a single eviction-dependent figure.
#[tokio::test]
async fn selective_third_no_partition_multiplication_under_cache_pressure() {
    let big = measure("q20_big_cache", &[code_le(1)], 64 << 20).await;
    // A cache five times smaller than the fixture's 650 KB of object bytes, so
    // the probe and directory extents really are evicted between the plan pass
    // and the per-partition scans.
    let small = measure("q20_small_cache", &[code_le(1)], 128 << 10).await;
    report(&big);
    report(&small);

    // No whole-object GET under either cache: the numeric arm keeps coverage
    // below the crossover regardless of eviction.
    assert_eq!(big.full_gets, 0, "q20 big cache: no whole-object GET");
    assert_eq!(small.full_gets, 0, "q20 small cache: no whole-object GET");

    // The plan phase's footer is carried in memory, not through the read cache,
    // so eviction cannot make a subset open re-probe: one probe per segment
    // under either cache size.
    assert_eq!(
        small.suffix_gets, SEGMENTS as u64,
        "q20 small cache: still one probe per segment, the carried footer is \
         not cache-resident state"
    );

    // Eviction can only add reads, never remove them.
    assert!(
        small.gets >= big.gets,
        "small cache issues at least as many GETs ({} vs {})",
        small.gets,
        big.gets
    );

    // The decisive #761 bound: the bytes never reach even a single full scan of
    // the objects. Each surviving block is owned by exactly one partition (the
    // ADR-0102 stripe) and the plan phase decodes no block, so every surviving
    // block is read once regardless of eviction; only the small directory
    // sections are ever re-fetched. So the 17.9 GB > 11.1 GB whole-object
    // re-read amplification the reproduction showed (bytes ~ 3x the object
    // bytes) is gone: bytes stay below one full pass. Bytes, not GET count, is
    // pinned here -- the raw GET count under eviction is scheduling-dependent,
    // but no GET is ever a whole-object read (asserted above), so partitions
    // cannot multiply the object bytes.
    let full = measure("full_scan", &[], 64 << 20).await;
    assert!(
        small.bytes < full.bytes,
        "q20 under pressure moves fewer bytes than a full scan ({} vs {}): each \
         surviving block is read once, none of the pruned blocks at all",
        small.bytes,
        full.bytes
    );
    // 264,405 with no eviction (16 surviving blocks' page bytes plus the fixed
    // probe and front-section bytes) against 479,147 under pressure: the
    // difference is re-fetched probe and directory extents, never a re-read
    // page. Both stay under the 649,900 a single full pass moves.
    assert_eq!(big.bytes, 264_405, "q20 with no eviction");
    assert_eq!(
        small.bytes, 479_147,
        "q20 under eviction: more directory re-reads, still under one full pass"
    );
}
