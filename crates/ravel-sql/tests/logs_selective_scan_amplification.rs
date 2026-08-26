//! Reproduction fixture for issue #761: a selective logs statement moves as many
//! object-store GETs and as many bytes as a predicate-free full scan of the same
//! data, even though it decodes only a fraction of the blocks.
//!
//! Root cause, from the code (`ravel_query::log_fetcher` and
//! `ravel_sql::logs_scan`):
//!
//! - A query carrying any block-level predicate (a `has_word` content arm here,
//!   a declared-column equality/`NumRange` prune arm for the ClickBench q20/q37
//!   shapes) is NOT `LogQuery::is_block_predicate_free`, so `LogsScanExec` takes
//!   the plan-then-stripe path, never #693's whole-segment fast path. The plan
//!   phase (`compute_plan_counts` -> `LogSegmentFetcher::plan_segment`, slow
//!   branch) fetches every relevant segment once, cold, before any partition
//!   drains a block.
//! - Above the block-range threshold that fetch is `BlockRangeFetcher::
//!   fetch_object_with_footer`, whose candidate set is resolved by
//!   `resolve_extents` -> `SkipIndex::candidate_blocks(ts_min, ts_max, None, &[])`.
//!   It passes NO numeric/prune arms and NO stream refs: candidate selection is
//!   ts-only. For a full (or month-wide) window every block is a ts candidate, so
//!   the coverage crossover (candidate_bytes / BLOCKS-section bytes >= 0.75)
//!   fires and one whole-object GET is issued. The selective predicate prunes
//!   blocks only later, at DECODE, so it shrinks `blocks_scanned` but not the
//!   bytes fetched.
//!
//! Net: the selective query pays the full-scan's whole-object read PLUS a suffix
//! probe per segment (the fast path pays neither), and decodes a fraction of the
//! blocks. That is q37's "19,690 GETs / 11.7 GB to scan 144 of 17,731 blocks"
//! against the full scan's "8,424 GETs / 11.1 GB", reproduced here as a ratio.
//!
//! The whole-object reads are served from a cache large enough to hold the whole
//! fixture, so the plan-phase reads are the deterministic cost this test pins.
//! `selective_reread_amplifies_bytes_under_cache_pressure` then shrinks the cache
//! to show the scan-phase re-reads that push q20 past the object bytes on the
//! wire (17.9 GB > 11.1 GB of objects); it asserts only the direction of the
//! effect, because the exact figure is eviction- and scheduling-dependent.

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
use ravel_sql::{LogsTableProvider, has_word_udf};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use uuid::Uuid;

const TENANT: [u8; 16] = [7u8; 16];

/// Segments in the fixture, and the partitions requested. Equal so a
/// predicate-free full scan clears the fast path's
/// `relevant_segments >= target_partitions` conjunct.
const SEGMENTS: usize = 8;
const PARTS: usize = 8;
/// Blocks per segment (one record per block). 6 so a `has_word` marker placed in
/// one block per segment survives ~1/6 of the blocks (the very-selective q37
/// shape) and a marker in two blocks survives ~1/3 (the q20 shape).
const BLOCKS_PER_SEG: usize = 6;
const TOTAL_BLOCKS: usize = SEGMENTS * BLOCKS_PER_SEG;

/// Body filler bytes per record. Large enough that each object clears the small
/// block-range threshold this fixture sets, so every read takes ADR-0107's ranged
/// path, and large relative to the tiny suffix probe so the probe is a rounding
/// error in the byte figures rather than a second whole-object read.
const BODY_BYTES: usize = 16 * 1024;
/// Suffix probe length for the ranged path. Deliberately tiny so the probe's
/// bytes do not dominate; the point of the byte figures is the whole-object read
/// the coverage crossover issues, not the probe.
const SUFFIX_LEN: u64 = 256;

/// A marker word present in exactly the first block of every segment: a
/// `has_word` query for it survives one block per segment (SEGMENTS of
/// TOTAL_BLOCKS), the very-selective q37 shape.
const MARKER_FEW: &str = "MARKERFEW";
/// A marker word present in the first two blocks of every segment: ~1/3 of the
/// blocks survive, the q20 shape.
const MARKER_THIRD: &str = "MARKERTHIRD";

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

/// Body for block `blk` of segment `seg`: the block's markers (if any) as leading
/// words, then unique filler so bloom pruning can isolate the marked block.
fn body(seg: usize, blk: usize) -> String {
    let mut head = String::new();
    if blk == 0 {
        head.push_str(MARKER_FEW);
        head.push(' ');
    }
    if blk < 2 {
        head.push_str(MARKER_THIRD);
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
        attrs: Vec::new(),
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
/// path), with a tiny suffix probe and ADR-0046's read cache sized to `cache_bytes`.
fn fetcher(store: Arc<dyn ObjectStoreBackend>, cache_bytes: u64) -> LogSegmentFetcher {
    let block_range = BlockRangeFetcher::new(Arc::clone(&store))
        .with_suffix_len(SUFFIX_LEN)
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
}

fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.name() == "LogsScanExec" {
        return Arc::clone(plan);
    }
    plan.children()
        .iter()
        .find_map(|c| {
            if c.name() == "LogsScanExec" {
                Some(Arc::clone(c))
            } else {
                Some(find_scan(c))
            }
        })
        .expect("a LogsScanExec leaf")
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
        acc_gets: snap.s3_requests(AccountedOp::Get),
        acc_bytes: snap.total_s3_bytes(),
    }
}

fn report(s: &Shape) {
    eprintln!(
        "[{}] gets={} (full={} suffix={}) bytes={} blocks_scanned={}/{} rows={} \
         | accounting: gets={} bytes={}",
        s.label,
        s.gets,
        s.full_gets,
        s.suffix_gets,
        s.bytes,
        s.blocks_scanned,
        s.blocks_total,
        s.rows,
        s.acc_gets,
        s.acc_bytes,
    );
}

/// The headline of issue #761: a selective statement issues MORE object-store
/// GETs and reads roughly the SAME bytes as a predicate-free full scan of the
/// same data, while decoding a small fraction of the blocks.
#[tokio::test]
async fn selective_scan_costs_more_gets_and_equal_bytes_as_full_scan() {
    // Cache large enough to hold the whole fixture with no eviction, so the
    // deterministic cost is exactly the plan phase's cold reads.
    let cache = 64 << 20;

    let full = measure("full_scan", &[], cache).await;
    let q37 = measure("selective_few (q37)", &[has_word(MARKER_FEW)], cache).await;
    let q20 = measure("selective_third (q20)", &[has_word(MARKER_THIRD)], cache).await;
    report(&full);
    report(&q37);
    report(&q20);

    // QueryAccounting agrees with the raw store counters on both axes: the
    // deliverable's figures are the ones the engine records.
    for s in [&full, &q37, &q20] {
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
    // zero suffix probes, every block scanned.
    assert_eq!(
        full.full_gets, SEGMENTS as u64,
        "full scan: one whole-object GET per segment"
    );
    assert_eq!(
        full.suffix_gets, 0,
        "full scan: fast path issues no suffix probes"
    );
    assert_eq!(
        full.blocks_scanned, TOTAL_BLOCKS,
        "full scan decodes every block"
    );

    // Selective (q37 shape): plan-then-stripe. The plan phase probes every
    // segment (SEGMENTS suffix GETs) and the coverage crossover reads every
    // segment whole (SEGMENTS full GETs); the cache absorbs the scan-phase
    // re-reads. So it issues ~2x the full scan's GETs, and reads a whole object
    // per segment despite decoding only ~1/BLOCKS_PER_SEG of the blocks.
    assert_eq!(
        q37.suffix_gets, SEGMENTS as u64,
        "q37: one plan-phase probe per segment"
    );
    assert_eq!(
        q37.full_gets, SEGMENTS as u64,
        "q37: coverage crossover reads each segment whole"
    );
    assert!(
        q37.gets > full.gets,
        "q37 issues more GETs than the full scan ({} vs {})",
        q37.gets,
        full.gets
    );
    // Same bytes as the full scan (within the tiny suffix-probe overhead), even
    // though it decodes far fewer blocks: the selective predicate never reached
    // the fetch's candidate set.
    assert!(
        q37.bytes >= full.bytes
            && q37.bytes <= full.bytes + (SEGMENTS as u64) * SUFFIX_LEN + full.bytes / 20,
        "q37 reads ~the full-scan bytes ({} vs {})",
        q37.bytes,
        full.bytes
    );
    assert!(
        q37.blocks_scanned <= SEGMENTS * 2,
        "q37 decodes ~one block per segment (got {})",
        q37.blocks_scanned
    );
    assert!(
        (q37.blocks_scanned as f64) < (full.blocks_scanned as f64) / 2.0,
        "q37 decodes far fewer blocks than the full scan ({} vs {})",
        q37.blocks_scanned,
        full.blocks_scanned
    );

    // q20 shape (~1/3 of blocks survive): same fetch cost, more blocks decoded.
    assert_eq!(q20.suffix_gets, SEGMENTS as u64);
    assert_eq!(q20.full_gets, SEGMENTS as u64);
    assert!(
        q20.bytes >= full.bytes,
        "q20 reads at least the full-scan bytes"
    );
    assert!(
        q20.blocks_scanned >= q37.blocks_scanned,
        "q20 decodes more blocks than q37 ({} vs {})",
        q20.blocks_scanned,
        q37.blocks_scanned
    );
}

/// q20's other half: on a cache too small to hold the working set, the
/// plan-phase whole-object reads are evicted before the scan phase re-reads
/// them, so bytes on the wire exceed the object bytes (the reference's
/// 17.9 GB > 11.1 GB). Only the direction is asserted: the exact figure is
/// eviction- and scheduling-dependent, so this is not a byte-exact pin.
#[tokio::test]
async fn selective_reread_amplifies_bytes_under_cache_pressure() {
    let big = measure("q20_big_cache", &[has_word(MARKER_THIRD)], 64 << 20).await;
    // A cache far smaller than the fixture's object bytes: plan-phase reads are
    // evicted before the scan phase re-reads them.
    let small = measure("q20_small_cache", &[has_word(MARKER_THIRD)], 32 << 10).await;
    report(&big);
    report(&small);

    // Eviction can only add reads, never remove them.
    assert!(
        small.gets >= big.gets,
        "small cache issues at least as many GETs ({} vs {})",
        small.gets,
        big.gets
    );
    assert!(
        small.bytes > big.bytes,
        "small cache moves more bytes than object size allows: {} vs {} (dataset objects ~{})",
        small.bytes,
        big.bytes,
        big.full_gets, // labelled below in the message we print
    );
}
