//! Integration tests for issue #693 part 3 deliverable 2: a [`footer::LogFooter`]
//! read once by the plan phase ([`LogSegmentFetcher::plan_segment`]'s fast path)
//! is carried into each per-partition subset open so the open reuses it and skips
//! its own etag-establishing suffix probe.
//!
//! Two properties:
//!
//! 1. Footer carried -> the subset open issues NO suffix probe (the plan's is the
//!    only one); footer omitted -> it probes again. Measured on an un-cached
//!    fetcher so the probe is a real store GET either way (a cache would coalesce
//!    the second probe onto the first and hide the difference the way a
//!    non-evicting fixture always does; the point here is the request the fetcher
//!    ISSUES, not what a cache absorbs).
//! 2. The footer-carried open still pins the etag on its first live section/block
//!    GET, so an object replaced mid-sequence surfaces the typed
//!    [`LogFetchError::EtagChanged`] rather than assembling bytes from two object
//!    states -- fail-closed, never mixed rows.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::footer::{self, kind};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, ColumnSelection, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, Etag, GetOutcome, GetRange, ListPage, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CONTENT_HASH: [u8; 32] = [9u8; 32];
const KEY: &str = "logs/seg.rlog";
/// Records, one block each, so the object carries this many blocks.
const N: usize = 6;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("request {ts} ok"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn build_object() -> Vec<u8> {
    let mut w = RlogWriter::new(one_record_cfg(), identity());
    for ts in 0..N as i64 {
        w.push(record(ts)).expect("push");
    }
    w.finish().expect("finish")
}

fn one_record_cfg() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

/// A second key in the same tenant, for the carry-pairing test below.
const OTHER_KEY: &str = "logs/other.rlog";

/// The same `N` records as [`build_object`], written as ONE block instead of
/// `N`. Same tenant and same schema, so its bytes and [`build_object`]'s decode
/// under each other's `SegmentRef`; the block shape is what tells the two apart,
/// so reading the wrong one shows up as a row count rather than an error.
fn build_other_object() -> Vec<u8> {
    let cfg = RlogConfig {
        block_target_records: N,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(cfg, identity());
    for ts in 0..N as i64 {
        w.push(record(ts)).expect("push");
    }
    w.finish().expect("finish")
}

/// [`seg_ref`] for [`build_other_object`]: same tenant and window, different key.
fn other_seg_ref(size: u64) -> SegmentRef {
    SegmentRef {
        data_object_key: OTHER_KEY.to_string(),
        ..seg_ref(size)
    }
}

fn seg_ref(size: u64) -> SegmentRef {
    SegmentRef {
        data_object_key: KEY.to_string(),
        object_size: size,
        min_event_ts_ns: 0,
        max_event_ts_ns: (N - 1) as i64,
        ingest_hour_bucket: 0,
        sample_count: N as u64,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// Bytes after the BLOCKS section (SKIP_IDX/BLOOM/POSTINGS then footer/trailer):
/// a probe suffix of exactly this length covers the whole tail (footer parses
/// with no range chase) yet reaches no block byte, so the block reads are real
/// byte-range GETs.
fn tail_len(bytes: &[u8]) -> u64 {
    let f = footer::open(bytes).expect("footer");
    let b = f.section(kind::BLOCKS).expect("BLOCKS");
    bytes.len() as u64 - (b.offset + b.len)
}

/// Absolute end of the BLOCKS section: block GETs start below this, every tail
/// section (SKIP_IDX/BLOOM/POSTINGS) at or above it.
fn blocks_end(bytes: &[u8]) -> u64 {
    let f = footer::open(bytes).expect("footer");
    let b = f.section(kind::BLOCKS).expect("BLOCKS");
    b.offset + b.len
}

/// An un-cached fetcher forced onto the ranged path (threshold 0), with the probe
/// sized to the tail and the coverage crossover disabled, so a footer-carried
/// open genuinely range-fetches SKIP_IDX and the blocks rather than reading the
/// whole object.
fn ranged_fetcher(store: Arc<dyn ObjectStoreBackend>, tail: u64) -> LogSegmentFetcher {
    LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range(
            BlockRangeFetcher::new(store)
                .with_suffix_len(tail)
                .with_whole_object_threshold(0)
                .with_coverage_threshold(2.0)
                .with_coalesce_gap(0),
        )
        .with_block_range_threshold(0)
}

// ---- shape-counting store ------------------------------------------------

struct SuffixCountingStore {
    inner: Arc<MemoryStore>,
    suffix: AtomicU64,
}

impl SuffixCountingStore {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(SuffixCountingStore {
            inner,
            suffix: AtomicU64::new(0),
        })
    }
    fn suffix_gets(&self) -> u64 {
        self.suffix.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStoreBackend for SuffixCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if matches!(range, GetRange::Suffix(_)) {
            self.suffix.fetch_add(1, Ordering::SeqCst);
        }
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
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Swaps the etag of any `Range` GET that starts BELOW `blocks_end` (the front
/// directory sections and the candidate blocks), so the footer-carried open's
/// first live GET (SKIP_IDX, at or above `blocks_end`) pins one etag and a later
/// front-section/block GET reports a different one -- the mid-sequence
/// replacement the pin must catch.
struct EtagSwapStore {
    inner: Arc<MemoryStore>,
    blocks_end: u64,
}

#[async_trait]
impl ObjectStoreBackend for EtagSwapStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let mut got = self.inner.get(key, range).await?;
        if let GetRange::Range(start, _) = range
            && start < self.blocks_end
        {
            got.etag = Etag("swapped-mid-sequence".to_string());
        }
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

async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    store
        .put(KEY, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    store
}

/// Plan the segment (one suffix probe), then a footer-carried subset open issues
/// NONE, so the whole statement pays exactly one suffix probe -- the plan's.
/// Omitting the footer probes again, for two. Rows are identical either way.
#[tokio::test]
async fn footer_carried_subset_open_skips_the_probe() {
    let bytes = build_object();
    let total = bytes.len() as u64;
    let tail = tail_len(&bytes);
    let seg = seg_ref(total);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let indices: Vec<usize> = (0..N).collect();

    // Footer carried: plan probe (1) + subset open (0) = 1 total.
    let counting = SuffixCountingStore::new(store_with_object(bytes.clone()).await);
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let fetcher = ranged_fetcher(store, tail);
    let acc = QueryAccounting::new();
    let (survivors, _stats, footer, _whole_object) = fetcher
        .plan_segment(&seg, TENANT, &query, &acc)
        .await
        .expect("plan")
        .expect("relevant");
    assert_eq!(survivors, N, "every block survives the full window");
    let footer = footer.expect("the fast plan path carries a footer");
    assert_eq!(
        counting.suffix_gets(),
        1,
        "the plan phase issues exactly one suffix probe"
    );
    let mut scan = fetcher
        .scan_accounted_with_tenant_subset(
            &seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &indices,
            Some(&footer),
            None,
            &acc,
        )
        .await
        .expect("subset scan")
        .expect("relevant");
    let mut rows_with_footer = 0usize;
    while let Some(block) = scan.next_block().expect("decode") {
        rows_with_footer += block.len();
    }
    assert_eq!(
        counting.suffix_gets(),
        1,
        "a footer-carried open issues no suffix probe of its own: total stays 1"
    );

    // Footer omitted (the red control): the open probes again, for two total.
    let counting2 = SuffixCountingStore::new(store_with_object(bytes).await);
    let store2: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting2) as Arc<dyn ObjectStoreBackend>;
    let fetcher2 = ranged_fetcher(store2, tail);
    let acc2 = QueryAccounting::new();
    let _ = fetcher2
        .plan_segment(&seg, TENANT, &query, &acc2)
        .await
        .expect("plan")
        .expect("relevant");
    let mut scan2 = fetcher2
        .scan_accounted_with_tenant_subset(
            &seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &indices,
            None,
            None,
            &acc2,
        )
        .await
        .expect("subset scan")
        .expect("relevant");
    let mut rows_no_footer = 0usize;
    while let Some(block) = scan2.next_block().expect("decode") {
        rows_no_footer += block.len();
    }
    assert_eq!(
        counting2.suffix_gets(),
        2,
        "omitting the footer makes the open re-probe: plan probe + open probe"
    );
    assert_eq!(
        rows_with_footer, rows_no_footer,
        "carrying the footer changes the read shape, never the rows"
    );
    assert_eq!(rows_with_footer, N, "all blocks decoded");
}

/// A footer-carried open still fails closed on a mid-sequence object
/// replacement: its first live GET pins the etag, and a later GET reporting a
/// different one surfaces [`LogFetchError::EtagChanged`] rather than mixing
/// bytes from two object states.
#[tokio::test]
async fn footer_carried_open_still_catches_an_etag_change() {
    let bytes = build_object();
    let total = bytes.len() as u64;
    let tail = tail_len(&bytes);
    let end = blocks_end(&bytes);
    let seg = seg_ref(total);
    let query = LogQuery::new(i64::MIN, i64::MAX);
    let indices: Vec<usize> = (0..N).collect();

    // Plan on a clean store to obtain the footer.
    let base = store_with_object(bytes).await;
    let plan_fetcher = ranged_fetcher(Arc::clone(&base) as Arc<dyn ObjectStoreBackend>, tail);
    let acc = QueryAccounting::new();
    let (_survivors, _stats, footer, _whole_object) = plan_fetcher
        .plan_segment(&seg, TENANT, &query, &acc)
        .await
        .expect("plan")
        .expect("relevant");
    let footer = footer.expect("the fast plan path carries a footer");

    // Now the scan runs against a store that swaps the etag on the front-section
    // and block GETs, simulating a replacement after the SKIP_IDX GET pinned the
    // etag.
    let swap = Arc::new(EtagSwapStore {
        inner: base,
        blocks_end: end,
    });
    let store: Arc<dyn ObjectStoreBackend> = swap;
    let fetcher = ranged_fetcher(store, tail);
    let result = fetcher
        .scan_accounted_with_tenant_subset(
            &seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &indices,
            Some(&footer),
            None,
            &QueryAccounting::new(),
        )
        .await;
    let err = result.err().map(|e| e.to_string());
    assert!(
        matches!(&err, Some(m) if m.contains("etag changed")),
        "a footer-carried open must catch a mid-sequence etag change, got {err:?}"
    );
}

/// A [`CarriedWholeObject`] is bound to the object and tenant its plan read
/// fetched, and a read that supplies it for anything else is refused before the
/// bytes are decoded (issue #835 review).
///
/// The carry branch answers from the carried bytes without consulting the
/// supplied `SegmentRef` at all, so without the guard this is not an error at
/// all: both fixtures are valid RLOG objects in one tenant, so the wrong bytes
/// decode happily and the scan returns the wrong object's rows. That is what
/// the row-count assertion pins. The two objects hold the same `N` records in
/// different block shapes -- `N` blocks of one row against one block of `N` --
/// so selecting block 0 of the intended object yields `N` rows and block 0 of
/// the carried one yields exactly 1.
#[tokio::test]
async fn a_carried_whole_object_is_refused_for_another_segment_or_tenant() {
    let carried_bytes = build_object();
    let target_bytes = build_other_object();
    let carried_seg = seg_ref(carried_bytes.len() as u64);
    let target_seg = other_seg_ref(target_bytes.len() as u64);
    let query = LogQuery::new(i64::MIN, i64::MAX);

    let store = Arc::new(MemoryStore::new());
    for (key, bytes) in [
        (KEY, carried_bytes.clone()),
        (OTHER_KEY, target_bytes.clone()),
    ] {
        store
            .put(key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
    }
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>);
    let acc = QueryAccounting::new();

    // The carry under test, produced by a real plan read of `carried_seg`.
    let (_survivors, _stats, _footer, carry) = fetcher
        .plan_segment(&carried_seg, TENANT, &query, &acc)
        .await
        .expect("plan")
        .expect("relevant");
    let carry = carry.expect(
        "a below-threshold plan read carries its whole object; without one this test is vacuous",
    );

    // Wrong segment, right tenant. Both objects decode, so the guard is the only
    // thing standing between this call and the wrong object's rows.
    let refused = fetcher
        .scan_accounted_with_tenant_subset(
            &target_seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &[0],
            None,
            Some(carry.clone()),
            &acc,
        )
        .await;
    let err = match refused {
        Err(e) => e,
        Ok(ok) => {
            let mut leaked = 0usize;
            let mut s = ok.expect("relevant");
            while let Some(b) = s.next_block().expect("decode") {
                leaked += b.len();
            }
            panic!(
                "a carry from {KEY} was decoded for a read of {OTHER_KEY} and returned \
                 {leaked} row(s) of the wrong object instead of being refused"
            );
        }
    };
    assert!(
        matches!(
            &err,
            ravel_query::LogFetchError::CarryMismatch { key, carried_key, .. }
                if key == OTHER_KEY && carried_key == KEY
        ),
        "expected CarryMismatch naming both objects, got {err:?}"
    );

    // Right segment, wrong tenant. The key matches, so only the tenant check
    // can refuse this one.
    let other_tenant = TenantHash([8u8; 16]);
    let refused = fetcher
        .scan_accounted_with_tenant_subset(
            &carried_seg,
            other_tenant,
            &query,
            &ColumnSelection::all(),
            &[0],
            None,
            Some(carry.clone()),
            &acc,
        )
        .await;
    let Err(err) = refused else {
        panic!("a carry fetched for another tenant must be refused, not decoded");
    };
    assert!(
        matches!(
            &err,
            ravel_query::LogFetchError::CarryMismatch { tenant, carried_tenant, .. }
                if *tenant == other_tenant && *carried_tenant == TENANT
        ),
        "expected CarryMismatch naming both tenants, got {err:?}"
    );

    // The matching pairing still works, and the guard costs it nothing.
    let mut scan = fetcher
        .scan_accounted_with_tenant_subset(
            &carried_seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &[0],
            None,
            Some(carry),
            &acc,
        )
        .await
        .expect("the carry's own segment and tenant are accepted")
        .expect("relevant");
    let mut carried_rows = 0usize;
    while let Some(block) = scan.next_block().expect("decode") {
        carried_rows += block.len();
    }
    assert_eq!(
        carried_rows, 1,
        "block 0 of the carried object holds exactly one row"
    );

    // The row count the refused call would have returned had it decoded the
    // carried bytes, against what its own object actually holds. They differ,
    // so a dropped guard returns wrong rows rather than an error.
    let mut target_scan = fetcher
        .scan_accounted_with_tenant_subset(
            &target_seg,
            TENANT,
            &query,
            &ColumnSelection::all(),
            &[0],
            None,
            None,
            &acc,
        )
        .await
        .expect("target scan")
        .expect("relevant");
    let mut target_rows = 0usize;
    while let Some(block) = target_scan.next_block().expect("decode") {
        target_rows += block.len();
    }
    assert_eq!(
        target_rows, N,
        "block 0 of the intended object holds all N rows"
    );
    assert_ne!(
        carried_rows, target_rows,
        "the fixtures must disagree on row count, or the guard's absence would be invisible"
    );
}
