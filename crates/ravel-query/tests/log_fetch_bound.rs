//! Integration tests for ADR-0996 decision 2's fetch bound and decision 3's
//! `data_objects_touched` recorder, both in
//! [`ravel_query::LogSegmentFetcher`]/[`ravel_query::BlockRangeFetcher`].
//!
//! The bound test drives a covering read of an object larger than the bound and
//! pins the request-count band (`ceil(size / bound)` GETs), the per-request wire
//! bound ([`BlockRangeFetcher::peak_fetch_run_bytes`] <= bound), and row
//! identity with the unbounded whole-object path. The recorder test pins that
//! `plan_segment` records exactly one touch per distinct object, and that a
//! partition's subset scans do not double-count.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{BlockRangeFetcher, LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CONTENT_HASH: [u8; 32] = [9u8; 32];

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// One record per block, so an N-record object has N blocks.
fn one_record_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 1,
        ..RlogConfig::default()
    }
}

fn record(name: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![("service.name".to_string(), AttrValue::Str(name.to_string()))];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn build_object(records: &[LogRecord]) -> Vec<u8> {
    let mut w = RlogWriter::new(one_record_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

fn seg_ref(key: &str, size: u64, records: &[LogRecord]) -> SegmentRef {
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: CONTENT_HASH,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

/// Counts `get` calls and records the largest single GET length seen, so a test
/// can prove both the request count and that no single request exceeded the
/// bound.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
    max_get_len: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        CountingStore {
            inner,
            gets: AtomicU64::new(0),
            max_get_len: AtomicU64::new(0),
        }
    }
    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
    fn max_get_len(&self) -> u64 {
        self.max_get_len.load(Ordering::SeqCst)
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
        self.max_get_len
            .fetch_max(got.data.len() as u64, Ordering::SeqCst);
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

/// A 40-block object (ts 0..=39, one record per block).
fn big_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records: Vec<LogRecord> = (0..40)
        .map(|ts| {
            record(
                "api",
                ts,
                "a log line long enough to make the object multi-kilobyte",
            )
        })
        .collect();
    let bytes = build_object(&records);
    (records, bytes)
}

/// ADR-0996 decision 2's covering-read contract: an object above the fetch
/// bound is read as `ceil(size / bound)` sequential covering sub-range GETs,
/// each at most the bound, and the decoded rows are byte-identical to the
/// unbounded whole-object path.
///
/// Prove-the-test: raise `with_max_fetch_run_bytes` above `object_size` (or drop
/// the builder call, restoring the 64 MiB default) and the GET count collapses
/// to 1, failing the `ceil(size / bound)` assertion.
#[tokio::test]
async fn object_above_bound_is_read_in_banded_covering_gets_rows_identical() {
    let (records, bytes) = big_object();
    let object_size = bytes.len() as u64;

    // A bound that splits the object into several covering sub-ranges. Chosen so
    // the band is a clean, > 1 number the assertion can name.
    let bound = object_size / 5;
    assert!(bound > 0, "fixture object must exceed the bound five-fold");
    let expected_gets = object_size.div_ceil(bound);
    assert!(
        expected_gets >= 5,
        "the fixture must genuinely segment, got {expected_gets} sub-ranges"
    );

    // Reference: the unbounded whole-object path (one GetRange::Full).
    let ref_mem = Arc::new(MemoryStore::new());
    ref_mem
        .put("logs/b.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/b.rlog", object_size, &records);
    let query = LogQuery::new(0, 39);
    let reference = LogSegmentFetcher::new(ref_mem as Arc<dyn ObjectStoreBackend>)
        .fetch(&seg, &query)
        .await
        .expect("whole fetch")
        .expect("in range");
    assert!(!reference.records.is_empty(), "the object is nonempty");

    // Bounded: request-minimal routing (both thresholds saturated) sends every
    // object through the whole-object funnel, where the fetch bound segments the
    // covering read.
    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/b.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let fetcher = LogSegmentFetcher::new(store)
        .with_block_range_threshold(u64::MAX)
        .with_request_cost_bytes(u64::MAX)
        .with_max_fetch_run_bytes(bound);

    let bounded = fetcher
        .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("bounded fetch")
        .expect("in range");

    // Rows byte-identical to the unbounded path.
    assert_eq!(
        reference.records, bounded.records,
        "segmented covering read must decode byte-identical rows"
    );
    // Exactly ceil(size / bound) GETs, and no other read (no probe under
    // request-minimal).
    assert_eq!(
        counting.get_count(),
        expected_gets,
        "object above the bound is read in ceil(size/bound) covering GETs"
    );
    // No single request moved more than the bound: the per-request wire size is
    // bounded even though the assembled object buffer is not (the frozen
    // ravel-logseg reader needs a contiguous object-indexed buffer; see
    // covering_read's note).
    assert!(
        counting.max_get_len() <= bound,
        "no single GET exceeded the bound: {} > {bound}",
        counting.max_get_len()
    );
    assert!(
        fetcher.block_range_fetcher().peak_fetch_run_bytes() <= bound,
        "the fetcher's own peak covering sub-range is bounded: {} > {bound}",
        fetcher.block_range_fetcher().peak_fetch_run_bytes()
    );
}

/// At or under the bound the covering read is a single GET (the ADR's first
/// regime), byte for byte the unbounded behaviour.
#[tokio::test]
async fn object_at_or_under_bound_is_one_covering_get() {
    let (records, bytes) = big_object();
    let object_size = bytes.len() as u64;

    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/s.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    let seg = seg_ref("logs/s.rlog", object_size, &records);
    let query = LogQuery::new(0, 39);

    let fetcher = LogSegmentFetcher::new(store)
        .with_block_range_threshold(u64::MAX)
        .with_request_cost_bytes(u64::MAX)
        .with_max_fetch_run_bytes(object_size); // exactly at the bound

    let out = fetcher
        .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("fetch")
        .expect("in range");
    assert!(!out.records.is_empty());
    assert_eq!(
        counting.get_count(),
        1,
        "an object at the bound is one covering GET"
    );
}

/// ADR-0996 decision 2: a zero bound clamps up to one at the fetcher (the typed
/// refusal is at config resolution, `EngineConfig::validate`), so it never
/// divides by zero. A one-byte bound still reads the whole object, in a
/// (large) band of single-byte-ish covering GETs, byte-identical.
#[tokio::test]
async fn zero_bound_clamps_to_one_and_never_divides_by_zero() {
    let (records, bytes) = big_object();
    let object_size = bytes.len() as u64;
    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/z.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/z.rlog", object_size, &records);
    // A BlockRangeFetcher built with a zero bound: clamped to 1, so the covering
    // read segments into `object_size` single-byte GETs rather than panicking.
    let br = BlockRangeFetcher::new(mem as Arc<dyn ObjectStoreBackend>)
        .with_whole_object_threshold(u64::MAX)
        .with_max_fetch_run_bytes(0);
    let (assembled, _stats) = br
        .fetch_object(&seg, TENANT, 0, 39, &QueryAccounting::new())
        .await
        .expect("fetch with clamped bound");
    assert_eq!(assembled.len() as u64, object_size);
    assert!(br.peak_fetch_run_bytes() <= 1, "clamped bound is one byte");
}

/// ADR-0996 decision 3: `plan_segment` records exactly one
/// `data_objects_touched` per distinct relevant object, over however many GETs
/// the plan issues, and an irrelevant segment records none.
///
/// Prove-the-test: delete the `add_data_objects_touched(1)` line in
/// `plan_segment` and this asserts 0 touches instead of 3.
#[tokio::test]
async fn plan_segment_records_one_touch_per_distinct_object() {
    let mem = Arc::new(MemoryStore::new());
    let mut segs = Vec::new();
    for i in 0..3u32 {
        let records: Vec<LogRecord> = (0..4)
            .map(|ts| record("api", i as i64 * 100 + ts, "body"))
            .collect();
        let bytes = build_object(&records);
        let key = format!("logs/obj-{i}.rlog");
        mem.put(&key, bytes.clone().into(), PutOptions::default())
            .await
            .expect("put");
        segs.push(seg_ref(&key, bytes.len() as u64, &records));
    }
    // A fourth object entirely outside the query window: relevant-`None`, no
    // fetch, no touch.
    let far_records: Vec<LogRecord> = (0..4)
        .map(|ts| record("api", 1_000_000 + ts, "body"))
        .collect();
    let far_bytes = build_object(&far_records);
    mem.put(
        "logs/far.rlog",
        far_bytes.clone().into(),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let far_seg = seg_ref("logs/far.rlog", far_bytes.len() as u64, &far_records);

    let store: Arc<dyn ObjectStoreBackend> = mem;
    let fetcher = LogSegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    // The plan pass runs once per segment (ravel-sql shares it behind a barrier
    // before any partition drains); here we call it once per segment directly.
    let query = LogQuery::new(0, 100_000);
    for seg in &segs {
        fetcher
            .plan_segment(seg, TENANT, &query, &accounting)
            .await
            .expect("plan");
    }
    let touched = fetcher
        .plan_segment(&far_seg, TENANT, &query, &accounting)
        .await
        .expect("plan far");
    assert!(
        touched.is_none(),
        "the far segment is irrelevant, not touched"
    );

    assert_eq!(
        accounting.snapshot().data_objects_touched,
        3,
        "three distinct relevant objects record exactly three touches"
    );
}

/// ADR-0996 decision 2's combined routing test: under request-minimal, with an
/// explicitly set (low) block-range threshold, the ranged path is never chosen
/// -- ranged opens == 0. The routing decision is `ranged_projection_pays`
/// (consumed by ravel-sql's `open_by_column_chunk`, which is what records a
/// ranged open); request-minimal resolves the block-range threshold to
/// `u64::MAX`, so no object can select the ranged path.
///
/// The full opens-counter differential lives in ravel-sql
/// (`logs_request_cost_knob_routing.rs`, task 996-6); this pins the ravel-query
/// routing predicate the counter is downstream of.
///
/// Prove-the-test: the counterfactual below keeps the low explicit threshold
/// WITHOUT request-minimal's override, and `ranged_projection_pays` returns
/// true -- exactly the ranged open the override exists to suppress.
#[tokio::test]
async fn request_minimal_overrides_explicit_block_range_threshold_no_ranged_open() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let object_size = 4 * 1024 * 1024; // 4 MiB, far above any small explicit threshold
    let narrow_projection = 0.1; // a narrow projection would normally pay to range

    // Request-minimal resolution overrides the explicit --logs-block-range-threshold
    // to u64::MAX (and saturates the request cost). `with_block_range_threshold`
    // pins the inner crossover to that same value, so no object qualifies.
    let request_minimal = LogSegmentFetcher::new(Arc::clone(&store))
        .with_block_range_threshold(u64::MAX)
        .with_request_cost_bytes(u64::MAX);
    assert!(
        !request_minimal.ranged_projection_pays(object_size, narrow_projection),
        "request-minimal must never choose the ranged path (ranged opens == 0)"
    );

    // Counterfactual (the flip): a low explicit threshold NOT overridden. The
    // inner crossover is pinned to 4 KiB, so a narrow projection of a 4 MiB
    // object saves far more than that and the ranged path IS chosen.
    let not_overridden = LogSegmentFetcher::new(store)
        .with_block_range_threshold(4096)
        .with_request_cost_bytes(u64::MAX);
    assert!(
        not_overridden.ranged_projection_pays(object_size, narrow_projection),
        "without the override a low explicit threshold still routes ranged"
    );
}

/// ADR-0996 decision 3: a striped multi-partition scan still records exactly one
/// touch per object. The plan runs once (the shared per-segment recorder) and
/// the per-partition subset scans that follow do NOT record touches, so N
/// partitions over one object leave the count at one.
#[tokio::test]
async fn striped_subset_scans_do_not_double_count_a_touch() {
    let records: Vec<LogRecord> = (0..8).map(|ts| record("api", ts, "body")).collect();
    let bytes = build_object(&records);
    let mem = Arc::new(MemoryStore::new());
    mem.put("logs/one.rlog", bytes.clone().into(), PutOptions::default())
        .await
        .expect("put");
    let seg = seg_ref("logs/one.rlog", bytes.len() as u64, &records);
    let store: Arc<dyn ObjectStoreBackend> = mem;
    let fetcher = LogSegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    let query = LogQuery::new(0, 7);

    // One plan pass records the single touch.
    fetcher
        .plan_segment(&seg, TENANT, &query, &accounting)
        .await
        .expect("plan");
    assert_eq!(accounting.snapshot().data_objects_touched, 1);

    // Several partitions each drain their own subset of the object's blocks.
    // These are not the designated recorder, so the count stays one.
    let columns = ravel_logseg::ColumnSelection::all();
    for indices in [vec![0usize], vec![1, 2], vec![3]] {
        let mut scan = fetcher
            .scan_accounted_with_tenant_subset(
                &seg,
                TENANT,
                &query,
                &columns,
                &indices,
                None,
                &accounting,
            )
            .await
            .expect("subset scan")
            .expect("in range");
        while scan.next_block().expect("block").is_some() {}
    }
    assert_eq!(
        accounting.snapshot().data_objects_touched,
        1,
        "per-partition subset scans must not re-record the touch"
    );
}
