//! Integration tests for ADR-0996 decision 2's fetch bound and decision 3's
//! `data_objects_touched` recorder, both in
//! [`ravel_query::LogSegmentFetcher`]/[`ravel_query::BlockRangeFetcher`].
//!
//! The bound tests drive a covering read of an object larger than the bound and
//! pin the request-count band (`ceil(size / bound)` GETs), the per-request wire
//! bound ([`BlockRangeFetcher::peak_fetch_run_bytes`], which counts only ISSUED
//! reads), the refusal of a zero bound, and row identity with the unbounded
//! whole-object path. The recorder tests pin that `plan_segment` records exactly
//! one touch per distinct object whose blocks a scan will fetch, that a
//! probe-only plan records none, and that a partition's subset scans do not
//! double-count.
//!
//! The routing tests pin ADR-0996's outcome at the shipped default: a saturated
//! resolved rate saturates the routing threshold too, so `cost-based` at the
//! reference profile routes every object whole and the plan phase issues no
//! footer probe. They run against an object above the production 512 KiB routing
//! threshold, which is the only band where the ranged path and the
//! skip-decidable plan branch are reachable at all.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldSel, FieldType, LogRecord, Predicate, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{
    BlockRangeFetcher, DEFAULT_LOG_REQUEST_COST_BYTES, DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
    EngineConfig, EngineConfigError, LogQuery, LogSegmentFetcher, LogsFetchPolicy,
    ResolvedLogsFetch, resolve_logs_fetch,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::cost_profile::StoreCostProfile;
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
        declared_column_stats: Default::default(),
    }
}

/// Counts `get` calls, counts the suffix-range GETs among them (the ADR-0107
/// etag-establishing footer probe is the only read shape that uses
/// [`GetRange::Suffix`]), and records the largest single GET length seen. A test
/// can therefore prove the request count, the probe count, and that no single
/// request exceeded the fetch bound, all from real store traffic rather than
/// from a configured value.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
    suffix_gets: AtomicU64,
    max_get_len: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        CountingStore {
            inner,
            gets: AtomicU64::new(0),
            suffix_gets: AtomicU64::new(0),
            max_get_len: AtomicU64::new(0),
        }
    }
    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
    /// Footer probes: every probe site issues `GetRange::Suffix`, and nothing
    /// else does.
    fn probe_count(&self) -> u64 {
        self.suffix_gets.load(Ordering::SeqCst)
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
        if matches!(range, GetRange::Suffix(_)) {
            self.suffix_gets.fetch_add(1, Ordering::SeqCst);
        }
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

/// Deterministic pseudo-random hex. The above-threshold fixture must exceed
/// 512 KiB as STORED bytes, and the writer zstds every page, so a repeated body
/// would compress away to nothing and leave the object below the routing
/// threshold the test is about.
fn noise_body(seed: u64, len: usize) -> String {
    const A: u64 = 6_364_136_223_846_793_005;
    const C: u64 = 1_442_695_040_888_963_407;
    let mut x = seed.wrapping_mul(A).wrapping_add(C);
    let mut s = String::with_capacity(len + 16);
    while s.len() < len {
        x = x.wrapping_mul(A).wrapping_add(C);
        s.push_str(&format!("{x:016x}"));
    }
    s.truncate(len);
    s
}

/// A record carrying an i64 `code` attribute, which FIELD_DIR resolves to a
/// dynamic column and SKIP_IDX carries per-block numeric stats for. That is what
/// makes a `Predicate::NumRange` on `code` a skip-decidable prune arm.
fn coded_record(ts: i64, code: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: noise_body(ts as u64, 1_600),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("code".to_string(), AttrValue::I64(code))],
    }
}

/// An object comfortably above the production 512 KiB routing threshold
/// ([`DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`]), every record carrying `code = 500`.
/// Above that threshold `plan_segment`'s skip-decidable branch (#761) and the
/// ranged fetch path are reachable; below it neither is, which is why the older
/// fixtures in this file exercise only the whole-object fallback.
fn above_threshold_object() -> (Vec<LogRecord>, Vec<u8>) {
    let records: Vec<LogRecord> = (0..700i64).map(|ts| coded_record(ts, 500)).collect();
    let bytes = build_object(&records);
    // The tightest consumer is `cost_based_at_the_reference_profile_routes_whole_object`'s
    // 0.9x counterfactual (`ranged_projection_pays(size, 0.1)`): it needs the
    // projection's SAVED bytes, `size * (1.0 - 0.1)`, to exceed the routing
    // threshold, which is a strictly larger object than merely `size >
    // threshold`. Guard the exact bound that assertion depends on rather than the
    // looser one, so a smaller fixture fails here instead of there.
    assert!(
        bytes.len() as f64 * 0.9 > DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD as f64,
        "the fixture must clear the 0.9x counterfactual bound (size * 0.9 > {} \
         bytes), got {} bytes",
        DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        bytes.len()
    );
    (records, bytes)
}

/// A skip-decidable query (`plan_skip_decidable`: no content arm, no stream
/// filter, a nonempty all-`NumRange` prune) over `code`, whose ts window
/// overlaps the fixture without containing it, so the predicate-free fast path
/// cannot take over.
fn coded_query(code_min: i64, code_max: i64) -> LogQuery {
    LogQuery::new(0, 399).with_prune(Predicate::NumRange {
        field: FieldSel::Attr("code".into()),
        ty: FieldType::I64,
        min: Some(code_min as u64),
        max: Some(code_max as u64),
    })
}

/// Puts `bytes` at `key` behind a [`CountingStore`] and returns the seg ref, the
/// counter, and the store handle.
async fn counted_store(
    key: &str,
    bytes: &[u8],
    records: &[LogRecord],
) -> (SegmentRef, Arc<CountingStore>, Arc<dyn ObjectStoreBackend>) {
    let mem = Arc::new(MemoryStore::new());
    mem.put(
        key,
        bytes::Bytes::copy_from_slice(bytes),
        PutOptions::default(),
    )
    .await
    .expect("put");
    let counting = Arc::new(CountingStore::new(mem));
    let store: Arc<dyn ObjectStoreBackend> = counting.clone();
    (seg_ref(key, bytes.len() as u64, records), counting, store)
}

/// Builds a fetcher the way `ravel-server`'s `build_sql_state` does: the
/// resolved routing threshold and request cost are handed to the fetcher
/// unconditionally, so `with_block_range_threshold` pins the inner crossover to
/// whatever `resolve_logs_fetch` produced.
fn fetcher_from(
    resolved: &ResolvedLogsFetch,
    store: Arc<dyn ObjectStoreBackend>,
) -> LogSegmentFetcher {
    LogSegmentFetcher::new(store)
        .with_block_range_threshold(resolved.block_range_threshold)
        .with_request_cost_bytes(resolved.request_cost_bytes)
}

/// The resolution the shipped default produces: `cost-based` at the reference
/// (intra-region, free-byte) profile, with no explicit overrides.
fn cost_based_at_reference() -> ResolvedLogsFetch {
    resolve_logs_fetch(
        LogsFetchPolicy::CostBased,
        &StoreCostProfile::reference(),
        None,
        DEFAULT_LOG_REQUEST_COST_BYTES,
        DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        None,
    )
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
        .with_max_fetch_run_bytes(bound)
        .expect("a nonzero bound is accepted");

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
    // Exactly the bound, not merely at or under it: every sub-range except the
    // last is a full bound-length read, and all of them were ISSUED (no cache is
    // wired here), so the peak must have moved to the bound. `<= bound` alone
    // would also pass if the counter never moved at all.
    assert_eq!(
        fetcher.block_range_fetcher().peak_fetch_run_bytes(),
        bound,
        "the fetcher's own peak covering sub-range is exactly the bound"
    );
}

/// [`BlockRangeFetcher::peak_fetch_run_bytes`] counts reads this fetcher ISSUED.
/// A covering read served entirely from the cache crosses no network and moves
/// no wire bytes, so it must leave the peak at zero.
///
/// Prove-the-test: move `observe_fetch_run` back above the `cached_extent` call
/// in `covering_read` (either regime) and the second assertion reads
/// `object_size` instead of the first read's peak, because the cache-served
/// second fetch re-observes an extent it never requested.
#[tokio::test]
async fn a_cache_served_covering_read_does_not_move_the_peak() {
    let (records, bytes) = big_object();
    let object_size = bytes.len() as u64;

    let (seg, counting, store) = counted_store("logs/c.rlog", &bytes, &records).await;
    let acc = QueryAccounting::new();

    // One cache, shared by two fetchers, large enough to hold the whole object.
    // `with_whole_object_threshold(u64::MAX)` sends the object down
    // `covering_read`, which the bound then segments.
    let cache = Arc::new(Cache::new(CacheLimits::new(1 << 20, 1024, 1 << 20)));
    let bound = object_size / 5;
    assert!(bound > 0, "the fixture must segment");

    let warm = BlockRangeFetcher::new(Arc::clone(&store))
        .with_whole_object_threshold(u64::MAX)
        .with_max_fetch_run_bytes(bound)
        .expect("a nonzero bound is accepted")
        .with_cache(Arc::clone(&cache));
    assert_eq!(warm.peak_fetch_run_bytes(), 0, "nothing issued yet");
    warm.fetch_object(&seg, TENANT, 0, 39, &acc)
        .await
        .expect("first fetch");
    let issued = counting.get_count();
    assert_eq!(
        issued,
        object_size.div_ceil(bound),
        "the first pass issued every covering sub-range"
    );
    assert_eq!(
        warm.peak_fetch_run_bytes(),
        bound,
        "the issued sub-ranges set the peak to the bound"
    );

    // A second fetcher over the same cache: every covering sub-range is served
    // from cache, so it issues nothing and its own peak must stay zero.
    let cold = BlockRangeFetcher::new(store)
        .with_whole_object_threshold(u64::MAX)
        .with_max_fetch_run_bytes(bound)
        .expect("a nonzero bound is accepted")
        .with_cache(cache);
    cold.fetch_object(&seg, TENANT, 0, 39, &acc)
        .await
        .expect("cache-served fetch");
    assert_eq!(
        counting.get_count(),
        issued,
        "no new store GET: the second fetcher hit the shared cache"
    );
    assert_eq!(
        cold.peak_fetch_run_bytes(),
        0,
        "a covering read served from cache issued nothing, so the peak stays zero"
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
        .with_max_fetch_run_bytes(object_size) // exactly at the bound
        .expect("a nonzero bound is accepted");

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

/// ADR-0996 decision 2: a zero bound is REFUSED at the setter, with the same
/// typed error `EngineConfig::validate` returns. Clamping it up to one instead
/// would turn a misconfigured bound into a silent one-byte-per-GET read of every
/// object -- a bound that "works" while multiplying the request bill by the
/// object size. The two checks are complementary: `validate` guards the config
/// surface, the setter guards every direct builder call that never passes
/// through an `EngineConfig`.
///
/// Prove-the-test: restore `self.max_fetch_run_bytes = n.max(1)` in
/// `BlockRangeFetcher::with_max_fetch_run_bytes` and both `expect_err` calls
/// panic on an `Ok`.
#[test]
fn zero_bound_is_refused_by_the_setter_not_clamped() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    assert_eq!(
        BlockRangeFetcher::new(Arc::clone(&store))
            .with_max_fetch_run_bytes(0)
            .err(),
        Some(EngineConfigError::ZeroFetchBound),
        "a zero bound is refused, not clamped to one"
    );
    assert_eq!(
        LogSegmentFetcher::new(Arc::clone(&store))
            .with_max_fetch_run_bytes(0)
            .err(),
        Some(EngineConfigError::ZeroFetchBound),
        "the outer builder refuses it too"
    );
    // The config surface still refuses it, unchanged: the setter does not
    // replace that check.
    let cfg = EngineConfig {
        logs_max_fetch_run_bytes: 0,
        ..EngineConfig::default()
    };
    assert_eq!(cfg.validate(), Err(EngineConfigError::ZeroFetchBound));
    // One byte is a legal (absurd) bound and is accepted, so the refusal is
    // exactly of zero and not of "small".
    assert!(
        BlockRangeFetcher::new(store)
            .with_max_fetch_run_bytes(1)
            .is_ok()
    );
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

/// ADR-0996 decision 3 and the `data_objects_touched` contract in
/// `ravel_types::accounting`: "A footer or index probe does not count ... if the
/// query then decides to fetch no blocks from that object, the object was never
/// touched."
///
/// `plan_segment`'s skip-decidable branch (#761) reads the probe, SKIP_IDX and
/// FIELD_DIR and nothing else. When its prune arm eliminates every block the
/// scan that would have fetched blocks never runs (`owned_work` assigns a
/// zero-survivor segment to no partition), so this must record ZERO touches.
/// Counting it would inflate the denominator of `range_amplification` in the
/// flattering direction.
///
/// The fixture is above the 512 KiB routing threshold on purpose: at or below
/// it `plan_segment` takes the whole-object fallback and this branch never runs,
/// which is why the probe-count assertion below is part of the claim.
///
/// Prove-the-test: restore `if matches!(result, Ok(Some(_)))` as the recording
/// predicate in `plan_segment` and the `data_objects_touched, 0` assertion reads
/// 1.
#[tokio::test]
async fn a_zero_survivor_skip_decidable_plan_records_no_touch() {
    let (records, bytes) = above_threshold_object();
    let (seg, counting, store) = counted_store("logs/skip-zero.rlog", &bytes, &records).await;

    // The shipped byte-minimal routing threshold, so the object is above it and
    // the skip-decidable branch is the one that runs.
    let fetcher = LogSegmentFetcher::new(store);
    assert_eq!(
        fetcher.block_range_threshold(),
        DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
    );
    let accounting = QueryAccounting::new();

    // `code` is 500 on every record, so this arm is disjoint from every block's
    // numeric stat and prunes them all.
    let (survivors, _stats, footer) = fetcher
        .plan_segment(&seg, TENANT, &coded_query(9_000, 10_000), &accounting)
        .await
        .expect("plan")
        .expect("the segment is ts-relevant, so this is not the irrelevant None");
    assert_eq!(survivors, 0, "the prune arm eliminates every block");
    assert!(
        footer.is_some(),
        "the skip-decidable branch carries its footer forward; a None footer \
         would mean the whole-object fallback ran instead"
    );
    assert!(
        counting.probe_count() > 0,
        "the skip-decidable branch issued its suffix probe, so the branch under \
         test is the one that ran"
    );

    assert_eq!(
        accounting.snapshot().data_objects_touched,
        0,
        "a probe-only plan that fetched no block bytes is not a touch"
    );
}

/// The sibling of the test above, same fixture and same branch, with a prune arm
/// that keeps every block: survivors > 0, a scan follows, and the object is
/// touched exactly once.
#[tokio::test]
async fn a_surviving_skip_decidable_plan_records_exactly_one_touch() {
    let (records, bytes) = above_threshold_object();
    let (seg, counting, store) = counted_store("logs/skip-live.rlog", &bytes, &records).await;

    let fetcher = LogSegmentFetcher::new(store);
    let accounting = QueryAccounting::new();

    let (survivors, _stats, footer) = fetcher
        .plan_segment(&seg, TENANT, &coded_query(0, 1_000), &accounting)
        .await
        .expect("plan")
        .expect("relevant");
    assert!(survivors > 0, "the wide prune arm keeps blocks");
    assert!(
        footer.is_some(),
        "same skip-decidable branch as the sibling"
    );
    assert!(counting.probe_count() > 0, "same branch, same probe");

    assert_eq!(
        accounting.snapshot().data_objects_touched,
        1,
        "a plan whose survivors a scan will fetch touches the object exactly once"
    );
}

/// A content arm plus a prune arm over `code`. The content arm (`HasWord` on the
/// body) defeats `plan_skip_decidable`, so `plan_segment` cannot take the
/// skip-decidable branch and falls through to the ranged whole-object fallback
/// even above the routing threshold. The `NumRange` prune arm still drives which
/// blocks the ranged fetch resolves.
fn content_and_prune_query(code_min: i64, code_max: i64) -> LogQuery {
    LogQuery::new(0, 399)
        .with_content(Predicate::HasWord {
            field: FieldSel::Body,
            word: "deadbeef".into(),
        })
        .with_prune(Predicate::NumRange {
            field: FieldSel::Attr("code".into()),
            ty: FieldType::I64,
            min: Some(code_min as u64),
            max: Some(code_max as u64),
        })
}

/// ADR-0996 decision 3 and the `data_objects_touched` contract in
/// `ravel_types::accounting` ("if the query then decides to fetch no blocks from
/// that object, the object was never touched"), on the FALLBACK path this time.
///
/// A content arm defeats `plan_skip_decidable`, so an above-threshold object
/// takes `plan_segment`'s ranged whole-object fallback rather than the
/// skip-decidable branch. When a disjoint `NumRange` arm prunes every block, the
/// ranged fetch resolves ZERO candidate-block extents and moves no block byte:
/// it reads only the footer, SKIP_IDX, FIELD_DIR and front/tail sections. That is
/// a probe-only read, not a touch, so this must record ZERO -- counting it would
/// inflate `range_amplification`'s denominator in the flattering direction.
///
/// Prove-the-test: this is the residual the fix removes. Flip the recorder in
/// `plan_segment` back to `*survivors > 0 || footer.is_none()` (the `touched`
/// flag replaced it): the fallback hands a `None` footer, so `footer.is_none()`
/// is true and this reads 1 instead of 0. The fix keys on the blocks the fetch
/// actually resolved (`Some(0)` here) instead.
#[tokio::test]
async fn a_zero_candidate_fallback_plan_records_no_touch() {
    let (records, bytes) = above_threshold_object();
    let (seg, counting, store) = counted_store("logs/fallback-zero.rlog", &bytes, &records).await;

    // `with_block_range_threshold` pins the inner whole-object crossover to the
    // same value, so an object above it takes the TRUE ranged path rather than
    // the size-threshold whole-object crossover (which would read every block and
    // legitimately be a touch). This is the band the residual lives in.
    let fetcher = LogSegmentFetcher::new(store)
        .with_block_range_threshold(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD);
    assert_eq!(
        fetcher.block_range_threshold(),
        DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        "the object is above the routing threshold, so the ranged fallback runs"
    );
    let accounting = QueryAccounting::new();

    // `code` is 500 on every record, so this arm is disjoint from every block's
    // numeric stat and prunes them all; the content arm forces the fallback.
    let (survivors, _stats, footer) = fetcher
        .plan_segment(
            &seg,
            TENANT,
            &content_and_prune_query(9_000, 10_000),
            &accounting,
        )
        .await
        .expect("plan")
        .expect("the segment is ts-relevant, so this is not the irrelevant None");
    assert_eq!(
        survivors, 0,
        "the disjoint prune arm eliminates every block"
    );
    assert!(
        footer.is_none(),
        "the content arm defeats skip-decidable, so this is the ranged fallback, \
         whose footer is not carried forward"
    );
    assert!(
        counting.probe_count() > 0,
        "the ranged fallback issued its suffix probe, so the branch under test ran"
    );

    assert_eq!(
        accounting.snapshot().data_objects_touched,
        0,
        "a fallback that resolved zero candidate blocks fetched no block byte and \
         is not a touch"
    );
}

/// The sibling of the test above, same fixture and same fallback branch, with a
/// prune arm that keeps every block. The ranged fetch resolves one or more
/// candidate-block extents and decodes them, so the object is touched exactly
/// once -- even if row filtering (the content arm) then leaves zero survivors,
/// because the blocks were fetched and decoded before that decision.
#[tokio::test]
async fn a_block_decoding_fallback_plan_records_exactly_one_touch() {
    let (records, bytes) = above_threshold_object();
    let (seg, counting, store) = counted_store("logs/fallback-live.rlog", &bytes, &records).await;

    let fetcher = LogSegmentFetcher::new(store)
        .with_block_range_threshold(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD);
    let accounting = QueryAccounting::new();

    // `code = 500` on every record falls inside this arm, so every block is a
    // candidate and the ranged fetch resolves and decodes blocks.
    let (_survivors, _stats, footer) = fetcher
        .plan_segment(
            &seg,
            TENANT,
            &content_and_prune_query(0, 1_000),
            &accounting,
        )
        .await
        .expect("plan")
        .expect("relevant");
    assert!(
        footer.is_none(),
        "same content-armed fallback branch as the sibling"
    );
    assert!(counting.probe_count() > 0, "same branch, same probe");

    assert_eq!(
        accounting.snapshot().data_objects_touched,
        1,
        "a fallback that decoded one or more blocks touches the object exactly \
         once, regardless of surviving rows"
    );
}

/// ADR-0996 decision 2, the outcome the whole ADR is for: at the shipped default
/// (`cost-based` + the reference profile) a narrow projection of an object above
/// 512 KiB must route WHOLE-OBJECT, so ravel-sql records zero ranged opens.
///
/// The routing predicate is `ranged_projection_pays`, which compares the
/// projection's saved bytes against `BlockRangeFetcher::effective_whole_object_threshold`.
/// That accessor returns the value `with_block_range_threshold` pinned VERBATIM
/// when one was set, bypassing the `5 x request_cost` derivation and its floors
/// -- and `ravel-server` always sets it. So a resolution that saturates only the
/// rate and leaves the routing threshold at 512 KiB delivers none of this: the
/// object still routes ranged, and the plan phase still issues the probe and
/// section GETs the ADR exists to remove.
///
/// Prove-the-test: key the routing override on `matches!(policy,
/// LogsFetchPolicy::RequestMinimal)` alone (the pre-fix condition in
/// `resolve_logs_fetch`) and both halves fail -- `ranged_projection_pays` returns
/// true, and the fetch issues a probe plus section and chunk GETs instead of the
/// single covering GET asserted here.
#[tokio::test]
async fn cost_based_at_the_reference_profile_routes_whole_object() {
    let resolved = cost_based_at_reference();
    assert_eq!(resolved.request_cost_bytes, u64::MAX, "free bytes saturate");

    let (records, bytes) = above_threshold_object();
    let object_size = bytes.len() as u64;
    let (seg, counting, store) = counted_store("logs/cb.rlog", &bytes, &records).await;
    let fetcher = fetcher_from(&resolved, store);

    // The routing predicate ravel-sql's `open_by_column_chunk` consumes: a
    // one-column-out-of-many projection of this object must not pay to range.
    assert!(
        !fetcher.ranged_projection_pays(object_size, 0.1),
        "cost-based at the reference profile must never choose the ranged path"
    );

    // ... and the read that follows is one covering GET with no probe, which is
    // what makes `data_GETs_per_touched_object` 1.0.
    let accounting = QueryAccounting::new();
    fetcher
        .plan_segment(&seg, TENANT, &coded_query(0, 1_000), &accounting)
        .await
        .expect("plan")
        .expect("relevant");
    assert_eq!(
        counting.probe_count(),
        0,
        "no footer probe: the object never reaches the ranged path"
    );
    assert_eq!(
        counting.get_count(),
        1,
        "one whole-object GET for the whole plan phase"
    );

    // The counterfactual the fix removes: the same saturated rate with the
    // routing threshold left at its configured 512 KiB routes ranged.
    let unfixed: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let stale = LogSegmentFetcher::new(unfixed)
        .with_block_range_threshold(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD)
        .with_request_cost_bytes(u64::MAX);
    assert!(
        stale.ranged_projection_pays(object_size, 0.1),
        "with the threshold left at 512 KiB the saturated rate changes nothing"
    );
}

/// ADR-0996 decision 2's plan-probe claim, pinned as BEHAVIOUR rather than as a
/// configuration flag: under a saturated resolution the plan phase issues ZERO
/// footer probes, because `plan_segment`'s two probe-issuing branches both gate
/// on `object_size > block_range_threshold` and the saturated threshold closes
/// them. No separate suppression switch is needed or exists.
///
/// The byte-minimal arm is the flip: same object, same query, and the probe
/// count is nonzero there, so the two zero assertions are not vacuous.
#[tokio::test]
async fn a_saturated_resolution_issues_no_plan_footer_probe() {
    let (records, bytes) = above_threshold_object();
    let reference = StoreCostProfile::reference();
    let query = coded_query(0, 1_000);

    let policies = [
        (LogsFetchPolicy::RequestMinimal, 0u64),
        (LogsFetchPolicy::CostBased, 0),
    ];
    for (policy, expected_probes) in policies {
        let resolved = resolve_logs_fetch(
            policy,
            &reference,
            None,
            DEFAULT_LOG_REQUEST_COST_BYTES,
            DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        let key = format!("logs/probe-{}.rlog", policy.as_str());
        let (seg, counting, store) = counted_store(&key, &bytes, &records).await;
        let fetcher = fetcher_from(&resolved, store);
        fetcher
            .plan_segment(&seg, TENANT, &query, &QueryAccounting::new())
            .await
            .expect("plan")
            .expect("relevant");
        assert_eq!(
            counting.probe_count(),
            expected_probes,
            "{} must issue no plan-phase footer probe",
            policy.as_str()
        );
    }

    // byte-minimal keeps the 512 KiB threshold, so the same above-threshold
    // object DOES take a probing branch. This is the demonstration that the
    // zeros above are a real property of the saturated resolution.
    let bm = resolve_logs_fetch(
        LogsFetchPolicy::ByteMinimal,
        &reference,
        None,
        DEFAULT_LOG_REQUEST_COST_BYTES,
        DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        None,
    );
    let (seg, counting, store) = counted_store("logs/probe-bm.rlog", &bytes, &records).await;
    let fetcher = fetcher_from(&bm, store);
    fetcher
        .plan_segment(&seg, TENANT, &query, &QueryAccounting::new())
        .await
        .expect("plan")
        .expect("relevant");
    assert!(
        counting.probe_count() > 0,
        "byte-minimal probes an above-threshold object, so the zeros above are \
         not vacuous"
    );
}
