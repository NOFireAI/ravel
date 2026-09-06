//! Acceptance tests for [`ravel_query::log_series::fetch_log_series`]
//! (ADR-1103, issue #1106).
//!
//! Two RLOG objects with disjoint ts ranges and distinct stream identities
//! exercise: zero-GET ts-range pruning before any object touches the store,
//! per-phase (Plan vs Scan) GET accounting, stream-label derivation from
//! resource attributes, the `ravel_log_lines`/`ravel_log_bytes` value
//! contract, and the `max_series`/`max_samples` budgets. A `FaultStore` test
//! proves a store-level failure surfaces as `LogSeriesError::Fetch`, not a
//! panic or a silently wrong result.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::LabelMatcher;
use ravel_query::erasure::ErasurePredicate;
use ravel_query::log_series::{
    BODY_MATCHER_LABEL, LOG_BYTES_METRIC, LOG_LINES_METRIC, LogMetric, LogSeriesError,
    LogSeriesRequest, SEVERITY_LABEL, fetch_log_series,
};
use ravel_query::{ByteLimit, LogSegmentFetcher, PhaseAccounting, QueryPhase, RequestLimit};
use ravel_types::accounting::AccountedOp;
use ravel_types::{METRIC_NAME_LABEL, TenantHash, TimeRange};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// An [`ObjectStoreBackend`] wrapper that counts `get` calls, so a test can
/// prove ts-range pruning skipped an object without touching storage.
struct CountingStore {
    inner: Arc<MemoryStore>,
    gets: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<MemoryStore>) -> Self {
        CountingStore {
            inner,
            gets: AtomicU64::new(0),
        }
    }

    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
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

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// A record on the stream identified by `service.namespace`/`service.name`/
/// `service.instance.id` (so `stream_label_set`'s job/instance derivation has
/// something to derive), with the given severity and body.
fn record(
    namespace: &str,
    name: &str,
    instance: &str,
    ts: i64,
    severity: &str,
    body: &str,
) -> LogRecord {
    let resource = vec![
        (
            "service.namespace".to_string(),
            AttrValue::Str(namespace.to_string()),
        ),
        ("service.name".to_string(), AttrValue::Str(name.to_string())),
        (
            "service.instance.id".to_string(),
            AttrValue::Str(instance.to_string()),
        ),
    ];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: severity.into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Cuts a block every 3 records so a multi-block object exercises
/// `next_block` more than once, matching `log_fetcher.rs`'s own convention.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

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
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// Object A: stream `pay/api`/`i-1`, ts 100..=110, severities INFO (ts != 105)
/// and ERROR (ts == 105), body "boom" at ts 105 and "ok" elsewhere. Object B:
/// stream `pay/worker`/`i-2`, ts 1000..=1010, all INFO/"ok". Disjoint ts
/// ranges and distinct stream identities.
async fn two_objects(store: &MemoryStore) -> (SegmentRef, SegmentRef) {
    let a: Vec<LogRecord> = (100..=110)
        .map(|ts| {
            if ts == 105 {
                record("pay", "api", "i-1", ts, "ERROR", "boom")
            } else {
                record("pay", "api", "i-1", ts, "INFO", "ok")
            }
        })
        .collect();
    let b: Vec<LogRecord> = (1000..=1010)
        .map(|ts| record("pay", "worker", "i-2", ts, "INFO", "ok"))
        .collect();
    let ref_a = write_object(store, "logs/a.rlog", &a).await;
    let ref_b = write_object(store, "logs/b.rlog", &b).await;
    (ref_a, ref_b)
}

/// A record on an arbitrary resource, for fixtures needing more than the
/// fixed `service.namespace`/`service.name`/`service.instance.id` shape
/// [`record`] provides: a resource missing `service.instance.id`, an extra
/// resource attribute (`k8s.pod.name`), or a non-scalar attribute value
/// (`tags`, list-valued) that must be excluded from the derived label set
/// rather than merely renamed.
fn resource_record(
    resource: &[(&str, AttrValue)],
    ts: i64,
    severity: &str,
    body: &str,
    record_attrs: &[(&str, &str)],
) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = resource
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let attrs: Vec<(String, AttrValue)> = record_attrs
        .iter()
        .map(|(k, v)| (k.to_string(), AttrValue::Str(v.to_string())))
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: severity.into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn full_window() -> TimeRange {
    TimeRange {
        start_ns: 0,
        end_ns: 1_000,
    }
}

/// ADR-1103 decisions 2/3 fixture (issue #1106 Finding 3): two objects, four
/// streams.
///
/// Object A: `s1` (`service.name=api`, `k8s.pod.name=p1`) with 5 ERROR
/// records at ts 100-103 (ts 102 shared by two of them; ts 100 and 101 carry
/// `user.id=u1` and a body containing "timeout") and 3 INFO records at ts
/// 104-106, plus `s2` (`service.name=worker`) with 4 ERROR records at ts
/// 107-110.
///
/// Object B: `s1` continuation with 2 more ERROR records at ts 300-301, `s3`
/// (`service.namespace=pay`, `service.name=api`, so `job="pay/api"` --
/// distinct from `s1`'s `job="api"`) with 1 ERROR record at ts 302, and `s4`
/// (`s1`'s exact resource plus a list-valued `tags` attribute, so its label
/// set equals `s1`'s even though its stream id -- and hence STREAM_DIR entry
/// and record count -- differ) with 1 ERROR record at ts 303.
///
/// Every body has a known length; the 8 `job="api"`,`severity_text="ERROR"`
/// bodies (5 from `s1` in A, 2 from `s1` in B, 1 from `s4`) are
/// "timeout-one" (11), "timeout-two-longer" (18), "err-c" (5), "err-d" (5),
/// "solo-exact-body" (15, unique across the whole fixture), "b-one" (5),
/// "b-two" (5), and "s4-body-x" (9) -- summing to exactly 73.
async fn promql_over_logs_fixture(store: &MemoryStore) -> (SegmentRef, SegmentRef) {
    let s1 = [
        ("service.name", AttrValue::Str("api".to_string())),
        ("k8s.pod.name", AttrValue::Str("p1".to_string())),
    ];
    let s2 = [("service.name", AttrValue::Str("worker".to_string()))];
    let s3 = [
        ("service.namespace", AttrValue::Str("pay".to_string())),
        ("service.name", AttrValue::Str("api".to_string())),
    ];
    let s4 = [
        ("service.name", AttrValue::Str("api".to_string())),
        ("k8s.pod.name", AttrValue::Str("p1".to_string())),
        (
            "tags",
            AttrValue::List(vec![AttrValue::Str("x".to_string())]),
        ),
    ];

    let a = vec![
        resource_record(&s1, 100, "ERROR", "timeout-one", &[("user.id", "u1")]),
        resource_record(
            &s1,
            101,
            "ERROR",
            "timeout-two-longer",
            &[("user.id", "u1")],
        ),
        resource_record(&s1, 102, "ERROR", "err-c", &[]),
        resource_record(&s1, 102, "ERROR", "err-d", &[]),
        resource_record(&s1, 103, "ERROR", "solo-exact-body", &[]),
        resource_record(&s1, 104, "INFO", "info-1", &[]),
        resource_record(&s1, 105, "INFO", "info-2", &[]),
        resource_record(&s1, 106, "INFO", "info-3", &[]),
        resource_record(&s2, 107, "ERROR", "work-1", &[]),
        resource_record(&s2, 108, "ERROR", "work-2", &[]),
        resource_record(&s2, 109, "ERROR", "work-3", &[]),
        resource_record(&s2, 110, "ERROR", "work-4", &[]),
    ];
    let b = vec![
        resource_record(&s1, 300, "ERROR", "b-one", &[]),
        resource_record(&s1, 301, "ERROR", "b-two", &[]),
        resource_record(&s3, 302, "ERROR", "pay-err-1", &[]),
        resource_record(&s4, 303, "ERROR", "s4-body-x", &[]),
    ];

    let ref_a = write_object(store, "logs/fixture-a.rlog", &a).await;
    let ref_b = write_object(store, "logs/fixture-b.rlog", &b).await;
    (ref_a, ref_b)
}

fn lines_request<'a>(matchers: &'a [LabelMatcher], window: TimeRange) -> LogSeriesRequest<'a> {
    LogSeriesRequest {
        metric: LogMetric::Lines,
        matchers,
        window,
        erasure: &[],
        max_samples: 1_000,
        max_series: 1_000,
        max_bytes_scanned: ByteLimit::Unlimited,
        max_s3_requests: RequestLimit::Unlimited,
        deadline: None,
    }
}

/// Acceptance test: a window overlapping only object A, filtered to stream
/// `job="pay/api"`, on `ravel_log_lines`. Object B is pruned by ts range
/// before any GET (zero-GET pruning). Object A's Plan phase reads only its
/// footer and STREAM_DIR section (ADR-1103 decision 2: no BLOCKS byte), and
/// its Scan phase then reads the projected column set for its surviving
/// blocks; no segment is read twice. `with_block_range_threshold(0)` routes
/// this (small, below the default 512 KiB threshold) fixture through the
/// same ranged probe-then-section path a production object above the
/// threshold takes, the way `log_fetcher.rs`'s own tests exercise that path
/// on small fixtures (e.g. `with_block_range_threshold(0)` at line 5753 and
/// others in that file).
#[tokio::test]
async fn log_series_fetch_counts_lines_and_bytes_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>)
        .with_block_range_threshold(0)
        .with_suffix_len(300);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "pay/api"),
    ];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();

    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(out.segments_fetched, 1, "only object A overlaps the window");
    assert_eq!(out.segments_pruned, 1, "object B is pruned by ts range");
    assert_eq!(
        out.records_scanned, 11,
        "all 11 of object A's records match the stream"
    );

    assert_eq!(out.series.len(), 2, "one series per distinct severity_text");
    let info = out
        .series
        .iter()
        .find(|s| s.labels.get("severity_text") == Some("INFO"))
        .expect("INFO series");
    let error = out
        .series
        .iter()
        .find(|s| s.labels.get("severity_text") == Some("ERROR"))
        .expect("ERROR series");
    assert_eq!(info.samples.len(), 10);
    assert_eq!(error.samples.len(), 1);
    assert!(
        info.samples.iter().all(|s| s.value == 1.0),
        "ravel_log_lines value is always 1.0"
    );
    assert_eq!(error.samples[0].ts_ns, 105);
    assert_eq!(info.labels.get("job"), Some("pay/api"));
    assert_eq!(info.labels.get("instance"), Some("i-1"));
    assert_eq!(info.labels.get(METRIC_NAME_LABEL), Some(LOG_LINES_METRIC));

    // Object B was pruned by ts range before any GET. Object A's Plan phase
    // (footer probe, then a separate range GET for the front STREAM_DIR
    // section the probe's tail suffix does not cover) and Scan phase (its
    // own footer probe, then the BLOCKS range reads for the 4 blocks
    // `small_blocks()` cuts across 11 records) are each pinned exactly, so a
    // regression that adds or removes a GET on this path is caught by name.
    // `with_suffix_len(300)` fixes the probe window so these counts do not
    // depend on the object's incidental total size.
    let snap = accounting.snapshot();
    let plan_gets = snap.phase(QueryPhase::Plan).s3_requests(AccountedOp::Get);
    let scan_gets = snap.phase(QueryPhase::Scan).s3_requests(AccountedOp::Get);
    assert_eq!(
        counting.get_count(),
        plan_gets + scan_gets,
        "every GET this fetch issues is charged to exactly one of Plan or Scan"
    );
    assert_eq!(
        plan_gets, 2,
        "Plan: one footer probe GET, one STREAM_DIR section GET"
    );
    assert_eq!(
        scan_gets, 6,
        "Scan: one footer probe GET, plus one BLOCKS range GET per surviving block"
    );

    let plan_bytes = snap.phase(QueryPhase::Plan).total_s3_bytes();
    let scan_bytes = snap.phase(QueryPhase::Scan).total_s3_bytes();
    assert!(
        plan_bytes < scan_bytes,
        "Plan reads footer+STREAM_DIR only (no BLOCKS byte); Scan reads the \
         projected BLOCKS data too, so Plan must move strictly fewer bytes \
         (plan_bytes={plan_bytes}, scan_bytes={scan_bytes})"
    );
}

/// `ravel_log_bytes`: sample values are the record body's length, not `1.0`.
#[tokio::test]
async fn log_series_bytes_metric_values_are_body_lengths() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_BYTES_METRIC),
        LabelMatcher::equal("job", "pay/api"),
    ];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = LogSeriesRequest {
        metric: LogMetric::Bytes,
        ..lines_request(&matchers, window)
    };
    let accounting = PhaseAccounting::new();

    let out = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect("fetch_log_series");

    let error = out
        .series
        .iter()
        .find(|s| s.labels.get("severity_text") == Some("ERROR"))
        .expect("ERROR series");
    assert_eq!(error.samples[0].value, "boom".len() as f64);
    let info = out
        .series
        .iter()
        .find(|s| s.labels.get("severity_text") == Some("INFO"))
        .expect("INFO series");
    assert!(info.samples.iter().all(|s| s.value == "ok".len() as f64));
}

/// A window disjoint from both objects prunes both by ts range with zero
/// GETs at all: the cheapest possible outcome for a query outside any
/// segment's span.
#[tokio::test]
async fn log_series_window_outside_both_segments_issues_zero_gets() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);

    let matchers = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
    let window = TimeRange {
        start_ns: 5_000,
        end_ns: 6_000,
    };
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();

    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(out.segments_pruned, 2);
    assert_eq!(out.segments_fetched, 0);
    assert_eq!(out.records_scanned, 0);
    assert!(out.series.is_empty());
    assert_eq!(
        counting.get_count(),
        0,
        "both segments pruned by ts range before any GET"
    );
}

/// `max_series` trips as soon as a matching record would create a series
/// beyond the limit: with the two-severities fixture and `max_series = 1`,
/// the first-seen severity's series fits but the second's does not.
#[tokio::test]
async fn log_series_series_budget_trips_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = LogSeriesRequest {
        max_series: 1,
        ..lines_request(&matchers, window)
    };
    let accounting = PhaseAccounting::new();

    let err = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect_err("two severities exceed max_series=1");
    match err {
        LogSeriesError::SeriesExceeded { count, max } => {
            assert_eq!(count, 2);
            assert_eq!(max, 1);
        }
        other => panic!("expected SeriesExceeded, got {other:?}"),
    }
}

/// `max_samples` trips once the running record count exceeds the budget,
/// regardless of how many series it lands in.
#[tokio::test]
async fn log_series_samples_budget_trips_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = LogSeriesRequest {
        max_samples: 5,
        ..lines_request(&matchers, window)
    };
    let accounting = PhaseAccounting::new();

    let err = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect_err("11 records exceed max_samples=5");
    match err {
        LogSeriesError::SamplesExceeded { count, max } => {
            assert_eq!(count, 6);
            assert_eq!(max, 5);
        }
        other => panic!("expected SamplesExceeded, got {other:?}"),
    }
}

/// `max_s3_requests` trips at the exact request count `log_series_fetch_counts_lines_and_bytes_exactly`
/// pins for this same fixture and window (2 Plan GETs + 6 Scan GETs = 8 for
/// object A's one segment): `Bounded(7)` is one under that total, so the
/// check after the segment completes must fail with the exact count, never
/// silently truncate or round down to the budget.
#[tokio::test]
async fn log_series_request_budget_trips_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>)
        .with_block_range_threshold(0)
        .with_suffix_len(300);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "pay/api"),
    ];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = LogSeriesRequest {
        max_s3_requests: RequestLimit::Bounded(7),
        ..lines_request(&matchers, window)
    };
    let accounting = PhaseAccounting::new();

    let err = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect_err("8 requests exceed max_s3_requests=7");
    match err {
        LogSeriesError::RequestsExceeded { requests, max } => {
            assert_eq!(requests, 8);
            assert_eq!(max, 7);
        }
        other => panic!("expected RequestsExceeded, got {other:?}"),
    }
    assert_eq!(
        counting.get_count(),
        8,
        "accounted request count must equal the store's real GET count"
    );
}

/// A deadline already in the past is caught before the first segment's Plan
/// read, never mid-scan silently.
#[tokio::test]
async fn log_series_deadline_exceeded_before_any_fetch() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);

    let matchers = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = LogSeriesRequest {
        deadline: Some(Instant::now()),
        ..lines_request(&matchers, window)
    };
    let accounting = PhaseAccounting::new();

    let err = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect_err("deadline already elapsed");
    assert!(matches!(err, LogSeriesError::DeadlineExceeded));
    assert_eq!(
        counting.get_count(),
        0,
        "the deadline check runs before any GET"
    );
}

/// A store-level failure on the Plan-phase discovery GET surfaces as
/// `LogSeriesError::Fetch`, never a panic, and the fault fires exactly once.
#[tokio::test]
async fn log_series_store_fault_surfaces_as_fetch_error() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::Transient("injected".into()))
            .with_key_contains("a.rlog")
            .with_occurrence(Occurrence::Always),
    );
    let fault_store = Arc::new(FaultStore::new(mem, plan));
    let fetcher = LogSegmentFetcher::new(fault_store.clone() as Arc<dyn ObjectStoreBackend>);

    let matchers = [LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC)];
    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();

    let err = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect_err("injected store fault");
    assert!(
        matches!(err, LogSeriesError::Fetch(_)),
        "expected Fetch, got {err:?}"
    );
    assert_eq!(
        fault_store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Transient),
        1,
        "the fault must have fired exactly once"
    );
}

/// Finding 3: `s1` (object A + object B) and `s4` (object B) share an
/// identical label set (`s4`'s only difference, a list-valued `tags`
/// resource attribute, is excluded from the label set entirely), so a
/// `job="api"`,`severity_text="ERROR"` selector merges their samples into
/// one series spanning both segments -- not two series that happen to sort
/// adjacently. Flip `series.entry(key)` in `fetch_log_series` (log_series.rs)
/// to key on `record.stream_id` instead of the label-set bytes and this test
/// fails with `series.len() == 3` (s1-in-A, s1-in-B, s4 no longer merge).
#[tokio::test]
async fn log_series_merges_streams_with_identical_label_sets() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];
    let req = lines_request(&matchers, full_window());
    let accounting = PhaseAccounting::new();

    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(
        out.segments_fetched, 2,
        "both objects hold a job=api severity_text=ERROR stream"
    );
    assert_eq!(out.segments_pruned, 0);
    assert_eq!(
        out.series.len(),
        1,
        "s1 (A and B) and s4 (B) share a label set and merge into one series"
    );

    let series = &out.series[0];
    assert_eq!(
        series.samples.len(),
        8,
        "5 from s1 in A, 2 from s1 in B, 1 from s4 in B"
    );
    let mut tss: Vec<i64> = series.samples.iter().map(|s| s.ts_ns).collect();
    tss.sort_unstable();
    assert_eq!(tss, vec![100, 101, 102, 102, 103, 300, 301, 303]);

    let mut names: Vec<&str> = series.labels.iter().map(|l| l.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "__name__",
            "job",
            "k8s_pod_name",
            "otel_scope_name",
            "otel_scope_version",
            "severity_text",
        ],
        "no instance (never set), no tags (list-valued, excluded from the label set)"
    );
    assert_eq!(series.labels.get("job"), Some("api"));
    assert_eq!(series.labels.get("k8s_pod_name"), Some("p1"));
    assert_eq!(series.labels.get("instance"), None);
    assert_eq!(series.labels.get("tags"), None);
}

/// Finding 3: `ravel_log_bytes` over the same 8 records sums to exactly 73
/// (11 + 18 + 5 + 5 + 15 + 5 + 5 + 9, see [`promql_over_logs_fixture`]'s
/// doc). Flip `record.body.len() as f64` to `1.0` in `fetch_log_series` (the
/// `LogMetric::Bytes` arm) and this test fails with `sum == 8.0`.
#[tokio::test]
async fn log_series_bytes_metric_sums_all_body_lengths_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_BYTES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];
    let req = LogSeriesRequest {
        metric: LogMetric::Bytes,
        ..lines_request(&matchers, full_window())
    };
    let accounting = PhaseAccounting::new();

    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(out.series.len(), 1);
    assert_eq!(out.series[0].samples.len(), 8);
    let total: f64 = out.series[0].samples.iter().map(|s| s.value).sum();
    assert_eq!(
        total, 73.0,
        "sum of all 8 job=api severity=ERROR body lengths"
    );
}

/// Finding 3 (F2b's severity non-equality post-filter, applied here at
/// fixture scale): `severity_text=~"ERR.*"` matches all 8 ERROR records
/// (proving the regex postfilter runs, not just the equality fast path);
/// `severity_text!="ERROR"` matches `s1`'s 3 INFO records (`s4` carries none).
/// Flip the postfilter's `.all(...)` to `.any(...)` in `fetch_log_series` and
/// the `!=` case fails with `total == 11` (every record passes an `any` over
/// a single always-different-from-"ERROR"-once-negated matcher).
#[tokio::test]
async fn log_series_severity_regex_and_negation_counts() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::regex(SEVERITY_LABEL, "ERR.*").unwrap(),
    ];
    let req = lines_request(&matchers, full_window());
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");
    let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
    assert_eq!(
        total, 8,
        "severity_text=~\"ERR.*\" matches all 8 ERROR records"
    );

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::not_equal(SEVERITY_LABEL, "ERROR"),
    ];
    let req = lines_request(&matchers, full_window());
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");
    let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
    assert_eq!(
        total, 3,
        "severity_text!=\"ERROR\" matches s1's 3 INFO records"
    );
}

/// Finding 1 (F1), at fixture scale: `__body__=~".*timeout.*"` finds the two
/// `user.id=u1` records, `!~".*timeout.*"` finds the other 6, an exact match
/// on the fixture's one unique body finds 1, and an unanchored `"timeout"`
/// pattern (no wildcards) finds 0 -- proving `__body__` regexes are fully
/// anchored. Flip `body_matchers` (log_series.rs) back to filtering
/// `stream_matchers`'s already-`__body__`-excluded list and every assertion
/// here fails: `.all()` over an empty slice is vacuously true, so all four
/// become "matches everything" (8, 8, 8, 8).
#[tokio::test]
async fn log_series_body_matcher_counts_on_the_fixture() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    async fn count(
        fetcher: &LogSegmentFetcher,
        refs: &[SegmentRef],
        matchers: &[LabelMatcher],
    ) -> usize {
        let req = lines_request(matchers, full_window());
        let accounting = PhaseAccounting::new();
        let out = fetch_log_series(fetcher, TENANT, refs, &req, &accounting)
            .await
            .expect("fetch_log_series");
        out.series.iter().map(|s| s.samples.len()).sum()
    }

    let base = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];

    let mut m = base.to_vec();
    m.push(LabelMatcher::regex(BODY_MATCHER_LABEL, ".*timeout.*").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m).await,
        2,
        "__body__=~\".*timeout.*\""
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::not_regex(BODY_MATCHER_LABEL, ".*timeout.*").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m).await,
        6,
        "__body__!~\".*timeout.*\""
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::equal(BODY_MATCHER_LABEL, "solo-exact-body"));
    assert_eq!(
        count(&fetcher, &refs, &m).await,
        1,
        "__body__ exact match on a unique body"
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::regex(BODY_MATCHER_LABEL, "timeout").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m).await,
        0,
        "unanchored \"timeout\" pattern requires the whole body to equal it"
    );
}

/// Finding 3: a windowless erasure on `user.id=u1` drops both records that
/// carry it (ts 100 and 101), leaving 6 of the 8 `job=api` ERROR samples, and
/// the `ravel_log_bytes` sum drops by exactly their two lengths (73 - 11 -
/// 18 = 44). A predicate windowed to `[101, 102)` erases only the ts=101
/// record, leaving 7. Flip `retain_log_records`'s `!p.has_window() ||
/// p.ts_in_window(...)` to `&&` in erasure.rs and the windowless case fails
/// (a predicate with no window has `has_window() == false`, so `&&` makes
/// its match always false and nothing is erased: `total == 8`).
#[tokio::test]
async fn log_series_erasure_excludes_matching_records_and_their_bytes() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];

    let windowless = [ErasurePredicate::windowless(vec![(
        "user.id".to_string(),
        "u1".to_string(),
    )])];
    let req = LogSeriesRequest {
        erasure: &windowless,
        ..lines_request(&matchers, full_window())
    };
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");
    let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
    assert_eq!(total, 6, "windowless erasure drops both user.id=u1 records");

    let bytes_matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_BYTES_METRIC),
        LabelMatcher::equal("job", "api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];
    let bytes_req = LogSeriesRequest {
        metric: LogMetric::Bytes,
        erasure: &windowless,
        ..lines_request(&bytes_matchers, full_window())
    };
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &bytes_req, &accounting)
        .await
        .expect("fetch_log_series");
    let sum: f64 = out
        .series
        .iter()
        .flat_map(|s| &s.samples)
        .map(|s| s.value)
        .sum();
    assert_eq!(
        sum, 44.0,
        "73 total minus \"timeout-one\" (11) and \"timeout-two-longer\" (18)"
    );

    let windowed = [ErasurePredicate::new(
        vec![("user.id".to_string(), "u1".to_string())],
        101,
        102,
    )];
    let req = LogSeriesRequest {
        erasure: &windowed,
        ..lines_request(&matchers, full_window())
    };
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");
    let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
    assert_eq!(
        total, 7,
        "the [101, 102) window excludes ts=100, so only the ts=101 record is erased"
    );
}

/// Finding 3: `job=~"api|worker"` selects `s1`+`s4` (merged, `job="api"`)
/// and `s2` (`job="worker"`) -- 2 series. `s3` (`job="pay/api"`) does not
/// match `"api|worker"` and is excluded. With `max_series: 1` the second
/// distinct series trips `SeriesExceeded`. Flip the anchoring `Self::compiled`
/// applies to a regex pattern (drop the `^(?:...)$` wrap in
/// `ravel-promql/src/source.rs`) and `s3` would also match unanchored
/// `"api|worker"` (its job value contains neither as a full match, so this
/// particular flip does not change this test's count, but the exact-count
/// assertion here is what catches a regression that widened the match set).
#[tokio::test]
async fn log_series_two_jobs_two_series_and_max_series_trips() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::regex("job", "api|worker").unwrap(),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];
    let req = lines_request(&matchers, full_window());
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");
    assert_eq!(
        out.series.len(),
        2,
        "job=api (s1+s4 merged) and job=worker (s2)"
    );

    let req = LogSeriesRequest {
        max_series: 1,
        ..lines_request(&matchers, full_window())
    };
    let accounting = PhaseAccounting::new();
    let err = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect_err("two distinct job series exceed max_series=1");
    match err {
        LogSeriesError::SeriesExceeded { count, max } => {
            assert_eq!(count, 2);
            assert_eq!(max, 1);
        }
        other => panic!("expected SeriesExceeded, got {other:?}"),
    }
}

/// Finding 3 (F2b's no-matching-stream segment prune): `job="nomatch"`
/// matches no stream in either object, so both are pruned after Plan-phase
/// STREAM_DIR discovery and neither reaches Scan. Every GET this fetch
/// issues is therefore a Plan-phase STREAM_DIR read (one whole-object GET
/// per object, both fixture objects being under the default
/// `block_range_threshold`) and Scan issues none. Flip the `if
/// matching_streams.is_empty() { segments_pruned += 1; continue; }` guard in
/// `fetch_log_series` to a no-op and this test fails: `scan_gets` becomes
/// nonzero (both objects proceed to Scan despite matching no stream).
#[tokio::test]
async fn log_series_nomatch_job_prunes_both_segments_with_zero_scan_gets() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "nomatch"),
    ];
    let req = lines_request(&matchers, full_window());
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert!(out.series.is_empty());
    assert_eq!(
        out.segments_pruned, 2,
        "no stream in either object has job=nomatch"
    );
    assert_eq!(out.segments_fetched, 0);

    let snap = accounting.snapshot();
    let plan_gets = snap.phase(QueryPhase::Plan).s3_requests(AccountedOp::Get);
    let scan_gets = snap.phase(QueryPhase::Scan).s3_requests(AccountedOp::Get);
    assert_eq!(
        scan_gets, 0,
        "Scan phase never runs: both segments pruned in Plan"
    );
    assert_eq!(
        plan_gets, 2,
        "one whole-object GET per object for STREAM_DIR discovery"
    );
    assert_eq!(
        counting.get_count(),
        plan_gets,
        "every GET this fetch issues is a Plan-phase STREAM_DIR read"
    );
}

/// Finding 3: a stream matcher on a label no series carries follows PromQL's
/// absent-as-empty-string rule (`ravel-promql/src/matchers.rs`):
/// `instance!=""` excludes every `job=api` series (absent reads as `""`, and
/// `"" != ""` is false), while `instance=""` and `instance!~".+"` keep all 11
/// `job=api` samples. Flip `LabelMatcher::is_match`'s `labels.get(&self.name)
/// .unwrap_or("")` to `.unwrap_or("<absent>")` and the `instance=""` case
/// fails (`"<absent>" == ""` is false, so it would exclude everything
/// instead of keeping it).
#[tokio::test]
async fn log_series_absent_label_matcher_follows_promql_empty_string_rule() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a, ref_b];

    let base = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
    ];

    for (extra, expect_total, label) in [
        (LabelMatcher::not_equal("instance", ""), 0, "instance!=\"\""),
        (LabelMatcher::equal("instance", ""), 11, "instance=\"\""),
        (
            LabelMatcher::not_regex("instance", ".+").unwrap(),
            11,
            "instance!~\".+\"",
        ),
    ] {
        let mut matchers = base.to_vec();
        matchers.push(extra);
        let req = lines_request(&matchers, full_window());
        let accounting = PhaseAccounting::new();
        let out = fetch_log_series(&fetcher, TENANT, &refs, &req, &accounting)
            .await
            .expect("fetch_log_series");
        let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
        assert_eq!(
            total, expect_total,
            "{label}: instance is absent on every job=api series"
        );
    }
}

/// Finding 3: a window covering only object A's ts range fetches exactly one
/// segment.
#[tokio::test]
async fn log_series_window_covering_only_object_a_fetches_one_segment() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = promql_over_logs_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "api"),
    ];
    let window = TimeRange {
        start_ns: 0,
        end_ns: 200,
    };
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(out.segments_fetched, 1, "only object A overlaps [0, 200]");
}

/// Finding 3 (CodeRabbit review on PR #1158, #1106, log_series.rs:370): two
/// `severity_text` equality matchers must conjoin like every other pair of
/// matchers in a PromQL selector -- `severity_text="ERROR"` AND
/// `severity_text="INFO"` can never both hold for one record, so the
/// selector must return zero series, not every ERROR record. A second case
/// with two IDENTICAL equalities must behave exactly like a single one.
///
/// Object A ([`two_objects`]) has exactly one ERROR record (ts=105) among 10
/// INFO records on stream `job="pay/api"`. The scan pushes down the first
/// `severity_text` equality (`"ERROR"`) as a content predicate, so only that
/// one record reaches the per-record loop; `severity_post` then evaluates
/// every other `severity_text` matcher against it.
///
/// Demonstrated failing against the pre-fix code: before the fix,
/// `severity_postfilters` excluded every `Eq`-op `severity_text` matcher
/// unconditionally (`m.op != MatchOp::Eq`, no index tracking), so the second
/// `severity_text="INFO"` matcher was dropped along with the pushed-down
/// first one instead of surviving as a post-filter. With no post-filter left
/// to reject it, the single scanned ERROR record was accepted unconditionally
/// and this test's first case returned 1 series / 1 sample instead of the
/// empty result PromQL conjunction requires. The fix (log_series.rs:385,
/// `severity_postfilters`) keeps every `severity_text` matcher except the one
/// at `pushed_index`, so the second `severity_text="INFO"` matcher survives
/// as a post-filter and rejects the ERROR record: `"ERROR" != "INFO"`.
#[tokio::test]
async fn log_series_conflicting_severity_equalities_return_no_samples() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, _ref_b) = two_objects(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let window = TimeRange {
        start_ns: 90,
        end_ns: 120,
    };

    // Conflicting: ERROR and INFO can never both hold for one record.
    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "pay/api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
        LabelMatcher::equal(SEVERITY_LABEL, "INFO"),
    ];
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(
        &fetcher,
        TENANT,
        std::slice::from_ref(&ref_a),
        &req,
        &accounting,
    )
    .await
    .expect("fetch_log_series");
    assert_eq!(
        out.series.len(),
        0,
        "severity_text=\"ERROR\" AND severity_text=\"INFO\" matches no record"
    );
    assert_eq!(
        out.records_scanned, 0,
        "the sole scanned ERROR record is rejected by the INFO post-filter"
    );

    // Identical: repeating the same equality changes nothing.
    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "pay/api"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
        LabelMatcher::equal(SEVERITY_LABEL, "ERROR"),
    ];
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &[ref_a], &req, &accounting)
        .await
        .expect("fetch_log_series");
    assert_eq!(
        out.series.len(),
        1,
        "two identical severity_text=\"ERROR\" equalities behave like one"
    );
    assert_eq!(out.series[0].samples.len(), 1);
    assert_eq!(out.series[0].samples[0].ts_ns, 105);
    assert_eq!(out.records_scanned, 1);
}

/// Finding 1 (F1), reproducing the review's exact bodies: `ok`, `ok`, `boom`,
/// `request timeout`. `__body__="ok"` finds the two exact matches,
/// `=~".*timeout.*"` finds the "request timeout" record, `!~".*timeout.*"`
/// finds the other three, and an unanchored `=~"timeout"` pattern (no
/// wildcards) finds none. Flip `body_matches`'s `.all(...)` to `.any(...)`
/// in log_series.rs and `!~".*timeout.*"` fails: `.any()` over one negated
/// matcher is the same predicate as `.all()` here (a single matcher), so the
/// flip that actually changes this test's outcome is the one already named
/// in `log_series_body_matcher_counts_on_the_fixture`'s doc (restoring the
/// `stream_matchers`-filtered list, which drops every `__body__` matcher and
/// makes all four counts here 4 instead of 2/1/3/0).
#[tokio::test]
async fn log_series_body_matcher_review_probe_ok_ok_boom_request_timeout() {
    let mem = Arc::new(MemoryStore::new());
    let resource = [("service.name", AttrValue::Str("bodyprobe".to_string()))];
    let records = vec![
        resource_record(&resource, 1, "INFO", "ok", &[]),
        resource_record(&resource, 2, "INFO", "ok", &[]),
        resource_record(&resource, 3, "INFO", "boom", &[]),
        resource_record(&resource, 4, "INFO", "request timeout", &[]),
    ];
    let ref_a = write_object(&mem, "logs/bodyprobe.rlog", &records).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);
    let refs = [ref_a];
    let window = TimeRange {
        start_ns: 0,
        end_ns: 10,
    };

    async fn count(
        fetcher: &LogSegmentFetcher,
        refs: &[SegmentRef],
        matchers: &[LabelMatcher],
        window: TimeRange,
    ) -> usize {
        let req = lines_request(matchers, window);
        let accounting = PhaseAccounting::new();
        let out = fetch_log_series(fetcher, TENANT, refs, &req, &accounting)
            .await
            .expect("fetch_log_series");
        out.series.iter().map(|s| s.samples.len()).sum()
    }

    let base = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "bodyprobe"),
    ];

    let mut m = base.to_vec();
    m.push(LabelMatcher::equal(BODY_MATCHER_LABEL, "ok"));
    assert_eq!(
        count(&fetcher, &refs, &m, window).await,
        2,
        "__body__=\"ok\""
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::regex(BODY_MATCHER_LABEL, ".*timeout.*").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m, window).await,
        1,
        "__body__=~\".*timeout.*\""
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::not_regex(BODY_MATCHER_LABEL, ".*timeout.*").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m, window).await,
        3,
        "__body__!~\".*timeout.*\""
    );

    let mut m = base.to_vec();
    m.push(LabelMatcher::regex(BODY_MATCHER_LABEL, "timeout").unwrap());
    assert_eq!(
        count(&fetcher, &refs, &m, window).await,
        0,
        "__body__=~\"timeout\" (unanchored) requires the whole body to equal it"
    );
}

/// Finding 2b (F3) item 4: `fetch_log_series`'s final per-series
/// `RlogWriter::finish` sorts every object's own rows by `(stream_ref, ts)`
/// (crates/ravel-logseg/src/writer.rs), so within one object the scan already
/// comes back ts-ascending regardless of push order: a single-object fixture
/// cannot exercise `fetch_log_series`'s own final
/// `samples.sort_by_key(|s| (s.ts_ns, s.value.to_bits()))`. Two objects for
/// the same stream can: each object is internally sorted, but the segment
/// loop pushes object A's records before object B's, so the per-series
/// `Vec<Sample>` accumulates [50, 60] then [10, 20] before the final sort
/// reorders it. Flip the sort key to a no-op and this test fails: the
/// samples come back in segment-scan order, [50, 60, 10, 20].
#[tokio::test]
async fn log_series_samples_come_back_ts_sorted_across_segments() {
    let mem = Arc::new(MemoryStore::new());
    let resource = [("service.name", AttrValue::Str("sortcheck".to_string()))];
    let later = vec![
        resource_record(&resource, 50, "INFO", "r1", &[]),
        resource_record(&resource, 60, "INFO", "r2", &[]),
    ];
    let earlier = vec![
        resource_record(&resource, 10, "INFO", "r3", &[]),
        resource_record(&resource, 20, "INFO", "r4", &[]),
    ];
    let ref_a = write_object(&mem, "logs/sort-a.rlog", &later).await;
    let ref_b = write_object(&mem, "logs/sort-b.rlog", &earlier).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
        LabelMatcher::equal("job", "sortcheck"),
    ];
    let window = TimeRange {
        start_ns: 0,
        end_ns: 100,
    };
    let req = lines_request(&matchers, window);
    let accounting = PhaseAccounting::new();
    let out = fetch_log_series(&fetcher, TENANT, &[ref_a, ref_b], &req, &accounting)
        .await
        .expect("fetch_log_series");

    assert_eq!(out.series.len(), 1);
    let tss: Vec<i64> = out.series[0].samples.iter().map(|s| s.ts_ns).collect();
    assert_eq!(
        tss,
        vec![10, 20, 50, 60],
        "ascending, even though segment A (later timestamps) is scanned before segment B"
    );
}

/// ADR-1103 decision 1: samples sharing a timestamp within a series order by
/// value bits ascending, never by scan/insertion order. Two `ravel_log_bytes`
/// records at the identical timestamp with different body lengths (so
/// different values), written in both orders, must come back in the same
/// ascending-by-value sequence [1.0, 5.0] regardless of which order they were
/// written and scanned in. Flip the final sort key from
/// `(s.ts_ns, s.value.to_bits())` back to bare `s.ts_ns` and the
/// long-then-short fixture returns [5.0, 1.0] instead, failing this test
/// (`sort_by_key` is stable, so a tie keeps push/scan order).
#[tokio::test]
async fn log_series_equal_timestamp_samples_order_by_value_bits() {
    let matchers = [
        LabelMatcher::equal(METRIC_NAME_LABEL, LOG_BYTES_METRIC),
        LabelMatcher::equal("job", "tiecheck"),
    ];
    let window = TimeRange {
        start_ns: 0,
        end_ns: 100,
    };

    let short_then_long = vec![
        resource_record(
            &[("service.name", AttrValue::Str("tiecheck".to_string()))],
            50,
            "INFO",
            "a",
            &[],
        ),
        resource_record(
            &[("service.name", AttrValue::Str("tiecheck".to_string()))],
            50,
            "INFO",
            "bbbbb",
            &[],
        ),
    ];
    let mem_a = Arc::new(MemoryStore::new());
    let ref_a = write_object(&mem_a, "logs/tie-a.rlog", &short_then_long).await;
    let fetcher_a = LogSegmentFetcher::new(mem_a as Arc<dyn ObjectStoreBackend>);
    let req_a = LogSeriesRequest {
        metric: LogMetric::Bytes,
        ..lines_request(&matchers, window)
    };
    let accounting_a = PhaseAccounting::new();
    let out_a = fetch_log_series(&fetcher_a, TENANT, &[ref_a], &req_a, &accounting_a)
        .await
        .expect("fetch_log_series");
    assert_eq!(out_a.series.len(), 1);
    let values_a: Vec<f64> = out_a.series[0].samples.iter().map(|s| s.value).collect();
    assert_eq!(
        values_a,
        vec![1.0, 5.0],
        "short body (1 byte) sorts before long body (5 bytes) at the shared timestamp"
    );

    let long_then_short = vec![
        resource_record(
            &[("service.name", AttrValue::Str("tiecheck".to_string()))],
            50,
            "INFO",
            "bbbbb",
            &[],
        ),
        resource_record(
            &[("service.name", AttrValue::Str("tiecheck".to_string()))],
            50,
            "INFO",
            "a",
            &[],
        ),
    ];
    let mem_b = Arc::new(MemoryStore::new());
    let ref_b = write_object(&mem_b, "logs/tie-b.rlog", &long_then_short).await;
    let fetcher_b = LogSegmentFetcher::new(mem_b as Arc<dyn ObjectStoreBackend>);
    let req_b = LogSeriesRequest {
        metric: LogMetric::Bytes,
        ..lines_request(&matchers, window)
    };
    let accounting_b = PhaseAccounting::new();
    let out_b = fetch_log_series(&fetcher_b, TENANT, &[ref_b], &req_b, &accounting_b)
        .await
        .expect("fetch_log_series");
    assert_eq!(out_b.series.len(), 1);
    let values_b: Vec<f64> = out_b.series[0].samples.iter().map(|s| s.value).collect();
    assert_eq!(
        values_b,
        vec![1.0, 5.0],
        "same ascending order even though the write/scan order was reversed"
    );
}

/// A single-stream, single-segment fixture for the `__body__` bloom-pruning
/// acceptance test (issue #1202): 12 INFO records at ts 1..=12, `small_blocks`
/// (`block_target_records: 3`) cutting them into blocks. Body at ts 4 is
/// "needle alpha found" (an exact-equality and an anchored-literal-regex
/// target); body at ts 7 is "needles-only" -- it contains "needle" as a
/// *substring* but tokenizes to the single token "needles", so no
/// `HasWord{word:"needle"}` bloom probe or `phrase_match` can match it. The
/// other ten bodies are ts-keyed fillers containing neither token.
async fn body_prune_fixture(store: &MemoryStore) -> SegmentRef {
    let records: Vec<LogRecord> = (1..=12)
        .map(|ts| {
            let body = match ts {
                4 => "needle alpha found".to_string(),
                7 => "needles-only".to_string(),
                _ => format!("filler-{ts}"),
            };
            record("prune", "check", "i-1", ts, "INFO", &body)
        })
        .collect();
    write_object(store, "logs/body-prune.rlog", &records).await
}

/// Issue #1202 acceptance test: `body_prune_literal` extracts a superset
/// literal from an extractable `__body__` matcher and pushes it as a
/// bloom-pruning `Predicate::HasWord`, so an equality or anchored-literal
/// regex matcher decodes only the one block holding its match, while a bare
/// `.*word.*` regex (no extractable literal) still decodes every block --
/// unchanged from before this change -- and both shapes return exactly the
/// same samples as the unpruned per-record filter alone would.
#[tokio::test]
async fn body_literal_prunes_blocks_without_changing_results() {
    let mem = Arc::new(MemoryStore::new());
    let seg = body_prune_fixture(&mem).await;
    let fetcher = LogSegmentFetcher::new(mem as Arc<dyn ObjectStoreBackend>);

    async fn run(
        fetcher: &LogSegmentFetcher,
        seg: &SegmentRef,
        body_matcher: LabelMatcher,
    ) -> ravel_query::log_series::LogSeriesOutput {
        let matchers = [
            LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
            body_matcher,
        ];
        let req = lines_request(&matchers, full_window());
        let accounting = PhaseAccounting::new();
        fetch_log_series(
            fetcher,
            TENANT,
            std::slice::from_ref(seg),
            &req,
            &accounting,
        )
        .await
        .expect("fetch_log_series")
    }

    // (ii) Exact equality: extracts the whole body as the literal, prunes to
    // the one block holding ts 4.
    let out = run(
        &fetcher,
        &seg,
        LabelMatcher::equal(BODY_MATCHER_LABEL, "needle alpha found"),
    )
    .await;
    assert_eq!(
        out.blocks_total, 4,
        "small_blocks cuts 12 records into 4 blocks of 3"
    );
    assert_eq!(
        out.blocks_scanned, 1,
        "the literal prunes to the one block holding ts 4"
    );
    let samples: Vec<_> = out.series.iter().flat_map(|s| s.samples.iter()).collect();
    assert_eq!(samples.len(), 1, "__body__=\"needle alpha found\"");
    assert_eq!(samples[0].ts_ns, 4);
    assert_eq!(samples[0].value, 1.0);

    // (iii) Anchored regex with no metacharacters: same literal, same prune.
    let out = run(
        &fetcher,
        &seg,
        LabelMatcher::regex(BODY_MATCHER_LABEL, "needle alpha found").unwrap(),
    )
    .await;
    assert_eq!(out.blocks_total, 4);
    assert_eq!(
        out.blocks_scanned, 1,
        "__body__=~\"needle alpha found\" extracts the whole (anchored) value, same as equality"
    );
    let samples: Vec<_> = out.series.iter().flat_map(|s| s.samples.iter()).collect();
    assert_eq!(samples.len(), 1, "__body__=~\"needle alpha found\"");
    assert_eq!(samples[0].ts_ns, 4);
    assert_eq!(samples[0].value, 1.0);

    // (iii-b) Two extractable `__body__` matchers on one selector: one
    // `Predicate::HasWord` arm is pushed per matcher, never collapsed, and the
    // conjunction of two supersets is still a superset of the conjunction. The
    // prune and the samples must be exactly what either arm alone produces.
    let out = {
        let matchers = [
            LabelMatcher::equal(METRIC_NAME_LABEL, LOG_LINES_METRIC),
            LabelMatcher::equal(BODY_MATCHER_LABEL, "needle alpha found"),
            LabelMatcher::regex(BODY_MATCHER_LABEL, "needle alpha found").unwrap(),
        ];
        let req = lines_request(&matchers, full_window());
        let accounting = PhaseAccounting::new();
        fetch_log_series(
            &fetcher,
            TENANT,
            std::slice::from_ref(&seg),
            &req,
            &accounting,
        )
        .await
        .expect("fetch_log_series")
    };
    assert_eq!(out.blocks_total, 4);
    assert_eq!(
        out.blocks_scanned, 1,
        "two extractable matchers push two arms and still prune to the one block holding ts 4"
    );
    let samples: Vec<_> = out.series.iter().flat_map(|s| s.samples.iter()).collect();
    assert_eq!(samples.len(), 1, "equality AND anchored regex on the body");
    assert_eq!(samples[0].ts_ns, 4);
    assert_eq!(samples[0].value, 1.0);

    // (iv) `.*needle.*` extracts no literal (no token-bounded run survives the
    // trim): every block is still decoded, and the per-record filter alone
    // finds both ts 4 ("needle alpha found") and ts 7 ("needles-only",
    // substring match, not a token match -- this is exactly why no literal
    // may be pushed here).
    let out = run(
        &fetcher,
        &seg,
        LabelMatcher::regex(BODY_MATCHER_LABEL, ".*needle.*").unwrap(),
    )
    .await;
    assert_eq!(out.blocks_total, 4);
    assert_eq!(
        out.blocks_scanned, 4,
        "no literal is extractable from .*needle.*, so no block is pruned"
    );
    let mut samples: Vec<_> = out
        .series
        .iter()
        .flat_map(|s| s.samples.iter())
        .map(|s| s.ts_ns)
        .collect();
    samples.sort_unstable();
    assert_eq!(samples, vec![4, 7], "__body__=~\".*needle.*\"");

    // Negated matcher: `body_prune_literal` must decline it (Ne/Nre always
    // None), so every one of the other 11 records still surfaces.
    let out = run(
        &fetcher,
        &seg,
        LabelMatcher::not_equal(BODY_MATCHER_LABEL, "needle alpha found"),
    )
    .await;
    assert_eq!(out.blocks_total, 4);
    assert_eq!(
        out.blocks_scanned, 4,
        "a negated matcher pushes no literal, so no block is pruned"
    );
    let total: usize = out.series.iter().map(|s| s.samples.len()).sum();
    assert_eq!(total, 11, "__body__!=\"needle alpha found\"");
}
