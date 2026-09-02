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
use ravel_query::log_series::{
    LOG_BYTES_METRIC, LOG_LINES_METRIC, LogMetric, LogSeriesError, LogSeriesRequest,
    fetch_log_series,
};
use ravel_query::{ByteLimit, LogSegmentFetcher, PhaseAccounting, QueryPhase};
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

fn lines_request<'a>(matchers: &'a [LabelMatcher], window: TimeRange) -> LogSeriesRequest<'a> {
    LogSeriesRequest {
        metric: LogMetric::Lines,
        matchers,
        window,
        erasure: &[],
        max_samples: 1_000,
        max_series: 1_000,
        max_bytes_scanned: ByteLimit::Unlimited,
        deadline: None,
    }
}

/// Acceptance test: a window overlapping only object A, filtered to stream
/// `job="pay/api"`, on `ravel_log_lines`. Object B is pruned by ts range
/// before any GET (zero-GET pruning); object A is fetched once for Plan-phase
/// stream discovery and once for the Scan-phase read (the documented
/// double-read, see `log_series.rs`'s module docs), and its 11 records land
/// in exactly two series (INFO x10, ERROR x1), each sample value `1.0`.
#[tokio::test]
async fn log_series_fetch_counts_lines_and_bytes_exactly() {
    let mem = Arc::new(MemoryStore::new());
    let (ref_a, ref_b) = two_objects(&mem).await;
    let counting = Arc::new(CountingStore::new(mem));
    let fetcher = LogSegmentFetcher::new(counting.clone() as Arc<dyn ObjectStoreBackend>);

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

    // Object B was pruned by ts range before any GET; object A cost exactly
    // two GETs (Plan-phase discovery, Scan-phase read).
    assert_eq!(
        counting.get_count(),
        2,
        "object B pruned pre-GET; object A read twice (Plan + Scan)"
    );
    let snap = accounting.snapshot();
    assert_eq!(
        snap.phase(QueryPhase::Plan).s3_requests(AccountedOp::Get),
        1,
        "Plan-phase discovery issues exactly one GET"
    );
    assert_eq!(
        snap.phase(QueryPhase::Scan).s3_requests(AccountedOp::Get),
        1,
        "Scan-phase read issues exactly one GET"
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
