//! A statement over two or more signal tables, run through the per-signal
//! resolve and plan of the HTTP statement path (issue #2910), the
//! table-qualified `min_commit_token` on that path (issue #2911), and the
//! one-table refusal the Flight SQL entry points keep.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Array, Float64Array, TimestampNanosecondArray};
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_object_store::fault::ScriptedFault;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule};
use ravel_object_store::instrument::InstrumentedStore;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{RequestBudgets, RequestLimit};
use ravel_rspan::{SpanRecord, StatusCode};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{CommitToken, Sample, Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

use crate::MinCommitToken;
use crate::SqlConfig;
use crate::error::{ErrorClass, MSG_UNSATISFIABLE, SqlError};
use crate::executor::tests::{
    empty_store, executor_over, executor_with, log_record, publish_log_records,
    publish_log_records_in_hour, publish_metric_samples, publish_spans, sql_request,
    wrap_fixture_store,
};
use crate::executor::{LiveAccounting, ParquetPlan, PinnedPlanInputs, SqlExecutor, SqlRequest};
use crate::output::QueryOutput;

/// A store that forwards every call to `inner` and records each request it
/// forwards, in arrival order, as `"<OP> <key or prefix>"`.
struct RecordingStore<S> {
    inner: S,
    log: Mutex<Vec<String>>,
}

impl<S> RecordingStore<S> {
    fn new(inner: S) -> Self {
        RecordingStore {
            inner,
            log: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, op: &str, key: &str) {
        self.log
            .lock()
            .expect("request log lock")
            .push(format!("{op} {key}"));
    }

    /// Every request recorded so far.
    fn requests(&self) -> Vec<String> {
        self.log.lock().expect("request log lock").clone()
    }

    /// The recorded requests whose key or prefix contains `needle`.
    fn requests_containing(&self, needle: &str) -> Vec<String> {
        self.requests()
            .into_iter()
            .filter(|request| request.contains(needle))
            .collect()
    }
}

#[async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for RecordingStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.record("PUT", key);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.record("GET", key);
        self.inner.get(key, range).await
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.record("GET", key);
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.record("GET", key);
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.record("HEAD", key);
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.record("PUT", key);
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.record("HEAD", key);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.record("LIST", prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.record("LIST", prefix);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.record("LIST", prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.record("DELETE", key);
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

type FixtureStore = Arc<InstrumentedStore<FaultStore<MemoryStore>>>;

/// `store` under a [`RecordingStore`], and an executor over it.
fn recorded(
    store: FixtureStore,
    config: SqlConfig,
) -> (Arc<RecordingStore<FixtureStore>>, SqlExecutor) {
    let recording = Arc::new(RecordingStore::new(store));
    let executor = executor_with(recording.clone(), config);
    (recording, executor)
}

fn samples(points: &[(i64, f64)]) -> Vec<Sample> {
    points
        .iter()
        .map(|&(ts_ns, value)| Sample { ts_ns, value })
        .collect()
}

fn logs_at(times: &[i64]) -> Vec<ravel_logseg::LogRecord> {
    times.iter().map(|&ts| log_record("checkout", ts)).collect()
}

fn i64_column(output: &QueryOutput, name: &str) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in output.batches() {
        let column = batch.column_by_name(name).expect("column");
        let array = column
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("a Timestamp(Nanosecond) column");
        values.extend(array.values().iter().copied());
    }
    values
}

fn f64_column(output: &QueryOutput, name: &str) -> Vec<f64> {
    let mut values = Vec::new();
    for batch in output.batches() {
        let column = batch.column_by_name(name).expect("column");
        let array = column
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a Float64 column");
        values.extend(array.values().iter().copied());
    }
    values
}

/// `(ts, value)` rows of a result, read off the named columns.
fn rows(output: &QueryOutput, ts: &str, value: &str) -> Vec<(i64, f64)> {
    i64_column(output, ts)
        .into_iter()
        .zip(f64_column(output, value))
        .collect()
}

const WINDOW: TimeRange = TimeRange {
    start_ns: 0,
    end_ns: 2_000,
};

/// The data objects a request log fetched, of the signal whose key segment
/// is `signal_dir` (`/m/` for metrics, `/l/` for logs).
fn data_gets(requests: &[String], signal_dir: &str) -> Vec<String> {
    let mut gets: Vec<String> = requests
        .iter()
        .filter(|r| r.starts_with("GET ") && r.contains(signal_dir) && r.ends_with(".rseg"))
        .cloned()
        .collect();
    gets.sort();
    gets.dedup();
    gets
}

/// The ingest hour every fixture segment is published into ends at 1 h; with
/// the catalog's 4,800 s seal margin it is sealed by a fold at 3 h.
const FOLD_NOW_NS: i64 = 3 * 3_600 * 1_000_000_000;

/// Folds each of `signals` at [`FOLD_NOW_NS`], sealing hour 0 so its segments
/// carry postings and fold-time statistics.
async fn fold_hour_zero(store: &FixtureStore, tenant_hash: TenantHash, signals: &[Signal]) {
    let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    for &signal in signals {
        let report = catalog
            .fold(&tenant_hash, signal, Uuid::new_v4(), FOLD_NOW_NS, &[], None)
            .await
            .expect("fold");
        assert_eq!(
            report.watermark_hour,
            Some(0),
            "test setup: the fold sealed hour 0 of {signal:?}"
        );
    }
}

/// The `__name__` prune a `samples JOIN logs` statement derives from its
/// `samples` scan applies to the metrics snapshot alone: the metrics snapshot
/// keeps only `x`'s segment, the logs snapshot keeps both of its segments, and
/// the rows are those of the same statement with the prune defeated.
#[tokio::test]
async fn join_filter_prunes_only_its_own_signal() {
    let tenant = TenantId::new("cross-signal-prune".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    let x_key =
        publish_metric_samples(&memory, &tenant, 1, "x", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_metric_samples(&memory, &tenant, 2, "y", samples(&[(0, 10.0), (100, 20.0)])).await;
    let l1 = publish_log_records(&memory, tenant_hash, Signal::Logs, 3, &logs_at(&[0, 100])).await;
    let l2 =
        publish_log_records(&memory, tenant_hash, Signal::Logs, 4, &logs_at(&[100, 200])).await;
    let store = wrap_fixture_store(memory);
    // Postings exist only for folded segments; without a fold nothing prunes.
    fold_hour_zero(&store, tenant_hash, &[Signal::Metrics, Signal::Logs]).await;

    const PRUNED: &str = "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts \
                          WHERE label(s.labels, '__name__') = 'x' ORDER BY s.ts, s.value";
    // The same filter, written so no equality on `__name__` reaches the
    // resolve: nothing is pruned, and the rows must not change.
    const UNPRUNED: &str = "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts \
                            WHERE concat(label(s.labels, '__name__'), '') = 'x' \
                            ORDER BY s.ts, s.value";

    let folded = |sql: &str| {
        let mut request = sql_request(sql, WINDOW);
        request.now_ns = FOLD_NOW_NS;
        request
    };
    let (pruned_log, pruned_executor) = recorded(store.clone(), SqlConfig::default());
    let pruned = pruned_executor
        .execute(tenant_hash, &folded(PRUNED))
        .await
        .expect("the pruned join runs");
    let (unpruned_log, unpruned_executor) = recorded(store, SqlConfig::default());
    let unpruned = unpruned_executor
        .execute(tenant_hash, &folded(UNPRUNED))
        .await
        .expect("the unpruned join runs");

    // Both snapshots together: x's one segment plus both logs segments.
    assert_eq!(pruned.stats.segments, 3);
    assert_eq!(unpruned.stats.segments, 4);
    // Per snapshot: the only metrics object the pruned run fetched is x's, and
    // it fetched both logs objects, so the three resolved segments are one
    // metrics and two logs.
    let pruned_requests = pruned_log.requests();
    assert_eq!(
        data_gets(&pruned_requests, "/m/"),
        vec![format!("GET {x_key}")]
    );
    let mut logs_keys = vec![format!("GET {l1}"), format!("GET {l2}")];
    logs_keys.sort();
    assert_eq!(data_gets(&pruned_requests, "/l/"), logs_keys);
    assert_eq!(data_gets(&unpruned_log.requests(), "/l/"), logs_keys);

    let expected = vec![(0, 1.0), (100, 2.0), (100, 2.0)];
    assert_eq!(rows(&pruned.output, "ts", "value"), expected);
    assert_eq!(rows(&unpruned.output, "ts", "value"), expected);

    // `segmentsPruned` is the metrics catalog's prune of y's segment, on the
    // samples entry alone: it accounts for the segment the samples entry lost
    // against the unpruned run.
    let pruned_counts: Vec<(&str, usize, u64)> = pruned
        .signals
        .iter()
        .map(|s| (s.table, s.segments, s.segments_pruned))
        .collect();
    assert_eq!(pruned_counts, [("samples", 1, 1), ("logs", 2, 0)]);
    let unpruned_counts: Vec<(&str, usize, u64)> = unpruned
        .signals
        .iter()
        .map(|s| (s.table, s.segments, s.segments_pruned))
        .collect();
    assert_eq!(unpruned_counts, [("samples", 2, 0), ("logs", 2, 0)]);
    assert_eq!(
        pruned.signals[0].segments as u64 + pruned.signals[0].segments_pruned,
        unpruned.signals[0].segments as u64
    );
}

/// The row window filters each scan on its own table's event-time column:
/// `spans` on `start_ts`, `logs` on `ts`. The spans rows kept are inside the
/// window by `start_ts` and outside it by `end_ts`, so a window on `end_ts`
/// would drop them; the reported predicate holds one entry per table, `logs`
/// first.
#[tokio::test]
async fn row_window_uses_each_tables_time_column() {
    let tenant = TenantId::new("cross-signal-row-window".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    let span = |start_ts_ns: i64, end_ts_ns: i64, id: u8| SpanRecord {
        trace_id: [1u8; 16],
        span_id: [id; 8],
        parent_span_id: None,
        name: "query".to_string(),
        start_ts_ns,
        end_ts_ns,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    };
    publish_spans(
        &memory,
        tenant_hash,
        1,
        &[span(100, 300, 1), span(300, 600, 2), span(450, 900, 3)],
    )
    .await;
    publish_log_records(
        &memory,
        tenant_hash,
        Signal::Logs,
        2,
        &logs_at(&[100, 300, 450, 600]),
    )
    .await;
    let store = wrap_fixture_store(memory);
    let (_, executor) = recorded(store, SqlConfig::default());

    let mut request = sql_request(
        "SELECT sp.start_ts, sp.end_ts, l.ts FROM spans sp JOIN logs l ON sp.name = l.body \
         ORDER BY sp.start_ts, l.ts",
        WINDOW,
    );
    request.window = TimeRange {
        start_ns: 200,
        end_ns: 500,
    };
    request.row_window = true;
    let outcome = executor
        .execute(tenant_hash, &request)
        .await
        .expect("the windowed join runs");

    let found: Vec<(i64, i64, i64)> = i64_column(&outcome.output, "start_ts")
        .into_iter()
        .zip(i64_column(&outcome.output, "end_ts"))
        .zip(i64_column(&outcome.output, "ts"))
        .map(|((start, end), ts)| (start, end, ts))
        .collect();
    assert_eq!(
        found,
        vec![
            (300, 600, 300),
            (300, 600, 450),
            (450, 900, 300),
            (450, 900, 450)
        ]
    );
    assert_eq!(
        outcome.stats.window_predicate.as_deref(),
        Some(
            "logs.ts >= TimestampNanosecond(200, None) AND logs.ts < TimestampNanosecond(500, None); \
             spans.start_ts >= TimestampNanosecond(200, None) AND \
             spans.start_ts < TimestampNanosecond(500, None)"
        )
    );
}

/// A statement over one signal table resolves only that signal, with the
/// request sequence pinned literally: the per-signal resolve issues exactly
/// what the one-snapshot resolve did for the same statement.
#[tokio::test]
async fn single_signal_statement_issues_the_same_requests() {
    let tenant = TenantId::new("cross-signal-one-table".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &logs_at(&[0, 100])).await;
    let store = wrap_fixture_store(memory);

    let (samples_log, samples_executor) = recorded(store.clone(), SqlConfig::default());
    let samples_outcome = samples_executor
        .execute(
            tenant_hash,
            &sql_request("SELECT ts, value FROM samples", WINDOW),
        )
        .await
        .expect("the samples statement runs");
    let (logs_log, logs_executor) = recorded(store, SqlConfig::default());
    let logs_outcome = logs_executor
        .execute(
            tenant_hash,
            &sql_request("SELECT ts, body FROM logs", WINDOW),
        )
        .await
        .expect("the logs statement runs");

    let phases = |outcome: &crate::executor::SqlOutcome| {
        let p = &outcome.phase_accounting;
        [
            p.resolve.total_s3_requests(),
            p.plan.total_s3_requests(),
            p.probe.total_s3_requests(),
            p.scan.total_s3_requests(),
        ]
    };
    // Resolve: the catalog HEAD, the commit listing, the one commit record and
    // the deletion listing. The metrics data object is read while planning,
    // the logs one while scanning.
    assert_eq!(phases(&samples_outcome), [4, 1, 0, 0]);
    assert_eq!(phases(&logs_outcome), [4, 0, 0, 1]);
    assert_eq!(
        samples_log.requests(),
        [
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/catalog/m/HEAD",
            "LIST t/ad5de50a1b1f440769f2b2f06adc9a19/m/c/0000/",
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/m/c/0000/19700101T00/\
             00000000-0000-0000-0000-000000000560.1.00000000000000000001.cmt",
            "LIST t/ad5de50a1b1f440769f2b2f06adc9a19/m/del/",
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/m/l0/0000/\
             00000000-0000-0000-0000-000000000560.1.00000000000000000001.bd378b9b0b189f96.rseg",
        ]
    );
    assert_eq!(
        logs_log.requests(),
        [
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/catalog/l/HEAD",
            "LIST t/ad5de50a1b1f440769f2b2f06adc9a19/l/c/0000/",
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/l/c/0000/19700101T00/\
             00000000-0000-0000-0000-000000000560.1.00000000000000000002.cmt",
            "LIST t/ad5de50a1b1f440769f2b2f06adc9a19/l/del/",
            "GET t/ad5de50a1b1f440769f2b2f06adc9a19/l/l0/0000/\
             00000000-0000-0000-0000-000000000560.1.00000000000000000002.78e1ee457a3c08eb.rseg",
        ]
    );
}

/// A retried statement re-resolves every signal it names, and the retry's
/// result reads the second resolve of both: a segment of each signal is
/// published while the first attempt is held, and the result holds the rows of
/// both.
#[tokio::test]
async fn retry_resolves_every_signal_again() {
    let tenant = TenantId::new("cross-signal-retry".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    let first_samples =
        publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &logs_at(&[0, 100])).await;
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(first_samples.clone())
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store: FixtureStore = Arc::new(InstrumentedStore::new(FaultStore::new(memory, plan)));
    let gate = store
        .inner()
        .hold(Op::Get, Some(first_samples.clone()), Occurrence::Nth(1));
    let publisher = {
        let store = store.clone();
        let tenant = tenant.clone();
        tokio::spawn(async move {
            gate.wait_until_held(1).await;
            // Both lanes of the first attempt have resolved by now: the
            // segments below exist only for a resolve that starts later.
            let memory = store.inner().inner();
            publish_metric_samples(memory, &tenant, 3, "m", samples(&[(200, 3.0)])).await;
            publish_log_records(memory, tenant_hash, Signal::Logs, 4, &logs_at(&[200])).await;
            for id in gate.held() {
                gate.release(id);
            }
        })
    };

    let (log, executor) = recorded(store.clone(), SqlConfig::default());
    let outcome = executor
        .execute(
            tenant_hash,
            &sql_request(
                "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts ORDER BY s.ts",
                WINDOW,
            ),
        )
        .await
        .expect("the retry recovers the statement");
    publisher.await.expect("the publisher ran");

    assert_eq!(
        store.inner().fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound fired"
    );
    assert_eq!(outcome.stats.resolves, 2);
    assert_eq!(outcome.stats.attempts, 2);
    // Each attempt's resolve of a signal lists its commit keyspace once.
    let hex = tenant_hash.to_hex();
    for signal_dir in ["m", "l"] {
        let listing = format!("LIST t/{hex}/{signal_dir}/c/0000/");
        assert_eq!(
            log.requests_containing(&listing),
            vec![listing.clone(), listing.clone()],
            "both attempts resolve the {signal_dir} signal"
        );
    }
    assert_eq!(
        rows(&outcome.output, "ts", "value"),
        vec![(0, 1.0), (100, 2.0), (200, 3.0)]
    );
}

/// `max_segments` is checked against the sum of the segments every signal
/// admits: two folded `samples` segments and two folded `logs` segments are
/// refused under a ceiling of 3, which each signal alone is under, and run
/// under a ceiling of 4.
///
/// The request budget, by contrast, is checked after each signal's resolve
/// against what the statement's resolve has issued so far, and a refusal on a
/// later lane names that lane's table. An empty resolve costs 3 requests, so a
/// budget of 4 passes the `samples` lane and refuses on the `logs` lane at 6.
#[tokio::test]
async fn summed_max_segments_refuses_across_signals() {
    let tenant = TenantId::new("cross-signal-max-segments".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0)])).await;
    publish_metric_samples(&memory, &tenant, 2, "m", samples(&[(100, 2.0)])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 3, &logs_at(&[0])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 4, &logs_at(&[100])).await;
    let store = wrap_fixture_store(memory);
    fold_hour_zero(&store, tenant_hash, &[Signal::Metrics, Signal::Logs]).await;

    let sql = "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts ORDER BY s.ts";
    let mut request = sql_request(sql, WINDOW);
    request.now_ns = FOLD_NOW_NS;
    let with_max = |max_segments: usize| {
        let mut config = SqlConfig::default();
        config.engine.max_segments = max_segments;
        executor_with(store.clone(), config)
    };

    let err = with_max(3)
        .execute(tenant_hash, &request)
        .await
        .expect_err("four admitted segments exceed a ceiling of 3");
    assert!(
        matches!(err, SqlError::TooManySegments { count: 4, max: 3 }),
        "expected TooManySegments {{ count: 4, max: 3 }}, got {err:?}"
    );

    let outcome = with_max(4)
        .execute(tenant_hash, &request)
        .await
        .expect("four admitted segments fit a ceiling of 4");
    assert_eq!(outcome.stats.segments, 4);
    assert_eq!(i64_column(&outcome.output, "ts"), vec![0, 100]);

    let executor = executor_over(empty_store());
    let mut request = sql_request(
        "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts",
        WINDOW,
    );
    request.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(4)),
        ..Default::default()
    });
    let err = executor
        .execute(TenantHash([9u8; 16]), &request)
        .await
        .expect_err("the logs lane's resolve exceeds the budget");
    let message = err.to_string();
    let SqlError::RequestBudgetExceeded {
        requests: 6,
        max: 4,
        table: Some("logs"),
        ..
    } = err
    else {
        panic!("expected a logs-lane RequestBudgetExceeded at 6 of 4, got {err:?}");
    };
    assert!(
        message.ends_with(" (refused on table logs)"),
        "the message names the lane: {message}"
    );
}

/// A statement over two signal tables is never fanned out to workers, under
/// a configuration that does fan out the `samples`-only statement over the
/// same snapshot.
#[cfg(feature = "flight-sql")]
#[tokio::test]
async fn a_two_table_statement_never_distributes() {
    use crate::distributed::{DistributedFlightConfig, StaticWorkerEndpoints};
    use crate::flight_ticket::{FlightTicket, SegmentPin};
    use ravel_query::distrib::partition::DistribThresholds;

    let tenant = TenantId::new("cross-signal-distribute".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0)])).await;
    publish_metric_samples(&memory, &tenant, 2, "m", samples(&[(100, 2.0)])).await;
    let executor = executor_over(wrap_fixture_store(memory));
    let (mut snapshot, _) = executor
        .resolve_snapshot(
            tenant_hash,
            &sql_request("SELECT ts, value FROM samples", WINDOW),
            &QueryAccounting::new(),
        )
        .await
        .expect("resolve");
    assert_eq!(snapshot.segments.len(), 2, "test setup: two segments");
    // Slices are cut whole-shard, so the second segment moves to its own
    // shard for the samples-only statement to fan out at all.
    snapshot.segments[1].shard = 1;

    let config = DistributedFlightConfig {
        workers: Arc::new(StaticWorkerEndpoints::new([
            "grpc://worker-a".to_string(),
            "grpc://worker-b".to_string(),
        ])),
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 2,
            max_parallel_slices: 8,
        },
        shared_ticket_key: None,
    };
    let template = |statement: &str| FlightTicket {
        tenant: tenant_hash,
        statement: statement.to_string(),
        segments: snapshot
            .segments
            .iter()
            .map(SegmentPin::from_segment_ref)
            .collect(),
        min_commit_tokens: Vec::new(),
        now_ns: 2_000,
        deadline_ns: 2_000 + 1_000_000_000,
        slice_index: 0,
        slice_count: 1,
        pending_erasure: Vec::new(),
        declared_columns: Vec::new(),
        parquet_tables: Vec::new(),
        budgets: None,
    };

    let one_table = "SELECT ts, value FROM samples";
    let slices = executor
        .plan_distributed_slices_for(&snapshot, one_table, &config, &template(one_table))
        .expect("the samples-only statement distributes");
    assert_eq!(slices.len(), 2);

    let two_tables = "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts";
    assert!(
        executor
            .plan_distributed_slices_for(&snapshot, two_tables, &config, &template(two_tables))
            .is_none()
    );
}

fn empty_snapshot() -> Snapshot {
    Snapshot {
        segments: Vec::new(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The HTTP statement path (`execute`, `execute_accounted`, `explain`,
/// `explain_accounted`) runs a statement over two signal tables, and
/// `execute` returns its exact rows. Every Flight SQL entry point refuses the
/// same statement with [`SqlError::CrossSignalQuery`] before it issues any
/// store request.
#[tokio::test]
async fn the_http_path_runs_two_signal_tables_and_flight_still_refuses() {
    const SQL: &str = "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts \
                       ORDER BY s.ts";
    let tenant = TenantId::new("cross-signal-entry-points".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(
        &memory,
        &tenant,
        1,
        "m",
        samples(&[(0, 1.0), (100, 2.0), (200, 3.0)]),
    )
    .await;
    publish_log_records(
        &memory,
        tenant_hash,
        Signal::Logs,
        2,
        &logs_at(&[100, 200, 300]),
    )
    .await;
    let store = wrap_fixture_store(memory);
    let request: SqlRequest = sql_request(SQL, WINDOW);
    let accounting = QueryAccounting::new();

    let (http_log, http) = recorded(store.clone(), SqlConfig::default());
    let outcome = http
        .execute(tenant_hash, &request)
        .await
        .expect("execute runs a samples JOIN logs statement");
    // ts 0 has no log record and ts 300 no sample; 100 and 200 join once each.
    assert_eq!(
        rows(&outcome.output, "ts", "value"),
        vec![(100, 2.0), (200, 3.0)]
    );
    assert_eq!(outcome.stats.segments, 2, "one segment per signal");
    http.execute_accounted(tenant_hash, &request, &LiveAccounting::new())
        .await
        .expect("execute_accounted runs it");
    let report = http
        .explain(tenant_hash, &request)
        .await
        .expect("explain plans it");
    assert_eq!(report.segments_resolved, 2);
    http.explain_accounted(tenant_hash, &request, &accounting)
        .await
        .expect("explain_accounted plans it");
    assert_eq!(
        data_gets(&http_log.requests(), "/l/").len(),
        1,
        "the HTTP path read the logs object"
    );

    let (recording, executor) = recorded(store, SqlConfig::default());
    let inputs = || PinnedPlanInputs {
        declared: Vec::new(),
        parquet: ParquetPlan::Unresolved,
        budgets: None,
    };

    let mut refused: Vec<(&str, SqlError)> = Vec::new();
    let mut take = |entry: &'static str, err: Option<SqlError>| {
        refused.push((entry, err.unwrap_or_else(|| panic!("{entry} must refuse"))));
    };
    take(
        "resolve_snapshot",
        executor
            .resolve_snapshot(tenant_hash, &request, &accounting)
            .await
            .err(),
    );
    take(
        "resolve_pinned",
        executor
            .resolve_pinned(tenant_hash, &request, &accounting)
            .await
            .err(),
    );
    take(
        "plan_pinned",
        executor
            .plan_pinned(tenant_hash, empty_snapshot(), SQL, &accounting, &[])
            .await
            .err(),
    );
    take(
        "plan_pinned_with_inputs",
        executor
            .plan_pinned_with_inputs(tenant_hash, empty_snapshot(), SQL, &accounting, inputs())
            .await
            .err(),
    );
    #[cfg(feature = "flight-sql")]
    take(
        "plan_pinned_distributed",
        executor
            .plan_pinned_distributed(
                tenant_hash,
                empty_snapshot(),
                SQL,
                &accounting,
                None,
                inputs(),
            )
            .await
            .err(),
    );

    for (entry, err) in &refused {
        assert!(
            matches!(err, SqlError::CrossSignalQuery),
            "{entry} must refuse with CrossSignalQuery, got {err:?}"
        );
    }
    assert_eq!(
        refused.len(),
        if cfg!(feature = "flight-sql") { 5 } else { 4 }
    );
    assert_eq!(recording.requests(), Vec::<String>::new());
}

fn token() -> CommitToken {
    CommitToken {
        shard: 0,
        writer_id: Uuid::from_u128(7),
        epoch: 1,
        seq: 1,
        ingest_hour_bucket: 0,
    }
}

/// The four HTTP entry points' errors for `request`, each labelled.
async fn http_entry_errors(
    executor: &SqlExecutor,
    tenant_hash: TenantHash,
    request: &SqlRequest,
) -> Vec<(&'static str, Option<SqlError>)> {
    vec![
        (
            "execute",
            executor.execute(tenant_hash, request).await.err(),
        ),
        (
            "execute_accounted",
            executor
                .execute_accounted(tenant_hash, request, &LiveAccounting::new())
                .await
                .err(),
        ),
        (
            "explain",
            executor.explain(tenant_hash, request).await.err(),
        ),
        (
            "explain_accounted",
            executor
                .explain_accounted(tenant_hash, request, &QueryAccounting::new())
                .await
                .err(),
        ),
    ]
}

/// An unqualified `min_commit_token` on a statement over two signal tables is
/// [`SqlError::CommitTokenNeedsQualifier`] at every HTTP entry point, naming
/// both tables in lane order, before any store request. The same token on a
/// statement over one signal table is admitted and reaches the catalog.
#[tokio::test]
async fn unqualified_token_on_two_signals_is_refused_before_any_listing() {
    let tenant_hash = TenantHash([4u8; 16]);
    let recording = Arc::new(RecordingStore::new(empty_store()));
    let executor = executor_with(recording.clone(), SqlConfig::default());
    // `logs` first in the text: the message still lists `samples` first.
    let mut request = sql_request(
        "SELECT s.ts FROM logs l JOIN samples s ON s.ts = l.ts",
        WINDOW,
    );
    request.min_tokens = vec![MinCommitToken::unqualified(token())];

    let refused = http_entry_errors(&executor, tenant_hash, &request).await;
    assert_eq!(refused.len(), 4);
    for (entry, err) in refused {
        let Some(err) = err else {
            panic!("{entry} must refuse the unqualified token");
        };
        assert!(
            matches!(err, SqlError::CommitTokenNeedsQualifier { .. }),
            "{entry} must refuse with CommitTokenNeedsQualifier, got {err:?}"
        );
        assert_eq!(err.class(), ErrorClass::BadRequest, "{entry}");
        assert!(
            err.client_message()
                .contains("qualify min_commit_token with one of samples, logs"),
            "{entry}: {}",
            err.client_message()
        );
    }
    assert_eq!(recording.requests(), Vec::<String>::new());

    let mut single = sql_request("SELECT ts FROM samples", WINDOW);
    single.min_tokens = vec![MinCommitToken::unqualified(token())];
    let outcome = executor.execute(tenant_hash, &single).await;
    assert!(
        !matches!(outcome, Err(SqlError::CommitTokenNeedsQualifier { .. })),
        "a token on a one-signal statement is admitted, got {outcome:?}"
    );
    assert!(
        !recording.requests().is_empty(),
        "the one-signal statement reached the catalog"
    );
}

/// The ingest hour the token-only logs commit is published into: far past
/// the listing window of a request at `now_ns` 2,000, so its rows reach a
/// result only through a token that names it.
const TOKEN_ONLY_HOUR: u32 = 5;

/// A qualified token bounds its own table's resolve and no other. `T` names a
/// logs commit, published in an ingest hour the listing never reaches, and
/// no samples commit: `logs:T` on `samples JOIN logs` returns the joined rows
/// and the logs resolve GETs `T`'s commit record, while `samples:T` waits on
/// a samples commit that does not exist and is the unsatisfiable-token 503.
#[tokio::test]
async fn qualified_token_bounds_only_its_signal() {
    let tenant = TenantId::new("cross-signal-qualified-token".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "x", samples(&[(0, 1.0), (100, 2.0)])).await;
    let (_, logs_token) = publish_log_records_in_hour(
        &memory,
        tenant_hash,
        Signal::Logs,
        2,
        TOKEN_ONLY_HOUR,
        &logs_at(&[0, 100]),
    )
    .await;
    assert_eq!(logs_token.ingest_hour_bucket, TOKEN_ONLY_HOUR);
    // The samples commit is seq 1 in hour 0: no samples commit carries T.
    assert_ne!(logs_token.seq, 1);
    let (recording, executor) = recorded(wrap_fixture_store(memory), SqlConfig::default());
    let short = |sql: &str, tokens: Vec<MinCommitToken>| {
        let mut request = sql_request(sql, WINDOW);
        request.min_tokens = tokens;
        request.deadline = std::time::Duration::from_secs(5);
        request
    };
    let unsatisfiable = |err: &SqlError| {
        err.class() == ErrorClass::Unavailable && err.client_message() == MSG_UNSATISFIABLE
    };

    // Precondition: T bounds no samples commit, so on samples alone it is the
    // 503, and the outcome below is about where T is routed.
    let err = executor
        .execute(
            tenant_hash,
            &short(
                "SELECT ts FROM samples",
                vec![MinCommitToken::unqualified(logs_token.clone())],
            ),
        )
        .await
        .expect_err("T names no samples commit");
    assert!(unsatisfiable(&err), "{err:?}");

    const JOIN: &str = "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts \
                        ORDER BY s.ts";
    // Control: without the token the listing never reaches the logs commit.
    let unbounded = executor
        .execute(tenant_hash, &short(JOIN, Vec::new()))
        .await
        .expect("the join runs without a token");
    assert_eq!(
        rows(&unbounded.output, "ts", "value"),
        Vec::<(i64, f64)>::new()
    );

    let logs_commit =
        ravel_commit::keys::commit_key_for_token(&tenant_hash, Signal::Logs, &logs_token)
            .expect("logs commit key");
    let samples_commit =
        ravel_commit::keys::commit_key_for_token(&tenant_hash, Signal::Metrics, &logs_token)
            .expect("samples commit key");
    let before = recording.requests().len();
    let bounded = executor
        .execute(
            tenant_hash,
            &short(
                JOIN,
                vec![MinCommitToken::qualified("logs", logs_token.clone())],
            ),
        )
        .await
        .expect("logs:T bounds the logs resolve");
    assert_eq!(
        rows(&bounded.output, "ts", "value"),
        vec![(0, 1.0), (100, 2.0)]
    );
    let issued = recording.requests().split_off(before);
    assert!(
        issued.contains(&format!("GET {logs_commit}")),
        "the logs resolve consulted T: {issued:#?}"
    );
    assert!(
        !issued
            .iter()
            .any(|request| request.ends_with(&samples_commit)),
        "the samples resolve did not consult T: {issued:#?}"
    );

    let err = executor
        .execute(
            tenant_hash,
            &short(JOIN, vec![MinCommitToken::qualified("samples", logs_token)]),
        )
        .await
        .expect_err("samples:T names no samples commit");
    assert!(unsatisfiable(&err), "{err:?}");
    assert_eq!(err.client_message(), MSG_UNSATISFIABLE);
}

/// A qualifier naming a table the statement does not read is
/// [`SqlError::CommitTokenQualifierNotRead`] at every HTTP entry point,
/// before any store request, and the message quotes the qualifier. A long
/// qualifier is quoted cut to 64 bytes on a char boundary.
#[tokio::test]
async fn a_qualifier_the_statement_does_not_read_is_refused_before_any_listing() {
    let tenant_hash = TenantHash([5u8; 16]);
    let recording = Arc::new(RecordingStore::new(empty_store()));
    let executor = executor_with(recording.clone(), SqlConfig::default());
    let long = format!("a{}", "\u{e9}".repeat(40));
    let cut = format!("a{}...", "\u{e9}".repeat(31));
    let cases = [
        (
            "SELECT ts FROM samples",
            "logs",
            "logs".to_owned(),
            "samples",
        ),
        (
            "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts",
            "spans",
            "spans".to_owned(),
            "samples, logs",
        ),
        // Exact match: the table name in another case is not the table.
        ("SELECT ts FROM logs", "Logs", "Logs".to_owned(), "logs"),
        ("SELECT ts FROM logs", long.as_str(), cut, "logs"),
    ];
    for (sql, qualifier, quoted, tables) in cases {
        let mut request = sql_request(sql, WINDOW);
        request.min_tokens = vec![MinCommitToken::qualified(qualifier, token())];
        for (entry, err) in http_entry_errors(&executor, tenant_hash, &request).await {
            let Some(err) = err else {
                panic!("{entry} must refuse {qualifier:?} on {sql}");
            };
            match &err {
                SqlError::CommitTokenQualifierNotRead {
                    qualifier: got,
                    tables: read,
                } => {
                    assert_eq!(got, &quoted, "{entry}");
                    assert_eq!(read, tables, "{entry}");
                }
                other => {
                    panic!("{entry} must refuse with CommitTokenQualifierNotRead, got {other:?}")
                }
            }
            assert_eq!(err.class(), ErrorClass::BadRequest, "{entry}");
            assert!(
                err.client_message()
                    .contains(&format!("qualifier {quoted:?}")),
                "{entry}: {}",
                err.client_message()
            );
        }
    }
    assert_eq!(recording.requests(), Vec::<String>::new());
}

/// The `samples JOIN logs` fixture of the per-table cost tests: two metrics
/// segments and one logs segment, so a swapped segment count shows.
async fn per_table_cost_fixture(name: &str) -> (FixtureStore, TenantHash) {
    let tenant = TenantId::new(name.to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_metric_samples(&memory, &tenant, 3, "m", samples(&[(200, 3.0)])).await;
    publish_log_records(
        &memory,
        tenant_hash,
        Signal::Logs,
        2,
        &logs_at(&[100, 200, 300]),
    )
    .await;
    (wrap_fixture_store(memory), tenant_hash)
}

const PER_TABLE_JOIN: &str = "SELECT s.ts, s.value FROM samples s JOIN logs l ON s.ts = l.ts \
                              ORDER BY s.ts";

/// Every numeric field of one rendered phase entry, by key.
fn rendered_fields(entry: &serde_json::Value) -> Vec<(String, u64)> {
    entry
        .as_object()
        .expect("a phase entry is an object")
        .iter()
        .filter(|(key, _)| key.as_str() != "phase")
        .map(|(key, value)| (key.clone(), value.as_u64().expect("a u64 field")))
        .collect()
}

/// The signal a recorded request's key belongs to: `/m/` for metrics, `/l/`
/// for logs.
fn request_signal(request: &str) -> &'static str {
    if request.contains("/m/") {
        "samples"
    } else if request.contains("/l/") {
        "logs"
    } else {
        panic!("a request outside both signals: {request}")
    }
}

/// `stats.signals` of a `samples JOIN logs` statement: one entry per table in
/// lane order, each the table's own cost, and the two summing to the
/// statement's per-phase total for every rendered field.
#[tokio::test]
async fn stats_signals_sum_to_totals_per_phase() {
    use ravel_query::QueryPhase;
    use ravel_types::accounting::AccountedOp;

    use crate::stats_json::phase_costs_json;

    let (store, tenant_hash) = per_table_cost_fixture("cross-signal-per-table-cost").await;
    let (recording, executor) = recorded(store, SqlConfig::default());
    let outcome = executor
        .execute(tenant_hash, &sql_request(PER_TABLE_JOIN, WINDOW))
        .await
        .expect("the join runs");
    assert_eq!(
        rows(&outcome.output, "ts", "value"),
        vec![(100, 2.0), (200, 3.0)]
    );

    // (i) Two entries, in lane order.
    let tables: Vec<&str> = outcome.signals.iter().map(|s| s.table).collect();
    assert_eq!(tables, ["samples", "logs"]);
    let samples_entry = &outcome.signals[0];
    let logs_entry = &outcome.signals[1];

    // (ii) For every phase and every rendered field, the entries sum to the
    // statement total.
    let total = phase_costs_json(&outcome.phase_accounting);
    let samples_json = phase_costs_json(&samples_entry.phases);
    let logs_json = phase_costs_json(&logs_entry.phases);
    for (index, phase) in QueryPhase::ALL.iter().enumerate() {
        let samples_fields = rendered_fields(&samples_json[index]);
        let logs_fields = rendered_fields(&logs_json[index]);
        let total_fields = rendered_fields(&total[index]);
        assert_eq!(total_fields.len(), 10, "every rendered field is compared");
        for (((key, samples), (logs_key, logs)), (total_key, total)) in
            samples_fields.iter().zip(&logs_fields).zip(&total_fields)
        {
            assert_eq!((key, logs_key), (total_key, total_key));
            assert_eq!(
                samples + logs,
                *total,
                "{} {key}: samples {samples} + logs {logs}",
                phase.name()
            );
        }
    }

    // (iii) Each entry's GET and LIST counts are its own signal's recorded
    // requests.
    let requests = recording.requests();
    let recorded_count = |table: &str, op: &str| {
        requests
            .iter()
            .filter(|r| r.starts_with(&format!("{op} ")) && request_signal(r) == table)
            .count() as u64
    };
    for entry in [samples_entry, logs_entry] {
        let pooled = entry.phases.pooled();
        assert_eq!(
            pooled.s3_requests(AccountedOp::Get),
            recorded_count(entry.table, "GET"),
            "{} GETs",
            entry.table
        );
        assert_eq!(
            pooled.s3_requests(AccountedOp::List),
            recorded_count(entry.table, "LIST"),
            "{} LISTs",
            entry.table
        );
        assert_eq!(
            pooled.s3_requests(AccountedOp::Head),
            recorded_count(entry.table, "HEAD"),
            "{} HEADs",
            entry.table
        );
    }
    assert_eq!(
        outcome.accounting.total_s3_requests(),
        requests.len() as u64,
        "every recorded request belongs to one entry"
    );
    let per_phase = |phases: &ravel_query::PhaseAccountingSnapshot| {
        QueryPhase::ALL.map(|phase| {
            let s = phases.phase(phase);
            (
                s.s3_requests(AccountedOp::Get),
                s.s3_requests(AccountedOp::List),
            )
        })
    };
    // (GET, LIST) per phase, resolve/plan/probe/scan/keyIndex. Resolve: the
    // catalog HEAD, the commit listing, the commit records and the deletion
    // listing; the metrics data objects are read while planning, the logs one
    // while scanning.
    assert_eq!(
        per_phase(&samples_entry.phases),
        [(3, 2), (2, 0), (0, 0), (0, 0), (0, 0)]
    );
    assert_eq!(
        per_phase(&logs_entry.phases),
        [(2, 2), (0, 0), (0, 0), (1, 0), (0, 0)]
    );

    // (iv) Each entry's segments are its own lane's, and they sum to the
    // statement's.
    assert_eq!(samples_entry.segments, 2);
    assert_eq!(logs_entry.segments, 1);
    assert_eq!(
        samples_entry.segments + logs_entry.segments,
        outcome.stats.segments
    );
    assert_eq!(data_gets(&requests, "/m/").len(), samples_entry.segments);
    assert_eq!(data_gets(&requests, "/l/").len(), logs_entry.segments);
    assert_eq!(
        (samples_entry.segments_pruned, logs_entry.segments_pruned),
        (0, 0),
        "an unfolded catalog prunes nothing by postings"
    );
}

/// A statement over one signal table, or over none, carries one
/// `stats.signals` entry, named for the table (`samples` when it names none), whose phases render exactly as `stats.phases` and whose
/// segments are `stats.pruning.segments`.
#[tokio::test]
async fn single_signal_statement_has_one_signals_entry() {
    use crate::stats_json::phase_costs_json;

    let tenant = TenantId::new("cross-signal-one-entry".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &logs_at(&[0, 100])).await;
    publish_spans(
        &memory,
        tenant_hash,
        3,
        &[SpanRecord {
            trace_id: [1u8; 16],
            span_id: [1u8; 8],
            parent_span_id: None,
            name: "query".to_string(),
            start_ts_ns: 100,
            end_ts_ns: 300,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: Vec::new(),
        }],
    )
    .await;
    let store = wrap_fixture_store(memory);
    let executor = executor_with(store, SqlConfig::default());

    for (sql, table, segments) in [
        ("SELECT ts, value FROM samples", "samples", 1),
        ("SELECT ts, body FROM logs", "logs", 1),
        ("SELECT start_ts FROM spans", "spans", 1),
        ("SELECT COUNT(*) FROM alerts", "alerts", 0),
        ("SELECT COUNT(*) FROM audit", "audit", 0),
        // A statement naming no table resolves `samples`.
        ("SELECT 1 AS one", "samples", 1),
    ] {
        let outcome = executor
            .execute(tenant_hash, &sql_request(sql, WINDOW))
            .await
            .unwrap_or_else(|err| panic!("{sql} runs: {err}"));
        let tables: Vec<&str> = outcome.signals.iter().map(|s| s.table).collect();
        assert_eq!(tables, [table], "{sql}");
        let entry = &outcome.signals[0];
        assert_eq!(
            phase_costs_json(&entry.phases),
            phase_costs_json(&outcome.phase_accounting),
            "{sql}"
        );
        assert!(
            entry.phases.pooled().total_s3_requests() > 0,
            "{sql}: the entry is charged"
        );
        assert_eq!(entry.segments, outcome.stats.segments, "{sql}");
        assert_eq!(entry.segments, segments, "{sql}");
    }
}

/// The byte and request budgets checked during the samples scan count the
/// whole statement, not the samples table alone: a request budget above the
/// samples table's own cost and below the statement's refuses the join.
#[tokio::test]
async fn a_statement_budget_still_counts_every_lane() {
    let (store, tenant_hash) = per_table_cost_fixture("cross-signal-statement-budget").await;
    // Each run gets its own executor, so neither reads the other's caches.
    let unbudgeted = executor_with(store.clone(), SqlConfig::default())
        .execute(tenant_hash, &sql_request(PER_TABLE_JOIN, WINDOW))
        .await
        .expect("the unbudgeted join runs");
    let samples_requests = unbudgeted.signals[0].phases.pooled().total_s3_requests();
    let statement_requests = unbudgeted.accounting.total_s3_requests();
    // The resolve-time check holds every lane's resolve, so a budget at that
    // sum passes it and leaves the refusal to the scan.
    let max = unbudgeted.phase_accounting.resolve.total_s3_requests();
    assert!(
        samples_requests < max && max < statement_requests,
        "the budget sits between the samples table's {samples_requests} requests \
         and the statement's {statement_requests}: {max}"
    );
    // Samples: 5 resolve + 2 data objects; statement: 9 resolve + 3 data
    // objects.
    assert_eq!((samples_requests, max, statement_requests), (7, 9, 12));

    let mut request = sql_request(PER_TABLE_JOIN, WINDOW);
    request.budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(max)),
        ..Default::default()
    });
    let err = executor_with(store, SqlConfig::default())
        .execute(tenant_hash, &request)
        .await
        .expect_err("the statement exceeds the budget");
    // The scan's checkpoint refuses with the same message the resolve-time
    // check uses, and no table suffix: how many requests were in flight when
    // it fired depends on scheduling, so only the excess is pinned.
    let message = err.to_string();
    let issued: u64 = message
        .strip_prefix("query issued ")
        .and_then(|rest| rest.strip_suffix(&format!(" S3 requests, exceeding the budget of {max}")))
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("an unexpected refusal: {message}"));
    assert!(issued > max, "{issued} requests against a budget of {max}");
}

/// `explain` reports one entry per table, measured on a handle of its own,
/// and the caller's accounting receives exactly their sum.
#[tokio::test]
async fn explain_reports_each_signal_table_and_the_caller_keeps_the_total() {
    use ravel_types::accounting::AccountedOp;

    let (store, tenant_hash) = per_table_cost_fixture("cross-signal-explain-cost").await;
    let (recording, executor) = recorded(store, SqlConfig::default());
    let accounting = QueryAccounting::new();
    let report = executor
        .explain_accounted(
            tenant_hash,
            &sql_request(PER_TABLE_JOIN, WINDOW),
            &accounting,
        )
        .await
        .expect("explain plans the join");
    let tables: Vec<&str> = report.signals.iter().map(|s| s.table).collect();
    assert_eq!(tables, ["samples", "logs"]);
    let caller = accounting.snapshot();
    let resolve_requests: u64 = report
        .signals
        .iter()
        .map(|s| s.phases.resolve.total_s3_requests())
        .sum();
    let resolve_bytes: u64 = report
        .signals
        .iter()
        .map(|s| s.phases.resolve.total_s3_bytes())
        .sum();
    let all_requests: u64 = report
        .signals
        .iter()
        .map(|s| s.phases.pooled().total_s3_requests())
        .sum();
    let all_bytes: u64 = report
        .signals
        .iter()
        .map(|s| s.phases.pooled().total_s3_bytes())
        .sum();
    // Resolve per table: the catalog HEAD, the commit listing, one GET per
    // commit record (two for samples, one for logs) and the deletion listing.
    // Planning reads no data object here: the metrics one is read only once a
    // scan opens it.
    assert_eq!(resolve_requests, 9);
    assert_eq!(all_requests, caller.total_s3_requests());
    assert_eq!(all_bytes, caller.total_s3_bytes());
    assert_eq!(
        caller.total_s3_requests(),
        recording.requests().len() as u64
    );
    assert_eq!(
        caller.s3_requests(AccountedOp::List),
        report
            .signals
            .iter()
            .map(|s| s.phases.pooled().s3_requests(AccountedOp::List))
            .sum::<u64>()
    );
    assert!(resolve_bytes > 0);
    assert_eq!(
        report
            .signals
            .iter()
            .map(|s| s.segments)
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(report.segments_resolved, 3);
}

/// `stats.signals` follows the fixed table order, not the order the statement
/// names its tables in: the same join written logs first reports samples then
/// logs, each entry equal to the samples-first statement's.
#[tokio::test]
async fn entries_follow_table_order_not_naming_order() {
    const LOGS_FIRST: &str = "SELECT s.ts, s.value FROM logs l JOIN samples s ON s.ts = l.ts \
                              ORDER BY s.ts";
    let (store, tenant_hash) = per_table_cost_fixture("cross-signal-naming-order").await;
    // Each run gets its own executor, so neither reads the other's caches.
    let samples_first = executor_with(store.clone(), SqlConfig::default())
        .execute(tenant_hash, &sql_request(PER_TABLE_JOIN, WINDOW))
        .await
        .expect("the samples-first join runs");
    let logs_first = executor_with(store, SqlConfig::default())
        .execute(tenant_hash, &sql_request(LOGS_FIRST, WINDOW))
        .await
        .expect("the logs-first join runs");
    assert_eq!(
        rows(&logs_first.output, "ts", "value"),
        rows(&samples_first.output, "ts", "value")
    );
    let tables: Vec<&str> = logs_first.signals.iter().map(|s| s.table).collect();
    assert_eq!(tables, ["samples", "logs"]);
    assert_eq!(logs_first.signals, samples_first.signals);
}

/// An `explain` that fails partway through still hands the caller the
/// requests it made before the failure: the logs lane's deletion listing
/// fails after the samples lane resolved, and the caller's accounting holds
/// every request the store saw.
#[tokio::test]
async fn a_failed_explain_still_merges_its_cost_into_the_caller() {
    let tenant = TenantId::new("cross-signal-explain-failure".to_string());
    let tenant_hash = tenant.hash();
    let memory = MemoryStore::new();
    publish_metric_samples(&memory, &tenant, 1, "m", samples(&[(0, 1.0), (100, 2.0)])).await;
    publish_log_records(&memory, tenant_hash, Signal::Logs, 2, &logs_at(&[100])).await;
    let logs_deletions = format!("t/{}/l/del/", tenant_hash.to_hex());
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::List,
            ScriptedFault::Permanent("injected logs deletion listing failure".to_string()),
        )
        .with_key_contains(logs_deletions.clone()),
    );
    let store: FixtureStore = Arc::new(InstrumentedStore::new(FaultStore::new(memory, plan)));
    let (recording, executor) = recorded(store.clone(), SqlConfig::default());
    let accounting = QueryAccounting::new();
    executor
        .explain_accounted(
            tenant_hash,
            &sql_request(PER_TABLE_JOIN, WINDOW),
            &accounting,
        )
        .await
        .expect_err("the logs deletion listing fails the explain");

    assert_eq!(
        store.inner().fault_count(Op::List, FaultKind::Permanent),
        1,
        "the injected failure fired"
    );
    let requests = recording.requests();
    let samples_deletions = format!("LIST t/{}/m/del/", tenant_hash.to_hex());
    assert!(
        requests.contains(&samples_deletions),
        "the samples lane resolved before the failure: {requests:?}"
    );
    assert_eq!(
        requests.last(),
        Some(&format!("LIST {logs_deletions}")),
        "the failed listing is the last request"
    );
    let caller = accounting.snapshot();
    assert!(caller.total_s3_requests() > 0);
    assert_eq!(caller.total_s3_requests(), requests.len() as u64);
}
