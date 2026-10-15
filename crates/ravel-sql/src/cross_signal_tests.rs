//! A statement over two or more signal tables, run through the per-signal
//! resolve and plan with the public cross-signal refusal switched off
//! ([`SqlExecutor::allow_cross_signal_for_test`]) (issue #2910, part A), and
//! the refusal itself at every public entry point with it on.

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
use ravel_types::{Sample, Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

use crate::SqlConfig;
use crate::error::SqlError;
use crate::executor::tests::{
    empty_store, executor_over, executor_with, log_record, publish_log_records,
    publish_metric_samples, publish_spans, sql_request, wrap_fixture_store,
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

/// `store` under a [`RecordingStore`], and an executor over it with the
/// cross-signal refusal off.
fn recorded(
    store: FixtureStore,
    config: SqlConfig,
) -> (Arc<RecordingStore<FixtureStore>>, SqlExecutor) {
    let recording = Arc::new(RecordingStore::new(store));
    let executor = executor_with(recording.clone(), config).allow_cross_signal_for_test();
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
        executor_with(store.clone(), config).allow_cross_signal_for_test()
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

    let executor = executor_over(empty_store()).allow_cross_signal_for_test();
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

/// With the refusal on, as it is for every executor outside this module,
/// every public entry point refuses a statement over two signal tables with
/// [`SqlError::CrossSignalQuery`] before it issues any store request.
#[tokio::test]
async fn the_public_entry_points_still_refuse_two_signal_tables() {
    const SQL: &str = "SELECT s.ts FROM samples s JOIN logs l ON s.ts = l.ts";
    let tenant_hash = TenantHash([3u8; 16]);
    let store = empty_store();
    let recording = Arc::new(RecordingStore::new(store.clone()));
    let executor = executor_with(recording.clone(), SqlConfig::default());
    let request: SqlRequest = sql_request(SQL, WINDOW);
    let accounting = QueryAccounting::new();
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
        "execute",
        executor.execute(tenant_hash, &request).await.err(),
    );
    take(
        "execute_accounted",
        executor
            .execute_accounted(tenant_hash, &request, &LiveAccounting::new())
            .await
            .err(),
    );
    take(
        "explain",
        executor.explain(tenant_hash, &request).await.err(),
    );
    take(
        "explain_accounted",
        executor
            .explain_accounted(tenant_hash, &request, &accounting)
            .await
            .err(),
    );
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
        if cfg!(feature = "flight-sql") { 9 } else { 8 }
    );
    assert_eq!(recording.requests(), Vec::<String>::new());
    let metrics = store.metrics().snapshot();
    assert_eq!(
        (metrics.list.calls, metrics.get.calls, metrics.head.calls),
        (0, 0, 0)
    );
}
