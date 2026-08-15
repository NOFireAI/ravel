//! End-to-end tests for the alert evaluator and its sinks (ADR-0043).
//!
//! Real RSEG segments, real commit records, a real catalog and `QueryEngine`,
//! a real `MemoryStore`, and real HTTP sinks served by an in-process axum
//! listener. Nothing here is a mock: a tick runs the same code the background
//! task runs, the alert records it writes are read back out of the object store
//! through their commit records, and the sink assertions are made on bytes that
//! actually crossed a socket. This mirrors the harness in
//! `analytics_endpoint.rs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use ravel_alerting::{AlertRecord, AlertState, Rule, RuleCondition, RuleQuery, ThresholdOp};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::{Predicate, RlogConfig, RlogReader};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::alert_sink::AlertSink;
use ravel_server::alerting::{
    ALERT_SHARD, AlertEvalConfig, AlertEvaluator, AlertQueryEngines, parse_rules,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value as Json;
use tokio::sync::oneshot;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
/// Start of the frozen clock: half an hour past the epoch. Inside the first
/// ingest hour, which is where the seeded segment's commit record lives, so
/// `Catalog::resolve`'s listing window (`max_ingest_lag` behind the query
/// window) covers it, and small enough that the per-hour LIST fan-out is cheap.
const NOW_NS: i64 = 30 * 60 * NS_PER_SEC;
const METRIC: &str = "cpu_usage";
const TENANT: &str = "acme";

// --- Clock -----------------------------------------------------------------

/// A clock a test advances by hand, so pending-duration elapse is exercised
/// with no wall-clock sleep.
#[derive(Clone)]
struct TestClock(Arc<AtomicI64>);

impl TestClock {
    fn at(now_ns: i64) -> TestClock {
        TestClock(Arc::new(AtomicI64::new(now_ns)))
    }

    fn advance(&self, by: Duration) {
        let by_ns = i64::try_from(by.as_nanos()).expect("test duration fits i64");
        self.0.fetch_add(by_ns, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

// --- Seeding ---------------------------------------------------------------

fn label_set(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publish one real RSEG segment holding `metric` at `samples`, plus its commit
/// record, for `tenant`.
async fn publish_metric(store: &dyn ObjectStoreBackend, tenant: &TenantId, samples: &[(i64, f64)]) {
    let tenant_hash = tenant.hash();
    let labels = label_set(METRIC);
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &labels).expect("series id"),
        labels,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(9_001);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

// --- Reading alert history back --------------------------------------------

/// Every alert record this tenant has, read the same way any reader would:
/// through the commit records, then the RLOG objects they name. Sorted by
/// timestamp so a test can assert the transition sequence.
async fn read_alert_records(
    store: &dyn ObjectStoreBackend,
    tenant: TenantHash,
) -> Vec<AlertRecord> {
    let prefix = keys::commit_shard_prefix(&tenant, Signal::Alerts, ALERT_SHARD).expect("prefix");
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in list_all(store, &prefix).await.expect("list") {
        let commit = record::decode(
            &store
                .get(&meta.key, GetRange::Full)
                .await
                .expect("get")
                .data,
        )
        .expect("decode commit record");
        assert_eq!(
            ravel_commit::signal::from_proto(commit.signal).expect("known signal"),
            Signal::Alerts,
            "an object under the alerts prefix must be tagged Signal::Alerts"
        );
        let data_key = keys::verify_object_key(&commit).expect("object key verifies");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(&object.data, &cfg).expect("open rlog");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        for row in &rows {
            out.push(AlertRecord::from_log_record(row).expect("decode alert record"));
        }
    }
    out.sort_by_key(|r| r.ts_ns);
    out
}

/// How many RLOG data objects exist under the tenant's alerts keyspace,
/// including any not yet committed.
async fn alert_object_count(store: &dyn ObjectStoreBackend, tenant: TenantHash) -> usize {
    let prefix = format!("t/{}/a/l0/", tenant.to_hex());
    list_all(store, &prefix).await.expect("list").len()
}

// --- Evaluator construction -------------------------------------------------

fn threshold_rule(for_duration: Option<Duration>) -> Rule {
    Rule {
        rule_id: "high-cpu".to_string(),
        query: RuleQuery::Promql(METRIC.to_string()),
        condition: RuleCondition::Threshold {
            op: ThresholdOp::Gt,
            threshold: 0.9,
        },
        labels: vec![("severity".to_string(), "page".to_string())],
        annotations: vec![("summary".to_string(), "cpu is hot".to_string())],
        for_duration,
        max_alert_generation: None,
        repeat_interval: None,
    }
}

/// A firing rule with an explicit repeat cadence, for the "repeat notifications
/// while firing" tests (ADR-0043 amendment). `for_duration` is `None` so the
/// first tick fires immediately; `repeat_interval` is `Some` so the default is
/// not silently under test.
fn repeat_rule(repeat_interval: Option<Duration>) -> Rule {
    Rule {
        repeat_interval,
        ..threshold_rule(None)
    }
}

fn evaluator(
    store: Arc<dyn ObjectStoreBackend>,
    clock: TestClock,
    rules: Vec<Rule>,
    sinks: Vec<AlertSink>,
) -> AlertEvaluator {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, Arc::clone(&store), EngineConfig::default());
    let config = AlertEvalConfig {
        enabled: true,
        sinks: Arc::new(sinks),
        ..AlertEvalConfig::default()
    };
    AlertEvaluator::new(
        store,
        AlertQueryEngines {
            promql: Arc::new(engine),
            #[cfg(feature = "sql")]
            sql: None,
        },
        Arc::new(clock),
        TenantId::new(TENANT).hash(),
        rules,
        &config,
    )
    .expect("build evaluator")
}

/// A store seeded with one series whose value is 1.0 (above the rules'
/// threshold of 0.9) shortly before `NOW_NS`.
async fn seeded_store() -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    publish_metric(store.as_ref(), &tenant, &[(NOW_NS - 30 * NS_PER_SEC, 1.0)]).await;
    store
}

/// A store seeded with above-threshold samples every 60 seconds from just
/// before `NOW_NS` through `last_ns`, so the instant query keeps finding a fresh
/// sample (within PromQL's 5-minute lookback) at every tick and the alert stays
/// `Firing` across the whole span. `last_ns` stays inside the first ingest hour
/// so the one seeded segment is always listed. Used by the repeat-cadence tests,
/// which must hold a rule firing across more than one 5-minute resolve timeout.
async fn seeded_store_spanning(last_ns: i64) -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let mut samples = Vec::new();
    let mut ts = NOW_NS - 30 * NS_PER_SEC;
    while ts <= last_ns {
        samples.push((ts, 1.0));
        ts += 60 * NS_PER_SEC;
    }
    publish_metric(store.as_ref(), &tenant, &samples).await;
    store
}

// --- A real HTTP sink endpoint ---------------------------------------------

/// Everything the in-process sink server saw, plus the status it answers with.
struct Capture {
    received: Mutex<Vec<(String, Json)>>,
    status: AtomicU16,
}

impl Capture {
    fn new(status: StatusCode) -> Arc<Capture> {
        Arc::new(Capture {
            received: Mutex::new(Vec::new()),
            status: AtomicU16::new(status.as_u16()),
        })
    }

    fn set_status(&self, status: StatusCode) {
        self.status.store(status.as_u16(), Ordering::SeqCst);
    }

    fn calls(&self) -> Vec<(String, Json)> {
        self.received.lock().expect("capture lock").clone()
    }
}

async fn sink_handler(
    State(capture): State<Arc<Capture>>,
    uri: Uri,
    body: axum::body::Bytes,
) -> StatusCode {
    let json: Json = serde_json::from_slice(&body).expect("a sink body must be JSON");
    capture
        .received
        .lock()
        .expect("capture lock")
        .push((uri.path().to_string(), json));
    StatusCode::from_u16(capture.status.load(Ordering::SeqCst)).expect("valid status")
}

/// A running sink endpoint on a loopback port. Answers every path, so one
/// server serves both the webhook URL and Alertmanager's `/api/v2/alerts`.
struct SinkServer {
    addr: SocketAddr,
    capture: Arc<Capture>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl SinkServer {
    async fn start(status: StatusCode) -> SinkServer {
        let capture = Capture::new(status);
        let app = Router::new()
            .fallback(sink_handler)
            .with_state(Arc::clone(&capture));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind sink listener");
        let addr = listener.local_addr().expect("sink addr");
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        SinkServer {
            addr,
            capture,
            shutdown: Some(tx),
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

// --- Tests ------------------------------------------------------------------

/// The core loop: a condition that holds writes exactly one firing record, and
/// a second tick with the condition still holding writes nothing (ADR-0043
/// decision 4 - no heartbeat record per tick).
#[tokio::test]
async fn a_true_condition_fires_once_and_a_repeat_tick_writes_nothing() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(None)],
        Vec::new(),
    );

    let first = evaluator.run_tick().await;
    assert!(!first.history_unavailable, "history read must succeed");
    assert_eq!(first.rules_evaluated, 1);
    assert_eq!(first.rules_failed, 0);
    assert_eq!(first.records_written, 1, "the onset is a transition");

    let records = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(records.len(), 1, "one Signal::Alerts record");
    assert_eq!(records[0].state, AlertState::Firing);
    assert_eq!(records[0].rule_id, "high-cpu");
    assert_eq!(records[0].ts_ns, NOW_NS);
    assert_eq!(
        records[0].generation, 0,
        "an ordinary metric rule is always generation 0"
    );
    assert_eq!(
        records[0].labels,
        vec![("severity".to_string(), "page".to_string())]
    );
    assert_eq!(
        records[0].annotations,
        vec![("summary".to_string(), "cpu is hot".to_string())]
    );
    assert_eq!(alert_object_count(store.as_ref(), tenant).await, 1);

    // Second tick, condition still true: still firing, nothing new written.
    clock.advance(Duration::from_secs(10));
    let second = evaluator.run_tick().await;
    assert_eq!(second.rules_evaluated, 1);
    assert_eq!(
        second.records_written, 0,
        "a tick that re-confirms firing writes no record"
    );
    assert_eq!(
        read_alert_records(store.as_ref(), tenant).await.len(),
        1,
        "the alert history is unchanged"
    );
    assert_eq!(
        alert_object_count(store.as_ref(), tenant).await,
        1,
        "no second object was even PUT"
    );
}

/// With `for`, the first tick opens a pending record and firing waits for the
/// duration to elapse - measured from the pending record's own timestamp, not
/// from an in-process timer (ADR-0043 decision 3).
#[tokio::test]
async fn a_for_duration_rule_pends_then_fires_once_the_duration_elapses() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(Some(Duration::from_secs(60)))],
        Vec::new(),
    );

    assert_eq!(evaluator.run_tick().await.records_written, 1);
    let records = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].state,
        AlertState::Pending,
        "the onset is pending while `for` has not elapsed"
    );

    // Halfway through `for`: still pending, no new record.
    clock.advance(Duration::from_secs(30));
    assert_eq!(evaluator.run_tick().await.records_written, 0);
    assert_eq!(read_alert_records(store.as_ref(), tenant).await.len(), 1);

    // Past `for`: fires, writing a second record.
    clock.advance(Duration::from_secs(31));
    assert_eq!(evaluator.run_tick().await.records_written, 1);
    let records = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(records.len(), 2, "pending then firing");
    assert_eq!(records[0].state, AlertState::Pending);
    assert_eq!(records[1].state, AlertState::Firing);
    assert_eq!(
        records[0].alert_id, records[1].alert_id,
        "both records belong to one alert identity"
    );
}

/// A condition that stops holding resolves. Here the series goes stale (no
/// sample within the lookback window), so the instant vector empties and the
/// threshold can no longer be met.
#[tokio::test]
async fn a_condition_that_clears_writes_a_resolved_record() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(None)],
        Vec::new(),
    );

    assert_eq!(evaluator.run_tick().await.records_written, 1);

    // An hour on, the only sample is far outside the lookback window.
    clock.advance(Duration::from_secs(3600));
    assert_eq!(evaluator.run_tick().await.records_written, 1);

    let records = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].state, AlertState::Firing);
    assert_eq!(records[1].state, AlertState::Resolved);

    // Still cleared on the next tick: resolved is not re-written.
    clock.advance(Duration::from_secs(60));
    assert_eq!(evaluator.run_tick().await.records_written, 0);
    assert_eq!(read_alert_records(store.as_ref(), tenant).await.len(), 2);
}

/// The webhook sink POSTs the transition as JSON to a real listener, and the
/// body carries the record that is already durable.
#[tokio::test]
async fn the_webhook_sink_posts_the_transition_body() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let webhook_url = format!("{}/hook", server.base_url());
    let mut evaluator = evaluator(
        Arc::clone(&store),
        TestClock::at(NOW_NS),
        vec![threshold_rule(None)],
        vec![AlertSink::webhook(webhook_url)],
    );

    let report = evaluator.run_tick().await;
    assert_eq!(report.records_written, 1);
    assert_eq!(report.notifications_delivered, 1);
    assert_eq!(report.notifications_failed, 0);

    let calls = server.capture.calls();
    assert_eq!(calls.len(), 1, "one transition, one POST");
    let (path, body) = &calls[0];
    assert_eq!(path, "/hook", "posted to the configured URL verbatim");

    let record = &read_alert_records(store.as_ref(), tenant).await[0];
    assert_eq!(body["alert_id"], Json::String(record.alert_id.to_hex()));
    assert_eq!(body["rule_id"], Json::String("high-cpu".to_string()));
    assert_eq!(body["state"], Json::String("firing".to_string()));
    assert_eq!(
        body["previous_state"],
        Json::Null,
        "this alert had no history"
    );
    assert_eq!(body["generation"], Json::from(0));
    assert_eq!(
        body["ts_unix_nano"],
        Json::from(NOW_NS),
        "the exact nanosecond timestamp of the durable record"
    );
    assert_eq!(body["labels"]["severity"], Json::String("page".to_string()));
    assert_eq!(
        body["annotations"]["summary"],
        Json::String("cpu is hot".to_string())
    );
    assert_eq!(body["body"], Json::String("high-cpu firing".to_string()));

    server.stop().await;
}

/// The Alertmanager sink POSTs to `/api/v2/alerts` in Alertmanager's own
/// payload shape: an array of alerts labelled by `alertname`.
#[tokio::test]
async fn the_alertmanager_sink_posts_the_api_v2_alerts_payload() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store().await;
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(None)],
        // Configured as a base URL: the well-known path is appended.
        vec![AlertSink::alertmanager(server.base_url())],
    );

    assert_eq!(evaluator.run_tick().await.notifications_delivered, 1);

    let calls = server.capture.calls();
    assert_eq!(calls.len(), 1);
    let (path, body) = &calls[0];
    assert_eq!(path, "/api/v2/alerts");

    let alerts = body.as_array().expect("api/v2/alerts takes an array");
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        alerts[0]["labels"]["alertname"],
        Json::String("high-cpu".to_string())
    );
    assert_eq!(
        alerts[0]["labels"]["severity"],
        Json::String("page".to_string())
    );
    assert_eq!(
        alerts[0]["annotations"]["summary"],
        Json::String("cpu is hot".to_string())
    );
    assert!(
        alerts[0]["startsAt"].is_string(),
        "startsAt is RFC3339 text"
    );
    assert!(
        alerts[0].get("endsAt").is_none(),
        "a firing alert leaves the resolve timeout to Alertmanager"
    );

    // Resolving sends endsAt, which is how Alertmanager is told it is over.
    clock.advance(Duration::from_secs(3600));
    assert_eq!(evaluator.run_tick().await.notifications_delivered, 1);
    let calls = server.capture.calls();
    assert_eq!(calls.len(), 2);
    let resolved = &calls[1].1.as_array().expect("array")[0];
    assert!(
        resolved["endsAt"].is_string(),
        "a resolved alert carries endsAt"
    );

    server.stop().await;
}

/// ADR-0043 decision 6, proven end to end: a sink that fails does not block the
/// record write, does not corrupt or duplicate it, and its notification is
/// retried on a later tick from the latest record - with no new transition
/// needed to trigger the retry.
#[tokio::test]
async fn a_failing_sink_never_blocks_the_record_write_and_retries_next_tick() {
    let server = SinkServer::start(StatusCode::INTERNAL_SERVER_ERROR).await;
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(None)],
        vec![AlertSink::webhook(format!("{}/hook", server.base_url()))],
    );

    let first = evaluator.run_tick().await;
    assert_eq!(
        first.records_written, 1,
        "the record is written even though the sink is failing"
    );
    assert_eq!(first.notifications_delivered, 0);
    assert_eq!(first.notifications_failed, 1);

    let after_failure = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(after_failure.len(), 1, "exactly one record, uncorrupted");
    assert_eq!(after_failure[0].state, AlertState::Firing);
    assert_eq!(server.capture.calls().len(), 1, "the sink was attempted");

    // The sink recovers. The next tick is a no-transition tick (the condition
    // still holds), so no record is written, and the notification is retried
    // from the latest record all the same.
    server.capture.set_status(StatusCode::OK);
    clock.advance(Duration::from_secs(10));
    let second = evaluator.run_tick().await;
    assert_eq!(
        second.records_written, 0,
        "the retry does not manufacture a second record"
    );
    assert_eq!(second.notifications_delivered, 1, "delivered on retry");
    assert_eq!(second.notifications_failed, 0);
    assert_eq!(server.capture.calls().len(), 2);

    let after_retry = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(
        after_retry, after_failure,
        "the durable record is byte-identical before and after sink delivery"
    );

    // A third tick has nothing left to deliver.
    clock.advance(Duration::from_secs(10));
    let third = evaluator.run_tick().await;
    assert_eq!(third.notifications_delivered, 0);
    assert_eq!(third.notifications_failed, 0);
    assert_eq!(
        server.capture.calls().len(),
        2,
        "a delivered notification is not re-sent forever"
    );

    server.stop().await;
}

/// An unreachable sink is a delivery failure like any other: logged, retried,
/// and never fatal to the write. Covers the transport-error path rather than
/// the non-2xx path above.
#[tokio::test]
async fn an_unreachable_sink_does_not_fail_the_tick() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let mut evaluator = evaluator(
        Arc::clone(&store),
        TestClock::at(NOW_NS),
        vec![threshold_rule(None)],
        // Port 1 on loopback: nothing listens, so the connection is refused.
        vec![AlertSink::webhook("http://127.0.0.1:1/hook")],
    );

    let report = evaluator.run_tick().await;
    assert_eq!(report.rules_failed, 0, "a sink error is not a rule error");
    assert_eq!(report.records_written, 1);
    assert_eq!(report.notifications_failed, 1);
    assert_eq!(read_alert_records(store.as_ref(), tenant).await.len(), 1);
}

/// The evaluator reads its rules from the same parsed config the CLI loads, and
/// a file-defined rule evaluates exactly like a hand-built one.
#[tokio::test]
async fn a_rule_loaded_from_the_config_file_shape_evaluates() {
    let document = format!(
        r#"{{
          "rules": [
            {{
              "tenant": "{TENANT}",
              "rule_id": "high-cpu",
              "promql": "{METRIC}",
              "condition": {{"type": "threshold", "op": "gt", "value": 0.9}},
              "labels": {{"severity": "page"}},
              "annotations": {{"summary": "cpu is hot"}}
            }}
          ]
        }}"#
    );
    let by_tenant: HashMap<TenantHash, Vec<Rule>> = parse_rules(&document).expect("valid rules");
    let tenant = TenantId::new(TENANT).hash();
    let rules = by_tenant
        .get(&tenant)
        .cloned()
        .expect("rules for the tenant");

    let store = seeded_store().await;
    let mut evaluator = evaluator(Arc::clone(&store), TestClock::at(NOW_NS), rules, Vec::new());
    assert_eq!(evaluator.run_tick().await.records_written, 1);

    let records = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(records[0].state, AlertState::Firing);
    assert_eq!(records[0].rule_id, "high-cpu");
}

/// A rule whose condition does not hold writes nothing at all: an alert that
/// never existed has no state to record.
#[tokio::test]
async fn a_condition_that_never_holds_writes_nothing() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let mut rule = threshold_rule(None);
    rule.condition = RuleCondition::Threshold {
        op: ThresholdOp::Gt,
        threshold: 100.0,
    };
    let mut evaluator = evaluator(
        Arc::clone(&store),
        TestClock::at(NOW_NS),
        vec![rule],
        Vec::new(),
    );

    let report = evaluator.run_tick().await;
    assert_eq!(report.rules_evaluated, 1);
    assert_eq!(report.records_written, 0);
    assert_eq!(alert_object_count(store.as_ref(), tenant).await, 0);
}

/// A restarted evaluator (a fresh `AlertEvaluator` over the same store, no
/// shared in-process state) must fold to the durable latest record and not
/// re-write anything already reflected in history - this is what "Ravel's
/// compute processes are disposable" (ADR-0043 decision 3) actually requires,
/// pinned end to end through pending, firing, and resolved.
#[tokio::test]
async fn a_restarted_evaluator_resumes_from_durable_state_and_writes_nothing_new() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);

    {
        let mut a = evaluator(
            Arc::clone(&store),
            clock.clone(),
            vec![threshold_rule(Some(Duration::from_secs(60)))],
            Vec::new(),
        );
        assert_eq!(a.run_tick().await.records_written, 1, "pending");
        clock.advance(Duration::from_secs(61));
        assert_eq!(a.run_tick().await.records_written, 1, "firing");
        clock.advance(Duration::from_secs(3600));
        assert_eq!(a.run_tick().await.records_written, 1, "resolved (stale)");
    }
    let history = read_alert_records(store.as_ref(), tenant).await;
    assert_eq!(history.len(), 3, "pending, firing, resolved");
    assert_eq!(history[0].state, AlertState::Pending);
    assert_eq!(history[1].state, AlertState::Firing);
    assert_eq!(history[2].state, AlertState::Resolved);
    let one_id = history[0].alert_id;
    assert!(history.iter().all(|r| r.alert_id == one_id));

    // A brand new evaluator instance, no shared state with `a` beyond the
    // store: the fold must land on Resolved (the greatest ts), not re-fire.
    clock.advance(Duration::from_secs(60));
    let mut b = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(Some(Duration::from_secs(60)))],
        Vec::new(),
    );
    let tick = b.run_tick().await;
    assert!(!tick.history_unavailable);
    assert_eq!(
        tick.records_written, 0,
        "a restarted evaluator that folds to Resolved must not re-write anything"
    );
    assert_eq!(read_alert_records(store.as_ref(), tenant).await.len(), 3);
}

/// The regression this fix targets: before `AlertEvaluator::bootstrap_
/// undelivered`, a notification stuck in the in-memory `undelivered` set when
/// the process died was never retried by a fresh evaluator, silently
/// downgrading sink delivery to at-most-once across a restart despite the
/// durable alert record itself being correct. Proves the fix: a fresh
/// evaluator's first tick re-delivers it once the sink is healthy again.
#[tokio::test]
async fn a_restarted_evaluator_redelivers_a_notification_stuck_at_the_old_process() {
    let server = SinkServer::start(StatusCode::INTERNAL_SERVER_ERROR).await;
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let url = format!("{}/hook", server.base_url());

    {
        let mut a = evaluator(
            Arc::clone(&store),
            clock.clone(),
            vec![threshold_rule(None)],
            vec![AlertSink::webhook(url.clone())],
        );
        let first = a.run_tick().await;
        assert_eq!(first.records_written, 1);
        assert_eq!(first.notifications_failed, 1, "sink is 500ing");
    }
    assert_eq!(server.capture.calls().len(), 1);
    assert_eq!(read_alert_records(store.as_ref(), tenant).await.len(), 1);

    // Process "restarts" (fresh evaluator instance); the sink is healthy now.
    server.capture.set_status(StatusCode::OK);
    clock.advance(Duration::from_secs(60));
    let mut b = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![threshold_rule(None)],
        vec![AlertSink::webhook(url)],
    );
    let second = b.run_tick().await;
    assert_eq!(second.records_written, 0, "still firing, no new record");
    assert_eq!(
        second.notifications_delivered, 1,
        "bootstrap_undelivered re-queued the still-firing alert on the first tick"
    );
    assert_eq!(
        server.capture.calls().len(),
        2,
        "at-least-once across a restart: the stuck notification is now delivered"
    );
    server.stop().await;
}

// --- Repeat notifications while firing (ADR-0043 amendment) -----------------

/// Every alertmanager alert object in a captured call, asserting each carries a
/// `startsAt` and never an `endsAt` (the firing re-send shape the amendment's
/// decision 6 requires).
fn assert_firing_shape(calls: &[(String, Json)]) {
    for (path, body) in calls {
        assert_eq!(
            path, "/api/v2/alerts",
            "posted to the alertmanager endpoint"
        );
        let alerts = body.as_array().expect("api/v2/alerts takes an array");
        for alert in alerts {
            assert!(
                alert["startsAt"].is_string(),
                "a firing send carries startsAt: {alert:?}"
            );
            assert!(
                alert.get("endsAt").is_none(),
                "a firing repeat must never carry endsAt: {alert:?}"
            );
        }
    }
}

/// Exit criterion 1: a rule held firing across more than one default
/// Alertmanager `resolve_timeout` (5 minutes) re-notifies at the repeat cadence
/// (1 minute), so the notification count grows and Alertmanager never sees the
/// silence that would false-clear the alert. Every send is a firing re-send
/// (`startsAt`, no `endsAt`) and no repeat writes a durable record.
#[tokio::test]
async fn a_persistently_firing_rule_repeats_across_the_resolve_timeout() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store_spanning(NOW_NS + 420 * NS_PER_SEC).await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let mut evaluator = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![repeat_rule(Some(Duration::from_secs(60)))],
        vec![AlertSink::alertmanager(server.base_url())],
    );

    // Onset: one firing transition, delivered once, no repeat yet.
    let onset = evaluator.run_tick().await;
    assert_eq!(onset.records_written, 1, "the onset is a transition");
    assert_eq!(onset.notifications_delivered, 1);
    assert_eq!(
        onset.repeats_queued, 0,
        "window 0 is the onset, not a repeat"
    );

    // Tick once a minute across a >5-minute firing span. Each minute is a new
    // repeat window; none writes a record.
    let mut repeats = 0;
    for _ in 0..6 {
        clock.advance(Duration::from_secs(60));
        let tick = evaluator.run_tick().await;
        assert_eq!(
            tick.records_written, 0,
            "a repeat is a re-send, never a durable record"
        );
        repeats += tick.repeats_queued;
    }
    assert!(
        repeats >= 5,
        "at least five repeats inside the 5-minute resolve timeout, got {repeats}"
    );

    // Nothing was written for the repeats: the whole episode is one record.
    assert_eq!(
        read_alert_records(store.as_ref(), tenant).await.len(),
        1,
        "the durable history is one firing record; repeats add none"
    );

    // Onset plus every repeat crossed the socket as a firing re-send.
    let calls = server.capture.calls();
    assert!(
        calls.len() >= 6,
        "onset plus repeats delivered, got {} calls",
        calls.len()
    );
    assert_firing_shape(&calls);

    server.stop().await;
}

/// Exit criterion 2: a restart mid-firing-episode resumes repeats on the
/// schedule derived from the durable record, not reset to zero. A fresh
/// evaluator re-derives the window index from the firing record's own timestamp
/// (`k = (now - record.ts_ns) / repeat_interval`), so its first repeat lands on
/// the current window, not on window 1. The proof that the schedule is
/// record-derived and not an in-memory timer reset to zero: the restart's own
/// tick already queues a repeat (a from-zero timer would compute window 0 there
/// and stay silent until a full interval later). `bootstrap_undelivered` covers
/// the restart instant regardless of the lease, so there is never a silence,
/// and the two compose into a single send (one entry per `alert_id`).
///
/// The dead process's lease lingers in the store, so the new sole replica only
/// repeats once it takes that expired lease over -- realistic single-replica
/// restart behavior. The clock is advanced past the old lease's expiry so the
/// new replica legitimately holds it.
#[tokio::test]
async fn repeats_resume_from_the_durable_record_after_a_restart() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store_spanning(NOW_NS + 400 * NS_PER_SEC).await;
    let clock = TestClock::at(NOW_NS);

    {
        let mut a = evaluator(
            Arc::clone(&store),
            clock.clone(),
            vec![repeat_rule(Some(Duration::from_secs(60)))],
            vec![AlertSink::alertmanager(server.base_url())],
        );
        assert_eq!(a.run_tick().await.records_written, 1, "onset fires");
        clock.advance(Duration::from_secs(60));
        assert_eq!(a.run_tick().await.repeats_queued, 1, "window 1 repeats");
    }
    let before_restart = server.capture.calls().len();

    // Process "restarts": fresh in-memory state, same durable store. Advance
    // past the dead replica's lease expiry (3 * 60s from its last renewal at
    // t+60s, i.e. t+240s) so the new replica can take the lease and repeat.
    clock.advance(Duration::from_secs(200)); // now t+260s => window 4 of the record
    let mut b = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![repeat_rule(Some(Duration::from_secs(60)))],
        vec![AlertSink::alertmanager(server.base_url())],
    );
    let restart = b.run_tick().await;
    assert_eq!(
        restart.records_written, 0,
        "a restart mid-episode writes no new record"
    );
    assert_eq!(
        restart.repeats_queued, 1,
        "the restart tick itself repeats on the record-derived window (4), proving the \
         schedule is not reset to zero"
    );
    assert_eq!(
        server.capture.calls().len() - before_restart,
        1,
        "at most one duplicate across the restart: bootstrap and the repeat compose to one send"
    );

    // The schedule continues from the record: the next window fires with no gap.
    clock.advance(Duration::from_secs(60)); // t+320s => window 5
    assert_eq!(
        b.run_tick().await.repeats_queued,
        1,
        "the next window repeats on schedule; no gap after the restart"
    );

    assert_firing_shape(&server.capture.calls());
    server.stop().await;
}

/// Exit criterion 3: a resolve-then-refire cycle re-anchors the repeat schedule
/// on the new episode's firing record. The old episode's window mark (here
/// window 4) does not suppress the new episode's first repeat (window 1), which
/// is exactly the anchor-staleness bug the ADR's self-review caught. The old
/// episode reaching a strictly higher window than the new one's first repeat is
/// what makes this non-vacuous: a numeric-only mark would suppress it.
#[tokio::test]
async fn a_refire_is_not_suppressed_by_the_old_episodes_window_mark() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT).hash();
    // One firing sample at onset, then a >5-minute gap (the alert resolves),
    // then a fresh burst that refires the alert on a new episode.
    publish_metric(
        store.as_ref(),
        &TenantId::new(TENANT),
        &[
            (NOW_NS - 30 * NS_PER_SEC, 1.0),
            (NOW_NS + 400 * NS_PER_SEC, 1.0),
            (NOW_NS + 460 * NS_PER_SEC, 1.0),
            (NOW_NS + 520 * NS_PER_SEC, 1.0),
        ],
    )
    .await;
    let clock = TestClock::at(NOW_NS);
    let mut ev = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![repeat_rule(Some(Duration::from_secs(60)))],
        Vec::new(),
    );

    // Old episode fires and repeats through window 4 (the onset sample stays
    // fresh for 5 minutes).
    assert_eq!(ev.run_tick().await.records_written, 1, "old episode fires");
    let mut old_repeats = 0;
    for _ in 0..4 {
        clock.advance(Duration::from_secs(60));
        old_repeats += ev.run_tick().await.repeats_queued;
    }
    assert_eq!(
        old_repeats, 4,
        "the old episode reached window 4 before resolving"
    );

    // Onset sample now stale (age > 5 minutes) and the refire burst not yet in
    // range: the alert resolves.
    clock.advance(Duration::from_secs(60)); // NOW + 300s
    assert_eq!(
        ev.run_tick().await.records_written,
        1,
        "the old episode resolves"
    );
    assert_eq!(
        ev.run_tick().await.repeats_queued,
        0,
        "a resolved alert never repeats"
    );

    // Refire: the burst at NOW+400s is now in range, opening a new episode.
    clock.advance(Duration::from_secs(120)); // NOW + 420s, sample at 400s is fresh
    let refire = ev.run_tick().await;
    assert_eq!(refire.records_written, 1, "the alert refires");
    assert_eq!(
        refire.repeats_queued, 0,
        "window 0 of the new episode is the transition, not a repeat"
    );

    // Window 1 of the NEW episode. The stale mark sits at window 4 of the old
    // episode; without the anchor-staleness check, 4 >= 1 would suppress this
    // send and the refired alert would silently never repeat.
    clock.advance(Duration::from_secs(60)); // NOW + 480s
    assert_eq!(
        ev.run_tick().await.repeats_queued,
        1,
        "the refired episode re-anchors and repeats despite the old window-4 mark"
    );

    // Exactly two firing records exist in history (old onset, new onset); no
    // repeat wrote one.
    let records = read_alert_records(store.as_ref(), tenant).await;
    let firing = records
        .iter()
        .filter(|r| r.state == AlertState::Firing)
        .count();
    assert_eq!(firing, 2, "two firing transitions, no repeat records");
}

/// Exit criterion 4: a tick less than one `repeat_interval` past the firing
/// record queues no repeat and sends nothing.
#[tokio::test]
async fn a_quiet_tick_within_the_interval_does_not_repeat() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store().await;
    let clock = TestClock::at(NOW_NS);
    let mut ev = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![repeat_rule(Some(Duration::from_secs(60)))],
        vec![AlertSink::alertmanager(server.base_url())],
    );

    assert_eq!(ev.run_tick().await.notifications_delivered, 1, "onset sent");
    assert_eq!(server.capture.calls().len(), 1);

    // Half an interval later: still firing, but no repeat window has elapsed.
    clock.advance(Duration::from_secs(30));
    let quiet = ev.run_tick().await;
    assert_eq!(quiet.repeats_queued, 0, "no window elapsed, no repeat");
    assert_eq!(quiet.notifications_delivered, 0, "nothing to deliver");
    assert_eq!(
        server.capture.calls().len(),
        1,
        "the notification count did not grow on a quiet tick"
    );

    server.stop().await;
}

/// Exit criterion 5: a repeat whose delivery fails stays in `undelivered` and
/// retries on the next tick, without corrupting the window mark into either a
/// flood (re-queuing every tick) or a permanent gap (never sending again).
#[tokio::test]
async fn a_failed_repeat_retries_without_flooding_or_gapping() {
    let server = SinkServer::start(StatusCode::INTERNAL_SERVER_ERROR).await;
    let store = seeded_store().await;
    let clock = TestClock::at(NOW_NS);
    let mut ev = evaluator(
        Arc::clone(&store),
        clock.clone(),
        vec![repeat_rule(Some(Duration::from_secs(60)))],
        vec![AlertSink::webhook(format!("{}/hook", server.base_url()))],
    );

    // Onset fails to deliver; the firing notification is stuck in undelivered.
    let onset = ev.run_tick().await;
    assert_eq!(onset.records_written, 1);
    assert_eq!(onset.notifications_failed, 1, "sink is 500ing");
    let after_onset = server.capture.calls().len();

    // Window 1, still failing: one repeat queued, one failed delivery, one POST.
    clock.advance(Duration::from_secs(60));
    let w1 = ev.run_tick().await;
    assert_eq!(w1.repeats_queued, 1, "window 1 repeat queued");
    assert_eq!(w1.records_written, 0, "no durable record for a repeat");
    assert_eq!(w1.notifications_failed, 1, "the repeat delivery failed");
    let after_w1 = server.capture.calls().len();
    assert_eq!(
        after_w1,
        after_onset + 1,
        "the failed repeat was attempted once"
    );

    // Still window 1 (advance < interval), still failing: the mark is not
    // re-queued (no flood), but the stuck entry retries.
    clock.advance(Duration::from_secs(30));
    let quiet = ev.run_tick().await;
    assert_eq!(
        quiet.repeats_queued, 0,
        "the same window does not re-queue: no flood into undelivered"
    );
    assert_eq!(quiet.notifications_failed, 1, "the stuck repeat retries");
    assert_eq!(
        server.capture.calls().len(),
        after_w1 + 1,
        "the retry crossed the socket rather than being skipped"
    );

    // Sink recovers; window 2: the mark advances by exactly one and the stuck
    // notification finally drains. No permanent gap.
    server.capture.set_status(StatusCode::OK);
    clock.advance(Duration::from_secs(30)); // NOW + 120s => window 2
    let w2 = ev.run_tick().await;
    assert_eq!(w2.repeats_queued, 1, "window 2 repeats: no permanent gap");
    assert_eq!(
        w2.notifications_delivered, 1,
        "the repeat finally delivered"
    );
    assert_eq!(w2.notifications_failed, 0);

    server.stop().await;
}

/// Exit criterion 6 (pending): a `Pending` alert never repeats, even once more
/// than `repeat_interval` has elapsed, because `for` exists precisely to keep
/// pending quiet.
#[tokio::test]
async fn a_pending_alert_does_not_repeat() {
    let store = seeded_store().await;
    let tenant = TenantId::new(TENANT).hash();
    let clock = TestClock::at(NOW_NS);
    let rule = Rule {
        for_duration: Some(Duration::from_secs(600)),
        repeat_interval: Some(Duration::from_secs(60)),
        ..threshold_rule(None)
    };
    let mut ev = evaluator(Arc::clone(&store), clock.clone(), vec![rule], Vec::new());

    assert_eq!(ev.run_tick().await.records_written, 1, "onset is pending");
    assert_eq!(
        read_alert_records(store.as_ref(), tenant).await[0].state,
        AlertState::Pending
    );

    // Well past repeat_interval but still inside `for`: still pending, no repeat.
    clock.advance(Duration::from_secs(120));
    let tick = ev.run_tick().await;
    assert_eq!(tick.records_written, 0, "still pending, no transition");
    assert_eq!(tick.repeats_queued, 0, "a pending alert never repeats");
}

/// Exit criterion 6 (removed rule): an alert whose rule is gone from the config
/// never repeats. The removed rule is not iterated, so no repeat is ever queued;
/// `bootstrap_undelivered` still redelivers the still-firing alert exactly once
/// across the restart (that is bootstrap, not a repeat), after which it falls
/// silent for Alertmanager to auto-resolve.
#[tokio::test]
async fn a_rule_removed_from_config_never_repeats() {
    let server = SinkServer::start(StatusCode::OK).await;
    let store = seeded_store().await;
    let clock = TestClock::at(NOW_NS);

    {
        let mut a = evaluator(
            Arc::clone(&store),
            clock.clone(),
            vec![repeat_rule(Some(Duration::from_secs(60)))],
            vec![AlertSink::alertmanager(server.base_url())],
        );
        assert_eq!(a.run_tick().await.records_written, 1, "the alert fires");
    }
    let after_fire = server.capture.calls().len();

    // Rule removed: a fresh evaluator with an empty rule set, same firing store.
    clock.advance(Duration::from_secs(60));
    let mut b = evaluator(
        Arc::clone(&store),
        clock.clone(),
        Vec::new(),
        vec![AlertSink::alertmanager(server.base_url())],
    );
    let restart = b.run_tick().await;
    assert_eq!(
        restart.repeats_queued, 0,
        "a removed rule is not iterated, so it never repeats"
    );
    let after_bootstrap = server.capture.calls().len();
    assert_eq!(
        after_bootstrap,
        after_fire + 1,
        "bootstrap redelivers the still-firing alert exactly once across the restart"
    );

    // Many intervals later: no repeat, no further send.
    clock.advance(Duration::from_secs(300));
    let later = b.run_tick().await;
    assert_eq!(
        later.repeats_queued, 0,
        "still no repeat for a removed rule"
    );
    assert_eq!(
        server.capture.calls().len(),
        after_bootstrap,
        "no repeat send after the single bootstrap redelivery"
    );

    server.stop().await;
}
