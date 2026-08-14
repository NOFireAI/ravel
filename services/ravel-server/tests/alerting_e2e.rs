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
