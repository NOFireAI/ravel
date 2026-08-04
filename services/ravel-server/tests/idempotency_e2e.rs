//! Experiment L6 (ADR-0051 §5, epic #452 EB-8): a client retry after a lost
//! ack must not double log rows when the request carries an
//! `x-ravel-idempotency-key`, and must still double (a documented,
//! counter-attributable outcome) when it does not.
//!
//! These drive the transport-agnostic [`handle_export_logs`] directly, over a
//! `MemoryStore` (the semantics oracle) or a `FaultStore` wrapping one, and
//! prove durability the way the log ingest e2e does: by listing and decoding
//! the RLOG objects the write produced, not through an in-process router view.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_ingest::{
    AdmissionController, AdmissionLimits, IngestConfig, LogIngestRouter, SystemClock, WriteMode,
};
use ravel_logseg::{Predicate, RlogConfig, RlogReader};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::LogIngestLimits;
use ravel_server::logs_ingest::{LogIngestState, handle_export_logs};
use ravel_types::{Signal, TenantId};

const TENANT: &str = "acme";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One log record under one resource stream, timestamped at `ts_ns` so the
/// event-time skew bounds admit it.
fn export_request(body: &str, ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(body.to_string())),
                    }),
                    attributes: vec![string_kv("http.route", "/checkout")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn log_state(
    store: Arc<dyn ObjectStoreBackend>,
    admission: Arc<AdmissionController>,
) -> LogIngestState {
    let router = Arc::new(LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            ..IngestConfig::default()
        },
        store.clone(),
        Arc::new(SystemClock),
    ));
    LogIngestState {
        router,
        limits: LogIngestLimits::default(),
        ack_deadline: Duration::from_secs(5),
        admission,
        store,
        recovery: None,
    }
}

/// The `SELECT count(*)`-equivalent: how many log records are durable in the
/// object store right now, decoded straight out of every RLOG object.
async fn durable_row_count(store: &dyn ObjectStoreBackend) -> usize {
    let prefix = format!("t/{}/l/l0/", TenantId::new(TENANT).hash().to_hex());
    let objects = list_all(store, &prefix)
        .await
        .expect("list log data objects");
    let mut count = 0;
    for object in objects {
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get log data object")
            .data;
        let reader = RlogReader::new(&bytes, &RlogConfig::default()).expect("open RLOG object");
        let (scanned, _stats) = reader
            .scan(&Predicate::And(Vec::new()))
            .expect("unfiltered scan");
        count += scanned.len();
    }
    count
}

fn logs_admitted(admission: &AdmissionController) -> u64 {
    admission
        .usage_snapshot()
        .into_iter()
        .find(|u| u.signal == Signal::Logs)
        .map(|u| u.series_admitted_total)
        .unwrap_or(0)
}

/// L6, keyed: the commit lands durably but the marker PUT's ack is lost
/// (`DuplicateDelivery` applies the write, then reports failure), modeling the
/// write completing while the client never learns it did. The retry with the
/// same key must replay the marker the failed PUT nonetheless persisted, so
/// the row count stays at one and the replayed commit-token header is
/// byte-identical to the original.
#[tokio::test]
async fn keyed_retry_after_suppressed_ack_does_not_double_rows() {
    // Suppress the ack on the marker PUT only: data and commit PUTs pass
    // through and stay durable; the marker PUT applies but its caller sees a
    // lost-ack error (the failure mode the ordering guarantee is built for).
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::DuplicateDelivery).with_key_contains("/idem/"),
    );
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let admission = Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        AdmissionLimits::default(),
    ));
    let state = log_state(store.clone(), admission.clone());

    let ts_ns = now_ns();
    let key = b"order-4711".to_vec();

    // First delivery: server-side write is fully durable (data + commit +
    // marker), even though `write_marker` observed a lost ack and logged it.
    let first = handle_export_logs(
        &state,
        TenantId::new(TENANT),
        WriteMode::Strict,
        export_request("checkout completed", ts_ns),
        ts_ns,
        Some(key.clone()),
    )
    .await
    .expect("first keyed delivery commits");
    let first_token = first
        .commit_token_header()
        .expect("a strict commit returns a token");

    assert_eq!(
        durable_row_count(store.as_ref()).await,
        1,
        "the first delivery stored exactly one row"
    );
    assert_eq!(
        fault.fault_count(Op::Put, FaultKind::DuplicateDelivery),
        1,
        "the marker PUT's ack was suppressed exactly once"
    );
    let admitted_after_first = logs_admitted(&admission);
    assert_eq!(
        admitted_after_first, 1,
        "the first delivery ran admission once"
    );

    // The retry: identical request, same key. The client never saw the first
    // ack, so it resends.
    let retry = handle_export_logs(
        &state,
        TenantId::new(TENANT),
        WriteMode::Strict,
        export_request("checkout completed", ts_ns),
        ts_ns,
        Some(key),
    )
    .await
    .expect("the keyed retry replays, never errors");

    assert_eq!(
        durable_row_count(store.as_ref()).await,
        1,
        "the keyed retry replayed the marker and wrote nothing new (L6)"
    );
    assert_eq!(
        retry.commit_token_header().as_deref(),
        Some(first_token.as_str()),
        "the replayed commit-token header matches the original byte-for-byte"
    );
    assert!(
        retry.response.partial_success.is_none(),
        "a replay reports the original outcome, no fresh rejection"
    );
    assert!(
        retry.tokens.is_empty(),
        "a replay issues no new commit of its own"
    );
    assert_eq!(
        logs_admitted(&admission),
        admitted_after_first,
        "the replay skipped admission entirely, so no extra stream admission is charged"
    );
}

/// The unkeyed contract: with no idempotency key the identical retry doubles
/// the rows, and that doubling is attributable to a second full admit-and-write
/// cycle through the admission usage counter. This is the documented
/// at-least-once behavior for callers that opt out, not a bug.
#[tokio::test]
async fn unkeyed_retry_doubles_rows_and_is_attributable_via_counters() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let admission = Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        AdmissionLimits::default(),
    ));
    let state = log_state(store.clone(), admission.clone());

    let ts_ns = now_ns();

    handle_export_logs(
        &state,
        TenantId::new(TENANT),
        WriteMode::Strict,
        export_request("checkout completed", ts_ns),
        ts_ns,
        None,
    )
    .await
    .expect("first unkeyed delivery commits");
    assert_eq!(durable_row_count(store.as_ref()).await, 1);
    assert_eq!(logs_admitted(&admission), 1, "one admit cycle so far");

    // Same request, no key: at-least-once means the lost-ack retry reingests.
    handle_export_logs(
        &state,
        TenantId::new(TENANT),
        WriteMode::Strict,
        export_request("checkout completed", ts_ns),
        ts_ns,
        None,
    )
    .await
    .expect("unkeyed retry reingests");

    assert_eq!(
        durable_row_count(store.as_ref()).await,
        2,
        "an unkeyed retry doubles the rows (documented at-least-once)"
    );
    assert_eq!(
        logs_admitted(&admission),
        2,
        "the doubling is attributable: admission ran a second full cycle"
    );
}
