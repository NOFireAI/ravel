//! Traceability rows `ClientRetry` / `DuplicateUnreachable`.
//! docs/consistency-model.md crash-matrix row "Ack round times out": "metrics
//! collapse the re-ingest by (series_id, ts); logs and spans have no
//! query-time dedup, so a retry duplicates the rows/spans." This is
//! documented at-least-once behavior for logs and spans, not a bug: a client
//! retry after a lost ack causes both copies to be stored, and a query
//! returns the record twice.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{IngestConfig, LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_logseg::{LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::record::StatusCode;
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery};
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantHash};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn config(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

fn log_record(body: &str, ts_ns: i64) -> NormalizedLogRecord {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&res, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs: ravel_logseg::stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn span(trace_id: [u8; 16], start_ns: i64) -> NormalizedSpan {
    NormalizedSpan {
        trace_id,
        span_id: [1u8; 8],
        parent_span_id: None,
        name: "handle".to_string(),
        start_ts_ns: start_ns,
        end_ts_ns: start_ns + 100,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// Follows a commit token to its RLOG object and returns every record an
/// unfiltered scan yields.
async fn read_back_logs(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    token: &CommitToken,
) -> Vec<LogRecord> {
    let commit_key =
        keys::commit_key_for_token(tenant_hash, Signal::Logs, token).expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let rec = record::decode(&commit_bytes).expect("decode commit record");
    let data_bytes = store
        .get(&rec.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data;
    let reader = RlogReader::new(&data_bytes, &RlogConfig::default()).expect("open rlog");
    let (records, _stats) = reader
        .scan(&Predicate::And(Vec::new()))
        .expect("unfiltered scan");
    records
}

/// Follows a commit token to its RSPAN object and returns every span for
/// `trace_id` an unfiltered-by-time scan yields.
async fn read_back_spans(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    token: &CommitToken,
    trace_id: [u8; 16],
) -> Vec<ravel_rspan::record::SpanRecord> {
    let commit_key =
        keys::commit_key_for_token(tenant_hash, Signal::Spans, token).expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let rec = record::decode(&commit_bytes).expect("decode commit record");
    let data_bytes = store
        .get(&rec.object_key, GetRange::Full)
        .await
        .expect("get data object")
        .data;
    let reader = RspanReader::new(&data_bytes, &RspanConfig::default()).expect("open rspan");
    let (records, _stats) = reader
        .scan(&SpanQuery::trace(trace_id, i64::MIN, i64::MAX))
        .expect("trace scan");
    records
}

/// A client that never observes its ack retries the identical log record.
/// Logs have no query-time dedup (docs/consistency-model.md): both copies
/// land as separate commits, and reading them back serves the record twice.
#[tokio::test]
async fn logs_and_spans_lost_ack_retry_serves_the_record_twice_logs() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = LogIngestRouter::new(config(1), Arc::clone(&store), clock.clone());

    let tid = tenant("acme");
    let build = || vec![log_record("checkout failed", BASE_NS - 60_000_000_000)];

    let first = router
        .write(
            tid.clone(),
            build(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first write commits");
    // The client never observes `first`'s ack and retries the exact payload.
    let retry = router
        .write(
            tid.clone(),
            build(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("retried write also commits");
    assert_ne!(
        first.tokens, retry.tokens,
        "a client-level retry is a brand new flush with its own commit"
    );
    router.shutdown().await;

    let mut bodies: Vec<String> = Vec::new();
    for token in first.tokens.iter().chain(retry.tokens.iter()) {
        for rec in read_back_logs(store.as_ref(), &tid.hash(), token).await {
            bodies.push(rec.body);
        }
    }
    assert_eq!(
        bodies,
        vec!["checkout failed".to_string(), "checkout failed".to_string()],
        "logs have no query-time dedup: a lost-ack retry serves the record twice"
    );
}

/// Same lost-ack retry over spans: no query-time dedup, so both copies of
/// the identical span are served.
#[tokio::test]
async fn logs_and_spans_lost_ack_retry_serves_the_record_twice_spans() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = SpanIngestRouter::new(config(1), Arc::clone(&store), clock.clone());

    let tid = tenant("acme");
    let trace_id = [7u8; 16];
    let build = || vec![span(trace_id, BASE_NS - 60_000_000_000)];

    let first = router
        .write(
            tid.clone(),
            build(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first write commits");
    let retry = router
        .write(
            tid.clone(),
            build(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("retried write also commits");
    assert_ne!(
        first.tokens, retry.tokens,
        "a client-level retry is a brand new flush with its own commit"
    );
    router.shutdown().await;

    let mut names: Vec<String> = Vec::new();
    for token in first.tokens.iter().chain(retry.tokens.iter()) {
        for rec in read_back_spans(store.as_ref(), &tid.hash(), token, trace_id).await {
            names.push(rec.name);
        }
    }
    assert_eq!(
        names,
        vec!["handle".to_string(), "handle".to_string()],
        "spans have no query-time dedup: a lost-ack retry serves the span twice"
    );
}
