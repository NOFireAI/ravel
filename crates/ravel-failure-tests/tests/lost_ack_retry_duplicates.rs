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

use common::{TestClock, catalog, tenant};
use ravel_ingest::{IngestConfig, LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_logseg::LogRecord;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_query::{LogQuery, LogSegmentFetcher, SpanSegmentFetcher};
use ravel_rspan::SpanQuery;
use ravel_rspan::record::{SpanRecord, StatusCode};
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantHash, TimeRange};

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const NS_PER_SEC: i64 = 1_000_000_000;

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

/// Discovers every commit for (tenant, `Signal::Logs`) in `range` through the
/// catalog -- the same resolution `ravel-sql`'s logs table provider drives
/// (`Catalog::resolve` -> per-segment `LogSegmentFetcher::fetch`) -- and
/// merges every resolved segment's records into one list. This is the
/// supported log query path: a client retry after a lost ack has no commit
/// token to follow, so a real query discovers both commits by time range,
/// not by token. Two identical commits therefore serve two records unless
/// something on this path dedups across them, which nothing here does
/// (docs/consistency-model.md).
async fn discover_and_read_logs(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    range: TimeRange,
    now_ns: i64,
) -> Vec<LogRecord> {
    let cat = catalog(Arc::clone(&store), 1);
    let snapshot = cat
        .resolve(&tenant_hash, Signal::Logs, range, &[], now_ns)
        .await
        .expect("resolve logs snapshot");
    let fetcher = LogSegmentFetcher::new(store);
    let query = LogQuery::new(range.start_ns, range.end_ns);
    let mut records = Vec::new();
    for seg_ref in &snapshot.segments {
        if let Some(output) = fetcher
            .fetch(seg_ref, &query)
            .await
            .expect("fetch log segment")
        {
            records.extend(output.records);
        }
    }
    records
}

/// Spans counterpart of [`discover_and_read_logs`]: `Catalog::resolve` ->
/// per-segment `SpanSegmentFetcher::fetch`, merged across every resolved
/// commit, matching how a real spans query discovers and reads them.
async fn discover_and_read_spans(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    range: TimeRange,
    now_ns: i64,
    trace_id: [u8; 16],
) -> Vec<SpanRecord> {
    let cat = catalog(Arc::clone(&store), 1);
    let snapshot = cat
        .resolve(&tenant_hash, Signal::Spans, range, &[], now_ns)
        .await
        .expect("resolve spans snapshot");
    let fetcher = SpanSegmentFetcher::new(store);
    let query = SpanQuery::trace(trace_id, range.start_ns, range.end_ns);
    let mut records = Vec::new();
    for seg_ref in &snapshot.segments {
        if let Some(output) = fetcher
            .fetch(seg_ref, &query, None, None, &[])
            .await
            .expect("fetch span segment")
        {
            records.extend(output.records.into_iter().map(|row| row.record));
        }
    }
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

    let event_ts = BASE_NS - 60_000_000_000;
    let range = TimeRange {
        start_ns: event_ts - NS_PER_SEC,
        end_ns: event_ts + NS_PER_SEC,
    };
    let bodies: Vec<String> =
        discover_and_read_logs(Arc::clone(&store), tid.hash(), range, clock.now())
            .await
            .into_iter()
            .map(|rec| rec.body)
            .collect();
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

    let event_ts = BASE_NS - 60_000_000_000;
    let range = TimeRange {
        start_ns: event_ts - NS_PER_SEC,
        end_ns: event_ts + NS_PER_SEC,
    };
    let names: Vec<String> =
        discover_and_read_spans(Arc::clone(&store), tid.hash(), range, clock.now(), trace_id)
            .await
            .into_iter()
            .map(|rec| rec.name)
            .collect();
    assert_eq!(
        names,
        vec!["handle".to_string(), "handle".to_string()],
        "spans have no query-time dedup: a lost-ack retry serves the span twice"
    );
}
