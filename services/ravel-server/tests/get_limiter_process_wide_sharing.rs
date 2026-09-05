//! ADR-1195: proves the SQL path's three fetcher types (`SegmentFetcher` for
//! `samples`, `LogSegmentFetcher` for `logs`, `SpanSegmentFetcher` for
//! `spans`) share exactly ONE process-wide `GetLimiter`, as `build_sql_state`
//! wires them (`services/ravel-server/src/query.rs`), rather than each owning
//! a private pool.
//!
//! `SegmentFetcher`/`LogSegmentFetcher` expose a `#[cfg(test)]`
//! `get_limiter_for_test()` accessor, but both are `pub(crate)` to
//! `ravel-query`, and `SpanSegmentFetcher` has no such accessor at all: none
//! is reachable from this crate, so an `Arc::ptr_eq` proof is not possible
//! here. This test observes the shared bound from the outside instead: real
//! RSEG/RLOG/RSPAN objects, a real `SqlState` built through the same
//! `build_catalog`/`build_sql_state` the server uses, and a `FaultStore` gate
//! on the data-object GETs (every data key ends in `.rseg` regardless of
//! signal, per `ravel_commit::keys::DATA_SUFFIX`; catalog manifest/commit-record
//! reads end in `.cmt`/other suffixes and are never held). Three concurrent
//! SQL queries, one per table, each need exactly one such GET
//! (`fetch_accounted`'s doc comment: "This funnel issues exactly one
//! whole-object GET per call"). A one-permit limiter must cap the gate's
//! held count at exactly 1 across all three signals at once; a three-permit
//! control must reach exactly 3. Since a fetcher blocked on the limiter's
//! semaphore never reaches the store at all, the other two tasks are simply
//! absent from the gate while one is held -- there is no separate assertion
//! needed to show they did not sneak past it.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::GetLimiter;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_server::store::build_cache;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// A clock frozen at [`NOW_NS`]: keeps the resolved listing window an
/// ordinary four hours instead of an epoch-wide fan-out of LISTs.
struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publish one real RSEG segment plus its `Signal::Metrics` commit record.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
    samples: &[(i64, f64)],
) {
    let tenant_hash = tenant.hash();
    let label_set = labels_for(metric);
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];

    let writer_id = Uuid::from_u128(2_000);
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

fn log_record(service: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(service.to_string()),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
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

/// Publish one real RLOG object plus its `Signal::Logs` commit record.
async fn publish_log_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let tenant_hash = tenant.hash();
    let mut min_event_ts_ns = i64::MAX;
    let mut max_event_ts_ns = i64::MIN;
    let mut streams = std::collections::HashSet::new();
    for rec in records {
        min_event_ts_ns = min_event_ts_ns.min(rec.ts_ns);
        max_event_ts_ns = max_event_ts_ns.max(rec.ts_ns);
        streams.insert(rec.stream_id);
    }

    let writer_id = Uuid::from_u128(9_000);
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for rec in records {
        writer.push(rec.clone()).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: streams.len() as u64,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
}

fn span_record(
    trace: [u8; 16],
    span_id: u8,
    service: &str,
    name: &str,
    start: i64,
    duration_ns: i64,
) -> ravel_rspan::SpanRecord {
    ravel_rspan::SpanRecord {
        trace_id: trace,
        span_id: [span_id; 8],
        parent_span_id: None,
        name: name.to_string(),
        start_ts_ns: start,
        end_ts_ns: start + duration_ns,
        status_code: ravel_rspan::StatusCode::Ok,
        status_message: None,
        attrs: vec![("service.name".to_string(), service.to_string())],
    }
}

/// Publish one real RSPAN object plus its `Signal::Spans` commit record.
async fn publish_span_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[ravel_rspan::SpanRecord],
) {
    let tenant_hash = tenant.hash();
    let mut min_event_ts_ns = i64::MAX;
    let mut max_event_ts_ns = i64::MIN;
    let mut traces = std::collections::HashSet::new();
    for rec in records {
        min_event_ts_ns = min_event_ts_ns.min(rec.start_ts_ns);
        max_event_ts_ns = max_event_ts_ns.max(rec.end_ts_ns);
        traces.insert(rec.trace_id);
    }

    let writer_id = Uuid::from_u128(5_000);
    let identity = ravel_rspan::ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = ravel_rspan::RspanWriter::new(ravel_rspan::RspanConfig::default(), identity);
    for rec in records {
        writer.push(rec.clone());
    }
    let bytes = writer.finish().expect("finish rspan object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: traces.len() as u64,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
}

async fn post_sql(app: &Router, token: &str, query: &str) -> StatusCode {
    let payload = serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    if status != StatusCode::OK {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        panic!(
            "query {query:?} failed: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    status
}

/// Publishes one segment per signal for `tenant`, wraps `store` in a
/// `FaultStore` gating whole-object data GETs (`.rseg`, shared by every
/// signal's data key; commit records and provisioning reads end differently
/// and are never held), and builds a real SQL router sharing `get_limiter`
/// across `SegmentFetcher`/`LogSegmentFetcher`/`SpanSegmentFetcher` exactly as
/// `ravel_server::query::build_sql_state` wires them.
async fn build_gated_app(get_limiter: Arc<GetLimiter>) -> (Router, GateHandle) {
    let tenant = TenantId::new("acme");
    let memory = MemoryStore::new();
    publish_segment(&memory, &tenant, "http_requests_total", &[(100, 1.0), (200, 2.0)]).await;
    publish_log_segment(
        &memory,
        &tenant,
        &[log_record("checkout", 100, "connection timeout")],
    )
    .await;
    publish_span_segment(
        &memory,
        &tenant,
        &[span_record([7; 16], 1, "checkout", "GET /cart", 100, 50_000)],
    )
    .await;

    let fault = Arc::new(FaultStore::new(memory, FaultPlan::default()));
    let gate = fault.hold(Op::Get, Some(".rseg".to_string()), Occurrence::Always);
    let store: Arc<dyn ObjectStoreBackend> = fault;

    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let cache = build_cache(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
        .expect("cache enabled by default");
    let catalog = build_catalog(
        Arc::clone(&store),
        1,
        cli.disable_cache,
        ravel_server::config::DEFAULT_CACHE_MAX_BYTES,
        cli.cache_dir.clone(),
    )
    .expect("catalog");

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let mut sql_state = build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        get_limiter,
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(ravel_query::QueryConcurrencyLimit::Unlimited),
        None,
    )
    .expect("sql state");
    sql_state.clock = Arc::new(FixedClock);
    (ravel_server::sql::router(sql_state), gate)
}

/// Fires the three per-signal queries this test relies on to each issue
/// exactly one gated data-object GET, and returns once all three have been
/// spawned (they block on the gate/limiter, not on completion).
fn spawn_three_signal_queries(
    app: Router,
) -> Vec<tokio::task::JoinHandle<StatusCode>> {
    let queries = [
        "SELECT value FROM samples ORDER BY ts",
        "SELECT ts, body FROM logs ORDER BY ts",
        "SELECT name, service_name FROM spans ORDER BY start_ts",
    ];
    queries
        .into_iter()
        .map(|query| {
            let app = app.clone();
            tokio::spawn(async move { post_sql(&app, "acme-token", query).await })
        })
        .collect()
}

/// ADR-1195: one process-wide `GetLimiter` at 1 permit must cap concurrent
/// data GETs at exactly 1 across all three signals at once. A fetcher
/// blocked on the limiter's own semaphore never reaches the store, so the
/// other two queries are simply invisible to the gate while the first is
/// held -- proving all three share the SAME limiter, not merely that each is
/// individually bounded.
#[tokio::test]
async fn one_shared_permit_bounds_peak_concurrent_gets_across_all_three_signals() {
    let limiter = Arc::new(GetLimiter::new(1).expect("1 permit is valid"));
    let (app, gate) = build_gated_app(limiter).await;
    let handles = spawn_three_signal_queries(app);

    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("one of the three queries issues its GET within 30 s");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gate.held_count(),
        1,
        "one process-wide permit must cap in-flight data GETs at exactly 1 \
         across samples/logs/spans, not merely at more than 0"
    );
    for id in gate.held() {
        assert!(gate.release(id), "held id must release");
    }

    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("releasing the first permit lets a second GET proceed within 30 s");
    assert_eq!(gate.held_count(), 1, "still exactly 1 held at a time");
    for id in gate.held() {
        assert!(gate.release(id), "held id must release");
    }

    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(1))
        .await
        .expect("releasing the second permit lets the third GET proceed within 30 s");
    assert_eq!(gate.held_count(), 1, "still exactly 1 held at a time");
    for id in gate.held() {
        assert!(gate.release(id), "held id must release");
    }

    for (query, handle) in ["samples", "logs", "spans"].into_iter().zip(handles) {
        let status = tokio::time::timeout(Duration::from_secs(30), handle)
            .await
            .unwrap_or_else(|_| panic!("{query} query completes within 30 s"))
            .expect("join query task");
        assert_eq!(status, StatusCode::OK, "{query} query must succeed");
    }
}

/// Control for the test above: the same three signals, the same three
/// concurrent queries, but a 3-permit limiter. Peak in-flight data GETs must
/// reach exactly 3 at once, proving the 1-permit test's bound of 1 comes
/// from the shared limiter's permit count, not from some other
/// serialization (object identity, tokio scheduling, per-signal listing
/// order).
#[tokio::test]
async fn three_shared_permits_allow_peak_concurrent_gets_of_three() {
    let limiter = Arc::new(GetLimiter::new(3).expect("3 permits is valid"));
    let (app, gate) = build_gated_app(limiter).await;
    let handles = spawn_three_signal_queries(app);

    tokio::time::timeout(Duration::from_secs(30), gate.wait_until_held(3))
        .await
        .expect("all three queries issue their GETs within 30 s");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gate.held_count(),
        3,
        "three shared permits must admit exactly 3 concurrent data GETs, not 1"
    );
    for id in gate.held() {
        assert!(gate.release(id), "held id must release");
    }

    for (query, handle) in ["samples", "logs", "spans"].into_iter().zip(handles) {
        let status = tokio::time::timeout(Duration::from_secs(30), handle)
            .await
            .unwrap_or_else(|_| panic!("{query} query completes within 30 s"))
            .expect("join query task");
        assert_eq!(status, StatusCode::OK, "{query} query must succeed");
    }
}
