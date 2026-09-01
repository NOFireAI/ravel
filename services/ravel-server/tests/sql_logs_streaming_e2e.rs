//! ADR-0087 reachability gate: a full-table analytical query over a wide,
//! large `logs` table completes inside a small SQL memory pool, driven through
//! the real `POST /api/v1/sql` handler.
//!
//! This is the test that fails against the pre-ADR-0087 scan, and it has to
//! live here rather than in `ravel-sql` because the failure it pins is a
//! property of the whole request path: the endpoint installs the per-query
//! `TenantDelegatingPool` (`SqlConfig::query_pool`), the executor drains the
//! plan's stream into a response, and only that combination shows a scan's
//! memory behaviour as a client-visible success or failure.
//!
//! The fixture is deliberately hostile in exactly the way the ADR describes:
//! [`ATTRS_PER_RECORD`] dynamic attributes on every one of [`TOTAL_RECORDS`]
//! records. Against the old collect-then-emit scan both queries fail: every
//! column was decoded regardless of projection, the whole partition was
//! collected in row form before any batch was emitted (a stage that was never
//! charged against the pool at all), and the per-batch `try_grow` only ever
//! grew, so the cumulative charge crossed an 8 MiB pool within the first few
//! thousand rows. Against the streaming, column-projecting scan neither query
//! decodes an attribute column at all, and the reservation tracks one block
//! plus one batch.
//!
//! Both a `COUNT(*)` (no projected column at all) and a `GROUP BY` over a
//! projected fixed column are exercised: the first is the degenerate case the
//! ADR's context paragraph names, the second proves a real aggregate over a
//! projected column still works under the same ceiling.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_server::sql::{SqlState, router};
use ravel_sql::{SqlConfig, SqlExecutor};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: `Catalog::resolve` issues one LIST per (shard,
/// ingest-hour) pair across the window.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Dynamic attribute columns on every record. The ADR's motivating figure is
/// "roughly 100 dynamic attributes per record"; this sits just above it.
const ATTRS_PER_RECORD: usize = 100;
/// RLOG objects in the fixture.
const OBJECTS: usize = 30;
/// Records per object. Kept modest because `RlogWriter` buffers a whole
/// object's records before `finish()`, so this, not `TOTAL_RECORDS`, bounds the
/// fixture builder's own peak memory.
const RECORDS_PER_OBJECT: usize = 10_000;
/// Total records in the `logs` table under test.
const TOTAL_RECORDS: usize = OBJECTS * RECORDS_PER_OBJECT;

/// Severity texts cycled across records, so the `GROUP BY` has several real
/// groups with known counts rather than one.
const SEVERITIES: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG"];

/// The per-query DataFusion memory-pool ceiling for this test: 8 MiB, a
/// thirty-second of the 256 MiB default. The fixture's cumulative emitted
/// bytes are orders of magnitude past this, so a scan whose reservation tracks
/// cumulative output cannot pass however many batches it splits into.
const SMALL_POOL_BYTES: usize = 8 * 1024 * 1024;
/// The tenant-wide ceiling behind the per-query pool. Above the per-query
/// ceiling (it is shared across a tenant's concurrent queries) but still small,
/// so it cannot silently become the thing under test.
const SMALL_TENANT_BYTES: usize = 32 * 1024 * 1024;

/// A clock frozen at [`NOW_NS`].
struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn identity(index: usize, tenant_hash: [u8; 16], writer_id: Uuid) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    }
}

/// The [`ATTRS_PER_RECORD`] dynamic attributes every fixture record carries,
/// built once and cloned per record.
///
/// The attribute *values* are shared across records on purpose (they compress
/// away, keeping the objects small); what matters to this test is the number of
/// distinct attribute *columns*, since that is what an unprojected decode has
/// to materialize per row. Building the vector once is purely a fixture-cost
/// choice: at 100 attributes over 300,000 records, formatting the keys and
/// values per record dominates the whole test's wall clock.
fn wide_attrs() -> Vec<(String, AttrValue)> {
    (0..ATTRS_PER_RECORD)
        .map(|i| {
            (
                format!("attr.{i:03}"),
                AttrValue::Str(format!("value-{i:03}")),
            )
        })
        .collect()
}

/// One record of the wide fixture: a fixed stream, a cycling severity, and
/// `attrs` (see [`wide_attrs`]).
fn wide_record(
    ts: i64,
    stream_id: ravel_types::logstream::LogStreamId,
    stream_attrs: &[u8],
    attrs: &[(String, AttrValue)],
) -> LogRecord {
    LogRecord {
        stream_id,
        stream_attrs: stream_attrs.to_vec(),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: SEVERITIES[(ts as usize) % SEVERITIES.len()].to_string(),
        body: format!("request {ts} completed"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs.to_vec(),
    }
}

/// Publish one RLOG object of `count` wide records starting at `first_ts`, plus
/// its `Signal::Logs` commit record, exactly as `ravel-ingest`'s log shard
/// actor does. Records are generated and pushed one at a time so the fixture
/// never holds the whole table in row form.
async fn publish_wide_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    first_ts: i64,
    count: usize,
) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(9_000 + index as u128);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        identity(index, tenant_hash.0, writer_id),
    );
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let attrs = wide_attrs();
    for i in 0..count {
        let rec = wide_record(first_ts + i as i64, stream_id, &stream_attrs, &attrs);
        writer.push(rec).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let min_event_ts_ns = first_ts;
    let max_event_ts_ns = first_ts + count as i64 - 1;

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: count as u64,
        // One stream: every fixture record shares the same resource attributes.
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        // The version the RlogWriter above actually stamped. A commit record
        // that names another one is refused before any GET is issued
        // (ADR-0892 decision 4), so this cannot be a placeholder.
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10 + index as i64,
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

/// The real `/api/v1/sql` router, with the per-query memory pool capped at
/// [`SMALL_POOL_BYTES`]. This is the whole point of driving the test through
/// the endpoint: `SqlConfig::max_query_bytes` is consumed where the endpoint
/// builds the query's `RuntimeEnv`, so the ceiling under test is the one
/// production installs.
fn build_router(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let config = SqlConfig {
        max_query_bytes: SMALL_POOL_BYTES,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        config,
        SMALL_TENANT_BYTES,
    );
    router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        // Generous: this test is about memory, and a debug-build scan over
        // 300k records must not be failed by a wall clock instead.
        max_deadline: Duration::from_secs(600),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

async fn post_json(app: &Router, token: &str, query: &str) -> (StatusCode, Value) {
    let payload = serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "timeout": 540.0,
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec();
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// The index of `name` in the response's column list.
fn column_index(value: &Value, name: &str) -> usize {
    value["data"]["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {value}"))
        .iter()
        .position(|c| c["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("no column {name} in {value}"))
}

/// The rows of a successful response, as positional JSON arrays.
fn rows(value: &Value) -> &Vec<Value> {
    value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows in {value}"))
}

/// The query's own reported peak reserved bytes (ADR-0044
/// `peak_intermediate_bytes`), the high-water mark of the per-query pool.
fn peak_intermediate_bytes(value: &Value) -> u64 {
    value["stats"]["accounting"]["peakIntermediateBytes"]
        .as_u64()
        .unwrap_or_else(|| panic!("no peakIntermediateBytes in {value}"))
}

/// The acceptance test (ADR-0087): a full-table `COUNT(*)` and a full-table
/// `GROUP BY` over 300,000 records carrying 100 dynamic attributes each both
/// return correct results through `POST /api/v1/sql` under an 8 MiB per-query
/// memory pool.
///
/// Failing this means the scan is holding, or charging for, memory proportional
/// to the table rather than to a block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wide_full_table_aggregates_complete_within_a_small_memory_pool() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    for index in 0..OBJECTS {
        let first_ts = (index * RECORDS_PER_OBJECT) as i64 + 1;
        publish_wide_object(store.as_ref(), &tenant, index, first_ts, RECORDS_PER_OBJECT).await;
    }
    let app = build_router(
        store,
        HashMap::from([("acme-token".to_string(), tenant.clone())]),
    );

    // 1. `COUNT(*)`: no column projected at all. The pre-change scan still
    //    received (and materialized) every column including the 100-attribute
    //    `attrs` map, which is precisely the case the ADR's context paragraph
    //    describes as crossing the pool "well under 100,000 rows".
    let (status, value) = post_json(&app, "acme-token", "SELECT COUNT(*) FROM logs").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "COUNT(*) must complete inside the {SMALL_POOL_BYTES}-byte pool: {value}"
    );
    assert_eq!(value["status"], "success", "{value}");
    let count_rows = rows(&value);
    assert_eq!(count_rows.len(), 1, "COUNT(*) returns one row: {value}");
    assert_eq!(
        count_rows[0][0].as_i64(),
        Some(TOTAL_RECORDS as i64),
        "every record must be counted: {value}"
    );
    // The query's own high-water mark, not just "it did not fail": a scan that
    // charged cumulative output would have reported far more than the pool.
    let peak = peak_intermediate_bytes(&value);
    assert!(
        peak < SMALL_POOL_BYTES as u64,
        "peak reserved bytes {peak} must stay under the {SMALL_POOL_BYTES}-byte          pool, not merely avoid tripping it: {value}"
    );

    // 2. A real aggregate over a projected fixed column. Four groups of known
    //    size, so a partially-drained scan shows up as wrong counts rather than
    //    merely as a different error.
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT severity_text, COUNT(*) AS n FROM logs GROUP BY severity_text",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GROUP BY must complete inside the {SMALL_POOL_BYTES}-byte pool: {value}"
    );
    assert_eq!(value["status"], "success", "{value}");

    let severity_col = column_index(&value, "severity_text");
    let n_col = column_index(&value, "n");
    let group_rows = rows(&value);
    assert_eq!(
        group_rows.len(),
        SEVERITIES.len(),
        "one row per severity: {value}"
    );
    let mut total = 0i64;
    let mut seen: Vec<String> = Vec::new();
    for row in group_rows {
        let severity = row[severity_col].as_str().expect("severity_text");
        let n = row[n_col].as_i64().expect("count");
        assert!(
            SEVERITIES.contains(&severity),
            "unexpected severity {severity} in {value}"
        );
        seen.push(severity.to_string());
        total += n;
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), SEVERITIES.len(), "duplicate groups in {value}");
    assert_eq!(
        total, TOTAL_RECORDS as i64,
        "the groups must account for every record: {value}"
    );
    let peak = peak_intermediate_bytes(&value);
    assert!(
        peak < SMALL_POOL_BYTES as u64,
        "peak reserved bytes {peak} must stay under the {SMALL_POOL_BYTES}-byte          pool: {value}"
    );
}
