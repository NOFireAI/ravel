//! Deliverable 4 (issues #445, #502): proves a server built from a
//! cache-enabled `Cli` config actually has the ADR-0046 read cache attached
//! on the log (SQL `logs` table) path, not just that construction succeeds.
//!
//! The cache is asserted empty before any query and non-empty after a real
//! SQL query over a published RLOG object: only `LogSegmentFetcher`'s
//! `fetch_accounted_with_tenant`, reached through `LogsScanExec` after
//! `ravel_server::query::build_sql_state`'s `with_cache` call, can have
//! populated it.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_server::store::build_cache;
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// A clock frozen at [`NOW_NS`]. `build_sql_state` defaults to a real
/// `SystemClock`; left as-is, this test's `start: 0.0` window would list one
/// prefix per (shard, ingest-hour) from the epoch to the real current hour
/// (about half a million LISTs) and now trip the issue #635 window-cost
/// ceiling with a 422. Freezing the clock keeps the resolved listing window an
/// ordinary four hours: the epoch-width span was incidental to what this test
/// asserts (cache attachment on the log path), not part of it, so narrowing it
/// is not "changing the test to fit the code".
struct FixedClock;
impl ravel_ingest::Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn cache_enabled_cli() -> Cli {
    Cli::try_parse_from(["ravel-server"]).expect("default flags parse")
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

/// Publish one real RLOG object plus its `Signal::Logs` commit record,
/// mirroring `ravel-ingest`'s log shard actor (see
/// `crates/ravel-sql/tests` fixtures and `tests/sql_endpoint.rs`).
async fn publish_log_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[LogRecord],
) {
    let tenant_hash = tenant.hash();
    let mut min_event_ts_ns = i64::MAX;
    let mut max_event_ts_ns = i64::MIN;
    let mut streams = std::collections::HashSet::new();
    for rec in records {
        min_event_ts_ns = min_event_ts_ns.min(rec.ts_ns);
        max_event_ts_ns = max_event_ts_ns.max(rec.ts_ns);
        streams.insert(rec.stream_id);
    }

    let writer_id = Uuid::new_v4();
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
        segment_format_version: 1,
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

async fn post_sql(app: &Router, token: &str, query: &str) -> (StatusCode, serde_json::Value) {
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
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

#[tokio::test]
async fn cache_enabled_config_attaches_cache_to_the_log_path() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme");
    publish_log_segment(
        store.as_ref(),
        &tenant,
        &[log_record("checkout", 100, "connection timeout")],
    )
    .await;

    let cli = cache_enabled_cli();
    let cache = build_cache(&cli).expect("cache enabled by default in a cache-enabled config");
    assert!(
        cache.is_empty(),
        "sanity: nothing has been fetched through this cache yet"
    );

    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let catalog = build_catalog(
        Arc::clone(&store),
        1,
        cli.disable_cache,
        cli.cache_max_bytes,
    )
    .expect("catalog");
    let mut sql_state = build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache.clone()),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
    )
    .expect("sql state");
    // Freeze the clock so the epoch-start window resolves to an ordinary
    // four-hour span rather than tripping the issue #635 cost ceiling.
    sql_state.clock = Arc::new(FixedClock);
    let app = ravel_server::sql::router(sql_state);

    let (status, body) =
        post_sql(&app, "acme-token", "SELECT ts, body FROM logs ORDER BY ts").await;
    assert_eq!(status, StatusCode::OK, "sql query failed: {body}");

    assert!(
        !cache.is_empty(),
        "a logs SQL query over a real RLOG object must populate the cache \
         attached via build_sql_state's with_cache call -- an empty cache \
         after a successful query means the log path never actually \
         consulted it"
    );
}
