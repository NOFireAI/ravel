//! Proves the spans fetcher built by `ravel_server::query::build_sql_state`
//! reads a trace by range and is fronted by the process read cache (issue
//! #2754): a `trace_id` lookup over `POST /api/v1/sql` issues only ranged GETs
//! against the RSPAN object, never a whole-object read, and the identical
//! second lookup issues no data GET at all, its scan phase reporting cache
//! hits for exactly the bytes the first lookup fetched.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use clap::Parser;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_server::store::build_cache;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// The trace the lookups ask for. The writer orders spans by trace id, so the
/// lowest id lands in the first block, far ahead of the 64 KiB tail probe.
const TARGET_TRACE: [u8; 16] = [1; 16];
const TRACES: u8 = 40;
const SPANS_PER_TRACE: u8 = 4;

/// See `cache_attachment_logs.rs`: a frozen clock keeps the epoch-start
/// window an ordinary four hours instead of tripping the window-cost ceiling.
struct FixedClock;
impl ravel_ingest::Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

/// A `MemoryStore` that records every GET's key and range.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    /// The ranges GET'd on `key` since the last call, in issue order.
    fn take(&self, key: &str) -> Vec<GetRange> {
        let mut gets = self.gets.lock().expect("get log");
        let (mine, rest): (Vec<_>, Vec<_>) = gets.drain(..).partition(|(k, _)| k == key);
        *gets = rest;
        mine.into_iter().map(|(_, range)| range).collect()
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// `len` hex characters from a fixed xorshift stream: half the entropy of a
/// byte, so zstd cannot shrink the attribute below about half its length.
fn noise(seed: u64, len: usize) -> String {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from_digit((x & 0xF) as u32, 16).expect("a nibble is a hex digit")
        })
        .collect()
}

fn span(trace: u8, n: u8) -> ravel_rspan::SpanRecord {
    let start = NOW_NS - 600 * NS_PER_SEC + i64::from(trace) * NS_PER_SEC + i64::from(n);
    ravel_rspan::SpanRecord {
        trace_id: [trace; 16],
        span_id: [n + 1; 8],
        parent_span_id: None,
        name: format!("span-{trace}-{n}"),
        start_ts_ns: start,
        end_ts_ns: start + 1_000_000,
        status_code: ravel_rspan::StatusCode::Ok,
        status_message: None,
        attrs: vec![
            (
                "payload".to_string(),
                noise(u64::from(trace) << 8 | u64::from(n), 4096),
            ),
            ("service.name".to_string(), "checkout".to_string()),
        ],
    }
}

/// Publishes one RSPAN object of [`TRACES`] traces, one trace per block, and
/// returns its data key and size.
async fn publish_span_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> (String, u64) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(2_754);
    let identity = ravel_rspan::ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let config = ravel_rspan::RspanConfig {
        block_target_records: usize::from(SPANS_PER_TRACE),
        ..ravel_rspan::RspanConfig::default()
    };
    let mut writer = ravel_rspan::RspanWriter::new(config, identity);
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for trace in 1..=TRACES {
        for n in 0..SPANS_PER_TRACE {
            let rec = span(trace, n);
            min_ts = min_ts.min(rec.start_ts_ns);
            max_ts = max_ts.max(rec.end_ts_ns);
            writer.push(rec);
        }
    }
    let bytes = writer.finish().expect("finish rspan object");
    let size = bytes.len() as u64;
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: size,
        content_hash,
        sample_count: u64::from(TRACES) * u64::from(SPANS_PER_TRACE),
        series_count: u64::from(TRACES),
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
    (data_key, size)
}

async fn post_sql(app: &Router, query: &str) -> (StatusCode, Value) {
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
        .header(header::AUTHORIZATION, "Bearer acme-token")
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
    let value: Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// The `stats.phases` entry for the scan phase, as `u64` fields.
fn scan_phase(value: &Value, field: &str) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == "scan")
        .unwrap_or_else(|| panic!("no scan phase in {value}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("scan.{field} is a u64 in {value}"))
}

#[tokio::test]
async fn spans_warm_trace_lookup_is_cache_served() {
    let recorder = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant = TenantId::new("acme");
    let (data_key, size) = publish_span_segment(store.as_ref(), &tenant).await;
    let probe = ravel_query::span_fetcher::DEFAULT_TRACE_PROBE_BYTES;
    assert!(
        size > 2 * probe,
        "the object ({size} bytes) must exceed the tail probe so the trace's \
         block is a GET of its own"
    );

    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let cache = build_cache(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
        .expect("cache enabled by default");
    let mut tokens = HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());
    let catalog = build_catalog(
        Arc::clone(&store),
        1,
        cli.disable_cache,
        ravel_server::config::DEFAULT_CACHE_MAX_BYTES,
        cli.cache_dir.clone(),
        cli.catalog_resolve_concurrency,
        None,
        cli.resolve_flush_cadence()
            .expect("flush cadence resolves")
            .max_flush_delay,
    )
    .expect("catalog");
    let mut sql_state = build_sql_state(
        catalog,
        Arc::clone(&store),
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    )
    .expect("sql state");
    sql_state.clock = Arc::new(FixedClock);
    let app = ravel_server::sql::router(sql_state);
    recorder.take(&data_key);

    let trace_hex: String = TARGET_TRACE.iter().map(|b| format!("{b:02x}")).collect();
    let sql = format!("SELECT name FROM spans WHERE trace_id = '{trace_hex}' ORDER BY name");
    let want_rows = serde_json::json!([["span-1-0"], ["span-1-1"], ["span-1-2"], ["span-1-3"]]);

    let (status, cold) = post_sql(&app, &sql).await;
    assert_eq!(status, StatusCode::OK, "cold lookup failed: {cold}");
    assert_eq!(cold["data"]["rows"], want_rows, "{cold}");
    let cold_gets = recorder.take(&data_key);
    assert!(
        cold_gets.len() >= 2,
        "the tail probe and the trace's block run: {cold_gets:?}"
    );
    let mut cold_bytes = 0;
    for range in &cold_gets {
        let GetRange::Range(start, end) = *range else {
            panic!("a trace lookup reads by range only, got {range:?} in {cold_gets:?}");
        };
        assert!(
            end - start < size,
            "no GET covers the whole object: {range:?} of {size}"
        );
        cold_bytes += end - start;
    }
    assert!(
        cold_gets.contains(&GetRange::Range(size - probe, size)),
        "the first GET is the 64 KiB tail probe: {cold_gets:?}"
    );
    assert!(
        cold_gets
            .iter()
            .any(|r| matches!(*r, GetRange::Range(_, end) if end <= size - probe)),
        "the trace's block run is read ahead of the probe: {cold_gets:?}"
    );
    assert!(cold_bytes < size, "{cold_bytes} of {size} bytes read");
    let cold_requests = cold_gets.len() as u64;
    assert_eq!(scan_phase(&cold, "s3GetRequests"), cold_requests, "{cold}");
    assert_eq!(scan_phase(&cold, "s3GetWireBytes"), cold_bytes, "{cold}");
    assert_eq!(scan_phase(&cold, "cacheMisses"), cold_requests, "{cold}");
    assert_eq!(scan_phase(&cold, "cacheHits"), 0, "{cold}");

    let (status, warm) = post_sql(&app, &sql).await;
    assert_eq!(status, StatusCode::OK, "warm lookup failed: {warm}");
    assert_eq!(warm["data"]["rows"], want_rows, "{warm}");
    let warm_gets = recorder.take(&data_key);
    assert!(
        warm_gets.is_empty(),
        "the warm lookup is served from the read cache: {warm_gets:?}"
    );
    assert_eq!(scan_phase(&warm, "s3GetRequests"), 0, "{warm}");
    assert_eq!(scan_phase(&warm, "s3GetWireBytes"), 0, "{warm}");
    assert_eq!(scan_phase(&warm, "cacheHits"), cold_requests, "{warm}");
    assert_eq!(scan_phase(&warm, "cacheMisses"), 0, "{warm}");
    assert_eq!(scan_phase(&warm, "cacheServedBytes"), cold_bytes, "{warm}");
}
