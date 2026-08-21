//! End-to-end reachability proof for the recent-hours read path (ADR-0073),
//! driven through the real production handlers
//! (`services/ravel-server/src/query.rs`'s `build_app_state`/`build_sql_state`,
//! the same factory functions `ravel_server::start` calls) rather than any
//! unit-level stub. Where narrower tests prove the admission seam and
//! SQL/PromQL parity in isolation,
//! this proves both real HTTP query surfaces actually observe the exemption
//! while a real `IngestRouter` drives the open hour past `max_segments`.
//!
//! The line whose reversion must make this fail:
//! `crates/ravel-query/src/segment_admission.rs`'s `admit`, which counts only
//! `origins.sealed_count` against `config.max_segments`. Reverting it to
//! compare the whole snapshot length (the pre-ADR-0073 shape) makes every hot
//! read below fail with `TooManySegments`, since `SEGMENT_COUNT` (7) exceeds
//! `MAX_SEGMENTS` (3).
#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, Float64Array, TimestampNanosecondArray};
use arrow::ipc::reader::StreamReader;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_ingest::{IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::NormalizedPoint;
use ravel_query::{
    EngineConfig, QueryAdmissionController, QueryConcurrencyLimit, QueryEngine, RequestLimit,
};
use ravel_server::query::{build_app_state, build_sql_state};
use ravel_server::tenant::build_resolver;
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId, TimeRange,
};
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "hot_open_hour";
// Below SEGMENT_COUNT, so the sealed-set cap would refuse every hot read
// below if recent segments were not exempt from it (ADR-0073 decision 2).
const MAX_SEGMENTS: usize = 3;
const SEGMENT_COUNT: usize = 7;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// First instant at which `hour` is sealed (mirrors
/// `crates/ravel-query/tests/recent_hours_admission.rs`).
fn now_at_seal(hour: i64) -> i64 {
    (hour + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

/// A tiny `target_bytes` (copied from
/// `crates/ravel-failure-tests/tests/crash_matrix.rs`'s `config()`) so a
/// single-sample `Strict` write flushes synchronously within the `write`
/// call, one L0 segment (and one `CommitToken`) per write, with no manual
/// wait.
fn ingest_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_max_attempts: 1,
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

/// An exact binary fraction: `format!("{v}")` (the JSON sample-value
/// encoding, `crates/ravel-query/src/http/json.rs`'s `format_value`) round-
/// trips any f64 bit-for-bit, so this choice is not load-bearing for
/// correctness -- it just keeps the printed fixture values readable.
fn sample_value(i: usize) -> f64 {
    1.0 + (i as f64) / 8.0
}

fn label_set() -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels")
}

/// Drives `SEGMENT_COUNT` real strict-mode writes through a real
/// `IngestRouter` against `store`, one flush (and one L0 segment) per write,
/// so the open hour accumulates more L0 segments than `MAX_SEGMENTS`. Every
/// sample lands in the real wall-clock "current" hour (spaced 2 real seconds
/// apart, floored to a whole millisecond so the PromQL JSON API's
/// millisecond-granularity timestamps never collide or round), so it reads
/// back as `Recent` under any handler's own `SystemTime::now()` without a
/// injected clock. Returns every write's commit token (index-aligned with
/// `sample_value`) and the millisecond-floored base timestamp the series
/// started at.
async fn ingest_hot_segments(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_id: &TenantId,
) -> (Vec<CommitToken>, i64) {
    let base_ms = now_ns() / 1_000_000;
    let base_ns = base_ms * 1_000_000;

    let router = IngestRouter::new(
        ingest_config(),
        Arc::clone(&store),
        Signal::Metrics,
        Arc::new(SystemClock),
    );

    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set()).expect("series id");
    let mut tokens = Vec::with_capacity(SEGMENT_COUNT);
    for i in 0..SEGMENT_COUNT {
        let ts_ns = base_ns + (i as i64) * 2 * NS_PER_SEC;
        let point = NormalizedPoint {
            series_id,
            labels: Arc::new(label_set()),
            sample: Sample {
                ts_ns,
                value: sample_value(i),
            },
            is_monotonic_sum: false,
        };
        let receipt = router
            .write(
                tenant_id.clone(),
                vec![point],
                WriteMode::Strict,
                Duration::from_secs(10),
            )
            .await
            .expect("strict write acks");
        assert_eq!(
            receipt.tokens.len(),
            1,
            "tiny target_bytes must flush exactly once per single-sample write"
        );
        tokens.push(receipt.tokens[0].clone());
    }
    router.shutdown().await;
    (tokens, base_ns)
}

fn hot_engine_config(max_s3_requests: RequestLimit) -> EngineConfig {
    EngineConfig {
        max_segments: MAX_SEGMENTS,
        max_s3_requests,
        deadline: Duration::from_secs(30),
        ..EngineConfig::default()
    }
}

fn promql_app(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_id: &TenantId,
    engine_config: EngineConfig,
) -> Router {
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant_id.clone());
    let state = build_app_state(
        catalog,
        store,
        build_resolver(tokens, false),
        None,
        engine_config,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
        None,
        None,
        None,
    );
    ravel_query::http::router(state)
}

fn sql_app(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_id: &TenantId,
    engine_config: EngineConfig,
) -> Router {
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant_id.clone());
    let state = build_sql_state(
        catalog,
        store,
        build_resolver(tokens, false),
        None,
        engine_config,
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
        None,
    )
    .expect("build_sql_state");
    ravel_server::sql::router(state)
}

fn encode_query_param(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn promql_get(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
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
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("parse response json");
    (status, json)
}

/// `(ts_ns, value_bits)` pairs read out of a Prometheus-shaped `matrix`
/// JSON body (`data.result[].values`), reconstructing exact nanoseconds from
/// the seconds-float timestamp: every ingested timestamp here is millisecond-
/// aligned, so rounding to the nearest millisecond recovers it exactly
/// regardless of the `ts_ns as f64 / 1e9` division's rounding
/// (`crates/ravel-query/src/http/json.rs`). The value string round-trips
/// its f64 bit-for-bit (`format_value`'s doc comment).
fn promql_matrix_bits(body: &serde_json::Value) -> Vec<(i64, u64)> {
    let results = body["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("expected matrix result array, got {body}"));
    let mut out = Vec::new();
    for series in results {
        assert_eq!(
            series["metric"]["__name__"], METRIC,
            "unexpected series in result: {series}"
        );
        for pair in series["values"].as_array().expect("values array") {
            let secs = pair[0].as_f64().expect("timestamp seconds");
            let ts_ns = (secs * 1000.0).round() as i64 * 1_000_000;
            let value: f64 = pair[1]
                .as_str()
                .expect("value is a JSON string")
                .parse()
                .expect("value string parses as f64");
            out.push((ts_ns, value.to_bits()));
        }
    }
    out.sort();
    out
}

/// Bit-exact `(ts_ns, value_bits)` pairs read off an Arrow IPC stream
/// (`SELECT ts, value FROM samples ORDER BY ts`), used both for the hot SQL
/// HTTP Arrow response and (via `QueryOutput::to_arrow_ipc`) the low-level
/// post-compaction comparison target -- no JSON round trip anywhere on this
/// path. `ravel-sql` depends on datafusion's own (older) vendored arrow, a
/// distinct Rust type from this crate's direct `arrow` dependency
/// (`services/ravel-server/Cargo.toml`'s comment on that pin), so going
/// through the wire IPC bytes rather than a shared `RecordBatch` type is
/// what lets one extraction function serve both call sites.
fn ipc_bytes_bits(bytes: &[u8]) -> Vec<(i64, u64)> {
    let reader = StreamReader::try_new(bytes, None).expect("ipc reader");
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts column");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value column");
        for i in 0..batch.num_rows() {
            out.push((ts.value(i), value.value(i).to_bits()));
        }
    }
    out.sort();
    out
}

const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

async fn sql_post_arrow(
    app: &Router,
    sql: &str,
    start_secs: f64,
    end_secs: f64,
) -> (StatusCode, Vec<(i64, u64)>) {
    let body = serde_json::json!({
        "query": sql,
        "start": start_secs,
        "end": end_secs,
    });
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .header("accept", ARROW_STREAM_MEDIA_TYPE)
        .body(Body::from(body.to_string()))
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
    if status != StatusCode::OK {
        return (status, Vec::new());
    }
    (status, ipc_bytes_bits(&bytes))
}

/// The open hour's boundaries and the evaluation instant used throughout:
/// `base_ns` anchors the first sample, `hour` is its real-wall-clock hour
/// bucket, and `eval_ns`/`eval_secs` sit strictly after the last sample.
struct HotWindow {
    hour: i64,
    window_start_ns: i64,
    window_end_ns: i64,
    eval_ns: i64,
}

fn hot_window(base_ns: i64) -> HotWindow {
    let hour = base_ns / NS_PER_HOUR;
    HotWindow {
        hour,
        window_start_ns: hour * NS_PER_HOUR,
        window_end_ns: (hour + 1) * NS_PER_HOUR,
        eval_ns: base_ns + (SEGMENT_COUNT as i64) * 2 * NS_PER_SEC,
    }
}

/// Post-compaction comparison target, at the low `QueryEngine`/`SqlExecutor`
/// level (not HTTP): a fresh `Catalog` folds the open hour sealed, then a
/// second, independent `Catalog`/engine pair (ruling out any head-cache
/// carryover from the fold itself) reads the now-fully-sealed hour. Sealing
/// only moves segments from listed-live to fold-derived; it must never
/// change what a query sees (ADR-0073 decision 5).
async fn post_compaction_bits(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_id: &TenantId,
    window: &HotWindow,
) -> (Vec<(i64, u64)>, Vec<(i64, u64)>) {
    let th = tenant_id.hash();
    let seal_now_ns = now_at_seal(window.hour);

    let fold_cat = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
    fold_cat
        .fold(&th, Signal::Metrics, Uuid::new_v4(), seal_now_ns, &[], None)
        .await
        .expect("fold seals the open hour");

    let promql_cat =
        Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(promql_cat, store.clone(), EngineConfig::default());
    let eval_t_ms = window.eval_ns / 1_000_000;
    let (value, _stats) = engine
        .instant_with_stats(
            th,
            &format!("{METRIC}[1h]"),
            eval_t_ms,
            &[],
            seal_now_ns,
            Duration::from_secs(30),
        )
        .await
        .expect("post-compaction PromQL read");
    let promql_bits = match value {
        ravel_promql::Value::Matrix(m) => {
            let mut out: Vec<(i64, u64)> = m
                .iter()
                .flat_map(|(_labels, samples)| samples.iter().map(|s| (s.ts_ns, s.value.to_bits())))
                .collect();
            out.sort();
            out
        }
        other => panic!("expected matrix, got {other:?}"),
    };

    let sql_cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let sql_config = SqlConfig {
        engine: EngineConfig::default(),
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        sql_cat,
        ravel_query::SegmentFetcher::new(store.clone()),
        ravel_query::LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        sql_config,
        1 << 30,
    );
    let outcome = executor
        .execute(
            th,
            &SqlRequest {
                sql: "SELECT ts, value FROM samples ORDER BY ts".to_string(),
                window: TimeRange {
                    start_ns: window.window_start_ns,
                    end_ns: window.window_end_ns,
                },
                min_tokens: vec![],
                now_ns: seal_now_ns,
                deadline: Duration::from_secs(30),
            },
        )
        .await
        .expect("post-compaction SQL read");
    let sql_bits = ipc_bytes_bits(&outcome.output.to_arrow_ipc().expect("arrow ipc encode"));

    (promql_bits, sql_bits)
}

/// Deliverable 1: sustaining flushes past `MAX_SEGMENTS`-worth of L0 objects
/// in the open hour, real PromQL and SQL reads over real HTTP must keep
/// succeeding and must return results bit-identical to a post-compaction
/// read of the same data.
#[tokio::test]
async fn recent_hour_reads_survive_the_open_hour_and_match_post_compaction_exactly() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant(TENANT);
    let (_tokens, base_ns) = ingest_hot_segments(store.clone(), &tid).await;
    let window = hot_window(base_ns);

    let promql_router = promql_app(
        store.clone(),
        &tid,
        hot_engine_config(RequestLimit::Bounded(1_000)),
    );
    let query = encode_query_param(&format!("{METRIC}[1h]"));
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        window.eval_ns / NS_PER_SEC
    );
    let (status, body) = promql_get(&promql_router, &uri).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "hot PromQL read must survive {SEGMENT_COUNT} recent segments over max_segments={MAX_SEGMENTS}: {body}"
    );
    assert_eq!(body["status"], "success");
    let hot_promql_bits = promql_matrix_bits(&body);
    assert_eq!(
        hot_promql_bits.len(),
        SEGMENT_COUNT,
        "expected all {SEGMENT_COUNT} samples back, got {hot_promql_bits:?}"
    );

    let sql_router = sql_app(
        store.clone(),
        &tid,
        hot_engine_config(RequestLimit::Bounded(1_000)),
    );
    let (status, hot_sql_bits) = sql_post_arrow(
        &sql_router,
        "SELECT ts, value FROM samples ORDER BY ts",
        (window.window_start_ns as f64) / 1e9,
        (window.window_end_ns as f64) / 1e9,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "hot SQL read must survive {SEGMENT_COUNT} recent segments over max_segments={MAX_SEGMENTS}"
    );
    assert_eq!(
        hot_sql_bits.len(),
        SEGMENT_COUNT,
        "expected all {SEGMENT_COUNT} samples back, got {hot_sql_bits:?}"
    );

    let (post_promql_bits, post_sql_bits) =
        post_compaction_bits(store.clone(), &tid, &window).await;

    assert_eq!(
        hot_promql_bits, post_promql_bits,
        "hot-window PromQL read must be bit-identical to the post-compaction read"
    );
    assert_eq!(
        hot_sql_bits, post_sql_bits,
        "hot-window SQL read must be bit-identical to the post-compaction read"
    );
    assert_eq!(
        hot_promql_bits, hot_sql_bits,
        "PromQL and SQL must agree on the same hot data (parity, over real HTTP)"
    );
}

/// Deliverable 1's third bullet: a deliberately low `max_s3_requests`
/// produces the typed `RequestBudgetExceeded` (HTTP 422 class), not a hang
/// and not a partial/truncated result. `fetch_concurrency: 1` (mirroring
/// `recent_hours_admission.rs`'s `request_budget_trips_typed`) makes the trip
/// deterministic: the first segment fetch runs to completion before the next
/// is even started, so a budget of 1 trips at the very next checkpoint.
#[tokio::test]
async fn low_request_budget_trips_typed_error_not_a_hang_or_partial_result() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant(TENANT);
    let (_tokens, base_ns) = ingest_hot_segments(store.clone(), &tid).await;
    let window = hot_window(base_ns);

    let engine_config = EngineConfig {
        max_segments: MAX_SEGMENTS,
        max_s3_requests: RequestLimit::Bounded(1),
        fetch_concurrency: 1,
        deadline: Duration::from_secs(30),
        ..EngineConfig::default()
    };
    let promql_router = promql_app(store, &tid, engine_config);
    let query = encode_query_param(&format!("{METRIC}[1h]"));
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        window.eval_ns / NS_PER_SEC
    );
    let (status, body) = promql_get(&promql_router, &uri).await;

    assert_eq!(
        status.as_u16(),
        422,
        "a tripped request budget must surface as HTTP 422, got {status}: {body}"
    );
    assert_eq!(
        body["status"], "error",
        "Prometheus-shaped error envelope: {body}"
    );
    assert_eq!(
        body["errorType"], "execution",
        "RequestBudgetExceeded maps to the 422 execution error type, not a generic error: {body}"
    );
    assert!(
        body.get("data").is_none(),
        "an error response must carry no partial/truncated data field: {body}"
    );
    let msg = body["error"].as_str().expect("error message is a string");
    assert!(
        msg.contains("exceeding the budget"),
        "the body must carry the RequestBudgetExceeded message, got: {msg}"
    );
}

/// Deliverable 2 (read-your-write): a strict-ack write's own returned commit
/// token, queried during the same hot (open-hour, over-cap) window, must
/// resolve -- never `TooManySegments`. `MAX_SEGMENTS` (3) is already below
/// `SEGMENT_COUNT` (7), so this exercises read-your-write precisely while the
/// sealed-set cap would otherwise refuse the query, proving
/// `resolve_min_token` never counts a token-resolved recent segment against
/// it (ADR-0073's Context).
#[tokio::test]
async fn read_your_write_resolves_during_the_open_hour_over_cap_window() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant(TENANT);
    let (tokens, base_ns) = ingest_hot_segments(store.clone(), &tid).await;
    let window = hot_window(base_ns);
    let last_token = tokens.last().expect("at least one token").encode();

    let promql_router = promql_app(store, &tid, hot_engine_config(RequestLimit::Bounded(1_000)));
    let query = encode_query_param(&format!("{METRIC}[1h]"));
    let uri = format!(
        "/api/v1/query?query={query}&time={}&min_commit_token={last_token}",
        window.eval_ns / NS_PER_SEC
    );
    let (status, body) = promql_get(&promql_router, &uri).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "read-your-write with the freshest strict-ack token must succeed, never TooManySegments: {body}"
    );
    assert_eq!(body["status"], "success");
    let bits = promql_matrix_bits(&body);
    let last_value = sample_value(SEGMENT_COUNT - 1);
    assert!(
        bits.iter().any(|(_, v)| *v == last_value.to_bits()),
        "expected the just-written sample ({last_value}) among the read-your-write results: {bits:?}"
    );
}
