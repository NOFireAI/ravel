//! `stats.phases[].segmentsOpened` on the logs and spans query paths
//! (issue #3745): the number of distinct data objects whose footer a query
//! opened under each phase, counted once per object per query, whether the
//! footer was pruned afterward or not.
//!
//! Logs: three small RLOG objects with no KEY_IDX route through the
//! unplanned whole-object path (ADR-2677 decision 2), so `scan` carries every
//! open; three large, KEY_IDX-declared objects forced onto the ranged path
//! (`with_block_range_threshold(0)`) route through `plan_segment_keyed` /
//! `probe_segment_keyed` (ADR-2707 decision 5), so `plan` carries every open,
//! including the two objects the key index prunes before any block read. A
//! window outside every object's event range opens nothing, anywhere.
//!
//! Spans: three RSPAN objects, each padded past the fetcher's 64 KiB tail
//! probe (`DEFAULT_TRACE_PROBE_BYTES`) with decoy spans, so a trace lookup's
//! tail probe is a ranged read for every object (`probe`), whether or not it
//! holds the trace; the one holding object's subsequent block read does not
//! reopen the footer, so `scan` stays at zero. Checked both through
//! `POST /api/v1/sql` and `GET /api/traces/{id}`, which run the same
//! statement (`ravel_server::traces::trace_statement`).

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use clap::Parser;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::key_index::{KeyType, TRACE_ID_FIELD};
use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{
    DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, EngineConfig, LogSegmentFetcher, ReadCache, SegmentFetcher,
};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, SpanSegmentFetcher, SqlConfig, SqlExecutor,
    StaticDeclaredColumns,
};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme".to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Every phase's `segmentsOpened`, keyed by the phase name `stats.phases[]`
/// serializes.
fn segments_opened(value: &Value) -> BTreeMap<String, u64> {
    value["stats"]["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("no phases in {value}"))
        .iter()
        .map(|phase| {
            let name = phase["phase"]
                .as_str()
                .unwrap_or_else(|| panic!("phase has no name: {phase}"))
                .to_string();
            let opened = phase["segmentsOpened"]
                .as_u64()
                .unwrap_or_else(|| panic!("{name}.segmentsOpened is a u64 in {value}"));
            (name, opened)
        })
        .collect()
}

fn want(resolve: u64, plan: u64, probe: u64, scan: u64, key_index: u64) -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("resolve".to_string(), resolve),
        ("plan".to_string(), plan),
        ("probe".to_string(), probe),
        ("scan".to_string(), scan),
        ("keyIndex".to_string(), key_index),
    ])
}

async fn post_sql(app: &Router, sql: &str, start_s: f64, end_s: f64) -> Value {
    let payload = serde_json::json!({
        "query": sql,
        "start": start_s,
        "end": end_s,
        "timeout": 60.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
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
    assert_eq!(status, StatusCode::OK, "{sql}: {value}");
    assert_eq!(value["status"], "success", "{sql}: {value}");
    value
}

async fn get_trace(app: &Router, trace_hex: &str) -> Value {
    let request = Request::builder()
        .method("GET")
        .uri(format!("/api/traces/{trace_hex}"))
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
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
    let value: Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success", "{value}");
    value
}

// --- logs ----------------------------------------------------------------

/// Writes one small RLOG object (no KEY_IDX) with a handful of records; `i`
/// seeds the object's single distinguishing record.
async fn publish_small_log(store: &dyn ObjectStoreBackend, seq: usize, status_code: i64) -> String {
    let tenant_hash = tenant().hash();
    let writer_id = Uuid::from_u128(3_745_000 + seq as u128);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        LogObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        },
    );
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let records: Vec<LogRecord> = (0..4)
        .map(|i| {
            let ts = 1_000 + (seq * 10 + i) as i64;
            LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("seq{seq}rec{i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("status_code".to_string(), AttrValue::I64(status_code))],
            }
        })
        .collect();
    let min = records.iter().map(|r| r.ts_ns).min().expect("records");
    let max = records.iter().map(|r| r.ts_ns).max().expect("records");
    let count = records.len() as u64;
    for r in records {
        writer.push(r).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10 + seq as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&key, Bytes::from(bytes), Default::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
    key
}

/// Three small, undeclared-key-index RLOG objects: object 0 carries
/// `status_code = 500`, objects 1 and 2 carry 400. Every object is well
/// under the fetcher's 512 KiB whole-object threshold, so the unplanned
/// route (ADR-2677 decision 2) opens each under `scan`.
async fn small_logs_router() -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for (seq, code) in [(0, 500), (1, 400), (2, 400)] {
        publish_small_log(store.as_ref(), seq, code).await;
    }
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let logs = LogSegmentFetcher::new(store.clone());
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        logs,
        SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new("status_code", DeclaredType::I64),
    ])));
    sql_router(executor, store)
}

fn sql_router(executor: SqlExecutor, store: Arc<dyn ObjectStoreBackend>) -> Router {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant());
    let state = ravel_server::sql::SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(60),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    };
    ravel_server::sql::router(state.clone()).merge(ravel_server::traces::router(state))
}

#[tokio::test]
async fn small_object_equality_opens_every_object_under_scan() {
    let app = small_logs_router().await;
    let v = post_sql(
        &app,
        "SELECT body FROM logs WHERE status_code = 500",
        0.0,
        NOW_NS as f64 / 1e9,
    )
    .await;
    assert_eq!(
        value_rows(&v),
        vec![
            "seq0rec0".to_string(),
            "seq0rec1".to_string(),
            "seq0rec2".to_string(),
            "seq0rec3".to_string(),
        ],
        "{v}"
    );
    assert_eq!(segments_opened(&v), want(0, 0, 0, 3, 0), "{v}");
}

fn value_rows(v: &Value) -> Vec<String> {
    v["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows: {v}"))
        .iter()
        .map(|row| row[0].as_str().expect("string cell").to_string())
        .collect()
}

#[tokio::test]
async fn a_window_outside_every_object_opens_nothing() {
    let app = small_logs_router().await;
    // The fixture's records sit at ts ~1_000ns, in hour bucket 0; a window
    // over the last minute of hour 4 cannot list them.
    let start = (NOW_NS - 60_000_000_000) as f64 / 1e9;
    let end = NOW_NS as f64 / 1e9;
    let v = post_sql(
        &app,
        "SELECT body FROM logs WHERE status_code = 500",
        start,
        end,
    )
    .await;
    assert!(value_rows(&v).is_empty(), "{v}");
    assert_eq!(segments_opened(&v), want(0, 0, 0, 0, 0), "{v}");
}

// --- logs: large, key-indexed objects -------------------------------------

const BLOCKS: usize = 8;
const RECORDS_PER_BLOCK: usize = 16;
const RECORDS: usize = BLOCKS * RECORDS_PER_BLOCK;

/// `o<object>r<record, three digits>|` plus padding, wide enough that the
/// object clears the fetcher's whole-object threshold once every record
/// carries it.
fn body_of(object: usize, i: usize) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&((object * RECORDS + i) as u64).to_le_bytes());
    let mut pad = vec![0u8; 6 * 1024];
    hasher.finalize_xof().fill(&mut pad);
    format!("o{object}r{i:03}|{}", hex(&pad))
}

/// Record `i` of object `object`. Every block carries `status_code` 400; only
/// object 0 overrides record 50 (block 3) to 500.
fn large_record(object: usize, i: usize) -> LogRecord {
    let code = if object == 0 && i == 50 { 500 } else { 400 };
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let ts = 1_000 + (i * 4 + object) as i64;
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body_of(object, i),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("status_code".to_string(), AttrValue::I64(code))],
    }
}

async fn publish_large_log(store: &dyn ObjectStoreBackend, seq: usize) -> String {
    let tenant_hash = tenant().hash();
    let writer_id = Uuid::from_u128(3_745_100 + seq as u128);
    let mut writer = RlogWriter::new(
        RlogConfig {
            block_target_records: RECORDS_PER_BLOCK,
            ..RlogConfig::default()
        },
        LogObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        },
    )
    .with_key_index_fields(vec![
        ("status_code".to_string(), KeyType::I64),
        (TRACE_ID_FIELD.to_string(), KeyType::Id16),
    ]);
    let records: Vec<LogRecord> = (0..RECORDS).map(|i| large_record(seq, i)).collect();
    let min = records.iter().map(|r| r.ts_ns).min().expect("records");
    let max = records.iter().map(|r| r.ts_ns).max().expect("records");
    let count = records.len() as u64;
    for r in records {
        writer.push(r).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10 + seq as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&key, Bytes::from(bytes), Default::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
    key
}

/// Three large, KEY_IDX-declared RLOG objects, forced onto the ranged path by
/// a zero block-range threshold: only object 0 holds `status_code = 500`.
async fn large_logs_router() -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for seq in 0..3 {
        publish_large_log(store.as_ref(), seq).await;
    }
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let logs = LogSegmentFetcher::new(store.clone()).with_block_range_threshold(0);
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        logs,
        SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("status_code", DeclaredType::I64)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["status_code".to_string()],
                trace_id: false,
            }),
    ));
    sql_router(executor, store)
}

#[tokio::test]
async fn key_indexed_equality_opens_every_object_under_plan_not_scan() {
    let app = large_logs_router().await;
    let v = post_sql(
        &app,
        "SELECT substr(body, 1, 6) AS id FROM logs WHERE status_code = 500",
        0.0,
        NOW_NS as f64 / 1e9,
    )
    .await;
    assert_eq!(
        value_rows_named(&v, "id"),
        vec!["o0r050".to_string()],
        "{v}"
    );
    // Objects 1 and 2 are pruned by KEY_IDX before any block read, but their
    // footer is still opened to run the probe, so the open counts under
    // `plan`, not `scan`.
    assert_eq!(segments_opened(&v), want(0, 3, 0, 0, 0), "{v}");
}

/// The same three objects under the fetcher's default thresholds and two SQL
/// partitions. Each object sits between the 512 KiB whole-object threshold
/// and `plan_whole_object_bound()`, so the plan pass leaves all three
/// unplanned (three skippable objects, two partitions) and probes their
/// KEY_IDX instead; the scan then opens each survivor whole.
async fn probe_only_logs_router() -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let logs = LogSegmentFetcher::new(store.clone());
    for seq in 0..3 {
        let key = publish_large_log(store.as_ref(), seq).await;
        let size = store.head(&key).await.expect("head").size;
        assert!(
            size > DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD && size <= logs.plan_whole_object_bound(),
            "object {seq} is {size} bytes, outside ({DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD}, {}]",
            logs.plan_whole_object_bound()
        );
    }
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        logs,
        SpanSegmentFetcher::new(store.clone()),
        SqlConfig {
            engine: EngineConfig {
                sql_partition_count: Some(2),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
        1 << 30,
    )
    .with_declared_column_source(Arc::new(
        StaticDeclaredColumns::new(vec![DeclaredColumn::new("status_code", DeclaredType::I64)])
            .with_key_index(KeyIndexDeclaration {
                columns: vec!["status_code".to_string()],
                trace_id: false,
            }),
    ));
    sql_router(executor, store)
}

fn phase_field(v: &Value, phase: &str, field: &str) -> u64 {
    v["stats"]["phases"]
        .as_array()
        .unwrap_or_else(|| panic!("no phases in {v}"))
        .iter()
        .find(|p| p["phase"] == phase)
        .unwrap_or_else(|| panic!("no {phase} phase in {v}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{phase}.{field} is a u64 in {v}"))
}

/// The keyed probe-only route: every object's KEY_IDX is probed under `plan`,
/// none is pruned (every object holds `status_code = 400`), and the scan
/// reads each survivor whole under `scan` without recording a second open.
///
/// FLIP: setting `footer_counted` to `false` for `SegmentPlan::Unplanned` in
/// `OwnedSeg` publication (`crates/ravel-sql/src/logs_scan/plan.rs`) records
/// each object again under `scan`: `left: {.., "plan": 3, .., "scan": 3}`.
#[tokio::test]
async fn probe_only_keyed_scan_opens_every_object_once_under_plan() {
    let app = probe_only_logs_router().await;
    let v = post_sql(
        &app,
        "SELECT count(*) AS n FROM logs WHERE status_code = 400",
        0.0,
        NOW_NS as f64 / 1e9,
    )
    .await;
    assert_eq!(v["data"]["rows"][0][0], 3 * RECORDS as u64 - 1, "{v}");
    assert_eq!(segments_opened(&v), want(0, 3, 0, 0, 0), "{v}");
    // The probe's tail GET (which holds the KEY_IDX under the default suffix
    // length) and the scan's whole-object GET each read every object once:
    // `scan` reads at zero opens because the plan pass recorded them.
    assert_eq!(phase_field(&v, "plan", "s3GetRequests"), 3, "{v}");
    assert!(
        phase_field(&v, "keyIndex", "decompressedOutputBytes") > 0,
        "{v}"
    );
    assert_eq!(phase_field(&v, "scan", "s3GetRequests"), 3, "{v}");
}

fn value_rows_named(v: &Value, column: &str) -> Vec<String> {
    let columns = v["data"]["columns"].as_array().expect("columns");
    let at = columns
        .iter()
        .position(|c| c["name"] == column)
        .unwrap_or_else(|| panic!("no {column} column: {v}"));
    v["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows: {v}"))
        .iter()
        .map(|row| row[at].as_str().expect("string cell").to_string())
        .collect()
}

// --- spans -----------------------------------------------------------------

const DECOYS: u64 = 2_400;
/// Low first byte, sorts ahead of every decoy (decoys set bit 0x80).
const TARGET_TRACE: [u8; 16] = [0x01; 16];

fn decoy_trace(seed: u64) -> [u8; 16] {
    let mut t = [0u8; 16];
    t.copy_from_slice(&blake3::hash(&seed.to_le_bytes()).as_bytes()[..16]);
    t[0] |= 0x80;
    t
}

/// One RSPAN object, well past the fetcher's 64 KiB tail-probe threshold:
/// `DECOYS` decoy spans, plus [`TARGET_TRACE`] when `holds_target`.
async fn publish_span_object(
    store: &dyn ObjectStoreBackend,
    seq: u64,
    holds_target: bool,
) -> String {
    let tenant_hash = tenant().hash();
    let writer_id = Uuid::from_u128(3_745_200 + seq as u128);
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig {
            block_target_records: 4,
            ..ravel_rspan::RspanConfig::default()
        },
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base = 60 * 1_000_000_000i64;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut count = 0u64;
    let mut push = |trace_id: [u8; 16], name: &str, start: i64| {
        min_ts = min_ts.min(start);
        max_ts = max_ts.max(start + 1_000);
        count += 1;
        writer.push(ravel_rspan::SpanRecord {
            trace_id,
            span_id: (count).to_le_bytes(),
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns: start,
            end_ts_ns: start + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "svc".to_string())],
        });
    };
    if holds_target {
        push(TARGET_TRACE, "target", base);
    }
    for i in 0..DECOYS {
        push(
            decoy_trace(seq * 1_000_000 + i),
            "decoy",
            base + i as i64 + 1,
        );
    }
    let bytes = writer.finish().expect("finish rspan object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: count,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: min_ts,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&key, Bytes::from(bytes), Default::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
    key
}

/// Three RSPAN objects, each padded past the tail-probe threshold: object 0
/// holds [`TARGET_TRACE`], objects 1 and 2 carry only decoys.
async fn spans_router() -> Router {
    spans_router_with_cache(None).await
}

/// [`spans_router`] with every fetcher sharing `cache` the way
/// `query::build_sql_executor` attaches the server's read cache.
async fn spans_router_with_cache(cache: Option<ReadCache>) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for (seq, holds) in [(0u64, true), (1, false), (2, false)] {
        publish_span_object(store.as_ref(), seq, holds).await;
    }
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let mut metrics = SegmentFetcher::new(store.clone());
    let mut logs = LogSegmentFetcher::new(store.clone());
    let mut spans = SpanSegmentFetcher::new(store.clone());
    if let Some(cache) = cache {
        metrics = metrics.with_cache(cache.clone());
        logs = logs.with_cache(cache.clone());
        spans = spans.with_cache(cache);
    }
    let executor = SqlExecutor::new(catalog, metrics, logs, spans, SqlConfig::default(), 1 << 30);
    sql_router(executor, store)
}

/// The fetcher cache the server builds for `--cache-max-bytes max_bytes`.
fn server_cache(max_bytes: u64) -> ReadCache {
    let max = max_bytes.to_string();
    let cli =
        ravel_server::Cli::try_parse_from(["ravel-server", "--cache-max-bytes", max.as_str()])
            .expect("cache flags parse");
    ravel_server::store::build_cache(&cli, max_bytes).expect("a nonzero cache is built")
}

const DATA_PHASES: [&str; 4] = ["plan", "probe", "scan", "keyIndex"];

/// ADR-3692 acceptance row 4 (a warm spans lookup reads no data object) on
/// an unfolded tail: the same trace lookup twice through one read cache. The
/// second run opens the same objects under the same phases as the first,
/// every one served from the cache, so `segmentsOpened` counts decoded
/// footers whatever served their bytes.
#[tokio::test]
async fn adr_3692_row_4_warm_trace_lookup_opens_the_same_objects_with_zero_data_gets() {
    let app = spans_router_with_cache(Some(server_cache(64 * 1024 * 1024))).await;
    let sql = format!(
        "SELECT name FROM spans WHERE trace_id = X'{}'",
        hex(&TARGET_TRACE)
    );
    let cold = post_sql(&app, &sql, 0.0, NOW_NS as f64 / 1e9).await;
    let warm = post_sql(&app, &sql, 0.0, NOW_NS as f64 / 1e9).await;
    assert_eq!(value_rows(&cold), vec!["target".to_string()], "{cold}");
    assert_eq!(value_rows(&warm), vec!["target".to_string()], "{warm}");
    assert_eq!(segments_opened(&cold), want(0, 0, 3, 0, 0), "{cold}");
    assert_eq!(segments_opened(&warm), want(0, 0, 3, 0, 0), "{warm}");
    for phase in DATA_PHASES {
        assert_eq!(
            phase_field(&warm, phase, "s3GetRequests"),
            0,
            "{phase}: {warm}"
        );
    }
    assert!(phase_field(&cold, "probe", "s3GetRequests") >= 3, "{cold}");
    assert!(phase_field(&warm, "probe", "cacheHits") >= 3, "{warm}");
}

#[tokio::test]
async fn trace_lookup_by_sql_opens_every_object_under_probe_not_scan() {
    let app = spans_router().await;
    let sql = format!(
        "SELECT name FROM spans WHERE trace_id = X'{}'",
        hex(&TARGET_TRACE)
    );
    let v = post_sql(&app, &sql, 0.0, NOW_NS as f64 / 1e9).await;
    assert_eq!(value_rows(&v), vec!["target".to_string()], "{v}");
    // Every object's tail is probed for KEY_IDX, pruned or not; only the
    // holding object's block run follows, which does not reopen the footer.
    assert_eq!(segments_opened(&v), want(0, 0, 3, 0, 0), "{v}");
}

#[tokio::test]
async fn trace_lookup_by_get_api_traces_opens_every_object_under_probe_not_scan() {
    let app = spans_router().await;
    let v = get_trace(&app, &hex(&TARGET_TRACE)).await;
    assert_eq!(
        v["data"]["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("no rows: {v}"))
            .len(),
        1,
        "{v}"
    );
    assert_eq!(segments_opened(&v), want(0, 0, 3, 0, 0), "{v}");
}
