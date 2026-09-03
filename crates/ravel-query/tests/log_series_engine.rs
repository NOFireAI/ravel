//! Acceptance tests proving the log-derived series source (issue #1106,
//! `ravel_query::log_series`) is fully wired into the query engine and the
//! Prometheus-compatible HTTP handlers (issue #1108, ADR-1103 task T3): a
//! PromQL query naming `ravel_log_lines`/`ravel_log_bytes` is answered end to
//! end from real RLOG objects published as real `Signal::Logs` commit
//! records, through the same `Catalog`/`QueryEngine`/HTTP-router stack
//! `tests/e2e.rs` and `tests/erasure_e2e.rs` already drive for metrics.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{erasure, keys, publish, record, signal};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::{InstantSample, LabelMatcher, Value};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use ravel_query::distrib::{Federation, RemoteCluster};
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine, QueryError, RequestLimit};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde_json::Value as JsonValue;
use tower::ServiceExt;
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS;
/// A fixed base far enough past the epoch that every test's fixture windows
/// (a few tens of seconds around it) sit comfortably inside `i64` ranges on
/// both the ts and ms axes, and past enough that `min_event_ts_ns`-based
/// sealing/pruning logic never mistakes it for "now".
const BASE: i64 = 1_700_000_000 * NS;
/// `now_ns` handed to every engine call: far enough past every fixture
/// timestamp that no staleness/lookback rule in the evaluator excludes a
/// sample, without depending on the wall clock (forbidden in workflow
/// scripts, and unnecessary here: the fixture is self-contained).
const NOW_NS: i64 = BASE + 3_600 * NS;
const DEADLINE: Duration = Duration::from_secs(5);

fn tenant(name: &str) -> TenantId {
    TenantId::new(name.to_string())
}

fn ms(ts_ns: i64) -> i64 {
    ts_ns / 1_000_000
}

/// The HTTP API's `time`/`start`/`end` params are Prometheus-style unix
/// seconds (`parse_timestamp_ms` treats a bare number as seconds and
/// converts to milliseconds internally), unlike the engine's direct
/// `instant`/`range` methods, which take milliseconds directly.
fn secs(ts_ns: i64) -> i64 {
    ts_ns / 1_000_000_000
}

fn identity(tenant_hash: TenantHash, seq: u64) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: [3u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    }
}

/// A log record on the stream identified by `resource`, with the given
/// severity, body, and record-level attributes (used by the erasure test to
/// carry a `user.id` attribute matching a pending erasure predicate).
fn record(
    resource: &[(&str, AttrValue)],
    ts: i64,
    severity: &str,
    body: &str,
    record_attrs: &[(&str, &str)],
) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = resource
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let attrs: Vec<(String, AttrValue)> = record_attrs
        .iter()
        .map(|(k, v)| (k.to_string(), AttrValue::Str(v.to_string())))
        .collect();
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: severity.into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Writes one real RLOG object (via `RlogWriter`) and publishes a real
/// `Signal::Logs` commit record for it onto `store`, computing every commit
/// field by hand from `records` exactly as an ingest sink would (mirrors
/// `services/ravel-cli/tests/catalog_signal.rs`'s `publish_rlog`,
/// generalized to a caller-supplied record set spanning multiple streams).
async fn publish_log_segment(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    seq: u64,
    records: &[LogRecord],
) {
    let mut writer = RlogWriter::new(RlogConfig::default(), identity(tenant_hash, seq));
    for r in records {
        writer.push(r.clone()).expect("push log record");
    }
    let object = bytes::Bytes::from(writer.finish().expect("finish rlog"));
    let content_hash = *blake3::hash(&object).as_bytes();

    let min_ts = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max_ts = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let min_observed = records
        .iter()
        .map(|r| r.observed_ts_ns)
        .min()
        .expect("nonempty");
    let max_observed = records
        .iter()
        .map(|r| r.observed_ts_ns)
        .max()
        .expect("nonempty");
    let series_count = records
        .iter()
        .map(|r| r.stream_id)
        .collect::<std::collections::HashSet<_>>()
        .len() as u64;

    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_bytes([3u8; 16]),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: object.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_observed,
        max_ingest_ts_ns: max_observed,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: max_observed,
        ingest_hour_bucket: u32::try_from(max_observed / NS_PER_HOUR).expect("fits u32"),
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, object)
        .await
        .expect("put rlog data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// The two-stream, two-object fixture every test in this file builds on.
///
/// Object 1 (`fixture-a`): stream `s1` (`service.name=api`) with 5 ERROR
/// records at ts `BASE+0..=BASE+3*NS` (`BASE+2*NS` shared by two records) and
/// 3 INFO records at `BASE+4*NS..=BASE+6*NS`; the first two ERROR records
/// carry `user.id=u1` (for the pending-erasure test) and bodies "abc"/"de"
/// (regex fixture bodies, 3 and 2 bytes); the rest are 1-byte bodies "x".
/// Object 2 (`fixture-b`): stream `s2` (`service.name=worker`) with 4 ERROR
/// records at `BASE+7*NS..=BASE+10*NS`, each a 1-byte body "y".
///
/// `s1`/ERROR byte total: 3 + 2 + 1 + 1 + 1 = 8. `s1`/INFO byte total: 3.
/// `s2`/ERROR byte total: 4. `job="api"` total lines: 8. `job="worker"` total
/// lines: 4.
async fn fixture(store: &MemoryStore, tenant_hash: TenantHash) {
    let s1 = [("service.name", AttrValue::Str("api".to_string()))];
    let s2 = [("service.name", AttrValue::Str("worker".to_string()))];

    let a = vec![
        record(&s1, BASE, "ERROR", "abc", &[("user.id", "u1")]),
        record(&s1, BASE + NS, "ERROR", "de", &[("user.id", "u1")]),
        record(&s1, BASE + 2 * NS, "ERROR", "x", &[]),
        record(&s1, BASE + 2 * NS, "ERROR", "x", &[]),
        record(&s1, BASE + 3 * NS, "ERROR", "x", &[]),
        record(&s1, BASE + 4 * NS, "INFO", "x", &[]),
        record(&s1, BASE + 5 * NS, "INFO", "x", &[]),
        record(&s1, BASE + 6 * NS, "INFO", "x", &[]),
    ];
    let b = vec![
        record(&s2, BASE + 7 * NS, "ERROR", "y", &[]),
        record(&s2, BASE + 8 * NS, "ERROR", "y", &[]),
        record(&s2, BASE + 9 * NS, "ERROR", "y", &[]),
        record(&s2, BASE + 10 * NS, "ERROR", "y", &[]),
    ];
    publish_log_segment(store, tenant_hash, 0, &a).await;
    publish_log_segment(store, tenant_hash, 1, &b).await;
}

/// Publishes one RSEG segment carrying `target_rps{job="api"}` = `value` at
/// `ts`, for the log/metrics binop test. Mirrors `tests/e2e.rs`'s
/// `publish_segment`/`series_input`.
async fn publish_metric(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    ts: i64,
    value: f64,
) {
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId};

    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "target_rps".to_string(),
        },
        Label {
            name: "job".to_string(),
            value: "api".to_string(),
        },
    ])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, "target_rps", &labels).expect("series id");
    let input = SeriesInput {
        series_id,
        labels,
        samples: vec![Sample { ts_ns: ts, value }],
    };
    let seg_identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: Uuid::from_bytes([4u8; 16]).to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let written = SegmentWriter::write(
        vec![input],
        seg_identity,
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
        writer_id: Uuid::from_bytes([4u8; 16]),
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: ts,
        ingest_hour_bucket: u32::try_from(ts / NS_PER_HOUR).expect("fits u32"),
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn build_engine(store: Arc<MemoryStore>, config: EngineConfig) -> (QueryEngine, TenantId) {
    let tid = tenant("tenant-a");
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    (QueryEngine::new(catalog, backend, config), tid)
}

fn build_router(store: Arc<MemoryStore>, config: EngineConfig, tenant_id: &TenantId) -> Router {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, config));
    let mut tokens = HashMap::new();
    tokens.insert("secret-a".to_string(), tenant_id.clone());
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)));
    router(state)
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

async fn call(app: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, JsonValue) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(token) = auth {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = builder.body(Body::empty()).expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: JsonValue = serde_json::from_slice(&body).expect("parse response json");
    (status, json)
}

fn vector_results(body: &JsonValue) -> &Vec<JsonValue> {
    body["data"]["result"]
        .as_array()
        .expect("vector result array")
}

fn label<'a>(sample: &'a InstantSample, name: &str) -> Option<&'a str> {
    sample.labels.get(name)
}

async fn put_dreq(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    matchers: &[(&str, &str)],
    window_start_ns: i64,
    window_end_ns: i64,
) {
    let request_id = Uuid::new_v4();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Logs) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: matchers
            .iter()
            .map(|(k, v)| ErasurePredicateMatcher {
                key: (*k).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
        window_start_ns,
        window_end_ns,
        reason: String::new(),
    };
    let key = keys::erasure_request_key(&tenant_hash, Signal::Logs, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

// ---------------------------------------------------------------------------
// Core value contract: lines, bytes, range, regex.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn log_lines_aggregated_by_job_matches_the_fixture_totals() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    let (value, _coverage) = engine
        .instant(
            th,
            "sum by (job) (count_over_time(ravel_log_lines[1h]))",
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Vector(vector) = value else {
        panic!("expected vector");
    };
    let mut totals: Vec<(String, f64)> = vector
        .iter()
        .map(|s| (label(s, "job").expect("job label").to_string(), s.value))
        .collect();
    totals.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        totals,
        vec![("api".to_string(), 8.0), ("worker".to_string(), 4.0)]
    );
}

#[tokio::test]
async fn log_bytes_summed_across_severities_matches_the_fixture_total() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    // `job="api"` splits into two series by `severity_text` (ERROR: 5
    // records, bytes 3+2+1+1+1=8; INFO: 3 records, bytes 1+1+1=3), so the
    // outer `sum` is required to see the combined 11-byte total across both.
    let (value, _coverage) = engine
        .instant(
            th,
            r#"sum(sum_over_time(ravel_log_bytes{job="api"}[1h]))"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Vector(vector) = value else {
        panic!("expected vector");
    };
    assert_eq!(vector.len(), 1);
    assert!(
        (vector[0].value - 11.0).abs() < 1e-9,
        "expected 11 bytes total, got {}",
        vector[0].value
    );
}

#[tokio::test]
async fn body_regex_matcher_selects_only_matching_lines() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    // Only "abc" and "de" (the two `user.id=u1` records) match `^[a-e]+$`;
    // the other three ERROR bodies are all "x".
    let (value, _coverage) = engine
        .instant(
            th,
            r#"count_over_time(ravel_log_lines{job="api",severity_text="ERROR",__body__=~"^[a-e]+$"}[1h])"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Vector(vector) = value else {
        panic!("expected vector");
    };
    assert_eq!(vector.len(), 1);
    assert!((vector[0].value - 2.0).abs() < 1e-9);
}

#[tokio::test]
async fn range_query_straddling_a_shared_timestamp_counts_both_records_at_it() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    // `s1`/ERROR carries records at BASE+0,1,2,2,3 (ts=BASE+2*NS shared by
    // two distinct records). A `[4s]` window straddling that shared
    // timestamp must count both copies, not collapse them to one: at
    // t=BASE+2s the window (t-4s, t] holds ts 0,1,2,2 (4 samples); at
    // t=BASE+3s it holds ts 0,1,2,2,3 (5 samples) -- the shared timestamp's
    // second copy is counted at both steps, proving it was never
    // deduplicated away.
    let (value, _coverage) = engine
        .range(
            th,
            r#"count_over_time(ravel_log_lines{job="api",severity_text="ERROR"}[4s])"#,
            ms(BASE + 2 * NS),
            ms(BASE + 3 * NS),
            1_000,
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Matrix(matrix) = value else {
        panic!("expected matrix");
    };
    assert_eq!(matrix.len(), 1);
    let samples = &matrix[0].1;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    assert_eq!(values, vec![4.0, 5.0]);
}

// ---------------------------------------------------------------------------
// Binop between a log series and a published metrics series.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn binop_between_aggregated_log_series_and_a_metrics_series() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    publish_metric(&store, &tid, th, BASE + 2 * NS, 3.0).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    let (value, _coverage) = engine
        .instant(
            th,
            r#"sum by (job) (count_over_time(ravel_log_lines{job="api"}[1h])) - target_rps{job="api"}"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Vector(vector) = value else {
        panic!("expected vector");
    };
    assert_eq!(vector.len(), 1);
    assert!(
        (vector[0].value - 5.0).abs() < 1e-9,
        "8 log lines - 3 rps = 5"
    );
}

// ---------------------------------------------------------------------------
// Nameless selectors and ordinary metric queries never touch Signal::Logs.
// ---------------------------------------------------------------------------

/// Wraps `inner` so every `get`/`list`/`list_delimited`/`head` touching a
/// `Signal::Logs` catalog or data key panics -- a hard, provable "never
/// called" rather than a counter a test could forget to assert on.
struct PanicsOnLogsTouch {
    inner: Arc<MemoryStore>,
}

fn touches_logs(key: &str) -> bool {
    key.contains(&format!("/{}/", Signal::Logs.key_prefix())) || key.contains(".rlog")
}

#[async_trait]
impl ObjectStoreBackend for PanicsOnLogsTouch {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        assert!(!touches_logs(key), "unexpected GET of a logs key: {key}");
        self.inner.get(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        assert!(!touches_logs(key), "unexpected HEAD of a logs key: {key}");
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        assert!(
            !touches_logs(prefix),
            "unexpected LIST of a logs prefix: {prefix}"
        );
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        assert!(
            !touches_logs(prefix),
            "unexpected delimited LIST of a logs prefix: {prefix}"
        );
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        self.inner.capabilities()
    }
}

#[tokio::test]
async fn nameless_selector_returns_only_metrics_series_and_never_touches_logs() {
    let mem = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&mem, th).await;
    publish_metric(&mem, &tid, th, BASE + 2 * NS, 3.0).await;

    let guarded: Arc<dyn ObjectStoreBackend> = Arc::new(PanicsOnLogsTouch { inner: mem });
    let catalog =
        Arc::new(Catalog::new(guarded.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, guarded, EngineConfig::default());

    let (series, _coverage) = engine
        .resolve_series(
            th,
            &[LabelMatcher::equal("job", "api")],
            TimeRange {
                start_ns: BASE - NS,
                end_ns: BASE + 20 * NS,
            },
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(series.len(), 1, "only the metrics series should match");
    assert_eq!(series[0].1.get("__name__"), Some("target_rps"));
}

#[tokio::test]
async fn ordinary_named_metric_query_never_resolves_signal_logs() {
    let mem = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&mem, th).await;
    publish_metric(&mem, &tid, th, BASE + 2 * NS, 3.0).await;

    let guarded: Arc<dyn ObjectStoreBackend> = Arc::new(PanicsOnLogsTouch { inner: mem });
    let tokens = {
        let mut m = HashMap::new();
        m.insert("secret-a".to_string(), tid.clone());
        m
    };
    let catalog =
        Arc::new(Catalog::new(guarded.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, guarded, EngineConfig::default()));
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)));
    let app = router(state);

    let uri = format!(
        "/api/v1/query?query={}&time={}",
        encode_query_param(r#"target_rps{job="api"}"#),
        secs(BASE + 20 * NS)
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(vector_results(&body).len(), 1);
}

// ---------------------------------------------------------------------------
// Budgets: max_samples / max_series -> HTTP 422.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exceeding_max_samples_budget_for_a_log_selector_returns_422() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let config = EngineConfig {
        max_samples: 3,
        ..EngineConfig::default()
    };
    let app = build_router(store, config, &tid);

    let uri = format!(
        "/api/v1/query?query={}&time={}",
        encode_query_param(r#"ravel_log_lines{job="api"}"#),
        secs(BASE + 20 * NS)
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert_eq!(body["errorType"], "execution");
}

#[tokio::test]
async fn exceeding_max_series_budget_for_a_log_selector_returns_422() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    // `job="api"` splits into 2 series (ERROR, INFO); a cap of 1 must trip.
    let config = EngineConfig {
        max_series: 1,
        ..EngineConfig::default()
    };
    let app = build_router(store, config, &tid);

    let uri = format!(
        "/api/v1/query?query={}&time={}",
        encode_query_param(r#"ravel_log_lines{job="api"}"#),
        secs(BASE + 20 * NS)
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert_eq!(body["errorType"], "execution");
}

// ---------------------------------------------------------------------------
// Query-wide shared budget across two log selectors (ADR-1103 decision 4
// step 4): the same `PhaseAccounting` handle is threaded through every log
// plan's `fetch_log_series` call, so the second selector's segments are
// checked against the ceiling with the first selector's spend already
// counted.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_log_selectors_share_one_request_budget() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;

    // One selector alone (one object, one stream) fits comfortably under 3
    // requests (plan + scan, at most a couple of blocks); two selectors
    // together each touch a distinct object, so the combined request count
    // must exceed a budget sized for one.
    let config = EngineConfig {
        max_s3_requests: RequestLimit::Bounded(3),
        ..EngineConfig::default()
    };
    let (engine, _tid) = build_engine(store, config);

    // `or` is PromQL's set operator: it unions the two vectors regardless of
    // label-set compatibility, so this exercises both log selectors without
    // a vector-matching error masking the budget outcome.
    let query = r#"count_over_time(ravel_log_lines{job="api"}[1h]) or count_over_time(ravel_log_lines{job="worker"}[1h])"#;
    let err = engine
        .instant(th, query, ms(BASE + 20 * NS), &[], NOW_NS, DEADLINE)
        .await
        .expect_err("combined budget must be exceeded");
    assert!(
        matches!(err, QueryError::RequestBudgetExceeded { .. }),
        "expected RequestBudgetExceeded, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Federation: log series are never fanned out to a remote cluster, only
// flagged with a warning. A `SliceFetcher` whose `fetch` panics if called is
// a hard proof, not a counter a test could forget to assert on.
// ---------------------------------------------------------------------------

struct PanicsIfFetched;

#[async_trait]
impl SliceFetcher for PanicsIfFetched {
    async fn fetch(
        &self,
        _request: ravel_proto::queryfrag::v1::FetchRequest,
    ) -> Result<SliceResponse, DistribError> {
        panic!("a log-only query must never fan out to a remote cluster");
    }
}

#[tokio::test]
async fn log_only_query_warns_instead_of_federating() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());
    let federation = Arc::new(Federation::new(vec![RemoteCluster {
        name: "remote-1".to_string(),
        fetcher: Arc::new(PanicsIfFetched),
        skip_unavailable: false,
        soft_timeout: Duration::from_secs(1),
    }]));
    let engine = engine.with_federation(federation);

    let (_value, stats) = engine
        .instant_with_stats(
            th,
            r#"count_over_time(ravel_log_lines{job="api"}[1h])"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds without ever calling the remote fetcher");
    assert_eq!(
        stats.warnings,
        vec![
            "ravel_log_lines is answered by this cluster only; log series are not federated"
                .to_string()
        ]
    );
}

// ---------------------------------------------------------------------------
// Pending erasure applies to log series through the engine, exactly as it
// does for metrics (tests/erasure_e2e.rs's pattern, `Signal::Logs` swapped
// in for `Signal::Metrics`).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pending_erasure_excludes_matching_log_records() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;

    let (value_before, _c) = {
        let (engine, _tid) = build_engine(store.clone(), EngineConfig::default());
        engine
            .instant(
                th,
                r#"count_over_time(ravel_log_lines{job="api",severity_text="ERROR"}[1h])"#,
                ms(BASE + 20 * NS),
                &[],
                NOW_NS,
                DEADLINE,
            )
            .await
            .expect("query succeeds")
    };
    let Value::Vector(before) = value_before else {
        panic!("expected vector");
    };
    assert!(
        (before[0].value - 5.0).abs() < 1e-9,
        "5 ERROR records before erasure"
    );

    // The two `user.id=u1` records (ts BASE, BASE+NS) fall inside this
    // window and must be excluded from the next resolve.
    put_dreq(&store, th, &[("user.id", "u1")], BASE - NS, BASE + 20 * NS).await;

    let (engine, _tid) = build_engine(store, EngineConfig::default());
    let (value_after, _c) = engine
        .instant(
            th,
            r#"count_over_time(ravel_log_lines{job="api",severity_text="ERROR"}[1h])"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    let Value::Vector(after) = value_after else {
        panic!("expected vector");
    };
    assert_eq!(after.len(), 1);
    assert!(
        (after[0].value - 3.0).abs() < 1e-9,
        "2 of 5 ERROR records erased, expected 3 remaining, got {}",
        after[0].value
    );
}

// ---------------------------------------------------------------------------
// /api/v1/series, /api/v1/labels, /api/v1/label/__name__/values,
// /api/v1/metadata for a log selector.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn series_endpoint_lists_log_series_for_a_log_selector() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let app = build_router(store, EngineConfig::default(), &tid);

    let matcher = encode_query_param(r#"ravel_log_lines{job="api"}"#);
    let window = format!("start={}&end={}", secs(BASE - NS), secs(NOW_NS));
    let uri = format!("/api/v1/series?match%5B%5D={matcher}&{window}");
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let results = body["data"].as_array().expect("series result array");
    assert_eq!(
        results.len(),
        2,
        "job=api splits into ERROR and INFO series"
    );
    let mut severities: Vec<&str> = results
        .iter()
        .map(|s| s["severity_text"].as_str().expect("severity_text label"))
        .collect();
    severities.sort_unstable();
    assert_eq!(severities, vec!["ERROR", "INFO"]);
    for s in results {
        assert_eq!(s["__name__"], "ravel_log_lines");
        assert_eq!(s["job"], "api");
    }

    let (status, body) = call(
        &app,
        &format!("/api/v1/labels?match%5B%5D={matcher}&{window}"),
        Some("secret-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let mut names: Vec<&str> = body["data"]
        .as_array()
        .expect("labels array")
        .iter()
        .map(|v| v.as_str().expect("label name string"))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "__name__",
            "job",
            "otel_scope_name",
            "otel_scope_version",
            "severity_text"
        ]
    );

    let (status, body) = call(
        &app,
        &format!("/api/v1/label/__name__/values?match%5B%5D={matcher}&{window}"),
        Some("secret-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let mut values: Vec<&str> = body["data"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|v| v.as_str().expect("value string"))
        .collect();
    values.sort_unstable();
    // ADR-1103: a match[] selector naming a log metric brings in both
    // reserved names, not only the one the selector matched.
    assert_eq!(values, vec!["ravel_log_bytes", "ravel_log_lines"]);

    // No match[] at all: `__name__` values must include both reserved log
    // metric names even though nothing else in the request names them.
    let (status, body) = call(
        &app,
        &format!("/api/v1/label/__name__/values?{window}"),
        Some("secret-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let mut values: Vec<&str> = body["data"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|v| v.as_str().expect("value string"))
        .collect();
    values.sort_unstable();
    assert!(values.contains(&"ravel_log_lines"));
    assert!(values.contains(&"ravel_log_bytes"));
}

#[tokio::test]
async fn metadata_endpoint_always_includes_the_two_reserved_log_metrics() {
    use ravel_cache::SystemClock;
    use ravel_query::http::{MetadataCache, MetadataCacheConfig};

    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(
        catalog,
        backend.clone(),
        EngineConfig::default(),
    ));
    let cache = Arc::new(MetadataCache::new(
        backend,
        MetadataCacheConfig::default(),
        Arc::new(SystemClock),
    ));
    let mut tok = HashMap::new();
    tok.insert("secret-a".to_string(), tid);
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tok)))
        .with_metadata_cache(cache);
    let app = router(state);

    let (status, body) = call(&app, "/api/v1/metadata", Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["data"]["ravel_log_lines"][0]["type"], "gauge");
    assert_eq!(
        body["data"]["ravel_log_lines"][0]["help"],
        "One sample per log line, value 1, derived from the logs signal at query time (ADR-1103); count_over_time counts lines."
    );
    assert_eq!(body["data"]["ravel_log_lines"][0]["unit"], "");
    assert_eq!(body["data"]["ravel_log_bytes"][0]["type"], "gauge");
    assert_eq!(
        body["data"]["ravel_log_bytes"][0]["help"],
        "One sample per log line whose value is the line body's length in bytes (ADR-1103); sum_over_time sums bytes."
    );
    assert_eq!(body["data"]["ravel_log_bytes"][0]["unit"], "bytes");
}

// ---------------------------------------------------------------------------
// Exact per-phase GET counts (pinned against observed behavior, matching
// tests/log_series.rs's own established convention for this kind of
// assertion: block layout, suffix length, and threshold config make the
// number infeasible to hand-derive from first principles).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn phase_accounting_reports_exact_get_counts_for_a_log_query() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    fixture(&store, th).await;
    let (engine, _tid) = build_engine(store, EngineConfig::default());

    let (_value, stats) = engine
        .instant_with_stats(
            th,
            r#"count_over_time(ravel_log_lines{job="api"}[1h])"#,
            ms(BASE + 20 * NS),
            &[],
            NOW_NS,
            DEADLINE,
        )
        .await
        .expect("query succeeds");
    use ravel_query::QueryPhase;
    use ravel_types::accounting::AccountedOp;
    let pooled = stats.phase_accounting.pooled();
    // Pinned against observed behavior (tests/log_series.rs's own convention:
    // block layout and catalog round trips are infeasible to hand-derive).
    // Resolve: the Signal::Logs catalog snapshot resolve, generic per-query
    // overhead shared with a metrics query, not log-series-specific. Plan:
    // two GETs, one per candidate object, because stream discovery reads
    // every candidate's directory before any of them can be excluded:
    // object A's footer probe and object B's STREAM_DIR read. Only A matches
    // `job="api"`; B is pruned out of the scan by that read, not before it.
    // Scan: one BLOCKS range read for object A's 8 records, which fit a
    // single block at this fixture's size.
    assert_eq!(
        stats
            .phase_accounting
            .phase(QueryPhase::Resolve)
            .s3_requests(AccountedOp::Get),
        3,
        "catalog snapshot resolve"
    );
    assert_eq!(
        stats
            .phase_accounting
            .phase(QueryPhase::Plan)
            .s3_requests(AccountedOp::Get),
        2,
        "one footer probe GET, one STREAM_DIR section GET"
    );
    assert_eq!(
        stats
            .phase_accounting
            .phase(QueryPhase::Probe)
            .s3_requests(AccountedOp::Get),
        0,
        "log fetch has no separate segment-catalog probe phase"
    );
    assert_eq!(
        stats
            .phase_accounting
            .phase(QueryPhase::Scan)
            .s3_requests(AccountedOp::Get),
        1,
        "one BLOCKS range read for object A's single block"
    );
    assert_eq!(
        pooled.s3_requests(AccountedOp::Get),
        6,
        "every GET this query issues is charged to exactly one phase"
    );
}
