//! End-to-end tests: real RSEG segments, written with `ravel_segment`,
//! published with `ravel_commit` onto a `MemoryStore`, queried through the
//! full stack (catalog -> fetcher -> engine -> evaluator -> HTTP handlers)
//! via `tower::ServiceExt::oneshot` (docs/query-engine.md).
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// Current time, floored to a whole second so every derived sample
/// timestamp is an exact-second value (avoids float precision concerns
/// when round-tripping through the `time`/`start`/`end` query params).
fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name.to_string())
}

fn make_labels(metric: &str, extra: &[(&str, &str)]) -> LabelSet {
    let mut pairs = vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }];
    for (k, v) in extra {
        pairs.push(Label {
            name: (*k).to_string(),
            value: (*v).to_string(),
        });
    }
    LabelSet::new(pairs).expect("valid labels")
}

fn series_input(
    tenant_id: &TenantId,
    metric: &str,
    extra: &[(&str, &str)],
    samples: &[(i64, f64)],
) -> SeriesInput {
    let label_set = make_labels(metric, extra);
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }
}

/// Writes a real RSEG segment and publishes its commit record, using
/// `footer_epoch` for the on-disk segment identity and `record_epoch` for
/// the published commit record's identity. Equal for every normal-path
/// test; deliberately different for the identity-mismatch test.
async fn publish_segment(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    shard: u32,
    writer_id: Uuid,
    footer_epoch: u64,
    record_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    series: Vec<SeriesInput>,
) -> (CommitToken, String) {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: footer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: record_epoch,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

/// Percent-encodes everything except unreserved characters, so arbitrary
/// PromQL text (braces, quotes, `=`) survives as a URI query component and
/// round-trips through the crate's own hand-rolled decoder.
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

fn build_app(
    store: Arc<MemoryStore>,
    catalog_config: CatalogConfig,
    engine_config: EngineConfig,
    tokens: HashMap<String, TenantId>,
) -> Router {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog = Arc::new(Catalog::new(backend.clone(), catalog_config).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, engine_config));
    let state = AppState {
        engine,
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
    };
    router(state)
}

fn one_tenant_app(
    store: Arc<MemoryStore>,
    engine_config: EngineConfig,
    tenant_id: &TenantId,
    token: &str,
) -> Router {
    let mut tokens = HashMap::new();
    tokens.insert(token.to_string(), tenant_id.clone());
    build_app(store, CatalogConfig::default(), engine_config, tokens)
}

async fn call(app: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, Value) {
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
    let json: Value = serde_json::from_slice(&body).expect("parse response json");
    (status, json)
}

fn vector_results(body: &Value) -> &Vec<Value> {
    body["data"]["result"]
        .as_array()
        .expect("vector result array")
}

fn value_string(sample: &Value) -> &str {
    sample["value"][1].as_str().expect("sample value string")
}

#[tokio::test]
async fn instant_query_returns_expected_value() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "get")],
            &[(now - NS_PER_MIN, 42.0)],
        ),
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "post")],
            &[(now - NS_PER_MIN, 7.0)],
        ),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("http_requests_total{method=\"get\"}");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["metric"]["method"], "get");
    assert_eq!(value_string(&results[0]), "42");
}

#[tokio::test]
async fn range_query_returns_expected_grid() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "queue_depth",
        &[],
        &[(now - 4 * NS_PER_MIN, 10.0), (now - 2 * NS_PER_MIN, 20.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("queue_depth");
    let start = (now - 4 * NS_PER_MIN) / NS_PER_SEC;
    let end = (now - NS_PER_MIN) / NS_PER_SEC;
    let uri = format!("/api/v1/query_range?query={query}&start={start}&end={end}&step=60s");
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let results = body["data"]["result"]
        .as_array()
        .expect("matrix result array");
    assert_eq!(results.len(), 1);
    let values = results[0]["values"].as_array().expect("values array");
    let observed: Vec<&str> = values
        .iter()
        .map(|v| v[1].as_str().expect("value string"))
        .collect();
    assert_eq!(observed, vec!["10", "10", "20", "20"]);
}

#[tokio::test]
async fn lookback_finds_sample_in_an_earlier_segment() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // Segment A (earlier flush): the only sample at or before the query
    // instant.
    let seg_a = vec![series_input(
        &tid,
        "cpu_seconds",
        &[],
        &[(now - 4 * NS_PER_MIN, 1.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        seg_a,
    )
    .await;

    // Segment B (a later, separate flush): its sample is after the query
    // instant, so lookback must not pick it.
    let seg_b = vec![series_input(
        &tid,
        "cpu_seconds",
        &[],
        &[(now - 30 * NS_PER_SEC, 99.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        seg_b,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("cpu_seconds");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - 2 * NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "1");
}

#[tokio::test]
async fn cross_segment_duplicate_sample_resolves_to_greatest_writer_seq() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");
    let writer_id = Uuid::new_v4();
    let ts = now - NS_PER_MIN;

    // Same series, same timestamp, same writer/epoch/created_unix_ns:
    // only writer_seq differs, so it is the deciding tiebreak
    // (ADR-0010 SS5 total order).
    let older = vec![series_input(&tid, "duplicated_metric", &[], &[(ts, 100.0)])];
    publish_segment(&store, th, 0, writer_id, 1, 1, 1, hour_bucket, now, older).await;
    let newer = vec![series_input(&tid, "duplicated_metric", &[], &[(ts, 200.0)])];
    publish_segment(&store, th, 0, writer_id, 1, 1, 2, hour_bucket, now, newer).await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("duplicated_metric");
    let uri = format!("/api/v1/query?query={query}&time={}", ts / NS_PER_SEC);
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "200");
}

#[tokio::test]
async fn min_commit_token_finds_segment_outside_the_listing_window() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let ts = now - NS_PER_MIN;

    // ingest_hour_bucket 0 (1970-01-01T00) is nowhere near the catalog's
    // real-time-based listing window, so ordinary listing will never find
    // this commit record; only an explicit min_commit_token GET can.
    let (token, _) = publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        0,
        now,
        vec![series_input(&tid, "delayed_metric", &[], &[(ts, 55.0)])],
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("delayed_metric");

    let without_token = format!("/api/v1/query?query={query}&time={}", ts / NS_PER_SEC);
    let (status, body) = call(&app, &without_token, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        vector_results(&body).len(),
        0,
        "must not be visible without the token"
    );

    let with_token = format!(
        "/api/v1/query?query={query}&time={}&min_commit_token={}",
        ts / NS_PER_SEC,
        token.encode()
    );
    let (status, body) = call(&app, &with_token, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(
        results.len(),
        1,
        "must be visible with the read-your-write token"
    );
    assert_eq!(value_string(&results[0]), "55");
}

#[tokio::test]
async fn exceeding_max_series_budget_returns_422() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(&tid, "up", &[("instance", "a")], &[(now - NS_PER_MIN, 1.0)]),
        series_input(&tid, "up", &[("instance", "b")], &[(now - NS_PER_MIN, 1.0)]),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let config = EngineConfig {
        max_series: 1,
        ..EngineConfig::default()
    };
    let app = one_tenant_app(store, config, &tid, "secret-a");
    let query = encode_query_param("up");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
}

#[tokio::test]
async fn identity_mismatch_between_commit_record_and_footer_returns_503() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "mismatched_metric",
        &[],
        &[(now - NS_PER_MIN, 1.0)],
    )];
    // Footer declares writer_epoch 7; the published commit record declares
    // writer_epoch 9. The fetcher must detect this and fail hard rather
    // than return wrong data.
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        7,
        9,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("mismatched_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "unavailable");
}

#[tokio::test]
async fn corrupt_segment_bytes_return_503_not_wrong_data() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "corrupt_metric",
        &[],
        &[(now - NS_PER_MIN, 1.0)],
    )];
    let (_, data_key) = publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    // Flip the last byte of the object: the trailer's final 4 bytes are
    // the RSEG magic, so this deterministically fails footer parsing
    // (`SegmentError::BadMagic`) regardless of internal layout details.
    let existing = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get object");
    let mut corrupted = existing.data.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xFF;
    store
        .put(&data_key, Bytes::from(corrupted), PutOptions::default())
        .await
        .expect("overwrite object");

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("corrupt_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "unavailable");
}

#[tokio::test]
async fn auth_rejects_missing_and_unknown_tokens() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("anything");
    let uri = format!("/api/v1/query?query={query}&time=0");

    let (status, body) = call(&app, &uri, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["errorType"], "unauthorized");

    let (status, body) = call(&app, &uri, Some("wrong-token")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["errorType"], "unauthorized");
}

#[tokio::test]
async fn cross_tenant_data_is_isolated() {
    let store = Arc::new(MemoryStore::new());
    let tenant_a = tenant("tenant-a");
    let tenant_b = tenant("tenant-b");
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // Only tenant B has data.
    let series = vec![series_input(
        &tenant_b,
        "secret_metric",
        &[],
        &[(now - NS_PER_MIN, 9.0)],
    )];
    publish_segment(
        &store,
        tenant_b.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let mut tokens = HashMap::new();
    tokens.insert("token-a".to_string(), tenant_a);
    tokens.insert("token-b".to_string(), tenant_b);
    let app = build_app(
        store,
        CatalogConfig::default(),
        EngineConfig::default(),
        tokens,
    );

    let query = encode_query_param("secret_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );

    let (status, body) = call(&app, &uri, Some("token-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        vector_results(&body).len(),
        0,
        "tenant A must not see tenant B's series"
    );

    let (status, body) = call(&app, &uri, Some("token-b")).await;
    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "9");
}

#[tokio::test]
async fn series_endpoint_reflects_published_labels() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "get")],
            &[(now - NS_PER_MIN, 1.0)],
        ),
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "post")],
            &[(now - NS_PER_MIN, 1.0)],
        ),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let matcher = encode_query_param("http_requests_total");
    let uri = format!("/api/v1/series?match%5B%5D={matcher}");
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = body["data"].as_array().expect("series result array");
    assert_eq!(results.len(), 2);
    let mut methods: Vec<&str> = results
        .iter()
        .map(|s| s["method"].as_str().expect("method label"))
        .collect();
    methods.sort_unstable();
    assert_eq!(methods, vec!["get", "post"]);
}

#[tokio::test]
async fn series_endpoint_requires_match_param() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let (status, body) = call(&app, "/api/v1/series", Some("secret-a")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["errorType"], "bad_data");
}
