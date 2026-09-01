//! End-to-end tests for `POST /api/v1/analytics` (ADR-0028).
//!
//! Real RSEG segments, real commit records, a real catalog and query engine,
//! and the route driven through `tower::ServiceExt::oneshot`, so everything
//! asserted here is what a client would actually receive. This mirrors the
//! harness in `sql_endpoint.rs`: ingest is exercised for real, then the
//! analytics endpoint runs the same range evaluation `/api/v1/query_range`
//! would, and the op is applied to the resulting matrix.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::float_cmp)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::analytics::{AnalyticsState, router};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// Frozen clock. Kept a few hours ahead of every sample so the default query
/// window and lookback comfortably cover the ingested series, while staying
/// small enough that `Catalog::resolve`'s per-hour LIST fan-out is cheap.
const NOW_NS: i64 = 6 * NS_PER_HOUR;

struct FixedClock;
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn labels_for(metric: &str, extra: &[(&str, &str)]) -> LabelSet {
    let mut labels = vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }];
    for (k, v) in extra {
        labels.push(Label {
            name: k.to_string(),
            value: v.to_string(),
        });
    }
    LabelSet::new(labels).expect("valid labels")
}

/// Publish one segment holding `series`, for `tenant`.
async fn publish(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    series: Vec<SeriesInput>,
) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(3_000 + index as u128);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
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
        writer_seq: index as u64 + 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10 + index as i64,
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

fn one_series(tenant: &TenantId, metric: &str, samples: &[(i64, f64)]) -> SeriesInput {
    let label_set = labels_for(metric, &[]);
    SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
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

fn build_router(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> Router {
    build_router_with_sink(store, tokens, Arc::new(ravel_maintain::NoopQueryAuditSink))
}

fn build_router_with_sink(
    store: Arc<dyn ObjectStoreBackend>,
    tokens: HashMap<String, TenantId>,
    audit_sink: Arc<dyn ravel_maintain::QueryAuditSink>,
) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, store, EngineConfig::default());
    router(AnalyticsState {
        engine: Arc::new(engine),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        clock: Arc::new(FixedClock),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        audit_sink,
    })
}

/// Like [`build_router`], but the query engine has an ADR-0071 distributed
/// context attached with an always-open cost gate. The live worker set is
/// empty, so every slice self-maps and runs locally through the
/// `RoutingSliceFetcher` (no network), which still records a per-slice
/// `stats.fragments[]` entry, exactly the surface finding 4 delivers.
fn build_router_distributed(
    store: Arc<dyn ObjectStoreBackend>,
    tokens: HashMap<String, TenantId>,
) -> Router {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_query::distrib::Distributed;
    use ravel_query::distrib::partition::DistribThresholds;
    use ravel_server::distrib::{
        FragmentAdmission, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let metrics = Arc::new(FragmentMetrics::new());
    let clock: Arc<dyn Clock> = Arc::new(FixedClock);
    let service = FragmentService::new(
        Arc::new(vec![[0u8; 32]]),
        Arc::new(StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        FragmentAdmission::new(8, metrics.clone()),
        catalog.clone(),
        Arc::clone(&store),
        None,
        clock.clone(),
        metrics.clone(),
    );
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        Arc::new(OnceLock::new()),
        Arc::new(RwLock::new(Arc::new(Vec::new()))),
        Arc::new(vec![[0u8; 32]]),
        service,
        metrics,
    ));
    let distributed = Arc::new(Distributed::new(
        fetcher,
        DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
    ));
    let engine =
        QueryEngine::new(catalog, store, EngineConfig::default()).with_distributed(distributed);
    router(AnalyticsState {
        engine: Arc::new(engine),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        clock,
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

fn tokens(pairs: &[(&str, &str)]) -> HashMap<String, TenantId> {
    pairs
        .iter()
        .map(|(token, tenant)| (token.to_string(), TenantId::new(tenant.to_string())))
        .collect()
}

/// A request body over an explicit `[start_sec, end_sec]` window and step. The
/// window is chosen to land one grid instant on each ingested sample so the
/// evaluated matrix recovers the injected values exactly (no carry-forward or
/// staleness gaps).
fn analytics_body(query: &str, start_sec: f64, end_sec: f64, step: &str, op: Value) -> String {
    serde_json::json!({
        "query": query,
        "start": start_sec,
        "end": end_sec,
        "step": step,
        "op": op,
    })
    .to_string()
}

async fn post(app: &Router, token: Option<&str>, payload: String) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/analytics")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = builder.body(Body::from(payload)).expect("build request");
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
    (status, bytes)
}

async fn post_json(app: &Router, token: &str, payload: String) -> (StatusCode, Value) {
    let (status, bytes) = post(app, Some(token), payload).await;
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

/// A single-series app whose one series carries `samples`.
async fn single_series_app(metric: &str, samples: &[(i64, f64)]) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish(
        store.as_ref(),
        &tenant,
        0,
        vec![one_series(&tenant, metric, samples)],
    )
    .await;
    build_router(store, tokens(&[("acme-token", "acme")]))
}

/// Samples on a fixed step grid starting at t=0, one sample per grid instant,
/// so a range query over `[0, (n-1)*step]` with the same step recovers each
/// value exactly. Returns `(samples, end_sec)` for the matching query window.
fn grid_samples(step_ns: i64, values: &[f64]) -> (Vec<(i64, f64)>, f64) {
    let samples: Vec<(i64, f64)> = values
        .iter()
        .enumerate()
        .map(|(i, v)| (i as i64 * step_ns, *v))
        .collect();
    let end_sec = (values.len() as i64 - 1) as f64 * step_ns as f64 / NS_PER_SEC as f64;
    (samples, end_sec)
}

// ---------------------------------------------------------------------------
// change_point
// ---------------------------------------------------------------------------

/// End-to-end: ingest a series with a level shift halfway, then change_point
/// reports `step_change` located near the injected boundary.
#[tokio::test]
async fn change_point_detects_an_injected_step() {
    let step_ns = 10 * NS_PER_SEC;
    // 60 points: a low level for the first 30, a high level for the last 30,
    // with tiny deterministic jitter so neither segment is perfectly constant.
    let values: Vec<f64> = (0..60)
        .map(|i| {
            let base = if i < 30 { 10.0 } else { 30.0 };
            base + ((i % 5) as f64) * 0.01
        })
        .collect();
    let (samples, end_sec) = grid_samples(step_ns, &values);
    let app = single_series_app("cp_metric", &samples).await;

    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "cp_metric",
            0.0,
            end_sec,
            "10s",
            serde_json::json!({"type": "change_point"}),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success");
    assert_eq!(value["data"]["resultType"], "analytics");
    let result = value["data"]["result"].as_array().expect("result array");
    assert_eq!(result.len(), 1, "one series expected: {value}");
    let cp = &result[0]["result"];
    assert_eq!(cp["kind"], "step_change", "{cp}");
    assert!(!cp["downsampled"].as_bool().expect("downsampled"));
    assert_eq!(cp["original_points"], 60);
    assert_eq!(cp["nan_excluded"], 0);

    // The boundary is between index 29 and 30, i.e. t in [30 s, 31 s]; accept a
    // small tolerance window around it (documented tolerance, ADR-0028).
    let ts_ns = cp["ts_ns"].as_i64().expect("ts_ns present");
    let boundary = 30 * step_ns;
    let tolerance = 5 * step_ns;
    assert!(
        (ts_ns - boundary).abs() <= tolerance,
        "change located at {ts_ns} ns, expected near {boundary} ns"
    );
}

/// ADR-0071 (finding 4): a distributed analytics run attaches a per-slice
/// `stats.fragments[]` to the response with the delivered field shape, and a
/// non-distributed run omits the field entirely.
#[tokio::test]
async fn analytics_stats_carries_fragments_only_when_distributed() {
    let step_ns = 10 * NS_PER_SEC;
    let values: Vec<f64> = (0..60).map(|i| 10.0 + (i % 5) as f64 * 0.01).collect();
    let (samples, end_sec) = grid_samples(step_ns, &values);
    let body = || {
        analytics_body(
            "frag_metric",
            0.0,
            end_sec,
            "10s",
            serde_json::json!({"type": "change_point"}),
        )
    };
    let tenant = TenantId::new("acme".to_string());

    // Distributed: fragments[] present, one entry per fanned-out slice with the
    // delivered field shape (worker endpoint, segment count, reported bytes,
    // outcome).
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish(
        store.as_ref(),
        &tenant,
        0,
        vec![one_series(&tenant, "frag_metric", &samples)],
    )
    .await;
    let distributed =
        build_router_distributed(Arc::clone(&store), tokens(&[("acme-token", "acme")]));
    let (status, value) = post_json(&distributed, "acme-token", body()).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let fragments = value["stats"]["fragments"]
        .as_array()
        .unwrap_or_else(|| panic!("a distributed run must carry a fragments array: {value}"));
    assert!(
        !fragments.is_empty(),
        "at least one slice fanned out: {value}"
    );
    let first = &fragments[0];
    assert!(first["workerEndpoint"].is_string(), "{first}");
    assert!(first["segmentCount"].is_u64(), "{first}");
    assert!(first["bytesReported"].is_u64(), "{first}");
    assert_eq!(
        first["status"], "ok",
        "the self-mapped local slice ran to completion: {first}"
    );

    // Non-distributed: no fragments field at all.
    let store2: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish(
        store2.as_ref(),
        &tenant,
        0,
        vec![one_series(&tenant, "frag_metric", &samples)],
    )
    .await;
    let plain = build_router(Arc::clone(&store2), tokens(&[("acme-token", "acme")]));
    let (status, value) = post_json(&plain, "acme-token", body()).await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(
        value["stats"]["fragments"].is_null(),
        "a non-distributed run must omit fragments: {value}"
    );
}

/// A series longer than 2000 points errors without `downsample`, and succeeds
/// with `downsample: true`, reporting the original point count.
#[tokio::test]
async fn over_cap_series_requires_downsample() {
    let step_ns = NS_PER_SEC;
    // 2500 evaluated points: over the 2000-point change_point cap, under the
    // query engine's own resolution budget.
    let values: Vec<f64> = (0..2500)
        .map(|i| {
            let base = if i < 1250 { 5.0 } else { 25.0 };
            base + ((i % 7) as f64) * 0.001
        })
        .collect();
    let (samples, end_sec) = grid_samples(step_ns, &values);
    let app = single_series_app("big_metric", &samples).await;

    // Without downsample: a 422 analytics error.
    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "big_metric",
            0.0,
            end_sec,
            "1s",
            serde_json::json!({"type": "change_point"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
    assert_eq!(value["status"], "error");
    assert!(
        value["error"].as_str().expect("message").contains("2000"),
        "error should name the cap: {value}"
    );

    // With downsample: success, downsampled true, original count preserved.
    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "big_metric",
            0.0,
            end_sec,
            "1s",
            serde_json::json!({"type": "change_point", "downsample": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let cp = &value["data"]["result"][0]["result"];
    assert!(cp["downsampled"].as_bool().expect("downsampled"));
    assert_eq!(cp["original_points"], 2500, "{cp}");
}

// ---------------------------------------------------------------------------
// summary
// ---------------------------------------------------------------------------

/// summary results match an independent naive reference computed in-test over
/// the same values, using the same algorithms ADR-0028 fixes.
#[tokio::test]
async fn summary_matches_a_naive_reference() {
    let step_ns = 10 * NS_PER_SEC;
    let values: Vec<f64> = [
        3.0, 1.0, 4.0, 1.5, 5.0, 9.0, 2.0, 6.0, 5.0, 3.0, 5.0, 8.0, 9.0, 7.0, 9.0, 3.0, 2.0, 3.0,
        8.0, 4.0, 6.0, 2.0, 6.0, 4.0, 3.0,
    ]
    .to_vec();
    let (samples, end_sec) = grid_samples(step_ns, &values);
    let app = single_series_app("sum_metric", &samples).await;

    let quantiles = [0.5, 0.9, 0.25];
    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "sum_metric",
            0.0,
            end_sec,
            "10s",
            serde_json::json!({"type": "summary", "percentiles": quantiles}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let s = &value["data"]["result"][0]["result"];

    let (median, mad, stddev, variance) = naive_moments(&values);
    assert_eq!(s["median"].as_f64().expect("median"), median);
    assert_eq!(s["mad"].as_f64().expect("mad"), mad);
    assert_eq!(s["stddev"].as_f64().expect("stddev"), stddev);
    assert_eq!(s["variance"].as_f64().expect("variance"), variance);
    assert_eq!(s["nan_excluded"], 0);

    let got = s["percentiles"].as_array().expect("percentiles");
    assert_eq!(got.len(), quantiles.len());
    for (entry, &q) in got.iter().zip(quantiles.iter()) {
        assert_eq!(entry["quantile"].as_f64().expect("q"), q);
        assert_eq!(
            entry["value"].as_f64().expect("v"),
            naive_percentile(&values, q),
            "percentile {q} mismatch"
        );
    }
}

/// Selection statistics sort by `f64::total_cmp`; moments fold naively in
/// grid order. Reference reproduces both.
fn naive_moments(values: &[f64]) -> (f64, f64, f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let median = median_of(&sorted);
    let mut devs: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
    devs.sort_by(|a, b| a.total_cmp(b));
    let mad = median_of(&devs);

    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n;
    (median, mad, variance.sqrt(), variance)
}

fn median_of(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Prometheus interpolation between the two ranks straddling `q * (n - 1)`.
fn naive_percentile(values: &[f64], q: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let rank = q * (sorted.len() as f64 - 1.0);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let frac = rank - lower as f64;
    sorted[lower] + frac * (sorted[upper] - sorted[lower])
}

// ---------------------------------------------------------------------------
// Budgets and request errors
// ---------------------------------------------------------------------------

/// More than 1000 matching series is a 422 before any op runs (ADR-0028
/// decision 4).
#[tokio::test]
async fn the_series_cap_is_a_422() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    // 1001 distinct series under one metric name, each with two samples.
    let series: Vec<SeriesInput> = (0..1001)
        .map(|i| {
            let label_set = labels_for("cap_metric", &[("id", &i.to_string())]);
            SeriesInput {
                series_id: SeriesId::compute(&tenant, "cap_metric", &label_set).expect("id"),
                labels: label_set,
                samples: vec![
                    Sample {
                        ts_ns: NS_PER_SEC,
                        value: 1.0,
                    },
                    Sample {
                        ts_ns: 2 * NS_PER_SEC,
                        value: 2.0,
                    },
                ],
            }
        })
        .collect();
    publish(store.as_ref(), &tenant, 0, series).await;
    let app = build_router(store, tokens(&[("acme-token", "acme")]));

    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "cap_metric",
            1.0,
            2.0,
            "1s",
            serde_json::json!({"type": "summary"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
    assert_eq!(value["status"], "error");
    assert!(
        value["error"].as_str().expect("message").contains("1000"),
        "error should name the cap: {value}"
    );
}

/// An unknown op type is a 400 (malformed body).
#[tokio::test]
async fn an_unknown_op_type_is_a_400() {
    let app = single_series_app("m", &[(NS_PER_SEC, 1.0)]).await;
    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body("m", 1.0, 1.0, "1s", serde_json::json!({"type": "forecast"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["errorType"], "bad_data");
}

/// A percentile outside [0, 1] is a 422, rejected up front.
#[tokio::test]
async fn an_invalid_percentile_is_a_422() {
    let (samples, end_sec) = grid_samples(10 * NS_PER_SEC, &[1.0, 2.0, 3.0, 4.0, 5.0]);
    let app = single_series_app("m", &samples).await;
    let (status, value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "m",
            0.0,
            end_sec,
            "10s",
            serde_json::json!({"type": "summary", "percentiles": [0.5, 1.5]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
    assert_eq!(value["status"], "error");
    assert!(
        value["error"].as_str().expect("message").contains("1.5"),
        "error should name the offending percentile: {value}"
    );
}

/// start/end/step parse errors behave exactly as `/api/v1/query_range`: a 400
/// `bad_data` naming the offending field, before any query runs.
#[tokio::test]
async fn a_step_parse_error_is_a_400() {
    let app = single_series_app("m", &[(NS_PER_SEC, 1.0)]).await;
    let payload = serde_json::json!({
        "query": "m",
        "start": 0.0,
        "end": 100.0,
        "step": "not-a-duration",
        "op": {"type": "summary"},
    })
    .to_string();
    let (status, value) = post_json(&app, "acme-token", payload).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["errorType"], "bad_data");
    assert!(
        value["error"].as_str().expect("message").contains("step"),
        "error should name the offending field: {value}"
    );
}

/// A malformed JSON body is a 400, not a 500.
#[tokio::test]
async fn a_malformed_body_is_a_400() {
    let app = single_series_app("m", &[(NS_PER_SEC, 1.0)]).await;
    let (status, value) = post_json(&app, "acme-token", "{ not json".to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["errorType"], "bad_data");
}

/// An unauthenticated request is rejected before any work.
#[tokio::test]
async fn an_unauthenticated_request_is_a_401() {
    let app = single_series_app("m", &[(NS_PER_SEC, 1.0)]).await;
    let (status, bytes) = post(
        &app,
        None,
        analytics_body("m", 1.0, 1.0, "1s", serde_json::json!({"type": "summary"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["errorType"], "unauthorized");
}

// ---------------------------------------------------------------------------
// Evidential audit coverage (ADR-0062 §2a)
// ---------------------------------------------------------------------------

/// Captures every submitted audit event and reports durability success.
struct RecordingSink {
    events: Arc<Mutex<Vec<ravel_maintain::AuditEvent>>>,
}

#[async_trait]
impl ravel_maintain::QueryAuditSink for RecordingSink {
    async fn submit(
        &self,
        event: ravel_maintain::AuditEvent,
    ) -> Result<(), ravel_maintain::MaintainError> {
        self.events.lock().expect("lock").push(event);
        Ok(())
    }
}

/// The value of a string `attrs` entry.
fn audit_attr<'a>(event: &'a ravel_maintain::AuditEvent, key: &str) -> Option<&'a str> {
    event
        .attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            ravel_logseg::AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

/// An executed analytics query submits exactly one audit event through the
/// sink, with `query.language=analytics` and the outcome status, and awaits its
/// durability before the response is released.
#[tokio::test]
async fn analytics_submits_one_audit_event_per_executed_query() {
    let step_ns = 10 * NS_PER_SEC;
    let values: Vec<f64> = (0..12).map(|i| 5.0 + (i % 3) as f64).collect();
    let (samples, end_sec) = grid_samples(step_ns, &values);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish(
        store.as_ref(),
        &tenant,
        0,
        vec![one_series(&tenant, "au_metric", &samples)],
    )
    .await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let app = build_router_with_sink(
        store,
        tokens(&[("acme-token", "acme")]),
        Arc::new(RecordingSink {
            events: Arc::clone(&events),
        }),
    );

    let (status, _value) = post_json(
        &app,
        "acme-token",
        analytics_body(
            "au_metric",
            0.0,
            end_sec,
            "10s",
            serde_json::json!({"type": "summary", "percentiles": [0.5]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let guard = events.lock().expect("lock");
    assert_eq!(
        guard.len(),
        1,
        "exactly one audit event per analytics query"
    );
    let event = &guard[0];
    assert_eq!(audit_attr(event, "kind"), Some("query"));
    assert_eq!(audit_attr(event, "query.language"), Some("analytics"));
    assert_eq!(audit_attr(event, "query.status"), Some("ok"));
    assert_eq!(audit_attr(event, "query.text"), Some("au_metric"));
    assert_eq!(
        audit_attr(event, "query.tenant"),
        Some(tenant.hash().to_hex().as_str())
    );
}

// ---------------------------------------------------------------------------
// ADR-0071 partial-coverage consent gate ("partial results are consent-gated
// and envelope-visible" amendment, section 2: the analytics endpoint bypasses
// the HTTP query endpoints' gate, so it carries its own).
// ---------------------------------------------------------------------------

/// The degraded cluster the gate tests federate to. It must appear in both the
/// refusal message and the opted-in response's `warnings`.
const DEAD_CLUSTER: &str = "west";
const GATE_OPERATOR_CRED: &str = "operator-west-credential";

/// An address bound then immediately released: a connection to it is refused,
/// so a remote pointed here is unavailable at query time (the same technique
/// `federation_handler_warnings.rs` uses).
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind dead");
    let addr = listener.local_addr().expect("dead local addr");
    drop(listener);
    addr.to_string()
}

/// A `Federation` over one unavailable remote cluster that opted into
/// `skip_unavailable`, dialed by the production `FederationSliceFetcher`
/// exactly as `--remote-cluster` wires it. The fan-out skips it and records a
/// partial-coverage warning naming the cluster, which is the only way this
/// endpoint can observe partial coverage at all (intra-cluster execution never
/// returns partial results).
fn dead_federation(name: &str, endpoint: &str) -> ravel_query::distrib::Federation {
    use ravel_query::distrib::{Federation, RemoteCluster};
    use ravel_server::config::RemoteClusterConfig;

    let config = RemoteClusterConfig {
        name: name.to_string(),
        endpoint: endpoint.to_string(),
        credential: GATE_OPERATOR_CRED.to_string(),
        tls: false,
        tls_ca_file: None,
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    };
    let fetcher = ravel_server::distrib::FederationSliceFetcher::connect(&config)
        .expect("federation client connects lazily");
    Federation::new(vec![RemoteCluster {
        name: name.to_string(),
        fetcher: Arc::new(fetcher),
        skip_unavailable: true,
        soft_timeout: Duration::from_secs(3),
    }])
}

/// The analytics router over a local store holding `samples`, with a federation
/// attached over one dead, skippable remote, so every evaluation this router
/// runs has partial coverage.
async fn federated_app(metric: &str, samples: &[(i64, f64)]) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish(
        store.as_ref(),
        &tenant,
        0,
        vec![one_series(&tenant, metric, samples)],
    )
    .await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let federation = Arc::new(dead_federation(DEAD_CLUSTER, &dead_endpoint().await));
    let engine =
        QueryEngine::new(catalog, store, EngineConfig::default()).with_federation(federation);
    router(AnalyticsState {
        engine: Arc::new(engine),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens(&[(
            "acme-token",
            "acme",
        )]))),
        clock: Arc::new(FixedClock),
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    })
}

/// A `summary` request over the whole ingested window. `allow_partial` is
/// omitted entirely when `allow_partial` is `None`, which is the "client never
/// asked" case the gate must refuse.
fn gate_body(metric: &str, end_sec: f64, allow_partial: Option<bool>) -> String {
    let mut body = serde_json::json!({
        "query": metric,
        "start": 0.0,
        "end": end_sec,
        "step": "10s",
        "op": {"type": "summary", "percentiles": [0.5]},
    });
    if let Some(allow) = allow_partial {
        body["allow_partial"] = serde_json::json!(allow);
    }
    body.to_string()
}

/// Samples for the gate tests: a plain 10-second grid, enough points for
/// `summary` to be meaningful and few enough to keep the run cheap.
fn gate_samples() -> (Vec<(i64, f64)>, f64) {
    let values: Vec<f64> = (0..12).map(|i| 10.0 + i as f64).collect();
    grid_samples(10 * NS_PER_SEC, &values)
}

/// Acceptance 1: partial coverage with no `allow_partial` in the body is
/// refused with the typed 503 the HTTP query endpoints use, and the refusal
/// names both the degraded cluster and the remedy.
///
/// Flip-line proof (prove-the-test evidence): delete the
/// `gate_partial(&coverage, body.allow_partial)?;` line in
/// `services/ravel-server/src/analytics.rs::run` (the line right after
/// `let coverage = Coverage::from_stats(&stats);`). Without it the request is a
/// 200 carrying an answer that silently omits the skipped cluster's data, and
/// the `SERVICE_UNAVAILABLE` assertion below fails. Verified failing against
/// the pre-change handler, which had no gate at all.
#[tokio::test]
async fn partial_coverage_without_consent_is_refused() {
    let (samples, end_sec) = gate_samples();
    let app = federated_app("gate_metric", &samples).await;

    let (status, value) =
        post_json(&app, "acme-token", gate_body("gate_metric", end_sec, None)).await;

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "partial coverage the client did not opt into must be refused: {value}"
    );
    assert_eq!(value["status"], "error", "{value}");
    assert_eq!(value["errorType"], "unavailable", "{value}");
    let message = value["error"]
        .as_str()
        .unwrap_or_else(|| panic!("the refusal must carry an error message: {value}"));
    assert!(
        message.contains(DEAD_CLUSTER),
        "the refusal must name the degraded cluster: {message:?}"
    );
    assert!(
        message.contains("allow_partial"),
        "the refusal must name the remedy: {message:?}"
    );
}

/// Acceptance 2: the same partial evaluation with `allow_partial: true` is a
/// 200 whose envelope states the coverage: top-level `partial` is `true` and
/// top-level `warnings` names the skipped cluster.
///
/// Flip-line proof: delete the `"partial": coverage.is_partial(),` and
/// `"warnings": stats.warnings,` entries from the `json!` response envelope in
/// `services/ravel-server/src/analytics.rs::run`. Both assertions below then
/// fail (the fields read as JSON null), while the request still returns 200 --
/// exactly the "a partial answer is indistinguishable from a complete one"
/// defect this pins. Verified failing against the pre-change handler, whose
/// envelope carried only `status`/`data`/`stats`.
#[tokio::test]
async fn partial_coverage_with_consent_is_reported_on_the_envelope() {
    let (samples, end_sec) = gate_samples();
    let app = federated_app("gate_metric", &samples).await;

    let (status, value) = post_json(
        &app,
        "acme-token",
        gate_body("gate_metric", end_sec, Some(true)),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "opted-in partial must succeed: {value}"
    );
    assert_eq!(
        value["partial"],
        serde_json::json!(true),
        "the envelope's top-level partial field must state the coverage: {value}"
    );
    let warnings = value["warnings"]
        .as_array()
        .unwrap_or_else(|| panic!("the envelope must carry a warnings array: {value}"));
    assert!(
        !warnings.is_empty(),
        "a skipped remote must produce at least one warning: {value}"
    );
    assert!(
        warnings.iter().any(|w| w
            .as_str()
            .map(|s| s.contains(DEAD_CLUSTER))
            .unwrap_or(false)),
        "the skipped cluster must be named in the warnings: {warnings:?}"
    );
    // The op still ran over the coverage that was available.
    assert_eq!(value["data"]["resultType"], "analytics", "{value}");
    assert!(
        value["data"]["result"].as_array().is_some(),
        "the partial answer still carries its result array: {value}"
    );
}

/// Acceptance 3: complete coverage is a 200 with `partial: false` and no
/// warnings, with or without the opt-in, and the rest of the envelope is
/// unchanged (`data` and `stats` still present and shaped as before).
#[tokio::test]
async fn complete_coverage_reports_not_partial_either_way() {
    let (samples, end_sec) = gate_samples();

    for allow_partial in [None, Some(false), Some(true)] {
        let app = single_series_app("gate_metric", &samples).await;
        let (status, value) = post_json(
            &app,
            "acme-token",
            gate_body("gate_metric", end_sec, allow_partial),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{allow_partial:?}: {value}");
        assert_eq!(
            value["partial"],
            serde_json::json!(false),
            "complete coverage must report partial=false ({allow_partial:?}): {value}"
        );
        assert_eq!(
            value["warnings"],
            serde_json::json!([]),
            "complete coverage carries no warnings ({allow_partial:?}): {value}"
        );

        // The pre-existing envelope, unchanged: status, the analytics data
        // block with one entry per series, and the accounting stats block.
        assert_eq!(value["status"], "success", "{value}");
        assert_eq!(value["data"]["resultType"], "analytics", "{value}");
        let result = value["data"]["result"]
            .as_array()
            .unwrap_or_else(|| panic!("data.result must be an array: {value}"));
        assert_eq!(
            result.len(),
            1,
            "one entry for the one ingested series: {value}"
        );
        assert_eq!(result[0]["metric"]["__name__"], "gate_metric", "{value}");
        assert!(
            result[0]["result"]["median"].is_f64(),
            "the summary op result is unchanged: {value}"
        );
        assert!(
            value["stats"]["accounting"].is_object(),
            "the stats block still carries accounting: {value}"
        );
        assert!(
            value["stats"]["estimate"].is_object(),
            "the stats block still carries the estimate: {value}"
        );
    }
}
