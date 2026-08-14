//! In-crate acceptance test for per-query cost accounting: it must
//! reach both a query response's `stats` object and the `/metrics` exposition,
//! and it must not perturb the query result.
//!
//! This drives the real `POST /api/v1/sql` handler and the real `GET /metrics`
//! handler over a shared [`QueryAccountingMetrics`] instance, exactly as
//! [`crate::start`] wires them together, so what is asserted is what an
//! operator and a client would actually observe. The SQL path is used because
//! its `SqlExecutor` returns the accounting snapshot and the cost estimate
//! directly (`ravel_sql::SqlOutcome`), so one path exercises the whole chain
//! end to end under the `sql` feature the gate builds.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::{AdmissionController, AdmissionLimits, Clock, SystemClock};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use tower::ServiceExt;
use uuid::Uuid;

use crate::config::Mode;
use crate::metrics::{MetricsState, QueryAccountingMetrics};
use crate::sql::{SqlState, router as sql_router};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose, mirroring `tests/sql_endpoint.rs`: `Catalog::resolve`
/// issues one LIST per (shard, ingest-hour) pair across the window, so a
/// wall-clock value would fan out to hundreds of thousands of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

/// Publish one real RSEG segment plus its commit record for `tenant`, so a
/// `SELECT ... FROM samples` resolves and fetches real data and the accounting
/// counters are non-trivial.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    samples: &[(i64, f64)],
) {
    let tenant_hash = tenant.hash();
    let metric = "m";
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
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

/// The shared handles a query path and the `/metrics` path both hold, built
/// once and cloned into each so they observe the same aggregator and catalog,
/// exactly as `crate::start` wires them.
struct Harness {
    sql: Router,
    metrics: Router,
    executor: Arc<SqlExecutor>,
}

fn harness(store: Arc<dyn ObjectStoreBackend>, configured: HashSet<TenantHash>) -> Harness {
    let query_accounting = Arc::new(QueryAccountingMetrics::new(configured));
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = Arc::new(SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    ));
    let tokens: HashMap<String, TenantId> =
        HashMap::from([("acme-token".to_string(), TenantId::new("acme".to_string()))]);

    let sql = sql_router(SqlState {
        executor: Arc::clone(&executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store: Arc::clone(&store),
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(30),
        query_accounting: Arc::clone(&query_accounting),
        query_admission: ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
    });

    let catalog_cache_metrics = catalog.byte_cache_metrics();
    let metrics = crate::metrics::router(MetricsState {
        mode: Mode::All,
        store_metrics: Arc::new(StoreMetrics::default()),
        ingest_router: None,
        log_ingest_router: None,
        span_ingest_router: None,
        catalog,
        tenant_discovery: None,
        maintenance_safety: None,
        maintenance_ownership: None,
        merge_memory: None,
        scrub: None,
        cache_metrics: None,
        catalog_cache_metrics,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        metrics_tenant_labels: false,
        query_accounting,
        ingest_concurrency: crate::ingest_concurrency::IngestConcurrencyController::shared(
            crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        distrib: None,
        durable_auth: None,
    });

    Harness {
        sql,
        metrics,
        executor,
    }
}

fn sql_body(query: &str) -> String {
    serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
    })
    .to_string()
}

async fn post_sql(app: &Router, query: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer acme-token")
        .body(Body::from(sql_body(query)))
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("body not JSON ({e}): {}", String::from_utf8_lossy(&bytes)));
    (status, value)
}

async fn scrape_metrics(app: &Router) -> String {
    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("metrics body is utf8")
}

/// THE ACCEPTANCE TEST: a query's cost accounting reaches both the
/// response `stats` object and the `/metrics` exposition. One real SQL query
/// over one real segment, then a real `/metrics` scrape, both through the same
/// aggregator the handler recorded into.
#[tokio::test]
async fn query_accounting_reaches_response_stats_and_metrics_endpoint() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, &[(100, 1.0), (200, 2.5)]).await;
    // No tenant configured, so `acme` folds into `tenant_hash="other"`.
    let h = harness(Arc::clone(&store), HashSet::new());

    // Half one: the response carries a `stats` object with this query's
    // accounting and estimate beside its data.
    let (status, body) = post_sql(&h.sql, "SELECT ts, value FROM samples ORDER BY ts").await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    let stats = &body["stats"];
    assert!(
        stats["accounting"].is_object(),
        "response must carry stats.accounting: {body}"
    );
    assert!(
        stats["estimate"].is_object(),
        "response must carry stats.estimate: {body}"
    );
    // The query opened the segment, so the actual store requests are non-zero:
    // accounting genuinely observed the fetch, it is not a zero placeholder.
    assert!(
        stats["accounting"]["s3GetRequests"].as_u64().unwrap() > 0,
        "the query fetched a segment, so s3GetRequests must be > 0: {body}"
    );

    // Half two: the same query's cost reached `/metrics`, folded into
    // `tenant_hash="other"`, one interactive query.
    let scrape = scrape_metrics(&h.metrics).await;
    assert!(
        scrape.contains(
            "ravel_query_queries_total{mode=\"all\",tenant_hash=\"other\",\
             workload_class=\"interactive\"} 1"
        ),
        "one interactive query must be counted at /metrics:\n{scrape}"
    );
    assert!(
        scrape.contains(
            "ravel_query_s3_requests_total{mode=\"all\",tenant_hash=\"other\",\
             workload_class=\"interactive\"}"
        ),
        "the actual request family must render for the query:\n{scrape}"
    );
    // The estimate family is a distinct, separately-named series beside the
    // actual (ADR-0044 section 3), so their divergence is measurable.
    assert!(
        scrape.contains(
            "ravel_query_estimated_requests_total{mode=\"all\",tenant_hash=\"other\",\
             workload_class=\"interactive\"}"
        ),
        "the estimate family must render as its own series:\n{scrape}"
    );
}

/// Instrumentation must not change the query result: the `data` a client
/// receives from the accounted handler is byte-identical to the raw executor
/// output with no `stats` attached (ADR-0044: "No query result may change").
#[tokio::test]
async fn query_result_is_byte_identical_with_and_without_accounting() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(
        store.as_ref(),
        &tenant,
        &[(100, 1.0), (200, 2.5), (300, 4.0)],
    )
    .await;
    let h = harness(Arc::clone(&store), HashSet::new());

    let query = "SELECT ts, value FROM samples ORDER BY ts";

    // Through the accounted handler: the result lives under `data`, beside the
    // `stats` object the feature adds.
    let (status, body) = post_sql(&h.sql, query).await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    let handler_data = body["data"].clone();

    // Directly through the executor, no stats attached at all: the raw result.
    let request = SqlRequest {
        sql: query.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
    };
    let outcome = h
        .executor
        .execute(tenant.hash(), &request)
        .await
        .expect("direct execute");
    let raw_data = outcome.output.to_json().expect("output to json");

    assert_eq!(
        serde_json::to_string(&handler_data).unwrap(),
        serde_json::to_string(&raw_data).unwrap(),
        "attaching accounting must not change the result payload"
    );
}
