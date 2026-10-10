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
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::{AdmissionController, AdmissionLimits, Clock, SystemClock};
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_query::http::service::{
    DEFAULT_MEMORY_ADMISSION_FRACTION, MEMORY_ADMISSION_MAX_WAIT, MemoryAdmissionGate,
};
use ravel_query::http::{StaticBearerTokenResolver, TenantResolver};
use ravel_query::{
    EngineConfig, FetchError, GetLimiter, LogFetchError, LogSegmentFetcher,
    QueryAdmissionController, QueryConcurrencyLimit, QueryError, ReadCache, SegmentFetcher,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SpanFetchError, SqlConfig, SqlError, SqlExecutor, SqlRequest};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use tower::ServiceExt;
use uuid::Uuid;

use crate::config::Mode;
use crate::metrics::{MetricsState, QueryAccountingMetrics};
use crate::sql::{SqlState, router as sql_router};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_SEC: i64 = 1_000_000_000;
/// Small on purpose, mirroring `tests/sql_endpoint.rs`: `Catalog::resolve`
/// issues one LIST per (shard, ingest-hour) pair across the window, so a
/// wall-clock value would fan out to hundreds of thousands of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// The SQL client message for a fetch refused by the process memory budget
/// (`ravel_sql`'s `MSG_FETCH_MEMORY_EXHAUSTED`, which the crate does not
/// re-export), spelled out as the wire contract a caller sees.
const FETCH_MEMORY_EXHAUSTED_MESSAGE: &str = "query memory budget exhausted: the process could not reserve memory to fetch segment data; retry";

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
    /// The memory admission wait installed on the SQL surface's admission
    /// controller at the default fraction, as `crate::start` installs it.
    memory_admission: Arc<MemoryAdmissionGate>,
}

fn harness(
    store: Arc<dyn ObjectStoreBackend>,
    configured: HashSet<TenantHash>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
) -> Harness {
    harness_with_max_wait(
        store,
        configured,
        process_memory_budget,
        MEMORY_ADMISSION_MAX_WAIT,
    )
}

/// [`harness`] with `max_wait` as the memory admission wait's cap.
fn harness_with_max_wait(
    store: Arc<dyn ObjectStoreBackend>,
    configured: HashSet<TenantHash>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    max_wait: Duration,
) -> Harness {
    let query_accounting = Arc::new(QueryAccountingMetrics::new(configured));
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = Arc::new(
        SqlExecutor::new(
            Arc::clone(&catalog),
            SegmentFetcher::new(store.clone()),
            LogSegmentFetcher::new(store.clone()),
            ravel_sql::SpanSegmentFetcher::new(store.clone()),
            SqlConfig::default(),
            1 << 30,
        )
        .with_process_memory_budget(Arc::clone(&process_memory_budget)),
    );
    let tokens: HashMap<String, TenantId> =
        HashMap::from([("acme-token".to_string(), TenantId::new("acme".to_string()))]);

    let memory_admission = Arc::new(
        MemoryAdmissionGate::new(
            Arc::clone(&process_memory_budget),
            DEFAULT_MEMORY_ADMISSION_FRACTION,
        )
        .with_max_wait(max_wait),
    );
    let sql = sql_router(SqlState {
        executor: Arc::clone(&executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store: Arc::clone(&store),
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(30),
        query_accounting: Arc::clone(&query_accounting),
        query_admission: Arc::new(
            ravel_query::QueryAdmissionController::new(
                ravel_query::QueryConcurrencyLimit::Unlimited,
            )
            .with_memory_gate(Arc::clone(&memory_admission)),
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
        cache_disk_metrics: None,
        catalog_cache_metrics,
        catalog_cache_disk_metrics: None,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        reconcile_cycle: Arc::new(crate::admission_reconcile::ReconcileCycleMetrics::default()),
        metrics_tenant_labels: false,
        metrics_tenant_allowlist: Arc::new(HashSet::new()),
        query_accounting,
        ingest_concurrency: crate::ingest_concurrency::IngestConcurrencyController::shared(
            crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        distrib: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_rejects: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_tls_dials: None,
        durable_auth: None,
        ingest_byte_metrics: std::sync::Arc::new(
            crate::ingest_byte_metrics::IngestByteMetrics::new(),
        ),
        normalize_reject_metrics: std::sync::Arc::new(
            crate::normalize_reject_metrics::NormalizeRejectMetrics::new(),
        ),
        metadata_cache: None,
        cache: None,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        audit_pipeline: None,
        process_memory_budget,
        process_memory_budget_is_fallback: false,
        memory_admission: Arc::clone(&memory_admission),
        cpu_gates: crate::cpu_gates::CpuGates::new(Default::default()),
        can_fold: true,
        fold_loop: Default::default(),
        refold: Default::default(),
        heartbeat: crate::health_listener::Heartbeat::new(Arc::new(SystemClock)),
    });

    Harness {
        sql,
        metrics,
        executor,
        memory_admission,
    }
}

/// Real wall-clock nanoseconds. Unlike `NOW_NS` above, the raw PromQL HTTP
/// handler's own `now_ns()` (`crates/ravel-query/src/http/handlers.rs`) is
/// not injectable and always reads `SystemTime::now()`, so a PromQL fixture
/// resolved through that handler must be anchored to real time rather than
/// the small fixed `NOW_NS` the SQL tests above use.
fn real_now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as i64
}

/// A splitmix64-derived `f64` in `[1.0, 2.0)`: fixed sign/exponent bits, a
/// fully mixed 52-bit mantissa. Consecutive values share no exploitable
/// structure, so neither Gorilla-style delta encoding nor RSEG's real
/// zstd/lz4 compression can shrink the column much below its raw width. This
/// is what lets a fixed sample count reliably force a segment's data object
/// past `ravel_query`'s 512 KiB whole-object-read threshold, which a
/// compressible or monotonic value sequence would not.
fn high_entropy_value(i: u64) -> f64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    f64::from_bits((z & 0x000F_FFFF_FFFF_FFFF) | 0x3FF0_0000_0000_0000)
}

/// Samples span 150 real seconds ending 250 seconds before `now`, so a query
/// `time` of `now - 300s` lands inside the covered range with margin on both
/// sides regardless of scheduling jitter between publish and query.
const LARGE_SEGMENT_SAMPLES: u64 = 150_000;
const LARGE_SEGMENT_START_OFFSET_S: i64 = 400;
const LARGE_SEGMENT_QUERY_OFFSET_S: i64 = 300;

/// Publishes one real RSEG segment for `tenant`/`metric`, anchored relative
/// to `now` (real wall-clock nanoseconds), with high-entropy sample values so
/// its data object exceeds the 512 KiB whole-object-read threshold: only past
/// that threshold does `ensure_ranges` reserve against the process memory
/// budget at all, rather than reading the whole object in one unbudgeted GET.
/// Asserts the threshold was really crossed rather than assuming it from the
/// sample count.
async fn publish_large_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
    now: i64,
) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, metric, &label_set).expect("series id");

    let base_ts_ns = now - LARGE_SEGMENT_START_OFFSET_S * NS_PER_SEC;
    let samples: Vec<Sample> = (0..LARGE_SEGMENT_SAMPLES)
        .map(|i| Sample {
            ts_ns: base_ts_ns + i as i64 * 1_000_000,
            value: high_entropy_value(i),
        })
        .collect();

    let series = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples,
    }];

    let writer_id = Uuid::from_u128(3_000);
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

    let object_size = written.bytes.len() as u64;
    assert!(
        object_size > 512 * 1024,
        "fixture must exceed the 512 KiB whole-object threshold to force a \
         budgeted range read, got {object_size} bytes"
    );

    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: now,
        ingest_hour_bucket: hour_bucket,
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

/// Builds the real PromQL `AppState` via `crate::query::build_app_state`
/// (the same constructor `crate::start` calls), plus a `/metrics` router
/// sharing one `Catalog` and the SAME `process_memory_budget` instance, so a
/// PromQL memory-budget acceptance test exercises the actual server wiring
/// rather than a hand-assembled `QueryEngine`.
fn promql_harness(
    store: Arc<dyn ObjectStoreBackend>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
    cache: Option<ReadCache>,
) -> (ravel_query::http::AppState, Router, Router) {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let query_accounting = Arc::new(QueryAccountingMetrics::new(HashSet::new()));
    let tokens: HashMap<String, TenantId> =
        HashMap::from([("acme-token".to_string(), TenantId::new("acme".to_string()))]);
    let tenant_resolver: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));

    let state = crate::query::build_app_state(
        Arc::clone(&catalog),
        Arc::clone(&store),
        tenant_resolver,
        cache,
        ravel_query::EngineConfig::default(),
        Arc::new(GetLimiter::new(8).expect("nonzero permits")),
        Arc::clone(&query_accounting),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
        None,
        None,
        None,
        Arc::clone(&process_memory_budget),
    );
    let promql = ravel_query::http::router(state.clone());

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
        cache_disk_metrics: None,
        catalog_cache_metrics,
        catalog_cache_disk_metrics: None,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        reconcile_cycle: Arc::new(crate::admission_reconcile::ReconcileCycleMetrics::default()),
        metrics_tenant_labels: false,
        metrics_tenant_allowlist: Arc::new(HashSet::new()),
        query_accounting,
        ingest_concurrency: crate::ingest_concurrency::IngestConcurrencyController::shared(
            crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        distrib: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_rejects: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_tls_dials: None,
        durable_auth: None,
        ingest_byte_metrics: std::sync::Arc::new(
            crate::ingest_byte_metrics::IngestByteMetrics::new(),
        ),
        normalize_reject_metrics: std::sync::Arc::new(
            crate::normalize_reject_metrics::NormalizeRejectMetrics::new(),
        ),
        metadata_cache: None,
        cache: None,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        audit_pipeline: None,
        process_memory_budget,
        process_memory_budget_is_fallback: false,
        memory_admission: Arc::new(MemoryAdmissionGate::disabled()),
        cpu_gates: crate::cpu_gates::CpuGates::new(Default::default()),
        can_fold: true,
        fold_loop: Default::default(),
        refold: Default::default(),
        heartbeat: crate::health_listener::Heartbeat::new(Arc::new(SystemClock)),
    });

    (state, promql, metrics)
}

/// The running fetch-reservation totals an instant query of `metric` at
/// `query_time_s` passes through, in order: entry `i` is the bytes held right
/// after the fetcher's `i`-th reservation. Read from typed refusals rather
/// than from the `fetch_reserved()` counter the gauges render: starting from a
/// 1-byte budget, each `FetchMemoryExhausted` names what was already held and
/// what the next reservation asked for, and the next attempt runs a budget of
/// exactly their sum, until the query answers. The last entry is therefore the
/// smallest budget that admits the query.
async fn fetch_reservation_steps(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    metric: &str,
    query_time_s: i64,
    now: i64,
) -> Vec<ReservationStep> {
    let mut steps: Vec<ReservationStep> = Vec::new();
    let mut limit = 1;
    for _ in 0..16 {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let (state, _promql, _metrics) =
            promql_harness(Arc::clone(store), Arc::clone(&budget), None);
        let result = state
            .engine
            .instant(
                tenant.hash(),
                metric,
                query_time_s * 1000,
                &[],
                now,
                Duration::from_secs(30),
            )
            .await;
        match result {
            Ok(_) => {
                assert!(
                    !steps.is_empty(),
                    "the oracle's 1-byte budget never refused"
                );
                return steps;
            }
            Err(QueryError::Fetch(FetchError::FetchMemoryExhausted {
                requested,
                reserved,
                limit: refused_at,
            })) => {
                assert_eq!(refused_at, limit);
                limit = reserved + requested;
                steps.push(ReservationStep {
                    held: reserved,
                    total: limit,
                });
            }
            Err(other) => panic!("expected FetchMemoryExhausted from the oracle, got {other:?}"),
        }
    }
    panic!("the oracle did not converge in 16 budget steps: {steps:?}");
}

/// One refusal from the [`fetch_reservation_steps`] oracle: what the query
/// already held when the fetcher made that reservation, and the running total
/// once the reservation is admitted (`held` plus what it asked for).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReservationStep {
    held: u64,
    total: u64,
}

/// Splits [`fetch_reservation_steps`] for the one-segment instant query these
/// tests run into the totals a held GET can observe, and the size of the
/// catalog decode's reservation (ADR-1702 decision 6). The query reserves three
/// times: the catalog-section range read, the catalog decode, then the page
/// read. The decode reserves between GETs, so no held GET sees its own running
/// total, and it charges decoded output rather than cache-bound bytes, so it is
/// never marked handed off.
///
/// The decode figure is the decode step's own total less the range read, which
/// is the pre-shrink charge. That is only the figure a held GET observes while
/// `shrink_to_retained` leaves the reservation alone, and nothing here can
/// check it: under the full budget the oracle run builds, the shrink's reserve
/// is refused whatever its order. The callers' `seen == observable` assertion
/// under an unlimited budget is what fails if the fixture ever shrinks.
fn split_decode_step(steps: &[ReservationStep]) -> (Vec<u64>, u64) {
    assert_eq!(
        steps.len(),
        3,
        "range read, catalog decode, page read: {steps:?}"
    );
    let range_read = steps[0].total;
    assert_eq!(
        steps[1].held, range_read,
        "the catalog decode reserves on top of the range read alone: {steps:?}"
    );
    assert!(
        steps[2].held >= range_read,
        "the range read is still held when the page read reserves: {steps:?}"
    );
    (
        vec![range_read, steps[2].total],
        steps[1].total - range_read,
    )
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
    post_sql_body(app, sql_body(query)).await
}

async fn post_sql_body(app: &Router, body: String) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer acme-token")
        .body(Body::from(body))
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
    let h = harness(
        Arc::clone(&store),
        HashSet::new(),
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    );

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
    let h = harness(
        Arc::clone(&store),
        HashSet::new(),
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    );

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
        row_window: false,
        max_rows: None,
        budgets: None,
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

/// ACCEPTANCE TEST (e): a query whose real execution outgrows the ADR-1170
/// process-wide memory budget is refused typed (`ResourcesExhausted`, HTTP
/// 422), never a panic, and the refusal rolls its charge back off the shared
/// process counter so the very next query, through the SAME executor and the
/// same tenant mutex, is still answered. This exercises the budget through
/// the real HTTP handler and the real `SqlExecutor::new`/
/// `with_process_memory_budget` wiring `crate::start` uses, not just the
/// ravel-sql unit tests that drive `TenantDelegatingPool` directly.
///
/// Prove-the-test for the rollback half: add an early `return;` to
/// `TenantMemoryAccountant::release_process_at_most`
/// (crates/ravel-sql/src/memory.rs), so a refused query's charge stays on the
/// shared counter. `budget.reserved()` then reads 64,000 against the expected
/// 0. The `SELECT 1` assertion below does NOT catch that: a query that
/// reserves nothing issues no `try_grow`, so it answers 200 against a counter
/// left fully saturated just as it does against one rolled back to 0. It is
/// here for the second half of the claim (the executor and the tenant mutex
/// are still usable), and the counter assertion is here for the first.
///
/// The budget is 256 KiB rather than a token 1 KiB for the same reason. This
/// statement needs 460,200 bytes to complete and takes them in increments no
/// larger than the 64,000-byte table itself, so a 256 KiB ceiling is crossed
/// only after several reservations have really been charged. Against a 1 KiB
/// or 16 KiB ceiling the FIRST `try_grow` is refused, which reserves nothing
/// by `MemoryBudget::try_reserve`'s all-or-nothing rule, and the counter never
/// leaves 0 for a rollback to return it to: the assertion then holds whether
/// or not the rollback exists (measured: `reserved()` reads 0 under both
/// ceilings with the rollback disabled).
#[tokio::test]
async fn a_query_over_the_process_budget_is_refused_and_the_process_keeps_serving() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let samples: Vec<(i64, f64)> = (0..4_000)
        .map(|i| (i as i64 * 1_000_000, i as f64))
        .collect();
    publish_segment(store.as_ref(), &tenant, &samples).await;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(256 * 1024));
    let h = harness(Arc::clone(&store), HashSet::new(), Arc::clone(&budget));

    // A real sort over 4,000 rows outgrows a 256 KiB process budget: refused
    // typed, not a panic and not a hang.
    let (status, body) = post_sql(&h.sql, "SELECT ts, value FROM samples ORDER BY ts").await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a sort over 4,000 rows must outgrow a 256 KiB process budget: {body}"
    );
    // Growth past the budget is refused at the reservation, never parked:
    // the admission wait only runs before a statement starts.
    assert_eq!(
        h.memory_admission.waits_total(),
        0,
        "a statement admitted under the threshold must not wait at admission"
    );

    // The refused query's charge rolled back off the SHARED process counter.
    // This is what makes the cross-tenant cascade survivable: the counter is
    // process-wide, so a charge left behind by one tenant's abort refuses
    // every other tenant's next reservation for as long as it sits there.
    assert_eq!(
        budget.reserved(),
        0,
        "the refused query must leave no charge on the shared process counter"
    );

    // And the executor itself is still usable through the same tenant mutex.
    let (status, body) = post_sql(&h.sql, "SELECT 1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the process must keep answering after a refusal: {body}"
    );
}

/// Polls `cond` on a 5 ms tick, at most 2000 times; panics with `what` if it
/// never holds.
async fn wait_until(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

/// #2044 acceptance: statements that arrive while the shared process budget
/// sits above the admission threshold wait at `POST /api/v1/sql` admission
/// instead of starting into a refusal, and every one of them answers 200
/// once the budget frees. This is the test proving the SQL HTTP path reaches
/// the wait (`ServerService::sql_execute_timed` through `QueryControls::admit`).
///
/// Prove-the-test: skip the `wait_for_headroom` call in
/// `QueryAdmissionController::admit_within`. The four statements then start
/// into the held budget and are refused at their sort reservation, and
/// `waits_total` never reaches 4 ("timed out waiting for every statement to
/// park at admission").
#[tokio::test]
async fn concurrent_sql_statements_wait_for_memory_headroom_and_all_succeed() {
    const STATEMENTS: u64 = 4;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let samples: Vec<(i64, f64)> = (0..4_000)
        .map(|i| (i as i64 * 1_000_000, i as f64))
        .collect();
    publish_segment(store.as_ref(), &tenant, &samples).await;
    let limit = 8 * 1024 * 1024;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
    // A cap well past how long parking takes, so no wait expires before the
    // budget frees.
    let h = harness_with_max_wait(
        Arc::clone(&store),
        HashSet::new(),
        Arc::clone(&budget),
        Duration::from_secs(25),
    );
    let threshold = h
        .memory_admission
        .threshold_bytes()
        .expect("a bounded budget at the default fraction enables the wait");
    assert_eq!(
        threshold,
        (limit as f64 * DEFAULT_MEMORY_ADMISSION_FRACTION) as u64
    );

    // Another holder sits above the threshold, leaving less headroom than
    // one sort needs.
    let held = budget
        .reserve(limit - 16 * 1024)
        .expect("the empty budget admits the holder");
    assert!(budget.reserved() >= threshold);

    let mut tasks = Vec::new();
    for _ in 0..STATEMENTS {
        let sql = h.sql.clone();
        tasks.push(tokio::spawn(async move {
            post_sql(&sql, "SELECT ts, value FROM samples ORDER BY ts").await
        }));
    }
    let gate = Arc::clone(&h.memory_admission);
    wait_until("every statement to park at admission", || {
        gate.waits_total() == STATEMENTS
    })
    .await;
    assert_eq!(gate.waits_expired_total(), 0);
    drop(held);

    for task in tasks {
        let (status, body) = task.await.expect("statement task");
        assert_eq!(
            status,
            StatusCode::OK,
            "a statement that waited for headroom must succeed: {body}"
        );
    }
    assert_eq!(h.memory_admission.waits_total(), STATEMENTS);
    assert_eq!(h.memory_admission.waits_expired_total(), 0);
    assert_eq!(
        budget.reserved(),
        0,
        "every statement released its reservations"
    );
}

/// #2044: a statement whose wait reaches the cap with the budget still above
/// the threshold is admitted anyway, and its own reservation decides, as it
/// would with no wait: here the sort does not fit the 16 KiB left and answers
/// 422. The wait never answers with a refusal of its own, and the expiry
/// counter moves on `/metrics`.
///
/// Prove-the-test: drop `max_wait` from the bound in
/// `MemoryAdmissionGate::wait_for_headroom`, so only half the deadline ends
/// the wait. The statement then waits 15 s of its 30 s deadline and the 5 s
/// timeout around the request fires.
#[tokio::test]
async fn a_sql_statement_whose_wait_expires_is_admitted_and_its_reservation_decides() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let samples: Vec<(i64, f64)> = (0..4_000)
        .map(|i| (i as i64 * 1_000_000, i as f64))
        .collect();
    publish_segment(store.as_ref(), &tenant, &samples).await;
    let limit = 8 * 1024 * 1024;
    let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
    let h = harness_with_max_wait(
        Arc::clone(&store),
        HashSet::new(),
        Arc::clone(&budget),
        Duration::from_millis(50),
    );
    let _held = budget
        .reserve(limit - 16 * 1024)
        .expect("the empty budget admits the holder");

    // hygiene-allow: wall-clock -- a bound proving the 50 ms cap ends the wait, far below half the 30 s deadline; the assertions are on the response
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(5),
        post_sql(&h.sql, "SELECT ts, value FROM samples ORDER BY ts"),
    )
    .await
    .expect("the 50 ms cap, not the deadline, ends the wait");
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "the admitted statement's sort is refused by its own reservation: {body}"
    );
    assert_eq!(h.memory_admission.waits_total(), 1);
    assert_eq!(h.memory_admission.waits_expired_total(), 1);

    let scrape = scrape_metrics(&h.metrics).await;
    assert!(
        scrape.contains("ravel_memory_admission_waits_total{mode=\"all\"} 1\n"),
        "{scrape}"
    );
    assert!(
        scrape.contains("ravel_memory_admission_waits_expired_total{mode=\"all\"} 1\n"),
        "{scrape}"
    );
}

/// ACCEPTANCE TEST: a PromQL fetch whose real execution outgrows the
/// ADR-1170 process-wide memory budget is refused typed
/// (`FetchError::FetchMemoryExhausted`), and, through the real HTTP router,
/// as `StatusCode::SERVICE_UNAVAILABLE`. This is the PromQL-path counterpart
/// of `a_query_over_the_process_budget_is_refused_and_the_process_keeps_serving`
/// above: it exercises `crate::query::build_app_state`'s
/// `.with_memory_budget(process_memory_budget)` wiring, not a hand-built
/// `QueryEngine`.
///
/// Divergence from the SQL path: PromQL answers a `FetchMemoryExhausted`
/// with HTTP 503 and `MSG_FETCH_MEMORY_EXHAUSTED`
/// (`crates/ravel-query/src/http/error.rs`), while the SQL test above is
/// refused 422 at the SQL memory pool. The message still tells a PromQL
/// caller that memory, not the object store, refused the query.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget)` from
/// `build_app_state` (services/ravel-server/src/query.rs). The typed check
/// then fails: a query that opens no reservation at all never returns
/// `FetchMemoryExhausted`, so the `match` falls to its `other => panic!`
/// branch, e.g. `panic: expected FetchMemoryExhausted, got Ok(...)`.
#[tokio::test]
async fn a_promql_fetch_over_the_process_budget_is_refused_and_the_process_keeps_serving() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let now = real_now_ns();
    publish_large_segment(store.as_ref(), &tenant, "big_gauge_budget", now).await;

    // 4 KiB is far smaller than the >512 KiB column range this segment forces
    // `ensure_ranges` to reserve in one all-or-nothing charge.
    let budget = Arc::new(ravel_memory::MemoryBudget::new(4 * 1024));
    let (state, promql, _metrics) = promql_harness(Arc::clone(&store), Arc::clone(&budget), None);
    let query_time_s = (now - LARGE_SEGMENT_QUERY_OFFSET_S * NS_PER_SEC) / NS_PER_SEC;

    // Typed check, direct against the real `build_app_state`-constructed
    // engine.
    let result = state
        .engine
        .instant(
            tenant.hash(),
            "big_gauge_budget",
            query_time_s * 1000,
            &[],
            now,
            Duration::from_secs(30),
        )
        .await;
    let (requested, reserved) = match result {
        Err(QueryError::Fetch(FetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        })) => {
            assert_eq!(
                limit,
                4 * 1024,
                "the refusal must name the configured limit"
            );
            (requested, reserved)
        }
        other => panic!("expected FetchMemoryExhausted, got {other:?}"),
    };
    assert!(
        requested > 4 * 1024 - reserved,
        "the refused reservation must need more than the budget remainder: \
         requested {requested}, reserved {reserved}"
    );
    assert_eq!(
        budget.reserved(),
        0,
        "the refused query releases every reservation it held"
    );
    assert_eq!(budget.fetch_reserved(), 0);

    // The refusal sits exactly at the remainder. The oracle's step totals
    // include the one this refusal names, and the smallest admitting budget
    // (its last step) admits the same query while one byte less refuses it.
    let steps =
        fetch_reservation_steps(&store, &tenant, "big_gauge_budget", query_time_s, now).await;
    assert!(
        steps.iter().any(|step| step.total == reserved + requested),
        "the refusal ({reserved} held + {requested} requested) must be one of the \
         fetcher's reservation steps {steps:?}"
    );
    let peak = steps
        .last()
        .expect("oracle returns at least one step")
        .total;
    for (limit, admitted) in [(peak, true), (peak - 1, false)] {
        let exact = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let (exact_state, _exact_promql, _exact_metrics) =
            promql_harness(Arc::clone(&store), Arc::clone(&exact), None);
        let result = exact_state
            .engine
            .instant(
                tenant.hash(),
                "big_gauge_budget",
                query_time_s * 1000,
                &[],
                now,
                Duration::from_secs(30),
            )
            .await;
        match (admitted, result) {
            (true, Ok(_)) => {}
            (false, Err(QueryError::Fetch(FetchError::FetchMemoryExhausted { .. }))) => {}
            (_, other) => panic!("budget {limit} (peak {peak}): unexpected {other:?}"),
        }
        assert_eq!(exact.reserved(), 0);
    }

    // The same refusal, through the real HTTP router: distinct status from
    // the SQL path's 422 for the same underlying condition (see doc comment
    // above).
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/query?query=big_gauge_budget&time={query_time_s}"
        ))
        .header(header::AUTHORIZATION, "Bearer acme-token")
        .body(Body::empty())
        .expect("build request");
    let response = promql.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // The refusal left nothing behind: the shared budget reads 0 again, so the
    // next query's reservations start from the full limit. That is the proof
    // the process keeps serving. The query below resolves no segment (an unpublished metric) and
    // needs no reservation, so it shows only that the same engine still routes
    // and answers after a refusal, not that the budget admits a new charge.
    assert_eq!(budget.reserved(), 0);
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/query?query=still_alive&time={query_time_s}"
        ))
        .header(header::AUTHORIZATION, "Bearer acme-token")
        .body(Body::empty())
        .expect("build request");
    let response = promql.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the engine must still answer a query that needs no reservation after a refusal"
    );
}

/// ACCEPTANCE TEST: while a PromQL fetch's budgeted range GET is in flight,
/// `ravel_memory_reserved_bytes{component="fetch"}` reads exactly the live
/// reservations' total, both through the direct counter and through a real
/// `/metrics` scrape, `component="sql"` reads 0, and both return to exactly 0
/// once the query completes. The query reserves in more than one step; every
/// step total from [`fetch_reservation_steps`] must be observed held, in
/// order, and nothing else. Those totals come from the fetcher's own typed
/// refusals, not from the counter under test.
///
/// A `FaultStore` holds every `Get` against the published segment's data key
/// so the reservation is observable while `budget.fetch_reserved()` is
/// nonzero; the segment's own unbudgeted footer read (which also matches the
/// hold filter) is released without asserting on it, distinguished from the
/// budgeted range read by `fetch_reserved() == 0` at that moment (the
/// footer/suffix GET in `open_segment` issues no reservation at all).
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget)` from
/// `build_app_state`, and the oracle panics with "the oracle's 1-byte budget
/// never refused". Make `MemoryBudget::sql_reserved` return `reserved()`
/// instead, and the held scrape fails "a fetch reservation must not be
/// counted under component=\"sql\"".
#[tokio::test]
async fn memory_gauges_report_a_nonzero_fetch_reservation_during_a_query() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let tenant = TenantId::new("acme".to_string());
    let now = real_now_ns();
    publish_large_segment(store.as_ref(), &tenant, "big_gauge_gauge", now).await;

    let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
    let (state, _promql, metrics) = promql_harness(Arc::clone(&store), Arc::clone(&budget), None);
    let query_time_s = (now - LARGE_SEGMENT_QUERY_OFFSET_S * NS_PER_SEC) / NS_PER_SEC;
    let steps =
        fetch_reservation_steps(&store, &tenant, "big_gauge_gauge", query_time_s, now).await;
    let (observable, _decode) = split_decode_step(&steps);

    let gate = fault_store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Always);
    let mut query = Box::pin(state.engine.instant(
        tenant.hash(),
        "big_gauge_gauge",
        query_time_s * 1000,
        &[],
        now,
        Duration::from_secs(30),
    ));

    let mut seen: Vec<u64> = Vec::new();
    loop {
        tokio::select! {
            result = &mut query => {
                assert_eq!(
                    seen, observable,
                    "every reservation step a GET follows must be observed held, in \
                     order, at its exact total; a mismatch at the page read means \
                     the catalog decode's reservation shrank"
                );
                let (_value, _coverage) = result.expect("query must succeed");
                break;
            }
            () = gate.wait_until_held(1) => {
                let held = gate.held_details();
                let (id, _, _) = held[0];
                let reserved = budget.fetch_reserved();
                if reserved > 0 {
                    assert!(
                        steps.iter().any(|step| step.total == reserved),
                        "the fetch counter ({reserved}) must hold exactly one of the \
                         fetcher's reservation totals {steps:?}"
                    );
                    if seen.last() != Some(&reserved) {
                        seen.push(reserved);
                    }
                    let expected = reserved;
                    assert_eq!(budget.reserved(), expected);
                    let scrape = scrape_metrics(&metrics).await;
                    assert!(
                        scrape.contains(&format!(
                            "ravel_memory_reserved_bytes{{mode=\"all\",component=\"fetch\"}} {expected}\n"
                        )),
                        "the fetch gauge must equal the fetcher's reservation ({expected}):\n{scrape}"
                    );
                    assert!(
                        scrape.contains(
                            "ravel_memory_reserved_bytes{mode=\"all\",component=\"sql\"} 0\n"
                        ),
                        "a fetch reservation must not be counted under component=\"sql\":\n{scrape}"
                    );
                }
                gate.release(id);
            }
        }
    }

    assert_eq!(
        budget.fetch_reserved(),
        0,
        "the reservation must release once the query completes"
    );
    assert_eq!(budget.reserved(), 0);
    let scrape = scrape_metrics(&metrics).await;
    assert!(
        scrape.contains("ravel_memory_reserved_bytes{mode=\"all\",component=\"fetch\"} 0\n"),
        "the fetch gauge must read back to 0 after completion:\n{scrape}"
    );
    assert!(
        scrape.contains("ravel_memory_reserved_bytes{mode=\"all\",component=\"sql\"} 0\n"),
        "the sql gauge must read 0 when only the fetch path ran:\n{scrape}"
    );
}

/// ACCEPTANCE TEST: with an ADR-0046 read cache configured, a PromQL fetch's
/// `Reservation` is marked handed off as soon as the cache takes its own copy
/// of the fetched bytes (`Reservation::mark_handed_off`), so while the fetch
/// is in flight `ravel_memory_handoff_overlap_bytes` reads EXACTLY the same
/// value as the live fetch reservations (the whole reservation is handed off,
/// never a partial amount; the catalog decode's reservation for decoded output
/// is the one live guard that is not), both through the direct counters and
/// through a real `/metrics` scrape; both return to exactly 0 once the query
/// completes.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget)` from
/// `build_app_state`, and the oracle panics as in the sibling gauge test
/// above. Drop the `reservation.mark_handed_off()` call in
/// `SegmentFetcher::ensure_ranges`, and the held check fails with the overlap
/// at 0 against the first step's exact total.
#[tokio::test]
async fn memory_handoff_overlap_equals_the_fetch_reservation_while_a_cached_fetch_is_held() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let tenant = TenantId::new("acme".to_string());
    let now = real_now_ns();
    publish_large_segment(store.as_ref(), &tenant, "big_gauge_overlap", now).await;

    let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
    let cache = ReadCache::Ram(Arc::new(Cache::new(CacheLimits::new(
        4 * 1024 * 1024,
        100,
        4 * 1024 * 1024,
    ))));
    let (state, _promql, metrics) =
        promql_harness(Arc::clone(&store), Arc::clone(&budget), Some(cache));
    let query_time_s = (now - LARGE_SEGMENT_QUERY_OFFSET_S * NS_PER_SEC) / NS_PER_SEC;
    let steps =
        fetch_reservation_steps(&store, &tenant, "big_gauge_overlap", query_time_s, now).await;
    let (observable, decode) = split_decode_step(&steps);

    let gate = fault_store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Always);
    let mut query = Box::pin(state.engine.instant(
        tenant.hash(),
        "big_gauge_overlap",
        query_time_s * 1000,
        &[],
        now,
        Duration::from_secs(30),
    ));

    let mut seen: Vec<u64> = Vec::new();
    loop {
        tokio::select! {
            result = &mut query => {
                assert_eq!(
                    seen, observable,
                    "every reservation step a GET follows must be observed held, in \
                     order, at its exact total; a mismatch at the page read means \
                     the catalog decode's reservation shrank"
                );
                let (_value, _coverage) = result.expect("query must succeed");
                break;
            }
            () = gate.wait_until_held(1) => {
                let held = gate.held_details();
                let (id, _, _) = held[0];
                let reserved = budget.fetch_reserved();
                if reserved > 0 {
                    assert!(
                        steps.iter().any(|step| step.total == reserved),
                        "the fetch counter ({reserved}) must hold exactly one of the \
                         fetcher's reservation totals {steps:?}"
                    );
                    if seen.last() != Some(&reserved) {
                        seen.push(reserved);
                    }
                    let expected = reserved;
                    // Past the first step the decoded catalog is held too, and
                    // its reservation is not a fetch the cache also holds.
                    let decoded_held = if reserved > observable[0] { decode } else { 0 };
                    let overlap = reserved - decoded_held;
                    assert_eq!(
                        budget.handoff_overlap(),
                        overlap,
                        "a cache-configured fetch hands off its WHOLE reservation, so the \
                         overlap must equal the fetch reservations exactly"
                    );
                    assert_eq!(
                        budget.reserved(),
                        expected,
                        "handed-off bytes stay counted once in the budget total"
                    );
                    let scrape = scrape_metrics(&metrics).await;
                    assert!(
                        scrape.contains(&format!(
                            "ravel_memory_handoff_overlap_bytes{{mode=\"all\"}} {overlap}\n"
                        )),
                        "the overlap gauge must equal the fetch reservations ({overlap}):\n{scrape}"
                    );
                    assert!(
                        scrape.contains(&format!(
                            "ravel_memory_reserved_bytes{{mode=\"all\",component=\"fetch\"}} {expected}\n"
                        )),
                        "the fetch gauge must equal the fetcher's reservation ({expected}):\n{scrape}"
                    );
                }
                gate.release(id);
            }
        }
    }

    assert_eq!(budget.fetch_reserved(), 0);
    assert_eq!(
        budget.handoff_overlap(),
        0,
        "the overlap must release once the fetch reservation drops"
    );
    let scrape = scrape_metrics(&metrics).await;
    assert!(
        scrape.contains("ravel_memory_handoff_overlap_bytes{mode=\"all\"} 0\n"),
        "the overlap gauge must read back to 0 after completion:\n{scrape}"
    );
}

/// Builds the real SQL `SqlState` via `crate::query::build_sql_state` (the
/// same constructor `crate::start` calls), plus a `/metrics` router sharing
/// one `Catalog` and the SAME `process_memory_budget` instance, so an
/// issue #2086 SQL memory-budget acceptance test exercises the actual server
/// wiring rather than a hand-assembled `SqlExecutor` (unlike `harness` above,
/// whose fetchers get no `.with_memory_budget` call at all).
fn sql_budget_harness(
    store: Arc<dyn ObjectStoreBackend>,
    process_memory_budget: Arc<ravel_memory::MemoryBudget>,
) -> (Arc<SqlExecutor>, Router, Router) {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let catalog_for_metrics = Arc::clone(&catalog);
    let catalog_cache_metrics = catalog.byte_cache_metrics();
    let query_accounting = Arc::new(QueryAccountingMetrics::new(HashSet::new()));
    let tokens: HashMap<String, TenantId> =
        HashMap::from([("acme-token".to_string(), TenantId::new("acme".to_string()))]);
    let tenant_resolver: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));

    let state = crate::query::build_sql_state(
        catalog,
        Arc::clone(&store),
        tenant_resolver,
        None,
        EngineConfig::default(),
        Arc::new(GetLimiter::new(8).expect("nonzero permits")),
        ravel_sql::DEFAULT_MAX_QUERY_BYTES,
        crate::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::clone(&query_accounting),
        QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
        None,
        Arc::clone(&process_memory_budget),
    )
    .expect("sql state builds");

    let executor = Arc::clone(&state.executor);
    let sql = sql_router(state);

    let metrics = crate::metrics::router(MetricsState {
        mode: Mode::All,
        store_metrics: Arc::new(StoreMetrics::default()),
        ingest_router: None,
        log_ingest_router: None,
        span_ingest_router: None,
        catalog: catalog_for_metrics,
        tenant_discovery: None,
        maintenance_safety: None,
        maintenance_ownership: None,
        merge_memory: None,
        scrub: None,
        cache_metrics: None,
        cache_disk_metrics: None,
        catalog_cache_metrics,
        catalog_cache_disk_metrics: None,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        reconcile_cycle: Arc::new(crate::admission_reconcile::ReconcileCycleMetrics::default()),
        metrics_tenant_labels: false,
        metrics_tenant_allowlist: Arc::new(HashSet::new()),
        query_accounting,
        ingest_concurrency: crate::ingest_concurrency::IngestConcurrencyController::shared(
            crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        distrib: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_rejects: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_tls_dials: None,
        durable_auth: None,
        ingest_byte_metrics: std::sync::Arc::new(
            crate::ingest_byte_metrics::IngestByteMetrics::new(),
        ),
        normalize_reject_metrics: std::sync::Arc::new(
            crate::normalize_reject_metrics::NormalizeRejectMetrics::new(),
        ),
        metadata_cache: None,
        cache: None,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        audit_pipeline: None,
        process_memory_budget,
        process_memory_budget_is_fallback: false,
        memory_admission: Arc::new(MemoryAdmissionGate::disabled()),
        cpu_gates: crate::cpu_gates::CpuGates::new(Default::default()),
        can_fold: true,
        fold_loop: Default::default(),
        refold: Default::default(),
        heartbeat: crate::health_listener::Heartbeat::new(Arc::new(SystemClock)),
    });

    (executor, sql, metrics)
}

/// Publishes one real RSEG segment for `tenant`/`metric`, anchored inside the
/// fixed `[0, NOW_NS]` window the SQL tests' `post_sql`/`sql_body` already
/// query, with high-entropy sample values so its data object exceeds the
/// 512 KiB whole-object-read threshold: only past that threshold does
/// `ensure_ranges` reserve against the process memory budget at all, rather
/// than reading the whole object in one unbudgeted GET. Asserts the threshold
/// was really crossed rather than assuming it from the sample count.
///
/// Unlike `publish_large_segment` above (anchored to real wall-clock time,
/// needed there because the raw PromQL HTTP handler's `now_ns()` is not
/// injectable), this fixture stays inside the small fixed SQL window, so
/// `Catalog::resolve`'s per-(shard, ingest-hour) LIST fan-out stays at a
/// handful of hours rather than one per real wall-clock hour since the epoch.
async fn publish_large_sql_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    metric: &str,
) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, metric, &label_set).expect("series id");

    let base_ts_ns = NOW_NS - 300 * NS_PER_SEC;
    let samples: Vec<Sample> = (0..LARGE_SEGMENT_SAMPLES)
        .map(|i| Sample {
            ts_ns: base_ts_ns + i as i64 * 1_000_000,
            value: high_entropy_value(i),
        })
        .collect();

    let series = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples,
    }];

    let writer_id = Uuid::from_u128(4_000);
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

    let object_size = written.bytes.len() as u64;
    assert!(
        object_size > 512 * 1024,
        "fixture must exceed the 512 KiB whole-object threshold to force a \
         budgeted range read, got {object_size} bytes"
    );

    let hour_bucket = u32::try_from(base_ts_ns / NS_PER_HOUR).expect("hour bucket");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: NOW_NS,
        ingest_hour_bucket: hour_bucket,
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

/// One log record on stream `service.name = "budget-svc"`, mirroring
/// `tests/sql_endpoint.rs`'s `log_record` fixture builder.
fn sql_log_record(ts: i64, body: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("budget-svc".to_string()),
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

/// Publishes one real RLOG object plus its `Signal::Logs` commit record.
/// `LogSegmentFetcher::whole_object_bytes` reserves against the process
/// memory budget for any object at or below the ADR-0996 fetch bound (64 MiB
/// by default; see `EngineConfig::logs_max_fetch_run_bytes`), so a single
/// small record is enough to force a budgeted reservation, unlike RSEG's
/// small-object path which needs a >512 KiB fixture.
async fn publish_small_log_segment(
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

    let writer_id = Uuid::from_u128(9_500);
    let identity = ravel_logseg::writer::ObjectIdentity {
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

/// One span on `service.name = "budget-svc"`, mirroring `tests/sql_endpoint.rs`'s
/// `span_record` fixture builder.
fn sql_span_record(trace: [u8; 16], start: i64) -> ravel_rspan::SpanRecord {
    ravel_rspan::SpanRecord {
        trace_id: trace,
        span_id: [1; 8],
        parent_span_id: None,
        name: "budget-span".to_string(),
        start_ts_ns: start,
        end_ts_ns: start + 1_000_000,
        status_code: ravel_rspan::StatusCode::Ok,
        status_message: None,
        attrs: vec![("service.name".to_string(), "budget-svc".to_string())],
    }
}

/// Publishes one real RSPAN object plus its `Signal::Spans` commit record.
/// Unlike RSEG and RLOG, `SpanSegmentFetcher::whole_object_bytes` reserves
/// unconditionally for every read regardless of object size, so a single
/// small span is enough to force a budgeted reservation.
async fn publish_small_span_segment(
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

    let writer_id = Uuid::from_u128(5_500);
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

/// ACCEPTANCE TEST (issue #2086): a SQL `samples` (RSEG) fetch whose real
/// execution outgrows the ADR-1170 process-wide memory budget is refused
/// typed (`SqlError::Fetch(FetchError::FetchMemoryExhausted)`) and, through
/// the real HTTP router, as `StatusCode::SERVICE_UNAVAILABLE` /
/// `errorType: "unavailable"`. This exercises `crate::query::build_sql_state`'s
/// `.with_memory_budget(process_memory_budget)` wiring on the metrics
/// fetcher, not a hand-built `SegmentFetcher` (`harness` above never calls
/// `.with_memory_budget` at all).
///
/// The fixture crosses the 512 KiB whole-object-read threshold
/// (`publish_large_sql_segment`) so the refused reservation is the ranged
/// read's (`ensure_ranges`) column fetch, the charge this test is about.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget.clone())`
/// from the metrics fetcher in `build_sql_state`
/// (services/ravel-server/src/query.rs). The fetch then reserves nothing and
/// the same tiny budget refuses the query in the SQL pool instead, as
/// `ResourcesExhausted` (422), so both the typed check and the 503 assertion
/// fail.
#[tokio::test]
async fn a_sql_metrics_fetch_over_the_process_budget_is_refused_and_the_process_keeps_serving() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_large_sql_segment(store.as_ref(), &tenant, "sql_budget_metric").await;

    let budget = Arc::new(ravel_memory::MemoryBudget::new(4 * 1024));
    let (executor, sql, _metrics) = sql_budget_harness(Arc::clone(&store), Arc::clone(&budget));

    let request = SqlRequest {
        sql: "SELECT ts, value FROM samples ORDER BY ts".to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    let err = executor
        .execute(tenant.hash(), &request)
        .await
        .expect_err("a 4 KiB budget must refuse a >512 KiB range read");
    match err {
        SqlError::Fetch(FetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        }) => {
            assert_eq!(
                limit,
                4 * 1024,
                "the refusal must name the configured limit"
            );
            assert!(
                requested > limit.saturating_sub(reserved),
                "the refused reservation must need more than the budget remainder: \
                 requested {requested}, reserved {reserved}"
            );
        }
        other => panic!("expected SqlError::Fetch(FetchMemoryExhausted), got {other:?}"),
    }
    assert_eq!(
        budget.reserved(),
        0,
        "the refused query must leave no charge on the shared process counter"
    );

    let (status, body) = post_sql(&sql, "SELECT ts, value FROM samples ORDER BY ts").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a refused fetch must answer 503 unavailable: {body}"
    );
    assert_eq!(body["errorType"], "unavailable", "body: {body}");
    assert_eq!(
        body["error"], FETCH_MEMORY_EXHAUSTED_MESSAGE,
        "a fetch memory refusal names memory, not storage: {body}"
    );

    assert_eq!(budget.reserved(), 0);
    let (status, body) = post_sql(&sql, "SELECT 1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the process must keep answering after a refusal: {body}"
    );
}

/// ACCEPTANCE TEST (issue #2086): a SQL `logs` (RLOG) fetch whose real
/// execution outgrows the ADR-1170 process-wide memory budget is refused
/// typed (`SqlError::LogFetch(LogFetchError::FetchMemoryExhausted)`) and,
/// through the real HTTP router, as `StatusCode::SERVICE_UNAVAILABLE` /
/// `errorType: "unavailable"`. Sibling of the metrics test above, for the
/// logs fetcher's own `.with_memory_budget` wiring in `build_sql_state`.
///
/// A single tiny log record is enough to force the reservation: unlike RSEG,
/// RLOG's whole-object read path reserves for any object at or below the
/// 64 MiB ADR-0996 fetch bound, so no large fixture is needed.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget.clone())`
/// from the logs fetcher in `build_sql_state`. The typed check then fails the
/// same way as the metrics test above.
#[tokio::test]
async fn a_sql_logs_fetch_over_the_process_budget_is_refused_and_the_process_keeps_serving() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let ts = NOW_NS - 300 * NS_PER_SEC;
    publish_small_log_segment(
        store.as_ref(),
        &tenant,
        &[sql_log_record(ts, "budget probe")],
    )
    .await;

    let budget = Arc::new(ravel_memory::MemoryBudget::new(1));
    let (executor, sql, _metrics) = sql_budget_harness(Arc::clone(&store), Arc::clone(&budget));

    let request = SqlRequest {
        sql: "SELECT ts, body FROM logs".to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    let err = executor
        .execute(tenant.hash(), &request)
        .await
        .expect_err("a 1-byte budget must refuse any whole-object logs read");
    match err {
        SqlError::LogFetch(LogFetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        }) => {
            assert_eq!(limit, 1, "the refusal must name the configured limit");
            assert!(
                requested > limit.saturating_sub(reserved),
                "the refused reservation must need more than the budget remainder: \
                 requested {requested}, reserved {reserved}"
            );
        }
        other => panic!("expected SqlError::LogFetch(FetchMemoryExhausted), got {other:?}"),
    }
    assert_eq!(
        budget.reserved(),
        0,
        "the refused query must leave no charge on the shared process counter"
    );

    let (status, body) = post_sql(&sql, "SELECT ts, body FROM logs").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a refused fetch must answer 503 unavailable: {body}"
    );
    assert_eq!(body["errorType"], "unavailable", "body: {body}");
    assert_eq!(
        body["error"], FETCH_MEMORY_EXHAUSTED_MESSAGE,
        "a fetch memory refusal names memory, not storage: {body}"
    );

    assert_eq!(budget.reserved(), 0);
    let (status, body) = post_sql(&sql, "SELECT 1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the process must keep answering after a refusal: {body}"
    );
}

/// ACCEPTANCE TEST (issue #2086): a SQL `spans` (RSPAN) fetch whose real
/// execution outgrows the ADR-1170 process-wide memory budget is refused
/// typed (`SqlError::SpanFetch(SpanFetchError::FetchMemoryExhausted)`) and,
/// through the real HTTP router, as `StatusCode::SERVICE_UNAVAILABLE` /
/// `errorType: "unavailable"`. Sibling of the two tests above, for the spans
/// fetcher's own `.with_memory_budget` wiring in `build_sql_state`.
///
/// A single tiny span is enough to force the reservation:
/// `SpanSegmentFetcher::whole_object_bytes` reserves unconditionally for
/// every read, with no size threshold at all.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget)` from
/// the spans fetcher in `build_sql_state`. The typed check then fails the
/// same way as the two tests above.
#[tokio::test]
async fn a_sql_spans_fetch_over_the_process_budget_is_refused_and_the_process_keeps_serving() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let ts = NOW_NS - 300 * NS_PER_SEC;
    publish_small_span_segment(store.as_ref(), &tenant, &[sql_span_record([7; 16], ts)]).await;

    let budget = Arc::new(ravel_memory::MemoryBudget::new(1));
    let (executor, sql, _metrics) = sql_budget_harness(Arc::clone(&store), Arc::clone(&budget));

    let request = SqlRequest {
        sql: "SELECT name, service_name FROM spans ORDER BY start_ts".to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    let err = executor
        .execute(tenant.hash(), &request)
        .await
        .expect_err("a 1-byte budget must refuse any whole-object spans read");
    match err {
        SqlError::SpanFetch(SpanFetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        }) => {
            assert_eq!(limit, 1, "the refusal must name the configured limit");
            assert!(
                requested > limit.saturating_sub(reserved),
                "the refused reservation must need more than the budget remainder: \
                 requested {requested}, reserved {reserved}"
            );
        }
        other => panic!("expected SqlError::SpanFetch(FetchMemoryExhausted), got {other:?}"),
    }
    assert_eq!(
        budget.reserved(),
        0,
        "the refused query must leave no charge on the shared process counter"
    );

    let (status, body) = post_sql(
        &sql,
        "SELECT name, service_name FROM spans ORDER BY start_ts",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a refused fetch must answer 503 unavailable: {body}"
    );
    assert_eq!(body["errorType"], "unavailable", "body: {body}");
    assert_eq!(
        body["error"], FETCH_MEMORY_EXHAUSTED_MESSAGE,
        "a fetch memory refusal names memory, not storage: {body}"
    );

    assert_eq!(budget.reserved(), 0);
    let (status, body) = post_sql(&sql, "SELECT 1").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the process must keep answering after a refusal: {body}"
    );
}

/// ACCEPTANCE TEST (issue #2086's observability requirement): while a SQL
/// `logs` fetch's budgeted whole-object GET is in flight,
/// `ravel_memory_reserved_bytes{mode="all",component="fetch"}` reads exactly
/// the fetcher's own live reservation, both through the direct counter and
/// through a real `/metrics` scrape, and returns to exactly 0 once the query
/// completes.
///
/// The gauge itself reads back to 0 after the query answers -- there is no
/// persisted counter or peak figure for a fetch reservation on `/metrics`
/// today, and adding a new metric family is out of this task's scope -- so
/// the reservation is observed HELD, mirroring the existing PromQL gauge test
/// (`memory_gauges_report_a_nonzero_fetch_reservation_during_a_query` above)
/// rather than read back after completion. The figure asserted on is
/// `ravel_memory_reserved_bytes{component="fetch"}`, the same gauge that test
/// uses, proved nonzero while a real `FaultStore`-held GET keeps a live
/// `Reservation` open on the SQL logs path specifically.
///
/// Prove-the-test: remove `.with_memory_budget(process_memory_budget.clone())`
/// from the logs fetcher in `build_sql_state`. `budget.fetch_reserved()` then
/// reads 0 throughout (nothing is ever reserved), so
/// `observed_nonzero` stays `false` and the query's own branch of the
/// `select!` panics: "the fetch gauge must be observed nonzero at least once
/// while the GET was held".
#[tokio::test]
async fn sql_logs_query_reserves_a_nonzero_fetch_gauge_while_the_get_is_held() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let tenant = TenantId::new("acme".to_string());
    let ts = NOW_NS - 300 * NS_PER_SEC;
    publish_small_log_segment(
        store.as_ref(),
        &tenant,
        &[sql_log_record(ts, "gauge probe")],
    )
    .await;

    let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
    let (executor, _sql, metrics) = sql_budget_harness(Arc::clone(&store), Arc::clone(&budget));

    let request = SqlRequest {
        sql: "SELECT ts, body FROM logs".to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };

    let gate = fault_store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Always);
    let mut query = Box::pin(executor.execute(tenant.hash(), &request));

    let mut observed_nonzero = false;
    loop {
        tokio::select! {
            result = &mut query => {
                assert!(
                    observed_nonzero,
                    "the fetch gauge must be observed nonzero at least once while the GET was held"
                );
                result.expect("query must succeed under an unlimited budget");
                break;
            }
            () = gate.wait_until_held(1) => {
                let held = gate.held_details();
                let (id, _, _) = held[0];
                let reserved = budget.fetch_reserved();
                if reserved > 0 {
                    observed_nonzero = true;
                    assert!(
                        budget.reserved() >= reserved,
                        "the total reserved must include the fetch share: total {}, fetch {reserved}",
                        budget.reserved()
                    );
                    let scrape = scrape_metrics(&metrics).await;
                    assert!(
                        scrape.contains(&format!(
                            "ravel_memory_reserved_bytes{{mode=\"all\",component=\"fetch\"}} {reserved}\n"
                        )),
                        "the fetch gauge must equal the fetcher's reservation ({reserved}):\n{scrape}"
                    );
                }
                gate.release(id);
            }
        }
    }

    assert_eq!(
        budget.fetch_reserved(),
        0,
        "the reservation must release once the query completes"
    );
    let scrape = scrape_metrics(&metrics).await;
    assert!(
        scrape.contains("ravel_memory_reserved_bytes{mode=\"all\",component=\"fetch\"} 0\n"),
        "the fetch gauge must read back to 0 after completion:\n{scrape}"
    );
}
