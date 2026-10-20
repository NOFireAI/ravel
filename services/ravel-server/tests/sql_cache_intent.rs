//! The read-cache access intent a `POST /api/v1/sql` statement reads under,
//! end to end: real RSEG segments and commit records, a real catalog, the
//! fetcher cache `store::build_cache` builds from parsed `Cli` flags, and the
//! route driven through `tower::ServiceExt::oneshot`.
//!
//! Every figure is read off what a client receives: `stats.cache` in the
//! response body and the `/metrics` scrape.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use clap::Parser;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, ReadCache, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::Cli;
use ravel_server::sql::{SqlState, router};
use ravel_server::store::build_cache;
use ravel_sql::{SqlConfig, SqlExecutor};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: resolve lists every ingest hour of the window, so a
/// wall-clock value would fan out to hundreds of thousands of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

/// A deterministic xorshift stream of values in `[0, 1)`, so a segment's
/// size is set by its sample count rather than by how well it compresses.
struct Values(u64);

impl Values {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Publish one single-series segment for `tenant` whose samples are spread
/// evenly across `hour`, committed in that ingest hour.
/// Returns the segment object's size. `index` must be unique per store: it
/// picks the writer identity.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    hour: i64,
    metric: &str,
    sample_count: usize,
) -> u64 {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let mut values = Values(0x9e37_79b9_7f4a_7c15 ^ index as u64);
    let base = hour * NS_PER_HOUR;
    let step = NS_PER_HOUR / sample_count as i64;
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: (0..sample_count)
            .map(|i| Sample {
                ts_ns: base + i as i64 * step,
                value: values.next(),
            })
            .collect(),
    }];

    let writer_id = Uuid::from_u128(5_000 + index as u128);
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
            min_ingest_ts_ns: base,
            max_ingest_ts_ns: base,
        },
    )
    .expect("write segment");
    let size = written.bytes.len() as u64;

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: size,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base + 10 + index as i64,
        ingest_hour_bucket: u32::try_from(hour).expect("small hour"),
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
    size
}

/// Publish one RLOG object for `tenant` holding `count` records spread over
/// `hour`, committed in that ingest hour.
async fn publish_log_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    hour: i64,
    count: usize,
) {
    let tenant_hash = tenant.hash();
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let base = hour * NS_PER_HOUR;
    let step = NS_PER_HOUR / count as i64;
    let records: Vec<LogRecord> = (0..count)
        .map(|i| LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: base + i as i64 * step,
            observed_ts_ns: base + i as i64 * step,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("request {i}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        })
        .collect();
    let min_event_ts_ns = records.first().expect("records").ts_ns;
    let max_event_ts_ns = records.last().expect("records").ts_ns;

    let writer_id = Uuid::from_u128(9_000 + index as u128);
    let identity = LogObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for rec in &records {
        writer.push(rec.clone()).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: base + 10 + index as i64,
        ingest_hour_bucket: u32::try_from(hour).expect("small hour"),
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

/// The fetcher cache the server builds for `--cache-max-bytes max_bytes`
/// with the default `--cache-bulk-share`.
fn server_cache(max_bytes: u64) -> ReadCache {
    let max = max_bytes.to_string();
    let cli = Cli::try_parse_from(["ravel-server", "--cache-max-bytes", max.as_str()])
        .expect("cache flags parse");
    build_cache(&cli, max_bytes).expect("a nonzero cache is built")
}

/// The SQL router over `store`, its metrics and logs fetchers sharing
/// `cache` the way `query::build_sql_executor` attaches it.
fn sql_app(store: Arc<dyn ObjectStoreBackend>, cache: &ReadCache, tenants: &[&str]) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()).with_cache(cache.clone()),
        LogSegmentFetcher::new(store.clone()).with_cache(cache.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()).with_cache(cache.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    let tokens: HashMap<String, TenantId> = tenants
        .iter()
        .map(|tenant| (format!("{tenant}-token"), TenantId::new(tenant.to_string())))
        .collect();
    router(SqlState {
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
    })
}

/// Run `query` for `tenant` over `[start_ns, NOW_NS]` and return its
/// `stats.cache` object.
async fn statement_cache(app: &Router, tenant: &str, query: &str, start_ns: i64) -> Value {
    let payload = serde_json::json!({
        "query": query,
        "start": start_ns as f64 / 1_000_000_000.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {tenant}-token"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("JSON body");
    assert_eq!(status, StatusCode::OK, "{query}: {value}");
    value["stats"]["cache"].clone()
}

fn count(cache: &Value, path: &[&str]) -> u64 {
    let mut value = cache;
    for key in path {
        value = &value[*key];
    }
    value
        .as_u64()
        .unwrap_or_else(|| panic!("stats.cache.{} is not a count: {cache}", path.join(".")))
}

const FULL_SCAN: &str = "SELECT count(*) AS n FROM samples";

/// FLIPs, in `crates/ravel-sql/src/access_intent.rs` `reads_whole_lane`:
///
/// - reducing the `TargetSignal::Metrics` lane to
///   `snapshot.segments_pruned == 0` and nothing else classifies both the
///   one-series statement and the partial-window one as bulkScan, because an
///   unfolded catalog prunes nothing;
/// - dropping the `window_covers_all_history(window, now_ns)` conjunct
///   classifies the partial-window one as bulkScan: resolve never returns a
///   segment whose events sit wholly outside the window, so every segment it
///   does return lies inside it and the containment conjunct alone passes.
#[tokio::test]
async fn a_full_tenant_scan_is_admitted_as_bulk_and_a_one_series_statement_as_interactive() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    for hour in 0..4 {
        let metric = format!("m{hour}");
        publish_segment(store.as_ref(), &tenant, hour as usize, hour, &metric, 64).await;
    }
    let cache = server_cache(8 * 1024 * 1024);
    let app = sql_app(store, &cache, &["acme"]);

    let first = statement_cache(&app, "acme", FULL_SCAN, 0).await;
    assert_eq!(first["intent"], "bulkScan", "{first}");
    assert_eq!(count(&first, &["bulkScan", "hits"]), 0, "{first}");
    assert_eq!(count(&first, &["misses"]), 4, "{first}");

    // The same scan again is served from the bulk region: one hit per
    // segment, all of them under bulkScan.
    let second = statement_cache(&app, "acme", FULL_SCAN, 0).await;
    assert_eq!(second["intent"], "bulkScan", "{second}");
    assert_eq!(count(&second, &["bulkScan", "hits"]), 4, "{second}");
    assert_eq!(count(&second, &["interactive", "hits"]), 0, "{second}");
    assert_eq!(count(&second, &["misses"]), 0, "{second}");

    // One series over the last hour prunes nothing on an unfolded catalog
    // and is still a dashboard query.
    let one_series = statement_cache(
        &app,
        "acme",
        "SELECT count(*) AS n FROM samples WHERE label(labels, '__name__') = 'm3'",
        3 * NS_PER_HOUR,
    )
    .await;
    // Every segment but the first, over a window that excludes it.
    let partial = statement_cache(&app, "acme", FULL_SCAN, NS_PER_HOUR).await;
    assert_eq!(
        [&one_series["intent"], &partial["intent"]],
        ["interactive", "interactive"],
        "one series: {one_series}, partial window: {partial}"
    );
}

/// FLIP: dropping the `window_covers_all_history(window, now_ns)` conjunct
/// from `reads_whole_lane` in `crates/ravel-sql/src/access_intent.rs` (the
/// logs rule before it carried one) classifies the one-hour statement as
/// bulkScan, because the window still contains the tenant's only segment.
#[tokio::test]
async fn a_predicate_free_logs_statement_is_bulk_only_over_an_epoch_to_now_window() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_log_segment(store.as_ref(), &tenant, 0, 3, 64).await;
    let cache = server_cache(8 * 1024 * 1024);
    let app = sql_app(store, &cache, &["acme"]);
    let statement = "SELECT severity_text, count(*) AS n FROM logs GROUP BY severity_text";

    let last_hour = statement_cache(&app, "acme", statement, 3 * NS_PER_HOUR).await;
    assert_eq!(last_hour["intent"], "interactive", "{last_hour}");
    assert!(count(&last_hour, &["misses"]) > 0, "{last_hour}");

    let all_history = statement_cache(&app, "acme", statement, 0).await;
    assert_eq!(all_history["intent"], "bulkScan", "{all_history}");
}

/// FLIP: dropping `&& predicate_free` from `classify_signal_lanes` in
/// `crates/ravel-sql/src/access_intent.rs` (the samples rule before it
/// carried no plan conjunct) classifies the `WHERE ts > ...` statement as
/// bulkScan: an unfolded catalog prunes nothing and every segment lies inside
/// the epoch-to-now window.
#[tokio::test]
async fn a_samples_statement_with_a_predicate_is_interactive_over_an_epoch_to_now_window() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    for hour in 0..4 {
        let metric = format!("m{hour}");
        publish_segment(store.as_ref(), &tenant, hour as usize, hour, &metric, 64).await;
    }
    let cache = server_cache(8 * 1024 * 1024);
    let app = sql_app(store, &cache, &["acme"]);

    let narrowed = statement_cache(
        &app,
        "acme",
        "SELECT count(*) AS n FROM samples WHERE ts > TIMESTAMP '1970-01-01 03:00:00'",
        0,
    )
    .await;
    assert_eq!(narrowed["intent"], "interactive", "{narrowed}");
    let whole = statement_cache(&app, "acme", FULL_SCAN, 0).await;
    assert_eq!(whole["intent"], "bulkScan", "{whole}");
}

/// Cache geometry for the hot-set test, scaled down from the ravel-cache
/// server-geometry hot-set tests: a 512 KiB cache, a hot set of four ~16 KiB
/// segments read by interactive statements, and a bulk loop three times the
/// cache's size.
const HOT_CACHE_BYTES: u64 = 512 * 1024;
const HOT_SEGMENTS: usize = 4;
const HOT_SAMPLES: usize = 2_200;
const BULK_SEGMENTS: usize = 24;
const BULK_SAMPLES: usize = 9_500;
const ROUNDS: usize = 4;
const HOT_STATEMENTS_PER_ROUND: usize = 8;

/// Statements are interleaved one at a time, so each statement's
/// `stats.cache` delta, `misses` included, is its own.
///
/// FLIP: making every `ReadCache` method in `crates/ravel-query/src/fetcher.rs`
/// read and admit as `ravel_cache::AccessIntent::Interactive` whatever intent
/// it is passed (the classification still reported, so the full-tenant
/// scan's reads go through the small and main queues like any other) drops
/// the hot-set hits to 124 of 128.
#[tokio::test]
async fn an_interactive_statement_keeps_its_hot_set_between_rounds_of_a_bulk_loop() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let hot = TenantId::new("hot".to_string());
    let bulk = TenantId::new("bulk".to_string());
    let mut hot_bytes = 0;
    for i in 0..HOT_SEGMENTS {
        let size =
            publish_segment(store.as_ref(), &hot, i, 3, &format!("hot{i}"), HOT_SAMPLES).await;
        assert!(
            (12 * 1024..=20 * 1024).contains(&size),
            "hot segment {i} is {size} bytes, not about 16 KiB"
        );
        hot_bytes += size;
    }
    let mut loop_bytes = 0;
    for i in 0..BULK_SEGMENTS {
        let size = publish_segment(
            store.as_ref(),
            &bulk,
            HOT_SEGMENTS + i,
            (i % 4) as i64,
            &format!("bulk{i}"),
            BULK_SAMPLES,
        )
        .await;
        loop_bytes += size;
    }
    assert!(hot_bytes < HOT_CACHE_BYTES / 4, "hot set {hot_bytes} bytes");
    assert!(
        loop_bytes >= 3 * HOT_CACHE_BYTES,
        "bulk loop {loop_bytes} bytes is under three caches"
    );
    let cache = server_cache(HOT_CACHE_BYTES);
    let app = sql_app(store, &cache, &["hot", "bulk"]);
    let hot_statement = "SELECT count(*) AS n FROM samples";

    let warm = statement_cache(&app, "hot", hot_statement, 3 * NS_PER_HOUR).await;
    assert_eq!(warm["intent"], "interactive", "{warm}");
    assert_eq!(count(&warm, &["misses"]), HOT_SEGMENTS as u64, "{warm}");

    let (mut hot_hits, mut hot_misses, mut bulk_hits) = (0, 0, 0);
    for round in 0..ROUNDS {
        let scan = statement_cache(&app, "bulk", FULL_SCAN, 0).await;
        assert_eq!(scan["intent"], "bulkScan", "round {round}: {scan}");
        if round > 0 {
            bulk_hits += count(&scan, &["bulkScan", "hits"]);
        }
        for _ in 0..HOT_STATEMENTS_PER_ROUND {
            let read = statement_cache(&app, "hot", hot_statement, 3 * NS_PER_HOUR).await;
            assert_eq!(read["intent"], "interactive", "round {round}: {read}");
            hot_hits += count(&read, &["interactive", "hits"]);
            hot_misses += count(&read, &["misses"]);
        }
    }

    let lookups = hot_hits + hot_misses;
    assert_eq!(
        lookups,
        (ROUNDS * HOT_STATEMENTS_PER_ROUND * HOT_SEGMENTS) as u64,
        "one lookup per hot segment per statement"
    );
    assert_eq!(
        (hot_hits, hot_misses),
        (128, 0),
        "every hot-set lookup after the warm read is an interactive hit"
    );
    // The loop must have been cached beside the hot set, or the hot set
    // held only because nothing bulk was ever admitted.
    assert!(
        bulk_hits > 0,
        "the bulk loop was never served from the cache"
    );
}

/// The `/metrics` router reading `cache`'s RAM-tier counters, as
/// `ravel-server` wires `MetricsState.cache_metrics`.
fn metrics_app(store: Arc<dyn ObjectStoreBackend>, cache: &ReadCache) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    ravel_server::metrics::router(ravel_server::metrics::MetricsState {
        mode: ravel_server::Mode::All,
        store_metrics: Arc::new(ravel_object_store::StoreMetrics::default()),
        ingest_router: None,
        log_ingest_router: None,
        span_ingest_router: None,
        catalog,
        tenant_discovery: None,
        maintenance_safety: None,
        maintenance_ownership: None,
        merge_memory: None,
        scrub: None,
        cache_metrics: Some(cache.ram_metrics()),
        cache_disk_metrics: None,
        catalog_cache_metrics: None,
        catalog_cache_disk_metrics: None,
        admission: Arc::new(ravel_ingest::AdmissionController::new(
            Arc::new(ravel_ingest::SystemClock),
            ravel_ingest::AdmissionLimits::default(),
        )),
        reconcile_cycle: Arc::new(
            ravel_server::admission_reconcile::ReconcileCycleMetrics::default(),
        ),
        metrics_tenant_labels: false,
        query_accounting: Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        metrics_tenant_allowlist: Arc::new(std::collections::HashSet::new()),
        ingest_buffer_budget: ravel_ingest::IngestByteBudget::shared(
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        ),
        ingest_concurrency: ravel_server::ingest_concurrency::IngestConcurrencyController::shared(
            ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(1024),
        ),
        distrib: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_rejects: None,
        #[cfg(feature = "flight-sql")]
        sql_slice_tls_dials: None,
        durable_auth: None,
        ingest_byte_metrics: Arc::new(ravel_server::ingest_byte_metrics::IngestByteMetrics::new()),
        normalize_reject_metrics: Arc::new(
            ravel_server::normalize_reject_metrics::NormalizeRejectMetrics::new(),
        ),
        metadata_cache: None,
        cache: Some(cache.clone()),
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        audit_pipeline: None,
        process_memory_budget: Arc::new(ravel_memory::MemoryBudget::unlimited()),
        process_memory_budget_is_fallback: false,
        cpu_gates: ravel_server::cpu_gates::CpuGates::new(Default::default()),
        can_fold: true,
        fold_loop: Default::default(),
        refold: Default::default(),
        heartbeat: ravel_server::health_listener::Heartbeat::new(Arc::new(
            ravel_ingest::SystemClock,
        )),
    })
}

async fn scrape(app: &Router) -> String {
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
    String::from_utf8(bytes.to_vec()).expect("utf8 metrics")
}

/// The value of the one sample line `series` names in `scrape`; absent or
/// rendered more than once is a failure.
fn sample(scrape: &str, series: &str) -> u64 {
    let prefix = format!("{series} ");
    let values: Vec<u64> = scrape
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.parse().expect("integer sample"))
        .collect();
    assert_eq!(values.len(), 1, "{series} rendered {} times", values.len());
    values[0]
}

/// FLIP: dropping `Label::CacheIntent(intent)` from the sample labels in
/// `render_cache_intent_family` (`services/ravel-server/src/metrics.rs`)
/// leaves no `intent` sample in the scrape, and `sample` fails on the first
/// series.
#[tokio::test]
async fn a_bulk_statement_moves_the_bulk_scan_intent_sample_on_a_real_scrape() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    for hour in 0..4 {
        let metric = format!("m{hour}");
        publish_segment(store.as_ref(), &tenant, hour as usize, hour, &metric, 64).await;
    }
    let cache = server_cache(8 * 1024 * 1024);
    let app = sql_app(Arc::clone(&store), &cache, &["acme"]);
    let metrics = metrics_app(store, &cache);

    const BULK_HITS: &str =
        r#"ravel_cache_intent_hits_total{mode="all",cache="fetch",intent="bulk_scan"}"#;
    const BULK_SERVED: &str =
        r#"ravel_cache_intent_bytes_served_total{mode="all",cache="fetch",intent="bulk_scan"}"#;
    const INTERACTIVE_HITS: &str =
        r#"ravel_cache_intent_hits_total{mode="all",cache="fetch",intent="interactive"}"#;

    let before = scrape(&metrics).await;
    assert_eq!(sample(&before, BULK_HITS), 0);
    statement_cache(&app, "acme", FULL_SCAN, 0).await;
    let second = statement_cache(&app, "acme", FULL_SCAN, 0).await;
    let after = scrape(&metrics).await;

    assert_eq!(
        sample(&after, BULK_HITS),
        count(&second, &["bulkScan", "hits"]),
        "{after}"
    );
    assert_eq!(sample(&after, BULK_HITS), 4, "{after}");
    assert_eq!(
        sample(&after, BULK_SERVED),
        count(&second, &["bulkScan", "servedBytes"]),
        "{after}"
    );
    assert_eq!(sample(&after, INTERACTIVE_HITS), 0, "{after}");
}
