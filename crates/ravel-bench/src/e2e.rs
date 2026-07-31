//! End-to-end ingest-then-query benchmark core (docs/benchmarking.md
//! "End-to-end"): drives `IngestRouter` and `QueryEngine` directly (no HTTP)
//! against a real or in-memory object store. Lives in the lib (not the
//! `s3_e2e_bench` bin) so `tests/s3_e2e_smoke.rs` can exercise the same path
//! the bin runs, matching how `ravel_bench::codecs` and
//! `ravel_bench::read_accounting` are tested directly rather than through a
//! bin. Report-only: never changes ravel-ingest/ravel-catalog/ravel-query
//! behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{Signal, TenantId, TimeRange};
use serde::Serialize;

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};

/// Bytes on the wire per logical sample: `ts_ns: i64` + `value: f64`
/// (docs/benchmarking.md "write amplification (bytes stored / bytes
/// ingested logical)"). Same constant `ingest_bench` uses.
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;
const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const VISIBILITY_POLL_MAX_ROUNDS: u32 = 30;

/// Inputs for one end-to-end run. `store_label` is display-only (the
/// `--store` flag's name); the actual backend is `store`.
pub struct E2eConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub ack_timeout_secs: u64,
    /// PromQL instant-selector query run repeatedly after ingest to build
    /// query-latency percentiles.
    pub query: String,
    /// Number of repeated instant queries run for the latency percentiles.
    pub query_count: usize,
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

struct LatencyStats {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    count: usize,
}

fn latency_stats(mut samples_ns: Vec<u64>) -> LatencyStats {
    samples_ns.sort_unstable();
    LatencyStats {
        p50_ns: percentile(&samples_ns, 0.50),
        p95_ns: percentile(&samples_ns, 0.95),
        p99_ns: percentile(&samples_ns, 0.99),
        max_ns: samples_ns.last().copied().unwrap_or(0),
        count: samples_ns.len(),
    }
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub accepted_points_per_sec: f64,
    pub accepted_points: u64,
    pub ack_latency_ms: LatencyReport,
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    /// Derived: one data-object PUT and one commit-record PUT per flush
    /// (`ravel_commit::publish::publish`), excluding retries.
    pub estimated_put_count: u64,
    pub bytes_written: u64,
    pub logical_bytes: u64,
    pub write_amplification: f64,
    pub visibility_lag_ms: VisibilityReport,
    /// Matched series in the query phase's PromQL instant vector. Zero
    /// (rather than the query phase being skipped) if the selector matched
    /// nothing; a non-zero `accepted_points` with a zero match count is
    /// itself the signal something is wrong, so this is left visible rather
    /// than folded away.
    pub query_matched_series: usize,
    pub query_latency_ms: LatencyReport,
    pub query_get_count: u64,
    pub query_list_count: u64,
    pub query_bytes_read: u64,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub query: String,
    pub query_count: usize,
}

#[derive(Serialize)]
pub struct LatencyReport {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: usize,
}

impl From<LatencyStats> for LatencyReport {
    fn from(s: LatencyStats) -> Self {
        LatencyReport {
            p50: s.p50_ns as f64 / 1e6,
            p95: s.p95_ns as f64 / 1e6,
            p99: s.p99_ns as f64 / 1e6,
            max: s.max_ns as f64 / 1e6,
            count: s.count,
        }
    }
}

#[derive(Serialize)]
pub struct VisibilityReport {
    pub resolved_count: usize,
    pub unresolved_count: usize,
    pub avg: f64,
    pub max: f64,
}

/// GET/LIST call counts and GET bytes returned, for the query phase (docs/
/// benchmarking.md "GET count, bytes read"). Not the same type as
/// `catalog_resolve_bench`'s `Counters`/`CountingStore`: that one has no byte
/// counter (it only needed request counts) and is private to that bin, so it
/// is duplicated here rather than shared -- see the final report for why
/// this was not worth extracting alongside `harness::store_from_env`.
#[derive(Default)]
struct QueryCounters {
    get: AtomicU64,
    list: AtomicU64,
    bytes_read: AtomicU64,
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    counters: Arc<QueryCounters>,
}

impl CountingStore {
    fn wrap(
        inner: Arc<dyn ObjectStoreBackend>,
    ) -> (Arc<dyn ObjectStoreBackend>, Arc<QueryCounters>) {
        let counters = Arc::new(QueryCounters::default());
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(CountingStore {
            inner,
            counters: Arc::clone(&counters),
        });
        (store, counters)
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.counters.get.fetch_add(1, Ordering::Relaxed);
        let outcome = self.inner.get(key, range).await?;
        self.counters
            .bytes_read
            .fetch_add(outcome.data.len() as u64, Ordering::Relaxed);
        Ok(outcome)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.counters.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.counters.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

pub async fn run(config: &E2eConfig) -> Report {
    let store = Arc::clone(&config.store);
    // Unique per run, not a fixed literal: `bytes_written`/`write_amplification`
    // below list by tenant prefix, so a fixed tenant would let consecutive
    // local runs against the same bucket inflate each other's byte counts.
    let tenant = TenantId::new(format!("bench-tenant-{}", uuid::Uuid::new_v4()));
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;

    let ingest_config = IngestConfig {
        shard_count: config.shards,
        ..IngestConfig::default()
    };
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = Arc::new(IngestRouter::new(
        ingest_config,
        Arc::clone(&store),
        signal,
        Arc::clone(&clock),
    ));
    let catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: config.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog config"),
    );

    let total_points = config.points_per_sec * config.duration_secs;
    let samples_per_series = (total_points as usize / config.target_series.max(1)).max(1);
    let run_start_ns = clock.now_ns();
    // Event timestamps must land within the real wall-clock span of the run:
    // the catalog's listing-based visibility poll below filters by event-time
    // overlap against a range anchored on `run_start_ns` (mirrors
    // `ingest_bench`'s identical constraint).
    let duration_ns = Duration::from_secs(config.duration_secs.max(1)).as_nanos() as i64;
    let interval_ns = (duration_ns / samples_per_series as i64).max(1);
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count: config.target_series,
        samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(config.batch_size),
        ..WorkloadConfig::default()
    };
    // The generator now stamps every series with a __name__ label (issue
    // #277), so the query phase's PromQL selector matches by name directly.
    let batches: Vec<Vec<_>> = generate_batches(&workload).expect("generate workload");

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let pacing_interval = if config.points_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(config.batch_size as f64 / config.points_per_sec as f64)
    };

    // Visibility lag is tracked concurrently with the write phase below, not
    // as a separate pass afterward (mirrors `ingest_bench`): acks span the
    // whole run, so measuring them only once every write has landed would
    // charge early acks for the rest of the run's wall time instead of their
    // real ack-to-queryable gap.
    let pending: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let lags_ns: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let writes_done = Arc::new(AtomicBool::new(false));

    let poller = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let clock = Arc::clone(&clock);
        let pending = Arc::clone(&pending);
        let lags_ns = Arc::clone(&lags_ns);
        let writes_done = Arc::clone(&writes_done);
        async move {
            for _ in 0..VISIBILITY_POLL_MAX_ROUNDS {
                tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
                let now_ns = clock.now_ns();
                let listing_range = TimeRange {
                    start_ns: run_start_ns,
                    end_ns: now_ns,
                };
                match catalog
                    .resolve(&tenant_hash, signal, listing_range, &[], now_ns)
                    .await
                {
                    Ok(snapshot) => {
                        let mut pending = pending.lock().expect("pending lock");
                        let mut lags_ns = lags_ns.lock().expect("lags lock");
                        for seg in snapshot.segments {
                            if let Some(ack_wall_ns) = pending.remove(&seg.data_object_key) {
                                lags_ns.push((now_ns - ack_wall_ns).max(0) as u64);
                            }
                        }
                        if writes_done.load(Ordering::Acquire) && pending.is_empty() {
                            break;
                        }
                    }
                    Err(err) => eprintln!("visibility: listing resolve failed: {err}"),
                }
            }
        }
    });

    let wall_start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(batches.len());
    let mut next_dispatch = tokio::time::Instant::now();
    for batch in batches {
        if pacing_interval > Duration::ZERO {
            tokio::time::sleep_until(next_dispatch).await;
            next_dispatch += pacing_interval;
        }
        let router = Arc::clone(&router);
        let clock = Arc::clone(&clock);
        let catalog = Arc::clone(&catalog);
        let pending = Arc::clone(&pending);
        let tenant = tenant.clone();
        let batch_len = batch.len() as u64;
        handles.push(tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = router
                .write(tenant, batch, WriteMode::Strict, ack_deadline)
                .await;
            let latency_ns = start.elapsed().as_nanos() as u64;
            let ack_wall_ns = clock.now_ns();
            if let Ok(receipt) = &result {
                // Resolve this ack's exact segment (read-your-write min-token
                // GET) right away, concurrently with every other in-flight
                // batch, so the poller above can see it on its very next
                // tick rather than only after the whole write phase ends.
                for token in &receipt.tokens {
                    let exact_range = TimeRange {
                        start_ns: ack_wall_ns,
                        end_ns: ack_wall_ns,
                    };
                    match catalog
                        .resolve(
                            &tenant_hash,
                            signal,
                            exact_range,
                            std::slice::from_ref(token),
                            ack_wall_ns,
                        )
                        .await
                    {
                        Ok(snapshot) => {
                            let mut pending = pending.lock().expect("pending lock");
                            for seg in snapshot.segments {
                                pending.entry(seg.data_object_key).or_insert(ack_wall_ns);
                            }
                        }
                        Err(err) => eprintln!("visibility: min-token resolve failed: {err}"),
                    }
                }
            }
            (batch_len, latency_ns, result)
        }));
    }

    let mut latencies_ns = Vec::with_capacity(handles.len());
    let mut accepted_points: u64 = 0;
    for handle in handles {
        let (batch_len, latency_ns, result) = handle.await.expect("join write task");
        latencies_ns.push(latency_ns);
        match result {
            Ok(_) => accepted_points += batch_len,
            Err(err) => eprintln!("write error: {err}"),
        }
    }
    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);

    router.flush_all().await;
    writes_done.store(true, Ordering::Release);
    poller.await.expect("join visibility poller");

    let visibility = {
        let pending = pending.lock().expect("pending lock");
        let lags_ns = lags_ns.lock().expect("lags lock");
        let unresolved_count = pending.len();
        let resolved_count = lags_ns.len();
        let avg = if lags_ns.is_empty() {
            0.0
        } else {
            lags_ns.iter().sum::<u64>() as f64 / lags_ns.len() as f64 / 1e6
        };
        let max = lags_ns.iter().max().copied().unwrap_or(0) as f64 / 1e6;
        VisibilityReport {
            resolved_count,
            unresolved_count,
            avg,
            max,
        }
    };

    let metrics = router.metrics().snapshot();
    // Scoped to this run's own tenant prefix (t/<tenant_hash_hex>/, see
    // ravel_commit::keys::data_key), not the whole bucket -- otherwise
    // pre-existing objects from other tenants/benches sharing the same
    // bucket would silently inflate bytes_written/write_amplification.
    let tenant_prefix = format!("t/{}/", tenant_hash.to_hex());
    let objects = list_all(store.as_ref(), &tenant_prefix)
        .await
        .expect("list tenant objects");
    let bytes_written: u64 = objects.iter().map(|o| o.size).sum();
    let logical_bytes = accepted_points * LOGICAL_BYTES_PER_SAMPLE;
    let write_amplification = if logical_bytes == 0 {
        0.0
    } else {
        bytes_written as f64 / logical_bytes as f64
    };
    let estimated_put_count =
        2 * (metrics.flushes_by_size + metrics.flushes_by_age + metrics.flushes_manual);

    // Query phase: wraps the same (now-populated) store in a fresh
    // GET/LIST/byte counter and a fresh Catalog/QueryEngine, then runs the
    // configured instant selector repeatedly for latency percentiles. Always
    // "hot" -- there is no segment-footer cache in `ravel-query` yet
    // (docs/benchmarking.md's cold-state variant is future work, see the
    // final report).
    let (query_store, query_counters) = CountingStore::wrap(Arc::clone(&store));
    let query_catalog = Arc::new(
        Catalog::new(
            Arc::clone(&query_store),
            CatalogConfig {
                shard_count: config.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("query catalog config"),
    );
    let engine = QueryEngine::new(query_catalog, query_store, EngineConfig::default());
    let query_t_ms = (run_start_ns + duration_ns) / 1_000_000;
    let query_now_ns = clock.now_ns();
    let query_deadline = Duration::from_secs(30);

    let mut query_latencies_ns = Vec::with_capacity(config.query_count);
    let mut query_matched_series = 0usize;
    for i in 0..config.query_count {
        let start = std::time::Instant::now();
        let value = engine
            .instant(
                tenant_hash,
                &config.query,
                query_t_ms,
                &[],
                query_now_ns,
                query_deadline,
            )
            .await
            .expect("instant query");
        query_latencies_ns.push(start.elapsed().as_nanos() as u64);
        if i == 0 {
            query_matched_series = match value {
                Value::Vector(v) => v.len(),
                _ => 0,
            };
        }
    }
    let (query_get_count, query_list_count, query_bytes_read) = (
        query_counters.get.load(Ordering::Relaxed),
        query_counters.list.load(Ordering::Relaxed),
        query_counters.bytes_read.load(Ordering::Relaxed),
    );

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            shards: config.shards,
            target_series: config.target_series,
            points_per_sec: config.points_per_sec,
            duration_secs: config.duration_secs,
            batch_size: config.batch_size,
            query: config.query.clone(),
            query_count: config.query_count,
        },
        accepted_points_per_sec: accepted_points as f64 / elapsed_secs,
        accepted_points,
        ack_latency_ms: latency_stats(latencies_ns).into(),
        flushes_by_size: metrics.flushes_by_size,
        flushes_by_age: metrics.flushes_by_age,
        flushes_manual: metrics.flushes_manual,
        put_retries: metrics.put_retries,
        abandoned_retry_exhausted: metrics.abandoned_retry_exhausted,
        abandoned_input_rejected: metrics.abandoned_input_rejected,
        acks_ok: metrics.acks_ok,
        acks_err: metrics.acks_err,
        estimated_put_count,
        bytes_written,
        logical_bytes,
        write_amplification,
        visibility_lag_ms: visibility,
        query_matched_series,
        query_latency_ms: latency_stats(query_latencies_ns).into(),
        query_get_count,
        query_list_count,
        query_bytes_read,
    }
}
