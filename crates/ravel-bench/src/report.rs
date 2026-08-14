//! Machine-readable benchmark report (epic #1053, T2). Drives one
//! ingest-then-query workload against a real or in-memory object store and
//! emits a single JSON document that carries the measured numbers *together
//! with the environment that produced them*, so a performance or cost figure
//! can be committed alongside the command and machine that measured it instead
//! of pasted from a log.
//!
//! Report-only: like the rest of `ravel-bench`, this never changes library
//! behavior, it only measures it. It reuses `ravel_bench::generator` for the
//! workload and drives `IngestRouter`/`Catalog`/`QueryEngine` directly (no
//! HTTP), the same way `ravel_bench::e2e` does.
//!
//! ## Request accounting and the object-store backend
//!
//! ADR-0075 decision 3 requires published performance and cost figures to come
//! from real object storage rather than a store with no request charges,
//! because a request-budget defect is free on the latter and billable on the
//! former. The whole workload runs through one [`CountingStore`] wrapper that
//! records every PUT, GET, and LIST plus the bytes each direction, so the
//! report always states, per operation kind, how many object-store requests
//! the workload issued -- the number the cost model is built from.
//!
//! Those counts are real call counts on any backend, `MemoryStore` included.
//! What differs is whether they are *billed*: a `MemoryStore` operation is
//! free, an S3 operation is not. The report marks this with
//! [`RequestCounts::backend_bills_requests`] rather than folding the
//! distinction away or emitting a misleading zero. A `MemoryStore` run is a
//! valid schema and correctness substrate; only a *published* number needs the
//! S3 backend, where `backend_bills_requests` is true.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{Signal, TenantId, TimeRange};
use serde::{Deserialize, Serialize};

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};

/// Bytes on the wire per logical sample: `ts_ns: i64` + `value: f64`. Same
/// constant `ingest_bench`/`s3_e2e_bench` use for write amplification.
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;
const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const VISIBILITY_POLL_MAX_ROUNDS: u32 = 30;

/// The shape of the workload driven for one report. Recorded verbatim in the
/// environment block: a latency number is meaningless without the series
/// count, rate, and batch size that produced it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadShape {
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    /// PromQL instant selector run for the query-latency numbers.
    pub query: String,
    /// Number of repeated warm queries behind the warm-latency percentiles.
    pub warm_query_count: usize,
}

/// Inputs for one report run. The store is supplied already constructed (from
/// `harness::store_from_env`) so the caller owns the `--store memory|s3`
/// choice; `store_backend`/`region`/`backend_bills_requests` describe *which
/// backend actually ran* and go straight into the environment block.
///
/// `git_commit` and `toolchain` are provenance inputs, not measured here:
/// keeping subprocess calls (`git`, `rustc`) out of the library mirrors this
/// repo's "time is injected" rule and lets the acceptance test supply fixed,
/// deterministic values. The `bench_report` bin fills them from `git
/// rev-parse` and `rustc --version`.
pub struct ReportRunConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    /// Backend that actually ran: `"memory"` or `"s3"`.
    pub store_backend: String,
    /// Region the backend ran in. Free-form and always populated: a
    /// `MemoryStore` has no region, so the bin passes a sentinel rather than
    /// an empty string, keeping the "no nulls" contract.
    pub region: String,
    /// Whether a request against this backend is billed. `false` for
    /// `MemoryStore`; `true` for S3. See the module docs.
    pub backend_bills_requests: bool,
    pub shards: u32,
    pub max_flush_delay_ms: u64,
    pub workload: WorkloadShape,
    pub ack_timeout_secs: u64,
    pub git_commit: String,
    pub toolchain: String,
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

fn latency_report(mut samples_ns: Vec<u64>) -> LatencyReport {
    samples_ns.sort_unstable();
    LatencyReport {
        p50: percentile(&samples_ns, 0.50) as f64 / 1e6,
        p95: percentile(&samples_ns, 0.95) as f64 / 1e6,
        p99: percentile(&samples_ns, 0.99) as f64 / 1e6,
        max: samples_ns.last().copied().unwrap_or(0) as f64 / 1e6,
        count: samples_ns.len(),
    }
}

/// The full report. One value per top-level concern the cost/performance model
/// needs; every leaf is populated by a run (see
/// `structured_report_populates_every_field`).
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchReport {
    pub environment: Environment,
    pub ingest: IngestSection,
    pub query: QuerySection,
    pub s3_requests: RequestCounts,
    pub bytes: BytesSection,
}

/// The provenance block. A number without this is not evidence.
#[derive(Debug, Serialize, Deserialize)]
pub struct Environment {
    /// Backend that actually ran: `"memory"` or `"s3"`. Records which store
    /// produced these numbers so a `MemoryStore` schema run is never mistaken
    /// for a published S3 measurement.
    pub store_backend: String,
    pub region: String,
    pub shard_count: u32,
    pub max_flush_delay_ms: u64,
    pub workload: WorkloadShape,
    pub git_commit: String,
    pub toolchain: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestSection {
    /// Strict-ack (`WriteMode::Strict`) latency percentiles: the wall time
    /// from submitting a batch to its durable ack. `p99` is the headline
    /// ingest number.
    pub strict_ack_latency_ms: LatencyReport,
    pub accepted_points: u64,
    pub accepted_points_per_sec: f64,
    pub write_amplification: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuerySection {
    /// First query against a freshly constructed engine and catalog: cold
    /// in-process state, nothing cached. A single sample, so every percentile
    /// equals it.
    pub cold_latency_ms: LatencyReport,
    /// Steady-state repeats of the same query on the warmed engine.
    pub warm_latency_ms: LatencyReport,
    /// Series the selector matched. A non-zero `accepted_points` with a zero
    /// match count is itself a signal something is wrong, so it stays visible.
    pub matched_series: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyReport {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: usize,
}

/// Object-store request counts for the whole workload, by operation kind --
/// the number the S3 cost model is built from.
///
/// `put`/`get`/`list` are real call counts on any backend. `backend_bills_
/// requests` states whether this backend charges for them: `false` on
/// `MemoryStore` (the counts are real but free), `true` on S3. This is the
/// explicit representation of "not a billable measurement on this backend",
/// used instead of a misleading zero -- a `MemoryStore` run still reports the
/// true call counts, it just marks them non-billable.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestCounts {
    pub backend_bills_requests: bool,
    pub put: u64,
    pub get: u64,
    pub list: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BytesSection {
    pub written: u64,
    pub read: u64,
}

/// Per-operation-kind object-store call counter, shared behind an `Arc`.
#[derive(Default)]
struct Counters {
    put: AtomicU64,
    get: AtomicU64,
    list: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
}

/// Wraps any `ObjectStoreBackend` and counts every PUT/GET/LIST and the bytes
/// each way. Distinct from `e2e`'s query-only `CountingStore` (which counts no
/// PUTs): this one wraps the *whole* workload, ingest included, because PUT
/// count is the request kind most missing today and the one the cost model
/// most needs.
struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    counters: Arc<Counters>,
}

impl CountingStore {
    fn wrap(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<dyn ObjectStoreBackend>, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
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
        self.counters.put.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
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
        // multipart: false to match the refusing default `put_multipart` this
        // wrapper inherits, exactly as `e2e::CountingStore` does.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

fn lock<'a, T>(m: &'a Mutex<T>, what: &str) -> MutexGuard<'a, T> {
    m.lock().unwrap_or_else(|_| panic!("{what} lock poisoned"))
}

/// Runs one ingest-then-query workload and returns the populated report. The
/// whole run goes through a single [`CountingStore`], so `s3_requests`/`bytes`
/// reflect the entire workload, ingest and query together.
pub async fn run(config: &ReportRunConfig) -> BenchReport {
    let (store, counters) = CountingStore::wrap(Arc::clone(&config.store));
    let w = &config.workload;

    // Unique tenant per run so consecutive runs against the same shared bucket
    // do not inflate each other's byte counts (mirrors `e2e::run`).
    let tenant = TenantId::new(format!("bench-report-{}", uuid::Uuid::new_v4()));
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;

    let ingest_config = IngestConfig {
        shard_count: config.shards,
        max_flush_delay: Duration::from_millis(config.max_flush_delay_ms),
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

    let total_points = w.points_per_sec * w.duration_secs;
    let samples_per_series = (total_points as usize / w.target_series.max(1)).max(1);
    let run_start_ns = clock.now_ns();
    let duration_ns = Duration::from_secs(w.duration_secs.max(1)).as_nanos() as i64;
    let interval_ns = (duration_ns / samples_per_series as i64).max(1);
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count: w.target_series,
        samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(w.batch_size),
        ..WorkloadConfig::default()
    };
    let batches = generate_batches(&workload).expect("generate workload");

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let pacing_interval = if w.points_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(w.batch_size as f64 / w.points_per_sec as f64)
    };

    // Visibility is polled only to gate a clean shutdown of the run; the report
    // does not surface a visibility-lag number (the e2e bin already does). The
    // poller resolves acked segments so `flush_all` below is not racing an
    // unresolved catalog.
    let pending: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let writes_done = Arc::new(AtomicBool::new(false));

    let poller = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let clock = Arc::clone(&clock);
        let pending = Arc::clone(&pending);
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
                        let mut pending = lock(&pending, "pending");
                        for seg in snapshot.segments {
                            pending.remove(&seg.data_object_key);
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
                            let mut pending = lock(&pending, "pending");
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

    let mut ack_latencies_ns = Vec::with_capacity(handles.len());
    let mut accepted_points: u64 = 0;
    for handle in handles {
        let (batch_len, latency_ns, result) = handle.await.expect("join write task");
        ack_latencies_ns.push(latency_ns);
        match result {
            Ok(_) => accepted_points += batch_len,
            Err(err) => eprintln!("write error: {err}"),
        }
    }
    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);

    router.flush_all().await;
    writes_done.store(true, Ordering::Release);
    poller.await.expect("join visibility poller");

    let logical_bytes = accepted_points * LOGICAL_BYTES_PER_SAMPLE;

    // Query phase. A fresh catalog and engine over the same (now populated)
    // wrapped store: the first query is cold in-process state, the repeats are
    // warm. Both go through the same counter, so their GETs/LISTs are part of
    // the workload request totals.
    let query_catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: config.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("query catalog config"),
    );
    let engine = QueryEngine::new(query_catalog, Arc::clone(&store), EngineConfig::default());
    let query_t_ms = (run_start_ns + duration_ns) / 1_000_000;
    let query_now_ns = clock.now_ns();
    let query_deadline = Duration::from_secs(30);

    let run_query = || async {
        let start = std::time::Instant::now();
        let value = engine
            .instant(
                tenant_hash,
                &w.query,
                query_t_ms,
                &[],
                query_now_ns,
                query_deadline,
            )
            .await
            .expect("instant query");
        (start.elapsed().as_nanos() as u64, value)
    };

    let (cold_ns, cold_value) = run_query().await;
    let matched_series = match cold_value {
        Value::Vector(v) => v.len(),
        _ => 0,
    };

    let mut warm_ns = Vec::with_capacity(w.warm_query_count);
    for _ in 0..w.warm_query_count {
        let (ns, _) = run_query().await;
        warm_ns.push(ns);
    }

    let put = counters.put.load(Ordering::Relaxed);
    let get = counters.get.load(Ordering::Relaxed);
    let list = counters.list.load(Ordering::Relaxed);
    let bytes_written = counters.bytes_written.load(Ordering::Relaxed);
    let bytes_read = counters.bytes_read.load(Ordering::Relaxed);
    let write_amplification = if logical_bytes == 0 {
        0.0
    } else {
        bytes_written as f64 / logical_bytes as f64
    };

    BenchReport {
        environment: Environment {
            store_backend: config.store_backend.clone(),
            region: config.region.clone(),
            shard_count: config.shards,
            max_flush_delay_ms: config.max_flush_delay_ms,
            workload: w.clone(),
            git_commit: config.git_commit.clone(),
            toolchain: config.toolchain.clone(),
        },
        ingest: IngestSection {
            strict_ack_latency_ms: latency_report(ack_latencies_ns),
            accepted_points,
            accepted_points_per_sec: accepted_points as f64 / elapsed_secs,
            write_amplification,
        },
        query: QuerySection {
            cold_latency_ms: latency_report(vec![cold_ns]),
            warm_latency_ms: latency_report(warm_ns),
            matched_series,
        },
        s3_requests: RequestCounts {
            backend_bills_requests: config.backend_bills_requests,
            put,
            get,
            list,
        },
        bytes: BytesSection {
            written: bytes_written,
            read: bytes_read,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    fn memory_config() -> ReportRunConfig {
        ReportRunConfig {
            store: Arc::new(MemoryStore::new()),
            store_backend: "memory".to_string(),
            region: "n/a-memory".to_string(),
            backend_bills_requests: false,
            shards: 2,
            max_flush_delay_ms: 500,
            workload: WorkloadShape {
                target_series: 20,
                points_per_sec: 4_000,
                duration_secs: 1,
                batch_size: 50,
                query: "bench_gauge".to_string(),
                warm_query_count: 5,
            },
            ack_timeout_secs: 5,
            git_commit: "0000000000000000000000000000000000000000".to_string(),
            toolchain: "rustc 0.0.0-test".to_string(),
        }
    }

    fn assert_latency_populated(l: &LatencyReport, what: &str) {
        assert!(l.count > 0, "{what}: latency count must be > 0");
        assert!(l.p50 > 0.0, "{what}: p50 must be > 0");
        assert!(l.p95 > 0.0, "{what}: p95 must be > 0");
        assert!(l.p99 > 0.0, "{what}: p99 must be > 0");
        assert!(l.max > 0.0, "{what}: max must be > 0");
    }

    /// Acceptance test (epic #1053 T2): a run against `MemoryStore` populates
    /// every field of the schema -- no nulls (guaranteed structurally: no
    /// `Option` in `BenchReport`), and no zero standing in for "not measured".
    ///
    /// Request counts are the field that "cannot be measured as a billable S3
    /// cost" on `MemoryStore`. The schema represents that explicitly with
    /// `backend_bills_requests`, asserted `false` here, while still reporting
    /// the true (non-zero) call counts -- not a misleading zero.
    #[tokio::test]
    async fn structured_report_populates_every_field() {
        let report = run(&memory_config()).await;

        // Environment: every provenance field non-empty / non-zero.
        let env = &report.environment;
        assert_eq!(env.store_backend, "memory");
        assert!(!env.region.is_empty(), "region must be populated");
        assert!(env.shard_count > 0, "shard_count must be > 0");
        assert!(env.max_flush_delay_ms > 0, "max_flush_delay_ms must be > 0");
        assert!(!env.git_commit.is_empty(), "git_commit must be populated");
        assert!(!env.toolchain.is_empty(), "toolchain must be populated");
        assert!(env.workload.target_series > 0);
        assert!(env.workload.points_per_sec > 0);
        assert!(env.workload.duration_secs > 0);
        assert!(env.workload.batch_size > 0);
        assert!(!env.workload.query.is_empty());
        assert!(env.workload.warm_query_count > 0);

        // Ingest.
        assert!(
            report.ingest.accepted_points > 0,
            "must ingest a non-zero point count"
        );
        assert!(report.ingest.accepted_points_per_sec > 0.0);
        assert!(
            report.ingest.write_amplification > 0.0,
            "write amplification must be > 0"
        );
        assert_latency_populated(&report.ingest.strict_ack_latency_ms, "strict_ack");

        // Query: cold and warm both measured, and the workload was actually
        // queryable (a zero match count with non-zero ingest would be a bug).
        assert!(
            report.query.matched_series > 0,
            "query must match at least one ingested series"
        );
        assert_latency_populated(&report.query.cold_latency_ms, "cold_query");
        assert_latency_populated(&report.query.warm_latency_ms, "warm_query");

        // Request counts: real, non-zero call counts for every operation kind,
        // plus the explicit not-billable marker for MemoryStore.
        assert!(
            !report.s3_requests.backend_bills_requests,
            "MemoryStore requests are free: backend_bills_requests must be false, the explicit \
             representation of a non-billable count instead of a misleading zero"
        );
        assert!(report.s3_requests.put > 0, "PUT count must be > 0");
        assert!(report.s3_requests.get > 0, "GET count must be > 0");
        assert!(report.s3_requests.list > 0, "LIST count must be > 0");

        // Bytes both directions.
        assert!(report.bytes.written > 0, "bytes written must be > 0");
        assert!(report.bytes.read > 0, "bytes read must be > 0");

        // The whole thing serializes to JSON with no null anywhere.
        let json = serde_json::to_value(&report).expect("serialize report");
        assert!(
            !json_has_null(&json),
            "report JSON must contain no null: {json}"
        );
    }

    fn json_has_null(v: &serde_json::Value) -> bool {
        match v {
            serde_json::Value::Null => true,
            serde_json::Value::Array(a) => a.iter().any(json_has_null),
            serde_json::Value::Object(o) => o.values().any(json_has_null),
            _ => false,
        }
    }
}
