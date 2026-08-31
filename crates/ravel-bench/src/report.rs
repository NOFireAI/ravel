//! Machine-readable benchmark report. Drives one ingest-then-query workload
//! against a real or in-memory object store and emits a single JSON document
//! that carries the measured numbers *together with the environment that
//! produced them*, so a performance or cost figure can be committed alongside
//! the command and machine that measured it instead of pasted from a log.
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
//! former. The whole workload runs through one
//! [`InstrumentedStore`](ravel_object_store::InstrumentedStore) that records
//! every PUT, GET, and LIST plus the bytes each direction, so the report always
//! states, per operation kind, how many object-store requests the workload
//! issued -- the number the cost model is built from. This is the same counter
//! the server scrapes (ADR-0104 decision 5): the bench reporter and production
//! read one implementation, so a count here cannot drift from a count there.
//!
//! Those counts are real call counts on any backend, `MemoryStore` included.
//! What differs is whether they are *billed*: a `MemoryStore` operation is
//! free, an S3 operation is not. `InstrumentedStore` counts calls but is
//! backend-agnostic about billing, so the report carries that distinction
//! itself in [`RequestCounts::backend_bills_requests`] rather than folding it
//! away or emitting a misleading zero. A `MemoryStore` run is a valid schema
//! and correctness substrate; only a *published* number needs the S3 backend,
//! where `backend_bills_requests` is true.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetricsSnapshot};
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
    /// The `StoreMetrics` handle the store's own HTTP connector records billed
    /// attempts into, when the backend has one (`S3Store::with_metrics`).
    /// `None` for a store with no attempt source (`MemoryStore`); the report
    /// then renders every attempt figure as absent, never zero. Sharing THIS
    /// handle with the run's `InstrumentedStore` is what makes
    /// `calls <= attempts` a property the reconciliation may assert
    /// (`instrument.rs`: the relation "is a property of the wiring, not a
    /// guarantee of this type").
    pub store_metrics: Option<Arc<ravel_object_store::StoreMetrics>>,
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
/// Two figures per op, both read from the SAME [`StoreMetricsSnapshot`]:
///
/// - `put`/`get`/`list` are completed *call* counts (one per logical
///   operation the workload issued), real on any backend.
/// - `put_attempts`/`get_attempts`/`list_attempts` are billed *HTTP attempts*
///   (issue #928, ADR-0927 decision 8): retries and range fan-out included, so
///   one logical GET that retried nine times bills ten attempts. This is the
///   figure S3 charges on, and the headline request number the ledger reads
///   (ADR-0996 decision 3).
///
/// `calls` stays beside `attempts` as the diagnostic: the reconciliation
/// `calls <= attempts` holds per op on a backend that records attempts, and
/// `attempts - calls` (the `*_retry_overhead` fields) is the retry (billed)
/// overhead a call count alone hides. A non-HTTP backend ([`MemoryStore`] via
/// `run`) records no attempts, leaving the attempt figures at zero; that is
/// honest, not a billing measurement, and `backend_bills_requests` marks it.
///
/// `backend_bills_requests` states whether this backend charges for requests:
/// `false` on `MemoryStore` (the counts are real but free), `true` on S3. This
/// is the explicit representation of "not a billable measurement on this
/// backend", used instead of a misleading zero.
///
/// [`MemoryStore`]: ravel_object_store::memory::MemoryStore
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestCounts {
    pub backend_bills_requests: bool,
    /// Completed PUT calls (one per logical PUT).
    pub put: u64,
    /// Completed GET calls (one per logical GET).
    pub get: u64,
    /// Completed LIST-family calls: paged `list` plus `list_delimited`, which
    /// S3 bills as one LIST each.
    pub list: u64,
    /// Billed HTTP attempts for PUT: retries and multipart part requests
    /// included, per ADR-0927 decision 8. `>= put` when present. `None` when
    /// the store has no attempt source (no HTTP connector wired a shared
    /// `StoreMetrics` handle): absence is NOT zero, and rendering it as zero
    /// would be the flattering figure ADR-0104's billing flag exists to
    /// prevent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub put_attempts: Option<u64>,
    /// Billed HTTP attempts for GET: retries and range fan-out included, per
    /// ADR-0927 decision 8 (one logical GET split into N ranged requests, or
    /// retried, bills N attempts). `>= get` when present; `None` when no
    /// attempt source exists (see `put_attempts`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_attempts: Option<u64>,
    /// Billed HTTP attempts for the LIST family (`list` + `list_delimited`):
    /// retries and continuation-page requests included, per ADR-0927 decision
    /// 8. `>= list` when present; `None` when no attempt source exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_attempts: Option<u64>,
    /// Retry (billed) overhead for PUT: `put_attempts - put`. The billed
    /// requests a completed-call count hides (ADR-0996 decision 3). `None`
    /// exactly when `put_attempts` is `None`: an unmeasured overhead is not a
    /// zero overhead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub put_retry_overhead: Option<u64>,
    /// Retry (billed) overhead for GET: `get_attempts - get`; `None` when
    /// unmeasured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_retry_overhead: Option<u64>,
    /// Retry (billed) overhead for the LIST family: `list_attempts - list`;
    /// `None` when unmeasured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_retry_overhead: Option<u64>,
}

impl RequestCounts {
    /// Build the request counts from an [`InstrumentedStore`] snapshot, pairing
    /// them with the caller-supplied billing flag.
    ///
    /// Both figures per op come from one snapshot: the completed-call counters
    /// (`put`/`get`, and `list` = paged LIST plus `list_delimited`) and the
    /// billed-attempt counters beside them (`attempts` = every HTTP request the
    /// op issued, retries and range fan-out included, ADR-0927 decision 8). The
    /// LIST attempt figure sums `list` and `list_delimited` the same way the
    /// call figure does, since S3 bills both as one LIST.
    ///
    /// Billing is not something the counter knows -- a call count is identical
    /// on `MemoryStore` and S3, only its price differs -- so
    /// `backend_bills_requests` is passed in by whoever chose the backend,
    /// `false` for `MemoryStore` and `true` for S3.
    ///
    /// `attempts_wired` says whether the underlying store records billed
    /// attempts into the SAME `StoreMetrics` handle this snapshot came from
    /// (an `S3Store::with_metrics` sharing the `InstrumentedStore`'s handle).
    /// It is a property of the WIRING, not of billing: `instrument.rs` states
    /// `attempts >= calls` "holds exactly when every store this decorator
    /// counts a `calls` on records its attempts into the same handle". Gating
    /// on the billing flag instead panics precisely on the real S3 path when
    /// the wiring is absent, after the paid workload already ran.
    ///
    /// When wired, the ADR-0996 decision-3 reconciliation `calls <= attempts`
    /// is asserted per op. When not wired, every attempt and overhead figure
    /// is `None`: absence is not zero, and a zero here would be the
    /// flattering figure ADR-0104 forbids.
    fn from_metrics(
        snapshot: &StoreMetricsSnapshot,
        backend_bills_requests: bool,
        attempts_wired: bool,
    ) -> Self {
        let put = snapshot.put.calls;
        let get = snapshot.get.calls;
        let list = snapshot.list_calls();
        let (put_attempts, get_attempts, list_attempts) = if attempts_wired {
            (
                Some(snapshot.put.attempts),
                Some(snapshot.get.attempts),
                // LIST family: sum both blocks, mirroring `list_calls`.
                Some(snapshot.list.attempts + snapshot.list_delimited.attempts),
            )
        } else {
            (None, None, None)
        };

        let counts = RequestCounts {
            backend_bills_requests,
            put,
            get,
            list,
            put_attempts,
            get_attempts,
            list_attempts,
            put_retry_overhead: put_attempts.map(|a| a.saturating_sub(put)),
            get_retry_overhead: get_attempts.map(|a| a.saturating_sub(get)),
            list_retry_overhead: list_attempts.map(|a| a.saturating_sub(list)),
        };
        if attempts_wired {
            counts.assert_calls_le_attempts();
        }
        counts
    }

    /// The ADR-0996 decision-3 reconciliation: billed `attempts` never
    /// undercount completed `calls`, so `calls <= attempts` holds for every op
    /// on a backend that records attempts (the wiring at
    /// `instrument.rs:44-52`). Panics naming the first op that violates it.
    /// Call only on a snapshot whose attempts are wired (a request-billing
    /// backend, or a fixture that drove `StoreMetrics::record_attempt`); a
    /// non-HTTP backend records no attempts and would fail this vacuously.
    fn assert_calls_le_attempts(&self) {
        for (name, calls, attempts) in [
            ("put", self.put, self.put_attempts),
            ("get", self.get, self.get_attempts),
            ("list", self.list, self.list_attempts),
        ] {
            let Some(attempts) = attempts else {
                // Only reachable if a caller asserts on an unwired snapshot;
                // an absent figure reconciles vacuously rather than as zero.
                continue;
            };
            assert!(
                calls <= attempts,
                "reconciliation: {name} calls {calls} exceed billed attempts {attempts}",
            );
        }
    }
}

/// Bytes moved over the object-store wire, by direction. Every figure names
/// its byte kind and whether retries are included (ADR-0996 decision 3: a byte
/// column is decoration without its kind, and two kinds are never summed).
///
/// Both are WIRE bytes as transferred, in the on-object (stored/compressed)
/// form, counted at the completion decorator that sits ABOVE the S3 retry
/// connector, so below-decorator HTTP retries are NOT included in either
/// figure -- unlike the `*_attempts` request counts, which are. A retry moves
/// bytes the object-store adapter re-reads; those are billed as an attempt but
/// do not re-enter `written`/`read` here.
#[derive(Debug, Serialize, Deserialize)]
pub struct BytesSection {
    /// PUT payload bytes as offered to the backend (wire, stored form). Every
    /// logical PUT call is counted once, failures included, since the payload
    /// was offered whether or not the backend accepted it. Retries not
    /// included.
    pub written: u64,
    /// GET bytes actually returned by completed calls (wire, stored form; a
    /// ranged read counts the range, not the whole object). A failed GET adds
    /// zero. Retries not included.
    pub read: u64,
}

fn lock<'a, T>(m: &'a Mutex<T>, what: &str) -> MutexGuard<'a, T> {
    m.lock().unwrap_or_else(|_| panic!("{what} lock poisoned"))
}

/// Runs one ingest-then-query workload and returns the populated report. The
/// whole run goes through a single
/// [`InstrumentedStore`](ravel_object_store::InstrumentedStore), so
/// `s3_requests`/`bytes` reflect the entire workload, ingest and query
/// together.
pub async fn run(config: &ReportRunConfig) -> BenchReport {
    let instrumented = Arc::new(match &config.store_metrics {
        // The store's connector already records attempts into this handle;
        // the decorator records its calls into the SAME block, which is the
        // wiring the calls <= attempts reconciliation is defined over.
        Some(handle) => {
            InstrumentedStore::with_metrics(Arc::clone(&config.store), Arc::clone(handle))
        }
        None => InstrumentedStore::new(Arc::clone(&config.store)),
    });
    let metrics = instrumented.metrics();
    let attempts_wired = config.store_metrics.is_some();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
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
        let (value, _coverage) = engine
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

    let snapshot = metrics.snapshot();
    let request_counts =
        RequestCounts::from_metrics(&snapshot, config.backend_bills_requests, attempts_wired);
    let bytes_written = snapshot.put.bytes;
    let bytes_read = snapshot.get.bytes;
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
        s3_requests: request_counts,
        bytes: BytesSection {
            written: bytes_written,
            read: bytes_read,
        },
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions, StoreOp};

    use super::*;

    /// Acceptance test (ADR-0104 decision 5, #507): the reporter now reads
    /// object-store request counts from `InstrumentedStore`, and the migration
    /// must preserve two properties the deleted `CountingStore` guaranteed.
    ///
    /// 1. The counts are exact. A fixed workload of 3 PUTs, 2 GETs, 1 `list`
    ///    and 1 `list_delimited` is driven through the same
    ///    `InstrumentedStore` wrapping `run` uses, and the derived
    ///    [`RequestCounts`] are asserted to the exact call count -- not `> 0`,
    ///    which would pass just as well against an accounting bug that reported
    ///    a fraction of the truth. `list` folds `list_delimited` in because S3
    ///    bills both as one LIST.
    /// 2. `backend_bills_requests` is carried, not dropped: `false` for a
    ///    `MemoryStore` run (real counts, but free), `true` for a
    ///    request-billing backend. The counter itself is backend-agnostic, so
    ///    the flag rides beside the counts rather than being inferred from a
    ///    zero.
    #[tokio::test]
    async fn instrumented_store_counts_match_and_preserve_billing_flag() {
        let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let metrics = store.metrics();

        // Fixed workload with known, exact counts.
        for (key, body) in [
            ("a", &b"aa"[..]),
            ("b", &b"bbbb"[..]),
            ("c", &b"cccccc"[..]),
        ] {
            store
                .put(key, Bytes::from_static(body), PutOptions::default())
                .await
                .expect("put");
        }
        for key in ["a", "b"] {
            store.get(key, GetRange::Full).await.expect("get");
        }
        store.list("", None).await.expect("list");
        store.list_delimited("").await.expect("list_delimited");

        // Fixture 1: attempts == calls, no retries. `MemoryStore` records no
        // attempts on its own (it bills nothing), so drive
        // `StoreMetrics::record_attempt` once per completed call -- exactly the
        // shape the S3 counting connector produces when nothing retries or
        // fans out: one billed HTTP request per logical op.
        for _ in 0..3 {
            metrics.record_attempt(StoreOp::Put);
        }
        for _ in 0..2 {
            metrics.record_attempt(StoreOp::Get);
        }
        metrics.record_attempt(StoreOp::List);
        metrics.record_attempt(StoreOp::ListDelimited);

        let snapshot = metrics.snapshot();

        // MemoryStore run: real counts, not billed.
        let memory_counts = RequestCounts::from_metrics(&snapshot, false, false);
        assert_eq!(memory_counts.put, 3, "exact PUT count");
        assert_eq!(memory_counts.get, 2, "exact GET count");
        assert_eq!(
            memory_counts.list, 2,
            "exact LIST count folds list + list_delimited"
        );
        assert_eq!(
            memory_counts.put_attempts, None,
            "no attempt source: absent, never zero"
        );
        assert_eq!(memory_counts.get_attempts, None, "absent, never zero");
        assert_eq!(
            memory_counts.get_retry_overhead, None,
            "unmeasured overhead is not zero"
        );
        assert!(
            !memory_counts.backend_bills_requests,
            "MemoryStore requests are free: backend_bills_requests must be false, the explicit \
             representation of a non-billable count instead of a misleading zero"
        );

        // Bytes track the offered/returned payloads, the same fields `run`
        // reports as bytes_written/bytes_read.
        assert_eq!(
            snapshot.put.bytes,
            2 + 4 + 6,
            "bytes written = PUT payloads"
        );
        assert_eq!(snapshot.get.bytes, 2 + 4, "bytes read = GET a + b");

        // Request-billing backend: same call counts, but now billed and the
        // reconciliation runs. Only the flag moves on the call figures; the
        // attempt figures equal the calls exactly (no retries), so every retry
        // overhead is exactly zero.
        let billed_counts = RequestCounts::from_metrics(&snapshot, true, true);
        assert!(
            billed_counts.backend_bills_requests,
            "a request-billing backend must report backend_bills_requests true"
        );
        assert_eq!(
            billed_counts.put, memory_counts.put,
            "counts are price-blind"
        );
        assert_eq!(
            billed_counts.get, memory_counts.get,
            "counts are price-blind"
        );
        assert_eq!(
            billed_counts.list, memory_counts.list,
            "counts are price-blind"
        );

        // Attempts pinned exactly, and exactly equal to calls: no retries.
        assert_eq!(billed_counts.put_attempts, Some(3), "exact PUT attempts");
        assert_eq!(billed_counts.get_attempts, Some(2), "exact GET attempts");
        assert_eq!(
            billed_counts.list_attempts,
            Some(2),
            "exact LIST attempts folds list + list_delimited"
        );
        assert_eq!(billed_counts.put_retry_overhead, Some(0), "no PUT retries");
        assert_eq!(billed_counts.get_retry_overhead, Some(0), "no GET retries");
        assert_eq!(
            billed_counts.list_retry_overhead,
            Some(0),
            "no LIST retries"
        );
    }

    /// Fixture 2 (ADR-0996 decision 3): a snapshot where billed attempts exceed
    /// completed calls, driving `StoreMetrics::record_attempt` to simulate
    /// retries and range fan-out below the completion decorator. Both figures
    /// are pinned exactly, and the retry overhead is `attempts - calls` to the
    /// unit -- never `> 0`, which would pass against an off-by-a-multiple
    /// accounting bug.
    #[tokio::test]
    async fn attempts_exceed_calls_pins_retry_overhead() {
        let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let metrics = store.metrics();

        // Two logical GETs, three logical PUTs, one paged LIST.
        for (key, body) in [
            ("a", &b"aa"[..]),
            ("b", &b"bbbb"[..]),
            ("c", &b"cccccc"[..]),
        ] {
            store
                .put(key, Bytes::from_static(body), PutOptions::default())
                .await
                .expect("put");
        }
        for key in ["a", "b"] {
            store.get(key, GetRange::Full).await.expect("get");
        }
        store.list("", None).await.expect("list");

        // Billed attempts below the decorator: the first GET fanned out into 4
        // ranged requests, the second retried once (2 attempts); one PUT
        // retried twice (3 + 1 + 1 = 5 total across three PUTs); the LIST
        // needed one continuation page (2 attempts).
        for _ in 0..(4 + 2) {
            metrics.record_attempt(StoreOp::Get);
        }
        for _ in 0..(3 + 2) {
            metrics.record_attempt(StoreOp::Put);
        }
        for _ in 0..2 {
            metrics.record_attempt(StoreOp::List);
        }

        let snapshot = metrics.snapshot();
        let counts = RequestCounts::from_metrics(&snapshot, true, true);

        // Calls: unchanged logical counts.
        assert_eq!(counts.get, 2, "exact GET calls");
        assert_eq!(counts.put, 3, "exact PUT calls");
        assert_eq!(counts.list, 1, "exact LIST calls");

        // Attempts: strictly above calls, pinned exactly.
        assert_eq!(counts.get_attempts, Some(6), "exact GET attempts");
        assert_eq!(counts.put_attempts, Some(5), "exact PUT attempts");
        assert_eq!(counts.list_attempts, Some(2), "exact LIST attempts");

        // Retry overhead = attempts - calls, exact.
        assert_eq!(counts.get_retry_overhead, Some(4), "GET overhead = 6 - 2");
        assert_eq!(counts.put_retry_overhead, Some(2), "PUT overhead = 5 - 3");
        assert_eq!(counts.list_retry_overhead, Some(1), "LIST overhead = 2 - 1");
    }

    /// The reconciliation is a real assertion, not a comment: build a
    /// `RequestCounts` with the operands swapped (calls above attempts, the
    /// relation `calls <= attempts` inverted) and confirm the check panics.
    /// A snapshot that genuinely satisfied `calls <= attempts` would pass this
    /// same code, so the demonstration proves the guard fires.
    #[test]
    fn reconciliation_panics_when_calls_exceed_attempts() {
        // calls > attempts on GET: attempts undercount calls, which the wiring
        // (instrument.rs:44-52) forbids on a request-billing backend.
        let swapped = RequestCounts {
            backend_bills_requests: true,
            put: 3,
            get: 10,
            list: 2,
            put_attempts: Some(3),
            get_attempts: Some(3),
            list_attempts: Some(2),
            put_retry_overhead: Some(0),
            get_retry_overhead: Some(0),
            list_retry_overhead: Some(0),
        };
        let panicked = std::panic::catch_unwind(|| swapped.assert_calls_le_attempts()).is_err();
        assert!(
            panicked,
            "reconciliation must panic when calls exceed attempts"
        );

        // The un-swapped orientation (calls <= attempts) does not panic.
        let ok = RequestCounts {
            get: 3,
            get_attempts: Some(10),
            get_retry_overhead: Some(7),
            ..swapped
        };
        ok.assert_calls_le_attempts();
    }

    fn memory_config() -> ReportRunConfig {
        ReportRunConfig {
            store: Arc::new(MemoryStore::new()),
            store_metrics: None,
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

    /// Acceptance test: a run against `MemoryStore` populates every field of
    /// the schema -- no nulls (guaranteed structurally: no `Option` in
    /// `BenchReport`), and no zero standing in for "not measured".
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

        // Attempts: MemoryStore has no attempt source, so every attempt and
        // overhead figure is ABSENT -- never a zero standing in for "not
        // measured" (the flattering-zero ADR-0104 forbids). The fields are
        // skipped in JSON entirely, which is what keeps the no-null contract.
        assert_eq!(
            report.s3_requests.put_attempts, None,
            "MemoryStore has no PUT attempt source: absent, not zero"
        );
        assert_eq!(report.s3_requests.get_attempts, None, "absent, not zero");
        assert_eq!(report.s3_requests.list_attempts, None, "absent, not zero");
        assert_eq!(report.s3_requests.put_retry_overhead, None);
        assert_eq!(report.s3_requests.get_retry_overhead, None);
        assert_eq!(report.s3_requests.list_retry_overhead, None);

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
