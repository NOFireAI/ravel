//! Group-by aggregation core-count scaling benchmark core (ADR-0102 decision
//! 4).
//!
//! One fixed group-by query over one fixed cardinality, swept across two axes:
//! `target_partitions` (the knob a running server sets from core count) and
//! `SqlConfig::parallel_final_aggregation` (ADR-0094's exact-typed final
//! aggregation, off by default). The deliverable is throughput and latency at
//! every (partitions x flag) combination, so the epic can see whether the
//! parallel final aggregation actually scales with cores before flipping its
//! default. This is a different instrument from `sql_corpus`/#428's
//! `sql_latency_bench` (per-query latency across a diverse SQL corpus): this
//! one holds the query fixed and sweeps the parallelism axis.
//!
//! `target_partitions` is set through `EngineConfig::fetch_concurrency`, which
//! `ravel_sql::session::session_config` feeds straight into DataFusion's
//! `with_target_partitions`. The dataset is deliberately multi-part (one RSEG
//! object per part, disjoint series): today's scan partitioning is
//! segment-granular (`min(target_partitions, segment_count)`, ADR-0102
//! decision 1 deferred), so a single-segment tenant would pin every scan to
//! one partition regardless of the axis. A multi-part tenant lets the scan
//! fan out up to the part count, which is what "scaling versus core count"
//! needs to be visible at all.
//!
//! The fixed query groups by `series_id` (a `FixedSizeBinary(16)`, non-float)
//! with `count(*)`/`min(ts)`/`max(ts)` aggregates (all exact over non-float
//! inputs), so it is provably exact-typed under ADR-0094's classification:
//! `parallel_final_aggregation` genuinely engages when on rather than being
//! silently disqualified. Group cardinality equals the distinct series count,
//! which the config controls directly.
//!
//! Lives in the lib (not just the `groupby_scaling_bench` bin), mirroring
//! `query_latency.rs`, so `tests/groupby_scaling_smoke.rs` exercises the same
//! path the bin runs. Report-only: never changes library behavior, only
//! measures it. Gated on the `sql-latency` feature (shared with `sql_corpus`),
//! so the default build never compiles ravel-sql/datafusion or this module.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{ByteLimit, EngineConfig, LogSegmentFetcher, RequestLimit, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor, SqlRequest};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde::Serialize;
use uuid::Uuid;

use crate::generator::{CardinalityProfile, WorkloadConfig, generate_raw};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Frozen query clock. Small on purpose, exactly as `flight_sql_egress` fixes
/// it: `Catalog::resolve` issues one LIST per (shard, ingest-hour), so a
/// wall-clock value would fan out to hundreds of thousands of LISTs. Every
/// published part lives in ingest-hour bucket 0 with event timestamps a few
/// microseconds after the epoch, well inside `[0, NOW_NS]`.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// The one query the sweep measures. Group by `series_id` (non-float key) with
/// `count(*)`/`min(ts)`/`max(ts)` (all exact over non-float inputs) so the
/// query is exact-typed under ADR-0094 and `parallel_final_aggregation`
/// actually engages when enabled.
pub const QUERY: &str = "SELECT series_id, count(*) AS rows, min(ts) AS first_ts, max(ts) AS last_ts \
     FROM samples GROUP BY series_id";

/// Default per-tenant memory ceiling passed to `SqlExecutor::new`. Matches the
/// value the bench used before it was a flag, so an invocation that does not
/// pass `--max-tenant-bytes` is unchanged. Exposed as a flag because the whole
/// point of this bench is measuring at production scale, where the default is
/// far too small: with the disk manager disabled (ADR-0102 decision 3) an
/// aggregation over this ceiling returns `SqlError::ResourcesExhausted` rather
/// than spilling, so a real sweep must be able to raise it.
pub const DEFAULT_MAX_TENANT_BYTES: usize = 1 << 30;

/// Default per-query wall deadline. Matches the value the bench used before it
/// was a flag; a large production sweep can raise it with `--deadline-secs`.
pub const DEFAULT_DEADLINE_SECS: u64 = 30;

/// Inputs for one scaling run.
pub struct GroupbyScalingConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// Number of RSEG objects the tenant is split across. Should be at least
    /// the largest `target_partitions` value for the scan to fan out that far
    /// (segment-granular partitioning, decision 1 deferred).
    pub parts: usize,
    /// Total distinct series across all parts. Equals the group cardinality of
    /// the fixed group-by query.
    pub series: usize,
    /// Samples per series. Total scanned rows are `series * samples_per_series`.
    pub samples_per_series: usize,
    /// The swept parallelism axis: DataFusion `target_partitions` values, each
    /// run once with the flag off and once on.
    pub target_partitions: Vec<usize>,
    /// Timed repetitions per (partitions x flag) combination. Latency
    /// min/median/max and the dispersion measure are taken across these; the
    /// fixture is built once up front and reused for every combination.
    ///
    /// At the bin's default of 3 this is a thin sample: it cannot support a
    /// claim like ADR-0094's ~14% parallel-vs-serial regression, whose signal
    /// is smaller than the run-to-run noise of three iterations. A production
    /// invocation that means to publish such a number should raise `--runs`
    /// well above the default (tens of runs) so the stddev this bench records
    /// is meaningful against the effect size.
    pub runs: usize,
    /// Per-tenant memory ceiling handed to `SqlExecutor::new`. See
    /// [`DEFAULT_MAX_TENANT_BYTES`].
    pub max_tenant_bytes: usize,
    /// Per-query wall deadline. See [`DEFAULT_DEADLINE_SECS`].
    pub deadline: Duration,
}

impl GroupbyScalingConfig {
    /// A cheap fixture for the acceptance test and CI: a handful of parts, a
    /// small dataset, two partition values, two runs. Fast enough to run in a
    /// unit-test harness while still exercising both axes end to end.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        GroupbyScalingConfig {
            store,
            store_label: store_label.to_string(),
            parts: 4,
            series: 40,
            samples_per_series: 20,
            target_partitions: vec![1, 2],
            runs: 2,
            max_tenant_bytes: DEFAULT_MAX_TENANT_BYTES,
            deadline: Duration::from_secs(DEFAULT_DEADLINE_SECS),
        }
    }
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub query: String,
    pub parts: usize,
    pub series: usize,
    pub samples_per_series: usize,
    pub total_samples: u64,
    /// Distinct groups the query actually returned (equals the distinct
    /// `series_id` count the dataset published, read from the query result, not
    /// `series` echoed back). A generation bug that collapses cardinality shows
    /// up here as `groups != series` rather than being folded away.
    pub groups: usize,
    pub target_partitions: Vec<usize>,
    pub runs: usize,
    /// Logical cores available to the process. A "scaling versus cores" report
    /// is uninterpretable without it.
    pub cores: usize,
    /// Build profile the measurement ran under, from `debug_assertions`
    /// (`"debug"` when on, `"release"` when off). A debug build's numbers are
    /// not comparable to a release build's.
    pub profile: String,
}

/// One (target_partitions x parallel_final_aggregation) measurement.
///
/// Every field is either the requested axis value or a fact OBSERVED from the
/// real plan/execution of this combination. `fanned_out`, `scan_partitions`,
/// and `runs_taken` in particular are read back from what actually happened,
/// not echoed from the request, so the report can prove each swept axis reached
/// execution instead of merely restating the config.
#[derive(Serialize)]
pub struct ComboResult {
    pub target_partitions: usize,
    pub parallel_final_aggregation: bool,
    /// Observed: did the physical plan fan its final aggregation across
    /// partitions? Read from the real plan text via the ADR-0094
    /// `partitioning=Hash(` marker (see [`fans_out_final_aggregation`]), so a
    /// flag that never reaches the executor shows up as `fanned_out == false`
    /// even with `parallel_final_aggregation == true`.
    pub fanned_out: bool,
    /// Observed: the RSEG scan node's actual output partition count in the real
    /// physical plan (`min(target_partitions, segment_count)` under today's
    /// segment-granular partitioning), read from the plan's properties rather
    /// than recomputed from the config, so a `target_partitions` that never
    /// reaches the scan is visible as a disagreement.
    pub scan_partitions: usize,
    /// Segments the successful attempt actually scanned, from `SqlStats`. With
    /// today's segment-granular partitioning this bounds the scan fan-out.
    pub segments_scanned: usize,
    /// Result rows (groups) the query returned; identical across combinations
    /// for the same dataset, kept per-combo as a correctness check.
    pub result_rows: usize,
    /// Observed: timed iterations actually performed, not `config.runs` echoed
    /// back. Equals the requested run count on success.
    pub runs_taken: usize,
    pub min_ms: f64,
    pub median_ms: f64,
    pub max_ms: f64,
    /// Sample standard deviation of the timed runs (ms). Zero when fewer than
    /// two samples were taken. The dispersion `min`/`median`/`max` alone cannot
    /// give: without it, a claimed parallel-vs-serial delta cannot be told from
    /// run-to-run noise.
    pub stddev_ms: f64,
    /// The full sorted per-run latency sample (ms), so a consumer can compute
    /// its own dispersion or confidence interval. The human table stays
    /// summary-only; this rides in the JSON report.
    pub samples_ms: Vec<f64>,
    /// Scanned input rows per second, computed from `total_samples` and the
    /// median latency. The throughput axis the ADR asks for.
    pub rows_per_sec: f64,
    /// Set when this ONE combination failed (e.g. a typed
    /// `SqlError::ResourcesExhausted` from an over-budget aggregation) rather
    /// than being measured. When present the timing fields are zero and the
    /// sweep continued past it instead of panicking the whole run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ComboResult {
    /// A labeled failure slot for one combination: the axes and whatever the
    /// plan inspection already observed, with zeroed timing and the error
    /// message. Lets the sweep record the failure and keep going.
    fn failed(
        target_partitions: usize,
        parallel: bool,
        fanned_out: bool,
        scan_partitions: usize,
        error: String,
    ) -> Self {
        ComboResult {
            target_partitions,
            parallel_final_aggregation: parallel,
            fanned_out,
            scan_partitions,
            segments_scanned: 0,
            result_rows: 0,
            runs_taken: 0,
            min_ms: 0.0,
            median_ms: 0.0,
            max_ms: 0.0,
            stddev_ms: 0.0,
            samples_ms: Vec::new(),
            rows_per_sec: 0.0,
            error: Some(error),
        }
    }
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub combos: Vec<ComboResult>,
}

/// Nearest-rank percentile over an already-sorted slice, matching
/// `query_latency.rs`'s convention so latency numbers are comparable across
/// this crate's benches.
fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Sample standard deviation (n-1 denominator) of `samples_ns` in nanoseconds.
/// Zero for fewer than two samples, where dispersion is undefined.
fn stddev_ns(samples_ns: &[u64]) -> f64 {
    if samples_ns.len() < 2 {
        return 0.0;
    }
    let n = samples_ns.len() as f64;
    let mean = samples_ns.iter().map(|&v| v as f64).sum::<f64>() / n;
    let variance = samples_ns
        .iter()
        .map(|&v| {
            let d = v as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / (n - 1.0);
    variance.sqrt()
}

/// ADR-0094's fan-out marker in a physical-plan string: a hash-partitioning
/// `RepartitionExec` (`partitioning=Hash(...)`) feeding a fanned-out `Final`
/// `AggregateExec`. Byte-identical to `fans_out_final_aggregation` in
/// crates/ravel-sql/tests/parallel_final_aggregation.rs, and distinct from the
/// `RoundRobinBatch` repartition DataFusion always inserts to parallelize the
/// scan (present under both plans, not the ADR-0094 behavior).
fn fans_out_final_aggregation(plan_text: &str) -> bool {
    plan_text.contains("partitioning=Hash(")
}

/// The RSEG scan node's actual output partition count in `plan`, read from the
/// real plan's properties. Under today's segment-granular partitioning this is
/// `min(target_partitions, segment_count)`. Read from the plan, never
/// recomputed from the config, so the recorded value proves the requested
/// `target_partitions` reached the scan. Returns 0 if no `RsegScanExec` is
/// present (it always is for the fixed metrics query here).
fn scan_partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.name() == "RsegScanExec" {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(scan_partition_count)
        .max()
        .unwrap_or(0)
}

/// `"debug"` under `debug_assertions`, `"release"` otherwise. Named so the
/// report records which build produced its numbers.
fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Logical cores visible to the process, or 1 if the platform cannot report it.
fn available_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Build the multi-part dataset, then sweep every (partitions x flag)
/// combination against it, returning the full report.
pub async fn run(config: &GroupbyScalingConfig) -> Report {
    let store = Arc::clone(&config.store);
    // Unique per run, like `query_latency::run`'s tenant, so consecutive local
    // runs against the same bucket never share a prefix.
    let tenant = TenantId::new(format!("bench-tenant-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    let total_samples = publish_dataset(store.as_ref(), &tenant, config).await;

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));

    let mut combos = Vec::with_capacity(config.target_partitions.len() * 2);
    let mut groups = 0usize;
    for &tp in &config.target_partitions {
        // Both flag states at every partition value: the on/off axis.
        for parallel in [false, true] {
            let result = run_combo(
                Arc::clone(&catalog),
                Arc::clone(&store),
                tenant_hash,
                tp,
                parallel,
                config.runs,
                config.max_tenant_bytes,
                config.deadline,
                total_samples,
            )
            .await;
            groups = groups.max(result.result_rows);
            combos.push(result);
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            query: QUERY.to_string(),
            parts: config.parts,
            series: config.series,
            samples_per_series: config.samples_per_series,
            total_samples,
            groups,
            target_partitions: config.target_partitions.clone(),
            runs: config.runs,
            cores: available_cores(),
            profile: build_profile().to_string(),
        },
        combos,
    }
}

/// Run the fixed query at one (target_partitions, parallel) combination:
/// inspect the real physical plan for what the swept axes actually produced,
/// then `runs` timed iterations plus one warm-up, returning the observed facts,
/// latency spread, and derived throughput. A typed `ResourcesExhausted` from an
/// over-budget aggregation fails only this combination and lets the sweep
/// continue, rather than panicking the whole run and losing every combination
/// already measured.
#[allow(clippy::too_many_arguments)]
async fn run_combo(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    target_partitions: usize,
    parallel: bool,
    runs: usize,
    max_tenant_bytes: usize,
    deadline: Duration,
    total_samples: u64,
) -> ComboResult {
    // `target_partitions` is set through `fetch_concurrency`; every budget is
    // lifted so the benchmark measures execution time, not a budget rejection.
    let engine = EngineConfig {
        max_series: usize::MAX,
        max_samples: usize::MAX,
        max_bytes_scanned: ByteLimit::Unlimited,
        max_s3_requests: RequestLimit::Unlimited,
        fetch_concurrency: target_partitions.max(1),
        ..EngineConfig::default()
    };
    let sql_config = SqlConfig {
        engine,
        parallel_final_aggregation: parallel,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config,
        max_tenant_bytes,
    );

    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let request = || SqlRequest {
        sql: QUERY.to_string(),
        window,
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline,
    };

    // Observe the physical plan this combination actually produces, before the
    // timed runs. Planning does not execute, so it never hits the memory
    // budget; the two facts below are always available even for a combination
    // whose execution later fails. Both come from the real plan, so a broken
    // axis (a flag that never reaches the executor, a `target_partitions` that
    // never reaches the scan) surfaces as an observed value disagreeing with
    // the request rather than an echo that always looks correct.
    let accounting = QueryAccounting::new();
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Metrics, window, &[], NOW_NS)
        .await
        .expect("resolve snapshot for plan inspection");
    let planned = executor
        .plan_pinned(tenant_hash, snapshot, QUERY, &accounting, &[])
        .await
        .expect("plan query for inspection");
    let plan = planned
        .create_physical_plan()
        .await
        .expect("build physical plan for inspection");
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let fanned_out = fans_out_final_aggregation(&plan_text);
    let scan_partitions = scan_partition_count(&plan);

    // One warm run establishes the result shape and primes any lazy init
    // before the timed iterations. A ResourcesExhausted here fails this ONE
    // combination and the sweep continues.
    let warm = match executor.execute(tenant_hash, &request()).await {
        Err(SqlError::ResourcesExhausted(msg)) => {
            return ComboResult::failed(
                target_partitions,
                parallel,
                fanned_out,
                scan_partitions,
                msg,
            );
        }
        other => other.expect("warm query"),
    };
    let result_rows = warm.output.num_rows();
    let segments_scanned = warm.stats.segments;

    let mut latencies_ns = Vec::with_capacity(runs.max(1));
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        let outcome = match executor.execute(tenant_hash, &request()).await {
            Err(SqlError::ResourcesExhausted(msg)) => {
                return ComboResult::failed(
                    target_partitions,
                    parallel,
                    fanned_out,
                    scan_partitions,
                    msg,
                );
            }
            other => other.expect("timed query"),
        };
        latencies_ns.push(start.elapsed().as_nanos() as u64);
        assert_eq!(
            outcome.output.num_rows(),
            result_rows,
            "group-by result row count is deterministic across runs"
        );
    }
    let runs_taken = latencies_ns.len();
    latencies_ns.sort_unstable();

    let median_ns = percentile(&latencies_ns, 0.50);
    let min_ns = latencies_ns.first().copied().unwrap_or(0);
    let max_ns = latencies_ns.last().copied().unwrap_or(0);
    let stddev = stddev_ns(&latencies_ns);
    let samples_ms = latencies_ns.iter().map(|&ns| ns as f64 / 1e6).collect();
    let rows_per_sec = if median_ns == 0 {
        0.0
    } else {
        total_samples as f64 / (median_ns as f64 / 1e9)
    };

    ComboResult {
        target_partitions,
        parallel_final_aggregation: parallel,
        fanned_out,
        scan_partitions,
        segments_scanned,
        result_rows,
        runs_taken,
        min_ms: min_ns as f64 / 1e6,
        median_ms: median_ns as f64 / 1e6,
        max_ms: max_ns as f64 / 1e6,
        stddev_ms: stddev / 1e6,
        samples_ms,
        rows_per_sec,
        error: None,
    }
}

/// Write `config.parts` RSEG objects, each carrying a disjoint slice of the
/// tenant's series, and publish each one's commit record. Returns the total
/// sample count the query scans.
async fn publish_dataset(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    config: &GroupbyScalingConfig,
) -> u64 {
    let parts = config.parts.max(1);
    let tenant_hash = tenant.hash();
    let mut total_samples = 0u64;
    let mut offset = 0usize;
    for part in 0..parts {
        // Distribute series as evenly as possible; the first `remainder` parts
        // carry one extra so the totals sum exactly to `config.series`.
        let base = config.series / parts;
        let remainder = config.series % parts;
        let series_in_part = base + usize::from(part < remainder);
        if series_in_part == 0 {
            continue;
        }
        total_samples += publish_part(
            store,
            tenant,
            tenant_hash,
            part,
            series_in_part,
            offset,
            config.samples_per_series,
        )
        .await;
        offset += series_in_part;
    }
    total_samples
}

/// Write one part's series into a single RSEG object and publish its commit
/// record. `series_idx_offset` keeps each part's series ids disjoint from the
/// others', and a per-part writer identity keeps the data-object keys distinct.
async fn publish_part(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    part: usize,
    series_count: usize,
    series_idx_offset: usize,
    samples_per_series: usize,
) -> u64 {
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count,
        samples_per_series,
        // Keep every event timestamp a few microseconds after the epoch so it
        // lands inside `[0, NOW_NS]` and ingest-hour bucket 0.
        start_ts_ns: 1_000,
        interval_ns: 1_000,
        cardinality: CardinalityProfile::many_small(series_count),
        series_idx_offset,
        ..WorkloadConfig::default()
    };
    let raw = generate_raw(&workload).expect("generate dataset");
    let part_samples: u64 = raw.iter().map(|(_, _, samples)| samples.len() as u64).sum();

    let series: Vec<SeriesInput> = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect();

    let writer_id = Uuid::from_u128(1_000 + part as u128);
    let writer_seq = part as u64 + 1;
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
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

    part_samples
}
