//! Group-by aggregation core-count scaling benchmark core (ADR-0102 decision
//! 4), extended for issue #680 with a distinct-key memory sweep.
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
//!
//! # The distinct-key memory sweep (issue #680)
//!
//! The second instrument in this module ([`run_distinct`]) measures peak memory
//! pool bytes rather than latency, over the `logs` table rather than `samples`.
//! It answers one question: does the peak an aggregation reaches scale with the
//! key's distinct count `D` alone, or with `D x target_partitions`?
//!
//! It has to be the `logs` table. The `samples` path puts `RsegDedupExec`
//! between the scan and any aggregate, and that operator declares
//! `Partitioning::UnknownPartitioning(1)` and
//! `Distribution::SinglePartition` on its input, so every metrics aggregation
//! runs its partial stage on exactly one partition no matter what
//! `target_partitions` says. `LogsScanExec` has no such collapse, which is
//! why the ClickBench failures (a `logs` tenant) are where they are.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{ByteLimit, EngineConfig, LogSegmentFetcher, RequestLimit, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
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
    partition_count_of(plan, "RsegScanExec")
}

/// The output partition count of the deepest node named `node` in `plan`, read
/// from the real plan's properties rather than recomputed from the config, so a
/// recorded value proves the requested `target_partitions` reached the scan.
/// Returns 0 when no such node is present.
fn partition_count_of(plan: &Arc<dyn ExecutionPlan>, node: &str) -> usize {
    if plan.name() == node {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(|child| partition_count_of(child, node))
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
    //
    // A transient failure here (e.g. an S3 error resolving the snapshot) fails
    // this ONE combination -- `fanned_out`/`scan_partitions` are unobservable
    // when planning itself failed, so they're reported as the unfanned/zero
    // defaults, same as any other failed combo -- and the sweep continues
    // rather than losing every combination already measured.
    let accounting = QueryAccounting::new();
    let snapshot = match catalog
        .resolve(&tenant_hash, Signal::Metrics, window, &[], NOW_NS)
        .await
    {
        Ok(s) => s,
        Err(e) => return ComboResult::failed(target_partitions, parallel, false, 0, e.to_string()),
    };
    let planned = match executor
        .plan_pinned(tenant_hash, snapshot, QUERY, &accounting, &[])
        .await
    {
        Ok(p) => p,
        Err(e) => return ComboResult::failed(target_partitions, parallel, false, 0, e.to_string()),
    };
    let plan = match planned.create_physical_plan().await {
        Ok(p) => p,
        Err(e) => return ComboResult::failed(target_partitions, parallel, false, 0, e.to_string()),
    };
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let fanned_out = fans_out_final_aggregation(&plan_text);
    let scan_partitions = scan_partition_count(&plan);

    // One warm run establishes the result shape and primes any lazy init
    // before the timed iterations. ANY execution error (not just
    // ResourcesExhausted -- a DeadlineExceeded on a long sweep hits the exact
    // same "don't lose the whole run" case) fails this ONE combination and the
    // sweep continues.
    let warm = match executor.execute(tenant_hash, &request()).await {
        Ok(outcome) => outcome,
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                parallel,
                fanned_out,
                scan_partitions,
                e.to_string(),
            );
        }
    };
    let result_rows = warm.output.num_rows();
    let segments_scanned = warm.stats.segments;

    let mut latencies_ns = Vec::with_capacity(runs.max(1));
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        let outcome = match executor.execute(tenant_hash, &request()).await {
            Ok(outcome) => outcome,
            Err(e) => {
                return ComboResult::failed(
                    target_partitions,
                    parallel,
                    fanned_out,
                    scan_partitions,
                    e.to_string(),
                );
            }
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
        segment_format_version: u32::from(ravel_segment::VERSION_V7),
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

// ---------------------------------------------------------------------------
// Distinct-key memory sweep (issue #680)
// ---------------------------------------------------------------------------

/// The high-cardinality attribute key, standing in for ClickBench's `UserID`
/// and `SearchPhrase`. Its distinct count is the swept `D`.
pub const HIGH_CARDINALITY_KEY: &str = "u";

/// The low-cardinality attribute key, standing in for ClickBench's `RegionID`.
/// Its value is a pure function of the high-cardinality value, so the pair
/// cardinality of the grouped query is exactly `D`, not `D x L`. ClickBench's
/// own `RegionID x UserID` pair count is likewise far below the product.
pub const LOW_CARDINALITY_KEY: &str = "r";

/// `COUNT(DISTINCT high)`, the shape of ClickBench q05/q06.
pub const DISTINCT_ONLY_QUERY: &str =
    "SELECT count(DISTINCT attrs['u']) AS distinct_high FROM logs";

/// `GROUP BY low ... COUNT(DISTINCT high)`, the shape of ClickBench q09/q14.
pub const GROUPBY_DISTINCT_QUERY: &str = "SELECT attrs['r'] AS low, count(DISTINCT attrs['u']) AS distinct_high \
     FROM logs GROUP BY attrs['r']";

/// The two measured query shapes, `(label, sql)`.
pub const DISTINCT_QUERIES: [(&str, &str); 2] = [
    ("distinct_only", DISTINCT_ONLY_QUERY),
    ("groupby_distinct", GROUPBY_DISTINCT_QUERY),
];

const DISTINCT_SCOPE_NAME: &str = "groupby-distinct-sweep";
const DISTINCT_SCOPE_VERSION: &str = "1.0";

/// Inputs for one distinct-key memory sweep.
///
/// The dataset shape is the load-bearing part. Every RLOG object carries the
/// full set of `D` distinct high-cardinality values, so a partition that owns
/// exactly one object still sees all `D`. That is what makes the two candidate
/// scalings distinguishable: if the partial aggregation keeps one hash table
/// per input partition, peak grows with `partitions x D`; if the state is
/// shared or the partial stage gives up, peak stays at `D`.
///
/// The row count follows from that requirement and cannot be cheated: with `P`
/// partitions each needing to see `D` values, the dataset needs at least
/// `P x D` rows. `objects x distinct_values x repeats_per_value` is exactly
/// that.
pub struct DistinctScalingConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// The swept distinct counts `D` of the high-cardinality key.
    pub distinct_values: Vec<usize>,
    /// The swept `target_partitions` values, fed through `fetch_concurrency`.
    /// Values above `objects` cannot fan out: the fetcher here is un-cached, so
    /// `LogsScanExec` keeps the `min(target_partitions, segment_count)` bound
    /// (ADR-0102 decision 1). The report records the observed count so a
    /// request that never reached the scan is visible rather than assumed.
    pub target_partitions: Vec<usize>,
    /// RLOG objects the tenant is split across. Should be at least the largest
    /// swept `target_partitions`.
    pub objects: usize,
    /// Rows per distinct value per object. One is enough for the peak to be
    /// reached; raise it to dilute the key and exercise the probe thresholds.
    pub repeats_per_value: usize,
    /// Distinct low-cardinality group keys `L` (ClickBench's `RegionID` has
    /// about 9000; the failing q09 groups by it).
    pub low_cardinality: usize,
    /// Per-query and per-tenant memory ceiling. Deliberately large: this sweep
    /// measures what a query reaches, so it must not be cut off by the ceiling
    /// whose sizing it exists to inform.
    pub max_bytes: usize,
    pub deadline: Duration,
    /// `SqlConfig::skip_partial_aggregation` (issue #680). The A/B axis of the
    /// fix: `false` reproduces the pre-fix session (DataFusion's own probe
    /// thresholds), `true` is the shipped default.
    pub skip_partial_aggregation: bool,
}

impl DistinctScalingConfig {
    /// A CI-sized fixture: one small `D`, two partition values, four objects.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        DistinctScalingConfig {
            store,
            store_label: store_label.to_string(),
            distinct_values: vec![2_000],
            target_partitions: vec![1, 4],
            objects: 4,
            repeats_per_value: 1,
            low_cardinality: 8,
            max_bytes: 1 << 30,
            deadline: Duration::from_secs(120),
            skip_partial_aggregation: SqlConfig::default().skip_partial_aggregation,
        }
    }
}

/// One `(query, D, target_partitions)` measurement.
///
/// `peak_pool_bytes` is the query's own memory-pool high-water mark, read from
/// the executed query's `QueryAccounting` (`peak_intermediate_bytes`), which
/// `TenantDelegatingPool` reports on every `grow`. It is the same figure the
/// 8 GiB ClickBench pool ran out of.
#[derive(Serialize)]
pub struct DistinctResult {
    pub query_label: String,
    pub distinct_values: usize,
    pub target_partitions: usize,
    /// Observed `LogsScanExec` output partition count from the real plan.
    pub scan_partitions: usize,
    pub peak_pool_bytes: u64,
    /// `peak_pool_bytes / distinct_values`: the per-distinct-entry cost the
    /// arithmetic in deliverable 2(b) needs.
    pub bytes_per_distinct: f64,
    pub result_rows: usize,
    pub total_records: usize,
    pub elapsed_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The fitted answer for one `(query, D)` row of the sweep: how the peak moved
/// between the smallest and largest measured partition count.
#[derive(Serialize)]
pub struct DistinctScalingFit {
    pub query_label: String,
    pub distinct_values: usize,
    pub min_partitions: usize,
    pub max_partitions: usize,
    pub peak_at_min: u64,
    pub peak_at_max: u64,
    /// `peak_at_max / peak_at_min`. The pin in issue #680's deliverable 2(a) is
    /// stated on this ratio.
    pub peak_ratio: f64,
    /// `ln(peak_ratio) / ln(max_partitions / min_partitions)`: 0.0 means peak
    /// scales with `D` alone, 1.0 means it scales with `D x partitions`.
    /// Undefined (and reported as 0.0) when the two partition counts are equal
    /// or either peak is zero.
    pub partition_exponent: f64,
}

#[derive(Serialize)]
pub struct DistinctReportConfig {
    pub store: String,
    pub queries: Vec<String>,
    pub distinct_values: Vec<usize>,
    pub target_partitions: Vec<usize>,
    pub objects: usize,
    pub repeats_per_value: usize,
    pub low_cardinality: usize,
    pub max_bytes: usize,
    pub skip_partial_aggregation: bool,
    pub cores: usize,
    pub profile: String,
}

#[derive(Serialize)]
pub struct DistinctReport {
    pub config: DistinctReportConfig,
    pub results: Vec<DistinctResult>,
    pub fits: Vec<DistinctScalingFit>,
}

/// Build one `logs` dataset per swept `D`, then run both distinct query shapes
/// at every `target_partitions` value against it, recording the peak memory
/// pool bytes each reached.
pub async fn run_distinct(config: &DistinctScalingConfig) -> DistinctReport {
    let store = Arc::clone(&config.store);
    let mut results = Vec::new();

    for &distinct in &config.distinct_values {
        // A fresh tenant per D, so the datasets never share a prefix and each
        // query scans exactly its own D.
        let tenant = TenantId::new(format!("bench-distinct-{}", Uuid::new_v4()));
        let tenant_hash = tenant.hash();
        let total_records =
            publish_distinct_dataset(store.as_ref(), &tenant, config, distinct).await;

        let catalog = match Catalog::new(Arc::clone(&store), CatalogConfig::default()) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                for &tp in &config.target_partitions {
                    for (label, _) in DISTINCT_QUERIES {
                        results.push(DistinctResult::failed(
                            label,
                            distinct,
                            tp,
                            total_records,
                            e.to_string(),
                        ));
                    }
                }
                continue;
            }
        };

        for &tp in &config.target_partitions {
            for (label, sql) in DISTINCT_QUERIES {
                results.push(
                    run_distinct_combo(
                        Arc::clone(&catalog),
                        Arc::clone(&store),
                        tenant_hash,
                        label,
                        sql,
                        distinct,
                        tp,
                        total_records,
                        config.max_bytes,
                        config.deadline,
                        config.skip_partial_aggregation,
                    )
                    .await,
                );
            }
        }
    }

    let fits = fit_distinct_scaling(&results);

    DistinctReport {
        config: DistinctReportConfig {
            store: config.store_label.clone(),
            queries: DISTINCT_QUERIES
                .iter()
                .map(|(_, sql)| (*sql).to_string())
                .collect(),
            distinct_values: config.distinct_values.clone(),
            target_partitions: config.target_partitions.clone(),
            objects: config.objects,
            repeats_per_value: config.repeats_per_value,
            low_cardinality: config.low_cardinality,
            max_bytes: config.max_bytes,
            skip_partial_aggregation: config.skip_partial_aggregation,
            cores: available_cores(),
            profile: build_profile().to_string(),
        },
        results,
        fits,
    }
}

impl DistinctResult {
    fn failed(
        query_label: &str,
        distinct_values: usize,
        target_partitions: usize,
        total_records: usize,
        error: String,
    ) -> Self {
        DistinctResult {
            query_label: query_label.to_string(),
            distinct_values,
            target_partitions,
            scan_partitions: 0,
            peak_pool_bytes: 0,
            bytes_per_distinct: 0.0,
            result_rows: 0,
            total_records,
            elapsed_ms: 0.0,
            error: Some(error),
        }
    }
}

/// Fit the partition axis for every `(query, D)` pair that measured at least
/// two distinct partition counts successfully. Failed combinations are skipped
/// rather than folded in as zeros, so a fit is either over real peaks or absent.
fn fit_distinct_scaling(results: &[DistinctResult]) -> Vec<DistinctScalingFit> {
    let mut fits = Vec::new();
    let mut seen: Vec<(String, usize)> = Vec::new();
    for r in results {
        let key = (r.query_label.clone(), r.distinct_values);
        if seen.contains(&key) {
            continue;
        }
        seen.push(key.clone());

        let mut group: Vec<&DistinctResult> = results
            .iter()
            .filter(|c| {
                c.query_label == key.0
                    && c.distinct_values == key.1
                    && c.error.is_none()
                    && c.peak_pool_bytes > 0
            })
            .collect();
        group.sort_by_key(|c| c.target_partitions);
        let (Some(low), Some(high)) = (group.first(), group.last()) else {
            continue;
        };
        if low.target_partitions == high.target_partitions {
            continue;
        }
        let peak_ratio = high.peak_pool_bytes as f64 / low.peak_pool_bytes as f64;
        let partition_ratio = high.target_partitions as f64 / low.target_partitions as f64;
        fits.push(DistinctScalingFit {
            query_label: key.0,
            distinct_values: key.1,
            min_partitions: low.target_partitions,
            max_partitions: high.target_partitions,
            peak_at_min: low.peak_pool_bytes,
            peak_at_max: high.peak_pool_bytes,
            peak_ratio,
            partition_exponent: peak_ratio.ln() / partition_ratio.ln(),
        });
    }
    fits
}

/// Execute one distinct query at one `target_partitions` value and record the
/// peak pool bytes it reached. A typed error (the ClickBench failure mode)
/// fails only this combination; the sweep continues.
#[allow(clippy::too_many_arguments)]
async fn run_distinct_combo(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    query_label: &str,
    sql: &str,
    distinct_values: usize,
    target_partitions: usize,
    total_records: usize,
    max_bytes: usize,
    deadline: Duration,
    skip_partial_aggregation: bool,
) -> DistinctResult {
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
        max_query_bytes: max_bytes,
        skip_partial_aggregation,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config,
        max_bytes,
    );

    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };

    // Observed scan fan-out, from the real physical plan. Planning does not
    // execute, so it is available even when the timed execution later fails.
    let accounting = QueryAccounting::new();
    let scan_partitions = match catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], NOW_NS)
        .await
    {
        Ok(snapshot) => match executor
            .plan_pinned(tenant_hash, snapshot, sql, &accounting, &[])
            .await
        {
            Ok(planned) => match planned.create_physical_plan().await {
                Ok(plan) => partition_count_of(&plan, "LogsScanExec"),
                Err(e) => {
                    return DistinctResult::failed(
                        query_label,
                        distinct_values,
                        target_partitions,
                        total_records,
                        e.to_string(),
                    );
                }
            },
            Err(e) => {
                return DistinctResult::failed(
                    query_label,
                    distinct_values,
                    target_partitions,
                    total_records,
                    e.to_string(),
                );
            }
        },
        Err(e) => {
            return DistinctResult::failed(
                query_label,
                distinct_values,
                target_partitions,
                total_records,
                e.to_string(),
            );
        }
    };

    let request = SqlRequest {
        sql: sql.to_string(),
        window,
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline,
    };
    let start = Instant::now();
    let outcome = match executor.execute(tenant_hash, &request).await {
        Ok(outcome) => outcome,
        Err(e) => {
            return DistinctResult::failed(
                query_label,
                distinct_values,
                target_partitions,
                total_records,
                e.to_string(),
            );
        }
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1e3;
    let peak_pool_bytes = outcome.accounting.peak_intermediate_bytes;

    DistinctResult {
        query_label: query_label.to_string(),
        distinct_values,
        target_partitions,
        scan_partitions,
        peak_pool_bytes,
        bytes_per_distinct: peak_pool_bytes as f64 / distinct_values.max(1) as f64,
        result_rows: outcome.output.num_rows(),
        total_records,
        elapsed_ms,
        error: None,
    }
}

/// Write `config.objects` RLOG objects, each carrying every one of the
/// `distinct` high-cardinality values `config.repeats_per_value` times, and
/// publish each object's commit record. Returns the total record count.
///
/// The uniform spread is the point (see [`DistinctScalingConfig`]): a partition
/// that owns exactly one object still sees all `distinct` values, so a
/// per-partition partial hash table is full-sized in every partition.
async fn publish_distinct_dataset(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    config: &DistinctScalingConfig,
    distinct: usize,
) -> usize {
    let objects = config.objects.max(1);
    let distinct = distinct.max(1);
    let repeats = config.repeats_per_value.max(1);
    let low = config.low_cardinality.max(1);
    let tenant_hash = tenant.hash();

    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("hits".to_string()),
    )];
    let stream_id = log_stream_id(&resource, DISTINCT_SCOPE_NAME, DISTINCT_SCOPE_VERSION, &[]);
    let stream_attrs =
        stream_attrs_bytes(&resource, DISTINCT_SCOPE_NAME, DISTINCT_SCOPE_VERSION, &[]);

    let mut total = 0usize;
    let mut ts = 1_000i64;
    for obj_idx in 0..objects {
        let writer_seq = (obj_idx + 1) as u64;
        let writer_id = Uuid::from_u128(0x6800_0100 + obj_idx as u128);
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut records = 0usize;
        for repeat in 0..repeats {
            for value in 0..distinct {
                min = min.min(ts);
                max = max.max(ts);
                writer
                    .push(LogRecord {
                        stream_id,
                        stream_attrs: stream_attrs.clone(),
                        ts_ns: ts,
                        observed_ts_ns: ts,
                        severity_num: 9,
                        severity_text: "INFO".to_string(),
                        body: String::new(),
                        trace_id: None,
                        span_id: None,
                        flags: repeat as u32,
                        attrs: vec![
                            (
                                HIGH_CARDINALITY_KEY.to_string(),
                                AttrValue::Str(format!("u{value:012}")),
                            ),
                            (
                                LOW_CARDINALITY_KEY.to_string(),
                                AttrValue::Str(format!("r{}", value % low)),
                            ),
                        ],
                    })
                    .expect("push record");
                ts += 1_000;
                records += 1;
            }
        }
        let bytes = writer.finish().expect("finish object");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
        total += records;
    }
    total
}
