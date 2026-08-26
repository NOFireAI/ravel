//! Intra-segment scan-partitioning core-count scaling benchmark (ADR-0102
//! decision 1, epic #361 item 1).
//!
//! The sibling of [`crate::groupby_scaling`], aimed at the specific capability
//! item 1 adds: a `logs` query touching FEWER segments than `target_partitions`
//! used to pin to at most `segment_count` scan partitions
//! (`min(target_partitions, segment_count)`), so raising `target_partitions`
//! past the segment count bought nothing. Block-level striping lets such a query
//! fan out to `target_partitions`. This bench holds one `logs` query and a fixed
//! FEW-segment / MANY-block dataset, and sweeps `target_partitions` to show the
//! scan fanning out past the segment count and the wall time responding.
//!
//! The fan-out is gated on ADR-0046's read cache
//! (`LogSegmentFetcher::has_cache`, the precondition ADR-0102 decision 1 names):
//! a cache-wired fetcher gets `target_partitions` partitions, an un-cached one
//! keeps the old `min(target_partitions, segment_count)` bound. So this bench
//! sweeps every `target_partitions` value on BOTH a cache-wired and an un-cached
//! fetcher, and the two sides answer different questions: the cached side shows
//! how far the scan fans out past the segment count and what the wall time does,
//! the un-cached side shows the request count staying put once the cap binds.
//!
//! It also reports the object-store request count each combination issues, read
//! from the executed query's `QueryAccounting` (the same source
//! [`crate::sql_latency`] and [`crate::pushdown_crossover`] use). The fetch unit
//! is the whole object (ADR-0087 decision 3: no ranged block reader), so every
//! partition owning blocks in a segment issues its own whole-object read at that
//! segment's key. Un-cached, the partition count is capped at the segment count
//! and each segment is assigned whole to one partition (ADR-0102's un-cached
//! amendment), so raising `target_partitions` changes nothing at any value: the
//! count is one plan read plus one scan read per segment. Cache-wired, the
//! partition count keeps climbing and those repeated reads are what ADR-0046's
//! single-flight cache absorbs.
//!
//! Every request-count figure in this report is a measurement of THIS fixture --
//! a `MemoryStore`, this segment/block/partition shape, a cache sized to hold the
//! whole dataset with no eviction -- and not a general "no amplification" result
//! about striping. Cache size, eviction, object size, and store latency all move
//! it.
//!
//! Finally it reports the cost of the planning prune itself
//! ([`PlanningLatency`]): `ravel_sql`'s `compute_plan_counts` awaits
//! [`LogSegmentFetcher::plan_segment`] once per segment, sequentially, before any
//! partition drains a block. That serialization is invisible at two or three
//! segments, so this measures it over the fixture's full segment set and reports
//! the serial wall time next to the same prunes issued concurrently.
//!
//! Report-only: it never changes library behavior. Gated on the `sql-latency`
//! feature, like the other SQL scaling benches.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    ByteLimit, CacheFetchError, EngineConfig, LogQuery, LogSegmentFetcher, RequestLimit,
    SegmentFetcher,
};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde::Serialize;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Frozen query clock, like [`crate::groupby_scaling`]: every record lands a few
/// microseconds after the epoch, inside `[0, NOW_NS]` and ingest-hour bucket 0,
/// so `Catalog::resolve` issues a bounded number of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// The one query the sweep measures: a bare projection scan (no aggregation), so
/// the scan node's partition fan-out drives the parallelism directly rather than
/// being masked by a downstream aggregate.
pub const QUERY: &str = "SELECT ts, body FROM logs";

const SCOPE_NAME: &str = "logs-scan-scaling-bench";
const SCOPE_VERSION: &str = "1.0";

/// Inputs for one scaling run.
pub struct LogsScanScalingConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// RLOG objects the tenant is split across. Smaller than the largest swept
    /// `target_partitions` so the run exercises the undersubscribed case this
    /// item fixes, but large enough that the planning prune's per-segment
    /// serialized await ([`PlanningLatency`]) is measurable rather than lost in
    /// noise.
    pub segments: usize,
    /// Records per object. With [`block_target_records`](Self::block_target_records)
    /// small, this sets how many blocks each segment carries, which is the pool
    /// the striping fans out across.
    pub records_per_object: usize,
    /// Writer block target. Small so each segment holds many blocks.
    pub block_target_records: usize,
    /// Swept `target_partitions` values (fed through `fetch_concurrency`).
    pub target_partitions: Vec<usize>,
    /// Timed repetitions per combination; the first is the cold run whose
    /// accounting counters are reported.
    pub runs: usize,
    pub deadline: Duration,
}

impl LogsScanScalingConfig {
    /// A cheap fixture for the acceptance test and CI.
    ///
    /// 32 segments, not the 2 this started at: at two segments the planning
    /// prune's per-segment serialized await is a rounding error, so the
    /// [`PlanningLatency`] figure said nothing. The swept partition counts
    /// straddle the segment count on both sides (1 below it, 32 at it, 64 above
    /// it) so the report shows the un-cached cap binding and the cached fan-out
    /// continuing past it. Objects stay small (96 records, 6 blocks each) to keep
    /// the run cheap despite the segment count.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        LogsScanScalingConfig {
            store,
            store_label: store_label.to_string(),
            segments: 32,
            records_per_object: 96,
            block_target_records: 16,
            target_partitions: vec![1, 32, 64],
            runs: 2,
            deadline: Duration::from_secs(60),
        }
    }
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub query: String,
    pub segments: usize,
    pub records_per_object: usize,
    pub block_target_records: usize,
    pub total_records: usize,
    pub target_partitions: Vec<usize>,
    pub runs: usize,
    pub cores: usize,
    pub profile: String,
}

/// One `(target_partitions x cache)` measurement. Every non-axis field is read
/// from the real plan or the executed query, never echoed from the request.
#[derive(Serialize)]
pub struct ComboResult {
    pub target_partitions: usize,
    /// Whether ADR-0046's read cache was wired into the logs fetcher.
    pub cache_wired: bool,
    /// Observed: the `LogsScanExec` node's declared output partition count in the
    /// real physical plan. Under block-level striping (ADR-0102) this is
    /// `target_partitions` when the cache is wired, even where the segment count
    /// is smaller -- the whole point of the item -- and
    /// `min(target_partitions, segment_count)` when it is not. Read from the plan
    /// rather than recomputed.
    pub scan_partitions: usize,
    /// Observed: partitions of the scan that actually decoded blocks, from
    /// `SqlStats`/metrics via a drained execution -- see `non_empty_partitions`.
    /// The capability check: this exceeds the segment count in the
    /// undersubscribed case.
    pub non_empty_partitions: usize,
    /// Segments the resolved snapshot scanned.
    pub segments_scanned: usize,
    pub blocks_total: u64,
    pub blocks_scanned: u64,
    /// Object-store GET requests the cold run issued (`QueryAccounting`). The
    /// number this deliverable exists to report. Un-cached, each segment is
    /// assigned whole to one partition, so this is one plan read plus one scan
    /// read per segment at every `target_partitions`. Cache-wired, the partition count
    /// keeps climbing and the repeated whole-object reads at one key are what
    /// single-flight absorbs. A figure for THIS fixture only, not a general
    /// result (see the module doc).
    pub object_store_get_requests: u64,
    /// Cache hits/misses the cold run recorded (zero when un-cached).
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub object_store_bytes: u64,
    pub result_rows: usize,
    pub runs_taken: usize,
    pub min_ms: f64,
    pub median_ms: f64,
    pub max_ms: f64,
    pub stddev_ms: f64,
    pub samples_ms: Vec<f64>,
    /// Scanned records per second, from `total_records` and the median latency.
    pub rows_per_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ComboResult {
    fn failed(
        target_partitions: usize,
        cache_wired: bool,
        scan_partitions: usize,
        error: String,
    ) -> Self {
        ComboResult {
            target_partitions,
            cache_wired,
            scan_partitions,
            non_empty_partitions: 0,
            segments_scanned: 0,
            blocks_total: 0,
            blocks_scanned: 0,
            object_store_get_requests: 0,
            cache_hits: 0,
            cache_misses: 0,
            object_store_bytes: 0,
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

/// The planning prune's cost, measured directly against the same call
/// `ravel_sql`'s `compute_plan_counts` makes
/// ([`LogSegmentFetcher::plan_segment`], one per segment).
///
/// `compute_plan_counts` awaits those prunes SEQUENTIALLY, once per query,
/// before any partition drains its first block, so this latency sits on the
/// critical path of every partition. `serial_ms` is that loop; `concurrent_ms`
/// is the same prunes issued together, i.e. what the serialization costs. Both
/// use a fresh un-cached fetcher, so neither is served warm by the other.
#[derive(Serialize)]
pub struct PlanningLatency {
    /// Segments pruned, i.e. how many serialized awaits the figure covers.
    pub segments: usize,
    /// Surviving blocks the prunes counted, summed over segments.
    pub total_blocks: usize,
    /// Wall time of the per-segment sequential loop `compute_plan_counts` runs.
    pub serial_ms: f64,
    /// Wall time of the same prunes awaited concurrently.
    pub concurrent_ms: f64,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    /// What the per-segment serialized planning prune costs on this fixture.
    pub planning: PlanningLatency,
    pub combos: Vec<ComboResult>,
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

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

/// The `LogsScanExec` node's declared output partition count in `plan`, read
/// from the real plan, so the recorded value proves the requested
/// `target_partitions` reached the scan. 0 if no `LogsScanExec` is present.
fn scan_partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.name() == "LogsScanExec" {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(scan_partition_count)
        .max()
        .unwrap_or(0)
}

/// The `LogsScanExec` node in `plan`, if any.
fn find_logs_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == "LogsScanExec" {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_logs_scan)
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn available_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Build the few-segment / many-block dataset, then sweep every
/// `(target_partitions x cache)` combination, returning the full report.
pub async fn run(config: &LogsScanScalingConfig) -> Report {
    let store = Arc::clone(&config.store);
    let tenant = TenantId::new(format!("bench-tenant-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    let total_records = publish_dataset(store.as_ref(), &tenant, config).await;

    let planning = measure_planning_latency(Arc::clone(&store), tenant_hash).await;

    let mut combos = Vec::with_capacity(config.target_partitions.len() * 2);
    for &tp in &config.target_partitions {
        for cache_wired in [false, true] {
            let result = run_combo(
                Arc::clone(&store),
                tenant_hash,
                tp,
                cache_wired,
                config.runs,
                config.deadline,
                total_records,
            )
            .await;
            combos.push(result);
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            query: QUERY.to_string(),
            segments: config.segments,
            records_per_object: config.records_per_object,
            block_target_records: config.block_target_records,
            total_records,
            target_partitions: config.target_partitions.clone(),
            runs: config.runs,
            cores: available_cores(),
            profile: build_profile().to_string(),
        },
        planning,
        combos,
    }
}

/// Time the planning prune both ways: the per-segment sequential loop
/// `ravel_sql`'s `compute_plan_counts` runs, and the same prunes awaited
/// concurrently. Each uses its own un-cached fetcher so neither reads bytes the
/// other warmed.
async fn measure_planning_latency(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
) -> PlanningLatency {
    let window = TimeRange {
        start_ns: 0,
        end_ns: NOW_NS,
    };
    let catalog = Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog");
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], NOW_NS)
        .await
        .expect("resolve snapshot");
    let segments = snapshot.segments;
    let query = LogQuery::new(0, NOW_NS);

    let serial_fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let accounting = QueryAccounting::new();
    let start = Instant::now();
    let mut total_blocks = 0usize;
    for seg in &segments {
        let planned = serial_fetcher
            .plan_segment(seg, tenant_hash, &query, &accounting)
            .await
            .expect("plan segment");
        if let Some((survivors, _)) = planned {
            total_blocks += survivors;
        }
    }
    let serial = start.elapsed();

    let concurrent_fetcher = LogSegmentFetcher::new(store);
    let accounting = QueryAccounting::new();
    let start = Instant::now();
    let planned = futures::future::join_all(segments.iter().map(|seg| {
        let fetcher = concurrent_fetcher.clone();
        let query = query.clone();
        let accounting = accounting.clone();
        async move {
            fetcher
                .plan_segment(seg, tenant_hash, &query, &accounting)
                .await
                .expect("plan segment")
        }
    }))
    .await;
    let concurrent = start.elapsed();
    assert_eq!(
        planned
            .iter()
            .filter_map(|p| p.as_ref().map(|(s, _)| *s))
            .sum::<usize>(),
        total_blocks,
        "both planning passes must prune to the same surviving-block count"
    );

    PlanningLatency {
        segments: segments.len(),
        total_blocks,
        serial_ms: serial.as_secs_f64() * 1e3,
        concurrent_ms: concurrent.as_secs_f64() * 1e3,
    }
}

/// One `(target_partitions, cache_wired)` measurement.
#[allow(clippy::too_many_arguments)]
async fn run_combo(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    target_partitions: usize,
    cache_wired: bool,
    runs: usize,
    deadline: Duration,
    total_records: usize,
) -> ComboResult {
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
        ..SqlConfig::default()
    };

    // A fresh catalog and a fresh (optionally cache-wired) logs fetcher per
    // combination, so the cold run genuinely pays the object-store traffic and
    // cache misses a warm repeat would elide.
    let catalog = match Catalog::new(Arc::clone(&store), CatalogConfig::default()) {
        Ok(c) => Arc::new(c),
        Err(e) => return ComboResult::failed(target_partitions, cache_wired, 0, e.to_string()),
    };
    let mut logs_fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    if cache_wired {
        // Big enough to hold every object with room to spare, so the striping's
        // repeated whole-object reads coalesce onto cache hits.
        let cache: Cache<CacheFetchError> = Cache::new(CacheLimits::new(1 << 30, 1 << 20, 1 << 30));
        logs_fetcher = logs_fetcher.with_cache(Arc::new(cache));
    }
    let executor = SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(Arc::clone(&store)),
        logs_fetcher,
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config,
        1 << 30,
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

    // Observe the physical plan's scan fan-out before the timed runs. Planning
    // does not execute, so it is always available even if execution later fails.
    let accounting = QueryAccounting::new();
    let snapshot = match catalog
        .resolve(&tenant_hash, Signal::Logs, window, &[], NOW_NS)
        .await
    {
        Ok(s) => s,
        Err(e) => return ComboResult::failed(target_partitions, cache_wired, 0, e.to_string()),
    };
    let planned = match executor
        .plan_pinned(tenant_hash, snapshot, QUERY, &accounting, &[])
        .await
    {
        Ok(p) => p,
        Err(e) => return ComboResult::failed(target_partitions, cache_wired, 0, e.to_string()),
    };
    let plan = match planned.create_physical_plan().await {
        Ok(p) => p,
        Err(e) => return ComboResult::failed(target_partitions, cache_wired, 0, e.to_string()),
    };
    let scan_partitions = scan_partition_count(&plan);

    // Cold run: its accounting is the informative one (it pays the GETs and
    // cache misses a warm repeat elides), and its metrics give the non-empty
    // partition count. Any execution error fails only this combination.
    let cold = match executor.execute(tenant_hash, &request()).await {
        Ok(outcome) => outcome,
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                scan_partitions,
                e.to_string(),
            );
        }
    };
    let result_rows = cold.output.num_rows();
    let segments_scanned = cold.stats.segments;
    let blocks_total = cold.stats.blocks_total;
    let blocks_scanned = cold.stats.blocks_scanned;
    let acc = &cold.accounting;
    let object_store_get_requests = acc.s3_requests(AccountedOp::Get);
    let cache_hits = acc.cache_hits;
    let cache_misses = acc.cache_misses;
    let object_store_bytes = acc.total_s3_bytes();

    // Drive the LogsScanExec node's own partitions to count how many actually
    // emit rows -- the capability check (exceeds the segment count when
    // undersubscribed). Driven directly on the scan node so plan-level
    // repartition/coalesce above it cannot mask the scan's real fan-out.
    let non_empty_partitions = match count_non_empty_scan_partitions(&plan).await {
        Ok(n) => n,
        Err(e) => {
            return ComboResult::failed(target_partitions, cache_wired, scan_partitions, e);
        }
    };

    let mut latencies_ns = Vec::with_capacity(runs.max(1));
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        let outcome = match executor.execute(tenant_hash, &request()).await {
            Ok(outcome) => outcome,
            Err(e) => {
                return ComboResult::failed(
                    target_partitions,
                    cache_wired,
                    scan_partitions,
                    e.to_string(),
                );
            }
        };
        latencies_ns.push(start.elapsed().as_nanos() as u64);
        assert_eq!(
            outcome.output.num_rows(),
            result_rows,
            "logs scan result row count is deterministic across runs"
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
        total_records as f64 / (median_ns as f64 / 1e9)
    };

    ComboResult {
        target_partitions,
        cache_wired,
        scan_partitions,
        non_empty_partitions,
        segments_scanned,
        blocks_total,
        blocks_scanned,
        object_store_get_requests,
        cache_hits,
        cache_misses,
        object_store_bytes,
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

/// Drive the `LogsScanExec` node's own partitions to exhaustion and count how
/// many emitted at least one row. Executed directly on the scan node so a
/// repartition/coalesce above it in the plan cannot mask the scan's fan-out.
/// Returns 0 if the plan carries no `LogsScanExec`.
async fn count_non_empty_scan_partitions(plan: &Arc<dyn ExecutionPlan>) -> Result<usize, String> {
    use datafusion::execution::TaskContext;
    use futures::StreamExt;

    let Some(scan) = find_logs_scan(plan) else {
        return Ok(0);
    };
    let ctx = Arc::new(TaskContext::default());
    let partitions = scan.output_partitioning().partition_count();
    let mut non_empty = 0;
    for p in 0..partitions {
        let mut stream = scan
            .execute(p, Arc::clone(&ctx))
            .map_err(|e| format!("execute scan partition {p}: {e}"))?;
        let mut rows = 0usize;
        while let Some(next) = stream.next().await {
            rows += next
                .map_err(|e| format!("drain scan partition {p}: {e}"))?
                .num_rows();
        }
        if rows > 0 {
            non_empty += 1;
        }
    }
    Ok(non_empty)
}

/// The single stream this dataset uses; small on purpose so every record shares
/// one `LogStreamId` and the fan-out is purely block-level, not stream-level.
fn resource() -> Vec<(String, AttrValue)> {
    vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )]
}

/// Write `config.segments` RLOG objects, each carrying `records_per_object`
/// records under `config.block_target_records`-record blocks, and publish each
/// one's commit record so a real `Catalog::resolve` finds it. Returns the total
/// record count the query scans.
async fn publish_dataset(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    config: &LogsScanScalingConfig,
) -> usize {
    let segments = config.segments.max(1);
    let per_object = config.records_per_object.max(1);
    let tenant_hash = tenant.hash();
    let cfg = RlogConfig {
        block_target_records: config.block_target_records.max(1),
        ..RlogConfig::default()
    };
    let mut total = 0usize;
    let mut ts = 1_000i64;
    for obj_idx in 0..segments {
        let writer_seq = (obj_idx + 1) as u64;
        let writer_id = Uuid::from_u128(0x5200_0100 + obj_idx as u128);
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq,
        };
        let mut writer = RlogWriter::new(cfg, identity);
        let res = resource();
        let stream_id = log_stream_id(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
        let stream_attrs = stream_attrs_bytes(&res, SCOPE_NAME, SCOPE_VERSION, &[]);
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for _ in 0..per_object {
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
                    body: format!("request {ts} ok"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push record");
            ts += 1_000;
            total += 1;
        }
        let bytes = writer.finish().expect("finish object");
        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            // The real BLAKE3 of the bytes, not a zero placeholder: ADR-0046's
            // cache key is `(tenant, content_hash, offset, object_size)`, so
            // objects sharing a zero hash collide with each other whenever their
            // sizes also match, and the cached side of this sweep would report
            // cross-segment collisions as if they were single-flight
            // coalescing.
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: per_object as u64,
            series_count: 1,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: 1,
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }
    total
}
