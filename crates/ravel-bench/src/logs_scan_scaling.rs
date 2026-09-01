//! Intra-segment scan-partitioning core-count scaling benchmark (ADR-0102
//! decision 1, epic #361 item 1).
//!
//! # The four row kinds, and the amplification each one can show (issue #693)
//!
//! The request-count figures here used to come from a single configuration:
//! whole-object reads served out of a cache sized to hold the entire dataset.
//! That is the one configuration in which the amplification #693 measured
//! cannot appear at all, because single-flight absorbs the repeats and a
//! whole-object fetch hides how much of each object a partition actually
//! needed. So the sweep carries two axes next to `target_partitions`:
//!
//! - `cache_wired`: ADR-0046's read cache on and off, the same counters read
//!   from the same [`QueryAccounting`] fields on both sides.
//! - `over_threshold`: whether each segment sits above the logs fetcher's
//!   block-range threshold (ADR-0107). The fixture publishes objects that clear
//!   the production [`ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`] on
//!   their own -- 96 records of 16 KiB body each -- and the `over_threshold`
//!   rows use that production threshold unchanged. The whole-object rows read
//!   the SAME objects with the threshold moved out of reach
//!   ([`WHOLE_OBJECT_BLOCK_RANGE_THRESHOLD`]), so the two differ only in read
//!   shape. Forcing the ranged path onto tiny objects with a threshold of 0 is
//!   the other route, and it is rejected here: the suffix probe is a fixed
//!   64 KiB ([`ravel_query::DEFAULT_LOG_SUFFIX_LEN`]), so on a 3 KiB object it
//!   re-reads the whole object and every ranged read sequence costs about two
//!   object-reads, which doubles the bytes figure as an artifact of object size
//!   rather than of the path.
//!
//! Each row also carries two figures derived in the report rather than left to
//! the reader: [`ComboResult::reads_per_segment`] and
//! [`ComboResult::bytes_amplification`]. What the four row kinds measure, for
//! this report's one statement (`SELECT ts, body FROM logs` over the full
//! window, which carries no block predicate and contains every segment):
//!
//! - **any row, `target_partitions` at or below the segment count** (both
//!   `over_threshold` values, both cache settings): #693 part 3's whole-segment
//!   fast path (`LogsScanExec::whole_segment_fast_path`). The plan phase is
//!   skipped, each segment is assigned whole to one partition, and each is read
//!   once with a single full-range GET. `reads_per_segment` is about 1 (it
//!   divides every accounted GET by the segment count, and the resolve's one
//!   catalog probe rides along, so 33 GETs over 32 segments reads 1.03) and
//!   `bytes_amplification` is 1.0 within float slack (the probe carries no
//!   bytes), with zero suffix probes, whether or not a cache is wired. Issue #739 dropped the threshold conjunct, so the
//!   whole-object (below-threshold) rows now take this path too: object size no
//!   longer decides the routing, and the cache has nothing left to absorb on
//!   this shape.
//! - **cache wired, `target_partitions` above the segment count** (both
//!   `over_threshold` values): the fast path's fourth conjunct
//!   (`relevant_segments >= target_partitions`) fails, so the unchanged
//!   plan-then-stripe path runs. The plan phase probes every segment, then each
//!   partition opens the segments it holds blocks in and fetches its candidate
//!   blocks, and the cache coalesces the repeats onto the extents already
//!   resident. This is the only combination in the sweep that still pays a plan
//!   phase, and it is why the sweep keeps a partition value above the segment
//!   count. The `over_threshold` axis now decides only the read shape here (a
//!   ranged sequence above the threshold, a whole-object read below it), not
//!   whether the plan phase runs.
//! - **un-cached, `target_partitions` above the segment count** (both
//!   `over_threshold` values): the un-cached partition count is capped at the
//!   segment count (the un-cached amendment), so `relevant_segments >=
//!   target_partitions` still holds and the fast path fires. Each segment is
//!   read once with a single full-range GET, so the GET count stays flat across
//!   the sweep.
//!
//! An earlier version of this report pinned the un-cached over-threshold rows
//! to `1 + min(partitions, blocks_per_segment)` passes over the dataset. That
//! was intra-segment block striping's law: it assumed `n` partitions each
//! opening the same segment and fetching their own candidate blocks. #693
//! part 1 confined it to the cache-wired path, and #693 part 3 removed it from
//! the predicate-free full-window shape entirely. Since #739 that removal no
//! longer depends on object size, so it governs the predicated paths and the
//! cache-wired combos above the segment count, which is where the sole
//! plan-then-stripe rows in this sweep sit.
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
//! [`crate::sql_latency`] and [`crate::pushdown_crossover`] use). On the fast
//! path each relevant segment is read once with a single whole-object GET,
//! whichever `over_threshold` value it carries. Only when the query stripes
//! (the cache-wired combos above the segment count) does each partition owning
//! blocks in a segment issue its own read sequence at that segment's key, and
//! only there does the `over_threshold` axis change the sequence shape (one
//! whole-object GET at or below the fetcher's block-range threshold, a suffix
//! probe plus directory sections plus coalesced candidate blocks above it,
//! ADR-0107). Un-cached, the partition count is capped at the segment count and
//! each segment is assigned whole to one partition (ADR-0102's un-cached
//! amendment), so raising `target_partitions` changes nothing at any value.
//! Cache-wired above the segment count the partition count keeps climbing and
//! those repeated reads are what ADR-0046's single-flight cache absorbs.
//!
//! Every request-count figure in this report is a measurement of THIS fixture --
//! a `MemoryStore`, this segment/block/partition shape, and on the cached rows a
//! cache sized to hold the whole dataset with no eviction -- and not a general
//! result about striping in either direction. Cache size, eviction, object size,
//! and store latency all move it.
//!
//! Finally it reports the cost of the planning prune itself
//! ([`PlanningLatency`]): `ravel_sql`'s `compute_plan_counts` awaits
//! [`LogSegmentFetcher::plan_segment`] once per segment, sequentially, before any
//! partition drains a block. That serialization is invisible at two or three
//! segments, so this measures it over the fixture's full segment set and reports
//! the serial wall time next to the same prunes issued concurrently.
//!
//! # Why [`PlanningLatency`] is still worth measuring
//!
//! Issue #693 part 3 lets a predicate-free, full-window scan skip the plan phase
//! entirely and read each relevant segment whole in one GET
//! (`LogsScanExec::whole_segment_fast_path`), which is exactly the shape this
//! report's statement has. That fast path was once gated on every segment sitting
//! above the block-range threshold; issue #739 removed that conjunct, so object
//! size no longer decides anything. Every combo at or below the segment count --
//! both `over_threshold` values, both cache settings -- now takes the fast path
//! and pays no plan phase at all. [`PlanningLatency`] is therefore not a component
//! of those rows' wall time: it is the standing measurement of what a query that
//! does NOT satisfy the fast path's conjuncts -- any predicate, a partial window
//! overlap, a pending erasure, or more partitions than relevant segments -- still
//! pays before its first block is drained. The cache-wired row above the segment
//! count (32 relevant segments cannot fill 64 partitions) is the one combo in this
//! sweep on the plan-then-stripe path, and the one that pays it.
//!
//! [`PlanningLatency`] is measured on its own fetchers by calling `plan_segment`
//! directly, so that figure describes `compute_plan_counts`'s serialized await
//! regardless of which path the swept combos take.
//!
//! Issue #693 tracks the request-count work this report's figures are quoted
//! from.
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
    ByteLimit, CacheFetchError, DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, EngineConfig, LogQuery,
    LogSegmentFetcher, RequestLimit, SegmentFetcher,
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
    /// Log body length in bytes. Sized so the published objects clear
    /// [`DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`], which is what makes the
    /// `over_threshold` rows a measurement of ADR-0107's ranged path and of
    /// #693 part 3's fast path, rather than of a threshold override (see the
    /// module doc).
    pub body_bytes: usize,
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
    /// continuing past it, and the value above it is also what exercises the
    /// whole-segment fast path's `relevant_segments >= target_partitions`
    /// conjunct failing. Objects stay at 96 records and 6 blocks each to keep the
    /// run cheap despite the segment count; their 16 KiB bodies are what carry
    /// each one past [`DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`], which the
    /// `over_threshold` rows need (see the module doc).
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        LogsScanScalingConfig {
            store,
            store_label: store_label.to_string(),
            segments: 32,
            records_per_object: 96,
            block_target_records: 16,
            body_bytes: 16 * 1024,
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
    pub body_bytes: usize,
    pub total_records: usize,
    /// Bytes of RLOG object the fixture published, summed over every segment.
    /// The denominator of [`ComboResult::bytes_amplification`], so a row's
    /// figure is "times the dataset" rather than a raw byte count nobody can
    /// scale.
    pub dataset_bytes: u64,
    /// Size of the smallest object published. The `over_threshold` rows are only
    /// honest if this exceeds [`OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD`], so the
    /// report carries it rather than asserting the shape in prose.
    pub min_object_bytes: u64,
    pub target_partitions: Vec<usize>,
    /// The block-range threshold the `over_threshold` rows build their fetcher
    /// with: ADR-0107's production default, which every object in this fixture
    /// exceeds.
    pub over_threshold_block_range_threshold: u64,
    /// The block-range threshold the whole-object rows build their fetcher with,
    /// high enough that no object reaches it.
    pub whole_object_block_range_threshold: u64,
    pub runs: usize,
    pub cores: usize,
    pub profile: String,
}

/// One `(target_partitions x cache x over_threshold)` measurement. Every
/// non-axis field is read from the real plan or the executed query, never echoed
/// from the request; the two derived figures at the end are computed from those
/// readings, not estimated.
#[derive(Serialize)]
pub struct ComboResult {
    pub target_partitions: usize,
    /// Whether ADR-0046's read cache was wired into the logs fetcher.
    pub cache_wired: bool,
    /// Whether the logs fetcher was built with ADR-0107's production block-range
    /// threshold ([`OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD`]), which every object
    /// in this fixture exceeds. False builds the same fetcher with the threshold
    /// out of reach ([`WHOLE_OBJECT_BLOCK_RANGE_THRESHOLD`]), so the same objects
    /// are read whole; the two rows differ only in read shape.
    ///
    /// Issue #739 dropped the fast path's `object_size > block_range_threshold`
    /// conjunct, so this axis no longer decides whether #693 part 3's
    /// whole-segment fast path can run: the whole-object rows take the fast path
    /// at or below the segment count exactly like the over-threshold rows. It
    /// now decides only the read shape on the plan-then-stripe path (the
    /// cache-wired combos above the segment count).
    pub over_threshold: bool,
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
    /// number this deliverable exists to report.
    ///
    /// On every row whose partition count is at or below the segment count --
    /// both `over_threshold` values, with or without a cache -- #693 part 3's
    /// whole-segment fast path skips the plan phase and reads each segment once,
    /// so this is exactly `segments_scanned` plus the catalog slack, with zero
    /// plan probes (since #739, object size no longer changes this). Above the
    /// segment count the un-cached rows still take the fast path (the partition
    /// count is capped at the segment count), while the cached rows decline it
    /// and stripe blocks, and the repeated reads at one key are what
    /// single-flight absorbs. A figure for THIS fixture only, not a general
    /// result (see the module doc).
    pub object_store_get_requests: u64,
    /// Cache hits/misses the cold run recorded (zero when un-cached).
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub object_store_bytes: u64,
    /// Derived: [`Self::object_store_get_requests`] over
    /// [`Self::segments_scanned`]. The "read sequences per segment" figure issue
    /// #693 is about, as a number the harness reports rather than one a reader
    /// divides by hand. Exactly 1.0 on the fast-path rows, which is the same
    /// statement as "the plan phase issued zero probes".
    pub reads_per_segment: f64,
    /// Derived: [`Self::object_store_bytes`] over
    /// [`ReportConfig::dataset_bytes`], i.e. how many times over the query read
    /// the dataset it scanned. 1.0 is "each byte fetched once".
    ///
    /// About 1.0 on the fast-path rows: each segment is read whole exactly once,
    /// and the only excess is the catalog traffic the same accounting counts.
    /// Since #739 the whole-object rows at or below the segment count take the
    /// fast path too, so they are also about 1.0. Only where blocks stripe
    /// across partitions -- the cached rows above the segment count, and any
    /// predicated query -- does the old `1 + min(partitions, blocks_per_segment)`
    /// law still describe it.
    pub bytes_amplification: f64,
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
    /// Blocks per segment on the fixture this row scanned, from the row's own
    /// readings. The ceiling on how many partitions can open one segment, and so
    /// the second term of block striping's
    /// `1 + min(partitions, blocks_per_segment)` law -- which now describes only
    /// the rows that still stripe (see [`Self::bytes_amplification`]).
    #[must_use]
    pub fn blocks_per_segment(&self) -> f64 {
        if self.segments_scanned == 0 {
            return 0.0;
        }
        self.blocks_total as f64 / self.segments_scanned as f64
    }

    fn failed(
        target_partitions: usize,
        cache_wired: bool,
        over_threshold: bool,
        scan_partitions: usize,
        error: String,
    ) -> Self {
        ComboResult {
            target_partitions,
            cache_wired,
            over_threshold,
            scan_partitions,
            non_empty_partitions: 0,
            segments_scanned: 0,
            blocks_total: 0,
            blocks_scanned: 0,
            object_store_get_requests: 0,
            cache_hits: 0,
            cache_misses: 0,
            object_store_bytes: 0,
            reads_per_segment: 0.0,
            bytes_amplification: 0.0,
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

impl Report {
    /// The single row at one point of the `(target_partitions x cache x
    /// over_threshold)` sweep, so a caller naming a configuration cannot
    /// silently read a neighbouring row's figures.
    #[must_use]
    pub fn combo(
        &self,
        target_partitions: usize,
        cache_wired: bool,
        over_threshold: bool,
    ) -> Option<&ComboResult> {
        self.combos.iter().find(|c| {
            c.target_partitions == target_partitions
                && c.cache_wired == cache_wired
                && c.over_threshold == over_threshold
        })
    }
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

/// The block-range threshold the `over_threshold = false` rows build their logs
/// fetcher with. No object can exceed `u64::MAX`, so those rows read whole
/// objects on the same dataset the `over_threshold` rows read in ranges, and the
/// two differ only in read shape.
pub const WHOLE_OBJECT_BLOCK_RANGE_THRESHOLD: u64 = u64::MAX;

/// The block-range threshold the `over_threshold = true` rows build their logs
/// fetcher with: ADR-0107's production default, which the fixture's objects
/// genuinely exceed rather than being routed around it.
pub const OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD: u64 = DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD;

/// Build the few-segment / many-block dataset, then sweep every
/// `(target_partitions x cache x over_threshold)` combination, returning the
/// full report.
pub async fn run(config: &LogsScanScalingConfig) -> Report {
    let store = Arc::clone(&config.store);
    let tenant = TenantId::new(format!("bench-tenant-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    let dataset = publish_dataset(store.as_ref(), &tenant, config).await;

    let planning = measure_planning_latency(Arc::clone(&store), tenant_hash).await;

    let mut combos = Vec::with_capacity(config.target_partitions.len() * 4);
    for &tp in &config.target_partitions {
        for cache_wired in [false, true] {
            for over_threshold in [false, true] {
                let result = run_combo(
                    Arc::clone(&store),
                    tenant_hash,
                    tp,
                    cache_wired,
                    over_threshold,
                    config.runs,
                    config.deadline,
                    &dataset,
                )
                .await;
                combos.push(result);
            }
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            query: QUERY.to_string(),
            segments: config.segments,
            records_per_object: config.records_per_object,
            block_target_records: config.block_target_records,
            body_bytes: config.body_bytes,
            total_records: dataset.total_records,
            dataset_bytes: dataset.total_bytes,
            min_object_bytes: dataset.min_object_bytes,
            target_partitions: config.target_partitions.clone(),
            over_threshold_block_range_threshold: OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD,
            whole_object_block_range_threshold: WHOLE_OBJECT_BLOCK_RANGE_THRESHOLD,
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
        if let Some((survivors, _, _)) = planned {
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
            .filter_map(|p| p.as_ref().map(|(s, _, _)| *s))
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

/// One `(target_partitions, cache_wired, over_threshold)` measurement.
#[allow(clippy::too_many_arguments)]
async fn run_combo(
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    target_partitions: usize,
    cache_wired: bool,
    over_threshold: bool,
    runs: usize,
    deadline: Duration,
    dataset: &Dataset,
) -> ComboResult {
    let total_records = dataset.total_records;
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
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                over_threshold,
                0,
                e.to_string(),
            );
        }
    };
    let mut logs_fetcher =
        LogSegmentFetcher::new(Arc::clone(&store)).with_block_range_threshold(if over_threshold {
            OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD
        } else {
            WHOLE_OBJECT_BLOCK_RANGE_THRESHOLD
        });
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
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                over_threshold,
                0,
                e.to_string(),
            );
        }
    };
    let planned = match executor
        .plan_pinned(tenant_hash, snapshot, QUERY, &accounting, &[])
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                over_threshold,
                0,
                e.to_string(),
            );
        }
    };
    let plan = match planned.create_physical_plan().await {
        Ok(p) => p,
        Err(e) => {
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                over_threshold,
                0,
                e.to_string(),
            );
        }
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
                over_threshold,
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
            return ComboResult::failed(
                target_partitions,
                cache_wired,
                over_threshold,
                scan_partitions,
                e,
            );
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
                    over_threshold,
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

    // The two derived figures, computed from this row's own readings so nobody
    // has to divide the report by hand: reads per segment, and how many times
    // over the query fetched the dataset it scanned.
    let reads_per_segment = if segments_scanned == 0 {
        0.0
    } else {
        object_store_get_requests as f64 / segments_scanned as f64
    };
    let bytes_amplification = if dataset.total_bytes == 0 {
        0.0
    } else {
        object_store_bytes as f64 / dataset.total_bytes as f64
    };

    ComboResult {
        target_partitions,
        cache_wired,
        over_threshold,
        scan_partitions,
        non_empty_partitions,
        segments_scanned,
        blocks_total,
        blocks_scanned,
        object_store_get_requests,
        cache_hits,
        cache_misses,
        object_store_bytes,
        reads_per_segment,
        bytes_amplification,
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

/// What the fixture published: the record count the query scans, and the byte
/// count [`ComboResult::bytes_amplification`] divides by.
struct Dataset {
    total_records: usize,
    total_bytes: u64,
    min_object_bytes: u64,
}

/// Pseudo-random printable filler of `len` bytes, from a SplitMix64 stream
/// seeded by `seed`.
///
/// The body dominates an RLOG record, and the writer compresses it: a repeated
/// or templated body would collapse back under
/// [`OVER_THRESHOLD_BLOCK_RANGE_THRESHOLD`] and quietly turn the
/// `over_threshold` rows into whole-object rows with a different label.
fn filler(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push(ALPHABET[(z & 63) as usize] as char);
    }
    out
}

/// The record body: the same human-readable head this bench always used, padded
/// with [`filler`] to `body_bytes`.
fn body_for(ts: i64, body_bytes: usize) -> String {
    let head = format!("request {ts} ok");
    match body_bytes.checked_sub(head.len() + 1) {
        Some(pad) if pad > 0 => format!("{head} {}", filler(ts as u64, pad)),
        _ => head,
    }
}

/// Write `config.segments` RLOG objects, each carrying `records_per_object`
/// records under `config.block_target_records`-record blocks, and publish each
/// one's commit record so a real `Catalog::resolve` finds it. Returns the record
/// and byte totals the sweep measures against.
async fn publish_dataset(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    config: &LogsScanScalingConfig,
) -> Dataset {
    let segments = config.segments.max(1);
    let per_object = config.records_per_object.max(1);
    let tenant_hash = tenant.hash();
    let cfg = RlogConfig {
        block_target_records: config.block_target_records.max(1),
        ..RlogConfig::default()
    };
    let mut total = 0usize;
    let mut total_bytes = 0u64;
    let mut min_object_bytes = u64::MAX;
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
                    body: body_for(ts, config.body_bytes),
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
        // Mechanical drift guard. This bench's scan reaches the object through
        // its own trailer, so a wrong declaration here does not change what it
        // reads and the smoke test passes either way. Comparing the declaration
        // against the trailer the writer just produced is the one place both
        // are visible, so the drift is caught at the write site instead.
        //
        // Scope note, because the inverse would be a serious claim: the field
        // is NOT dropped in general. It is carried from the commit record
        // through fold.rs and snapshot_resolve.rs onto the SegmentRef, and
        // ravel-sql reads it to choose the whole-segment ranged route
        // (logs_scan.rs, open_by_column_chunk). A declaration that lies there
        // does change which read path runs.
        let declared_segment_format_version = u32::from(ravel_logseg::footer::VERSION);
        assert_eq!(
            u32::from(ravel_logseg::footer::trailer_version(&bytes).expect("trailer version")),
            declared_segment_format_version,
            "RlogWriter trailer must match the declared segment_format_version"
        );
        total_bytes += bytes.len() as u64;
        min_object_bytes = min_object_bytes.min(bytes.len() as u64);
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
            segment_format_version: declared_segment_format_version,
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
    Dataset {
        total_records: total,
        total_bytes,
        min_object_bytes: if total_bytes == 0 {
            0
        } else {
            min_object_bytes
        },
    }
}
