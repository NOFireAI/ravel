//! Issue #680: a high-cardinality aggregate's pre-final group state must not
//! scale with the scan's partition count.
//!
//! DataFusion builds one partial-aggregation hash table per input partition and
//! merges them in a single final stage. When a group key's distinct values all
//! appear in every partition -- which is the normal case for a
//! high-cardinality key over a tenant's objects -- the pre-final state is
//! roughly `partitions x distinct` entries, not `distinct`. Spilling is
//! disabled by design (ADR-0102 decision 3, ADR-0013), so that multiplier does
//! not degrade into slow, it fails the query with a typed pool error.
//!
//! `ravel_bench::groupby_scaling::run_distinct` measured it over the `logs`
//! table across `D` in {10k, 100k, 1M} and `target_partitions` in {1, 4, 16,
//! 32}: the 32-partition peak ran 5.0x to 16.2x the single-partition peak for
//! the identical dataset and query. The fix is
//! `SqlConfig::skip_partial_aggregation` (on by default), which tightens
//! DataFusion's two skip-partial-aggregation probe thresholds in
//! `session_config` so a partial partition stops building a hash table once its
//! probe shows the key does not reduce.
//!
//! # Row counts, not bytes
//!
//! What this test pins is the probe's decision, not the memory pool's
//! high-water mark. A peak-bytes figure depends on how many partial hash tables
//! happen to be simultaneously resident, which is a scheduling property: it
//! moves with machine load and partition timing, and an earlier version of this
//! test that pinned a ratio of peak-byte figures passed on an idle box and
//! failed on loaded 4-core CI runners.
//!
//! The probe itself is deterministic. Each partition decides from its own first
//! [`SKIP_PARTIAL_AGGREGATION_PROBE_ROWS`] rows and the distinct ratio within
//! them, so over a fixed fixture the decision, and every row count that follows
//! from it, is the same on every machine under every load. Three figures come
//! out of DataFusion's own execution metrics on the grouping `Partial`
//! `AggregateExec`, summed across its partitions:
//!
//! * `output_rows` (the `BaselineMetrics` counter every operator publishes),
//! * `skipped_aggregation_rows` (the counter `GroupedHashAggregateStream`
//!   increments for each row it forwards without aggregating, which is zero
//!   exactly when no partition's probe ever fired),
//! * `output_rows - skipped_aggregation_rows`, the rows the partial stage
//!   emitted out of its hash tables, which is the number of group entries those
//!   tables held. That last figure is the quantity issue #680 is about: what
//!   the pre-final state costs, counted in entries instead of bytes.
//!
//! Its input row count is `output_rows` of its child, and the fixture's own
//! [`TOTAL_ROWS`] independently.
//!
//! Prove-the-test: the single flipped line is `skip_partial_aggregation` in
//! `SqlConfig` (crates/ravel-sql/src/config.rs), which gates the
//! `options.execution.skip_partial_aggregation_probe_{rows,ratio}_threshold`
//! writes in `session_config` (crates/ravel-sql/src/session.rs). Stop writing
//! either threshold and the on-side session becomes DataFusion's stock
//! 0.8-after-100,000 probe, which this fixture never reaches: every partition
//! then aggregates, `skipped_aggregation_rows` falls to 0, `output_rows` falls
//! from the input row count to one row per distinct value per partition, and
//! the first assertion below fails.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::any::Any;
use std::sync::Arc;

use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, displayable, execute_stream,
};
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    LogsTableProvider, SKIP_PARTIAL_AGGREGATION_PROBE_ROWS, SessionTable, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

/// Distinct values of the high-cardinality key.
///
/// Every object writes the same `DISTINCT` values in the same order, [`REPEATS`]
/// whole rounds of them, and the scan spreads each object's blocks over every
/// partition, so each partition ends up holding all `DISTINCT` values in its own
/// hash table. That repetition across partitions is what makes the pre-final
/// state `partitions x DISTINCT` instead of `DISTINCT`, and it is the shape
/// issue #680 is about. It also has to stay above
/// `2 x SKIP_PARTIAL_AGGREGATION_PROBE_ROWS`, or one full-sized per-partition
/// table would fit inside [`MAX_TIGHTENED_GROUP_ENTRIES`] and the bound this
/// test holds the tightened session to would prove nothing.
const DISTINCT: usize = 25_000;

/// How many times each object repeats its full run of [`DISTINCT`] values.
///
/// Whole rounds, never interleaved, and `RlogConfig::default()` targets 8192
/// records per block, so a block is a run of 8192 consecutive positions and
/// therefore 8192 distinct values. A partition's first batch is one such block:
/// the probe reads a ratio of 1.0 over its prefix, met with room to spare rather
/// than by a hair, and because that prefix is all-distinct the tightened partial
/// stage emits exactly as many rows as it aggregated. That is what makes its
/// total `output_rows` its input row count exactly, not approximately.
///
/// Three, not two, and the reason is the scan's block-to-partition rule: global
/// block `i` goes to partition `i % target_partitions` (ADR-0102, `logs_scan`
/// module docs). Two rounds put 8 blocks in an object, a multiple of the four
/// partitions, so every partition draws the same object-local block offsets from
/// every object and sees only the slice of the value space those offsets cover;
/// measured, the pre-final state fell to 41,072 entries instead of
/// `4 x DISTINCT`. Three rounds put 10 blocks in an object, the offsets rotate
/// between objects, and each partition covers the whole value space. Beyond
/// that, more rounds only push [`MAX_ROWS_PER_PARTITION`] towards DataFusion's stock
/// 100,000-row threshold, which it must stay clear of.
const REPEATS: usize = 3;

/// RLOG objects.
///
/// Also the fan-out the scan reaches: with an un-cached fetcher `LogsScanExec`
/// keeps the segment-count bound (`min(target_partitions, segment_count)`,
/// ADR-0102 decision 1).
///
/// Four rather than more. [`MAX_TIGHTENED_GROUP_ENTRIES`] grows with the
/// partition count while [`MAX_ROWS_PER_PARTITION`] must stay under DataFusion's
/// stock 100,000-row probe threshold, and past five partitions no fixture size
/// satisfies both.
const OBJECTS: usize = 4;

/// The scan's fan-out. Equal to [`OBJECTS`] so the fanned-out side really
/// reaches this many partitions.
const FANNED_OUT_PARTITIONS: usize = OBJECTS;

/// Rows in one object.
const ROWS_PER_OBJECT: usize = DISTINCT * REPEATS;

/// Rows in the whole fixture, and so the partial stage's input row count.
const TOTAL_ROWS: usize = ROWS_PER_OBJECT * OBJECTS;

/// Records per RLOG block, which is `RlogConfig::default().block_target_records`.
/// [`block_size_matches_the_writer`] holds the two together.
const BLOCK_RECORDS: usize = 8192;

/// Blocks the scan hands to the busiest partition, which owns every
/// `FANNED_OUT_PARTITIONS`-th block of the global list (ADR-0102; see the
/// `logs_scan` module docs).
const MAX_BLOCKS_PER_PARTITION: usize =
    (ROWS_PER_OBJECT.div_ceil(BLOCK_RECORDS) * OBJECTS).div_ceil(FANNED_OUT_PARTITIONS);

/// The most rows any one partition of the fanned-out plan can hold.
///
/// An upper bound, not an average: an object's last block is short, and which
/// partitions those short blocks land on depends on the block arithmetic. Every
/// partition holds at most [`MAX_BLOCKS_PER_PARTITION`] full blocks.
///
/// This is the number that has to stay below [`STOCK_PROBE_ROWS_THRESHOLD`],
/// and it is what makes the option-off side deterministic rather than merely
/// likely: the stock probe never gets to evaluate a ratio at all, so no
/// partition skips, whatever the key looks like.
const MAX_ROWS_PER_PARTITION: usize = MAX_BLOCKS_PER_PARTITION * BLOCK_RECORDS;

/// DataFusion's own default for
/// `datafusion.execution.skip_partial_aggregation_probe_rows_threshold`, which
/// is what the option-off session runs with.
/// [`stock_probe_threshold_matches_datafusion`] holds the two together.
const STOCK_PROBE_ROWS_THRESHOLD: usize = 100_000;

const _: () = assert!(
    MAX_ROWS_PER_PARTITION < STOCK_PROBE_ROWS_THRESHOLD,
    "a partition must hold fewer rows than the stock probe threshold, or the \
     option-off session's probe fires too and both sides of this test skip"
);
const _: () = assert!(
    DISTINCT > 2 * SKIP_PARTIAL_AGGREGATION_PROBE_ROWS,
    "one full-sized per-partition table must not fit inside the bound the \
     tightened session is held to, or that bound proves nothing"
);

/// The largest number of group entries the tightened partial stage may hold
/// across all partitions.
///
/// Structural, not tuned. A partition stops building its table as soon as the
/// probe fires, and the probe fires on the first batch that takes the partition
/// to [`SKIP_PARTIAL_AGGREGATION_PROBE_ROWS`] rows. That batch can carry a full
/// `batch_size` of its own, and DataFusion's `batch_size` default is the same
/// 8192, so a partition's table can reach two probe thresholds' worth of
/// entries before it is frozen, and no more. Note what does not appear in this
/// bound: [`DISTINCT`]. That is the whole point of the fix.
const MAX_TIGHTENED_GROUP_ENTRIES: usize =
    2 * SKIP_PARTIAL_AGGREGATION_PROBE_ROWS * FANNED_OUT_PARTITIONS;

const SCOPE_NAME: &str = "skip-partial-aggregation-test";
const SCOPE_VERSION: &str = "1.0";
const TENANT: TenantHash = TenantHash([9u8; 16]);

/// `COUNT(DISTINCT high)` over the `logs` table: the shape of the ClickBench
/// statements that fail (q05/q06), and the one that puts every distinct value
/// into a group key. DataFusion rewrites it into a `GROUP BY` over the key
/// under a scalar count, so the plan below that count is exactly the
/// partial/final pair issue #680 is about.
const QUERY: &str = "SELECT count(DISTINCT attrs['u']) AS distinct_high FROM logs";

/// Write [`OBJECTS`] RLOG objects into `store`, each carrying all [`DISTINCT`]
/// values [`REPEATS`] times over, and return a snapshot over them.
async fn write_high_cardinality_logs(store: &Arc<dyn ObjectStoreBackend>) -> Snapshot {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("hits".to_string()),
    )];
    let stream_id = log_stream_id(&resource, SCOPE_NAME, SCOPE_VERSION, &[]);
    let stream_attrs = stream_attrs_bytes(&resource, SCOPE_NAME, SCOPE_VERSION, &[]);

    let mut segments = Vec::with_capacity(OBJECTS);
    let mut ts = 0i64;
    for object in 0..OBJECTS {
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: *Uuid::from_u128(0x680_0000 + object as u128).as_bytes(),
            writer_epoch: 1,
            writer_seq: object as u64 + 1,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        let min_ts = ts;
        for _round in 0..REPEATS {
            for value in 0..DISTINCT {
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
                        flags: 0,
                        attrs: vec![("u".to_string(), AttrValue::Str(format!("u{value:012}")))],
                    })
                    .expect("push");
                ts += 1;
            }
        }
        let bytes = writer.finish().expect("finish");
        let size = bytes.len() as u64;
        let key = format!("logs/skip-partial-{object}.rlog");
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        segments.push(SegmentRef {
            data_object_key: key,
            object_size: size,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: ts - 1,
            ingest_hour_bucket: 0,
            sample_count: ROWS_PER_OBJECT as u64,
            series_count: 1,
            shard: 0,
            content_hash: [object as u8; 32],
            writer_id: Uuid::from_u128(0x680_0000 + object as u128),
            writer_epoch: 1,
            writer_seq: object as u64 + 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        });
    }

    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The `LogsScanExec` output partition count of `plan`, read from the real
/// plan's properties.
fn logs_scan_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.name() == "LogsScanExec" {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(logs_scan_partitions)
        .max()
        .unwrap_or(0)
}

/// Row counts read off the grouping `Partial` `AggregateExec` of an executed
/// plan, summed over its partitions.
#[derive(Debug, Default)]
struct PartialAggregate {
    /// How many grouping `Partial` `AggregateExec` nodes were found. The plan
    /// this test asserts over has exactly one; the scalar `Partial` count above
    /// it has no group key, no hash table, and no probe, so it is not counted.
    nodes: usize,
    /// `BaselineMetrics::output_rows`: every row the partial stage emitted,
    /// whether it came out of a hash table or was forwarded unaggregated.
    output_rows: usize,
    /// `skipped_aggregation_rows`: rows forwarded without being aggregated.
    /// Exactly zero when no partition's probe ever fired.
    skipped_rows: usize,
    /// `BaselineMetrics::output_rows` of the child, which is the partial
    /// stage's input row count.
    input_rows: usize,
}

impl PartialAggregate {
    /// Rows the partial stage emitted out of its hash tables, which is the
    /// number of group entries those tables held.
    fn group_entries(&self) -> usize {
        self.output_rows - self.skipped_rows
    }
}

/// Sum [`PartialAggregate`] over `plan`. Call it only after the plan's stream
/// has been drained: DataFusion fills these metrics in as the operators run.
fn partial_aggregate_rows(plan: &Arc<dyn ExecutionPlan>) -> PartialAggregate {
    let mut totals = PartialAggregate::default();
    for child in plan.children() {
        let below = partial_aggregate_rows(child);
        totals.nodes += below.nodes;
        totals.output_rows += below.output_rows;
        totals.skipped_rows += below.skipped_rows;
        totals.input_rows += below.input_rows;
    }

    let Some(aggregate) = (plan.as_ref() as &dyn Any).downcast_ref::<AggregateExec>() else {
        return totals;
    };
    if *aggregate.mode() != AggregateMode::Partial || aggregate.group_expr().is_empty() {
        return totals;
    }

    let metrics = plan
        .metrics()
        .expect("an AggregateExec publishes a MetricsSet");
    totals.nodes += 1;
    totals.output_rows += metrics
        .output_rows()
        .expect("an AggregateExec publishes BaselineMetrics::output_rows");
    totals.skipped_rows += metrics
        .sum_by_name("skipped_aggregation_rows")
        .expect(
            "a grouping Partial AggregateExec over an unordered single group set \
             publishes skipped_aggregation_rows",
        )
        .as_usize();
    totals.input_rows += plan
        .children()
        .iter()
        .map(|child| {
            child
                .metrics()
                .and_then(|metrics| metrics.output_rows())
                .expect("the child of a Partial AggregateExec publishes output_rows")
        })
        .sum::<usize>();
    totals
}

/// One run's observations.
struct Run {
    partial: PartialAggregate,
    scan_partitions: usize,
    /// The pool high-water mark. Diagnostic only: how many partial hash tables
    /// are simultaneously resident is scheduling-dependent, so no assertion may
    /// rest on this. It is printed because it is what a reader chasing #680
    /// wants to see next to the row counts.
    peak_bytes: u64,
    /// The single scalar the query returned, so a run that silently produced
    /// the wrong answer cannot contribute row counts.
    answer: i64,
    /// The executed plan with its metrics, printed alongside a failure.
    plan_with_metrics: String,
}

/// Run [`QUERY`] at `target_partitions` over the fixture, with
/// `skip_partial_aggregation` as given, and report what its partial stage did.
async fn run(
    store: &Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    target_partitions: usize,
    skip_partial_aggregation: bool,
) -> Run {
    let mut config = SqlConfig {
        skip_partial_aggregation,
        // Comfortably above anything this fixture reaches, so the run measures
        // what the plan builds instead of tripping the ceiling.
        max_query_bytes: 8 << 30,
        ..SqlConfig::default()
    };
    config.engine.fetch_concurrency = target_partitions;

    let accounting = QueryAccounting::new();
    let accountant = TenantMemoryAccountant::new(16 << 30);
    let (pool, _breach) = config.query_pool(accountant, accounting.clone());

    let provider = Arc::new(LogsTableProvider::new(
        snapshot,
        TENANT,
        LogSegmentFetcher::new(Arc::clone(store)),
        QueryAccounting::new(),
    ));
    let ctx =
        build_session(&config, pool, SessionTable::Logs(provider), false, false).expect("session");

    let plan = ctx
        .sql(QUERY)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let scan_partitions = logs_scan_partitions(&plan);

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).expect("execute");
    let mut answer = -1i64;
    while let Some(next) = stream.next().await {
        let batch = next.expect("batch");
        if batch.num_rows() == 1 {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("a count aggregate is Int64");
            answer = column.value(0);
        }
    }
    drop(stream);

    Run {
        partial: partial_aggregate_rows(&plan),
        scan_partitions,
        peak_bytes: accounting.snapshot().peak_intermediate_bytes,
        answer,
        plan_with_metrics: format!("{}", displayable(plan.as_ref()).indent(true)),
    }
}

/// Check the parts of a run that must hold whichever way the option is set: the
/// query answered correctly, the scan really fanned out, and the plan really
/// carries one grouping partial stage over the whole fixture.
fn check_run_shape(label: &str, observed: &Run) {
    assert_eq!(
        observed.answer, DISTINCT as i64,
        "{label}: the COUNT(DISTINCT) run must count every distinct value"
    );
    assert_eq!(
        observed.scan_partitions, FANNED_OUT_PARTITIONS,
        "{label}: target_partitions must reach LogsScanExec, else the two sides \
         of the comparison are the same plan"
    );
    assert_eq!(
        observed.partial.nodes, 1,
        "{label}: expected exactly one grouping Partial AggregateExec, found {}. \
         The row counts below are summed over those nodes, so a plan with a \
         different aggregate structure needs the test rewritten, not the numbers \
         adjusted.\n{}",
        observed.partial.nodes, observed.plan_with_metrics
    );
    assert_eq!(
        observed.partial.input_rows, TOTAL_ROWS,
        "{label}: the partial stage read {} rows, not the fixture's {TOTAL_ROWS}. \
         Every count below is stated against that input.\n{}",
        observed.partial.input_rows, observed.plan_with_metrics
    );
}

/// The pin, plus its own red demonstration, in one test.
///
/// Both runs go over one fixture at [`FANNED_OUT_PARTITIONS`], one with the
/// option on and one with it off, and every assertion is an exact equality or
/// an exact structural bound over row counts from DataFusion's metrics. Nothing
/// here depends on wall-clock time, on how the runtime interleaved the
/// partitions, or on the memory pool.
///
/// 1. With the option on, every partition's probe fires and the partial stage
///    forwards: its `output_rows` equals its input row count exactly, and the
///    rows it did aggregate fit in [`MAX_TIGHTENED_GROUP_ENTRIES`], a bound
///    that does not mention [`DISTINCT`].
/// 2. With the option off, no partition's probe fires at all: nothing is
///    skipped, and the partial stage emits one row per distinct value it saw
///    per partition, at most `FANNED_OUT_PARTITIONS x DISTINCT` and strictly
///    fewer than it read. Those emitted rows are the group entries the
///    pre-final state held, which is issue #680's multiplier stated in entries
///    instead of bytes.
/// 3. That off-side entry count exceeds the bound assertion 1 holds the on side
///    to, which is what keeps assertion 1 from being vacuous: the fixture has
///    to actually reproduce the unbounded multiplier for a bound on it to mean
///    anything.
#[tokio::test(flavor = "multi_thread")]
async fn high_cardinality_partial_aggregate_forwards_instead_of_building_tables() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = write_high_cardinality_logs(&store).await;

    let tightened = run(&store, snapshot.clone(), FANNED_OUT_PARTITIONS, true).await;
    let stock = run(&store, snapshot.clone(), FANNED_OUT_PARTITIONS, false).await;
    check_run_shape("tightened", &tightened);
    check_run_shape("stock", &stock);

    println!(
        "tightened: input_rows={} output_rows={} skipped={} group_entries={} \
         peak_bytes={}\n\
         stock:     input_rows={} output_rows={} skipped={} group_entries={} \
         peak_bytes={}",
        tightened.partial.input_rows,
        tightened.partial.output_rows,
        tightened.partial.skipped_rows,
        tightened.partial.group_entries(),
        tightened.peak_bytes,
        stock.partial.input_rows,
        stock.partial.output_rows,
        stock.partial.skipped_rows,
        stock.partial.group_entries(),
        stock.peak_bytes,
    );

    assert_eq!(
        tightened.partial.output_rows, tightened.partial.input_rows,
        "the tightened partial stage emitted {} rows for {} input rows. Every \
         partition's probe should have fired inside its all-distinct prefix and \
         forwarded the rest unaggregated, making the two equal. Fewer means \
         partitions were still building hash tables: check that session_config \
         still writes both \
         datafusion.execution.skip_partial_aggregation_probe_* thresholds when \
         SqlConfig::skip_partial_aggregation is on.\n{}",
        tightened.partial.output_rows, tightened.partial.input_rows, tightened.plan_with_metrics
    );
    assert!(
        tightened.partial.group_entries() <= MAX_TIGHTENED_GROUP_ENTRIES,
        "the tightened partial stage aggregated {} rows into group entries, above \
         the {MAX_TIGHTENED_GROUP_ENTRIES} its probe prefix allows \
         (2 x {SKIP_PARTIAL_AGGREGATION_PROBE_ROWS} x {FANNED_OUT_PARTITIONS}). \
         The pre-final state is following DISTINCT again, which is issue \
         #680.\n{}",
        tightened.partial.group_entries(),
        tightened.plan_with_metrics
    );

    assert_eq!(
        stock.partial.skipped_rows, 0,
        "the stock session skipped {} rows. Its {STOCK_PROBE_ROWS_THRESHOLD}-row \
         probe threshold sits above the {MAX_ROWS_PER_PARTITION} rows a partition \
         can hold here, so it must never fire; if it does, this fixture has \
         stopped reproducing the pre-fix behaviour and both it and the assertions \
         below need rewriting.\n{}",
        stock.partial.skipped_rows, stock.plan_with_metrics
    );
    assert!(
        stock.partial.output_rows <= FANNED_OUT_PARTITIONS * DISTINCT
            && stock.partial.output_rows < stock.partial.input_rows,
        "the stock partial stage emitted {} rows for {} input rows. Aggregating \
         every row, which is what it must do here, emits at most one row per \
         distinct value per partition ({FANNED_OUT_PARTITIONS} x {DISTINCT}) and \
         strictly fewer rows than it read.\n{}",
        stock.partial.output_rows,
        stock.partial.input_rows,
        stock.plan_with_metrics
    );

    assert!(
        stock.partial.group_entries() > MAX_TIGHTENED_GROUP_ENTRIES,
        "the stock session held only {} group entries, at or below the \
         {MAX_TIGHTENED_GROUP_ENTRIES} the tightened session is held to, so that \
         bound proves nothing. Either DataFusion now bounds the partial stage on \
         its own, or the fixture stopped giving every partition a full-sized \
         hash table; both the fixture and the bound need rewriting, not \
         relaxing.",
        stock.partial.group_entries()
    );
}

/// [`BLOCK_RECORDS`] is the fixture's block arithmetic, and both
/// [`MAX_ROWS_PER_PARTITION`] and the all-distinct probe prefix are derived from
/// it. A change to the writer's default would move both silently.
#[test]
fn block_size_matches_the_writer() {
    assert_eq!(
        RlogConfig::default().block_target_records,
        BLOCK_RECORDS,
        "the RLOG writer's default block size moved; MAX_ROWS_PER_PARTITION and \
         the probe prefix this test reasons about are both derived from it"
    );
}

/// [`STOCK_PROBE_ROWS_THRESHOLD`] is what makes the option-off side of the test
/// deterministic. If DataFusion lowers its default below
/// [`MAX_ROWS_PER_PARTITION`], the off side starts skipping too and the fixture
/// needs resizing, not the assertions relaxing.
#[test]
fn stock_probe_threshold_matches_datafusion() {
    let stock = datafusion::prelude::SessionConfig::new()
        .options()
        .execution
        .skip_partial_aggregation_probe_rows_threshold;
    assert_eq!(
        stock, STOCK_PROBE_ROWS_THRESHOLD,
        "DataFusion's default skip_partial_aggregation_probe_rows_threshold moved"
    );
}
