//! Bucketed final aggregation (ADR-3565 decision 1, issue #3608).
//!
//! A `FinalPartitioned` aggregate over unique-ish keys builds one hash table
//! per partition as large as the partition's groups, and every probe into it
//! misses cache. [`BucketedFinalAggregateExec`] replaces it, per partition, with
//! two phases:
//!
//! 1. **Single table, then route.** It merges its input into one
//!    `GroupValues` table and one accumulator set, exactly as the stock final,
//!    until the table holds `engage_groups` groups. It then takes the table's
//!    state (`GroupsAccumulator::state` and `GroupValues::emit`, both
//!    `EmitTo::All`) and routes those rows, and every later input row, into
//!    `2^bits` buckets chosen by the top `bits` bits of the key hash. The hash
//!    uses [`BUCKET_HASH_SEED`], not the repartition exchange's seed 0: with
//!    the exchange's own hash every row of a partition would share its low
//!    bits, and the buckets would be correlated with the partition. A bucket
//!    stores its rows column by column (keys, then every accumulator's state)
//!    in chunks of `routed_chunk_rows` rows, so its slack is at most one chunk
//!    per column.
//! 2. **One bucket at a time.** When input ends it merges bucket 0 into a
//!    reused table and accumulator set, evaluates and emits it, releases the
//!    bucket's chunks, clears the table with `clear_shrink`, and moves to
//!    bucket 1. A partition that never reaches `engage_groups` evaluates its
//!    single table instead, as the stock final does.
//!
//! The bucket is a function of the key alone, so every group lands in exactly
//! one bucket and each bucket's groups are complete when input ends: each
//! bucket runs the same `merge_batch` and `evaluate` over the same state the
//! stock final would, and the answers are the same up to row order
//! (`answers_match_the_stock_final_at_1_4_and_32_partitions` in
//! `tests/bucketed_final_aggregate.rs`).
//!
//! # What qualifies
//!
//! [`BucketedFinalAggregateRule::admits`] is the whole predicate: a
//! `FinalPartitioned` aggregate with no limit, linear input order and no
//! dynamic filter; one grouping set with no `__grouping_id` key; every key a
//! fixed-width integer, date, timestamp, time or interval; every aggregate
//! with a `GroupsAccumulator`, no ordering requirement, and state fields that
//! are fixed-width integer, temporal or `Decimal128`, never float. Anything
//! else keeps the stock plan (`every_refusal_condition_leaves_the_plan_unchanged`).
//!
//! The rule runs before `CompactSlices`, which wraps only `AggregateExec`, so
//! a converted aggregate gets neither half of that rule: the slice compaction
//! (redundant, every batch emitted here owns its buffers) and the TopK
//! pre-filter, which the per-bucket top-k below replaces.
//!
//! # Per-bucket top-k (ADR-3565 decision 4)
//!
//! The rule walks the plan top down with the TopK pre-filter's reach rule,
//! [`TopKPrefilter::above`]: a `SortExec` with a fetch starts a TopK, a
//! `ProjectionExec` passing its sort keys as bare columns maps it through, a
//! fetch-less `CoalescePartitionsExec` forwards it, and any other node ends
//! it. When the TopK reaches a converted aggregate and every sort key is one
//! of its output columns, the exec keeps it (`topk=[...], k=...` on its
//! `EXPLAIN` line), and in phase 2 cuts each bucket's evaluated output to
//! the k rows that sort first under that ordering before emitting it,
//! counting the rest in `topk_rows_dropped`. A partition that never engaged
//! is emitted uncut; the TopK above handles its one table.
//!
//! The cut is exact for the reason the pre-filter's is: a bucket's groups are
//! complete when it is evaluated, so a row outside its bucket's top k has k
//! rows ahead of it that reach the same TopK and cannot be in its result. At
//! a tie on the k-th value, which tied rows survive may differ from the stock
//! plan, as SQL allows (`per_bucket_topk_is_exact_on_ties`). The sort above
//! then sees at most `2^bits x k` rows per engaged partition.
//!
//! The cut shrinks the charged pending output of a bucket to at most k rows
//! while it drains (`per_bucket_topk_releases_the_pending_bucket_output`).
//! The table that output came from was charged at its full size just before,
//! so the cut is not a bound on peak memory.
//!
//! # Memory
//!
//! One consumer per partition, named `BucketedFinalAggregate[p]`, outside
//! every aggregate prefix `crate::memory` recognises, so it is charged one to
//! one with no reserve-ahead factor and no held release. Every bucket chunk is
//! charged at its allocated capacity before a row is written to it; the
//! tables, the per-batch scratch and a bucket's pending output are charged by
//! their `size()` after each batch. One transient is not charged: under a
//! TopK, the evaluated bucket batch and the sort's index vector are held
//! uncharged while `cut_to_topk` runs, at most one bucket's evaluated output
//! per partition, and only the cut result is charged as pending output.
//! Without spill, a refused grow fails the stream with the pool's
//! `ResourcesExhausted`, prefixed with the consumer's name. Every byte is
//! released as its bucket is emitted, and all of it when the stream drops.
//!
//! # Bucket spill (ADR-3565 amendment 2)
//!
//! [`BucketedFinalAggregateExec::with_spill`] is the mode the refusal retry is
//! to run the exec in; nothing outside tests sets it until T3 #4075 wires it.
//! With it, and a disk manager that allows temp files, the consumer
//! registers `with_can_spill(true)` and a refused charge is a spill trigger
//! instead of an error. The stream writes every chunk it retains, for
//! every bucket it has not started aggregating, to that bucket's scratch
//! file, releases their charge and asks again; from then on each chunk is
//! written when it fills and each bucket's last chunk when input ends. A
//! refusal while the stream still holds its stock table first routes that
//! table's state straight to the files in slices of `routed_chunk_rows`
//! rows, under the table's own charge and with no grow, then asks again.
//! When input ends each bucket is read back from its file, aggregated alone,
//! emitted, and its file deleted before the next bucket starts, so every
//! routed row is written at most once and nothing is merged. A refusal with
//! nothing left to write fails the stream with `ResourcesExhausted` naming
//! [`BUCKET_SPILL_UNDERSHOOT`] when the pool refused over the spill-mode cap
//! or a fair share of it; every other refusal, the spill-mode hard bound
//! among them, passes through with its own message. A chunk refused with
//! nothing retained is not a failure: that bucket's rows of the batch are
//! written to its file directly, as the forced route writes them. Scratch
//! files come
//! from the session's disk manager, so its quota and its typed failures
//! apply, and a dropped stream deletes them.
//!
//! # Yielding and output
//!
//! The stream returns `Pending` (after waking itself) after every input batch
//! it merges or routes, and emits each bucket before it aggregates the next.
//! A bucket with no output (empty, or cut to nothing at `k = 0`) returns
//! `Pending` the same way, so one poll aggregates at most one bucket, and a
//! dropped stream or a breached ceiling acts within one batch in phase 1
//! and one bucket in phase 2. Every output batch is a copy of at most
//! `batch_size` rows: no operator above holds a slice that pins a whole
//! bucket's buffers. No output is emitted before input ends.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, MutableArrayData, UInt32Array, make_array,
};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use datafusion::arrow::compute::{lexsort_to_indices, take, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::util::bit_util;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::spill_file::SpillFile;
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{Aggregate, EmitTo, GroupsAccumulator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    evaluate_many,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
    SpillMetrics,
};
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, InputOrderMode, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, SpillManager, Statistics, StatisticsArgs,
};
use futures::{Stream, StreamExt};

use crate::bucket_spill_tuning::routed_column_capacities;
use crate::compact_slices::TopKPrefilter;
use crate::config::BucketedFinalAggregation;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const BUCKETED_FINAL_AGGREGATE_RULE: &str = "bucketed_final_aggregate";

/// The name prefix of the exec's per-partition memory consumer:
/// `"BucketedFinalAggregate[{partition}]"`.
pub const BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX: &str = "BucketedFinalAggregate[";

/// The largest `bits` the exec honours; a larger configured value is read as
/// this. 65,536 buckets per partition is already far past the point where a
/// bucket's table fits a cache level.
pub const BUCKETED_FINAL_AGGREGATE_MAX_BITS: u8 = 16;

/// The seed of the bucket hash. Any value but the repartition exchange's 0
/// (`REPARTITION_RANDOM_STATE`) decorrelates the bucket from the partition.
const BUCKET_HASH_SEED: u64 = 0x5241_5645_4c42_4b54;

/// The phrase a bucket spill's terminal refusal carries in its
/// `ResourcesExhausted` message: the stream had nothing left to write and a
/// bucket still did not fit (ADR-3565 amendment 2 item 5).
pub const BUCKET_SPILL_UNDERSHOOT: &str = "bucket spill under-shoot";

/// The phrases of the spill-mode pool's refusals over the spill-mode cap and
/// over a consumer's fair share of it (`memory.rs`): the only refusals a
/// stream with nothing left to write labels as an under-shoot. Every other
/// refusal (the hard bound, the query pool, the tenant, process and host
/// gates) passes through with its own message.
const SPILL_MODE_SHARE_PHRASES: [&str; 2] = ["over the spill-mode cap", "over its fair share"];

/// The name of the exec's counter of the Arrow bytes decoded from its scratch
/// files on read-back: buffer lengths after decompression, which the
/// lz4_frame-compressed file bytes DataFusion's `spilled_bytes` counts do not
/// equal.
pub const BUCKET_SPILL_BYTES_READ: &str = "bucket_spill_bytes_read";

/// The name of the exec's counter of the rows decoded from its scratch files
/// on read-back.
pub const BUCKET_SPILL_ROWS_READ: &str = "bucket_spill_rows_read";

/// The name of the exec's counter of the batches (IPC messages) appended to
/// its scratch files.
pub const BUCKET_SPILL_BATCHES_WRITTEN: &str = "bucket_spill_batches_written";

/// Replaces every qualifying `FinalPartitioned` [`AggregateExec`] with a
/// [`BucketedFinalAggregateExec`]. See the module docs.
#[derive(Debug)]
pub struct BucketedFinalAggregateRule {
    config: BucketedFinalAggregation,
}

impl BucketedFinalAggregateRule {
    /// A rule that builds every exec it plans with `config`.
    pub fn new(config: BucketedFinalAggregation) -> Self {
        BucketedFinalAggregateRule { config }
    }

    /// Whether `aggregate` qualifies for the bucketed final (ADR-3565
    /// decision 1's conditions). See the module docs.
    pub fn admits(aggregate: &AggregateExec) -> bool {
        if *aggregate.mode() != AggregateMode::FinalPartitioned
            || aggregate.limit_options().is_some()
            || *aggregate.input_order_mode() != InputOrderMode::Linear
            || !aggregate.dynamic_expressions_produced().is_empty()
        {
            return false;
        }
        let group_by = aggregate.group_expr();
        let single_set = matches!(group_by.groups(), [set] if set.iter().all(|null| !null));
        if group_by.has_grouping_set()
            || !single_set
            || group_by.expr().is_empty()
            || !group_by.null_expr().is_empty()
        {
            return false;
        }
        let Ok(group_schema) = group_by.group_schema(&aggregate.input().schema()) else {
            return false;
        };
        let keys_qualify = group_schema.fields().iter().all(|field| {
            field.name() != Aggregate::INTERNAL_GROUPING_ID && is_bucketed_key(field.data_type())
        });
        keys_qualify
            && aggregate.aggr_expr().iter().all(|aggr| {
                aggr.groups_accumulator_supported()
                    && aggr.order_bys().is_empty()
                    && aggr.state_fields().is_ok_and(|fields| {
                        fields
                            .iter()
                            .all(|field| is_bucketed_state(field.data_type()))
                    })
            })
    }

    /// Rewrite `node` and its subtree, children first. `topk_above` is the
    /// TopK `node`'s output reaches, in `node`'s output columns, under
    /// [`TopKPrefilter::above`]'s reach rule (the one `CompactSlices` uses);
    /// a converted aggregate cuts each bucket to it.
    fn rewrite(
        &self,
        node: Arc<dyn ExecutionPlan>,
        topk_above: Option<TopKPrefilter>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let topk_below = TopKPrefilter::above(&node, topk_above.as_ref());
        let children = node.children();
        let mut rewritten = Vec::with_capacity(children.len());
        let mut changed = false;
        for child in children {
            let new_child = self.rewrite(Arc::clone(child), topk_below.clone())?;
            changed |= !Arc::ptr_eq(child, &new_child);
            rewritten.push(new_child);
        }
        let node = if changed {
            node.replace_children(
                rewritten,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?
        } else {
            node
        };
        let Some(aggregate) = node.downcast_ref::<AggregateExec>() else {
            return Ok(node);
        };
        if !Self::admits(aggregate) {
            return Ok(node);
        }
        let exec = BucketedFinalAggregateExec::new(Arc::new(aggregate.clone()), self.config)
            .with_topk(topk_above);
        Ok(Arc::new(exec))
    }
}

impl PhysicalOptimizerRule for BucketedFinalAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.rewrite(plan, None)
    }

    fn name(&self) -> &str {
        BUCKETED_FINAL_AGGREGATE_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A group key type the exec buckets: a fixed-width integer or temporal type
/// `new_group_values` keeps in a primitive table. Decimal128 and Duration
/// also dispatch to one and are left out until measured (ADR-3565).
fn is_bucketed_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Interval(_)
    )
}

/// An accumulator state type the exec routes: fixed width and not float, so
/// a chunk copies it by bytes and no float sum is reassociated.
fn is_bucketed_state(data_type: &DataType) -> bool {
    is_bucketed_key(data_type)
        || matches!(
            data_type,
            DataType::Duration(_) | DataType::Decimal128(_, _)
        )
}

/// The two-phase final aggregate. One child, the replaced aggregate's
/// partitioning, output schema and input distribution. See the module docs.
pub struct BucketedFinalAggregateExec {
    aggregate: Arc<AggregateExec>,
    config: BucketedFinalAggregation,
    topk: Option<TopKPrefilter>,
    spill: bool,
    metrics: ExecutionPlanMetricsSet,
}

impl BucketedFinalAggregateExec {
    /// Run `aggregate`, a `FinalPartitioned` aggregate that
    /// [`BucketedFinalAggregateRule::admits`], in two phases under `config`,
    /// with no per-bucket top-k. A stream over an aggregate that does not
    /// qualify fails with an internal error on its first batch rather than
    /// misreading a column.
    pub fn new(aggregate: Arc<AggregateExec>, config: BucketedFinalAggregation) -> Self {
        BucketedFinalAggregateExec {
            aggregate,
            config,
            topk: None,
            spill: false,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Spill routed state to the session's disk manager on a refused charge
    /// (the module docs' bucket spill), the refusal retry's mode. Without a
    /// disk manager that can create files the exec runs as without spill.
    pub fn with_spill(mut self, spill: bool) -> Self {
        self.spill = spill;
        self
    }

    /// Whether the exec spills on a refused charge.
    pub fn spill(&self) -> bool {
        self.spill
    }

    /// Whether a partition of this exec has executed with spill on and a
    /// disk manager that allows temp files: `execute` registers the spill
    /// counters only then. False before the first `execute`.
    pub fn spill_available(&self) -> bool {
        self.metrics
            .clone_inner()
            .iter()
            .any(|metric| metric.value().name() == BUCKET_SPILL_BYTES_READ)
    }

    /// Cut each routed bucket's output to its own top `topk.fetch()` rows
    /// under `topk.ordering()` (see the module docs). A TopK whose sort keys
    /// are not all bare columns of this exec's output, by index and name, is
    /// dropped, as is `None`: every row is then emitted.
    pub fn with_topk(mut self, topk: Option<TopKPrefilter>) -> Self {
        self.topk = topk.filter(|topk| topk.fits(&self.aggregate.schema()));
        self
    }

    /// The TopK each routed bucket is cut to, if any.
    pub fn topk(&self) -> Option<&TopKPrefilter> {
        self.topk.as_ref()
    }

    /// The aggregate this exec replaced, whose input, expressions and
    /// properties it runs with.
    pub fn aggregate(&self) -> &Arc<AggregateExec> {
        &self.aggregate
    }

    /// The tuning this exec runs with.
    pub fn config(&self) -> BucketedFinalAggregation {
        self.config
    }
}

/// The tuning and the bytes read back of the spilling bucketed execs in a
/// plan, which the spill retry's report carries as its bucket phase beside
/// the typed spill metrics `accumulate_spill_counts` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BucketSpillFigures {
    pub(crate) bits: u8,
    pub(crate) routed_chunk_rows: usize,
    pub(crate) bytes_read: u64,
    /// Whether any of them executed with a disk manager that allows temp
    /// files ([`BucketedFinalAggregateExec::spill_available`]).
    pub(crate) disk: bool,
}

/// [`BucketSpillFigures`] of `plan`, or `None` when no bucketed exec in it
/// runs with spill on.
pub(crate) fn bucket_spill_figures(plan: &Arc<dyn ExecutionPlan>) -> Option<BucketSpillFigures> {
    let mut found = None;
    collect_bucket_spill_figures(plan, &mut found);
    found
}

fn collect_bucket_spill_figures(
    plan: &Arc<dyn ExecutionPlan>,
    found: &mut Option<BucketSpillFigures>,
) {
    if let Some(exec) = plan.downcast_ref::<BucketedFinalAggregateExec>()
        && exec.spill()
    {
        let bytes_read = plan.metrics().map_or(0, |metrics| {
            metrics
                .iter()
                .filter_map(|metric| match metric.value() {
                    MetricValue::Count { name, count }
                        if name.as_ref() == BUCKET_SPILL_BYTES_READ =>
                    {
                        Some(count.value() as u64)
                    }
                    _ => None,
                })
                .sum()
        });
        let figures = found.get_or_insert(BucketSpillFigures {
            bits: exec.config.bits,
            routed_chunk_rows: exec.config.routed_chunk_rows,
            bytes_read: 0,
            disk: false,
        });
        figures.bytes_read += bytes_read;
        figures.disk |= exec.spill_available();
    }
    for child in plan.children() {
        collect_bucket_spill_figures(child, found);
    }
}

impl fmt::Debug for BucketedFinalAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BucketedFinalAggregateExec")
    }
}

impl DisplayAs for BucketedFinalAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let keys: Vec<&str> = self
                    .aggregate
                    .group_expr()
                    .expr()
                    .iter()
                    .map(|(_, name)| name.as_str())
                    .collect();
                let aggregates: Vec<&str> = self
                    .aggregate
                    .aggr_expr()
                    .iter()
                    .map(|aggr| aggr.name())
                    .collect();
                write!(
                    f,
                    "BucketedFinalAggregateExec: gby=[{}], aggr=[{}], engage_groups={}, \
                     bits={}, routed_chunk_rows={}",
                    keys.join(", "),
                    aggregates.join(", "),
                    self.config.engage_groups,
                    self.config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS),
                    self.config.routed_chunk_rows.max(1),
                )?;
                if let Some(topk) = &self.topk {
                    write!(f, ", topk=[{}], k={}", topk.ordering(), topk.fetch())?;
                }
                if self.spill {
                    write!(f, ", spill=true")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for BucketedFinalAggregateExec {
    fn name(&self) -> &'static str {
        "BucketedFinalAggregateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.aggregate.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.aggregate.children()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.aggregate.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.aggregate.benefits_from_input_partitioning()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        self.aggregate.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let rebuilt = Arc::clone(&self.aggregate).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let Some(aggregate) = rebuilt.downcast_ref::<AggregateExec>() else {
            return Err(DataFusionError::Internal(
                "BucketedFinalAggregateExec: the rebuilt aggregate is not an AggregateExec"
                    .to_string(),
            ));
        };
        Ok(Arc::new(
            BucketedFinalAggregateExec::new(Arc::new(aggregate.clone()), self.config)
                .with_topk(self.topk.clone())
                .with_spill(self.spill),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self
            .aggregate
            .input()
            .execute(partition, Arc::clone(&context))?;
        let spill =
            (self.spill && context.runtime_env().disk_manager.tmp_files_enabled()).then(|| {
                SpillCounters {
                    spill: SpillMetrics::new(&self.metrics, partition),
                    bytes_read: MetricBuilder::new(&self.metrics)
                        .counter(BUCKET_SPILL_BYTES_READ, partition),
                    rows_read: MetricBuilder::new(&self.metrics)
                        .counter(BUCKET_SPILL_ROWS_READ, partition),
                    batches_written: MetricBuilder::new(&self.metrics)
                        .counter(BUCKET_SPILL_BATCHES_WRITTEN, partition),
                }
            });
        let stream = BucketedStream::try_new(
            &self.aggregate,
            self.config,
            self.topk.clone(),
            input,
            partition,
            &context,
            BucketMetrics::new(&self.metrics, partition),
            spill,
        )?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.aggregate.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        self.aggregate.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.aggregate.cardinality_effect()
    }
}

/// What the exec reports through `EXPLAIN ANALYZE`, per partition.
struct BucketMetrics {
    baseline: BaselineMetrics,
    /// Buckets holding at least one routed row when input ended; zero for a
    /// partition that never engaged.
    buckets_non_empty: Count,
    /// Rows routed into buckets: the single table's state rows at
    /// engagement plus every later input row.
    routed_rows: Count,
    /// 1 when the partition's table reached `engage_groups` and routed, else
    /// 0.
    engaged: Count,
    /// Rows the per-bucket top-k cut from routed buckets' output; zero
    /// without a TopK and for a partition that never engaged.
    topk_rows_dropped: Count,
}

impl BucketMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        BucketMetrics {
            baseline: BaselineMetrics::new(metrics, partition),
            buckets_non_empty: MetricBuilder::new(metrics).counter("buckets_non_empty", partition),
            routed_rows: MetricBuilder::new(metrics).counter("routed_rows", partition),
            engaged: MetricBuilder::new(metrics).counter("engaged", partition),
            topk_rows_dropped: MetricBuilder::new(metrics).counter("topk_rows_dropped", partition),
        }
    }
}

/// One fixed-width column of an input batch, read row by row.
struct SourceColumn<'a> {
    values: &'a [u8],
    offset: usize,
    nulls: Option<&'a NullBuffer>,
}

/// One column of a bucket's open chunk: values and a validity bitmap,
/// allocated at the chunk's full row capacity.
struct ColumnChunk {
    data_type: DataType,
    width: usize,
    values: MutableBuffer,
    validity: MutableBuffer,
    null_count: usize,
}

impl ColumnChunk {
    fn finish(self, len: usize) -> DFResult<ArrayRef> {
        let nulls = (self.null_count > 0)
            .then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from(self.validity), 0, len)));
        let data = ArrayData::builder(self.data_type)
            .len(len)
            .add_buffer(Buffer::from(self.values))
            .nulls(nulls)
            .build()?;
        Ok(make_array(data))
    }
}

/// The type and width of every column a final `aggregate`'s stream routes:
/// its keys, then every accumulator's state, or the first type that is not
/// fixed width. The bucket spill derivation reads the widths through this
/// same function.
pub(crate) fn routed_types(
    aggregate: &AggregateExec,
) -> DFResult<Result<Vec<(DataType, usize)>, DataType>> {
    let input_schema = aggregate.input().schema();
    let group_by = aggregate.group_expr();
    let state_args = aggregate_expressions(
        aggregate.aggr_expr(),
        aggregate.mode(),
        group_by.num_group_exprs(),
    )?;
    let group_schema = group_by.group_schema(&input_schema)?;
    let mut types = Vec::new();
    for data_type in group_schema
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .chain(
            state_args
                .iter()
                .flatten()
                .map(|expr| expr.data_type(&input_schema))
                .collect::<DFResult<Vec<_>>>()?,
        )
    {
        let Some(width) = data_type.primitive_width() else {
            return Ok(Err(data_type));
        };
        types.push((data_type, width));
    }
    Ok(Ok(types))
}

/// A bucket's chunk still taking rows.
struct OpenChunk {
    columns: Vec<ColumnChunk>,
    len: usize,
    charged: usize,
}

impl OpenChunk {
    /// A chunk of `rows` rows per column, allocated but not yet charged.
    fn allocate(types: &[(DataType, usize)], rows: usize) -> Self {
        let columns: Vec<ColumnChunk> = types
            .iter()
            .map(|(data_type, width)| {
                let (values, validity) =
                    routed_column_capacities(*width, rows).unwrap_or((usize::MAX, usize::MAX));
                ColumnChunk {
                    data_type: data_type.clone(),
                    width: *width,
                    values: MutableBuffer::with_capacity(values),
                    validity: MutableBuffer::from_len_zeroed(validity),
                    null_count: 0,
                }
            })
            .collect();
        let charged = columns
            .iter()
            .map(|column| column.values.capacity() + column.validity.capacity())
            .sum();
        OpenChunk {
            columns,
            len: 0,
            charged,
        }
    }

    fn append(&mut self, sources: &[SourceColumn<'_>], rows: &[u32]) {
        for (column, source) in self.columns.iter_mut().zip(sources) {
            let width = column.width;
            for (at, &row) in rows.iter().enumerate() {
                let row = row as usize;
                let start = (source.offset + row) * width;
                column
                    .values
                    .extend_from_slice(&source.values[start..start + width]);
                if source.nulls.is_some_and(|nulls| nulls.is_null(row)) {
                    column.null_count += 1;
                } else {
                    bit_util::set_bit(column.validity.as_slice_mut(), self.len + at);
                }
            }
        }
        self.len += rows.len();
    }

    fn seal(self) -> DFResult<SealedChunk> {
        let len = self.len;
        let columns = self
            .columns
            .into_iter()
            .map(|column| column.finish(len))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(SealedChunk {
            columns,
            charged: self.charged,
        })
    }
}

/// A finished chunk: one array per routed column, and the bytes charged for
/// it.
struct SealedChunk {
    columns: Vec<ArrayRef>,
    charged: usize,
}

#[derive(Default)]
struct Bucket {
    sealed: Vec<SealedChunk>,
    open: Option<OpenChunk>,
    rows: usize,
}

/// The phase-1 router: every column's type and width, and the buckets.
struct Router {
    types: Vec<(DataType, usize)>,
    key_count: usize,
    bits: u8,
    chunk_rows: usize,
    random_state: RandomState,
    buckets: Vec<Bucket>,
    hashes: Vec<u64>,
    order: Vec<u32>,
    offsets: Vec<usize>,
    /// Each bucket's next slot in `order` while a batch is sorted, reused
    /// across batches.
    next: Vec<usize>,
    /// The most rows one call routes: the stream slices larger batches to
    /// it, so a router whose `hashes` and `order` were sized up front never
    /// grows them. `usize::MAX` after an engage, which sizes them per batch.
    scratch_rows: usize,
}

/// The per-row bytes of a router's `hashes` and `order`.
const ROUTER_ROW_SCRATCH: usize = size_of::<u64>() + size_of::<u32>();

impl Router {
    fn new(types: Vec<(DataType, usize)>, key_count: usize, bits: u8, chunk_rows: usize) -> Self {
        let bucket_count = 1usize << bits;
        Router {
            types,
            key_count,
            bits,
            chunk_rows,
            random_state: RandomState::with_seed(BUCKET_HASH_SEED),
            buckets: (0..bucket_count).map(|_| Bucket::default()).collect(),
            hashes: Vec::new(),
            order: Vec::new(),
            offsets: vec![0; bucket_count + 1],
            next: Vec::with_capacity(bucket_count + 1),
            scratch_rows: usize::MAX,
        }
    }

    /// Size `hashes` and `order` for `rows` rows now, and route at most that
    /// many rows per call from here on.
    fn fix_scratch(&mut self, rows: usize) {
        let rows = rows.max(1);
        self.hashes.clear();
        self.hashes.shrink_to(rows);
        self.hashes.reserve_exact(rows);
        self.order.clear();
        self.order.shrink_to(rows);
        self.order.reserve_exact(rows);
        self.scratch_rows = rows;
    }

    /// The bucket of a key hash: its top `bits` bits.
    fn bucket_of(&self, hash: u64) -> usize {
        match self.bits {
            0 => 0,
            bits => (hash >> (64 - u32::from(bits))) as usize,
        }
    }

    fn scratch_size(&self) -> usize {
        self.hashes.capacity() * size_of::<u64>()
            + self.order.capacity() * size_of::<u32>()
            + self.offsets.capacity() * size_of::<usize>()
            + self.next.capacity() * size_of::<usize>()
            + self.buckets.capacity() * size_of::<Bucket>()
    }

    /// Route `columns` (keys, then every accumulator's state, in the router's
    /// order) into the buckets, charging each new chunk to `reservation`
    /// before a row is written to it. With `spill`, a refused chunk is a
    /// spill trigger, and once one has fired every chunk is written when it
    /// fills.
    fn route(
        &mut self,
        columns: &[ArrayRef],
        reservation: &mut MemoryReservation,
        mut spill: Option<&mut BucketSpill>,
    ) -> DFResult<usize> {
        if columns.len() != self.types.len() {
            return Err(DataFusionError::Internal(format!(
                "BucketedFinalAggregateExec: {} routed columns, expected {}",
                columns.len(),
                self.types.len()
            )));
        }
        let rows = columns.first().map_or(0, |column| column.len());
        if rows == 0 {
            return Ok(0);
        }
        let data: Vec<ArrayData> = columns.iter().map(|column| column.to_data()).collect();
        let mut sources = Vec::with_capacity(columns.len());
        for ((column, data), (data_type, width)) in columns.iter().zip(&data).zip(&self.types) {
            let values = match data.buffers() {
                [values] => values.as_slice(),
                _ => &[],
            };
            if column.data_type() != data_type
                || column.len() != rows
                || values.len() < (data.offset() + rows) * width
            {
                return Err(DataFusionError::Internal(format!(
                    "BucketedFinalAggregateExec: a routed {} column of {} rows does not match \
                     the expected {data_type} column of {rows} rows",
                    column.data_type(),
                    column.len(),
                )));
            }
            sources.push(SourceColumn {
                values,
                offset: data.offset(),
                nulls: column.nulls(),
            });
        }

        self.sort_by_bucket(columns, rows)?;

        let bucket_count = self.buckets.len();
        for bucket in 0..bucket_count {
            let (start, end) = (self.offsets[bucket], self.offsets[bucket + 1]);
            let mut done = start;
            while done < end {
                if self.buckets[bucket]
                    .open
                    .as_ref()
                    .is_none_or(|open| open.len == self.chunk_rows)
                {
                    if let Some(full) = self.buckets[bucket].open.take() {
                        let sealed = full.seal()?;
                        match spill.as_deref_mut() {
                            Some(spill) if spill.triggered => {
                                spill.write(bucket, sealed.columns)?;
                                reservation.shrink(sealed.charged);
                            }
                            _ => self.buckets[bucket].sealed.push(sealed),
                        }
                    }
                    let chunk = OpenChunk::allocate(&self.types, self.chunk_rows);
                    if let Err(err) = try_grow_named(reservation, chunk.charged) {
                        let Some(spill) = spill.as_deref_mut() else {
                            return Err(err);
                        };
                        if !is_refusal(&err) {
                            return Err(err);
                        }
                        self.spill_retained(0, reservation, spill)?;
                        match try_grow_named(reservation, chunk.charged) {
                            Ok(()) => {}
                            Err(err) if is_refusal(&err) => {
                                // Not even one chunk fits with nothing
                                // retained: the bucket's rows of this batch go
                                // to its file uncharged, as the forced route
                                // writes them.
                                drop(chunk);
                                self.write_rows(bucket, columns, done, end, spill)?;
                                done = end;
                                continue;
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    self.buckets[bucket].open = Some(chunk);
                }
                let target = &mut self.buckets[bucket];
                let Some(open) = target.open.as_mut() else {
                    return Err(DataFusionError::Internal(
                        "BucketedFinalAggregateExec: a bucket has no open chunk".to_string(),
                    ));
                };
                let take = (end - done).min(self.chunk_rows - open.len);
                open.append(&sources, &self.order[done..done + take]);
                target.rows += take;
                done += take;
            }
        }
        Ok(rows)
    }

    /// Hash `rows` rows of `columns`' keys and counting-sort the row indices
    /// by bucket into `order`, bucket `b`'s at `offsets[b]..offsets[b + 1]`.
    fn sort_by_bucket(&mut self, columns: &[ArrayRef], rows: usize) -> DFResult<()> {
        self.hashes.clear();
        self.hashes.resize(rows, 0);
        create_hashes(
            &columns[..self.key_count.min(columns.len())],
            &self.random_state,
            &mut self.hashes,
        )?;
        let bucket_count = self.buckets.len();
        self.offsets.clear();
        self.offsets.resize(bucket_count + 1, 0);
        for index in 0..rows {
            let bucket = self.bucket_of(self.hashes[index]);
            self.offsets[bucket + 1] += 1;
        }
        for bucket in 0..bucket_count {
            self.offsets[bucket + 1] += self.offsets[bucket];
        }
        self.order.clear();
        self.order.resize(rows, 0);
        self.next.clear();
        self.next.extend_from_slice(&self.offsets);
        for index in 0..rows {
            let bucket = self.bucket_of(self.hashes[index]);
            self.order[self.next[bucket]] = index as u32;
            self.next[bucket] += 1;
        }
        Ok(())
    }

    /// Split `columns` (at most one slice of the stock table's state) by
    /// bucket and write each bucket's rows to its file as one piece, with no
    /// chunk and no charge: the phase 1 trigger's forced route.
    fn write_slice(&mut self, columns: &[ArrayRef], spill: &mut BucketSpill) -> DFResult<()> {
        let rows = columns.first().map_or(0, |column| column.len());
        if rows == 0 {
            return Ok(());
        }
        self.sort_by_bucket(columns, rows)?;
        for bucket in 0..self.buckets.len() {
            let (start, end) = (self.offsets[bucket], self.offsets[bucket + 1]);
            self.write_rows(bucket, columns, start, end, spill)?;
        }
        Ok(())
    }

    /// Write the rows `order[start..end]` of `columns`, all of `bucket`, to
    /// its file as one piece, with no chunk and no charge.
    fn write_rows(
        &mut self,
        bucket: usize,
        columns: &[ArrayRef],
        start: usize,
        end: usize,
        spill: &mut BucketSpill,
    ) -> DFResult<()> {
        if start >= end {
            return Ok(());
        }
        let indices = UInt32Array::from(self.order[start..end].to_vec());
        let piece = columns
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        spill.write(bucket, piece)?;
        self.buckets[bucket].rows += end - start;
        Ok(())
    }

    /// Whether any bucket from `from` on still holds a chunk in memory.
    fn retains(&self, from: usize) -> bool {
        self.buckets
            .iter()
            .skip(from)
            .any(|bucket| !bucket.sealed.is_empty() || bucket.open.is_some())
    }

    /// Write every chunk held by the buckets from `from` on to their files
    /// and release their charge; from here on each chunk is written when it
    /// fills.
    fn spill_retained(
        &mut self,
        from: usize,
        reservation: &mut MemoryReservation,
        spill: &mut BucketSpill,
    ) -> DFResult<()> {
        spill.triggered = true;
        for (bucket, target) in self.buckets.iter_mut().enumerate().skip(from) {
            let mut chunks = std::mem::take(&mut target.sealed);
            if let Some(open) = target.open.take() {
                chunks.push(open.seal()?);
            }
            for chunk in chunks {
                spill.write(bucket, chunk.columns)?;
                reservation.shrink(chunk.charged);
            }
        }
        Ok(())
    }

    /// One chunk of `bucket` still in memory, the open one sealed last, or
    /// `None` once it holds none.
    fn pop_chunk(&mut self, bucket: usize) -> DFResult<Option<SealedChunk>> {
        let Some(target) = self.buckets.get_mut(bucket) else {
            return Ok(None);
        };
        if let Some(chunk) = target.sealed.pop() {
            return Ok(Some(chunk));
        }
        target.open.take().map(OpenChunk::seal).transpose()
    }
}

/// One bucket's scratch file while it is written: `Some(batch)` appends,
/// `None` finishes the file and hands it over (`None` again when nothing was
/// written). DataFusion's in-progress spill file is not nameable outside its
/// crate, so the closure holds it.
type BucketWriter =
    Box<dyn FnMut(Option<&RecordBatch>) -> DFResult<Option<Arc<dyn SpillFile>>> + Send>;

/// The spill counters of one partition: DataFusion's typed spill metrics,
/// which `accumulate_spill_counts` reads, the bytes and rows decoded on
/// read-back, and the batches written.
struct SpillCounters {
    spill: SpillMetrics,
    bytes_read: Count,
    rows_read: Count,
    batches_written: Count,
}

/// A stream's bucket spill: the spill manager and each bucket's file.
struct BucketSpill {
    manager: SpillManager,
    schema: SchemaRef,
    writers: Vec<Option<BucketWriter>>,
    /// Set once a trigger has fired: every chunk is written when it fills.
    triggered: bool,
    bytes_read: Count,
    rows_read: Count,
    batches_written: Count,
}

impl BucketSpill {
    /// Append `columns`, one piece of `bucket`'s rows, to its file, creating
    /// the file on its first write.
    fn write(&mut self, bucket: usize, columns: Vec<ArrayRef>) -> DFResult<()> {
        let rows = columns.first().map_or(0, |column| column.len());
        if rows == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let Some(slot) = self.writers.get_mut(bucket) else {
            return Err(DataFusionError::Internal(format!(
                "BucketedFinalAggregateExec: no scratch file slot for bucket {bucket}"
            )));
        };
        let writer = match slot {
            Some(writer) => writer,
            None => {
                let mut file = self
                    .manager
                    .create_in_progress_file("BucketedFinalAggregate bucket spill")?;
                slot.insert(Box::new(move |batch: Option<&RecordBatch>| match batch {
                    Some(batch) => file.append_batch(batch).map(|_| None),
                    None => file.finish(),
                }))
            }
        };
        writer(Some(&batch))?;
        self.batches_written.add(1);
        Ok(())
    }

    /// Finish `bucket`'s file and hand it over; `None` when it has none.
    fn finish(&mut self, bucket: usize) -> DFResult<Option<Arc<dyn SpillFile>>> {
        match self.writers.get_mut(bucket).and_then(Option::take) {
            Some(mut writer) => writer(None),
            None => Ok(None),
        }
    }
}

/// A bucket's file being read back, kept until its last batch is merged and
/// dropped, which deletes it, before the next bucket starts.
struct ReadBack {
    bucket: usize,
    stream: SendableRecordBatchStream,
    _file: Arc<dyn SpillFile>,
}

/// The Arrow bytes `batch` decodes to: each buffer's length and `ceil(n/8)`
/// per validity bitmap, not the capacity of the allocation a buffer shares,
/// and not the compressed bytes in the file.
fn decoded_bytes(batch: &RecordBatch) -> usize {
    fn data_bytes(data: &ArrayData) -> usize {
        data.buffers().iter().map(Buffer::len).sum::<usize>()
            + data.nulls().map_or(0, |nulls| nulls.len().div_ceil(8))
            + data.child_data().iter().map(data_bytes).sum::<usize>()
    }
    batch
        .columns()
        .iter()
        .map(|column| data_bytes(&column.to_data()))
        .sum()
}

/// Whether `err` is a pool refusal, the only error a spill answers.
fn is_refusal(err: &DataFusionError) -> bool {
    matches!(err, DataFusionError::ResourcesExhausted(_))
}

/// A refusal over the spill-mode cap or a fair share of it with nothing left
/// to write, typed as the bucket spill's under-shoot with its predicate;
/// every other refusal passes through unchanged.
fn undershoot(err: DataFusionError) -> DataFusionError {
    match err {
        DataFusionError::ResourcesExhausted(message)
            if SPILL_MODE_SHARE_PHRASES
                .iter()
                .any(|phrase| message.contains(phrase)) =>
        {
            DataFusionError::ResourcesExhausted(format!(
                "{message}; {BUCKET_SPILL_UNDERSHOOT}: the stream had no routed state left to \
                 write and a bucket's table did not fit its share, which the derived tuning \
                 allows only when r x s > 2 (r: the table bytes all buckets needed over the \
                 projected T; s: the largest bucket's table bytes over its even part, \
                 T / (P x 2^bits))"
            ))
        }
        other => other,
    }
}

/// `reservation.try_grow(additional)`, with a refusal naming the consumer.
fn try_grow_named(reservation: &mut MemoryReservation, additional: usize) -> DFResult<()> {
    reservation.try_grow(additional).map_err(|err| match err {
        DataFusionError::ResourcesExhausted(message) => {
            DataFusionError::ResourcesExhausted(format!(
                "{} refused {additional} bytes: {message}",
                reservation.consumer().name()
            ))
        }
        other => other,
    })
}

/// A bucket's evaluated output, handed out `batch_size` rows at a time.
struct PendingOutput {
    batch: RecordBatch,
    offset: usize,
}

enum Phase {
    Ingest,
    Drain { next_bucket: usize },
    Done,
}

struct BucketedStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    group_by: PhysicalGroupBy,
    state_args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    group_values: Box<dyn GroupValues>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_indices: Vec<usize>,
    /// Every routed column's type and width: keys, then each accumulator's
    /// state.
    routed_types: Vec<(DataType, usize)>,
    key_count: usize,
    router: Option<Router>,
    pending: Option<PendingOutput>,
    phase: Phase,
    config: BucketedFinalAggregation,
    /// The TopK each routed bucket's output is cut to before it is emitted.
    topk: Option<TopKPrefilter>,
    batch_size: usize,
    reservation: MemoryReservation,
    /// The part of the reservation charged by `size()`: tables, scratch and
    /// pending output. The rest is bucket chunks, charged at capacity.
    table_bytes: usize,
    metrics: BucketMetrics,
    /// The bucket spill, in spill mode only.
    spill: Option<BucketSpill>,
    /// The bucket file being read back in phase 2, if any.
    reading: Option<ReadBack>,
    /// The size of the read-back batch being merged, charged with the table.
    read_back_bytes: usize,
}

impl BucketedStream {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        aggregate: &AggregateExec,
        config: BucketedFinalAggregation,
        topk: Option<TopKPrefilter>,
        input: SendableRecordBatchStream,
        partition: usize,
        context: &Arc<TaskContext>,
        metrics: BucketMetrics,
        spill: Option<SpillCounters>,
    ) -> DFResult<Self> {
        let input_schema = aggregate.input().schema();
        let group_by = aggregate.group_expr().clone();
        let state_args = aggregate_expressions(
            aggregate.aggr_expr(),
            aggregate.mode(),
            group_by.num_group_exprs(),
        )?;
        let accumulators = aggregate
            .aggr_expr()
            .iter()
            .map(|aggr| aggr.create_groups_accumulator())
            .collect::<DFResult<Vec<_>>>()?;
        let group_schema = group_by.group_schema(&input_schema)?;
        let key_count = group_schema.fields().len();
        let routed_types = routed_types(aggregate)?.map_err(|data_type| {
            DataFusionError::Internal(format!(
                "BucketedFinalAggregateExec: routed column type {data_type} is not fixed width"
            ))
        })?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let spill = spill.map(|counters| {
            let schema = Arc::new(Schema::new(
                routed_types
                    .iter()
                    .enumerate()
                    .map(|(index, (data_type, _))| {
                        Field::new(format!("routed_{index}"), data_type.clone(), true)
                    })
                    .collect::<Vec<_>>(),
            ));
            let bits = config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS);
            BucketSpill {
                manager: SpillManager::new(
                    context.runtime_env(),
                    counters.spill,
                    Arc::clone(&schema),
                )
                .with_compression_type(context.session_config().spill_compression()),
                schema,
                writers: (0..1usize << bits).map(|_| None).collect(),
                triggered: false,
                bytes_read: counters.bytes_read,
                rows_read: counters.rows_read,
                batches_written: counters.batches_written,
            }
        });
        let reservation = MemoryConsumer::new(format!(
            "{BUCKETED_FINAL_AGGREGATE_CONSUMER_PREFIX}{partition}]"
        ))
        .with_can_spill(spill.is_some())
        .register(context.memory_pool());
        Ok(BucketedStream {
            schema: aggregate.schema(),
            input: Some(input),
            group_by,
            state_args,
            group_values,
            accumulators,
            group_indices: Vec::new(),
            routed_types,
            key_count,
            router: None,
            pending: None,
            phase: Phase::Ingest,
            config,
            topk,
            batch_size: context.session_config().batch_size().max(1),
            reservation,
            table_bytes: 0,
            metrics,
            spill,
            reading: None,
            read_back_bytes: 0,
        })
    }

    /// Merge `columns` (keys, then state) into the reused table and
    /// accumulators.
    fn merge(&mut self, columns: &[ArrayRef]) -> DFResult<()> {
        let (keys, mut states) = columns.split_at(self.key_count.min(columns.len()));
        self.group_values.intern(keys, &mut self.group_indices)?;
        let total_groups = self.group_values.len();
        for (accumulator, args) in self.accumulators.iter_mut().zip(&self.state_args) {
            let (values, rest) = states.split_at(args.len().min(states.len()));
            accumulator.merge_batch(values, &self.group_indices, total_groups)?;
            states = rest;
        }
        Ok(())
    }

    /// Keys, then every accumulator's state columns, of one input batch.
    fn routed_columns(&self, batch: &RecordBatch) -> DFResult<Vec<ArrayRef>> {
        let Some(mut columns) = evaluate_group_by(&self.group_by, batch)?.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "BucketedFinalAggregateExec: the group by evaluated to no grouping set".to_string(),
            ));
        };
        for args in evaluate_many(&self.state_args, batch)? {
            columns.extend(args);
        }
        Ok(columns)
    }

    fn ingest(&mut self, batch: &RecordBatch) -> DFResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        let columns = self.routed_columns(batch)?;
        if let Some(router) = self.router.as_mut() {
            let rows = batch.num_rows();
            let step = router.scratch_rows.max(1);
            let mut start = 0;
            while start < rows {
                let len = step.min(rows - start);
                let slice: Vec<ArrayRef> = if len == rows {
                    columns.clone()
                } else {
                    columns
                        .iter()
                        .map(|column| column.slice(start, len))
                        .collect()
                };
                let routed = router.route(&slice, &mut self.reservation, self.spill.as_mut())?;
                self.metrics.routed_rows.add(routed);
                start += len;
            }
        } else {
            self.merge(&columns)?;
            if self.group_values.len() >= self.config.engage_groups {
                self.engage()?;
            }
        }
        self.charge_table()
    }

    /// Take the single table's state and route it; every later batch is
    /// routed too.
    fn engage(&mut self) -> DFResult<()> {
        let bits = self.config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS);
        let mut router = Router::new(
            self.routed_types.clone(),
            self.key_count,
            bits,
            self.config.routed_chunk_rows.max(1),
        );
        let mut state = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            state.extend(accumulator.state(EmitTo::All)?);
        }
        self.group_values.clear_shrink(0);
        self.group_indices = Vec::new();
        let routed = router.route(&state, &mut self.reservation, self.spill.as_mut())?;
        self.metrics.routed_rows.add(routed);
        self.metrics.engaged.add(1);
        self.router = Some(router);
        Ok(())
    }

    /// The phase 1 trigger: take the stock table's state, as the engage
    /// does, and write it to the bucket files in slices of at most
    /// `routed_chunk_rows` rows, under the table's existing charge. Nothing
    /// is asked of the pool here; the caller asks again once the table's
    /// charge is recomputed.
    fn force_route(&mut self) -> DFResult<()> {
        let Some(spill) = self.spill.as_mut() else {
            return Err(DataFusionError::Internal(
                "BucketedFinalAggregateExec: a forced route without spill".to_string(),
            ));
        };
        let bits = self.config.bits.min(BUCKETED_FINAL_AGGREGATE_MAX_BITS);
        let slice_rows = self.config.routed_chunk_rows.max(1);
        let mut router = Router::new(self.routed_types.clone(), self.key_count, bits, slice_rows);
        let mut state = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            state.extend(accumulator.state(EmitTo::All)?);
        }
        let rows = state.first().map_or(0, |column| column.len());
        let mut start = 0;
        while start < rows {
            let len = slice_rows.min(rows - start);
            let slice: Vec<ArrayRef> = state
                .iter()
                .map(|column| column.slice(start, len))
                .collect();
            router.write_slice(&slice, spill)?;
            start += len;
        }
        drop(state);
        self.group_values.clear_shrink(0);
        self.group_indices = Vec::new();
        spill.triggered = true;
        // The router's per-row scratch is sized once, inside what the table
        // held, so routing later batches never asks the pool for it. A table
        // that held under `ROUTER_ROW_SCRATCH` spare bytes routes one row per
        // call: accepted, since a floor would be a grow the pool just refused.
        router.fix_scratch(0);
        let held = self
            .table_bytes
            .saturating_sub(self.table_size())
            .saturating_sub(router.scratch_size());
        router.fix_scratch((held / ROUTER_ROW_SCRATCH).min(slice_rows.max(self.batch_size)));
        self.metrics.routed_rows.add(rows);
        self.metrics.engaged.add(1);
        self.router = Some(router);
        Ok(())
    }

    /// Answer a refused charge: in spill mode, write what the stream retains
    /// for the buckets it has not started aggregating (or route the stock
    /// table to the files), so the caller can ask again; otherwise, or when
    /// nothing is left to write, the refusal is the stream's error.
    fn relieve(&mut self, err: DataFusionError) -> DFResult<()> {
        if self.spill.is_none() || !is_refusal(&err) {
            return Err(err);
        }
        if self.router.is_none() {
            if self.group_values.is_empty() {
                return Err(undershoot(err));
            }
            return self.force_route();
        }
        let from = match self.phase {
            Phase::Ingest => 0,
            Phase::Drain { next_bucket } => next_bucket,
            Phase::Done => usize::MAX,
        };
        let (Some(router), Some(spill)) = (self.router.as_mut(), self.spill.as_mut()) else {
            return Err(err);
        };
        if !router.retains(from) {
            return Err(undershoot(err));
        }
        router.spill_retained(from, &mut self.reservation, spill)
    }

    /// Bytes charged by `size()`: the tables, the per-batch scratch and the
    /// pending output.
    fn table_size(&self) -> usize {
        self.group_values.size()
            + self
                .accumulators
                .iter()
                .map(|accumulator| accumulator.size())
                .sum::<usize>()
            + self.group_indices.capacity() * size_of::<usize>()
            + self.router.as_ref().map_or(0, Router::scratch_size)
            + self
                .pending
                .as_ref()
                .map_or(0, |pending| pending.batch.get_array_memory_size())
            + self.read_back_bytes
    }

    fn charge_table(&mut self) -> DFResult<()> {
        loop {
            let size = self.table_size();
            if size <= self.table_bytes {
                self.reservation.shrink(self.table_bytes - size);
                self.table_bytes = size;
                return Ok(());
            }
            match try_grow_named(&mut self.reservation, size - self.table_bytes) {
                Ok(()) => {
                    self.table_bytes = size;
                    return Ok(());
                }
                Err(err) => self.relieve(err)?,
            }
        }
    }

    /// Start reading `bucket`'s file back, if it has one.
    fn start_read_back(&mut self, bucket: usize) -> DFResult<bool> {
        let Some(spill) = self.spill.as_mut() else {
            return Ok(false);
        };
        let Some(file) = spill.finish(bucket)? else {
            return Ok(false);
        };
        let stream = spill
            .manager
            .read_spill_as_stream_unbuffered(Arc::clone(&file), None)?;
        self.reading = Some(ReadBack {
            bucket,
            stream,
            _file: file,
        });
        Ok(true)
    }

    /// Merge one batch read back from the current bucket's file, charged
    /// while it is merged, and count it as decoded.
    fn merge_read_back(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        self.read_back_bytes = batch.get_array_memory_size();
        if let Some(spill) = self.spill.as_ref() {
            spill.bytes_read.add(decoded_bytes(batch));
            spill.rows_read.add(batch.num_rows());
        }
        self.charge_table()?;
        self.merge(batch.columns())?;
        self.read_back_bytes = 0;
        self.charge_table()
    }

    /// Evaluate the reused table into the pending output and clear it,
    /// keeping room for `keep_groups` groups. With `cut` and a TopK set, the
    /// pending output is only the evaluated rows' own top k.
    fn evaluate_table(&mut self, keep_groups: usize, cut: bool) -> DFResult<()> {
        if self.group_values.is_empty() {
            return Ok(());
        }
        let mut columns = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.evaluate(EmitTo::All)?);
        }
        let mut batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.group_values.clear_shrink(keep_groups);
        if cut && let Some(topk) = &self.topk {
            let evaluated = batch.num_rows();
            batch = cut_to_topk(batch, topk)?;
            self.metrics
                .topk_rows_dropped
                .add(evaluated - batch.num_rows());
        }
        if batch.num_rows() > 0 {
            self.pending = Some(PendingOutput { batch, offset: 0 });
        }
        Ok(())
    }

    /// Merge the chunks one bucket still holds in memory, after its file if
    /// it had one, release each as it is merged, and evaluate the bucket into
    /// the pending output, cut to the TopK when one is set. The bucket's
    /// groups are complete here: every input row was routed before phase 2
    /// began.
    fn aggregate_bucket(&mut self, bucket: usize) -> DFResult<()> {
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        loop {
            let chunk = match self.router.as_mut() {
                Some(router) => router.pop_chunk(bucket)?,
                None => None,
            };
            let Some(chunk) = chunk else {
                break;
            };
            self.merge(&chunk.columns)?;
            drop(chunk.columns);
            self.reservation.shrink(chunk.charged);
            self.charge_table()?;
        }
        let groups = self.group_values.len();
        self.evaluate_table(groups, true)?;
        self.charge_table()
    }

    /// The next `batch_size` rows of the pending output, copied so the batch
    /// owns its buffers.
    fn next_output(&mut self) -> DFResult<Option<RecordBatch>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let total = pending.batch.num_rows();
        let len = (total - pending.offset).min(self.batch_size);
        let batch = copy_rows(&pending.batch, pending.offset, len)?;
        pending.offset += len;
        if pending.offset >= total {
            self.pending = None;
            self.charge_table()?;
        }
        self.metrics.baseline.record_output(batch.num_rows());
        Ok(Some(batch))
    }

    fn finish_input(&mut self) -> DFResult<()> {
        self.input = None;
        match self.router.as_mut() {
            Some(router) => {
                let non_empty = router
                    .buckets
                    .iter()
                    .filter(|bucket| bucket.rows > 0)
                    .count();
                self.metrics.buckets_non_empty.add(non_empty);
                // After a trigger each bucket's last, partly filled chunk
                // goes to its file, so every bucket is read back whole.
                match self.spill.as_mut() {
                    Some(spill) if spill.triggered => {
                        router.spill_retained(0, &mut self.reservation, spill)
                    }
                    _ => Ok(()),
                }
            }
            None => {
                self.evaluate_table(0, false)?;
                self.charge_table()
            }
        }
    }

    /// Drop every buffer and release the whole reservation.
    fn release(&mut self) {
        self.input = None;
        self.router = None;
        self.pending = None;
        self.reading = None;
        self.spill = None;
        self.read_back_bytes = 0;
        self.group_values.clear_shrink(0);
        self.accumulators.clear();
        self.group_indices = Vec::new();
        self.reservation.free();
        self.table_bytes = 0;
        self.phase = Phase::Done;
        self.metrics.baseline.done();
    }

    fn fail(&mut self, err: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.release();
        Poll::Ready(Some(Err(err)))
    }
}

/// The `topk.fetch()` rows of `batch` that sort first under `topk`'s
/// ordering, taken into fresh buffers; `batch` itself when it has no more
/// rows than that. Among rows tied with the k-th, which survive is
/// unspecified. `batch` and the sort's index vector are held uncharged for
/// the duration of this call: the bucket's table was charged in full before
/// evaluation, and only the cut result is charged as pending output, so the
/// asked-bytes figure under-reports the transient by at most one bucket's
/// evaluated output per partition.
fn cut_to_topk(batch: RecordBatch, topk: &TopKPrefilter) -> DFResult<RecordBatch> {
    if batch.num_rows() <= topk.fetch() {
        return Ok(batch);
    }
    let columns = topk
        .ordering()
        .iter()
        .map(|sort| sort.evaluate_to_sort_column(&batch))
        .collect::<DFResult<Vec<_>>>()?;
    let indices = lexsort_to_indices(&columns, Some(topk.fetch()))?;
    Ok(take_record_batch(&batch, &indices)?)
}

/// `len` rows of `batch` from `offset`, copied into fresh buffers.
fn copy_rows(batch: &RecordBatch, offset: usize, len: usize) -> DFResult<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            let mut copy = MutableArrayData::new(vec![&data], false, len);
            copy.try_extend(0, offset, offset + len)?;
            Ok(make_array(copy.freeze()))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(len)),
    )?)
}

impl Stream for BucketedStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            match this.phase {
                Phase::Ingest => {
                    let Some(input) = this.input.as_mut() else {
                        this.phase = Phase::Drain { next_bucket: 0 };
                        continue;
                    };
                    match ready!(input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            if let Err(err) = this.ingest(&batch) {
                                return this.fail(err);
                            }
                            // Yield after every batch, so a dropped stream
                            // or a breached ceiling acts within one batch.
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Some(Err(err)) => return this.fail(err),
                        None => {
                            if let Err(err) = this.finish_input() {
                                return this.fail(err);
                            }
                            this.phase = Phase::Drain { next_bucket: 0 };
                        }
                    }
                }
                Phase::Drain { next_bucket } => {
                    match this.next_output() {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => {}
                        Err(err) => return this.fail(err),
                    }
                    if let Some(reading) = this.reading.as_mut() {
                        let bucket = reading.bucket;
                        match ready!(reading.stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => {
                                if let Err(err) = this.merge_read_back(&batch) {
                                    return this.fail(err);
                                }
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            Some(Err(err)) => return this.fail(err),
                            None => {
                                // Dropping the read-back deletes the file
                                // before the next bucket starts.
                                this.reading = None;
                                if let Err(err) = this.aggregate_bucket(bucket) {
                                    return this.fail(err);
                                }
                                if this.pending.is_none() {
                                    cx.waker().wake_by_ref();
                                    return Poll::Pending;
                                }
                                continue;
                            }
                        }
                    }
                    let bucket_count = this
                        .router
                        .as_ref()
                        .map_or(0, |router| router.buckets.len());
                    if next_bucket >= bucket_count {
                        this.release();
                        return Poll::Ready(None);
                    }
                    this.phase = Phase::Drain {
                        next_bucket: next_bucket + 1,
                    };
                    match this.start_read_back(next_bucket) {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(err) => return this.fail(err),
                    }
                    if let Err(err) = this.aggregate_bucket(next_bucket) {
                        return this.fail(err);
                    }
                    // A bucket with output yields by returning its first
                    // batch; one without yields here, so one poll walks at
                    // most one bucket however many are empty.
                    if this.pending.is_none() {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Phase::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for BucketedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::bucket_spill_tuning::routed_chunk_charge;

    const POOL: &str = "query memory pool spill threshold reached: ";

    fn refused(rest: &str) -> DataFusionError {
        DataFusionError::ResourcesExhausted(format!("{POOL}{rest}"))
    }

    fn labelled(err: &DataFusionError) -> bool {
        err.to_string().contains(BUCKET_SPILL_UNDERSHOOT)
    }

    /// Only the spill-mode pool's cap and fair-share refusals are labelled;
    /// the hard bound, the query pool and the gates keep their own message.
    #[test]
    fn undershoot_labels_cap_and_share_refusals_only() {
        let cap = refused(
            "spill-capable consumers would hold 901 bytes charged and enveloped, over the \
             spill-mode cap 900 (per-query limit 1000 less headroom 100)",
        );
        let share = refused(
            "BucketedFinalAggregate[0] would hold 701 bytes charged and enveloped, over its \
             fair share 450 of the spill-mode cap 900 (2 spill-capable consumers; per-query \
             limit 1000 less headroom 100)",
        );
        assert!(labelled(&undershoot(cap)));
        assert!(labelled(&undershoot(share)));

        let hard = refused(
            "spill-capable consumers would hold 951 bytes charged and enveloped, over the \
             spill-mode hard bound 950 (per-query limit 1000 less half the headroom 50)",
        );
        let passes = [
            hard,
            DataFusionError::ResourcesExhausted(
                "query memory pool exhausted: 2048 bytes asked".to_string(),
            ),
            refused("process memory budget exhausted for this aggregate's reserve-ahead envelope"),
            DataFusionError::Internal("over its fair share".to_string()),
        ];
        for err in passes {
            let before = err.to_string();
            let after = undershoot(err);
            assert!(!labelled(&after), "{after}");
            assert_eq!(after.to_string(), before);
        }
    }

    /// The derivation charges a routed chunk as `OpenChunk::allocate`
    /// charges it, for every width the routed types take and row counts on
    /// and off the 64-byte and 8-row boundaries.
    #[test]
    fn routed_chunk_charge_is_the_allocated_charge() {
        let types = [
            (DataType::Int8, 1),
            (DataType::Int16, 2),
            (DataType::UInt32, 4),
            (DataType::Int64, 8),
            (DataType::Decimal128(38, 4), 16),
            (DataType::Decimal256(76, 4), 32),
        ];
        for (data_type, width) in &types {
            assert_eq!(data_type.primitive_width(), Some(*width), "{data_type}");
        }
        for rows in [1, 7, 8, 9, 63, 64, 65, 1023, 1024, 2368, 8192] {
            for set in [&types[..], &types[3..4], &types[1..3], &types[4..]] {
                let widths: Vec<usize> = set.iter().map(|(_, width)| *width).collect();
                assert_eq!(
                    Some(OpenChunk::allocate(set, rows).charged),
                    routed_chunk_charge(&widths, rows),
                    "{rows} rows of {widths:?}"
                );
            }
        }
    }
}
