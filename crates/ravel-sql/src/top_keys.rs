//! Bound the final aggregate of `GROUP BY ... ORDER BY <group key> LIMIT n`
//! (issue #4004, ADR-4002).
//!
//! # The problem
//!
//! A statement that groups, orders by its group key and takes a limit builds
//! every group in its final aggregate before the sort above picks `n` of
//! them, so the final stage's memory grows with the number of distinct keys,
//! not with `n`. A best-n filter under the stock aggregate cannot fix that:
//! when keys arrive ever-better, each one is admitted once and the aggregate
//! still holds every group. The bound has to live in the aggregate.
//!
//! # The rewrite
//!
//! [`TopKeysAggregateLimit`] replaces a final-stage `AggregateExec` with a
//! [`TopKeysAggregateExec`] when all of these hold:
//!
//! - a `SortExec` with a fetch `n` in `1..=SqlConfig::bounded_top_keys_max_limit`
//!   and `preserve_partitioning = false` reads the aggregate through nothing
//!   but `ProjectionExec`, `CoalescePartitionsExec` and `CooperativeExec`
//!   nodes, none of which carries a fetch. A `GlobalLimitExec` between them
//!   belongs to a subquery whose skip could eat kept keys, and ends the walk;
//!   a statement's own `OFFSET s` is already in the sort's fetch of `n + s`;
//! - every sort expression is deterministic, is not a float, and reads only
//!   columns that resolve, by column index through each projection's
//!   expressions (never by name), to a non-float group-key column of the
//!   aggregate. A key, several keys, a prefix of the keys, a function of keys
//!   such as `date_trunc('hour', m)`, the tie-order rewrite's two-term sort
//!   and its `__ravel_tiebreak_*` project-through all resolve;
//! - the aggregate passes [`TopKeysAggregateLimit::admits`]: a final mode
//!   (`FinalPartitioned`, `Final`, `Single`, `SinglePartitioned`), linear
//!   input, one grouping set with no null mask and no grouping id, no limit
//!   and no dynamic filter, and at least one aggregate expression, each with
//!   a `GroupsAccumulator`, no `ORDER BY` and no `FILTER`, and either no
//!   float state field or a float state on the allowlist below. Its input is
//!   not a [`crate::FirstGroupsExec`].
//!
//! 0 turns the rule off. `EXPLAIN` prints the exec as
//! `TopKeysAggregateExec: n=<n>`.
//!
//! # The exec
//!
//! Each output partition holds a DataFusion `GroupValues` from the public
//! `new_group_values` (so two keys are one group exactly as in the replaced
//! aggregate), one `GroupsAccumulator` per aggregate expression, the sort
//! expressions over the group-key columns, an arrow-row `RowConverter` over
//! them with the sort's own `SortOptions`, and a threshold: the encoded sort
//! row of the worst key it kept at its last rebuild. For each input batch it
//! evaluates the keys, drops the rows whose sort row is after the threshold
//! (none before the first rebuild), interns the rest and merges their
//! partial states (`Final` modes) or updates from their rows (`Single`
//! modes). When it holds `2n` groups or more it emits every key and state,
//! ranks the groups by sort row and then by group index, keeps the first `n`,
//! rebuilds the table and fresh accumulators from them, and moves the
//! threshold to the n-th kept row. At end of input it evaluates every group,
//! applies the same cut once and emits at most `n` rows.
//!
//! # Why the answer is exact
//!
//! The sort value of a group is computed from its key alone, and the final
//! stage's hash distribution (or its single partition, in the `Final` and
//! `Single` modes) gives each partition every partial state or row of its
//! keys. A key evicted at a rebuild has `n` kept keys ranked ahead of it.
//! The threshold only moves forward, so its later rows pass only when its
//! sort value ties the threshold; it then re-enters with a group index above
//! every key already held, the kept keys get indexes 0 to n-1 before any
//! later row, and so `n` keys stay ranked ahead of it at every later cut:
//! it is absent from the output, never partial. Every group strictly better
//! than the global n-th sort value is therefore kept complete by its partition,
//! enough groups tied with it are kept complete to fill `n`, and the global
//! sort picks `n` complete groups. The mask keeps rows at the threshold, so
//! the worst kept key's later rows are never lost. Among keys whose sort
//! values tie, which survive is arrival order, which SQL leaves unspecified;
//! under the tie-order rewrite the sort is total on the keys and the answer
//! is fully determined. Float keys are refused because `GroupValues` folds
//! -0.0 into +0.0 while arrow-row orders them apart, so one group could have
//! two sort values.
//!
//! A rebuild merges each kept state into a fresh accumulator, so a state is
//! admitted only when that merge returns it bit for bit. Integer states are:
//! an integer sum or count seeded at 0 is unchanged by adding 0. A float
//! state generally is not, since a fresh sum seeded at `0.0` turns a `-0.0`
//! state into `+0.0` and quiets a signalling NaN. The float states admitted
//! are the ones [`float_state_is_identity`] names, keyed on the accumulator
//! implementation that runs rather than on the function's name:
//!
//! - non-distinct `avg`/`mean` resolved to Ravel's sequential-fold UDAF
//!   ([`crate::avg::sequential_avg_udaf`]) over a `Float64` argument (a
//!   `Float32` argument is cast to `Float64` by its coercion), with state
//!   `[Float64 sum, Int64 count]`. Its fresh group holds no sum and a count of
//!   0; merging a state copies an incoming sum into an empty slot without
//!   arithmetic, keeps a null sum null, and adds the count to 0, so the
//!   rebuilt state and the average evaluated from it are the incoming ones.
//!
//! A float `sum` runs DataFusion's built-in accumulator seeded at `0.0` and
//! stays refused, as does any other float state.
//!
//! # Memory
//!
//! One consumer per partition, `TopKeys[<partition>]`, outside every
//! aggregate prefix `crate::memory` recognises, so the pool charges it one
//! to one with no reserve-ahead envelope. The table, accumulators, scratch
//! and pending output are charged by their `size()` after each batch; the
//! emitted keys and states of a rebuild, the sort rows of a batch and of a
//! rebuild, and the evaluated output before its cut are each allocated, then
//! charged by their built size before the step that consumes them, and
//! released after; a refused grow fails the stream after that allocation, not
//! before it. Resident state is at most `2n` groups
//! plus one batch's new keys. A refused grow fails the stream with the pool's
//! `ResourcesExhausted`, prefixed with the consumer's name. Nothing spills.
//!
//! The stream returns `Pending` (after waking itself) after every input
//! batch, so a dropped stream or a breached ceiling acts within one batch.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use datafusion::arrow::compute::{filter, take, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{Aggregate, AggregateUDF, EmitTo, GroupsAccumulator};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    evaluate_many,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, InputOrderMode, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, StatisticsArgs,
};
use futures::{Stream, StreamExt};

use crate::avg::sequential_avg_udaf;
use crate::first_groups::FirstGroupsExec;
use crate::minmax::is_float;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const TOP_KEYS_AGGREGATE_RULE: &str = "top_keys_aggregate";

/// The name prefix of the exec's per-partition memory consumer:
/// `"TopKeys[{partition}]"`.
pub const TOP_KEYS_CONSUMER_PREFIX: &str = "TopKeys[";

/// Replaces the final aggregate under a global sort on its group keys with a
/// [`TopKeysAggregateExec`]. See the module docs.
#[derive(Debug)]
pub struct TopKeysAggregateLimit {
    max_limit: usize,
}

impl TopKeysAggregateLimit {
    /// A rule that bounds an aggregate when the sort above it fetches at most
    /// `max_limit` rows. 0 plans nothing.
    pub fn new(max_limit: usize) -> Self {
        TopKeysAggregateLimit { max_limit }
    }

    /// Whether `aggregate` may be replaced by a [`TopKeysAggregateExec`]: a
    /// final mode, linear input, one grouping set with no null mask and no
    /// grouping id column, no limit or dynamic filter of its own, and at
    /// least one aggregate expression, each with a `GroupsAccumulator`, no
    /// `ORDER BY` and no `FILTER`, and either no float state field or a float
    /// state whose rebuild is an identity (see the module docs).
    pub fn admits(aggregate: &AggregateExec) -> bool {
        if !matches!(
            aggregate.mode(),
            AggregateMode::FinalPartitioned
                | AggregateMode::Final
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned
        ) || aggregate.aggr_expr().is_empty()
            || aggregate.limit_options().is_some()
            || !aggregate.dynamic_expressions_produced().is_empty()
            || *aggregate.input_order_mode() != InputOrderMode::Linear
            || aggregate.filter_expr().iter().any(Option::is_some)
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
        let no_grouping_id = group_by
            .group_schema(&aggregate.input().schema())
            .is_ok_and(|schema| {
                schema
                    .fields()
                    .iter()
                    .all(|field| field.name() != Aggregate::INTERNAL_GROUPING_ID)
            });
        let input_schema = aggregate.input_schema();
        no_grouping_id
            && aggregate.aggr_expr().iter().all(|aggr| {
                aggr.groups_accumulator_supported()
                    && aggr.order_bys().is_empty()
                    && (aggr
                        .state_fields()
                        .is_ok_and(|fields| fields.iter().all(|field| !is_float(field.data_type())))
                        || float_state_is_identity(aggr, &input_schema))
            })
    }

    /// `sort` with its aggregate replaced, when the whole gate holds; `None`
    /// otherwise.
    fn replace_under(
        &self,
        sort: &Arc<dyn ExecutionPlan>,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(sort_exec) = sort.downcast_ref::<SortExec>() else {
            return Ok(None);
        };
        let Some(n) = sort_exec
            .fetch()
            .filter(|fetch| (1..=self.max_limit).contains(fetch))
        else {
            return Ok(None);
        };
        if sort_exec.preserve_partitioning() {
            return Ok(None);
        }
        let mut ordering: Vec<PhysicalSortExpr> = sort_exec.expr().iter().cloned().collect();
        let mut path: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut node = Arc::clone(sort_exec.input());
        let aggregate = loop {
            if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
                break aggregate.clone();
            }
            if node.fetch().is_some() {
                return Ok(None);
            }
            if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
                let Some(mapped) = through_projection(&ordering, projection) else {
                    return Ok(None);
                };
                ordering = mapped;
            } else if !(node.is::<CoalescePartitionsExec>() || node.is::<CooperativeExec>()) {
                return Ok(None);
            }
            let next = match node.children()[..] {
                [child] => Arc::clone(child),
                _ => return Ok(None),
            };
            path.push(node);
            node = next;
        };
        if !Self::admits(&aggregate) || aggregate.input().is::<FirstGroupsExec>() {
            return Ok(None);
        }
        if !orders_by_keys(&ordering, &aggregate) {
            return Ok(None);
        }
        let mut child: Arc<dyn ExecutionPlan> = Arc::new(TopKeysAggregateExec::try_new(
            Arc::new(aggregate),
            ordering,
            n,
        )?);
        for node in path.into_iter().rev() {
            child = replace_child(node, child)?;
        }
        Ok(Some(replace_child(Arc::clone(sort), child)?))
    }
}

impl PhysicalOptimizerRule for TopKeysAggregateLimit {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.max_limit == 0 {
            return Ok(plan);
        }
        plan.transform_down(|node| match self.replace_under(&node)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        })
        .data()
    }

    fn name(&self) -> &str {
        TOP_KEYS_AGGREGATE_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn replace_child(
    node: Arc<dyn ExecutionPlan>,
    child: Arc<dyn ExecutionPlan>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    node.replace_children(
        vec![child],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// `ordering`, written over `projection`'s output, rewritten over its input:
/// every column is replaced by the projection expression at its index.
/// `None` when a column's index is outside the projection.
fn through_projection(
    ordering: &[PhysicalSortExpr],
    projection: &ProjectionExec,
) -> Option<Vec<PhysicalSortExpr>> {
    let exprs = projection.expr();
    ordering
        .iter()
        .map(|sort| {
            let expr = Arc::clone(&sort.expr)
                .transform_up(|expr| match expr.downcast_ref::<Column>() {
                    Some(column) => match exprs.get(column.index()) {
                        Some(source) => Ok(Transformed::yes(Arc::clone(&source.expr))),
                        None => Err(DataFusionError::Internal(String::new())),
                    },
                    None => Ok(Transformed::no(expr)),
                })
                .data()
                .ok()?;
            Some(PhysicalSortExpr::new(expr, sort.options))
        })
        .collect()
}

/// Whether every expression of `ordering`, written over `aggregate`'s
/// output, is deterministic, not a float, and reads only non-float group-key
/// columns (the first `group_expr().expr().len()` output columns), and the
/// sort rows can be encoded.
fn orders_by_keys(ordering: &[PhysicalSortExpr], aggregate: &AggregateExec) -> bool {
    let keys = aggregate.group_expr().expr().len();
    let schema = aggregate.schema();
    let mut fields = Vec::with_capacity(ordering.len());
    for sort in ordering {
        let Ok(data_type) = sort.expr.data_type(&schema) else {
            return false;
        };
        if is_float(&data_type) {
            return false;
        }
        let mut reads_keys_only = true;
        let walked = sort.expr.apply(|expr| {
            if expr.is_volatile_node() {
                reads_keys_only = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            if let Some(column) = expr.downcast_ref::<Column>() {
                let is_key = column.index() < keys
                    && schema
                        .fields()
                        .get(column.index())
                        .is_some_and(|field| !is_float(field.data_type()));
                if !is_key {
                    reads_keys_only = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if walked.is_err() || !reads_keys_only {
            return false;
        }
        fields.push(SortField::new_with_options(data_type, sort.options));
    }
    !fields.is_empty() && RowConverter::new(fields).is_ok()
}

/// A final aggregate that keeps only the `n` groups whose sort rows come
/// first. One child, the replaced aggregate's partitioning, output schema,
/// equivalence properties and input distribution. See the module docs.
pub struct TopKeysAggregateExec {
    aggregate: Arc<AggregateExec>,
    ordering: Vec<PhysicalSortExpr>,
    n: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TopKeysAggregateExec {
    /// Run `aggregate`, which [`TopKeysAggregateLimit::admits`], keeping its
    /// `n` best groups under `ordering`, whose columns index the aggregate's
    /// group-key columns. Fails when `ordering` is empty or its rows cannot
    /// be encoded.
    pub fn try_new(
        aggregate: Arc<AggregateExec>,
        ordering: Vec<PhysicalSortExpr>,
        n: usize,
    ) -> DFResult<Self> {
        if ordering.is_empty() || n == 0 {
            return Err(DataFusionError::Internal(
                "TopKeysAggregateExec: an empty ordering or n = 0".to_string(),
            ));
        }
        sort_converter(&ordering, &aggregate.schema())?;
        Ok(TopKeysAggregateExec {
            aggregate,
            ordering,
            n,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The groups each partition emits at most.
    pub fn n(&self) -> usize {
        self.n
    }

    /// The ordering groups are ranked by, over the group-key columns.
    pub fn ordering(&self) -> &[PhysicalSortExpr] {
        &self.ordering
    }

    /// The aggregate this exec replaced, whose input, expressions and
    /// properties it runs with.
    pub fn aggregate(&self) -> &Arc<AggregateExec> {
        &self.aggregate
    }
}

/// The row converter over `ordering`'s values, typed against `schema`.
fn sort_converter(ordering: &[PhysicalSortExpr], schema: &Schema) -> DFResult<RowConverter> {
    let fields = ordering
        .iter()
        .map(|sort| {
            Ok(SortField::new_with_options(
                sort.expr.data_type(schema)?,
                sort.options,
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(RowConverter::new(fields)?)
}

impl fmt::Debug for TopKeysAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TopKeysAggregateExec")
    }
}

impl DisplayAs for TopKeysAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TopKeysAggregateExec: n={}", self.n)
            }
            DisplayFormatType::TreeRender => write!(f, "n={}", self.n),
        }
    }
}

impl ExecutionPlan for TopKeysAggregateExec {
    fn name(&self) -> &'static str {
        "TopKeysAggregateExec"
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
                "TopKeysAggregateExec: the rebuilt aggregate is not an AggregateExec".to_string(),
            ));
        };
        Ok(Arc::new(TopKeysAggregateExec::try_new(
            Arc::new(aggregate.clone()),
            self.ordering.clone(),
            self.n,
        )?))
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
        let stream = TopKeysStream::try_new(
            &self.aggregate,
            self.ordering.clone(),
            self.n,
            input,
            partition,
            &context,
            TopKeysMetrics::new(&self.metrics, partition),
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
        let stats = self.aggregate.statistics_from_inputs(input_stats, args)?;
        Ok(Arc::new(stats.as_ref().clone().to_inexact()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

/// What the exec reports through `EXPLAIN ANALYZE`, per partition.
struct TopKeysMetrics {
    baseline: BaselineMetrics,
    /// Input rows whose sort row was after the threshold, dropped.
    dropped_rows: Count,
    /// Groups cut at a rebuild or at end of input.
    evictions: Count,
    /// Times the table was rebuilt from its best `n` groups.
    rebuilds: Count,
    /// Groups interned after a rebuild whose sort row equals the threshold
    /// when interned. Every key evicted at a rebuild and seen again is among
    /// them, since its later rows pass the mask only on a tie.
    interned_at_threshold: Count,
}

impl TopKeysMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        TopKeysMetrics {
            baseline: BaselineMetrics::new(metrics, partition),
            dropped_rows: MetricBuilder::new(metrics).counter("dropped_rows", partition),
            evictions: MetricBuilder::new(metrics).counter("evictions", partition),
            rebuilds: MetricBuilder::new(metrics).counter("rebuilds", partition),
            interned_at_threshold: MetricBuilder::new(metrics)
                .counter("interned_at_threshold", partition),
        }
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

fn arrays_size(arrays: &[ArrayRef]) -> usize {
    arrays
        .iter()
        .map(|array| array.get_array_memory_size())
        .sum()
}

/// The first `n` group indices of `rows` ranked by sort row, then by index.
fn ranked(rows: &Rows, n: usize) -> UInt32Array {
    let mut order: Vec<u32> = (0..rows.num_rows() as u32).collect();
    let by_rank = |a: &u32, b: &u32| {
        rows.row(*a as usize)
            .cmp(&rows.row(*b as usize))
            .then(a.cmp(b))
    };
    if n < order.len() {
        order.select_nth_unstable_by(n - 1, by_rank);
        order.truncate(n);
    }
    order.sort_unstable_by(by_rank);
    UInt32Array::from(order)
}

/// The evaluated output, handed out `batch_size` rows at a time.
struct PendingOutput {
    batch: RecordBatch,
    offset: usize,
}

struct TopKeysStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    group_by: PhysicalGroupBy,
    /// The group-key columns' schema, every field nullable, that the sort
    /// expressions are evaluated against.
    key_schema: SchemaRef,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// Each accumulator's input columns: partial states in the `Final`
    /// modes, aggregate arguments in the `Single` modes.
    args: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    merge: bool,
    group_values: Box<dyn GroupValues>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_indices: Vec<usize>,
    ordering: Vec<PhysicalSortExpr>,
    converter: RowConverter,
    /// The sort row of the n-th group kept at the last rebuild; `None`
    /// before the first.
    threshold: Option<OwnedRow>,
    n: usize,
    pending: Option<PendingOutput>,
    batch_size: usize,
    reservation: MemoryReservation,
    /// The part of the reservation charged by `size()`; the rest is a
    /// transient charged and released within one call.
    table_bytes: usize,
    metrics: TopKeysMetrics,
    done: bool,
}

impl TopKeysStream {
    fn try_new(
        aggregate: &AggregateExec,
        ordering: Vec<PhysicalSortExpr>,
        n: usize,
        input: SendableRecordBatchStream,
        partition: usize,
        context: &Arc<TaskContext>,
        metrics: TopKeysMetrics,
    ) -> DFResult<Self> {
        let input_schema = aggregate.input().schema();
        let group_by = aggregate.group_expr().clone();
        let args = aggregate_expressions(
            aggregate.aggr_expr(),
            aggregate.mode(),
            group_by.num_group_exprs(),
        )?;
        let aggr_expr = aggregate.aggr_expr().to_vec();
        let accumulators = fresh_accumulators(&aggr_expr)?;
        let group_schema = group_by.group_schema(&input_schema)?;
        let key_schema = Arc::new(Schema::new(
            group_schema
                .fields()
                .iter()
                .map(|field| Field::clone(field).with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let converter = sort_converter(&ordering, &aggregate.schema())?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let reservation = MemoryConsumer::new(format!("{TOP_KEYS_CONSUMER_PREFIX}{partition}]"))
            .register(context.memory_pool());
        Ok(TopKeysStream {
            schema: aggregate.schema(),
            input: Some(input),
            group_by,
            key_schema,
            aggr_expr,
            args,
            merge: matches!(
                aggregate.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ),
            group_values,
            accumulators,
            group_indices: Vec::new(),
            ordering,
            converter,
            threshold: None,
            n,
            pending: None,
            batch_size: context.session_config().batch_size().max(1),
            reservation,
            table_bytes: 0,
            metrics,
            done: false,
        })
    }

    /// The encoded sort rows of `keys`, the group-key columns of `rows` rows.
    fn sort_rows(&self, keys: &[ArrayRef], rows: usize) -> DFResult<Rows> {
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.key_schema),
            keys.to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let columns = self
            .ordering
            .iter()
            .map(|sort| sort.expr.evaluate(&batch)?.into_array(rows))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(self.converter.convert_columns(&columns)?)
    }

    fn ingest(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        let Some(mut keys) = evaluate_group_by(&self.group_by, batch)?.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "TopKeysAggregateExec: the group by evaluated to no grouping set".to_string(),
            ));
        };
        let mut values = evaluate_many(&self.args, batch)?;
        // Whether each kept row's sort row equals the threshold, and the
        // flags that count each new group once; both empty before the first
        // rebuild, and charged as `flags` bytes.
        let mut tied = Vec::new();
        let mut flags = 0;
        if let Some(threshold) = &self.threshold {
            let sort_rows = self.sort_rows(&keys, rows)?;
            let scratch = sort_rows.size();
            flags = 2 * rows;
            try_grow_named(&mut self.reservation, scratch + flags)?;
            tied.reserve_exact(rows);
            let mut mask = Vec::with_capacity(rows);
            for row in 0..rows {
                let order = sort_rows.row(row).cmp(&threshold.row());
                mask.push(order.is_le());
                if order.is_le() {
                    tied.push(order.is_eq());
                }
            }
            let mask = BooleanArray::from(mask);
            drop(sort_rows);
            self.reservation.shrink(scratch);
            let kept = mask.true_count();
            self.metrics.dropped_rows.add(rows - kept);
            if kept == 0 {
                self.reservation.shrink(flags);
                return Ok(());
            }
            if kept < rows {
                keys = keys
                    .iter()
                    .map(|key| filter(key, &mask))
                    .collect::<Result<_, _>>()?;
                for args in &mut values {
                    *args = args
                        .iter()
                        .map(|arg| filter(arg, &mask))
                        .collect::<Result<_, _>>()?;
                }
            }
        }
        let held = self.group_values.len();
        self.group_values.intern(&keys, &mut self.group_indices)?;
        let total_groups = self.group_values.len();
        if !tied.is_empty() {
            let mut counted = vec![false; total_groups - held];
            for (&group, &tied) in self.group_indices.iter().zip(&tied) {
                let Some(seen) = group.checked_sub(held).and_then(|new| counted.get_mut(new))
                else {
                    continue;
                };
                if tied && !*seen {
                    *seen = true;
                    self.metrics.interned_at_threshold.add(1);
                }
            }
        }
        drop(tied);
        self.reservation.shrink(flags);
        for (accumulator, args) in self.accumulators.iter_mut().zip(&values) {
            if self.merge {
                accumulator.merge_batch(args, &self.group_indices, total_groups)?;
            } else {
                accumulator.update_batch(args, &self.group_indices, None, total_groups)?;
            }
        }
        drop(keys);
        drop(values);
        self.charge_table()?;
        if self.group_values.len() >= self.n.saturating_mul(2) {
            self.rebuild()?;
        }
        Ok(())
    }

    /// Keep the best `n` groups: emit every key and state, rank them, and
    /// rebuild the table and fresh accumulators from the first `n`.
    fn rebuild(&mut self) -> DFResult<()> {
        let groups = self.group_values.len();
        if groups <= self.n {
            return Ok(());
        }
        let keys = self.group_values.emit(EmitTo::All)?;
        let mut states = Vec::with_capacity(self.accumulators.len());
        for accumulator in &mut self.accumulators {
            states.push(accumulator.state(EmitTo::All)?);
        }
        let sort_rows = self.sort_rows(&keys, groups)?;
        let transient = arrays_size(&keys)
            + states.iter().map(|state| arrays_size(state)).sum::<usize>()
            + sort_rows.size();
        try_grow_named(&mut self.reservation, transient)?;

        let order = ranked(&sort_rows, self.n);
        let worst = order.value(order.len() - 1) as usize;
        self.threshold = Some(sort_rows.row(worst).owned());
        drop(sort_rows);
        let kept_keys = keys
            .iter()
            .map(|key| take(key, &order, None))
            .collect::<Result<Vec<_>, _>>()?;
        drop(keys);
        let mut kept_states = Vec::with_capacity(states.len());
        for state in states {
            kept_states.push(
                state
                    .iter()
                    .map(|column| take(column, &order, None))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        self.group_values.clear_shrink(self.n);
        self.accumulators = fresh_accumulators(&self.aggr_expr)?;
        self.group_values
            .intern(&kept_keys, &mut self.group_indices)?;
        let total_groups = self.group_values.len();
        for (accumulator, state) in self.accumulators.iter_mut().zip(&kept_states) {
            accumulator.merge_batch(state, &self.group_indices, total_groups)?;
        }
        drop(kept_keys);
        drop(kept_states);
        self.reservation.shrink(transient);
        self.metrics.evictions.add(groups - self.n);
        self.metrics.rebuilds.add(1);
        self.charge_table()
    }

    /// Evaluate every group, cut to the best `n`, and hold the result as the
    /// pending output.
    fn finish_input(&mut self) -> DFResult<()> {
        self.input = None;
        let groups = self.group_values.len();
        if groups == 0 {
            return Ok(());
        }
        let elapsed = self.metrics.baseline.elapsed_compute().clone();
        let _timer = elapsed.timer();
        let mut columns = self.group_values.emit(EmitTo::All)?;
        let keys = columns.len();
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.evaluate(EmitTo::All)?);
        }
        self.group_values.clear_shrink(0);
        self.accumulators.clear();
        self.group_indices = Vec::new();
        self.charge_table()?;
        let evaluated = arrays_size(&columns);
        try_grow_named(&mut self.reservation, evaluated)?;
        let mut batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        if groups > self.n {
            let sort_rows = self.sort_rows(&batch.columns()[..keys], groups)?;
            let scratch = sort_rows.size();
            try_grow_named(&mut self.reservation, scratch)?;
            let order = ranked(&sort_rows, self.n);
            drop(sort_rows);
            batch = take_record_batch(&batch, &order)?;
            self.reservation.shrink(scratch);
            self.metrics.evictions.add(groups - self.n);
        }
        self.pending = Some(PendingOutput { batch, offset: 0 });
        self.reservation.shrink(evaluated);
        self.charge_table()
    }

    /// Bytes charged by `size()`: the table, the accumulators, the scratch
    /// and the pending output.
    fn table_size(&self) -> usize {
        self.group_values.size()
            + self
                .accumulators
                .iter()
                .map(|accumulator| accumulator.size())
                .sum::<usize>()
            + self.group_indices.capacity() * size_of::<usize>()
            + self.converter.size()
            + self
                .threshold
                .as_ref()
                .map_or(0, |threshold| threshold.row().as_ref().len())
            + self
                .pending
                .as_ref()
                .map_or(0, |pending| pending.batch.get_array_memory_size())
    }

    fn charge_table(&mut self) -> DFResult<()> {
        let size = self.table_size();
        if size > self.table_bytes {
            try_grow_named(&mut self.reservation, size - self.table_bytes)?;
        } else {
            self.reservation.shrink(self.table_bytes - size);
        }
        self.table_bytes = size;
        Ok(())
    }

    /// The next `batch_size` rows of the pending output.
    fn next_output(&mut self) -> DFResult<Option<RecordBatch>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let total = pending.batch.num_rows();
        let len = (total - pending.offset).min(self.batch_size);
        let batch = pending.batch.slice(pending.offset, len);
        pending.offset += len;
        if pending.offset >= total {
            self.pending = None;
            self.charge_table()?;
        }
        self.metrics.baseline.record_output(batch.num_rows());
        Ok(Some(batch))
    }

    /// Drop every buffer and release the whole reservation.
    fn release(&mut self) {
        self.input = None;
        self.pending = None;
        self.threshold = None;
        self.group_values.clear_shrink(0);
        self.accumulators.clear();
        self.group_indices = Vec::new();
        self.reservation.free();
        self.table_bytes = 0;
        self.done = true;
        self.metrics.baseline.done();
    }

    fn fail(&mut self, err: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.release();
        Poll::Ready(Some(Err(err)))
    }
}

/// Whether `aggr` is a float-state aggregate that merging into a fresh
/// accumulator returns bit for bit: non-distinct sequential-fold `avg` over a
/// `Float64` argument with state `[Float64, Int64]`. `AggregateUDF` equality
/// downcasts to the implementation type, so a same-named function with a
/// different accumulator does not match. `input_schema` is the schema the
/// aggregate's argument expressions are evaluated against.
fn float_state_is_identity(aggr: &AggregateFunctionExpr, input_schema: &Schema) -> bool {
    static SEQUENTIAL_AVG: LazyLock<AggregateUDF> = LazyLock::new(sequential_avg_udaf);
    let float64_argument = match &aggr.expressions()[..] {
        [argument] => argument
            .data_type(input_schema)
            .is_ok_and(|data_type| data_type == DataType::Float64),
        _ => false,
    };
    let sum_and_count = aggr.state_fields().is_ok_and(|fields| {
        matches!(
            fields
                .iter()
                .map(|field| field.data_type())
                .collect::<Vec<_>>()[..],
            [DataType::Float64, DataType::Int64]
        )
    });
    aggr.fun() == &*SEQUENTIAL_AVG && !aggr.is_distinct() && float64_argument && sum_and_count
}

fn fresh_accumulators(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
) -> DFResult<Vec<Box<dyn GroupsAccumulator>>> {
    aggr_expr
        .iter()
        .map(|aggr| aggr.create_groups_accumulator())
        .collect()
}

impl Stream for TopKeysStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        if let Some(input) = this.input.as_mut() {
            match ready!(input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if let Err(err) = this.ingest(&batch) {
                        return this.fail(err);
                    }
                    // Yield after every batch, so a dropped stream or a
                    // breached ceiling acts within one batch.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Some(Err(err)) => return this.fail(err),
                None => {
                    if let Err(err) = this.finish_input() {
                        return this.fail(err);
                    }
                }
            }
        }
        match this.next_output() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => {
                this.release();
                Poll::Ready(None)
            }
            Err(err) => this.fail(err),
        }
    }
}

impl RecordBatchStream for TopKeysStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
