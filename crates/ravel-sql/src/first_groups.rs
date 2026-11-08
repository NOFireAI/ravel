//! Bound the groups a final aggregate tracks under an unordered
//! `GROUP BY ... LIMIT n` (issue #3985, ADR-3985).
//!
//! # The problem
//!
//! A `GROUP BY` with a `LIMIT` and no `ORDER BY` may return any `n` groups,
//! but each group it returns must carry every one of its rows. DataFusion
//! 55.2 plans the statement as a partial aggregate, a hash exchange on the
//! group keys, a final aggregate and a limit above it, and the final
//! aggregate builds a table of every group its partition receives before it
//! emits one row. With a million distinct groups that table, charged with
//! the aggregate's undercount factor and reserve-ahead envelope, is the
//! largest consumer of the query: ClickBench's index 17 was refused at the
//! process ceiling and answered only by the ADR-0954 refusal-retry.
//!
//! # The rewrite
//!
//! [`FirstGroupsLimit`] places a [`FirstGroupsExec`] as the direct child of a
//! final-stage `AggregateExec` (mode `FinalPartitioned`, `Final`, `Single` or
//! `SinglePartitioned`) whose output reaches a limit through nothing but
//! projections, fetch-less partition merges and [`CompactSlicesExec`] nodes
//! that carry no TopK pre-filter ([`FirstGroupsLimit::bound_below`]). The
//! limit is a `GlobalLimitExec`, a `LocalLimitExec` or a
//! `CoalescePartitionsExec` with a fetch; any other node with a fetch (a
//! sort, a merge, a filter) ends the walk, and so does every node not named
//! here, so a sort, a `HAVING` filter, a window, a join or a union between
//! the limit and the aggregate leaves the plan unchanged. With `OFFSET s
//! LIMIT n` the bound is `n + s`; under nested limits it is the rows the
//! innermost one passes up. The aggregate must hold at least one aggregate
//! expression, one grouping set with no null mask and no grouping id, no
//! limit of its own, no dynamic filter and linear (unordered) input
//! ([`FirstGroupsLimit::admits`]), and the bound must be at least 1 and at
//! most `SqlConfig::bounded_groups_max_limit`, where 0 turns the rule off.
//! `EXPLAIN` prints the exec as `FirstGroupsExec: n=<bound>`.
//!
//! # The exec
//!
//! Each output partition interns the group keys of every input batch into a
//! DataFusion `GroupValues` (built by the public `new_group_values` over the
//! aggregate's group schema, the same table the aggregate itself builds, so
//! two keys are one group here exactly when they are one group there) and
//! passes on only the rows whose group index is below the bound. Group
//! indices are dense and assigned in first-seen order, so those are the rows
//! of the first `bound` groups the partition saw. When the table holds more
//! than `bound` groups after a batch, the exec emits its keys, keeps groups
//! `0..bound` and rebuilds the table from them, so it holds at most `bound`
//! groups between batches and `bound` plus one batch's new groups while
//! interning one. It charges the table to the query pool under its own
//! consumer, `FirstGroups[<partition>]`, which is not an aggregate stream's
//! name, so the pool charges it one to one with no envelope. It reads its
//! input to the end, since a kept group's last row may arrive last, and it
//! reports its input's partitioning and orderings unchanged
//! (`maintains_input_order` is true): it drops rows, never reorders them.
//!
//! # Why the answer is exact
//!
//! The exec sits between the exchange and the final aggregate, never below
//! the exchange and never under a partial aggregate: the rule only looks at
//! the input of an aggregate in a final mode
//! ([`FirstGroupsLimit::admits`]), and the final aggregate's own input
//! distribution requirement (`AggregateExec::input_distribution_requirements`,
//! `datafusion-physical-plan-55.2.0/src/aggregates/mod.rs:2000-2012`) is
//! `KeyPartitioned` on its group expressions for `FinalPartitioned` and
//! `SinglePartitioned` and `SinglePartition` for `Final` and `Single`. Either
//! way every partial state (or raw row) of a group reaches the one final
//! partition that owns the group, and the exec sees all of them there. A
//! group the exec keeps passes every one of its rows, so the aggregate
//! computes it in full; a group the exec drops reaches the aggregate with no
//! row at all, so it is absent rather than partial. Each final partition then
//! emits `min(groups it owns, bound)` complete groups, together at least
//! `min(groups in the input, bound)`, which is all the limit above reads.
//! `the_returned_groups_carry_every_row_from_every_input_partition` in
//! `tests/first_groups.rs` pins this against the two wrong placements: a
//! filter below the exchange, and a stop after the first `bound` groups.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{Aggregate, EmitTo};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, evaluate_group_by,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputOrderMode, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, Statistics,
    StatisticsArgs,
};
use futures::{Stream, StreamExt};

use crate::compact_slices::CompactSlicesExec;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const FIRST_GROUPS_RULE: &str = "first_groups_limit";

/// The name prefix of the exec's per-partition memory consumer:
/// `"FirstGroups[{partition}]"`.
pub const FIRST_GROUPS_CONSUMER_PREFIX: &str = "FirstGroups[";

/// Places a [`FirstGroupsExec`] under every final aggregate an unordered
/// limit reads. See the module docs.
#[derive(Debug)]
pub struct FirstGroupsLimit {
    max_limit: usize,
}

impl FirstGroupsLimit {
    /// A rule that bounds an aggregate when the limit above it needs at most
    /// `max_limit` rows. 0 plans nothing.
    pub fn new(max_limit: usize) -> Self {
        FirstGroupsLimit { max_limit }
    }

    /// Whether `aggregate` may be fed by a [`FirstGroupsExec`]: a final mode,
    /// at least one aggregate expression, one grouping set with no null mask
    /// and no grouping id column, no limit or dynamic filter of its own, and
    /// linear input.
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
        group_by
            .group_schema(&aggregate.input().schema())
            .is_ok_and(|schema| {
                schema
                    .fields()
                    .iter()
                    .all(|field| field.name() != Aggregate::INTERNAL_GROUPING_ID)
            })
    }

    /// The rows `node`'s child must supply when `needed` rows of `node`'s own
    /// output are read (`None`: all of them), if `node` is a limit or a node
    /// that passes every row of its single child through as one output row.
    /// `Err(())` for any other node, which ends the walk.
    fn bound_below(node: &Arc<dyn ExecutionPlan>, needed: Option<usize>) -> Result<Option<usize>, ()> {
        let limited = |skip: usize, fetch: usize| {
            Some(skip.saturating_add(needed.map_or(fetch, |needed| needed.min(fetch))))
        };
        if let Some(limit) = node.downcast_ref::<GlobalLimitExec>() {
            return Ok(match limit.fetch() {
                Some(fetch) => limited(limit.skip(), fetch),
                None => needed.map(|needed| needed.saturating_add(limit.skip())),
            });
        }
        if let Some(limit) = node.downcast_ref::<LocalLimitExec>() {
            return Ok(limited(0, limit.fetch()));
        }
        if let Some(coalesce) = node.downcast_ref::<CoalescePartitionsExec>() {
            return Ok(match coalesce.fetch() {
                Some(fetch) => limited(0, fetch),
                None => needed,
            });
        }
        if node.fetch().is_some() {
            return Err(());
        }
        if node.is::<ProjectionExec>() || node.is::<CooperativeExec>() {
            return Ok(needed);
        }
        if let Some(compact) = node.downcast_ref::<CompactSlicesExec>()
            && compact.topk().is_none()
        {
            return Ok(needed);
        }
        Err(())
    }

    /// `node` with a [`FirstGroupsExec`] under the aggregate its output
    /// reaches, when that aggregate qualifies and `needed` (the rows read of
    /// `node`'s output) is a bound in `1..=max_limit`; `None` otherwise.
    fn insert(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        needed: Option<usize>,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            let Some(bound) = needed.filter(|bound| (1..=self.max_limit).contains(bound)) else {
                return Ok(None);
            };
            if !Self::admits(aggregate) || aggregate.input().is::<FirstGroupsExec>() {
                return Ok(None);
            }
            let first = FirstGroupsExec::try_new(
                Arc::clone(aggregate.input()),
                aggregate.group_expr().clone(),
                bound,
            )?;
            return Ok(Some(Self::replace_child(node, Arc::new(first))?));
        }
        let Ok(needed_below) = Self::bound_below(node, needed) else {
            return Ok(None);
        };
        let [child] = node.children()[..] else {
            return Ok(None);
        };
        match self.insert(child, needed_below)? {
            Some(new_child) => Ok(Some(Self::replace_child(node, new_child)?)),
            None => Ok(None),
        }
    }

    fn replace_child(
        node: &Arc<dyn ExecutionPlan>,
        child: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Arc::clone(node).replace_children(
            vec![child],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn rewrite(&self, node: Arc<dyn ExecutionPlan>) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let is_limit = node.is::<GlobalLimitExec>()
            || node.is::<LocalLimitExec>()
            || node
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some_and(|coalesce| coalesce.fetch().is_some());
        if !is_limit {
            return Ok(Transformed::no(node));
        }
        match self.insert(&node, None)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        }
    }
}

impl PhysicalOptimizerRule for FirstGroupsLimit {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.max_limit == 0 {
            return Ok(plan);
        }
        plan.transform_down(|node| self.rewrite(node)).data()
    }

    fn name(&self) -> &str {
        FIRST_GROUPS_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Passes on the rows of the first `n` groups each partition sees, under a
/// final aggregate's group expressions. One child; its partitioning, schema
/// and orderings. See the module docs.
pub struct FirstGroupsExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: PhysicalGroupBy,
    group_schema: SchemaRef,
    n: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl FirstGroupsExec {
    /// Keep the rows of the first `n` groups of `input` under `group_by`,
    /// evaluated against `input`'s schema.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        n: usize,
    ) -> DFResult<Self> {
        let group_schema = group_by.group_schema(&input.schema())?;
        Ok(FirstGroupsExec {
            input,
            group_by,
            group_schema,
            n,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The groups each partition keeps.
    pub fn n(&self) -> usize {
        self.n
    }

    /// The group expressions keys are evaluated by.
    pub fn group_by(&self) -> &PhysicalGroupBy {
        &self.group_by
    }
}

impl fmt::Debug for FirstGroupsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FirstGroupsExec")
    }
}

impl DisplayAs for FirstGroupsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FirstGroupsExec: n={}", self.n)
            }
            DisplayFormatType::TreeRender => write!(f, "n={}", self.n),
        }
    }
}

impl ExecutionPlan for FirstGroupsExec {
    fn name(&self) -> &'static str {
        "FirstGroupsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        let mut recursion = TreeNodeRecursion::Continue;
        for (expr, _) in self.group_by.expr() {
            recursion = f(expr)?;
            if recursion == TreeNodeRecursion::Stop {
                break;
            }
        }
        Ok(recursion)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "FirstGroupsExec: expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(FirstGroupsExec::try_new(
            children.swap_remove(0),
            self.group_by.clone(),
            self.n,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("{FIRST_GROUPS_CONSUMER_PREFIX}{partition}]"))
            .register(context.memory_pool());
        Ok(Box::pin(FirstGroupsStream {
            schema: input.schema(),
            input,
            group_by: self.group_by.clone(),
            group_schema: Arc::clone(&self.group_schema),
            group_values: new_group_values(Arc::clone(&self.group_schema), &GroupOrdering::None)?,
            group_indices: Vec::new(),
            n: self.n,
            reservation,
            metrics: FirstGroupsMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        let input = input_stats.first().ok_or_else(|| {
            DataFusionError::Internal("FirstGroupsExec: no input statistics".to_string())
        })?;
        Ok(Arc::new(input.as_ref().clone().to_inexact()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

/// What the exec reports through `EXPLAIN ANALYZE`, per partition.
struct FirstGroupsMetrics {
    baseline: BaselineMetrics,
    /// Input rows of groups past the bound, dropped.
    dropped_rows: Count,
    /// Times the table was cut back to the bound.
    rebuilds: Count,
}

impl FirstGroupsMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        FirstGroupsMetrics {
            baseline: BaselineMetrics::new(metrics, partition),
            dropped_rows: MetricBuilder::new(metrics).counter("dropped_rows", partition),
            rebuilds: MetricBuilder::new(metrics).counter("rebuilds", partition),
        }
    }
}

struct FirstGroupsStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    group_by: PhysicalGroupBy,
    group_schema: SchemaRef,
    group_values: Box<dyn GroupValues>,
    group_indices: Vec<usize>,
    n: usize,
    reservation: MemoryReservation,
    metrics: FirstGroupsMetrics,
}

impl FirstGroupsStream {
    /// `batch`'s rows of the first `n` groups, or `None` when it holds none.
    fn filter(&mut self, batch: &RecordBatch) -> DFResult<Option<RecordBatch>> {
        let keys = evaluate_group_by(&self.group_by, batch)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                DataFusionError::Internal("FirstGroupsExec: no grouping set".to_string())
            })?;
        self.group_values.intern(&keys, &mut self.group_indices)?;
        let kept = self
            .group_indices
            .iter()
            .filter(|index| **index < self.n)
            .count();
        if self.group_values.len() > self.n {
            self.cut_to_bound()?;
        }
        self.charge()?;
        let dropped = batch.num_rows() - kept;
        self.metrics.dropped_rows.add(dropped);
        if kept == 0 {
            return Ok(None);
        }
        if dropped == 0 {
            return Ok(Some(batch.clone()));
        }
        let mask: BooleanArray = self
            .group_indices
            .iter()
            .map(|index| Some(*index < self.n))
            .collect();
        Ok(Some(filter_record_batch(batch, &mask)?))
    }

    /// Rebuild the table from its first `n` groups, which keep their
    /// indices: interning `n` distinct keys into an empty table numbers them
    /// `0..n` in order.
    fn cut_to_bound(&mut self) -> DFResult<()> {
        let keys = self.group_values.emit(EmitTo::All)?;
        let kept: Vec<ArrayRef> = keys.iter().map(|key| key.slice(0, self.n)).collect();
        self.group_values = new_group_values(Arc::clone(&self.group_schema), &GroupOrdering::None)?;
        let mut indices = Vec::with_capacity(self.n);
        self.group_values.intern(&kept, &mut indices)?;
        if self.group_values.len() != self.n || indices.iter().enumerate().any(|(i, index)| i != *index)
        {
            return Err(DataFusionError::Internal(format!(
                "FirstGroupsExec: rebuilding {} groups numbered them differently",
                self.n
            )));
        }
        self.metrics.rebuilds.add(1);
        Ok(())
    }

    /// Resize the reservation to the table and the index buffer.
    fn charge(&mut self) -> DFResult<()> {
        let size = self.group_values.size()
            + self.group_indices.capacity() * std::mem::size_of::<usize>();
        let held = self.reservation.size();
        if size > held {
            let additional = size - held;
            self.reservation.try_grow(additional).map_err(|err| match err {
                DataFusionError::ResourcesExhausted(message) => {
                    DataFusionError::ResourcesExhausted(format!(
                        "{} refused {additional} bytes: {message}",
                        self.reservation.consumer().name()
                    ))
                }
                other => other,
            })?;
        } else {
            self.reservation.shrink(held - size);
        }
        Ok(())
    }
}

impl Stream for FirstGroupsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            let polled = ready!(this.input.poll_next_unpin(cx));
            let elapsed = this.metrics.baseline.elapsed_compute().clone();
            let timer = elapsed.timer();
            let result = match polled {
                Some(Ok(batch)) => match this.filter(&batch) {
                    Ok(None) => {
                        timer.done();
                        continue;
                    }
                    Ok(Some(batch)) => Some(Ok(batch)),
                    Err(err) => Some(Err(err)),
                },
                Some(Err(err)) => Some(Err(err)),
                None => {
                    this.group_values.clear_shrink(0);
                    this.group_indices = Vec::new();
                    this.reservation.free();
                    None
                }
            };
            timer.done();
            return this.metrics.baseline.record_poll(Poll::Ready(result));
        }
    }
}

impl RecordBatchStream for FirstGroupsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
