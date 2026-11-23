//! Dominant-key combining before the hash exchange (issue #4676, ADR-0102
//! decision 2's amendment of 2026-11-24).
//!
//! A `Partial` aggregate partition that skips aggregation (the probe
//! thresholds in `crate::session`) forwards one state row per input row. When
//! one key holds most of a scan's rows, most of them cross the hash exchange
//! to the single final partition that owns that key, and that partition's
//! merge is the critical path of the statement.
//! [`DominantKeyCombinerExec`] sits between the partial aggregate and the
//! exchange and merges the rows of a frequent key there, in every input
//! partition, so the exchange carries a few state rows for it instead.
//!
//! # The table
//!
//! Per input partition the exec keeps a table of `k` counters, by default
//! `k = 2P - 1` for `P` the exchange's output partitions, each keyed by a
//! group key hash. Per row, `h` its key hash:
//!
//! - `h` holds a counter: the counter is incremented. A counter with no state
//!   yet creates the state from this row, stores the row's encoded key, and
//!   merges the row. A counter with state merges the row when the row's
//!   encoded key bytes equal the stored key (two NULLs are equal); a row
//!   whose key only collides with the stored key's hash is forwarded.
//! - `h` holds no counter and one is free: the row takes it at 1 and is
//!   forwarded. A row seen for the first time is forwarded, never merged.
//! - Otherwise one decrement pass runs: every counter loses 1, a counter at 0
//!   is freed, and the state it held, if any, is emitted as one row. The row
//!   is forwarded.
//!
//! At end of input every held state is emitted once. The output has the
//! input's schema, its rows are state rows the final aggregate merges as it
//! merges the partial's own, and the partition count is the input's, so
//! the exchange and the final stage above are unchanged.
//!
//! # Exactness
//!
//! A state row is the partial's state of some subset of one key's rows, and
//! the final aggregate merges every state row of a key, whatever the subsets.
//! Every input row is either forwarded or merged into exactly one held state,
//! and every held state is emitted exactly once, so the final sees the same
//! multiset of contributions. Only the merge order changes, which is why the
//! rule admits only state types [`is_bucketed_state`] admits: no float sum
//! is reassociated. A float key is refused as well: `GroupValues` folds
//! `-0.0` into `+0.0` and the encoded key does not, so two counters could
//! name one group.
//!
//! # The bound
//!
//! A decrement pass consumes `k + 1` of a partition's `n` rows (the `k`
//! increments it cancels and the row that triggered it), so there are at
//! most `n / (k + 1)` passes. A key forwards the rows that trigger passes
//! while it holds no counter, plus its admissions, and each admission after
//! the first follows a pass that freed its counter, which it did not trigger:
//! at most `n / (k + 1) + 1` forwarded rows, `n / (2P) + 1` at `k = 2P - 1`.
//! State is held only for keys holding a counter: at most `k` groups per
//! input partition, each one key and one fixed-width state.
//!
//! # Memory
//!
//! One consumer per partition, `DominantKeyCombiner[<partition>]`, not
//! spillable and outside every aggregate prefix `crate::memory` recognises,
//! so it is charged one to one. After every input batch the reservation is
//! set to `GroupValues::size()` plus every accumulator's `size()` plus the
//! counter table (`k` counters of [`COUNTER_BYTES`] bytes and an index of
//! `(2k).next_power_of_two()` four-byte entries) plus the stored encoded key
//! bytes. A batch whose rows are compared against a stored key is row-encoded
//! once, and that encoding is charged while the batch is processed. Every byte
//! is released at end of input, on an error, and when the stream drops.
//!
//! # Eviction
//!
//! `EmitTo` takes only all groups or the first `n`, so a pass that frees a
//! held state emits every held group's key and state (at most `k` rows),
//! takes the freed groups as output, and rebuilds a fresh table and
//! accumulator set from the kept ones, as `crate::top_keys` does. A rebuild
//! costs `O(k)` and follows a pass, of which there are at most
//! `n / (k + 1)`: `O(1)` amortized per input row.

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder, UInt32Array};
use datafusion::arrow::compute::{filter_record_batch, take};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::logical_expr::{Aggregate, EmitTo, GroupsAccumulator};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, InputOrderMode, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, StatisticsArgs,
};
use futures::{Stream, StreamExt};

use crate::bucketed_final_aggregate::is_bucketed_state;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const DOMINANT_KEY_COMBINER_RULE: &str = "dominant_key_combiner";

/// The name prefix of the exec's per-partition memory consumer:
/// `"DominantKeyCombiner[{partition}]"`.
pub const DOMINANT_KEY_COMBINER_CONSUMER_PREFIX: &str = "DominantKeyCombiner[";

/// The seed of the key hash: the hash exchange's own (`create_hashes` over
/// the key columns at seed 0, as `RepartitionExec` hashes them).
const KEY_HASH_SEED: u64 = 0;

/// Bytes one counter of the table takes: its hash, its count, and the
/// optional held group (a group index and the boxed encoded key).
pub const COUNTER_BYTES: usize = size_of::<Counter>();

/// An index entry with no counter.
const EMPTY: u32 = u32::MAX;

/// A replacement for the key hash, `hashes[i]` for row `i` of the key
/// columns. Production uses the exchange's hash; tests pass one that makes
/// distinct keys collide.
pub type KeyHasher = Arc<dyn Fn(&[ArrayRef], &mut [u64]) -> DFResult<()> + Send + Sync>;

/// The counters per input partition for an exchange of `partitions` output
/// partitions: `2P - 1`.
pub fn counters_for(partitions: usize) -> usize {
    partitions.max(1).saturating_mul(2) - 1
}

/// Puts a [`DominantKeyCombinerExec`] between every admitted `Partial`
/// aggregate and the hash `RepartitionExec` directly above it. See the
/// module docs.
#[derive(Debug, Default)]
pub struct DominantKeyCombiner;

impl DominantKeyCombiner {
    /// Whether `aggregate`'s output may be combined: a `Partial` aggregate
    /// with no limit, linear input order and no dynamic filter; one grouping
    /// set with no null mask and no `__grouping_id` key; keys `GroupValues`
    /// and the row encoding both take, none of them float; every aggregate
    /// with a `GroupsAccumulator`, no ordering requirement and every state
    /// field fixed width and not float. Zero aggregates qualify.
    pub fn admits(aggregate: &AggregateExec) -> bool {
        if *aggregate.mode() != AggregateMode::Partial
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
        let group_schema = Arc::new(group_schema);
        group_schema
            .fields()
            .iter()
            .all(|field| field.name() != Aggregate::INTERNAL_GROUPING_ID)
            && keys_admitted(&group_schema)
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
}

/// Whether every key column of `schema` can be held: not float, and taken by
/// both `new_group_values` and the row encoding.
fn keys_admitted(schema: &SchemaRef) -> bool {
    !schema.fields().is_empty()
        && schema.fields().iter().all(|field| {
            !matches!(
                field.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            )
        })
        && key_converter(schema).is_ok()
        && new_group_values(Arc::clone(schema), &GroupOrdering::None).is_ok()
}

fn key_converter(schema: &Schema) -> DFResult<RowConverter> {
    Ok(RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?)
}

impl PhysicalOptimizerRule for DominantKeyCombiner {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(repartition) = node.downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };
            let Partitioning::Hash(_, partitions) = repartition.partitioning() else {
                return Ok(Transformed::no(node));
            };
            let partitions = *partitions;
            let input = Arc::clone(repartition.input());
            let admitted = !repartition.preserve_order()
                && input
                    .downcast_ref::<AggregateExec>()
                    .is_some_and(Self::admits);
            if !admitted {
                return Ok(Transformed::no(node));
            }
            let Ok(exec) = DominantKeyCombinerExec::over_partial(input, counters_for(partitions))
            else {
                return Ok(Transformed::no(node));
            };
            let rebuilt = node.replace_children(
                vec![Arc::new(exec)],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            Ok(Transformed::yes(rebuilt))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        DOMINANT_KEY_COMBINER_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Merges a dominant key's state rows before the hash exchange. One child, a
/// partial aggregate's output (keys, then every aggregate's state columns),
/// whose schema and partitioning it keeps. See the module docs.
pub struct DominantKeyCombinerExec {
    input: Arc<dyn ExecutionPlan>,
    key_count: usize,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    counters: usize,
    hasher: Option<KeyHasher>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DominantKeyCombinerExec {
    /// Combine `input`, whose first `key_count` columns are the group keys
    /// and whose remaining columns are the state columns of `aggr_expr` in
    /// order, with a table of `counters` counters per partition. Fails when
    /// the schema does not have that shape or a key type cannot be held.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_count: usize,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        counters: usize,
    ) -> DFResult<Self> {
        let schema = input.schema();
        let internal = |what: String| {
            Err(DataFusionError::Internal(format!(
                "DominantKeyCombinerExec: {what}"
            )))
        };
        if counters == 0 || counters >= EMPTY as usize / 2 {
            return internal(format!("{counters} counters"));
        }
        if key_count == 0 || key_count > schema.fields().len() {
            return internal(format!(
                "{key_count} keys over {} columns",
                schema.fields().len()
            ));
        }
        let mut column = key_count;
        for aggr in &aggr_expr {
            for field in aggr.state_fields()? {
                let Some(actual) = schema.fields().get(column) else {
                    return internal(format!("no column for state {}", field.name()));
                };
                if actual.data_type() != field.data_type() {
                    return internal(format!(
                        "state column {column} is {}, not {}",
                        actual.data_type(),
                        field.data_type()
                    ));
                }
                column += 1;
            }
        }
        if column != schema.fields().len() {
            return internal(format!(
                "{} columns, {column} keys and states",
                schema.fields().len()
            ));
        }
        if !keys_admitted(&key_schema(&schema, key_count)) {
            return internal("a key type cannot be held".to_string());
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(DominantKeyCombinerExec {
            input,
            key_count,
            aggr_expr,
            counters,
            hasher: None,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// [`Self::try_new`] over `partial`, a `Partial` [`AggregateExec`].
    pub fn over_partial(partial: Arc<dyn ExecutionPlan>, counters: usize) -> DFResult<Self> {
        let Some(aggregate) = partial.downcast_ref::<AggregateExec>() else {
            return Err(DataFusionError::Internal(
                "DominantKeyCombinerExec: the input is not an AggregateExec".to_string(),
            ));
        };
        let key_count = aggregate.group_expr().expr().len();
        let aggr_expr = aggregate.aggr_expr().to_vec();
        Self::try_new(partial, key_count, aggr_expr, counters)
    }

    /// Hash keys with `hasher` instead of the exchange's hash.
    pub fn with_hasher(mut self, hasher: KeyHasher) -> Self {
        self.hasher = Some(hasher);
        self
    }

    /// The counters each partition's table holds.
    pub fn counters(&self) -> usize {
        self.counters
    }
}

fn key_schema(schema: &Schema, key_count: usize) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .take(key_count)
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

impl fmt::Debug for DominantKeyCombinerExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DominantKeyCombinerExec")
    }
}

impl DisplayAs for DominantKeyCombinerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let schema = self.input.schema();
                let keys: Vec<&str> = schema
                    .fields()
                    .iter()
                    .take(self.key_count)
                    .map(|field| field.name().as_str())
                    .collect();
                let aggregates: Vec<&str> = self.aggr_expr.iter().map(|aggr| aggr.name()).collect();
                write!(
                    f,
                    "DominantKeyCombinerExec: gby=[{}], aggr=[{}], counters={}",
                    keys.join(", "),
                    aggregates.join(", "),
                    self.counters
                )
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for DominantKeyCombinerExec {
    fn name(&self) -> &'static str {
        "DominantKeyCombinerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DominantKeyCombinerExec: expected one child, got {}",
                children.len()
            )));
        }
        let mut exec = DominantKeyCombinerExec::try_new(
            children.swap_remove(0),
            self.key_count,
            self.aggr_expr.clone(),
            self.counters,
        )?;
        exec.hasher = self.hasher.clone();
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let stream = CombinerStream::try_new(self, input, partition, &context)?;
        Ok(Box::pin(stream))
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
            DataFusionError::Internal("DominantKeyCombinerExec: no input statistics".to_string())
        })?;
        Ok(Arc::new(input.as_ref().clone().to_inexact()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

/// What the exec reports through `EXPLAIN ANALYZE`, per partition.
struct CombinerMetrics {
    baseline: BaselineMetrics,
    /// Input rows merged into a held state.
    merged_rows: Count,
    /// Input rows passed on unmerged.
    forwarded_rows: Count,
    /// State rows emitted: at eviction and at end of input.
    emitted_states: Count,
    /// The most groups the partition held at once.
    held_groups_peak: Gauge,
    /// Decrement passes run.
    decrement_passes: Count,
}

impl CombinerMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        CombinerMetrics {
            baseline: BaselineMetrics::new(metrics, partition),
            merged_rows: MetricBuilder::new(metrics).counter("merged_rows", partition),
            forwarded_rows: MetricBuilder::new(metrics).counter("forwarded_rows", partition),
            emitted_states: MetricBuilder::new(metrics).counter("emitted_states", partition),
            held_groups_peak: MetricBuilder::new(metrics).gauge("held_groups_peak", partition),
            decrement_passes: MetricBuilder::new(metrics).counter("decrement_passes", partition),
        }
    }
}

/// One counter: a key hash, its count, and the group it holds, if any.
struct Counter {
    hash: u64,
    count: u64,
    held: Option<Held>,
}

/// A held group: its index in the table and its row-encoded key.
struct Held {
    group: usize,
    key: Box<[u8]>,
}

/// `k` counters and an open-addressing index from hash to counter, at most
/// half full. Counters are only removed by a decrement pass, which rebuilds
/// the index.
struct CounterTable {
    counters: Vec<Counter>,
    index: Vec<u32>,
    capacity: usize,
    key_bytes: usize,
}

impl CounterTable {
    fn new(capacity: usize) -> Self {
        CounterTable {
            counters: Vec::with_capacity(capacity),
            index: vec![EMPTY; capacity.saturating_mul(2).next_power_of_two().max(2)],
            capacity,
            key_bytes: 0,
        }
    }

    fn mask(&self) -> usize {
        self.index.len() - 1
    }

    fn find(&self, hash: u64) -> Option<usize> {
        let mask = self.mask();
        let mut at = hash as usize & mask;
        loop {
            let slot = *self.index.get(at)?;
            if slot == EMPTY {
                return None;
            }
            let slot = slot as usize;
            if self.counters.get(slot)?.hash == hash {
                return Some(slot);
            }
            at = (at + 1) & mask;
        }
    }

    fn place(&mut self, hash: u64, slot: usize) {
        let mask = self.mask();
        let mut at = hash as usize & mask;
        while let Some(entry) = self.index.get_mut(at) {
            if *entry == EMPTY {
                *entry = slot as u32;
                return;
            }
            at = (at + 1) & mask;
        }
    }

    /// Take a free counter for `hash` at 1; false when none is free.
    fn admit(&mut self, hash: u64) -> bool {
        if self.counters.len() >= self.capacity {
            return false;
        }
        let slot = self.counters.len();
        self.counters.push(Counter {
            hash,
            count: 1,
            held: None,
        });
        self.place(hash, slot);
        true
    }

    /// Whether the next pass frees a counter that holds a group.
    fn pass_frees_a_group(&self) -> bool {
        self.counters
            .iter()
            .any(|counter| counter.count <= 1 && counter.held.is_some())
    }

    /// Subtract 1 from every counter, free those at 0, and return the groups
    /// they held.
    fn decrement(&mut self) -> Vec<usize> {
        let mut freed = Vec::new();
        let mut key_bytes = self.key_bytes;
        self.counters.retain_mut(|counter| {
            counter.count = counter.count.saturating_sub(1);
            if counter.count > 0 {
                return true;
            }
            if let Some(held) = &counter.held {
                freed.push(held.group);
                key_bytes -= held.key.len();
            }
            false
        });
        self.key_bytes = key_bytes;
        self.index.fill(EMPTY);
        for slot in 0..self.counters.len() {
            let hash = self.counters[slot].hash;
            self.place(hash, slot);
        }
        freed
    }

    fn bytes(&self) -> usize {
        self.counters.capacity() * COUNTER_BYTES
            + self.index.capacity() * size_of::<u32>()
            + self.key_bytes
    }
}

struct CombinerStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    key_count: usize,
    /// State columns per aggregate, in order.
    state_widths: Vec<usize>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    key_schema: SchemaRef,
    group_values: Box<dyn GroupValues>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    converter: RowConverter,
    table: CounterTable,
    hasher: Option<KeyHasher>,
    random_state: RandomState,
    hashes: Vec<u64>,
    group_indices: Vec<usize>,
    pending_rows: Vec<u32>,
    pending_groups: Vec<usize>,
    output: VecDeque<RecordBatch>,
    reservation: MemoryReservation,
    metrics: CombinerMetrics,
}

impl CombinerStream {
    fn try_new(
        exec: &DominantKeyCombinerExec,
        input: SendableRecordBatchStream,
        partition: usize,
        context: &Arc<TaskContext>,
    ) -> DFResult<Self> {
        let schema = exec.input.schema();
        let key_schema = key_schema(&schema, exec.key_count);
        let state_widths = exec
            .aggr_expr
            .iter()
            .map(|aggr| aggr.state_fields().map(|fields| fields.len()))
            .collect::<DFResult<Vec<_>>>()?;
        let reservation = MemoryConsumer::new(format!(
            "{DOMINANT_KEY_COMBINER_CONSUMER_PREFIX}{partition}]"
        ))
        .register(context.memory_pool());
        let mut stream = CombinerStream {
            schema,
            input: Some(input),
            key_count: exec.key_count,
            state_widths,
            aggr_expr: exec.aggr_expr.clone(),
            group_values: new_group_values(Arc::clone(&key_schema), &GroupOrdering::None)?,
            accumulators: fresh_accumulators(&exec.aggr_expr)?,
            converter: key_converter(&key_schema)?,
            key_schema,
            table: CounterTable::new(exec.counters),
            hasher: exec.hasher.clone(),
            random_state: RandomState::with_seed(KEY_HASH_SEED),
            hashes: Vec::new(),
            group_indices: Vec::new(),
            pending_rows: Vec::new(),
            pending_groups: Vec::new(),
            output: VecDeque::new(),
            reservation,
            metrics: CombinerMetrics::new(&exec.metrics, partition),
        };
        stream.charge()?;
        Ok(stream)
    }

    /// The bytes the reservation holds between batches.
    fn held_bytes(&self) -> usize {
        self.group_values.size()
            + self
                .accumulators
                .iter()
                .map(|accumulator| accumulator.size())
                .sum::<usize>()
            + self.table.bytes()
    }

    fn charge(&mut self) -> DFResult<()> {
        let bytes = self.held_bytes();
        self.reservation.try_resize(bytes)
    }

    fn hash_keys(&mut self, keys: &[ArrayRef], rows: usize) -> DFResult<()> {
        self.hashes.clear();
        self.hashes.resize(rows, 0);
        match &self.hasher {
            Some(hasher) => hasher(keys, &mut self.hashes),
            None => create_hashes(keys, &self.random_state, &mut self.hashes).map(|_| ()),
        }
    }

    fn process(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let columns = batch.columns();
        let (keys, states) = columns.split_at(self.key_count.min(columns.len()));
        self.hash_keys(keys, rows)?;
        let mut encoded: Option<Rows> = None;
        let mut forward = BooleanBufferBuilder::new(rows);
        let mut merged = 0usize;
        for row in 0..rows {
            let hash = self.hashes[row];
            let Some(slot) = self.table.find(hash) else {
                if !self.table.admit(hash) {
                    self.decrement_pass(states)?;
                }
                forward.append(true);
                continue;
            };
            self.table.counters[slot].count += 1;
            let group = if self.table.counters[slot].held.is_none() {
                Some(self.create(keys, row, slot, encoded.as_ref())?)
            } else {
                if encoded.is_none() {
                    let rows_encoded = self.converter.convert_columns(keys)?;
                    self.reservation.try_grow(rows_encoded.size())?;
                    encoded = Some(rows_encoded);
                }
                match (&self.table.counters[slot].held, &encoded) {
                    (Some(held), Some(encoded)) if encoded.row(row).as_ref() == &*held.key => {
                        Some(held.group)
                    }
                    _ => None,
                }
            };
            match group {
                Some(group) => {
                    self.pending_rows.push(row as u32);
                    self.pending_groups.push(group);
                    merged += 1;
                    forward.append(false);
                }
                None => forward.append(true),
            }
        }
        self.flush(states)?;
        drop(encoded);
        self.metrics.merged_rows.add(merged);
        self.metrics.forwarded_rows.add(rows - merged);
        if merged == 0 {
            self.output.push_back(batch.clone());
        } else if merged < rows {
            let mask = BooleanArray::new(forward.finish(), None);
            self.output.push_back(filter_record_batch(batch, &mask)?);
        }
        self.charge()
    }

    /// Create the group of counter `slot` from row `row` of `keys`, and
    /// return its index.
    fn create(
        &mut self,
        keys: &[ArrayRef],
        row: usize,
        slot: usize,
        encoded: Option<&Rows>,
    ) -> DFResult<usize> {
        let single: Vec<ArrayRef> = keys.iter().map(|key| key.slice(row, 1)).collect();
        let expected = self.group_values.len();
        self.group_values.intern(&single, &mut self.group_indices)?;
        if self.group_indices.as_slice() != [expected] {
            return Err(DataFusionError::Internal(format!(
                "DominantKeyCombinerExec: a new key interned as {:?}, not group {expected}",
                self.group_indices
            )));
        }
        let key: Box<[u8]> = match encoded {
            Some(encoded) => encoded.row(row).as_ref().into(),
            None => self.converter.convert_columns(&single)?.row(0).as_ref().into(),
        };
        self.table.key_bytes += key.len();
        self.table.counters[slot].held = Some(Held {
            group: expected,
            key,
        });
        self.metrics
            .held_groups_peak
            .set_max(self.group_values.len());
        Ok(expected)
    }

    /// Merge the pending rows of `states` into their held groups.
    fn flush(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let total_groups = self.group_values.len();
        let indices = UInt32Array::from(std::mem::take(&mut self.pending_rows));
        let mut rest = states;
        for (accumulator, &width) in self.accumulators.iter_mut().zip(&self.state_widths) {
            let (columns, tail) = rest.split_at(width.min(rest.len()));
            let values = columns
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()?;
            accumulator.merge_batch(&values, &self.pending_groups, total_groups)?;
            rest = tail;
        }
        self.pending_groups.clear();
        Ok(())
    }

    fn decrement_pass(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.metrics.decrement_passes.add(1);
        if self.table.pass_frees_a_group() {
            self.flush(states)?;
        }
        let freed = self.table.decrement();
        if !freed.is_empty() {
            self.evict(&freed)?;
        }
        Ok(())
    }

    /// Every held group's keys, then every accumulator's state, taking them
    /// out of the table.
    fn take_all(&mut self) -> DFResult<Vec<ArrayRef>> {
        let mut columns = self.group_values.emit(EmitTo::All)?;
        for accumulator in &mut self.accumulators {
            columns.extend(accumulator.state(EmitTo::All)?);
        }
        self.group_values = new_group_values(Arc::clone(&self.key_schema), &GroupOrdering::None)?;
        self.accumulators = fresh_accumulators(&self.aggr_expr)?;
        Ok(columns)
    }

    /// Emit the `freed` groups as state rows and rebuild the table from the
    /// rest, renumbering the counters that still hold a group.
    fn evict(&mut self, freed: &[usize]) -> DFResult<()> {
        let groups = self.group_values.len();
        let columns = self.take_all()?;
        let mut evicted = vec![false; groups];
        for &group in freed {
            if let Some(flag) = evicted.get_mut(group) {
                *flag = true;
            }
        }
        let (out, kept): (Vec<u32>, Vec<u32>) =
            (0..groups as u32).partition(|&group| evicted[group as usize]);
        let out = UInt32Array::from(out);
        let taken = columns
            .iter()
            .map(|column| take(column.as_ref(), &out, None))
            .collect::<Result<Vec<_>, _>>()?;
        self.metrics.emitted_states.add(out.len());
        self.output
            .push_back(RecordBatch::try_new(Arc::clone(&self.schema), taken)?);

        let mut renumbered = vec![None; groups];
        for (new, &old) in kept.iter().enumerate() {
            renumbered[old as usize] = Some(new);
        }
        if !kept.is_empty() {
            let kept = UInt32Array::from(kept);
            let columns = columns
                .iter()
                .map(|column| take(column.as_ref(), &kept, None))
                .collect::<Result<Vec<_>, _>>()?;
            let (keys, mut rest) = columns.split_at(self.key_count.min(columns.len()));
            self.group_values.intern(keys, &mut self.group_indices)?;
            let total_groups = self.group_values.len();
            if total_groups != kept.len()
                || self
                    .group_indices
                    .iter()
                    .enumerate()
                    .any(|(at, &group)| at != group)
            {
                return Err(DataFusionError::Internal(
                    "DominantKeyCombinerExec: the kept keys did not rebuild in order".to_string(),
                ));
            }
            for (accumulator, &width) in self.accumulators.iter_mut().zip(&self.state_widths) {
                let (values, tail) = rest.split_at(width.min(rest.len()));
                accumulator.merge_batch(values, &self.group_indices, total_groups)?;
                rest = tail;
            }
        }
        for counter in &mut self.table.counters {
            if let Some(held) = &mut counter.held {
                held.group = renumbered
                    .get(held.group)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "DominantKeyCombinerExec: a held counter lost its group".to_string(),
                        )
                    })?;
            }
        }
        Ok(())
    }

    /// Emit every held state once and release the table.
    fn finish(&mut self) -> DFResult<()> {
        let groups = self.group_values.len();
        if groups > 0 {
            let columns = self.take_all()?;
            self.metrics.emitted_states.add(groups);
            self.output
                .push_back(RecordBatch::try_new(Arc::clone(&self.schema), columns)?);
        }
        self.release();
        Ok(())
    }

    /// Drop the input, the table and every charged byte.
    fn release(&mut self) {
        self.input = None;
        self.table = CounterTable::new(0);
        self.accumulators.clear();
        self.group_values.clear_shrink(0);
        self.pending_rows = Vec::new();
        self.pending_groups = Vec::new();
        self.reservation.free();
    }
}

fn fresh_accumulators(
    aggr_expr: &[Arc<AggregateFunctionExpr>],
) -> DFResult<Vec<Box<dyn GroupsAccumulator>>> {
    aggr_expr
        .iter()
        .map(|aggr| aggr.create_groups_accumulator())
        .collect()
}

impl Stream for CombinerStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(batch) = this.output.pop_front() {
                this.metrics.baseline.record_output(batch.num_rows());
                return Poll::Ready(Some(Ok(batch)));
            }
            let Some(input) = this.input.as_mut() else {
                this.metrics.baseline.done();
                return Poll::Ready(None);
            };
            let step = match ready!(input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let elapsed = this.metrics.baseline.elapsed_compute().clone();
                    let _timer = elapsed.timer();
                    this.process(&batch)
                }
                Some(Err(error)) => Err(error),
                None => {
                    let elapsed = this.metrics.baseline.elapsed_compute().clone();
                    let _timer = elapsed.timer();
                    this.finish()
                }
            };
            if let Err(error) = step {
                this.output.clear();
                this.release();
                return Poll::Ready(Some(Err(error)));
            }
        }
    }
}

impl RecordBatchStream for CombinerStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_two_p_minus_one() {
        assert_eq!(counters_for(1), 1);
        assert_eq!(counters_for(4), 7);
        assert_eq!(counters_for(32), 63);
        assert_eq!(counters_for(0), 1);
    }

    #[test]
    fn a_counter_is_forty_bytes() {
        assert_eq!(COUNTER_BYTES, 40);
    }
}
