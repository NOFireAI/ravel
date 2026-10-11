//! Give an aggregate's output slices buffers of their own before an exchange
//! or a sort charges them to the memory pool (issue #2720).
//!
//! # The over-count
//!
//! DataFusion 55.2's migrated aggregate streams materialize every group into
//! one output batch and hand it downstream as `batch.slice(offset,
//! batch_size)` pieces (`MaterializedOutput::next_batch`). A slice shares the
//! whole batch's buffers, and `RecordBatch::get_array_memory_size` reports the
//! capacity of every buffer an array references, so each slice reports the
//! size of the entire output. `RepartitionExec` reserves exactly that figure
//! for every slice it buffers (`RepartitionExec::send`), and `SortExec` and
//! its TopK sizing count their input the same way. With `n` slices in flight
//! at the exchange the pool is charged `n` times the aggregate's output, and
//! when the charge exceeds the pool `RepartitionExec` asks to spill, which the
//! session's disabled disk manager refuses: a plan whose live bytes fit the
//! pool several times over is answered with "the query memory pool is full
//! and spill is disabled". Upstream: apache/datafusion#24704.
//!
//! # The rewrite
//!
//! [`CompactSlices`] is a physical optimizer rule that places a
//! [`CompactSlicesExec`] directly above every `AggregateExec`, of any mode,
//! whose output reaches one of the operators that charge their input by
//! `get_array_memory_size`: a `RepartitionExec`, a `SortExec` (TopK included)
//! or a `SortPreservingMergeExec`, looking through the `CoalesceBatchesExec`,
//! `ProjectionExec`, `FilterExec` and `CoalescePartitionsExec` nodes that sit
//! between them, and above nothing else. The exec rewrites each batch so that
//! its arrays own buffers sized to the batch: the consumer then charges the
//! slice's own bytes.
//!
//! A batch whose charge is already within 2x of the bytes its rows reference
//! is passed through untouched, so the rule costs nothing on an aggregate
//! that emits one batch, and nothing on a plan without an aggregate. The copy
//! is one pass over each sliced batch; it happens once per aggregate output
//! that feeds an exchange or a sort, and it is reported through the exec's
//! `copied_bytes`, `compacted_batches` and `passed_through_batches` metrics.
//!
//! `arrow::compute::concat_batches` over a single batch does not copy
//! (`concat` of one array returns a slice of it), and `Array::shrink_to_fit`
//! only reallocates a buffer it owns alone, which a slice never does. So the
//! copy is `concat` of the column with an empty array of its type, which
//! takes the real path and sizes its buffers to the rows it writes, and for a
//! byte-view column `gc`, which copies only the bytes the slice's views
//! reference.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryViewArray, GenericByteViewArray, StringViewArray,
    new_empty_array,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{ByteViewType, DataType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream, Statistics, StatisticsArgs,
};
use futures::StreamExt;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const COMPACT_SLICES_RULE: &str = "compact_aggregate_slices";

/// How far above its referenced bytes a batch's charge may sit before the
/// exec copies it. A builder-produced batch can carry up to twice its length
/// in capacity; a slice of a batch of `n` pieces carries `n` times.
const COMPACT_ABOVE_FACTOR: usize = 2;

/// Views of this many bytes or fewer are stored inline in the view itself
/// and reference no data buffer.
const INLINE_VIEW_LEN: u32 = 12;

/// Inserts a [`CompactSlicesExec`] above every aggregate whose output an
/// exchange or a sort charges by buffer capacity. See the module docs.
#[derive(Debug, Default)]
pub struct CompactSlices;

impl PhysicalOptimizerRule for CompactSlices {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        rewrite(plan, false)
    }

    fn name(&self) -> &str {
        COMPACT_SLICES_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// True for an operator that charges each input batch by
/// `get_array_memory_size`: the consumers the rewrite protects.
fn charges_by_capacity(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.is::<RepartitionExec>() || node.is::<SortExec>() || node.is::<SortPreservingMergeExec>()
}

/// True for an operator that passes its input batches on, or the arrays of
/// them it does not compute, without copying: a charge above it is a charge
/// on the batches below it.
fn passes_batches_through(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.is::<CoalesceBatchesExec>()
        || node.is::<ProjectionExec>()
        || node.is::<FilterExec>()
        || node.is::<CoalescePartitionsExec>()
}

/// Rewrite `node` and its subtree. `charged_above` is whether the nearest
/// ancestor that is not a pass-through is a consumer that charges by
/// capacity, which is the only thing the decision at an aggregate depends on.
fn rewrite(node: Arc<dyn ExecutionPlan>, charged_above: bool) -> DFResult<Arc<dyn ExecutionPlan>> {
    let charged_below = if charges_by_capacity(&node) {
        true
    } else if passes_batches_through(&node) {
        charged_above
    } else {
        false
    };

    let children = node.children();
    let mut rewritten = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let new_child = rewrite(Arc::clone(child), charged_below)?;
        changed |= !Arc::ptr_eq(child, &new_child);
        rewritten.push(new_child);
    }
    let node = if changed {
        node.with_new_children(rewritten)?
    } else {
        node
    };

    if charged_above && node.is::<AggregateExec>() {
        return Ok(Arc::new(CompactSlicesExec::new(node)));
    }
    Ok(node)
}

/// A pass-through whose stream gives every sliced batch buffers of its own.
/// One child, same partitioning, same ordering, same rows. See the module
/// docs.
pub struct CompactSlicesExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// What the exec reports through `EXPLAIN ANALYZE`.
#[derive(Clone)]
struct CompactMetrics {
    /// Bytes written by the copies: the referenced bytes of every batch the
    /// exec compacted, which is what the consumer above is charged afterwards.
    copied_bytes: Count,
    /// Batches the exec copied.
    compacted_batches: Count,
    /// Batches whose charge was already within the factor and went through
    /// untouched.
    passed_through_batches: Count,
}

impl CompactMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        CompactMetrics {
            copied_bytes: MetricBuilder::new(metrics).counter("copied_bytes", partition),
            compacted_batches: MetricBuilder::new(metrics).counter("compacted_batches", partition),
            passed_through_batches: MetricBuilder::new(metrics)
                .counter("passed_through_batches", partition),
        }
    }
}

impl CompactSlicesExec {
    /// Wrap `input`. Every plan property is the input's own.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        CompactSlicesExec {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The plan this exec passes through.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl fmt::Debug for CompactSlicesExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactSlicesExec")
    }
}

impl DisplayAs for CompactSlicesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CompactSlicesExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for CompactSlicesExec {
    fn name(&self) -> &'static str {
        "CompactSlicesExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
                "CompactSlicesExec: expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(CompactSlicesExec::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let metrics = CompactMetrics::new(&self.metrics, partition);
        let stream = input.map(move |batch| batch.and_then(|batch| compact_batch(batch, &metrics)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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
        input_stats.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("CompactSlicesExec: no input statistics".to_string())
        })
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// The bytes `batch`'s rows reference: for a slice, the slice's own share.
/// This is the figure a consumer should be charged, and the figure the copy
/// produces.
fn referenced_bytes(batch: &RecordBatch) -> DFResult<usize> {
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(column.to_data().get_slice_memory_size()?);
        if let Some(views) = column.as_any().downcast_ref::<StringViewArray>() {
            total = total.saturating_add(referenced_view_bytes(views));
        } else if let Some(views) = column.as_any().downcast_ref::<BinaryViewArray>() {
            total = total.saturating_add(referenced_view_bytes(views));
        }
    }
    Ok(total)
}

/// Bytes in `array`'s data buffers that its (possibly sliced) views point at.
/// The views buffer itself is in the array layout and counted by
/// `get_slice_memory_size`; the data buffers are not.
fn referenced_view_bytes<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> usize {
    array
        .views()
        .iter()
        .map(|view| {
            // The low 32 bits of a view are its length, whatever its layout.
            let len = *view as u32;
            if len > INLINE_VIEW_LEN { len as usize } else { 0 }
        })
        .fold(0usize, usize::saturating_add)
}

/// Return `batch` with buffers of its own when its charge is more than
/// [`COMPACT_ABOVE_FACTOR`] times the bytes it references, and `batch` itself
/// otherwise.
fn compact_batch(batch: RecordBatch, metrics: &CompactMetrics) -> DFResult<RecordBatch> {
    let charged = batch.get_array_memory_size();
    let referenced = referenced_bytes(&batch)?;
    if charged <= referenced.saturating_mul(COMPACT_ABOVE_FACTOR) {
        metrics.passed_through_batches.add(1);
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .map(compact_array)
        .collect::<DFResult<Vec<_>>>()?;
    let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
        .with_row_count(Some(batch.num_rows()));
    let compact = RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;
    metrics.copied_bytes.add(referenced);
    metrics.compacted_batches.add(1);
    Ok(compact)
}

/// A copy of `array` whose buffers hold exactly its rows.
fn compact_array(array: &ArrayRef) -> DFResult<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => Ok(Arc::new(array.as_string_view().gc())),
        DataType::BinaryView => Ok(Arc::new(array.as_binary_view().gc())),
        other => {
            let empty = new_empty_array(other);
            Ok(concat(&[array.as_ref(), empty.as_ref()])?)
        }
    }
}
