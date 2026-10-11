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
//! size of the entire output. A round-robin `RepartitionExec` pushes each
//! input batch through its producer-side coalescer, whose large-batch bypass
//! (`BatchCoalescer::push_batch`: a batch of more than `batch_size / 2` rows
//! arriving on an empty buffer is emitted as it is) passes a `batch_size`-row
//! slice on unchanged, and then reserves exactly that figure for every batch
//! it buffers (`RepartitionExec::send`). `SortExec`, its TopK and
//! `SortPreservingMergeExec` size each input batch with
//! `get_record_batch_memory_size`, which counts every buffer the batch
//! references once, in full, and the sort and the merge sum that figure over
//! the batches they hold. With `n` slices in flight at the exchange the pool
//! is charged `n` times the aggregate's output, and when the charge exceeds
//! the pool `RepartitionExec` asks to spill, which the session's disabled disk
//! manager refuses: a plan whose live bytes fit the pool several times over
//! is answered with "the query memory pool is full and spill is disabled".
//! Upstream: apache/datafusion#24704.
//!
//! A hash or range `RepartitionExec` is excluded, for a narrower reason than
//! owning its buffers. `partition_grouped_take` does one `take_arrays` over
//! the input batch and returns `reordered_batch.slice(start, len)` per
//! partition, and `take_byte_view` and `take_dict` reuse the source data
//! buffers, so a Utf8View or Dictionary column under a hash exchange still
//! references the aggregate's whole buffers, and every partition's piece
//! references the whole taken batch. What bounds the charge in practice is
//! the exchange's coalescer: a piece of at most `batch_size / 2` rows is
//! copied into the coalescer's own buffers (sparse byte-view buffers are
//! garbage-collected on the way), so the batch `send` charges owns its
//! bytes. The over-count survives a piece of more than `batch_size / 2` rows,
//! which two partitions or skewed keys produce, and it survives whole under
//! `preserve_order` and with unbounded input, which run without a coalescer.
//! The rule leaves the aggregate below a hash or range exchange alone and
//! accepts that gap.
//!
//! # The rewrite
//!
//! [`CompactSlices`] is a physical optimizer rule that places a
//! [`CompactSlicesExec`] directly above every `AggregateExec`, of any mode,
//! whose output reaches one of the operators that charge their input by
//! buffer capacity: a round-robin `RepartitionExec`, a `SortExec` (TopK
//! included) or a `SortPreservingMergeExec`, looking through the
//! `ProjectionExec`, `FilterExec` and `CoalescePartitionsExec` nodes that sit
//! between them, and above nothing else. The exec rewrites each batch so that
//! its arrays own buffers sized to the batch: the consumer then charges the
//! slice's own bytes.
//!
//! A batch whose charge is already within 2x of the bytes its rows reference
//! is passed through untouched, so on an aggregate that emits one batch the
//! rule costs one walk over each batch's buffers and no copy, and on a plan
//! without an aggregate it costs nothing. The referenced bytes are computed
//! into nested columns: a List, LargeList or Map column counts the child
//! range its offsets cover, a FixedSizeList column its child slice, a Struct
//! column its sliced children, a Dictionary column its keys slice plus its
//! values once, and a byte-view column its views plus the data bytes those
//! views point at. `ArrayData::get_slice_memory_size` alone would not do:
//! it adds every child in full, because `ArrayData::slice` only slices the
//! children of a Struct, so a sliced List column would read as unsliced. The
//! copy is one pass over each sliced batch; it happens once per aggregate
//! output that feeds an exchange or a sort, and it is reported through the
//! exec's `copied_bytes`, `compacted_batches` and `passed_through_batches`
//! metrics.
//!
//! `arrow::compute::concat_batches` over a single batch does not copy
//! (`concat` of one array returns a slice of it), and `Array::shrink_to_fit`
//! only reallocates a buffer it owns alone, which a slice never does. So the
//! copy is `concat` of the column with an empty array of its type, which
//! takes the real path and sizes its buffers to the rows it writes; for a
//! byte-view column `gc`, which copies only the bytes the slice's views
//! reference; and for a Dictionary column a cast to its value type and back,
//! which re-encodes the slice over the values it references alone
//! (`concat` drops a Dictionary's unreferenced values only when it holds at
//! least as many values as the output has rows,
//! `should_merge_dictionary_values`; below that `MutableArrayData` copies the
//! values whole). The copy follows the nesting the measure does: a List or
//! LargeList column rebuilds its offsets from zero over its compacted child
//! range, and a Struct column compacts its children, so a byte-view or
//! Dictionary child is copied the same way at any depth. `concat` through a
//! Map or FixedSizeList copies exactly the rows of a plain child but keeps a
//! byte-view or Dictionary child's buffers whole: `MutableArrayData` carries
//! a view array's data buffers over as they are. Such a column stays
//! correct and is charged above its rows; the gap is accepted.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericByteViewArray, GenericListArray, OffsetSizeTrait, StructArray,
    new_empty_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{can_cast_types, cast, concat};
use datafusion::arrow::datatypes::{ByteViewType, DataType, FieldRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
    Statistics, StatisticsArgs,
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

/// True for an operator that charges each input batch by the capacity of the
/// buffers it references: the consumers the rewrite protects. A hash or range
/// repartition is left out: its pieces are `take`n and then coalesced, and
/// the coalescer's copy is what bounds the charge there (the module docs name
/// the cases where it does not).
fn charges_by_capacity(node: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(repartition) = node.downcast_ref::<RepartitionExec>() {
        return matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_));
    }
    node.is::<SortExec>() || node.is::<SortPreservingMergeExec>()
}

/// True for an operator above which a charge is a charge on the batches below
/// it. A `ProjectionExec` passes the columns it does not compute on as the
/// same arrays. A `FilterExec` whose predicate keeps every row of a batch
/// passes it on as a slice of itself (the filter kernel's all-selected path,
/// then the coalescer's large-batch bypass); when it drops rows the filtered
/// batch owns its rows, and the exec above it has nothing to compact and
/// passes those batches through. A `CoalescePartitionsExec` forwards batches
/// unchanged. (DataFusion 55.2 no longer plans a `CoalesceBatchesExec`; its
/// consumers coalesce for themselves.)
fn passes_batches_through(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.is::<ProjectionExec>() || node.is::<FilterExec>() || node.is::<CoalescePartitionsExec>()
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
        node.replace_children(
            rewritten,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?
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
    /// The referenced-bytes estimate of every batch the exec compacted,
    /// summed: an estimate of the bytes the copies wrote, and the figure the
    /// consumer above is charged afterwards to within the copy's own slack.
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
        total = total.saturating_add(referenced_array_bytes(column.as_ref())?);
    }
    Ok(total)
}

/// The bytes `array`'s rows reference, into its children.
/// `ArrayData::get_slice_memory_size` sizes an array's own buffers to its
/// slice but adds every child in full, because `ArrayData::slice` only slices
/// the children of a Struct; this keeps the own-buffer figure and replaces
/// the children's with the part the slice reaches.
fn referenced_array_bytes(array: &dyn Array) -> DFResult<usize> {
    let data = array.to_data();
    let whole = data.get_slice_memory_size()?;
    let mut children_in_full = 0usize;
    for child in data.child_data() {
        children_in_full = children_in_full.saturating_add(child.get_slice_memory_size()?);
    }
    let own = whole.saturating_sub(children_in_full);
    let reached = match array.data_type() {
        DataType::Utf8View => referenced_view_bytes(array.as_string_view()),
        DataType::BinaryView => referenced_view_bytes(array.as_binary_view()),
        DataType::List(_) => referenced_list_bytes(array.as_list::<i32>())?,
        DataType::LargeList(_) => referenced_list_bytes(array.as_list::<i64>())?,
        DataType::Map(_, _) => {
            let map = array.as_map();
            referenced_offset_range_bytes(map.entries(), map.value_offsets())?
        }
        DataType::FixedSizeList(_, _) => {
            referenced_array_bytes(array.as_fixed_size_list().values().as_ref())?
        }
        DataType::Struct(_) => {
            let mut total = 0usize;
            for column in array.as_struct().columns() {
                total = total.saturating_add(referenced_array_bytes(column.as_ref())?);
            }
            total
        }
        DataType::Dictionary(_, _) => {
            referenced_array_bytes(array.as_any_dictionary().values().as_ref())?
        }
        _ => children_in_full,
    };
    Ok(own.saturating_add(reached))
}

/// The referenced bytes of the child rows a (possibly sliced) list array's
/// offsets cover.
fn referenced_list_bytes<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> DFResult<usize> {
    referenced_offset_range_bytes(list.values().as_ref(), list.value_offsets())
}

/// The referenced bytes of `values[offsets[0]..offsets[len]]`, the child
/// range a list or map slice reaches; `offsets` is the slice's own `len + 1`
/// offsets.
fn referenced_offset_range_bytes<O: OffsetSizeTrait>(
    values: &dyn Array,
    offsets: &[O],
) -> DFResult<usize> {
    let (Some(first), Some(last)) = (offsets.first(), offsets.last()) else {
        return Ok(0);
    };
    let start = first.as_usize();
    let len = last.as_usize().saturating_sub(start);
    referenced_array_bytes(values.slice(start, len).as_ref())
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
            if len > INLINE_VIEW_LEN {
                len as usize
            } else {
                0
            }
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
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let compact = RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;
    metrics.copied_bytes.add(referenced);
    metrics.compacted_batches.add(1);
    Ok(compact)
}

/// A copy of `array` whose buffers hold exactly its rows, into its children.
/// A Dictionary column is re-encoded through its value type so the copy holds
/// only the values the slice references (a Dictionary whose value type arrow
/// cannot pack, by `can_cast_types`, goes through `concat` and keeps every
/// value); a List or LargeList column rebuilds its offsets from zero over its
/// compacted child range; a Struct column compacts its children. Everything
/// else goes through `concat`, which copies exactly the rows of a plain
/// column but keeps a byte-view or Dictionary child's buffers whole under a
/// Map or FixedSizeList.
fn compact_array(array: &ArrayRef) -> DFResult<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => Ok(Arc::new(array.as_string_view().gc())),
        DataType::BinaryView => Ok(Arc::new(array.as_binary_view().gc())),
        dictionary @ DataType::Dictionary(_, value_type)
            if can_cast_types(value_type, dictionary) =>
        {
            let rows = cast(array.as_ref(), value_type)?;
            Ok(cast(rows.as_ref(), dictionary)?)
        }
        DataType::List(field) => compact_list(array.as_list::<i32>(), field),
        DataType::LargeList(field) => compact_list(array.as_list::<i64>(), field),
        DataType::Struct(fields) => {
            let strukt = array.as_struct();
            let columns = strukt
                .columns()
                .iter()
                .map(compact_array)
                .collect::<DFResult<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                compact_nulls(strukt.nulls()),
            )?))
        }
        other => {
            let empty = new_empty_array(other);
            Ok(concat(&[array.as_ref(), empty.as_ref()])?)
        }
    }
}

/// `list` over its compacted child range, with offsets rebuilt from zero.
fn compact_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    field: &FieldRef,
) -> DFResult<ArrayRef> {
    let offsets = list.value_offsets();
    let (Some(first), Some(last)) = (offsets.first(), offsets.last()) else {
        let empty = new_empty_array(list.data_type());
        return Ok(concat(&[list, empty.as_ref()])?);
    };
    let start = first.as_usize();
    let len = last.as_usize().saturating_sub(start);
    let values = compact_array(&list.values().slice(start, len))?;
    let lengths = offsets
        .windows(2)
        .map(|pair| pair[1].as_usize().saturating_sub(pair[0].as_usize()));
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::from_lengths(lengths),
        values,
        compact_nulls(list.nulls()),
    )?))
}

/// A null bitmap of its own for a (possibly sliced) one.
fn compact_nulls(nulls: Option<&NullBuffer>) -> Option<NullBuffer> {
    nulls.map(|nulls| nulls.iter().collect())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{
        DictionaryArray, Int32Array, Int64Array, ListArray, StringArray, StringViewArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::compute::{SortOptions, concat_batches};
    use datafusion::arrow::datatypes::{Field, Int32Type, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::{collect, displayable};

    /// Rows per batch in the sliced-batch tests, and the number of pieces
    /// the batch is cut into.
    const ROWS: usize = 10_000;
    const PIECES: usize = 100;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]))
    }

    /// One `rows`-row batch with buffers sized to its rows: every `k` and
    /// every `s` is long enough to live outside its view.
    fn batch_of(rows: usize) -> RecordBatch {
        let keys: Vec<String> = (0..rows).map(|i| format!("key-{i:08}-padding")).collect();
        let strs: Vec<String> = (0..rows).map(|i| format!("str-{i:08}-padding")).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(StringViewArray::from(strs)),
            ],
        )
        .expect("batch")
    }

    /// `batch` cut into `PIECES` slices, the way an aggregate hands out its
    /// materialized output.
    fn pieces_of(batch: &RecordBatch) -> Vec<RecordBatch> {
        let piece = batch.num_rows() / PIECES;
        (0..PIECES).map(|i| batch.slice(i * piece, piece)).collect()
    }

    /// Rows of the nested fixture that share one dictionary value.
    const ROWS_PER_DICT_VALUE: usize = 10;

    fn dictionary_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    /// A Dictionary column, a `List<Utf8View>` column and a nullable Utf8View
    /// column: the shapes whose children a slice does not slice.
    fn nested_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("d", dictionary_type(), false),
            Field::new_list("l", Field::new_list_field(DataType::Utf8View, true), false),
            Field::new("n", DataType::Utf8View, true),
        ]))
    }

    /// One `rows`-row batch over [`nested_schema`]: `d` repeats each value for
    /// [`ROWS_PER_DICT_VALUE`] rows, `l` holds two long strings per row, `n`
    /// is null on every third row and long otherwise.
    fn nested_batch_of(rows: usize) -> RecordBatch {
        let keys = Int32Array::from(
            (0..rows)
                .map(|i| (i / ROWS_PER_DICT_VALUE) as i32)
                .collect::<Vec<_>>(),
        );
        let values: Vec<String> = (0..rows.div_ceil(ROWS_PER_DICT_VALUE))
            .map(|v| format!("dict-value-{v:08}-padding"))
            .collect();
        let dictionary =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(StringArray::from(values)))
                .expect("dictionary");
        let items: Vec<String> = (0..rows)
            .flat_map(|i| {
                [
                    format!("item-a-{i:08}-padding"),
                    format!("item-b-{i:08}-padding"),
                ]
            })
            .collect();
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8View, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2usize, rows)),
            Arc::new(StringViewArray::from(items)),
            None,
        );
        let nullable: StringViewArray = (0..rows)
            .map(|i| (i % 3 != 0).then(|| format!("nullable-{i:08}-padding")))
            .collect();
        RecordBatch::try_new(
            nested_schema(),
            vec![
                Arc::new(dictionary) as ArrayRef,
                Arc::new(list),
                Arc::new(nullable),
            ],
        )
        .expect("batch")
    }

    fn list_only_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new_list(
            "l",
            Field::new_list_field(DataType::Int64, true),
            false,
        )]))
    }

    /// One `rows`-row batch of a single `List<Int64>` column with two items
    /// per row, its buffers sized exactly to their contents.
    fn list_only_batch_of(rows: usize) -> RecordBatch {
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(2usize, rows)),
            Arc::new(Int64Array::from((0..2 * rows as i64).collect::<Vec<_>>())),
            None,
        );
        RecordBatch::try_new(list_only_schema(), vec![Arc::new(list) as ArrayRef]).expect("batch")
    }

    /// Run `batches` (one partition) through a `CompactSlicesExec` and return
    /// the exec and what came out.
    async fn run(batches: Vec<RecordBatch>) -> (Arc<CompactSlicesExec>, Vec<RecordBatch>) {
        let schema = batches.first().expect("a batch").schema();
        let source = MemorySourceConfig::try_new_exec(&[batches], schema, None).expect("source");
        let exec = Arc::new(CompactSlicesExec::new(source));
        let out = collect(
            Arc::clone(&exec) as Arc<dyn ExecutionPlan>,
            Arc::new(TaskContext::default()),
        )
        .await
        .expect("collect");
        (exec, out)
    }

    fn metric(exec: &CompactSlicesExec, name: &str) -> usize {
        exec.metrics()
            .expect("metrics")
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or(0)
    }

    /// FLIP: replace `compact_array` with `Ok(Arc::clone(array))` (or route
    /// every batch through the pass-through arm) and every piece still
    /// reports the whole batch: measured 3,090,432 charged against 30,904
    /// referenced per 100-row piece.
    #[tokio::test]
    async fn a_sliced_batch_comes_out_with_buffers_of_its_own() {
        let pieces = pieces_of(&batch_of(ROWS));
        let piece_rows = ROWS / PIECES;
        for piece in &pieces {
            let charged = piece.get_array_memory_size();
            let referenced = referenced_bytes(piece).expect("sized");
            assert!(
                charged > COMPACT_ABOVE_FACTOR * referenced,
                "the fixture must be over-charged before the exec: {charged} charged, \
                 {referenced} referenced"
            );
        }

        let (exec, out) = run(pieces).await;
        assert_eq!(out.len(), PIECES);
        let mut referenced_total = 0usize;
        for piece in &out {
            assert_eq!(piece.num_rows(), piece_rows);
            let charged = piece.get_array_memory_size();
            let referenced = referenced_bytes(piece).expect("sized");
            referenced_total += referenced;
            assert!(
                charged <= COMPACT_ABOVE_FACTOR * referenced,
                "a compacted piece is charged within {COMPACT_ABOVE_FACTOR}x of its own \
                 rows: {charged} charged, {referenced} referenced"
            );
        }
        assert_eq!(metric(&exec, "compacted_batches"), PIECES);
        assert_eq!(metric(&exec, "passed_through_batches"), 0);
        assert_eq!(metric(&exec, "copied_bytes"), referenced_total);

        let expected = batch_of(ROWS);
        let got = concat_batches(&schema(), &out).expect("concat");
        assert_eq!(got, expected, "the rows are unchanged");
    }

    /// Every column of every piece compares equal to the piece that went in,
    /// a compacted Dictionary piece holds only the values its keys reference
    /// (10 of the batch's 1,000), and every piece comes out charged within 2x
    /// of its own rows, the `List<Utf8View>` child included.
    ///
    /// FLIP: make the Dictionary arm of `compact_array` return
    /// `Ok(Arc::clone(array))` and the values assertion fails with 1000
    /// against 10. FLIP: route a List column through the `concat` arm and the
    /// charge assertion fails on piece 0 with 522,270 charged against 12,181
    /// referenced: `MutableArrayData` carries the child's view data buffers
    /// over whole.
    #[tokio::test]
    async fn nested_columns_come_out_with_their_own_buffers_and_the_same_contents() {
        let whole = nested_batch_of(ROWS);
        let pieces = pieces_of(&whole);
        let piece_rows = ROWS / PIECES;
        let (exec, out) = run(pieces.clone()).await;
        assert_eq!(out.len(), PIECES);
        for (i, (before, after)) in pieces.iter().zip(&out).enumerate() {
            assert_eq!(after.num_rows(), piece_rows);
            for (column, (b, a)) in before.columns().iter().zip(after.columns()).enumerate() {
                assert_eq!(a, b, "piece {i}, column {column} compares equal");
            }
            let values_before = before.column(0).as_dictionary::<Int32Type>().values().len();
            let values_after = after.column(0).as_dictionary::<Int32Type>().values().len();
            assert_eq!(values_before, ROWS / ROWS_PER_DICT_VALUE);
            assert_eq!(
                values_after,
                piece_rows / ROWS_PER_DICT_VALUE,
                "piece {i} keeps only the dictionary values it references"
            );
            let charged = after.get_array_memory_size();
            let referenced = referenced_bytes(after).expect("sized");
            assert!(
                charged <= COMPACT_ABOVE_FACTOR * referenced,
                "piece {i} is charged within {COMPACT_ABOVE_FACTOR}x of its own rows: \
                 {charged} charged, {referenced} referenced"
            );
        }
        assert_eq!(metric(&exec, "compacted_batches"), PIECES);
        assert_eq!(metric(&exec, "passed_through_batches"), 0);
        let got = concat_batches(&nested_schema(), &out).expect("concat");
        assert_eq!(got, whole, "the rows are unchanged");
    }

    /// FLIP: make `compact_batch` always copy (drop the pass-through arm) and
    /// the pointer equality fails on every column.
    #[tokio::test]
    async fn an_unsliced_nested_batch_passes_through_untouched() {
        let batch = nested_batch_of(8_000);
        let (exec, out) = run(vec![batch.clone()]).await;
        assert_eq!(out.len(), 1);
        for (before, after) in batch.columns().iter().zip(out[0].columns()) {
            assert!(
                Arc::ptr_eq(before, after),
                "an unsliced column is the same array after the exec"
            );
        }
        assert_eq!(out[0], batch);
        assert_eq!(metric(&exec, "compacted_batches"), 0);
        assert_eq!(metric(&exec, "passed_through_batches"), 1);
        assert_eq!(metric(&exec, "copied_bytes"), 0);
    }

    /// `ArrayData::slice` leaves a List's child whole, so
    /// `get_slice_memory_size` on a piece counts the child in full and the
    /// piece's charge sits within 2x of that figure: measured by it, every
    /// piece would pass through with the 100x charge intact.
    ///
    /// FLIP: make `referenced_array_bytes` return `data.get_slice_memory_size()`
    /// and the second precondition fails on piece 0 with 200,212 charged
    /// against 160,404 referenced: by that measure the piece is within 2x and
    /// `compact_batch` would pass it through.
    #[tokio::test]
    async fn a_sliced_list_batch_is_classified_as_sliced() {
        let pieces = pieces_of(&list_only_batch_of(ROWS));
        for (i, piece) in pieces.iter().enumerate() {
            let charged = piece.get_array_memory_size();
            let child_in_full = piece
                .column(0)
                .to_data()
                .get_slice_memory_size()
                .expect("sized");
            assert!(
                charged <= COMPACT_ABOVE_FACTOR * child_in_full,
                "piece {i}: the whole-child measure would pass it through, {charged} charged \
                 against {child_in_full} by get_slice_memory_size"
            );
            let referenced = referenced_bytes(piece).expect("sized");
            assert!(
                charged > COMPACT_ABOVE_FACTOR * referenced,
                "piece {i}: over-charged by the slice measure, {charged} charged against \
                 {referenced} referenced"
            );
        }

        let (exec, out) = run(pieces.clone()).await;
        assert_eq!(metric(&exec, "compacted_batches"), PIECES);
        assert_eq!(metric(&exec, "passed_through_batches"), 0);
        for (i, (before, after)) in pieces.iter().zip(&out).enumerate() {
            assert_eq!(after, before, "piece {i} compares equal");
            let charged = after.get_array_memory_size();
            let referenced = referenced_bytes(after).expect("sized");
            assert!(
                charged <= COMPACT_ABOVE_FACTOR * referenced,
                "piece {i} is charged within {COMPACT_ABOVE_FACTOR}x of its own rows: \
                 {charged} charged, {referenced} referenced"
            );
        }
    }

    /// The batch stays under the source's `batch_size`, so it reaches the
    /// exec as one batch.
    ///
    /// FLIP: make `compact_batch` always copy (drop the pass-through arm) and
    /// the pointer equality fails on every column.
    #[tokio::test]
    async fn an_unsliced_batch_passes_through_untouched() {
        let batch = batch_of(8_000);
        let (exec, out) = run(vec![batch.clone()]).await;
        assert_eq!(out.len(), 1);
        for (before, after) in batch.columns().iter().zip(out[0].columns()) {
            assert!(
                Arc::ptr_eq(before, after),
                "an unsliced column is the same array after the exec"
            );
        }
        assert_eq!(metric(&exec, "compacted_batches"), 0);
        assert_eq!(metric(&exec, "passed_through_batches"), 1);
        assert_eq!(metric(&exec, "copied_bytes"), 0);
    }

    // ----- plan placement -----

    fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CompactSlices
            .optimize(plan, &ConfigOptions::default())
            .expect("rule runs")
    }

    fn scan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let empty: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
        MemorySourceConfig::try_new_exec(&empty, schema(), None).expect("source")
    }

    fn count_v(input_schema: &SchemaRef) -> Vec<Arc<AggregateFunctionExpr>> {
        vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("v", input_schema).expect("v")])
                .schema(Arc::clone(input_schema))
                .alias("count(v)")
                .build()
                .expect("count"),
        )]
    }

    /// `AggregateExec(Partial) -> CoalescePartitionsExec -> AggregateExec(Final)`
    /// over `input`, grouping on `k` and counting `v`: the single-partition
    /// final DataFusion 55.2 plans for a non-exact-typed statement.
    fn partial_then_final(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let input_schema = input.schema();
        let partial = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![(
                    col("k", &input_schema).expect("k"),
                    "k".to_string(),
                )]),
                count_v(&input_schema),
                vec![None],
                input,
                Arc::clone(&input_schema),
            )
            .expect("partial"),
        );
        let merged = Arc::new(CoalescePartitionsExec::new(partial));
        let merged_schema = merged.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::new_single(vec![(
                    col("k", &merged_schema).expect("k"),
                    "k".to_string(),
                )]),
                count_v(&input_schema),
                vec![None],
                merged,
                input_schema,
            )
            .expect("final"),
        )
    }

    /// `AggregateExec(Single)` over `input`, grouping on `k` and counting
    /// `v`, in every partition of `input`.
    fn single(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let input_schema = input.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![(
                    col("k", &input_schema).expect("k"),
                    "k".to_string(),
                )]),
                count_v(&input_schema),
                vec![None],
                input,
                input_schema,
            )
            .expect("single"),
        )
    }

    fn filter_v_positive(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let predicate = binary(
            col("v", &schema).expect("v"),
            Operator::Gt,
            lit(0i64),
            &schema,
        )
        .expect("predicate");
        Arc::new(FilterExec::try_new(predicate, input).expect("filter"))
    }

    fn having_count_above_one(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let predicate = binary(
            col("count(v)", &schema).expect("count"),
            Operator::Gt,
            lit(1i64),
            &schema,
        )
        .expect("predicate");
        Arc::new(FilterExec::try_new(predicate, input).expect("filter"))
    }

    fn project_all(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = schema
            .fields()
            .iter()
            .map(|f| (col(f.name(), &schema).expect("column"), f.name().clone()))
            .collect();
        Arc::new(ProjectionExec::try_new(exprs, input).expect("projection"))
    }

    fn topk_by_count(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            col("count(v)", &schema).expect("count"),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .expect("one sort key");
        Arc::new(SortExec::new(ordering, input).with_fetch(Some(fetch)))
    }

    fn round_robin(input: Arc<dyn ExecutionPlan>, partitions: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(partitions))
                .expect("repartition"),
        )
    }

    /// The 55.2 q29 shape, 32 target partitions:
    /// scan -> Filter -> Aggregate(Partial) -> CoalescePartitions ->
    /// Aggregate(Final) -> Repartition(RoundRobinBatch(32)) -> Filter ->
    /// Projection -> CoalescePartitions -> SortExec TopK(25).
    fn q29_plan() -> Arc<dyn ExecutionPlan> {
        let aggregated = partial_then_final(filter_v_positive(scan(32)));
        let fanned_out = round_robin(aggregated, 32);
        let filtered = project_all(having_count_above_one(fanned_out));
        topk_by_count(Arc::new(CoalescePartitionsExec::new(filtered)), 25)
    }

    /// Indices of the plan-text lines that start (after indentation) with
    /// `prefix`.
    fn lines_starting_with(text: &str, prefix: &str) -> Vec<usize> {
        text.lines()
            .enumerate()
            .filter(|(_, line)| line.trim_start().starts_with(prefix))
            .map(|(i, _)| i)
            .collect()
    }

    /// FLIP: make `charges_by_capacity` return `false` for a `RepartitionExec`
    /// and the exec count is 0; make `passes_batches_through` return `true`
    /// for an `AggregateExec` and the partial aggregate gets one too (count
    /// 2).
    #[test]
    fn the_q29_plan_gets_one_exec_between_the_final_aggregate_and_the_round_robin_repartition() {
        let before = plan_text(&q29_plan());
        assert!(
            !before.contains("CompactSlicesExec"),
            "the hand-built plan carries no exec:\n{before}"
        );
        let after = plan_text(&optimize(q29_plan()));

        let execs = lines_starting_with(&after, "CompactSlicesExec");
        assert_eq!(execs.len(), 1, "exactly one exec:\n{after}");
        let exec_line = execs[0];
        let repartition =
            lines_starting_with(&after, "RepartitionExec: partitioning=RoundRobinBatch(32)");
        assert_eq!(
            repartition,
            vec![exec_line - 1],
            "the repartition is directly above:\n{after}"
        );
        let final_aggregate = lines_starting_with(&after, "AggregateExec: mode=Final");
        assert_eq!(
            final_aggregate,
            vec![exec_line + 1],
            "the final aggregate is directly below:\n{after}"
        );
        let partial = lines_starting_with(&after, "AggregateExec: mode=Partial");
        assert_eq!(partial.len(), 1);
        assert!(
            !after
                .lines()
                .nth(partial[0] - 1)
                .expect("line")
                .trim_start()
                .starts_with("CompactSlicesExec"),
            "the partial aggregate, whose parent is a CoalescePartitionsExec feeding the \
             final aggregate, gets nothing:\n{after}"
        );
    }

    /// The sort edge: a final aggregate feeding a TopK through a
    /// `CoalescePartitionsExec` gets one exec, directly above it.
    ///
    /// FLIP: make `charges_by_capacity` return `false` for a `SortExec` and
    /// the exec count is 0.
    #[test]
    fn a_final_aggregate_feeding_a_topk_through_coalesce_partitions_gets_one_exec() {
        let aggregated = partial_then_final(scan(4));
        let plan = topk_by_count(Arc::new(CoalescePartitionsExec::new(aggregated)), 10);
        let after = plan_text(&optimize(plan));
        let execs = lines_starting_with(&after, "CompactSlicesExec");
        assert_eq!(execs.len(), 1, "exactly one exec:\n{after}");
        let final_aggregate = lines_starting_with(&after, "AggregateExec: mode=Final");
        assert_eq!(final_aggregate, vec![execs[0] + 1], "{after}");
        assert_eq!(
            lines_starting_with(&after, "SortExec: TopK"),
            vec![execs[0] - 2],
            "{after}"
        );
    }

    /// FLIP: drop the `charged_above` test in `rewrite` (wrap every
    /// `AggregateExec`) and the text gains a line.
    #[test]
    fn an_aggregate_under_a_root_projection_gets_nothing() {
        let plan = project_all(partial_then_final(scan(4)));
        let before = plan_text(&plan);
        let after = plan_text(&optimize(plan));
        assert_eq!(before, after);
        assert!(!after.contains("CompactSlicesExec"));
    }

    /// The merge edge: an aggregate feeding a `SortPreservingMergeExec` gets
    /// one exec, directly above it.
    ///
    /// FLIP: make `charges_by_capacity` return `false` for a
    /// `SortPreservingMergeExec` and the exec count is 0.
    #[test]
    fn an_aggregate_feeding_a_sort_preserving_merge_gets_one_exec() {
        let aggregated = single(scan(4));
        let schema = aggregated.schema();
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(
            col("count(v)", &schema).expect("count"),
        )])
        .expect("one sort key");
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, aggregated));
        let after = plan_text(&optimize(plan));
        let execs = lines_starting_with(&after, "CompactSlicesExec");
        assert_eq!(execs.len(), 1, "exactly one exec:\n{after}");
        assert_eq!(
            lines_starting_with(&after, "SortPreservingMergeExec"),
            vec![execs[0] - 1],
            "the merge is directly above:\n{after}"
        );
        assert_eq!(
            lines_starting_with(&after, "AggregateExec: mode=Single"),
            vec![execs[0] + 1],
            "the aggregate is directly below:\n{after}"
        );
    }

    /// A hash repartition's pieces are `take`n and then coalesced, and the
    /// coalescer's copy bounds the charge there (see the module docs for the
    /// cases it does not cover), so the aggregate below it is left alone.
    ///
    /// FLIP: make `charges_by_capacity` return `true` for every
    /// `RepartitionExec` and the text gains a line.
    #[test]
    fn an_aggregate_under_a_hash_repartition_gets_nothing() {
        let aggregated = partial_then_final(scan(4));
        let schema = aggregated.schema();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(
                aggregated,
                Partitioning::Hash(vec![col("k", &schema).expect("k")], 4),
            )
            .expect("repartition"),
        );
        let before = plan_text(&plan);
        let after = plan_text(&optimize(plan));
        assert_eq!(before, after);
    }

    /// FLIP: wrap every node that sits under a charging consumer (drop the
    /// `is::<AggregateExec>()` test) and the text gains lines.
    #[test]
    fn a_plan_without_an_aggregate_is_unchanged() {
        let plan = topk_by_count_on_v(round_robin(filter_v_positive(scan(4)), 4));
        let before = plan_text(&plan);
        let after = plan_text(&optimize(Arc::clone(&plan)));
        assert_eq!(before, after);
        assert!(!after.contains("CompactSlicesExec"));
    }

    fn topk_by_count_on_v(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("v", &schema).expect("v"))])
                .expect("one sort key");
        Arc::new(SortExec::new(ordering, input).with_fetch(Some(10)))
    }
}
