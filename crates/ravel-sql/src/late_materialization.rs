//! Late materialization for a wide `ORDER BY ... LIMIT k` over `logs`
//! (ADR-0774, issue #774).
//!
//! # What it costs to not do this
//!
//! On the ClickBench reference tenant (100M rows, 8,424 objects, 105 declared
//! columns) three statements bound the problem:
//!
//! | statement | time |
//! |---|---|
//! | `SELECT COUNT(*) FROM logs WHERE URL LIKE '%google%'` | 22.9 s |
//! | `SELECT ts, URL FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` | 24.8 s |
//! | `SELECT * FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` | > 900 s |
//!
//! The plan for the third is `SortExec TopK(fetch=10, ts ASC)` over
//! `CoalescePartitionsExec` over `FilterExec like(URL, '%google%')` over
//! `LogsScanExec` projecting all 105 columns, with `prune=0` and `content=0`: a
//! substring `LIKE` is neither a block prune nor a bloom probe, correctly. So
//! the scan decodes every column of every block before the filter or the TopK
//! sees a row, and the two-column variant proves the filter and the sort
//! themselves are cheap. The whole difference is the 103 columns nobody looks
//! at until ten rows have already been chosen.
//!
//! # The rewrite
//!
//! [`TopKLateMaterialization`] is a physical optimizer rule that splits such a
//! plan into two phases:
//!
//! - **Phase 1** is the same `SortExec` TopK over the same `FilterExec` over a
//!   `LogsScanExec` narrowed to exactly the columns the filter and the sort
//!   read, plus a synthetic `__ravel_row_ref` column (see [`RowRef`]) carrying
//!   each row's address. Column indices in the filter predicate and the sort
//!   expressions are remapped onto the narrow schema.
//! - **Phase 2** is [`LogsRowFetchExec`]: it reads phase 1's at-most-`k` rows,
//!   groups their row refs by `(segment, block)`, re-opens exactly those
//!   blocks with the ORIGINAL column selection, decodes the referenced rows,
//!   and emits them in phase-1 order under the original output schema.
//!
//! No projection node is inserted above phase 2 to drop the row ref: the fetch
//! node's output schema IS the schema the `SortExec` it replaced had (it is
//! `Arc::clone`d off the original scan), so the row-ref column never reaches
//! the plan's output and a projection to remove it would be a no-op node. The
//! rule declares `schema_check() == true` so DataFusion asserts that for every
//! query rather than leaving it to this comment.
//!
//! # Why the two phases agree
//!
//! A row ref is not a byte offset. It is `(segment ordinal, surviving-block
//! position, surviving-row position)`, all three relative to the query, so it
//! resolves only if phase 2 asks the same question phase 1 did. It does:
//!
//! - **Same objects.** The segment ordinal indexes the snapshot's segment list,
//!   which both phases hold as the same `Arc`. Data objects are immutable, and
//!   the block-range read pins the etag across its GETs, so re-reading a block
//!   phase 1 already decoded returns the same bytes.
//! - **Same surviving blocks.** Block pruning (skip index, POSTINGS, bloom)
//!   consults the ts bounds, the content predicates, and the prune-only
//!   predicates, and never the [`ravel_logseg::ColumnSelection`]
//!   (`RlogReader::scan_blocks`). [`LogsScanExec::reproject`] carries all three
//!   over verbatim, so the surviving-block list is the same list in the same
//!   order and position `i` in it means the same block.
//! - **Same surviving rows.** Within a block, the surviving rows are those
//!   matching the exact content predicate, evaluated once per block by
//!   `BlockScan::decode_block`. `resolve_columns` always adds every column a
//!   content predicate names, on both the narrow and the wide selection, so
//!   widening the selection cannot change the evaluation. Position `i` in the
//!   surviving-row list is therefore the same row.
//! - **Same ties.** Phase 1's TopK is the same `SortExec` with the same fetch
//!   over the same rows in the same input order, so it selects the same `k`
//!   rows in the same order the single-phase plan would have. Phase 2 does not
//!   sort; it emits in phase-1 order.
//!
//! The one shape where the third bullet fails is a pending selective erasure
//! (ADR-0064): the scan layer's `retain_unerased` drops rows from the block's
//! record list AFTER the reader produced it, so a phase-1 position would not be
//! a phase-2 position. [`LogsScanExec::late_materialization_candidate`] refuses
//! there, which is also the fail-closed direction.
//!
//! # Memory
//!
//! Phase 1 holds what the narrow scan holds -- one decoded block per partition
//! plus the batch in flight (ADR-0087 decision 2) -- plus the TopK's `k` narrow
//! rows. Phase 2 holds at most `k` records and the batches built from them, and
//! decodes one block at a time; its concurrency bounds how many object-sized
//! assembly buffers exist at once, exactly as a scan partition's does. Neither
//! phase holds a wide row for a row the TopK discarded, which is the whole
//! point.
//!
//! # Cost
//!
//! Phase 2 re-reads bytes phase 1 already read: `k` block fetches, accounted
//! like any other scan read because they go through the same
//! `LogSegmentFetcher::scan_accounted_with_tenant_subset` funnel the striped
//! scan path uses.
//!
//! Be precise about what a "block fetch" costs, because the request count and
//! the byte count do not say the same thing. That entry point restricts the
//! DECODE to the named block, but its byte fetch is the query's ordinary fetch
//! for the whole object: one whole-object GET at or below the block-range
//! threshold (ADR-0107), and above it the version-4 coalesced ranges over the
//! fetch-side candidate set (ADR-0699 decision 5) -- which, for a query whose
//! only predicate is a residual the skip index cannot decide, is every block.
//! So phase 2 costs `k` requests and up to `k` objects' bytes, not `k` blocks'
//! bytes. Narrowing that fetch to the named indices is a ravel-query
//! follow-up, deliberately not done here.
//!
//! Either way the cost is bounded by `k` and by one object, while what it
//! removes is bounded by the table.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt, TryStreamExt};
use ravel_logseg::LogRecord;

use crate::logs_scan::{LogsScanExec, RowFetchSource, records_memory};

/// The name of the synthetic row-ref column phase 1 appends.
///
/// It is not in the `logs` schema and cannot be written by a client: the SQL
/// surface has no way to declare an attribute whose column name starts with
/// `__ravel_`, and the column is appended past every projected index, so no
/// remapped column reference can collide with it. It never reaches a result --
/// [`LogsRowFetchExec`] consumes it and emits the original schema.
pub const ROW_REF_COLUMN: &str = "__ravel_row_ref";

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const TOPK_LATE_MATERIALIZATION_RULE: &str = "topk_late_materialization";

/// Bits of the packed row ref given to the segment ordinal, the
/// surviving-block position, and the surviving-row position. They sum to 64.
///
/// The split is chosen so that the packed `u64` orders by
/// `(segment, block, row)` and so that no field can be reached by real data:
///
/// - `segment`: 1,048,576 segments in one resolved snapshot. The ClickBench
///   reference tenant resolves 8,424, and segment admission caps a query's
///   snapshot far below this.
/// - `block`: 16,777,216 blocks in one segment. A block is a target 8,192
///   records, so this is beyond any object an L1 compaction produces.
/// - `row`: 1,048,576 surviving rows in one block, against that same 8,192
///   target.
///
/// [`RowRef::pack`] still refuses out of range rather than truncating: a
/// silently wrapped ref would fetch a real row from the wrong place, and this
/// crate's rule is exact semantics by default.
const SEGMENT_BITS: u32 = 20;
const BLOCK_BITS: u32 = 24;
const ROW_BITS: u32 = 20;

/// One row's address inside the query's own view of the snapshot (ADR-0774).
///
/// Every field is a position in a list the query defines, not a stored
/// identifier: `segment` indexes the resolved snapshot's segment list, `block`
/// indexes that segment's surviving-block list for this query's pruning, and
/// `row` indexes that block's surviving-row list under this query's exact
/// content predicate. See the module docs for why all three are stable across
/// the two phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowRef {
    pub(crate) segment: usize,
    pub(crate) block: usize,
    pub(crate) row: usize,
}

impl RowRef {
    /// Pack into the one `UInt64` the row-ref column carries: segment in the
    /// high bits, then block, then row, so the packed value orders by the same
    /// tuple and grouping by `(segment, block)` is a sort.
    pub(crate) fn pack(self) -> DFResult<u64> {
        let seg = fit(self.segment, SEGMENT_BITS, "segment ordinal")?;
        let block = fit(self.block, BLOCK_BITS, "block position")?;
        let row = fit(self.row, ROW_BITS, "row position")?;
        Ok((seg << (BLOCK_BITS + ROW_BITS)) | (block << ROW_BITS) | row)
    }

    /// The inverse of [`Self::pack`]. Total: every `u64` decodes to some
    /// address, and whether that address exists is what
    /// [`RowFetchSource::fetch_block`] answers.
    pub(crate) fn unpack(packed: u64) -> Self {
        let row_mask = (1u64 << ROW_BITS) - 1;
        let block_mask = (1u64 << BLOCK_BITS) - 1;
        RowRef {
            segment: (packed >> (BLOCK_BITS + ROW_BITS)) as usize,
            block: ((packed >> ROW_BITS) & block_mask) as usize,
            row: (packed & row_mask) as usize,
        }
    }
}

/// `value` as a `u64` if it fits in `bits`, or a typed error naming the field.
fn fit(value: usize, bits: u32, what: &str) -> DFResult<u64> {
    let limit = 1u64 << bits;
    let value = u64::try_from(value)
        .map_err(|_| DataFusionError::Internal(format!("row-ref {what} {value} out of range")))?;
    if value >= limit {
        return Err(DataFusionError::Internal(format!(
            "row-ref {what} {value} does not fit in {bits} bits (limit {limit})"
        )));
    }
    Ok(value)
}

/// The Arrow field the row-ref column occupies. Non-nullable: every row a scan
/// emits has an address.
pub(crate) fn row_ref_field() -> Field {
    Field::new(ROW_REF_COLUMN, DataType::UInt64, false)
}

// ---------------------------------------------------------------------------
// The rule
// ---------------------------------------------------------------------------

/// Rewrites a wide `logs` TopK into a narrow TopK plus a row-ref fetch
/// (ADR-0774). See the module docs.
#[derive(Debug)]
pub struct TopKLateMaterialization {
    /// How many columns a scan must project BEYOND what the filter and the
    /// sort read before the rewrite is worth its extra `k` block reads. See
    /// [`crate::SqlConfig::late_materialization_extra_columns`].
    extra_columns: usize,
}

impl TopKLateMaterialization {
    /// The rule with `extra_columns` as its width threshold.
    pub fn new(extra_columns: usize) -> Self {
        TopKLateMaterialization { extra_columns }
    }
}

impl PhysicalOptimizerRule for TopKLateMaterialization {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| self.rewrite(node)).data()
    }

    fn name(&self) -> &str {
        TOPK_LATE_MATERIALIZATION_RULE
    }

    fn schema_check(&self) -> bool {
        // The rewrite is required to be invisible to the client: same column
        // order, names, types, and nullability. Let DataFusion assert it rather
        // than trusting that `LogsRowFetchExec` was handed the right schema.
        true
    }
}

/// A pass-through node between the TopK and the scan, in top-down order.
///
/// The allowlist is deliberately short and everything outside it refuses the
/// rewrite. Each admitted node preserves its input's schema and its rows'
/// identity (a filter removes rows, it does not synthesize them), so a row the
/// TopK keeps is a row the scan emitted and its row ref is meaningful. An
/// `AggregateExec`, a join, a window, or a `ProjectionExec` breaks one of those
/// two properties, and a node carrying its own `fetch` would truncate rows the
/// row refs no longer describe.
enum PassThrough {
    /// A residual filter. Its predicate's column references are remapped onto
    /// the narrow schema, and its columns join the set phase 1 must project.
    Filter(Arc<FilterExec>),
    /// A node with no column references to remap, rebuilt through
    /// `with_new_children`.
    Opaque(Arc<dyn ExecutionPlan>),
}

impl TopKLateMaterialization {
    /// Try to rewrite one node. Returns the node unchanged unless it is a TopK
    /// over an admitted chain over a wide-enough [`LogsScanExec`].
    fn rewrite(
        &self,
        node: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(sort) = node.downcast_ref::<SortExec>() else {
            return Ok(Transformed::no(node));
        };
        // A sort with no fetch materializes every row whatever this rule does.
        let Some(fetch) = sort.fetch() else {
            return Ok(Transformed::no(node));
        };
        // A per-partition sort keeps `fetch` rows PER partition and leaves the
        // merge above it to pick the final `k`. The bound this rule reasons
        // about, and the single-partition fetch node it builds, both assume one
        // stream of at most `k` rows.
        if sort.preserve_partitioning() {
            return Ok(Transformed::no(node));
        }

        // Walk down to the scan, collecting the chain and the columns every
        // filter on the way reads.
        let mut chain: Vec<PassThrough> = Vec::new();
        let mut needed: HashSet<usize> = HashSet::new();
        let mut current = Arc::clone(sort.input());
        let scan = loop {
            if let Some(scan) = current.downcast_ref::<LogsScanExec>() {
                break scan;
            }
            let Some(step) = classify(&current) else {
                return Ok(Transformed::no(node));
            };
            if let PassThrough::Filter(filter) = &step {
                for column in collect_columns(filter.predicate()) {
                    needed.insert(column.index());
                }
            }
            let Some(child) = current.children().first().map(|c| Arc::clone(c)) else {
                return Ok(Transformed::no(node));
            };
            chain.push(step);
            current = child;
        };

        if !scan.late_materialization_candidate() {
            return Ok(Transformed::no(node));
        }
        // Every pass-through node preserves its input schema, so the sort
        // expressions index the scan's output directly.
        for sort_expr in sort.expr().iter() {
            for column in collect_columns(&sort_expr.expr) {
                needed.insert(column.index());
            }
        }
        let width = scan.projection().len();
        // A sort over no column of the scan (a constant sort key) leaves phase
        // 1 with nothing to sort on; there is nothing to late-materialize.
        if needed.is_empty() || needed.iter().any(|&i| i >= width) {
            return Ok(Transformed::no(node));
        }
        if width - needed.len() <= self.extra_columns {
            return Ok(Transformed::no(node));
        }

        // Phase 1: the narrow scan, plus the row-ref column past its last
        // index. `narrow` is ascending, so the remap preserves relative column
        // order and a reader of the phase-1 plan sees the projection in the
        // same order the wide scan had.
        let mut narrow: Vec<usize> = needed.into_iter().collect();
        narrow.sort_unstable();
        let remap: HashMap<usize, usize> = narrow
            .iter()
            .enumerate()
            .map(|(new, &old)| (old, new))
            .collect();
        let narrow_projection: Vec<usize> = narrow.iter().map(|&i| scan.projection()[i]).collect();
        let source = scan.row_fetch_source();
        let mut phase1: Arc<dyn ExecutionPlan> = Arc::new(scan.reproject(narrow_projection, true)?);

        for step in chain.iter().rev() {
            phase1 = match step {
                PassThrough::Filter(filter) => Arc::new(FilterExec::try_new(
                    remap_columns(filter.predicate(), &remap)?,
                    phase1,
                )?),
                PassThrough::Opaque(plan) => Arc::clone(plan).with_new_children(vec![phase1])?,
            };
        }

        let narrow_ordering = remap_ordering(sort.expr(), &remap)?;
        let phase1: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(narrow_ordering, phase1).with_fetch(Some(fetch)));

        // Phase 2, carrying the original ordering: the fetch emits phase 1's
        // rows in phase 1's order, so the ordering the `SortExec` established
        // still holds over the restored schema.
        let fetch_node = LogsRowFetchExec::try_new(phase1, source, sort.expr().clone())?;
        Ok(Transformed::new(
            Arc::new(fetch_node) as Arc<dyn ExecutionPlan>,
            true,
            // The subtree below is already rewritten; there is no second TopK
            // inside it to find.
            TreeNodeRecursion::Jump,
        ))
    }
}

/// Whether `plan` may sit between the TopK and the scan, and how it is rebuilt.
///
/// Fails closed: an unrecognized node refuses the rewrite. A node carrying its
/// own `fetch` is refused too, whatever its type, because it truncates its
/// input independently of the TopK.
fn classify(plan: &Arc<dyn ExecutionPlan>) -> Option<PassThrough> {
    if plan.fetch().is_some() {
        return None;
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        // A `FilterExec` may carry its own projection, which changes the output
        // schema and the column indices above it. Refuse rather than reason
        // about two remaps at once.
        if filter.projection().is_some() {
            return None;
        }
        return Some(PassThrough::Filter(Arc::new(filter.clone())));
    }
    // `CoalescePartitionsExec` merges the scan's partitions under the TopK;
    // `CooperativeExec` is the yield wrapper DataFusion 54 puts directly above
    // a leaf. Neither reads a column or changes a schema.
    if plan.is::<CoalescePartitionsExec>() || plan.is::<CooperativeExec>() {
        return Some(PassThrough::Opaque(Arc::clone(plan)));
    }
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        // A hash repartition's expressions carry column indices that would need
        // the same remap; round-robin and unknown carry none.
        return match repartition.partitioning() {
            Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_) => {
                Some(PassThrough::Opaque(Arc::clone(plan)))
            }
            Partitioning::Hash(_, _) => None,
        };
    }
    None
}

/// `expr` with every column index rewritten through `remap`.
///
/// A column the map does not cover is an internal error, not a pass-through:
/// the map covers exactly the columns collected from this expression and the
/// sort, so a miss means the collection and the rewrite disagree, and emitting
/// a stale index would read a different column's values.
fn remap_columns(
    expr: &Arc<dyn PhysicalExpr>,
    remap: &HashMap<usize, usize>,
) -> DFResult<Arc<dyn PhysicalExpr>> {
    Arc::clone(expr)
        .transform(|node| {
            let Some(column) = node.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };
            let Some(&index) = remap.get(&column.index()) else {
                return Err(DataFusionError::Internal(format!(
                    "{TOPK_LATE_MATERIALIZATION_RULE}: column {} at index {} is not in the \
                     narrowed projection",
                    column.name(),
                    column.index()
                )));
            };
            Ok(Transformed::yes(
                Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
            ))
        })
        .data()
}

/// `ordering` with every column index rewritten through `remap`.
fn remap_ordering(ordering: &LexOrdering, remap: &HashMap<usize, usize>) -> DFResult<LexOrdering> {
    let mut exprs = Vec::with_capacity(ordering.len());
    for sort_expr in ordering.iter() {
        let mut remapped = sort_expr.clone();
        remapped.expr = remap_columns(&sort_expr.expr, remap)?;
        exprs.push(remapped);
    }
    LexOrdering::new(exprs).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "{TOPK_LATE_MATERIALIZATION_RULE}: remapped sort ordering is empty"
        ))
    })
}

// ---------------------------------------------------------------------------
// Phase 2
// ---------------------------------------------------------------------------

/// Phase 2 of the rewrite: turn at most `k` row refs into the rows a wide
/// single-phase scan would have emitted (ADR-0774).
///
/// One partition, `EmissionType::Final`: every row ref must be in hand before
/// the first block is fetched, both so the fetch groups by block and so the
/// output can be emitted in phase-1 order.
pub struct LogsRowFetchExec {
    /// Phase 1: the narrow TopK, whose last column is the row ref.
    input: Arc<dyn ExecutionPlan>,
    /// The read half of the scan this replaces.
    source: RowFetchSource,
    /// Index of the row-ref column in `input`'s schema.
    row_ref_index: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// What phase 2 publishes through `EXPLAIN ANALYZE`, so a report can say what
/// the second phase actually cost rather than inferring it from the wall clock.
#[derive(Clone)]
struct FetchMetrics {
    /// Row refs read out of phase 1's output, i.e. the rows the TopK kept.
    row_refs: Count,
    /// Distinct `(segment, block)` pairs those rows live in: the number of
    /// block fetches this phase issued. At most `row_refs`, and fewer whenever
    /// two winners share a block.
    blocks_fetched: Count,
    /// Distinct segments those blocks live in.
    segments_fetched: Count,
}

impl FetchMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        FetchMetrics {
            row_refs: MetricBuilder::new(metrics).counter("row_refs", partition),
            blocks_fetched: MetricBuilder::new(metrics).counter("blocks_fetched", partition),
            segments_fetched: MetricBuilder::new(metrics).counter("segments_fetched", partition),
        }
    }
}

impl LogsRowFetchExec {
    /// Build the fetch node over `input`, whose last column must be the row-ref
    /// column [`LogsScanExec::reproject`] appended.
    ///
    /// `ordering` is the ordering phase 1's `SortExec` established, expressed
    /// over the RESTORED schema (i.e. the original sort expressions). It is
    /// truthful because this node emits phase 1's rows in phase 1's order.
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        source: RowFetchSource,
        ordering: LexOrdering,
    ) -> DFResult<Self> {
        let input_schema = input.schema();
        let row_ref_index = input_schema.fields().len().checked_sub(1).ok_or_else(|| {
            DataFusionError::Internal(
                "late materialization: phase 1 emitted no columns at all".into(),
            )
        })?;
        let field = input_schema.field(row_ref_index);
        if field.name() != ROW_REF_COLUMN || field.data_type() != &DataType::UInt64 {
            return Err(DataFusionError::Internal(format!(
                "late materialization: phase 1's last column is {} {:?}, expected \
                 {ROW_REF_COLUMN} UInt64",
                field.name(),
                field.data_type()
            )));
        }
        let schema = Arc::clone(source.schema());
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering]);
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(LogsRowFetchExec {
            input,
            source,
            row_ref_index,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for LogsRowFetchExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsRowFetchExec {{ columns: {} }}",
            self.source.projected_columns()
        )
    }
}

impl DisplayAs for LogsRowFetchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        // The restored width and the row-ref column, not the 105 column names:
        // phase 1's `LogsScanExec` line already lists its narrow projection,
        // `__ravel_row_ref` included, so both phases are legible from one
        // `EXPLAIN` without one of them being a paragraph.
        write!(
            f,
            "LogsRowFetchExec: row_ref={ROW_REF_COLUMN}, restored_columns={}",
            self.source.projected_columns()
        )
    }
}

impl ExecutionPlan for LogsRowFetchExec {
    fn name(&self) -> &str {
        "LogsRowFetchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "LogsRowFetchExec takes exactly one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(LogsRowFetchExec::try_new(
            child,
            self.source.clone(),
            // The ordering is over the restored schema, which does not change
            // with the child.
            self.properties.output_ordering().cloned().ok_or_else(|| {
                DataFusionError::Internal("LogsRowFetchExec lost its output ordering".into())
            })?,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Nothing is known: the row count is whatever the TopK kept, which is at
    /// most its fetch but can be fewer, and no column statistic survives the
    /// re-read.
    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LogsRowFetchExec has one partition, asked for {partition}"
            )));
        }
        let reservation = MemoryConsumer::new("LogsRowFetchExec").register(context.memory_pool());
        let input = self.input.execute(0, context)?;
        let fut = Box::pin(fetch_rows(
            input,
            self.source.clone(),
            self.row_ref_index,
            reservation,
            FetchMetrics::new(&self.metrics, partition),
        ));
        Ok(Box::pin(RowFetchStream {
            schema: self.schema(),
            state: RowFetchState::Fetching(fut),
            emitted: 0,
        }))
    }
}

/// Read phase 1's rows, re-read the blocks they came from, and build the
/// restored output batches.
///
/// The reservation travels in and back out: it is grown inside for what this
/// phase holds (the fetched records first, then the batches built from them,
/// with the record term released as the records are dropped) and returned so
/// the stream can release each batch's share as it hands it downstream.
async fn fetch_rows(
    input: SendableRecordBatchStream,
    source: RowFetchSource,
    row_ref_index: usize,
    reservation: MemoryReservation,
    metrics: FetchMetrics,
) -> DFResult<(Vec<RecordBatch>, MemoryReservation)> {
    let batches: Vec<RecordBatch> = input.try_collect().await?;
    let mut packed: Vec<u64> = Vec::new();
    for batch in &batches {
        let column = batch.column(row_ref_index);
        let refs = column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "late materialization: row-ref column is {:?}, expected UInt64",
                    column.data_type()
                ))
            })?;
        for i in 0..refs.len() {
            if refs.is_null(i) {
                return Err(DataFusionError::Internal(
                    "late materialization: a phase-1 row carries a null row ref".into(),
                ));
            }
            packed.push(refs.value(i));
        }
    }
    metrics.row_refs.add(packed.len());
    if packed.is_empty() {
        // No winner, so no block to read: a statement whose predicate matched
        // nothing issues no phase-2 request at all.
        return Ok((Vec::new(), reservation));
    }

    // Group by block, keeping each row's output position so the result can be
    // restored to phase-1 order. `BTreeMap` so blocks are fetched in
    // (segment, block) order, which is the order their bytes sit in.
    let mut groups: BTreeMap<(usize, usize), Vec<(usize, usize)>> = BTreeMap::new();
    for (position, &value) in packed.iter().enumerate() {
        let row_ref = RowRef::unpack(value);
        groups
            .entry((row_ref.segment, row_ref.block))
            .or_default()
            .push((row_ref.row, position));
    }
    metrics.blocks_fetched.add(groups.len());
    let segments: HashSet<usize> = groups.keys().map(|&(segment, _)| segment).collect();
    metrics.segments_fetched.add(segments.len());

    // One fetch per block, `concurrency` in flight. Built as a `Vec` of
    // not-yet-polled futures for the same reason `compute_plan_counts` is: a
    // closure returning a future that borrows its argument cannot satisfy the
    // higher-ranked bound `buffered` needs here.
    let mut fetches = Vec::with_capacity(groups.len());
    for (&(segment, block), rows) in &groups {
        fetches.push(source.fetch_block(segment, block, rows));
    }
    let fetched: Vec<Vec<(usize, LogRecord)>> = futures::stream::iter(fetches)
        .buffered(source.concurrency().max(1))
        .try_collect()
        .await?;

    let mut slots: Vec<Option<LogRecord>> = vec![None; packed.len()];
    for (position, record) in fetched.into_iter().flatten() {
        // Two rows cannot share an output position: positions are the indices
        // of `packed`, and each contributes exactly one entry to one group.
        let slot = slots.get_mut(position).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "late materialization: fetched row for position {position} past {} winners",
                packed.len()
            ))
        })?;
        *slot = Some(record);
    }
    let mut records = Vec::with_capacity(slots.len());
    for (position, slot) in slots.into_iter().enumerate() {
        records.push(slot.ok_or_else(|| {
            DataFusionError::Internal(format!(
                "late materialization: no row fetched for winner {position}"
            ))
        })?);
    }

    let record_bytes = records_memory(&records);
    reservation.try_grow(record_bytes)?;
    let mut out = Vec::new();
    let mut batch_bytes = 0usize;
    let mut start = 0usize;
    while start < records.len() {
        let end = (start + RowFetchSource::BATCH_ROWS).min(records.len());
        let batch = source.build_batch(&records[start..end])?;
        batch_bytes = batch_bytes.saturating_add(batch.get_array_memory_size());
        out.push(batch);
        start = end;
    }
    // Both terms are live at once here, so charge the batches before releasing
    // the records rather than after.
    reservation.try_grow(batch_bytes)?;
    drop(records);
    reservation.shrink(record_bytes);
    Ok((out, reservation))
}

type FetchFuture =
    Pin<Box<dyn Future<Output = DFResult<(Vec<RecordBatch>, MemoryReservation)>> + Send>>;

enum RowFetchState {
    /// Draining phase 1 and re-reading its blocks.
    Fetching(FetchFuture),
    /// Handing the restored batches downstream one poll at a time, releasing
    /// each one's reservation as the next is emitted.
    Emitting {
        batches: VecDeque<RecordBatch>,
        reservation: MemoryReservation,
    },
    Done,
}

/// Phase 2's record-batch stream.
struct RowFetchStream {
    schema: SchemaRef,
    state: RowFetchState,
    /// Reservation bytes covering the batch handed downstream on the previous
    /// poll, released when the next one goes out (the same hand-off the scan's
    /// stream uses).
    emitted: usize,
}

impl Stream for RowFetchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                RowFetchState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((batches, reservation))) => {
                        this.state = RowFetchState::Emitting {
                            batches: batches.into(),
                            reservation,
                        };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = RowFetchState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                RowFetchState::Emitting {
                    batches,
                    reservation,
                } => {
                    reservation.shrink(std::mem::take(&mut this.emitted));
                    match batches.pop_front() {
                        Some(batch) => {
                            this.emitted = batch.get_array_memory_size();
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        None => this.state = RowFetchState::Done,
                    }
                }
                RowFetchState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for RowFetchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Round trip at the corners of every field, so a change to the bit split
    /// that loses a field's range fails here rather than in a query.
    #[test]
    fn row_refs_round_trip_at_every_field_boundary() {
        let max_segment = (1usize << SEGMENT_BITS) - 1;
        let max_block = (1usize << BLOCK_BITS) - 1;
        let max_row = (1usize << ROW_BITS) - 1;
        for row_ref in [
            RowRef {
                segment: 0,
                block: 0,
                row: 0,
            },
            RowRef {
                segment: max_segment,
                block: 0,
                row: 0,
            },
            RowRef {
                segment: 0,
                block: max_block,
                row: 0,
            },
            RowRef {
                segment: 0,
                block: 0,
                row: max_row,
            },
            RowRef {
                segment: max_segment,
                block: max_block,
                row: max_row,
            },
            RowRef {
                segment: 8_423,
                block: 17_730,
                row: 8_191,
            },
        ] {
            let packed = row_ref.pack().expect("in range");
            assert_eq!(RowRef::unpack(packed), row_ref, "round trip {row_ref:?}");
        }
    }

    /// The packed value orders by `(segment, block, row)`, which is what makes
    /// the `BTreeMap` grouping fetch blocks in the order their bytes sit in.
    #[test]
    fn packed_row_refs_order_by_segment_then_block_then_row() {
        let ascending = [
            RowRef {
                segment: 0,
                block: 0,
                row: 0,
            },
            RowRef {
                segment: 0,
                block: 0,
                row: 1,
            },
            RowRef {
                segment: 0,
                block: 1,
                row: 0,
            },
            RowRef {
                segment: 1,
                block: 0,
                row: 0,
            },
        ];
        let packed: Vec<u64> = ascending
            .iter()
            .map(|r| r.pack().expect("in range"))
            .collect();
        let mut sorted = packed.clone();
        sorted.sort_unstable();
        assert_eq!(packed, sorted);
    }

    /// A field past its range is a typed error, never a truncated ref that
    /// would address a real row somewhere else.
    #[test]
    fn an_out_of_range_field_refuses_rather_than_wrapping() {
        for (row_ref, field) in [
            (
                RowRef {
                    segment: 1 << SEGMENT_BITS,
                    block: 0,
                    row: 0,
                },
                "segment ordinal",
            ),
            (
                RowRef {
                    segment: 0,
                    block: 1 << BLOCK_BITS,
                    row: 0,
                },
                "block position",
            ),
            (
                RowRef {
                    segment: 0,
                    block: 0,
                    row: 1 << ROW_BITS,
                },
                "row position",
            ),
        ] {
            let err = row_ref.pack().expect_err("out of range");
            assert!(
                err.to_string().contains(field),
                "error names the offending field: {err}"
            );
        }
    }

    /// The three fields fill the `u64` exactly: a split that left a spare bit
    /// (or overflowed) would make `pack` lossy for the top field.
    #[test]
    fn the_bit_split_covers_exactly_sixty_four_bits() {
        assert_eq!(SEGMENT_BITS + BLOCK_BITS + ROW_BITS, 64);
    }
}
