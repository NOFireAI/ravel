//! Answer two more ClickBench aggregate shapes straight from ADR-0850's
//! per-object column statistics, with zero data-block GETs (issue #850, item
//! 2 of epic #849; the first shape, predicate-free/ts-contained `COUNT(*)`
//! and `MIN`/`MAX`, is [`crate::logs_scan::LogsScanExec::partition_statistics`]
//! feeding DataFusion's own built-in `AggregateStatistics` physical optimizer
//! rule -- that rule requires zero `GROUP BY` and zero residual filter, so it
//! can never fire for either shape here and this rule is additive, not
//! competing):
//!
//! - **q02**: `COUNT(*) FROM logs WHERE <declared column> <> <literal>`, one
//!   residual `FilterExec` over a [`LogsScanExec`], no `GROUP BY`. Answered by
//!   [`LogsScanExec::declared_not_equal_count`]: `non_null_count -
//!   count(value = literal)`, summed across every touched segment's exact
//!   dictionary.
//! - **q08**: `<declared column>, COUNT(*) FROM logs GROUP BY <declared
//!   column>`, a plain column group key directly over a [`LogsScanExec`], no
//!   filter (a filter combined with a `GROUP BY` is out of scope for this
//!   rule). Answered by [`LogsScanExec::declared_group_counts`]: every
//!   touched segment's exact dictionary merged by value, plus the summed
//!   `null_count` as a synthetic NULL group when it is nonzero.
//!
//! Both delegate the actual statistics read to `LogsScanExec` (the crate's
//! established split: `logs_scan.rs` owns every column-stats read,
//! sibling optimizer-rule modules only match plan shape and splice in a
//! replacement, the same way [`crate::late_materialization`] calls
//! `scan.late_materialization_candidate()` rather than reaching into
//! `LogsScanExec`'s fields). A `None` from either method means the ADR-0850
//! safety lemma requires falling back to scanning (no loaded column-stats
//! object, an unsupported declared type, a segment outside the loaded stats,
//! a pending selective erasure, or -- specific to this path -- a segment
//! whose dictionary was omitted for exceeding the cardinality ceiling): this
//! rule simply declines the rewrite and the unmodified plan runs, scanning
//! exactly as it would have without ADR-0850.
//!
//! [`MetadataOnlyExec`] is the observable marker (issue #850 deliverable 3):
//! a purpose-named leaf `ExecutionPlan` whose `EXPLAIN` line always reads
//! `MetadataOnlyExec: metadata_only=true, rows=<n>`, distinct from the
//! generic `PlaceholderRowExec`/`ProjectionExec` pair DataFusion's own
//! built-in rule (and any future one) produces, so a test can assert this
//! specific optimization fired by matching that exact string rather than a
//! node type multiple rules could have produced.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    self, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;

use crate::logs_scan::{DeclaredGroupCounts, LogsScanExec};
use crate::logs_schema::FIRST_DECLARED_COL;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const METADATA_ONLY_AGGREGATE_RULE: &str = "metadata_only_aggregate";

/// Rewrites the q02 (`COUNT(*) WHERE <declared column> <> <literal>`) and q08
/// (`GROUP BY <declared column>, COUNT(*)`) aggregate shapes into a
/// [`MetadataOnlyExec`] over ADR-0850 statistics, when it can prove the
/// answer exact. See the module docs for the shapes and every fallback
/// condition.
#[derive(Debug, Default)]
pub struct MetadataOnlyAggregate;

impl PhysicalOptimizerRule for MetadataOnlyAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(rewrite).data()
    }

    fn name(&self) -> &str {
        METADATA_ONLY_AGGREGATE_RULE
    }

    fn schema_check(&self) -> bool {
        // The replacement batch is built directly from the outer aggregate's
        // own schema, but let DataFusion assert that rather than trusting it.
        true
    }
}

/// A pass-through node between the aggregate stages, or between the raw
/// aggregate and the scan: partition shuffles the rewrite walks through
/// without needing to remap anything, since the whole subtree above the scan
/// is about to be discarded wholesale rather than rebuilt in place. Unlike
/// [`crate::late_materialization`]'s allowlist, a hash repartition is
/// admitted here for exactly that reason: the standard two-stage `GROUP BY`
/// plan hash-repartitions by the group key between the partial and final
/// aggregate, and this rule has no expression to remap across it.
fn shuffle_child(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.fetch().is_some() {
        return None;
    }
    if plan.is::<CoalescePartitionsExec>() || plan.is::<CooperativeExec>() {
        return plan.children().first().map(|c| Arc::clone(c));
    }
    if plan.downcast_ref::<RepartitionExec>().is_some() {
        return plan.children().first().map(|c| Arc::clone(c));
    }
    None
}

/// A pass-through node between the raw aggregate and the scan, in top-down
/// order: at most one residual filter, plus the same opaque whitelist
/// [`shuffle_child`] admits.
enum ScanChainStep {
    Filter(Arc<FilterExec>),
    Opaque,
}

fn classify_scan_chain(plan: &Arc<dyn ExecutionPlan>) -> Option<ScanChainStep> {
    if plan.fetch().is_some() {
        return None;
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if filter.projection().is_some() {
            return None;
        }
        return Some(ScanChainStep::Filter(Arc::new(filter.clone())));
    }
    if plan.is::<CoalescePartitionsExec>() || plan.is::<CooperativeExec>() {
        return Some(ScanChainStep::Opaque);
    }
    if plan.downcast_ref::<RepartitionExec>().is_some() {
        return Some(ScanChainStep::Opaque);
    }
    None
}

/// `Some(None)` for a bare `COUNT(*)` (q02 shape candidate), `Some(Some(k))`
/// for a single plain-`Column` group key at schema index `k` (q08 shape
/// candidate), `None` for anything else this rule does not attempt: a
/// grouping set, a null-expr placeholder, more than one group column, or a
/// group expression that is not a plain column reference.
fn group_index(group_by: &physical_plan::aggregates::PhysicalGroupBy) -> Option<Option<usize>> {
    if group_by.has_grouping_set() || !group_by.null_expr().is_empty() {
        return None;
    }
    match group_by.expr() {
        [] => Some(None),
        [(expr, _)] => {
            let column = expr.downcast_ref::<Column>()?;
            Some(Some(column.index()))
        }
        _ => None,
    }
}

/// Whether `agg`'s aggregate list is exactly one `COUNT(*)`-shaped
/// expression: physically `count(<non-null literal>)` (DataFusion's
/// `count_all()`), never a zero-argument call, so this is the exact,
/// version-grounded shape check rather than a name guess. `is_distinct` and
/// a per-aggregate `FILTER (WHERE ...)` clause are both refused: neither
/// changes what `non_null_count` means, but this rule does not attempt to
/// reason about them.
fn is_count_star(agg: &physical_plan::aggregates::AggregateExec) -> bool {
    let aggr_exprs = agg.aggr_expr();
    let filter_exprs = agg.filter_expr();
    if aggr_exprs.len() != 1 || filter_exprs.len() != 1 {
        return false;
    }
    if filter_exprs[0].is_some() {
        return false;
    }
    let expr = &aggr_exprs[0];
    if expr.is_distinct() {
        return false;
    }
    if expr.fun().name() != "count" {
        return false;
    }
    let args = expr.expressions();
    args.len() == 1 && args[0].downcast_ref::<Literal>().is_some()
}

/// A `<declared column> <> <literal>` (or reversed) predicate's column
/// index and literal value, or `None` for any other shape.
fn not_equal_literal(predicate: &Arc<dyn PhysicalExpr>) -> Option<(usize, ScalarValue)> {
    let binary = predicate.downcast_ref::<BinaryExpr>()?;
    if *binary.op() != Operator::NotEq {
        return None;
    }
    let left = binary.left();
    let right = binary.right();
    if let (Some(col), Some(lit)) = (
        left.downcast_ref::<Column>(),
        right.downcast_ref::<Literal>(),
    ) {
        return Some((col.index(), lit.value().clone()));
    }
    if let (Some(lit), Some(col)) = (
        left.downcast_ref::<Literal>(),
        right.downcast_ref::<Column>(),
    ) {
        return Some((col.index(), lit.value().clone()));
    }
    None
}

/// Try to rewrite one node. Returns it unchanged unless it is a Final-output
/// `COUNT(*)` aggregate, with or without a single declared-column group key,
/// reachable down to a [`LogsScanExec`] in one of the two shapes the module
/// docs describe, AND [`LogsScanExec::declared_not_equal_count`] /
/// [`LogsScanExec::declared_group_counts`] can prove the answer exact.
fn rewrite(node: Arc<dyn ExecutionPlan>) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = node.downcast_ref::<physical_plan::aggregates::AggregateExec>() else {
        return Ok(Transformed::no(node));
    };
    if agg.mode().output_mode() != physical_plan::aggregates::AggregateOutputMode::Final {
        return Ok(Transformed::no(node));
    }

    // Descend to the Raw-input-mode aggregate: `agg` itself when it is
    // Single/SinglePartitioned, or found through a chain of partition-shuffle
    // nodes over a Final/FinalPartitioned stage's Partial input.
    let mut current: Arc<dyn ExecutionPlan> = Arc::clone(&node);
    let (group, raw_input) = loop {
        if let Some(a) = current.downcast_ref::<physical_plan::aggregates::AggregateExec>() {
            if a.mode().input_mode() == physical_plan::aggregates::AggregateInputMode::Raw {
                let Some(group) = group_index(a.group_expr()) else {
                    return Ok(Transformed::no(node));
                };
                if !is_count_star(a) {
                    return Ok(Transformed::no(node));
                }
                break (group, Arc::clone(a.input()));
            }
            current = Arc::clone(a.input());
            continue;
        }
        let Some(child) = shuffle_child(&current) else {
            return Ok(Transformed::no(node));
        };
        current = child;
    };

    // Walk down from the raw stage's input to the scan, admitting at most one
    // residual filter.
    let mut current = raw_input;
    let mut filter: Option<Arc<FilterExec>> = None;
    let scan = loop {
        if let Some(scan) = current.downcast_ref::<LogsScanExec>() {
            break scan;
        }
        let Some(step) = classify_scan_chain(&current) else {
            return Ok(Transformed::no(node));
        };
        if let ScanChainStep::Filter(f) = &step {
            if filter.is_some() {
                return Ok(Transformed::no(node));
            }
            filter = Some(Arc::clone(f));
        }
        let Some(child) = current.children().first().map(|c| Arc::clone(c)) else {
            return Ok(Transformed::no(node));
        };
        current = child;
    };

    let schema = node.schema();
    let batch = match group {
        // q02: COUNT(*) WHERE <declared column> <> <literal>. A predicate-
        // free COUNT(*) here is already handled by DataFusion's own built-in
        // AggregateStatistics rule, so require the filter this rule exists
        // for rather than reproducing that case.
        None => {
            let Some(filter) = filter else {
                return Ok(Transformed::no(node));
            };
            let Some((col_index, literal)) = not_equal_literal(filter.predicate()) else {
                return Ok(Transformed::no(node));
            };
            if col_index < FIRST_DECLARED_COL || literal.is_null() {
                return Ok(Transformed::no(node));
            }
            let Some(count) =
                scan.declared_not_equal_count(col_index - FIRST_DECLARED_COL, &literal)
            else {
                return Ok(Transformed::no(node));
            };
            let Some(count) = count_to_i64(count) else {
                return Ok(Transformed::no(node));
            };
            match count_star_batch(&schema, count) {
                Some(batch) => batch,
                None => return Ok(Transformed::no(node)),
            }
        }
        // q08: <declared column>, COUNT(*) GROUP BY <declared column>. A
        // filter combined with a GROUP BY is out of scope for this rule.
        Some(col_index) => {
            if filter.is_some() || col_index < FIRST_DECLARED_COL {
                return Ok(Transformed::no(node));
            }
            let Some(counts) = scan.declared_group_counts(col_index - FIRST_DECLARED_COL) else {
                return Ok(Transformed::no(node));
            };
            match group_counts_batch(&schema, counts) {
                Some(batch) => batch,
                None => return Ok(Transformed::no(node)),
            }
        }
    };

    Ok(Transformed::new(
        Arc::new(MetadataOnlyExec::new(schema, batch)) as Arc<dyn ExecutionPlan>,
        true,
        datafusion::common::tree_node::TreeNodeRecursion::Jump,
    ))
}

/// `count` as the single-row, single-column `COUNT(*)` result batch under
/// `schema`. `None` when `schema` is not the one-column shape this rewrite
/// expects (an internal-error case: the raw stage's `COUNT(*)` shape check
/// already passed, so a schema of any other width would mean this rule's own
/// assumptions about the plan are wrong), which the caller treats as a
/// decline rather than a panic.
fn count_star_batch(schema: &SchemaRef, count: i64) -> Option<RecordBatch> {
    if schema.fields().len() != 1 {
        return None;
    }
    let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
    RecordBatch::try_new(Arc::clone(schema), vec![array]).ok()
}

/// `counts` as the two-column `<declared column>, COUNT(*)` result batch
/// under `schema` (group value column, then count column), with a synthetic
/// NULL group row appended when `counts.null_count > 0`. `None` on a schema
/// shape mismatch (see [`count_star_batch`]) or a per-group count that does
/// not fit `i64` (`COUNT(*)`'s declared output type): both are declines, not
/// partial answers, since the ADR-0850 safety lemma is exactness or
/// fallback, never a truncated result.
fn group_counts_batch(schema: &SchemaRef, counts: DeclaredGroupCounts) -> Option<RecordBatch> {
    if schema.fields().len() != 2 {
        return None;
    }
    let mut values: Vec<ScalarValue> = Vec::with_capacity(counts.counts.len() + 1);
    let mut tallies: Vec<i64> = Vec::with_capacity(counts.counts.len() + 1);
    for (value, count) in counts.counts {
        values.push(value);
        tallies.push(count_to_i64(count)?);
    }
    if counts.null_count > 0 {
        values.push(ScalarValue::try_from(schema.field(0).data_type()).ok()?);
        tallies.push(count_to_i64(counts.null_count)?);
    }
    let group_array = ScalarValue::iter_to_array(values).ok()?;
    let count_array: ArrayRef = Arc::new(Int64Array::from(tallies));
    RecordBatch::try_new(Arc::clone(schema), vec![group_array, count_array]).ok()
}

/// `count` as `i64`, `COUNT(*)`'s declared output type. `None` on overflow,
/// which the caller treats as a decline (see [`group_counts_batch`]'s doc):
/// astronomically unlikely for a real row count, but exactness-or-fallback
/// admits no silent truncation.
fn count_to_i64(count: u64) -> Option<i64> {
    i64::try_from(count).ok()
}

/// A single fixed [`RecordBatch`] answer to a metadata-only aggregate,
/// produced entirely from ADR-0850 catalog statistics with zero data-block
/// GETs. The `EXPLAIN` marker issue #850 deliverable 3 requires: `name()`
/// and [`DisplayAs`] both read `MetadataOnlyExec`, distinct from the generic
/// nodes DataFusion's own built-in statistics rule produces, so a test can
/// attribute a plan specifically to this optimization.
#[derive(Debug)]
pub struct MetadataOnlyExec {
    schema: SchemaRef,
    batch: RecordBatch,
    properties: Arc<PlanProperties>,
}

impl MetadataOnlyExec {
    fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        MetadataOnlyExec {
            schema,
            batch,
            properties,
        }
    }
}

impl DisplayAs for MetadataOnlyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MetadataOnlyExec: metadata_only=true, rows={}",
                    self.batch.num_rows()
                )
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for MetadataOnlyExec {
    fn name(&self) -> &'static str {
        "MetadataOnlyExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "MetadataOnlyExec: expected zero children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MetadataOnlyExec: invalid partition {partition}, expected 0"
            )));
        }
        Ok(Box::pin(physical_plan::memory::MemoryStream::try_new(
            vec![self.batch.clone()],
            Arc::clone(&self.schema),
            None,
        )?))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(
            physical_plan::common::compute_record_batch_statistics(
                &[vec![self.batch.clone()]],
                &self.schema,
                None,
            ),
        ))
    }
}
