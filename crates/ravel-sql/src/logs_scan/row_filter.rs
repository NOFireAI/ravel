//! The scan's own row filter (ADR-0093, exact not-equal amendment): the
//! conjuncts of [`crate::logs_pushdown::LogsPushdown::row_filters`] as the
//! physical expressions the session built, evaluated on each batch after the
//! column build and before the batch is emitted or charged.
//!
//! The provider binds each expression against the resolved full `logs` schema.
//! [`LogsScanExec::build`](super::LogsScanExec) widens the build projection by
//! the columns they read, then [`RowFilters::bind`] rebinds each column by name
//! to its position in that build schema
//! (`the_filter_reads_its_own_column_when_the_projection_is_widened`). The
//! batch is filtered with `filter_record_batch`, the call `FilterExec` makes,
//! so a NULL verdict drops the row, and then narrowed to the output columns
//! (`a_filter_column_outside_the_projection_is_built_and_dropped`). A
//! dictionary column keeps its values array through the filter
//! (`a_row_filter_keeps_the_served_plain_blocks_memo_values`).

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_plan::metrics::Count;

/// The row filter conjuncts a scan carries, bound to the resolved full `logs`
/// schema. Empty for every scan whose statement has no such conjunct, which
/// leaves the scan's build and output identical.
#[derive(Clone, Default)]
pub(crate) struct RowFilters {
    exprs: Arc<Vec<Arc<dyn PhysicalExpr>>>,
}

impl RowFilters {
    pub(crate) fn new(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        RowFilters {
            exprs: Arc::new(exprs),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    /// The names of the columns the conjuncts read, each once, in first-use
    /// order.
    pub(super) fn column_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for e in self.exprs.iter() {
            let mut columns: Vec<Column> = collect_columns(e).into_iter().collect();
            columns.sort_by_key(Column::index);
            for c in columns {
                if !names.iter().any(|n| n == c.name()) {
                    names.push(c.name().to_string());
                }
            }
        }
        names
    }

    /// The conjuncts rebound by column name to `build_schema`, with the build
    /// columns `keep` names, in order, forming each emitted batch of `output`.
    /// `keep` is `None` when the build and output columns are the same.
    pub(super) fn bind(
        &self,
        build_schema: &SchemaRef,
        keep: Option<Vec<usize>>,
        output: SchemaRef,
    ) -> DFResult<Option<Arc<BoundRowFilters>>> {
        if self.exprs.is_empty() {
            return Ok(None);
        }
        let exprs = self
            .exprs
            .iter()
            .map(|e| {
                Arc::clone(e)
                    .transform(|node| {
                        let Some(column) = node.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(node));
                        };
                        let index = build_schema.index_of(column.name())?;
                        Ok(Transformed::yes(
                            Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
                        ))
                    })
                    .data()
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Some(Arc::new(BoundRowFilters {
            exprs,
            keep,
            output,
        })))
    }
}

/// The conjuncts with their columns by name only: the indices they are bound
/// to here are full-schema ones, which match neither the scan's output
/// projection nor its build schema.
impl fmt::Display for RowFilters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, e) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            e.fmt_sql(f)?;
        }
        Ok(())
    }
}

/// [`RowFilters`] bound to one scan's build schema.
pub(crate) struct BoundRowFilters {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    keep: Option<Vec<usize>>,
    output: SchemaRef,
}

impl BoundRowFilters {
    /// `batch`, built in the build schema, with every row some conjunct does
    /// not hold true for removed and the build-only columns dropped. Adds the
    /// rows removed to `dropped`.
    pub(super) fn apply(&self, batch: RecordBatch, dropped: &Count) -> DFResult<RecordBatch> {
        let built = batch.num_rows();
        let mut batch = batch;
        for e in &self.exprs {
            let verdict = e.evaluate(&batch)?.into_array(batch.num_rows())?;
            if verdict.data_type() != &DataType::Boolean {
                return Err(DataFusionError::Internal(format!(
                    "logs scan row filter {e} evaluated to {}",
                    verdict.data_type()
                )));
            }
            batch = filter_record_batch(&batch, verdict.as_boolean())?;
        }
        dropped.add(built - batch.num_rows());
        let Some(keep) = &self.keep else {
            return Ok(batch);
        };
        let columns = keep.iter().map(|&i| Arc::clone(batch.column(i))).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output),
            columns,
            &options,
        )?)
    }
}

/// `batches` with `filters` applied to each, keeping only the batches a row
/// survived in, so a block whose every row the filter drops holds and charges
/// nothing.
pub(super) fn filter_batches(
    filters: &BoundRowFilters,
    batches: Vec<RecordBatch>,
    dropped: &Count,
) -> DFResult<Vec<RecordBatch>> {
    let mut kept = Vec::with_capacity(batches.len());
    for batch in batches {
        let batch = filters.apply(batch, dropped)?;
        if batch.num_rows() > 0 {
            kept.push(batch);
        }
    }
    Ok(kept)
}
