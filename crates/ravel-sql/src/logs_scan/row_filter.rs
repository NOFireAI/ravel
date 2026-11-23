//! The scan's own row filter (ADR-0093, exact not-equal and I64 equality
//! amendments): the conjuncts of
//! [`crate::logs_pushdown::LogsPushdown::row_filters`] as the physical
//! expressions the session built, evaluated before a batch is emitted or
//! charged. The columnar path evaluates them on each batch it built. The row
//! path builds only the columns they read ([`BoundRowFilters::reads`]),
//! evaluates them there ([`BoundRowFilters::survivors`]), and builds the
//! other columns for the surviving rows only (`build_row_batch_filtered`).
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

use datafusion::arrow::array::{AsArray, UInt32Array};
use datafusion::arrow::compute::{filter, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, UInt32Type};
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

    /// The conjuncts with `rewrite` applied to each, typed against `schema`
    /// (the resolved full `logs` schema), or `None` when it changes none.
    pub(super) fn rewritten(
        &self,
        schema: &Schema,
        rewrite: RewriteExpr,
    ) -> DFResult<Option<RowFilters>> {
        Ok(rewrite_all(&self.exprs, schema, rewrite)?.map(RowFilters::new))
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
        let mut reads: Vec<usize> = Vec::new();
        for e in &exprs {
            for c in collect_columns(e) {
                if !reads.contains(&c.index()) {
                    reads.push(c.index());
                }
            }
        }
        reads.sort_unstable();
        let read_schema = Arc::new(build_schema.project(&reads)?);
        let read_exprs = exprs
            .iter()
            .map(|e| {
                Arc::clone(e)
                    .transform(|node| {
                        let Some(column) = node.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(node));
                        };
                        let index = read_schema.index_of(column.name())?;
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
            reads,
            read_schema,
            read_exprs,
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

/// An expression rewrite that keeps the columns the expression reads, typed
/// against the schema the expression is bound to.
pub(crate) type RewriteExpr =
    fn(Arc<dyn PhysicalExpr>, &Schema) -> DFResult<Transformed<Arc<dyn PhysicalExpr>>>;

/// `exprs` with `rewrite` applied to each, or `None` when it changes none.
fn rewrite_all(
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    rewrite: RewriteExpr,
) -> DFResult<Option<Vec<Arc<dyn PhysicalExpr>>>> {
    let mut changed = false;
    let out = exprs
        .iter()
        .map(|e| {
            let t = rewrite(Arc::clone(e), schema)?;
            changed |= t.transformed;
            Ok(t.data)
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(changed.then_some(out))
}

/// [`RowFilters`] bound to one scan's build schema.
pub(crate) struct BoundRowFilters {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    keep: Option<Vec<usize>>,
    output: SchemaRef,
    /// The build-schema positions of the columns `exprs` read, ascending.
    reads: Vec<usize>,
    /// The build schema projected to `reads`.
    read_schema: SchemaRef,
    /// `exprs` rebound to `read_schema`.
    read_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl BoundRowFilters {
    /// These filters with `rewrite` applied to every conjunct, typed against
    /// `build_schema` (the schema they are bound to) and, for the read-only
    /// copies, against [`Self::read_schema`]. A rewrite that keeps the columns
    /// an expression reads keeps every position recorded here valid.
    pub(super) fn rewritten(
        &self,
        build_schema: &Schema,
        rewrite: RewriteExpr,
    ) -> DFResult<BoundRowFilters> {
        let exprs =
            rewrite_all(&self.exprs, build_schema, rewrite)?.unwrap_or_else(|| self.exprs.clone());
        let read_exprs = rewrite_all(&self.read_exprs, &self.read_schema, rewrite)?
            .unwrap_or_else(|| self.read_exprs.clone());
        Ok(BoundRowFilters {
            exprs,
            keep: self.keep.clone(),
            output: Arc::clone(&self.output),
            reads: self.reads.clone(),
            read_schema: Arc::clone(&self.read_schema),
            read_exprs,
        })
    }

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
        self.narrow(batch)
    }

    /// The build-schema positions of the columns the conjuncts read,
    /// ascending: the columns of [`Self::read_schema`].
    pub(super) fn reads(&self) -> &[usize] {
        &self.reads
    }

    /// The build schema projected to [`Self::reads`].
    pub(super) fn read_schema(&self) -> &SchemaRef {
        &self.read_schema
    }

    /// The positions of `batch`'s rows that every conjunct holds true for,
    /// ascending. `batch` holds the [`Self::read_schema`] columns only. The
    /// conjuncts run one after another over the rows the previous ones kept,
    /// as in [`Self::apply`], so a NULL verdict drops the row.
    pub(super) fn survivors(&self, batch: RecordBatch) -> DFResult<Vec<usize>> {
        let rows = u32::try_from(batch.num_rows()).map_err(|_| {
            DataFusionError::Internal("logs scan row filter chunk exceeds u32 rows".into())
        })?;
        let mut positions = UInt32Array::from_iter_values(0..rows);
        let mut batch = batch;
        for e in &self.read_exprs {
            let verdict = e.evaluate(&batch)?.into_array(batch.num_rows())?;
            if verdict.data_type() != &DataType::Boolean {
                return Err(DataFusionError::Internal(format!(
                    "logs scan row filter {e} evaluated to {}",
                    verdict.data_type()
                )));
            }
            batch = filter_record_batch(&batch, verdict.as_boolean())?;
            positions = filter(&positions, verdict.as_boolean())?
                .as_primitive::<UInt32Type>()
                .clone();
        }
        Ok(positions.values().iter().map(|&p| p as usize).collect())
    }

    /// `batch`, in the build schema, narrowed to the output columns.
    pub(super) fn narrow(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
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
