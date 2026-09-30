//! The panic boundary around a Parquet table's scan.
//!
//! The parquet crate panics on some corrupt page headers and page bodies
//! instead of returning an error, and without a page index a filtered scan
//! decodes every page by its header. The page decode runs inside the scan
//! stream's `poll_next` (DataFusion's `FileStream` polls the decoder in
//! place and spawns no task for it), so a boundary around that stream sees
//! every such panic.

use std::any::Any;
use std::cell::Cell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

use crate::error::ParquetReadError;

thread_local! {
    /// The key of the file the scan stream this thread is polling opened
    /// last. [`BoundaryStream`] installs its own value for the length of one
    /// poll and takes it back after.
    static OPENED: Cell<Option<String>> = const { Cell::new(None) };
}

/// Record `key` as the file the stream being polled on this thread opened.
///
/// A partition's scan opens its next file only once the previous one is
/// finished, so the file a stream opened last is the one it is decoding.
pub(crate) fn record_opened(key: String) {
    OPENED.set(Some(key));
}

/// A Parquet table's scan, whose output stream turns a panic raised while it
/// is polled into [`ParquetReadError::Corrupt`] naming the table and the file.
///
/// It is transparent to the optimizer: its properties are the inner plan's,
/// and every rewrite DataFusion would apply to the inner plan (projection,
/// limit and filter pushdown, file-scan repartitioning) is applied to it
/// below this node. EXPLAIN shows one extra node,
/// `ParquetPanicBoundaryExec: table=<table>`, above the scan.
#[derive(Debug)]
pub(crate) struct ParquetPanicBoundaryExec {
    table: String,
    inner: Arc<dyn ExecutionPlan>,
}

impl ParquetPanicBoundaryExec {
    pub(crate) fn new(table: String, inner: Arc<dyn ExecutionPlan>) -> Self {
        ParquetPanicBoundaryExec { table, inner }
    }

    pub(crate) fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }

    fn over(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(ParquetPanicBoundaryExec::new(self.table.clone(), inner))
    }
}

impl DisplayAs for ParquetPanicBoundaryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ParquetPanicBoundaryExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => write!(f, "table={}", self.table),
        }
    }
}

impl ExecutionPlan for ParquetPanicBoundaryExec {
    fn name(&self) -> &str {
        "ParquetPanicBoundaryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    /// Rows pass through unchanged, but `false` keeps sort pushdown from
    /// placing a sort between this node and the scan.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [inner]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("ParquetPanicBoundaryExec takes one child".to_string())
        })?;
        Ok(self.over(inner))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .inner
            .repartitioned(target_partitions, config)?
            .map(|inner| self.over(inner)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let inner = self.inner.execute(partition, context)?;
        Ok(Box::pin(BoundaryStream {
            schema: inner.schema(),
            inner,
            table: self.table.clone(),
            opened: None,
            panicked: false,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit).map(|inner| self.over(inner))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    /// The projection is valid against the inner plan too, whose schema this
    /// node's is.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .inner
            .try_swapping_with_projection(projection)?
            .map(|inner| self.over(inner)))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DfResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DfResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_new_state(state)
            .map(|inner| self.over(inner))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DfResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        self.inner
            .try_pushdown_sort(order)?
            .try_map(|inner| Ok(self.over(inner)))
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_preserve_order(preserve_order)
            .map(|inner| self.over(inner))
    }
}

/// One partition of the scan, polled under `catch_unwind`.
struct BoundaryStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    table: String,
    /// The file this partition opened last, carried between polls.
    opened: Option<String>,
    panicked: bool,
}

impl BoundaryStream {
    fn corrupt(&self, payload: &(dyn Any + Send)) -> DataFusionError {
        let reason = if let Some(message) = payload.downcast_ref::<&'static str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "a panic with a non-string payload".to_string()
        };
        let error = ParquetReadError::Corrupt {
            key: self
                .opened
                .clone()
                .unwrap_or_else(|| "(unknown)".to_string()),
            message: format!(
                "the Parquet decoder panicked while scanning table {}: {reason}",
                self.table
            ),
        };
        DataFusionError::External(Box::new(error))
    }
}

impl Stream for BoundaryStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.panicked {
            return Poll::Ready(None);
        }
        // `AssertUnwindSafe` is honest because a stream that panicked is
        // never polled again: the state the panic may have left inconsistent
        // is only dropped.
        let outer = OPENED.replace(self.opened.take());
        let polled = catch_unwind(AssertUnwindSafe(|| self.inner.poll_next_unpin(cx)));
        self.opened = OPENED.replace(outer);
        match polled {
            Ok(next) => next,
            Err(payload) => {
                self.panicked = true;
                // `payload.as_ref()`: `&payload` would downcast the box itself.
                Poll::Ready(Some(Err(self.corrupt(payload.as_ref()))))
            }
        }
    }
}

impl RecordBatchStream for BoundaryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
