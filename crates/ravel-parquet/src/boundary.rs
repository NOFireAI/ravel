//! The panic boundary around a Parquet table's scan.
//!
//! The parquet crate panics on some corrupt page headers and page bodies
//! instead of returning an error, and without a page index a filtered scan
//! decodes every page by its header. The page decode runs inside the scan
//! stream's `poll_next` (DataFusion's `FileStream` polls the decoder in
//! place and spawns no task for it), so a boundary around that stream sees
//! every such panic.
//!
//! The same boundary types the decoder's errors. An error the stream yields
//! whose chain holds no [`ParquetReadError`] and ends at a `ParquetError`, or
//! at the `ArrowError::ParquetError` the parquet crate converts one into,
//! becomes [`ParquetReadError::Corrupt`] naming the file the partition opened
//! last. Every other error, a budget refusal, a store failure or a DataFusion
//! error among them, passes through unchanged, and so does every error
//! yielded before the partition opened a file.

use std::any::Any;
use std::cell::Cell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TreeNodeRecursion;
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
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, StatisticsArgs,
};
use futures::{Stream, StreamExt};
use parquet::errors::ParquetError;

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
/// is polled, and an untyped decoder error it yields, into
/// [`ParquetReadError::Corrupt`] naming the table and the file.
///
/// It is transparent to the optimizer: its properties are the inner plan's,
/// and every rewrite DataFusion would apply to the inner plan (projection,
/// limit and filter pushdown, file-scan repartitioning) is applied to it
/// below this node. File-scan repartitioning needs no forwarding: DataFusion
/// repartitions a scan from the node directly above it, which is this one.
/// EXPLAIN shows one extra node, `ParquetPanicBoundaryExec: table=<table>`,
/// above the scan.
#[derive(Debug)]
pub(crate) struct ParquetPanicBoundaryExec {
    table: String,
    inner: Arc<dyn ExecutionPlan>,
}

impl ParquetPanicBoundaryExec {
    pub(crate) fn new(table: String, inner: Arc<dyn ExecutionPlan>) -> Self {
        ParquetPanicBoundaryExec { table, inner }
    }

    #[cfg(test)]
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

    // Visits nothing: this node holds no physical expression it evaluates.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DfResult<Arc<Statistics>> {
        input_stats.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("ParquetPanicBoundaryExec takes one child".to_string())
        })
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

    /// `err` as [`ParquetReadError::Corrupt`] naming the file this partition
    /// opened last when it is an untyped decoder error, and as is otherwise.
    fn decoder_failed(&self, err: DataFusionError) -> DataFusionError {
        match &self.opened {
            Some(key) if is_untyped_decoder_error(&err) => {
                DataFusionError::External(Box::new(ParquetReadError::Corrupt {
                    key: key.clone(),
                    message: format!(
                        "the Parquet decoder failed while scanning table {}: {err}",
                        self.table
                    ),
                }))
            }
            _ => err,
        }
    }
}

/// Whether `err` is one the Parquet decoder raised and nothing typed: its
/// chain holds no [`ParquetReadError`] and ends at a `ParquetError`, or at
/// the `ArrowError::ParquetError` the parquet crate converts one into.
fn is_untyped_decoder_error(err: &DataFusionError) -> bool {
    let mut current: &(dyn std::error::Error + 'static) = err;
    loop {
        if current.is::<ParquetReadError>() {
            return false;
        }
        match current.source() {
            Some(source) => current = source,
            None => break,
        }
    }
    current.is::<ParquetError>()
        || matches!(
            current.downcast_ref::<ArrowError>(),
            Some(ArrowError::ParquetError(_))
        )
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
            Ok(Poll::Ready(Some(Err(err)))) => Poll::Ready(Some(Err(self.decoder_failed(err)))),
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use datafusion::prelude::SessionConfig;
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::{ByteLimit, RequestLimit};

    use crate::error::ParquetReadError;
    use crate::limits::ReadLimits;
    use crate::test_support::{Fixture, footer_len_of, parquet_bytes, read_all, read_error};

    const GOOD: &str = "lake/t/good.parquet";
    const BAD: &str = "lake/t/bad.parquet";

    /// A file whose first value of column `b`, "four", has its PLAIN length
    /// prefix raised past the end of its page, so the decoder returns an
    /// error reading it rather than panicking.
    fn byte_array_longer_than_its_page() -> Vec<u8> {
        let mut bytes = parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]).to_vec();
        let prefix = [4, 0, 0, 0, b'f', b'o', b'u', b'r'];
        let at: Vec<usize> = bytes
            .windows(prefix.len())
            .enumerate()
            .filter(|(_, w)| *w == prefix)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(at.len(), 1, "one length-prefixed \"four\": {at:?}");
        bytes[at[0] + 3] = 0x7f;
        bytes
    }

    /// Scan `a` and `b` of a table of `GOOD`, intact, then `second` at
    /// `BAD`, in one partition, with reads admitted against `limits`. With
    /// `exhaust`, the budget is used up after the table is built and before
    /// the scan.
    async fn scan_good_then(
        second: Vec<u8>,
        memory: Arc<ravel_memory::MemoryBudget>,
        exhaust: bool,
    ) -> datafusion::error::Result<String> {
        let store = Arc::new(MemoryStore::new());
        let limits = ReadLimits::new(
            Arc::clone(&memory),
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        );
        let fixture =
            Fixture::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>).with_limits(limits);
        let good = fixture
            .put_file(
                &store,
                GOOD,
                parquet_bytes(&[1, 2, 3], &["one", "two", "six"]),
                false,
            )
            .await;
        let (size, footer_len) = (second.len() as u64, footer_len_of(&second));
        let bad = fixture
            .put_raw(&store, BAD, Bytes::from(second), size, footer_len)
            .await;
        let table = fixture.provider("t", 1, vec![good, bad], false).await;
        let ctx = fixture.session_with(
            SessionConfig::new().with_target_partitions(1),
            &[("t", table)],
        );
        if exhaust {
            memory.reserve_unchecked(memory.limit());
        }
        read_all(&ctx, "t", &["a", "b"]).await
    }

    /// A decoder error in the second file of a one-partition scan is
    /// `Corrupt` naming that file, not the first one the partition opened.
    #[tokio::test]
    async fn a_decoder_error_in_the_second_file_is_corrupt_naming_it() {
        let memory = Arc::new(ravel_memory::MemoryBudget::new(1 << 30));
        let err = scan_good_then(byte_array_longer_than_its_page(), memory, false)
            .await
            .expect_err("the corrupt file must fail the scan");
        match read_error(&err) {
            Some(ParquetReadError::Corrupt { key, message }) => {
                assert_eq!(key, BAD, "{err}");
                assert!(
                    message.contains("the Parquet decoder failed while scanning table t: "),
                    "{message}"
                );
            }
            other => panic!("expected Corrupt for {BAD}, got {other:?} from {err:?}"),
        }
    }

    /// A read the memory budget refuses during the scan surfaces as
    /// `MemoryExhausted`, not as `Corrupt`.
    #[tokio::test]
    async fn a_memory_refusal_during_the_scan_stays_memory_exhausted() {
        let memory = Arc::new(ravel_memory::MemoryBudget::new(1 << 30));
        let intact = parquet_bytes(&[4, 5, 6], &["four", "five", "sixx"]).to_vec();
        let err = scan_good_then(intact, memory, true)
            .await
            .expect_err("an exhausted budget must fail the scan");
        assert!(
            matches!(
                read_error(&err),
                Some(ParquetReadError::MemoryExhausted { .. })
            ),
            "{err:?}"
        );
    }
}
