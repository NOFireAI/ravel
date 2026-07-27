//! `RsegDedupExec`: single-partition streaming dedup over the merged,
//! `(series_id, ts)`-sorted stream.
//!
//! For each `(series_id, ts)` group of adjacent rows it emits the winning
//! candidate under the full dedup total order (docs/catalog-and-mvcc.md,
//! `is_greater` in crates/ravel-query/src/engine.rs): greatest
//! `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`, ties broken
//! by greatest `value.to_bits()`. Because that order is total over the
//! provenance tuple plus the value bits, the winner does not depend on
//! arrival order within a group, so merge interleaving at equal keys cannot
//! change the result (review F6). The operator holds one candidate of state
//! per in-flight group, strips the provenance columns, emits the public
//! schema, and counts yielded (post-dedup) rows against `max_samples`.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    FixedSizeBinaryArray, Float64Array, Int64Array, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};

use crate::error::SqlError;
use crate::schema::{PUBLIC_COLUMNS, public_schema};

/// The winner-selection key: greater wins. Value bits are the final
/// tiebreak, exactly as `is_greater` orders candidates.
type DedupKey = (i64, u64, u64, u32, u64);

/// Streaming dedup operator. Input is the single-partition, `(series_id,
/// ts)`-sorted stream produced by `SortPreservingMergeExec`.
#[derive(Debug)]
pub struct RsegDedupExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    max_samples: usize,
    properties: Arc<PlanProperties>,
    /// The (series_id, ts) ordering this operator requires of its input,
    /// expressed over the *input's* schema (internal, with provenance
    /// columns) rather than this operator's own output schema. Without
    /// declaring this, the DataFusion optimizer sees no required
    /// distribution or ordering on the sole child and is free to strip
    /// `SortPreservingMergeExec` entirely (`EnforceDistribution` only
    /// re-adds what is required), collapsing the scan's partitions away
    /// without even a `CoalescePartitionsExec` in its place -- silently
    /// executing only one scan partition. Found by Opus checkpoint review
    /// of B1 (issue #20): reproduced via `SessionContext`/`DataFrame`,
    /// which runs the optimizer, vs. the crate's own tests, which all call
    /// `collect()` directly on a hand-built plan and never exercise it.
    input_ordering: OrderingRequirements,
}

impl RsegDedupExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, max_samples: usize) -> DFResult<Self> {
        let schema = public_schema();
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_exprs = ["series_id", "ts"]
            .into_iter()
            .map(|name| Ok(PhysicalSortExpr::new(col(name, &schema)?, asc)))
            .collect::<DFResult<Vec<_>>>()?;
        let ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty dedup ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(&schema), vec![ordering]);
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        // Same (series_id, ts) ordering, but resolved against the child's
        // schema (internal, still carrying provenance columns) since this
        // is a requirement on the input, not a promise about our output.
        let input_schema = input.schema();
        let input_sort_exprs = ["series_id", "ts"]
            .into_iter()
            .map(|name| Ok(PhysicalSortExpr::new(col(name, &input_schema)?, asc)))
            .collect::<DFResult<Vec<_>>>()?;
        let input_ordering = LexOrdering::new(input_sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty dedup input ordering".into()))?
            .into();

        Ok(RsegDedupExec {
            input,
            schema,
            max_samples,
            properties,
            input_ordering,
        })
    }
}

impl DisplayAs for RsegDedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RsegDedupExec: max_samples={}", self.max_samples)
    }
}

impl ExecutionPlan for RsegDedupExec {
    fn name(&self) -> &str {
        "RsegDedupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // Single-partition dedup state (`pending` in `DedupStream`) is only
    // correct if every row for a given (series_id, ts) actually reaches
    // this one partition, in order. Without these two overrides the
    // defaults (`UnspecifiedDistribution`, no required ordering) tell the
    // optimizer this operator has no requirements, so `EnforceDistribution`
    // drops `SortPreservingMergeExec` outright instead of enforcing it.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(self.input_ordering.clone())]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("RsegDedupExec needs one child".into()))?;
        Ok(Arc::new(RsegDedupExec::new(input, self.max_samples)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RsegDedupExec is single-partition, got partition {partition}"
            )));
        }
        let input = self.input.execute(0, context)?;
        Ok(Box::pin(DedupStream {
            input,
            schema: Arc::clone(&self.schema),
            max_samples: self.max_samples,
            pending: None,
            out: Vec::new(),
            out_rows: 0,
            yielded: 0,
            input_done: false,
        }))
    }
}

/// One in-flight group's winner: the group key plus a one-row slice of the
/// internal-schema batch it came from (kept alive by refcount, no copy).
struct Pending {
    series: [u8; 16],
    ts: i64,
    key: DedupKey,
    row: RecordBatch,
}

/// Flush the accumulated winner rows into one output batch once this many
/// have piled up.
const FLUSH_ROWS: usize = 1024;

struct DedupStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    max_samples: usize,
    pending: Option<Pending>,
    /// Accumulated one-row public-schema slices awaiting a flush.
    out: Vec<RecordBatch>,
    out_rows: usize,
    yielded: usize,
    input_done: bool,
}

impl DedupStream {
    /// Emit `pending` as a winner: project to the public schema, count it,
    /// and enforce the sample budget.
    fn finalize(&mut self, pending: Pending) -> DFResult<()> {
        let public = pending
            .row
            .project(&(0..PUBLIC_COLUMNS).collect::<Vec<_>>())
            .map_err(DataFusionError::from)?;
        self.out.push(public);
        self.out_rows += 1;
        self.yielded += 1;
        if self.yielded > self.max_samples {
            return Err(SqlError::TooManySamples {
                count: self.yielded,
                max: self.max_samples,
            }
            .into());
        }
        Ok(())
    }

    fn flush(&mut self) -> DFResult<RecordBatch> {
        let batch = concat_batches(&self.schema, self.out.iter()).map_err(DataFusionError::from)?;
        self.out.clear();
        self.out_rows = 0;
        Ok(batch)
    }

    /// Fold one input batch into the running dedup state. Groups that end
    /// inside this batch are finalized; the last (possibly-continuing) group
    /// is left as `pending`.
    fn process_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let cols = InternalCols::new(batch)?;
        for i in 0..batch.num_rows() {
            let series = cols.series_id(i)?;
            let ts = cols.ts(i);
            let key: DedupKey = (
                cols.created(i),
                cols.epoch(i),
                cols.seq(i),
                cols.in_page(i),
                cols.value(i).to_bits(),
            );
            match &mut self.pending {
                Some(p) if p.series == series && p.ts == ts => {
                    if key > p.key {
                        p.key = key;
                        p.row = batch.slice(i, 1);
                    }
                }
                _ => {
                    if let Some(prev) = self.pending.take() {
                        self.finalize(prev)?;
                    }
                    self.pending = Some(Pending {
                        series,
                        ts,
                        key,
                        row: batch.slice(i, 1),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Stream for DedupStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.out_rows >= FLUSH_ROWS {
                return Poll::Ready(Some(this.flush()));
            }
            if this.input_done {
                if let Some(prev) = this.pending.take()
                    && let Err(e) = this.finalize(prev)
                {
                    return Poll::Ready(Some(Err(e)));
                }
                if this.out_rows > 0 {
                    return Poll::Ready(Some(this.flush()));
                }
                return Poll::Ready(None);
            }
            match this.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(e) = this.process_batch(&batch) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => this.input_done = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for DedupStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Downcast handles for the internal-schema columns, resolved once per
/// batch.
struct InternalCols<'a> {
    ts: &'a TimestampNanosecondArray,
    value: &'a Float64Array,
    series_id: &'a FixedSizeBinaryArray,
    created: &'a Int64Array,
    epoch: &'a UInt64Array,
    seq: &'a UInt64Array,
    in_page: &'a UInt32Array,
}

impl<'a> InternalCols<'a> {
    fn new(batch: &'a RecordBatch) -> DFResult<Self> {
        use crate::schema::{
            COL_CREATED_UNIX_NS, COL_IN_PAGE_INDEX, COL_SERIES_ID, COL_TS, COL_VALUE,
            COL_WRITER_EPOCH, COL_WRITER_SEQ,
        };
        fn cast<'b, T: 'static>(batch: &'b RecordBatch, idx: usize, what: &str) -> DFResult<&'b T> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| {
                    DataFusionError::from(SqlError::Internal(format!(
                        "dedup: column {what} has unexpected type"
                    )))
                })
        }
        Ok(InternalCols {
            ts: cast(batch, COL_TS, "ts")?,
            value: cast(batch, COL_VALUE, "value")?,
            series_id: cast(batch, COL_SERIES_ID, "series_id")?,
            created: cast(batch, COL_CREATED_UNIX_NS, "created_unix_ns")?,
            epoch: cast(batch, COL_WRITER_EPOCH, "writer_epoch")?,
            seq: cast(batch, COL_WRITER_SEQ, "writer_seq")?,
            in_page: cast(batch, COL_IN_PAGE_INDEX, "in_page_index")?,
        })
    }

    fn ts(&self, i: usize) -> i64 {
        self.ts.value(i)
    }
    fn value(&self, i: usize) -> f64 {
        self.value.value(i)
    }
    fn series_id(&self, i: usize) -> DFResult<[u8; 16]> {
        let bytes = self.series_id.value(i);
        <[u8; 16]>::try_from(bytes).map_err(|_| {
            DataFusionError::from(SqlError::Internal("series_id is not 16 bytes".into()))
        })
    }
    fn created(&self, i: usize) -> i64 {
        self.created.value(i)
    }
    fn epoch(&self, i: usize) -> u64 {
        self.epoch.value(i)
    }
    fn seq(&self, i: usize) -> u64 {
        self.seq.value(i)
    }
    fn in_page(&self, i: usize) -> u32 {
        self.in_page.value(i)
    }
}
