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
//! change the result. The operator holds one candidate of state
//! per in-flight group, strips the provenance columns, emits the public
//! schema, and counts yielded (post-dedup) rows against `max_samples`.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int64Array,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Int32Type, SchemaRef};
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
use crate::schema::{COL_LABELS, PUBLIC_COLUMNS, public_schema};

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
    /// executing only one scan partition. Reproduced via `SessionContext`/`DataFrame`,
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
            out_dict_values: None,
            out_multi_dict: false,
            labels_memo: None,
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
    /// The labels dictionary `values` pointer of the first row accumulated
    /// into `out` since the last flush, or `None` before any row has been
    /// pushed. Compared against each new row's pointer to maintain
    /// `out_multi_dict` without re-walking `out` at flush time.
    out_dict_values: Option<ArrayRef>,
    /// Whether the rows accumulated in `out` since the last flush span more
    /// than one distinct labels-dictionary pointer. See [`Self::flush`].
    out_multi_dict: bool,
    /// Last one-row labels compaction `flush` performed, for reuse when the
    /// next row's source dictionary and key are unchanged. See
    /// [`LabelsMemo`].
    labels_memo: Option<LabelsMemo>,
}

/// The last (source dictionary, key) pair `flush`'s per-row labels
/// compaction was run on, plus the one-entry array it produced.
///
/// Two consecutive winner rows folded from the same upstream batch
/// (`process_batch`'s loop) share both their labels dictionary's `values`
/// array (by pointer -- see [`crate::labels::compact_labels`] on why slicing
/// never copies it) and, when they also share a dictionary key, resolve to
/// the exact same one-row label set. The
/// one-entry array `compact_labels` would rebuild for the second row is then
/// bit-identical to the one already produced for the first, so it is reused
/// instead of rebuilt. `values` must be compared alongside `key`: a new
/// upstream batch renumbers its dictionary from key 0, so the same key can
/// mean an entirely different label set once the source dictionary changes,
/// and comparing the key alone would silently relabel one series' rows with
/// another's.
struct LabelsMemo {
    values: ArrayRef,
    key: i32,
    compacted: ArrayRef,
}

impl DedupStream {
    /// Emit `pending` as a winner: project to the public schema, track its
    /// labels dictionary's pointer against the rest of `self.out` (see
    /// [`Self::track_out_dict`]), count it, and enforce the sample budget.
    ///
    /// Unlike an earlier version of this operator, this does not compact the
    /// row's labels dictionary itself: whether that is worth doing at all
    /// depends on what the *other* rows accumulated in this flush window
    /// look like, which only [`Self::flush`] can see. See its docs.
    fn finalize(&mut self, pending: Pending) -> DFResult<()> {
        let public = pending
            .row
            .project(&(0..PUBLIC_COLUMNS).collect::<Vec<_>>())
            .map_err(DataFusionError::from)?;
        self.track_out_dict(public.column(COL_LABELS));
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

    /// Update `out_multi_dict` with one more accumulated row's labels
    /// dictionary. Cheap: a pointer comparison against the first dictionary
    /// seen since the last flush, never a walk of `self.out`. Once
    /// `out_multi_dict` is set it stays set until the next flush clears it,
    /// so later rows are skipped once the answer is already "more than one".
    ///
    /// A labels column that is not a one-row `Dictionary(Int32, _)` (the
    /// `compact_row_labels` fallback shape, e.g. a null row) is treated as
    /// automatically distinct, forcing the real-compaction path in `flush`
    /// rather than risking a shared-dictionary assumption that does not
    /// hold.
    fn track_out_dict(&mut self, labels: &ArrayRef) {
        if self.out_multi_dict {
            return;
        }
        let this_values = labels
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .filter(|dict| dict.len() == 1 && !dict.is_null(0))
            .map(|dict| Arc::clone(dict.values()));
        match (&self.out_dict_values, this_values) {
            (None, Some(values)) => self.out_dict_values = Some(values),
            (Some(seen), Some(values)) => {
                if !Arc::ptr_eq(seen, &values) {
                    self.out_multi_dict = true;
                }
            }
            (_, None) => self.out_multi_dict = true,
        }
    }

    /// Compact a one-row labels slice's dictionary down to the single entry
    /// it references, reusing the previous call's result when this row's
    /// source dictionary and key match the last one seen. See
    /// [`LabelsMemo`]. Falls back to a plain [`crate::labels::compact_labels`]
    /// call for any shape the memo does not cover (not a one-row
    /// `Dictionary(Int32, _)`, or a null row).
    fn compact_row_labels(&mut self, labels: &ArrayRef) -> DFResult<ArrayRef> {
        if let Some(dict) = labels.as_any().downcast_ref::<DictionaryArray<Int32Type>>()
            && dict.len() == 1
            && !dict.is_null(0)
        {
            let key = dict.keys().value(0);
            let values = dict.values();
            if let Some(memo) = &self.labels_memo
                && memo.key == key
                && Arc::ptr_eq(&memo.values, values)
            {
                return Ok(Arc::clone(&memo.compacted));
            }
            let compacted = crate::labels::compact_labels(labels)?;
            self.labels_memo = Some(LabelsMemo {
                values: Arc::clone(values),
                key,
                compacted: Arc::clone(&compacted),
            });
            return Ok(compacted);
        }
        Ok(crate::labels::compact_labels(labels)?)
    }

    /// Collapse the accumulated winner rows in `self.out` into one output
    /// batch.
    ///
    /// Compacting each row's labels dictionary down to its one referenced
    /// entry before concatenating is worth doing only when the accumulated
    /// rows do not already share one dictionary pointer; see
    /// [`crate::labels::compact_labels`] for why `concat_batches` cares about
    /// that. `self.out_multi_dict` (maintained by `finalize` via
    /// `track_out_dict`, cheaply, without walking `self.out`) answers that
    /// exactly: if every accumulated row shares one dictionary pointer,
    /// `concat_batches` was always going to share it for free, and compacting
    /// each row down to one entry first (a `MapBuilder` rebuild per row) buys
    /// nothing -- it is pure overhead paid to defend against a copy that was
    /// never going to happen. Only when `out_multi_dict` is set does this
    /// compact each row first, exactly as an earlier version of this
    /// operator did unconditionally.
    ///
    /// Allocation churn (cumulative bytes allocated over the run, via
    /// `stats_alloc`; not peak resident bytes -- see
    /// `tests/dedup_finalize_allocation*.rs` and
    /// `tests/peak_alloc_instrument.rs`) measured on a 5,000-series/1-segment
    /// corpus, where every accumulated row shares one dictionary pointer so
    /// the unconditional per-row compaction bought nothing: 100,683,176 bytes
    /// allocated compacting unconditionally, 29,948,424 bytes allocated
    /// skipping it here (the same figure as deleting the compaction outright;
    /// measured with a scratch corpus, not committed as a permanent test).
    ///
    /// Allocation churn measured on the 10,000-series/500-segment corpus in
    /// `tests/dedup_finalize_allocation.rs`: 84,838,905 bytes allocated, well
    /// under the 201,280,697 an earlier round of this operator allocated
    /// compacting unconditionally. `out_multi_dict` being set less often than
    /// the per-segment split alone would suggest is why: the upstream
    /// `SortPreservingMergeExec` already rebuilds one shared dictionary per
    /// *its own* output batch when interleaving several small segments'
    /// worth of rows into it (same pointer-equality-or-copy rule
    /// `compact_labels` describes for `concat_batches` applies to arrow's
    /// merge machinery too), so most flush windows here draw from a single
    /// already-unified dictionary and only the rare window spanning a merge
    /// batch boundary needs real per-row compaction.
    fn flush(&mut self) -> DFResult<RecordBatch> {
        let batch = if self.out_multi_dict {
            let rows = std::mem::take(&mut self.out);
            let compacted = rows
                .into_iter()
                .map(|row| {
                    let mut columns = row.columns().to_vec();
                    columns[COL_LABELS] = self.compact_row_labels(&columns[COL_LABELS])?;
                    RecordBatch::try_new(Arc::clone(&self.schema), columns)
                        .map_err(DataFusionError::from)
                })
                .collect::<DFResult<Vec<_>>>()?;
            concat_batches(&self.schema, compacted.iter()).map_err(DataFusionError::from)?
        } else {
            concat_batches(&self.schema, self.out.iter()).map_err(DataFusionError::from)?
        };
        self.out.clear();
        self.out_rows = 0;
        self.out_dict_values = None;
        self.out_multi_dict = false;
        // Whether or not the loop above ran, `concat_batches` can still leave
        // the flushed batch's dictionary holding more entries than this batch
        // actually references (a shared source dictionary carries every
        // series in its upstream scan batch, not just the ones that ended up
        // here). Compact once more so the flushed batch holds exactly one
        // entry per distinct series it references; the schema and every
        // decoded label set are unchanged.
        //
        // This holds per flush only: each flush is compacted independently of
        // every other, so two flush batches' dictionaries are never merged
        // together by anything in this crate.
        let mut columns = batch.columns().to_vec();
        columns[COL_LABELS] = crate::labels::compact_labels(&columns[COL_LABELS])?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(DataFusionError::from)?;
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{MapArray, StringArray};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use ravel_types::{Label, LabelSet};

    use super::*;
    use crate::labels::build_labels_dict;
    use crate::schema::{internal_schema, public_schema};

    fn empty_input() -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            internal_schema(),
            futures::stream::empty(),
        ))
    }

    fn new_dedup_stream() -> DedupStream {
        DedupStream {
            input: empty_input(),
            schema: public_schema(),
            max_samples: usize::MAX,
            pending: None,
            out: Vec::new(),
            out_rows: 0,
            yielded: 0,
            input_done: false,
            out_dict_values: None,
            out_multi_dict: false,
            labels_memo: None,
        }
    }

    fn label_set(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    /// A one-row internal-schema batch carrying a freshly-built labels
    /// dictionary with exactly one entry, so its single row's dictionary key
    /// is always 0 -- but the dictionary's `values` array is a distinct
    /// allocation each call, standing in for two different upstream scan
    /// batches.
    fn one_row_batch(series_id: [u8; 16], ts: i64, value: f64, labels: &LabelSet) -> RecordBatch {
        let labels_col =
            build_labels_dict(std::slice::from_ref(labels), &[0]).expect("labels dict");
        RecordBatch::try_new(
            internal_schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![ts])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(series_id))
                        .expect("series_id array"),
                ),
                labels_col,
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt32Array::from(vec![0u32])),
            ],
        )
        .expect("build internal batch")
    }

    /// Decode a one-row `Dictionary(Int32, Map(Utf8, Utf8))` labels column
    /// into its `(name, value)` pairs.
    fn decode_row0_labels(labels: &ArrayRef) -> Vec<(String, String)> {
        let dict = labels
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary labels");
        let maps = dict
            .values()
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map values");
        let keys = maps
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys utf8");
        let values = maps
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map values utf8");
        let entry = dict.keys().value(0) as usize;
        let offsets = maps.value_offsets();
        let start = offsets[entry] as usize;
        let end = offsets[entry + 1] as usize;
        (start..end)
            .map(|j| (keys.value(j).to_string(), values.value(j).to_string()))
            .collect()
    }

    /// Regression for `LabelsMemo`: two winner rows finalized back to back can
    /// each be dictionary key 0 in their own freshly-built one-entry
    /// dictionary while carrying entirely different label sets -- exactly
    /// what happens across two separate upstream scan batches, each of which
    /// numbers its own local dictionary from 0. A memo that compared only the
    /// key, not also the source dictionary's `values` array by pointer, would
    /// reuse the first row's compacted array for the second, silently
    /// relabeling the second series' output with the first series' labels.
    ///
    /// `finalize` itself no longer compacts (that decision is deferred to
    /// `flush`, see its docs), so this drives a real `flush` to exercise
    /// `compact_row_labels`/`LabelsMemo`: the two rows' distinct source
    /// dictionaries set `out_multi_dict`, which is what makes `flush` take
    /// the per-row-compaction path at all.
    #[test]
    fn memo_distinguishes_same_key_across_different_source_dictionaries() {
        let mut stream = new_dedup_stream();
        let labels_a = label_set(&[("job", "a")]);
        let labels_b = label_set(&[("job", "b")]);
        let batch_a = one_row_batch([1u8; 16], 1, 1.0, &labels_a);
        let batch_b = one_row_batch([2u8; 16], 1, 2.0, &labels_b);

        // Folding batch_b's row finalizes batch_a's pending row first (a new
        // group starts), so this exercises two `finalize` calls back to
        // back, each seeing a dictionary key of 0 from a distinct `values`
        // array.
        stream.process_batch(&batch_a).expect("process batch a");
        stream.process_batch(&batch_b).expect("process batch b");
        let pending_b = stream.pending.take().expect("pending b row");
        stream.finalize(pending_b).expect("finalize b");

        assert_eq!(stream.out.len(), 2, "both rows must have been finalized");
        assert!(
            stream.out_multi_dict,
            "the two rows come from distinct source dictionaries"
        );
        let flushed = stream.flush().expect("flush");
        assert_eq!(flushed.num_rows(), 2, "both rows must survive the flush");
        let got_a = decode_row0_labels(&flushed.slice(0, 1).column(COL_LABELS).clone());
        let got_b = decode_row0_labels(&flushed.slice(1, 1).column(COL_LABELS).clone());
        assert_eq!(
            got_a,
            vec![("job".to_string(), "a".to_string())],
            "first finalized row must keep its own label set"
        );
        assert_eq!(
            got_b,
            vec![("job".to_string(), "b".to_string())],
            "second finalized row must not inherit the first row's labels \
             via a key-only memo match"
        );
    }
}
