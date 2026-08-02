//! `SpansScanExec`: the leaf of the `spans` pipeline, the span-signal sibling
//! of [`crate::logs_scan::LogsScanExec`] (ADR-0041, phase 5).
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through [`SpanSegmentFetcher::fetch`] with one shared
//! [`ravel_rspan::SpanQuery`] (the extracted ts window, plus the trace_id
//! fast-path key when a `trace_id =` equality was pushed), decodes the returned
//! [`ravel_rspan::SpanRecord`]s into Arrow arrays matching
//! [`crate::spans_schema::spans_schema`], and emits them in `(trace_id,
//! start_ts)` order -- the object's native sort order (ADR-0041: records sort
//! by `(trace_id, start_ts)`).
//!
//! # Why `(trace_id, start_ts)`, not `start_ts` alone
//!
//! [`RspanReader::scan`] emits one object's surviving rows already in
//! `(trace_id, start_ts)` order (candidate blocks are visited in ascending
//! order, and rows within a block are stored sorted). A partition draws from
//! several objects, so this stage does a single stable sort by `(trace_id,
//! start_ts)` to interleave them while preserving that native key order.
//! Re-sorting by `start_ts` alone would discard the trace_id-primary order the
//! format is built around and force a full re-materialization the format never
//! needs; the declared ordering is therefore `(trace_id asc, start_ts asc)`, so
//! a later merge stage can honor it with a `SortPreservingMergeExec`.
//!
//! # Correctness
//!
//! `SpansTableProvider::supports_filters_pushdown` is always `Inexact`, so
//! DataFusion re-applies every original filter above this scan. This stage
//! pushes only the ts window and (optionally) the trace_id equality into the
//! reader, both of which the reader evaluates *exactly* per row -- so the
//! pushed prune removes only rows the residual would also remove, and no row
//! the query needs is ever dropped. RSPAN's `attrs` is already the merged
//! resource+scope+span map on each record, so unlike `logs` there is no
//! stream-identity blob to decode or re-verify here: [`build_batch`] copies
//! `record.attrs` straight into the `Map(Utf8, Utf8)` column.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Int64Array, MapBuilder, StringArray, StringBuilder,
    TimestampNanosecondArray, UInt8Array,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_rspan::{SpanQuery, SpanRecord};

use crate::error::SqlError;
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_schema::{SPAN_ID_WIDTH, TRACE_ID_WIDTH, spans_schema};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Span segment scan producing per-partition `(trace_id, start_ts)`-ordered
/// batches over the public `spans` schema.
pub struct SpansScanExec {
    fetcher: SpanSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// The shared query handed to every segment fetch: the ts window and, on
    /// the fast path, the trace_id key. One query covers the whole scan.
    query: SpanQuery,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SpansScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, driven by `query`.
    pub fn new(
        fetcher: SpanSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        query: SpanQuery,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = spans_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(SpansScanExec {
            fetcher,
            partitions,
            query,
            schema,
            properties,
        })
    }

    /// The [`SpanQuery`] this scan issues. Exposed so tests can prove a
    /// `trace_id =` filter is compiled into a [`SpanQuery::trace`] fast-path
    /// lookup rather than a bare [`SpanQuery::ts_range`] scan.
    pub fn query(&self) -> SpanQuery {
        self.query
    }

    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Declared order is (trace_id asc, start_ts asc): the object's native
        // sort key (ADR-0041), preserved by this stage's stable sort.
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new(col("trace_id", schema)?, asc),
            PhysicalSortExpr::new(col("start_ts", schema)?, asc),
        ])
        .ok_or_else(|| DataFusionError::Internal("empty spans scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for SpansScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpansScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for SpansScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SpansScanExec: partitions={}, trace_id={}",
            self.partitions.len(),
            self.query.trace_id.is_some(),
        )
    }
}

impl ExecutionPlan for SpansScanExec {
    fn name(&self) -> &str {
        "SpansScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let segs = self.partitions.get(partition).cloned().unwrap_or_default();
        let fetcher = self.fetcher.clone();
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("SpansScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(fetcher, segs, self.query));
        Ok(Box::pin(SpanScanStream {
            schema,
            reservation,
            state: SpanScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its records in `(trace_id,
/// start_ts)` order. Every fetched record already satisfies `query` exactly
/// (the reader re-checks the ts overlap and trace_id per row).
async fn prepare_partition(
    fetcher: SpanSegmentFetcher,
    segs: Vec<SegmentRef>,
    query: SpanQuery,
) -> DFResult<Vec<SpanRecord>> {
    let mut out: Vec<SpanRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher.fetch(seg, &query).await.map_err(SqlError::from)? else {
            continue;
        };
        out.extend(output.records);
    }
    // Stable sort by the native (trace_id, start_ts) key so records tying on
    // both keep the reader's per-object emission order.
    out.sort_by(|a, b| {
        a.trace_id
            .cmp(&b.trace_id)
            .then_with(|| a.start_ts_ns.cmp(&b.start_ts_ns))
    });
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<SpanRecord>>> + Send>>;

enum SpanScanState {
    Fetching(PrepareFuture),
    Emitting {
        records: Vec<SpanRecord>,
        pos: usize,
    },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits
/// `(trace_id, start_ts)`-ordered bounded batches, growing the memory
/// reservation by each batch's measured size so a byte-budget overrun surfaces
/// as the pool's `ResourcesExhausted`. The reservation lives on the stream so
/// it frees exactly once on drop.
struct SpanScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    state: SpanScanState,
}

impl Stream for SpanScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                SpanScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(records)) => {
                        this.state = SpanScanState::Emitting { records, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SpanScanState::Emitting { records, pos } => {
                    if *pos >= records.len() {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(records.len());
                    let batch = match build_batch(&records[*pos..end], Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = SpanScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    if let Err(e) = this.reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                SpanScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for SpanScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Decode a slice of records into one `spans`-schema [`RecordBatch`].
fn build_batch(records: &[SpanRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let mut trace = FixedSizeBinaryBuilder::with_capacity(records.len(), TRACE_ID_WIDTH);
    for r in records {
        trace
            .append_value(r.trace_id)
            .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?;
    }
    let trace = trace.finish();

    let mut span = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
    for r in records {
        span.append_value(r.span_id)
            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?;
    }
    let span = span.finish();

    let mut parent = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
    for r in records {
        match &r.parent_span_id {
            Some(id) => parent
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("parent_span_id array build: {e}")))?,
            None => parent.append_null(),
        }
    }
    let parent = parent.finish();

    let name = StringArray::from(records.iter().map(|r| r.name.as_str()).collect::<Vec<_>>());
    let start_ts =
        TimestampNanosecondArray::from(records.iter().map(|r| r.start_ts_ns).collect::<Vec<_>>());
    let end_ts =
        TimestampNanosecondArray::from(records.iter().map(|r| r.end_ts_ns).collect::<Vec<_>>());
    let status_code = UInt8Array::from(
        records
            .iter()
            .map(|r| r.status_code.to_u8())
            .collect::<Vec<_>>(),
    );
    let status_message = StringArray::from(
        records
            .iter()
            .map(|r| r.status_message.as_deref())
            .collect::<Vec<_>>(),
    );

    // `attrs` map: RSPAN already merged resource+scope+span into `record.attrs`
    // with unique, ascending keys, so it copies straight into the column with
    // no decode or re-verification (unlike `logs`, whose merged view is rebuilt
    // at scan time from a stream-identity blob).
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for r in records {
        for (k, v) in &r.attrs {
            attrs.keys().append_value(k);
            attrs.values().append_value(v);
        }
        attrs
            .append(true)
            .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
    }
    let attrs = attrs.finish();

    // service_name (ADR-0045 decision 5): RSPAN v1 has no dedicated column
    // for it yet, so it is looked up in the same merged attrs map the
    // `attrs` column above copies verbatim; NULL when the span carries no
    // `service.name` attribute.
    let service_name = StringArray::from(
        records
            .iter()
            .map(|r| {
                r.attrs
                    .iter()
                    .find(|(k, _)| k == "service.name")
                    .map(|(_, v)| v.as_str())
            })
            .collect::<Vec<_>>(),
    );
    // duration_ns is computed, never stored (ADR-0045 decision 5, rejected
    // alternative 3). `saturating_sub` rather than a bare `-`: end_ts_ns >=
    // start_ts_ns is a format invariant, not one this column should assume
    // and panic on if corrupt or adversarial data ever violates it.
    let duration_ns = Int64Array::from(
        records
            .iter()
            .map(|r| r.end_ts_ns.saturating_sub(r.start_ts_ns))
            .collect::<Vec<_>>(),
    );

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace),
        Arc::new(span),
        Arc::new(parent),
        Arc::new(name),
        Arc::new(start_ts),
        Arc::new(end_ts),
        Arc::new(status_code),
        Arc::new(status_message),
        Arc::new(attrs),
        Arc::new(service_name),
        Arc::new(duration_ns),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
