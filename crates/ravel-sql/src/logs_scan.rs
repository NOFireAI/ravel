//! `LogsScanExec`: the leaf of the `logs` pipeline, the log-signal sibling of
//! [`crate::scan::RsegScanExec`] (ADR-0033).
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through [`LogSegmentFetcher::fetch`] (one
//! [`LogQuery`] per segment: the extracted ts range, stream-attribute
//! equalities, and content predicates), decodes the returned
//! [`ravel_logseg::LogRecord`]s into Arrow arrays matching
//! [`crate::logs_schema::logs_schema`], and emits them sorted by `ts`
//! ascending. That per-partition ordering is declared through
//! `PlanProperties` so a later merge stage (#240) can honor it with a
//! `SortPreservingMergeExec`.
//!
//! `RlogReader::scan` (inside `fetch`) already emits a segment's records
//! grouped by `(stream_ref, ts)`, not globally by `ts`, and a partition draws
//! from several segments, so this stage sorts each partition's collected
//! records by `ts` before emitting. Declaring `ts` ascending is therefore
//! truthful for the stream this stage produces; nothing declares a global
//! cross-partition order (that is a merge's job, not the leaf's).
//!
//! # Correctness: the merged `attrs` column plus DataFusion's residual
//!
//! This scan pushes only two predicate kinds into [`LogSegmentFetcher::fetch`]:
//! the ts range (a segment-level and reader-level prune, exact) and content
//! predicates (`has_word`, whose SQL semantics equal the reader's exact filter,
//! [`crate::logs_pushdown`]). It does **not** push stream-attribute equalities,
//! and it performs no per-record re-verification: it emits every record the
//! fetcher returns. Attribute filtering is entirely DataFusion's job.
//!
//! The reason is the ADR-0033 merge. `attrs` is the resource + scope + record
//! attributes merged into one map with the record winning on a key collision, so
//! a record's `attrs['k']` value can differ from its stream-identifying
//! resource/scope attributes. Any prune keyed on stream-level attributes — the
//! fetcher's STREAM_DIR match resolved into a `Predicate::StreamIn`, or a
//! scan-level re-check of `stream_attrs` — is therefore **not** a sound
//! over-approximation of `attrs['k'] = 'v'`: it drops a record whose match lives
//! only in its per-record dynamic attributes (resource `service.name = worker`,
//! record attribute `service.name = api`, query `= 'api'`), which the merged map
//! resolves to `api` and must keep. Pushing such a predicate as a fetch prune is
//! a data-loss bug; so this scan does not, and stream-attribute equalities are
//! not extracted into the fetch at all ([`crate::logs_pushdown`]).
//!
//! Correctness comes solely from the merged `attrs` column plus the residual.
//! Pushdown is always `Inexact`, so DataFusion re-applies the *original*
//! predicate against the emitted batch. [`build_batch`] populates the `attrs`
//! column from the fully merged view (ADR-0033 amendment, issue #239), so the
//! residual evaluates `attrs['k'] = 'v'` against exactly the data a row's SQL
//! semantics demand: a resource-only match survives (the residual sees it in the
//! merged column), and a record-attribute override survives (the merge resolves
//! the key to the record's value, which wins). The merged column and the
//! residual are the whole correctness story.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, MapBuilder, StringArray, StringBuilder,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
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
use ravel_logseg::{LogRecord, Predicate};
use ravel_query::{LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::error::SqlError;
use crate::logs_schema::{SPAN_ID_WIDTH, TRACE_ID_WIDTH, logs_schema};
use crate::rlog_attrs::{attr_value_to_string, merged_attrs};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Log segment scan producing per-partition ts-ascending batches over the
/// public `logs` schema.
pub struct LogsScanExec {
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Inclusive ts bounds for the fetch's [`LogQuery`].
    ts_min: i64,
    ts_max: i64,
    /// Content predicates (`has_word`) handed to `RlogReader::scan`, applied
    /// exactly there.
    content: Arc<Vec<Predicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), threaded into every
    /// per-partition fetch so log fetches are recorded like every other
    /// funnel.
    accounting: QueryAccounting,
}

impl LogsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds and content predicates. Stream-attribute equalities are
    /// deliberately not accepted: they are not pushed into the fetch, because a
    /// stream-level prune is unsound against the merged `attrs` column (see the
    /// module doc). DataFusion's residual filters attributes.
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        content: Arc<Vec<Predicate>>,
        accounting: QueryAccounting,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = logs_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(LogsScanExec {
            tenant_hash,
            fetcher,
            partitions,
            ts_min,
            ts_max,
            content,
            schema,
            properties,
            accounting,
        })
    }

    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_expr = PhysicalSortExpr::new(col("ts", schema)?, asc);
        let ordering = LexOrdering::new(vec![sort_expr])
            .ok_or_else(|| DataFusionError::Internal("empty logs scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for LogsScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for LogsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsScanExec: partitions={}, content={}",
            self.partitions.len(),
            self.content.len()
        )
    }
}

impl ExecutionPlan for LogsScanExec {
    fn name(&self) -> &str {
        "LogsScanExec"
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
        let tenant_hash = self.tenant_hash;
        let content = Arc::clone(&self.content);
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("LogsScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            tenant_hash,
            segs,
            self.ts_min,
            self.ts_max,
            content,
            self.accounting.clone(),
        ));
        Ok(Box::pin(LogScanStream {
            schema,
            reservation,
            state: LogScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its records sorted by `ts`
/// ascending. Only the ts range and content predicates prune the fetch;
/// attribute filtering is DataFusion's residual over [`build_batch`]'s merged
/// `attrs` column (see the module doc). Every fetched record is emitted.
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    tenant_hash: TenantHash,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
    content: Arc<Vec<Predicate>>,
    accounting: QueryAccounting,
) -> DFResult<Vec<LogRecord>> {
    let mut query = LogQuery::new(ts_min, ts_max);
    for c in content.iter() {
        query = query.with_content(c.clone());
    }

    let accounting = QueryAccounting::new();
    let mut out: Vec<LogRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher
            .fetch_accounted_with_tenant(seg, tenant_hash, &query, &accounting)
            .await
            .map_err(SqlError::from)?
        else {
            continue;
        };
        // Emit every fetched record: stream-attribute equalities are not pushed
        // (a stream-level prune is unsound against the merged `attrs` column),
        // so nothing here narrows below what DataFusion's residual keeps.
        out.extend(output.records);
    }
    // Stable sort so records with equal ts keep the reader's emission order.
    out.sort_by_key(|r| r.ts_ns);
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<LogRecord>>> + Send>>;

enum LogScanState {
    Fetching(PrepareFuture),
    Emitting { records: Vec<LogRecord>, pos: usize },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits ts-ascending
/// bounded batches, growing the memory reservation by each batch's measured
/// size so a byte-budget overrun surfaces as the pool's `ResourcesExhausted`.
/// The reservation lives on the stream (not the state) so it is the same one
/// for the partition's lifetime and frees exactly once on drop.
struct LogScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    state: LogScanState,
}

impl Stream for LogScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                LogScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(records)) => {
                        this.state = LogScanState::Emitting { records, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = LogScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Emitting { records, pos } => {
                    if *pos >= records.len() {
                        this.state = LogScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(records.len());
                    let batch = match build_batch(&records[*pos..end], Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = LogScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    if let Err(e) = this.reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = LogScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                LogScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for LogScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Decode a slice of records into one `logs`-schema [`RecordBatch`].
fn build_batch(records: &[LogRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(records.iter().map(|r| r.ts_ns).collect::<Vec<_>>());
    let observed_ts = TimestampNanosecondArray::from(
        records.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
    );
    let severity_num = UInt8Array::from(records.iter().map(|r| r.severity_num).collect::<Vec<_>>());
    let severity_text = StringArray::from(
        records
            .iter()
            .map(|r| r.severity_text.as_str())
            .collect::<Vec<_>>(),
    );
    let body = StringArray::from(records.iter().map(|r| r.body.as_str()).collect::<Vec<_>>());

    let mut trace = FixedSizeBinaryBuilder::with_capacity(records.len(), TRACE_ID_WIDTH);
    for r in records {
        match &r.trace_id {
            Some(id) => trace
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?,
            None => trace.append_null(),
        }
    }
    let trace = trace.finish();

    let mut span = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
    for r in records {
        match &r.span_id {
            Some(id) => span
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
            None => span.append_null(),
        }
    }
    let span = span.finish();

    let flags = UInt32Array::from(records.iter().map(|r| r.flags).collect::<Vec<_>>());

    // `attrs` map: each record's stream-identity (resource + scope) attributes
    // merged with its dynamic per-record attributes, values rendered to text.
    // DataFusion's mandatory `Inexact` residual re-applies `attrs['k'] = 'v'`
    // against this column, and that residual is the sole exactness mechanism, so
    // the column must carry the fully merged view. Populating it from `r.attrs`
    // alone silently dropped every record whose matched attribute was a genuine
    // resource attribute (ADR-0033 amendment, issue #239). See `merged_attrs`.
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for r in records {
        for (k, v) in merged_attrs(r)? {
            attrs.keys().append_value(&k);
            attrs.values().append_value(attr_value_to_string(&v));
        }
        attrs
            .append(true)
            .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
    }
    let attrs = attrs.finish();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(observed_ts),
        Arc::new(severity_num),
        Arc::new(severity_text),
        Arc::new(body),
        Arc::new(trace),
        Arc::new(span),
        Arc::new(flags),
        Arc::new(attrs),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
