//! `AuditScanExec`: the leaf of the `audit` pipeline, the audit-signal sibling of
//! [`crate::logs_scan::LogsScanExec`] (ADR-0040, issue #383).
//!
//! An audit record rides RLOG v1 verbatim, so this scan reads through the exact
//! same [`LogSegmentFetcher`] the `logs` and `alerts` tables use. It pushes only
//! a `ts_ns` range (the `audit` table promotes no field into a typed column and
//! has no equality fast path), and emits the generic RLOG record shape: event
//! time, severity text, body, and the whole merged attribute map.
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions, fetches each
//! partition's segments, and emits its records sorted by `ts_ns` ascending. The
//! provider reports `Inexact`, so DataFusion re-applies the originals above the
//! scan and any attribute predicate (`attrs['kind'] = 'legal_hold'`, ...) is a
//! residual over the emitted `attrs` column.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, MapBuilder, StringArray, StringBuilder, TimestampNanosecondArray,
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
use ravel_logseg::LogRecord;
use ravel_query::{LogQuery, LogSegmentFetcher};

use crate::audit_schema::audit_schema;
use crate::error::SqlError;
use crate::rlog_attrs::{attr_value_to_string, merged_attrs};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Audit segment scan producing per-partition ts-ascending batches over the
/// public `audit` schema.
pub struct AuditScanExec {
    fetcher: LogSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Inclusive ts bounds for the fetch's [`LogQuery`].
    ts_min: i64,
    ts_max: i64,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl AuditScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds.
    pub fn new(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = audit_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(AuditScanExec {
            fetcher,
            partitions,
            ts_min,
            ts_max,
            schema,
            properties,
        })
    }

    fn compute_properties(schema: &SchemaRef, n: usize) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let sort_expr = PhysicalSortExpr::new(col("ts_ns", schema)?, asc);
        let ordering = LexOrdering::new(vec![sort_expr])
            .ok_or_else(|| DataFusionError::Internal("empty audit scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for AuditScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AuditScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for AuditScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AuditScanExec: partitions={}", self.partitions.len())
    }
}

impl ExecutionPlan for AuditScanExec {
    fn name(&self) -> &str {
        "AuditScanExec"
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

        let reservation = MemoryConsumer::new(format!("AuditScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(fetcher, segs, self.ts_min, self.ts_max));
        Ok(Box::pin(AuditScanStream {
            schema,
            reservation,
            state: AuditScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its records sorted by
/// `ts_ns` ascending. Only the ts range prunes the fetch; the residual
/// re-applies everything above.
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
) -> DFResult<Vec<LogRecord>> {
    let query = LogQuery::new(ts_min, ts_max);

    let mut out: Vec<LogRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher.fetch(seg, &query).await.map_err(SqlError::from)? else {
            continue;
        };
        out.extend(output.records);
    }
    // Stable sort so records with equal ts keep the reader's emission order.
    out.sort_by_key(|r| r.ts_ns);
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<LogRecord>>> + Send>>;

enum AuditScanState {
    Fetching(PrepareFuture),
    Emitting { records: Vec<LogRecord>, pos: usize },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits ts-ascending
/// bounded batches, growing the memory reservation by each batch's measured size
/// so a byte-budget overrun surfaces as the pool's `ResourcesExhausted`.
struct AuditScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    state: AuditScanState,
}

impl Stream for AuditScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                AuditScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(records)) => {
                        this.state = AuditScanState::Emitting { records, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = AuditScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                AuditScanState::Emitting { records, pos } => {
                    if *pos >= records.len() {
                        this.state = AuditScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(records.len());
                    let batch = match build_batch(&records[*pos..end], Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = AuditScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    if let Err(e) = this.reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = AuditScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                AuditScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for AuditScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Decode a slice of records into one `audit`-schema [`RecordBatch`]: the generic
/// RLOG record shape (ts, severity text, body) plus the full merged attribute
/// map ([`merged_attrs`]).
fn build_batch(records: &[LogRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(records.iter().map(|r| r.ts_ns).collect::<Vec<_>>());
    let severity_text = StringArray::from(
        records
            .iter()
            .map(|r| r.severity_text.as_str())
            .collect::<Vec<_>>(),
    );
    let body = StringArray::from(records.iter().map(|r| r.body.as_str()).collect::<Vec<_>>());

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

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(severity_text),
        Arc::new(body),
        Arc::new(attrs.finish()),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
