//! `AlertsScanExec`: the leaf of the `alerts` pipeline, the alert-signal sibling
//! of [`crate::logs_scan::LogsScanExec`] (ADR-0040, issue #383).
//!
//! An alert record rides RLOG v1 verbatim, so this scan reads through the exact
//! same [`LogSegmentFetcher`] the `logs` table uses; the only differences are
//! the pushed predicates (a `ts_ns` range plus optional exact `alert_id`/
//! `rule_id` [`Predicate::Equals`] fast paths, crate::alerts_pushdown) and the
//! output shape (crate::alerts_schema): the four alert scalar fields promoted to
//! typed columns from the record's merged attributes, plus the whole merged map
//! as one `attrs` column.
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions, fetches each
//! partition's segments, and emits its records sorted by `ts_ns` ascending, the
//! same partition/sort discipline as the `logs` scan.
//!
//! Every fetched record is emitted. The pushed predicates are all exact (ts
//! range and per-record `Equals`), and the provider still reports `Inexact`, so
//! DataFusion re-applies the originals above the scan and any residual predicate
//! (`state = 'firing'`, `attrs['label.env'] = 'prod'`, ...) is evaluated there
//! against the emitted columns.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, Int64Builder, MapBuilder, StringBuilder, TimestampNanosecondArray,
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
use ravel_logseg::{AttrValue, LogRecord, Predicate};
use ravel_query::{LogQuery, LogSegmentFetcher};

use crate::alerts_schema::{ALERT_ID_KEY, GENERATION_KEY, RULE_ID_KEY, STATE_KEY, alerts_schema};
use crate::error::SqlError;
use crate::rlog_attrs::{attr_value_to_string, find_attr, merged_attrs};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Alert segment scan producing per-partition ts-ascending batches over the
/// public `alerts` schema.
pub struct AlertsScanExec {
    fetcher: LogSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Inclusive ts bounds for the fetch's [`LogQuery`].
    ts_min: i64,
    ts_max: i64,
    /// Exact per-record attribute equalities (`alert_id`/`rule_id`) handed to
    /// `RlogReader::scan`, applied exactly there.
    content: Arc<Vec<Predicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl AlertsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds and content predicates.
    pub fn new(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        content: Arc<Vec<Predicate>>,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = alerts_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(AlertsScanExec {
            fetcher,
            partitions,
            ts_min,
            ts_max,
            content,
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
            .ok_or_else(|| DataFusionError::Internal("empty alerts scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for AlertsScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AlertsScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for AlertsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AlertsScanExec: partitions={}, content={}",
            self.partitions.len(),
            self.content.len()
        )
    }
}

impl ExecutionPlan for AlertsScanExec {
    fn name(&self) -> &str {
        "AlertsScanExec"
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
        let content = Arc::clone(&self.content);
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("AlertsScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            segs,
            self.ts_min,
            self.ts_max,
            content,
        ));
        Ok(Box::pin(AlertScanStream {
            schema,
            reservation,
            state: AlertScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its records sorted by
/// `ts_ns` ascending. The ts range and the `alert_id`/`rule_id` equalities prune
/// the fetch exactly; the residual re-applies everything above.
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
    content: Arc<Vec<Predicate>>,
) -> DFResult<Vec<LogRecord>> {
    let mut query = LogQuery::new(ts_min, ts_max);
    for c in content.iter() {
        query = query.with_content(c.clone());
    }

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

enum AlertScanState {
    Fetching(PrepareFuture),
    Emitting { records: Vec<LogRecord>, pos: usize },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits ts-ascending
/// bounded batches, growing the memory reservation by each batch's measured size
/// so a byte-budget overrun surfaces as the pool's `ResourcesExhausted`.
struct AlertScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    state: AlertScanState,
}

impl Stream for AlertScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                AlertScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(records)) => {
                        this.state = AlertScanState::Emitting { records, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = AlertScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                AlertScanState::Emitting { records, pos } => {
                    if *pos >= records.len() {
                        this.state = AlertScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(records.len());
                    let batch = match build_batch(&records[*pos..end], Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = AlertScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    if let Err(e) = this.reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = AlertScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                AlertScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for AlertScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Decode a slice of records into one `alerts`-schema [`RecordBatch`]. The four
/// scalar fields are promoted from each record's merged attributes ([`find_attr`]
/// over [`merged_attrs`]), and the full merged map is rendered into the `attrs`
/// column, so a promoted column and the `attrs` map for the same key never
/// disagree.
fn build_batch(records: &[LogRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(records.iter().map(|r| r.ts_ns).collect::<Vec<_>>());

    let mut alert_id = StringBuilder::new();
    let mut rule_id = StringBuilder::new();
    let mut state = StringBuilder::new();
    let mut generation = Int64Builder::new();
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for r in records {
        let merged = merged_attrs(r)?;

        append_str_attr(&mut alert_id, find_attr(&merged, ALERT_ID_KEY));
        append_str_attr(&mut rule_id, find_attr(&merged, RULE_ID_KEY));
        append_str_attr(&mut state, find_attr(&merged, STATE_KEY));
        append_generation(&mut generation, find_attr(&merged, GENERATION_KEY));

        for (k, v) in &merged {
            attrs.keys().append_value(k);
            attrs.values().append_value(attr_value_to_string(v));
        }
        attrs
            .append(true)
            .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(alert_id.finish()),
        Arc::new(rule_id.finish()),
        Arc::new(state.finish()),
        Arc::new(generation.finish()),
        Arc::new(attrs.finish()),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

/// Promote a string-valued attribute into its typed column: a `Str` value is
/// appended verbatim, anything else (absent, or a non-string type) becomes NULL
/// rather than a fabricated or coerced value.
fn append_str_attr(builder: &mut StringBuilder, value: Option<&AttrValue>) {
    match value {
        Some(AttrValue::Str(s)) => builder.append_value(s),
        _ => builder.append_null(),
    }
}

/// Promote the `generation` attribute into its `Int64` column. It is written as
/// an `I64` attribute (ADR-0040 decision 4); a decimal `Str` is also accepted
/// for resilience. Anything else, or an unparseable string, becomes NULL.
fn append_generation(builder: &mut Int64Builder, value: Option<&AttrValue>) {
    match value {
        Some(AttrValue::I64(v)) => builder.append_value(*v),
        Some(AttrValue::Str(s)) => match s.parse::<i64>() {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        },
        _ => builder.append_null(),
    }
}
