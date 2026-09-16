//! `AlertsScanExec`: the leaf of the `alerts` pipeline, the alert-signal sibling
//! of [`crate::logs_scan::LogsScanExec`] (ADR-0040).
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
//!
//! Each emitted record carries the write identity of the object it came from
//! (`writer_id`/`writer_epoch`/`writer_seq`, ADR-1101 decision 1's row
//! contract), stamped from that object's [`SegmentRef`] as the partition is
//! fetched. Those three columns are what give a SQL fold over the history a
//! total order; see crate::alerts_schema.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, Int64Builder, MapBuilder, StringBuilder, TimestampNanosecondArray, UInt64Builder,
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
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{LogQuery, LogSegmentFetcher};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::alerts_schema::{ALERT_ID_KEY, GENERATION_KEY, RULE_ID_KEY, STATE_KEY, alerts_schema};
use crate::error::SqlError;
use crate::rlog_attrs::{attr_value_to_string, find_attr, merged_attrs, retain_unerased_by};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Alert segment scan producing per-partition ts-ascending batches over the
/// public `alerts` schema.
pub struct AlertsScanExec {
    tenant_hash: TenantHash,
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
    /// Pending selective-erasure predicates from the resolved snapshot
    /// (ADR-0064 decision 2); fed to [`LogQuery::with_erasure`].
    erasure: Arc<Vec<ErasurePredicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), threaded into every
    /// per-partition fetch.
    accounting: QueryAccounting,
}

impl AlertsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds and content predicates.
    // `tenant_hash` widened this past clippy\'s 7-argument
    // threshold; the codebase allows it at the equivalent sites
    // (scan.rs, ravel-query\'s fetcher.rs).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        content: Arc<Vec<Predicate>>,
        erasure: Arc<Vec<ErasurePredicate>>,
        accounting: QueryAccounting,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = alerts_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(AlertsScanExec {
            tenant_hash,
            fetcher,
            partitions,
            ts_min,
            ts_max,
            content,
            erasure,
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
        let tenant_hash = self.tenant_hash;
        let content = Arc::clone(&self.content);
        let erasure = Arc::clone(&self.erasure);
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("AlertsScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            tenant_hash,
            segs,
            self.ts_min,
            self.ts_max,
            content,
            erasure,
            self.accounting.clone(),
        ));
        Ok(Box::pin(AlertScanStream {
            schema,
            reservation,
            state: AlertScanState::Fetching(fut),
        }))
    }
}

/// The write identity of the object a record was read from (ADR-1101 decision
/// 1's row contract): the three [`SegmentRef`] fields the `alerts` table
/// promotes to columns. Carried per record rather than per partition because a
/// partition fetches several objects and the records are then sorted together.
#[derive(Clone)]
struct WriteIdentity {
    writer_id: String,
    writer_epoch: u64,
    writer_seq: u64,
}

impl WriteIdentity {
    fn from_segment(seg: &SegmentRef) -> Self {
        WriteIdentity {
            // The hyphenated uuid string, the same spelling the commit record's
            // own writer id renders to.
            writer_id: seg.writer_id.to_string(),
            writer_epoch: seg.writer_epoch,
            writer_seq: seg.writer_seq,
        }
    }
}

/// One fetched alert record and the identity of the object it came from.
type StampedRecord = (LogRecord, WriteIdentity);

/// Fetch every segment in this partition and return its records sorted by
/// `ts_ns` ascending, each stamped with its object's write identity. The ts
/// range and the `alert_id`/`rule_id` equalities prune the fetch exactly; the
/// residual re-applies everything above.
#[allow(clippy::too_many_arguments)]
async fn prepare_partition(
    fetcher: LogSegmentFetcher,
    tenant_hash: TenantHash,
    segs: Vec<SegmentRef>,
    ts_min: i64,
    ts_max: i64,
    content: Arc<Vec<Predicate>>,
    erasure: Arc<Vec<ErasurePredicate>>,
    accounting: QueryAccounting,
) -> DFResult<Vec<StampedRecord>> {
    let mut query = LogQuery::new(ts_min, ts_max).with_erasure((*erasure).clone());
    for c in content.iter() {
        query = query.with_content(c.clone());
    }

    let mut out: Vec<StampedRecord> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher
            .fetch_accounted_with_tenant(seg, tenant_hash, &query, &accounting)
            .await
            .map_err(SqlError::from)?
        else {
            continue;
        };
        // Stamped here, where the record and the `SegmentRef` it was read from
        // are both in hand: once the partition's records are merged and sorted
        // there is no way back to the object.
        let identity = WriteIdentity::from_segment(seg);
        out.extend(
            output
                .records
                .into_iter()
                .map(|record| (record, identity.clone())),
        );
    }
    // Scan-layer selective-erasure exclusion (ADR-0064). This is the
    // authoritative exclusion because it sees the same merged `attrs` view the
    // surface returns (resource + scope + record), so a subject named only in a
    // resource/scope attribute is dropped; the fetcher-level filter matches
    // per-record attributes alone and cannot see it.
    retain_unerased_by(&mut out, &erasure, |(record, _)| record)?;
    // Stable sort so records with equal ts keep the reader's emission order.
    out.sort_by_key(|(record, _)| record.ts_ns);
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<StampedRecord>>> + Send>>;

enum AlertScanState {
    Fetching(PrepareFuture),
    Emitting {
        records: Vec<StampedRecord>,
        pos: usize,
    },
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

/// Decode a slice of stamped records into one `alerts`-schema [`RecordBatch`].
/// The four scalar fields are promoted from each record's merged attributes
/// ([`find_attr`] over [`merged_attrs`]), the full merged map is rendered into
/// the `attrs` column, so a promoted column and the `attrs` map for the same key
/// never disagree, and the three write-identity columns come from the object's
/// own [`SegmentRef`], never from an attribute.
fn build_batch(records: &[StampedRecord], schema: SchemaRef) -> DFResult<RecordBatch> {
    let ts =
        TimestampNanosecondArray::from(records.iter().map(|(r, _)| r.ts_ns).collect::<Vec<_>>());

    let mut alert_id = StringBuilder::new();
    let mut rule_id = StringBuilder::new();
    let mut state = StringBuilder::new();
    let mut generation = Int64Builder::new();
    let mut writer_id = StringBuilder::new();
    let mut writer_epoch = UInt64Builder::new();
    let mut writer_seq = UInt64Builder::new();
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for (r, identity) in records {
        let merged = merged_attrs(r)?;

        append_str_attr(&mut alert_id, find_attr(&merged, ALERT_ID_KEY));
        append_str_attr(&mut rule_id, find_attr(&merged, RULE_ID_KEY));
        append_str_attr(&mut state, find_attr(&merged, STATE_KEY));
        append_generation(&mut generation, find_attr(&merged, GENERATION_KEY));

        writer_id.append_value(&identity.writer_id);
        writer_epoch.append_value(identity.writer_epoch);
        writer_seq.append_value(identity.writer_seq);

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
        Arc::new(writer_id.finish()),
        Arc::new(writer_epoch.finish()),
        Arc::new(writer_seq.finish()),
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
