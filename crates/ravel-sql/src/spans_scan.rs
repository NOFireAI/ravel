//! `SpansScanExec`: the leaf of the `spans` pipeline, the span-signal sibling
//! of [`crate::logs_scan::LogsScanExec`] (ADR-0041, phase 5).
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through [`SpanSegmentFetcher::fetch_accounted`] with one shared
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
//! pushes the ts window, the optional trace_id equality (both evaluated
//! *exactly* per row), and the optional `service_name`/`name` equalities as
//! per-block bloom probes (ADR-0054). The bloom probes are widen-only
//! (ADR-0013): a negative probe proves the token absent so the block truly held
//! no matching row, and a bloom false positive only wastes a block decode,
//! never drops a needed row -- the exact per-row ts/trace_id check and
//! DataFusion's residual do the rest. RSPAN's `attrs` is already the merged
//! resource+scope+span map on each record, so unlike `logs` there is no
//! stream-identity blob to decode or re-verify here: [`build_batch`] copies
//! `record.attrs` straight into the `Map(Utf8, Utf8)` column, and reads
//! `service_name` from the [`SpanRow`]'s direct v3-column value.

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
use ravel_query::erasure::{ErasurePredicate, is_erased_span};
use ravel_rspan::{BloomPredicate, COL_NAME, COL_SERVICE_NAME, SpanQuery};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::error::SqlError;
use crate::spans_fetcher::{SpanRow, SpanSegmentFetcher};
use crate::spans_schema::{SPAN_ID_WIDTH, TRACE_ID_WIDTH, spans_schema};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Metric name for the count of batches emitted by the columnar fast path, so
/// `EXPLAIN ANALYZE` shows which path a partition ran (ADR-0110 decision 6).
const METRIC_COLUMNAR_BATCHES: &str = "columnar_batches";
/// Metric name for the count of batches emitted by the row path fallback.
const METRIC_ROWPATH_BATCHES: &str = "rowpath_batches";

/// Span segment scan producing per-partition `(trace_id, start_ts)`-ordered
/// batches over the public `spans` schema.
pub struct SpansScanExec {
    tenant_hash: TenantHash,
    fetcher: SpanSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// The shared query handed to every segment fetch: the ts window and, on
    /// the fast path, the trace_id key. One query covers the whole scan.
    query: SpanQuery,
    /// The `service_name = <literal>` equality pushed, if any: a `COL_SERVICE_NAME`
    /// bloom probe applied per block before decode (ADR-0054). One value covers
    /// the whole scan, like `query`.
    service_name: Option<String>,
    /// The `name = <literal>` equality pushed, if any: a `COL_NAME` bloom probe.
    name: Option<String>,
    /// The inclusive `[min, max]` `duration_ns` window pushed, if any: a
    /// skip-index prune dropping blocks whose duration range cannot
    /// overlap it. `None` when no duration filter was pushed. Widen-only.
    duration_ns: Option<(i64, i64)>,
    /// The `status_code` bitmask pushed, if any: a skip-index
    /// prune dropping blocks that share no status bit with it. `None` when no
    /// status filter was pushed. Widen-only.
    status_mask: Option<u8>,
    /// Pending selective-erasure predicates from the resolved snapshot
    /// (ADR-0064 decision 2). Applied per decoded [`SpanRow`] via
    /// [`is_erased_span`] immediately after `fetcher.fetch_accounted` returns, before
    /// rows are sorted or built into batches. A no-op when empty.
    erasure: Arc<Vec<ErasurePredicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), threaded into every
    /// per-partition fetch so span fetches are recorded like every other
    /// funnel, mirroring `LogsScanExec`.
    accounting: QueryAccounting,
}

impl SpansScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, driven by `query`,
    /// the optional `service_name`/`name` bloom-probe literals, and the
    /// optional `duration_ns`/`status_mask` skip-index prune shapes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: SpanSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        query: SpanQuery,
        service_name: Option<String>,
        name: Option<String>,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        erasure: Arc<Vec<ErasurePredicate>>,
        accounting: QueryAccounting,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = spans_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(SpansScanExec {
            tenant_hash,
            fetcher,
            partitions,
            query,
            service_name,
            name,
            duration_ns,
            status_mask,
            erasure,
            schema,
            properties,
            accounting,
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
        let tenant_hash = self.tenant_hash;
        let schema = Arc::clone(&self.schema);

        let reservation = MemoryConsumer::new(format!("SpansScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            tenant_hash,
            segs,
            self.query,
            self.service_name.clone(),
            self.name.clone(),
            self.duration_ns,
            self.status_mask,
            Arc::clone(&self.erasure),
            self.accounting.clone(),
        ));
        Ok(Box::pin(SpanScanStream {
            schema,
            reservation,
            state: SpanScanState::Fetching(fut),
        }))
    }
}

/// Fetch every segment in this partition and return its rows in `(trace_id,
/// start_ts)` order. Every fetched row already satisfies `query` exactly (the
/// scan re-checks the ts overlap and trace_id per row); the
/// `service_name`/`name` bloom probes only ever widened the block read set, so
/// nothing here narrows beyond what the exact per-row check keeps.
#[allow(clippy::too_many_arguments)]
async fn prepare_partition(
    fetcher: SpanSegmentFetcher,
    tenant_hash: TenantHash,
    segs: Vec<SegmentRef>,
    query: SpanQuery,
    service_name: Option<String>,
    name: Option<String>,
    duration_ns: Option<(i64, i64)>,
    status_mask: Option<u8>,
    erasure: Arc<Vec<ErasurePredicate>>,
    accounting: QueryAccounting,
) -> DFResult<Vec<SpanRow>> {
    // The bloom predicates for this scan: one per pushed literal, field-scoped
    // to the column it names (ADR-0054). The same set applies to every segment.
    let mut predicates: Vec<BloomPredicate> = Vec::new();
    if let Some(s) = &service_name {
        predicates.push(BloomPredicate {
            field_id: COL_SERVICE_NAME,
            literal: s.as_str(),
        });
    }
    if let Some(n) = &name {
        predicates.push(BloomPredicate {
            field_id: COL_NAME,
            literal: n.as_str(),
        });
    }

    let mut out: Vec<SpanRow> = Vec::new();
    for seg in &segs {
        let Some(output) = fetcher
            .fetch_accounted(
                seg,
                tenant_hash,
                &query,
                duration_ns,
                status_mask,
                &predicates,
                &accounting,
            )
            .await
            .map_err(SqlError::from)?
        else {
            continue;
        };
        out.extend(output.records);
    }
    // Selective-erasure exclusion (ADR-0064 decision 2): applied
    // to each decoded row immediately after fetch, before sort/build. A
    // no-op when `erasure` is empty.
    if !erasure.is_empty() {
        out.retain(|row| !is_erased_span(&row.record.attrs, row.record.start_ts_ns, &erasure));
    }
    // Stable sort by the native (trace_id, start_ts) key so rows tying on both
    // keep the reader's per-object emission order.
    out.sort_by(|a, b| {
        a.record
            .trace_id
            .cmp(&b.record.trace_id)
            .then_with(|| a.record.start_ts_ns.cmp(&b.record.start_ts_ns))
    });
    Ok(out)
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Vec<SpanRow>>> + Send>>;

enum SpanScanState {
    Fetching(PrepareFuture),
    Emitting { records: Vec<SpanRow>, pos: usize },
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

/// Decode a slice of rows into one `spans`-schema [`RecordBatch`].
fn build_batch(rows: &[SpanRow], schema: SchemaRef) -> DFResult<RecordBatch> {
    let mut trace = FixedSizeBinaryBuilder::with_capacity(rows.len(), TRACE_ID_WIDTH);
    for row in rows {
        trace
            .append_value(row.record.trace_id)
            .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?;
    }
    let trace = trace.finish();

    let mut span = FixedSizeBinaryBuilder::with_capacity(rows.len(), SPAN_ID_WIDTH);
    for row in rows {
        span.append_value(row.record.span_id)
            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?;
    }
    let span = span.finish();

    let mut parent = FixedSizeBinaryBuilder::with_capacity(rows.len(), SPAN_ID_WIDTH);
    for row in rows {
        match &row.record.parent_span_id {
            Some(id) => parent
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("parent_span_id array build: {e}")))?,
            None => parent.append_null(),
        }
    }
    let parent = parent.finish();

    let name = StringArray::from(
        rows.iter()
            .map(|row| row.record.name.as_str())
            .collect::<Vec<_>>(),
    );
    let start_ts = TimestampNanosecondArray::from(
        rows.iter()
            .map(|row| row.record.start_ts_ns)
            .collect::<Vec<_>>(),
    );
    let end_ts = TimestampNanosecondArray::from(
        rows.iter()
            .map(|row| row.record.end_ts_ns)
            .collect::<Vec<_>>(),
    );
    let status_code = UInt8Array::from(
        rows.iter()
            .map(|row| row.record.status_code.to_u8())
            .collect::<Vec<_>>(),
    );
    let status_message = StringArray::from(
        rows.iter()
            .map(|row| row.record.status_message.as_deref())
            .collect::<Vec<_>>(),
    );

    // `attrs` map: RSPAN already merged resource+scope+span into `record.attrs`
    // with unique, ascending keys, so it copies straight into the column with
    // no decode or re-verification (unlike `logs`, whose merged view is rebuilt
    // at scan time from a stream-identity blob). `service.name` is present here
    // too: the reader re-inserts it into the record's attrs from the v3 column,
    // so this map is the same one a v1 object would have produced.
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        for (k, v) in &row.record.attrs {
            attrs.keys().append_value(k);
            attrs.values().append_value(v);
        }
        attrs
            .append(true)
            .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
    }
    let attrs = attrs.finish();

    // service_name (v3, ADR-0054): read straight from the dictionary-encoded
    // `COL_SERVICE_NAME` column by the fetcher and carried on each [`SpanRow`],
    // not looked up by linear scan of the merged attrs map at build time. NULL
    // when the span carried no `service.name`.
    let service_name = StringArray::from(
        rows.iter()
            .map(|row| row.service_name.as_deref())
            .collect::<Vec<_>>(),
    );
    // duration_ns is computed, never stored (ADR-0045 decision 5, rejected
    // alternative 3). `saturating_sub` rather than a bare `-`: end_ts_ns >=
    // start_ts_ns is a format invariant, not one this column should assume
    // and panic on if corrupt or adversarial data ever violates it.
    let duration_ns = Int64Array::from(
        rows.iter()
            .map(|row| row.record.end_ts_ns.saturating_sub(row.record.start_ts_ns))
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, FixedSizeBinaryArray, StringArray};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{col, lit};
    use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};
    use uuid::Uuid;

    use super::*;
    use crate::spans_provider::SpansTableProvider;
    use crate::spans_pushdown::extract_spans;
    use crate::spans_schema::{SPAN_COL_SERVICE_NAME, SPAN_COL_TRACE_ID};

    fn span_with_service(trace: u8, start: i64, service: &str) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [trace; 8],
            parent_span_id: None,
            name: format!("op-{trace}"),
            start_ts_ns: start,
            end_ts_ns: start + 1,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![
                ("service.name".to_string(), service.to_string()),
                ("http.method".to_string(), "GET".to_string()),
            ],
        }
    }

    /// A span whose `status_code` and `duration_ns` (`end_ts - start_ts`) are
    /// set independently, so a `status_code = ... AND duration_ns >= ...`
    /// filter matches or misses it by design. Distinct ascending trace_ids so
    /// each record lands in its own block under `block_target_records = 1`.
    fn span_with_status_and_duration(trace: u8, status: StatusCode, duration: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [trace; 8],
            parent_span_id: None,
            name: format!("op-{trace}"),
            start_ts_ns: 0,
            end_ts_ns: duration,
            status_code: status,
            status_message: None,
            attrs: vec![("service.name".to_string(), "svc".to_string())],
        }
    }

    /// REACHABILITY test: a real SQL `spans` query carrying BOTH a
    /// `status_code` filter AND a `duration_ns` filter is parsed by DataFusion,
    /// extracted into `SpansPushdown`, and driven through the same
    /// provider/fetcher path production uses. It proves the two things this
    /// scan closes:
    ///
    /// 1. Blocks are ACTUALLY SKIPPED: with the extracted `status_mask` and
    ///    `duration_window()` fed to the skip index (exactly what
    ///    `SpansTableProvider::build_scan` now passes into
    ///    `SpansScanExec::new`), the fetcher decodes strictly fewer blocks than
    ///    the same scan with both prune shapes disabled (`None, None`, the
    ///    unpruned wire-up).
    /// 2. Results are UNCHANGED under pruning: the exact set of matching rows
    ///    is identical whether the skip index prunes or not (the widen-safe
    ///    invariant), and equals the independently computed oracle. The full
    ///    SQL query, run end to end through `build_session` so DataFusion's
    ///    `Inexact` residual re-applies the predicate, returns that same set.
    ///
    /// Flip to watch assertion (1) fail: in the `pruned` fetch below, pass
    /// `None, None` in place of `duration_window`/`status_mask` (equivalently,
    /// revert `build_scan` to `None, None`); the skip index then
    /// keeps every block and `pruned.stats.blocks_scanned < full...` fails.
    #[tokio::test]
    async fn status_and_duration_filters_actually_skip_blocks_and_preserve_results() {
        use crate::config::SqlConfig;
        use crate::memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};
        use crate::session::{SessionTable, build_session};
        use ravel_types::accounting::QueryAccounting;

        // One span per block, so a per-block prune is visible as a block count.
        let cfg = RspanConfig {
            block_target_records: 1,
            ..RspanConfig::default()
        };
        // (status, duration_ns). The query keeps status = Error (2) AND
        // duration_ns >= 500: traces 0, 3, 5 match; 1 fails status, 2 fails
        // duration, 4 fails status.
        let specs = [
            (StatusCode::Error, 1000),
            (StatusCode::Ok, 1000),
            (StatusCode::Error, 100),
            (StatusCode::Error, 1000),
            (StatusCode::Unset, 1000),
            (StatusCode::Error, 2000),
        ];
        let records: Vec<SpanRecord> = specs
            .iter()
            .enumerate()
            .map(|(i, (status, duration))| {
                span_with_status_and_duration(i as u8, *status, *duration)
            })
            .collect();

        let mut w = RspanWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [1u8; 16],
                shard: 0,
                writer_id: [2u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for r in &records {
            w.push(r.clone());
        }
        let bytes = w.finish().expect("finish");
        let object_size = bytes.len() as u64;

        let store = MemoryStore::new();
        let key = "spans/status-duration.rspan";
        store
            .put(key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");

        let seg = SegmentRef {
            data_object_key: key.to_string(),
            object_size,
            min_event_ts_ns: 0,
            max_event_ts_ns: 2000,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        };

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = SpanSegmentFetcher::new(Arc::clone(&store));

        // The real SQL filter, parsed and type-coerced by DataFusion, then
        // extracted into the pushdown production feeds the skip index.
        let sql = "SELECT trace_id, status_code, duration_ns FROM spans \
                   WHERE status_code = 2 AND duration_ns >= 500";
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = SpansTableProvider::new(
            snapshot,
            TenantHash([1u8; 16]),
            fetcher.clone(),
            QueryAccounting::new(),
        );
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let breach = CeilingBreach::new();
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
            TenantDelegatingPool::new(1 << 30, tenant, breach, QueryAccounting::new()),
        );
        let ctx = build_session(
            &SqlConfig::default(),
            pool,
            SessionTable::Spans(Arc::new(provider)),
            false,
        )
        .expect("spans session builds");

        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("filtered query plans");
        let mut predicates = Vec::new();
        collect_filter_predicates(&plan, &mut predicates);
        let pushdown = extract_spans(&predicates);
        assert_eq!(
            pushdown.status_mask,
            Some(ravel_rspan::skip_index::STATUS_BIT_ERROR),
            "the WHERE clause must extract the Error status bit"
        );
        assert_eq!(
            pushdown.duration_window(),
            Some((500, i64::MAX)),
            "the WHERE clause must extract the [500, MAX] duration window"
        );

        // (1) Blocks actually skipped. `pruned` passes exactly what build_scan
        // now forwards; `full` disables both prune shapes (the unpruned call).
        let query = pushdown.span_query();
        let pruned = fetcher
            .fetch(
                &seg,
                &query,
                pushdown.duration_window(),
                pushdown.status_mask,
                &[],
            )
            .await
            .expect("fetch pruned")
            .expect("relevant");
        let full = fetcher
            .fetch(&seg, &query, None, None, &[])
            .await
            .expect("fetch full")
            .expect("relevant");

        assert_eq!(full.stats.blocks_total, 6, "six blocks total");
        assert_eq!(
            full.stats.blocks_scanned, 6,
            "with pruning disabled every block is decoded"
        );
        assert!(
            pruned.stats.blocks_scanned < full.stats.blocks_scanned,
            "the status+duration skip-index prune must decode fewer blocks \
             ({} of {}) than the unpruned scan ({})",
            pruned.stats.blocks_scanned,
            pruned.stats.blocks_total,
            full.stats.blocks_scanned,
        );
        assert_eq!(
            pruned.stats.blocks_scanned, 3,
            "exactly the three Error/>=500 blocks (traces 0, 3, 5) survive"
        );

        // (2) Results unchanged under pruning: the exact predicate re-evaluated
        // over each fetch's rows yields the identical set, and it equals the
        // oracle. `duration_ns` is computed the same way the scan's
        // `build_batch` computes it.
        let matches_predicate = |r: &SpanRecord| {
            r.status_code.to_u8() == 2 && r.end_ts_ns.saturating_sub(r.start_ts_ns) >= 500
        };
        let pruned_rows: BTreeSet<[u8; 16]> = pruned
            .records
            .iter()
            .map(|r| &r.record)
            .filter(|r| matches_predicate(r))
            .map(|r| r.trace_id)
            .collect();
        let full_rows: BTreeSet<[u8; 16]> = full
            .records
            .iter()
            .map(|r| &r.record)
            .filter(|r| matches_predicate(r))
            .map(|r| r.trace_id)
            .collect();
        let oracle: BTreeSet<[u8; 16]> = BTreeSet::from([[0u8; 16], [3u8; 16], [5u8; 16]]);
        assert_eq!(
            pruned_rows, full_rows,
            "the matching row set is identical with pruning on vs off"
        );
        assert_eq!(pruned_rows, oracle, "and equals the oracle");

        // The full SQL query, end to end through the session (residual applied),
        // returns exactly the oracle rows.
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let mut sql_rows: BTreeSet<[u8; 16]> = BTreeSet::new();
        for batch in &batches {
            let trace = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("trace_id col");
            for i in 0..batch.num_rows() {
                sql_rows.insert(trace.value(i).try_into().expect("16-byte trace"));
            }
        }
        assert_eq!(
            sql_rows, oracle,
            "the real SQL query returns exactly the matching rows under pruning"
        );
    }

    /// Collect every `WHERE`/`HAVING` predicate in `plan` as a top-level AND
    /// conjunct (mirrors `executor::collect_filter_predicates` and the sibling
    /// helper in `session`'s tests), recursing through inputs.
    fn collect_filter_predicates(
        plan: &datafusion::logical_expr::LogicalPlan,
        out: &mut Vec<datafusion::logical_expr::Expr>,
    ) {
        if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
            out.push(filter.predicate.clone());
        }
        for input in plan.inputs() {
            collect_filter_predicates(input, out);
        }
    }

    /// Acceptance test: a `WHERE service_name = '<literal>'` query
    /// driven through the real [`SpansTableProvider`] (its SQL scan entry point,
    /// not a crate-internal call)
    /// returns exactly the matching rows AND decodes strictly fewer blocks than
    /// a full scan, proving the v3 bloom prune actually fired rather than just
    /// that pushdown parsing succeeded.
    #[tokio::test]
    async fn service_name_predicate_prunes_via_bloom_and_returns_correct_rows() {
        // One span per block (block_target_records = 1) so each block's bloom
        // and service_name column hold a single known value, and a per-block
        // prune is visible as a block count. Distinct ascending trace_ids keep
        // records one-per-block in this order.
        let cfg = RspanConfig {
            block_target_records: 1,
            ..RspanConfig::default()
        };
        // Six blocks; "checkout" occupies three of them (0, 2, 5). A bloom prune
        // on service_name = "checkout" must drop the other three before decode.
        let services = [
            "checkout",
            "payments",
            "checkout",
            "inventory",
            "payments",
            "checkout",
        ];
        let records: Vec<SpanRecord> = services
            .iter()
            .enumerate()
            .map(|(i, svc)| span_with_service(i as u8, i as i64, svc))
            .collect();

        let mut w = RspanWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [1u8; 16],
                shard: 0,
                writer_id: [2u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for r in &records {
            w.push(r.clone());
        }
        let bytes = w.finish().expect("finish");
        let object_size = bytes.len() as u64;

        let store = MemoryStore::new();
        let key = "spans/service.rspan";
        store
            .put(key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");

        let seg = SegmentRef {
            data_object_key: key.to_string(),
            object_size,
            min_event_ts_ns: 0,
            max_event_ts_ns: services.len() as i64,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        };

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = SpanSegmentFetcher::new(Arc::clone(&store));
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = SpansTableProvider::new(
            snapshot,
            TenantHash([1u8; 16]),
            fetcher.clone(),
            QueryAccounting::new(),
        );

        // (1) Correctness through the real provider scan entry point.
        let filters = vec![col("service_name").eq(lit("checkout"))];
        let plan = provider.plan_filters(4, &filters).expect("build plan");
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("collect");

        let mut got: BTreeSet<([u8; 16], String)> = BTreeSet::new();
        for batch in &batches {
            assert_eq!(batch.schema(), spans_schema(), "public spans schema");
            let trace = batch
                .column(SPAN_COL_TRACE_ID)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("trace_id col");
            let service = batch
                .column(SPAN_COL_SERVICE_NAME)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("service_name col");
            for i in 0..batch.num_rows() {
                let tid: [u8; 16] = trace.value(i).try_into().expect("16-byte trace");
                assert!(!service.is_null(i), "matched row must carry a service_name");
                got.insert((tid, service.value(i).to_string()));
            }
        }

        // The oracle: exactly the "checkout" spans (traces 0, 2, 5), each with
        // the service_name read from the v3 column, and nothing else.
        let mut want: BTreeSet<([u8; 16], String)> = BTreeSet::new();
        for (i, svc) in services.iter().enumerate() {
            if *svc == "checkout" {
                want.insert(([i as u8; 16], "checkout".to_string()));
            }
        }
        assert_eq!(got, want, "query returns exactly the checkout rows");

        // (2) The bloom prune fired: decoding strictly fewer blocks than a full
        // scan. Reproduce the exact query + predicate the provider pushes, and
        // spy on the reader's ScanStats via the fetcher (blocks_scanned counts
        // blocks actually decoded).
        let pushdown = extract_spans(&filters);
        assert_eq!(
            pushdown.service_name.as_deref(),
            Some("checkout"),
            "the WHERE clause must extract a service_name equality"
        );
        let query = pushdown.span_query();
        let literal = pushdown.service_name.as_deref().expect("service_name");
        let predicates = [BloomPredicate {
            field_id: COL_SERVICE_NAME,
            literal,
        }];

        let full = fetcher
            .fetch(&seg, &query, None, None, &[])
            .await
            .expect("fetch full")
            .expect("relevant");
        let pruned = fetcher
            .fetch(&seg, &query, None, None, &predicates)
            .await
            .expect("fetch pruned")
            .expect("relevant");

        assert_eq!(
            full.stats.blocks_total, pruned.stats.blocks_total,
            "same object, same total block count"
        );
        assert_eq!(
            full.stats.blocks_scanned, 6,
            "a full scan decodes every block"
        );
        assert!(
            pruned.stats.blocks_scanned < full.stats.blocks_scanned,
            "the bloom prune must decode fewer blocks ({} of {}) than a full scan ({})",
            pruned.stats.blocks_scanned,
            pruned.stats.blocks_total,
            full.stats.blocks_scanned,
        );
        // The surviving blocks are exactly the three "checkout" ones: the prune
        // is sound (no checkout row dropped) and effective (non-checkout blocks
        // gone). No false negatives means all three survive; the distinct
        // single-token service names make a false positive vanishingly unlikely.
        assert_eq!(
            pruned.stats.blocks_scanned, 3,
            "exactly the three checkout blocks survive the bloom prune"
        );
        let pruned_traces: BTreeSet<[u8; 16]> =
            pruned.records.iter().map(|r| r.record.trace_id).collect();
        assert_eq!(
            pruned_traces,
            BTreeSet::from([[0u8; 16], [2u8; 16], [5u8; 16]]),
            "the decoded rows are exactly the checkout traces"
        );
        // Every decoded row's service_name came from the v3 column directly.
        for row in &pruned.records {
            assert_eq!(row.service_name.as_deref(), Some("checkout"));
        }
    }
}
