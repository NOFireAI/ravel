//! `RsegScanExec`: the leaf of the samples pipeline.
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through the shared `SegmentFetcher` SoA surface
//! (ticket A1a), and produces one stream sorted by `(series_id, ts,
//! created_unix_ns, writer_epoch, writer_seq, in_page_index)`. That full
//! ordering is declared through `PlanProperties` so the optimizer honors it
//! with a `SortPreservingMergeExec`, never a `CoalescePartitionsExec` (review
//! F12).
//!
//! Emission is a streaming k-way merge, not a materialize-then-sort. Each
//! segment decodes into its own sorted run (bounded by one segment), the runs
//! are merged by a binary heap, and rows are emitted in bounded batches. This
//! replaces B1's build-one-giant-`Vec<ScanRow>`-then-`sort_by_key`-then-one-
//! batch pattern: that pattern sorted every sample from every segment before
//! any budget check and produced a single end-of-partition batch, which
//! defeats both the per-batch byte accounting the memory pool needs (below) and
//! the reduced fetch that pushdown enables. A partition's stream is still
//! globally sorted across all its batches, so the declared ordering holds.
//!
//! Pushdown (ticket B2, docs/arrow-datafusion-plan.md "Filter pushdown"):
//! label/series matchers are threaded into `fetch_soa` so the fetcher prunes
//! series (and their page GETs) against SERIES_TABLE. A `series_id` allow-set
//! is applied as a post-fetch row filter, because the fetcher's matcher API is
//! label-only; segment-level ts pruning happens one level up in the provider.
//! Every prune is widen-only (see crate::pushdown).
//!
//! Memory (review F10/F13; issue #188): each partition registers a
//! `MemoryConsumer` against the `TaskContext`'s pool and grows one
//! `MemoryReservation` in two phases:
//!
//! - Fetch/decode phase (`prepare_partition`): after each segment's
//!   `fetch_soa` call returns, the reservation grows by that segment's
//!   decoded run size (`runs` entries, `size_of::<ScanRow>()` each) plus
//!   `FetchStats::raw_f64_bytes` (the one fetched-buffer byte figure
//!   `SegmentFetcher` already exposes without a ravel-query API change --
//!   see the crate module doc's context-discipline note). A `try_grow`
//!   failure here returns before the next segment is fetched at all.
//! - Batch phase (`ScanStream::poll_next`, `ScanState::Merging`): the
//!   existing per-batch grow by `RecordBatch::get_array_memory_size`.
//!
//! These two phases charge disjoint, simultaneously-live allocations, not the
//! same bytes twice: the decoded `ScanRow` runs stay allocated in `Merger` for
//! the whole partition (batches only advance a cursor into them, they never
//! free the run), while each built `RecordBatch` is a separate Arrow-array
//! allocation constructed from a slice of those rows. Both are real,
//! concurrently resident memory at the point a batch is emitted, so charging
//! both is the correct accounting, not double-counting. docs/arrow-
//! datafusion-plan.md's "Session config, memory pool, budgets" section
//! describes only the shrink-per-yielded-batch half of this; it does not yet
//! describe the fetch/decode charge added here (see the note added there).
//!
//! Each partition's reservation is threaded through `prepare_partition` (the
//! fetch/decode phase owns it first) and back into `ScanStream` (the batch
//! phase continues growing the same reservation), so a dropped or cancelled
//! stream at any point frees the one reservation via `Drop` (the pool forwards
//! the shrink to the tenant accountant), and a query that outgrows its byte
//! budget fails with the pool's `ResourcesExhausted`.
//!
//! `max_series` (issue #187) is enforced the same way: `prepare_partition`
//! tracks the distinct `series_id` count in the `labels` map it is already
//! building, and fails with `SqlError::TooManySeries` the moment a new
//! series pushes that count past `max_series`, before decoding that series'
//! samples or fetching any further segment. This is a per-partition count,
//! not a cross-partition global one: `RsegScanExec` partitions by segment
//! (round-robin), not by series, so a series repeated across many segments
//! can be counted independently by more than one partition's stream. A true
//! global cap would need a distinct-series set shared across a partition's
//! concurrently-executing sibling streams, which is a larger change than this
//! ticket's scope; the per-partition cap is the enforced approximation, and
//! it still turns an unbounded-cardinality query into a bounded one per
//! partition, failing before that partition's remaining segments are
//! fetched.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_promql::LabelMatcher;
use ravel_query::{ByteLimit, SegmentFetcher};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{LabelSet, TenantHash};

use crate::error::SqlError;
use crate::labels::build_labels_dict;
use crate::schema::{COL_SERIES_ID, COL_TS, internal_schema};

/// Rows accumulated into one output batch before it is emitted. Bounds the
/// per-batch working set and gives the memory reservation a per-batch
/// granularity.
const BATCH_ROWS: usize = 8192;

/// The total sort key: `(series_id, ts, created_unix_ns, writer_epoch,
/// writer_seq, in_page_index)`.
type SortKey = ([u8; 16], i64, i64, u64, u64, u32);

/// One decoded sample plus its full provenance tuple.
#[derive(Clone, Copy)]
struct ScanRow {
    series_id: [u8; 16],
    ts: i64,
    value: f64,
    created_unix_ns: i64,
    writer_epoch: u64,
    writer_seq: u64,
    in_page_index: u32,
}

impl ScanRow {
    fn sort_key(&self) -> SortKey {
        (
            self.series_id,
            self.ts,
            self.created_unix_ns,
            self.writer_epoch,
            self.writer_seq,
            self.in_page_index,
        )
    }
}

/// Segment scan producing per-partition `(series_id, ts, provenance)`-sorted
/// batches over the internal schema.
pub struct RsegScanExec {
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` is executed as
    /// DataFusion partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Label matchers pushed into `fetch_soa` (widen-only series pruning).
    matchers: Arc<Vec<LabelMatcher>>,
    /// Optional `series_id` allow-set applied as a post-fetch row filter.
    /// `None` means unconstrained.
    series_ids: Option<Arc<HashSet<[u8; 16]>>>,
    /// Per-partition distinct-series_id budget (issue #187); see the module
    /// doc for why this is per-partition, not a cross-partition total.
    max_series: usize,
    /// Per-tenant bytes-scanned budget (ADR-0061 decision 1, issue #722),
    /// checked once per completed segment fetch against the running
    /// `QueryAccounting` total. `Unlimited` never trips, so a caller that does
    /// not opt in behaves exactly as before this budget existed.
    max_bytes_scanned: ByteLimit,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// partition's `fetch_soa_accounted` call.
    accounting: QueryAccounting,
}

impl RsegScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given
    /// pushdown matchers, optional `series_id` allow-set, per-partition
    /// `max_series` budget (issue #187), and per-tenant `max_bytes_scanned`
    /// budget (ADR-0061 decision 1, issue #722).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: SegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        matchers: Arc<Vec<LabelMatcher>>,
        series_ids: Option<Arc<HashSet<[u8; 16]>>>,
        max_series: usize,
        max_bytes_scanned: ByteLimit,
        accounting: QueryAccounting,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let schema = internal_schema();
        let properties = Arc::new(Self::compute_properties(&schema, n)?);
        Ok(RsegScanExec {
            tenant_hash,
            fetcher,
            partitions,
            matchers,
            series_ids,
            max_series,
            max_bytes_scanned,
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
        let sort_exprs = [
            "series_id",
            "ts",
            "created_unix_ns",
            "writer_epoch",
            "writer_seq",
            "in_page_index",
        ]
        .into_iter()
        .map(|name| Ok(PhysicalSortExpr::new(col(name, schema)?, asc)))
        .collect::<DFResult<Vec<_>>>()?;
        let ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty scan ordering".into()))?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for RsegScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RsegScanExec {{ partitions: {} }}",
            self.partitions.len()
        )
    }
}

impl DisplayAs for RsegScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RsegScanExec: partitions={}, matchers={}",
            self.partitions.len(),
            self.matchers.len()
        )
    }
}

impl ExecutionPlan for RsegScanExec {
    fn name(&self) -> &str {
        "RsegScanExec"
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
        let tenant = self.tenant_hash;
        let matchers = Arc::clone(&self.matchers);
        let series_ids = self.series_ids.clone();
        let schema = Arc::clone(&self.schema);

        // One reservation per partition stream, registered against whatever
        // pool the TaskContext carries (the tenant-delegating pool in
        // production, an unbounded pool under bare `collect`). Owned by the
        // stream, so dropping the stream frees it and the pool forwards the
        // shrink to the tenant accountant (review F13).
        let reservation = MemoryConsumer::new(format!("RsegScanExec[{partition}]"))
            .register(context.memory_pool());

        let fut = Box::pin(prepare_partition(
            fetcher,
            tenant,
            segs,
            matchers,
            series_ids,
            self.max_series,
            self.max_bytes_scanned,
            reservation,
            self.accounting.clone(),
        ));
        Ok(Box::pin(ScanStream {
            schema,
            state: ScanState::Fetching(fut),
        }))
    }
}

/// Decoded, per-segment sorted runs plus the per-series label sets, ready to
/// merge. Built by [`prepare_partition`].
struct Prepared {
    runs: Vec<Vec<ScanRow>>,
    labels: HashMap<[u8; 16], LabelSet>,
}

/// Fetch every segment in this partition (matchers pushed into the fetcher),
/// decode each into its own ts/provenance-sorted run, and collect per-series
/// labels. Applies the `series_id` allow-set as a post-fetch row filter.
///
/// Enforces three budgets before the next segment is ever fetched: the
/// per-tenant bytes-scanned budget against the running `QueryAccounting`
/// total (ADR-0061 decision 1, issue #722), the distinct-series count against
/// `max_series` (issue #187), and the reservation's byte budget against this
/// segment's decoded size (issue #188). `reservation` is threaded through and
/// returned so the caller's batch phase continues growing the same one (see
/// module doc).
#[allow(clippy::too_many_arguments)]
async fn prepare_partition(
    fetcher: SegmentFetcher,
    tenant: TenantHash,
    segs: Vec<SegmentRef>,
    matchers: Arc<Vec<LabelMatcher>>,
    series_ids: Option<Arc<HashSet<[u8; 16]>>>,
    max_series: usize,
    max_bytes_scanned: ByteLimit,
    reservation: MemoryReservation,
    accounting: QueryAccounting,
) -> DFResult<(Prepared, MemoryReservation)> {
    let mut runs: Vec<Vec<ScanRow>> = Vec::with_capacity(segs.len());
    let mut labels: HashMap<[u8; 16], LabelSet> = HashMap::new();

    for seg in &segs {
        let (series, stats) = fetcher
            .fetch_soa_accounted(tenant, seg, &matchers, &accounting)
            .await
            .map_err(SqlError::from)?;
        // Per-tenant bytes-scanned budget (ADR-0061 decision 1): this fetch
        // has just charged its S3 bytes into the shared `accounting` handle,
        // so check the running total against the tenant's cap here, once per
        // completed segment fetch, before decoding this segment or fetching
        // the next one. This loop is genuinely sequential, so a trip here
        // straightforwardly means the remaining segments' GETs never happen.
        let scanned = accounting.snapshot().total_s3_bytes();
        if max_bytes_scanned.is_exceeded_by(scanned) {
            let max = match max_bytes_scanned {
                ByteLimit::Bounded(max) => max,
                ByteLimit::Unlimited => scanned,
            };
            return Err(SqlError::TooManyBytesScanned { scanned, max }.into());
        }
        let mut run: Vec<ScanRow> = Vec::new();
        for fs in series {
            let sid = fs.series_id.0;
            if let Some(allow) = &series_ids
                && !allow.contains(&sid)
            {
                continue;
            }
            if !labels.contains_key(&sid) && labels.len() >= max_series {
                return Err(SqlError::TooManySeries {
                    count: labels.len() + 1,
                    max: max_series,
                }
                .into());
            }
            labels.entry(sid).or_insert_with(|| fs.labels.clone());
            for (i, (&ts, &value)) in fs.timestamps.iter().zip(fs.values.iter()).enumerate() {
                run.push(ScanRow {
                    series_id: sid,
                    ts,
                    value,
                    created_unix_ns: fs.created_unix_ns,
                    writer_epoch: fs.writer_epoch,
                    writer_seq: fs.writer_seq,
                    in_page_index: u32::try_from(i).unwrap_or(u32::MAX),
                });
            }
        }
        // Charge this segment's decoded run plus its fetched buffer bytes
        // before fetching the next segment: a byte-budget overrun surfaces
        // here as the pool's typed ResourcesExhausted, not after every
        // segment in the partition has already been pulled.
        let segment_bytes = run
            .len()
            .saturating_mul(std::mem::size_of::<ScanRow>())
            .saturating_add(usize::try_from(stats.raw_f64_bytes).unwrap_or(usize::MAX));
        reservation.try_grow(segment_bytes)?;
        // Sort this segment's rows into one run under the full total order.
        // Bounded by a single segment, never all segments at once.
        run.sort_by_key(ScanRow::sort_key);
        if !run.is_empty() {
            runs.push(run);
        }
    }

    Ok((Prepared { runs, labels }, reservation))
}

/// Head-of-run entry in the merge heap: the next row's key and its run index.
/// `Reverse` turns the max-heap into a min-heap on the key.
type HeapEntry = Reverse<(SortKey, usize)>;

/// Streaming k-way merge over the per-segment sorted runs.
struct Merger {
    runs: Vec<Vec<ScanRow>>,
    cursors: Vec<usize>,
    heap: BinaryHeap<HeapEntry>,
    labels: HashMap<[u8; 16], LabelSet>,
}

impl Merger {
    fn new(prepared: Prepared) -> Self {
        let Prepared { runs, labels } = prepared;
        let mut cursors = vec![0usize; runs.len()];
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (idx, run) in runs.iter().enumerate() {
            if let Some(first) = run.first() {
                heap.push(Reverse((first.sort_key(), idx)));
                cursors[idx] = 1;
            }
        }
        Merger {
            runs,
            cursors,
            heap,
            labels,
        }
    }

    /// Pop up to `n` rows in global sort order.
    fn next_batch(&mut self, n: usize) -> Vec<ScanRow> {
        let mut out = Vec::with_capacity(n.min(self.heap.len().max(1)));
        while out.len() < n {
            let Some(Reverse((_key, idx))) = self.heap.pop() else {
                break;
            };
            let pos = self.cursors[idx] - 1;
            out.push(self.runs[idx][pos]);
            if let Some(next) = self.runs[idx].get(self.cursors[idx]) {
                self.heap.push(Reverse((next.sort_key(), idx)));
                self.cursors[idx] += 1;
            }
        }
        out
    }
}

/// The fetch/decode phase's future: builds `Prepared` and hands back the same
/// reservation it charged, for the batch phase to keep growing.
type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<(Prepared, MemoryReservation)>> + Send>>;

enum ScanState {
    Fetching(PrepareFuture),
    Merging(Merger, MemoryReservation),
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits merged
/// bounded batches, growing the memory reservation by each batch's measured
/// size. The reservation itself lives inside `state`: the fetch/decode phase
/// (`prepare_partition`) owns and grows it first, then hands it back to be
/// carried into `Merging`, so it is the same reservation for the whole
/// partition's lifetime and frees exactly once on drop.
struct ScanStream {
    schema: SchemaRef,
    state: ScanState,
}

impl Stream for ScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                ScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((prepared, reservation))) => {
                        this.state = ScanState::Merging(Merger::new(prepared), reservation);
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = ScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ScanState::Merging(merger, reservation) => {
                    let rows = merger.next_batch(BATCH_ROWS);
                    if rows.is_empty() {
                        this.state = ScanState::Done;
                        return Poll::Ready(None);
                    }
                    let batch = match build_batch(&rows, &merger.labels, Arc::clone(&this.schema)) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = ScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    // Grow the reservation by the batch's measured footprint.
                    // A byte-budget overrun surfaces here as the pool's typed
                    // ResourcesExhausted error, never a silent partial result.
                    if let Err(e) = reservation.try_grow(batch.get_array_memory_size()) {
                        this.state = ScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                ScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn build_batch(
    rows: &[ScanRow],
    labels_by_series: &HashMap<[u8; 16], LabelSet>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let ts = TimestampNanosecondArray::from(rows.iter().map(|r| r.ts).collect::<Vec<_>>());
    let value = Float64Array::from(rows.iter().map(|r| r.value).collect::<Vec<_>>());
    let series_id = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.series_id))
        .map_err(|e| SqlError::Internal(format!("series_id array build: {e}")))?;
    let created = Int64Array::from(rows.iter().map(|r| r.created_unix_ns).collect::<Vec<_>>());
    let epoch = UInt64Array::from(rows.iter().map(|r| r.writer_epoch).collect::<Vec<_>>());
    let seq = UInt64Array::from(rows.iter().map(|r| r.writer_seq).collect::<Vec<_>>());
    let in_page = UInt32Array::from(rows.iter().map(|r| r.in_page_index).collect::<Vec<_>>());

    // Dictionary-encode labels: one dictionary entry per distinct series in
    // this batch, an Int32 key per row. Rows are globally sorted by series_id,
    // so distinct series appear in contiguous runs (a series split across two
    // batches simply appears in each batch's dictionary).
    let mut distinct: Vec<LabelSet> = Vec::new();
    let mut keys: Vec<i32> = Vec::with_capacity(rows.len());
    let mut last: Option<[u8; 16]> = None;
    let mut idx: i32 = -1;
    for r in rows {
        if last != Some(r.series_id) {
            let labels = labels_by_series
                .get(&r.series_id)
                .cloned()
                .unwrap_or_default();
            distinct.push(labels);
            idx += 1;
            last = Some(r.series_id);
        }
        keys.push(idx);
    }
    let labels = build_labels_dict(&distinct, &keys).map_err(DataFusionError::from)?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts),
        Arc::new(value),
        Arc::new(series_id),
        labels,
        Arc::new(created),
        Arc::new(epoch),
        Arc::new(seq),
        Arc::new(in_page),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    debug_assert_eq!(COL_TS, 0);
    debug_assert_eq!(COL_SERIES_ID, 2);
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}
