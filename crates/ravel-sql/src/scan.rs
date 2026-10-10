//! `RsegScanExec`: the leaf of the samples pipeline.
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! fetches its segments through the shared `SegmentFetcher` SoA surface
//! (ticket A1a), and produces one stream sorted by `(series_id, ts,
//! created_unix_ns, writer_epoch, writer_seq, in_page_index)`. That full
//! ordering is declared through `PlanProperties` so the optimizer honors it
//! with a `SortPreservingMergeExec`, never a `CoalescePartitionsExec`.
//!
//! Emission is a streaming k-way merge, not a materialize-then-sort, and it is
//! columnar end to end (ADR-0099 decision 6). Each fetched series' SoA
//! (`FetchedSeriesSoa`'s `timestamps`/`values`, moved straight out of the
//! decoder) is kept as one merge run: the fetched vectors become
//! `ScalarBuffer`s without a copy, the runs are merged by a binary heap over
//! `(run, offset)` cursors, and `build_batch` gathers columns from those
//! cursors. No per-sample row struct is ever built. A partition's stream is
//! still globally sorted across all its batches, so the declared ordering
//! holds.
//!
//! Where a batch's rows all come from one run at consecutive ascending offsets
//! -- one series covered by one run, the common shape -- the `ts` and `value`
//! columns are slices of that run's buffers, shared with the run rather than
//! copied. Any other batch (rows straddling runs, or interleaved by
//! timestamp) gathers its values through the cursors. The two paths are
//! counted as the `adopted_batches`/`gathered_batches` metrics, so
//! `EXPLAIN ANALYZE` and a test can see which one ran instead of inferring it
//! from output that is identical by construction.
//!
//! The four provenance columns are each sample's ADR-0010 §5 dedup key, the
//! same key the PromQL merge resolves duplicates by. For an ordinary run the
//! three columns `created_unix_ns`, `writer_epoch`, and `writer_seq` are
//! constant within the run, so they are filled by run length, and
//! `in_page_index` is the sample's position in the fetcher's on-disk order.
//! A run that merged several writes' samples carries an explicit per-sample
//! key column instead (`FetchedSeriesSoa::per_sample_priorities`, ADR-0092
//! decision 1), because merging destroys what the run-wide triple and position
//! said; all four columns are read from it per row.
//!
//! Pushdown: label/series matchers are threaded into `fetch_soa` so the fetcher prunes
//! series (and their page GETs) against SERIES_TABLE. A `series_id` allow-set
//! is applied as a post-fetch row filter, because the fetcher's matcher API is
//! label-only; segment-level ts pruning happens one level up in the provider.
//! Every prune is widen-only (see crate::pushdown).
//!
//! Memory: each partition registers a
//! `MemoryConsumer` against the `TaskContext`'s pool and grows one
//! `MemoryReservation` in two phases:
//!
//! - Fetch/decode phase (`prepare_partition`): after each segment's
//!   `fetch_soa` call returns, the reservation grows by that segment's
//!   decoded SoA bytes -- the `timestamps` and `values` buffers the merge
//!   keeps as its runs, plus `per_sample_priorities` when the fetched series
//!   carries that column -- plus `FetchStats::raw_f64_bytes` (the one
//!   fetched-buffer byte figure `SegmentFetcher` already exposes without a
//!   ravel-query API change -- see the crate module doc's context-discipline
//!   note). A `try_grow` failure here returns before the next segment is
//!   fetched at all. This is the same live-bytes contract the pre-columnar
//!   scan charged as `rows * size_of::<ScanRow>()`, measured on the buffers
//!   that now exist instead of on a row struct that no longer does
//!   (ADR-0099 decision 6).
//! - Batch phase (`ScanStream::poll_next`, `ScanState::Merging`): a per-batch
//!   grow by the bytes that batch's own Arrow arrays allocate.
//!
//! These two phases charge disjoint, simultaneously-live allocations, not the
//! same bytes twice: the decoded SoA runs stay allocated in `Merger` for the
//! whole partition (batches only advance a cursor into them, they never free
//! the run), while a built `RecordBatch`'s arrays are separate Arrow
//! allocations. The one place those two would overlap is an adopted batch's
//! `ts`/`value` columns, which are slices of a run's buffers and allocate
//! nothing: the batch charge therefore sums the batch's columns and skips
//! exactly those two on the adoption path, because the fetch/decode phase has
//! already charged those bytes and charging them again per batch would count
//! one buffer once per batch that reads it. Every other column, on either
//! path, is freshly allocated and charged in full. What the reservation
//! tracks is concurrently-held scan memory, never cumulative output; the
//! per-query and per-tenant ceilings it is checked against are in
//! docs/query-engine.md's "Budgets" section.
//!
//! Each partition's reservation is threaded through `prepare_partition` (the
//! fetch/decode phase owns it first) and back into `ScanStream` (the batch
//! phase continues growing the same reservation), so a dropped or cancelled
//! stream at any point frees the one reservation via `Drop` (the pool forwards
//! the shrink to the tenant accountant), and a query that outgrows its byte
//! budget fails with the pool's `ResourcesExhausted`.
//!
//! `max_series` is enforced the same way: `prepare_partition`
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
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_promql::LabelMatcher;
use ravel_query::erasure::{ErasurePredicate, retain_series_soa};
use ravel_query::{
    ByteLimit, FetchedSeriesSoa, PhaseAccounting, RequestLimit, SamplePriority, SegmentFetcher,
    request_budget_exceeded,
};
use ravel_types::{LabelSet, TenantHash};

use crate::error::SqlError;
use crate::labels::build_labels_dict;
use crate::schema::{COL_SERIES_ID, COL_TS, COL_VALUE, internal_schema};

/// Rows accumulated into one output batch before it is emitted. Bounds the
/// per-batch working set and gives the memory reservation a per-batch
/// granularity.
const BATCH_ROWS: usize = 8192;

/// The total sort key: `(series_id, ts, created_unix_ns, writer_epoch,
/// writer_seq, in_page_index)`.
type SortKey = ([u8; 16], i64, i64, u64, u64, u32);

/// Where one run's per-sample dedup keys come from, the same two shapes the
/// PromQL merge distinguishes: a run-wide triple plus the sample's on-disk
/// position, or an explicit per-sample column for a run that merged several
/// writes' samples (ADR-0092 decision 1), where position no longer
/// reconstructs the key.
enum RunKeys {
    RunWide {
        created_unix_ns: i64,
        writer_epoch: u64,
        writer_seq: u64,
        /// Per-sample in-page index, when it is not the offset itself. `None`
        /// -- every run a segment in on-disk order produces -- means offset
        /// `i` has in-page index `i`. `Some` carries the original positions of
        /// a run whose timestamps did not arrive key-ascending and had to be
        /// reordered here.
        in_page: Option<Vec<u32>>,
    },
    /// `FetchedSeriesSoa::per_sample_priorities`, reordered with the run so
    /// it stays parallel to `ts`/`values`.
    PerSample(Vec<SamplePriority>),
}

/// One fetched series' samples from one segment, kept in the SoA form the
/// fetcher decoded them into: one merge run, ordered by [`SortKey`].
///
/// `series_id` is constant across the run. A run-wide run's provenance is
/// constant too, so its ordering is decided by `ts` then in-page index alone;
/// a per-sample run orders equal timestamps by each sample's own key.
struct Run {
    series_id: [u8; 16],
    /// Sample timestamps, key-ascending; `values[i]` is `ts[i]`'s sample.
    /// Adopted from `FetchedSeriesSoa::timestamps` without a copy.
    ts: ScalarBuffer<i64>,
    values: ScalarBuffer<f64>,
    keys: RunKeys,
}

impl Run {
    /// Build a run from one fetched series, reordering only if the fetched
    /// samples are not already key-ascending.
    ///
    /// A segment stores a run's samples in ascending ts order with ties in
    /// insertion order (docs/segment-format.md, "Sample order within a
    /// page"), which is exactly [`SortKey`]'s order inside a run-wide run, so
    /// the common path moves the fetched vectors into buffers untouched. The
    /// fetcher can still concatenate several runs of one series from one L0
    /// object into a single SoA, and that concatenation is not ordered; a
    /// run-merged run's per-sample keys need not ascend within a timestamp
    /// either. The merge and the scan's declared ordering both require a
    /// sorted run, so such a run is stable-sorted here: a run-wide run keeps
    /// its original positions as in-page indices, a per-sample run carries
    /// its key column along.
    ///
    /// A per-sample column that is not parallel to the run's timestamps is
    /// rejected, as the PromQL merge rejects it, rather than truncated.
    fn from_soa(series_id: [u8; 16], fs: FetchedSeriesSoa) -> Result<Run, SqlError> {
        let FetchedSeriesSoa {
            mut timestamps,
            mut values,
            created_unix_ns,
            writer_epoch,
            writer_seq,
            per_sample_priorities,
            ..
        } = fs;
        if let Some(column) = &per_sample_priorities
            && column.len() != timestamps.len()
        {
            return Err(SqlError::RunInvariant(
                ravel_query::QueryError::PrioritySampleCountMismatch {
                    priorities: column.len(),
                    samples: timestamps.len(),
                },
            ));
        }
        // A series whose two SoA vectors disagree in length carries no
        // sample past the shorter of the two, the same truncation the
        // zip over them did before this path was columnar.
        let n = timestamps.len().min(values.len());
        timestamps.truncate(n);
        values.truncate(n);

        let Some(mut column) = per_sample_priorities else {
            if timestamps.windows(2).all(|w| w[0] <= w[1]) {
                return Ok(Run {
                    series_id,
                    ts: timestamps.into(),
                    values: values.into(),
                    keys: RunKeys::RunWide {
                        created_unix_ns,
                        writer_epoch,
                        writer_seq,
                        in_page: None,
                    },
                });
            }
            let mut order: Vec<usize> = (0..timestamps.len()).collect();
            order.sort_by_key(|&i| timestamps[i]);
            return Ok(Run {
                series_id,
                ts: gather(&timestamps, &order).into(),
                values: gather(&values, &order).into(),
                keys: RunKeys::RunWide {
                    created_unix_ns,
                    writer_epoch,
                    writer_seq,
                    in_page: Some(
                        order
                            .iter()
                            .map(|&i| u32::try_from(i).unwrap_or(u32::MAX))
                            .collect(),
                    ),
                },
            });
        };
        column.truncate(n);
        let key = |i: usize| (timestamps[i], column[i].as_tuple());
        if (1..n).all(|i| key(i - 1) <= key(i)) {
            return Ok(Run {
                series_id,
                ts: timestamps.into(),
                values: values.into(),
                keys: RunKeys::PerSample(column),
            });
        }
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| key(i));
        Ok(Run {
            series_id,
            ts: gather(&timestamps, &order).into(),
            values: gather(&values, &order).into(),
            keys: RunKeys::PerSample(gather(&column, &order)),
        })
    }

    fn len(&self) -> usize {
        self.ts.len()
    }

    /// The `(created_unix_ns, writer_epoch, writer_seq, in_page_index)` dedup
    /// key of the sample at `offset`. A run-wide run's in-page index is the
    /// sample's position in the fetcher's on-disk order.
    fn priority_at(&self, offset: usize) -> (i64, u64, u64, u32) {
        match &self.keys {
            RunKeys::RunWide {
                created_unix_ns,
                writer_epoch,
                writer_seq,
                in_page,
            } => {
                let in_page = match in_page {
                    // Unreachable while `from_soa` keeps the indices parallel
                    // to the samples; a sample with no index must lose every
                    // duplicate in its run, as a keyless per-sample one does.
                    Some(idx) => idx.get(offset).copied().unwrap_or(0),
                    None => u32::try_from(offset).unwrap_or(u32::MAX),
                };
                (*created_unix_ns, *writer_epoch, *writer_seq, in_page)
            }
            // Unreachable while `from_soa` keeps the column parallel to the
            // samples; a sample with no key must lose every duplicate.
            RunKeys::PerSample(column) => column
                .get(offset)
                .map_or((i64::MIN, 0, 0, 0), |p| p.as_tuple()),
        }
    }

    /// The full sort key of the sample at `offset`. Callers hold an offset
    /// below `len()`.
    fn key_at(&self, offset: usize) -> SortKey {
        let (created, epoch, seq, in_page) = self.priority_at(offset);
        (
            self.series_id,
            self.ts[offset],
            created,
            epoch,
            seq,
            in_page,
        )
    }

    /// Bytes this run holds: the two adopted sample buffers, plus the
    /// explicit in-page indices or per-sample key column when it has them.
    fn soa_bytes(&self) -> usize {
        let keys = match &self.keys {
            RunKeys::RunWide { in_page, .. } => in_page.as_ref().map_or(0, |idx| {
                idx.len().saturating_mul(std::mem::size_of::<u32>())
            }),
            RunKeys::PerSample(column) => column
                .len()
                .saturating_mul(std::mem::size_of::<SamplePriority>()),
        };
        self.ts
            .len()
            .saturating_mul(std::mem::size_of::<i64>())
            .saturating_add(self.values.len().saturating_mul(std::mem::size_of::<f64>()))
            .saturating_add(keys)
    }
}

/// `src` permuted by `order`.
fn gather<T: Copy>(src: &[T], order: &[usize]) -> Vec<T> {
    order.iter().map(|&i| src[i]).collect()
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
    /// Per-partition distinct-series_id budget; see the module
    /// doc for why this is per-partition, not a cross-partition total.
    max_series: usize,
    /// Per-tenant bytes-scanned budget (ADR-0061 decision 1),
    /// checked once per completed segment fetch against the running pooled
    /// (`PhaseAccountingSnapshot::pooled`) total. `Unlimited` never trips, so
    /// a caller that does not opt in behaves exactly as before this budget
    /// existed.
    max_bytes_scanned: ByteLimit,
    /// Per-tenant S3 request budget (ADR-0073 decision 4),
    /// checked once per completed segment fetch against the running pooled
    /// total, the same checkpoint as `max_bytes_scanned`. Mirrors
    /// `ravel_query::engine`'s PromQL enforcement so both query languages
    /// trip the same budget the same way.
    max_s3_requests: RequestLimit,
    /// Pending selective-erasure predicates from the resolved snapshot
    /// (ADR-0064 decision 2). Applied to each segment's decoded
    /// `FetchedSeriesSoa` series via [`retain_series_soa`] immediately after
    /// `fetch_soa_accounted` returns -- after fetch, after the ADR-0046 read
    /// cache that fetch routes through, before any row reaches DataFusion.
    erasure: Arc<Vec<ErasurePredicate>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's phase-split accounting handle (ADR-0044, issue #796),
    /// cloned into every partition's `fetch_soa_phase_accounted` call.
    phase_accounting: PhaseAccounting,
    /// Per-partition batch-path counters (ADR-0099 decision 6), published so
    /// `EXPLAIN ANALYZE` and tests can see which batch-building path ran.
    metrics: ExecutionPlanMetricsSet,
}

/// The per-partition batch-path counters this scan publishes as DataFusion
/// metrics. A batch is `adopted` when its `ts`/`value` columns are slices of
/// one run's buffers and `gathered` when its values were copied through the
/// merge cursors; every emitted batch increments exactly one of the two.
struct ScanMetrics {
    adopted: Count,
    gathered: Count,
}

impl ScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        ScanMetrics {
            adopted: MetricBuilder::new(metrics).counter("adopted_batches", partition),
            gathered: MetricBuilder::new(metrics).counter("gathered_batches", partition),
        }
    }
}

/// The DataFusion counter name this scan publishes for the histogram-kind
/// series its fetch dropped (issue #1738). Read back off the executed plan by
/// `crate::executor`, which turns a nonzero total into the response warning
/// that says the answer excludes them; a `samples` result with no warning
/// beside it is otherwise indistinguishable from a complete one.
pub(crate) const HISTOGRAM_SERIES_SKIPPED_METRIC: &str = "histogram_series_skipped";

impl RsegScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given
    /// pushdown matchers, optional `series_id` allow-set, per-partition
    /// `max_series` budget, and per-tenant `max_bytes_scanned`
    /// budget (ADR-0061 decision 1).
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
        max_s3_requests: RequestLimit,
        erasure: Arc<Vec<ErasurePredicate>>,
        phase_accounting: PhaseAccounting,
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
            max_s3_requests,
            erasure,
            schema,
            properties,
            phase_accounting,
            metrics: ExecutionPlanMetricsSet::new(),
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
        let erasure = Arc::clone(&self.erasure);
        let schema = Arc::clone(&self.schema);

        // One reservation per partition stream, registered against whatever
        // pool the TaskContext carries (the tenant-delegating pool in
        // production, an unbounded pool under bare `collect`). Owned by the
        // stream, so dropping the stream frees it and the pool forwards the
        // shrink to the tenant accountant.
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
            self.max_s3_requests,
            erasure,
            reservation,
            self.phase_accounting.clone(),
            MetricBuilder::new(&self.metrics).counter(HISTOGRAM_SERIES_SKIPPED_METRIC, partition),
        ));
        Ok(Box::pin(ScanStream {
            schema,
            state: ScanState::Fetching(fut),
            metrics: ScanMetrics::new(&self.metrics, partition),
        }))
    }
}

/// Decoded, sorted SoA runs -- one per (segment, fetched series) -- plus the
/// per-series label sets, ready to merge. Built by [`prepare_partition`].
struct Prepared {
    runs: Vec<Run>,
    labels: HashMap<[u8; 16], LabelSet>,
}

/// Fetch every segment in this partition (matchers pushed into the fetcher),
/// keep each fetched series' SoA as one sorted run, and collect per-series
/// labels. Applies the `series_id` allow-set as a post-fetch row filter.
///
/// Enforces four budgets before the next segment is ever fetched: the
/// per-tenant bytes-scanned budget against the running pooled
/// total (ADR-0061 decision 1), the per-tenant S3 request budget
/// against the same running pooled total (ADR-0073 decision 4),
/// the distinct-series count against `max_series`, and the
/// reservation's byte budget against this segment's decoded size.
/// `reservation` is threaded through and returned so the caller's
/// batch phase continues growing the same one (see module doc).
#[allow(clippy::too_many_arguments)]
async fn prepare_partition(
    fetcher: SegmentFetcher,
    tenant: TenantHash,
    segs: Vec<SegmentRef>,
    matchers: Arc<Vec<LabelMatcher>>,
    series_ids: Option<Arc<HashSet<[u8; 16]>>>,
    max_series: usize,
    max_bytes_scanned: ByteLimit,
    max_s3_requests: RequestLimit,
    erasure: Arc<Vec<ErasurePredicate>>,
    reservation: MemoryReservation,
    phase_accounting: PhaseAccounting,
    histogram_series_skipped: Count,
) -> DFResult<(Prepared, MemoryReservation)> {
    let mut runs: Vec<Run> = Vec::with_capacity(segs.len());
    let mut labels: HashMap<[u8; 16], LabelSet> = HashMap::new();

    for seg in &segs {
        let (mut series, stats) = fetcher
            .fetch_soa_phase_accounted(tenant, seg, &matchers, &phase_accounting)
            .await
            .map_err(SqlError::from)?;
        // Histogram-kind series this segment matched and the scalar-only fetch
        // above did not return (issue #1738). Counted before any budget check
        // or erasure filter, so a statement that trips a budget later still
        // reports what it had already excluded by table shape.
        histogram_series_skipped
            .add(usize::try_from(stats.histogram_series_skipped).unwrap_or(usize::MAX));
        // Selective-erasure exclusion (ADR-0064 decision 2):
        // applied to the decoded series immediately after fetch, after the
        // ADR-0046 read cache `fetch_soa_phase_accounted` routes through,
        // before any row below reaches DataFusion. A no-op when `erasure` is
        // empty.
        retain_series_soa(&mut series, &erasure);
        // Per-tenant bytes-scanned budget (ADR-0061 decision 1): this fetch
        // has just charged its S3 bytes into the shared `phase_accounting`
        // handle, so check the running pooled total against the tenant's cap
        // here, once per completed segment fetch, before decoding this
        // segment or fetching the next one. This loop is genuinely
        // sequential, so a trip here straightforwardly means the remaining
        // segments' GETs never happen.
        //
        // `pooled_snapshot()`, not `snapshot().pooled()`: this handle can be
        // `PhaseAccounting::pooled_over` a single shared `QueryAccounting`
        // (see `executor.rs`'s `plan_pinned`/`plan_pinned_distributed`/
        // `worker_fragment_stream`), whose four phases are clones of one
        // counter. Summing four snapshots of that one counter would report
        // 4x the real total.
        let scanned = phase_accounting.pooled_snapshot().total_s3_bytes();
        if max_bytes_scanned.is_exceeded_by(scanned) {
            let max = match max_bytes_scanned {
                ByteLimit::Bounded(max) => max,
                ByteLimit::Unlimited => scanned,
            };
            return Err(SqlError::TooManyBytesScanned { scanned, max }.into());
        }
        // Per-tenant S3 request budget (ADR-0073 decision
        // 4): same checkpoint as the bytes-scanned budget above, so a trip
        // here also means the remaining segments' GETs never happen. Mirrors
        // `ravel_query::engine`'s PromQL enforcement exactly. Same aliasing
        // hazard as the bytes-scanned check above, so `pooled_snapshot()`
        // here too.
        // `fold_lag` is whatever the checked budget carried. This scan's
        // budget is a bare `RequestLimit` built from the session config, which
        // carries none, so a trip here reads as a plain budget refusal; the
        // resolve-boundary check in `executor.rs`, which does hold the
        // resolve's origins, is where a SQL refusal names fold lag (ADR-1306
        // decision 6).
        if let Some(ravel_query::QueryError::RequestBudgetExceeded {
            requests,
            max,
            fold_lag,
        }) = request_budget_exceeded(
            phase_accounting.pooled_snapshot().total_s3_requests(),
            max_s3_requests,
        ) {
            return Err(SqlError::RequestBudgetExceeded {
                requests,
                max,
                fold_lag,
            }
            .into());
        }
        // The SoA bytes this segment contributes to the merge, plus the
        // per-sample priority column when the fetched series carries one:
        // both are live at this point, and the buffers stay live in `Merger`
        // for the whole partition.
        let mut segment_bytes = usize::try_from(stats.raw_f64_bytes).unwrap_or(usize::MAX);
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
            let run = Run::from_soa(sid, fs)?;
            segment_bytes = segment_bytes.saturating_add(run.soa_bytes());
            if run.len() > 0 {
                runs.push(run);
            }
        }
        // Charge this segment's decoded SoA plus its fetched buffer bytes
        // before fetching the next segment: a byte-budget overrun surfaces
        // here as the pool's typed ResourcesExhausted, not after every
        // segment in the partition has already been pulled.
        reservation.try_grow(segment_bytes)?;
    }

    Ok((Prepared { runs, labels }, reservation))
}

/// Head-of-run entry in the merge heap: the next row's key and its run index.
/// `Reverse` turns the max-heap into a min-heap on the key.
type HeapEntry = Reverse<(SortKey, usize)>;

/// One merged row: the run it came from and its offset inside that run. The
/// merge yields these instead of copying a sample out of its run.
type Cursor = (usize, usize);

/// Streaming k-way merge over the sorted SoA runs.
struct Merger {
    runs: Vec<Run>,
    /// Next unpopped offset in each run.
    cursors: Vec<usize>,
    heap: BinaryHeap<HeapEntry>,
    labels: HashMap<[u8; 16], LabelSet>,
    /// The current batch's cursors, in emitted order. Reused across batches,
    /// so a batch costs no cursor allocation.
    batch: Vec<Cursor>,
}

impl Merger {
    fn new(prepared: Prepared) -> Self {
        let Prepared { runs, labels } = prepared;
        let cursors = vec![0usize; runs.len()];
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (idx, run) in runs.iter().enumerate() {
            if run.len() > 0 {
                heap.push(Reverse((run.key_at(0), idx)));
            }
        }
        Merger {
            runs,
            cursors,
            heap,
            labels,
            batch: Vec::with_capacity(BATCH_ROWS),
        }
    }

    /// Advance the merge by up to `n` rows in global sort order, into
    /// `self.batch`. Returns the number of rows it holds.
    fn fill(&mut self, n: usize) -> usize {
        self.batch.clear();
        while self.batch.len() < n {
            let Some(Reverse((_key, idx))) = self.heap.pop() else {
                break;
            };
            let offset = self.cursors[idx];
            self.batch.push((idx, offset));
            let next = offset + 1;
            self.cursors[idx] = next;
            if next < self.runs[idx].len() {
                self.heap.push(Reverse((self.runs[idx].key_at(next), idx)));
            }
        }
        self.batch.len()
    }

    /// The run and start offset of the current batch when every one of its
    /// rows comes from that one run at consecutive ascending offsets, so the
    /// `ts`/`value` columns can be slices of the run's buffers rather than
    /// gathered copies. `None` means the batch straddles runs or interleaves
    /// them, and its values are gathered.
    fn contiguous_run(&self) -> Option<(usize, usize)> {
        let &(run, start) = self.batch.first()?;
        for (i, &(r, offset)) in self.batch.iter().enumerate() {
            if r != run || offset != start + i {
                return None;
            }
        }
        Some((run, start))
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
    metrics: ScanMetrics,
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
                    if merger.fill(BATCH_ROWS) == 0 {
                        this.state = ScanState::Done;
                        return Poll::Ready(None);
                    }
                    let (batch, batch_bytes) =
                        match build_batch(merger, Arc::clone(&this.schema), &this.metrics) {
                            Ok(b) => b,
                            Err(e) => {
                                this.state = ScanState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                        };
                    // Grow the reservation by the bytes this batch's own
                    // arrays allocate (see the module doc: an adopted
                    // ts/value column allocates none, the fetch/decode phase
                    // already charged it). A byte-budget overrun surfaces
                    // here as the pool's typed ResourcesExhausted error,
                    // never a silent partial result.
                    if let Err(e) = reservation.try_grow(batch_bytes) {
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

/// Build one `RecordBatch` from the merge's current batch of cursors, and
/// return it with the bytes its own arrays allocate (the batch-phase
/// reservation charge; see the module doc).
///
/// `ts` and `value` are slices of one run's buffers when the batch is
/// contiguous inside that run, and gathered copies otherwise. A run-wide run's
/// three constant provenance columns are filled by run length either way; a
/// per-sample run's are read per row from its key column. `in_page_index`,
/// `series_id`, and the labels dictionary key are per row.
fn build_batch(
    merger: &Merger,
    schema: SchemaRef,
    metrics: &ScanMetrics,
) -> DFResult<(RecordBatch, usize)> {
    let cursors = &merger.batch;
    let rows = cursors.len();
    let adopted = merger.contiguous_run();

    let (ts, value): (ArrayRef, ArrayRef) = match adopted {
        Some((run, start)) => {
            metrics.adopted.add(1);
            let r = &merger.runs[run];
            (
                Arc::new(TimestampNanosecondArray::new(r.ts.slice(start, rows), None)),
                Arc::new(Float64Array::new(r.values.slice(start, rows), None)),
            )
        }
        None => {
            metrics.gathered.add(1);
            let mut ts: Vec<i64> = Vec::with_capacity(rows);
            let mut value: Vec<f64> = Vec::with_capacity(rows);
            for &(run, offset) in cursors {
                let r = &merger.runs[run];
                ts.push(r.ts[offset]);
                value.push(r.values[offset]);
            }
            (
                Arc::new(TimestampNanosecondArray::from(ts)),
                Arc::new(Float64Array::from(value)),
            )
        }
    };

    let series_id = FixedSizeBinaryArray::try_from_iter(
        cursors.iter().map(|&(run, _)| merger.runs[run].series_id),
    )
    .map_err(|e| SqlError::Internal(format!("series_id array build: {e}")))?;

    let mut created: Vec<i64> = Vec::with_capacity(rows);
    let mut epoch: Vec<u64> = Vec::with_capacity(rows);
    let mut seq: Vec<u64> = Vec::with_capacity(rows);
    let mut in_page: Vec<u32> = Vec::with_capacity(rows);
    // Dictionary-encode labels: one dictionary entry per distinct series in
    // this batch, an Int32 key per row. Rows are globally sorted by series_id,
    // so distinct series appear in contiguous runs (a series split across two
    // batches simply appears in each batch's dictionary), and two runs of the
    // same series that meet inside a batch share its one entry.
    let mut distinct: Vec<LabelSet> = Vec::new();
    let mut keys: Vec<i32> = Vec::with_capacity(rows);
    let mut last_series: Option<[u8; 16]> = None;
    let mut key: i32 = -1;

    // Walk the batch one run-length at a time: the provenance a run-wide run
    // stamps on every one of its samples is written per run, never per row.
    let mut i = 0;
    while i < rows {
        let (run, _) = cursors[i];
        let mut end = i + 1;
        while end < rows && cursors[end].0 == run {
            end += 1;
        }
        let len = end - i;
        let r = &merger.runs[run];
        match &r.keys {
            RunKeys::RunWide {
                created_unix_ns,
                writer_epoch,
                writer_seq,
                ..
            } => {
                created.extend(std::iter::repeat_n(*created_unix_ns, len));
                epoch.extend(std::iter::repeat_n(*writer_epoch, len));
                seq.extend(std::iter::repeat_n(*writer_seq, len));
                for &(_, offset) in &cursors[i..end] {
                    in_page.push(r.priority_at(offset).3);
                }
            }
            RunKeys::PerSample(_) => {
                for &(_, offset) in &cursors[i..end] {
                    let (c, e, s, p) = r.priority_at(offset);
                    created.push(c);
                    epoch.push(e);
                    seq.push(s);
                    in_page.push(p);
                }
            }
        }
        if last_series != Some(r.series_id) {
            distinct.push(merger.labels.get(&r.series_id).cloned().unwrap_or_default());
            key += 1;
            last_series = Some(r.series_id);
        }
        keys.extend(std::iter::repeat_n(key, len));
        i = end;
    }
    let labels = build_labels_dict(&distinct, &keys).map_err(DataFusionError::from)?;

    let columns: Vec<ArrayRef> = vec![
        ts,
        value,
        Arc::new(series_id),
        labels,
        Arc::new(Int64Array::from(created)),
        Arc::new(UInt64Array::from(epoch)),
        Arc::new(UInt64Array::from(seq)),
        Arc::new(UInt32Array::from(in_page)),
    ];
    debug_assert_eq!(schema.fields().len(), columns.len());
    debug_assert_eq!(COL_TS, 0);
    debug_assert_eq!(COL_SERIES_ID, 2);

    // The batch-phase charge: every column this batch allocated. An adopted
    // ts/value column is a slice of a run's buffer, already charged by the
    // fetch/decode phase, so it is skipped here rather than counted once per
    // batch that reads it.
    let mut batch_bytes = 0usize;
    for (i, column) in columns.iter().enumerate() {
        if adopted.is_some() && (i == COL_TS || i == COL_VALUE) {
            continue;
        }
        batch_bytes = batch_bytes.saturating_add(column.get_array_memory_size());
    }

    let batch = RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)?;
    Ok((batch, batch_bytes))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array, UInt32Array, UInt64Array};
    use ravel_types::SeriesId;

    use super::*;
    use crate::error::{ErrorClass, MSG_UNAVAILABLE};
    use crate::schema::{COL_CREATED_UNIX_NS, COL_IN_PAGE_INDEX, COL_WRITER_EPOCH, COL_WRITER_SEQ};

    fn priority(created: i64, epoch: u64, seq: u64, in_page: u32) -> SamplePriority {
        SamplePriority {
            created_unix_ns: created,
            writer_epoch: epoch,
            writer_seq: seq,
            in_page_index: in_page,
        }
    }

    fn soa(
        sid: u8,
        timestamps: Vec<i64>,
        values: Vec<f64>,
        column: Option<Vec<SamplePriority>>,
    ) -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([sid; 16]),
            labels: LabelSet::default(),
            timestamps,
            values,
            created_unix_ns: 1,
            writer_epoch: 1,
            writer_seq: 1,
            per_sample_priorities: column,
        }
    }

    /// `(created_unix_ns, writer_epoch, writer_seq, in_page_index)`.
    type Priority = (i64, u64, u64, u32);

    fn mismatch(priorities: usize, samples: usize) -> String {
        SqlError::RunInvariant(ravel_query::QueryError::PrioritySampleCountMismatch {
            priorities,
            samples,
        })
        .to_string()
    }

    /// A per-sample column with fewer entries than the run has samples is
    /// the typed mismatch, not an index past the column's end.
    #[test]
    fn from_soa_rejects_a_column_shorter_than_the_samples() {
        let fs = soa(
            1,
            vec![10, 10, 20],
            vec![1.0, 2.0, 3.0],
            Some(vec![priority(5, 1, 1, 0), priority(5, 1, 2, 0)]),
        );
        match Run::from_soa([1; 16], fs) {
            Err(err) => {
                assert_eq!(err.class(), ErrorClass::Unavailable, "{err}");
                assert_eq!(err.client_message(), MSG_UNAVAILABLE, "{err}");
                assert_eq!(err.to_string(), mismatch(2, 3));
            }
            Ok(run) => panic!("short column accepted, run of {}", run.len()),
        }
    }

    /// A per-sample column with more entries than the run has samples is
    /// the typed mismatch, not silently cut down to the samples.
    #[test]
    fn from_soa_rejects_a_column_longer_than_the_samples() {
        let fs = soa(
            1,
            vec![10, 20],
            vec![1.0, 2.0],
            Some(vec![
                priority(5, 1, 1, 0),
                priority(5, 1, 1, 1),
                priority(5, 1, 1, 2),
            ]),
        );
        match Run::from_soa([1; 16], fs) {
            Err(err) => {
                assert_eq!(err.class(), ErrorClass::Unavailable, "{err}");
                assert_eq!(err.client_message(), MSG_UNAVAILABLE, "{err}");
                assert_eq!(err.to_string(), mismatch(3, 2));
            }
            Ok(run) => panic!("long column accepted, run of {}", run.len()),
        }
    }

    /// A per-sample run with no key at an offset ranks that sample below
    /// every real key, so it loses any duplicate rather than winning it.
    #[test]
    fn a_missing_per_sample_key_ranks_least() {
        let run = Run {
            series_id: [1; 16],
            ts: vec![10, 10].into(),
            values: vec![1.0, 2.0].into(),
            keys: RunKeys::PerSample(vec![priority(5, 1, 1, 0)]),
        };
        assert_eq!(run.priority_at(1), (i64::MIN, 0, 0, 0));
        assert!(run.priority_at(1) < run.priority_at(0));
    }

    /// A run-wide run with no in-page index at an offset ranks that sample
    /// below every indexed sample of the run, the same direction a missing
    /// per-sample key fails in.
    #[test]
    fn a_missing_run_wide_in_page_index_ranks_least() {
        let run = Run {
            series_id: [1; 16],
            ts: vec![10, 10].into(),
            values: vec![1.0, 2.0].into(),
            keys: RunKeys::RunWide {
                created_unix_ns: 5,
                writer_epoch: 1,
                writer_seq: 1,
                in_page: Some(vec![1]),
            },
        };
        assert_eq!(run.priority_at(1), (5, 1, 1, 0));
        assert!(run.priority_at(1) < run.priority_at(0));
    }

    /// The provenance a scan batch emits never decreases inside one
    /// `(series, ts)` group, for a per-sample run whose column arrives out
    /// of key order and for a run-wide run of the same series merged with
    /// it. Each row's emitted key is also the one its column holds for its
    /// value, so the reorder moved the key with its sample.
    #[test]
    fn emitted_keys_never_decrease_within_a_series_ts_group() {
        let per_sample = soa(
            7,
            vec![10, 10, 10, 20, 20],
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            Some(vec![
                priority(300, 2, 1, 0),
                priority(300, 1, 9, 0),
                priority(100, 5, 5, 0),
                priority(200, 1, 1, 1),
                priority(200, 1, 1, 0),
            ]),
        );
        let run_wide = soa(7, vec![10, 20], vec![6.0, 7.0], None);
        let runs = vec![
            Run::from_soa([7; 16], per_sample).expect("parallel column"),
            Run::from_soa([7; 16], run_wide).expect("run-wide run"),
        ];
        let mut merger = Merger::new(Prepared {
            runs,
            labels: HashMap::new(),
        });
        assert_eq!(merger.fill(BATCH_ROWS), 7);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = ScanMetrics::new(&metrics_set, 0);
        let (batch, _) = build_batch(&merger, internal_schema(), &metrics).expect("build batch");

        fn column<T: 'static>(batch: &RecordBatch, idx: usize) -> &T {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<T>()
                .expect("internal schema column type")
        }
        let ts = column::<TimestampNanosecondArray>(&batch, COL_TS);
        let value = column::<Float64Array>(&batch, COL_VALUE);
        let created = column::<Int64Array>(&batch, COL_CREATED_UNIX_NS);
        let epoch = column::<UInt64Array>(&batch, COL_WRITER_EPOCH);
        let seq = column::<UInt64Array>(&batch, COL_WRITER_SEQ);
        let in_page = column::<UInt32Array>(&batch, COL_IN_PAGE_INDEX);
        let rows: Vec<(i64, Priority, f64)> = (0..batch.num_rows())
            .map(|i| {
                (
                    ts.value(i),
                    (
                        created.value(i),
                        epoch.value(i),
                        seq.value(i),
                        in_page.value(i),
                    ),
                    value.value(i),
                )
            })
            .collect();
        for pair in rows.windows(2) {
            if pair[0].0 == pair[1].0 {
                assert!(
                    pair[0].1 <= pair[1].1,
                    "key decreased inside ts {}: {:?} then {:?}",
                    pair[0].0,
                    pair[0].1,
                    pair[1].1
                );
            }
        }
        assert_eq!(
            rows,
            vec![
                (10, (1, 1, 1, 0), 6.0),
                (10, (100, 5, 5, 0), 3.0),
                (10, (300, 1, 9, 0), 2.0),
                (10, (300, 2, 1, 0), 1.0),
                (20, (1, 1, 1, 1), 7.0),
                (20, (200, 1, 1, 0), 5.0),
                (20, (200, 1, 1, 1), 4.0),
            ]
        );
    }
}
