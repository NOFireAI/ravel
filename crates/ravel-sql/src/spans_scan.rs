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
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_query::erasure::{ErasurePredicate, is_erased_span};
use ravel_rspan::block::DecodedBlock;
use ravel_rspan::record::{
    COL_END_TS, COL_PARENT_SPAN_ID, COL_SPAN_ID, COL_START_TS, COL_STATUS_CODE, COL_STATUS_MESSAGE,
    COL_TRACE_ID,
};
use ravel_rspan::{BloomPredicate, COL_NAME, COL_SERVICE_NAME, SpanQuery};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::error::SqlError;
use crate::spans_fetcher::{SpanRow, SpanSegmentFetcher};
use crate::spans_schema::{
    SPAN_COL_ATTRS, SPAN_COL_DURATION_NS, SPAN_COL_END_TS, SPAN_COL_NAME, SPAN_COL_PARENT_SPAN_ID,
    SPAN_COL_SERVICE_NAME, SPAN_COL_SPAN_ID, SPAN_COL_START_TS, SPAN_COL_STATUS_CODE,
    SPAN_COL_STATUS_MESSAGE, SPAN_COL_TRACE_ID, SPAN_ID_WIDTH, TRACE_ID_WIDTH, spans_schema,
};

/// Rows accumulated into one output batch before it is emitted.
const BATCH_ROWS: usize = 8192;

/// Metric name for the count of batches emitted by the columnar fast path, so
/// `EXPLAIN ANALYZE` shows which path a partition ran (ADR-0110 decision 6).
const METRIC_COLUMNAR_BATCHES: &str = "columnar_batches";
/// Metric name for the count of batches emitted by the row path fallback.
const METRIC_ROWPATH_BATCHES: &str = "rowpath_batches";
/// Metric name for the column pages the decode decompressed, on either path.
const METRIC_PAGES_DECODED: &str = "pages_decoded";
/// Metric name for the column pages the decode walked past because the
/// projection excluded them: the page-level proof projection reached decode.
const METRIC_PAGES_SKIPPED: &str = "pages_skipped";

/// The query-shape half of the columnar fast-path eligibility rule (ADR-0110
/// decision 3), decided once at plan time by the provider and re-derived by the
/// scan. The fast path is taken only when this AND the per-block
/// `has_attrs_raw_page() == false` check both hold; otherwise the row path runs
/// unchanged.
///
/// Two clauses live here because they do not vary per block:
///
/// - **(a) the projection does not include the `attrs` map column.** `attrs`
///   ([`SPAN_COL_ATTRS`]) is the only column needing the dynamic per-key
///   columns, the `attrs_raw` overflow decode, and the `_events_raw`
///   reconstruction, which is precisely the work the fast path avoids. A
///   `None` (all columns) projection includes `attrs`, so it is ineligible.
/// - **(b) no pending selective-erasure predicate applies.** `is_erased_span`
///   matches against the merged attribute map, exactly the structure the fast
///   path never builds, so a scan carrying an erasure predicate drains the row
///   path. This clause fails closed on purpose: the failure mode of getting
///   erasure wrong is an erased span served to a client, not a slow query.
///   Erasure is a rare tenant state and columnar erasure evaluation is a
///   separate change (ADR-0110 decision 3).
pub(crate) fn columnar_static_eligible(
    projection: Option<&Vec<usize>>,
    erasure: &[ErasurePredicate],
) -> bool {
    erasure.is_empty() && projection.is_some_and(|p| !p.contains(&SPAN_COL_ATTRS))
}

/// The RSPAN column ids a columnar decode must materialize for `projection`
/// (ADR-0110 decision 3): the union of the projected columns, the ordering key
/// columns [`COL_TRACE_ID`]/[`COL_START_TS`] (needed for the stable interleave
/// even when the query projects neither), and the per-row predicate columns
/// (those two plus [`COL_END_TS`]). So `SELECT name` still decodes trace_id and
/// start_ts; dropping them because the projection omits them would break the
/// advertised `(trace_id asc, start_ts asc)` order.
fn union_projected_columns(projection: &[usize]) -> Vec<u32> {
    // Ordering + predicate columns are always decoded.
    let mut cols = vec![COL_TRACE_ID, COL_START_TS, COL_END_TS];
    let mut push = |c: u32| {
        if !cols.contains(&c) {
            cols.push(c);
        }
    };
    for &idx in projection {
        match idx {
            SPAN_COL_TRACE_ID => push(COL_TRACE_ID),
            SPAN_COL_SPAN_ID => push(COL_SPAN_ID),
            SPAN_COL_PARENT_SPAN_ID => push(COL_PARENT_SPAN_ID),
            SPAN_COL_NAME => push(COL_NAME),
            SPAN_COL_START_TS => push(COL_START_TS),
            SPAN_COL_END_TS => push(COL_END_TS),
            SPAN_COL_STATUS_CODE => push(COL_STATUS_CODE),
            SPAN_COL_STATUS_MESSAGE => push(COL_STATUS_MESSAGE),
            SPAN_COL_SERVICE_NAME => push(COL_SERVICE_NAME),
            // duration_ns is computed from start_ts/end_ts, both already in the
            // set above; the `attrs` map is ruled out by eligibility, so no
            // other index reaches here.
            _ => {}
        }
    }
    cols
}

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
    /// The pushed-down column projection (ADR-0110 decision 4). `Some` for the
    /// eligible fast path: the scan emits `schema` (already the projected
    /// schema) and the provider adds no `ProjectionExec`. `None` reproduces the
    /// pre-ADR-0110 behavior: the full eleven-column schema, with the provider
    /// wrapping a `ProjectionExec` above the scan for any column selection.
    projection: Option<Arc<Vec<usize>>>,
    /// Whether this scan may attempt the columnar fast path: the query-shape
    /// clauses of [`columnar_static_eligible`] (projection excludes `attrs`, no
    /// pending erasure). The remaining per-block clause (no `attrs_raw` overflow
    /// page) is checked as each block is decoded, in [`prepare_partition`].
    columnar_eligible: bool,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), threaded into every
    /// per-partition fetch so span fetches are recorded like every other
    /// funnel, mirroring `LogsScanExec`.
    accounting: QueryAccounting,
    /// Partition metrics (ADR-0110 decision 5): `columnar_batches`/
    /// `rowpath_batches` show which path a partition ran, and
    /// `pages_decoded`/`pages_skipped` show projection reaching decode, so
    /// `EXPLAIN ANALYZE` and a test can assert eligibility directly. Both page
    /// counters are written on both paths (#669), so a zero always means the
    /// decode did that much and never that the arm forgot to count.
    metrics: ExecutionPlanMetricsSet,
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
        projection: Option<Vec<usize>>,
        accounting: QueryAccounting,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let full = spans_schema();
        // `Some(proj)` projects the schema so the scan emits the projected
        // shape directly (ADR-0110 decision 4); `None` keeps the full schema
        // and the provider's `ProjectionExec`.
        let (schema, projection): (SchemaRef, Option<Arc<Vec<usize>>>) = match projection {
            Some(p) => {
                for &i in &p {
                    if i >= full.fields().len() {
                        return Err(DataFusionError::Internal(format!(
                            "spans scan projection index {i} out of range"
                        )));
                    }
                }
                (Arc::new(full.project(&p)?), Some(Arc::new(p)))
            }
            None => (full, None),
        };
        let columnar_eligible = columnar_static_eligible(projection.as_deref(), &erasure);
        let properties = Arc::new(Self::compute_properties(&schema, n, projection.as_deref())?);
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
            projection,
            columnar_eligible,
            schema,
            properties,
            accounting,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The [`SpanQuery`] this scan issues. Exposed so tests can prove a
    /// `trace_id =` filter is compiled into a [`SpanQuery::trace`] fast-path
    /// lookup rather than a bare [`SpanQuery::ts_range`] scan.
    pub fn query(&self) -> SpanQuery {
        self.query
    }

    fn compute_properties(
        schema: &SchemaRef,
        n: usize,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<PlanProperties> {
        let asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Declared order is (trace_id asc, start_ts asc): the object's native
        // sort key (ADR-0041), preserved by this stage's stable sort. Under a
        // projection the scan still emits rows in that order, but DataFusion can
        // only be told about columns present in the output: advertise the
        // longest PREFIX of (trace_id, start_ts) the projection keeps. start_ts
        // is a secondary key, so it can be advertised only when trace_id is too;
        // dropping trace_id leaves the rows unsorted by start_ts alone. A `None`
        // projection keeps both.
        let has = |field: usize| projection.is_none_or(|p| p.contains(&field));
        let mut exprs = Vec::new();
        if has(SPAN_COL_TRACE_ID) {
            exprs.push(PhysicalSortExpr::new(col("trace_id", schema)?, asc));
            if has(SPAN_COL_START_TS) {
                exprs.push(PhysicalSortExpr::new(col("start_ts", schema)?, asc));
            }
        }
        let eq = match LexOrdering::new(exprs) {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering])
            }
            None => EquivalenceProperties::new(Arc::clone(schema)),
        };
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
            self.columnar_eligible,
            self.projection.clone(),
            self.accounting.clone(),
        ));
        Ok(Box::pin(SpanScanStream {
            schema,
            reservation,
            metrics: SpanScanMetrics::new(&self.metrics, partition),
            state: SpanScanState::Fetching(fut),
        }))
    }
}

/// The per-partition metrics this scan publishes (ADR-0110 decision 5). `Count`
/// is a shared handle, so a clone written by the stream updates the same counter
/// `SpansScanExec::metrics` exposes to `EXPLAIN ANALYZE`.
#[derive(Clone)]
struct SpanScanMetrics {
    /// Column pages this partition's decode decompressed, on whichever path it
    /// ran (#669): the columnar attempt's per-block counts, the row path's, or
    /// both when an `attrs_raw` block made the partition fall back after
    /// decoding some blocks columnar.
    pages_decoded: Count,
    /// Column pages this partition's decode walked past because the projection
    /// excluded them. A row-path partition reports 0 because it decodes every
    /// page of every block it scans, which is a measurement of that path, not an
    /// unwritten counter: `pages_decoded` is nonzero beside it.
    pages_skipped: Count,
    /// Output batches this partition built through the columnar fast path,
    /// straight from a [`ravel_rspan::ColumnarBlockView`] with no `SpanRecord`
    /// and no `SpanRow`. The two paths' output is identical by construction, so
    /// this and [`Self::rowpath_batches`] are the only externally visible proof
    /// of which path a query took.
    columnar_batches: Count,
    /// Output batches this partition built through the row path: an ineligible
    /// query (`attrs` projected, a pending erasure predicate) or an eligible one
    /// that hit a block carrying an `attrs_raw` overflow page.
    rowpath_batches: Count,
}

impl SpanScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        SpanScanMetrics {
            pages_decoded: MetricBuilder::new(metrics).counter(METRIC_PAGES_DECODED, partition),
            pages_skipped: MetricBuilder::new(metrics).counter(METRIC_PAGES_SKIPPED, partition),
            columnar_batches: MetricBuilder::new(metrics)
                .counter(METRIC_COLUMNAR_BATCHES, partition),
            rowpath_batches: MetricBuilder::new(metrics).counter(METRIC_ROWPATH_BATCHES, partition),
        }
    }
}

/// One decoded block held for the whole partition on the columnar fast path,
/// with the ascending block-row indices that survived the query's ts/trace_id
/// predicates. The columnar builder reads its columns through
/// [`DecodedBlock::view`] and gathers them over `rows`.
struct HeldBlock {
    block: DecodedBlock,
    rows: Vec<usize>,
}

/// The columnar fast path's materialized partition (ADR-0110 decisions 3, 6):
/// every decoded block the partition owns, plus a stable `(trace_id, start_ts)`
/// ordering over their surviving rows as `(block index, block row)` pairs. The
/// stream chunks `order` into [`BATCH_ROWS`] batches and gathers each projected
/// column out of the referenced block's view.
struct ColumnarPartition {
    blocks: Vec<HeldBlock>,
    /// `(block index, block row)` for every surviving row, stably sorted by
    /// `(trace_id, start_ts)` so the emitted batches carry the advertised order.
    order: Vec<(usize, usize)>,
    /// The projected schema indices, in output order.
    projection: Arc<Vec<usize>>,
    /// Aggregate page counts across the partition's blocks, for the
    /// `pages_decoded`/`pages_skipped` metrics.
    pages_decoded: usize,
    pages_skipped: usize,
}

/// What one partition's fetch produced: either the columnar fast path's held
/// blocks and ordering, or a row-path `Vec<SpanRow>` (the ineligible path, or
/// an eligible one that fell back on an `attrs_raw` block). `projection` on the
/// row variant is the pushed projection (`Some` on the eligible-fallback path,
/// so the built batch matches the plan's projected schema; `None` on the
/// ineligible path, where a `ProjectionExec` above the scan does the selection).
enum Prepared {
    Rows {
        rows: Vec<SpanRow>,
        projection: Option<Arc<Vec<usize>>>,
        /// Aggregate page counts across every block this partition decoded, for
        /// the `pages_decoded`/`pages_skipped` metrics, exactly as the columnar
        /// variant carries its own (#669). The row fetch decodes every page of
        /// every block it scans, so `pages_skipped` is 0 unless an abandoned
        /// columnar attempt contributed to it.
        pages_decoded: usize,
        pages_skipped: usize,
    },
    Columnar(ColumnarPartition),
}

/// What the columnar attempt produced: the materialized partition, or the
/// `attrs_raw` fallback, carrying the pages the abandoned attempt had already
/// decoded. Those pages really were decompressed (the query's page-byte
/// accounting counts them too), so they belong in the partition's totals rather
/// than being dropped because the attempt was discarded.
enum ColumnarAttempt {
    Ready(ColumnarPartition),
    FellBack {
        pages_decoded: usize,
        pages_skipped: usize,
    },
}

/// Fetch every segment in this partition and return its rows in `(trace_id,
/// start_ts)` order, columnar (the fast path) or as `SpanRow`s (the row path).
///
/// Every fetched row already satisfies `query` exactly (the scan re-checks the
/// ts overlap and trace_id per row); the `service_name`/`name` bloom probes only
/// ever widened the block read set, so nothing here narrows beyond what the
/// exact per-row check keeps.
///
/// When `attempt_columnar` (the query-shape clauses of
/// [`columnar_static_eligible`] held at plan time) the partition drains the
/// columnar exit. A block carrying an `attrs_raw` overflow page fails the last
/// eligibility clause (ADR-0110 decision 3): the whole partition falls back to
/// the row path so no `attrs_raw` block is ever served columnar.
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
    attempt_columnar: bool,
    projection: Option<Arc<Vec<usize>>>,
    accounting: QueryAccounting,
) -> DFResult<Prepared> {
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

    // Pages this partition's decode touched, published as the
    // `pages_decoded`/`pages_skipped` metrics. Non-zero before the row loop only
    // when an abandoned columnar attempt already decoded blocks.
    let mut pages_decoded = 0usize;
    let mut pages_skipped = 0usize;

    if attempt_columnar {
        // `attempt_columnar` implies the provider pushed a projection.
        let proj = projection
            .clone()
            .ok_or_else(|| DataFusionError::Internal("columnar scan without projection".into()))?;
        match prepare_columnar(
            &fetcher,
            tenant_hash,
            &segs,
            &query,
            duration_ns,
            status_mask,
            &predicates,
            &proj,
            &accounting,
        )
        .await?
        {
            ColumnarAttempt::Ready(part) => return Ok(Prepared::Columnar(part)),
            // A block carried an `attrs_raw` overflow page: fall through to the
            // row path for the whole partition, still emitting the projected
            // schema, and keep the abandoned attempt's page counts.
            ColumnarAttempt::FellBack {
                pages_decoded: d,
                pages_skipped: s,
            } => {
                pages_decoded = d;
                pages_skipped = s;
            }
        }
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
        // The same decode-side counts the columnar arm reads off each block; the
        // row fetch requests every column, so it decodes every page of every
        // block it scanned and skips none (#669).
        pages_decoded += output.pages_decoded;
        pages_skipped += output.pages_skipped;
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
    Ok(Prepared::Rows {
        rows: out,
        projection,
        pages_decoded,
        pages_skipped,
    })
}

/// Drain the columnar exit for the whole partition (ADR-0110 decisions 3, 6).
/// Returns [`ColumnarAttempt::FellBack`] when a block carries an `attrs_raw`
/// overflow page, signalling the caller to fall back to the row path; otherwise
/// the held blocks and the stable `(trace_id, start_ts)` ordering over their
/// surviving rows.
#[allow(clippy::too_many_arguments)]
async fn prepare_columnar(
    fetcher: &SpanSegmentFetcher,
    tenant_hash: TenantHash,
    segs: &[SegmentRef],
    query: &SpanQuery,
    duration_ns: Option<(i64, i64)>,
    status_mask: Option<u8>,
    predicates: &[BloomPredicate<'_>],
    projection: &Arc<Vec<usize>>,
    accounting: &QueryAccounting,
) -> DFResult<ColumnarAttempt> {
    let union = union_projected_columns(projection);
    let mut blocks: Vec<HeldBlock> = Vec::new();
    let mut pages_decoded = 0usize;
    let mut pages_skipped = 0usize;
    for seg in segs {
        let Some(mut scan) = fetcher
            .fetch_accounted_columnar(
                seg,
                tenant_hash,
                query,
                duration_ns,
                status_mask,
                predicates,
                &union,
                accounting,
            )
            .await
            .map_err(SqlError::from)?
        else {
            continue;
        };
        while let Some(cb) = scan.next_block().map_err(SqlError::from)? {
            // Counted before the eligibility check below: this block's pages
            // were decompressed by `next_block` whether or not the partition
            // goes on to abandon the attempt.
            pages_decoded += cb.block.pages_decoded();
            pages_skipped += cb.block.pages_skipped();
            // Last eligibility clause (ADR-0110 decision 3): a block carrying an
            // `attrs_raw` overflow page fails closed to the row path, answered
            // from page descriptors without decoding the page.
            if cb.block.has_attrs_raw_page() {
                return Ok(ColumnarAttempt::FellBack {
                    pages_decoded,
                    pages_skipped,
                });
            }
            if !cb.rows.is_empty() {
                blocks.push(HeldBlock {
                    block: cb.block,
                    rows: cb.rows,
                });
            }
        }
    }

    // Build the (trace_id, start_ts) ordering. Blocks arrive in segment order,
    // then block order, and each block's `rows` is ascending, matching the row
    // path's extend order; a stable sort by (trace_id, start_ts) then keeps ties
    // in that same order, so the two paths interleave identically.
    let mut keyed: Vec<([u8; 16], i64, usize, usize)> = Vec::new();
    for (bi, held) in blocks.iter().enumerate() {
        let view = held.block.view();
        let trace = view.fixed_column(COL_TRACE_ID).map_err(corrupt)?;
        let start = view.i64_column(COL_START_TS).map_err(corrupt)?;
        for &r in &held.rows {
            let tid: [u8; 16] = trace
                .value_at(r)
                .ok_or_else(|| DataFusionError::Internal("missing trace_id in survivor".into()))?
                .try_into()
                .map_err(|_| DataFusionError::Internal("trace_id width".into()))?;
            let ts = start
                .value_at(r)
                .ok_or_else(|| DataFusionError::Internal("missing start_ts in survivor".into()))?;
            keyed.push((tid, ts, bi, r));
        }
    }
    keyed.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let order: Vec<(usize, usize)> = keyed.into_iter().map(|(_, _, bi, r)| (bi, r)).collect();

    Ok(ColumnarAttempt::Ready(ColumnarPartition {
        blocks,
        order,
        projection: Arc::clone(projection),
        pages_decoded,
        pages_skipped,
    }))
}

/// Map a [`ravel_rspan::SpanSegError`] from a view accessor into a DataFusion
/// error. A `ColumnNotRequested` here is a caller bug (a column read that the
/// union in [`union_projected_columns`] did not request); it surfaces loudly
/// rather than as a silent all-`NULL` column.
fn corrupt(e: ravel_rspan::SpanSegError) -> DataFusionError {
    DataFusionError::Internal(format!("columnar spans view: {e}"))
}

type PrepareFuture = Pin<Box<dyn Future<Output = DFResult<Prepared>> + Send>>;

enum SpanScanState {
    Fetching(PrepareFuture),
    EmittingRows {
        rows: Vec<SpanRow>,
        projection: Option<Arc<Vec<usize>>>,
        pos: usize,
    },
    EmittingColumnar {
        part: ColumnarPartition,
        pos: usize,
    },
    Done,
}

/// Per-partition record-batch stream: awaits the fetch, then emits
/// `(trace_id, start_ts)`-ordered bounded batches, growing the memory
/// reservation by each batch's measured size so a byte-budget overrun surfaces
/// as the pool's `ResourcesExhausted`. The reservation lives on the stream so
/// it frees exactly once on drop. The per-batch reservation charge
/// (`batch.get_array_memory_size()`) is identical on both paths (ADR-0110
/// decision 3).
struct SpanScanStream {
    schema: SchemaRef,
    reservation: MemoryReservation,
    metrics: SpanScanMetrics,
    state: SpanScanState,
}

impl SpanScanStream {
    /// Charge `batch` against the reservation and hand it out, or fail the
    /// stream if the byte budget is exceeded.
    fn emit(&mut self, batch: RecordBatch) -> Poll<Option<DFResult<RecordBatch>>> {
        if let Err(e) = self.reservation.try_grow(batch.get_array_memory_size()) {
            self.state = SpanScanState::Done;
            return Poll::Ready(Some(Err(e)));
        }
        Poll::Ready(Some(Ok(batch)))
    }
}

impl Stream for SpanScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                SpanScanState::Fetching(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Prepared::Rows {
                        rows,
                        projection,
                        pages_decoded,
                        pages_skipped,
                    })) => {
                        // Published on this path for the same reason as on the
                        // columnar one (#669): the counters are what an EXPLAIN
                        // ANALYZE reader has to judge decode work by, and a
                        // metric left unwritten reads as a measured zero.
                        this.metrics.pages_decoded.add(pages_decoded);
                        this.metrics.pages_skipped.add(pages_skipped);
                        this.state = SpanScanState::EmittingRows {
                            rows,
                            projection,
                            pos: 0,
                        };
                    }
                    Poll::Ready(Ok(Prepared::Columnar(part))) => {
                        this.metrics.pages_decoded.add(part.pages_decoded);
                        this.metrics.pages_skipped.add(part.pages_skipped);
                        this.state = SpanScanState::EmittingColumnar { part, pos: 0 };
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SpanScanState::EmittingRows {
                    rows,
                    projection,
                    pos,
                } => {
                    if *pos >= rows.len() {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(rows.len());
                    let batch = match build_row_batch(
                        &rows[*pos..end],
                        Arc::clone(&this.schema),
                        projection.as_deref().map(Vec::as_slice),
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = SpanScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    this.metrics.rowpath_batches.add(1);
                    return this.emit(batch);
                }
                SpanScanState::EmittingColumnar { part, pos } => {
                    if *pos >= part.order.len() {
                        this.state = SpanScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (*pos + BATCH_ROWS).min(part.order.len());
                    let batch = match build_columnar_batch(
                        &part.blocks,
                        &part.order[*pos..end],
                        Arc::clone(&this.schema),
                        &part.projection,
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            this.state = SpanScanState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    *pos = end;
                    this.metrics.columnar_batches.add(1);
                    return this.emit(batch);
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

/// Build one `spans` [`RecordBatch`] from a slice of [`SpanRow`]s, the row path
/// (ADR-0110's fallback and the ineligible path).
///
/// `projection` `None` builds the full eleven-column schema (the ineligible
/// path, where a `ProjectionExec` above the scan does the column selection);
/// `Some(indices)` builds exactly those schema columns in order, so an eligible
/// query that fell back on an `attrs_raw` block still emits the projected schema
/// the plan advertises. `schema` must match: the full schema for `None`, the
/// projected schema for `Some`.
fn build_row_batch(
    rows: &[SpanRow],
    schema: SchemaRef,
    projection: Option<&[usize]>,
) -> DFResult<RecordBatch> {
    let all: [usize; 11] = [
        SPAN_COL_TRACE_ID,
        SPAN_COL_SPAN_ID,
        SPAN_COL_PARENT_SPAN_ID,
        SPAN_COL_NAME,
        SPAN_COL_START_TS,
        SPAN_COL_END_TS,
        SPAN_COL_STATUS_CODE,
        SPAN_COL_STATUS_MESSAGE,
        SPAN_COL_ATTRS,
        SPAN_COL_SERVICE_NAME,
        SPAN_COL_DURATION_NS,
    ];
    let indices = projection.unwrap_or(&all);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for &idx in indices {
        columns.push(row_column(idx, rows)?);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

/// Build one `spans` column array for schema index `idx` from the row path's
/// [`SpanRow`]s.
fn row_column(idx: usize, rows: &[SpanRow]) -> DFResult<ArrayRef> {
    Ok(match idx {
        SPAN_COL_TRACE_ID => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), TRACE_ID_WIDTH);
            for row in rows {
                b.append_value(row.record.trace_id)
                    .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?;
            }
            Arc::new(b.finish())
        }
        SPAN_COL_SPAN_ID => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), SPAN_ID_WIDTH);
            for row in rows {
                b.append_value(row.record.span_id)
                    .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?;
            }
            Arc::new(b.finish())
        }
        SPAN_COL_PARENT_SPAN_ID => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), SPAN_ID_WIDTH);
            for row in rows {
                match &row.record.parent_span_id {
                    Some(id) => b.append_value(id).map_err(|e| {
                        SqlError::Internal(format!("parent_span_id array build: {e}"))
                    })?,
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SPAN_COL_NAME => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.record.name.as_str())
                .collect::<Vec<_>>(),
        )),
        SPAN_COL_START_TS => Arc::new(TimestampNanosecondArray::from(
            rows.iter()
                .map(|row| row.record.start_ts_ns)
                .collect::<Vec<_>>(),
        )),
        SPAN_COL_END_TS => Arc::new(TimestampNanosecondArray::from(
            rows.iter()
                .map(|row| row.record.end_ts_ns)
                .collect::<Vec<_>>(),
        )),
        SPAN_COL_STATUS_CODE => Arc::new(UInt8Array::from(
            rows.iter()
                .map(|row| row.record.status_code.to_u8())
                .collect::<Vec<_>>(),
        )),
        SPAN_COL_STATUS_MESSAGE => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.record.status_message.as_deref())
                .collect::<Vec<_>>(),
        )),
        SPAN_COL_ATTRS => {
            // `attrs` map: RSPAN already merged resource+scope+span into
            // `record.attrs` with unique, ascending keys, so it copies straight
            // into the column with no decode or re-verification (unlike `logs`,
            // whose merged view is rebuilt at scan time from a stream-identity
            // blob). `service.name` is present here too: the reader re-inserts
            // it into the record's attrs from the v3 column, so this map is the
            // same one a v1 object would have produced.
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
            Arc::new(attrs.finish())
        }
        // service_name (v3, ADR-0054): read straight from the dictionary-encoded
        // `COL_SERVICE_NAME` column by the fetcher and carried on each
        // [`SpanRow`], not looked up by linear scan of the merged attrs map at
        // build time. NULL when the span carried no `service.name`.
        SPAN_COL_SERVICE_NAME => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.service_name.as_deref())
                .collect::<Vec<_>>(),
        )),
        // duration_ns is computed, never stored (ADR-0045 decision 5, rejected
        // alternative 3). `saturating_sub` rather than a bare `-`: end_ts_ns >=
        // start_ts_ns is a format invariant, not one this column should assume
        // and panic on if corrupt or adversarial data ever violates it.
        SPAN_COL_DURATION_NS => Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.record.end_ts_ns.saturating_sub(row.record.start_ts_ns))
                .collect::<Vec<_>>(),
        )),
        other => {
            return Err(DataFusionError::Internal(format!(
                "spans row column index {other} out of range"
            )));
        }
    })
}

/// Build one `spans` [`RecordBatch`] straight from the columnar view (ADR-0110
/// decision 3), gathering each projected column out of the referenced block's
/// view over `order` (`(block index, block row)` pairs in output order). No
/// `SpanRecord` and no `SpanRow`. `attrs` is never among `projection` here (the
/// fast path is not eligible when it is), so the dynamic attribute pages, the
/// `attrs_raw` page, and the event pages are never touched.
fn build_columnar_batch(
    blocks: &[HeldBlock],
    order: &[(usize, usize)],
    schema: SchemaRef,
    projection: &[usize],
) -> DFResult<RecordBatch> {
    let views: Vec<_> = blocks.iter().map(|h| h.block.view()).collect();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &idx in projection {
        columns.push(columnar_column(idx, &views, order)?);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

/// Build one `spans` column array for schema index `idx` by gathering the
/// mapped RSPAN column out of each row's block view over `order`.
fn columnar_column(
    idx: usize,
    views: &[ravel_rspan::ColumnarBlockView<'_>],
    order: &[(usize, usize)],
) -> DFResult<ArrayRef> {
    // Every fixed/i64/str accessor is fallible only when the column was not
    // requested by the decode; `union_projected_columns` requests every column
    // any projected index reads, so a projected build never hits that error.
    Ok(match idx {
        SPAN_COL_TRACE_ID => {
            let cols = fixed_cols(views, COL_TRACE_ID)?;
            let mut b = FixedSizeBinaryBuilder::with_capacity(order.len(), TRACE_ID_WIDTH);
            for &(bi, r) in order {
                let v = cols[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing trace_id".into()))?;
                b.append_value(v)
                    .map_err(|e| SqlError::Internal(format!("trace_id array build: {e}")))?;
            }
            Arc::new(b.finish())
        }
        SPAN_COL_SPAN_ID => {
            let cols = fixed_cols(views, COL_SPAN_ID)?;
            let mut b = FixedSizeBinaryBuilder::with_capacity(order.len(), SPAN_ID_WIDTH);
            for &(bi, r) in order {
                let v = cols[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing span_id".into()))?;
                b.append_value(v)
                    .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?;
            }
            Arc::new(b.finish())
        }
        SPAN_COL_PARENT_SPAN_ID => {
            let cols = fixed_cols(views, COL_PARENT_SPAN_ID)?;
            let mut b = FixedSizeBinaryBuilder::with_capacity(order.len(), SPAN_ID_WIDTH);
            for &(bi, r) in order {
                match cols[bi].value_at(r) {
                    Some(v) => b.append_value(v).map_err(|e| {
                        SqlError::Internal(format!("parent_span_id array build: {e}"))
                    })?,
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SPAN_COL_NAME => {
            let cols = str_cols(views, COL_NAME)?;
            let mut b = StringBuilder::new();
            for &(bi, r) in order {
                let v = cols[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing name".into()))?;
                b.append_value(str_utf8(v)?);
            }
            Arc::new(b.finish())
        }
        SPAN_COL_START_TS => {
            let cols = i64_cols(views, COL_START_TS)?;
            Arc::new(TimestampNanosecondArray::from(gather_i64(
                &cols, order, "start_ts",
            )?))
        }
        SPAN_COL_END_TS => {
            let cols = i64_cols(views, COL_END_TS)?;
            Arc::new(TimestampNanosecondArray::from(gather_i64(
                &cols, order, "end_ts",
            )?))
        }
        SPAN_COL_STATUS_CODE => {
            let cols = i64_cols(views, COL_STATUS_CODE)?;
            let mut out: Vec<u8> = Vec::with_capacity(order.len());
            for &(bi, r) in order {
                let v = cols[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing status_code".into()))?;
                out.push(
                    u8::try_from(v)
                        .map_err(|_| DataFusionError::Internal("status_code range".into()))?,
                );
            }
            Arc::new(UInt8Array::from(out))
        }
        SPAN_COL_STATUS_MESSAGE => {
            let cols = str_cols(views, COL_STATUS_MESSAGE)?;
            let mut b = StringBuilder::new();
            for &(bi, r) in order {
                match cols[bi].value_at(r) {
                    Some(v) => b.append_value(str_utf8(v)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SPAN_COL_SERVICE_NAME => {
            let cols = str_cols(views, COL_SERVICE_NAME)?;
            let mut b = StringBuilder::new();
            for &(bi, r) in order {
                match cols[bi].value_at(r) {
                    Some(v) => b.append_value(str_utf8(v)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        SPAN_COL_DURATION_NS => {
            // Computed exactly as the row path: end - start with saturating_sub
            // (end >= start is a format invariant, not one this column panics on).
            let starts = i64_cols(views, COL_START_TS)?;
            let ends = i64_cols(views, COL_END_TS)?;
            let mut out: Vec<i64> = Vec::with_capacity(order.len());
            for &(bi, r) in order {
                let s = starts[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing start_ts".into()))?;
                let e = ends[bi]
                    .value_at(r)
                    .ok_or_else(|| DataFusionError::Internal("missing end_ts".into()))?;
                out.push(e.saturating_sub(s));
            }
            Arc::new(Int64Array::from(out))
        }
        other => {
            // `attrs` (SPAN_COL_ATTRS) never reaches here: the fast path is
            // ineligible when it is projected.
            return Err(DataFusionError::Internal(format!(
                "spans columnar column index {other} not supported on the fast path"
            )));
        }
    })
}

/// Per-block fixed-column handles for `col`, one per view, in block order.
fn fixed_cols<'a>(
    views: &[ravel_rspan::ColumnarBlockView<'a>],
    col: u32,
) -> DFResult<Vec<ravel_rspan::BytesColumn<'a>>> {
    views
        .iter()
        .map(|v| v.fixed_column(col).map_err(corrupt))
        .collect()
}

/// Per-block string-column handles for `col`, one per view, in block order.
fn str_cols<'a>(
    views: &[ravel_rspan::ColumnarBlockView<'a>],
    col: u32,
) -> DFResult<Vec<ravel_rspan::BytesColumn<'a>>> {
    views
        .iter()
        .map(|v| v.str_column(col).map_err(corrupt))
        .collect()
}

/// Per-block i64-column handles for `col`, one per view, in block order.
fn i64_cols<'a>(
    views: &[ravel_rspan::ColumnarBlockView<'a>],
    col: u32,
) -> DFResult<Vec<ravel_rspan::I64Column<'a>>> {
    views
        .iter()
        .map(|v| v.i64_column(col).map_err(corrupt))
        .collect()
}

/// Gather a required i64 column over `order`, erroring on a missing cell.
fn gather_i64(
    cols: &[ravel_rspan::I64Column<'_>],
    order: &[(usize, usize)],
    what: &str,
) -> DFResult<Vec<i64>> {
    let mut out = Vec::with_capacity(order.len());
    for &(bi, r) in order {
        out.push(
            cols[bi]
                .value_at(r)
                .ok_or_else(|| DataFusionError::Internal(format!("missing {what}")))?,
        );
    }
    Ok(out)
}

/// Validate a byte column cell as UTF-8, mirroring the row path's `string_from`.
fn str_utf8(bytes: &[u8]) -> DFResult<&str> {
    std::str::from_utf8(bytes).map_err(|_| DataFusionError::Internal("value not utf-8".into()))
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
            segment_format_version: u32::from(ravel_rspan::footer::VERSION),
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
            segment_format_version: u32::from(ravel_rspan::footer::VERSION),
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
