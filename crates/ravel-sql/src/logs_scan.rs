//! `LogsScanExec`: the leaf of the `logs` pipeline, the log-signal sibling of
//! [`crate::scan::RsegScanExec`] (ADR-0033).
//!
//! Partitions the snapshot's segments round-robin into
//! `N = min(target_partitions, segment_count)` partitions. Each partition
//! opens its segments one at a time through
//! [`LogSegmentFetcher::scan_accounted_with_tenant`] (one [`LogQuery`] per
//! segment: the extracted ts range, stream-attribute equalities, and content
//! predicates), decodes **one block at a time**, and turns each block's
//! [`ravel_logseg::LogRecord`]s into Arrow arrays matching this scan's
//! projection of [`crate::logs_schema::logs_schema`].
//!
//! # Streaming, and why no ordering is declared (ADR-0087)
//!
//! This stage declares **no** output ordering. It used to declare `ts`
//! ascending per partition, and earned that by collecting the whole partition
//! and sorting it before emitting anything -- which made peak memory
//! proportional to the partition, i.e. to the table. `RlogReader` itself only
//! emits a segment's records grouped by `(stream_ref, ts)`, not globally by
//! `ts`, and a partition draws from several segments, so a block-at-a-time
//! scan cannot truthfully claim a global per-partition `ts` order.
//!
//! Declaring one anyway would be silently wrong, not merely optimistic:
//! DataFusion trusts a leaf's declared ordering and would skip the sort an
//! `ORDER BY ts` needs. So the guarantee is gone, and an `ORDER BY ts` gets an
//! explicit `SortExec` that DataFusion inserts above this leaf. Nothing here
//! sorts, buffers a partition, or otherwise reintroduces the bound this
//! removes.
//!
//! Memory is reserved against the query's DataFusion pool for what the scan
//! *currently holds* -- the decoded block being drained plus the batch just
//! handed downstream -- and released as each goes away, so the pool bounds
//! concurrently-held scan memory rather than cumulative bytes emitted.
//!
//! The two batch-building paths hold different things, and each charges what it
//! actually holds. The row path's [`ravel_logseg::BlockScan::next_block`] drops
//! the decoded block before it returns, so what remains resident is the
//! `Vec<LogRecord>` it built ([`records_memory`]) plus the batch handed
//! downstream. The columnar path's `next_block_columnar` hands out a view
//! *borrowing* the decoded block, which the reader releases only when the next
//! block is decoded, so the block stays resident alongside the Arrow batches
//! built from it: both terms are charged together
//! ([`LogScanStream::hold_batches`]) and released together. Charging the
//! batches alone would admit a query at a fraction of its resident footprint.
//!
//! # Column projection
//!
//! The scan's output schema *is* the projection DataFusion asked for; there is
//! no `ProjectionExec` above it dropping columns the scan already paid to
//! produce. The projected columns, plus every field a pushed content predicate
//! names, plus every attribute key a pending erasure predicate names, are
//! resolved into a [`ColumnSelection`] that the reader uses to decode only
//! those columns' pages ([`resolve_columns`]). Any reference to the SQL `attrs`
//! map column resolves to every dynamic column plus `attrs_raw`, because the
//! map's contract is that every key is present; per-key `attrs['k']`
//! projection is out of scope (ADR-0087 decision 3).
//!
//! # Correctness: the merged `attrs` column plus DataFusion's residual
//!
//! This scan pushes three predicate kinds into [`LogSegmentFetcher::fetch`]:
//! the ts range (a segment-level and reader-level prune, exact), content
//! predicates (`has_word`, whose SQL semantics equal the reader's exact filter,
//! [`crate::logs_pushdown`]), and the prune-only channel
//! ([`crate::logs_pushdown::LogsPushdown::prune`], attribute equalities that
//! drive POSTINGS block pruning and are never evaluated per row). It does
//! **not** push stream-attribute equalities, and it performs no per-record
//! re-verification: it emits every record the fetcher returns. Attribute
//! filtering is entirely DataFusion's job.
//!
//! The prune channel changes only how much of an object the fetch decodes. An
//! arm proves a block holds no record carrying the term, so dropping that block
//! cannot drop a row the query needs, and an arm the object's POSTINGS index
//! does not cover prunes nothing (ADR-0049 decision 5, ADR-0013's widen-only
//! rule). What it costs is visible: the `blocks_total`,
//! `blocks_scanned`, and `blocks_pruned_by_postings` DataFusion metrics below
//! report it per partition, so `EXPLAIN ANALYZE` shows whether a query pruned.
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
//! column from the fully merged view (ADR-0033 amendment), so the
//! residual evaluates `attrs['k'] = 'v'` against exactly the data a row's SQL
//! semantics demand: a resource-only match survives (the residual sees it in the
//! merged column), and a record-attribute override survives (the merge resolves
//! the key to the record's value, which wins). The merged column and the
//! residual are the whole correctness story.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Int64Builder, MapBuilder,
    StringArray, StringBuilder, TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::EquivalenceProperties;
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
use ravel_logseg::{
    AttrColumn, ColumnSelection, ColumnarBlockView, FieldSel, FieldType, LogRecord, Predicate,
    ScanStats,
};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{ColumnarBlockOutcome, LogQuery, LogSegmentFetcher, LogSegmentScan};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::error::SqlError;
use crate::logs_schema::{
    FIRST_DECLARED_COL, LOG_COL_ATTRS, LOG_COL_BODY, LOG_COL_FLAGS, LOG_COL_OBSERVED_TS,
    LOG_COL_SEVERITY_NUM, LOG_COL_SEVERITY_TEXT, LOG_COL_SPAN_ID, LOG_COL_TRACE_ID, LOG_COL_TS,
    SPAN_ID_WIDTH, TRACE_ID_WIDTH,
};
use crate::rlog_attrs::{
    attr_value_to_string, decode_stream_attrs, find_attr, merged_attrs, retain_unerased,
};
use ravel_logseg::record::canonical_value_bytes;

/// Rows accumulated into one output batch before it is emitted.
///
/// A block usually decodes to fewer rows than this (RLOG's default block target
/// is 8192 records, and predicate evaluation only removes rows), so one block
/// normally becomes one batch. This bounds the other direction: a block written
/// with a larger target is still emitted in pieces of at most this many rows, so
/// one batch's Arrow footprint stays bounded whatever the writer chose.
const BATCH_ROWS: usize = 8192;

/// Rough resident size of a decoded record in row form, for the memory
/// reservation.
///
/// Deliberately an estimate, not an exact figure: the point is that the pool
/// sees a charge proportional to what the scan actually holds, which nothing
/// charged at all before (ADR-0087 context). It counts the struct itself, the
/// owned string and blob payloads, and the attribute vector's spine and
/// contents. It does not chase `AttrValue::List`/`Map` recursively past their
/// direct children, so a deeply nested attribute is undercounted; the fixed
/// per-record and per-attribute terms dominate at the cardinalities this
/// bound exists for.
fn records_memory(records: &[LogRecord]) -> usize {
    let mut total = std::mem::size_of_val(records);
    for r in records {
        total += r.stream_attrs.len() + r.severity_text.len() + r.body.len();
        total += r.attrs.len() * std::mem::size_of::<(String, AttrValue)>();
        for (k, v) in &r.attrs {
            total += k.len() + attr_value_memory(v);
        }
    }
    total
}

fn attr_value_memory(v: &AttrValue) -> usize {
    match v {
        AttrValue::Str(s) => s.len(),
        AttrValue::Bytes(b) => b.len(),
        AttrValue::I64(_) | AttrValue::F64(_) | AttrValue::Bool(_) => 0,
        AttrValue::List(items) => items.len() * std::mem::size_of::<AttrValue>(),
        AttrValue::Map(entries) => entries.len() * std::mem::size_of::<(String, AttrValue)>(),
    }
}

/// The block columns one query needs decoded (ADR-0087 decision 3, extended by
/// ADR-0090 decision 4).
///
/// Five contributors, and every one of them is load-bearing:
///
/// - the **projected schema columns**, which are what the query's output and
///   DataFusion's residual `FilterExec` above this leaf read (the projection
///   DataFusion hands `TableProvider::scan` already includes the columns its
///   residual filters need, which is why the residual is safe over a projected
///   scan);
/// - the **`ts`/`stream_ref` fixed columns**, added unconditionally by
///   [`ColumnSelection`] because every rebuilt record and every exact
///   ts re-check needs them;
/// - every field a **pushed content predicate** names, because
///   `RlogReader` evaluates those exactly per row and a column it cannot see
///   reads as absent, i.e. as not matching, i.e. as dropped rows;
/// - every attribute key a **pending erasure predicate** names, at record level
///   for the fetcher's own filter and at merged resource/scope/record level for
///   [`retain_unerased`] here (ADR-0064). A key the selection omits makes an
///   erased row reappear.
/// - every **declared typed attribute column** the projection names (ADR-0090
///   decision 4). A declared column occupies a schema index at or above
///   [`FIRST_DECLARED_COL`]; DataFusion already folds a residual-filter column
///   into the projection it hands the scan, so a declared column named only in
///   a `WHERE` clause is still decoded. This adds the declared key to the
///   selection exactly like the content- and erasure-predicate contributors do;
///   declared-column predicates are NOT extracted into the prune or
///   content-predicate channels in this ADR (a typed comparison is a residual
///   filter above the scan; typed-predicate pushdown is #278).
///
/// The prune-only channel contributes nothing: its arms drive POSTINGS block
/// pruning and are never evaluated per row, so no page has to be decoded for
/// them.
///
/// Resource and scope attributes cost nothing to keep: they live in STREAM_DIR,
/// reached through `stream_ref`, not in a block column. So an erasure subject
/// named only at resource level is matched under any selection.
fn resolve_columns(
    projection: &[usize],
    content: &[Predicate],
    erasure: &[ErasurePredicate],
    declared: &[DeclaredColumn],
) -> ColumnSelection {
    let mut sel = ColumnSelection::fixed_only();
    for &i in projection {
        sel = match i {
            // `ts` is always decoded; naming it changes nothing.
            LOG_COL_TS => sel,
            LOG_COL_OBSERVED_TS => sel.with_observed_ts(),
            LOG_COL_SEVERITY_NUM => sel.with_severity_num(),
            LOG_COL_SEVERITY_TEXT => sel.with_severity_text(),
            LOG_COL_BODY => sel.with_body(),
            LOG_COL_TRACE_ID => sel.with_trace_id(),
            LOG_COL_SPAN_ID => sel.with_span_id(),
            LOG_COL_FLAGS => sel.with_flags(),
            // The merged `attrs` map exposes every key, so referencing it at
            // all means every dynamic column plus the overflow.
            LOG_COL_ATTRS => sel.with_all_attrs(),
            // A declared typed attribute column (index >= FIRST_DECLARED_COL):
            // decode exactly that key's dynamic column, the same per-key path
            // an erasure predicate uses. `i` here is never a fixed index
            // (0..=8 are matched above), so the subtraction cannot underflow;
            // `declared.get` fails open (decode everything) only if the index
            // is somehow past the declared set, which `LogsScanExec::new`'s
            // projection validation already rules out.
            other => match other
                .checked_sub(FIRST_DECLARED_COL)
                .and_then(|k| declared.get(k))
            {
                Some(dc) => sel.with_attr(dc.key.clone()),
                None => ColumnSelection::all(),
            },
        };
    }
    for p in content {
        sel = content_columns(p, sel);
    }
    for p in erasure {
        for (key, _) in p.matchers() {
            sel = sel.with_attr(key);
        }
    }
    sel
}

/// Add every column an exact content predicate reads. `TsRange` and `StreamIn`
/// need only the two always-decoded fixed columns.
fn content_columns(pred: &Predicate, sel: ColumnSelection) -> ColumnSelection {
    match pred {
        Predicate::And(arms) => arms.iter().fold(sel, |acc, a| content_columns(a, acc)),
        // `NumRange` is prune-only (ADR-0095 decision 6): it never reaches the
        // exact content channel, so it reads no columns, same as ts/stream. The
        // planner-side pushdown that would emit it is #278's job.
        Predicate::TsRange { .. } | Predicate::StreamIn(_) | Predicate::NumRange { .. } => sel,
        Predicate::HasWord { field, .. } | Predicate::Equals { field, .. } => match field {
            FieldSel::Body => sel.with_body(),
            FieldSel::SeverityText => sel.with_severity_text(),
            FieldSel::Attr(name) => sel.with_attr(name.clone()),
        },
    }
}

/// The query-shape half of the columnar fast-path eligibility rule (ADR-0099
/// decision 2), decided once at plan time. The fast path is taken only when
/// this AND the per-block `has_attrs_raw_page() == false` check both hold;
/// otherwise the row path runs unchanged.
///
/// Two clauses live here because they do not vary per block:
///
/// - **(a) the projection touches only fixed and declared typed columns.** A
///   reference to the merged `attrs` map ([`LOG_COL_ATTRS`]) makes the query
///   ineligible: the map needs the stream-blob overlay the fast path exists to
///   avoid. A declared typed column (index `>= FIRST_DECLARED_COL`) is fine --
///   it resolves to a FIELD_DIR column the view reads directly.
/// - **(c) no pending erasure predicate applies.** Erasure exclusion is
///   record-level and has no columnar form yet, so a scan carrying one drains
///   the row path. This clause fails closed on purpose: the failure mode of
///   getting erasure wrong is an erased record served to a client, not a slow
///   query. In practice this also falls out of handling
///   [`ColumnarBlockOutcome::ErasurePending`], but it is asserted here as its
///   own condition so the fast path is never even attempted under erasure.
///
/// Content predicates are deliberately absent: the reader evaluates them into
/// the surviving-row set before the view is handed out, so the fast path never
/// re-evaluates them and their shape cannot make it unsound.
fn columnar_static_eligible(projection: &[usize], erasure: &[ErasurePredicate]) -> bool {
    erasure.is_empty() && projection.iter().all(|&i| i != LOG_COL_ATTRS)
}

/// Log segment scan producing block-at-a-time batches over a projection of the
/// public `logs` schema. Declares no ordering (ADR-0087 decision 1).
pub struct LogsScanExec {
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    /// Round-robin segment assignment; `partitions[k]` runs as DataFusion
    /// partition `k`.
    partitions: Vec<Vec<SegmentRef>>,
    /// Inclusive ts bounds for the fetch's [`LogQuery`].
    ts_min: i64,
    ts_max: i64,
    /// Content predicates (`has_word`) handed to `RlogReader::scan_pruned` as
    /// its exact per-row filter.
    content: Arc<Vec<Predicate>>,
    /// Prune-only predicates (attribute equalities) handed to the fetch as
    /// `LogQuery::prune`. They drive POSTINGS block pruning inside the reader
    /// and are never evaluated per row, so they cannot change which records the
    /// fetch returns for a block it reads, only which blocks it reads.
    prune: Arc<Vec<Predicate>>,
    /// Pending selective-erasure predicates from the resolved snapshot
    /// (ADR-0064 decision 2). Fed to [`LogQuery::with_erasure`] so
    /// `LogSegmentFetcher::fetch`'s existing post-fetch, post-cache filter
    /// (`retain_log_records`) engages; empty when the snapshot has no pending
    /// erasure, which is a no-op there.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// Indices into the resolved full schema this scan emits, in output order.
    /// Always concrete: a `None` projection from DataFusion becomes every index.
    projection: Arc<Vec<usize>>,
    /// The block columns the reader must decode, resolved once from
    /// `projection`, `content`, `erasure`, and `declared` (see
    /// [`resolve_columns`]).
    columns: ColumnSelection,
    /// The tenant's declared typed attribute columns (ADR-0090), in schema-
    /// append order. Index `k` here is schema index `FIRST_DECLARED_COL + k`.
    /// Empty for a zero-declaration query, which is byte-identical to the
    /// pre-ADR-0090 scan.
    declared: Arc<Vec<DeclaredColumn>>,
    /// This scan's output schema: the resolved full schema
    /// (`logs_schema_with_declared(&declared)`) projected by `projection`.
    schema: SchemaRef,
    /// Whether this scan may take the columnar fast path (ADR-0099 decision 2),
    /// decided once from the query shape: the projection touches only fixed and
    /// declared columns (no `attrs` map), and no pending erasure predicate
    /// applies. The remaining per-block clause (no `attrs_raw` overflow page) is
    /// checked as each block is decoded; see [`columnar_static_eligible`].
    columnar_eligible: bool,
    properties: Arc<PlanProperties>,
    /// This query's accounting handle (ADR-0044), threaded into every
    /// per-partition fetch so log fetches are recorded like every other
    /// funnel.
    accounting: QueryAccounting,
    /// Block-level pruning counters, reported through `EXPLAIN ANALYZE`.
    metrics: ExecutionPlanMetricsSet,
}

/// The per-partition block counters this scan publishes as DataFusion metrics.
///
/// They are the only externally visible difference the prune channel makes:
/// `blocks_total` is what the fetched objects hold, `blocks_scanned` is what the
/// reader actually decoded, and `blocks_pruned_by_postings` is how many
/// candidate blocks POSTINGS removed. Rows are unaffected either way, so an
/// operator watching a prune land watches these, not the result.
#[derive(Clone)]
struct BlockMetrics {
    total: Count,
    scanned: Count,
    pruned_by_postings: Count,
    /// Column pages this partition decompressed and decoded.
    pages_decoded: Count,
    /// Column pages this partition walked past because the resolved
    /// [`ColumnSelection`] excluded them. This is the externally visible proof
    /// that column projection reached the page level rather than being a
    /// post-decode filter: a query that touches two of a hundred attributes
    /// leaves this large and `pages_decoded` small.
    pages_skipped: Count,
    /// Output batches this partition built through the columnar fast path
    /// (ADR-0099 decisions 2-3), straight from a [`ColumnarBlockView`] with no
    /// `LogRecord` and no `merged_attrs`. The output of the two paths is
    /// identical by construction, so this and [`Self::rowpath_batches`] are the
    /// only externally visible proof of which path a query took.
    columnar_batches: Count,
    /// Output batches this partition built through the row path: an ineligible
    /// query (an `attrs` projection, a pending erasure predicate) or an
    /// eligible one that hit a block carrying an `attrs_raw` overflow page.
    rowpath_batches: Count,
}

impl BlockMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        BlockMetrics {
            total: MetricBuilder::new(metrics).counter("blocks_total", partition),
            scanned: MetricBuilder::new(metrics).counter("blocks_scanned", partition),
            pruned_by_postings: MetricBuilder::new(metrics)
                .counter("blocks_pruned_by_postings", partition),
            pages_decoded: MetricBuilder::new(metrics).counter("pages_decoded", partition),
            pages_skipped: MetricBuilder::new(metrics).counter("pages_skipped", partition),
            columnar_batches: MetricBuilder::new(metrics).counter("columnar_batches", partition),
            rowpath_batches: MetricBuilder::new(metrics).counter("rowpath_batches", partition),
        }
    }

    /// Accumulates one segment's [`ScanStats`]. `blocks_pruned_by_postings` is
    /// the drop across the postings step alone (`blocks_after_skip` minus
    /// `blocks_after_postings`), so it credits POSTINGS with nothing the skip
    /// index or the bloom did. `saturating_sub` because a degraded postings
    /// section leaves the two counts equal rather than ordered by construction.
    fn record(&self, stats: &ScanStats) {
        self.total.add(stats.blocks_total as usize);
        self.scanned.add(stats.blocks_scanned as usize);
        self.pruned_by_postings.add(
            stats
                .blocks_after_skip
                .saturating_sub(stats.blocks_after_postings) as usize,
        );
        self.pages_decoded.add(stats.pages_decoded as usize);
        self.pages_skipped.add(stats.pages_skipped as usize);
    }
}

impl LogsScanExec {
    /// Build a scan over `segments`, split round-robin into
    /// `min(target_partitions, segments.len())` partitions, with the given ts
    /// bounds, content predicates, and prune-only predicates. Stream-attribute
    /// equalities are deliberately not accepted: they are not pushed into the
    /// fetch, because a stream-level prune is unsound against the merged `attrs`
    /// column (see the module doc). DataFusion's residual filters attributes.
    ///
    /// `prune` is the POSTINGS channel, not a filter. An empty `prune` makes
    /// this scan read and emit exactly what it did before the channel existed.
    // `tenant_hash` widened this past clippy\'s 7-argument
    // threshold; the codebase allows it at the equivalent sites
    // (scan.rs, ravel-query\'s fetcher.rs).
    /// `full_schema` is the resolved full `logs` schema this scan projects, i.e.
    /// `logs_schema_with_declared(&declared)` for the tenant's `declared`
    /// columns (ADR-0090 decision 3). It is passed in rather than built here so
    /// the provider resolves it once and the projection, batch builder, and
    /// column-set resolution all agree with the schema the planner saw.
    /// `declared` is the same tenant's declared columns in schema-append order,
    /// so [`build_batch`] and [`resolve_columns`] can map a projected declared
    /// index back to its key and type. Both are empty/base for a
    /// zero-declaration query.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        content: Arc<Vec<Predicate>>,
        prune: Arc<Vec<Predicate>>,
        erasure: Arc<Vec<ErasurePredicate>>,
        projection: Option<&Vec<usize>>,
        accounting: QueryAccounting,
        full_schema: SchemaRef,
        declared: Arc<Vec<DeclaredColumn>>,
    ) -> DFResult<Self> {
        let n = target_partitions.max(1).min(segments.len().max(1));
        let mut partitions: Vec<Vec<SegmentRef>> = vec![Vec::new(); n];
        for (i, seg) in segments.iter().enumerate() {
            partitions[i % n].push(seg.clone());
        }
        let full = full_schema;
        // A `None` projection means every column, in schema order. Resolving it
        // here rather than carrying an `Option` keeps one code path for the
        // schema, the batch builder, and the column-set resolution.
        let projection: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..full.fields().len()).collect(),
        };
        for &i in &projection {
            if i >= full.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "logs scan projection index {i} out of range"
                )));
            }
        }
        let columns = resolve_columns(&projection, &content, &erasure, &declared);
        let schema: SchemaRef = Arc::new(full.project(&projection)?);
        let columnar_eligible = columnar_static_eligible(&projection, &erasure);
        let properties = Arc::new(Self::compute_properties(&schema, n));
        Ok(LogsScanExec {
            tenant_hash,
            fetcher,
            partitions,
            ts_min,
            ts_max,
            content,
            prune,
            erasure,
            projection: Arc::new(projection),
            columns,
            declared,
            schema,
            columnar_eligible,
            properties,
            accounting,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// No output ordering (ADR-0087 decision 1). A block-streaming scan emits a
    /// partition's blocks in stored order, which is `(stream_ref, ts)` within a
    /// block and segment order across a partition, so no `ts` ordering holds.
    /// Downstream operators that need one get an explicit sort.
    fn compute_properties(schema: &SchemaRef, n: usize) -> PlanProperties {
        let eq = EquivalenceProperties::new(Arc::clone(schema));
        PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
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
            "LogsScanExec: partitions={}, content={}, prune={}, projection=[{}]",
            self.partitions.len(),
            self.content.len(),
            self.prune.len(),
            self.schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let segments: VecDeque<SegmentRef> = self
            .partitions
            .get(partition)
            .cloned()
            .unwrap_or_default()
            .into();

        let mut query =
            LogQuery::new(self.ts_min, self.ts_max).with_erasure((*self.erasure).clone());
        for c in self.content.iter() {
            query = query.with_content(c.clone());
        }
        // The prune channel, kept out of `content` on purpose: the reader
        // evaluates a content arm exactly per row against per-record attributes
        // only, which would drop a resource/scope-only match the merged
        // residual must keep.
        for p in self.prune.iter() {
            query = query.with_prune(p.clone());
        }

        let reservation = MemoryConsumer::new(format!("LogsScanExec[{partition}]"))
            .register(context.memory_pool());

        Ok(Box::pin(LogScanStream {
            schema: Arc::clone(&self.schema),
            projection: Arc::clone(&self.projection),
            declared: Arc::clone(&self.declared),
            ctx: Arc::new(PartitionCtx {
                fetcher: self.fetcher.clone(),
                tenant_hash: self.tenant_hash,
                query,
                columns: self.columns.clone(),
                accounting: self.accounting.clone(),
            }),
            erasure: Arc::clone(&self.erasure),
            columnar_eligible: self.columnar_eligible,
            blocks: BlockMetrics::new(&self.metrics, partition),
            segments,
            reservation,
            held: 0,
            emitted: 0,
            pending: Pending::None,
            current_seg: None,
            seg_columnar_blocks: 0,
            state: LogScanState::NextSegment,
        }))
    }
}

/// Everything one partition's fetches need, shared by every per-segment open
/// future so each can be `'static` without cloning the query per segment.
struct PartitionCtx {
    fetcher: LogSegmentFetcher,
    tenant_hash: TenantHash,
    query: LogQuery,
    columns: ColumnSelection,
    accounting: QueryAccounting,
}

type OpenFuture = Pin<Box<dyn Future<Output = DFResult<Option<LogSegmentScan>>> + Send>>;

/// Fetch one segment's bytes and open its pruned, column-projected scan.
/// `Ok(None)` means the catalog summary proved the segment irrelevant, with no
/// GET issued.
fn open_segment(ctx: Arc<PartitionCtx>, seg: SegmentRef) -> OpenFuture {
    Box::pin(async move {
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                &ctx.accounting,
            )
            .await
            .map_err(SqlError::from)?;
        Ok(scan)
    })
}

enum LogScanState {
    /// Advance to the next segment of this partition, or finish.
    NextSegment,
    /// Awaiting one segment's GET and prune.
    Opening(OpenFuture),
    /// Draining one segment's surviving blocks through the columnar fast path
    /// (ADR-0099 decision 2). Entered only when the scan is statically eligible;
    /// a block carrying an `attrs_raw` overflow page falls this segment back to
    /// the row path via [`LogScanState::ReopenRows`].
    Columnar(Box<LogSegmentScan>),
    /// Draining one segment's surviving blocks through the row path
    /// ([`LogSegmentScan::next_block`]), rebuilding a [`LogRecord`] per row: the
    /// unchanged pre-ADR-0099 path, taken by an ineligible scan or by the
    /// `attrs_raw` fallback. `skip` blocks are drained and discarded first, to
    /// step past the blocks a fallback already emitted columnar before it hit
    /// the overflow page (0 for an ineligible scan that never ran the fast path).
    Rows {
        scan: Box<LogSegmentScan>,
        skip: usize,
    },
    /// Re-opening the current segment to restart it on the row path after a
    /// block turned out to carry an `attrs_raw` overflow page. `skip` is the
    /// number of blocks already emitted columnar for this segment, which the
    /// re-opened row scan drains and discards so no row is emitted twice.
    ReopenRows {
        fut: OpenFuture,
        skip: usize,
    },
    Done,
}

/// The block currently being drained into output batches, and the form it is
/// held in. The reservation charge tracked by [`LogScanStream::held`] covers
/// whichever variant is live.
enum Pending {
    /// Nothing held.
    None,
    /// Row path: the block's surviving records, drained `BATCH_ROWS` at a time
    /// from `pos`.
    Rows { records: Vec<LogRecord>, pos: usize },
    /// Columnar fast path: the block's already-built output batches, emitted one
    /// per poll. Built whole from the [`ColumnarBlockView`] so the view (which
    /// borrows the scan) is dropped before the next block is decoded. The
    /// decoded block itself is still resident behind the reader until then, so
    /// the charge covering this variant includes it (see
    /// [`LogScanStream::hold_batches`]).
    Batches(VecDeque<RecordBatch>),
}

/// Per-partition record-batch stream (ADR-0087 decisions 1 and 2).
///
/// Holds at most one segment's decoded block plus the batch built from it, and
/// charges the query's memory pool for exactly that: `held` is the reservation
/// covering `pending`, `emitted` the reservation covering the batch handed
/// downstream on the previous poll. Both are released as their data goes away,
/// so the reservation tracks live resident memory rather than cumulative output
/// and a pool overrun surfaces as `ResourcesExhausted` at the moment the scan
/// genuinely holds too much.
///
/// The reservation lives on the stream (not on the state) so it is the same one
/// for the partition's lifetime and frees exactly once on drop.
struct LogScanStream {
    schema: SchemaRef,
    /// Indices into the resolved full schema to emit, in output order.
    projection: Arc<Vec<usize>>,
    /// The tenant's declared typed attribute columns (ADR-0090), consulted by
    /// [`build_batch`] and [`build_columnar_batches`] for a projected declared
    /// index.
    declared: Arc<Vec<DeclaredColumn>>,
    ctx: Arc<PartitionCtx>,
    erasure: Arc<Vec<ErasurePredicate>>,
    /// Whether this scan may attempt the columnar fast path (the query-shape
    /// clauses of [`columnar_static_eligible`]). When false, every segment
    /// drains the row path.
    columnar_eligible: bool,
    blocks: BlockMetrics,
    segments: VecDeque<SegmentRef>,
    reservation: MemoryReservation,
    /// Reservation bytes currently covering `pending`.
    held: usize,
    /// Reservation bytes currently covering the batch emitted last poll.
    emitted: usize,
    /// The block being drained into batches, in row or columnar form.
    pending: Pending,
    /// The segment currently being drained, kept so the `attrs_raw` fallback can
    /// re-open it on the row path. Set when a segment's open resolves.
    current_seg: Option<SegmentRef>,
    /// How many blocks of the current segment the columnar fast path has already
    /// emitted. The `attrs_raw` fallback re-opens the segment and skips this many
    /// blocks so none is emitted twice. Reset when a new segment starts draining.
    seg_columnar_blocks: usize,
    state: LogScanState,
}

impl LogScanStream {
    /// Emit the next row-path batch out of `pending`, moving the reservation
    /// with it: the previous batch's charge is released (it is downstream's
    /// now), the new batch's charge is taken before it is handed over.
    fn emit_next_row_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let Pending::Rows { records, pos } = &mut self.pending else {
            return Err(DataFusionError::Internal(
                "emit_next_row_batch called without a row block held".into(),
            ));
        };
        let end = (*pos + BATCH_ROWS).min(records.len());
        let batch = build_batch(
            &records[*pos..end],
            Arc::clone(&self.schema),
            &self.projection,
            &self.declared,
        )?;
        *pos = end;
        let bytes = batch.get_array_memory_size();
        self.reservation.try_grow(bytes)?;
        self.emitted = bytes;
        self.blocks.rowpath_batches.add(1);
        Ok(batch)
    }

    /// Emit the next columnar-path batch: pop the front pre-built batch, relabel
    /// its already-reserved bytes from `held` to `emitted`, and release the
    /// batch handed out on the previous poll. No new reservation is taken -- the
    /// decoded block and all of its batches were charged once in
    /// [`Self::hold_batches`], and the block's share stays in `held` until the
    /// block is released.
    fn emit_next_columnar_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let Pending::Batches(queue) = &mut self.pending else {
            return Err(DataFusionError::Internal(
                "emit_next_columnar_batch called without columnar batches held".into(),
            ));
        };
        let Some(batch) = queue.pop_front() else {
            return Err(DataFusionError::Internal(
                "emit_next_columnar_batch called on an empty queue".into(),
            ));
        };
        let bytes = batch.get_array_memory_size();
        // The batch's bytes were reserved as part of `held`; moving it
        // downstream relabels that charge rather than growing or releasing it.
        self.held = self.held.saturating_sub(bytes);
        self.emitted = bytes;
        self.blocks.columnar_batches.add(1);
        Ok(batch)
    }

    /// Take ownership of one decoded block's records (row path), charging the
    /// pool for `records_memory` before it is held. An empty block (every row
    /// filtered out) charges nothing and leaves the stream to ask for the next.
    fn hold_block(&mut self, records: Vec<LogRecord>) -> DFResult<()> {
        let bytes = records_memory(&records);
        self.reservation.try_grow(bytes)?;
        self.held = bytes;
        self.pending = Pending::Rows { records, pos: 0 };
        Ok(())
    }

    /// Take ownership of one block's pre-built columnar batches, charging the
    /// pool for everything the fast path holds while they drain: their total
    /// Arrow footprint **plus** `block_bytes`, the decoded block's own heap
    /// footprint ([`ColumnarBlockView::decoded_bytes`]).
    ///
    /// Both terms are live at the same time, which is why both are charged. The
    /// batches were built from a view borrowing the decoded block, and
    /// `BlockScan` releases that block only when the next one is decoded -- so
    /// for as long as this stream is emitting these batches, the block is
    /// resident too. The batch term alone would report a fraction of the true
    /// footprint and break ADR-0087 decision 2's contract that the pool bounds
    /// concurrently-held scan memory.
    ///
    /// The block's share of the charge stays in [`Self::held`] until
    /// [`Self::release_block`]: [`Self::emit_next_columnar_batch`] moves only
    /// the emitted batch's bytes from `held` to `emitted`.
    fn hold_batches(&mut self, batches: Vec<RecordBatch>, block_bytes: usize) -> DFResult<()> {
        let batch_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let bytes = batch_bytes.saturating_add(block_bytes);
        self.reservation.try_grow(bytes)?;
        self.held = bytes;
        self.pending = Pending::Batches(batches.into());
        Ok(())
    }

    /// True when the current block still has a batch to emit.
    fn has_pending(&self) -> bool {
        match &self.pending {
            Pending::None => false,
            Pending::Rows { records, pos } => *pos < records.len(),
            Pending::Batches(queue) => !queue.is_empty(),
        }
    }

    /// Drop the drained block and release its charge.
    fn release_block(&mut self) {
        self.reservation.shrink(std::mem::take(&mut self.held));
        self.pending = Pending::None;
    }

    /// Abandon the stream on error, releasing everything the scan still holds.
    fn fail(&mut self, e: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.state = LogScanState::Done;
        self.release_block();
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        Poll::Ready(Some(Err(e)))
    }

    /// Take one decoded block's surviving records through the row path: apply
    /// scan-layer selective-erasure exclusion (ADR-0064) and hold the records.
    ///
    /// The exclusion here is authoritative because it sees the same merged
    /// `attrs` view the surface returns (resource + scope + record), so a
    /// subject named only in a resource/scope attribute is dropped; the
    /// fetcher-level filter matches per-record attributes alone and cannot see
    /// it. An empty `records` is normal, not end-of-segment: a block can survive
    /// pruning and hold no matching row, or have every matching row erased.
    fn take_row_block(&mut self, mut records: Vec<LogRecord>) -> DFResult<()> {
        retain_unerased(&mut records, &self.erasure)?;
        self.hold_block(records)
    }
}

impl Stream for LogScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // Anything buffered from the current block goes out first.
            if this.has_pending() {
                let emitted = match &this.pending {
                    Pending::Rows { .. } => this.emit_next_row_batch(),
                    Pending::Batches(_) => this.emit_next_columnar_batch(),
                    Pending::None => unreachable!("has_pending() ruled this out"),
                };
                return match emitted {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(e) => this.fail(e),
                };
            }
            // The block is drained: release it before decoding another, so the
            // reservation never covers two blocks at once.
            if this.held > 0 || !matches!(this.pending, Pending::None) {
                this.release_block();
            }

            match &mut this.state {
                LogScanState::NextSegment => match this.segments.pop_front() {
                    Some(seg) => {
                        this.current_seg = Some(seg.clone());
                        this.seg_columnar_blocks = 0;
                        this.state =
                            LogScanState::Opening(open_segment(Arc::clone(&this.ctx), seg));
                    }
                    None => {
                        this.state = LogScanState::Done;
                    }
                },
                LogScanState::Opening(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.state = if this.columnar_eligible {
                            LogScanState::Columnar(Box::new(scan))
                        } else {
                            LogScanState::Rows {
                                scan: Box::new(scan),
                                skip: 0,
                            }
                        };
                    }
                    // The segment's ts span could not satisfy the query: no GET
                    // was issued and there is nothing to drain.
                    Poll::Ready(Ok(None)) => this.state = LogScanState::NextSegment,
                    Poll::Ready(Err(e)) => return this.fail(e),
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::ReopenRows { fut, skip } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        let skip = *skip;
                        this.state = LogScanState::Rows {
                            scan: Box::new(scan),
                            skip,
                        };
                    }
                    // Cannot happen for a segment already opened once this scan;
                    // treat a vanished segment as end-of-segment rather than
                    // panicking.
                    Poll::Ready(Ok(None)) => this.state = LogScanState::NextSegment,
                    Poll::Ready(Err(e)) => return this.fail(e),
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Columnar(scan) => {
                    // One of three outcomes, folded into an owned step so the
                    // view (which borrows `scan`) is dropped before `this.state`
                    // or the reservation is touched.
                    enum Step {
                        Exhausted(ScanStats),
                        // A block carrying an `attrs_raw` overflow page (or, only
                        // defensively, an unexpected pending erasure): fall the
                        // rest of this segment back to the row path.
                        Fallback,
                        // A clean block's built batches (possibly empty for a
                        // block with no surviving row), and the decoded block's
                        // own heap footprint, read off the view before its
                        // borrow ends because the block stays resident behind
                        // the reader while those batches drain.
                        Held {
                            batches: Vec<RecordBatch>,
                            block_bytes: usize,
                        },
                        // A decode or build error, carried out of the view's
                        // borrow so it can be handled once the borrow ends.
                        Failed(DataFusionError),
                    }
                    // The view borrows `scan`, so every outcome is folded into an
                    // owned `Step` here; `this.fail`/`this.state` are only touched
                    // after the match, once that borrow has ended.
                    let step = match scan.next_block_columnar() {
                        Ok(ColumnarBlockOutcome::Exhausted) => Step::Exhausted(scan.stats()),
                        // The fast path is only entered with no erasure, so this
                        // is unreachable in practice; fall back rather than
                        // risk serving an erased record columnar.
                        Ok(ColumnarBlockOutcome::ErasurePending) => Step::Fallback,
                        Ok(ColumnarBlockOutcome::Block(view)) => {
                            if view.has_attrs_raw_page() {
                                Step::Fallback
                            } else {
                                match build_columnar_batches(
                                    &view,
                                    &this.schema,
                                    &this.projection,
                                    &this.declared,
                                ) {
                                    Ok(batches) => Step::Held {
                                        batches,
                                        block_bytes: view.decoded_bytes(),
                                    },
                                    Err(e) => Step::Failed(e),
                                }
                            }
                        }
                        Err(e) => Step::Failed(SqlError::from(e).into()),
                    };
                    match step {
                        Step::Failed(e) => return this.fail(e),
                        Step::Exhausted(stats) => {
                            this.blocks.record(&stats);
                            this.state = LogScanState::NextSegment;
                        }
                        Step::Fallback => {
                            // Re-open the segment and restart it on the row path,
                            // skipping the blocks already emitted columnar so no
                            // row is emitted twice. The abandoned columnar scan's
                            // partial counters are dropped; the row scan re-counts
                            // the whole segment.
                            let seg = match this.current_seg.clone() {
                                Some(seg) => seg,
                                None => {
                                    return this.fail(DataFusionError::Internal(
                                        "attrs_raw fallback with no current segment".into(),
                                    ));
                                }
                            };
                            let fut = open_segment(Arc::clone(&this.ctx), seg);
                            this.state = LogScanState::ReopenRows {
                                fut,
                                skip: this.seg_columnar_blocks,
                            };
                        }
                        Step::Held {
                            batches,
                            block_bytes,
                        } => {
                            // Count every consumed clean block, empty or not, so
                            // a later `attrs_raw` fallback skips exactly the
                            // blocks the columnar cursor advanced past.
                            this.seg_columnar_blocks += 1;
                            // A block with no surviving row is not held at all:
                            // the loop asks for the next block immediately,
                            // which releases it inside this same poll, so there
                            // is no interval during which it is resident and
                            // uncharged.
                            if !batches.is_empty()
                                && let Err(e) = this.hold_batches(batches, block_bytes)
                            {
                                return this.fail(e);
                            }
                        }
                    }
                }
                LogScanState::Rows { scan, skip } => {
                    // Drain and discard the blocks a columnar fallback already
                    // emitted, then hold the next block's records.
                    if *skip > 0 {
                        match scan.next_block() {
                            Ok(Some(_)) => *skip -= 1,
                            Ok(None) => {
                                let stats = scan.stats();
                                this.blocks.record(&stats);
                                this.state = LogScanState::NextSegment;
                            }
                            Err(e) => return this.fail(SqlError::from(e).into()),
                        }
                        continue;
                    }
                    match scan.next_block() {
                        Ok(Some(records)) => {
                            if let Err(e) = this.take_row_block(records) {
                                return this.fail(e);
                            }
                        }
                        // Only `None` ends the segment. Its counters are final
                        // now, so publish them before moving on.
                        Ok(None) => {
                            let stats = scan.stats();
                            this.blocks.record(&stats);
                            this.state = LogScanState::NextSegment;
                        }
                        Err(e) => return this.fail(SqlError::from(e).into()),
                    }
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

/// Decode a slice of records into one [`RecordBatch`] over `schema`, which is
/// `logs_schema_with_declared(&declared)` projected by `projection`.
///
/// Only the projected columns are built. A column the query did not ask for is
/// never materialized here, which is the second half of the projection story:
/// the reader does not decode its pages ([`resolve_columns`]) and this does not
/// allocate an Arrow array for it. `attrs` is the expensive one to skip -- at a
/// hundred attributes a row it dominated the batch's footprint even for
/// `COUNT(*)`, which projects nothing at all.
///
/// The merged attribute view ([`merged_attrs`]) is computed **once per record**
/// (ADR-0090 decision 5) into `merged`, shared by the `attrs` map arm and every
/// declared-column arm, rather than decoded again per declared column. A
/// declared column (schema index >= [`FIRST_DECLARED_COL`]) is built as a
/// native typed Arrow array from that precomputed view via [`find_attr`]:
/// NULL for an absent key or a variant that does not match the declared type,
/// never a cast (ADR-0090 decision 7), with the one `bytes` normalization of a
/// `List`/`Map` value through [`canonical_value_bytes`].
fn build_batch(
    records: &[LogRecord],
    schema: SchemaRef,
    projection: &[usize],
    declared: &[DeclaredColumn],
) -> DFResult<RecordBatch> {
    // Precompute the merged attribute view once per record when any projected
    // column needs it -- the `attrs` map or any declared typed column. Hoisted
    // out of the per-column loop so a query projecting `attrs` and several
    // declared columns decodes each record's stream_attrs blob exactly once.
    let needs_merged = projection
        .iter()
        .any(|&i| i == LOG_COL_ATTRS || i >= FIRST_DECLARED_COL);
    let merged: Vec<Vec<(String, AttrValue)>> = if needs_merged {
        let mut v = Vec::with_capacity(records.len());
        for r in records {
            v.push(merged_attrs(r)?);
        }
        v
    } else {
        Vec::new()
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &i in projection {
        let array: ArrayRef = match i {
            LOG_COL_TS => Arc::new(TimestampNanosecondArray::from(
                records.iter().map(|r| r.ts_ns).collect::<Vec<_>>(),
            )),
            LOG_COL_OBSERVED_TS => Arc::new(TimestampNanosecondArray::from(
                records.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_NUM => Arc::new(UInt8Array::from(
                records.iter().map(|r| r.severity_num).collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_TEXT => Arc::new(StringArray::from(
                records
                    .iter()
                    .map(|r| r.severity_text.as_str())
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_BODY => Arc::new(StringArray::from(
                records.iter().map(|r| r.body.as_str()).collect::<Vec<_>>(),
            )),
            LOG_COL_TRACE_ID => {
                let mut trace =
                    FixedSizeBinaryBuilder::with_capacity(records.len(), TRACE_ID_WIDTH);
                for r in records {
                    match &r.trace_id {
                        Some(id) => trace.append_value(id).map_err(|e| {
                            SqlError::Internal(format!("trace_id array build: {e}"))
                        })?,
                        None => trace.append_null(),
                    }
                }
                Arc::new(trace.finish())
            }
            LOG_COL_SPAN_ID => {
                let mut span = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
                for r in records {
                    match &r.span_id {
                        Some(id) => span
                            .append_value(id)
                            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
                        None => span.append_null(),
                    }
                }
                Arc::new(span.finish())
            }
            LOG_COL_FLAGS => Arc::new(UInt32Array::from(
                records.iter().map(|r| r.flags).collect::<Vec<_>>(),
            )),
            // `attrs` map: each record's stream-identity (resource + scope)
            // attributes merged with its dynamic per-record attributes, values
            // rendered to text. DataFusion's mandatory `Inexact` residual
            // re-applies `attrs['k'] = 'v'` against this column, and that
            // residual is the sole exactness mechanism, so the column must carry
            // the fully merged view. Populating it from `r.attrs` alone silently
            // dropped every record whose matched attribute was a genuine
            // resource attribute (ADR-0033 amendment). See `merged_attrs`.
            LOG_COL_ATTRS => {
                let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                for row in &merged {
                    for (k, v) in row {
                        attrs.keys().append_value(k);
                        attrs.values().append_value(attr_value_to_string(v));
                    }
                    attrs
                        .append(true)
                        .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
                }
                Arc::new(attrs.finish())
            }
            // A declared typed attribute column (ADR-0090 decisions 5-7):
            // index >= FIRST_DECLARED_COL selects `declared[i - FIRST_DECLARED_COL]`.
            // The declared key is still present in the `attrs` map arm above
            // (decision 6, keys stay in the map); this arm additionally
            // materializes it as a native typed column from the same merged
            // view.
            other => match other
                .checked_sub(FIRST_DECLARED_COL)
                .and_then(|k| declared.get(k))
            {
                Some(dc) => declared_column_array(dc, &merged),
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
        };
        columns.push(array);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    // The row count is carried explicitly: an empty projection (what a bare
    // `COUNT(*)` asks for, and the cheapest case this change exists to make
    // work) has no column to infer it from, and inferring zero rows there would
    // silently lose every row.
    let options = RecordBatchOptions::new().with_row_count(Some(records.len()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(DataFusionError::from)
}

/// Build one declared typed attribute column as a native Arrow array from the
/// per-record precomputed merged views (ADR-0090 decisions 5-7).
///
/// For each record, the key is looked up via [`find_attr`] against that record's
/// merged view. A value whose [`AttrValue`] variant matches the declared type is
/// appended natively; every other case -- an absent key, or a present value of a
/// different variant -- appends NULL, never a cast and never an error. The one
/// exception is a `Bytes`-declared column: a `List`/`Map` value is first
/// normalized to its canonical encoding via [`canonical_value_bytes`] (the same
/// function the write path uses in `ravel_logseg::record::resolve_value`), so a
/// value that fit the object's dynamic-column budget and was stored as a `Bytes`
/// column reads identically to the same logical value that overflowed into
/// `attrs_raw` and decoded back as `List`/`Map`.
fn declared_column_array(dc: &DeclaredColumn, merged: &[Vec<(String, AttrValue)>]) -> ArrayRef {
    match dc.ty {
        DeclaredType::Str => {
            let mut b = StringBuilder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::I64 => {
            let mut b = Int64Builder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::I64(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Bytes(bytes)) => b.append_value(bytes),
                    // A record-level `List`/`Map` value that fit the dynamic-
                    // column budget was canonicalized into a `Bytes` column at
                    // write time; one that overflowed decodes back as
                    // `List`/`Map`. Canonicalize the latter here so both storage
                    // locations produce the identical `bytes` value (decision 7).
                    Some(v @ (AttrValue::List(_) | AttrValue::Map(_))) => {
                        b.append_value(canonical_value_bytes(v))
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

// ---------------------------------------------------------------------------
// Columnar fast path (ADR-0099 decision 2)
// ---------------------------------------------------------------------------

/// One declared column's FIELD_DIR resolution for a block, done once via
/// [`ColumnarBlockView::resolve_attr`] rather than per row via
/// [`find_attr`] (ADR-0099 decision 2).
struct DeclaredPlan<'d> {
    dc: &'d DeclaredColumn,
    /// The FIELD_DIR column carrying this key at the declared type, if any. A
    /// record row whose value lives here reads that value; a row whose value
    /// lives in a different-typed column of the same key reads NULL (record
    /// wins, wrong variant), matching the row path exactly.
    matching: Option<AttrColumn>,
    /// Every FIELD_DIR column for this key, across all stored types. Used only
    /// to answer "does this record set the key at all" so record-wins precedence
    /// matches the merged view: if the record sets the key (any type), the
    /// resource/scope fallback is not consulted.
    all_cols: Vec<AttrColumn>,
}

/// The [`FieldType`] a declared column resolves its FIELD_DIR column at. A
/// `match` (not a two-arm `if`) so a future declared `f64` (ADR-0090, deferred)
/// slots in as one more arm rather than silently falling through.
fn declared_field_type(ty: DeclaredType) -> FieldType {
    match ty {
        DeclaredType::Str => FieldType::Str,
        DeclaredType::I64 => FieldType::I64,
        DeclaredType::Bool => FieldType::Bool,
        DeclaredType::Bytes => FieldType::Bytes,
    }
}

/// Whether a FIELD_DIR column carries a *readable* value at surviving row `i`.
///
/// "Readable" is what makes this agree with the row path: a `Str` cell holding
/// bytes that are not UTF-8 is not a value there
/// (`get_attr_value`/`read_typed_cell`), so it must not count as the record
/// setting the key either -- otherwise it would suppress the resource/scope
/// fallback the row path applies.
fn attr_present(view: &ColumnarBlockView<'_>, col: AttrColumn, i: usize) -> bool {
    match col.ty {
        FieldType::Str => read_str_cell(view, col.column_id, i).is_some(),
        FieldType::Bytes => view.bytes_at(col.column_id, i).is_some(),
        FieldType::I64 => view.i64_at(col.column_id, i).is_some(),
        FieldType::F64 => view.f64_bits_at(col.column_id, i).is_some(),
        FieldType::Bool => view.bool_at(col.column_id, i).is_some(),
    }
}

/// The block's per-`stream_ref` decoded resource/scope scalar attributes, cached
/// so a block's streams are each decoded once even though the fallback is a
/// per-row lookup. This is the fast path's only stream-blob decode, reached only
/// for a declared key a record row does not set in a FIELD_DIR column; a query
/// whose declared keys are all record attributes never enters it.
fn resource_attrs<'c>(
    view: &ColumnarBlockView<'_>,
    cache: &'c mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    stream_ref: u32,
) -> DFResult<&'c Arc<Vec<(String, AttrValue)>>> {
    match cache.entry(stream_ref) {
        std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
        std::collections::hash_map::Entry::Vacant(e) => {
            let blob = view.stream_attrs_of(stream_ref).ok_or_else(|| {
                DataFusionError::from(SqlError::CorruptStreamAttrs(
                    "columnar fast path: stream_ref has no STREAM_DIR entry".to_string(),
                ))
            })?;
            let decoded = Arc::new(decode_stream_attrs(blob)?);
            Ok(e.insert(decoded))
        }
    }
}

/// The merged value of a declared key at surviving row `i`, under the same
/// record-wins-over-resource precedence the row path's [`merged_attrs`] +
/// [`find_attr`] produce: the record's own value if it sets the key (in any
/// FIELD_DIR column), otherwise the resource/scope scalar.
///
/// Returns the record-column value directly when the record sets the key at the
/// declared type; `None` when the record sets it at a different type (wrong
/// variant, NULL by ADR-0090 decision 7). Only when the record does not set the
/// key at all is the resource/scope fallback consulted, returning a cloned
/// [`AttrValue`] whose variant the caller checks against the declared type.
fn declared_merged_value(
    view: &ColumnarBlockView<'_>,
    plan: &DeclaredPlan<'_>,
    i: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<Option<AttrValue>> {
    let record_sets_key = plan.all_cols.iter().any(|&c| attr_present(view, c, i));
    if record_sets_key {
        let Some(mc) = plan.matching else {
            return Ok(None);
        };
        return Ok(read_typed_cell(view, mc, i));
    }
    let Some(stream_ref) = view.stream_ref(i) else {
        return Ok(None);
    };
    let resource = resource_attrs(view, cache, stream_ref)?;
    Ok(find_attr(resource, &plan.dc.key).cloned())
}

/// A `Str`-typed FIELD_DIR cell at surviving row `i`, as `&str`: `None` when the
/// cell is NULL **or** when its bytes are not UTF-8.
///
/// Treating invalid UTF-8 as no value is what the row path does
/// (`get_attr_value`'s `String::from_utf8(b).ok()`, `ravel-logseg`'s
/// `reader.rs`), which makes the attribute absent for that record and lets the
/// resource/scope value show through. Substituting U+FFFD instead would both
/// invent a value and suppress that fallback, and this crate's rule is exact
/// semantics by default.
fn read_str_cell<'v>(view: &ColumnarBlockView<'v>, column_id: u32, i: usize) -> Option<&'v str> {
    std::str::from_utf8(view.bytes_at(column_id, i)?).ok()
}

/// Read the value of a FIELD_DIR column at surviving row `i` as an
/// [`AttrValue`], or `None` when the cell is NULL (or, for `Str`, not UTF-8;
/// see [`read_str_cell`]).
fn read_typed_cell(view: &ColumnarBlockView<'_>, col: AttrColumn, i: usize) -> Option<AttrValue> {
    match col.ty {
        FieldType::Str => {
            read_str_cell(view, col.column_id, i).map(|s| AttrValue::Str(s.to_string()))
        }
        FieldType::I64 => view.i64_at(col.column_id, i).map(AttrValue::I64),
        FieldType::F64 => view
            .f64_bits_at(col.column_id, i)
            .map(|bits| AttrValue::F64(f64::from_bits(bits))),
        FieldType::Bool => view.bool_at(col.column_id, i).map(AttrValue::Bool),
        FieldType::Bytes => view
            .bytes_at(col.column_id, i)
            .map(|b| AttrValue::Bytes(b.to_vec())),
    }
}

/// Build one declared typed attribute column for surviving rows `start..end`
/// straight from the view (ADR-0099 decision 2). Byte-identical to
/// [`declared_column_array`] over the same input: a value whose variant matches
/// the declared type is appended natively, and every other case -- absent key,
/// or a value of a different variant -- appends NULL, never a cast (ADR-0090
/// decision 7). The `Bytes` arm applies the same `List`/`Map` canonicalization.
///
/// The `match` on the declared type mirrors [`declared_column_array`], so a
/// future declared `f64` slots in as one arm on both paths.
fn build_declared_columnar_array(
    view: &ColumnarBlockView<'_>,
    plan: &DeclaredPlan<'_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    Ok(match plan.dc.ty {
        DeclaredType::Str => {
            let mut b = StringBuilder::new();
            for i in start..end {
                match declared_merged_value(view, plan, i, cache)? {
                    Some(AttrValue::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::I64 => {
            let mut b = Int64Builder::new();
            for i in start..end {
                match declared_merged_value(view, plan, i, cache)? {
                    Some(AttrValue::I64(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::new();
            for i in start..end {
                match declared_merged_value(view, plan, i, cache)? {
                    Some(AttrValue::Bool(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            for i in start..end {
                match declared_merged_value(view, plan, i, cache)? {
                    Some(AttrValue::Bytes(bytes)) => b.append_value(bytes),
                    // Parity with the row path: a resource/scope `List`/`Map`
                    // value is canonicalized. In the eligible (no `attrs_raw`)
                    // case a record's `List`/`Map` is already stored as a
                    // canonicalized `Bytes` column, and `decode_stream_attrs`
                    // omits nested resource values, so this arm is effectively
                    // dead here; it is kept identical to `declared_column_array`.
                    Some(v @ (AttrValue::List(_) | AttrValue::Map(_))) => {
                        b.append_value(canonical_value_bytes(&v))
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// A UTF-8 log field (`body`, `severity_text`) read from the view; a violation
/// is the same client-visible corruption class the row path's `string_from_bytes`
/// produces, never a panic or silently-wrong data.
fn view_str(bytes: &[u8]) -> DFResult<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| SqlError::CorruptStreamAttrs("log text field not utf-8".to_string()).into())
}

/// Build all of one block's output batches straight from its columnar view
/// (ADR-0099 decision 2), chunked at [`BATCH_ROWS`] exactly as the row path
/// chunks its records, so the two paths' batches are byte-identical. Returns an
/// empty vec for a block with no surviving row. The view borrows the scan, so
/// the whole block is built here and the batches handed back owned, letting the
/// caller drop the view before decoding the next block.
fn build_columnar_batches(
    view: &ColumnarBlockView<'_>,
    schema: &SchemaRef,
    projection: &[usize],
    declared: &[DeclaredColumn],
) -> DFResult<Vec<RecordBatch>> {
    let n = view.surviving_count();
    // Resolve each projected declared column's FIELD_DIR column once for the
    // whole block (ADR-0099 decision 2), not per row and not per chunk.
    let mut plans: HashMap<usize, DeclaredPlan> = HashMap::new();
    for &idx in projection {
        if idx >= FIRST_DECLARED_COL
            && let Some(dc) = declared.get(idx - FIRST_DECLARED_COL)
        {
            plans.insert(
                idx,
                DeclaredPlan {
                    dc,
                    matching: view.resolve_attr(&dc.key, declared_field_type(dc.ty)),
                    all_cols: view.attr_columns_for(&dc.key).collect(),
                },
            );
        }
    }
    let mut cache: HashMap<u32, Arc<Vec<(String, AttrValue)>>> = HashMap::new();
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + BATCH_ROWS).min(n);
        out.push(build_columnar_batch(
            view, schema, projection, &plans, &mut cache, start, end,
        )?);
        start = end;
    }
    Ok(out)
}

/// Build one output batch for surviving rows `start..end` from the view, one
/// array per projected column. The column set is the same eligible set
/// [`columnar_static_eligible`] admits: fixed columns and declared typed
/// columns, never the `attrs` map.
#[allow(clippy::too_many_arguments)]
fn build_columnar_batch(
    view: &ColumnarBlockView<'_>,
    schema: &SchemaRef,
    projection: &[usize],
    plans: &HashMap<usize, DeclaredPlan<'_>>,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    start: usize,
    end: usize,
) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &idx in projection {
        let array: ArrayRef = match idx {
            LOG_COL_TS => Arc::new(TimestampNanosecondArray::from(
                (start..end)
                    .map(|i| view.ts(i).unwrap_or_default())
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_OBSERVED_TS => Arc::new(TimestampNanosecondArray::from(
                (start..end)
                    .map(|i| view.observed_ts(i).unwrap_or_default())
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_NUM => Arc::new(UInt8Array::from(
                (start..end)
                    .map(|i| view.severity_num(i).unwrap_or_default() as u8)
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_TEXT => {
                let mut b = StringBuilder::new();
                for i in start..end {
                    match view.severity_text(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_BODY => {
                let mut b = StringBuilder::new();
                for i in start..end {
                    match view.body(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_TRACE_ID => {
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, TRACE_ID_WIDTH);
                for i in start..end {
                    match view.trace_id(i) {
                        Some(id) => b.append_value(id).map_err(|e| {
                            SqlError::Internal(format!("trace_id array build: {e}"))
                        })?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_SPAN_ID => {
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, SPAN_ID_WIDTH);
                for i in start..end {
                    match view.span_id(i) {
                        Some(id) => b
                            .append_value(id)
                            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_FLAGS => Arc::new(UInt32Array::from(
                (start..end)
                    .map(|i| view.flags(i).unwrap_or_default() as u32)
                    .collect::<Vec<_>>(),
            )),
            // Ruled out by `columnar_static_eligible`; a projection reaching the
            // fast path never carries the `attrs` map.
            LOG_COL_ATTRS => {
                return Err(DataFusionError::Internal(
                    "columnar fast path reached with an attrs map projection".into(),
                ));
            }
            other => match plans.get(&other) {
                Some(plan) => build_declared_columnar_array(view, plan, start, end, cache)?,
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs columnar scan projection index {other} out of range"
                    )));
                }
            },
        };
        columns.push(array);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    // Carry the row count explicitly so an empty projection (a bare `COUNT(*)`)
    // still reports its rows, exactly as the row path does.
    let options = RecordBatchOptions::new().with_row_count(Some(end - start));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(DataFusionError::from)
}
