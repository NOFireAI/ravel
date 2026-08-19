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

use std::collections::VecDeque;
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
use ravel_logseg::{ColumnSelection, FieldSel, LogRecord, Predicate, ScanStats};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{LogQuery, LogSegmentFetcher, LogSegmentScan};
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
use crate::rlog_attrs::{attr_value_to_string, find_attr, merged_attrs, retain_unerased};
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
        Predicate::TsRange { .. } | Predicate::StreamIn(_) => sel,
        Predicate::HasWord { field, .. } | Predicate::Equals { field, .. } => match field {
            FieldSel::Body => sel.with_body(),
            FieldSel::SeverityText => sel.with_severity_text(),
            FieldSel::Attr(name) => sel.with_attr(name.clone()),
        },
    }
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
            blocks: BlockMetrics::new(&self.metrics, partition),
            segments,
            reservation,
            held: 0,
            emitted: 0,
            pending: Vec::new(),
            pos: 0,
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
    /// Draining one segment's surviving blocks, one at a time.
    Draining(Box<LogSegmentScan>),
    Done,
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
    /// [`build_batch`] for a projected declared index.
    declared: Arc<Vec<DeclaredColumn>>,
    ctx: Arc<PartitionCtx>,
    erasure: Arc<Vec<ErasurePredicate>>,
    blocks: BlockMetrics,
    segments: VecDeque<SegmentRef>,
    reservation: MemoryReservation,
    /// Reservation bytes currently covering `pending`.
    held: usize,
    /// Reservation bytes currently covering the batch emitted last poll.
    emitted: usize,
    /// The block being drained into batches.
    pending: Vec<LogRecord>,
    pos: usize,
    state: LogScanState,
}

impl LogScanStream {
    /// Build the next batch out of `pending`, moving the reservation with it:
    /// the previous batch's charge is released (it is downstream's now), the new
    /// batch's charge is taken before it is handed over.
    fn emit_next_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let end = (self.pos + BATCH_ROWS).min(self.pending.len());
        let batch = build_batch(
            &self.pending[self.pos..end],
            Arc::clone(&self.schema),
            &self.projection,
            &self.declared,
        )?;
        self.pos = end;
        let bytes = batch.get_array_memory_size();
        self.reservation.try_grow(bytes)?;
        self.emitted = bytes;
        Ok(batch)
    }

    /// Take ownership of one decoded block, charging the pool for it before it
    /// is held. An empty block (every row filtered out) charges nothing and
    /// leaves the stream to ask for the next one.
    fn hold_block(&mut self, records: Vec<LogRecord>) -> DFResult<()> {
        let bytes = records_memory(&records);
        self.reservation.try_grow(bytes)?;
        self.held = bytes;
        self.pending = records;
        self.pos = 0;
        Ok(())
    }

    /// Drop the drained block and release its charge.
    fn release_block(&mut self) {
        self.reservation.shrink(std::mem::take(&mut self.held));
        self.pending = Vec::new();
        self.pos = 0;
    }

    /// Abandon the stream on error, releasing everything the scan still holds.
    fn fail(&mut self, e: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.state = LogScanState::Done;
        self.release_block();
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        Poll::Ready(Some(Err(e)))
    }
}

impl Stream for LogScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // Anything buffered from the current block goes out first.
            if this.pos < this.pending.len() {
                return match this.emit_next_batch() {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(e) => this.fail(e),
                };
            }
            // The block is drained: release it before decoding another, so the
            // reservation never covers two blocks at once.
            if this.held > 0 {
                this.release_block();
            }

            match &mut this.state {
                LogScanState::NextSegment => match this.segments.pop_front() {
                    Some(seg) => {
                        this.state =
                            LogScanState::Opening(open_segment(Arc::clone(&this.ctx), seg));
                    }
                    None => {
                        this.state = LogScanState::Done;
                    }
                },
                LogScanState::Opening(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.state = LogScanState::Draining(Box::new(scan));
                    }
                    // The segment's ts span could not satisfy the query: no GET
                    // was issued and there is nothing to drain.
                    Poll::Ready(Ok(None)) => this.state = LogScanState::NextSegment,
                    Poll::Ready(Err(e)) => return this.fail(e),
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Draining(scan) => {
                    let decoded = scan.next_block().map_err(SqlError::from);
                    match decoded {
                        Ok(Some(mut records)) => {
                            // Scan-layer selective-erasure exclusion (ADR-0064).
                            // This is the authoritative exclusion because it
                            // sees the same merged `attrs` view the surface
                            // returns (resource + scope + record), so a subject
                            // named only in a resource/scope attribute is
                            // dropped; the fetcher-level filter matches
                            // per-record attributes alone and cannot see it.
                            if let Err(e) = retain_unerased(&mut records, &this.erasure) {
                                return this.fail(e);
                            }
                            // Every surviving record is emitted:
                            // stream-attribute equalities are not pushed (a
                            // stream-level prune is unsound against the merged
                            // `attrs` column), so nothing here narrows below
                            // what DataFusion's residual keeps.
                            //
                            // An empty `records` here is normal, not the end of
                            // the segment: a block can survive pruning and hold
                            // no row matching the exact filter, or have every
                            // matching row erased. The loop just asks for the
                            // next block.
                            if let Err(e) = this.hold_block(records) {
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
                        Err(e) => return this.fail(e.into()),
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
