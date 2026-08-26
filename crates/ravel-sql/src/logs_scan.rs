//! `LogsScanExec`: the leaf of the `logs` pipeline, the log-signal sibling of
//! [`crate::scan::RsegScanExec`] (ADR-0033).
//!
//! Partitions the snapshot's work across `target_partitions` partitions in one
//! of two modes, chosen once at construction from whether the fetcher carries
//! ADR-0046's read cache ([`LogSegmentFetcher::has_cache`], ADR-0102 amended by
//! #693):
//!
//! - **Cache-wired: intra-segment block striping.** Every `(segment,
//!   surviving-block)` unit across all segments is flattened into one ordered
//!   list (segment order, then block order) and unit `i` is assigned to
//!   partition `i % n`, where `n = target_partitions.max(1).
//!   min(total_block_count.max(1))`. This lets a query touching fewer segments
//!   than `target_partitions` still fan out to `target_partitions` partitions:
//!   the old segment-granular rule (`min(target_partitions, segment_count)`)
//!   pinned such a query to at most `segment_count` partitions.
//! - **Un-cached: segment-granular.** Each segment is assigned whole to one
//!   partition (relevant segment `j` in snapshot order to partition `j % n`),
//!   so every segment is opened by exactly one partition. The partition count is
//!   capped at the segment count.
//!
//! The block-striping fan-out is gated on the cache because without single-
//! flight to coalesce re-opens it would multiply object-store reads by the
//! partition count. See [`LogsScanExec::new`], [`owned_work`], and the
//! request-count paragraph below for why.
//!
//! The per-segment surviving-block counts the assignment needs are determined
//! once, before draining, by [`LogSegmentFetcher::plan_segment`] (a prune with
//! no block decode), shared across partitions through a
//! [`tokio::sync::OnceCell`]. Each partition then opens each segment it owns
//! blocks in through [`LogSegmentFetcher::scan_accounted_with_tenant_subset`]
//! (the same [`LogQuery`] and cache-aware GET as the whole-segment path,
//! restricted to that partition's own block-index list), decodes **one block
//! at a time**, and turns each block's [`ravel_logseg::LogRecord`]s into Arrow
//! arrays matching this scan's projection of [`crate::logs_schema::logs_schema`].
//!
//! # Object-store request count, and the cache gate
//!
//! Every partition that owns blocks in a segment opens that segment itself, so
//! it issues its own read sequence at that segment's key. The shape of that
//! sequence depends on the object's size (ADR-0107,
//! [`LogSegmentFetcher::with_block_range_threshold`], 512 KiB by default): at or
//! below the threshold it is one whole-object GET; above it, a suffix probe, one
//! GET per directory section, and coalesced GETs for the candidate blocks
//! skip-index pruning kept. What those reads cost depends on the cache, and so
//! does how many partitions are planned:
//!
//! - **Un-cached fetcher.** The assignment is segment-granular (#693): each
//!   segment is assigned whole to one partition ([`owned_work`]), so a segment
//!   is opened by exactly one partition and issues exactly one scan read
//!   sequence, whatever `target_partitions` is. The request count is one plan
//!   read per relevant segment (`compute_plan_counts`) plus one scan read per
//!   relevant segment, and the partition count is capped at the segment count
//!   (a query touching fewer segments than `target_partitions` cannot fan out
//!   past them, since a partition with no segment does no work). This is the
//!   pre-ADR-0102 whole-segment request count restored: without the cache there
//!   is no single-flight to coalesce re-opens, so striping a segment across
//!   partitions would multiply object-store reads by the partition count for no
//!   benefit. `ravel-bench`'s `logs_scan_scaling` report measures both.
//! - **Cache-wired fetcher.** The partition count is `target_partitions`, so
//!   several partitions can stripe one segment's blocks. Every GET of either
//!   shape is keyed by the extent it fetched and routed through the cache's
//!   single-flight -- the whole object below the threshold, and the probe, each
//!   section, and each block above it -- so the partitions striping one segment
//!   coalesce onto one request per distinct extent rather than one sequence
//!   each, and on the bench fixture the scan reads are flat across the whole
//!   `target_partitions` sweep. The plan reads do NOT coalesce with them: a
//!   [`tokio::sync::OnceCell`] barrier makes every partition await the whole
//!   plan pass before draining, so each plan read is the first, cold touch of
//!   its extent and completes before any scan read starts -- there is no
//!   concurrent in-flight GET for the scan to collapse onto, and an evicted plan
//!   entry is simply re-fetched by the scan (issue #691). Coalescing is not free
//!   of request count in general either: a read that misses the in-flight
//!   window, or finds its key already evicted, issues its own GET. So striding
//!   past the segment count *can* raise the GET count above the whole-segment
//!   path's, and that is the cost ADR-0102 accepts in exchange for the cache
//!   absorbing the repeat reads and for the extra scan parallelism.
//! - **Predicate-free full-window fast path (#693 part 3).** When the query has
//!   no block-level predicate, no pending erasure, and its window fully contains
//!   every relevant, above-threshold segment, and there are at least
//!   `target_partitions` such segments, the plan phase is skipped entirely
//!   ([`LogsScanExec::whole_segment_fast_path`]): whole segments are assigned
//!   round-robin (the same rule the un-cached path uses), and each is read in one
//!   whole-object GET ([`LogSegmentFetcher::scan_whole_accounted_with_tenant`]),
//!   so the request count is exactly one GET per relevant segment, zero suffix
//!   probes. When there are fewer relevant segments than partitions the striped
//!   path runs instead, but the plan footer it read is carried to each subset
//!   open ([`LogSegmentFetcher::fetch_object_with_footer`]) so those opens skip
//!   their own re-probe. Any other query shape runs the plan-then-stripe path
//!   above unchanged.
//!
//! Above the threshold each partition's read already covers only the pruned
//! candidate blocks rather than every byte of the segment, so bytes on the wire
//! are pruning-proportional; what stays per-partition is the object-sized
//! assembly buffer each one builds
//! (`crates/ravel-query/tests/log_block_range.rs` measures both).
//!
//! Any "no amplification" or "flat request count" figure reported for this path
//! (here, in `ravel-bench`'s `logs_scan_scaling` report, or in that bench's
//! smoke test) is a measurement of one fixture -- a `MemoryStore`, a specific
//! segment/block/partition shape, a cache sized to hold the whole dataset -- and
//! not a general result about striping. Cache size, eviction, object size, and
//! store latency all move it.
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
    ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, FixedSizeBinaryBuilder, Int32Array,
    Int64Builder, MapBuilder, StringArray, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use datafusion::arrow::datatypes::{Int32Type, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::Precision;
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
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt, TryStreamExt};
use ravel_catalog::SegmentRef;
use ravel_logseg::footer::LogFooter;
use ravel_logseg::{
    AttrColumn, ColumnSelection, ColumnarBlockView, FieldSel, FieldType, LogRecord, Predicate,
    ScanStats,
};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{ColumnarBlockOutcome, LogQuery, LogSegmentFetcher, LogSegmentScan};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use tokio::sync::OnceCell;

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
///   selection exactly like the content- and erasure-predicate contributors do.
///   Since #278/ADR-0093, an I64/Bool comparison or a Str/Bytes equality on a
///   declared column IS extracted into the prune-only channel
///   (`crate::logs_pushdown::extract_logs`) -- see that module's doc for the
///   exact allowlist. The declared column is still, separately, added to the
///   selection here regardless, because the prune channel narrows candidate
///   blocks but never substitutes for DataFusion's own Inexact residual
///   re-evaluation of the original predicate above the scan.
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
    /// Every segment in the snapshot, in snapshot order. Blocks are flattened
    /// across these (segment order, then block order) and striped across
    /// partitions; the per-segment block counts the striping needs are
    /// resolved lazily into [`Self::counts`].
    segments: Arc<Vec<SegmentRef>>,
    /// DataFusion partition count this scan declares. Equal to
    /// `target_partitions.max(1)`; the assignment stride `n` is
    /// `target_partitions.max(1).min(total_block_count.max(1))`, which only
    /// differs when there are fewer blocks than partitions, and then the
    /// partitions past `n` simply run empty streams (DataFusion tolerates
    /// them), so the observed non-empty partition count is identical either way.
    target_partitions: usize,
    /// The shared per-segment block plan (surviving-block counts and
    /// whole-segment prune stats), computed once by the first partition to
    /// poll and reused by the rest (ADR-0102). `None` entries are ts-irrelevant
    /// segments that issue no GET.
    counts: Arc<OnceCell<Arc<PlanCounts>>>,
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
    /// The assignment mode, decided once at construction from
    /// [`LogSegmentFetcher::has_cache`] (ADR-0102, amended by #693). `true` (a
    /// cache is wired) stripes a segment's surviving blocks across partitions;
    /// `false` assigns each segment whole to one partition, so an un-cached scan
    /// opens each segment exactly once. Threaded into the stream and
    /// [`owned_work`]; the same predicate gates `declared_partitions` above.
    stripe_blocks: bool,
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

    /// Accumulates one segment's *whole-segment* prune totals: `blocks_total`
    /// and `blocks_pruned_by_postings` (the drop across the postings step
    /// alone, `blocks_after_skip` minus `blocks_after_postings`, so it credits
    /// POSTINGS with nothing the skip index or the bloom did; `saturating_sub`
    /// because a degraded postings section leaves the two counts equal rather
    /// than ordered by construction).
    ///
    /// On the striped path this is recorded once per relevant segment by
    /// partition 0 during planning (ADR-0102), never per partition: several
    /// partitions stripe one segment's blocks, and each re-prunes the whole
    /// segment to open its own subset, so attributing the whole-segment totals
    /// per partition would multiply them. On the predicate-free full-window
    /// whole-segment fast path (#693 part 3) there is no plan phase, but each
    /// segment has exactly one owning partition, so its owner records these
    /// totals once at exhaustion straight from the scan's own stats -- still
    /// exactly once per segment. Either way the per-partition decode counts come
    /// from [`Self::record_scan`].
    fn record_segment_totals(&self, stats: &ScanStats) {
        self.total.add(stats.blocks_total as usize);
        self.pruned_by_postings.add(
            stats
                .blocks_after_skip
                .saturating_sub(stats.blocks_after_postings) as usize,
        );
    }

    /// Accumulates what one partition's cursor actually decoded: the blocks it
    /// scanned and the column pages it decoded or skipped. Per partition, unlike
    /// [`Self::record_segment_totals`], because each partition decodes only its
    /// own striped subset of a segment's blocks. Summed across every partition
    /// this equals what a single whole-segment scan would have reported for
    /// `blocks_scanned`/`pages_*`.
    fn record_scan(&self, stats: &ScanStats) {
        self.scanned.add(stats.blocks_scanned as usize);
        self.pages_decoded.add(stats.pages_decoded as usize);
        self.pages_skipped.add(stats.pages_skipped as usize);
    }
}

impl LogsScanExec {
    /// Build a scan over `segments`, striping their blocks round-robin across
    /// `target_partitions` partitions when `fetcher` carries ADR-0046's read
    /// cache and across `min(target_partitions, segments.len())` when it does
    /// not (see the `declared_partitions` comment below), with the given ts
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
        // Blocks, not segments, are what get striped (ADR-0102), but the
        // per-segment block counts are not known until a prune runs, which is
        // async and cannot happen in this synchronous constructor. So the
        // declared partition count is a function of `target_partitions` alone
        // and the block-level assignment (with the stride `n` capped by the real
        // block count) happens lazily on first poll, shared through `counts`.
        // When there are fewer blocks than partitions the surplus partitions run
        // empty, which is equivalent to capping `n` here and DataFusion handles
        // empty partitions.
        //
        // Striping past the segment count is gated on the fetcher carrying
        // ADR-0046's read cache, which is the precondition ADR-0102 decision 1
        // names for it: K partitions sharing one segment each open it
        // themselves, so each issues its own read sequence at that key -- one
        // whole-object GET at or below the ADR-0107 block-range threshold, and a
        // probe plus section and candidate-block ranges above it. With the cache
        // every GET of either shape is keyed by its extent and coalesces onto one
        // request through single-flight; without it each is a real request, and
        // requesting more partitions than there are segments would multiply
        // object-store GETs for no reason. So an un-cached fetcher falls back to
        // the pre-ADR-0102 bound, `min(target_partitions, segment_count)`, and
        // never plans a partition whose extra GETs nothing absorbs.
        // `stripe_blocks` decides both the partition count and the per-unit
        // assignment, and is computed once from the same `has_cache` predicate:
        // with the cache a segment's blocks stripe across partitions, without it
        // each segment is assigned whole to one partition (see `owned_work`).
        let stripe_blocks = fetcher.has_cache();
        let declared_partitions = if stripe_blocks {
            target_partitions.max(1)
        } else {
            target_partitions.max(1).min(segments.len().max(1))
        };
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
        let properties = Arc::new(Self::compute_properties(&schema, declared_partitions));
        Ok(LogsScanExec {
            tenant_hash,
            fetcher,
            segments: Arc::new(segments.to_vec()),
            target_partitions: declared_partitions,
            counts: Arc::new(OnceCell::new()),
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
            stripe_blocks,
            properties,
            accounting,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// No output ordering (ADR-0087 decision 1). A block-streaming scan emits a
    /// partition's blocks in stored order, which is `(stream_ref, ts)` within a
    /// block and, across a partition, whatever striped subset of segments'
    /// blocks it was assigned, so no `ts` ordering holds. Striping blocks rather
    /// than whole segments (ADR-0102) removes even the per-segment grouping a
    /// partition used to have, which only reinforces that no ordering can be
    /// declared. Downstream operators that need one get an explicit sort.
    /// Whether the sum of the snapshot's `SegmentRef::sample_count` values is
    /// the exact row count of this scan's output, and its ts span the exact
    /// min/max over every touched segment (issue #698, widened by issue #723).
    /// Both hold only when nothing between the committed counts and the emitted
    /// rows can remove a row:
    ///
    /// - the ts bound fully contains every resolved segment: `ts_min <=
    ///   seg.min_event_ts_ns && seg.max_event_ts_ns <= ts_max` (inclusive) for
    ///   every entry in `self.segments`, matching the fast-path containment
    ///   convention in `ravel_query::log_fetcher`. A bound that clips even one
    ///   segment removes rows the sum still counts (and leaves the true span
    ///   unknown), so it fails closed; reading a clipped segment's real count
    ///   from its block index is issue #721. The no-ts-bound case (`i64::MIN`/
    ///   `i64::MAX`, the sentinels a `None` `LogsPushdown::ts_lo`/`ts_hi` lower
    ///   to) is the trivial instance: every segment's span is inside
    ///   `[i64::MIN, i64::MAX]`. `self.segments` is already the ts-relevant
    ///   (overlapping) subset the provider resolved, so a fully-out-of-range
    ///   segment never reaches this check;
    /// - no content predicate (`LogsPushdown::content`, the `has_word` arms
    ///   evaluated exactly per row);
    /// - no attribute-equality prune predicate (`LogsPushdown::prune`); a prune
    ///   is block-only and widen-safe, so it never changes the row count, but a
    ///   present prune means a predicate was pushed, so treat it as not-exact
    ///   too and only claim the count for a truly predicate-free query;
    /// - no pending selective erasure (ADR-0064 decision 2): a pending erasure
    ///   predicate removes rows the committed counts still include, so the sum
    ///   overstates the answer. Fail closed.
    ///
    /// The projection is irrelevant to the count, so it is not consulted.
    /// `LogsPushdown` has exactly these four fields (`ts_lo`, `ts_hi`,
    /// `content`, `prune`); each is covered here, and `erasure` comes from the
    /// resolved snapshot rather than the pushdown.
    fn stats_are_exact(&self) -> bool {
        self.content.is_empty()
            && self.prune.is_empty()
            && self.erasure.is_empty()
            && self
                .segments
                .iter()
                .all(|seg| self.ts_min <= seg.min_event_ts_ns && seg.max_event_ts_ns <= self.ts_max)
    }

    /// Whether this scan can take the predicate-free full-window whole-segment
    /// fast path (#693 part 3, deliverable 1), returning the count of relevant
    /// (ts-overlapping) segments when it can. Decided with ZERO I/O from the
    /// resolved snapshot and `query`, using the same conjuncts
    /// [`ravel_query::LogSegmentFetcher::plan_segment`]'s own fast-path gate uses
    /// per segment:
    ///
    /// - the query carries no block-level predicate
    ///   ([`LogQuery::is_block_predicate_free`]: no content, prune-only,
    ///   stream-attribute, or pending-erasure arm), and
    /// - every relevant segment has a well-formed span (`min <= max`), is above
    ///   the fetcher's block-range threshold, and is fully CONTAINED in the
    ///   window (`ts_min <= seg.min && seg.max <= ts_max`). Containment is
    ///   strictly stronger than the overlap
    ///   [`LogsTableProvider::pruned_segments`] already filtered on, so no block
    ///   of a relevant segment can fall outside the window and every block
    ///   survives -- the survivor count is the whole segment.
    ///
    /// and finally there are at least `target_partitions` relevant segments, so
    /// whole-segment round-robin still fills every partition. Fewer segments than
    /// partitions is the striped path's job (deliverable 2 carries the plan
    /// footer there so its subset opens still skip re-probing); a partial
    /// overlap, a predicate, a pending erasure, or a below-threshold segment all
    /// fall to the unchanged plan-then-stripe path, byte for byte.
    ///
    /// The threshold conjunct keeps this strictly to the band where skipping the
    /// plan phase saves a GET: at or below the threshold a segment is already
    /// read whole in one GET on both the plan and scan paths (they coalesce on
    /// the same `(0, object_size)` cache key), so the fast path would add nothing
    /// and would only diverge from the existing small-object tests.
    fn whole_segment_fast_path(&self, query: &LogQuery) -> Option<usize> {
        if !query.is_block_predicate_free() {
            return None;
        }
        let threshold = self.fetcher.block_range_threshold();
        let mut relevant = 0usize;
        for seg in self.segments.iter() {
            if !LogSegmentFetcher::ts_range_relevant(seg, self.ts_min, self.ts_max) {
                continue;
            }
            relevant += 1;
            let contained = seg.min_event_ts_ns <= seg.max_event_ts_ns
                && seg.object_size > threshold
                && self.ts_min <= seg.min_event_ts_ns
                && seg.max_event_ts_ns <= self.ts_max;
            if !contained {
                return None;
            }
        }
        (relevant >= self.target_partitions).then_some(relevant)
    }

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
            "LogsScanExec {{ partitions: {}, segments: {} }}",
            self.target_partitions,
            self.segments.len()
        )
    }
}

impl DisplayAs for LogsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogsScanExec: partitions={}, content={}, prune={}, projection=[{}]",
            self.target_partitions,
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

    /// Report the exact row count for a predicate-free, erasure-free
    /// `COUNT(*)` straight from the catalog's committed row counts, so
    /// DataFusion's `AggregateStatistics` physical-optimizer rule rewrites the
    /// aggregate into a literal and never executes this scan (issue #698). On
    /// the ClickBench tenant (issue #680) a scanning `count(*)` moved 23 GB
    /// from object storage to add up 8424 numbers the resolve already had.
    ///
    /// `num_rows` is `Exact` only for the whole-plan request (`partition` is
    /// `None`) and only when [`Self::stats_are_exact`] holds; a per-partition
    /// request gets `Absent`, because a partition's count is its lazily
    /// resolved striped share of the blocks, not known here. Under the same
    /// condition the `ts` column's `column_statistics` report an `Exact`
    /// min/max spanning every touched segment (issue #723); every other
    /// column's stays `Absent`. `total_byte_size` stays `Absent`.
    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        // Validate the partition index exactly as the trait default does, so an
        // out-of-range request is an internal error, never a silent answer.
        if let Some(idx) = partition {
            let partition_count = self.properties().partitioning.partition_count();
            if idx >= partition_count {
                return Err(DataFusionError::Internal(format!(
                    "Invalid partition index: {idx}, the partition count is {partition_count}"
                )));
            }
        }

        let mut stats = Statistics::new_unknown(&self.schema());
        if partition.is_none() && self.stats_are_exact() {
            // Sum in u64 with checked addition; overflow of either the sum or
            // the usize conversion falls back to Absent rather than a wrong
            // count or a panic.
            let total: Option<u64> = self
                .segments
                .iter()
                .try_fold(0u64, |acc, seg| acc.checked_add(seg.sample_count));
            if let Some(total) = total
                && let Ok(n) = usize::try_from(total)
            {
                stats.num_rows = Precision::Exact(n);
            }

            // The ts span is exactly `[min(min_event_ts_ns), max(max_event_ts_ns)]`
            // over the touched segments: the containment check in
            // `stats_are_exact` proves the bound removes no rows, so no segment's
            // extremum is clipped away. Report it on the `ts` column wherever the
            // projection keeps it (a projected-out ts leaves nothing to fill);
            // empty segments leave both `None`, so the column stays `Absent`.
            if let (Some(min), Some(max)) = (
                self.segments.iter().map(|s| s.min_event_ts_ns).min(),
                self.segments.iter().map(|s| s.max_event_ts_ns).max(),
            ) && let Some(ts_idx) = self.schema.fields().iter().position(|f| f.name() == "ts")
            {
                let col = &mut stats.column_statistics[ts_idx];
                col.min_value = Precision::Exact(ScalarValue::TimestampNanosecond(Some(min), None));
                col.max_value = Precision::Exact(ScalarValue::TimestampNanosecond(Some(max), None));
            }
        }
        Ok(Arc::new(stats))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
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

        let ctx = Arc::new(PartitionCtx {
            fetcher: self.fetcher.clone(),
            tenant_hash: self.tenant_hash,
            query,
            columns: self.columns.clone(),
            accounting: self.accounting.clone(),
        });

        // #693 part 3 deliverable 1: a predicate-free query whose window fully
        // contains every relevant, above-threshold segment, with at least
        // `target_partitions` of them, skips the plan phase entirely. Each
        // partition computes its whole-segment round-robin share with no I/O and
        // opens each owned segment with one whole-object GET (no plan probe, no
        // scan-side probe). Any other shape falls to the plan-then-stripe path
        // below, byte for byte.
        let (work, fast_whole_segment, state) = if let Some(relevant) =
            self.whole_segment_fast_path(&ctx.query)
        {
            let n = self.target_partitions.max(1).min(relevant.max(1));
            let work = owned_whole_segments(&self.segments, self.ts_min, self.ts_max, partition, n);
            (work, true, LogScanState::NextSegment)
        } else {
            // The block-level assignment needs each segment's surviving-block
            // count, which a prune must produce (async). The first partition
            // to poll runs that prune for every segment through the shared
            // `counts` cell, with as many prunes in flight as the scan has
            // partitions; the rest await its result. Building the future here
            // (not awaiting it) keeps `execute` synchronous, as DataFusion
            // requires.
            let counts_fut = plan_counts_future(
                Arc::clone(&self.counts),
                Arc::clone(&ctx),
                Arc::clone(&self.segments),
                self.target_partitions,
            );
            (VecDeque::new(), false, LogScanState::Planning(counts_fut))
        };

        Ok(Box::pin(LogScanStream {
            schema: Arc::clone(&self.schema),
            projection: Arc::clone(&self.projection),
            declared: Arc::clone(&self.declared),
            ctx,
            erasure: Arc::clone(&self.erasure),
            columnar_eligible: self.columnar_eligible,
            blocks: BlockMetrics::new(&self.metrics, partition),
            partition,
            target_partitions: self.target_partitions,
            stripe_blocks: self.stripe_blocks,
            fast_whole_segment,
            segments: Arc::clone(&self.segments),
            work,
            reservation,
            held: 0,
            emitted: 0,
            pending: Pending::None,
            current_seg: None,
            current_indices: Vec::new(),
            current_footer: None,
            seg_columnar_blocks: 0,
            state,
        }))
    }
}

/// The shared per-segment block plan (ADR-0102): for each segment, in snapshot
/// order, the count of blocks that survive this query's pruning and the
/// whole-segment [`ScanStats`] the prune produced, or `None` for a
/// ts-irrelevant segment (no GET, no blocks). Computed once and reused by every
/// partition.
struct PlanCounts {
    segs: Vec<Option<SegPlan>>,
    total_blocks: usize,
}

/// One relevant segment's contribution to [`PlanCounts`].
struct SegPlan {
    /// Blocks surviving this query's pruning; the count the block-index stripe
    /// is computed over.
    survivors: usize,
    /// The whole-segment prune stats ([`BlockMetrics::record_segment_totals`]
    /// consumes `blocks_total` and the postings drop). `blocks_scanned`/`pages`
    /// are zero here -- planning decodes nothing.
    stats: ScanStats,
    /// The footer the plan fast path read for this segment (#693 part 3,
    /// deliverable 2), or `None` when the plan slow branch opened the scan
    /// instead. Carried to each per-partition subset open through [`OwnedSeg`] so
    /// the open reuses it and skips its own suffix probe.
    footer: Option<LogFooter>,
}

type CountsFuture = Pin<Box<dyn Future<Output = DFResult<Arc<PlanCounts>>> + Send>>;

/// A future resolving the shared [`PlanCounts`] through `cell`: the first
/// partition to poll computes it (pruning every segment once), the rest await
/// that one computation. Errors are not cached, so a transient fetch failure
/// can be retried by a later poll.
fn plan_counts_future(
    cell: Arc<OnceCell<Arc<PlanCounts>>>,
    ctx: Arc<PartitionCtx>,
    segments: Arc<Vec<SegmentRef>>,
    plan_concurrency: usize,
) -> CountsFuture {
    Box::pin(async move {
        let counts = cell
            .get_or_try_init(|| compute_plan_counts(&ctx, &segments, plan_concurrency))
            .await?;
        Ok(Arc::clone(counts))
    })
}

/// Prune every segment once (no block decode) to build the shared block plan.
///
/// The prunes run `plan_concurrency` at a time (`buffered`, so `segs` keeps
/// snapshot order and [`owned_work`] can index it by segment position). Every
/// partition awaits this whole pass before it drains anything, so the plan
/// phase sits alone on the query's critical path: run serially it costs one
/// object-store round trip per segment in sequence (issue #691 measured about
/// 20 minutes per statement on 8424 objects, one GET in flight for the whole
/// time). Concurrency here changes only how many of those reads are in flight,
/// never their count (still one plan read sequence per segment) or their
/// semantics (the first error aborts the plan, and `get_or_try_init` does not
/// cache it); the fetcher's own in-flight GET semaphore remains the global
/// bound, so an oversized `plan_concurrency` is safe.
// Issue #693 part 3: the predicate-free full-window fast path in `execute`
// skips this whole pass; see `owned_work` and `plan_segment_fast`.
async fn compute_plan_counts(
    ctx: &PartitionCtx,
    segments: &[SegmentRef],
    plan_concurrency: usize,
) -> DFResult<Arc<PlanCounts>> {
    // Not-yet-polled futures, one per segment, so `buffered` decides how many
    // run at once. Built with a loop rather than a `map` closure: a closure
    // returning a future that borrows its argument cannot satisfy the
    // higher-ranked bound this `Send` boxed future needs.
    let mut prunes = Vec::with_capacity(segments.len());
    for seg in segments {
        prunes.push(
            ctx.fetcher
                .plan_segment(seg, ctx.tenant_hash, &ctx.query, &ctx.accounting),
        );
    }
    let planned: Vec<Option<(usize, ScanStats, Option<LogFooter>)>> = futures::stream::iter(prunes)
        .buffered(plan_concurrency.max(1))
        .map_err(SqlError::from)
        .try_collect()
        .await?;
    let mut segs = Vec::with_capacity(segments.len());
    let mut total_blocks = 0usize;
    for entry in planned {
        match entry {
            Some((survivors, stats, footer)) => {
                total_blocks += survivors;
                segs.push(Some(SegPlan {
                    survivors,
                    stats,
                    footer,
                }));
            }
            None => segs.push(None),
        }
    }
    Ok(Arc::new(PlanCounts { segs, total_blocks }))
}

/// One segment this partition owns blocks in, and the block-index list (into
/// the segment's surviving-block list) it must drain.
struct OwnedSeg {
    seg: SegmentRef,
    indices: Vec<usize>,
    /// The plan-phase footer for this segment (#693 part 3, deliverable 2),
    /// carried to the subset open so it skips its own suffix probe. `None` on the
    /// whole-segment fast path (no plan phase) and when the plan slow branch ran.
    footer: Option<LogFooter>,
}

/// This partition's share of the block assignment, in one of two modes
/// (ADR-0102, amended by #693), both with `n = target_partitions.max(1).
/// min(total.max(1))`.
///
/// - **`stripe_blocks` true** (a read cache is wired): the flattened block
///   assignment. Unit `i` in the segment-then-block order over all surviving
///   blocks goes to partition `i % n`, so a segment's blocks stripe across
///   partitions and single-flight coalesces the re-opens.
/// - **`stripe_blocks` false** (un-cached): the segment-granular assignment.
///   Counting only segments with a surviving block, in snapshot order, segment
///   `j` goes to partition `j % n` and that partition drains all of the
///   segment's surviving block indices (`0..survivors`). Each segment is opened
///   by exactly one partition, so with nothing to coalesce re-opens the scan
///   still costs one read per segment rather than one per partition per segment.
///
/// Returns, per owned segment, the surviving-block indices this partition
/// drains, in ascending order.
fn owned_work(
    counts: &PlanCounts,
    segments: &[SegmentRef],
    partition: usize,
    n: usize,
    stripe_blocks: bool,
) -> VecDeque<OwnedSeg> {
    let mut work = VecDeque::new();
    if stripe_blocks {
        let mut global = 0usize;
        for (seg_idx, plan) in counts.segs.iter().enumerate() {
            let Some(plan) = plan else { continue };
            let mut indices = Vec::new();
            for local in 0..plan.survivors {
                if (global + local) % n == partition {
                    indices.push(local);
                }
            }
            global += plan.survivors;
            if !indices.is_empty() {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    indices,
                    footer: plan.footer.clone(),
                });
            }
        }
    } else {
        let mut seg_ordinal = 0usize;
        for (seg_idx, plan) in counts.segs.iter().enumerate() {
            let Some(plan) = plan else { continue };
            if plan.survivors == 0 {
                continue;
            }
            if seg_ordinal % n == partition {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    indices: (0..plan.survivors).collect(),
                    footer: plan.footer.clone(),
                });
            }
            seg_ordinal += 1;
        }
    }
    work
}

/// The predicate-free full-window whole-segment assignment (#693 part 3,
/// deliverable 1): the same round-robin [`owned_work`]'s un-cached branch uses,
/// but computed with NO plan phase at all. Relevant (ts-overlapping) segments in
/// snapshot order are numbered `0..`, and segment `j` goes to partition `j % n`,
/// which then drains the whole segment through [`open_segment_whole`]. `n` is the
/// same `min(target_partitions, relevant)` the caller derives, so every
/// partition gets whole segments and none is split.
///
/// Each relevant segment is proved fully contained in the query window and
/// predicate-free before this runs (see [`LogsScanExec::whole_segment_fast_path`]),
/// so every block survives and the block-index list is unused here.
fn owned_whole_segments(
    segments: &[SegmentRef],
    ts_min: i64,
    ts_max: i64,
    partition: usize,
    n: usize,
) -> VecDeque<OwnedSeg> {
    let mut work = VecDeque::new();
    let mut ordinal = 0usize;
    for seg in segments {
        if !LogSegmentFetcher::ts_range_relevant(seg, ts_min, ts_max) {
            continue;
        }
        if ordinal % n == partition {
            work.push_back(OwnedSeg {
                seg: seg.clone(),
                indices: Vec::new(),
                footer: None,
            });
        }
        ordinal += 1;
    }
    work
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

/// Fetch one segment's bytes and open its pruned, column-projected scan
/// restricted to the surviving-block positions in `indices` (ADR-0102).
/// `Ok(None)` means the catalog summary proved the segment irrelevant, with no
/// GET issued -- which cannot happen for a segment that was already counted
/// with survivors, but is handled as end-of-segment rather than panicking.
fn open_segment_subset(
    ctx: Arc<PartitionCtx>,
    seg: SegmentRef,
    indices: Vec<usize>,
    footer: Option<LogFooter>,
) -> OpenFuture {
    Box::pin(async move {
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant_subset(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                &indices,
                footer.as_ref(),
                &ctx.accounting,
            )
            .await
            .map_err(SqlError::from)?;
        Ok(scan)
    })
}

/// Fetch one whole segment's object in a single GET and open its pruned,
/// column-projected scan over all of its blocks (#693 part 3, deliverable 1).
/// Used only on the predicate-free full-window whole-segment path, where the
/// segment is assigned to exactly one partition and every block survives, so a
/// whole-object read is optimal and no plan phase or suffix probe is needed.
fn open_segment_whole(ctx: Arc<PartitionCtx>, seg: SegmentRef) -> OpenFuture {
    Box::pin(async move {
        let scan = ctx
            .fetcher
            .scan_whole_accounted_with_tenant(
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
    /// Awaiting the shared per-segment block plan (ADR-0102). Once it resolves,
    /// this partition's owned `(segment, block-index-list)` work is computed and
    /// the stream advances to draining.
    Planning(CountsFuture),
    /// Advance to the next owned segment of this partition, or finish.
    NextSegment,
    /// Awaiting one owned segment's GET and prune (restricted to this
    /// partition's block-index list).
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
    /// block turned out to carry an `attrs_raw` overflow page. The re-opened
    /// scan is given the SAME block-index list this partition owns for the
    /// segment (ADR-0102), so `skip` is a position within that list: the number
    /// of this partition's blocks already emitted columnar. The re-opened row
    /// scan drains and discards exactly those, then resumes, so no row this
    /// partition owns is emitted twice or dropped -- and because the list is
    /// this partition's own, a fallback another partition independently triggers
    /// for the same segment concerns a disjoint list and cannot interfere.
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
    /// This stream's DataFusion partition index, and the total declared count.
    /// Together with the shared [`PlanCounts`] they determine which
    /// `(segment, block-index-list)` units this partition owns (ADR-0102).
    partition: usize,
    target_partitions: usize,
    /// The assignment mode (ADR-0102, amended by #693): block striping when a
    /// cache is wired, segment-granular otherwise. Passed to [`owned_work`].
    stripe_blocks: bool,
    /// The predicate-free full-window whole-segment fast path (#693 part 3,
    /// deliverable 1). When set, `work` was filled by [`owned_whole_segments`]
    /// with no plan phase, each segment is opened with one whole-object GET
    /// ([`open_segment_whole`]), and the segment's whole-segment prune totals are
    /// recorded per segment at exhaustion (each segment has one owner, so no
    /// double count) instead of by partition 0 during planning.
    fast_whole_segment: bool,
    /// Every segment in the snapshot, snapshot order, shared with the exec. The
    /// owned-work computation indexes this by segment position.
    segments: Arc<Vec<SegmentRef>>,
    /// This partition's owned segments and their block-index lists, filled once
    /// [`LogScanState::Planning`] resolves. Drained front to back.
    work: VecDeque<OwnedSeg>,
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
    /// The surviving-block index list this partition owns for [`Self::
    /// current_seg`], kept so the `attrs_raw` fallback re-opens the row path
    /// over the SAME list (ADR-0102). Set alongside `current_seg`.
    current_indices: Vec<usize>,
    /// The plan-phase footer for [`Self::current_seg`] (#693 part 3, deliverable
    /// 2), kept so the `attrs_raw` fallback re-opens the subset with the same
    /// footer it first used. `None` on the whole-segment fast path.
    current_footer: Option<LogFooter>,
    /// How many of this partition's blocks in the current segment the columnar
    /// fast path has already emitted. The `attrs_raw` fallback re-opens the
    /// segment over `current_indices` and skips this many positions so none is
    /// emitted twice. Reset when a new segment starts draining.
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
                LogScanState::Planning(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(counts)) => {
                        // Cap the stride by the real block count (ADR-0102): with
                        // fewer blocks than partitions this collapses the extra
                        // partitions to empty work, exactly what the declared
                        // `min(target_partitions, total_blocks)` would give.
                        let n = this
                            .target_partitions
                            .max(1)
                            .min(counts.total_blocks.max(1));
                        // Partition 0 publishes every relevant segment's
                        // whole-segment prune totals, once, so striping a segment
                        // across partitions does not multiply them.
                        if this.partition == 0 {
                            for plan in counts.segs.iter().flatten() {
                                this.blocks.record_segment_totals(&plan.stats);
                            }
                        }
                        this.work = owned_work(
                            &counts,
                            &this.segments,
                            this.partition,
                            n,
                            this.stripe_blocks,
                        );
                        this.state = LogScanState::NextSegment;
                    }
                    Poll::Ready(Err(e)) => return this.fail(e),
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::NextSegment => match this.work.pop_front() {
                    Some(OwnedSeg {
                        seg,
                        indices,
                        footer,
                    }) => {
                        this.current_seg = Some(seg.clone());
                        this.current_indices = indices.clone();
                        this.current_footer = footer.clone();
                        this.seg_columnar_blocks = 0;
                        // Whole-segment fast path reads the object in one GET
                        // (#693 part 3); the striped path opens only this
                        // partition's subset, reusing the plan footer if any.
                        this.state = if this.fast_whole_segment {
                            LogScanState::Opening(open_segment_whole(Arc::clone(&this.ctx), seg))
                        } else {
                            LogScanState::Opening(open_segment_subset(
                                Arc::clone(&this.ctx),
                                seg,
                                indices,
                                footer,
                            ))
                        };
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
                            this.blocks.record_scan(&stats);
                            // The whole-segment fast path has no plan phase, so
                            // the whole-segment prune totals are recorded here,
                            // per segment (one owner per segment, no double
                            // count), instead of by partition 0 during planning.
                            if this.fast_whole_segment {
                                this.blocks.record_segment_totals(&stats);
                            }
                            this.state = LogScanState::NextSegment;
                        }
                        Step::Fallback => {
                            // Re-open the segment on the row path over the SAME
                            // block-index list this partition owns (ADR-0102),
                            // skipping the blocks already emitted columnar so no
                            // row is emitted twice. `skip` is a position within
                            // this partition's own list, so the count and the list
                            // line up even when the segment's blocks are striped
                            // across several partitions.
                            //
                            // Publish the abandoned columnar cursor's partial
                            // counters (issue #474) before it is dropped: the
                            // re-opened row scan below re-decodes this partition's
                            // whole list from the start, so those blocks' pages are
                            // decoded twice, but the abandoned cursor's own count
                            // of its first pass was previously discarded along with
                            // it. `record_scan` accumulates, so this and the row
                            // scan's own eventual `record_scan` call sum to the
                            // real total decode work across both passes, matching
                            // what `EXPLAIN ANALYZE` claims the counters prove
                            // (ADR-0087): that projection reached the page level.
                            this.blocks.record_scan(&scan.stats());
                            let seg = match this.current_seg.clone() {
                                Some(seg) => seg,
                                None => {
                                    return this.fail(DataFusionError::Internal(
                                        "attrs_raw fallback with no current segment".into(),
                                    ));
                                }
                            };
                            let fut = if this.fast_whole_segment {
                                open_segment_whole(Arc::clone(&this.ctx), seg)
                            } else {
                                open_segment_subset(
                                    Arc::clone(&this.ctx),
                                    seg,
                                    this.current_indices.clone(),
                                    this.current_footer.clone(),
                                )
                            };
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
                                this.blocks.record_scan(&stats);
                                if this.fast_whole_segment {
                                    this.blocks.record_segment_totals(&stats);
                                }
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
                            this.blocks.record_scan(&stats);
                            if this.fast_whole_segment {
                                this.blocks.record_segment_totals(&stats);
                            }
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
        // Dictionary-typed to match the fast path's schema (ADR-0099 decision
        // 5): every batch DataFusion validates carries one type per column, so
        // the row path must produce `Dictionary(Int32, Utf8)` too or every
        // fallback batch (erasure pending, `attrs` projected, an `attrs_raw`
        // page) would fail schema validation at runtime. The builder dedups; a
        // wrong variant or absent key is a NULL cell, never a cast (decision 7).
        DeclaredType::Str => {
            let mut b = StringDictionaryBuilder::<Int32Type>::new();
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
        DeclaredType::Str => build_declared_str_columnar(view, plan, start, end, cache)?,
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

/// Build a declared `Str` column as an Arrow `Dictionary(Int32, Utf8)` for
/// surviving rows `start..end` (ADR-0099 decision 5).
///
/// Two cases, both producing the same logical values [`declared_column_array`]'s
/// `Str` arm produces on the row path, so a fast-path batch and a fallback batch
/// validate against the one schema DataFusion checks:
///
/// - **Dict-encoded page** ([`ColumnarBlockView::str_dict`] returns `Some`): the
///   page's distinct values become the Arrow dictionary and the record rows
///   reuse the page's ids directly, with no per-row string allocation. A page
///   dict entry that is not UTF-8 becomes a NULL dictionary value, so a row
///   keyed to it reads NULL; that only happens for a row the record sets in a
///   *different*-typed column of the same key (record wins, wrong variant is
///   NULL by ADR-0090 decision 7). A row the record does not set at all reads
///   through to the resource/scope fallback, whose value is appended to the
///   dictionary (the one per-row copy, unavoidable because that value is not in
///   the page).
/// - **Plain page** (`str_dict` returns `None`, or the key has no `Str`
///   FIELD_DIR column at all): a degenerate identity dictionary, one entry per
///   non-null surviving row with keys `0..`, built from [`declared_merged_value`]
///   exactly as the pre-dictionary code did. No hashing and no dedup pass, so
///   this case stays exactly as expensive as it was.
fn build_declared_str_columnar(
    view: &ColumnarBlockView<'_>,
    plan: &DeclaredPlan<'_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let n = end - start;
    Ok(
        match plan.matching.and_then(|mc| view.str_dict(mc.column_id)) {
            Some(col) => {
                // Dictionary values start as the page's distinct byte values,
                // decoded to UTF-8; a non-UTF-8 entry becomes a NULL value. Ids
                // address these in the page's order, so a record row's page id maps
                // straight to a dictionary index. Resource/scope fallback values are
                // appended past the page dict.
                let mut values = StringBuilder::new();
                for v in col.dict() {
                    match std::str::from_utf8(v) {
                        Ok(s) => values.append_value(s),
                        Err(_) => values.append_null(),
                    }
                }
                let mut next_extra = i32::try_from(col.dict().len()).map_err(|_| {
                    DataFusionError::Internal("declared Str dictionary exceeds i32 keys".into())
                })?;
                let mut keys: Vec<Option<i32>> = Vec::with_capacity(n);
                for i in start..end {
                    if plan.all_cols.iter().any(|&c| attr_present(view, c, i)) {
                        // Record wins. `id_at` is `Some` only when the record's
                        // `Str` column has a value at this row; `None` (record set
                        // the key in another-typed column) is a NULL cell. An `id`
                        // pointing at a non-UTF-8 (NULL) dictionary value also reads
                        // NULL, matching `read_str_cell`.
                        match col.id_at(i) {
                            Some(id) => keys.push(Some(i32::try_from(id).map_err(|_| {
                                DataFusionError::Internal(
                                    "declared Str dictionary id exceeds i32".into(),
                                )
                            })?)),
                            None => keys.push(None),
                        }
                    } else if let Some(stream_ref) = view.stream_ref(i) {
                        let resource = resource_attrs(view, cache, stream_ref)?;
                        match find_attr(resource, &plan.dc.key) {
                            Some(AttrValue::Str(s)) => {
                                values.append_value(s);
                                keys.push(Some(next_extra));
                                next_extra += 1;
                            }
                            _ => keys.push(None),
                        }
                    } else {
                        keys.push(None);
                    }
                }
                let dict = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(keys),
                    Arc::new(values.finish()),
                )
                .map_err(DataFusionError::from)?;
                Arc::new(dict)
            }
            None => {
                // Identity dictionary: one entry per non-null row, no dedup.
                let mut values = StringBuilder::new();
                let mut keys: Vec<Option<i32>> = Vec::with_capacity(n);
                let mut next = 0i32;
                for i in start..end {
                    match declared_merged_value(view, plan, i, cache)? {
                        Some(AttrValue::Str(s)) => {
                            values.append_value(&s);
                            keys.push(Some(next));
                            next += 1;
                        }
                        _ => keys.push(None),
                    }
                }
                let dict = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(keys),
                    Arc::new(values.finish()),
                )
                .map_err(DataFusionError::from)?;
                Arc::new(dict)
            }
        },
    )
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
