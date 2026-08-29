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
//! - **Predicate-free full-window fast path (#693 part 3, amended by #739).**
//!   When the query has no block-level predicate, no pending erasure, and its
//!   window fully contains every relevant segment, and there are at least
//!   `target_partitions` of them, the plan phase is skipped entirely
//!   ([`LogsScanExec::whole_segment_fast_path`]): whole segments are assigned
//!   round-robin (the same rule the un-cached path uses), with no plan phase and
//!   no suffix probe from it. How each assigned segment is then READ is a
//!   separate, per-segment decision (#862, [`PartitionCtx::open_by_column_chunk`]):
//!   a projection wide enough to want most of the object's bytes takes the one
//!   whole-object GET ([`LogSegmentFetcher::scan_whole_accounted_with_tenant`]),
//!   and a narrow one takes the probe-and-range path
//!   ([`LogSegmentFetcher::scan_accounted_with_tenant`]), which brings one
//!   coalesced range per surviving `(row group, projected column)`. Every block
//!   surviving is what the fast path's conjuncts prove; under RLOG v4 that no
//!   longer implies every byte is needed. The arbiter is the fetch layer's
//!   request-cost model ([`LogSegmentFetcher::ranged_projection_pays`]), so the
//!   ranged path is taken only where the bytes it skips outweigh the round trips
//!   it adds. Object size does not enter into the ASSIGNMENT: a segment at or
//!   below the block-range threshold is read whole by both entries, on the same
//!   `(0, object_size)` cache key, so it joins the assignment without changing
//!   what is read (#739 -- as a query-wide conjunct the threshold let one small
//!   tail object per `(shard, hour)` veto an entire 8,424-object snapshot).
//!   When there are fewer relevant segments than partitions the striped
//!   path runs instead, but the plan footer it read is carried to each subset
//!   open ([`LogSegmentFetcher::fetch_object_with_footer`]) so those opens skip
//!   their own re-probe. Any other query shape runs the plan-then-stripe path
//!   above unchanged, and publishes a `fast_path_rejected_*` counter naming the
//!   conjunct that sent it there.
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
//! # Row refs, for TopK late materialization (ADR-0774)
//!
//! [`LogsScanExec::reproject`] builds a sibling of an existing scan over a
//! narrower projection, optionally appending one synthetic `UInt64` column
//! (`__ravel_row_ref`) past every projected index. Each row's value packs the
//! `(segment ordinal, surviving-block position, surviving-row position)` this
//! stream is currently at -- cursor state the scan already holds, so nothing
//! extra is read or decoded to produce it.
//!
//! Only [`crate::late_materialization::TopKLateMaterialization`] builds such a
//! scan, and a scan without it is byte-identical to the pre-ADR-0774 one. What
//! that address means, and why it still resolves when a second phase re-reads
//! the block with a wider column selection, is that module's doc.
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
//! An attribute predicate's pushdown is always `Inexact`
//! ([`crate::logs_pushdown::filter_is_exact`] answers `true` for the ts and
//! `has_word` shapes only), so DataFusion re-applies the *original* predicate
//! against the emitted batch. [`build_batch`] populates the `attrs`
//! column from the fully merged view (ADR-0033 amendment), so the
//! residual evaluates `attrs['k'] = 'v'` against exactly the data a row's SQL
//! semantics demand: a resource-only match survives (the residual sees it in the
//! merged column), and a record-attribute override survives (the merge resolves
//! the key to the record's value, which wins). The merged column and the
//! residual are the whole correctness story.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, FixedSizeBinaryBuilder, Int32Array,
    Int64Builder, MapBuilder, StringArray, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{Int32Type, Schema, SchemaRef};
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
use ravel_catalog::{LoadedColumnStats, SegmentRef};
use ravel_logseg::footer::LogFooter;
use ravel_logseg::{
    AttrColumn, BoolCursor, BytesCursor, ColumnSelection, ColumnarBlockView, F64BitsCursor,
    FieldSel, FieldType, I64Cursor, LogRecord, Predicate, ScanStats,
};
use ravel_proto::catalog::v1::ColumnValue;
use ravel_proto::catalog::v1::column_value::Kind as ColumnValueKind;
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{ColumnarBlockOutcome, LogQuery, LogSegmentFetcher, LogSegmentScan};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use tokio::sync::OnceCell;

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::error::SqlError;
use crate::late_materialization::{RowRef, row_ref_field};
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
pub(crate) fn records_memory(records: &[LogRecord]) -> usize {
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

/// Block columns every RLOG object carries whatever the tenant declares: the
/// ten fixed columns of `ravel_logseg::record` (`ts`, `observed_ts`,
/// `stream_ref`, `severity_num`, `severity_text`, `body`, `trace_id`, `span_id`,
/// `flags`, `attrs_raw`). The denominator of [`ResolvedColumns::fraction_of`],
/// alongside the tenant's declared attribute columns.
const FIXED_OBJECT_COLUMNS: usize = 10;

/// Object columns a [`ColumnSelection`] decodes whatever it names: `ts` (the
/// exact ts re-check) and `stream_ref` (stream identity). See
/// `ravel_logseg::columns`.
const IMPLICIT_OBJECT_COLUMNS: usize = 2;

/// The column selection one query resolves to, and how wide it is.
///
/// The width is what makes the whole-segment fast path's routing decision
/// possible (issue #862): whether reading a segment by column chunk beats
/// reading it whole is a question about how much of the object the projection
/// wants, and `ColumnSelection` itself exposes no count. Both come out of one
/// walk over the projection in [`resolve_columns`], so they cannot disagree.
struct ResolvedColumns {
    /// What the reader decodes, and on a version-4 object what the fetch brings
    /// (ADR-0699 decision 5).
    selection: ColumnSelection,
    /// Distinct object columns the selection names, or `None` when it names
    /// every column (`SELECT *`, any reference to the merged `attrs` map, or
    /// the fail-open widening).
    width: Option<usize>,
}

impl ResolvedColumns {
    /// The share of an object's bytes this selection is expected to read, as a
    /// column-count ratio over the object's column population (the fixed
    /// columns plus `declared_columns` dynamic ones).
    ///
    /// A ratio of counts, not of bytes: per-column byte volumes are not known
    /// until PAGE_DIR is read, and the point of this figure is to be available
    /// with no I/O at all. It feeds
    /// [`LogSegmentFetcher::ranged_projection_pays`], whose threshold is an
    /// order of magnitude away from the borderline cases, and the byte-exact
    /// decision still runs one layer down in the fetcher's coverage crossover.
    /// A tenant carrying dynamic attributes it never declared has more object
    /// columns than this denominator counts, which over-states the fraction and
    /// so errs toward the unchanged whole-object read.
    fn fraction_of(&self, declared_columns: usize) -> f64 {
        let Some(width) = self.width else {
            return 1.0;
        };
        let total = FIXED_OBJECT_COLUMNS + declared_columns;
        (width as f64 / total as f64).min(1.0)
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
) -> ResolvedColumns {
    // Accumulated as sets first, and turned into a `ColumnSelection` below, so
    // the same walk that decides what to decode also counts how many distinct
    // object columns that is. Counting from the finished selection is not an
    // option (it exposes no width), and a second walk beside this one would be
    // free to drift from it.
    let mut all = false;
    let mut all_attrs = false;
    let mut fixed: BTreeSet<usize> = BTreeSet::new();
    let mut attrs: BTreeSet<String> = BTreeSet::new();

    for &i in projection {
        match i {
            // `ts` is always decoded; naming it changes nothing.
            LOG_COL_TS => {}
            LOG_COL_OBSERVED_TS
            | LOG_COL_SEVERITY_NUM
            | LOG_COL_SEVERITY_TEXT
            | LOG_COL_BODY
            | LOG_COL_TRACE_ID
            | LOG_COL_SPAN_ID
            | LOG_COL_FLAGS => {
                fixed.insert(i);
            }
            // The merged `attrs` map exposes every key, so referencing it at
            // all means every dynamic column plus the overflow.
            LOG_COL_ATTRS => all_attrs = true,
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
                Some(dc) => {
                    attrs.insert(dc.key.clone());
                }
                None => all = true,
            },
        }
    }
    for p in content {
        content_columns(p, &mut fixed, &mut attrs);
    }
    for p in erasure {
        for (key, _) in p.matchers() {
            attrs.insert(key.clone());
        }
    }

    let selection = if all {
        ColumnSelection::all()
    } else {
        let mut sel = ColumnSelection::fixed_only();
        for &i in &fixed {
            sel = match i {
                LOG_COL_OBSERVED_TS => sel.with_observed_ts(),
                LOG_COL_SEVERITY_NUM => sel.with_severity_num(),
                LOG_COL_SEVERITY_TEXT => sel.with_severity_text(),
                LOG_COL_BODY => sel.with_body(),
                LOG_COL_TRACE_ID => sel.with_trace_id(),
                LOG_COL_SPAN_ID => sel.with_span_id(),
                // `fixed` is only ever filled from the arm above, which admits
                // exactly the seven names matched here; `flags` is the last.
                _ => sel.with_flags(),
            };
        }
        if all_attrs {
            sel = sel.with_all_attrs();
        }
        for key in &attrs {
            sel = sel.with_attr(key.clone());
        }
        sel
    };
    let width = if all || all_attrs {
        None
    } else {
        Some(IMPLICIT_OBJECT_COLUMNS + fixed.len() + attrs.len())
    };
    ResolvedColumns { selection, width }
}

/// Add every column an exact content predicate reads. `TsRange` and `StreamIn`
/// need only the two always-decoded fixed columns.
fn content_columns(pred: &Predicate, fixed: &mut BTreeSet<usize>, attrs: &mut BTreeSet<String>) {
    match pred {
        Predicate::And(arms) => {
            for a in arms {
                content_columns(a, fixed, attrs);
            }
        }
        // `NumRange` is prune-only (ADR-0095 decision 6): it never reaches the
        // exact content channel, so it reads no columns, same as ts/stream. The
        // planner-side pushdown that would emit it is #278's job.
        Predicate::TsRange { .. } | Predicate::StreamIn(_) | Predicate::NumRange { .. } => {}
        Predicate::HasWord { field, .. } | Predicate::Equals { field, .. } => match field {
            FieldSel::Body => {
                fixed.insert(LOG_COL_BODY);
            }
            FieldSel::SeverityText => {
                fixed.insert(LOG_COL_SEVERITY_TEXT);
            }
            FieldSel::Attr(name) => {
                attrs.insert(name.clone());
            }
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

/// The identity tuple `fold::entry_identity` uses for `seg`, so a column-
/// stats lookup joins by identity rather than ordinal position (ADR-0850).
/// An L1 (compacted) segment's `writer_id` is `Uuid::nil()` by convention
/// ([`SegmentRef::writer_id`]'s doc); column stats are only ever built for L0
/// entries (a fold-time filter, not enforced here), so an L1 segment's
/// identity simply never matches an entry in the loaded stats and the lookup
/// falls back to scanning it, with no special case needed.
fn segment_identity(seg: &SegmentRef) -> ravel_catalog::EntryIdentity {
    (
        seg.ingest_hour_bucket,
        seg.shard,
        *seg.writer_id.as_bytes(),
        seg.writer_epoch,
        seg.writer_seq,
    )
}

/// Decode `value` as the Arrow scalar `ty` projects to, or `None` when
/// `value`'s oneof kind does not match `ty` (a corrupt or version-mismatched
/// stat). `Str` is unreachable here: callers check for it before ever calling
/// this (see [`LogsScanExec::declared_min_max_all`]).
fn declared_scalar(ty: DeclaredType, value: &ColumnValue) -> Option<ScalarValue> {
    match (ty, value.kind.as_ref()) {
        (DeclaredType::I64, Some(ColumnValueKind::I64(v))) => Some(ScalarValue::Int64(Some(*v))),
        (DeclaredType::Bool, Some(ColumnValueKind::B(v))) => Some(ScalarValue::Boolean(Some(*v))),
        (DeclaredType::Bytes, Some(ColumnValueKind::BytesVal(v))) => {
            Some(ScalarValue::Binary(Some(v.clone())))
        }
        _ => None,
    }
}

/// The `None`-valued Arrow scalar `ty` projects to, for a declared column
/// whose exact MIN/MAX is `NULL` (every covered segment's column entirely
/// null, or zero segments). `Str` is unreachable, matching [`declared_scalar`].
fn declared_null_scalar(ty: DeclaredType) -> ScalarValue {
    match ty {
        DeclaredType::I64 => ScalarValue::Int64(None),
        DeclaredType::Bool => ScalarValue::Boolean(None),
        DeclaredType::Bytes => ScalarValue::Binary(None),
        DeclaredType::Str => ScalarValue::Utf8(None),
    }
}

/// Exact `GROUP BY <declared column>, COUNT(*)` result from
/// [`LogsScanExec::declared_group_counts`] (ADR-0850's q08 shape): one exact
/// count per distinct non-null value, plus the count of NULL rows kept
/// separately so a caller can decide whether SQL's NULL group applies (it
/// does when `null_count > 0`, and a zero-segment or all-null scan still
/// answers correctly with `counts` empty and `null_count` set accordingly).
pub(crate) struct DeclaredGroupCounts {
    pub(crate) counts: Vec<(ScalarValue, u64)>,
    pub(crate) null_count: u64,
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
    /// The share of an object's bytes `columns` is expected to read
    /// ([`ResolvedColumns::fraction_of`]), resolved once beside it. This is what
    /// the whole-segment fast path routes on (issue #862): it is the only input
    /// [`LogSegmentFetcher::ranged_projection_pays`] needs that the catalog
    /// summary does not already carry.
    projected_fraction: f64,
    /// The tenant's declared typed attribute columns (ADR-0090), in schema-
    /// append order. Index `k` here is schema index `FIRST_DECLARED_COL + k`.
    /// Empty for a zero-declaration query, which is byte-identical to the
    /// pre-ADR-0090 scan.
    declared: Arc<Vec<DeclaredColumn>>,
    /// Exact per-segment column statistics for the tenant's declared columns
    /// (ADR-0850), loaded once per plan and threaded down from
    /// [`crate::executor::SqlExecutor`]. `None` when no usable column-stats
    /// object exists (nothing folded yet, no configured typed columns, or the
    /// last fold's build/PUT failed): every metadata-only path degrades to
    /// scanning in that case. A live segment absent from
    /// `LoadedColumnStats::segments` has no exact statistics either, checked
    /// per column at the point of use rather than here.
    column_stats: Option<Arc<LoadedColumnStats>>,
    /// The resolved full `logs` schema this scan projects, i.e.
    /// `logs_schema_with_declared(&declared)`. Kept so [`Self::reproject`] can
    /// build a narrower sibling scan over the same table without re-deriving
    /// it (ADR-0774).
    full_schema: SchemaRef,
    /// This scan's output schema: the resolved full schema
    /// (`logs_schema_with_declared(&declared)`) projected by `projection`, plus
    /// the synthetic row-ref column appended when `row_refs` is set.
    schema: SchemaRef,
    /// Whether this scan appends the synthetic `__ravel_row_ref` column
    /// (ADR-0774): one `UInt64` per row packing the row's `(segment ordinal,
    /// surviving-block position, surviving-row position)` address, so a second
    /// phase can re-read exactly the rows a TopK kept. Set only by
    /// [`Self::reproject`], i.e. only by
    /// [`crate::late_materialization::TopKLateMaterialization`]; every other
    /// scan is byte-identical to the pre-ADR-0774 one.
    row_refs: bool,
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

/// Which conjunct kept a statement off the predicate-free full-window
/// whole-segment fast path (issue #739). Recorded as a DataFusion counter by
/// [`BlockMetrics::record_fast_path_rejection`] so a report reading the scan's
/// metrics can say why a statement striped instead of guessing from GET counts.
///
/// The variants are exactly the query-wide conjuncts
/// [`LogsScanExec::whole_segment_fast_path`] tests, in the order it tests them,
/// and the first failure wins: a query with both a content predicate and a
/// partial window reports [`Self::BlockPredicate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FastPathRejection {
    /// The snapshot carries a pending selective erasure (ADR-0064 decision 2).
    PendingErasure,
    /// The query carries a content, prune-only, or stream-attribute arm, so a
    /// block can be excluded and the survivor count is not the block count.
    BlockPredicate,
    /// Some relevant segment is not fully contained in the query window (a
    /// partial ts overlap), or its catalog span is ill-formed (`min > max`), so
    /// containment cannot be proved without reading the segment.
    SegmentNotContained,
    /// Fewer relevant segments than partitions: whole-segment round-robin would
    /// leave partitions empty, which is the striped path's job.
    FewerSegmentsThanPartitions,
}

impl FastPathRejection {
    /// The counter name this reason is published under. One static name per
    /// variant, so `EXPLAIN ANALYZE` and any metrics reader see the reason
    /// directly rather than a numeric code they must decode.
    fn metric_name(self) -> &'static str {
        match self {
            FastPathRejection::PendingErasure => "fast_path_rejected_pending_erasure",
            FastPathRejection::BlockPredicate => "fast_path_rejected_block_predicate",
            FastPathRejection::SegmentNotContained => "fast_path_rejected_segment_not_contained",
            FastPathRejection::FewerSegmentsThanPartitions => {
                "fast_path_rejected_fewer_segments_than_partitions"
            }
        }
    }
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
    /// Relevant segments whose plan phase had to read the whole object rather
    /// than counting survivors from the skip index (#761). Two causes: a
    /// predicate the skip index cannot decide (a `has_word`/text content arm,
    /// which only bloom prunes and only at decode; an attribute-equality
    /// POSTINGS prune; a stream filter), or an object at or below the
    /// block-range threshold, which the fetch reads whole in one GET regardless.
    /// Published once by partition 0, so a report can tell a fully-skip-planned
    /// query from one still paying the plan-phase read.
    plan_full_reads: Count,
    /// Whole-segment fast-path segments this partition opened with one
    /// whole-object GET, and the ones it opened by column chunk instead (issue
    /// #862). Which way the request-cost model routed a statement is otherwise
    /// invisible short of counting store requests, and the two together are this
    /// partition's fast-path segment count, so a report can see both the split
    /// and that nothing fell through it.
    fast_path_whole_object_segments: Count,
    fast_path_ranged_segments: Count,
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
            plan_full_reads: MetricBuilder::new(metrics).counter("plan_full_reads", partition),
            fast_path_whole_object_segments: MetricBuilder::new(metrics)
                .counter("fast_path_whole_object_segments", partition),
            fast_path_ranged_segments: MetricBuilder::new(metrics)
                .counter("fast_path_ranged_segments", partition),
        }
    }

    /// Records which way the whole-segment fast path routed one segment (issue
    /// #862). Called once per segment, at its first open; an `attrs_raw`
    /// fallback re-opens the same segment the same way and does not re-count.
    fn record_fast_path_route(&self, by_column_chunk: bool) {
        if by_column_chunk {
            self.fast_path_ranged_segments.add(1);
        } else {
            self.fast_path_whole_object_segments.add(1);
        }
    }

    /// Publishes why this partition did NOT take the whole-segment fast path
    /// (issue #739): one increment on the counter named by `reason`, per
    /// partition, exactly once per `execute` call that struck out. The counter is
    /// created only when a rejection happens, so a statement that takes the fast
    /// path publishes none of these names at all.
    ///
    /// Takes the metrics set rather than living on `self` because the rejection
    /// is decided before the per-partition [`BlockMetrics`] is built, and because
    /// which counter exists depends on the reason.
    fn record_fast_path_rejection(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        reason: FastPathRejection,
    ) {
        MetricBuilder::new(metrics)
            .counter(reason.metric_name(), partition)
            .add(1);
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
        let projection: Option<Vec<usize>> = projection.cloned();
        Self::build(
            tenant_hash,
            fetcher,
            segments,
            target_partitions,
            ts_min,
            ts_max,
            content,
            prune,
            erasure,
            projection,
            accounting,
            full_schema,
            declared,
            false,
        )
    }

    /// The same scan over a different projection of the same table, optionally
    /// appending the synthetic row-ref column (ADR-0774).
    ///
    /// Everything that decides which bytes are read -- the tenant, the fetcher,
    /// the segment list, the ts bounds, the content and prune predicates, the
    /// erasure list -- is carried over unchanged, so the narrow phase-1 scan
    /// this builds prunes to the same surviving blocks, in the same order, and
    /// evaluates the same exact content filter over the same surviving rows as
    /// the wide scan it replaces. That identity is what makes a row-ref
    /// resolvable: see [`crate::late_materialization`].
    ///
    /// `projection` is in terms of the resolved FULL schema, exactly like
    /// [`Self::new`]'s.
    pub(crate) fn reproject(&self, projection: Vec<usize>, row_refs: bool) -> DFResult<Self> {
        Self::build(
            self.tenant_hash,
            self.fetcher.clone(),
            &self.segments,
            self.target_partitions,
            self.ts_min,
            self.ts_max,
            Arc::clone(&self.content),
            Arc::clone(&self.prune),
            Arc::clone(&self.erasure),
            Some(projection),
            self.accounting.clone(),
            Arc::clone(&self.full_schema),
            Arc::clone(&self.declared),
            row_refs,
        )
        .map(|scan| scan.with_column_stats(self.column_stats.clone()))
    }

    /// Attach this plan's loaded column statistics (ADR-0850), resolved once
    /// per plan by [`crate::executor::SqlExecutor`] and threaded down through
    /// [`crate::logs_provider::LogsTableProvider::with_column_stats`]. A
    /// builder method rather than a constructor parameter for the same reason
    /// [`crate::logs_provider::LogsTableProvider::with_declared_columns`] is
    /// one: every existing call site of [`Self::new`] stays source-compatible.
    pub(crate) fn with_column_stats(
        mut self,
        column_stats: Option<Arc<LoadedColumnStats>>,
    ) -> Self {
        self.column_stats = column_stats;
        self
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        target_partitions: usize,
        ts_min: i64,
        ts_max: i64,
        content: Arc<Vec<Predicate>>,
        prune: Arc<Vec<Predicate>>,
        erasure: Arc<Vec<ErasurePredicate>>,
        projection: Option<Vec<usize>>,
        accounting: QueryAccounting,
        full_schema: SchemaRef,
        declared: Arc<Vec<DeclaredColumn>>,
        row_refs: bool,
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
            Some(p) => p,
            None => (0..full.fields().len()).collect(),
        };
        for &i in &projection {
            if i >= full.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "logs scan projection index {i} out of range"
                )));
            }
        }
        let resolved = resolve_columns(&projection, &content, &erasure, &declared);
        let projected_fraction = resolved.fraction_of(declared.len());
        let columns = resolved.selection;
        let projected = full.project(&projection)?;
        // The row-ref column is synthesized per row from the scan's own cursor
        // position, not decoded, so it contributes nothing to `columns` and
        // sits last, past every projected column, where a remapped column index
        // cannot collide with it.
        let schema: SchemaRef = if row_refs {
            let mut fields = projected.fields().to_vec();
            fields.push(Arc::new(row_ref_field()));
            Arc::new(Schema::new(fields))
        } else {
            Arc::new(projected)
        };
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
            projected_fraction,
            declared,
            column_stats: None,
            full_schema: full,
            schema,
            row_refs,
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

    /// Exact MIN/MAX for the declared column at schema index
    /// `FIRST_DECLARED_COL + k` over every segment this scan touches
    /// (ADR-0850), joined against `self.column_stats` by identity. `None`
    /// means the safety lemma from #849 requires falling back to scanning:
    /// no loaded column-stats object at all, an unsupported declared type
    /// (`Str`, projected as `Dictionary(Int32, Utf8)`; not implemented here),
    /// a relevant segment with no entry in the loaded stats (never built, or
    /// postdates the fold that built them), or a segment whose stat for this
    /// column has no entry or decodes to a value of the wrong kind (a
    /// corrupt/version-mismatched stat).
    ///
    /// `Some((min, max))` with both a `None`-valued scalar is still an exact
    /// answer, not a fallback: every covered segment's column was entirely
    /// null, or there are zero segments, and SQL `MIN`/`MAX` over all-NULL or
    /// zero-row input is `NULL`.
    /// Exact MIN/MAX for every declared column at once, indexed by `k`
    /// (`FIRST_DECLARED_COL + k`), resolved in a SINGLE pass over the touched
    /// segments (ADR-0850). `result[k]` is `None` with the same meaning the
    /// per-column form would give -- the safety lemma requires falling back to
    /// scanning for that column: no loaded column-stats object, an unsupported
    /// declared type (`Str`), a relevant segment with no entry in the loaded
    /// stats, or a segment whose stat for this column is missing or decodes to
    /// a value of the wrong kind.
    ///
    /// `Some((min, max))` with both a `None`-valued scalar is still exact, not
    /// a fallback: every covered segment's column was entirely null, or there
    /// are zero segments, and SQL `MIN`/`MAX` over all-NULL or zero-row input
    /// is `NULL`.
    ///
    /// Resolving every column in one segment walk (with a per-segment
    /// name->stat map) instead of one full walk per column keeps
    /// `partition_statistics` cost at `O(segments x columns_per_segment)`
    /// rather than `O(segments x declared x columns_per_segment)`; DataFusion
    /// may call `partition_statistics` several times per plan, so the per-call
    /// cost matters.
    fn declared_min_max_all(&self) -> Vec<Option<(ScalarValue, ScalarValue)>> {
        let n = self.declared.len();
        let mut result: Vec<Option<(ScalarValue, ScalarValue)>> = vec![None; n];
        let Some(stats) = self.column_stats.as_ref() else {
            return result;
        };

        // Per-column running extrema plus a "declined" flag: a column declines
        // its whole answer the moment any touched segment lacks its entry or
        // carries a wrong-kind value, exactly as the per-column form does. A
        // `Str` column declines up front.
        struct Acc {
            declined: bool,
            min: Option<ScalarValue>,
            max: Option<ScalarValue>,
        }
        let mut acc: Vec<Acc> = self
            .declared
            .iter()
            .map(|d| Acc {
                declined: matches!(d.ty, DeclaredType::Str),
                min: None,
                max: None,
            })
            .collect();

        for seg in self.segments.iter() {
            let Some(seg_stats) = stats.segments.get(&segment_identity(seg)) else {
                // A touched segment with no stats: every column must decline.
                for a in acc.iter_mut() {
                    a.declined = true;
                }
                break;
            };
            let by_name: HashMap<&str, &ravel_proto::catalog::v1::ColumnStat> = seg_stats
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c))
                .collect();
            for (k, a) in acc.iter_mut().enumerate() {
                if a.declined {
                    continue;
                }
                let declared = &self.declared[k];
                let Some(stat) = by_name.get(declared.key.as_str()) else {
                    a.declined = true;
                    continue;
                };
                // Non-null rows with no recorded extremum cannot answer MIN/MAX.
                // Without this the accumulator stays `None` and the loop below
                // substitutes a NULL scalar, which is then reported as
                // `Precision::Exact` -- claiming the extremum of non-null data is
                // exactly NULL. Fail closed and let the scan answer instead.
                if stat.non_null_count > 0 && (stat.min.is_none() || stat.max.is_none()) {
                    a.declined = true;
                    continue;
                }
                if let Some(min_val) = stat.min.as_ref() {
                    match declared_scalar(declared.ty, min_val) {
                        Some(v) => {
                            a.min = Some(match a.min.take() {
                                Some(cur)
                                    if v.partial_cmp(&cur) != Some(std::cmp::Ordering::Less) =>
                                {
                                    cur
                                }
                                _ => v,
                            });
                        }
                        None => {
                            a.declined = true;
                            continue;
                        }
                    }
                }
                if let Some(max_val) = stat.max.as_ref() {
                    match declared_scalar(declared.ty, max_val) {
                        Some(v) => {
                            a.max = Some(match a.max.take() {
                                Some(cur)
                                    if v.partial_cmp(&cur) != Some(std::cmp::Ordering::Greater) =>
                                {
                                    cur
                                }
                                _ => v,
                            });
                        }
                        None => {
                            a.declined = true;
                            continue;
                        }
                    }
                }
            }
        }

        for (k, a) in acc.into_iter().enumerate() {
            if a.declined {
                continue;
            }
            let null_scalar = declared_null_scalar(self.declared[k].ty);
            result[k] = Some((
                a.min.unwrap_or_else(|| null_scalar.clone()),
                a.max.unwrap_or(null_scalar),
            ));
        }
        result
    }

    /// Exact count of rows whose declared column at schema index
    /// `FIRST_DECLARED_COL + k` is non-null and not equal to `literal`
    /// (ADR-0850's q02 shape: `COUNT(*) WHERE <declared column> <> <literal>`,
    /// which per SQL three-valued logic excludes NULL rows the same way the
    /// scan path's per-row `<>` evaluation would). `None` means fall back to
    /// scanning, for every reason [`Self::declared_min_max_all`] does at its call
    /// site ([`Self::stats_are_exact`]: no pending erasure, no content or
    /// prune predicate, and a ts bound that clips no touched segment) plus no
    /// `Str` support, a loaded column-stats object covering every touched
    /// segment, and one reason specific to this path: any covered segment
    /// whose dictionary is absent because its distinct-value count exceeded
    /// the fold's cardinality ceiling (ADR-0850 decision 3) has no exact
    /// per-value count to subtract, so a count derived from it could be wrong
    /// outright, not merely unavailable.
    ///
    /// The [`Self::stats_are_exact`] gate is what makes a clipping ts bound
    /// safe. Segments are resolved on OVERLAP, and a pure `ts` bound is
    /// reported `Exact` by `LogsTableProvider::supports_filters_pushdown`, so
    /// the `FilterExec` that carried it is deleted and the bound survives only
    /// as `self.ts_min`/`self.ts_max`. A per-segment dictionary carries no
    /// intra-segment time distribution, so a clipped segment's contribution
    /// cannot be derived from it at all; summing its whole-segment counts
    /// would answer a different query than the one asked.
    pub(crate) fn declared_not_equal_count(&self, k: usize, literal: &ScalarValue) -> Option<u64> {
        if !self.stats_are_exact() {
            return None;
        }
        let declared = self.declared.get(k)?;
        if matches!(declared.ty, DeclaredType::Str) {
            return None;
        }
        let stats = self.column_stats.as_ref()?;
        let mut total: u64 = 0;
        for seg in self.segments.iter() {
            let seg_stats = stats.segments.get(&segment_identity(seg))?;
            let stat = seg_stats.columns.iter().find(|c| c.name == declared.key)?;
            if !stat.dictionary_present {
                return None;
            }
            let mut matching: u64 = 0;
            let mut dict_total: u64 = 0;
            for entry in &stat.dictionary {
                let value = entry.value.as_ref()?;
                let v = declared_scalar(declared.ty, value)?;
                if v == *literal {
                    matching += entry.count;
                }
                dict_total = dict_total.checked_add(entry.count)?;
            }
            // Fail closed: an internally-inconsistent record (dictionary
            // counts that do not sum to `non_null_count`) is rejected at load
            // by `decode_column_stats`, but decline here too rather than
            // subtract from a count this dictionary cannot account for.
            if dict_total != stat.non_null_count {
                return None;
            }
            total += stat.non_null_count.checked_sub(matching)?;
        }
        Some(total)
    }

    /// The declared-column index (into `self.declared`) that this scan's
    /// output column `output_col` projects, or `None` when that output column
    /// is not a declared typed column. The metadata-only aggregate rule
    /// receives filter and group-key column indices in this scan's *output*
    /// space (projection pushdown has already rewritten them by the time the
    /// rule fires), so it resolves them through `self.projection` before
    /// indexing `self.declared`; passing a raw output index straight into
    /// [`Self::declared_not_equal_count`]/[`Self::declared_group_counts`]
    /// would consult the wrong column whenever the scan is projected.
    pub(crate) fn declared_index_for_output(&self, output_col: usize) -> Option<usize> {
        let full = *self.projection.get(output_col)?;
        full.checked_sub(FIRST_DECLARED_COL)
            .filter(|k| *k < self.declared.len())
    }

    /// Exact GROUP BY value -> COUNT(*) for the declared column at schema
    /// index `FIRST_DECLARED_COL + k` (ADR-0850's q08 shape), merging every
    /// touched segment's exact dictionary. `None` means fall back to
    /// scanning, for the same reasons [`Self::declared_not_equal_count`]
    /// does, including the [`Self::stats_are_exact`] gate: this shape carries
    /// no `FilterExec` at all, so a `WHERE ts < ...` bound that clips a
    /// touched segment reaches here purely as `self.ts_min`/`self.ts_max` and
    /// nothing else in the plan would refuse for it.
    /// `ScalarValue`'s `Eq`/`Hash` are used directly as the merge key:
    /// `DeclaredType` has no floating-point variant, so the NaN/-0.0 hazards
    /// that motivate this repo's bit-pattern float-comparison rule elsewhere
    /// never arise for a declared column's value domain.
    pub(crate) fn declared_group_counts(&self, k: usize) -> Option<DeclaredGroupCounts> {
        if !self.stats_are_exact() {
            return None;
        }
        let declared = self.declared.get(k)?;
        if matches!(declared.ty, DeclaredType::Str) {
            return None;
        }
        let stats = self.column_stats.as_ref()?;
        let mut merged: HashMap<ScalarValue, u64> = HashMap::new();
        let mut null_count: u64 = 0;
        for seg in self.segments.iter() {
            let seg_stats = stats.segments.get(&segment_identity(seg))?;
            let stat = seg_stats.columns.iter().find(|c| c.name == declared.key)?;
            if !stat.dictionary_present {
                return None;
            }
            let mut dict_total: u64 = 0;
            for entry in &stat.dictionary {
                let value = entry.value.as_ref()?;
                let v = declared_scalar(declared.ty, value)?;
                dict_total = dict_total.checked_add(entry.count)?;
                *merged.entry(v).or_insert(0) += entry.count;
            }
            // Fail closed on an internally-inconsistent record, as in
            // `declared_not_equal_count`: rejected at load, declined here too.
            if dict_total != stat.non_null_count {
                return None;
            }
            null_count += stat.null_count;
        }
        Some(DeclaredGroupCounts {
            counts: merged.into_iter().collect(),
            null_count,
        })
    }

    /// Whether this scan can take the predicate-free full-window whole-segment
    /// fast path (#693 part 3 deliverable 1, amended by #739), returning the
    /// count of relevant (ts-overlapping) segments when it can and the conjunct
    /// that refused when it cannot. Decided with ZERO I/O from the resolved
    /// snapshot and `query`:
    ///
    /// - the snapshot carries no pending selective erasure, and the query
    ///   carries no block-level predicate
    ///   ([`LogQuery::is_block_predicate_free`]: no content, prune-only,
    ///   stream-attribute, or pending-erasure arm), and
    /// - every relevant segment has a well-formed span (`min <= max`) and is
    ///   fully CONTAINED in the window
    ///   (`ts_min <= seg.min && seg.max <= ts_max`). Containment is
    ///   strictly stronger than the overlap
    ///   [`LogsTableProvider::pruned_segments`] already filtered on, so no block
    ///   of a relevant segment can fall outside the window and every block
    ///   survives -- the survivor count is the whole segment.
    ///
    /// and finally there are at least `target_partitions` relevant segments, so
    /// whole-segment round-robin still fills every partition. Fewer segments than
    /// partitions is the striped path's job (deliverable 2 carries the plan
    /// footer there so its subset opens still skip re-probing); a partial
    /// overlap, a predicate, or a pending erasure falls to the unchanged
    /// plan-then-stripe path, byte for byte.
    ///
    /// # The block-range threshold is not a conjunct (issue #739)
    ///
    /// It used to be one, query-wide: every relevant segment had to satisfy
    /// `object_size > block_range_threshold`, on the reasoning that at or below
    /// the threshold there is no probe to save. Query-wide, that made a single
    /// small object veto the whole snapshot. A bulk load leaves one small tail
    /// object per `(shard, hour)`, so on the 8,424-object ClickBench tenant
    /// (#680) a predicate-free full-window statement striped after all and issued
    /// 22,473 GETs instead of 8,424.
    ///
    /// The conjunct is gone rather than made per segment, because per segment it
    /// decides nothing: at or below the threshold
    /// [`ravel_query::LogSegmentFetcher::scan_whole_accounted_with_tenant`] and
    /// the striped path's ranged entry both land in the same `whole_object_bytes`
    /// read -- one `GetRange::Full` on the `(0, object_size)` cache key, the same
    /// accounting, and no etag pin on either, since one GET observes one object
    /// state. So a sub-threshold segment reads identically whichever entry opens
    /// it, and it can join the whole-segment assignment while the above-threshold
    /// segments around it keep the probe the fast path removes.
    ///
    /// # This decides the assignment, not the read (issue #862)
    ///
    /// Every conjunct here is about which BLOCKS survive, and the answer it
    /// establishes is always "all of them" -- which is what lets the plan phase
    /// go. None of them is about which COLUMNS the projection wants, and under
    /// RLOG v4 those are independent questions: reading every block no longer
    /// means needing every byte. So the read shape is chosen per segment at open
    /// time by [`PartitionCtx::open_by_column_chunk`], and a narrow projection
    /// takes the probe-and-range path from inside this fast path rather than
    /// falling out of it. Rejecting here instead would be strictly worse: the
    /// plan-then-stripe path it falls to adds a whole plan pass per segment, so
    /// a narrow statement would pay MORE requests to move fewer bytes.
    fn whole_segment_fast_path(&self, query: &LogQuery) -> Result<usize, FastPathRejection> {
        // Checked ahead of `is_block_predicate_free` (which folds erasure in)
        // only so the recorded reason names erasure rather than the generic
        // block-predicate arm.
        if !self.erasure.is_empty() {
            return Err(FastPathRejection::PendingErasure);
        }
        if !query.is_block_predicate_free() {
            return Err(FastPathRejection::BlockPredicate);
        }
        let mut relevant = 0usize;
        for seg in self.segments.iter() {
            if !LogSegmentFetcher::ts_range_relevant(seg, self.ts_min, self.ts_max) {
                continue;
            }
            relevant += 1;
            let contained = seg.min_event_ts_ns <= seg.max_event_ts_ns
                && self.ts_min <= seg.min_event_ts_ns
                && seg.max_event_ts_ns <= self.ts_max;
            if !contained {
                return Err(FastPathRejection::SegmentNotContained);
            }
        }
        if relevant < self.target_partitions {
            return Err(FastPathRejection::FewerSegmentsThanPartitions);
        }
        Ok(relevant)
    }

    /// Indices into the resolved full schema this scan emits, in output order
    /// (ADR-0774: what a late-materialization rewrite narrows and then
    /// restores).
    pub(crate) fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Whether this scan can be split into a narrow phase 1 and a row-ref
    /// fetch (ADR-0774).
    ///
    /// The one refusal is a pending selective erasure. A row-ref addresses a
    /// row by its position in the block's surviving-row list, and the scan
    /// layer's erasure exclusion ([`retain_unerased`]) removes rows from that
    /// list after the reader produced it, so the position a phase-1 row
    /// carries would not be the position phase 2 reads. Refusing is also the
    /// fail-closed direction: the failure mode of getting erasure wrong is an
    /// erased record served to a client.
    ///
    /// A scan that already emits row refs is not a candidate either: it is
    /// itself phase 1 of a rewrite this rule already performed.
    pub(crate) fn late_materialization_candidate(&self) -> bool {
        self.erasure.is_empty() && !self.row_refs
    }

    /// Everything [`crate::late_materialization::LogsRowFetchExec`] needs to
    /// re-read this scan's rows one block at a time (ADR-0774). Every field is
    /// this scan's own, so phase 2 fetches the same objects, prunes to the same
    /// surviving blocks, and decodes the same columns the single-phase scan
    /// would have.
    pub(crate) fn row_fetch_source(&self) -> RowFetchSource {
        RowFetchSource {
            tenant_hash: self.tenant_hash,
            fetcher: self.fetcher.clone(),
            segments: Arc::clone(&self.segments),
            ts_min: self.ts_min,
            ts_max: self.ts_max,
            content: Arc::clone(&self.content),
            prune: Arc::clone(&self.prune),
            columns: self.columns.clone(),
            projection: Arc::clone(&self.projection),
            declared: Arc::clone(&self.declared),
            schema: Arc::clone(&self.schema),
            accounting: self.accounting.clone(),
            concurrency: self.target_partitions,
        }
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

/// The read half of a [`LogsScanExec`], detached so a second phase can re-open
/// individual blocks of it (ADR-0774).
///
/// It is a value, not a plan node: [`crate::late_materialization::
/// LogsRowFetchExec`] holds one and uses it to turn a batch of row refs into
/// the rows the wide single-phase scan would have emitted. Every field is
/// cloned straight off the scan that produced the row refs, so the fetch it
/// drives is the same fetch, restricted to one block.
#[derive(Clone)]
pub(crate) struct RowFetchSource {
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    /// Every segment in the snapshot, in snapshot order. A row-ref's segment
    /// field indexes this.
    segments: Arc<Vec<SegmentRef>>,
    ts_min: i64,
    ts_max: i64,
    content: Arc<Vec<Predicate>>,
    prune: Arc<Vec<Predicate>>,
    /// The FULL projection's column selection, i.e. what the single-phase scan
    /// would have decoded. Used as both the fetch and the decode selection, as
    /// `scan_accounted_with_tenant_subset` requires (ADR-0699 decision 5).
    columns: ColumnSelection,
    projection: Arc<Vec<usize>>,
    declared: Arc<Vec<DeclaredColumn>>,
    /// The scan's original output schema, which is also this fetch's: the
    /// rewrite restores column order, names, and nullability exactly.
    schema: SchemaRef,
    accounting: QueryAccounting,
    /// How many block fetches may be in flight at once. The scan's declared
    /// partition count, which is the query's `target_partitions`: phase 2 has
    /// at most `k` blocks to read and no partitions of its own, so it borrows
    /// the same fan-out figure rather than inventing one.
    concurrency: usize,
}

impl RowFetchSource {
    /// This fetch's output schema: the original scan's, with no row-ref column.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The number of columns the restored projection carries, for `EXPLAIN`.
    pub(crate) fn projected_columns(&self) -> usize {
        self.projection.len()
    }

    /// How many block fetches may be in flight at once.
    pub(crate) fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// The [`LogQuery`] the originating scan ran. Rebuilt from the same fields
    /// in the same order as [`LogsScanExec::execute`], so the pruning is
    /// identical and a row-ref's surviving-block position means the same thing
    /// in both phases. Erasure is deliberately absent: a scan carrying one is
    /// not a late-materialization candidate
    /// ([`LogsScanExec::late_materialization_candidate`]).
    fn query(&self) -> LogQuery {
        let mut query = LogQuery::new(self.ts_min, self.ts_max);
        for c in self.content.iter() {
            query = query.with_content(c.clone());
        }
        for p in self.prune.iter() {
            query = query.with_prune(p.clone());
        }
        query
    }

    /// Decode one block of one segment and return the records at the named
    /// surviving-row positions, paired with the output position each belongs
    /// at.
    ///
    /// `block` is a position in the segment's surviving-block list for this
    /// query, exactly as [`LogsScanExec`]'s striped path uses, and `rows` are
    /// positions in that block's surviving-row list. Both are what phase 1
    /// recorded while draining the same block of the same immutable object
    /// under the same query, so both resolve; an index that does not is a typed
    /// error, never a wrong row.
    pub(crate) async fn fetch_block(
        &self,
        segment: usize,
        block: usize,
        rows: &[(usize, usize)],
    ) -> DFResult<Vec<(usize, LogRecord)>> {
        let seg = self.segments.get(segment).ok_or_else(|| {
            DataFusionError::Internal(format!("row-ref segment ordinal {segment} out of range"))
        })?;
        let query = self.query();
        let opened = self
            .fetcher
            .scan_accounted_with_tenant_subset(
                seg,
                self.tenant_hash,
                &query,
                &self.columns,
                &[block],
                None,
                &self.accounting,
            )
            .await
            .map_err(SqlError::from)?;
        // `None` means the catalog summary proved the segment ts-irrelevant.
        // Phase 1 read a row out of it under the same bounds, so this cannot
        // happen; say so as an error rather than silently returning no rows.
        let Some(mut scan) = opened else {
            return Err(DataFusionError::Internal(format!(
                "row-ref segment {} became ts-irrelevant between phases",
                seg.data_object_key
            )));
        };
        let records = scan.next_block().map_err(SqlError::from)?.ok_or_else(|| {
            DataFusionError::Internal(format!(
                "row-ref block {block} not in segment {}'s surviving list",
                seg.data_object_key
            ))
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for &(row, position) in rows {
            let record = records.get(row).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "row-ref row {row} past block {block}'s {} surviving rows in segment {}",
                    records.len(),
                    seg.data_object_key
                ))
            })?;
            out.push((position, record.clone()));
        }
        Ok(out)
    }

    /// Build the output batch for `records`, in the original scan's schema.
    pub(crate) fn build_batch(&self, records: &[LogRecord]) -> DFResult<RecordBatch> {
        build_batch(
            records,
            Arc::clone(&self.schema),
            &self.projection,
            &self.declared,
            None,
        )
    }

    /// Rows accumulated into one output batch, shared with the scan so a
    /// late-materialized result is chunked exactly as the single-phase one.
    pub(crate) const BATCH_ROWS: usize = BATCH_ROWS;
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

    /// Report the exact row count and `ts` span straight from the catalog's
    /// committed row counts and segment bounds, for any query where nothing
    /// removes a row the committed counts still include: no content/prune
    /// predicate, no pending erasure, and a `ts` bound (if any) that fully
    /// CONTAINS every resolved segment (issue #698, widened by #723).
    ///
    /// For the genuinely predicate-free query (no `ts` bound at all, the
    /// trivial contained case) this statistic reaches DataFusion's
    /// `AggregateStatistics` physical-optimizer rule undisturbed and the rule
    /// rewrites the aggregate into a literal, so this scan is never executed
    /// (issue #698: on the ClickBench tenant, issue #680, a scanning
    /// `count(*)` moved 23 GB from object storage to add up 8424 numbers the
    /// resolve already had). A query with an actual, contained `ts` bound now
    /// reaches the rule the same way: `LogsTableProvider::supports_filters_\
    /// pushdown` reports a pure `ts` bound `Exact` (issue #733), so no
    /// `FilterExec` survives above this node to report its own non-exact
    /// statistics in place of this one.
    ///
    /// `num_rows` is `Exact` only for the whole-plan request (`partition` is
    /// `None`) and only when [`Self::stats_are_exact`] holds; a per-partition
    /// request gets `Absent`, because a partition's count is its lazily
    /// resolved striped share of the blocks, not known here. Under the same
    /// condition the `ts` column's `column_statistics` report an `Exact`
    /// min/max spanning every touched segment; every other column's stays
    /// `Absent`. `total_byte_size` stays `Absent`.
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
            ) && let Some(ts_idx) = self.projection.iter().position(|&i| i == LOG_COL_TS)
            {
                let col = &mut stats.column_statistics[ts_idx];
                col.min_value = Precision::Exact(ScalarValue::TimestampNanosecond(Some(min), None));
                col.max_value = Precision::Exact(ScalarValue::TimestampNanosecond(Some(max), None));
            }

            // ADR-0850: the same gate widens to a declared column's exact
            // min/max, joined against `self.column_stats` by identity rather
            // than ordinal position. `declared_min_max_all` resolves every
            // declared column in one segment walk and enforces the per-column
            // fallback (an uncovered segment, a corrupt/type-mismatched stat,
            // or an unsupported declared type all report `None`, leaving the
            // column `Absent`); this loop only decides which output index to
            // fill.
            let declared_min_max = self.declared_min_max_all();
            for (k, min_max) in declared_min_max.into_iter().enumerate() {
                let schema_idx = FIRST_DECLARED_COL + k;
                if let Some(out_idx) = self.projection.iter().position(|&i| i == schema_idx)
                    && let Some((min, max)) = min_max
                {
                    let col = &mut stats.column_statistics[out_idx];
                    col.min_value = Precision::Exact(min);
                    col.max_value = Precision::Exact(max);
                }
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
            projected_fraction: self.projected_fraction,
            accounting: self.accounting.clone(),
        });

        // #693 part 3 deliverable 1, amended by #739: a predicate-free query
        // whose window fully contains every relevant segment, with at least
        // `target_partitions` of them, skips the plan phase entirely, whatever
        // those segments' sizes are. Each partition computes its whole-segment
        // round-robin share with no I/O and opens each owned segment with one
        // whole-object GET (no plan probe, no scan-side probe). Any other shape
        // falls to the plan-then-stripe path below, byte for byte, and records
        // which conjunct sent it there.
        let (work, fast_whole_segment, state) = match self.whole_segment_fast_path(&ctx.query) {
            Ok(relevant) => {
                let n = self.target_partitions.max(1).min(relevant.max(1));
                let work =
                    owned_whole_segments(&self.segments, self.ts_min, self.ts_max, partition, n);
                (work, true, LogScanState::NextSegment)
            }
            Err(reason) => {
                BlockMetrics::record_fast_path_rejection(&self.metrics, partition, reason);
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
            }
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
            row_refs: self.row_refs,
            current_seg: None,
            current_seg_ordinal: 0,
            block_cursor: 0,
            pending_range: None,
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
    /// Relevant segments the plan phase read whole instead of counting from the
    /// skip index (#761). A segment planned from footer alone or from the skip
    /// index carries its plan footer forward (`SegPlan::footer` is `Some`); the
    /// whole-object fallback carries none, so this is the count of relevant
    /// segments with no footer, published as the `plan_full_reads` metric (see
    /// [`BlockMetrics::plan_full_reads`] for the two causes).
    full_reads: usize,
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
    let mut full_reads = 0usize;
    for entry in planned {
        match entry {
            Some((survivors, stats, footer)) => {
                total_blocks += survivors;
                // A relevant segment planned from the skip index carries its
                // footer forward; the whole-object fallback (#761) carries none.
                if footer.is_none() {
                    full_reads += 1;
                }
                segs.push(Some(SegPlan {
                    survivors,
                    stats,
                    footer,
                }));
            }
            None => segs.push(None),
        }
    }
    Ok(Arc::new(PlanCounts {
        segs,
        total_blocks,
        full_reads,
    }))
}

/// One segment this partition owns blocks in, and the block-index list (into
/// the segment's surviving-block list) it must drain.
struct OwnedSeg {
    seg: SegmentRef,
    /// This segment's position in the snapshot's segment list, i.e. the
    /// segment field of every row-ref built from it (ADR-0774).
    ordinal: usize,
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
                    ordinal: seg_idx,
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
                    ordinal: seg_idx,
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
    for (seg_idx, seg) in segments.iter().enumerate() {
        if !LogSegmentFetcher::ts_range_relevant(seg, ts_min, ts_max) {
            continue;
        }
        if ordinal % n == partition {
            work.push_back(OwnedSeg {
                seg: seg.clone(),
                ordinal: seg_idx,
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
    /// [`LogsScanExec::projected_fraction`], carried so the whole-segment fast
    /// path can route each segment as it opens it (issue #862).
    projected_fraction: f64,
    accounting: QueryAccounting,
}

impl PartitionCtx {
    /// Whether the whole-segment fast path should open `seg` by column chunk
    /// rather than with one whole-object GET (issue #862).
    ///
    /// The fast path's own conjuncts ([`LogsScanExec::whole_segment_fast_path`])
    /// prove every block of `seg` survives, which is why the whole-object read
    /// was unconditionally optimal under RLOG v3: reading every block meant
    /// needing every byte. Under v4 it does not. Every block surviving says
    /// nothing about how many COLUMNS the projection wants, and the ranged entry
    /// point ([`open_segment_ranged`]) already fetches one coalesced range per
    /// surviving `(row group, projected column)` from the very same
    /// [`ColumnSelection`] (ADR-0699 decision 5), so a narrow projection can
    /// leave most of the object unread.
    ///
    /// The arbiter is the fetch layer's request-cost model, not a threshold
    /// invented here: the ranged path pays only when the bytes the projection
    /// skips outweigh the round trips the protocol adds. That keeps a wide
    /// projection (`SELECT *`, or any reference to the merged `attrs` map) on
    /// the unchanged whole-object read, where it belongs -- the ranged path
    /// would fetch the same bytes and pay a probe on top.
    fn open_by_column_chunk(&self, seg: &SegmentRef) -> bool {
        self.fetcher
            .ranged_projection_pays(seg.object_size, self.projected_fraction)
    }
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
/// whole-object read is optimal for a projection wide enough to want most of
/// the object's bytes ([`PartitionCtx::open_by_column_chunk`]) and no plan phase
/// or suffix probe is needed.
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

/// The whole-segment fast path's other entry point (issue #862): open the same
/// whole segment through the probe-and-range protocol, which brings one
/// coalesced range per surviving `(row group, projected column)` instead of
/// every byte of the object.
///
/// Taken when [`PartitionCtx::open_by_column_chunk`] judges the skipped bytes
/// worth the extra round trips. It passes the SAME [`ColumnSelection`] the
/// decode uses, which is what ADR-0699 decision 5 requires of a version-4 fetch,
/// and no block-index subset: the fast path's conjuncts already proved every
/// block of this segment survives, so the ranged read's candidate set is the
/// whole segment and the rows it yields are the rows
/// [`open_segment_whole`] would have yielded.
fn open_segment_ranged(ctx: Arc<PartitionCtx>, seg: SegmentRef) -> OpenFuture {
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

/// One segment's open on the whole-segment fast path, routed by
/// [`PartitionCtx::open_by_column_chunk`].
fn open_segment_fast(ctx: Arc<PartitionCtx>, seg: SegmentRef, by_column_chunk: bool) -> OpenFuture {
    if by_column_chunk {
        open_segment_ranged(ctx, seg)
    } else {
        open_segment_whole(ctx, seg)
    }
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

/// The address a run of consecutive output rows carries in the synthetic
/// row-ref column (ADR-0774): the block they were decoded from, plus the
/// surviving-row position of the first of them.
///
/// `segment` is a position in the snapshot's segment list, `block` a position
/// in that segment's surviving-block list for this query, and `first_row` a
/// position in that block's surviving-row list. All three are cursor state the
/// scan already has; nothing is decoded to produce them.
#[derive(Clone, Copy)]
struct RowRefRange {
    segment: usize,
    block: usize,
    first_row: usize,
}

/// The surviving-block index of the block a partition's cursor is about to
/// yield, or `None` when the scan emits no row refs (ADR-0774).
///
/// `indices` is the partition's own list of surviving-block positions, so the
/// cursor's `i`-th block is that list's `i`-th entry. The whole-segment fast
/// path leaves the list empty and drains every survivor in order, so there the
/// cursor position *is* the surviving-block index.
///
/// A free function rather than a method because the columnar drain calls it
/// while the block cursor holds a mutable borrow of the stream's state field.
fn block_index(row_refs: bool, indices: &[usize], cursor: usize) -> DFResult<Option<usize>> {
    if !row_refs {
        return Ok(None);
    }
    if indices.is_empty() {
        return Ok(Some(cursor));
    }
    indices.get(cursor).copied().map(Some).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "row-ref cursor at block {cursor} past this partition's {} owned blocks",
            indices.len()
        ))
    })
}

/// The row-ref column for `rows` consecutive rows starting at `range`.
fn row_ref_array(range: RowRefRange, rows: usize) -> DFResult<ArrayRef> {
    let mut packed = Vec::with_capacity(rows);
    for i in 0..rows {
        packed.push(
            RowRef {
                segment: range.segment,
                block: range.block,
                row: range.first_row + i,
            }
            .pack()?,
        );
    }
    Ok(Arc::new(UInt64Array::from(packed)))
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
    /// with no plan phase, each segment is opened through [`open_segment_fast`]
    /// (one whole-object GET, or the ranged path when the projection is narrow
    /// enough to pay for it -- #862), and the segment's whole-segment prune
    /// totals are recorded per segment at exhaustion (each segment has one owner,
    /// so no double count) instead of by partition 0 during planning.
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
    /// Whether this stream appends the synthetic row-ref column (ADR-0774).
    row_refs: bool,
    /// The segment currently being drained, kept so the `attrs_raw` fallback can
    /// re-open it on the row path. Set when a segment's open resolves.
    current_seg: Option<SegmentRef>,
    /// [`Self::current_seg`]'s position in the snapshot's segment list, i.e.
    /// the segment field of every row-ref stamped while draining it.
    current_seg_ordinal: usize,
    /// How many blocks of the current segment's cursor this partition has
    /// consumed, i.e. the position within [`Self::current_indices`] of the
    /// block being drained. Reset when a new segment starts and carried across
    /// the `attrs_raw` re-open, which resumes at exactly this position. It is
    /// what turns a cursor position into a stable surviving-block index
    /// ([`Self::current_block`]).
    block_cursor: usize,
    /// The address the row-path batch builder stamps from while draining the
    /// held block. `None` when this scan emits no row refs.
    pending_range: Option<RowRefRange>,
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
    /// The surviving-block index of the block the cursor is about to yield, or
    /// `None` when this scan emits no row refs.
    ///
    /// `current_indices` is this partition's own list of surviving-block
    /// positions, so the cursor's `i`-th block is that list's `i`-th entry. The
    /// whole-segment fast path leaves the list empty and drains every survivor
    /// in order, so there the cursor position *is* the surviving-block index.
    fn current_block(&self) -> DFResult<Option<usize>> {
        block_index(self.row_refs, &self.current_indices, self.block_cursor)
    }

    /// The row-ref address for the block the cursor is about to yield, and
    /// advance the cursor past it.
    fn take_block_range(&mut self) -> DFResult<Option<RowRefRange>> {
        let range = self.current_block()?.map(|block| RowRefRange {
            segment: self.current_seg_ordinal,
            block,
            first_row: 0,
        });
        self.block_cursor += 1;
        Ok(range)
    }

    /// Emit the next row-path batch out of `pending`, moving the reservation
    /// with it: the previous batch's charge is released (it is downstream's
    /// now), the new batch's charge is taken before it is handed over.
    fn emit_next_row_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let pending_range = self.pending_range;
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
            pending_range.map(|r| RowRefRange {
                first_row: r.first_row + *pos,
                ..r
            }),
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
                            this.blocks.plan_full_reads.add(counts.full_reads);
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
                        ordinal,
                        indices,
                        footer,
                    }) => {
                        this.current_seg = Some(seg.clone());
                        this.current_seg_ordinal = ordinal;
                        this.current_indices = indices.clone();
                        this.current_footer = footer.clone();
                        this.seg_columnar_blocks = 0;
                        this.block_cursor = 0;
                        // Whole-segment fast path reads the object in one GET
                        // (#693 part 3), or by column chunk when the projection
                        // is narrow enough to pay for the extra round trips
                        // (#862); the striped path opens only this partition's
                        // subset, reusing the plan footer if any.
                        this.state = if this.fast_whole_segment {
                            let by_chunk = this.ctx.open_by_column_chunk(&seg);
                            this.blocks.record_fast_path_route(by_chunk);
                            LogScanState::Opening(open_segment_fast(
                                Arc::clone(&this.ctx),
                                seg,
                                by_chunk,
                            ))
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
                                // The row-ref address of the block just decoded,
                                // resolved from the cursor position alone. It is
                                // resolved here rather than before the decode
                                // because the cursor sits one past its last block
                                // once the scan is exhausted, and folded into
                                // `Step::Failed` rather than returned early
                                // because `scan` borrows `this.state` for the
                                // whole arm.
                                let built = block_index(
                                    this.row_refs,
                                    &this.current_indices,
                                    this.block_cursor,
                                )
                                .and_then(|block| {
                                    build_columnar_batches(
                                        &view,
                                        &this.schema,
                                        &this.projection,
                                        &this.declared,
                                        block.map(|block| RowRefRange {
                                            segment: this.current_seg_ordinal,
                                            block,
                                            first_row: 0,
                                        }),
                                    )
                                });
                                match built {
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
                            // Re-opened through the same routing decision the
                            // first open took (#862), and deliberately not
                            // re-counted: the route metric counts segments, not
                            // opens.
                            let fut = if this.fast_whole_segment {
                                let by_chunk = this.ctx.open_by_column_chunk(&seg);
                                open_segment_fast(Arc::clone(&this.ctx), seg, by_chunk)
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
                            // blocks the columnar cursor advanced past. The
                            // row-ref cursor moves with it, so a fallback
                            // re-opens at the same surviving-block position.
                            this.seg_columnar_blocks += 1;
                            this.block_cursor += 1;
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
                            // Stamp the block's row-ref address before the
                            // records are held: the batch builder reads it out
                            // of `pending_range` as it chunks them.
                            match this.take_block_range() {
                                Ok(range) => this.pending_range = range,
                                Err(e) => return this.fail(e),
                            }
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
    row_refs: Option<RowRefRange>,
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
    if let Some(range) = row_refs {
        columns.push(row_ref_array(range, records.len())?);
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

/// A FIELD_DIR column of a declared key, resolved once for the whole block into
/// a per-column cursor (#875). The scan's row loop read one cell with one
/// `HashMap<u32, _>` lookup per cell; the cursor resolves the column's storage
/// once and then indexes the resolved slice per row with no lookup.
enum DeclaredCursor<'a> {
    Str(BytesCursor<'a>),
    Bytes(BytesCursor<'a>),
    I64(I64Cursor<'a>),
    F64(F64BitsCursor<'a>),
    Bool(BoolCursor<'a>),
}

impl<'a> DeclaredCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, col: AttrColumn) -> Self {
        match col.ty {
            FieldType::Str => DeclaredCursor::Str(view.bytes_cursor(col.column_id)),
            FieldType::Bytes => DeclaredCursor::Bytes(view.bytes_cursor(col.column_id)),
            FieldType::I64 => DeclaredCursor::I64(view.i64_cursor(col.column_id)),
            FieldType::F64 => DeclaredCursor::F64(view.f64_bits_cursor(col.column_id)),
            FieldType::Bool => DeclaredCursor::Bool(view.bool_cursor(col.column_id)),
        }
    }

    /// Whether the record sets the key in this column at surviving row `i`, with
    /// the same "readable" rule the row path uses: an invalid-UTF-8 `Str` cell is
    /// not a value (matches [`read_str_cell`]), so it must not suppress the
    /// resource/scope fallback.
    fn present_at(&self, i: usize) -> bool {
        match self {
            DeclaredCursor::Str(c) => c.str_at(i).is_some(),
            DeclaredCursor::Bytes(c) => c.at(i).is_some(),
            DeclaredCursor::I64(c) => c.at(i).is_some(),
            DeclaredCursor::F64(c) => c.at(i).is_some(),
            DeclaredCursor::Bool(c) => c.at(i).is_some(),
        }
    }

    /// The cell at surviving row `i` as an [`AttrValue`], or `None` when NULL (or,
    /// for `Str`, not UTF-8). Used for the non-`Str` builders and to compare a
    /// record value's variant against the declared type; the `Str` builder reads
    /// `&str` directly so it never allocates a throwaway `String` (#875).
    fn value_at(&self, i: usize) -> Option<AttrValue> {
        match self {
            DeclaredCursor::Str(c) => c.str_at(i).map(|s| AttrValue::Str(s.to_string())),
            DeclaredCursor::Bytes(c) => c.at(i).map(|b| AttrValue::Bytes(b.to_vec())),
            DeclaredCursor::I64(c) => c.at(i).map(AttrValue::I64),
            DeclaredCursor::F64(c) => c.at(i).map(|bits| AttrValue::F64(f64::from_bits(bits))),
            DeclaredCursor::Bool(c) => c.at(i).map(AttrValue::Bool),
        }
    }
}

/// One declared column's FIELD_DIR resolution for a block, done once rather than
/// per row (ADR-0099 decision 2). Every FIELD_DIR column of the key is resolved
/// to a cursor once when the plan is built; the row loop then reads through the
/// cursors with no per-cell column lookup (#875).
struct DeclaredPlan<'d, 'a> {
    dc: &'d DeclaredColumn,
    /// The raw FIELD_DIR columns of this key, across all stored types. Kept
    /// because [`ColumnarBlockView::str_dict`] resolves by column id.
    cols: Vec<AttrColumn>,
    /// Cursors parallel to [`Self::cols`], resolved once for the block.
    cursors: Vec<DeclaredCursor<'a>>,
    /// Index into [`Self::cols`]/[`Self::cursors`] of the declared-type column,
    /// if the key has one. A record row whose value lives here reads that value;
    /// a row whose value lives in a different-typed column of the same key reads
    /// NULL (record wins, wrong variant), matching the row path exactly.
    matching_idx: Option<usize>,
}

impl<'d, 'a> DeclaredPlan<'d, 'a> {
    fn build(view: &ColumnarBlockView<'a>, dc: &'d DeclaredColumn) -> DeclaredPlan<'d, 'a> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
        let declared_ty = declared_field_type(dc.ty);
        let matching_idx = cols.iter().position(|c| c.ty == declared_ty);
        let cursors = cols
            .iter()
            .map(|&c| DeclaredCursor::resolve(view, c))
            .collect();
        DeclaredPlan {
            dc,
            cols,
            cursors,
            matching_idx,
        }
    }

    /// The declared-type FIELD_DIR column, if the key has one.
    fn matching_col(&self) -> Option<AttrColumn> {
        self.matching_idx.map(|k| self.cols[k])
    }

    /// The cursor over the declared-type column, if any.
    fn matching_cursor(&self) -> Option<&DeclaredCursor<'a>> {
        self.matching_idx.map(|k| &self.cursors[k])
    }

    /// True when the key's only FIELD_DIR column is the declared-type one, so a
    /// single cursor read is both the presence answer and the value: the
    /// double read (`attr_present` then `read_typed_cell`) collapses to one
    /// (deliverable 2, #875).
    fn single_matching(&self) -> bool {
        self.cursors.len() == 1 && self.matching_idx == Some(0)
    }

    /// Whether the record sets the key in any of its FIELD_DIR columns at
    /// surviving row `i`. Only consulted in the multi-column case; the fused case
    /// gets the same answer from the single matching read.
    fn record_sets_key(&self, i: usize) -> bool {
        self.cursors.iter().any(|c| c.present_at(i))
    }
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
    plan: &DeclaredPlan<'_, '_>,
    i: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<Option<AttrValue>> {
    if plan.single_matching() {
        // Fused: the one matching-column read is both the presence test and the
        // value (deliverable 2, #875). `value_at` is `Some` exactly when the
        // record sets the key at the declared type; `None` (absent, or a `Str`
        // cell that is not UTF-8) falls through to the resource/scope value,
        // matching the row path.
        if let Some(v) = plan.matching_cursor().and_then(|c| c.value_at(i)) {
            return Ok(Some(v));
        }
    } else if plan.record_sets_key(i) {
        // Record wins. Its value is the declared-type column's cell, or NULL when
        // the record set the key only in a different-typed column (ADR-0090
        // decision 7); either way the resource/scope fallback is not consulted.
        return Ok(plan.matching_cursor().and_then(|c| c.value_at(i)));
    }
    let Some(stream_ref) = view.stream_ref(i) else {
        return Ok(None);
    };
    let resource = resource_attrs(view, cache, stream_ref)?;
    Ok(find_attr(resource, &plan.dc.key).cloned())
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
    plan: &DeclaredPlan<'_, '_>,
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
///   non-null surviving row with keys `0..`. The record's own `Str` value is read
///   as `&str` straight from the [`BytesCursor`] into the builder, so no
///   throwaway `String` is allocated per cell (deliverable 3, #875). No hashing
///   and no dedup pass, so this case stays exactly as expensive as it was.
fn build_declared_str_columnar(
    view: &ColumnarBlockView<'_>,
    plan: &DeclaredPlan<'_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let n = end - start;
    Ok(
        match plan
            .matching_col()
            .and_then(|mc| view.str_dict(mc.column_id))
        {
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
                    if plan.record_sets_key(i) {
                        // Record wins. `id_at` is `Some` only when the record's
                        // `Str` column has a value at this row; `None` (record set
                        // the key in another-typed column) is a NULL cell. An `id`
                        // pointing at a non-UTF-8 (NULL) dictionary value also reads
                        // NULL, matching the UTF-8 rule on the `Str` cursor.
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
                // Identity dictionary: one entry per non-null row, no dedup. The
                // record's own value is appended as `&str` straight from the
                // cursor -- no per-cell `String` (deliverable 3, #875).
                let mut values = StringBuilder::new();
                let mut keys: Vec<Option<i32>> = Vec::with_capacity(n);
                let mut next = 0i32;
                // The declared-type (`Str`) column's cursor, when the key has a
                // plain-page `Str` column; `None` when it has no `Str` column at
                // all (then only the resource/scope fallback yields a value).
                let matching_str = match plan.matching_cursor() {
                    Some(DeclaredCursor::Str(c)) => Some(c),
                    _ => None,
                };
                for i in start..end {
                    // `appended` == "this row is decided, do not consult the
                    // resource/scope fallback" (record wins over resource).
                    let mut appended = false;
                    if plan.single_matching() {
                        // Fused: one cursor read is both presence and value.
                        if let Some(s) = matching_str.and_then(|c| c.str_at(i)) {
                            values.append_value(s);
                            keys.push(Some(next));
                            next += 1;
                            appended = true;
                        }
                        // A `None` here (absent, or not UTF-8) falls through to
                        // the fallback, matching the row path.
                    } else if plan.record_sets_key(i) {
                        // Record wins: its declared-type `Str` cell, or NULL when
                        // it set the key only in a different-typed column.
                        match matching_str.and_then(|c| c.str_at(i)) {
                            Some(s) => {
                                values.append_value(s);
                                keys.push(Some(next));
                                next += 1;
                            }
                            None => keys.push(None),
                        }
                        appended = true;
                    }
                    if appended {
                        continue;
                    }
                    if let Some(stream_ref) = view.stream_ref(i) {
                        let resource = resource_attrs(view, cache, stream_ref)?;
                        match find_attr(resource, &plan.dc.key) {
                            Some(AttrValue::Str(s)) => {
                                values.append_value(s);
                                keys.push(Some(next));
                                next += 1;
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
    row_refs: Option<RowRefRange>,
) -> DFResult<Vec<RecordBatch>> {
    let n = view.surviving_count();
    // Resolve each projected declared column's FIELD_DIR columns to cursors once
    // for the whole block (ADR-0099 decision 2, #875), not per row and not per
    // chunk: the row loop then reads through the cursors with no column lookup.
    let mut plans: HashMap<usize, DeclaredPlan<'_, '_>> = HashMap::new();
    for &idx in projection {
        if idx >= FIRST_DECLARED_COL
            && let Some(dc) = declared.get(idx - FIRST_DECLARED_COL)
        {
            plans.insert(idx, DeclaredPlan::build(view, dc));
        }
    }
    let mut cache: HashMap<u32, Arc<Vec<(String, AttrValue)>>> = HashMap::new();
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + BATCH_ROWS).min(n);
        out.push(build_columnar_batch(
            view,
            schema,
            projection,
            &plans,
            &mut cache,
            start,
            end,
            row_refs.map(|r| RowRefRange {
                first_row: r.first_row + start,
                ..r
            }),
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
    plans: &HashMap<usize, DeclaredPlan<'_, '_>>,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    start: usize,
    end: usize,
    row_refs: Option<RowRefRange>,
) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &idx in projection {
        let array: ArrayRef = match idx {
            LOG_COL_TS => {
                let cur = view.ts_cursor();
                Arc::new(TimestampNanosecondArray::from(
                    (start..end)
                        .map(|i| cur.at(i).unwrap_or_default())
                        .collect::<Vec<_>>(),
                ))
            }
            LOG_COL_OBSERVED_TS => {
                let cur = view.observed_ts_cursor();
                Arc::new(TimestampNanosecondArray::from(
                    (start..end)
                        .map(|i| cur.at(i).unwrap_or_default())
                        .collect::<Vec<_>>(),
                ))
            }
            LOG_COL_SEVERITY_NUM => {
                let cur = view.severity_num_cursor();
                Arc::new(UInt8Array::from(
                    (start..end)
                        .map(|i| cur.at(i).unwrap_or_default() as u8)
                        .collect::<Vec<_>>(),
                ))
            }
            LOG_COL_SEVERITY_TEXT => {
                let cur = view.severity_text_cursor();
                let mut b = StringBuilder::new();
                for i in start..end {
                    match cur.at(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_BODY => {
                let cur = view.body_cursor();
                let mut b = StringBuilder::new();
                for i in start..end {
                    match cur.at(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_TRACE_ID => {
                let cur = view.trace_id_cursor();
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, TRACE_ID_WIDTH);
                for i in start..end {
                    match cur.at(i) {
                        Some(id) => b.append_value(id).map_err(|e| {
                            SqlError::Internal(format!("trace_id array build: {e}"))
                        })?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_SPAN_ID => {
                let cur = view.span_id_cursor();
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, SPAN_ID_WIDTH);
                for i in start..end {
                    match cur.at(i) {
                        Some(id) => b
                            .append_value(id)
                            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_FLAGS => {
                let cur = view.flags_cursor();
                Arc::new(UInt32Array::from(
                    (start..end)
                        .map(|i| cur.at(i).unwrap_or_default() as u32)
                        .collect::<Vec<_>>(),
                ))
            }
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
    if let Some(range) = row_refs {
        columns.push(row_ref_array(range, end - start)?);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    // Carry the row count explicitly so an empty projection (a bare `COUNT(*)`)
    // still reports its rows, exactly as the row path does.
    let options = RecordBatchOptions::new().with_row_count(Some(end - start));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(DataFusionError::from)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod columnar_lookup_tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{
        ColumnSelection, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter,
    };
    use ravel_types::logstream::LogStreamId;

    fn sid(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn rec_k(ts: i64, k: Option<i64>) -> LogRecord {
        LogRecord {
            stream_id: sid(0),
            stream_attrs: stream_attrs_bytes(
                &[("service.name".into(), AttrValue::Str("svc".into()))],
                "scope",
                "1",
                &[],
            ),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "keep".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: k
                .map(|v| vec![("k".to_string(), AttrValue::I64(v))])
                .unwrap_or_default(),
        }
    }

    /// Deliverable 2 (#875): for a single-matching-column declared scan the
    /// presence test and the value read fuse to one cursor read, so the block's
    /// declared column is resolved exactly once. The pre-change path ran
    /// `attr_present` and then `read_typed_cell` -- two full cell reads, each
    /// resolving the column by id -- for `2 * rows` resolutions per block.
    /// Flipping the fused read back to that double read makes the count below
    /// `2 * rows` instead of `1`.
    #[test]
    fn single_matching_declared_scan_reads_each_cell_once() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        // Every row sets `k`, so the record always wins and no resource/scope
        // fallback (which would resolve `stream_ref` per row) is consulted: the
        // count isolates the declared column's own resolution.
        for (ts, v) in [(100i64, 10i64), (101, 20), (102, 30), (103, 40)] {
            w.push(rec_k(ts, Some(v))).expect("push");
        }
        let obj = w.finish().expect("finish");
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 4);

        let dc = DeclaredColumn::new("k", DeclaredType::I64);
        let base = view.column_lookups();
        let plan = DeclaredPlan::build(&view, &dc);
        assert!(
            plan.single_matching(),
            "the key has exactly one FIELD_DIR column, of the declared type"
        );
        let mut cache = HashMap::new();
        let arr = build_declared_columnar_array(&view, &plan, 0, rows, &mut cache)
            .expect("declared array");
        let lookups = view.column_lookups() - base;
        assert_eq!(
            lookups, 1,
            "the declared column is resolved once per block, not 2*rows (the pre-change double read)"
        );

        let ints = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("declared I64 array");
        assert_eq!(ints.len(), 4);
        for (i, want) in [10i64, 20, 30, 40].into_iter().enumerate() {
            assert!(ints.is_valid(i), "row {i} is non-null");
            assert_eq!(ints.value(i), want, "row {i}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod projection_width_tests {
    //! [`ResolvedColumns::width`] and the fraction it feeds the whole-segment
    //! fast path's routing decision (issue #862).
    //!
    //! The width counts DISTINCT OBJECT COLUMNS, which is not the length of the
    //! projection: `ts` and `stream_ref` are always decoded and never named, a
    //! declared column and an erasure matcher on the same key are one column,
    //! and the merged `attrs` map is every dynamic column at once.

    use super::*;

    /// Ten declared columns, so the object's column population is
    /// `FIXED_OBJECT_COLUMNS + 10 = 20`, the denominator every case below uses.
    fn declared() -> Vec<DeclaredColumn> {
        (0..10)
            .map(|k| DeclaredColumn::new(format!("d{k:02}"), DeclaredType::Str))
            .collect()
    }

    fn resolve(projection: &[usize]) -> ResolvedColumns {
        resolve_columns(projection, &[], &[], &declared())
    }

    /// `ts` plus one declared column: three object columns, the q07 shape.
    #[test]
    fn a_single_declared_column_is_three_object_columns() {
        let r = resolve(&[LOG_COL_TS, FIRST_DECLARED_COL]);
        assert_eq!(r.width, Some(3));
        assert_eq!(r.fraction_of(10), 3.0 / 20.0);
        assert!(!r.selection.is_all());
        assert!(!r.selection.wants_all_attrs());
    }

    /// Naming `ts` alone adds nothing: it is decoded either way.
    #[test]
    fn ts_alone_is_the_two_implicit_columns() {
        let r = resolve(&[LOG_COL_TS]);
        assert_eq!(r.width, Some(IMPLICIT_OBJECT_COLUMNS));
        assert_eq!(r.fraction_of(10), 2.0 / 20.0);
    }

    /// The merged `attrs` map means every dynamic column plus the overflow, so
    /// the width is unknown-and-wide and the fraction saturates. This is the
    /// `SELECT *` case, which must keep the whole-object read.
    #[test]
    fn the_attrs_map_widens_to_every_column() {
        let r = resolve(&[LOG_COL_TS, LOG_COL_BODY, LOG_COL_ATTRS]);
        assert_eq!(r.width, None);
        assert_eq!(r.fraction_of(10), 1.0);
        assert!(r.selection.wants_all_attrs());
    }

    /// Every fixed column and every declared column, but not `attrs`: nineteen
    /// of twenty, which is wide by arithmetic rather than by the `attrs`
    /// shortcut.
    #[test]
    fn every_column_but_attrs_is_nineteen_of_twenty() {
        let mut projection: Vec<usize> = (0..LOG_COL_ATTRS).collect();
        projection.extend((0..10).map(|k| FIRST_DECLARED_COL + k));
        let r = resolve(&projection);
        assert_eq!(r.width, Some(19));
        assert_eq!(r.fraction_of(10), 19.0 / 20.0);
    }

    /// A repeated key counts once, whichever contributor names it: the declared
    /// projection and an erasure matcher on the same attribute resolve to the
    /// same object column.
    #[test]
    fn a_key_named_twice_counts_once() {
        let erasure = vec![ErasurePredicate::windowless(vec![(
            "d00".to_string(),
            "v".to_string(),
        )])];
        let r = resolve_columns(
            &[LOG_COL_TS, FIRST_DECLARED_COL],
            &[],
            &erasure,
            &declared(),
        );
        assert_eq!(r.width, Some(3));
    }

    /// A content predicate contributes its column to both the selection and the
    /// width, so a residual filter's column is never counted as free.
    #[test]
    fn a_content_predicate_widens_the_count() {
        let content = vec![Predicate::HasWord {
            field: FieldSel::Body,
            word: "x".to_string(),
        }];
        let r = resolve_columns(&[LOG_COL_TS], &content, &[], &declared());
        assert_eq!(r.width, Some(3), "ts, stream_ref, body");
    }

    /// A tenant with no declared columns has a ten-column object population, so
    /// the same two-column selection is a larger share of it. The denominator is
    /// the tenant's, not a constant.
    #[test]
    fn the_denominator_follows_the_declared_set() {
        let r = resolve_columns(&[LOG_COL_TS], &[], &[], &[]);
        assert_eq!(r.width, Some(2));
        assert_eq!(r.fraction_of(0), 2.0 / 10.0);
    }
}
