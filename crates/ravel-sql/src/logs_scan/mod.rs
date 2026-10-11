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
//! A segment at or under [`LogSegmentFetcher::plan_whole_object_bound`] (the
//! larger of the block-range threshold and the projection break-even) is not
//! planned when the scan holds at least `target_partitions` such segments
//! relevant to the query window (ADR-2677 decision 2,
//! [`LogSegmentFetcher::plans_segment`], [`skips_sub_bound_plans`]): the plan
//! pass issues no read for it, [`owned_work`] gives it whole to one partition,
//! and that partition opens it with one whole-object read and prunes blocks
//! from the directory that read already holds. With fewer such segments, every
//! segment takes the plan-then-stripe route described here.
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
//!   read per relevant segment above the plan bound (`compute_plan_counts`)
//!   plus one scan read per relevant segment, and the partition count is capped at the segment count
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
//! # Whole-object fallback carry, and its memory bound (issue #835)
//!
//! `plan_segment`'s whole-object fallback (an undecidable block predicate, e.g.
//! a text `has_word` arm the skip index cannot resolve) already reads the
//! entire object to count survivors. Before #835 that read was thrown away:
//! the scan phase re-opened the same object and, unless a read cache happened
//! to be wired AND large enough to still hold every relevant object's bytes by
//! the time the scan reached it, paid a second real wire GET for content the
//! plan phase had just fetched. This is a different failure from issue #691's
//! "an evicted plan entry is simply re-fetched by the scan" a few paragraphs
//! up: #691 is about a cache-keyed footer/probe/section entry on the ABOVE-
//! threshold ranged path, which by design may be re-fetched on a miss; #835 is
//! about the whole object itself never being cache-size-dependent in the first
//! place. Only a segment above the plan bound is planned, so the carry below
//! applies only there; a segment at or under it is read once by its owner
//! without any plan read to carry. Since #835, [`LogSegmentFetcher::plan_segment`]'s fallback branch carries its fetched
//! bytes forward as a [`ravel_query::CarriedWholeObject`] (`SegPlan::whole_object`,
//! threaded through [`OwnedSeg::whole_object`] to the subset open), so the
//! object is read once for the statement, rather than twice, for the first
//! `plan_concurrency` segments whose plan completes: their subset open issues
//! neither a store GET nor a cache lookup. Every other relevant segment is re-fetched by the
//! scan exactly as it was before #835; the peak bytes this carry retains is
//! bounded by the plan fan-out (`plan_concurrency` objects) times object
//! size, not by corpus size. Removing the remaining duplicate reads needs
//! the carry to stream per partition instead of being held at the plan
//! barrier, tracked separately as issue #1272. A reused read is charged to
//! [`ravel_types::accounting::QueryAccounting::add_bytes_reused`], not folded
//! into the GET-bytes figure the issuing (plan) phase already recorded.
//!
//! This carry's memory cost is bounded by `plan_concurrency`, not by corpus
//! size: [`compute_plan_counts`] consumes the segment-plan stream as each
//! `plan_segment` call completes (`buffer_unordered(plan_concurrency)`), not
//! after the whole pass finishes, so it never retains more carried whole
//! objects than are actually in flight at once. The first `plan_concurrency`
//! segments to COMPLETE with a carried whole object -- arrival order, not
//! segment order, so which segments those are is scheduling-dependent -- keep
//! their [`SegPlan::whole_object`]; every later arrival has its
//! `whole_object` forced to `None` before it is stored, so that segment's
//! subset open pays a second read during the scan, a wire GET whenever the
//! cache does not still hold the object, exactly as it would have
//! before #835. `compute_plan_counts` sums only the bytes actually retained
//! and records that figure via
//! [`ravel_types::accounting::QueryAccounting::observe_intermediate_bytes`]; a
//! dropped whole object is never charged to
//! [`ravel_types::accounting::QueryAccounting::add_bytes_reused`], since the
//! scan performs a real GET for it and that GET's own accounting covers it.
//!
//! The barrier itself is unchanged by this bound: no partition drains a block
//! until every segment's survivor count is known, because the flattened
//! block-striping assignment (ADR-0102) needs every segment's count to
//! compute unit `i`'s owning partition, not just the segments before it in
//! isolation. Only the CARRIED-BYTES retention is now concurrency-bounded;
//! survivor counts, stats, and footers for every relevant segment are still
//! held until the pass completes, because `owned_work` indexes all of them by
//! segment position regardless. Letting a partition start draining before the
//! whole pass completes would need ADR-0102's partitioning protocol itself to
//! change, and is not made here.
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
//! built from it. The two are charged separately: the batches when they are
//! held ([`LogScanStream::hold_batches`]) and released as they are emitted, the
//! decoded block in `LogScanStream::resident` until the decode that replaces it
//! has run, including any wait for a read-gate permit before that decode.
//! Charging the batches alone would admit a query at a fraction of its
//! resident footprint.
//!
//! # Column projection
//!
//! The scan's output schema *is* the projection DataFusion asked for; there is
//! no `ProjectionExec` above it dropping columns the scan already paid to
//! produce. The projected columns, plus every field a pushed content predicate
//! names, plus every attribute key a pending erasure predicate names, are
//! resolved into a [`ColumnSelection`] that the reader uses to decode only
//! those columns' pages ([`resolve_columns`]). A reference to the whole SQL
//! `attrs` map column (a bare `attrs` projection, `SELECT *`) resolves to every
//! dynamic column plus `attrs_raw`, because the map's contract is that every key
//! is present. A projection that reaches the map only through literal-key
//! `attrs['k']` subscripts is rewritten by
//! [`crate::attrs_per_key::AttrsPerKeyProjection`] into synthetic per-key
//! columns (issue #1768), which resolve to just those keys' FIELD_DIR columns
//! plus `attrs_raw` like a declared column does (ADR-0087, amended
//! 2026-09-14).
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
//!
//! # Block decodes leave the runtime worker
//!
//! A block decode at or above the read gate's inline floor runs on the read
//! CPU gate rather than on a tokio worker (ADR-1702 decision 1): with a gate on
//! the fetcher, each block is one `log_block` job, the columnar decode and its
//! Arrow build together, the row path's through
//! [`LogSegmentScan::next_block_on_gate`], and the stream returns `Pending`
//! from [`LogScanState::DecodingColumnar`] or [`LogScanState::DecodingRows`]
//! while it runs. With no gate the decode runs inline.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, FixedSizeBinaryBuilder, Int32Array,
    Int64Builder, MapBuilder, NullBufferBuilder, StringArray, StringBuilder,
    StringDictionaryBuilder, TimestampNanosecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::ColumnStatistics;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::StatisticsArgs;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use ravel_catalog::{
    DeclaredColumnStats, LoadedColumnStats, SegmentRef, unique_column_stat,
    validate_min_max_presence,
};
use ravel_commit::declared_stats::{StatCarrier, observe_declared_stat_drops};
use ravel_cpu_gate::{CpuGateError, JobSize, ReadGate, ReadSite};
use ravel_logseg::footer::LogFooter;
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{
    AttrColumn, BoolCursor, BytesCursor, ColumnSelection, ColumnarBlockView, DictValues,
    F64BitsCursor, FieldSel, FieldType, I64Cursor, LogRecord, LogSegError, Predicate, ScanStats,
    SegmentDirectories, StrDictColumn,
};
use ravel_object_store::StoreError;
use ravel_proto::catalog::v1::column_value::Kind as ColumnValueKind;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_query::erasure::ErasurePredicate;
use ravel_query::{
    CarriedWholeObject, ColumnarBlockOutcome, LogFetchError, LogQuery, LogSegmentFetcher,
    LogSegmentScan, PhaseAccounting,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::declared_stats::{DeclaredStatType, DeclaredStatValue};
use ravel_types::logstream::AttrValue;
use tokio::sync::OnceCell;

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::error::SqlError;
use crate::late_materialization::{RowRef, row_ref_field};
use crate::logs_schema::{
    FIRST_DECLARED_COL, LOG_COL_ATTRS, LOG_COL_BODY, LOG_COL_FLAGS, LOG_COL_OBSERVED_TS,
    LOG_COL_SEVERITY_NUM, LOG_COL_SEVERITY_TEXT, LOG_COL_SPAN_ID, LOG_COL_TRACE_ID, LOG_COL_TS,
    SPAN_ID_WIDTH, TRACE_ID_WIDTH, attr_key_field_name,
};
use crate::rlog_attrs::{
    attr_value_to_string, decode_stream_attrs, find_attr, merged_attrs, retain_unerased,
};
use ravel_logseg::record::canonical_value_bytes;

mod batch;
mod plan;
mod scan;
mod threshold;

use batch::*;
use plan::*;
use scan::*;
use threshold::*;

pub(crate) use threshold::ConfirmTopKThreshold;

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
    attr_keys: &[String],
    full_len: usize,
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
            // A synthetic per-key attribute column (index >= full_len): decode
            // exactly that key's FIELD_DIR column(s) plus `attrs_raw`, the same
            // per-key selection a declared column resolves to (issue #1768).
            // This is what keeps the whole `attrs` map off the selection so the
            // scan decodes one key's pages instead of every dynamic column.
            other if other >= full_len => match attr_keys.get(other - full_len) {
                Some(key) => {
                    attrs.insert(key.clone());
                }
                None => all = true,
            },
            // A declared typed attribute column (FIRST_DECLARED_COL..full_len):
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

/// One segment's coverage of one declared column as one carrier states it.
///
/// `min`/`max` both absent is a coverage statement, not a gap: the column had
/// zero non-null values in that segment, which is exact.
///
/// Only a stamp's coverage ([`stamp_coverage`]) answers a statistic or skips a
/// segment, and its `null_count` passed the row-count clauses before the
/// container [`stamp_coverage`] takes could be built. A `.cstat` entry's
/// coverage ([`cstat_coverage`]) never answers: it is compared with a stamp's,
/// or read only for its defect tally.
#[derive(Clone)]
pub(crate) struct SegmentCoverage {
    pub(crate) min: Option<ScalarValue>,
    pub(crate) max: Option<ScalarValue>,
    pub(crate) null_count: u64,
}

impl SegmentCoverage {
    /// Whether two carriers state the same triple for one segment and column.
    /// Exact equality on all three fields, with no widening or narrowing.
    pub(crate) fn agrees_with(&self, other: &SegmentCoverage) -> bool {
        self.min == other.min && self.max == other.max && self.null_count == other.null_count
    }
}

/// The exact statistics the stamps prove for one declared column over every
/// segment a scan touches, from [`LogsScanExec::declared_min_max_all`].
///
/// `null_count` is `None` when the per-segment NULL counts overflow `u64`
/// when summed; the extrema are still exact in that case.
#[derive(Clone)]
pub(crate) struct DeclaredExactStats {
    pub(crate) min: ScalarValue,
    pub(crate) max: ScalarValue,
    pub(crate) null_count: Option<u64>,
}

/// Keep whichever of `current` and `candidate` is the extreme in the `want`
/// direction, under `ScalarValue`'s own ordering (the comparator DataFusion's
/// `MIN`/`MAX` accumulators apply, which is what makes the shortcut answer the
/// same question the scan would). A tie keeps the incumbent.
fn keep_extreme(
    current: Option<ScalarValue>,
    candidate: ScalarValue,
    want: std::cmp::Ordering,
) -> Option<ScalarValue> {
    match current {
        Some(cur) if candidate.partial_cmp(&cur) != Some(want) => Some(cur),
        _ => Some(candidate),
    }
}

/// The stamp type a declared column of `ty` may carry, or `None` when the
/// ADR-0873 decision 2 allowlist excludes the type.
///
/// The float gate is here, and it is a gate rather than an accident of the
/// current vocabulary: `DeclaredType` has no float variant today (ADR-0090
/// declares four types), and when ADR-0101's declarable `f64` lands, this
/// exhaustive match stops compiling until someone decides its comparator, its
/// NaN rule, and its `-0.0` rule -- rather than the new type quietly reaching
/// a `Precision::Exact` extremum through a wildcard arm. An excluded type has
/// no stamp, which is not an error: its statistics questions are answered by
/// the scan.
fn stamp_type_of(ty: DeclaredType) -> Option<DeclaredStatType> {
    match ty {
        DeclaredType::I64 => Some(DeclaredStatType::I64),
        DeclaredType::Bool => Some(DeclaredStatType::Bool),
        // Unbounded byte strings: a stamped extremum would put arbitrary user
        // data on the commit record and the hot resolve path, and truncating
        // it would make the stat a bound rather than an extremum.
        DeclaredType::Str | DeclaredType::Bytes => None,
    }
}

/// One stamped extremum as the Arrow scalar `ty` projects to, or `None` when
/// the stamp's value kind disagrees with `ty`.
fn stamp_scalar(ty: DeclaredType, value: DeclaredStatValue) -> Option<ScalarValue> {
    match (ty, value) {
        (DeclaredType::I64, DeclaredStatValue::I64(v)) => Some(ScalarValue::Int64(Some(v))),
        (DeclaredType::Bool, DeclaredStatValue::Bool(b)) => Some(ScalarValue::Boolean(Some(b))),
        _ => None,
    }
}

/// What the `SegmentRef` stamps say about `declared` for one segment, or
/// `None` when they say nothing usable: an excluded type, no entry for the
/// column, or an entry this query cannot read (its `declared_type` or a value
/// kind disagreeing with the type the tenant declares the column as).
///
/// The parameter is [`DeclaredColumnStats`], the validated container, not a
/// slice of entries, and the signature is the argument: callers trust the NULL
/// count returned here with no further check (summed into `COUNT(col)`, and
/// compared with `sample_count` to skip an all-NULL segment), so this
/// function's correctness rests on the entries having passed the full
/// statistics validity predicate, row-count clauses included.
/// `DeclaredColumnStats`'s only non-empty constructor is `from_validated`, so
/// possessing one is that proof (ADR-0873 decision 2), and a caller cannot
/// satisfy this signature with entries it decoded itself. A
/// `&[DeclaredColumnStat]` parameter would leave the same claim resting on a
/// caller invariant nothing checks, which is the #970 shape.
///
/// Because the container is the validated form, at most one entry names the
/// column (ADR-0873 clause 6 drops every occurrence of a duplicated name, so a
/// duplicate arrives here as no entry at all) and the lookup is a lookup rather
/// than a pick between claims.
pub(crate) fn stamp_coverage(
    declared: &DeclaredColumn,
    stamps: &DeclaredColumnStats,
) -> Option<SegmentCoverage> {
    let stat_type = stamp_type_of(declared.ty)?;
    let stat = stamps.column(&declared.key)?;
    if stat.declared_type() != stat_type {
        return None;
    }
    // A one-sided pair cannot exist in the validated form (clause 2), so
    // mapping each side independently cannot produce one here either.
    let min = match stat.min() {
        Some(v) => Some(stamp_scalar(declared.ty, v)?),
        None => None,
    };
    let max = match stat.max() {
        Some(v) => Some(stamp_scalar(declared.ty, v)?),
        None => None,
    };
    Some(SegmentCoverage {
        min,
        max,
        null_count: stat.null_count(),
    })
}

/// The one `.cstat` entry naming `key` in `seg_stats`, or `None` when no figure
/// that entry carries may be granted `Precision::Exact`: no entry for the
/// column, more than one entry under the name (`unique_column_stat` refuses to
/// pick between two claims about one immutable object), or row accounting
/// (`non_null_count + null_count`) that does not reconcile against the joined
/// [`SegmentRef::sample_count`].
///
/// This is ADR-0873 decision 2 clause 4's `.cstat` arm, and it binds here rather
/// than in `unique_column_stat` because the reconciliation needs the joined
/// segment, which a lookup over one segment's entry list does not have. Every
/// `.cstat` read goes through this wrapper -- [`cstat_coverage`] and
/// [`merged_view_entry`], the second being the only route by which
/// [`LogsScanExec::declared_not_equal_count`],
/// [`LogsScanExec::declared_group_counts`] and
/// [`LogsScanExec::declared_column_sum`] reach an entry -- so a new path cannot
/// reach an entry without the gate (#1037).
///
/// The refusal covers the WHOLE entry -- extrema, dictionary and sum alike --
/// because a stale or miscounted entry describes rows this immutable object does
/// not have, or omits rows it does, so no figure it carries describes the object
/// the query reads. The add is checked, so an overflowing sum is itself a
/// disagreement rather than a wrap.
///
/// The two DEFECT refusals, a duplicated column name and a row-accounting
/// disagreement, each report one drop observation under [`StatCarrier::Cstat`]
/// per read; the metric has observation semantics, so a defective entry read by
/// two paths of one statement is counted by each. An absent entry is not a
/// defect and is not counted: it is the ordinary uncovered state of every tenant
/// whose fold never built that column.
fn reconciled_column_stat<'a>(
    seg_stats: &'a ColumnStatsSegment,
    seg: &SegmentRef,
    key: &str,
) -> Option<&'a ColumnStat> {
    let stat = match unique_column_stat(seg_stats, key) {
        Some(stat) => stat,
        None => {
            // `unique_column_stat` folds "no entry" and "more than one entry"
            // into one `None`, and only the second is a defect: a duplicated
            // name means no rule can pick the right claim about one immutable
            // object. Re-deriving which case it was costs one pass over the
            // entries of one segment, on the refusal path only.
            let duplicated = seg_stats.columns.iter().any(|column| column.name == key);
            if duplicated {
                observe_declared_stat_drops(StatCarrier::Cstat, 1);
            }
            return None;
        }
    };
    let accounted = stat.non_null_count.checked_add(stat.null_count);
    if accounted != Some(seg.sample_count) {
        observe_declared_stat_drops(StatCarrier::Cstat, 1);
        return None;
    }
    Some(stat)
}

/// What the loaded `.cstat` object says about `declared` for one segment, or
/// `None` when that carrier grants nothing: no object, no entry for this
/// segment, no entry for the column, more than one entry under the column's
/// name (`unique_column_stat` refuses to pick), an entry whose extrema
/// presence disagrees with its `non_null_count` in either direction (#970), an
/// entry whose row accounting (`non_null_count + null_count`) does not
/// reconcile against the joined [`SegmentRef::sample_count`] (#1023), or a
/// value of the wrong kind.
///
/// Three of those refusals are DEFECTS rather than absences, and each reports
/// one drop observation under [`StatCarrier::Cstat`] (ADR-0873 decision 2's
/// metric, whose stamp-carrier labels are fed by the carrier reads in
/// `ravel-commit`): a duplicated column name and a row-accounting disagreement
/// with the joined `sample_count`, both in [`reconciled_column_stat`], plus a
/// presence contradiction here. The other refusals are not counted, and the
/// distinction is the point of the metric: an absent object, an absent segment
/// entry, and an absent column entry are the ordinary uncovered state of every
/// tenant whose fold never built that column, while a value of a kind the
/// tenant's current declared type does not match is a legal consequence of
/// re-declaring a column, not a writer bug.
///
/// The entry tallies the record-level cells only, so what it says is not what
/// SQL returns for a row whose value comes from its resource or scope
/// attributes; see [`segment_declared_coverage`].
pub(crate) fn cstat_coverage(
    declared: &DeclaredColumn,
    seg: &SegmentRef,
    seg_stats: Option<&ColumnStatsSegment>,
) -> Option<SegmentCoverage> {
    let stat = reconciled_column_stat(seg_stats?, seg, &declared.key)?;
    entry_coverage(declared, stat)
}

/// [`cstat_coverage`] for an entry [`reconciled_column_stat`] already granted.
fn entry_coverage(declared: &DeclaredColumn, stat: &ColumnStat) -> Option<SegmentCoverage> {
    // The presence clause is called, not restated: `LoadedColumnStats` has
    // public fields and can be populated by a carrier that never decoded an
    // object, so the check has to bind here, where coverage is granted.
    if validate_min_max_presence(stat).is_err() {
        observe_declared_stat_drops(StatCarrier::Cstat, 1);
        return None;
    }
    let min = match stat.min.as_ref() {
        Some(v) => Some(declared_scalar(declared.ty, v)?),
        None => None,
    };
    let max = match stat.max.as_ref() {
        Some(v) => Some(declared_scalar(declared.ty, v)?),
        None => None,
    };
    Some(SegmentCoverage {
        min,
        max,
        null_count: stat.null_count,
    })
}

/// The loaded `.cstat` entry for `seg`, joined by content hash and then by
/// segment identity, or `None` when no statistics are loaded or none name it.
pub(crate) fn segment_column_stats<'a>(
    column_stats: Option<&'a LoadedColumnStats>,
    seg: &SegmentRef,
) -> Option<&'a ColumnStatsSegment> {
    column_stats.and_then(|stats| stats.stat_for(&seg.content_hash, &segment_identity(seg)))
}

/// This segment's exact coverage of one declared column, which is its
/// `SegmentRef` stamp, where `seg_stats` is [`segment_column_stats`] for `seg`.
///
/// SQL returns the merged value for a declared column: the record's own
/// attribute when the record sets the key, otherwise the stream's resource or
/// scope attribute of the same name. The stamp is folded from that merged
/// value. A `.cstat` entry tallies the record-level cells only, so a row whose
/// value comes from its resource or scope reads NULL to the entry, and the
/// entry's extrema and NULL count can differ from the stamp's with neither
/// carrier wrong. The entry therefore never answers and is never compared with
/// the stamp here: a segment the stamp does not cover declines the column even
/// when a `.cstat` entry covers it, and where both exist the stamp's triple is
/// returned as is. The entry is still read, so a defective one is counted on
/// the [`StatCarrier::Cstat`] drop tally, but only on segments this is called
/// for: [`LogsScanExec::declared_min_max_all`] stops calling it for a column at
/// the first touched segment with no stamp for that column, so on a stamp-less
/// tenant only the first touched segment's entry is read on that path.
///
/// `None` declines the column for this segment: a `Str` column (no scalar form
/// on the statistics paths), or a segment with no usable stamp for it.
pub(crate) fn segment_declared_coverage(
    declared: &DeclaredColumn,
    seg: &SegmentRef,
    seg_stats: Option<&ColumnStatsSegment>,
) -> Option<SegmentCoverage> {
    if matches!(declared.ty, DeclaredType::Str) {
        return None;
    }
    let _ = cstat_coverage(declared, seg, seg_stats);
    stamp_coverage(declared, &seg.declared_column_stats)
}

/// The `.cstat` entry for `declared` on `seg` when it may answer for the
/// merged value SQL returns, or `None`.
///
/// The entry describes the record-level cells only (see
/// [`segment_declared_coverage`]), and the dictionary and sum the exact
/// aggregate paths read have no counterpart on the stamp. The entry is used
/// only when the segment's stamp states the same `min`, `max` and NULL count:
/// every row the entry counts as non-null sets the key on the record, which is
/// the value SQL reads for that row, so equal NULL counts mean no row took its
/// value from the resource or scope. A segment with no stamp for the column,
/// or whose stamp differs from the entry, declines.
fn merged_view_entry<'a>(
    declared: &DeclaredColumn,
    seg: &SegmentRef,
    seg_stats: &'a ColumnStatsSegment,
) -> Option<&'a ColumnStat> {
    let stat = reconciled_column_stat(seg_stats, seg, &declared.key)?;
    let stamp = stamp_coverage(declared, &seg.declared_column_stats)?;
    stamp
        .agrees_with(&entry_coverage(declared, stat)?)
        .then_some(stat)
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

/// Exact aggregate inputs for the `SUM(col + k)` and `AVG(col)` decompositions
/// (ADR-0850's q03/q04/q30 shapes, #861), from
/// [`LogsScanExec::declared_column_sum`]: the exact sum of every touched
/// segment's non-null values, in `i128` so the cross-segment fold cannot
/// overflow, plus the exact count of those non-null rows. `SUM(col + k)` is
/// `sum + k * non_null_count`, `AVG(col)` is `sum / non_null_count`, and both
/// are `NULL` when `non_null_count == 0` (SQL sum/avg over all-null or zero-row
/// input), so the caller needs both figures.
pub(crate) struct DeclaredColumnSum {
    pub(crate) sum: i128,
    pub(crate) non_null_count: u64,
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
    /// The statement's prefetch pool (ADR-2414 decision A2): every
    /// partition's issued, unconsumed fast-path opens, one slot per
    /// partition, so a partition whose current open the fetch memory budget
    /// refuses can drop all of them, not only its own.
    prefetch_pool: Arc<PrefetchPool>,
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
    /// Synthetic per-key attribute columns (issue #1768). Each renders one
    /// `attrs['k']` subscript as a `Utf8` column using the merged map's rules
    /// (record-wins over resource/scope, values rendered as text, NULL for an
    /// absent key), so a projection that reaches the `attrs` map only through
    /// literal-key subscripts drops the whole-map projection: the reader decodes
    /// only these keys' FIELD_DIR columns plus `attrs_raw` (like a declared
    /// column) instead of every dynamic column, and the scan stays on the
    /// columnar fast path. Index `j` here is the synthetic schema index
    /// `full_schema.fields().len() + j`, past every declared column. Empty for
    /// every query the [`crate::attrs_per_key::AttrsPerKeyProjection`] rule did
    /// not rewrite, which is byte-identical to the pre-#1768 scan.
    attr_keys: Arc<Vec<String>>,
    /// Exact per-segment column statistics for the tenant's declared columns
    /// (ADR-0850), loaded once per plan and threaded down from
    /// [`crate::executor::SqlExecutor`]. `None` when no usable column-stats
    /// object exists (nothing folded yet, no configured typed columns, or the
    /// last fold's build/PUT failed): every metadata-only path degrades to
    /// scanning in that case. A live segment absent from
    /// `LoadedColumnStats::segments` has no exact statistics either, checked
    /// per column at the point of use rather than here.
    column_stats: Option<Arc<LoadedColumnStats>>,
    /// Whether this scan publishes its per-segment scan timeline
    /// (`SqlConfig::segment_timing`, issue #913). `false` by default,
    /// installed with [`Self::with_segment_timing`]; gates
    /// [`LogScanStream::mark_segment`] to a no-op (no label allocation, no
    /// metric registration) so a production query pays nothing for a
    /// timeline only the bench reporter reads.
    segment_timing: bool,
    /// Segments the provider skipped by declared-column statistics before
    /// building this scan (ADR-2121 D1), published as the
    /// `segments_pruned_by_stats` counter. Installed with
    /// [`Self::with_segments_pruned_by_stats`] and carried across every
    /// rebuild, because the skipped segments are not in [`Self::segments`]
    /// for a rebuild to recount.
    segments_pruned_by_stats: usize,
    /// The row count DataFusion's `LimitPushdown` optimizer rule pushed into
    /// this scan (issue #362), installed with [`Self::with_fetch`]. `None`
    /// (the default) reproduces the pre-#362 scan exactly: every segment this
    /// partition owns is opened.
    ///
    /// Set, it caps each partition's own output at exactly `fetch` rows:
    /// [`LogScanStream`] truncates the batch that reaches it and opens no
    /// further owned segment. The cap must be exact, because `LimitPushdown`
    /// removes the `GlobalLimitExec` above a scan that accepted the fetch, and
    /// a single-partition plan has no `CoalescePartitionsExec` above it either
    /// (issue #2616). With several partitions the `CoalescePartitionsExec`
    /// that absorbed the pushdown applies the cross-partition total. Emitting
    /// fewer than `fetch` while more of a partition's owned data remains would
    /// silently under-answer the query and must never happen.
    fetch: Option<usize>,
    /// A dynamic filter pushed into this scan whose leading child is `ts`
    /// (ADR-2677 decision 6), kept for its `ts` threshold and never applied
    /// to a row: the TopK stays the exact evaluator. It changes nothing until
    /// [`ConfirmTopKThreshold`] matches it to an `ORDER BY ts ASC NULLS LAST
    /// ... LIMIT k` sort; see [`Self::active_topk`] and [`TopKThreshold`].
    topk: Option<TopKThreshold>,
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
    /// This query's phase-split accounting handle (ADR-0044, issue #796),
    /// threaded into every per-partition fetch so log fetches are recorded
    /// like every other funnel, split by phase.
    phase_accounting: PhaseAccounting,
    /// Block-level pruning counters, reported through `EXPLAIN ANALYZE`.
    metrics: ExecutionPlanMetricsSet,
    /// The one monotonic origin every partition's timeline offsets
    /// (`seg_*_offset`, `first_batch_elapsed`, `stream_elapsed`) are measured
    /// from, so events from different partitions can be ordered against each
    /// other. Taken when the plan node is built, before any partition exists.
    created_at: Instant,
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
    /// Integer column value buffers this partition's block decodes built, one
    /// per integer column decoded from its own value page per block
    /// ([`ScanStats::int_values_buffers`], ADR-2773 decision 1).
    int_values_buffers: Count,
    /// Blocks this partition decoded whose rows were all kept without
    /// evaluating the pushed exact predicate, every arm proven by the block's
    /// skip-index bounds ([`ScanStats::blocks_predicate_dropped`], ADR-2773
    /// decision D2).
    blocks_predicate_dropped: Count,
    /// Rows of this partition's decoded blocks the pushed exact predicate was
    /// evaluated over ([`ScanStats::rows_predicate_evaluated`]).
    rows_predicate_evaluated: Count,
    /// String dictionary values arrays this partition's declared `Str` columns
    /// built: one per row group dictionary per scan opening, one per per-block
    /// dictionary page, one per plain-page block, and one per extended copy
    /// built for a set of fallback values, again after the copy was freed
    /// ([`StrValuesCache`], ADR-2773 decision D3).
    str_dict_values_built: Count,
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
    /// POSTINGS prune; a stream filter), or an object at or below the larger
    /// of the block-range threshold and the projection break-even, which the
    /// fetch reads whole in one GET regardless.
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
    /// Wall time this partition spent with a segment open in flight: from the
    /// `NextSegment` arm constructing the open future to the `Opening` arm
    /// seeing it ready. It is the exposed open stall, not network time: an
    /// open can mix cache hits, carried bytes, and real GETs, and the
    /// partition does no decode while it waits.
    open_elapsed: Time,
    /// `Opening` polls that returned `Pending`, the count of times this
    /// partition yielded to the runtime with an open in flight.
    open_pending_polls: Count,
    /// Segments this partition opened (first opens only; the `attrs_raw`
    /// reopen counts under [`Self::reopens`]).
    segments_opened: Count,
    /// Wall time in `ReopenRows` opens (the `attrs_raw` fallback), kept apart
    /// from [`Self::open_elapsed`] so a fallback's second open is visible.
    reopen_elapsed: Time,
    reopens: Count,
    /// Current fast-path opens (a first open, a consumed prefetch, or the
    /// `attrs_raw` reopen) the fetch memory budget refused and this partition
    /// retried once, after dropping every unconsumed prefetch of the statement
    /// and turning its pipeline off (ADR-2414 decision A2,
    /// [`LogScanStream::on_open_refused`]).
    prefetch_memory_reopens: Count,
    /// Unconsumed prefetches, of any partition of the statement, that this
    /// partition's refused opens dropped ([`PrefetchPool::revoke_all`]). Each
    /// dropped prefetch's segment goes back to its owner's work, to be opened
    /// again at its turn.
    prefetch_revocations: Count,
    /// Wall time in the synchronous decode and Arrow build sites inside
    /// `poll_next`: `next_block_columnar` plus `build_columnar_batches` on the
    /// columnar path, `next_block` on the row path. A decode on the read gate
    /// is timed from the job's submission to its return, so this includes the
    /// wait for a permit. Nothing nested is timed twice, and the
    /// buffered-output drain is under [`Self::emit_elapsed`].
    decode_build_elapsed: Time,
    /// Wall time handing buffered output downstream: `emit_next_row_batch`
    /// (which builds the row-path batch) and `emit_next_columnar_batch`.
    emit_elapsed: Time,
    /// This partition's wait on the shared plan barrier, from the stream's
    /// creation to the `Planning` arm seeing the counts. Every partition waits
    /// on the one cell, so the SUM over partitions overstates the query-level
    /// delay; the barrier's own cost is [`Self::plan_init_elapsed`].
    planning_wait_elapsed: Time,
    /// Wall time of the one `compute_plan_counts` run, recorded by whichever
    /// partition's poll initialized the shared cell. Non-zero on exactly one
    /// partition per query, so its sum over partitions is the query's planning
    /// cost counted once.
    plan_init_elapsed: Time,
    /// Offset from the exec's creation to this partition's first emitted
    /// batch, and to the stream reporting `Done`. Both on the exec's clock.
    first_batch_elapsed: Time,
    stream_elapsed: Time,
    /// `poll_next` calls, and how many of them returned `Pending`.
    polls: Count,
    polls_pending: Count,
    /// Blocks of opened segments this partition skipped undecoded because
    /// their minimum ts was above the TopK threshold (ADR-2677 decision 6).
    blocks_skipped_by_threshold: Count,
    /// Owned segments (on the striped path, owned shares of a segment) this
    /// partition did not open for their blocks because their minimum ts was
    /// above the TopK threshold. Their blocks are not in
    /// [`Self::blocks_skipped_by_threshold`]; on the planned path the plan
    /// phase has already read their footers and counted their blocks in
    /// `blocks_total`. A segment at or under the plan bound that is skipped
    /// here is never read at all, and its blocks are not in `blocks_total`.
    segments_skipped_by_threshold: Count,
    /// Segments this partition opened without a plan read
    /// ([`PlanCounts::unplanned`], ADR-2677 decision 2): one whole-object GET
    /// each, pruned in the scan.
    unplanned_segments: Count,
    /// The fast path's ranged-open prefetch depth this partition ran with
    /// (`1` is no prefetch), added once at execute, so the plan-wide sum is
    /// the partitions' depths summed.
    prefetch_share: Count,
}

impl BlockMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        BlockMetrics {
            open_elapsed: MetricBuilder::new(metrics).subset_time("open_elapsed", partition),
            open_pending_polls: MetricBuilder::new(metrics)
                .counter("open_pending_polls", partition),
            segments_opened: MetricBuilder::new(metrics).counter("segments_opened", partition),
            reopen_elapsed: MetricBuilder::new(metrics).subset_time("reopen_elapsed", partition),
            reopens: MetricBuilder::new(metrics).counter("reopens", partition),
            prefetch_memory_reopens: MetricBuilder::new(metrics)
                .counter("prefetch_memory_reopens", partition),
            prefetch_revocations: MetricBuilder::new(metrics)
                .counter("prefetch_revocations", partition),
            decode_build_elapsed: MetricBuilder::new(metrics)
                .subset_time("decode_build_elapsed", partition),
            emit_elapsed: MetricBuilder::new(metrics).subset_time("emit_elapsed", partition),
            planning_wait_elapsed: MetricBuilder::new(metrics)
                .subset_time("planning_wait_elapsed", partition),
            plan_init_elapsed: MetricBuilder::new(metrics)
                .subset_time("plan_init_elapsed", partition),
            first_batch_elapsed: MetricBuilder::new(metrics)
                .subset_time("first_batch_elapsed", partition),
            stream_elapsed: MetricBuilder::new(metrics).subset_time("stream_elapsed", partition),
            polls: MetricBuilder::new(metrics).counter("polls", partition),
            polls_pending: MetricBuilder::new(metrics).counter("polls_pending", partition),
            blocks_skipped_by_threshold: MetricBuilder::new(metrics)
                .counter("blocks_skipped_by_threshold", partition),
            segments_skipped_by_threshold: MetricBuilder::new(metrics)
                .counter("segments_skipped_by_threshold", partition),
            unplanned_segments: MetricBuilder::new(metrics)
                .counter("segments_unplanned", partition),
            total: MetricBuilder::new(metrics).counter("blocks_total", partition),
            scanned: MetricBuilder::new(metrics).counter("blocks_scanned", partition),
            pruned_by_postings: MetricBuilder::new(metrics)
                .counter("blocks_pruned_by_postings", partition),
            pages_decoded: MetricBuilder::new(metrics).counter("pages_decoded", partition),
            pages_skipped: MetricBuilder::new(metrics).counter("pages_skipped", partition),
            int_values_buffers: MetricBuilder::new(metrics)
                .counter("int_values_buffers", partition),
            blocks_predicate_dropped: MetricBuilder::new(metrics)
                .counter("blocks_predicate_dropped", partition),
            rows_predicate_evaluated: MetricBuilder::new(metrics)
                .counter("rows_predicate_evaluated", partition),
            str_dict_values_built: MetricBuilder::new(metrics)
                .counter("str_dict_values_built", partition),
            columnar_batches: MetricBuilder::new(metrics).counter("columnar_batches", partition),
            rowpath_batches: MetricBuilder::new(metrics).counter("rowpath_batches", partition),
            plan_full_reads: MetricBuilder::new(metrics).counter("plan_full_reads", partition),
            fast_path_whole_object_segments: MetricBuilder::new(metrics)
                .counter("fast_path_whole_object_segments", partition),
            fast_path_ranged_segments: MetricBuilder::new(metrics)
                .counter("fast_path_ranged_segments", partition),
            prefetch_share: MetricBuilder::new(metrics).counter("prefetch_share", partition),
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
    /// exactly once per segment. A pushed `fetch` that stops the owner
    /// mid-segment records them at that stop instead; they are final from the
    /// open, so the figure is the whole segment's. A fast-path segment the
    /// owner never opened, because `fetch` was already met, records none.
    /// Either way the per-partition decode counts come from
    /// [`Self::record_scan`].
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
    /// `blocks_scanned`/`pages_*`. A partition that a pushed `fetch` stops
    /// mid-segment records what its scan decoded up to the stop, so the sum is
    /// then the decode work actually done, not the whole segment's.
    fn record_scan(&self, stats: &ScanStats) {
        self.scanned.add(stats.blocks_scanned as usize);
        self.pages_decoded.add(stats.pages_decoded as usize);
        self.pages_skipped.add(stats.pages_skipped as usize);
        self.int_values_buffers
            .add(stats.int_values_buffers as usize);
        self.blocks_predicate_dropped
            .add(stats.blocks_predicate_dropped as usize);
        self.rows_predicate_evaluated
            .add(stats.rows_predicate_evaluated as usize);
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
        phase_accounting: PhaseAccounting,
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
            phase_accounting,
            full_schema,
            declared,
            Vec::new(),
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
            self.phase_accounting.clone(),
            Arc::clone(&self.full_schema),
            Arc::clone(&self.declared),
            self.attr_keys.as_ref().clone(),
            row_refs,
        )
        .map(|scan| {
            scan.with_column_stats(self.column_stats.clone())
                .with_segment_timing(self.segment_timing)
                .with_segments_pruned_by_stats(self.segments_pruned_by_stats)
                .with_fetch_pushed(self.fetch)
                .with_topk(self.topk.clone())
        })
    }

    /// A sibling scan over the same table that materializes `attr_keys` as
    /// synthetic per-key `Utf8` columns instead of the whole `attrs` map (issue
    /// #1768). `projection` is over this scan's EXTENDED schema: the resolved
    /// full schema (`logs_schema_with_declared`) followed by one `Utf8` field
    /// per entry of `attr_keys`, in `attr_keys` order, so a projection index
    /// `full_schema.fields().len() + j` selects `attr_keys[j]`. The whole-map
    /// projection index [`LOG_COL_ATTRS`] must not appear in `projection`; the
    /// [`crate::attrs_per_key::AttrsPerKeyProjection`] rule that calls this
    /// replaces it with the per-key indices and rewrites the `get_field` above
    /// the scan to match. Everything that decides which bytes are read is
    /// carried over unchanged, so the surviving-block and surviving-row sets are
    /// identical to the wide scan this replaces.
    pub(crate) fn reproject_attr_keys(
        &self,
        projection: Vec<usize>,
        attr_keys: Vec<String>,
    ) -> DFResult<Self> {
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
            self.phase_accounting.clone(),
            Arc::clone(&self.full_schema),
            Arc::clone(&self.declared),
            attr_keys,
            false,
        )
        .map(|scan| {
            scan.with_column_stats(self.column_stats.clone())
                .with_segment_timing(self.segment_timing)
                .with_segments_pruned_by_stats(self.segments_pruned_by_stats)
                .with_fetch_pushed(self.fetch)
                .with_topk(self.topk.clone())
        })
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

    /// Turn on this scan's per-segment scan timeline (`SqlConfig::
    /// segment_timing`, issue #913). A builder method for the same reason
    /// [`Self::with_column_stats`] is one: every existing call site of
    /// [`Self::new`] stays source-compatible, and `false` (the default)
    /// reproduces the pre-gate scan exactly.
    pub(crate) fn with_segment_timing(mut self, segment_timing: bool) -> Self {
        self.segment_timing = segment_timing;
        self
    }

    /// Record how many segments the provider skipped by declared-column
    /// statistics before building this scan (ADR-2121 D1), and publish the
    /// figure as the `segments_pruned_by_stats` counter on this scan's metric
    /// set, where `EXPLAIN ANALYZE` and the executor's `SqlStats` read it.
    /// Registered at plan time, so a scan that skipped nothing reports `0`
    /// rather than omitting the counter.
    pub(crate) fn with_segments_pruned_by_stats(mut self, pruned: usize) -> Self {
        self.segments_pruned_by_stats = pruned;
        MetricBuilder::new(&self.metrics)
            .global_counter("segments_pruned_by_stats")
            .add(pruned);
        self
    }

    /// Carry a pushed `fetch` (issue #362) across a rebuild that otherwise
    /// starts from `fetch: None` ([`Self::build`]'s default), the same way
    /// [`Self::with_column_stats`] and [`Self::with_segment_timing`] carry
    /// their fields across [`Self::reproject`] and [`Self::reproject_attr_keys`].
    /// Not `pub(crate)`: the only external installer of a NEW fetch value is
    /// the `ExecutionPlan::with_fetch` trait method, which builds the whole
    /// struct itself rather than starting from [`Self::build`].
    fn with_fetch_pushed(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Install (or carry across a rebuild) the TopK threshold this scan
    /// skips by. The threshold names the `ts` column by name, not by index,
    /// so it stays valid across [`Self::reproject`] and
    /// [`Self::reproject_attr_keys`], which change the output column indices.
    fn with_topk(mut self, topk: Option<TopKThreshold>) -> Self {
        self.topk = topk;
        self
    }

    /// This scan keeping `filter`, the dynamic filter of a `SortExec` TopK
    /// over it, as an unconfirmed threshold when that filter's leading key is
    /// the bare `ts` column, and keeping nothing otherwise: any threshold
    /// carried from an earlier TopK is dropped. Late materialization builds
    /// its phase-1 TopK after DataFusion's filter pushdown has run, so it
    /// hands that TopK's filter over through this instead.
    pub(crate) fn with_topk_filter(self, filter: &Arc<DynamicFilterPhysicalExpr>) -> Self {
        self.with_topk(TopKThreshold::from_filter(filter))
    }

    /// The threshold this scan acts on: its kept filter once
    /// [`ConfirmTopKThreshold`] has confirmed it, and none otherwise. Every
    /// execution-time use of [`Self::topk`] goes through this.
    fn active_topk(&self) -> Option<&TopKThreshold> {
        self.topk.as_ref().filter(|topk| topk.confirmed)
    }

    /// This scan with its kept filter confirmed when `ascending` holds the
    /// filter's id, or dropped when it does not; `None` when that changes
    /// nothing (no kept filter, or already confirmed).
    fn confirm_topk(&self, ascending: &HashSet<u64>) -> Option<Self> {
        let topk = self.topk.as_ref()?;
        let matched = topk
            .expression_id()
            .is_some_and(|id| ascending.contains(&id));
        if matched && topk.confirmed {
            return None;
        }
        let topk = matched.then(|| TopKThreshold {
            filter: Arc::clone(&topk.filter),
            confirmed: true,
        });
        Some(self.sibling(self.fetch, topk))
    }

    /// A sibling of this scan with `fetch` and `topk` installed.
    ///
    /// An exhaustive struct literal rather than a call through [`Self::build`]:
    /// every field of [`LogsScanExec`] is named here explicitly, so a field
    /// added to the struct later without a matching line here is a compile
    /// error, not a silently-dropped one. `counts`, `prefetch_pool` and
    /// `metrics` are shared with `self` rather than rebuilt: the optimizer
    /// replaces `self` with the returned plan, so `self` is never executed and
    /// nothing is double-counted or double-planned.
    fn sibling(&self, fetch: Option<usize>, topk: Option<TopKThreshold>) -> Self {
        LogsScanExec {
            tenant_hash: self.tenant_hash,
            fetcher: self.fetcher.clone(),
            segments: Arc::clone(&self.segments),
            target_partitions: self.target_partitions,
            counts: Arc::clone(&self.counts),
            prefetch_pool: Arc::clone(&self.prefetch_pool),
            ts_min: self.ts_min,
            ts_max: self.ts_max,
            content: Arc::clone(&self.content),
            prune: Arc::clone(&self.prune),
            erasure: Arc::clone(&self.erasure),
            projection: Arc::clone(&self.projection),
            columns: self.columns.clone(),
            projected_fraction: self.projected_fraction,
            declared: Arc::clone(&self.declared),
            attr_keys: Arc::clone(&self.attr_keys),
            column_stats: self.column_stats.clone(),
            segment_timing: self.segment_timing,
            segments_pruned_by_stats: self.segments_pruned_by_stats,
            fetch,
            topk,
            full_schema: Arc::clone(&self.full_schema),
            schema: Arc::clone(&self.schema),
            row_refs: self.row_refs,
            columnar_eligible: self.columnar_eligible,
            stripe_blocks: self.stripe_blocks,
            properties: Arc::clone(&self.properties),
            phase_accounting: self.phase_accounting.clone(),
            metrics: self.metrics.clone(),
            created_at: self.created_at,
        }
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
        phase_accounting: PhaseAccounting,
        full_schema: SchemaRef,
        declared: Arc<Vec<DeclaredColumn>>,
        attr_keys: Vec<String>,
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
        let full_len = full.fields().len();
        // The scan's EXTENDED schema is the resolved full schema followed by one
        // `Utf8` field per synthetic per-key attribute column (issue #1768), in
        // `attr_keys` order. A projection index at or past `full_len` selects
        // `attr_keys[i - full_len]`. With no per-key columns this is exactly the
        // full schema, so every pre-#1768 caller is unaffected.
        let effective: SchemaRef = if attr_keys.is_empty() {
            Arc::clone(&full)
        } else {
            let mut fields = full.fields().to_vec();
            for key in &attr_keys {
                fields.push(Arc::new(Field::new(
                    attr_key_field_name(key),
                    DataType::Utf8,
                    true,
                )));
            }
            Arc::new(Schema::new(fields))
        };
        // A `None` projection means every column, in schema order. Resolving it
        // here rather than carrying an `Option` keeps one code path for the
        // schema, the batch builder, and the column-set resolution. A `None`
        // projection never selects a synthetic per-key column: those are only
        // ever introduced through an explicit projection by the
        // `AttrsPerKeyProjection` rule.
        let projection: Vec<usize> = match projection {
            Some(p) => p,
            None => (0..full_len).collect(),
        };
        for &i in &projection {
            if i >= effective.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "logs scan projection index {i} out of range"
                )));
            }
        }
        let resolved = resolve_columns(
            &projection,
            &content,
            &erasure,
            &declared,
            &attr_keys,
            full_len,
        );
        let projected_fraction = resolved.fraction_of(declared.len());
        let columns = resolved.selection;
        let projected = effective.project(&projection)?;
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
            prefetch_pool: Arc::new(PrefetchPool::new(declared_partitions)),
            ts_min,
            ts_max,
            content,
            prune,
            erasure,
            projection: Arc::new(projection),
            columns,
            projected_fraction,
            declared,
            attr_keys: Arc::new(attr_keys),
            column_stats: None,
            segment_timing: false,
            segments_pruned_by_stats: 0,
            fetch: None,
            topk: None,
            full_schema: full,
            schema,
            row_refs,
            columnar_eligible,
            stripe_blocks,
            properties,
            phase_accounting,
            metrics: ExecutionPlanMetricsSet::new(),
            created_at: Instant::now(),
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

    /// Narrow whole-plan statistics computed under [`Self::stats_are_exact`]
    /// to what a scan carrying a pushed `fetch` emits: each partition stops at
    /// `min(fetch, rows it owns)`, so over `n` partitions and `total` committed
    /// rows the scan emits between `min(fetch, total)` and
    /// `min(fetch * n, total)`. `num_rows` becomes `min(fetch, total)`, `Exact`
    /// when the two bounds meet (one partition, `fetch == 0`, or `total <=
    /// fetch`) and `Inexact` otherwise.
    ///
    /// Unless every committed row is emitted (`total <= fetch`), the scan emits
    /// a subset whose min/max/NULL count the catalog figures only bound, so
    /// every column statistic drops to `Inexact` too. An unknown total proves
    /// nothing either way and is treated the same.
    fn apply_fetch_to_statistics(&self, stats: &mut Statistics, fetch: usize) {
        if let Precision::Exact(total) = stats.num_rows {
            if total <= fetch {
                return;
            }
            let partitions = self.properties().partitioning.partition_count();
            let upper = fetch.saturating_mul(partitions).min(total);
            stats.num_rows = if upper == fetch {
                Precision::Exact(fetch)
            } else {
                Precision::Inexact(fetch)
            };
        }
        stats.column_statistics = std::mem::take(&mut stats.column_statistics)
            .into_iter()
            .map(ColumnStatistics::to_inexact)
            .collect();
    }

    /// Exact MIN/MAX (plus the exact NULL count) for every declared column at
    /// once, indexed by `k` (`FIRST_DECLARED_COL + k`), resolved in a SINGLE
    /// pass over the touched segments.
    ///
    /// The one carrier is the `SegmentRef` stamp
    /// (`SegmentRef::declared_column_stats`, ADR-0873), which rides snapshot
    /// resolution itself and so covers the live tail and token-resolved
    /// segments. It is a `DeclaredColumnStats`, the constructor-gated validated
    /// container, and [`stamp_coverage`] takes that type rather than a slice of
    /// its entries: holding one is proof the statistics validity predicate ran,
    /// row-count clauses included, so this reader re-checks only what is local
    /// to the query (the declared type it was resolved under) and its
    /// `null_count` needs no reconciliation here. A `.cstat` entry does not
    /// answer, because it describes the record-level cells rather than the
    /// merged value SQL returns ([`segment_declared_coverage`]).
    ///
    /// `result[k]` is `None` when the #849 safety lemma requires falling back
    /// to scanning for that column: a touched segment with no usable stamp for
    /// it (never stamped, a type the stamp vocabulary excludes such as `Bytes`,
    /// or a stamp the reader refuses), whether or not a `.cstat` entry covers
    /// it, or an unsupported declared type (`Str`, projected as
    /// `Dictionary(Int32, Utf8)`, has no scalar form on this path).
    ///
    /// A result whose `min`/`max` are `None`-valued scalars is still exact,
    /// not a fallback: every covered segment's column was entirely null, or
    /// there are zero segments, and SQL `MIN`/`MAX` over all-NULL or zero-row
    /// input is `NULL`.
    ///
    /// [`DeclaredExactStats::null_count`] is the sum of the stamps' NULL
    /// counts, each of which passed clauses 4 and 5 against the carrying
    /// record's own row count before the type existed.
    ///
    /// Resolving every column in one segment walk instead of one full walk per
    /// column keeps `statistics_from_inputs` cost at
    /// `O(segments x columns_per_segment)` rather than
    /// `O(segments x declared x columns_per_segment)`; DataFusion may call
    /// `statistics_from_inputs` several times per plan, so the per-call cost
    /// matters.
    fn declared_min_max_all(&self) -> Vec<Option<DeclaredExactStats>> {
        let n = self.declared.len();
        let mut result: Vec<Option<DeclaredExactStats>> = vec![None; n];

        // Per-column running extrema and NULL-count sum, plus a "declined"
        // flag: a column declines its whole answer the moment one touched
        // segment has no stamp for it. A `Str` column declines up front.
        struct Acc {
            declined: bool,
            min: Option<ScalarValue>,
            max: Option<ScalarValue>,
            null_count: Option<u64>,
        }
        let mut acc: Vec<Acc> = self
            .declared
            .iter()
            .map(|d| Acc {
                declined: matches!(d.ty, DeclaredType::Str),
                min: None,
                max: None,
                null_count: Some(0),
            })
            .collect();

        for seg in self.segments.iter() {
            let seg_stats = segment_column_stats(self.column_stats.as_deref(), seg);
            for (k, a) in acc.iter_mut().enumerate() {
                if a.declined {
                    continue;
                }
                // No stamp (the ordinary state of pre-stamp history, never an
                // error): the column falls back to a scan.
                let Some(coverage) = segment_declared_coverage(&self.declared[k], seg, seg_stats)
                else {
                    a.declined = true;
                    continue;
                };

                if let Some(min) = coverage.min {
                    a.min = keep_extreme(a.min.take(), min, std::cmp::Ordering::Less);
                }
                if let Some(max) = coverage.max {
                    a.max = keep_extreme(a.max.take(), max, std::cmp::Ordering::Greater);
                }
                a.null_count = a
                    .null_count
                    .and_then(|sum| sum.checked_add(coverage.null_count));
            }
            // Nothing left to resolve once every column has declined, which is
            // the common shape on a tenant with no stamps: one segment
            // decides it and the remaining segments would only be walked to
            // re-skip every column.
            if acc.iter().all(|a| a.declined) {
                break;
            }
        }

        for (k, a) in acc.into_iter().enumerate() {
            if a.declined {
                continue;
            }
            let null_scalar = declared_null_scalar(self.declared[k].ty);
            result[k] = Some(DeclaredExactStats {
                min: a.min.unwrap_or_else(|| null_scalar.clone()),
                max: a.max.unwrap_or(null_scalar),
                null_count: a.null_count,
            });
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
    /// segment whose entry [`merged_view_entry`] grants (an entry whose row
    /// accounting disagrees with the joined `SegmentRef::sample_count`
    /// describes rows this object does not have, and an entry the segment's
    /// stamp does not match may omit rows whose value comes from the resource
    /// or scope, so neither one's dictionary counts can be summed), and one
    /// reason specific to this path: any
    /// covered segment whose dictionary is absent because its distinct-value
    /// count exceeded the fold's cardinality ceiling (ADR-0850 decision 3) has
    /// no exact per-value count to subtract, so a count derived from it could
    /// be wrong outright, not merely unavailable.
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
            let seg_stats = stats.stat_for(&seg.content_hash, &segment_identity(seg))?;
            let stat = merged_view_entry(declared, seg, seg_stats)?;
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
    /// does, the [`merged_view_entry`] gate on every entry read and the
    /// [`Self::stats_are_exact`] gate included: this shape carries
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
            let seg_stats = stats.stat_for(&seg.content_hash, &segment_identity(seg))?;
            let stat = merged_view_entry(declared, seg, seg_stats)?;
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

    /// Exact sum and non-null count for the declared column at schema index
    /// `FIRST_DECLARED_COL + k` (ADR-0850's q03/q04/q30 shapes, #861), summed
    /// across every touched segment's exact per-object statistics. `None` means
    /// fall back to scanning, for the same reasons
    /// [`Self::declared_not_equal_count`] does (the [`Self::stats_are_exact`]
    /// gate: no pending erasure, no content or prune predicate, a ts bound that
    /// clips no touched segment; and the [`merged_view_entry`] gate, which
    /// refuses a segment's whole entry, its stored sum included, when the
    /// entry's row accounting disagrees with the joined
    /// `SegmentRef::sample_count` or the segment's stamp does not match it),
    /// plus two specific to this path:
    ///
    /// - the column is not integer-typed. A sum is stored for `I64` columns
    ///   only (#861): a float fold would be order-dependent, so a non-`I64`
    ///   column carries no sum and its `SUM`/`AVG` scans;
    /// - a touched segment's stat omits its sum. That happens for a would-be
    ///   float column, for an exact per-object sum that overflowed `i64` at fold
    ///   time, or for a segment never covered by the loaded stats. In every case
    ///   there is no exact sum to add, so the whole answer declines rather than
    ///   undercounting.
    ///
    /// Unlike the q02/q08 dictionary paths this needs no `dictionary_present`:
    /// the sum is exact independently of whether the value dictionary was kept,
    /// so a high-cardinality integer column still decomposes.
    pub(crate) fn declared_column_sum(&self, k: usize) -> Option<DeclaredColumnSum> {
        if !self.stats_are_exact() {
            return None;
        }
        let declared = self.declared.get(k)?;
        if !matches!(declared.ty, DeclaredType::I64) {
            return None;
        }
        let stats = self.column_stats.as_ref()?;
        let mut sum: i128 = 0;
        let mut non_null_count: u64 = 0;
        for seg in self.segments.iter() {
            let seg_stats = stats.stat_for(&seg.content_hash, &segment_identity(seg))?;
            let stat = merged_view_entry(declared, seg, seg_stats)?;
            let seg_sum = stat.sum?;
            sum = sum.checked_add(i128::from(seg_sum))?;
            non_null_count = non_null_count.checked_add(stat.non_null_count)?;
        }
        Some(DeclaredColumnSum {
            sum,
            non_null_count,
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

    /// The width of the resolved full schema
    /// (`logs_schema_with_declared(&declared).fields().len()`). A projection
    /// index at or past this selects a synthetic per-key attribute column
    /// (issue #1768); the [`crate::attrs_per_key::AttrsPerKeyProjection`] rule
    /// builds its per-key indices as `full_schema_len() + j`.
    pub(crate) fn full_schema_len(&self) -> usize {
        self.full_schema.fields().len()
    }

    /// Whether this scan already carries synthetic per-key attribute columns
    /// (issue #1768). The `AttrsPerKeyProjection` rule refuses to rewrite a scan
    /// twice, so a scan it already touched reports `true` and is left alone.
    pub(crate) fn has_attr_keys(&self) -> bool {
        !self.attr_keys.is_empty()
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
            attr_keys: Arc::clone(&self.attr_keys),
            full_len: self.full_schema.fields().len(),
            schema: Arc::clone(&self.schema),
            accounting: self.phase_accounting.scan().clone(),
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
    /// Synthetic per-key attribute columns (issue #1768), so phase 2 rebuilds
    /// the same per-key `Utf8` columns the single-phase scan would have.
    attr_keys: Arc<Vec<String>>,
    /// The resolved full schema width, so a projection index past it maps to
    /// `attr_keys[index - full_len]` (issue #1768).
    full_len: usize,
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
        // On the fetcher's read gate when it carries one, like the scan's own
        // row path.
        let records = scan
            .next_block_on_gate()
            .await
            .map_err(SqlError::from)?
            .ok_or_else(|| {
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
            &self.attr_keys,
            self.full_len,
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
        )?;
        if self.active_topk().is_some() {
            write!(f, ", topk_threshold=ts")?;
        }
        Ok(())
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

    // Visits nothing: content/prune are Ravel `Predicate`s, not
    // `PhysicalExpr`s, and reporting the kept dynamic filter would tell a
    // join or aggregate producer, which checks for a consumer through this
    // walk, to compute a filter the scan only applies from a confirmed TopK.
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

    /// The row count DataFusion's `LimitPushdown` optimizer rule has pushed
    /// into this scan (issue #362), if any. See [`Self::fetch`] the field's
    /// doc comment for what this scan may act on it for.
    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Install a pushed `fetch` (issue #362), returning a sibling scan that is
    /// otherwise identical to this one (see [`Self::sibling`]).
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(self.sibling(limit, self.topk.clone())))
    }

    /// Keep the first dynamic filter whose leading key is `ts` as an
    /// unconfirmed threshold (ADR-2677 decision 6), and report every parent
    /// filter unsupported.
    ///
    /// The scan never applies the filter to a row: it only reads its `ts`
    /// threshold to skip whole blocks and segments that cannot hold a
    /// qualifying row, so the TopK above stays the exact evaluator. Only the
    /// `Post` phase carries TopK filters. A filter leading with `ts` may come
    /// from a descending or nulls-first TopK, an aggregate, or a join, none
    /// of which the skip can serve, so it does nothing until
    /// [`ConfirmTopKThreshold`] matches it to an ascending nulls-last TopK.
    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let filters = vec![PushedDown::No; child_pushdown_result.parent_filters.len()];
        if phase != FilterPushdownPhase::Post || self.topk.is_some() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                filters,
            ));
        }
        let topk = child_pushdown_result.parent_filters.iter().find_map(|f| {
            let filter: Arc<dyn PhysicalExpr> = Arc::clone(&f.filter);
            let dynamic = Arc::downcast::<DynamicFilterPhysicalExpr>(filter).ok()?;
            TopKThreshold::from_filter(&dynamic)
        });
        let mut propagation = FilterPushdownPropagation::with_parent_pushdown_result(filters);
        if let Some(topk) = topk {
            let node: Arc<dyn ExecutionPlan> = Arc::new(self.sibling(self.fetch, Some(topk)));
            propagation = propagation.with_updated_node(node);
        }
        Ok(propagation)
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
    /// min/max spanning every touched segment.
    ///
    /// A declared typed column reports an `Exact` min/max too, and an `Exact`
    /// `null_count`, whenever [`Self::declared_min_max_all`] resolves it from
    /// the ADR-0873 `SegmentRef` stamps. Every column that is neither `ts` nor
    /// a resolved declared column stays `Absent`, as does a declared column
    /// any touched segment leaves unstamped. `total_byte_size` stays `Absent`.
    ///
    /// A pushed `fetch` narrows all of this to what the scan emits under it,
    /// as [`Self::apply_fetch_to_statistics`] describes.
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        let partition = args.partition();
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

            // ADR-0873: the same gate widens to a declared column's exact
            // min/max and NULL count, taken from the `SegmentRef` stamps.
            // `declared_min_max_all` resolves every declared column in one
            // segment walk and enforces the per-column fallback (an unstamped
            // segment, a refused stamp, or an unsupported declared type all
            // report `None`, leaving the column `Absent`); this loop only
            // decides which output index to fill.
            // Skip the whole walk when the projection carries no declared
            // column: statistics_from_inputs runs several times per plan, and a
            // ts-only statement must not pay one lookup per (segment, column).
            let projects_declared = self.projection.iter().any(|&i| i >= FIRST_DECLARED_COL);
            let declared_min_max = if projects_declared {
                self.declared_min_max_all()
            } else {
                Vec::new()
            };
            for (k, exact) in declared_min_max.into_iter().enumerate() {
                let schema_idx = FIRST_DECLARED_COL + k;
                if let Some(out_idx) = self.projection.iter().position(|&i| i == schema_idx)
                    && let Some(exact) = exact
                {
                    let col = &mut stats.column_statistics[out_idx];
                    col.min_value = Precision::Exact(exact.min);
                    col.max_value = Precision::Exact(exact.max);
                    // An unproven NULL count leaves `null_count` `Absent`
                    // rather than reporting an unproven figure as exact;
                    // the extrema stay exact either way.
                    if let Some(nulls) = exact.null_count
                        && let Ok(nulls) = usize::try_from(nulls)
                    {
                        col.null_count = Precision::Exact(nulls);
                    }
                }
            }

            if let Some(fetch) = self.fetch {
                self.apply_fetch_to_statistics(&mut stats, fetch);
            }
        }
        Ok(Arc::new(stats))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.prefetch_pool.register(partition)?;
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
            phase_accounting: self.phase_accounting.clone(),
        });

        // #693 part 3 deliverable 1, amended by #739: a predicate-free query
        // whose window fully contains every relevant segment, with at least
        // `target_partitions` of them, skips the plan phase entirely, whatever
        // those segments' sizes are. Each partition computes its whole-segment
        // round-robin share with no I/O and opens each owned segment with one
        // whole-object GET (no plan probe, no scan-side probe). Any other shape
        // falls to the plan-then-stripe path below, byte for byte, and records
        // which conjunct sent it there.
        let blocks = BlockMetrics::new(&self.metrics, partition);
        let (work, fast_whole_segment, state) = match self.whole_segment_fast_path(&ctx.query) {
            Ok(relevant) => {
                let n = self.target_partitions.max(1).min(relevant.max(1));
                let order = visit_order(&self.segments, self.active_topk().is_some());
                let work = owned_whole_segments(
                    &self.segments,
                    &order,
                    self.ts_min,
                    self.ts_max,
                    partition,
                    n,
                );
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
                    self.stripe_blocks,
                    blocks.plan_init_elapsed.clone(),
                );
                (VecDeque::new(), false, LogScanState::Planning(counts_fut))
            }
        };
        // ADR-2414 decision A2: the fast path pipelines its ranged opens at
        // its share of the GET permits, which `get_limiter_permits` reads off
        // the process-shared limiter the server wires into the fetcher. Not
        // under a confirmed TopK threshold: a prefetch opens a segment before
        // the threshold that could skip it is read.
        let prefetch_share =
            if fast_whole_segment && self.fetch.is_none() && self.active_topk().is_none() {
                fast_path_prefetch_share(self.fetcher.get_limiter_permits(), self.target_partitions)
            } else {
                1
            };
        blocks.prefetch_share.add(prefetch_share);

        Ok(Box::pin(LogScanStream {
            schema: Arc::clone(&self.schema),
            projection: Arc::clone(&self.projection),
            declared: Arc::clone(&self.declared),
            attr_keys: Arc::clone(&self.attr_keys),
            full_len: self.full_schema.fields().len(),
            ctx,
            erasure: Arc::clone(&self.erasure),
            columnar_eligible: self.columnar_eligible,
            str_values: Arc::new(std::sync::Mutex::new(StrValuesCache::new(
                blocks.str_dict_values_built.clone(),
            ))),
            blocks,
            metrics: self.metrics.clone(),
            segment_timing: self.segment_timing,
            origin: self.created_at,
            stream_started: Instant::now(),
            open_started: None,
            first_batch_seen: false,
            done_seen: false,
            partition,
            stripe_blocks: self.stripe_blocks,
            fast_whole_segment,
            prefetch_share,
            topk: self.active_topk().cloned(),
            prefetch_pool: Arc::clone(&self.prefetch_pool),
            current_by_chunk: false,
            retrying: false,
            segments: Arc::clone(&self.segments),
            work,
            resident: reservation.new_empty(),
            reservation,
            held: 0,
            emitted: 0,
            str_values_charge: 0,
            pending: Pending::None,
            row_refs: self.row_refs,
            current_seg: None,
            current_seg_ordinal: 0,
            block_cursor: 0,
            consecutive_fallbacks: 0,
            pending_range: None,
            current_indices: Vec::new(),
            current_survivors: None,
            current_footer: None,
            current_whole_object: None,
            current_dirs: None,
            current_unplanned: false,
            state,
            decoded: None,
            fetch: self.fetch,
            rows_emitted: 0,
        }))
    }
}

/// Per-partition record-batch stream (ADR-0087 decisions 1 and 2).
///
/// Holds at most one segment's decoded block plus the batch built from it, and
/// charges the query's memory pool for exactly that: `held` is the charge
/// covering `pending`'s records or batches, `emitted` the charge covering the
/// batch handed downstream on the previous poll, and `resident` the columnar
/// path's decoded block, which travels into a read-gate job with the scan and
/// back. Each is released as its data goes away, so the reservation tracks
/// live resident memory rather than cumulative output and a pool overrun
/// surfaces as `ResourcesExhausted` at the moment the scan genuinely holds too
/// much.
///
/// The reservations live on the stream (not on the state) so they are the same
/// ones for the partition's lifetime, and each frees exactly once on drop.
struct LogScanStream {
    schema: SchemaRef,
    /// Indices into the resolved full schema to emit, in output order.
    projection: Arc<Vec<usize>>,
    /// The tenant's declared typed attribute columns (ADR-0090), consulted by
    /// [`build_batch`] and [`build_columnar_batches`] for a projected declared
    /// index.
    declared: Arc<Vec<DeclaredColumn>>,
    /// Synthetic per-key attribute columns (issue #1768), consulted by
    /// [`build_batch`] and [`build_columnar_batches`] for a projected per-key
    /// index (>= [`Self::full_len`]).
    attr_keys: Arc<Vec<String>>,
    /// The resolved full schema width (`full_schema.fields().len()`): a
    /// projection index at or past it selects a synthetic per-key attribute
    /// column, `attr_keys[index - full_len]`.
    full_len: usize,
    ctx: Arc<PartitionCtx>,
    erasure: Arc<Vec<ErasurePredicate>>,
    /// Whether this scan may attempt the columnar fast path (the query-shape
    /// clauses of [`columnar_static_eligible`]). When false, every segment
    /// drains the row path.
    columnar_eligible: bool,
    blocks: BlockMetrics,
    /// The Arrow values of the shared string dictionaries this partition's
    /// columnar blocks read, built once per dictionary and fallback list
    /// (ADR-2773 decision D3).
    str_values: SharedStrValuesCache,
    /// Reservation bytes covering the live entries of [`Self::str_values`],
    /// charged once per entry rather than once per batch sharing it.
    str_values_charge: usize,
    /// This stream's DataFusion partition index. Together with the shared
    /// [`PlanCounts`] it determines which `(segment, block-index-list)` units
    /// this partition owns (ADR-0102).
    partition: usize,
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
    /// The pipeline depth of the ranged fast path (ADR-2414 decision A2,
    /// [`fast_path_prefetch_share`]): how many of this partition's owned
    /// segments may be open at once, counting the one being opened or drained.
    /// While that segment is a ranged open, [`Self::top_up_prefetch`] issues the
    /// next owned segments' ranged opens up to this bound, and they are consumed
    /// in owned order, so rows and per-segment counters are those of the
    /// sequential walk, and so is per-query accounting unless a refused open
    /// drops prefetches, whose requests and bytes are then counted on top. A
    /// prefetched open holds the fetched
    /// column-chunk bytes of its segment, reserved against the fetcher's memory
    /// budget like any open's, so the bytes this partition holds for opened
    /// segments are bounded by this share times one segment's projected bytes,
    /// and a statement's by its partition count times that.
    ///
    /// `1`, no prefetch, everywhere else: on the striped route, and under a
    /// pushed `fetch`, which may stop the partition before segments it would
    /// have prefetched. A whole-object fast-path open is never prefetched and
    /// stops the pipeline at its turn, because an in-flight whole-object open
    /// holds a full object per slot; the `attrs_raw` fallback reopen also stays
    /// sequential, with the prefetches behind it left in flight unless the
    /// budget refuses it ([`Self::on_open_refused`]).
    prefetch_share: usize,
    /// [`LogsScanExec::active_topk`]: the confirmed TopK threshold this
    /// partition reads before opening each owned segment and before decoding
    /// each block, and that orders the planned path's segments.
    topk: Option<TopKThreshold>,
    /// The statement's prefetch pool, shared with the exec and every other
    /// partition's stream. This partition's issued opens of the owned
    /// segments after the current one live in its slot, [`Self::partition`].
    prefetch_pool: Arc<PrefetchPool>,
    /// Whether the current fast-path segment was opened ranged, the
    /// precondition for prefetching behind it.
    current_by_chunk: bool,
    /// Whether the open in flight is [`Self::on_open_refused`]'s one retry of
    /// a refused open. Cleared when an open resolves and at the next segment.
    retrying: bool,
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
    /// The charge for the decoded block the columnar cursor's reader still
    /// holds. The reader frees that block only when it decodes the next one,
    /// so this moves into the next block's job with the scan and is resized to
    /// the new block right after its decode ([`ColumnarBlockJob::run`]), not
    /// released when the old block's batches drain: on the read gate the next
    /// decode can wait for a permit while the old block stays resident.
    /// Charging the batches alone would break ADR-0087 decision 2's contract
    /// that the pool bounds concurrently-held scan memory.
    resident: MemoryReservation,
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
    /// fully emitted, i.e. the position within [`Self::current_indices`] of the
    /// block being drained. Advanced by both the columnar path and the row
    /// path, so it stays correct across an `attrs_raw` fallback's `Columnar` ->
    /// `RowFallbackBlock` -> `Columnar` round trip: it is the `skip` count a
    /// later fallback's [`LogScanState::ReopenRows`] re-derives from. A
    /// row-ref's block comes from the scan's own next block, not from this
    /// count ([`Self::current_block`]). Reset when a new segment starts.
    block_cursor: usize,
    /// Count of `attrs_raw`-overflow fallbacks (issue #1769) since the last
    /// clean columnar block in the current segment, reset to 0 at
    /// `NextSegment` and by [`ColumnarStep::Held`]. `max_dynamic_columns`
    /// (`crates/ravel-logseg/src/writer.rs`, `block.rs`) is a PER-OBJECT
    /// budget, so once a tenant's declared-plus-dynamic key count for a
    /// segment exceeds it, the overflow keys recur across most of that
    /// object's blocks: two fallbacks in a row is evidence the object's
    /// budget is exhausted for its remaining blocks, not that this one block
    /// was unlucky. [`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS`] is the count of
    /// consecutive fallbacks tolerated before the rest of this partition's
    /// block list for the segment is committed to the row path in one
    /// reopen instead of retrying columnar block by block,
    /// which is what bounds the segment's total reopens to a constant
    /// (`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS + 1`) instead of one per
    /// overflowing block.
    consecutive_fallbacks: usize,
    /// The address the row-path batch builder stamps from while draining the
    /// held block. `None` when this scan emits no row refs.
    pending_range: Option<RowRefRange>,
    /// The surviving-block index list this partition owns for [`Self::
    /// current_seg`], kept so the `attrs_raw` fallback re-opens the row path
    /// over the SAME list (ADR-0102). Set alongside `current_seg`.
    current_indices: Vec<usize>,
    /// The segment's whole surviving-block list, for resolving a row-ref's
    /// block position ([`OwnedSeg::survivors`]).
    current_survivors: Option<Arc<Vec<usize>>>,
    /// The plan-phase footer for [`Self::current_seg`] (#693 part 3, deliverable
    /// 2), kept so the `attrs_raw` fallback re-opens the subset with the same
    /// footer it first used. `None` on the whole-segment fast path.
    current_footer: Option<LogFooter>,
    /// The plan fallback's whole-object bytes for [`Self::current_seg`]
    /// (issue #835), consumed exactly once per stream by the FIRST open of
    /// this segment (`Option::take` at the `NextSegment` state, before
    /// `open_segment_subset`). `None` whenever the plan phase did not carry
    /// whole-object bytes forward for this segment, always `None` on the
    /// whole-segment fast path, and `None` here after that first take even
    /// though the carry existed -- a later `ReopenRows`/`attrs_raw`-overflow
    /// reopen of the SAME segment therefore takes the normal fetch/cache path
    /// rather than reusing these bytes a second time.
    ///
    /// Once per stream, not once per buffer: under block striping a segment's
    /// survivors can be owned by several partitions, each of which clones the
    /// carry and charges the object's length to
    /// `QueryAccounting::add_bytes_reused`. That matches what the figure
    /// counts, one avoided store call per open, and is the same convention
    /// those opens were charged under as cache hits before this carry existed.
    /// The memory bound is unaffected: `Bytes` clones share one allocation.
    current_whole_object: Option<CarriedWholeObject>,
    /// This partition's handle on the segment's directories, decoded once by
    /// the plan phase (ADR-2414 decision A1), kept so the `attrs_raw` fallback
    /// re-opens the subset through the same reused decode rather than a fresh
    /// one. Dropped when this partition finishes the segment
    /// ([`Self::finish_segment`]), which releases the directories' reservation
    /// once every owner has. `None` on the whole-segment fast path (no plan
    /// phase).
    current_dirs: Option<Arc<CarriedDirs>>,
    /// [`OwnedSeg::unplanned`] for [`Self::current_seg`]: opened and reopened
    /// whole, its survivor list taken from the open, and its prune totals
    /// recorded by this owner at exhaustion as on the fast path, since the
    /// plan produced none for partition 0 to publish.
    current_unplanned: bool,
    state: LogScanState,
    /// The block a [`LogScanState::DecodingColumnar`] or
    /// [`LogScanState::DecodingRows`] job decoded, taken by the state the job
    /// returned its scan to on the next turn of the poll loop.
    decoded: Option<Decoded>,
    /// The `fetch` pushed into the exec (issue #362), carried unchanged from
    /// [`LogsScanExec::fetch`]. `None` means this stream drains every segment
    /// it owns, exactly the pre-#362 behavior.
    fetch: Option<usize>,
    /// Rows this partition has emitted so far, summed at every batch actually
    /// handed downstream (post content-filter and post `attrs_raw` erasure --
    /// whatever a caller of this stream actually receives). Once it reaches
    /// `fetch` the stream ends, so a partition finishes with exactly
    /// `min(fetch, rows it owns)` rows, never more and never fewer.
    rows_emitted: usize,
    /// The exec's metric set, kept so per-segment timeline points can be
    /// published as labelled metrics (`segment=<ordinal>`) alongside the
    /// per-partition totals in [`Self::blocks`].
    metrics: ExecutionPlanMetricsSet,
    /// Whether [`Self::mark_segment`] does anything (`SqlConfig::
    /// segment_timing`, issue #913). `false` by default: no label
    /// allocation, no metric registration, so `accumulate_scan_timing` finds
    /// no per-segment rows.
    segment_timing: bool,
    /// The exec's creation instant; every `*_offset` metric is measured from it.
    origin: Instant,
    /// When this partition's stream was built; `planning_wait_elapsed` and
    /// `stream_elapsed` start here.
    stream_started: Instant,
    /// When the open (or reopen) currently in flight was constructed.
    open_started: Option<Instant>,
    /// Whether `first_batch_elapsed` has been recorded.
    first_batch_seen: bool,
    /// Whether `stream_elapsed` has been recorded. `LogScanState::Done` keeps
    /// returning `Poll::Ready(None)`, and the metric holds an offset from the
    /// origin rather than an interval, so a second poll after completion would
    /// overwrite a correct figure with a larger one that still looks monotone.
    done_seen: bool,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod cstat_reconcile_tests {
    //! The ADR-0873 clause 4 row-accounting gate at the three exact aggregate
    //! paths (#1037), pinned at the granularity the drop metric is defined on:
    //! ONE read of the entry.
    //!
    //! These call [`LogsScanExec::declared_not_equal_count`],
    //! [`LogsScanExec::declared_group_counts`] and
    //! [`LogsScanExec::declared_column_sum`] directly rather than through a
    //! statement, because a planned statement also resolves
    //! `statistics_from_inputs`, which reads the same entry again through
    //! [`cstat_coverage`]: the metric has observation semantics (one increment
    //! per read, no per-entry dedup), so an exact-delta assertion means
    //! something only when the test knows how many reads it performed. The
    //! answer-level half of these deliverables, where the fallback scan's
    //! aggregate is compared against the truth over real RLOG objects, is in
    //! tests/logs_metadata_agg.rs.
    //!
    //! All tests hold one lock: the tally is process-wide and monotonic.
    //! Nothing here reads an object, so the segment is fabricated and the store
    //! stays empty.

    use super::*;
    use ravel_commit::declared_stats::declared_stat_drops_observed;
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const COL: &str = "status";
    /// Rows in the fabricated segment, the figure an entry's
    /// `non_null_count + null_count` must reconcile against.
    const SAMPLE_COUNT: u64 = 5;

    static DROPS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn drops_lock() -> std::sync::MutexGuard<'static, ()> {
        match DROPS.lock() {
            Ok(guard) => guard,
            // A poisoned lock means a sibling test panicked; the tally is still
            // sound to read, and re-reporting that failure here would only
            // obscure it.
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// A `sample_count`-row segment stamped with what a writer stamps for the
    /// rows a reconciled [`status_stat`] describes (`[200, 404]`, four non-null
    /// rows), so the stamp matches that entry and the entry may answer. A
    /// segment too small to hold four rows carries no stamp.
    fn seg_ref(sample_count: u64) -> SegmentRef {
        let declared_column_stats = match sample_count.checked_sub(4) {
            Some(null_count) => {
                let stat = ravel_types::declared_stats::DeclaredColumnStat::new(
                    COL,
                    DeclaredStatType::I64,
                    Some(DeclaredStatValue::I64(200)),
                    Some(DeclaredStatValue::I64(404)),
                    null_count,
                )
                .expect("valid stamp");
                let mut record = ravel_proto::commit::v1::CommitRecord {
                    sample_count,
                    ..Default::default()
                };
                ravel_commit::declared_stats::stamp_commit_record(&mut record, &[stat]);
                DeclaredColumnStats::from_validated(
                    &ravel_commit::declared_stats::read_commit_record(&record),
                )
            }
            None => DeclaredColumnStats::default(),
        };
        SegmentRef {
            data_object_key: "logs/seg-1.rlog".to_string(),
            object_size: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 1_000,
            ingest_hour_bucket: 0,
            sample_count,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: uuid::Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: ravel_catalog::SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats,
        }
    }

    fn i64_value(v: i64) -> ColumnValue {
        ColumnValue {
            kind: Some(ColumnValueKind::I64(v)),
        }
    }

    /// A `status` entry over the dictionary `{200: 3, 404: 1}`: four non-null
    /// rows, an exact sum of 1004, and `null_count` supplied by the caller so
    /// the same otherwise-valid entry can be built reconciled (`null_count =
    /// 1`, so `4 + 1 == SAMPLE_COUNT`) or divergent.
    fn status_stat(null_count: u64) -> ColumnStat {
        ColumnStat {
            name: COL.to_string(),
            declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
            non_null_count: 4,
            null_count,
            min: Some(i64_value(200)),
            max: Some(i64_value(404)),
            dictionary_present: true,
            dictionary: vec![
                ravel_proto::catalog::v1::DictEntry {
                    value: Some(i64_value(200)),
                    count: 3,
                },
                ravel_proto::catalog::v1::DictEntry {
                    value: Some(i64_value(404)),
                    count: 1,
                },
            ],
            sum: Some(1004),
        }
    }

    /// A scan over one fabricated `sample_count`-row segment carrying `columns`
    /// as its `.cstat` entries. No predicate and unbounded ts, so
    /// `stats_are_exact` holds and the entry itself is what decides each read.
    fn scan_with(sample_count: u64, columns: Vec<ColumnStat>) -> LogsScanExec {
        let seg = seg_ref(sample_count);
        let declared = vec![DeclaredColumn::new(COL, DeclaredType::I64)];
        let mut segments = HashMap::new();
        segments.insert(
            segment_identity(&seg),
            ColumnStatsSegment {
                ingest_hour_bucket: seg.ingest_hour_bucket,
                shard: seg.shard,
                writer_id: seg.writer_id.as_bytes().to_vec(),
                writer_epoch: seg.writer_epoch,
                writer_seq: seg.writer_seq,
                columns,
            },
        );
        let stats = Arc::new(LoadedColumnStats {
            segments,
            by_content_hash: HashMap::new(),
            part_blake3: Vec::new(),
        });
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let schema = crate::logs_schema::logs_schema_with_declared(&declared);
        LogsScanExec::new(
            TENANT,
            LogSegmentFetcher::new(backend),
            std::slice::from_ref(&seg),
            1,
            i64::MIN,
            i64::MAX,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            None,
            PhaseAccounting::new(),
            schema,
            Arc::new(declared),
        )
        .expect("scan")
        .with_column_stats(Some(stats))
    }

    /// Perform exactly one read of the entry and report what it answered plus
    /// the `cstat`-labelled drop delta across that single read.
    fn one_read<T>(
        sample_count: u64,
        columns: Vec<ColumnStat>,
        read: impl FnOnce(&LogsScanExec) -> Option<T>,
    ) -> (Option<T>, u64) {
        let guard = drops_lock();
        let scan = scan_with(sample_count, columns);
        let before = declared_stat_drops_observed(StatCarrier::Cstat);
        let answered = read(&scan);
        let delta = declared_stat_drops_observed(StatCarrier::Cstat) - before;
        drop(guard);
        (answered, delta)
    }

    fn not_equal_404(scan: &LogsScanExec) -> Option<u64> {
        scan.declared_not_equal_count(0, &ScalarValue::Int64(Some(404)))
    }

    /// q02 (`declared_not_equal_count`): an entry accounting for four of the
    /// segment's five rows describes an object that does not exist, so the read
    /// is refused and counted once. Every other reason this path declines is
    /// excluded by construction: the dictionary is present and its counts sum
    /// to `non_null_count`, so before this change the site subtracted from an
    /// unreconciled entry and answered 3.
    ///
    /// Prove-the-test: replace `reconciled_column_stat` with
    /// `unique_column_stat(seg_stats, &declared.key)?` at the
    /// `declared_not_equal_count` site (logs_scan.rs) and this reads
    /// `(Some(3), 0)`; both assertions fail.
    #[test]
    fn q02_read_of_an_unreconciled_entry_is_refused_and_counted_once() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(0)], not_equal_404);
        assert_eq!(answered, None, "4 + 0 != 5, so the entry grants nothing");
        assert_eq!(delta, 1, "one refused read, counted once");
    }

    /// The reconciled half of the same site: `4 + 1 == 5`, so the entry still
    /// answers exactly (four non-null rows less the single 404) with no drop.
    /// Without this a reconciliation that refused every entry would satisfy the
    /// test above.
    #[test]
    fn q02_read_of_a_reconciled_entry_answers_exactly_with_no_drop() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(1)], not_equal_404);
        assert_eq!(answered, Some(3), "4 non-null rows less the one 404");
        assert_eq!(delta, 0, "a reconciled entry is not a defect");
    }

    /// q08 (`declared_group_counts`): the same divergent entry is refused and
    /// counted once.
    ///
    /// Prove-the-test: revert the `declared_group_counts` site to
    /// `unique_column_stat(seg_stats, &declared.key)?` and this reads groups
    /// `{200: 3, 404: 1}` with a NULL group of 0 and a delta of 0.
    #[test]
    fn q08_read_of_an_unreconciled_entry_is_refused_and_counted_once() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(0)], |scan| {
            scan.declared_group_counts(0)
                .map(|counts| (counts.counts, counts.null_count))
        });
        assert!(
            answered.is_none(),
            "4 + 0 != 5, so the entry grants nothing"
        );
        assert_eq!(delta, 1, "one refused read, counted once");
    }

    /// The reconciled half of the q08 site: the exact groups and the exact NULL
    /// group, with no drop.
    #[test]
    fn q08_read_of_a_reconciled_entry_answers_exactly_with_no_drop() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(1)], |scan| {
            scan.declared_group_counts(0).map(|counts| {
                let mut pairs: Vec<(i64, u64)> = counts
                    .counts
                    .into_iter()
                    .map(|(value, count)| match value {
                        ScalarValue::Int64(Some(v)) => (v, count),
                        other => panic!("unexpected group key {other:?}"),
                    })
                    .collect();
                pairs.sort_unstable();
                (pairs, counts.null_count)
            })
        });
        assert_eq!(
            answered,
            Some((vec![(200, 3), (404, 1)], 1)),
            "the exact dictionary plus the one NULL row"
        );
        assert_eq!(delta, 0, "a reconciled entry is not a defect");
    }

    /// q03/q04/q30 (`declared_column_sum`): the same divergent entry is refused
    /// and counted once. Its `sum` is present, so `stat.sum?` cannot be what
    /// declines here.
    ///
    /// Prove-the-test: revert the `declared_column_sum` site to
    /// `unique_column_stat(seg_stats, &declared.key)?` and this reads
    /// `Some((1004, 4))` with a delta of 0.
    #[test]
    fn sum_read_of_an_unreconciled_entry_is_refused_and_counted_once() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(0)], |scan| {
            scan.declared_column_sum(0)
                .map(|cs| (cs.sum, cs.non_null_count))
        });
        assert_eq!(answered, None, "4 + 0 != 5, so the entry grants nothing");
        assert_eq!(delta, 1, "one refused read, counted once");
    }

    /// The reconciled half of the sum site: the exact sum and non-null count,
    /// with no drop.
    #[test]
    fn sum_read_of_a_reconciled_entry_answers_exactly_with_no_drop() {
        let (answered, delta) = one_read(SAMPLE_COUNT, vec![status_stat(1)], |scan| {
            scan.declared_column_sum(0)
                .map(|cs| (cs.sum, cs.non_null_count))
        });
        assert_eq!(answered, Some((1004, 4)), "200*3 + 404*1 over 4 rows");
        assert_eq!(delta, 0, "a reconciled entry is not a defect");
    }

    /// The overflow arm of the same gate: `non_null_count + null_count` that
    /// does not fit `u64` is a disagreement, not a wrap. The segment here holds
    /// ZERO rows, which is what makes the arm observable rather than incidental:
    /// `u64::MAX + 1` wraps to exactly 0, so a wrapping add reconciles this
    /// entry against an empty object and grants a sum over `u64::MAX` rows that
    /// no object holds.
    ///
    /// Prove-the-test: replace the `checked_add` in `reconciled_column_stat`
    /// with `wrapping_add`; the entry then reconciles instead of being
    /// refused, so the drop delta reads 0 and the `delta` assertion fails.
    /// The `answered == None` assertion does not distinguish the two paths:
    /// `reconciled_column_stat` (the overflow refusal and its drop count) runs
    /// before `stamp_coverage`, and this zero-row segment carries no stamp, so
    /// `merged_view_entry` declines at `stamp_coverage` whether or not the
    /// entry was refused. The drop counter is what pins the overflow refusal.
    #[test]
    fn an_overflowing_row_accounting_is_refused_and_counted_once() {
        let mut overflowing = status_stat(1);
        overflowing.non_null_count = u64::MAX;
        overflowing.null_count = 1;
        let (answered, delta) = one_read(0, vec![overflowing], |scan| {
            scan.declared_column_sum(0)
                .map(|cs| (cs.sum, cs.non_null_count))
        });
        assert_eq!(
            answered, None,
            "no stamp on a zero-row segment, so nothing answers"
        );
        assert_eq!(
            delta, 1,
            "the overflowing entry is refused and counted once"
        );
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
        let declared = declared();
        let full_len = FIRST_DECLARED_COL + declared.len();
        resolve_columns(projection, &[], &[], &declared, &[], full_len)
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
        let declared = declared();
        let full_len = FIRST_DECLARED_COL + declared.len();
        let r = resolve_columns(
            &[LOG_COL_TS, FIRST_DECLARED_COL],
            &[],
            &erasure,
            &declared,
            &[],
            full_len,
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
        let declared = declared();
        let full_len = FIRST_DECLARED_COL + declared.len();
        let r = resolve_columns(&[LOG_COL_TS], &content, &[], &declared, &[], full_len);
        assert_eq!(r.width, Some(3), "ts, stream_ref, body");
    }

    /// A tenant with no declared columns has a ten-column object population, so
    /// the same two-column selection is a larger share of it. The denominator is
    /// the tenant's, not a constant.
    #[test]
    fn the_denominator_follows_the_declared_set() {
        let r = resolve_columns(&[LOG_COL_TS], &[], &[], &[], &[], FIRST_DECLARED_COL);
        assert_eq!(r.width, Some(2));
        assert_eq!(r.fraction_of(0), 2.0 / 10.0);
    }
}
