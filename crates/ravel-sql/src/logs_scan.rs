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
//! place. Since #835, [`LogSegmentFetcher::plan_segment`]'s fallback branch carries its fetched
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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, DictionaryArray, FixedSizeBinaryBuilder, Int32Array,
    Int64Builder, MapBuilder, StringArray, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::ColumnStatistics;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
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
    AttrColumn, BoolCursor, BytesCursor, ColumnSelection, ColumnarBlockView, F64BitsCursor,
    FieldSel, FieldType, I64Cursor, LogRecord, LogSegError, Predicate, ScanStats,
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
    /// column keeps `partition_statistics` cost at
    /// `O(segments x columns_per_segment)` rather than
    /// `O(segments x declared x columns_per_segment)`; DataFusion may call
    /// `partition_statistics` several times per plan, so the per-call cost
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

    /// The row count DataFusion's `LimitPushdown` optimizer rule has pushed
    /// into this scan (issue #362), if any. See [`Self::fetch`] the field's
    /// doc comment for what this scan may act on it for.
    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Install a pushed `fetch` (issue #362), returning a sibling scan that is
    /// otherwise identical to this one.
    ///
    /// An exhaustive struct literal rather than a call through [`Self::build`]:
    /// every field of [`LogsScanExec`] is named here explicitly, so a field
    /// added to the struct later without a matching line here is a compile
    /// error, not a silently-dropped one (the exact bug class the
    /// `segment_timing` fix on this file addressed for [`Self::reproject`]).
    /// `counts`, `prefetch_pool` and `metrics` are shared with `self` via
    /// `Arc`/`Arc<Mutex<_>>` clone rather than rebuilt, matching upstream's own `with_fetch` on leaf
    /// nodes like `StreamingTableExec`: the optimizer replaces `self` with the
    /// returned plan in the tree, so `self` is dropped and never executed, and
    /// nothing is ever double-counted or double-planned.
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(LogsScanExec {
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
            fetch: limit,
            full_schema: Arc::clone(&self.full_schema),
            schema: Arc::clone(&self.schema),
            row_refs: self.row_refs,
            columnar_eligible: self.columnar_eligible,
            stripe_blocks: self.stripe_blocks,
            properties: Arc::clone(&self.properties),
            phase_accounting: self.phase_accounting.clone(),
            metrics: self.metrics.clone(),
            created_at: self.created_at,
        }))
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

            // ADR-0873: the same gate widens to a declared column's exact
            // min/max and NULL count, taken from the `SegmentRef` stamps.
            // `declared_min_max_all` resolves every declared column in one
            // segment walk and enforces the per-column fallback (an unstamped
            // segment, a refused stamp, or an unsupported declared type all
            // report `None`, leaving the column `Absent`); this loop only
            // decides which output index to fill.
            // Skip the whole walk when the projection carries no declared
            // column: partition_statistics runs several times per plan, and a
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
                    self.stripe_blocks,
                    blocks.plan_init_elapsed.clone(),
                );
                (VecDeque::new(), false, LogScanState::Planning(counts_fut))
            }
        };
        // ADR-2414 decision A2: the fast path pipelines its ranged opens at
        // its share of the GET permits, which `get_limiter_permits` reads off
        // the process-shared limiter the server wires into the fetcher.
        let prefetch_share = if fast_whole_segment && self.fetch.is_none() {
            fast_path_prefetch_share(self.fetcher.get_limiter_permits(), self.target_partitions)
        } else {
            1
        };

        Ok(Box::pin(LogScanStream {
            schema: Arc::clone(&self.schema),
            projection: Arc::clone(&self.projection),
            declared: Arc::clone(&self.declared),
            attr_keys: Arc::clone(&self.attr_keys),
            full_len: self.full_schema.fields().len(),
            ctx,
            erasure: Arc::clone(&self.erasure),
            columnar_eligible: self.columnar_eligible,
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
            prefetch_pool: Arc::clone(&self.prefetch_pool),
            current_by_chunk: false,
            retrying: false,
            segments: Arc::clone(&self.segments),
            work,
            resident: reservation.new_empty(),
            reservation,
            held: 0,
            emitted: 0,
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
            state,
            decoded: None,
            fetch: self.fetch,
            rows_emitted: 0,
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
    /// The partition count [`owned_work`] deals over: `target_partitions`,
    /// capped by the surviving block count (ADR-0102).
    /// [`compute_plan_counts`] counts each segment's owners against this same
    /// figure.
    partitions: usize,
    /// Relevant segments the plan phase read whole instead of counting from the
    /// skip index (#761). A segment planned from footer alone or from the skip
    /// index carries its plan footer forward (`SegPlan::footer` is `Some`); the
    /// whole-object fallback carries none, so this is the count of relevant
    /// segments with no footer, published as the `plan_full_reads` metric (see
    /// [`BlockMetrics::plan_full_reads`] for the two causes).
    full_reads: usize,
}

/// What the plan phase learned about a segment with at least one surviving
/// block (ADR-2414 decision A1).
struct PlannedBlocks {
    /// Whole-object block indices surviving this query's pruning, ascending:
    /// the set [`owned_work`] deals, whole row group by whole row group, into
    /// each partition's share, and the list every per-partition open's own
    /// pruning must reproduce.
    indices: Arc<Vec<usize>>,
    /// `indices` grouped into whole row groups ([`row_groups`]), computed
    /// here so the deal does not need the directories, which the segment's
    /// last owner may already have released.
    groups: Vec<Vec<usize>>,
    /// This segment's directories, one handle per owning partition.
    carried: CarriedDirsSlot,
}

impl PlannedBlocks {
    /// A segment's plan with no owners counted yet: until
    /// [`CarriedDirsSlot::set_owners`] runs, the directories stay held for as
    /// long as the plan counts live.
    fn new(
        indices: Vec<usize>,
        dirs: Arc<SegmentDirectories>,
        reservation: ravel_memory::Reservation,
    ) -> Self {
        let groups = row_groups(&indices, dirs.page_dir());
        PlannedBlocks {
            indices: Arc::new(indices),
            groups,
            carried: CarriedDirsSlot::new(CarriedDirs {
                dirs,
                _reservation: reservation,
            }),
        }
    }
}

/// A segment's directories, decoded once by the plan phase
/// ([`LogSegmentFetcher::plan_segment`]) and carried to every per-partition
/// subset open through [`OwnedSeg`] so the open reuses them via
/// `RlogReader::from_decoded` instead of decoding them again, together with
/// the fetch memory budget's reservation for them
/// ([`LogSegmentFetcher::reserve_carried_directories`]). Both are released
/// when the last handle drops.
struct CarriedDirs {
    dirs: Arc<SegmentDirectories>,
    _reservation: ravel_memory::Reservation,
}

/// Hands one [`CarriedDirs`] handle to each partition that owns a row group of
/// the segment, and keeps its own until the last of them has taken one. Each
/// partition drops its handle when it finishes the segment, so the reservation
/// is released once every owner has finished it rather than when the
/// statement ends.
struct CarriedDirsSlot {
    state: std::sync::Mutex<CarriedDirsState>,
}

struct CarriedDirsState {
    held: Option<Arc<CarriedDirs>>,
    /// Owners that have not taken their handle yet. `usize::MAX` until the
    /// deal counts them.
    takers_left: usize,
}

impl CarriedDirsSlot {
    fn new(carried: CarriedDirs) -> Self {
        CarriedDirsSlot {
            state: std::sync::Mutex::new(CarriedDirsState {
                held: Some(Arc::new(carried)),
                takers_left: usize::MAX,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CarriedDirsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many partitions own a row group of this segment.
    fn set_owners(&self, owners: usize) {
        let mut state = self.lock();
        state.takers_left = owners;
        if owners == 0 {
            state.held = None;
        }
    }

    /// One owner's handle. The slot lets its own go once every owner has
    /// taken one.
    fn take(&self) -> Option<Arc<CarriedDirs>> {
        let mut state = self.lock();
        let handle = state.held.clone();
        state.takers_left = state.takers_left.saturating_sub(1);
        if state.takers_left == 0 {
            state.held = None;
        }
        handle
    }
}

/// How many partitions [`owned_work`] gives a row group of a segment with
/// `groups` row groups when it deals over `partitions`: the segment's groups
/// are consecutive in the round-robin order, so they land on `min(groups,
/// partitions)` distinct partitions. Without striping one partition owns the
/// whole segment.
fn segment_owners(groups: usize, partitions: usize, stripe_blocks: bool) -> usize {
    if stripe_blocks {
        groups.min(partitions)
    } else {
        1
    }
}

/// One relevant segment's contribution to [`PlanCounts`].
struct SegPlan {
    /// The surviving blocks and carried directories, or `None` when no block
    /// of this segment survives: no partition opens such a segment, so it
    /// keeps no directories resident and reserves nothing.
    planned: Option<PlannedBlocks>,
    /// The whole-segment prune stats ([`BlockMetrics::record_segment_totals`]
    /// consumes `blocks_total` and the postings drop). `blocks_scanned`/`pages`
    /// are zero here -- planning decodes nothing.
    stats: ScanStats,
    /// The footer the plan fast path read for this segment (#693 part 3,
    /// deliverable 2), or `None` when the plan slow branch opened the scan
    /// instead. Carried to each per-partition subset open through [`OwnedSeg`] so
    /// the open reuses it and skips its own suffix probe.
    footer: Option<LogFooter>,
    /// The whole-object bytes the plan fallback branch already fetched for
    /// this segment (issue #835), when that branch resolved the entire
    /// object. Carried to the subset open through [`OwnedSeg`] so it does not
    /// pay a second wire GET for bytes the plan phase already holds. `None`
    /// on the fast/skip-decidable footer branches (no block read), on the
    /// fallback's ranged crossover (not the whole object), and when
    /// [`compute_plan_counts`]'s concurrency bound dropped an already-fetched
    /// whole object because `plan_concurrency` other segments' bytes were
    /// retained first -- that segment's subset open re-fetches instead.
    whole_object: Option<CarriedWholeObject>,
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
    stripe_blocks: bool,
    plan_init_elapsed: Time,
) -> CountsFuture {
    Box::pin(async move {
        let counts = cell
            .get_or_try_init(|| async {
                // Only the initializing partition runs this closure, so the
                // metric it records is the barrier's cost counted once.
                let started = Instant::now();
                let counts =
                    compute_plan_counts(&ctx, &segments, plan_concurrency, stripe_blocks).await;
                plan_init_elapsed.add_elapsed(started);
                counts
            })
            .await?;
        Ok(Arc::clone(counts))
    })
}

/// One segment's [`LogSegmentFetcher::plan_segment`] result: the surviving
/// whole-object block indices and this segment's decoded directories
/// (ADR-2414 decision A1), stats, a carried footer (skip-decidable
/// branches), and a carried whole-object body (the fallback branch, issue
/// #835). `None` when the segment was pruned by ts bounds alone and never
/// reached the fetcher.
type PlanSegmentResult = Option<(
    Vec<usize>,
    Arc<SegmentDirectories>,
    ScanStats,
    Option<LogFooter>,
    Option<CarriedWholeObject>,
)>;

/// Prune every segment once (no block decode) to build the shared block plan.
///
/// The prunes run `plan_concurrency` at a time (`buffer_unordered`, consumed
/// as each completes rather than after the whole pass), and results are
/// written into a position-indexed `Vec` so `segs` keeps snapshot order and
/// [`owned_work`] can still index it by segment position even though
/// completion order does not match it. Every partition awaits this whole pass
/// before it drains anything, so the plan phase sits alone on the query's
/// critical path: run serially it costs one object-store round trip per
/// segment in sequence (issue #691 measured about 20 minutes per statement on
/// 8424 objects, one GET in flight for the whole time). Concurrency here
/// changes only how many of those reads are in flight, never their count
/// (still one plan read sequence per segment) or their semantics (the first
/// error aborts the plan, and `get_or_try_init` does not cache it). Which
/// error that is became scheduling-dependent when the pass moved to
/// completion order: a query whose segments fail differently can surface
/// either one. The
/// fetcher's own in-flight GET semaphore remains the global bound, so an
/// oversized `plan_concurrency` is safe.
///
/// `plan_concurrency` here is `self.target_partitions`, the query's SQL
/// partition count -- not `--store-get-concurrency`'s object-store GET
/// limiter (ADR-1195), which bounds in-flight wire requests process-wide
/// and, since ADR-1195 T2, is a separate knob from this one.
///
/// `plan_concurrency` also bounds how many carried whole objects (issue #835)
/// this pass retains at once (issue #835 follow-up): the first
/// `plan_concurrency` segments to COMPLETE with a carried whole object (not
/// the first `plan_concurrency` by segment position -- completion order under
/// `buffer_unordered` is scheduling-dependent) keep their bytes; every later
/// one has its `whole_object` forced to `None` before it is stored, so its
/// subset open pays a real re-fetch in the scan phase instead of holding
/// bytes the rest of the pass has not yet caught up to consuming. See this
/// module's doc, "Whole-object fallback carry, and its memory bound".
// Issue #693 part 3: the predicate-free full-window fast path in `execute`
// skips this whole pass; see `owned_work` and `plan_segment_fast`.
async fn compute_plan_counts(
    ctx: &PartitionCtx,
    segments: &[SegmentRef],
    plan_concurrency: usize,
    stripe_blocks: bool,
) -> DFResult<Arc<PlanCounts>> {
    // Not-yet-polled futures, one per segment tagged with its snapshot
    // position, so `buffer_unordered` decides how many run at once and the
    // consumer loop below can still store each result at its original index.
    // Built with a loop rather than a `map` closure: a closure returning a
    // future that borrows its argument cannot satisfy the higher-ranked bound
    // this `Send` boxed future needs.
    let mut prunes = Vec::with_capacity(segments.len());
    for (idx, seg) in segments.iter().enumerate() {
        // Refuse an unreadable version BEFORE the plan probe, not after. This
        // is a second entry point into the fetch layer alongside the three
        // route choices below, and `plan_segment` issues its footer probe
        // before the open phase can reject the version -- so without this the
        // ordering guarantee holds on the scan path and quietly fails here.
        refuse_unreadable_version(seg)?;
        let prune = ctx.fetcher.plan_segment(
            seg,
            ctx.tenant_hash,
            &ctx.query,
            ctx.phase_accounting.plan(),
        );
        prunes.push(async move { (idx, prune.await) });
    }
    let budget = plan_concurrency.max(1);
    let mut stream = futures::stream::iter(prunes).buffer_unordered(budget);
    let mut segs: Vec<Option<SegPlan>> = std::iter::repeat_with(|| None)
        .take(segments.len())
        .collect();
    let mut total_blocks = 0usize;
    let mut full_reads = 0usize;
    // Only the RETAINED carried bytes -- the first `budget` segments to
    // complete with a whole object -- are summed here, so this is the true
    // peak `compute_plan_counts` holds at once, not the whole corpus. A
    // dropped whole object never reaches this sum: its `whole_object` is
    // `None` before `SegPlan` is built below.
    let mut carried_bytes = 0u64;
    let mut carried_seen = 0usize;
    while let Some((idx, entry)) = stream.next().await {
        let entry: PlanSegmentResult = entry.map_err(SqlError::from)?;
        if let Some((indices, dirs, stats, footer, whole_object)) = entry {
            let survivors = indices.len();
            total_blocks += survivors;
            // A relevant segment planned from the skip index carries its
            // footer forward; the whole-object fallback (#761) carries none.
            if footer.is_none() {
                full_reads += 1;
            }
            let whole_object = match whole_object {
                // `survivors > 0` mirrors the condition `owned_work` uses to
                // decide whether any partition ever opens this segment: a
                // zero-survivor segment is dropped there regardless, so
                // spending the budget on its carry here would retain bytes
                // no partition ever reuses and starve a segment that would
                // have.
                Some(w) if survivors > 0 && carried_seen < budget => {
                    carried_seen += 1;
                    carried_bytes += w.byte_len();
                    Some(w)
                }
                // Either no whole object to begin with, no surviving block to
                // spend it on, or one arrived after the budget was already
                // spent by earlier completions: drop it before it is ever
                // stored, so it is never retained and never charged as
                // reused.
                _ => None,
            };
            // Only a segment some partition will open keeps its directories,
            // and only then are they reserved: `owned_work` skips a
            // zero-survivor segment, so carrying them would hold decoded
            // bytes nothing reuses.
            let planned = if survivors > 0 {
                let reservation = ctx
                    .fetcher
                    .reserve_carried_directories(&dirs)
                    .map_err(SqlError::from)?;
                Some(PlannedBlocks::new(indices, dirs, reservation))
            } else {
                None
            };
            segs[idx] = Some(SegPlan {
                planned,
                stats,
                footer,
                whole_object,
            });
        }
    }
    // A single call with the final sum is only correct because `carried_bytes`
    // is monotonically non-decreasing across this loop: nothing here ever
    // releases a retained buffer mid-pass, so the last value is also the
    // peak. If a future change lets retention shrink before this function
    // returns (an eviction, an early release), this call must move inside
    // the loop so `observe_intermediate_bytes` sees every local maximum, not
    // just the end state.
    ctx.phase_accounting
        .plan()
        .observe_intermediate_bytes(carried_bytes);
    // Cap the stride by the real block count (ADR-0102): with fewer blocks
    // than partitions the extra partitions get empty work.
    let partitions = plan_concurrency.max(1).min(total_blocks.max(1));
    for planned in segs.iter().flatten().filter_map(|s| s.planned.as_ref()) {
        planned.carried.set_owners(segment_owners(
            planned.groups.len(),
            partitions,
            stripe_blocks,
        ));
    }
    Ok(Arc::new(PlanCounts {
        segs,
        partitions,
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
    /// The segment's whole surviving-block list this query's plan produced
    /// (ascending whole-object block indices), of which `indices` is this
    /// partition's share. A row-ref addresses a block by its position in this
    /// list ([`RowRefRange`]), not by its whole-object index, so the scan
    /// resolves each block's row-ref through it. `None` on the whole-segment
    /// fast path, where every block survives and a block's whole-object index
    /// already is its surviving-block position.
    survivors: Option<Arc<Vec<usize>>>,
    /// The plan-phase footer for this segment (#693 part 3, deliverable 2),
    /// carried to the subset open so it skips its own suffix probe. `None` on the
    /// whole-segment fast path (no plan phase) and when the plan slow branch ran.
    footer: Option<LogFooter>,
    /// The plan fallback's whole-object bytes for this segment (issue #835),
    /// carried to the subset open so it does not pay a second wire GET.
    /// `None` whenever [`SegPlan::whole_object`] was `None`, and always
    /// `None` on the whole-segment fast path (no plan phase).
    whole_object: Option<CarriedWholeObject>,
    /// This partition's handle on the segment's directories, decoded once by
    /// the plan phase (ADR-2414 decision A1), carried to the subset open so it
    /// reuses them instead of decoding them again. `None` on the whole-segment
    /// fast path (no plan phase ever runs there).
    dirs: Option<Arc<CarriedDirs>>,
}

/// Groups `indices` (a segment's surviving whole-object block indices,
/// ascending) into whole row groups by `page_dir`'s own group boundaries, so
/// [`owned_work`] can deal a row group's blocks to one partition as a unit
/// (ADR-2414 decision A1) instead of splitting it across partitions. Indices
/// are ascending and row groups are contiguous ranges of block numbers, so
/// consecutive indices mapping to the same group's `first_block` are always
/// one unbroken run. A block [`PageDir::locate_block`] cannot place keeps its
/// own singleton group rather than joining one it does not belong to.
fn row_groups(indices: &[usize], page_dir: &PageDir) -> Vec<Vec<usize>> {
    let mut groups: Vec<(Option<u32>, Vec<usize>)> = Vec::new();
    for &idx in indices {
        let block = u32::try_from(idx).unwrap_or(u32::MAX);
        let first_block = page_dir.locate_block(block).map(|(g, _)| g.first_block);
        match groups.last_mut() {
            Some((cur, items)) if cur.is_some() && *cur == first_block => items.push(idx),
            _ => groups.push((first_block, vec![idx])),
        }
    }
    groups.into_iter().map(|(_, items)| items).collect()
}

/// This partition's share of the block assignment, in one of two modes
/// (ADR-0102, amended by #693), both with `n = target_partitions.max(1).
/// min(total.max(1))`.
///
/// - **`stripe_blocks` true** (a read cache is wired): the flattened
///   row-group assignment (ADR-2414 decision A1). Each segment's surviving
///   blocks are grouped into whole row groups by [`row_groups`], and group
///   `i` in the segment-then-group order over all surviving row groups goes
///   to partition `i % n` -- every block of a row group to the same
///   partition, never split. A row group is the unit the format stores
///   column-major, and a column chunk's dictionary page is shared by every
///   block of the group, so splitting a group across partitions makes each of
///   them decode that dictionary again. Dealing round-robin over groups keeps
///   the assignment deterministic, and the number of groups per partition
///   differs by at most one; block counts are as even as the groups are, and
///   a segment's last group may be short. A segment with fewer groups than
///   partitions leaves some of them without a share of it.
/// - **`stripe_blocks` false** (un-cached): the segment-granular assignment.
///   Counting only segments with a surviving block, in snapshot order, segment
///   `j` goes to partition `j % n` and that partition drains all of the
///   segment's surviving block indices. Each segment is opened
///   by exactly one partition, so with nothing to coalesce re-opens the scan
///   still costs one read per segment rather than one per partition per segment.
///
/// Returns, per owned segment, the surviving whole-object block indices this
/// partition drains, in ascending order.
fn owned_work(
    counts: &PlanCounts,
    segments: &[SegmentRef],
    partition: usize,
    n: usize,
    stripe_blocks: bool,
) -> VecDeque<OwnedSeg> {
    let mut work = VecDeque::new();
    if stripe_blocks {
        let mut global_group = 0usize;
        for (seg_idx, plan) in counts.segs.iter().enumerate() {
            let Some(plan) = plan else { continue };
            let Some(planned) = &plan.planned else {
                continue;
            };
            let mut indices = Vec::new();
            for group in &planned.groups {
                if global_group % n == partition {
                    indices.extend_from_slice(group);
                }
                global_group += 1;
            }
            if !indices.is_empty() {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    ordinal: seg_idx,
                    indices,
                    survivors: Some(Arc::clone(&planned.indices)),
                    footer: plan.footer.clone(),
                    whole_object: plan.whole_object.clone(),
                    dirs: planned.carried.take(),
                });
            }
        }
    } else {
        let mut seg_ordinal = 0usize;
        for (seg_idx, plan) in counts.segs.iter().enumerate() {
            let Some(plan) = plan else { continue };
            let Some(planned) = &plan.planned else {
                continue;
            };
            if seg_ordinal % n == partition {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    ordinal: seg_idx,
                    indices: planned.indices.to_vec(),
                    survivors: Some(Arc::clone(&planned.indices)),
                    footer: plan.footer.clone(),
                    whole_object: plan.whole_object.clone(),
                    dirs: planned.carried.take(),
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
                survivors: None,
                footer: None,
                whole_object: None,
                dirs: None,
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
    phase_accounting: PhaseAccounting,
}

impl PartitionCtx {
    /// Whether the whole-segment fast path should open `seg` by column chunk
    /// rather than with one whole-object GET (issue #862).
    ///
    /// The fast path's own conjuncts ([`LogsScanExec::whole_segment_fast_path`])
    /// prove every block of `seg` survives. That does not make the whole-object
    /// read optimal: every block surviving says nothing about how many COLUMNS
    /// the projection wants, and the ranged entry point ([`open_segment_ranged`])
    /// already fetches one coalesced range per surviving `(row group, projected
    /// column)` from the very same [`ColumnSelection`] (ADR-0699 decision 5), so
    /// a narrow projection can leave most of the object unread.
    ///
    /// The arbiter is the fetch layer's request-cost model, not a threshold
    /// invented here: the ranged path pays only when the bytes the projection
    /// skips outweigh the round trips the protocol adds. That keeps a wide
    /// projection (`SELECT *`, or any reference to the merged `attrs` map) on
    /// the unchanged whole-object read, where it belongs -- the ranged path
    /// would fetch the same bytes and pay a probe on top.
    ///
    /// The route is unconditional in the format version (ADR-0892 decision 3):
    /// every readable RLOG object stores pages column-major with a PAGE_DIR, so
    /// the `ColumnSelection` always selects which chunks are FETCHED. An object
    /// whose declared version this build cannot read never reaches here at all;
    /// [`refuse_unreadable_version`] turns it away before any route is chosen.
    fn open_by_column_chunk(&self, seg: &SegmentRef) -> bool {
        self.fetcher
            .ranged_projection_pays(seg.object_size, self.projected_fraction)
    }

    /// Record one whole-segment fast-path open on the route
    /// [`open_by_column_chunk`](Self::open_by_column_chunk) just chose, on this
    /// query's accounting handle (ADR-0904 decision 5).
    ///
    /// The plan metrics carry the same split per partition
    /// ([`BlockMetrics::record_fast_path_route`]), but only for a statement whose
    /// `EXPLAIN ANALYZE` output someone reads. The accounting handle is the
    /// per-query figure a caller can assert on, which is what makes the
    /// request-cost knob's effect on the route provable rather than inferred
    /// from the configured value.
    ///
    /// Called once per segment, beside the metric and under the same rule: an
    /// `attrs_raw` fallback re-opens the same segment the same way and does not
    /// re-count, so the two counters sum to the fast-path segment count.
    fn record_open_shape(&self, by_column_chunk: bool) {
        if by_column_chunk {
            self.phase_accounting.scan().add_logs_ranged_opens(1);
        } else {
            self.phase_accounting.scan().add_logs_whole_object_opens(1);
        }
    }

    /// Record this query's one touch of the data object the caller is about to
    /// open, for the whole-segment fast path (ADR-0996 decision 3, issue
    /// #1006).
    ///
    /// # Why this site is the fast path's single recorder
    ///
    /// The counter is the denominator of `range_amplification =
    /// data_GET_requests / data_objects_touched`, so it must count each data
    /// object once per query however many GETs are issued against it. That
    /// makes it a designated-recorder question rather than a
    /// record-where-you-fetch one: the striped path splits one segment's blocks
    /// across partitions, so a per-partition site would multiply the count by
    /// the partition count, the same way per-partition
    /// [`BlockMetrics::record_segment_totals`] would (ADR-0102's
    /// partition-0-at-planning rule).
    ///
    /// Here the owning partition is the recorder. [`owned_whole_segments`]
    /// assigns relevant segment `j` whole to partition `j % n`, so the caller
    /// below runs exactly once per relevant segment across the whole query, not
    /// once per partition. Both routes it guards fetch blocks bytes (whole
    /// object or coalesced column-chunk ranges), so the object is touched in the
    /// counter's sense and not merely probed; and both are reached only after
    /// [`LogsScanExec::whole_segment_fast_path`] proved the segment relevant on
    /// the same ts bounds the fetch re-checks, so neither can decline the fetch
    /// underneath a recorded touch. An `attrs_raw` fallback re-opens the same
    /// segment without coming back through here, which is what keeps several
    /// opens of one object at one touch, exactly as it does for
    /// [`record_open_shape`](Self::record_open_shape).
    ///
    /// # Route exclusivity: no object can be recorded twice
    ///
    /// The planned route records the same counter in the fetch layer, at
    /// `ravel_query::LogSegmentFetcher::plan_segment`, and no statement reaches
    /// both recorders. [`LogsScanExec::execute`] decides from
    /// [`LogsScanExec::whole_segment_fast_path`]: on `Ok` the partition drains
    /// whole segments through the caller of this method and never builds a
    /// [`plan_counts_future`], and on `Err` it plans and never sets
    /// `fast_whole_segment`, so this method is unreachable. That decision reads
    /// only the resolved snapshot and the query, both fixed for the statement
    /// and neither dependent on the partition, so every partition of one
    /// statement takes the same route and exactly one of the two recorders can
    /// fire for a given object.
    fn record_data_object_touched(&self) {
        self.phase_accounting.scan().add_data_objects_touched(1);
    }
}

/// Refuse a segment whose declared RLOG format version this build cannot read,
/// BEFORE any request is issued for it (ADR-0892 decision 4).
///
/// Every open path below calls this first. Without it an unreadable object is
/// found out by `ravel_logseg::footer::open_from_suffix`, which runs on bytes
/// the ranged path has already paid a suffix probe (and possibly a footer-range
/// GET) to fetch, or that the whole-object path has fetched entirely. The
/// snapshot already carries the version in
/// [`SegmentRef::segment_format_version`], so the round trips buy nothing.
///
/// This is a pre-filter, not the gate: the object's own trailer is still
/// checked on open, so a catalog entry that disagrees with the bytes is caught
/// there. The error it produces is the same shape that check produces, so a
/// caller cannot tell which one refused the object.
fn refuse_unreadable_version(seg: &SegmentRef) -> Result<(), SqlError> {
    // The catalog carries the version as u32 and an RLOG trailer is u16, so a
    // value that does not fit is one no build could read; it is reported at the
    // u16 ceiling rather than silently truncated into the supported window.
    let declared = u16::try_from(seg.segment_format_version).unwrap_or(u16::MAX);
    if ravel_logseg::footer::SUPPORTED_VERSIONS.contains(declared) {
        return Ok(());
    }
    Err(SqlError::LogFetch(LogFetchError::Corrupt {
        key: seg.data_object_key.clone(),
        source: LogSegError::UnsupportedVersion(declared),
    }))
}

type OpenFuture = Pin<Box<dyn Future<Output = DFResult<Option<LogSegmentScan>>> + Send>>;

/// Fetch one segment's bytes and open its pruned, column-projected scan
/// restricted to the whole-object block indices in `indices` (ADR-2414
/// decision A1: `owned_work` deals whole row groups, so these are raw block
/// indices, not ordinal survivor positions).
/// `Ok(None)` means the catalog summary proved the segment irrelevant, with no
/// GET issued -- which cannot happen for a segment that was already counted
/// with survivors, but is handled as end-of-segment rather than panicking.
///
/// `survivors` is the segment's whole surviving-block list the plan phase
/// produced ([`OwnedSeg::survivors`]), of which `indices` is this partition's
/// share. Every caller of this function reaches it only through `owned_work`
/// (never `owned_whole_segments`, which never calls this function), so
/// `survivors` is architecturally always `Some`; `None` is refused with a
/// typed error rather than silently scanning without the cross-check (issue
/// #2417), since a caller that could reach this function without it is a bug
/// in this module, not a runtime condition to tolerate.
fn open_segment_subset(
    ctx: Arc<PartitionCtx>,
    seg: SegmentRef,
    indices: Vec<usize>,
    survivors: Option<Arc<Vec<usize>>>,
    footer: Option<LogFooter>,
    whole_object: Option<CarriedWholeObject>,
    dirs: Option<Arc<SegmentDirectories>>,
) -> OpenFuture {
    Box::pin(async move {
        refuse_unreadable_version(&seg)?;
        let survivors = survivors.ok_or_else(|| {
            SqlError::Internal("striped segment open with no plan-phase survivor list".into())
        })?;
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant_subset_raw(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                &indices,
                &survivors,
                footer.as_ref(),
                whole_object,
                dirs.as_ref(),
                ctx.phase_accounting.scan(),
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
        refuse_unreadable_version(&seg)?;
        let scan = ctx
            .fetcher
            .scan_whole_accounted_with_tenant(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                ctx.phase_accounting.scan(),
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
        refuse_unreadable_version(&seg)?;
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                ctx.phase_accounting.scan(),
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

/// How many of one partition's ranged fast-path segment opens may be held at
/// once, counting the segment being drained (ADR-2414 decision A2): the
/// partition's share of the process's GET permits, `store_get_concurrency /
/// partitions`, and at least 2 so the next segment's round trips always
/// overlap the current one's decode.
fn fast_path_prefetch_share(store_get_concurrency: usize, partitions: usize) -> usize {
    (store_get_concurrency / partitions.max(1)).max(2)
}

/// Whether `e` is the fetcher's memory budget refusing a reservation
/// ([`LogFetchError::FetchMemoryExhausted`]), as an open surfaces it.
fn is_fetch_memory_refusal(e: &DataFusionError) -> bool {
    matches!(
        e,
        DataFusionError::External(inner)
            if matches!(
                inner.downcast_ref::<SqlError>(),
                Some(SqlError::LogFetch(LogFetchError::FetchMemoryExhausted { .. }))
            )
    )
}

/// A ranged fast-path open issued ahead of its partition's turn to drain
/// its segment (ADR-2414 decision A2), held in that partition's
/// [`PrefetchSlot`]. It is the same [`open_segment_fast`] future the
/// sequential open would build, polled from
/// [`LogScanStream::drive_prefetches`] until it resolves; the resolved open
/// is kept, error included, until the segment's turn comes, so an error is
/// reported for the segment that produced it and only once the segments
/// before it have been emitted. Its fetch reservations are owned by the
/// future or by the bytes it resolved to, so dropping it, from whichever
/// partition's task, releases them at once.
struct Prefetch {
    /// The owned work item the open was issued for, kept whole so a
    /// revocation can hand it back to its owner's work.
    owned: OwnedSeg,
    /// When the open was issued: the start of this segment's `open_elapsed`.
    issued: Instant,
    open: PrefetchOpen,
}

enum PrefetchOpen {
    InFlight(OpenFuture),
    Ready {
        opened: Box<DFResult<Option<LogSegmentScan>>>,
        at: Instant,
    },
}

/// One partition's share of a [`PrefetchPool`].
#[derive(Default)]
struct PrefetchSlot {
    /// The issued opens of the owner's segments after its current one, in
    /// owned order.
    prefetched: VecDeque<Prefetch>,
    /// Segments whose prefetch a revocation dropped, in owned order, for the
    /// owner to move back to the front of its work.
    handed_back: VecDeque<OwnedSeg>,
}

/// One statement's prefetched fast-path opens, a slot per partition
/// (ADR-2414 decision A2). The fetch memory budget is process-wide, so a
/// partition whose current open is refused may be refused for bytes another
/// partition's prefetches hold; [`Self::revoke_all`] lets it drop them all
/// synchronously, without waiting for any other stream to be polled.
///
/// A slot's lock is never held across an await, and no code path holds two
/// slot locks at once.
struct PrefetchPool {
    /// Set by the first revocation: no partition of the statement issues
    /// another prefetch.
    off: AtomicBool,
    slots: Vec<std::sync::Mutex<PrefetchSlot>>,
    /// Whether each partition's stream has been built, so a second `execute`
    /// of one partition cannot share a slot with the first.
    registered: Vec<AtomicBool>,
}

impl PrefetchPool {
    fn new(partitions: usize) -> Self {
        PrefetchPool {
            off: AtomicBool::new(false),
            slots: (0..partitions)
                .map(|_| std::sync::Mutex::new(PrefetchSlot::default()))
                .collect(),
            registered: (0..partitions).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    fn is_off(&self) -> bool {
        self.off.load(Ordering::SeqCst)
    }

    /// Claims `partition`'s slot for the stream `execute` is building.
    fn register(&self, partition: usize) -> DFResult<()> {
        let flag = self.registered.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "logs scan partition {partition} has no prefetch slot"
            ))
        })?;
        if flag.swap(true, Ordering::SeqCst) {
            return Err(DataFusionError::Internal(format!(
                "logs scan partition {partition} executed twice"
            )));
        }
        Ok(())
    }

    fn slot(&self, partition: usize) -> DFResult<std::sync::MutexGuard<'_, PrefetchSlot>> {
        self.slots
            .get(partition)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "logs scan partition {partition} has no prefetch slot"
                ))
            })?
            .lock()
            .map_err(|_| {
                DataFusionError::Internal(format!(
                    "logs scan partition {partition}'s prefetch slot lock is poisoned"
                ))
            })
    }

    /// Turns the statement's pipeline off, then drops every unconsumed
    /// prefetch of every partition, one slot at a time in index order:
    /// each is popped from the back and its segment pushed on the front of
    /// that slot's `handed_back`, so owned order is kept. Dropping a
    /// prefetch releases its fetch reservations before this returns.
    /// Returns how many were dropped.
    fn revoke_all(&self) -> DFResult<usize> {
        self.off.store(true, Ordering::SeqCst);
        let mut dropped = 0;
        for partition in 0..self.slots.len() {
            let mut slot = self.slot(partition)?;
            while let Some(Prefetch { owned, open, .. }) = slot.prefetched.pop_back() {
                drop(open);
                slot.handed_back.push_front(owned);
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    /// Drops everything `partition`'s slot holds. Used on the stream's way
    /// out, so it clears a poisoned slot too rather than leaving its
    /// prefetches' bytes reserved.
    fn clear(&self, partition: usize) {
        if let Some(slot) = self.slots.get(partition) {
            let mut slot = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.prefetched.clear();
            slot.handed_back.clear();
        }
    }

    /// Gives `partition`'s slot back when its stream is dropped, so a later
    /// `execute` of the same plan instance (a parent that runs its input
    /// once per iteration) claims it again. Two live streams on one slot
    /// stay refused by [`Self::register`].
    fn unregister(&self, partition: usize) {
        if let Some(flag) = self.registered.get(partition) {
            flag.store(false, Ordering::SeqCst);
        }
    }
}

/// Which open [`LogScanStream::on_open_refused`] retries.
enum Reopen {
    /// The segment's fast-path open, on the route its first open took.
    Fast,
    /// The `attrs_raw` row-path reopen, skipping the `skip` blocks of this
    /// partition's list already emitted.
    Rows { skip: usize },
}

/// Consecutive `attrs_raw`-overflow fallbacks (see
/// [`LogScanStream::consecutive_fallbacks`]) tolerated within one segment
/// before the rest of this partition's block list is committed to the row
/// path in one reopen, rather than reopening again to retry columnar. `1`:
/// the first fallback still gets a columnar retry (the sparse case, where a
/// single overflowing block among clean ones is common and worth resuming
/// columnar for), but a second fallback immediately after it -- with no
/// clean columnar block in between -- escalates. This bounds a segment to at
/// most `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS + 1` reopens regardless of how
/// many of its blocks overflow.
const MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS: usize = 1;

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
    /// (ADR-0099 decision 2). Entered when the scan is statically eligible, and
    /// re-entered (issue #1769) once [`LogScanState::RowFallbackBlock`] has
    /// taken the one block that carried an `attrs_raw` overflow page, so only
    /// that block runs the row path instead of the rest of the segment.
    Columnar(Box<LogSegmentScan>),
    /// Draining one segment's surviving blocks through the row path
    /// ([`LogSegmentScan::next_block`]), rebuilding a [`LogRecord`] per row: the
    /// unchanged pre-ADR-0099 path, taken by a scan that is statically
    /// ineligible for the columnar fast path, OR handed the
    /// still-open scan by [`LogScanState::RowFallbackBlock`] once a segment
    /// has hit [`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS`] consecutive `attrs_raw`
    /// fallbacks, committing the rest of this partition's block list to the
    /// row path in one reopen instead of retrying columnar block by block.
    /// Never carries a `skip`: a fallback reopen needs one only while it is
    /// still resuming columnar, which is `RowFallbackBlock`'s job.
    Rows(Box<LogSegmentScan>),
    /// Re-opening the current segment to restart it on the row path after a
    /// block turned out to carry an `attrs_raw` overflow page. The re-opened
    /// scan is given the SAME block-index list this partition owns for the
    /// segment (ADR-0102), so `skip` is a position within that list: the number
    /// of this partition's blocks already fully emitted (columnar, or by an
    /// earlier `RowFallbackBlock`). Ready, it becomes
    /// [`LogScanState::RowFallbackBlock`], never `Rows`: this state exists only
    /// for the `attrs_raw` fallback.
    ReopenRows {
        fut: OpenFuture,
        skip: usize,
    },
    /// The re-opened row scan from [`LogScanState::ReopenRows`]: drain and
    /// discard `skip` blocks (already emitted columnar or by an earlier
    /// fallback, so re-decoding them here does not re-emit a row), then take
    /// exactly the next block -- the one that carried the `attrs_raw` overflow
    /// page -- through the row path (issue #1769). That block done, the same
    /// still-open scan is handed back to [`LogScanState::Columnar`] so the
    /// blocks after it keep decoding columnar instead of falling the rest of
    /// the segment to rows -- unless this is this segment's
    /// [`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS`]-th consecutive fallback with no
    /// clean columnar block in between, in which case it is handed to
    /// [`LogScanState::Rows`] instead, committing the rest of
    /// the block list to the row path with no further reopen. A block-index
    /// list this partition owns is never touched by another partition's
    /// fallback (ADR-0102), so this reopen cannot race one.
    RowFallbackBlock {
        scan: Box<LogSegmentScan>,
        skip: usize,
    },
    /// One columnar block's decode and Arrow build, running as a read-gate
    /// job that owns the scan (ADR-1702 decision 7). Resolved, the scan goes
    /// back to [`LogScanState::Columnar`] with the block's outcome in
    /// [`LogScanStream::decoded`].
    DecodingColumnar {
        job: ColumnarJobFuture,
        /// When the job was submitted, so `decode_build_elapsed` includes the
        /// wait for a gate permit.
        started: Instant,
    },
    /// One row-path block decoding on the read gate through
    /// [`LogSegmentScan::next_block_on_gate`]. Resolved, the scan goes back
    /// to the state `resume` names with the block in
    /// [`LogScanStream::decoded`].
    DecodingRows {
        job: RowJobFuture,
        resume: RowResume,
        started: Instant,
    },
    Done,
}

type ColumnarJobFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (Box<LogSegmentScan>, MemoryReservation, ColumnarStep),
                    CpuGateError,
                >,
            > + Send,
    >,
>;

type RowJobFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    Box<LogSegmentScan>,
                    Option<usize>,
                    Result<Option<Vec<LogRecord>>, LogFetchError>,
                ),
            > + Send,
    >,
>;

/// The row-path state a [`LogScanState::DecodingRows`] job returns its scan
/// to.
#[derive(Clone, Copy)]
enum RowResume {
    Rows,
    FallbackBlock { skip: usize },
}

/// A block a gate job decoded, taken by the state the job resumed.
enum Decoded {
    Columnar(ColumnarStep),
    Rows {
        /// The block's whole-object index, read before the decode.
        block: Option<usize>,
        next: Result<Option<Vec<LogRecord>>, LogFetchError>,
    },
}

/// What [`LogScanStream::next_row_block`] did.
enum RowNext {
    /// The block's whole-object index, read before the decode, and the block.
    Ready(Option<usize>, Result<Option<Vec<LogRecord>>, LogFetchError>),
    /// The decode went to the read gate; the state is now
    /// [`LogScanState::DecodingRows`].
    Submitted,
    Failed(DataFusionError),
}

/// One columnar block's outcome, owned, so the view (which borrows the scan)
/// is dropped before the stream's state or reservation is touched.
enum ColumnarStep {
    Exhausted(ScanStats),
    /// A block carrying an `attrs_raw` overflow page (or, only defensively,
    /// an unexpected pending erasure): fall the rest of this segment back to
    /// the row path.
    Fallback,
    /// A clean block's built batches (possibly empty for a block with no
    /// surviving row). The decoded block itself, which stays resident behind
    /// the reader while those batches drain, is charged by
    /// [`ColumnarBlockJob::run`].
    Held {
        batches: Vec<RecordBatch>,
    },
    /// A decode or build error, carried out of the view's borrow.
    Failed(DataFusionError),
}

/// Everything one columnar block's decode and build read besides the scan,
/// owned so the two can run together as one read-gate job.
struct ColumnarBlockJob {
    schema: SchemaRef,
    projection: Arc<Vec<usize>>,
    declared: Arc<Vec<DeclaredColumn>>,
    attr_keys: Arc<Vec<String>>,
    full_len: usize,
    /// The segment field of the block's row refs.
    segment: usize,
    /// The block's surviving-block position for its row refs, from
    /// [`block_index`]; consulted only when a block is decoded.
    block: DFResult<Option<usize>>,
    /// Panic inside the gate job instead of running it.
    #[cfg(test)]
    panic_for_test: bool,
}

impl ColumnarBlockJob {
    /// Decodes `scan`'s next block and builds its batches.
    ///
    /// `resident` is the charge for the block the reader held before this
    /// call. The decode frees that block, so the charge is resized to the
    /// block it decoded as soon as the decode returns, and freed when it
    /// decoded none.
    fn run(self, scan: &mut LogSegmentScan, resident: &MemoryReservation) -> ColumnarStep {
        match scan.next_block_columnar() {
            Ok(ColumnarBlockOutcome::Exhausted) => {
                resident.free();
                ColumnarStep::Exhausted(scan.stats())
            }
            // The fast path is only entered with no erasure, so this is
            // unreachable in practice; fall back rather than risk serving an
            // erased record columnar.
            Ok(ColumnarBlockOutcome::ErasurePending) => {
                resident.free();
                ColumnarStep::Fallback
            }
            Ok(ColumnarBlockOutcome::Block(view)) => {
                if let Err(e) = resident.try_resize(view.decoded_bytes()) {
                    return ColumnarStep::Failed(e);
                }
                if view.has_attrs_raw_page() {
                    return ColumnarStep::Fallback;
                }
                let built = self.block.and_then(|block| {
                    build_columnar_batches(
                        &view,
                        &self.schema,
                        &self.projection,
                        &self.declared,
                        &self.attr_keys,
                        self.full_len,
                        block.map(|block| RowRefRange {
                            segment: self.segment,
                            block,
                            first_row: 0,
                        }),
                    )
                });
                match built {
                    Ok(batches) => ColumnarStep::Held { batches },
                    Err(e) => ColumnarStep::Failed(e),
                }
            }
            Err(e) => {
                resident.free();
                ColumnarStep::Failed(SqlError::from(e).into())
            }
        }
    }
}

/// `job` on `gate` as one [`ReadSite::LogBlock`] job: the block's decode and
/// its Arrow build both leave the runtime worker, and the scan comes back
/// with the outcome. `resident`, the charge for the block the scan's reader
/// holds, goes with it, so a job waiting for a permit keeps that block
/// charged until its decode frees it, and a job that never hands the scan
/// back drops the charge with the scan.
fn columnar_block_on_gate(
    gate: Arc<ReadGate>,
    size: JobSize,
    mut scan: Box<LogSegmentScan>,
    job: ColumnarBlockJob,
    resident: MemoryReservation,
) -> ColumnarJobFuture {
    Box::pin(async move {
        gate.run(ReadSite::LogBlock, size, move || {
            #[cfg(test)]
            assert!(!job.panic_for_test, "injected columnar block job panic");
            let step = job.run(&mut scan, &resident);
            (scan, resident, step)
        })
        .await
    })
}

/// Object keys whose next columnar block job panics on the gate. Keyed by
/// object so a test arming it leaves every other test's scan alone.
#[cfg(test)]
static PANIC_NEXT_COLUMNAR_JOB: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Whether the next columnar block job over `key` panics, consuming the
/// arming.
#[cfg(test)]
fn take_columnar_job_panic(key: &str) -> bool {
    PANIC_NEXT_COLUMNAR_JOB
        .lock()
        .is_ok_and(|mut keys| match keys.iter().position(|k| k == key) {
            Some(i) => {
                keys.swap_remove(i);
                true
            }
            None => false,
        })
}

/// The row path's next block on the read gate, the scan returned with it.
fn row_block_on_gate(mut scan: Box<LogSegmentScan>) -> RowJobFuture {
    Box::pin(async move {
        let block = scan.next_block_index();
        let next = scan.next_block_on_gate().await;
        (scan, block, next)
    })
}

/// A columnar block job that failed on the gate, classified the way the
/// fetcher classifies its own gated block decodes: a panic is the decode's
/// failure, a job that never ran is transient.
fn log_block_gate_failed(key: &str, err: CpuGateError) -> DataFusionError {
    let fetch = match err {
        CpuGateError::Panicked => LogFetchError::Corrupt {
            key: key.to_string(),
            source: LogSegError::Corrupted(format!("read CPU gate: {err}")),
        },
        CpuGateError::Cancelled | CpuGateError::Closed => LogFetchError::Store {
            key: key.to_string(),
            source: StoreError::Transient(format!("read CPU gate: {err}")),
        },
    };
    SqlError::from(fetch).into()
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

/// The surviving-block index of a just-decoded block, or `None` when the scan
/// emits no row refs (ADR-0774).
///
/// `decoded_block` is the whole-object block index the scan itself named as the
/// next block to decode, read before the decode
/// ([`LogSegmentScan::next_block_index`]), not a position into a
/// separately-tracked owned-block list: resolving a row-ref from the scan's
/// own drain order, rather than from a cursor counted alongside it, means a
/// list that has drifted out of step with the scan's actual blocks cannot
/// silently stamp the wrong row-ref (issue #2417). `survivors` is the
/// segment's whole surviving-block list; a row-ref carries a block's position
/// in it, not its whole-object index.
///
/// The whole-segment fast path passes `None`: it has no plan-phase survivor
/// list, and late materialization resolves the row-ref as a position among the
/// blocks its own reopen keeps. The two agree only while the open has pruned
/// no block before this one, so the decoded block must equal `cursor`, the
/// block's position in this scan's drain. A block that follows one the open
/// pruned (its skip index disagreeing with the catalog's bounds) fails with a
/// typed `Corrupted` error naming `key` rather than stamping a row-ref that
/// would resolve to another block.
///
/// A free function rather than a method because the columnar drain calls it
/// while the block cursor holds a mutable borrow of the stream's state field.
fn block_index(
    row_refs: bool,
    survivors: Option<&[usize]>,
    decoded_block: Option<usize>,
    cursor: usize,
    key: &str,
) -> DFResult<Option<usize>> {
    if !row_refs {
        return Ok(None);
    }
    let owned = decoded_block.ok_or_else(|| {
        DataFusionError::Internal(
            "row-ref requested but the scan has not decoded a block yet".into(),
        )
    })?;
    let Some(survivors) = survivors else {
        if owned != cursor {
            return Err(SqlError::from(LogFetchError::Corrupt {
                key: key.to_string(),
                source: LogSegError::Corrupted(format!(
                    "segment survivor mismatch: whole-segment open decoded block {owned} \
                     at drain position {cursor}"
                )),
            })
            .into());
        }
        return Ok(Some(owned));
    };
    survivors.binary_search(&owned).map(Some).map_err(|_| {
        DataFusionError::Internal(format!(
            "owned block {owned} is not among the segment's surviving blocks"
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

impl LogScanStream {
    /// Publish one timeline point for the segment being drained: `name` at the
    /// current offset from the exec's origin, labelled with the segment's
    /// snapshot ordinal so a reader can line the partitions up on one clock.
    fn mark_segment(&self, name: &'static str) {
        self.mark_segment_at(self.current_seg_ordinal, name);
    }

    /// [`Self::mark_segment`] for the segment at snapshot ordinal `ordinal`,
    /// which a prefetched open reaches before it is the current segment.
    fn mark_segment_at(&self, ordinal: usize, name: &'static str) {
        if !self.segment_timing {
            return;
        }
        MetricBuilder::new(&self.metrics)
            .with_new_label("segment", ordinal.to_string())
            .subset_time(name, self.partition)
            .add_elapsed(self.origin);
    }

    /// Issue the ranged opens of this partition's next owned segments, in
    /// owned order, into its slot of the statement's [`PrefetchPool`], until
    /// [`Self::prefetch_share`] opens are held counting the current one
    /// (ADR-2414 decision A2). Only behind a ranged current segment, only
    /// while the next owned segment is itself a ranged open (the first
    /// whole-object segment stays in `work` and opens sequentially at its
    /// turn), and never once a revocation has turned the statement's
    /// pipeline off.
    fn top_up_prefetch(&mut self, cx: &mut Context<'_>) -> DFResult<()> {
        if !self.current_by_chunk || self.prefetch_pool.is_off() {
            return Ok(());
        }
        loop {
            let ranged = self
                .work
                .front()
                .is_some_and(|next| self.ctx.open_by_column_chunk(&next.seg));
            if !ranged {
                break;
            }
            let ordinal = {
                let mut slot = self.prefetch_pool.slot(self.partition)?;
                // Re-read under the lock: a revoker sets `off` before it
                // drains this slot, so nothing pushed here outlives a drain.
                if self.prefetch_pool.is_off() || slot.prefetched.len() + 1 >= self.prefetch_share {
                    break;
                }
                let Some(next) = self.work.pop_front() else {
                    break;
                };
                let ordinal = next.ordinal;
                let open = open_segment_fast(Arc::clone(&self.ctx), next.seg.clone(), true);
                slot.prefetched.push_back(Prefetch {
                    owned: next,
                    issued: Instant::now(),
                    open: PrefetchOpen::InFlight(open),
                });
                ordinal
            };
            self.mark_segment_at(ordinal, "seg_open_start_offset");
        }
        self.drive_prefetches(cx)
    }

    /// Poll every in-flight prefetched open in this partition's slot once,
    /// keeping each result until its segment's turn. A pending open has
    /// registered `cx`'s waker, so the stream is polled again when it can
    /// make progress.
    fn drive_prefetches(&mut self, cx: &mut Context<'_>) -> DFResult<()> {
        let mut ready = Vec::new();
        {
            let mut slot = self.prefetch_pool.slot(self.partition)?;
            for prefetch in slot.prefetched.iter_mut() {
                if let PrefetchOpen::InFlight(fut) = &mut prefetch.open
                    && let Poll::Ready(opened) = fut.as_mut().poll(cx)
                {
                    prefetch.open = PrefetchOpen::Ready {
                        opened: Box::new(opened),
                        at: Instant::now(),
                    };
                    ready.push(prefetch.owned.ordinal);
                }
            }
        }
        for ordinal in ready {
            self.mark_segment_at(ordinal, "seg_open_ready_offset");
        }
        Ok(())
    }

    /// Moves the segments a revocation handed back to this partition to the
    /// front of its work, in owned order.
    fn reclaim_handed_back(&mut self) -> DFResult<()> {
        let mut slot = self.prefetch_pool.slot(self.partition)?;
        while let Some(owned) = slot.handed_back.pop_back() {
            self.work.push_front(owned);
        }
        Ok(())
    }

    /// The next owned segment's prefetched open, if its turn has come with
    /// one in this partition's slot. Segments a revocation handed back are
    /// moved to the front of `work` first, under the same lock, so none is
    /// skipped by a revocation landing between the two reads.
    fn take_next_prefetch(&mut self) -> DFResult<Option<Prefetch>> {
        let mut slot = self.prefetch_pool.slot(self.partition)?;
        if slot.handed_back.is_empty() {
            return Ok(slot.prefetched.pop_front());
        }
        while let Some(owned) = slot.handed_back.pop_back() {
            self.work.push_front(owned);
        }
        Ok(None)
    }

    /// Handles a refused current open on the whole-segment fast path: a first
    /// open, a consumed prefetch ([`Reopen::Fast`]), or the `attrs_raw`
    /// row-path reopen ([`Reopen::Rows`]).
    ///
    /// The fetch memory budget is process-wide, so the bytes that refused
    /// the open may be held by prefetches of any partition of the statement.
    /// On a fetch memory refusal this turns the statement's pipeline off,
    /// drops every unconsumed prefetch of every partition
    /// ([`PrefetchPool::revoke_all`]; each segment goes back to the front of
    /// its owner's work, in owned order), moves this partition's own handed
    /// back segments into its work, and retries the refused open once, at
    /// once, on the same route and, for the row-path reopen, with the same
    /// `skip`. It waits for no other stream: the bytes are released by the
    /// drop itself. Any other error, any error off the fast path, and a
    /// refusal of the retry itself are returned untouched.
    ///
    /// The property this gives: within one statement a current open (a first
    /// open, a consumed prefetch, or the `attrs_raw` reopen) is reported
    /// refused only if the budget refused it twice, the second
    /// time after every unconsumed prefetch of every partition of the
    /// statement had been dropped and the statement's pipeline turned off.
    /// Between the drain and the retry a sibling's current open may reserve
    /// first; the retry then meets one open per partition, which is the
    /// sequential walk's own peak. Across statements the budget stays
    /// fail-fast: another statement's prefetches can hold the bytes, and
    /// that refusal is typed and reported like any contended reservation.
    fn on_open_refused(&mut self, e: DataFusionError, reopen: Reopen) -> DFResult<()> {
        if !(self.fast_whole_segment && is_fetch_memory_refusal(&e)) || self.retrying {
            return Err(e);
        }
        let Some(seg) = self.current_seg.clone() else {
            return Err(e);
        };
        let dropped = self.prefetch_pool.revoke_all()?;
        self.reclaim_handed_back()?;
        self.retrying = true;
        self.blocks.prefetch_revocations.add(dropped);
        self.blocks.prefetch_memory_reopens.add(1);
        tracing::debug!(
            partition = self.partition,
            segment = self.current_seg_ordinal,
            dropped,
            error = %e,
            "open refused by the fetch memory budget; dropped the statement's \
             prefetches and retrying once"
        );
        self.state = match reopen {
            Reopen::Fast => {
                self.open_started.get_or_insert_with(Instant::now);
                LogScanState::Opening(open_segment_fast(
                    Arc::clone(&self.ctx),
                    seg,
                    self.current_by_chunk,
                ))
            }
            Reopen::Rows { skip } => {
                self.open_started = Some(Instant::now());
                LogScanState::ReopenRows {
                    fut: self.row_reopen(seg),
                    skip,
                }
            }
        };
        Ok(())
    }

    /// The `attrs_raw` fallback's row-path reopen of `seg`, the current
    /// segment: the same routing decision its first open took (#862) on the
    /// fast path, and on the striped route the same block-index list, survivor
    /// list, footer and directories, with the carried whole object `take()`n,
    /// so it is consumed at most once by construction.
    fn row_reopen(&mut self, seg: SegmentRef) -> OpenFuture {
        if self.fast_whole_segment {
            let by_chunk = self.ctx.open_by_column_chunk(&seg);
            open_segment_fast(Arc::clone(&self.ctx), seg, by_chunk)
        } else {
            open_segment_subset(
                Arc::clone(&self.ctx),
                seg,
                self.current_indices.clone(),
                self.current_survivors.clone(),
                self.current_footer.clone(),
                self.current_whole_object.take(),
                self.current_dirs.as_ref().map(|c| Arc::clone(&c.dirs)),
            )
        }
    }

    /// The surviving-block index of the block just decoded, or `None` when
    /// this scan emits no row refs. `decoded_block` is that block's
    /// whole-object index, read from the scan before it decoded the block
    /// ([`LogSegmentScan::next_block_index`]), not the cursor's position in
    /// [`Self::current_indices`] (issue #2417).
    fn current_block(&self, decoded_block: Option<usize>) -> DFResult<Option<usize>> {
        block_index(
            self.row_refs,
            self.current_survivors.as_deref().map(Vec::as_slice),
            decoded_block,
            self.block_cursor,
            self.current_seg
                .as_ref()
                .map_or("", |s| s.data_object_key.as_str()),
        )
    }

    /// The row-ref address for the block just decoded (`decoded_block`, its
    /// whole-object index), and advance the cursor past it.
    fn take_block_range(&mut self, decoded_block: Option<usize>) -> DFResult<Option<RowRefRange>> {
        let range = self.current_block(decoded_block)?.map(|block| RowRefRange {
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
            &self.attr_keys,
            self.full_len,
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
    /// block's batches were charged once in [`Self::hold_batches`], and the
    /// decoded block itself stays charged in [`Self::resident`].
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
    /// pool for their total Arrow footprint. The decoded block they were built
    /// from is charged separately, in [`Self::resident`].
    ///
    /// [`Self::emit_next_columnar_batch`] moves each emitted batch's bytes
    /// from `held` to `emitted`.
    fn hold_batches(&mut self, batches: Vec<RecordBatch>) -> DFResult<()> {
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
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

    /// Drop the drained block's records or batches and release their charge.
    /// The columnar cursor's decoded block stays charged in
    /// [`Self::resident`] until the next decode replaces it.
    fn release_block(&mut self) {
        self.reservation.shrink(std::mem::take(&mut self.held));
        self.pending = Pending::None;
    }

    /// The scan the state holds reported exhaustion: publish its final
    /// counters and move to the next owned segment.
    fn end_scanned_segment(&mut self) {
        if let LogScanState::Columnar(scan)
        | LogScanState::Rows(scan)
        | LogScanState::RowFallbackBlock { scan, .. } = &self.state
        {
            let stats = scan.stats();
            self.blocks.record_scan(&stats);
            if self.fast_whole_segment {
                self.blocks.record_segment_totals(&stats);
            }
        }
        self.mark_segment("seg_done_offset");
        self.finish_segment();
    }

    /// The next block of the row-path scan the state holds: the one a
    /// [`LogScanState::DecodingRows`] job just decoded, or, with none waiting,
    /// a new decode. That decode goes to the read gate as a job when the
    /// scan's fetcher carries one, leaving the state at
    /// [`LogScanState::DecodingRows`] to return to `resume`, and runs inline
    /// otherwise. Exhaustion decodes nothing and always runs inline.
    fn next_row_block(&mut self, resume: RowResume) -> RowNext {
        match self.decoded.take() {
            Some(Decoded::Rows { block, next }) => return RowNext::Ready(block, next),
            Some(Decoded::Columnar(_)) => {
                return RowNext::Failed(DataFusionError::Internal(
                    "a columnar block resumed on the row path".into(),
                ));
            }
            None => {}
        }
        let (LogScanState::Rows(scan) | LogScanState::RowFallbackBlock { scan, .. }) =
            &mut self.state
        else {
            return RowNext::Failed(DataFusionError::Internal(
                "row block requested off a row-path state".into(),
            ));
        };
        // Unlike the columnar exit, the row exit frees its decoded block before
        // returning the records (`BlockScan::next_block`), so no previous
        // block is resident behind the reader while this job waits for a
        // permit, and there is no block charge to carry into it.
        if scan.remaining_blocks() > 0 && scan.block_gate().is_some() {
            let (LogScanState::Rows(scan) | LogScanState::RowFallbackBlock { scan, .. }) =
                std::mem::replace(&mut self.state, LogScanState::Done)
            else {
                return RowNext::Failed(DataFusionError::Internal(
                    "row block requested off a row-path state".into(),
                ));
            };
            self.state = LogScanState::DecodingRows {
                job: row_block_on_gate(scan),
                resume,
                started: Instant::now(),
            };
            return RowNext::Submitted;
        }
        let decode_started = Instant::now();
        let block = scan.next_block_index();
        let next = scan.next_block();
        self.blocks.decode_build_elapsed.add_elapsed(decode_started);
        RowNext::Ready(block, next)
    }

    /// This partition is done with the current segment: drop its handle on the
    /// segment's directories and move to the next owned segment.
    fn finish_segment(&mut self) {
        self.current_dirs = None;
        self.state = LogScanState::NextSegment;
    }

    /// This partition has emitted its pushed `fetch` rows: stop, mid-segment
    /// if need be, and release everything the scan still holds.
    ///
    /// A scan still open is dropped here instead of running to its
    /// end-of-segment record, so its counters are published first: the decode
    /// counts as they stand (`record_scan` accumulates), and on the
    /// whole-segment fast path the segment's prune totals, which are final from
    /// the open and so are the whole segment's, not a partial count.
    fn finish_at_fetch(&mut self) {
        match std::mem::replace(&mut self.state, LogScanState::Done) {
            LogScanState::Columnar(scan)
            | LogScanState::Rows(scan)
            | LogScanState::RowFallbackBlock { scan, .. } => {
                let stats = scan.stats();
                self.blocks.record_scan(&stats);
                if self.fast_whole_segment {
                    self.blocks.record_segment_totals(&stats);
                }
            }
            // A block job runs only once the previous block's batches or
            // records are all emitted, so no row has been emitted since the
            // last check and `fetch` cannot be reached with one in flight. Dropping it abandons the job; the scan folds its
            // accounting when it drops, with the future or with the job's
            // discarded result.
            LogScanState::Planning(_)
            | LogScanState::NextSegment
            | LogScanState::Opening(_)
            | LogScanState::ReopenRows { .. }
            | LogScanState::DecodingColumnar { .. }
            | LogScanState::DecodingRows { .. }
            | LogScanState::Done => {}
        }
        self.current_dirs = None;
        self.work.clear();
        self.prefetch_pool.clear(self.partition);
        self.release_block();
        self.resident.free();
    }

    /// Abandon the stream on error, releasing everything the scan still holds.
    fn fail(&mut self, e: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.state = LogScanState::Done;
        self.current_dirs = None;
        self.work.clear();
        self.prefetch_pool.clear(self.partition);
        self.release_block();
        self.resident.free();
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
        this.blocks.polls.add(1);
        let polled = this.poll_inner(cx);
        match &polled {
            Poll::Pending => this.blocks.polls_pending.add(1),
            Poll::Ready(Some(Ok(_))) if !this.first_batch_seen => {
                this.first_batch_seen = true;
                this.blocks.first_batch_elapsed.add_elapsed(this.origin);
            }
            Poll::Ready(None) if !this.done_seen => {
                this.done_seen = true;
                this.blocks.stream_elapsed.add_elapsed(this.origin);
            }
            Poll::Ready(_) => {}
        }
        polled
    }
}

/// The prefetch pool belongs to the exec and outlives this stream, so a
/// stream dropped mid-statement (a cancelled query, a satisfied limit above
/// it) drops its own slot's prefetches here, releasing their fetch
/// reservations with it rather than when the plan is dropped.
impl Drop for LogScanStream {
    fn drop(&mut self) {
        self.prefetch_pool.clear(self.partition);
        self.prefetch_pool.unregister(self.partition);
    }
}

impl LogScanStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<DFResult<RecordBatch>>> {
        let this = self;
        if let Err(e) = this
            .reclaim_handed_back()
            .and_then(|()| this.drive_prefetches(cx))
        {
            return this.fail(e);
        }
        loop {
            if this.fetch.is_some_and(|fetch| this.rows_emitted >= fetch) {
                this.finish_at_fetch();
                return Poll::Ready(None);
            }
            // Anything buffered from the current block goes out first.
            if this.has_pending() {
                let emit_started = Instant::now();
                let emitted = match &this.pending {
                    Pending::Rows { .. } => this.emit_next_row_batch(),
                    Pending::Batches(_) => this.emit_next_columnar_batch(),
                    Pending::None => unreachable!("has_pending() ruled this out"),
                };
                this.blocks.emit_elapsed.add_elapsed(emit_started);
                return match emitted {
                    Ok(batch) => {
                        // DataFusion drops the limit above a scan that
                        // accepted `with_fetch`, so the cap is exact here.
                        let batch = match this.fetch {
                            Some(fetch) if this.rows_emitted + batch.num_rows() > fetch => {
                                batch.slice(0, fetch.saturating_sub(this.rows_emitted))
                            }
                            _ => batch,
                        };
                        this.rows_emitted += batch.num_rows();
                        Poll::Ready(Some(Ok(batch)))
                    }
                    Err(e) => this.fail(e),
                };
            }
            // The block is drained: release its records or batches before
            // decoding another. A columnar block's decoded bytes stay charged
            // until the next decode replaces them (`resident`).
            if this.held > 0 || !matches!(this.pending, Pending::None) {
                this.release_block();
            }

            match &mut this.state {
                LogScanState::Planning(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(counts)) => {
                        this.blocks
                            .planning_wait_elapsed
                            .add_elapsed(this.stream_started);
                        // The stride the plan counted each segment's owners
                        // against: `target_partitions` capped by the real block
                        // count (ADR-0102), so with fewer blocks than partitions
                        // the extra partitions get empty work.
                        let n = counts.partitions;
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
                LogScanState::NextSegment => {
                    let prefetch = match this.take_next_prefetch() {
                        Ok(prefetch) => prefetch,
                        Err(e) => return this.fail(e),
                    };
                    // The next owned segment's open was issued ahead of its
                    // turn (ADR-2414 decision A2). Everything the sequential
                    // walk records for a fast-path segment is recorded here,
                    // at its turn and in owned order; every prefetch is a
                    // ranged open.
                    if let Some(Prefetch {
                        owned: OwnedSeg { seg, ordinal, .. },
                        issued,
                        open,
                    }) = prefetch
                    {
                        this.current_seg = Some(seg);
                        this.current_seg_ordinal = ordinal;
                        this.current_indices = Vec::new();
                        this.current_survivors = None;
                        this.current_footer = None;
                        this.current_dirs = None;
                        this.current_whole_object = None;
                        this.current_by_chunk = true;
                        this.retrying = false;
                        this.block_cursor = 0;
                        this.consecutive_fallbacks = 0;
                        this.blocks.segments_opened.add(1);
                        this.blocks.record_fast_path_route(true);
                        this.ctx.record_open_shape(true);
                        this.ctx.record_data_object_touched();
                        match open {
                            PrefetchOpen::InFlight(fut) => {
                                this.open_started = Some(issued);
                                this.state = LogScanState::Opening(fut);
                            }
                            PrefetchOpen::Ready { opened, at } => {
                                this.open_started = None;
                                this.blocks
                                    .open_elapsed
                                    .add_duration(at.saturating_duration_since(issued));
                                match *opened {
                                    Ok(Some(scan)) => {
                                        this.state = if this.columnar_eligible {
                                            LogScanState::Columnar(Box::new(scan))
                                        } else {
                                            LogScanState::Rows(Box::new(scan))
                                        };
                                        if let Err(e) = this.top_up_prefetch(cx) {
                                            return this.fail(e);
                                        }
                                    }
                                    Ok(None) => this.finish_segment(),
                                    Err(e) => {
                                        if let Err(e) = this.on_open_refused(e, Reopen::Fast) {
                                            return this.fail(e);
                                        }
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    match this.work.pop_front() {
                        Some(OwnedSeg {
                            seg,
                            ordinal,
                            indices,
                            survivors,
                            footer,
                            whole_object,
                            dirs,
                        }) => {
                            this.current_seg = Some(seg.clone());
                            this.current_seg_ordinal = ordinal;
                            this.current_indices = indices.clone();
                            this.current_survivors = survivors.clone();
                            this.current_footer = footer.clone();
                            let open_dirs = dirs.as_ref().map(|c| Arc::clone(&c.dirs));
                            this.current_dirs = dirs;
                            // Moved, not cloned: this stream consumes the carried
                            // whole object exactly once, by whichever open below
                            // `take()`s it first. A later `ReopenRows` reopen
                            // must see `None` and pay for its own fetch (real GET
                            // or read-cache hit), or `tenant_bytes_with_footer`
                            // charges `add_bytes_reused` twice for one buffer
                            // (issue #835 follow-up).
                            this.current_whole_object = whole_object;
                            this.retrying = false;
                            this.block_cursor = 0;
                            this.consecutive_fallbacks = 0;
                            this.blocks.segments_opened.add(1);
                            this.open_started = Some(Instant::now());
                            this.mark_segment("seg_open_start_offset");
                            // Whole-segment fast path reads the object in one GET
                            // (#693 part 3), or by column chunk when the projection
                            // is narrow enough to pay for the extra round trips
                            // (#862); the striped path opens only this partition's
                            // subset, reusing the plan footer if any.
                            this.state = if this.fast_whole_segment {
                                let by_chunk = this.ctx.open_by_column_chunk(&seg);
                                this.current_by_chunk = by_chunk;
                                this.blocks.record_fast_path_route(by_chunk);
                                this.ctx.record_open_shape(by_chunk);
                                // The fast path's owning-partition recorder for
                                // ADR-0996 decision 3; see
                                // `PartitionCtx::record_data_object_touched` for why
                                // this site is once per segment per query and why
                                // the planned route's recorder cannot also fire.
                                this.ctx.record_data_object_touched();
                                LogScanState::Opening(open_segment_fast(
                                    Arc::clone(&this.ctx),
                                    seg,
                                    by_chunk,
                                ))
                            } else {
                                // `take()`, not the moved-in value directly: this
                                // IS the one consumption of the carried whole
                                // object (see the comment above where it moved
                                // into `current_whole_object`). Taking it here
                                // leaves `None` behind for any later reopen.
                                let whole_object = this.current_whole_object.take();
                                LogScanState::Opening(open_segment_subset(
                                    Arc::clone(&this.ctx),
                                    seg,
                                    indices,
                                    survivors,
                                    footer,
                                    whole_object,
                                    open_dirs,
                                ))
                            };
                        }
                        None => {
                            this.state = LogScanState::Done;
                        }
                    }
                }
                // The current open is polled before the opens behind it are
                // topped up, so its first reservation goes first (its later
                // ones can still follow a prefetch's); the top-up then
                // runs whether it resolved or is still in flight, which keeps
                // the next segments' round trips overlapping this one's.
                LogScanState::Opening(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.retrying = false;
                        if let Some(started) = this.open_started.take() {
                            this.blocks.open_elapsed.add_elapsed(started);
                        }
                        this.mark_segment("seg_open_ready_offset");
                        this.state = if this.columnar_eligible {
                            LogScanState::Columnar(Box::new(scan))
                        } else {
                            LogScanState::Rows(Box::new(scan))
                        };
                        if let Err(e) = this.top_up_prefetch(cx) {
                            return this.fail(e);
                        }
                    }
                    // The segment's ts span could not satisfy the query: no GET
                    // was issued and there is nothing to drain.
                    Poll::Ready(Ok(None)) => {
                        if let Some(started) = this.open_started.take() {
                            this.blocks.open_elapsed.add_elapsed(started);
                        }
                        this.retrying = false;
                        this.finish_segment();
                    }
                    Poll::Ready(Err(e)) => {
                        if let Err(e) = this.on_open_refused(e, Reopen::Fast) {
                            return this.fail(e);
                        }
                    }
                    Poll::Pending => {
                        this.blocks.open_pending_polls.add(1);
                        if let Err(e) = this.top_up_prefetch(cx) {
                            return this.fail(e);
                        }
                        return Poll::Pending;
                    }
                },
                LogScanState::ReopenRows { fut, skip } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.retrying = false;
                        if let Some(started) = this.open_started.take() {
                            this.blocks.reopen_elapsed.add_elapsed(started);
                        }
                        let skip = *skip;
                        this.state = LogScanState::RowFallbackBlock {
                            scan: Box::new(scan),
                            skip,
                        };
                    }
                    // Cannot happen for a segment already opened once this scan;
                    // treat a vanished segment as end-of-segment rather than
                    // panicking.
                    Poll::Ready(Ok(None)) => {
                        this.retrying = false;
                        this.finish_segment();
                    }
                    Poll::Ready(Err(e)) => {
                        let skip = *skip;
                        if let Err(e) = this.on_open_refused(e, Reopen::Rows { skip }) {
                            return this.fail(e);
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Columnar(scan) => {
                    let step = match this.decoded.take() {
                        Some(Decoded::Columnar(step)) => step,
                        Some(Decoded::Rows { .. }) => {
                            return this.fail(DataFusionError::Internal(
                                "a row-path block resumed on the columnar path".into(),
                            ));
                        }
                        None => {
                            // The row-ref address of the block this call
                            // decodes, resolved from the scan's own next block
                            // (issue #2417), not from the cursor's position in
                            // `current_indices`. Read before the decode, since
                            // the view it returns holds the scan borrowed; only
                            // a decoded block consumes it, and with no next
                            // block none is decoded.
                            let block = match scan.next_block_index() {
                                Some(next) => block_index(
                                    this.row_refs,
                                    this.current_survivors.as_deref().map(Vec::as_slice),
                                    Some(next),
                                    this.block_cursor,
                                    this.current_seg
                                        .as_ref()
                                        .map_or("", |s| s.data_object_key.as_str()),
                                ),
                                None => Ok(None),
                            };
                            let job = ColumnarBlockJob {
                                schema: Arc::clone(&this.schema),
                                projection: Arc::clone(&this.projection),
                                declared: Arc::clone(&this.declared),
                                attr_keys: Arc::clone(&this.attr_keys),
                                full_len: this.full_len,
                                segment: this.current_seg_ordinal,
                                block,
                                #[cfg(test)]
                                panic_for_test: take_columnar_job_panic(
                                    this.current_seg
                                        .as_ref()
                                        .map_or("", |s| s.data_object_key.as_str()),
                                ),
                            };
                            match scan.block_gate() {
                                // Exhaustion decodes nothing, so it submits no
                                // job.
                                Some((gate, size)) if scan.remaining_blocks() > 0 => {
                                    let LogScanState::Columnar(scan) =
                                        std::mem::replace(&mut this.state, LogScanState::Done)
                                    else {
                                        return this.fail(DataFusionError::Internal(
                                            "columnar decode submitted off a non-columnar state"
                                                .into(),
                                        ));
                                    };
                                    // The previous block's charge goes with the
                                    // scan: the reader frees that block only
                                    // when the job's decode runs.
                                    let resident = this.resident.take();
                                    this.state = LogScanState::DecodingColumnar {
                                        job: columnar_block_on_gate(
                                            gate, size, scan, job, resident,
                                        ),
                                        started: Instant::now(),
                                    };
                                    continue;
                                }
                                _ => {
                                    let decode_started = Instant::now();
                                    let step = job.run(scan, &this.resident);
                                    this.blocks.decode_build_elapsed.add_elapsed(decode_started);
                                    step
                                }
                            }
                        }
                    };
                    match step {
                        ColumnarStep::Failed(e) => return this.fail(e),
                        ColumnarStep::Exhausted(stats) => {
                            this.blocks.record_scan(&stats);
                            // The whole-segment fast path has no plan phase, so
                            // the whole-segment prune totals are recorded here,
                            // per segment (one owner per segment, no double
                            // count), instead of by partition 0 during planning.
                            if this.fast_whole_segment {
                                this.blocks.record_segment_totals(&stats);
                            }
                            this.mark_segment("seg_done_offset");
                            this.finish_segment();
                        }
                        ColumnarStep::Fallback => {
                            // Re-open the segment on the row path over the SAME
                            // block-index list this partition owns (ADR-0102),
                            // skipping the blocks already fully emitted (columnar,
                            // or row-emitted by an earlier fallback in this same
                            // segment) so no row is emitted twice. `skip` is
                            // `block_cursor`, a position within this partition's
                            // own list, so the count and the list line up even
                            // when the segment's blocks are striped across several
                            // partitions. `RowFallbackBlock` (issue #1769) takes
                            // only the next block -- the one that just failed the
                            // columnar attempt -- through the row path, then hands
                            // this reopened scan back to `Columnar` for the rest.
                            //
                            // Publish the abandoned columnar cursor's partial
                            // counters (issue #474) before it is dropped: the
                            // re-opened row scan below re-decodes this partition's
                            // list from the start up to `skip`, so those blocks'
                            // pages are decoded twice, but the abandoned cursor's
                            // own count of its first pass was previously discarded
                            // along with it. `record_scan` accumulates, so this
                            // and the reopened scan's own eventual `record_scan`
                            // call(s) sum to the real total decode work across all
                            // passes, matching what `EXPLAIN ANALYZE` claims the
                            // counters prove (ADR-0087): that projection reached
                            // the page level.
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
                            // re-counted: the route metric and the accounting
                            // handle's opens-by-shape counters both tally
                            // segments, not opens.
                            let fut = this.row_reopen(seg);
                            this.blocks.reopens.add(1);
                            this.consecutive_fallbacks += 1;
                            this.open_started = Some(Instant::now());
                            this.state = LogScanState::ReopenRows {
                                fut,
                                skip: this.block_cursor,
                            };
                            // The abandoned cursor, and the block its reader
                            // held, went with the state it was in.
                            this.resident.free();
                        }
                        ColumnarStep::Held { batches } => {
                            // A clean columnar block: the streak of consecutive
                            // fallbacks that would otherwise escalate to a
                            // full row-path commit is broken.
                            this.consecutive_fallbacks = 0;
                            // Count every consumed clean block, empty or not, so
                            // a later `attrs_raw` fallback's `ReopenRows` skips
                            // exactly the blocks the cursor advanced past. The
                            // row-ref cursor moves with it, so a fallback
                            // re-opens at the same surviving-block position.
                            this.block_cursor += 1;
                            // The decoded block, whether or not a row survived,
                            // is already charged in `resident`: the job that
                            // decoded it resized that charge from the previous
                            // block's as its decode returned, and the next job
                            // resizes it again only once its own decode has
                            // freed this block. Outside the decode calls
                            // themselves there is no interval during which it
                            // is resident and uncharged, a wait for a gate
                            // permit included.
                            if !batches.is_empty()
                                && let Err(e) = this.hold_batches(batches)
                            {
                                return this.fail(e);
                            }
                        }
                    }
                }
                LogScanState::Rows(_) => {
                    let (decoded_block, next) = match this.next_row_block(RowResume::Rows) {
                        RowNext::Ready(block, next) => (block, next),
                        RowNext::Submitted => continue,
                        RowNext::Failed(e) => return this.fail(e),
                    };
                    match next {
                        Ok(Some(records)) => {
                            // Stamp the block's row-ref address before the
                            // records are held: the batch builder reads it out
                            // of `pending_range` as it chunks them. Resolved
                            // from the block the scan itself yielded (issue
                            // #2417), not from the cursor's position in
                            // `current_indices`.
                            match this.take_block_range(decoded_block) {
                                Ok(range) => this.pending_range = range,
                                Err(e) => return this.fail(e),
                            }
                            if let Err(e) = this.take_row_block(records) {
                                return this.fail(e);
                            }
                        }
                        // Only `None` ends the segment. Its counters are final
                        // now, so publish them before moving on.
                        Ok(None) => this.end_scanned_segment(),
                        Err(e) => return this.fail(SqlError::from(e).into()),
                    }
                }
                LogScanState::RowFallbackBlock { skip, .. } => {
                    // Drain and discard the blocks already fully emitted
                    // (columnar, or row-emitted by an earlier fallback in this
                    // same segment), then take exactly the next block -- the one
                    // that carried the `attrs_raw` overflow page -- through the
                    // row path.
                    let skip = *skip;
                    let (decoded_block, next) =
                        match this.next_row_block(RowResume::FallbackBlock { skip }) {
                            RowNext::Ready(block, next) => (block, next),
                            RowNext::Submitted => continue,
                            RowNext::Failed(e) => return this.fail(e),
                        };
                    if skip > 0 {
                        match next {
                            Ok(Some(_)) => {
                                if let LogScanState::RowFallbackBlock { skip, .. } = &mut this.state
                                {
                                    *skip -= 1;
                                }
                            }
                            Ok(None) => this.end_scanned_segment(),
                            Err(e) => return this.fail(SqlError::from(e).into()),
                        }
                        continue;
                    }
                    match next {
                        Ok(Some(records)) => {
                            // Stamp the block's row-ref address before the
                            // records are held: the batch builder reads it out
                            // of `pending_range` as it chunks them. Resolved
                            // from the block the scan itself yielded (issue
                            // #2417), not from the cursor's position in
                            // `current_indices`.
                            match this.take_block_range(decoded_block) {
                                Ok(range) => this.pending_range = range,
                                Err(e) => return this.fail(e),
                            }
                            if let Err(e) = this.take_row_block(records) {
                                return this.fail(e);
                            }
                            // Exactly one block goes through the row path per
                            // fallback (issue #1769): hand the still-open scan
                            // back to the columnar cursor for the blocks after
                            // it instead of falling the rest of the segment to
                            // rows -- UNLESS this segment has now hit
                            // `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS` fallbacks in
                            // a row with no clean columnar block between them
                            // (issue #1769 follow-up): the per-object `attrs_raw` budget
                            // that caused this block to overflow almost
                            // certainly causes the rest of this partition's
                            // block list to as well, so retrying columnar
                            // block by block would pay one reopen per
                            // remaining block for no columnar benefit. Commit
                            // the still-open scan straight to `Rows` instead:
                            // it already sits right after the block just
                            // emitted, so the remaining blocks drain through
                            // the row path with no further reopen, bounding
                            // this segment's total reopens to
                            // `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS + 1`.
                            let scan = match std::mem::replace(&mut this.state, LogScanState::Done)
                            {
                                LogScanState::RowFallbackBlock { scan, .. } => scan,
                                _ => unreachable!("state just matched as RowFallbackBlock"),
                            };
                            this.state = if this.consecutive_fallbacks
                                > MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS
                            {
                                LogScanState::Rows(scan)
                            } else {
                                LogScanState::Columnar(scan)
                            };
                        }
                        // Cannot happen: the columnar cursor that triggered this
                        // fallback had already decoded this exact block, so a
                        // fresh cursor positioned at the same index (past the
                        // `skip` already discarded) must yield it too. Treated
                        // as end-of-segment rather than panicking, matching
                        // `ReopenRows`'s own `Ok(None)` handling above.
                        Ok(None) => this.end_scanned_segment(),
                        Err(e) => return this.fail(SqlError::from(e).into()),
                    }
                }
                LogScanState::DecodingColumnar { job, started } => {
                    let started = *started;
                    let ran = match job.as_mut().poll(cx) {
                        Poll::Ready(ran) => ran,
                        Poll::Pending => return Poll::Pending,
                    };
                    this.blocks.decode_build_elapsed.add_elapsed(started);
                    match ran {
                        Ok((scan, resident, step)) => {
                            this.resident = resident;
                            this.decoded = Some(Decoded::Columnar(step));
                            this.state = LogScanState::Columnar(scan);
                        }
                        Err(err) => {
                            let key = this
                                .current_seg
                                .as_ref()
                                .map_or("", |s| s.data_object_key.as_str());
                            let e = log_block_gate_failed(key, err);
                            return this.fail(e);
                        }
                    }
                }
                LogScanState::DecodingRows {
                    job,
                    resume,
                    started,
                } => {
                    let (resume, started) = (*resume, *started);
                    let (scan, block, next) = match job.as_mut().poll(cx) {
                        Poll::Ready(ran) => ran,
                        Poll::Pending => return Poll::Pending,
                    };
                    this.blocks.decode_build_elapsed.add_elapsed(started);
                    this.decoded = Some(Decoded::Rows { block, next });
                    this.state = match resume {
                        RowResume::Rows => LogScanState::Rows(scan),
                        RowResume::FallbackBlock { skip } => {
                            LogScanState::RowFallbackBlock { scan, skip }
                        }
                    };
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
    attr_keys: &[String],
    full_len: usize,
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
            // A synthetic per-key attribute column (issue #1768): index
            // >= full_len selects `attr_keys[i - full_len]`, rendered as `Utf8`
            // straight from the merged view with the map's own rules. This is
            // the row-path (attrs_raw fallback) sibling of the columnar
            // [`build_attr_key_columnar_array`], and it renders each value with
            // the same [`attr_value_to_string`] the `attrs` map arm above uses,
            // so the divergence from a declared column (a non-`Str` value under
            // a `Str` declaration renders as text here, reads NULL there) is
            // reproduced exactly.
            other if other >= full_len => match attr_keys.get(other - full_len) {
                Some(key) => attr_key_column_array(key, &merged),
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
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

/// Build one synthetic per-key attribute column as a `Utf8` array from the
/// per-record precomputed merged views (issue #1768), the row-path sibling of
/// [`build_attr_key_columnar_array`].
///
/// For each record the key is looked up in that record's merged view via
/// [`find_attr`] and rendered to text with [`attr_value_to_string`], exactly as
/// the `attrs` map column renders it; an absent key is NULL. This is what makes
/// `attrs['k']` diverge from a declared `Str` column `"k"` by design: a record
/// holding a non-`Str` value under the key renders as text here (`7`) while the
/// declared column reads NULL. The row path runs only for the `attrs_raw`
/// overflow fallback of an otherwise-columnar query; the common case is
/// [`build_attr_key_columnar_array`].
fn attr_key_column_array(key: &str, merged: &[Vec<(String, AttrValue)>]) -> ArrayRef {
    let mut b = StringBuilder::new();
    for row in merged {
        match find_attr(row, key) {
            Some(v) => b.append_value(attr_value_to_string(v)),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

// ---------------------------------------------------------------------------
// Columnar fast path (ADR-0099 decision 2)
// ---------------------------------------------------------------------------

/// A FIELD_DIR column of a declared key, resolved once for the whole block into
/// a per-column cursor (#875). The scan's row loop read one cell with one
/// `HashMap<u32, _>` lookup per cell; the cursor resolves the column's storage
/// once and then indexes the resolved slice per row with no lookup.
enum DeclaredCursor<'a> {
    Str(StrCursor<'a>),
    Bytes(BytesCursor<'a>),
    I64(I64Cursor<'a>),
    F64(F64BitsCursor<'a>),
    Bool(BoolCursor<'a>),
}

#[cfg(test)]
thread_local! {
    /// UTF-8 validations [`cell_text`] ran on this thread, so a test can pin how
    /// often a declared `Str` cell's bytes are validated.
    static CELL_TEXT_VALIDATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A `Str` cell's bytes as text, or `None` when they are not UTF-8. The
/// columnar path validates every attribute `Str` cell and dictionary entry
/// through here.
fn cell_text(bytes: &[u8]) -> Option<&str> {
    #[cfg(test)]
    CELL_TEXT_VALIDATIONS.with(|n| n.set(n.get() + 1));
    std::str::from_utf8(bytes).ok()
}

/// A `Str` FIELD_DIR column resolved once per block, read so that a declared
/// `Str` build validates each cell's UTF-8 at most once per block (ADR-2121 D3).
///
/// On a dictionary page each entry is validated the first time any row or the
/// dictionary build asks for it, and every later read of that entry, from the
/// presence search or from the build, is a lookup of the row's id. A plain page
/// validates the cell on each [`text_at`](Self::text_at) call, so a caller that
/// needs both presence and text reads it once and keeps the result
/// ([`winner_at`]).
struct StrCursor<'a> {
    cells: BytesCursor<'a>,
    /// The page's ids and per-entry text, `Some` only for a dictionary page.
    dict: Option<StrDictText<'a>>,
}

/// A dictionary page's ids plus each entry's text, validated on first use.
struct StrDictText<'a> {
    col: StrDictColumn<'a>,
    /// Parallel to `col.dict()`: `Some(Some(text))` for a validated UTF-8
    /// entry, `Some(None)` for a validated non-UTF-8 entry, unset until first
    /// read.
    text: Vec<std::cell::OnceCell<Option<&'a str>>>,
}

impl<'a> StrDictText<'a> {
    /// The text of dictionary entry `id`, or `None` when it is not UTF-8.
    fn entry(&self, id: usize) -> Option<&'a str> {
        let slot = self.text.get(id)?;
        let dict = self.col.dict();
        *slot.get_or_init(|| dict.get(id).and_then(|b| cell_text(b)))
    }

    /// The number of dictionary entries.
    fn len(&self) -> usize {
        self.text.len()
    }
}

impl<'a> StrCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, column_id: u32) -> Self {
        let dict = view.str_dict(column_id).map(|col| StrDictText {
            text: (0..col.dict().len())
                .map(|_| std::cell::OnceCell::new())
                .collect(),
            col,
        });
        StrCursor {
            cells: view.bytes_cursor(column_id),
            dict,
        }
    }

    /// The cell at surviving row `i` as text, or `None` when absent or not
    /// UTF-8. Treating invalid UTF-8 as no value matches the row path
    /// (`String::from_utf8(..).ok()`), which lets a resource/scope fallback show
    /// through.
    fn text_at(&self, i: usize) -> Option<&'a str> {
        match &self.dict {
            Some(d) => d.entry(d.col.id_at(i)? as usize),
            None => cell_text(self.cells.at(i)?),
        }
    }
}

/// Whether a cursor sets the key at a row, and for a `Str` cursor the text its
/// presence test already validated.
enum Presence<'a> {
    Absent,
    Present,
    Text(&'a str),
}

impl<'a> DeclaredCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, col: AttrColumn) -> Self {
        match col.ty {
            FieldType::Str => DeclaredCursor::Str(StrCursor::resolve(view, col.column_id)),
            FieldType::Bytes => DeclaredCursor::Bytes(view.bytes_cursor(col.column_id)),
            FieldType::I64 => DeclaredCursor::I64(view.i64_cursor(col.column_id)),
            FieldType::F64 => DeclaredCursor::F64(view.f64_bits_cursor(col.column_id)),
            FieldType::Bool => DeclaredCursor::Bool(view.bool_cursor(col.column_id)),
        }
    }

    /// Whether the record sets the key in this column at surviving row `i`, with
    /// the same "readable" rule the row path uses: an invalid-UTF-8 `Str` cell is
    /// not a value (matches [`read_str_cell`]), so it must not suppress the
    /// resource/scope fallback. A `Str` cell's validated text is returned with
    /// the answer so the caller does not validate it again.
    fn presence_at(&self, i: usize) -> Presence<'a> {
        let present = match self {
            DeclaredCursor::Str(c) => return c.text_at(i).map_or(Presence::Absent, Presence::Text),
            DeclaredCursor::Bytes(c) => c.at(i).is_some(),
            DeclaredCursor::I64(c) => c.at(i).is_some(),
            DeclaredCursor::F64(c) => c.at(i).is_some(),
            DeclaredCursor::Bool(c) => c.at(i).is_some(),
        };
        if present {
            Presence::Present
        } else {
            Presence::Absent
        }
    }

    /// The cell at surviving row `i` as an [`AttrValue`], or `None` when NULL (or,
    /// for `Str`, not UTF-8). Used for the multi-occurrence non-`Str` builders and
    /// the synthetic per-key columns; the `Str` builder reads `&str` directly so
    /// it never allocates a throwaway `String` (#875).
    fn value_at(&self, i: usize) -> Option<AttrValue> {
        match self {
            DeclaredCursor::Str(c) => c.text_at(i).map(|s| AttrValue::Str(s.to_string())),
            DeclaredCursor::Bytes(c) => c.at(i).map(|b| AttrValue::Bytes(b.to_vec())),
            DeclaredCursor::I64(c) => c.at(i).map(AttrValue::I64),
            DeclaredCursor::F64(c) => c.at(i).map(|bits| AttrValue::F64(f64::from_bits(bits))),
            DeclaredCursor::Bool(c) => c.at(i).map(AttrValue::Bool),
        }
    }
}

/// The index into `cols`/`cursors` of the record's WINNING occurrence of a key
/// at surviving row `i`, or `None` when the record does not set the key, plus
/// the winner's text when it is a `Str` cursor (validated once, by the presence
/// test).
///
/// The highest FIELD_DIR type byte wins, and among equal type bytes the last
/// one, the tie-break `max_by_key` gives; see [`DeclaredPlan::winning_idx`] for
/// the rule this implements.
fn winner_at<'a>(
    cols: &[AttrColumn],
    cursors: &[DeclaredCursor<'a>],
    i: usize,
) -> Option<(usize, Option<&'a str>)> {
    let mut best: Option<(usize, u8, Option<&'a str>)> = None;
    for (k, (col, cursor)) in cols.iter().zip(cursors).enumerate() {
        let text = match cursor.presence_at(i) {
            Presence::Absent => continue,
            Presence::Present => None,
            Presence::Text(s) => Some(s),
        };
        let ty = col.ty.to_u8();
        if best.is_none_or(|(_, b, _)| ty >= b) {
            best = Some((k, ty, text));
        }
    }
    best.map(|(k, _, text)| (k, text))
}

/// One declared column's FIELD_DIR resolution for a block, done once rather than
/// per row (ADR-0099 decision 2). Every FIELD_DIR column of the key is resolved
/// to a cursor once when the plan is built; the row loop then reads through the
/// cursors with no per-cell column lookup (#875).
struct DeclaredPlan<'d, 'a> {
    dc: &'d DeclaredColumn,
    /// The raw FIELD_DIR columns of this key, across all stored types. Kept for
    /// their type bytes, which order the occurrences ([`winner_at`]).
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

    /// The index into [`Self::cols`]/[`Self::cursors`] of the record's WINNING
    /// occurrence of the key at surviving row `i`, or `None` when the record does
    /// not set the key at all.
    ///
    /// A record that carries one name in several FIELD_DIR columns (a name
    /// written as both a string and an integer splits into two columns) resolves
    /// to one value, and which one is fixed by docs/log-segment-format.md
    /// ("Within the record layer"): `rebuild_record` lays a record's columnar
    /// occurrences out ascending by FIELD_DIR type byte and `merged_attrs` folds
    /// that list last-wins, so the occurrence with the highest type byte wins.
    /// The `attrs_raw` overflow tier, which beats every columnar occurrence,
    /// cannot appear here: a block carrying an `attrs_raw` page falls the whole
    /// segment back to the row path before the fast path builds a column.
    ///
    /// `max_by_key` returns the LAST maximum, which is the tie-break the same
    /// paragraph pins; the type byte is read from [`Self::cols`] rather than
    /// from FIELD_DIR position, so the answer does not depend on the directory's
    /// sort order.
    ///
    /// The fused `single_matching` reads get the same answer from the single
    /// matching read.
    fn winning_idx(&self, i: usize) -> Option<usize> {
        self.winner(i).map(|(k, _)| k)
    }

    /// [`winning_idx`](Self::winning_idx) plus the winner's text when it is the
    /// `Str` cursor, as its presence test validated it ([`winner_at`]).
    fn winner(&self, i: usize) -> Option<(usize, Option<&'a str>)> {
        winner_at(&self.cols, &self.cursors, i)
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

/// Per-column, per-block resolver for a declared key's merged value under the
/// same record-wins-over-resource precedence the row path's [`merged_attrs`] +
/// [`find_attr`] produce.
///
/// Three parts of the merge that do not vary by row are resolved once here rather
/// than in the per-row read. [`DeclaredPlan::single_matching`] and the
/// declared-type cursor ([`DeclaredPlan::matching_cursor`]) are plan-invariant
/// and become fields. The resource/scope fallback ([`find_attr`] over the decoded
/// stream attributes) returns the same answer for every row sharing a
/// `stream_ref`, so it is memoized per `stream_ref`: its scan and the one clone
/// of its result run once per distinct stream in the block, not once per row.
///
/// The precedence is ADR-0090 decision 7: the record's own value wins if it sets
/// the key in any FIELD_DIR column; a record whose winning occurrence
/// ([`DeclaredPlan::winning_idx`]) is at a different type yields NULL and does
/// NOT consult the fallback; only a record that does not set the key at all
/// reads the resource/scope value. The `single_matching` fast path still treats
/// a `Str` cell that is not valid UTF-8 as absent, falling through to the
/// fallback.
struct DeclaredResolver<'p, 'd, 'a> {
    plan: &'p DeclaredPlan<'d, 'a>,
    /// [`DeclaredPlan::single_matching`], resolved once for the block.
    single_matching: bool,
    /// [`DeclaredPlan::matching_cursor`], resolved once for the block.
    matching_cursor: Option<&'p DeclaredCursor<'a>>,
    /// Memo of the resource/scope fallback value per `stream_ref`, so the
    /// [`find_attr`] scan runs once per distinct stream, not once per row.
    fallback: HashMap<u32, Option<AttrValue>>,
    /// How many times the fallback scan actually ran: exactly the number of
    /// distinct stream refs that reached the fallback. Pinned by the
    /// memoization test.
    resolve_count: usize,
}

impl<'p, 'd, 'a> DeclaredResolver<'p, 'd, 'a> {
    fn new(plan: &'p DeclaredPlan<'d, 'a>) -> Self {
        DeclaredResolver {
            plan,
            single_matching: plan.single_matching(),
            matching_cursor: plan.matching_cursor(),
            fallback: HashMap::new(),
            resolve_count: 0,
        }
    }

    /// The resource/scope fallback value for `stream_ref`, memoized. The
    /// [`find_attr`] scan and the one clone of its result run once per distinct
    /// stream; every later row of the same stream reads the memo.
    fn fallback_value(
        &mut self,
        view: &ColumnarBlockView<'_>,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
        stream_ref: u32,
    ) -> DFResult<&Option<AttrValue>> {
        match self.fallback.entry(stream_ref) {
            std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resource = resource_attrs(view, cache, stream_ref)?;
                let value = find_attr(resource, &self.plan.dc.key).cloned();
                self.resolve_count += 1;
                Ok(e.insert(value))
            }
        }
    }

    /// The cursor over the key's one FIELD_DIR column when
    /// [`DeclaredPlan::single_matching`] holds for the block, else `None`.
    fn single_cursor(&self) -> Option<&'p DeclaredCursor<'a>> {
        self.matching_cursor.filter(|_| self.single_matching)
    }

    /// The resource/scope value for surviving row `i`'s stream, for a row the
    /// record does not set the key at; `None` when the stream has none.
    fn fallback_at(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<&AttrValue>> {
        let Some(stream_ref) = view.stream_ref(i) else {
            return Ok(None);
        };
        Ok(self.fallback_value(view, cache, stream_ref)?.as_ref())
    }

    /// The merged value of the declared key at surviving row `i`. Returns the
    /// record-column value when the record's WINNING occurrence of the key
    /// ([`DeclaredPlan::winning_idx`]) is at the declared type; `None` when that
    /// winner is at a different type (wrong variant, NULL by ADR-0090 decision
    /// 7), including the case where the record also has a declared-type cell at
    /// this row that the winner shadows. Only when the record does not set the
    /// key at all is the resource/scope fallback consulted, whose variant the
    /// caller checks against the declared type.
    fn merged_value(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<AttrValue>> {
        if self.single_matching {
            // Fused: the one matching-column read is both the presence test and
            // the value (deliverable 2, #875). `value_at` is `Some` exactly when
            // the record sets the key at the declared type; `None` (absent, or a
            // `Str` cell that is not UTF-8) falls through to the resource/scope
            // value, matching the row path.
            if let Some(v) = self.matching_cursor.and_then(|c| c.value_at(i)) {
                return Ok(Some(v));
            }
        } else if let Some(win) = self.plan.winning_idx(i) {
            // Record wins. Its value is the cell of the occurrence that wins the
            // record layer's last-occurrence-wins order
            // ([`DeclaredPlan::winning_idx`]), or NULL when that winner is of a
            // type other than the declared one (ADR-0090 decision 7); either way
            // the fallback is not consulted.
            return Ok(if Some(win) == self.plan.matching_idx {
                self.plan.cursors[win].value_at(i)
            } else {
                None
            });
        }
        Ok(self.fallback_at(view, i, cache)?.cloned())
    }
}

/// Append a merged `I64` value, or NULL for an absent value or another variant.
fn append_i64(b: &mut Int64Builder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::I64(v)) => b.append_value(*v),
        _ => b.append_null(),
    }
}

/// Append a merged `Bool` value, or NULL for an absent value or another variant.
fn append_bool(b: &mut BooleanBuilder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::Bool(v)) => b.append_value(*v),
        _ => b.append_null(),
    }
}

/// Append a merged `Bytes` value, or NULL for an absent value or another
/// variant.
fn append_bytes(b: &mut BinaryBuilder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::Bytes(bytes)) => b.append_value(bytes),
        // Parity with the row path: a resource/scope `List`/`Map` value is
        // canonicalized. In the eligible (no `attrs_raw`) case a record's
        // `List`/`Map` is already stored as a canonicalized `Bytes` column, and
        // `decode_stream_attrs` omits nested resource values, so this arm is
        // effectively dead here; it is kept identical to `declared_column_array`.
        Some(v @ (AttrValue::List(_) | AttrValue::Map(_))) => {
            b.append_value(canonical_value_bytes(v))
        }
        _ => b.append_null(),
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
///
/// When [`DeclaredPlan::single_matching`] holds, the `I64`, `Bool` and `Bytes`
/// arms read each row's cell straight from the one cursor (ADR-2121 D2): a
/// present cell is appended with no [`AttrValue`], and only an absent cell
/// takes the resource/scope fallback, the order
/// [`DeclaredResolver::merged_value`] follows. Every other block goes through
/// `merged_value` per cell.
fn build_declared_columnar_array(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let plan = resolver.plan;
    let single = resolver.single_cursor();
    Ok(match plan.dc.ty {
        DeclaredType::Str => build_declared_str_columnar(view, resolver, start, end, cache)?,
        DeclaredType::I64 => {
            let mut b = Int64Builder::with_capacity(end - start);
            if let Some(DeclaredCursor::I64(c)) = single {
                for i in start..end {
                    match c.at(i) {
                        Some(v) => b.append_value(v),
                        None => append_i64(&mut b, resolver.fallback_at(view, i, cache)?),
                    }
                }
            } else {
                for i in start..end {
                    append_i64(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::with_capacity(end - start);
            if let Some(DeclaredCursor::Bool(c)) = single {
                for i in start..end {
                    match c.at(i) {
                        Some(v) => b.append_value(v),
                        None => append_bool(&mut b, resolver.fallback_at(view, i, cache)?),
                    }
                }
            } else {
                for i in start..end {
                    append_bool(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            if let Some(DeclaredCursor::Bytes(c)) = single {
                for i in start..end {
                    match c.at(i) {
                        Some(bytes) => b.append_value(bytes),
                        None => append_bytes(&mut b, resolver.fallback_at(view, i, cache)?),
                    }
                }
            } else {
                for i in start..end {
                    append_bytes(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
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
///   the page). Each entry is validated once per block ([`StrCursor`]), and a
///   row's presence is a lookup of its id.
/// - **Plain page** (`str_dict` returns `None`, or the key has no `Str`
///   FIELD_DIR column at all): a degenerate identity dictionary, one entry per
///   non-null surviving row with keys `0..`. The record's own `Str` value is read
///   as `&str` straight from the [`StrCursor`] into the builder, so no
///   throwaway `String` is allocated per cell (deliverable 3, #875), and the
///   text the presence search validated is the text appended. No hashing
///   and no dedup pass, so this case stays exactly as expensive as it was.
fn build_declared_str_columnar(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let n = end - start;
    let plan = resolver.plan;
    // The declared-type (`Str`) column's cursor; `None` when the key has no
    // `Str` column at all (then only the resource/scope fallback yields a value).
    let matching_str = match plan.matching_cursor() {
        Some(DeclaredCursor::Str(c)) => Some(c),
        _ => None,
    };
    Ok(match matching_str.and_then(|c| c.dict.as_ref()) {
        Some(dict) => {
            // Dictionary values start as the page's distinct values as text;
            // a non-UTF-8 entry becomes a NULL value. Ids address these in the
            // page's order, so a record row's page id maps straight to a
            // dictionary index. Resource/scope fallback values are appended
            // past the page dict.
            let mut values = StringBuilder::new();
            for id in 0..dict.len() {
                match dict.entry(id) {
                    Some(s) => values.append_value(s),
                    None => values.append_null(),
                }
            }
            let mut next_extra = i32::try_from(dict.len()).map_err(|_| {
                DataFusionError::Internal("declared Str dictionary exceeds i32 keys".into())
            })?;
            let mut keys: Vec<Option<i32>> = Vec::with_capacity(n);
            for i in start..end {
                if let Some(win) = plan.winning_idx(i) {
                    // Record wins. Its value is the winning occurrence's cell
                    // ([`DeclaredPlan::winning_idx`]); a winner in another-typed
                    // column of the same key is a NULL cell, even when this row
                    // also has a `Str` cell the winner shadows. An `id` pointing
                    // at a non-UTF-8 (NULL) dictionary value also reads NULL,
                    // matching the UTF-8 rule on the `Str` cursor.
                    match (Some(win) == plan.matching_idx)
                        .then(|| dict.col.id_at(i))
                        .flatten()
                    {
                        Some(id) => keys.push(Some(i32::try_from(id).map_err(|_| {
                            DataFusionError::Internal(
                                "declared Str dictionary id exceeds i32".into(),
                            )
                        })?)),
                        None => keys.push(None),
                    }
                } else if let Some(stream_ref) = view.stream_ref(i) {
                    match resolver.fallback_value(view, cache, stream_ref)? {
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
            for i in start..end {
                // `appended` == "this row is decided, do not consult the
                // resource/scope fallback" (record wins over resource).
                let mut appended = false;
                if resolver.single_matching {
                    // Fused: one cursor read is both presence and value.
                    if let Some(s) = matching_str.and_then(|c| c.text_at(i)) {
                        values.append_value(s);
                        keys.push(Some(next));
                        next += 1;
                        appended = true;
                    }
                    // A `None` here (absent, or not UTF-8) falls through to
                    // the fallback, matching the row path.
                } else if let Some((win, text)) = plan.winner(i) {
                    // Record wins: the winning occurrence's `Str` cell, as the
                    // presence search validated it, or NULL when that winner
                    // sits in a different-typed column of the same key
                    // ([`DeclaredPlan::winning_idx`]).
                    match (Some(win) == plan.matching_idx).then_some(text).flatten() {
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
                    match resolver.fallback_value(view, cache, stream_ref)? {
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
    })
}

/// One synthetic per-key attribute column's FIELD_DIR resolution for a block
/// (issue #1768), the map-rendering sibling of [`DeclaredPlan`]. It resolves
/// every FIELD_DIR column of the key to a cursor once, so the row loop reads a
/// value with no per-cell column lookup.
struct AttrKeyPlan<'a> {
    key: String,
    /// The raw FIELD_DIR columns of this key, across all stored types.
    cols: Vec<AttrColumn>,
    /// Cursors parallel to [`Self::cols`], resolved once for the block.
    cursors: Vec<DeclaredCursor<'a>>,
}

impl<'a> AttrKeyPlan<'a> {
    fn build(view: &ColumnarBlockView<'a>, key: &str) -> AttrKeyPlan<'a> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(key).collect();
        let cursors = cols
            .iter()
            .map(|&c| DeclaredCursor::resolve(view, c))
            .collect();
        AttrKeyPlan {
            key: key.to_string(),
            cols,
            cursors,
        }
    }

    /// The index of the record's WINNING occurrence of the key at surviving row
    /// `i`, or `None` when the record sets the key in no FIELD_DIR column.
    /// Identical rule to [`DeclaredPlan::winning_idx`]: the highest FIELD_DIR
    /// type byte wins (the record layer's last-occurrence-wins order,
    /// docs/log-segment-format.md), and a `Str` cell that is not valid UTF-8 is
    /// not "present", so it does not shadow the resource/scope fallback. The
    /// `attrs_raw` overflow tier cannot appear here: a block carrying an
    /// `attrs_raw` page falls the whole segment back to the row path before this
    /// builds a column.
    fn winning_idx(&self, i: usize) -> Option<usize> {
        winner_at(&self.cols, &self.cursors, i).map(|(k, _)| k)
    }
}

/// Per-key, per-block resolver for a synthetic attribute column's merged value
/// rendered as text (issue #1768). Unlike [`DeclaredResolver`], which yields
/// NULL for a record value whose variant does not match the declared type, this
/// renders the WINNING value of any variant to text with
/// [`attr_value_to_string`], reproducing the `attrs` map's own rendering and its
/// divergence from a declared column. The resource/scope fallback is memoized
/// per `stream_ref` exactly as `DeclaredResolver` does.
struct AttrKeyResolver<'p, 'a> {
    plan: &'p AttrKeyPlan<'a>,
    fallback: HashMap<u32, Option<String>>,
}

impl<'p, 'a> AttrKeyResolver<'p, 'a> {
    fn new(plan: &'p AttrKeyPlan<'a>) -> Self {
        AttrKeyResolver {
            plan,
            fallback: HashMap::new(),
        }
    }

    /// The merged value of the key at surviving row `i`, rendered to text, or
    /// `None` (NULL) when the key is absent. The record's own winning
    /// occurrence wins over the resource/scope value; only a record that sets
    /// the key in no FIELD_DIR column consults the fallback.
    fn text_at(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<String>> {
        if let Some(win) = self.plan.winning_idx(i) {
            // A winning occurrence is present by construction, so `value_at` is
            // `Some`; render it to text like the `attrs` map column does.
            return Ok(self.plan.cursors[win]
                .value_at(i)
                .as_ref()
                .map(attr_value_to_string));
        }
        let Some(stream_ref) = view.stream_ref(i) else {
            return Ok(None);
        };
        match self.fallback.entry(stream_ref) {
            std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut().clone()),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resource = resource_attrs(view, cache, stream_ref)?;
                let value = find_attr(resource, &self.plan.key).map(attr_value_to_string);
                Ok(e.insert(value).clone())
            }
        }
    }
}

/// Build one synthetic per-key attribute column as a `Utf8` array for surviving
/// rows `start..end` straight from the view (issue #1768). Byte-identical to
/// [`attr_key_column_array`] over the same input.
fn build_attr_key_columnar_array(
    view: &ColumnarBlockView<'_>,
    resolver: &mut AttrKeyResolver<'_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let mut b = StringBuilder::new();
    for i in start..end {
        match resolver.text_at(view, i, cache)? {
            Some(s) => b.append_value(s),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
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
    attr_keys: &[String],
    full_len: usize,
    row_refs: Option<RowRefRange>,
) -> DFResult<Vec<RecordBatch>> {
    let n = view.surviving_count();
    // Resolve each projected declared column's FIELD_DIR columns to cursors once
    // for the whole block (ADR-0099 decision 2, #875), not per row and not per
    // chunk: the row loop then reads through the cursors with no column lookup.
    // A synthetic per-key column (index >= full_len) is out of `declared`'s
    // range, so this loop skips it; it gets its own plan below.
    let mut plans: HashMap<usize, DeclaredPlan<'_, '_>> = HashMap::new();
    for &idx in projection {
        if (FIRST_DECLARED_COL..full_len).contains(&idx)
            && let Some(dc) = declared.get(idx - FIRST_DECLARED_COL)
        {
            plans.insert(idx, DeclaredPlan::build(view, dc));
        }
    }
    // Synthetic per-key attribute columns (issue #1768): one plan per projected
    // per-key index, resolved once for the whole block like the declared plans.
    let mut attr_plans: HashMap<usize, AttrKeyPlan<'_>> = HashMap::new();
    for &idx in projection {
        if idx >= full_len
            && let Some(key) = attr_keys.get(idx - full_len)
        {
            attr_plans.insert(idx, AttrKeyPlan::build(view, key));
        }
    }
    // One resolver per declared column for the WHOLE block, not per chunk:
    // the fallback memo must survive across `BATCH_ROWS` chunk boundaries, or
    // a block larger than one chunk repeats the find_attr scan per chunk for
    // every stream that spans them.
    let mut resolvers: HashMap<usize, DeclaredResolver<'_, '_, '_>> = plans
        .iter()
        .map(|(idx, plan)| (*idx, DeclaredResolver::new(plan)))
        .collect();
    let mut attr_resolvers: HashMap<usize, AttrKeyResolver<'_, '_>> = attr_plans
        .iter()
        .map(|(idx, plan)| (*idx, AttrKeyResolver::new(plan)))
        .collect();
    let mut cache: HashMap<u32, Arc<Vec<(String, AttrValue)>>> = HashMap::new();
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + BATCH_ROWS).min(n);
        out.push(build_columnar_batch(
            view,
            schema,
            projection,
            &mut resolvers,
            &mut attr_resolvers,
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
    resolvers: &mut HashMap<usize, DeclaredResolver<'_, '_, '_>>,
    attr_resolvers: &mut HashMap<usize, AttrKeyResolver<'_, '_>>,
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
            other => {
                if let Some(resolver) = resolvers.get_mut(&other) {
                    build_declared_columnar_array(view, resolver, start, end, cache)?
                } else if let Some(resolver) = attr_resolvers.get_mut(&other) {
                    build_attr_key_columnar_array(view, resolver, start, end, cache)?
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "logs columnar scan projection index {other} out of range"
                    )));
                }
            }
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
mod cstat_reconcile_tests {
    //! The ADR-0873 clause 4 row-accounting gate at the three exact aggregate
    //! paths (#1037), pinned at the granularity the drop metric is defined on:
    //! ONE read of the entry.
    //!
    //! These call [`LogsScanExec::declared_not_equal_count`],
    //! [`LogsScanExec::declared_group_counts`] and
    //! [`LogsScanExec::declared_column_sum`] directly rather than through a
    //! statement, because a planned statement also resolves
    //! `partition_statistics`, which reads the same entry again through
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
mod columnar_lookup_tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, Int64Array};
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{
        ColumnSelection, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter,
    };
    use ravel_types::logstream::LogStreamId;

    pub(super) fn sid(n: u8) -> LogStreamId {
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
        let arr = build_declared_columnar_array(
            &view,
            &mut DeclaredResolver::new(&plan),
            0,
            rows,
            &mut cache,
        )
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

    /// A record on stream `stream`, with the given resource scalars and dynamic
    /// per-record attributes.
    fn rec(
        stream: u8,
        ts: i64,
        resource: &[(&str, AttrValue)],
        attrs: &[(&str, AttrValue)],
    ) -> LogRecord {
        let res: Vec<(String, AttrValue)> = resource
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_attrs_bytes(&res, "scope", "1", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        }
    }

    /// Write `records` as one RLOG object and open a fresh scan over the whole
    /// object with no predicate.
    fn write_and_scan<'o>(
        records: &[LogRecord],
        cfg: &RlogConfig,
        obj: &'o mut Vec<u8>,
    ) -> RlogReader<'o> {
        let mut w = RlogWriter::new(
            *cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        for r in records {
            w.push(r.clone()).expect("push");
        }
        *obj = w.finish().expect("finish");
        RlogReader::new(obj, cfg).expect("open")
    }

    /// Byte-identity oracle at block granularity: `build_declared_columnar_array`
    /// must produce the exact column `declared_column_array` builds from the row
    /// path's merged view over the SAME block, across every branch of the merge in
    /// one input.
    ///
    /// The input exercises, for declared I64 key `k`: a record that sets `k` at
    /// the declared type (record wins), a record that sets `k` at a DIFFERENT type
    /// while its stream carries `k` in resource (must be NULL and must NOT fall
    /// through to the resource value: ADR-0090 decision 7), two rows of one stream
    /// whose records do not set `k` and whose stream has `k` in resource (fallback
    /// value, memoized once), and a row whose record does not set `k` and whose
    /// stream has no `k` (NULL). Both paths read the block in the same surviving
    /// order (`next_block` and `next_block_columnar` are documented to), so the
    /// two arrays compare element by element.
    ///
    /// Failing-against-pre-change was demonstrated by flipping the
    /// `record_sets_key` arm of `DeclaredResolver::merged_value` to fall through
    /// to the fallback when the declared-type cell is `None`: the different-typed
    /// row then reads its stream's resource `k=777` instead of NULL, and both the
    /// element-wise and the sorted-multiset assertions below fail.
    #[test]
    fn columnar_declared_column_is_byte_identical_to_row_path() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        // stream 3: resource has k=I64(777). Its two records set k -- once at the
        // declared type (wins), once at a different type (NULL, decision 7).
        // stream 1: resource has k=I64(999); two records set nothing (fallback).
        // stream 2: no resource k; one record sets nothing (NULL fallback).
        let records = vec![
            rec(
                3,
                100,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(777)),
                ],
                &[("k", AttrValue::I64(10))],
            ),
            rec(
                3,
                101,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(777)),
                ],
                &[("k", AttrValue::Str("x".into()))],
            ),
            rec(
                1,
                102,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(999)),
                ],
                &[],
            ),
            rec(
                1,
                103,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(999)),
                ],
                &[],
            ),
            rec(2, 104, &[("svc", AttrValue::Str("b".into()))], &[]),
        ];
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        // Row path: decode the block's records in surviving order, merge, build.
        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let block = scan.next_block(&obj).expect("row exit").expect("one block");
        assert!(
            scan.next_block(&obj).expect("row exit").is_none(),
            "the input fits one block"
        );
        let merged: Vec<Vec<(String, AttrValue)>> = block
            .iter()
            .map(|r| merged_attrs(r).expect("merge"))
            .collect();
        let row_arr = declared_column_array(&dc, &merged);
        let row = row_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row I64 array");

        // Columnar path over the same object.
        let mut obj2 = Vec::new();
        let reader2 = write_and_scan(&records, &cfg, &mut obj2);
        let mut scan2 = reader2
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan2
            .next_block_columnar(&obj2)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        let plan = DeclaredPlan::build(&view, &dc);
        assert!(
            !plan.single_matching(),
            "the key has an I64 and a Str FIELD_DIR column, so the multi-column \
             precedence path is what runs"
        );
        let mut cache = HashMap::new();
        let col_arr = build_declared_columnar_array(
            &view,
            &mut DeclaredResolver::new(&plan),
            0,
            rows,
            &mut cache,
        )
        .expect("columnar array");
        let col = col_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columnar I64 array");

        // Byte-identity: same length, same null bitmap, same values, in order.
        assert_eq!(col.len(), row.len(), "row counts match");
        assert_eq!(col.len(), 5, "all five records survive in one block");
        for i in 0..col.len() {
            assert_eq!(col.is_null(i), row.is_null(i), "null bit at row {i}");
            if !col.is_null(i) {
                assert_eq!(col.value(i), row.value(i), "value at row {i}");
            }
        }

        // Pin the semantic outcome independent of block ordering: the different-
        // typed record is NULL (not 777), both fallbacks are 999, the record-wins
        // row is 10, the no-resource row is NULL. If decision 7 were violated the
        // second NULL would become 777 and this multiset would not match.
        let mut got: Vec<Option<i64>> = (0..col.len())
            .map(|i| (!col.is_null(i)).then(|| col.value(i)))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![None, None, Some(10), Some(999), Some(999)],
            "declared column merge outcome"
        );
    }

    /// The fallback resolve runs once per DISTINCT stream ref in the block, not
    /// once per row: `DeclaredResolver::resolve_count` equals the number of
    /// distinct streams, never the row count. Three streams (2 + 2 + 1 rows),
    /// none setting the key, so every row reaches the fallback; the memo makes the
    /// scan run exactly three times.
    ///
    /// Failing-against-pre-change was demonstrated by resolving per row (dropping
    /// the memo and incrementing on every call): the count then reads 5, the row
    /// count, and the `== 3` assertion fails.
    #[test]
    fn fallback_resolves_once_per_distinct_stream() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let records = vec![
            rec(
                1,
                100,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(1)),
                ],
                &[],
            ),
            rec(
                1,
                101,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(1)),
                ],
                &[],
            ),
            rec(
                2,
                102,
                &[
                    ("svc", AttrValue::Str("b".into())),
                    ("k", AttrValue::I64(2)),
                ],
                &[],
            ),
            rec(
                2,
                103,
                &[
                    ("svc", AttrValue::Str("b".into())),
                    ("k", AttrValue::I64(2)),
                ],
                &[],
            ),
            rec(
                3,
                104,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(3)),
                ],
                &[],
            ),
        ];
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 5, "five surviving rows");

        let plan = DeclaredPlan::build(&view, &dc);
        // No record sets `k`, so the fallback is consulted for every row and the
        // resolve count is purely a function of the memoization.
        let mut streams = std::collections::HashSet::new();
        for i in 0..rows {
            streams.insert(view.stream_ref(i).expect("stream ref"));
        }
        assert_eq!(streams.len(), 3, "three distinct streams in the block");

        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        for i in 0..rows {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        assert_eq!(
            resolver.resolve_count, 3,
            "the fallback scan runs once per distinct stream ref, not once per row"
        );
    }

    /// The fallback memo survives across `BATCH_ROWS` chunk boundaries: the
    /// resolver set is built once per BLOCK in `build_columnar_batches`, so a
    /// block larger than one chunk does not repeat the `find_attr` scan for a
    /// stream that spans chunks. This drives `build_columnar_batch` twice over
    /// the same resolver map, exactly as the chunk loop does, and pins the
    /// TOTAL resolve count to the distinct-stream count, not
    /// distinct-streams-per-chunk summed.
    ///
    /// Failing-against-pre-change was demonstrated by rebuilding the resolver
    /// map between the two calls (the per-chunk construction this fixes): the
    /// count then reads 6 (3 streams x 2 chunks) and the `== 3` fails.
    #[test]
    fn fallback_memo_survives_chunk_boundaries() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        // Six rows, three streams, every stream present in BOTH halves.
        let mut records = Vec::new();
        for half in 0..2u8 {
            for stream in 1..=3u8 {
                records.push(rec(
                    stream,
                    100 + i64::from(half) * 10 + i64::from(stream),
                    &[
                        ("svc", AttrValue::Str(format!("s{stream}"))),
                        ("k", AttrValue::I64(i64::from(stream))),
                    ],
                    &[],
                ));
            }
        }
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 6, "six surviving rows");

        let plan = DeclaredPlan::build(&view, &dc);
        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        // Two chunk-shaped passes over ONE resolver, as build_columnar_batches
        // now does; the memo carries from the first half into the second.
        for i in 0..3 {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        for i in 3..rows {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        assert_eq!(
            resolver.resolve_count, 3,
            "one scan per distinct stream across BOTH chunks; a per-chunk \
             resolver would read 6"
        );
    }

    /// One declared column's cells rendered to a form the two paths can be
    /// compared in: `None` for NULL, otherwise the value's text (lowercase hex
    /// for `bytes`). `Str` is a `Dictionary(Int32, Utf8)` on both paths, so its
    /// keys are resolved through the dictionary values here rather than
    /// compared as ids: the two paths build different dictionaries for the same
    /// logical column and only the logical values must match.
    pub(super) fn declared_cells(arr: &ArrayRef, ty: DeclaredType) -> Vec<Option<String>> {
        match ty {
            DeclaredType::Str => {
                let d = arr
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("declared Str dictionary array");
                let values = d
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 dictionary values");
                (0..d.len())
                    .map(|i| {
                        if d.is_null(i) {
                            return None;
                        }
                        let k = usize::try_from(d.keys().value(i)).expect("non-negative key");
                        (!values.is_null(k)).then(|| values.value(k).to_string())
                    })
                    .collect()
            }
            DeclaredType::I64 => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("declared I64 array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                    .collect()
            }
            DeclaredType::Bool => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("declared Bool array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                    .collect()
            }
            DeclaredType::Bytes => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("declared Bytes array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| hex::encode(a.value(i))))
                    .collect()
            }
        }
    }

    /// Issue #1182, the reader half: a record carrying one attribute name more
    /// than once resolves that name to ONE value, and both reader paths must
    /// pick the same one.
    ///
    /// The rule is docs/log-segment-format.md, "Within the record layer": the
    /// record's columnar occurrences ascending by FIELD_DIR type byte
    /// (str=1 i64=2 f64=3 bool=4 bytes=5), then its `attrs_raw` overflow
    /// occurrences ascending by canonical encoded value bytes, last entry wins.
    /// It is NOT the record's write order, which the on-disk format does not
    /// preserve. So among the columnar occurrences the HIGHEST type byte wins,
    /// and the two records below that carry the same pair in opposite write
    /// orders resolve identically.
    ///
    /// Every row is asserted against a literal expected value for each of the
    /// four declarable types, so a drift names both the consumer and the type.
    /// The two consumers this can reach in-crate are the row path
    /// (`declared_column_array` over `merged_attrs` + `find_attr`, already
    /// correct before #1182) and the columnar fast path
    /// (`DeclaredResolver::merged_value` + `build_declared_str_columnar`, which
    /// was first-occurrence-per-(name, type)-wins). The other two consumers of
    /// the same rule live in crates this one does not depend on and pin the
    /// SAME table of records and expectations:
    /// `ravel_ingest::log_declared_stats::tests::duplicate_key_stamp_follows_the_readers_rule`
    /// and `ravel_maintain::rlog::tests::duplicate_key_stamp_follows_the_readers_rule`.
    ///
    /// Prove-the-test: restoring `merged_value`'s `record_sets_key` arm
    /// (`return Ok(self.matching_cursor.and_then(|c| c.value_at(i)))`) makes the
    /// declared I64 column read 9 at ts 102 and 1 at ts 103 where the row path
    /// reads NULL; restoring either `record_sets_key` arm of
    /// `build_declared_str_columnar` makes the declared Str column read "x" at
    /// ts 100 and 101 where the row path reads NULL.
    #[test]
    fn a_duplicate_key_resolves_last_occurrence_wins_on_both_reader_paths() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 16,
            ..RlogConfig::default()
        };
        let res = [("svc", AttrValue::Str("a".into()))];
        let records = vec![
            // I64 written first, Str second: i64 (2) > str (1), so `k` is 5 and
            // a declared Str column reads NULL, not "x".
            rec(
                1,
                100,
                &res,
                &[("k", AttrValue::I64(5)), ("k", AttrValue::Str("x".into()))],
            ),
            // The same pair in the opposite write order: same winner.
            rec(
                1,
                101,
                &res,
                &[("k", AttrValue::Str("x".into())), ("k", AttrValue::I64(5))],
            ),
            // bool (4) > i64 (2): a declared I64 column reads NULL, not 9.
            rec(
                1,
                102,
                &res,
                &[("k", AttrValue::Bool(true)), ("k", AttrValue::I64(9))],
            ),
            // bytes (5) beats every other type.
            rec(
                1,
                103,
                &res,
                &[
                    ("k", AttrValue::I64(1)),
                    ("k", AttrValue::Bytes(vec![0xab])),
                ],
            ),
            // A single occurrence resolves to itself: the Str column is not
            // simply always NULL, and the I64 column is not simply always set.
            rec(1, 104, &res, &[("k", AttrValue::Str("y".into()))]),
        ];
        // The merged view every consumer must agree on, per record, keyed by ts.
        let want_merged: Vec<(i64, AttrValue)> = vec![
            (100, AttrValue::I64(5)),
            (101, AttrValue::I64(5)),
            (102, AttrValue::Bool(true)),
            (103, AttrValue::Bytes(vec![0xab])),
            (104, AttrValue::Str("y".into())),
        ];
        // The same view projected onto each declarable type: a value of another
        // variant is NULL, never a cast (ADR-0090 decision 7).
        let want_str = [None, None, None, None, Some("y".to_string())];
        let want_i64 = [
            Some("5".to_string()),
            Some("5".to_string()),
            None,
            None,
            None,
        ];
        let want_bool = [None, None, Some("true".to_string()), None, None];
        let want_bytes = [None, None, None, Some("ab".to_string()), None];

        // Row path (consumer 1, correct before #1182): decode the block's
        // records, merge, build.
        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let block = scan.next_block(&obj).expect("row exit").expect("one block");
        assert!(
            scan.next_block(&obj).expect("row exit").is_none(),
            "the input fits one block"
        );
        let merged: Vec<Vec<(String, AttrValue)>> = block
            .iter()
            .map(|r| merged_attrs(r).expect("merge"))
            .collect();
        // Consumer 1, field by field: `merged_attrs` + `find_attr` resolve the
        // winner the format doc pins, for the record with that ts.
        assert_eq!(block.len(), want_merged.len(), "every record survives");
        for (r, row) in block.iter().zip(merged.iter()) {
            let (_, want) = want_merged
                .iter()
                .find(|(ts, _)| *ts == r.ts_ns)
                .expect("a record per expected ts");
            assert_eq!(
                find_attr(row, "k"),
                Some(want),
                "row path merged view at ts {}",
                r.ts_ns
            );
        }

        // Columnar fast path (consumer 2) over the same object.
        let mut obj2 = Vec::new();
        let reader2 = write_and_scan(&records, &cfg, &mut obj2);
        let mut scan2 = reader2
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan2
            .next_block_columnar(&obj2)
            .expect("columnar exit")
            .expect("one block");
        assert!(
            !view.has_attrs_raw_page(),
            "no occurrence overflows, so the columnar path is what a scan takes \
             here rather than falling back to the row path"
        );
        let rows = view.surviving_count();
        assert_eq!(rows, records.len(), "all records survive in one block");

        for (ty, want) in [
            (DeclaredType::Str, &want_str),
            (DeclaredType::I64, &want_i64),
            (DeclaredType::Bool, &want_bool),
            (DeclaredType::Bytes, &want_bytes),
        ] {
            let dc = DeclaredColumn::new("k", ty);
            let plan = DeclaredPlan::build(&view, &dc);
            assert!(
                !plan.single_matching(),
                "{ty:?}: `k` has a Str, I64, Bool and Bytes FIELD_DIR column, so \
                 the multi-column precedence arm is what runs"
            );
            let mut cache = HashMap::new();
            let col = declared_cells(
                &build_declared_columnar_array(
                    &view,
                    &mut DeclaredResolver::new(&plan),
                    0,
                    rows,
                    &mut cache,
                )
                .expect("columnar array"),
                ty,
            );
            let row = declared_cells(&declared_column_array(&dc, &merged), ty);

            // Consumer 1 against its literal expectation, in block order.
            for (i, r) in block.iter().enumerate() {
                let k = want_merged
                    .iter()
                    .position(|(ts, _)| *ts == r.ts_ns)
                    .expect("a record per expected ts");
                assert_eq!(
                    row[i], want[k],
                    "{ty:?}: row path at ts {} (block position {i})",
                    r.ts_ns
                );
            }
            // Consumer 2 against consumer 1, cell by cell. Both paths read the
            // block's surviving rows in the same order, so this compares the
            // same record on both sides.
            assert_eq!(
                col.len(),
                row.len(),
                "{ty:?}: the two paths build the same row count"
            );
            for i in 0..row.len() {
                assert_eq!(
                    col[i], row[i],
                    "{ty:?}: columnar path disagrees with the row path at block \
                     position {i}"
                );
            }
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    //! ADR-2121 D2 and D3: the declared-column builds read a single-column
    //! block's cells with no per-cell `AttrValue` and validate a `Str` cell's
    //! UTF-8 at most once per block, and still build the arrays the per-cell
    //! path built.
    //!
    //! `RlogWriter` only writes valid UTF-8 `Str` cells, so every fixture here
    //! is a writer-produced object whose one block is rewritten with the cells
    //! a test names, through the writer's own `write_block` and block layout.

    use super::columnar_lookup_tests::{declared_cells, sid};
    use super::*;
    use datafusion::arrow::array::{BinaryArray, BooleanArray, Int64Array};
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use ravel_logseg::block::{ColumnPlan, write_block};
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::footer::{
        COMP_NONE, LogFooter, SectionDesc, kind, open as open_footer, write_footer_and_trailer,
    };
    use ravel_logseg::reader::read_section;
    use ravel_logseg::record::{ColumnValue, ResolvedRow, stream_attrs_bytes};
    use ravel_logseg::skip_index::SkipIndex;
    use ravel_logseg::writer::BlocksBuilder;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};

    const KEY: &str = "k";
    const STREAMS: usize = 3;
    /// Every occurrence type a key can be stored under; a generated block's key
    /// has a FIELD_DIR column for a subset of them.
    const TYPES: [FieldType; 5] = [
        FieldType::Str,
        FieldType::I64,
        FieldType::F64,
        FieldType::Bool,
        FieldType::Bytes,
    ];
    const DECLARED: [DeclaredType; 4] = [
        DeclaredType::Str,
        DeclaredType::I64,
        DeclaredType::Bool,
        DeclaredType::Bytes,
    ];
    /// `Str` cells of a dictionary-heavy block: two valid values and two that
    /// are not UTF-8, so a page dictionary carries non-UTF-8 entries.
    const STR_POOL: [&[u8]; 4] = [b"a", b"bc", &[b'b', 0xff], &[0xc3]];

    fn cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 256,
            max_dynamic_columns: 16,
            ..RlogConfig::default()
        }
    }

    /// One block row: its stream and its record cell per entry of [`TYPES`]. A
    /// cell of a type the block's key has no column for is not written.
    #[derive(Clone, Debug)]
    struct Row {
        stream: usize,
        cells: Vec<Option<ColumnValue>>,
    }

    #[derive(Clone, Debug)]
    struct Block {
        /// The types the key has a FIELD_DIR column for.
        types: Vec<FieldType>,
        rows: Vec<Row>,
        /// Per stream: the key's resource value and its scope value.
        overlays: Vec<(Option<AttrValue>, Option<AttrValue>)>,
        /// Rows `lo..=hi` survive the scan's content predicate.
        lo: usize,
        hi: usize,
        /// Where the surviving rows split into two builds, in percent.
        split_pct: usize,
    }

    /// A valid seed value of each type, so the writer's FIELD_DIR carries a
    /// column for it.
    fn seed_value(ty: FieldType) -> AttrValue {
        match ty {
            FieldType::Str => AttrValue::Str("seed".into()),
            FieldType::I64 => AttrValue::I64(0),
            FieldType::F64 => AttrValue::F64(0.5),
            FieldType::Bool => AttrValue::Bool(false),
            FieldType::Bytes => AttrValue::Bytes(vec![9]),
        }
    }

    /// Write `block` as one RLOG object: the writer lays out STREAM_DIR,
    /// FIELD_DIR and the footer from seed records (row `r` on stream `r % 3`,
    /// setting the key at every type in `block.types`), then the block itself is
    /// replaced with `block.rows` verbatim, non-UTF-8 `Str` cells included.
    fn encode(block: &Block, cfg: &RlogConfig) -> Vec<u8> {
        assert!(
            block.rows.len() >= STREAMS,
            "every stream has a seed record"
        );
        let stream_attrs = |s: usize| {
            let (resource, scope) = &block.overlays[s];
            let mut res = vec![("svc".to_string(), AttrValue::Str("s".into()))];
            res.extend(resource.iter().map(|v| (KEY.to_string(), v.clone())));
            let scope: Vec<(String, AttrValue)> =
                scope.iter().map(|v| (KEY.to_string(), v.clone())).collect();
            stream_attrs_bytes(&res, "scope", "1", &scope)
        };
        let mut w = RlogWriter::new(
            *cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        for r in 0..block.rows.len() {
            let s = r % STREAMS;
            w.push(LogRecord {
                stream_id: sid(s as u8),
                stream_attrs: stream_attrs(s),
                ts_ns: r as i64 * 10,
                observed_ts_ns: r as i64 * 10,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: block
                    .types
                    .iter()
                    .map(|&ty| (KEY.to_string(), seed_value(ty)))
                    .collect(),
            })
            .expect("push");
        }
        let obj = w.finish().expect("finish");

        let footer = open_footer(&obj).expect("open the writer's footer");
        let desc = *footer.section(kind::FIELD_DIR).expect("FIELD_DIR");
        let dir = FieldDir::decode(&read_section(&obj, &desc, cfg).expect("read"), u64::MAX)
            .expect("decode FIELD_DIR");
        let mut plans: Vec<(usize, ColumnPlan)> = block
            .types
            .iter()
            .map(|&ty| {
                let t = TYPES.iter().position(|&x| x == ty).expect("a known type");
                let column_id = dir.column(KEY, ty).expect("a column per type").column_id;
                (t, ColumnPlan { column_id, ty })
            })
            .collect();
        plans.sort_by_key(|(_, p)| p.column_id);
        let rows: Vec<ResolvedRow> = block
            .rows
            .iter()
            .enumerate()
            .map(|(r, row)| ResolvedRow {
                stream_ref: row.stream as u32,
                ts_ns: r as i64 * 10,
                observed_ts_ns: r as i64 * 10,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs_raw: None,
                columns: plans
                    .iter()
                    .filter_map(|(t, p)| row.cells[*t].clone().map(|v| (p.column_id, v)))
                    .collect(),
                indexed_terms: Vec::new(),
                stat_winners: Vec::new(),
            })
            .collect();
        let plans: Vec<ColumnPlan> = plans.into_iter().map(|(_, p)| p).collect();
        let written = write_block(&rows, &plans, cfg.zstd_level).expect("write one block");
        let mut layout = BlocksBuilder::version_4(cfg.group_target_blocks);
        layout.push(written);
        let (blocks_bytes, l0, page_dir) = layout.finish();
        let skip = SkipIndex::build(l0).encode();
        let page_dir_bytes = page_dir.encode();

        let mut out: Vec<u8> = Vec::new();
        let mut sections: Vec<SectionDesc> = Vec::new();
        for d in &footer.sections {
            let (bytes, comp, uncomp_len) = match d.kind {
                kind::BLOCKS => (blocks_bytes.clone(), COMP_NONE, blocks_bytes.len() as u64),
                kind::SKIP_IDX => (skip.clone(), COMP_NONE, skip.len() as u64),
                kind::PAGE_DIR => (
                    page_dir_bytes.clone(),
                    COMP_NONE,
                    page_dir_bytes.len() as u64,
                ),
                _ => {
                    let start = usize::try_from(d.offset).expect("offset fits");
                    let end = start + usize::try_from(d.len).expect("len fits");
                    (obj[start..end].to_vec(), d.comp, d.uncomp_len)
                }
            };
            sections.push(SectionDesc {
                kind: d.kind,
                offset: out.len() as u64,
                len: bytes.len() as u64,
                crc32c: crc32c::crc32c(&bytes),
                comp,
                uncomp_len,
            });
            out.extend_from_slice(&bytes);
        }
        let patched = LogFooter { sections, ..footer };
        write_footer_and_trailer(&mut out, &patched);
        out
    }

    /// The content predicate that keeps rows `lo..=hi`.
    fn window(block: &Block) -> Predicate {
        Predicate::TsRange {
            min_ns: block.lo as i64 * 10,
            max_ns: block.hi as i64 * 10,
        }
    }

    // --- the pre-change per-cell path, rebuilt from the view's own accessors --

    /// Whether the record sets the key in `col` at row `i`: a `Str` cell counts
    /// only when it is UTF-8.
    fn ref_present(view: &ColumnarBlockView<'_>, col: AttrColumn, i: usize) -> bool {
        match col.ty {
            FieldType::Str => view.bytes_cursor(col.column_id).str_at(i).is_some(),
            FieldType::Bytes => view.bytes_cursor(col.column_id).at(i).is_some(),
            FieldType::I64 => view.i64_cursor(col.column_id).at(i).is_some(),
            FieldType::F64 => view.f64_bits_cursor(col.column_id).at(i).is_some(),
            FieldType::Bool => view.bool_cursor(col.column_id).at(i).is_some(),
        }
    }

    /// The record's winning occurrence at row `i`: the highest type byte, the
    /// last one on a tie.
    fn ref_winner(view: &ColumnarBlockView<'_>, cols: &[AttrColumn], i: usize) -> Option<usize> {
        cols.iter()
            .enumerate()
            .filter(|(_, c)| ref_present(view, **c, i))
            .max_by_key(|(_, c)| c.ty.to_u8())
            .map(|(k, _)| k)
    }

    /// The row's stream's resource/scope value of the key.
    fn ref_fallback(view: &ColumnarBlockView<'_>, i: usize) -> Option<AttrValue> {
        let blob = view.stream_attrs(i)?;
        let attrs = decode_stream_attrs(blob).expect("stream attrs");
        find_attr(&attrs, KEY).cloned()
    }

    /// The merged value at row `i` with its variant not yet checked: the record's
    /// winning cell when it is of the declared type, NULL when the winner is of
    /// another type, and the fallback only when the record does not set the key.
    fn ref_merged(
        view: &ColumnarBlockView<'_>,
        dc: &DeclaredColumn,
        i: usize,
    ) -> Option<AttrValue> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
        let Some(win) = ref_winner(view, &cols, i) else {
            return ref_fallback(view, i);
        };
        let col = cols[win];
        if col.ty != declared_field_type(dc.ty) {
            return None;
        }
        match col.ty {
            FieldType::Str => view
                .bytes_cursor(col.column_id)
                .str_at(i)
                .map(|s| AttrValue::Str(s.to_string())),
            FieldType::I64 => view.i64_cursor(col.column_id).at(i).map(AttrValue::I64),
            FieldType::Bool => view.bool_cursor(col.column_id).at(i).map(AttrValue::Bool),
            FieldType::Bytes => view
                .bytes_cursor(col.column_id)
                .at(i)
                .map(|b| AttrValue::Bytes(b.to_vec())),
            FieldType::F64 => None,
        }
    }

    /// The array the pre-change build produced for rows `start..end`, down to
    /// the `Str` dictionary's layout: a dictionary page's entries in page order
    /// (NULL for a non-UTF-8 entry) with fallback values appended past them, or
    /// an identity dictionary over a plain page.
    fn per_cell_reference(
        view: &ColumnarBlockView<'_>,
        dc: &DeclaredColumn,
        start: usize,
        end: usize,
    ) -> ArrayRef {
        match dc.ty {
            DeclaredType::I64 => Arc::new(Int64Array::from(
                (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::I64(v)) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DeclaredType::Bool => Arc::new(BooleanArray::from(
                (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::Bool(v)) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DeclaredType::Bytes => {
                let cells: Vec<Option<Vec<u8>>> = (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::Bytes(b)) => Some(b),
                        _ => None,
                    })
                    .collect();
                Arc::new(BinaryArray::from(
                    cells.iter().map(|c| c.as_deref()).collect::<Vec<_>>(),
                ))
            }
            DeclaredType::Str => {
                let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
                let str_col = cols.iter().find(|c| c.ty == FieldType::Str);
                let mut values: Vec<Option<String>> = Vec::new();
                let mut keys: Vec<Option<i32>> = Vec::new();
                match str_col.and_then(|c| view.str_dict(c.column_id)) {
                    Some(page) => {
                        values.extend(
                            page.dict()
                                .iter()
                                .map(|v| std::str::from_utf8(v).ok().map(str::to_string)),
                        );
                        for i in start..end {
                            let key = match ref_winner(view, &cols, i) {
                                Some(win) if cols[win].ty == FieldType::Str => {
                                    page.id_at(i).map(|id| id as i32)
                                }
                                Some(_) => None,
                                None => match ref_fallback(view, i) {
                                    Some(AttrValue::Str(s)) => {
                                        values.push(Some(s));
                                        Some(values.len() as i32 - 1)
                                    }
                                    _ => None,
                                },
                            };
                            keys.push(key);
                        }
                    }
                    None => {
                        for i in start..end {
                            keys.push(match ref_merged(view, dc, i) {
                                Some(AttrValue::Str(s)) => {
                                    values.push(Some(s));
                                    Some(values.len() as i32 - 1)
                                }
                                _ => None,
                            });
                        }
                    }
                }
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(keys),
                        Arc::new(StringArray::from(values)),
                    )
                    .expect("reference dictionary"),
                )
            }
        }
    }

    // --- generated blocks ---------------------------------------------------

    fn str_cell(pooled: bool) -> BoxedStrategy<Option<ColumnValue>> {
        if pooled {
            prop_oneof![
                Just(None),
                prop::sample::select(STR_POOL.to_vec())
                    .prop_map(|b| Some(ColumnValue::Str(b.to_vec()))),
            ]
            .boxed()
        } else {
            prop_oneof![
                Just(None),
                "[a-d]{0,3}".prop_map(|s| Some(ColumnValue::Str(s.into_bytes()))),
                prop::collection::vec(any::<u8>(), 1..4).prop_map(|b| Some(ColumnValue::Str(b))),
            ]
            .boxed()
        }
    }

    fn row(pooled: bool) -> impl Strategy<Value = Row> {
        (
            0..STREAMS,
            str_cell(pooled),
            prop::option::of((-3i64..3).prop_map(ColumnValue::I64)),
            prop::option::of(any::<u64>().prop_map(ColumnValue::F64)),
            prop::option::of(any::<bool>().prop_map(ColumnValue::Bool)),
            prop::option::of(prop::collection::vec(any::<u8>(), 0..3).prop_map(ColumnValue::Bytes)),
        )
            .prop_map(|(stream, s, i, f, b, y)| Row {
                stream,
                cells: vec![s, i, f, b, y],
            })
    }

    fn overlay_value(ty: FieldType) -> BoxedStrategy<AttrValue> {
        match ty {
            FieldType::Str => prop::sample::select(vec!["r", "a"])
                .prop_map(|s| AttrValue::Str(s.into()))
                .boxed(),
            FieldType::I64 => (-2i64..2).prop_map(AttrValue::I64).boxed(),
            FieldType::F64 => Just(AttrValue::F64(1.5)).boxed(),
            FieldType::Bool => any::<bool>().prop_map(AttrValue::Bool).boxed(),
            FieldType::Bytes => Just(AttrValue::Bytes(vec![7, 7])).boxed(),
        }
    }

    /// A resource or scope value of one of `types`, or none. The writer gives
    /// the key a FIELD_DIR column for each type a stream carries it at, so a
    /// block whose overlays stay inside the record's types keeps a
    /// single-column key single.
    fn overlay(types: Vec<FieldType>) -> BoxedStrategy<Option<AttrValue>> {
        if types.is_empty() {
            return Just(None).boxed();
        }
        prop_oneof![
            Just(None),
            prop::sample::select(types).prop_flat_map(|ty| overlay_value(ty).prop_map(Some)),
        ]
        .boxed()
    }

    fn block() -> impl Strategy<Value = Block> {
        (
            prop::collection::vec(any::<bool>(), TYPES.len()),
            any::<bool>(),
            any::<bool>(),
            STREAMS..40usize,
        )
            .prop_flat_map(|(mask, pooled, narrow, n)| {
                let types: Vec<FieldType> = TYPES
                    .iter()
                    .zip(&mask)
                    .filter(|(_, m)| **m)
                    .map(|(t, _)| *t)
                    .collect();
                let overlay_types = if narrow {
                    types.clone()
                } else {
                    TYPES.to_vec()
                };
                (
                    Just(types),
                    prop::collection::vec(row(pooled), n),
                    prop::collection::vec(
                        (overlay(overlay_types.clone()), overlay(overlay_types)),
                        STREAMS,
                    ),
                    0..n,
                    0..n,
                    0usize..=100,
                )
            })
            .prop_map(|(types, rows, overlays, a, b, split_pct)| Block {
                types,
                rows,
                overlays,
                lo: a.min(b),
                hi: a.max(b),
                split_pct,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// ADR-2121 D2 and D3's contract: for every declarable type, the
        /// declared-column build over a generated block equals the pre-change
        /// per-cell build ([`per_cell_reference`]) array for array, chunk by
        /// chunk with one resolver across the chunks as the scan uses it, and
        /// equals the row path's `declared_column_array` cell for cell.
        ///
        /// The generated blocks carry resource and scope values of every type,
        /// NULL cells, one key under up to five types, a surviving-row window,
        /// non-UTF-8 `Str` cells on plain pages and non-UTF-8 entries in page
        /// dictionaries ([`the_block_generator_reaches_every_case_the_property_names`]).
        ///
        /// Flipped assertions: each mutation below fails the chunk comparison
        /// against the reference, first on an `I64` column:
        /// - the single-cursor gather taken without `single_matching`
        ///   (`single_cursor` returning `matching_cursor` alone): a row whose
        ///   winning occurrence is of another type reads the declared cell or
        ///   the fallback instead of NULL;
        /// - an absent cell appended as NULL instead of reading the
        ///   resource/scope value in the gather arms;
        /// - `Str` presence reduced to "bytes present" (a `Str` cell with bytes
        ///   is `Presence::Present` even when they are not UTF-8): a non-UTF-8
        ///   cell stops falling through to the resource/scope value.
        #[test]
        fn the_page_fast_path_builds_the_same_array_as_the_per_cell_path(block in block()) {
            let cfg = cfg();
            let obj = encode(&block, &cfg);
            let reader = RlogReader::new(&obj, &cfg).expect("open");
            let pred = window(&block);

            let mut rows = reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("scan");
            let records = rows.next_block(&obj).expect("row exit").expect("one block");
            let merged: Vec<Vec<(String, AttrValue)>> = records
                .iter()
                .map(|r| merged_attrs(r).expect("merge"))
                .collect();

            let mut scan = reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("scan");
            let view = scan
                .next_block_columnar(&obj)
                .expect("columnar exit")
                .expect("one block");
            let n = view.surviving_count();
            prop_assert_eq!(n, block.hi - block.lo + 1);
            prop_assert_eq!(records.len(), n);
            let split = n * block.split_pct / 100;

            for ty in DECLARED {
                let dc = DeclaredColumn::new(KEY, ty);
                let plan = DeclaredPlan::build(&view, &dc);
                let mut resolver = DeclaredResolver::new(&plan);
                let mut cache = HashMap::new();
                let mut cells = Vec::with_capacity(n);
                for (start, end) in [(0, split), (split, n)] {
                    let built =
                        build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                            .expect("declared array");
                    let reference = per_cell_reference(&view, &dc, start, end);
                    prop_assert_eq!(
                        built.to_data(),
                        reference.to_data(),
                        "{:?} rows {}..{}",
                        ty,
                        start,
                        end
                    );
                    cells.extend(declared_cells(&built, ty));
                }
                let row_path = declared_cells(&declared_column_array(&dc, &merged), ty);
                prop_assert_eq!(cells, row_path, "{:?} against the row path", ty);
            }
        }
    }

    /// The generator is not vacuous: over the deterministic runner's first 256
    /// blocks it produces every case the property names.
    #[test]
    fn the_block_generator_reaches_every_case_the_property_names() {
        let cfg = cfg();
        let mut runner = TestRunner::deterministic();
        let strategy = block();
        let mut seen: BTreeSet<&'static str> = BTreeSet::new();
        for _ in 0..256 {
            let block = strategy.new_tree(&mut runner).expect("a block").current();
            let obj = encode(&block, &cfg);
            let reader = RlogReader::new(&obj, &cfg).expect("open");
            let mut scan = reader
                .scan_blocks(&window(&block), &[], &ColumnSelection::all())
                .expect("scan");
            let view = scan
                .next_block_columnar(&obj)
                .expect("columnar exit")
                .expect("one block");
            if view.surviving_count() < block.rows.len() {
                seen.insert("a surviving-row subset");
            }
            let cols: Vec<AttrColumn> = view.attr_columns_for(KEY).collect();
            if cols.len() == 1 {
                seen.insert("a single-column key");
            }
            if let Some(c) = cols.iter().find(|c| c.ty == FieldType::Str) {
                match view.str_dict(c.column_id) {
                    Some(page) if page.dict().iter().any(|v| std::str::from_utf8(v).is_err()) => {
                        seen.insert("a non-UTF-8 dictionary entry");
                    }
                    Some(_) => {}
                    None => {
                        let cells = view.bytes_cursor(c.column_id);
                        if (0..view.surviving_count())
                            .any(|i| cells.at(i).is_some() && cells.str_at(i).is_none())
                        {
                            seen.insert("a non-UTF-8 plain-page cell");
                        }
                    }
                }
            }
            for i in 0..view.surviving_count() {
                let present = cols.iter().filter(|c| ref_present(&view, **c, i)).count();
                if present > 1 {
                    seen.insert("one key under several types in one row");
                }
                if present == 0 && ref_fallback(&view, i).is_some() {
                    seen.insert("a resource or scope value shown through");
                }
                if present == 0 && ref_fallback(&view, i).is_none() {
                    seen.insert("a NULL row");
                }
            }
            if block
                .overlays
                .iter()
                .any(|(res, scope)| res.is_none() && scope.is_some())
            {
                seen.insert("a scope-only value");
            }
        }
        let want: BTreeSet<&'static str> = [
            "a surviving-row subset",
            "a single-column key",
            "a non-UTF-8 dictionary entry",
            "a non-UTF-8 plain-page cell",
            "one key under several types in one row",
            "a resource or scope value shown through",
            "a NULL row",
            "a scope-only value",
        ]
        .into_iter()
        .collect();
        assert_eq!(seen, want);
    }

    // --- UTF-8 validation count -------------------------------------------

    fn validations() -> u64 {
        CELL_TEXT_VALIDATIONS.with(|n| n.get())
    }

    /// A block with the key under `types`, one stream with no overlay, `cells`
    /// per row parallel to `types`, and rows `lo..=hi` surviving.
    fn fixed_block(
        types: &[FieldType],
        cells: Vec<Vec<Option<ColumnValue>>>,
        lo: usize,
        hi: usize,
    ) -> Block {
        Block {
            types: types.to_vec(),
            rows: cells
                .into_iter()
                .map(|by_type| {
                    let mut all = vec![None; TYPES.len()];
                    for (ty, cell) in types.iter().zip(by_type) {
                        let t = TYPES.iter().position(|x| x == ty).expect("a known type");
                        all[t] = cell;
                    }
                    Row {
                        stream: 0,
                        cells: all,
                    }
                })
                .collect(),
            overlays: vec![(None, None); STREAMS],
            lo,
            hi,
            split_pct: 50,
        }
    }

    fn str_value(s: &[u8]) -> Option<ColumnValue> {
        Some(ColumnValue::Str(s.to_vec()))
    }

    /// Build the declared `Str` column over `block` in two chunks and return
    /// the UTF-8 validations the build ran, whether the page was a dictionary
    /// page, and the cells.
    fn count_str_build(block: &Block) -> (u64, bool, Vec<Option<String>>) {
        let cfg = cfg();
        let obj = encode(block, &cfg);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let mut scan = reader
            .scan_blocks(&window(block), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let dc = DeclaredColumn::new(KEY, DeclaredType::Str);
        let n = view.surviving_count();
        let before = validations();
        let plan = DeclaredPlan::build(&view, &dc);
        let dict_page = matches!(
            plan.matching_cursor(),
            Some(DeclaredCursor::Str(c)) if c.dict.is_some()
        );
        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        let mut cells = Vec::new();
        for (start, end) in [(0, n / 2), (n / 2, n)] {
            let built = build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                .expect("declared array");
            cells.extend(declared_cells(&built, DeclaredType::Str));
        }
        (validations() - before, dict_page, cells)
    }

    /// ADR-2121 D3: a declared `Str` cell's UTF-8 is validated at most once per
    /// block. On a plain page each surviving cell is validated once, whether
    /// the key has one occurrence (the fused read) or also an `I64` one (the
    /// presence search's validation is the text appended). On a dictionary page
    /// each entry is validated once for the block, however many rows and
    /// chunks read it, and a row costs no validation.
    ///
    /// Flipped assertions, each seen failing:
    /// - the plain-page multi-occurrence build re-reading the winner's text
    ///   (`matching_str.and_then(|c| c.text_at(i))` in place of the presence
    ///   search's `text`) counts 9 where 6 is expected;
    /// - a dictionary-page presence test that validates the row's entry bytes
    ///   (`cell_text(self.cells.at(i)?)` in `StrCursor::text_at`'s dictionary
    ///   arm) counts one per row on top of the entries, 15 where 3 is expected;
    /// - dropping the `OnceCell` from `StrDictText::entry` (a plain
    ///   `cell_text` per call) validates each entry per chunk and per row, 18
    ///   where 3 is expected.
    #[test]
    fn a_declared_str_cell_is_validated_at_most_once_per_block() {
        // Plain page, one occurrence: rows 1..=6 survive, and 1, 3, 4 and 6 of
        // them carry a cell (4 of them), one of which is not UTF-8.
        let plain = fixed_block(
            &[FieldType::Str],
            vec![
                vec![str_value(b"r0")],
                vec![str_value(b"r1")],
                vec![None],
                vec![str_value(&[b'r', 0xff])],
                vec![str_value(b"r4")],
                vec![None],
                vec![str_value(b"r6")],
                vec![str_value(b"r7")],
            ],
            1,
            6,
        );
        let (count, dict_page, cells) = count_str_build(&plain);
        assert!(!dict_page, "unique values encode as a plain page");
        assert_eq!(count, 4, "one validation per surviving present cell");
        assert_eq!(
            cells,
            vec![
                Some("r1".into()),
                None,
                None,
                Some("r4".into()),
                None,
                Some("r6".into())
            ]
        );

        // Plain page, `Str` and `I64` occurrences: every row has a `Str` cell,
        // rows 0..=3 an `I64` one that wins over it. Rows 1..=6 survive.
        let multi = fixed_block(
            &[FieldType::Str, FieldType::I64],
            (0..8)
                .map(|r| {
                    vec![
                        str_value(format!("m{r}").as_bytes()),
                        (r < 4).then_some(ColumnValue::I64(r)),
                    ]
                })
                .collect(),
            1,
            6,
        );
        let (count, dict_page, cells) = count_str_build(&multi);
        assert!(!dict_page, "unique values encode as a plain page");
        assert_eq!(count, 6, "one validation per surviving present cell");
        assert_eq!(
            cells,
            vec![
                None,
                None,
                None,
                Some("m4".into()),
                Some("m5".into()),
                Some("m6".into())
            ]
        );

        // Dictionary page: 12 rows over three entries, one not UTF-8, all rows
        // surviving, built in two chunks.
        let entries: [&[u8]; 3] = [b"a", b"bc", &[b'b', 0xff]];
        let dict = fixed_block(
            &[FieldType::Str],
            (0..12).map(|r| vec![str_value(entries[r % 3])]).collect(),
            0,
            11,
        );
        let (count, dict_page, cells) = count_str_build(&dict);
        assert!(
            dict_page,
            "three distinct of twelve encode as a dictionary page"
        );
        assert_eq!(
            count, 3,
            "one validation per dictionary entry, none per row"
        );
        let want: Vec<Option<String>> = (0..12)
            .map(|r| ["a", "bc"].get(r % 3).map(|s| s.to_string()))
            .collect();
        assert_eq!(cells, want);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod owned_work_tests {
    //! ADR-2414 decision A1: `owned_work` deals whole row groups, never a
    //! row group's blocks across partitions. Fixtures are writer-produced
    //! objects of one block per record, `GROUP_BLOCKS` blocks per row group.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const GROUP_BLOCKS: usize = 4;
    const GROUPS: usize = 3;
    const BLOCKS: usize = GROUPS * GROUP_BLOCKS;

    fn object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: GROUP_BLOCKS,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn dirs() -> Arc<SegmentDirectories> {
        let obj = object();
        Arc::new(
            RlogReader::decode_directories(&obj[..], &RlogConfig::default()).expect("directories"),
        )
    }

    fn seg_ref(key: &str) -> SegmentRef {
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// `segments` identical objects, each with every block surviving.
    fn plan(segments: usize) -> (PlanCounts, Vec<SegmentRef>) {
        let dirs = dirs();
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let segs = (0..segments)
            .map(|_| {
                Some(SegPlan {
                    planned: Some(PlannedBlocks::new(
                        (0..BLOCKS).collect(),
                        Arc::clone(&dirs),
                        budget.reserve(0).expect("reserve"),
                    )),
                    stats: ScanStats::default(),
                    footer: None,
                    whole_object: None,
                })
            })
            .collect();
        let refs = (0..segments).map(|s| seg_ref(&format!("seg{s}"))).collect();
        (
            PlanCounts {
                segs,
                partitions: segments * BLOCKS,
                full_reads: 0,
            },
            refs,
        )
    }

    /// Every partition's owned `(segment ordinal, blocks)` list.
    fn deal(
        counts: &PlanCounts,
        refs: &[SegmentRef],
        n: usize,
        stripe: bool,
    ) -> Vec<Vec<(usize, Vec<usize>)>> {
        (0..n)
            .map(|p| {
                owned_work(counts, refs, p, n, stripe)
                    .into_iter()
                    .map(|w| (w.ordinal, w.indices))
                    .collect()
            })
            .collect()
    }

    /// One object with more blocks (12) than partitions (3): every row group
    /// lands whole on one partition, every partition owns exactly one group,
    /// and every block is owned exactly once.
    ///
    /// Fails against the per-block deal (`unit i -> partition i % n`), which
    /// puts blocks 0, 3, 6, 9 on partition 0 and so splits every group, and
    /// against a deal that hands every group to partition 0 (partitions 1 and 2
    /// would own nothing).
    #[test]
    fn a_row_group_stays_on_one_partition() {
        let (counts, refs) = plan(1);
        let dealt = deal(&counts, &refs, 3, true);
        assert_eq!(
            dealt,
            vec![
                vec![(0, vec![0, 1, 2, 3])],
                vec![(0, vec![4, 5, 6, 7])],
                vec![(0, vec![8, 9, 10, 11])],
            ],
            "each partition owns exactly one whole row group"
        );
        assert_eq!(dealt, deal(&counts, &refs, 3, true), "the deal is stable");
    }

    /// Group numbering runs on across segments, not from zero per segment:
    /// two objects of three groups over four partitions deal groups
    /// 0..6 to partitions 0, 1, 2, 3, 0, 1, so partition 0 owns segment 0's
    /// first group and segment 1's second, and group counts differ by at most
    /// one.
    ///
    /// Fails against a deal that restarts the group count per segment (segment
    /// 1 would again start at partition 0, leaving partition 3 with one group
    /// and partition 0 with two of segment 1's).
    #[test]
    fn group_numbering_runs_on_across_segments() {
        let (counts, refs) = plan(2);
        let dealt = deal(&counts, &refs, 4, true);
        assert_eq!(
            dealt,
            vec![
                vec![(0, vec![0, 1, 2, 3]), (1, vec![4, 5, 6, 7])],
                vec![(0, vec![4, 5, 6, 7]), (1, vec![8, 9, 10, 11])],
                vec![(0, vec![8, 9, 10, 11])],
                vec![(1, vec![0, 1, 2, 3])],
            ]
        );
        let groups: Vec<usize> = dealt
            .iter()
            .map(|p| p.iter().map(|(_, b)| b.len() / GROUP_BLOCKS).sum())
            .collect();
        let (lo, hi) = (
            groups.iter().min().expect("partitions"),
            groups.iter().max().expect("partitions"),
        );
        assert!(hi - lo <= 1, "group counts are balanced: {groups:?}");
    }

    /// A pruned survivor list is grouped by the object's own row-group
    /// boundaries, not by runs of consecutive survivors or by a fixed survivor
    /// count: blocks 3 and 4 are consecutive survivors but sit in different
    /// groups, and 4 and 5 share one.
    ///
    /// Fails against grouping consecutive survivor runs (`[[3,4,5],[9]]`) and
    /// against chunking survivors by `GROUP_BLOCKS` (`[[3,4,5,9]]`).
    #[test]
    fn survivors_group_by_page_dir_boundaries() {
        let dirs = dirs();
        assert_eq!(
            row_groups(&[3, 4, 5, 9], dirs.page_dir()),
            vec![vec![3], vec![4, 5], vec![9]]
        );
        assert_eq!(
            row_groups(&[], dirs.page_dir()),
            Vec::<Vec<usize>>::new(),
            "no survivors, no groups"
        );
    }

    /// Blocks PAGE_DIR cannot place each keep a singleton group, adjacent ones
    /// included: they share no group, so nothing makes them one unit.
    ///
    /// Fails against matching on the `Option` key alone, which merges 100 and
    /// 101 into one group keyed `None`.
    #[test]
    fn unplaceable_blocks_keep_singleton_groups() {
        let dirs = dirs();
        assert!(dirs.page_dir().locate_block(100).is_none());
        assert_eq!(
            row_groups(&[9, 100, 101], dirs.page_dir()),
            vec![vec![9], vec![100], vec![101]]
        );
    }

    /// Without a read cache the assignment stays segment-granular: segment `j`
    /// goes whole to partition `j % n`.
    #[test]
    fn the_uncached_deal_is_still_per_segment() {
        let (counts, refs) = plan(3);
        let dealt = deal(&counts, &refs, 2, false);
        let all: Vec<usize> = (0..BLOCKS).collect();
        assert_eq!(
            dealt,
            vec![vec![(0, all.clone()), (2, all.clone())], vec![(1, all)]]
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod carried_directory_reservation_tests {
    //! ADR-2414 decision A1: the plan phase's carried directories are charged to
    //! the fetch memory budget a carried whole object is charged to, for the
    //! segments a partition will open and no others, until every partition
    //! owning one of the segment's row groups has finished it; and an open whose
    //! own pruning disagrees with the plan's survivor list fails closed.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::footer::kind;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_query::BlockRangeFetcher;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const BLOCKS: usize = 12;
    /// Spacing of the timestamps of the sparse object, wider than the query
    /// window below, so no block of it can hold a row in the window.
    const SPARSE_STEP: i64 = 10;

    /// One-record blocks with timestamps `ts_step * i`.
    fn object(seq: u64, ts_step: i64) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: 4,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for i in 0..BLOCKS as i64 {
            let ts = ts_step * i;
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts} {}", "payload ".repeat(32)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    /// `(stored, decoded)` bytes of the four directory sections of `obj`.
    fn directory_bytes(obj: &[u8]) -> (u64, u64) {
        let footer = ravel_logseg::footer::open(obj).expect("footer");
        [
            kind::STREAM_DIR,
            kind::FIELD_DIR,
            kind::SKIP_IDX,
            kind::PAGE_DIR,
        ]
        .iter()
        .map(|k| footer.section(*k).expect("section"))
        .fold((0, 0), |(stored, decoded), d| {
            (stored + d.len, decoded + d.uncomp_len)
        })
    }

    fn seg_ref(key: &str, obj: &[u8], ts_step: i64, seq: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: obj.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: ts_step * (BLOCKS as i64 - 1),
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [seq as u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: seq,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// Two segments with a surviving block and one that overlaps the query
    /// window but has none: the budget holds exactly the first two segments'
    /// decoded directory bytes while the plan counts live, and nothing once
    /// they drop.
    ///
    /// Fails against no reservation (`reserved` is 0), against reserving for
    /// every relevant segment (the zero-survivor segment's bytes are added),
    /// and against reserving the stored section lengths (the fixture's
    /// sections compress, so stored and decoded totals differ).
    #[tokio::test]
    async fn carried_directories_reserve_decoded_bytes_for_segments_with_survivors() {
        let store = Arc::new(MemoryStore::new());
        let dense = [object(1, 1), object(2, 1)];
        let sparse = object(3, SPARSE_STEP);
        let mut segments = Vec::new();
        for (i, obj) in dense.iter().enumerate() {
            let key = format!("t/dense{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        store
            .put(
                "t/sparse.rlog",
                bytes::Bytes::from(sparse.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put");
        segments.push(seg_ref("t/sparse.rlog", &sparse, SPARSE_STEP, 3));

        let with_survivors: (u64, u64) = dense
            .iter()
            .map(|o| directory_bytes(o))
            .fold((0, 0), |a, d| (a.0 + d.0, a.1 + d.1));
        let sparse_decoded = directory_bytes(&sparse).1;
        assert_ne!(
            with_survivors.0, with_survivors.1,
            "the fixture's sections compress, so stored and decoded totals differ"
        );
        assert!(sparse_decoded > 0);

        // The window is inside every segment's catalog range and inside a gap
        // of the sparse object's timestamps: it keeps block 11 of each dense
        // object (ts 11) and no block of the sparse one.
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let fetcher = LogSegmentFetcher::new(store.clone())
            .with_memory_budget(Arc::clone(&budget))
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let ctx = PartitionCtx {
            fetcher,
            tenant_hash: TENANT,
            query: LogQuery::new(11, 19),
            columns: ColumnSelection::all(),
            projected_fraction: 1.0,
            phase_accounting: PhaseAccounting::new(),
        };
        assert_eq!(budget.reserved(), 0);
        let counts = compute_plan_counts(&ctx, &segments, 4, true)
            .await
            .expect("plan counts");
        assert_eq!(
            counts.segs[0].as_ref().map(|s| s.planned.is_some()),
            Some(true)
        );
        assert_eq!(
            counts.segs[2].as_ref().map(|s| s.planned.is_some()),
            Some(false),
            "the zero-survivor segment carries no directories"
        );
        assert_eq!(
            budget.reserved(),
            with_survivors.1,
            "the decoded directory bytes of the segments with survivors, nothing for the other"
        );
        drop(counts);
        assert_eq!(budget.reserved(), 0, "released when the plan counts drop");

        // The refusal, on objects whose directories are much larger than what a
        // plan read buffers (a long repetitive attribute value decodes to far
        // more than its compressed section): a budget of exactly the two
        // segments' decoded directories admits their plan, and one byte less
        // refuses it with the typed fetch memory error a refused whole-object
        // reservation reports, on the reservation that completes the
        // directories. The reads run one at a time, so each reads with only the
        // earlier segments' directories held. On the fixture above a plan
        // read's buffers outweigh a segment's directories, so any limit that
        // refuses there refuses a read's buffer first, with the same error, and
        // a smaller limit here does the same: neither shows that the
        // directories are what trips.
        let store = Arc::new(MemoryStore::new());
        let objects = [overflow_object(1), overflow_object(2)];
        let mut segments = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let key = format!("t/large{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        let per_segment: Vec<u64> = objects.iter().map(|o| directory_bytes(o).1).collect();
        let total: u64 = per_segment.iter().sum();
        let with_limit = |limit: u64| PartitionCtx {
            fetcher: LogSegmentFetcher::new(store.clone())
                .with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(limit)))
                .with_block_range(
                    BlockRangeFetcher::new(store.clone())
                        .with_suffix_len(256)
                        .with_whole_object_threshold(0),
                )
                .with_block_range_threshold(0),
            tenant_hash: TENANT,
            query: LogQuery::new(11, 19),
            columns: ColumnSelection::all(),
            projected_fraction: 1.0,
            phase_accounting: PhaseAccounting::new(),
        };
        let exact = compute_plan_counts(&with_limit(total), &segments, 1, true).await;
        assert!(
            exact.is_ok(),
            "the directories fit a budget of exactly their size: {:?}",
            exact.as_ref().err()
        );
        drop(exact);
        let err = compute_plan_counts(&with_limit(total - 1), &segments, 1, true)
            .await
            .err()
            .expect("a budget one byte short of the directories refuses");
        let typed = match &err {
            DataFusionError::External(e) => e.downcast_ref::<SqlError>(),
            _ => None,
        };
        let Some(SqlError::LogFetch(LogFetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
        })) = typed
        else {
            panic!("expected the typed fetch memory error, got: {err:?}");
        };
        assert_eq!(
            (*requested, *reserved, *limit),
            (per_segment[1], per_segment[0], total - 1),
            "the second segment's directories are refused with the first's held"
        );
    }

    /// A segment's rows through a scan whose plan counts are seeded with
    /// `plan_indices` as the survivor list, over a ts window that keeps blocks
    /// 6..12 of the object. Returns the rows' timestamps, or the stream's error.
    async fn scan_with_plan(plan_indices: Vec<usize>) -> Result<Vec<i64>, String> {
        use datafusion::arrow::array::Array;
        let store = Arc::new(MemoryStore::new());
        let obj = object(1, 1);
        store
            .put(
                "t/dense0.rlog",
                bytes::Bytes::from(obj.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let seg = seg_ref("t/dense0.rlog", &obj, 1, 1);
        let dirs = Arc::new(
            RlogReader::decode_directories(&obj[..], &RlogConfig::default()).expect("directories"),
        );
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        let exec = LogsScanExec::new(
            TENANT,
            LogSegmentFetcher::new(store),
            std::slice::from_ref(&seg),
            1,
            6,
            i64::MAX,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            None,
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan");
        let counts = PlanCounts {
            segs: vec![Some(SegPlan {
                planned: Some(PlannedBlocks::new(
                    plan_indices,
                    dirs,
                    budget.reserve(0).expect("reserve"),
                )),
                stats: ScanStats::default(),
                footer: None,
                whole_object: None,
            })],
            partitions: 1,
            full_reads: 1,
        };
        assert!(exec.counts.set(Arc::new(counts)).is_ok());
        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut ts = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            let col = batch
                .column(crate::logs_schema::LOG_COL_TS)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts type")
                .clone();
            ts.extend((0..col.len()).map(|i| col.value(i)));
        }
        Ok(ts)
    }

    /// A plan whose survivor list is the blocks this open's own pruning keeps
    /// reads them all; a plan listing a block the open pruned (or missing one it
    /// kept) is refused with the typed mismatch error instead of reading the
    /// intersection.
    ///
    /// Fails against an open that intersects the dealt blocks with its own
    /// survivors (it returns rows for the plan that lists blocks 0..12, here 6
    /// of them, with no error), and against an open that only compares the two
    /// lists' lengths (the plan below that lists 5..11 has the same length as
    /// the truth and must still be refused).
    #[tokio::test]
    async fn an_open_that_prunes_differently_from_the_plan_fails_closed() {
        assert_eq!(
            scan_with_plan((6..12).collect()).await,
            Ok((6..12).collect::<Vec<i64>>()),
            "a plan that matches the open reads every surviving block"
        );
        for wrong in [(0..12).collect::<Vec<usize>>(), (5..11).collect()] {
            let err = scan_with_plan(wrong.clone())
                .await
                .expect_err("a plan that differs from the open must refuse");
            assert!(
                err.contains("segment survivor mismatch"),
                "plan {wrong:?} refused with the typed error, got: {err}"
            );
        }
    }

    /// One stream whose resource attribute is this long, so each object's
    /// STREAM_DIR, and with it the carried directories, dwarfs the few page
    /// bytes an open holds for a timestamp projection.
    const RESOURCE_VALUE_LEN: usize = 64 * 1024;

    /// One-record blocks, `ts` 0..12, in groups of four, whose two attributes
    /// overflow a one-column dynamic budget: every block carries an `attrs_raw`
    /// page, so the columnar drain falls back to a row-path reopen.
    fn overflow_object(seq: u64) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: 4,
            max_dynamic_columns: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        };
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("{seq}-{}", "r".repeat(RESOURCE_VALUE_LEN))),
        )];
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![
                        ("a".to_string(), AttrValue::Str(format!("a{ts}"))),
                        ("b".to_string(), AttrValue::Str(format!("b{ts}"))),
                    ],
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    fn metric_total(exec: &LogsScanExec, name: &str) -> usize {
        exec.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name)
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// Three segments of three row groups, striped over two partitions, so
    /// both partitions own a group of every segment. A segment's directories
    /// stay reserved while either owner is still on it and are released once
    /// both have finished it, not when the statement ends.
    ///
    /// Partition 0 drains first: every segment is still reserved, because
    /// partition 1 has not finished any. Partition 1 is then held on its first
    /// GET of segment 0, and segment 0 is still reserved. Released, it finishes
    /// segment 0, including the `attrs_raw` row-path reopens of it that come
    /// after partition 0 finished it, and is held again on segment 1: segment
    /// 0's directories are gone from the budget and the other two are not. At
    /// the end nothing is reserved while the exec is still alive.
    ///
    /// Fails against releasing on the first owner's finish (segment 0 is gone
    /// while partition 1 is still on it) and against never releasing (segment
    /// 0 is still reserved once both owners have finished it).
    #[tokio::test]
    async fn carried_directories_are_released_when_every_owner_finishes_the_segment() {
        use futures::StreamExt;
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};

        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let objects: Vec<Vec<u8>> = (1..=3).map(overflow_object).collect();
        let mut segments = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let key = format!("t/held{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        let dirs: Vec<u64> = objects.iter().map(|o| directory_bytes(o).1).collect();
        let all: u64 = dirs.iter().sum();
        // What an open holds besides the directories (a timestamp projection's
        // pages and the tail sections) is far below this.
        let slack = dirs.iter().min().copied().unwrap_or(0) / 2;
        assert!(slack as usize > RESOURCE_VALUE_LEN / 4);

        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let cache_bytes = 64u64 << 20;
        let cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>> =
            Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
                cache_bytes,
                4096,
                cache_bytes,
            )));
        let fetcher = LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
            .with_memory_budget(Arc::clone(&budget))
            .with_cache(cache)
            .with_block_range(
                BlockRangeFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        // The window starts after each segment's first block, so no segment is
        // contained in it and the striped route runs.
        let exec = LogsScanExec::new(
            TENANT,
            fetcher,
            &segments,
            2,
            1,
            BLOCKS as i64,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan");
        let task_ctx = Arc::new(TaskContext::default());
        let first = exec.execute(0, Arc::clone(&task_ctx)).expect("execute");
        let second = exec.execute(1, task_ctx).expect("execute");

        let drained: Vec<RecordBatch> =
            first.map(|b| b.expect("partition 0 batch")).collect().await;
        // Groups 0, 2, 4, 6, 8 of nine: two groups of segments 0 and 2 (the
        // first group is short of block 0) and one of segment 1.
        assert_eq!(rows(&drained), 18);
        assert!(
            budget.reserved() >= all && budget.reserved() < all + slack,
            "partition 1 has finished no segment, so all three are reserved: {} of {all}",
            budget.reserved()
        );

        let gate = store.hold(Op::Get, Some("t/held".to_string()), Occurrence::Always);
        let task = tokio::spawn(async move {
            second
                .map(|b| b.expect("partition 1 batch"))
                .collect::<Vec<_>>()
                .await
        });
        gate.wait_until_held(1).await;
        let on = |key: &str| gate.held_details().iter().all(|(_, _, k)| k.contains(key));
        assert!(on("held0"), "partition 1 opens segment 0 first");
        assert!(
            budget.reserved() >= all && budget.reserved() < all + slack,
            "partition 1 is still on segment 0: {} of {all}",
            budget.reserved()
        );

        // Let partition 1 through segment 0 until it waits on segment 1.
        loop {
            gate.wait_until_held(1).await;
            if on("held1") {
                break;
            }
            for (id, _, key) in gate.held_details() {
                if key.contains("held0") {
                    gate.release(id);
                }
            }
        }
        let rest = all - dirs[0];
        assert!(
            budget.reserved() >= rest && budget.reserved() < rest + slack,
            "both owners finished segment 0, so only segments 1 and 2 stay reserved: {} of {rest}",
            budget.reserved()
        );

        let releaser = {
            let gate = gate.clone();
            tokio::spawn(async move {
                loop {
                    gate.wait_until_held(1).await;
                    for (id, _, _) in gate.held_details() {
                        gate.release(id);
                    }
                }
            })
        };
        let second_rows = task.await.expect("partition 1 task");
        releaser.abort();
        assert_eq!(rows(&second_rows), 3 * (BLOCKS - 1) - 18);
        // The read cache keeps what it admitted, with that read's reservation,
        // until it drops with the exec.
        assert!(
            budget.reserved() < slack,
            "every segment's directories released while the exec is alive: {}",
            budget.reserved()
        );
        assert_eq!(
            metric_total(&exec, "reopens"),
            12,
            "two `attrs_raw` reopens per partition per segment"
        );
        drop(exec);
        assert_eq!(budget.reserved(), 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod fast_path_prefetch_tests {
    //! ADR-2414 decision A2: on the ranged whole-segment fast path a partition
    //! issues its next owned segments' opens while the current one opens and
    //! decodes, at most its share of the GET permits at once, and consumes
    //! them in owned order.

    use std::time::Duration;

    use super::*;
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::physical_plan::ExecutionPlan;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_query::BlockRangeFetcher;
    use ravel_types::accounting::QueryAccountingSnapshot;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([9u8; 16]);
    const BLOCKS: i64 = 4;
    const SEGMENTS: usize = 6;
    /// Segment `i` holds timestamps `i * SPAN .. i * SPAN + BLOCKS`.
    const SPAN: i64 = 100;

    /// Segment `seq`'s object: `BLOCKS` one-record blocks. With
    /// `overflow_last`, the last block carries a second attribute past a
    /// one-column dynamic budget, so it alone has an `attrs_raw` page and the
    /// columnar drain falls back to one row-path reopen on it.
    fn object(seq: usize, overflow_last: bool) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            max_dynamic_columns: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for j in 0..BLOCKS {
            let ts = seq as i64 * SPAN + j;
            let mut attrs = vec![("a".to_string(), AttrValue::Str(format!("a{ts}")))];
            if overflow_last && j == BLOCKS - 1 {
                attrs.push(("b".to_string(), AttrValue::Str(format!("b{ts}"))));
            }
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts} {}", "payload ".repeat(16)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs,
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn key(seq: usize) -> String {
        format!("t/pf{seq}.rlog")
    }

    /// `SEGMENTS` segments in one store, segment `overflow` (if any) built
    /// with an overflowing last block.
    async fn fixture(overflow: Option<usize>) -> (Arc<FaultStore<MemoryStore>>, Vec<SegmentRef>) {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let mut segments = Vec::new();
        for seq in 0..SEGMENTS {
            let obj = object(seq, overflow == Some(seq));
            store
                .put(
                    &key(seq),
                    bytes::Bytes::from(obj.clone()),
                    PutOptions::default(),
                )
                .await
                .expect("put");
            segments.push(SegmentRef {
                data_object_key: key(seq),
                object_size: obj.len() as u64,
                min_event_ts_ns: seq as i64 * SPAN,
                max_event_ts_ns: seq as i64 * SPAN + BLOCKS - 1,
                ingest_hour_bucket: 0,
                sample_count: BLOCKS as u64,
                series_count: 0,
                shard: 0,
                content_hash: [seq as u8 + 1; 32],
                writer_id: Uuid::from_u128(4),
                writer_epoch: 1,
                writer_seq: seq as u64 + 1,
                created_unix_ns: 0,
                level: SegmentLevel::L0,
                segment_format_version: u32::from(ravel_logseg::footer::VERSION),
                declared_column_stats: Default::default(),
            });
        }
        (store, segments)
    }

    /// A `ts`-only scan over `segments` at `partitions` partitions, whose
    /// fetcher has `permits` GET permits and routes every segment ranged.
    /// The window contains every segment and there is no predicate, so the
    /// whole-segment fast path runs; partition 0 owns segments 0, 2 and 4
    /// at two partitions.
    fn exec(
        store: &Arc<FaultStore<MemoryStore>>,
        segments: &[SegmentRef],
        partitions: usize,
        permits: usize,
        accounting: PhaseAccounting,
    ) -> LogsScanExec {
        exec_with(
            ranged_fetcher(store, permits),
            segments,
            partitions,
            accounting,
        )
    }

    /// The fetcher [`exec`] scans with: `permits` GET permits, every segment
    /// routed ranged.
    fn ranged_fetcher(store: &Arc<FaultStore<MemoryStore>>, permits: usize) -> LogSegmentFetcher {
        let store = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
        LogSegmentFetcher::new(Arc::clone(&store))
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0)
            .with_max_concurrent_gets(permits)
    }

    /// [`exec`] over a caller-built fetcher.
    fn exec_with(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        partitions: usize,
        accounting: PhaseAccounting,
    ) -> LogsScanExec {
        LogsScanExec::new(
            TENANT,
            fetcher,
            segments,
            partitions,
            0,
            SEGMENTS as i64 * SPAN,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            accounting,
            crate::logs_schema::logs_schema_with_declared(&[]),
            Arc::new(Vec::new()),
        )
        .expect("scan")
        .with_segment_timing(true)
    }

    fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("ts column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn metric_total(plan: &dyn ExecutionPlan, name: &str) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name && m.labels().is_empty())
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// Whether segment `ordinal`'s open has resolved: its
    /// `seg_open_ready_offset` timeline point exists.
    fn opened(plan: &dyn ExecutionPlan, ordinal: usize) -> bool {
        let label = ordinal.to_string();
        plan.metrics().expect("metrics").iter().any(|m| {
            m.value().name() == "seg_open_ready_offset"
                && m.labels()
                    .iter()
                    .any(|l| l.name() == "segment" && l.value() == label)
        })
    }

    fn held_on(gate: &GateHandle, seq: usize) -> bool {
        let k = key(seq);
        gate.held_details().iter().any(|(_, _, held)| *held == k)
    }

    /// Yields to the partition task until segment `ordinal` has opened.
    async fn wait_opened(plan: &dyn ExecutionPlan, ordinal: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !opened(plan, ordinal) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("segment {ordinal} never opened"));
    }

    /// Releases every held call until the task it is spawned beside ends.
    fn release_all(gate: GateHandle) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                gate.wait_until_held(1).await;
                for id in gate.held() {
                    gate.release(id);
                }
            }
        })
    }

    /// Partition 0's rows and scan-phase accounting with the pipeline off: a
    /// pushed fetch no row count reaches disables it, and stops nothing.
    async fn sequential(
        overflow: Option<usize>,
        permits: usize,
    ) -> (Vec<i64>, usize, QueryAccountingSnapshot) {
        let (store, segments) = fixture(overflow).await;
        let accounting = PhaseAccounting::new();
        let plan = exec(&store, &segments, 2, permits, accounting.clone())
            .with_fetch(Some(usize::MAX))
            .expect("fetch pushdown");
        let batches: Vec<RecordBatch> = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .map(|b| b.expect("batch"))
            .collect()
            .await;
        (
            timestamps(&batches),
            metric_total(plan.as_ref(), "segments_opened"),
            accounting.scan().snapshot(),
        )
    }

    /// With the first segment's GETs held, the second segment opens
    /// completely and the third is not issued: two opens, the share, are
    /// held at once. Released, the rows, the segments opened and the
    /// scan-phase accounting equal the sequential walk's, rows first segment
    /// first.
    ///
    /// Fails against no prefetch (segment 2 never opens under the hold),
    /// against unbounded prefetch (segment 4's GET is held), and against
    /// emitting segments in completion order (segment 2, opened first, would
    /// lead the rows).
    async fn prefetches_within_the_share(partitions: usize, permits: usize) {
        let (store, segments) = fixture(None).await;
        let gate = store.hold(Op::Get, Some(key(0)), Occurrence::Always);
        store.hold(Op::Get, Some(key(4)), Occurrence::Always);
        let accounting = PhaseAccounting::new();
        let plan = Arc::new(exec(
            &store,
            &segments,
            partitions,
            permits,
            accounting.clone(),
        ));
        let stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let task =
            tokio::spawn(
                async move { stream.map(|b| b.expect("batch")).collect::<Vec<_>>().await },
            );

        gate.wait_until_held(1).await;
        assert!(held_on(&gate, 0), "segment 0's first GET is held");
        wait_opened(plan.as_ref(), 2).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(held_on(&gate, 0), "segment 0 is still held");
        assert!(
            !held_on(&gate, 4) && !opened(plan.as_ref(), 4),
            "segment 4 is not issued while segments 0 and 2 hold the share"
        );

        let releaser = release_all(gate);
        let batches = task.await.expect("partition task");
        releaser.abort();

        let (rows, opened_segments, scan) = sequential(None, permits).await;
        assert_eq!(
            rows,
            vec![0, 1, 2, 3, 200, 201, 202, 203, 400, 401, 402, 403],
            "the sequential walk's rows, segment 0 first"
        );
        assert_eq!(timestamps(&batches), rows);
        assert_eq!(metric_total(plan.as_ref(), "segments_opened"), 3);
        assert_eq!(opened_segments, 3);
        let piped = accounting.scan().snapshot();
        assert_eq!(piped.data_objects_touched, 3);
        assert_eq!(piped.logs_ranged_opens, 3);
        assert_eq!(
            piped, scan,
            "scan-phase accounting equals the sequential walk's"
        );
    }

    #[tokio::test]
    async fn a_partition_prefetches_its_next_segments_ranges() {
        // Four permits at two partitions: a share of 2, with permits to spare
        // so the limiter cannot be what stops segment 4.
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        prefetches_within_the_share(2, 4).await;
    }

    /// Two permits at two partitions is a share of 1, floored at 2: the next
    /// segment still opens under the hold. Fails against a share without the
    /// floor, where segment 2 never opens.
    #[tokio::test]
    async fn the_share_is_at_least_two() {
        assert_eq!(fast_path_prefetch_share(2, 2), 2);
        prefetches_within_the_share(2, 2).await;
    }

    /// With the second segment's GETs held, every row of the first is emitted:
    /// its decode is not waiting on the prefetch.
    ///
    /// Fails against an implementation that waits for the prefetches before
    /// draining the current segment (the first batch never arrives).
    #[tokio::test]
    async fn the_current_segment_drains_while_the_next_is_held() {
        let (store, segments) = fixture(None).await;
        let gate = store.hold(Op::Get, Some(key(2)), Occurrence::Always);
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        while rows.len() < BLOCKS as usize {
            let batch = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("segment 0 drains under the hold")
                .expect("a batch")
                .expect("batch");
            rows.extend(timestamps(&[batch]));
        }
        assert_eq!(rows, vec![0, 1, 2, 3]);
        assert!(held_on(&gate, 2), "segment 2's GET is still held");

        let releaser = release_all(gate);
        let rest: Vec<RecordBatch> = stream.map(|b| b.expect("batch")).collect().await;
        releaser.abort();
        assert_eq!(
            timestamps(&rest),
            vec![200, 201, 202, 203, 400, 401, 402, 403]
        );
    }

    /// The second owned segment falls back to the row path for its last block
    /// while the third's open is in flight behind it: the reopen is the one
    /// sequential open, the third segment's prefetch is consumed after it, and
    /// every row comes once, in order.
    ///
    /// Fails against a reopen that drops the in-flight prefetches (segment 4's
    /// rows are missing) and against counting the reopened segment again
    /// (`segments_opened` reads 4).
    #[tokio::test]
    async fn a_fallback_reopen_drains_the_prefetches_behind_it() {
        let (store, segments) = fixture(Some(2)).await;
        let gate = store.hold(Op::Get, Some(key(4)), Occurrence::Always);
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        while rows.len() < 2 * BLOCKS as usize {
            let batch = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("segments 0 and 2 drain under the hold")
                .expect("a batch")
                .expect("batch");
            rows.extend(timestamps(&[batch]));
        }
        assert!(
            held_on(&gate, 4),
            "segment 4's open was issued before segment 2 finished"
        );
        assert_eq!(
            metric_total(&plan, "reopens"),
            1,
            "one reopen, on segment 2"
        );

        let releaser = release_all(gate);
        let rest: Vec<RecordBatch> = stream.map(|b| b.expect("batch")).collect().await;
        releaser.abort();
        rows.extend(timestamps(&rest));
        assert_eq!(
            rows,
            vec![0, 1, 2, 3, 200, 201, 202, 203, 400, 401, 402, 403]
        );
        assert_eq!(metric_total(&plan, "reopens"), 1);
        assert_eq!(metric_total(&plan, "segments_opened"), 3);
        let (sequential_rows, _, _) = sequential(Some(2), 4).await;
        assert_eq!(rows, sequential_rows);
    }

    /// A prefetched open's error surfaces at that segment's turn, after the
    /// segments before it have emitted every row.
    #[tokio::test]
    async fn a_prefetch_error_surfaces_at_its_own_segment() {
        let (store, mut segments) = fixture(None).await;
        segments[2].segment_format_version = u32::MAX;
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        let err = loop {
            match stream.next().await.expect("an item before the end") {
                Ok(batch) => rows.extend(timestamps(&[batch])),
                Err(e) => break e,
            }
        };
        assert_eq!(rows, vec![0, 1, 2, 3], "segment 0 is emitted whole first");
        assert!(
            err.to_string().contains(&key(2)),
            "the error names segment 2: {err}"
        );
    }

    /// What partition 0 of a two-partition exec produced, executed alone,
    /// under a fetch memory budget of `limit` bytes: its rows,
    /// `segments_opened`, `fast_path_ranged_segments`,
    /// `prefetch_memory_reopens`, and the scan phase's data objects touched,
    /// or the error the stream ended with. `pipelined: false` turns the
    /// pipeline off the way [`sequential`] does. Every reservation is checked
    /// released before an error is returned.
    async fn budgeted(
        limit: u64,
        pipelined: bool,
    ) -> Result<(Vec<i64>, usize, usize, usize, u64), DataFusionError> {
        let (store, segments) = fixture(None).await;
        let accounting = PhaseAccounting::new();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(fetcher, &segments, 2, accounting.clone()));
        let plan: Arc<dyn ExecutionPlan> = if pipelined {
            exec
        } else {
            exec.with_fetch(Some(usize::MAX)).expect("fetch pushdown")
        };
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        let drained = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(item) = stream.next().await {
                rows.extend(timestamps(&[item?]));
            }
            Ok::<(), DataFusionError>(())
        })
        .await
        .expect("the scan ends rather than retrying a refused open forever");
        drop(stream);
        assert_eq!(
            budget.reserved(),
            0,
            "every fetch reservation is released, on success and on error"
        );
        drained?;
        Ok((
            rows,
            metric_total(plan.as_ref(), "segments_opened"),
            metric_total(plan.as_ref(), "fast_path_ranged_segments"),
            metric_total(plan.as_ref(), "prefetch_memory_reopens"),
            accounting.scan().snapshot().data_objects_touched,
        ))
    }

    /// One partition, partition 0 of a two-partition exec executed alone: a
    /// ranged prefetch the fetch memory budget refuses does not fail a query
    /// the sequential walk runs. At the smallest budget the sequential walk
    /// completes in, the pipelined walk (share 2) is refused its second open,
    /// turns the pipeline off and retries the refused segment once at its
    /// turn, and returns the sequential walk's rows, segments opened,
    /// ranged-route count and data objects touched, with the retry counted
    /// exactly once. One byte below that budget the retry is refused too, and
    /// the query fails with the typed refusal, every reservation released.
    ///
    /// Fails against failing the query on the prefetch's refusal (the first
    /// pipelined run errors) and against retrying every refusal (the run one
    /// byte short never ends, and the timeout fires).
    #[tokio::test]
    async fn a_prefetch_refused_by_the_memory_budget_reopens_sequentially() {
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        let ceiling: u64 = 1 << 24;
        assert!(
            budgeted(ceiling, false).await.is_ok(),
            "the ceiling fits the sequential walk"
        );
        // The smallest budget the sequential walk completes in.
        let (mut lo, mut hi) = (0u64, ceiling);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if budgeted(mid, false).await.is_ok() {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let fits = hi;

        let unlimited = budgeted(ceiling, true)
            .await
            .expect("pipelined, no pressure");
        assert_eq!(unlimited.3, 0, "no refusal, no fallback reopen");

        let sequential = budgeted(fits, false).await.expect("sequential fits");
        let pipelined = budgeted(fits, true)
            .await
            .expect("the pipelined walk falls back instead of failing");
        assert_eq!(
            sequential.0,
            (0..BLOCKS)
                .chain(200..200 + BLOCKS)
                .chain(400..400 + BLOCKS)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            (&pipelined.0, pipelined.1, pipelined.2, pipelined.4),
            (&sequential.0, sequential.1, sequential.2, sequential.4),
            "rows, segments opened, ranged opens and data objects touched match \
             the sequential walk"
        );
        assert_eq!((sequential.1, sequential.2, sequential.4), (3, 3, 3));
        assert_eq!(
            (sequential.3, pipelined.3),
            (0, 1),
            "the fallback is counted once, on the pipelined run only"
        );

        for pipelined in [false, true] {
            let err = budgeted(fits - 1, pipelined)
                .await
                .expect_err("one byte short refuses the sequential open too");
            assert!(
                is_fetch_memory_refusal(&err),
                "the typed refusal surfaces (pipelined {pipelined}): {err}"
            );
        }
    }

    /// The largest budget [`fits_with`] bisects from; every run fits in it.
    const CEILING: u64 = 1 << 24;

    /// How [`run_statement`] drives the partitions of one exec.
    #[derive(Clone)]
    enum Drive {
        /// One item from each live partition in turn, partition 0 first.
        Lockstep,
        /// One spawned task per partition.
        Spawned,
        /// Each listed partition to its end, in the order given.
        Order(Vec<usize>),
    }

    /// What every partition of one statement produced.
    struct Statement {
        /// Each partition's timestamps, in emitted order.
        rows: Vec<Vec<i64>>,
        plan: Arc<dyn ExecutionPlan>,
        scan: QueryAccountingSnapshot,
    }

    impl Statement {
        fn metric(&self, name: &str) -> usize {
            metric_total(self.plan.as_ref(), name)
        }
    }

    /// The fixture's rows for `segments`, in that order.
    fn rows_of(segments: &[usize]) -> Vec<i64> {
        segments
            .iter()
            .flat_map(|&s| (0..BLOCKS).map(move |j| s as i64 * SPAN + j))
            .collect()
    }

    /// Executes every partition of ONE exec over the fixture (segment
    /// `overflow`, if any, falling back to the `attrs_raw` reopen), at
    /// `partitions` partitions and 4 GET permits, under one fetch memory
    /// budget of `limit` bytes, driven by `drive`. Every stream is dropped
    /// and the budget's reserved bytes are asserted 0 before any error is
    /// returned. `pipelined: false` turns the pipeline off the way
    /// [`sequential`] does.
    async fn run_statement(
        limit: u64,
        partitions: usize,
        overflow: Option<usize>,
        pipelined: bool,
        drive: Drive,
    ) -> Result<Statement, DataFusionError> {
        let (store, segments) = fixture(overflow).await;
        let accounting = PhaseAccounting::new();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(
            fetcher,
            &segments,
            partitions,
            accounting.clone(),
        ));
        let plan: Arc<dyn ExecutionPlan> = if pipelined {
            exec
        } else {
            exec.with_fetch(Some(usize::MAX)).expect("fetch pushdown")
        };
        let streams: Vec<SendableRecordBatchStream> = (0..partitions)
            .map(|p| {
                plan.execute(p, Arc::new(TaskContext::default()))
                    .expect("execute")
            })
            .collect();
        let mut rows = vec![Vec::new(); partitions];
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            drive_streams(streams, drive, &mut rows),
        )
        .await
        .expect("the statement ends rather than waiting or retrying forever");
        assert_eq!(
            budget.reserved(),
            0,
            "every stream dropped releases every fetch reservation"
        );
        outcome?;
        Ok(Statement {
            rows,
            plan,
            scan: accounting.scan().snapshot(),
        })
    }

    /// Drives `streams` (partition `p` at index `p`) to their ends, or to the
    /// first error, collecting each partition's timestamps into `rows`. Every
    /// stream is dropped before this returns.
    async fn drive_streams(
        streams: Vec<SendableRecordBatchStream>,
        drive: Drive,
        rows: &mut [Vec<i64>],
    ) -> Result<(), DataFusionError> {
        match drive {
            Drive::Lockstep => {
                let mut live: Vec<Option<SendableRecordBatchStream>> =
                    streams.into_iter().map(Some).collect();
                while live.iter().any(Option::is_some) {
                    for (p, slot) in live.iter_mut().enumerate() {
                        let Some(stream) = slot.as_mut() else {
                            continue;
                        };
                        match stream.next().await {
                            Some(Ok(batch)) => rows[p].extend(timestamps(&[batch])),
                            Some(Err(e)) => return Err(e),
                            None => *slot = None,
                        }
                    }
                }
                Ok(())
            }
            Drive::Spawned => {
                let tasks: Vec<_> = streams
                    .into_iter()
                    .map(|mut stream| {
                        tokio::spawn(async move {
                            let mut out = Vec::new();
                            while let Some(item) = stream.next().await {
                                out.extend(timestamps(&[item?]));
                            }
                            Ok::<_, DataFusionError>(out)
                        })
                    })
                    .collect();
                let mut first_error = None;
                for (p, task) in tasks.into_iter().enumerate() {
                    match task.await.expect("partition task") {
                        Ok(out) => rows[p] = out,
                        Err(e) => {
                            first_error.get_or_insert(e);
                        }
                    }
                }
                first_error.map_or(Ok(()), Err)
            }
            Drive::Order(order) => {
                let mut live: Vec<Option<SendableRecordBatchStream>> =
                    streams.into_iter().map(Some).collect();
                for p in order {
                    let Some(mut stream) = live[p].take() else {
                        continue;
                    };
                    while let Some(item) = stream.next().await {
                        rows[p].extend(timestamps(&[item?]));
                    }
                }
                Ok(())
            }
        }
    }

    /// The smallest budget at which `partitions` partitions with the
    /// pipeline off, driven by `drive`, complete.
    async fn fits_with(partitions: usize, overflow: Option<usize>, drive: Drive) -> u64 {
        assert!(
            run_statement(CEILING, partitions, overflow, false, drive.clone())
                .await
                .is_ok(),
            "the ceiling fits"
        );
        let (mut lo, mut hi) = (0u64, CEILING);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if run_statement(mid, partitions, overflow, false, drive.clone())
                .await
                .is_ok()
            {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        hi
    }

    /// Whether partition `p`'s slot holds a prefetch that resolved with an
    /// open scan.
    fn holds_open_prefetch(exec: &LogsScanExec, p: usize) -> bool {
        exec.prefetch_pool
            .slot(p)
            .expect("slot")
            .prefetched
            .iter()
            .any(|f| matches!(&f.open, PrefetchOpen::Ready { opened, .. } if matches!(**opened, Ok(Some(_)))))
    }

    /// What [`reopen_refused`] observed.
    struct ReopenRun {
        rows: [Vec<i64>; 2],
        exec: Arc<LogsScanExec>,
        /// Whether partition 1 held its current open and an open prefetch
        /// while partition 0's `attrs_raw` reopen GET was held.
        sibling_held_both: bool,
        /// Calls the hold gate held for the reopen, counted when partition 0
        /// was parked on it.
        reopen_held: usize,
        /// How the statement ended, observed after every stream was dropped
        /// and the reserved bytes checked.
        outcome: Result<(), DataFusionError>,
    }

    /// The overflow fixture (segment 0's last block falls back to the
    /// `attrs_raw` reopen) at two partitions under a budget of `limit`, in a
    /// forced order: partition 0 drains segment 0's three clean blocks and
    /// parks on its reopen's first GET, held by a gate on segment 0's key;
    /// only then is partition 1 polled until it has emitted its first batch,
    /// which leaves it holding segment 1's open and segment 3's prefetch;
    /// then the gate releases everything and partition 0, then partition 1,
    /// run to their ends. Every stream is dropped and reserved bytes are
    /// asserted 0 before the outcome is returned.
    async fn reopen_refused(limit: u64) -> ReopenRun {
        let (store, segments) = fixture(Some(0)).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(fetcher, &segments, 2, PhaseAccounting::new()));
        let gate = store.hold(Op::Get, Some(key(0)), Occurrence::Always);
        let mut s0 = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 0");
        let mut s1 = exec
            .execute(1, Arc::new(TaskContext::default()))
            .expect("execute 1");
        let mut rows = [Vec::new(), Vec::new()];
        let mut sibling_held_both = false;
        let mut reopen_held = 0;
        let outcome = tokio::time::timeout(Duration::from_secs(10), async {
            reopen_held = loop {
                match futures::poll!(s0.next()) {
                    Poll::Ready(Some(item)) => rows[0].extend(timestamps(&[item?])),
                    Poll::Ready(None) => {
                        return Err(DataFusionError::Internal(
                            "partition 0 ended before its reopen".into(),
                        ));
                    }
                    Poll::Pending => {
                        if rows[0].len() == BLOCKS as usize - 1 && held_on(&gate, 0) {
                            break gate.held_count();
                        }
                        for id in gate.held() {
                            gate.release(id);
                        }
                        tokio::task::yield_now().await;
                    }
                }
            };
            loop {
                match futures::poll!(s1.next()) {
                    Poll::Ready(Some(item)) => {
                        rows[1].extend(timestamps(&[item?]));
                        break;
                    }
                    Poll::Ready(None) => {
                        return Err(DataFusionError::Internal(
                            "partition 1 ended before its first batch".into(),
                        ));
                    }
                    Poll::Pending => tokio::task::yield_now().await,
                }
            }
            sibling_held_both = holds_open_prefetch(&exec, 1);
            let releaser = release_all(gate.clone());
            let drained = async {
                while let Some(item) = s0.next().await {
                    rows[0].extend(timestamps(&[item?]));
                }
                while let Some(item) = s1.next().await {
                    rows[1].extend(timestamps(&[item?]));
                }
                Ok::<(), DataFusionError>(())
            }
            .await;
            releaser.abort();
            drained
        })
        .await
        .expect("the statement ends rather than waiting or retrying forever");
        drop(s0);
        drop(s1);
        assert_eq!(budget.reserved(), 0, "every fetch reservation is released");
        ReopenRun {
            rows,
            exec,
            sibling_held_both,
            reopen_held,
            outcome,
        }
    }

    /// `name`'s value on partition `p` alone.
    fn partition_metric(plan: &dyn ExecutionPlan, name: &str, p: usize) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| {
                m.value().name() == name && m.labels().is_empty() && m.partition() == Some(p)
            })
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// The smallest budget the one-partition pipeline-off run completes in.
    async fn fits_1() -> u64 {
        fits_with(1, None, Drive::Order(vec![0])).await
    }

    /// The smallest budget the two-partition pipeline-off run, driven in
    /// lockstep, completes in.
    async fn fits_2(overflow: Option<usize>) -> u64 {
        fits_with(2, overflow, Drive::Lockstep).await
    }

    /// At two partitions in lockstep and the budget the pipeline-off run
    /// needs, partition 0 holds its current open and a prefetch when
    /// partition 1's first open is refused: the refusal drops partition 0's
    /// prefetch, hands its segment back, and partition 1's retry succeeds.
    /// Rows per partition, segments opened, the route split and data objects
    /// touched are the pipeline-off run's.
    ///
    /// Fails against the branch before the pool (partition 1 holds nothing
    /// behind its open and fails with no retry), against dropping only the
    /// refused partition's own prefetches (the retry is refused again),
    /// and against dropping without handing the segments back (segment 2's
    /// rows are missing).
    #[tokio::test]
    async fn a_refused_open_drops_every_partitions_prefetches_and_retries_once() {
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        let fits_1 = fits_1().await;
        let fits_2 = fits_2(None).await;
        assert!(fits_2 >= fits_1, "{fits_2} >= {fits_1}");
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("the pipeline-off run fits");
        assert_eq!(off.rows, [rows_of(&[0, 2, 4]), rows_of(&[1, 3, 5])]);
        let on = run_statement(fits_2, 2, None, true, Drive::Lockstep)
            .await
            .expect("the pipelined run retries instead of failing");
        assert_eq!(on.rows, off.rows);
        assert_eq!(
            (
                on.metric("segments_opened"),
                on.metric("fast_path_ranged_segments"),
                on.scan.data_objects_touched
            ),
            (6, 6, 6)
        );
        let reopens = on.metric("prefetch_memory_reopens");
        assert!(
            (1..=2).contains(&reopens),
            "a refusal occurs and each partition retries at most once: {reopens}"
        );
        let revocations = on.metric("prefetch_revocations");
        assert!(
            revocations <= 2,
            "at most one prefetch per partition: {revocations}"
        );
        assert_eq!(off.metric("prefetch_memory_reopens"), 0);
    }

    /// Two partitions on spawned tasks at the two-partition pipeline-off
    /// budget both complete with the pipeline-off rows. One byte below the
    /// one-partition pipeline-off budget, which no interleaving of the two
    /// tasks completes in, the run ends with the typed refusal inside the
    /// timeout. Both runs release every reservation once their streams are
    /// dropped ([`run_statement`]), and so does dropping both streams after
    /// their first batch, while each slot holds an open prefetch and the
    /// plan is still alive.
    ///
    /// Fails against retrying every refusal (the run one byte short never
    /// ends and the timeout fires) and against a `Drop` that does not clear
    /// the stream's slot (the cancelled prefetches stay reserved).
    #[test]
    fn all_partitions_spawned_match_the_sequential_walk_below_the_pipelined_peak() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        // The body runs on a worker so the timeout below still fires if a
        // refused open retried forever inside one poll; the shutdown then
        // abandons that worker instead of waiting for it.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async {
                let body = tokio::spawn(spawned_partitions());
                match tokio::time::timeout(Duration::from_secs(60), body).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Ok(Err(e)) => panic!("test body task: {e}"),
                    Err(_) => panic!("the test body never ended"),
                }
            });
        }));
        rt.shutdown_timeout(Duration::from_secs(1));
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    /// The body of
    /// [`all_partitions_spawned_match_the_sequential_walk_below_the_pipelined_peak`].
    async fn spawned_partitions() {
        let fits_1 = fits_1().await;
        let fits_2 = fits_2(None).await;
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        let spawned = run_statement(fits_2, 2, None, true, Drive::Spawned)
            .await
            .expect("both spawned partitions complete");
        let sorted = |rows: &[Vec<i64>]| {
            let mut all: Vec<i64> = rows.concat();
            all.sort_unstable();
            all
        };
        assert_eq!(sorted(&spawned.rows), sorted(&off.rows));
        assert_eq!(
            spawned.rows, off.rows,
            "each partition's own rows, in order"
        );
        assert_eq!(spawned.metric("segments_opened"), 6);

        let err = run_statement(fits_1 - 1, 2, None, true, Drive::Spawned)
            .await
            .err()
            .expect("one byte below the one-partition fit refuses");
        assert!(is_fetch_memory_refusal(&err), "typed refusal: {err}");

        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(CEILING));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = exec_with(fetcher, &segments, 2, PhaseAccounting::new());
        let mut streams: Vec<SendableRecordBatchStream> = (0..2)
            .map(|p| {
                exec.execute(p, Arc::new(TaskContext::default()))
                    .expect("execute")
            })
            .collect();
        for stream in &mut streams {
            stream
                .next()
                .await
                .expect("a first batch")
                .expect("first batch");
        }
        assert!(
            holds_open_prefetch(&exec, 0) && holds_open_prefetch(&exec, 1),
            "each slot holds an open prefetch when the streams are dropped"
        );
        drop(streams);
        assert_eq!(
            budget.reserved(),
            0,
            "dropped streams release their prefetches with the plan still alive"
        );
    }

    /// Partition 1 alone is polled until it holds its current open and a
    /// prefetch, at the two-partition pipeline-off budget. Then only
    /// partition 0 is polled: its first open is refused, it drops partition
    /// 1's prefetch from its own task, retries and finishes segments 0, 2
    /// and 4 with partition 1 never polled in between. Partition 1 then
    /// reopens segment 3 from the segments handed back to it.
    ///
    /// Fails against any design in which the refused open waits for
    /// partition 1 to be polled (the timeout fires), against dropping only
    /// partition 0's own prefetches (its retry is refused again), and
    /// against handing segment 3 back into partition 0's work (partition 0's
    /// rows gain segment 3, partition 1's lose it).
    #[tokio::test]
    async fn the_refused_partition_completes_without_its_sibling_being_polled() {
        let fits_2 = fits_2(None).await;
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(fits_2));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = exec_with(fetcher, &segments, 2, PhaseAccounting::new());
        let mut s0 = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 0");
        let mut s1 = exec
            .execute(1, Arc::new(TaskContext::default()))
            .expect("execute 1");

        let mut rows1 = timestamps(&[s1
            .next()
            .await
            .expect("partition 1's first batch")
            .expect("batch")]);
        assert!(
            holds_open_prefetch(&exec, 1),
            "partition 1 holds segment 3's prefetch"
        );

        let rows0 = tokio::time::timeout(Duration::from_secs(10), async {
            let mut rows = Vec::new();
            while let Some(item) = s0.next().await {
                rows.extend(timestamps(&[item.expect("partition 0 completes")]));
            }
            rows
        })
        .await
        .expect("partition 0 completes without partition 1 being polled");
        assert_eq!(rows0, off.rows[0]);
        assert_eq!(partition_metric(&exec, "prefetch_memory_reopens", 0), 1);
        assert_eq!(partition_metric(&exec, "prefetch_revocations", 0), 1);
        drop(s0);
        assert!(
            budget.reserved() > 0,
            "partition 1's current open is still held"
        );

        while let Some(item) = s1.next().await {
            rows1.extend(timestamps(&[item.expect("partition 1 completes")]));
        }
        assert_eq!(rows1, off.rows[1], "segment 3 is reopened from handed_back");
        drop(s1);
        assert_eq!(budget.reserved(), 0);
    }

    /// One partition owning all six segments, share 4, at the budget the
    /// pipeline-off run needs: segment 0 opens, the three prefetches behind
    /// it are refused, and the first of them is consumed refused at its
    /// turn, which drops the other two and retries it once. The dropped
    /// segments go back to work in owned order, so rows come segment 0 to
    /// segment 5, in order.
    ///
    /// Fails against popping from the front in the revoker's drain or in the
    /// owner's hand-back drain (segments 2 and 3 swap), against dropping
    /// without handing back (segments 2 and 3 are missing), against leaving
    /// the pipeline on (the retried segment's prefetches are refused and
    /// retried again, two reopens), and against failing the query at the
    /// consumed prefetch's refusal.
    #[tokio::test]
    async fn released_prefetches_go_back_to_work_in_owned_order() {
        assert_eq!(fast_path_prefetch_share(4, 1), 4);
        let fits_1 = fits_1().await;
        let run = run_statement(fits_1, 1, None, true, Drive::Order(vec![0]))
            .await
            .expect("the pipelined run retries instead of failing");
        assert_eq!(run.rows, [rows_of(&[0, 1, 2, 3, 4, 5])]);
        assert_eq!(run.metric("segments_opened"), 6);
        assert_eq!(run.metric("prefetch_memory_reopens"), 1);
        assert_eq!(
            run.metric("prefetch_revocations"),
            2,
            "segments 2 and 3's refused prefetches"
        );
    }

    /// The fixture variant whose segment 0 falls back to the `attrs_raw`
    /// reopen, in the order [`reopen_refused`] forces: partition 1 takes its
    /// current open and a prefetch while partition 0 is parked on its
    /// reopen's first GET, so the reopen is refused once released. The
    /// refusal drops both partitions' prefetches and retries the row-path
    /// reopen with the same `skip`; rows per partition are the pipeline-off
    /// run's, with no duplicate.
    ///
    /// The budget is the smallest at which partition 1 holds both opens at
    /// that point, bisected.
    ///
    /// Fails against the reopen's refusal still failing the query, against
    /// retrying it as a fast-path open (segment 0's first three rows come
    /// twice), against losing `skip` (the same), and against not handing
    /// segment 3 back (partition 1 loses its rows).
    #[tokio::test]
    async fn a_refused_attrs_raw_reopen_drops_the_prefetches_behind_it() {
        let fits_2 = fits_2(Some(0)).await;
        let off = run_statement(fits_2, 2, Some(0), false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        assert_eq!(off.rows, [rows_of(&[0, 2, 4]), rows_of(&[1, 3, 5])]);
        assert_eq!(off.metric("reopens"), 1);

        // Whether partition 1 held both opens at the park point depends only
        // on what happened before it, not on how the run then ends.
        assert!(reopen_refused(CEILING).await.sibling_held_both);
        let (mut lo, mut hi) = (fits_2, CEILING);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if reopen_refused(mid).await.sibling_held_both {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let run = reopen_refused(hi).await;
        if let Err(e) = &run.outcome {
            panic!("the refused reopen is retried: {e}");
        }
        assert!(run.sibling_held_both);
        assert_eq!(
            run.reopen_held, 1,
            "the gate parked partition 0 on the reopen"
        );
        assert_eq!(run.rows.to_vec(), off.rows);
        let plan = run.exec.as_ref();
        assert_eq!(metric_total(plan, "reopens"), 1);
        assert_eq!(partition_metric(plan, "prefetch_memory_reopens", 0), 1);
        assert_eq!(metric_total(plan, "prefetch_memory_reopens"), 1);
        assert!(partition_metric(plan, "prefetch_revocations", 0) >= 1);
    }

    /// Two statements over clones of one fetcher share one budget, one
    /// partition each, share 4, at twice the one-partition pipeline-off
    /// budget. Statement 1 is driven to its first batch; statement 2's first
    /// open is then refused, its own pool holds nothing to drop, its one
    /// retry is refused too, and it ends with the typed refusal at once,
    /// leaving the budget where it found it. Statement 1 then completes.
    ///
    /// Fails against a refused open that waits for statement 1 (the timeout
    /// fires) and against retrying more than once (two reopens counted).
    #[tokio::test]
    async fn another_statements_prefetches_fail_an_open_fast_and_typed() {
        let limit = 2 * fits_1().await;
        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let one = exec_with(fetcher.clone(), &segments, 1, PhaseAccounting::new());
        let two = exec_with(fetcher, &segments, 1, PhaseAccounting::new());
        let mut s1 = one
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 1");
        let mut rows1 = timestamps(&[s1
            .next()
            .await
            .expect("statement 1's first batch")
            .expect("batch")]);
        let before = budget.reserved();
        assert!(before > 0, "statement 1 holds its opens");

        let mut s2 = two
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 2");
        let ended = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(item) = s2.next().await {
                item?;
            }
            Ok::<(), DataFusionError>(())
        })
        .await
        .expect("statement 2 ends rather than waiting for statement 1");
        let err = ended.expect_err("statement 2 is refused twice");
        assert!(is_fetch_memory_refusal(&err), "typed refusal: {err}");
        assert_eq!(metric_total(&two, "prefetch_memory_reopens"), 1);
        drop(s2);
        assert_eq!(budget.reserved(), before);

        while let Some(item) = s1.next().await {
            rows1.extend(timestamps(&[item.expect("statement 1 completes")]));
        }
        assert_eq!(rows1, rows_of(&[0, 1, 2, 3, 4, 5]));
        drop(s1);
        assert_eq!(budget.reserved(), 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod read_gate_tests {
    //! ADR-1702 task 8: with a read gate on the fetcher, the logs scan
    //! decodes each block as one `log_block` job. The placement test, that a
    //! parked block decode leaves the runtime free, is
    //! `tests/logs_scan_read_gate.rs`, in a binary of its own.

    use super::*;
    use datafusion::physical_plan::ExecutionPlan;
    use ravel_catalog::SegmentLevel;
    use ravel_cpu_gate::{CpuGateConfig, InstantClock};
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    /// Records in the fixture's one object. The writer cuts a block per
    /// record (`block_target_records: 1`), so this is also its block count,
    /// and with no predicate and a window over every record each block
    /// survives pruning and is decoded exactly once.
    const BLOCKS: u64 = 4;
    const KEY: &str = "t/gate0.rlog";

    /// One object of [`BLOCKS`] one-record blocks, each carrying one
    /// attribute, so no block has an `attrs_raw` page and the columnar
    /// drain never falls back.
    fn object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [5u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts} {}", "payload ".repeat(16)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("a".to_string(), AttrValue::Str(format!("a{ts}")))],
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
        fixture_at(KEY).await
    }

    /// The fixture stored under `key`.
    async fn fixture_at(key: &str) -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let obj = object();
        store
            .put(key, bytes::Bytes::from(obj.clone()), PutOptions::default())
            .await
            .expect("put");
        let segment = SegmentRef {
            data_object_key: key.to_string(),
            object_size: obj.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS,
            series_count: 0,
            shard: 0,
            content_hash: [3u8; 32],
            writer_id: Uuid::from_u128(5),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        };
        (store, vec![segment])
    }

    /// A read gate whose byte floor is 0, so every block is a gate job.
    fn floor_zero_gate() -> Arc<ReadGate> {
        Arc::new(ReadGate::new(
            CpuGateConfig {
                permits: 2,
                inline_floor_bytes: 0,
                eval_floor_samples: 0,
            },
            Arc::new(InstantClock::new()),
        ))
    }

    fn site_counts(gate: &ReadGate, site: ReadSite) -> (u64, u64) {
        gate.snapshot()
            .sites
            .iter()
            .find(|counts| counts.site == site)
            .map_or((0, 0), |counts| (counts.jobs, counts.inline))
    }

    fn total_inline(gate: &ReadGate) -> u64 {
        gate.snapshot().sites.iter().map(|c| c.inline).sum()
    }

    /// `ts` alone is the columnar fast path; adding `attrs` makes the scan
    /// statically ineligible for it, so every block drains the row path.
    fn projection(rows: bool) -> Vec<usize> {
        if rows {
            vec![crate::logs_schema::LOG_COL_TS, LOG_COL_ATTRS]
        } else {
            vec![crate::logs_schema::LOG_COL_TS]
        }
    }

    /// A one-partition scan over the whole fixture window, with `gate` on
    /// the fetcher when given.
    fn exec(
        store: &Arc<dyn ObjectStoreBackend>,
        segments: &[SegmentRef],
        gate: Option<Arc<ReadGate>>,
        rows: bool,
    ) -> LogsScanExec {
        let fetcher = LogSegmentFetcher::new(Arc::clone(store));
        let fetcher = match gate {
            Some(gate) => fetcher.with_read_gate(gate),
            None => fetcher,
        };
        LogsScanExec::new(
            TENANT,
            fetcher,
            segments,
            1,
            0,
            BLOCKS as i64,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&projection(rows)),
            PhaseAccounting::new(),
            crate::logs_schema::logs_schema_with_declared(&[]),
            Arc::new(Vec::new()),
        )
        .expect("scan")
    }

    async fn collect(plan: &LogsScanExec) -> Vec<RecordBatch> {
        plan.execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .map(|b| b.expect("batch"))
            .collect()
            .await
    }

    fn metric_total(plan: &dyn ExecutionPlan, name: &str) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name && m.labels().is_empty())
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// With the floor at 0, a scan moves the `log_block` job count by exactly
    /// the fixture's [`BLOCKS`], on the columnar path and on the row path, runs
    /// nothing inline, and returns what the same scan returns with no gate.
    ///
    /// Fails with the columnar path's `gate.run` wrap removed (the columnar
    /// case reads `(0, 0)`), and with the row path back on the inline
    /// `next_block` (the row case reads `(0, 0)`).
    #[tokio::test]
    async fn log_block_decodes_run_through_the_read_gate() {
        let (store, segments) = fixture().await;
        for rows in [false, true] {
            let gate = floor_zero_gate();
            let gated = exec(&store, &segments, Some(Arc::clone(&gate)), rows);
            let got = collect(&gated).await;
            assert_eq!(
                site_counts(&gate, ReadSite::LogBlock),
                (BLOCKS, 0),
                "one log_block job per fixture block (rows path: {rows})"
            );
            assert_eq!(total_inline(&gate), 0, "nothing ran inline (rows: {rows})");
            let path_batches = if rows {
                "rowpath_batches"
            } else {
                "columnar_batches"
            };
            assert!(
                metric_total(&gated, path_batches) > 0,
                "the scan took the path under test (rows: {rows})"
            );
            let want = collect(&exec(&store, &segments, None, rows)).await;
            assert_eq!(got, want, "gated and inline output agree (rows: {rows})");
        }
    }

    /// The late-materialization re-read of one block
    /// ([`RowFetchSource::fetch_block`]) is one `log_block` job per block, and
    /// returns the record the same re-read returns with no gate.
    ///
    /// Fails with `fetch_block` back on the inline `next_block`: the count
    /// reads `(0, 0)`.
    #[tokio::test]
    async fn row_ref_block_fetches_run_through_the_read_gate() {
        let (store, segments) = fixture().await;
        let gate = floor_zero_gate();
        let gated = exec(&store, &segments, Some(Arc::clone(&gate)), false).row_fetch_source();
        let inline = exec(&store, &segments, None, false).row_fetch_source();
        for block in 0..BLOCKS as usize {
            let got = gated
                .fetch_block(0, block, &[(0, block)])
                .await
                .expect("gated fetch");
            let want = inline
                .fetch_block(0, block, &[(0, block)])
                .await
                .expect("inline fetch");
            assert_eq!(got.len(), 1, "one record per one-record block");
            assert_eq!(got, want, "gated and inline re-reads agree (block {block})");
        }
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (BLOCKS, 0));
        assert_eq!(total_inline(&gate), 0, "nothing ran inline");
    }

    /// A columnar block job that panics on the gate fails the scan, as its
    /// first and only item, with the fetcher's corrupt error for the object:
    /// the class the fetcher's own gated block decode reports for a panic.
    ///
    /// Fails with `log_block_gate_failed` mapping `Panicked` to the transient
    /// store error (the match reads `Store`), and with the job's panic hook
    /// removed (the stream yields the fixture's four batches instead).
    #[tokio::test]
    async fn a_panicking_columnar_block_job_fails_the_scan_as_corrupt() {
        const PANIC_KEY: &str = "t/gate-panic.rlog";
        let (store, segments) = fixture_at(PANIC_KEY).await;
        let gate = floor_zero_gate();
        PANIC_NEXT_COLUMNAR_JOB
            .lock()
            .expect("panic hook")
            .push(PANIC_KEY.to_string());
        let gated = exec(&store, &segments, Some(Arc::clone(&gate)), false);
        let items: Vec<DFResult<RecordBatch>> = gated
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .collect()
            .await;
        assert_eq!(items.len(), 1, "the failure ends the stream");
        let Some(Err(DataFusionError::External(err))) = items.into_iter().next() else {
            panic!("the scan's only item is an external error");
        };
        match err.downcast_ref::<SqlError>() {
            Some(SqlError::LogFetch(LogFetchError::Corrupt {
                key,
                source: LogSegError::Corrupted(message),
            })) => {
                assert_eq!(key, PANIC_KEY);
                assert_eq!(
                    message,
                    &format!("read CPU gate: {}", CpuGateError::Panicked)
                );
            }
            other => panic!("expected the corrupt log fetch error, got {other:?}"),
        }
        assert_eq!(
            site_counts(&gate, ReadSite::LogBlock),
            (1, 0),
            "the first block's job took a permit and panicked"
        );
    }

    /// A columnar block job that never ran, cancelled or refused by a closed
    /// gate, maps to the transient store error the fetcher's `gate_not_run`
    /// builds for its own gated decodes: `StoreError::Transient` carrying
    /// "read CPU gate: " and the gate error. Checked on the mapping itself:
    /// the gate reports `Cancelled` only for a blocking task the runtime
    /// cancelled at shutdown, which leaves nothing to poll the scan, and
    /// `ReadGate` has no way to close its semaphore.
    ///
    /// Fails with either variant mapped to `Corrupt`.
    #[test]
    fn a_columnar_block_job_that_never_ran_is_transient() {
        for gate_err in [CpuGateError::Cancelled, CpuGateError::Closed] {
            let err = log_block_gate_failed(KEY, gate_err);
            let DataFusionError::External(err) = err else {
                panic!("an external error for {gate_err:?}");
            };
            match err.downcast_ref::<SqlError>() {
                Some(SqlError::LogFetch(LogFetchError::Store {
                    key,
                    source: StoreError::Transient(message),
                })) => {
                    assert_eq!(key, KEY);
                    assert_eq!(message, &format!("read CPU gate: {gate_err}"));
                }
                other => panic!("expected a transient store error, got {other:?}"),
            }
        }
    }
}
