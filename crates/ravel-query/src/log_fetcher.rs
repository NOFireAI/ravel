//! LogSegmentFetcher: a thin fetch abstraction over one RLOG log segment
//! (crate `ravel-logseg`, docs/log-segment-format.md; ADR-0033).
//!
//! This is the log-signal sibling of [`crate::SegmentFetcher`], which serves
//! the RSEG metric path. The two never share code and never touch each other:
//! RSEG and RLOG share only conventions, not bytes. Where `SegmentFetcher`
//! re-implements the footer-first suffix-GET / range-chase / decode protocol
//! itself, an RLOG object is read through [`ravel_logseg::RlogReader`], which
//! already performs the whole open/prune/verify/decode pipeline internally.
//! This wrapper therefore does only what the reader cannot: it decides
//! per-object relevance from the catalog summary before fetching anything,
//! resolves stream-identifying attribute equalities against the object's
//! STREAM_DIR, and combines those with the caller's ts-range and word/phrase
//! predicates into one [`ravel_logseg::Predicate`] handed to
//! [`RlogReader::scan_pruned`], alongside the caller's prune-only channel
//! ([`LogQuery::prune`]). Skip-index, POSTINGS, and bloom pruning stay entirely
//! inside the reader; nothing here duplicates format-layer logic.
//!
//! One part of this is approximate, and callers must know it: the
//! stream-attribute matching in [`LogSegmentFetcher::matching_streams`] is a
//! byte-containment search that over-approximates. It can return streams that
//! do not carry the queried attribute as a genuine top-level resource or scope
//! attribute, so the records [`LogSegmentFetcher::fetch`] returns can include
//! records from such streams. Read that method's documentation before using
//! the stream-attribute filter for anything user-facing. Everything else here
//! is exact: ts-range pruning and the content predicates are evaluated exactly
//! by the reader.
//!
//! Two read shapes, split by object size (ADR-0107). The tenant-aware funnels
//! fetch an object at or below [`LogSegmentFetcher::with_block_range_threshold`]
//! (512 KiB by default) with a single [`GetRange::Full`], which is every small
//! RLOG object and every fixture here. Above it they read only the parts
//! skip-index pruning proved relevant, through [`BlockRangeFetcher`]: a suffix
//! probe that pins the etag, the directory sections, and coalesced ranges over
//! the relevant data, held as the fetched regions themselves (a
//! [`LogObjectBytes`], issue #2066) that the reader decodes from at the
//! object's absolute offsets. What a "relevant part" is depends on the object's
//! version. A version-3 object's block is one contiguous byte range, so the
//! ranges are candidate blocks and the projection is a decode choice only. A
//! version-4 object stores each row group's pages column-major and lists them
//! in PAGE_DIR (ADR-0699), so a block is not a byte range at all: the ranges
//! are the surviving blocks' pages inside each projected column's chunk, and
//! the [`ColumnSelection`] the scan passes to decode is the fetch selection too
//! (decision 5). Every GET of either shape is routed through ADR-0046's
//! read cache when one is wired, keyed by the extent it fetched, so concurrent
//! callers for the same extent collapse onto one request. The untenanted
//! [`LogSegmentFetcher::fetch`]/[`LogSegmentFetcher::fetch_accounted`] entry
//! points have no cache key and always read the whole object in one GET.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::EngineConfigError;
use crate::erasure::ErasurePredicate;
use crate::fetcher::{MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT, ReadCache, bound_runs, gate_not_run};
use crate::phase_accounting::{PhaseAccounting, PhaseWireByteCounter, QueryPhase};
use crate::reserved_bytes::attach_reservation;
use bytes::Bytes;
use ravel_cache::{CacheKey, ReadOutcome, SingleFlightError};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_cpu_gate::{CpuGateError, JobSize, ReadGate, ReadSite};
use ravel_logseg::block::NumStat;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, SectionDesc, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::skip_index::{Level0Entry, NumRangeArm, SkipIndex, merge_stats};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, BlockScan, ByteSource, ColumnSelection, ColumnarBlockView, LogRecord, LogSegError,
    LogStreamId, Predicate, RlogConfig, RlogReader, ScanStats, SegmentDirectories, SparseObject,
    SuffixOutcome, decode_section_accounted, open_from_suffix, read_section_accounted_from,
};
use ravel_object_store::{Etag, GetOutcome, GetRange, ObjectStoreBackend, StoreError};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::canonical_attr_bytes;
use tracing::Instrument;

/// Upper bound on STREAM_DIR entries accepted when decoding the directory out
/// of band (mirrors the reader's own internal cap). A directory claiming more
/// is treated as corrupt rather than allocated.
const MAX_STREAMS: u64 = 1 << 24;

/// An equality on a stream-identifying attribute: a resource or scope
/// attribute whose `(key, value)` participates in [`LogStreamId`] identity
/// (docs/log-segment-format.md "STREAM_DIR"). These are resolved against the
/// object's STREAM_DIR into a set of matching stream ids, never evaluated per
/// record (per-record attributes are not part of stream identity and are
/// matched through [`Predicate`] instead).
///
/// This is an approximate filter, not an exact one. Resolution is byte
/// containment over the stored canonical blob, so it also matches a stream
/// that carries the pair only nested inside a map or list attribute value
/// rather than as a top-level resource or scope attribute. See
/// [`LogSegmentFetcher::matching_streams`] for the exact guarantee (no false
/// negatives, possible false positives) and for what a caller has to do about
/// it.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamAttrEquals {
    pub key: String,
    pub value: AttrValue,
}

impl StreamAttrEquals {
    pub fn new(key: impl Into<String>, value: AttrValue) -> Self {
        StreamAttrEquals {
            key: key.into(),
            value,
        }
    }
}

/// One log query against a single segment: an inclusive ts range, zero or more
/// stream-attribute equalities (ANDed, resolved against STREAM_DIR), zero or
/// more content predicates (`HasWord`/`Equals`, ANDed, passed straight to the
/// reader as its exact per-row filter), and zero or more prune-only predicates.
/// The ts range is always applied; everything else is optional.
///
/// The ts range and the content predicates are exact. The `stream_attrs`
/// filters are not: they over-approximate, and a fetch can return records from
/// a stream that does not genuinely carry the requested attribute. See
/// [`LogSegmentFetcher::matching_streams`].
///
/// `prune` is not a filter at all: its arms drive POSTINGS block pruning inside
/// [`RlogReader::scan_pruned`] and are never evaluated per row, so they never
/// remove a record from the result. A query built without
/// [`with_prune`](Self::with_prune) reads and returns exactly what it did before
/// the channel existed: an empty `prune` makes `scan_pruned` equivalent to
/// `scan`.
#[derive(Clone, Debug, PartialEq)]
pub struct LogQuery {
    pub ts_min_ns: i64,
    pub ts_max_ns: i64,
    pub stream_attrs: Vec<StreamAttrEquals>,
    pub content: Vec<Predicate>,
    /// Prune-only predicates (today: `Equals` on `FieldSel::Attr`, from
    /// `ravel_sql::LogsPushdown::prune`). These narrow which blocks the fetch
    /// decodes and nothing else: the reader never evaluates them per row, so
    /// adding an arm can only reduce work, never rows. An arm whose field the
    /// object's POSTINGS index does not cover prunes nothing at all
    /// (docs/adrs/0049-rlog-postings.md decision 5, ADR-0013's widen-only
    /// rule).
    ///
    /// A caller that needs the predicate to actually filter must evaluate it
    /// itself (in SQL: DataFusion's `Inexact` residual over the merged `attrs`
    /// column, which stays the sole exact evaluator). Putting a merged-view
    /// attribute equality in `content` instead would drop every record whose
    /// match lives only in its resource or scope attributes.
    pub prune: Vec<Predicate>,
    /// Pending selective-erasure predicates for this query's resolved snapshot
    /// (ADR-0064 decision 2). Every decoded row whose per-record
    /// attributes match any predicate (intersected with the predicate's
    /// event-time window) is dropped in [`scan_bytes`](LogSegmentFetcher::
    /// scan_bytes), after the fetch and after any cache layer, before the
    /// result reaches the caller. Empty for a query with no pending erasure,
    /// which reads and returns exactly what it did before this field existed.
    ///
    /// The caller populates this from the resolved snapshot's attached
    /// predicates. The resolver already surfaces them: it attaches every pending
    /// request to `Snapshot::pending_erasure` on each resolve. The SQL log scan
    /// makes the last hop: `ravel_sql::logs_scan::LogsScanExec::execute` calls
    /// [`LogQuery::with_erasure`] with the snapshot's pending predicates, so log
    /// erasure exclusion is live on the SQL surface. The metric surface needs no
    /// such hop: `QueryEngine` reads `Snapshot::pending_erasure` directly at its
    /// own fetch funnels.
    pub erasure: Vec<ErasurePredicate>,
}

impl LogQuery {
    /// A query over the inclusive ts range `[ts_min_ns, ts_max_ns]` with no
    /// stream-attribute, content, or prune predicates.
    pub fn new(ts_min_ns: i64, ts_max_ns: i64) -> Self {
        LogQuery {
            ts_min_ns,
            ts_max_ns,
            stream_attrs: Vec::new(),
            content: Vec::new(),
            prune: Vec::new(),
            erasure: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_stream_attr(mut self, filter: StreamAttrEquals) -> Self {
        self.stream_attrs.push(filter);
        self
    }

    #[must_use]
    pub fn with_content(mut self, pred: Predicate) -> Self {
        self.content.push(pred);
        self
    }

    /// Adds one prune-only predicate (see [`LogQuery::prune`]). Adding an arm
    /// changes which blocks the fetch decodes, never which records it returns.
    #[must_use]
    pub fn with_prune(mut self, pred: Predicate) -> Self {
        self.prune.push(pred);
        self
    }

    /// Attaches the resolved snapshot's pending erasure predicates (see
    /// [`LogQuery::erasure`]). Rows matching any of them are excluded at scan
    /// time, after fetch and after cache.
    #[must_use]
    pub fn with_erasure(mut self, predicates: Vec<ErasurePredicate>) -> Self {
        self.erasure = predicates;
        self
    }

    /// Whether this query carries no block-level predicate that could exclude a
    /// block: no content filter, no prune-only arm, no stream-attribute
    /// equality, and no pending erasure. The always-present ts range is not
    /// consulted here; ts pruning is a separate span check the caller applies
    /// (see [`LogSegmentFetcher::plan_segment`]).
    ///
    /// For such a query every block in a ts-contained segment survives pruning,
    /// so the survivor count is the segment's total block count with no fetch or
    /// decode work. Erasure is folded in and read fail-closed on purpose: it
    /// filters rows, not blocks, so it never changes the survivor count, but
    /// treating a query with pending erasure as predicate-free would let the
    /// fast path fire in a state the plan phase should stay conservative about.
    ///
    /// Destructures `Self` by name rather than checking fields ad hoc: this
    /// gates a query-correctness invariant (a false positive here silently
    /// drops rows via the plan fast path), so a future field addition to
    /// `LogQuery` must be a build break here, not a silent gap.
    #[must_use]
    pub fn is_block_predicate_free(&self) -> bool {
        let Self {
            ts_min_ns: _,
            ts_max_ns: _,
            stream_attrs,
            content,
            prune,
            erasure,
        } = self;
        content.is_empty() && prune.is_empty() && stream_attrs.is_empty() && erasure.is_empty()
    }
}

/// Exact per-block figures for one segment, derived from its SKIP_IDX alone
/// (#698 deliverable 2, ADR-0699): how many records sit in blocks the query
/// window fully contains, the merged per-numeric-column stats over exactly those
/// blocks, and the indices of the blocks the window only partially overlaps.
///
/// Everything here is exact, not an estimate. A block whose `[min_ts, max_ts]`
/// lies inside `[query.ts_min_ns, query.ts_max_ns]` contributes every one of its
/// records to the answer, so its stored `record_count` and its stored
/// [`NumStat`]s are the truth for it and no block byte has to move. A block the
/// window merely clips contributes an unknown subset, so it is named in
/// `partial_block_indices` and left for the caller to decode. A block the window
/// misses entirely contributes nothing and appears nowhere.
///
/// # No production caller yet
///
/// Nothing in this commit calls
/// [`plan_segment_block_stats`](LogSegmentFetcher::plan_segment_block_stats).
/// The caller is #698 deliverable 1 (fleet task ca9c1b10): `ravel_sql`'s
/// `LogsScanExec::statistics`, which feeds DataFusion's `AggregateStatistics`
/// so a `COUNT(*)`/`MIN`/`MAX` over a segment can be answered from the plan
/// instead of a scan. Until that lands the epic capability is NOT reachable end
/// to end.
///
/// ADR-0699's RLOG row groups plus PAGE_DIR make "read only the footer, the skip
/// index, and the page directory" structural for every statement on the next
/// format version. This type is the version-3 equivalent of that read, reachable
/// today against the format on disk.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockStatsReport {
    /// Summed `record_count` over every block the query window fully contains.
    /// Exact: containment means no row of those blocks is filtered out by the ts
    /// range, and the fail-closed conditions on
    /// [`plan_segment_block_stats`](LogSegmentFetcher::plan_segment_block_stats)
    /// guarantee nothing else can filter one either.
    pub record_count: u64,
    /// [`ravel_logseg::skip_index::merge_stats`] over exactly the fully
    /// contained blocks' [`Level0Entry`] values, with that function's own
    /// semantics unchanged: `FieldType`-aware `total_cmp` min/max, OR-ed
    /// `has_nan`, and a `null_count` that folds in the whole `record_count` of
    /// any contained block carrying no stat for the column (no entry means the
    /// block is all-null for it).
    pub stats: Vec<NumStat>,
    /// Indices into `SkipIndex::l0` of every block that overlaps the query
    /// window without being contained by it. The caller decodes these itself and
    /// adds their contribution to the figures above. The length is whatever the
    /// segment's block spans produce: a ts-ordered segment yields at most one at
    /// each end, but RLOG sorts rows by `(stream_ref, ts)`, so a multi-stream
    /// segment interleaves spans and can yield many more.
    pub partial_block_indices: Vec<usize>,
}

/// The records matching one fetch, plus the reader's own scan pruning counters.
#[derive(Clone, Debug)]
pub struct LogFetchOutput {
    pub records: Vec<LogRecord>,
    pub stats: ScanStats,
}

/// A fetched, pruned, not-yet-decoded scan over one segment (ADR-0087).
///
/// This is the streaming counterpart of [`LogFetchOutput`]: the object's bytes
/// are resident and its blocks are pruned, but no block has been decoded. The
/// caller pulls one block at a time with [`next_block`](Self::next_block) and
/// can drop each block's records before asking for the next, so peak decoded
/// memory is one block rather than one segment.
///
/// Everything else is identical to [`LogSegmentFetcher::fetch`]: the same
/// combined predicate is the exact per-row filter, the same prune channel drives
/// POSTINGS, and the same selective-erasure exclusion
/// ([`crate::erasure::retain_log_records`]) is applied to each block's rows
/// after fetch and after cache. Draining this to exhaustion yields exactly the
/// records `fetch` would return, in the same order, which is how `fetch` is
/// implemented.
pub struct LogSegmentScan {
    /// The object's bytes as the fetch holds them, whole or placed. Block
    /// extents are absolute offsets into the object.
    bytes: LogObjectBytes,
    /// `None` only once a gated block decode that held the cursor failed or
    /// was abandoned; every later call then fails rather than skip blocks.
    scan: Option<BlockScan>,
    /// The gate failure that lost the cursor, so a later call reports the same
    /// class the losing call reported (a panicked decode stays a `Corrupt`,
    /// not a redacted permanent store error). `None` when the cursor was
    /// abandoned instead: a caller dropped the future while the job was queued
    /// or running, and no classified failure exists to repeat.
    lost_to: Option<CpuGateError>,
    /// The cursor's counters as of the last time it was held here, reported
    /// once `scan` is gone.
    lost_stats: ScanStats,
    erasure: Vec<ErasurePredicate>,
    /// The log path's `decode` span, entered around each block decode and
    /// completed with the block counters when the scan runs out.
    span: tracing::Span,
    /// The object key, for error attribution.
    key: String,
    /// This query's accounting handle, folded once at exhaustion with the
    /// scan's decode-time `page_bytes_fetched`/`page_bytes_decoded` totals
    /// (ADR-0107 decision 4). The same handle T1's fetch path already recorded
    /// wire bytes against; these are a separate, additive axis.
    accounting: QueryAccounting,
    /// Set once [`next_block`](Self::next_block) has reported exhaustion, so
    /// the span's counters are recorded exactly once.
    finished: bool,
    /// The read CPU gate [`next_block_on_gate`](Self::next_block_on_gate)
    /// decodes on; `None` decodes inline.
    read_gate: Option<Arc<ReadGate>>,
    /// The job size of each gated block decode: the largest uncompressed
    /// block in the object's PAGE_DIR, since the cursor does not say which
    /// block it decodes next.
    block_job_bytes: u64,
    /// Test hook: makes the next gated block decode panic inside the job, the
    /// only way to drive a real [`CpuGateError::Panicked`] through this path.
    #[cfg(test)]
    panic_next_gate_job: bool,
}

impl LogSegmentScan {
    /// The reader's pruning counters. `blocks_scanned`, `pages_decoded`, and
    /// `pages_skipped` grow as blocks are drained; read this after the last
    /// [`next_block`](Self::next_block) for the whole segment's figures.
    pub fn stats(&self) -> ScanStats {
        self.scan.as_ref().map_or(self.lost_stats, BlockScan::stats)
    }

    /// Surviving blocks not yet decoded.
    pub fn remaining_blocks(&self) -> usize {
        self.scan.as_ref().map_or(0, BlockScan::remaining_blocks)
    }

    /// The whole-object block index of the block the next
    /// [`next_block`](Self::next_block) or
    /// [`next_block_columnar`](Self::next_block_columnar) call will decode, or
    /// `None` once the scan is exhausted or its cursor is lost to a gate
    /// failure. See [`BlockScan::next_block_index`]: it is read from the
    /// cursor's own survivor list, not from a position into a
    /// separately-tracked index list that can drift out of step with it.
    pub fn next_block_index(&self) -> Option<usize> {
        self.scan.as_ref().and_then(BlockScan::next_block_index)
    }

    /// Arms the test hook: the next gated block decode panics inside its job.
    #[cfg(test)]
    pub(crate) fn panic_next_gate_job_for_test(&mut self) {
        self.panic_next_gate_job = true;
    }

    /// [`next_block`](Self::next_block) with the block decode run on the
    /// fetcher's read gate (ADR-1702 decision 4): the whole block, all its
    /// pages, is one job, so the decode leaves the runtime worker once the
    /// block is over the gate's byte floor. Every block of an object is sized
    /// at that object's largest block (`block_job_bytes`), not at its own: the
    /// cursor does not say which block it decodes next. Without a gate this is
    /// `next_block`. Reaching exhaustion submits no job.
    pub async fn next_block_on_gate(&mut self) -> Result<Option<Vec<LogRecord>>, LogFetchError> {
        let Some(gate) = self.read_gate.clone() else {
            return self.next_block();
        };
        if self.remaining_blocks() == 0 {
            return self.next_block();
        }
        let Some(mut scan) = self.scan.take() else {
            return Err(self.lost_error());
        };
        self.lost_stats = scan.stats();
        let bytes = self.bytes.clone();
        let span = self.span.clone();
        #[cfg(test)]
        let panic_in_job = std::mem::take(&mut self.panic_next_gate_job);
        let ran = gate
            .run(
                ReadSite::LogBlock,
                JobSize::Bytes(self.block_job_bytes),
                move || {
                    #[cfg(test)]
                    assert!(!panic_in_job, "injected gated block decode panic");
                    let decoded = span.in_scope(|| scan.next_block(&bytes));
                    (scan, decoded)
                },
            )
            .await;
        let (scan, decoded) = match ran {
            Ok(ran) => ran,
            Err(err) => {
                // The cursor went into the job, so it is gone whatever the
                // failure was. Keep the failure so a later call repeats its
                // class rather than degrading to a permanent store error.
                self.lost_to = Some(err);
                return Err(log_gate_failed(&self.key, err));
            }
        };
        self.scan = Some(scan);
        self.finish_block(decoded)
    }

    /// What an exit reports once the cursor is gone: the classified gate
    /// failure that lost it when there was one, and the bare permanent
    /// [`scan_lost`] when the cursor was abandoned instead.
    fn lost_error(&self) -> LogFetchError {
        match self.lost_to {
            Some(err) => log_gate_failed(&self.key, err),
            None => scan_lost(&self.key),
        }
    }

    /// Decode the next surviving block and return its matching, unerased rows,
    /// or `None` once every surviving block has been decoded.
    ///
    /// `Some(vec![])` is normal and distinct from `None`: a block can survive
    /// pruning and hold no row that matches the exact filter, or have every
    /// matching row erased. Only `None` ends the scan.
    pub fn next_block(&mut self) -> Result<Option<Vec<LogRecord>>, LogFetchError> {
        let Some(scan) = self.scan.as_mut() else {
            return Err(self.lost_error());
        };
        let bytes = &self.bytes;
        let decoded = self.span.in_scope(|| scan.next_block(bytes));
        self.finish_block(decoded)
    }

    /// The row exit's handling of one decoded block, shared by the inline and
    /// the gated decode.
    fn finish_block(
        &mut self,
        decoded: Result<Option<Vec<LogRecord>>, LogSegError>,
    ) -> Result<Option<Vec<LogRecord>>, LogFetchError> {
        let decoded = decoded.map_err(|source| corrupt(&self.key, source))?;
        let Some(mut records) = decoded else {
            self.finish();
            return Ok(None);
        };
        // Selective-erasure exclusion (ADR-0064 decision 2), per block rather
        // than per segment. Filtering a block's rows and filtering the
        // concatenation of every block's rows give the same survivors: the
        // predicate is per record and carries no cross-record state.
        crate::erasure::retain_log_records(&mut records, &self.erasure);
        Ok(Some(records))
    }

    /// Whether a pending erasure predicate applies to this scan, which is what
    /// makes [`next_block_columnar`](Self::next_block_columnar) refuse.
    pub fn erasure_pending(&self) -> bool {
        !self.erasure.is_empty()
    }

    /// Columnar counterpart of [`next_block`](Self::next_block) (ADR-0099
    /// decision 1): the same block, the same surviving rows in the same order,
    /// handed out as a borrowed [`ColumnarBlockView`] instead of rebuilt
    /// records. Same object bytes, same pruning, same `decode` span and the same
    /// block counters recorded on exhaustion.
    ///
    /// The returned view borrows this scan, so it must be dropped before the
    /// next call to either exit.
    ///
    /// # This exit refuses when erasure is pending
    ///
    /// [`next_block`](Self::next_block) excludes rows matching a pending
    /// erasure predicate ([`crate::erasure::retain_log_records`], ADR-0064
    /// decision 2). That exclusion is record-level and there is no columnar
    /// form of it yet, so a view cannot honour it and this method hands one out
    /// only when the scan carries no erasure predicate; otherwise it returns
    /// [`ColumnarBlockOutcome::ErasurePending`] without decoding anything or
    /// advancing the cursor, and the caller must drain the row exit instead.
    ///
    /// Failing closed here is deliberate: the failure mode of getting erasure
    /// wrong is an erased record served to a client, not a slow query
    /// (ADR-0099 decision 2).
    pub fn next_block_columnar(&mut self) -> Result<ColumnarBlockOutcome<'_>, LogFetchError> {
        if !self.erasure.is_empty() {
            return Ok(ColumnarBlockOutcome::ErasurePending);
        }
        // Exhaustion is reported from the block count rather than from a `None`
        // out of the cursor. The returned view borrows the cursor for as long as
        // the caller holds it, which is longer than the counter-recording read
        // of `scan.stats()` could borrow it for, so the two cannot share one
        // call site.
        if self.scan.is_some() && self.remaining_blocks() == 0 {
            self.finish();
            return Ok(ColumnarBlockOutcome::Exhausted);
        }
        // Destructured so entering the span borrows only `span` and the view's
        // borrow of the cursor is not held by a closure.
        let Self {
            bytes,
            scan: Some(scan),
            span,
            key,
            ..
        } = self
        else {
            return Err(self.lost_error());
        };
        let entered = span.enter();
        let decoded = scan.next_block_columnar(bytes);
        drop(entered);
        match decoded.map_err(|source| corrupt(key, source))? {
            Some(view) => Ok(ColumnarBlockOutcome::Block(view)),
            // Unreachable: a cursor with blocks remaining yields one. Reported
            // as exhaustion rather than unwrapped, and `finished` is left unset
            // so a following call still records the counters.
            None => Ok(ColumnarBlockOutcome::Exhausted),
        }
    }

    /// Records the scan's block counters on the `decode` span, exactly once,
    /// when an exit reports exhaustion or, failing that, when the scan is
    /// dropped (see the `Drop` impl below).
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let stats = self.stats();
        self.span.record("blocks_scanned", stats.blocks_scanned);
        self.span.record("blocks_total", stats.blocks_total);
        self.span
            .record("decompressed_bytes", stats.decompressed_bytes);
        // Decode-time column-filtering accounting (ADR-0107 decision 4), folded
        // once at exhaustion into the query's handle: page_bytes_fetched vs.
        // page_bytes_decoded expose how much of each fetched block a narrow
        // projection discards. A separate, additive axis from the wire bytes T1
        // records through `add_s3_bytes`; see the QueryAccounting field docs.
        self.accounting
            .add_page_bytes_fetched(stats.page_bytes_fetched);
        self.accounting
            .add_page_bytes_decoded(stats.page_bytes_decoded);
        // Bytes zstd produced opening this object's directory sections and
        // decoding its scanned block pages (issue #1401). This is the scan
        // funnel's decompressed-byte output; charged to the `scan` phase, the
        // same handle its page-byte figures land on, so a warm-cache scan that
        // moves no wire bytes still reports the decode work it did.
        self.accounting
            .add_decompressed_bytes(stats.decompressed_bytes);
    }
}

impl Drop for LogSegmentScan {
    /// A scan a caller abandons before exhaustion (`GlobalLimitExec` dropping
    /// the stream once a `LIMIT` is satisfied is the reachable case) never
    /// hits either `next_block` exit's exhaustion arm, so without this,
    /// `finish` never runs and the partial decode work it already did is
    /// missing from the query's accounting (#617). `finish` is idempotent, so
    /// this is a no-op on the already-exhausted path.
    fn drop(&mut self) {
        self.finish();
    }
}

/// What [`LogSegmentScan::next_block_columnar`] produced.
///
/// Three outcomes rather than an `Option` because "no view" has two distinct
/// causes that a caller must not conflate: the scan ran out of blocks, or it
/// carries a pending erasure predicate the columnar path cannot evaluate. An
/// `Option` would make the second look like the first and silently truncate a
/// query's results.
#[derive(Debug)]
pub enum ColumnarBlockOutcome<'a> {
    /// The next surviving block, as a borrowed columnar view.
    Block(ColumnarBlockView<'a>),
    /// Every surviving block has been decoded. `Block(view)` with a zero
    /// surviving-row count is different: that block survived pruning and simply
    /// held no matching row.
    Exhausted,
    /// A pending erasure predicate applies to this scan, so no view is handed
    /// out. Nothing was decoded and the cursor did not advance; drain
    /// [`LogSegmentScan::next_block`] instead.
    ErasurePending,
}

/// Errors fetching and decoding one RLOG segment. Every variant is a hard
/// error: the caller never receives partial or silently-wrong data.
#[derive(Debug, thiserror::Error)]
pub enum LogFetchError {
    #[error("object store error reading log segment {key}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("corrupt log segment {key}: {source}")]
    Corrupt {
        key: String,
        #[source]
        source: LogSegError,
    },
    /// The object's etag changed between the block-range fetcher's
    /// etag-establishing probe and a later block-range or metadata GET, so the
    /// store returned bytes from two different object states mid-sequence. The
    /// single-GET whole-object funnels never observe this (one GET, one state);
    /// [`BlockRangeFetcher`]'s multi-GET sequence removes that property and must
    /// replace it explicitly, mirroring `crate::FetchError::EtagChanged`
    /// (ADR-0107 decision 1).
    #[error("etag changed between reads of log segment {key}: store returned inconsistent data")]
    EtagChanged { key: String },
    /// The fetch-layer memory budget (ADR-1170 decision 2) refused the
    /// reservation for the bytes this GET would materialize. Carries only the
    /// three accounting figures, never an object key or tenant value: the
    /// refusal is a resource condition, not corruption, and must not leak which
    /// object or tenant provoked it. Mirrors
    /// [`crate::FetchError::FetchMemoryExhausted`].
    #[error(
        "fetch memory exhausted: requested {requested} bytes, {reserved} of {limit} byte budget already reserved"
    )]
    FetchMemoryExhausted {
        requested: u64,
        reserved: u64,
        limit: u64,
    },
    /// A [`CarriedWholeObject`] produced by one `plan_segment` read was supplied
    /// to a read of a different object, or of the same object under a different
    /// tenant. The carry short-circuits the fetch, so decoding it would answer
    /// from the wrong object's bytes whenever both are decodable, rather than
    /// fail; the mismatch is rejected before any decode. Terminal like
    /// `SpanFetchError::TenantMismatch`, never a retry: the pairing is fixed by
    /// the caller, so the same request retried anywhere reproduces it.
    #[error(
        "carried whole object from {carried_key} (tenant {carried_tenant:?}) was supplied to a \
         read of log segment {key} (tenant {tenant:?})"
    )]
    CarryMismatch {
        key: String,
        carried_key: String,
        tenant: TenantHash,
        carried_tenant: TenantHash,
    },
}

/// Fetches and scans one RLOG log segment at a time. Constructed with the same
/// [`ObjectStoreBackend`] trait object [`crate::SegmentFetcher`] takes.
#[derive(Clone)]
pub struct LogSegmentFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RlogConfig,
    /// ADR-0046's read cache, consulted by
    /// [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant) --
    /// the only funnel here that can supply the `tenant_hash` a cache key
    /// needs. `fetch`/`fetch_accounted` never consult it and are unchanged by
    /// its presence: the one production `LogSegmentFetcher` is shared across
    /// all tenants (`services/ravel-server/src/query.rs`), so caching cannot
    /// be wired into a method with no per-call tenant identity. Either tier
    /// configuration (see [`ReadCache`]); every production caller builds the
    /// RAM variant.
    cache: Option<ReadCache>,
    /// Object size above which a tenant-aware fetch reads only the
    /// pruning-relevant blocks through [`Self::block_range`] instead of the
    /// whole object (ADR-0107). At or below it the whole-object path in
    /// [`tenant_bytes`](Self::tenant_bytes) is unchanged, which keeps every
    /// small-object read (all current test fixtures, and RLOG's typical object
    /// size) byte-for-byte as before.
    block_range_threshold: u64,
    /// The block-range fetcher used for objects above `block_range_threshold`.
    /// Kept in sync with `store`/`cfg`/`cache` by the builders.
    block_range: BlockRangeFetcher,
    /// Bounds this fetcher's OWN whole-object GETs (`fetch_accounted` and
    /// `whole_object_bytes`'s two sites), the funnel every object at or below
    /// `block_range_threshold` routes through. Distinct from
    /// `block_range`'s limiter, which bounds only the above-threshold
    /// ranged path; [`Self::with_get_limiter`] sets both to the same `Arc` so
    /// a caller shares one pool across both funnels, and
    /// [`Self::with_block_range`] re-applies this field's current limiter to
    /// the replacement `BlockRangeFetcher` so builder order cannot silently
    /// drop it (ADR-1195).
    get_limiter: Arc<crate::GetLimiter>,
    /// Per-phase tail-section probe misses across every read this fetcher has
    /// served (#883). Shared by every clone, like `block_range`'s GET semaphore,
    /// and read through [`probe_miss_counter`](Self::probe_miss_counter).
    probe_misses: ProbeMissCounter,
    /// Per-phase WIRE bytes across every read this fetcher has served (#913).
    /// Shared by every clone, and pushed into `block_range` by every builder
    /// below so this fetcher's own whole-object GETs and the block-range
    /// fetcher's ranged GETs accumulate into one set of totals. Read through
    /// [`phase_wire_byte_counter`](Self::phase_wire_byte_counter).
    wire_bytes: PhaseWireByteCounter,
    /// The process-wide fetch memory budget (ADR-1170 decision 2). This
    /// fetcher's own whole-object GETs (`fetch_accounted` and
    /// `whole_object_bytes`) reserve the object's size against it before the
    /// GET and own the reservation for the fetched buffer's lifetime;
    /// `block_range` reserves its own ranged reads against the same budget,
    /// which every builder below keeps in sync (like `wire_bytes` and
    /// `get_limiter`). Default [`ravel_memory::MemoryBudget::unlimited`], so a
    /// fetcher built with plain `new` never refuses.
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    /// The read CPU gate [`LogSegmentScan::next_block_on_gate`] decodes blocks
    /// on (ADR-1702 decision 4). `None`, the default, decodes inline.
    read_gate: Option<Arc<ReadGate>>,
}

impl LogSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        let wire_bytes = PhaseWireByteCounter::new();
        let memory_budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        LogSegmentFetcher {
            store: store.clone(),
            cfg: RlogConfig::default(),
            cache: None,
            block_range_threshold: DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            block_range: BlockRangeFetcher::new(store)
                .with_wire_byte_counter(wire_bytes.clone())
                .with_memory_budget(memory_budget.clone()),
            get_limiter: Arc::new(crate::GetLimiter::new_unchecked(
                DEFAULT_LOG_MAX_CONCURRENT_GETS,
            )),
            probe_misses: ProbeMissCounter::new(),
            wire_bytes,
            memory_budget,
            read_gate: None,
        }
    }

    /// This fetcher's accumulating per-phase WIRE byte counter (#913): the
    /// numerator channel of fetch amplification. Clone it out before handing
    /// the fetcher to whatever will own it, then read
    /// [`PhaseWireByteCounter::snapshot`] on each side of an execution to
    /// attribute that execution's bytes; the counter itself never resets.
    ///
    /// Every figure it holds is WIRE bytes, never stored or decompressed
    /// bytes. The denominator of fetch amplification is
    /// `QueryAccountingSnapshot::page_bytes_decoded`, which counts STORED page
    /// bytes; the two are different quantities and must never be summed.
    ///
    /// Survives every builder below, including
    /// [`with_block_range`](Self::with_block_range), which replaces the
    /// `BlockRangeFetcher` but re-attaches this counter to the replacement.
    #[must_use]
    pub fn phase_wire_byte_counter(&self) -> PhaseWireByteCounter {
        self.wire_bytes.clone()
    }

    /// This fetcher's accumulating per-phase probe-miss counter (#883). Clone it
    /// out before handing the fetcher to whatever will own it, then read
    /// [`ProbeMissCounter::snapshot`] around each execution to attribute misses
    /// to that execution; the counter itself never resets.
    ///
    /// Survives every builder below, including
    /// [`with_block_range`](Self::with_block_range), which replaces the
    /// `BlockRangeFetcher` but not this.
    #[must_use]
    pub fn probe_miss_counter(&self) -> ProbeMissCounter {
        self.probe_misses.clone()
    }

    /// Record one block-range read's probe misses on both channels that report
    /// them: the `page_fetch` span field, beside the `s3_requests`/`s3_bytes`
    /// the same read produced, and this fetcher's [`ProbeMissCounter`] under
    /// `phase`. Written by one call so the span and the counter cannot drift
    /// apart into two accounting channels that disagree.
    fn record_probe_misses(
        &self,
        span: &tracing::Span,
        stats: &BlockRangeStats,
        phase: ProbePhase,
    ) {
        span.record("probe_misses", stats.probe_misses);
        self.probe_misses.record(phase, stats.probe_misses);
    }

    /// Overrides the [`RlogConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RlogConfig) -> Self {
        self.cfg = cfg;
        self.block_range = self.block_range.with_config(cfg);
        self
    }

    /// Wires ADR-0046's read cache into
    /// [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant) and,
    /// per block, into the block-range path (ADR-0107 decision 3). Accepts
    /// either tier configuration through [`ReadCache`] (an `Arc<Cache<..>>` or
    /// an `Arc<TieredCache<..>>` both convert via [`From`]), so existing call
    /// sites are unchanged.
    #[must_use]
    pub fn with_cache(mut self, cache: impl Into<ReadCache>) -> Self {
        let cache = cache.into();
        self.block_range = self.block_range.with_cache(cache.clone());
        self.cache = Some(cache);
        self
    }

    /// Overrides the object-size threshold above which the tenant-aware fetch
    /// path reads only pruning-relevant blocks (ADR-0107). Also sets the
    /// block-range fetcher's own size-threshold pre-probe crossover to the same
    /// value, so the two agree: an object routed here (size above `n`) never
    /// trips the inner "small object, read whole" crossover. `0` routes every
    /// object through the true ranged path, which the tests use on small
    /// fixtures.
    #[must_use]
    pub fn with_block_range_threshold(mut self, n: u64) -> Self {
        self.block_range_threshold = n;
        self.block_range = self.block_range.with_whole_object_threshold(n);
        self
    }

    /// Bounds the in-flight object-store GETs of BOTH RLOG paths with one
    /// private [`crate::GetLimiter`] of `n` permits (0 is clamped to 1): the
    /// fetcher's own whole-object funnel and the block-range path share it, so
    /// a standalone fetcher built this way is governed by `n` on either path
    /// and not by [`DEFAULT_LOG_MAX_CONCURRENT_GETS`]. This is the seam
    /// `--fetch-concurrency` (ADR-0088) reaches the logs signal through; a scan
    /// planned at more partitions than this pool has permits queues on it
    /// (issue #700). A fetcher a `QueryEngine` owns is wired with
    /// [`Self::with_get_limiter`] to the engine's shared limiter instead.
    #[must_use]
    pub fn with_max_concurrent_gets(self, n: usize) -> Self {
        let limiter = Arc::new(crate::GetLimiter::new_unchecked(n.max(1)));
        self.with_get_limiter(limiter)
    }

    /// Wires this fetcher's OWN whole-object GETs and the block-range path to
    /// the same caller-owned [`crate::GetLimiter`] (ADR-1195), so both funnels
    /// draw permits from the same pool as every other fetcher (and, via
    /// [`crate::QueryEngine::with_get_limiter`], every other engine) holding
    /// the same `Arc`.
    #[must_use]
    pub fn with_get_limiter(mut self, limiter: std::sync::Arc<crate::GetLimiter>) -> Self {
        self.get_limiter = Arc::clone(&limiter);
        self.block_range = self.block_range.with_get_limiter(limiter);
        self
    }

    /// Wires this fetcher's OWN whole-object GETs and the block-range path to
    /// the same caller-owned [`ravel_memory::MemoryBudget`] (ADR-1170 decision
    /// 2), so both funnels reserve against the same budget as every other
    /// fetcher (and, via [`crate::QueryEngine::with_memory_budget`], every other
    /// engine) holding the same `Arc`. Mirrors [`Self::with_get_limiter`].
    #[must_use]
    pub fn with_memory_budget(mut self, budget: Arc<ravel_memory::MemoryBudget>) -> Self {
        self.memory_budget = Arc::clone(&budget);
        self.block_range = self.block_range.with_memory_budget(budget);
        self
    }

    /// Runs this fetcher's RLOG decodes on `gate` (ADR-1702 decision 4), each
    /// as one job:
    ///
    /// - every scan open, the directory sections plus the POSTINGS probe, as a
    ///   `log_postings` job, on every funnel that opens one;
    /// - every surviving block, with all its pages, as a `log_block` job: in
    ///   [`fetch_accounted`](Self::fetch_accounted) and
    ///   [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant),
    ///   and in a [`LogSegmentScan`] drained through
    ///   [`LogSegmentScan::next_block_on_gate`] (its `next_block` and
    ///   `next_block_columnar` exits still decode inline);
    /// - every directory section the block-range path decodes on its own
    ///   (SKIP_IDX, PAGE_DIR, FIELD_DIR, and the planning reads' sections), as
    ///   a `log_section` job.
    ///
    /// Two decodes stay inline: a direct
    /// [`matching_streams`](Self::matching_streams) call, and the STREAM_DIR
    /// decode of `fetch_stream_dir`'s whole-object fallback.
    #[must_use]
    pub fn with_read_gate(mut self, gate: Arc<ReadGate>) -> Self {
        self.block_range = self.block_range.with_read_gate(Arc::clone(&gate));
        self.read_gate = Some(gate);
        self
    }

    /// Opens the pruned scan over `bytes` ([`open_scan`](Self::open_scan), or,
    /// when `indices` is set, [`open_scan_subset`](Self::open_scan_subset) for
    /// ordinal survivor positions or
    /// [`open_scan_raw_subset`](Self::open_scan_raw_subset) for whole-object
    /// block indices depending on `raw`) inside `span`. With a read gate set
    /// the open is one `log_postings` job (ADR-1702 decision 4): it decodes
    /// the directory sections and probes POSTINGS, which `RlogReader::scan_blocks`
    /// runs internally, so the probe cannot be a job of its own from this
    /// crate. With `block_sizes` the job also reads the job size of each
    /// later block decode; without a gate that size is 0 and unread.
    #[allow(clippy::too_many_arguments)]
    async fn open_scan_on_gate(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: Option<&[usize]>,
        raw: bool,
        expected_survivors: &[usize],
        block_sizes: bool,
        accounting: &QueryAccounting,
        span: &tracing::Span,
    ) -> Result<(BlockScan, u64), LogFetchError> {
        let Some(gate) = &self.read_gate else {
            let scan = span.in_scope(|| match indices {
                None => self.open_scan(key, bytes, query, columns, accounting),
                Some(indices) if raw => self.open_scan_raw_subset(
                    key,
                    bytes,
                    query,
                    columns,
                    indices,
                    expected_survivors,
                    accounting,
                ),
                Some(indices) => {
                    self.open_scan_subset(key, bytes, query, columns, indices, accounting)
                }
            })?;
            return Ok((scan, 0));
        };
        let size = JobSize::Bytes(open_job_len(bytes, block_sizes));
        let fetcher = self.clone();
        let job_key = key.to_string();
        let job_bytes = bytes.clone();
        let query = query.clone();
        let columns = columns.clone();
        let indices = indices.map(<[usize]>::to_vec);
        let expected_survivors = expected_survivors.to_vec();
        let accounting = accounting.clone();
        let span = span.clone();
        gate.run(ReadSite::LogPostings, size, move || {
            span.in_scope(|| {
                let (key, bytes) = (job_key.as_str(), &job_bytes);
                let scan = match &indices {
                    None => fetcher.open_scan(key, bytes, &query, &columns, &accounting),
                    Some(indices) if raw => fetcher.open_scan_raw_subset(
                        key,
                        bytes,
                        &query,
                        &columns,
                        indices,
                        &expected_survivors,
                        &accounting,
                    ),
                    Some(indices) => {
                        fetcher.open_scan_subset(key, bytes, &query, &columns, indices, &accounting)
                    }
                }?;
                let block_job_bytes = if block_sizes {
                    max_block_uncompressed_len(bytes, &fetcher.cfg, &accounting)
                } else {
                    0
                };
                Ok((scan, block_job_bytes))
            })
        })
        .await
        .map_err(|err| log_gate_failed(key, err))?
    }

    /// [`open_scan_on_gate`](Self::open_scan_on_gate), specialized to the shape
    /// [`plan_segment`](Self::plan_segment)'s fallback branch needs: no index
    /// restriction, no block-size probe, and the open's decoded
    /// [`SegmentDirectories`] returned alongside the scan so the fallback can
    /// carry them forward for later per-partition opens of this segment to
    /// reuse via [`RlogReader::from_decoded`] instead of decoding them again
    /// (ADR-2414 decision A1).
    async fn open_scan_on_gate_with_directories(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
        span: &tracing::Span,
    ) -> Result<(BlockScan, Arc<SegmentDirectories>), LogFetchError> {
        let Some(gate) = &self.read_gate else {
            return span.in_scope(|| {
                self.open_scan_with_directories(key, bytes, query, columns, accounting)
            });
        };
        let size = JobSize::Bytes(open_job_len(bytes, false));
        let fetcher = self.clone();
        let job_key = key.to_string();
        let job_bytes = bytes.clone();
        let query = query.clone();
        let columns = columns.clone();
        let accounting = accounting.clone();
        let span = span.clone();
        gate.run(ReadSite::LogPostings, size, move || {
            span.in_scope(|| {
                fetcher.open_scan_with_directories(
                    job_key.as_str(),
                    &job_bytes,
                    &query,
                    &columns,
                    &accounting,
                )
            })
        })
        .await
        .map_err(|err| log_gate_failed(key, err))?
    }

    /// [`open_scan_on_gate`](Self::open_scan_on_gate)'s raw-subset case, reusing
    /// an already-decoded [`SegmentDirectories`] via
    /// [`open_scan_raw_subset_with_decoded`](Self::open_scan_raw_subset_with_decoded)
    /// instead of decoding the directories again (ADR-2414 decision A1): the
    /// striped route's per-partition open of a segment the plan phase already
    /// opened once. The block-size job the gate gives each later block decode
    /// is read from the already-decoded `PageDir`
    /// ([`max_block_uncompressed_len_from_page_dir`]), never a third decode.
    #[allow(clippy::too_many_arguments)]
    async fn open_scan_on_gate_from_decoded(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        expected_survivors: &[usize],
        dirs: &Arc<SegmentDirectories>,
        block_sizes: bool,
        span: &tracing::Span,
    ) -> Result<(BlockScan, u64), LogFetchError> {
        let Some(gate) = &self.read_gate else {
            let scan = span.in_scope(|| {
                self.open_scan_raw_subset_with_decoded(
                    key,
                    bytes,
                    query,
                    columns,
                    indices,
                    expected_survivors,
                    dirs,
                )
            })?;
            let block_job_bytes = if block_sizes {
                max_block_uncompressed_len_from_page_dir(dirs.page_dir())
            } else {
                0
            };
            return Ok((scan, block_job_bytes));
        };
        let size = JobSize::Bytes(open_job_len(bytes, block_sizes));
        let fetcher = self.clone();
        let job_key = key.to_string();
        let job_bytes = bytes.clone();
        let query = query.clone();
        let columns = columns.clone();
        let indices = indices.to_vec();
        let expected_survivors = expected_survivors.to_vec();
        let dirs = Arc::clone(dirs);
        let span = span.clone();
        gate.run(ReadSite::LogPostings, size, move || {
            span.in_scope(|| {
                let scan = fetcher.open_scan_raw_subset_with_decoded(
                    job_key.as_str(),
                    &job_bytes,
                    &query,
                    &columns,
                    &indices,
                    &expected_survivors,
                    &dirs,
                )?;
                let block_job_bytes = if block_sizes {
                    max_block_uncompressed_len_from_page_dir(dirs.page_dir())
                } else {
                    0
                };
                Ok((scan, block_job_bytes))
            })
        })
        .await
        .map_err(|err| log_gate_failed(key, err))?
    }

    /// A drainable scan over `bytes`, carrying this fetcher's read gate and
    /// the job size each of its gated block decodes is charged at.
    #[allow(clippy::too_many_arguments)]
    fn scan_handle(
        &self,
        key: &str,
        bytes: LogObjectBytes,
        scan: BlockScan,
        block_job_bytes: u64,
        query: &LogQuery,
        span: tracing::Span,
        accounting: &QueryAccounting,
    ) -> LogSegmentScan {
        LogSegmentScan {
            bytes,
            lost_stats: scan.stats(),
            scan: Some(scan),
            lost_to: None,
            erasure: query.erasure.clone(),
            span,
            key: key.to_string(),
            accounting: accounting.clone(),
            finished: false,
            read_gate: self.read_gate.clone(),
            block_job_bytes,
            #[cfg(test)]
            panic_next_gate_job: false,
        }
    }

    /// This fetcher's own memory budget (bounds `fetch_accounted` and
    /// `whole_object_bytes`), for a test to `Arc::ptr_eq` against another
    /// fetcher's or an engine's, proving they share one budget. See
    /// [`Self::block_range_memory_budget_for_test`] for the block-range path's.
    #[cfg(test)]
    pub(crate) fn memory_budget_for_test(&self) -> &Arc<ravel_memory::MemoryBudget> {
        &self.memory_budget
    }

    /// The block-range path's current memory budget, for the same `Arc::ptr_eq`
    /// purpose as [`Self::memory_budget_for_test`].
    #[cfg(test)]
    pub(crate) fn block_range_memory_budget_for_test(&self) -> &Arc<ravel_memory::MemoryBudget> {
        self.block_range.memory_budget_for_test()
    }

    /// Reserves `n` bytes against this fetcher's budget before a whole-object
    /// GET, mapping a refusal to [`LogFetchError::FetchMemoryExhausted`]. The
    /// guard is owned for the fetched buffer's lifetime (ADR-1170 decision 2).
    fn reserve_fetch(&self, n: u64) -> Result<ravel_memory::Reservation, LogFetchError> {
        self.memory_budget
            .reserve(n)
            .map_err(|e| LogFetchError::FetchMemoryExhausted {
                requested: e.requested,
                reserved: e.reserved,
                limit: e.limit,
            })
    }

    /// Reserves [`SegmentDirectories::decoded_bytes`] of `dirs` against this
    /// fetcher's memory budget, the budget a [`CarriedWholeObject`]'s bytes
    /// are reserved against, for a caller that keeps `dirs` resident across a
    /// query (ADR-2414 decision A1: `ravel_sql::logs_scan` holds each planned
    /// segment's directories for the opens that follow). The guard releases
    /// the bytes when it drops, so the holder keeps it for as long as it keeps
    /// `dirs`. A refusal is [`LogFetchError::FetchMemoryExhausted`], the error
    /// a refused whole-object reservation reports.
    ///
    /// Taken after the decode, not before as a whole-object GET's is, because
    /// the directories' size is known only once they are decoded.
    pub fn reserve_carried_directories(
        &self,
        dirs: &SegmentDirectories,
    ) -> Result<ravel_memory::Reservation, LogFetchError> {
        self.reserve_fetch(dirs.decoded_bytes())
    }

    /// This fetcher's own limiter (bounds `fetch_accounted` and
    /// `whole_object_bytes`), for a test to `Arc::ptr_eq` against another
    /// fetcher's or an engine's, proving two fetchers actually share one
    /// `GetLimiter` rather than each holding an equal-but-distinct one. See
    /// [`Self::block_range_get_limiter_for_test`] for the block-range path's
    /// limiter.
    #[cfg(test)]
    pub(crate) fn get_limiter_for_test(&self) -> &std::sync::Arc<crate::GetLimiter> {
        &self.get_limiter
    }

    /// The block-range path's current limiter, for the same `Arc::ptr_eq`
    /// purpose as [`Self::get_limiter_for_test`]. Kept separate because
    /// `with_block_range` can, in principle, be handed a `BlockRangeFetcher`
    /// wired to a different limiter than this instance's own before this
    /// builder re-applies the shared one; a test asserting both proves that
    /// re-apply actually happened.
    #[cfg(test)]
    pub(crate) fn block_range_get_limiter_for_test(&self) -> &std::sync::Arc<crate::GetLimiter> {
        self.block_range.get_limiter_for_test()
    }

    /// This fetcher's `GetLimiter` permit count (ADR-1195): the bound both
    /// its own whole-object funnel and the block-range path draw from, and,
    /// when wired via [`Self::with_get_limiter`], the process-wide GET
    /// concurrency bound shared with every other fetcher and engine holding
    /// the same `Arc`. Mirrors
    /// [`SegmentFetcher::get_limiter_permits`](crate::fetcher::SegmentFetcher::get_limiter_permits).
    #[must_use]
    pub fn get_limiter_permits(&self) -> usize {
        self.get_limiter.permits()
    }

    /// Pins the block-range fetcher's suffix-probe length
    /// ([`BlockRangeFetcher::with_suffix_len`]), overriding the per-object
    /// derivation ([`derive_suffix_len`], #883). This is the seam a measurement
    /// pass sweeps the probe length through: the probe floor can only be
    /// tightened against measured [`BlockRangeStats::probe_misses`], and a sweep
    /// needs to set the window from outside the fetcher.
    #[must_use]
    pub fn with_suffix_len(mut self, n: u64) -> Self {
        self.block_range = self.block_range.with_suffix_len(n);
        self
    }

    /// Sets the block-range fetcher's request cost
    /// ([`BlockRangeFetcher::with_request_cost_bytes`]), the byte-denominated
    /// cost of one store round trip that drives its whole-object crossover and
    /// coalescing gap (ADR-0107). A property of the store and instance, not the
    /// RLOG format.
    #[must_use]
    pub fn with_request_cost_bytes(mut self, n: u64) -> Self {
        self.block_range = self.block_range.with_request_cost_bytes(n);
        self
    }

    /// Sets the fetch bound ([`BlockRangeFetcher::with_max_fetch_run_bytes`],
    /// ADR-0996 decision 2): one covering GET's maximum length. An object above
    /// the bound is read as `ceil(object_size / n)` sequential covering
    /// sub-range GETs on every read shape, whole-object funnel included. Zero is
    /// refused with the same typed error
    /// [`EngineConfig::validate`](crate::EngineConfig::validate) returns.
    pub fn with_max_fetch_run_bytes(mut self, n: u64) -> Result<Self, EngineConfigError> {
        self.block_range = self.block_range.with_max_fetch_run_bytes(n)?;
        Ok(self)
    }

    /// Replaces the block-range fetcher (ADR-0107) with a fully configured one,
    /// for callers and tests that need to set the coalescing gap, coverage
    /// crossover, or concurrency bound directly. The replacement keeps this
    /// instance's `block_range_threshold`; pair it with
    /// [`with_block_range_threshold`](Self::with_block_range_threshold) to route
    /// small fixtures through it.
    ///
    /// The replacement is re-attached to this instance's per-phase WIRE byte
    /// counter (#913), so a caller that took the handle from
    /// [`phase_wire_byte_counter`](Self::phase_wire_byte_counter) before or
    /// after this call reads the same totals either way.
    ///
    /// It is also re-wired to this instance's current `get_limiter` (ADR-1195),
    /// overriding whatever limiter `block_range` was built with. This makes
    /// builder order irrelevant: `with_get_limiter().with_block_range(...)`
    /// and `with_block_range(...).with_get_limiter(...)` both end with the
    /// block-range path sharing this fetcher's limiter, rather than the first
    /// order silently dropping it in favor of the replacement's own.
    #[must_use]
    pub fn with_block_range(mut self, block_range: BlockRangeFetcher) -> Self {
        let block_range = block_range
            .with_wire_byte_counter(self.wire_bytes.clone())
            .with_get_limiter(Arc::clone(&self.get_limiter))
            .with_memory_budget(Arc::clone(&self.memory_budget));
        self.block_range = match &self.read_gate {
            Some(gate) => block_range.with_read_gate(Arc::clone(gate)),
            None => block_range,
        };
        self
    }

    /// The block-range fetcher this instance routes large-object reads through
    /// (ADR-0107). Exposed so tests can drive the block-range protocol directly.
    #[must_use]
    pub fn block_range_fetcher(&self) -> &BlockRangeFetcher {
        &self.block_range
    }

    /// Whether ADR-0046's read cache is wired ([`with_cache`](Self::with_cache)
    /// was called). A caller that would issue several GETs at the same key needs
    /// this: with a cache those coalesce onto one fetch through single-flight,
    /// without one each is a real object-store request. ADR-0102 decision 1 names
    /// the cache as the precondition for that fan-out, and `LogsScanExec::new`
    /// gates its partition count on this.
    ///
    /// This holds on both fetch shapes, which is what keeps that gate honest
    /// (ADR-0107): at or below
    /// [`with_block_range_threshold`](Self::with_block_range_threshold) the
    /// coalesced request is the one whole-object GET keyed `(0, object_size)`;
    /// above it, the block-range path's probe, directory sections, and per-block
    /// ranges each coalesce on their own extent key, so a segment striped across
    /// N partitions still costs one request per distinct extent rather than N.
    #[must_use]
    pub fn has_cache(&self) -> bool {
        self.cache.is_some()
    }

    /// The object-size threshold above which a tenant-aware fetch reads only
    /// pruning-relevant blocks (ADR-0107,
    /// [`with_block_range_threshold`](Self::with_block_range_threshold)). Exposed
    /// so `ravel_sql::logs_scan`'s predicate-free full-window fast path (#693
    /// part 3) can decide, with no I/O, whether a segment is in the band where
    /// skipping the plan phase and reading the whole object in one GET actually
    /// saves a probe: at or below this size the whole-object read is already the
    /// only GET, so the fast path adds nothing.
    #[must_use]
    pub fn block_range_threshold(&self) -> u64 {
        self.block_range_threshold
    }

    /// Whether the probe-and-range path is worth its extra round trips on an
    /// object of `object_size` bytes whose projection is expected to read
    /// `projected_fraction` of them (`0.0` = one column out of many, `1.0` =
    /// every column). Answered from the catalog summary and the resolved
    /// projection alone, with no I/O, so a scan can route a segment before it
    /// opens it (issue #862).
    ///
    /// The arbiter is the request-cost model this module already runs on
    /// ([`DEFAULT_LOG_REQUEST_COST_BYTES`]), not a second threshold. A saved
    /// request is worth `request_cost_bytes` saved bytes; the ranged protocol
    /// adds [`WHOLE_OBJECT_REQUEST_MULTIPLE`] round trips over one whole-object
    /// GET; so it pays exactly when it saves more than
    /// `WHOLE_OBJECT_REQUEST_MULTIPLE * request_cost_bytes` bytes. That product
    /// is `BlockRangeFetcher::effective_whole_object_threshold`, which the fetch
    /// layer already compares the WHOLE object size against -- the same question
    /// at `projected_fraction == 0.0`. A narrower projection changes what the
    /// ranged path SAVES, never what it costs, so the same threshold generalizes
    /// by moving the projection into the saving.
    ///
    /// At or below [`Self::block_range_threshold`] the answer is always false:
    /// there [`tenant_bytes`](Self::tenant_bytes) reads the whole object
    /// whichever entry point opens it, so routing buys nothing and would only
    /// add a probe.
    ///
    /// `projected_fraction` is an estimate, deliberately: the exact projected
    /// byte volume is not known until PAGE_DIR is read. The byte-exact decision
    /// still happens one layer down, in the coverage crossover
    /// ([`DEFAULT_LOG_COVERAGE_THRESHOLD`]), which falls back to a single
    /// whole-object GET when the projected pages turn out to cover the object
    /// after all. This call decides only whether it is worth reading the
    /// directory to ask. A non-finite fraction fails closed to the whole-object
    /// read.
    #[must_use]
    pub fn ranged_projection_pays(&self, object_size: u64, projected_fraction: f64) -> bool {
        if object_size <= self.block_range_threshold || !projected_fraction.is_finite() {
            return false;
        }
        let fraction = projected_fraction.clamp(0.0, 1.0);
        let saved = object_size as f64 * (1.0 - fraction);
        saved > self.block_range.effective_whole_object_threshold() as f64
    }

    /// Per-object relevance from the catalog summary alone, with no object
    /// read: true iff the segment's event-ts span (`SegmentRef`'s
    /// `min_event_ts_ns..=max_event_ts_ns`, the same bounds the footer carries)
    /// overlaps the inclusive query range. A `false` return lets [`fetch`] skip
    /// the object without a GET, which is the point of pruning by time before
    /// touching object storage.
    ///
    /// [`fetch`]: Self::fetch
    #[must_use]
    pub fn ts_range_relevant(seg_ref: &SegmentRef, ts_min_ns: i64, ts_max_ns: i64) -> bool {
        seg_ref.min_event_ts_ns <= ts_max_ns && ts_min_ns <= seg_ref.max_event_ts_ns
    }

    /// Resolves stream-attribute equalities against an already-fetched object's
    /// STREAM_DIR, returning the ids of streams whose canonical resource+scope
    /// blob matches every filter (ANDed). An empty `filters` returns every
    /// stream in the object.
    ///
    /// # This match is approximate: it over-approximates
    ///
    /// The returned set is a pruning hint, not an exact evaluation of the
    /// caller's equality predicate. It can contain streams that do not carry
    /// the queried `(key, value)` as a genuine top-level resource or scope
    /// attribute. It is not an exact filter and must not be presented as one.
    ///
    /// Matching is raw-byte containment: each filter's `(key, value)` is
    /// encoded with the frozen [`canonical_attr_bytes`] grammar and searched
    /// for as a contiguous sub-sequence anywhere in the stored blob. Nested
    /// `AttrValue::Map` and `AttrValue::List` values are written with the same
    /// byte grammar as top-level entries, because `encode_attrs` and
    /// `encode_value` in `ravel_types::logstream` recurse into each other. A
    /// `(key, value)` pair nested inside a map or list value is therefore
    /// byte-identical to the same pair appearing as a top-level entry, and this
    /// search cannot tell the two apart. Concretely: a stream whose only
    /// resource attribute is `k8s.labels = Map([("service.name", "api")])`
    /// matches the filter `service.name = "api"`, even though it carries no
    /// top-level `service.name` attribute at all and is a different
    /// [`LogStreamId`] from the stream that does. Both stream ids come back.
    ///
    /// There are no false negatives in the other direction: a stream that
    /// really does carry the attribute is never missed. The writer emits each
    /// attribute entry's `len(key) key encode_value(value)` bytes contiguously
    /// and canonicalizes only the entry *order*, so whenever the attribute is
    /// present the needle occurs verbatim in the blob.
    ///
    /// # What a caller must do about it
    ///
    /// Treat the returned set as a pruning hint and re-apply the attribute
    /// equality yourself, at the record level, on whatever comes back. Nothing
    /// downstream does this for you, and nothing will report that it was
    /// skipped. [`Predicate::StreamIn`] *is* evaluated exactly by
    /// [`RlogReader::scan`] -- but exactly against whichever stream set it was
    /// handed, so an over-broad set from this method silently produces
    /// over-broad final results. The other predicate kinds (ts range,
    /// `HasWord`, `Equals`) are exact and unaffected.
    ///
    /// Making this exact requires walking the blob entry by entry so that
    /// nesting depth is known, which requires either a public STREAM_DIR blob
    /// decoder in `ravel-logseg` or an entry-walking decoder here. This path
    /// deliberately adds neither. The real logs query path owns that decision
    /// and must do one of two things: make the match
    /// exact, or re-apply the equality on returned records and state the
    /// limitation in its user-facing query semantics. Silently inheriting this
    /// over-approximation into a user-facing query would violate the "exact
    /// semantics by default, approximation is opt-in and visible" invariant.
    ///
    /// `accounting` receives the STREAM_DIR decompression this decode performs
    /// (issue #1401 finding 3); `bytes` is assumed already fetched and its wire
    /// bytes already charged by the caller.
    pub fn matching_streams<S: ByteSource + ?Sized>(
        &self,
        bytes: &S,
        filters: &[StreamAttrEquals],
        accounting: &QueryAccounting,
    ) -> Result<Vec<LogStreamId>, LogSegError> {
        let dir = self.decode_stream_dir(bytes, accounting)?;
        Ok(matching_streams_in(&dir, filters))
    }

    /// Fetches, prunes, and scans one segment for records matching `query`.
    ///
    /// The ts-range relevance pre-check runs first, from the catalog summary
    /// only: an object whose span cannot satisfy the range returns `Ok(None)`
    /// with no GET. Otherwise the whole object is fetched once
    /// ([`GetRange::Full`]), the STREAM_DIR is consulted to resolve any
    /// stream-attribute equalities into a [`Predicate::StreamIn`], and the
    /// combined predicate (ts range AND resolved streams AND content) is handed
    /// to [`RlogReader::scan_pruned`] together with `query.prune`, whose
    /// skip-index, POSTINGS, and bloom pruning do the block-level work.
    ///
    /// # The returned records are exact except for `stream_attrs`
    ///
    /// The ts range and the content predicates hold exactly on every returned
    /// record. `query.prune` does not hold on every returned record and is not
    /// meant to: it only drops blocks proven to hold no match, so the record set
    /// is the same one an empty `prune` would return (see [`LogQuery::prune`]).
    /// `query.stream_attrs` does not hold either: it is resolved by
    /// [`matching_streams`], which over-approximates, so the returned records
    /// can include records from a stream that does not carry the requested
    /// attribute as a genuine top-level resource or scope attribute (a nested
    /// map or list value with the same bytes is enough to match). No false
    /// negatives: every record that does match is returned.
    ///
    /// A caller that needs exact stream-attribute semantics must re-apply the
    /// equality on the returned records itself. That path must either do that
    /// or document the limitation in its user-facing query semantics; it cannot
    /// assume this method filtered exactly.
    ///
    /// [`matching_streams`]: Self::matching_streams
    pub async fn fetch(
        &self,
        seg_ref: &SegmentRef,
        query: &LogQuery,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        self.fetch_accounted(seg_ref, query, &QueryAccounting::new())
            .await
    }

    /// Accounted counterpart of [`fetch`](Self::fetch): identical behavior,
    /// plus the object GET is recorded against `accounting` (ADR-0044 "2.
    /// Accounting is recorded at existing funnels only" -- this call is the
    /// funnel `LogSegmentFetcher` did not have before). `engine.rs` builds
    /// its own `LogSegmentFetcher` (`QueryEngine::log_fetcher`) and drives
    /// it through the tenant-aware
    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant) (via
    /// `log_series::fetch_log_series`), not this entry point; ravel-sql's
    /// `logs_scan` reaches the same funnel through
    /// `scan_accounted_with_tenant`/`scan_accounted_with_tenant_subset`,
    /// while `audit_scan` and `alerts_scan` call
    /// [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant).
    /// This untenanted `fetch`/`fetch_accounted` pair has no production
    /// caller left; only tests exercise it.
    pub async fn fetch_accounted(
        &self,
        seg_ref: &SegmentRef,
        query: &LogQuery,
        caller_accounting: &QueryAccounting,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        // Issue #796: this funnel has no separate plan step of its own (one
        // unconditional whole-object GET, then decode), so its GET is charged
        // to the `scan` phase. See `crate::phase_accounting`'s module docs for
        // the phase taxonomy and `scan_accounted_with_tenant` below for the
        // tenant-aware funnel this untenanted entry point mirrors.
        let phase = PhaseAccounting::new();
        let accounting = phase.scan();
        let result = async {
            if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
                return Ok(None);
            }
            let key = &seg_ref.data_object_key;
            // Two separable phases here (ADR-0044 decision 5): the
            // whole-object GET, then the STREAM_DIR resolve + `RlogReader` scan in
            // `scan_bytes`. They are named `page_fetch` and `decode` to match the
            // metric path's phase names. This entry point is reached by the
            // unaccounted `fetch` (and by tests). Production callers in
            // `ravel-sql` do not come here: the logs scan uses
            // `scan_accounted_with_tenant` and `_subset`, and the alerts and
            // audit scans use `fetch_accounted_with_tenant`; each carries its
            // own copy of these spans over its own (cache-aware) GET path.
            // Reserve the whole object's bytes before the GET (ADR-1170
            // decision 2): a refusal fails this fetch typed with zero GETs
            // issued. Attached to the fetched buffer below rather than held as
            // a local, because a gated block decode clones those bytes into a
            // job that outlives a dropped caller.
            let reservation = self.reserve_fetch(seg_ref.object_size)?;
            let fetch_span = tracing::debug_span!(
                "page_fetch",
                signal = "logs",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
            );
            let got = async {
                // Held across the GET only: dropped when this inner block
                // returns, before `decode_spanned` below (ADR-1195).
                let _permit =
                    self.get_limiter
                        .acquire()
                        .await
                        .map_err(|_| LogFetchError::Store {
                            key: key.to_string(),
                            source: StoreError::Transient(
                                "GetLimiter semaphore closed unexpectedly".to_string(),
                            ),
                        })?;
                self.store
                    .get(key, GetRange::Full)
                    .await
                    .map_err(|source| LogFetchError::Store {
                        key: key.to_string(),
                        source,
                    })
            }
            .instrument(fetch_span.clone())
            .await?;
            let bytes = attach_reservation(got.data, reservation);
            accounting.record_s3_request(AccountedOp::Get);
            accounting.add_s3_bytes(AccountedOp::Get, bytes.len() as u64);
            // #913: a data read, so the whole object's wire bytes are the
            // scan phase's (`ReadPhases::SCAN.blocks`).
            self.wire_bytes
                .record(ReadPhases::SCAN.blocks, bytes.len() as u64);
            // This funnel issues exactly one whole-object GET per call.
            fetch_span.record("s3_requests", 1u64);
            fetch_span.record("s3_bytes", bytes.len() as u64);
            self.decode_spanned(key, &bytes.into(), query, accounting)
                .await
        }
        .await;
        caller_accounting.merge_snapshot(&phase.snapshot().pooled());
        result
    }

    /// Cache-aware counterpart of [`fetch_accounted`](Self::fetch_accounted):
    /// identical scan behavior, but the object's bytes are served through
    /// ADR-0046's read cache (via [`with_cache`](Self::with_cache)) rather
    /// than an unconditional store GET. This is RLOG's sole read funnel
    /// (`RlogFetcher::fetch` in ADR-0046 decision 1), and its only GET is
    /// always [`GetRange::Full`], so the whole object keys as `(0,
    /// seg_ref.object_size)` -- the same convention
    /// `SegmentFetcher::guarded_get` uses for a whole-object GET.
    ///
    /// `tenant_hash` is an explicit parameter, not a field on
    /// `LogSegmentFetcher`, because the one production instance
    /// (`services/ravel-server/src/query.rs`) is shared across every tenant;
    /// a per-instance tenant would make that instance usable by exactly one
    /// tenant. Production callers in `ravel-sql` all reach a tenant-aware
    /// funnel: the logs scan through
    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant) and
    /// its `_subset` form, the alerts and audit scans through this method.
    /// The untenanted [`fetch_accounted`](Self::fetch_accounted) remains for
    /// tests and for the unaccounted `fetch`.
    ///
    /// With no cache configured (`with_cache` never called), this fetches
    /// exactly like [`fetch_accounted`](Self::fetch_accounted): every GET
    /// goes to the store, and no cache accounting is recorded.
    pub async fn fetch_accounted_with_tenant(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        caller_accounting: &QueryAccounting,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        // Issue #796: `tenant_bytes` (below the block-range threshold, a
        // whole-object GET) and the block-range path it falls through to
        // above threshold both do data reads with no separate plan step of
        // their own here, so this funnel's GETs are charged to `scan`.
        // `decode_spanned` (below) charges only the decode's decompressed
        // bytes (issue #1401) -- unlike the `LogSegmentScan`-returning funnels
        // below, this one fully decodes and returns before this function
        // returns, so buffering into a disposable `PhaseAccounting` and
        // merging once is safe here.
        let phase = PhaseAccounting::new();
        let accounting = phase.scan();
        // This funnel's decode (`scan_bytes`) reads every column, so the fetch
        // selection is the same: on a version-4 object it brings every column
        // chunk of every surviving block (ADR-0699 decision 5).
        let all = ColumnSelection::all();
        let result = async {
            let Some((bytes, _blocks_read)) = self
                .tenant_bytes(
                    seg_ref,
                    tenant_hash,
                    query,
                    &all,
                    ProbePhase::Scan,
                    accounting,
                )
                .await?
            else {
                return Ok(None);
            };
            self.decode_spanned(&seg_ref.data_object_key, &bytes, query, accounting)
                .await
        }
        .await;
        caller_accounting.merge_snapshot(&phase.snapshot().pooled());
        result
    }

    /// Streaming counterpart of
    /// [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant)
    /// (ADR-0087 decisions 2 and 3): the same GET, the same cache, the same
    /// pruning, but the object's blocks are returned as a [`LogSegmentScan`]
    /// the caller drains one block at a time instead of one
    /// already-fully-decoded `Vec<LogRecord>`.
    ///
    /// `columns` narrows what each block decodes. `ColumnSelection::all()`
    /// makes this exactly `fetch_accounted_with_tenant` in slow motion; a
    /// narrower selection leaves unselected columns undecoded, so the yielded
    /// records carry a partial view and the caller must have included every
    /// column it (or anything downstream of it, including its selective-erasure
    /// exclusion) reads.
    ///
    /// This bounds *decoded* memory, not raw bytes: the object's bytes are
    /// resident before the first block is decoded either way. Which read shape
    /// produced them depends on the object's size
    /// ([`with_block_range_threshold`](Self::with_block_range_threshold),
    /// ADR-0107). At or below the threshold it is one [`GetRange::Full`] GET of
    /// the whole object. Above it the bytes are only the directory sections and
    /// the pruning-relevant pages, fetched by [`BlockRangeFetcher`] as a probe
    /// plus coalesced ranges and held as those regions (issue #2066), so both
    /// the raw bytes moved and the raw bytes held are proportional to pruning
    /// rather than to object size, unless a crossover reads the whole object.
    ///
    /// # Issue #796 phase attribution
    ///
    /// Every GET this funnel issues (directly, or through
    /// [`BlockRangeFetcher`]'s probe-then-range protocol above the
    /// block-range threshold) is `scan` phase by this file's mapping: unlike
    /// [`plan_segment`](Self::plan_segment), nothing here is a standalone
    /// planning read. `accounting` is taken and stored as-is (not buffered
    /// through a disposable [`PhaseAccounting`](crate::phase_accounting::PhaseAccounting)
    /// like [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant)):
    /// the returned [`LogSegmentScan`] keeps writing to it after this call
    /// returns (`LogSegmentScan::finish`'s deferred `page_bytes_fetched`/
    /// `page_bytes_decoded`, ADR-0107 decision 4), so a merge-once buffer
    /// would silently drop those writes. A caller wanting a live `scan`-phase
    /// split for this funnel would need to pass a `PhaseAccounting::scan()`
    /// handle directly; no in-scope caller does, which is a #796 report
    /// finding, not a bug fixed here.
    pub async fn scan_accounted_with_tenant(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        let Some((bytes, _blocks_read)) = self
            .tenant_bytes(
                seg_ref,
                tenant_hash,
                query,
                columns,
                ProbePhase::Scan,
                accounting,
            )
            .await?
        else {
            return Ok(None);
        };
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let (scan, block_job_bytes) = self
            .open_scan_on_gate(
                key,
                &bytes,
                query,
                columns,
                None,
                false,
                &[],
                true,
                accounting,
                &span,
            )
            .await?;
        Ok(Some(self.scan_handle(
            key,
            bytes,
            scan,
            block_job_bytes,
            query,
            span,
            accounting,
        )))
    }

    /// Whole-object streaming scan for the predicate-free full-window
    /// whole-segment path (#693 part 3): reads the ENTIRE object in one
    /// [`GetRange::Full`] GET, bypassing the block-range probe-and-range protocol
    /// entirely, then opens the pruned, column-projected scan over all of its
    /// blocks.
    ///
    /// This is the counterpart of
    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant) for a
    /// segment that `ravel_sql::logs_scan` has proved (with zero I/O, from the
    /// resolved snapshot) is fully contained in a predicate-free query window: it
    /// is going to read every block, so a whole-object GET is strictly optimal --
    /// no suffix probe, no per-block ranges, no coverage computation. Above the
    /// block-range threshold `scan_accounted_with_tenant` would instead issue a
    /// probe and then (all blocks being candidates) a coverage-crossover
    /// whole-object GET, i.e. two GETs where this issues one. The whole object is
    /// keyed `(0, object_size)`, the same key
    /// [`tenant_bytes`](Self::tenant_bytes)'s whole-object read and
    /// [`BlockRangeFetcher::fetch_object`]'s crossovers use, so it composes with
    /// them under the cache's single-flight rather than adding a distinct extent.
    ///
    /// A single GET observes one object state, so no [`EtagPin`] is needed: the
    /// multi-GET consistency the block-range path must defend does not arise here.
    ///
    /// Issue #796: `scan` phase, same reasoning and the same not-buffered
    /// `accounting` handling as
    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant)'s doc
    /// comment.
    pub async fn scan_whole_accounted_with_tenant(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }
        let bytes = self
            .whole_object_bytes(seg_ref, tenant_hash, ReadPhases::SCAN.blocks, accounting)
            .await?;
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let (scan, block_job_bytes) = self
            .open_scan_on_gate(
                key,
                &bytes,
                query,
                columns,
                None,
                false,
                &[],
                true,
                accounting,
                &span,
            )
            .await?;
        Ok(Some(self.scan_handle(
            key,
            bytes,
            scan,
            block_job_bytes,
            query,
            span,
            accounting,
        )))
    }

    /// Prune one segment for intra-segment scan partitioning (ADR-0102) WITHOUT
    /// decoding any block: returns how many of its blocks survive this query's
    /// pruning, plus the whole-segment [`ScanStats`] the prune produced, or
    /// `Ok(None)` when the catalog summary proved the segment irrelevant (no
    /// GET, exactly like [`scan_accounted_with_tenant`](Self::
    /// scan_accounted_with_tenant)).
    ///
    /// The survivor count is column-independent -- pruning never consults the
    /// [`ColumnSelection`], which only narrows which pages a decoded block
    /// reads -- so this opens the scan with [`ColumnSelection::all`] and decodes
    /// nothing (`blocks_scanned`/`pages_*` in the returned stats are therefore
    /// zero; the totals describe the whole segment). The byte read goes through
    /// the same cache-aware funnel [`scan_accounted_with_tenant`](Self::
    /// scan_accounted_with_tenant) uses and admits the same per-extent cache
    /// entries.
    ///
    /// It does NOT single-flight with the per-partition subset scans that
    /// follow, though: `ravel_sql::logs_scan` awaits the whole plan pass behind a
    /// `OnceCell` barrier before any partition drains, so the plan read is the
    /// FIRST, cold touch of each extent and completes before the subset scans
    /// even start -- there is no concurrent in-flight GET for them to collapse
    /// onto. A subset scan then either reuses a cache entry the plan admitted or,
    /// if it was already evicted, issues its own GET (issue #691 measured the
    /// probe landing in S3-FIFO probation at freq 0 and being evicted before the
    /// scan). The way to avoid the extra read is not to coalesce it but to
    /// eliminate it: #693 part 3 carries this footer forward
    /// ([`fetch_object_with_footer`](Self::fetch_object_with_footer)) so a subset
    /// scan skips its own probe, and the predicate-free full-window whole-segment
    /// path skips the plan pass entirely.
    ///
    /// [`scan_accounted_with_tenant`]: Self::scan_accounted_with_tenant
    ///
    /// Issue #796: every GET this method issues -- the fast path's footer
    /// probe and directory reads, the skip-decidable path's
    /// `fetch_plan_directories`, and the
    /// fallback's whole-object read (a planning read here, not a scan, per
    /// its own comment below) -- is `plan` phase. Buffered through a
    /// disposable [`PhaseAccounting`](crate::phase_accounting::PhaseAccounting)
    /// and merged once before returning: safe here because, unlike the
    /// `LogSegmentScan`-returning funnels, nothing this method returns keeps
    /// writing to the accounting handle after it returns (the fallback's
    /// `scan.remaining_blocks()`/`scan.stats()` are read synchronously, and
    /// the `BlockScan` itself carries no accounting handle).
    pub async fn plan_segment(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        caller_accounting: &QueryAccounting,
    ) -> Result<
        Option<(
            Vec<usize>,
            Arc<SegmentDirectories>,
            ScanStats,
            Option<footer::LogFooter>,
            Option<CarriedWholeObject>,
        )>,
        LogFetchError,
    > {
        let phase = PhaseAccounting::new();
        let accounting = phase.plan();
        let result = async {
            // Fast path (#693): a query with no block-level predicate whose ts
            // window fully CONTAINS the segment's span prunes nothing -- every block
            // survives -- so the survivor count is the footer's `block_count` and no
            // block-range fetch or decode is needed. The ADR-0107 suffix probe runs
            // to read the footer, and `fetch_plan_directories` reads and decodes the
            // four directories it did not cover; no BLOCKS byte is read. Containment
            // is strictly stronger than
            // `ts_range_relevant`'s overlap (a partially-overlapping window still
            // needs real ts pruning), and it implies relevance, so the fast path
            // never has to return the irrelevant-`None`. A zero object size cannot
            // be range-probed (every extent, starting with the probe's cache key,
            // is derived from it), so it falls through to the whole-object slow
            // path, as does an inverted span (`min > max`, the shape a zero-record
            // payload would produce, structurally unreachable today but not worth
            // trusting blindly): the slow path's `ts_range_relevant` would return
            // `None` for a genuinely empty segment, and the fast path should agree
            // rather than returning `Some((0, ..))` for an input that should never
            // reach a segment at all. `object_size > block_range_threshold` keeps
            // the fast path strictly to the band where it actually saves a GET: at
            // or below the threshold `tenant_bytes` already takes a single
            // whole-object read (`fetch_footer` has no matching whole-object
            // crossover of its own, so below the threshold it would read the same
            // object twice under two different cache keys instead of once).
            if seg_ref.object_size > self.block_range_threshold
                && seg_ref.min_event_ts_ns <= seg_ref.max_event_ts_ns
                && query.is_block_predicate_free()
                && query.ts_min_ns <= seg_ref.min_event_ts_ns
                && seg_ref.max_event_ts_ns <= query.ts_max_ns
            {
                let (indices, dirs, stats, footer) = self
                    .plan_segment_fast(seg_ref, tenant_hash, accounting)
                    .await?;
                // Footer-carrying branch: no block byte is read here, so the
                // touch is the scan that follows a nonzero survivor count.
                // No whole-object bytes to carry either: this branch reads
                // the footer and the four directories, never a block.
                let touched = !indices.is_empty();
                return Ok(Some((indices, dirs, stats, Some(footer), touched, None)));
            }

            // Skip-index-only survivor count (#761): when every block-level predicate
            // the query carries is decidable from the skip index alone -- ts bounds
            // and prune-only NumRange arms, no text/content arm, no attribute-equality
            // POSTINGS prune, no stream filter -- the surviving-block count is exactly
            // `candidate_blocks` over those arms, with no BLOCKS byte fetched and no
            // block decoded. The reader's full prune (skip, then POSTINGS, then bloom)
            // reduces to the skip step for such a query, so this count equals the
            // survivor list a subset open will stripe over, and the footer read here
            // carries forward so those opens skip their own probe (#693 part 3).
            //
            // The relevance check is the one the fallback's `tenant_bytes` runs: it
            // is what turns an out-of-window segment into the `None` the caller
            // drops, and unlike the fast path's containment test this branch's
            // guard does not imply it.
            if seg_ref.object_size > self.block_range_threshold
                && seg_ref.min_event_ts_ns <= seg_ref.max_event_ts_ns
                && Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns)
                && Self::plan_skip_decidable(query)
            {
                // The `page_fetch` phase span the other plan paths carry (#782), so
                // the trace shows the probe and section GETs this branch issues.
                // `s3_bytes` reports block bytes only, which are structurally zero
                // here; the directory-overhead bytes are recorded in `accounting`.
                let fetch_span = tracing::debug_span!(
                    "page_fetch",
                    signal = "logs",
                    s3_requests = tracing::field::Empty,
                    s3_bytes = tracing::field::Empty,
                    probe_misses = tracing::field::Empty,
                );
                // ADR-2414 decision A1: decode all four directories here, not
                // just SKIP_IDX + FIELD_DIR -- this branch's segment is still
                // opened per-partition by the striped route when
                // `segment_count < target_partitions`, and that open must
                // reuse the decode rather than repeat it.
                let (footer, dirs, stats) = async {
                    self.block_range
                        .fetch_plan_directories(seg_ref, tenant_hash, accounting)
                        .await
                }
                .instrument(fetch_span.clone())
                .await?;
                let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
                fetch_span.record("s3_requests", requests);
                fetch_span.record("s3_bytes", stats.block_bytes_fetched);
                // Probe misses (#883): tail sections the derived probe window did
                // not cover, reported alongside the request and byte counts for
                // this plan phase. A nonzero value here is the extra request a
                // too-small derivation costs.
                self.record_probe_misses(&fetch_span, &stats, ProbePhase::Plan);
                accounting.add_decompressed_bytes(dirs.open_decompressed_bytes());

                let refs: Vec<&Predicate> = query.prune.iter().collect();
                let numeric = dirs.field_dir().numeric_range_arms(&refs);
                let indices = dirs.skip_index().candidate_blocks(
                    query.ts_min_ns,
                    query.ts_max_ns,
                    None,
                    &numeric,
                );
                let survivors = indices.len();
                let blocks = dirs.skip_index().l0.len() as u32;
                let plan_stats = ScanStats {
                    blocks_total: blocks,
                    blocks_after_skip: survivors as u32,
                    blocks_after_postings: survivors as u32,
                    blocks_after_bloom: survivors as u32,
                    blocks_scanned: 0,
                    pages_decoded: 0,
                    pages_skipped: 0,
                    page_bytes_fetched: 0,
                    page_bytes_decoded: 0,
                    decompressed_bytes: 0,
                    bloom_degraded: false,
                    postings_degraded: false,
                };
                // Footer-carrying branch: only the probe and the four
                // directories were read, no BLOCKS byte, so the touch is the
                // scan a nonzero survivor count will drive. No whole-object
                // bytes either, for the same reason as the fast path above.
                return Ok(Some((
                    indices,
                    dirs,
                    plan_stats,
                    Some(footer),
                    survivors > 0,
                    None,
                )));
            }

            // Fallback: a predicate the skip index cannot decide (a `has_word`/text
            // content arm with only a per-block bloom, an attribute-equality POSTINGS
            // prune, a stream filter), or a below-threshold object. Read as before --
            // the survivor count then needs the reader's full prune over the fetched
            // buffer -- and hand no footer forward. This whole-object plan read is the
            // amplification #761 could not remove for these shapes; the caller counts
            // it (a `None` footer on a relevant segment) as a `plan_full_reads` so a
            // report can see which queries still pay it. Issue #835: when it resolved
            // the WHOLE object (`blocks_read` is `None`), these bytes are carried
            // forward as a [`CarriedWholeObject`] so the scan that follows does not
            // pay a second wire GET for them -- the amplification #761 could not
            // remove from this plan read no longer forces a second one at scan time.
            let all = ColumnSelection::all();
            let Some((bytes, blocks_read)) = self
                .tenant_bytes(
                    seg_ref,
                    tenant_hash,
                    query,
                    &all,
                    ProbePhase::Plan,
                    accounting,
                )
                .await?
            else {
                return Ok(None);
            };
            let key = &seg_ref.data_object_key;
            let span = decode_span();
            // ADR-2414 decision A1: capture the directories this open decoded
            // (over the fetched buffer) so a later per-partition open of this
            // segment reuses them via `RlogReader::from_decoded` instead of
            // decoding a second time.
            let (scan, dirs) = self
                .open_scan_on_gate_with_directories(key, &bytes, query, &all, accounting, &span)
                .await?;
            // Opening the scan decoded the four directory sections over the
            // fetched buffer (and ran the POSTINGS probe for an eligible prune
            // arm); those bytes sit in the reader's own stats, not on any
            // handle, so charge them to the plan phase here (issue #1401). No
            // block is decoded on this branch: the scan exists for its
            // survivor count.
            accounting.add_decompressed_bytes(scan.stats().decompressed_bytes);
            // This fallback read fetched blocks iff `tenant_bytes` resolved at
            // least one (ranged path) or read the whole object (`None`, every
            // block present). A ranged read that pruned every block resolved zero
            // extents and moved no block byte, so it is NOT a touch even though it
            // read the directory sections; a read that decoded one or more blocks
            // IS a touch even if row filtering then leaves zero survivors.
            let touched = match blocks_read {
                None => true,
                Some(n) => n > 0,
            };
            // `blocks_read` is `None` exactly when the whole object is present in
            // `bytes` (the below-threshold path, or an above-threshold read that
            // crossed over to a whole-object GET): safe to carry forward whatever
            // the scan's own column selection turns out to be. `Some(_)` is a
            // ranged read fetched under `ColumnSelection::all`, which is not
            // necessarily what the scan will select on a version-4 object (ADR-0699
            // decision 5), so it is not carried.
            let carried = match blocks_read {
                None => Some(CarriedWholeObject {
                    bytes: bytes.clone(),
                    source_key: seg_ref.data_object_key.clone(),
                    source_tenant: tenant_hash,
                }),
                Some(_) => None,
            };
            Ok(Some((
                scan.survivor_block_indices(),
                dirs,
                scan.stats(),
                None,
                touched,
                carried,
            )))
        }
        .await;
        caller_accounting.merge_snapshot(&phase.snapshot().pooled());
        // ADR-0996 decision 3: record this data object as touched, once, at the
        // plan-then-stripe designated-recorder point. `plan_segment` runs once
        // per segment -- `ravel_sql::logs_scan` awaits the shared plan pass
        // behind a barrier before any partition drains -- so a striped
        // multi-partition scan still records exactly one touch per object here,
        // over however many GETs the plan and its subset scans issue. An object
        // belongs to exactly one segment, so this is one touch per distinct
        // object. The predicate-free full-window fast path skips this method
        // entirely (#693 part 3); its per-segment touch is recorded at
        // ravel-sql's `record_open_shape` site, left for task 996-6 (this task
        // must not touch ravel-sql).
        //
        // `data_objects_touched` counts objects whose BLOCK bytes the query
        // fetched, and its contract
        // (`ravel_types::accounting::QueryAccountingSnapshot::data_objects_touched`)
        // excludes a probe outright: "if the query then decides to fetch no
        // blocks from that object, the object was never touched". Two of the
        // three outcomes here are not touches, and both would inflate the
        // denominator of `range_amplification` in the flattering direction:
        //
        // - `Ok(None)`: the catalog summary proved the segment irrelevant with
        //   no fetch at all.
        // - `Ok(Some((0, .., Some(footer))))`: the skip-decidable branch (#761)
        //   read the probe and the four directories and pruned every block
        //   away. `owned_work` assigns a zero-survivor segment to no partition,
        //   so no scan GET ever follows and not one block byte moves.
        //
        // The fallback (a `None` footer) is a touch only when it actually read
        // this object's blocks. Above the routing threshold it takes the ranged
        // path, which resolves candidate-block extents from the skip index: when
        // a prune arm eliminates every block (e.g. a content arm defeats
        // skip-decidable while a disjoint NumRange arm prunes all blocks) it
        // resolves zero extents and fetches only the directory sections, moving
        // no block byte -- not a touch. So the recorder keys on the blocks each
        // branch actually read (the `touched` flag), not on footer presence: the
        // footer-carrying branches set it from their survivor count, and the
        // fallback from whether `tenant_bytes` read any block (contract:
        // `ravel_types::accounting`, blocks read are cache-inclusive, so the
        // signal is resolved blocks, never wire bytes).
        if let Ok(Some((_, _, _, _, touched, _))) = &result
            && *touched
        {
            caller_accounting.add_data_objects_touched(1);
        }
        result.map(|opt| {
            opt.map(|(indices, dirs, stats, footer, _, carried)| {
                (indices, dirs, stats, footer, carried)
            })
        })
    }

    /// Whether [`plan_segment`](Self::plan_segment)'s survivor count can be read
    /// from the skip index alone, without fetching or decoding any block (#761).
    ///
    /// True when the query carries no block-level predicate the skip index cannot
    /// evaluate: no content/text arm (those prune only via per-block bloom at
    /// decode), no stream-attribute filter, and every prune-only arm a
    /// [`Predicate::NumRange`] (an attribute-equality `Equals` prunes only via
    /// POSTINGS). For such a query the reader's full prune -- skip, then POSTINGS,
    /// then bloom -- collapses to its skip step, so `candidate_blocks` over the
    /// resolved NumRange arms is the exact survivor list the scan will stripe,
    /// and the count is safe to compute from SKIP_IDX + FIELD_DIR alone. Pending
    /// erasure is ignored on purpose: it filters rows within surviving blocks
    /// after decode, so it never changes which blocks survive or how many.
    ///
    /// At least one arm is required, so this stays the SELECTIVE path #761 is
    /// about. A query with no prune arm at all would also be skip-decidable, but
    /// planning it this way is a pessimization rather than a saving: its
    /// plan-phase read is already only the ts-candidate blocks, and it warms
    /// exactly the extents the per-partition subset opens then stripe, so
    /// removing it would replace one shared read with N concurrent per-partition
    /// ones for the same bytes. Such a query keeps the pre-#761 plan read (the
    /// fully-contained case having taken `plan_segment_fast` above).
    ///
    /// Destructures `LogQuery` by name for the reason
    /// [`LogQuery::is_block_predicate_free`] does: a false positive here would
    /// publish a survivor count the scan does not stripe, so a future field must
    /// be a build break rather than a silent gap.
    fn plan_skip_decidable(query: &LogQuery) -> bool {
        let LogQuery {
            ts_min_ns: _,
            ts_max_ns: _,
            stream_attrs,
            content,
            prune,
            erasure: _,
        } = query;
        content.is_empty()
            && stream_attrs.is_empty()
            && !prune.is_empty()
            && prune
                .iter()
                .all(|p| matches!(p, Predicate::NumRange { .. }))
    }

    /// The predicate-free plan fast path (#693): read the footer via the
    /// ADR-0107 suffix probe, read and decode the four directories, and derive
    /// the whole-segment plan counts from `footer.block_count` without fetching
    /// or decoding any block. Returns the
    /// parsed [`footer::LogFooter`] alongside the counts so the per-partition
    /// subset opens can reuse it and skip re-probing (#693 part 3, deliverable
    /// 2; see [`fetch_object_with_footer`](Self::fetch_object_with_footer)).
    ///
    /// For a genuinely predicate-free, ts-contained query these are exactly the
    /// counts real pruning would compute: every block survives every stage, so
    /// `blocks_total`/`blocks_after_skip`/`blocks_after_postings`/
    /// `blocks_after_bloom` all equal the block count, no block is scanned or
    /// decoded, and neither pruning stage degrades. `footer.block_count` equals
    /// the read-time `ScanStats.blocks_total` (`skip.l0.len()`) on every
    /// well-formed object: both are stamped from the writer's one-entry-per-block
    /// `block_spans` counter (see the `footer_block_count_matches_unpruned_\
    /// blocks_total` round-trip proof in `ravel-logseg`).
    /// ADR-2414 decision A1: reads and decodes all four footer directories
    /// (not just the footer [`fetch_footer`](BlockRangeFetcher::fetch_footer)
    /// alone would read), so the returned [`SegmentDirectories`] is ready for
    /// every later open of this segment within the query to reuse via
    /// [`RlogReader::from_decoded`] -- this branch's segment is still opened
    /// per-partition by the striped route when `segment_count <
    /// target_partitions`, and that open must not decode the directories
    /// again.
    async fn plan_segment_fast(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<
        (
            Vec<usize>,
            Arc<SegmentDirectories>,
            ScanStats,
            footer::LogFooter,
        ),
        LogFetchError,
    > {
        // The `page_fetch` phase span the slow path also carries, so the trace
        // shows the probe this path issues. `s3_bytes` reports block bytes only
        // (zero here, matching the slow path's block-range branch convention);
        // the probe's directory-overhead bytes are recorded in `accounting`.
        let fetch_span = tracing::debug_span!(
            "page_fetch",
            signal = "logs",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
            probe_misses = tracing::field::Empty,
        );
        let (footer, dirs, stats) = async {
            self.block_range
                .fetch_plan_directories(seg_ref, tenant_hash, accounting)
                .await
        }
        .instrument(fetch_span.clone())
        .await?;
        let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
        fetch_span.record("s3_requests", requests);
        fetch_span.record("s3_bytes", stats.block_bytes_fetched);
        // Probe misses (#883): a footer chase here is a probe too short to reach
        // even the footer, reported per phase beside the request/byte counts.
        self.record_probe_misses(&fetch_span, &stats, ProbePhase::Plan);

        let n = usize::try_from(footer.block_count).map_err(|_| LogFetchError::Corrupt {
            key: seg_ref.data_object_key.clone(),
            source: LogSegError::Corrupted("footer block_count out of range".into()),
        })?;
        let blocks = footer.block_count as u32;
        // The directory decode just performed is charged here, the one place
        // it happened (ADR-2414 decision A1): every per-partition open of this
        // segment that follows reuses `dirs` and decodes nothing more.
        accounting.add_decompressed_bytes(dirs.open_decompressed_bytes());
        let plan_stats = ScanStats {
            blocks_total: blocks,
            blocks_after_skip: blocks,
            blocks_after_postings: blocks,
            blocks_after_bloom: blocks,
            blocks_scanned: 0,
            pages_decoded: 0,
            pages_skipped: 0,
            page_bytes_fetched: 0,
            page_bytes_decoded: 0,
            decompressed_bytes: 0,
            bloom_degraded: false,
            postings_degraded: false,
        };
        // Every block survives (the fast path's containment check proved it),
        // so the raw whole-object block index equals the ordinal position.
        let raw_indices: Vec<usize> = (0..n).collect();
        Ok((raw_indices, dirs, plan_stats, footer))
    }

    /// Report this segment's exact per-block record counts and per-numeric-column
    /// min/max/null_count for `query`, from the footer and SKIP_IDX alone (#698
    /// deliverable 2, ADR-0699). No BLOCKS byte is fetched and no block is
    /// decoded: the answer comes out of the skip index the ADR-0107 probe already
    /// has to read.
    ///
    /// See [`BlockStatsReport`] for what the three fields mean. Blocks the query
    /// window fully contains are answered here in full; blocks it only clips are
    /// named in `partial_block_indices` for the caller to decode; blocks it
    /// misses contribute nothing.
    ///
    /// # No production caller yet
    ///
    /// Nothing in this commit calls this method. #698 deliverable 1 (fleet task
    /// ca9c1b10) is the follow-up that wires it into `ravel_sql`'s
    /// `LogsScanExec::statistics` / `AggregateStatistics`. Until that lands the
    /// epic capability is NOT reachable end to end.
    ///
    /// # Fail-closed conditions
    ///
    /// `Ok(None)` means "no fast path, fall back to a real scan". It is returned,
    /// without any GET, when:
    ///
    /// - [`ts_range_relevant`](Self::ts_range_relevant) proves the catalog
    ///   summary irrelevant to the query window, the same pre-check
    ///   [`plan_segment`](Self::plan_segment) and
    ///   [`tenant_bytes`](Self::tenant_bytes) already apply;
    /// - the query carries any block-level predicate, i.e.
    ///   [`is_block_predicate_free`](LogQuery::is_block_predicate_free) is false:
    ///   a non-empty `erasure`, `content`, `prune`, or `stream_attrs`. Each of
    ///   those can exclude rows a contained block's stored `record_count` counts,
    ///   so the stored figures would over-report. Erasure is included even though
    ///   it filters rows rather than blocks, for exactly that reason: an erased
    ///   row is still counted in the block's `record_count`;
    /// - `seg_ref.object_size <= self.block_range_threshold`. The read pays off
    ///   only above the threshold, mirroring
    ///   [`plan_segment_fast`](Self::plan_segment_fast)'s own gate: at or below
    ///   it, the whole-object funnel already takes one GET, and
    ///   [`BlockRangeFetcher::fetch_skip_index`] has no whole-object crossover of
    ///   its own, so it would read the same object under a second cache key.
    ///
    /// The ts range itself is NOT a fail-closed condition. Unlike
    /// [`plan_segment`](Self::plan_segment)'s fast path this does not need the
    /// window to contain the whole segment: a window that only clips it still
    /// gets exact figures for the blocks it does contain, plus the partial
    /// blocks named.
    pub async fn plan_segment_block_stats(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        accounting: &QueryAccounting,
    ) -> Result<Option<BlockStatsReport>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }
        if !query.is_block_predicate_free() {
            return Ok(None);
        }
        if seg_ref.object_size <= self.block_range_threshold {
            return Ok(None);
        }

        // The `page_fetch` phase span the other plan paths carry, so the trace
        // shows the probe and the one section GET this path issues. `s3_bytes`
        // reports block bytes only, which are structurally zero here; the
        // directory-overhead bytes are recorded in `accounting`.
        let fetch_span = tracing::debug_span!(
            "page_fetch",
            signal = "logs",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
            probe_misses = tracing::field::Empty,
        );
        let (skip, stats) = async {
            self.block_range
                .fetch_skip_index(seg_ref, tenant_hash, accounting)
                .await
        }
        .instrument(fetch_span.clone())
        .await?;
        let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
        fetch_span.record("s3_requests", requests);
        fetch_span.record("s3_bytes", stats.block_bytes_fetched);
        // Probe misses (#883): SKIP_IDX not covered by the derived probe window,
        // reported per phase beside the request/byte counts.
        self.record_probe_misses(&fetch_span, &stats, ProbePhase::Plan);

        let (ts_min, ts_max) = (query.ts_min_ns, query.ts_max_ns);
        let mut record_count = 0u64;
        let mut contained: Vec<Level0Entry> = Vec::new();
        let mut partial_block_indices = Vec::new();
        for (i, entry) in skip.l0.iter().enumerate() {
            // Containment is inclusive at both ends, the same convention
            // `plan_segment`'s fast path applies to the whole-segment span.
            if ts_min <= entry.min_ts && entry.max_ts <= ts_max {
                record_count += u64::from(entry.record_count);
                contained.push(entry.clone());
            } else if entry.max_ts >= ts_min && entry.min_ts <= ts_max {
                // Overlaps but is not contained: an unknown subset of its rows
                // matches, so only the caller's own decode can settle it. A block
                // disjoint from the window falls through both arms and
                // contributes nothing, which is correct and not an omission.
                partial_block_indices.push(i);
            }
        }

        Ok(Some(BlockStatsReport {
            record_count,
            stats: merge_stats(&contained),
            partial_block_indices,
        }))
    }

    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant),
    /// restricted to the blocks named in `indices` (intra-segment scan
    /// partitioning, ADR-0102). Same GET, same cache, same pruning; the
    /// returned [`LogSegmentScan`] drains only the named subset of the
    /// segment's surviving blocks.
    ///
    /// The returned scan's whole-segment stats totals are reported by
    /// [`plan_segment`](Self::plan_segment) instead, to keep one segment's
    /// totals from being counted once per partition (see
    /// `ravel_sql::logs_scan`).
    ///
    /// `footer`, when `Some`, is the [`footer::LogFooter`] a prior
    /// [`plan_segment`](Self::plan_segment) fast path already read for this exact
    /// (immutable) object (#693 part 3, deliverable 2). Supplying it lets the
    /// block-range read skip its own etag-establishing suffix probe and pin on
    /// the first section/block GET instead (see
    /// [`fetch_object_with_footer`](Self::fetch_object_with_footer)); `None`
    /// probes as before. It changes only the read shape, never the bytes decoded.
    ///
    /// `carried_whole`, when `Some`, is the whole object's bytes a prior
    /// [`plan_segment`](Self::plan_segment) fallback already fetched for this
    /// exact (immutable) object (issue #835). Supplying it skips this call's own
    /// wire GET entirely -- no cache lookup, no store round trip -- and charges
    /// the reused bytes to `accounting.add_bytes_reused` instead of a cache hit
    /// or miss. `None` fetches as before (cache-aware whole-object or ranged
    /// read, per [`tenant_bytes_with_footer`](Self::tenant_bytes_with_footer)).
    ///
    /// [`scan_accounted_with_tenant`]: Self::scan_accounted_with_tenant
    ///
    /// `indices` are ordinal positions into this object's survivor list (a
    /// row-ref's recorded position, ADR-0774). See
    /// [`scan_accounted_with_tenant_subset_raw`](Self::scan_accounted_with_tenant_subset_raw)
    /// for the whole-object-block-index counterpart.
    ///
    /// Issue #796: `scan` phase, same reasoning and the same not-buffered
    /// `accounting` handling as
    /// [`scan_accounted_with_tenant`](Self::scan_accounted_with_tenant)'s doc
    /// comment.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_accounted_with_tenant_subset(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        footer: Option<&footer::LogFooter>,
        carried_whole: Option<CarriedWholeObject>,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        self.scan_accounted_with_tenant_subset_impl(
            seg_ref,
            tenant_hash,
            query,
            columns,
            indices,
            false,
            // Unused: the ordinal case never reaches the raw-subset open that
            // checks a survivor list against this.
            &[],
            footer,
            carried_whole,
            None,
            accounting,
        )
        .await
    }

    /// [`scan_accounted_with_tenant_subset`](Self::scan_accounted_with_tenant_subset),
    /// but `indices` names whole-object block indices (ADR-2414 decision A1):
    /// the partition's share of a segment's row groups as
    /// `ravel_sql::logs_scan::owned_work` deals them, a subset of the PLAN
    /// phase's own survivor list rather than positions into this open's own
    /// (expected-identical) pruning result.
    ///
    /// `dirs`, when `Some`, is the [`SegmentDirectories`] a prior
    /// [`plan_segment`](Self::plan_segment) already decoded for this exact
    /// object: the open reuses them via [`RlogReader::from_decoded`] instead
    /// of decoding STREAM_DIR, FIELD_DIR, SKIP_IDX, and PAGE_DIR again
    /// (ADR-2414 decision A1). `None` decodes as before.
    ///
    /// `expected_survivors` is the whole segment's surviving-block list this
    /// query's [`plan_segment`](Self::plan_segment) already produced, in the
    /// same whole-object-index space as `indices` (not that plan's own
    /// per-partition deal). This open reruns the same skip/POSTINGS/bloom
    /// proofs over the same immutable object and predicate, so its own
    /// `BlockScan::survivor_block_indices` is expected to agree with it
    /// exactly (see [`RlogReader::scan_blocks_raw_subset`]); a mismatch means
    /// `indices` was computed against a different survivor list than this
    /// open actually has, and the call refuses with a typed error rather than
    /// silently keeping whatever intersection happens to still be present
    /// (issue #2417).
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_accounted_with_tenant_subset_raw(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        expected_survivors: &[usize],
        footer: Option<&footer::LogFooter>,
        carried_whole: Option<CarriedWholeObject>,
        dirs: Option<&Arc<SegmentDirectories>>,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        self.scan_accounted_with_tenant_subset_impl(
            seg_ref,
            tenant_hash,
            query,
            columns,
            indices,
            true,
            expected_survivors,
            footer,
            carried_whole,
            dirs,
            accounting,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_accounted_with_tenant_subset_impl(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        raw: bool,
        expected_survivors: &[usize],
        footer: Option<&footer::LogFooter>,
        carried_whole: Option<CarriedWholeObject>,
        dirs: Option<&Arc<SegmentDirectories>>,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        // `indices` is only raw whole-object block indices when `raw` holds
        // (ADR-2414 decision A1 deliverable 3): the ordinal case's indices are
        // positions into the pruned survivor list, a different index space
        // that must never restrict the object-level candidate set below.
        let owned_blocks = raw.then(|| OwnedBlocks {
            blocks: indices,
            dirs: dirs.map(|d| &**d),
        });
        let Some((bytes, _blocks_read)) = self
            .tenant_bytes_with_footer(
                seg_ref,
                tenant_hash,
                query,
                columns,
                footer,
                carried_whole,
                owned_blocks,
                ProbePhase::Scan,
                accounting,
            )
            .await?
        else {
            return Ok(None);
        };
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let (scan, block_job_bytes) = match dirs {
            Some(dirs) if raw => {
                self.open_scan_on_gate_from_decoded(
                    key,
                    &bytes,
                    query,
                    columns,
                    indices,
                    expected_survivors,
                    dirs,
                    true,
                    &span,
                )
                .await?
            }
            _ => {
                self.open_scan_on_gate(
                    key,
                    &bytes,
                    query,
                    columns,
                    Some(indices),
                    raw,
                    expected_survivors,
                    true,
                    accounting,
                    &span,
                )
                .await?
            }
        };
        Ok(Some(self.scan_handle(
            key,
            bytes,
            scan,
            block_job_bytes,
            query,
            span,
            accounting,
        )))
    }

    /// The byte-fetch half of the tenant-aware funnel: the ts-range pre-check,
    /// then the object's bytes from the read cache or a whole-object GET, with
    /// the `page_fetch` span and the accounting both entry points share.
    /// `Ok(None)` means the catalog summary proved the object irrelevant and no
    /// GET was issued.
    ///
    /// `phase` is which of the caller's phases this read belongs to, for the
    /// probe-miss charge (#883). It is a parameter rather than a constant
    /// because this funnel serves both: the scan funnels below reach it as a
    /// data read, and [`plan_segment`](Self::plan_segment)'s fallback reaches it
    /// as a planning read for a predicate the skip index cannot decide.
    ///
    /// The second element of the returned tuple reports how many logical BLOCKS
    /// this read brought into the buffer, cache-inclusive, so a caller that must
    /// decide whether the object was TOUCHED (its blocks read) can key on the
    /// blocks actually resolved rather than on wire bytes: `None` means the whole
    /// object was read (every block is present, so a decode of any of them is a
    /// touch), and `Some(n)` means the ranged path resolved exactly `n`
    /// candidate-block extents into the buffer (a touch iff `n > 0`). See
    /// [`tenant_bytes_with_footer`](Self::tenant_bytes_with_footer).
    async fn tenant_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        phase: ProbePhase,
        accounting: &QueryAccounting,
    ) -> Result<Option<(LogObjectBytes, Option<u64>)>, LogFetchError> {
        self.tenant_bytes_with_footer(
            seg_ref,
            tenant_hash,
            query,
            columns,
            None,
            None,
            None,
            phase,
            accounting,
        )
        .await
    }

    /// [`tenant_bytes`](Self::tenant_bytes), optionally carrying a plan-phase
    /// [`footer::LogFooter`] (#693 part 3, deliverable 2). When `footer` is
    /// `Some` and the object is above the block-range threshold, the block-range
    /// read skips its own suffix probe and uses the carried footer, pinning the
    /// etag on the first section/block GET instead. `None` is the unchanged
    /// probe-first behavior. Below the threshold the whole-object read never
    /// probes anyway, so the footer is irrelevant there.
    ///
    /// `columns` is the same [`ColumnSelection`] the caller will hand the
    /// decode. On a version-4 object it is the FETCH selection too (ADR-0699
    /// decision 5): the block-range read brings one coalesced range per
    /// surviving `(row group, projected column)` rather than every column of
    /// every surviving block. A caller that decodes with a wider selection than
    /// it fetched with would address pages this never brought, which is a typed
    /// `Corrupted` error rather than wrong data, so the two must be the same
    /// value.
    ///
    /// The second element of the returned tuple is the count of logical BLOCKS
    /// brought into the buffer (see [`tenant_bytes`](Self::tenant_bytes)):
    /// `Some(candidate_blocks)` on the ranged (above-threshold) path, the exact
    /// number of block extents the fetch resolved and decoded, and `None` on the
    /// whole-object (below-threshold) path, where every block is present. It lets
    /// a caller decide "were this object's blocks read" from the blocks actually
    /// resolved, not from wire bytes, which a fully-pruned ranged read moves none
    /// of even though the read happened.
    ///
    /// `carried_whole`, when `Some`, is a [`CarriedWholeObject`] a prior
    /// `plan_segment` fallback fetched for this exact object (issue #835): its
    /// bytes are the whole object, valid for any `columns` selection, so this
    /// short-circuits both the below- and above-threshold branches below,
    /// issuing no store GET and no cache lookup at all. The reused bytes are
    /// charged to `accounting.add_bytes_reused`, not to a cache hit -- the
    /// object was never asked of the cache on this call. Safe with no etag
    /// re-check: see [`CarriedWholeObject`]'s doc. A carry whose own
    /// `(key, tenant)` is not this call's is rejected as
    /// [`LogFetchError::CarryMismatch`] before any decode.
    ///
    /// `owned_blocks`, when `Some` (ADR-2414 decision A1 deliverable 3), is
    /// the caller's own raw whole-object block indices: the striped route's
    /// per-partition subset open passes its `owned_work` share so the
    /// above-threshold ranged read covers only those blocks, through
    /// [`BlockRangeFetcher::fetch_object_with_footer_subset`]. A caller with
    /// no such list (every other funnel here) passes `None`, the unchanged
    /// behavior.
    #[allow(clippy::too_many_arguments)]
    async fn tenant_bytes_with_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        footer: Option<&footer::LogFooter>,
        carried_whole: Option<CarriedWholeObject>,
        owned_blocks: Option<OwnedBlocks<'_>>,
        phase: ProbePhase,
        accounting: &QueryAccounting,
    ) -> Result<Option<(LogObjectBytes, Option<u64>)>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }

        if let Some(carried) = carried_whole {
            // The carry answers without consulting `seg_ref`, so a carry paired
            // with another segment would decode that segment's rows out of these
            // bytes wherever both objects are decodable. Prove the pairing first.
            if carried.source_key != seg_ref.data_object_key || carried.source_tenant != tenant_hash
            {
                return Err(LogFetchError::CarryMismatch {
                    key: seg_ref.data_object_key.clone(),
                    carried_key: carried.source_key,
                    tenant: tenant_hash,
                    carried_tenant: carried.source_tenant,
                });
            }
            accounting.add_bytes_reused(carried.bytes.len() as u64);
            return Ok(Some((carried.bytes, None)));
        }

        // #913: which phase this read's metadata GETs and its BLOCKS-section
        // GETs charge their wire bytes to. The caller already named the phase
        // it reads on, so the split is derived here rather than passed again.
        let phases = phase.read_phases();

        // ADR-0107: an object above the block-range threshold is fetched by
        // reading only the blocks skip-index pruning proved relevant, not the
        // whole object. Small objects (all current fixtures, RLOG's typical
        // size) fall through to the unchanged whole-object path below. The
        // block-range fetcher records its own store/cache accounting; this span
        // reports its store-GET totals so the phase stays visible.
        if seg_ref.object_size > self.block_range_threshold {
            let fetch_span = tracing::debug_span!(
                "page_fetch",
                signal = "logs",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
                probe_misses = tracing::field::Empty,
            );
            // What the plan read that produced this footer already counted
            // (#883, issue #885 review). `plan_segment` has exactly two
            // footer-carrying branches, and since ADR-2414 decision A1 both
            // read the segment's directories through
            // `fetch_plan_directories`, which counts the SKIP_IDX and PAGE_DIR
            // probe misses into the plan phase. A footer
            // carried for this (segment, query) pair therefore always
            // arrives with its tail misses already counted.
            let carried = footer.map(|f| CarriedFooter {
                footer: f,
                tail_misses_counted: true,
            });
            let (bytes, stats) = async {
                self.block_range
                    .fetch_object_with_footer_subset(
                        seg_ref,
                        tenant_hash,
                        query.ts_min_ns,
                        query.ts_max_ns,
                        &query.prune,
                        columns,
                        carried,
                        phases,
                        owned_blocks,
                        accounting,
                    )
                    .await
            }
            .instrument(fetch_span.clone())
            .await?;
            let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
            fetch_span.record("s3_requests", requests);
            fetch_span.record("s3_bytes", stats.block_bytes_fetched);
            // Probe misses (#883): SKIP_IDX/PAGE_DIR (and any footer chase) not
            // covered by the derived probe window on this read, reported beside
            // the request/byte counts and charged to the caller's phase.
            self.record_probe_misses(&fetch_span, &stats, phase);
            // Blocks read on the ranged path: the candidate extents the fetch
            // resolved, EXCEPT when a crossover (size-threshold or coverage) took
            // the whole object anyway, where every block is present -- reported as
            // `None`, the same "whole object" signal the below-threshold branch
            // returns, so the caller counts a touch without a block count it does
            // not have here. A non-crossover ranged read that resolved zero
            // candidates moved no block bytes and is `Some(0)`.
            let blocks_read = if stats.whole_object {
                None
            } else {
                Some(stats.candidate_blocks)
            };
            return Ok(Some((bytes, blocks_read)));
        }

        // Whole-object (below-threshold) read: the entire object is in the buffer,
        // so every block is present; `None` signals that to the caller.
        Ok(Some((
            self.whole_object_bytes(seg_ref, tenant_hash, phases.blocks, accounting)
                .await?,
            None,
        )))
    }

    /// One whole-object [`GetRange::Full`] read, cache-keyed `(0, object_size)`,
    /// with the `page_fetch` span and accounting the tenant-aware funnels share.
    /// The caller has already decided the object is relevant; this only fetches.
    ///
    /// Used by the below-threshold branch of
    /// [`tenant_bytes_with_footer`](Self::tenant_bytes_with_footer) and by the
    /// predicate-free full-window whole-segment path
    /// ([`scan_whole_accounted_with_tenant`](Self::scan_whole_accounted_with_tenant),
    /// #693 part 3), which reads the whole object in one GET regardless of size.
    ///
    /// `phase` is the [`QueryPhase`] the GET's WIRE bytes are charged to
    /// (#913). This read exists to obtain block data, so its caller passes
    /// [`ReadPhases::blocks`]: on a scan that is [`QueryPhase::Scan`], and on
    /// `plan_segment`'s whole-object fallback it is [`QueryPhase::Plan`], which
    /// keeps a planning read out of the scan-phase numerator.
    async fn whole_object_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        phase: QueryPhase,
        accounting: &QueryAccounting,
    ) -> Result<LogObjectBytes, LogFetchError> {
        let key = &seg_ref.data_object_key;

        // Fetch bound (ADR-0996 decision 2): an object above the bound is read
        // as `ceil(object_size / bound)` sequential covering sub-range GETs
        // instead of one whole-object GET, so no single request moves more than
        // the bound. Under request-minimal the routing threshold is saturated,
        // so every object takes this whole-object funnel and this is where a
        // large one is segmented. On the reference corpus every object is far
        // under the 64 MiB default, so this branch is a protective cap only. The
        // segmented read routes through the block-range fetcher's covering read,
        // which shares this fetcher's cache and wire-byte counter, and whose
        // per-sub-range accounting records the same GET total this funnel would.
        if seg_ref.object_size > self.block_range.max_fetch_run_bytes() {
            let pin = EtagPin::default();
            let fetch_span = tracing::debug_span!(
                "page_fetch",
                signal = "logs",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
            );
            let (bytes, live_gets, live_bytes) = self
                .block_range
                .covering_read(
                    seg_ref,
                    tenant_hash,
                    seg_ref.object_size,
                    GetRange::Full,
                    phase,
                    &pin,
                    accounting,
                )
                .instrument(fetch_span.clone())
                .await?;
            fetch_span.record("s3_requests", live_gets);
            fetch_span.record("s3_bytes", live_bytes);
            return Ok(bytes);
        }

        // Same two phases as `fetch_accounted`, spanned on the path production
        // log/alerts/audit traffic actually takes (ADR-0044 decision 5): the
        // whole-object GET (`page_fetch`) here, then the STREAM_DIR resolve +
        // `RlogReader` prune and decode (`decode`) in whichever of the two
        // callers this feeds. Duplicated rather than shared with
        // `fetch_accounted` because the byte-fetch differs -- this one is
        // cache-aware and may serve a hit with no store GET at all. The
        // recorded `s3_requests`/`s3_bytes` reflect this call's own store GETs:
        // one on the uncached path or when this call ran the cache-miss fetch,
        // zero on a cache hit or a late serve (no GET by this call).
        let fetch_span = tracing::debug_span!(
            "page_fetch",
            signal = "logs",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );

        // Reserve the whole object's bytes before the direct whole-object GET
        // (ADR-1170 decision 2), so a refusal fails typed with zero GETs. The
        // guard travels with the returned `Bytes` (below), owned for the
        // fetched buffer's lifetime rather than released when the GET completes.
        let reservation = self.reserve_fetch(seg_ref.object_size)?;

        let Some(cache) = &self.cache else {
            let got = async {
                // Held across the GET only: dropped when this inner block
                // returns, before this function hands the bytes to its
                // caller's decode (ADR-1195).
                let _permit =
                    self.get_limiter
                        .acquire()
                        .await
                        .map_err(|_| LogFetchError::Store {
                            key: key.to_string(),
                            source: StoreError::Transient(
                                "GetLimiter semaphore closed unexpectedly".to_string(),
                            ),
                        })?;
                self.store
                    .get(key, GetRange::Full)
                    .await
                    .map_err(|source| LogFetchError::Store {
                        key: key.to_string(),
                        source,
                    })
            }
            .instrument(fetch_span.clone())
            .await?;
            accounting.record_s3_request(AccountedOp::Get);
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
            self.wire_bytes.record(phase, got.data.len() as u64);
            fetch_span.record("s3_requests", 1u64);
            fetch_span.record("s3_bytes", got.data.len() as u64);
            return Ok(attach_reservation(got.data, reservation).into());
        };

        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, 0, seg_ref.object_size);
        // One read-through call, accounted from the returned [`ReadOutcome`]:
        // a single call avoids the peek-then-`get_or_fetch` double-count on the
        // tiered tier (see [`ReadCache::get_or_fetch`]).
        let (bytes, outcome) = async {
            cache
                .get_or_fetch(cache_key, || async move {
                    // Held across the GET only: dropped before this closure
                    // returns the bytes to its caller's decode (ADR-1195).
                    let _permit = self.get_limiter.acquire().await.map_err(|_| {
                        StoreError::Transient(
                            "GetLimiter semaphore closed unexpectedly".to_string(),
                        )
                    })?;
                    let got = self.store.get(key, GetRange::Full).await?;
                    accounting.record_s3_request(AccountedOp::Get);
                    accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
                    self.wire_bytes.record(phase, got.data.len() as u64);
                    Ok(got.data)
                })
                .await
        }
        .instrument(fetch_span.clone())
        .await
        // Shared with the block-range path's mapping, which is the only one
        // whose closure can produce the `EtagChanged`/`Corrupt` classes: this
        // funnel's closure is one unconditional whole-object GET that
        // verifies nothing, so those arms are unreachable here rather than
        // wrong.
        .map_err(|err| from_cache_error(key, err))?;
        let mut reservation = reservation;
        match outcome {
            ReadOutcome::Hit => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                // Served from cache: no S3 GET on this call.
                fetch_span.record("s3_requests", 0u64);
                fetch_span.record("s3_bytes", 0u64);
                // The returned `Bytes` is a clone of the cache entry's, so the
                // same allocation is under the cache's byte ledger AND this
                // fetch guard for as long as the caller holds it. That is the
                // two-ledger overlap decision 2's handoff rule describes, and
                // it is no less real for arriving by a hit than by an insert:
                // leaving it unmarked understates `handoff_overlap` by every
                // cache hit, which is what makes the `unique` term inexact.
                reservation.mark_handed_off();
            }
            // A late serve (another caller's flight, or the RAM recheck) made
            // no GET of its own and is not a hit: the miss its lookup counted,
            // no S3 cost. Its bytes are the cache entry's, as on a hit.
            ReadOutcome::LateServe => {
                accounting.record_cache_miss();
                fetch_span.record("s3_requests", 0u64);
                fetch_span.record("s3_bytes", 0u64);
                reservation.mark_handed_off();
            }
            // This call's own store GET produced the bytes.
            ReadOutcome::Fetched => {
                accounting.record_cache_miss();
                fetch_span.record("s3_requests", 1u64);
                fetch_span.record("s3_bytes", bytes.len() as u64);
                // The fetched buffer was just offered to the read cache, which
                // has its own byte ledger (ADR-1170 decision 2's handoff rule):
                // mark this reservation handed off, whether or not the cache
                // admitted it, so the transient overlap is visible while both
                // this returned buffer and the cache entry hold the same bytes.
                // The overlap clears when this guard drops.
                reservation.mark_handed_off();
            }
        }
        Ok(attach_reservation(bytes, reservation).into())
    }

    /// Runs [`scan_bytes`](Self::scan_bytes) inside the log path's `decode`
    /// span, recording the reader's block-scan counts on it afterward.
    ///
    /// # How this span's field set relates to the metric path's `decode`
    ///
    /// The metric path's `decode` span (`crate::fetcher`) carries `page_kind`,
    /// `series_count`, and `decompressed_bytes`. This one carries `signal =
    /// "logs"`, `blocks_scanned`/`blocks_total`, and `decompressed_bytes`
    /// (documented in docs/guides/tracing.md). The byte figure is
    /// [`ScanStats::decompressed_bytes`]: what zstd produced opening the
    /// object's directory sections, probing POSTINGS, and decoding the drained
    /// blocks, the same total `scan_bytes` charges to the accounting handle.
    ///
    /// `blocks_scanned`/`blocks_total` are a pruning-effectiveness signal --
    /// how much of the object's block index the scan actually had to touch
    /// after skip-index, POSTINGS, and bloom pruning -- analogous to the
    /// metric path's `catalog_resolve` `segments_pruned`, which is likewise a
    /// pruning count rather than a byte count.
    async fn decode_spanned(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let span = decode_span();
        match &self.read_gate {
            None => span.in_scope(|| self.scan_bytes(key, bytes, query, accounting, &span)),
            Some(gate) => {
                self.scan_bytes_on_gate(gate, key, bytes, query, accounting, &span)
                    .await
            }
        }
    }

    /// [`scan_bytes`](Self::scan_bytes) on the read gate (ADR-1702 decision
    /// 4): the open is one `log_postings` job and each surviving block, all its
    /// pages, one `log_block` job, sized like
    /// [`LogSegmentScan::next_block_on_gate`]'s. Same records, same counters,
    /// charged the same way, including on a drain that stops early.
    async fn scan_bytes_on_gate(
        &self,
        gate: &Arc<ReadGate>,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        accounting: &QueryAccounting,
        span: &tracing::Span,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let (mut scan, block_job_bytes) = self
            .open_scan_on_gate(
                key,
                bytes,
                query,
                &ColumnSelection::all(),
                None,
                false,
                &[],
                true,
                accounting,
                span,
            )
            .await?;
        // The counters as of the last block that came back, which is all a
        // drain that loses its cursor to a failed job can still report.
        let mut stats = scan.stats();
        let mut records = Vec::new();
        let drained = loop {
            let decoded = if scan.remaining_blocks() == 0 {
                span.in_scope(|| scan.next_block(bytes))
            } else {
                let job_bytes = bytes.clone();
                let job_span = span.clone();
                let ran = gate
                    .run(
                        ReadSite::LogBlock,
                        JobSize::Bytes(block_job_bytes),
                        move || {
                            let decoded = job_span.in_scope(|| scan.next_block(&job_bytes));
                            (scan, decoded)
                        },
                    )
                    .await;
                match ran {
                    Ok((cursor, decoded)) => {
                        scan = cursor;
                        decoded
                    }
                    Err(err) => break Err(log_gate_failed(key, err)),
                }
            };
            stats = scan.stats();
            match decoded {
                Ok(Some(mut rows)) => {
                    crate::erasure::retain_log_records(&mut rows, &query.erasure);
                    records.extend(rows);
                }
                Ok(None) => break Ok(()),
                Err(source) => break Err(corrupt(key, source)),
            }
        };
        accounting.add_decompressed_bytes(stats.decompressed_bytes);
        span.record("blocks_scanned", stats.blocks_scanned);
        span.record("blocks_total", stats.blocks_total);
        span.record("decompressed_bytes", stats.decompressed_bytes);
        drained?;
        Ok(Some(LogFetchOutput { records, stats }))
    }

    /// Shared tail of both fetch entry points: open the pruned scan and drain
    /// every block of it. This is [`LogSegmentScan`] collected eagerly, so the
    /// two paths cannot drift: same predicate, same prune channel, same
    /// per-record erasure exclusion, same order.
    ///
    /// `span` is the `decode` span this runs inside; its counters are recorded
    /// here, at the same point the accounting handle is charged, so a drain
    /// that stops on a corrupt block still reports what it did before the
    /// error rather than leaving the span's fields empty.
    fn scan_bytes(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        accounting: &QueryAccounting,
        span: &tracing::Span,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let mut scan = self.open_scan(key, bytes, query, &ColumnSelection::all(), accounting)?;
        let mut records = Vec::new();
        let drained = loop {
            match scan.next_block(bytes) {
                Ok(Some(mut rows)) => {
                    // Selective-erasure exclusion (ADR-0064 decision 2): drop
                    // every row a pending erasure predicate matches. Applied
                    // here, on the decoded records, so it excludes rows
                    // identically whether `bytes` came from the store or from
                    // a cache hit -- the whole point of filtering after fetch
                    // and after cache. A no-op when `query.erasure` is empty.
                    crate::erasure::retain_log_records(&mut rows, &query.erasure);
                    records.extend(rows);
                }
                Ok(None) => break Ok(()),
                Err(source) => break Err(corrupt(key, source)),
            }
        };
        // The eager counterpart of `LogSegmentScan::finish`: the bytes zstd
        // produced opening the directories, probing POSTINGS, and decoding the
        // drained blocks are charged once, to the handle this funnel's GET
        // already landed on, whether the drain finished or stopped on a
        // corrupt block (issue #1401). Work done before an error is still work
        // done.
        let stats = scan.stats();
        accounting.add_decompressed_bytes(stats.decompressed_bytes);
        span.record("blocks_scanned", stats.blocks_scanned);
        span.record("blocks_total", stats.blocks_total);
        span.record("decompressed_bytes", stats.decompressed_bytes);
        drained?;
        Ok(Some(LogFetchOutput { records, stats }))
    }

    /// Resolve stream-attribute equalities against STREAM_DIR
    /// (over-approximating, see [`matching_streams`](Self::matching_streams)),
    /// build the combined exact predicate, and open the pruned scan with
    /// `query.prune` as the prune-only channel.
    ///
    /// Identical regardless of whether `bytes` came from the store or the cache
    /// -- the reader's block-level skip-index and bloom verification run
    /// unconditionally either way, so a corrupt cache entry fails exactly like
    /// a corrupt store read.
    ///
    /// [`matching_streams`]: Self::matching_streams
    fn open_scan(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<BlockScan, LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query, accounting)?;
        let reader =
            RlogReader::from_source(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        // `prune` is passed as the reader's prune-only channel, never folded
        // into `pred`: an arm there would become an exact per-row filter and
        // drop resource/scope-only matches (docs/adrs/0049-rlog-postings.md
        // amendment 2026-08-03). An empty channel makes this identical to
        // `scan`.
        reader
            .scan_blocks(&pred, &query.prune, columns)
            .map_err(|source| corrupt(key, source))
    }

    /// [`open_scan`](Self::open_scan), restricted to the surviving blocks at the
    /// positions in `indices` (intra-segment scan partitioning, ADR-0102). The
    /// predicate, prune channel, and pruning are identical to `open_scan`; only
    /// the set of blocks the returned cursor will drain differs. `indices`
    /// index into the same ordered survivor list `open_scan` would produce over
    /// this (immutable) object, so they line up with a prior
    /// [`plan_segment`](Self::plan_segment) count or a row-ref's recorded
    /// position (ADR-0774).
    fn open_scan_subset(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        accounting: &QueryAccounting,
    ) -> Result<BlockScan, LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query, accounting)?;
        let reader =
            RlogReader::from_source(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        reader
            .scan_blocks_subset(&pred, &query.prune, columns, indices)
            .map_err(|source| corrupt(key, source))
    }

    /// [`open_scan_subset`](Self::open_scan_subset), but `indices` names
    /// whole-object block indices (ADR-2414 decision A1), not ordinal survivor
    /// positions: the partition's share of a segment's row groups as
    /// `ravel_sql::logs_scan::owned_work` deals them, a subset of the PLAN
    /// phase's own survivor list. `expected_survivors` is that list; see
    /// [`RlogReader::scan_blocks_raw_subset`] for the fail-closed check against
    /// this open's own (expected-identical) pruning result.
    #[allow(clippy::too_many_arguments)]
    fn open_scan_raw_subset(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        expected_survivors: &[usize],
        accounting: &QueryAccounting,
    ) -> Result<BlockScan, LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query, accounting)?;
        let reader =
            RlogReader::from_source(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        reader
            .scan_blocks_raw_subset(&pred, &query.prune, columns, indices, expected_survivors)
            .map_err(|source| corrupt(key, source))
    }

    /// [`open_scan_raw_subset`](Self::open_scan_raw_subset), reusing an
    /// already-decoded [`SegmentDirectories`] via [`RlogReader::from_decoded`]
    /// instead of decoding STREAM_DIR, FIELD_DIR, SKIP_IDX, and PAGE_DIR again
    /// from `bytes` (ADR-2414 decision A1): the striped route's per-partition
    /// open of a segment the plan phase already opened once.
    #[allow(clippy::too_many_arguments)]
    fn open_scan_raw_subset_with_decoded(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        expected_survivors: &[usize],
        dirs: &SegmentDirectories,
    ) -> Result<BlockScan, LogFetchError> {
        let stream_ids = if query.stream_attrs.is_empty() {
            None
        } else {
            Some(matching_streams_in(dirs.stream_dir(), &query.stream_attrs))
        };
        let pred = combined_predicate_with_streams(query, stream_ids);
        let reader = RlogReader::from_decoded(bytes, dirs);
        reader
            .scan_blocks_raw_subset(&pred, &query.prune, columns, indices, expected_survivors)
            .map_err(|source| corrupt(key, source))
    }

    /// [`open_scan`](Self::open_scan), also returning the [`SegmentDirectories`]
    /// this open decoded (ADR-2414 decision A1), for a caller that will open
    /// this same segment again later within the query and wants to reuse the
    /// decode via [`RlogReader::from_decoded`] instead of repeating it.
    fn open_scan_with_directories(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<(BlockScan, Arc<SegmentDirectories>), LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query, accounting)?;
        let reader =
            RlogReader::from_source(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        let dirs = Arc::new(reader.directories());
        let scan = reader
            .scan_blocks(&pred, &query.prune, columns)
            .map_err(|source| corrupt(key, source))?;
        Ok((scan, dirs))
    }

    /// The combined exact predicate (`ts range AND resolved streams AND
    /// content`) both scan-opening paths hand to the reader. Stream-attribute
    /// equalities are resolved against STREAM_DIR here (over-approximating, see
    /// [`matching_streams`](Self::matching_streams)); the prune-only channel is
    /// passed separately by the caller.
    fn combined_predicate(
        &self,
        key: &str,
        bytes: &LogObjectBytes,
        query: &LogQuery,
        accounting: &QueryAccounting,
    ) -> Result<Predicate, LogFetchError> {
        let stream_ids = if query.stream_attrs.is_empty() {
            None
        } else {
            Some(
                self.matching_streams(bytes, &query.stream_attrs, accounting)
                    .map_err(|source| corrupt(key, source))?,
            )
        };

        Ok(combined_predicate_with_streams(query, stream_ids))
    }

    /// Fetch and decode just the STREAM_DIR section (ADR-1103 decision 2's
    /// cost model): the same ADR-0107 probe [`fetch_footer`](Self::fetch_footer)
    /// issues, then the one front section the footer's directory locates it
    /// at, reading no BLOCKS byte and no other directory section. Mirrors
    /// [`fetch_skip_index`](Self::fetch_skip_index)'s shape; the only
    /// difference is that STREAM_DIR sits at the object FRONT, so unlike
    /// SKIP_IDX it is essentially never covered by the tail suffix probe and
    /// [`plan_section_raw`](Self::plan_section_raw) issues its own range GET
    /// for it.
    ///
    /// Returns `None` when the object's footer carries no STREAM_DIR section
    /// at all (never observed on a real object today, but the same
    /// "kind the footer does not carry is skipped" tolerance
    /// [`place_front_sections`](Self::place_front_sections) documents applies
    /// here rather than treating an absent directory as corruption). Otherwise
    /// returns every entry's stream id and its raw (still-encoded)
    /// stream-attrs blob, for the caller to decode with
    /// [`ravel_logseg::record::decode_stream_attrs`].
    ///
    /// At or below [`Self::block_range_threshold`] this takes the same
    /// whole-object crossover [`plan_segment`](Self::plan_segment) and
    /// [`tenant_bytes`](Self::tenant_bytes) apply, via
    /// [`whole_object_bytes`](Self::whole_object_bytes): a ranged probe would
    /// pay for a second cache key on an object [`fetch_footer`](Self::fetch_footer)'s
    /// doc explains is already read whole in one GET below the threshold. Above
    /// it, the ranged probe-then-section path below is what actually saves the
    /// BLOCKS bytes the ADR-1103 cost model counts on.
    pub(crate) async fn fetch_stream_dir(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<Option<Vec<(LogStreamId, Vec<u8>)>>, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let dir: Option<StreamDir> = if seg_ref.object_size > self.block_range_threshold {
            // The `page_fetch` phase span the other plan paths carry (#782), so
            // the trace shows the probe and section GETs this method issues.
            // `s3_bytes` reports block bytes only, which are structurally zero
            // here; the directory-overhead bytes are recorded in `accounting`.
            let fetch_span = tracing::debug_span!(
                "page_fetch",
                signal = "logs",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
                probe_misses = tracing::field::Empty,
            );
            let (dir, stats) = async {
                let mut stats = BlockRangeStats::default();
                let pin = EtagPin::default();
                let phase = ReadPhases::PLAN.metadata;
                let (footer, resident) = self
                    .block_range
                    .probe_footer(seg_ref, tenant_hash, phase, &pin, accounting, &mut stats)
                    .await?;

                let dir = match footer.section(kind::STREAM_DIR).copied() {
                    None => None,
                    Some(stream_desc) => {
                        let raw = self
                            .block_range
                            .plan_section_raw(
                                seg_ref,
                                tenant_hash,
                                &stream_desc,
                                &resident,
                                phase,
                                &pin,
                                accounting,
                                &mut stats,
                            )
                            .await?;
                        Some(
                            StreamDir::decode(&raw, MAX_STREAMS)
                                .map_err(|source| corrupt(key, source))?,
                        )
                    }
                };
                Ok::<_, LogFetchError>((dir, stats))
            }
            .instrument(fetch_span.clone())
            .await?;
            let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
            fetch_span.record("s3_requests", requests);
            fetch_span.record("s3_bytes", stats.block_bytes_fetched);
            // Probe misses (#883): a footer chase or section miss here is a
            // probe too short to reach it, reported per phase beside the
            // request/byte counts like every other plan-phase read.
            self.record_probe_misses(&fetch_span, &stats, ProbePhase::Plan);
            dir
        } else {
            let bytes = self
                .whole_object_bytes(seg_ref, tenant_hash, QueryPhase::Plan, accounting)
                .await?;
            let footer = footer::open_source(&bytes).map_err(|source| corrupt(key, source))?;
            match footer.section(kind::STREAM_DIR).copied() {
                None => None,
                Some(desc) => {
                    let raw = read_section_accounted_from(&bytes, &desc, &self.cfg, accounting)
                        .map_err(|source| corrupt(key, source))?;
                    Some(
                        StreamDir::decode(&raw, MAX_STREAMS)
                            .map_err(|source| corrupt(key, source))?,
                    )
                }
            }
        };
        Ok(dir.map(|dir| {
            dir.entries()
                .iter()
                .map(|entry| (entry.stream_id, entry.blob.clone()))
                .collect()
        }))
    }

    /// Decodes the STREAM_DIR section of an object from its own public section
    /// descriptor, using the crate's public whole-section reader.
    /// This does not go through [`RlogReader`], which decodes STREAM_DIR
    /// internally but exposes no accessor for it.
    ///
    /// Charges the bytes zstd produced to `accounting` (issue #1401 finding 3):
    /// `bytes` was already fetched by the caller, so this is the one
    /// in-memory decompression this method itself performs, and it lands
    /// under whichever phase handle the caller is threading.
    fn decode_stream_dir<S: ByteSource + ?Sized>(
        &self,
        bytes: &S,
        accounting: &QueryAccounting,
    ) -> Result<StreamDir, LogSegError> {
        let footer = footer::open_source(bytes)?;
        let desc = footer
            .section(kind::STREAM_DIR)
            .ok_or_else(|| LogSegError::Corrupted("missing STREAM_DIR section".into()))?;
        let raw = read_section_accounted_from(bytes, desc, &self.cfg, accounting)?;
        StreamDir::decode(&raw, MAX_STREAMS)
    }
}

/// The stream ids in `dir` whose attribute blob carries every filter's needle
/// ([`LogSegmentFetcher::matching_streams`]'s resolution over an
/// already-decoded STREAM_DIR).
fn matching_streams_in(dir: &StreamDir, filters: &[StreamAttrEquals]) -> Vec<LogStreamId> {
    let needles: Vec<Vec<u8>> = filters.iter().map(stream_attr_needle).collect();
    let mut out = Vec::new();
    for entry in dir.entries() {
        if needles.iter().all(|n| blob_contains(&entry.blob, n)) {
            out.push(entry.stream_id);
        }
    }
    out
}

/// `ts range AND resolved streams AND content`, the predicate both
/// scan-opening paths hand to the reader. `stream_ids` is `Some` when the query
/// carries stream-attribute equalities; an empty set is intentional: it means
/// no stream in the object satisfies the filter, and the reader short-circuits
/// an empty StreamIn to zero records.
fn combined_predicate_with_streams(
    query: &LogQuery,
    stream_ids: Option<Vec<LogStreamId>>,
) -> Predicate {
    let mut arms = Vec::with_capacity(2 + query.content.len());
    arms.push(Predicate::TsRange {
        min_ns: query.ts_min_ns,
        max_ns: query.ts_max_ns,
    });
    if let Some(ids) = stream_ids {
        arms.push(Predicate::StreamIn(ids));
    }
    arms.extend(query.content.iter().cloned());
    Predicate::And(arms)
}

/// Default suffix length of the etag-establishing probe GET (ADR-0107 decision
/// 1, ADR-0699 decision 5). The RLOG tail metadata (SKIP_IDX, PAGE_DIR, BLOOM,
/// POSTINGS) and the footer sit after the (large) BLOCKS section, so a suffix
/// of this size covers the whole tail in one GET and no extra metadata GET is
/// needed for them.
///
/// 256 KiB rather than the 64 KiB this started at (issue #766). The plan phase
/// needs SKIP_IDX, and under version 4 the fetch phase needs PAGE_DIR as well;
/// both sit *before* BLOOM in the object, so a suffix probe reaches them only
/// by spanning BLOOM too. On the reference ClickBench tenant BLOOM averages
/// 86 KB, so the 64 KiB probe missed SKIP_IDX on 68.8% of above-threshold
/// objects and cost 4,415 extra GETs per predicated statement. The
/// `probe_covers_the_plan_sections_on_a_wide_row_group_object` test measures
/// the tail on a 32-block-group, 105-column object and pins that this value
/// covers it.
///
/// This is a request-count choice with a byte cost: the probe is a fixed
/// per-object read that a narrow projection does not shrink, so on a small
/// object it can dominate the column chunks themselves. It is cache-routed and
/// shared by every partition and by the plan and scan phases of one statement,
/// which is what keeps that cost to once per object per query rather than once
/// per read. `BlockRangeFetcher::with_suffix_len` pins it explicitly.
///
/// Since #883 this 256 KiB is the CEILING of a per-object derivation
/// ([`derive_suffix_len`]), not a flat default: the fixed 256 KiB was sized for
/// objects with far more blocks than the reference ClickBench tenant carries
/// (mean object 3.47 MB, ~4 blocks), where it reads 7.6% of an entire object to
/// locate a tail of tens of KB and costs a ~0.91 GB plan-phase floor across a
/// full-corpus pass. The derivation shrinks the probe for a small object while
/// keeping this ceiling for a wide one whose tail genuinely approaches it. The
/// ceiling stays 256 KiB because the widest object measured (a 105-column,
/// 128-block object, issue #766) needs a probe this large to carry footer +
/// SKIP_IDX + PAGE_DIR past the BLOOM section between them in one GET; beyond it
/// a longer probe is pure over-read.
pub const DEFAULT_LOG_SUFFIX_LEN: u64 = 256 * 1024;

/// Floor of the per-object suffix-probe derivation ([`derive_suffix_len`], issue
/// #883): the smallest probe issued for any object above the whole-object
/// threshold. The plan and scan probes must reach SKIP_IDX (and, on a version-4
/// object, PAGE_DIR), which sit BEFORE the BLOOM section in the object, so a
/// suffix probe reaches them only by spanning BLOOM too. On the reference
/// ClickBench tenant BLOOM averages 86 KB (issue #766); 128 KiB covers that
/// average plus the adjacent PAGE_DIR/SKIP_IDX/POSTINGS/footer with headroom, so
/// a floor-sized probe still captures the plan sections of a low-block-count
/// object in ONE request. Sized deliberately above the pre-#766 64 KiB probe,
/// which missed SKIP_IDX on 68.8% of above-threshold objects: a miss costs a
/// second round trip, and at ~1.8 MiB per request (`DEFAULT_LOG_REQUEST_COST\
/// _BYTES`) trading 64 KiB of over-read for an extra GET is a large net loss.
/// The floor is what enforces "fewer bytes at NO more requests": lowering it
/// trades bytes for miss risk and must be validated against the per-phase
/// [`BlockRangeStats::probe_misses`] on a real corpus first.
pub const LOG_SUFFIX_FLOOR_BYTES: u64 = 128 * 1024;

/// Divisor of the per-object suffix-probe derivation ([`derive_suffix_len`],
/// issue #883): the probe targets `object_size / LOG_SUFFIX_SIZE_DIVISOR`,
/// clamped to `[LOG_SUFFIX_FLOOR_BYTES, DEFAULT_LOG_SUFFIX_LEN]`. The tail a
/// probe must reach (footer, POSTINGS, BLOOM, PAGE_DIR, SKIP_IDX) grows with the
/// block and column count that also drives object size, so for a fixed schema
/// the tail is a roughly constant fraction of the object and a fraction of the
/// size is a sound proxy for it. `32` places the ceiling break-even at
/// `DEFAULT_LOG_SUFFIX_LEN * 32` = 8 MiB (an object at or above 8 MiB probes the
/// full 256 KiB) and the floor break-even at `LOG_SUFFIX_FLOOR_BYTES * 32` = 4
/// MiB (an object at or below 4 MiB probes the 128 KiB floor). The reference
/// tenant's 3.47 MB mean object therefore probes the floor, 128 KiB rather than
/// 256 KiB: half the plan-phase probe bytes for the same one request.
pub const LOG_SUFFIX_SIZE_DIVISOR: u64 = 32;

/// The suffix-probe length for an object of `total_size` bytes when no explicit
/// probe is pinned (issue #883): `total_size / LOG_SUFFIX_SIZE_DIVISOR` clamped
/// to `[LOG_SUFFIX_FLOOR_BYTES, DEFAULT_LOG_SUFFIX_LEN]`. The result is not
/// capped to `total_size` here (a probe longer than the object is a well-formed
/// suffix GET the store returns whole); callers that need the effective,
/// object-capped value go through [`BlockRangeFetcher::effective_suffix_len`].
///
/// See [`LOG_SUFFIX_FLOOR_BYTES`], [`LOG_SUFFIX_SIZE_DIVISOR`], and
/// [`DEFAULT_LOG_SUFFIX_LEN`] for the reasoning behind each bound. A too-small
/// result costs a second request on a probe miss, which is the exchange rate
/// this derivation is calibrated against, so it is pinned by a test
/// (`derives_probe_from_object_size`) and reported per phase via
/// [`BlockRangeStats::probe_misses`].
#[must_use]
pub fn derive_suffix_len(total_size: u64) -> u64 {
    (total_size / LOG_SUFFIX_SIZE_DIVISOR).clamp(LOG_SUFFIX_FLOOR_BYTES, DEFAULT_LOG_SUFFIX_LEN)
}

/// Default cost of one store request, expressed as a latency-bandwidth product:
/// the byte volume whose transfer time (at the store's single-stream bandwidth)
/// equals one request's round-trip latency. This is the exchange rate between
/// the two things a range-vs-whole-object decision trades: a saved request is
/// worth this many saved bytes, so a decision that avoids `k` requests to move
/// `b` extra bytes is a win exactly when `b < k * request_cost`.
///
/// The default is derived from q20 on in-region S3 from an r6a.4xlarge at 8
/// concurrent fetch permits: 20.95 ms of occupied permit time per GET, of which
/// ~1.01 ms is payload transfer at ~90 MB/s single-stream, so ~95% of each
/// request is round-trip latency. 20.95 ms x 90 MB/s ~= 1.8 MiB of transfer buys
/// one round trip.
///
/// This is a property of the STORE AND INSTANCE (its latency and per-stream
/// bandwidth at the fetch concurrency in use), NOT of the RLOG format, which is
/// why it is a configurable tunable rather than a frozen constant: a different
/// store, a cross-region bucket, or a different permit count has a different
/// value, and every one of the fetch-layer thresholds below is derived from it
/// so that recalibrating the store recalibrates all of them at once.
/// [`BlockRangeFetcher::with_request_cost_bytes`] overrides it.
pub const DEFAULT_LOG_REQUEST_COST_BYTES: u64 = 1_887_437; // ~1.8 MiB

/// Floor on the coalescing gap, under the request-cost-derived default. Two
/// wanted extents separated by less than the effective gap fuse into one GET.
/// The principled value is [`DEFAULT_LOG_REQUEST_COST_BYTES`]: it is never worth
/// a second request to skip a hole whose bytes cost less to transfer than the
/// request would cost, so the effective gap is the request cost (much larger
/// than this floor). This 64 KiB floor (`crate::fetcher::DEFAULT_COALESCE_GAP`,
/// ADR-0107 decision 1: "start at RSEG's 64 KiB") applies only when a caller
/// drives the request cost below it.
pub const DEFAULT_LOG_COALESCE_GAP: u64 = 64 * 1024;

/// Multiple of the request cost at which a whole-object read breaks even against
/// the probe+ranged path. The ranged protocol adds on the order of this many
/// store round trips over a single whole-object GET (a probe, one coalesced
/// front-section GET, and a small number of block/chunk-run GETs -- ~5.46
/// GETs/object measured on q20), so it cannot save enough bytes to pay for
/// itself until the object exceeds this many request-costs. 5 x 1.8 MiB ~= 8.9
/// MiB reproduces q20's measured whole-object break-even.
pub const WHOLE_OBJECT_REQUEST_MULTIPLE: u64 = 5;

/// Floor on the size-threshold pre-probe crossover, under the
/// request-cost-derived default. An object at or below the effective threshold
/// is read whole in one GET instead of probing and range-fetching (ADR-0107
/// decision 1, "size-threshold, pre-probe whole-object read"). The principled
/// value is `WHOLE_OBJECT_REQUEST_MULTIPLE * request_cost`: below it the extra
/// round trips the ranged path adds cost more than the bytes they could save at
/// ANY selectivity. This 512 KiB floor (matching
/// `crate::fetcher::DEFAULT_WHOLE_OBJECT_THRESHOLD`) applies only when a caller
/// drives the request cost low enough that the derived threshold falls under it.
pub const DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD: u64 = 512 * 1024;

/// Default coverage fraction at or above which the post-pruning crossover falls
/// back to one whole-object GET (ADR-0107 decision 1, "coverage-based,
/// post-pruning fallback"). When the coalesced candidate ranges already cover
/// this much of the object, a single whole-object GET beats many range GETs plus
/// the probe. This crossover is new to ADR-0107 and is a decompose-time
/// measurement, not a claim about RSEG's behavior.
pub const DEFAULT_LOG_COVERAGE_THRESHOLD: f64 = 0.75;

/// Default bound on concurrent byte-range GETs one block-range fetch keeps in
/// flight. Sized independently from `crate::fetcher::SegmentFetcher`'s semaphore
/// (ADR-0107 decision 1: "sized independently for RLOG's call volume"); RSEG and
/// RLOG never share the permit pool.
pub const DEFAULT_LOG_MAX_CONCURRENT_GETS: usize = 16;

/// Upper bound on decoded SKIP_IDX block count, mirroring the reader's own
/// internal cap (`ravel_logseg::reader::MAX_BLOCKS`, not exported). A section
/// claiming more blocks is treated as corrupt rather than allocated.
const MAX_BLOCKS: u64 = 1 << 24;

/// Upper bound on decoded FIELD_DIR entry count, mirroring the reader's own
/// internal cap (`ravel_logseg::reader::MAX_FIELDS`, not exported). A section
/// claiming more entries is treated as corrupt rather than allocated.
const MAX_FIELDS: u64 = 1 << 20;

/// Per-object counters from one [`BlockRangeFetcher::fetch_object`] call, for
/// tests (the GET-count assertion of ADR-0107) and callers that want the
/// pruning-proportional figures. `block_range_gets`/`block_bytes_fetched` count
/// only store round trips for candidate blocks (cache hits excluded), which is
/// the pruning-proportional quantity; `probe_gets`/`metadata_gets` are the fixed
/// directory overhead the protocol adds on top.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockRangeStats {
    /// Store GETs establishing the etag and footer (the suffix probe, plus a
    /// footer-range chase if the suffix did not cover the whole footer).
    pub probe_gets: u64,
    /// Store GETs for non-BLOCKS directory sections not already covered by the
    /// probe (STREAM_DIR/FIELD_DIR at the object front, and any tail section a
    /// short probe missed).
    pub metadata_gets: u64,
    /// Store GETs for coalesced candidate-block ranges (cache misses only).
    pub block_range_gets: u64,
    /// Candidate blocks (page ranges, on a page-granular read) served from the
    /// read cache with no store round trip, counted exactly where the query
    /// accounting records a cache hit for them: at each block's peek on a
    /// block-range read, and at each page range's read-through lookup on a
    /// page-granular one. A block or page range whose lookup missed and that
    /// was then served with no GET of this call's own (another caller's
    /// flight, a RAM recheck, or `fetch_run`'s re-peek) stays a miss in the
    /// accounting and is counted in neither this nor
    /// [`block_range_gets`](Self::block_range_gets).
    pub block_cache_hits: u64,
    /// Stored bytes of candidate blocks read from the store (cache hits excluded).
    pub block_bytes_fetched: u64,
    /// Candidate blocks resolved from the skip index for this query.
    pub candidate_blocks: u64,
    /// Set when a crossover (size-threshold pre-probe, or coverage-based
    /// post-pruning) took the whole-object path instead of range fetches.
    pub whole_object: bool,
    /// Tail sections this read had to locate pages or blocks through that the
    /// suffix probe's WINDOW did not cover, so a report can show the residual
    /// miss rate the probe length leaves (issue #766). SKIP_IDX always counts;
    /// PAGE_DIR counts on a version-4 object. FIELD_DIR and STREAM_DIR never
    /// do: they sit at the object's front, where no suffix probe can reach them
    /// at any length, so counting them would put a floor under the metric and
    /// hide the quantity it exists to expose.
    ///
    /// Measured against the probe window, not against what the read cache
    /// happened to hold, so it is a property of `suffix_len` and the object's
    /// shape rather than of cache residency. A miss costs one extra GET, not
    /// one per section: the missed tail sections are adjacent in the object and
    /// are fetched as one coalesced range.
    pub probe_misses: u64,
}

/// The query phase a tail-section probe miss is charged to (#883).
///
/// Only two of [`QueryPhase`]'s four can issue a suffix probe: `Resolve` reads
/// commit records and `Probe` is the RSEG segment-catalog fetch, neither of
/// which touches an RLOG object's tail. Naming the two that can, rather than
/// taking a `QueryPhase` with two impossible arms, keeps the charge total: every
/// value here maps to a real phase, so a miss can never be recorded against a
/// phase that issued no request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbePhase {
    /// A probe issued to build a fetch plan: [`LogSegmentFetcher::plan_segment`]
    /// and [`LogSegmentFetcher::plan_segment_block_stats`], which read the
    /// footer, SKIP_IDX, and FIELD_DIR and no block byte.
    Plan,
    /// A probe issued by a data read: the block/page/chunk fetch behind
    /// [`LogSegmentFetcher::fetch_accounted_with_tenant`] and
    /// [`LogSegmentFetcher::scan_accounted_with_tenant`].
    Scan,
}

impl ProbePhase {
    /// This phase in [`QueryPhase`]'s vocabulary, which is what
    /// `docs/query-engine.md` and the per-phase request and byte counters use.
    #[must_use]
    pub fn query_phase(self) -> QueryPhase {
        match self {
            ProbePhase::Plan => QueryPhase::Plan,
            ProbePhase::Scan => QueryPhase::Scan,
        }
    }

    /// The [`ReadPhases`] a read issued on this phase's behalf charges its
    /// wire bytes to.
    #[must_use]
    pub fn read_phases(self) -> ReadPhases {
        match self {
            ProbePhase::Plan => ReadPhases::PLAN,
            ProbePhase::Scan => ReadPhases::SCAN,
        }
    }

    /// Lower-case, stable name for a report column or a JSON key, identical to
    /// [`QueryPhase::name`] for the same phase.
    #[must_use]
    pub fn name(self) -> &'static str {
        self.query_phase().name()
    }
}

/// Which [`QueryPhase`] each of one RLOG read's two GET kinds charges its WIRE
/// bytes to (issue #913).
///
/// A single read of one object issues two kinds of request and they answer
/// different questions. The metadata GETs -- the suffix probe, a footer chase,
/// SKIP_IDX, PAGE_DIR, the front directories, BLOOM/POSTINGS, and the object's
/// trailing bytes -- are what it costs to find out where the data is. The
/// BLOCKS-section GETs are the data. Charging both to one phase makes fetch
/// amplification unmeasurable: the numerator would carry directory bytes that
/// have nothing to do with how much of a block a projection wanted.
///
/// The split is on the request, not on the byte: a whole-object GET a data read
/// issued to obtain block data is charged in full to
/// [`Self::blocks`], directory bytes included, because those bytes are exactly
/// the amplification such a read pays. A planning read fetches no block data at
/// all, so both of its kinds are [`QueryPhase::Plan`] and its whole-object
/// fallback never lands in a scan figure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPhases {
    /// Phase for every GET that is not BLOCKS-section data.
    pub metadata: QueryPhase,
    /// Phase for BLOCKS-section data ranges, and for a whole-object GET issued
    /// to obtain block data.
    pub blocks: QueryPhase,
}

impl ReadPhases {
    /// A data read: its metadata GETs are [`QueryPhase::Probe`] and its block
    /// data is [`QueryPhase::Scan`]. The `scan` figure this produces is the
    /// numerator of fetch amplification.
    pub const SCAN: ReadPhases = ReadPhases {
        metadata: QueryPhase::Probe,
        blocks: QueryPhase::Scan,
    };

    /// A planning read ([`LogSegmentFetcher::plan_segment`] and
    /// [`LogSegmentFetcher::plan_segment_block_stats`]). It decodes no block, so
    /// everything it moves -- including its whole-object fallback -- is
    /// [`QueryPhase::Plan`].
    pub const PLAN: ReadPhases = ReadPhases {
        metadata: QueryPhase::Plan,
        blocks: QueryPhase::Plan,
    };
}

/// Per-phase tail-section probe misses ([`BlockRangeStats::probe_misses`])
/// accumulated across every read one [`LogSegmentFetcher`] served (#883).
///
/// `BlockRangeStats` reports one read's misses and is dropped at the fetch
/// boundary, so a caller that measures whole statements -- the SQL latency bench
/// -- never sees the figure: it reaches `ravel-sql` only as a `tracing` span
/// field, and nothing aggregates spans into a report. This is the same number on
/// a channel a caller can read, so a measurement pass can put probe misses
/// beside the request and byte counts they explain, which is what gates any
/// tightening of the probe floor ([`LOG_SUFFIX_FLOOR_BYTES`]).
///
/// It is an accumulator, not a per-query handle: the counters only ever grow, so
/// a caller measuring one execution takes a [`snapshot`](Self::snapshot) before
/// and after and reads [`ProbeMissCounts::saturating_sub`] of the two. Cheap to
/// clone (one `Arc`), and every clone of the owning `LogSegmentFetcher` shares
/// the same counters, exactly as its GET semaphore does.
#[derive(Debug, Clone, Default)]
pub struct ProbeMissCounter(Arc<ProbeMissInner>);

#[derive(Debug, Default)]
struct ProbeMissInner {
    plan: AtomicU64,
    scan: AtomicU64,
}

impl ProbeMissCounter {
    /// New counter with both phases at zero.
    #[must_use]
    pub fn new() -> Self {
        ProbeMissCounter::default()
    }

    /// Add one read's probe misses to `phase`. `n` is
    /// [`BlockRangeStats::probe_misses`] verbatim, including zero: a read that
    /// missed nothing must not be skipped, or the counter would only be written
    /// on the paths that already have a problem.
    fn record(&self, phase: ProbePhase, n: u64) {
        let cell = match phase {
            ProbePhase::Plan => &self.0.plan,
            ProbePhase::Scan => &self.0.scan,
        };
        cell.fetch_add(n, Ordering::Relaxed);
    }

    /// Point-in-time copy of both phases' totals.
    #[must_use]
    pub fn snapshot(&self) -> ProbeMissCounts {
        ProbeMissCounts {
            plan: self.0.plan.load(Ordering::Relaxed),
            scan: self.0.scan.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of a [`ProbeMissCounter`]'s per-phase totals (#883).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProbeMissCounts {
    /// Misses charged to [`ProbePhase::Plan`].
    pub plan: u64,
    /// Misses charged to [`ProbePhase::Scan`].
    pub scan: u64,
}

impl ProbeMissCounts {
    /// Field-wise difference `self - earlier`, the misses one measured
    /// execution added to a counter that keeps accumulating across executions.
    /// Saturating: a snapshot pair read out of order reports zero rather than
    /// wrapping to a huge count that would read as a catastrophic probe miss.
    #[must_use]
    pub fn saturating_sub(&self, earlier: &ProbeMissCounts) -> ProbeMissCounts {
        ProbeMissCounts {
            plan: self.plan.saturating_sub(earlier.plan),
            scan: self.scan.saturating_sub(earlier.scan),
        }
    }

    /// Misses across both phases.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.plan.saturating_add(self.scan)
    }

    /// One phase's total, named through [`ProbePhase`] rather than by field.
    #[must_use]
    pub fn phase(&self, phase: ProbePhase) -> u64 {
        match phase {
            ProbePhase::Plan => self.plan,
            ProbePhase::Scan => self.scan,
        }
    }
}

/// A [`footer::LogFooter`] a prior plan read produced for this exact (immutable)
/// object, carried into a scan read so it can skip its own suffix probe (#693
/// part 3), together with what that plan read already counted.
///
/// `tail_misses_counted` exists because the plan phase has TWO footer-carrying
/// reads and they differ in exactly that. [`BlockRangeFetcher::fetch_plan_sections`]
/// runs `ensure_tail_plan_sections`, which counts this object's tail-section
/// probe misses into the plan phase's own [`BlockRangeStats`];
/// [`BlockRangeFetcher::fetch_footer`] reads the footer alone and counts nothing
/// about SKIP_IDX or PAGE_DIR. A scan handed a bare footer cannot tell the two
/// apart, so it either counts the first object twice or drops the second
/// object's real miss (#883, issue #885 review).
#[derive(Clone, Copy, Debug)]
pub struct CarriedFooter<'a> {
    /// The footer the plan read parsed, valid for this exact object.
    pub footer: &'a footer::LogFooter,
    /// True when the plan read that produced [`Self::footer`] already counted
    /// this object's tail-section probe misses
    /// ([`BlockRangeStats::probe_misses`]).
    pub tail_misses_counted: bool,
}

/// What one per-partition subset open of a segment already knows about it
/// (ADR-2414 decision A1): the whole-object block indices its partition owns,
/// and, when the plan phase decoded them, the segment's directories.
///
/// `blocks` is ascending. With `dirs` the ranged read fetches and decodes none
/// of STREAM_DIR, FIELD_DIR, SKIP_IDX or PAGE_DIR: the reader built over the
/// result takes them from `dirs`.
#[derive(Clone, Copy)]
struct OwnedBlocks<'a> {
    blocks: &'a [usize],
    dirs: Option<&'a SegmentDirectories>,
}

/// The whole-object bytes [`LogSegmentFetcher::plan_segment`]'s whole-object
/// fallback already fetched for this exact (immutable) `SegmentRef`, carried
/// into the scan so it does not issue a second wire GET for the same object
/// (issue #835).
///
/// Reuse needs no etag re-check, unlike [`CarriedFooter`]'s scan read: the
/// plan and the scan of one statement share the very same `SegmentRef` out of
/// one resolved snapshot, and `LogSegmentFetcher`'s whole-object cache key is
/// keyed on `seg_ref.content_hash` (see [`EtagPin`]'s doc: "a cache key
/// carries the object's `content_hash`, so an entry is by construction bytes
/// of this exact content rather than of whatever the store holds now"). No
/// live GET spans the gap between the plan read and the scan read for this
/// carry to defend against -- there is no second live GET at all -- so the
/// same reasoning that exempts a cache hit from the pin applies here.
///
/// Always the ENTIRE object: the plan fallback only carries these bytes
/// forward when its own read resolved every block (the below-threshold
/// whole-object path, or an above-threshold read that crossed over to a
/// whole-object GET), never a column-selection-scoped ranged read. So the
/// bytes are valid for the scan's column selection whatever it is, even
/// though the plan fetched with [`ColumnSelection::all`].
///
/// The bytes travel with the identity of the read that produced them, and a
/// consumer proves the pairing before decoding. Nothing in the type system
/// stops a caller from handing this carry to a read of a different segment:
/// the carry branch answers from these bytes without consulting the supplied
/// `SegmentRef` at all, so a mismatched pairing would return the wrong
/// object's rows wherever both objects decode. The key alone would settle it
/// in practice (a data-object key embeds the tenant hex), but the tenant is
/// stored and checked too rather than inferred from that.
#[derive(Clone)]
pub struct CarriedWholeObject {
    bytes: LogObjectBytes,
    /// `data_object_key` of the segment the plan read fetched these bytes for.
    source_key: String,
    /// Tenant the plan read fetched them under.
    source_tenant: TenantHash,
}

impl CarriedWholeObject {
    /// The carried object's byte length, for a caller that needs to account
    /// for held-but-not-yet-consumed carry bytes (issue #835's memory bound)
    /// without decoding or copying them.
    pub fn byte_len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// One candidate block's absolute byte extent in the object and its stored crc,
/// resolved from a `SkipIndex` level-0 entry (`block_offset`/`block_len`/
/// `block_crc32c`). The extent start is absolute: `blocks_offset + block_offset`
/// (the same arithmetic `ravel_logseg::RlogRangeReader` uses).
#[derive(Clone, Copy, Debug)]
struct BlockExtent {
    abs_start: u64,
    len: u64,
    crc32c: u32,
}

impl BlockExtent {
    fn abs_end(&self) -> u64 {
        self.abs_start.saturating_add(self.len)
    }
}

/// One absolute byte extent of the object, with no checksum attached.
///
/// Version 4's fetch unit is a run of pages inside a column chunk (ADR-0699
/// decision 5), which carries no checksum of its own: the per-page `crc32c`
/// values live in PAGE_DIR and are verified at decode, one page at a time. So
/// the version-4 path ranges over these rather than over [`BlockExtent`], whose
/// `crc32c` a version-3 read verifies before the bytes are placed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ByteExtent {
    abs_start: u64,
    len: u64,
}

impl ByteExtent {
    fn abs_end(&self) -> u64 {
        self.abs_start.saturating_add(self.len)
    }
}

/// Figures for the bytes one [`BlockRangeFetcher`]'s assembled reads hold
/// ([`BlockRangeFetcher::assembly_buffer_stats`], issues #1771 and #2066),
/// shared by all its clones: a gauge and its high-water mark.
///
/// An assembled read is one built from more than one GET: a ranged read's
/// probe, directory sections and page runs, or a covering read segmented at
/// the fetch bound. It holds exactly the regions it placed, as fetched, so the
/// gauge is the summed length of every placed region still held by an
/// [`ObjectAssembler`] or by the [`LogObjectBytes`] it produced, and it falls
/// when the reader drops them. Nothing is pooled or retained between reads, so
/// the object-sized buffer pool this used to report on and its `allocated`,
/// `reused` and `zeroed_bytes` counters are retired (issue #2066).
///
/// Zero while every object is read whole in one GET: under a resolved request
/// cost that saturates the routing threshold, that is every object at or under
/// the fetch bound, and a larger one is read as segmented covering sub-ranges
/// that charge this gauge. The saturated case includes `cost-based` at the
/// reference `s3-intra-region-2026` profile, which prices transfer and
/// retrieval at zero; the same policy on an egress-billed deployment resolves
/// a finite rate, routes objects above the threshold through the ranged path,
/// and charges this gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AssemblyBufferStats {
    /// Bytes of placed regions currently held by assembled reads.
    pub live_bytes: u64,
    /// High-water mark of [`Self::live_bytes`] over this fetcher's life.
    pub peak_live_bytes: u64,
}

/// The [`AssemblyBufferStats`] gauge, shared by every clone of one fetcher.
#[derive(Debug, Default)]
struct AssemblyGauge {
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
}

impl AssemblyGauge {
    fn charge(&self, n: u64) {
        let live = self.live_bytes.fetch_add(n, Ordering::Relaxed) + n;
        // Raise the high-water mark, retrying only while another thread's
        // observed peak is lower than ours.
        let mut seen = self.peak_live_bytes.load(Ordering::Relaxed);
        while seen < live {
            match self.peak_live_bytes.compare_exchange_weak(
                seen,
                live,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => seen = actual,
            }
        }
    }

    fn release(&self, n: u64) {
        self.live_bytes.fetch_sub(n, Ordering::Relaxed);
    }

    fn stats(&self) -> AssemblyBufferStats {
        AssemblyBufferStats {
            live_bytes: self.live_bytes.load(Ordering::Relaxed),
            peak_live_bytes: self.peak_live_bytes.load(Ordering::Relaxed),
        }
    }
}

/// The regions an assembled read placed, the fetch-layer reservations that
/// cover them (ADR-1170 decision 2), and the gauge they are charged to. The
/// reservations and the gauge charge release together, when the last holder
/// drops this: the [`ObjectAssembler`] on an error or crossover path, else the
/// last clone of the [`LogObjectBytes`] handed to the reader.
struct PlacedObject {
    regions: SparseObject,
    reservations: Vec<ravel_memory::Reservation>,
    gauge: Arc<AssemblyGauge>,
}

impl Drop for PlacedObject {
    fn drop(&mut self) {
        self.gauge.release(self.regions.placed_len());
    }
}

/// One RLOG object's bytes as a read holds them (issue #2066): the whole object
/// from a single GET, a cache entry or a carry, or the regions an assembled
/// read placed. Either way a [`ByteSource`] the reader opens with
/// [`RlogReader::from_source`] and decodes blocks from, addressed at the
/// object's absolute offsets. Cloning shares the held bytes.
///
/// A placed object holds only its regions, so a read of any range no region
/// holds fails with [`LogSegError::Unplaced`] instead of returning bytes.
#[derive(Clone)]
pub struct LogObjectBytes(ObjectRepr);

#[derive(Clone)]
enum ObjectRepr {
    Whole(Bytes),
    Placed(Arc<PlacedObject>),
}

impl LogObjectBytes {
    /// The object's length in bytes, placed or not.
    pub fn len(&self) -> usize {
        match &self.0 {
            ObjectRepr::Whole(bytes) => bytes.len(),
            ObjectRepr::Placed(placed) => {
                usize::try_from(placed.regions.object_len()).unwrap_or(usize::MAX)
            }
        }
    }

    /// Whether the object is zero bytes long.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes this value keeps alive: the whole length for a whole object,
    /// the summed placed regions for an assembled one.
    pub fn held_len(&self) -> u64 {
        match &self.0 {
            ObjectRepr::Whole(bytes) => bytes.len() as u64,
            ObjectRepr::Placed(placed) => placed.regions.placed_len(),
        }
    }

    /// The whole object's contiguous bytes, or `None` for an assembled read.
    pub fn as_whole(&self) -> Option<&Bytes> {
        match &self.0 {
            ObjectRepr::Whole(bytes) => Some(bytes),
            ObjectRepr::Placed(_) => None,
        }
    }
}

impl std::fmt::Debug for LogObjectBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogObjectBytes")
            .field("whole", &self.as_whole().is_some())
            .field("object_len", &self.object_len())
            .field("held_len", &self.held_len())
            .finish()
    }
}

impl From<Bytes> for LogObjectBytes {
    fn from(bytes: Bytes) -> Self {
        LogObjectBytes(ObjectRepr::Whole(bytes))
    }
}

impl ByteSource for LogObjectBytes {
    fn object_len(&self) -> u64 {
        match &self.0 {
            ObjectRepr::Whole(bytes) => bytes.object_len(),
            ObjectRepr::Placed(placed) => placed.regions.object_len(),
        }
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        match &self.0 {
            ObjectRepr::Whole(bytes) => bytes.read(start, len),
            ObjectRepr::Placed(placed) => placed.regions.read(start, len),
        }
    }
}

/// Assembles an object from separately fetched regions without an
/// object-sized buffer (issue #2066): each [`place`](Self::place) keeps the
/// [`Bytes`] it is given, uncopied, at their absolute offset, and
/// [`into_bytes`](Self::into_bytes) hands the reader a [`LogObjectBytes`] over
/// exactly those regions. The reader addresses the object at the offsets its
/// footer and directories name, reading only the directory sections and the
/// pages of blocks the fetch placed; a read of anything else (a pruned block's
/// pages, the gaps between runs) fails typed with [`LogSegError::Unplaced`]
/// rather than decoding bytes that were never fetched (ADR-0107: gap bytes
/// "never interpreted, never verified").
///
/// Memory: the assembler holds the placed bytes and nothing else, and every
/// placement is covered by a fetch-layer reservation (ADR-1170 decision 2)
/// its caller took before the GET that fetched it (or, for a cache hit, before
/// placing it) and handed over with [`hold`](Self::hold). Both release when
/// the reader drops the bytes.
struct ObjectAssembler {
    placed: PlacedObject,
}

impl ObjectAssembler {
    fn new(gauge: &Arc<AssemblyGauge>, total_size: u64) -> Self {
        ObjectAssembler {
            placed: PlacedObject {
                regions: SparseObject::new(total_size),
                reservations: Vec::new(),
                gauge: Arc::clone(gauge),
            },
        }
    }

    /// Whether one earlier [`place`](Self::place) covered all of
    /// `[start, end)`.
    fn covers(&self, start: u64, end: u64) -> bool {
        self.placed.regions.holds_in_one_region(start, end)
    }

    /// The stored bytes at `[start, start + len)`, or a typed error when the
    /// placed regions do not hold every byte of it.
    fn slice(&self, key: &str, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogFetchError> {
        self.placed
            .regions
            .read(start, len)
            .map_err(|source| corrupt(key, source))
    }

    /// Keeps `bytes` as the object's bytes at `start`, zero-copy.
    fn place(&mut self, key: &str, start: u64, bytes: Bytes) -> Result<(), LogFetchError> {
        let len = bytes.len() as u64;
        self.placed
            .regions
            .place(start, bytes)
            .map_err(|_| corrupt_range(key))?;
        self.placed.gauge.charge(len);
        Ok(())
    }

    /// Keeps `reservation` for as long as the placed bytes are held.
    fn hold(&mut self, reservation: ravel_memory::Reservation) {
        self.placed.reservations.push(reservation);
    }

    /// The fetch-layer bytes this assembler holds reserved.
    #[cfg(test)]
    fn reserved(&self) -> u64 {
        self.placed.reservations.iter().map(|r| r.size()).sum()
    }

    fn into_bytes(self) -> LogObjectBytes {
        LogObjectBytes(ObjectRepr::Placed(Arc::new(self.placed)))
    }
}

/// `[offset, offset + len)` sliced out of whichever already-fetched region
/// wholly contains it, or `None` when no region does. The regions are the
/// `(start, bytes)` pairs [`BlockRangeFetcher::probe_footer`] left resident;
/// this is [`ObjectAssembler::covers`] + [`ObjectAssembler::slice`] for a read
/// that keeps only those regions.
fn resident_slice(regions: &[(u64, Bytes)], offset: u64, len: u64) -> Option<Bytes> {
    let end = offset.checked_add(len)?;
    regions.iter().find_map(|(start, bytes)| {
        let region_end = start.checked_add(bytes.len() as u64)?;
        if *start > offset || end > region_end {
            return None;
        }
        let rel = usize::try_from(offset - start).ok()?;
        let rel_end = rel.checked_add(usize::try_from(len).ok()?)?;
        Some(bytes.slice(rel..rel_end))
    })
}

fn corrupt_range(key: &str) -> LogFetchError {
    LogFetchError::Corrupt {
        key: key.to_string(),
        source: LogSegError::Corrupted("block-range assembly out of bounds".into()),
    }
}

/// The RLOG-specific coalescing block-range fetcher (ADR-0107). It fetches only
/// the blocks skip-index pruning proved relevant instead of one whole-object GET
/// per segment, mirroring [`crate::SegmentFetcher`]'s protocol -- gap
/// coalescing, whole-object crossover(s), etag pinning, and a bounded GET
/// semaphore -- as its own implementation rather than a shared abstraction (RSEG
/// and RLOG object layouts differ enough that a shared type would need leaky
/// per-format branches; the "RSEG and RLOG never share fetch code" convention in
/// this module's header stays intact).
///
/// The result is a [`LogObjectBytes`] holding only the directory sections and
/// candidate blocks' bytes, which [`RlogReader::from_source`]/[`BlockScan`]
/// read at the object's absolute offsets: the reader re-prunes and decodes
/// exactly the survivor blocks, which are a subset of the fetched candidate
/// set, and a read of an unfetched gap fails typed (`LogSegError::Unplaced`). Cache admission is per block, not per coalesced GET (ADR-0107
/// decision 3): after a live range GET, the response is split at block
/// boundaries, each block's `block_crc32c` is verified independently, and one
/// cache entry per block is admitted keyed `(tenant_hash, content_hash,
/// abs_start, block_len)`; the gap bytes between blocks are discarded.
#[derive(Clone)]
pub struct BlockRangeFetcher {
    store: Arc<dyn ObjectStoreBackend>,
    cfg: RlogConfig,
    /// ADR-0046's read cache, consulted per block (decision 3) and per directory
    /// section. `None` sends every GET to the store. Either tier configuration
    /// (see [`ReadCache`]); every production caller builds the RAM variant.
    cache: Option<ReadCache>,
    /// Suffix-probe length. `None` derives it per object from the object size
    /// ([`derive_suffix_len`], issue #883); `Some(n)` pins it, which the tests
    /// use to force an exact probe window. See [`Self::effective_suffix_len`].
    suffix_len: Option<u64>,
    /// Coalescing gap. `None` derives it from `request_cost_bytes` (the
    /// principled default); `Some(n)` pins it, which the tests use to force an
    /// exact run count. See [`Self::effective_coalesce_gap`].
    coalesce_gap: Option<u64>,
    /// Size-threshold pre-probe crossover. `None` derives it from
    /// `request_cost_bytes`; `Some(n)` pins it (and `Some(0)` forces the ranged
    /// path). See [`Self::effective_whole_object_threshold`].
    whole_object_threshold: Option<u64>,
    coverage_threshold: f64,
    /// Cost of one store request as a byte volume (a latency-bandwidth product);
    /// the single quantity every range-vs-whole-object decision here is driven
    /// from ([`DEFAULT_LOG_REQUEST_COST_BYTES`]). A property of the store and
    /// instance, so it is a tunable, not a constant.
    request_cost_bytes: u64,
    /// The fetch bound (ADR-0996 decision 2): one covering GET's maximum length.
    /// An object at or under it is read in one covering `GetRange::Full`; a
    /// larger one is read as `ceil(object_size / max_fetch_run_bytes)`
    /// sequential covering sub-range GETs, each at most the bound, so no single
    /// request moves more than this. Decoupled from `request_cost_bytes`: the
    /// rate decides WHICH read shape, the bound only segments HOW a chosen
    /// covering read is laid out. [`DEFAULT_LOG_MAX_FETCH_RUN_BYTES`] by default;
    /// zero is refused before it reaches here ([`EngineConfig::validate`]).
    max_fetch_run_bytes: u64,
    /// The largest single covering sub-range GET this fetcher (and every clone)
    /// has issued, in bytes: the peak wire size of one request on the segmented
    /// covering path, which never exceeds `max_fetch_run_bytes`. The resident
    /// bytes are NOT bounded by this -- see the resident-memory note on
    /// [`Self::covering_read`] (issue #1007): this field only ever reports the
    /// wire side.
    peak_fetch_run: Arc<AtomicU64>,
    /// Bounds in-flight byte-range GETs. By default its own private instance
    /// (ADR-0107 decision 1); [`Self::with_get_limiter`] wires it to the one
    /// process-shared limiter every query-side fetcher can hold instead
    /// (ADR-1195).
    get_limiter: Arc<crate::GetLimiter>,
    /// The bytes this fetcher's assembled reads hold ([`AssemblyBufferStats`]),
    /// shared by every clone the way `get_limiter` is.
    assembly_gauge: Arc<AssemblyGauge>,
    /// Per-phase WIRE bytes across every read this fetcher has served (#913).
    /// Written at [`Self::store_get`], the single GET funnel here, beside the
    /// `QueryAccounting` write of the same bytes. Shared by every clone, and by
    /// the owning [`LogSegmentFetcher`], which sets its own counter here so one
    /// execution's whole-object and ranged reads land in the same totals.
    wire_bytes: PhaseWireByteCounter,
    /// The process-wide fetch memory budget (ADR-1170 decision 2). Every ranged
    /// read reserves against it before each GET the length that GET places,
    /// held by the [`ObjectAssembler`] and then the returned [`LogObjectBytes`]
    /// for as long as the placed bytes live. Default unlimited (never
    /// refuses); [`Self::with_memory_budget`] wires the shared one.
    memory_budget: Arc<ravel_memory::MemoryBudget>,
    /// The read CPU gate the directory section decodes run on. `None`, the
    /// default, decodes inline.
    read_gate: Option<Arc<ReadGate>>,
}

impl BlockRangeFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        BlockRangeFetcher {
            store,
            cfg: RlogConfig::default(),
            cache: None,
            suffix_len: None,
            coalesce_gap: None,
            whole_object_threshold: None,
            coverage_threshold: DEFAULT_LOG_COVERAGE_THRESHOLD,
            request_cost_bytes: DEFAULT_LOG_REQUEST_COST_BYTES,
            max_fetch_run_bytes: crate::DEFAULT_LOG_MAX_FETCH_RUN_BYTES,
            peak_fetch_run: Arc::new(AtomicU64::new(0)),
            get_limiter: Arc::new(crate::GetLimiter::new_unchecked(
                DEFAULT_LOG_MAX_CONCURRENT_GETS,
            )),
            assembly_gauge: Arc::new(AssemblyGauge::default()),
            wire_bytes: PhaseWireByteCounter::new(),
            memory_budget: Arc::new(ravel_memory::MemoryBudget::unlimited()),
            read_gate: None,
        }
    }

    /// Wires this fetcher's ranged reads to a caller-owned
    /// [`ravel_memory::MemoryBudget`] (ADR-1170 decision 2), so they reserve
    /// against the same budget as the owning [`LogSegmentFetcher`]'s
    /// whole-object funnel and every other fetcher holding the same `Arc`.
    #[must_use]
    pub fn with_memory_budget(mut self, budget: Arc<ravel_memory::MemoryBudget>) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Runs this fetcher's directory section decodes on `gate` (ADR-1702
    /// decision 4), one `log_section` job per section. The owning
    /// [`LogSegmentFetcher::with_read_gate`] sets it.
    #[must_use]
    pub fn with_read_gate(mut self, gate: Arc<ReadGate>) -> Self {
        self.read_gate = Some(gate);
        self
    }

    /// One RLOG directory section decode, on the read gate as one
    /// `log_section` job sized by the section's uncompressed length when a
    /// gate is set, inline otherwise. The gated path copies the stored bytes
    /// into the job.
    async fn decode_section_on_gate(
        &self,
        key: &str,
        stored: &[u8],
        desc: &SectionDesc,
        accounting: &QueryAccounting,
    ) -> Result<Vec<u8>, LogFetchError> {
        let Some(gate) = &self.read_gate else {
            return decode_section_accounted(stored, desc, &self.cfg, accounting)
                .map_err(|source| corrupt(key, source));
        };
        let stored = Bytes::copy_from_slice(stored);
        let (desc, cfg, accounting) = (*desc, self.cfg, accounting.clone());
        gate.run(
            ReadSite::LogSection,
            JobSize::Bytes(desc.uncomp_len),
            move || decode_section_accounted(&stored, &desc, &cfg, &accounting),
        )
        .await
        .map_err(|err| log_gate_failed(key, err))?
        .map_err(|source| corrupt(key, source))
    }

    /// This fetcher's current memory budget, for a test to `Arc::ptr_eq` the
    /// way [`Self::get_limiter_for_test`] serves the limiter.
    #[cfg(test)]
    pub(crate) fn memory_budget_for_test(&self) -> &Arc<ravel_memory::MemoryBudget> {
        &self.memory_budget
    }

    /// Reserves `n` bytes against this fetcher's budget before a ranged read,
    /// mapping a refusal to [`LogFetchError::FetchMemoryExhausted`]. The guard
    /// is owned for the covered buffer's lifetime (ADR-1170 decision 2).
    fn reserve_fetch(&self, n: u64) -> Result<ravel_memory::Reservation, LogFetchError> {
        self.memory_budget
            .reserve(n)
            .map_err(|e| LogFetchError::FetchMemoryExhausted {
                requested: e.requested,
                reserved: e.reserved,
                limit: e.limit,
            })
    }

    /// Records this fetcher's WIRE bytes into `counter` instead of its own
    /// (#913). [`LogSegmentFetcher`] calls this so its direct whole-object GETs
    /// and this fetcher's ranged GETs accumulate into one counter a measurement
    /// pass can read.
    #[must_use]
    pub fn with_wire_byte_counter(mut self, counter: PhaseWireByteCounter) -> Self {
        self.wire_bytes = counter;
        self
    }

    /// This fetcher's accumulating per-phase WIRE byte counter (#913). Cloned
    /// out before the fetcher is handed away, then read with a
    /// [`PhaseWireByteCounter::snapshot`] on each side of an execution.
    #[must_use]
    pub fn phase_wire_byte_counter(&self) -> PhaseWireByteCounter {
        self.wire_bytes.clone()
    }

    /// The bytes this fetcher's assembled reads hold now, and the most they
    /// have held at once ([`AssemblyBufferStats`], issues #1771 and #2066).
    /// Shared by every clone.
    #[must_use]
    pub fn assembly_buffer_stats(&self) -> AssemblyBufferStats {
        self.assembly_gauge.stats()
    }

    #[must_use]
    pub fn with_config(mut self, cfg: RlogConfig) -> Self {
        self.cfg = cfg;
        self
    }

    #[must_use]
    pub fn with_cache(mut self, cache: impl Into<ReadCache>) -> Self {
        self.cache = Some(cache.into());
        self
    }

    /// Pins the suffix-probe length explicitly, overriding the per-object
    /// derivation ([`derive_suffix_len`], issue #883). The tests use this to
    /// force an exact probe window; production callers leave it unset so each
    /// object is probed proportionally to its size.
    #[must_use]
    pub fn with_suffix_len(mut self, n: u64) -> Self {
        self.suffix_len = Some(n.max(1));
        self
    }

    /// The suffix-probe length in effect for an object of `total_size` bytes: an
    /// explicit [`Self::with_suffix_len`] override when set, else the per-object
    /// derivation [`derive_suffix_len`] (issue #883). Capped to `total_size` (a
    /// probe cannot read more than the object) and floored at 1 (a zero-length
    /// suffix is not a valid GET; a zero-size object is handled before any
    /// probe). This is the single place every probe site computes its window, so
    /// the derivation, the [`BlockRangeStats::probe_misses`] window check, and
    /// the cache key all agree on one length per object.
    fn effective_suffix_len(&self, total_size: u64) -> u64 {
        let want = self
            .suffix_len
            .unwrap_or_else(|| derive_suffix_len(total_size));
        want.min(total_size).max(1)
    }

    #[must_use]
    pub fn with_coalesce_gap(mut self, n: u64) -> Self {
        self.coalesce_gap = Some(n);
        self
    }

    /// Sets the size-threshold pre-probe crossover explicitly, overriding the
    /// request-cost-derived default. An object whose size is at or below `n` is
    /// read whole in one GET; `0` disables the size crossover (every object takes
    /// the probe + range path), which the tests use to force the ranged path on a
    /// small fixture.
    #[must_use]
    pub fn with_whole_object_threshold(mut self, n: u64) -> Self {
        self.whole_object_threshold = Some(n);
        self
    }

    /// Sets the cost of one store request as a byte volume (a latency-bandwidth
    /// product; see [`DEFAULT_LOG_REQUEST_COST_BYTES`]). This drives the
    /// whole-object crossover and the coalescing gap whenever those are left at
    /// their defaults, so recalibrating the store (a faster tier, a cross-region
    /// bucket, a different fetch concurrency) recalibrates every fetch-layer
    /// range-vs-whole-object decision through this one knob.
    #[must_use]
    pub fn with_request_cost_bytes(mut self, n: u64) -> Self {
        self.request_cost_bytes = n;
        self
    }

    /// Sets the fetch bound (ADR-0996 decision 2): one covering GET's maximum
    /// length. An object at or under it is one covering GET; a larger one is
    /// segmented into `ceil(object_size / n)` covering sub-range GETs.
    ///
    /// Zero is REFUSED, with the same typed error
    /// [`EngineConfig::validate`](crate::EngineConfig::validate) returns. The two
    /// checks are not redundant: `validate` guards the config surface, and this
    /// guards every other way a bound reaches the fetcher (a direct builder call
    /// in a test or an embedding crate, a value that never passed through an
    /// `EngineConfig` at all). Clamping to 1 here instead would turn a
    /// misconfigured bound into a silent one-byte-per-GET read of every object,
    /// which is a far worse outcome than the refusal and is invisible in the
    /// counters until the request bill arrives.
    pub fn with_max_fetch_run_bytes(mut self, n: u64) -> Result<Self, EngineConfigError> {
        if n == 0 {
            return Err(EngineConfigError::ZeroFetchBound);
        }
        self.max_fetch_run_bytes = n;
        Ok(self)
    }

    /// The largest single covering sub-range GET this fetcher has ISSUED, in
    /// bytes (ADR-0996 decision 2). Never exceeds `max_fetch_run_bytes`; a test
    /// reads it to prove the segmented covering path bounded each request's wire
    /// size. Cumulative and shared by every clone; nothing resets it.
    ///
    /// A read served from the cache issues no request and moves no wire bytes,
    /// so it never enters this figure: the observation is taken after
    /// [`Self::cached_extent`] reports the miss, not before it.
    #[must_use]
    pub fn peak_fetch_run_bytes(&self) -> u64 {
        self.peak_fetch_run.load(Ordering::Relaxed)
    }

    /// The fetch bound in effect ([`Self::with_max_fetch_run_bytes`], ADR-0996
    /// decision 2). Read by [`LogSegmentFetcher::whole_object_bytes`] to route an
    /// above-bound object through the segmented covering read.
    #[must_use]
    pub fn max_fetch_run_bytes(&self) -> u64 {
        self.max_fetch_run_bytes
    }

    /// Record one ISSUED covering sub-range GET's length against the peak,
    /// keeping the running maximum. Called on the covering path once the read is
    /// known to have crossed the network, so
    /// [`Self::peak_fetch_run_bytes`] reflects the largest single request rather
    /// than the largest extent a cache happened to serve.
    fn observe_fetch_run(&self, len: u64) {
        self.peak_fetch_run.fetch_max(len, Ordering::Relaxed);
    }

    /// A covering read of `[0, total_size)` bounded by
    /// [`Self::max_fetch_run_bytes`] (ADR-0996 decision 2's covering-read
    /// contract).
    ///
    /// - `total_size <= max_fetch_run_bytes`: ONE covering GET. `full_range` is
    ///   the range the single read uses (`GetRange::Full` at a whole-object
    ///   crossover, so it keys `(0, object_size)` and single-flights with the
    ///   other whole-object funnels).
    /// - `total_size > max_fetch_run_bytes`: `ceil(total_size /
    ///   max_fetch_run_bytes)` sequential covering sub-range GETs, each at most
    ///   the bound, each kept as its own placed region of the returned
    ///   [`LogObjectBytes`]. Bounds each request's wire size; the request count
    ///   is `ceil(total_size / bound)` (the ADR's floor, and its cap too on this
    ///   unaligned split, since no run is short of the bound except the last).
    ///
    /// Resident memory is still the whole object on both branches (issue
    /// #1007): a covering read hands the reader every byte before the first
    /// block decodes. Bounding it to a window needs the decode to run per
    /// sub-range, split at block boundaries, which this does not do.
    /// [`Self::peak_fetch_run_bytes`] bounds only the request wire size.
    #[allow(clippy::too_many_arguments)]
    async fn covering_read(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        total_size: u64,
        full_range: GetRange,
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, u64, u64), LogFetchError> {
        // Reserve the covering read's whole extent before any GET (ADR-1170
        // decision 2): a refusal fails typed with zero GETs. The guard travels
        // with the returned bytes -- attached directly in the single-GET
        // branch, or carried by the `ObjectAssembler` in the segmented branch --
        // so it releases when the reader drops them.
        //
        // This always reserves fresh, and a coverage crossover holding a live
        // assembler for the same object drops it first rather than handing its
        // guard across. Accepting a caller's live reservation would mean
        // proving, at every such call site, that no borrow into the assembler's
        // buffer survives the await, and a caller that got that wrong would
        // leave the buffer resident under no ledger, which is the gap decision 2
        // forbids. The cost of reserving fresh is that a saturated budget can
        // hand the freed extent to another task in between, turning a fetch that
        // a handover would have completed into a typed refusal. That is
        // fail-closed and allowed.
        let mut reservation = self.reserve_fetch(total_size)?;
        if total_size <= self.max_fetch_run_bytes {
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    0,
                    total_size,
                    full_range,
                    phase,
                    pin,
                    accounting,
                )
                .await?;
            if live {
                self.observe_fetch_run(total_size);
                if self.cache.is_some() {
                    // Cache miss that just admitted these bytes: `cached_extent`'s
                    // leader-miss insert put them under the cache's own ledger too,
                    // the same overlap the whole-object funnel's `ReadOutcome::Fetched`
                    // arm marks. `live` alone cannot distinguish this from an
                    // uncached direct GET (`cached_extent` reports `live = true`
                    // for both), so the cache-configured check decides it here.
                    reservation.mark_handed_off();
                }
            } else {
                // Cache hit or late serve: `bytes` clones the cache entry's allocation, so
                // the cache cap and this guard both cover it for as long as
                // the caller holds it (ADR-1170 decision 2), same as the
                // whole-object funnel's hit arm.
                reservation.mark_handed_off();
            }
            let live_bytes = if live { total_size } else { 0 };
            return Ok((
                attach_reservation(bytes, reservation).into(),
                u64::from(live),
                live_bytes,
            ));
        }

        // Segmented: every sub-range is kept as fetched rather than copied into
        // one object-sized buffer, so the object is held once, not once plus
        // the sub-range in flight. With a cache wired each sub-range is also a
        // cache entry (a hit, or `cached_extent`'s miss-path insert), the same
        // overlap the single-GET branch marks.
        let key = seg_ref.data_object_key.as_str();
        if self.cache.is_some() {
            reservation.mark_handed_off();
        }
        let mut asm = ObjectAssembler::new(&self.assembly_gauge, total_size);
        asm.hold(reservation);
        let mut live_gets = 0u64;
        // Wire bytes actually moved: a sub-range served from cache moves none,
        // so `bytes.len()` (the whole object) must never be charged as fetched
        // on a partially cached covering read.
        let mut live_bytes = 0u64;
        let mut start = 0u64;
        while start < total_size {
            let len = self.max_fetch_run_bytes.min(total_size - start);
            let end = start + len;
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    start,
                    len,
                    GetRange::Range(start, end),
                    phase,
                    pin,
                    accounting,
                )
                .await?;
            if live {
                self.observe_fetch_run(len);
            }
            asm.place(key, start, bytes)?;
            live_gets += u64::from(live);
            if live {
                live_bytes += len;
            }
            start = end;
        }
        Ok((asm.into_bytes(), live_gets, live_bytes))
    }

    /// The coalescing gap in effect: an explicit [`Self::with_coalesce_gap`]
    /// override when set, else the request cost (floored by
    /// [`DEFAULT_LOG_COALESCE_GAP`]). It is never worth a second request to skip
    /// a hole whose bytes transfer for less than one request costs, so the
    /// principled gap is exactly one request cost.
    fn effective_coalesce_gap(&self) -> u64 {
        self.coalesce_gap
            .unwrap_or_else(|| self.request_cost_bytes.max(DEFAULT_LOG_COALESCE_GAP))
    }

    /// The size-threshold pre-probe crossover in effect: an explicit
    /// [`Self::with_whole_object_threshold`] override when set (including the `0`
    /// that forces the ranged path), else `WHOLE_OBJECT_REQUEST_MULTIPLE`
    /// request-costs (floored by [`DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`]). Below
    /// the break-even the ranged path's extra round trips cost more than the
    /// bytes they could save at any selectivity, so the whole-object read wins.
    fn effective_whole_object_threshold(&self) -> u64 {
        self.whole_object_threshold.unwrap_or_else(|| {
            self.request_cost_bytes
                .saturating_mul(WHOLE_OBJECT_REQUEST_MULTIPLE)
                .max(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD)
        })
    }

    /// Sets the coverage-based post-pruning crossover fraction (0.0..=1.0). When
    /// the coalesced candidate ranges cover at least this fraction of the object,
    /// one whole-object GET is issued instead. A value `> 1.0` disables it.
    #[must_use]
    pub fn with_coverage_threshold(mut self, f: f64) -> Self {
        self.coverage_threshold = f;
        self
    }

    #[must_use]
    pub fn with_max_concurrent_gets(mut self, n: usize) -> Self {
        self.get_limiter = Arc::new(crate::GetLimiter::new_unchecked(n.max(1)));
        self
    }

    /// Wires this fetcher to a caller-owned [`crate::GetLimiter`] (ADR-1195),
    /// so it draws GET permits from the same pool as every other fetcher (and,
    /// via [`crate::QueryEngine::with_get_limiter`], every other engine)
    /// holding the same `Arc`.
    #[must_use]
    pub fn with_get_limiter(mut self, limiter: Arc<crate::GetLimiter>) -> Self {
        self.get_limiter = limiter;
        self
    }

    /// This fetcher's current limiter, for a test to `Arc::ptr_eq` against
    /// another fetcher's or an engine's.
    #[cfg(test)]
    pub(crate) fn get_limiter_for_test(&self) -> &Arc<crate::GetLimiter> {
        &self.get_limiter
    }

    /// Whether ADR-0046's read cache is wired into this fetcher. Every GET the
    /// block-range protocol issues is routed through the cache's single-flight
    /// when it is, which is what makes several partitions striping one segment
    /// collapse onto one real GET (ADR-0102 decision 1's premise).
    #[must_use]
    pub fn has_cache(&self) -> bool {
        self.cache.is_some()
    }

    /// One bounded store GET, recorded against `accounting`. A `NotFound` (or any
    /// other store error) surfaces as [`LogFetchError::Store`], the SAME typed
    /// error a whole-object GET already produces, so a `NotFound` on a pinned
    /// segment maps to the existing `SnapshotInvalidated` retry path
    /// (ADR-0107 decision 1; `ravel_sql::SqlError::is_segment_not_found`) without
    /// a second mapping.
    ///
    /// `phase` is the [`QueryPhase`] this request's WIRE bytes are charged to
    /// on [`Self::phase_wire_byte_counter`] (#913). Every GET this fetcher
    /// issues passes through here, so the per-phase totals and the
    /// `QueryAccounting` GET total are written from one place and cannot drift.
    async fn store_get(
        &self,
        key: &str,
        range: GetRange,
        phase: QueryPhase,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, LogFetchError> {
        let _permit = self
            .get_limiter
            .acquire()
            .await
            .map_err(|_| LogFetchError::Store {
                key: key.to_string(),
                source: StoreError::Transient(
                    "GetLimiter semaphore closed unexpectedly".to_string(),
                ),
            })?;
        let got = self
            .store
            .get(key, range)
            .await
            .map_err(|source| LogFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        accounting.record_s3_request(AccountedOp::Get);
        accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        self.wire_bytes.record(phase, got.data.len() as u64);
        Ok(got)
    }

    /// A store GET whose etag is checked against the sequence's pinned etag: a
    /// mismatch means the object was replaced mid-sequence and is a hard
    /// [`LogFetchError::EtagChanged`] (ADR-0107 decision 1), never silently
    /// mixed data. The first live GET of a sequence establishes the pin (see
    /// [`EtagPin`]).
    async fn store_get_pinned(
        &self,
        key: &str,
        range: GetRange,
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, LogFetchError> {
        let got = self.store_get(key, range, phase, accounting).await?;
        pin.check(key, &got.etag)?;
        Ok(got)
    }

    /// One absolute `[start, start + len)` extent of the object, served from
    /// ADR-0046's read cache when it is resident and otherwise fetched with one
    /// live etag-pinned GET that concurrent callers for the same extent collapse
    /// onto through the cache's single-flight, exactly as the whole-object funnel
    /// in [`LogSegmentFetcher::tenant_bytes`] does for its one key. Without a
    /// cache every call is a live GET, as before.
    ///
    /// This is what keeps ADR-0102 decision 1's premise true above the
    /// block-range threshold: the partitions striping one segment resolve the
    /// same extents and coalesce onto one real request each instead of one per
    /// partition.
    ///
    /// `range` is passed alongside `[start, len)` rather than derived from it
    /// because the etag-establishing probe must stay a [`GetRange::Suffix`]
    /// (a `Range` GET of the same bytes is a different request to the store)
    /// while still keying as the absolute extent it returns.
    ///
    /// The returned flag is true only when this call's own GET crossed the
    /// network, so callers count only real store GETs: a cache hit and a late
    /// serve (a single-flight follower of another caller's GET on either cache
    /// kind, or a RAM-only recheck serve) both report false. Only a hit is a
    /// cache hit in the query accounting; a late serve stays a miss.
    /// [`cached_extent_outcome`](Self::cached_extent_outcome) returns the
    /// [`ReadOutcome`] for a caller that must tell those two apart.
    ///
    /// `phase` is the [`QueryPhase`] a LIVE fetch's wire bytes are charged to
    /// (#913). A cache hit crossed no network and is charged nothing: its bytes
    /// are `QueryAccounting::cache_bytes`, a different quantity.
    #[allow(clippy::too_many_arguments)]
    async fn cached_extent(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        start: u64,
        len: u64,
        range: GetRange,
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, bool), LogFetchError> {
        let (bytes, outcome) = self
            .cached_extent_outcome(
                seg_ref,
                tenant_hash,
                start,
                len,
                range,
                phase,
                pin,
                accounting,
            )
            .await?;
        Ok((bytes, outcome == ReadOutcome::Fetched))
    }

    /// [`cached_extent`](Self::cached_extent), returning the [`ReadOutcome`]
    /// instead of the `live` flag. Without a cache every call is
    /// [`ReadOutcome::Fetched`]. With a cache the query accounting is recorded
    /// here, a hit for [`ReadOutcome::Hit`] and a miss otherwise; without one
    /// neither is recorded.
    #[allow(clippy::too_many_arguments)]
    async fn cached_extent_outcome(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        start: u64,
        len: u64,
        range: GetRange,
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, ReadOutcome), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let Some(cache) = &self.cache else {
            let got = self
                .store_get_pinned(key, range, phase, pin, accounting)
                .await?;
            check_extent_len(key, got.data.len(), len)?;
            return Ok((got.data, ReadOutcome::Fetched));
        };
        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, start, len);
        // One read-through call, accounted from the returned [`ReadOutcome`]: a
        // single call avoids the peek-then-`get_or_fetch` double-count on the
        // tiered tier (see [`ReadCache::get_or_fetch`]).
        let (bytes, outcome) = cache
            .get_or_fetch(cache_key, || async move {
                let got = self
                    .store_get_pinned(key, range, phase, pin, accounting)
                    .await
                    .map_err(to_cache_error)?;
                check_extent_len(key, got.data.len(), len).map_err(to_cache_error)?;
                Ok(got.data)
            })
            .await
            .map_err(|err| from_cache_error(key, err))?;
        match outcome {
            ReadOutcome::Hit => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
            }
            ReadOutcome::Fetched | ReadOutcome::LateServe => accounting.record_cache_miss(),
        }
        Ok((bytes, outcome))
    }

    /// [`cached_extent`](Self::cached_extent) placed into `asm`: reserves `len`
    /// before the GET (ADR-1170 decision 2, so a refusal issues none), then
    /// places the returned bytes zero-copy and hands `asm` the reservation,
    /// which it holds for as long as those bytes live. Returns the placed
    /// bytes and whether the read crossed the network.
    #[allow(clippy::too_many_arguments)]
    async fn place_extent(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        start: u64,
        len: u64,
        range: GetRange,
        phase: QueryPhase,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, bool), LogFetchError> {
        let reservation = self.reserve_fetch(len)?;
        let (bytes, live) = self
            .cached_extent(
                seg_ref,
                tenant_hash,
                start,
                len,
                range,
                phase,
                pin,
                accounting,
            )
            .await?;
        self.hold_placement(asm, reservation);
        asm.place(&seg_ref.data_object_key, start, bytes.clone())?;
        Ok((bytes, live))
    }

    /// Hands `asm` a reservation for bytes it places. With a cache wired those
    /// bytes were offered to the cache, which has its own byte ledger, so the
    /// reservation is marked handed off whether or not the cache admitted
    /// them, exactly as the whole-object funnel marks its own. A hit and an
    /// admitted miss really do put the same allocation under both ledgers; a
    /// value the cache refused (over its single-entry cap) or a disk-tier hit
    /// that allocated afresh is marked too, so the figure is an upper bound on
    /// the overlap rather than an exact count of it.
    fn hold_placement(
        &self,
        asm: &mut ObjectAssembler,
        mut reservation: ravel_memory::Reservation,
    ) {
        if self.cache.is_some() {
            reservation.mark_handed_off();
        }
        asm.hold(reservation);
    }

    /// Fetch and parse just the [`LogFooter`](footer::LogFooter) via the
    /// ADR-0107 etag-establishing suffix probe (plus one footer-range chase if
    /// the suffix did not cover the whole footer), reading no block-range or
    /// directory-section bytes. Returns the footer and the probe-only
    /// [`BlockRangeStats`] (`probe_gets` set; the block counters stay zero).
    ///
    /// This is the read the predicate-free plan fast path
    /// ([`LogSegmentFetcher::plan_segment`], #693) needs: the survivor count for
    /// such a query is `footer.block_count`, so none of the object's blocks have
    /// to move. The GET is cache-routed through [`Self::cached_extent`], the
    /// same cache [`Self::fetch_object`]'s probe uses, so a later block-range
    /// fetch of the same segment that happens to probe the identical extent
    /// (same offset and length -- true above `whole_object_threshold`, where
    /// both paths use the same suffix key) hits that cached entry rather than
    /// re-fetching it. Each call creates its own [`EtagPin`]; pins are not
    /// shared across calls.
    ///
    /// The caller guarantees `seg_ref.object_size > self.block_range_threshold`
    /// (via [`LogSegmentFetcher::plan_segment`]'s fast-path gate): this method
    /// has no whole-object crossover of its own, so calling it at or below that
    /// threshold would read the object under a different cache key than
    /// [`Self::fetch_object`]'s whole-object path uses, costing an extra GET
    /// instead of saving one. A zero object size cannot be range-probed either
    /// (every extent, starting with the probe's cache key, is derived from it),
    /// which the same threshold guard rules out.
    ///
    /// This reads the footer and nothing else, so the only `probe_misses` it can
    /// report is a footer chase. It counts NO tail-section miss: SKIP_IDX and
    /// PAGE_DIR are not read here, and the read that does go on to locate blocks
    /// through them is the one that pays for a window too short to reach them.
    /// A footer carried out of here must therefore travel as a
    /// [`CarriedFooter`] with `tail_misses_counted: false`, or that read's miss
    /// is counted by nobody.
    pub async fn fetch_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(footer::LogFooter, BlockRangeStats), LogFetchError> {
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();
        // A planning read moves no BLOCKS-section data, so every byte it
        // transfers is `Plan` wire bytes (#913, `ReadPhases::PLAN`).
        let (footer, _resident) = self
            .probe_footer(
                seg_ref,
                tenant_hash,
                ReadPhases::PLAN.metadata,
                &pin,
                accounting,
                &mut stats,
            )
            .await?;
        Ok((footer, stats))
    }

    /// The probe half [`fetch_footer`](Self::fetch_footer) and
    /// [`fetch_skip_index`](Self::fetch_skip_index) share: the ADR-0107
    /// etag-establishing suffix GET, plus one footer-range chase when the suffix
    /// did not cover the whole footer, both counted into `stats.probe_gets`.
    ///
    /// The second element is every absolute region this call left resident,
    /// as `(start, bytes)` pairs. A caller reading a further section can slice it
    /// from one of those instead of issuing a GET the probe already paid for,
    /// which is the same "already covered" short circuit
    /// [`place_section`](Self::place_section) gets from [`ObjectAssembler`]
    /// without materializing an object-sized buffer for a directory-only read.
    ///
    /// `phase` is the metadata phase of the read this probe belongs to
    /// ([`ReadPhases::metadata`], #913): the probe and its chase read no
    /// BLOCKS-section data.
    #[allow(clippy::too_many_arguments)]
    async fn probe_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(footer::LogFooter, Vec<(u64, Bytes)>), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let total_size = seg_ref.object_size;

        let suffix = self.effective_suffix_len(total_size);
        let probe_start = total_size - suffix;
        let (probe_bytes, probe_live) = self
            .cached_extent(
                seg_ref,
                tenant_hash,
                probe_start,
                suffix,
                GetRange::Suffix(suffix),
                phase,
                pin,
                accounting,
            )
            .await?;
        if probe_live {
            stats.probe_gets = 1;
        }
        let mut resident = vec![(probe_start, probe_bytes.clone())];

        let footer = match open_from_suffix(&probe_bytes, total_size)
            .map_err(|source| corrupt(key, source))?
        {
            SuffixOutcome::Ready(footer) => footer,
            SuffixOutcome::NeedRange { offset, len } => {
                // The probe suffix did not even reach the footer: a probe miss
                // that forces a follow-up request (#883). Counted against the
                // window, so it fires whether or not this call's chase GET was a
                // cache hit, matching the tail-section miss accounting.
                stats.probe_misses += 1;
                let (bytes, live) = self
                    .cached_extent(
                        seg_ref,
                        tenant_hash,
                        offset,
                        len,
                        GetRange::Range(offset, offset + len),
                        phase,
                        pin,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.probe_gets += 1;
                }
                resident.push((offset, bytes.clone()));
                match open_from_suffix(&bytes, total_size).map_err(|source| corrupt(key, source))? {
                    SuffixOutcome::Ready(footer) => footer,
                    SuffixOutcome::NeedRange { .. } => {
                        return Err(corrupt(
                            key,
                            LogSegError::Corrupted("footer not covered".into()),
                        ));
                    }
                }
            }
        };
        Ok((footer, resident))
    }

    /// Fetch and decode just the SKIP_IDX section: the same ADR-0107 probe
    /// [`fetch_footer`](Self::fetch_footer) issues, then the one section the
    /// footer's directory locates it at, reading no BLOCKS byte and no other
    /// directory section. Returns the decoded index and the read's
    /// [`BlockRangeStats`] (`probe_gets` for the probe, `metadata_gets` for the
    /// SKIP_IDX section GET when the probe did not already cover it; the block
    /// counters stay zero).
    ///
    /// This is one section further than [`fetch_footer`](Self::fetch_footer) and
    /// stops exactly where [`fetch_object`](Self::fetch_object) diverges: that
    /// method decodes SKIP_IDX for the same reason, then goes on to resolve
    /// candidate extents, weigh the coverage crossover, and fetch blocks. Nothing
    /// here does any of that -- the caller
    /// ([`LogSegmentFetcher::plan_segment_block_stats`]) wants the index's own
    /// per-block figures, not the blocks they describe.
    ///
    /// The section GET is cache-routed through [`Self::cached_extent`] on the
    /// section's own extent, the same key
    /// [`place_section`](Self::place_section) uses, so a later block-range fetch
    /// of the same segment hits the cached entry rather than re-fetching it.
    /// Each call creates its own [`EtagPin`]; pins are not shared across calls.
    ///
    /// The caller guarantees `seg_ref.object_size > self.block_range_threshold`,
    /// for the reason [`fetch_footer`](Self::fetch_footer) documents: this method
    /// has no whole-object crossover of its own, so below that threshold it would
    /// read the object under a different cache key than the whole-object path
    /// uses, costing a GET instead of saving one.
    pub async fn fetch_skip_index(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(SkipIndex, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();
        let phase = ReadPhases::PLAN.metadata;
        let (footer, mut resident) = self
            .probe_footer(seg_ref, tenant_hash, phase, &pin, accounting, &mut stats)
            .await?;

        let skip_desc = *footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing SKIP_IDX".into())))?;

        // SKIP_IDX only: this entry point's caller
        // (`plan_segment_block_stats`) reads the index's own per-block figures
        // and no page, so warming PAGE_DIR here would move bytes nothing goes on
        // to use. `fetch_plan_sections`, whose caller IS followed by a scan,
        // brings the pair.
        self.ensure_tail_plan_sections(
            seg_ref,
            tenant_hash,
            &footer,
            &[kind::SKIP_IDX],
            phase,
            &mut resident,
            &pin,
            accounting,
            &mut stats,
        )
        .await?;
        let raw = self
            .plan_section_raw(
                seg_ref,
                tenant_hash,
                &skip_desc,
                &resident,
                phase,
                &pin,
                accounting,
                &mut stats,
            )
            .await?;
        let skip = SkipIndex::decode(&raw, MAX_BLOCKS).map_err(|source| corrupt(key, source))?;
        Ok((skip, stats))
    }

    /// Bring the named TAIL sections into `resident`. Sections the probe already
    /// covered cost nothing; the rest are fetched as coalesced runs, so a probe
    /// too short for two adjacent sections (SKIP_IDX and PAGE_DIR always are --
    /// the writer emits PAGE_DIR immediately after SKIP_IDX) costs one extra GET
    /// rather than two (issue #766). Every named section the probe WINDOW did
    /// not cover is counted in [`BlockRangeStats::probe_misses`], whether or not
    /// a GET was needed for it.
    ///
    /// A kind the footer does not carry is skipped, which is how a version-3
    /// object passes PAGE_DIR here harmlessly.
    #[allow(clippy::too_many_arguments)]
    async fn ensure_tail_plan_sections(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &footer::LogFooter,
        kinds: &[u32],
        phase: QueryPhase,
        resident: &mut Vec<(u64, Bytes)>,
        pin: &EtagPin,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let total_size = seg_ref.object_size;
        let suffix = self.effective_suffix_len(total_size);
        let mut missing: Vec<ByteExtent> = Vec::new();
        for &k in kinds {
            let Some(desc) = footer.section(k) else {
                continue;
            };
            if !probe_window_covers(desc, total_size, suffix) {
                stats.probe_misses += 1;
            }
            if resident_slice(resident, desc.offset, desc.len).is_none() {
                missing.push(ByteExtent {
                    abs_start: desc.offset,
                    len: desc.len,
                });
            }
        }
        for run in coalesce_byte_extents(&missing, self.effective_coalesce_gap()) {
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    run.abs_start,
                    run.len,
                    GetRange::Range(run.abs_start, run.abs_end()),
                    phase,
                    pin,
                    accounting,
                )
                .await?;
            if live {
                stats.metadata_gets += 1;
            }
            resident.push((run.abs_start, bytes));
        }
        Ok(())
    }

    /// Decode one whole-compressed directory section from a region the probe
    /// already left resident, or a cache-routed range GET when it did not.
    /// Section-kind agnostic, so [`fetch_skip_index`](Self::fetch_skip_index)
    /// and [`fetch_plan_sections`](Self::fetch_plan_sections) pull SKIP_IDX and
    /// FIELD_DIR through the same path without an object-sized buffer, unlike
    /// [`place_section`](Self::place_section), which needs an
    /// [`ObjectAssembler`] to place into.
    #[allow(clippy::too_many_arguments)]
    async fn plan_section_raw(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        desc: &SectionDesc,
        resident: &[(u64, Bytes)],
        phase: QueryPhase,
        pin: &EtagPin,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<Vec<u8>, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let stored = match resident_slice(resident, desc.offset, desc.len) {
            Some(bytes) => bytes,
            None => {
                let (start, end) = (desc.offset, desc.offset + desc.len);
                let (bytes, live) = self
                    .cached_extent(
                        seg_ref,
                        tenant_hash,
                        desc.offset,
                        desc.len,
                        GetRange::Range(start, end),
                        phase,
                        pin,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.metadata_gets += 1;
                }
                bytes
            }
        };
        // A planning read decompresses this directory section to count survivors
        // or resolve columns; charge what zstd produced to the phase handle this
        // read belongs to (issue #1401), the same handle its GETs are charged
        // against above. Separate from the scan's own directory decode in
        // `RlogReader::new`, which the scan phase charges through `ScanStats`.
        self.decode_section_on_gate(key, &stored, desc, accounting)
            .await
    }

    /// Read the footer, SKIP_IDX, and FIELD_DIR for one segment and decode all
    /// three, fetching no BLOCKS byte (#761): the plan phase's counterpart of
    /// [`fetch_skip_index`](Self::fetch_skip_index) for a query carrying
    /// prune-only NumRange arms. The footer is returned so the caller can carry
    /// it to each per-partition subset open (they then skip re-probing, #693 part
    /// 3), the SKIP_IDX drives the survivor count, and the FIELD_DIR resolves the
    /// arms to this object's column ids so that count is computed with the same
    /// pruning the scan will apply.
    ///
    /// One ADR-0107 suffix probe plus, where the probe did not already cover
    /// them, one range GET per section: SKIP_IDX (near the tail, usually covered
    /// by a production-sized probe) and FIELD_DIR (a front section, generally its
    /// own GET). No whole-object crossover, so the caller must guarantee
    /// `object_size > block_range_threshold` for the reason
    /// [`fetch_footer`](Self::fetch_footer) documents.
    pub async fn fetch_plan_sections(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(footer::LogFooter, SkipIndex, FieldDir, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();
        let phase = ReadPhases::PLAN.metadata;
        let (footer, mut resident) = self
            .probe_footer(seg_ref, tenant_hash, phase, &pin, accounting, &mut stats)
            .await?;

        let skip_desc = *footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing SKIP_IDX".into())))?;
        // SKIP_IDX and, on a version-4 object, PAGE_DIR: the pair is adjacent, so
        // a probe too short for both costs one coalesced GET rather than two.
        // PAGE_DIR is brought here even though the survivor count does not need
        // it, because the scan this plan feeds locates its pages through it,
        // fetches it under this same extent key, and would otherwise pay a
        // second round trip for bytes the probe already had.
        self.ensure_tail_plan_sections(
            seg_ref,
            tenant_hash,
            &footer,
            &[kind::SKIP_IDX, kind::PAGE_DIR],
            phase,
            &mut resident,
            &pin,
            accounting,
            &mut stats,
        )
        .await?;
        let skip_raw = self
            .plan_section_raw(
                seg_ref,
                tenant_hash,
                &skip_desc,
                &resident,
                phase,
                &pin,
                accounting,
                &mut stats,
            )
            .await?;
        let skip =
            SkipIndex::decode(&skip_raw, MAX_BLOCKS).map_err(|source| corrupt(key, source))?;

        let field_desc = *footer
            .section(kind::FIELD_DIR)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing FIELD_DIR".into())))?;
        let field_raw = self
            .plan_section_raw(
                seg_ref,
                tenant_hash,
                &field_desc,
                &resident,
                phase,
                &pin,
                accounting,
                &mut stats,
            )
            .await?;
        let field_dir =
            FieldDir::decode(&field_raw, MAX_FIELDS).map_err(|source| corrupt(key, source))?;

        Ok((footer, skip, field_dir, stats))
    }

    /// Read and decode all four footer directories (STREAM_DIR, FIELD_DIR,
    /// SKIP_IDX, PAGE_DIR) for one segment, fetching no BLOCKS byte: the plan
    /// phase's counterpart of [`RlogReader::from_source`] for a query whose
    /// plan needs the full directory set (ADR-2414 decision A1), not just the
    /// SKIP_IDX/FIELD_DIR pair [`fetch_plan_sections`](Self::fetch_plan_sections)
    /// brings for prune-only arms.
    ///
    /// One ADR-0107 suffix probe plus, where the probe did not already cover
    /// them, coalesced range GETs for the missing sections. The returned
    /// [`SegmentDirectories`] is what every later open of this segment within
    /// the same query reuses via [`RlogReader::from_decoded`], so its decode
    /// (and the `open_decompressed_bytes` it carries) happens exactly once per
    /// (query, segment) rather than once per partition that opens it.
    pub async fn fetch_plan_directories(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(footer::LogFooter, Arc<SegmentDirectories>, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();
        let phase = ReadPhases::PLAN.metadata;
        let (footer, mut resident) = self
            .probe_footer(seg_ref, tenant_hash, phase, &pin, accounting, &mut stats)
            .await?;

        // SKIP_IDX and PAGE_DIR are the tail sections whose probe misses the
        // plan phase counts (the same pair `fetch_plan_sections` counts). The
        // front pair is brought through a scratch stats value: a front section
        // is never inside the suffix probe, so counting it would add two
        // misses per object that report a property of the section order, not
        // of the probe length.
        self.ensure_tail_plan_sections(
            seg_ref,
            tenant_hash,
            &footer,
            &[kind::SKIP_IDX, kind::PAGE_DIR],
            phase,
            &mut resident,
            &pin,
            accounting,
            &mut stats,
        )
        .await?;
        let mut front_stats = BlockRangeStats::default();
        self.ensure_tail_plan_sections(
            seg_ref,
            tenant_hash,
            &footer,
            &[kind::STREAM_DIR, kind::FIELD_DIR],
            phase,
            &mut resident,
            &pin,
            accounting,
            &mut front_stats,
        )
        .await?;
        stats.metadata_gets += front_stats.metadata_gets;

        let mut sparse = SparseObject::new(seg_ref.object_size);
        for (start, bytes) in resident {
            sparse
                .place(start, bytes)
                .map_err(|source| corrupt(key, source))?;
        }
        let dirs = RlogReader::decode_directories(&sparse, &self.cfg)
            .map_err(|source| corrupt(key, source))?;
        Ok((footer, Arc::new(dirs), stats))
    }

    /// Fetch one segment's object as decode-ready [`LogObjectBytes`], reading
    /// only the blocks skip-index pruning (over `[ts_min_ns, ts_max_ns]`) proved
    /// relevant. Returns the bytes plus the [`BlockRangeStats`] for the fetch.
    ///
    /// The caller has already decided the object is ts-relevant. `ts_min_ns`/
    /// `ts_max_ns` are the inclusive query bounds used for skip-index candidate
    /// selection here; stream/POSTINGS/bloom/numeric pruning still runs at decode
    /// inside the reader over the placed regions (it can only narrow the
    /// candidate set further, so every survivor block is fetched).
    ///
    /// A data read, so its wire bytes are charged under [`ReadPhases::SCAN`]
    /// (#913).
    pub async fn fetch_object(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            &[],
            &ColumnSelection::all(),
            None,
            ReadPhases::SCAN,
            accounting,
        )
        .await
    }

    /// [`fetch_object`](Self::fetch_object) with a column projection, which on
    /// a version-4 object is a FETCH projection (ADR-0699 decision 5): the read
    /// brings one coalesced range per surviving `(row group, projected column)`
    /// instead of every column of every surviving block. On a version-3 object
    /// `columns` changes nothing about what is fetched -- a block is one
    /// contiguous range there and the projection is a decode choice only
    /// (ADR-0087).
    pub async fn fetch_object_projected(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            &[],
            columns,
            None,
            ReadPhases::SCAN,
            accounting,
        )
        .await
    }

    /// [`fetch_object`](Self::fetch_object), optionally reusing a
    /// [`footer::LogFooter`] a prior plan phase already read for this exact
    /// (immutable) object (#693 part 3, deliverable 2).
    ///
    /// When `plan_footer` is `Some`, the etag-establishing suffix probe is
    /// skipped: the carried [`CarriedFooter::footer`] already gives every
    /// section's offset and
    /// length, so the read goes straight to fetching SKIP_IDX, the remaining
    /// directory sections, and the candidate blocks. The [`EtagPin`] is then
    /// established on the FIRST of those live GETs
    /// ([`store_get_pinned`](Self::store_get_pinned)) rather than on the probe,
    /// and still fails closed: a mid-sequence replacement makes a later live GET
    /// report a different etag ([`LogFetchError::EtagChanged`]), and a
    /// replacement that predates the whole sequence is caught by the carried
    /// footer's per-section crc (a section read at the old offset from a
    /// different object fails its stored `crc32c`, a hard [`LogFetchError::Corrupt`]).
    /// Either way the fetch errors rather than assembling bytes from two object
    /// states. `None` keeps the probe-first behavior unchanged.
    ///
    /// [`CarriedFooter::tail_misses_counted`] says whether the plan read that
    /// produced the footer already counted this object's tail-section probe
    /// misses; see the invariant stated at `count_tail_misses` below.
    ///
    /// `prune` carries the query's prune-only predicates (#761). Its NumRange
    /// arms are resolved against this object's FIELD_DIR and applied to the
    /// candidate-block selection, so a selective query reads only the surviving
    /// blocks instead of tripping the coverage crossover into a whole-object GET.
    /// See [`resolve_extents`](Self::resolve_extents) for why this stays
    /// byte-identical to the unpruned read.
    ///
    /// `columns` is the query's [`ColumnSelection`]. On a version-4 object it
    /// selects which column chunks are fetched as well as which are decoded
    /// (ADR-0699 decision 5); on a version-3 object it is ignored by the fetch,
    /// which reads whole blocks.
    ///
    /// `phases` splits this read's WIRE bytes between the phase that pays for
    /// finding the data and the phase that pays for the data (#913). A caller
    /// scanning passes [`ReadPhases::SCAN`]; `plan_segment`'s whole-object
    /// fallback passes [`ReadPhases::PLAN`], which keeps a planning read's
    /// bytes out of the scan-phase numerator.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_object_with_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        prune: &[Predicate],
        columns: &ColumnSelection,
        plan_footer: Option<CarriedFooter<'_>>,
        phases: ReadPhases,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer_impl(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            prune,
            columns,
            plan_footer,
            phases,
            None,
            accounting,
        )
        .await
    }

    /// [`fetch_object_with_footer`](Self::fetch_object_with_footer), additionally
    /// restricting a version-4 object's ranged read to `owned_blocks`'s
    /// whole-object block indices (ADR-2414 decision A1 deliverable 3): the
    /// striped route's per-partition open, whose `owned_work` dealt this
    /// segment's row groups across partitions, asks for only its own row
    /// groups' blocks rather than every block the ts/numeric candidate set
    /// would otherwise keep for the WHOLE object. Ignored on the version-3
    /// path (no row-group concept to restrict by); see
    /// [`fetch_object_v4`](Self::fetch_object_v4)'s own doc for where the
    /// intersection happens.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_object_with_footer_subset(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        prune: &[Predicate],
        columns: &ColumnSelection,
        plan_footer: Option<CarriedFooter<'_>>,
        phases: ReadPhases,
        owned_blocks: Option<OwnedBlocks<'_>>,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer_impl(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            prune,
            columns,
            plan_footer,
            phases,
            owned_blocks,
            accounting,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_object_with_footer_impl(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        prune: &[Predicate],
        columns: &ColumnSelection,
        plan_footer: Option<CarriedFooter<'_>>,
        phases: ReadPhases,
        owned_blocks: Option<OwnedBlocks<'_>>,
        accounting: &QueryAccounting,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();

        // The invariant behind every `probe_misses` site: a tail-section miss is
        // counted exactly once per object per read path, by whichever layer
        // actually issued the probe. This read issued it when no footer was
        // carried, so it counts its own. A carried footer names a plan read that
        // issued the probe instead, and `tail_misses_counted` says whether that
        // read already counted the tail sections (`fetch_plan_sections`, via
        // `ensure_tail_plan_sections`) or read the footer alone and counted
        // nothing (`fetch_footer`). Both directions are real defects and they do
        // not cancel: counting a `fetch_plan_sections` object again double-reports
        // it, and skipping a `fetch_footer` object drops a miss that costs this
        // read a real extra request. Under-reporting is the worse of the two,
        // because `probe_misses` is the figure that gates any future tightening
        // of the derived probe floor and a metric that hides misses makes the
        // probe look safer than it is.
        let count_tail_misses = !plan_footer.is_some_and(|carried| carried.tail_misses_counted);

        // An object whose commit record carries no size cannot be range-planned
        // at all: every range in this protocol, starting with the probe's own
        // cache key, is derived from that size. Read it whole, uncached (the
        // cache key would have to claim a length too).
        if seg_ref.object_size == 0 {
            let got = self
                .store_get(key, GetRange::Full, phases.blocks, accounting)
                .await?;
            stats.probe_gets = 1;
            stats.whole_object = true;
            stats.block_bytes_fetched = got.data.len() as u64;
            return Ok((got.data.into(), stats));
        }

        // Size-threshold pre-probe crossover (ADR-0107 decision 1): a small
        // object is read whole in one GET, mirroring `SegmentFetcher`. Keyed
        // `(0, object_size)`, the same key `LogSegmentFetcher::tenant_bytes`
        // gives its whole-object read, so the two compose and concurrent callers
        // coalesce instead of each paying the GET. The threshold is request-cost
        // driven (deliverable 2): below the break-even the ranged path's extra
        // round trips cost more than the bytes they could save at any
        // selectivity, so the whole-object read is the faster option even though
        // it moves the most bytes.
        if seg_ref.object_size <= self.effective_whole_object_threshold() {
            // Covering read, bounded by the fetch bound (ADR-0996 decision 2):
            // one `GetRange::Full` for an object at or under the bound, else
            // `ceil(object_size / bound)` sequential covering sub-range GETs.
            // Under request-minimal the crossover is saturated, so this is the
            // path every above-`block_range_threshold` object takes.
            let (bytes, live_gets, live_bytes) = self
                .covering_read(
                    seg_ref,
                    tenant_hash,
                    seg_ref.object_size,
                    GetRange::Full,
                    phases.blocks,
                    &pin,
                    accounting,
                )
                .await?;
            if live_gets > 0 {
                // The first covering GET keeps the historical `probe_gets`
                // name; any further segmented GETs are block-data reads.
                stats.probe_gets = 1;
                stats.block_range_gets = live_gets.saturating_sub(1);
                stats.block_bytes_fetched = live_bytes;
            }
            stats.whole_object = true;
            return Ok((bytes, stats));
        }

        // The object size from the commit record is the authoritative total on
        // this path: it already decided the crossover above, it already keys the
        // whole-object funnel's cache entry, and the probe's own cache key needs
        // the suffix's absolute extent BEFORE the GET that would report a size.
        // A size that disagrees with the stored object fails closed rather than
        // silently mixing: the probe's length check rejects a short read, and a
        // footer parsed at wrong absolute offsets is a `Corrupt`.
        let total_size = seg_ref.object_size;
        // The assembler holds only what this read places (issue #2066). Each
        // placement reserves its own length before the GET that fetches it
        // (ADR-1170 decision 2, `place_extent`), and the assembler, then its
        // `into_bytes` result, holds those guards until the reader drops the
        // bytes. A refusal fails typed with zero GETs for the refused range.
        let mut asm = ObjectAssembler::new(&self.assembly_gauge, total_size);

        // Footer: reused from the plan phase when carried (deliverable 2), else
        // read via the etag-establishing suffix probe. The probe is a suffix GET
        // that pins the etag every later live GET is checked against and carries
        // the footer (and, for a small object, the whole tail directory);
        // cache-routed like every other GET here, so concurrent partitions'
        // probes collapse onto one request. When the footer is carried the probe
        // is skipped entirely and the pin is established below on the first live
        // section/block GET instead; the carried footer's per-section crc still
        // catches a replaced object (see the method doc).
        let footer = match plan_footer {
            Some(carried) => carried.footer.clone(),
            None => {
                let suffix = self.effective_suffix_len(total_size);
                let probe_start = total_size - suffix;
                let (probe_bytes, probe_live) = self
                    .place_extent(
                        seg_ref,
                        tenant_hash,
                        probe_start,
                        suffix,
                        GetRange::Suffix(suffix),
                        phases.metadata,
                        &pin,
                        &mut asm,
                        accounting,
                    )
                    .await?;
                if probe_live {
                    stats.probe_gets = 1;
                }
                // Parse from the probe suffix, chasing one range if the suffix
                // did not cover the whole footer (mirrors
                // `SegmentFetcher::open_segment`).
                match open_from_suffix(&probe_bytes, total_size)
                    .map_err(|source| corrupt(key, source))?
                {
                    SuffixOutcome::Ready(footer) => footer,
                    SuffixOutcome::NeedRange { offset, len } => {
                        // The probe suffix did not reach the footer: a probe
                        // miss forcing a follow-up request (#883), counted
                        // against the window like the tail-section misses below.
                        stats.probe_misses += 1;
                        let (bytes, live) = self
                            .place_extent(
                                seg_ref,
                                tenant_hash,
                                offset,
                                len,
                                GetRange::Range(offset, offset + len),
                                phases.metadata,
                                &pin,
                                &mut asm,
                                accounting,
                            )
                            .await?;
                        if live {
                            stats.probe_gets += 1;
                        }
                        match open_from_suffix(&bytes, total_size)
                            .map_err(|source| corrupt(key, source))?
                        {
                            SuffixOutcome::Ready(footer) => footer,
                            SuffixOutcome::NeedRange { .. } => {
                                return Err(corrupt(
                                    key,
                                    LogSegError::Corrupted("footer not covered".into()),
                                ));
                            }
                        }
                    }
                }
            }
        };

        // RLOG version 4 (ADR-0699) stores a row group's pages column-major, so
        // a block is no longer a contiguous byte range and its SKIP_IDX
        // `block_offset`/`block_len` describe a page span overlapping its
        // neighbours', with the block crc defined over its pages in column_id
        // order rather than over that span. The block-range protocol below
        // assumes both, so a version-4 object takes decision 5's chunk path
        // instead: PAGE_DIR turns each surviving `(row group, projected
        // column)` into one coalesced range. Dispatched here, after the footer
        // is resolved, because PAGE_DIR's presence is only known from the
        // footer -- which covers both the probe path and the plan-carried
        // footer path (#693 part 3).
        if footer.section(kind::PAGE_DIR).is_some() {
            return self
                .fetch_object_v4(
                    seg_ref,
                    tenant_hash,
                    ts_min_ns,
                    ts_max_ns,
                    prune,
                    columns,
                    &footer,
                    plan_footer.is_none(),
                    count_tail_misses,
                    asm,
                    &pin,
                    phases,
                    owned_blocks,
                    accounting,
                    stats,
                )
                .await;
        }

        let skip_desc = footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing SKIP_IDX".into())))?;
        let blocks_desc = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing BLOCKS".into())))?;

        // Residual miss rate for the version-3 scan path (#883, mirroring the
        // version-4 path and `ensure_tail_plan_sections`): SKIP_IDX is the tail
        // section this read locates candidate blocks through, counted against
        // the probe WINDOW rather than against cache residency, so the figure is
        // a property of the derived probe length and this object's shape. A
        // window too short to reach SKIP_IDX forces the section GET below, which
        // is exactly the extra request a too-small derivation would cost.
        // Counted here only when no earlier layer already counted it for this
        // object, per the invariant at `count_tail_misses` above.
        if count_tail_misses
            && !probe_window_covers(skip_desc, total_size, self.effective_suffix_len(total_size))
        {
            stats.probe_misses += 1;
        }

        // SKIP_IDX first, and nothing the candidate set does not need. The
        // coverage crossover below can decide the whole object is cheaper than
        // the candidate ranges, and every OTHER section fetched first is then
        // wasted: a wide-time-range query on a large object would pay probe +
        // section GETs and THEN a whole-object GET, strictly worse than the
        // plain whole-object path it falls back to. Resolving the candidate
        // extents needs SKIP_IDX always and FIELD_DIR only when the query
        // carries a NumRange arm to resolve (#761), so those are the only two
        // sections fetched before the decision, and the second only on demand.
        self.place_section(
            seg_ref,
            tenant_hash,
            skip_desc,
            phases.metadata,
            &pin,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;

        // Decode the skip index (now resident) and resolve the candidate blocks.
        // A ranged read of this shape, so charged to whichever phase handle the
        // caller passed in (issue #1401 finding 3): `decode_section_accounted`
        // has no phase tag of its own, only the handle it is given.
        let skip_stored = asm.slice(key, skip_desc.offset, skip_desc.len)?;
        let skip_raw = self
            .decode_section_on_gate(key, &skip_stored, skip_desc, accounting)
            .await?;
        let skip =
            SkipIndex::decode(&skip_raw, MAX_BLOCKS).map_err(|source| corrupt(key, source))?;

        // Resolve the query's prune-only NumRange arms against THIS object's
        // FIELD_DIR so `candidate_blocks` can drop blocks the numeric bounds
        // prove hold no match (#761). Without this the candidate set was ts-only
        // and every block survived, so a month-wide selective query crossed the
        // coverage threshold and read the whole object. On the ranged branch
        // FIELD_DIR is a front section the section loop below fetches anyway,
        // so placing it here only reorders that GET; on the coverage-crossover
        // branch it is one extra small GET in front of the whole-object read,
        // unavoidable because the arms decide the candidate set that decides
        // coverage. It is skipped entirely when the query carries no NumRange
        // arm (a predicate-free or text-only query keeps the pre-#761 ts-only
        // candidate set). Bloom/POSTINGS arms are not resolved
        // here: they narrow further at decode over the fetched buffer, which is
        // sound because that narrowing only ever skips a fetched block.
        let numeric: Vec<NumRangeArm> = if prune
            .iter()
            .any(|p| matches!(p, Predicate::NumRange { .. }))
        {
            let field_dir = self
                .place_and_decode_field_dir(
                    seg_ref,
                    tenant_hash,
                    &footer,
                    phases.metadata,
                    &pin,
                    &mut asm,
                    accounting,
                    &mut stats,
                )
                .await?;
            let refs: Vec<&Predicate> = prune.iter().collect();
            field_dir.numeric_range_arms(&refs)
        } else {
            Vec::new()
        };

        let extents = self.resolve_extents(
            key,
            &skip,
            blocks_desc.offset,
            ts_min_ns,
            ts_max_ns,
            &numeric,
        )?;
        stats.candidate_blocks = extents.len() as u64;

        // Coverage-based post-pruning crossover (ADR-0107 decision 1): when the
        // candidate ranges already cover most of the blocks, one whole-object GET
        // beats many range GETs. The comparison is against the BLOCKS section's
        // own size, not the object size: the numerator is BLOCKS-only candidate
        // bytes, so measuring it against an object that also carries the
        // directory sections can never reach 1.0 even when every block is a
        // candidate, and under-triggers by exactly the metadata fraction.
        let candidate_bytes: u64 = extents.iter().map(|e| e.len).sum();
        let coverage = candidate_bytes as f64 / blocks_desc.len.max(1) as f64;
        if coverage >= self.coverage_threshold {
            // `extents` is already owned (resolved above from the decoded skip
            // index, not borrowed from `asm`), so the assembler holds no live
            // borrow here. Drop it -- releasing its placed regions and their
            // reservation guards -- BEFORE the covering GET reserves fresh
            // (ADR-1170 decision 2), so the probe and sections placed so far are
            // not held beside the whole object the covering read brings.
            drop(asm);
            // Keyed and single-flighted like every other GET here, on the same
            // `(0, object_size)` key the whole-object funnel uses: without that,
            // N partitions all crossing over would issue N whole-object GETs,
            // which is the amplification this path exists to avoid. Bounded by
            // the fetch bound (ADR-0996 decision 2): one covering GET at or under
            // the bound, else segmented covering sub-ranges.
            let (bytes, live_gets, live_bytes) = self
                .covering_read(
                    seg_ref,
                    tenant_hash,
                    total_size,
                    GetRange::Full,
                    phases.blocks,
                    &pin,
                    accounting,
                )
                .await?;
            if live_gets > 0 {
                stats.block_range_gets = live_gets;
                stats.block_bytes_fetched = live_bytes;
            }
            stats.whole_object = true;
            // Still admit per-block cache entries so a later partition's fetch of
            // a subset composes with this one (ADR-0107 decision 3).
            self.admit_blocks_from_whole(seg_ref, tenant_hash, &bytes, &extents)
                .await;
            return Ok((bytes, stats));
        }

        // The suffix probe normally places the object's tail -- the footer and
        // trailer bytes `RlogReader` re-reads to open the placed regions, which
        // are not themselves listed sections. When the footer was carried the
        // probe was skipped, so on this (non-coverage) branch that tail is still
        // unplaced; place it now as one range over `[last_section_end,
        // object_size)`. Before #761 a carried footer implied an all-candidate
        // (predicate-free, fully-contained) query whose coverage always crossed
        // over above, so this range was unreachable in practice. The skip-index
        // plan branch now carries a footer for a SELECTIVE query too, which is
        // exactly the query that stays below the crossover, so this is a live
        // per-segment range GET on that path rather than a fail-safe.
        if plan_footer.is_some() {
            let tail_start = footer
                .sections
                .iter()
                .map(|s| s.offset + s.len)
                .max()
                .unwrap_or(0);
            if tail_start < total_size && !asm.covers(tail_start, total_size) {
                let (_, live) = self
                    .place_extent(
                        seg_ref,
                        tenant_hash,
                        tail_start,
                        total_size - tail_start,
                        GetRange::Range(tail_start, total_size),
                        phases.metadata,
                        &pin,
                        &mut asm,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.metadata_gets += 1;
                }
            }
        }

        // The two front sections (STREAM_DIR/FIELD_DIR): one coalesced GET over
        // their adjacent span (deliverable 4), skipped entirely when an earlier
        // FIELD_DIR resolution already brought them.
        self.place_front_sections(
            seg_ref,
            tenant_hash,
            &footer,
            phases.metadata,
            &pin,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;

        // Any remaining tail section (BLOOM/POSTINGS) a short probe missed: one
        // GET each, never coalesced with the front across the BLOCKS gap. The
        // reader re-verifies each section's crc on decode, so a corrupt section
        // hit fails closed there (ADR-0046).
        for section in &footer.sections {
            if matches!(
                section.kind,
                kind::BLOCKS | kind::STREAM_DIR | kind::FIELD_DIR
            ) {
                continue;
            }
            self.place_section(
                seg_ref,
                tenant_hash,
                section,
                phases.metadata,
                &pin,
                &mut asm,
                accounting,
                &mut stats,
            )
            .await?;
        }

        self.fetch_blocks(
            seg_ref,
            tenant_hash,
            &pin,
            &extents,
            phases.blocks,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;
        Ok((asm.into_bytes(), stats))
    }

    /// The version-4 read (ADR-0699 decision 5): one coalesced range per
    /// surviving `(row group, projected column)`, instead of the version-3
    /// protocol's one range per surviving block.
    ///
    /// The footer is already resolved -- by the suffix probe (`probed`), whose
    /// bytes are already in `asm`, or carried from the plan phase -- so this
    /// picks up at the directories. SKIP_IDX says which blocks survive, PAGE_DIR
    /// says where their pages are, and the query's [`ColumnSelection`] resolved
    /// against FIELD_DIR says which column chunks to read. The pages the
    /// surviving blocks hold in the kept chunks become byte extents that the
    /// same coalescing rule the version-3 path uses fuses into GETs: a pruned
    /// block's pages are the gaps in those runs, read through when the gap is
    /// under `coalesce_gap` and split around otherwise. When the selection keeps
    /// every column and every block of a group survives, that group's pages are
    /// one contiguous span and coalesce into exactly one range, which is why
    /// there is no separate whole-group case here.
    ///
    /// `owned_blocks`, when `Some` (ADR-2414 decision A1 deliverable 3), further
    /// narrows the ts/numeric candidate set to the whole-object block indices
    /// this call's caller actually owns: the striped route's per-partition
    /// open, whose `owned_work` dealt this segment's row groups across
    /// partitions, so a partition's ranged plan covers only its own blocks
    /// instead of every block the ts bounds and skip index would otherwise
    /// keep for the WHOLE object. `None` is every other caller's unchanged
    /// behavior (the version-4 whole-segment and ordinal-subset paths, which
    /// have no per-partition block list to restrict by).
    ///
    /// # Checksums
    ///
    /// Nothing here verifies a block crc, and nothing can: under version 4 that
    /// crc covers the block's pages in `column_id` order, which a projected read
    /// does not hold (docs/log-segment-format.md, "BLOCKS"). Verification moves
    /// to the decode instead, per page, against PAGE_DIR's own `crc32c`
    /// (`decode_v4_block`), so every byte this fetch brings and the reader goes
    /// on to interpret is still checksum-covered on its own access path
    /// (ADR-0010 §4). A read whose selection keeps every one of a block's pages
    /// verifies the block crc as well. That also makes the coalesced range a
    /// legitimate cache unit even though it can span a pruned block's pages: a
    /// corrupt cache hit fails at the first page crc it feeds the decode, the
    /// same way a corrupt live fetch does, and the gap bytes are never
    /// interpreted at all.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_object_v4(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        prune: &[Predicate],
        columns: &ColumnSelection,
        footer: &footer::LogFooter,
        probed: bool,
        count_tail_misses: bool,
        mut asm: ObjectAssembler,
        pin: &EtagPin,
        phases: ReadPhases,
        owned_blocks: Option<OwnedBlocks<'_>>,
        accounting: &QueryAccounting,
        mut stats: BlockRangeStats,
    ) -> Result<(LogObjectBytes, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let total_size = seg_ref.object_size;
        let dirs = owned_blocks.and_then(|o| o.dirs);
        let owned_blocks = owned_blocks.map(|o| o.blocks);
        let blocks_desc = *footer
            .section(kind::BLOCKS)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing BLOCKS".into())))?;
        let skip_desc = *footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing SKIP_IDX".into())))?;
        let page_desc = *footer
            .section(kind::PAGE_DIR)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing PAGE_DIR".into())))?;

        // Issue #766's residual miss rate: the two tail sections this read has
        // to locate pages through, counted against the probe WINDOW rather than
        // against cache residency, so the figure reports what the probe length
        // costs on this object's shape.
        // Same gate as the version-3 path: `count_tail_misses` is false only
        // when the plan read that carried the footer already counted these two
        // sections, per the invariant `fetch_object_with_footer` states.
        let suffix = self.effective_suffix_len(total_size);
        if count_tail_misses {
            for desc in [&skip_desc, &page_desc] {
                if !probe_window_covers(desc, total_size, suffix) {
                    stats.probe_misses += 1;
                }
            }
        }

        // A carried footer skipped the probe (#693 part 3), which on a
        // version-4 object would leave every tail section -- SKIP_IDX, PAGE_DIR,
        // BLOOM, POSTINGS -- and the footer/trailer bytes to be fetched one at a
        // time. The plan phase that carried the footer already read that whole
        // tail as its probe and admitted it under its extent key, so asking the
        // cache for the same extent places all of it at once for no GET. Only
        // done with a cache wired: without one every `cached_extent` call is a
        // live GET, and re-reading the tail on the wire would move more bytes
        // than the per-section reads it replaces.
        if !probed && self.cache.is_some() && suffix > 0 {
            let probe_start = total_size - suffix;
            let (_, live) = self
                .place_extent(
                    seg_ref,
                    tenant_hash,
                    probe_start,
                    suffix,
                    GetRange::Range(probe_start, total_size),
                    phases.metadata,
                    pin,
                    &mut asm,
                    accounting,
                )
                .await?;
            if live {
                stats.probe_gets += 1;
            }
        }

        // SKIP_IDX and PAGE_DIR first, and nothing the candidate set does not
        // need: the coverage crossover below can still decide the whole object
        // is cheaper, and every other section fetched before that decision would
        // then be wasted (the same ordering rule the version-3 path follows).
        // The two are adjacent in the object -- the writer emits PAGE_DIR
        // immediately after SKIP_IDX -- so a probe that missed both costs one
        // coalesced GET rather than two.
        //
        // With `dirs` (ADR-2414 decision A1: the plan phase already decoded this
        // segment's directories) none of the four sections is fetched or decoded
        // here: the reader built over the result takes them from `dirs`, so
        // placing them would only move bytes nobody reads, and decoding them
        // would charge a second `decompressed_bytes` for the same sections.
        let local_skip;
        let local_page_dir;
        let (skip, page_dir): (&SkipIndex, &PageDir) = if let Some(dirs) = dirs {
            (dirs.skip_index(), &**dirs.page_dir())
        } else {
            self.place_sections_coalesced(
                seg_ref,
                tenant_hash,
                &[skip_desc, page_desc],
                phases.metadata,
                pin,
                &mut asm,
                accounting,
                &mut stats,
            )
            .await?;
            let skip_raw = self
                .placed_section_raw(key, &asm, &skip_desc, accounting)
                .await?;
            local_skip =
                SkipIndex::decode(&skip_raw, MAX_BLOCKS).map_err(|source| corrupt(key, source))?;
            let page_raw = self
                .placed_section_raw(key, &asm, &page_desc, accounting)
                .await?;
            local_page_dir = PageDir::decode(&page_raw).map_err(|source| corrupt(key, source))?;
            (&local_skip, &local_page_dir)
        };
        page_dir
            .validate_extents(blocks_desc.len)
            .map_err(|source| corrupt(key, source))?;

        // FIELD_DIR, when either channel needs it: the prune-only NumRange arms
        // resolve to this object's column ids through it (#761), and so does the
        // projection. An all-columns query with no numeric arm needs neither, and
        // FIELD_DIR is a front section a suffix probe never covers, so skipping
        // it there is a real saved GET.
        let wants_numeric = prune
            .iter()
            .any(|p| matches!(p, Predicate::NumRange { .. }));
        let (numeric, selected) = if wants_numeric || !columns.is_all() {
            let local_field_dir;
            let field_dir: &FieldDir = if let Some(dirs) = dirs {
                dirs.field_dir()
            } else {
                local_field_dir = self
                    .place_and_decode_field_dir(
                        seg_ref,
                        tenant_hash,
                        footer,
                        phases.metadata,
                        pin,
                        &mut asm,
                        accounting,
                        &mut stats,
                    )
                    .await?;
                &local_field_dir
            };
            let numeric = if wants_numeric {
                let refs: Vec<&Predicate> = prune.iter().collect();
                field_dir.numeric_range_arms(&refs)
            } else {
                Vec::new()
            };
            // The same resolution `RlogReader::scan_blocks` runs on the same
            // FIELD_DIR, so the pages fetched here are exactly the pages the
            // decode addresses.
            (numeric, columns.resolve(field_dir))
        } else {
            (Vec::new(), None)
        };

        let mut candidates = skip.candidate_blocks(ts_min_ns, ts_max_ns, None, &numeric);
        // Deliverable 3 (ADR-2414 decision A1): a partition's ranged plan
        // covers only its own blocks, not every surviving block of the
        // object. `owned_blocks` is ascending (`owned_work` deals whole row
        // groups in ascending order), and `candidate_blocks` is ascending
        // too (`SkipIndex::candidate_blocks`'s own doc), so a binary search
        // per candidate is enough; no caller passes an unsorted list.
        // The pages other partitions' row groups hold in the selected columns
        // are fences: this partition's runs are never bridged or coalesced
        // across one, so the spans partitions fetch for one object stay
        // disjoint and their wire bytes sum to at most the object's size.
        let mut fences: Vec<(u64, u64)> = Vec::new();
        if let Some(owned) = owned_blocks {
            let owned_groups: HashSet<u32> = owned
                .iter()
                .filter_map(|&b| u32::try_from(b).ok())
                .filter_map(|b| page_dir.locate_block(b).map(|(g, _)| g.first_block))
                .collect();
            let foreign: Vec<usize> = candidates
                .iter()
                .copied()
                .filter(|&c| {
                    u32::try_from(c)
                        .ok()
                        .and_then(|b| page_dir.locate_block(b))
                        .is_none_or(|(g, _)| !owned_groups.contains(&g.first_block))
                })
                .collect();
            let foreign_extents = projected_page_extents(
                key,
                page_dir,
                blocks_desc.offset,
                &foreign,
                selected.as_ref(),
            )?;
            fences = merge_fences(&foreign_extents);
            candidates.retain(|c| owned.binary_search(c).is_ok());
        }
        stats.candidate_blocks = candidates.len() as u64;
        let wanted = projected_page_extents(
            key,
            page_dir,
            blocks_desc.offset,
            &candidates,
            selected.as_ref(),
        )?;

        // Coverage-based post-pruning crossover (ADR-0107 decision 1), against
        // the BLOCKS section's own size for the reason the version-3 path
        // documents. The numerator is now the projected page bytes rather than
        // whole candidate blocks, so a narrow projection stays far below the
        // threshold even when every block survives, and an all-columns read of
        // every block reaches ~1.0 and takes the single GET.
        //
        // Computed against the BRIDGED run set (ADR-2066 decision 1), not the
        // raw `wanted` extents: bridging an L0 projection down to
        // `MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT` runs can itself push the byte
        // count over the threshold, and the covering-read check must still
        // apply after that bridging, not before it. Deliberately NOT filtered
        // by what `asm` already covers (unlike `bounded_chunk_runs`): this
        // query's own projected footprint against the object's size is what
        // the crossover decides on, and an unrelated probe that already
        // brought some or all of those bytes (a small object's suffix probe
        // routinely covers the whole object) must not shrink that footprint,
        // or an all-columns read of such an object would skip `covering_read`
        // and its whole-object cache admission entirely.
        let wanted_bytes: u64 = self.bridged_run_bytes(&seg_ref.level, &wanted, &fences);
        let coverage = wanted_bytes as f64 / blocks_desc.len.max(1) as f64;
        if coverage >= self.coverage_threshold {
            // `wanted` is already owned (resolved above from the decoded skip
            // index and page directory, not borrowed from `asm`), so dropping
            // the assembler here is safe, as the version-3 coverage crossover
            // does: it releases the placed regions and their reservation
            // guards BEFORE the covering GET reserves fresh (ADR-1170
            // decision 2).
            drop(asm);
            // Bounded by the fetch bound (ADR-0996 decision 2), like the
            // version-3 coverage crossover above.
            let (bytes, live_gets, live_bytes) = self
                .covering_read(
                    seg_ref,
                    tenant_hash,
                    total_size,
                    GetRange::Full,
                    phases.blocks,
                    pin,
                    accounting,
                )
                .await?;
            if live_gets > 0 {
                stats.block_range_gets = live_gets;
                stats.block_bytes_fetched = live_bytes;
            }
            stats.whole_object = true;
            // No per-block cache admission from the whole object here, unlike
            // the version-3 path: a version-4 block is not a contiguous extent,
            // so there is no block-keyed entry a later ranged read would look
            // for. The chunk ranges are the cache unit instead, and this read
            // resolved none of them.
            return Ok((bytes, stats));
        }

        // The suffix probe normally places the object's tail -- the footer and
        // trailer bytes `RlogReader` re-reads to open the placed regions,
        // which are not themselves listed sections. A carried footer skipped the
        // probe, so place that tail now (the version-3 path does the same).
        if !probed {
            let tail_start = footer
                .sections
                .iter()
                .map(|s| s.offset + s.len)
                .max()
                .unwrap_or(0);
            if tail_start < total_size && !asm.covers(tail_start, total_size) {
                let (_, live) = self
                    .place_extent(
                        seg_ref,
                        tenant_hash,
                        tail_start,
                        total_size - tail_start,
                        GetRange::Range(tail_start, total_size),
                        phases.metadata,
                        pin,
                        &mut asm,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.metadata_gets += 1;
                }
            }
        }

        // The two front sections (STREAM_DIR/FIELD_DIR): one coalesced GET over
        // their adjacent span (ADR-2066 decision 1). On the narrow-projection
        // path `place_and_decode_field_dir` already brought both, so this is a
        // no-op there; on an all-columns v4 read it is the read's one front GET.
        // Skipped when `dirs` carries the decoded directories (ADR-2414
        // decision A1): the reader takes both from it.
        if dirs.is_none() {
            self.place_front_sections(
                seg_ref,
                tenant_hash,
                footer,
                phases.metadata,
                pin,
                &mut asm,
                accounting,
                &mut stats,
            )
            .await?;
        }

        // Any remaining tail section (BLOOM/POSTINGS) a short probe missed: one
        // GET each, never coalesced with the front across the BLOCKS gap. The
        // reader re-verifies each section's crc on decode, so a corrupt section
        // hit fails closed there (ADR-0046). With `dirs`, SKIP_IDX and PAGE_DIR
        // are not placed either: nothing reads them from the buffer.
        for section in &footer.sections {
            if matches!(
                section.kind,
                kind::BLOCKS | kind::STREAM_DIR | kind::FIELD_DIR
            ) || (dirs.is_some() && matches!(section.kind, kind::SKIP_IDX | kind::PAGE_DIR))
            {
                continue;
            }
            self.place_section(
                seg_ref,
                tenant_hash,
                section,
                phases.metadata,
                pin,
                &mut asm,
                accounting,
                &mut stats,
            )
            .await?;
        }

        self.fetch_chunk_ranges(
            seg_ref,
            tenant_hash,
            pin,
            &wanted,
            &fences,
            phases.blocks,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;
        Ok((asm.into_bytes(), stats))
    }

    /// The coalesced, already-covered-filtered, request-bounded byte ranges a
    /// version-4 chunk fetch will actually issue for `wanted`: at most
    /// [`chunk_run_cap`] runs for `level`, bridging the smallest gaps first
    /// ([`bound_runs`]) when coalescing leaves more runs than that (ADR-2066
    /// decision 1). This answers "what does this fetch still need to GET", so
    /// [`fetch_chunk_ranges`](Self::fetch_chunk_ranges) is the only caller;
    /// the coverage crossover in [`fetch_object_v4`] uses
    /// [`bridged_run_bytes`](Self::bridged_run_bytes) instead, which answers
    /// a different question and must not filter by `asm` coverage.
    fn bounded_chunk_runs(
        &self,
        level: &SegmentLevel,
        wanted: &[ByteExtent],
        fences: &[(u64, u64)],
        asm: &ObjectAssembler,
    ) -> Vec<ByteExtent> {
        let runs: Vec<(u64, u64)> = coalesce_fenced(wanted, self.effective_coalesce_gap(), fences)
            .into_iter()
            .filter(|r| !asm.covers(r.abs_start, r.abs_end()))
            .map(|r| (r.abs_start, r.abs_end()))
            .collect();
        bound_runs_fenced(runs, chunk_run_cap(level), fences)
            .into_iter()
            .map(|(start, end)| ByteExtent {
                abs_start: start,
                len: end - start,
            })
            .collect()
    }

    /// The coalesced, request-bounded byte total a version-4 chunk fetch
    /// would need for `wanted` on its own, before subtracting whatever `asm`
    /// already holds: at most [`chunk_run_cap`] runs for `level`, the same cap
    /// [`bounded_chunk_runs`](Self::bounded_chunk_runs) applies, bridging the
    /// smallest gaps first ([`bound_runs`]) when coalescing leaves more runs
    /// than that (ADR-2066 decision 1). Used only by the
    /// coverage crossover in [`fetch_object_v4`]: that decision is about this
    /// query's own projected footprint against the object's size, and must
    /// not shrink just because an unrelated earlier read (a suffix probe, on
    /// a small object, routinely covers the whole thing) already brought some
    /// of those bytes -- an all-columns read must still take the crossover
    /// and admit the object under `covering_read`'s own whole-object cache
    /// key, or a second full read of the same object never gets a
    /// whole-object cache hit.
    /// [`bounded_chunk_runs`](Self::bounded_chunk_runs) answers the different
    /// question of what an actual fetch still needs to GET, and keeps its own
    /// `asm`-covers filter for that.
    fn bridged_run_bytes(
        &self,
        level: &SegmentLevel,
        wanted: &[ByteExtent],
        fences: &[(u64, u64)],
    ) -> u64 {
        let runs: Vec<(u64, u64)> = coalesce_fenced(wanted, self.effective_coalesce_gap(), fences)
            .into_iter()
            .map(|r| (r.abs_start, r.abs_end()))
            .collect();
        bound_runs_fenced(runs, chunk_run_cap(level), fences)
            .into_iter()
            .map(|(start, end)| end - start)
            .sum()
    }

    /// Fetch the coalesced page ranges into `asm`, every run concurrently,
    /// through the same [`cached_extent`](Self::cached_extent) path every other
    /// GET here takes, and the etag pin holds across the sequence.
    ///
    /// `fences` are the byte extents other partitions' row groups hold in the
    /// selected columns (empty for a read that owns the whole object): a run is
    /// never coalesced or bridged across one ([`coalesce_fenced`],
    /// [`bound_runs_fenced`]), so the runs partitions dealt different row
    /// groups of one object issue are pairwise disjoint and their wire bytes
    /// sum to at most the object's size. A partition's request count is its own
    /// runs, at most [`chunk_run_cap`] for an L0 object unless a fence forbids
    /// the bridge that would reach the cap.
    ///
    /// These are the BLOCKS-section data ranges, so `phase` is the read's
    /// [`ReadPhases::blocks`] and the WIRE bytes recorded here are the
    /// numerator of fetch amplification (#913). A run's coalescing holes are
    /// included, because the store transferred them, and so are any bytes a
    /// gap bridged to hold an L0 object's run count at
    /// [`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT`] (ADR-2066 decision 1): those
    /// bytes are real transfer too, reserved and charged the same as any
    /// other run's.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_chunk_ranges(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        pin: &EtagPin,
        wanted: &[ByteExtent],
        fences: &[(u64, u64)],
        phase: QueryPhase,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        // A run the probe already brought costs nothing: its bytes are in
        // `asm` at the right offsets already. Bounding to at most
        // `chunk_run_cap(level)` runs happens AFTER that filter,
        // per ADR-2066 decision 1: an already-covered run should never count
        // against the cap or force a bridge a genuinely uncovered run set
        // would not have needed.
        let runs: Vec<ByteExtent> = self.bounded_chunk_runs(&seg_ref.level, wanted, fences, &*asm);
        // Reserve every run's bytes before `join_all` issues a GET (ADR-1170
        // decision 2): a refusal fails typed with zero GETs. Each run's buffer
        // is placed into `asm` as it arrived, so this one guard covers what the
        // runs place and `asm` holds it for as long as they live.
        let reserved: u64 = runs.iter().map(|r| r.len).fold(0u64, u64::saturating_add);
        let reservation = self.reserve_fetch(reserved)?;
        let outcomes = futures::future::join_all(runs.iter().map(|run| async move {
            let (bytes, served) = self
                .cached_extent_outcome(
                    seg_ref,
                    tenant_hash,
                    run.abs_start,
                    run.len,
                    GetRange::Range(run.abs_start, run.abs_end()),
                    phase,
                    pin,
                    accounting,
                )
                .await?;
            Ok::<_, LogFetchError>((run.abs_start, bytes, served))
        }))
        .await;
        self.hold_placement(asm, reservation);
        for outcome in outcomes {
            let (start, bytes, served) = outcome?;
            match served {
                ReadOutcome::Fetched => {
                    stats.block_range_gets += 1;
                    stats.block_bytes_fetched =
                        stats.block_bytes_fetched.saturating_add(bytes.len() as u64);
                }
                ReadOutcome::Hit => stats.block_cache_hits += 1,
                // No GET by this call and a query-accounting miss: neither.
                ReadOutcome::LateServe => {}
            }
            asm.place(key, start, bytes)?;
        }
        Ok(())
    }

    /// Place several sections into `asm` with one GET per coalesced run rather
    /// than one per section, for sections a caller needs together. Sections
    /// already covered by an earlier read cost nothing; the rest are merged
    /// under the same `coalesce_gap` policy the block ranges use, which is what
    /// keeps a probe too short for both SKIP_IDX and PAGE_DIR to one extra GET
    /// instead of two (issue #766).
    #[allow(clippy::too_many_arguments)]
    async fn place_sections_coalesced(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        sections: &[SectionDesc],
        phase: QueryPhase,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let missing: Vec<ByteExtent> = sections
            .iter()
            .filter(|s| !asm.covers(s.offset, s.offset + s.len))
            .map(|s| ByteExtent {
                abs_start: s.offset,
                len: s.len,
            })
            .collect();
        for run in coalesce_byte_extents(&missing, self.effective_coalesce_gap()) {
            let (_, live) = self
                .place_extent(
                    seg_ref,
                    tenant_hash,
                    run.abs_start,
                    run.len,
                    GetRange::Range(run.abs_start, run.abs_end()),
                    phase,
                    pin,
                    asm,
                    accounting,
                )
                .await?;
            if live {
                stats.metadata_gets += 1;
            }
        }
        Ok(())
    }

    /// Decode one whole-compressed section out of the assembler, where an
    /// earlier `place_*` call already put its stored bytes. Charges the bytes
    /// zstd produced to `accounting` (issue #1401 finding 3): a ranged read's
    /// SKIP_IDX and PAGE_DIR decode, so its callers pass the same handle their
    /// GETs are charged against.
    async fn placed_section_raw(
        &self,
        key: &str,
        asm: &ObjectAssembler,
        desc: &SectionDesc,
        accounting: &QueryAccounting,
    ) -> Result<Vec<u8>, LogFetchError> {
        let stored = asm.slice(key, desc.offset, desc.len)?;
        self.decode_section_on_gate(key, &stored, desc, accounting)
            .await
    }

    /// Resolve each candidate block index (from `skip.candidate_blocks`) to its
    /// absolute byte extent and stored crc. The byte extent is always the block's
    /// full extent from its SKIP_IDX level-0 entry, never a sub-block slice
    /// (ADR-0107 decision 1).
    ///
    /// `numeric` are the query's prune-only [`NumRangeArm`]s, already resolved to
    /// this object's own column ids against its FIELD_DIR
    /// ([`FieldDir::numeric_range_arms`]). They narrow the candidate set to the
    /// blocks whose recorded numeric bounds can still hold a matching row, exactly
    /// as [`ravel_logseg::RlogReader::scan_blocks`] narrows it at decode with the
    /// same arms and the same directory. This is why fetch-side pruning is
    /// byte-identical to the unpruned read: the skip index's per-block bounds are
    /// conservative (ADR-0013), so a block dropped here is one the decode-side
    /// prune would have dropped anyway, never a block a surviving row lives in.
    /// Bloom/POSTINGS pruning is a strict further narrowing the reader still runs
    /// over the fetched buffer, so it can only skip blocks this already fetched,
    /// never demand one it did not. With `numeric` empty (a predicate the skip
    /// index cannot decide, or none) every ts-candidate block is kept, the
    /// pre-#761 behavior.
    fn resolve_extents(
        &self,
        key: &str,
        skip: &SkipIndex,
        blocks_offset: u64,
        ts_min_ns: i64,
        ts_max_ns: i64,
        numeric: &[NumRangeArm],
    ) -> Result<Vec<BlockExtent>, LogFetchError> {
        let candidates = skip.candidate_blocks(ts_min_ns, ts_max_ns, None, numeric);
        let mut out = Vec::with_capacity(candidates.len());
        for i in candidates {
            let entry = skip.l0.get(i).ok_or_else(|| {
                corrupt(
                    key,
                    LogSegError::Corrupted("skip block index out of range".into()),
                )
            })?;
            let abs_start = blocks_offset
                .checked_add(entry.block_offset)
                .ok_or_else(|| corrupt_range(key))?;
            out.push(BlockExtent {
                abs_start,
                len: entry.block_len,
                crc32c: entry.block_crc32c,
            });
        }
        Ok(out)
    }

    /// Place the object's two front directory sections -- STREAM_DIR (kind 1)
    /// then FIELD_DIR (kind 2), which the writer emits ADJACENT at the object
    /// front (docs/log-segment-format.md) -- in ONE range GET over their combined
    /// span, instead of one GET each (deliverable 4). No suffix probe of any
    /// length reaches a front section, so without coalescing the two are two of
    /// the ~5.46 store round trips an object's ranged read costs (ADR-0107), for
    /// a byte volume that is a rounding error against one request cost; presenting
    /// them together to one GET removes one whole request per object. A section
    /// already resident (an earlier call, or a footer that omits one of the two)
    /// costs nothing, and the span then covers only whichever remains. A section
    /// resident in the shared read cache under its OWN per-section key (a plan
    /// read that fetched it alone) is also served from there and dropped from
    /// the span, so a scan never re-fetches live bytes the plan phase already
    /// cached under a narrower key. A combined GET of both sections admits each
    /// under its per-section key as well as the span's, so while those entries
    /// stay resident the next read of the object serves both from those peeks. Counts one [`BlockRangeStats::metadata_gets`]
    /// when it fetches, never a `probe_miss` (the front is unreachable by any
    /// probe, so counting it there would put a floor under that metric -- see
    /// the `probe_misses` field doc).
    #[allow(clippy::too_many_arguments)]
    async fn place_front_sections(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &footer::LogFooter,
        phase: QueryPhase,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut missing: Vec<SectionDesc> = Vec::new();
        for k in [kind::STREAM_DIR, kind::FIELD_DIR] {
            let Some(desc) = footer.section(k) else {
                continue;
            };
            if asm.covers(desc.offset, desc.offset + desc.len) {
                continue;
            }
            // A section already resident under its OWN per-section cache key
            // (an earlier plan-phase read that fetched it alone, e.g.
            // `plan_section_raw`'s FIELD_DIR read) is placed straight from
            // the cache instead of being folded into the combined span
            // below: asking for the wider span uses a different cache key
            // and would re-fetch live bytes the cache already holds.
            if let Some(cache) = &self.cache {
                let cache_key =
                    CacheKey::new(tenant_hash.0, seg_ref.content_hash, desc.offset, desc.len);
                if let Some(bytes) = cache.get(&cache_key).await {
                    accounting.record_cache_hit();
                    accounting.add_cache_bytes(bytes.len() as u64);
                    let reservation = self.reserve_fetch(bytes.len() as u64)?;
                    self.hold_placement(asm, reservation);
                    asm.place(key, desc.offset, bytes)?;
                    continue;
                }
            }
            missing.push(*desc);
        }
        if missing.is_empty() {
            return Ok(());
        }
        let start = missing.iter().map(|d| d.offset).min().unwrap_or(0);
        let end = missing.iter().map(|d| d.offset + d.len).max().unwrap_or(0);
        let (bytes, live) = self
            .place_extent(
                seg_ref,
                tenant_hash,
                start,
                end - start,
                GetRange::Range(start, end),
                phase,
                pin,
                asm,
                accounting,
            )
            .await?;
        if live {
            stats.metadata_gets += 1;
        }
        // The span's own key is not one the peek above consults, so admit each
        // section under its per-section key too, or a later read with a fresh
        // assembler peeks and misses both again. A lone section's span key is
        // already its per-section key. `insert` records no miss and the bytes
        // were charged once by the GET above. The span entry `cached_extent`
        // admitted stays too, redundant with these two: on a tiered cache a
        // first-touch object writes three small entries instead of one, which
        // is accepted because later reads hit the per-section keys and the
        // span entry ages out.
        if missing.len() > 1
            && let Some(cache) = &self.cache
        {
            let span = [(start, bytes.clone())];
            for desc in &missing {
                let section = resident_slice(&span, desc.offset, desc.len)
                    .ok_or_else(|| corrupt_range(key))?;
                let cache_key =
                    CacheKey::new(tenant_hash.0, seg_ref.content_hash, desc.offset, desc.len);
                cache.insert(cache_key, section).await;
            }
        }
        Ok(())
    }

    /// Place FIELD_DIR (and, when not already resident, STREAM_DIR alongside
    /// it in the same GET -- ADR-2066 decision 1) into `asm` and decode
    /// FIELD_DIR, so the caller can resolve a query's prune-only NumRange arms
    /// and its projection to this object's own column ids before the
    /// candidate set is chosen.
    ///
    /// Before ADR-2066 this fetched FIELD_DIR alone (via
    /// [`place_section`](Self::place_section)), on the reasoning that a query
    /// which later crosses over to a whole-object read never needs STREAM_DIR
    /// fetched here at all. That left STREAM_DIR to
    /// [`place_front_sections`](Self::place_front_sections), called once more
    /// after the crossover decision -- a second GET whenever this path ran,
    /// since FIELD_DIR was already resident and STREAM_DIR was the only
    /// section left for it to coalesce with. Calling `place_front_sections`
    /// here instead brings both sections in the one GET their adjacency
    /// allows: a query that crosses over afterward reads STREAM_DIR bytes it
    /// does not need, the same bridged-bytes-for-requests trade ADR-2066
    /// makes for chunk runs, but a narrow projection of a one-row-group
    /// object -- the case that never crosses over -- costs one front-section
    /// GET instead of two. FIELD_DIR is compressed as a whole section
    /// (docs/log-segment-format.md), the same shape SKIP_IDX is decoded with
    /// just above.
    #[allow(clippy::too_many_arguments)]
    async fn place_and_decode_field_dir(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &footer::LogFooter,
        phase: QueryPhase,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<FieldDir, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let desc = *footer
            .section(kind::FIELD_DIR)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing FIELD_DIR".into())))?;
        self.place_front_sections(
            seg_ref,
            tenant_hash,
            footer,
            phase,
            pin,
            asm,
            accounting,
            stats,
        )
        .await?;
        let stored = asm.slice(key, desc.offset, desc.len)?;
        let raw = self
            .decode_section_on_gate(key, &stored, &desc, accounting)
            .await?;
        FieldDir::decode(&raw, MAX_FIELDS).map_err(|source| corrupt(key, source))
    }

    /// Place one directory section into `asm`, fetching it through the read
    /// cache's single-flight when it is not already covered by an earlier read.
    /// The bytes are the section's exact `[offset, offset+len)` stored form
    /// (crc-verified by the reader on decode) and the cache key is the section's
    /// own extent, so two partitions missing the same section issue one GET
    /// between them rather than one each.
    #[allow(clippy::too_many_arguments)]
    async fn place_section(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        section: &SectionDesc,
        phase: QueryPhase,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let (start, end) = (section.offset, section.offset + section.len);
        if asm.covers(start, end) {
            return Ok(());
        }
        let (_, live) = self
            .place_extent(
                seg_ref,
                tenant_hash,
                section.offset,
                section.len,
                GetRange::Range(start, end),
                phase,
                pin,
                asm,
                accounting,
            )
            .await?;
        if live {
            stats.metadata_gets += 1;
        }
        Ok(())
    }

    /// Fetch the candidate blocks into `asm`: serve each from the per-block cache
    /// when present (re-verifying `block_crc32c` on the cached bytes -- ADR-0046's
    /// corrupt-hit gate, which this admitting funnel owns), coalesce the misses
    /// within `coalesce_gap`, and fetch every coalesced run concurrently through
    /// [`fetch_run`](Self::fetch_run), which splits each response at block
    /// boundaries, verifies each block's crc independently, admits one cache
    /// entry per block, and hands the blocks back to be placed.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_blocks(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        pin: &EtagPin,
        extents: &[BlockExtent],
        phase: QueryPhase,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut missing: Vec<BlockExtent> = Vec::new();
        for ext in extents {
            // Already resident from the probe suffix (a probe wide enough to
            // reach into BLOCKS): verify its crc from the buffer and admit it,
            // but issue no GET. This keeps the probe from being re-fetched block
            // by block, so the block-range GET count stays proportional to the
            // blocks the probe did NOT already carry.
            if asm.covers(ext.abs_start, ext.abs_end()) {
                let block = asm.slice(key, ext.abs_start, ext.len)?;
                verify_block_crc(key, &block, ext)?;
                // Copy only for the cache: without one the verified block is
                // read in place from its placed region.
                if let Some(cache) = &self.cache {
                    let cache_key =
                        CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
                    cache
                        .insert(cache_key, Bytes::copy_from_slice(&block))
                        .await;
                }
                continue;
            }
            let cache_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            if let Some(cache) = &self.cache
                && let Some(bytes) = cache.get(&cache_key).await
            {
                // Corrupt-hit gate (ADR-0046 §4 / ADR-0107 decision 3): a cached
                // block is re-verified against its stored crc before use, exactly
                // as a live fetch of that block is, and fails closed on mismatch.
                verify_block_crc(key, &bytes, ext)?;
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                stats.block_cache_hits += 1;
                let reservation = self.reserve_fetch(bytes.len() as u64)?;
                self.hold_placement(asm, reservation);
                asm.place(key, ext.abs_start, bytes)?;
                continue;
            }
            if self.cache.is_some() {
                accounting.record_cache_miss();
            }
            missing.push(*ext);
        }

        // Every coalesced run concurrently, not one await at a time (mirrors
        // `crate::fetcher::SegmentFetcher::ensure_ranges`' `join_all`). Awaiting
        // the runs in series made `get_limiter` inert: a sequential loop never
        // has more than one GET in flight to bound.
        let runs = coalesce_extents(&missing, self.effective_coalesce_gap());
        // Reserve before `join_all` issues a GET (ADR-1170 decision 2), so a
        // refusal fails typed with zero GETs: the runs' transient wire buffers,
        // held to the end of this call while `fetch_run` copies each block out
        // of them, and the blocks themselves, which `asm` holds for as long as
        // they live.
        let reserved: u64 = runs.iter().map(|r| r.len).fold(0u64, u64::saturating_add);
        let block_bytes: u64 = missing
            .iter()
            .map(|e| e.len)
            .fold(0u64, u64::saturating_add);
        let placed = self.reserve_fetch(block_bytes)?;
        let _transient = self.reserve_fetch(reserved)?;
        let outcomes = futures::future::join_all(runs.iter().map(|run| {
            let blocks: Vec<BlockExtent> = missing
                .iter()
                .copied()
                .filter(|e| e.abs_start >= run.abs_start && e.abs_end() <= run.abs_end())
                .collect();
            self.fetch_run(seg_ref, tenant_hash, pin, *run, blocks, phase, accounting)
        }))
        .await;
        self.hold_placement(asm, placed);
        for outcome in outcomes {
            let run = outcome?;
            stats.block_range_gets += run.gets;
            stats.block_bytes_fetched = stats.block_bytes_fetched.saturating_add(run.bytes);
            for (start, bytes) in run.blocks {
                asm.place(key, start, bytes)?;
            }
        }
        Ok(())
    }

    /// Fetch one coalesced run's blocks with one range GET, split at block
    /// boundaries and verified block by block.
    ///
    /// With a cache attached the run is single-flighted on its FIRST block's
    /// cache key: the partitions striping one segment resolve the identical
    /// candidate set from the same skip index, so they produce the identical runs
    /// and collapse onto one real GET instead of one each (ADR-0102 decision 1's
    /// premise, which the block-range path would otherwise break). The key is a
    /// block's own extent, never the coalesced run's, because cache admission
    /// here is per block and a run's gap bytes are never cached (ADR-0107
    /// decision 3): the leader verifies every block in the run and admits one
    /// entry per block before it returns, so a follower finds the run's other
    /// blocks resident and issues no GET of its own. A follower that still
    /// misses one -- evicted in the meantime, or larger than the cache's
    /// single-entry cap -- fetches that block alone, still single-flighted.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_run(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        pin: &EtagPin,
        run: BlockExtent,
        blocks: Vec<BlockExtent>,
        phase: QueryPhase,
        accounting: &QueryAccounting,
    ) -> Result<RunOutcome, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let range = GetRange::Range(run.abs_start, run.abs_end());
        let Some(cache) = &self.cache else {
            let got = self
                .store_get_pinned(key, range, phase, pin, accounting)
                .await?;
            let blocks = split_run(key, run.abs_start, &got.data, &blocks)?;
            return Ok(RunOutcome {
                blocks,
                gets: 1,
                bytes: got.data.len() as u64,
            });
        };
        let Some(lead) = blocks.first().copied() else {
            // A run with no block in it cannot happen (runs are built from the
            // blocks themselves), and fetching bytes no block claims would admit
            // exactly the unverifiable gap bytes decision 3 forbids.
            return Ok(RunOutcome::default());
        };
        let lead_key = CacheKey::new(
            tenant_hash.0,
            seg_ref.content_hash,
            lead.abs_start,
            lead.len,
        );
        // Set by our own closure, so `Some` after the await means this call's
        // own GET fetched the run and it already holds every block; `None`
        // means the closure never ran: this call followed another caller's
        // in-flight GET, or it led the flight and the leader's RAM recheck
        // served the lead block from a flight that finished after the peek.
        let led: std::sync::OnceLock<(Vec<(u64, Bytes)>, u64)> = std::sync::OnceLock::new();
        // `fetch_peeked`, not `get_or_fetch`: fetch_blocks already peeked every
        // block of this run with `cache.get` (the one accounted miss), so a
        // second read-through here would re-peek and double-count the miss on
        // the tiered tier. Both tiers' `fetch_peeked` keep single-flight
        // (concurrent partitions collapse onto one leader), and both leaders
        // recheck RAM, uncounted, before running the closure. `led` is set
        // inside the closure iff THIS call ran it, so `led.get().is_some()`
        // says whether this call's own GET produced the run, not whether it
        // led the flight.
        let lead_bytes = cache
            .fetch_peeked(lead_key, || async {
                let got = self
                    .store_get_pinned(key, range, phase, pin, accounting)
                    .await
                    .map_err(to_cache_error)?;
                let split =
                    split_run(key, run.abs_start, &got.data, &blocks).map_err(to_cache_error)?;
                let Some((_, lead_bytes)) = split.first().cloned() else {
                    return Err(to_cache_error(corrupt_range(key)));
                };
                // One entry per block, all verified above. The lead block's own
                // admission is `get_or_fetch`'s, under the key it was called
                // with, so it is admitted exactly once.
                for (start, bytes) in split.iter().skip(1) {
                    cache
                        .insert(
                            CacheKey::new(
                                tenant_hash.0,
                                seg_ref.content_hash,
                                *start,
                                bytes.len() as u64,
                            ),
                            bytes.clone(),
                        )
                        .await;
                }
                let _ = led.set((split, got.data.len() as u64));
                Ok(lead_bytes)
            })
            .await
            .map_err(|err| from_cache_error(key, err))?;
        if let Some((split, bytes)) = led.get() {
            return Ok(RunOutcome {
                blocks: split.clone(),
                gets: 1,
                bytes: *bytes,
            });
        }
        // Not this call's own fetch: the lead block rode another caller's
        // flight or was served from the cache (a disk hit of a concurrent
        // `get_or_fetch`, or the RAM tier after a finished flight), so it is
        // verified like every other cache-served block before use. The leader
        // admitted the rest of the run before it returned.
        verify_block_crc(key, &lead_bytes, &lead)?;
        let mut out = Vec::with_capacity(blocks.len());
        out.push((lead.abs_start, lead_bytes));
        let mut outcome = RunOutcome::default();
        // Every block of the run already recorded its one query-accounting
        // outcome, a miss, when `fetch_blocks` peeked it. Served from the cache
        // now, it stays a miss with no GET (docs/guides/caching.md), and the
        // re-peek is uncounted so the tier metrics keep that one lookup too;
        // only a block this call's own GET fetches adds a request.
        for ext in blocks.iter().skip(1) {
            let block_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            if let Some(bytes) = cache.peek_uncounted(&block_key).await {
                verify_block_crc(key, &bytes, ext)?;
                out.push((ext.abs_start, bytes));
                continue;
            }
            // `fetch_peeked` for the same reason as the lead above: this block
            // was just peeked with `cache.get`, so the deferred fetch must not
            // re-count the miss on the tiered tier. Bytes this call's closure
            // did not produce (another caller's flight, or a leader's RAM
            // recheck) are verified after it returns.
            let fetched_here = std::sync::atomic::AtomicBool::new(false);
            let bytes = cache
                .fetch_peeked(block_key, || async {
                    let got = self
                        .store_get_pinned(
                            key,
                            GetRange::Range(ext.abs_start, ext.abs_end()),
                            phase,
                            pin,
                            accounting,
                        )
                        .await
                        .map_err(to_cache_error)?;
                    verify_block_crc(key, &got.data, ext).map_err(to_cache_error)?;
                    fetched_here.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(got.data)
                })
                .await
                .map_err(|err| from_cache_error(key, err))?;
            if fetched_here.load(std::sync::atomic::Ordering::Relaxed) {
                // Only a block this call's own GET produced is a store read.
                outcome.gets += 1;
                outcome.bytes = outcome.bytes.saturating_add(bytes.len() as u64);
            } else {
                verify_block_crc(key, &bytes, ext)?;
            }
            out.push((ext.abs_start, bytes));
        }
        outcome.blocks = out;
        Ok(outcome)
    }

    /// Split whole-object bytes (the coverage-crossover path) at block boundaries
    /// and admit one verified cache entry per candidate block, so a later
    /// partition's block-range fetch composes with this whole-object read
    /// (ADR-0107 decision 3). A block whose crc does not verify is simply not
    /// admitted; the reader's own decode still gates correctness of what is
    /// returned.
    ///
    /// Async because each admission may write a file on the disk tier, and
    /// [`ReadCache::insert`] moves that write to the blocking pool (issue
    /// #1891). Its only caller is already async.
    async fn admit_blocks_from_whole(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        object: &LogObjectBytes,
        extents: &[BlockExtent],
    ) {
        let Some(cache) = &self.cache else {
            return;
        };
        for ext in extents {
            let Ok(block) = object.read(ext.abs_start, ext.len) else {
                continue;
            };
            if crc32c::crc32c(&block) != ext.crc32c {
                continue;
            }
            let cache_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            let block = Bytes::copy_from_slice(&block);
            cache.insert(cache_key, block).await;
        }
    }
}

/// One coalesced run's fetched blocks (`(abs_start, bytes)`, crc-verified) plus
/// the store cost this caller paid for them: `gets` real range GETs moving
/// `bytes` stored bytes. A block served from the cache here adds nothing to
/// [`BlockRangeStats::block_cache_hits`]: its `fetch_blocks` peek already
/// decided its one hit-or-miss outcome.
///
/// A block counts as fetched only when this call ran the closure, so a
/// single-flight follower that rode another caller's GET, and a leader served
/// by the RAM recheck, which runs no closure, both report zero gets and zero
/// bytes here, the same late-serve figures [`BlockRangeFetcher::cached_extent`]
/// and the whole-object funnels report on either cache kind.
#[derive(Default)]
struct RunOutcome {
    blocks: Vec<(u64, Bytes)>,
    gets: u64,
    bytes: u64,
}

/// The etag every LIVE GET of one fetch sequence is checked against (ADR-0107
/// decision 1's mandatory etag pinning). Whichever live GET completes first pins
/// it -- normally the suffix probe, or the first section/block GET when the probe
/// was served from the read cache -- and every live GET after it must report the
/// same etag or the fetch fails with [`LogFetchError::EtagChanged`] rather than
/// assembling bytes from two object states.
///
/// Cache-served bytes are not checked against it and need no check: a cache key
/// carries the object's `content_hash`, so an entry is by construction bytes of
/// this exact content rather than of whatever the store holds now. What the pin
/// has to rule out is a sequence of LIVE GETs spanning a replacement, which is
/// exactly what it still does.
#[derive(Default)]
struct EtagPin(std::sync::OnceLock<Etag>);

impl EtagPin {
    /// Pin `got` if nothing is pinned yet, otherwise require it to match.
    fn check(&self, key: &str, got: &Etag) -> Result<(), LogFetchError> {
        if self.0.get_or_init(|| got.clone()) == got {
            return Ok(());
        }
        Err(LogFetchError::EtagChanged {
            key: key.to_string(),
        })
    }
}

/// A live GET must return exactly the extent that was asked for: a short read
/// would be placed at the right offset with the wrong bytes after it, and cached
/// under a key claiming a length it does not have.
fn check_extent_len(key: &str, got: usize, want: u64) -> Result<(), LogFetchError> {
    if got as u64 == want {
        return Ok(());
    }
    Err(corrupt(
        key,
        LogSegError::Corrupted(format!("short read: got {got} bytes of {want}")),
    ))
}

/// Split one coalesced run's response at block boundaries, verifying each
/// block's `block_crc32c` independently before the caller can admit or place it.
/// The gap bytes between blocks are dropped here: never cached, never
/// interpreted (ADR-0107 decision 3).
fn split_run(
    key: &str,
    run_start: u64,
    data: &Bytes,
    blocks: &[BlockExtent],
) -> Result<Vec<(u64, Bytes)>, LogFetchError> {
    let mut out = Vec::with_capacity(blocks.len());
    for ext in blocks {
        let offset = ext
            .abs_start
            .checked_sub(run_start)
            .ok_or_else(|| corrupt_range(key))?;
        let rel = usize::try_from(offset).map_err(|_| corrupt_range(key))?;
        let rel_end = rel
            .checked_add(usize::try_from(ext.len).map_err(|_| corrupt_range(key))?)
            .ok_or_else(|| corrupt_range(key))?;
        let block = data.get(rel..rel_end).ok_or_else(|| corrupt_range(key))?;
        verify_block_crc(key, block, ext)?;
        out.push((ext.abs_start, Bytes::copy_from_slice(block)));
    }
    Ok(out)
}

/// This module's error into the cache's single-flight error channel, preserving
/// the class: a follower waiting on a leader's fetch must see the same store
/// error, etag change, or hard corruption the leader saw, never a flattened one.
fn to_cache_error(err: LogFetchError) -> crate::fetcher::CacheFetchError {
    match err {
        LogFetchError::Store { source, .. } => {
            crate::fetcher::CacheFetchError::Store(Arc::new(source))
        }
        LogFetchError::EtagChanged { key } => crate::fetcher::CacheFetchError::EtagChanged { key },
        LogFetchError::Corrupt { key, source } => crate::fetcher::CacheFetchError::Corrupt {
            key,
            message: source.to_string(),
        },
        // Unreachable in practice: every fetch-memory reservation (ADR-1170
        // decision 2) is taken BEFORE the `get_or_fetch` closure this channel
        // carries, so a refusal fails the caller before any cache single-flight
        // begins and never travels to a follower. Mapped to a transient store
        // error so the exhaustive match holds without inventing a cache-channel
        // variant for a class that cannot arrive here.
        LogFetchError::FetchMemoryExhausted { .. } => {
            crate::fetcher::CacheFetchError::Store(Arc::new(StoreError::Transient(
                "fetch memory exhausted inside cache single-flight (unreachable)".to_string(),
            )))
        }
        // A carry mismatch is refused before any fetch, so it never reaches a
        // single-flight waiter today. Preserved as hard corruption rather than
        // flattened, so it stays in its class if a carry ever fronts the cache.
        carry @ LogFetchError::CarryMismatch { .. } => {
            let key = match &carry {
                LogFetchError::CarryMismatch { key, .. } => key.clone(),
                _ => String::new(),
            };
            crate::fetcher::CacheFetchError::Corrupt {
                key,
                message: carry.to_string(),
            }
        }
    }
}

/// The inverse of [`to_cache_error`], plus the single-flight channel's own
/// `LeaderLost` (a leader whose future was cancelled or panicked before
/// producing a result), which is transient and retryable.
fn from_cache_error(
    key: &str,
    err: SingleFlightError<crate::fetcher::CacheFetchError>,
) -> LogFetchError {
    match err {
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::Store(source)) => {
            LogFetchError::Store {
                key: key.to_string(),
                source: crate::fetcher::clone_store_error(&source),
            }
        }
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::EtagChanged { key }) => {
            LogFetchError::EtagChanged { key }
        }
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::Corrupt { key, message }) => {
            LogFetchError::Corrupt {
                key,
                source: LogSegError::Corrupted(message),
            }
        }
        // Unreachable from this closure: it holds no `ReadLimits` and never
        // admits against a request or byte budget, so it never constructs
        // `BudgetRefused`. Handled explicitly rather than through a wildcard
        // so a future budget check added here cannot silently fall through
        // as a store error.
        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::BudgetRefused {
            key,
            message,
        }) => LogFetchError::Store {
            key,
            source: StoreError::Transient(format!(
                "cache single-flight closure reported a budget refusal, which the RLOG funnel \
                 never produces: {message}"
            )),
        },
        SingleFlightError::LeaderLost => LogFetchError::Store {
            key: key.to_string(),
            source: StoreError::Transient(
                "cache single-flight leader lost before producing a result".to_string(),
            ),
        },
    }
}

/// Verify one block's bytes against its stored `block_crc32c`; a mismatch is a
/// hard [`LogFetchError::Corrupt`], never silently-wrong data (ADR-0107 decision
/// 3). The block is the smallest unit the RLOG format can verify.
fn verify_block_crc(key: &str, bytes: &[u8], ext: &BlockExtent) -> Result<(), LogFetchError> {
    if bytes.len() as u64 != ext.len {
        return Err(corrupt(
            key,
            LogSegError::Corrupted("block length mismatch".into()),
        ));
    }
    if crc32c::crc32c(bytes) != ext.crc32c {
        return Err(corrupt(
            key,
            LogSegError::Corrupted("block crc mismatch".into()),
        ));
    }
    Ok(())
}

/// Merge candidate block extents into ordered, non-overlapping runs, joining two
/// whole-block extents whose gap is at most `max_gap` into one range (the RLOG
/// analogue of `crate::fetcher::coalesce_ranges`). Each returned `BlockExtent`
/// describes a coalesced GET's `[abs_start, abs_start+len)`; its `crc32c` is not
/// meaningful (coalesced ranges are split back to per-block extents by the
/// caller and each block's own crc is verified there).
fn coalesce_extents(extents: &[BlockExtent], max_gap: u64) -> Vec<BlockExtent> {
    let ranges: Vec<ByteExtent> = extents
        .iter()
        .map(|e| ByteExtent {
            abs_start: e.abs_start,
            len: e.len,
        })
        .collect();
    coalesce_byte_extents(&ranges, max_gap)
        .into_iter()
        .map(|r| BlockExtent {
            abs_start: r.abs_start,
            len: r.len,
            crc32c: 0,
        })
        .collect()
}

/// Most chunk-run GETs one version-4 read of an object at `level` issues
/// (ADR-2066 decision 1). An L0 flush is capped at
/// [`MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT`], as the metrics path caps an L0
/// segment's page ranges; an L1 part is left unbounded for the same reason it
/// is there: a compacted part can be far larger than a flush, so bridging its
/// runs down to the cap would move most of the object.
fn chunk_run_cap(level: &SegmentLevel) -> usize {
    match level {
        SegmentLevel::L0 => MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT,
        SegmentLevel::L1 { .. } => usize::MAX,
    }
}

/// [`coalesce_extents`] over plain byte extents: the same rule (sort by start,
/// join two runs whose gap is at most `max_gap`), applied to version 4's
/// page-range fetch units. This is the one place the gap policy lives, so the
/// version-3 block path and the version-4 chunk path cannot drift apart on it.
fn coalesce_byte_extents(extents: &[ByteExtent], max_gap: u64) -> Vec<ByteExtent> {
    let mut ranges: Vec<(u64, u64)> = extents.iter().map(|e| (e.abs_start, e.abs_end())).collect();
    ranges.sort_by_key(|r| r.0);
    let mut out: Vec<ByteExtent> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = out.last_mut()
            && start <= last.abs_end().saturating_add(max_gap)
        {
            let new_end = last.abs_end().max(end);
            last.len = new_end - last.abs_start;
            continue;
        }
        out.push(ByteExtent {
            abs_start: start,
            len: end - start,
        });
    }
    out
}

/// Sorts `extents` and merges every overlapping or touching pair, giving the
/// ordered, disjoint `(start, end)` list [`gap_crosses_fence`] searches.
fn merge_fences(extents: &[ByteExtent]) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = extents.iter().map(|e| (e.abs_start, e.abs_end())).collect();
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match out.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => out.push((start, end)),
        }
    }
    out
}

/// Whether the gap `[from, to)` holds any byte of a fence. `fences` is the
/// ordered, disjoint list [`merge_fences`] returns.
fn gap_crosses_fence(fences: &[(u64, u64)], from: u64, to: u64) -> bool {
    let at = fences.partition_point(|f| f.1 <= from);
    fences.get(at).is_some_and(|f| f.0 < to)
}

/// [`coalesce_byte_extents`] that never joins two extents across a gap holding
/// fence bytes, whatever `max_gap` says. With no fences it is that function.
fn coalesce_fenced(extents: &[ByteExtent], max_gap: u64, fences: &[(u64, u64)]) -> Vec<ByteExtent> {
    let mut ranges: Vec<(u64, u64)> = extents.iter().map(|e| (e.abs_start, e.abs_end())).collect();
    ranges.sort_by_key(|r| r.0);
    let mut out: Vec<ByteExtent> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = out.last_mut()
            && start <= last.abs_end().saturating_add(max_gap)
            && !gap_crosses_fence(fences, last.abs_end(), start)
        {
            let new_end = last.abs_end().max(end);
            last.len = new_end - last.abs_start;
            continue;
        }
        out.push(ByteExtent {
            abs_start: start,
            len: end - start,
        });
    }
    out
}

/// [`bound_runs`] that never bridges a gap holding fence bytes. The smallest
/// bridgeable gaps go first, ties earliest first, exactly as `bound_runs`
/// picks them; when too few gaps are bridgeable to reach `max_runs`, the
/// result keeps more runs than that rather than cross a fence.
fn bound_runs_fenced(
    runs: Vec<(u64, u64)>,
    max_runs: usize,
    fences: &[(u64, u64)],
) -> Vec<(u64, u64)> {
    if fences.is_empty() {
        return bound_runs(runs, max_runs);
    }
    let max_runs = max_runs.max(1);
    if runs.len() <= max_runs {
        return runs;
    }
    let mut gaps: Vec<(u64, usize)> = runs
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| !gap_crosses_fence(fences, pair[0].1, pair[1].0))
        .map(|(i, pair)| (pair[1].0.saturating_sub(pair[0].1), i))
        .collect();
    gaps.sort_unstable();
    let mut bridged = vec![false; runs.len().saturating_sub(1)];
    for (_, i) in gaps.into_iter().take(runs.len() - max_runs) {
        bridged[i] = true;
    }
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(max_runs);
    let mut bridge_previous = false;
    for (run, bridge_next) in runs.into_iter().zip(bridged.into_iter().chain([false])) {
        match out.last_mut() {
            Some(last) if bridge_previous => last.1 = last.1.max(run.1),
            _ => out.push(run),
        }
        bridge_previous = bridge_next;
    }
    out
}

/// The absolute byte extents of exactly the pages the surviving blocks
/// `candidates` hold for the columns `selected` keeps, over every row group
/// holding at least one survivor (ADR-0699 decision 5). `selected` is `None`
/// for an all-columns read.
///
/// `candidates` are whole-object level-0 block indices, strictly ascending, as
/// [`SkipIndex::candidate_blocks`] returns them. PAGE_DIR's decode proves the
/// groups partition the object's blocks into consecutive runs from block 0, so
/// one forward pass over the groups splits the candidates between them with no
/// per-block search. A candidate no group claims is corruption (the reader's
/// own open checks the same invariant from the other side, PAGE_DIR block count
/// against the skip index's), never a silently dropped block.
fn projected_page_extents(
    key: &str,
    page_dir: &PageDir,
    blocks_offset: u64,
    candidates: &[usize],
    selected: Option<&HashSet<u32>>,
) -> Result<Vec<ByteExtent>, LogFetchError> {
    let mut out = Vec::new();
    let mut at = 0usize;
    for (gi, group) in page_dir.groups.iter().enumerate() {
        let group_end = u64::from(group.first_block) + u64::from(group.block_count);
        let mut within: Vec<u32> = Vec::new();
        while let Some(&b) = candidates.get(at) {
            if b as u64 >= group_end {
                break;
            }
            let b = u32::try_from(b).map_err(|_| corrupt_range(key))?;
            let rel = b
                .checked_sub(group.first_block)
                .ok_or_else(|| corrupt_range(key))?;
            within.push(rel);
            at += 1;
        }
        if within.is_empty() {
            continue;
        }
        let ranges = page_dir
            .projected_page_ranges(gi, &within, selected)
            .ok_or_else(|| corrupt_range(key))?;
        for (offset, len) in ranges {
            let abs_start = blocks_offset
                .checked_add(offset)
                .ok_or_else(|| corrupt_range(key))?;
            out.push(ByteExtent { abs_start, len });
        }
    }
    if at != candidates.len() {
        return Err(corrupt(
            key,
            LogSegError::Corrupted("candidate block outside the page directory".into()),
        ));
    }
    Ok(out)
}

/// Whether a suffix probe of `suffix` bytes over an object of `total_size`
/// bytes covers `desc` entirely. This asks about the probe WINDOW, not about
/// what any particular read has resident, so it answers "would a longer probe
/// have saved this GET" rather than "did the cache hold it" (issue #766, the
/// `probe_misses` counter).
fn probe_window_covers(desc: &SectionDesc, total_size: u64, suffix: u64) -> bool {
    let probe_start = total_size.saturating_sub(suffix);
    desc.offset >= probe_start && desc.offset.saturating_add(desc.len) <= total_size
}

/// The canonical-byte needle for one stream-attribute equality: the single
/// `(key, value)` entry as it appears inside a larger canonical attribute set,
/// i.e. `canonical_attr_bytes([(key, value)])` with its leading one-entry count
/// varint stripped. The count of a single-entry set is `1`, a one-byte varint,
/// so exactly one leading byte is removed. The result is never empty in
/// practice; if it somehow were, [`blob_contains`] treats it as matching
/// nothing rather than everything.
fn stream_attr_needle(filter: &StreamAttrEquals) -> Vec<u8> {
    let full = canonical_attr_bytes(std::slice::from_ref(&(
        filter.key.clone(),
        filter.value.clone(),
    )));
    // `encode_attrs` writes the entry count first; for one entry it is the
    // single byte 0x01. Everything after is `len(key) key encode_value(value)`.
    full.get(1..).unwrap_or(&[]).to_vec()
}

/// True if `needle` occurs as a contiguous sub-sequence of `blob`. An empty
/// needle matches nothing: in a filter-matching context "no bytes to find" must
/// never mean "found in every stream", so a degenerate needle fails closed.
fn blob_contains(blob: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return false;
    }
    if needle.len() > blob.len() {
        return false;
    }
    blob.windows(needle.len()).any(|w| w == needle)
}

/// The log path's `decode` phase span, shared by the collecting and the
/// streaming funnel so both report the same fields (docs/guides/tracing.md).
fn decode_span() -> tracing::Span {
    tracing::debug_span!(
        "decode",
        signal = "logs",
        blocks_scanned = tracing::field::Empty,
        blocks_total = tracing::field::Empty,
        decompressed_bytes = tracing::field::Empty,
    )
}

fn corrupt(key: &str, source: LogSegError) -> LogFetchError {
    LogFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}

/// A failed RLOG decode job (ADR-1702 decision 2): a panic is the decode's own
/// error, a job that never ran is transient.
fn log_gate_failed(key: &str, err: CpuGateError) -> LogFetchError {
    match err {
        CpuGateError::Panicked => {
            corrupt(key, LogSegError::Corrupted(format!("read CPU gate: {err}")))
        }
        CpuGateError::Cancelled | CpuGateError::Closed => LogFetchError::Store {
            key: key.to_string(),
            source: gate_not_run(err),
        },
    }
}

/// The job size of a scan open on the read gate: the uncompressed lengths of
/// the directory sections `RlogReader::new` decodes, plus POSTINGS when the
/// object carries it, since the open probes it. With `block_sizes`, PAGE_DIR
/// counts twice, for [`max_block_uncompressed_len`]'s own read of it; that
/// second decode is charged to the query's accounting as well, so the job's
/// size and the bytes it reports are the same work.
/// `u64::MAX` when the footer does not open.
fn open_job_len<S: ByteSource + ?Sized>(bytes: &S, block_sizes: bool) -> u64 {
    let Ok(footer) = footer::open_source(bytes) else {
        return u64::MAX;
    };
    let len = |k| footer.section(k).map_or(0, |desc| desc.uncomp_len);
    let mut total = [
        kind::STREAM_DIR,
        kind::FIELD_DIR,
        kind::SKIP_IDX,
        kind::PAGE_DIR,
        kind::POSTINGS,
    ]
    .into_iter()
    .fold(0u64, |acc, k| acc.saturating_add(len(k)));
    if block_sizes {
        total = total.saturating_add(len(kind::PAGE_DIR));
    }
    total
}

/// A [`LogSegmentScan`] whose cursor went with a gated decode that was
/// abandoned: the caller dropped the future while the job was queued or
/// running, so no classified failure exists to repeat. Permanent: the blocks it
/// had not decoded cannot be read through this scan, and skipping them would
/// return a partial segment. A cursor lost to a gate failure reports that
/// failure's own class instead ([`LogSegmentScan::lost_error`]), so a panicked
/// decode does not turn into a permanent store error the HTTP layer redacts.
fn scan_lost(key: &str) -> LogFetchError {
    LogFetchError::Store {
        key: key.to_string(),
        source: StoreError::Permanent(
            "read CPU gate: the scan's cursor was lost with a failed block decode".to_string(),
        ),
    }
}

/// The largest uncompressed block in `bytes`' PAGE_DIR, summed over every
/// page of the block and each row-group dictionary page it decodes through.
/// `u64::MAX` when the directory cannot be read, so a
/// block of unknown size is never taken for a small one; the scan's own open
/// already refused such an object.
///
/// This decodes PAGE_DIR a second time, after the open's own decode of it, and
/// charges that decode to `accounting` like every other: the bytes zstd
/// produced here count against the query's `TooManyBytesScanned` budget the
/// same as the ones the open produced.
///
/// Only [`open_scan_on_gate`](LogSegmentFetcher::open_scan_on_gate)'s gated
/// closure, which has no decoded [`SegmentDirectories`] of its own, calls
/// this. [`open_scan_on_gate_from_decoded`](LogSegmentFetcher::open_scan_on_gate_from_decoded)
/// has one already and reads
/// [`max_block_uncompressed_len_from_page_dir`] instead (ADR-2414 decision
/// A1): the job size for a later block decode must come from the carried
/// `PageDir`, never a third decode of it.
fn max_block_uncompressed_len<S: ByteSource + ?Sized>(
    bytes: &S,
    cfg: &RlogConfig,
    accounting: &QueryAccounting,
) -> u64 {
    let page_dir = footer::open_source(bytes).ok().and_then(|footer| {
        let desc = *footer.section(kind::PAGE_DIR)?;
        let raw = read_section_accounted_from(bytes, &desc, cfg, accounting).ok()?;
        PageDir::decode(&raw).ok()
    });
    let Some(page_dir) = page_dir else {
        return u64::MAX;
    };
    max_block_uncompressed_len_from_page_dir(&page_dir)
}

/// [`max_block_uncompressed_len`]'s computation, over an already-decoded
/// [`PageDir`] and with no further section read or accounting charge
/// (ADR-2414 decision A1).
fn max_block_uncompressed_len_from_page_dir(page_dir: &PageDir) -> u64 {
    let mut largest = 0u64;
    for group in &page_dir.groups {
        let mut per_block = vec![0u64; group.block_count as usize];
        for chunk in &group.chunks {
            // A row-group dictionary page decodes alongside every block of its
            // chunk, so each such block is charged it once.
            let dict_len = chunk.dict_page().map_or(0, |d| d.uncomp_len);
            let mut charged: Option<u32> = None;
            for page in &chunk.pages {
                if let Some(len) = per_block.get_mut(page.block as usize) {
                    *len = len.saturating_add(page.uncomp_len);
                    if charged != Some(page.block) {
                        *len = len.saturating_add(dict_len);
                        charged = Some(page.block);
                    }
                }
            }
        }
        largest = per_block.into_iter().fold(largest, u64::max);
    }
    largest
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod plan_fast_path_tests {
    //! Tests for the predicate-free plan fast path (#693 part 2): a query with
    //! no block-level predicate whose ts window fully contains the segment span
    //! is planned from `LogFooter.block_count` via the ADR-0107 suffix probe
    //! alone, with no block-range fetch or decode.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{FieldSel, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const CONTENT_HASH: [u8; 32] = [9u8; 32];
    const KEY: &str = "t/seg.rlog";

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn record(ts: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("request.id".to_string(), AttrValue::Str(format!("r{ts}")))],
        }
    }

    /// One block per record, so `n` records span `n` blocks.
    fn build_object(records: &[LogRecord]) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// Bytes after the BLOCKS section: SKIP_IDX/BLOOM/POSTINGS then footer and
    /// trailer. A probe suffix of exactly this length covers the whole tail
    /// (footer parses with no range chase) yet reaches no block byte.
    fn tail_len(bytes: &[u8]) -> u64 {
        let f = footer::open(bytes).expect("footer");
        let b = f.section(kind::BLOCKS).expect("BLOCKS");
        bytes.len() as u64 - (b.offset + b.len)
    }

    /// Stored bytes of the two front directory sections (STREAM_DIR and
    /// FIELD_DIR): the part of a segment's directories the suffix probe does not
    /// cover, which `plan_segment` reads with one extra range GET so the
    /// segment's decoded directories can be carried to every later open
    /// (ADR-2414 decision A1).
    fn front_dirs_len(bytes: &[u8]) -> u64 {
        let f = footer::open(bytes).expect("footer");
        [kind::STREAM_DIR, kind::FIELD_DIR]
            .iter()
            .map(|k| f.section(*k).expect("front section").len)
            .sum()
    }

    async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// Fetcher whose suffix probe reads exactly the object tail, so the fast
    /// path's footer read is measurable and the slow path (routed through the
    /// block-range fetcher by `block_range_threshold = 0`) reads block bytes on
    /// top of it.
    fn fetcher(store: Arc<MemoryStore>, tail: u64) -> LogSegmentFetcher {
        LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(0)
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(tail)
                    .with_whole_object_threshold(0),
            )
    }

    /// (Test b) The fast path fires for a predicate-free, fully-contained query:
    /// the returned count is the block count, the stats show nothing scanned,
    /// and only the tail probe plus the two front directory sections crossed the
    /// wire -- never a block-range fetch sized to the object.
    ///
    /// Non-vacuity: deleting the fast-path branch in `plan_segment` (the
    /// `if seg_ref.object_size > 0 && query.is_block_predicate_free() ...`
    /// block that `return`s `plan_segment_fast(...)`) routes this same query
    /// through the block-range slow path, whose all-block candidate set trips
    /// the coverage crossover into a whole-object GET, so `total_s3_bytes` jumps
    /// from `tail + front` to roughly `tail + object_size`. The
    /// `read == tail + front` assertion then fails. The `predicate_present_cases_take_the_slow_path` test below
    /// exercises exactly that slow path and shows the larger byte count directly.
    #[tokio::test]
    async fn fast_path_reads_the_probe_and_the_front_directory_sections() {
        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let tail = tail_len(&bytes);
        let front = front_dirs_len(&bytes);
        assert!(
            tail < total,
            "the object must carry a nonempty BLOCKS section"
        );

        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher(store, tail);
        let acc = QueryAccounting::new();

        // Predicate-free, ts window strictly contains [min, max].
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let (indices, _dirs, stats, _footer, _carried) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");
        let count = indices.len();

        assert_eq!(count, N, "survivor count is the segment block count");
        assert_eq!(stats.blocks_total, N as u32);
        assert_eq!(stats.blocks_after_skip, N as u32);
        assert_eq!(stats.blocks_after_postings, N as u32);
        assert_eq!(stats.blocks_after_bloom, N as u32);
        assert_eq!(stats.blocks_scanned, 0);
        assert_eq!(stats.pages_decoded, 0);
        assert!(!stats.bloom_degraded && !stats.postings_degraded);

        let read = acc.snapshot().total_s3_bytes();
        assert_eq!(
            read,
            tail + front,
            "fast path reads the footer probe ({tail} B) and the front directory \
             sections ({front} B), not the {total} B object"
        );
    }

    /// (Test b, boundary) Containment is inclusive: a ts window whose bounds
    /// equal the segment span exactly still takes the fast path.
    #[tokio::test]
    async fn fast_path_fires_on_inclusive_boundary() {
        const N: usize = 4;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let tail = tail_len(&bytes);
        let front = front_dirs_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher(store, tail);
        let acc = QueryAccounting::new();

        // Exact span bounds: ts_min == min_event_ts_ns, ts_max == max_event_ts_ns.
        let query = LogQuery::new(seg.min_event_ts_ns, seg.max_event_ts_ns);
        let (indices, _dirs, _stats, _footer, _carried) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");
        let count = indices.len();
        assert_eq!(count, N);
        assert_eq!(
            acc.snapshot().total_s3_bytes(),
            tail + front,
            "fast path fired"
        );
    }

    /// (Test c) Every case that is NOT predicate-free-and-contained goes through
    /// the real block-range fetch and returns the same survivor count today's
    /// code produces. Each asserts more than the tail was read (the block-range
    /// path ran) and the expected survivor count.
    #[tokio::test]
    async fn predicate_present_cases_take_the_slow_path() {
        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let tail = tail_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher(store, tail);

        // (i) A content predicate present. "hello" is in every block's body, so
        // no block is pruned: same survivor count (N) as the fast path, but via
        // the real fetch.
        let q = LogQuery::new(i64::MIN, i64::MAX).with_content(Predicate::HasWord {
            field: FieldSel::Body,
            word: "hello".into(),
        });
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(count, N, "content: no block pruned");
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "content: block-range fetch ran"
        );

        // (ii) A stream-attribute filter present. All records share one stream,
        // so all blocks survive: count N, via the real fetch.
        let q = LogQuery::new(i64::MIN, i64::MAX).with_stream_attr(StreamAttrEquals::new(
            "service.name",
            AttrValue::Str("svc".into()),
        ));
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(count, N, "stream_attr: single stream, all blocks survive");
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "stream_attr: block-range fetch ran"
        );

        // (iii) A non-empty erasure list. Erasure filters rows, not blocks, so
        // the survivor count is unchanged (N), but the fast path must decline.
        let q = LogQuery::new(i64::MIN, i64::MAX).with_erasure(vec![ErasurePredicate::windowless(
            vec![("request.id".into(), "r0".into())],
        )]);
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(count, N, "erasure: block count unchanged");
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "erasure: block-range fetch ran"
        );

        // (iv) A ts window that overlaps but does not fully contain the span:
        // [2, +inf) drops blocks 0 and 1, so real block-level ts pruning must
        // run and the survivor count is N-2, not N.
        let q = LogQuery::new(2, i64::MAX);
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(count, N - 2, "partial overlap: ts pruning ran");
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "partial overlap: block-range fetch ran"
        );

        // (v) A prune-only predicate present (the ClickBench `attrs['k']='v'`
        // shape, via LogsPushdown::prune). "hello" is in every block's body, so
        // no block is pruned: same survivor count (N) as the fast path, but via
        // the real fetch. `prune` is the one guarded field with no case above
        // it, and it is the field that actually removes blocks
        // (`candidates.retain` in the reader) on the real ClickBench workload.
        let q = LogQuery::new(i64::MIN, i64::MAX).with_prune(Predicate::HasWord {
            field: FieldSel::Body,
            word: "hello".into(),
        });
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(count, N, "prune: no block pruned");
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "prune: block-range fetch ran"
        );
    }

    /// (Test c, threshold) An object at or below `block_range_threshold`,
    /// predicate-free and fully contained, still takes the slow path: the fast
    /// path's `fetch_footer` has no whole-object crossover of its own, so
    /// firing it at or below the threshold would read the object under a
    /// different cache key than the whole-object path already uses, costing an
    /// extra GET instead of saving one.
    #[tokio::test]
    async fn small_object_at_or_below_threshold_takes_the_slow_path() {
        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let tail = tail_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        // block_range_threshold == total: exactly at the boundary, so the fast
        // path's `object_size > block_range_threshold` conjunct is false.
        let f = LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(total)
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(tail)
                    .with_whole_object_threshold(0),
            );
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let acc = QueryAccounting::new();
        let (indices, _dirs, _, _, _) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan")
            .expect("relevant");
        let count = indices.len();
        assert_eq!(
            count, N,
            "at-threshold: same survivor count as the fast path"
        );
        assert!(
            acc.snapshot().total_s3_bytes() > tail,
            "at-threshold: block-range fetch ran, not the footer-only probe"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod whole_object_get_limiter_tests {
    //! Pins the ADR-1195 gap the adversarial review of #1195 found: the RLOG
    //! whole-object funnel (`fetch_accounted`, the path every object at or
    //! below `block_range_threshold` takes) issued its GET with no permit at
    //! all, so it could never be bounded no matter how the caller configured
    //! `get_limiter`. The first tests drive `fetch_accounted`, which takes
    //! the whole-object path unconditionally; the later ones drive the
    //! production funnel `fetch_accounted_with_tenant` on objects below the
    //! default `block_range_threshold` (`DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`),
    //! with and without a cache attached, so both `whole_object_bytes` permit
    //! sites are covered on the path the cost-based default policy takes for
    //! small objects.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{RlogWriter, stream_attrs_bytes};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const CONTENT_HASH: [u8; 32] = [9u8; 32];
    const KEY: &str = "t/whole.rlog";

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn record(ts: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("request.id".to_string(), AttrValue::Str(format!("r{ts}")))],
        }
    }

    fn build_object() -> Vec<u8> {
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        w.push(record(0)).expect("push");
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64) -> SegmentRef {
        seg_ref_with(KEY, CONTENT_HASH, size)
    }

    fn seg_ref_with(key: &str, content_hash: [u8; 32], size: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: size,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 0,
            sample_count: 1,
            series_count: 0,
            shard: 0,
            content_hash,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    async fn store_with_object(bytes: Vec<u8>) -> MemoryStore {
        let store = MemoryStore::new();
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// Object well under `DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`, so every
    /// fetcher below takes the unconditional whole-object GET in
    /// `fetch_accounted`, never `block_range`.
    async fn small_object_backend() -> (Arc<FaultStore<MemoryStore>>, SegmentRef) {
        let bytes = build_object();
        let size = bytes.len() as u64;
        assert!(
            size < DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "fixture object must stay below the whole-object threshold"
        );
        let store = store_with_object(bytes).await;
        let fault = Arc::new(FaultStore::new(store, FaultPlan::default()));
        (fault, seg_ref(size))
    }

    const CONTENT_HASH_A: [u8; 32] = [10u8; 32];
    const CONTENT_HASH_B: [u8; 32] = [11u8; 32];
    const KEY_A: &str = "t/whole_a.rlog";
    const KEY_B: &str = "t/whole_b.rlog";

    /// Two objects with distinct store keys and content hashes, so two
    /// concurrent fetches never collapse into one cache single-flight (the
    /// cache key is `content_hash`, not the store key): each independently
    /// reaches its own `whole_object_bytes` GET and its own permit
    /// acquisition, which is what makes the cache-attached test below prove
    /// the `GetLimiter`, not incidental single-flight serialization, bounds
    /// concurrency.
    async fn two_object_backend() -> (Arc<FaultStore<MemoryStore>>, SegmentRef, SegmentRef) {
        let bytes = build_object();
        let size = bytes.len() as u64;
        assert!(
            size < DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "fixture object must stay below the whole-object threshold"
        );
        let store = MemoryStore::new();
        store
            .put(KEY_A, Bytes::from(bytes.clone()), PutOptions::default())
            .await
            .expect("put a");
        store
            .put(KEY_B, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put b");
        let fault = Arc::new(FaultStore::new(store, FaultPlan::default()));
        (
            fault,
            seg_ref_with(KEY_A, CONTENT_HASH_A, size),
            seg_ref_with(KEY_B, CONTENT_HASH_B, size),
        )
    }

    /// Two `LogSegmentFetcher`s sharing one `GetLimiter::new(1)` must never
    /// let both whole-object GETs be in flight at once.
    ///
    /// Non-vacuity: before Deliverable 1 wired a private `get_limiter` field
    /// onto `LogSegmentFetcher` and threaded it through `fetch_accounted`'s
    /// GET (the `let _permit = self.get_limiter.acquire()...` block added
    /// ahead of `self.store.get(key, GetRange::Full)` there), that GET took
    /// no permit at all, so `gate.wait_until_held(2)` below would resolve
    /// immediately instead of timing out and this test would fail at the
    /// `is_err()` assertion.
    #[tokio::test]
    async fn shared_get_limiter_bounds_whole_object_gets_to_one() {
        let (fault, seg) = small_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let shared = Arc::new(crate::GetLimiter::new(1).expect("1 permit is valid"));
        let fetcher_a = LogSegmentFetcher::new(backend.clone()).with_get_limiter(shared.clone());
        let fetcher_b = LogSegmentFetcher::new(backend).with_get_limiter(shared);
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let seg_a = seg.clone();
        let query_a = query.clone();
        let handle_a = tokio::spawn(async move { fetcher_a.fetch(&seg_a, &query_a).await });
        let seg_b = seg.clone();
        let query_b = query.clone();
        let handle_b = tokio::spawn(async move { fetcher_b.fetch(&seg_b, &query_b).await });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("one of the two fetches issues its GET within 30 s");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                gate.wait_until_held(2)
            )
            .await
            .is_err(),
            "one shared permit must cap in-flight whole-object GETs at exactly 1"
        );
        assert_eq!(
            gate.held_count(),
            1,
            "peak in-flight whole-object GETs must be exactly 1"
        );

        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("releasing the first permit lets the second GET proceed within 30 s");
        assert_eq!(
            gate.held_count(),
            1,
            "the second GET must now be the only one held"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }

        let a = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a)
            .await
            .expect("fetch a completes within 30 s")
            .expect("join fetch a")
            .expect("fetch a")
            .expect("fetch a found the segment relevant");
        let b = tokio::time::timeout(std::time::Duration::from_secs(30), handle_b)
            .await
            .expect("fetch b completes within 30 s")
            .expect("join fetch b")
            .expect("fetch b")
            .expect("fetch b found the segment relevant");
        assert_eq!(a.records.len(), 1);
        assert_eq!(b.records.len(), 1);
    }

    /// Control for the test above: two `LogSegmentFetcher`s with PRIVATE
    /// `GetLimiter::new(1)` instances (not shared) must reach 2 in-flight
    /// whole-object GETs, proving the shared case above bounds because the
    /// limiter is shared, not because the store or fixture serializes them.
    #[tokio::test]
    async fn private_get_limiters_do_not_share_a_bound() {
        let (fault, seg) = small_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let fetcher_a = LogSegmentFetcher::new(backend.clone()).with_get_limiter(Arc::new(
            crate::GetLimiter::new(1).expect("1 permit is valid"),
        ));
        let fetcher_b = LogSegmentFetcher::new(backend).with_get_limiter(Arc::new(
            crate::GetLimiter::new(1).expect("1 permit is valid"),
        ));
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let seg_a = seg.clone();
        let query_a = query.clone();
        let handle_a = tokio::spawn(async move { fetcher_a.fetch(&seg_a, &query_a).await });
        let seg_b = seg.clone();
        let query_b = query.clone();
        let handle_b = tokio::spawn(async move { fetcher_b.fetch(&seg_b, &query_b).await });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(2))
            .await
            .expect("both fetches issue their GET within 30 s: private limiters do not share");
        assert_eq!(
            gate.held_count(),
            2,
            "private limiters must let both whole-object GETs be in flight at once"
        );

        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
        let a = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a)
            .await
            .expect("fetch a completes within 30 s")
            .expect("join fetch a")
            .expect("fetch a")
            .expect("fetch a found the segment relevant");
        let b = tokio::time::timeout(std::time::Duration::from_secs(30), handle_b)
            .await
            .expect("fetch b completes within 30 s")
            .expect("join fetch b")
            .expect("fetch b")
            .expect("fetch b found the segment relevant");
        assert_eq!(a.records.len(), 1);
        assert_eq!(b.records.len(), 1);
    }

    const TENANT: TenantHash = TenantHash([7u8; 16]);

    /// `with_get_limiter` followed by `with_block_range` must leave the
    /// replacement `BlockRangeFetcher` sharing this instance's limiter, not
    /// the private default `BlockRangeFetcher::new` builds for itself.
    ///
    /// Non-vacuity: deleting the
    /// `.with_get_limiter(Arc::clone(&self.get_limiter))` re-apply inside
    /// `with_block_range` (log_fetcher.rs:768) leaves the second assertion
    /// below comparing `shared` against the replacement's own default
    /// limiter, a distinct `Arc`, so it fails.
    #[test]
    fn with_block_range_reapplies_shared_limiter_onto_the_replacement() {
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let shared = Arc::new(crate::GetLimiter::new(2).expect("2 permits is valid"));
        let fetcher = LogSegmentFetcher::new(backend.clone())
            .with_get_limiter(shared.clone())
            .with_block_range(BlockRangeFetcher::new(backend));

        assert!(
            Arc::ptr_eq(fetcher.get_limiter_for_test(), &shared),
            "with_block_range must not disturb this fetcher's own limiter"
        );
        assert!(
            Arc::ptr_eq(fetcher.block_range_get_limiter_for_test(), &shared),
            "with_block_range must re-apply the shared limiter onto the \
             replacement BlockRangeFetcher, not leave it on its own default"
        );
    }

    /// `with_max_concurrent_gets(n)` must wire the SAME private limiter onto
    /// both the whole-object path and the block-range path, with `n` permits.
    ///
    /// Non-vacuity: if the builder set only `self.block_range`'s limiter (the
    /// bug shape this pins), `get_limiter_for_test` would still report the
    /// default `DEFAULT_LOG_MAX_CONCURRENT_GETS`-permit limiter `new` built,
    /// so the `ptr_eq` below fails and `permits()` reads
    /// `DEFAULT_LOG_MAX_CONCURRENT_GETS`, not 3.
    #[test]
    fn with_max_concurrent_gets_wires_both_paths_to_one_private_limiter() {
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let fetcher = LogSegmentFetcher::new(backend).with_max_concurrent_gets(3);

        assert!(
            Arc::ptr_eq(
                fetcher.get_limiter_for_test(),
                fetcher.block_range_get_limiter_for_test()
            ),
            "with_max_concurrent_gets must wire one private limiter onto both \
             the whole-object and block-range paths"
        );
        assert_eq!(fetcher.get_limiter_for_test().permits(), 3);
    }

    /// Two standalone fetchers each built with `with_max_concurrent_gets(1)`
    /// hold PRIVATE limiters: two concurrent `fetch_accounted_with_tenant`
    /// calls per fetcher (the production tenant-aware entry point, not
    /// `fetch_accounted`) must peak at exactly 2 in-flight whole-object GETs
    /// (one per fetcher's own 1-permit limiter), never 1 and never 3+.
    #[tokio::test]
    async fn max_concurrent_gets_one_bounds_whole_object_funnel_per_fetcher() {
        let (fault, seg) = small_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let fetcher_a = LogSegmentFetcher::new(backend.clone()).with_max_concurrent_gets(1);
        let fetcher_b = LogSegmentFetcher::new(backend).with_max_concurrent_gets(1);
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let mut handles = Vec::new();
        for fetcher in [fetcher_a.clone(), fetcher_a, fetcher_b.clone(), fetcher_b] {
            let seg = seg.clone();
            let query = query.clone();
            handles.push(tokio::spawn(async move {
                fetcher
                    .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
                    .await
            }));
        }

        for _ in 0..2 {
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(2))
                .await
                .expect("one GET per fetcher issues within 30s");
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    gate.wait_until_held(3)
                )
                .await
                .is_err(),
                "two independent private 1-permit limiters must together peak \
                 at exactly 2 in-flight GETs, never 3"
            );
            assert_eq!(
                gate.held_count(),
                2,
                "peak in-flight whole-object GETs must be exactly 2"
            );
            for id in gate.held() {
                assert!(gate.release(id), "held id must release");
            }
        }

        for handle in handles {
            let output = tokio::time::timeout(std::time::Duration::from_secs(30), handle)
                .await
                .expect("fetch completes within 30s")
                .expect("join fetch")
                .expect("fetch")
                .expect("fetch found the segment relevant");
            assert_eq!(output.records.len(), 1);
        }
    }

    /// The same two fetchers, wired instead with `with_get_limiter` to ONE
    /// shared `GetLimiter::new(1)`, must peak at exactly 1 in-flight
    /// whole-object GET across all four calls -- proving the private-limiter
    /// bound above is a property of the limiter being private, not of the
    /// fixture or store serializing calls on its own.
    #[tokio::test]
    async fn shared_get_limiter_bounds_whole_object_funnel_across_fetchers_tenant_aware() {
        let (fault, seg) = small_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let shared = Arc::new(crate::GetLimiter::new(1).expect("1 permit is valid"));
        let fetcher_a = LogSegmentFetcher::new(backend.clone()).with_get_limiter(shared.clone());
        let fetcher_b = LogSegmentFetcher::new(backend).with_get_limiter(shared);
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let mut handles = Vec::new();
        for fetcher in [fetcher_a.clone(), fetcher_a, fetcher_b.clone(), fetcher_b] {
            let seg = seg.clone();
            let query = query.clone();
            handles.push(tokio::spawn(async move {
                fetcher
                    .fetch_accounted_with_tenant(&seg, TENANT, &query, &QueryAccounting::new())
                    .await
            }));
        }

        for _ in 0..4 {
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
                .await
                .expect("next GET issues within 30s");
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    gate.wait_until_held(2)
                )
                .await
                .is_err(),
                "one shared permit across two fetchers must cap in-flight GETs \
                 at exactly 1"
            );
            assert_eq!(
                gate.held_count(),
                1,
                "peak in-flight whole-object GETs must be exactly 1"
            );
            for id in gate.held() {
                assert!(gate.release(id), "held id must release");
            }
        }

        for handle in handles {
            let output = tokio::time::timeout(std::time::Duration::from_secs(30), handle)
                .await
                .expect("fetch completes within 30s")
                .expect("join fetch")
                .expect("fetch")
                .expect("fetch found the segment relevant");
            assert_eq!(output.records.len(), 1);
        }
    }

    /// `whole_object_bytes`'s NO-cache GET (log_fetcher.rs:2038) must be
    /// bounded by the shared limiter, reached here through the production
    /// tenant-aware funnel (`fetch_accounted_with_tenant`), not the
    /// untenanted `fetch_accounted` the tests above use.
    ///
    /// Non-vacuity: removing the `let _permit =
    /// self.get_limiter.acquire()...` at log_fetcher.rs:2038 lets both GETs
    /// proceed immediately, so `wait_until_held(2)` below resolves instead of
    /// timing out.
    #[tokio::test]
    async fn whole_object_bytes_no_cache_permit_bounds_tenant_funnel() {
        let (fault, seg_a, seg_b) = two_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let shared = Arc::new(crate::GetLimiter::new(1).expect("1 permit is valid"));
        let fetcher_a = LogSegmentFetcher::new(backend.clone()).with_get_limiter(shared.clone());
        let fetcher_b = LogSegmentFetcher::new(backend).with_get_limiter(shared);
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let query_a = query.clone();
        let handle_a = tokio::spawn(async move {
            fetcher_a
                .fetch_accounted_with_tenant(&seg_a, TENANT, &query_a, &QueryAccounting::new())
                .await
        });
        let handle_b = tokio::spawn(async move {
            fetcher_b
                .fetch_accounted_with_tenant(&seg_b, TENANT, &query, &QueryAccounting::new())
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("one of the two fetches issues its GET within 30s");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                gate.wait_until_held(2)
            )
            .await
            .is_err(),
            "one shared permit must cap in-flight no-cache whole_object_bytes \
             GETs at exactly 1"
        );
        assert_eq!(
            gate.held_count(),
            1,
            "peak in-flight GETs must be exactly 1"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("releasing the first permit lets the second GET proceed within 30s");
        assert_eq!(
            gate.held_count(),
            1,
            "the second GET must now be the only one held"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }

        let a = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a)
            .await
            .expect("fetch a completes within 30s")
            .expect("join fetch a")
            .expect("fetch a")
            .expect("fetch a found the segment relevant");
        let b = tokio::time::timeout(std::time::Duration::from_secs(30), handle_b)
            .await
            .expect("fetch b completes within 30s")
            .expect("join fetch b")
            .expect("fetch b")
            .expect("fetch b found the segment relevant");
        assert_eq!(a.records.len(), 1);
        assert_eq!(b.records.len(), 1);
    }

    /// `whole_object_bytes`'s cache-attached GET closure
    /// (log_fetcher.rs:2075, inside `cache.get_or_fetch`) must be bounded by
    /// the shared limiter too. Two DISTINCT objects (content hashes) keep
    /// this from being a single-flight no-op: same-key concurrent
    /// `get_or_fetch` calls would collapse into one physical fetch
    /// regardless of the permit acquisition this test exists to pin.
    ///
    /// Non-vacuity: removing the `let _permit =
    /// self.get_limiter.acquire()...` inside the `cache.get_or_fetch`
    /// closure at log_fetcher.rs:2075 lets both GETs proceed immediately, so
    /// `wait_until_held(2)` below resolves instead of timing out.
    #[tokio::test]
    async fn whole_object_bytes_cache_permit_bounds_tenant_funnel() {
        let (fault, seg_a, seg_b) = two_object_backend().await;
        let gate: GateHandle = fault.hold(Op::Get, None, Occurrence::Always);
        let backend: Arc<dyn ObjectStoreBackend> = fault;

        let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
            16 * 1024 * 1024,
            100,
            16 * 1024 * 1024,
        )));
        let shared = Arc::new(crate::GetLimiter::new(1).expect("1 permit is valid"));
        let fetcher_a = LogSegmentFetcher::new(backend.clone())
            .with_cache(cache.clone())
            .with_get_limiter(shared.clone());
        let fetcher_b = LogSegmentFetcher::new(backend)
            .with_cache(cache)
            .with_get_limiter(shared);
        let query = LogQuery::new(i64::MIN, i64::MAX);

        let query_a = query.clone();
        let handle_a = tokio::spawn(async move {
            fetcher_a
                .fetch_accounted_with_tenant(&seg_a, TENANT, &query_a, &QueryAccounting::new())
                .await
        });
        let handle_b = tokio::spawn(async move {
            fetcher_b
                .fetch_accounted_with_tenant(&seg_b, TENANT, &query, &QueryAccounting::new())
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("one of the two fetches issues its GET within 30s");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                gate.wait_until_held(2)
            )
            .await
            .is_err(),
            "one shared permit must cap in-flight cached whole_object_bytes \
             GETs at exactly 1"
        );
        assert_eq!(
            gate.held_count(),
            1,
            "peak in-flight GETs must be exactly 1"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("releasing the first permit lets the second GET proceed within 30s");
        assert_eq!(
            gate.held_count(),
            1,
            "the second GET must now be the only one held"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }

        let a = tokio::time::timeout(std::time::Duration::from_secs(30), handle_a)
            .await
            .expect("fetch a completes within 30s")
            .expect("join fetch a")
            .expect("fetch a")
            .expect("fetch a found the segment relevant");
        let b = tokio::time::timeout(std::time::Duration::from_secs(30), handle_b)
            .await
            .expect("fetch b completes within 30s")
            .expect("join fetch b")
            .expect("fetch b")
            .expect("fetch b found the segment relevant");
        assert_eq!(a.records.len(), 1);
        assert_eq!(b.records.len(), 1);
    }

    /// Deliverable 4 (RLOG whole-object): a budget too small for one object's
    /// bytes refuses the fetch with a typed `FetchMemoryExhausted` carrying the
    /// requested/reserved/limit counts, BEFORE any GET reaches the store, and
    /// leaves the budget with nothing reserved.
    ///
    /// Non-vacuity: replacing `self.reserve_fetch(seg_ref.object_size)?` in
    /// `whole_object_bytes` with an infallible reserve lets the GET fire and the
    /// fetch succeed, so the `FetchMemoryExhausted` match and the zero-GET
    /// assertion below both fail.
    #[tokio::test]
    async fn whole_object_budget_refusal_issues_zero_gets() {
        use ravel_object_store::InstrumentedStore;
        let bytes = build_object();
        let size = bytes.len() as u64;
        let memory = MemoryStore::new();
        memory
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        let instrumented = Arc::new(InstrumentedStore::new(memory));
        let metrics = instrumented.metrics();
        let backend: Arc<dyn ObjectStoreBackend> = instrumented;

        // One byte short of a single object: the reservation cannot be granted.
        let limit = size - 1;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = LogSegmentFetcher::new(backend).with_memory_budget(budget.clone());

        let err = fetcher
            .whole_object_bytes(
                &seg_ref(size),
                TENANT,
                QueryPhase::Plan,
                &QueryAccounting::new(),
            )
            .await
            .expect_err("a budget below one object's size must refuse the fetch");
        match err {
            LogFetchError::FetchMemoryExhausted {
                requested,
                reserved,
                limit: reported_limit,
            } => {
                assert_eq!(requested, size, "the refusal names the object's byte size");
                assert_eq!(reserved, 0, "nothing else was reserved on this budget");
                assert_eq!(reported_limit, limit, "the refusal names the budget limit");
            }
            other => panic!("expected FetchMemoryExhausted, got {other:?}"),
        }
        assert_eq!(
            metrics.snapshot().get.calls,
            0,
            "a budget refusal issues zero GETs: the reservation precedes the GET"
        );
        assert_eq!(
            budget.reserved(),
            0,
            "a refused reservation leaves nothing reserved"
        );
    }

    /// Deliverable 4 (concurrent cap): four distinct objects fetched
    /// concurrently through one fetcher whose shared `MemoryBudget` holds
    /// exactly three objects' worth of bytes admit exactly three; the fourth
    /// refuses typed. Every admitted buffer carries its guard, so the budget
    /// stays at three reservations until the buffers drop, then returns to zero.
    ///
    /// Non-vacuity: an infallible reserve in `whole_object_bytes` admits all
    /// four, so the `succeeded == 3` / `refused == 1` assertions fail.
    #[tokio::test]
    async fn four_concurrent_whole_object_fetches_admit_three_under_a_three_object_budget() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let memory = MemoryStore::new();
        let keys = ["t/c0.rlog", "t/c1.rlog", "t/c2.rlog", "t/c3.rlog"];
        for k in keys {
            memory
                .put(k, Bytes::from(bytes.clone()), PutOptions::default())
                .await
                .expect("put");
        }
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(memory);
        let budget = Arc::new(ravel_memory::MemoryBudget::new(size * 3));
        let fetcher = Arc::new(LogSegmentFetcher::new(backend).with_memory_budget(budget.clone()));

        let hashes = [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]];
        let results = futures::future::join_all(keys.iter().zip(hashes).map(|(k, h)| {
            let fetcher = fetcher.clone();
            let seg = seg_ref_with(k, h, size);
            async move {
                fetcher
                    .whole_object_bytes(&seg, TENANT, QueryPhase::Plan, &QueryAccounting::new())
                    .await
            }
        }))
        .await;

        let succeeded = results.iter().filter(|r| r.is_ok()).count();
        let refused = results
            .iter()
            .filter(|r| matches!(r, Err(LogFetchError::FetchMemoryExhausted { .. })))
            .count();
        assert_eq!(
            succeeded, 3,
            "a three-object budget admits exactly three of four concurrent fetches"
        );
        assert_eq!(
            refused, 1,
            "the fourth fetch refuses with the typed budget error"
        );
        assert_eq!(
            budget.reserved(),
            size * 3,
            "the three admitted buffers hold their reservations while alive"
        );
        drop(results);
        assert_eq!(
            budget.reserved(),
            0,
            "dropping every buffer returns the budget to zero"
        );
    }

    /// Deliverable 3 (handoff): a whole-object fetch that admits its buffer to
    /// the read cache marks the reservation handed off, so the budget reports
    /// the transient overlap (both the returned buffer and the cache entry hold
    /// the same bytes) while the buffer is held, and clears it on drop.
    ///
    /// Non-vacuity: dropping the `reservation.mark_handed_off()` call on the
    /// `ReadOutcome::Fetched` arm of `whole_object_bytes` leaves `handoff_overlap()`
    /// at 0 while the buffer is held, so the first assertion fails.
    #[tokio::test]
    async fn cache_insert_marks_the_reservation_handed_off() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let store = store_with_object(bytes).await;
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
            16 * 1024 * 1024,
            100,
            16 * 1024 * 1024,
        )));
        let budget = Arc::new(ravel_memory::MemoryBudget::new(16 * 1024 * 1024));
        let fetcher = LogSegmentFetcher::new(backend)
            .with_cache(cache)
            .with_memory_budget(budget.clone());

        let buf = fetcher
            .whole_object_bytes(
                &seg_ref(size),
                TENANT,
                QueryPhase::Plan,
                &QueryAccounting::new(),
            )
            .await
            .expect("whole_object_bytes");
        assert_eq!(buf.len() as u64, size);
        assert_eq!(
            budget.handoff_overlap(),
            size,
            "the cache insert marks the fetched buffer handed off while both ledgers hold it"
        );
        drop(buf);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the overlap clears when the buffer drops"
        );
        assert_eq!(
            budget.reserved(),
            0,
            "the reservation releases with the buffer"
        );
    }

    /// The same overlap on a cache HIT, which is the case the first version of
    /// this work left unmarked. The returned `Bytes` clones the cache entry's
    /// allocation, so the cache cap and the fetch guard both cover it for as
    /// long as the caller holds it, exactly as after an insert. Leaving a hit
    /// unmarked understates `handoff_overlap` by every hit, and it is that
    /// undercount which makes decision 3's `unique` term inexact and its
    /// derived reserve undersized.
    ///
    /// Non-vacuity: dropping the `reservation.mark_handed_off()` call on the
    /// `ReadOutcome::Hit` arm leaves `handoff_overlap()` at 0 on the second fetch
    /// while its buffer is held, so the hit assertion fails while the insert
    /// assertion above still passes.
    #[tokio::test]
    async fn cache_hit_marks_the_reservation_handed_off() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let store = store_with_object(bytes).await;
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
            16 * 1024 * 1024,
            100,
            16 * 1024 * 1024,
        )));
        let budget = Arc::new(ravel_memory::MemoryBudget::new(16 * 1024 * 1024));
        let fetcher = LogSegmentFetcher::new(backend)
            .with_cache(cache)
            .with_memory_budget(budget.clone());

        // First fetch admits the object and is the insert case; drop it so the
        // only overlap the second fetch can report is its own.
        let first = fetcher
            .whole_object_bytes(
                &seg_ref(size),
                TENANT,
                QueryPhase::Plan,
                &QueryAccounting::new(),
            )
            .await
            .expect("first whole_object_bytes");
        drop(first);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the insert's overlap clears before the hit is measured"
        );

        let accounting = QueryAccounting::new();
        let hit = fetcher
            .whole_object_bytes(&seg_ref(size), TENANT, QueryPhase::Plan, &accounting)
            .await
            .expect("second whole_object_bytes");
        assert_eq!(
            accounting.snapshot().cache_hits,
            1,
            "the second fetch is served from the cache, not the store"
        );
        assert_eq!(
            budget.handoff_overlap(),
            size,
            "a cache hit holds the same bytes under the cache cap and this guard"
        );
        drop(hit);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the overlap clears when the buffer drops"
        );
        assert_eq!(budget.reserved(), 0, "the reservation releases with it");
    }

    /// A block-range read that crosses over to a covering whole-object read
    /// (ADR-0107 decision 1) holds exactly one object-sized reservation AT ANY
    /// INSTANT, never two at once. The coverage crossover drops the assembler
    /// -- releasing its placed regions and their reservation guards -- before
    /// the covering GET reserves fresh (ADR-1170 decision 2): on this fixture
    /// the probe alone placed the whole object, so keeping the assembler would
    /// hold two object widths.
    ///
    /// `with_block_range_threshold(0)` routes the object to the block-range path
    /// and sets that path's own whole-object crossover to zero, so the object is
    /// above it and the read skips the pre-probe crossover and reaches the
    /// coverage crossover (a predicate-free, all-columns read makes every block a
    /// candidate, so coverage is ~1.0, past the 0.75 default).
    ///
    /// Sampling `budget.reserved()` only after the fetch completes (as an
    /// earlier version of this test did) cannot tell a true peak of one object
    /// apart from a peak of two that happens to collapse back to one by the
    /// time the fetch returns -- both end at `size` and drop to `0`. This test
    /// instead holds the covering GET in flight with a `FaultStore` gate and
    /// samples the budget from the outside while it is genuinely still
    /// pending. This fixture's object is small enough that
    /// `effective_suffix_len` covers the whole object in the initial suffix
    /// probe, so the only two live `Op::Get` calls this fetch issues are: (1)
    /// that probe, and (2) the covering GET itself. `Occurrence::Nth(2)` holds
    /// exactly the second.
    ///
    /// Non-vacuity: removing the `drop(asm)` line immediately before the
    /// `covering_read` call at the version-4 coverage crossover (the dispatch
    /// takes that branch because `RlogWriter` always emits `PAGE_DIR`, not
    /// because of the stamped `segment_format_version`, which it never reads)
    /// restores the shape ADR-1170 decision 2 forbids: the assembler stays
    /// alive holding its own reservation for the rest of the function while
    /// `covering_read` reserves a second, independent one for the same object.
    /// The budget below is sized for two objects specifically so this second
    /// reservation still succeeds instead of failing closed -- with that
    /// single line removed, `budget.reserved()` while the covering GET is
    /// held reads `2 * size`, not `size`, and the in-flight assertion below
    /// panics on the mismatch (confirmed by making exactly that edit and
    /// observing the panic before reverting it).
    ///
    /// The version-3 crossover applies the identical `drop(asm)` fix at the
    /// same line shape, but is not covered by a fixture here: `seg_ref_with`
    /// stamps `segment_format_version = footer::VERSION` (4), `RlogWriter`
    /// always emits `PAGE_DIR`, and the dispatch in `fetch_object_with_footer`
    /// takes the version-4 branch whenever `PAGE_DIR` is present, so no object
    /// built through the production writer ever reaches the version-3 branch.
    /// Hand-crafting a footer without `PAGE_DIR` to reach it would mean
    /// bypassing `RlogWriter` entirely, which is out of scope for this crate.
    #[tokio::test]
    async fn coverage_crossover_reserves_the_object_once() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let store = store_with_object(bytes).await;
        let fault = Arc::new(FaultStore::new(store, FaultPlan::default()));
        // The covering GET is the second live `Get` this fetch issues (see
        // doc comment above): hold exactly that one.
        let gate = fault.hold(Op::Get, Some(KEY.to_string()), Occurrence::Nth(2));
        let backend: Arc<dyn ObjectStoreBackend> = fault;
        // Room for TWO objects, not one: the in-flight sample below must be
        // able to observe a peak of two live reservations if the code regresses
        // to that shape, rather than have the fetch fail closed with
        // `FetchMemoryExhausted` before the covering GET is even attempted
        // (which would still prove a regression, just not the specific peak).
        let budget = Arc::new(ravel_memory::MemoryBudget::new(size * 2));
        let fetcher = LogSegmentFetcher::new(backend)
            .with_block_range_threshold(0)
            .with_memory_budget(budget.clone());

        let task = tokio::spawn(async move {
            fetcher
                .scan_accounted_with_tenant(
                    &seg_ref(size),
                    TENANT,
                    &LogQuery::new(i64::MIN, i64::MAX),
                    &ColumnSelection::all(),
                    &QueryAccounting::new(),
                )
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(30), gate.wait_until_held(1))
            .await
            .expect("the covering GET reaches the gate within 30 s");
        assert_eq!(
            budget.reserved(),
            size,
            "exactly one object-sized reservation is live while the covering \
             GET is in flight -- the assembler already dropped its own before \
             this GET reserved fresh, so the peak is one object, not two"
        );
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }

        let scan = task
            .await
            .expect("join")
            .expect("a crossover fetch of one object must fit a one-object budget")
            .expect("the segment is relevant to a full-window query");

        assert_eq!(
            budget.reserved(),
            size,
            "the object is reserved exactly once across the coverage crossover, \
             never twice"
        );
        drop(scan);
        assert_eq!(
            budget.reserved(),
            0,
            "dropping the scan releases the single reservation"
        );
    }

    /// `covering_read`'s single-GET branch (`total_size <= max_fetch_run_bytes`)
    /// on a cache HIT: `cached_extent` returns `(bytes, false)`, a clone of the
    /// resident cache entry, so the cache cap and this fetch guard both cover
    /// the same allocation for the buffer's life. This is the same class the
    /// whole-object funnel's `ReadOutcome::Hit` arm handles
    /// (`cache_hit_marks_the_reservation_handed_off` above), left unmarked here
    /// because `covering_read` is a separate call site.
    ///
    /// Reached via the coverage crossover (`with_block_range_threshold(0)`,
    /// same routing `coverage_crossover_reserves_the_object_once` uses): the
    /// object is small enough that `total_size` stays under
    /// `max_fetch_run_bytes`, so this exercises the single-GET branch, not the
    /// segmented loop.
    ///
    /// Non-vacuity: dropping the `reservation.mark_handed_off()` call added to
    /// the `else` arm of `covering_read`'s single-GET branch leaves
    /// `handoff_overlap()` at 0 while the second (cache-hit) scan's buffer is
    /// held, so the hit assertion below fails with: assertion `left == right`
    /// failed: a covering-read cache hit holds the same bytes under the cache
    /// cap and this guard -- left: 0, right: 477 (the fixture object's size;
    /// confirmed by making exactly that edit, observing the failure, and
    /// reverting it by hand).
    #[tokio::test]
    async fn covering_read_cache_hit_marks_the_reservation_handed_off() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let store = store_with_object(bytes).await;
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
            16 * 1024 * 1024,
            100,
            16 * 1024 * 1024,
        )));
        let budget = Arc::new(ravel_memory::MemoryBudget::new(16 * 1024 * 1024));
        let fetcher = LogSegmentFetcher::new(backend)
            .with_block_range_threshold(0)
            .with_cache(cache)
            .with_memory_budget(budget.clone());

        // First scan admits the object to the cache (the insert case); drop it
        // so the only overlap the second scan can report is its own.
        let first = fetcher
            .scan_accounted_with_tenant(
                &seg_ref(size),
                TENANT,
                &LogQuery::new(i64::MIN, i64::MAX),
                &ColumnSelection::all(),
                &QueryAccounting::new(),
            )
            .await
            .expect("first scan")
            .expect("the segment is relevant to a full-window query");
        drop(first);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the insert's overlap clears before the hit is measured"
        );

        let accounting = QueryAccounting::new();
        let hit = fetcher
            .scan_accounted_with_tenant(
                &seg_ref(size),
                TENANT,
                &LogQuery::new(i64::MIN, i64::MAX),
                &ColumnSelection::all(),
                &accounting,
            )
            .await
            .expect("second scan")
            .expect("the segment is relevant to a full-window query");
        // Two hits, not one: the suffix probe (ADR-0107) and the covering GET
        // each read a distinct cache-keyed extent of this small object, and
        // both are now cache-resident from the first scan.
        assert_eq!(
            accounting.snapshot().cache_hits,
            2,
            "the second scan's suffix probe and covering read are both served \
             from the cache, not the store"
        );
        assert_eq!(
            budget.handoff_overlap(),
            size,
            "a covering-read cache hit holds the same bytes under the cache cap \
             and this guard"
        );
        drop(hit);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the overlap clears when the buffer drops"
        );
        assert_eq!(budget.reserved(), 0, "the reservation releases with it");
    }

    /// `covering_read`'s single-GET branch on a cache MISS: `cached_extent`
    /// reports `live = true` for both an uncached direct GET and a cache miss
    /// that its own leader just inserted, so `live` alone cannot tell them
    /// apart -- the fix branches on `self.cache.is_some()` instead. This is
    /// the cache-configured case, the very first scan against a fresh cache
    /// (no warm-up), which is a genuine miss for both the suffix probe and
    /// the covering read.
    ///
    /// Non-vacuity: dropping the `reservation.mark_handed_off()` call added to
    /// the `if live` arm of `covering_read`'s single-GET branch leaves
    /// `handoff_overlap()` at 0 while this scan's buffer is held, so the
    /// assertion below fails with: assertion `left == right` failed: a
    /// covering-read cache miss holds the same freshly admitted bytes under
    /// the cache cap and this guard -- left: 0, right: 477 (the fixture
    /// object's size; confirmed by making exactly that edit, observing the
    /// failure, and reverting it by hand).
    #[tokio::test]
    async fn covering_read_cache_miss_marks_the_reservation_handed_off() {
        let bytes = build_object();
        let size = bytes.len() as u64;
        let store = store_with_object(bytes).await;
        let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
            16 * 1024 * 1024,
            100,
            16 * 1024 * 1024,
        )));
        let budget = Arc::new(ravel_memory::MemoryBudget::new(16 * 1024 * 1024));
        // A small, explicit suffix (unlike the sibling hit test, which relies
        // on the default `derive_suffix_len` covering the whole object): the
        // suffix probe then caches a distinct, smaller key than the covering
        // read's full `(0, total_size)` key, so the covering read cannot ride
        // the probe's own insert to a hit. With the default suffix both key
        // the same sub-range, and the probe's insert makes the "covering GET"
        // that follows in the same scan a hit, not a miss -- there is then no
        // way to reach the `if live` arm on a first, fresh-cache scan at all.
        let fetcher = LogSegmentFetcher::new(backend)
            .with_block_range_threshold(0)
            .with_suffix_len(16)
            .with_cache(cache)
            .with_memory_budget(budget.clone());

        let accounting = QueryAccounting::new();
        let scan = fetcher
            .scan_accounted_with_tenant(
                &seg_ref(size),
                TENANT,
                &LogQuery::new(i64::MIN, i64::MAX),
                &ColumnSelection::all(),
                &accounting,
            )
            .await
            .expect("scan")
            .expect("the segment is relevant to a full-window query");
        assert_eq!(
            accounting.snapshot().cache_hits,
            0,
            "the very first scan against a fresh cache, with a suffix probe key \
             distinct from the covering read's, is a genuine miss on both"
        );
        assert_eq!(
            budget.handoff_overlap(),
            size,
            "a covering-read cache miss holds the same freshly admitted bytes \
             under the cache cap and this guard"
        );
        drop(scan);
        assert_eq!(
            budget.handoff_overlap(),
            0,
            "the overlap clears when the buffer drops"
        );
        assert_eq!(budget.reserved(), 0, "the reservation releases with it");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod whole_segment_projection_tests {
    //! Pins #790: `scan_whole_accounted_with_tenant` (the predicate-free
    //! whole-segment fast path, #693 part 3) decodes only the pages its
    //! `ColumnSelection` projects, the same PAGE_DIR-driven per-page column
    //! skip `decode_v4_block` already applies on every other scan path
    //! (ADR-0699 decisions 1, 2, 5), rather than decoding every column of
    //! every block after its one whole-object GET.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([11u8; 16]);
    const CONTENT_HASH: [u8; 32] = [13u8; 32];
    const KEY: &str = "t/whole-projection.rlog";

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [11u8; 16],
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn record(ts: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("body-{ts}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("request.id".to_string(), AttrValue::Str(format!("r{ts}")))],
        }
    }

    /// One block per record (`block_target_records: 1`), all inside the one
    /// default row group (`group_target_blocks` 32 covers `N` < 32 blocks), so
    /// PAGE_DIR lists exactly one page per column per block with no
    /// cross-group split to complicate the page count.
    fn build_object(records: &[LogRecord]) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// Drains a whole-segment scan to completion and returns its final stats.
    async fn drain(
        f: &LogSegmentFetcher,
        seg: &SegmentRef,
        columns: &ColumnSelection,
    ) -> ScanStats {
        let acc = QueryAccounting::new();
        let mut scan = f
            .scan_whole_accounted_with_tenant(
                seg,
                TENANT,
                &LogQuery::new(i64::MIN, i64::MAX),
                columns,
                &acc,
            )
            .await
            .expect("scan")
            .expect("relevant");
        while scan.next_block().expect("next_block").is_some() {}
        scan.stats()
    }

    /// A one-column projection (`ts`/`stream_ref` implicit, `body` requested --
    /// the shape `SELECT ts, body FROM logs` resolves to, ADR-0087 decision 3)
    /// through the whole-segment fast path decodes exactly `wanted` pages per
    /// block and skips the rest. `wanted` is 3 by construction:
    /// `ColumnSelection::fixed_only().with_body()` never touches the `attrs`/
    /// `all_attrs` branch of `ColumnSelection::resolve` (`crates/ravel-logseg/
    /// src/columns.rs`), so it resolves to exactly `{COL_TS, COL_STREAM_REF,
    /// COL_BODY}` on any object, independent of that object's FIELD_DIR.
    ///
    /// `ColumnSelection::all()` on the SAME object is the baseline every other
    /// page belongs to: `decode_v4_block`'s `wanted` closure (`crates/
    /// ravel-logseg/src/reader.rs`) always returns `true` when `columns` is
    /// `None` (which `all()` resolves to), so it decodes every page and skips
    /// none. `all.pages_decoded - narrow.pages_decoded` is therefore exactly
    /// the page count `narrow.pages_skipped` reports; the two are cross-checked
    /// rather than asserted separately so a change that skips too many or too
    /// few pages cannot land as a bytes-shrink alone.
    ///
    /// Non-vacuity: with `decode_v4_block`'s `let wanted = |cid: u32| match
    /// columns { None => true, Some(set) => set.contains(&cid) };` changed to
    /// always return `true` regardless of `columns` (decode every page, i.e.
    /// reverting the projection this test pins), `narrow.pages_decoded` comes
    /// out equal to `all.pages_decoded` and the `pages_decoded` assertion below
    /// fails: `left: 48, right: 18` (confirmed by making exactly that edit and
    /// rerunning). 48 is `8 * N`, this fixture's real per-block page count: the
    /// seven fixed columns it populates (`ts`, `observed_ts`, `stream_ref`,
    /// `severity_num`, `severity_text`, `body`, `flags`) plus its one dynamic
    /// attribute column (`request.id`); `trace_id`/`span_id` are unset and get
    /// no page, and nothing here overflows into `attrs_raw`.
    #[tokio::test]
    async fn narrow_projection_decodes_only_wanted_columns() {
        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = LogSegmentFetcher::new(store);

        let narrow = ColumnSelection::fixed_only().with_body();
        let all_stats = drain(&f, &seg, &ColumnSelection::all()).await;
        let narrow_stats = drain(&f, &seg, &narrow).await;

        const WANTED: u64 = 3;
        assert_eq!(
            all_stats.pages_skipped, 0,
            "ColumnSelection::all() decodes every page and skips none"
        );
        assert_eq!(
            narrow_stats.pages_decoded,
            WANTED * N as u64,
            "one page per wanted column (ts, stream_ref, body) per block, over \
             {N} blocks"
        );
        assert_eq!(
            narrow_stats.pages_skipped,
            all_stats.pages_decoded - narrow_stats.pages_decoded,
            "every page the narrow selection didn't decode was skipped, not \
             fetched or decoded"
        );
        assert!(
            narrow_stats.page_bytes_decoded < all_stats.page_bytes_decoded,
            "projecting to one column must decode fewer bytes than decoding \
             every column: narrow {} B, all {} B",
            narrow_stats.page_bytes_decoded,
            all_stats.page_bytes_decoded
        );
        assert_eq!(
            narrow_stats.blocks_scanned, N as u32,
            "every block is still visited -- the fast path narrows decode, not \
             which blocks it reads"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod plan_skip_decidable_span_tests {
    //! Pins #782: `plan_segment`'s skip-decidable branch (a query whose only
    //! block-level predicate is a prune-only `NumRange` arm, #761) opens a
    //! `page_fetch` span around its `fetch_plan_sections` read and records
    //! that read's real request/byte counts on it, mirroring the pattern
    //! `plan_segment_fast` and `plan_segment_block_stats` already carry.
    //! Before the fix this branch's read was invisible to tracing: no span
    //! wrapped it at all, so a trace over a query taking this branch showed
    //! no `page_fetch` phase, only the ones plan_segment's other branches
    //! open.
    //!
    //! The same span capture pins the other figure those spans carry across the
    //! plan/scan boundary: `probe_misses`, which must count a tail-section miss
    //! exactly once per object per read path (#883, issue #885 review).

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{FieldSel, FieldType, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([21u8; 16]);
    const CONTENT_HASH: [u8; 32] = [23u8; 32];
    const KEY: &str = "t/skip-decidable-span.rlog";

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [21u8; 16],
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn record(ts: i64, code: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("code".to_string(), AttrValue::I64(code))],
        }
    }

    /// One block per record, so `n` records span `n` blocks -- irrelevant to
    /// this test's own claim (it never inspects `ScanStats.blocks_*`), kept
    /// only so the fixture matches the sibling modules' known-good shape.
    fn build_object(records: &[LogRecord]) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// `block_range_threshold(0)` routes every object through the block-range
    /// fetcher (`fetch_plan_sections`'s real path) rather than the small-object
    /// whole-read shortcut, so the skip-decidable branch's own read is the one
    /// under test.
    fn fetcher(store: Arc<MemoryStore>) -> LogSegmentFetcher {
        LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(0)
            .with_block_range(BlockRangeFetcher::new(store).with_whole_object_threshold(0))
    }

    /// The subset of a `page_fetch` span's fields this test asserts on.
    #[derive(Default, Debug)]
    struct Captured {
        signal: Option<String>,
        s3_requests: Option<u64>,
        s3_bytes: Option<u64>,
        probe_misses: Option<u64>,
    }

    struct FieldVisitor<'a>(&'a mut Captured);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "s3_requests" => self.0.s3_requests = Some(value),
                "s3_bytes" => self.0.s3_bytes = Some(value),
                "probe_misses" => self.0.probe_misses = Some(value),
                _ => {}
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "signal" {
                self.0.signal = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    /// Records every `page_fetch` span's fields as of its close, keyed by span
    /// id, so a test that opens exactly one such span can read back what it
    /// carried once `plan_segment` returns and the span guard drops.
    #[derive(Clone, Default)]
    struct PageFetchCollector {
        live: Arc<Mutex<HashMap<u64, (String, Captured)>>>,
        closed: Arc<Mutex<Vec<Captured>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PageFetchCollector {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() != "page_fetch" {
                return;
            }
            let mut captured = Captured::default();
            attrs.record(&mut FieldVisitor(&mut captured));
            if let Ok(mut live) = self.live.lock() {
                live.insert(
                    id.into_u64(),
                    (attrs.metadata().name().to_string(), captured),
                );
            }
        }

        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if let Ok(mut live) = self.live.lock()
                && let Some((_, captured)) = live.get_mut(&id.into_u64())
            {
                values.record(&mut FieldVisitor(captured));
            }
        }

        fn on_close(&self, id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let taken = self
                .live
                .lock()
                .ok()
                .and_then(|mut live| live.remove(&id.into_u64()));
            if let (Some((_, captured)), Ok(mut closed)) = (taken, self.closed.lock()) {
                closed.push(captured);
            }
        }
    }

    /// A query whose only block-level predicate is a prune-only `NumRange` arm
    /// takes `plan_segment`'s skip-decidable branch (`Self::plan_skip_decidable`:
    /// `content` and `stream_attrs` empty, `prune` nonempty and every arm a
    /// `NumRange`), which must open a `page_fetch` span around its
    /// `fetch_plan_sections` read and record that read's real request/byte
    /// counts on it -- not zero, not absent, the read this branch actually
    /// issued.
    ///
    /// Non-vacuity: with the `fetch_span`/`.instrument(fetch_span.clone())`
    /// wrapping removed from `plan_segment`'s skip-decidable branch (reverting
    /// #782, i.e. calling `fetch_plan_sections` bare the way this branch did
    /// before the fix), no span named `page_fetch` closes during this call at
    /// all, `closed.len()` comes out `0`, and the `expect("plan_segment's \
    /// skip-decidable branch opened exactly one page_fetch span")` below panics
    /// instead of the two field assertions running.
    ///
    /// The subscriber is installed with the crate's own proven pattern for a
    /// test-scoped tracing capture (`crates/ravel-query/src/fetcher.rs`'s
    /// `phase_spans_never_record_onto_the_ambient_span_when_disabled`): a
    /// `tracing_subscriber::registry()` layered with the collector and
    /// installed via `set_default()`, held for the test body's scope. This is
    /// thread-local, so it isolates cleanly from any `page_fetch` span a
    /// concurrently-running sibling test opens under its own subscriber (or
    /// none) -- unlike a process-global default, which every thread's spans
    /// would route through and which this test's own sibling `plan_segment`
    /// tests, several of which also open `page_fetch` spans, would pollute.
    #[tokio::test]
    // Holds the test_tracing serialization guard across `.await`; the
    // current-thread test runtime runs this future to completion with no other
    // task, so there is no deadlock risk the lint guards against.
    #[allow(clippy::await_holding_lock)]
    async fn skip_decidable_branch_opens_page_fetch_span() {
        // Counts a `debug_span!` `page_fetch` span, so it is subject to the same
        // process-global max-level race as
        // `a_carried_footer_is_counted_once_across_plan_and_scan`. The guard
        // pins the global level floor at TRACE and serializes against any
        // concurrent sub-DEBUG subscriber; see crate::test_tracing.
        let _serial = crate::test_tracing::guard();

        let collector = PageFetchCollector::default();
        let subscriber = tracing_subscriber::registry().with(collector.clone());
        let _guard = subscriber.set_default();

        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(|ts| record(ts, 500)).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher(store);
        let acc = QueryAccounting::new();

        let query = LogQuery::new(i64::MIN, i64::MAX).with_prune(Predicate::NumRange {
            field: FieldSel::Attr("code".into()),
            ty: FieldType::I64,
            min: Some(0i64 as u64),
            max: Some(1_000i64 as u64),
        });
        let (indices, _dirs, _stats, footer, carried) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");
        let count = indices.len();
        assert_eq!(count, N, "wide NumRange bound excludes no block");
        assert!(
            footer.is_some(),
            "skip-decidable branch forwards the parsed footer like the fast path does"
        );
        assert!(
            carried.is_none(),
            "skip-decidable branch reads no block byte, so it carries no whole-object bytes"
        );

        let closed = collector.closed.lock().expect("lock");
        let span = closed
            .first()
            .expect("plan_segment's skip-decidable branch opened exactly one page_fetch span");
        assert_eq!(
            closed.len(),
            1,
            "exactly one page_fetch span for this one plan_segment call, not zero and not several"
        );
        assert_eq!(span.signal.as_deref(), Some("logs"));
        assert!(
            span.s3_requests.is_some_and(|n| n > 0),
            "the span must carry the real request count fetch_plan_sections issued, got {:?}",
            span.s3_requests
        );
        assert!(
            span.s3_bytes.is_some(),
            "the span must record s3_bytes (structurally 0 for this branch, but present, not \
             Empty), got {:?}",
            span.s3_bytes
        );
    }

    /// A suffix probe pinned to start exactly at PAGE_DIR's end: it covers the
    /// footer and the trailer, so no plan read chases the footer, but neither of
    /// the two tail sections a version-4 scan locates pages through. Whichever
    /// layer probes this object therefore owes exactly two tail-section misses.
    fn suffix_missing_both_tail_sections(bytes: &[u8]) -> u64 {
        let total = bytes.len() as u64;
        let f = footer::open(bytes).expect("footer");
        let skip = f.section(kind::SKIP_IDX).expect("SKIP_IDX");
        let page = f.section(kind::PAGE_DIR).expect("PAGE_DIR");
        let suffix = total - (page.offset + page.len);
        assert!(
            skip.offset < total - suffix && page.offset < total - suffix,
            "the pinned probe must reach neither tail section"
        );
        suffix
    }

    fn fetcher_with_suffix(store: Arc<MemoryStore>, suffix: u64) -> LogSegmentFetcher {
        LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(0)
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_whole_object_threshold(0)
                    .with_suffix_len(suffix),
            )
    }

    /// End to end over the two phases: `plan_segment` reads a footer, hands it to
    /// a subset scan, and the object's tail-section probe misses are counted
    /// exactly once across the pair -- by the plan phase when the plan read
    /// located those sections, and by the scan when it did not.
    ///
    /// This is what pins the flag `tenant_bytes_with_footer` sets on a carried
    /// footer. The fetcher-level tests in `tests/log_block_range.rs` and
    /// `tests/log_page_dir_fetch.rs` construct the [`CarriedFooter`] by hand and
    /// so cannot catch a wrong flag here.
    ///
    /// Both plan branches count the tail misses themselves (ADR-2414 decision
    /// A1: each reads the segment's directories), so the flag is `true` for
    /// every carried footer. Prove-the-test: hardcoding it to `false` makes the
    /// scan count the same two sections again, so both cases read a scan-phase 2
    /// against the expected 0 and a total of 4 against 2.
    #[tokio::test]
    // Holds the test_tracing serialization guard across `.await`; the
    // current-thread test runtime runs this future to completion with no other
    // task, so there is no deadlock risk the lint guards against.
    #[allow(clippy::await_holding_lock)]
    async fn a_carried_footer_is_counted_once_across_plan_and_scan() {
        // The `page_fetch` spans this counts are `debug_span!`s, so they are
        // enabled only while the process-global max-level hint is at least
        // DEBUG. That hint is a single atomic recomputed by callsite churn on
        // every thread, and a default-less thread lowers it to OFF, while a
        // sibling INFO-only subscriber lowers it to INFO; either silently
        // disables one of the two spans and drops the count to 1. The guard
        // pins the floor at TRACE and serializes against the sub-DEBUG
        // subscriber (see crate::test_tracing).
        let _serial = crate::test_tracing::guard();

        // (query, plan-phase misses, scan-phase misses). The predicate-free
        // query takes `plan_segment_fast`, whose `fetch_plan_directories` reads
        // and counts both tail sections. The NumRange query takes the
        // skip-decidable branch, whose `fetch_plan_sections` does the same. The
        // scan must count neither. Either way the total is 2.
        let cases = [
            (LogQuery::new(i64::MIN, i64::MAX), 2u64, 0u64),
            (
                LogQuery::new(i64::MIN, i64::MAX).with_prune(Predicate::NumRange {
                    field: FieldSel::Attr("code".into()),
                    ty: FieldType::I64,
                    min: Some(0i64 as u64),
                    max: Some(1_000i64 as u64),
                }),
                2,
                0,
            ),
        ];

        for (query, want_plan, want_scan) in cases {
            let collector = PageFetchCollector::default();
            let subscriber = tracing_subscriber::registry().with(collector.clone());
            let _guard = subscriber.set_default();

            const N: usize = 6;
            let records: Vec<LogRecord> = (0..N as i64).map(|ts| record(ts, 500)).collect();
            let bytes = build_object(&records);
            let suffix = suffix_missing_both_tail_sections(&bytes);
            let seg = seg_ref(bytes.len() as u64, &records);
            let store = store_with_object(bytes).await;
            let f = fetcher_with_suffix(store, suffix);
            let acc = QueryAccounting::new();

            let (indices, _dirs, _stats, footer, _carried) = f
                .plan_segment(&seg, TENANT, &query, &acc)
                .await
                .expect("plan_segment")
                .expect("relevant segment");
            let count = indices.len();
            assert!(footer.is_some(), "both branches carry their footer forward");

            let indices: Vec<usize> = (0..count).collect();
            let scan = f
                .scan_accounted_with_tenant_subset(
                    &seg,
                    TENANT,
                    &query,
                    &ColumnSelection::all(),
                    &indices,
                    footer.as_ref(),
                    None,
                    &acc,
                )
                .await
                .expect("subset scan")
                .expect("relevant segment");
            drop(scan);

            let closed = collector.closed.lock().expect("lock");
            assert_eq!(
                closed.len(),
                2,
                "one page_fetch span for the plan read and one for the scan read"
            );
            assert_eq!(
                closed[0].probe_misses,
                Some(want_plan),
                "plan phase misses for {query:?}"
            );
            assert_eq!(
                closed[1].probe_misses,
                Some(want_scan),
                "scan phase misses for {query:?}"
            );
            assert_eq!(
                closed[0].probe_misses.unwrap_or_default()
                    + closed[1].probe_misses.unwrap_or_default(),
                2,
                "one count per missed tail section, whichever phase issued the probe"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod fetch_stream_dir_tests {
    //! Pins two review findings on `LogSegmentFetcher::fetch_stream_dir`
    //! (#1106): the above-threshold branch's STREAM_DIR probe must report
    //! through the same `page_fetch` span and `record_probe_misses` channel
    //! every other plan-phase read carries, and the below-threshold and
    //! above-threshold branches must agree on the absent-STREAM_DIR
    //! tolerance the method's own doc comment promises.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([31u8; 16]);
    const CONTENT_HASH: [u8; 32] = [33u8; 32];
    const KEY: &str = "t/fetch-stream-dir.rlog";

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [31u8; 16],
            shard: 0,
            writer_id: [6u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn record(ts: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![],
        }
    }

    fn build_object(records: &[LogRecord]) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// The subset of a `page_fetch` span's fields this test asserts on.
    #[derive(Default, Debug)]
    struct Captured {
        signal: Option<String>,
        s3_requests: Option<u64>,
        s3_bytes: Option<u64>,
        probe_misses: Option<u64>,
    }

    struct FieldVisitor<'a>(&'a mut Captured);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "s3_requests" => self.0.s3_requests = Some(value),
                "s3_bytes" => self.0.s3_bytes = Some(value),
                "probe_misses" => self.0.probe_misses = Some(value),
                _ => {}
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "signal" {
                self.0.signal = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    /// Records every `page_fetch` span's fields as of its close, the same
    /// test-scoped tracing capture `plan_skip_decidable_span_tests` uses.
    #[derive(Clone, Default)]
    struct PageFetchCollector {
        live: Arc<Mutex<HashMap<u64, Captured>>>,
        closed: Arc<Mutex<Vec<Captured>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PageFetchCollector {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() != "page_fetch" {
                return;
            }
            let mut captured = Captured::default();
            attrs.record(&mut FieldVisitor(&mut captured));
            if let Ok(mut live) = self.live.lock() {
                live.insert(id.into_u64(), captured);
            }
        }

        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if let Ok(mut live) = self.live.lock()
                && let Some(captured) = live.get_mut(&id.into_u64())
            {
                values.record(&mut FieldVisitor(captured));
            }
        }

        fn on_close(&self, id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let taken = self
                .live
                .lock()
                .ok()
                .and_then(|mut live| live.remove(&id.into_u64()));
            if let (Some(captured), Ok(mut closed)) = (taken, self.closed.lock()) {
                closed.push(captured);
            }
        }
    }

    /// `block_range_threshold(0)` routes every nonzero-size object through the
    /// ranged branch, so the STREAM_DIR probe under test is the one this test
    /// exercises rather than the whole-object shortcut.
    fn fetcher_above_threshold(store: Arc<MemoryStore>) -> LogSegmentFetcher {
        LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(0)
            .with_block_range(BlockRangeFetcher::new(store).with_whole_object_threshold(0))
    }

    /// Finding 1 (#1106): the above-threshold
    /// branch of `fetch_stream_dir` must open the `page_fetch` span and call
    /// `record_probe_misses`, exactly like `plan_segment_fast`,
    /// `plan_segment`'s skip-decidable branch, and `plan_segment_block_stats`
    /// already do, so the STREAM_DIR read it issues shows up in the trace and
    /// in the probe-miss counter instead of being invisible on both.
    ///
    /// Non-vacuity: with the `fetch_span`/`.instrument` wrapping reverted to
    /// the bare `probe_footer`/`plan_section_raw` calls this method had before
    /// the fix, no span named `page_fetch` closes during this call,
    /// `closed.len()` comes out `0`, and the `expect` below panics instead of
    /// the field assertions running.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn above_threshold_branch_opens_page_fetch_span_and_records_probe_misses() {
        let _serial = crate::test_tracing::guard();

        let collector = PageFetchCollector::default();
        let subscriber = tracing_subscriber::registry().with(collector.clone());
        let _guard = subscriber.set_default();

        const N: usize = 4;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher_above_threshold(store);
        let acc = QueryAccounting::new();

        let entries = f
            .fetch_stream_dir(&seg, TENANT, &acc)
            .await
            .expect("fetch_stream_dir")
            .expect("STREAM_DIR present");
        assert_eq!(entries.len(), 1, "one stream, every record shares it");

        let closed = collector.closed.lock().expect("lock");
        assert_eq!(
            closed.len(),
            1,
            "exactly one page_fetch span for this one fetch_stream_dir call"
        );
        let span = &closed[0];
        assert_eq!(span.signal.as_deref(), Some("logs"));
        assert!(
            span.s3_requests.is_some_and(|n| n > 0),
            "must carry the real request count probe_footer/plan_section_raw issued, got {:?}",
            span.s3_requests
        );
        assert!(
            span.probe_misses.is_some(),
            "probe_misses must be recorded (structurally 0 or more, but present, not Empty), \
             got {:?}",
            span.probe_misses
        );
    }

    /// Finding 2 (#1106): the doc comment on
    /// `fetch_stream_dir` says an absent STREAM_DIR section returns `None`
    /// regardless of which branch answers the read. `RlogWriter::build_object`
    /// always writes a STREAM_DIR section unconditionally (see the
    /// `push_section(&mut object, &mut sections, kind::STREAM_DIR, ...)` call
    /// both encoder paths make, `crates/ravel-logseg/src/writer.rs:716` and
    /// `:1536`) -- there is no writer knob to omit it -- so the absent-section
    /// case is not reachable through a real written object today, matching
    /// the method's own doc ("never observed on a real object today"). This
    /// test instead pins what the finding actually requires: the two branches
    /// must agree on the same object. It drives one fixture through both the
    /// below-threshold whole-object branch (the default threshold, well above
    /// this small fixture's size) and the above-threshold ranged branch
    /// (`with_block_range_threshold(0)`), and asserts they decode the same
    /// STREAM_DIR entries.
    #[tokio::test]
    async fn both_threshold_branches_agree_on_the_same_object() {
        const N: usize = 3;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let seg = seg_ref(total, &records);

        // Independent ground truth for the decompressed-byte pins below (issue
        // #1401 finding 3): STREAM_DIR is a whole-read directory section, always
        // stored COMP_ZSTD, so its uncomp_len is the exact bytes one decode of it
        // produces, read straight from the footer rather than through either
        // branch under test.
        let stream_desc = *footer::open(&bytes)
            .expect("open")
            .section(kind::STREAM_DIR)
            .expect("STREAM_DIR present");
        assert_eq!(
            stream_desc.comp,
            footer::COMP_ZSTD,
            "fixture STREAM_DIR must be zstd for this test"
        );
        let expected = stream_desc.uncomp_len;

        let below_store = store_with_object(bytes.clone()).await;
        let below = LogSegmentFetcher::new(below_store);
        assert!(
            total <= below.block_range_threshold(),
            "fixture must be small enough to take the below-threshold branch by default"
        );
        let below_acc = QueryAccounting::new();
        let mut below_entries = below
            .fetch_stream_dir(&seg, TENANT, &below_acc)
            .await
            .expect("fetch_stream_dir (below threshold)")
            .expect("STREAM_DIR present");
        assert_eq!(
            below_acc.snapshot().decompressed_bytes,
            expected,
            "below-threshold branch charges exactly STREAM_DIR's zstd uncomp_len"
        );

        let above_store = store_with_object(bytes).await;
        let above = fetcher_above_threshold(above_store);
        let above_acc = QueryAccounting::new();
        let mut above_entries = above
            .fetch_stream_dir(&seg, TENANT, &above_acc)
            .await
            .expect("fetch_stream_dir (above threshold)")
            .expect("STREAM_DIR present");
        assert_eq!(
            above_acc.snapshot().decompressed_bytes,
            expected,
            "above-threshold branch charges exactly STREAM_DIR's zstd uncomp_len"
        );

        below_entries.sort_by_key(|(id, _)| *id);
        above_entries.sort_by_key(|(id, _)| *id);
        assert_eq!(
            above_entries, below_entries,
            "both branches must decode the same STREAM_DIR entries for the same object"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod plan_block_stats_tests {
    //! Tests for the SKIP_IDX-only block-stats fast path (#698 deliverable 2):
    //! [`LogSegmentFetcher::plan_segment_block_stats`] answers a segment's exact
    //! per-block record count and per-numeric-column min/max/null_count from the
    //! footer plus the SKIP_IDX section, fetching no BLOCKS byte.
    //!
    //! The baselines here are computed by fully decoding the same fixture's
    //! blocks through the ordinary reader path
    //! ([`LogSegmentScan::next_block`], one `Vec<LogRecord>` per block) and
    //! folding the records by hand. Nothing in a baseline reads the skip index,
    //! so a wrong stored stat or a wrong containment rule shows up as a
    //! mismatch rather than cancelling out.

    use super::*;
    use async_trait::async_trait;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{FieldType, RlogWriter, read_section, stream_attrs_bytes};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, ListPage, ObjectMeta, PageToken, PutOptions, PutOutcome,
    };
    use ravel_types::logstream::log_stream_id;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const CONTENT_HASH: [u8; 32] = [9u8; 32];
    const KEY: &str = "t/stats.rlog";
    /// The numeric attribute names the fixtures put on their records. `latency`
    /// is F64 (negative values and a NaN, so a naive `u64`-bits fold disagrees
    /// with `merge_stats`'s `total_cmp` one) and `code` is I64 (negative too).
    const LATENCY: &str = "latency";
    const CODE: &str = "code";

    /// Counts `get` calls and records the range of each, so a test can pin both
    /// how many reads happened and which bytes they covered. Everything else
    /// delegates to the inner [`MemoryStore`].
    struct RangeRecordingStore {
        inner: Arc<MemoryStore>,
        gets: AtomicU64,
        ranges: Mutex<Vec<GetRange>>,
    }

    impl RangeRecordingStore {
        fn new(inner: Arc<MemoryStore>) -> Self {
            RangeRecordingStore {
                inner,
                gets: AtomicU64::new(0),
                ranges: Mutex::new(Vec::new()),
            }
        }

        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::SeqCst)
        }

        fn ranges(&self) -> Vec<GetRange> {
            self.ranges.lock().expect("ranges lock").clone()
        }
    }

    #[async_trait]
    impl ObjectStoreBackend for RangeRecordingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.ranges.lock().expect("ranges lock").push(range);
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                multipart: false,
                ..self.inner.capabilities()
            }
        }
    }

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    /// One record on stream `service`, carrying the two numeric attributes when
    /// `nums` is `Some`. A record built with `None` resolves neither name (no
    /// stream layer carries them either), so it counts only in `null_count`.
    fn record(service: &str, ts: i64, nums: Option<(f64, i64)>) -> LogRecord {
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str(service.to_string()),
        )];
        let attrs = match nums {
            Some((latency, code)) => vec![
                (LATENCY.to_string(), AttrValue::F64(latency)),
                (CODE.to_string(), AttrValue::I64(code)),
            ],
            None => Vec::new(),
        };
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    /// Two records per block, so a block can hold a NaN row alongside a row with
    /// a real value. That matters: a block whose only `latency` row is NaN gets
    /// `min_bits == max_bits == 0` from the writer (`f64_stat`'s `unwrap_or(0)`),
    /// which would fold `+0.0` into the merged bounds and make the hand baseline
    /// below wrong for a reason that has nothing to do with this fast path.
    fn build_object(records: &[LogRecord]) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 2,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        w.finish().expect("finish")
    }

    fn seg_ref(size: u64, records: &[LogRecord]) -> SegmentRef {
        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// The object's footer+trailer byte length. A probe suffix of exactly this
    /// covers the footer with no range chase and reaches no other section, so
    /// SKIP_IDX costs its own measurable GET.
    fn footer_region_len(bytes: &[u8]) -> u64 {
        let trailer = &bytes[bytes.len() - footer::TRAILER_LEN..];
        let footer_len = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        u64::from(footer_len) + footer::TRAILER_LEN as u64
    }

    /// The absolute `[start, end)` of the object's BLOCKS and SKIP_IDX sections.
    fn section_extents(bytes: &[u8]) -> ((u64, u64), (u64, u64)) {
        let f = footer::open(bytes).expect("footer");
        let b = f.section(kind::BLOCKS).expect("BLOCKS");
        let s = f.section(kind::SKIP_IDX).expect("SKIP_IDX");
        ((b.offset, b.offset + b.len), (s.offset, s.offset + s.len))
    }

    /// `(latency_column_id, code_column_id)` from the object's FIELD_DIR. A
    /// name-to-id lookup, independent of anything the fast path computes.
    fn numeric_column_ids(bytes: &[u8]) -> (u32, u32) {
        let f = footer::open(bytes).expect("footer");
        let desc = f.section(kind::FIELD_DIR).expect("FIELD_DIR");
        let raw = read_section(bytes, desc, &RlogConfig::default()).expect("field dir raw");
        let dir = FieldDir::decode(&raw, 1 << 20).expect("field dir decode");
        (
            dir.column(LATENCY, FieldType::F64)
                .expect("latency column")
                .column_id,
            dir.column(CODE, FieldType::I64)
                .expect("code column")
                .column_id,
        )
    }

    async fn store_with_object(bytes: Vec<u8>) -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        store
    }

    /// A fetcher whose probe reads exactly the footer region, so SKIP_IDX is a
    /// distinct, countable GET and no read can accidentally cover a block.
    /// `block_range_threshold = 0` puts every nonempty fixture above the gate.
    fn fetcher(store: Arc<dyn ObjectStoreBackend>, probe: u64) -> LogSegmentFetcher {
        LogSegmentFetcher::new(store.clone())
            .with_block_range_threshold(0)
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(probe)
                    .with_whole_object_threshold(0),
            )
    }

    /// One block as the reader actually decodes it: its rows' ts span and their
    /// resolved numeric attribute values. This is the baseline side of every
    /// assertion below and reads no skip-index byte.
    #[derive(Debug)]
    struct DecodedBlock {
        min_ts: i64,
        max_ts: i64,
        records: Vec<LogRecord>,
    }

    /// Fully decodes the object block by block through the ordinary scan path.
    /// The query is predicate-free over the whole ts axis, so nothing is pruned
    /// and the yielded blocks are the segment's blocks in `SkipIndex::l0` order.
    async fn decode_blocks(
        store: Arc<dyn ObjectStoreBackend>,
        seg: &SegmentRef,
    ) -> Vec<DecodedBlock> {
        let f = LogSegmentFetcher::new(store);
        let acc = QueryAccounting::new();
        let mut scan = f
            .scan_accounted_with_tenant(
                seg,
                TENANT,
                &LogQuery::new(i64::MIN, i64::MAX),
                &ColumnSelection::all(),
                &acc,
            )
            .await
            .expect("scan")
            .expect("relevant");
        let mut out = Vec::new();
        while let Some(records) = scan.next_block().expect("next_block") {
            let min_ts = records
                .iter()
                .map(|r| r.ts_ns)
                .min()
                .expect("nonempty block");
            let max_ts = records
                .iter()
                .map(|r| r.ts_ns)
                .max()
                .expect("nonempty block");
            out.push(DecodedBlock {
                min_ts,
                max_ts,
                records,
            });
        }
        out
    }

    /// The hand-folded expectation over the decoded blocks a `[ts_min, ts_max]`
    /// window fully contains: total record count, the two numeric columns'
    /// bounds/null counts/NaN flags, and the indices of the blocks the window
    /// only clips.
    #[derive(Debug, PartialEq)]
    struct Baseline {
        record_count: u64,
        latency_min: f64,
        latency_max: f64,
        latency_nulls: u32,
        latency_has_nan: bool,
        code_min: i64,
        code_max: i64,
        code_nulls: u32,
        partial: Vec<usize>,
    }

    fn attr_f64(rec: &LogRecord, name: &str) -> Option<f64> {
        rec.attrs.iter().find_map(|(k, v)| match v {
            AttrValue::F64(f) if k == name => Some(*f),
            _ => None,
        })
    }

    fn attr_i64(rec: &LogRecord, name: &str) -> Option<i64> {
        rec.attrs.iter().find_map(|(k, v)| match v {
            AttrValue::I64(i) if k == name => Some(*i),
            _ => None,
        })
    }

    /// Folds `blocks` by hand under the SAME containment rule the fast path is
    /// supposed to use, with `contains` supplied by the caller so a test can
    /// deliberately fold under the WRONG rule (overlap instead of containment)
    /// and show the assertion failing.
    fn baseline(blocks: &[DecodedBlock], ts_min: i64, ts_max: i64) -> Baseline {
        let mut record_count = 0u64;
        let mut latency_min = f64::INFINITY;
        let mut latency_max = f64::NEG_INFINITY;
        let mut latency_nulls = 0u32;
        let mut latency_has_nan = false;
        let mut code_min = i64::MAX;
        let mut code_max = i64::MIN;
        let mut code_nulls = 0u32;
        let mut partial = Vec::new();
        for (i, b) in blocks.iter().enumerate() {
            if ts_min <= b.min_ts && b.max_ts <= ts_max {
                record_count += b.records.len() as u64;
                for rec in &b.records {
                    match attr_f64(rec, LATENCY) {
                        // NaN is counted in `has_nan` and excluded from the
                        // bounds (ADR-0095), and is NOT a null.
                        Some(v) if v.is_nan() => latency_has_nan = true,
                        Some(v) => {
                            if v.total_cmp(&latency_min).is_lt() {
                                latency_min = v;
                            }
                            if v.total_cmp(&latency_max).is_gt() {
                                latency_max = v;
                            }
                        }
                        None => latency_nulls += 1,
                    }
                    match attr_i64(rec, CODE) {
                        Some(v) => {
                            code_min = code_min.min(v);
                            code_max = code_max.max(v);
                        }
                        None => code_nulls += 1,
                    }
                }
            } else if b.max_ts >= ts_min && b.min_ts <= ts_max {
                partial.push(i);
            }
        }
        Baseline {
            record_count,
            latency_min,
            latency_max,
            latency_nulls,
            latency_has_nan,
            code_min,
            code_max,
            code_nulls,
            partial,
        }
    }

    /// Reads the report's two numeric stats back into the baseline's shape.
    fn as_baseline(report: &BlockStatsReport, latency_col: u32, code_col: u32) -> Baseline {
        let latency = report
            .stats
            .iter()
            .find(|s| s.column_id == latency_col)
            .expect("latency stat");
        let code = report
            .stats
            .iter()
            .find(|s| s.column_id == code_col)
            .expect("code stat");
        Baseline {
            record_count: report.record_count,
            latency_min: f64::from_bits(latency.min_bits),
            latency_max: f64::from_bits(latency.max_bits),
            latency_nulls: latency.null_count,
            latency_has_nan: latency.has_nan,
            code_min: code.min_bits as i64,
            code_max: code.max_bits as i64,
            code_nulls: code.null_count,
            partial: report.partial_block_indices.clone(),
        }
    }

    /// Fixture A: one stream, ts 0..=9, two records per block, so five
    /// ts-ordered blocks `[0,1] [2,3] [4,5] [6,7] [8,9]`. The window `[1, 8]`
    /// contains the middle three and clips exactly one at each end -- the
    /// realistic two-partial case.
    ///
    /// The numeric payload is chosen so a wrong fold is visible rather than
    /// coincidentally right: block `[2,3]` carries `-2.5` and a NaN, block
    /// `[4,5]` carries `7.5` and `1.0`, block `[6,7]` carries no numeric
    /// attribute at all (so `merge_stats`'s "no entry means the whole block is
    /// null" arm has to fire), and the two clipped blocks carry `1000.0`/`999`
    /// -- values that appear in the answer only if containment is wrong.
    fn fixture_a() -> Vec<LogRecord> {
        vec![
            record("api", 0, Some((1000.0, 999))),
            record("api", 1, Some((1000.0, 999))),
            record("api", 2, Some((-2.5, -7))),
            record("api", 3, Some((f64::NAN, 3))),
            record("api", 4, Some((7.5, 5))),
            record("api", 5, Some((1.0, 9))),
            record("api", 6, None),
            record("api", 7, None),
            record("api", 8, Some((1000.0, 999))),
            record("api", 9, Some((1000.0, 999))),
        ]
    }

    /// Fixture B: three streams, four records each, two records per block. RLOG
    /// sorts rows by `(stream_ref, ts)` before cutting blocks, so each stream
    /// contributes two blocks and the six blocks' ts spans interleave rather
    /// than forming one ordered sequence:
    /// `[0,10] [100,110]`, `[5,15] [105,115]`, `[2,12] [102,112]`.
    /// The window `[4, 110]` contains two of them and clips FOUR, which is what
    /// makes a `partial_block_indices` hardcoded to the two-partial shape wrong.
    fn fixture_b() -> Vec<LogRecord> {
        let mut out = Vec::new();
        for (service, tss) in [
            ("api", [0i64, 10, 100, 110]),
            ("worker", [5, 15, 105, 115]),
            ("cron", [2, 12, 102, 112]),
        ] {
            for ts in tss {
                out.push(record(service, ts, Some((ts as f64, ts))));
            }
        }
        out
    }

    /// The fast path's `record_count` and merged `stats` equal a baseline folded
    /// by hand over the same blocks decoded through the ordinary reader, and the
    /// read that produced them is exactly the footer probe plus the SKIP_IDX
    /// section: two GETs, neither touching a BLOCKS byte.
    ///
    /// Non-vacuity: this test was first run with
    /// `plan_segment_block_stats`'s containment arm relaxed from
    /// `ts_min <= entry.min_ts && entry.max_ts <= ts_max` to the overlap test
    /// `entry.max_ts >= ts_min && entry.min_ts <= ts_max` (the `if` at the head
    /// of the `for (i, entry) in skip.l0.iter().enumerate()` loop). That folds
    /// the two clipped blocks in, and the assertion fails with `record_count`
    /// 10 instead of 6, `latency_max` 1000.0 instead of 7.5, `code_max` 999
    /// instead of 9, and an empty `partial_block_indices` instead of `[0, 4]`.
    #[tokio::test]
    async fn block_stats_match_a_decoded_baseline_reading_no_block_bytes() {
        let records = fixture_a();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let probe = footer_region_len(&bytes);
        let ((blocks_start, blocks_end), (skip_start, skip_end)) = section_extents(&bytes);
        let (latency_col, code_col) = numeric_column_ids(&bytes);
        let seg = seg_ref(total, &records);

        let mem = store_with_object(bytes).await;
        let baseline_blocks = decode_blocks(mem.clone() as Arc<dyn ObjectStoreBackend>, &seg).await;
        assert_eq!(
            baseline_blocks.len(),
            5,
            "two records per block over ten rows"
        );

        let store = Arc::new(RangeRecordingStore::new(mem));
        let f = fetcher(store.clone() as Arc<dyn ObjectStoreBackend>, probe);
        let acc = QueryAccounting::new();

        let (ts_min, ts_max) = (1i64, 8i64);
        let report = f
            .plan_segment_block_stats(&seg, TENANT, &LogQuery::new(ts_min, ts_max), &acc)
            .await
            .expect("plan_segment_block_stats")
            .expect("fast path applies");

        let want = baseline(&baseline_blocks, ts_min, ts_max);
        assert_eq!(want.record_count, 6, "three fully contained two-row blocks");
        assert_eq!(want.partial, vec![0, 4], "one clipped block at each end");
        assert_eq!(as_baseline(&report, latency_col, code_col), want);
        // Spelled out, so a change to `baseline` cannot quietly move both sides.
        assert_eq!(report.record_count, 6);
        assert_eq!(report.partial_block_indices, vec![0, 4]);
        let latency = report
            .stats
            .iter()
            .find(|s| s.column_id == latency_col)
            .expect("latency stat");
        assert_eq!(
            f64::from_bits(latency.min_bits),
            -2.5,
            "total_cmp order: the negative is the min, not the u64-bits maximum"
        );
        assert_eq!(f64::from_bits(latency.max_bits), 7.5);
        assert!(latency.has_nan, "block [2,3] carries a NaN latency");
        assert_eq!(
            latency.null_count, 2,
            "block [6,7] carries no stat: all its rows are null"
        );
        let code = report
            .stats
            .iter()
            .find(|s| s.column_id == code_col)
            .expect("code stat");
        assert_eq!(code.min_bits as i64, -7);
        assert_eq!(code.max_bits as i64, 9);
        assert_eq!(code.null_count, 2);

        // Exactly two reads: the suffix probe covering the footer region, and
        // the SKIP_IDX section. No BLOCKS byte moved.
        assert_eq!(store.get_count(), 2, "footer probe + SKIP_IDX section only");
        let ranges = store.ranges();
        assert_eq!(
            ranges[0],
            GetRange::Suffix(probe),
            "the first read is the etag-establishing suffix probe"
        );
        assert_eq!(
            ranges[1],
            GetRange::Range(skip_start, skip_end),
            "the second read is exactly the SKIP_IDX section extent"
        );
        for range in &ranges {
            let (start, end) = match *range {
                GetRange::Range(s, e) => (s, e),
                GetRange::Suffix(n) => (total - n, total),
                GetRange::Full => (0, total),
            };
            assert!(
                end <= blocks_start || start >= blocks_end,
                "read {range:?} overlaps BLOCKS [{blocks_start}, {blocks_end})"
            );
        }
        assert_eq!(
            acc.snapshot().total_s3_bytes(),
            probe + (skip_end - skip_start),
            "bytes read are the probe plus the SKIP_IDX section, nothing else"
        );
    }

    /// `partial_block_indices` is the true overlapping-but-not-contained set,
    /// however large. A three-stream segment's blocks interleave in ts, so the
    /// window `[4, 110]` clips FOUR of the six blocks -- a length any
    /// "one partial at each end" assumption gets wrong.
    #[tokio::test]
    async fn partial_block_indices_is_not_two_when_block_spans_interleave() {
        let records = fixture_b();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let probe = footer_region_len(&bytes);
        let ((blocks_start, blocks_end), (skip_start, skip_end)) = section_extents(&bytes);
        let (latency_col, code_col) = numeric_column_ids(&bytes);
        let seg = seg_ref(total, &records);

        let mem = store_with_object(bytes).await;
        let baseline_blocks = decode_blocks(mem.clone() as Arc<dyn ObjectStoreBackend>, &seg).await;
        assert_eq!(baseline_blocks.len(), 6, "three streams, two blocks each");
        let spans: Vec<(i64, i64)> = baseline_blocks
            .iter()
            .map(|b| (b.min_ts, b.max_ts))
            .collect();
        assert!(
            spans.windows(2).any(|w| w[1].0 < w[0].0),
            "the fixture's block spans must genuinely interleave, not be ts-ordered: {spans:?}"
        );

        let store = Arc::new(RangeRecordingStore::new(mem));
        let f = fetcher(store.clone() as Arc<dyn ObjectStoreBackend>, probe);
        let acc = QueryAccounting::new();

        let (ts_min, ts_max) = (4i64, 110i64);
        let report = f
            .plan_segment_block_stats(&seg, TENANT, &LogQuery::new(ts_min, ts_max), &acc)
            .await
            .expect("plan_segment_block_stats")
            .expect("fast path applies");

        let want = baseline(&baseline_blocks, ts_min, ts_max);
        assert_eq!(
            want.partial.len(),
            4,
            "four of six blocks are clipped by [4, 110]: {spans:?}"
        );
        assert_eq!(want.record_count, 4, "two fully contained two-row blocks");
        assert_eq!(as_baseline(&report, latency_col, code_col), want);
        assert_eq!(report.partial_block_indices.len(), 4);
        assert_eq!(report.record_count, 4);

        assert_eq!(store.get_count(), 2, "footer probe + SKIP_IDX section only");
        for range in &store.ranges() {
            let (start, end) = match *range {
                GetRange::Range(s, e) => (s, e),
                GetRange::Suffix(n) => (total - n, total),
                GetRange::Full => (0, total),
            };
            assert!(
                end <= blocks_start || start >= blocks_end,
                "read {range:?} overlaps BLOCKS [{blocks_start}, {blocks_end})"
            );
        }
        assert_eq!(
            acc.snapshot().total_s3_bytes(),
            probe + (skip_end - skip_start)
        );
    }

    /// A non-empty `query.erasure` makes the function decline, with no GET at
    /// all, even though every other condition holds. Erasure drops rows a
    /// contained block's stored `record_count` still counts, so reporting the
    /// stored figures would over-report.
    #[tokio::test]
    async fn erasure_present_declines_the_fast_path() {
        let records = fixture_a();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let probe = footer_region_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = Arc::new(RangeRecordingStore::new(store_with_object(bytes).await));
        let f = fetcher(store.clone() as Arc<dyn ObjectStoreBackend>, probe);
        let acc = QueryAccounting::new();

        // Identical to the passing case above except for the erasure list.
        let query = LogQuery::new(1, 8).with_erasure(vec![ErasurePredicate::windowless(vec![(
            "request.id".into(),
            "r0".into(),
        )])]);
        let got = f
            .plan_segment_block_stats(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment_block_stats");
        assert!(got.is_none(), "erasure pending: fail closed");
        assert_eq!(store.get_count(), 0, "declining costs no GET");

        // Control: the same query without erasure does fire, so the decline is
        // attributable to the erasure list and nothing else.
        let acc = QueryAccounting::new();
        assert!(
            f.plan_segment_block_stats(&seg, TENANT, &LogQuery::new(1, 8), &acc)
                .await
                .expect("plan_segment_block_stats")
                .is_some(),
            "without erasure the same query takes the fast path"
        );
    }

    /// An object whose size sits exactly AT `block_range_threshold` declines,
    /// matching `plan_segment_fast`'s `>` (not `>=`) convention: at or below the
    /// threshold the whole-object funnel already pays one GET, and
    /// `fetch_skip_index` has no whole-object crossover of its own.
    #[tokio::test]
    async fn object_exactly_at_the_block_range_threshold_declines() {
        let records = fixture_a();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let probe = footer_region_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = Arc::new(RangeRecordingStore::new(store_with_object(bytes).await));
        let inner = store.clone() as Arc<dyn ObjectStoreBackend>;
        let at = LogSegmentFetcher::new(inner.clone())
            .with_block_range_threshold(total)
            .with_block_range(
                BlockRangeFetcher::new(inner)
                    .with_suffix_len(probe)
                    .with_whole_object_threshold(0),
            );
        let acc = QueryAccounting::new();
        let got = at
            .plan_segment_block_stats(&seg, TENANT, &LogQuery::new(1, 8), &acc)
            .await
            .expect("plan_segment_block_stats");
        assert!(got.is_none(), "at the threshold, not above it: fail closed");
        assert_eq!(store.get_count(), 0, "declining costs no GET");

        // One byte below the object size puts it above the threshold, and the
        // same call now fires: the boundary is the only thing being tested.
        let above = LogSegmentFetcher::new(store.clone() as Arc<dyn ObjectStoreBackend>)
            .with_block_range_threshold(total - 1)
            .with_block_range(
                BlockRangeFetcher::new(store.clone() as Arc<dyn ObjectStoreBackend>)
                    .with_suffix_len(probe)
                    .with_whole_object_threshold(0),
            );
        assert!(
            above
                .plan_segment_block_stats(&seg, TENANT, &LogQuery::new(1, 8), &acc)
                .await
                .expect("plan_segment_block_stats")
                .is_some(),
            "one byte above the threshold the fast path fires"
        );
    }

    /// A segment the catalog summary proves irrelevant declines with no GET, the
    /// same `ts_range_relevant` pre-check `plan_segment` and `tenant_bytes`
    /// apply.
    #[tokio::test]
    async fn irrelevant_segment_declines_without_a_get() {
        let records = fixture_a();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let probe = footer_region_len(&bytes);
        let seg = seg_ref(total, &records);
        let store = Arc::new(RangeRecordingStore::new(store_with_object(bytes).await));
        let f = fetcher(store.clone() as Arc<dyn ObjectStoreBackend>, probe);
        let acc = QueryAccounting::new();

        // The segment spans ts 0..=9; this window is entirely above it.
        let got = f
            .plan_segment_block_stats(&seg, TENANT, &LogQuery::new(1_000, 2_000), &acc)
            .await
            .expect("plan_segment_block_stats");
        assert!(got.is_none(), "ts-irrelevant segment");
        assert_eq!(store.get_count(), 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod assembly_buffer_tests {
    //! Issue #2066: an [`ObjectAssembler`] holds the regions it placed and
    //! nothing else, refuses every range it did not place, and charges the
    //! gauge and the fetch budget for exactly those regions until the reader
    //! drops the bytes.

    use super::*;

    fn gauge() -> Arc<AssemblyGauge> {
        Arc::new(AssemblyGauge::default())
    }

    /// A read inside a placed region returns its bytes; a range no region
    /// holds is a typed `Corrupt` wrapping `Unplaced`, never zeros.
    ///
    /// Prove-the-test: a source that zero-fills an unplaced range returns
    /// `Ok([0; 8])` for the gap read and the `expect_err` fails.
    #[test]
    fn slice_refuses_any_range_no_fetch_placed() {
        let mut asm = ObjectAssembler::new(&gauge(), 64);
        asm.place("k", 8, Bytes::from_static(&[0xAB; 8]))
            .expect("place");
        assert_eq!(&*asm.slice("k", 8, 8).expect("placed range"), &[0xAB; 8]);
        let gap = asm.slice("k", 0, 8).expect_err("gap read must fail");
        assert!(
            matches!(
                gap,
                LogFetchError::Corrupt {
                    source: LogSegError::Unplaced { start: 0, end: 8 },
                    ..
                }
            ),
            "got {gap:?}"
        );
        let straddle = asm.slice("k", 12, 8).expect_err("half-placed read");
        assert!(matches!(
            straddle,
            LogFetchError::Corrupt {
                source: LogSegError::Unplaced { start: 12, end: 20 },
                ..
            }
        ));
        assert!(
            asm.place("k", 60, Bytes::from_static(&[1; 8])).is_err(),
            "a region past the object end is refused"
        );
    }

    /// The gauge carries the placed bytes, not the object size: 24 of a 4096
    /// byte object here. It stays charged while any clone of the returned
    /// bytes lives and returns to zero with the last one; the high-water mark
    /// keeps the peak.
    ///
    /// Prove-the-test: charging `total_size` at construction (the object-sized
    /// buffer this replaced) reads `live_bytes: 4096` at the first assertion.
    #[test]
    fn the_gauge_counts_placed_bytes_until_the_reader_drops_them() {
        let g = gauge();
        let mut asm = ObjectAssembler::new(&g, 4096);
        assert_eq!(g.stats(), AssemblyBufferStats::default());
        asm.place("k", 0, Bytes::from_static(&[1; 16]))
            .expect("place");
        asm.place("k", 4000, Bytes::from_static(&[2; 8]))
            .expect("place");
        assert_eq!(
            g.stats(),
            AssemblyBufferStats {
                live_bytes: 24,
                peak_live_bytes: 24,
            }
        );
        let bytes = asm.into_bytes();
        assert_eq!(bytes.held_len(), 24);
        assert_eq!(bytes.len(), 4096, "the object's length, not the held bytes");
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(g.stats().live_bytes, 24, "a clone keeps the regions held");
        drop(clone);
        assert_eq!(
            g.stats(),
            AssemblyBufferStats {
                live_bytes: 0,
                peak_live_bytes: 24,
            }
        );
    }

    /// An assembler dropped without handing its bytes on (an error or a
    /// coverage crossover) releases its gauge charge and its reservations.
    #[test]
    fn a_dropped_assembler_releases_what_it_held() {
        let g = gauge();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1024));
        let mut asm = ObjectAssembler::new(&g, 512);
        asm.hold(budget.reserve(32).expect("reserve"));
        asm.place("k", 100, Bytes::from_static(&[3; 32]))
            .expect("place");
        assert_eq!(asm.reserved(), 32);
        assert_eq!(budget.fetch_reserved(), 32);
        drop(asm);
        assert_eq!(budget.fetch_reserved(), 0);
        assert_eq!(g.stats().live_bytes, 0);
    }

    /// The reservations travel with the returned bytes: released only when
    /// the last clone drops, not when the assembler is consumed.
    #[test]
    fn reservations_release_with_the_last_clone_of_the_bytes() {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1024));
        let mut asm = ObjectAssembler::new(&gauge(), 512);
        asm.hold(budget.reserve(40).expect("reserve"));
        asm.place("k", 0, Bytes::from_static(&[4; 40]))
            .expect("place");
        let bytes = asm.into_bytes();
        let clone = bytes.clone();
        drop(bytes);
        assert_eq!(budget.fetch_reserved(), 40);
        drop(clone);
        assert_eq!(budget.fetch_reserved(), 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod ranged_projection_cost_tests {
    //! The arithmetic behind [`LogSegmentFetcher::ranged_projection_pays`]
    //! (issue #862), pinned at its break-even rather than at a convenient
    //! distance from it.
    //!
    //! The break-even is the fetch layer's existing whole-object crossover,
    //! `WHOLE_OBJECT_REQUEST_MULTIPLE * request_cost_bytes`, applied to the
    //! bytes the projection SKIPS rather than to the object's whole size. Every
    //! case below states the saving it produces and which side of that figure it
    //! falls on.
    //!
    //! Note which crossover is in effect. `with_block_range_threshold` sets the
    //! funnel's routing threshold AND the block-range fetcher's size crossover
    //! to the same value, so a caller that sets it (every production caller does,
    //! at `EngineConfig::logs_block_range_threshold`, 512 KiB by default) pins
    //! the break-even to that figure rather than to the request-cost-derived
    //! one. The tests below cover both configurations, because both ship.

    use super::*;
    use ravel_object_store::memory::MemoryStore;

    /// A fetcher at the DERIVED break-even: request cost 200,000 bytes, so the
    /// crossover is `5 * 200,000 = 1,000,000` saved bytes (above the 512 KiB
    /// floor, which would otherwise mask the multiple). The routing threshold is
    /// left at its 512 KiB default.
    fn derived() -> LogSegmentFetcher {
        let store = Arc::new(MemoryStore::new());
        let block_range = BlockRangeFetcher::new(store.clone()).with_request_cost_bytes(200_000);
        LogSegmentFetcher::new(store).with_block_range(block_range)
    }

    /// The routing threshold takes precedence: at or below it every entry point
    /// reads the whole object anyway, so a probe would buy nothing.
    #[test]
    fn at_or_below_the_routing_threshold_never_ranges() {
        let f = derived();
        assert_eq!(
            f.block_range_threshold(),
            DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert!(
            !f.ranged_projection_pays(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, 0.0),
            "at the threshold"
        );
        assert!(!f.ranged_projection_pays(0, 0.0), "a sizeless object");
    }

    /// A projection reading everything saves nothing, at any object size.
    #[test]
    fn a_full_projection_never_ranges() {
        let f = derived();
        assert!(!f.ranged_projection_pays(1_000_000_000, 1.0));
        // Out-of-range and non-finite fractions fail closed the same way.
        assert!(!f.ranged_projection_pays(1_000_000_000, 1.5));
        assert!(!f.ranged_projection_pays(1_000_000_000, f64::NAN));
    }

    /// The derived break-even itself, from both sides. A 2,000,000-byte object
    /// at a fraction of 0.5 saves exactly 1,000,000 bytes, which IS the
    /// break-even and therefore not a win (the comparison is strict); one more
    /// byte of saving is.
    #[test]
    fn the_break_even_is_five_request_costs_of_saving() {
        let f = derived();
        assert!(
            !f.ranged_projection_pays(2_000_000, 0.5),
            "saving exactly 1,000,000 bytes does not beat a 1,000,000-byte break-even"
        );
        assert!(
            f.ranged_projection_pays(2_000_002, 0.5),
            "saving 1,000,001 bytes does"
        );
    }

    /// Raising the request cost raises the break-even proportionally: the same
    /// object and projection that paid at a cost of 200,000 does not at
    /// 2,000,000. This is what makes recalibrating the store recalibrate the
    /// routing rather than needing a second knob.
    #[test]
    fn the_request_cost_moves_the_break_even() {
        let store = Arc::new(MemoryStore::new());
        let dear = LogSegmentFetcher::new(store.clone())
            .with_block_range(BlockRangeFetcher::new(store).with_request_cost_bytes(2_000_000));
        assert!(derived().ranged_projection_pays(2_000_002, 0.5));
        assert!(!dear.ranged_projection_pays(2_000_002, 0.5));
    }

    /// The shipped production configuration: `with_block_range_threshold` pins
    /// both crossovers, so the break-even is that figure and NOT the derived
    /// one. At the 512 KiB default a 3.5 MB object -- the ClickBench reference
    /// tenant's average after L1 compaction -- ranges for a narrow projection
    /// and does not for a projection reading nine tenths of its columns.
    #[test]
    fn an_explicit_block_range_threshold_is_the_break_even() {
        let store = Arc::new(MemoryStore::new());
        let f = LogSegmentFetcher::new(store)
            .with_block_range_threshold(DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD);
        let object = 3_500_000u64;
        assert!(
            f.ranged_projection_pays(object, 3.0 / 115.0),
            "a three-of-115-column projection saves ~3.4 MB, past a 512 KiB break-even"
        );
        assert!(
            !f.ranged_projection_pays(object, 0.9),
            "a nine-tenths projection saves 350 KB, short of it"
        );
    }

    /// An explicit whole-object threshold overrides the derived break-even, so a
    /// test fixture pinning one gets exactly the break-even it pinned. `0` makes
    /// any nonzero saving a win.
    #[test]
    fn an_explicit_threshold_is_the_break_even() {
        let store = Arc::new(MemoryStore::new());
        let f = LogSegmentFetcher::new(store).with_block_range_threshold(0);
        assert!(
            f.ranged_projection_pays(1, 0.0),
            "at a break-even of zero, any saving wins"
        );
        assert!(
            !f.ranged_projection_pays(1, 1.0),
            "a full projection still saves nothing"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod read_gate_tests {
    //! ADR-1702 follow-up task 7, RLOG site: the LogQL fetch path's block
    //! decodes run on the read gate, one job per surviving block.

    use super::*;
    use crate::read_gate_test_support::{floor_zero_gate, site_counts, total_inline};
    use ravel_catalog::SegmentLevel;
    use ravel_cpu_gate::{CpuGateConfig, InstantClock};
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{FieldSel, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const KEY: &str = "t/gated.rlog";
    const BLOCKS: usize = 4;

    fn record(ts: i64) -> LogRecord {
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("hello world {ts}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![("request.id".to_string(), AttrValue::Str(format!("r{ts}")))],
        }
    }

    /// One block per record, so the object carries exactly [`BLOCKS`] blocks.
    async fn fixture() -> (Arc<MemoryStore>, SegmentRef) {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        // POSTINGS indexes `request.id`, so an equality on it is probed.
        let mut writer =
            RlogWriter::new(cfg, identity).with_indexed_fields(vec!["request.id".to_string()]);
        for ts in 0..BLOCKS as i64 {
            writer.push(record(ts)).expect("push");
        }
        let bytes = writer.finish().expect("finish");
        let seg_ref = SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: bytes.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [9u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        };
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        (store, seg_ref)
    }

    /// Opens the scan the LogQL series path opens and drains it through the
    /// exit that path drains, returning every record in scan order.
    async fn drain(fetcher: &LogSegmentFetcher, seg_ref: &SegmentRef) -> Vec<LogRecord> {
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let mut scan = fetcher
            .scan_accounted_with_tenant(
                seg_ref,
                TENANT,
                &query,
                &ColumnSelection::all(),
                &QueryAccounting::new(),
            )
            .await
            .expect("scan")
            .expect("the segment is relevant");
        let mut records = Vec::new();
        while let Some(block) = scan.next_block_on_gate().await.expect("block") {
            records.extend(block);
        }
        assert_eq!(scan.stats().blocks_scanned, BLOCKS as u32);
        records
    }

    /// With the byte floor at 0, every surviving block is exactly one
    /// `LogBlock` job, reaching exhaustion submits none, nothing runs inline,
    /// and the records equal the ungated scan's.
    ///
    /// FLIP: in `next_block_on_gate`, read the gate as `None` so every block
    /// takes the inline `next_block`; the `LogBlock` assertion then reads
    /// `left: (0, 0), right: (4, 0)`.
    #[tokio::test]
    async fn log_block_decodes_run_through_the_read_gate() {
        let (store, seg_ref) = fixture().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let inline = drain(&LogSegmentFetcher::new(backend.clone()), &seg_ref).await;
        assert_eq!(inline.len(), BLOCKS);

        let gate = floor_zero_gate();
        let fetcher = LogSegmentFetcher::new(backend.clone()).with_read_gate(gate.clone());
        let gated = drain(&fetcher, &seg_ref).await;
        assert_eq!(format!("{gated:?}"), format!("{inline:?}"));
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (BLOCKS as u64, 0));
        assert_eq!(total_inline(&gate), 0);

        // A floor above every block's size runs each block inline and counts
        // it there, which also shows the job size was read from the object's
        // PAGE_DIR rather than taken from the unreadable-directory fallback.
        let high_floor = Arc::new(ReadGate::new(
            CpuGateConfig {
                permits: 1,
                inline_floor_bytes: u64::MAX,
                eval_floor_samples: 0,
            },
            Arc::new(InstantClock::new()),
        ));
        let fetcher = LogSegmentFetcher::new(backend).with_read_gate(high_floor.clone());
        let small = drain(&fetcher, &seg_ref).await;
        assert_eq!(format!("{small:?}"), format!("{inline:?}"));
        assert_eq!(
            site_counts(&high_floor, ReadSite::LogBlock),
            (0, BLOCKS as u64)
        );
    }

    /// The job size is the largest block's summed uncompressed page length,
    /// and the sizing read charges its own PAGE_DIR decode to the accounting
    /// handle the caller passed in.
    ///
    /// FLIP: reading the sizing PAGE_DIR through `ravel_logseg::read_section`
    /// instead of `read_section_accounted` reads `left: 0, right: 326` on the
    /// charge assertion.
    #[test]
    fn block_job_size_is_the_largest_uncompressed_block() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut writer = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: TENANT.0,
                shard: 0,
                writer_id: [2u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        let mut big = record(1);
        big.body = "x".repeat(10_000);
        writer.push(record(0)).expect("push");
        writer.push(big).expect("push");
        for ts in 2..4 {
            writer.push(record(ts)).expect("push");
        }
        let bytes = writer.finish().expect("finish");
        let page_dir = {
            let footer = footer::open(&bytes).expect("footer");
            let desc = *footer.section(kind::PAGE_DIR).expect("PAGE_DIR");
            PageDir::decode(&ravel_logseg::read_section(&bytes, &desc, &cfg).expect("read"))
                .expect("decode")
        };
        // All four records carry the same severity text, so its chunk stores
        // one row-group dictionary page that each block decodes through. Two
        // one-record blocks would not: the dictionary page's own PAGE_DIR
        // entry outweighs what two id pages save.
        let dict_pages = |block: u32| page_dir.block_dict_pages(block).expect("block");
        assert!(
            !dict_pages(1).is_empty(),
            "the fixture has a dictionary page"
        );
        let per_block = |block: u32| -> u64 {
            page_dir
                .block_pages(block)
                .expect("block")
                .iter()
                .chain(&dict_pages(block))
                .map(|page| page.desc.uncomp_len)
                .sum()
        };
        assert!(per_block(1) > 10_000);
        assert!(per_block(0) < per_block(1));
        let charged = QueryAccounting::new();
        assert_eq!(
            max_block_uncompressed_len(&bytes, &cfg, &charged),
            per_block(1)
        );
        let sizing_probe = QueryAccounting::new();
        let page_dir_decompressed = {
            let footer = footer::open(&bytes).expect("footer");
            let desc = *footer.section(kind::PAGE_DIR).expect("PAGE_DIR");
            ravel_logseg::read_section_accounted(&bytes, &desc, &cfg, &sizing_probe).expect("read");
            sizing_probe.snapshot().decompressed_bytes
        };
        assert!(page_dir_decompressed > 0, "the fixture compresses PAGE_DIR");
        assert_eq!(
            charged.snapshot().decompressed_bytes,
            page_dir_decompressed,
            "the sizing read charges exactly its own PAGE_DIR decode"
        );
        let unreadable = QueryAccounting::new();
        assert_eq!(
            max_block_uncompressed_len(&b"not an object"[..], &cfg, &unreadable),
            u64::MAX
        );
        assert_eq!(unreadable.snapshot().decompressed_bytes, 0);
    }

    async fn fetch_with_tenant(
        fetcher: &LogSegmentFetcher,
        seg_ref: &SegmentRef,
        query: &LogQuery,
    ) -> LogFetchOutput {
        fetcher
            .fetch_accounted_with_tenant(seg_ref, TENANT, query, &QueryAccounting::new())
            .await
            .expect("fetch")
            .expect("the segment is relevant")
    }

    /// The row fetch the SQL alerts and audit scans, distributed fragments and
    /// cache warming reach: with the byte floor at 0, the scan open is exactly
    /// one `LogPostings` job, each block exactly one `LogBlock` job, nothing
    /// runs inline, and the records and counters equal the ungated fetch's.
    /// The gated fetch's decompressed-byte charge is the ungated one plus
    /// exactly one PAGE_DIR: sizing the block jobs reads that directory a
    /// second time, and that read is accounted like every other. The object is
    /// below the block-range threshold, so no section is decoded on its own.
    /// The untenanted `fetch_accounted` drains the same way.
    ///
    /// FLIP: in `decode_spanned`, take the `None` arm whatever the gate; the
    /// `LogBlock` assertion then reads `left: (0, 0), right: (4, 0)`. Reading
    /// the gate as `None` in `open_scan_on_gate` instead reads
    /// `left: (0, 0), right: (1, 0)` on the `LogPostings` one. Reading the
    /// sizing PAGE_DIR in `max_block_uncompressed_len` through
    /// `ravel_logseg::read_section` instead of `read_section_accounted` leaves
    /// that decode unaccounted and the charge assertion reads
    /// `left: 450, right: 766`.
    #[tokio::test]
    async fn fetch_accounted_with_tenant_decodes_on_the_read_gate() {
        let (store, seg_ref) = fixture().await;
        // The sizing read the gated path adds, measured through the same
        // accounted reader it uses, so the expected charge is a figure rather
        // than a direction.
        let page_dir_decompressed = {
            let object = store.get(KEY, GetRange::Full).await.expect("get").data;
            let cfg = RlogConfig::default();
            let footer = footer::open(&object).expect("footer");
            let desc = *footer.section(kind::PAGE_DIR).expect("PAGE_DIR");
            let probe = QueryAccounting::new();
            ravel_logseg::read_section_accounted(&object, &desc, &cfg, &probe).expect("read");
            probe.snapshot().decompressed_bytes
        };
        assert!(page_dir_decompressed > 0, "the fixture compresses PAGE_DIR");
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let ungated_accounting = QueryAccounting::new();
        let inline = LogSegmentFetcher::new(backend.clone())
            .fetch_accounted_with_tenant(&seg_ref, TENANT, &query, &ungated_accounting)
            .await
            .expect("ungated fetch")
            .expect("relevant");
        assert_eq!(inline.records.len(), BLOCKS);

        let gate = floor_zero_gate();
        let fetcher = LogSegmentFetcher::new(backend).with_read_gate(gate.clone());
        let gated_accounting = QueryAccounting::new();
        let gated = fetcher
            .fetch_accounted_with_tenant(&seg_ref, TENANT, &query, &gated_accounting)
            .await
            .expect("gated fetch")
            .expect("relevant");
        assert_eq!(format!("{gated:?}"), format!("{inline:?}"));
        assert_eq!(
            gated_accounting.snapshot().decompressed_bytes,
            ungated_accounting.snapshot().decompressed_bytes + page_dir_decompressed
        );
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (BLOCKS as u64, 0));
        assert_eq!(site_counts(&gate, ReadSite::LogPostings), (1, 0));
        assert_eq!(site_counts(&gate, ReadSite::LogSection), (0, 0));
        assert_eq!(total_inline(&gate), 0);

        let untenanted = fetcher
            .fetch_accounted(&seg_ref, &query, &QueryAccounting::new())
            .await
            .expect("untenanted fetch")
            .expect("relevant");
        assert_eq!(format!("{untenanted:?}"), format!("{inline:?}"));
        assert_eq!(site_counts(&gate, ReadSite::LogPostings), (2, 0));
        assert_eq!(
            site_counts(&gate, ReadSite::LogBlock),
            (2 * BLOCKS as u64, 0)
        );
        assert_eq!(total_inline(&gate), 0);
    }

    /// The POSTINGS probe runs inside the scan open's `LogPostings` job: an
    /// equality on the indexed `request.id` prunes four blocks to the one
    /// holding it, and the fetch is exactly one `LogPostings` job and one
    /// `LogBlock` job.
    ///
    /// FLIP: reading the gate as `None` in `open_scan_on_gate` leaves the
    /// probe inline and the `LogPostings` assertion reads
    /// `left: (0, 0), right: (1, 0)`.
    #[tokio::test]
    async fn the_postings_probe_runs_in_the_scan_open_job() {
        let (store, seg_ref) = fixture().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let query = LogQuery::new(i64::MIN, i64::MAX).with_content(Predicate::Equals {
            field: FieldSel::Attr("request.id".to_string()),
            value: AttrValue::Str("r2".to_string()),
        });
        let inline =
            fetch_with_tenant(&LogSegmentFetcher::new(backend.clone()), &seg_ref, &query).await;
        let gate = floor_zero_gate();
        let gated = fetch_with_tenant(
            &LogSegmentFetcher::new(backend).with_read_gate(gate.clone()),
            &seg_ref,
            &query,
        )
        .await;
        assert_eq!(format!("{gated:?}"), format!("{inline:?}"));
        assert_eq!(gated.records.len(), 1);
        assert_eq!(gated.stats.blocks_after_skip, BLOCKS as u32);
        assert_eq!(gated.stats.blocks_after_postings, 1, "{:?}", gated.stats);
        assert!(!gated.stats.postings_degraded);
        assert_eq!(site_counts(&gate, ReadSite::LogPostings), (1, 0));
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (1, 0));
        assert_eq!(total_inline(&gate), 0);
    }

    /// Above the block-range threshold the fetch decodes the SKIP_IDX and
    /// PAGE_DIR it placed from ranged reads before opening the scan: each is
    /// exactly one `LogSection` job, beside the open's `LogPostings` job and
    /// the blocks' `LogBlock` jobs.
    ///
    /// FLIP: reading the gate as `None` in
    /// `BlockRangeFetcher::decode_section_on_gate` reads
    /// `left: (0, 0), right: (2, 0)` on the `LogSection` assertion.
    #[tokio::test]
    async fn block_range_sections_decode_on_the_read_gate() {
        let (store, seg_ref) = fixture().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let inline = fetch_with_tenant(
            &LogSegmentFetcher::new(backend.clone()).with_block_range_threshold(0),
            &seg_ref,
            &query,
        )
        .await;
        let gate = floor_zero_gate();
        let gated = fetch_with_tenant(
            &LogSegmentFetcher::new(backend)
                .with_block_range_threshold(0)
                .with_read_gate(gate.clone()),
            &seg_ref,
            &query,
        )
        .await;
        assert_eq!(format!("{gated:?}"), format!("{inline:?}"));
        assert_eq!(site_counts(&gate, ReadSite::LogSection), (2, 0));
        assert_eq!(site_counts(&gate, ReadSite::LogPostings), (1, 0));
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (BLOCKS as u64, 0));
        assert_eq!(total_inline(&gate), 0);
    }

    /// A gated RLOG decode that panicked is the log fetcher's decode error, and
    /// one that never ran is transient.
    ///
    /// FLIP: move `CpuGateError::Panicked` into the other arm of
    /// `log_gate_failed` and the first match panics with a `Store` error.
    #[tokio::test]
    async fn a_panicked_log_job_is_the_decode_error() {
        let gate = floor_zero_gate();
        let err = gate
            .run(ReadSite::LogBlock, JobSize::Bytes(1), || -> u8 {
                panic!("block decode panicked")
            })
            .await
            .expect_err("a panicking job fails");
        match log_gate_failed(KEY, err) {
            LogFetchError::Corrupt {
                source: LogSegError::Corrupted(message),
                ..
            } => assert!(message.contains("panicked"), "{message}"),
            other => panic!("expected the decode error, got {other:?}"),
        }
        assert!(matches!(
            log_gate_failed(KEY, CpuGateError::Cancelled),
            LogFetchError::Store {
                source: StoreError::Transient(_),
                ..
            }
        ));
    }

    /// The error class a fetch error carries, which is what the HTTP layer
    /// maps to a status: a permanent store error is redacted to a 503 and a
    /// corrupt segment is not.
    fn class(err: &LogFetchError) -> &'static str {
        match err {
            LogFetchError::Corrupt { .. } => "corrupt",
            LogFetchError::Store {
                source: StoreError::Transient(_),
                ..
            } => "transient",
            LogFetchError::Store { .. } => "permanent",
            other => panic!("unexpected error {other:?}"),
        }
    }

    /// A gated block decode that panicked takes the cursor with it. The call
    /// that lost it reports the decode's own error, and so does every call
    /// after it, on both exits: the scan keeps the class rather than degrading
    /// to a permanent store error the HTTP layer would redact to a 503.
    ///
    /// FLIP: return `scan_lost(&self.key)` unconditionally from
    /// `LogSegmentScan::lost_error` and the second-call assertion reads
    /// `left: "permanent", right: "corrupt"`.
    #[tokio::test]
    async fn a_lost_log_scan_repeats_the_failure_class() {
        let (store, seg_ref) = fixture().await;
        let backend: Arc<dyn ObjectStoreBackend> = store;
        let fetcher = LogSegmentFetcher::new(backend).with_read_gate(floor_zero_gate());
        let query = LogQuery::new(i64::MIN, i64::MAX);
        let mut scan = fetcher
            .scan_accounted_with_tenant(
                &seg_ref,
                TENANT,
                &query,
                &ColumnSelection::all(),
                &QueryAccounting::new(),
            )
            .await
            .expect("scan")
            .expect("the segment is relevant");

        scan.panic_next_gate_job_for_test();
        let first = scan
            .next_block_on_gate()
            .await
            .expect_err("the panicking job fails the call that submitted it");
        assert_eq!(class(&first), "corrupt", "{first:?}");

        let second = scan
            .next_block_on_gate()
            .await
            .expect_err("the cursor went with the panicking job");
        assert_eq!(class(&second), class(&first), "{second:?}");
        let inline = scan
            .next_block()
            .expect_err("the inline exit has no cursor either");
        assert_eq!(class(&inline), class(&first), "{inline:?}");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod fetch_run_corruption_gate_tests {
    //! ADR-0046 decision 4's corruption gate on the blocks `fetch_run` did not
    //! fetch itself: the lead block, and a non-lead block resolved through
    //! `fetch_peeked` whose CRC is checked after it returns. A caller whose peek
    //! missed while a flight was running and that reached the single flight
    //! only after it finished is served the RAM tier's bytes by the leader's
    //! RAM recheck; in `with_corruption` mode those bytes arrive corrupted and
    //! must be refused exactly as a corrupted peek hit is.
    //!
    //! The same late RAM serve is also pinned here for its query accounting:
    //! a cache miss with zero GETs and zero fetched bytes, per
    //! docs/guides/caching.md.

    use super::*;
    use ravel_cache::{Cache, CacheLimits, DiskCache, TieredCache};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const CONTENT_HASH: [u8; 32] = [9u8; 32];
    const KEY: &str = "t/corrupt-gate.rlog";

    fn seg_ref(size: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 0,
            sample_count: 1,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// FLIP: removing the follower branch's `verify_block_crc` on the lead
    /// block returns the late caller's corrupted bytes as `Ok`.
    #[tokio::test]
    async fn a_late_ram_served_lead_block_is_crc_verified_under_corruption() {
        let object = Bytes::from_static(b"block-range bytes the corruption gate covers");
        let store = MemoryStore::new();
        store
            .put(KEY, object.clone(), PutOptions::default())
            .await
            .expect("put");
        let block = object.slice(0..16);
        let ext = BlockExtent {
            abs_start: 0,
            len: block.len() as u64,
            crc32c: crc32c::crc32c(&block),
        };

        let limits = CacheLimits::new(1024 * 1024, 100, 1024 * 1024);
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let tiered = Arc::new(TieredCache::new(
            Cache::with_corruption(limits),
            DiskCache::new(tmp.path().to_path_buf(), limits),
        ));
        let fetcher = BlockRangeFetcher::new(Arc::new(store)).with_cache(tiered.clone());
        let seg = seg_ref(object.len() as u64);

        // The leader: runs the fetch, verifies the fresh bytes, admits them.
        let leader_acc = QueryAccounting::new();
        let led = fetcher
            .fetch_run(
                &seg,
                TENANT,
                &EtagPin::default(),
                ext,
                vec![ext],
                QueryPhase::Scan,
                &leader_acc,
            )
            .await
            .expect("the leader's fresh fetch verifies");
        assert_eq!(led.gets, 1);
        assert_eq!(led.blocks, vec![(0, block.clone())]);
        assert_eq!(leader_acc.snapshot().total_s3_requests(), 1);

        // The late caller: its peek missed during the flight, and it reaches
        // the single flight after the flight left the map, so the RAM tier
        // serves it, corrupted.
        let late_acc = QueryAccounting::new();
        let Err(err) = fetcher
            .fetch_run(
                &seg,
                TENANT,
                &EtagPin::default(),
                ext,
                vec![ext],
                QueryPhase::Scan,
                &late_acc,
            )
            .await
        else {
            panic!("a corrupted late RAM serve must be refused");
        };
        assert!(
            matches!(
                &err,
                LogFetchError::Corrupt { key, source }
                    if key == KEY && source.to_string().contains("block crc mismatch")
            ),
            "{err:?}"
        );
        assert_eq!(
            late_acc.snapshot().total_s3_requests(),
            0,
            "the late caller was served from RAM, not refetched"
        );
    }

    /// A non-lead block whose peek missed, and which the tiered leader's RAM
    /// recheck then serves because the block was admitted in between, is
    /// verified after `fetch_peeked` returns, not only inside this call's own
    /// fetch closure.
    ///
    /// The run is polled once, which stops it in the tail block's
    /// `spawn_blocking` disk peek after its RAM peek missed; the tail block is
    /// then admitted to RAM only (the disk tier declines every entry), so the
    /// resumed peek misses and the RAM recheck serves the corrupted bytes. An
    /// attempt whose disk peek finished inside that one poll went on to lead
    /// the tail block's flight with an empty RAM tier and fetch the block from
    /// the store; it is still pending then, parked in the disk-tier admission,
    /// so the guard detects it by the tail key's flight still being in flight
    /// or a store request already made, and rebuilds it.
    ///
    /// FLIP: removing the `verify_block_crc` after the non-lead block's
    /// `fetch_peeked` returns the corrupted tail block as `Ok`, and the test
    /// panics with "a corrupted late RAM serve of a non-lead block must be
    /// refused".
    #[tokio::test]
    async fn a_late_ram_served_non_lead_block_is_crc_verified_under_corruption() {
        let object = Bytes::from_static(b"block-range bytes the corruption gate covers");
        let lead_block = object.slice(0..16);
        let tail_block = object.slice(16..32);
        let lead_key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, 16);
        let tail_key = CacheKey::new(TENANT.0, CONTENT_HASH, 16, 16);
        let limits = CacheLimits::new(1024 * 1024, 100, 1024 * 1024);
        let disk_declines_all = CacheLimits::new(1024 * 1024, 100, 1);

        // Corruption mode serves every RAM hit corrupted, the lead's included.
        // The lead extent carries the crc of those served bytes so the lead
        // passes its gate and the run reaches the tail block, whose extent
        // carries the true crc.
        let probe: Cache<&'static str> = Cache::with_corruption(limits);
        probe.insert(lead_key, lead_block.clone());
        let served_lead = probe.get(&lead_key).expect("probe RAM hit");
        assert_ne!(served_lead, lead_block, "corruption mode changes the bytes");
        let lead = BlockExtent {
            abs_start: 0,
            len: 16,
            crc32c: crc32c::crc32c(&served_lead),
        };
        let tail = BlockExtent {
            abs_start: 16,
            len: 16,
            crc32c: crc32c::crc32c(&tail_block),
        };
        let run = BlockExtent {
            abs_start: 0,
            len: 32,
            crc32c: 0,
        };
        let seg = seg_ref(object.len() as u64);

        let mut attempts = 0;
        let (result, requests) = loop {
            attempts += 1;
            assert!(
                attempts <= 20,
                "the tail block's disk peek finished inside the first poll every time"
            );
            let store = MemoryStore::new();
            store
                .put(KEY, object.clone(), PutOptions::default())
                .await
                .expect("put");
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let tiered = Arc::new(TieredCache::new(
                Cache::with_corruption(limits),
                DiskCache::new(tmp.path().to_path_buf(), disk_declines_all),
            ));
            let ram_metrics = tiered.ram_metrics();
            tiered.insert(lead_key, lead_block.clone());
            let fetcher = BlockRangeFetcher::new(Arc::new(store)).with_cache(tiered.clone());
            let acc = QueryAccounting::new();
            let misses_before = ram_metrics.snapshot().misses;
            let pin = EtagPin::default();

            let mut fetch = Box::pin(fetcher.fetch_run(
                &seg,
                TENANT,
                &pin,
                run,
                vec![lead, tail],
                QueryPhase::Scan,
                &acc,
            ));
            let first = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(fetch.as_mut(), cx))
            })
            .await;
            if first.is_ready()
                || tiered.is_in_flight(&tail_key)
                || acc.snapshot().total_s3_requests() > 0
            {
                continue;
            }
            assert_eq!(
                ram_metrics.snapshot().misses,
                misses_before,
                "the poll stopped in the tail block's uncounted disk re-peek"
            );
            tiered.insert(tail_key, tail_block.clone());
            assert_eq!(tiered.disk_len(), 0, "the disk tier declined both blocks");
            let result = fetch.await;
            break (result, acc.snapshot().total_s3_requests());
        };

        let Err(err) = result else {
            panic!("a corrupted late RAM serve of a non-lead block must be refused");
        };
        assert!(
            matches!(
                &err,
                LogFetchError::Corrupt { key, source }
                    if key == KEY && source.to_string().contains("block crc mismatch")
            ),
            "{err:?}"
        );
        assert_eq!(
            requests, 0,
            "both blocks were served from RAM, none refetched"
        );
    }

    /// How the tail block of [`late_ram_served_run`]'s run reaches `fetch_run`'s
    /// non-lead branch.
    #[derive(Clone, Copy)]
    enum TailServe {
        /// Admitted before `fetch_run` re-peeks it, so the re-peek hits.
        RePeekHit,
        /// Admitted after the re-peek missed, so `fetch_peeked`'s RAM recheck
        /// serves it.
        RamRecheck,
    }

    /// Occupies the blocking pool's only thread until the returned sender is
    /// dropped. The pool queues blocking tasks first in, first out, so a disk
    /// peek spawned after this call cannot start before the drop.
    fn hold_blocking_pool() -> std::sync::mpsc::Sender<()> {
        let (release, held) = std::sync::mpsc::channel::<()>();
        drop(tokio::task::spawn_blocking(move || {
            let _ = held.recv();
        }));
        release
    }

    /// Polls `fut` until `reached` holds after a poll, waiting between polls on
    /// `fut`'s own wakeups. `fut` finishing first fails the test with
    /// `checkpoint`, the point it should have stopped at.
    async fn poll_until<F: std::future::Future + ?Sized>(
        mut fut: std::pin::Pin<&mut F>,
        reached: impl Fn() -> bool,
        checkpoint: &str,
    ) {
        std::future::poll_fn(move |cx| match fut.as_mut().poll(cx) {
            std::task::Poll::Ready(_) => panic!("the run finished before {checkpoint}"),
            std::task::Poll::Pending if reached() => std::task::Poll::Ready(()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        })
        .await;
    }

    /// Runs `fetch_blocks` over a two-block run whose blocks both peek a miss
    /// and are then served from RAM without a GET: the lead block by the RAM
    /// recheck of `fetch_peeked`, the tail block as `tail` says. The run gets a
    /// runtime with one blocking thread, which [`hold_blocking_pool`] keeps
    /// busy, so each `spawn_blocking` disk peek is held behind a gate the test
    /// releases: the run stops at a peek once its RAM tier miss is counted. The
    /// disk tier declines every entry, so each admission lands in RAM only.
    fn late_ram_served_run(
        tail: TailServe,
    ) -> (
        ravel_types::accounting::QueryAccountingSnapshot,
        BlockRangeStats,
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("runtime")
            .block_on(late_ram_served_run_on_one_blocking_thread(tail))
    }

    async fn late_ram_served_run_on_one_blocking_thread(
        tail: TailServe,
    ) -> (
        ravel_types::accounting::QueryAccountingSnapshot,
        BlockRangeStats,
    ) {
        let object = Bytes::from_static(b"block-range bytes the corruption gate covers");
        let lead_block = object.slice(0..16);
        let tail_block = object.slice(16..32);
        let lead_key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, 16);
        let tail_key = CacheKey::new(TENANT.0, CONTENT_HASH, 16, 16);
        let limits = CacheLimits::new(1024 * 1024, 100, 1024 * 1024);
        let disk_declines_all = CacheLimits::new(1024 * 1024, 100, 1);
        let extents = [
            BlockExtent {
                abs_start: 0,
                len: 16,
                crc32c: crc32c::crc32c(&lead_block),
            },
            BlockExtent {
                abs_start: 16,
                len: 16,
                crc32c: crc32c::crc32c(&tail_block),
            },
        ];
        let seg = seg_ref(object.len() as u64);
        let pin = EtagPin::default();

        let store = MemoryStore::new();
        store
            .put(KEY, object.clone(), PutOptions::default())
            .await
            .expect("put");
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let tiered = Arc::new(TieredCache::new(
            Cache::new(limits),
            DiskCache::new(tmp.path().to_path_buf(), disk_declines_all),
        ));
        let ram_metrics = tiered.ram_metrics();
        let ram_misses = || ram_metrics.snapshot().misses;
        let disk_metrics = tiered.disk_metrics();
        let disk_misses = || disk_metrics.snapshot().misses;
        let fetcher = BlockRangeFetcher::new(Arc::new(store)).with_cache(tiered.clone());
        let acc = QueryAccounting::new();
        let mut stats = BlockRangeStats::default();
        let mut asm = ObjectAssembler::new(&fetcher.assembly_gauge, object.len() as u64);
        let mut fetch = Box::pin(fetcher.fetch_blocks(
            &seg,
            TENANT,
            &pin,
            &extents,
            QueryPhase::Scan,
            &mut asm,
            &acc,
            &mut stats,
        ));

        // Each gate is queued before the one ahead of it is released, so it
        // takes the blocking thread before the next disk peek can.
        let lead_peek_gate = hold_blocking_pool();
        poll_until(
            fetch.as_mut(),
            || ram_misses() >= 1,
            "the lead block's disk peek",
        )
        .await;
        assert_eq!(ram_misses(), 1, "held in the lead block's disk peek");
        tiered.insert(lead_key, lead_block.clone());

        // Then in the tail block's disk peek: the lead's peek missed, and the
        // run has not reached `fetch_run` yet.
        let tail_peek_gate = hold_blocking_pool();
        drop(lead_peek_gate);
        poll_until(
            fetch.as_mut(),
            || ram_misses() >= 2,
            "the tail block's disk peek",
        )
        .await;
        assert_eq!(ram_misses(), 2, "held in the tail block's disk peek");

        let re_peek_gate = if let TailServe::RamRecheck = tail {
            // Then in `fetch_run`'s re-peek of the tail block, after the lead
            // was served by the RAM recheck and before the tail's own
            // `fetch_peeked`. The re-peek is uncounted, so the checkpoint is
            // the tail block's disk peek having recorded its miss: the next
            // pending poll is the re-peek's own disk lookup, held by the gate.
            let re_peek_gate = hold_blocking_pool();
            drop(tail_peek_gate);
            poll_until(
                fetch.as_mut(),
                || disk_misses() >= 2,
                "fetch_run's re-peek of the tail block",
            )
            .await;
            assert_eq!(disk_misses(), 2, "the tail block's disk peek finished");
            assert_eq!(ram_misses(), 2, "the re-peek recorded no RAM miss");
            assert!(!tiered.is_in_flight(&tail_key));
            re_peek_gate
        } else {
            tail_peek_gate
        };
        assert_eq!(acc.snapshot().total_s3_requests(), 0);
        tiered.insert(tail_key, tail_block.clone());
        assert_eq!(tiered.disk_len(), 0, "the disk tier declined both blocks");
        drop(re_peek_gate);
        fetch.await.expect("both blocks verify");
        assert_eq!(
            asm.slice(KEY, 0, 16).expect("lead placed").as_ref(),
            lead_block.as_ref()
        );
        assert_eq!(
            asm.slice(KEY, 16, 16).expect("tail placed").as_ref(),
            tail_block.as_ref()
        );
        (acc.snapshot(), stats)
    }

    /// A late RAM serve is a cache miss with zero GETs and zero fetched bytes
    /// in the query's accounting (docs/guides/caching.md): the run records
    /// exactly one miss per block, from `fetch_blocks`' peek, and nothing else.
    ///
    /// FLIP: restoring `accounting.record_cache_miss()` before the non-lead
    /// block's `fetch_peeked` in `fetch_run` makes `cache_misses` read 3.
    #[test]
    fn a_late_ram_served_log_run_counts_one_miss_per_block_and_no_get() {
        let (snapshot, stats) = late_ram_served_run(TailServe::RamRecheck);
        assert_eq!(snapshot.cache_misses, 2, "one miss per block");
        assert_eq!(snapshot.cache_hits, 0);
        assert_eq!(snapshot.cache_bytes, 0);
        assert_eq!(snapshot.total_s3_requests(), 0);
        assert_eq!(snapshot.total_s3_bytes(), 0);
        assert_eq!(stats.block_range_gets, 0);
        assert_eq!(stats.block_bytes_fetched, 0);
    }

    /// The tail block of a run whose lead was served late from RAM, served by
    /// `fetch_run`'s re-peek rather than the recheck, is accounted the same
    /// way: its `fetch_blocks` peek already recorded its miss.
    ///
    /// FLIP: restoring `accounting.record_cache_hit()` and
    /// `accounting.add_cache_bytes(..)` on that re-peek's hit makes
    /// `cache_hits` read 1 and `cache_bytes` 16. Restoring
    /// `outcome.cache_hits += 1` on that hit, folded into
    /// `stats.block_cache_hits` by `fetch_blocks`, makes `block_cache_hits`
    /// read 1 against the accounting's 0.
    #[test]
    fn a_late_ram_served_log_run_counts_no_hit_for_a_re_peeked_block() {
        let (snapshot, stats) = late_ram_served_run(TailServe::RePeekHit);
        assert_eq!(snapshot.cache_misses, 2, "one miss per block");
        assert_eq!(snapshot.cache_hits, 0);
        assert_eq!(snapshot.cache_bytes, 0);
        assert_eq!(snapshot.total_s3_requests(), 0);
        assert_eq!(snapshot.total_s3_bytes(), 0);
        assert_eq!(stats.block_range_gets, 0);
        assert_eq!(stats.block_bytes_fetched, 0);
        assert_eq!(stats.block_cache_hits, snapshot.cache_hits);
        assert_eq!(stats.block_cache_hits, 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod late_serve_accounting_tests {
    //! Two callers reading the same bytes through a cache at once, the second
    //! joining the first's in-flight GET, report what each actually did: one
    //! GET between them, one tier lookup per block each caller peeked, a
    //! follower that is a query cache miss charged no GET on either cache
    //! kind, and a `block_cache_hits` that agrees with the query accounting. A
    //! `FaultStore` hold gate parks the leader's GET until the follower is in
    //! position; `MemoryStore` alone never yields, so it cannot produce the
    //! overlap.

    use super::*;
    use crate::fetcher::CacheFetchError;
    use ravel_cache::{Cache, CacheLimits, CacheMetrics, DiskCache, TieredCache};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::accounting::QueryAccountingSnapshot;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const CONTENT_HASH: [u8; 32] = [9u8; 32];
    const KEY: &str = "t/late-serve.rlog";
    const BLOCK: u64 = 16;
    const BLOCKS: u64 = 3;

    fn seg_ref(size: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: size,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 0,
            sample_count: 1,
            series_count: 0,
            shard: 0,
            content_hash: CONTENT_HASH,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    fn object() -> Bytes {
        Bytes::from((0..(BLOCK * BLOCKS) as u8).collect::<Vec<u8>>())
    }

    fn limits() -> CacheLimits {
        CacheLimits::new(1024 * 1024, 100, 1024 * 1024)
    }

    /// `object()` under `KEY`, behind a gate holding every GET.
    async fn held_store() -> (Arc<dyn ObjectStoreBackend>, GateHandle) {
        let memory = MemoryStore::new();
        memory
            .put(KEY, object(), PutOptions::default())
            .await
            .expect("put");
        let fault = Arc::new(FaultStore::new(memory, FaultPlan::default()));
        let gate = fault.hold(Op::Get, None, Occurrence::Always);
        (fault, gate)
    }

    /// Releases the one held GET once `ready` says the second caller is parked
    /// on the first's flight.
    async fn release_when(gate: &GateHandle, ready: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            gate.wait_until_held(1).await;
            while !ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the leader's GET is held and the follower parked within 30 s");
        assert_eq!(gate.held_count(), 1, "exactly one GET reached the store");
        for id in gate.held() {
            assert!(gate.release(id), "held id must release");
        }
    }

    fn extents() -> Vec<BlockExtent> {
        let object = object();
        (0..BLOCKS)
            .map(|i| {
                let start = i * BLOCK;
                BlockExtent {
                    abs_start: start,
                    len: BLOCK,
                    crc32c: crc32c::crc32c(&object[start as usize..(start + BLOCK) as usize]),
                }
            })
            .collect()
    }

    /// Two concurrent `fetch_blocks` calls over one coalesced run of
    /// [`BLOCKS`] blocks; returns each caller's accounting and stats.
    async fn race_one_run(
        cache: ReadCache,
        ready: impl Fn() -> bool,
    ) -> [(QueryAccountingSnapshot, BlockRangeStats); 2] {
        let (store, gate) = held_store().await;
        let fetcher = BlockRangeFetcher::new(store).with_cache(cache);
        let seg = seg_ref(BLOCK * BLOCKS);
        let pin = EtagPin::default();
        let extents = extents();
        let run = |acc: QueryAccounting| {
            let fetcher = &fetcher;
            let seg = &seg;
            let pin = &pin;
            let extents = &extents;
            async move {
                let mut stats = BlockRangeStats::default();
                let mut asm = ObjectAssembler::new(&fetcher.assembly_gauge, BLOCK * BLOCKS);
                fetcher
                    .fetch_blocks(
                        seg,
                        TENANT,
                        pin,
                        extents,
                        QueryPhase::Scan,
                        &mut asm,
                        &acc,
                        &mut stats,
                    )
                    .await
                    .expect("both callers' blocks verify");
                assert_eq!(
                    asm.slice(KEY, 0, BLOCK * BLOCKS)
                        .expect("every block placed")
                        .as_ref(),
                    object().as_ref()
                );
                (acc.snapshot(), stats)
            }
        };
        let (first, second, ()) = tokio::join!(
            run(QueryAccounting::new()),
            run(QueryAccounting::new()),
            release_when(&gate, ready),
        );
        [first, second]
    }

    /// The figures both cache shapes share: one GET in total, every block a
    /// query-accounting miss for each caller, and `block_cache_hits` agreeing
    /// with the accounting's hits for each caller.
    fn assert_one_get_and_agreeing_hits(callers: &[(QueryAccountingSnapshot, BlockRangeStats); 2]) {
        let gets: u64 = callers.iter().map(|(acc, _)| acc.total_s3_requests()).sum();
        assert_eq!(gets, 1, "one store GET across both callers");
        let range_gets: u64 = callers.iter().map(|(_, s)| s.block_range_gets).sum();
        assert_eq!(range_gets, 1);
        for (acc, stats) in callers {
            assert_eq!(acc.cache_misses, BLOCKS, "one miss per peeked block");
            assert_eq!(acc.cache_hits, 0);
            assert_eq!(stats.block_cache_hits, acc.cache_hits);
        }
    }

    fn assert_one_lookup_per_peek(tier: &CacheMetrics, name: &str) {
        let snap = tier.snapshot();
        assert_eq!(snap.hits, 0, "{name}: no lookup beyond the peeks hit");
        assert_eq!(
            snap.misses,
            2 * BLOCKS,
            "{name}: one lookup per block per caller"
        );
    }

    /// RAM-only cache: the follower's re-peek of the two non-lead blocks finds
    /// them resident and records nothing on the RAM tier or in its stats.
    ///
    /// FLIP: restoring `cache.get(&block_key)` for the re-peek in `fetch_run`
    /// makes the RAM tier read 2 hits; restoring `outcome.cache_hits += 1` on
    /// that hit (folded into `stats.block_cache_hits` by `fetch_blocks`) makes
    /// the follower's `block_cache_hits` 2 against its accounting's 0.
    #[tokio::test]
    async fn a_ram_only_follower_run_records_one_lookup_per_peeked_block() {
        let ram: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(limits()));
        let ram_metrics = ram.metrics();
        // The follower joins the flight in the same poll that records its last
        // peek's miss.
        let ready_metrics = ram_metrics.clone();
        let callers = race_one_run(ReadCache::Ram(ram), move || {
            ready_metrics.snapshot().misses >= 2 * BLOCKS
        })
        .await;
        assert_one_get_and_agreeing_hits(&callers);
        assert_one_lookup_per_peek(&ram_metrics, "ram");
        assert_eq!(ram_metrics.snapshot().single_flight_collapses, 1);
    }

    /// Tiered cache whose RAM tier admits nothing, so the follower's re-peek
    /// falls through to the disk tier, which holds every block: neither tier
    /// records the re-peek.
    ///
    /// FLIP: restoring `cache.get(&block_key)` for the re-peek in `fetch_run`
    /// makes the RAM tier read 8 misses and the disk tier 2 hits.
    #[tokio::test]
    async fn a_tiered_follower_run_records_one_lookup_per_peeked_block() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let ram_declines_all = CacheLimits::new(1024 * 1024, 100, 1);
        let tiered = Arc::new(TieredCache::new(
            Cache::new(ram_declines_all),
            DiskCache::new(tmp.path().to_path_buf(), limits()),
        ));
        let ram_metrics = tiered.ram_metrics();
        let disk_metrics = tiered.disk_metrics();
        let lead_key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, BLOCK);
        let ready_tiered = tiered.clone();
        let callers = race_one_run(ReadCache::Tiered(tiered.clone()), move || {
            ready_tiered.in_flight_waiters(&lead_key) >= 1
        })
        .await;
        assert_one_get_and_agreeing_hits(&callers);
        assert_one_lookup_per_peek(&ram_metrics, "ram");
        assert_one_lookup_per_peek(&disk_metrics, "disk");
        assert_eq!(
            tiered.disk_len(),
            BLOCKS as usize,
            "the disk tier holds every block"
        );
        assert_eq!(tiered.ram_len(), 0, "the RAM tier declined every block");
    }

    /// The two cache kinds the read-through tests below run against.
    #[derive(Clone, Copy, Debug)]
    enum Kind {
        Ram,
        Tiered,
    }

    /// Whether a second caller of a key is parked on the first one's flight.
    type Parked = Box<dyn Fn(&CacheKey) -> bool>;

    /// A fresh cache of `kind` and a check that a second caller of `key` is
    /// parked on the first one's flight. On the RAM-only cache the follower
    /// joins the flight in the same poll that records its peek's miss, so two
    /// RAM misses mean it is parked; the tiered cache counts its waiters.
    fn cache_of(kind: Kind, tmp: &tempfile::TempDir) -> (ReadCache, Parked) {
        match kind {
            Kind::Ram => {
                let ram: Arc<Cache<CacheFetchError>> = Arc::new(Cache::new(limits()));
                let metrics = ram.metrics();
                (
                    ReadCache::Ram(ram),
                    Box::new(move |_| metrics.snapshot().misses >= 2),
                )
            }
            Kind::Tiered => {
                let tiered = Arc::new(TieredCache::new(
                    Cache::new(limits()),
                    DiskCache::new(tmp.path().to_path_buf(), limits()),
                ));
                (
                    ReadCache::Tiered(tiered.clone()),
                    Box::new(move |key| tiered.in_flight_waiters(key) >= 1),
                )
            }
        }
    }

    /// The leader of a read-through ran the one GET and recorded one miss.
    fn assert_fetching_leader(snap: &QueryAccountingSnapshot, kind: Kind) {
        assert_eq!(snap.total_s3_requests(), 1, "{kind:?}: the leader's GET");
        assert_eq!(snap.cache_misses, 1, "{kind:?}");
        assert_eq!(snap.cache_hits, 0, "{kind:?}");
    }

    /// A late serve's query accounting: no GET of its own, and the one cache
    /// miss its lookup counted, with no hit and no cache bytes.
    fn assert_late_served(snap: &QueryAccountingSnapshot, kind: Kind) {
        assert_eq!(snap.total_s3_requests(), 0, "{kind:?}: no GET of its own");
        assert_eq!(snap.total_s3_bytes(), 0, "{kind:?}");
        assert_eq!(snap.cache_misses, 1, "{kind:?}: a late serve stays a miss");
        assert_eq!(snap.cache_hits, 0, "{kind:?}");
        assert_eq!(snap.cache_bytes, 0, "{kind:?}");
    }

    /// Two `cached_extent` calls for one extent, the second following the
    /// first's flight: only the call whose GET ran reports `live`, and the
    /// follower records a query cache miss, on both cache kinds.
    ///
    /// FLIP: mapping `ReadOutcome::LateServe` to the `ReadOutcome::Hit` arm in
    /// `cached_extent_outcome` makes the follower a hit with cache bytes (both
    /// kinds); reporting a tiered follower as `ReadOutcome::Fetched` (the
    /// `(Served::Upstream, Role::Follower)` arm of `Served::outcome`)
    /// makes it live.
    #[tokio::test]
    async fn a_cached_extent_follower_is_a_miss_and_not_live() {
        for kind in [Kind::Ram, Kind::Tiered] {
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let (cache, parked) = cache_of(kind, &tmp);
            let (store, gate) = held_store().await;
            let fetcher = BlockRangeFetcher::new(store).with_cache(cache);
            let seg = seg_ref(BLOCK * BLOCKS);
            let key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, BLOCK);
            let pin = EtagPin::default();
            let leader_acc = QueryAccounting::new();
            let follower_acc = QueryAccounting::new();
            let extent = |acc| {
                fetcher.cached_extent(
                    &seg,
                    TENANT,
                    0,
                    BLOCK,
                    GetRange::Range(0, BLOCK),
                    QueryPhase::Scan,
                    &pin,
                    acc,
                )
            };
            let (leader, follower, ()) = tokio::join!(
                extent(&leader_acc),
                extent(&follower_acc),
                release_when(&gate, || parked(&key)),
            );
            let (leader_bytes, leader_live) = leader.expect("leader extent");
            let (follower_bytes, follower_live) = follower.expect("follower extent");
            assert_eq!(leader_bytes, object().slice(0..BLOCK as usize));
            assert_eq!(follower_bytes, leader_bytes);
            assert!(
                leader_live,
                "{kind:?}: the leader's GET crossed the network"
            );
            assert!(!follower_live, "{kind:?}: the follower made no GET");
            assert_fetching_leader(&leader_acc.snapshot(), kind);
            assert_late_served(&follower_acc.snapshot(), kind);
        }
    }

    /// Two page-granular reads of one page range, the second following the
    /// first's flight: the follower counts neither a `block_range_gets` nor a
    /// `block_cache_hits`, agreeing with its query accounting's one miss, on
    /// both cache kinds.
    ///
    /// FLIP: counting `ReadOutcome::LateServe` in `block_cache_hits` in
    /// `fetch_chunk_ranges` (its `ReadOutcome::LateServe => {}` arm) makes the
    /// follower's `block_cache_hits` 1 against its accounting's 0 hits.
    #[tokio::test]
    async fn a_page_range_follower_counts_neither_a_get_nor_a_block_hit() {
        for kind in [Kind::Ram, Kind::Tiered] {
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let (cache, parked) = cache_of(kind, &tmp);
            let (store, gate) = held_store().await;
            let fetcher = BlockRangeFetcher::new(store).with_cache(cache);
            let seg = seg_ref(BLOCK * BLOCKS);
            let size = BLOCK * BLOCKS;
            let key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, size);
            let pin = EtagPin::default();
            let wanted = [ByteExtent {
                abs_start: 0,
                len: size,
            }];
            let read = |acc: QueryAccounting| {
                let fetcher = &fetcher;
                let seg = &seg;
                let pin = &pin;
                let wanted = &wanted;
                async move {
                    let mut stats = BlockRangeStats::default();
                    let mut asm = ObjectAssembler::new(&fetcher.assembly_gauge, size);
                    fetcher
                        .fetch_chunk_ranges(
                            seg,
                            TENANT,
                            pin,
                            wanted,
                            &[],
                            QueryPhase::Scan,
                            &mut asm,
                            &acc,
                            &mut stats,
                        )
                        .await
                        .expect("page range read");
                    assert_eq!(
                        asm.slice(KEY, 0, size).expect("range placed").as_ref(),
                        object().as_ref()
                    );
                    (acc.snapshot(), stats)
                }
            };
            let ((leader_snap, leader_stats), (follower_snap, follower_stats), ()) = tokio::join!(
                read(QueryAccounting::new()),
                read(QueryAccounting::new()),
                release_when(&gate, || parked(&key)),
            );
            assert_fetching_leader(&leader_snap, kind);
            assert_eq!(leader_stats.block_range_gets, 1, "{kind:?}");
            assert_eq!(leader_stats.block_bytes_fetched, size, "{kind:?}");
            assert_eq!(leader_stats.block_cache_hits, 0, "{kind:?}");
            assert_late_served(&follower_snap, kind);
            assert_eq!(follower_stats.block_range_gets, 0, "{kind:?}");
            assert_eq!(follower_stats.block_bytes_fetched, 0, "{kind:?}");
            assert_eq!(
                follower_stats.block_cache_hits, follower_snap.cache_hits,
                "{kind:?}: block_cache_hits agrees with the query accounting"
            );
        }
    }

    /// `(s3_requests, s3_bytes)` of every `page_fetch` span closed while this
    /// layer is the thread's subscriber.
    #[derive(Clone, Default)]
    struct PageFetchCosts {
        open: Arc<std::sync::Mutex<std::collections::HashMap<u64, (u64, u64)>>>,
        closed: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
    }

    struct CostVisitor<'a>(&'a mut (u64, u64));

    impl tracing::field::Visit for CostVisitor<'_> {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "s3_requests" => self.0.0 = value,
                "s3_bytes" => self.0.1 = value,
                _ => {}
            }
        }

        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PageFetchCosts {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() == "page_fetch" {
                let mut cost = (u64::MAX, u64::MAX);
                attrs.record(&mut CostVisitor(&mut cost));
                self.open.lock().expect("lock").insert(id.into_u64(), cost);
            }
        }

        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if let Some(cost) = self.open.lock().expect("lock").get_mut(&id.into_u64()) {
                values.record(&mut CostVisitor(cost));
            }
        }

        fn on_close(&self, id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            if let Some(cost) = self.open.lock().expect("lock").remove(&id.into_u64()) {
                self.closed.lock().expect("lock").push(cost);
            }
        }
    }

    /// Two whole-object reads of one object, the second following the first's
    /// flight: one GET between them, the follower's `page_fetch` span records
    /// zero requests and zero bytes, and its query accounting records a cache
    /// miss, not a hit, on both cache kinds.
    ///
    /// FLIP: mapping `ReadOutcome::LateServe` to the `ReadOutcome::Hit` arm in
    /// `LogSegmentFetcher::whole_object_bytes` makes the follower a hit with
    /// cache bytes (both kinds); mapping it to the `ReadOutcome::Fetched` arm,
    /// or reporting a tiered follower as `ReadOutcome::Fetched`, makes the
    /// follower's span record one request, so the spans read `[(1, 48), (1, 48)]`.
    #[tokio::test]
    // Holds the test_tracing serialization guard across `.await`; the
    // current-thread test runtime runs this future to completion with no other
    // task, so there is no deadlock risk the lint guards against.
    #[allow(clippy::await_holding_lock)]
    async fn a_whole_object_follower_is_a_miss_with_no_get_in_its_span() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let _serial = crate::test_tracing::guard();
        for kind in [Kind::Ram, Kind::Tiered] {
            let costs = PageFetchCosts::default();
            let _subscriber = tracing_subscriber::registry()
                .with(costs.clone())
                .set_default();
            let tmp = tempfile::TempDir::new().expect("tempdir");
            let (cache, parked) = cache_of(kind, &tmp);
            let (store, gate) = held_store().await;
            let fetcher = LogSegmentFetcher::new(store).with_cache(cache);
            let size = BLOCK * BLOCKS;
            let seg = seg_ref(size);
            let key = CacheKey::new(TENANT.0, CONTENT_HASH, 0, size);
            let leader_acc = QueryAccounting::new();
            let follower_acc = QueryAccounting::new();
            let (leader, follower, ()) = tokio::join!(
                fetcher.whole_object_bytes(&seg, TENANT, QueryPhase::Scan, &leader_acc),
                fetcher.whole_object_bytes(&seg, TENANT, QueryPhase::Scan, &follower_acc),
                release_when(&gate, || parked(&key)),
            );
            leader.expect("leader read");
            follower.expect("follower read");
            assert_fetching_leader(&leader_acc.snapshot(), kind);
            assert_late_served(&follower_acc.snapshot(), kind);
            let mut spans = costs.closed.lock().expect("lock").clone();
            spans.sort_unstable();
            assert_eq!(
                spans,
                vec![(0, 0), (1, size)],
                "{kind:?}: the leader's span records its GET, the follower's none"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod owned_block_plan_tests {
    //! ADR-2414 decision A1, deliverables 1 and 3: a per-partition open's ranged
    //! plan covers only the partition's own blocks, and with the plan phase's
    //! decoded directories in hand it fetches no front directory section.
    //!
    //! The object has three row groups of four one-record blocks. The projection
    //! is the fixed columns only (`ts` and the stream reference), so the ranged
    //! plan stays below the coverage crossover and every fetched byte is a page
    //! of a named block. What was fetched is read back off the returned buffer:
    //! a page the plan did not place is [`LogSegError::Unplaced`] there.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const KEY: &str = "t/owned.rlog";
    const GROUP_BLOCKS: usize = 4;
    const BLOCKS: usize = 3 * GROUP_BLOCKS;

    /// With `two_streams`, even timestamps belong to one stream and odd ones to
    /// another, so a stream-attribute filter has something to select; the
    /// writer then lays each stream's records out in its own blocks.
    fn record(ts: i64, two_streams: bool) -> LogRecord {
        let service = if two_streams && ts % 2 == 0 {
            "even"
        } else if two_streams {
            "odd"
        } else {
            "svc"
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str(service.into()))];
        LogRecord {
            stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("body {ts} {}", "x".repeat(256)),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    struct Fixture {
        fetcher: LogSegmentFetcher,
        seg: SegmentRef,
        object: Vec<u8>,
    }

    async fn fixture() -> Fixture {
        fixture_with(false).await
    }

    async fn fixture_with(two_streams: bool) -> Fixture {
        fixture_with_gap(two_streams, Some(0)).await
    }

    /// `coalesce_gap` `None` keeps the fetcher's default gap, which bridges
    /// every hole this object has.
    async fn fixture_with_gap(two_streams: bool, coalesce_gap: Option<u64>) -> Fixture {
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
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer.push(record(ts, two_streams)).expect("push");
        }
        let object = writer.finish().expect("finish");
        let store = Arc::new(MemoryStore::new());
        store
            .put(KEY, Bytes::from(object.clone()), PutOptions::default())
            .await
            .expect("put");
        let seg = SegmentRef {
            data_object_key: KEY.to_string(),
            object_size: object.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [9u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(footer::VERSION),
            declared_column_stats: Default::default(),
        };
        // A probe reaching the whole tail, so SKIP_IDX and PAGE_DIR arrive with
        // it and only the front sections and block pages are separate reads.
        let parsed = footer::open(&object).expect("footer");
        let blocks = parsed.section(kind::BLOCKS).expect("BLOCKS");
        let tail = object.len() as u64 - (blocks.offset + blocks.len);
        let mut range_fetcher = BlockRangeFetcher::new(store.clone())
            .with_suffix_len(tail)
            .with_whole_object_threshold(0);
        if let Some(gap) = coalesce_gap {
            range_fetcher = range_fetcher.with_coalesce_gap(gap);
        }
        let fetcher = LogSegmentFetcher::new(store)
            .with_block_range_threshold(0)
            .with_block_range(range_fetcher);
        Fixture {
            fetcher,
            seg,
            object,
        }
    }

    /// Whether every page of `block` the projection reads is in `bytes`.
    fn block_is_placed(fx: &Fixture, bytes: &LogObjectBytes, block: usize) -> bool {
        let dirs = RlogReader::decode_directories(&fx.object[..], &RlogConfig::default())
            .expect("directories");
        let selected = ColumnSelection::fixed_only()
            .resolve(dirs.field_dir())
            .expect("a narrow projection resolves to a column set");
        let blocks_offset = footer::open(&fx.object)
            .expect("footer")
            .section(kind::BLOCKS)
            .expect("BLOCKS")
            .offset;
        let pages: Vec<_> = dirs
            .page_dir()
            .block_pages(block as u32)
            .expect("block in the directory")
            .into_iter()
            .filter(|p| selected.contains(&p.desc.column_id))
            .collect();
        assert!(
            !pages.is_empty(),
            "the projection reads pages of every block"
        );
        let placed = pages
            .iter()
            .map(|p| bytes.read(blocks_offset + p.offset, p.desc.len).is_ok())
            .collect::<Vec<_>>();
        assert!(
            placed.iter().all(|&p| p) || placed.iter().all(|&p| !p),
            "block {block}'s pages are placed together or not at all: {placed:?}"
        );
        placed[0]
    }

    async fn plan(
        fx: &Fixture,
        ts_min_ns: i64,
        owned: Option<OwnedBlocks<'_>>,
    ) -> (LogObjectBytes, BlockRangeStats) {
        fx.fetcher
            .block_range
            .fetch_object_with_footer_subset(
                &fx.seg,
                TENANT,
                ts_min_ns,
                i64::MAX,
                &[],
                &ColumnSelection::fixed_only(),
                None,
                ReadPhases::SCAN,
                owned,
                &QueryAccounting::new(),
            )
            .await
            .expect("fetch")
    }

    fn placed_blocks(fx: &Fixture, bytes: &LogObjectBytes) -> Vec<usize> {
        (0..BLOCKS)
            .filter(|&b| block_is_placed(fx, bytes, b))
            .collect()
    }

    /// A partition owning the middle row group places exactly that group's four
    /// blocks, and fetches fewer bytes than the plan over every block.
    ///
    /// Fails against a plan keyed on the ts bounds alone (all twelve blocks are
    /// candidates and placed).
    #[tokio::test]
    async fn the_ranged_plan_covers_only_the_partitions_blocks() {
        let fx = fixture().await;
        let owned = [4usize, 5, 6, 7];
        let (bytes, stats) = plan(
            &fx,
            i64::MIN,
            Some(OwnedBlocks {
                blocks: &owned,
                dirs: None,
            }),
        )
        .await;
        assert_eq!(stats.candidate_blocks, 4, "only the owned blocks");
        assert_eq!(placed_blocks(&fx, &bytes), vec![4, 5, 6, 7]);

        let (all_bytes, all_stats) = plan(&fx, i64::MIN, None).await;
        assert_eq!(all_stats.candidate_blocks, BLOCKS as u64);
        assert_eq!(
            placed_blocks(&fx, &all_bytes),
            (0..BLOCKS).collect::<Vec<_>>()
        );
        assert!(
            stats.block_bytes_fetched * 2 < all_stats.block_bytes_fetched,
            "one row group of three fetches under half the bytes: {} vs {}",
            stats.block_bytes_fetched,
            all_stats.block_bytes_fetched
        );
    }

    /// The owned list names whole-object block indices, so it intersects the
    /// ts candidate set by value. With the ts window starting at block 2 the
    /// candidates are blocks 2..12; owning blocks 4..8 places those four, not
    /// the four at positions 4..8 of the candidate list (blocks 6..10).
    ///
    /// Fails against an implementation that reads the owned list as positions
    /// into the candidate list.
    #[tokio::test]
    async fn the_owned_list_is_whole_object_block_indices() {
        let fx = fixture().await;
        let owned = [4usize, 5, 6, 7];
        let (bytes, stats) = plan(
            &fx,
            2,
            Some(OwnedBlocks {
                blocks: &owned,
                dirs: None,
            }),
        )
        .await;
        assert_eq!(stats.candidate_blocks, 4);
        assert_eq!(placed_blocks(&fx, &bytes), vec![4, 5, 6, 7]);
    }

    /// With the plan phase's directories in hand the open fetches no front
    /// directory section: STREAM_DIR and FIELD_DIR are not placed, where
    /// without them both are.
    ///
    /// Fails against an open that ignores the carried directories and fetches
    /// the sections again.
    #[tokio::test]
    async fn carried_directories_fetch_no_front_section() {
        let fx = fixture().await;
        let dirs = RlogReader::decode_directories(&fx.object[..], &RlogConfig::default())
            .expect("directories");
        let owned = [4usize, 5, 6, 7];
        let parsed = footer::open(&fx.object).expect("footer");
        let front_placed = |bytes: &LogObjectBytes| {
            [kind::STREAM_DIR, kind::FIELD_DIR].map(|k| {
                let desc = parsed.section(k).expect("front section");
                bytes.read(desc.offset, desc.len).is_ok()
            })
        };

        let (with, _) = plan(
            &fx,
            i64::MIN,
            Some(OwnedBlocks {
                blocks: &owned,
                dirs: Some(&dirs),
            }),
        )
        .await;
        assert_eq!(front_placed(&with), [false, false]);
        assert_eq!(placed_blocks(&fx, &with), vec![4, 5, 6, 7]);

        let (without, _) = plan(
            &fx,
            i64::MIN,
            Some(OwnedBlocks {
                blocks: &owned,
                dirs: None,
            }),
        )
        .await;
        assert_eq!(front_placed(&without), [true, true]);
    }

    /// A stream-attribute filter resolves against the carried STREAM_DIR: the
    /// open's ranged fetch places no front section, so reading STREAM_DIR out
    /// of the fetched buffer would fail, and the rows must still be exactly
    /// the matching stream's.
    ///
    /// Fails against a subset open that resolves the filter from the buffer
    /// (`Unplaced` on STREAM_DIR) rather than from the carried directories.
    #[tokio::test]
    async fn a_stream_filter_resolves_from_the_carried_directories() {
        let fx = fixture_with(true).await;
        let query = LogQuery::new(i64::MIN, i64::MAX).with_stream_attr(StreamAttrEquals::new(
            "service.name",
            AttrValue::Str("even".into()),
        ));
        let acc = QueryAccounting::new();
        let (indices, dirs, _stats, _footer, _carried) = fx
            .fetcher
            .plan_segment(&fx.seg, TENANT, &query, &acc)
            .await
            .expect("plan")
            .expect("relevant segment");
        assert_eq!(
            indices,
            vec![0, 1, 2, 3, 4, 5],
            "the matching stream's records fill the first six blocks"
        );

        let mut scan = fx
            .fetcher
            .scan_accounted_with_tenant_subset_raw(
                &fx.seg,
                TENANT,
                &query,
                &ColumnSelection::all(),
                &indices,
                &indices,
                None,
                None,
                Some(&dirs),
                &acc,
            )
            .await
            .expect("open")
            .expect("relevant segment");
        let mut ts = Vec::new();
        while let Some(rows) = scan.next_block().expect("block") {
            ts.extend(rows.iter().map(|r| r.ts_ns));
        }
        assert_eq!(ts, vec![0, 2, 4, 6, 8, 10]);
    }

    /// Two partitions dealt interleaved row groups of one object (A the first
    /// and third, B the middle one) fetch disjoint spans: neither places a page
    /// of the other's group, though the default coalesce gap is wider than the
    /// hole between A's two groups. Their wire bytes then sum to at most the
    /// BLOCKS section's length, and each issues one request per owned group.
    ///
    /// Fails against a plan that bridges and coalesces over the whole
    /// candidate set's holes regardless of ownership (A's run spans group 1,
    /// so A places B's blocks and the sum passes the section length), and
    /// against a plan that stops bridging for a partition altogether (A and B
    /// issue one GET per column chunk, not one per group).
    #[tokio::test]
    async fn partitions_dealt_interleaved_groups_fetch_disjoint_spans() {
        let fx = fixture_with_gap(false, None).await;
        let blocks_len = footer::open(&fx.object)
            .expect("footer")
            .section(kind::BLOCKS)
            .expect("BLOCKS")
            .len;
        let owned_a: Vec<usize> = (0..GROUP_BLOCKS).chain(2 * GROUP_BLOCKS..BLOCKS).collect();
        let owned_b: Vec<usize> = (GROUP_BLOCKS..2 * GROUP_BLOCKS).collect();
        let dirs = RlogReader::decode_directories(&fx.object[..], &RlogConfig::default())
            .expect("directories");
        let own = |blocks| {
            Some(OwnedBlocks {
                blocks,
                dirs: Some(&dirs),
            })
        };
        let (bytes_a, stats_a) = plan(&fx, i64::MIN, own(&owned_a[..])).await;
        let (bytes_b, stats_b) = plan(&fx, i64::MIN, own(&owned_b[..])).await;

        assert_eq!(placed_blocks(&fx, &bytes_a), owned_a);
        assert_eq!(placed_blocks(&fx, &bytes_b), owned_b);
        assert!(
            stats_a.block_bytes_fetched + stats_b.block_bytes_fetched <= blocks_len,
            "wire bytes across partitions {} + {} exceed the object's BLOCKS section {blocks_len}",
            stats_a.block_bytes_fetched,
            stats_b.block_bytes_fetched,
        );
        assert_eq!(
            stats_b.block_range_gets, 1,
            "one run for the one owned group: holes inside a group are still bridged"
        );
        assert_eq!(
            stats_a.block_range_gets, 2,
            "one run per owned group, not one spanning the group between them"
        );
    }

    #[test]
    fn a_fence_stops_coalescing_and_bridging() {
        let ext = |start: u64, end: u64| ByteExtent {
            abs_start: start,
            len: end - start,
        };
        assert_eq!(
            merge_fences(&[ext(500, 510), ext(240, 260), ext(200, 250)]),
            vec![(200, 260), (500, 510)]
        );
        let fences = merge_fences(&[ext(200, 250), ext(240, 260)]);

        // Within the gap limit but holding fence bytes: kept apart. The same
        // extents with no fence join.
        let wanted = [ext(0, 100), ext(150, 190), ext(300, 400)];
        let runs = |fences: &[(u64, u64)]| -> Vec<(u64, u64)> {
            coalesce_fenced(&wanted, 1_000, fences)
                .iter()
                .map(|r| (r.abs_start, r.abs_end()))
                .collect()
        };
        assert_eq!(runs(&[]), vec![(0, 400)]);
        assert_eq!(runs(&fences), vec![(0, 190), (300, 400)]);

        // Bounding to one run: the gap holding a fence is the one left open
        // even though it is not the smallest.
        let bound = |runs: Vec<(u64, u64)>, cap: usize| bound_runs_fenced(runs, cap, &fences);
        let three = vec![(0, 190), (300, 400), (700, 800)];
        assert_eq!(bound(three.clone(), 2), vec![(0, 190), (300, 800)]);
        assert_eq!(bound(three.clone(), 1), vec![(0, 190), (300, 800)]);
        assert_eq!(
            bound_runs(three, 1),
            vec![(0, 800)],
            "unfenced bridges both"
        );
    }
}
