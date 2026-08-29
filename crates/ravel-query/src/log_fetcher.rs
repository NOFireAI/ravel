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
//! the relevant data, assembled into an object-sized buffer the unchanged
//! reader decodes from. What a "relevant part" is depends on the object's
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

use std::collections::HashSet;
use std::sync::Arc;

use crate::erasure::ErasurePredicate;
use crate::fetcher::ReadCache;
use crate::phase_accounting::PhaseAccounting;
use bytes::Bytes;
use ravel_cache::{CacheKey, SingleFlightError, Source};
use ravel_catalog::SegmentRef;
use ravel_logseg::block::NumStat;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, SectionDesc, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::skip_index::{Level0Entry, NumRangeArm, SkipIndex, merge_stats};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, BlockScan, ColumnSelection, ColumnarBlockView, LogRecord, LogSegError, LogStreamId,
    Predicate, RlogConfig, RlogReader, ScanStats, SuffixOutcome, decode_section, open_from_suffix,
    read_section,
};
use ravel_object_store::{Etag, GetOutcome, GetRange, ObjectStoreBackend, StoreError};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::canonical_attr_bytes;
use tokio::sync::Semaphore;
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
    /// The whole object. Block extents are absolute offsets into it.
    bytes: Bytes,
    scan: BlockScan,
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
}

impl LogSegmentScan {
    /// The reader's pruning counters. `blocks_scanned`, `pages_decoded`, and
    /// `pages_skipped` grow as blocks are drained; read this after the last
    /// [`next_block`](Self::next_block) for the whole segment's figures.
    pub fn stats(&self) -> ScanStats {
        self.scan.stats()
    }

    /// Surviving blocks not yet decoded.
    pub fn remaining_blocks(&self) -> usize {
        self.scan.remaining_blocks()
    }

    /// Decode the next surviving block and return its matching, unerased rows,
    /// or `None` once every surviving block has been decoded.
    ///
    /// `Some(vec![])` is normal and distinct from `None`: a block can survive
    /// pruning and hold no row that matches the exact filter, or have every
    /// matching row erased. Only `None` ends the scan.
    pub fn next_block(&mut self) -> Result<Option<Vec<LogRecord>>, LogFetchError> {
        let span = self.span.clone();
        let decoded = span
            .in_scope(|| self.scan.next_block(&self.bytes))
            .map_err(|source| corrupt(&self.key, source))?;
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
        if self.scan.remaining_blocks() == 0 {
            self.finish();
            return Ok(ColumnarBlockOutcome::Exhausted);
        }
        // Destructured so entering the span borrows only `span` and the view's
        // borrow of the cursor is not held by a closure.
        let Self {
            bytes,
            scan,
            span,
            key,
            ..
        } = self;
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
        let stats = self.scan.stats();
        self.span.record("blocks_scanned", stats.blocks_scanned);
        self.span.record("blocks_total", stats.blocks_total);
        // Decode-time column-filtering accounting (ADR-0107 decision 4), folded
        // once at exhaustion into the query's handle: page_bytes_fetched vs.
        // page_bytes_decoded expose how much of each fetched block a narrow
        // projection discards. A separate, additive axis from the wire bytes T1
        // records through `add_s3_bytes`; see the QueryAccounting field docs.
        self.accounting
            .add_page_bytes_fetched(stats.page_bytes_fetched);
        self.accounting
            .add_page_bytes_decoded(stats.page_bytes_decoded);
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
}

impl LogSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        LogSegmentFetcher {
            store: store.clone(),
            cfg: RlogConfig::default(),
            cache: None,
            block_range_threshold: DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            block_range: BlockRangeFetcher::new(store),
        }
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

    /// Bounds the in-flight object-store GETs of the block-range path (ADR-0107
    /// decision 1's permit pool, [`DEFAULT_LOG_MAX_CONCURRENT_GETS`] by
    /// default). This is the seam `--fetch-concurrency` (ADR-0088) reaches the
    /// logs signal through; a scan planned at more partitions than this pool
    /// has permits queues on it (issue #700).
    #[must_use]
    pub fn with_max_concurrent_gets(mut self, n: usize) -> Self {
        self.block_range = self.block_range.with_max_concurrent_gets(n);
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

    /// Replaces the block-range fetcher (ADR-0107) with a fully configured one,
    /// for callers and tests that need to set the coalescing gap, coverage
    /// crossover, or concurrency bound directly. The replacement keeps this
    /// instance's `block_range_threshold`; pair it with
    /// [`with_block_range_threshold`](Self::with_block_range_threshold) to route
    /// small fixtures through it.
    #[must_use]
    pub fn with_block_range(mut self, block_range: BlockRangeFetcher) -> Self {
        self.block_range = block_range;
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
    pub fn matching_streams(
        &self,
        bytes: &[u8],
        filters: &[StreamAttrEquals],
    ) -> Result<Vec<LogStreamId>, LogSegError> {
        let dir = self.decode_stream_dir(bytes)?;
        let needles: Vec<Vec<u8>> = filters.iter().map(stream_attr_needle).collect();
        let mut out = Vec::new();
        for entry in dir.entries() {
            if needles.iter().all(|n| blob_contains(&entry.blob, n)) {
                out.push(entry.stream_id);
            }
        }
        Ok(out)
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
    /// funnel `LogSegmentFetcher` did not have before). `engine.rs` has no
    /// references to `LogSegmentFetcher` at all; the real production callers
    /// (ravel-sql's `logs_provider`, `alerts_scan`, `audit_scan`, and
    /// `audit_provider`) still call the unaccounted [`fetch`](Self::fetch).
    /// Wiring them onto this funnel is future work; `fetch` stays the
    /// unaccounted entry point until then, so those callers need no
    /// signature change yet.
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
            // unaccounted `fetch` (and by tests); the real production log/alerts/
            // audit callers in `ravel-sql` go through
            // `fetch_accounted_with_tenant`, which carries its own copy of these
            // spans over its own (cache-aware) GET path. Wiring those callers onto
            // an accounted funnel at all is separate, still-open future work.
            let fetch_span = tracing::debug_span!(
                "page_fetch",
                signal = "logs",
                s3_requests = tracing::field::Empty,
                s3_bytes = tracing::field::Empty,
            );
            let got = async {
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
            // This funnel issues exactly one whole-object GET per call.
            fetch_span.record("s3_requests", 1u64);
            fetch_span.record("s3_bytes", got.data.len() as u64);
            self.decode_spanned(key, &got.data, query)
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
    /// tenant. Wiring production callers (`ravel-sql`'s `logs_provider`,
    /// `alerts_scan`, `audit_scan`, `audit_provider`) onto this method
    /// instead of [`fetch_accounted`](Self::fetch_accounted) is out of
    /// scope here: it is a `ravel-sql` change, and moving those callers onto
    /// the accounted funnel is separately tracked future work.
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
        // `decode_spanned` (below) records no accounting of its own -- unlike
        // the `LogSegmentScan`-returning funnels below, this one fully
        // decodes and returns before this function returns, so buffering into
        // a disposable `PhaseAccounting` and merging once is safe here.
        let phase = PhaseAccounting::new();
        let accounting = phase.scan();
        // This funnel's decode (`scan_bytes`) reads every column, so the fetch
        // selection is the same: on a version-4 object it brings every column
        // chunk of every surviving block (ADR-0699 decision 5).
        let all = ColumnSelection::all();
        let result = async {
            let Some(bytes) = self
                .tenant_bytes(seg_ref, tenant_hash, query, &all, accounting)
                .await?
            else {
                return Ok(None);
            };
            self.decode_spanned(&seg_ref.data_object_key, &bytes, query)
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
    /// the whole object. Above it the bytes are an object-sized buffer with only
    /// the directory sections and the pruning-relevant blocks populated, fetched
    /// by [`BlockRangeFetcher`] as a probe plus coalesced block ranges, so the
    /// raw bytes moved are proportional to pruning rather than to object size --
    /// but the resident buffer is still object-sized, per call.
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
        let Some(bytes) = self
            .tenant_bytes(seg_ref, tenant_hash, query, columns, accounting)
            .await?
        else {
            return Ok(None);
        };
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let scan = span.in_scope(|| self.open_scan(key, &bytes, query, columns))?;
        Ok(Some(LogSegmentScan {
            bytes,
            scan,
            erasure: query.erasure.clone(),
            span,
            key: key.to_string(),
            accounting: accounting.clone(),
            finished: false,
        }))
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
            .whole_object_bytes(seg_ref, tenant_hash, accounting)
            .await?;
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let scan = span.in_scope(|| self.open_scan(key, &bytes, query, columns))?;
        Ok(Some(LogSegmentScan {
            bytes,
            scan,
            erasure: query.erasure.clone(),
            span,
            key: key.to_string(),
            accounting: accounting.clone(),
            finished: false,
        }))
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
    /// probe, the skip-decidable path's `fetch_plan_sections`, and the
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
    ) -> Result<Option<(usize, ScanStats, Option<footer::LogFooter>)>, LogFetchError> {
        let phase = PhaseAccounting::new();
        let accounting = phase.plan();
        let result = async {
            // Fast path (#693): a query with no block-level predicate whose ts
            // window fully CONTAINS the segment's span prunes nothing -- every block
            // survives -- so the survivor count is the footer's `block_count` and no
            // block-range fetch or decode is needed. Only the ADR-0107 suffix probe
            // runs, to read the footer. Containment is strictly stronger than
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
                let (n, stats, footer) = self
                    .plan_segment_fast(seg_ref, tenant_hash, accounting)
                    .await?;
                return Ok(Some((n, stats, Some(footer))));
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
                let (footer, skip, field_dir, stats) = async {
                    self.block_range
                        .fetch_plan_sections(seg_ref, tenant_hash, accounting)
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
                fetch_span.record("probe_misses", stats.probe_misses);

                let refs: Vec<&Predicate> = query.prune.iter().collect();
                let numeric = field_dir.numeric_range_arms(&refs);
                let survivors = skip
                    .candidate_blocks(query.ts_min_ns, query.ts_max_ns, None, &numeric)
                    .len();
                let blocks = skip.l0.len() as u32;
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
                    bloom_degraded: false,
                    postings_degraded: false,
                };
                return Ok(Some((survivors, plan_stats, Some(footer))));
            }

            // Fallback: a predicate the skip index cannot decide (a `has_word`/text
            // content arm with only a per-block bloom, an attribute-equality POSTINGS
            // prune, a stream filter), or a below-threshold object. Read as before --
            // the survivor count then needs the reader's full prune over the fetched
            // buffer -- and hand no footer forward. This whole-object plan read is the
            // amplification #761 could not remove for these shapes; the caller counts
            // it (a `None` footer on a relevant segment) as a `plan_full_reads` so a
            // report can see which queries still pay it.
            let all = ColumnSelection::all();
            let Some(bytes) = self
                .tenant_bytes(seg_ref, tenant_hash, query, &all, accounting)
                .await?
            else {
                return Ok(None);
            };
            let key = &seg_ref.data_object_key;
            let span = decode_span();
            let scan = span.in_scope(|| self.open_scan(key, &bytes, query, &all))?;
            Ok(Some((scan.remaining_blocks(), scan.stats(), None)))
        }
        .await;
        caller_accounting.merge_snapshot(&phase.snapshot().pooled());
        result
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

    /// The predicate-free plan fast path (#693): read only the footer via the
    /// ADR-0107 suffix probe and derive the whole-segment plan counts from
    /// `footer.block_count` without fetching or decoding any block. Returns the
    /// parsed [`footer::LogFooter`] alongside the counts so the per-partition
    /// subset opens can reuse it and skip re-probing (#693 part 3, deliverable
    /// 2; see [`fetch_object_with_footer`](Self::fetch_object_with_footer)).
    ///
    /// For a genuinely predicate-free, ts-contained query these are exactly the
    /// counts real pruning would compute: every block survives every stage, so
    /// `blocks_total`/`blocks_after_skip`/`blocks_after_postings`/
    /// `blocks_after_bloom` all equal the block count, nothing is scanned or
    /// decoded, and neither pruning stage degrades. `footer.block_count` equals
    /// the read-time `ScanStats.blocks_total` (`skip.l0.len()`) on every
    /// well-formed object: both are stamped from the writer's one-entry-per-block
    /// `block_spans` counter (see the `footer_block_count_matches_unpruned_\
    /// blocks_total` round-trip proof in `ravel-logseg`).
    async fn plan_segment_fast(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(usize, ScanStats, footer::LogFooter), LogFetchError> {
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
        let (footer, stats) = async {
            self.block_range
                .fetch_footer(seg_ref, tenant_hash, accounting)
                .await
        }
        .instrument(fetch_span.clone())
        .await?;
        let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
        fetch_span.record("s3_requests", requests);
        fetch_span.record("s3_bytes", stats.block_bytes_fetched);
        // Probe misses (#883): a footer chase here is a probe too short to reach
        // even the footer, reported per phase beside the request/byte counts.
        fetch_span.record("probe_misses", stats.probe_misses);

        let n = usize::try_from(footer.block_count).map_err(|_| LogFetchError::Corrupt {
            key: seg_ref.data_object_key.clone(),
            source: LogSegError::Corrupted("footer block_count out of range".into()),
        })?;
        let blocks = footer.block_count as u32;
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
            bloom_degraded: false,
            postings_degraded: false,
        };
        Ok((n, plan_stats, footer))
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
        fetch_span.record("probe_misses", stats.probe_misses);

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
    /// restricted to the surviving blocks at the positions in `indices`
    /// (intra-segment scan partitioning, ADR-0102). Same GET, same cache, same
    /// pruning; the returned [`LogSegmentScan`] drains only the named subset of
    /// the segment's surviving blocks, in the order given.
    ///
    /// `indices` index into the ordered survivor list this query's pruning
    /// produces over this (immutable) object, the same list a prior
    /// [`plan_segment`](Self::plan_segment) counted, so a partition hands the
    /// exact positions it was assigned. The returned scan's whole-segment stats
    /// totals are reported by [`plan_segment`] instead, to keep one segment's
    /// totals from being counted once per partition (see `ravel_sql::
    /// logs_scan`).
    ///
    /// `footer`, when `Some`, is the [`footer::LogFooter`] a prior
    /// [`plan_segment`](Self::plan_segment) fast path already read for this exact
    /// (immutable) object (#693 part 3, deliverable 2). Supplying it lets the
    /// block-range read skip its own etag-establishing suffix probe and pin on
    /// the first section/block GET instead (see
    /// [`fetch_object_with_footer`](Self::fetch_object_with_footer)); `None`
    /// probes as before. It changes only the read shape, never the bytes decoded.
    ///
    /// [`scan_accounted_with_tenant`]: Self::scan_accounted_with_tenant
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
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        let Some(bytes) = self
            .tenant_bytes_with_footer(seg_ref, tenant_hash, query, columns, footer, accounting)
            .await?
        else {
            return Ok(None);
        };
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let scan = span.in_scope(|| self.open_scan_subset(key, &bytes, query, columns, indices))?;
        Ok(Some(LogSegmentScan {
            bytes,
            scan,
            erasure: query.erasure.clone(),
            span,
            key: key.to_string(),
            accounting: accounting.clone(),
            finished: false,
        }))
    }

    /// The byte-fetch half of the tenant-aware funnel: the ts-range pre-check,
    /// then the object's bytes from the read cache or a whole-object GET, with
    /// the `page_fetch` span and the accounting both entry points share.
    /// `Ok(None)` means the catalog summary proved the object irrelevant and no
    /// GET was issued.
    async fn tenant_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<Option<Bytes>, LogFetchError> {
        self.tenant_bytes_with_footer(seg_ref, tenant_hash, query, columns, None, accounting)
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
    #[allow(clippy::too_many_arguments)]
    async fn tenant_bytes_with_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        footer: Option<&footer::LogFooter>,
        accounting: &QueryAccounting,
    ) -> Result<Option<Bytes>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }

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
            let (bytes, stats) = async {
                self.block_range
                    .fetch_object_with_footer(
                        seg_ref,
                        tenant_hash,
                        query.ts_min_ns,
                        query.ts_max_ns,
                        &query.prune,
                        columns,
                        footer,
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
            // covered by the derived probe window on this scan-phase read,
            // reported beside the request/byte counts.
            fetch_span.record("probe_misses", stats.probe_misses);
            return Ok(Some(bytes));
        }

        Ok(Some(
            self.whole_object_bytes(seg_ref, tenant_hash, accounting)
                .await?,
        ))
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
    async fn whole_object_bytes(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<Bytes, LogFetchError> {
        let key = &seg_ref.data_object_key;

        // Same two phases as `fetch_accounted`, spanned on the path production
        // log/alerts/audit traffic actually takes (ADR-0044 decision 5): the
        // whole-object GET (`page_fetch`) here, then the STREAM_DIR resolve +
        // `RlogReader` prune and decode (`decode`) in whichever of the two
        // callers this feeds. Duplicated rather than shared with
        // `fetch_accounted` because the byte-fetch differs -- this one is
        // cache-aware and may serve a hit with no store GET at all. The
        // recorded `s3_requests`/`s3_bytes` reflect this call's own store GETs:
        // one on the uncached or cache-miss path, zero on a cache hit (the
        // served bytes are cache, not S3).
        let fetch_span = tracing::debug_span!(
            "page_fetch",
            signal = "logs",
            s3_requests = tracing::field::Empty,
            s3_bytes = tracing::field::Empty,
        );

        let Some(cache) = &self.cache else {
            let got = async {
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
            fetch_span.record("s3_requests", 1u64);
            fetch_span.record("s3_bytes", got.data.len() as u64);
            return Ok(got.data);
        };

        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, 0, seg_ref.object_size);
        // One read-through call, accounted from the returned [`Source`]: a
        // single call avoids the peek-then-`get_or_fetch` double-count on the
        // tiered tier (see [`ReadCache::get_or_fetch`]).
        let (bytes, source) = async {
            cache
                .get_or_fetch(cache_key, || async move {
                    let got = self.store.get(key, GetRange::Full).await?;
                    accounting.record_s3_request(AccountedOp::Get);
                    accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
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
        match source {
            Source::Cache => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                // Served from cache: no S3 GET on this call.
                fetch_span.record("s3_requests", 0u64);
                fetch_span.record("s3_bytes", 0u64);
            }
            // A miss issues one store GET for the resulting bytes. A
            // single-flight follower that rode another caller's GET is the
            // rare exception; recording one GET here still bounds this call's
            // own attribution and never under-counts the query total, which is
            // what the span is for.
            Source::Upstream => {
                accounting.record_cache_miss();
                fetch_span.record("s3_requests", 1u64);
                fetch_span.record("s3_bytes", bytes.len() as u64);
            }
        }
        Ok(bytes)
    }

    /// Runs [`scan_bytes`](Self::scan_bytes) inside the log path's `decode`
    /// span, recording the reader's block-scan counts on it afterward.
    ///
    /// # Why this span's field set diverges from the metric path's `decode`
    ///
    /// The metric path's `decode` span (`crate::fetcher`) carries `page_kind`,
    /// `series_count`, and `decompressed_bytes`. This one carries `signal =
    /// "logs"` plus `blocks_scanned`/`blocks_total`, and no `decompressed_bytes`
    /// (documented in docs/guides/tracing.md). No decompressed-byte
    /// count is cheaply available here: [`ScanStats`] carries block counts, not
    /// bytes, and decompression happens per block inside
    /// [`RlogReader::scan_pruned`] (`read_block`) where the total is never
    /// summed. Surfacing one would need a new `ScanStats` field and a structural
    /// change to `ravel-logseg`, out of scope for that fix.
    ///
    /// `blocks_scanned`/`blocks_total` are instead a real, already-computed
    /// pruning-effectiveness signal -- how much of the object's block index the
    /// scan actually had to touch after skip-index, POSTINGS, and bloom pruning
    /// -- analogous to the metric path's `catalog_resolve` `segments_pruned`,
    /// which is likewise a pruning count rather than a byte count. Every phase
    /// span in the codebase carries at least one count field; before this the
    /// logs `decode` span carried none.
    fn decode_spanned(
        &self,
        key: &str,
        bytes: &Bytes,
        query: &LogQuery,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let span = decode_span();
        let out = span.in_scope(|| self.scan_bytes(key, bytes, query))?;
        if let Some(output) = &out {
            span.record("blocks_scanned", output.stats.blocks_scanned);
            span.record("blocks_total", output.stats.blocks_total);
        }
        Ok(out)
    }

    /// Shared tail of both fetch entry points: open the pruned scan and drain
    /// every block of it. This is [`LogSegmentScan`] collected eagerly, so the
    /// two paths cannot drift: same predicate, same prune channel, same
    /// per-record erasure exclusion, same order.
    fn scan_bytes(
        &self,
        key: &str,
        bytes: &Bytes,
        query: &LogQuery,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let mut scan = self.open_scan(key, bytes, query, &ColumnSelection::all())?;
        let mut records = Vec::new();
        loop {
            let block = scan.next_block(bytes).map_err(|s| corrupt(key, s))?;
            let Some(mut rows) = block else { break };
            // Selective-erasure exclusion (ADR-0064 decision 2): drop every
            // row a pending erasure predicate matches. Applied here, on the
            // decoded records, so it excludes rows identically whether `bytes`
            // came from the store or from a cache hit -- the whole point of
            // filtering after fetch and after cache. A no-op when
            // `query.erasure` is empty.
            crate::erasure::retain_log_records(&mut rows, &query.erasure);
            records.extend(rows);
        }
        Ok(Some(LogFetchOutput {
            records,
            stats: scan.stats(),
        }))
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
        bytes: &Bytes,
        query: &LogQuery,
        columns: &ColumnSelection,
    ) -> Result<BlockScan, LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query)?;
        let reader = RlogReader::new(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
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
    /// [`plan_segment`](Self::plan_segment) count.
    fn open_scan_subset(
        &self,
        key: &str,
        bytes: &Bytes,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
    ) -> Result<BlockScan, LogFetchError> {
        let pred = self.combined_predicate(key, bytes, query)?;
        let reader = RlogReader::new(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        reader
            .scan_blocks_subset(&pred, &query.prune, columns, indices)
            .map_err(|source| corrupt(key, source))
    }

    /// The combined exact predicate (`ts range AND resolved streams AND
    /// content`) both scan-opening paths hand to the reader. Stream-attribute
    /// equalities are resolved against STREAM_DIR here (over-approximating, see
    /// [`matching_streams`](Self::matching_streams)); the prune-only channel is
    /// passed separately by the caller.
    fn combined_predicate(
        &self,
        key: &str,
        bytes: &Bytes,
        query: &LogQuery,
    ) -> Result<Predicate, LogFetchError> {
        let stream_ids = if query.stream_attrs.is_empty() {
            None
        } else {
            Some(
                self.matching_streams(bytes, &query.stream_attrs)
                    .map_err(|source| corrupt(key, source))?,
            )
        };

        let mut arms = Vec::with_capacity(2 + query.content.len());
        arms.push(Predicate::TsRange {
            min_ns: query.ts_min_ns,
            max_ns: query.ts_max_ns,
        });
        if let Some(ids) = stream_ids {
            // An empty set is intentional: it means no stream in this object
            // satisfies the attribute filter, and the reader short-circuits an
            // empty StreamIn to zero records.
            arms.push(Predicate::StreamIn(ids));
        }
        arms.extend(query.content.iter().cloned());
        Ok(Predicate::And(arms))
    }

    /// Decodes the STREAM_DIR section of an object from its own public section
    /// descriptor, using the crate's public whole-section reader.
    /// This does not go through [`RlogReader`], which decodes STREAM_DIR
    /// internally but exposes no accessor for it.
    fn decode_stream_dir(&self, bytes: &[u8]) -> Result<StreamDir, LogSegError> {
        let footer = footer::open(bytes)?;
        let desc = footer
            .section(kind::STREAM_DIR)
            .ok_or_else(|| LogSegError::Corrupted("missing STREAM_DIR section".into()))?;
        let raw = read_section(bytes, desc, &self.cfg)?;
        StreamDir::decode(&raw, MAX_STREAMS)
    }
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
    /// Candidate blocks served from the read cache with no store round trip.
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

/// Assembles an object-sized buffer from separately fetched regions so the
/// existing whole-object reader ([`RlogReader`]/[`BlockScan`]) can decode from
/// it unchanged: block extents are absolute offsets into the object, so the
/// buffer must be indexable at those offsets. Only the fetched directory
/// sections and candidate blocks are populated; the gap bytes between them (the
/// pruned blocks) stay zero and are never read -- `BlockScan` restricted to the
/// candidate blocks only slices the extents that were placed (ADR-0107: gap
/// bytes "never interpreted, never verified").
struct ObjectAssembler {
    buf: Vec<u8>,
    placed: Vec<(u64, u64)>,
}

impl ObjectAssembler {
    fn new(total_size: usize) -> Self {
        ObjectAssembler {
            buf: vec![0u8; total_size],
            placed: Vec::new(),
        }
    }

    fn covers(&self, start: u64, end: u64) -> bool {
        self.placed.iter().any(|(s, e)| *s <= start && end <= *e)
    }

    fn slice(&self, start: u64, len: u64) -> Option<&[u8]> {
        let s = usize::try_from(start).ok()?;
        let e = s.checked_add(usize::try_from(len).ok()?)?;
        self.buf.get(s..e)
    }

    fn place(&mut self, key: &str, start: u64, bytes: &[u8]) -> Result<(), LogFetchError> {
        let s = usize::try_from(start).map_err(|_| corrupt_range(key))?;
        let e = s
            .checked_add(bytes.len())
            .ok_or_else(|| corrupt_range(key))?;
        let slot = self.buf.get_mut(s..e).ok_or_else(|| corrupt_range(key))?;
        slot.copy_from_slice(bytes);
        self.placed
            .push((start, start.saturating_add(bytes.len() as u64)));
        Ok(())
    }

    fn into_bytes(self) -> Bytes {
        Bytes::from(self.buf)
    }
}

/// `[offset, offset + len)` sliced out of whichever already-fetched region
/// wholly contains it, or `None` when no region does. The regions are the
/// `(start, bytes)` pairs [`BlockRangeFetcher::probe_footer`] left resident;
/// this is [`ObjectAssembler::covers`] + [`ObjectAssembler::slice`] for a read
/// that never builds an object-sized buffer.
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
/// The result is an object-sized [`Bytes`] with only the directory sections and
/// candidate blocks populated, ready to hand to [`RlogReader`]/[`BlockScan`]
/// unchanged: the reader re-prunes and decodes exactly the survivor blocks,
/// which are a subset of the fetched candidate set, so decode never touches an
/// unfetched gap. Cache admission is per block, not per coalesced GET (ADR-0107
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
    /// Bounds in-flight byte-range GETs. Its own instance, never shared with
    /// `SegmentFetcher`'s RSEG semaphore (ADR-0107 decision 1).
    get_semaphore: Arc<Semaphore>,
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
            get_semaphore: Arc::new(Semaphore::new(DEFAULT_LOG_MAX_CONCURRENT_GETS)),
        }
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
        self.get_semaphore = Arc::new(Semaphore::new(n.max(1)));
        self
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
    async fn store_get(
        &self,
        key: &str,
        range: GetRange,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, LogFetchError> {
        let _permit = self
            .get_semaphore
            .acquire()
            .await
            .map_err(|_| LogFetchError::Store {
                key: key.to_string(),
                source: StoreError::Transient("fetch concurrency semaphore closed".to_string()),
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
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<GetOutcome, LogFetchError> {
        let got = self.store_get(key, range, accounting).await?;
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
    /// The returned flag is true when this call crossed the network, so callers
    /// count only real store GETs. A single-flight follower that rode another
    /// caller's in-flight GET reports true as well: the same attribution
    /// convention `tenant_bytes` documents, which bounds this call's own cost
    /// and never under-counts the query total.
    #[allow(clippy::too_many_arguments)]
    async fn cached_extent(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        start: u64,
        len: u64,
        range: GetRange,
        pin: &EtagPin,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, bool), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let Some(cache) = &self.cache else {
            let got = self.store_get_pinned(key, range, pin, accounting).await?;
            check_extent_len(key, got.data.len(), len)?;
            return Ok((got.data, true));
        };
        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, start, len);
        // One read-through call, accounted from the returned [`Source`]: a single
        // call avoids the peek-then-`get_or_fetch` double-count on the tiered
        // tier (see [`ReadCache::get_or_fetch`]). The returned flag is `live`
        // (this call crossed the network), which is [`Source::Upstream`].
        let (bytes, source) = cache
            .get_or_fetch(cache_key, || async move {
                let got = self
                    .store_get_pinned(key, range, pin, accounting)
                    .await
                    .map_err(to_cache_error)?;
                check_extent_len(key, got.data.len(), len).map_err(to_cache_error)?;
                Ok(got.data)
            })
            .await
            .map_err(|err| from_cache_error(key, err))?;
        match source {
            Source::Cache => {
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                Ok((bytes, false))
            }
            Source::Upstream => {
                accounting.record_cache_miss();
                Ok((bytes, true))
            }
        }
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
    pub async fn fetch_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        accounting: &QueryAccounting,
    ) -> Result<(footer::LogFooter, BlockRangeStats), LogFetchError> {
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();
        let (footer, _resident) = self
            .probe_footer(seg_ref, tenant_hash, &pin, accounting, &mut stats)
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
    async fn probe_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
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
        let (footer, mut resident) = self
            .probe_footer(seg_ref, tenant_hash, &pin, accounting, &mut stats)
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
        decode_section(&stored, desc, &self.cfg).map_err(|source| corrupt(key, source))
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
        let (footer, mut resident) = self
            .probe_footer(seg_ref, tenant_hash, &pin, accounting, &mut stats)
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
                &pin,
                accounting,
                &mut stats,
            )
            .await?;
        let field_dir =
            FieldDir::decode(&field_raw, MAX_FIELDS).map_err(|source| corrupt(key, source))?;

        Ok((footer, skip, field_dir, stats))
    }

    /// Fetch one segment's object as an assembled, decode-ready buffer, reading
    /// only the blocks skip-index pruning (over `[ts_min_ns, ts_max_ns]`) proved
    /// relevant. Returns the buffer plus the [`BlockRangeStats`] for the fetch.
    ///
    /// The caller has already decided the object is ts-relevant. `ts_min_ns`/
    /// `ts_max_ns` are the inclusive query bounds used for skip-index candidate
    /// selection here; stream/POSTINGS/bloom/numeric pruning still runs at decode
    /// inside the reader over the assembled buffer (it can only narrow the
    /// candidate set further, so every survivor block is fetched).
    pub async fn fetch_object(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            &[],
            &ColumnSelection::all(),
            None,
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
    ) -> Result<(Bytes, BlockRangeStats), LogFetchError> {
        self.fetch_object_with_footer(
            seg_ref,
            tenant_hash,
            ts_min_ns,
            ts_max_ns,
            &[],
            columns,
            None,
            accounting,
        )
        .await
    }

    /// [`fetch_object`](Self::fetch_object), optionally reusing a
    /// [`footer::LogFooter`] a prior plan phase already read for this exact
    /// (immutable) object (#693 part 3, deliverable 2).
    ///
    /// When `plan_footer` is `Some`, the etag-establishing suffix probe is
    /// skipped: the carried footer already gives every section's offset and
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
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_object_with_footer(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        ts_min_ns: i64,
        ts_max_ns: i64,
        prune: &[Predicate],
        columns: &ColumnSelection,
        plan_footer: Option<&footer::LogFooter>,
        accounting: &QueryAccounting,
    ) -> Result<(Bytes, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut stats = BlockRangeStats::default();
        let pin = EtagPin::default();

        // An object whose commit record carries no size cannot be range-planned
        // at all: every range in this protocol, starting with the probe's own
        // cache key, is derived from that size. Read it whole, uncached (the
        // cache key would have to claim a length too).
        if seg_ref.object_size == 0 {
            let got = self.store_get(key, GetRange::Full, accounting).await?;
            stats.probe_gets = 1;
            stats.whole_object = true;
            stats.block_bytes_fetched = got.data.len() as u64;
            return Ok((got.data, stats));
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
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    0,
                    seg_ref.object_size,
                    GetRange::Full,
                    &pin,
                    accounting,
                )
                .await?;
            if live {
                stats.probe_gets = 1;
                stats.block_bytes_fetched = bytes.len() as u64;
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
        let total = usize::try_from(total_size).map_err(|_| corrupt_range(key))?;
        let mut asm = ObjectAssembler::new(total);

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
            Some(f) => f.clone(),
            None => {
                let suffix = self.effective_suffix_len(total_size);
                let probe_start = total_size - suffix;
                let (probe_bytes, probe_live) = self
                    .cached_extent(
                        seg_ref,
                        tenant_hash,
                        probe_start,
                        suffix,
                        GetRange::Suffix(suffix),
                        &pin,
                        accounting,
                    )
                    .await?;
                if probe_live {
                    stats.probe_gets = 1;
                }
                asm.place(key, probe_start, &probe_bytes)?;
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
                            .cached_extent(
                                seg_ref,
                                tenant_hash,
                                offset,
                                len,
                                GetRange::Range(offset, offset + len),
                                &pin,
                                accounting,
                            )
                            .await?;
                        if live {
                            stats.probe_gets += 1;
                        }
                        asm.place(key, offset, &bytes)?;
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
                    asm,
                    &pin,
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
        if !probe_window_covers(skip_desc, total_size, self.effective_suffix_len(total_size)) {
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
            &pin,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;

        // Decode the skip index (now resident) and resolve the candidate blocks.
        let skip_start = usize::try_from(skip_desc.offset).map_err(|_| corrupt_range(key))?;
        let skip_end = skip_start
            .checked_add(usize::try_from(skip_desc.len).map_err(|_| corrupt_range(key))?)
            .ok_or_else(|| corrupt_range(key))?;
        let skip_stored = asm
            .buf
            .get(skip_start..skip_end)
            .ok_or_else(|| corrupt_range(key))?;
        let skip_raw = decode_section(skip_stored, skip_desc, &self.cfg)
            .map_err(|source| corrupt(key, source))?;
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
            // Keyed and single-flighted like every other GET here, on the same
            // `(0, object_size)` key the whole-object funnel uses: without that,
            // N partitions all crossing over would issue N whole-object GETs,
            // which is the amplification this path exists to avoid.
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    0,
                    total_size,
                    GetRange::Full,
                    &pin,
                    accounting,
                )
                .await?;
            if live {
                stats.block_range_gets = 1;
                stats.block_bytes_fetched = bytes.len() as u64;
            }
            stats.whole_object = true;
            // Still admit per-block cache entries so a later partition's fetch of
            // a subset composes with this one (ADR-0107 decision 3).
            self.admit_blocks_from_whole(seg_ref, tenant_hash, &bytes, &extents);
            return Ok((bytes, stats));
        }

        // The suffix probe normally places the object's tail -- the footer and
        // trailer bytes `RlogReader` re-reads to open the assembled buffer, which
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
                let (bytes, live) = self
                    .cached_extent(
                        seg_ref,
                        tenant_hash,
                        tail_start,
                        total_size - tail_start,
                        GetRange::Range(tail_start, total_size),
                        &pin,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.metadata_gets += 1;
                }
                asm.place(key, tail_start, &bytes)?;
            }
        }

        // The two front sections (STREAM_DIR/FIELD_DIR): one coalesced GET over
        // their adjacent span (deliverable 4), skipped entirely when an earlier
        // FIELD_DIR resolution already brought them.
        self.place_front_sections(
            seg_ref,
            tenant_hash,
            &footer,
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
        mut asm: ObjectAssembler,
        pin: &EtagPin,
        accounting: &QueryAccounting,
        mut stats: BlockRangeStats,
    ) -> Result<(Bytes, BlockRangeStats), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let total_size = seg_ref.object_size;
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
        let suffix = self.effective_suffix_len(total_size);
        for desc in [&skip_desc, &page_desc] {
            if !probe_window_covers(desc, total_size, suffix) {
                stats.probe_misses += 1;
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
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    probe_start,
                    suffix,
                    GetRange::Range(probe_start, total_size),
                    pin,
                    accounting,
                )
                .await?;
            if live {
                stats.probe_gets += 1;
            }
            asm.place(key, probe_start, &bytes)?;
        }

        // SKIP_IDX and PAGE_DIR first, and nothing the candidate set does not
        // need: the coverage crossover below can still decide the whole object
        // is cheaper, and every other section fetched before that decision would
        // then be wasted (the same ordering rule the version-3 path follows).
        // The two are adjacent in the object -- the writer emits PAGE_DIR
        // immediately after SKIP_IDX -- so a probe that missed both costs one
        // coalesced GET rather than two.
        self.place_sections_coalesced(
            seg_ref,
            tenant_hash,
            &[skip_desc, page_desc],
            pin,
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;
        let skip_raw = self.placed_section_raw(key, &asm, &skip_desc)?;
        let skip =
            SkipIndex::decode(&skip_raw, MAX_BLOCKS).map_err(|source| corrupt(key, source))?;
        let page_raw = self.placed_section_raw(key, &asm, &page_desc)?;
        let page_dir = PageDir::decode(&page_raw).map_err(|source| corrupt(key, source))?;
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
            let field_dir = self
                .place_and_decode_field_dir(
                    seg_ref,
                    tenant_hash,
                    footer,
                    pin,
                    &mut asm,
                    accounting,
                    &mut stats,
                )
                .await?;
            let numeric = if wants_numeric {
                let refs: Vec<&Predicate> = prune.iter().collect();
                field_dir.numeric_range_arms(&refs)
            } else {
                Vec::new()
            };
            // The same resolution `RlogReader::scan_blocks` runs on the same
            // FIELD_DIR, so the pages fetched here are exactly the pages the
            // decode addresses.
            (numeric, columns.resolve(&field_dir))
        } else {
            (Vec::new(), None)
        };

        let candidates = skip.candidate_blocks(ts_min_ns, ts_max_ns, None, &numeric);
        stats.candidate_blocks = candidates.len() as u64;
        let wanted = projected_page_extents(
            key,
            &page_dir,
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
        let wanted_bytes: u64 = wanted.iter().map(|e| e.len).sum();
        let coverage = wanted_bytes as f64 / blocks_desc.len.max(1) as f64;
        if coverage >= self.coverage_threshold {
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    0,
                    total_size,
                    GetRange::Full,
                    pin,
                    accounting,
                )
                .await?;
            if live {
                stats.block_range_gets = 1;
                stats.block_bytes_fetched = bytes.len() as u64;
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
        // trailer bytes `RlogReader` re-reads to open the assembled buffer,
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
                let (bytes, live) = self
                    .cached_extent(
                        seg_ref,
                        tenant_hash,
                        tail_start,
                        total_size - tail_start,
                        GetRange::Range(tail_start, total_size),
                        pin,
                        accounting,
                    )
                    .await?;
                if live {
                    stats.metadata_gets += 1;
                }
                asm.place(key, tail_start, &bytes)?;
            }
        }

        // The two front sections (STREAM_DIR/FIELD_DIR): one coalesced GET over
        // their adjacent span (deliverable 4). On the narrow-projection path
        // `place_and_decode_field_dir` already brought both, so this is a no-op
        // there; on an all-columns v4 read it is the read's one front GET.
        self.place_front_sections(
            seg_ref,
            tenant_hash,
            footer,
            pin,
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
            &mut asm,
            accounting,
            &mut stats,
        )
        .await?;
        Ok((asm.into_bytes(), stats))
    }

    /// Fetch the coalesced page ranges into `asm`, every run concurrently,
    /// through the same [`cached_extent`](Self::cached_extent) path every other
    /// GET here takes: concurrent partitions striping one segment resolve the
    /// identical candidate set and the identical projection, so they produce the
    /// identical runs and collapse onto one real request each rather than one
    /// per partition (ADR-0102 decision 1's premise), and the etag pin holds
    /// across the sequence.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_chunk_ranges(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        pin: &EtagPin,
        wanted: &[ByteExtent],
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let runs: Vec<ByteExtent> = coalesce_byte_extents(wanted, self.effective_coalesce_gap())
            .into_iter()
            // A run the probe already brought costs nothing: its bytes are in
            // `asm` at the right offsets already.
            .filter(|r| !asm.covers(r.abs_start, r.abs_end()))
            .collect();
        let outcomes = futures::future::join_all(runs.iter().map(|run| async move {
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    run.abs_start,
                    run.len,
                    GetRange::Range(run.abs_start, run.abs_end()),
                    pin,
                    accounting,
                )
                .await?;
            Ok::<_, LogFetchError>((run.abs_start, bytes, live))
        }))
        .await;
        for outcome in outcomes {
            let (start, bytes, live) = outcome?;
            if live {
                stats.block_range_gets += 1;
                stats.block_bytes_fetched =
                    stats.block_bytes_fetched.saturating_add(bytes.len() as u64);
            } else {
                stats.block_cache_hits += 1;
            }
            asm.place(key, start, &bytes)?;
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
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let missing: Vec<ByteExtent> = sections
            .iter()
            .filter(|s| !asm.covers(s.offset, s.offset + s.len))
            .map(|s| ByteExtent {
                abs_start: s.offset,
                len: s.len,
            })
            .collect();
        for run in coalesce_byte_extents(&missing, self.effective_coalesce_gap()) {
            let (bytes, live) = self
                .cached_extent(
                    seg_ref,
                    tenant_hash,
                    run.abs_start,
                    run.len,
                    GetRange::Range(run.abs_start, run.abs_end()),
                    pin,
                    accounting,
                )
                .await?;
            if live {
                stats.metadata_gets += 1;
            }
            asm.place(key, run.abs_start, &bytes)?;
        }
        Ok(())
    }

    /// Decode one whole-compressed section out of the assembled buffer, where an
    /// earlier `place_*` call already put its stored bytes.
    fn placed_section_raw(
        &self,
        key: &str,
        asm: &ObjectAssembler,
        desc: &SectionDesc,
    ) -> Result<Vec<u8>, LogFetchError> {
        let stored = asm
            .slice(desc.offset, desc.len)
            .ok_or_else(|| corrupt_range(key))?;
        decode_section(stored, desc, &self.cfg).map_err(|source| corrupt(key, source))
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
    /// costs nothing, and the span then covers only whichever remains. Counts one
    /// [`BlockRangeStats::metadata_gets`] when it fetches, never a `probe_miss`
    /// (the front is unreachable by any probe, so counting it there would put a
    /// floor under that metric -- see the `probe_misses` field doc).
    #[allow(clippy::too_many_arguments)]
    async fn place_front_sections(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &footer::LogFooter,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let mut start = u64::MAX;
        let mut end = 0u64;
        for k in [kind::STREAM_DIR, kind::FIELD_DIR] {
            let Some(desc) = footer.section(k) else {
                continue;
            };
            if asm.covers(desc.offset, desc.offset + desc.len) {
                continue;
            }
            start = start.min(desc.offset);
            end = end.max(desc.offset + desc.len);
        }
        if start >= end {
            return Ok(());
        }
        let (bytes, live) = self
            .cached_extent(
                seg_ref,
                tenant_hash,
                start,
                end - start,
                GetRange::Range(start, end),
                pin,
                accounting,
            )
            .await?;
        if live {
            stats.metadata_gets += 1;
        }
        asm.place(key, start, &bytes)
    }

    /// Place FIELD_DIR into `asm` (via [`place_section`](Self::place_section), on
    /// its own per-section cache key, so a subset scan reuses the FIELD_DIR the
    /// plan phase already cached under that key) and decode it, so the caller can
    /// resolve a query's prune-only NumRange arms and its projection to this
    /// object's own column ids before the candidate set is chosen.
    ///
    /// STREAM_DIR is deliberately NOT coalesced in here: this runs BEFORE the
    /// coverage crossover, and a query that then crosses over to a whole-object
    /// read never needs STREAM_DIR, so bringing it eagerly would move directory
    /// bytes the crossover discards. STREAM_DIR is instead brought by
    /// [`place_front_sections`](Self::place_front_sections) after the crossover,
    /// where -- with FIELD_DIR already resident -- it is the only remaining front
    /// section and coalesces with nothing; on an all-columns read that skips this
    /// method both front sections arrive cold there and coalesce into one GET.
    /// FIELD_DIR is compressed as a whole section (docs/log-segment-format.md),
    /// the same shape SKIP_IDX is decoded with just above.
    #[allow(clippy::too_many_arguments)]
    async fn place_and_decode_field_dir(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        footer: &footer::LogFooter,
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<FieldDir, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let desc = *footer
            .section(kind::FIELD_DIR)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing FIELD_DIR".into())))?;
        self.place_section(seg_ref, tenant_hash, &desc, pin, asm, accounting, stats)
            .await?;
        let start = usize::try_from(desc.offset).map_err(|_| corrupt_range(key))?;
        let end = start
            .checked_add(usize::try_from(desc.len).map_err(|_| corrupt_range(key))?)
            .ok_or_else(|| corrupt_range(key))?;
        let stored = asm.buf.get(start..end).ok_or_else(|| corrupt_range(key))?;
        let raw =
            decode_section(stored, &desc, &self.cfg).map_err(|source| corrupt(key, source))?;
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
        pin: &EtagPin,
        asm: &mut ObjectAssembler,
        accounting: &QueryAccounting,
        stats: &mut BlockRangeStats,
    ) -> Result<(), LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let (start, end) = (section.offset, section.offset + section.len);
        if asm.covers(start, end) {
            return Ok(());
        }
        let (bytes, live) = self
            .cached_extent(
                seg_ref,
                tenant_hash,
                section.offset,
                section.len,
                GetRange::Range(start, end),
                pin,
                accounting,
            )
            .await?;
        if live {
            stats.metadata_gets += 1;
        }
        asm.place(key, section.offset, &bytes)
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
                let block = asm
                    .slice(ext.abs_start, ext.len)
                    .ok_or_else(|| corrupt_range(key))?;
                verify_block_crc(key, block, ext)?;
                if let Some(cache) = &self.cache {
                    let cache_key =
                        CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
                    cache.insert(cache_key, Bytes::copy_from_slice(block));
                }
                continue;
            }
            let cache_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            if let Some(cache) = &self.cache
                && let Some(bytes) = cache.get(&cache_key)
            {
                // Corrupt-hit gate (ADR-0046 §4 / ADR-0107 decision 3): a cached
                // block is re-verified against its stored crc before use, exactly
                // as a live fetch of that block is, and fails closed on mismatch.
                verify_block_crc(key, &bytes, ext)?;
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                stats.block_cache_hits += 1;
                asm.place(key, ext.abs_start, &bytes)?;
                continue;
            }
            if self.cache.is_some() {
                accounting.record_cache_miss();
            }
            missing.push(*ext);
        }

        // Every coalesced run concurrently, not one await at a time (mirrors
        // `crate::fetcher::SegmentFetcher::ensure_ranges`' `join_all`). Awaiting
        // the runs in series made `get_semaphore` inert: a sequential loop never
        // has more than one GET in flight to bound.
        let runs = coalesce_extents(&missing, self.effective_coalesce_gap());
        let outcomes = futures::future::join_all(runs.iter().map(|run| {
            let blocks: Vec<BlockExtent> = missing
                .iter()
                .copied()
                .filter(|e| e.abs_start >= run.abs_start && e.abs_end() <= run.abs_end())
                .collect();
            self.fetch_run(seg_ref, tenant_hash, pin, *run, blocks, accounting)
        }))
        .await;
        for outcome in outcomes {
            let run = outcome?;
            stats.block_range_gets += run.gets;
            stats.block_cache_hits += run.cache_hits;
            stats.block_bytes_fetched = stats.block_bytes_fetched.saturating_add(run.bytes);
            for (start, bytes) in run.blocks {
                asm.place(key, start, &bytes)?;
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
    async fn fetch_run(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        pin: &EtagPin,
        run: BlockExtent,
        blocks: Vec<BlockExtent>,
        accounting: &QueryAccounting,
    ) -> Result<RunOutcome, LogFetchError> {
        let key = seg_ref.data_object_key.as_str();
        let range = GetRange::Range(run.abs_start, run.abs_end());
        let Some(cache) = &self.cache else {
            let got = self.store_get_pinned(key, range, pin, accounting).await?;
            let blocks = split_run(key, run.abs_start, &got.data, &blocks)?;
            return Ok(RunOutcome {
                blocks,
                gets: 1,
                bytes: got.data.len() as u64,
                cache_hits: 0,
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
        // Set by our own closure, so `Some` after the await means this call led
        // the fetch and already holds every block of the run; `None` means it
        // followed another caller's in-flight GET.
        let led: std::sync::OnceLock<(Vec<(u64, Bytes)>, u64)> = std::sync::OnceLock::new();
        // `fetch_peeked`, not `get_or_fetch`: fetch_blocks already peeked every
        // block of this run with `cache.get` (the one accounted miss), so a
        // second read-through here would re-peek and double-count the miss on
        // the tiered tier. The RAM tier's `fetch_peeked` is its miss-only
        // `get_or_fetch` and keeps single-flight (concurrent partitions collapse
        // onto one leader); the tiered tier runs the fetch and `insert`s with no
        // second miss. Either way `led` is set inside the closure iff THIS call
        // ran it, so `led.get().is_some()` still distinguishes leader from
        // follower.
        let lead_bytes = cache
            .fetch_peeked(lead_key, || async {
                let got = self
                    .store_get_pinned(key, range, pin, accounting)
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
                    cache.insert(
                        CacheKey::new(
                            tenant_hash.0,
                            seg_ref.content_hash,
                            *start,
                            bytes.len() as u64,
                        ),
                        bytes.clone(),
                    );
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
                cache_hits: 0,
            });
        }
        // Follower: the lead block is the leader's own verified bytes, and the
        // leader admitted the rest of the run before it returned.
        let mut out = Vec::with_capacity(blocks.len());
        out.push((lead.abs_start, lead_bytes));
        let mut outcome = RunOutcome::default();
        for ext in blocks.iter().skip(1) {
            let block_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            if let Some(bytes) = cache.get(&block_key) {
                verify_block_crc(key, &bytes, ext)?;
                accounting.record_cache_hit();
                accounting.add_cache_bytes(bytes.len() as u64);
                outcome.cache_hits += 1;
                out.push((ext.abs_start, bytes));
                continue;
            }
            accounting.record_cache_miss();
            // `fetch_peeked` for the same reason as the lead above: this block
            // was just peeked with `cache.get`, so the deferred fetch must not
            // re-count the miss on the tiered tier.
            let bytes = cache
                .fetch_peeked(block_key, || async {
                    let got = self
                        .store_get_pinned(
                            key,
                            GetRange::Range(ext.abs_start, ext.abs_end()),
                            pin,
                            accounting,
                        )
                        .await
                        .map_err(to_cache_error)?;
                    verify_block_crc(key, &got.data, ext).map_err(to_cache_error)?;
                    Ok(got.data)
                })
                .await
                .map_err(|err| from_cache_error(key, err))?;
            outcome.gets += 1;
            outcome.bytes = outcome.bytes.saturating_add(bytes.len() as u64);
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
    fn admit_blocks_from_whole(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        object: &Bytes,
        extents: &[BlockExtent],
    ) {
        let Some(cache) = &self.cache else {
            return;
        };
        for ext in extents {
            let (Ok(start), Ok(len)) = (usize::try_from(ext.abs_start), usize::try_from(ext.len))
            else {
                continue;
            };
            let Some(end) = start.checked_add(len) else {
                continue;
            };
            let Some(block) = object.get(start..end) else {
                continue;
            };
            if crc32c::crc32c(block) != ext.crc32c {
                continue;
            }
            let cache_key =
                CacheKey::new(tenant_hash.0, seg_ref.content_hash, ext.abs_start, ext.len);
            cache.insert(cache_key, Bytes::copy_from_slice(block));
        }
    }
}

/// One coalesced run's fetched blocks (`(abs_start, bytes)`, crc-verified) plus
/// the store cost this caller paid for them: `gets` real range GETs moving
/// `bytes` stored bytes, and `cache_hits` blocks served with no round trip.
///
/// A single-flight follower that rode another caller's GET reports zero gets and
/// zero bytes here, unlike [`BlockRangeFetcher::cached_extent`] and the
/// whole-object funnels, which attribute one request to a follower because they
/// cannot tell one from a leader. This path can: the leader is whichever call
/// ran the closure. Reporting what actually crossed the network is strictly
/// better information, and the block-range GET count is the figure ADR-0107's
/// acceptance test is written against.
#[derive(Default)]
struct RunOutcome {
    blocks: Vec<(u64, Bytes)>,
    gets: u64,
    bytes: u64,
    cache_hits: u64,
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
    )
}

fn corrupt(key: &str, source: LogSegError) -> LogFetchError {
    LogFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
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
    /// and only the tail probe crossed the wire -- never a block-range fetch
    /// sized to the object.
    ///
    /// Non-vacuity: deleting the fast-path branch in `plan_segment` (the
    /// `if seg_ref.object_size > 0 && query.is_block_predicate_free() ...`
    /// block that `return`s `plan_segment_fast(...)`) routes this same query
    /// through the block-range slow path, whose all-block candidate set trips
    /// the coverage crossover into a whole-object GET, so `total_s3_bytes` jumps
    /// from `tail` to roughly `tail + object_size`. The `read == tail` assertion
    /// then fails. The `predicate_present_cases_take_the_slow_path` test below
    /// exercises exactly that slow path and shows the larger byte count directly.
    #[tokio::test]
    async fn fast_path_reads_only_footer_probe() {
        const N: usize = 6;
        let records: Vec<LogRecord> = (0..N as i64).map(record).collect();
        let bytes = build_object(&records);
        let total = bytes.len() as u64;
        let tail = tail_len(&bytes);
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
        let (count, stats, _footer) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");

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
            read, tail,
            "fast path reads only the footer probe ({tail} B), not the {total} B object"
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
        let seg = seg_ref(total, &records);
        let store = store_with_object(bytes).await;
        let f = fetcher(store, tail);
        let acc = QueryAccounting::new();

        // Exact span bounds: ts_min == min_event_ts_ns, ts_max == max_event_ts_ns.
        let query = LogQuery::new(seg.min_event_ts_ns, seg.max_event_ts_ns);
        let (count, _stats, _footer) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");
        assert_eq!(count, N);
        assert_eq!(acc.snapshot().total_s3_bytes(), tail, "fast path fired");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &q, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
        let (count, _, _) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan")
            .expect("relevant");
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
    }

    struct FieldVisitor<'a>(&'a mut Captured);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "s3_requests" => self.0.s3_requests = Some(value),
                "s3_bytes" => self.0.s3_bytes = Some(value),
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
    async fn skip_decidable_branch_opens_page_fetch_span() {
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
        let (count, _stats, footer) = f
            .plan_segment(&seg, TENANT, &query, &acc)
            .await
            .expect("plan_segment")
            .expect("relevant segment");
        assert_eq!(count, N, "wide NumRange bound excludes no block");
        assert!(
            footer.is_some(),
            "skip-decidable branch forwards the parsed footer like the fast path does"
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
