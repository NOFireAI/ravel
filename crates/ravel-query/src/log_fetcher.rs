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
//! RLOG object and every fixture here. Above it they read only the blocks
//! skip-index pruning proved relevant, through [`BlockRangeFetcher`]: a suffix
//! probe that pins the etag, the directory sections, and coalesced
//! candidate-block ranges, assembled into an object-sized buffer the unchanged
//! reader decodes from. Every GET of either shape is routed through ADR-0046's
//! read cache when one is wired, keyed by the extent it fetched, so concurrent
//! callers for the same extent collapse onto one request. The untenanted
//! [`LogSegmentFetcher::fetch`]/[`LogSegmentFetcher::fetch_accounted`] entry
//! points have no cache key and always read the whole object in one GET.

use std::sync::Arc;

use crate::erasure::ErasurePredicate;
use bytes::Bytes;
use ravel_cache::{Cache, CacheKey, SingleFlightError};
use ravel_catalog::SegmentRef;
use ravel_logseg::footer::{self, SectionDesc, kind};
use ravel_logseg::skip_index::SkipIndex;
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
    /// predicates. The resolver already surfaces them: it attaches
    /// every pending request to `Snapshot::pending_erasure` on each resolve.
    /// What is still missing is the last hop -- the `ravel-sql` scans that
    /// build this query (`logs_scan`, `alerts_scan`, `audit_scan`) do not yet
    /// map that field into this one, so on the SQL log surface this stays
    /// empty and log erasure exclusion is inert. The metric surface needs no
    /// such hop: `QueryEngine` reads `Snapshot::pending_erasure` directly at
    /// its own fetch funnels.
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
    /// when an exit reports exhaustion.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let stats = self.scan.stats();
        self.span.record("blocks_scanned", stats.blocks_scanned);
        self.span.record("blocks_total", stats.blocks_total);
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
    /// be wired into a method with no per-call tenant identity.
    cache: Option<Arc<Cache<crate::fetcher::CacheFetchError>>>,
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
    /// per block, into the block-range path (ADR-0107 decision 3).
    #[must_use]
    pub fn with_cache(mut self, cache: Arc<Cache<crate::fetcher::CacheFetchError>>) -> Self {
        self.cache = Some(cache.clone());
        self.block_range = self.block_range.with_cache(cache);
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
        accounting: &QueryAccounting,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
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
        accounting: &QueryAccounting,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
        let Some(bytes) = self
            .tenant_bytes(seg_ref, tenant_hash, query, accounting)
            .await?
        else {
            return Ok(None);
        };
        self.decode_spanned(&seg_ref.data_object_key, &bytes, query)
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
    pub async fn scan_accounted_with_tenant(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        let Some(bytes) = self
            .tenant_bytes(seg_ref, tenant_hash, query, accounting)
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
    /// scan_accounted_with_tenant) uses, so in production it is single-flight
    /// coalesced with the per-partition subset scans that follow rather than
    /// issuing an extra distinct GET. That holds on both read shapes: one
    /// whole-object key at or below
    /// [`with_block_range_threshold`](Self::with_block_range_threshold), and
    /// per-extent keys (probe, sections, blocks) above it, each of which the
    /// following subset scans hit rather than re-fetch (ADR-0107).
    ///
    /// [`scan_accounted_with_tenant`]: Self::scan_accounted_with_tenant
    pub async fn plan_segment(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        accounting: &QueryAccounting,
    ) -> Result<Option<(usize, ScanStats)>, LogFetchError> {
        let Some(bytes) = self
            .tenant_bytes(seg_ref, tenant_hash, query, accounting)
            .await?
        else {
            return Ok(None);
        };
        let key = &seg_ref.data_object_key;
        let span = decode_span();
        let scan = span.in_scope(|| self.open_scan(key, &bytes, query, &ColumnSelection::all()))?;
        Ok(Some((scan.remaining_blocks(), scan.stats())))
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
    /// [`scan_accounted_with_tenant`]: Self::scan_accounted_with_tenant
    pub async fn scan_accounted_with_tenant_subset(
        &self,
        seg_ref: &SegmentRef,
        tenant_hash: TenantHash,
        query: &LogQuery,
        columns: &ColumnSelection,
        indices: &[usize],
        accounting: &QueryAccounting,
    ) -> Result<Option<LogSegmentScan>, LogFetchError> {
        let Some(bytes) = self
            .tenant_bytes(seg_ref, tenant_hash, query, accounting)
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
        accounting: &QueryAccounting,
    ) -> Result<Option<Bytes>, LogFetchError> {
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }
        let key = &seg_ref.data_object_key;

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
            );
            let (bytes, stats) = async {
                self.block_range
                    .fetch_object(
                        seg_ref,
                        tenant_hash,
                        query.ts_min_ns,
                        query.ts_max_ns,
                        accounting,
                    )
                    .await
            }
            .instrument(fetch_span.clone())
            .await?;
            let requests = stats.probe_gets + stats.metadata_gets + stats.block_range_gets;
            fetch_span.record("s3_requests", requests);
            fetch_span.record("s3_bytes", stats.block_bytes_fetched);
            return Ok(Some(bytes));
        }

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
            return Ok(Some(got.data));
        };

        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, 0, seg_ref.object_size);
        let bytes = if let Some(bytes) = cache.get(&cache_key) {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            // Served from cache: no S3 GET on this call.
            fetch_span.record("s3_requests", 0u64);
            fetch_span.record("s3_bytes", 0u64);
            bytes
        } else {
            accounting.record_cache_miss();
            let bytes = async {
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
            // A miss issues one store GET for the resulting bytes. A
            // single-flight follower that rode another caller's GET is the
            // rare exception; recording one GET here still bounds this call's
            // own attribution and never under-counts the query total, which is
            // what the span is for.
            fetch_span.record("s3_requests", 1u64);
            fetch_span.record("s3_bytes", bytes.len() as u64);
            bytes
        };
        Ok(Some(bytes))
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
/// 1). Mirrors `crate::fetcher::DEFAULT_SUFFIX_LEN`. The RLOG tail metadata
/// (SKIP_IDX, BLOOM, POSTINGS) and the footer sit after the (large) BLOCKS
/// section, so a suffix of this size normally covers the whole tail in one GET
/// and no extra metadata GET is needed for them.
pub const DEFAULT_LOG_SUFFIX_LEN: u64 = 64 * 1024;

/// Default maximum gap between two wanted block extents that still get coalesced
/// into a single GET. Same 64 KiB start as `crate::fetcher::DEFAULT_COALESCE_GAP`
/// (ADR-0107 decision 1: "start at RSEG's 64 KiB").
pub const DEFAULT_LOG_COALESCE_GAP: u64 = 64 * 1024;

/// Default object size at or below which the size-threshold pre-probe crossover
/// reads the whole object in one GET instead of probing and range-fetching
/// (ADR-0107 decision 1, "size-threshold, pre-probe whole-object read"). Mirrors
/// `crate::fetcher::DEFAULT_WHOLE_OBJECT_THRESHOLD` (512 KiB) exactly: below it
/// the extra probe + per-range round trips cost more than the bytes they save.
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
    /// section. `None` sends every GET to the store.
    cache: Option<Arc<Cache<crate::fetcher::CacheFetchError>>>,
    suffix_len: u64,
    coalesce_gap: u64,
    whole_object_threshold: u64,
    coverage_threshold: f64,
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
            suffix_len: DEFAULT_LOG_SUFFIX_LEN,
            coalesce_gap: DEFAULT_LOG_COALESCE_GAP,
            whole_object_threshold: DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            coverage_threshold: DEFAULT_LOG_COVERAGE_THRESHOLD,
            get_semaphore: Arc::new(Semaphore::new(DEFAULT_LOG_MAX_CONCURRENT_GETS)),
        }
    }

    #[must_use]
    pub fn with_config(mut self, cfg: RlogConfig) -> Self {
        self.cfg = cfg;
        self
    }

    #[must_use]
    pub fn with_cache(mut self, cache: Arc<Cache<crate::fetcher::CacheFetchError>>) -> Self {
        self.cache = Some(cache);
        self
    }

    #[must_use]
    pub fn with_suffix_len(mut self, n: u64) -> Self {
        self.suffix_len = n.max(1);
        self
    }

    #[must_use]
    pub fn with_coalesce_gap(mut self, n: u64) -> Self {
        self.coalesce_gap = n;
        self
    }

    /// Sets the size-threshold pre-probe crossover. An object whose size is at or
    /// below `n` is read whole in one GET; `0` disables the size crossover (every
    /// object takes the probe + range path), which the tests use to force the
    /// ranged path on a small fixture.
    #[must_use]
    pub fn with_whole_object_threshold(mut self, n: u64) -> Self {
        self.whole_object_threshold = n;
        self
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
        if let Some(bytes) = cache.get(&cache_key) {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            return Ok((bytes, false));
        }
        accounting.record_cache_miss();
        let bytes = cache
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
        Ok((bytes, true))
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
        // coalesce instead of each paying the GET.
        if seg_ref.object_size <= self.whole_object_threshold {
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

        // Etag-establishing probe: a suffix GET that pins the etag every later
        // live GET is checked against, and carries the footer (and, for a small
        // object, the whole tail directory). Cache-routed like every other GET
        // here, so concurrent partitions' probes collapse onto one request.
        let suffix = self.suffix_len.min(total_size);
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

        let mut asm = ObjectAssembler::new(total);
        asm.place(key, probe_start, &probe_bytes)?;

        // Footer: parse from the probe suffix, chasing one range if the suffix
        // did not cover the whole footer (mirrors `SegmentFetcher::open_segment`).
        let footer = match open_from_suffix(&probe_bytes, total_size)
            .map_err(|source| corrupt(key, source))?
        {
            SuffixOutcome::Ready(footer) => footer,
            SuffixOutcome::NeedRange { offset, len } => {
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

        let skip_desc = footer
            .section(kind::SKIP_IDX)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing SKIP_IDX".into())))?;
        let blocks_desc = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| corrupt(key, LogSegError::Corrupted("missing BLOCKS".into())))?;

        // SKIP_IDX first and alone. The coverage crossover below can decide the
        // whole object is cheaper than the candidate ranges, and every OTHER
        // section it would have fetched first is then wasted: a wide-time-range
        // query on a large object would pay probe + section GETs and THEN a
        // whole-object GET, strictly worse than the plain whole-object path it
        // falls back to. Resolving the candidate extents needs SKIP_IDX and
        // nothing else, so only SKIP_IDX is fetched before the decision.
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

        let extents = self.resolve_extents(key, &skip, blocks_desc.offset, ts_min_ns, ts_max_ns)?;
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

        // The remaining non-BLOCKS sections: STREAM_DIR/FIELD_DIR (object front)
        // and any tail section (BLOOM/POSTINGS) a short probe missed. The reader
        // re-verifies each section's crc on decode, so a corrupt section hit
        // fails closed there (ADR-0046).
        for section in &footer.sections {
            if section.kind == kind::BLOCKS {
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

    /// Resolve each candidate block index (from `skip.candidate_blocks`) to its
    /// absolute byte extent and stored crc. The byte extent is always the block's
    /// full extent from its SKIP_IDX level-0 entry, never a sub-block slice
    /// (ADR-0107 decision 1).
    fn resolve_extents(
        &self,
        key: &str,
        skip: &SkipIndex,
        blocks_offset: u64,
        ts_min_ns: i64,
        ts_max_ns: i64,
    ) -> Result<Vec<BlockExtent>, LogFetchError> {
        let candidates = skip.candidate_blocks(ts_min_ns, ts_max_ns, None, &[]);
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
        let runs = coalesce_extents(&missing, self.coalesce_gap);
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
        let lead_bytes = cache
            .get_or_fetch(lead_key, || async {
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
            let bytes = cache
                .get_or_fetch(block_key, || async {
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
    let mut ranges: Vec<(u64, u64)> = extents.iter().map(|e| (e.abs_start, e.abs_end())).collect();
    ranges.sort_by_key(|r| r.0);
    let mut out: Vec<BlockExtent> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = out.last_mut()
            && start <= last.abs_end().saturating_add(max_gap)
        {
            let new_end = last.abs_end().max(end);
            last.len = new_end - last.abs_start;
            continue;
        }
        out.push(BlockExtent {
            abs_start: start,
            len: end - start,
            crc32c: 0,
        });
    }
    out
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
