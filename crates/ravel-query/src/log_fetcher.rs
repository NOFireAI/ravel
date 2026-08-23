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
//! v1 fetches the whole object with a single [`GetRange::Full`]. RLOG objects
//! are not yet large enough to justify the suffix-then-range-chase read
//! `SegmentFetcher` uses for RSEG; this may deserve revisiting once they
//! grow.

use std::sync::Arc;

use crate::erasure::ErasurePredicate;
use bytes::Bytes;
use ravel_cache::{Cache, CacheKey, SingleFlightError};
use ravel_catalog::SegmentRef;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, BlockScan, ColumnSelection, ColumnarBlockView, LogRecord, LogSegError, LogStreamId,
    Predicate, RlogConfig, RlogReader, ScanStats, read_section,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
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
}

impl LogSegmentFetcher {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        LogSegmentFetcher {
            store,
            cfg: RlogConfig::default(),
            cache: None,
        }
    }

    /// Overrides the [`RlogConfig`] used for section-size caps when decoding.
    #[must_use]
    pub fn with_config(mut self, cfg: RlogConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Wires ADR-0046's read cache into
    /// [`fetch_accounted_with_tenant`](Self::fetch_accounted_with_tenant).
    #[must_use]
    pub fn with_cache(mut self, cache: Arc<Cache<crate::fetcher::CacheFetchError>>) -> Self {
        self.cache = Some(cache);
        self
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
    /// The whole object is still fetched with one [`GetRange::Full`] GET: this
    /// bounds *decoded* memory, not raw bytes. A ranged block read is
    /// [`ravel_logseg::RlogRangeReader`]'s territory and out of scope here
    /// (ADR-0087 decision 3).
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
    /// issuing an extra distinct GET.
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
            .map_err(|err| LogFetchError::Store {
                    key: key.to_string(),
                    source: match err {
                        SingleFlightError::Upstream(crate::fetcher::CacheFetchError::Store(
                            source,
                        )) => crate::fetcher::clone_store_error(&source),
                        // Unreachable in practice: this funnel never sets an
                        // `expected_etag` (module doc, no suffix/range-chase
                        // read exists to compare against), so its closure
                        // never constructs `EtagChanged`. Handled explicitly
                        // rather than a wildcard so a future etag check added
                        // here can't silently fall through this arm.
                        SingleFlightError::Upstream(
                            crate::fetcher::CacheFetchError::EtagChanged { .. },
                        ) => StoreError::Transient(
                            "cache single-flight closure reported an etag change, which this funnel never produces"
                                .to_string(),
                        ),
                        SingleFlightError::LeaderLost => StoreError::Transient(
                            "cache single-flight leader lost before producing a result".to_string(),
                        ),
                    },
                })?;
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
