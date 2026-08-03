//! LogSegmentFetcher: a thin fetch abstraction over one RLOG log segment
//! (crate `ravel-logseg`, docs/log-segment-format.md; ADR-0033, epic #236).
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
//! `SegmentFetcher` uses for RSEG; see the module note in the issue #238
//! report for when that may deserve revisiting.

use std::sync::Arc;

use bytes::Bytes;
use ravel_cache::{Cache, CacheKey, SingleFlightError};
use ravel_catalog::SegmentRef;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::stream_dir::StreamDir;
use ravel_logseg::{
    AttrValue, LogRecord, LogSegError, LogStreamId, Predicate, RlogConfig, RlogReader, ScanStats,
    read_section,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::canonical_attr_bytes;

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
}

/// The records matching one fetch, plus the reader's own scan pruning counters.
#[derive(Clone, Debug)]
pub struct LogFetchOutput {
    pub records: Vec<LogRecord>,
    pub stats: ScanStats,
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
    /// decoder in `ravel-logseg` or an entry-walking decoder here. Issue #238
    /// deliberately adds neither. Issue #239, which builds the real logs query
    /// path, owns that decision and must do one of two things: make the match
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
    /// equality on the returned records itself. Issue #239 must either do that
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
    /// Wiring them onto this funnel is issue #424; `fetch` stays the
    /// unaccounted entry point until that lands, so those callers need no
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
        let got = self
            .store
            .get(key, GetRange::Full)
            .await
            .map_err(|source| LogFetchError::Store {
                key: key.to_string(),
                source,
            })?;
        accounting.record_s3_request(AccountedOp::Get);
        accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
        self.scan_bytes(key, &got.data, query)
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
    /// scope here: it is a `ravel-sql` change, and issue #424 already tracks
    /// moving those callers onto the accounted funnel in the first place.
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
        if !Self::ts_range_relevant(seg_ref, query.ts_min_ns, query.ts_max_ns) {
            return Ok(None);
        }
        let key = &seg_ref.data_object_key;

        let Some(cache) = &self.cache else {
            let got = self
                .store
                .get(key, GetRange::Full)
                .await
                .map_err(|source| LogFetchError::Store {
                    key: key.to_string(),
                    source,
                })?;
            accounting.record_s3_request(AccountedOp::Get);
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
            return self.scan_bytes(key, &got.data, query);
        };

        let cache_key = CacheKey::new(tenant_hash.0, seg_ref.content_hash, 0, seg_ref.object_size);
        let bytes = if let Some(bytes) = cache.get(&cache_key) {
            accounting.record_cache_hit();
            accounting.add_cache_bytes(bytes.len() as u64);
            bytes
        } else {
            accounting.record_cache_miss();
            cache
                .get_or_fetch(cache_key, || async move {
                    let got = self.store.get(key, GetRange::Full).await?;
                    accounting.record_s3_request(AccountedOp::Get);
                    accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
                    Ok(got.data)
                })
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
                })?
        };
        self.scan_bytes(key, &bytes, query)
    }

    /// Shared tail of both fetch entry points: resolve stream-attribute
    /// equalities against STREAM_DIR (over-approximating, see
    /// [`matching_streams`](Self::matching_streams)), build the combined exact
    /// predicate, and scan it with `query.prune` as the prune-only channel.
    /// Identical regardless of whether `bytes` came from the store or the cache
    /// -- `RlogReader::scan_pruned`'s block-level skip-index and bloom
    /// verification run unconditionally either way, so a corrupt cache entry
    /// fails exactly like a corrupt store read.
    fn scan_bytes(
        &self,
        key: &str,
        bytes: &Bytes,
        query: &LogQuery,
    ) -> Result<Option<LogFetchOutput>, LogFetchError> {
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
        let pred = Predicate::And(arms);

        let reader = RlogReader::new(bytes, &self.cfg).map_err(|source| corrupt(key, source))?;
        // `prune` is passed as the reader's prune-only channel, never folded
        // into `pred`: an arm there would become an exact per-row filter and
        // drop resource/scope-only matches (docs/adrs/0049-rlog-postings.md
        // amendment 2026-08-03). An empty channel makes this identical to
        // `scan`.
        let (records, stats) = reader
            .scan_pruned(&pred, &query.prune)
            .map_err(|source| corrupt(key, source))?;
        Ok(Some(LogFetchOutput { records, stats }))
    }

    /// Decodes the STREAM_DIR section of an object from its own public section
    /// descriptor, using the crate's public whole-section reader (issue #221).
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

fn corrupt(key: &str, source: LogSegError) -> LogFetchError {
    LogFetchError::Corrupt {
        key: key.to_string(),
        source,
    }
}
